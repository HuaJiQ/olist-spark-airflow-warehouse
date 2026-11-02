
import argparse
import sys
from decimal import Decimal
from getpass import getpass
from pathlib import Path

import psycopg
import pyarrow.dataset as ds

from batch_manager import BATCH_ROOT, load_batch, update_status
from check_publish_ready import check_publish_ready


ROOT = Path(__file__).resolve().parents[2]

# 复用M4已经验证过的表名和字段映射
sys.path.insert(0, str(ROOT / "src" / "serving"))
from sync_ads_to_postgres import TABLES


def load_datasets(batch_id):
    datasets = {}

    for folder, (_, columns) in TABLES.items():
        path = BATCH_ROOT / batch_id / "ads" / folder

        dataset = ds.dataset(str(path), format="parquet")

        missing = set(columns) - set(dataset.schema.names)

        if missing:
            raise ValueError(
                f"{folder}: missing columns {missing}"
            )

        datasets[folder] = dataset

    return datasets


def copy_table(cur, folder, table, columns, dataset):
    copy_columns = (
        ["metric_id"] + columns
        if folder == "customer_repurchase"
        else columns
    )

    sql_columns = ", ".join(copy_columns)

    copy_sql = (
        f"COPY {table} ({sql_columns}) FROM STDIN"
    )

    inserted = 0

    with cur.copy(copy_sql) as copy:
        for batch in dataset.to_batches(
            columns=columns,
            batch_size=5000
        ):
            for record in batch.to_pylist():
                values = [record[col] for col in columns]

                if folder == "customer_repurchase":
                    values = [1] + values

                copy.write_row(tuple(values))
                inserted += 1

    cur.execute(f"SELECT COUNT(*) FROM {table}")
    db_rows = cur.fetchone()[0]
    source_rows = dataset.count_rows()

    if inserted != source_rows or db_rows != source_rows:
        raise ValueError(
            f"{table}: row count mismatch"
        )

    print(
        f"[COPY CHECK] {table}: {db_rows} rows, PASS",
        flush=True
    )

    return db_rows


def check_database_metrics(cur):
    # 不同统计维度的商品金额必须一致
    cur.execute("""
        SELECT
            (SELECT SUM(delivered_item_amount)
             FROM ads_daily_sales),
            (SELECT SUM(item_amount)
             FROM ads_category_sales)
    """)

    daily_amount, category_amount = cur.fetchone()

    if daily_amount != category_amount:
        raise ValueError("ADS amount reconciliation failed")

    # 从客户画像重新计算复购客户数
    cur.execute("""
        SELECT
            COUNT(*),
            COUNT(*) FILTER (
                WHERE delivered_order_count > 1
            )
        FROM ads_customer_purchase_profile
    """)

    customers, repeat_customers = cur.fetchone()

    cur.execute("""
        SELECT
            purchasing_customer_count,
            single_purchase_customer_count,
            repeat_customer_count,
            repurchase_rate
        FROM ads_customer_repurchase
        WHERE metric_id = 1
    """)

    result = cur.fetchone()

    if result is None:
        raise ValueError("Repurchase summary missing")

    total, single, repeat, rate = result

    if not (
        customers == total
        and repeat_customers == repeat
        and single == total - repeat
    ):
        raise ValueError("Customer metric reconciliation failed")

    expected_rate = repeat / total

    if abs(expected_rate - rate) > 1e-10:
        raise ValueError("Repurchase rate reconciliation failed")

    print(
        f"[AMOUNT CHECK] {daily_amount} = "
        f"{category_amount}: PASS",
        flush=True
    )

    print(
        f"[CUSTOMER CHECK] repeat={repeat}: PASS",
        flush=True
    )

    return daily_amount


def publish(
    batch_id,
    apply,
    fail_after_copy=None,
    fail_after_commit=False
):
    manifest = load_batch(batch_id)

    print(f"[BATCH] {batch_id}", flush=True)
    print(f"[STATUS] {manifest['status']}", flush=True)

    if manifest["status"] not in ("VALIDATED", "PUBLISHED"):
        raise ValueError("Batch is not ready for publication")

    if manifest["status"] == "VALIDATED":
        check_publish_ready(batch_id)

    datasets = load_datasets(batch_id)

    if not apply:
        print("DRY RUN ONLY: no database changes", flush=True)
        return

    password = getpass("PostgreSQL password: ")

    # 连接上下文正常结束时提交，异常时回滚
    with psycopg.connect(
        host="127.0.0.1",
        port=5432,
        dbname="olist_warehouse",
        user="olist",
        password=password,
        connect_timeout=10,
    ) as conn:

        with conn.cursor() as cur:

            # 事务级互斥锁：同一套发布程序串行执行
            cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (2026110201,)
            )

            cur.execute("""
                SELECT batch_id
                FROM ads_current_batch
                WHERE singleton_id = 1
            """)
            current_row = cur.fetchone()
            current_batch = (
                current_row[0] if current_row else None
            )

            cur.execute("""
                SELECT batch_id
                FROM ads_publication_log
                WHERE batch_id = %s
            """, (batch_id,))
            already_published = cur.fetchone() is not None

            if already_published:
                if current_batch != batch_id:
                    raise ValueError(
                        "Batch was published previously "
                        "but is not the current batch"
                    )

                print(
                    "[RECOVERY] Database already published",
                    flush=True
                )

            else:
                if manifest["status"] != "VALIDATED":
                    raise ValueError(
                        "Unrecorded batch must be VALIDATED"
                    )

                print("[PUBLISH] Replacing ADS snapshot")

                cur.execute("""
                    TRUNCATE TABLE
                        ads_daily_sales,
                        ads_customer_purchase_profile,
                        ads_customer_repurchase,
                        ads_category_sales
                """)

                counts = {}

                for folder, (table, columns) in TABLES.items():
                    counts[folder] = copy_table(
                        cur, folder, table, columns,
                        datasets[folder]
                    )

                    print(
                        f"[PUBLISH] Completed {folder}",
                        flush=True
                    )

                    if fail_after_copy == folder:
                        raise RuntimeError(
                            f"INJECTED FAILURE after copying {folder}"
                        )

                check_database_metrics(cur)

                # 记录成功发布的批次
                cur.execute("""
                    INSERT INTO ads_publication_log (
                        batch_id,
                        daily_sales_rows,
                        customer_profile_rows,
                        customer_repurchase_rows,
                        category_sales_rows,
                        status
                    )
                    VALUES (%s, %s, %s, %s, %s, 'PUBLISHED')
                """, (
                    batch_id,
                    counts["daily_sales"],
                    counts["customer_purchase_profile"],
                    counts["customer_repurchase"],
                    counts["category_sales"],
                ))

                # 同一事务内更新当前服务批次
                cur.execute("""
                    INSERT INTO ads_current_batch (
                        singleton_id, batch_id
                    )
                    VALUES (1, %s)
                    ON CONFLICT (singleton_id)
                    DO UPDATE SET
                        batch_id = EXCLUDED.batch_id,
                        updated_at = CURRENT_TIMESTAMP
                """, (batch_id,))

                print(
                    "[PUBLISH] Database checks passed",
                    flush=True
                )

    # 只有走出连接上下文，数据库提交才算完成
    print("DATABASE PUBLICATION COMMITTED: PASS", flush=True)
    # ===== 测试专用：模拟数据库提交后程序崩溃 =====
    if fail_after_commit:
        raise RuntimeError(
            "INJECTED FAILURE after database COMMIT"
        )

    # 数据库提交后再更新本地状态
    if load_batch(batch_id)["status"] == "VALIDATED":
        update_status(batch_id, "PUBLISHED")

    print("BATCH PUBLICATION: PASS", flush=True)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--batch-id", required=True)

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually publish to PostgreSQL"
    )
    parser.add_argument(
        "--fail-after-copy",
        choices=list(TABLES.keys()),
        help="Inject a failure after copying a specified table"
    )
    parser.add_argument(
        "--fail-after-commit",
        action="store_true",
        help="Simulate failure after database commit"
    )


    args = parser.parse_args()

    if args.fail_after_copy and not args.apply:
        parser.error(
            "--fail-after-copy requires --apply"
        )

    if args.fail_after_commit and not args.apply:
        parser.error("--fail-after-commit requires --apply")

    if args.fail_after_commit and args.fail_after_copy:
        parser.error("Choose only one failure injection mode")

    publish(
        args.batch_id,
        args.apply,
        args.fail_after_copy,
        args.fail_after_commit
    )


if __name__ == "__main__":
    main()
