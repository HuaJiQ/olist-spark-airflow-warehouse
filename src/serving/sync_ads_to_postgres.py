
from pathlib import Path
from getpass import getpass

import pyarrow.dataset as ds
import psycopg


# 项目根目录
ROOT = Path(__file__).resolve().parents[2]

# Parquet目录、数据库表名、字段的映射
TABLES = {
    "daily_sales": (
        "ads_daily_sales",
        [
            "dt",
            "total_order_count",
            "delivered_order_count",
            "delivered_item_amount",
            "delivered_freight_amount",
            "delivered_aov",
        ],
    ),
    "customer_purchase_profile": (
        "ads_customer_purchase_profile",
        [
            "customer_unique_id",
            "delivered_order_count",
            "delivered_item_amount",
            "is_repeat_customer",
        ],
    ),
    "customer_repurchase": (
        "ads_customer_repurchase",
        [
            "purchasing_customer_count",
            "single_purchase_customer_count",
            "repeat_customer_count",
            "repurchase_rate",
        ],
    ),
    "category_sales": (
        "ads_category_sales",
        [
            "category_name",
            "item_count",
            "order_count",
            "item_amount",
            "freight_amount",
        ],
    ),
}


def main():
    print("SYNC SCRIPT STARTED", flush=True)
    datasets = {}

    # 1. 预先检查四张Parquet表
    for folder, (table, columns) in TABLES.items():
        path = ROOT / "data" / "ads" / folder

        dataset = ds.dataset(str(path), format="parquet")

        missing_columns = set(columns) - set(
            dataset.schema.names
        )

        assert not missing_columns, (f"{folder} missing columns: {missing_columns}")

        datasets[folder] = dataset

        print(
            f"[SOURCE] {folder}: "
            f"{dataset.count_rows()} rows",
            flush=True
        )

    # 2. 连接PostgreSQL
    password = getpass("PostgreSQL password: ")

    with psycopg.connect(
        host="127.0.0.1",
        port=5432,
        dbname="olist_warehouse",
        user="olist",
        password=password,
        connect_timeout=10,
    ) as conn:

        with conn.cursor() as cur:
            # 3. 同一个事务内清空旧的ADS快照
            cur.execute("""
                TRUNCATE TABLE
                    ads_daily_sales,
                    ads_customer_purchase_profile,
                    ads_customer_repurchase,
                    ads_category_sales
            """)

            print("[SYNC] Old snapshot cleared")

            # 4. 按批次从Parquet复制到PostgreSQL
            for folder, (table, columns) in TABLES.items():
                dataset = datasets[folder]

                copy_columns = (
                    ["metric_id"] + columns
                    if folder == "customer_repurchase"
                    else columns
                )

                column_sql = ", ".join(copy_columns)

                copy_sql = (
                    f"COPY {table} ({column_sql}) "
                    "FROM STDIN"
                )

                inserted_rows = 0

                with cur.copy(copy_sql) as copy:
                    for batch in dataset.to_batches(
                        columns=columns,
                        batch_size=5000
                    ):
                        for record in batch.to_pylist():
                            values = [
                                record[column]
                                for column in columns
                            ]

                            # 整体复购表新增固定主键
                            if folder == "customer_repurchase":
                                values = [1] + values

                            copy.write_row(tuple(values))
                            inserted_rows += 1

                # 5. 导入后检查行数
                cur.execute(
                    f"SELECT COUNT(*) FROM {table}"
                )

                db_rows = cur.fetchone()[0]
                expected_rows = dataset.count_rows()

                print(
                    f"[CHECK] {table}: "
                    f"source={expected_rows}, "
                    f"database={db_rows}",
                    flush=True
                )

                assert inserted_rows == expected_rows
                assert db_rows == expected_rows

            # 6. 全部成功后才提交事务
            print("ADS POSTGRES SYNC CHECK: PASS",flush=True)

    print("ADS POSTGRES SYNC COMMITTED: PASS",flush=True)


if __name__ == "__main__":
    main()
