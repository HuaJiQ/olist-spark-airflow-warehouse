
import argparse
import json
from decimal import Decimal
from pathlib import Path

import pyarrow.dataset as ds

from batch_manager import (
    BATCH_ROOT,
    load_batch,
    now_utc,
    update_status,
)

REQUIRED_TABLES = {
    "daily_sales": "dt",
    "customer_purchase_profile": "customer_unique_id",
    "customer_repurchase": None,
    "category_sales": "category_name",
}


def check(condition, message):
    if not condition:
        raise ValueError(message)
    print(f"[PASS] {message}", flush=True)


def total(rows, column):
    return sum(
        (Decimal(str(row[column] or 0)) for row in rows),
        Decimal("0")
    )


def validate_batch(batch_id):
    manifest = load_batch(batch_id)

    # 删除旧的质量报告，防止把历史PASS误认为本次通过
    report_path = BATCH_ROOT / batch_id / "quality_report.json"

    if report_path.exists():
        report_path.unlink()

    check(
        manifest["status"] == "RUNNING",
        "Batch is RUNNING"
    )

    batch_ads_root = (
        BATCH_ROOT / batch_id / "ads"
    ).resolve()

    data = {}

    # 1. 检查四张表是否齐全
    check(
        set(REQUIRED_TABLES).issubset(
            manifest["tables"].keys()
        ),
        "All required tables are recorded"
    )

    for name, primary_key in REQUIRED_TABLES.items():
        info = manifest["tables"][name]

        # 只能读取当前批次的指定ADS目录
        expected = (batch_ads_root / name).resolve()
        actual = Path(info["path"]).resolve()

        check(
            actual == expected,
            f"{name}: path belongs to batch"
        )

        check(
            actual.is_dir(),
            f"{name}: directory exists"
        )

        dataset = ds.dataset(
            str(actual),
            format="parquet"
        )

        if primary_key is not None:
            check(
                primary_key in dataset.schema.names,
                f"{name}: key column exists"
            )

        rows = dataset.to_table().to_pylist()
        data[name] = rows

        check(
            len(rows) == info["row_count"],
            f"{name}: row count matches manifest"
        )

        check(
            len(rows) > 0,
            f"{name}: nonempty"
        )

        if primary_key is not None:
            keys = [row[primary_key] for row in rows]

            check(
                all(key is not None for key in keys),
                f"{name}: no null keys"
            )

            check(
                len(keys) == len(set(keys)),
                f"{name}: unique keys"
            )
        else:
            check(
                len(rows) == 1,
                f"{name}: exactly one summary row"
            )

    # 2. 检查销售金额守恒
    daily = data["daily_sales"]
    category = data["category_sales"]

    daily_amount = total(
        daily, "delivered_item_amount"
    )
    category_amount = total(
        category, "item_amount"
    )

    check(
        daily_amount == category_amount,
        "Daily/category item amount reconciled"
    )

    # 3. 检查客户画像与整体复购指标
    profiles = data["customer_purchase_profile"]
    summary_rows = data["customer_repurchase"]

    check(
        len(summary_rows) == 1,
        "Exactly one repurchase summary"
    )

    summary = summary_rows[0]

    check(
        len(profiles)
        == summary["purchasing_customer_count"],
        "Purchasing customer count reconciled"
    )

    repeat_count = sum(
        1 for row in profiles
        if row["delivered_order_count"] > 1
    )

    check(
        repeat_count == summary["repeat_customer_count"],
        "Repeat customer count reconciled"
    )

    check(
        len(profiles) - repeat_count
        == summary["single_purchase_customer_count"],
        "Single purchase customer count reconciled"
    )

    check(
        all(
            row["delivered_order_count"] >= 1
            and row["is_repeat_customer"]
                == (row["delivered_order_count"] > 1)
            for row in profiles
        ),
        "Customer repeat flags are consistent"
    )

    rate = (
        repeat_count / len(profiles)
    )

    check(
        abs(rate - summary["repurchase_rate"]) < 1e-10,
        "Repurchase rate reconciled"
    )

    # 4. 保存质量报告
    report = {
        "batch_id": batch_id,
        "checked_at": now_utc(),
        "result": "PASS",
        "table_rows": {
            name: len(rows)
            for name, rows in data.items()
        },
        "delivered_item_amount": str(daily_amount),
        "repeat_customer_count": repeat_count,
    }

    with report_path.open(
        "w", encoding="utf-8"
    ) as f:
        json.dump(
            report, f,
            ensure_ascii=False,
            indent=2
        )

    print(
        f"[QUALITY REPORT] {report_path}",
        flush=True
    )
    print("BATCH QUALITY GATE: PASS", flush=True)

    # 全部检查通过后，自动转换批次状态
    update_status(batch_id, "VALIDATED")

    print(
        "BATCH STATUS: VALIDATED",
        flush=True
    )

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--batch-id",
        required=True
    )

    args = parser.parse_args()
    validate_batch(args.batch_id)


if __name__ == "__main__":
    main()
