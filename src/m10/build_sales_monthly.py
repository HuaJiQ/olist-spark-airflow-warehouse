import argparse
import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DEFAULT_SOURCE = "/workspace/data/m10/dws/sales_daily"
DEFAULT_TARGET = "/workspace/data/m10/ads/sales_monthly"

COUNT_COLUMNS = [
    "order_count",
    "item_order_count",
    "payment_order_count",
    "delivered_order_count",
    "canceled_order_count",
    "orders_without_items",
    "orders_without_payment",
]

AMOUNT_COLUMNS = [
    "item_amount",
    "freight_amount",
    "payment_amount",
    "delivered_item_amount",
]


def main():
    parser = argparse.ArgumentParser(
        description="Build M10 monthly sales ADS"
    )

    parser.add_argument(
        "--batch-id",
        default=None,
        help="Optional isolated batch ID"
    )

    args = parser.parse_args()

    if args.batch_id is not None:
        if not re.fullmatch(
            r"[A-Za-z0-9_-]{1,64}",
            args.batch_id
        ):
            raise ValueError("Invalid batch_id")

        base = (
            f"/workspace/data/batches/"
            f"{args.batch_id}/m10"
        )

        source = f"{base}/dws/sales_daily"
        target = f"{base}/ads/sales_monthly"

    else:
        source = DEFAULT_SOURCE
        target = DEFAULT_TARGET

    print(f"[M10 MONTHLY INPUT] {source}", flush=True)
    print(f"[M10 MONTHLY OUTPUT] {target}", flush=True)

    spark = (
        SparkSession.builder
        .appName("Olist-M10-ADS-Sales-Monthly")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. 检查数据来源和输出目录
        if not Path(source).is_dir():
            raise FileNotFoundError(source)

        if Path(target).exists():
            raise FileExistsError(
                f"Output already exists: {target}"
            )

        daily = spark.read.parquet(source)

        # 2. 将日日期映射为月初日期
        monthly_base = daily.withColumn(
            "month",
            F.trunc(F.col("dt"), "month")
        )

        # 3. 将日级指标汇总到月级
        monthly = monthly_base.groupBy("month").agg(
            *[
                F.sum(column).alias(column)
                for column in COUNT_COLUMNS + AMOUNT_COLUMNS
            ]
        )

        # 4. 月度平均订单商品金额
        monthly = monthly.withColumn(
            "avg_item_amount_per_order",
            F.when(
                F.col("item_order_count") > 0,
                F.round(
                    F.col("item_amount")
                    / F.col("item_order_count"),
                    2
                )
            )
        )

        # 5. 最终已送达订单占比
        monthly = monthly.withColumn(
            "delivered_order_rate",
            F.when(
                F.col("order_count") > 0,
                F.round(
                    F.col("delivered_order_count")
                    / F.col("order_count"),
                    4
                )
            )
        )

        print("===== MONTHLY SALES =====")

        monthly.orderBy("month").show(
            40, truncate=False
        )

        # 6. 检查月度粒度
        monthly_rows = monthly.count()

        invalid_months = monthly.filter(
            F.col("month").isNull()
        ).count()

        duplicate_months = (
            monthly.groupBy("month")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print("Monthly rows:", monthly_rows)
        print("Invalid months:", invalid_months)
        print("Duplicate months:", duplicate_months)

        if invalid_months or duplicate_months:
            raise ValueError("Monthly grain check failed")

        # 7. 检查每一项加总指标的跨层守恒
        print("===== DWS TO ADS RECONCILIATION =====")

        for column in COUNT_COLUMNS + AMOUNT_COLUMNS:
            before = daily.agg(
                F.sum(column)
            ).first()[0]

            after = monthly.agg(
                F.sum(column)
            ).first()[0]

            print(
                f"[RECONCILE] {column}: "
                f"DWS={before}, ADS={after}"
            )

            if before != after:
                raise ValueError(
                    f"Reconciliation failed: {column}"
                )

        # 8. 检查订单总数
        total_orders = monthly.agg(
            F.sum("order_count")
        ).first()[0]

        if total_orders != 99441:
            raise ValueError(
                f"Unexpected order count: {total_orders}"
            )

        # 9. 写入Parquet并读回
        monthly.write.mode("errorifexists").parquet(
            target
        )

        saved = spark.read.parquet(target)

        if saved.count() != monthly_rows:
            raise ValueError("Read-back count mismatch")

        before_schema = [
            (field.name, field.dataType)
            for field in monthly.schema.fields
        ]

        after_schema = [
            (field.name, field.dataType)
            for field in saved.schema.fields
        ]

        if before_schema != after_schema:
            raise ValueError(
                "Read-back column name/type mismatch"
            )

        print("M10 MONTHLY SALES QUALITY CHECK: PASS")
        print("Saved to:", target)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()