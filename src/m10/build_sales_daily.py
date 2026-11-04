import argparse
import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SOURCE = "/workspace/data/m9/dws/order_financial_summary"
DEFAULT_TARGET = "/workspace/data/m10/dws/sales_daily"


def main():
    parser = argparse.ArgumentParser(
        description="Build M10 daily sales"
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

        target = (
            f"/workspace/data/batches/"
            f"{args.batch_id}/m10/dws/sales_daily"
        )
    else:
        target = DEFAULT_TARGET

    print(f"[M10 DAILY OUTPUT] {target}", flush=True)

    spark = (
        SparkSession.builder
        .appName("Olist-M10-Sales-Daily")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        if not Path(SOURCE).is_dir():
            raise FileNotFoundError(SOURCE)

        if Path(target).exists():
            raise FileExistsError(
                f"Output already exists: {target}"
            )

        orders = spark.read.parquet(SOURCE)

        print("===== INPUT SCHEMA =====")
        orders.printSchema()

        # 1. 明确日期和业务状态
        base = (
            orders
            .withColumn("dt", F.col("purchase_date"))
            .withColumn(
                "is_delivered",
                F.col("order_status") == "delivered"
            )
            .withColumn(
                "is_canceled",
                F.col("order_status") == "canceled"
            )
        )

        # 2. 按订单购买日期聚合
        daily = (
            base.groupBy("dt")
            .agg(
                F.count("*").alias("order_count"),

                F.sum(
                    F.col("has_items").cast("long")
                ).alias("item_order_count"),

                F.sum(
                    F.col("has_payment").cast("long")
                ).alias("payment_order_count"),

                F.sum(
                    F.col("is_delivered").cast("long")
                ).alias("delivered_order_count"),

                F.sum(
                    F.col("is_canceled").cast("long")
                ).alias("canceled_order_count"),

                F.sum("item_count").alias(
                    "item_line_count"
                ),

                F.sum("item_amount").alias(
                    "item_amount"
                ),

                F.sum("freight_amount").alias(
                    "freight_amount"
                ),

                F.sum("payment_amount").alias(
                    "payment_amount"
                ),

                F.sum(
                    F.when(
                        F.col("is_delivered"),
                        F.col("item_amount")
                    )
                ).alias("delivered_item_amount"),

                F.sum(
                    (~F.col("has_items")).cast("long")
                ).alias("orders_without_items"),

                F.sum(
                    (~F.col("has_payment")).cast("long")
                ).alias("orders_without_payment")
            )
        )

        # 3. 客单价：有商品记录的订单作为分母
        daily = daily.withColumn(
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

        # 4. 基础质量检查
        source_orders = orders.count()

        target_orders = daily.agg(
            F.sum("order_count")
        ).first()[0]

        invalid_dates = daily.filter(
            F.col("dt").isNull()
        ).count()

        duplicate_dates = (
            daily.groupBy("dt")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print("Source orders:", source_orders)
        print("Aggregated orders:", target_orders)
        print("Invalid dates:", invalid_dates)
        print("Duplicate dates:", duplicate_dates)

        if source_orders != target_orders:
            raise ValueError("Order count reconciliation failed")

        if invalid_dates or duplicate_dates:
            raise ValueError("Invalid daily grain")

        # 5. 检查三项金额是否守恒
        for column in [
            "item_amount",
            "freight_amount",
            "payment_amount"
        ]:
            source_total = orders.agg(
                F.sum(column)
            ).first()[0]

            target_total = daily.agg(
                F.sum(column)
            ).first()[0]

            print(
                f"[RECONCILE] {column}: "
                f"source={source_total}, "
                f"daily={target_total}"
            )

            if source_total != target_total:
                raise ValueError(
                    f"Amount reconciliation failed: {column}"
                )

        # 6. 查看结果
        print("===== DAILY SALES SAMPLE =====")
        daily.orderBy("dt").show(10, truncate=False)

        # 7. 写入并读回
        daily.write.mode("errorifexists").parquet(target)

        saved = spark.read.parquet(target)

        if saved.count() != daily.count():
            raise ValueError("Read-back row count mismatch")

        before_fields = [
            (field.name, field.dataType)
            for field in daily.schema.fields
        ]

        after_fields = [
            (field.name, field.dataType)
            for field in saved.schema.fields
        ]

        if before_fields != after_fields:
            raise ValueError("Read-back schema mismatch")

        print("M10 SALES DAILY QUALITY CHECK: PASS")
        print("Saved to:", target)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()