import argparse
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OLD_PATH = "/workspace/data/ads/daily_sales"
DEFAULT_NEW_PATH = "/workspace/data/m10/dws/sales_daily"

def main():
    parser = argparse.ArgumentParser(
        description="Validate M10 sales consistency"
    )

    parser.add_argument(
        "--batch-id",
        default=None,
        help="Optional batch ID"
    )

    args = parser.parse_args()

    if args.batch_id is not None:
        if not re.fullmatch(
            r"[A-Za-z0-9_-]{1,64}",
            args.batch_id
        ):
            raise ValueError("Invalid batch_id")

        new_path = (
            f"/workspace/data/batches/"
            f"{args.batch_id}/m10/dws/sales_daily"
        )
    else:
        new_path = DEFAULT_NEW_PATH

    print(f"[VALIDATE M10] New DWS: {new_path}", flush=True)

    spark = (
        SparkSession.builder
        .appName("M10-Sales-Regression-Test")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        old = spark.read.parquet(OLD_PATH)
        new = spark.read.parquet(new_path)

        # 1. 选择业务口径一致的字段
        old_metrics = old.select(
            "dt",
            F.col("total_order_count").alias("old_orders"),
            F.col("delivered_order_count").alias(
                "old_delivered"
            ),
            F.col("delivered_item_amount").alias(
                "old_amount"
            )
        )

        new_metrics = new.select(
            "dt",
            F.col("order_count").alias("new_orders"),
            F.col("delivered_order_count").alias(
                "new_delivered"
            ),
            F.col("delivered_item_amount").alias(
                "new_amount"
            )
        )

        # 2. FULL OUTER JOIN确保双方日期都被覆盖
        compared = old_metrics.join(
            new_metrics,
            on="dt",
            how="full_outer"
        )

        # 3. 日期覆盖检查
        missing_dates = compared.filter(
            F.col("old_orders").isNull()
            | F.col("new_orders").isNull()
        ).count()

        # 4. 订单数量比较
        count_mismatches = compared.filter(
            ~F.col("old_orders").eqNullSafe(
                F.col("new_orders")
            )
            | ~F.col("old_delivered").eqNullSafe(
                F.col("new_delivered")
            )
        ).count()

        # 5. 金额比较
        # 无已送达订单的日期，SUM可能为NULL或0
        # 此处仅对比该指标时统一为0
        zero = F.lit(0).cast("decimal(28,2)")

        amount_mismatches = compared.filter(
            F.coalesce(F.col("old_amount"), zero)
            !=
            F.coalesce(F.col("new_amount"), zero)
        ).count()

        print("===== SALES CONSISTENCY =====")
        print("Old ADS rows:", old.count())
        print("New DWS rows:", new.count())
        print("Missing dates:", missing_dates)
        print("Count mismatches:", count_mismatches)
        print("Amount mismatches:", amount_mismatches)

        if (
            missing_dates > 0
            or count_mismatches > 0
            or amount_mismatches > 0
        ):
            print("===== DIFFERENCE SAMPLE =====")

            compared.filter(
                F.col("old_orders").isNull()
                | F.col("new_orders").isNull()
                | ~F.col("old_orders").eqNullSafe(
                    F.col("new_orders")
                )
                | ~F.col("old_delivered").eqNullSafe(
                    F.col("new_delivered")
                )
                | (
                    F.coalesce(F.col("old_amount"), zero)
                    !=
                    F.coalesce(F.col("new_amount"), zero)
                )
            ).show(20, truncate=False)

            raise ValueError("Sales consistency failed")

        print("M10 SALES CONSISTENCY: PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()