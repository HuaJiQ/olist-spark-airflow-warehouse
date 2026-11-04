
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "/workspace/data/m9"

ORDER_PATH = f"{BASE}/dwd/fact_order"
SUMMARY_PATH = f"{BASE}/dws/order_financial_summary"


def main():

    spark = (
        SparkSession.builder
        .appName("Olist-M9-Order-Lifecycle-Audit")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        orders = spark.read.parquet(ORDER_PATH)
        summary = spark.read.parquet(SUMMARY_PATH)

        # 1. 关联订单和财务汇总
        df = orders.join(
            summary.select(
                "order_id",
                "item_count",
                "item_amount",
                "freight_amount",
                "payment_amount",
                "has_items",
                "has_payment"
            ),
            on="order_id",
            how="left"
        )

        # 2. 将时间字段转换为是否存在
        df = (
            df
            .withColumn(
                "has_approval",
                F.col("approved_ts").isNotNull()
            )
            .withColumn(
                "has_carrier",
                F.col("carrier_ts").isNotNull()
            )
            .withColumn(
                "has_delivery",
                F.col("delivered_ts").isNotNull()
            )
        )

        # 3. 分析缺少商品明细订单的生命周期
        missing_items = df.filter(
            ~F.col("has_items")
        )

        print("===== MISSING ITEMS: LIFECYCLE BY STATUS =====")

        missing_items.groupBy(
            "order_status"
        ).agg(
            F.count("*").alias("order_count"),

            F.sum(
                F.when(
                    F.col("has_approval"), 1
                ).otherwise(0)
            ).alias("approved_count"),

            F.sum(
                F.when(
                    F.col("has_carrier"), 1
                ).otherwise(0)
            ).alias("carrier_count"),

            F.sum(
                F.when(
                    F.col("has_delivery"), 1
                ).otherwise(0)
            ).alias("delivered_count"),

            F.sum(
                F.col("payment_amount")
            ).alias("payment_amount")
        ).orderBy(
            F.desc("order_count")
        ).show(50, truncate=False)

        # 4. 查看缺少商品明细的典型订单
        print("===== MISSING ITEMS: SAMPLE ORDERS =====")

        missing_items.select(
            "order_id",
            "order_status",
            "purchase_ts",
            "approved_ts",
            "carrier_ts",
            "delivered_ts",
            "payment_amount"
        ).orderBy(
            "order_status",
            "purchase_ts"
        ).show(30, truncate=False)

        # 5. 计算订单各阶段时间间隔
        print("===== LIFECYCLE TIME ANALYSIS =====")

        timeline = (
            df
            .filter(
                F.col("approved_ts").isNotNull()
                & F.col("carrier_ts").isNotNull()
            )
            .withColumn(
                "approval_to_carrier_hours",
                (
                    F.unix_timestamp("carrier_ts")
                    - F.unix_timestamp("approved_ts")
                ) / 3600
            )
        )

        timeline.groupBy(
            "order_status"
        ).agg(
            F.count("*").alias("order_count"),
            F.round(
                F.avg("approval_to_carrier_hours"), 2
            ).alias("avg_hours"),
            F.round(
                F.expr(
                    "percentile_approx("
                    "approval_to_carrier_hours, 0.5)"
                ), 2
            ).alias("median_hours")
        ).show(50, truncate=False)

        # 6. 检查已交付状态但缺少商品明细的订单
        print("===== DELIVERED WITHOUT ITEMS =====")

        df.filter(
            (F.col("order_status") == "delivered")
            & (~F.col("has_items"))
        ).select(
            "order_id",
            "purchase_ts",
            "approved_ts",
            "carrier_ts",
            "delivered_ts",
            "payment_amount"
        ).show(50, truncate=False)

        print("M9 ORDER LIFECYCLE AUDIT: COMPLETE")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()