
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "/workspace/data/m9"

ORDER_PATH = f"{BASE}/dwd/fact_order"
SUMMARY_PATH = f"{BASE}/dws/order_financial_summary"
PAYMENT_PATH = f"{BASE}/dwd/fact_payment"


def main():
    spark = (
        SparkSession.builder
        .appName("M9-Suspicious-Orders-Audit")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        orders = spark.read.parquet(ORDER_PATH)
        summary = spark.read.parquet(SUMMARY_PATH)
        payments = spark.read.parquet(PAYMENT_PATH)

        # 1. 找出缺少商品明细的shipped订单
        print("===== SHIPPED WITHOUT ITEMS =====")

        shipped = (
            orders.join(
                summary.select(
                    "order_id",
                    "has_items",
                    "has_payment",
                    "payment_amount"
                ),
                "order_id"
            )
            .filter(
                (F.col("order_status") == "shipped")
                & (~F.col("has_items"))
            )
        )

        shipped.select(
            "order_id",
            "purchase_ts",
            "approved_ts",
            "carrier_ts",
            "payment_amount"
        ).show(20, truncate=False)

        # 查看这笔订单的原始支付明细
        shipped_payments = payments.join(
            shipped.select("order_id"),
            "order_id"
        )

        shipped_payments.select(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value"
        ).show(50, truncate=False)

        # 2. 找出有商品但没有支付记录的订单
        print("===== DELIVERED WITHOUT PAYMENT =====")

        missing_payment = (
            orders.join(
                summary.select(
                    "order_id",
                    "has_items",
                    "has_payment",
                    "item_amount",
                    "freight_amount"
                ),
                "order_id"
            )
            .filter(
                F.col("has_items")
                & (~F.col("has_payment"))
            )
        )

        missing_payment.select(
            "order_id",
            "order_status",
            "purchase_ts",
            "approved_ts",
            "carrier_ts",
            "delivered_ts",
            "item_amount",
            "freight_amount"
        ).show(20, truncate=False)

        # 3. 分析603笔unavailable订单的支付特征
        print("===== UNAVAILABLE PAYMENT ANALYSIS =====")

        unavailable = (
            orders.filter(
                F.col("order_status") == "unavailable"
            )
            .select(
                "order_id",
                "purchase_ts",
                "approved_ts"
            )
        )

        unavailable_payments = payments.join(
            unavailable,
            "order_id",
            "inner"
        )

        unavailable_payments.groupBy(
            "payment_type"
        ).agg(
            F.count("*").alias("payment_records"),
            F.countDistinct("order_id").alias(
                "order_count"
            ),
            F.sum("payment_value").alias(
                "payment_amount"
            )
        ).orderBy(
            F.desc("payment_amount")
        ).show(30, truncate=False)

        # 4. 分析下单至审批的时间间隔
        print("===== UNAVAILABLE APPROVAL DELAY =====")

        approval_delay = (
            unavailable
            .filter(F.col("approved_ts").isNotNull())
            .withColumn(
                "approval_delay_hours",
                (
                    F.unix_timestamp("approved_ts")
                    - F.unix_timestamp("purchase_ts")
                ) / 3600
            )
        )

        approval_delay.select(
            F.count("*").alias("approved_orders"),
            F.sum(
                F.when(
                    F.col("approval_delay_hours") < 0,
                    1
                ).otherwise(0)
            ).alias("negative_delay_count"),
            F.round(
                F.avg("approval_delay_hours"), 2
            ).alias("avg_delay_hours"),
            F.round(
                F.expr(
                    "percentile_approx("
                    "approval_delay_hours, 0.5)"
                ), 2
            ).alias("median_delay_hours"),
            F.round(
                F.expr(
                    "percentile_approx("
                    "approval_delay_hours, 0.9)"
                ), 2
            ).alias("p90_delay_hours"),
            F.round(
                F.max("approval_delay_hours"), 2
            ).alias("max_delay_hours")
        ).show(truncate=False)

        print("M9 SUSPICIOUS ORDER AUDIT: COMPLETE")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()