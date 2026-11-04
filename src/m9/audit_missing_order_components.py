
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "/workspace/data/m9"

SUMMARY_PATH = f"{BASE}/dws/order_financial_summary"
PAYMENT_PATH = f"{BASE}/dwd/fact_payment"


def main():
    spark = (
        SparkSession.builder
        .appName("M9-Missing-Order-Audit")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        summary = spark.read.parquet(SUMMARY_PATH)
        payments = spark.read.parquet(PAYMENT_PATH)

        # 1. 找出缺少商品明细的订单
        missing_items = summary.filter(
            (~F.col("has_items"))
            & F.col("has_payment")
        )

        # 2. 找出缺少支付记录的订单
        missing_payments = summary.filter(
            F.col("has_items")
            & (~F.col("has_payment"))
        )

        print("===== MISSING ITEMS BY STATUS =====")

        missing_items.groupBy(
            "order_status"
        ).agg(
            F.count("*").alias("order_count"),
            F.sum("payment_amount").alias(
                "payment_amount"
            )
        ).orderBy(
            F.desc("payment_amount")
        ).show(30, truncate=False)

        print("===== MISSING ITEMS PAYMENT TOTAL =====")

        missing_items.agg(
            F.count("*").alias("orders"),
            F.sum("payment_amount").alias(
                "total_payment"
            )
        ).show()

        print("===== MISSING PAYMENTS DETAILS =====")

        missing_payments.select(
            "order_id",
            "order_status",
            "item_amount",
            "freight_amount",
            "payment_amount"
        ).show(20, truncate=False)

        # 3. 查看缺少商品明细订单的支付方式
        print("===== PAYMENT TYPES OF MISSING ITEM ORDERS =====")

        missing_item_payments = payments.join(
            missing_items.select("order_id"),
            on="order_id",
            how="inner"
        )

        missing_item_payments.groupBy(
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

        # 4. 正确统计支付记录数量和支付方式数量
        print("===== PAYMENT RECORD COUNT =====")

        payments.groupBy(
            "order_id"
        ).agg(
            F.count("*").alias("payment_record_count"),
            F.countDistinct("payment_type").alias(
                "payment_type_count"
            ),
            F.sum("payment_value").alias(
                "total_payment"
            )
        ).groupBy(
            "payment_record_count",
            "payment_type_count"
        ).count().orderBy(
            "payment_record_count",
            "payment_type_count"
        ).show(50, truncate=False)

        # 5. 查看金额差异最大的订单支付明细
        top_orders = (
            summary
            .filter(
                F.col("has_items")
                & F.col("has_payment")
            )
            .withColumn(
                "expected_amount",
                F.col("item_amount")
                + F.col("freight_amount")
            )
            .withColumn(
                "difference",
                F.col("payment_amount")
                - F.col("expected_amount")
            )
            .orderBy(
                F.desc(F.abs(F.col("difference")))
            )
            .limit(10)
            .select("order_id", "difference")
        )

        print("===== TOP DIFFERENCE PAYMENT DETAILS =====")

        payments.join(
            top_orders,
            on="order_id",
            how="inner"
        ).select(
            "order_id",
            "difference",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value"
        ).orderBy(
            "order_id",
            "payment_sequential"
        ).show(100, truncate=False)

        print("M9 MISSING ORDER AUDIT: COMPLETE")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()