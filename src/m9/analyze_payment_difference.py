
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE = "/workspace/data/m9"

SUMMARY_PATH = f"{BASE}/dws/order_financial_summary"
PAYMENT_PATH = f"{BASE}/dwd/fact_payment"


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-M9-Payment-Difference-Analysis")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. 读取已经构建好的数据
        summary = spark.read.parquet(SUMMARY_PATH)
        payments = spark.read.parquet(PAYMENT_PATH)

        # 2. 统计缺失记录
        print("===== MISSING RECORD ANALYSIS =====")

        summary.groupBy(
            "has_items",
            "has_payment"
        ).count().orderBy(
            "has_items",
            "has_payment"
        ).show()

        # 3. 仅分析商品和支付记录都存在的订单
        valid = (
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
            .withColumn(
                "difference_type",
                F.when(
                    F.abs(F.col("difference"))
                    <= F.lit(0.01),
                    "MATCHED"
                )
                .when(
                    F.col("difference") > 0,
                    "PAYMENT_HIGHER"
                )
                .otherwise("PAYMENT_LOWER")
            )
        )

        print("===== DIFFERENCE CLASSIFICATION =====")

        valid.groupBy(
            "difference_type"
        ).agg(
            F.count("*").alias("order_count"),
            F.sum("difference").alias("total_difference")
        ).show()

        # 4. 分析订单状态
        print("===== DIFFERENCE BY ORDER STATUS =====")

        valid.groupBy(
            "order_status"
        ).agg(
            F.count("*").alias("order_count"),
            F.sum(
                F.when(
                    F.col("difference_type") != "MATCHED",
                    1
                ).otherwise(0)
            ).alias("different_order_count"),
            F.sum("expected_amount").alias("expected_amount"),
            F.sum("payment_amount").alias("payment_amount"),
            F.sum("difference").alias("total_difference")
        ).orderBy(
            F.desc("total_difference")
        ).show(50, truncate=False)

        # 5. 分析差异最大的订单
        print("===== TOP 20 ABSOLUTE DIFFERENCES =====")

        valid.orderBy(
            F.desc(F.abs(F.col("difference")))
        ).select(
            "order_id",
            "order_status",
            "item_amount",
            "freight_amount",
            "payment_amount",
            "difference"
        ).show(20, truncate=False)

        # 6. 分析支付方式
        print("===== PAYMENT TYPE ANALYSIS =====")

        payments.groupBy(
            "payment_type"
        ).agg(
            F.count("*").alias("payment_records"),
            F.countDistinct("order_id").alias("order_count"),
            F.sum("payment_value").alias("payment_amount")
        ).orderBy(
            F.desc("payment_amount")
        ).show(30, truncate=False)

        # 7. 分析同一订单使用多种支付方式的情况
        print("===== MULTI PAYMENT ORDERS =====")

        payment_types = (
            payments
            .groupBy("order_id")
            .agg(
                F.countDistinct("payment_type").alias(
                    "payment_type_count"
                )
            )
        )

        payment_types.groupBy(
            "payment_type_count"
        ).count().orderBy(
            "payment_type_count"
        ).show()

        print("M9 PAYMENT DIFFERENCE ANALYSIS: COMPLETE")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()