
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "/workspace/data/m9"

spark = (
    SparkSession.builder
    .appName("M9-Unavailable-Outlier-Check")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    orders = spark.read.parquet(
        f"{BASE}/dwd/fact_order"
    )

    summary = spark.read.parquet(
        f"{BASE}/dws/order_financial_summary"
    )

    unavailable = (
        orders.join(
            summary.select(
                "order_id",
                "has_items",
                "has_payment",
                "item_amount",
                "payment_amount"
            ),
            "order_id",
            "left"
        )
        .filter(F.col("order_status") == "unavailable")
        .withColumn(
            "approval_delay_hours",
            (
                F.unix_timestamp("approved_ts")
                - F.unix_timestamp("purchase_ts")
            ) / 3600
        )
    )

    print("===== UNAVAILABLE: HAS ITEMS =====")

    unavailable.groupBy("has_items").count().show()

    print("===== TOP 10 APPROVAL DELAYS =====")

    unavailable.orderBy(
        F.desc("approval_delay_hours")
    ).select(
        "order_id",
        "has_items",
        "purchase_ts",
        "approved_ts",
        "approval_delay_hours",
        "payment_amount"
    ).show(10, truncate=False)

finally:
    spark.stop()