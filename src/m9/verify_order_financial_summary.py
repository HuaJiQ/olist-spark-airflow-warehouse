from pyspark.sql import SparkSession

PATH = "/workspace/data/m9/dws/order_financial_summary"

spark = (
    SparkSession.builder
    .appName("M9-Verify-Financial-Summary")
    .getOrCreate()
)

try:
    df = spark.read.parquet(PATH)

    print("===== SAVED SCHEMA =====")
    df.printSchema()

    print("===== SAVED ROWS =====")
    count = df.count()
    print("Rows:", count)

    expected_columns = {
        "order_id",
        "item_count",
        "item_amount",
        "freight_amount",
        "payment_count",
        "payment_amount",
        "has_items",
        "has_payment",
    }

    missing = expected_columns - set(df.columns)

    print("Missing columns:", missing)

    if count != 99441:
        raise ValueError("Row count mismatch")

    if missing:
        raise ValueError("Required columns missing")

    print("M9 SAVED FINANCIAL SUMMARY: BASIC CHECK PASS")

finally:
    spark.stop()