
from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-M6-Spark-Smoke")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        path = "/workspace/data/dws/order_summary"

        print(f"[INPUT] {path}", flush=True)

        orders = spark.read.parquet(path)

        rows = orders.count()

        print(f"[DWS ROWS] {rows}", flush=True)

        if rows != 99441:
            raise ValueError(
                f"Unexpected DWS rows: {rows}"
            )

        print(
            "[M6 SPARK SMOKE] PASS",
            flush=True
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
