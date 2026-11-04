
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


INPUT_PATH = "/workspace/data/ods/customers"
OUTPUT_PATH = "/workspace/data/m9/dwd/dim_customer"


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-M9-DWD-Dim-Customer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. 防止输入缺失和覆盖旧结果
        if not Path(INPUT_PATH).is_dir():
            raise FileNotFoundError(INPUT_PATH)

        if Path(OUTPUT_PATH).exists():
            raise FileExistsError(
                f"Output already exists: {OUTPUT_PATH}"
            )

        # 2. 读取ODS
        customers = spark.read.parquet(INPUT_PATH)

        print("===== ODS SCHEMA =====")
        customers.printSchema()

        # 3. 构建客户维度
        dim_customer = customers.select(
            F.trim("customer_id").alias("customer_id"),
            F.trim("customer_unique_id").alias("customer_unique_id"),
            F.trim("customer_zip_code_prefix").alias("customer_zip_code_prefix"),
            F.trim("customer_city").alias("customer_city"),
            F.trim("customer_state").alias("customer_state")
        )

        print("===== DIM CUSTOMER SCHEMA =====")
        dim_customer.printSchema()

        # 4. 行数守恒
        source_count = customers.count()
        target_count = dim_customer.count()

        if source_count != 99441 or target_count != source_count:
            raise ValueError("Customer row count mismatch")

        # 5. 主键和客户身份标识检查
        invalid_keys = dim_customer.filter(
            F.col("customer_id").isNull()
            | (F.col("customer_id") == "")
        ).count()

        missing_unique_ids = dim_customer.filter(
            F.col("customer_unique_id").isNull()
            | (F.col("customer_unique_id") == "")
        ).count()

        duplicate_keys = (
            dim_customer
            .groupBy("customer_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        unique_customer_count = (
            dim_customer
            .select("customer_unique_id")
            .distinct()
            .count()
        )

        print("Source rows:", source_count)
        print("Target rows:", target_count)
        print("Invalid customer IDs:", invalid_keys)
        print("Duplicate customer IDs:", duplicate_keys)
        print("Missing unique IDs:", missing_unique_ids)
        print("Distinct real customers:", unique_customer_count)

        if any([invalid_keys, duplicate_keys, missing_unique_ids]):
            raise ValueError("Customer dimension key check failed")

        if unique_customer_count != 96096:
            raise ValueError("Unexpected unique customer count")

        # 6. 展示样本
        print("===== CUSTOMER SAMPLE =====")
        dim_customer.show(5, truncate=False)

        # 7. 写入独立M9路径
        dim_customer.write.mode("errorifexists").parquet(
            OUTPUT_PATH
        )

        # 8. 读回验证
        saved = spark.read.parquet(OUTPUT_PATH)

        if saved.count() != target_count:
            raise ValueError("Read-back row count mismatch")

        if saved.schema != dim_customer.schema:
            raise ValueError("Read-back schema mismatch")

        print("M9 DIM CUSTOMER QUALITY CHECK: PASS")
        print("Saved to:", OUTPUT_PATH)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()