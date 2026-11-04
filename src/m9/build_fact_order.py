
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# 1. 输入和输出路径：Docker容器内部路径
INPUT_PATH = "/workspace/data/ods/orders"
OUTPUT_PATH = "/workspace/data/m9/dwd/fact_order"

TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"

# 原始时间字段 -> DWD标准化字段
TIME_COLUMNS = {
    "order_purchase_timestamp": "purchase_ts",
    "order_approved_at": "approved_ts",
    "order_delivered_carrier_date": "carrier_ts",
    "order_delivered_customer_date": "delivered_ts",
    "order_estimated_delivery_date": "estimated_delivery_ts",
}


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-M9-DWD-Fact-Order")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 2. 检查输入与输出路径
        if not Path(INPUT_PATH).is_dir():
            raise FileNotFoundError(INPUT_PATH)

        if Path(OUTPUT_PATH).exists():
            raise FileExistsError(
                f"Output already exists: {OUTPUT_PATH}"
            )

        # 3. 读取ODS订单数据
        orders = spark.read.parquet(INPUT_PATH)

        print("===== ODS SCHEMA =====")
        orders.printSchema()

        # 4. 转换时间字段、统一命名
        selected = [
            F.col("order_id"),
            F.col("customer_id"),
            F.col("order_status"),
        ]

        for old_name, new_name in TIME_COLUMNS.items():
            selected.append(
                F.try_to_timestamp(
                    F.col(old_name),
                    F.lit(TIME_FORMAT)
                ).alias(new_name)
            )

        fact_order = (
            orders.select(*selected)
            .withColumn(
                "purchase_date",
                F.to_date("purchase_ts")
            )
        )

        print("===== DWD SCHEMA =====")
        fact_order.printSchema()

        # 5. 数据质量检查：主键、行数
        source_count = orders.count()
        target_count = fact_order.count()

        null_keys = fact_order.filter(
            F.col("order_id").isNull()
            | (F.trim(F.col("order_id")) == "")
        ).count()

        distinct_keys = (
            fact_order.select("order_id")
            .distinct()
            .count()
        )

        print("Source rows:", source_count)
        print("Target rows:", target_count)
        print("Null order IDs:", null_keys)
        print("Distinct order IDs:", distinct_keys)

        assert source_count == 99441, (
            f"Unexpected source count: {source_count}"
        )
        assert source_count == target_count
        assert null_keys == 0
        assert distinct_keys == target_count

        # 6. 数据质量检查：时间转换
        for old_name, new_name in TIME_COLUMNS.items():
            original = orders.select(
                "order_id",
                F.col(old_name).alias("raw_time")
            )

            converted = fact_order.select(
                "order_id",
                F.col(new_name).alias("parsed_time")
            )

            bad_count = (
                original.join(
                    converted, on="order_id", how="inner"
                )
                .filter(
                    F.col("raw_time").isNotNull()
                    & (F.trim(F.col("raw_time")) != "")
                    & F.col("parsed_time").isNull()
                )
                .count()
            )

            print(f"Time parse failures [{old_name}]: {bad_count}")
            assert bad_count == 0, (
                f"Time conversion failed: {old_name}"
            )

        # 7. 输出少量样本检查
        print("===== SAMPLE =====")
        fact_order.select(
            "order_id",
            "order_status",
            "purchase_ts",
            "purchase_date"
        ).show(5, truncate=False)

        # 8. 保存为Parquet；禁止覆盖已有目录
        fact_order.write.mode("errorifexists").parquet(
            OUTPUT_PATH
        )

        # 9. 读回验证
        saved = spark.read.parquet(OUTPUT_PATH)

        assert saved.count() == target_count
        assert saved.schema == fact_order.schema

        print("M9 FACT ORDER QUALITY CHECK: PASS")
        print("Saved to:", OUTPUT_PATH)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
