
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


INPUT_PATH = "/workspace/data/ods/order_items"
OUTPUT_PATH = "/workspace/data/m9/dwd/fact_order_item"


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-M9-DWD-Fact-Order-Item")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. 检查输入、输出目录
        if not Path(INPUT_PATH).is_dir():
            raise FileNotFoundError(INPUT_PATH)

        if Path(OUTPUT_PATH).exists():
            raise FileExistsError(
                f"Output already exists: {OUTPUT_PATH}"
            )

        # 2. 读取ODS商品明细
        items = spark.read.parquet(INPUT_PATH)

        print("===== ODS SCHEMA =====")
        items.printSchema()

        # 3. 字段类型标准化
        fact_item = items.select(
            F.col("order_id"),
            F.expr("try_cast(order_item_id AS INT)")
                .alias("order_item_id"),
            F.col("product_id"),
            F.col("seller_id"),
            F.try_to_timestamp(
                F.col("shipping_limit_date"),
                F.lit("yyyy-MM-dd HH:mm:ss")
            ).alias("shipping_limit_ts"),
            F.expr("try_cast(price AS DECIMAL(18,2))")
                .alias("price"),
            F.expr("try_cast(freight_value AS DECIMAL(18,2))")
                .alias("freight_value"),
        )

        print("===== DWD SCHEMA =====")
        fact_item.printSchema()

        # 4. 检查行数
        source_count = items.count()
        target_count = fact_item.count()

        assert source_count == 112650
        assert target_count == source_count

        # 5. 检查复合主键空值
        invalid_keys = fact_item.filter(
            F.col("order_id").isNull()
            | (F.trim("order_id") == "")
            | F.col("order_item_id").isNull()
        ).count()

        # 6. 检查复合主键重复
        duplicate_keys = (
            fact_item
            .groupBy("order_id", "order_item_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print("Source rows:", source_count)
        print("Target rows:", target_count)
        print("Invalid keys:", invalid_keys)
        print("Duplicate keys:", duplicate_keys)

        assert invalid_keys == 0
        assert duplicate_keys == 0

        # 7. 检查金额及时间转换失败
        for raw_col, converted_col in [
            ("price", "price"),
            ("freight_value", "freight_value"),
            ("shipping_limit_date", "shipping_limit_ts")
        ]:
            raw = items.select(
                "order_id", "order_item_id",
                F.col(raw_col).alias("raw_value")
            )

            converted = fact_item.select(
                "order_id",
                F.col("order_item_id").cast("string")
                    .alias("order_item_id"),
                F.col(converted_col).alias("new_value")
            )

            failed = (
                raw.join(
                    converted,
                    on=["order_id", "order_item_id"],
                    how="inner"
                )
                .filter(
                    F.col("raw_value").isNotNull()
                    & (F.trim("raw_value") != "")
                    & F.col("new_value").isNull()
                )
                .count()
            )

            print(f"Conversion failures [{raw_col}]:", failed)
            assert failed == 0

        # 8. 检查金额非负
        negative_amounts = fact_item.filter(
            (F.col("price") < 0)
            | (F.col("freight_value") < 0)
        ).count()

        print("Negative amount rows:", negative_amounts)
        assert negative_amounts == 0

        # 9. 展示样本
        fact_item.show(5, truncate=False)

        # 10. 写入独立M9目录
        fact_item.write.mode("errorifexists").parquet(
            OUTPUT_PATH
        )

        # 11. 读回验证
        saved = spark.read.parquet(OUTPUT_PATH)

        assert saved.count() == target_count
        assert saved.schema == fact_item.schema

        print("M9 FACT ORDER ITEM QUALITY CHECK: PASS")
        print("Saved to:", OUTPUT_PATH)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()