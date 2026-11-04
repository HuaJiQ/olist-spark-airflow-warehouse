
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


INPUT_PATH = "/workspace/data/ods/sellers"
OUTPUT_PATH = "/workspace/data/m9/dwd/dim_seller"
ITEM_PATH = "/workspace/data/m9/dwd/fact_order_item"


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-M9-DWD-Dim-Seller")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. 检查输入与输出路径
        for path in [INPUT_PATH, ITEM_PATH]:
            if not Path(path).is_dir():
                raise FileNotFoundError(path)

        if Path(OUTPUT_PATH).exists():
            raise FileExistsError(
                f"Output already exists: {OUTPUT_PATH}"
            )

        # 2. 读取ODS商家表
        sellers = spark.read.parquet(INPUT_PATH)

        print("===== ODS SCHEMA =====")
        sellers.printSchema()

        # 3. 构建商家维度
        dim_seller = sellers.select(
            F.trim("seller_id").alias("seller_id"),
            F.trim("seller_zip_code_prefix").alias(
                "seller_zip_code_prefix"
            ),
            F.trim("seller_city").alias("seller_city"),
            F.upper(F.trim("seller_state")).alias(
                "seller_state"
            )
        )

        print("===== DIM SELLER SCHEMA =====")
        dim_seller.printSchema()

        # 4. 行数、主键质量
        source_count = sellers.count()
        target_count = dim_seller.count()

        invalid_keys = dim_seller.filter(
            F.col("seller_id").isNull()
            | (F.col("seller_id") == "")
        ).count()

        duplicate_keys = (
            dim_seller
            .groupBy("seller_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print("Source rows:", source_count)
        print("Target rows:", target_count)
        print("Invalid seller IDs:", invalid_keys)
        print("Duplicate seller IDs:", duplicate_keys)

        if source_count != 3095 or target_count != source_count:
            raise ValueError("Seller row count mismatch")

        if invalid_keys or duplicate_keys:
            raise ValueError("Seller key quality check failed")

        # 5. 检查商家描述属性缺失
        for column in [
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        ]:
            missing = dim_seller.filter(
                F.col(column).isNull()
                | (F.col(column) == "")
            ).count()

            print(f"Missing [{column}]:", missing)

        # 6. 订单商品事实表的商家外键覆盖
        items = spark.read.parquet(ITEM_PATH)

        unmatched = (
            items
            .select("seller_id")
            .distinct()
            .join(
                dim_seller.select("seller_id"),
                on="seller_id",
                how="left_anti"
            )
            .count()
        )

        print("Unmatched seller IDs:", unmatched)

        if unmatched != 0:
            raise ValueError("Seller foreign key check failed")

        # 7. 展示样本
        print("===== SELLER SAMPLE =====")
        dim_seller.show(5, truncate=False)

        # 8. 写入Parquet并读回检查
        dim_seller.write.mode("errorifexists").parquet(
            OUTPUT_PATH
        )

        saved = spark.read.parquet(OUTPUT_PATH)

        if saved.count() != target_count:
            raise ValueError("Read-back row count mismatch")

        if saved.schema != dim_seller.schema:
            raise ValueError("Read-back schema mismatch")

        print("M9 DIM SELLER QUALITY CHECK: PASS")
        print("Saved to:", OUTPUT_PATH)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()