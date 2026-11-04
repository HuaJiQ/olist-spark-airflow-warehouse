from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PRODUCT_PATH = "/workspace/data/ods/products"
CATEGORY_PATH = "/workspace/data/ods/category_translation"
OUTPUT_PATH = "/workspace/data/m9/dwd/dim_product"

NUMERIC_COLUMNS = {
    "product_name_lenght": "product_name_length",
    "product_description_lenght": "product_description_length",
    "product_photos_qty": "product_photos_qty",
    "product_weight_g": "product_weight_g",
    "product_length_cm": "product_length_cm",
    "product_height_cm": "product_height_cm",
    "product_width_cm": "product_width_cm",
}

def main():
    spark = (
        SparkSession.builder
        .appName("Olist-M9-DWD-Dim-Product")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. 检查文件路径
        for path in [PRODUCT_PATH, CATEGORY_PATH]:
            if not Path(path).is_dir():
                raise FileNotFoundError(path)

        if Path(OUTPUT_PATH).exists():
            raise FileExistsError(
                f"Output already exists: {OUTPUT_PATH}"
            )

        # 2. 读取ODS
        products = spark.read.parquet(PRODUCT_PATH)
        categories = spark.read.parquet(CATEGORY_PATH)

        print("===== PRODUCTS SCHEMA =====")
        products.printSchema()

        print("===== CATEGORY SCHEMA =====")
        categories.printSchema()

        # 3. 品类字段标准化
        product_base = products.select(
            F.trim("product_id").alias("product_id"),
            F.trim("product_category_name").alias(
                "product_category_name"
            ),
            *[
                F.expr(
                    f"try_cast({source} AS INT)"
                ).alias(target)
                for source, target in NUMERIC_COLUMNS.items()
            ]
        )

        category_dim = categories.select(
            F.trim("product_category_name").alias(
                "product_category_name"
            ),
            F.trim("product_category_name_english").alias(
                "product_category_name_english"
            )
        )

        # 4. 检查翻译表关联键
        invalid_category_keys = category_dim.filter(
            F.col("product_category_name").isNull()
            | (F.col("product_category_name") == "")
        ).count()

        duplicate_categories = (
            category_dim
            .groupBy("product_category_name")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print("Invalid category keys:", invalid_category_keys)
        print("Duplicate category keys:", duplicate_categories)

        if invalid_category_keys or duplicate_categories:
            raise ValueError("Invalid category mapping keys")

        # 5. LEFT JOIN保留所有商品
        dim_product = product_base.join(
            category_dim,
            on="product_category_name",
            how="left"
        )

        # 6. 主键和行数检查
        source_count = products.count()
        target_count = dim_product.count()

        invalid_product_keys = dim_product.filter(
            F.col("product_id").isNull()
            | (F.col("product_id") == "")
        ).count()

        duplicate_product_keys = (
            dim_product
            .groupBy("product_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print("Source rows:", source_count)
        print("Target rows:", target_count)
        print("Invalid product keys:", invalid_product_keys)
        print("Duplicate product keys:", duplicate_product_keys)

        if source_count != 32951 or target_count != source_count:
            raise ValueError("Product row count mismatch")

        if invalid_product_keys or duplicate_product_keys:
            raise ValueError("Product primary key check failed")

        # 7. 检查原始非空数值是否转换失败
        converted = products.select(
            *[
                F.col(source).alias(f"raw_{target}")
                for source, target in NUMERIC_COLUMNS.items()
            ],
            *[
                F.expr(f"try_cast({source} AS INT)").alias(
                    target
                )
                for source, target in NUMERIC_COLUMNS.items()
            ]
        )

        for target in NUMERIC_COLUMNS.values():
            failures = converted.filter(
                F.col(f"raw_{target}").isNotNull()
                & (F.trim(F.col(f"raw_{target}")) != "")
                & F.col(target).isNull()
            ).count()

            print(f"Conversion failures [{target}]:", failures)

            if failures:
                raise ValueError(
                    f"Numeric conversion failed: {target}"
                )

        # 8. 报告维度属性缺失，不盲目删除商品
        missing_categories = dim_product.filter(
            F.col("product_category_name").isNull()
            | (F.col("product_category_name") == "")
        ).count()

        untranslated = dim_product.filter(
            F.col("product_category_name").isNotNull()
            & (F.col("product_category_name") != "")
            & F.col("product_category_name_english").isNull()
        ).count()

        print("Missing product categories:", missing_categories)
        print("Untranslated product categories:", untranslated)

        for target in NUMERIC_COLUMNS.values():
            null_count = dim_product.filter(
                F.col(target).isNull()
            ).count()
            print(f"Null values [{target}]:", null_count)

        # 9. 保存与读回检查
        print("===== PRODUCT SAMPLE =====")
        dim_product.show(5, truncate=False)

        dim_product.write.mode("errorifexists").parquet(
            OUTPUT_PATH
        )

        saved = spark.read.parquet(OUTPUT_PATH)

        if saved.count() != target_count:
            raise ValueError("Read-back row count mismatch")

        if saved.schema != dim_product.schema:
            raise ValueError("Read-back schema mismatch")

        print("M9 DIM PRODUCT QUALITY CHECK: PASS")
        print("Saved to:", OUTPUT_PATH)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()