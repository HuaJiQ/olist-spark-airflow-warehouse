
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-ADS-Category-Sales")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # 1. 读取订单商品明细
    items = spark.read.parquet(
        "/workspace/data/dwd/order_item"
    )

    # 2. 读取商品维度
    products = spark.read.parquet(
        "/workspace/data/dwd/dim_product"
    )

    # 3. 读取订单事实表
    orders = spark.read.parquet(
        "/workspace/data/dwd/order"
    )

    # 4. 商品明细关联商品类别
    items_with_category = items.join(
        products.select(
            "product_id",
            "product_category_name"
        ),
        on="product_id",
        how="left"
    )

    # 5. 关联订单最终状态
    items_with_status = items_with_category.join(
        orders.select(
            "order_id",
            "order_status"
        ),
        on="order_id",
        how="left"
    )

    # 6. 检查Join有没有造成行数膨胀
    source_rows = items.count()
    joined_rows = items_with_status.count()

    print("Source item rows:", source_rows)
    print("Joined item rows:", joined_rows)

    assert source_rows == joined_rows, (
        "Category Join changed item row count"
    )

    print("CATEGORY JOIN CHECK: PASS", flush=True)

    missing_categories = items_with_status.filter(
        F.col("product_category_name").isNull() |
        (F.trim(F.col("product_category_name")) == "")
    ).count()

    print("Items with missing category:", missing_categories)

    missing_delivered_categories = items_with_status.filter(
        (F.col("order_status") == "delivered") &
        (
            F.col("product_category_name").isNull() |
            (F.trim(F.col("product_category_name")) == "")
        )
    ).count()

    print(
        "Delivered items with missing category:",
        missing_delivered_categories
    )

    
    # 只统计最终已送达的订单商品明细
    delivered_items = items_with_status.filter(
        F.col("order_status") == "delivered"
    )

    # 缺失商品类别统一标记
    delivered_items = delivered_items.withColumn(
        "category_name",
        F.when(
            F.col("product_category_name").isNull() |
            (F.trim(F.col("product_category_name")) == ""),
            F.lit("unknown")
        ).otherwise(F.col("product_category_name"))
    )

    # 按商品类别汇总
    category_sales = (
        delivered_items
        .groupBy("category_name")
        .agg(
            F.count("*").alias("item_count"),
            F.countDistinct("order_id").alias("order_count"),
            F.sum("price").alias("item_amount"),
            F.sum("freight_value").alias("freight_amount")
        )
    )

    print("===== Category Sales Top 10 =====")


    category_sales.orderBy(
        F.desc("item_amount")
    ).show(10, truncate=False)

    source_amount = delivered_items.agg(
        F.sum("price")
    ).first()[0]

    category_amount = category_sales.agg(
        F.sum("item_amount")
    ).first()[0]

    print("Delivered item amount:", source_amount)
    print("Category total amount:", category_amount)

    assert source_amount == category_amount, ("Category sales amount mismatch")

    print("CATEGORY AMOUNT RECONCILIATION: PASS", flush=True)

    
    # ===== 商品类别ADS质量检查 =====

    # 1. 类别汇总总行数
    category_rows = category_sales.count()

    # 2. 不同类别数量
    unique_categories = (
        category_sales
        .select("category_name")
        .distinct()
        .count()
    )

    # 3. 对账商品明细行数
    source_item_count = delivered_items.count()

    ads_item_count = category_sales.agg(
        F.sum("item_count")
    ).first()[0]

    # 4. 验证类别不能为空
    invalid_categories = category_sales.filter(
        F.col("category_name").isNull() |
        (F.trim(F.col("category_name")) == "")
    ).count()

    print("Category rows:", category_rows)
    print("Unique categories:", unique_categories)
    print("Delivered item rows:", source_item_count)
    print("ADS summed item rows:", ads_item_count)
    print("Invalid categories:", invalid_categories)

    assert category_rows == unique_categories, ("Duplicate category rows")
    assert source_item_count == ads_item_count, ("Category item count mismatch")
    assert invalid_categories == 0, ("Invalid ADS category")

    print("ADS CATEGORY QUALITY CHECK: PASS", flush=True)

    
    # ===== 保存商品类别ADS表 =====

    ads_path = "/workspace/data/ads/category_sales"

    category_sales.write.mode("overwrite").parquet(ads_path)

    saved_ads = spark.read.parquet(ads_path)
    saved_rows = saved_ads.count()

    # 检查写入前后行数
    assert saved_rows == category_rows, ("Category ADS row count mismatch")

    # 检查字段名与类型
    before_schema = [
        (field.name, field.dataType)
        for field in category_sales.schema.fields
    ]

    after_schema = [
        (field.name, field.dataType)
        for field in saved_ads.schema.fields
    ]

    assert before_schema == after_schema, ("Category ADS schema mismatch")

    print(f"[ADS CHECK] {ads_path}: {saved_rows} rows, PASS",flush=True)

    print("ADS CATEGORY SALES: PASS", flush=True)


    spark.stop()


if __name__ == "__main__":
    main()
