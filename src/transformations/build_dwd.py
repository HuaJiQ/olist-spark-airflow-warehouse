
from pyspark.sql import functions as F

# 新增：公共写入校验函数
def write_and_verify(spark, df, path, expected_rows):
    df.write.mode("overwrite").parquet(path)

    saved_df = spark.read.parquet(path)
    saved_rows = saved_df.count()

    assert saved_rows == expected_rows, (
        f"Expected {expected_rows}, got {saved_rows}"
    )
    assert saved_df.schema == df.schema, "Schema mismatch"

    print(f"[DWD CHECK] {path}: {saved_rows} rows, PASS")


def transform_order_items(items):

    result = (
        items
        .select(
            F.col("order_id"),
            F.col("order_item_id")
                .cast("int")
                .alias("order_item_id"),
            F.col("product_id"),
            F.col("seller_id"),
            F.to_timestamp(
                "shipping_limit_date"
            ).alias("shipping_limit_date"),
            F.col("price")
                .cast("decimal(18,2)")
                .alias("price"),

            F.col("freight_value")
                .cast("decimal(18,2)")
                .alias("freight_value")
        )
    )

    return result

def transform_payments(payments):
    return payments.select(
        F.col("order_id"),
        F.col("payment_sequential")
            .cast("int").alias("payment_sequential"),
        F.col("payment_type"),
        F.col("payment_installments")
            .cast("int").alias("payment_installments"),
        F.col("payment_value")
            .cast("decimal(18,2)").alias("payment_value")
    )


def transform_orders(orders):
    return orders.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("order_status"),

        F.to_timestamp(
            "order_purchase_timestamp"
        ).alias("order_purchase_timestamp"),

        F.to_timestamp(
            "order_approved_at"
        ).alias("order_approved_at"),

        F.to_timestamp(
            "order_delivered_carrier_date"
        ).alias("order_delivered_carrier_date"),

        F.to_timestamp(
            "order_delivered_customer_date"
        ).alias("order_delivered_customer_date"),

        F.to_timestamp(
            "order_estimated_delivery_date"
        ).alias("order_estimated_delivery_date")
    )

def transform_customers(customers):
    return customers.select(
        F.col("customer_id"),
        F.col("customer_unique_id"),
        F.col("customer_zip_code_prefix"),
        F.col("customer_city"),
        F.col("customer_state")
    )


def transform_products(products):
    return products.select(
        F.col("product_id"),
        F.col("product_category_name"),

        F.col("product_name_lenght")
            .cast("int")
            .alias("product_name_length"),

        F.col("product_description_lenght")
            .cast("int")
            .alias("product_description_length"),

        F.col("product_photos_qty")
            .cast("int")
            .alias("product_photos_qty"),

        F.col("product_weight_g")
            .cast("int")
            .alias("product_weight_g"),

        F.col("product_length_cm")
            .cast("int")
            .alias("product_length_cm"),

        F.col("product_height_cm")
            .cast("int")
            .alias("product_height_cm"),

        F.col("product_width_cm")
            .cast("int")
            .alias("product_width_cm")
    )


if __name__ == "__main__":

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("Olist-M2-DWD")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    items = spark.read.parquet(
        "/workspace/data/ods/order_items"
    )

    dwd_items = transform_order_items(items)

   # 1. 查看转换后的字段类型
    print("===== DWD Schema =====")
    dwd_items.printSchema()

    # 2. 检查商品价格是否非法
    invalid_price = dwd_items.filter(
        (F.col("price") < 0) |
        (F.col("price").isNull())
    )

    print("Invalid price rows:", invalid_price.count())

    # 3. 检查运费是否非法
    invalid_freight = dwd_items.filter(
        (F.col("freight_value") < 0) |
        (F.col("freight_value").isNull())
)

    print("Invalid freight rows:", invalid_freight.count())

    # 4. 检查复合主键是否重复
    total_rows = dwd_items.count()

    unique_rows = (
        dwd_items
        .select("order_id", "order_item_id")
        .distinct()
        .count()
)

    print("Total rows:", total_rows)
    print("Unique keys:", unique_rows)
    print("Duplicate difference:", total_rows - unique_rows)

    # 6. 检查复合主键是否为空
    invalid_keys = dwd_items.filter(
        F.col("order_id").isNull() |
        (F.trim(F.col("order_id")) == "") |
        F.col("order_item_id").isNull()
    ).count()

    print("Invalid key rows:", invalid_keys)

    # 7. 基础质量门禁：有问题就停止
    assert total_rows == 112650, "Unexpected row count"
    assert total_rows == unique_rows, "Duplicate composite keys"
    assert invalid_keys == 0, "Null or blank composite keys"
    assert invalid_price.count() == 0, "Invalid price"
    assert invalid_freight.count() == 0, "Invalid freight"

    print("DWD QUALITY CHECK: PASS")

    write_and_verify(
        spark,
        dwd_items,
        "/workspace/data/dwd/order_item",
        total_rows
    )

    print("DWD ORDER ITEM: PASS", flush=True)
    
    # 读取ODS支付数据
    payments = spark.read.parquet(
        "/workspace/data/ods/order_payments"
    )

    # 调用转换函数
    dwd_payments = transform_payments(payments)

    print("===== DWD Payment Schema =====")
    dwd_payments.printSchema()

    print("===== DWD Payment Sample =====")
    dwd_payments.show(5, truncate=False)
    
    total_payment_rows = dwd_payments.count()

    unique_payment_keys = (
        dwd_payments
        .select("order_id", "payment_sequential")
        .distinct()
        .count()
)

    print("Total payment rows:", total_payment_rows)
    print("Unique payment keys:", unique_payment_keys)
    print(
        "Duplicate payment keys:",
        total_payment_rows - unique_payment_keys
)

    invalid_payment_keys = dwd_payments.filter(
        F.col("order_id").isNull() |
        (F.trim(F.col("order_id")) == "") |
        F.col("payment_sequential").isNull()
    ).count()

    print("Invalid payment keys:", invalid_payment_keys)
    invalid_payment_amount = dwd_payments.filter(
        (F.col("payment_value").isNull()) |
        (F.col("payment_value") < 0)
    ).count()

    print("Invalid payment amount rows:", invalid_payment_amount)

    # 1. 支付表质量门禁
    assert total_payment_rows == 103886, "Unexpected payment row count"
    assert total_payment_rows == unique_payment_keys, "Duplicate payment keys"
    assert invalid_payment_keys == 0, "Invalid payment keys"
    assert invalid_payment_amount == 0, "Invalid payment amounts"

    print("DWD PAYMENT QUALITY CHECK: PASS", flush=True)

    payment_path = "/workspace/data/dwd/payment"
    # 2. 写入正式DWD目录
    write_and_verify(
        spark,
        dwd_payments,
        payment_path,
        total_payment_rows
    )
    print("DWD PAYMENT: PASS", flush=True)
    orders = spark.read.parquet(
        "/workspace/data/ods/orders"
)

    dwd_orders = transform_orders(orders)

    print("===== DWD Order Schema =====")
    dwd_orders.printSchema()

    total_order_rows = dwd_orders.count()

    unique_order_ids = (
        dwd_orders.select("order_id")
        .distinct()
        .count()
)

    invalid_order_ids = dwd_orders.filter(
        F.col("order_id").isNull() |
        (F.trim(F.col("order_id")) == "")
    ).count()

    print("Order rows:", total_order_rows)
    print("Unique order IDs:", unique_order_ids)
    print("Invalid order IDs:", invalid_order_ids)

    
    # 1. 检查下单时间是否为空
    invalid_purchase_time = dwd_orders.filter(
        F.col("order_purchase_timestamp").isNull()
    ).count()

    print("Invalid purchase time:", invalid_purchase_time)

    # 2. 统计各订单状态
    print("===== Order Status =====")

    dwd_orders.groupBy("order_status").count().orderBy(
        F.desc("count")
    ).show(truncate=False)

    # 3. 统计已送达但没有实际送达时间的订单
    delivered_without_date = dwd_orders.filter(
        (F.col("order_status") == "delivered") &
        (F.col("order_delivered_customer_date").isNull())
    ).count()

    print("Delivered without date:", delivered_without_date)

    
    # 4. 订单事实表质量门禁
    assert total_order_rows == 99441, "Unexpected order count"
    assert total_order_rows == unique_order_ids, "Duplicate order IDs"
    assert invalid_order_ids == 0, "Invalid order IDs"
    assert invalid_purchase_time == 0, "Invalid purchase time"

    print("DWD ORDER QUALITY CHECK: PASS", flush=True)

    # 5. 使用已经封装好的公共写入函数
    write_and_verify(
        spark,
        dwd_orders,
        "/workspace/data/dwd/order",
        total_order_rows
    )

    print("DWD ORDER: PASS", flush=True)

    customers = spark.read.parquet(
    "/workspace/data/ods/customers"
)
    dim_customers = transform_customers(customers)
    dim_customers.printSchema()

    customer_rows = dim_customers.count()

    unique_customer_ids = (
        dim_customers
        .select("customer_id")
        .distinct()
        .count()
)

    invalid_customer_ids = dim_customers.filter(
        (F.col("customer_id").isNull()) |
        (F.trim(F.col("customer_id")) == "")
    ).count()

    print("Customer rows:", customer_rows)
    print("Unique customer IDs:", unique_customer_ids)
    print("Invalid customer IDs:", invalid_customer_ids)

    
    # 检查真实客户标识是否为空
    invalid_unique_ids = dim_customers.filter(
        (F.col("customer_unique_id").isNull()) |
        (F.trim(F.col("customer_unique_id")) == "")
    ).count()

    # 统计不同的真实客户标识数量
    unique_real_customers = (
        dim_customers
        .select("customer_unique_id")
        .distinct()
        .count()
    )

    print("Invalid unique customer IDs:", invalid_unique_ids)
    print("Unique real customers:", unique_real_customers)

    
    assert customer_rows == 99441, "Unexpected customer count"
    assert customer_rows == unique_customer_ids, (
        "Duplicate customer_id"
    )
    assert invalid_customer_ids == 0, (
        "Invalid customer_id"
    )
    assert invalid_unique_ids == 0, (
        "Invalid customer_unique_id"
    )
    print("DIM CUSTOMER QUALITY CHECK: PASS", flush=True)
     # ④ 新增：调用已有函数，写入客户维度表
    write_and_verify(
        spark,
        dim_customers,
        "/workspace/data/dwd/dim_customer",
        customer_rows
    )
    print("DIM CUSTOMER: PASS", flush=True)

    
    # ===== 商品维度表 =====

    products = spark.read.parquet(
        "/workspace/data/ods/products"
)

    dim_products = transform_products(products)

    print("===== DIM Product Schema =====")
    dim_products.printSchema()

    # 1. 统计总记录数
    product_rows = dim_products.count()

    # 2. 统计唯一商品ID
    unique_product_ids = (
        dim_products
        .select("product_id")
        .distinct()
        .count()
)

    # 3. 检查商品ID为空或空字符串
    invalid_product_ids = dim_products.filter(
        (F.col("product_id").isNull()) |
        (F.trim(F.col("product_id")) == "")
    ).count()

    # 4. 检查商品重量和尺寸是否为负数
    numeric_columns = [
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]

    for column_name in numeric_columns:
        negative_rows = dim_products.filter(
            F.col(column_name) < 0
        ).count()

        null_rows = dim_products.filter(
            F.col(column_name).isNull()
        ).count()
        print(
            f"{column_name}: "
            f"negative={negative_rows}, "
            f"null={null_rows}"
    )
        assert negative_rows == 0, (
            f"Negative value found in {column_name}"
    )

    print("Product rows:", product_rows)
    print("Unique product IDs:", unique_product_ids)
    print("Invalid product IDs:", invalid_product_ids)

    # 5. 基础质量门禁
    assert product_rows == 32951, "Unexpected product count"
    assert product_rows == unique_product_ids, "Duplicate product IDs"
    assert invalid_product_ids == 0, "Invalid product IDs"

    print("DIM PRODUCT QUALITY CHECK: PASS", flush=True)

    # 6. 写入Parquet
    write_and_verify(
        spark,
        dim_products,
        "/workspace/data/dwd/dim_product",
        product_rows
    )
    print("DIM PRODUCT: PASS", flush=True)
    
    missing_product_attributes = dim_products.filter(
        F.col("product_weight_g").isNull() |
        F.col("product_length_cm").isNull() |
        F.col("product_height_cm").isNull() |
        F.col("product_width_cm").isNull()
)

    print(
        "Products with missing physical attributes:",
        missing_product_attributes.count()
)

    missing_product_attributes.select(
        "product_id",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ).show(10, truncate=False)

    # 5. 最后关闭Spark
    spark.stop()
