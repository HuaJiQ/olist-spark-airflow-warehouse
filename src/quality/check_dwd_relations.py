
from pyspark.sql import SparkSession


def main():
    # 1. 创建SparkSession
    spark = (
        SparkSession.builder
        .appName("Olist-DWD-Relations-Check")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # 2. 读取五张DWD表
    orders = spark.read.parquet(
        "/workspace/data/dwd/order"
    )

    items = spark.read.parquet(
        "/workspace/data/dwd/order_item"
    )

    payments = spark.read.parquet(
        "/workspace/data/dwd/payment"
    )

    customers = spark.read.parquet(
        "/workspace/data/dwd/dim_customer"
    )

    products = spark.read.parquet(
        "/workspace/data/dwd/dim_product"
    )

    print("DWD tables loaded successfully", flush=True)

    # 3. 检查订单明细能否匹配商品维度
    orphan_items = items.join(
        products.select("product_id"),
        on="product_id",
        how="left_anti"
    )

    # 4. 统计关联失败的记录数量
    orphan_item_count = orphan_items.count()

    print(
        "Order items without product dimension:",
        orphan_item_count,
        flush=True
    )

    # 5. 展示关联失败的记录
    orphan_items.select(
        "order_id",
        "product_id"
    ).show(5, truncate=False)

    
    # ===== 检查2：订单明细是否存在对应订单 =====
    orphan_item_orders = items.join(
        orders.select("order_id"),
        on="order_id",
        how="left_anti"
    )

    orphan_item_orders_count = orphan_item_orders.count()

    print(
        "Order items without order:",
        orphan_item_orders_count,
        flush=True
    )

    # ===== 检查3：支付记录是否存在对应订单 =====
    orphan_payments = payments.join(
        orders.select("order_id"),
        on="order_id",
        how="left_anti"
    )

    orphan_payments_count = orphan_payments.count()

    print(
        "Payments without order:",
        orphan_payments_count,
        flush=True
    )

    # ===== 检查4：订单是否存在对应客户 =====
    orphan_order_customers = orders.join(
        customers.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )

    orphan_order_customers_count = orphan_order_customers.count()

    print(
        "Orders without customer:",
        orphan_order_customers_count,
        flush=True
    )

    assert orphan_item_count == 0, ("Order item references missing product")
    assert orphan_item_orders_count == 0, ("Order item references missing order")
    assert orphan_payments_count == 0, ("Payment references missing order")
    assert orphan_order_customers_count == 0, ("Order references missing customer")

    print("DWD RELATION QUALITY CHECK: PASS", flush=True)

    # 6. 所有操作完成后再关闭Spark
    spark.stop()


# 7. Python程序入口
if __name__ == "__main__":
    main()


