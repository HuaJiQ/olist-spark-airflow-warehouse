
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. 创建Spark应用
spark = (
    SparkSession.builder
    .appName("Olist-Day03-Profiling")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 2. 数据目录
base = "/workspace/data/raw/olist"

# 3. 定义CSV读取函数
def read_csv(filename):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(f"{base}/{filename}")
    )

# 4. 读取三张表
orders = read_csv("olist_orders_dataset.csv")
items = read_csv("olist_order_items_dataset.csv")
payments = read_csv("olist_order_payments_dataset.csv")

# 5. 查看字段结构和行数
for name, df in [
    ("orders", orders),
    ("items", items),
    ("payments", payments)
]:
    print(f"\n===== {name} =====", flush=True)
    df.printSchema()
    print("Rows:", df.count(), flush=True)

# 6. 检查订单主表的order_id唯一性
order_rows = orders.count()
unique_orders = (
    orders.select("order_id")
    .distinct()
    .count()
)

print("\n===== Order ID Check =====", flush=True)
print("Rows:", order_rows)
print("Unique order_id:", unique_orders)
print("Duplicate difference:", order_rows - unique_orders)

# 7. 统计每笔订单的商品明细数
item_counts = (
    items
    .groupBy("order_id")
    .agg(F.count("*").alias("item_count"))
)

# 8. 统计每笔订单的支付记录数
payment_counts = (
    payments
    .groupBy("order_id")
    .agg(F.count("*").alias("payment_count"))
)

# 9. 找到具有多条明细的订单
print("\n===== Multi-item Orders =====")
item_counts.filter(
    F.col("item_count") > 1
).orderBy(
    F.desc("item_count")
).show(10, truncate=False)

# 10. 找到具有多条支付记录的订单
print("\n===== Multi-payment Orders =====")
payment_counts.filter(
    F.col("payment_count") > 1
).orderBy(
    F.desc("payment_count")
).show(10, truncate=False)

# 11. 汇总多明细、多支付订单数量
multi_item_orders = item_counts.filter(
    F.col("item_count") > 1
).count()

multi_payment_orders = payment_counts.filter(
    F.col("payment_count") > 1
).count()

print("Multi-item order count:", multi_item_orders)
print("Multi-payment order count:", multi_payment_orders)

# 12. 关闭Spark
spark.stop()
