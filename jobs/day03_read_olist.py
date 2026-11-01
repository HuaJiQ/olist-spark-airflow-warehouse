
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. 创建Spark程序
spark = (
    SparkSession.builder
    .appName("Olist-Day01-Data-Inspection")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 2. 数据路径（容器内部路径）
data_path = "/workspace/data/raw/olist"

# 3. 读取真实订单CSV
orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(f"{data_path}/olist_orders_dataset.csv")
)

# 4. 查看字段结构
print("===== Schema =====")
orders.printSchema()

# 5. 查看前5条数据
print("===== Sample Rows =====")
orders.show(5, truncate=False)

# 6. 统计总行数
total_rows = orders.count()
print("Total orders:", total_rows)

# 7. 检查订单ID是否重复
distinct_orders = orders.select("order_id").distinct().count()
print("Distinct order IDs:", distinct_orders)

# 8. 按订单状态统计
orders.groupBy("order_status") \
    .agg(F.count("*").alias("order_count")) \
    .orderBy(F.desc("order_count")) \
    .show()

# 9. 验收结果
assert total_rows == distinct_orders, "Duplicate order_id!"

spark.stop()

