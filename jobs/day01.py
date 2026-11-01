
from pyspark.sql import SparkSession

# 1. 创建 SparkSession
spark = (
    SparkSession.builder
    .appName("Ecommerce-Day01")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Spark Version:", spark.version)

# 2. 模拟100万条订单
# id模拟订单ID，category模拟商品类别
orders = (
    spark.range(0, 1_000_000, 1, 8)
    .selectExpr(
        "id AS order_id",
        "CAST(id % 10 AS INT) AS category"
    )
)

print("Initial partitions:", orders.rdd.getNumPartitions())

# 3. 注册SQL临时视图
orders.createOrReplaceTempView("orders_demo")

# 4. 按商品类别统计订单数量
result = spark.sql("""
    SELECT
        category,
        COUNT(*) AS order_count
    FROM orders_demo
    GROUP BY category

""")

# 5. 查看物理执行计划
print("===== Physical Plan =====")
result.explain()

# 6. 执行计算
print("===== Query Result =====")
rows = result.collect()

for row in rows:
    print(row)

# 7. 保持程序运行，以便查看Spark UI
input("Press Enter to stop Spark...")

spark.stop()
