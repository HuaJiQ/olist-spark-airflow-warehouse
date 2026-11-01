
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==============================
# 1. Spark实验环境
# ==============================
spark = (
    SparkSession.builder
    .appName("Ecommerce-Day02-Broadcast")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 初次运行采用60万条，控制本地资源消耗
N = 600_000

print("Spark Version:", spark.version, flush=True)
print("Total orders:", N, flush=True)

# ==============================
# 2. 生成订单事实数据
# ==============================
# 80%的订单集中在product_id=0
# 其余20%分配给product_id=1~999

orders = (
    spark.range(0, N, 1, 8)
    .select(
        F.col("id").alias("order_id"),
        F.when(
            (F.col("id") % 10) < 8,
            F.lit(0)
        ).otherwise(
            (F.col("id") % 999) + 1
        ).cast("long").alias("product_id")
    )
)

# ==============================
# 3. 生成商品维度数据
# ==============================
products = (
    spark.range(0, 1000, 1, 4)
    .select(
        F.col("id").alias("product_id"),
        (F.col("id") % 10)
        .cast("int").alias("category")
    )
)

# ==============================
# 4. 执行事实表与维度表Join
# ==============================
joined = orders.join(
    F.broadcast(products),#向Spark优化器提供广播该小表的Hint
    on="product_id",
    how="inner"
)

# 按商品类别汇总订单
result = (
    joined
    .groupBy("category")
    .agg(F.count("*").alias("order_count"))
)

print("===== Physical Plan =====", flush=True)
result.explain("formatted")

# ==============================
# 5. 执行SQL计算并计时
# ==============================
print("===== Execution Start =====", flush=True)
start = time.perf_counter()

rows = result.collect()

elapsed = time.perf_counter() - start

print("===== Query Result =====", flush=True)

for row in sorted(rows, key=lambda r: r["category"]):
    print(row, flush=True)

total = sum(r["order_count"] for r in rows)

print("Total joined orders:", total, flush=True)
print("Elapsed seconds:", round(elapsed, 3), flush=True)

assert total == N, "Join result count mismatch!"

# ==============================
# 6. 保留Spark UI供观察
# ==============================
print(
    "Spark UI: http://localhost:4040",
    flush=True
)
print(
    "Keeping application alive for 10 minutes...",
    flush=True
)

try:
    time.sleep(300)
finally:
    spark.stop()
