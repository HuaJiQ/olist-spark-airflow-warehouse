
import time
import statistics

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. 创建Spark应用
spark = (
    SparkSession.builder
    .appName("Ecommerce-Day02-Salting")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

N = 600_000
SALTS = 8

# 2. 生成订单事实表
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

# 3. 生成商品维度表
products = (
    spark.range(0, 1000, 1, 4)
    .select(
        F.col("id").alias("product_id"),
        (F.col("id") % 10)
        .cast("int").alias("category")
    )
)

# 4. 左侧订单加盐
orders_salted = orders.withColumn(
    "salt",
    F.when(
        F.col("product_id") == 0,
        F.pmod(F.col("order_id"), F.lit(SALTS))
    ).otherwise(F.lit(0))
)

# 5. 右侧复制热点商品记录
products_salted = (
    products
    .withColumn(
        "salt",
        F.explode(
            F.when(
                F.col("product_id") == 0,
                F.sequence(
                    F.lit(0),
                    F.lit(SALTS - 1)
                )
            ).otherwise(
                F.array(F.lit(0))
            )
        )
    )
)

# 6. 创建两种查询
def build_query(strategy):

    if strategy == "baseline":
        joined = orders.join(
            products.hint("merge"),
            on="product_id",
            how="inner"
        )

    elif strategy == "salting":
        joined = orders_salted.join(
            products_salted.hint("merge"),
            on=["product_id", "salt"],
            how="inner"
        )

    else:
        raise ValueError("Unknown strategy")

    return (
        joined
        .groupBy("category")
        .agg(F.count("*").alias("order_count"))
    )

# 7. 验证加盐数据
salted_product_count = products_salted.count()

print(
    "Salted products:",
    salted_product_count,
    flush=True
)

assert salted_product_count == 999 + SALTS

# 8. 检查物理计划
for strategy in ["baseline", "salting"]:
    print(
        f"\n===== Physical Plan: {strategy} =====",
        flush=True
    )
    build_query(strategy).explain(mode="simple")

# 9. 单次测试
def run_once(strategy):

    query = build_query(strategy)

    start = time.perf_counter()
    rows = query.collect()
    elapsed = time.perf_counter() - start

    output = sorted(
        (int(row["category"]), int(row["order_count"]))
        for row in rows
    )

    assert sum(count for _, count in output) == N

    return elapsed, output

# 10. 两种方案分别预热
reference = None

for strategy in ["baseline", "salting"]:
    elapsed, output = run_once(strategy)

    if reference is None:
        reference = output
    else:
        assert output == reference

    print(
        f"Warmup {strategy}: {elapsed:.3f}s",
        flush=True
    )

# 11. 正式交替测试
times = {
    "baseline": [],
    "salting": []
}

for round_id in range(4):

    if round_id % 2 == 0:
        order = ["baseline", "salting"]
    else:
        order = ["salting", "baseline"]

    for strategy in order:
        elapsed, output = run_once(strategy)

        assert output == reference

        times[strategy].append(elapsed)

        print(
            f"Round={round_id}, "
            f"Strategy={strategy}, "
            f"Elapsed={elapsed:.3f}s",
            flush=True
        )

# 12. 打印结果
print("\n===== Salting Benchmark =====", flush=True)

for strategy, values in times.items():
    median = statistics.median(values)

    print(
        f"{strategy}: "
        f"times={values}, "
        f"median={median:.3f}s",
        flush=True
    )

print("Result equality: PASS", flush=True)

# 13. 保留Spark UI
print(
    "Spark UI: http://localhost:4040",
    flush=True
)

try:
    time.sleep(300)
finally:
    spark.stop()
