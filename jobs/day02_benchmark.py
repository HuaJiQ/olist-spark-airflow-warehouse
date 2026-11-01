
import time
import statistics

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ========= 1. 创建Spark环境 =========

spark = (
    SparkSession.builder
    .appName("Ecommerce-Day02-Benchmark")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ========= 2. 构造相同的实验数据 =========

N = 600_000

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

products = (
    spark.range(0, 1000, 1, 4)
    .select(
        F.col("id").alias("product_id"),
        (F.col("id") % 10)
        .cast("int").alias("category")
    )
)

# ========= 3. 定义两种Join方案 =========

def build_query(strategy):

    if strategy == "sort_merge":
        joined = orders.join(
            products.hint("merge"),
            on="product_id",
            how="inner"
        )

    elif strategy == "broadcast":
        joined = orders.join(
            F.broadcast(products),
            on="product_id",
            how="inner"
        )

    else:
        raise ValueError("Unknown strategy")

    result = (
        joined
        .groupBy("category")
        .agg(F.count("*").alias("order_count"))
    )

    return result


# ========= 4. 检查物理执行计划 =========

for name in ["sort_merge", "broadcast"]:
    print(f"\n===== Plan: {name} =====", flush=True)
    build_query(name).explain(mode="simple")


# ========= 5. 定义单次测试函数 =========

def run_once(strategy):

    query = build_query(strategy)

    start = time.perf_counter()

    rows = query.collect()

    elapsed = time.perf_counter() - start

    # 按category排序，使结果可以公平比较
    output = sorted(
        (int(r["category"]), int(r["order_count"]))
        for r in rows
    )

    # 校验Join没有丢失或重复统计订单
    assert sum(count for _, count in output) == N

    return elapsed, output


# ========= 6. 交替运行两种方案 =========

times = {
    "sort_merge": [],
    "broadcast": []
}

reference = None

# 第1轮预热，不计入最终中位数
# 第2~4轮正式计时
for round_id in range(4):

    print(f"\n===== Round {round_id} =====", flush=True)

    if round_id % 2 == 0:
        strategies = ["sort_merge", "broadcast"]
    else:
        strategies = ["broadcast", "sort_merge"]

    for name in strategies:

        elapsed, output = run_once(name)

        if reference is None:
            reference = output
        else:
            assert output == reference, "Result mismatch!"

        print(
            f"Round={round_id}, "
            f"Strategy={name}, "
            f"Elapsed={elapsed:.3f}s",
            flush=True
        )

        if round_id > 0:
            times[name].append(elapsed)


# ========= 7. 统计中位数 =========

print("\n===== Benchmark Summary =====", flush=True)

for name in ["sort_merge", "broadcast"]:

    values = times[name]
    median_time = statistics.median(values)

    print(
        f"{name}: "
        f"times={values}, "
        f"median={median_time:.3f}s",
        flush=True
    )

print("Result equality: PASS", flush=True)

# ========= 8. 保留Spark UI =========

print(
    "Spark UI: http://localhost:4040",
    flush=True
)

time.sleep(600)

spark.stop()
