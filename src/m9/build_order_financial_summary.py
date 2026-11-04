
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE = "/workspace/data/m9"
OUTPUT = f"{BASE}/dws/order_financial_summary"


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-M9-Order-Financial-Summary")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. 检查源表与输出目录
        paths = {
            "orders": f"{BASE}/dwd/fact_order",
            "items": f"{BASE}/dwd/fact_order_item",
            "payments": f"{BASE}/dwd/fact_payment",
        }

        for path in paths.values():
            if not Path(path).is_dir():
                raise FileNotFoundError(path)

        if Path(OUTPUT).exists():
            raise FileExistsError(
                f"Output already exists: {OUTPUT}"
            )

        # 2. 读取三张DWD事实表
        orders = spark.read.parquet(paths["orders"])
        items = spark.read.parquet(paths["items"])
        payments = spark.read.parquet(paths["payments"])

        # 3. 商品明细先聚合到订单粒度
        item_agg = (
            items.groupBy("order_id")
            .agg(
                F.count("*").alias("item_count"),
                F.sum("price").alias("item_amount"),
                F.sum("freight_value").alias("freight_amount"),
            )
        )

        # 4. 支付明细先聚合到订单粒度
        payment_agg = (
            payments.groupBy("order_id")
            .agg(
                F.count("*").alias("payment_count"),
                F.sum("payment_value").alias("payment_amount"),
            )
        )

        # 5. 以订单为主表，关联两个订单级结果
        summary = (
            orders
            .join(item_agg, on="order_id", how="left")
            .join(payment_agg, on="order_id", how="left")
            .withColumn(
                "has_items",
                F.col("item_count").isNotNull()
            )
            .withColumn(
                "has_payment",
                F.col("payment_count").isNotNull()
            )
        )

        # 6. 验证订单粒度没有膨胀
        order_count = orders.count()
        summary_count = summary.count()

        duplicate_orders = (
            summary.groupBy("order_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print("Source orders:", order_count)
        print("Summary orders:", summary_count)
        print("Duplicate orders:", duplicate_orders)

        if order_count != 99441:
            raise ValueError("Unexpected order count")

        if summary_count != order_count or duplicate_orders:
            raise ValueError("Order-grain check failed")

        # 7. 检查缺失明细和支付记录的订单
        missing_items = summary.filter(
            ~F.col("has_items")
        ).count()

        missing_payments = summary.filter(
            ~F.col("has_payment")
        ).count()

        print("Orders without items:", missing_items)
        print("Orders without payment:", missing_payments)

        if missing_items != 775 or missing_payments != 1:
            raise ValueError("Unexpected relation coverage")

        # 8. 金额守恒：汇总前后应一致
        checks = [
            (items, "price", "item_amount"),
            (items, "freight_value", "freight_amount"),
            (payments, "payment_value", "payment_amount"),
        ]

        for source_df, source_col, target_col in checks:
            source_total = source_df.agg(
                F.sum(source_col)
            ).first()[0]

            summary_total = summary.agg(
                F.sum(target_col)
            ).first()[0]

            print(
                f"[RECONCILE] {target_col}: "
                f"source={source_total}, "
                f"summary={summary_total}"
            )

            if source_total != summary_total:
                raise ValueError(
                    f"Amount mismatch: {target_col}"
                )

        # 9. 输出样本
        print("===== ORDER FINANCIAL SUMMARY =====")
        summary.select(
            "order_id",
            "item_count",
            "item_amount",
            "freight_amount",
            "payment_count",
            "payment_amount",
            "has_items",
            "has_payment"
        ).show(10, truncate=False)

        # 10. 写入并读回验证
        summary.write.mode("errorifexists").parquet(OUTPUT)
        saved = spark.read.parquet(OUTPUT)

        # 1. 读回行数检查
        saved_count = saved.count()

        if saved_count != summary_count:
            raise ValueError(
            f"Read-back row count mismatch: "
            f"{saved_count} != {summary_count}"
            )

        # 2. 打印写入前后的完整Schema
        print("===== BEFORE WRITE SCHEMA =====")
        summary.printSchema()

        print("===== AFTER READ SCHEMA =====")
        saved.printSchema()

        # 3. 比较字段名称与数据类型
        # 不直接比较nullable及metadata
        before_fields = [
            (field.name, field.dataType)
            for field in summary.schema.fields
            ]

        after_fields = [
            (field.name, field.dataType)
            for field in saved.schema.fields
            ]

        if before_fields != after_fields:
            print("Before:", before_fields)
            print("After:", after_fields)
            raise ValueError(
                "Read-back field name/type mismatch"
            )

        print("M9 FINANCIAL SUMMARY QUALITY CHECK: PASS")
        print("Saved to:", OUTPUT)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()