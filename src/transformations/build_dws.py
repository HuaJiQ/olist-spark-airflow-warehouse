
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-M3-DWS")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # 读取DWD层数据
    items = spark.read.parquet("/workspace/data/dwd/order_item")

    payments = spark.read.parquet("/workspace/data/dwd/payment")

    orders = spark.read.parquet("/workspace/data/dwd/order")

    # 1. 商品明细按订单聚合
    item_agg = items.groupBy("order_id").agg(
        F.count("*").alias("item_count"),
        F.sum("price").alias("item_amount"),
        F.sum("freight_value").alias("freight_amount")
    )

    # 2. 支付明细按订单聚合
    payment_agg = payments.groupBy("order_id").agg(
        F.count("*").alias("payment_count"),
        F.sum("payment_value").alias("payment_amount")
    )

    print("===== Item Aggregation =====")
    item_agg.show(5, truncate=False)

    print("===== Payment Aggregation =====")
    payment_agg.show(5, truncate=False)

    
    # 1. 将订单与商品聚合表关联
    order_with_items = orders.join(
        item_agg,
        on="order_id",
        how="left"
    )

    # 2. 再关联支付聚合表
    dws_order_summary = order_with_items.join(
        payment_agg,
        on="order_id",
        how="left"
    )
  
    # ===== 增加业务质量标记 =====

    dws_order_summary = (
        dws_order_summary
        .withColumn(
            "has_items",
            F.col("item_count").isNotNull()
        )
        .withColumn(
            "has_payment",
            F.col("payment_count").isNotNull()
        )
        .withColumn(
            "is_delivered_without_payment",
            (F.col("order_status") == "delivered") &
            (F.col("payment_count").isNull())
        )
    )


    # 3. 查看结果
    print("===== DWS Order Summary =====")
    dws_order_summary.select(
        "order_id",
        "order_status",
        "item_count",
        "item_amount",
        "freight_amount",
        "payment_count",
        "payment_amount"
    ).show(10, truncate=False)
   
    # 订单主表记录数
    order_rows = orders.count()

    # DWS汇总表记录数
    summary_rows = dws_order_summary.count()

    # DWS汇总表唯一订单ID
    unique_summary_orders = (
        dws_order_summary
        .select("order_id")
        .distinct()
        .count()
)

    print("Original order rows:", order_rows)
    print("DWS summary rows:", summary_rows)
    print("Unique DWS orders:", unique_summary_orders)

    # 质量门禁
    assert order_rows == summary_rows, ("Join caused row count change")
    assert summary_rows == unique_summary_orders, ("Duplicate orders in DWS")

    print("DWS ORDER GRAIN CHECK: PASS", flush=True)
    
    # ===== DWS金额守恒检查 =====

    # 1. 聚合前的明细总金额
    source_totals = items.agg(
        F.sum("price").alias("item_amount"),
        F.sum("freight_value").alias("freight_amount")
    ).first()

    source_payment_total = payments.agg(
        F.sum("payment_value").alias("payment_amount")
    ).first()

    # 2. 聚合后的DWS总金额
    dws_totals = dws_order_summary.agg(
        F.sum("item_amount").alias("item_amount"),
        F.sum("freight_amount").alias("freight_amount"),
        F.sum("payment_amount").alias("payment_amount")
    ).first()

    # 3. 输出金额对比
    print("===== Amount Reconciliation =====")

    print(
        "Item amount:",
        source_totals["item_amount"],
        dws_totals["item_amount"]
    )

    print(
        "Freight amount:",
        source_totals["freight_amount"],
        dws_totals["freight_amount"]
    )

    print(
        "Payment amount:",
        source_payment_total["payment_amount"],
        dws_totals["payment_amount"]
    )

    # 4. 验证金额守恒
    assert (
        source_totals["item_amount"] == dws_totals["item_amount"]
    ), "Item amount mismatch"

    assert (
        source_totals["freight_amount"] == dws_totals["freight_amount"]
    ), "Freight amount mismatch"

    assert (
        source_payment_total["payment_amount"] == dws_totals["payment_amount"]
    ), "Payment amount mismatch"

    print("DWS AMOUNT RECONCILIATION: PASS", flush=True)

    # ===== 检查DWS中的缺失明细 =====
    orders_without_items = dws_order_summary.filter(
        F.col("item_count").isNull()
    ).count()

    orders_without_payments = dws_order_summary.filter(
        F.col("payment_count").isNull()
    ).count()

    print("Orders without items:", orders_without_items)
    print("Orders without payments:", orders_without_payments)

    
    # ===== 写入DWS订单汇总表 =====
    dws_path = "/workspace/data/dws/order_summary"
    dws_order_summary.write.mode("overwrite").parquet(dws_path)
    saved_dws = spark.read.parquet(dws_path)
    saved_rows = saved_dws.count()

    print("===== Schema Before Write =====")
    dws_order_summary.printSchema()

    print("===== Schema After Read =====")
    saved_dws.printSchema()

    assert saved_rows == summary_rows, ("DWS write/read row count mismatch")
    
    # 检查字段名称和数据类型是否一致
    schema_before = [
        (field.name, field.dataType)
        for field in dws_order_summary.schema.fields
    ]

    schema_after = [
        (field.name, field.dataType)
        for field in saved_dws.schema.fields
    ]

    print("Schema before:", schema_before)
    print("Schema after:", schema_after)

    assert schema_before == schema_after, (
        "DWS column names or data types mismatch"
    )

    print("DWS SCHEMA CHECK: PASS", flush=True)


    print(
        f"[DWS CHECK] {dws_path}: {saved_rows} rows, PASS",
        flush=True
    )
    print("DWS ORDER SUMMARY: PASS", flush=True)

    # ===== DWS缺失关联的业务状态分析 =====
    print("===== Orders Without Items by Status =====")

    dws_order_summary.filter(
        F.col("item_count").isNull()
    ).groupBy(
        "order_status"
    ).count().orderBy(
        F.desc("count")
    ).show(truncate=False)

    print("===== Orders Without Payments =====")

    dws_order_summary.filter(
        F.col("payment_count").isNull()
    ).select(
        "order_id",
        "order_status",
        "order_purchase_timestamp",
        "item_count",
        "item_amount",
        "freight_amount"
    ).show(truncate=False)
    
    print("===== DWS Quality Flags =====")

    dws_order_summary.groupBy(
        "has_items",
        "has_payment"
    ).count().show(truncate=False)

    delivered_without_payment_count = (
        dws_order_summary
        .filter(F.col("is_delivered_without_payment"))
        .count()
    )

    print("Delivered orders without payment:",delivered_without_payment_count)

    # 质量标记必须与原有缺失统计一致
    assert dws_order_summary.filter(
        F.col("has_items") == False
    ).count() == orders_without_items

    assert dws_order_summary.filter(
        F.col("has_payment") == False
    ).count() == orders_without_payments

    print("DWS QUALITY FLAGS CHECK: PASS", flush=True)


    spark.stop()


if __name__ == "__main__":
    main()
