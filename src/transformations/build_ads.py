
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

def main():
    # 1. 创建SparkSession
    spark = (
        SparkSession.builder
        .appName("Olist-M3-ADS-Daily-Sales")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # 2. 读取DWS订单汇总表
    dws = spark.read.parquet(
        "/workspace/data/dws/order_summary"
    )

    print("===== DWS Input =====")
    print("DWS rows:", dws.count())
    dws.printSchema()

    
    # ===== 每日销售指标 =====

    # 1. 提取下单日期
    orders_with_dt = dws.withColumn(
        "dt",
        F.to_date("order_purchase_timestamp")
    )

    # 2. 判断订单是否为已送达状态
    delivered = (
        F.col("order_status") == "delivered"
    )

    # 3. 按下单日期计算指标
    daily_sales = (
        orders_with_dt
        .groupBy("dt")
        .agg(
            # 当天下单总数
            F.count("*").alias("total_order_count"),

            # 最终已送达订单数
            F.sum(
                F.when(delivered, 1).otherwise(0)
            ).alias("delivered_order_count"),

            # 已送达订单商品金额
            F.sum(
                F.when(
                    delivered,
                    F.coalesce(
                        F.col("item_amount"),
                        F.lit(0)
                    )
                ).otherwise(0)
            ).alias("delivered_item_amount"),

            # 已送达订单运费
            F.sum(
                F.when(
                    delivered,
                    F.coalesce(
                        F.col("freight_amount"),
                        F.lit(0)
                    )
                ).otherwise(0)
            ).alias("delivered_freight_amount")
        )
    )

    # 4. 计算已送达订单平均商品金额
    daily_sales = daily_sales.withColumn(
        "delivered_aov",
        F.when(
            F.col("delivered_order_count") > 0,
            F.round(
                F.col("delivered_item_amount") /
                F.col("delivered_order_count"),
                2
            )
        ).otherwise(F.lit(None).cast("decimal(18,2)"))
    )

    print("===== ADS Daily Sales =====")

    daily_sales.orderBy("dt").show(
        15,
        truncate=False
    )
    
    # ===== ADS质量检查 =====
    # 1. DWS总订单数
    source_order_count = dws.count()

    # 2. ADS汇总订单数
    ads_order_count = daily_sales.agg(
        F.sum("total_order_count")
    ).first()[0]

    # 3. 每日日期唯一性
    daily_rows = daily_sales.count()

    unique_dates = (
        daily_sales
        .select("dt")
        .distinct()
        .count()
    )

    # 4. 检查无效日期
    invalid_dates = daily_sales.filter(
        F.col("dt").isNull()
    ).count()

    print("Source orders:", source_order_count)
    print("ADS summed orders:", ads_order_count)
    print("ADS daily rows:", daily_rows)
    print("Unique dates:", unique_dates)
    print("Invalid dates:", invalid_dates)

    assert source_order_count == ads_order_count, ("ADS order count mismatch")
    assert daily_rows == unique_dates, ("Duplicate ADS dates")
    assert invalid_dates == 0, ("Invalid ADS dates")

    print("ADS DAILY SALES QUALITY CHECK: PASS", flush=True)

    
    # ===== ADS已送达订单金额对账 =====

    # 1. 从DWS独立计算已送达订单指标
    delivered_orders = dws.filter(
        F.col("order_status") == "delivered"
    )

    dws_delivered_totals = delivered_orders.agg(
        F.count("*").alias("order_count"),
        F.sum("item_amount").alias("item_amount"),
        F.sum("freight_amount").alias("freight_amount")
    ).first()

    # 2. 从ADS每日指标表计算整体指标
    ads_delivered_totals = daily_sales.agg(
        F.sum("delivered_order_count").alias("order_count"),
        F.sum("delivered_item_amount").alias("item_amount"),
        F.sum("delivered_freight_amount").alias("freight_amount")
    ).first()

    # 3. 分别比较
    for metric in [
        "order_count",
        "item_amount",
        "freight_amount"
    ]:
        source_value = dws_delivered_totals[metric]
        target_value = ads_delivered_totals[metric]

        print(
            f"[RECONCILE] {metric}: "
            f"DWS={source_value}, ADS={target_value}"
        )

        assert source_value == target_value, (
            f"ADS reconciliation mismatch: {metric}"
        )

    print("ADS AMOUNT RECONCILIATION: PASS", flush=True)

        # 已送达但没有支付记录的订单数
    delivered_without_payment = dws.filter(
        (F.col("order_status") == "delivered") &
        (F.col("has_payment") == False)
    ).count()

    # 已送达但没有商品明细的订单数
    delivered_without_items = dws.filter(
        (F.col("order_status") == "delivered") &
        (F.col("has_items") == False)
    ).count()

    print("Delivered without payment:",delivered_without_payment)
    print("Delivered without items:",delivered_without_items)

    # ===== 写入ADS =====
    ads_path = "/workspace/data/ads/daily_sales"
    daily_sales.write.mode("overwrite").parquet(ads_path)

    # 读回验证
    saved_ads = spark.read.parquet(ads_path)
    saved_rows = saved_ads.count()

    assert saved_rows == daily_rows, ("ADS write/read row count mismatch")

    # 比较字段名与类型
    before_schema = [
        (field.name, field.dataType)
        for field in daily_sales.schema.fields
    ]

    after_schema = [
        (field.name, field.dataType)
        for field in saved_ads.schema.fields
    ]

    assert before_schema == after_schema, ("ADS schema mismatch")

    print(
        f"[ADS CHECK] {ads_path}: {saved_rows} rows, PASS",
        flush=True
    )

    print("ADS DAILY SALES: PASS", flush=True)


    spark.stop()


if __name__ == "__main__":
    main()