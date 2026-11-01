
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-ADS-Customer-Repurchase")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # 1. 读取DWS订单汇总
    orders = spark.read.parquet(
        "/workspace/data/dws/order_summary"
    )

    # 2. 读取客户维度
    customers = spark.read.parquet(
        "/workspace/data/dwd/dim_customer"
    )

    # 3. 订单关联客户维度
    orders_with_customer = orders.join(
        customers.select(
            "customer_id",
            "customer_unique_id"
        ),
        on="customer_id",
        how="left"
    )

    print("===== Orders With Customer =====")

    orders_with_customer.select(
        "order_id",
        "customer_id",
        "customer_unique_id",
        "order_status"
    ).show(5, truncate=False)

    source_rows = orders.count()
    joined_rows = orders_with_customer.count()

    missing_customer_ids = orders_with_customer.filter(
        F.col("customer_unique_id").isNull() |
        (F.trim(F.col("customer_unique_id")) == "")
    ).count()

    print("Source order rows:", source_rows)
    print("Joined order rows:", joined_rows)
    print("Missing real customer IDs:", missing_customer_ids)

    assert source_rows == joined_rows, ("Customer Join changed order count")
    assert missing_customer_ids == 0, ("Missing customer_unique_id after Join")

    print("CUSTOMER JOIN CHECK: PASS", flush=True)

    
    # 只保留最终已送达订单
    delivered_orders = orders_with_customer.filter(
        F.col("order_status") == "delivered"
    )

    # 按真实客户标识进行聚合
    customer_orders = (
        delivered_orders
        .groupBy("customer_unique_id")
        .agg(
            F.countDistinct("order_id").alias(
                "delivered_order_count"
            ),

            F.sum("item_amount").alias(
                "delivered_item_amount"
            )
        )
    )

    print("===== Customer Repurchase Sample =====")

    customer_orders.orderBy(
        F.desc("delivered_order_count")
    ).show(10, truncate=False)
    
    # ===== 客户复购指标 =====
    # 1. 至少有1笔已送达订单的客户总数
    total_customers = customer_orders.count()

    # 2. 只有1笔已送达订单的客户数
    single_purchase_customers = customer_orders.filter(
        F.col("delivered_order_count") == 1
    ).count()

    # 3. 至少有2笔已送达订单的客户数
    repeat_customers = customer_orders.filter(
        F.col("delivered_order_count") >= 2
    ).count()

    # 4. 计算复购率
    repurchase_rate = (
        repeat_customers / total_customers
        if total_customers > 0
        else None
    )

    print("===== Customer Repurchase Metrics =====")
    print("Total purchasing customers:", total_customers)
    print("Single-purchase customers:", single_purchase_customers)
    print("Repeat customers:", repeat_customers)
    print("Repurchase rate:", repurchase_rate)

    # 5. 检查客户分类是否完整
    assert (
        single_purchase_customers + repeat_customers
        == total_customers
    ), "Customer classification mismatch"

    print("CUSTOMER REPURCHASE CLASSIFICATION: PASS", flush=True)

    
    # ===== 客户订单数量守恒 =====

    # DWS中已送达订单总数
    delivered_order_rows = delivered_orders.count()

    # 客户聚合后的订单数量之和
    customer_aggregated_orders = customer_orders.agg(
        F.sum("delivered_order_count")
    ).first()[0]

    print("DWS delivered orders:", delivered_order_rows)
    print("Customer aggregated orders:", customer_aggregated_orders)

    assert (
        delivered_order_rows == customer_aggregated_orders
    ), "Customer order count mismatch"

    print("CUSTOMER ORDER RECONCILIATION: PASS", flush=True)

    dws_customer_amount = delivered_orders.agg(
        F.sum("item_amount")
    ).first()[0]

    ads_customer_amount = customer_orders.agg(
        F.sum("delivered_item_amount")
    ).first()[0]

    print("DWS delivered amount:", dws_customer_amount)
    print("Customer aggregated amount:", ads_customer_amount)

    assert dws_customer_amount == ads_customer_amount, (
        "Customer amount reconciliation mismatch"
    )

    print("CUSTOMER AMOUNT RECONCILIATION: PASS", flush=True)

    
    # ===== 客户粒度ADS表 =====

    customer_profile = customer_orders.withColumn(
        "is_repeat_customer",
        F.col("delivered_order_count") >= 2
    )

    print("===== Customer Purchase Profile =====")

    customer_profile.orderBy(
        F.desc("delivered_order_count")
    ).show(10, truncate=False)

    # 校验复购标记
    flagged_repeat_customers = customer_profile.filter(
        F.col("is_repeat_customer")
    ).count()

    assert flagged_repeat_customers == repeat_customers, (
        "Repeat customer flag mismatch"
    )

    print("CUSTOMER PROFILE QUALITY CHECK: PASS", flush=True)


    
    # ===== 整体复购指标表 =====

    repurchase_summary = spark.createDataFrame(
        [
            (
                total_customers,
                single_purchase_customers,
                repeat_customers,
                repurchase_rate
            )
        ],
        schema="""
            purchasing_customer_count long,
            single_purchase_customer_count long,
            repeat_customer_count long,
            repurchase_rate double
        """
    )

    print("===== ADS Customer Repurchase =====")
    repurchase_summary.show(truncate=False)

    assert repurchase_summary.count() == 1

    print("CUSTOMER REPURCHASE SUMMARY CHECK: PASS", flush=True)

    
    # ===== 写入客户购买画像 =====

    profile_path = (
        "/workspace/data/ads/customer_purchase_profile"
    )

    customer_profile.write.mode("overwrite").parquet(
        profile_path
    )

    saved_profile = spark.read.parquet(profile_path)

    assert saved_profile.count() == total_customers

    print(
        "ADS CUSTOMER PROFILE: PASS",
        flush=True
    )

    # ===== 写入整体复购指标 =====

    summary_path = ("/workspace/data/ads/customer_repurchase")

    repurchase_summary.write.mode("overwrite").parquet(summary_path)

    saved_summary = spark.read.parquet(summary_path)

    assert saved_summary.count() == 1

    print("ADS CUSTOMER REPURCHASE: PASS",flush=True)


    spark.stop()


if __name__ == "__main__":
    main()
