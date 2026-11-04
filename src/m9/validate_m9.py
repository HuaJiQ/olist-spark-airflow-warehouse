
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE = "/workspace/data/m9"

# 当前Olist数据集的验收基线
TABLES = {
    "fact_order": {
        "count": 99441,
        "keys": ["order_id"],
    },
    "fact_order_item": {
        "count": 112650,
        "keys": ["order_id", "order_item_id"],
    },
    "fact_payment": {
        "count": 103886,
        "keys": ["order_id", "payment_sequential"],
    },
    "dim_customer": {
        "count": 99441,
        "keys": ["customer_id"],
    },
    "dim_product": {
        "count": 32951,
        "keys": ["product_id"],
    },
    "dim_seller": {
        "count": 3095,
        "keys": ["seller_id"],
    },
}

failures = []


def check(name, condition):
    """统一记录P0检查结果。"""
    status = "PASS" if condition else "FAIL"
    print(f"[P0][{status}] {name}")

    if not condition:
        failures.append(name)


def money_total(df, column):
    """使用Decimal进行精确金额汇总。"""
    value = df.agg(
        F.sum(F.col(column)).alias("total")
    ).first()["total"]

    return value if value is not None else Decimal("0.00")


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-M9-Quality-Validation")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        frames = {}

        print("===== 1. DWD TABLE QUALITY =====")

        for name, config in TABLES.items():
            path = f"{BASE}/dwd/{name}"
            df = spark.read.parquet(path)
            frames[name] = df

            actual_count = df.count()

            invalid_key_condition = None

            for key in config["keys"]:
                condition = (
                    F.col(key).isNull()
                    | (
                        F.trim(
                            F.col(key).cast("string")
                        ) == ""
                    )
                )

                invalid_key_condition = (
                    condition
                    if invalid_key_condition is None
                    else invalid_key_condition | condition
                )

            invalid_keys = df.filter(
                invalid_key_condition
            ).count()

            duplicate_keys = (
                df.groupBy(*config["keys"])
                .count()
                .filter(F.col("count") > 1)
                .count()
            )

            check(
                f"{name}: row count",
                actual_count == config["count"]
            )

            check(
                f"{name}: valid keys",
                invalid_keys == 0
            )

            check(
                f"{name}: unique keys",
                duplicate_keys == 0
            )

        print("===== 2. FOREIGN KEY QUALITY =====")

        relations = [
            ("fact_order", "customer_id",
             "dim_customer", "customer_id"),
            ("fact_order_item", "order_id",
             "fact_order", "order_id"),
            ("fact_payment", "order_id",
             "fact_order", "order_id"),
            ("fact_order_item", "product_id",
             "dim_product", "product_id"),
            ("fact_order_item", "seller_id",
             "dim_seller", "seller_id"),
        ]

        for left, left_key, right, right_key in relations:
            child = frames[left].select(
                F.col(left_key).alias("key")
            )

            parent = frames[right].select(
                F.col(right_key).alias("key")
            )

            missing_fk = (
                child.distinct()
                .join(parent, on="key", how="left_anti")
                .count()
            )

            check(
                f"{left}.{left_key} -> {right}",
                missing_fk == 0
            )

        print("===== 3. DWS ORDER SUMMARY =====")

        summary = spark.read.parquet(
            f"{BASE}/dws/order_financial_summary"
        )

        summary_count = summary.count()

        duplicate_orders = (
            summary.groupBy("order_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        invalid_order_ids = summary.filter(
            F.col("order_id").isNull()
            | (F.trim("order_id") == "")
        ).count()

        check(
            "DWS summary row count",
            summary_count == 99441
        )

        check(
            "DWS summary unique order_id",
            duplicate_orders == 0
        )

        check(
            "DWS summary non-null order_id",
            invalid_order_ids == 0
        )

        # 核对DWS的订单集合与DWD一致
        orders = frames["fact_order"].select("order_id")
        summary_ids = summary.select("order_id")

        missing_in_summary = (
            orders.join(
                summary_ids,
                "order_id",
                "left_anti"
            ).count()
        )

        check(
            "All DWD orders represented in DWS",
            missing_in_summary == 0
        )

        print("===== 4. MONEY RECONCILIATION =====")

        reconciliation = [
            (
                frames["fact_order_item"],
                "price",
                "item_amount"
            ),
            (
                frames["fact_order_item"],
                "freight_value",
                "freight_amount"
            ),
            (
                frames["fact_payment"],
                "payment_value",
                "payment_amount"
            ),
        ]

        for source, source_col, target_col in reconciliation:
            before = money_total(source, source_col)
            after = money_total(summary, target_col)

            print(
                f"{target_col}: "
                f"source={before}, dws={after}"
            )

            check(
                f"Money reconciliation: {target_col}",
                before == after
            )

        print("===== 5. RECORD COVERAGE =====")

        coverage = (
            summary.groupBy("has_items", "has_payment")
            .count()
        )

        coverage.show(truncate=False)

        missing_items = summary.filter(
            ~F.col("has_items")
        ).count()

        missing_payments = summary.filter(
            ~F.col("has_payment")
        ).count()

        check(
            "Known baseline: orders without items",
            missing_items == 775
        )

        check(
            "Known baseline: orders without payment",
            missing_payments == 1
        )

        print("===== 6. QUALITY WARNINGS =====")

        delivered_no_payment = summary.filter(
            (F.col("order_status") == "delivered")
            & (~F.col("has_payment"))
        ).count()

        shipped_no_items = summary.filter(
            (F.col("order_status") == "shipped")
            & (~F.col("has_items"))
        ).count()

        product_missing_category = frames["dim_product"].filter(
            F.col("product_category_name").isNull()
            | (F.col("product_category_name") == "")
        ).count()

        product_untranslated = frames["dim_product"].filter(
            F.col("product_category_name").isNotNull()
            & (F.col("product_category_name") != "")
            & F.col("product_category_name_english").isNull()
        ).count()

        print(
            "[P1] Delivered without payment:",
            delivered_no_payment
        )
        print(
            "[P1] Shipped without items:",
            shipped_no_items
        )
        print(
            "[P2] Product category missing:",
            product_missing_category
        )
        print(
            "[P2] Product category untranslated:",
            product_untranslated
        )

        print("===== FINAL RESULT =====")

        if failures:
            print("M9 QUALITY CHECK: FAIL")
            for failure in failures:
                print("FAILED:", failure)
            raise ValueError(
                f"{len(failures)} P0 checks failed"
            )

        print("M9 QUALITY CHECK: PASS")
        print(
            "P0 rules passed; "
            "P1/P2 findings require separate review."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
