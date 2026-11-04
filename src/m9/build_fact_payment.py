from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


INPUT_PATH = "/workspace/data/ods/order_payments"
OUTPUT_PATH = "/workspace/data/m9/dwd/fact_payment"


def main():
    spark = (
        SparkSession.builder
        .appName("Olist-M9-DWD-Fact-Payment")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. 输入输出路径检查
        if not Path(INPUT_PATH).is_dir():
            raise FileNotFoundError(INPUT_PATH)

        if Path(OUTPUT_PATH).exists():
            raise FileExistsError(
                f"Output already exists: {OUTPUT_PATH}"
            )

        # 2. 读取ODS数据
        payments = spark.read.parquet(INPUT_PATH)

        print("===== ODS SCHEMA =====")
        payments.printSchema()

        # 3. 标准化字段类型
        fact_payment = payments.select(
            F.col("order_id"),
            F.expr(
                "try_cast(payment_sequential AS INT)"
            ).alias("payment_sequential"),
            F.col("payment_type"),
            F.expr(
                "try_cast(payment_installments AS INT)"
            ).alias("payment_installments"),
            F.expr(
                "try_cast(payment_value AS DECIMAL(18,2))"
            ).alias("payment_value")
        )

        print("===== DWD SCHEMA =====")
        fact_payment.printSchema()

        # 4. 行数校验
        source_count = payments.count()
        target_count = fact_payment.count()

        print("Source rows:", source_count)
        print("Target rows:", target_count)

        if source_count != 103886:
            raise ValueError("Unexpected source row count")

        if source_count != target_count:
            raise ValueError("Row count mismatch")

        # 5. 复合主键检查
        invalid_keys = fact_payment.filter(
            F.col("order_id").isNull()
            | (F.trim("order_id") == "")
            | F.col("payment_sequential").isNull()
        ).count()

        duplicate_keys = (
            fact_payment
            .groupBy("order_id", "payment_sequential")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print("Invalid keys:", invalid_keys)
        print("Duplicate keys:", duplicate_keys)

        if invalid_keys != 0 or duplicate_keys != 0:
            raise ValueError("Payment primary key check failed")

        # 6. 检查支付金额和分期期数转换
        # 把源列与转换结果放在同一张表中检查
        checked = payments.select(
            F.col("payment_value").alias("raw_value"),
            F.expr(
                "try_cast(payment_value AS DECIMAL(18,2))"
            ).alias("parsed_value"),
            F.col("payment_installments").alias("raw_installments"),
            F.expr(
                "try_cast(payment_installments AS INT)"
            ).alias("parsed_installments"),
        )

        bad_values = checked.filter(
            F.col("raw_value").isNotNull()
            & (F.trim("raw_value") != "")
            & F.col("parsed_value").isNull()
        ).count()

        bad_installments = checked.filter(
            F.col("raw_installments").isNotNull()
            & (F.trim("raw_installments") != "")
            & F.col("parsed_installments").isNull()
        ).count()

        # 额外检查：原始支付金额与分期期数是否缺失
        missing_values = fact_payment.filter(
            F.col("payment_value").isNull()
        ).count()

        missing_installments = fact_payment.filter(
            F.col("payment_installments").isNull()
        ).count()

        negative_values = fact_payment.filter(
            F.col("payment_value") < 0
        ).count()

        print("Payment conversion failures:", bad_values)
        print("Installment conversion failures:", bad_installments)
        print("Missing payment values:", missing_values)
        print("Missing installments:", missing_installments)
        print("Negative payment values:", negative_values)

        if any([
            bad_values,
            bad_installments,
            missing_values,
            missing_installments,
            negative_values
        ]):
            raise ValueError("Payment data quality check failed")

        # 7. 查看样本
        print("===== PAYMENT SAMPLE =====")
        fact_payment.show(5, truncate=False)

        # 8. 独立写入M9目录
        fact_payment.write.mode("errorifexists").parquet(
            OUTPUT_PATH
        )

        # 9. 读回验证
        saved = spark.read.parquet(OUTPUT_PATH)

        if saved.count() != target_count:
            raise ValueError("Read-back row count mismatch")

        if saved.schema != fact_payment.schema:
            raise ValueError("Read-back schema mismatch")

        print("M9 FACT PAYMENT QUALITY CHECK: PASS")
        print("Saved to:", OUTPUT_PATH)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()