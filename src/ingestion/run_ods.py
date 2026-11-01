"""M1: Ingest nine Olist CSVs as raw-string ODS parquet and produce a quality report.

Usage (inside official Spark docker image):
  spark-submit --master 'local[2]' /workspace/src/ingestion/run_ods.py
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from olist_manifest import TABLES

ROOT = Path(os.environ.get("WAREHOUSE_ROOT", "/workspace"))
RAW = ROOT / "data/raw/olist"
ODS = ROOT / "data/ods"
REPORTS = ROOT / "reports"


def validate_files():
    missing = [filename for filename, _ in TABLES.values() if not (RAW / filename).is_file()]
    if missing:
        raise FileNotFoundError("Missing Olist CSV files: " + ", ".join(missing))


def main():
    validate_files()  # no partial ODS writes if input files are missing
    spark = (SparkSession.builder.appName("Olist-M1-ODS-Ingestion")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    REPORTS.mkdir(parents=True, exist_ok=True)
    results = {}
    failures = []
    try:
        for name, (filename, required) in TABLES.items():
            
            print(f"\n[ODS] {name} <- {filename}", flush=True)
            df = (spark.read
                        .option("header", "true")
                        .option("inferSchema", "false")
                        .option("encoding", "UTF-8")
                        .option("quote", '"')
                        .option("escape", '"')
                        .option("multiLine", "true")
                        .option("mode", "FAILFAST")
                        .csv(str(RAW / filename))
    )
            missing_cols = sorted(set(required) - set(df.columns))
            if missing_cols:
                raise ValueError(f"{name}: missing required columns {missing_cols}")
            rows = df.count()
            null_keys = {}
            key = required[0]
            null_keys[key] = df.filter(F.col(key).isNull() | (F.trim(F.col(key)) == "")).count()
            results[name] = {"source_file": filename, "rows": rows,
                             "columns": df.columns, "null_or_blank_primary_id": null_keys}
            # This is a rebuildable, full-snapshot ODS. No historical incremental claim.
            
            # 1. 确定ODS输出路径
            ods_path = str(ODS / name)

            # 2. 将CSV读取后的数据写入Parquet
            df.write.mode("overwrite").parquet(ods_path)

            # 3. 重新读取刚才写入的Parquet
            ods_df = spark.read.parquet(ods_path)

            # 4. 统计ODS记录数
            ods_rows = ods_df.count()

            # 5. 验证CSV与ODS记录数是否一致
            assert ods_rows == rows, (
                f"{name}: source={rows}, ods={ods_rows}"
            )

            print(
                f"[CHECK] {name}: source={rows}, ods={ods_rows}, PASS",
                flush=True
            )

            
            print(f"[OK] {name}: {rows} rows", flush=True)

        # Specific business-key audit; don't use count(*) vs distinct to infer null handling.
        orders = spark.read.parquet(str(ODS / "orders"))
        total = orders.count()
        nonnull_distinct = orders.select("order_id").where(F.col("order_id").isNotNull()).distinct().count()
        bad = total - nonnull_distinct
        results["orders"]["duplicate_or_null_order_id_excess"] = bad
        if bad:
            failures.append(f"orders.order_id is not unique/non-null: {bad}")
        report = {"timestamp_utc": datetime.now(timezone.utc).isoformat(),
                  "mode": "full_snapshot_rebuild", "tables": results,
                  "failures": failures, "status": "FAIL" if failures else "PASS"}
        report_file = REPORTS / "m1_ods_quality.json"
        report_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nQuality report: {report_file}", flush=True)
        print(f"M1 STATUS: {report['status']}", flush=True)
        if failures:
            raise RuntimeError("Quality checks failed: " + "; ".join(failures))

    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
