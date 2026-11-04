
import hashlib
import json
import urllib.error
import urllib.request

from pathlib import Path
from datetime import datetime

import pyarrow.dataset as ds
from airflow.sdk import DAG, task, get_current_context


RUNNER_URL = "http://spark-runner:8765/run"
BATCH_ROOT = Path("/workspace/data/batches")

COUNT_COLUMNS = [
    "order_count",
    "item_order_count",
    "payment_order_count",
    "delivered_order_count",
    "canceled_order_count",
    "orders_without_items",
    "orders_without_payment",
]

AMOUNT_COLUMNS = [
    "item_amount",
    "freight_amount",
    "payment_amount",
    "delivered_item_amount",
]


def request_spark(job_name, batch_id):
    """调用Spark Runner，只有真正成功才继续。"""
    payload = json.dumps({
        "job": job_name,
        "batch_id": batch_id,
    }).encode("utf-8")

    request = urllib.request.Request(
        RUNNER_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    print(
        f"[M10] Start {job_name}, batch={batch_id}",
        flush=True,
    )

    try:
        with urllib.request.urlopen(
            request, timeout=660
        ) as response:
            result = json.load(response)

    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(
            "utf-8", errors="replace"
        )
        raise RuntimeError(
            f"Runner HTTP {exc.code}: {detail}"
        ) from exc

    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"Runner unavailable: {exc}"
        ) from exc

    if (
        result.get("success") is not True
        or result.get("returncode") != 0
        or result.get("batch_id") != batch_id
        or result.get("job") != job_name
    ):
        raise RuntimeError(
            f"Spark job failed: {result}"
        )

    print(
        result.get("stdout", ""),
        flush=True,
    )

    print(f"[M10] {job_name}: PASS", flush=True)
    return batch_id


with DAG(
    dag_id="olist_m10_sales",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["olist", "m10", "sales"],
):

    @task(retries=0)
    def prepare_batch():
        context = get_current_context()
        run_id = context["run_id"]

        digest = hashlib.sha256(
            run_id.encode("utf-8")
        ).hexdigest()[:16]

        batch_id = f"m10_{digest}"

        batch_dir = BATCH_ROOT / batch_id

        # 同一批次不允许重新覆盖
        batch_dir.mkdir(
            parents=True,
            exist_ok=False,
        )

        print(
            f"[M10] New batch: {batch_id}",
            flush=True,
        )

        return batch_id

    @task(retries=0)
    def build_daily(batch_id: str):
        return request_spark(
            "build_m10_sales_daily", batch_id
        )

    @task(retries=0)
    def build_monthly(batch_id: str):
        return request_spark(
            "build_m10_sales_monthly", batch_id
        )

    @task(retries=0)
    def validate_sales(batch_id: str):
        return request_spark(
            "validate_m10_sales", batch_id
        )

    @task(retries=0)
    def verify_outputs(batch_id: str):
        base = BATCH_ROOT / batch_id / "m10"

        daily_path = base / "dws" / "sales_daily"
        monthly_path = base / "ads" / "sales_monthly"

        for path in [daily_path, monthly_path]:
            if not path.is_dir():
                raise FileNotFoundError(path)

        # 读取需要验收的列
        fields = COUNT_COLUMNS + AMOUNT_COLUMNS

        daily = ds.dataset(
            str(daily_path), format="parquet"
        ).to_table(
            columns=["dt"] + fields
        ).to_pylist()

        monthly = ds.dataset(
            str(monthly_path), format="parquet"
        ).to_table(
            columns=["month"] + fields
        ).to_pylist()

        print("Daily rows:", len(daily))
        print("Monthly rows:", len(monthly))

        # 当前Olist固定快照基线
        if len(daily) != 634:
            raise ValueError("Unexpected daily rows")

        if len(monthly) != 25:
            raise ValueError("Unexpected monthly rows")

        # 粒度检查：日期不能为空、不能重复
        daily_dates = [row["dt"] for row in daily]
        months = [row["month"] for row in monthly]

        if (
            any(value is None for value in daily_dates)
            or len(set(daily_dates)) != len(daily_dates)
        ):
            raise ValueError("Invalid daily grain")

        if (
            any(value is None for value in months)
            or len(set(months)) != len(months)
        ):
            raise ValueError("Invalid monthly grain")

        # 逐项检查DWS与ADS汇总一致
        for column in fields:
            daily_total = sum(
                row[column]
                for row in daily
                if row[column] is not None
            )

            monthly_total = sum(
                row[column]
                for row in monthly
                if row[column] is not None
            )

            if daily_total != monthly_total:
                raise ValueError(
                    f"Reconciliation failed: {column}: "
                    f"{daily_total} != {monthly_total}"
                )

            print(
                f"[RECONCILE] {column}: PASS",
                flush=True,
            )

        total_orders = sum(
            row["order_count"] for row in monthly
        )

        if total_orders != 99441:
            raise ValueError(
                f"Unexpected orders: {total_orders}"
            )

        print(
            f"M10 END-TO-END VALIDATION: PASS "
            f"batch={batch_id}",
            flush=True,
        )

        return batch_id

    batch = prepare_batch()
    daily = build_daily(batch)
    monthly = build_monthly(daily)
    validated = validate_sales(monthly)
    verify_outputs(validated)