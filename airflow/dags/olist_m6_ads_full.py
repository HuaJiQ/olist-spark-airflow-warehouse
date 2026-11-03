
import hashlib
import json
import subprocess
import urllib.error
import urllib.request

from datetime import datetime
from pathlib import Path

import pyarrow.dataset as ds
from airflow.sdk import DAG, task, get_current_context


BATCH_MANAGER = "/workspace/src/pipeline/batch_manager.py"
BATCH_ROOT = Path("/workspace/data/batches")
RUNNER_URL = "http://spark-runner:8765/run"

EXPECTED_ROWS = {
    "daily_sales": 634,
    "customer_purchase_profile": 93358,
    "customer_repurchase": 1,
    "category_sales": 74,
}


def run_manager(*args):
    command = ["python", BATCH_MANAGER, *args]
    subprocess.run(command, check=True)


def request_spark(job_name, batch_id):
    """调用Spark Runner并检查结果。"""
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
        f"[AIRFLOW] Submit {job_name}, batch={batch_id}",
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
            f"Spark Runner HTTP {exc.code}: {detail}"
        ) from exc

    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"Cannot reach Spark Runner: {exc}"
        ) from exc

    if (
        result.get("success") is not True
        or result.get("returncode") != 0
        or result.get("batch_id") != batch_id
        or result.get("job") != job_name
    ):
        raise RuntimeError(
            f"Spark job unsuccessful: {result}"
        )

    print(
        "[SPARK STDOUT]\n" + result.get("stdout", ""),
        flush=True,
    )

    print(f"[SPARK JOB PASS] {job_name}", flush=True)
    return batch_id


with DAG(
    dag_id="olist_m6_ads_full",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["olist", "m6", "ads", "full"],
):

    @task(retries=0)
    def prepare_batch():
        run_id = get_current_context()["run_id"]
        digest = hashlib.sha256(
            run_id.encode("utf-8")
        ).hexdigest()[:16]

        batch_id = f"m6_full_{digest}"
        manifest = BATCH_ROOT / batch_id / "manifest.json"

        if manifest.exists():
            raise RuntimeError(
                f"Batch already exists: {batch_id}. "
                "Inspect before attempting recovery."
            )

        run_manager(
            "create", "--batch-id", batch_id
        )
        run_manager(
            "status",
            "--batch-id", batch_id,
            "--to", "RUNNING",
        )

        print(f"[BATCH RUNNING] {batch_id}", flush=True)
        return batch_id

    @task(retries=0)
    def build_daily_sales(batch_id: str):
        return request_spark(
            "build_daily_sales", batch_id
        )

    @task(retries=0)
    def build_customer_ads(batch_id: str):
        return request_spark(
            "build_customer_ads", batch_id
        )

    @task(retries=0)
    def build_category_sales(batch_id: str):
        return request_spark(
            "build_category_sales", batch_id
        )

    @task(retries=0)
    def record_all_ads(batch_id: str):
        manifest_path = (
            BATCH_ROOT / batch_id / "manifest.json"
        )

        with manifest_path.open(
            encoding="utf-8"
        ) as f:
            manifest = json.load(f)

        if manifest["status"] != "RUNNING":
            raise RuntimeError(
                "Batch must be RUNNING before recording"
            )

        # 先检查全部四张表，再登记任何一张表。
        for table_name, expected in EXPECTED_ROWS.items():
            path = (
                BATCH_ROOT / batch_id
                / "ads" / table_name
            )

            if not path.is_dir():
                raise FileNotFoundError(
                    f"Missing ADS table: {path}"
                )

            actual = ds.dataset(
                str(path), format="parquet"
            ).count_rows()

            if actual != expected:
                raise RuntimeError(
                    f"{table_name}: expected {expected}, "
                    f"got {actual}"
                )

            print(
                f"[ADS CHECK] {table_name}: "
                f"{actual} rows, PASS",
                flush=True,
            )

        # 预检查通过后，使用M5接口登记。
        for table_name in EXPECTED_ROWS:
            path = (
                BATCH_ROOT / batch_id
                / "ads" / table_name
            )

            run_manager(
                "record",
                "--batch-id", batch_id,
                "--table", table_name,
                "--path", str(path),
            )

        with manifest_path.open(
            encoding="utf-8"
        ) as f:
            manifest = json.load(f)

        if set(manifest["tables"]) != set(EXPECTED_ROWS):
            raise RuntimeError(
                "Manifest table registration incomplete"
            )

        for table_name, expected in EXPECTED_ROWS.items():
            info = manifest["tables"][table_name]

            expected_path = (
                f"data/batches/{batch_id}/ads/{table_name}"
            )

            if (
                info["row_count"] != expected
                or info["quality_pass"] is not True
                or info["path"] != expected_path
            ):
                raise RuntimeError(
                    f"Manifest mismatch: {table_name}"
                )

        print(
            f"[M6 FOUR ADS] PASS batch_id={batch_id}",
            flush=True,
        )
        return batch_id

    batch = prepare_batch()
    daily = build_daily_sales(batch)
    customer = build_customer_ads(daily)
    category = build_category_sales(customer)
    record_all_ads(category)