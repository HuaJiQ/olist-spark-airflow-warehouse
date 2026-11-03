
import hashlib
import json
import subprocess
import urllib.error
import urllib.request

from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG, task, get_current_context


BATCH_MANAGER = "/workspace/src/pipeline/batch_manager.py"
BATCH_ROOT = Path("/workspace/data/batches")
RUNNER_URL = "http://spark-runner:8765/run"

EXPECTED_ROWS = 634


def run_manager(*args):
    """通过现有批次管理程序操作Manifest。"""
    command = [
        "python",
        BATCH_MANAGER,
        *args,
    ]
    subprocess.run(command, check=True)


with DAG(
    dag_id="olist_m6_ads_daily",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["olist", "m6", "ads"],
):

    @task(retries=0)
    def prepare_batch():
        run_id = get_current_context()["run_id"]

        digest = hashlib.sha256(
            run_id.encode("utf-8")
        ).hexdigest()[:16]

        batch_id = f"m6_ads_{digest}"

        manifest_path = (
            BATCH_ROOT / batch_id / "manifest.json"
        )

        # 不允许重用已有批次，避免意外覆盖。
        if manifest_path.exists():
            raise RuntimeError(
                f"Batch already exists: {batch_id}. "
                "Inspect the batch before retrying."
            )

        run_manager(
            "create",
            "--batch-id", batch_id,
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
        payload = json.dumps({
            "job": "build_daily_sales",
            "batch_id": batch_id,
        }).encode("utf-8")

        request = urllib.request.Request(
            RUNNER_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        print(
            f"[AIRFLOW] Building daily_sales for {batch_id}",
            flush=True,
        )

        try:
            with urllib.request.urlopen(
                request,
                timeout=660,
            ) as response:
                result = json.load(response)

        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(
                "utf-8",
                errors="replace",
            )
            raise RuntimeError(
                f"Runner HTTP {exc.code}: {detail}"
            ) from exc

        except urllib.error.URLError as exc:
            raise RuntimeError(
                f"Runner connection failed: {exc}"
            ) from exc

        if (
            result.get("success") is not True
            or result.get("returncode") != 0
            or result.get("batch_id") != batch_id
        ):
            raise RuntimeError(
                f"Spark job failed: {result}"
            )

        print(
            "[SPARK STDOUT]\n"
            + result.get("stdout", ""),
            flush=True,
        )

        print(
            "[AIRFLOW] Spark daily_sales build succeeded",
            flush=True,
        )

        return batch_id

    @task(retries=0)
    def record_daily_sales(batch_id: str):
        path = (
            BATCH_ROOT / batch_id / "ads" / "daily_sales"
        )

        run_manager(
            "record",
            "--batch-id", batch_id,
            "--table", "daily_sales",
            "--path", str(path),
        )

        manifest_path = (
            BATCH_ROOT / batch_id / "manifest.json"
        )

        with manifest_path.open(
            encoding="utf-8"
        ) as f:
            manifest = json.load(f)

        table_info = manifest["tables"]["daily_sales"]
        row_count = table_info["row_count"]

        if row_count != EXPECTED_ROWS:
            raise RuntimeError(
                f"Expected {EXPECTED_ROWS} rows, "
                f"got {row_count}"
            )

        if not table_info["quality_pass"]:
            raise RuntimeError(
                "daily_sales quality check failed"
            )

        expected_path = (
            f"data/batches/{batch_id}/ads/daily_sales"
        )

        if table_info["path"] != expected_path:
            raise RuntimeError(
                "Unexpected recorded table path"
            )

        print(
            f"[MANIFEST] daily_sales rows={row_count}",
            flush=True,
        )

        print(
            f"[M6 ADS DAILY] PASS batch_id={batch_id}",
            flush=True,
        )

    record_daily_sales(
        build_daily_sales(
            prepare_batch()
        )
    )