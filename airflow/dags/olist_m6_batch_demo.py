
import hashlib
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, task, get_current_context


BATCH_MANAGER = "/workspace/src/pipeline/batch_manager.py"
BATCH_ROOT = Path("/workspace/data/batches")


with DAG(
    dag_id="olist_m6_batch_demo",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["olist", "m6", "batch"],
):

    @task(retries=1, retry_delay=timedelta(seconds=10))
    def create_batch():
        context = get_current_context()
        run_id = context["run_id"]

        # 同一次DAG Run始终生成相同的batch_id
        digest = hashlib.sha256(
            run_id.encode("utf-8")
        ).hexdigest()[:16]

        batch_id = f"m6_demo_{digest}"

        manifest_path = (
            BATCH_ROOT / batch_id / "manifest.json"
        )

        if manifest_path.exists():
            print(
                f"[RETRY] Batch already exists: {batch_id}",
                flush=True
            )
        else:
            command = [
                "python",
                BATCH_MANAGER,
                "create",
                "--batch-id",
                batch_id,
            ]

            print(f"[COMMAND] {command}", flush=True)

            subprocess.run(
                command,
                check=True,
            )

        if not manifest_path.is_file():
            raise RuntimeError(
                f"Manifest was not created: {manifest_path}"
            )

        print(f"[BATCH CREATED] {batch_id}", flush=True)

        return batch_id

    @task
    def verify_manifest(batch_id: str):
        manifest_path = (
            BATCH_ROOT / batch_id / "manifest.json"
        )

        with manifest_path.open(
            "r",
            encoding="utf-8"
        ) as file:
            manifest = json.load(file)

        actual_id = manifest.get("batch_id")
        actual_status = manifest.get("status")

        if actual_id != batch_id:
            raise ValueError(
                f"Batch ID mismatch: {actual_id}"
            )

        if actual_status != "CREATED":
            raise ValueError(
                f"Unexpected batch status: {actual_status}"
            )

        print(
            f"[MANIFEST CHECK] {batch_id}: CREATED, PASS",
            flush=True
        )

        return batch_id

    @task
    def finish(batch_id: str):
        print(
            f"[M6 BATCH DEMO] {batch_id}: PASS",
            flush=True
        )

    created_id = create_batch()
    verified_id = verify_manifest(created_id)
    finish(verified_id)
