
import json
import urllib.error
import urllib.request
from datetime import datetime

from airflow.sdk import DAG, task


RUNNER_URL = "http://spark-runner:8765/run"


with DAG(
    dag_id="olist_m6_spark_smoke",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["olist", "m6", "spark"],
):

    @task(retries=0)
    def run_spark_smoke():
        payload = json.dumps({
            "job": "olist_dws_smoke"
        }).encode("utf-8")

        request = urllib.request.Request(
            RUNNER_URL,
            data=payload,
            headers={
                "Content-Type": "application/json"
            },
            method="POST",
        )

        print(
            "[AIRFLOW] Submitting Spark job",
            flush=True
        )

        try:
            with urllib.request.urlopen(
                request,
                timeout=240
            ) as response:
                result = json.load(response)

        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(
                "utf-8",
                errors="replace"
            )
            raise RuntimeError(
                f"Spark Runner HTTP {exc.code}: {detail}"
            ) from exc

        except urllib.error.URLError as exc:
            raise RuntimeError(
                f"Cannot connect to Spark Runner: {exc}"
            ) from exc

        print(
            "[SPARK STDOUT]\n"
            + result.get("stdout", ""),
            flush=True
        )

        if result.get("stderr"):
            print(
                "[SPARK STDERR]\n"
                + result["stderr"],
                flush=True
            )

        if (
            result.get("success") is not True
            or result.get("returncode") != 0
        ):
            raise RuntimeError(
                f"Spark job failed: {result}"
            )

        if "[M6 SPARK SMOKE] PASS" not in result.get(
            "stdout", ""
        ):
            raise RuntimeError(
                "Spark success marker not found"
            )

        print(
            "[AIRFLOW SPARK JOB] PASS",
            flush=True
        )

    run_spark_smoke()
