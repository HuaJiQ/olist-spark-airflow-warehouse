import subprocess
import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BATCH_ROOT = PROJECT_ROOT / "data" / "batches"

WATCH_STATUSES = {"CREATED", "RUNNING"}
FINAL_STATUSES = {"FAILED", "PUBLISHED"}
# 本地实验产生的非批次目录
IGNORED_DIRECTORIES = {
    "m6_write_probe_001",
}

def get_airflow_runs(dag_id):
    """通过Docker读取指定DAG的运行记录，只读。"""

    command = [
        "docker", "exec", "olist-airflow",
        "airflow", "dags", "list-runs",
        dag_id, "-o", "json",
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=True,
    )

    runs = json.loads(result.stdout)

    if not isinstance(runs, list):
        raise ValueError("Airflow response is not a list")

    return {
        run["run_id"]: run["state"]
        for run in runs
        if (
            isinstance(run, dict)
            and run.get("dag_id") == dag_id
            and isinstance(run.get("run_id"), str)
        )
    }

def scan_batches(threshold_minutes):
    now = datetime.now(timezone.utc)
    threshold = timedelta(minutes=threshold_minutes)

    findings = []

    for batch_dir in sorted(BATCH_ROOT.iterdir()):
        if not batch_dir.is_dir():
            continue

        if batch_dir.name in IGNORED_DIRECTORIES:
            continue

        manifest_path = batch_dir / "manifest.json"

        if not manifest_path.is_file():
            findings.append({
                "batch_id": batch_dir.name,
                "status": "UNKNOWN",
                "age_minutes": None,
                "issues": ["MISSING_MANIFEST"],
            })
            continue

        try:
            manifest = json.loads(
                manifest_path.read_text(encoding="utf-8")
            )

            batch_id = manifest["batch_id"]
            status = manifest["status"]
            updated_at = datetime.fromisoformat(
                manifest["updated_at"].replace("Z", "+00:00")
            )

            if updated_at.tzinfo is None:
                raise ValueError("updated_at must include timezone")

            updated_at = updated_at.astimezone(timezone.utc)
            age = now - updated_at

            issues = []

            if batch_id != batch_dir.name:
                issues.append("BATCH_ID_MISMATCH")

            if status in WATCH_STATUSES and age > threshold:
                issues.append("STALE_CANDIDATE")

            if status not in WATCH_STATUSES | FINAL_STATUSES | {"VALIDATED"}:
                issues.append("UNKNOWN_STATUS")

            source = manifest.get("source")

            if source is not None:
                if (
                    not isinstance(source, dict)
                    or source.get("system") != "airflow"
                    or not isinstance(source.get("dag_id"), str)
                    or not source.get("dag_id")
                    or not isinstance(source.get("run_id"), str)
                    or not source.get("run_id")
                ):
                    issues.append("INVALID_SOURCE")
                    source = None

            tables = manifest.get("tables", {})

            if not isinstance(tables, dict):
                raise ValueError("tables must be an object")

            for table_name, info in tables.items():
                actual_path = info.get("path", "")

                expected_path = (
                    f"data/batches/{batch_dir.name}"
                    f"/ads/{table_name}"
                )

                if actual_path != expected_path:
                    issues.append(
                        f"PATH_MISMATCH:{table_name}"
                    )

                expected_dir = (
                    batch_dir / "ads" / table_name
                )

                if not expected_dir.is_dir():
                    issues.append(
                        f"BATCH_OUTPUT_MISSING:{table_name}"
                    )

            findings.append({
                "batch_id": batch_dir.name,
                "status": status,
                "age_minutes": round(
                    age.total_seconds() / 60, 1
                ),
                "source": source,
                "issues": issues,
            })

        except (ValueError, KeyError, TypeError, OSError) as exc:
            findings.append({
                "batch_id": batch_dir.name,
                "status": "UNREADABLE",
                "age_minutes": None,
                "issues": [f"MANIFEST_ERROR:{exc}"],
            })
        except json.JSONDecodeError as exc:
            findings.append({
                "batch_id": batch_dir.name,
                "status": "UNREADABLE",
                "age_minutes": None,
                "issues": [f"JSON_ERROR:{exc}"],
            })

    return findings


def main():
    parser = argparse.ArgumentParser(
        description="Read-only batch health scanner"
    )

    parser.add_argument(
        "--threshold-minutes",
        type=int,
        default=60,
    )
    parser.add_argument(
        "--check-airflow",
        action="store_true",
        help="Also query Airflow DAG Run states",
    )

    args = parser.parse_args()

    if args.threshold_minutes <= 0:
        parser.error("threshold must be positive")

    if not BATCH_ROOT.is_dir():
        parser.error(f"Batch directory not found: {BATCH_ROOT}")

    findings = scan_batches(args.threshold_minutes)
    
    if args.check_airflow:
        airflow_cache = {}

        for item in findings:
            # 第一版只查询超时批次
            if "STALE_CANDIDATE" not in item["issues"]:
                continue

            source = item.get("source")

            if not source:
                item["airflow_state"] = "SOURCE_UNKNOWN"
                continue

            dag_id = source["dag_id"]
            run_id = source["run_id"]

            # 同一个DAG只查询一次，减少重复调用
            if dag_id not in airflow_cache:
                try:
                    airflow_cache[dag_id] = get_airflow_runs(
                        dag_id
                    )
                except (
                    subprocess.SubprocessError,
                    OSError,
                    ValueError,
                    KeyError,
                    TypeError,
                ) as exc:
                    airflow_cache[dag_id] = None
                    print(
                        f"[AIRFLOW QUERY ERROR] {dag_id}: {exc}",
                        flush=True,
                    )

            runs = airflow_cache[dag_id]

            if runs is None:
                item["airflow_state"] = "AIRFLOW_UNAVAILABLE"
            elif run_id not in runs:
                item["airflow_state"] = "DAG_RUN_NOT_FOUND"
            else:
                state = runs[run_id]

                state_map = {
                    "running": "DAG_RUN_ACTIVE",
                    "queued": "DAG_RUN_ACTIVE",
                    "success": "DAG_RUN_SUCCESS",
                    "failed": "DAG_RUN_FAILED",
                }

                item["airflow_state"] = state_map.get(
                    state,
                    f"DAG_RUN_STATE_UNKNOWN:{state}",
                )

    print("===== BATCH HEALTH SCAN =====")
    print(f"Threshold: {args.threshold_minutes} minutes")

    for item in findings:
        if not item["issues"]:
            continue

        is_historical = item["status"] in FINAL_STATUSES

        level = (
            "HISTORICAL_INFO"
            if is_historical
            else "ACTIVE_WARNING"
            )

        print(f"\n[{level}] {item['batch_id']}")
        print(f"  Status: {item['status']}")
        print(f"  Age(min): {item['age_minutes']}")

        source = item.get("source")

        if source:
            print(f"  DAG: {source['dag_id']}")
            print(f"  Run: {source['run_id']}")

        if "airflow_state" in item:
            print(f"  Airflow: {item['airflow_state']}")

        for issue in item["issues"]:
            print(f"  [WARN] {issue}")

    active_count = sum(
        bool(item["issues"])
        and item["status"] not in FINAL_STATUSES
        for item in findings
    )

    historical_count = sum(
        bool(item["issues"])
        and item["status"] in FINAL_STATUSES
        for item in findings
    )

    healthy_count = sum(
        not item["issues"]
        for item in findings
    )

    print(f"\nScanned batches: {len(findings)}")
    print(f"Active warnings: {active_count}")
    print(f"Historical info: {historical_count}")
    print(f"No warnings: {healthy_count}")
    print("READ-ONLY SCAN COMPLETE")

if __name__ == "__main__":
    main()