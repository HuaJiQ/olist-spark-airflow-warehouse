
import argparse
from pathlib import Path

import pyarrow.dataset as ds

from batch_manager import BATCH_ROOT, load_batch
from validate_batch import REQUIRED_TABLES


def check_publish_ready(batch_id):
    # 读取批次Manifest
    manifest = load_batch(batch_id)

    if manifest["status"] != "VALIDATED":
        raise ValueError(
            "Only VALIDATED batches can be published"
        )

    # 确定当前批次目录
    batch_dir = (BATCH_ROOT / batch_id).resolve()
    report_path = batch_dir / "quality_report.json"

    if not report_path.is_file():
        raise FileNotFoundError(
            "Quality report not found"
        )

    import json

    with report_path.open("r", encoding="utf-8") as f:
        report = json.load(f)

    if report.get("batch_id") != batch_id:
        raise ValueError("Quality report batch ID mismatch")

    if report.get("result") != "PASS":
        raise ValueError("Quality report is not PASS")

    for table_name in REQUIRED_TABLES:
        info = manifest["tables"].get(table_name)

        if info is None:
            raise ValueError(
                f"Missing table: {table_name}"
            )

        expected = (
            batch_dir / "ads" / table_name
        ).resolve()

        actual = Path(info["path"]).resolve()

        if actual != expected:
            raise ValueError(
                f"Invalid path: {table_name}"
            )

        actual_rows = ds.dataset(
            str(actual), format="parquet"
        ).count_rows()

        report_rows = report["table_rows"][table_name]

        if not (
            actual_rows == info["row_count"]
            == report_rows
        ):
            raise ValueError(
                f"Row count mismatch: {table_name}"
            )

        print(
            f"[READY] {table_name}: {actual_rows} rows",
            flush=True
        )

    print(
        f"BATCH {batch_id} READY FOR PUBLICATION",
        flush=True
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()

    check_publish_ready(args.batch_id)

