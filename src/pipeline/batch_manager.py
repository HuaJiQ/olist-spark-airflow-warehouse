
import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path


# 找到项目根目录：D:\SparkEcommerce
ROOT = Path(__file__).resolve().parents[2]

# 所有批次的存储目录
BATCH_ROOT = ROOT / "data" / "batches"

# 定义允许的状态变化
TRANSITIONS = {
    "CREATED": {"RUNNING"},
    "RUNNING": {"FAILED", "VALIDATED"},
    "FAILED": {"RUNNING"},
    "VALIDATED": {"PUBLISHED"},
    "PUBLISHED": set(),
}


def now_utc():
    """返回带UTC时区的当前时间。"""
    return datetime.now(timezone.utc).isoformat()


def get_manifest_path(batch_id):
    """根据批次ID生成manifest文件路径。"""

    # 防止路径穿越及非法文件名
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", batch_id):
        raise ValueError("Invalid batch_id")

    return BATCH_ROOT / batch_id / "manifest.json"


def save_manifest(path, manifest):
    """
    写入临时文件，再替换正式文件。

    避免普通写文件过程中意外退出，
    留下只写了一半的JSON文件。
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = path.with_suffix(".json.tmp")

    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(
            manifest,
            f,
            ensure_ascii=False,
            indent=2
        )

    temp_path.replace(path)


def create_batch(batch_id):
    """创建新批次。"""

    path = get_manifest_path(batch_id)

    # 拒绝覆盖已存在的manifest
    if path.exists():
        raise FileExistsError(
            f"Batch already exists: {batch_id}"
        )

    manifest = {
        "batch_id": batch_id,
        "status": "CREATED",
        "created_at": now_utc(),
        "updated_at": now_utc(),
        "tables": {},
        "error": None,
    }

    save_manifest(path, manifest)
    print(f"[CREATED] {batch_id}")


def load_batch(batch_id):
    """读取已有批次信息。"""

    path = get_manifest_path(batch_id)

    if not path.exists():
        raise FileNotFoundError(
            f"Batch not found: {batch_id}"
        )

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def update_status(batch_id, new_status, error=None):
    """检查状态是否合法，并更新manifest。"""

    path = get_manifest_path(batch_id)
    manifest = load_batch(batch_id)

    old_status = manifest["status"]

    allowed = TRANSITIONS.get(old_status, set())

    if new_status not in allowed:
        raise ValueError(
            f"Invalid transition: "
            f"{old_status} -> {new_status}"
        )

    manifest["status"] = new_status
    manifest["updated_at"] = now_utc()

    if new_status == "FAILED":
        manifest["error"] = error or "Unknown error"
    elif new_status == "RUNNING":
        manifest["error"] = None

    save_manifest(path, manifest)

    print(f"[STATUS] {old_status} -> {new_status}")

def record_table(batch_id, table_name, parquet_path):
    """读取Parquet，记录表级元数据。"""

    import pyarrow.dataset as ds

    manifest_path = get_manifest_path(batch_id)
    manifest = load_batch(batch_id)

    if manifest["status"] != "RUNNING":
        raise ValueError(
            "Tables can only be recorded while RUNNING"
        )

    # 限制表名，只允许正常的标识符
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", table_name):
        raise ValueError("Invalid table name")

    source = Path(parquet_path).resolve()

    expected = (
        BATCH_ROOT / batch_id / "ads" / table_name
    ).resolve()

    if source != expected:
        raise ValueError(
            f"Table path must belong to current batch: {expected}"
        )

    if not source.is_dir():
        raise FileNotFoundError(
            f"Parquet directory not found: {source}"
        )

    dataset = ds.dataset(str(source), format="parquet")

    row_count = dataset.count_rows()

    # 当前只检查非空，后续增加金额和主键校验
    quality_pass = row_count > 0

    manifest["tables"][table_name] = {
        "path": source.relative_to(ROOT).as_posix(),
        "row_count": row_count,
        "quality_pass": quality_pass,
        "checked_at": now_utc(),
    }

    manifest["updated_at"] = now_utc()

    save_manifest(manifest_path, manifest)

    print(
        f"[TABLE] {table_name}: "
        f"rows={row_count}, "
        f"quality_pass={quality_pass}"
    )

def show_batch(batch_id):
    """显示批次信息。"""

    manifest = load_batch(batch_id)

    print(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2
        )
    )


def main():
    parser = argparse.ArgumentParser(
        description="Olist batch manager"
    )

    parser.add_argument(
        "action",
        choices=["create", "status", "show", "record"]
    )

    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--to")
    parser.add_argument("--error")
    parser.add_argument("--table")
    parser.add_argument("--path")

    args = parser.parse_args()

    if args.action == "create":
        create_batch(args.batch_id)

    elif args.action == "status":
        if not args.to:
            parser.error(
                "--to is required for status"
            )
        update_status(
            args.batch_id,
            args.to,
            args.error
        )

    elif args.action == "record":
        if not args.table or not args.path:
            parser.error(
                "--table and --path are required for record"
            )

        record_table(
            args.batch_id,
            args.table,
            args.path
        )

    elif args.action == "show":
        show_batch(args.batch_id)


if __name__ == "__main__":
    main()
