
import re
from pathlib import Path, PureWindowsPath

from batch_manager import ROOT, BATCH_ROOT


# M5历史批次创建时使用的Windows项目目录
LEGACY_WINDOWS_ROOT = PureWindowsPath(
    "D:/SparkEcommerce"
)


def resolve_table_path(batch_id, table_name, recorded_path):
    """
    将Manifest中的路径转换为当前运行环境中的真实路径。

    支持：
    1. 项目相对路径
    2. 历史Windows绝对路径
    3. 当前环境绝对路径

    只允许当前批次指定ADS表的目录。
    """

    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", batch_id):
        raise ValueError("Invalid batch_id")

    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", table_name):
        raise ValueError("Invalid table name")

    if not isinstance(recorded_path, str) or not recorded_path:
        raise ValueError("Invalid recorded path")

    expected = (
        BATCH_ROOT / batch_id / "ads" / table_name
    ).resolve()

    native_path = Path(recorded_path)
    windows_path = PureWindowsPath(recorded_path)

    if native_path.is_absolute():
        # 当前操作系统能够直接识别的绝对路径
        actual = native_path.resolve()

    elif windows_path.is_absolute():
        # 兼容M5历史Windows Manifest
        try:
            relative = windows_path.relative_to(
                LEGACY_WINDOWS_ROOT
            )
        except ValueError as exc:
            raise ValueError(
                "Windows path is outside legacy project root"
            ) from exc

        actual = (
            ROOT / Path(*relative.parts)
        ).resolve()

    else:
        # 新Manifest：项目相对路径
        if "\\" in recorded_path or ":" in recorded_path:
            raise ValueError(
                "Invalid relative path format"
            )

        actual = (
            ROOT / recorded_path
        ).resolve()

    if actual != expected:
        raise ValueError(
            f"Path does not belong to batch table: "
            f"{batch_id}/{table_name}"
        )

    return expected