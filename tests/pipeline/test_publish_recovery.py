import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src" / "pipeline"))

import publish_batch


class TestPublishRecovery(unittest.TestCase):

    # 测试1：RUNNING状态禁止发布
    @patch("publish_batch.load_batch")
    def test_reject_running_batch(self, mock_load_batch):
        mock_load_batch.return_value = {
            "batch_id": "test_batch",
            "status": "RUNNING",
        }

        with self.assertRaisesRegex(
            ValueError,
            "Batch is not ready for publication",
        ):
            publish_batch.publish(
                batch_id="test_batch",
                apply=True,
            )

    # 测试2：当前已发布批次重复执行
    @patch("publish_batch.get_database_config")
    @patch("publish_batch.update_status")
    @patch("publish_batch.psycopg.connect")
    @patch("publish_batch.load_datasets")
    @patch("publish_batch.load_batch")
    def test_republish_current_batch(
        self,
        mock_load_batch,
        mock_load_datasets,
        mock_connect,
        mock_update_status,
        mock_db_config,
    ):
        mock_load_batch.return_value = {
            "batch_id": "test_batch",
            "status": "PUBLISHED",
        }

        # 不读取真实Parquet
        mock_load_datasets.return_value = {}

        mock_db_config.return_value = {
            "dbname": "mock_database"
        }

        # 模拟数据库连接和游标
        conn = mock_connect.return_value.__enter__.return_value
        cur = conn.cursor.return_value.__enter__.return_value

        # 当前批次与发布日志均为test_batch
        cur.fetchone.side_effect = [
            ("test_batch",),
            ("test_batch",),
        ]

        publish_batch.publish(
            batch_id="test_batch",
            apply=True,
        )

        # 不应再次修改Manifest状态
        mock_update_status.assert_not_called()

        # 不应重新替换ADS快照
        sql_statements = [
            str(call.args[0]).upper()
            for call in cur.execute.call_args_list
        ]

        self.assertFalse(
            any("TRUNCATE" in sql for sql in sql_statements)
        )

    # 测试3：历史已发布批次不得覆盖当前批次
    @patch("publish_batch.get_database_config")
    @patch("publish_batch.update_status")
    @patch("publish_batch.psycopg.connect")
    @patch("publish_batch.load_datasets")
    @patch("publish_batch.load_batch")
    def test_reject_historical_republish(
        self,
        mock_load_batch,
        mock_load_datasets,
        mock_connect,
        mock_update_status,
        mock_db_config,
    ):
        # 准备测试状态
        mock_load_batch.return_value = {
            "batch_id": "batch_A",
            "status": "PUBLISHED",
        }
        mock_load_datasets.return_value = {}
        mock_db_config.return_value = {
            "dbname": "mock_database"
        }

        # 模拟数据库
        conn = mock_connect.return_value.__enter__.return_value
        cur = conn.cursor.return_value.__enter__.return_value

        # 当前服务批次是B，但A存在于历史发布日志
        cur.fetchone.side_effect = [
            ("batch_B",),
            ("batch_A",),
        ]

        # 应拒绝历史批次A重新发布
        with self.assertRaisesRegex(
            ValueError,
            "Batch was published previously",
        ):
            publish_batch.publish(
                batch_id="batch_A",
                apply=True,
            )

        # 不允许修改Manifest
        mock_update_status.assert_not_called()

        # 不允许替换ADS服务表
        sql_statements = [
            str(call.args[0]).upper()
            for call in cur.execute.call_args_list
        ]
        self.assertFalse(
            any("TRUNCATE" in sql for sql in sql_statements)
        )

    # 测试4：数据库已提交，但Manifest尚未更新
    @patch("publish_batch.get_database_config")
    @patch("publish_batch.update_status")
    @patch("publish_batch.psycopg.connect")
    @patch("publish_batch.load_datasets")
    @patch("publish_batch.check_publish_ready")
    @patch("publish_batch.load_batch")
    def test_recover_after_database_commit(
        self,
        mock_load_batch,
        mock_check_ready,
        mock_load_datasets,
        mock_connect,
        mock_update_status,
        mock_db_config,
    ):
        # 数据库已发布，但Manifest仍是VALIDATED
        mock_load_batch.return_value = {
            "batch_id": "batch_A",
            "status": "VALIDATED",
        }

        mock_load_datasets.return_value = {}
        mock_db_config.return_value = {
            "dbname": "mock_database"
        }

        conn = mock_connect.return_value.__enter__.return_value
        cur = conn.cursor.return_value.__enter__.return_value

        # 数据库当前批次与发布日志均为batch_A
        cur.fetchone.side_effect = [
            ("batch_A",),
            ("batch_A",),
        ]

        publish_batch.publish(
            batch_id="batch_A",
            apply=True,
        )

        # 重试时仍执行发布准备检查
        mock_check_ready.assert_called_once_with("batch_A")

        # 只修复Manifest状态
        mock_update_status.assert_called_once_with(
            "batch_A", "PUBLISHED"
        )

        # 不能重新写入ADS或重复记录发布日志
        sql_statements = [
            str(call.args[0]).upper()
            for call in cur.execute.call_args_list
        ]

        self.assertFalse(
            any("TRUNCATE" in sql for sql in sql_statements)
        )
        self.assertFalse(
            any(
                "INSERT INTO ADS_PUBLICATION_LOG" in sql
                for sql in sql_statements
            )
        )

if __name__ == "__main__":
    unittest.main()