import fcntl
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from marketcow.__main__ import main, operate_spool
from marketcow.clickhouse_writer import LocalClickHouseSpool
from marketcow.config import Settings
from marketcow.spool_operator import SpoolOperator


class SpoolOperatorTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name) / "data-development"
        self.spool = LocalClickHouseSpool(
            self.root / "spool/clickhouse", self.root, quota_bytes=1048576,
            quota_warning_ratio=0.5,
        )
        self.operator = SpoolOperator(self.spool)

    def tearDown(self):
        self.folder.cleanup()

    def test_checksums_detect_truncation_invalid_json_and_tampering(self):
        good = self.spool.enqueue("raw", "good", [{"value": 1}], "outage")
        self.assertEqual(self.spool.read(good, require_checksum=True)["batch_id"], "good")
        truncated = self.spool.pending / "truncated.json"
        truncated.write_text('{"dataset":"raw"', encoding="utf-8")
        invalid = self.spool.pending / "invalid.json"
        invalid.write_text("[]", encoding="utf-8")
        tampered = self.spool.enqueue("raw", "tampered", [{"value": 1}], "outage")
        payload = json.loads(tampered.read_text(encoding="utf-8"))
        payload["rows"] = [{"value": 2}]
        tampered.write_text(json.dumps(payload), encoding="utf-8")
        result = self.operator.list_items("wal-pending", 10)
        self.assertEqual(sum(item["status"] == "corrupt" for item in result["items"]), 3)
        moved = self.operator.quarantine_corrupt(10)
        self.assertEqual(moved["moved"], 3)
        self.assertTrue(good.exists())
        self.assertGreaterEqual(len(list(self.spool.quarantine.glob("*.json"))), 3)

    def test_quota_warns_then_rejects_before_atomic_write(self):
        self.spool._atomic_json(self.spool.pending / "warning.json", {
            "blob": "x" * 600000,
        })
        self.assertTrue(self.spool.usage()["warning"])
        with self.assertRaisesRegex(OSError, "quota exceeded"):
            self.spool._atomic_json(self.spool.pending / "rejected.json", {
                "blob": "y" * 600000,
            })
        self.assertFalse((self.spool.pending / "rejected.json").exists())

    def test_audit_links_raw_wal_intents_and_bounds_orphans(self):
        self.spool.enqueue("raw", "linked", [{"value": 1}], "outage")
        self.spool.save_intent("intent", [{"value": 1}], ["linked", "missing"])
        self.spool.enqueue("canonical", "orphan", [{"value": 2}], "outage")
        result = self.operator.audit(100)
        self.assertEqual(result["missing_wal_references"], ["missing"])
        self.assertEqual(result["orphan_wal"], ["orphan"])
        self.assertEqual(result["status"], "attention")

    def test_dead_letter_retry_and_replayed_retention_boundary(self):
        failed = self.root / "spool/clickhouse/canonical-scheduler/failed"
        failed.mkdir(parents=True)
        self.spool._atomic_json(failed / "task.json", {
            "task_id": "task", "symbol": "MU", "interval": "1m",
            "adjustment": "raw", "start": "2026-07-20T00:00:00Z",
            "end": "2026-07-20T00:01:00Z", "attempts": 10,
            "next_attempt_epoch": 999, "last_error": "outage",
        })
        retried = self.operator.retry_scheduler_failed(10)
        self.assertEqual(retried["retried"], 1)
        pending = self.root / "spool/clickhouse/canonical-scheduler/pending/task.json"
        self.assertEqual(self.spool.read(pending)["attempts"], 0)

        for name, replayed_at in (("old", "2026-07-20T00:00:00Z"),
                                  ("boundary", "2026-07-20T00:01:40Z"),
                                  ("new", "2026-07-20T00:01:41Z")):
            self.spool._atomic_json(self.spool.replayed / f"{name}.json", {
                "dataset": "raw", "batch_id": name, "rows": [],
                "replayed_at": replayed_at,
            })
        cleaned = self.operator.cleanup_replayed(
            100, 10, datetime(2026, 7, 20, 0, 3, 20, tzinfo=timezone.utc)
        )
        self.assertEqual(cleaned["removed"], 2)
        self.assertTrue((self.spool.replayed / "new.json").exists())
        self.assertTrue(self.operator.audit_log.exists())

    def test_mutations_are_serial_and_permission_error_does_not_touch_healthy(self):
        lock_path = self.spool.root / ".operator.lock"
        with lock_path.open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(RuntimeError, "operation is active"):
                self.operator.cleanup_replayed(0)
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        corrupt = self.spool.pending / "corrupt.json"
        corrupt.write_text("{", encoding="utf-8")
        healthy = self.spool.enqueue("raw", "healthy", [], "outage")
        with patch.object(self.spool, "quarantine_item", side_effect=PermissionError("denied")):
            result = self.operator.quarantine_corrupt(10)
        self.assertEqual(result["errors"], 1)
        self.assertTrue(healthy.exists())

    def test_allowed_root_symlink_and_production_cli_boundaries(self):
        outside = Path(self.folder.name) / "outside"
        outside.mkdir()
        link = self.root / "escape"
        link.symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "allowed development root"):
            LocalClickHouseSpool(link / "spool", self.root)
        production = Settings(
            self.root / "db", self.root / "raw",
            storage_root=self.root, clickhouse_spool_path=self.spool.root,
        )
        with self.assertRaisesRegex(ValueError, "development-only"):
            operate_spool(production, "status")

        scheduler = self.spool.root / "canonical-scheduler"
        scheduler.mkdir()
        (scheduler / "failed").symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "escapes the allowed spool root"):
            self.operator.list_items("scheduler-failed")

    def test_read_only_cli_does_not_create_an_absent_spool(self):
        absent = self.root / "absent-spool"
        settings = Settings(
            self.root / "db", self.root / "raw", profile="development",
            storage_root=self.root, clickhouse_spool_path=absent,
        )
        result = operate_spool(settings, "audit")
        self.assertEqual(result["status"], "ok")
        self.assertFalse(result["present"])
        self.assertFalse(absent.exists())

    def test_cli_machine_result_and_exit_semantics(self):
        environment = {
            "MARKETCOW_HOME": str(self.root),
            "MARKETCOW_CLICKHOUSE_SPOOL": str(self.spool.root),
        }
        with patch.dict(os.environ, environment, clear=True), patch("builtins.print") as output:
            self.assertEqual(main(["--profile", "development", "spool", "audit"]), 0)
        payload = json.loads(output.call_args.args[0])
        self.assertEqual(payload["status"], "ok")
        self.assertIn("checked", payload)


if __name__ == "__main__":
    unittest.main()
