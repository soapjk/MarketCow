import fcntl
import json
import os
import tempfile
import unittest
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from marketcow.__main__ import main, operate_spool
from marketcow.clickhouse_writer import LocalClickHouseSpool
from marketcow.clickhouse_writer import normalize_bar, stable_batch_id
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

    def test_legacy_migration_covers_wal_raw_intents_and_scheduler_restart(self):
        from tests.test_clickhouse_writer import raw_bar

        rows = [normalize_bar("raw", raw_bar())]
        batch_id = stable_batch_id("raw", rows)
        intent_id = stable_batch_id("raw", rows)
        wal = self.spool.pending / f"{batch_id}.json"
        wal.write_text(json.dumps({
            "dataset": "raw", "batch_id": batch_id, "rows": rows, "attempts": 1,
            "created_at": "2026-07-20T00:00:00Z",
            "last_attempt_at": "2026-07-20T00:00:01Z", "last_error": "outage",
            "intent_id": intent_id,
        }), encoding="utf-8")
        raw_intent = self.spool.processing_intents / f"{intent_id}.json"
        raw_intent.write_text(json.dumps({
            "intent_id": intent_id, "rows": rows, "pending": [batch_id],
            "callback_attempts": 0, "last_callback_error": "",
        }), encoding="utf-8")
        group = {"symbol": "000000.SH", "interval": "1m", "adjustment": "raw",
                 "start": rows[0]["bar_time"], "end": rows[0]["bar_time"]}
        task_id = hashlib.sha256(json.dumps(
            group, sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest()
        scheduler = self.spool.root / "canonical-scheduler/failed"
        scheduler.mkdir(parents=True)
        task = scheduler / f"{task_id}.json"
        task.write_text(json.dumps({
            "task_id": task_id, **group, "attempts": 10,
            "created_at_epoch": 1.0, "next_attempt_epoch": 2.0,
            "last_error": "outage",
        }), encoding="utf-8")

        result = self.operator.migrate_legacy(10)
        self.assertEqual(result["migrated"], 3)
        self.assertEqual(result["invalid"], 0)
        for path in (wal, raw_intent, task):
            self.assertIn("_checksum", self.spool.read(path, require_checksum=True))
        again = self.operator.migrate_legacy(10)
        self.assertEqual(again["migrated"], 0)

    def test_legacy_migration_rejects_unknown_schema_and_preserves_on_write_failure(self):
        malicious = self.spool.pending / ("a" * 64 + ".json")
        malicious.write_text(json.dumps({
            "dataset": "raw", "batch_id": "a" * 64, "rows": [], "attempts": 1,
            "created_at": "2026-07-20T00:00:00Z",
            "last_attempt_at": "2026-07-20T00:00:01Z", "last_error": "",
            "unexpected": "bypass",
        }), encoding="utf-8")
        result = self.operator.migrate_legacy(10)
        self.assertEqual(result["invalid"], 1)
        self.assertFalse(malicious.exists())
        quarantined = next(self.spool.quarantine.glob("*" + malicious.name))
        self.assertNotIn("_checksum", json.loads(quarantined.read_text(encoding="utf-8")))

        from tests.test_clickhouse_writer import raw_bar
        rows = [normalize_bar("raw", raw_bar())]
        batch_id = stable_batch_id("raw", rows)
        healthy = self.spool.pending / f"{batch_id}.json"
        original = {"dataset": "raw", "batch_id": batch_id, "rows": rows,
                    "attempts": 1, "created_at": "2026-07-20T00:00:00Z",
                    "last_attempt_at": "2026-07-20T00:00:01Z", "last_error": ""}
        healthy.write_text(json.dumps(original), encoding="utf-8")
        with patch.object(self.spool, "_atomic_json", side_effect=PermissionError("denied")):
            failed = self.operator.migrate_legacy(10)
        self.assertEqual(failed["errors"], 1)
        self.assertEqual(json.loads(healthy.read_text(encoding="utf-8")), original)

    def test_legacy_migration_budget_ignores_signed_prefix_and_reaches_later_kinds(self):
        from tests.test_clickhouse_writer import raw_bar

        for index in range(5):
            self.spool._atomic_json(self.spool.pending / f"signed-{index}.json", {
                "fixture": index,
            })
        rows = [normalize_bar("raw", raw_bar())]
        intent_id = stable_batch_id("raw", rows)
        intent = self.spool.intents / f"{intent_id}.json"
        intent.write_text(json.dumps({
            "intent_id": intent_id, "rows": rows, "pending": [],
            "callback_attempts": 0, "last_callback_error": "",
        }), encoding="utf-8")
        group = {"symbol": "MU", "interval": "1m", "adjustment": "raw",
                 "start": rows[0]["bar_time"], "end": rows[0]["bar_time"]}
        task_id = hashlib.sha256(json.dumps(
            group, sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest()
        scheduler = self.spool.root / "canonical-scheduler/pending"
        scheduler.mkdir(parents=True)
        task = scheduler / f"{task_id}.json"
        task.write_text(json.dumps({
            "task_id": task_id, **group, "attempts": 0,
            "created_at_epoch": 1.0, "next_attempt_epoch": 2.0, "last_error": "",
        }), encoding="utf-8")

        first = self.operator.migrate_legacy(limit=1)
        self.assertEqual(first["checked"], 1)
        self.assertEqual(first["migrated"], 1)
        self.assertEqual(first["remaining"], 1)
        self.assertTrue(first["truncated"])
        restarted = SpoolOperator(self.spool)
        second = restarted.migrate_legacy(limit=1)
        self.assertEqual(second["migrated"], 1)
        self.assertEqual(second["remaining"], 0)
        self.assertFalse(second["truncated"])
        for path in (intent, task):
            self.spool.read(path, require_checksum=True)

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
