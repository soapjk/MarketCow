from __future__ import annotations

import json
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from marketcow.clickhouse_writer import LocalClickHouseSpool
from marketcow.clickhouse_writer import ReliableClickHouseWriter
from marketcow.spool_operator import MAX_AUDIT_BYTES, SpoolOperator
from marketcow.v2_observability import (
    V2_TELEMETRY_SCHEMA, V2Telemetry, record_v2_operation, validate_v2_snapshot,
)


class V2ObservabilityTest(unittest.TestCase):
    def setUp(self):
        self.now = 100.0
        self.telemetry = V2Telemetry(
            clock=lambda: self.now,
            wall_clock=lambda: datetime(2026, 7, 21, tzinfo=timezone.utc),
        )

    def test_versioned_schema_units_buckets_labels_and_no_duckdb(self):
        self.telemetry.record_authoritative("acknowledged")
        self.telemetry.record_replay("replayed", 2)
        self.telemetry.record_backup_restore("restore", "resumed")
        self.telemetry.record_operator("audit", "ok")
        self.telemetry.histogram(
            "v2_postgresql_query_latency_seconds", 0.025,
            operation="query", outcome="ok",
        )
        self.telemetry.histogram(
            "query_latency_seconds", 0.01, backend="duckdb",
            query="range", outcome="fallback",
        )
        snapshot = self.telemetry.snapshot()
        validate_v2_snapshot(snapshot)
        self.assertEqual(snapshot["schema"], V2_TELEMETRY_SCHEMA)
        rendered = json.dumps(snapshot, sort_keys=True)
        self.assertNotIn("duckdb", rendered)
        self.assertNotIn('"fallback"', rendered)
        pg = snapshot["metric_contract"]["v2_postgresql_query_latency_seconds"]
        self.assertEqual(pg["unit"], "seconds")
        self.assertEqual(pg["buckets"], [0.001, 0.005, 0.025, 0.1, 0.5, 2.0, 8.0])

    def test_concurrent_snapshot_cardinality_and_redaction_are_bounded(self):
        def update():
            for _ in range(100):
                self.telemetry.record_operator("list", "ok")
                self.telemetry.safe_log(
                    "operator", payload={
                        "password=hunter2": "postgresql://user:secret@localhost/db",
                        "path": "/Volumes/T9/private/token",
                    },
                )

        threads = [threading.Thread(target=update) for _ in range(4)]
        for thread in threads:
            thread.start()
        snapshots = [self.telemetry.snapshot() for _ in range(20)]
        for thread in threads:
            thread.join()
        for snapshot in snapshots + [self.telemetry.snapshot()]:
            validate_v2_snapshot(snapshot)
        rendered = json.dumps(self.telemetry.snapshot())
        self.assertNotIn("hunter2", rendered)
        self.assertNotIn("user:secret", rendered)
        self.assertNotIn("/Volumes/T9", rendered)
        self.assertLessEqual(len(self.telemetry.snapshot()["logs"]), 200)

    def test_all_telemetry_failures_are_fail_open(self):
        class Broken:
            def record_operator(self, *_args, **_kwargs):
                raise RuntimeError("telemetry failed")

        marker = []
        record_v2_operation(Broken(), "record_operator", "audit", "ok")
        marker.append("business-result")
        self.assertEqual(marker, ["business-result"])

    def test_operator_audit_is_bounded_and_redacted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spool = LocalClickHouseSpool(root / "spool", root)
            operator = SpoolOperator(spool)
            for _ in range(400):
                operator._trace(
                    "audit", "error",
                    "password=hunter2 postgresql://user:secret@localhost/db " + "x" * 900,
                )
            data = operator.audit_log.read_bytes()
            self.assertLessEqual(len(data), MAX_AUDIT_BYTES)
            self.assertNotIn(b"hunter2", data)
            self.assertNotIn(b"user:secret", data)
            self.assertEqual(operator.audit_log.stat().st_mode & 0o777, 0o600)
            for line in data.splitlines():
                self.assertIsInstance(json.loads(line), dict)
            operator.audit_log.unlink()
            operator.audit_log.symlink_to(root / "outside-audit")
            with self.assertRaises(OSError):
                operator._trace("audit", "ok")
            self.assertFalse((root / "outside-audit").exists())

    def test_authoritative_write_and_replay_emit_v2_outcomes(self):
        class Repository:
            def insert_raw_bars(self, rows, batch_id=""):
                return len(rows)

        row = {
            "symbol": "000001.SZ", "market": "CN", "interval": "1m",
            "adjustment": "raw", "bar_time": "2026-07-21T01:00:00Z",
            "open": "1", "high": "1", "low": "1", "close": "1",
            "volume": "1", "amount": None, "source": "fixture",
            "source_sequence": "1", "observed_at": "2026-07-21T01:00:01Z",
            "ingested_at": "2026-07-21T01:00:02Z", "raw_artifact_id": None,
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spool = LocalClickHouseSpool(root / "spool", root)
            spool.telemetry = self.telemetry
            result = ReliableClickHouseWriter(Repository(), spool, 1000).write("raw", [row])
            self.assertTrue(result["acknowledged"])
            self.assertEqual(ReliableClickHouseWriter(Repository(), spool, 1000).replay()[
                "replayed"
            ], 0)
        metric = next(item for item in self.telemetry.snapshot()["metrics"]
                      if item["name"] == "v2_authoritative_write_total")
        self.assertEqual(metric["labels"], {"outcome": "acknowledged"})


if __name__ == "__main__":
    unittest.main()
