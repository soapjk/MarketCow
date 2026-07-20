import threading
import unittest
from datetime import datetime, timezone

from marketcow.telemetry import MAX_LOG_EVENTS, SCHEMA_VERSION, Telemetry, sanitize_text
from marketcow.contract_gate import compare_contract


class TelemetryTest(unittest.TestCase):
    def setUp(self):
        self.wall = lambda: datetime(2026, 7, 20, tzinfo=timezone.utc)
        self.telemetry = Telemetry(clock=lambda: 10.0, wall_clock=self.wall)

    def test_schema_units_buckets_and_fixed_clock(self):
        self.telemetry.counter("canonical_rebuild_total", outcome="ok")
        self.telemetry.gauge("wal_items", 3, state="pending")
        self.telemetry.histogram("query_latency_seconds", 0.025,
                                 backend="duckdb", query="range", outcome="ok")
        snapshot = self.telemetry.snapshot()
        self.assertEqual(snapshot["schema"], SCHEMA_VERSION)
        self.assertEqual(snapshot["generated_at"], "2026-07-20T00:00:00+00:00")
        self.assertEqual(snapshot["restart_semantics"], "process_local_reset")
        histogram = next(item for item in snapshot["metrics"]
                         if item["name"] == "query_latency_seconds")
        self.assertEqual(histogram["unit"], "seconds")
        self.assertEqual(histogram["value"]["count"], 1)
        self.assertEqual(histogram["value"]["buckets"][-1], 1)

    def test_label_cardinality_is_strict_and_symbol_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "labels"):
            self.telemetry.gauge("wal_items", 1, state="pending", symbol="MU")
        with self.assertRaisesRegex(ValueError, "not allowed"):
            self.telemetry.gauge("wal_items", 1, state="arbitrary")
        self.assertLessEqual(self.telemetry.snapshot()["limits"]["metric_series"], 500)

    def test_concurrent_update_and_snapshot_are_consistent(self):
        def update():
            for _ in range(1000):
                self.telemetry.counter("canonical_rebuild_total", outcome="ok")

        threads = [threading.Thread(target=update) for _ in range(8)]
        for thread in threads:
            thread.start()
        snapshots = [self.telemetry.snapshot() for _ in range(20)]
        for thread in threads:
            thread.join()
        final = next(item for item in self.telemetry.snapshot()["metrics"]
                     if item["name"] == "canonical_rebuild_total")
        self.assertEqual(final["value"], 8000)
        self.assertTrue(all(snapshot["schema"] == SCHEMA_VERSION for snapshot in snapshots))

    def test_logs_are_bounded_redacted_and_truncated(self):
        secret = ("postgresql://user:password@127.0.0.1/db token=abc "
                  "/Volumes/T9/projects/private/data " + "x" * 2000)
        for index in range(MAX_LOG_EVENTS + 10):
            self.telemetry.log("cache", index=index)
        self.telemetry.log("query", "error", error=RuntimeError(secret))
        snapshot = self.telemetry.snapshot()
        self.assertEqual(len(snapshot["logs"]), MAX_LOG_EVENTS)
        rendered = str(snapshot)
        self.assertNotIn("user:password", rendered)
        self.assertNotIn("token=abc", rendered)
        self.assertNotIn("/Volumes/T9", rendered)
        self.assertLessEqual(len(sanitize_text(secret)), 1000)

    def test_safe_failure_is_fail_open_and_disabled_has_no_fake_pressure(self):
        self.telemetry.safe("gauge", "wal_items", 1, state="invalid")
        snapshot = self.telemetry.snapshot()
        self.assertEqual(snapshot["dropped_updates"], 1)
        self.assertFalse(snapshot["clickhouse"]["enabled"])
        self.assertFalse(any(item["name"] == "clickhouse_pressure"
                             for item in snapshot["metrics"]))

    def test_contract_cache_fallback_and_clickhouse_pressure_series(self):
        compare_contract({"bars": [1]}, {"bars": [2]}, telemetry=self.telemetry,
                         contract="range")
        self.telemetry.counter(
            "backend_fallback_total", from_backend="clickhouse_canonical",
            to_backend="duckdb", query="range",
        )
        self.telemetry.histogram("cache_age_seconds", 901, status="stale")
        enabled = Telemetry(wall_clock=self.wall, clickhouse_enabled=True)
        enabled.clickhouse_pressure(2, 0.75)
        names = {item["name"] for item in self.telemetry.snapshot()["metrics"]}
        self.assertEqual(names, {
            "backend_fallback_total", "cache_age_seconds", "contract_mismatch_total",
        })
        pressure = [item for item in enabled.snapshot()["metrics"]
                    if item["name"] == "clickhouse_pressure"]
        self.assertEqual(len(pressure), 2)


if __name__ == "__main__":
    unittest.main()
