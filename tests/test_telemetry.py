import threading
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.duckdb_repositories import create_stage1_repositories
from marketcow.storage import Warehouse
from marketcow.telemetry import (
    MAX_LOG_EVENTS,
    SCHEMA_VERSION,
    Telemetry,
    instrument_duckdb_market_bars,
    sanitize_text,
    telemetry_call,
)
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

    def test_nested_mapping_keys_are_redacted(self):
        self.telemetry.log("query", payload={
            "password=hunter2": {"/Volumes/T9/private/key": "value"},
            "safe": {"token=abc": "ok"},
        })
        rendered = str(self.telemetry.snapshot())
        self.assertNotIn("hunter2", rendered)
        self.assertNotIn("/Volumes/T9", rendered)
        self.assertNotIn("token=abc", rendered)

    def test_default_duckdb_records_write_query_and_cache_without_clickhouse_effects(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            settings = Settings(
                root / "data-development/warehouse.duckdb",
                root / "data-development/raw", profile="development", port=8791,
                storage_root=root / "data-development",
                clickhouse_spool_path=root / "data-development/spool/clickhouse",
            )
            warehouse = Warehouse(settings.database_path)
            with patch("clickhouse_connect.get_client") as get_client:
                repositories, resources = create_stage1_repositories(settings, warehouse)
                repositories.market_bars.upsert_price_bars(
                    "MU", "1d", "raw", "fixture", "2026-07-20T00:00:01Z",
                    [{"timestamp": 1784505600, "open": 1, "high": 2, "low": 1,
                      "close": 2, "volume": 3}],
                )
                repositories.market_bars.get_price_bars("MU", "1d", "raw", 10)
                service = type("Service", (), {
                    "market_bar_repository": repositories.market_bars,
                    "close": lambda self: None,
                })()
                with TestClient(create_app(
                    settings, service,
                    lambda: datetime(2026, 7, 20, 0, 0, 2,
                                     tzinfo=timezone.utc),
                )) as client:
                    response = client.get(
                        "/v1/quotes/MU/history?interval=1d&adjustment=raw&refresh=false"
                    )
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.json()["cache_status"], "fresh")
            snapshot = repositories.market_bars.telemetry.snapshot()
            names = {item["name"] for item in snapshot["metrics"]}
            self.assertIn("ingest_write_latency_seconds", names)
            self.assertIn("query_latency_seconds", names)
            self.assertIn("cache_age_seconds", names)
            self.assertIs(repositories.market_bars, warehouse)
            self.assertIsNone(resources)
            get_client.assert_not_called()
            self.assertFalse(settings.clickhouse_spool_path.exists())
            self.assertFalse(any(item["name"] == "clickhouse_pressure"
                                 for item in snapshot["metrics"]))

    def test_safe_failure_is_fail_open_and_disabled_has_no_fake_pressure(self):
        self.telemetry.safe("gauge", "wal_items", 1, state="invalid")
        snapshot = self.telemetry.snapshot()
        self.assertEqual(snapshot["dropped_updates"], 1)
        self.assertFalse(snapshot["clickhouse"]["enabled"])
        self.assertFalse(any(item["name"] == "clickhouse_pressure"
                             for item in snapshot["metrics"]))

    def test_all_telemetry_surface_failures_preserve_primary_success_and_error(self):
        class BrokenTelemetry:
            def __getattribute__(self, name):
                if name.startswith("__"):
                    return super().__getattribute__(name)
                raise RuntimeError(f"telemetry {name} failed")

        with tempfile.TemporaryDirectory() as folder:
            warehouse = Warehouse(Path(folder) / "warehouse.duckdb")
            instrument_duckdb_market_bars(warehouse, BrokenTelemetry())
            count = warehouse.upsert_price_bars(
                "MU", "1d", "raw", "fixture", "2026-07-20T00:00:01Z",
                [{"timestamp": 1784505600, "open": 1, "high": 2, "low": 1,
                  "close": 2, "volume": 3}],
            )
            self.assertEqual(count, 1)
            self.assertEqual(len(warehouse.get_price_bars("MU", "1d", "raw", 10)), 1)
            settings = Settings(
                Path(folder) / "warehouse.duckdb", Path(folder) / "raw",
                storage_root=Path(folder) / "data-development",
            )
            service = type("Service", (), {
                "market_bar_repository": warehouse, "close": lambda self: None,
            })()
            with TestClient(create_app(settings, service)) as client:
                response = client.get(
                    "/v1/quotes/MU/history?interval=1d&adjustment=raw&refresh=false"
                )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["count"], 1)

        class PrimaryError(RuntimeError):
            pass

        class Repository:
            _marketcow_telemetry_instrumented = False

            def __getattr__(self, name):
                if name == "upsert_price_bars":
                    def fail(*args, **kwargs):
                        raise PrimaryError("primary sentinel")
                    return fail
                if name.startswith("get_"):
                    return lambda *args, **kwargs: []
                return lambda *args, **kwargs: None

        repository = instrument_duckdb_market_bars(Repository(), BrokenTelemetry())
        with self.assertRaisesRegex(PrimaryError, "primary sentinel"):
            repository.upsert_price_bars("MU", "1d", "raw", "fixture", "now", [])
        self.assertIsNone(telemetry_call(BrokenTelemetry(), "snapshot"))

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
