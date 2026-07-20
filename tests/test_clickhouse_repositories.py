import os
import tempfile
import unittest
import uuid
from pathlib import Path

import clickhouse_connect

from marketcow.clickhouse_repositories import (
    ClickHouseDatabase,
    ClickHouseMarketBarRepository,
)
from marketcow.clickhouse_shadow import ShadowMarketBarRepository
from marketcow.clickhouse_canonical import CanonicalMarketBarBuilder
from marketcow.clickhouse_writer import LocalClickHouseSpool, ReliableClickHouseWriter
from marketcow.storage import Warehouse


class ClickHouseDatabaseBoundaryTest(unittest.TestCase):
    def test_only_simple_isolated_local_database_is_allowed(self):
        with self.assertRaisesRegex(ValueError, "simple identifier"):
            ClickHouseDatabase("127.0.0.1", 8123, "bad-name_test")
        with self.assertRaisesRegex(ValueError, "loopback"):
            ClickHouseDatabase("clickhouse.example.com", 8123, "marketcow_test")
        with self.assertRaisesRegex(ValueError, "must end"):
            ClickHouseDatabase("127.0.0.1", 8123, "marketcow_production")

    def test_closed_database_rejects_operations(self):
        database = ClickHouseDatabase("127.0.0.1", 8123, "marketcow_test")
        with self.assertRaisesRegex(RuntimeError, "not open"):
            database.migrate()


@unittest.skipUnless(
    os.getenv("MARKETCOW_TEST_CLICKHOUSE_HOST"),
    "set MARKETCOW_TEST_CLICKHOUSE_HOST to run ClickHouse integration tests",
)
class ClickHouseRepositoryIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.host = os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"]
        cls.port = int(os.getenv("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123"))
        cls.username = os.getenv("MARKETCOW_TEST_CLICKHOUSE_USERNAME", "default")
        cls.password = os.getenv("MARKETCOW_TEST_CLICKHOUSE_PASSWORD", "")
        cls.database_name = "marketcow_" + uuid.uuid4().hex[:12] + "_test"
        cls.database = ClickHouseDatabase(
            cls.host, cls.port, cls.database_name, cls.username, cls.password
        )
        cls.database.open()
        cls.repository = ClickHouseMarketBarRepository(cls.database)

    @classmethod
    def tearDownClass(cls):
        cls.database.close()
        client = clickhouse_connect.get_client(
            host=cls.host, port=cls.port, username=cls.username,
            password=cls.password, database="default",
        )
        try:
            client.command(f"DROP DATABASE IF EXISTS `{cls.database_name}`")
        finally:
            client.close()

    def test_migration_is_idempotent_and_diagnostics_are_healthy(self):
        self.database.migrate()
        diagnostics = self.database.diagnostics()
        self.assertEqual(diagnostics["status"], "ok")
        self.assertEqual(diagnostics["database"], self.database_name)
        self.assertTrue({"schema_migrations", "market_bar_raw",
                         "market_bar_canonical"}.issubset(diagnostics["tables"]))
        versions = self.database.client.query(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).result_rows
        self.assertEqual(versions, [(1,), (2,), (3,)])

    def test_raw_and_canonical_round_trip_with_replacing_keys(self):
        raw = {
            "symbol": "600519.SH", "market": "CN", "interval": "1m",
            "adjustment": "raw", "bar_time": "2026-07-20T01:31:00Z",
            "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0,
            "volume": 1000.0, "amount": 101000.0, "source": "fixture",
            "source_sequence": "1", "observed_at": "2026-07-20T01:31:01Z",
            "ingested_at": "2026-07-20T01:31:02Z", "raw_artifact_id": "artifact-1",
        }
        self.assertEqual(self.repository.insert_raw_bars([raw]), 1)
        self.assertEqual(self.repository.insert_raw_bars([raw]), 1)
        rows = self.repository.query_raw_bars("600519.SH")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source"], "fixture")
        canonical = {
            **{key: raw[key] for key in ["symbol", "market", "interval", "adjustment",
                "bar_time", "open", "high", "low", "close", "volume", "amount",
                "observed_at", "ingested_at", "raw_artifact_id"]},
            "selected_source": "fixture", "source_count": 1,
            "quality_status": "single_source", "input_fingerprint": "fixture-hash",
            "version": 1,
            "updated_at": "2026-07-20T01:31:03Z",
        }
        self.assertEqual(self.repository.insert_canonical_bars([canonical]), 1)
        count = self.database.client.query(
            "SELECT count() FROM market_bar_canonical FINAL "
            "WHERE symbol='600519.SH'"
        ).result_rows[0][0]
        self.assertEqual(count, 1)

    def test_chunked_writer_repeat_batch_and_spool_replay(self):
        with tempfile.TemporaryDirectory() as folder:
            writer = ReliableClickHouseWriter(
                self.repository,
                LocalClickHouseSpool(Path(folder) / "spool", Path(folder)), 1000,
            )
            rows = [{
                "symbol": f"{index:06d}.SZ", "market": "CN", "interval": "1m",
                "adjustment": "raw", "bar_time": "2026-07-20T02:00:00Z",
                "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 100,
                "amount": 1050, "source": "writer_fixture", "source_sequence": str(index),
                "observed_at": "2026-07-20T02:00:01Z",
                "ingested_at": "2026-07-20T02:00:02Z", "raw_artifact_id": "artifact-w",
            } for index in range(2001)]
            first = writer.write("raw", rows)
            second = writer.write("raw", rows)
            self.assertEqual(first["batches"], 3)
            self.assertEqual(second["batches"], 3)
            logical = self.database.client.query(
                "SELECT count() FROM market_bar_raw FINAL WHERE source='writer_fixture'"
            ).result_rows[0][0]
            self.assertEqual(logical, 2001)

            original = self.repository.database.client
            self.repository.database.client = None
            try:
                failed = writer.write("raw", [rows[0]])
            finally:
                self.repository.database.client = original
            self.assertEqual(failed["spooled"], 1)
            self.assertEqual(writer.replay(), {"attempted": 1, "replayed": 1, "failed": 0})

    def test_canonical_bounded_build_is_stable_and_replayable(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            writer = ReliableClickHouseWriter(
                self.repository, LocalClickHouseSpool(root / "spool", root), 1000
            )
            builder = CanonicalMarketBarBuilder(
                self.repository, writer, ("preferred_fixture", "other_fixture")
            )
            base = {
                "symbol": "CANONICAL.HK", "market": "HK", "interval": "1m",
                "adjustment": "raw", "open": 10, "high": 12, "low": 9,
                "close": 11, "volume": 100, "amount": 1100,
                "source_sequence": "1", "observed_at": "2026-07-20T03:00:01Z",
                "ingested_at": "2026-07-20T03:00:02Z", "raw_artifact_id": "c1",
            }
            self.repository.insert_raw_bars([
                {**base, "bar_time": "2026-07-20T02:59:59Z", "source": "outside_fixture"},
                {**base, "bar_time": "2026-07-20T03:00:00Z", "source": "other_fixture"},
                {**base, "bar_time": "2026-07-20T03:00:00Z",
                 "source": "preferred_fixture", "close": 11.000000001,
                 "raw_artifact_id": "c2"},
            ])
            arguments = ("CANONICAL.HK", "1m", "raw",
                         "2026-07-20T03:00:00Z", "2026-07-20T03:00:00Z", 100)
            self.assertEqual(builder.rebuild(*arguments)["status"], "ok")
            self.assertEqual(builder.rebuild(*arguments)["status"], "ok")
            canonical, truncated = self.repository.query_range(
                "canonical", *arguments
            )
            self.assertFalse(truncated)
            self.assertEqual(len(canonical), 1)
            self.assertEqual(canonical[0]["selected_source"], "preferred_fixture")
            self.assertEqual(canonical[0]["quality_status"], "multi_source_consistent")
            self.assertEqual(canonical[0]["version"], 1)

            original = self.repository.database.client
            self.repository.database.client = None
            try:
                failed = writer.write("canonical", [canonical[0]])
            finally:
                self.repository.database.client = original
            self.assertEqual(failed["spooled"], 1)
            self.assertEqual(writer.replay(), {"attempted": 1, "replayed": 1, "failed": 0})

    def test_canonical_history_contract_filters_limits_orders_and_final(self):
        base = {
            "symbol": "HISTORY.HK", "market": "HK", "interval": "1m",
            "adjustment": "raw", "open": 10, "high": 12, "low": 9,
            "close": 11, "raw_close": 22, "adjustment_factor": 0.5,
            "volume": 100, "amount": None,
            "selected_source": "fixture", "source_count": 1,
            "quality_status": "single_source", "input_fingerprint": "history-1",
            "observed_at": "2026-07-20T04:00:01Z",
            "ingested_at": "2026-07-20T04:00:02Z", "raw_artifact_id": None,
            "updated_at": "2026-07-20T04:00:03Z",
        }
        self.repository.insert_canonical_bars([
            {**base, "bar_time": "2026-07-20T04:00:00Z", "version": 1},
            {**base, "bar_time": "2026-07-20T04:01:00Z", "version": 1,
             "close": 12, "input_fingerprint": "history-old"},
            {**base, "bar_time": "2026-07-20T04:01:00Z", "version": 2,
             "close": 13, "input_fingerprint": "history-new"},
            {**base, "bar_time": "2026-07-20T04:02:00Z", "version": 1,
             "close": 14, "input_fingerprint": "history-3"},
            {**base, "bar_time": "2026-07-20T04:03:00Z", "version": 1,
             "interval": "5m", "input_fingerprint": "filtered"},
        ])
        bars = self.repository.get_canonical_price_bars(
            "HISTORY.HK", "1m", "raw", 2
        )
        self.assertEqual([bar["timestamp"] for bar in bars], sorted(
            bar["timestamp"] for bar in bars
        ))
        self.assertEqual([bar["close"] for bar in bars], [13.0, 14.0])
        self.assertEqual(bars[0]["source"], "fixture")
        self.assertIsNone(bars[0]["amount"])
        self.assertEqual(bars[0]["source_payload"]["version"], 2)
        self.assertEqual(bars[0]["raw_close"], 22.0)
        self.assertEqual(bars[0]["adjustment_factor"], 0.5)
        ranged, truncated = self.repository.get_canonical_price_bars_range(
            "HISTORY.HK", "1m", "raw", "2026-07-20T12:00:00+08:00",
            "2026-07-20T04:02:00Z", 1,
        )
        self.assertTrue(truncated)
        self.assertEqual(len(ranged), 1)
        self.assertEqual(ranged[0]["bar_at"], "2026-07-20T04:00:00+00:00")
        empty, truncated = self.repository.get_canonical_price_bars_range(
            "HISTORY.HK", "1m", "raw", "2026-07-21T00:00:00Z",
            "2026-07-21T01:00:00Z", 10,
        )
        self.assertEqual(empty, [])
        self.assertFalse(truncated)
        with self.assertRaisesRegex(ValueError, "include a timezone"):
            self.repository.get_canonical_price_bars_range(
                "HISTORY.HK", "1m", "raw", "2026-07-20T04:00:00",
                "2026-07-20T04:02:00", 10,
            )

    def test_canonical_cross_section_exact_time_filter_final_and_truncation(self):
        base = {
            "market": "US", "interval": "1m", "adjustment": "raw",
            "bar_time": "2026-07-20T05:00:00Z", "open": 10, "high": 12,
            "low": 9, "close": 11, "raw_close": None,
            "adjustment_factor": None, "volume": 100, "amount": None,
            "selected_source": "fixture", "source_count": 1,
            "quality_status": "single_source", "observed_at": "2026-07-20T05:00:01Z",
            "ingested_at": "2026-07-20T05:00:02Z", "raw_artifact_id": None,
            "updated_at": "2026-07-20T05:00:03Z",
        }
        rows = []
        for symbol in ("CROSS-A", "CROSS-B", "CROSS-C"):
            rows.append({**base, "symbol": symbol, "version": 1,
                         "input_fingerprint": symbol + "-v1"})
        rows.extend([
            {**base, "symbol": "CROSS-B", "version": 2, "close": 22,
             "input_fingerprint": "CROSS-B-v2"},
            {**base, "symbol": "CROSS-STALE", "version": 1,
             "bar_time": "2026-07-20T04:59:00Z", "input_fingerprint": "stale"},
            {**base, "symbol": "CROSS-WRONG", "version": 1, "interval": "5m",
             "input_fingerprint": "wrong"},
        ])
        self.repository.insert_canonical_bars(rows)
        bars, truncated = self.repository.get_canonical_price_bars_cross_section(
            "1m", "raw", "2026-07-20T13:00:00+08:00", 2,
            ["CROSS-C", "CROSS-B", "CROSS-A", "CROSS-A"],
        )
        self.assertEqual([row["symbol"] for row in bars], ["CROSS-A", "CROSS-B"])
        self.assertEqual(bars[1]["close"], 22.0)
        self.assertTrue(truncated)
        empty, truncated = self.repository.get_canonical_price_bars_cross_section(
            "1m", "adjusted", "2026-07-20T05:00:00Z", 10
        )
        self.assertEqual(empty, [])
        self.assertFalse(truncated)

    def test_raw_multisource_range_filter_final_provenance_and_truncation(self):
        base = {
            "symbol": "RAW.HK", "market": "HK", "interval": "1m",
            "adjustment": "raw", "bar_time": "2026-07-20T06:00:00Z",
            "open": 10, "high": 12, "low": 9, "close": 11,
            "raw_close": 22, "adjustment_factor": 0.5,
            "volume": 100, "amount": 1100, "source_sequence": "1",
            "observed_at": "2026-07-20T06:00:01.123Z",
            "ingested_at": "2026-07-20T06:00:02.456Z",
            "raw_artifact_id": "artifact-old",
        }
        self.repository.insert_raw_bars([
            {**base, "source": "alpha"},
            {**base, "source": "alpha", "close": 21,
             "ingested_at": "2026-07-20T06:00:03Z",
             "raw_artifact_id": "artifact-new"},
            {**base, "source": "beta", "source_sequence": "2"},
            {**base, "source": "alpha", "bar_time": "2026-07-20T06:01:00Z",
             "source_sequence": "3"},
            {**base, "source": "wrong", "interval": "5m"},
        ])
        bars, truncated = self.repository.get_raw_price_bars_range(
            "RAW.HK", "1m", "raw", "2026-07-20T14:00:00+08:00",
            "2026-07-20T06:01:00Z", 2,
        )
        self.assertEqual([(bar["timestamp"], bar["source"]) for bar in bars], [
            (1784527200, "alpha"), (1784527200, "beta")
        ])
        self.assertEqual(bars[0]["close"], 21.0)
        self.assertEqual(bars[0]["raw_artifact_id"], "artifact-new")
        self.assertEqual(bars[0]["source_sequence"], "1")
        self.assertEqual(bars[0]["observed_at"], "2026-07-20T06:00:01.123000+00:00")
        self.assertEqual(bars[1]["ingested_at"], "2026-07-20T06:00:02.456000+00:00")
        self.assertTrue(truncated)
        filtered, truncated = self.repository.get_raw_price_bars_range(
            "RAW.HK", "1m", "raw", "2026-07-20T06:00:00Z",
            "2026-07-20T06:01:00Z", 10, ["beta", "beta"],
        )
        self.assertEqual([bar["source"] for bar in filtered], ["beta"])
        self.assertFalse(truncated)
        with self.assertRaisesRegex(ValueError, "include a timezone"):
            self.repository.get_raw_price_bars_range(
                "RAW.HK", "1m", "raw", "2026-07-20T06:00:00",
                "2026-07-20T06:01:00Z", 10,
            )

    def test_real_shadow_dual_write_replay_and_reconciliation(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            warehouse = Warehouse(root / "warehouse.duckdb")
            writer = ReliableClickHouseWriter(
                self.repository, LocalClickHouseSpool(root / "spool", root), 1000
            )
            adapter = ShadowMarketBarRepository(warehouse, writer)
            fixture_bars = [{
                "timestamp": 100, "bar_at": "1970-01-01T00:01:40Z",
                "open": 20, "high": 22, "low": 19, "close": 21,
                "volume": 200, "amount": 4200,
            }]
            provenance = {"observed_at": "2026-07-20T02:00:01Z",
                          "raw_artifact_id": "shadow-artifact"}
            self.assertEqual(adapter.upsert_price_bars(
                "000777.SZ", "1m", "raw", "shadow_fixture",
                "2026-07-20T02:00:02Z", fixture_bars, provenance,
            ), 1)
            reconciliation = adapter.reconcile_last_write()
            self.assertEqual(reconciliation["status"], "consistent", reconciliation)

            original = self.database.client
            self.database.client = None
            try:
                self.assertEqual(adapter.upsert_price_bars(
                    "000778.SZ", "1m", "raw", "shadow_fixture",
                    "2026-07-20T02:00:02Z", fixture_bars, provenance,
                ), 1)
            finally:
                self.database.client = original
            self.assertEqual(adapter.diagnostics()["shadow"]["status"], "spooled")
            self.assertEqual(writer.replay(), {"attempted": 1, "replayed": 1, "failed": 0})
            reconciliation = adapter.reconcile_last_write()
            self.assertEqual(reconciliation["status"], "consistent", reconciliation)

            changed = adapter._raw_rows(
                "000778.SZ", "1m", "raw", "shadow_fixture",
                "2026-07-20T02:00:03Z", fixture_bars, provenance,
            )
            changed[0]["close"] = 99.0
            self.repository.insert_raw_bars(changed, batch_id="intentional-mismatch")
            mismatch = adapter.reconcile_last_write()
            self.assertEqual(mismatch["status"], "mismatch")
            self.assertEqual(mismatch["mismatch_count"], 1)


if __name__ == "__main__":
    unittest.main()
