import os
import json
from decimal import Decimal
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect
from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.clickhouse_repositories import (
    CLICKHOUSE_MIGRATIONS,
    ClickHouseDatabase,
    ClickHouseMarketBarRepository,
)
from marketcow.repositories import MarketBarRepository
from marketcow.clickhouse_shadow import ShadowMarketBarRepository
from marketcow.clickhouse_canonical import CanonicalMarketBarBuilder
from marketcow.clickhouse_writer import LocalClickHouseSpool, ReliableClickHouseWriter
from marketcow.storage import Warehouse
from marketcow.clickhouse_writer import normalize_bar
from marketcow.config import Settings
from marketcow.contract_gate import LEGACY_PAYLOAD_PATHS, assert_contract_equal
from marketcow.clickhouse_scheduler import BackgroundCanonicalScheduler
from marketcow.local_backup import BackupComponent


class MarketBarService:
    def __init__(self, repository):
        self.market_bar_repository = repository

    def close(self):
        pass


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
    def test_backup_component_extracts_real_clickhouse_schema(self):
        component = BackupComponent.clickhouse(
            self.database, "2026-07-20T00:00:00Z"
        )
        payload = json.loads(component.files["logical.json"])
        self.assertIn("schema_migrations", payload)
        self.assertIn("market_bar_raw", payload)
        self.assertTrue(component.canonical_rebuildable)

    def test_background_scheduler_clickhouse_outage_recovery(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            scheduler_database = ClickHouseDatabase(
                self.host, self.port, self.database_name, self.username, self.password
            )
            scheduler_database.open()
            scheduler_repository = ClickHouseMarketBarRepository(scheduler_database)
            writer = ReliableClickHouseWriter(scheduler_repository, spool, 1000)
            builder = CanonicalMarketBarBuilder(scheduler_repository, writer)
            raw = normalize_bar("raw", {
                "symbol": "SCHEDULER-RECOVERY", "market": "US", "interval": "1m",
                "adjustment": "raw", "bar_time": "2026-07-20T07:00:00Z",
                "open": 10, "high": 11, "low": 9, "close": 10.5,
                "raw_close": 10.5, "adjustment_factor": 1, "volume": 100,
                "amount": 1050, "source": "fixture", "source_sequence": "1",
                "observed_at": "2026-07-20T07:00:01Z",
                "ingested_at": "2026-07-20T07:00:02Z", "raw_artifact_id": "sched-1",
            })
            self.repository.insert_raw_bars([raw])
            original = scheduler_database.client
            scheduler_database.client = None
            scheduler = BackgroundCanonicalScheduler(
                builder, spool, poll_seconds=0.05, backoff_base_seconds=0.05,
                backoff_max_seconds=0.1, max_attempts=5,
            )
            try:
                scheduler.enqueue_rows([raw])
                deadline = __import__("time").monotonic() + 2
                while scheduler.diagnostics()["last"].get("status") != "failed":
                    self.assertLess(__import__("time").monotonic(), deadline)
                    __import__("time").sleep(0.01)
                self.assertEqual(scheduler.diagnostics()["pending"], 1)
                scheduler_database.client = original
                deadline = __import__("time").monotonic() + 3
                while scheduler.diagnostics()["last"].get("status") != "ok":
                    self.assertLess(__import__("time").monotonic(), deadline)
                    __import__("time").sleep(0.01)
                rows, truncated = self.repository.query_range(
                    "canonical", "SCHEDULER-RECOVERY", "1m", "raw",
                    "2026-07-20T07:00:00Z", "2026-07-20T07:00:00Z", 10,
                )
                self.assertFalse(truncated)
                self.assertEqual(len(rows), 1)
            finally:
                scheduler_database.client = original
                scheduler.close()
                scheduler_database.close()

    def test_sv2_contract_gate_all_query_types_and_fallback(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            warehouse = Warehouse(root / "warehouse.duckdb")
            writer = ReliableClickHouseWriter(
                self.repository, LocalClickHouseSpool(root / "spool", root), 1000
            )
            builder = CanonicalMarketBarBuilder(self.repository, writer)
            raw_rows = []
            for symbol, close in (("GATE-A", 10.25), ("GATE-B", 20.5)):
                bars = []
                for timestamp in (1784527200, 1784527260):
                    bar = {
                        "timestamp": timestamp,
                        "bar_at": datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
                        "open": close - 1, "high": close + 1, "low": close - 2,
                        "close": close, "raw_close": close,
                        "adjustment_factor": 1.0, "volume": 0, "amount": close * 10,
                    }
                    bars.append(bar)
                    raw_rows.append(normalize_bar("raw", {
                        "symbol": symbol, "market": "US", "interval": "1m",
                        "adjustment": "raw", "bar_time": bar["bar_at"], **bar,
                        "volume": 0,
                        "source": "tushare", "source_sequence": str(timestamp),
                        "observed_at": "2026-07-20T12:01:00.456Z",
                        "ingested_at": "2026-07-20T12:02:00.123Z",
                        "raw_artifact_id": f"gate-{symbol}",
                    }))
                warehouse.upsert_price_bars(
                    symbol, "1m", "raw", "tushare",
                    "2026-07-20T12:02:00.123Z", bars,
                    {"observed_at": "2026-07-20T12:01:00.456Z",
                     "raw_artifact_id": f"gate-{symbol}"},
                )
            self.repository.insert_raw_bars(raw_rows)
            for symbol in ("GATE-A", "GATE-B"):
                outcome = builder.rebuild(
                    symbol, "1m", "raw", "2026-07-20T06:00:00Z",
                    "2026-07-20T06:01:00Z", 100,
                )
                self.assertEqual(outcome["status"], "ok")
            adapter = ShadowMarketBarRepository(
                warehouse, writer, builder, canonical_reads_enabled=True,
                raw_reads_enabled=True,
            )
            cases = {
                "recent": ("get_price_bars", ("GATE-A", "1m", "raw", 2)),
                "range": ("get_price_bars_range", ("GATE-A", "1m", "raw", "2026-07-20T06:00:00Z", "2026-07-20T06:01:00Z", 2)),
                "canonical_page": ("get_price_bars_page", ("GATE-A", "1m", "raw", "2026-07-20T06:00:00Z", "2026-07-20T06:01:00Z", 1, None)),
                "exact_cross_section_page": ("get_price_bars_cross_section_page", ("1m", "raw", "2026-07-20T06:00:00Z", 1, ["GATE-A", "GATE-B"], None)),
                "matrix": ("get_price_bars_matrix_page", ("1m", "raw", ["2026-07-20T06:00:00Z", "2026-07-20T06:01:00Z"], ["GATE-A", "GATE-B"], 3, None)),
                "raw_range": ("get_raw_price_bars_range", ("GATE-A", "1m", "raw", "2026-07-20T06:00:00Z", "2026-07-20T06:01:00Z", 10, None)),
                "raw_page": ("get_raw_price_bars_page", ("GATE-A", "1m", "raw", "2026-07-20T06:00:00Z", "2026-07-20T06:01:00Z", 1, None, None)),
                "single_as_of": ("get_price_bar_as_of", ("GATE-A", "1m", "raw", "2026-07-20T06:01:30Z", 100)),
                "cross_section_as_of": ("get_price_bars_as_of_page", ("1m", "raw", "2026-07-20T06:01:30Z", 100, ["GATE-A", "GATE-B"], 2, None)),
            }
            expected = {
                label: getattr(warehouse, method)(*arguments)
                for label, (method, arguments) in cases.items()
            }
            for label, (method, arguments) in cases.items():
                assert_contract_equal(
                    expected[label], getattr(adapter, method)(*arguments), label,
                    LEGACY_PAYLOAD_PATHS
                    if label in {"recent", "range", "raw_range", "raw_page"} else (),
                )
            settings = Settings(
                root / "warehouse.duckdb", root / "raw", storage_root=root / "development",
                market_bar_cursor_secret="real-contract-gate-secret-1234567890abcdef",
            )
            now = lambda: datetime(2026, 7, 20, 12, 5, tzinfo=timezone.utc)
            paths = {
                "recent": "/v1/quotes/GATE-A/history?refresh=false&limit=2",
                "range": "/v1/quotes/GATE-A/history?start=2026-07-20T06:00:00Z&end=2026-07-20T06:01:00Z&limit=2",
                "canonical_page": "/v1/quotes/GATE-A/history?start=2026-07-20T06:00:00Z&end=2026-07-20T06:01:00Z&page_size=1",
                "exact_cross_section_page": "/v1/quotes/cross-section?bar_at=2026-07-20T06:00:00Z&symbols=GATE-A,GATE-B&page_size=1",
                "matrix": "/v1/quotes/cross-section/matrix?bar_ats=2026-07-20T06:00:00Z,2026-07-20T06:01:00Z&symbols=GATE-A,GATE-B&page_size=3",
                "raw_page": "/v1/quotes/GATE-A/raw-history?start=2026-07-20T06:00:00Z&end=2026-07-20T06:01:00Z&page_size=1",
                "single_as_of": "/v1/quotes/GATE-A/as-of?as_of=2026-07-20T06:01:30Z&max_lookback_seconds=100",
                "cross_section_as_of": "/v1/quotes/cross-section/as-of?as_of=2026-07-20T06:01:30Z&max_lookback_seconds=100&symbols=GATE-A,GATE-B&page_size=2",
            }
            with TestClient(create_app(settings, MarketBarService(warehouse), now)) as direct_client:
                with TestClient(create_app(settings, MarketBarService(adapter), now)) as click_client:
                    api_expected = {label: direct_client.get(path).json()
                                    for label, path in paths.items()}
                    for label, path in paths.items():
                        assert_contract_equal(
                            api_expected[label], click_client.get(path).json(), label + " api",
                            LEGACY_PAYLOAD_PATHS
                            if label in {"recent", "range", "raw_page"} else (),
                        )
            original = self.repository.database.client
            self.repository.database.client = None
            try:
                for label, (method, arguments) in cases.items():
                    assert_contract_equal(
                        expected[label], getattr(adapter, method)(*arguments),
                        label + " fallback",
                        LEGACY_PAYLOAD_PATHS
                        if label in {"recent", "range", "raw_range", "raw_page"} else (),
                    )
                with TestClient(create_app(settings, MarketBarService(adapter), now)) as fallback_client:
                    for label, path in paths.items():
                        assert_contract_equal(
                            api_expected[label], fallback_client.get(path).json(),
                            label + " api fallback", LEGACY_PAYLOAD_PATHS
                            if label in {"recent", "range", "raw_page"} else (),
                        )
            finally:
                self.repository.database.client = original

    def test_canonical_keyset_multisource_matches_duckdb_fallback(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            warehouse = Warehouse(root / "warehouse.duckdb")
            writer = ReliableClickHouseWriter(
                self.repository, LocalClickHouseSpool(root / "spool", root), 1000
            )
            builder = CanonicalMarketBarBuilder(self.repository, writer)
            for timestamp in (1784527200, 1784527260):
                bar = {
                    "timestamp": timestamp,
                    "bar_at": datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
                    "open": 80, "high": 90, "low": 70,
                    "close": 88, "volume": 100, "amount": 8800,
                }
                warehouse.upsert_price_bars(
                    "PRIORITY.HK", "1m", "raw", "tushare",
                    "2026-07-20T06:00:00Z", [bar],
                    {"observed_at": "2026-07-20T05:00:00Z",
                     "raw_artifact_id": "tushare-priority"},
                )
                warehouse.upsert_price_bars(
                    "PRIORITY.HK", "1m", "raw", "yahoo_chart",
                    "2026-07-20T07:00:00Z", [{**bar, "close": 99}],
                    {"observed_at": "2026-07-20T06:30:00Z",
                     "raw_artifact_id": "yahoo-newer"},
                )
                bar_time = warehouse.get_price_bars_page(
                    "PRIORITY.HK", "1m", "raw", "2026-07-20T06:00:00Z",
                    "2026-07-20T07:00:00Z", 10,
                )[0][-1]["bar_at"]
                self.repository.insert_raw_bars([
                    {
                        "symbol": "PRIORITY.HK", "market": "HK", "interval": "1m",
                        "adjustment": "raw", "bar_time": bar_time, **bar,
                        "source": "tushare", "source_sequence": str(timestamp),
                        "observed_at": "2026-07-20T05:00:00Z",
                        "ingested_at": "2026-07-20T06:00:00Z",
                        "raw_artifact_id": "tushare-priority",
                    },
                    {
                        "symbol": "PRIORITY.HK", "market": "HK", "interval": "1m",
                        "adjustment": "raw", "bar_time": bar_time, **bar, "close": 99,
                        "source": "yahoo_chart", "source_sequence": str(timestamp),
                        "observed_at": "2026-07-20T06:30:00Z",
                        "ingested_at": "2026-07-20T07:00:00Z",
                        "raw_artifact_id": "yahoo-newer",
                    },
                ])
            arguments = (
                "PRIORITY.HK", "1m", "raw", "2026-07-20T06:00:00Z",
                "2026-07-20T07:00:00Z", 1, None,
            )
            self.assertEqual(builder.rebuild(*arguments[:5], 100)["status"], "ok")
            adapter = ShadowMarketBarRepository(
                warehouse, writer, builder, canonical_reads_enabled=True
            )
            clickhouse_page = adapter.get_price_bars_page(*arguments)
            self.assertTrue(clickhouse_page[1])
            self.assertEqual(clickhouse_page[0][0]["source"], "tushare")
            self.assertEqual(clickhouse_page[0][0]["close"], 88.0)
            original = self.repository.database.client
            self.repository.database.client = None
            try:
                fallback_page = adapter.get_price_bars_page(*arguments)
            finally:
                self.repository.database.client = original
            self.assertEqual(fallback_page, clickhouse_page)

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
        self.assertEqual(versions, [(1,), (2,), (3,), (4,), (5,)])

    def test_upgrade_from_migration_four_and_repeat_migrate(self):
        name = "marketcow_upgrade_" + uuid.uuid4().hex[:10] + "_test"
        bootstrap = clickhouse_connect.get_client(
            host=self.host, port=self.port, username=self.username,
            password=self.password, database="default",
        )
        upgraded = None
        try:
            bootstrap.command(f"CREATE DATABASE `{name}`")
            legacy = ClickHouseDatabase(
                self.host, self.port, name, self.username, self.password
            )
            legacy.client = legacy._connect(name)
            legacy.client.command(
                "CREATE TABLE schema_migrations (version UInt32, description String, "
                "applied_at DateTime64(3, 'UTC') DEFAULT now64(3)) "
                "ENGINE = MergeTree ORDER BY version"
            )
            for version, description, statements in CLICKHOUSE_MIGRATIONS[:4]:
                for statement in statements:
                    legacy.client.command(statement)
                legacy.client.insert(
                    "schema_migrations", [[version, description]],
                    column_names=["version", "description"],
                )
            legacy.close()
            upgraded = ClickHouseDatabase(
                self.host, self.port, name, self.username, self.password
            )
            upgraded.open()
            upgraded.migrate()
            versions = upgraded.client.query(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).result_rows
            self.assertEqual(versions, [(1,), (2,), (3,), (4,), (5,)])
            self.assertEqual(upgraded.client.query(
                "SELECT count() FROM system.tables WHERE database=currentDatabase() "
                "AND name='market_quote_latest'"
            ).result_rows[0][0], 1)
        finally:
            if upgraded is not None:
                upgraded.close()
            bootstrap.command(f"DROP DATABASE IF EXISTS `{name}`")
            bootstrap.close()

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

    def test_direct_market_bar_repository_complete_contract(self):
        self.assertIsInstance(self.repository, MarketBarRepository)
        point = "2026-07-20T08:00:00Z"
        timestamp = int(datetime.fromisoformat(point.replace("Z", "+00:00")).timestamp())
        self.assertEqual(self.repository.upsert_price_bars(
            "DIRECT.HK", "1m", "raw", "tushare", "2026-07-20T08:00:02.456Z",
            [{"timestamp": timestamp, "bar_at": point, "open": Decimal("10.0"),
              "high": Decimal("12.0"), "low": Decimal("9.0"),
              "close": Decimal("11.0"), "raw_close": None,
              "adjustment_factor": None, "volume": 100, "amount": None}],
            {"observed_at": "2026-07-20T08:00:01.123Z",
             "raw_artifact_id": "direct-artifact"},
        ), 1)
        self.repository.insert_canonical_bars([{
            "symbol": "DIRECT.HK", "market": "HK", "interval": "1m",
            "adjustment": "raw", "bar_time": point, "open": 10, "high": 12,
            "low": 9, "close": 11, "raw_close": None,
            "adjustment_factor": None, "volume": 100, "amount": None,
            "selected_source": "tushare", "source_count": 1,
            "quality_status": "single_source", "input_fingerprint": "direct-v1",
            "version": 1, "observed_at": "2026-07-20T08:00:01.123Z",
            "ingested_at": "2026-07-20T08:00:02.456Z",
            "raw_artifact_id": "direct-artifact",
            "updated_at": "2026-07-20T08:00:03Z",
        }])
        quote = {
            "symbol": "DIRECT.HK", "price": 11.0, "source": "tushare",
            "observed_at": "2026-07-20T08:00:01.123Z",
            "ingested_at": "2026-07-20T08:00:02.456Z", "amount": None,
        }
        self.repository.upsert_quote(quote)
        self.repository.upsert_quote(dict(reversed(list(quote.items()))))
        self.assertEqual(self.repository.get_latest_quotes(["DIRECT.HK"]), [quote])
        self.assertEqual(len(self.repository.get_price_bars(
            "DIRECT.HK", "1m", "raw", 10
        )), 1)
        arguments = ("DIRECT.HK", "1m", "raw", point, point)
        self.assertEqual(len(self.repository.get_price_bars_range(*arguments, 10)[0]), 1)
        self.assertEqual(len(self.repository.get_price_bars_page(*arguments, 10)[0]), 1)
        self.assertEqual(len(self.repository.get_price_bars_cross_section(
            "1m", "raw", point, 10, ["DIRECT.HK"]
        )[0]), 1)
        self.assertEqual(len(self.repository.get_price_bars_cross_section_page(
            "1m", "raw", point, 10, ["DIRECT.HK"]
        )[0]), 1)
        self.assertEqual(len(self.repository.get_price_bars_matrix_page(
            "1m", "raw", [point], ["DIRECT.HK"], 10
        )[0]), 1)
        self.assertEqual(self.repository.get_price_bar_as_of(
            "DIRECT.HK", "1m", "raw", point, 60
        )["timestamp"], timestamp)
        self.assertEqual(len(self.repository.get_price_bars_as_of_page(
            "1m", "raw", point, 60, ["DIRECT.HK"], 10
        )[0]), 1)
        raw_range = self.repository.get_raw_price_bars_range(*arguments, 10)
        raw_page = self.repository.get_raw_price_bars_page(*arguments, 10)
        self.assertEqual(raw_range, raw_page)
        self.assertEqual(raw_range[0][0]["observed_at"],
                         "2026-07-20T08:00:01.123000+00:00")
        self.assertIsNone(raw_range[0][0]["amount"])

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
            replayed = writer.replay()
            self.assertEqual({key: replayed[key] for key in ("attempted", "replayed", "failed")},
                             {"attempted": 1, "replayed": 1, "failed": 0})

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
            replayed = writer.replay()
            self.assertEqual({key: replayed[key] for key in ("attempted", "replayed", "failed")},
                             {"attempted": 1, "replayed": 1, "failed": 0})

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
        first_page, more = self.repository.get_canonical_price_bars_page(
            "HISTORY.HK", "1m", "raw", "2026-07-20T12:00:00+08:00",
            "2026-07-20T04:02:00Z", 1,
        )
        self.assertTrue(more)
        self.assertEqual(first_page[0]["bar_at"], "2026-07-20T04:00:00+00:00")
        second_page, more = self.repository.get_canonical_price_bars_page(
            "HISTORY.HK", "1m", "raw", "2026-07-20T04:00:00Z",
            "2026-07-20T04:02:00Z", 1, first_page[0]["timestamp"],
        )
        self.assertTrue(more)
        self.assertEqual(second_page[0]["close"], 13.0)
        self.assertEqual(second_page[0]["source"], "fixture")
        last_page, more = self.repository.get_canonical_price_bars_page(
            "HISTORY.HK", "1m", "raw", "2026-07-20T04:00:00Z",
            "2026-07-20T04:02:00Z", 1, second_page[0]["timestamp"],
        )
        self.assertFalse(more)
        self.assertEqual(last_page[0]["close"], 14.0)
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

    def test_canonical_cross_section_keyset_page_matches_duckdb_fallback(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            warehouse = Warehouse(root / "warehouse.duckdb")
            writer = ReliableClickHouseWriter(
                self.repository, LocalClickHouseSpool(root / "spool", root), 1000
            )
            base = {
                "market": "US", "interval": "1m", "adjustment": "raw",
                "bar_time": "2026-07-20T05:30:00Z", "open": 10, "high": 12,
                "low": 9, "close": 11, "raw_close": 22,
                "adjustment_factor": 0.5, "volume": 100, "amount": 1100,
                "selected_source": "tushare", "source_count": 1,
                "quality_status": "single_source",
                "observed_at": "2026-07-20T05:30:01Z",
                "ingested_at": "2026-07-20T05:30:02Z",
                "raw_artifact_id": "cross-page-artifact",
                "updated_at": "2026-07-20T05:30:03Z",
            }
            canonical = []
            for symbol in ("PAGE-A", "PAGE-B", "PAGE-C"):
                close = 22 if symbol == "PAGE-B" else 11
                warehouse.upsert_price_bars(
                    symbol, "1m", "raw", "tushare", "2026-07-20T05:30:02Z",
                    [{"timestamp": 1784525400, "bar_at": base["bar_time"],
                      "open": 10, "high": 12, "low": 9, "close": close,
                      "raw_close": 22, "adjustment_factor": 0.5,
                      "volume": 100, "amount": 1100}],
                    {"observed_at": base["observed_at"],
                     "raw_artifact_id": base["raw_artifact_id"]},
                )
                canonical.append({
                    **base, "symbol": symbol, "close": close, "version": 1,
                    "input_fingerprint": symbol + "-v1",
                })
            canonical.append({
                **base, "symbol": "PAGE-B", "close": 22, "version": 2,
                "input_fingerprint": "PAGE-B-v2",
            })
            self.repository.insert_canonical_bars(canonical)
            adapter = ShadowMarketBarRepository(
                warehouse, writer, canonical_reads_enabled=True
            )
            arguments = (
                "1m", "raw", "2026-07-20T13:30:00+08:00", 1,
                ["PAGE-C", "PAGE-B", "PAGE-A", "PAGE-A"], None,
            )
            clickhouse_page = adapter.get_price_bars_cross_section_page(*arguments)
            duckdb_page = warehouse.get_price_bars_cross_section_page(*arguments)
            self.assertEqual(clickhouse_page, duckdb_page)
            self.assertTrue(clickhouse_page[1])
            after = clickhouse_page[0][-1]["symbol"]
            next_arguments = (*arguments[:-1], after)
            self.assertEqual(
                adapter.get_price_bars_cross_section_page(*next_arguments),
                warehouse.get_price_bars_cross_section_page(*next_arguments),
            )

            settings = Settings(
                root / "warehouse.duckdb", root / "raw",
                market_bar_cursor_secret="clickhouse-cross-page-secret-1234567890",
                storage_root=root / "data-development",
            )
            path = (
                "/v1/quotes/cross-section?interval=1m&adjustment=raw"
                "&bar_at=2026-07-20T13:30:00%2B08:00&page_size=1"
                "&symbols=PAGE-C,PAGE-B,PAGE-A,PAGE-A"
            )
            app = create_app(
                settings, MarketBarService(adapter),
                lambda: datetime(2026, 7, 20, 6, 0, tzinfo=timezone.utc),
            )
            with TestClient(app) as client:
                clickhouse_payload = client.get(path).json()

            original = self.repository.database.client
            self.repository.database.client = None
            try:
                fallback_page = adapter.get_price_bars_cross_section_page(*arguments)
                with TestClient(app) as client:
                    fallback_payload = client.get(path).json()
            finally:
                self.repository.database.client = original
            self.assertEqual(fallback_page, clickhouse_page)
            self.assertEqual(fallback_payload, clickhouse_payload)
            self.assertTrue(adapter.diagnostics()["read"]["fallback"])

    def test_canonical_matrix_keyset_page_matches_duckdb_fallback(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            warehouse = Warehouse(root / "warehouse.duckdb")
            writer = ReliableClickHouseWriter(
                self.repository, LocalClickHouseSpool(root / "spool", root), 1000
            )
            points = ("2026-07-20T05:40:00Z", "2026-07-20T05:41:00Z")
            canonical = []
            for point in points:
                timestamp = int(datetime.fromisoformat(
                    point.replace("Z", "+00:00")
                ).timestamp())
                for symbol in ("MATRIX-A", "MATRIX-B", "MATRIX-C"):
                    if point == points[1] and symbol == "MATRIX-C":
                        continue
                    close = 22 if point == points[0] and symbol == "MATRIX-B" else 11
                    warehouse.upsert_price_bars(
                        symbol, "1m", "raw", "tushare", "2026-07-20T05:42:02Z",
                        [{"timestamp": timestamp, "bar_at": point, "open": 10,
                          "high": 12, "low": 9, "close": close, "raw_close": 22,
                          "adjustment_factor": 0.5, "volume": 100, "amount": 1100}],
                        {"observed_at": "2026-07-20T05:42:01Z",
                         "raw_artifact_id": "matrix-artifact"},
                    )
                    canonical.append({
                        "symbol": symbol, "market": "US", "interval": "1m",
                        "adjustment": "raw", "bar_time": point, "open": 10,
                        "high": 12, "low": 9, "close": close, "raw_close": 22,
                        "adjustment_factor": 0.5, "volume": 100, "amount": 1100,
                        "selected_source": "tushare", "source_count": 1,
                        "quality_status": "single_source",
                        "observed_at": "2026-07-20T05:42:01Z",
                        "ingested_at": "2026-07-20T05:42:02Z",
                        "raw_artifact_id": "matrix-artifact", "version": 1,
                        "input_fingerprint": f"{point}-{symbol}-v1",
                        "updated_at": "2026-07-20T05:42:03Z",
                    })
            canonical.append({
                **next(row for row in canonical if row["symbol"] == "MATRIX-B"),
                "close": 22, "version": 2, "input_fingerprint": "matrix-b-v2",
            })
            self.repository.insert_canonical_bars(canonical)
            adapter = ShadowMarketBarRepository(
                warehouse, writer, canonical_reads_enabled=True
            )
            arguments = (
                "1m", "raw", ["2026-07-20T13:41:00+08:00", points[0]],
                ["MATRIX-C", "MATRIX-B", "MATRIX-A", "MATRIX-A"], 2, None,
            )
            clickhouse_page = adapter.get_price_bars_matrix_page(*arguments)
            duckdb_page = warehouse.get_price_bars_matrix_page(*arguments)
            self.assertEqual(clickhouse_page, duckdb_page)
            self.assertTrue(clickhouse_page[1])
            after = (clickhouse_page[0][-1]["timestamp"],
                     clickhouse_page[0][-1]["symbol"])
            next_arguments = (*arguments[:-1], after)
            self.assertEqual(adapter.get_price_bars_matrix_page(*next_arguments),
                             warehouse.get_price_bars_matrix_page(*next_arguments))

            settings = Settings(
                root / "warehouse.duckdb", root / "raw",
                market_bar_cursor_secret="clickhouse-matrix-secret-1234567890abcdef",
                storage_root=root / "data-development",
            )
            path = (
                "/v1/quotes/cross-section/matrix?interval=1m&adjustment=raw"
                "&bar_ats=2026-07-20T13:41:00%2B08:00,2026-07-20T05:40:00Z"
                "&symbols=MATRIX-C,MATRIX-B,MATRIX-A,MATRIX-A&page_size=2"
            )
            app = create_app(
                settings, MarketBarService(adapter),
                lambda: datetime(2026, 7, 20, 6, 0, tzinfo=timezone.utc),
            )
            with TestClient(app) as client:
                clickhouse_payload = client.get(path).json()
            original = self.repository.database.client
            self.repository.database.client = None
            try:
                fallback_page = adapter.get_price_bars_matrix_page(*arguments)
                with TestClient(app) as client:
                    fallback_payload = client.get(path).json()
            finally:
                self.repository.database.client = original
            self.assertEqual(fallback_page, clickhouse_page)
            self.assertEqual(fallback_payload, clickhouse_payload)
            self.assertTrue(adapter.diagnostics()["read"]["fallback"])

    def test_canonical_as_of_matches_duckdb_and_api_fallback(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            warehouse = Warehouse(root / "warehouse.duckdb")
            writer = ReliableClickHouseWriter(
                self.repository, LocalClickHouseSpool(root / "spool", root), 1000
            )
            fixtures = {
                "ASOF-A": [(100, 11), (200, 22), (300, 33)],
                "ASOF-B": [(150, 15)],
                "ASOF-C": [(240, 24)],
            }
            canonical = []
            for symbol, values in fixtures.items():
                warehouse.upsert_price_bars(
                    symbol, "1m", "raw", "tushare", "2026-07-20T05:50:02Z",
                    [{"timestamp": timestamp,
                      "bar_at": datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
                      "open": close, "high": close + 1, "low": close - 1,
                      "close": close, "raw_close": close,
                      "adjustment_factor": 1.0, "volume": 100, "amount": 1000}
                     for timestamp, close in values],
                    {"observed_at": "2026-07-20T05:50:01Z",
                     "raw_artifact_id": "as-of-artifact"},
                )
                for timestamp, close in values:
                    point = datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
                    canonical.append({
                        "symbol": symbol, "market": "US", "interval": "1m",
                        "adjustment": "raw", "bar_time": point, "open": close,
                        "high": close + 1, "low": close - 1, "close": close,
                        "raw_close": close, "adjustment_factor": 1.0,
                        "volume": 100, "amount": 1000,
                        "selected_source": "tushare", "source_count": 1,
                        "quality_status": "single_source",
                        "observed_at": "2026-07-20T05:50:01Z",
                        "ingested_at": "2026-07-20T05:50:02Z",
                        "raw_artifact_id": "as-of-artifact", "version": 1,
                        "input_fingerprint": f"{symbol}-{timestamp}-v1",
                        "updated_at": "2026-07-20T05:50:03Z",
                    })
            versioned = next(
                row for row in canonical
                if row["symbol"] == "ASOF-A" and row["bar_time"].endswith("03:20+00:00")
            )
            canonical.append({
                **versioned, "version": 2, "input_fingerprint": "ASOF-A-200-v2"
            })
            self.repository.insert_canonical_bars(canonical)
            adapter = ShadowMarketBarRepository(
                warehouse, writer, canonical_reads_enabled=True
            )
            single = ("ASOF-A", "1m", "raw", "1970-01-01T08:04:10+08:00", 100)
            clickhouse_single = adapter.get_price_bar_as_of(*single)
            self.assertEqual(clickhouse_single, warehouse.get_price_bar_as_of(*single))
            self.assertEqual(clickhouse_single["timestamp"], 200)
            self.assertEqual(clickhouse_single["staleness_seconds"], 50)
            page = (
                "1m", "raw", "1970-01-01T00:04:10Z", 100,
                ["ASOF-C", "ASOF-B", "ASOF-A", "ASOF-A"], 1, None,
            )
            clickhouse_page = adapter.get_price_bars_as_of_page(*page)
            self.assertEqual(clickhouse_page, warehouse.get_price_bars_as_of_page(*page))
            self.assertTrue(clickhouse_page[1])

            settings = Settings(
                root / "warehouse.duckdb", root / "raw",
                market_bar_cursor_secret="clickhouse-as-of-secret-1234567890abcdef",
                storage_root=root / "data-development",
            )
            path = (
                "/v1/quotes/cross-section/as-of?interval=1m&adjustment=raw"
                "&as_of=1970-01-01T08:04:10%2B08:00&max_lookback_seconds=100"
                "&symbols=ASOF-C,ASOF-B,ASOF-A,ASOF-A&page_size=1"
            )
            single_path = (
                "/v1/quotes/ASOF-A/as-of?interval=1m&adjustment=raw"
                "&as_of=1970-01-01T08:04:10%2B08:00&max_lookback_seconds=100"
            )
            app = create_app(
                settings, MarketBarService(adapter),
                lambda: datetime(2026, 7, 20, 6, 0, tzinfo=timezone.utc),
            )
            with TestClient(app) as client:
                clickhouse_payload = client.get(path).json()
                clickhouse_single_payload = client.get(single_path).json()
            original = self.repository.database.client
            self.repository.database.client = None
            try:
                fallback_single = adapter.get_price_bar_as_of(*single)
                fallback_page = adapter.get_price_bars_as_of_page(*page)
                with TestClient(app) as client:
                    fallback_payload = client.get(path).json()
                    fallback_single_payload = client.get(single_path).json()
            finally:
                self.repository.database.client = original
            self.assertEqual(fallback_single, clickhouse_single)
            self.assertEqual(fallback_page, clickhouse_page)
            self.assertEqual(fallback_payload, clickhouse_payload)
            self.assertEqual(fallback_single_payload, clickhouse_single_payload)
            self.assertTrue(adapter.diagnostics()["read"]["fallback"])

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

    def test_raw_keyset_pages_match_duckdb_and_failure_fallback(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            warehouse = Warehouse(root / "warehouse.duckdb")
            writer = ReliableClickHouseWriter(
                self.repository, LocalClickHouseSpool(root / "spool", root), 1000
            )
            for source in ("alpha", "beta"):
                rows = []
                for timestamp in (1784527200, 1784527260):
                    row = {
                        "timestamp": timestamp,
                        "bar_at": datetime.fromtimestamp(
                            timestamp, timezone.utc
                        ).isoformat(),
                        "open": 10, "high": 12, "low": 9,
                        "close": 11 if source == "alpha" else 12,
                        "raw_close": 22, "adjustment_factor": 0.5,
                        "volume": 100, "amount": 1100,
                    }
                    rows.append(row)
                warehouse.upsert_price_bars(
                    "RAW-PAGE.HK", "1m", "raw", source,
                    "2026-07-20T06:00:02.456Z", rows,
                    {"observed_at": "2026-07-20T06:00:01.123Z",
                     "raw_artifact_id": f"artifact-{source}"},
                )
                self.repository.insert_raw_bars([
                    {
                        "symbol": "RAW-PAGE.HK", "market": "HK", "interval": "1m",
                        "adjustment": "raw", "bar_time": row["bar_at"], **row,
                        "source": source, "source_sequence": str(row["timestamp"]),
                        "observed_at": "2026-07-20T06:00:01.123Z",
                        "ingested_at": "2026-07-20T06:00:02.456Z",
                        "raw_artifact_id": f"artifact-{source}",
                    } for row in rows
                ])
            adapter = ShadowMarketBarRepository(
                warehouse, writer, raw_reads_enabled=True
            )
            arguments = (
                "RAW-PAGE.HK", "1m", "raw", "2026-07-20T14:00:00+08:00",
                "2026-07-20T06:01:00Z", 1, ["beta", "alpha", "alpha"], None,
            )
            clickhouse_page = adapter.get_raw_price_bars_page(*arguments)
            duckdb_page = warehouse.get_raw_price_bars_page(*arguments)
            self.assertEqual(clickhouse_page, duckdb_page)
            self.assertTrue(clickhouse_page[1])
            after = (clickhouse_page[0][-1]["timestamp"],
                     clickhouse_page[0][-1]["source"])
            next_arguments = (*arguments[:-1], after)
            self.assertEqual(adapter.get_raw_price_bars_page(*next_arguments),
                             warehouse.get_raw_price_bars_page(*next_arguments))

            original = self.repository.database.client
            self.repository.database.client = None
            try:
                fallback_page = adapter.get_raw_price_bars_page(*arguments)
            finally:
                self.repository.database.client = original
            self.assertEqual(fallback_page, clickhouse_page)
            self.assertTrue(adapter.diagnostics()["read"]["fallback"])

    def test_raw_equal_ingestion_winner_is_stable_across_insert_and_merge_order(self):
        base = {
            "market": "US", "interval": "1m", "adjustment": "raw",
            "bar_time": "2026-07-20T07:00:00Z", "open": 1, "high": 2,
            "low": 0.5, "raw_close": None, "adjustment_factor": None,
            "volume": 10, "amount": None, "source": "fixture",
            "source_sequence": "1", "observed_at": "2026-07-20T07:00:01.123Z",
            "ingested_at": "2026-07-20T07:00:02.456Z", "raw_artifact_id": None,
        }
        winners = []
        for symbol, closes in (("ORDER-A", (41, 42)), ("ORDER-B", (42, 41))):
            candidates = [normalize_bar("raw", {**base, "symbol": symbol, "close": close})
                          for close in closes]
            for candidate in candidates:
                self.repository.insert_raw_bars([candidate])
            self.database.client.command("OPTIMIZE TABLE market_bar_raw FINAL")
            bars, _ = self.repository.get_raw_price_bars_range(
                symbol, "1m", "raw", "2026-07-20T07:00:00Z",
                "2026-07-20T07:00:00Z", 10,
            )
            winners.append(bars[0]["close"])
        self.assertEqual(winners[0], winners[1])

    def test_real_shadow_dual_write_replay_and_reconciliation(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            warehouse = Warehouse(root / "warehouse.duckdb")
            writer = ReliableClickHouseWriter(
                self.repository, LocalClickHouseSpool(root / "spool", root), 1000
            )
            canonical = CanonicalMarketBarBuilder(
                self.repository, writer, ("shadow_fixture",)
            )
            adapter = ShadowMarketBarRepository(
                warehouse, writer, canonical,
                auto_canonical_enabled=True, auto_canonical_limit=100,
            )
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
            self.assertEqual(
                adapter.diagnostics()["shadow"]["status"], "durable_pending"
            )
            replayed = writer.replay()
            self.assertEqual({key: replayed[key] for key in ("attempted", "replayed", "failed")},
                             {"attempted": 1, "replayed": 1, "failed": 0})
            self.assertEqual(adapter.diagnostics()["auto_canonical"]["status"], "ok")
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
