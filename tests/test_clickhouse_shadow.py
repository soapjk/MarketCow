import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from marketcow.clickhouse_shadow import ShadowMarketBarRepository
from marketcow.clickhouse_writer import LocalClickHouseSpool, ReliableClickHouseWriter
from marketcow.config import Settings
from marketcow.duckdb_repositories import create_stage1_repositories
from marketcow.storage import Warehouse


def bars():
    return [{"timestamp": 1, "bar_at": "1970-01-01T00:00:01Z", "open": 9,
             "high": 11, "low": 8, "close": 10, "volume": 100, "amount": 1000}]


class FakePrimary:
    def __init__(self, fail=False):
        self.fail = fail
        self.writes = []

    def upsert_price_bars(self, *args):
        if self.fail:
            raise RuntimeError("primary failed")
        self.writes.append(args)
        return len(args[5])

    def get_latest_quotes(self, symbols):
        return [{"symbol": symbol} for symbol in symbols]

    def get_price_bars(self, *args):
        return ["duckdb-read"]

    def get_price_bars_range(self, *args):
        return (["duckdb-range"], True)

    def get_price_bars_page(self, *args):
        return (["duckdb-page"], True)

    def get_price_bars_cross_section(self, *args):
        return (["duckdb-cross-section"], True)

    def get_price_bars_cross_section_page(self, *args):
        return (["duckdb-cross-section-page"], True)

    def get_price_bars_matrix_page(self, *args):
        return (["duckdb-matrix-page"], True)

    def get_price_bar_as_of(self, *args):
        return {"symbol": "duckdb-as-of"}

    def get_price_bars_as_of_page(self, *args):
        return (["duckdb-as-of-page"], True)

    def get_raw_price_bars_range(self, *args):
        return (["duckdb-raw"], True)

    def get_raw_price_bars_page(self, *args):
        return (["duckdb-raw-page"], True)


class CapturingWriter:
    def __init__(self, result=None):
        self.calls = []
        self.result = result or {"rows": 1, "written": 1, "spooled": 0, "batches": 1}

    def write(self, dataset, rows):
        self.calls.append((dataset, rows))
        return self.result


class FakeSpool:
    def diagnostics(self):
        return {"pending": 0, "failed": 0, "replayed": 0,
                "oldest_pending_lag_seconds": 0}


class ReconcileRepository:
    def __init__(self, rows):
        self.rows = rows

    def query_raw_batch(self, *args):
        return self.rows

    def get_canonical_price_bars(self, *args):
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows

    def get_canonical_price_bars_range(self, *args):
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows, False

    def get_canonical_price_bars_page(self, *args):
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows, False

    def get_canonical_price_bars_cross_section(self, *args):
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows, False

    def get_canonical_price_bars_cross_section_page(self, *args):
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows, False

    def get_canonical_price_bars_matrix_page(self, *args):
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows, False

    def get_canonical_price_bar_as_of(self, *args):
        if isinstance(self.rows, Exception):
            raise self.rows
        return None if not self.rows else self.rows[0]

    def get_canonical_price_bars_as_of_page(self, *args):
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows, False

    def get_raw_price_bars_range(self, *args):
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows, False

    def get_raw_price_bars_page(self, *args):
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows, False


class FailingClickHouseRepository:
    def insert_raw_bars(self, rows, batch_id=""):
        raise ConnectionError("clickhouse unavailable")


class FakeCanonicalBuilder:
    def __init__(self):
        self.calls = []
        self.last_diagnostics = {"status": "not_run"}

    def rebuild(self, *args):
        self.calls.append(args)
        return {"status": "ok", "written": 1, "spooled": 0, "truncated": False}


class FakeBackgroundScheduler:
    def __init__(self, status="ok"):
        self.status = status
        self.calls = []

    def enqueue_rows(self, rows):
        self.calls.append(rows)
        return {"status": self.status, "accepted": int(self.status == "ok"),
                "duplicate": 0, "full": int(self.status != "ok")}

    def enqueue_replayed_rows(self, rows):
        outcome = self.enqueue_rows(rows)
        if outcome["full"]:
            raise RuntimeError("canonical scheduler queue is full")

    def diagnostics(self):
        return {"enabled": True, "pending": len(self.calls)}


class ShadowMarketBarRepositoryTest(unittest.TestCase):
    def test_background_enqueue_is_primary_fail_open_and_replay_hooked(self):
        primary, writer = FakePrimary(), CapturingWriter()
        writer.spool = FakeSpool()
        scheduler = FakeBackgroundScheduler("full")
        writer.on_raw_replayed = None
        adapter = ShadowMarketBarRepository(
            primary, writer, FakeCanonicalBuilder(), background_scheduler=scheduler
        )
        self.assertEqual(adapter.upsert_price_bars(
            "MU", "1m", "raw", "fixture", "2026-07-20T01:00:02Z", bars()
        ), 1)
        self.assertEqual(len(primary.writes), 1)
        self.assertEqual(adapter.diagnostics()["auto_canonical"]["status"], "full")
        scheduler.status = "ok"
        writer.on_raw_replayed([adapter._raw_rows(
            "MU", "1m", "raw", "fixture", "2026-07-20T01:00:03Z", bars(), {}
        )[0]])
        self.assertEqual(len(scheduler.calls), 2)

    def test_keyset_page_read_and_same_cursor_fallback(self):
        arguments = (
            "MU", "1m", "raw", "2026-07-20T00:00:00Z",
            "2026-07-20T01:00:00Z", 50, 1752969900,
        )
        primary = FakePrimary()
        writer = CapturingWriter()
        writer.repository = ReconcileRepository(["clickhouse-page"])
        writer.spool = FakeSpool()
        adapter = ShadowMarketBarRepository(
            primary, writer, canonical_reads_enabled=True
        )
        self.assertEqual(adapter.get_price_bars_page(*arguments),
                         (["clickhouse-page"], False))
        self.assertTrue(adapter.diagnostics()["read"]["keyset_page"])
        writer.repository.rows = ConnectionError("unavailable")
        self.assertEqual(adapter.get_price_bars_page(*arguments),
                         (["duckdb-page"], True))
        diagnostic = adapter.diagnostics()["read"]
        self.assertTrue(diagnostic["fallback"])
        self.assertEqual(diagnostic["backend"], "duckdb")
    def test_auto_canonical_only_after_success_and_raw_replay(self):
        primary, writer, builder = FakePrimary(), CapturingWriter(), FakeCanonicalBuilder()
        writer.on_raw_replayed = None
        adapter = ShadowMarketBarRepository(
            primary, writer, builder, auto_canonical_enabled=True,
            auto_canonical_limit=123,
        )
        adapter.upsert_price_bars(
            "MU", "1m", "raw", "fixture", "2026-07-20T01:00:02Z", bars()
        )
        self.assertEqual(builder.calls[0], (
            "MU", "1m", "raw", "1970-01-01T00:00:01.000+00:00",
            "1970-01-01T00:00:01.000+00:00", 123,
        ))
        writer.result = {"rows": 1, "written": 0, "spooled": 1, "batches": 1}
        adapter.upsert_price_bars(
            "MU", "1m", "raw", "fixture", "2026-07-20T01:00:03Z", bars()
        )
        self.assertEqual(len(builder.calls), 1)
        writer.on_raw_replayed([adapter._raw_rows(
            "MU", "1m", "raw", "fixture", "2026-07-20T01:00:03Z", bars(), {}
        )[0]])
        self.assertEqual(len(builder.calls), 2)
    def test_primary_first_mapping_and_reads_remain_primary(self):
        primary, writer = FakePrimary(), CapturingWriter()
        adapter = ShadowMarketBarRepository(primary, writer)
        count = adapter.upsert_price_bars(
            "600519.SH", "1m", "raw", "fixture", "2026-07-20T01:00:02Z",
            bars(), {"observed_at": "2026-07-20T01:00:01Z",
                     "raw_artifact_id": "artifact-1"},
        )
        self.assertEqual(count, 1)
        self.assertEqual(len(primary.writes), 1)
        self.assertEqual(writer.calls[0][0], "raw")
        mapped = writer.calls[0][1][0]
        self.assertEqual(mapped["market"], "CN")
        self.assertEqual(mapped["source"], "fixture")
        self.assertEqual(mapped["source_sequence"], "1")
        self.assertEqual(mapped["raw_artifact_id"], "artifact-1")
        self.assertEqual(adapter.get_price_bars("x", "1m", "raw", 1), ["duckdb-read"])
        self.assertEqual(adapter.get_latest_quotes(["x"]), [{"symbol": "x"}])

    def test_opt_in_canonical_read_and_bounded_failure_fallback(self):
        primary, writer = FakePrimary(), CapturingWriter()
        writer.repository = ReconcileRepository([{"timestamp": 1, "source": "canonical"}])
        writer.spool = FakeSpool()
        adapter = ShadowMarketBarRepository(
            primary, writer, canonical_reads_enabled=True
        )
        self.assertEqual(adapter.get_price_bars("x", "1m", "raw", 1),
                         [{"timestamp": 1, "source": "canonical"}])
        self.assertEqual(adapter.diagnostics()["read"]["backend"],
                         "clickhouse_canonical")
        writer.repository.rows = ConnectionError("x" * 5000)
        self.assertEqual(adapter.get_price_bars("x", "1m", "raw", 1),
                         ["duckdb-read"])
        diagnostics = adapter.diagnostics()["read"]
        self.assertTrue(diagnostics["fallback"])
        self.assertEqual(diagnostics["backend"], "duckdb")
        self.assertEqual(len(diagnostics["error"]), 4000)

    def test_range_read_reports_truncation_and_falls_back_same_range(self):
        primary, writer = FakePrimary(), CapturingWriter()
        writer.repository = ReconcileRepository([{"timestamp": 1}])
        writer.spool = FakeSpool()
        adapter = ShadowMarketBarRepository(
            primary, writer, canonical_reads_enabled=True
        )
        arguments = ("x", "1m", "raw", "2026-07-20T00:00:00Z",
                     "2026-07-20T01:00:00Z", 10)
        self.assertEqual(adapter.get_price_bars_range(*arguments),
                         ([{"timestamp": 1}], False))
        self.assertFalse(adapter.diagnostics()["read"]["truncated"])
        writer.repository.rows = ConnectionError("range unavailable")
        self.assertEqual(adapter.get_price_bars_range(*arguments),
                         (["duckdb-range"], True))
        diagnostics = adapter.diagnostics()["read"]
        self.assertTrue(diagnostics["fallback"])
        self.assertTrue(diagnostics["truncated"])
        self.assertTrue(diagnostics["range"])

    def test_cross_section_read_and_same_query_fallback_diagnostics(self):
        primary, writer = FakePrimary(), CapturingWriter()
        writer.repository = ReconcileRepository([{"symbol": "AAPL"}])
        writer.spool = FakeSpool()
        adapter = ShadowMarketBarRepository(
            primary, writer, canonical_reads_enabled=True
        )
        arguments = ("1m", "raw", "2026-07-20T01:00:00Z", 10, ["AAPL"])
        self.assertEqual(adapter.get_price_bars_cross_section(*arguments),
                         ([{"symbol": "AAPL"}], False))
        diagnostics = adapter.diagnostics()["read"]
        self.assertTrue(diagnostics["cross_section"])
        self.assertEqual(diagnostics["backend"], "clickhouse_canonical")
        writer.repository.rows = ConnectionError("cross section unavailable")
        self.assertEqual(adapter.get_price_bars_cross_section(*arguments),
                         (["duckdb-cross-section"], True))
        diagnostics = adapter.diagnostics()["read"]
        self.assertTrue(diagnostics["fallback"])
        self.assertTrue(diagnostics["truncated"])
        self.assertEqual(diagnostics["backend"], "duckdb")

    def test_cross_section_page_same_boundary_fallback_diagnostics(self):
        primary, writer = FakePrimary(), CapturingWriter()
        writer.repository = ReconcileRepository([{"symbol": "MSFT"}])
        writer.spool = FakeSpool()
        adapter = ShadowMarketBarRepository(
            primary, writer, canonical_reads_enabled=True
        )
        arguments = (
            "1m", "raw", "2026-07-20T01:00:00Z", 10,
            ["AAPL", "MSFT"], "AAPL",
        )
        self.assertEqual(adapter.get_price_bars_cross_section_page(*arguments),
                         ([{"symbol": "MSFT"}], False))
        diagnostics = adapter.diagnostics()["read"]
        self.assertTrue(diagnostics["cross_section"])
        self.assertTrue(diagnostics["keyset_page"])
        writer.repository.rows = ConnectionError("cross section page unavailable")
        self.assertEqual(adapter.get_price_bars_cross_section_page(*arguments),
                         (["duckdb-cross-section-page"], True))
        diagnostics = adapter.diagnostics()["read"]
        self.assertTrue(diagnostics["fallback"])
        self.assertTrue(diagnostics["truncated"])

    def test_matrix_page_same_boundary_fallback_diagnostics(self):
        primary, writer = FakePrimary(), CapturingWriter()
        writer.repository = ReconcileRepository([{"symbol": "MSFT", "timestamp": 2}])
        writer.spool = FakeSpool()
        adapter = ShadowMarketBarRepository(
            primary, writer, canonical_reads_enabled=True
        )
        arguments = (
            "1m", "raw", ["1970-01-01T00:00:01Z", "1970-01-01T00:00:02Z"],
            ["AAPL", "MSFT"], 10, (1, "AAPL"),
        )
        self.assertEqual(adapter.get_price_bars_matrix_page(*arguments),
                         ([{"symbol": "MSFT", "timestamp": 2}], False))
        diagnostics = adapter.diagnostics()["read"]
        self.assertTrue(diagnostics["matrix"])
        self.assertTrue(diagnostics["keyset_page"])
        writer.repository.rows = ConnectionError("matrix unavailable")
        self.assertEqual(adapter.get_price_bars_matrix_page(*arguments),
                         (["duckdb-matrix-page"], True))
        diagnostics = adapter.diagnostics()["read"]
        self.assertTrue(diagnostics["fallback"])
        self.assertTrue(diagnostics["matrix"])

    def test_as_of_single_and_page_bounded_fallback_diagnostics(self):
        primary, writer = FakePrimary(), CapturingWriter()
        writer.repository = ReconcileRepository([{"symbol": "AAPL"}])
        writer.spool = FakeSpool()
        adapter = ShadowMarketBarRepository(
            primary, writer, canonical_reads_enabled=True
        )
        single = ("AAPL", "1m", "raw", "1970-01-01T00:04:10Z", 100)
        self.assertEqual(adapter.get_price_bar_as_of(*single), {"symbol": "AAPL"})
        page = (
            "1m", "raw", "1970-01-01T00:04:10Z", 100,
            ["AAPL", "MSFT"], 10, "AAPL",
        )
        self.assertEqual(adapter.get_price_bars_as_of_page(*page),
                         ([{"symbol": "AAPL"}], False))
        writer.repository.rows = ConnectionError("as-of unavailable")
        self.assertEqual(adapter.get_price_bar_as_of(*single),
                         {"symbol": "duckdb-as-of"})
        self.assertEqual(adapter.get_price_bars_as_of_page(*page),
                         (["duckdb-as-of-page"], True))
        diagnostics = adapter.diagnostics()["read"]
        self.assertTrue(diagnostics["fallback"])
        self.assertTrue(diagnostics["as_of"])

    def test_raw_multisource_opt_in_and_same_query_fallback_diagnostics(self):
        primary, writer = FakePrimary(), CapturingWriter()
        writer.repository = ReconcileRepository([{"source": "fixture"}])
        writer.spool = FakeSpool()
        adapter = ShadowMarketBarRepository(primary, writer, raw_reads_enabled=True)
        arguments = ("x", "1m", "raw", "2026-07-20T00:00:00Z",
                     "2026-07-20T01:00:00Z", 10, ["fixture"])
        self.assertEqual(adapter.get_raw_price_bars_range(*arguments),
                         ([{"source": "fixture"}], False))
        diagnostics = adapter.diagnostics()["read"]
        self.assertEqual(diagnostics["backend"], "clickhouse_raw")
        self.assertTrue(diagnostics["raw_multisource"])
        writer.repository.rows = ConnectionError("raw unavailable")
        self.assertEqual(adapter.get_raw_price_bars_range(*arguments),
                         (["duckdb-raw"], True))
        diagnostics = adapter.diagnostics()["read"]
        self.assertTrue(diagnostics["fallback"])
        self.assertTrue(diagnostics["truncated"])

    def test_raw_keyset_page_uses_same_query_for_bounded_fallback(self):
        primary, writer = FakePrimary(), CapturingWriter()
        writer.repository = ReconcileRepository([{"source": "beta", "timestamp": 100}])
        writer.spool = FakeSpool()
        adapter = ShadowMarketBarRepository(primary, writer, raw_reads_enabled=True)
        arguments = (
            "AAPL", "1m", "raw", "1970-01-01T00:01:40Z",
            "1970-01-01T00:02:00Z", 1, ["alpha", "beta"], (100, "alpha"),
        )
        self.assertEqual(adapter.get_raw_price_bars_page(*arguments),
                         ([{"source": "beta", "timestamp": 100}], False))
        self.assertTrue(adapter.diagnostics()["read"]["keyset_page"])
        writer.repository.rows = ConnectionError("raw page unavailable")
        self.assertEqual(adapter.get_raw_price_bars_page(*arguments),
                         (["duckdb-raw-page"], True))
        diagnostics = adapter.diagnostics()["read"]
        self.assertTrue(diagnostics["fallback"])
        self.assertTrue(diagnostics["keyset_page"])

    def test_primary_failure_never_attempts_shadow(self):
        writer = CapturingWriter()
        adapter = ShadowMarketBarRepository(FakePrimary(True), writer)
        with self.assertRaisesRegex(RuntimeError, "primary failed"):
            adapter.upsert_price_bars(
                "MU", "1m", "raw", "fixture", "2026-07-20T01:00:02Z", bars()
            )
        self.assertEqual(writer.calls, [])

    def test_shadow_failure_is_fail_open_and_spooled(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            spool = LocalClickHouseSpool(root / "spool", root)
            writer = ReliableClickHouseWriter(FailingClickHouseRepository(), spool, 1000)
            adapter = ShadowMarketBarRepository(FakePrimary(), writer)
            count = adapter.upsert_price_bars(
                "MU", "1m", "raw", "fixture", "2026-07-20T01:00:02Z", bars()
            )
            self.assertEqual(count, 1)
            self.assertEqual(adapter.diagnostics()["shadow"]["status"], "spooled")
            self.assertEqual(adapter.diagnostics()["spool"]["pending"], 1)

    def test_disabled_factory_has_zero_clickhouse_side_effects(self):
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
            self.assertIs(repositories.market_bars, warehouse)
            self.assertIsNone(resources)
            get_client.assert_not_called()
            self.assertFalse(settings.clickhouse_spool_path.exists())

    def test_enabled_factory_closes_clickhouse_resource(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / "data-development"
            settings = Settings(
                root / "warehouse.duckdb", root / "raw", profile="development", port=8791,
                clickhouse_enabled=True, clickhouse_database="marketcow_test",
                clickhouse_spool_path=root / "spool/clickhouse", storage_root=root,
            )
            warehouse = Warehouse(settings.database_path)
            with patch("marketcow.clickhouse_repositories.ClickHouseDatabase.open"), patch(
                "marketcow.clickhouse_repositories.ClickHouseDatabase.close"
            ) as close:
                repositories, resources = create_stage1_repositories(settings, warehouse)
                self.assertIsInstance(repositories.market_bars, ShadowMarketBarRepository)
                resources.close()
            close.assert_called_once()

    def test_background_factory_starts_and_reverse_closes_scheduler(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / "data-development"
            settings = Settings(
                root / "warehouse.duckdb", root / "raw", profile="development", port=8791,
                clickhouse_enabled=True, clickhouse_database="marketcow_test",
                clickhouse_spool_path=root / "spool/clickhouse", storage_root=root,
                clickhouse_background_canonical=True,
                clickhouse_scheduler_poll_seconds=0.05,
            )
            with patch("marketcow.clickhouse_repositories.ClickHouseDatabase.open"), patch(
                "marketcow.clickhouse_repositories.ClickHouseDatabase.close"
            ) as close:
                repositories, resources = create_stage1_repositories(
                    settings, Warehouse(settings.database_path)
                )
                scheduler = repositories.market_bars.background_scheduler
                self.assertTrue(scheduler.diagnostics()["thread_alive"])
                resources.close()
                self.assertFalse(scheduler._thread.is_alive())
            self.assertEqual(close.call_count, 2)

    def test_enabled_factory_startup_failure_is_explicit_and_creates_no_spool(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / "data-development"
            spool = root / "spool/clickhouse"
            settings = Settings(
                root / "warehouse.duckdb", root / "raw", profile="development", port=8791,
                clickhouse_enabled=True, clickhouse_database="marketcow_test",
                clickhouse_spool_path=spool, storage_root=root,
            )
            with patch(
                "marketcow.clickhouse_repositories.ClickHouseDatabase.open",
                side_effect=ConnectionError("bounded startup failure"),
            ):
                with self.assertRaisesRegex(ConnectionError, "bounded startup failure"):
                    create_stage1_repositories(settings, Warehouse(settings.database_path))
            self.assertFalse(spool.exists())

    def test_reconciliation_reports_consistency_and_bounded_mismatch(self):
        with tempfile.TemporaryDirectory() as folder:
            warehouse = Warehouse(Path(folder) / "warehouse.duckdb")
            writer = CapturingWriter()
            writer.spool = FakeSpool()
            writer.repository = ReconcileRepository([])
            adapter = ShadowMarketBarRepository(warehouse, writer)
            adapter.upsert_price_bars(
                "600519.SH", "1m", "raw", "fixture", "2026-07-20T01:00:02Z",
                bars(), {"observed_at": "2026-07-20T01:00:01Z",
                         "raw_artifact_id": "artifact-1"},
            )
            writer.repository.rows = writer.calls[0][1]
            consistent = adapter.reconcile_last_write()
            self.assertEqual(consistent["status"], "consistent")
            self.assertEqual(consistent["duckdb_count"], 1)
            self.assertEqual(consistent["clickhouse_count"], 1)
            self.assertEqual(consistent["ingestion_lag_seconds"], 0)
            writer.repository.rows = [{**writer.calls[0][1][0], "close": 99.0}]
            mismatch = adapter.reconcile_last_write(mismatch_limit=1)
            self.assertEqual(mismatch["status"], "mismatch")
            self.assertEqual(mismatch["mismatch_count"], 1)
            self.assertEqual(mismatch["mismatches"][0]["fields"], ["close"])


if __name__ == "__main__":
    unittest.main()
