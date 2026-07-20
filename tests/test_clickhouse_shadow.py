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


class FailingClickHouseRepository:
    def insert_raw_bars(self, rows, batch_id=""):
        raise ConnectionError("clickhouse unavailable")


class ShadowMarketBarRepositoryTest(unittest.TestCase):
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
