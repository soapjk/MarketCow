import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock

from marketcow.local_backfill import (
    BACKFILL_VERSION,
    BackfillTargets,
    LocalStorageBackfill,
)
from marketcow.clickhouse_canonical import CanonicalMarketBarBuilder
from marketcow.clickhouse_repositories import ClickHouseDatabase, ClickHouseMarketBarRepository
from marketcow.clickhouse_writer import LocalClickHouseSpool, ReliableClickHouseWriter
from marketcow.postgres_repositories import PostgresDatabase
from marketcow.storage import Warehouse


class _Postgres:
    schema = "migration_fixture_test"


class _ClickHouse:
    database = "migration_fixture_test"


class LocalStorageBackfillTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.base = Path(self.folder.name)
        self.source = Warehouse(self.base / "source-test" / "market.duckdb")
        self.targets = BackfillTargets(
            self.base / "migration-test", _Postgres(), _ClickHouse(), Mock(), Mock(),
            "test", self.base,
        )

    def tearDown(self):
        self.folder.cleanup()

    def test_preflight_boundaries_reject_production_escape_and_symlink(self):
        production = BackfillTargets(
            self.base / "production", _Postgres(), _ClickHouse(), Mock(), Mock(),
            "production", self.base,
        )
        with self.assertRaisesRegex(ValueError, "development/test-only"):
            LocalStorageBackfill(self.source, production)
        outside = self.base.parent / "escaped-test"
        with self.assertRaisesRegex(ValueError, "escapes"):
            LocalStorageBackfill(self.source, BackfillTargets(
                outside, _Postgres(), _ClickHouse(), Mock(), Mock(), "test", self.base
            ))
        linked = self.base / "linked-test"
        linked.symlink_to(self.base / "actual-test")
        with self.assertRaisesRegex(ValueError, "symlink"):
            LocalStorageBackfill(self.source, BackfillTargets(
                linked, _Postgres(), _ClickHouse(), Mock(), Mock(), "test", self.base
            ))

    def test_checkpoint_is_bound_signed_and_atomic(self):
        drill = LocalStorageBackfill(self.source, self.targets)
        drill.state.mkdir(parents=True)
        checkpoint = {
            "version": BACKFILL_VERSION, "run_id": "stable", "source_path_hash": "s",
            "source_fingerprint": "f", "targets": drill._target_ids(),
            "snapshot_watermark": None, "phase": "backfill", "domains": {},
            "catchup_passes": 0, "last_live_fingerprint": None, "errors": [],
        }
        drill._save(checkpoint)
        loaded = json.loads(drill.checkpoint_path.read_text())
        drill._validate_checkpoint(loaded)
        loaded["phase"] = "complete"
        with self.assertRaisesRegex(ValueError, "checksum"):
            drill._validate_checkpoint(loaded)

    def test_content_checksum_normalizes_json_float_and_datetime_shapes(self):
        columns = ["id", "payload_json", "value"]
        left = [("a", '{"x":1}', 1.0)]
        right = [("a", {"x": 1}, 1.0)]
        self.assertEqual(
            LocalStorageBackfill._rows_checksum(columns, left, ("payload_json",)),
            LocalStorageBackfill._rows_checksum(columns, right, ("payload_json",)),
        )

    def test_raw_mapping_preserves_provenance_and_utc_boundary(self):
        columns = [
            "symbol", "interval", "adjustment", "timestamp", "bar_at", "open",
            "high", "low", "close", "raw_close", "adjustment_factor", "volume",
            "amount", "source", "ingested_at", "observed_at", "source_sequence",
            "raw_artifact_id",
        ]
        row = [
            "0700.HK", "1d", "none", 1784505600, "2026-07-20T00:00:00+00:00",
            1, 2, 0.5, 1.5, 1.5, 1, 100, 200, "fixture",
            "2026-07-20T00:00:01+00:00", "2026-07-20T08:00:00+08:00", "7", "raw-1",
        ]
        mapped = LocalStorageBackfill._raw_row(columns, row)
        self.assertEqual(mapped["market"], "HK")
        self.assertEqual(mapped["raw_artifact_id"], "raw-1")
        self.assertEqual(mapped["source_sequence"], "7")

    def test_durable_market_range_survives_restart_before_canonical(self):
        drill = LocalStorageBackfill(self.source, self.targets, batch_size=2)
        checkpoint = {"domains": {}}
        batches = [
            (["symbol", "interval", "adjustment", "timestamp", "bar_at", "open",
              "high", "low", "close", "raw_close", "adjustment_factor", "volume",
              "amount", "source", "ingested_at", "observed_at", "source_sequence",
              "raw_artifact_id"], [
                 ("MU", "1d", "none", 1, "2026-07-01T00:00:00Z", 1, 1, 1, 1,
                  1, 1, 1, None, "a", "2026-07-20T00:00:00Z",
                  "2026-07-20T00:00:00Z", "1", None),
                 ("MU", "1d", "none", 2, "2026-07-02T00:00:00Z", 2, 2, 2, 2,
                  2, 1, 2, None, "a", "2026-07-20T00:00:00Z",
                  "2026-07-20T00:00:00Z", "2", None),
             ]),
            ([], []),
        ]
        drill._scan = Mock(side_effect=batches)
        drill._save = Mock()
        self.targets.writer.write.return_value = {"spooled": 0, "written": 2}
        self.targets.canonical_builder.rebuild.return_value = {"status": "ok"}
        drill._copy_market(checkpoint, Path(self.source.path), "snapshot", None)
        state = checkpoint["domains"]["clickhouse:raw:snapshot"]
        self.assertTrue(state["canonical_done"])
        self.targets.canonical_builder.rebuild.assert_called_once_with(
            "MU", "1d", "none", "2026-07-01T00:00:00Z",
            "2026-07-02T00:00:00Z", 50000,
        )

    def test_report_is_bounded_and_contains_zero_lag_recovery(self):
        drill = LocalStorageBackfill(self.source, self.targets)
        report = drill._report({
            "run_id": "r", "source_fingerprint": "f", "snapshot_watermark": None,
            "targets": drill._target_ids(), "catchup_passes": 2,
        }, {"domains": [], "mismatches": []})
        self.assertEqual(report["lag"], 0)
        self.assertIn("atomic checkpoints", report["recovery"])
        self.assertNotIn(str(self.base), json.dumps(report))


if __name__ == "__main__":
    unittest.main()


@unittest.skipUnless(
    os.getenv("MARKETCOW_TEST_POSTGRES_DSN") and
    os.getenv("MARKETCOW_TEST_CLICKHOUSE_HOST"),
    "set PostgreSQL and ClickHouse test variables for the joint backfill drill",
)
class LocalStorageBackfillIntegrationTest(unittest.TestCase):
    def test_real_interrupted_backfill_increment_and_idempotent_reconcile(self):
        import clickhouse_connect
        import psycopg

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = Warehouse(root / "source-test/market.duckdb")
            with source.connect() as connection:
                connection.execute(
                    "INSERT INTO provider_health VALUES (?, ?, ?, ?, ?, ?)",
                    ["fixture", "healthy", "2026-07-20T00:00:00Z",
                     "2026-07-20T00:00:00Z", None, 0],
                )
            source.upsert_price_bars(
                "MU", "1d", "none", "fixture", "2026-07-20T00:00:01Z",
                [{"timestamp": 1784505600, "bar_at": "2026-07-20T00:00:00Z",
                  "open": 1, "high": 2, "low": .5, "close": 1.5,
                  "raw_close": 1.5, "adjustment_factor": 1, "volume": 100,
                  "amount": 150}],
                {"observed_at": "2026-07-20T00:00:00Z", "raw_artifact_id": "raw-1"},
            )
            suffix = uuid.uuid4().hex[:10]
            pg_schema = f"migration_{suffix}_test"
            ch_name = f"migration_{suffix}_test"
            postgres = PostgresDatabase(os.environ["MARKETCOW_TEST_POSTGRES_DSN"], pg_schema)
            clickhouse = ClickHouseDatabase(
                os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"],
                int(os.getenv("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123")), ch_name,
                os.getenv("MARKETCOW_TEST_CLICKHOUSE_USERNAME", "default"),
                os.getenv("MARKETCOW_TEST_CLICKHOUSE_PASSWORD", ""),
            )
            postgres.open()
            clickhouse.open()
            repository = ClickHouseMarketBarRepository(clickhouse)
            spool = LocalClickHouseSpool(root / "migration-test/spool", root)
            writer = ReliableClickHouseWriter(repository, spool, 1000)
            builder = CanonicalMarketBarBuilder(repository, writer)
            targets = BackfillTargets(
                root / "migration-test", postgres, clickhouse, writer, builder,
                "test", root, contract_gate=lambda: {"status": "ok", "checks": 9},
            )
            fired = {"value": False}

            def crash(stage, name):
                if stage == "after_write" and name == "postgres:provider_health:snapshot" \
                        and not fired["value"]:
                    fired["value"] = True
                    raise RuntimeError("injected interruption")

            try:
                with self.assertRaisesRegex(RuntimeError, "interruption"):
                    LocalStorageBackfill(source, targets, 2).run(crash)
                # A primary write arriving while the frozen snapshot is being resumed is
                # picked up by the deterministic catch-up pass.
                source.upsert_price_bars(
                    "MU", "1d", "none", "fixture", "2026-07-21T00:00:01Z",
                    [{"timestamp": 1784592000, "bar_at": "2026-07-21T00:00:00Z",
                      "open": 2, "high": 3, "low": 1, "close": 2.5,
                      "raw_close": 2.5, "adjustment_factor": 1, "volume": 200,
                      "amount": 500}],
                    {"observed_at": "2026-07-21T00:00:00Z", "raw_artifact_id": "raw-2"},
                )
                report = LocalStorageBackfill(source, targets, 2).run()
                self.assertEqual(report["status"], "complete")
                self.assertEqual(report["lag"], 0)
                self.assertFalse(report["mismatches"])
                self.assertEqual(clickhouse.client.query(
                    "SELECT count() FROM market_bar_raw FINAL WHERE symbol='MU'"
                ).result_rows[0][0], 2)
                self.assertEqual(clickhouse.client.query(
                    "SELECT count() FROM market_bar_canonical FINAL WHERE symbol='MU'"
                ).result_rows[0][0], 2)
                repeated = LocalStorageBackfill(source, targets, 2).run()
                self.assertEqual(repeated["run_id"], report["run_id"])
                self.assertEqual(repeated["lag"], 0)
            finally:
                clickhouse.close()
                bootstrap = clickhouse_connect.get_client(
                    host=os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"],
                    port=int(os.getenv("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123")),
                    username=os.getenv("MARKETCOW_TEST_CLICKHOUSE_USERNAME", "default"),
                    password=os.getenv("MARKETCOW_TEST_CLICKHOUSE_PASSWORD", ""),
                )
                bootstrap.command(f"DROP DATABASE IF EXISTS `{ch_name}`")
                bootstrap.close()
                postgres.close()
                with psycopg.connect(os.environ["MARKETCOW_TEST_POSTGRES_DSN"],
                                     autocommit=True) as connection:
                    connection.execute(f'DROP SCHEMA IF EXISTS "{pg_schema}" CASCADE')
