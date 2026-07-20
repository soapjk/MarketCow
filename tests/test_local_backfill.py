import json
import os
import shutil
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
from marketcow.postgres_repositories import PostgresFundamentalRepository
from marketcow.contract_gate import compare_contract, LEGACY_PAYLOAD_PATHS
from marketcow.storage import Warehouse


class _Postgres:
    schema = "migration_fixture_test"


class _ClickHouse:
    database = "migration_fixture_test"


def _seed_all_postgres_domains(warehouse):
    special = {
        "run_id": "run-1", "request_id": "request-1", "artifact_id": "artifact-1",
        "provider": "fixture", "event_id": "event-1", "indicator_id": "indicator-1",
        "symbol": "FULL", "report_period": "20260331", "statement": "income",
        "report_date": "2026-03-31", "version_id": "version-1", "metric": "revenue",
        "source": "fixture", "source_a": "fixture-a", "source_b": "fixture-b",
        "status": "healthy", "quality_status": "ok", "job_name": "fixture",
        "started_at": "2026-06-01T00:00:00Z", "finished_at": "2026-06-01T00:01:00Z",
        "published_at": "2026-05-01T00:00:00Z", "observed_at": "2026-06-01T00:00:00Z",
        "ingested_at": "2026-06-01T00:00:01Z", "fetched_at": "2026-06-01T00:00:01Z",
        "rebuilt_at": "2026-06-01T00:00:02Z", "last_attempt_at": "2026-06-01T00:00:00Z",
        "last_success_at": "2026-06-01T00:00:00Z", "storage_path": "cold://artifact-1",
        "sha256": "a" * 64, "dataset": "fixture", "api_name": "fixture_api",
    }
    with warehouse.connect() as connection:
        for domain in __import__(
            "marketcow.local_backfill", fromlist=["POSTGRES_DOMAINS"]
        ).POSTGRES_DOMAINS:
            info = connection.execute(f"PRAGMA table_info('{domain.table}')").fetchall()
            columns = [row[1] for row in info]
            values = []
            for _, name, data_type, *_ in info:
                upper = str(data_type).upper()
                if name in special:
                    value = special[name]
                elif name.endswith("_json"):
                    value = json.dumps({"fixture": name}, sort_keys=True)
                elif "BOOL" in upper:
                    value = True
                elif any(token in upper for token in ("INT", "DOUBLE", "FLOAT", "DECIMAL")):
                    value = 1 if "INT" in upper else 10.5
                elif name.endswith("_date") or name in {"trade_date", "latest_date"}:
                    value = "2026-06-01"
                else:
                    value = f"fixture-{name}"
                values.append(value)
            marks = ",".join("?" for _ in columns)
            quoted = ",".join(f'"{name}"' for name in columns)
            connection.execute(
                f'INSERT INTO "{domain.table}" ({quoted}) VALUES ({marks})', values
            )
        for table in ("fundamental_snapshot_history", "tdx_financial_snapshot_history"):
            info = connection.execute(f"PRAGMA table_info('{table}')").fetchall()
            columns = [row[1] for row in info]
            row = list(connection.execute(f'SELECT * FROM "{table}" LIMIT 1').fetchone())
            row[columns.index("version_id")] = f"{table}-version-2"
            row[columns.index("ingested_at")] = "2026-06-02T00:00:01Z"
            if "price" in columns:
                row[columns.index("price")] = 11.5
            connection.execute(
                f'INSERT INTO "{table}" VALUES ({",".join("?" for _ in columns)})', row
            )


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
            "catchup_passes": 0, "last_live_fingerprint": None,
            "completion_fingerprint": None, "errors": [],
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

    def test_canonical_gate_rejects_equal_count_wrong_source_value_and_version(self):
        drill = LocalStorageBackfill(self.source, self.targets)
        columns = ["symbol", "selected_source", "close", "observed_at", "version"]
        self.targets.writer.repository.CANONICAL_COLUMNS = columns
        expected = [{
            "symbol": "MU", "selected_source": "priority", "close": 10,
            "observed_at": "2026-07-20T00:00:00Z", "version": 1,
        }]
        actual = [["MU", "other", 99, "2026-07-20T00:00:00Z", 7]]
        self.assertFalse(drill._canonical_matches(expected, actual))
        actual[0] = ["MU", "priority", 10, "2026-07-20T00:00:00+00:00", 1]
        self.assertTrue(drill._canonical_matches(expected, actual))

    def test_final_canonical_spool_or_truncation_never_advances(self):
        drill = LocalStorageBackfill(self.source, self.targets)
        drill._active_raw_rows = Mock(return_value=[{"symbol": "MU"}])
        self.targets.canonical_builder.build_rows.return_value = ([{"symbol": "MU"}], {}, {})
        self.targets.clickhouse._require_client = Mock(return_value=Mock())
        self.targets.writer.write.return_value = {"written": 0, "spooled": 1}
        with self.assertRaisesRegex(RuntimeError, "did not complete"):
            drill._finalize_canonical()
        self.targets.clickhouse._require_client.return_value.command.assert_called_once_with(
            "TRUNCATE TABLE market_bar_canonical"
        )

    def test_reconcile_window_mutation_forces_another_catchup_pass(self):
        drill = LocalStorageBackfill(self.source, self.targets)
        checkpoint = {
            "version": BACKFILL_VERSION, "run_id": "r", "source_path_hash": "s",
            "source_fingerprint": "snapshot", "targets": drill._target_ids(),
            "snapshot_watermark": None, "phase": "catchup", "domains": {},
            "catchup_passes": 0, "last_live_fingerprint": None,
            "completion_fingerprint": None, "errors": [],
        }
        drill._preflight = Mock()
        drill._load_or_initialize = Mock(return_value=checkpoint)
        drill._copy_domain = Mock()
        drill._copy_market = Mock()
        drill._finalize_canonical = Mock(return_value=[])
        drill.reconcile = Mock(return_value={"status": "ok", "domains": [], "mismatches": []})
        drill._save = Mock()
        # First pass is stable, but the source changes before reconciliation.  The
        # second pass and its reconcile window are stable.
        drill._logical_fingerprint = Mock(side_effect=[
            "a", "a", "changed", "changed", "changed", "changed", "changed",
        ])
        report = drill.run(max_catchup_passes=2)
        self.assertEqual(checkpoint["catchup_passes"], 2)
        self.assertEqual(checkpoint["completion_fingerprint"], "changed")
        self.assertEqual(report["lag"], 0)
        drill.reconcile.assert_called_once()

    def test_market_after_write_fault_retries_same_key_without_skipping(self):
        drill = LocalStorageBackfill(self.source, self.targets, batch_size=2)
        columns = [
            "symbol", "interval", "adjustment", "timestamp", "bar_at", "open",
            "high", "low", "close", "raw_close", "adjustment_factor", "volume",
            "amount", "source", "ingested_at", "observed_at", "source_sequence",
            "raw_artifact_id",
        ]
        rows = [("MU", "1d", "none", 1, "2026-07-01T00:00:00Z", 1, 1, 1,
                 1, 1, 1, 1, None, "a", "2026-07-20T00:00:00Z",
                 "2026-07-20T00:00:00Z", "1", None)]
        checkpoint = {"domains": {}}
        drill._scan = Mock(return_value=(columns, rows))
        drill._save = Mock()
        self.targets.writer.write.return_value = {"spooled": 0, "written": 1}
        with self.assertRaisesRegex(RuntimeError, "batch crash"):
            drill._copy_market(
                checkpoint, Path(self.source.path), "snapshot",
                lambda stage, _name: (_ for _ in ()).throw(RuntimeError("batch crash"))
                if stage == "after_write" else None,
            )
        state = checkpoint["domains"]["clickhouse:raw:snapshot"]
        self.assertIsNone(state["after"])
        self.assertEqual(state["rows"], 0)

    def test_postgres_outage_and_after_checkpoint_windows_are_retryable(self):
        drill = LocalStorageBackfill(self.source, self.targets, batch_size=2)
        domain = __import__(
            "marketcow.local_backfill", fromlist=["POSTGRES_DOMAINS"]
        ).POSTGRES_DOMAINS[1]
        checkpoint = {"domains": {}}
        drill._scan = Mock(side_effect=[(["provider"], [("fixture",)]), ([], [])])
        drill._upsert_postgres = Mock(side_effect=ConnectionError("temporary pg outage"))
        with self.assertRaisesRegex(ConnectionError, "outage"):
            drill._copy_domain(checkpoint, Path(self.source.path), domain, "snapshot", None)
        state = checkpoint["domains"]["postgres:provider_health:snapshot"]
        self.assertIsNone(state["after"])
        drill._upsert_postgres = Mock()
        drill._scan = Mock(side_effect=[(["provider"], [("fixture",)]), ([], [])])
        drill._save = Mock()
        fired = {"value": False}

        def after_checkpoint(stage, _name):
            if stage == "after_checkpoint" and not fired["value"]:
                fired["value"] = True
                raise RuntimeError("checkpoint crash")

        with self.assertRaisesRegex(RuntimeError, "checkpoint crash"):
            drill._copy_domain(
                checkpoint, Path(self.source.path), domain, "snapshot", after_checkpoint
            )
        self.assertEqual(state["after"], ["fixture"])
        self.assertEqual(state["rows"], 1)

    def test_truncated_canonical_rebuild_keeps_durable_range_pending(self):
        drill = LocalStorageBackfill(self.source, self.targets, batch_size=2)
        checkpoint = {"domains": {}}
        columns = [
            "symbol", "interval", "adjustment", "timestamp", "bar_at", "open",
            "high", "low", "close", "raw_close", "adjustment_factor", "volume",
            "amount", "source", "ingested_at", "observed_at", "source_sequence",
            "raw_artifact_id",
        ]
        row = ("MU", "1d", "none", 1, "2026-07-01T00:00:00Z", 1, 1, 1,
               1, 1, 1, 1, None, "a", "2026-07-20T00:00:00Z",
               "2026-07-20T00:00:00Z", "1", None)
        drill._scan = Mock(side_effect=[(columns, [row]), (columns, [])])
        drill._save = Mock()
        self.targets.writer.write.return_value = {"spooled": 0, "written": 1}
        self.targets.canonical_builder.rebuild.return_value = {"status": "truncated"}
        with self.assertRaisesRegex(RuntimeError, "rebuild"):
            drill._copy_market(checkpoint, Path(self.source.path), "snapshot", None)
        state = checkpoint["domains"]["clickhouse:raw:snapshot"]
        self.assertTrue(state["done"])
        self.assertFalse(state["canonical_done"])
        self.assertTrue(state["ranges"])

    def test_report_is_bounded_and_contains_zero_lag_recovery(self):
        drill = LocalStorageBackfill(self.source, self.targets)
        report = drill._report({
            "run_id": "r", "source_fingerprint": "f", "snapshot_watermark": None,
            "targets": drill._target_ids(), "catchup_passes": 2,
            "last_live_fingerprint": "final", "completion_fingerprint": "final",
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
            _seed_all_postgres_domains(source)
            source.upsert_price_bars(
                "MU", "1d", "none", "fixture", "2026-07-20T00:00:01Z",
                [{"timestamp": 1784505600, "bar_at": "2026-07-20T00:00:00Z",
                  "open": 1, "high": 2, "low": .5, "close": 1.5,
                  "raw_close": 1.5, "adjustment_factor": 1, "volume": 100,
                  "amount": 150}],
                {"observed_at": "2026-07-20T00:00:00Z", "raw_artifact_id": "raw-1"},
            )
            # Equal-ingestion conflicting retries converge by the shared content rank,
            # independent of arrival order; the full raw checksum below proves the
            # selected logical version is identical in ClickHouse.
            for close in (6.5, 1.5):
                source.upsert_price_bars(
                    "MU", "1d", "none", "fixture", "2026-07-20T00:00:01Z",
                    [{"timestamp": 1784505600, "bar_at": "2026-07-20T00:00:00Z",
                      "open": 1, "high": 7, "low": .5, "close": close,
                      "raw_close": close, "adjustment_factor": 1, "volume": 100,
                      "amount": 150}],
                    {"observed_at": "2026-07-20T00:00:00Z", "raw_artifact_id": "raw-1"},
                )
            source.upsert_price_bars(
                "MU", "1d", "none", "baostock", "2026-07-19T00:00:01Z",
                [{"timestamp": 1784505600, "bar_at": "2026-07-20T00:00:00Z",
                  "open": 8, "high": 9, "low": 7, "close": 8.5,
                  "raw_close": 8.5, "adjustment_factor": 1, "volume": 80,
                  "amount": 680}],
                {"observed_at": "2026-07-19T00:00:00Z",
                 "raw_artifact_id": "raw-priority"},
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

            def migrated_target_contract_gate():
                canonical_expected = source.get_price_bars_page(
                    "MU", "1d", "none", "2026-07-19T00:00:00Z",
                    "2026-07-22T00:00:00Z", 100, None,
                )
                canonical_actual = repository.get_canonical_price_bars_page(
                    "MU", "1d", "none", "2026-07-19T00:00:00Z",
                    "2026-07-22T00:00:00Z", 100, None,
                )
                raw_expected = source.get_raw_price_bars_range(
                    "MU", "1d", "none", "2026-07-19T00:00:00Z",
                    "2026-07-22T00:00:00Z", 100,
                )
                raw_actual = repository.get_raw_price_bars_range(
                    "MU", "1d", "none", "2026-07-19T00:00:00Z",
                    "2026-07-22T00:00:00Z", 100,
                )
                comparisons = [
                    compare_contract(canonical_expected, canonical_actual,
                                     LEGACY_PAYLOAD_PATHS),
                    compare_contract(raw_expected, raw_actual, LEGACY_PAYLOAD_PATHS),
                ]
                pit = PostgresFundamentalRepository(postgres).query_fundamentals(
                    symbol="FULL", as_of="2026-07-01T00:00:00Z", limit=10
                )
                ok = all(item["status"] == "ok" for item in comparisons) and bool(pit)
                if not ok:
                    raise AssertionError({"comparisons": comparisons, "pit": pit})
                return {"status": "ok" if ok else "mismatch", "checks": 3}

            targets = BackfillTargets(
                root / "migration-test", postgres, clickhouse, writer, builder,
                "test", root, contract_gate=migrated_target_contract_gate,
            )
            fired = {"value": False}

            def crash(stage, name):
                if stage == "after_write" and name == "postgres:provider_health:snapshot" \
                        and not fired["value"]:
                    fired["value"] = True
                    raise RuntimeError("injected interruption")

            try:
                with postgres.connection() as connection:
                    connection.execute(
                        "INSERT INTO schema_migrations(version, description) VALUES (999, 'future')"
                    )
                with self.assertRaisesRegex(ValueError, "unknown"):
                    LocalStorageBackfill(source, targets, 2)._preflight()
                with postgres.connection() as connection:
                    connection.execute("DELETE FROM schema_migrations WHERE version=999")
                clickhouse.client.insert(
                    "schema_migrations", [[999, "future"]],
                    column_names=["version", "description"],
                )
                with self.assertRaisesRegex(ValueError, "unknown"):
                    LocalStorageBackfill(source, targets, 2)._preflight()
                clickhouse.client.command(
                    "ALTER TABLE schema_migrations DELETE WHERE version=999 "
                    "SETTINGS mutations_sync=2"
                )
                with self.assertRaisesRegex(RuntimeError, "interruption"):
                    LocalStorageBackfill(source, targets, 2).run(crash)
                original_insert = repository.insert_raw_bars
                repository.insert_raw_bars = Mock(side_effect=ConnectionError("temporary outage"))
                with self.assertRaisesRegex(RuntimeError, "spooled"):
                    LocalStorageBackfill(source, targets, 2).run()
                repository.insert_raw_bars = original_insert
                replay = writer.replay(10)
                self.assertEqual(replay["replayed"], 1)
                with source.connect() as connection:
                    connection.execute(
                        "UPDATE economic_indicator_latest SET value=42.5, "
                        "ingested_at='2026-07-20T00:00:03Z' WHERE indicator_id='indicator-1'"
                    )
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
                self.assertEqual(len([
                    item for item in report["domains"]
                    if item["domain"] in {
                        domain.table for domain in __import__(
                            "marketcow.local_backfill", fromlist=["POSTGRES_DOMAINS"]
                        ).POSTGRES_DOMAINS
                    } and item["rows"] >= 1
                ]), 16)
                self.assertEqual(clickhouse.client.query(
                    "SELECT count() FROM market_bar_raw FINAL WHERE symbol='MU'"
                ).result_rows[0][0], 3)
                self.assertEqual(clickhouse.client.query(
                    "SELECT count() FROM market_bar_canonical FINAL WHERE symbol='MU'"
                ).result_rows[0][0], 2)
                selected = clickhouse.client.query(
                    "SELECT selected_source, close, version FROM market_bar_canonical FINAL "
                    "WHERE symbol='MU' AND bar_time=toDateTime64('2026-07-20 00:00:00',3,'UTC')"
                ).result_rows[0]
                self.assertEqual(selected, ("baostock", 8.5, 1))
                repeated = LocalStorageBackfill(source, targets, 2).run()
                self.assertEqual(repeated["run_id"], report["run_id"])
                self.assertEqual(repeated["lag"], 0)
                if export_root := os.getenv("MARKETCOW_READINESS_EVIDENCE_ROOT"):
                    target = Path(export_root) / "SV2-022A"
                    target.mkdir(parents=True, exist_ok=True)
                    exported = LocalStorageBackfill(source, targets, 2)
                    shutil.copy2(exported.state / "report.json", target / "report.json")
                    shutil.copy2(exported.checkpoint_path, target / "checkpoint.json")
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
