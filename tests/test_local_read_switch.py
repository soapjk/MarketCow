import json
import os
import shutil
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock

from marketcow.local_backfill import BACKFILL_VERSION, LocalStorageBackfill
from marketcow.local_read_switch import LocalReadSwitchDrill, ReadSwitchInputs


class _Repository:
    canonical_reads_enabled = False
    raw_reads_enabled = False


class LocalReadSwitchTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.state = self.root / "switch-test"
        self.artifacts = self.root / "artifacts"
        self.artifacts.mkdir()
        checkpoint = {
            "version": BACKFILL_VERSION, "run_id": "backfill-run",
            "source_path_hash": "source-logical", "source_fingerprint": "snapshot",
            "targets": {"postgres_schema": "fixture_test",
                        "clickhouse_database": "fixture_test"},
            "snapshot_watermark": "2026-07-20T00:00:00Z", "phase": "complete",
            "domains": {}, "catchup_passes": 2,
            "last_live_fingerprint": "complete-fingerprint",
            "completion_fingerprint": "complete-fingerprint", "errors": [],
        }
        LocalStorageBackfill._sign(checkpoint)
        self.backfill_checkpoint = self.artifacts / "backfill-checkpoint.json"
        self.backfill_checkpoint.write_text(json.dumps(checkpoint))
        self.backfill_report = self.artifacts / "backfill-report.json"
        self.backfill_report.write_text(json.dumps({
            "status": "complete", "lag": 0, "domains": [{"status": "ok"}],
        }))
        self.restore_report = self.artifacts / "restore-report.json"
        self.restore_report.write_text(json.dumps({
            "status": "complete", "verification": {"contract_gate": "ok"},
        }))
        self.repository = _Repository()
        self.gate = {
            "lag": 0, "reconcile": "ok", "contract": "ok", "spool_pending": 0,
            "canonical_queue": 0, "readiness": "healthy",
        }
        self.golden = Mock(return_value={"status": "ok", "samples": 9, "fallbacks": 0})

    def tearDown(self):
        self.folder.cleanup()

    def inputs(self, **overrides):
        values = dict(
            root=self.state, repository=self.repository,
            backfill_checkpoint=self.backfill_checkpoint,
            backfill_report=self.backfill_report, restore_report=self.restore_report,
            backup_artifact_id="backup-local-1", restore_artifact_id="restore-local-1",
            profile="test", allowed_root=self.root, gate=lambda: dict(self.gate),
            golden=self.golden,
        )
        values.update(overrides)
        return ReadSwitchInputs(**values)

    def test_switch_and_explicit_rollback_are_durable_and_idempotent(self):
        drill = LocalReadSwitchDrill(self.inputs())
        report = drill.run(2)
        self.assertEqual(report["final_backend"], "clickhouse")
        self.assertTrue(self.repository.canonical_reads_enabled)
        self.assertTrue(self.repository.raw_reads_enabled)
        restarted_repository = _Repository()
        restarted = LocalReadSwitchDrill(self.inputs(repository=restarted_repository))
        self.assertTrue(restarted_repository.canonical_reads_enabled)
        self.assertTrue(restarted_repository.raw_reads_enabled)
        rolled = restarted.rollback()
        self.assertEqual(rolled["final_backend"], "duckdb")
        self.assertFalse(restarted_repository.canonical_reads_enabled)
        self.assertFalse(restarted_repository.raw_reads_enabled)
        self.assertEqual(restarted.rollback()["final_backend"], "duckdb")

    def test_all_preflight_stop_conditions_rollback_to_duckdb(self):
        for key, value in (
            ("lag", 1), ("reconcile", "mismatch"), ("contract", "mismatch"),
            ("spool_pending", 1), ("canonical_queue", 1),
            ("readiness", "unavailable"),
        ):
            with self.subTest(condition=key):
                root = self.root / f"{key}-test"
                repository = _Repository()
                gate = dict(self.gate)
                gate[key] = value
                drill = LocalReadSwitchDrill(self.inputs(
                    root=root, repository=repository, gate=lambda gate=gate: gate
                ))
                with self.assertRaisesRegex(RuntimeError, "stop condition"):
                    drill.run()
                self.assertFalse(repository.canonical_reads_enabled)
                self.assertFalse(repository.raw_reads_enabled)

    def test_contract_mismatch_during_observation_stops_and_rolls_back(self):
        calls = {"value": 0}

        def golden(_backend):
            calls["value"] += 1
            return {"status": "mismatch" if calls["value"] == 3 else "ok",
                    "samples": 1, "fallbacks": 0}

        drill = LocalReadSwitchDrill(self.inputs(golden=golden))
        with self.assertRaisesRegex(RuntimeError, "golden contract"):
            drill.run(3)
        self.assertFalse(self.repository.canonical_reads_enabled)
        self.assertFalse(self.repository.raw_reads_enabled)

    def test_incremental_write_is_observed_before_raw_switch(self):
        incremental = Mock()
        drill = LocalReadSwitchDrill(self.inputs(incremental_write=incremental))
        first = drill.run(1)
        repeated = LocalReadSwitchDrill(
            self.inputs(incremental_write=incremental)
        ).run(1)
        incremental.assert_called_once_with()
        self.assertEqual(repeated["events"], first["events"])
        backends = [call.args[0] for call in self.golden.call_args_list]
        self.assertEqual(backends, [
            "duckdb", "clickhouse_canonical", "clickhouse_canonical", "clickhouse_raw",
            "clickhouse_canonical", "clickhouse_raw",
        ])

    def test_crash_after_apply_recovers_conservatively_then_can_resume(self):
        fired = {"value": False}

        def crash(stage, name):
            if stage == "after_apply" and name == "canonical_enabled" and not fired["value"]:
                fired["value"] = True
                raise RuntimeError("crash")

        with self.assertRaisesRegex(RuntimeError, "crash"):
            LocalReadSwitchDrill(self.inputs()).run(1, crash)
        self.assertFalse(self.repository.canonical_reads_enabled)
        restarted_repository = _Repository()
        report = LocalReadSwitchDrill(self.inputs(repository=restarted_repository)).run(1)
        self.assertEqual(report["final_backend"], "clickhouse")

    def test_artifact_binding_tamper_incomplete_and_production_are_rejected(self):
        document = json.loads(self.backfill_checkpoint.read_text())
        document["completion_fingerprint"] = "tampered"
        self.backfill_checkpoint.write_text(json.dumps(document))
        with self.assertRaisesRegex(ValueError, "checksum"):
            LocalReadSwitchDrill(self.inputs()).run()
        with self.assertRaisesRegex(ValueError, "production"):
            LocalReadSwitchDrill(self.inputs(backup_artifact_id="production-backup"))

    def test_artifacts_must_be_contained_and_config_is_bound(self):
        outside = Path(self.folder.name).parent / f"outside-{uuid.uuid4().hex}.json"
        outside.write_text(self.backfill_report.read_text())
        self.addCleanup(outside.unlink)
        with self.assertRaisesRegex(ValueError, "escapes allowed root"):
            LocalReadSwitchDrill(self.inputs(backfill_report=outside)).run()

        drill = LocalReadSwitchDrill(self.inputs())
        drill.run(1)
        changed = json.loads(self.restore_report.read_text())
        changed["verification"]["new"] = "evidence"
        self.restore_report.write_text(json.dumps(changed))
        restarted = _Repository()
        with self.assertRaisesRegex(ValueError, "binding mismatch"):
            LocalReadSwitchDrill(self.inputs(repository=restarted)).run(1)
        self.assertFalse(restarted.canonical_reads_enabled)
        self.assertFalse(restarted.raw_reads_enabled)

    def test_report_is_bounded_and_contains_no_absolute_paths(self):
        rendered = json.dumps(LocalReadSwitchDrill(self.inputs()).run(1))
        self.assertNotIn(str(self.root), rendered)
        report = json.loads((self.state / ".storage-v2-read-switch/report.json").read_text())
        self.assertLessEqual(len(report["events"]), 100)


@unittest.skipUnless(
    os.getenv("MARKETCOW_TEST_CLICKHOUSE_HOST"),
    "set MARKETCOW_TEST_CLICKHOUSE_HOST for real read switch integration",
)
class LocalReadSwitchIntegrationTest(unittest.TestCase):
    def test_real_canonical_raw_switch_increment_fallback_and_rollback(self):
        import clickhouse_connect
        from datetime import datetime, timezone
        from fastapi.testclient import TestClient
        from marketcow.api import create_app
        from marketcow.clickhouse_canonical import CanonicalMarketBarBuilder
        from marketcow.clickhouse_repositories import ClickHouseDatabase, ClickHouseMarketBarRepository
        from marketcow.clickhouse_shadow import ShadowMarketBarRepository
        from marketcow.clickhouse_writer import LocalClickHouseSpool, ReliableClickHouseWriter
        from marketcow.contract_gate import compare_contract, LEGACY_PAYLOAD_PATHS
        from marketcow.config import Settings
        from marketcow.storage import Warehouse

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            warehouse = Warehouse(root / "source-test/market.duckdb")
            database_name = "switch_" + uuid.uuid4().hex[:10] + "_test"
            database = ClickHouseDatabase(
                os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"],
                int(os.getenv("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123")), database_name,
                os.getenv("MARKETCOW_TEST_CLICKHOUSE_USERNAME", "default"),
                os.getenv("MARKETCOW_TEST_CLICKHOUSE_PASSWORD", ""),
            )
            database.open()
            repository = ClickHouseMarketBarRepository(database)
            spool = LocalClickHouseSpool(root / "switch-test/spool", root)
            writer = ReliableClickHouseWriter(repository, spool, 1000)
            builder = CanonicalMarketBarBuilder(repository, writer)
            adapter = ShadowMarketBarRepository(warehouse, writer, builder)
            bars = [{
                "timestamp": 1784505600, "bar_at": "2026-07-20T00:00:00Z",
                "open": 1, "high": 2, "low": .5, "close": 1.5,
                "raw_close": 1.5, "adjustment_factor": 1, "volume": 100, "amount": 150,
            }]
            adapter.upsert_price_bars(
                "MU", "1d", "none", "fixture", "2026-07-20T00:00:01Z", bars,
                {"observed_at": "2026-07-20T00:00:00Z", "raw_artifact_id": "raw-1"},
            )
            self.assertEqual(builder.rebuild(
                "MU", "1d", "none", "2026-07-20T00:00:00Z",
                "2026-07-20T00:00:00Z", 100,
            )["status"], "ok")
            artifact_root = root / "artifacts"
            artifact_root.mkdir()
            checkpoint = {
                "version": BACKFILL_VERSION, "run_id": "real-backfill",
                "source_path_hash": "source", "source_fingerprint": "snapshot",
                "targets": {"postgres_schema": "switch_test",
                            "clickhouse_database": database_name},
                "snapshot_watermark": "2026-07-20T00:00:01Z", "phase": "complete",
                "domains": {}, "catchup_passes": 1, "last_live_fingerprint": "stable",
                "completion_fingerprint": "stable", "errors": [],
            }
            LocalStorageBackfill._sign(checkpoint)
            cp = artifact_root / "backfill.json"
            cp.write_text(json.dumps(checkpoint))
            br = artifact_root / "report.json"
            br.write_text(json.dumps({"status": "complete", "lag": 0}))
            rr = artifact_root / "restore.json"
            rr.write_text(json.dumps({"status": "complete", "verification": {"gate": "ok"}}))
            fallback = {"count": 0}

            class Service:
                def __init__(self, market_bars):
                    self.market_bar_repository = market_bars

                def refresh_quote_history(self, *_args):
                    raise AssertionError("read switch golden must never refresh upstream")

                def close(self):
                    pass

            settings = Settings(
                warehouse.path, root / "raw", storage_root=root / "source-test",
                market_bar_cursor_secret="switch-test-secret-1234567890abcdef",
                market_bar_cache_freshness_seconds=86400,
            )
            now = lambda: datetime(2026, 7, 22, tzinfo=timezone.utc)
            api_paths = [
                "/v1/quotes/MU/history?interval=1d&adjustment=none&refresh=false",
                "/v1/quotes/MU/history?interval=1d&adjustment=none"
                "&start=2026-07-19T00:00:00Z&end=2026-07-22T00:00:00Z&page_size=1",
                "/v1/quotes/cross-section?interval=1d&adjustment=none"
                "&bar_at=2026-07-20T00:00:00Z&page_size=1&symbols=MU",
                "/v1/quotes/MU/raw-history?interval=1d&adjustment=none"
                "&start=2026-07-19T00:00:00Z&end=2026-07-22T00:00:00Z&page_size=1",
            ]

            def api_contracts():
                results = []
                with TestClient(create_app(settings, Service(warehouse), now)) as left:
                    with TestClient(create_app(settings, Service(adapter), now)) as right:
                        for path in api_paths:
                            expected, actual = left.get(path), right.get(path)
                            if expected.status_code != actual.status_code:
                                results.append("http")
                                continue
                            comparison = compare_contract(
                                expected.json(), actual.json(), LEGACY_PAYLOAD_PATHS,
                            )
                            if comparison["status"] != "ok":
                                results.append(path)
                return results

            def golden(backend):
                start, end = "2026-07-19T00:00:00Z", "2026-07-22T00:00:00Z"
                canonical_calls = [
                    ("get_price_bars", ("MU", "1d", "none", 100)),
                    ("get_price_bars_range", ("MU", "1d", "none", start, end, 100)),
                    ("get_price_bars_page", ("MU", "1d", "none", start, end, 100, None)),
                    ("get_price_bars_cross_section", ("1d", "none", "2026-07-20T00:00:00Z", 100, ["MU"])),
                    ("get_price_bars_cross_section_page", ("1d", "none", "2026-07-20T00:00:00Z", 100, ["MU"], None)),
                    ("get_price_bars_matrix_page", ("1d", "none", ["2026-07-20T00:00:00Z"], ["MU"], 100, None)),
                    ("get_price_bar_as_of", ("MU", "1d", "none", "2026-07-20T12:00:00Z", 86400)),
                    ("get_price_bars_as_of_page", ("1d", "none", "2026-07-20T12:00:00Z", 86400, ["MU"], 100, None)),
                ]
                raw_calls = [
                    ("get_raw_price_bars_range", ("MU", "1d", "none", start, end, 100, None)),
                    ("get_raw_price_bars_page", ("MU", "1d", "none", start, end, 100, None, None)),
                ]
                calls = raw_calls if backend == "clickhouse_raw" else canonical_calls
                failures = []
                used_fallback = 0
                for method, arguments in calls:
                    expected = getattr(warehouse, method)(*arguments)
                    actual = getattr(adapter, method)(*arguments)
                    comparison = compare_contract(
                        expected, actual, LEGACY_PAYLOAD_PATHS,
                    )
                    if comparison["status"] != "ok":
                        failures.append(method)
                    used_fallback += int(bool(adapter._last_read.get("fallback")))
                failures.extend(api_contracts())
                fallback["count"] += used_fallback
                return {"status": "mismatch" if failures else "ok",
                        "samples": len(calls) + len(api_paths),
                        "fallbacks": used_fallback}

            def incremental():
                adapter.upsert_price_bars(
                    "MU", "1d", "none", "fixture", "2026-07-21T00:00:01Z",
                    [{**bars[0], "timestamp": 1784592000,
                      "bar_at": "2026-07-21T00:00:00Z", "close": 2.5,
                      "raw_close": 2.5}],
                    {"observed_at": "2026-07-21T00:00:00Z", "raw_artifact_id": "raw-2"},
                )
                builder.rebuild(
                    "MU", "1d", "none", "2026-07-21T00:00:00Z",
                    "2026-07-21T00:00:00Z", 100,
                )

            def gate():
                backfill_state = json.loads(cp.read_text())
                backfill_result = json.loads(br.read_text())
                try:
                    readiness = (
                        "healthy" if database.diagnostics()["status"] == "ok"
                        else "unavailable"
                    )
                except Exception:
                    readiness = "unavailable"
                stable = (
                    backfill_state.get("phase") == "complete" and
                    backfill_state.get("completion_fingerprint") ==
                    backfill_state.get("last_live_fingerprint")
                )
                contract = golden("duckdb")["status"]
                canonical_pending = 0
                for pending in spool.pending.glob("*.json"):
                    try:
                        canonical_pending += int(
                            json.loads(pending.read_text()).get("dataset") == "canonical"
                        )
                    except (OSError, ValueError):
                        canonical_pending += 1
                return {
                    "lag": backfill_result.get("lag", 1) if stable else 1,
                    "reconcile": (
                        "ok" if backfill_result.get("status") == "complete" and
                        not backfill_result.get("mismatches", []) else "mismatch"
                    ),
                    "contract": contract,
                    "spool_pending": spool.diagnostics()["pending"],
                    "canonical_queue": canonical_pending, "readiness": readiness,
                }
            inputs = ReadSwitchInputs(
                root / "switch-test", adapter, cp, br, rr, "backup-verified",
                "restore-verified", "test", root, gate, golden, incremental,
            )
            try:
                drill = LocalReadSwitchDrill(inputs)
                interrupted = {"value": False}

                def interrupt(stage, name):
                    if (stage == "after_apply" and name == "canonical_enabled" and
                            not interrupted["value"]):
                        interrupted["value"] = True
                        raise RuntimeError("real switch interruption")

                with self.assertRaisesRegex(RuntimeError, "real switch interruption"):
                    drill.run(2, interrupt)
                self.assertFalse(adapter.canonical_reads_enabled)
                self.assertFalse(adapter.raw_reads_enabled)
                drill = LocalReadSwitchDrill(inputs)
                self.assertEqual(drill.run(2)["final_backend"], "clickhouse")
                original = database.client
                database.client = None
                adapter.upsert_price_bars(
                    "MU", "1d", "none", "fixture", "2026-07-22T00:00:01Z",
                    [{**bars[0], "timestamp": 1784678400,
                      "bar_at": "2026-07-22T00:00:00Z", "close": 3.5,
                      "raw_close": 3.5}],
                    {"observed_at": "2026-07-22T00:00:00Z",
                     "raw_artifact_id": "raw-outage"},
                )
                self.assertEqual(spool.diagnostics()["pending"], 1)
                # The outage-period primary write is immediately readable via fallback.
                self.assertEqual(golden("clickhouse_canonical")["status"], "ok")
                self.assertGreaterEqual(fallback["count"], 1)
                with self.assertRaisesRegex(RuntimeError, "stop condition"):
                    drill.run(1)
                self.assertFalse(adapter.canonical_reads_enabled)
                self.assertFalse(adapter.raw_reads_enabled)
                database.client = original
                self.assertEqual(writer.replay(10)["replayed"], 1)
                self.assertEqual(builder.rebuild(
                    "MU", "1d", "none", "2026-07-22T00:00:00Z",
                    "2026-07-22T00:00:00Z", 100,
                )["status"], "ok")
                self.assertEqual(spool.diagnostics()["pending"], 0)
                self.assertEqual(golden("clickhouse_canonical")["status"], "ok")
                self.assertEqual(golden("clickhouse_raw")["status"], "ok")
                drill = LocalReadSwitchDrill(inputs)
                self.assertEqual(drill.run(1)["final_backend"], "clickhouse")
                rolled = drill.rollback("outage_drill")
                self.assertEqual(rolled["final_backend"], "duckdb")
                self.assertFalse(adapter.canonical_reads_enabled)
                self.assertFalse(adapter.raw_reads_enabled)
                self.assertEqual(golden("duckdb")["status"], "ok")
            finally:
                database.close()
                bootstrap = clickhouse_connect.get_client(
                    host=os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"],
                    port=int(os.getenv("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123")),
                    username=os.getenv("MARKETCOW_TEST_CLICKHOUSE_USERNAME", "default"),
                    password=os.getenv("MARKETCOW_TEST_CLICKHOUSE_PASSWORD", ""),
                )
                bootstrap.command(f"DROP DATABASE IF EXISTS `{database_name}`")
                bootstrap.close()


@unittest.skipUnless(
    os.getenv("MARKETCOW_TEST_POSTGRES_DSN") and
    os.getenv("MARKETCOW_TEST_CLICKHOUSE_HOST"),
    "set PostgreSQL and ClickHouse integration settings for composed switch drill",
)
class LocalReadSwitchComposedIntegrationTest(unittest.TestCase):
    def test_real_backup_restore_backfill_outputs_bind_the_switch(self):
        import clickhouse_connect
        import psycopg

        from marketcow.clickhouse_canonical import CanonicalMarketBarBuilder
        from marketcow.clickhouse_repositories import (
            ClickHouseDatabase, ClickHouseMarketBarRepository,
        )
        from marketcow.clickhouse_shadow import ShadowMarketBarRepository
        from marketcow.clickhouse_writer import LocalClickHouseSpool, ReliableClickHouseWriter
        from marketcow.contract_gate import LEGACY_PAYLOAD_PATHS, compare_contract
        from marketcow.local_backup import (
            BackupComponent, LocalStorageBackup, _hash, _json,
        )
        from marketcow.local_backfill import BackfillTargets, LocalStorageBackfill
        from marketcow.local_restore import LocalStorageRestore, RestoreTargets
        from marketcow.postgres_repositories import PostgresDatabase
        from marketcow.storage import Warehouse
        from tests.test_local_backfill import _seed_all_postgres_domains

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source_root = root / "source-development"
            source = Warehouse(source_root / "warehouse/market_data.duckdb")
            _seed_all_postgres_domains(source)
            source.upsert_price_bars(
                "MU", "1d", "none", "fixture", "2026-07-20T00:00:01Z",
                [{"timestamp": 1784505600, "bar_at": "2026-07-20T00:00:00Z",
                  "open": 1, "high": 2, "low": .5, "close": 1.5,
                  "raw_close": 1.5, "adjustment_factor": 1, "volume": 100,
                  "amount": 150}],
                {"observed_at": "2026-07-20T00:00:00Z",
                 "raw_artifact_id": "composed-raw"},
            )
            suffix = uuid.uuid4().hex[:10]
            pg_schema = f"switch_chain_{suffix}_test"
            ch_name = f"switch_chain_{suffix}_test"
            postgres = PostgresDatabase(os.environ["MARKETCOW_TEST_POSTGRES_DSN"], pg_schema)
            clickhouse = ClickHouseDatabase(
                os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"],
                int(os.getenv("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123")), ch_name,
                os.getenv("MARKETCOW_TEST_CLICKHOUSE_USERNAME", "default"),
                os.getenv("MARKETCOW_TEST_CLICKHOUSE_PASSWORD", ""),
            )
            postgres.pool.open(wait=True)
            bootstrap = clickhouse_connect.get_client(
                host=os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"],
                port=int(os.getenv("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123")),
                username=os.getenv("MARKETCOW_TEST_CLICKHOUSE_USERNAME", "default"),
                password=os.getenv("MARKETCOW_TEST_CLICKHOUSE_PASSWORD", ""),
            )
            bootstrap.command(f"CREATE DATABASE `{ch_name}`")
            bootstrap.close()
            clickhouse.client = clickhouse._connect(ch_name)
            try:
                captured = "2026-07-20T00:01:00Z"
                watermark = {"captured_at": captured}
                cold = source_root / "archive"
                cold.mkdir(parents=True)
                (cold / "catalog.json").write_text(json.dumps({"artifacts": []}))
                spool_source = source_root / "spool/clickhouse"
                (spool_source / "replayed").mkdir(parents=True)
                (spool_source / "replayed/marker.json").write_text(json.dumps({"done": True}))
                components = [
                    BackupComponent.json(
                        "postgresql", "logical-json", "postgresql-schema-v1", {},
                        watermark,
                    ),
                    BackupComponent.json(
                        "clickhouse", "logical-json", "clickhouse-schema-v1", {},
                        watermark, True,
                    ),
                    BackupComponent.tree("duckdb", "duckdb-file", "1",
                                         source_root / "warehouse", source_root, watermark),
                    BackupComponent.tree("cold_archive", "parquet-tree", "manifest-v1",
                                         cold, source_root, watermark),
                    BackupComponent.tree("spool", "wal-tree", "spool-v1",
                                         spool_source, source_root, watermark, True),
                    BackupComponent("cursor_key", "sealed-secret", "cursor-v1",
                                    {"cursor.key": b"k" * 48}, watermark),
                ]
                backup = LocalStorageBackup(
                    source_root / "backups", source_root, b"w" * 32,
                )
                backup_result = backup.create(components, "2026-07-20T00:01:01Z")
                artifact = Path(backup_result["artifact_path"])
                restored_root = root / "restored-test"
                restore = LocalStorageRestore(backup, RestoreTargets(
                    restored_root, postgres, clickhouse, "test", root,
                ))
                restore.restore([artifact])
                restore_report = restore.record_verification({
                    "bundle": "verified", "empty_environment": "verified",
                })
                restored = Warehouse(restored_root / "warehouse/market_data.duckdb")
                repository = ClickHouseMarketBarRepository(clickhouse)
                spool = LocalClickHouseSpool(restored_root / "spool/clickhouse", root)
                writer = ReliableClickHouseWriter(repository, spool, 1000)
                builder = CanonicalMarketBarBuilder(repository, writer)

                def target_contract():
                    expected = restored.get_raw_price_bars_range(
                        "MU", "1d", "none", "2026-07-19T00:00:00Z",
                        "2026-07-21T00:00:00Z", 100,
                    )
                    actual = repository.get_raw_price_bars_range(
                        "MU", "1d", "none", "2026-07-19T00:00:00Z",
                        "2026-07-21T00:00:00Z", 100,
                    )
                    pg_nonempty = 0
                    with postgres.connection() as connection:
                        for domain in __import__(
                            "marketcow.local_backfill", fromlist=["POSTGRES_DOMAINS"]
                        ).POSTGRES_DOMAINS:
                            pg_nonempty += int(connection.execute(
                                f'SELECT count(*) AS count FROM "{domain.table}"'
                            ).fetchone()["count"] > 0)
                    comparison = compare_contract(expected, actual, LEGACY_PAYLOAD_PATHS)
                    return {"status": "ok" if comparison["status"] == "ok" and
                            pg_nonempty == 16 else "mismatch", "checks": 17}

                backfill = LocalStorageBackfill(restored, BackfillTargets(
                    root / "backfill-test", postgres, clickhouse, writer, builder,
                    "test", root, contract_gate=target_contract,
                ), 2)
                backfill_result = backfill.run()
                self.assertEqual(backfill_result["lag"], 0)
                backfill_report_path = backfill.state / "report.json"
                restore_report_path = restore.state_root / "report.json"
                adapter = ShadowMarketBarRepository(restored, writer, builder)

                def golden(_backend):
                    pairs = [
                        (restored.get_price_bars_page(
                            "MU", "1d", "none", "2026-07-19T00:00:00Z",
                            "2026-07-21T00:00:00Z", 10, None,
                        ), adapter.get_price_bars_page(
                            "MU", "1d", "none", "2026-07-19T00:00:00Z",
                            "2026-07-21T00:00:00Z", 10, None,
                        )),
                        (restored.get_raw_price_bars_page(
                            "MU", "1d", "none", "2026-07-19T00:00:00Z",
                            "2026-07-21T00:00:00Z", 10, None, None,
                        ), adapter.get_raw_price_bars_page(
                            "MU", "1d", "none", "2026-07-19T00:00:00Z",
                            "2026-07-21T00:00:00Z", 10, None, None,
                        )),
                    ]
                    ok = all(compare_contract(left, right, LEGACY_PAYLOAD_PATHS)[
                        "status"] == "ok" for left, right in pairs)
                    return {"status": "ok" if ok else "mismatch", "samples": 2,
                            "fallbacks": int(bool(adapter._last_read.get("fallback")))}

                def gate():
                    checkpoint = json.loads(backfill.checkpoint_path.read_text())
                    report = json.loads(backfill_report_path.read_text())
                    contract = target_contract()["status"]
                    canonical_pending = 0
                    for pending in spool.pending.glob("*.json"):
                        try:
                            canonical_pending += int(
                                json.loads(pending.read_text()).get("dataset") == "canonical"
                            )
                        except (OSError, ValueError):
                            canonical_pending += 1
                    return {
                        "lag": report["lag"] if checkpoint["phase"] == "complete" else 1,
                        "reconcile": "ok" if not report["mismatches"] else "mismatch",
                        "contract": contract,
                        "spool_pending": spool.diagnostics()["pending"],
                        "canonical_queue": canonical_pending,
                        "readiness": "healthy" if clickhouse.diagnostics()["status"] == "ok"
                        else "unavailable",
                    }

                restore_id = _hash(_json(restore_report))[:24]
                inputs = ReadSwitchInputs(
                    root / "switch-test", adapter, backfill.checkpoint_path,
                    backfill_report_path, restore_report_path,
                    backup_result["backup_id"], restore_id, "test", root, gate, golden,
                )
                drill = LocalReadSwitchDrill(inputs)
                self.assertEqual(drill.run(1)["final_backend"], "clickhouse")
                self.assertEqual(LocalReadSwitchDrill(inputs).run(1)["final_backend"],
                                 "clickhouse")
                rollback_report = drill.rollback("readiness_evidence")
                self.assertEqual(rollback_report["final_backend"], "duckdb")
                if export_root := os.getenv("MARKETCOW_READINESS_EVIDENCE_ROOT"):
                    target = Path(export_root) / "SV2-022B"
                    target.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(drill.report_path, target / "report.json")
                    shutil.copy2(drill.checkpoint_path, target / "checkpoint.json")
                tampered = json.loads(restore_report_path.read_text())
                tampered["verification"]["bundle"] = "tampered"
                restore_report_path.write_text(json.dumps(tampered))
                with self.assertRaisesRegex(ValueError, "binding mismatch"):
                    LocalReadSwitchDrill(inputs)
                self.assertFalse(adapter.canonical_reads_enabled)
                self.assertFalse(adapter.raw_reads_enabled)
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
