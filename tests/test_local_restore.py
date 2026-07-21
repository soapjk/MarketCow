import json
import os
import shutil
import tempfile
import unittest
import uuid
from datetime import datetime
from pathlib import Path

from marketcow.local_backup import BackupComponent, LocalStorageBackup
from marketcow.local_restore import LocalStorageRestore, RestoreTargets
from marketcow.market_bar_cursor import decode_cursor, encode_cursor


class _Result:
    def __init__(self, one=None):
        self.one = one or {"count": 0}

    def fetchone(self):
        return self.one


class _Connection:
    def execute(self, *_args, **_kwargs):
        return _Result()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


class _Pool:
    def connection(self):
        return _Connection()


class _Postgres:
    schema = "restore_fixture_test"
    pool = _Pool()

    def migrate(self):
        return None


class _Query:
    result_rows = []


class _ClickHouseClient:
    def query(self, _query):
        return _Query()


class _ClickHouse:
    database = "restore_fixture_test"
    client = _ClickHouseClient()

    def _require_client(self):
        return self.client

    def migrate(self):
        return None


class LocalStorageRestoreTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        base = Path(self.folder.name)
        self.source = base / "source-development"
        self.source.mkdir()
        self.wrapping = b"w" * 32
        self.backup = LocalStorageBackup(
            self.source / "backups", self.source, self.wrapping
        )
        self.cursor = b"cursor-restore-key-with-more-than-32-bytes"
        self.artifact = Path(self.backup.create(
            self.components(), "2026-07-20T00:00:01Z"
        )["artifact_path"])
        self.target = base / "empty-test"

    def tearDown(self):
        self.folder.cleanup()

    def components(self, suffix=""):
        watermark = {"captured_at": "2026-07-20T00:00:00Z"}
        return [
            BackupComponent.json("postgresql", "logical-json", "postgresql-schema-v1",
                                 {}, watermark),
            BackupComponent.json("clickhouse", "logical-json", "clickhouse-schema-v1",
                                 {}, watermark, True),
            BackupComponent("duckdb", "duckdb-file", "1",
                            {"market_data.duckdb": b"duckdb" + suffix.encode()}, watermark),
            BackupComponent("cold_archive", "parquet-tree", "manifest-v1",
                            {"part/data.parquet": b"PAR1" + suffix.encode()}, watermark),
            BackupComponent("spool", "wal-tree", "spool-v1",
                            {"pending/item.json": b'{"batch":"one"}'}, watermark, True),
            BackupComponent("cursor_key", "sealed-secret", "cursor-v1",
                            {"cursor.key": self.cursor}, watermark),
        ]

    def restore(self, backup=None, target=None):
        backup = backup or self.backup
        return LocalStorageRestore(
            backup,
            RestoreTargets(target or self.target, _Postgres(), _ClickHouse(), "test",
                           Path(self.folder.name)),
        )

    def test_empty_restore_files_cursor_report_and_repeat_are_idempotent(self):
        binding = {"symbol": "RESTORE", "page_size": 10}
        old_cursor = encode_cursor(binding, "2026-07-20T00:00:00Z", 100,
                                   self.cursor.decode())
        restore = self.restore()
        report = restore.restore([self.artifact])
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["steps"], [
            "postgresql", "clickhouse", "duckdb", "cold_archive", "spool", "cursor_key",
        ])
        self.assertEqual((self.target / "warehouse/market_data.duckdb").read_bytes(),
                         b"duckdb")
        self.assertEqual((self.target / "archive/part/data.parquet").read_bytes(), b"PAR1")
        key = self.target / ".market-bar-cursor.key"
        self.assertEqual(key.read_bytes(), self.cursor)
        self.assertEqual(key.stat().st_mode & 0o777, 0o600)
        self.assertEqual(
            decode_cursor(old_cursor, binding, 101, 3600, key.read_text()),
            "2026-07-20T00:00:00Z",
        )
        rendered = (self.target / ".storage-v2-restore/report.json").read_text()
        self.assertNotIn(str(self.target), rendered)
        verified = restore.record_verification({"contract_gate": "ok"})
        self.assertEqual(verified["verification"], {"contract_gate": "ok"})
        with self.assertRaisesRegex(ValueError, "sensitive"):
            restore.record_verification({"authorization": "Bearer leaked"})
        repeated = restore.restore([self.artifact])
        self.assertEqual(repeated["steps"], report["steps"])
        self.assertEqual(repeated["verification"], {"contract_gate": "ok"})

    def test_every_component_boundary_recovers_from_durable_checkpoint(self):
        for fail_name in ("postgresql", "clickhouse", "duckdb", "cold_archive", "spool",
                          "cursor_key"):
            with self.subTest(component=fail_name):
                target = Path(self.folder.name) / f"boundary-{fail_name}-test"
                restore = self.restore(target=target)
                fired = {"value": False}

                def fault(stage, name):
                    if stage == "after_write" and name == fail_name and not fired["value"]:
                        fired["value"] = True
                        raise RuntimeError("injected crash")

                with self.assertRaisesRegex(RuntimeError, "injected"):
                    restore.restore([self.artifact], fault)
                report = self.restore(target=target).restore([self.artifact])
                self.assertEqual(report["status"], "complete")

    def test_preflight_wrong_key_nonempty_path_and_symlink_write_nothing(self):
        wrong = LocalStorageBackup(self.source / "backups", self.source, b"x" * 32)
        with self.assertRaisesRegex(ValueError, "authentication"):
            self.restore(backup=wrong).restore([self.artifact])
        self.assertFalse(self.target.exists())
        self.target.mkdir()
        (self.target / "existing").write_text("data")
        with self.assertRaisesRegex(ValueError, "empty"):
            self.restore().restore([self.artifact])
        self.assertEqual((self.target / "existing").read_text(), "data")
        link = Path(self.folder.name) / "linked-test"
        link.symlink_to(self.target)
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.restore(target=link)
        outside = Path(self.folder.name).parent / f"outside-{uuid.uuid4().hex}-test"
        parent_link = Path(self.folder.name) / "parent-link"
        parent_link.symlink_to(outside)
        try:
            with self.assertRaisesRegex(ValueError, "escapes"):
                self.restore(target=parent_link / "escaped-test")
        finally:
            parent_link.unlink()

    def test_incremental_chain_order_version_and_checkpoint_binding(self):
        first = self.backup.verify(self.artifact)
        incremental = Path(self.backup.create(
            self.components("-next"), "2026-07-20T00:00:02Z", "incremental",
            first["backup_id"],
        )["artifact_path"])
        report = self.restore().restore([self.artifact, incremental])
        self.assertEqual(report["backup_chain"], [first["backup_id"], incremental.name])
        self.assertEqual((self.target / "warehouse/market_data.duckdb").read_bytes(),
                         b"duckdb-next")
        other = Path(self.folder.name) / "order-test"
        with self.assertRaisesRegex(ValueError, "begin with a full"):
            self.restore(target=other).restore([incremental])

        future_components = self.components()
        future_components[0] = BackupComponent.json(
            "postgresql", "logical-json", "future-schema-v999", {},
            {"captured_at": "2026-07-20T00:00:00Z"},
        )
        future_artifact = Path(self.backup.create(
            future_components, "2026-07-20T00:00:03Z"
        )["artifact_path"])
        with self.assertRaisesRegex(ValueError, "unsupported"):
            self.restore(target=Path(self.folder.name) / "future-test").restore(
                [future_artifact]
            )

        manifest = self.artifact / "manifest.json"
        document = json.loads(manifest.read_text())
        document["components"][0]["version"] = "future-v999"
        # The untrusted edit is rejected by the bundle checksum before version handling.
        manifest.write_text(json.dumps(document))
        with self.assertRaisesRegex(ValueError, "manifest checksum"):
            self.restore(target=Path(self.folder.name) / "version-test").restore(
                [self.artifact]
            )

    def test_production_profile_and_nonisolated_root_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "development/test-only"):
            LocalStorageRestore(self.backup, RestoreTargets(
                Path(self.folder.name) / "production", profile="production",
                allowed_root=Path(self.folder.name),
            ))
        with self.assertRaisesRegex(ValueError, "isolated"):
            self.restore(target=Path(self.folder.name) / "arbitrary")


@unittest.skipUnless(
    os.getenv("MARKETCOW_TEST_POSTGRES_DSN") and
    os.getenv("MARKETCOW_TEST_CLICKHOUSE_HOST"),
    "set PostgreSQL and ClickHouse integration settings to run restore drill",
)
class LocalStorageRestoreIntegrationTest(unittest.TestCase):
    def test_real_postgres_clickhouse_empty_environment_restore(self):
        import clickhouse_connect
        import psycopg
        from fastapi.testclient import TestClient

        from marketcow.api import create_app
        from marketcow.clickhouse_canonical import CanonicalMarketBarBuilder
        from marketcow.clickhouse_repositories import (
            ClickHouseDatabase, ClickHouseMarketBarRepository,
        )
        from marketcow.clickhouse_writer import (
            LocalClickHouseSpool, ReliableClickHouseWriter, normalize_bar,
        )
        from marketcow.cold_archive import ParquetColdArchive
        from marketcow.config import Settings
        from marketcow.contract_gate import LEGACY_PAYLOAD_PATHS, assert_contract_equal
        from marketcow.postgres_repositories import (
            PostgresDatabase, PostgresFundamentalRepository,
        )
        from marketcow.storage import Warehouse

        class FailingRepository:
            def insert_raw_bars(self, _rows, batch_id=""):
                raise ConnectionError("synthetic ClickHouse outage")

        class RestoredService:
            def __init__(self, repository):
                self.market_bar_repository = repository

            def close(self):
                return None

            def refresh_quote_history(self, *_args):
                raise AssertionError("restored cached query must not refresh upstream")

        dsn = os.environ["MARKETCOW_TEST_POSTGRES_DSN"]
        host = os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"]
        port = int(os.getenv("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123"))
        username = os.getenv("MARKETCOW_TEST_CLICKHOUSE_USERNAME", "default")
        password = os.getenv("MARKETCOW_TEST_CLICKHOUSE_PASSWORD", "")
        suffix = uuid.uuid4().hex[:10]
        source_schema = f"restore_source_{suffix}_test"
        target_schema = f"restore_target_{suffix}_test"
        source_ch_name = f"restore_source_{suffix}_test"
        target_ch_name = f"restore_target_{suffix}_test"
        source_pg = PostgresDatabase(dsn, source_schema, 1, 1)
        target_pg = PostgresDatabase(dsn, target_schema, 1, 1)
        source_ch = ClickHouseDatabase(host, port, source_ch_name, username, password)
        target_ch = ClickHouseDatabase(host, port, target_ch_name, username, password)
        bootstrap = None
        try:
            source_pg.open()
            with source_pg.connection() as connection:
                connection.execute(
                    "INSERT INTO provider_health "
                    "(provider,status,last_attempt_at,consecutive_failures) "
                    "VALUES (%s,%s,%s,%s)",
                    ("restore-fixture", "healthy", "2026-07-20T00:00:00Z", 0),
                )
                connection.execute(
                    "INSERT INTO raw_artifact_manifest "
                    "(artifact_id,dataset,source,observed_at,ingested_at,storage_path,"
                    "sha256,byte_size,metadata_json) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
                    ("restore-artifact", "market_price_bar_raw", "fixture",
                     "2026-07-01T00:00:01Z", "2026-07-01T00:00:02Z",
                     "cold://restore-artifact", "fixture-sha256", 1, '{}'),
                )
            source_fundamentals = PostgresFundamentalRepository(source_pg)
            fundamental = {
                "symbol": "RESTORE", "is_active": True,
                "report_period": "20260331", "published_at": "2026-04-30",
                "price": 10.0, "source": "fixture",
                "observed_at": "2026-05-01", "ingested_at": "2026-05-01",
                "fetched_at": "2026-05-01",
            }
            source_fundamentals.replace_fundamentals("20260331", [fundamental])
            source_fundamentals.replace_fundamentals("20260331", [{
                **fundamental, "price": 20.0, "observed_at": "2026-07-01",
                "ingested_at": "2026-07-01", "fetched_at": "2026-07-01",
            }])
            source_ch.open()
            source_ch_repository = ClickHouseMarketBarRepository(source_ch)
            source_ch_repository.insert_raw_bars([{
                "symbol": "RESTORE", "market": "US", "interval": "1m",
                "adjustment": "raw", "bar_time": "2026-07-01T00:00:00Z",
                "open": 1, "high": 2, "low": 0.5, "close": 1.5,
                "raw_close": 1.5, "adjustment_factor": 1, "volume": 10,
                "amount": 15, "source": "fixture", "source_sequence": "1",
                "observed_at": "2026-07-01T00:00:01Z",
                "ingested_at": "2026-07-01T00:00:02Z",
                "raw_artifact_id": "restore-artifact",
            }])
            with tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                source_root = root / "backup-development"
                source_root.mkdir()
                source_warehouse = Warehouse(
                    source_root / "warehouse/market_data.duckdb"
                )
                bar_at = "2026-07-01T00:00:00Z"
                bar_epoch = int(datetime.fromisoformat(
                    bar_at.replace("Z", "+00:00")
                ).timestamp())
                source_warehouse.upsert_price_bars(
                    "RESTORE", "1m", "raw", "fixture",
                    "2026-07-01T00:00:02Z", [
                        {
                            "timestamp": bar_epoch, "bar_at": bar_at,
                            "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
                            "raw_close": 1.5, "adjustment_factor": 1.0,
                            "volume": 10.0, "amount": 15.0,
                            "source_payload": {"fixture": True},
                        },
                        {
                            "timestamp": bar_epoch + 30,
                            "bar_at": "2026-07-01T00:00:30Z",
                            "open": 2.0, "high": 3.0, "low": 1.5, "close": 2.5,
                            "raw_close": 2.5, "adjustment_factor": 1.0,
                            "volume": 20.0, "amount": 50.0,
                            "source_payload": {"fixture": True},
                        },
                    ], {"observed_at": "2026-07-01T00:00:01Z",
                         "raw_artifact_id": "restore-artifact"},
                )
                source_archive = ParquetColdArchive(
                    source_warehouse.path, source_root / "archive", source_root
                )
                cold_result = source_archive.export_partition(
                    "US", "1m", "fixture", 2026, 7
                )
                cold_relative = Path(cold_result["artifact_path"]).relative_to(
                    source_archive.archive_root
                )

                source_spool = LocalClickHouseSpool(
                    source_root / "spool/clickhouse", source_root
                )
                pending_bar = normalize_bar("raw", {
                    "symbol": "SPOOL", "market": "US", "interval": "1m",
                    "adjustment": "raw", "bar_time": "2026-07-01T00:00:30Z",
                    "open": 3, "high": 4, "low": 2, "close": 3.5,
                    "raw_close": 3.5, "adjustment_factor": 1, "volume": 20,
                    "amount": 70, "source": "fixture", "source_sequence": "1",
                    "observed_at": "2026-07-01T00:00:31Z",
                    "ingested_at": "2026-07-01T00:00:32Z",
                    "raw_artifact_id": "spool-artifact",
                })
                failed = ReliableClickHouseWriter(
                    FailingRepository(), source_spool, 1000
                ).write("raw", [pending_bar])
                self.assertEqual(failed["spooled"], 1)

                captured = "2026-07-01T00:01:00Z"
                watermark = {"captured_at": captured}
                components = [
                    BackupComponent.postgresql(source_pg, captured),
                    BackupComponent.clickhouse(source_ch, captured),
                    BackupComponent.tree(
                        "duckdb", "duckdb-file", "1", source_root / "warehouse",
                        source_root, watermark,
                    ),
                    BackupComponent.tree(
                        "cold_archive", "parquet-tree", "manifest-v1",
                        source_root / "archive", source_root, watermark,
                    ),
                    BackupComponent.tree(
                        "spool", "wal-tree", "spool-v1",
                        source_root / "spool/clickhouse", source_root, watermark, True,
                    ),
                    BackupComponent("cursor_key", "sealed-secret", "cursor-v1",
                                    {"cursor.key": b"c" * 48}, watermark),
                ]
                backup = LocalStorageBackup(source_root / "backups", source_root,
                                            b"w" * 32)
                artifact = Path(backup.create(
                    components, "2026-07-01T00:01:01Z"
                )["artifact_path"])
                target_pg.pool.open(wait=True)
                bootstrap = clickhouse_connect.get_client(
                    host=host, port=port, username=username, password=password,
                    database="default",
                )
                bootstrap.command(f"CREATE DATABASE `{target_ch_name}`")
                target_ch.client = target_ch._connect(target_ch_name)
                target_root = root / "restored-test"
                report = LocalStorageRestore(backup, RestoreTargets(
                    target_root, target_pg, target_ch, "test", root
                )).restore([artifact])
                self.assertEqual(report["status"], "complete")
                with target_pg.connection() as connection:
                    count = connection.execute(
                        "SELECT count(*) AS count FROM provider_health "
                        "WHERE provider='restore-fixture'"
                    ).fetchone()["count"]
                self.assertEqual(count, 1)
                with target_pg.connection() as connection:
                    artifact_count = connection.execute(
                        "SELECT count(*) AS count FROM raw_artifact_manifest "
                        "WHERE artifact_id='restore-artifact' AND "
                        "storage_path='cold://restore-artifact'"
                    ).fetchone()["count"]
                self.assertEqual(artifact_count, 1)
                target_fundamentals = PostgresFundamentalRepository(target_pg)
                self.assertEqual(target_fundamentals.query_fundamentals(
                    symbol="RESTORE", as_of="2026-06-01", limit=1
                )[0]["price"], 10.0)

                target_ch_repository = ClickHouseMarketBarRepository(target_ch)
                count = target_ch.client.query(
                    "SELECT count() FROM market_bar_raw FINAL WHERE symbol='RESTORE'"
                ).result_rows[0][0]
                self.assertEqual(count, 1)
                restored_warehouse = Warehouse(
                    target_root / "warehouse/market_data.duckdb"
                )
                duckdb_bars = restored_warehouse.get_price_bars_range(
                    "RESTORE", "1m", "raw", "2026-07-01T00:00:00Z",
                    "2026-07-01T00:01:00Z", 10,
                )[0]
                self.assertEqual(len(duckdb_bars), 2)
                self.assertEqual(restored_warehouse.get_price_bar_as_of(
                    "RESTORE", "1m", "raw", "2026-07-01T00:00:10Z", 60
                )["close"], 1.5)

                restored_archive = ParquetColdArchive(
                    restored_warehouse.path, target_root / "archive", target_root
                )
                restored_cold_artifact = restored_archive.archive_root / cold_relative
                self.assertEqual(restored_archive.verify(
                    restored_cold_artifact
                )["status"], "verified")
                cold_rows = restored_archive.query(
                    restored_cold_artifact, "symbol=?", ["RESTORE"]
                )
                self.assertEqual(restored_archive.read_for_backfill(
                    restored_cold_artifact
                ), cold_rows)
                self.assertEqual(cold_rows[0]["close"], duckdb_bars[0]["close"])

                restored_spool = LocalClickHouseSpool(
                    target_root / "spool/clickhouse", target_root
                )
                restored_writer = ReliableClickHouseWriter(
                    target_ch_repository, restored_spool, 1000
                )
                builder = CanonicalMarketBarBuilder(
                    target_ch_repository, restored_writer
                )

                def rebuild_replayed(rows):
                    self.assertLessEqual(
                        max(row["bar_time"] for row in rows),
                        report["watermark"]["latest_captured_at"],
                    )
                    for symbol in sorted({row["symbol"] for row in rows}):
                        result = builder.rebuild(
                            symbol, "1m", "raw", "2026-07-01T00:00:00Z",
                            report["watermark"]["latest_captured_at"], 100,
                        )
                        self.assertEqual(result["status"], "ok", result)

                restored_writer.on_raw_replayed = rebuild_replayed
                first_replay = restored_writer.replay(10)
                second_replay = restored_writer.replay(10)
                self.assertEqual(first_replay["replayed"], 1)
                self.assertEqual(first_replay["callback_ok"], 1)
                self.assertEqual(first_replay["remaining"], 0)
                self.assertEqual(second_replay["replayed"], 0)
                self.assertEqual(second_replay["callback_attempted"], 0)
                self.assertEqual(target_ch.client.query(
                    "SELECT count() FROM market_bar_raw FINAL WHERE symbol='SPOOL'"
                ).result_rows[0][0], 1)

                # A post-snapshot row must not enter the bounded canonical rebuild.
                target_ch_repository.insert_raw_bars([{
                    **pending_bar, "symbol": "FUTURE",
                    "bar_time": "2026-07-01T00:02:00Z",
                    "observed_at": "2026-07-01T00:02:01Z",
                    "ingested_at": "2026-07-01T00:02:02Z",
                    "raw_artifact_id": "future-artifact",
                }])
                restored_raw = target_ch_repository.query_raw_bars("RESTORE")
                self.assertEqual(len(restored_raw), 1, restored_raw)
                self.assertEqual(restored_raw[0]["interval"], "1m", restored_raw)
                self.assertEqual(restored_raw[0]["adjustment"], "raw", restored_raw)
                self.assertEqual(str(restored_raw[0]["bar_time"])[:16],
                                 "2026-07-01 00:00", restored_raw)
                for symbol in ("RESTORE", "SPOOL"):
                    rebuilt = builder.rebuild(
                        symbol, "1m", "raw", "2026-07-01T00:00:00Z",
                        report["watermark"]["latest_captured_at"], 100,
                    )
                    self.assertEqual(rebuilt["status"], "ok", rebuilt)
                    self.assertEqual(rebuilt["written"], 1, rebuilt)
                self.assertEqual(target_ch.client.query(
                    "SELECT count() FROM market_bar_canonical FINAL "
                    "WHERE symbol='FUTURE'"
                ).result_rows[0][0], 0)
                canonical, _ = target_ch_repository.query_range(
                    "canonical", "RESTORE", "1m", "raw",
                    "2026-07-01T00:00:00Z", "2026-07-01T00:01:00Z", 10,
                )
                comparable = lambda row: {key: row[key] for key in (
                    "symbol", "interval", "adjustment", "open", "high", "low",
                    "close", "raw_close", "adjustment_factor", "volume", "amount",
                )}
                assert_contract_equal(
                    {"bars": [comparable(duckdb_bars[0])]},
                    {"bars": [comparable(canonical[0])]},
                    "restored DuckDB/ClickHouse golden", LEGACY_PAYLOAD_PATHS,
                )

                settings = Settings(
                    database_path=restored_warehouse.path,
                    raw_path=target_root / "raw", profile="development",
                    storage_root=target_root, market_bar_cursor_secret="",
                    market_bar_cache_freshness_seconds=3600,
                )
                app = create_app(
                    settings, RestoredService(restored_warehouse),
                    now_provider=lambda: datetime.fromisoformat(
                        "2026-07-01T00:10:00+00:00"
                    ),
                )
                with TestClient(app) as client:
                    response = client.get(
                        "/v1/quotes/RESTORE/history?interval=1m&adjustment=raw&"
                        "start=2026-07-01T00:00:00Z&end=2026-07-01T00:01:00Z&"
                        "page_size=1&refresh=false"
                    )
                self.assertEqual(response.status_code, 200)
                payload = response.json()
                self.assertEqual(payload["count"], 1)
                self.assertEqual(payload["cache_status"], "fresh")
                self.assertIsNotNone(payload["next_cursor"])
                with TestClient(app) as client:
                    second = client.get(
                        "/v1/quotes/RESTORE/history?interval=1m&adjustment=raw&"
                        "start=2026-07-01T00:00:00Z&end=2026-07-01T00:01:00Z&"
                        "page_size=1&refresh=false&cursor=" + payload["next_cursor"]
                    )
                self.assertEqual(second.status_code, 200)
                self.assertEqual(second.json()["bars"][0]["close"], 2.5)
                self.assertIsNone(second.json()["next_cursor"])
                self.assertEqual((target_root / ".market-bar-cursor.key").read_bytes(),
                                 b"c" * 48)
                report_document = LocalStorageRestore(
                    backup, RestoreTargets(target_root, target_pg, target_ch, "test", root)
                ).record_verification({
                    "postgres_pit": "ok", "clickhouse_raw": "ok",
                    "duckdb_api_contract": "ok", "cold_verify_query_backfill": "ok",
                    "spool_replay_once": "ok", "canonical_boundary": "ok",
                })
                report_path = target_root / ".storage-v2-restore/report.json"
                rendered = report_path.read_text()
                self.assertEqual(report_document["verification"]["canonical_boundary"],
                                 "ok")
                self.assertNotIn(str(target_root), rendered)
                self.assertNotIn(password, rendered)
                if export_root := os.getenv("MARKETCOW_READINESS_EVIDENCE_ROOT"):
                    target = Path(export_root)
                    backup_target = target / "SV2-021A" / artifact.name
                    backup_target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copytree(artifact, backup_target, dirs_exist_ok=True)
                    key_path = target / "SV2-021A/wrapping.key"
                    key_path.write_bytes(b"w" * 32)
                    os.chmod(key_path, 0o600)
                    restore_target = target / "SV2-021B"
                    restore_target.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(report_path, restore_target / "report.json")
        finally:
            source_pg.close()
            target_pg.close()
            source_ch.close()
            target_ch.close()
            if bootstrap is None:
                bootstrap = clickhouse_connect.get_client(
                    host=host, port=port, username=username, password=password,
                    database="default",
                )
            bootstrap.command(f"DROP DATABASE IF EXISTS `{source_ch_name}`")
            bootstrap.command(f"DROP DATABASE IF EXISTS `{target_ch_name}`")
            bootstrap.close()
            with psycopg.connect(dsn, autocommit=True) as connection:
                connection.execute(f'DROP SCHEMA IF EXISTS "{source_schema}" CASCADE')
                connection.execute(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')


if __name__ == "__main__":
    unittest.main()
