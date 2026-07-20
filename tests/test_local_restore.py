import json
import os
import tempfile
import unittest
import uuid
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
        self.assertEqual(restore.restore([self.artifact])["steps"], report["steps"])

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

        from marketcow.clickhouse_repositories import (
            ClickHouseDatabase, ClickHouseMarketBarRepository,
        )
        from marketcow.postgres_repositories import PostgresDatabase

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
            source_ch.open()
            ClickHouseMarketBarRepository(source_ch).insert_raw_bars([{
                "symbol": "RESTORE", "market": "US", "interval": "1m",
                "adjustment": "raw", "bar_time": "2026-07-20T00:00:00Z",
                "open": 1, "high": 2, "low": 0.5, "close": 1.5,
                "raw_close": 1.5, "adjustment_factor": 1, "volume": 10,
                "amount": 15, "source": "fixture", "source_sequence": "1",
                "observed_at": "2026-07-20T00:00:01Z",
                "ingested_at": "2026-07-20T00:00:02Z",
                "raw_artifact_id": "restore-artifact",
            }])
            captured = "2026-07-20T00:00:03Z"
            watermark = {"captured_at": captured}
            components = [
                BackupComponent.postgresql(source_pg, captured),
                BackupComponent.clickhouse(source_ch, captured),
                BackupComponent("duckdb", "duckdb-file", "1",
                                {"market_data.duckdb": b"synthetic-duckdb"}, watermark),
                BackupComponent("cold_archive", "parquet-tree", "manifest-v1",
                                {"fixture/data.parquet": b"PAR1fixture"}, watermark),
                BackupComponent("spool", "wal-tree", "spool-v1",
                                {"pending/raw.json": b'{"batch":"fixture"}'},
                                watermark, True),
                BackupComponent("cursor_key", "sealed-secret", "cursor-v1",
                                {"cursor.key": b"c" * 48}, watermark),
            ]
            with tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                source_root = root / "backup-development"
                source_root.mkdir()
                backup = LocalStorageBackup(source_root / "backups", source_root,
                                            b"w" * 32)
                artifact = Path(backup.create(
                    components, "2026-07-20T00:00:04Z"
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
                count = target_ch.client.query(
                    "SELECT count() FROM market_bar_raw FINAL WHERE symbol='RESTORE'"
                ).result_rows[0][0]
                self.assertEqual(count, 1)
                self.assertEqual((target_root / ".market-bar-cursor.key").read_bytes(),
                                 b"c" * 48)
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
