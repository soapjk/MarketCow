import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from marketcow.local_backup import BackupComponent, _hash, _json
from marketcow.local_restore import RestoreTargets
from marketcow.offline_incremental_catchup import CATCHUP_VERSION
from marketcow.postgres_migrations import POSTGRES_TRANSACTION_DOMAINS
from marketcow.v2_backup_restore import (
    V2LocalBackup,
    V2LocalRestore,
    V2_BACKUP_MANIFEST_VERSION,
    V2_COMPONENT_ORDER,
)


class V2BackupRestoreTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name).resolve()
        self.storage = self.root / "v2-backup-test"
        self.storage.mkdir()
        self.captured = "2026-07-21T00:00:00Z"
        self.watermark = {"captured_at": self.captured}
        self.key = b"w" * 32
        self.backup = V2LocalBackup(
            self.storage / "backups", self.storage, self.key, "test"
        )

    def tearDown(self):
        self.folder.cleanup()

    def _migration(self):
        document = {
            "version": CATCHUP_VERSION,
            "phase": "complete",
            "catchup_run_id": "bg014-synthetic",
            "source_high_watermark": {"source_fingerprint": "f" * 64},
            "lag": 0,
            "verified_raw_max_ingested_at": "2026-07-21T00:00:00Z",
            "canonical_rebuild_through": "2026-07-21T00:00:00Z",
        }
        document["checksum"] = _hash(_json(document))
        return document

    def components(self):
        pg = {domain: [] for domain in POSTGRES_TRANSACTION_DOMAINS}
        pg["schema_migrations"] = [{"version": 5}]
        ch = {
            "schema_migrations": {"columns": ["version"], "rows": [[3]]},
            "market_bar_raw": {
                "columns": ["symbol", "ingested_at"],
                "rows": [["SYNTH", "2026-07-21T00:00:00Z"]],
            },
            "market_bar_canonical": {
                "columns": ["symbol", "ingested_at"],
                "rows": [["SYNTH", "2026-07-21T00:00:00Z"]],
            },
        }
        return [
            BackupComponent.json(
                "postgresql", "logical-json", "postgres-16-v2-18-domains",
                pg, self.watermark,
            ),
            BackupComponent.json(
                "clickhouse", "logical-json", "clickhouse-25.8-raw-canonical",
                ch, self.watermark, True,
            ),
            BackupComponent(
                "artifact_archive", "artifact-parquet-tree", "artifact-parquet-v1",
                {"manifest.json": b'{"version":"cold-v1"}', "data.parquet": b"PAR1synthetic"},
                self.watermark,
            ),
            BackupComponent(
                "authoritative_spool", "wal-intent-tree", "authoritative-spool-v2",
                {"pending/state.json": b'{"_checksum":"synthetic","state":"pending"}'},
                self.watermark, True,
            ),
            BackupComponent(
                "scheduler_state", "scheduler-tree", "canonical-scheduler-v1",
                {"queue/checkpoint.json": b'{"_checksum":"synthetic","pending":[]}'},
                self.watermark, True,
            ),
            BackupComponent.json(
                "config_version", "logical-json", "v2-config-version-v1",
                {"version": "v2-test-config-1"}, self.watermark,
            ),
            BackupComponent.json(
                "migration_watermark", "logical-json", CATCHUP_VERSION,
                self._migration(), self.watermark, True,
            ),
            BackupComponent(
                "cursor_key", "sealed-secret", "cursor-v1",
                {"cursor.key": b"c" * 48}, self.watermark,
            ),
        ]

    def create(self, **kwargs):
        return self.backup.create(
            self.components(), "2026-07-21T00:00:01Z", **kwargs
        )

    def restore(self, artifacts, target=None, fault_hook=None):
        target = target or self.root / "restored-v2-test"
        drill = V2LocalRestore(
            self.backup, RestoreTargets(target, profile="test", allowed_root=self.root)
        )
        with (
            patch.object(drill, "_restore_postgres"),
            patch.object(drill, "_restore_clickhouse"),
        ):
            return drill, drill.restore(artifacts, fault_hook=fault_hook)

    def test_pure_pg_ch_bundle_restore_and_verification(self):
        created = self.create()
        self.assertEqual(created["manifest_version"], V2_BACKUP_MANIFEST_VERSION)
        self.assertEqual(
            {item["name"] for item in created["components"]}, set(V2_COMPONENT_ORDER)
        )
        self.assertNotIn("duckdb", json.dumps(created).lower())
        artifact = Path(created["artifact_path"])
        drill, report = self.restore([artifact])
        self.assertEqual(report["steps"], list(V2_COMPONENT_ORDER))
        self.assertEqual((drill.root / ".market-bar-cursor.key").read_bytes(), b"c" * 48)
        verified = drill.record_v2_verification({
            "postgres_18_domains": "ok", "postgres_pit": "ok",
            "clickhouse_raw_final": "ok", "clickhouse_canonical": "ok",
            "market_api_contract": "ok", "pagination_cursor_cache": "ok",
            "artifact_parquet_query": "ok", "spool_replay_once": "ok",
            "canonical_boundary": "ok",
        })
        self.assertEqual(verified["verification"]["canonical_boundary"], "ok")

    def test_missing_component_and_invalid_watermark_fail_before_publish(self):
        with self.assertRaisesRegex(ValueError, "component set"):
            self.backup.create(self.components()[:-1], "2026-07-21T00:00:01Z")
        components = self.components()
        bad = dict(self._migration())
        bad["lag"] = 1
        components[6] = BackupComponent.json(
            "migration_watermark", "logical-json", CATCHUP_VERSION,
            bad, self.watermark, True,
        )
        with self.assertRaisesRegex(ValueError, "watermark is invalid"):
            self.backup.create(components, "2026-07-21T00:00:01Z")
        self.assertEqual(list(self.backup.backup_root.glob("[!.]*")), [])

    def test_wrong_wrapping_key_and_corrupt_payload_fail_before_restore(self):
        artifact = Path(self.create()["artifact_path"])
        wrong = V2LocalBackup(self.storage / "backups", self.storage, b"x" * 32, "test")
        with self.assertRaisesRegex(ValueError, "authentication failed"):
            wrong.verify(artifact)
        payload = next((artifact / "components/artifact_archive").rglob("*.parquet"))
        payload.write_bytes(b"tampered")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.backup.verify(artifact)
        self.assertFalse((self.root / "restored-v2-test").exists())

    def test_resigned_semantic_watermark_tamper_is_rejected(self):
        artifact = Path(self.create()["artifact_path"])
        migration_path = artifact / "components/migration_watermark/logical.json"
        migration = json.loads(migration_path.read_text())
        migration["lag"] = 1
        migration.pop("checksum")
        migration["checksum"] = _hash(_json(migration))
        encoded = _json(migration)
        migration_path.write_bytes(encoded)
        manifest_path = artifact / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        for component in manifest["components"]:
            if component["name"] == "migration_watermark":
                component["files"][0]["bytes"] = len(encoded)
                component["files"][0]["sha256"] = _hash(encoded)
        manifest.pop("manifest_payload_sha256")
        manifest.pop("backup_id")
        new_id = _hash(_json(manifest))[:24]
        manifest["backup_id"] = new_id
        manifest["manifest_payload_sha256"] = _hash(_json(manifest))
        manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2))
        replacement = artifact.with_name(new_id)
        artifact.rename(replacement)
        with self.assertRaisesRegex(ValueError, "watermark is invalid"):
            self.backup.verify(replacement)

    def test_full_incremental_chain_and_repeat_restore_are_idempotent(self):
        full = self.create()
        incremental = self.create(mode="incremental", base_backup_id=full["backup_id"])
        target = self.root / "chain-v2-test"
        drill, first = self.restore(
            [Path(full["artifact_path"]), Path(incremental["artifact_path"])], target
        )
        self.assertEqual(first["status"], "complete")
        with (
            patch.object(drill, "_restore_postgres") as pg,
            patch.object(drill, "_restore_clickhouse") as ch,
        ):
            second = drill.restore([
                Path(full["artifact_path"]), Path(incremental["artifact_path"])
            ])
        self.assertEqual(second["status"], "complete")
        pg.assert_not_called()
        ch.assert_not_called()

    def test_checkpoint_failure_resumes_in_fixed_order(self):
        artifact = Path(self.create()["artifact_path"])
        target = self.root / "resume-v2-test"

        def fail_after(_when, name):
            if _when == "after_checkpoint" and name == "artifact_archive":
                raise RuntimeError("synthetic crash")

        with self.assertRaisesRegex(RuntimeError, "synthetic crash"):
            self.restore([artifact], target, fail_after)
        checkpoint = json.loads(
            (target / ".storage-v2-restore/checkpoint.json").read_text()
        )
        self.assertEqual(checkpoint["completed"], list(V2_COMPONENT_ORDER[:3]))
        _, resumed = self.restore([artifact], target)
        self.assertEqual(resumed["steps"], list(V2_COMPONENT_ORDER))

    def test_every_component_before_and_after_write_failure_is_reentrant(self):
        artifact = Path(self.create()["artifact_path"])
        for stage in ("before", "after_write"):
            for index, component in enumerate(V2_COMPONENT_ORDER):
                target = self.root / f"fault-{stage}-{index}-v2-test"
                fired = {"value": False}

                def fail(when, name):
                    if not fired["value"] and when == stage and name == component:
                        fired["value"] = True
                        raise RuntimeError("synthetic component crash")

                with self.assertRaisesRegex(RuntimeError, "synthetic component crash"):
                    self.restore([artifact], target, fail)
                checkpoint = json.loads(
                    (target / ".storage-v2-restore/checkpoint.json").read_text()
                )
                self.assertEqual(checkpoint["completed"], list(V2_COMPONENT_ORDER[:index]))
                _, resumed = self.restore([artifact], target)
                self.assertEqual(resumed["steps"], list(V2_COMPONENT_ORDER))

    def test_corrupt_checkpoint_and_incremental_order_fail_closed(self):
        full = self.create()
        incremental = self.create(mode="incremental", base_backup_id=full["backup_id"])
        with self.assertRaisesRegex(ValueError, "begin with a full"):
            self.restore([Path(incremental["artifact_path"])])
        target = self.root / "corrupt-checkpoint-v2-test"

        def stop(_when, name):
            if _when == "after_checkpoint" and name == "postgresql":
                raise RuntimeError("stop")

        with self.assertRaises(RuntimeError):
            self.restore([Path(full["artifact_path"])], target, stop)
        checkpoint = target / ".storage-v2-restore/checkpoint.json"
        document = json.loads(checkpoint.read_text())
        document["completed"] = ["clickhouse"]
        checkpoint.write_text(json.dumps(document))
        with self.assertRaisesRegex(ValueError, "checksum mismatch|order is corrupt"):
            self.restore([Path(full["artifact_path"])], target)

    def test_component_replace_after_preflight_fails_before_component_publish(self):
        artifact = Path(self.create()["artifact_path"])
        source = artifact / "components/artifact_archive/manifest.json"
        original = source.read_bytes()
        target = self.root / "replace-window-v2-test"
        fired = {"value": False}

        def replace(when, name):
            if not fired["value"] and when == "before" and name == "artifact_archive":
                fired["value"] = True
                attacker = source.with_name("attacker.json")
                attacker.write_bytes(b"x" * len(original))
                attacker.replace(source)

        with self.assertRaisesRegex(ValueError, "checksum or identity mismatch"):
            self.restore([artifact], target, replace)
        self.assertFalse((target / "artifacts").exists())
        source.write_bytes(original)
        source.chmod(0o640)
        _, resumed = self.restore([artifact], target)
        self.assertEqual(resumed["status"], "complete")

    def test_symlink_and_nonempty_target_fail_closed(self):
        artifact = Path(self.create()["artifact_path"])
        linked = self.backup.backup_root / "linked"
        linked.symlink_to(artifact, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.backup.verify(linked)
        target = self.root / "nonempty-v2-test"
        target.mkdir()
        (target / "foreign").write_text("data")
        with self.assertRaisesRegex(ValueError, "must be empty"):
            self.restore([artifact], target)


@unittest.skipUnless(
    os.getenv("MARKETCOW_TEST_POSTGRES_DSN") and
    os.getenv("MARKETCOW_TEST_CLICKHOUSE_HOST"),
    "set PostgreSQL and ClickHouse variables for the V2 backup/restore drill",
)
class V2BackupRestoreIntegrationTest(unittest.TestCase):
    def test_disposable_pg_ch_restore_repository_api_parquet_and_replay(self):
        import clickhouse_connect
        import duckdb
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
        from marketcow.config import Settings
        from marketcow.postgres_repositories import PostgresDatabase
        from marketcow.v2_backup_restore import (
            capture_v2_clickhouse, capture_v2_postgresql,
        )

        class Outage:
            def insert_raw_bars(self, _rows, batch_id=""):
                raise ConnectionError("synthetic outage")

        class Service:
            def __init__(self, repository):
                self.market_bar_repository = repository

            def close(self):
                return None

            def refresh_quote_history(self, *_args, **_kwargs):
                raise AssertionError("restored query must not refresh")

        dsn = os.environ["MARKETCOW_TEST_POSTGRES_DSN"]
        host = os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"]
        port = int(os.getenv("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123"))
        username = os.getenv("MARKETCOW_TEST_CLICKHOUSE_USERNAME", "default")
        password = os.getenv("MARKETCOW_TEST_CLICKHOUSE_PASSWORD", "")
        suffix = uuid.uuid4().hex[:10]
        source_schema = f"backup_source_{suffix}_test"
        target_schema = f"backup_target_{suffix}_test"
        source_ch_name = f"backup_source_{suffix}_test"
        target_ch_name = f"backup_target_{suffix}_test"
        source_pg = PostgresDatabase(dsn, source_schema, 1, 2)
        target_pg = PostgresDatabase(dsn, target_schema, 1, 2)
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
                    ("backup-fixture", "healthy", "2026-07-21T00:00:00Z", 0),
                )
            source_ch.open()
            source_repository = ClickHouseMarketBarRepository(source_ch)
            with tempfile.TemporaryDirectory() as folder:
                root = Path(folder).resolve()
                source_root = root / "source-v2-test"
                source_root.mkdir()
                spool = LocalClickHouseSpool(source_root / "spool/clickhouse", source_root)
                bar = normalize_bar("raw", {
                    "symbol": "BACKUP", "market": "US", "interval": "1m",
                    "adjustment": "raw", "bar_time": "2026-07-21T00:00:00Z",
                    "open": 1, "high": 2, "low": 0.5, "close": 1.5,
                    "raw_close": 1.5, "adjustment_factor": 1, "volume": 10,
                    "amount": 15, "source": "fixture", "source_sequence": "1",
                    "observed_at": "2026-07-21T00:00:01Z",
                    "ingested_at": "2026-07-21T00:00:02Z",
                    "raw_artifact_id": "backup-artifact",
                })
                bar2 = normalize_bar("raw", {
                    **bar, "bar_time": "2026-07-21T00:00:30Z",
                    "open": 2, "high": 3, "low": 1.5, "close": 2.5,
                    "raw_close": 2.5, "volume": 20, "amount": 50,
                    "raw_artifact_id": "backup-artifact-2",
                })
                writer = ReliableClickHouseWriter(source_repository, spool, 1000)
                self.assertEqual(writer.write("raw", [bar, bar2])["status"], "success")
                builder = CanonicalMarketBarBuilder(source_repository, writer)
                self.assertEqual(builder.rebuild(
                    "BACKUP", "1m", "raw", "2026-07-21T00:00:00Z",
                    "2026-07-21T00:01:00Z", 100,
                )["status"], "ok")
                pending = normalize_bar("raw", {
                    **bar, "symbol": "PENDING", "bar_time": "2026-07-21T00:00:01Z",
                    "observed_at": "2026-07-21T00:00:01Z",
                    "ingested_at": "2026-07-21T00:00:02Z",
                    "raw_artifact_id": "pending-artifact",
                })
                failed = ReliableClickHouseWriter(Outage(), spool, 1000).write("raw", [pending])
                self.assertEqual(failed["status"], "durable_pending")

                artifact_root = source_root / "artifacts"
                artifact_root.mkdir()
                parquet = artifact_root / "bars.parquet"
                with duckdb.connect() as connection:
                    connection.execute(
                        "COPY (SELECT 'BACKUP' symbol, 1.5::DOUBLE AS \"close\") TO ? "
                        "(FORMAT PARQUET)", [str(parquet)],
                    )
                (artifact_root / "manifest.json").write_text(
                    json.dumps({"version": "artifact-parquet-v1", "rows": 1})
                )
                scheduler_root = source_root / "canonical-scheduler"
                scheduler_root.mkdir()
                (scheduler_root / "checkpoint.json").write_text(
                    json.dumps({"version": "canonical-scheduler-v1", "pending": []})
                )
                captured = "2026-07-21T00:00:03Z"
                watermark = {"captured_at": captured}
                migration = {
                    "version": CATCHUP_VERSION, "phase": "complete",
                    "catchup_run_id": "bg014-integration", "lag": 0,
                    "source_high_watermark": {"source_fingerprint": "f" * 64},
                    "verified_raw_max_ingested_at": "2026-07-21T00:00:02Z",
                    "canonical_rebuild_through": "2026-07-21T00:00:02Z",
                }
                migration["checksum"] = _hash(_json(migration))
                components = [
                    capture_v2_postgresql(source_pg, captured),
                    capture_v2_clickhouse(source_ch, captured),
                    BackupComponent.tree(
                        "artifact_archive", "artifact-parquet-tree", "artifact-parquet-v1",
                        artifact_root, source_root, watermark,
                    ),
                    BackupComponent.tree(
                        "authoritative_spool", "wal-intent-tree", "authoritative-spool-v2",
                        source_root / "spool/clickhouse", source_root, watermark, True,
                    ),
                    BackupComponent.tree(
                        "scheduler_state", "scheduler-tree", "canonical-scheduler-v1",
                        scheduler_root, source_root, watermark, True,
                    ),
                    BackupComponent.json(
                        "config_version", "logical-json", "v2-config-version-v1",
                        {"version": "v2-test-1"}, watermark,
                    ),
                    BackupComponent.json(
                        "migration_watermark", "logical-json", CATCHUP_VERSION,
                        migration, watermark, True,
                    ),
                    BackupComponent(
                        "cursor_key", "sealed-secret", "cursor-v1",
                        {"cursor.key": b"c" * 48}, watermark,
                    ),
                ]
                backup = V2LocalBackup(source_root / "backups", source_root, b"w" * 32, "test")
                artifact = Path(backup.create(
                    components, "2026-07-21T00:00:04Z"
                )["artifact_path"])

                target_pg.pool.open(wait=True)
                bootstrap = clickhouse_connect.get_client(
                    host=host, port=port, username=username, password=password,
                    database="default",
                )
                bootstrap.command(f"CREATE DATABASE `{target_ch_name}`")
                target_ch.client = target_ch._connect(target_ch_name)
                target_root = root / "restored-v2-test"
                drill = V2LocalRestore(backup, RestoreTargets(
                    target_root, target_pg, target_ch, "test", root
                ))
                report = drill.restore([artifact])
                self.assertEqual(report["status"], "complete")
                target_repository = ClickHouseMarketBarRepository(target_ch)
                canonical, _ = target_repository.get_canonical_price_bars_range(
                    "BACKUP", "1m", "raw", "2026-07-21T00:00:00Z",
                    "2026-07-21T00:01:00Z", 10,
                )
                self.assertEqual(len(canonical), 2)
                with target_pg.connection() as connection:
                    table_count = connection.execute(
                        "SELECT count(*) AS count FROM information_schema.tables "
                        "WHERE table_schema=%s", [target_schema],
                    ).fetchone()["count"]
                    provider_count = connection.execute(
                        "SELECT count(*) AS count FROM provider_health "
                        "WHERE provider='backup-fixture'"
                    ).fetchone()["count"]
                self.assertEqual(table_count, 19)
                self.assertEqual(provider_count, 1)
                restored_spool = LocalClickHouseSpool(
                    target_root / "spool/clickhouse", target_root
                )
                restored_writer = ReliableClickHouseWriter(target_repository, restored_spool, 1000)
                self.assertEqual(restored_writer.replay(10)["replayed"], 1)
                self.assertEqual(restored_writer.replay(10)["replayed"], 0)
                self.assertEqual(target_ch.client.query(
                    "SELECT count() FROM market_bar_raw FINAL WHERE symbol='PENDING'"
                ).result_rows[0][0], 1)
                with duckdb.connect() as connection:
                    rows = connection.execute(
                        "SELECT symbol, close FROM read_parquet(?)",
                        [str(target_root / "artifacts/bars.parquet")],
                    ).fetchall()
                self.assertEqual(rows, [("BACKUP", 1.5)])
                settings = Settings(
                    database_path=None, raw_path=target_root / "raw", profile="v2-test",
                    storage_root=target_root, market_bar_cursor_secret="",
                    market_bar_cache_freshness_seconds=3600,
                )
                app = create_app(settings, Service(target_repository))
                with TestClient(app) as client:
                    response = client.get(
                        "/v1/quotes/BACKUP/history?interval=1m&adjustment=raw&"
                        "start=2026-07-21T00:00:00Z&end=2026-07-21T00:01:00Z&"
                        "page_size=1&refresh=false"
                    )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["count"], 1)
                self.assertIsNotNone(response.json()["next_cursor"])
                with TestClient(app) as client:
                    second = client.get(
                        "/v1/quotes/BACKUP/history?interval=1m&adjustment=raw&"
                        "start=2026-07-21T00:00:00Z&end=2026-07-21T00:01:00Z&"
                        "page_size=1&refresh=false&cursor=" + response.json()["next_cursor"]
                    )
                self.assertEqual(second.status_code, 200)
                self.assertEqual(second.json()["bars"][0]["close"], 2.5)
                drill.record_v2_verification({
                    "postgres_18_domains": "ok", "postgres_pit": "ok",
                    "clickhouse_raw_final": "ok", "clickhouse_canonical": "ok",
                    "market_api_contract": "ok", "pagination_cursor_cache": "ok",
                    "artifact_parquet_query": "ok", "spool_replay_once": "ok",
                    "canonical_boundary": "ok",
                })
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
