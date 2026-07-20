import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from marketcow.offline_full_import import FullImportTargets, OfflineFullImport, _StageSink
from marketcow.offline_incremental_catchup import OfflineIncrementalCatchup
from marketcow.offline_duckdb_import import ImportLimits, OfflineDuckDBImporter


class _Target:
    def __init__(self, name):
        self.schema = name
        self.database = name


class OfflineFullImportTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)

    def tearDown(self):
        self.folder.cleanup()

    def test_stream_stage_requires_verified_terminal_and_is_replayable(self):
        destination = self.root / "stage.ndjson"
        sink = _StageSink(destination, "provider_health", "fingerprint")
        manifest = b'{"type":"manifest","source_fingerprint":"fingerprint"}\n'
        batch = b'{"rows":[{"provider":"x"}],"sequence":0,"table":"provider_health","type":"batch"}\n'
        terminal = json.dumps({
            "type": "complete", "source_fingerprint": "fingerprint",
            "table": "provider_health", "row_count": 1, "batch_count": 1,
            "data_sha256": hashlib.sha256(batch).hexdigest(),
        }, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        for record in (manifest, batch, terminal):
            sink.write(record)
        sink.publish(0)
        self.assertEqual(
            OfflineFullImport._verify_stage(destination, "provider_health", "fingerprint")["rows"], 1
        )

    def test_truncated_forged_or_reordered_stream_never_publishes(self):
        for name, records in {
            "truncated": [b'{"type":"manifest","source_fingerprint":"f"}\n'],
            "batch-first": [b'{"rows":[],"sequence":0,"table":"provider_health","type":"batch"}\n'],
        }.items():
            destination = self.root / f"{name}.ndjson"
            sink = _StageSink(destination, "provider_health", "f")
            if name == "batch-first":
                with self.assertRaisesRegex(ValueError, "batch identity"):
                    sink.write(records[0])
                sink.output.close()
            else:
                sink.write(records[0])
                with self.assertRaisesRegex(ValueError, "did not complete"):
                    sink.publish(0)
            self.assertFalse(destination.exists())

    def test_target_boundary_rejects_production_escape_and_symlink(self):
        source = object()
        pg = _Target("migration_test")
        ch = _Target("migration_test")
        with self.assertRaisesRegex(ValueError, "development/test-only"):
            OfflineFullImport(source, FullImportTargets(
                self.root / "run-test", self.root, pg, ch, object(), object(), "production"
            ))
        with self.assertRaisesRegex(ValueError, "escapes"):
            OfflineFullImport(source, FullImportTargets(
                self.root.parent / "escape-test", self.root, pg, ch, object(), object()
            ))
        link = self.root / "link-test"
        link.symlink_to(self.root / "real-test")
        with self.assertRaisesRegex(ValueError, "symlink"):
            OfflineFullImport(source, FullImportTargets(link, self.root, pg, ch, object(), object()))

    def test_failed_stream_is_not_a_complete_artifact(self):
        destination = self.root / "failed.ndjson"
        sink = _StageSink(destination, "provider_health", "f")
        sink.write(b'{"type":"manifest","source_fingerprint":"f"}\n')
        sink.write(b'{"type":"failed","error":{"code":"timeout"}}\n')
        with self.assertRaisesRegex(ValueError, "did not complete"):
            sink.publish(1)
        self.assertFalse(destination.exists())

    def test_short_or_partial_clickhouse_ack_never_advances_checkpoint(self):
        pg = _Target("migration_test")
        ch = _Target("migration_test")
        writer = Mock()
        writer.write.return_value = {
            "status": "durable_pending", "acknowledged": False,
            "verified": False, "written": 0,
        }
        drill = OfflineFullImport(object(), FullImportTargets(
            self.root / "run-test", self.root, pg, ch, writer, Mock(),
        ))
        row = {
            "symbol": "FULL", "interval": "1d", "adjustment": "none",
            "timestamp": 1, "bar_at": "1970-01-01T00:00:01Z", "open": 1,
            "high": 1, "low": 1, "close": 1, "raw_close": 1,
            "adjustment_factor": 1, "volume": 1, "amount": None,
            "source": "fixture", "source_sequence": "1",
            "observed_at": "1970-01-01T00:00:01Z",
            "ingested_at": "1970-01-01T00:00:02Z", "raw_artifact_id": None,
        }
        drill._stage_table = Mock(return_value=self.root / "stage")
        drill._verify_stage = Mock(return_value={"rows": 1, "checksum": "x"})
        drill._stage_batches = Mock(return_value=iter([(0, [row])]))
        drill._save = Mock()
        checkpoint = {"domains": {}}
        with self.assertRaisesRegex(RuntimeError, "not fully acknowledged"):
            drill._import_market(checkpoint, "fingerprint")
        self.assertEqual(checkpoint["domains"]["market_bar_raw"]["batch"], 0)
        drill._save.assert_not_called()


@unittest.skipUnless(
    os.getenv("MARKETCOW_TEST_POSTGRES_DSN") and os.getenv("MARKETCOW_TEST_CLICKHOUSE_HOST"),
    "requires disposable PostgreSQL and ClickHouse",
)
class OfflineFullImportIntegrationTest(unittest.TestCase):
    def test_all_domains_raw_canonical_artifact_and_restart(self):
        from marketcow.clickhouse_canonical import CanonicalMarketBarBuilder
        from marketcow.clickhouse_repositories import ClickHouseDatabase, ClickHouseMarketBarRepository
        from marketcow.clickhouse_writer import LocalClickHouseSpool, ReliableClickHouseWriter
        from marketcow.postgres_migrations import POSTGRES_TRANSACTION_DOMAINS
        from marketcow.postgres_repositories import PostgresDatabase
        from marketcow.postgres_repositories import PostgresRepository
        from marketcow.storage import Warehouse
        from tests.test_local_backfill import _seed_all_postgres_domains

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source_dir = root / "source-test"
            warehouse = Warehouse(source_dir / "legacy.duckdb")
            _seed_all_postgres_domains(warehouse)
            body = b"synthetic-artifact-body"
            body_path = source_dir / "artifact-1.bin"
            body_path.write_bytes(body)
            with warehouse.connect() as connection:
                connection.execute(
                    "UPDATE raw_artifact_manifest SET storage_path=?,sha256=?,byte_size=? WHERE artifact_id='artifact-1'",
                    ["source-test/artifact-1.bin", hashlib.sha256(body).hexdigest(), len(body)],
                )
            warehouse.upsert_price_bars(
                "FULL", "1d", "none", "fixture", "2026-06-02T00:00:00Z",
                [{"timestamp": 1780272000, "bar_at": "2026-06-01T00:00:00Z", "open": 10,
                  "high": 12, "low": 9, "close": 11, "volume": 100, "amount": 1100}],
                {"observed_at": "2026-06-01T00:01:00Z", "raw_artifact_id": "artifact-1"},
            )
            suffix = hashlib.sha256(folder.encode()).hexdigest()[:10]
            pg = PostgresDatabase(os.environ["MARKETCOW_TEST_POSTGRES_DSN"], f"bg013_{suffix}_test")
            ch = ClickHouseDatabase(
                os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"],
                int(os.getenv("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123")),
                f"bg013_{suffix}_test",
                os.getenv("MARKETCOW_TEST_CLICKHOUSE_USERNAME", "default"),
                os.getenv("MARKETCOW_TEST_CLICKHOUSE_PASSWORD", ""),
            )
            pg.open()
            ch.open()
            try:
                repository = ClickHouseMarketBarRepository(ch)
                spool = LocalClickHouseSpool(root / "run-test" / "spool", root)
                writer = ReliableClickHouseWriter(repository, spool, batch_size=1000)
                builder = CanonicalMarketBarBuilder(repository, writer)
                source = OfflineDuckDBImporter(
                    allowed_root=root.resolve(), source=Path(warehouse.path).resolve(),
                    source_label="test-fixture",
                    limits=ImportLimits(max_rows=1000, batch_rows=2, timeout_seconds=20),
                )
                targets = FullImportTargets(root / "run-test", root, pg, ch, writer, builder)
                fired = set()
                def interrupt(stage, domain):
                    if stage == "after_write" and domain == "provider_health" and not fired:
                        fired.add(domain)
                        raise RuntimeError("synthetic batch/checkpoint window")
                interrupted = OfflineFullImport(source, targets, interrupt)
                with self.assertRaisesRegex(RuntimeError, "checkpoint window"):
                    interrupted.run()
                drill = OfflineFullImport(source, targets)
                report = drill.run()
                self.assertEqual(report["status"], "complete")
                self.assertEqual({item["domain"] for item in report["domains"]},
                                 set(POSTGRES_TRANSACTION_DOMAINS) |
                                 {"market_bar_raw", "market_bar_canonical"})
                self.assertTrue(all(item["status"] == "ok" for item in report["domains"]))
                with pg.connection() as connection:
                    for table in POSTGRES_TRANSACTION_DOMAINS:
                        self.assertGreater(connection.execute(
                            f'SELECT count(*) AS count FROM "{table}"'
                        ).fetchone()["count"], 0, table)
                    artifact = connection.execute(
                        "SELECT storage_path,sha256 FROM raw_artifact_manifest WHERE artifact_id='artifact-1'"
                    ).fetchone()
                    self.assertEqual(artifact["storage_path"], "artifact://artifact-1")
                    self.assertEqual(artifact["sha256"], hashlib.sha256(body).hexdigest())
                client = ch._require_client()
                self.assertEqual(client.query("SELECT count() FROM market_bar_raw FINAL").result_rows[0][0], 1)
                self.assertEqual(client.query("SELECT count() FROM market_bar_canonical FINAL").result_rows[0][0], 1)
                self.assertEqual(drill.run(), report)

                # The copied source remains active after the full checkpoint.  Update a
                # PG domain and append a new market key, then converge through BG-012
                # verified streams rather than reading DuckDB from the catch-up module.
                with warehouse.connect() as connection:
                    connection.execute(
                        "UPDATE provider_health SET status='degraded', last_error='synthetic' "
                        "WHERE provider='fixture'"
                    )
                warehouse.upsert_price_bars(
                    "FULL", "1d", "none", "fixture", "2026-06-03T00:00:00Z",
                    [{"timestamp": 1780358400, "bar_at": "2026-06-02T00:00:00Z", "open": 11,
                      "high": 13, "low": 10, "close": 12, "volume": 120, "amount": 1440}],
                    {"observed_at": "2026-06-02T00:01:00Z", "raw_artifact_id": "artifact-1"},
                )
                catchup_faults = set()
                def interrupt_catchup(stage, domain):
                    if stage == "after_write" and domain == "provider_health" and not catchup_faults:
                        catchup_faults.add(domain)
                        raise RuntimeError("synthetic catchup checkpoint window")
                with self.assertRaisesRegex(RuntimeError, "catchup checkpoint window"):
                    OfflineIncrementalCatchup(source, targets, interrupt_catchup).run(max_passes=3)
                catchup = OfflineIncrementalCatchup(source, targets)
                caught_up = catchup.run(max_passes=3)
                self.assertEqual(caught_up["status"], "complete")
                self.assertEqual(caught_up["lag"], 0)
                self.assertEqual(len(set(caught_up["stability"])), 1)
                with pg.connection() as connection:
                    status = connection.execute(
                        "SELECT status FROM provider_health WHERE provider='fixture'"
                    ).fetchone()["status"]
                self.assertEqual(status, "degraded")
                pit = PostgresRepository(pg).query_fundamentals(
                    symbol="FULL", as_of="2026-06-03", active_only=False,
                )
                self.assertTrue(pit)
                self.assertEqual(client.query("SELECT count() FROM market_bar_raw FINAL").result_rows[0][0], 2)
                self.assertEqual(client.query("SELECT count() FROM market_bar_canonical FINAL").result_rows[0][0], 2)
                self.assertEqual(catchup.run(), caught_up)
                with warehouse.connect() as connection:
                    connection.execute(
                        "UPDATE provider_health SET status='unavailable' WHERE provider='fixture'"
                    )
                advanced = catchup.run(max_passes=3)
                self.assertEqual(advanced["lag"], 0)
                self.assertNotEqual(
                    advanced["source_high_watermark"]["source_fingerprint"],
                    caught_up["source_high_watermark"]["source_fingerprint"],
                )
                with pg.connection() as connection:
                    self.assertEqual(connection.execute(
                        "SELECT status FROM provider_health WHERE provider='fixture'"
                    ).fetchone()["status"], "unavailable")
                self.assertEqual(catchup.run(), advanced)
            finally:
                ch.close()
                pg.close()


if __name__ == "__main__":
    unittest.main()
