import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from marketcow.local_backup import (
    MANIFEST_VERSION,
    BackupComponent,
    LocalStorageBackup,
)


class LocalStorageBackupTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name) / "data-development"
        self.root.mkdir()
        self.backup = LocalStorageBackup(
            self.root / "backups", self.root, b"w" * 32
        )
        self.cursor_plaintext = b"cursor-token-super-secret-value"

    def tearDown(self):
        self.folder.cleanup()

    def components(self, suffix=""):
        watermark = {"captured_at": "2026-07-20T00:00:00Z", "sequence": 10}
        return [
            BackupComponent.json("postgresql", "logical-json", "postgres-16",
                                 {"rows": [[1, suffix]]}, watermark),
            BackupComponent.json("clickhouse", "logical-json", "clickhouse-25.8",
                                 {"raw": [[1]], "canonical": [[1]]}, watermark,
                                 canonical_rebuildable=True),
            BackupComponent("duckdb", "duckdb-file", "1", {"warehouse.duckdb":
                            b"synthetic-duckdb-" + suffix.encode()}, watermark),
            BackupComponent("cold_archive", "parquet-tree", "manifest-v1",
                            {"artifact/data.parquet": b"PAR1synthetic",
                             "artifact/manifest.json": b'{"schema":"v1"}'}, watermark),
            BackupComponent("spool", "wal-tree", "spool-v1",
                            {"pending/item.json": b'{"batch":"fixture"}',
                             "intent/item.json": b'{"range":"fixture"}'}, watermark,
                            canonical_rebuildable=True),
            BackupComponent("cursor_key", "sealed-secret", "cursor-v1",
                            {"cursor.key": self.cursor_plaintext}, watermark),
        ]

    def test_full_backup_manifest_checksums_permissions_and_sensitive_scan(self):
        result = self.backup.create(
            self.components(), "2026-07-20T00:00:01+00:00"
        )
        self.assertEqual(result["manifest_version"], MANIFEST_VERSION)
        self.assertEqual(result["status"], "verified")
        self.assertEqual(len(result["components"]), 6)
        self.assertIn("RPO", result["rpo_assumption"].upper())
        self.assertIn("60 minutes", result["rto_assumption"])
        artifact = Path(result["artifact_path"])
        rendered = b"".join(path.read_bytes() for path in artifact.rglob("*")
                            if path.is_file())
        self.assertNotIn(self.cursor_plaintext, rendered)
        cursor = next(path for path in artifact.rglob("*.sealed"))
        self.assertEqual(cursor.stat().st_mode & 0o777, 0o600)
        self.assertTrue(result["cross_component_watermark"]["latest_captured_at"])

    def test_repeat_and_incremental_backup_are_deterministic(self):
        first = self.backup.create(self.components(), "2026-07-20T00:00:01Z")
        second = self.backup.create(self.components(), "2026-07-20T00:00:01Z")
        self.assertEqual(first["backup_id"], second["backup_id"])
        incremental = self.backup.create(
            self.components("next"), "2026-07-20T00:00:02Z", "incremental",
            first["backup_id"],
        )
        self.assertEqual(incremental["mode"], "incremental")
        self.assertEqual(incremental["base_backup_id"], first["backup_id"])
        self.assertNotEqual(incremental["backup_id"], first["backup_id"])

    def test_missing_component_manifest_and_file_corruption_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "component set"):
            self.backup.create(self.components()[:-1], "2026-07-20T00:00:01Z")
        result = self.backup.create(self.components(), "2026-07-20T00:00:01Z")
        artifact = Path(result["artifact_path"])
        manifest = artifact / "manifest.json"
        original = manifest.read_bytes()
        document = json.loads(original)
        document["snapshot_at"] = "2099-01-01T00:00:00Z"
        manifest.write_text(json.dumps(document))
        with self.assertRaisesRegex(ValueError, "manifest checksum"):
            self.backup.verify(artifact)
        manifest.write_bytes(original)
        data = next(path for path in artifact.rglob("logical.json"))
        data.write_bytes(data.read_bytes() + b"corrupt")
        with self.assertRaisesRegex(ValueError, "checksum"):
            self.backup.verify(artifact)

    def test_sensitive_payload_symlink_escape_and_permissions_are_rejected(self):
        components = self.components()
        components[0] = BackupComponent.json(
            "postgresql", "logical-json", "postgres-16",
            {"dsn": "postgresql://user:password@host/db"},
            {"captured_at": "2026-07-20T00:00:00Z"},
        )
        with self.assertRaisesRegex(ValueError, "sensitive"):
            self.backup.create(components, "2026-07-20T00:00:01Z")
        source = self.root / "source"
        source.mkdir()
        outside = Path(self.folder.name) / "outside"
        outside.write_text("secret")
        (source / "link").symlink_to(outside)
        with self.assertRaisesRegex(ValueError, "symlink"):
            BackupComponent.tree("spool", "wal", "1", source, self.root,
                                 {"captured_at": "2026-07-20T00:00:00Z"})
        result = self.backup.create(self.components(), "2026-07-20T00:00:01Z")
        artifact = Path(result["artifact_path"])
        cursor = next(path for path in artifact.rglob("*.sealed"))
        cursor.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "permission"):
            self.backup.verify(artifact)

    def test_crash_publish_and_concurrent_mutex_recover(self):
        def crash(stage):
            if stage == "before_publish":
                raise RuntimeError("crash")

        with self.assertRaisesRegex(RuntimeError, "crash"):
            self.backup.create(self.components(), "2026-07-20T00:00:01Z",
                               fault_hook=crash)
        self.assertEqual(list(self.backup.staging.iterdir()), [])
        results, errors = [], []

        def create():
            try:
                results.append(self.backup.create(
                    self.components(), "2026-07-20T00:00:01Z"
                ))
            except Exception as error:
                errors.append(error)

        threads = [threading.Thread(target=create) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len({item["backup_id"] for item in results}), 1)

    def test_no_restore_or_external_write_surface_and_atomic_publish(self):
        with patch("os.replace", wraps=__import__("os").replace) as replace:
            result = self.backup.create(self.components(), "2026-07-20T00:00:01Z")
        self.assertEqual(replace.call_count, 1)
        self.assertEqual(Path(result["artifact_path"]).parent, self.backup.backup_root)
        self.assertFalse(hasattr(self.backup, "restore"))


if __name__ == "__main__":
    unittest.main()
