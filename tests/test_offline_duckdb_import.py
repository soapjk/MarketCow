from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import duckdb

from marketcow.offline_duckdb_import import (
    ImportLimits,
    OfflineDuckDBError,
    OfflineDuckDBImporter,
    main,
)
from marketcow.storage import Warehouse


class OfflineDuckDBImportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.source = self.root / "fixture" / "legacy.duckdb"
        Warehouse(self.source)

    def tearDown(self):
        self.temp.cleanup()

    def importer(self, **changes):
        arguments = {
            "allowed_root": self.root,
            "source": self.source,
            "source_label": "test-fixture",
            "limits": ImportLimits(max_file_bytes=64 * 1024**2, max_rows=1000, batch_rows=2),
        }
        arguments.update(changes)
        return OfflineDuckDBImporter(**arguments)

    def test_validate_and_extract_are_read_only_and_fingerprint_bound(self):
        before = self.source.read_bytes()
        before_stat = self.source.stat()
        with duckdb.connect(str(self.source)) as connection:
            connection.execute(
                "INSERT INTO provider_health VALUES ('fixture', 'healthy', NULL, NULL, NULL, 0)"
            )
        before = self.source.read_bytes()
        before_stat = self.source.stat()

        manifest = self.importer().inspect()
        batches = list(self.importer().batches("provider_health"))

        self.assertEqual(manifest["status"], "validated")
        self.assertEqual(manifest["migrations"], [2, 3, 4])
        self.assertEqual(manifest["file_sha256"], hashlib.sha256(before).hexdigest())
        self.assertTrue(manifest["source"].startswith("duckdb-copy://"))
        self.assertNotIn(str(self.root), json.dumps(manifest))
        self.assertEqual(batches[0][0]["provider"], "fixture")
        after_stat = self.source.stat()
        self.assertEqual(self.source.read_bytes(), before)
        self.assertEqual(after_stat.st_mtime_ns, before_stat.st_mtime_ns)

    def test_path_label_symlink_production_and_corrupt_sources_fail_closed(self):
        outside = self.root.parent / "outside.duckdb"
        cases = [
            ({"source_label": "production"}, "source_label_rejected"),
            ({"source": outside}, "path_rejected"),
        ]
        for changes, code in cases:
            with self.subTest(code=code), self.assertRaises(OfflineDuckDBError) as caught:
                self.importer(**changes)
            self.assertEqual(caught.exception.code, code)

        production = self.root / "production" / "copy.duckdb"
        production.parent.mkdir()
        production.write_bytes(self.source.read_bytes())
        with self.assertRaises(OfflineDuckDBError) as caught:
            self.importer(source=production)
        self.assertEqual(caught.exception.code, "production_rejected")

        link = self.root / "linked.duckdb"
        link.symlink_to(self.source)
        with self.assertRaises(OfflineDuckDBError) as caught:
            self.importer(source=link)
        self.assertEqual(caught.exception.code, "symlink_rejected")

        corrupt = self.root / "fixture" / "corrupt.duckdb"
        corrupt.write_bytes(b"not a duckdb password=do-not-leak /absolute/private/path")
        with self.assertRaises(OfflineDuckDBError) as caught:
            self.importer(source=corrupt).inspect()
        self.assertEqual(caught.exception.code, "duckdb_open_failed")
        self.assertNotIn("password", str(caught.exception))
        self.assertNotIn(str(corrupt), str(caught.exception))

    def test_schema_version_inventory_and_resource_bounds(self):
        with duckdb.connect(str(self.source)) as connection:
            connection.execute("INSERT INTO schema_migrations VALUES (999, 'future', 'now')")
        with self.assertRaises(OfflineDuckDBError) as caught:
            self.importer().inspect()
        self.assertEqual(caught.exception.code, "migration_rejected")
        with duckdb.connect(str(self.source)) as connection:
            connection.execute("DELETE FROM schema_migrations WHERE version=999")
            connection.execute("CREATE TABLE malicious_payload(secret VARCHAR)")
        with self.assertRaises(OfflineDuckDBError) as caught:
            self.importer().inspect()
        self.assertEqual(caught.exception.code, "schema_rejected")
        with duckdb.connect(str(self.source)) as connection:
            connection.execute("DROP TABLE malicious_payload")
            connection.execute("ALTER TABLE provider_health ADD COLUMN malicious VARCHAR")
        with self.assertRaises(OfflineDuckDBError) as caught:
            self.importer().inspect()
        self.assertEqual(caught.exception.code, "schema_rejected")
        # Recreate the canonical fixture before exercising row bounds.
        self.source.unlink()
        Warehouse(self.source)
        with duckdb.connect(str(self.source)) as connection:
            connection.execute(
                "INSERT INTO provider_health VALUES ('one', 'healthy', NULL, NULL, NULL, 0), "
                "('two', 'healthy', NULL, NULL, NULL, 0)"
            )
        with self.assertRaises(OfflineDuckDBError) as caught:
            self.importer(limits=ImportLimits(max_file_bytes=64 * 1024**2, max_rows=1, batch_rows=1)).inspect()
        self.assertEqual(caught.exception.code, "row_limit_rejected")
        with self.assertRaises(ValueError):
            ImportLimits(memory_mb=8)
        with self.assertRaises(ValueError):
            ImportLimits(max_rows=1, batch_rows=2)

    def test_deadline_and_table_allowlist(self):
        ticks = iter((0.0, 0.0, 2.0))
        importer = self.importer(
            limits=ImportLimits(max_file_bytes=64 * 1024**2, max_rows=1000, batch_rows=2, timeout_seconds=1),
            clock=lambda: next(ticks, 2.0),
        )
        with self.assertRaises(OfflineDuckDBError) as caught:
            importer.inspect()
        self.assertEqual(caught.exception.code, "timeout")
        with self.assertRaises(OfflineDuckDBError) as caught:
            list(self.importer().batches("schema_migrations; DROP TABLE provider_health"))
        self.assertEqual(caught.exception.code, "table_rejected")

    def test_cli_machine_output_and_zero_online_import_side_effects(self):
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = main([
                "validate", "--allowed-root", str(self.root), "--source", str(self.source),
                "--source-label", "test-fixture", "--max-file-bytes", str(64 * 1024**2),
            ])
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(stdout.getvalue())["status"], "validated")

        code = """
import builtins, runpy, sys
blocked = ('marketcow.v2_factory', 'marketcow.api', 'marketcow.service', 'psycopg', 'clickhouse_connect')
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name.startswith(blocked):
        raise AssertionError('online import attempted')
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
sys.argv = ['marketcow-offline-duckdb', 'validate', '--allowed-root', sys.argv[1], '--source', sys.argv[2], '--source-label', 'test-fixture', '--max-file-bytes', str(64 * 1024**2)]
runpy.run_module('marketcow.offline_duckdb_import', run_name='__main__')
"""
        result = subprocess.run(
            [sys.executable, "-c", code, str(self.root), str(self.source)],
            text=True, capture_output=True, check=False,
            env={**os.environ, "MARKETCOW_HOME": str(self.root / "absent-home")},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.root / "absent-home").exists())
        self.assertEqual(json.loads(result.stdout)["status"], "validated")

    def test_cli_errors_are_bounded_and_redacted(self):
        stderr = io.StringIO()
        secret = self.root / "fixture" / "password=plaintext.duckdb"
        secret.write_bytes(b"broken")
        with contextlib.redirect_stderr(stderr):
            status = main([
                "validate", "--allowed-root", str(self.root), "--source", str(secret),
                "--source-label", "test-fixture",
            ])
        payload = json.loads(stderr.getvalue())
        self.assertEqual(status, 2)
        self.assertEqual(payload["status"], "rejected")
        rendered = json.dumps(payload)
        self.assertNotIn("plaintext", rendered)
        self.assertNotIn(str(self.root), rendered)


if __name__ == "__main__":
    unittest.main()
