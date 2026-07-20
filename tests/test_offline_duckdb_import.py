from __future__ import annotations

import contextlib
import hashlib
import io
import json
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

import duckdb

from marketcow.offline_duckdb_import import (
    ImportLimits,
    OfflineDuckDBError,
    OfflineDuckDBImporter,
    main,
)
from marketcow.storage import Warehouse


class BlockingImporter(OfflineDuckDBImporter):
    def _stream_worker(self, command, table, sender):
        time.sleep(10)


class TruncatedImporter(OfflineDuckDBImporter):
    def _stream_worker(self, command, table, sender):
        sender.send_bytes(self._record_bytes({"type": "manifest", "status": "validated"}))
        sender.send_bytes(self._record_bytes({"type": "batch", "sequence": 0, "rows": [{"value": 1}]}))
        sender.close()


class MeasuringSink:
    def __init__(self):
        self.calls = 0
        self.max_write = 0
        self.last = b""

    def write(self, payload):
        if isinstance(payload, str):
            payload = payload.encode()
        self.calls += 1
        self.max_write = max(self.max_write, len(payload))
        self.last = payload

    def flush(self):
        pass


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
        output = io.BytesIO()
        self.assertEqual(self.importer().stream("extract", "provider_health", output), 0)
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        batches = [record["rows"] for record in records if record["type"] == "batch"]

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
            self.importer().stream("extract", "schema_migrations; DROP TABLE provider_health", io.BytesIO())
        self.assertEqual(caught.exception.code, "table_rejected")

    def test_cli_machine_output_and_zero_online_import_side_effects(self):
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            status = main([
                "validate", "--allowed-root", str(self.root), "--source", str(self.source),
                "--source-label", "test-fixture", "--max-file-bytes", str(64 * 1024**2),
            ])
        self.assertEqual(status, 0)
        records = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual([record["type"] for record in records], ["manifest", "complete"])
        self.assertEqual(records[-1]["status"], "complete")

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
        records = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(records[-1]["status"], "complete")

    def test_extract_stream_is_batch_bounded_and_has_verified_terminal(self):
        payload = "x" * 4096
        with duckdb.connect(str(self.source)) as connection:
            connection.executemany(
                "INSERT INTO provider_health VALUES (?, 'unhealthy', NULL, NULL, ?, 1)",
                [(f"provider-{index}", payload) for index in range(300)],
            )
        limits = ImportLimits(
            max_file_bytes=64 * 1024**2,
            max_rows=1000,
            batch_rows=10,
            max_value_bytes=8 * 1024,
            max_row_bytes=16 * 1024,
            max_batch_bytes=64 * 1024,
            max_output_bytes=4 * 1024**2,
        )
        sink = MeasuringSink()
        status = self.importer(limits=limits).stream("extract", "provider_health", sink)
        self.assertEqual(status, 0)
        self.assertGreater(sink.calls, 20)
        self.assertLessEqual(sink.max_write, limits.max_batch_bytes)
        terminal = json.loads(sink.last)
        self.assertEqual(terminal["type"], "complete")
        self.assertEqual(terminal["row_count"], 300)
        self.assertGreaterEqual(terminal["batch_count"], 30)
        self.assertEqual(len(terminal["data_sha256"]), 64)

    def test_serialized_value_and_total_output_limits_fail_terminally(self):
        with duckdb.connect(str(self.source)) as connection:
            connection.execute(
                "INSERT INTO provider_health VALUES ('huge', 'unhealthy', NULL, NULL, ?, 1)",
                ["x" * 9000],
            )
        output = io.BytesIO()
        limits = ImportLimits(
            max_file_bytes=64 * 1024**2,
            max_rows=1000,
            batch_rows=2,
            max_value_bytes=8 * 1024,
            max_row_bytes=16 * 1024,
            max_batch_bytes=32 * 1024,
            max_output_bytes=64 * 1024,
        )
        status = self.importer(limits=limits).stream("extract", "provider_health", output)
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(status, 2)
        self.assertEqual(records[-1]["type"], "failed")
        self.assertEqual(records[-1]["error"]["code"], "value_limit_rejected")
        self.assertFalse(any(record["type"] == "complete" for record in records))

    def test_hard_timeout_kills_blocked_worker_within_bound(self):
        importer = BlockingImporter(
            allowed_root=self.root,
            source=self.source,
            source_label="test-fixture",
            limits=ImportLimits(
                max_file_bytes=64 * 1024**2, max_rows=1000, batch_rows=2,
                timeout_seconds=0.2,
            ),
        )
        output = io.BytesIO()
        before_children = {child.pid for child in multiprocessing.active_children()}
        started = time.monotonic()
        status = importer.stream("validate", None, output)
        elapsed = time.monotonic() - started
        self.assertEqual(status, 2)
        self.assertLess(elapsed, 1.5)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "timeout")
        self.assertEqual(
            {child.pid for child in multiprocessing.active_children()} - before_children,
            set(),
        )

    def test_truncated_child_stream_cannot_be_mistaken_for_success(self):
        importer = TruncatedImporter(
            allowed_root=self.root,
            source=self.source,
            source_label="test-fixture",
            limits=ImportLimits(max_file_bytes=64 * 1024**2, max_rows=1000, batch_rows=2),
        )
        output = io.BytesIO()
        status = importer.stream("extract", "provider_health", output)
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(status, 2)
        self.assertEqual(records[-1]["error"]["code"], "incomplete_stream")
        self.assertFalse(any(record["type"] == "complete" for record in records))

    def test_cli_errors_are_bounded_and_redacted(self):
        stdout = io.StringIO()
        secret = self.root / "fixture" / "password=plaintext.duckdb"
        secret.write_bytes(b"broken")
        with contextlib.redirect_stdout(stdout):
            status = main([
                "validate", "--allowed-root", str(self.root), "--source", str(secret),
                "--source-label", "test-fixture",
            ])
        payload = json.loads(stdout.getvalue())
        self.assertEqual(status, 2)
        self.assertEqual(payload["status"], "failed")
        rendered = json.dumps(payload)
        self.assertNotIn("plaintext", rendered)
        self.assertNotIn(str(self.root), rendered)


if __name__ == "__main__":
    unittest.main()
