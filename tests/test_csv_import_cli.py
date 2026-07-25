from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from marketcow.__main__ import (
    _public_csv_job,
    _write_csv_import_evidence,
    build_parser,
)
from marketcow.config import Settings


class CsvImportCliTest(unittest.TestCase):
    def test_import_bars_parser_exposes_dry_run_and_durable_options(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            settings = Settings(
                raw_path=root / "raw", storage_root=root / "storage",
                allowed_root=root, postgres_dsn="postgresql://u:p@127.0.0.1/test",
                clickhouse_password="secret", profile="test", port=8793,
                postgres_schema="test", clickhouse_database="test",
                clickhouse_spool_path=root / "spool",
            )
            args = build_parser(settings).parse_args([
                "import-bars", "--file", "bars.csv", "--config", "vendor.json",
                "--dry-run",
            ])
        self.assertEqual(args.command, "import-bars")
        self.assertTrue(args.dry_run)
        self.assertEqual(args.chunk_rows, 100000)
        self.assertEqual(args.max_attempts, 3)
        self.assertEqual(args.evidence_output, "")

    def test_smoke_evidence_is_redacted_and_create_only(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "evidence.json"
            payload = _public_csv_job({
                "job_id": "job", "status": "succeeded",
                "storage_path": "/secret/purchased.csv",
                "request_json": {"path": "/secret/purchased.csv"},
                "quality_report_json": {"status": "passed"},
            })
            _write_csv_import_evidence(str(target), payload)
            body = target.read_text(encoding="utf-8")
            self.assertNotIn("/secret", body)
            self.assertIn('"status": "succeeded"', body)
            with self.assertRaises(FileExistsError):
                _write_csv_import_evidence(str(target), payload)


if __name__ == "__main__":
    unittest.main()
