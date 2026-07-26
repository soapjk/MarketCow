from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from marketcow.__main__ import (
    _public_csv_job,
    _write_csv_import_evidence,
    build_parser,
    import_csv_bars,
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

    def test_cli_disables_background_scheduler_to_avoid_server_lease(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = root / "declaration.json"
            config.write_text("""{
              "contract_version":"marketcow.csv-bars.v2",
              "source":"vendor","interval":"1m","adjustment":"raw",
              "profile":{"name":"vendor","version":"1",
                "columns":{"timestamp":"t","open":"o","high":"h",
                  "low":"l","close":"c"},
                "timezone_name":"UTC","fixed_external_symbol":"AAPL.US"},
              "instruments":{"namespace":"provider:vendor",
                "symbols":{"AAPL.US":"AAPL.XNAS"}}
            }""", encoding="utf-8")
            settings = Settings(
                raw_path=root / "raw", storage_root=root,
                allowed_root=root, postgres_dsn="postgresql://local",
                clickhouse_password="secret",
                clickhouse_background_canonical=True,
            )
            captured = {}

            class Imports:
                def dry_run(self, *_args):
                    return {
                        "contract_version": "marketcow.csv-bars.v2",
                        "status": "valid", "file": {"name": "sample.csv"},
                        "source": "vendor", "profile": "vendor@1",
                        "namespace": "provider:vendor", "interval": "1m",
                        "adjustment": "raw",
                    }

                def close(self):
                    pass

            class Service:
                def __init__(self, value):
                    captured["settings"] = value

                def close(self):
                    pass

            with patch(
                "marketcow.service.FundamentalService", Service
            ), patch(
                "marketcow.csv_import_service.create_csv_import_service",
                return_value=Imports(),
            ):
                result = import_csv_bars(settings, SimpleNamespace(
                    config=str(config), file=str(root / "sample.csv"),
                    dry_run=True, evidence_output="",
                ))
        self.assertEqual(result["status"], "valid")
        self.assertFalse(
            captured["settings"].clickhouse_background_canonical
        )


if __name__ == "__main__":
    unittest.main()
