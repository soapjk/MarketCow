from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from tests.test_provider_routing import StubService


class CsvImports:
    def __init__(self):
        self.calls = []

    def dry_run(self, path, declaration, max_error_samples=100):
        self.calls.append(("dry", path, declaration, max_error_samples))
        return {"status": "valid", "rows_valid": 1}

    def create_import(self, path, declaration, **kwargs):
        self.calls.append(("create", path, declaration, kwargs))
        return {"job_id": "job", "status": "queued"}, True

    def list(self, limit):
        return [{"job_id": "job", "status": "running"}]

    def get(self, job_id):
        return {"job_id": job_id, "status": "running"}

    def cancel(self, job_id):
        return {"job_id": job_id, "status": "cancel_requested"}


def declaration():
    return {
        "source": "vendor", "interval": "1m", "adjustment": "raw",
        "profile": {
            "name": "vendor", "version": "1",
            "columns": {
                "symbol": "symbol", "timestamp": "time", "open": "open",
                "high": "high", "low": "low", "close": "close",
            },
            "timezone_name": "UTC",
        },
        "instruments": {
            "namespace": "provider:vendor",
            "symbols": {"AAPL.US": "AAPL.XNAS"},
        },
    }


class CsvImportApiTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        root = Path(self.folder.name)
        settings = Settings(
            raw_path=root / "raw", storage_root=root / "storage",
            allowed_root=root, postgres_dsn="postgresql://u:p@127.0.0.1/test",
            clickhouse_password="secret", profile="test", port=8793,
            postgres_schema="test", clickhouse_database="test",
            clickhouse_spool_path=root / "spool",
        )
        self.app = create_app(settings, StubService())
        self.imports = CsvImports()
        self.app.state.csv_import_service = self.imports
        self.client = TestClient(self.app)

    def tearDown(self):
        self.folder.cleanup()

    def test_dry_run_create_list_get_and_cancel_share_one_service(self):
        dry = self.client.post("/v1/admin/csv-imports/dry-run", json={
            "path": "/allowed/vendor.csv", "declaration": declaration(),
            "max_error_samples": 5,
        })
        self.assertEqual(dry.status_code, 200)
        created = self.client.post("/v1/admin/csv-imports", json={
            "path": "/allowed/vendor.csv", "declaration": declaration(),
            "idempotency_key": "vendor-import-1", "chunk_rows": 1000,
            "max_attempts": 2,
        })
        self.assertEqual(created.status_code, 200)
        self.assertTrue(created.json()["created"])
        self.assertEqual(
            self.client.get("/v1/admin/csv-imports").json()["count"], 1
        )
        self.assertEqual(
            self.client.get("/v1/admin/csv-imports/job").json()["job_id"], "job"
        )
        canceled = self.client.post("/v1/admin/csv-imports/job/cancel")
        self.assertEqual(canceled.json()["status"], "cancel_requested")
        self.assertEqual(self.imports.calls[0][0], "dry")
        self.assertEqual(self.imports.calls[1][0], "create")

    def test_ui_contains_dry_run_progress_and_cancel_controls(self):
        response = self.client.get("/v1/admin/csv-imports-ui")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Dry-run", response.text)
        self.assertIn("Start import", response.text)
        self.assertIn("cancelJob", response.text)
        self.assertIn("setInterval(load,2000)", response.text)

    def test_us_mapping_is_not_inferred_by_api_model(self):
        payload = declaration()
        payload["instruments"]["symbols"] = {"AAPL.US": "AAPL.US"}
        response = self.client.post("/v1/admin/csv-imports/dry-run", json={
            "path": "/allowed/vendor.csv", "declaration": payload,
        })
        self.assertEqual(response.status_code, 400)
        self.assertIn("SYMBOL.MIC", response.json()["detail"])


if __name__ == "__main__":
    unittest.main()
