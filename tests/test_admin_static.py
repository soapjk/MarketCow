from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from tests.test_market_data_api import Service


ROOT = Path(__file__).resolve().parents[1]
ADMIN_INDEX = ROOT / "web" / "dist" / "index.html"


@unittest.skipUnless(ADMIN_INDEX.is_file(), "run npm build to verify the static admin mount")
class AdminStaticTest(unittest.TestCase):
    def test_built_frontend_is_mounted_under_admin(self):
        with TemporaryDirectory() as folder:
            root = Path(folder) / "runtime"
            settings = Settings(
                raw_path=root / "raw", storage_root=root, allowed_root=root.parent,
                postgres_dsn="postgresql://u:p@127.0.0.1/test",
                clickhouse_password="secret", profile="test", port=8793,
                postgres_schema="test", clickhouse_database="test",
                clickhouse_spool_path=root / "spool",
            )
            with TestClient(create_app(settings, Service())) as client:
                response = client.get("/admin/")
                self.assertEqual(response.status_code, 200)
                self.assertIn('<div id="root"></div>', response.text)
                self.assertIn('/admin/assets/', response.text)
                self.assertEqual(response.headers["x-content-type-options"], "nosniff")


if __name__ == "__main__":
    unittest.main()
