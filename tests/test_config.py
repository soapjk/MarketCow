from __future__ import annotations

import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from marketcow.config import Settings


class SettingsTest(unittest.TestCase):
    def settings(self, root: Path) -> Settings:
        return Settings(
            raw_path=root / "raw", storage_root=root,
            allowed_root=root.parent,
            postgres_dsn="postgresql://user:password@127.0.0.1/marketcow_test",
            clickhouse_password="secret", profile="test", port=8793,
            postgres_schema="marketcow_test", clickhouse_database="marketcow_test",
            clickhouse_spool_path=root / "spool/clickhouse",
        )

    def test_from_env_uses_single_profile_contract(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            root = Path(folder)
            env = {
                "MARKETCOW_PROFILE": "test", "MARKETCOW_HOME": str(root),
                "MARKETCOW_ALLOWED_ROOT": str(root.parent),
                "MARKETCOW_POSTGRES_DSN":
                    "postgresql://user:password@127.0.0.1/marketcow_test",
                "MARKETCOW_CLICKHOUSE_PASSWORD": "secret",
            }
            with patch.dict(os.environ, env, clear=True):
                settings = Settings.from_env()
            settings.validate_preflight()
            self.assertEqual(settings.profile, "test")
            self.assertEqual(settings.postgres_schema, "marketcow_test")
            self.assertEqual(settings.clickhouse_database, "marketcow_test")

    def test_old_profile_and_missing_database_credentials_fail(self):
        with patch.dict(os.environ, {"MARKETCOW_PROFILE": "v2-test"}, clear=True):
            with self.assertRaisesRegex(ValueError, "production, development or test"):
                Settings.from_env()
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            root = Path(folder)
            settings = self.settings(root)
            with self.assertRaisesRegex(ValueError, "PostgreSQL"):
                replace(settings, postgres_dsn="").validate_preflight()
            with self.assertRaisesRegex(ValueError, "ClickHouse"):
                replace(settings, clickhouse_password="").validate_preflight()

    def test_project_longport_credentials_take_precedence(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            root = Path(folder)
            env = {
                "MARKETCOW_PROFILE": "test", "MARKETCOW_HOME": str(root),
                "MARKETCOW_ALLOWED_ROOT": str(root.parent),
                "MARKETCOW_POSTGRES_DSN":
                    "postgresql://user:password@127.0.0.1/marketcow_test",
                "MARKETCOW_CLICKHOUSE_PASSWORD": "secret",
                "MARKETCOW_LONGPORT_APP_KEY": "project-key",
                "MARKETCOW_LONGPORT_APP_SECRET": "project-secret",
                "MARKETCOW_LONGPORT_ACCESS_TOKEN": "project-token",
                "LONGBRIDGE_APP_KEY": "sdk-key",
            }
            with patch.dict(os.environ, env, clear=True):
                settings = Settings.from_env()
            self.assertEqual(settings.longport_app_key, "project-key")
            self.assertEqual(settings.longport_app_secret, "project-secret")
            self.assertEqual(settings.longport_access_token, "project-token")

    def test_paths_and_targets_are_isolated(self):
        with tempfile.TemporaryDirectory(suffix="-test") as folder:
            root = Path(folder)
            settings = self.settings(root)
            settings.validate_preflight()
            with self.assertRaisesRegex(ValueError, "escapes"):
                replace(settings, raw_path=root.parent / "outside").validate_preflight()
            with self.assertRaisesRegex(ValueError, "loopback"):
                replace(settings, clickhouse_host="example.com").validate_preflight()


if __name__ == "__main__":
    unittest.main()
