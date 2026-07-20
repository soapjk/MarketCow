import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from marketcow.__main__ import is_loopback_host
from marketcow.config import Settings


class SettingsTest(unittest.TestCase):
    def test_defaults_runtime_data_to_current_directory(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch.dict(os.environ, {}, clear=True), patch("pathlib.Path.cwd", return_value=Path(folder)):
                settings = Settings.from_env()

        self.assertEqual(settings.database_path, Path(folder) / "data/warehouse/market_data.duckdb")
        self.assertEqual(settings.raw_path, Path(folder) / "data/raw")
        self.assertEqual(settings.profile, "production")
        self.assertEqual(settings.port, 8790)

    def test_development_defaults_are_isolated(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch.dict(os.environ, {}, clear=True), patch("pathlib.Path.cwd", return_value=Path(folder)):
                settings = Settings.from_env("development")

        self.assertEqual(settings.database_path, Path(folder) / "data-development/warehouse/market_data.duckdb")
        self.assertEqual(settings.raw_path, Path(folder) / "data-development/raw")
        self.assertEqual(settings.profile, "development")
        self.assertEqual(settings.port, 8791)

    def test_development_rejects_production_port_and_paths(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with patch("pathlib.Path.cwd", return_value=root):
                with self.assertRaisesRegex(ValueError, "production port"):
                    Settings(root / "data-development/db.duckdb", root / "data-development/raw", port=8790,
                             profile="development").validate_runtime_isolation()
                with self.assertRaisesRegex(ValueError, "production data paths"):
                    Settings(root / "data/warehouse/market_data.duckdb", root / "data/raw", port=8791,
                             profile="development").validate_runtime_isolation()

    def test_unknown_profile_is_rejected(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "production or development"):
                Settings.from_env("staging")

    def test_postgres_metadata_is_explicit_and_development_only(self):
        root = Path("/tmp/marketcow-config-test")
        base = dict(
            database_path=root / "db.duckdb",
            raw_path=root / "raw",
            metadata_backend="postgres",
            postgres_dsn="postgresql://localhost/marketcow_development",
        )
        with self.assertRaisesRegex(ValueError, "development-only"):
            Settings(**base).validate_runtime_isolation()
        with self.assertRaisesRegex(ValueError, "POSTGRES_DSN"):
            Settings(
                root / "db.duckdb", root / "raw", profile="development", port=8791,
                metadata_backend="postgres", postgres_dsn="",
            ).validate_runtime_isolation()
        with self.assertRaisesRegex(ValueError, "schema must end"):
            Settings(
                **base, profile="development", port=8791,
                postgres_schema="marketcow_production",
            ).validate_runtime_isolation()

    def test_postgres_environment_configuration(self):
        with tempfile.TemporaryDirectory() as folder:
            environment = {
                "MARKETCOW_METADATA_BACKEND": "postgres",
                "MARKETCOW_POSTGRES_DSN": "postgresql://localhost/marketcow_development",
                "MARKETCOW_POSTGRES_SCHEMA": "tenant_test",
            }
            with patch.dict(os.environ, environment, clear=True), patch(
                "pathlib.Path.cwd", return_value=Path(folder)
            ):
                settings = Settings.from_env("development")
        self.assertEqual(settings.metadata_backend, "postgres")
        self.assertEqual(settings.postgres_schema, "tenant_test")
        settings.validate_runtime_isolation()

    def test_clickhouse_is_explicit_loopback_and_development_only(self):
        root = Path("/tmp/marketcow-config-test")
        base = dict(database_path=root / "db.duckdb", raw_path=root / "raw",
                    clickhouse_enabled=True)
        with self.assertRaisesRegex(ValueError, "development-only"):
            Settings(**base).validate_runtime_isolation()
        with self.assertRaisesRegex(ValueError, "loopback"):
            Settings(**base, profile="development", port=8791,
                     clickhouse_host="clickhouse.example.com").validate_runtime_isolation()
        with self.assertRaisesRegex(ValueError, "database must end"):
            Settings(**base, profile="development", port=8791,
                     clickhouse_database="marketcow_production").validate_runtime_isolation()

    def test_clickhouse_environment_configuration_is_disabled_by_default(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch.dict(os.environ, {}, clear=True), patch(
                "pathlib.Path.cwd", return_value=Path(folder)
            ):
                default = Settings.from_env("development")
            environment = {
                "MARKETCOW_CLICKHOUSE_ENABLED": "true",
                "MARKETCOW_CLICKHOUSE_HOST": "localhost",
                "MARKETCOW_CLICKHOUSE_PORT": "18123",
                "MARKETCOW_CLICKHOUSE_DATABASE": "tenant_test",
                "MARKETCOW_CLICKHOUSE_USERNAME": "fixture",
                "MARKETCOW_CLICKHOUSE_PASSWORD": "secret",
                "MARKETCOW_CLICKHOUSE_BATCH_SIZE": "10000",
                "MARKETCOW_CLICKHOUSE_SPOOL": str(Path(folder) / "spool"),
            }
            with patch.dict(os.environ, environment, clear=True), patch(
                "pathlib.Path.cwd", return_value=Path(folder)
            ):
                enabled = Settings.from_env("development")
        self.assertFalse(default.clickhouse_enabled)
        self.assertTrue(enabled.clickhouse_enabled)
        self.assertEqual(enabled.clickhouse_port, 18123)
        self.assertEqual(enabled.clickhouse_database, "tenant_test")
        self.assertEqual(enabled.clickhouse_batch_size, 10000)
        self.assertEqual(enabled.clickhouse_spool_path, Path(folder) / "spool")
        enabled.validate_runtime_isolation()

    def test_clickhouse_rejects_unsafe_batch_and_spool_configuration(self):
        root = Path("/tmp/marketcow-config-test")
        base = dict(database_path=root / "db.duckdb", raw_path=root / "raw",
                    profile="development", port=8791, clickhouse_enabled=True,
                    clickhouse_database="marketcow_test")
        with self.assertRaisesRegex(ValueError, "batch size"):
            Settings(**base, clickhouse_batch_size=100).validate_runtime_isolation()
        with patch("pathlib.Path.cwd", return_value=root):
            with self.assertRaisesRegex(ValueError, "production data"):
                Settings(**base, clickhouse_spool_path=root / "data/spool").validate_runtime_isolation()

    def test_marketcow_home_changes_both_default_paths(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch.dict(os.environ, {"MARKETCOW_HOME": folder}, clear=True):
                settings = Settings.from_env()

        self.assertEqual(settings.database_path, Path(folder) / "warehouse/market_data.duckdb")
        self.assertEqual(settings.raw_path, Path(folder) / "raw")

    def test_loopback_host_detection(self):
        self.assertTrue(is_loopback_host("127.0.0.1"))
        self.assertTrue(is_loopback_host("::1"))
        self.assertTrue(is_loopback_host("localhost"))
        self.assertFalse(is_loopback_host("0.0.0.0"))
        self.assertFalse(is_loopback_host("example.com"))
