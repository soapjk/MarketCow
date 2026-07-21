import os
import socket
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from marketcow.__main__ import is_loopback_host
from marketcow.config import Settings


class SettingsTest(unittest.TestCase):
    @staticmethod
    def _v2_settings(root: Path, **overrides):
        values = dict(
            database_path=None,
            raw_path=root / "raw",
            storage_root=root,
            clickhouse_spool_path=root / "spool/clickhouse",
            profile="v2-development",
            port=8792,
            runtime_architecture="postgres_clickhouse_v2",
            metadata_backend="postgres",
            postgres_dsn="postgresql://fixture:secret@127.0.0.1/marketcow_development",
            postgres_dsn_ref="MARKETCOW_V2_POSTGRES_DSN",
            postgres_schema="marketcow_development",
            clickhouse_enabled=True,
            clickhouse_host="127.0.0.1",
            clickhouse_database="marketcow_development",
            clickhouse_password="secret",
            clickhouse_password_ref="MARKETCOW_V2_CLICKHOUSE_PASSWORD",
            market_bar_read_backend="clickhouse_canonical",
            raw_market_bar_read_backend="clickhouse_raw",
            v2_allowed_root=root.parent,
            runtime_config_schema="marketcow.v2-runtime-config.v1",
        )
        values.update(overrides)
        return Settings(**values)

    def test_v2_environment_defaults_to_pg_clickhouse_without_duckdb(self):
        with tempfile.TemporaryDirectory() as folder:
            environment = {
                "MARKETCOW_V2_POSTGRES_DSN":
                    "postgresql://fixture:secret@127.0.0.1/marketcow_development",
                "MARKETCOW_POSTGRES_DSN_REF": "MARKETCOW_V2_POSTGRES_DSN",
                "MARKETCOW_V2_CLICKHOUSE_PASSWORD": "secret",
                "MARKETCOW_CLICKHOUSE_PASSWORD_REF":
                    "MARKETCOW_V2_CLICKHOUSE_PASSWORD",
                "MARKETCOW_V2_ALLOWED_ROOT": folder,
            }
            with patch.dict(os.environ, environment, clear=True), patch(
                "pathlib.Path.cwd", return_value=Path(folder)
            ):
                settings = Settings.from_env("v2-development")
        self.assertIsNone(settings.database_path)
        self.assertEqual(settings.metadata_backend, "postgres")
        self.assertTrue(settings.clickhouse_enabled)
        self.assertEqual(settings.market_bar_read_backend, "clickhouse_canonical")
        self.assertEqual(settings.raw_market_bar_read_backend, "clickhouse_raw")
        self.assertEqual(settings.storage_root, Path(folder) / "data-v2-development")
        settings.validate_runtime_isolation()

    def test_v2_production_requires_dedicated_loopback_targets_and_root(self):
        with tempfile.TemporaryDirectory() as folder:
            environment = {
                "MARKETCOW_V2_POSTGRES_DSN":
                    "postgresql://fixture:secret@127.0.0.1/marketcow_production",
                "MARKETCOW_POSTGRES_DSN_REF": "MARKETCOW_V2_POSTGRES_DSN",
                "MARKETCOW_V2_CLICKHOUSE_PASSWORD": "secret",
                "MARKETCOW_CLICKHOUSE_PASSWORD_REF":
                    "MARKETCOW_V2_CLICKHOUSE_PASSWORD",
                "MARKETCOW_V2_ALLOWED_ROOT": folder,
            }
            with patch.dict(os.environ, environment, clear=True), patch(
                "pathlib.Path.cwd", return_value=Path(folder)
            ):
                settings = Settings.from_env("v2-production")
        self.assertEqual(settings.port, 8790)
        self.assertIsNone(settings.database_path)
        self.assertEqual(settings.storage_root, Path(folder) / "data-v2-production")
        self.assertEqual(settings.postgres_schema, "marketcow_production")
        self.assertEqual(settings.clickhouse_database, "marketcow_production")
        settings.validate_runtime_isolation()

        with self.assertRaisesRegex(ValueError, "must use port 8790"):
            self._v2_settings(
                Path(folder) / "data-v2-production",
                profile="v2-production", port=8792,
                postgres_dsn="postgresql://fixture:secret@127.0.0.1/marketcow_production",
                postgres_schema="marketcow_production",
                clickhouse_database="marketcow_production",
            ).validate_runtime_isolation()

    def test_v2_environment_requires_explicit_root_and_nonempty_secret_values(self):
        with tempfile.TemporaryDirectory() as folder:
            base = {
                "MARKETCOW_POSTGRES_DSN_REF": "MARKETCOW_V2_POSTGRES_DSN",
                "MARKETCOW_V2_POSTGRES_DSN":
                    "postgresql://fixture:secret@127.0.0.1/marketcow_development",
                "MARKETCOW_CLICKHOUSE_PASSWORD_REF":
                    "MARKETCOW_V2_CLICKHOUSE_PASSWORD",
                "MARKETCOW_V2_CLICKHOUSE_PASSWORD": "clickhouse-secret",
                "MARKETCOW_V2_ALLOWED_ROOT": folder,
            }
            cases = (
                ({"MARKETCOW_V2_ALLOWED_ROOT": None}, "explicit allowed root"),
                ({"MARKETCOW_V2_POSTGRES_DSN": None}, "requires PostgreSQL credentials"),
                ({"MARKETCOW_V2_POSTGRES_DSN": ""}, "requires PostgreSQL credentials"),
                ({"MARKETCOW_V2_CLICKHOUSE_PASSWORD": None},
                 "requires ClickHouse credentials"),
                ({"MARKETCOW_V2_CLICKHOUSE_PASSWORD": ""},
                 "requires ClickHouse credentials"),
            )
            for changes, message in cases:
                environment = dict(base)
                for key, value in changes.items():
                    if value is None:
                        environment.pop(key, None)
                    else:
                        environment[key] = value
                with self.subTest(changes=changes), patch.dict(
                    os.environ, environment, clear=True
                ), patch("pathlib.Path.cwd", return_value=Path(folder)):
                    settings = Settings.from_env("v2-development")
                    with self.assertRaisesRegex(ValueError, message) as captured:
                        settings.validate_runtime_isolation()
                    self.assertNotIn("clickhouse-secret", str(captured.exception))
                    self.assertNotIn("fixture:secret", str(captured.exception))

    def test_v2_preflight_requires_both_databases_and_forbids_duckdb(self):
        root = Path("/Volumes/T9/projects/marketcow-storage-v2/data-v2-development")
        base = self._v2_settings(root)
        base.validate_runtime_isolation()
        cases = (
            ({"postgres_dsn": ""}, "requires PostgreSQL"),
            ({"clickhouse_enabled": False}, "requires ClickHouse"),
            ({"metadata_backend": "duckdb"}, "must be PostgreSQL"),
            ({"market_bar_read_backend": "duckdb"}, "must use ClickHouse"),
            ({"raw_market_bar_read_backend": "duckdb"}, "must use ClickHouse"),
            ({"database_path": root / "warehouse.duckdb"}, "must not define a DuckDB"),
            ({"v2_allowed_root": None}, "explicit allowed root"),
            ({"clickhouse_password": ""}, "requires ClickHouse credentials"),
            ({"clickhouse_password": "   "}, "requires ClickHouse credentials"),
        )
        with patch.object(Path, "mkdir") as mkdir, patch.object(
            socket, "create_connection"
        ) as connect, patch.object(threading.Thread, "start") as start, patch(
            "builtins.open"
        ) as opened:
            for overrides, message in cases:
                with self.subTest(overrides=overrides), self.assertRaisesRegex(
                    ValueError, message
                ):
                    self._v2_settings(root, **overrides).validate_runtime_isolation()
        mkdir.assert_not_called()
        connect.assert_not_called()
        start.assert_not_called()
        opened.assert_not_called()

    def test_v2_preflight_rejects_production_and_non_loopback_targets(self):
        root = Path("/Volumes/T9/projects/marketcow-storage-v2/data-v2-development")
        cases = (
            ({"port": 8790}, "production port"),
            ({"host": "0.0.0.0"}, "service host must be loopback"),
            ({"postgres_dsn": "postgresql://user:topsecret@db.example/marketcow_development"},
             "PostgreSQL target must be loopback"),
            ({"postgres_dsn": "postgresql://user:topsecret@127.0.0.1/marketcow_production"},
             "database must match"),
            ({"postgres_schema": "marketcow_production"},
             "schema must match"),
            ({"clickhouse_host": "clickhouse.example"},
             "ClickHouse target must be loopback"),
            ({"clickhouse_database": "marketcow_production"},
             "ClickHouse database must match"),
        )
        for overrides, message in cases:
            with self.subTest(overrides=overrides), self.assertRaisesRegex(
                ValueError, message
            ) as captured:
                self._v2_settings(root, **overrides).validate_runtime_isolation()
            self.assertNotIn("topsecret", str(captured.exception))

    def test_v2_preflight_is_pure_and_rejects_escape_before_side_effects(self):
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            root = base / "data-v2-development"
            outside = base / "outside"
            root.mkdir()
            outside.mkdir()
            link = root / "linked"
            link.symlink_to(outside, target_is_directory=True)
            settings = self._v2_settings(root, clickhouse_spool_path=link / "spool")
            with patch.object(Path, "mkdir") as mkdir, patch.object(
                socket, "create_connection"
            ) as connect, patch.object(threading.Thread, "start") as start, patch(
                "builtins.open"
            ) as opened:
                with self.assertRaisesRegex(ValueError, "must stay within"):
                    settings.validate_runtime_isolation()
            mkdir.assert_not_called()
            connect.assert_not_called()
            start.assert_not_called()
            opened.assert_not_called()
            allowed = base / "allowed"
            allowed.mkdir()
            root_link = allowed / "data-v2-development"
            root_link.symlink_to(outside, target_is_directory=True)
            escaped = self._v2_settings(
                root_link, v2_allowed_root=allowed,
                raw_path=root_link / "raw",
                clickhouse_spool_path=root_link / "spool/clickhouse",
            )
            with self.assertRaisesRegex(ValueError, "escapes its allowed root"):
                escaped.validate_runtime_isolation()

    def test_v2_timeout_and_secret_reference_bounds(self):
        root = Path("/tmp/marketcow/data-v2-test")
        valid = self._v2_settings(
            root, profile="v2-test", port=8793,
            postgres_dsn="postgresql://user:secret@localhost/marketcow_test",
            postgres_schema="marketcow_test", clickhouse_database="marketcow_test",
        )
        valid.validate_runtime_isolation()
        for field in ("postgres_connect_timeout", "postgres_read_timeout",
                      "clickhouse_connect_timeout", "clickhouse_read_timeout"):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "timeout"):
                self._v2_settings(root, profile="v2-test", port=8793,
                                  postgres_dsn="postgresql://u:s@localhost/marketcow_test",
                                  postgres_schema="marketcow_test",
                                  clickhouse_database="marketcow_test",
                                  **{field: 31}).validate_runtime_isolation()
        with self.assertRaisesRegex(ValueError, "environment reference"):
            self._v2_settings(
                root, profile="v2-test", port=8793,
                postgres_dsn="postgresql://u:s@localhost/marketcow_test",
                postgres_schema="marketcow_test", clickhouse_database="marketcow_test",
                postgres_dsn_ref="secret-value",
            ).validate_runtime_isolation()

    def test_background_scheduler_is_explicit_bounded_and_development_only(self):
        root = Path("/tmp/marketcow-scheduler/data-development")
        base = dict(
            database_path=root / "db", raw_path=root / "raw", storage_root=root,
            clickhouse_spool_path=root / "spool/clickhouse",
            clickhouse_database="marketcow_test", clickhouse_background_canonical=True,
        )
        with self.assertRaisesRegex(ValueError, "development-only"):
            Settings(**base, clickhouse_enabled=True).validate_runtime_isolation()
        with self.assertRaisesRegex(ValueError, "CLICKHOUSE_ENABLED"):
            Settings(**base, profile="development", port=8791).validate_runtime_isolation()
        Settings(
            **base, profile="development", port=8791, clickhouse_enabled=True,
            clickhouse_scheduler_queue_cap=1, clickhouse_scheduler_scan_limit=1,
            clickhouse_scheduler_poll_seconds=0.05,
        ).validate_runtime_isolation()
        with self.assertRaisesRegex(ValueError, "mutually exclusive"):
            Settings(
                **base, profile="development", port=8791, clickhouse_enabled=True,
                clickhouse_auto_canonical=True,
            ).validate_runtime_isolation()
        with self.assertRaisesRegex(ValueError, "queue cap"):
            Settings(
                **base, profile="development", port=8791, clickhouse_enabled=True,
                clickhouse_scheduler_queue_cap=0,
            ).validate_runtime_isolation()

    def test_auto_canonical_is_explicit_development_only(self):
        root = Path("/tmp/marketcow-auto/data-development")
        base = dict(database_path=root / "db", raw_path=root / "raw",
                    clickhouse_auto_canonical=True)
        with self.assertRaisesRegex(ValueError, "development-only"):
            Settings(**base, clickhouse_enabled=True).validate_runtime_isolation()
        with self.assertRaisesRegex(ValueError, "CLICKHOUSE_ENABLED"):
            Settings(**base, profile="development", port=8791).validate_runtime_isolation()
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
            with self.assertRaisesRegex(ValueError, "must be production"):
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
                "MARKETCOW_CLICKHOUSE_SPOOL": str(
                    Path(folder) / "data-development/spool/clickhouse"
                ),
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
        self.assertEqual(
            enabled.clickhouse_spool_path,
            Path(folder) / "data-development/spool/clickhouse",
        )
        enabled.validate_runtime_isolation()

    def test_canonical_read_backend_is_explicit_and_development_only(self):
        root = Path("/tmp/marketcow-config-test/data-development")
        base = dict(
            database_path=root / "db.duckdb", raw_path=root / "raw",
            clickhouse_database="marketcow_test", storage_root=root,
            clickhouse_spool_path=root / "spool/clickhouse",
            market_bar_read_backend="clickhouse_canonical",
        )
        with self.assertRaisesRegex(ValueError, "development-only"):
            Settings(**base, clickhouse_enabled=True).validate_runtime_isolation()
        with self.assertRaisesRegex(ValueError, "CLICKHOUSE_ENABLED"):
            Settings(**base, profile="development", port=8791).validate_runtime_isolation()
        Settings(
            **base, profile="development", port=8791, clickhouse_enabled=True
        ).validate_runtime_isolation()
        with self.assertRaisesRegex(ValueError, "must be duckdb or"):
            Settings(
                root / "db.duckdb", root / "raw", profile="development", port=8791,
                market_bar_read_backend="unknown",
            ).validate_runtime_isolation()

    def test_raw_read_backend_is_independent_explicit_and_development_only(self):
        root = Path("/tmp/marketcow-config-test/data-development")
        base = dict(
            database_path=root / "db.duckdb", raw_path=root / "raw",
            storage_root=root, clickhouse_spool_path=root / "spool/clickhouse",
            clickhouse_database="marketcow_test",
            raw_market_bar_read_backend="clickhouse_raw",
        )
        with self.assertRaisesRegex(ValueError, "development-only"):
            Settings(**base, clickhouse_enabled=True).validate_runtime_isolation()
        with self.assertRaisesRegex(ValueError, "CLICKHOUSE_ENABLED"):
            Settings(**base, profile="development", port=8791).validate_runtime_isolation()
        settings = Settings(
            **base, profile="development", port=8791, clickhouse_enabled=True
        )
        settings.validate_runtime_isolation()
        self.assertEqual(settings.market_bar_read_backend, "duckdb")
        with self.assertRaisesRegex(ValueError, "must be duckdb or clickhouse_raw"):
            Settings(
                root / "db.duckdb", root / "raw", profile="development", port=8791,
                raw_market_bar_read_backend="unknown",
            ).validate_runtime_isolation()

    def test_clickhouse_rejects_unsafe_batch_and_spool_configuration(self):
        root = Path("/tmp/marketcow-config-test")
        base = dict(database_path=root / "db.duckdb", raw_path=root / "raw",
                    profile="development", port=8791, clickhouse_enabled=True,
                    clickhouse_database="marketcow_test",
                    storage_root=root / "data-development")
        with self.assertRaisesRegex(ValueError, "batch size"):
            Settings(**base, clickhouse_batch_size=100).validate_runtime_isolation()
        with self.assertRaisesRegex(ValueError, "connect timeout"):
            Settings(**base, clickhouse_connect_timeout=60).validate_runtime_isolation()
        formal = Path("/Volumes/T9/projects/market-data-service/data/spool/clickhouse")
        with self.assertRaisesRegex(ValueError, "within development storage root"):
            Settings(**base, clickhouse_spool_path=formal).validate_runtime_isolation()
        Settings(
            **base,
            clickhouse_spool_path=root / "data-development/spool/clickhouse",
        ).validate_runtime_isolation()
        with tempfile.TemporaryDirectory() as folder:
            temporary = Path(folder)
            development = temporary / "data-development"
            outside = temporary / "outside"
            development.mkdir()
            outside.mkdir()
            local_base = {**base, "storage_root": development}
            with self.assertRaisesRegex(ValueError, "within development storage root"):
                Settings(
                    **local_base, clickhouse_spool_path=development / "../outside/spool"
                ).validate_runtime_isolation()
            link = development / "linked-outside"
            link.symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "within development storage root"):
                Settings(
                    **local_base, clickhouse_spool_path=link / "spool"
                ).validate_runtime_isolation()

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
