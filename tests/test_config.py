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
