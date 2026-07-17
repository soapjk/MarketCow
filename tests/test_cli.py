import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from marketcow.__main__ import diagnose, initialize, main, sync_cn
from marketcow.config import Settings


class FakeSyncService:
    def __init__(self, settings):
        self.settings = settings

    def refresh_market_fundamentals(self, report_period, include_valuation):
        return {
            "status": "success",
            "report_period": report_period or "latest",
            "include_valuation": include_valuation,
            "row_count": 5000,
        }

    def sync_tdx_financials(self, limit_periods):
        return {"status": "success", "period_count": limit_periods}


class PartiallyFailingSyncService(FakeSyncService):
    def refresh_market_fundamentals(self, report_period, include_valuation):
        raise RuntimeError("fundamentals source unavailable")


class CliTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        self.settings = Settings(root / "warehouse/market.duckdb", root / "raw")

    def tearDown(self):
        self.tempdir.cleanup()

    def test_init_and_doctor_prepare_a_ready_local_service(self):
        result = initialize(self.settings)
        diagnosis = diagnose(self.settings)

        self.assertEqual(result["status"], "ready")
        self.assertTrue(self.settings.database_path.exists())
        self.assertTrue(self.settings.raw_path.is_dir())
        self.assertEqual(diagnosis["status"], "ready")
        self.assertEqual(diagnosis["checks"]["database"]["data"]["fundamentals"], 0)

    def test_doctor_explains_when_init_has_not_run(self):
        diagnosis = diagnose(self.settings)

        self.assertEqual(diagnosis["status"], "attention")
        self.assertIn("marketcow init", diagnosis["checks"]["database"]["message"])

    def test_sync_cn_runs_bounded_default_steps(self):
        with patch("marketcow.__main__.FundamentalService", FakeSyncService):
            result = sync_cn(self.settings)

        self.assertEqual(result["steps"]["fundamentals"]["row_count"], 5000)
        self.assertEqual(result["steps"]["tdx_financials"]["period_count"], 4)

    def test_sync_cn_rejects_two_skip_flags(self):
        with patch("marketcow.__main__.FundamentalService", FakeSyncService):
            with self.assertRaisesRegex(ValueError, "nothing to do"):
                sync_cn(self.settings, skip_fundamentals=True, skip_tdx=True)

    def test_sync_cn_continues_independent_step_after_provider_failure(self):
        with patch("marketcow.__main__.FundamentalService", PartiallyFailingSyncService):
            result = sync_cn(self.settings)

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["steps"]["fundamentals"]["status"], "failed")
        self.assertEqual(result["steps"]["tdx_financials"]["status"], "success")

    def test_main_init_uses_configured_paths(self):
        with patch("marketcow.__main__.Settings.from_env", return_value=self.settings):
            with redirect_stdout(StringIO()):
                exit_code = main(["init"])

        self.assertEqual(exit_code, 0)
        self.assertTrue(self.settings.database_path.exists())
