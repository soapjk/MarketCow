from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from marketcow.history_coverage import (
    assess_history_response,
    split_history_shard,
)
from marketcow.history_capabilities import provider_history_capability


class HistoryCoverageTest(unittest.TestCase):
    def test_silent_truncation_below_row_budget_is_found_by_session_dates(self):
        start = datetime(2026, 7, 20, tzinfo=timezone.utc)
        end = datetime(2026, 7, 24, tzinfo=timezone.utc)
        bars = [
            {"bar_at": (start + timedelta(seconds=index)).isoformat()}
            for index in range(2999)
        ]

        report = assess_history_response(
            "tushare", "1m", bars, start, end
        )

        self.assertEqual(report["status"], "split_required")
        self.assertIn("missing_session_dates", report["reasons"])
        self.assertEqual(
            report["missing_session_dates"],
            ["2026-07-21", "2026-07-22", "2026-07-23"],
        )

    def test_covered_sessions_below_budget_are_provisionally_complete(self):
        start = datetime(2026, 7, 20, tzinfo=timezone.utc)
        end = datetime(2026, 7, 24, tzinfo=timezone.utc)
        bars = [
            {
                "bar_at": (
                    start + timedelta(days=day, hours=2, minutes=minute)
                ).isoformat()
            }
            for day in range(4)
            for minute in range(242)
        ]

        report = assess_history_response(
            "tushare", "1m", bars, start, end
        )

        self.assertEqual(report["status"], "provisionally_complete")
        self.assertEqual(report["missing_session_dates"], [])

    def test_minimum_tushare_shard_cannot_be_split(self):
        request = {
            "provider": "tushare", "interval": "1m", "adjustment": "raw"
        }
        shard = {
            "range_start": "2026-07-20T00:00:00+00:00",
            "range_end": "2026-07-21T00:00:00+00:00",
        }
        self.assertEqual(
            split_history_shard(shard, "600519.XSHG", request, 1), []
        )

    def test_documented_suspension_exempts_an_absent_session(self):
        start = datetime(2026, 7, 20, tzinfo=timezone.utc)
        end = datetime(2026, 7, 22, tzinfo=timezone.utc)
        bars = [
            {
                "bar_at": (
                    datetime(2026, 7, 20, 2, tzinfo=timezone.utc)
                    + timedelta(minutes=minute)
                ).isoformat()
            }
            for minute in range(242)
        ]

        report = assess_history_response(
            "tushare", "1m", bars, start, end, ["2026-07-21"]
        )

        self.assertEqual(report["status"], "provisionally_complete")
        self.assertEqual(report["exempt_session_dates"], ["2026-07-21"])

    def test_yahoo_capability_uses_instrument_mic_calendar(self):
        self.assertEqual(
            provider_history_capability(
                "yahoo", "1m", "600519.XSHG"
            ).calendar_name,
            "XSHG",
        )
        self.assertEqual(
            provider_history_capability(
                "yahoo", "1m", "700.XHKG"
            ).calendar_name,
            "XHKG",
        )
        self.assertEqual(
            provider_history_capability(
                "yahoo", "1m", "AAPL.XNAS"
            ).calendar_name,
            "XNYS",
        )


if __name__ == "__main__":
    unittest.main()
