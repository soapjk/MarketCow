from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from marketcow.service import FundamentalService


class TushareHistoryWindowTest(unittest.TestCase):
    def test_tushare_window_uses_exact_formatted_boundaries(self):
        calls = []
        provider = SimpleNamespace(
            name="tushare_fixture",
            base_url="https://example.test",
            call=lambda api, params, fields: calls.append(
                (api, params, fields)
            ) or {"data": {"fields": [], "items": []}},
            minute_bars=lambda _result: [],
        )
        service = SimpleNamespace(
            tushare_provider=provider,
            _persist_tushare_response=lambda *_args: {
                "storage_path": "/tmp/raw", "artifact_id": "artifact"
            },
            market_bar_repository=SimpleNamespace(
                upsert_price_bars=lambda *_args: 0
            ),
            metadata_repository=SimpleNamespace(
                record_provider_health=lambda *_args: None
            ),
        )
        start = datetime(2026, 1, 1, 1, 2, 3, tzinfo=timezone.utc)
        end = datetime(2026, 1, 2, 4, 5, 6, tzinfo=timezone.utc)

        result = FundamentalService.refresh_tushare_minute_history_window(
            service, "600519.XSHG", start, end, "5m", "raw"
        )

        self.assertEqual(calls[0][0], "stk_mins")
        self.assertEqual(calls[0][1]["start_date"], "2026-01-01 01:02:03")
        self.assertEqual(calls[0][1]["end_date"], "2026-01-02 04:05:06")
        self.assertIn(start.isoformat(), result["range"])
        self.assertIn(end.isoformat(), result["range"])


if __name__ == "__main__":
    unittest.main()
