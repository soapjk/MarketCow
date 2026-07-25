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
            adjustment_factors=lambda _result, _symbol: [],
        )
        persisted = []
        service = SimpleNamespace(
            tushare_provider=provider,
            _persist_tushare_response=lambda api, *_args: persisted.append(api) or {
                "storage_path": f"/tmp/{api}", "artifact_id": f"artifact-{api}"
            },
            market_bar_repository=SimpleNamespace(
                upsert_price_bars=lambda *_args: 0,
                upsert_adjustment_factors=lambda *_args: 0,
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
        self.assertEqual(calls[1], (
            "adj_factor",
            {
                "ts_code": "600519.SH",
                "start_date": "20260101",
                "end_date": "20260102",
            },
            "ts_code,trade_date,adj_factor",
        ))
        self.assertEqual(persisted, ["stk_mins", "adj_factor"])
        self.assertIn(start.isoformat(), result["range"])
        self.assertIn(end.isoformat(), result["range"])
        self.assertEqual(result["adjustment_factor_count"], 0)

    def test_tushare_window_fails_when_a_bar_date_has_no_factor(self):
        provider = SimpleNamespace(
            name="tushare_fixture",
            base_url="https://example.test",
            call=lambda api, _params, _fields: (
                {
                    "data": {
                        "fields": ["trade_time", "close"],
                        "items": [["2026-01-02 09:35:00", 10]],
                    }
                }
                if api == "stk_mins"
                else {"data": {"fields": [], "items": []}}
            ),
            minute_bars=lambda _result: [{
                "bar_at": "2026-01-02T01:35:00+00:00", "close": 10
            }],
            adjustment_factors=lambda _result, _symbol: [],
        )
        service = SimpleNamespace(
            tushare_provider=provider,
            _persist_tushare_response=lambda api, *_args: {
                "storage_path": f"/tmp/{api}", "artifact_id": f"artifact-{api}"
            },
            market_bar_repository=SimpleNamespace(
                upsert_price_bars=lambda *_args: self.fail(
                    "bars must not be persisted without factors"
                ),
                upsert_adjustment_factors=lambda *_args: self.fail(
                    "incomplete factors must not be persisted"
                ),
            ),
            metadata_repository=SimpleNamespace(
                record_provider_health=lambda *_args: None
            ),
        )

        with self.assertRaisesRegex(ValueError, "missing 1 bar dates"):
            FundamentalService.refresh_tushare_minute_history_window(
                service,
                "600519.XSHG",
                datetime(2026, 1, 2, tzinfo=timezone.utc),
                datetime(2026, 1, 3, tzinfo=timezone.utc),
                "5m",
                "raw",
            )

    def test_raw_tushare_bars_are_enriched_with_the_daily_factor(self):
        saved_bars = []
        provider = SimpleNamespace(
            name="tushare_fixture",
            base_url="https://example.test",
            call=lambda api, _params, _fields: (
                {"data": {"fields": [], "items": []}}
                if api == "stk_mins"
                else {
                    "data": {
                        "fields": ["ts_code", "trade_date", "adj_factor"],
                        "items": [["600519.SH", "20260102", 12.3456]],
                    }
                }
            ),
            minute_bars=lambda _result: [{
                "bar_at": "2026-01-02T01:35:00+00:00",
                "close": 10,
                "raw_close": 10,
                "adjustment_factor": None,
            }],
            adjustment_factors=lambda _result, _symbol: [{
                "trade_date": "2026-01-02",
                "adjustment_factor": "12.3456",
            }],
        )
        service = SimpleNamespace(
            tushare_provider=provider,
            _persist_tushare_response=lambda api, *_args: {
                "storage_path": f"/tmp/{api}", "artifact_id": f"artifact-{api}"
            },
            market_bar_repository=SimpleNamespace(
                upsert_price_bars=lambda _symbol, _interval, _adjustment,
                _source, _ingested_at, bars, _provenance: (
                    saved_bars.extend(bars) or len(bars)
                ),
                upsert_adjustment_factors=lambda *_args: 1,
            ),
            metadata_repository=SimpleNamespace(
                record_provider_health=lambda *_args: None
            ),
        )

        result = FundamentalService.refresh_tushare_minute_history_window(
            service,
            "600519.XSHG",
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            datetime(2026, 1, 3, tzinfo=timezone.utc),
            "5m",
            "raw",
        )

        self.assertEqual(result["adjustment"], "raw")
        self.assertEqual(result["bars"][0]["raw_close"], 10)
        self.assertEqual(result["bars"][0]["adjustment_factor"], "12.3456")
        self.assertEqual(
            result["bars"][0]["corporate_action_factor"], "12.3456"
        )
        self.assertEqual(
            result["bars"][0]["applied_adjustment_multiplier"], "1"
        )
        self.assertEqual(result["bars"][0]["factor_source"], "tushare_fixture")
        self.assertEqual(
            result["bars"][0]["factor_artifact_id"], "artifact-adj_factor"
        )
        self.assertEqual(saved_bars[0]["adjustment_factor"], "12.3456")


if __name__ == "__main__":
    unittest.main()
