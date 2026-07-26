import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from marketcow.providers.yahoo_quote import YahooQuoteProvider, normalize_yahoo_symbol


PAYLOAD = {
    "chart": {
        "result": [{
            "meta": {
                "shortName": "Tencent", "regularMarketPrice": 500.0,
                "previousClose": 490.0, "regularMarketTime": 1780000000,
                "currency": "HKD", "exchangeName": "HKG",
                "exchangeTimezoneName": "Asia/Hong_Kong", "timezone": "HKT",
                "currentTradingPeriod": {"post": {"start": 1779999000, "end": 1780001000}},
            },
            "timestamp": [1779996400, 1780000000],
            "indicators": {
                "quote": [{
                    "open": [480.0, 495.0], "high": [490.0, 505.0],
                    "low": [475.0, 492.0], "close": [485.0, 500.0],
                    "volume": [100, 200],
                }],
                "adjclose": [{"adjclose": [242.5, 250.0]}],
            },
        }],
        "error": None,
    }
}


class YahooQuoteProviderTest(unittest.TestCase):
    def test_normalizes_hk_and_us_symbols(self):
        self.assertEqual(normalize_yahoo_symbol("700.XHKG"), ("0700.HK", "HK"))
        self.assertEqual(normalize_yahoo_symbol("AAPL.XNAS"), ("AAPL", "US"))
        for value in ("00700.HK", "700", "aapl", "CNY=X"):
            with self.assertRaises(ValueError):
                normalize_yahoo_symbol(value)

    def test_quote_uses_latest_prepost_bar(self):
        provider = YahooQuoteProvider()
        with patch.object(provider, "_fetch_chart", return_value=(PAYLOAD, "https://example/0700.HK")):
            quote = provider.fetch_quote("700.XHKG")
        self.assertEqual(quote["instrument_id"], "700.XHKG")
        self.assertEqual(quote["provider_symbol"], "0700.HK")
        self.assertEqual(quote["price"], 500.0)
        self.assertEqual(quote["previous_close"], 490.0)
        self.assertEqual(quote["session"], "post_market")
        self.assertEqual(quote["price_adjustment"], "raw")

    def test_qfq_history_emits_explicit_adjustment_contract(self):
        provider = YahooQuoteProvider()
        with patch.object(provider, "_fetch_chart", return_value=(PAYLOAD, "https://example/0700.HK")):
            history = provider.fetch_history("700.XHKG", "1y", "1d", "qfq")
        self.assertEqual(history["instrument_id"], "700.XHKG")
        self.assertEqual(history["bars"][0]["close"], 242.5)
        self.assertEqual(history["bars"][0]["open"], 240.0)
        self.assertEqual(history["bars"][0]["adjustment_factor"], 0.5)
        self.assertEqual(history["adjustment"], "qfq")
        self.assertEqual(
            history["bars"][0]["corporate_action_factor"], "0.5"
        )
        self.assertEqual(
            history["bars"][0]["applied_adjustment_multiplier"], "0.5"
        )
        self.assertEqual(history["bars"][0]["reference_factor"], "1")

    def test_raw_history_keeps_real_factor_without_applying_it(self):
        provider = YahooQuoteProvider()
        with patch.object(
            provider, "_fetch_chart",
            return_value=(PAYLOAD, "https://example/0700.HK"),
        ):
            history = provider.fetch_history("700.XHKG", "1y", "1d", "raw")

        self.assertEqual(history["adjustment"], "raw")
        self.assertEqual(history["bars"][0]["close"], 485.0)
        self.assertEqual(
            history["bars"][0]["corporate_action_factor"], "0.5"
        )
        self.assertEqual(
            history["bars"][0]["applied_adjustment_multiplier"], "1.0"
        )
        self.assertIsNone(history["bars"][0]["adjustment_reference_date"])

    def test_history_window_uses_explicit_period_boundaries(self):
        provider = YahooQuoteProvider()
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 1, 8, tzinfo=timezone.utc)
        with patch.object(
            provider, "_fetch_chart",
            return_value=(PAYLOAD, "https://example/0700.HK"),
        ) as fetch:
            provider.fetch_history_window(
                "700.XHKG", start, end, "1m", "raw"
            )

        params = fetch.call_args.args[1]
        self.assertEqual(params["period1"], int(start.timestamp()))
        self.assertEqual(params["period2"], int(end.timestamp()))
        self.assertNotIn("range", params)


if __name__ == "__main__":
    unittest.main()
