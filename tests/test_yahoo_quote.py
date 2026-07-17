import unittest
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
        self.assertEqual(normalize_yahoo_symbol("00700.HK"), ("0700.HK", "HK"))
        self.assertEqual(normalize_yahoo_symbol("700"), ("0700.HK", "HK"))
        self.assertEqual(normalize_yahoo_symbol("aapl"), ("AAPL", "US"))
        self.assertEqual(normalize_yahoo_symbol("CNY=X"), ("CNY=X", "FX"))
        self.assertEqual(normalize_yahoo_symbol("HKD=X"), ("HKD=X", "FX"))

    def test_quote_uses_latest_prepost_bar(self):
        provider = YahooQuoteProvider()
        with patch.object(provider, "_fetch_chart", return_value=(PAYLOAD, "https://example/0700.HK")):
            quote = provider.fetch_quote("0700.HK")
        self.assertEqual(quote["price"], 500.0)
        self.assertEqual(quote["previous_close"], 490.0)
        self.assertEqual(quote["session"], "post_market")
        self.assertEqual(quote["price_adjustment"], "raw")

    def test_history_defaults_to_adjusted_ohlc(self):
        provider = YahooQuoteProvider()
        with patch.object(provider, "_fetch_chart", return_value=(PAYLOAD, "https://example/0700.HK")):
            history = provider.fetch_history("0700.HK", "1y", "1d", "adjusted")
        self.assertEqual(history["bars"][0]["close"], 242.5)
        self.assertEqual(history["bars"][0]["open"], 240.0)
        self.assertEqual(history["bars"][0]["adjustment_factor"], 0.5)


if __name__ == "__main__":
    unittest.main()
