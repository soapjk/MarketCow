from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from marketcow.providers.yahoo_fx import FxRateError, YahooFxProvider


def payload(rate: float, timestamp: int = 1785148800):
    return {
        "chart": {
            "result": [{
                "meta": {
                    "regularMarketPrice": rate,
                    "regularMarketTime": timestamp,
                },
                "timestamp": [timestamp],
                "indicators": {"quote": [{"close": [rate]}]},
            }],
            "error": None,
        }
    }


class ChartProvider:
    def __init__(self, rates=None):
        self.rates = rates or {"CNY=X": 7.2, "HKD=X": 7.8}
        self.calls = []
        self.error = None

    def _fetch_chart(self, symbol, _params):
        self.calls.append(symbol)
        if self.error is not None:
            raise self.error
        return payload(self.rates[symbol]), f"https://example/{symbol}"

    @staticmethod
    def _result(value):
        return value["chart"]["result"][0]


class YahooFxProviderTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)
        self.chart = ChartProvider()
        self.provider = YahooFxProvider(
            self.chart,
            cache_ttl_seconds=300,
            stale_max_seconds=3600,
            now_provider=lambda: self.now,
        )

    def test_fetches_auditable_usd_rates_and_then_uses_cache(self):
        first = self.provider.get_rates("USD", ["CNY", "HKD"])

        self.assertEqual(first["rates"], {"USD": 1.0, "CNY": 7.2, "HKD": 7.8})
        self.assertEqual(first["source"], "yahoo_chart")
        self.assertEqual(first["cache_status"], "refreshed")
        self.assertFalse(first["cached"])
        self.assertFalse(first["stale"])
        self.assertEqual(self.chart.calls, ["CNY=X", "HKD=X"])

        second = self.provider.get_rates("USD", ["CNY", "HKD"])
        self.assertEqual(second["cache_status"], "hit")
        self.assertTrue(second["cached"])
        self.assertEqual(self.chart.calls, ["CNY=X", "HKD=X"])

    def test_computes_cross_rate_without_hardcoding_market_values(self):
        result = self.provider.get_rates("CNY", ["USD", "HKD"])

        self.assertAlmostEqual(result["rates"]["USD"], 1 / 7.2)
        self.assertAlmostEqual(result["rates"]["HKD"], 7.8 / 7.2)
        self.assertEqual(result["rates"]["CNY"], 1.0)

    def test_returns_stale_cache_with_machine_readable_upstream_error(self):
        self.provider.get_rates("USD", ["CNY"])
        self.now += timedelta(seconds=301)
        self.chart.error = RuntimeError("offline")

        result = self.provider.get_rates("USD", ["CNY"])

        self.assertTrue(result["cached"])
        self.assertTrue(result["stale"])
        self.assertEqual(result["cache_status"], "stale_if_error")
        self.assertEqual(result["errors"][0]["code"], "provider_unavailable")

    def test_distinguishes_no_data_provider_unavailable_and_expired_cache(self):
        self.chart.rates["CNY=X"] = 0
        with self.assertRaises(FxRateError) as no_data:
            self.provider.get_rates("USD", ["CNY"])
        self.assertEqual(no_data.exception.code, "no_data")

        self.chart.rates["CNY=X"] = 7.2
        self.provider.get_rates("USD", ["CNY"])
        self.now += timedelta(seconds=3601)
        self.chart.error = RuntimeError("offline")
        with self.assertRaises(FxRateError) as stale:
            self.provider.get_rates("USD", ["CNY"])
        self.assertEqual(stale.exception.code, "stale_data")

        empty = YahooFxProvider(
            ChartProvider(), now_provider=lambda: self.now
        )
        empty.quote_provider.error = RuntimeError("offline")
        with self.assertRaises(FxRateError) as unavailable:
            empty.get_rates("USD", ["HKD"])
        self.assertEqual(unavailable.exception.code, "provider_unavailable")

    def test_rejects_unsupported_or_duplicate_currencies(self):
        with self.assertRaises(ValueError):
            self.provider.get_rates("EUR", ["USD"])
        with self.assertRaises(ValueError):
            self.provider.get_rates("USD", ["CNY", "CNY"])


if __name__ == "__main__":
    unittest.main()
