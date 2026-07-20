import unittest
from unittest.mock import Mock, patch

from marketcow.providers.tushare_provider import TushareError, TushareProvider


class TushareProviderTest(unittest.TestCase):
    def provider_with(self, result, interval=0):
        response = Mock()
        response.json.return_value = result
        session = Mock()
        session.headers = {}
        session.post.return_value = response
        return TushareProvider("secret", "https://proxy.test", "https://rt.test", interval, session=session), session

    def test_generic_call_preserves_tushare_envelope(self):
        provider, session = self.provider_with({
            "code": 0, "msg": None,
            "data": {"fields": ["ts_code", "close"], "items": [["000001.SZ", 10.5]]},
        })
        result = provider.call("daily", {"trade_date": "20260717"}, "ts_code,close")
        self.assertEqual(result["data"]["items"][0][0], "000001.SZ")
        session.post.assert_called_once_with(
            "https://proxy.test/",
            json={"api_name": "daily", "token": "secret", "params": {"trade_date": "20260717"},
                  "fields": "ts_code,close"}, timeout=30.0,
        )

    def test_missing_token_fails_before_network(self):
        provider = TushareProvider("", "https://proxy.test", "https://rt.test", 0)
        with self.assertRaisesRegex(TushareError, "TUSHARE_TOKEN"):
            provider.call("daily")

    def test_upstream_business_error_is_raised(self):
        provider, _ = self.provider_with({"code": -2001, "msg": "permission denied"})
        with self.assertRaisesRegex(TushareError, "permission denied"):
            provider.call("daily")

    def test_minute_rows_are_normalized_without_losing_source_payload(self):
        result = {
            "code": 0,
            "data": {
                "fields": ["ts_code", "trade_time", "open", "high", "low", "close", "vol", "amount"],
                "items": [["600000.SH", "2026-07-17 09:35:00", 10.0, 10.2, 9.9, 10.1, 1200, 12120]],
            },
        }
        bars = TushareProvider.minute_bars(result)
        self.assertEqual(bars[0]["close"], 10.1)
        self.assertEqual(bars[0]["amount"], 12120)
        self.assertEqual(bars[0]["source_payload"]["ts_code"], "600000.SH")
        self.assertEqual(bars[0]["bar_at"], "2026-07-17T01:35:00+00:00")

    @patch("time.sleep")
    def test_rate_limit_is_enforced(self, sleep):
        provider, _ = self.provider_with({"code": 0, "data": {"fields": [], "items": []}}, 0.5)
        provider.call("daily")
        provider.call("weekly")
        sleep.assert_called()


if __name__ == "__main__":
    unittest.main()
