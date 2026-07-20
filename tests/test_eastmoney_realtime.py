import unittest
import time
from unittest.mock import Mock

import requests

from marketcow.providers.eastmoney_realtime import EastmoneyRealtimeQuoteProvider, normalize_a_symbol


class EastmoneyRealtimeQuoteProviderTest(unittest.TestCase):
    def test_normalizes_etf_symbol(self):
        self.assertEqual(normalize_a_symbol("513180"), "513180.SH")
        self.assertEqual(normalize_a_symbol("513180.SH"), "513180.SH")

    def test_maps_etf_quote_scale(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"data": {"f43": 603, "f57": "513180", "f58": "恒生科技ETF华夏", "f59": 3, "f60": 595, "f86": 1784103114, "f170": 134}}
        provider = EastmoneyRealtimeQuoteProvider()
        provider.session.get = Mock(return_value=response)

        quote = provider.fetch_quote("513180.SH")

        self.assertEqual(quote["price"], 0.603)
        self.assertEqual(quote["change_pct"], 1.34)
        self.assertEqual(quote["currency"], "CNY")

    def test_retries_and_curl_share_one_wall_clock_budget(self):
        provider = EastmoneyRealtimeQuoteProvider(timeout=0.1, request_budget=0.15)

        def timeout(*args, **kwargs):
            time.sleep(kwargs["timeout"])
            raise requests.Timeout("simulated timeout")

        provider.session.get = timeout
        started = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, "budget exhausted"):
            provider.fetch_quote("513180.SH")
        self.assertLess(time.monotonic() - started, 0.4)


if __name__ == "__main__":
    unittest.main()
