import unittest
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock

import requests
from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.providers.eastmoney_realtime import EastmoneyRealtimeQuoteProvider
from marketcow.providers.sina_realtime import SinaRealtimeQuoteProvider
from marketcow.service import FundamentalService
from marketcow.storage import Warehouse


class SinaRealtimeQuoteProviderTest(unittest.TestCase):
    def test_normalizes_etf_symbol(self):
        self.assertEqual(
            SinaRealtimeQuoteProvider.sina_code("513180.SH"),
            ("513180.SH", "sh513180"),
        )
        self.assertEqual(
            SinaRealtimeQuoteProvider.sina_code("159583.SZ"),
            ("159583.SZ", "sz159583"),
        )

    def test_parses_a_share_quote(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.content = (
            'var hq_str_sh600036="招商银行,37.000,37.180,37.760,37.960,36.750,'
            '37.750,37.760,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,'
            '2026-07-15,15:34:59,00";'
        ).encode("gbk")
        provider = SinaRealtimeQuoteProvider()
        provider.session.get = Mock(return_value=response)

        quote = provider.fetch_quote("600036.SH")

        self.assertEqual(quote["price"], 37.76)
        self.assertEqual(quote["previous_close"], 37.18)
        self.assertEqual(quote["source"], "sina_finance_hq")
        self.assertEqual(quote["quote_at"], "2026-07-15T15:34:59+08:00")

    def test_all_fallbacks_share_one_wall_clock_budget(self):
        provider = SinaRealtimeQuoteProvider(timeout=0.1, request_budget=0.15)

        def timeout(*args, **kwargs):
            time.sleep(kwargs["timeout"])
            raise requests.Timeout("simulated timeout")

        provider.session.get = timeout
        provider.direct_session.get = timeout
        started = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, "request budget"):
            provider.fetch_quote("600036.SH")
        self.assertLess(time.monotonic() - started, 0.4)


class AShareQuoteRoutingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.settings = Settings(root / "test.duckdb", root / "raw")

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def quote(source: str, price: float = 37.76):
        return {
            "instrument_id": "CN.XSHG.600036",
            "symbol": "600036.SH",
            "name": "招商银行",
            "market": "CN",
            "exchange": "XSHG",
            "currency": "CNY",
            "price": price,
            "previous_close": 37.18,
            "change": price - 37.18,
            "change_pct": 1.56,
            "session": "regular",
            "quote_at": "2026-07-15T15:34:59+08:00",
            "source": source,
            "source_url": "https://example.test/quote",
            "raw_response_locator": "fixture",
            "_raw_payload": {"fixture": True},
        }

    def service(self, sina, eastmoney):
        return FundamentalService(
            self.settings,
            warehouse=Warehouse(self.settings.database_path),
            sina_quote_provider=sina,
            a_quote_provider=eastmoney,
        )

    def test_prefers_sina_for_a_share(self):
        sina = Mock(name="sina_finance_hq")
        sina.name = "sina_finance_hq"
        sina.fetch_quote.return_value = self.quote("sina_finance_hq")
        eastmoney = Mock(name="eastmoney_quote_center")
        eastmoney.name = "eastmoney_quote_center"
        service = self.service(sina, eastmoney)

        result = service.refresh_quote("600036.SH")

        self.assertEqual(result["source"], "sina_finance_hq")
        self.assertFalse(result["is_cached"])
        eastmoney.fetch_quote.assert_not_called()

    def test_falls_back_to_eastmoney_then_cache(self):
        sina = Mock()
        sina.name = "sina_finance_hq"
        sina.fetch_quote.side_effect = RuntimeError("sina unavailable")
        eastmoney = Mock()
        eastmoney.name = "eastmoney_quote_center"
        eastmoney.fetch_quote.return_value = self.quote("eastmoney_quote_center", 37.75)
        service = self.service(sina, eastmoney)
        fresh = service.refresh_quote("600036.SH")
        self.assertEqual(fresh["source"], "eastmoney_quote_center")

        eastmoney.fetch_quote.side_effect = RuntimeError("eastmoney unavailable")
        cached = service.refresh_quote("600036.SH")
        self.assertTrue(cached["is_cached"])
        self.assertEqual(cached["price"], 37.75)
        self.assertIn("sina unavailable", cached["cache_reason"])

    def test_upstream_timeouts_return_cached_quote_without_starving_snapshot(self):
        seed_sina = Mock()
        seed_sina.name = "sina_finance_hq"
        seed_sina.fetch_quote.return_value = self.quote("sina_finance_hq")
        service = self.service(seed_sina, Mock())
        service.refresh_quote("600036.SH")

        def timeout(*args, **kwargs):
            time.sleep(kwargs["timeout"])
            raise requests.Timeout("simulated timeout")

        sina = SinaRealtimeQuoteProvider(timeout=0.1, request_budget=0.15)
        sina.session.get = timeout
        sina.direct_session.get = timeout
        eastmoney = EastmoneyRealtimeQuoteProvider(timeout=0.1, request_budget=0.15)
        eastmoney.session.get = timeout
        service.sina_quote_provider = sina
        service.a_quote_provider = eastmoney
        client = TestClient(create_app(self.settings, service))

        started = time.monotonic()
        response = client.get("/v1/quotes/600036.SH")
        quote_elapsed = time.monotonic() - started
        started = time.monotonic()
        snapshot = client.get("/v1/snapshot?limit=10")
        snapshot_elapsed = time.monotonic() - started

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["is_cached"])
        self.assertLess(quote_elapsed, 0.8)
        self.assertEqual(snapshot.status_code, 200)
        self.assertLess(snapshot_elapsed, 0.5)


if __name__ == "__main__":
    unittest.main()
