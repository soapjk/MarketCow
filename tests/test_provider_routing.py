import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.provider_routing import (
    MARKET_BAR_HISTORY,
    REALTIME_QUOTE,
    ProviderNotSupported,
    select_providers,
)


class StubService:
    def __init__(self):
        self.calls = []
        self.market_bar_repository = SimpleNamespace(get_latest_quotes=lambda _symbols: [])

    def refresh_quote(self, symbol, provider=None, allow_fallback=False):
        self.calls.append(("quote", symbol, provider, allow_fallback))
        return {"symbol": symbol, "price": 1.0, "source": provider or "auto"}

    def get_quote(self, symbol, force_refresh=False, provider=None, allow_fallback=False):
        return self.refresh_quote(symbol, provider, allow_fallback)

    def refresh_quote_history(
        self, symbol, range_, interval, adjustment, provider=None, allow_fallback=False
    ):
        self.calls.append((
            "history", symbol, range_, interval, adjustment, provider, allow_fallback
        ))
        return {"symbol": symbol, "bars": [], "count": 0, "source": provider or "auto"}

    def close(self):
        pass


class ProviderRoutingTest(unittest.TestCase):
    def setUp(self):
        self.folder = TemporaryDirectory()
        root = Path(self.folder.name)
        self.settings = Settings(root / "db.duckdb", root / "raw")

    def tearDown(self):
        self.folder.cleanup()

    def test_explicit_provider_is_strict_and_unsupported_is_rejected(self):
        self.assertEqual(
            select_providers(
                REALTIME_QUOTE, "CN", "tushare", ("sina", "eastmoney"),
                allow_fallback=False,
            ),
            ("tushare",),
        )
        with self.assertRaises(ProviderNotSupported):
            select_providers(
                REALTIME_QUOTE, "US", "sina", ("yahoo",), allow_fallback=False
            )

    def test_auto_selection_only_uses_capable_providers(self):
        self.assertEqual(
            select_providers(
                MARKET_BAR_HISTORY, "US", None, ("tushare", "yahoo", "sina"),
                allow_fallback=True,
            ),
            ("yahoo",),
        )

    def test_public_post_quote_query_passes_provider_policy(self):
        service = StubService()
        client = TestClient(create_app(self.settings, service))
        response = client.post("/v1/quotes/query", json={
            "symbols": ["000001.SZ", "600519.SH"],
            "refresh": True,
            "provider": "eastmoney",
            "allow_fallback": False,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["count"], 2)
        self.assertEqual(service.calls, [
            ("quote", "000001.SZ", "eastmoney", False),
            ("quote", "600519.SH", "eastmoney", False),
        ])

    def test_public_post_history_query_is_capability_named(self):
        service = StubService()
        client = TestClient(create_app(self.settings, service))
        response = client.post("/v1/market-bars/query", json={
            "symbols": ["AAPL"], "range": "5d", "interval": "1d",
            "adjustment": "adjusted", "refresh": True, "provider": "yahoo",
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(service.calls[0], (
            "history", "AAPL", "5d", "1d", "adjusted", "yahoo", False,
        ))

    def test_provider_specific_routes_are_not_public_schema(self):
        schema = TestClient(create_app(self.settings, StubService())).get("/openapi.json").json()
        paths = schema["paths"]
        self.assertTrue(paths["/v1/tushare/{api_name}"]["post"]["deprecated"])
        self.assertTrue(paths["/v1/tushare/realtime-quote"]["post"]["deprecated"])
        self.assertIn("/v1/quotes/query", paths)
        self.assertIn("/v1/market-bars/query", paths)

    def test_provider_requires_upstream_refresh(self):
        client = TestClient(create_app(self.settings, StubService()))
        response = client.post("/v1/quotes/query", json={
            "symbols": ["AAPL"], "provider": "yahoo", "refresh": False,
        })
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"]["code"], "provider_requires_refresh")


if __name__ == "__main__":
    unittest.main()
