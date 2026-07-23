import unittest
import os
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from marketcow.providers.contracts import validate_realtime_quote
from marketcow.providers.longport_quote import (
    LongPortError,
    LongPortQuoteProvider,
    _direct_connection_environment,
    normalize_longport_symbol,
)


def quote(symbol, price="101.25", timestamp=None, **sessions):
    return SimpleNamespace(
        symbol=symbol,
        last_done=Decimal(price),
        prev_close=Decimal("100"),
        open=Decimal("100.5"),
        high=Decimal("102"),
        low=Decimal("99.5"),
        timestamp=timestamp or datetime(2026, 7, 21, 14, 30, tzinfo=timezone.utc),
        volume=1234,
        turnover=Decimal("125000"),
        trade_status=SimpleNamespace(name="Normal"),
        pre_market_quote=sessions.get("pre_market_quote"),
        post_market_quote=sessions.get("post_market_quote"),
        overnight_quote=sessions.get("overnight_quote"),
    )


class FakeContext:
    def __init__(self, rows=None, error=None):
        self.rows = rows or []
        self.error = error
        self.calls = []
        self.closed = False

    def quote(self, symbols):
        self.calls.append(symbols)
        if self.error:
            raise self.error
        return self.rows

    def depth(self, symbol):
        self.calls.append(symbol)
        if self.error:
            raise self.error
        return self.rows

    def close(self):
        self.closed = True


class LongPortQuoteProviderTest(unittest.TestCase):
    def test_direct_connection_environment_restores_proxy_settings(self):
        names = ("http_proxy", "https_proxy", "all_proxy",
                 "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY")
        previous = {name: os.environ.get(name) for name in names}
        try:
            os.environ["https_proxy"] = "socks5h://127.0.0.1:7890"
            os.environ.pop("HTTP_PROXY", None)
            with _direct_connection_environment():
                self.assertTrue(all(name not in os.environ for name in names))
            self.assertEqual(
                os.environ["https_proxy"], "socks5h://127.0.0.1:7890"
            )
            self.assertNotIn("HTTP_PROXY", os.environ)
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value

    def test_symbol_mapping_covers_current_market_contract(self):
        self.assertEqual(normalize_longport_symbol("600519.SH"), (
            "600519.SH", "CN", "600519.SH",
        ))
        self.assertEqual(normalize_longport_symbol("0700.HK"), (
            "0700.HK", "HK", "700.HK",
        ))
        self.assertEqual(normalize_longport_symbol("BRK.B"), (
            "BRK-B", "US", "BRK.B.US",
        ))
        with self.assertRaises(ValueError):
            normalize_longport_symbol("CNY=X")

    def test_batch_is_one_sdk_call_and_preserves_request_order(self):
        context = FakeContext([quote("AAPL.US"), quote("700.HK", "321.4")])
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: context,
        )
        result = provider.fetch_quotes(["AAPL", "0700.HK"])
        self.assertEqual(context.calls, [["AAPL.US", "700.HK"]])
        self.assertEqual([row["symbol"] for row in result], ["AAPL", "0700.HK"])
        self.assertEqual([row["price"] for row in result], [101.25, 321.4])
        for row in result:
            validate_realtime_quote(row)

    def test_latest_extended_session_quote_is_selected(self):
        post = SimpleNamespace(
            last_done=Decimal("103.5"),
            timestamp=datetime(2026, 7, 21, 21, 15, tzinfo=timezone.utc),
        )
        context = FakeContext([quote("AAPL.US", post_market_quote=post)])
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: context,
        )
        result = provider.fetch_quote("AAPL")
        self.assertEqual(result["price"], 103.5)
        self.assertEqual(result["session"], "post_market")

    def test_missing_credentials_and_sdk_errors_are_bounded_and_redacted(self):
        with self.assertRaisesRegex(LongPortError, "credentials are not configured"):
            LongPortQuoteProvider("", "", "").fetch_quote("AAPL")
        secret = "do-not-leak"
        provider = LongPortQuoteProvider(
            "key", secret, "token",
            context_factory=lambda: FakeContext(error=RuntimeError(secret)),
        )
        with self.assertRaises(LongPortError) as raised:
            provider.fetch_quote("AAPL")
        self.assertNotIn(secret, str(raised.exception))

    def test_incomplete_batch_fails_closed_and_close_is_idempotent(self):
        context = FakeContext([quote("AAPL.US")])
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: context,
        )
        with self.assertRaisesRegex(LongPortError, "incomplete"):
            provider.fetch_quotes(["AAPL", "MSFT"])
        provider.close()
        provider.close()
        self.assertTrue(context.closed)

    def test_depth_calculates_top_of_book_spread_and_preserves_levels(self):
        depth = SimpleNamespace(
            asks=[
                SimpleNamespace(position=2, price=Decimal("101.20"), volume=300, order_num=2),
                SimpleNamespace(position=1, price=Decimal("101.10"), volume=200, order_num=1),
            ],
            bids=[
                SimpleNamespace(position=2, price=Decimal("100.90"), volume=500, order_num=4),
                SimpleNamespace(position=1, price=Decimal("101.00"), volume=400, order_num=3),
            ],
        )
        context = FakeContext(depth)
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: context,
        )

        result = provider.fetch_spread("AAPL")

        self.assertEqual(context.calls, ["AAPL.US"])
        self.assertEqual(result["best_bid"], 101.0)
        self.assertEqual(result["best_ask"], 101.1)
        self.assertAlmostEqual(result["spread"], 0.1)
        self.assertAlmostEqual(result["spread_bps"], 9.896091044, places=6)
        self.assertEqual([row["position"] for row in result["asks"]], [1, 2])
        self.assertEqual(result["bid_volume"], 400)

    def test_depth_requires_both_sides_and_redacts_sdk_errors(self):
        one_sided = SimpleNamespace(
            asks=[SimpleNamespace(position=1, price=Decimal("101"), volume=1, order_num=1)],
            bids=[],
        )
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: FakeContext(one_sided),
        )
        with self.assertRaisesRegex(LongPortError, "two-sided"):
            provider.fetch_spread("AAPL")

        secret = "depth-secret"
        provider = LongPortQuoteProvider(
            "key", secret, "token",
            context_factory=lambda: FakeContext(error=RuntimeError(secret)),
        )
        with self.assertRaises(LongPortError) as raised:
            provider.fetch_spread("AAPL")
        self.assertNotIn(secret, str(raised.exception))


if __name__ == "__main__":
    unittest.main()
