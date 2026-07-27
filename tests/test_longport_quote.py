import unittest
import os
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

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

    def static_info(self, symbols):
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
        self.assertEqual(normalize_longport_symbol("600519.XSHG"), (
            "600519.XSHG", "CN", "600519.SH",
        ))
        self.assertEqual(normalize_longport_symbol("700.XHKG"), (
            "700.XHKG", "HK", "700.HK",
        ))
        self.assertEqual(normalize_longport_symbol("BRK-B.XNYS"), (
            "BRK-B.XNYS", "US", "BRK.B.US",
        ))
        with self.assertRaises(ValueError):
            normalize_longport_symbol("CNY=X")

    def test_batch_is_one_sdk_call_and_preserves_request_order(self):
        context = FakeContext([quote("AAPL.US"), quote("700.HK", "321.4")])
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: context,
        )
        result = provider.fetch_quotes(["AAPL.XNAS", "700.XHKG"])
        self.assertEqual(context.calls, [["AAPL.US", "700.HK"]])
        self.assertEqual([row["symbol"] for row in result], ["AAPL.XNAS", "700.XHKG"])
        self.assertEqual([row["price"] for row in result], [101.25, 321.4])
        for row in result:
            validate_realtime_quote(row)

    def test_static_metadata_resolves_exchange_to_mic_without_guessing(self):
        context = FakeContext([
            SimpleNamespace(
                symbol="BRK.B.US", exchange="NYSE", currency="USD", lot_size=1,
                name_en="Berkshire Hathaway B", name_cn="",
            ),
            SimpleNamespace(
                symbol="MU.US", exchange="NASD", currency="USD", lot_size=1,
                name_en="Micron", name_cn="",
            ),
            SimpleNamespace(
                symbol="DRAM.US", exchange="AMEX", currency="USD", lot_size=1,
                name_en="Roundhill Memory ETF", name_cn="",
            ),
            SimpleNamespace(
                symbol="SPY.US", exchange="ARCA", currency="USD", lot_size=1,
                name_en="SPDR S&P 500 ETF", name_cn="",
            ),
        ])
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: context,
        )

        result = provider.resolve_instruments([
            "BRK.B.US", "MU.US", "DRAM.US", "SPY.US", "MISSING.US",
        ])

        self.assertEqual(context.calls, [[
            "BRK.B.US", "MU.US", "DRAM.US", "SPY.US", "MISSING.US",
        ]])
        self.assertEqual(
            [item.get("instrument_id") for item in result],
            ["BRK-B.XNYS", "MU.XNAS", "DRAM.XASE", "SPY.ARCX", None],
        )
        self.assertEqual(result[-1]["error"]["code"], "not_found")

    def test_unknown_or_conflicting_static_exchange_is_ambiguous(self):
        context = FakeContext([
            SimpleNamespace(
                symbol="ODD.US", exchange="UNKNOWN", currency="USD", lot_size=1,
                name_en="Odd", name_cn="",
            ),
        ])
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: context,
        )

        result = provider.resolve_instruments(["ODD.US"])

        self.assertEqual(result[0]["status"], "error")
        self.assertEqual(result[0]["error"]["code"], "ambiguous")

    def test_static_metadata_mixed_markets_preserve_request_order(self):
        context = FakeContext([
            SimpleNamespace(
                symbol="600519.SH", exchange="SSE", currency="CNY", lot_size=100,
                name_en="Kweichow Moutai", name_cn="贵州茅台",
            ),
            SimpleNamespace(
                symbol="700.HK", exchange="SEHK", currency="HKD", lot_size=100,
                name_en="Tencent", name_cn="腾讯控股",
            ),
            SimpleNamespace(
                symbol="MU.US", exchange="NASD", currency="USD", lot_size=1,
                name_en="Micron", name_cn="美光科技",
            ),
        ])
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: context,
        )

        result = provider.resolve_instruments([
            "MU.US", "600519.SH", "700.HK",
        ])

        self.assertEqual(
            [item["instrument_id"] for item in result],
            ["MU.XNAS", "600519.XSHG", "700.XHKG"],
        )
        self.assertEqual(
            [item["source_exchange"] for item in result],
            ["NASD", "SSE", "SEHK"],
        )

    def test_latest_extended_session_quote_is_selected(self):
        post = SimpleNamespace(
            last_done=Decimal("103.5"),
            timestamp=datetime(2026, 7, 21, 21, 15, tzinfo=timezone.utc),
        )
        context = FakeContext([quote("AAPL.US", post_market_quote=post)])
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: context,
        )
        result = provider.fetch_quote("AAPL.XNAS")
        self.assertEqual(result["price"], 103.5)
        self.assertEqual(result["session"], "post_market")

    def test_naive_sdk_timestamp_is_interpreted_as_utc_plus_eight(self):
        context = FakeContext([
            quote(
                "700.HK", "448",
                timestamp=datetime(2026, 7, 23, 10, 50, 21),
            ),
        ])
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: context,
        )

        result = provider.fetch_quote("700.XHKG")

        self.assertEqual(result["quote_at"], "2026-07-23T02:50:21+00:00")
        self.assertEqual(result["market"], "HK")
        self.assertEqual(result["currency"], "HKD")

    def test_naive_us_extended_session_uses_same_sdk_wall_timezone(self):
        post = SimpleNamespace(
            last_done=Decimal("103.5"),
            timestamp=datetime(2026, 7, 23, 7, 59, 59),
        )
        context = FakeContext([
            quote(
                "AAPL.US",
                timestamp=datetime(2026, 7, 23, 4, 0),
                post_market_quote=post,
            ),
        ])
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: context,
        )

        result = provider.fetch_quote("AAPL.XNAS")

        self.assertEqual(result["quote_at"], "2026-07-22T23:59:59+00:00")
        self.assertEqual(result["session"], "post_market")

    def test_aware_sdk_timestamp_preserves_the_instant(self):
        context = FakeContext([
            quote(
                "700.HK", "448",
                timestamp=datetime(
                    2026, 7, 23, 10, 50, 21,
                    tzinfo=ZoneInfo("Asia/Hong_Kong"),
                ),
            ),
        ])
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: context,
        )

        result = provider.fetch_quote("700.XHKG")

        self.assertEqual(result["quote_at"], "2026-07-23T02:50:21+00:00")

    def test_missing_credentials_and_sdk_errors_are_bounded_and_redacted(self):
        with self.assertRaisesRegex(LongPortError, "credentials are not configured"):
            LongPortQuoteProvider("", "", "").fetch_quote("AAPL.XNAS")
        secret = "do-not-leak"
        provider = LongPortQuoteProvider(
            "key", secret, "token",
            context_factory=lambda: FakeContext(error=RuntimeError(secret)),
        )
        with self.assertRaises(LongPortError) as raised:
            provider.fetch_quote("AAPL.XNAS")
        self.assertNotIn(secret, str(raised.exception))

    def test_incomplete_batch_fails_closed_and_close_is_idempotent(self):
        context = FakeContext([quote("AAPL.US")])
        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=lambda: context,
        )
        with self.assertRaisesRegex(LongPortError, "incomplete"):
            provider.fetch_quotes(["AAPL.XNAS", "MSFT.XNAS"])
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

        result = provider.fetch_spread("AAPL.XNAS")

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
            provider.fetch_spread("AAPL.XNAS")

        secret = "depth-secret"
        provider = LongPortQuoteProvider(
            "key", secret, "token",
            context_factory=lambda: FakeContext(error=RuntimeError(secret)),
        )
        with self.assertRaises(LongPortError) as raised:
            provider.fetch_spread("AAPL.XNAS")
        self.assertNotIn(secret, str(raised.exception))

    def test_spread_state_uses_provider_quote_time_and_trade_status(self):
        depth = SimpleNamespace(
            asks=[SimpleNamespace(
                position=1, price=Decimal("101.10"), volume=200, order_num=1
            )],
            bids=[SimpleNamespace(
                position=1, price=Decimal("101.00"), volume=400, order_num=3
            )],
        )

        class Context:
            def depth(self, _symbol):
                return depth

            def quote(self, _symbols):
                return [quote(
                    "AAPL.US",
                    timestamp=datetime(
                        2026, 7, 24, 14, 30, tzinfo=timezone.utc
                    ),
                )]

        provider = LongPortQuoteProvider(
            "key", "secret", "token", context_factory=Context
        )
        result = provider.fetch_spread_state("AAPL.XNAS")
        self.assertEqual(result["trade_status"], "active")
        self.assertEqual(result["session"], "regular")
        self.assertTrue(result["tradable"])
        self.assertEqual(result["quote_at"], "2026-07-24T14:30:00+00:00")


if __name__ == "__main__":
    unittest.main()
