from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from marketcow.cross_market import cross_market_snapshot, exact_relationship
from marketcow.service import FundamentalService


class CrossMarketTest(unittest.TestCase):
    def setUp(self):
        self.derivative = {
            "instrument_id": "AAPL-PERP.XYZH",
            "instrument_type": "equity_perpetual",
            "symbol": "AAPL-PERP", "currency": "USD",
            "provider_symbols": {"hyperliquid": "xyz:AAPL"},
        }
        self.underlying = {
            "instrument_id": "AAPL.XNAS",
            "instrument_type": "equity", "symbol": "AAPL",
        }
        self.at = "2026-07-24T10:00:00+00:00"

    def test_exact_relationship_is_auditable(self):
        result = exact_relationship(self.derivative, self.underlying)
        self.assertEqual(
            result["relationship_id"], "AAPL-PERP.XYZH~AAPL.XNAS"
        )
        self.assertEqual(result["hedge_quality"], "exact_underlying")
        self.assertEqual(result["source"]["provider_symbol"], "xyz:AAPL")

    def test_snapshot_calculates_executable_sides_and_quality(self):
        result = cross_market_snapshot(
            exact_relationship(self.derivative, self.underlying),
            {
                "observed_at": self.at, "depth": 5,
                "bids": [{"price": "322.15", "size": "180"}],
                "asks": [{"price": "322.16", "size": "95"}],
            },
            {
                "observed_at": "2026-07-24T10:00:00.050+00:00",
                "best_bid": "321.71", "best_ask": "321.72",
                "bid_volume": 400, "ask_volume": 360,
                "bids": [{}] * 5, "asks": [{}] * 5,
            },
            {
                "mark_price": "322.14", "oracle_price": "321.72",
                "external_oracle_price": "321.70",
                "funding_rate": "0.00001", "open_interest": "1000",
                "market_status": "active", "oracle_status": "external_live",
            },
            max_age_ms=1000, max_skew_ms=250,
            now=datetime(2026, 7, 24, 10, 0, 0, 100000, tzinfo=timezone.utc),
        )
        self.assertEqual(
            result["gross_basis"]["short_derivative_long_underlying"]["absolute"],
            "0.43",
        )
        self.assertEqual(
            result["capacity"][
                "top_level_short_derivative_long_underlying"
            ],
            "180",
        )
        self.assertEqual(result["timing"]["data_skew_ms"], 50)
        self.assertIn(
            "underlying_session_unverified", result["quality"]["blockers"]
        )
        self.assertFalse(result["quality"]["usable_for_immediate_hedge"])

    def test_stale_and_skew_are_not_hidden(self):
        result = cross_market_snapshot(
            exact_relationship(self.derivative, self.underlying),
            {
                "observed_at": self.at, "depth": 1,
                "bids": [{"price": "10", "size": "1"}],
                "asks": [{"price": "11", "size": "1"}],
            },
            {
                "observed_at": "2026-07-24T10:00:01+00:00",
                "best_bid": "9", "best_ask": "10",
                "bid_volume": 1, "ask_volume": 1,
                "bids": [{}], "asks": [{}],
            },
            {
                "market_status": "active", "oracle_status": "unavailable",
            },
            max_age_ms=100, max_skew_ms=100,
            now=datetime(2026, 7, 24, 10, 0, 2, tzinfo=timezone.utc),
        )
        self.assertIn("stale_market_data", result["quality"]["blockers"])
        self.assertIn("data_skew_exceeded", result["quality"]["blockers"])
        self.assertIn("oracle_unavailable", result["quality"]["blockers"])

    def test_provider_timestamped_tradable_state_removes_session_blocker(self):
        result = cross_market_snapshot(
            exact_relationship(self.derivative, self.underlying),
            {
                "observed_at": self.at, "depth": 1,
                "bids": [{"price": "11", "size": "1"}],
                "asks": [{"price": "12", "size": "1"}],
            },
            {
                "observed_at": "2026-07-24T10:00:00.010+00:00",
                "quote_at": "2026-07-24T10:00:00.005+00:00",
                "best_bid": "10", "best_ask": "11",
                "bid_volume": 1, "ask_volume": 1,
                "bids": [{}], "asks": [{}],
                "session": "regular", "trade_status": "active",
                "tradable": True,
            },
            {
                "market_status": "active", "oracle_status": "external_live",
            },
            max_age_ms=100, max_skew_ms=100,
            now=datetime(
                2026, 7, 24, 10, 0, 0, 20_000, tzinfo=timezone.utc
            ),
        )
        self.assertEqual(result["quality"]["blockers"], [])
        self.assertTrue(result["quality"]["market_overlap"])
        self.assertTrue(result["quality"]["usable_for_immediate_hedge"])
        self.assertEqual(
            result["underlying"]["session_status"], "verified"
        )
        self.assertEqual(result["timing"]["underlying_status_age_ms"], 15)

    def test_closed_market_without_depth_returns_limited_snapshot(self):
        result = cross_market_snapshot(
            exact_relationship(self.derivative, self.underlying),
            {
                "observed_at": self.at, "depth": 1,
                "bids": [{"price": "11", "size": "1"}],
                "asks": [{"price": "12", "size": "1"}],
            },
            {
                "observed_at": self.at, "quote_at": self.at,
                "best_bid": None, "best_ask": None,
                "bid_volume": 0, "ask_volume": 0,
                "bids": [], "asks": [],
                "session": "closed", "trade_status": "active",
                "tradable": False,
            },
            {
                "market_status": "active", "oracle_status": "internal_only",
            },
            max_age_ms=100, max_skew_ms=100,
            now=datetime(
                2026, 7, 24, 10, 0, 0, 20_000, tzinfo=timezone.utc
            ),
        )
        self.assertIsNone(
            result["gross_basis"][
                "short_derivative_long_underlying"
            ]["absolute"]
        )
        self.assertIn("insufficient_depth", result["quality"]["blockers"])
        self.assertIn(
            "underlying_not_tradable", result["quality"]["blockers"]
        )
        self.assertFalse(result["quality"]["usable_for_immediate_hedge"])

    def test_service_uses_underlying_symbol_not_instrument_id_at_provider(self):
        rows = {
            "AAPL-PERP.XYZH": self.derivative,
            "AAPL.XNAS": self.underlying,
        }
        service = FundamentalService.__new__(FundamentalService)
        service.metadata_repository = SimpleNamespace(
            get_instrument=lambda key: rows.get(key)
        )
        calls = []
        service.hyperliquid_provider = SimpleNamespace(
            fetch_order_book=lambda symbol, depth: {
                "observed_at": self.at, "depth": depth,
                "bids": [{"price": "11", "size": "1"}],
                "asks": [{"price": "12", "size": "1"}],
            },
            fetch_asset_context=lambda symbol: {
                "market_status": "active", "oracle_status": "internal_only",
            },
        )
        service.longport_quote_provider = SimpleNamespace(
            fetch_spread=lambda symbol: (
                calls.append(symbol) or {
                    "observed_at": self.at,
                    "best_bid": "10", "best_ask": "11",
                    "bid_volume": 1, "ask_volume": 1,
                    "bids": [{}], "asks": [{}],
                }
            )
        )
        service.get_cross_market_snapshot(
            "AAPL-PERP.XYZH", "AAPL.XNAS",
            depth=1, max_age_ms=10**12, max_skew_ms=250,
        )
        self.assertEqual(calls, ["AAPL"])


if __name__ == "__main__":
    unittest.main()
