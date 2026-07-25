from __future__ import annotations

import unittest
from datetime import datetime, timezone

from marketcow.market_data_contracts import (
    InstrumentContract,
    validate_instrument_identity,
)
from marketcow.providers.hyperliquid import (
    HyperliquidProvider,
    hyperliquid_instrument,
)


class Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class Session:
    def __init__(self):
        self.headers = {}
        self.calls = []

    def post(self, _url, json, timeout):
        self.calls.append((json, timeout))
        kind = json["type"]
        if kind == "metaAndAssetCtxs":
            if json.get("dex") == "xyz":
                return Response([
                    {"universe": [{
                        "name": "xyz:AAPL", "szDecimals": 2,
                        "maxLeverage": 10,
                    }]},
                    [{
                        "midPx": "322.15", "markPx": "322.14",
                        "oraclePx": "321.72", "externalPerpPx": "321.70",
                        "funding": "0.00001", "openInterest": "12345",
                    }],
                ])
            return Response([
                {"universe": [{"name": "BTC", "szDecimals": 5}]},
                [{
                    "midPx": "65000.5", "markPx": "65000.0",
                    "oraclePx": "65010.0", "funding": "0.0000125",
                    "openInterest": "123", "prevDayPx": "64000",
                }],
            ])
        if kind == "perpDexs":
            return Response([None, {"name": "xyz"}])
        if kind == "spotMetaAndAssetCtxs":
            return Response([
                {
                    "tokens": [
                        {
                            "name": "USDC", "index": 0, "szDecimals": 8,
                            "tokenId": "0x00",
                        },
                        {
                            "name": "HYPE", "index": 150, "szDecimals": 2,
                            "tokenId": "0x96",
                        },
                    ],
                    "universe": [{
                        "name": "@107", "index": 107, "tokens": [150, 0],
                        "isCanonical": True,
                    }],
                },
                [{"midPx": "58.44", "prevDayPx": "57.00"}],
            ])
        if kind == "allMids":
            if json.get("dex") == "xyz":
                return Response({"xyz:AAPL": "322.15"})
            return Response({"BTC": "65000.5", "@107": "58.44"})
        if kind == "l2Book":
            return Response({
                "coin": json["coin"], "time": 1_700_000_000_000,
                "levels": [
                    [{"px": "322.15", "sz": "100", "n": 2}],
                    [{"px": "322.16", "sz": "80", "n": 1}],
                ],
            })
        if kind == "candleSnapshot":
            return Response([{
                "t": 1_700_000_000_000, "T": 1_700_003_599_999,
                "s": "BTC", "i": "1h", "o": "64000", "h": "65100",
                "l": "63900", "c": "65000", "v": "12.5", "n": 100,
            }])
        if kind == "fundingHistory":
            return Response([{
                "coin": "BTC", "time": 1_700_000_000_000,
                "fundingRate": "0.0000125", "premium": "-0.0001",
            }])
        raise AssertionError(kind)


class HyperliquidProviderTest(unittest.TestCase):
    def setUp(self):
        self.provider = HyperliquidProvider(timeout=1, request_budget=2)
        self.provider.session = Session()

    def test_explicit_symbol_identity_avoids_equity_ambiguity(self):
        self.assertEqual(
            hyperliquid_instrument("btc-perp.hypl"),
            ("BTC-PERP.HYPL", "BTC", "crypto_perpetual"),
        )
        self.assertEqual(
            hyperliquid_instrument("hype-usdc.hypl"),
            ("HYPE-USDC.HYPL", "HYPE/USDC", "crypto_spot"),
        )
        with self.assertRaises(ValueError):
            hyperliquid_instrument("BTC")

    def test_catalog_preserves_spot_index_and_token_identity(self):
        rows = self.provider.instruments()
        perp = next(row for row in rows if row["instrument_id"] == "BTC-PERP.HYPL")
        spot = next(row for row in rows if row["instrument_id"] == "HYPE-USDC.HYPL")
        self.assertEqual(perp["provider_symbols"]["hyperliquid"], "BTC")
        self.assertEqual(spot["provider_symbols"]["hyperliquid"], "@107")
        self.assertEqual(spot["venue_metadata"]["spot_index"], 107)
        self.assertEqual(spot["venue_metadata"]["token_ids"], ["0x96", "0x00"])
        hip3 = next(row for row in rows if row["instrument_id"] == "AAPL-PERP.XYZH")
        self.assertEqual(hip3["instrument_type"], "equity_perpetual")
        self.assertEqual(
            hip3["provider_symbols"]["hyperliquid"], "xyz:AAPL"
        )
        for row in rows:
            payload = {
                key: value for key, value in row.items()
                if key != "venue_metadata"
            }
            contract = InstrumentContract.model_validate({
                **payload, "schema_version": 1,
            })
            validate_instrument_identity(contract)

    def test_hip3_order_book_and_context(self):
        book = self.provider.fetch_order_book("AAPL-PERP.XYZH", 5)
        context = self.provider.fetch_asset_context("AAPL-PERP.XYZH")
        self.assertEqual(book["bids"][0]["price"], "322.15")
        self.assertEqual(book["bids"][0]["order_count"], 2)
        self.assertEqual(context["external_oracle_price"], "321.70")
        self.assertEqual(context["oracle_status"], "external_live")

    def test_quote_includes_perpetual_context(self):
        quote = self.provider.fetch_quote("BTC-PERP.HYPL")
        self.assertEqual(quote["price"], 65000.5)
        self.assertEqual(quote["mark_price"], "65000.0")
        self.assertEqual(quote["oracle_price"], "65010.0")
        self.assertEqual(quote["funding_rate"], "0.0000125")
        self.assertEqual(quote["open_interest"], "123")

    def test_history_normalizes_candles_and_requires_raw(self):
        result = self.provider.fetch_history(
            "BTC-PERP.HYPL", "5d", "1h", "raw"
        )
        self.assertEqual(result["instrument_id"], "BTC-PERP.HYPL")
        self.assertEqual(result["bars"][0]["close"], 65000.0)
        self.assertEqual(result["bars"][0]["trade_count"], 100)
        with self.assertRaisesRegex(ValueError, "only supports raw"):
            self.provider.fetch_history(
                "BTC-PERP.HYPL", "5d", "1h", "adjusted"
            )

    def test_history_window_sends_exact_millisecond_boundaries(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 1, 2, tzinfo=timezone.utc)

        self.provider.fetch_history_window(
            "BTC-PERP.HYPL", start, end, "1h", "raw"
        )

        request = self.provider.session.calls[-1][0]["req"]
        self.assertEqual(request["startTime"], int(start.timestamp() * 1000))
        self.assertEqual(request["endTime"], int(end.timestamp() * 1000))

    def test_funding_history_is_typed_and_perpetual_only(self):
        start = datetime(2023, 11, 14, tzinfo=timezone.utc)
        result = self.provider.fetch_funding_history(
            "BTC-PERP.HYPL", start, start.replace(day=15)
        )
        self.assertEqual(result["items"][0]["funding_rate"], "0.0000125")
        self.assertEqual(result["items"][0]["premium"], "-0.0001")
        with self.assertRaisesRegex(ValueError, "only available"):
            self.provider.fetch_funding_history(
                "HYPE-USDC.HYPL", start, start.replace(day=15)
            )


if __name__ == "__main__":
    unittest.main()
