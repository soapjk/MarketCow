from __future__ import annotations

import json
import threading
import unittest

from marketcow.hyperliquid_realtime import (
    HyperliquidRealtimeProvider,
    RoutingRealtimeProvider,
)


class FakeApp:
    def __init__(self, _url, on_open, on_message, on_error, on_close):
        self.on_open = on_open
        self.on_message = on_message
        self.on_error = on_error
        self.on_close = on_close
        self.sent = []
        self.closed = threading.Event()

    def run_forever(self):
        self.on_open(self)
        self.closed.wait(1)

    def send(self, message):
        self.sent.append(json.loads(message))

    def close(self):
        self.closed.set()
        self.on_close(self)


class FakeVenue:
    def __init__(self):
        self.sink = None
        self.subscribed = []
        self.unsubscribed = []
        self.closed = False

    def set_sink(self, sink):
        self.sink = sink

    def subscribe(self, mappings, data_types):
        self.subscribed.append((mappings, data_types))

    def unsubscribe(self, filters):
        self.unsubscribed.append(set(filters))

    def close(self):
        self.closed = True


class HyperliquidRealtimeTest(unittest.TestCase):
    def test_bbo_and_trade_are_normalized_to_stream_contract(self):
        apps = []

        def factory(*args, **kwargs):
            app = FakeApp(*args, **kwargs)
            apps.append(app)
            return app

        provider = HyperliquidRealtimeProvider(app_factory=factory)
        events = []
        provider.set_sink(events.append)
        provider.subscribe(
            {"BTC-PERP.HYPL": "BTC"}, {"quote", "trade", "asset_context"}
        )
        subscribed = {
            value["subscription"]["type"] for value in apps[0].sent
            if value["method"] == "subscribe"
        }
        self.assertEqual(subscribed, {"l2Book", "trades", "activeAssetCtx"})

        provider._on_message(None, json.dumps({
            "channel": "l2Book",
            "data": {
                "coin": "BTC", "time": 1_700_000_000_000,
                "levels": [
                    [{"px": "64999.5", "sz": "1.2", "n": 2}],
                    [{"px": "65000.5", "sz": "0.8", "n": 1}],
                ],
            },
        }))
        provider._on_message(None, json.dumps({
            "channel": "trades",
            "data": [{
                "coin": "BTC", "time": 1_700_000_000_001,
                "px": "65000", "sz": "0.1", "side": "B", "tid": 42,
            }],
        }))
        provider._on_message(None, json.dumps({
            "channel": "activeAssetCtx",
            "data": {
                "coin": "BTC", "time": 1_700_000_000_002,
                "ctx": {
                    "markPx": "65001", "oraclePx": "65000",
                    "funding": "0.00001", "openInterest": "100",
                },
            },
        }))
        self.assertEqual(
            [event["event_type"] for event in events],
            ["order_book_snapshot", "quote", "trade", "asset_context"],
        )
        self.assertEqual(events[1]["payload"]["bid_price"], "64999.5")
        self.assertEqual(events[0]["payload"]["bids"][0]["order_count"], 2)
        self.assertEqual(events[2]["payload"]["aggressor_side"], "BUYER")
        self.assertEqual(events[3]["payload"]["oracle_status"], "internal_only")
        provider.close()

    def test_routing_preserves_longport_and_hyperliquid(self):
        longport, hyperliquid = FakeVenue(), FakeVenue()
        router = RoutingRealtimeProvider(longport, hyperliquid)
        router.subscribe({
            "AAPL.XNAS": "AAPL.US",
            "BTC-PERP.HYPL": "BTC",
        }, {"quote"})
        self.assertEqual(
            longport.subscribed[0][0], {"AAPL.XNAS": "AAPL.US"}
        )
        self.assertEqual(
            hyperliquid.subscribed[0][0], {"BTC-PERP.HYPL": "BTC"}
        )
        router.unsubscribe({
            ("AAPL.XNAS", "quote"), ("BTC-PERP.HYPL", "quote"),
        })
        self.assertEqual(
            hyperliquid.unsubscribed[0], {("BTC-PERP.HYPL", "quote")}
        )
        router.close()
        self.assertTrue(longport.closed and hyperliquid.closed)


if __name__ == "__main__":
    unittest.main()
