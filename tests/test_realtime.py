from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.market_data_contracts import SubscribeRequest
from marketcow.realtime import (
    LongPortRealtimeProvider,
    MinuteTradeBarAggregator,
    RealtimeHub,
)


INSTRUMENT = {
    "instrument_id": "AAPL.XNAS",
    "symbol": "AAPL",
    "provider_symbols": {"longport": "AAPL.US"},
}


class Provider:
    def __init__(self):
        self.sink = None
        self.subscriptions = []
        self.unsubscriptions = []
        self.closed = False

    def set_sink(self, sink):
        self.sink = sink

    def subscribe(self, mappings, data_types):
        self.subscriptions.append((mappings, data_types))

    def unsubscribe(self, filters):
        self.unsubscriptions.append(set(filters))

    def close(self):
        self.closed = True


def quote(price="100"):
    return {
        "event_type": "quote", "instrument_id": "AAPL.XNAS",
        "source": "longport", "ts_event": "2026-07-23T01:00:00Z",
        "payload": {
            "bid_price": price, "ask_price": "101",
            "bid_size": "10", "ask_size": "11",
        },
    }


def trade(ts="2026-07-23T01:00:10Z", price="100", size="2", trade_id="t1"):
    return {
        "event_type": "trade", "instrument_id": "AAPL.XNAS",
        "source": "longport", "ts_event": ts,
        "payload": {
            "price": price, "size": size, "trade_id": trade_id,
            "aggressor_side": "NO_AGGRESSOR",
        },
    }


class RealtimeHubTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.provider = Provider()
        self.hub = RealtimeHub(
            lambda value: INSTRUMENT if value == "AAPL.XNAS" else None,
            self.provider, queue_capacity=2, replay_capacity=2,
            clock=lambda: datetime(2026, 7, 23, 1, 0, 30, tzinfo=timezone.utc),
        )

    async def asyncTearDown(self):
        await self.hub.close()

    async def test_subscription_sequence_replay_and_unsubscribe(self):
        client = self.hub.new_client()
        request = SubscribeRequest(
            type="subscribe", request_id="s1", instruments=["AAPL.XNAS"],
            data_types=["quote"], bar_types=[], book_depth=1,
        )
        ack = await self.hub.subscribe(client, request)
        self.assertEqual(ack["action"], "subscribe")
        first = await self.hub.publish(quote())
        second = await self.hub.publish(quote("100.5"))
        self.assertEqual((first["sequence"], second["sequence"]), (1, 2))
        self.assertEqual((await client.queue.get())["sequence"], 1)
        self.assertEqual((await client.queue.get())["sequence"], 2)

        resumed = self.hub.new_client()
        await self.hub.subscribe(resumed, request.model_copy(update={
            "resume_after": 1, "resume_stream_id": self.hub.stream_id,
        }))
        self.assertEqual((await resumed.queue.get())["sequence"], 2)
        unack = await self.hub.unsubscribe(
            client, SimpleNamespace(
                request_id="u1", instruments=["AAPL.XNAS"], data_types=["quote"]
            )
        )
        self.assertEqual(unack["subscriptions"], [])

    async def test_unrecoverable_gap_and_backpressure_are_explicit(self):
        client = self.hub.new_client()
        request = SubscribeRequest(
            type="subscribe", request_id="s1", instruments=["AAPL.XNAS"],
            data_types=["quote"], bar_types=[], book_depth=1,
        )
        await self.hub.subscribe(client, request)
        await self.hub.publish(quote())
        await self.hub.publish(quote("100.1"))
        await self.hub.publish(quote("100.2"))
        self.assertEqual(client.closed_reason, "slow_consumer")
        with self.assertRaisesRegex(RuntimeError, "gap_unrecoverable"):
            await self.hub.subscribe(
                self.hub.new_client(),
                request.model_copy(update={
                    "resume_after": 0, "resume_stream_id": self.hub.stream_id,
                }),
            )

    async def test_order_book_snapshot_sequence_is_recovery_baseline(self):
        client = self.hub.new_client()
        request = SubscribeRequest(
            type="subscribe", request_id="book", instruments=["AAPL.XNAS"],
            data_types=["order_book"], bar_types=[], book_depth=1,
        )
        await self.hub.subscribe(client, request)
        event = await self.hub.publish({
            "event_type": "order_book_snapshot",
            "instrument_id": "AAPL.XNAS", "source": "longport",
            "ts_event": "2026-07-23T01:00:00Z",
            "payload": {
                "book_type": "L1_MBP", "depth": 1, "baseline_sequence": 0,
                "bids": [{"price": "100", "size": "1", "order_id": "0"}],
                "asks": [{"price": "101", "size": "2", "order_id": "0"}],
            },
        })
        delivered = await client.queue.get()
        self.assertEqual(delivered["payload"]["baseline_sequence"], event["sequence"])

    async def test_trade_aggregates_source_pure_nonempty_minute_bar(self):
        emitted, persisted = [], []

        async def emit(event):
            emitted.append(event)

        aggregator = MinuteTradeBarAggregator(emit, persisted.append)
        await aggregator.on_trade(
            "AAPL.XNAS", "longport", "2026-07-23T01:00:10Z",
            SimpleNamespace(price="100", size="2"),
        )
        await aggregator.on_trade(
            "AAPL.XNAS", "longport", "2026-07-23T01:00:59.999Z",
            SimpleNamespace(price="101", size="3"),
        )
        await aggregator.on_trade(
            "AAPL.XNAS", "longport", "2026-07-23T01:01:00Z",
            SimpleNamespace(price="99", size="1"),
        )
        self.assertEqual(len(emitted), 1)
        payload = emitted[0]["payload"]
        self.assertEqual(
            (payload["open"], payload["high"], payload["low"], payload["close"]),
            ("100", "101", "100", "101"),
        )
        self.assertEqual(payload["volume"], "5")
        self.assertEqual(len(persisted), 1)
        await aggregator.on_trade(
            "AAPL.XNAS", "longport", "2026-07-23T01:00:30Z",
            SimpleNamespace(price="1000", size="1"),
        )
        await aggregator.flush()
        self.assertEqual(emitted[-1]["payload"]["open"], "99")
        self.assertEqual(emitted[-2]["payload"]["reason_code"], "late_trade_dropped")


class LongPortRealtimeProviderTest(unittest.TestCase):
    def test_depth_and_trade_callbacks_normalize_contract_payloads(self):
        class Context:
            def __init__(self):
                self.unsubscriptions = []

            def set_on_depth(self, callback):
                self.depth_callback = callback

            def set_on_trades(self, callback):
                self.trade_callback = callback

            def subscribe(self, symbols, sub_types):
                self.request = (symbols, sub_types)

            def unsubscribe(self, symbols, sub_types):
                self.unsubscriptions.append((symbols, sub_types))

            def close(self):
                pass

        context = Context()
        events = []
        provider = LongPortRealtimeProvider(
            "key", "secret", "token", context_factory=lambda: context
        )
        provider.set_sink(events.append)
        provider.subscribe(
            {"AAPL.XNAS": "AAPL.US"}, {"quote", "trade", "bar"}
        )
        level = lambda price, volume: SimpleNamespace(price=price, volume=volume)
        context.depth_callback(
            "AAPL.US", SimpleNamespace(
                bids=[level("100", 2)], asks=[level("101", 3)]
            )
        )
        context.trade_callback(
            "AAPL.US", SimpleNamespace(trades=[SimpleNamespace(
                timestamp=datetime(2026, 7, 23, 1, 0, tzinfo=timezone.utc),
                price="100.5", volume=4, direction="Buy",
            )])
        )
        self.assertEqual(
            [event["event_type"] for event in events],
            [
                "stream_status", "stream_status", "order_book_snapshot",
                "quote", "trade",
            ],
        )
        self.assertEqual(events[2]["payload"]["bids"][0]["order_id"], "0")
        self.assertEqual(events[3]["payload"]["bid_size"], "2")
        self.assertEqual(events[4]["payload"]["aggressor_side"], "BUYER")
        self.assertTrue(events[4]["payload"]["trade_id"].startswith("longport:"))
        provider.subscribe({"AAPL.XNAS": "AAPL.US"}, {"quote"})
        provider.unsubscribe({("AAPL.XNAS", "quote")})
        self.assertEqual(context.unsubscriptions, [])
        provider.unsubscribe({("AAPL.XNAS", "quote")})
        self.assertEqual(len(context.unsubscriptions), 1)


class RealtimeWebSocketTest(unittest.TestCase):
    def test_subscribe_ack_error_and_heartbeat(self):
        metadata = SimpleNamespace(
            get_instrument=lambda value: INSTRUMENT if value == "AAPL.XNAS" else None
        )
        service = SimpleNamespace(
            metadata_repository=metadata, market_bar_repository=SimpleNamespace(),
            online_resources=None, close=lambda: None,
        )
        settings = Settings(
            raw_path=__import__("pathlib").Path("/tmp/test/raw"),
            storage_root=__import__("pathlib").Path("/tmp/test"),
            allowed_root=__import__("pathlib").Path("/tmp"),
            postgres_dsn="postgresql://u:p@127.0.0.1/marketcow_test",
            clickhouse_password="x", profile="test", port=8793,
            postgres_schema="marketcow_test", clickhouse_database="marketcow_test",
            clickhouse_spool_path=__import__("pathlib").Path("/tmp/test/spool"),
            realtime_heartbeat_seconds=0.05,
        )
        hub = RealtimeHub(metadata.get_instrument, Provider())
        with TestClient(create_app(settings, service, realtime_hub=hub)) as client:
            with client.websocket_connect("/v1/market-data/stream") as websocket:
                websocket.send_json({
                    "type": "subscribe", "request_id": "s1",
                    "instruments": ["AAPL.XNAS"], "data_types": ["quote"],
                    "bar_types": [], "book_depth": 1,
                })
                self.assertEqual(websocket.receive_json()["type"], "ack")
                websocket.send_json({
                    "type": "subscribe", "request_id": "bad",
                    "instruments": ["UNKNOWN.XNAS"], "data_types": ["quote"],
                    "bar_types": [], "book_depth": 1,
                })
                self.assertEqual(websocket.receive_json()["type"], "error")
                self.assertEqual(websocket.receive_json()["type"], "heartbeat")
                websocket.close()


if __name__ == "__main__":
    unittest.main()
