from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.market_data_contracts import SubscribeRequest
from marketcow.realtime import (
    LongPortError,
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


def quote(price="100", instrument_id="AAPL.XNAS"):
    return {
        "event_type": "quote", "instrument_id": instrument_id,
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
            lambda value: (
                INSTRUMENT if value == "AAPL.XNAS"
                else {
                    **INSTRUMENT, "instrument_id": "MSFT.XNAS",
                    "symbol": "MSFT",
                    "provider_symbols": {"longport": "MSFT.US"},
                } if value == "MSFT.XNAS" else None
            ),
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

    async def test_filtered_events_deliver_contiguous_watermarks_and_resume(self):
        client = self.hub.new_client()
        request = SubscribeRequest(
            type="subscribe", request_id="s1", instruments=["AAPL.XNAS"],
            data_types=["quote"], bar_types=[], book_depth=1,
        )
        await self.hub.subscribe(client, request)
        await self.hub.publish(quote(instrument_id="AAPL.XNAS"))
        await self.hub.publish(quote(instrument_id="MSFT.XNAS"))
        await self.hub.publish(quote("100.5", instrument_id="AAPL.XNAS"))
        delivered = [await client.queue.get() for _ in range(2)]
        self.assertEqual([item["sequence"] for item in delivered], [1, 2])
        self.assertEqual(delivered[1]["type"], "sequence_watermark")
        self.assertEqual(client.closed_reason, "slow_consumer")

        replay_hub = RealtimeHub(
            self.hub.instrument_lookup, Provider(),
            queue_capacity=4, replay_capacity=4,
            clock=self.hub.clock,
        )
        await replay_hub.publish(quote(instrument_id="AAPL.XNAS"))
        await replay_hub.publish(quote(instrument_id="MSFT.XNAS"))
        await replay_hub.publish(quote("100.5", instrument_id="AAPL.XNAS"))
        resumed = replay_hub.new_client()
        await replay_hub.subscribe(resumed, request.model_copy(update={
            "resume_after": 0, "resume_stream_id": replay_hub.stream_id,
        }))
        frames = [await resumed.queue.get() for _ in range(3)]
        self.assertEqual([item["sequence"] for item in frames], [1, 2, 3])
        self.assertEqual(frames[1]["type"], "sequence_watermark")
        await replay_hub.close()

    async def test_oversized_replay_is_structured_and_validation_is_atomic(self):
        hub = RealtimeHub(
            self.hub.instrument_lookup, Provider(),
            queue_capacity=2, replay_capacity=10, clock=self.hub.clock,
        )
        for price in ("100", "100.1", "100.2"):
            await hub.publish(quote(price))
        request = SubscribeRequest(
            type="subscribe", request_id="s1", instruments=["AAPL.XNAS"],
            data_types=["quote"], bar_types=[], book_depth=1,
            resume_after=0, resume_stream_id=hub.stream_id,
        )
        with self.assertRaisesRegex(RuntimeError, "replay_too_large"):
            await hub.subscribe(hub.new_client(), request)
        sequence = hub._sequence
        with self.assertRaises(Exception):
            await hub.publish({
                **quote(), "payload": {
                    "bid_price": "102", "ask_price": "101",
                    "bid_size": "1", "ask_size": "1",
                },
            })
        self.assertEqual(hub._sequence, sequence)
        self.assertEqual(len(hub._replay), 3)
        await hub.close()

    async def test_provider_future_error_is_observed_without_sequence_hole(self):
        client = self.hub.new_client()
        request = SubscribeRequest(
            type="subscribe", request_id="s1", instruments=["AAPL.XNAS"],
            data_types=["quote"], bar_types=[], book_depth=1,
        )
        await self.hub.subscribe(client, request)
        self.hub.ingest_from_provider({
            **quote(), "payload": {
                "bid_price": "102", "ask_price": "101",
                "bid_size": "1", "ask_size": "1",
            },
        })
        frame = await asyncio.wait_for(client.queue.get(), timeout=1)
        self.assertEqual(frame["event_type"], "stream_status")
        self.assertIn("provider_payload_invalid", frame["payload"]["reason_code"])
        self.assertEqual(frame["sequence"], 1)
        self.assertEqual(self.hub._sequence, 1)
        self.assertEqual(len(self.hub._replay), 1)

    async def test_duplicate_and_failed_subscribe_are_transactional(self):
        request = SubscribeRequest(
            type="subscribe", request_id="s1", instruments=["AAPL.XNAS"],
            data_types=["quote"], bar_types=[], book_depth=1,
        )
        client = self.hub.new_client()
        await self.hub.subscribe(client, request)
        await self.hub.subscribe(client, request)
        self.assertEqual(len(self.provider.subscriptions), 1)

        class FailedProvider(Provider):
            def subscribe(self, mappings, data_types):
                raise RuntimeError("provider failed")

        failed = FailedProvider()
        hub = RealtimeHub(self.hub.instrument_lookup, failed)
        failed_client = hub.new_client()
        with self.assertRaisesRegex(RuntimeError, "provider failed"):
            await hub.subscribe(failed_client, request)
        self.assertEqual(failed_client.filters, set())
        await hub.close()

    async def test_provider_capability_deltas_pair_combined_filters(self):
        provider = Provider()
        hub = RealtimeHub(self.hub.instrument_lookup, provider)
        client = hub.new_client()
        quote_book = SubscribeRequest(
            type="subscribe", request_id="qb", instruments=["AAPL.XNAS"],
            data_types=["quote", "order_book"], bar_types=[], book_depth=1,
        )
        await hub.subscribe(client, quote_book)
        self.assertEqual(provider.subscriptions[0][1], {"quote"})
        await hub.unsubscribe(client, SimpleNamespace(
            request_id="uq", instruments=["AAPL.XNAS"], data_types=["quote"]
        ))
        self.assertEqual(provider.unsubscriptions, [])
        self.assertEqual(
            client.filters, {("AAPL.XNAS", "order_book_snapshot")}
        )
        await hub.remove_client(client)
        self.assertEqual(
            provider.unsubscriptions[-1], {("AAPL.XNAS", "quote")}
        )
        await hub.close()

        provider = Provider()
        hub = RealtimeHub(self.hub.instrument_lookup, provider)
        client = hub.new_client()
        await hub.subscribe(client, SubscribeRequest(
            type="subscribe", request_id="t", instruments=["AAPL.XNAS"],
            data_types=["trade"], bar_types=[], book_depth=1,
        ))
        await hub.subscribe(client, SubscribeRequest(
            type="subscribe", request_id="b", instruments=["AAPL.XNAS"],
            data_types=["bar"], bar_types=["1-MINUTE"], book_depth=1,
        ))
        self.assertEqual(len(provider.subscriptions), 1)
        await hub.remove_client(client)
        self.assertEqual(
            provider.unsubscriptions, [{("AAPL.XNAS", "trade")}]
        )
        await hub.close()

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

    async def test_timer_closes_bar_and_persist_failure_is_fail_closed(self):
        gate = asyncio.Event()
        delays = []
        order = []

        async def sleep(delay):
            delays.append(delay)
            await gate.wait()

        async def persist(event):
            order.append(("persist", event["event_type"]))

        async def emit(event):
            order.append(("emit", event["event_type"]))

        aggregator = MinuteTradeBarAggregator(
            emit, persist, clock=lambda: datetime(
                2026, 7, 23, 1, 0, 30, tzinfo=timezone.utc
            ), sleep=sleep,
        )
        await aggregator.on_trade(
            "AAPL.XNAS", "longport", "2026-07-23T01:00:10Z",
            SimpleNamespace(price="100", size="1", session="regular"),
        )
        gate.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(delays, [30.0])
        self.assertEqual(order, [("persist", "bar"), ("emit", "bar")])
        await aggregator.on_trade(
            "AAPL.XNAS", "longport", "2026-07-23T01:00:20Z",
            SimpleNamespace(price="999", size="1", session="regular"),
        )
        self.assertEqual(order[-1], ("emit", "stream_status"))

        failed_events = []

        def failed(_event):
            raise RuntimeError("storage down")

        failed_aggregator = MinuteTradeBarAggregator(
            failed_events.append, failed,
            clock=lambda: datetime(2026, 7, 23, 1, 0, 30, tzinfo=timezone.utc),
            sleep=lambda _delay: asyncio.sleep(0),
        )
        await failed_aggregator.on_trade(
            "AAPL.XNAS", "longport", "2026-07-23T01:00:10Z",
            SimpleNamespace(price="100", size="1", session="regular"),
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(
            [event["payload"]["reason_code"] for event in failed_events],
            ["bar_persist_failed"],
        )


class LongPortRealtimeProviderTest(unittest.TestCase):
    def test_depth_and_trade_callbacks_normalize_contract_payloads(self):
        class Context:
            def __init__(self):
                self.unsubscriptions = []
                self.subscriptions = []

            def set_on_depth(self, callback):
                self.depth_callback = callback

            def set_on_trades(self, callback):
                self.trade_callback = callback

            def subscribe(self, symbols, sub_types):
                self.request = (symbols, sub_types)
                self.subscriptions.append((symbols, sub_types))

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
            ["order_book_snapshot", "quote", "trade"],
        )
        self.assertEqual(events[0]["payload"]["bids"][0]["order_id"], "0")
        self.assertEqual(events[1]["payload"]["bid_size"], "2")
        self.assertEqual(events[1]["payload"]["ts_event_source"], "marketcow_observation")
        self.assertTrue(events[1]["quality"]["degraded"])
        self.assertEqual(events[2]["payload"]["aggressor_side"], "BUYER")
        self.assertTrue(events[2]["payload"]["trade_id"].startswith("longport:"))
        provider.subscribe({"AAPL.XNAS": "AAPL.US"}, {"quote"})
        self.assertEqual(len(context.subscriptions), 2)
        provider.unsubscribe({("AAPL.XNAS", "quote")})
        self.assertEqual(context.unsubscriptions, [])
        provider.unsubscribe({("AAPL.XNAS", "quote")})
        self.assertEqual(len(context.unsubscriptions), 1)
        context.trade_callback(
            "AAPL.US", SimpleNamespace(trades=[SimpleNamespace(
                timestamp=datetime(2026, 7, 23, 1, 1, tzinfo=timezone.utc),
                price="100.5", volume=4, direction="Buy",
                trade_session="Overnight",
            )])
        )
        self.assertEqual(
            events[-1]["payload"]["reason_code"], "overnight_trade_rejected"
        )
        provider.enable_overnight = True
        context.trade_callback(
            "AAPL.US", SimpleNamespace(trades=[SimpleNamespace(
                timestamp=datetime(2026, 7, 23, 1, 2, tzinfo=timezone.utc),
                price="100.5", volume=4, direction="Buy",
                trade_session="Overnight",
            )])
        )
        self.assertEqual(events[-1]["payload"]["session"], "overnight")

    def test_partial_provider_subscribe_failure_rolls_back_refcount(self):
        class Context:
            def __init__(self):
                self.unsubscriptions = []

            def set_on_depth(self, callback):
                self.depth_callback = callback

            def set_on_trades(self, callback):
                self.trade_callback = callback

            def subscribe(self, symbols, sub_types):
                if "Trade" in str(sub_types[0]):
                    raise RuntimeError("trade permission denied")

            def unsubscribe(self, symbols, sub_types):
                self.unsubscriptions.append((symbols, sub_types))

        context = Context()
        provider = LongPortRealtimeProvider(
            "key", "secret", "token", context_factory=lambda: context
        )
        provider.set_sink(lambda _event: None)
        with self.assertRaises(LongPortError):
            provider.subscribe(
                {"AAPL.XNAS": "AAPL.US"}, {"quote", "trade"}
            )
        self.assertEqual(provider._references, {})
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
                websocket.send_json([])
                malformed = websocket.receive_json()
                self.assertEqual(malformed["type"], "error")
                self.assertIsNone(malformed["request_id"])
                websocket.send_text("{")
                self.assertEqual(websocket.receive_json()["code"], "invalid_json")
                self.assertEqual(websocket.receive_json()["type"], "heartbeat")
                websocket.close()
            with client.websocket_connect("/v1/market-data/stream") as websocket:
                websocket.send_json({
                    "type": "subscribe", "request_id": "ahead",
                    "instruments": ["AAPL.XNAS"], "data_types": ["quote"],
                    "bar_types": [], "book_depth": 1,
                    "resume_after": 999,
                    "resume_stream_id": hub.stream_id,
                })
                response = websocket.receive_json()
                self.assertEqual(response["type"], "error")
                self.assertEqual(response["code"], "gap_unrecoverable")
                websocket.close()

    def test_slow_consumer_closes_with_1013_reason(self):
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
            realtime_heartbeat_seconds=1,
        )
        hub = RealtimeHub(metadata.get_instrument, Provider(), queue_capacity=1)
        with TestClient(create_app(settings, service, realtime_hub=hub)) as client:
            with client.websocket_connect("/v1/market-data/stream") as websocket:
                websocket.send_json({
                    "type": "subscribe", "request_id": "s1",
                    "instruments": ["AAPL.XNAS"], "data_types": ["quote"],
                    "bar_types": [], "book_depth": 1,
                })
                self.assertEqual(websocket.receive_json()["type"], "ack")
                for price in ("100", "100.1", "100.2", "100.3"):
                    hub.ingest_from_provider(quote(price))
                first = websocket.receive_json()
                self.assertIn(first["type"] if "type" in first else first["event_type"], {
                    "quote", "sequence_watermark",
                })
                with self.assertRaises(WebSocketDisconnect) as raised:
                    while True:
                        websocket.receive_json()
                self.assertEqual(raised.exception.code, 1013)
                self.assertEqual(raised.exception.reason, "slow_consumer")


if __name__ == "__main__":
    unittest.main()
