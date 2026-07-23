from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable, Optional

from .market_data_contracts import (
    STREAM_EVENT_ADAPTER,
    BarPayload,
    QualityContract,
    SubscribeRequest,
    TradePayload,
    utc,
)
from .providers.longport_quote import (
    LongPortError,
    _direct_connection_environment,
    _timestamp,
)


def _decimal(value: Any) -> str:
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError("market value must be finite")
    return format(number, "f")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _event_kind(data_type: str) -> str:
    return "order_book_snapshot" if data_type == "order_book" else data_type


def _data_type(event_kind: str) -> str:
    return "order_book" if event_kind == "order_book_snapshot" else event_kind


@dataclass
class ClientSubscription:
    queue_capacity: int
    queue: asyncio.Queue[dict[str, Any]] = field(init=False)
    filters: set[tuple[str, str]] = field(default_factory=set)
    closed_reason: Optional[str] = None

    def __post_init__(self) -> None:
        self.queue = asyncio.Queue(maxsize=self.queue_capacity)

    def matches(self, event: dict[str, Any]) -> bool:
        instrument = event.get("instrument_id")
        kind = event["event_type"]
        if kind == "stream_status":
            return bool(self.filters) and (
                instrument is None
                or any(item[0] == instrument for item in self.filters)
            )
        return instrument is not None and (instrument, kind) in self.filters


class MinuteTradeBarAggregator:
    """Aggregate provider trades into non-empty, source-pure UTC minute bars."""

    def __init__(
        self,
        emit: Callable[[dict[str, Any]], Any],
        persist: Optional[Callable[[dict[str, Any]], Any]] = None,
    ) -> None:
        self._emit = emit
        self._persist = persist
        self._open: dict[tuple[str, str], dict[str, Any]] = {}

    @staticmethod
    def _window(ts_event: str) -> tuple[datetime, datetime]:
        event = datetime.fromisoformat(ts_event.replace("Z", "+00:00"))
        start = event.astimezone(timezone.utc).replace(second=0, microsecond=0)
        return start, start + timedelta(minutes=1)

    async def on_trade(
        self, instrument_id: str, source: str, ts_event: str, payload: TradePayload
    ) -> None:
        start, end = self._window(ts_event)
        key = (instrument_id, source)
        current = self._open.get(key)
        if current is not None and start < current["window_start_dt"]:
            result = self._emit({
                "event_type": "stream_status", "instrument_id": instrument_id,
                "source": source, "ts_event": ts_event,
                "quality": {
                    "status": "degraded", "delayed": False,
                    "stale": False, "degraded": True,
                },
                "payload": {
                    "state": "degraded", "reason_code": "late_trade_dropped",
                    "last_sequence": None, "resume_supported": True,
                },
            })
            if asyncio.iscoroutine(result):
                await result
            return
        if current is not None and start > current["window_start_dt"]:
            await self._close(current)
            current = None
        price, size = Decimal(payload.price), Decimal(payload.size)
        if current is None:
            current = {
                "instrument_id": instrument_id, "source": source,
                "window_start_dt": start, "window_end_dt": end,
                "open": price, "high": price, "low": price, "close": price,
                "volume": size,
            }
            self._open[key] = current
            return
        current["high"] = max(current["high"], price)
        current["low"] = min(current["low"], price)
        current["close"] = price
        current["volume"] += size

    async def flush(self) -> None:
        for current in list(self._open.values()):
            await self._close(current)
        self._open.clear()

    async def _close(self, current: dict[str, Any]) -> None:
        payload = BarPayload(
            interval="1-MINUTE", adjustment="raw", price_type="LAST",
            aggregation_source="EXTERNAL",
            window_start=_iso(current["window_start_dt"]),
            window_end=_iso(current["window_end_dt"]),
            open=_decimal(current["open"]), high=_decimal(current["high"]),
            low=_decimal(current["low"]), close=_decimal(current["close"]),
            volume=_decimal(current["volume"]),
        ).model_dump(mode="json")
        event = {
            "event_type": "bar", "instrument_id": current["instrument_id"],
            "source": current["source"], "ts_event": payload["window_end"],
            "payload": payload,
        }
        if self._persist is not None:
            result = self._persist(event)
            if asyncio.iscoroutine(result):
                await result
        result = self._emit(event)
        if asyncio.iscoroutine(result):
            await result


class RealtimeHub:
    def __init__(
        self,
        instrument_lookup: Callable[[str], Optional[dict[str, Any]]],
        provider: Any = None,
        *,
        queue_capacity: int = 256,
        replay_capacity: int = 4096,
        clock: Callable[[], datetime] = _now,
        persist_bar: Optional[Callable[[dict[str, Any]], Any]] = None,
    ) -> None:
        self.stream_id = f"market-data-{uuid.uuid4().hex}"
        self.instrument_lookup = instrument_lookup
        self.provider = provider
        self.queue_capacity = queue_capacity
        self.clock = clock
        self._sequence = 0
        self._replay: deque[dict[str, Any]] = deque(maxlen=replay_capacity)
        self._clients: set[int] = set()
        self._client_by_id: dict[int, ClientSubscription] = {}
        self._lock = asyncio.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.bars = MinuteTradeBarAggregator(self.publish, persist_bar)
        if provider is not None:
            provider.set_sink(self.ingest_from_provider)

    def new_client(self) -> ClientSubscription:
        client = ClientSubscription(self.queue_capacity)
        identity = id(client)
        self._clients.add(identity)
        self._client_by_id[identity] = client
        return client

    async def remove_client(self, client: ClientSubscription) -> None:
        self._clients.discard(id(client))
        self._client_by_id.pop(id(client), None)
        if self.provider is not None and client.filters:
            self.provider.unsubscribe(client.filters)

    async def subscribe(
        self, client: ClientSubscription, request: SubscribeRequest
    ) -> dict[str, Any]:
        self._loop = asyncio.get_running_loop()
        mappings = {}
        for instrument_id in request.instruments:
            row = self.instrument_lookup(instrument_id)
            if row is None:
                raise ValueError(f"unknown instrument: {instrument_id}")
            symbol = row.get("provider_symbols", {}).get("longport")
            if not symbol:
                raise ValueError(f"instrument lacks provider:longport mapping: {instrument_id}")
            mappings[instrument_id] = symbol
        filters = {
            (instrument, _event_kind(data_type))
            for instrument in request.instruments
            for data_type in request.data_types
        }
        if request.resume_after is not None:
            if request.resume_stream_id != self.stream_id:
                raise RuntimeError("gap_unrecoverable")
            await self._resume(client, request.resume_after, filters)
        client.filters.update(filters)
        if self.provider is not None:
            self.provider.subscribe(mappings, set(request.data_types))
        return {
            "type": "ack", "action": "subscribe", "request_id": request.request_id,
            "schema_version": 1, "stream_id": self.stream_id,
            "sequence": self._sequence,
            "subscriptions": [
                {"instrument_id": instrument, "data_type": kind}
                for instrument, kind in sorted(
                    (instrument, _data_type(kind))
                    for instrument, kind in client.filters
                )
            ],
            "resume": {
                "requested_after": request.resume_after,
                "replayed_through": self._sequence,
            },
        }

    async def unsubscribe(
        self, client: ClientSubscription, request: Any
    ) -> dict[str, Any]:
        removed = {
            (instrument, _event_kind(kind))
            for instrument in request.instruments
            for kind in request.data_types
        }
        client.filters.difference_update(removed)
        if self.provider is not None:
            provider_removed = set()
            for instrument, kind in removed:
                if kind in {"trade", "bar"} and (
                    (instrument, "trade") in client.filters
                    or (instrument, "bar") in client.filters
                ):
                    continue
                provider_removed.add((instrument, kind))
            self.provider.unsubscribe(provider_removed)
        return {
            "type": "ack", "action": "unsubscribe", "request_id": request.request_id,
            "schema_version": 1, "stream_id": self.stream_id,
            "sequence": self._sequence,
            "subscriptions": [
                {"instrument_id": instrument, "data_type": kind}
                for instrument, kind in sorted(
                    (instrument, _data_type(kind))
                    for instrument, kind in client.filters
                )
            ],
        }

    async def _resume(
        self, client: ClientSubscription, after: int,
        filters: set[tuple[str, str]],
    ) -> None:
        if after > self._sequence:
            raise ValueError("resume_after is ahead of stream")
        oldest = self._replay[0]["sequence"] if self._replay else self._sequence + 1
        if after < oldest - 1:
            raise RuntimeError("gap_unrecoverable")
        for event in self._replay:
            if event["sequence"] > after and (
                event.get("instrument_id"), event["event_type"]
            ) in filters:
                client.queue.put_nowait(event)

    async def publish(self, raw: dict[str, Any]) -> dict[str, Any]:
        now = _iso(self.clock())
        async with self._lock:
            self._sequence += 1
            payload = dict(raw["payload"])
            if raw["event_type"] == "order_book_snapshot":
                payload["baseline_sequence"] = self._sequence
            event = {
                "schema_version": 1, "stream_id": self.stream_id,
                "sequence": self._sequence, "source": raw["source"],
                "ts_event": utc(raw["ts_event"]),
                "ts_ingest": utc(raw.get("ts_ingest", now)),
                "ts_publish": now,
                "quality": raw.get("quality") or QualityContract(
                    status="live", delayed=False, stale=False, degraded=False
                ).model_dump(),
                "event_type": raw["event_type"], "payload": payload,
            }
            if raw.get("instrument_id") is not None:
                event["instrument_id"] = raw["instrument_id"]
            normalized = STREAM_EVENT_ADAPTER.validate_python(event).model_dump(mode="json")
            self._replay.append(normalized)
            for identity in tuple(self._clients):
                client = self._client_by_id.get(identity)
                if client is None or not client.matches(normalized):
                    continue
                if client.queue.full():
                    client.closed_reason = "slow_consumer"
                    continue
                client.queue.put_nowait(normalized)
        if normalized["event_type"] == "trade":
            await self.bars.on_trade(
                normalized["instrument_id"], normalized["source"],
                normalized["ts_event"], TradePayload.model_validate(normalized["payload"])
            )
        return normalized

    def ingest_from_provider(self, event: dict[str, Any]) -> None:
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        asyncio.run_coroutine_threadsafe(self.publish(event), loop)

    def heartbeat(self) -> dict[str, Any]:
        return {
            "type": "heartbeat", "schema_version": 1, "stream_id": self.stream_id,
            "last_sequence": self._sequence, "server_time": _iso(self.clock()),
        }

    async def close(self) -> None:
        try:
            await self.bars.flush()
        finally:
            if self.provider is not None:
                self.provider.close()
            for identity in tuple(self._clients):
                client = self._client_by_id.get(identity)
                if client is not None:
                    client.closed_reason = "server_shutdown"


class LongPortRealtimeProvider:
    """Reference-counted LongPort Depth/Trade callback adapter."""

    def __init__(
        self, app_key: str, app_secret: str, access_token: str, *,
        enable_overnight: bool = False,
        context_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        self.credentials = (app_key, app_secret, access_token)
        self.enable_overnight = enable_overnight
        self.context_factory = context_factory
        self._context: Any = None
        self._sink: Callable[[dict[str, Any]], None] = lambda _event: None
        self._mapping: dict[str, str] = {}
        self._symbol_by_instrument: dict[str, str] = {}
        self._references: dict[tuple[str, str], int] = {}
        self._lock = threading.RLock()

    def set_sink(self, sink: Callable[[dict[str, Any]], None]) -> None:
        self._sink = sink

    def _connect(self) -> Any:
        if not all(self.credentials):
            raise LongPortError("LongPort credentials are not configured")
        if self.context_factory is not None:
            context = self.context_factory()
        else:
            from longbridge.openapi import Config, QuoteContext

            with _direct_connection_environment():
                config = Config.from_apikey(
                    *self.credentials, enable_overnight=self.enable_overnight,
                    enable_print_quote_packages=False,
                )
                context = QuoteContext(config)
        context.set_on_depth(self._on_depth)
        context.set_on_trades(self._on_trades)
        return context

    def subscribe(self, mappings: dict[str, str], data_types: set[str]) -> None:
        from longbridge.openapi import SubType

        with self._lock:
            self._status("connecting", "longport_subscription_starting")
            try:
                context = self._context or self._connect()
                self._context = context
                wants_depth = bool({"quote", "order_book"} & data_types)
                wants_trade = "trade" in data_types or "bar" in data_types
                new_depth, new_trade = [], []
                for instrument, symbol in mappings.items():
                    self._mapping[symbol.upper()] = instrument
                    self._symbol_by_instrument[instrument] = symbol
                    if wants_depth:
                        key = (symbol, "quote")
                        if self._references.get(key, 0) == 0:
                            new_depth.append(symbol)
                        self._references[key] = self._references.get(key, 0) + 1
                    if wants_trade:
                        key = (symbol, "trade")
                        if self._references.get(key, 0) == 0:
                            new_trade.append(symbol)
                        self._references[key] = self._references.get(key, 0) + 1
                if new_depth:
                    context.subscribe(sorted(new_depth), [SubType.Depth])
                if new_trade:
                    context.subscribe(sorted(new_trade), [SubType.Trade])
                self._status("live", "longport_subscription_active")
            except LongPortError:
                self._status("degraded", "longport_subscription_failed")
                raise
            except Exception as exc:
                self._status("degraded", "longport_subscription_failed")
                raise LongPortError("LongPort realtime subscription failed") from exc

    def unsubscribe(self, filters: Iterable[tuple[str, str]]) -> None:
        # SDK unsubscription is reference-counted; closing a client cannot remove
        # another client's provider subscription.
        with self._lock:
            from longbridge.openapi import SubType

            normalized = {
                (
                    instrument,
                    "trade" if kind == "bar"
                    else "quote" if kind == "order_book_snapshot"
                    else kind,
                )
                for instrument, kind in filters
            }
            unsubscribe_depth, unsubscribe_trade = [], []
            for instrument, provider_kind in normalized:
                symbol = self._symbol_by_instrument.get(instrument)
                key = (symbol, provider_kind)
                if symbol is not None and key in self._references:
                    self._references[key] = max(0, self._references[key] - 1)
                    if self._references[key] == 0:
                        (
                            unsubscribe_depth
                            if provider_kind == "quote"
                            else unsubscribe_trade
                        ).append(symbol)
            if self._context is not None and unsubscribe_depth:
                self._context.unsubscribe(sorted(unsubscribe_depth), [SubType.Depth])
            if self._context is not None and unsubscribe_trade:
                self._context.unsubscribe(sorted(unsubscribe_trade), [SubType.Trade])

    def _status(self, state: str, reason: str) -> None:
        now = _iso(_now())
        self._sink({
            "event_type": "stream_status", "source": "longport",
            "ts_event": now,
            "quality": {
                "status": "live" if state == "live" else "degraded",
                "delayed": False, "stale": False, "degraded": state != "live",
            },
            "payload": {
                "state": state, "reason_code": reason,
                "last_sequence": None, "resume_supported": True,
            },
        })

    @staticmethod
    def _callback(args: tuple[Any, ...]) -> tuple[str, Any]:
        if len(args) != 2:
            raise LongPortError("LongPort callback shape is invalid")
        return str(args[0]).upper(), args[1]

    def _on_depth(self, *args: Any) -> None:
        symbol, push = self._callback(args)
        instrument = self._mapping.get(symbol)
        if instrument is None:
            return
        bids, asks = list(push.bids or ()), list(push.asks or ())
        now = _iso(_now())
        book_bids = [] if not bids else [{
            "price": _decimal(bids[0].price),
            "size": _decimal(bids[0].volume), "order_id": "0",
        }]
        book_asks = [] if not asks else [{
            "price": _decimal(asks[0].price),
            "size": _decimal(asks[0].volume), "order_id": "0",
        }]
        self._sink({
            "event_type": "order_book_snapshot", "instrument_id": instrument,
            "source": "longport", "ts_event": now,
            "payload": {
                "book_type": "L1_MBP", "depth": 1, "baseline_sequence": 0,
                "bids": book_bids, "asks": book_asks,
            },
        })
        if not bids or not asks:
            return
        self._sink({
            "event_type": "quote", "instrument_id": instrument,
            "source": "longport", "ts_event": now, "payload": {
                "bid_price": _decimal(bids[0].price),
                "ask_price": _decimal(asks[0].price),
                "bid_size": _decimal(bids[0].volume),
                "ask_size": _decimal(asks[0].volume),
            },
        })

    def _on_trades(self, *args: Any) -> None:
        symbol, push = self._callback(args)
        instrument = self._mapping.get(symbol)
        if instrument is None:
            return
        for index, trade in enumerate(push.trades or ()):
            event_time = _timestamp(trade.timestamp)
            if event_time is None:
                continue
            raw = {
                "symbol": symbol, "timestamp": _iso(event_time),
                "price": _decimal(trade.price), "volume": str(trade.volume),
                "direction": str(trade.direction), "index": index,
            }
            trade_id = "longport:" + hashlib.sha256(json.dumps(
                raw, sort_keys=True, separators=(",", ":")
            ).encode()).hexdigest()
            direction = str(trade.direction).upper()
            aggressor = (
                "BUYER" if "BUY" in direction else
                "SELLER" if "SELL" in direction else "NO_AGGRESSOR"
            )
            self._sink({
                "event_type": "trade", "instrument_id": instrument,
                "source": "longport", "ts_event": raw["timestamp"],
                "payload": {
                    "price": raw["price"], "size": _decimal(trade.volume),
                    "trade_id": trade_id, "aggressor_side": aggressor,
                },
            })

    def close(self) -> None:
        with self._lock:
            context, self._context = self._context, None
            close = getattr(context, "close", None)
            if callable(close):
                close()
