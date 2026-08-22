from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Iterable

import websockets

from .polymarket_live import (
    GapEntry,
    LiveBook,
    LiveBootstrapResponse,
    LiveEventEnvelope,
    LiveEventPage,
    LiveReadHealth,
    LiveSnapshotPage,
    LiveStateStore,
    PolymarketLiveReadError,
    PolymarketLiveReadStore,
    content_sha256,
    live_event_identity,
)


LOGGER = logging.getLogger(__name__)
STREAM_SCHEMA = "marketcow.polymarket.live-stream.v1"


def _validate_event(event: LiveEventEnvelope) -> None:
    if content_sha256(event.canonical_payload) != event.canonical_payload_sha256:
        raise ValueError("live stream canonical payload hash mismatch")
    if content_sha256(event.raw_payload) != event.raw_payload_sha256:
        raise ValueError("live stream raw payload hash mismatch")
    if live_event_identity(event) != event.event_id:
        raise ValueError("live stream event identity mismatch")


class PolymarketLiveProjection:
    """Process-local hot projection populated exclusively by the live stream."""

    def __init__(self, *, replay_capacity: int = 10_000) -> None:
        if replay_capacity < 1:
            raise ValueError("live projection replay capacity must be positive")
        self.replay_capacity = replay_capacity
        self._lock = threading.RLock()
        self._events: deque[LiveEventEnvelope] = deque(maxlen=replay_capacity)
        self._books: dict[str, LiveBook] = {}
        self._gaps: list[GapEntry] = []
        self._markets: dict[str, Any] = {}
        self._catalog_source: dict[str, Any] | None = None
        self._catalog_revision: str | None = None
        self._active_recovery_id: str | None = None
        self._latest_cursor = 0
        self._persisted_cursor = 0
        self._persistence_queue_depth = 0
        self._persistence_error: str | None = None
        self._connected = False
        self._ready = False
        self._error_code: str | None = "polymarket_live_stream_disconnected"
        self._last_message_at: datetime | None = None
        self._subscribers: set[asyncio.Queue[LiveEventEnvelope]] = set()

    @property
    def ready(self) -> bool:
        with self._lock:
            return self._ready and self._connected and self._error_code is None

    @property
    def latest_cursor(self) -> int:
        with self._lock:
            return self._latest_cursor

    def mark_connecting(self) -> None:
        with self._lock:
            self._connected = False
            self._ready = False
            self._error_code = "polymarket_live_stream_connecting"

    def mark_disconnected(self, code: str = "polymarket_live_stream_disconnected") -> None:
        with self._lock:
            self._connected = False
            self._ready = False
            self._error_code = code

    def install_state(self, payload: dict[str, Any]) -> None:
        if payload.get("schema_version") != STREAM_SCHEMA:
            raise ValueError("unsupported Polymarket live stream schema")
        if payload.get("type") != "state":
            raise ValueError("Polymarket live stream did not start with state")
        markets = payload.get("markets")
        books = payload.get("books")
        gaps = payload.get("gaps")
        if not isinstance(markets, list) or not isinstance(books, list) or not isinstance(gaps, list):
            raise ValueError("Polymarket live stream state is incomplete")
        from .polymarket_live import LiveMarket

        validated_markets = [LiveMarket.model_validate(item) for item in markets]
        validated_books = [LiveBook.model_validate(item) for item in books]
        validated_gaps = [GapEntry.model_validate(item) for item in gaps]
        latest_cursor = int(payload.get("latest_cursor", -1))
        persisted_cursor = int(payload.get("persisted_cursor", -1))
        if latest_cursor < 0 or persisted_cursor < 0 or persisted_cursor > latest_cursor:
            raise ValueError("Polymarket live stream cursor watermarks are invalid")
        with self._lock:
            self._events.clear()
            self._markets = {
                item.identity.market_id: item for item in validated_markets
            }
            self._books = {item.token_id: item for item in validated_books}
            self._gaps = validated_gaps
            self._catalog_source = payload.get("catalog_source")
            self._catalog_revision = payload.get("catalog_revision")
            self._active_recovery_id = payload.get("active_recovery_id") or None
            self._latest_cursor = latest_cursor
            self._persisted_cursor = persisted_cursor
            self._persistence_queue_depth = int(
                payload.get("persistence_queue_depth", 0)
            )
            self._persistence_error = payload.get("persistence_error") or None
            self._connected = True
            self._ready = False
            self._error_code = None
            self._last_message_at = datetime.now(timezone.utc)

    def add_history(self, raw_events: Iterable[dict[str, Any]]) -> None:
        events = [LiveEventEnvelope.model_validate(item) for item in raw_events]
        for event in events:
            _validate_event(event)
        with self._lock:
            for event in events:
                if self._events and event.cursor != self._events[-1].cursor + 1:
                    raise ValueError("Polymarket live replay history contains a cursor gap")
                if event.cursor > self._latest_cursor:
                    raise ValueError("Polymarket live replay exceeds its state watermark")
                self._events.append(event)
            self._last_message_at = datetime.now(timezone.utc)

    def mark_ready(self, payload: dict[str, Any]) -> None:
        cursor = int(payload.get("latest_cursor", -1))
        with self._lock:
            if cursor != self._latest_cursor:
                raise ValueError("Polymarket live stream ready watermark changed")
            if self._events and self._events[-1].cursor != cursor:
                raise ValueError("Polymarket live replay does not reach ready watermark")
            self._connected = True
            self._ready = True
            self._error_code = None
            self._last_message_at = datetime.now(timezone.utc)

    def apply_live(self, raw: dict[str, Any]) -> LiveEventEnvelope:
        event = LiveEventEnvelope.model_validate(raw)
        _validate_event(event)
        with self._lock:
            if not self._ready:
                raise ValueError("Polymarket live event arrived before replay was ready")
            if event.cursor != self._latest_cursor + 1:
                self._ready = False
                self._error_code = "polymarket_live_stream_cursor_gap"
                raise ValueError(
                    "Polymarket live stream cursor gap: "
                    f"expected {self._latest_cursor + 1}, observed {event.cursor}"
                )
            self._latest_cursor = event.cursor
            self._events.append(event)
            self._apply_event_state(event)
            self._last_message_at = datetime.now(timezone.utc)
            for queue in tuple(self._subscribers):
                if queue.full():
                    self._subscribers.discard(queue)
                    continue
                queue.put_nowait(event)
        return event

    def update_persistence(
        self, cursor: int, *, queue_depth: int = 0, error: str | None = None,
    ) -> None:
        with self._lock:
            # The persistence worker and each WebSocket sender are independent
            # consumers of the collector's published queue. A sender may report
            # a durable watermark a few events ahead of the last event this API
            # process has decoded. Only regression is invalid; the local event
            # projection will deterministically catch up to the global durable
            # watermark.
            if cursor < self._persisted_cursor:
                raise ValueError("Polymarket persisted cursor watermark is invalid")
            self._persisted_cursor = cursor
            self._persistence_queue_depth = max(0, queue_depth)
            self._persistence_error = error or None

    def watermarks(self) -> dict[str, int | bool | str | None]:
        with self._lock:
            return {
                "published_cursor": self._latest_cursor,
                "persisted_cursor": self._persisted_cursor,
                "persistence_lag_events": (
                    max(0, self._latest_cursor - self._persisted_cursor)
                ),
                "persistence_queue_depth": self._persistence_queue_depth,
                "live_stream_connected": self._connected,
                "persistence_error": self._persistence_error,
            }

    def confirm_book(self, raw: dict[str, Any]) -> None:
        book = LiveBook.model_validate(raw)
        with self._lock:
            previous = self._books.get(book.token_id)
            if previous is None or previous.state_checksum != book.state_checksum:
                self._ready = False
                self._error_code = "polymarket_live_stream_confirmation_mismatch"
                raise ValueError("Polymarket live book confirmation mismatches state")
            self._books[book.token_id] = book
            self._last_message_at = datetime.now(timezone.utc)

    def _apply_event_state(self, event: LiveEventEnvelope) -> None:
        for gap in event.gaps:
            identity = content_sha256(gap.model_dump(mode="json"))
            if all(
                content_sha256(item.model_dump(mode="json")) != identity
                for item in self._gaps
            ):
                self._gaps.append(gap.model_copy(deep=True))
        if (
            event.applied
            and event.token_id
            and event.event_type in {
                "book", "price_change", "best_bid_ask", "last_trade_price",
                "tick_size_change",
            }
        ):
            self._books[event.token_id] = LiveBook.model_validate(
                event.canonical_payload
            )
        if event.event_type == "recovery_started" and event.applied:
            self._active_recovery_id = str(
                event.canonical_payload.get("recovery_id") or ""
            ) or None
        if event.event_type == "recovery_completed" and event.applied:
            recovered = set(
                event.canonical_payload.get("resolved_gap_token_ids")
                or event.canonical_payload.get("recovered_token_ids") or []
            )
            for gap in self._gaps:
                if not gap.resolved and gap.token_id in recovered:
                    gap.resolved = True
                    gap.resolution = "live_stream_recovery_completed"
            self._active_recovery_id = None

    def _require_ready(self) -> None:
        if not self.ready:
            with self._lock:
                code = self._error_code or "polymarket_live_stream_not_ready"
            raise PolymarketLiveReadError(
                code,
                "Polymarket real-time in-memory projection is not ready",
                503,
            )

    def events_after(
        self,
        market_ids: Iterable[str],
        after_cursor: int,
        limit: int,
    ) -> LiveEventPage:
        self._require_ready()
        selected = set(PolymarketLiveReadStore._scope(market_ids))
        page_limit = min(limit, PolymarketLiveReadStore.max_event_page_items)
        with self._lock:
            if after_cursor > self._latest_cursor:
                raise PolymarketLiveReadError(
                    "polymarket_live_cursor_ahead",
                    "Requested cursor is ahead of the real-time projection",
                    409,
                )
            oldest = self._events[0].cursor if self._events else self._latest_cursor + 1
            if after_cursor < oldest - 1:
                raise PolymarketLiveReadError(
                    "resume_cursor_expired",
                    "Resume cursor predates the in-memory replay window",
                    409,
                )
            if after_cursor > 0 and any(
                event.cursor > after_cursor
                and event.event_type == "catalog_revision"
                for event in self._events
            ):
                raise PolymarketLiveReadError(
                    "resume_cursor_expired",
                    "Resume cursor predates the current catalog revision",
                    409,
                )
            candidates = [
                event for event in self._events
                if event.cursor > after_cursor
                and (event.market_id is None or event.market_id in selected)
            ]
            bounded: list[LiveEventEnvelope] = []
            size = 0
            has_more = False
            for event in candidates:
                event_size = len(event.model_dump_json())
                if len(bounded) >= page_limit or (
                    bounded
                    and size + event_size > PolymarketLiveReadStore.max_event_page_bytes
                ):
                    has_more = True
                    break
                bounded.append(event)
                size += event_size
            next_cursor = bounded[-1].cursor if bounded else after_cursor
        return LiveEventPage(
            after_cursor=after_cursor,
            next_cursor=next_cursor,
            has_more=has_more,
            items=bounded,
        )

    def events_json(
        self,
        market_ids: Iterable[str],
        after_cursor: int,
        limit: int,
        *,
        _phase_ms: dict[str, float] | None = None,
    ) -> tuple[bytes, dict[str, float], LiveEventPage]:
        phases = _phase_ms if _phase_ms is not None else {}
        lookup_started = time.perf_counter()
        page = self.events_after(market_ids, after_cursor, limit)
        phases.update({
            "scope_bootstrap_ms": 0.0,
            "stable_boundary_wait_ms": 0.0,
            "sqlite_query_ms": 0.0,
            "memory_projection_ms": (time.perf_counter() - lookup_started) * 1000,
            "model_construction_ms": 0.0,
        })
        serialization_started = time.perf_counter()
        payload = page.model_dump_json().encode("utf-8")
        phases["json_serialization_ms"] = (
            time.perf_counter() - serialization_started
        ) * 1000
        return payload, phases, page

    def _selected_markets(self, market_ids: list[str]) -> list[Any]:
        with self._lock:
            missing = [item for item in market_ids if item not in self._markets]
            if missing:
                raise PolymarketLiveReadError(
                    "polymarket_market_not_found",
                    "One or more markets are outside the live projection",
                    404,
                )
            markets = [self._markets[item].model_copy(deep=True) for item in market_ids]
            books = dict(self._books)
        for market in markets:
            expected = {outcome.token_id for outcome in market.identity.outcomes}
            live_books = [books[token] for token in expected if token in books]
            if len(live_books) != len(expected):
                continue
            ticks = {book.tick_size for book in live_books}
            if len(ticks) != 1:
                raise PolymarketLiveReadError(
                    "polymarket_instrument_book_binding_incomplete",
                    "Outcome books disagree on the live market tick",
                    503,
                )
            tick = next(iter(ticks))
            instrument = market.rules.instrument
            if instrument.price_increment == tick:
                continue
            base_revision = instrument.revision
            instrument.price_increment = tick
            instrument.revision = content_sha256({
                "base_revision": base_revision,
                "live_price_increment": tick,
                "binding": "polymarket_clob_book_tick_v1",
            })
            market.metadata_revision = content_sha256({
                "base_revision": market.metadata_revision,
                "instrument_revision": instrument.revision,
            })
        return markets

    def _transient(
        self, reader: PolymarketLiveReadStore, market_ids: list[str]
    ) -> LiveStateStore:
        selected = self._selected_markets(market_ids)
        related_ids = sorted({
            pair.market_id
            for market in selected
            for relation in market.relations
            for pair in relation.outcome_pairs
            if pair.market_id not in market_ids
        })
        markets = selected + self._selected_markets(related_ids)
        with self._lock:
            transient = LiveStateStore(reader.root / ".live-stream-projection")
            transient._recovered = True
            transient.now_provider = reader.now_provider
            transient.catalog = {
                market.identity.market_id: market for market in markets
            }
            transient.token_to_market = {
                outcome.token_id: market.identity.market_id
                for market in markets for outcome in market.identity.outcomes
            }
            transient.catalog_revision = self._catalog_revision
            transient.catalog_source = self._catalog_source
            transient.cursor = self._latest_cursor
            transient.active_recovery_id = self._active_recovery_id
            transient.books = dict(self._books)
            transient.gaps = [gap.model_copy(deep=True) for gap in self._gaps]
        return transient

    def bootstrap(
        self, reader: PolymarketLiveReadStore, market_ids: Iterable[str]
    ) -> LiveBootstrapResponse:
        self._require_ready()
        selected = PolymarketLiveReadStore._scope(market_ids)
        with self._lock:
            cursor = self._latest_cursor
            revision = self._catalog_revision
            source = self._catalog_source
        markets = self._selected_markets(selected)
        return LiveBootstrapResponse(
            catalog_revision=revision,
            catalog_source=source,
            cursor=cursor,
            markets=markets,
            active_token_ids=[
                outcome.token_id
                for market in markets if market.active and not market.closed
                for outcome in market.identity.outcomes
            ],
            sequence_semantics="deterministic_normalized",
            recovery={
                "bootstrap": "CLOB POST /books full snapshots",
                "disconnect": "new book_epoch followed by full /books recovery",
                "resume": "in-memory live stream replay required before event resume",
            },
            source_policy="official_free_only",
        )

    def snapshot(
        self, reader: PolymarketLiveReadStore, market_ids: Iterable[str]
    ) -> LiveSnapshotPage:
        self._require_ready()
        selected = PolymarketLiveReadStore._scope(market_ids)
        transient = self._transient(reader, selected)
        page = LiveSnapshotPage(
            catalog_revision=transient.catalog_revision,
            cursor=transient.cursor,
            count=len(selected),
            items=[transient.frame(market_id) for market_id in selected],
        )
        maximum_age = reader._stable_snapshot_max_book_age_seconds
        if maximum_age is not None:
            now = reader.now_provider()
            books = {
                book.token_id: book
                for frame in page.items
                for book in (*frame.tokens, *frame.relation_tokens)
            }
            if not books or any(
                not 0 <= (now - book.received_at).total_seconds() <= maximum_age
                for book in books.values()
            ):
                raise PolymarketLiveReadStore._stable_boundary_unavailable(
                    "snapshot response"
                )
        return page

    def snapshot_json(
        self, reader: PolymarketLiveReadStore, market_ids: Iterable[str]
    ) -> bytes:
        page = self.snapshot(reader, market_ids)
        payload = page.model_dump_json().encode("utf-8")
        maximum_age = reader._stable_snapshot_max_book_age_seconds
        if maximum_age is None:
            return payload
        observed_at = reader.now_provider()
        books = {
            book.token_id: book
            for frame in page.items
            for book in (*frame.tokens, *frame.relation_tokens)
        }
        if not books or any(
            not 0 <= (observed_at - book.received_at).total_seconds() <= maximum_age
            for book in books.values()
        ):
            raise PolymarketLiveReadStore._stable_boundary_unavailable(
                "snapshot response"
            )
        return payload

    def health(self) -> LiveReadHealth:
        with self._lock:
            markets = list(self._markets.values())
            active = [market for market in markets if market.active and not market.closed]
            token_count = sum(len(market.identity.outcomes) for market in active)
            complete = sum(
                all(
                    outcome.token_id in self._books
                    and self._books[outcome.token_id].bids
                    and self._books[outcome.token_id].asks
                    for outcome in market.identity.outcomes
                )
                for market in active
            )
            unresolved = sum(not gap.resolved for gap in self._gaps)
            ready = (
                self._ready and self._connected and self._error_code is None
                and self._persistence_error is None
                and self._active_recovery_id is None
                and unresolved == 0 and complete == len(active)
            )
            reasons = []
            if self._error_code:
                reasons.append(self._error_code)
            if self._persistence_error:
                reasons.append("polymarket_async_persistence_failed")
            if self._active_recovery_id:
                reasons.append("recovery_in_progress")
            if unresolved:
                reasons.append("unresolved_gap")
            if complete != len(active):
                reasons.append("incomplete_book_state")
            return LiveReadHealth(
                status="index_ready" if ready else "degraded",
                catalog_revision=self._catalog_revision,
                catalog_index_ready=bool(self._catalog_revision),
                latest_state_ready=ready,
                market_count=len(active),
                token_count=token_count,
                book_token_count=len(self._books),
                book_complete_market_count=complete,
                unresolved_gap_count=unresolved,
                latest_cursor=self._latest_cursor,
                persisted_cursor=self._persisted_cursor,
                persistence_lag_events=(
                    max(0, self._latest_cursor - self._persisted_cursor)
                ),
                persistence_queue_depth=self._persistence_queue_depth,
                live_stream_connected=self._connected,
                reason_codes=sorted(set(reasons)),
            )

    def subscribe(self, *, capacity: int = 1024) -> asyncio.Queue[LiveEventEnvelope]:
        queue: asyncio.Queue[LiveEventEnvelope] = asyncio.Queue(maxsize=capacity)
        with self._lock:
            self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[LiveEventEnvelope]) -> None:
        with self._lock:
            self._subscribers.discard(queue)


class PolymarketLiveStreamServer:
    """Loopback WebSocket publisher over the collector's in-memory state."""

    def __init__(
        self,
        store: LiveStateStore,
        *,
        host: str = "127.0.0.1",
        port: int = 8794,
        replay_capacity: int = 10_000,
        client_queue_capacity: int = 4096,
    ) -> None:
        self.store = store
        self.host = host
        self.port = port
        self.replay_capacity = replay_capacity
        self.client_queue_capacity = client_queue_capacity
        self._loop: asyncio.AbstractEventLoop | None = None
        self._server: Any = None
        self._clients: set[asyncio.Queue[dict[str, Any]]] = set()

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self.store.live_event_sink = self.publish
        self.store.live_book_sink = self.publish_book_confirmation
        self._server = await websockets.serve(
            self._handler,
            self.host,
            self.port,
            max_size=64 * 1024 * 1024,
            ping_interval=20,
            ping_timeout=60,
        )
        LOGGER.info(
            "polymarket_live_stream_started host=%s port=%d replay_capacity=%d",
            self.host, self.port, self.replay_capacity,
        )

    async def close(self) -> None:
        self.store.live_event_sink = None
        self.store.live_book_sink = None
        if self._server is not None:
            self._broadcast({"type": "close"})
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    def publish(self, event: LiveEventEnvelope) -> None:
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        loop.call_soon_threadsafe(
            self._broadcast,
            {"type": "event", "event": event.model_copy(deep=True)},
        )

    def publish_book_confirmation(self, book: LiveBook) -> None:
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        loop.call_soon_threadsafe(
            self._broadcast,
            {"type": "book_confirmation", "book": book.model_copy(deep=True)},
        )

    def _broadcast(self, message: dict[str, Any]) -> None:
        for queue in tuple(self._clients):
            if queue.full():
                self._clients.discard(queue)
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                queue.put_nowait({"type": "close"})
                continue
            queue.put_nowait(message)

    def _state(self) -> tuple[dict[str, Any], list[LiveEventEnvelope]]:
        with self.store._sync_lock:
            history = list(self.store.events)[-self.replay_capacity:]
            state = {
                "schema_version": STREAM_SCHEMA,
                "type": "state",
                "catalog_revision": self.store.catalog_revision,
                "catalog_source": self.store.catalog_source,
                "latest_cursor": self.store.cursor,
                "persisted_cursor": getattr(
                    self.store, "persisted_cursor", self.store.cursor
                ),
                "persistence_queue_depth": self.store._persistence_queue.qsize(),
                "persistence_error": self.store.persistence_error,
                "active_recovery_id": self.store.active_recovery_id,
                "markets": [
                    market.model_dump(mode="json")
                    for market in sorted(
                        self.store.catalog.values(),
                        key=lambda item: item.identity.market_id,
                    )
                ],
                "books": [
                    book.model_dump(mode="json")
                    for _, book in sorted(self.store.books.items())
                ],
                "gaps": [gap.model_dump(mode="json") for gap in self.store.gaps],
                "history_oldest_cursor": (
                    history[0].cursor if history else self.store.cursor + 1
                ),
            }
        return state, history

    async def _handler(self, websocket: Any) -> None:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=self.client_queue_capacity
        )
        self._clients.add(queue)
        try:
            raw = await asyncio.wait_for(websocket.recv(), timeout=5)
            request = json.loads(raw)
            if request.get("type") != "subscribe":
                await websocket.close(code=1008, reason="subscribe required")
                return
            state, history = await asyncio.to_thread(self._state)
            await websocket.send(json.dumps(state, separators=(",", ":")))
            for offset in range(0, len(history), 100):
                chunk = {
                    "schema_version": STREAM_SCHEMA,
                    "type": "history",
                    "items": [
                        event.model_dump(mode="json")
                        for event in history[offset:offset + 100]
                    ],
                }
                await websocket.send(json.dumps(chunk, separators=(",", ":")))
            await websocket.send(json.dumps({
                "schema_version": STREAM_SCHEMA,
                "type": "ready",
                "latest_cursor": state["latest_cursor"],
            }, separators=(",", ":")))
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=1.0)
                except TimeoutError:
                    await websocket.send(json.dumps({
                        "schema_version": STREAM_SCHEMA,
                        "type": "persistence",
                        "persisted_cursor": self.store.persisted_cursor,
                        "persistence_queue_depth": (
                            self.store._persistence_queue.qsize()
                        ),
                        "persistence_error": self.store.persistence_error,
                    }, separators=(",", ":")))
                    continue
                if message["type"] == "close":
                    return
                if message["type"] == "event":
                    event = message["event"]
                    if event.cursor <= state["latest_cursor"]:
                        continue
                    payload = {
                        "schema_version": STREAM_SCHEMA,
                        "type": "event",
                        "event": event.model_dump(mode="json"),
                        "persisted_cursor": self.store.persisted_cursor,
                        "persistence_queue_depth": (
                            self.store._persistence_queue.qsize()
                        ),
                        "persistence_error": self.store.persistence_error,
                    }
                else:
                    payload = {
                        "schema_version": STREAM_SCHEMA,
                        "type": "book_confirmation",
                        "book": message["book"].model_dump(mode="json"),
                        "persisted_cursor": self.store.persisted_cursor,
                        "persistence_queue_depth": (
                            self.store._persistence_queue.qsize()
                        ),
                        "persistence_error": self.store.persistence_error,
                    }
                await websocket.send(json.dumps(payload, separators=(",", ":")))
        except (asyncio.CancelledError, websockets.ConnectionClosed):
            pass
        finally:
            self._clients.discard(queue)


class PolymarketLiveStreamClient:
    def __init__(
        self,
        uri: str,
        projection: PolymarketLiveProjection,
        *,
        reconnect_seconds: float = 0.25,
    ) -> None:
        self.uri = uri
        self.projection = projection
        self.reconnect_seconds = reconnect_seconds

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self.projection.mark_connecting()
            try:
                async with websockets.connect(
                    self.uri,
                    max_size=64 * 1024 * 1024,
                    ping_interval=20,
                    ping_timeout=60,
                ) as websocket:
                    await websocket.send(json.dumps({"type": "subscribe"}))
                    async for raw in websocket:
                        message = json.loads(raw)
                        kind = message.get("type")
                        if kind == "state":
                            self.projection.install_state(message)
                        elif kind == "history":
                            self.projection.add_history(message.get("items") or [])
                        elif kind == "ready":
                            self.projection.mark_ready(message)
                        elif kind == "event":
                            self.projection.apply_live(message["event"])
                            self.projection.update_persistence(
                                int(message.get("persisted_cursor", 0)),
                                queue_depth=int(
                                    message.get("persistence_queue_depth", 0)
                                ),
                                error=message.get("persistence_error"),
                            )
                        elif kind == "book_confirmation":
                            self.projection.confirm_book(message["book"])
                            self.projection.update_persistence(
                                int(message.get("persisted_cursor", 0)),
                                queue_depth=int(
                                    message.get("persistence_queue_depth", 0)
                                ),
                                error=message.get("persistence_error"),
                            )
                        elif kind == "persisted":
                            self.projection.update_persistence(
                                int(message["cursor"])
                            )
                        elif kind == "persistence":
                            self.projection.update_persistence(
                                int(message.get("persisted_cursor", 0)),
                                queue_depth=int(
                                    message.get("persistence_queue_depth", 0)
                                ),
                                error=message.get("persistence_error"),
                            )
                        else:
                            raise ValueError("unsupported Polymarket stream frame")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.projection.mark_disconnected(
                    "polymarket_live_stream_cursor_gap"
                    if "cursor" in str(exc).lower()
                    else "polymarket_live_stream_disconnected"
                )
                LOGGER.warning(
                    "polymarket_live_stream_disconnected uri=%s error=%s detail=%s",
                    self.uri, type(exc).__name__, str(exc)[:300],
                )
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.reconnect_seconds)
            except TimeoutError:
                pass
