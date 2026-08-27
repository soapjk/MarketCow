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
    LiveFullSyncResponse,
    LiveReadHealth,
    LiveSnapshotPage,
    LiveStateStore,
    PolymarketLiveReadError,
    PolymarketLiveReadStore,
    bind_market_instrument_to_books,
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


def _decode_stream_message(
    raw: str | bytes,
) -> tuple[dict[str, Any], LiveEventEnvelope | LiveBook | None]:
    """Decode and authenticate one frame in one worker scheduling hop."""
    message = json.loads(raw)
    kind = message.get("type")
    if kind == "event":
        event = LiveEventEnvelope.model_validate(message["event"])
        _validate_event(event)
        return message, event
    if kind == "book_confirmation":
        return message, LiveBook.model_validate(message["book"])
    return message, None


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
        self._scope_id: str | None = None
        self._active_recovery_id: str | None = None
        self._latest_cursor = 0
        self._persisted_cursor = 0
        # The collector can report durability ahead of the last event decoded
        # by this API process.  Keep that source watermark private until the
        # corresponding event has joined the local projection.  The public
        # persisted cursor is therefore the durable boundary of this exact
        # projection generation, rather than a watermark from a future one.
        self._source_persisted_cursor = 0
        self._persistence_queue_depth = 0
        self._persistence_error: str | None = None
        self._derived_index_error: str | None = None
        self._connected = False
        self._disconnect_count = 0
        self._event_loop_stall_max_ms = 0.0
        self._ready = False
        self._error_code: str | None = "polymarket_live_stream_disconnected"
        self._last_message_at: datetime | None = None
        self._subscribers: set[asyncio.Queue[LiveEventEnvelope]] = set()
        self._generation = 0

    @property
    def ready(self) -> bool:
        with self._lock:
            return self._ready and self._connected and self._error_code is None

    @property
    def latest_cursor(self) -> int:
        with self._lock:
            return self._latest_cursor

    @property
    def scope_id(self) -> str | None:
        with self._lock:
            return self._scope_id

    def market_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._markets)

    def mark_connecting(self) -> None:
        with self._lock:
            self._connected = False
            self._ready = False
            self._error_code = "polymarket_live_stream_connecting"
            self._generation += 1

    def mark_disconnected(self, code: str = "polymarket_live_stream_disconnected") -> None:
        with self._lock:
            if self._connected or self._ready:
                self._disconnect_count += 1
            self._connected = False
            self._ready = False
            self._error_code = code
            self._generation += 1

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
        # Resolved gaps belong to the durable audit/history path.  Keeping the
        # complete ledger in the hot projection made every broad snapshot copy
        # and rescan tens of thousands of irrelevant entries.  The live read
        # contract only needs unresolved gaps to fail closed; recovery removes
        # them once their completion event has been applied.
        validated_gaps = [
            gap for item in gaps
            if not (gap := GapEntry.model_validate(item)).resolved
        ]
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
            self._scope_id = payload.get("scope_id") or None
            self._active_recovery_id = payload.get("active_recovery_id") or None
            self._latest_cursor = latest_cursor
            self._persisted_cursor = persisted_cursor
            self._source_persisted_cursor = persisted_cursor
            self._persistence_queue_depth = int(
                payload.get("persistence_queue_depth", 0)
            )
            self._persistence_error = payload.get("persistence_error") or None
            self._derived_index_error = (
                payload.get("derived_index_error") or None
            )
            self._connected = True
            self._ready = False
            self._error_code = None
            self._last_message_at = datetime.now(timezone.utc)
            next_generation = self._generation + 1
            for market_id, market in tuple(self._markets.items()):
                dynamic_boundaries = [
                    item.boundary_cursor
                    for item in market.rules.instrument.provenance
                    if item.source == "polymarket_clob"
                    and item.boundary_cursor is not None
                ]
                observed_at = max(
                    (
                        book.received_at
                        for outcome in market.identity.outcomes
                        if (book := self._books.get(outcome.token_id)) is not None
                    ),
                    default=self._last_message_at,
                )
                self._markets[market_id] = bind_market_instrument_to_books(
                    market,
                    self._books,
                    boundary_cursor=(
                        dynamic_boundaries[-1]
                        if dynamic_boundaries else latest_cursor
                    ),
                    projection_generation=next_generation,
                    observed_at=observed_at,
                )
            self._generation = next_generation

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
            self._generation += 1

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
            self._generation += 1

    def apply_live(self, raw: dict[str, Any]) -> LiveEventEnvelope:
        event = LiveEventEnvelope.model_validate(raw)
        _validate_event(event)
        return self.apply_validated_live(event)

    def apply_validated_live(
        self, event: LiveEventEnvelope,
    ) -> LiveEventEnvelope:
        """Apply an already authenticated event without CPU work on the loop."""
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
            self._advance_persisted_cursor_locked()
            self._events.append(event)
            self._apply_event_state(event)
            self._last_message_at = datetime.now(timezone.utc)
            self._generation += 1
            for queue in tuple(self._subscribers):
                if queue.full():
                    self._subscribers.discard(queue)
                    continue
                queue.put_nowait(event)
        return event

    def update_persistence(
        self, cursor: int, *, queue_depth: int = 0, error: str | None = None,
        derived_index_error: str | None = None,
    ) -> None:
        with self._lock:
            # The persistence worker and each WebSocket sender are independent
            # consumers of the collector's published queue. A sender may report
            # a durable watermark a few events ahead of the last event this API
            # process has decoded. Only regression is invalid; the local event
            # projection will deterministically catch up to the global durable
            # watermark.  A future source watermark is retained but is not
            # exposed as part of the current projection generation.
            if cursor < self._source_persisted_cursor:
                raise ValueError("Polymarket persisted cursor watermark is invalid")
            self._source_persisted_cursor = cursor
            self._advance_persisted_cursor_locked()
            self._persistence_queue_depth = max(0, queue_depth)
            self._persistence_error = error or None
            self._derived_index_error = derived_index_error or None
            self._generation += 1

    def _advance_persisted_cursor_locked(self) -> None:
        """Publish durability only for events present in this lock generation."""
        if self._persisted_cursor > self._latest_cursor:
            raise RuntimeError("Polymarket projection cursor invariant is invalid")
        if self._source_persisted_cursor >= self._latest_cursor:
            self._persisted_cursor = self._latest_cursor
        elif self._source_persisted_cursor > self._persisted_cursor:
            self._persisted_cursor = self._source_persisted_cursor

    def watermarks(self) -> dict[str, int | bool | str | None]:
        with self._lock:
            return {
                "published_cursor": self._latest_cursor,
                "persisted_cursor": self._persisted_cursor,
                "persistence_lag_events": (
                    self._latest_cursor - self._persisted_cursor
                ),
                "persistence_queue_depth": self._persistence_queue_depth,
                "live_stream_connected": self._connected,
                "live_stream_disconnect_count": self._disconnect_count,
                "event_loop_stall_max_ms": self._event_loop_stall_max_ms,
                "persistence_error": self._persistence_error,
                "derived_index_error": self._derived_index_error,
            }

    def observe_event_loop_stall(self, milliseconds: float) -> None:
        with self._lock:
            self._event_loop_stall_max_ms = max(
                self._event_loop_stall_max_ms, max(0.0, milliseconds)
            )

    def confirm_book(self, raw: dict[str, Any]) -> None:
        book = LiveBook.model_validate(raw)
        self.confirm_validated_book(book)

    def confirm_validated_book(self, book: LiveBook) -> None:
        """Apply an already validated confirmation in constant bounded time."""
        with self._lock:
            previous = self._books.get(book.token_id)
            if previous is None or previous.state_checksum != book.state_checksum:
                self._ready = False
                self._error_code = "polymarket_live_stream_confirmation_mismatch"
                raise ValueError("Polymarket live book confirmation mismatches state")
            self._books[book.token_id] = book
            self._last_message_at = datetime.now(timezone.utc)
            self._generation += 1

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
            market_id = event.market_id
            if market_id and market_id in self._markets:
                self._markets[market_id] = bind_market_instrument_to_books(
                    self._markets[market_id],
                    self._books,
                    boundary_cursor=event.cursor,
                    projection_generation=self._generation + 1,
                    observed_at=event.received_at,
                )
        if event.event_type == "market_terminal" and event.applied:
            from .polymarket_live import LiveMarket

            terminal = LiveMarket.model_validate(
                event.canonical_payload.get("market")
            )
            self._markets[terminal.identity.market_id] = terminal
        if event.event_type == "recovery_started" and event.applied:
            self._active_recovery_id = str(
                event.canonical_payload.get("recovery_id") or ""
            ) or None
        if event.event_type == "recovery_completed" and event.applied:
            recovered = set(
                event.canonical_payload.get("resolved_gap_token_ids")
                or event.canonical_payload.get("recovered_token_ids") or []
            )
            self._gaps = [
                gap for gap in self._gaps if gap.token_id not in recovered
            ]
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
        # Hold the ingestion lock only long enough to capture an immutable
        # watermark and event-reference snapshot.  Pydantic JSON sizing below
        # can take seconds for a 1 MiB page and must never delay the stream
        # consumer from applying the next event or book confirmation.
        with self._lock:
            if after_cursor > self._latest_cursor:
                raise PolymarketLiveReadError(
                    "polymarket_live_cursor_ahead",
                    "Requested cursor is ahead of the real-time projection",
                    409,
                )
            latest_cursor = self._latest_cursor
            events = tuple(self._events)
            oldest = events[0].cursor if events else latest_cursor + 1
            if after_cursor < oldest - 1:
                raise PolymarketLiveReadError(
                    "resume_cursor_expired",
                    "Resume cursor predates the in-memory replay window",
                    409,
                )
            if after_cursor > 0 and any(
                event.cursor > after_cursor
                and event.event_type == "catalog_revision"
                for event in events
            ):
                raise PolymarketLiveReadError(
                    "resume_cursor_expired",
                    "Resume cursor predates the current catalog revision",
                    409,
                )
        candidates = [
            event for event in events
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
            source_markets = [self._markets[item] for item in market_ids]
            books = dict(self._books)
            cursor = self._latest_cursor
            generation = self._generation
        # Deep-copying the 100-market catalog is response construction, not an
        # atomic projection operation.  Do it outside the ingestion lock.
        markets = [market.model_copy(deep=True) for market in source_markets]
        for market_index, market in enumerate(markets):
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
            if market.rules.instrument.price_increment != tick:
                market = bind_market_instrument_to_books(
                    market,
                    books,
                    boundary_cursor=cursor,
                    projection_generation=generation,
                    observed_at=max(book.received_at for book in live_books),
                )
                if market.rules.instrument.price_increment != tick:
                    raise PolymarketLiveReadError(
                        "polymarket_instrument_book_binding_incomplete",
                        "Live book tick differs from atomic instrument facts",
                        503,
                    )
                markets[market_index] = market
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
            revision = self._catalog_revision
            scope_id = self._scope_id
            source = self._catalog_source
            cursor = self._latest_cursor
            recovery_id = self._active_recovery_id
            books = dict(self._books)
            gaps = tuple(self._gaps)
        transient = LiveStateStore(reader.root / ".live-stream-projection")
        transient._recovered = True
        transient.now_provider = reader.now_provider
        transient.catalog = {
            market.identity.market_id: market for market in markets
        }
        transient.token_to_market = {
            outcome.token_id: market.identity.market_id
            for market in markets
            if market.lifecycle_state == "active" and market.accepting_orders
            for outcome in market.identity.outcomes
        }
        transient.catalog_revision = revision
        transient.scope_id = scope_id
        transient.catalog_source = source
        transient.cursor = cursor
        transient.active_recovery_id = recovery_id
        transient.books = books
        transient.gaps = [gap.model_copy(deep=True) for gap in gaps]
        return transient

    @staticmethod
    def _freshness_budget_exhausted() -> PolymarketLiveReadError:
        return PolymarketLiveReadError(
            "polymarket_snapshot_freshness_budget_exhausted",
            "The scoped in-memory snapshot cannot retain the required delivery headroom",
            503,
        )

    def _capture_scope(
        self,
        reader: PolymarketLiveReadStore,
        market_ids: Iterable[str],
        phases: dict[str, float],
    ) -> dict[str, Any]:
        """Capture one complete scoped projection generation under one lock."""
        selected = PolymarketLiveReadStore._scope(market_ids)
        lock_started = time.perf_counter()
        with self._lock:
            phases["lock_wait_ms"] = (time.perf_counter() - lock_started) * 1000
            if not self._ready or not self._connected or self._error_code is not None:
                raise PolymarketLiveReadError(
                    self._error_code or "polymarket_live_stream_not_ready",
                    "Polymarket real-time in-memory projection is not ready",
                    503,
                )
            if self._active_recovery_id or self._persistence_error:
                raise PolymarketLiveReadStore._stable_boundary_unavailable("full-sync")
            selected_markets = [self._markets.get(market_id) for market_id in selected]
            if any(market is None for market in selected_markets):
                raise PolymarketLiveReadError(
                    "polymarket_market_not_found",
                    "One or more markets are outside the live projection",
                    404,
                )
            relation_market_ids = sorted({
                pair.market_id
                for market in selected_markets if market is not None
                for relation in market.relations
                for pair in relation.outcome_pairs
                if pair.market_id not in selected
            })
            relation_markets = [self._markets.get(item) for item in relation_market_ids]
            if any(market is None for market in relation_markets):
                raise PolymarketLiveReadStore._stable_boundary_unavailable("full-sync")
            all_markets = [
                market for market in (*selected_markets, *relation_markets)
                if market is not None
            ]
            own_token_ids = {
                outcome.token_id
                for market in selected_markets
                if market is not None
                and market.lifecycle_state == "active"
                and market.accepting_orders
                for outcome in market.identity.outcomes
            }
            relation_token_ids = {
                pair.yes_token_id
                for market in selected_markets if market is not None
                and market.lifecycle_state == "active"
                for relation in market.relations
                for pair in relation.outcome_pairs
            }
            required_token_ids = own_token_ids | relation_token_ids
            source_books = {
                token_id: self._books.get(token_id)
                for token_id in required_token_ids
            }
            unresolved = [
                gap for gap in self._gaps
                if not gap.resolved and gap.token_id in required_token_ids
            ]
            checked_at = reader.now_provider()
            generation = self._generation
            revision = self._catalog_revision
            scope_id = self._scope_id
            cursor = self._latest_cursor
            catalog_source = self._catalog_source
            persisted_cursor = self._persisted_cursor
            if persisted_cursor > cursor:
                raise PolymarketLiveReadStore._stable_boundary_unavailable(
                    "full-sync"
                )
            persistence_queue_depth = self._persistence_queue_depth
            derived_index_error = self._derived_index_error
            connected = self._connected
            disconnect_count = self._disconnect_count
            event_loop_stall_max_ms = self._event_loop_stall_max_ms
            if (
                not revision
                or generation < 1
                or (
                    not required_token_ids
                    and any(
                        market is not None and market.lifecycle_state == "active"
                        for market in selected_markets
                    )
                )
                or unresolved
                or any(book is None for book in source_books.values())
            ):
                raise PolymarketLiveReadStore._stable_boundary_unavailable("full-sync")
            for market in selected_markets:
                if market is None:
                    continue
                if market.lifecycle_state in {"closed", "resolved", "invalid"}:
                    continue
                expected = sorted(
                    outcome.token_id for outcome in market.identity.outcomes
                )
                market_books = [source_books.get(token_id) for token_id in expected]
                observed_ticks = {
                    book.tick_size for book in market_books if book is not None
                }
                observed_tick_versions = {
                    book.tick_version for book in market_books if book is not None
                }
                dynamic_provenance = [
                    item for item in market.rules.instrument.provenance
                    if item.source == "polymarket_clob"
                ]
                dynamic_binding_invalid = bool(dynamic_provenance) and not any(
                    item.tick_version in observed_tick_versions
                    and item.boundary_cursor is not None
                    and item.boundary_cursor <= cursor
                    and item.projection_generation is not None
                    and item.projection_generation <= generation
                    and set(item.token_ids) == set(expected)
                    for item in dynamic_provenance
                )
                if (
                    any(book is None for book in market_books)
                    or len(observed_ticks) != 1
                    or len(observed_tick_versions) != 1
                    or dynamic_binding_invalid
                    or market.rules.instrument.price_increment
                    != market_books[0].tick_size
                ):
                    raise PolymarketLiveReadError(
                        "polymarket_instrument_book_binding_incomplete",
                        "Live book tick differs from atomic instrument facts",
                        503,
                    )
            # Projection models are publication snapshots: ingestion replaces
            # market/book/gap instances and never mutates an instance after it
            # becomes visible.  Capture their references while holding the
            # generation lock, then build and serialize the response after
            # releasing it.  Deep-copying 100 markets and 200 books here used
            # to hold the ingestion lock for hundreds of milliseconds per
            # broad read; sustained health/bootstrap traffic could therefore
            # starve the WebSocket consumer long enough for every book to age
            # beyond the fail-closed freshness budget.
            copy_started = time.perf_counter()
            markets = tuple(all_markets)
            books = {
                token_id: book
                for token_id, book in source_books.items()
                if book is not None
            }
            gaps = tuple(unresolved)
            phases["projection_copy_ms"] = (
                time.perf_counter() - copy_started
            ) * 1000
        oldest = min(
            (book.received_at for book in books.values()),
            default=checked_at,
        )
        maximum_age_ms = (checked_at - oldest).total_seconds() * 1000
        if maximum_age_ms < 0:
            raise self._freshness_budget_exhausted()
        phases.update({
            "scope_count": float(len(selected)),
            "maximum_book_age_ms": maximum_age_ms,
            "cursor": float(cursor),
            "freshness_check_ms": 0.0,
        })
        return {
            "selected": selected,
            "selected_count": len(selected),
            "markets": markets,
            "books": books,
            "gaps": gaps,
            "checked_at": checked_at,
            "oldest": oldest,
            "maximum_age_ms": maximum_age_ms,
            "generation": generation,
            "revision": revision,
            "scope_id": scope_id,
            "cursor": cursor,
            "catalog_source": catalog_source,
            "persisted_cursor": persisted_cursor,
            "persistence_queue_depth": persistence_queue_depth,
            "derived_index_error": derived_index_error,
            "connected": connected,
            "disconnect_count": disconnect_count,
            "event_loop_stall_max_ms": event_loop_stall_max_ms,
        }

    def _build_atomic_components(
        self, reader: PolymarketLiveReadStore, capture: dict[str, Any],
        phases: dict[str, float],
    ) -> tuple[LiveReadHealth, LiveBootstrapResponse, LiveSnapshotPage]:
        build_started = time.perf_counter()
        selected = capture["selected"]
        markets = capture["markets"]
        selected_markets = [
            market for market in markets
            if market.identity.market_id in selected
        ]
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
        transient.catalog_revision = capture["revision"]
        transient.catalog_source = capture["catalog_source"]
        transient.cursor = capture["cursor"]
        transient.books = capture["books"]
        transient.gaps = capture["gaps"]
        frames = [
            transient.frame(market_id, now=capture["checked_at"])
            for market_id in selected
        ]
        referenced_token_ids = {
            token_id for frame in frames
            for token_id in (*frame.token_ids, *frame.relation_token_ids)
        }
        page_books = {
            token_id: capture["books"][token_id]
            for token_id in sorted(referenced_token_ids)
            if token_id in capture["books"]
        }
        active_count = sum(
            market.lifecycle_state == "active" for market in selected_markets
        )
        terminal_count = len(selected_markets) - active_count
        complete_count = sum(frame.status == "ready" for frame in frames)
        missing_count = sum(frame.status == "fail_closed" for frame in frames)
        exact_ready = (
            len(selected) == 100
            and active_count == 100
            and complete_count == 100
            and missing_count == 0
        )
        ready = missing_count == 0
        reason_codes = sorted({
            reason for frame in frames for reason in frame.reason_codes
        })
        common = {
            "projection_generation": capture["generation"],
            "scope_market_ids": selected,
            "freshness_checked_at": capture["checked_at"],
            "scope_id": capture["scope_id"],
        }
        health = LiveReadHealth(
            status="index_ready" if ready and not terminal_count else "degraded",
            catalog_revision=capture["revision"],
            catalog_index_ready=True,
            latest_state_ready=ready and not terminal_count,
            market_count=len(selected),
            token_count=len(referenced_token_ids),
            book_token_count=len(page_books),
            book_complete_market_count=sum(frame.status == "ready" for frame in frames),
            active_market_count=active_count,
            terminal_market_count=terminal_count,
            complete_market_count=complete_count,
            missing_market_count=missing_count,
            scope_status=(
                "exact_ready" if exact_ready
                else "terminal_degraded" if terminal_count and not missing_count
                else "data_degraded"
            ),
            unresolved_gap_count=0,
            latest_cursor=capture["cursor"],
            persisted_cursor=capture["persisted_cursor"],
            persistence_lag_events=(
                capture["cursor"] - capture["persisted_cursor"]
            ),
            persistence_queue_depth=capture["persistence_queue_depth"],
            derived_index_error=capture["derived_index_error"],
            live_stream_connected=capture["connected"],
            live_stream_disconnect_count=capture["disconnect_count"],
            event_loop_stall_max_ms=capture["event_loop_stall_max_ms"],
            events_read_source="memory_projection",
            realtime_sqlite_query_ms=0,
            reason_codes=reason_codes,
            oldest_book_received_at=capture["oldest"],
            maximum_book_age_ms=capture["maximum_age_ms"],
            **common,
        )
        bootstrap = LiveBootstrapResponse(
            catalog_revision=capture["revision"],
            catalog_source=capture["catalog_source"],
            cursor=capture["cursor"],
            markets=selected_markets,
            active_token_ids=sorted({
                outcome.token_id
                for market in selected_markets if market.active and not market.closed
                for outcome in market.identity.outcomes
            }),
            sequence_semantics="deterministic_normalized",
            recovery={
                "bootstrap": "CLOB POST /books full snapshots",
                "disconnect": "new book_epoch followed by full /books recovery",
                "resume": "in-memory live stream replay required before event resume",
            },
            source_policy="official_free_only",
            **common,
        )
        snapshot = LiveSnapshotPage(
            catalog_revision=capture["revision"], cursor=capture["cursor"],
            count=len(selected), books=page_books, items=frames, **common,
        )
        phases["frame_build_ms"] = (time.perf_counter() - build_started) * 1000
        return health, bootstrap, snapshot

    def _serialize_scoped_response(
        self, reader: PolymarketLiveReadStore, market_ids: Iterable[str], kind: str,
        *, _phase_ms: dict[str, float] | None = None,
    ) -> tuple[bytes, dict[str, float], Any]:
        phases = _phase_ms if _phase_ms is not None else {}
        operation_started = time.perf_counter()
        capture = self._capture_scope(reader, market_ids, phases)
        health, bootstrap, snapshot = self._build_atomic_components(
            reader, capture, phases,
        )
        maximum_seconds = reader.consumer_maximum_book_age_seconds
        if maximum_seconds is None:
            raise ValueError("consumer maximum book age must be configured")
        delivery_ms = reader.minimum_delivery_headroom_seconds * 1000
        if kind == "health":
            model: Any = health
        elif kind == "bootstrap":
            model = bootstrap
        elif kind == "snapshot":
            model = snapshot
        elif kind == "full-sync":
            model = LiveFullSyncResponse(
                catalog_revision=capture["revision"], cursor=capture["cursor"],
                projection_generation=capture["generation"],
                scope_market_ids=capture["selected"],
                freshness_checked_at=capture["checked_at"],
                scope_id=capture["scope_id"],
                oldest_book_received_at=capture["oldest"],
                maximum_book_age_ms=capture["maximum_age_ms"],
                consumer_maximum_book_age_ms=maximum_seconds * 1000,
                minimum_delivery_headroom_ms=delivery_ms,
                freshness_budget_remaining_ms=0,
                health=health, bootstrap=bootstrap, snapshot=snapshot,
            )
        else:
            raise ValueError(f"unsupported scoped response kind: {kind}")
        serialization_started = time.perf_counter()
        payload = model.model_dump_json().encode("utf-8")
        serialization_ms = (time.perf_counter() - serialization_started) * 1000
        known_cost_ms = (time.perf_counter() - operation_started) * 1000
        remaining_ms = (
            maximum_seconds * 1000
            - capture["maximum_age_ms"]
            - known_cost_ms
        )
        if kind == "full-sync":
            # Reserve two measured serialization passes beyond the already
            # observed first pass.  The payload therefore reports a
            # conservative, directly verifiable delivery budget without a
            # self-referential serialization loop.
            remaining_ms -= 2 * serialization_ms
            model = model.model_copy(update={
                "freshness_budget_remaining_ms": remaining_ms,
            })
            serialization_started = time.perf_counter()
            payload = model.model_dump_json().encode("utf-8")
            serialization_ms += (
                time.perf_counter() - serialization_started
            ) * 1000
            known_cost_ms = (time.perf_counter() - operation_started) * 1000
            actual_remaining_ms = (
                maximum_seconds * 1000
                - capture["maximum_age_ms"]
                - known_cost_ms
            )
            if actual_remaining_ms < remaining_ms:
                raise self._freshness_budget_exhausted()
        check_started = time.perf_counter()
        phases.update({
            "json_serialize_ms": serialization_ms,
            "response_body_bytes": float(len(payload)),
            "freshness_headroom_ms": remaining_ms,
            "freshness_check_ms": (time.perf_counter() - check_started) * 1000,
        })
        if remaining_ms <= delivery_ms:
            raise self._freshness_budget_exhausted()
        return payload, phases, model

    def bootstrap(
        self, reader: PolymarketLiveReadStore, market_ids: Iterable[str]
    ) -> LiveBootstrapResponse:
        return self._serialize_scoped_response(
            reader, market_ids, "bootstrap",
        )[2]

    def bootstrap_json(
        self, reader: PolymarketLiveReadStore, market_ids: Iterable[str],
        *, _phase_ms: dict[str, float] | None = None,
    ) -> bytes:
        return self._serialize_scoped_response(
            reader, market_ids, "bootstrap", _phase_ms=_phase_ms,
        )[0]

    def snapshot(
        self, reader: PolymarketLiveReadStore, market_ids: Iterable[str]
    ) -> LiveSnapshotPage:
        return self._serialize_scoped_response(
            reader, market_ids, "snapshot",
        )[2]

    def snapshot_json(
        self, reader: PolymarketLiveReadStore, market_ids: Iterable[str],
        *, _phase_ms: dict[str, float] | None = None,
    ) -> bytes:
        return self._serialize_scoped_response(
            reader, market_ids, "snapshot", _phase_ms=_phase_ms,
        )[0]

    def full_sync_json(
        self, reader: PolymarketLiveReadStore, market_ids: Iterable[str],
        *, _phase_ms: dict[str, float] | None = None,
    ) -> tuple[bytes, dict[str, float], LiveFullSyncResponse]:
        payload, phases, model = self._serialize_scoped_response(
            reader, market_ids, "full-sync", _phase_ms=_phase_ms,
        )
        return payload, phases, model

    def health(
        self, reader: PolymarketLiveReadStore,
        market_ids: Iterable[str],
    ) -> LiveReadHealth:
        return self._serialize_scoped_response(
            reader, market_ids, "health",
        )[2]

    def health_json(
        self, reader: PolymarketLiveReadStore, market_ids: Iterable[str],
        *, _phase_ms: dict[str, float] | None = None,
    ) -> bytes:
        return self._serialize_scoped_response(
            reader, market_ids, "health", _phase_ms=_phase_ms,
        )[0]

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
            # Events are immutable publication records after _emit returns.
            # Copying their canonical/raw payload here would put response-size
            # CPU work back on the collector's receive/publish path.
            {"type": "event", "event": event},
        )

    def publish_book_confirmation(self, book: LiveBook) -> None:
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        loop.call_soon_threadsafe(
            self._broadcast,
            # LiveStateStore replaces books instead of mutating published
            # instances, so a reference is a stable publication snapshot.
            {"type": "book_confirmation", "book": book},
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
            markets = sorted(
                self.store.catalog.values(),
                key=lambda item: item.identity.market_id,
            )
            books = sorted(self.store.books.items())
            gaps = [gap for gap in self.store.gaps if not gap.resolved]
            state = {
                "schema_version": STREAM_SCHEMA,
                "type": "state",
                "catalog_revision": self.store.catalog_revision,
                "scope_id": self.store.scope_id,
                "catalog_source": self.store.catalog_source,
                "latest_cursor": self.store.cursor,
                "persisted_cursor": getattr(
                    self.store, "persisted_cursor", self.store.cursor
                ),
                "persistence_queue_depth": self.store._persistence_queue.qsize(),
                "persistence_error": self.store.persistence_error,
                "derived_index_error": self.store.derived_index_error,
                "active_recovery_id": self.store.active_recovery_id,
                "history_oldest_cursor": (
                    history[0].cursor if history else self.store.cursor + 1
                ),
            }
        # Serialization is intentionally outside the collector state lock.
        # Captured model instances are replaced, not mutated, by publication.
        state.update({
            "markets": [market.model_dump(mode="json") for market in markets],
            "books": [book.model_dump(mode="json") for _, book in books],
            # The loopback real-time projection only consumes unresolved gaps.
            # SQLite/JSONL retain the complete audit ledger.
            "gaps": [gap.model_dump(mode="json") for gap in gaps],
        })
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
                        "derived_index_error": self.store.derived_index_error,
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
                        "derived_index_error": self.store.derived_index_error,
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
                        "derived_index_error": self.store.derived_index_error,
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
                        # JSON decoding, Pydantic validation, and integrity hash
                        # verification are CPU-bound.  Keeping them off the API
                        # event loop prevents broad frame serialization or a
                        # slow HTTP writer from starving WebSocket receive.
                        message, validated = await asyncio.to_thread(
                            _decode_stream_message, raw
                        )
                        kind = message.get("type")
                        if kind == "state":
                            await asyncio.to_thread(
                                self.projection.install_state, message
                            )
                        elif kind == "history":
                            await asyncio.to_thread(
                                self.projection.add_history,
                                message.get("items") or [],
                            )
                        elif kind == "ready":
                            self.projection.mark_ready(message)
                        elif kind == "event":
                            if not isinstance(validated, LiveEventEnvelope):
                                raise ValueError("validated event frame is missing")
                            event = validated
                            self.projection.apply_validated_live(event)
                            self.projection.update_persistence(
                                int(message.get("persisted_cursor", 0)),
                                queue_depth=int(
                                    message.get("persistence_queue_depth", 0)
                                ),
                                error=message.get("persistence_error"),
                                derived_index_error=message.get(
                                    "derived_index_error"
                                ),
                            )
                        elif kind == "book_confirmation":
                            if not isinstance(validated, LiveBook):
                                raise ValueError(
                                    "validated book confirmation is missing"
                                )
                            book = validated
                            self.projection.confirm_validated_book(book)
                            self.projection.update_persistence(
                                int(message.get("persisted_cursor", 0)),
                                queue_depth=int(
                                    message.get("persistence_queue_depth", 0)
                                ),
                                error=message.get("persistence_error"),
                                derived_index_error=message.get(
                                    "derived_index_error"
                                ),
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
                                derived_index_error=message.get(
                                    "derived_index_error"
                                ),
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
