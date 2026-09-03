from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import threading
import uuid
import fcntl
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator, Literal

from pydantic import BaseModel, Field, model_validator
from fastapi import FastAPI

from .polymarket_contracts import content_sha256, decimal_text
from .polymarket_live import (
    LiveBook,
    LiveEventEnvelope,
    LiveMarket,
    PolymarketLiveReadError,
    PolymarketLiveReadStore,
    _iter_raw_catalog_rows,
)


DISCOVERY_SCHEMA_VERSION = "marketcow.polymarket.discovery.v2"
DISCOVERY_EVENT_SCHEMA_VERSION = "marketcow.polymarket.discovery-events.v2"
DISCOVERY_RELATION_SCHEMA_VERSION = "marketcow.polymarket.discovery-relation.v2"
LIFECYCLE_HISTORY_SCHEMA_VERSION = "marketcow.polymarket.lifecycle-history.v2"
LOGGER = logging.getLogger(__name__)


def install_discovery_openapi_extension(app: FastAPI) -> None:
    """Describe the WebSocket contract in an OpenAPI vendor extension."""
    original = app.openapi

    def openapi() -> dict[str, Any]:
        if app.openapi_schema is None:
            schema = original()
            schema["x-websocket-paths"] = {
                "/v1/prediction-markets/polymarket/live/discovery/stream": {
                    "schema_version": DISCOVERY_EVENT_SCHEMA_VERSION,
                    "query_parameters": {
                        "after_cursor": {"type": "integer", "minimum": 0}
                    },
                    "server_message": {
                        "$ref": "#/components/schemas/DiscoveryEventPage"
                    },
                    "resync_message_type": "resync_required",
                    "resync_close_code": 1012,
                    "resume_semantics": "strictly_after_cursor",
                }
            }
            app.openapi_schema = schema
        return app.openapi_schema

    app.openapi = openapi


class DiscoveryDepthLevel(BaseModel):
    notional: str
    buy_cost_at_notional: str | None
    sell_proceeds_at_notional: str | None
    buy_status: Literal["complete", "insufficient_depth", "book_unavailable"]
    sell_status: Literal["complete", "insufficient_depth", "book_unavailable"]


class DiscoveryOutcomeQuote(BaseModel):
    outcome: str
    token_id: str
    best_bid: str | None
    best_bid_size: str | None
    best_ask: str | None
    best_ask_size: str | None
    last_trade_price: str | None
    depth: list[DiscoveryDepthLevel]
    book_observed_at: datetime | None
    book_age_ms: int | None = Field(default=None, ge=0)
    book_revision: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class DiscoveryMarketQuote(BaseModel):
    market_id: str
    condition_id: str
    event_id: str
    yes_token_id: str | None
    no_token_id: str | None
    active: bool
    closed: bool
    accepting_orders: bool
    lifecycle_state: Literal["active", "closed", "resolved", "invalid"]
    start_at: datetime | None
    end_at: datetime | None
    negative_risk: bool
    negative_risk_relation_id: str | None
    outcomes: list[DiscoveryOutcomeQuote]
    book_observed_at: datetime | None
    book_age_ms: int | None = Field(default=None, ge=0)
    book_status: Literal[
        "ready", "missing_book", "incomplete_book", "stale_book",
        "inconsistent_tick", "missing_tick", "missing_minimum_order_size",
        "missing_fee_schedule", "incomplete_relation", "source_gap",
        "missing_outcome_identity",
    ]
    tick_size: str | None
    minimum_order_size: str | None
    fee_schedule_id: str | None
    missing_fields: list[str]
    metadata_revision: str
    book_revision: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    catalog_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    cursor: int = Field(ge=0)

    @model_validator(mode="after")
    def fail_closed_contract(self):
        if len(self.outcomes) != 2:
            raise ValueError("discovery quote requires exactly two source outcomes")
        has_yes_no = self.yes_token_id is not None and self.no_token_id is not None
        if has_yes_no != (
            {item.outcome.casefold() for item in self.outcomes} == {"yes", "no"}
        ):
            raise ValueError("YES/NO token identity must match the source outcomes")
        if not has_yes_no and (
            self.book_status != "missing_outcome_identity"
            or not {"yes_token_id", "no_token_id"}.issubset(self.missing_fields)
        ):
            raise ValueError("non-YES/NO markets must fail closed explicitly")
        if self.book_status == "ready" and self.missing_fields:
            raise ValueError("ready discovery quote cannot have missing fields")
        if self.book_status != "ready" and not self.missing_fields:
            raise ValueError("fail-closed discovery quote requires missing_fields")
        return self


class DiscoverySnapshotPage(BaseModel):
    schema_version: Literal["marketcow.polymarket.discovery.v2"] = (
        DISCOVERY_SCHEMA_VERSION
    )
    snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    catalog_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    boundary_cursor: int = Field(ge=0)
    observed_at: datetime
    depth_notionals: list[str] = Field(min_length=1)
    active_market_count: int = Field(ge=0)
    page_size: int = Field(ge=1)
    page_count: int = Field(ge=0)
    next_page_cursor: str | None
    items: list[DiscoveryMarketQuote]
    relation_count: int = Field(ge=0)
    resync_required: Literal[False] = False


class DiscoveryEvent(BaseModel):
    cursor: int = Field(ge=1)
    event_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    event_type: Literal[
        "quote_changed", "book_fail_closed", "relation_changed",
        "market_lifecycle_changed",
    ]
    market_id: str | None
    token_id: str | None
    relation_id: str | None
    catalog_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    observed_at: datetime
    quote: DiscoveryOutcomeQuote | None
    book_status: str
    missing_fields: list[str]
    raw_payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class DiscoveryEventPage(BaseModel):
    schema_version: Literal["marketcow.polymarket.discovery-events.v2"] = (
        DISCOVERY_EVENT_SCHEMA_VERSION
    )
    catalog_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    after_cursor: int = Field(ge=0)
    next_cursor: int = Field(ge=0)
    boundary_cursor: int = Field(ge=0)
    has_more: bool
    resync_required: bool
    items: list[DiscoveryEvent]


class DiscoveryMetadataFact(BaseModel):
    market_id: str
    question: str
    title: str
    description: str | None
    rules: str | None
    rules_revision: str | None
    rules_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    resolution_source: str | None
    resolution_source_url: str | None
    event_id: str
    event_start_at: datetime | None
    event_end_at: datetime | None
    market_close_at: datetime | None
    resolution_status: str | None
    resolution: str | None
    resolution_proposed_at: datetime | None
    challenge_deadline_at: datetime | None
    disputed_at: datetime | None
    resolved_at: datetime | None
    redeemable: bool | None
    redeemable_at: datetime | None
    terminal_at: datetime | None
    observed_at: datetime
    missing_fields: list[str]
    source: Literal["polymarket_gamma"] = "polymarket_gamma"
    source_revision: str
    source_url: str
    evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class DiscoveryMetadataPage(BaseModel):
    schema_version: Literal["marketcow.polymarket.discovery.v2"] = (
        DISCOVERY_SCHEMA_VERSION
    )
    snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    catalog_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    page_size: int = Field(ge=1)
    next_page_cursor: str | None
    items: list[DiscoveryMetadataFact]


class DiscoveryRelationMember(BaseModel):
    market_id: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    outcome_label: str
    pair_revision: str = Field(pattern=r"^[0-9a-f]{64}$")


class DiscoveryRelation(BaseModel):
    schema_version: Literal["marketcow.polymarket.discovery-relation.v2"] = (
        DISCOVERY_RELATION_SCHEMA_VERSION
    )
    relation_id: str
    relation_type: Literal["standard_negative_risk"] = "standard_negative_risk"
    member_market_ids: list[str]
    members: list[DiscoveryRelationMember]
    expected_member_count: int | None
    actual_member_count: int = Field(ge=0)
    complete: bool
    valid_from: datetime
    valid_to: datetime | None
    relation_revision: str
    catalog_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    provenance: dict[str, Any]
    evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    reason_codes: list[str]
    quotes: list[DiscoveryMarketQuote] = Field(default_factory=list)

    @model_validator(mode="after")
    def complete_members_are_atomic(self):
        if self.actual_member_count != len(self.members):
            raise ValueError("relation member count disagrees")
        if self.member_market_ids != [item.market_id for item in self.members]:
            raise ValueError("relation member identity disagrees")
        if self.complete != (
            self.expected_member_count is not None
            and self.expected_member_count == self.actual_member_count
            and self.actual_member_count >= 2
            and not self.reason_codes
        ):
            raise ValueError("relation completeness disagrees")
        return self


class LifecycleHistoryEvent(BaseModel):
    market_id: str
    event_type: Literal[
        "market_closed", "resolution_proposed", "resolution_disputed",
        "market_resolved", "redemption_available",
    ]
    source_event_id: str
    source_observed_at: datetime
    received_at: datetime
    resolution: str | None
    raw_payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    cursor: int = Field(ge=1)


class LifecycleHistoryPage(BaseModel):
    schema_version: Literal["marketcow.polymarket.lifecycle-history.v2"] = (
        LIFECYCLE_HISTORY_SCHEMA_VERSION
    )
    after_cursor: int = Field(ge=0)
    next_cursor: int = Field(ge=0)
    has_more: bool
    items: list[LifecycleHistoryEvent]


class _DiscoveryBoundary:
    def __init__(
        self,
        *,
        snapshot_id: str,
        catalog_revision: str,
        boundary_cursor: int,
        observed_at: datetime,
        database_path: Path,
        active_market_count: int,
        relation_count: int,
        realtime_universe_id: str | None = None,
    ) -> None:
        self.snapshot_id = snapshot_id
        self.catalog_revision = catalog_revision
        self.boundary_cursor = boundary_cursor
        self.observed_at = observed_at
        self.database_path = database_path
        self.active_market_count = active_market_count
        self.relation_count = relation_count
        self.realtime_universe_id = realtime_universe_id


def _decimal_config(values: Iterable[str]) -> tuple[str, ...]:
    result: list[str] = []
    for value in values:
        normalized = decimal_text(value, "depth_notional", allow_zero=False)
        if normalized in result:
            raise ValueError("depth notionals must be unique")
        result.append(normalized)
    if not result:
        raise ValueError("at least one explicit depth notional is required")
    if result != sorted(result, key=Decimal):
        raise ValueError("depth notionals must be strictly increasing")
    return tuple(result)


def _depth_value(levels: list[dict[str, str]], size: Decimal) -> str | None:
    remaining = size
    total = Decimal("0")
    for level in levels:
        level_size = Decimal(level["size"])
        fill = min(remaining, level_size)
        total += fill * Decimal(level["price"])
        remaining -= fill
        if remaining == 0:
            return format(total, "f")
    return None


def _outcome_quote(
    outcome: str,
    token_id: str,
    book: LiveBook | None,
    *,
    observed_at: datetime,
    notionals: tuple[str, ...],
) -> DiscoveryOutcomeQuote:
    if book is None:
        return DiscoveryOutcomeQuote(
            outcome=outcome,
            token_id=token_id,
            best_bid=None,
            best_bid_size=None,
            best_ask=None,
            best_ask_size=None,
            last_trade_price=None,
            depth=[DiscoveryDepthLevel(
                notional=value,
                buy_cost_at_notional=None,
                sell_proceeds_at_notional=None,
                buy_status="book_unavailable",
                sell_status="book_unavailable",
            ) for value in notionals],
            book_observed_at=None,
            book_age_ms=None,
            book_revision=None,
        )
    bids = sorted(book.bids, key=lambda item: Decimal(item["price"]), reverse=True)
    asks = sorted(book.asks, key=lambda item: Decimal(item["price"]))
    age_ms = max(0, int((observed_at - book.received_at).total_seconds() * 1000))
    return DiscoveryOutcomeQuote(
        outcome=outcome,
        token_id=token_id,
        best_bid=bids[0]["price"] if bids else None,
        best_bid_size=bids[0]["size"] if bids else None,
        best_ask=asks[0]["price"] if asks else None,
        best_ask_size=asks[0]["size"] if asks else None,
        last_trade_price=book.last_trade_price,
        depth=[DiscoveryDepthLevel(
            notional=value,
            buy_cost_at_notional=(buy := _depth_value(asks, Decimal(value))),
            sell_proceeds_at_notional=(sell := _depth_value(bids, Decimal(value))),
            buy_status="complete" if buy is not None else "insufficient_depth",
            sell_status="complete" if sell is not None else "insufficient_depth",
        ) for value in notionals],
        book_observed_at=book.received_at,
        book_age_ms=age_ms,
        book_revision=book.state_checksum,
    )


def _raw_instant(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


class PolymarketDiscoveryStore:
    """Atomic, full-catalog lightweight discovery projection.

    The hot-scope reader keeps its 100-market guard. This projection deliberately
    bypasses that guard and reads the full collector's immutable catalog plus one
    SQLite read transaction. Immutable boundaries remain cached by snapshot ID so
    later pages cannot mix catalog or book revisions.
    """

    def __init__(
        self,
        reader: PolymarketLiveReadStore,
        *,
        depth_notionals: Iterable[str],
        maximum_book_age_ms: int,
        retained_snapshots: int = 8,
    ) -> None:
        self.reader = reader
        self.depth_notionals = _decimal_config(depth_notionals)
        if maximum_book_age_ms <= 0:
            raise ValueError("maximum discovery book age must be positive")
        if retained_snapshots < 2:
            raise ValueError("at least two discovery snapshots must be retained")
        self.maximum_book_age_ms = maximum_book_age_ms
        self.retained_snapshots = retained_snapshots
        self._lock = threading.RLock()
        self._snapshots: OrderedDict[str, _DiscoveryBoundary] = OrderedDict()
        self._token_outcomes_revision: str | None = None
        self._token_outcomes: dict[str, str] = {}

    @staticmethod
    def _page_offset(snapshot_id: str, page_cursor: str | None) -> int:
        if page_cursor is None:
            return 0
        prefix, separator, raw_offset = page_cursor.partition(":")
        if separator != ":" or prefix != snapshot_id:
            raise PolymarketLiveReadError(
                "discovery_page_cursor_invalid",
                "Discovery page cursor is not bound to the requested snapshot",
                422,
            )
        try:
            offset = int(raw_offset)
        except ValueError as exc:
            raise PolymarketLiveReadError(
                "discovery_page_cursor_invalid",
                "Discovery page cursor offset is invalid",
                422,
            ) from exc
        if offset < 0:
            raise PolymarketLiveReadError(
                "discovery_page_cursor_invalid",
                "Discovery page cursor offset is invalid",
                422,
            )
        return offset

    def _load_catalog(self) -> tuple[
        str, list[LiveMarket], dict[str, dict[str, Any]], str, datetime,
    ]:
        payload, normalized_path, _, _ = self.reader._manifest_binding()
        revision = str(payload["catalog_revision"])
        markets: list[LiveMarket] = []
        with normalized_path.open("rb") as stream:
            for line in stream:
                body = line.removesuffix(b"\n")
                if body:
                    markets.append(LiveMarket.model_validate_json(body))
        source = payload.get("catalog_source")
        if not isinstance(source, dict):
            raise PolymarketLiveReadError(
                "discovery_catalog_source_missing",
                "Discovery catalog has no authoritative source binding",
                503,
            )
        raw_path = Path(str(source.get("raw_path") or "")).resolve()
        raw_rows: dict[str, dict[str, Any]] = {}
        for _, row in _iter_raw_catalog_rows(raw_path, str(source.get("raw_format") or "")):
            market_id = str(row.get("id") or "")
            if market_id:
                raw_rows[market_id] = row
        source_url = str(source.get("source_url") or "")
        observed_at = _raw_instant(source.get("observed_at"))
        if not source_url or observed_at is None:
            raise PolymarketLiveReadError(
                "discovery_catalog_source_invalid",
                "Discovery catalog source identity is incomplete",
                503,
            )
        return revision, markets, raw_rows, source_url, observed_at

    def _relations(
        self,
        markets: list[LiveMarket],
        catalog_revision: str,
        raw_rows: dict[str, dict[str, Any]],
    ) -> dict[str, DiscoveryRelation]:
        copies: dict[str, list[Any]] = {}
        for market in markets:
            for relation in market.relations:
                if relation.relation_type == "standard_negative_risk":
                    copies.setdefault(relation.relation_id, []).append(relation)
        result: dict[str, DiscoveryRelation] = {}
        raw_groups: dict[str, list[dict[str, Any]]] = {}
        for raw in raw_rows.values():
            event = (raw.get("events") or [{}])[0]
            group_id = str(
                raw.get("negRiskMarketID")
                or raw.get("neg_risk_market_id")
                or event.get("negRiskMarketID")
                or ""
            )
            if bool(raw.get("negRisk") or raw.get("neg_risk")) and group_id:
                raw_groups.setdefault(f"neg-risk:{group_id}", []).append(raw)
        for relation_id, values in sorted(copies.items()):
            first = values[0]
            pairs = sorted(first.outcome_pairs, key=lambda item: item.market_id)
            members = [DiscoveryRelationMember(
                market_id=item.market_id,
                condition_id=item.condition_id,
                yes_token_id=item.yes_token_id,
                no_token_id=item.no_token_id,
                outcome_label=item.outcome_label,
                pair_revision=item.pair_revision,
            ) for item in pairs]
            reason_codes = set(first.missing_fields)
            if any(value.revision != first.revision for value in values):
                reason_codes.add("relation_revision_disagreement")
            if any(value.outcome_pairs != first.outcome_pairs for value in values):
                reason_codes.add("relation_member_disagreement")
            source_group = raw_groups.get(relation_id, [])
            source_group_count = len(source_group)
            if not source_group:
                reason_codes.add("source_relation_group_missing")
            if len(members) != source_group_count:
                reason_codes.add("member_count_mismatch")
            expected = source_group_count
            if expected < 2:
                reason_codes.add("insufficient_members")
            result[relation_id] = DiscoveryRelation(
                relation_id=relation_id,
                member_market_ids=[item.market_id for item in members],
                members=members,
                expected_member_count=expected,
                actual_member_count=len(members),
                complete=not reason_codes and len(members) == expected and expected >= 2,
                valid_from=first.valid_from,
                valid_to=first.valid_to,
                relation_revision=first.revision,
                catalog_revision=catalog_revision,
                provenance=first.provenance.model_dump(mode="json"),
                evidence_sha256=content_sha256({
                    "catalog_revision": catalog_revision,
                    "relation_id": relation_id,
                    "revision": first.revision,
                    "members": [item.model_dump(mode="json") for item in members],
                    "raw_member_payload_sha256": sorted(
                        content_sha256(item) for item in source_group
                    ),
                }),
                reason_codes=sorted(reason_codes),
            )
        return result

    def capture(self) -> _DiscoveryBoundary:
        catalog_revision, markets, raw_rows, source_url, catalog_observed_at = (
            self._load_catalog()
        )
        active = sorted(
            (market for market in markets if market.active and not market.closed),
            key=lambda item: item.identity.market_id,
        )
        relations = self._relations(markets, catalog_revision, raw_rows)
        with self.reader._state_snapshot() as (_, connection, state):
            if state["catalog_revision"] != catalog_revision:
                raise PolymarketLiveReadError(
                    "discovery_cross_revision_boundary",
                    "Discovery catalog and book revisions disagree",
                    409,
                )
            rows = list(connection.execute(
                """SELECT b.token_id, b.market_id, b.payload_json, b.payload_sha256,
                    c.exchange_at AS confirmed_exchange_at,
                    c.received_at AS confirmed_received_at,
                    c.state_checksum AS confirmed_state_checksum,
                    c.source_hash AS confirmed_source_hash,
                    c.confirmation_sha256
                    FROM books b LEFT JOIN book_confirmations c
                    ON c.token_id=b.token_id"""
            ))
            gap_market_ids = {
                str(row[0]) for row in connection.execute(
                    "SELECT DISTINCT market_id FROM gaps WHERE resolved=0 AND market_id IS NOT NULL"
                )
            }
            boundary_cursor = int(state["latest_cursor"])
        books = {
            str(row["token_id"]): self.reader._indexed_book(row)
            for row in rows
        }
        observed_at = self.reader.now_provider()
        quotes: list[DiscoveryMarketQuote] = []
        metadata: list[DiscoveryMetadataFact] = []
        for market in active:
            by_outcome = {
                item.outcome.casefold(): item for item in market.identity.outcomes
            }
            if set(by_outcome) != {"yes", "no"}:
                raise PolymarketLiveReadError(
                    "discovery_market_identity_invalid",
                    "Discovery market does not have an explicit YES/NO identity",
                    409,
                )
            market_books = {
                name: books.get(outcome.token_id)
                for name, outcome in by_outcome.items()
            }
            outcome_quotes = [
                _outcome_quote(
                    name.upper(), by_outcome[name].token_id, market_books[name],
                    observed_at=observed_at,
                    notionals=self.depth_notionals,
                )
                for name in ("yes", "no")
            ]
            missing: list[str] = []
            status = "ready"
            present = [book for book in market_books.values() if book is not None]
            if len(present) != 2:
                status = "missing_book"
                missing.extend(
                    f"book:{name}_token" for name, book in market_books.items()
                    if book is None
                )
            elif any(not book.bids or not book.asks for book in present):
                status = "incomplete_book"
                missing.append("two_sided_book")
            elif any(
                int((observed_at - book.received_at).total_seconds() * 1000)
                > self.maximum_book_age_ms
                for book in present
            ):
                status = "stale_book"
                missing.append("fresh_book")
            ticks = {book.tick_size for book in present}
            instrument = market.rules.instrument
            if instrument.price_increment is None:
                status = "missing_tick"
                missing.append("tick_size")
            elif present and (
                len(ticks) != 1 or instrument.price_increment not in ticks
            ):
                status = "inconsistent_tick"
                missing.append("atomic_tick_revision")
            if instrument.minimum_order_size is None:
                status = "missing_minimum_order_size"
                missing.append("minimum_order_size")
            if not market.rules.fee_schedule.complete:
                status = "missing_fee_schedule"
                missing.extend(
                    f"fee_schedule:{value}"
                    for value in market.rules.fee_schedule.missing_fields
                )
            relation_id = next((
                item.relation_id for item in market.relations
                if item.relation_type == "standard_negative_risk"
            ), None)
            if market.identity.neg_risk and (
                relation_id is None
                or relation_id not in relations
                or not relations[relation_id].complete
            ):
                status = "incomplete_relation"
                missing.append("complete_negative_risk_relation")
            if market.identity.market_id in gap_market_ids:
                status = "source_gap"
                missing.append("unresolved_source_gap")
            book_observed_at = (
                min(book.received_at for book in present) if present else None
            )
            book_age_ms = (
                max(0, int((observed_at - book_observed_at).total_seconds() * 1000))
                if book_observed_at is not None else None
            )
            book_revision = (
                content_sha256({
                    book.token_id: book.state_checksum for book in sorted(
                        present, key=lambda value: value.token_id
                    )
                }) if len(present) == 2 else None
            )
            quote = DiscoveryMarketQuote(
                market_id=market.identity.market_id,
                condition_id=market.identity.condition_id,
                event_id=market.identity.event_id,
                yes_token_id=by_outcome["yes"].token_id,
                no_token_id=by_outcome["no"].token_id,
                active=market.active,
                closed=market.closed,
                accepting_orders=market.accepting_orders,
                lifecycle_state=market.lifecycle_state,
                start_at=market.start_at,
                end_at=market.end_at,
                negative_risk=market.identity.neg_risk,
                negative_risk_relation_id=relation_id,
                outcomes=outcome_quotes,
                book_observed_at=book_observed_at,
                book_age_ms=book_age_ms,
                book_status=status,
                tick_size=instrument.price_increment,
                minimum_order_size=instrument.minimum_order_size,
                fee_schedule_id=(
                    market.rules.fee_schedule.schedule_id
                    if market.rules.fee_schedule.complete else None
                ),
                missing_fields=sorted(set(missing)),
                metadata_revision=market.metadata_revision,
                book_revision=book_revision,
                catalog_revision=catalog_revision,
                cursor=boundary_cursor,
            )
            quotes.append(quote)
            raw = raw_rows.get(market.identity.market_id)
            if raw is None:
                raise PolymarketLiveReadError(
                    "discovery_raw_evidence_missing",
                    "Discovery metadata lacks its raw source row",
                    409,
                )
            rules_text = raw.get("rules") or raw.get("description")
            description = raw.get("description")
            resolution_source = raw.get("resolutionSource") or raw.get("resolution_source")
            resolution_url = (
                raw.get("resolutionSourceUrl")
                or raw.get("resolution_source_url")
            )
            proposed_at = _raw_instant(
                raw.get("resolutionProposedAt") or raw.get("resolution_proposed_at")
            )
            challenge_at = _raw_instant(
                raw.get("challengeDeadlineAt") or raw.get("challenge_deadline_at")
            )
            disputed_at = _raw_instant(raw.get("disputedAt") or raw.get("disputed_at"))
            resolved_at = _raw_instant(raw.get("resolvedAt") or raw.get("resolved_at"))
            redeemable_at = _raw_instant(
                raw.get("redeemableAt") or raw.get("redeemable_at")
            )
            market_close_at = _raw_instant(
                raw.get("closedTime") or raw.get("closedAt") or raw.get("closed_at")
            )
            event = (raw.get("events") or [{}])[0]
            event_start_at = _raw_instant(
                event.get("startDate") or raw.get("startDate")
            )
            event_end_at = _raw_instant(
                event.get("endDate") or raw.get("endDate")
            )
            missing_metadata = [
                name for name, value in {
                    "description": description,
                    "rules": rules_text,
                    "resolution_source": resolution_source,
                    "resolution_source_url": resolution_url,
                    "event_start_at": event_start_at,
                    "event_end_at": event_end_at,
                    "market_close_at": market_close_at,
                    "resolution_status": raw.get("resolutionStatus"),
                    "resolution_proposed_at": proposed_at,
                    "challenge_deadline_at": challenge_at,
                    "disputed_at": disputed_at,
                    "resolved_at": resolved_at,
                    "redeemable": (
                        raw.get("redeemable")
                        if isinstance(raw.get("redeemable"), bool) else None
                    ),
                    "redeemable_at": redeemable_at,
                }.items() if value is None
            ]
            rules_revision = (
                content_sha256({"rules": str(rules_text), "source": market.raw_payload_sha256})
                if rules_text is not None else None
            )
            metadata.append(DiscoveryMetadataFact(
                market_id=market.identity.market_id,
                question=market.question,
                title=market.title,
                description=str(description) if description is not None else None,
                rules=str(rules_text) if rules_text is not None else None,
                rules_revision=rules_revision,
                rules_sha256=(
                    hashlib.sha256(str(rules_text).encode("utf-8")).hexdigest()
                    if rules_text is not None else None
                ),
                resolution_source=(
                    str(resolution_source) if resolution_source is not None else None
                ),
                resolution_source_url=(
                    str(resolution_url) if resolution_url is not None else None
                ),
                event_id=market.identity.event_id,
                event_start_at=event_start_at,
                event_end_at=event_end_at,
                market_close_at=market_close_at,
                resolution_status=(
                    str(raw["resolutionStatus"])
                    if raw.get("resolutionStatus") is not None else None
                ),
                resolution=market.resolution,
                resolution_proposed_at=proposed_at,
                challenge_deadline_at=challenge_at,
                disputed_at=disputed_at,
                resolved_at=resolved_at,
                redeemable=(
                    raw["redeemable"]
                    if isinstance(raw.get("redeemable"), bool) else None
                ),
                redeemable_at=redeemable_at,
                terminal_at=market.terminal_at,
                observed_at=market.observed_at,
                missing_fields=sorted(missing_metadata),
                source_revision=market.metadata_revision,
                source_url=source_url,
                evidence_sha256=content_sha256({
                    "catalog_revision": catalog_revision,
                    "market_id": market.identity.market_id,
                    "raw_payload_sha256": market.raw_payload_sha256,
                    "observed_at": catalog_observed_at.isoformat(),
                }),
            ))
        snapshot_id = content_sha256({
            "schema_version": DISCOVERY_SCHEMA_VERSION,
            "catalog_revision": catalog_revision,
            "boundary_cursor": boundary_cursor,
            "depth_notionals": self.depth_notionals,
            "market_revisions": [
                [item.market_id, item.metadata_revision, item.book_revision, item.book_status]
                for item in quotes
            ],
        })
        boundary = _DiscoveryBoundary(
            snapshot_id=snapshot_id,
            catalog_revision=catalog_revision,
            boundary_cursor=boundary_cursor,
            observed_at=observed_at,
            quotes=quotes,
            metadata=metadata,
            relations=relations,
        )
        quote_by_market = {item.market_id: item for item in quotes}
        for relation in boundary.relations.values():
            relation.quotes = [
                quote_by_market[market_id]
                for market_id in relation.member_market_ids
                if market_id in quote_by_market
            ]
        with self._lock:
            self._snapshots[snapshot_id] = boundary
            self._snapshots.move_to_end(snapshot_id)
            while len(self._snapshots) > self.retained_snapshots:
                self._snapshots.popitem(last=False)
        return boundary

    def boundary(self, snapshot_id: str | None) -> _DiscoveryBoundary:
        if snapshot_id is None:
            return self.capture()
        with self._lock:
            boundary = self._snapshots.get(snapshot_id)
        if boundary is None:
            raise PolymarketLiveReadError(
                "discovery_snapshot_expired",
                "Discovery snapshot is no longer retained; resync is required",
                410,
            )
        return boundary

    def snapshot_page(
        self,
        *,
        snapshot_id: str | None,
        page_cursor: str | None,
        page_size: int,
    ) -> DiscoverySnapshotPage:
        if not 1 <= page_size <= 1_000:
            raise PolymarketLiveReadError(
                "discovery_page_size_invalid", "page_size must be in [1, 1000]", 422
            )
        boundary = self.boundary(snapshot_id)
        offset = self._page_offset(boundary.snapshot_id, page_cursor)
        items = boundary.quotes[offset:offset + page_size]
        next_offset = offset + len(items)
        return DiscoverySnapshotPage(
            snapshot_id=boundary.snapshot_id,
            catalog_revision=boundary.catalog_revision,
            boundary_cursor=boundary.boundary_cursor,
            observed_at=boundary.observed_at,
            depth_notionals=list(self.depth_notionals),
            active_market_count=len(boundary.quotes),
            page_size=page_size,
            page_count=len(items),
            next_page_cursor=(
                f"{boundary.snapshot_id}:{next_offset}"
                if next_offset < len(boundary.quotes) else None
            ),
            items=items,
            relation_count=len(boundary.relations),
        )

    def metadata_page(
        self,
        *,
        snapshot_id: str,
        page_cursor: str | None,
        page_size: int,
    ) -> DiscoveryMetadataPage:
        boundary = self.boundary(snapshot_id)
        offset = self._page_offset(boundary.snapshot_id, page_cursor)
        items = boundary.metadata[offset:offset + page_size]
        next_offset = offset + len(items)
        return DiscoveryMetadataPage(
            snapshot_id=boundary.snapshot_id,
            catalog_revision=boundary.catalog_revision,
            page_size=page_size,
            next_page_cursor=(
                f"{boundary.snapshot_id}:{next_offset}"
                if next_offset < len(boundary.metadata) else None
            ),
            items=items,
        )

    def relation(self, relation_id: str, snapshot_id: str) -> DiscoveryRelation:
        boundary = self.boundary(snapshot_id)
        relation = boundary.relations.get(relation_id)
        if relation is None:
            raise PolymarketLiveReadError(
                "discovery_relation_not_found", "Relation is not in this snapshot", 404
            )
        return relation

    def _read_events(
        self, after_cursor: int, limit: int,
    ) -> tuple[list[LiveEventEnvelope], int, bool, str]:
        if not 1 <= limit <= 10_000:
            raise PolymarketLiveReadError(
                "discovery_event_limit_invalid", "limit must be in [1, 10000]", 422
            )
        with self.reader._state_snapshot() as (_, connection, metadata):
            earliest = connection.execute(
                "SELECT MIN(cursor) FROM event_offsets"
            ).fetchone()[0]
            boundary_cursor = int(metadata["latest_cursor"])
            if after_cursor > boundary_cursor:
                raise PolymarketLiveReadError(
                    "discovery_cursor_ahead", "Cursor is ahead of the live boundary", 422
                )
            if after_cursor and earliest is not None and after_cursor < int(earliest) - 1:
                raise PolymarketLiveReadError(
                    "discovery_cursor_expired",
                    "Discovery cursor expired; resync from a snapshot",
                    410,
                )
            rows = list(connection.execute(
                """SELECT cursor, byte_offset, byte_length, line_sha256
                    FROM event_offsets WHERE cursor > ? ORDER BY cursor LIMIT ?""",
                (after_cursor, limit + 1),
            ))
            catalog_revision = str(metadata["catalog_revision"])
        has_more = len(rows) > limit
        rows = rows[:limit]
        events: list[LiveEventEnvelope] = []
        with self.reader.event_path.open("rb") as stream:
            for row in rows:
                stream.seek(int(row["byte_offset"]))
                line = stream.read(int(row["byte_length"]))
                if hashlib.sha256(line).hexdigest() != row["line_sha256"]:
                    raise PolymarketLiveReadError(
                        "polymarket_state_integrity_failed", "Event hash mismatch", 409
                    )
                events.append(LiveEventEnvelope.model_validate_json(line))
        return events, boundary_cursor, has_more, catalog_revision

    def events_page(self, after_cursor: int, limit: int) -> DiscoveryEventPage:
        events, boundary_cursor, has_more, catalog_revision = self._read_events(
            after_cursor, limit
        )
        items: list[DiscoveryEvent] = []
        resync_required = any(
            event.event_type in {
                "catalog_revision", "market_terminal", "market_resolved"
            }
            for event in events
        )
        if not resync_required:
            with self._lock:
                token_outcomes = (
                    dict(self._token_outcomes)
                    if self._token_outcomes_revision == catalog_revision else None
                )
            if token_outcomes is None:
                loaded_revision, markets, _, _, _ = self._load_catalog()
                if loaded_revision != catalog_revision:
                    raise PolymarketLiveReadError(
                        "discovery_cross_revision_boundary",
                        "Discovery event catalog changed while building a page",
                        409,
                    )
                token_outcomes = {
                    outcome.token_id: outcome.outcome.upper()
                    for market in markets
                    for outcome in market.identity.outcomes
                }
                with self._lock:
                    self._token_outcomes_revision = catalog_revision
                    self._token_outcomes = dict(token_outcomes)
        for event in events:
            if event.event_type == "catalog_revision":
                changes = event.canonical_payload.get("relation_changes") or {}
                relation_ids = sorted(set(
                    (changes.get("added_relation_ids") or [])
                    + (changes.get("removed_relation_ids") or [])
                    + (changes.get("changed_relation_ids") or [])
                )) or [None]
                for relation_id in relation_ids:
                    items.append(DiscoveryEvent(
                        cursor=event.cursor,
                        event_id=(
                            content_sha256({
                                "source_event_id": event.event_id,
                                "relation_id": relation_id,
                            }) if relation_id is not None else event.event_id
                        ),
                        event_type="relation_changed",
                        market_id=None,
                        token_id=None,
                        relation_id=relation_id,
                        catalog_revision=catalog_revision,
                        observed_at=event.received_at,
                        quote=None,
                        book_status="resync_required",
                        missing_fields=["atomic_catalog_resync"],
                        raw_payload_sha256=event.raw_payload_sha256,
                    ))
                continue
            if event.event_type in {"market_terminal", "market_resolved"}:
                items.append(DiscoveryEvent(
                    cursor=event.cursor,
                    event_id=event.event_id,
                    event_type="market_lifecycle_changed",
                    market_id=event.market_id,
                    token_id=event.token_id,
                    relation_id=None,
                    catalog_revision=catalog_revision,
                    observed_at=event.received_at,
                    quote=None,
                    book_status="resync_required",
                    missing_fields=["atomic_catalog_resync"],
                    raw_payload_sha256=event.raw_payload_sha256,
                ))
                continue
            if resync_required:
                continue
            if event.event_type not in {
                "book", "price_change", "best_bid_ask", "last_trade_price",
                "tick_size_change",
            }:
                continue
            quote = None
            if event.applied and event.token_id:
                try:
                    book = LiveBook.model_validate(event.canonical_payload)
                    quote = _outcome_quote(
                        token_outcomes[event.token_id], event.token_id, book,
                        observed_at=event.received_at,
                        notionals=self.depth_notionals,
                    )
                except Exception:
                    quote = None
            fail_closed = not event.applied or quote is None
            items.append(DiscoveryEvent(
                cursor=event.cursor,
                event_id=event.event_id,
                event_type="book_fail_closed" if fail_closed else "quote_changed",
                market_id=event.market_id,
                token_id=event.token_id,
                relation_id=None,
                catalog_revision=catalog_revision,
                observed_at=event.received_at,
                quote=quote,
                book_status="source_gap" if fail_closed else "ready",
                missing_fields=(
                    [event.fail_closed_reason or "authoritative_book_unavailable"]
                    if fail_closed else []
                ),
                raw_payload_sha256=event.raw_payload_sha256,
            ))
        next_cursor = events[-1].cursor if events else after_cursor
        return DiscoveryEventPage(
            catalog_revision=catalog_revision,
            after_cursor=after_cursor,
            next_cursor=next_cursor,
            boundary_cursor=boundary_cursor,
            has_more=has_more,
            resync_required=resync_required,
            items=items,
        )

    def lifecycle_history(
        self,
        *,
        market_id: str | None,
        start_at: datetime | None,
        end_at: datetime | None,
        after_cursor: int,
        limit: int,
    ) -> LifecycleHistoryPage:
        if (
            (start_at is not None and start_at.tzinfo is None)
            or (end_at is not None and end_at.tzinfo is None)
            or (
                start_at is not None
                and end_at is not None
                and start_at >= end_at
            )
        ):
            raise PolymarketLiveReadError(
                "lifecycle_history_time_range_invalid",
                "Lifecycle history requires an ordered timezone-aware range",
                422,
            )
        events, _, has_more, _ = self._read_events(after_cursor, limit)
        mapping = {
            "market_terminal": "market_closed",
            "resolution_proposed": "resolution_proposed",
            "resolution_disputed": "resolution_disputed",
            "market_resolved": "market_resolved",
            "redemption_available": "redemption_available",
        }
        items = []
        for event in events:
            mapped = mapping.get(event.event_type)
            if mapped is None or event.market_id is None:
                continue
            if market_id is not None and event.market_id != market_id:
                continue
            if start_at is not None and event.exchange_at < start_at:
                continue
            if end_at is not None and event.exchange_at >= end_at:
                continue
            items.append(LifecycleHistoryEvent(
                market_id=event.market_id,
                event_type=mapped,
                source_event_id=str(
                    event.canonical_payload.get("source_event_id")
                    or event.raw_payload.get("source_event_id")
                    or event.event_id
                ),
                source_observed_at=event.exchange_at,
                received_at=event.received_at,
                resolution=(
                    str(event.canonical_payload.get("resolution"))
                    if event.canonical_payload.get("resolution") is not None else None
                ),
                raw_payload_sha256=event.raw_payload_sha256,
                cursor=event.cursor,
            ))
        next_cursor = events[-1].cursor if events else after_cursor
        return LifecycleHistoryPage(
            after_cursor=after_cursor,
            next_cursor=next_cursor,
            has_more=has_more,
            items=items,
        )


_InMemoryPolymarketDiscoveryStore = PolymarketDiscoveryStore


class PolymarketDiscoveryStore(_InMemoryPolymarketDiscoveryStore):
    """Single-flight, disk-materialized full-market discovery projection.

    The initial catalog is streamed into SQLite one market at a time. Later book
    changes append only the affected market quote versions. HTTP pagination never
    invokes materialization and keeps only the requested page of Pydantic objects
    in memory.
    """

    def __init__(
        self,
        reader: PolymarketLiveReadStore,
        *,
        depth_notionals: Iterable[str],
        maximum_book_age_ms: int,
        retained_snapshots: int = 8,
        refresh_interval_seconds: float = 0.5,
    ) -> None:
        super().__init__(
            reader,
            depth_notionals=depth_notionals,
            maximum_book_age_ms=maximum_book_age_ms,
            retained_snapshots=retained_snapshots,
        )
        if refresh_interval_seconds <= 0:
            raise ValueError("discovery refresh interval must be positive")
        self.refresh_interval_seconds = refresh_interval_seconds
        self.materialization_root = reader.root / "discovery-materialized-v2"
        self.materialization_root.mkdir(parents=True, exist_ok=True)
        self.current_manifest_path = self.materialization_root / "current.json"
        self.materialization_lock_path = self.materialization_root / ".materialization.lock"
        self._build_lock = threading.Lock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._materialization_state = "not_started"
        self._materialization_error: str | None = None
        self._publish_count = 0
        self._restore_published()

    @staticmethod
    def _open_database(path: Path, *, readonly: bool = False) -> sqlite3.Connection:
        if readonly:
            connection = sqlite3.connect(
                f"file:{path}?mode=ro", uri=True, timeout=0.5
            )
            connection.execute("PRAGMA query_only=ON")
        else:
            connection = sqlite3.connect(path, timeout=1.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=500")
        connection.execute("PRAGMA cache_size=-16384")
        connection.execute("PRAGMA temp_store=FILE")
        return connection

    @classmethod
    @contextmanager
    def _database(
        cls, path: Path, *, readonly: bool = False
    ) -> Iterator[sqlite3.Connection]:
        connection = cls._open_database(path, readonly=readonly)
        try:
            yield connection
        finally:
            connection.close()

    def _restore_published(self) -> None:
        if not self.current_manifest_path.exists():
            return
        try:
            payload = json.loads(self.current_manifest_path.read_bytes())
            database_path = Path(str(payload["database_path"])).resolve()
            if not database_path.is_relative_to(self.materialization_root.resolve()):
                raise ValueError("materialized database escapes its root")
            with self._database(database_path, readonly=True) as connection:
                rows = list(connection.execute(
                    "SELECT snapshot_id, catalog_revision, boundary_cursor, "
                    "observed_at, active_market_count, relation_count, "
                    "realtime_universe_id "
                    "FROM snapshots WHERE boundary_cursor<=? "
                    "ORDER BY boundary_cursor DESC LIMIT ?",
                    (
                        int(payload["boundary_cursor"]),
                        self.retained_snapshots,
                    ),
                ))
            if not rows or str(rows[0]["snapshot_id"]) != payload["snapshot_id"]:
                raise ValueError("materialization manifest is not a publication fence")
            for row in reversed(rows):
                boundary = _DiscoveryBoundary(
                    snapshot_id=str(row["snapshot_id"]),
                    catalog_revision=str(row["catalog_revision"]),
                    boundary_cursor=int(row["boundary_cursor"]),
                    observed_at=_raw_instant(row["observed_at"]),
                    database_path=database_path,
                    active_market_count=int(row["active_market_count"]),
                    relation_count=int(row["relation_count"]),
                    realtime_universe_id=(
                        str(row["realtime_universe_id"])
                        if row["realtime_universe_id"] is not None else None
                    ),
                )
                self._snapshots[boundary.snapshot_id] = boundary
            if rows:
                self._materialization_state = "ready"
        except (OSError, ValueError, KeyError, sqlite3.Error):
            self._snapshots.clear()

    def start_background_materialization(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                self._wake.set()
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._materialization_loop,
                name="marketcow-discovery-materializer",
                daemon=True,
            )
            self._thread.start()

    def stop_background_materialization(self) -> None:
        self._stop.set()
        self._wake.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)

    def _materialization_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.materialize_once()
            except Exception as exc:
                with self._lock:
                    self._materialization_state = "failed"
                    self._materialization_error = f"{type(exc).__name__}: {exc}"
                LOGGER.exception("polymarket_discovery_materialization_failed")
            self._wake.wait(self.refresh_interval_seconds)
            self._wake.clear()

    def materialization_status(self) -> dict[str, Any]:
        with self._lock:
            latest = next(reversed(self._snapshots.values()), None)
            return {
                "state": self._materialization_state,
                "error": self._materialization_error,
                "snapshot_id": latest.snapshot_id if latest else None,
                "boundary_cursor": latest.boundary_cursor if latest else None,
            }

    def _catalog_paths(
        self,
    ) -> tuple[
        str, Path, Path, str, str, datetime, frozenset[str] | None, str | None
    ]:
        payload, normalized_path, _, _ = self.reader._manifest_binding()
        source = payload.get("catalog_source")
        if not isinstance(source, dict):
            raise PolymarketLiveReadError(
                "discovery_catalog_source_missing",
                "Discovery catalog has no authoritative source binding",
                503,
            )
        raw_path = Path(str(source.get("raw_path") or "")).resolve()
        source_url = str(source.get("source_url") or "")
        observed_at = _raw_instant(source.get("observed_at"))
        if not source_url or observed_at is None:
            raise PolymarketLiveReadError(
                "discovery_catalog_source_invalid",
                "Discovery catalog source identity is incomplete",
                503,
            )
        realtime_universe = payload.get("realtime_universe")
        realtime_market_ids = None
        realtime_universe_id = None
        if isinstance(realtime_universe, dict):
            universe_payload = dict(realtime_universe)
            realtime_universe_id = str(
                universe_payload.pop("universe_id", "")
            )
            for field in (
                "maximum_market_count", "source_market_count",
                "eligible_market_count", "market_count", "token_count",
            ):
                universe_payload[field] = int(universe_payload[field])
            if universe_payload.get("book_backed_market_count") is not None:
                universe_payload["book_backed_market_count"] = int(
                    universe_payload["book_backed_market_count"]
                )
            market_ids = [
                str(value) for value in universe_payload.get("market_ids") or []
            ]
            realtime_market_ids = frozenset(market_ids)
            if (
                universe_payload.get("catalog_revision")
                != payload.get("catalog_revision")
                or len(realtime_market_ids) != len(market_ids)
                or int(universe_payload.get("market_count") or -1)
                != len(realtime_market_ids)
                or content_sha256(universe_payload) != realtime_universe_id
            ):
                raise PolymarketLiveReadError(
                    "discovery_realtime_universe_invalid",
                    "Discovery realtime universe is not bound to its catalog",
                    409,
                )
        return (
            str(payload["catalog_revision"]),
            normalized_path,
            raw_path,
            str(source.get("raw_format") or ""),
            source_url,
            observed_at,
            realtime_market_ids,
            realtime_universe_id,
        )

    @staticmethod
    def _create_schema(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            PRAGMA journal_mode=DELETE;
            PRAGMA synchronous=OFF;
            CREATE TABLE raw_catalog (
                market_id TEXT PRIMARY KEY, payload_json BLOB NOT NULL
            );
            CREATE TABLE relation_sources (
                relation_id TEXT NOT NULL, payload_sha256 TEXT NOT NULL
            );
            CREATE INDEX relation_sources_id ON relation_sources(relation_id);
            CREATE TABLE source_books (
                token_id TEXT PRIMARY KEY, market_id TEXT NOT NULL,
                payload_json BLOB NOT NULL, payload_sha256 TEXT NOT NULL,
                confirmed_exchange_at TEXT, confirmed_received_at TEXT,
                confirmed_state_checksum TEXT, confirmed_source_hash TEXT,
                confirmation_sha256 TEXT
            );
            CREATE TABLE gap_markets (market_id TEXT PRIMARY KEY);
            CREATE TABLE markets (
                market_id TEXT PRIMARY KEY, static_payload BLOB NOT NULL,
                metadata_payload BLOB NOT NULL
            );
            CREATE TABLE tokens (
                token_id TEXT PRIMARY KEY, market_id TEXT NOT NULL,
                outcome TEXT NOT NULL
            );
            CREATE TABLE relation_copies (
                relation_id TEXT PRIMARY KEY, payload_json BLOB NOT NULL,
                copy_count INTEGER NOT NULL, revision_disagreement INTEGER NOT NULL,
                member_disagreement INTEGER NOT NULL
            );
            CREATE TABLE relations (
                relation_id TEXT PRIMARY KEY, payload_json BLOB NOT NULL,
                complete INTEGER NOT NULL
            );
            CREATE TABLE quote_versions (
                market_id TEXT NOT NULL, cursor INTEGER NOT NULL,
                payload_json BLOB NOT NULL, metadata_revision TEXT NOT NULL,
                book_revision TEXT, book_status TEXT NOT NULL,
                PRIMARY KEY (market_id, cursor)
            );
            CREATE INDEX quote_versions_cursor ON quote_versions(cursor);
            CREATE TABLE snapshots (
                snapshot_id TEXT PRIMARY KEY, catalog_revision TEXT NOT NULL,
                boundary_cursor INTEGER NOT NULL, observed_at TEXT NOT NULL,
                active_market_count INTEGER NOT NULL, relation_count INTEGER NOT NULL,
                realtime_universe_id TEXT
            );
            CREATE INDEX snapshots_cursor ON snapshots(boundary_cursor);
            """
        )

    @staticmethod
    def _relation_id_from_raw(raw: dict[str, Any]) -> str | None:
        events = raw.get("events")
        event = events[0] if isinstance(events, list) and events else {}
        group_id = str(
            raw.get("negRiskMarketID")
            or raw.get("neg_risk_market_id")
            or event.get("negRiskMarketID")
            or ""
        )
        if bool(raw.get("negRisk") or raw.get("neg_risk")) and group_id:
            return f"neg-risk:{group_id}"
        return None

    def _metadata_fact(
        self,
        market: LiveMarket,
        raw: dict[str, Any],
        *,
        catalog_revision: str,
        source_url: str,
        catalog_observed_at: datetime,
    ) -> DiscoveryMetadataFact:
        rules_text = raw.get("rules") or raw.get("description")
        description = raw.get("description")
        resolution_source = raw.get("resolutionSource") or raw.get("resolution_source")
        resolution_url = raw.get("resolutionSourceUrl") or raw.get("resolution_source_url")
        proposed_at = _raw_instant(raw.get("resolutionProposedAt") or raw.get("resolution_proposed_at"))
        challenge_at = _raw_instant(raw.get("challengeDeadlineAt") or raw.get("challenge_deadline_at"))
        disputed_at = _raw_instant(raw.get("disputedAt") or raw.get("disputed_at"))
        resolved_at = _raw_instant(raw.get("resolvedAt") or raw.get("resolved_at"))
        redeemable_at = _raw_instant(raw.get("redeemableAt") or raw.get("redeemable_at"))
        market_close_at = _raw_instant(raw.get("closedTime") or raw.get("closedAt") or raw.get("closed_at"))
        events = raw.get("events")
        event = events[0] if isinstance(events, list) and events else {}
        event_start_at = _raw_instant(event.get("startDate") or raw.get("startDate"))
        event_end_at = _raw_instant(event.get("endDate") or raw.get("endDate"))
        redeemable = raw.get("redeemable") if isinstance(raw.get("redeemable"), bool) else None
        missing = [
            name for name, value in {
                "description": description, "rules": rules_text,
                "resolution_source": resolution_source,
                "resolution_source_url": resolution_url,
                "event_start_at": event_start_at, "event_end_at": event_end_at,
                "market_close_at": market_close_at,
                "resolution_status": raw.get("resolutionStatus"),
                "resolution_proposed_at": proposed_at,
                "challenge_deadline_at": challenge_at, "disputed_at": disputed_at,
                "resolved_at": resolved_at, "redeemable": redeemable,
                "redeemable_at": redeemable_at,
            }.items() if value is None
        ]
        rules_revision = (
            content_sha256({"rules": str(rules_text), "source": market.raw_payload_sha256})
            if rules_text is not None else None
        )
        return DiscoveryMetadataFact(
            market_id=market.identity.market_id,
            question=market.question,
            title=market.title,
            description=str(description) if description is not None else None,
            rules=str(rules_text) if rules_text is not None else None,
            rules_revision=rules_revision,
            rules_sha256=(hashlib.sha256(str(rules_text).encode()).hexdigest() if rules_text is not None else None),
            resolution_source=str(resolution_source) if resolution_source is not None else None,
            resolution_source_url=str(resolution_url) if resolution_url is not None else None,
            event_id=market.identity.event_id,
            event_start_at=event_start_at,
            event_end_at=event_end_at,
            market_close_at=market_close_at,
            resolution_status=str(raw["resolutionStatus"]) if raw.get("resolutionStatus") is not None else None,
            resolution=market.resolution,
            resolution_proposed_at=proposed_at,
            challenge_deadline_at=challenge_at,
            disputed_at=disputed_at,
            resolved_at=resolved_at,
            redeemable=redeemable,
            redeemable_at=redeemable_at,
            terminal_at=market.terminal_at,
            observed_at=market.observed_at,
            missing_fields=sorted(missing),
            source_revision=market.metadata_revision,
            source_url=source_url,
            evidence_sha256=content_sha256({
                "catalog_revision": catalog_revision,
                "market_id": market.identity.market_id,
                "raw_payload_sha256": market.raw_payload_sha256,
                "observed_at": catalog_observed_at.isoformat(),
            }),
        )

    def _market_quote(
        self,
        market: LiveMarket,
        books: dict[str, LiveBook | None],
        *,
        relation_complete: bool,
        has_gap: bool,
        catalog_revision: str,
        boundary_cursor: int,
        observed_at: datetime,
    ) -> DiscoveryMarketQuote:
        by_outcome = {item.outcome.casefold(): item for item in market.identity.outcomes}
        has_yes_no = set(by_outcome) == {"yes", "no"}
        ordered_outcomes = (
            [(name, by_outcome[name]) for name in ("yes", "no")]
            if has_yes_no
            else [(item.outcome, item) for item in market.identity.outcomes]
        )
        market_books = [
            (name, outcome, books.get(outcome.token_id))
            for name, outcome in ordered_outcomes
        ]
        outcome_quotes = [
            _outcome_quote(
                name.upper() if has_yes_no else name,
                outcome.token_id,
                book,
                observed_at=observed_at, notionals=self.depth_notionals,
            ) for name, outcome, book in market_books
        ]
        missing: list[str] = []
        status = "ready"
        present = [book for _, _, book in market_books if book is not None]
        if len(present) != 2:
            status = "missing_book"
            missing.extend(
                f"book:{outcome.token_id}" for _, outcome, book in market_books
                if book is None
            )
        elif any(not book.bids or not book.asks for book in present):
            status = "incomplete_book"
            missing.append("two_sided_book")
        elif any(int((observed_at - book.received_at).total_seconds() * 1000) > self.maximum_book_age_ms for book in present):
            status = "stale_book"
            missing.append("fresh_book")
        instrument = market.rules.instrument
        ticks = {book.tick_size for book in present}
        if instrument.price_increment is None:
            status = "missing_tick"
            missing.append("tick_size")
        elif present and (len(ticks) != 1 or instrument.price_increment not in ticks):
            status = "inconsistent_tick"
            missing.append("atomic_tick_revision")
        if instrument.minimum_order_size is None:
            status = "missing_minimum_order_size"
            missing.append("minimum_order_size")
        if not market.rules.fee_schedule.complete:
            status = "missing_fee_schedule"
            missing.extend(f"fee_schedule:{value}" for value in market.rules.fee_schedule.missing_fields)
        relation_id = next((item.relation_id for item in market.relations if item.relation_type == "standard_negative_risk"), None)
        if market.identity.neg_risk and (relation_id is None or not relation_complete):
            status = "incomplete_relation"
            missing.append("complete_negative_risk_relation")
        if has_gap:
            status = "source_gap"
            missing.append("unresolved_source_gap")
        if not has_yes_no:
            status = "missing_outcome_identity"
            missing.extend(("yes_token_id", "no_token_id"))
        book_observed_at = min((book.received_at for book in present), default=None)
        book_age_ms = max(0, int((observed_at - book_observed_at).total_seconds() * 1000)) if book_observed_at else None
        book_revision = content_sha256({book.token_id: book.state_checksum for book in sorted(present, key=lambda value: value.token_id)}) if len(present) == 2 else None
        return DiscoveryMarketQuote(
            market_id=market.identity.market_id,
            condition_id=market.identity.condition_id,
            event_id=market.identity.event_id,
            yes_token_id=by_outcome["yes"].token_id if has_yes_no else None,
            no_token_id=by_outcome["no"].token_id if has_yes_no else None,
            active=market.active,
            closed=market.closed,
            accepting_orders=market.accepting_orders,
            lifecycle_state=market.lifecycle_state,
            start_at=market.start_at,
            end_at=market.end_at,
            negative_risk=market.identity.neg_risk,
            negative_risk_relation_id=relation_id,
            outcomes=outcome_quotes,
            book_observed_at=book_observed_at,
            book_age_ms=book_age_ms,
            book_status=status,
            tick_size=instrument.price_increment,
            minimum_order_size=instrument.minimum_order_size,
            fee_schedule_id=market.rules.fee_schedule.schedule_id if market.rules.fee_schedule.complete else None,
            missing_fields=sorted(set(missing)),
            metadata_revision=market.metadata_revision,
            book_revision=book_revision,
            catalog_revision=catalog_revision,
            cursor=boundary_cursor,
        )

    def _copy_state_boundary(
        self, connection: sqlite3.Connection, catalog_revision: str
    ) -> int:
        with self.reader._state_snapshot() as (_, source, state):
            if state["catalog_revision"] != catalog_revision:
                raise PolymarketLiveReadError(
                    "discovery_cross_revision_boundary",
                    "Discovery catalog and book revisions disagree",
                    409,
                )
            rows = source.execute(
                """SELECT b.token_id, b.market_id, b.payload_json, b.payload_sha256,
                c.exchange_at AS confirmed_exchange_at,
                c.received_at AS confirmed_received_at,
                c.state_checksum AS confirmed_state_checksum,
                c.source_hash AS confirmed_source_hash,
                c.confirmation_sha256
                FROM books b LEFT JOIN book_confirmations c ON c.token_id=b.token_id"""
            )
            while batch := rows.fetchmany(1000):
                connection.executemany(
                    "INSERT INTO source_books VALUES (?,?,?,?,?,?,?,?,?)",
                    [tuple(row) for row in batch],
                )
            gaps = source.execute(
                "SELECT DISTINCT market_id FROM gaps WHERE resolved=0 AND market_id IS NOT NULL"
            )
            while batch := gaps.fetchmany(1000):
                connection.executemany(
                    "INSERT OR IGNORE INTO gap_markets VALUES (?)",
                    [(str(row[0]),) for row in batch],
                )
            return int(state["latest_cursor"])

    @staticmethod
    def _book_from_materialized_row(row: sqlite3.Row | None) -> LiveBook | None:
        if row is None:
            return None
        return PolymarketLiveReadStore._indexed_book(row)

    def _finalize_relations(
        self, connection: sqlite3.Connection, catalog_revision: str
    ) -> int:
        count = 0
        for row in connection.execute("SELECT * FROM relation_copies ORDER BY relation_id"):
            source = json.loads(bytes(row["payload_json"]))
            members = [DiscoveryRelationMember(
                market_id=item["market_id"], condition_id=item["condition_id"],
                yes_token_id=item["yes_token_id"], no_token_id=item["no_token_id"],
                outcome_label=item["outcome_label"], pair_revision=item["pair_revision"],
            ) for item in sorted(source["outcome_pairs"], key=lambda value: value["market_id"])]
            source_hashes = [str(value[0]) for value in connection.execute(
                "SELECT payload_sha256 FROM relation_sources WHERE relation_id=? ORDER BY payload_sha256",
                (row["relation_id"],),
            )]
            expected = len(source_hashes)
            reasons = set(source.get("missing_fields") or [])
            if row["revision_disagreement"]:
                reasons.add("relation_revision_disagreement")
            if row["member_disagreement"]:
                reasons.add("relation_member_disagreement")
            if expected == 0:
                reasons.add("source_relation_group_missing")
            if len(members) != expected:
                reasons.add("member_count_mismatch")
            if expected < 2:
                reasons.add("insufficient_members")
            relation = DiscoveryRelation(
                relation_id=str(row["relation_id"]),
                member_market_ids=[item.market_id for item in members],
                members=members,
                expected_member_count=expected,
                actual_member_count=len(members),
                complete=not reasons and len(members) == expected and expected >= 2,
                valid_from=source["valid_from"], valid_to=source.get("valid_to"),
                relation_revision=source["revision"],
                catalog_revision=catalog_revision,
                provenance=source["provenance"],
                evidence_sha256=content_sha256({
                    "catalog_revision": catalog_revision,
                    "relation_id": row["relation_id"],
                    "revision": source["revision"],
                    "members": [item.model_dump(mode="json") for item in members],
                    "raw_member_payload_sha256": source_hashes,
                }),
                reason_codes=sorted(reasons),
            )
            connection.execute(
                "INSERT INTO relations VALUES (?,?,?)",
                (relation.relation_id, relation.model_dump_json().encode(), int(relation.complete)),
            )
            count += 1
        return count

    def _full_materialize(self) -> _DiscoveryBoundary:
        (
            catalog_revision, normalized_path, raw_path, raw_format,
            source_url, catalog_observed_at, realtime_market_ids,
            realtime_universe_id,
        ) = self._catalog_paths()
        for stale in self.materialization_root.glob(".building-*.sqlite3*"):
            stale.unlink(missing_ok=True)
        staging = self.materialization_root / f".building-{uuid.uuid4().hex}.sqlite3"
        connection = self._open_database(staging)
        try:
            self._create_schema(connection)
            boundary_cursor = self._copy_state_boundary(connection, catalog_revision)
            for _, raw in _iter_raw_catalog_rows(raw_path, raw_format):
                market_id = str(raw.get("id") or "")
                if not market_id:
                    continue
                if relation_id := self._relation_id_from_raw(raw):
                    connection.execute(
                        "INSERT INTO relation_sources VALUES (?,?)",
                        (relation_id, content_sha256(raw)),
                    )
                if (
                    realtime_market_ids is not None
                    and market_id not in realtime_market_ids
                ):
                    continue
                raw_body = json.dumps(
                    raw, sort_keys=True, separators=(",", ":")
                ).encode()
                connection.execute(
                    "INSERT OR REPLACE INTO raw_catalog VALUES (?,?)",
                    (market_id, raw_body),
                )
            connection.commit()
            observed_at = self.reader.now_provider()
            active_count = 0
            with normalized_path.open("rb") as stream:
                for line_number, line in enumerate(stream, 1):
                    body = line.removesuffix(b"\n")
                    if not body:
                        continue
                    market = LiveMarket.model_validate_json(body)
                    if (
                        realtime_market_ids is not None
                        and market.identity.market_id not in realtime_market_ids
                    ):
                        continue
                    if not market.active or market.closed:
                        continue
                    raw_row = connection.execute(
                        "SELECT payload_json FROM raw_catalog WHERE market_id=?",
                        (market.identity.market_id,),
                    ).fetchone()
                    if raw_row is None:
                        raise PolymarketLiveReadError(
                            "discovery_raw_evidence_missing",
                            "Discovery metadata lacks its raw source row",
                            409,
                        )
                    raw = json.loads(bytes(raw_row[0]))
                    metadata = self._metadata_fact(
                        market, raw, catalog_revision=catalog_revision,
                        source_url=source_url,
                        catalog_observed_at=catalog_observed_at,
                    )
                    connection.execute(
                        "INSERT INTO markets VALUES (?,?,?)",
                        (market.identity.market_id, body, metadata.model_dump_json().encode()),
                    )
                    for outcome in market.identity.outcomes:
                        connection.execute(
                            "INSERT INTO tokens VALUES (?,?,?)",
                            (outcome.token_id, market.identity.market_id, outcome.outcome.upper()),
                        )
                    for relation in market.relations:
                        if relation.relation_type != "standard_negative_risk":
                            continue
                        relation_body = relation.model_dump_json().encode()
                        existing = connection.execute(
                            "SELECT payload_json, copy_count FROM relation_copies WHERE relation_id=?",
                            (relation.relation_id,),
                        ).fetchone()
                        if existing is None:
                            connection.execute(
                                "INSERT INTO relation_copies VALUES (?,?,1,0,0)",
                                (relation.relation_id, relation_body),
                            )
                        else:
                            first = json.loads(bytes(existing["payload_json"]))
                            current = relation.model_dump(mode="json")
                            connection.execute(
                                "UPDATE relation_copies SET copy_count=copy_count+1, "
                                "revision_disagreement=revision_disagreement OR ?, "
                                "member_disagreement=member_disagreement OR ? WHERE relation_id=?",
                                (int(first["revision"] != current["revision"]), int(first["outcome_pairs"] != current["outcome_pairs"]), relation.relation_id),
                            )
                    books = {}
                    for outcome in market.identity.outcomes:
                        book_row = connection.execute(
                            "SELECT * FROM source_books WHERE token_id=?",
                            (outcome.token_id,),
                        ).fetchone()
                        books[outcome.token_id] = self._book_from_materialized_row(
                            book_row
                        )
                    relation_id = next((
                        item.relation_id for item in market.relations
                        if item.relation_type == "standard_negative_risk"
                    ), None)
                    has_gap = connection.execute(
                        "SELECT 1 FROM gap_markets WHERE market_id=?",
                        (market.identity.market_id,),
                    ).fetchone() is not None
                    quote = self._market_quote(
                        market,
                        books,
                        relation_complete=(
                            not market.identity.neg_risk or relation_id is not None
                        ),
                        has_gap=has_gap,
                        catalog_revision=catalog_revision,
                        boundary_cursor=boundary_cursor,
                        observed_at=observed_at,
                    )
                    connection.execute(
                        "INSERT INTO quote_versions VALUES (?,?,?,?,?,?)",
                        (
                            quote.market_id, boundary_cursor,
                            quote.model_dump_json().encode(),
                            quote.metadata_revision, quote.book_revision,
                            quote.book_status,
                        ),
                    )
                    active_count += 1
                    if line_number % 500 == 0:
                        connection.commit()
            relation_count = self._finalize_relations(connection, catalog_revision)
            for relation_row in connection.execute(
                "SELECT payload_json FROM relations WHERE complete=0"
            ):
                relation = DiscoveryRelation.model_validate_json(
                    bytes(relation_row[0])
                )
                for market_id in relation.member_market_ids:
                    quote_row = connection.execute(
                        "SELECT payload_json FROM quote_versions WHERE market_id=? AND cursor=?",
                        (market_id, boundary_cursor),
                    ).fetchone()
                    if quote_row is None:
                        continue
                    quote = DiscoveryMarketQuote.model_validate_json(
                        bytes(quote_row[0])
                    )
                    missing_fields = sorted(set(
                        quote.missing_fields + ["complete_negative_risk_relation"]
                    ))
                    quote = quote.model_copy(update={
                        "book_status": "incomplete_relation",
                        "missing_fields": missing_fields,
                    })
                    connection.execute(
                        "UPDATE quote_versions SET payload_json=?, book_status=? "
                        "WHERE market_id=? AND cursor=?",
                        (
                            quote.model_dump_json().encode(),
                            quote.book_status, market_id, boundary_cursor,
                        ),
                    )
            connection.commit()
            digest = hashlib.sha256()
            digest.update(DISCOVERY_SCHEMA_VERSION.encode())
            digest.update(catalog_revision.encode())
            digest.update((realtime_universe_id or "").encode())
            digest.update(str(boundary_cursor).encode())
            digest.update(json.dumps(self.depth_notionals).encode())
            for row in connection.execute(
                "SELECT market_id, metadata_revision, book_revision, book_status FROM quote_versions ORDER BY market_id"
            ):
                digest.update(json.dumps(tuple(row), separators=(",", ":")).encode())
            snapshot_id = digest.hexdigest()
            connection.execute(
                "INSERT INTO snapshots VALUES (?,?,?,?,?,?,?)",
                (
                    snapshot_id, catalog_revision, boundary_cursor,
                    observed_at.isoformat(), active_count, relation_count,
                    realtime_universe_id,
                ),
            )
            connection.executescript(
                "DROP TABLE raw_catalog; DROP TABLE relation_sources; "
                "DROP TABLE source_books; DROP TABLE gap_markets; DROP TABLE relation_copies;"
            )
            connection.commit()
        except BaseException:
            connection.close()
            staging.unlink(missing_ok=True)
            raise
        connection.close()
        database_path = self.materialization_root / (
            f"catalog-{catalog_revision}-{uuid.uuid4().hex}.sqlite3"
        )
        os.replace(staging, database_path)
        with self._database(database_path) as published:
            published.execute("PRAGMA journal_mode=WAL")
            published.execute("PRAGMA synchronous=NORMAL")
        return _DiscoveryBoundary(
            snapshot_id=snapshot_id, catalog_revision=catalog_revision,
            boundary_cursor=boundary_cursor, observed_at=observed_at,
            database_path=database_path, active_market_count=active_count,
            relation_count=relation_count,
            realtime_universe_id=realtime_universe_id,
        )

    def _incremental_materialize(self, current: _DiscoveryBoundary) -> _DiscoveryBoundary | None:
        observed_at = self.reader.now_provider()
        current_realtime_universe_id = self._catalog_paths()[-1]
        if current_realtime_universe_id != current.realtime_universe_id:
            return self._full_materialize()
        connection = self._open_database(current.database_path)
        requires_full_materialization = False
        changed_market_ids: set[str] = set()
        try:
            connection.execute("BEGIN IMMEDIATE")
            with self.reader._state_snapshot() as (_, source, state):
                catalog_revision = str(state["catalog_revision"])
                boundary_cursor = int(state["latest_cursor"])
                if catalog_revision != current.catalog_revision:
                    requires_full_materialization = True
                elif boundary_cursor <= current.boundary_cursor:
                    connection.rollback()
                    return None
                else:
                    earliest = source.execute(
                        "SELECT MIN(cursor) FROM event_offsets"
                    ).fetchone()[0]
                    if (
                        earliest is not None
                        and current.boundary_cursor < int(earliest) - 1
                    ):
                        requires_full_materialization = True
                    else:
                        event_rows = source.execute(
                            """SELECT cursor, byte_offset, byte_length, line_sha256
                            FROM event_offsets WHERE cursor>? AND cursor<=?
                            ORDER BY cursor""",
                            (current.boundary_cursor, boundary_cursor),
                        )
                        with self.reader.event_path.open("rb") as stream:
                            for event_row in event_rows:
                                stream.seek(int(event_row["byte_offset"]))
                                line = stream.read(int(event_row["byte_length"]))
                                if hashlib.sha256(line).hexdigest() != event_row["line_sha256"]:
                                    raise PolymarketLiveReadError(
                                        "polymarket_state_integrity_failed",
                                        "Event hash mismatch",
                                        409,
                                    )
                                event = LiveEventEnvelope.model_validate_json(line)
                                if event.event_type in {
                                    "catalog_revision", "market_terminal", "market_resolved"
                                }:
                                    requires_full_materialization = True
                                elif (
                                    event.market_id is not None
                                    and event.event_type in {
                                        "book", "price_change", "best_bid_ask",
                                        "last_trade_price", "tick_size_change",
                                    }
                                ):
                                    changed_market_ids.add(event.market_id)
                    if not requires_full_materialization:
                        for market_id in sorted(changed_market_ids):
                            market_row = connection.execute(
                                "SELECT static_payload FROM markets WHERE market_id=?",
                                (market_id,),
                            ).fetchone()
                            if market_row is None:
                                continue
                            market = LiveMarket.model_validate_json(
                                bytes(market_row[0])
                            )
                            books = {}
                            for outcome in market.identity.outcomes:
                                row = source.execute(
                                    """SELECT b.token_id, b.market_id,
                                    b.payload_json, b.payload_sha256,
                                    c.exchange_at AS confirmed_exchange_at,
                                    c.received_at AS confirmed_received_at,
                                    c.state_checksum AS confirmed_state_checksum,
                                    c.source_hash AS confirmed_source_hash,
                                    c.confirmation_sha256 FROM books b
                                    LEFT JOIN book_confirmations c
                                    ON c.token_id=b.token_id WHERE b.token_id=?""",
                                    (outcome.token_id,),
                                ).fetchone()
                                books[outcome.token_id] = (
                                    self.reader._indexed_book(row) if row else None
                                )
                            relation_id = next((
                                item.relation_id for item in market.relations
                                if item.relation_type == "standard_negative_risk"
                            ), None)
                            relation_row = (
                                connection.execute(
                                    "SELECT complete FROM relations WHERE relation_id=?",
                                    (relation_id,),
                                ).fetchone()
                                if relation_id else None
                            )
                            relation_complete = (
                                not market.identity.neg_risk
                                or bool(relation_row and relation_row[0])
                            )
                            has_gap = source.execute(
                                "SELECT 1 FROM gaps WHERE resolved=0 AND market_id=? LIMIT 1",
                                (market_id,),
                            ).fetchone() is not None
                            quote = self._market_quote(
                                market, books,
                                relation_complete=relation_complete,
                                has_gap=has_gap,
                                catalog_revision=catalog_revision,
                                boundary_cursor=boundary_cursor,
                                observed_at=observed_at,
                            )
                            connection.execute(
                                "INSERT INTO quote_versions VALUES (?,?,?,?,?,?)",
                                (
                                    market_id, boundary_cursor,
                                    quote.model_dump_json().encode(),
                                    quote.metadata_revision, quote.book_revision,
                                    quote.book_status,
                                ),
                            )
            if requires_full_materialization:
                connection.rollback()
                return self._full_materialize()
            snapshot_id = content_sha256({
                "schema_version": DISCOVERY_SCHEMA_VERSION,
                "catalog_revision": catalog_revision,
                "realtime_universe_id": current.realtime_universe_id,
                "boundary_cursor": boundary_cursor,
                "previous_snapshot_id": current.snapshot_id,
                "changed_market_ids": sorted(changed_market_ids),
            })
            connection.execute(
                "INSERT INTO snapshots VALUES (?,?,?,?,?,?,?)",
                (
                    snapshot_id, catalog_revision, boundary_cursor,
                    observed_at.isoformat(), current.active_market_count,
                    current.relation_count, current.realtime_universe_id,
                ),
            )
            connection.commit()
        finally:
            connection.close()
        return _DiscoveryBoundary(
            snapshot_id=snapshot_id, catalog_revision=catalog_revision,
            boundary_cursor=boundary_cursor, observed_at=observed_at,
            database_path=current.database_path,
            active_market_count=current.active_market_count,
            relation_count=current.relation_count,
            realtime_universe_id=current.realtime_universe_id,
        )

    def _publish(self, boundary: _DiscoveryBoundary) -> None:
        with self._lock:
            previous_database_paths = {
                item.database_path for item in self._snapshots.values()
            }
            previous = next(reversed(self._snapshots.values()), None)
            if (
                previous is not None
                and previous.realtime_universe_id
                != boundary.realtime_universe_id
            ):
                self._snapshots.clear()
            self._snapshots[boundary.snapshot_id] = boundary
            self._snapshots.move_to_end(boundary.snapshot_id)
            while len(self._snapshots) > self.retained_snapshots:
                self._snapshots.popitem(last=False)
            retained = list(self._snapshots)
            retained_database_paths = {
                item.database_path for item in self._snapshots.values()
            }
            self._materialization_state = "ready"
            self._materialization_error = None
            self._publish_count += 1
            publish_count = self._publish_count
        with self._database(boundary.database_path) as connection:
            placeholders = ",".join("?" for _ in retained)
            connection.execute(
                f"DELETE FROM snapshots WHERE snapshot_id NOT IN ({placeholders})",
                retained,
            )
            if publish_count % self.retained_snapshots == 0:
                floor_row = connection.execute(
                    "SELECT MIN(boundary_cursor) FROM snapshots"
                ).fetchone()
                if floor_row is not None and floor_row[0] is not None:
                    floor = int(floor_row[0])
                    connection.executescript(
                        "DROP TABLE IF EXISTS temp.retained_quote_floor;"
                        "CREATE TEMP TABLE retained_quote_floor AS "
                        "SELECT market_id, MAX(cursor) AS cursor FROM quote_versions "
                        f"WHERE cursor<={floor} GROUP BY market_id;"
                        "CREATE INDEX retained_quote_floor_market "
                        "ON retained_quote_floor(market_id);"
                    )
                    connection.execute(
                        "DELETE FROM quote_versions AS q WHERE q.cursor<? AND NOT EXISTS ("
                        "SELECT 1 FROM retained_quote_floor k WHERE k.market_id=q.market_id "
                        "AND k.cursor=q.cursor)",
                        (floor,),
                    )
            connection.commit()
        manifest = {
            "schema_version": "marketcow.polymarket.discovery-materialization.v2",
            "database_path": str(boundary.database_path.resolve()),
            "snapshot_id": boundary.snapshot_id,
            "catalog_revision": boundary.catalog_revision,
            "boundary_cursor": boundary.boundary_cursor,
            "realtime_universe_id": boundary.realtime_universe_id,
        }
        temporary = self.current_manifest_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(manifest, sort_keys=True))
        os.replace(temporary, self.current_manifest_path)
        for database_path in previous_database_paths - retained_database_paths:
            database_path.unlink(missing_ok=True)
            Path(str(database_path) + "-wal").unlink(missing_ok=True)
            Path(str(database_path) + "-shm").unlink(missing_ok=True)

    def materialize_once(self) -> _DiscoveryBoundary | None:
        if not self._build_lock.acquire(blocking=False):
            return None
        process_lock = self.materialization_lock_path.open("a+b")
        try:
            try:
                fcntl.flock(process_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self._restore_published()
                return None
            self._restore_published()
            with self._lock:
                self._materialization_state = "building"
                current = next(reversed(self._snapshots.values()), None)
            boundary = (
                self._full_materialize()
                if current is None
                else self._incremental_materialize(current)
            )
            if boundary is not None:
                self._publish(boundary)
            else:
                with self._lock:
                    self._materialization_state = "ready"
            return boundary
        finally:
            try:
                fcntl.flock(process_lock, fcntl.LOCK_UN)
            finally:
                process_lock.close()
            self._build_lock.release()

    def capture(self) -> _DiscoveryBoundary:
        boundary = self.materialize_once()
        if boundary is not None:
            return boundary
        with self._lock:
            current = next(reversed(self._snapshots.values()), None)
        if current is None:
            raise PolymarketLiveReadError(
                "discovery_snapshot_materializing",
                "Discovery snapshot is being materialized",
                503,
            )
        return current

    def boundary(self, snapshot_id: str | None) -> _DiscoveryBoundary:
        with self._lock:
            boundary = (
                self._snapshots.get(snapshot_id)
                if snapshot_id is not None
                else next(reversed(self._snapshots.values()), None)
            )
        current_realtime_universe_id = self._catalog_paths()[-1]
        if (
            boundary is not None
            and boundary.realtime_universe_id != current_realtime_universe_id
        ):
            self.start_background_materialization()
            boundary = None
        if boundary is None:
            self.start_background_materialization()
            code = "discovery_snapshot_expired" if snapshot_id else "discovery_snapshot_materializing"
            status = 410 if snapshot_id else 503
            raise PolymarketLiveReadError(
                code,
                "Discovery snapshot is not published; retry from the latest snapshot",
                status,
            )
        return boundary

    @staticmethod
    def _quote_query() -> str:
        return (
            "SELECT q.payload_json FROM markets m JOIN quote_versions q "
            "ON q.market_id=m.market_id WHERE q.cursor=(SELECT MAX(q2.cursor) "
            "FROM quote_versions q2 WHERE q2.market_id=m.market_id AND q2.cursor<=?) "
            "ORDER BY m.market_id LIMIT ? OFFSET ?"
        )

    def snapshot_page(self, *, snapshot_id: str | None, page_cursor: str | None, page_size: int) -> DiscoverySnapshotPage:
        if not 1 <= page_size <= 1000:
            raise PolymarketLiveReadError("discovery_page_size_invalid", "page_size must be in [1, 1000]", 422)
        boundary = self.boundary(snapshot_id)
        offset = self._page_offset(boundary.snapshot_id, page_cursor)
        with self._database(boundary.database_path, readonly=True) as connection:
            rows = list(connection.execute(self._quote_query(), (boundary.boundary_cursor, page_size, offset)))
        items = [
            DiscoveryMarketQuote.model_validate_json(bytes(row[0])).model_copy(
                update={"cursor": boundary.boundary_cursor}
            )
            for row in rows
        ]
        next_offset = offset + len(items)
        return DiscoverySnapshotPage(
            snapshot_id=boundary.snapshot_id,
            catalog_revision=boundary.catalog_revision,
            boundary_cursor=boundary.boundary_cursor,
            observed_at=boundary.observed_at,
            depth_notionals=list(self.depth_notionals),
            active_market_count=boundary.active_market_count,
            page_size=page_size,
            page_count=len(items),
            next_page_cursor=f"{boundary.snapshot_id}:{next_offset}" if next_offset < boundary.active_market_count else None,
            items=items,
            relation_count=boundary.relation_count,
        )

    def metadata_page(self, *, snapshot_id: str, page_cursor: str | None, page_size: int) -> DiscoveryMetadataPage:
        boundary = self.boundary(snapshot_id)
        offset = self._page_offset(boundary.snapshot_id, page_cursor)
        with self._database(boundary.database_path, readonly=True) as connection:
            rows = list(connection.execute(
                "SELECT metadata_payload FROM markets ORDER BY market_id LIMIT ? OFFSET ?",
                (page_size, offset),
            ))
        items = [DiscoveryMetadataFact.model_validate_json(bytes(row[0])) for row in rows]
        next_offset = offset + len(items)
        return DiscoveryMetadataPage(
            snapshot_id=boundary.snapshot_id,
            catalog_revision=boundary.catalog_revision,
            page_size=page_size,
            next_page_cursor=f"{boundary.snapshot_id}:{next_offset}" if next_offset < boundary.active_market_count else None,
            items=items,
        )

    def relation(self, relation_id: str, snapshot_id: str) -> DiscoveryRelation:
        boundary = self.boundary(snapshot_id)
        with self._database(boundary.database_path, readonly=True) as connection:
            row = connection.execute(
                "SELECT payload_json FROM relations WHERE relation_id=?", (relation_id,)
            ).fetchone()
            if row is None:
                raise PolymarketLiveReadError("discovery_relation_not_found", "Relation is not in this snapshot", 404)
            relation = DiscoveryRelation.model_validate_json(bytes(row[0]))
            quotes = []
            for market_id in relation.member_market_ids:
                quote_row = connection.execute(
                    "SELECT payload_json FROM quote_versions WHERE market_id=? AND cursor<=? ORDER BY cursor DESC LIMIT 1",
                    (market_id, boundary.boundary_cursor),
                ).fetchone()
                if quote_row is not None:
                    quotes.append(
                        DiscoveryMarketQuote.model_validate_json(
                            bytes(quote_row[0])
                        ).model_copy(update={"cursor": boundary.boundary_cursor})
                    )
        return relation.model_copy(update={"quotes": quotes})

    def _outcomes_for_tokens(self, catalog_revision: str, token_ids: set[str]) -> dict[str, str]:
        boundary = self.boundary(None)
        if boundary.catalog_revision != catalog_revision:
            raise PolymarketLiveReadError(
                "discovery_cross_revision_boundary",
                "Discovery events require a new atomic snapshot",
                409,
            )
        if not token_ids:
            return {}
        placeholders = ",".join("?" for _ in token_ids)
        with self._database(boundary.database_path, readonly=True) as connection:
            return {str(row[0]): str(row[1]) for row in connection.execute(
                f"SELECT token_id, outcome FROM tokens WHERE token_id IN ({placeholders})",
                sorted(token_ids),
            )}

    def events_page(self, after_cursor: int, limit: int) -> DiscoveryEventPage:
        events, boundary_cursor, has_more, catalog_revision = self._read_events(after_cursor, limit)
        resync_required = any(event.event_type in {"catalog_revision", "market_terminal", "market_resolved"} for event in events)
        token_outcomes = {} if resync_required else self._outcomes_for_tokens(
            catalog_revision, {event.token_id for event in events if event.token_id}
        )
        items: list[DiscoveryEvent] = []
        for event in events:
            if event.event_type == "catalog_revision":
                changes = event.canonical_payload.get("relation_changes") or {}
                relation_ids = sorted(set((changes.get("added_relation_ids") or []) + (changes.get("removed_relation_ids") or []) + (changes.get("changed_relation_ids") or []))) or [None]
                for relation_id in relation_ids:
                    items.append(DiscoveryEvent(
                        cursor=event.cursor,
                        event_id=content_sha256({"source_event_id": event.event_id, "relation_id": relation_id}) if relation_id else event.event_id,
                        event_type="relation_changed", market_id=None, token_id=None,
                        relation_id=relation_id, catalog_revision=catalog_revision,
                        observed_at=event.received_at, quote=None,
                        book_status="resync_required", missing_fields=["atomic_catalog_resync"],
                        raw_payload_sha256=event.raw_payload_sha256,
                    ))
                continue
            if event.event_type in {"market_terminal", "market_resolved"}:
                items.append(DiscoveryEvent(
                    cursor=event.cursor, event_id=event.event_id,
                    event_type="market_lifecycle_changed", market_id=event.market_id,
                    token_id=event.token_id, relation_id=None,
                    catalog_revision=catalog_revision, observed_at=event.received_at,
                    quote=None, book_status="resync_required",
                    missing_fields=["atomic_catalog_resync"],
                    raw_payload_sha256=event.raw_payload_sha256,
                ))
                continue
            if resync_required or event.event_type not in {"book", "price_change", "best_bid_ask", "last_trade_price", "tick_size_change"}:
                continue
            quote = None
            if event.applied and event.token_id and event.token_id in token_outcomes:
                try:
                    quote = _outcome_quote(
                        token_outcomes[event.token_id], event.token_id,
                        LiveBook.model_validate(event.canonical_payload),
                        observed_at=event.received_at, notionals=self.depth_notionals,
                    )
                except Exception:
                    quote = None
            fail_closed = not event.applied or quote is None
            items.append(DiscoveryEvent(
                cursor=event.cursor, event_id=event.event_id,
                event_type="book_fail_closed" if fail_closed else "quote_changed",
                market_id=event.market_id, token_id=event.token_id, relation_id=None,
                catalog_revision=catalog_revision, observed_at=event.received_at,
                quote=quote, book_status="source_gap" if fail_closed else "ready",
                missing_fields=[event.fail_closed_reason or "authoritative_book_unavailable"] if fail_closed else [],
                raw_payload_sha256=event.raw_payload_sha256,
            ))
        next_cursor = events[-1].cursor if events else after_cursor
        return DiscoveryEventPage(
            catalog_revision=catalog_revision, after_cursor=after_cursor,
            next_cursor=next_cursor, boundary_cursor=boundary_cursor,
            has_more=has_more, resync_required=resync_required, items=items,
        )
