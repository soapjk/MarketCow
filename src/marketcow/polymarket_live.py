from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable, Literal

import requests
import websockets
from pydantic import BaseModel, Field, model_validator

from .polymarket_contracts import (
    CONTRACT_VERSION,
    GapEntry,
    OutcomeToken,
    PredictionMarketIdentity,
    canonical_json,
    content_sha256,
    decimal_text,
)
from .polymarket_history import _validate_book
from .polymarket_sources import _atomic_write, utc_now


LIVE_SCHEMA_VERSION = "marketcow.polymarket.live.v1"
PUBLIC_DATA_KINDS = frozenset({"trades", "activity", "positions", "holders"})


def _instant(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, str)) and str(value).isdigit():
        number = int(value)
        if number > 10**12:
            number /= 1000
        parsed = datetime.fromtimestamp(number, timezone.utc)
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _list(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = [part.strip() for part in value.split(",")]
    return [str(item) for item in (value or [])]


def _bool(value: Any) -> bool:
    if isinstance(value, str):
        if value.lower() not in {"true", "false"}:
            raise ValueError("boolean source field must be true or false")
        return value.lower() == "true"
    return bool(value)


def _levels(value: Any, field: str) -> list[dict[str, str]]:
    levels = []
    for item in value or []:
        levels.append({
            "price": decimal_text(item.get("price"), f"{field}.price"),
            "size": decimal_text(item.get("size"), f"{field}.size"),
        })
    return levels


def _state_checksum(token_id: str, tick_size: str, bids: dict[str, str], asks: dict[str, str]) -> str:
    return content_sha256({
        "token_id": token_id,
        "tick_size": tick_size,
        "bids": [
            {"price": price, "size": bids[price]}
            for price in sorted(bids, key=Decimal, reverse=True)
        ],
        "asks": [
            {"price": price, "size": asks[price]}
            for price in sorted(asks, key=Decimal)
        ],
    })


class LiveRuleSet(BaseModel):
    tick_size: str
    minimum_order_size: str
    maker_fee_bps: str | None = None
    taker_fee_bps: str | None = None
    fee_rate: str | None = None
    fee_exponent: str | None = None
    fee_rebate_rate: str | None = None
    fee_taker_only: bool | None = None
    fee_rounding_mode: Literal["UNSPECIFIED"] = "UNSPECIFIED"
    fee_calculation_status: Literal["informational_only"] = "informational_only"
    fee_complete: bool
    rules_complete: bool

    @model_validator(mode="after")
    def decimals(self):
        self.tick_size = decimal_text(self.tick_size, "tick_size", allow_zero=False)
        self.minimum_order_size = decimal_text(
            self.minimum_order_size, "minimum_order_size", allow_zero=False
        )
        if self.maker_fee_bps is not None:
            self.maker_fee_bps = decimal_text(self.maker_fee_bps, "maker_fee_bps")
        if self.taker_fee_bps is not None:
            self.taker_fee_bps = decimal_text(self.taker_fee_bps, "taker_fee_bps")
        if self.fee_rate is not None:
            self.fee_rate = decimal_text(self.fee_rate, "fee_rate")
        if self.fee_exponent is not None:
            self.fee_exponent = decimal_text(self.fee_exponent, "fee_exponent")
        if self.fee_rebate_rate is not None:
            self.fee_rebate_rate = decimal_text(
                self.fee_rebate_rate, "fee_rebate_rate"
            )
        return self


class LiveRelation(BaseModel):
    relation_id: str
    relation_type: Literal["binary_complements", "standard_negative_risk"]
    members: list[str] = Field(min_length=2)
    convertible: bool
    revision: str
    valid_from: datetime
    source: Literal["polymarket_gamma"] = "polymarket_gamma"


class LiveMarket(BaseModel):
    identity: PredictionMarketIdentity
    question: str
    title: str
    active: bool
    closed: bool
    accepting_orders: bool
    lifecycle_state: Literal["active", "closed", "resolved", "invalid"]
    resolution: str | None = None
    start_at: datetime | None = None
    end_at: datetime | None = None
    metadata_revision: str
    observed_at: datetime
    rules: LiveRuleSet
    relations: list[LiveRelation]
    raw_payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def fail_closed_metadata(self):
        if len(self.identity.outcomes) != 2:
            raise ValueError("live binary market requires two outcome tokens")
        if self.identity.neg_risk and not any(
            item.relation_type == "standard_negative_risk" for item in self.relations
        ):
            raise ValueError("negative-risk market requires source-backed relation")
        return self


class LiveBook(BaseModel):
    token_id: str
    condition_id: str
    book_epoch: str
    sequence: int = Field(ge=1)
    sequence_semantics: Literal["deterministic_normalized"] = "deterministic_normalized"
    exchange_at: datetime
    received_at: datetime
    tick_version: str
    tick_size: str
    bids: list[dict[str, str]]
    asks: list[dict[str, str]]
    last_trade_price: str | None = None
    state_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_hash: str | None = None


class LiveEventEnvelope(BaseModel):
    contract_version: Literal["marketcow.prediction_market.v1"] = CONTRACT_VERSION
    schema_version: Literal["marketcow.polymarket.live.v1"] = LIVE_SCHEMA_VERSION
    cursor: int = Field(ge=1)
    event_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    event_type: Literal[
        "book", "price_change", "best_bid_ask", "last_trade_price",
        "tick_size_change", "new_market", "market_resolved", "catalog_revision",
        "recovery_started", "recovery_completed", "subscription_change",
    ]
    market_id: str | None = None
    condition_id: str | None = None
    token_id: str | None = None
    book_epoch: str | None = None
    sequence: int | None = None
    exchange_at: datetime
    received_at: datetime
    canonical_payload: dict[str, Any]
    canonical_payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    raw_payload: dict[str, Any]
    raw_payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    applied: bool
    fail_closed_reason: str | None = None


class LiveCheckpoint(BaseModel):
    schema_version: Literal["marketcow.polymarket.live-checkpoint.v1"] = (
        "marketcow.polymarket.live-checkpoint.v1"
    )
    cursor: int = Field(ge=0)
    catalog_revision: str | None = None
    created_at: datetime
    books: dict[str, LiveBook]
    unresolved_gaps: list[GapEntry]
    state_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class MarketFrame(BaseModel):
    market_id: str
    condition_id: str
    frame_at: datetime
    cursor: int
    status: Literal["ready", "fail_closed"]
    reason_codes: list[str]
    tokens: list[LiveBook]
    relation_ids: list[str]
    relation_tokens: list[LiveBook]


class LiveHealth(BaseModel):
    status: Literal["ready", "degraded", "empty"]
    catalog_revision: str | None
    active_market_count: int
    subscribed_token_count: int
    book_token_count: int
    ready_market_count: int
    unresolved_gap_count: int
    latest_cursor: int
    latest_received_at: datetime | None
    lag_ms: int | None
    source_policy: Literal["official_free_only"] = "official_free_only"


class LiveBootstrapResponse(BaseModel):
    contract_version: Literal["marketcow.prediction_market.v1"] = CONTRACT_VERSION
    schema_version: Literal["marketcow.polymarket.live-bootstrap.v1"] = (
        "marketcow.polymarket.live-bootstrap.v1"
    )
    catalog_revision: str | None
    catalog_source: dict[str, Any] | None
    cursor: int = Field(ge=0)
    markets: list[LiveMarket]
    active_token_ids: list[str]
    sequence_semantics: Literal["deterministic_normalized"]
    recovery: dict[str, str]
    source_policy: Literal["official_free_only"]


class LiveSnapshotPage(BaseModel):
    schema_version: Literal["marketcow.polymarket.live-snapshot.v1"] = (
        "marketcow.polymarket.live-snapshot.v1"
    )
    catalog_revision: str | None
    cursor: int = Field(ge=0)
    count: int = Field(ge=0)
    items: list[MarketFrame]


class LiveEventPage(BaseModel):
    schema_version: Literal["marketcow.polymarket.live-events.v1"] = (
        "marketcow.polymarket.live-events.v1"
    )
    after_cursor: int = Field(ge=0)
    next_cursor: int = Field(ge=0)
    has_more: bool
    items: list[LiveEventEnvelope]


class LiveGapPage(BaseModel):
    schema_version: Literal["marketcow.polymarket.live-gaps.v1"] = (
        "marketcow.polymarket.live-gaps.v1"
    )
    count: int = Field(ge=0)
    items: list[GapEntry]


class PublicDataPage(BaseModel):
    schema_version: Literal["marketcow.polymarket.public-data-page.v1"] = (
        "marketcow.polymarket.public-data-page.v1"
    )
    kind: Literal["trades", "activity", "positions", "holders"]
    count: int = Field(ge=0)
    items: list[dict[str, Any]]


class GammaKeysetCatalog:
    """Complete active-market discovery using Gamma's keyset endpoint."""

    endpoint = "https://gamma-api.polymarket.com/markets/keyset"

    def __init__(
        self,
        *,
        requester: Callable[..., Any] = requests.get,
        timeout: float = 20,
        page_limit: int = 100,
        max_pages: int = 1000,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.requester = requester
        self.timeout = timeout
        self.page_limit = min(100, max(1, page_limit))
        self.max_pages = max(1, max_pages)
        self.sleeper = sleeper

    def fetch_all(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        seen: set[str] = set()
        attempts = 0
        for page_number in range(1, self.max_pages + 1):
            params: dict[str, Any] = {
                "limit": self.page_limit, "closed": "false", "ascending": "true",
            }
            if cursor:
                params["after_cursor"] = cursor
            while True:
                response = self.requester(
                    self.endpoint, params=params, timeout=self.timeout,
                    headers={"Accept": "application/json", "User-Agent": "MarketCow/0.2"},
                )
                if response.status_code != 429 and response.status_code < 500:
                    break
                attempts += 1
                if attempts > 5:
                    response.raise_for_status()
                retry_after = min(8.0, float(response.headers.get("Retry-After") or 2 ** (attempts - 1)))
                self.sleeper(retry_after)
            response.raise_for_status()
            payload = json.loads(response.text, parse_float=str, parse_int=str)
            page = payload.get("markets") or []
            if not isinstance(page, list):
                raise RuntimeError("Gamma keyset response markets must be a list")
            rows.extend(page)
            next_cursor = payload.get("next_cursor")
            if not next_cursor:
                return rows, {
                    "pages": page_number, "market_count": len(rows),
                    "complete": True, "last_cursor": cursor,
                }
            next_cursor = str(next_cursor)
            if next_cursor in seen:
                raise RuntimeError("Gamma keyset cursor loop detected")
            seen.add(next_cursor)
            cursor = next_cursor
        raise RuntimeError("Gamma keyset pagination exceeded max_pages")


class GammaLiveNormalizer:
    """Provider boundary for live catalog metadata and explicit relations."""

    @staticmethod
    def normalize(rows: list[dict[str, Any]], observed_at: datetime) -> list[LiveMarket]:
        result = []
        for row in rows:
            tokens = _list(row.get("clobTokenIds") or row.get("clob_token_ids"))
            outcomes = _list(row.get("outcomes"))
            if len(tokens) != 2 or len(outcomes) != 2:
                continue
            condition_id = str(row.get("conditionId") or row.get("condition_id") or "")
            market_id = str(row.get("id") or "")
            if not condition_id or not market_id:
                continue
            event = (row.get("events") or [{}])[0]
            event_id = str(row.get("event_id") or event.get("id") or market_id)
            instruments = [f"POLY:{condition_id}:{token}" for token in tokens]
            neg_risk = bool(row.get("negRisk") or row.get("neg_risk"))
            neg_risk_id = str(
                row.get("negRiskMarketID") or row.get("neg_risk_market_id")
                or event.get("negRiskMarketID") or ""
            ) or None
            identity = PredictionMarketIdentity(
                event_id=event_id,
                market_id=market_id,
                condition_id=condition_id,
                slug=str(row.get("slug") or market_id),
                outcomes=[
                    OutcomeToken(token_id=token, outcome=outcome, instrument_id=instrument)
                    for token, outcome, instrument in zip(tokens, outcomes, instruments)
                ],
                neg_risk=neg_risk,
                neg_risk_market_id=neg_risk_id,
            )
            raw_hash = content_sha256(row)
            revision = content_sha256({
                "market_id": market_id,
                "updated_at": row.get("updatedAt") or row.get("updated_at"),
                "raw_payload_sha256": raw_hash,
            })
            relations = [LiveRelation(
                relation_id=f"binary:{condition_id}",
                relation_type="binary_complements",
                members=instruments,
                convertible=True,
                revision=revision,
                valid_from=observed_at,
            )]
            if neg_risk:
                relations.append(LiveRelation(
                    relation_id=f"neg-risk:{neg_risk_id}",
                    relation_type="standard_negative_risk",
                    members=instruments,
                    convertible=True,
                    revision=revision,
                    valid_from=observed_at,
                ))
            fee_schedule = row.get("fee_schedule") or row.get("feeSchedule") or {}
            fee_enabled = row.get("feesEnabled")
            maker = fee_schedule.get("maker_fee_bps") or fee_schedule.get("makerFeeBps")
            taker = (
                fee_schedule.get("taker_fee_bps") or fee_schedule.get("takerFeeBps")
                or row.get("takerBaseFee") or row.get("taker_base_fee")
            )
            if fee_enabled is False:
                maker = maker if maker is not None else "0"
                taker = taker if taker is not None else "0"
            fee_rate = fee_schedule.get("rate")
            fee_exponent = fee_schedule.get("exponent")
            fee_rebate_rate = fee_schedule.get("rebateRate") or fee_schedule.get("rebate_rate")
            fee_taker_only = fee_schedule.get("takerOnly")
            if fee_taker_only is None:
                fee_taker_only = fee_schedule.get("taker_only")
            tick = row.get("orderPriceMinTickSize") or row.get("minimumTickSize") or row.get("tickSize")
            minimum = row.get("orderMinSize") or row.get("minimumOrderSize")
            rules_complete = tick is not None and minimum is not None
            fee_complete = (
                maker is not None and taker is not None
            ) or (
                fee_rate is not None and fee_exponent is not None
                and fee_taker_only is not None
            )
            closed = bool(row.get("closed"))
            resolution = row.get("resolution")
            lifecycle = "resolved" if resolution not in {None, ""} else "closed" if closed else "active"
            result.append(LiveMarket(
                identity=identity,
                question=str(row.get("question") or row.get("title") or market_id),
                title=str(row.get("title") or row.get("question") or market_id),
                active=bool(row.get("active", not closed)),
                closed=closed,
                accepting_orders=bool(row.get("acceptingOrders", row.get("accepting_orders", False))),
                lifecycle_state=lifecycle,
                resolution=str(resolution) if resolution not in {None, ""} else None,
                start_at=_instant(row["startDate"]) if row.get("startDate") else None,
                end_at=_instant(row["endDate"]) if row.get("endDate") else None,
                metadata_revision=revision,
                observed_at=observed_at,
                rules=LiveRuleSet(
                    tick_size=str(tick or "1"), minimum_order_size=str(minimum or "1"),
                    maker_fee_bps=(str(maker) if maker is not None else None),
                    taker_fee_bps=(str(taker) if taker is not None else None),
                    fee_rate=(str(fee_rate) if fee_rate is not None else None),
                    fee_exponent=(str(fee_exponent) if fee_exponent is not None else None),
                    fee_rebate_rate=(
                        str(fee_rebate_rate) if fee_rebate_rate is not None else None
                    ),
                    fee_taker_only=(
                        _bool(fee_taker_only) if fee_taker_only is not None else None
                    ),
                    fee_complete=fee_complete, rules_complete=rules_complete,
                ),
                relations=relations,
                raw_payload_sha256=raw_hash,
            ))
        neg_risk_groups: dict[str, list[str]] = defaultdict(list)
        for market in result:
            if market.identity.neg_risk and market.identity.neg_risk_market_id:
                neg_risk_groups[market.identity.neg_risk_market_id].extend(
                    outcome.instrument_id for outcome in market.identity.outcomes
                )
        for market in result:
            group_id = market.identity.neg_risk_market_id
            if not market.identity.neg_risk or not group_id:
                continue
            members = sorted(set(neg_risk_groups[group_id]))
            relation = next(
                item for item in market.relations
                if item.relation_type == "standard_negative_risk"
            )
            relation.members = members
            relation.revision = content_sha256({
                "relation_id": relation.relation_id, "members": members,
                "market_revisions": sorted(
                    item.metadata_revision for item in result
                    if item.identity.neg_risk_market_id == group_id
                ),
            })
        return result


class ClobBooksClient:
    endpoint = "https://clob.polymarket.com/books"

    def __init__(self, *, requester: Callable[..., Any] = requests.post, timeout: float = 20, batch_size: int = 500):
        self.requester = requester
        self.timeout = timeout
        self.batch_size = max(1, batch_size)

    def fetch(self, token_ids: Iterable[str]) -> list[dict[str, Any]]:
        tokens = list(dict.fromkeys(str(item) for item in token_ids))
        rows = []
        for start in range(0, len(tokens), self.batch_size):
            response = self.requester(
                self.endpoint,
                json=[{"token_id": token} for token in tokens[start:start + self.batch_size]],
                timeout=self.timeout,
                headers={"Content-Type": "application/json", "User-Agent": "MarketCow/0.2"},
            )
            response.raise_for_status()
            payload = json.loads(response.text, parse_float=str, parse_int=str)
            if not isinstance(payload, list):
                raise RuntimeError("CLOB /books response must be a list")
            rows.extend(payload)
        return rows


class SubscriptionPlanner:
    """Deterministic full-market subscription sharding and dynamic diffs."""

    def __init__(self, shard_size: int = 500):
        self.shard_size = max(1, shard_size)
        self.current: set[str] = set()

    def shards(self, tokens: Iterable[str]) -> list[list[str]]:
        ordered = sorted(set(tokens))
        return [ordered[index:index + self.shard_size] for index in range(0, len(ordered), self.shard_size)]

    def initial_messages(self, tokens: Iterable[str]) -> list[dict[str, Any]]:
        self.current = set(tokens)
        return [
            {"assets_ids": shard, "type": "market", "custom_feature_enabled": True}
            for shard in self.shards(self.current)
        ]

    def update_messages(self, tokens: Iterable[str]) -> list[dict[str, Any]]:
        desired = set(tokens)
        added, removed = desired - self.current, self.current - desired
        messages = [
            {"assets_ids": shard, "operation": "subscribe", "custom_feature_enabled": True}
            for shard in self.shards(added)
        ] + [
            {"assets_ids": shard, "operation": "unsubscribe"}
            for shard in self.shards(removed)
        ]
        self.current = desired
        return messages


class LiveStateStore:
    """Append-only live state, deterministic recovery, and fail-closed frames."""

    def __init__(
        self,
        root: Path,
        *,
        replay_capacity: int = 100_000,
        max_frame_skew_ms: int = 5_000,
        stale_after_ms: int = 30_000,
        now_provider: Callable[[], datetime] = utc_now,
    ):
        self.root = root.resolve()
        self.event_path = self.root / "events.jsonl"
        self.checkpoint_path = self.root / "checkpoint.json"
        self.catalog_path = self.root / "catalog.json"
        self.raw_catalog_root = self.root / "raw" / "gamma-catalog"
        self.raw_catalog_path: Path | None = None
        self.replay_capacity = max(1, replay_capacity)
        self.max_frame_skew_ms = max(0, max_frame_skew_ms)
        self.stale_after_ms = max(1, stale_after_ms)
        self.now_provider = now_provider
        self.catalog: dict[str, LiveMarket] = {}
        self.token_to_market: dict[str, str] = {}
        self.books: dict[str, LiveBook] = {}
        self.gaps: list[GapEntry] = []
        self.events: deque[LiveEventEnvelope] = deque(maxlen=self.replay_capacity)
        self.seen_raw_hashes: set[str] = set()
        self.cursor = 0
        self.catalog_revision: str | None = None
        self.catalog_source: dict[str, Any] | None = None
        self.recover()

    def _append(self, value: dict[str, Any]) -> None:
        self.event_path.parent.mkdir(parents=True, exist_ok=True)
        with self.event_path.open("ab") as stream:
            stream.write(canonical_json(value) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())

    def replace_catalog(self, markets: list[LiveMarket], raw_rows: list[dict[str, Any]]) -> dict[str, Any]:
        by_id = {market.identity.market_id: market for market in markets}
        for market_id, previous in self.catalog.items():
            if market_id not in by_id and previous.lifecycle_state == "resolved":
                by_id[market_id] = previous
        markets = list(by_id.values())
        revision = content_sha256([
            market.model_dump(mode="json") for market in sorted(markets, key=lambda item: item.identity.market_id)
        ])
        previous_tokens = set(self.token_to_market)
        self.catalog = by_id
        self.token_to_market = {
            outcome.token_id: market.identity.market_id
            for market in markets if market.active and not market.closed
            for outcome in market.identity.outcomes
        }
        self.catalog_revision = revision
        raw_body = canonical_json(raw_rows)
        raw_sha256 = hashlib.sha256(raw_body).hexdigest()
        self.raw_catalog_path = self.raw_catalog_root / f"{raw_sha256}.json"
        self.catalog_source = {
            "source": "polymarket_gamma",
            "source_url": GammaKeysetCatalog.endpoint,
            "observed_at": self.now_provider().astimezone(timezone.utc).isoformat(),
            "raw_payload_sha256": raw_sha256,
            "raw_path": str(self.raw_catalog_path),
        }
        payload = {
            "schema_version": LIVE_SCHEMA_VERSION,
            "catalog_revision": revision,
            "markets": [market.model_dump(mode="json") for market in markets],
            "catalog_source": self.catalog_source,
        }
        if not self.raw_catalog_path.exists():
            _atomic_write(self.raw_catalog_path, raw_body)
        _atomic_write(self.catalog_path, canonical_json(payload))
        changed = sorted(previous_tokens ^ set(self.token_to_market))
        self._emit(
            "catalog_revision", {"catalog_revision": revision, "changed_tokens": changed},
            {"markets": raw_rows}, applied=True,
        )
        return {"catalog_revision": revision, "changed_tokens": changed}

    def _emit(
        self,
        event_type: str,
        canonical_payload: dict[str, Any],
        raw_payload: dict[str, Any],
        *,
        applied: bool,
        market_id: str | None = None,
        condition_id: str | None = None,
        token_id: str | None = None,
        book_epoch: str | None = None,
        sequence: int | None = None,
        exchange_at: datetime | None = None,
        received_at: datetime | None = None,
        reason: str | None = None,
    ) -> LiveEventEnvelope:
        received = received_at or self.now_provider()
        exchange = exchange_at or received
        self.cursor += 1
        raw_hash = content_sha256(raw_payload)
        canonical_hash = content_sha256(canonical_payload)
        event_id = content_sha256({
            "cursor": self.cursor, "event_type": event_type,
            "raw_payload_sha256": raw_hash, "canonical_payload_sha256": canonical_hash,
        })
        envelope = LiveEventEnvelope(
            cursor=self.cursor, event_id=event_id, event_type=event_type,
            market_id=market_id, condition_id=condition_id, token_id=token_id,
            book_epoch=book_epoch, sequence=sequence,
            exchange_at=exchange, received_at=received,
            canonical_payload=canonical_payload,
            canonical_payload_sha256=canonical_hash,
            raw_payload=raw_payload, raw_payload_sha256=raw_hash,
            applied=applied, fail_closed_reason=reason,
        )
        self.events.append(envelope)
        self._append(envelope.model_dump(mode="json"))
        self.seen_raw_hashes.add(raw_hash)
        return envelope

    def _market_for_token(self, token_id: str) -> LiveMarket:
        market_id = self.token_to_market.get(token_id)
        if not market_id or market_id not in self.catalog:
            raise ValueError("token is outside the active canonical catalog")
        return self.catalog[market_id]

    def apply_snapshot(
        self,
        raw: dict[str, Any],
        *,
        recovery_id: str | None = None,
        received_at: datetime | None = None,
    ) -> LiveEventEnvelope:
        token_id = str(raw.get("asset_id") or raw.get("token_id") or "")
        market = self._market_for_token(token_id)
        tick = decimal_text(raw.get("tick_size") or market.rules.tick_size, "tick_size", allow_zero=False)
        bids, asks = _levels(raw.get("bids"), "bids"), _levels(raw.get("asks"), "asks")
        state_bids = {item["price"]: item["size"] for item in bids}
        state_asks = {item["price"]: item["size"] for item in asks}
        _validate_book({"tick_size": tick, "bids": state_bids, "asks": state_asks})
        received = received_at or self.now_provider()
        exchange = _instant(raw.get("timestamp") or received)
        previous = self.books.get(token_id)
        epoch = content_sha256({
            "token_id": token_id, "recovery_id": recovery_id or "initial",
            "source_hash": raw.get("hash"), "exchange_at": exchange.isoformat(),
        })
        sequence = 1 if previous is None or previous.book_epoch != epoch else previous.sequence + 1
        checksum = _state_checksum(token_id, tick, state_bids, state_asks)
        book = LiveBook(
            token_id=token_id, condition_id=market.identity.condition_id,
            book_epoch=epoch, sequence=sequence,
            exchange_at=exchange, received_at=received,
            tick_version=content_sha256({"tick_size": tick}), tick_size=tick,
            bids=bids, asks=asks,
            last_trade_price=(
                decimal_text(raw["last_trade_price"], "last_trade_price")
                if raw.get("last_trade_price") is not None else None
            ),
            state_checksum=checksum, source_hash=str(raw.get("hash") or "") or None,
        )
        self.books[token_id] = book
        return self._emit(
            "book", book.model_dump(mode="json"), raw, applied=True,
            market_id=market.identity.market_id, condition_id=market.identity.condition_id,
            token_id=token_id, book_epoch=epoch, sequence=sequence,
            exchange_at=exchange, received_at=received,
        )

    def apply_websocket(self, raw: dict[str, Any], *, received_at: datetime | None = None) -> list[LiveEventEnvelope]:
        raw_hash = content_sha256(raw)
        event_type = str(raw.get("event_type") or raw.get("type") or "")
        received = received_at or self.now_provider()
        exchange = _instant(raw.get("timestamp") or received)
        if raw_hash in self.seen_raw_hashes:
            self.gaps.append(GapEntry(
                code="duplicate", observed=raw_hash, event_at=exchange,
                detected_at=received, resolved=True, resolution="ignored_idempotently",
            ))
            return []
        if event_type == "book":
            return [self.apply_snapshot(raw, received_at=received)]
        if event_type in {"new_market", "market_resolved"}:
            condition_id = str(raw.get("condition_id") or raw.get("market") or "")
            affected = next((
                market for market in self.catalog.values()
                if market.identity.condition_id == condition_id
            ), None)
            canonical = {
                "requires_catalog_refresh": True,
                "condition_id": condition_id,
            }
            if event_type == "market_resolved" and affected is not None:
                affected.active = False
                affected.closed = True
                affected.accepting_orders = False
                affected.lifecycle_state = "resolved"
                affected.resolution = str(
                    raw.get("winning_outcome") or raw.get("winning_asset_id") or ""
                ) or "resolved"
                affected.metadata_revision = content_sha256({
                    "previous_revision": affected.metadata_revision,
                    "market_resolved_raw_sha256": raw_hash,
                })
                for outcome in affected.identity.outcomes:
                    self.token_to_market.pop(outcome.token_id, None)
                canonical.update({
                    "market_id": affected.identity.market_id,
                    "lifecycle_state": "resolved",
                    "resolution": affected.resolution,
                    "metadata_revision": affected.metadata_revision,
                })
            return [self._emit(event_type, {
                **canonical,
            }, raw, applied=affected is not None or event_type == "new_market",
            market_id=(affected.identity.market_id if affected else None),
            condition_id=condition_id or None,
            exchange_at=exchange, received_at=received,
            reason=(None if affected is not None else "catalog_refresh_required"))]
        if event_type == "price_change":
            changes = raw.get("price_changes") or raw.get("changes") or []
            envelopes = []
            for change in changes:
                token_id = str(change.get("asset_id") or change.get("token_id") or "")
                envelopes.append(self._apply_token_event(event_type, token_id, change, raw, exchange, received))
            return envelopes
        token_id = str(raw.get("asset_id") or raw.get("token_id") or "")
        return [self._apply_token_event(event_type, token_id, raw, raw, exchange, received)]

    def _apply_token_event(
        self,
        event_type: str,
        token_id: str,
        payload: dict[str, Any],
        raw: dict[str, Any],
        exchange: datetime,
        received: datetime,
    ) -> LiveEventEnvelope:
        if event_type not in {"price_change", "best_bid_ask", "last_trade_price", "tick_size_change"}:
            raise ValueError("unsupported public market-channel event")
        market = self._market_for_token(token_id)
        previous = self.books.get(token_id)
        if previous is None:
            self.gaps.append(GapEntry(
                code="missing_snapshot", token_id=token_id,
                observed=content_sha256(raw), event_at=exchange, detected_at=received,
            ))
            return self._emit(
                event_type, {"requires_snapshot_recovery": True}, raw, applied=False,
                market_id=market.identity.market_id, condition_id=market.identity.condition_id,
                token_id=token_id, exchange_at=exchange, received_at=received,
                reason="missing_snapshot",
            )
        if exchange < previous.exchange_at:
            self.gaps.append(GapEntry(
                code="out_of_order", token_id=token_id,
                expected=previous.exchange_at.isoformat(), observed=exchange.isoformat(),
                event_at=exchange, detected_at=received,
            ))
            return self._emit(
                event_type, {"ignored": "out_of_order"}, raw, applied=False,
                market_id=market.identity.market_id, condition_id=market.identity.condition_id,
                token_id=token_id, book_epoch=previous.book_epoch, sequence=previous.sequence,
                exchange_at=exchange, received_at=received, reason="out_of_order",
            )
        book = previous.model_copy(deep=True)
        book.sequence += 1
        book.exchange_at = exchange
        book.received_at = received
        if event_type == "price_change":
            side = str(payload.get("side") or "").upper()
            side_name = "bids" if side in {"BUY", "BID"} else "asks" if side in {"SELL", "ASK"} else ""
            if not side_name:
                raise ValueError("price_change side must be BUY/SELL")
            price = decimal_text(payload.get("price"), "price_change.price")
            size = decimal_text(payload.get("size"), "price_change.size")
            levels = {item["price"]: item["size"] for item in getattr(book, side_name)}
            if Decimal(size) == 0:
                levels.pop(price, None)
            else:
                levels[price] = size
            setattr(book, side_name, [
                {"price": item, "size": levels[item]}
                for item in sorted(levels, key=Decimal, reverse=side_name == "bids")
            ])
        elif event_type == "last_trade_price":
            book.last_trade_price = decimal_text(payload.get("price"), "last_trade_price")
        elif event_type == "tick_size_change":
            new_tick = payload.get("new_tick_size") or payload.get("tick_size")
            book.tick_size = decimal_text(new_tick, "new_tick_size", allow_zero=False)
            book.tick_version = content_sha256({"tick_size": book.tick_size})
        bids = {item["price"]: item["size"] for item in book.bids}
        asks = {item["price"]: item["size"] for item in book.asks}
        try:
            _validate_book({"tick_size": book.tick_size, "bids": bids, "asks": asks})
        except ValueError as exc:
            self.gaps.append(GapEntry(
                code="source_mismatch", token_id=token_id, observed=str(exc),
                event_at=exchange, detected_at=received,
            ))
            return self._emit(
                event_type, {"requires_snapshot_recovery": True}, raw, applied=False,
                market_id=market.identity.market_id, condition_id=market.identity.condition_id,
                token_id=token_id, book_epoch=previous.book_epoch, sequence=previous.sequence,
                exchange_at=exchange, received_at=received, reason="invalid_book_update",
            )
        book.state_checksum = _state_checksum(token_id, book.tick_size, bids, asks)
        self.books[token_id] = book
        return self._emit(
            event_type, book.model_dump(mode="json"), raw, applied=True,
            market_id=market.identity.market_id, condition_id=market.identity.condition_id,
            token_id=token_id, book_epoch=book.book_epoch, sequence=book.sequence,
            exchange_at=exchange, received_at=received,
        )

    def mark_recovery_started(self, reason: str) -> str:
        recovery_id = content_sha256({"cursor": self.cursor, "reason": reason, "at": self.now_provider().isoformat()})
        now = self.now_provider()
        for token_id in self.token_to_market:
            self.gaps.append(GapEntry(
                code="coverage_gap", token_id=token_id, observed=recovery_id,
                detected_at=now,
            ))
        self._emit("recovery_started", {"recovery_id": recovery_id, "reason": reason}, {}, applied=True)
        return recovery_id

    def recover_from_books(self, rows: list[dict[str, Any]], recovery_id: str) -> None:
        recovered = set()
        for row in rows:
            token_id = str(row.get("asset_id") or row.get("token_id") or "")
            self.apply_snapshot(row, recovery_id=recovery_id)
            recovered.add(token_id)
        expected = set(self.token_to_market)
        missing = expected - recovered
        if missing:
            raise RuntimeError(f"REST /books recovery missing {len(missing)} active tokens")
        for gap in self.gaps:
            if not gap.resolved and gap.token_id in recovered:
                gap.resolved = True
                gap.resolution = f"rest_books_snapshot:{recovery_id}"
        self._emit("recovery_completed", {
            "recovery_id": recovery_id, "recovered_token_count": len(recovered),
        }, {}, applied=True)
        self.checkpoint()

    def frame(self, market_id: str, *, now: datetime | None = None) -> MarketFrame:
        market = self.catalog.get(market_id)
        if market is None:
            raise KeyError(market_id)
        books = [self.books[token.token_id] for token in market.identity.outcomes if token.token_id in self.books]
        reasons = []
        if len(books) != 2:
            reasons.append("missing_outcome_book")
        if not market.rules.rules_complete:
            reasons.append("rules_incomplete")
        if not market.rules.fee_complete:
            reasons.append("fee_incomplete")
        if any(not gap.resolved and gap.token_id in {item.token_id for item in market.identity.outcomes} for gap in self.gaps):
            reasons.append("unresolved_gap")
        current = now or self.now_provider()
        if books:
            skew = (max(item.exchange_at for item in books) - min(item.exchange_at for item in books)).total_seconds() * 1000
            if skew > self.max_frame_skew_ms:
                reasons.append("token_frame_skew")
            if any((current - item.received_at).total_seconds() * 1000 > self.stale_after_ms for item in books):
                reasons.append("stale_book")
        relation_ids = [item.relation_id for item in market.relations]
        relation_token_ids = {
            instrument.rsplit(":", 1)[-1]
            for relation in market.relations
            if relation.relation_type == "standard_negative_risk"
            for instrument in relation.members
        }
        relation_books = [
            self.books[token_id] for token_id in sorted(relation_token_ids)
            if token_id in self.books
        ]
        if relation_token_ids:
            if len(relation_books) != len(relation_token_ids):
                reasons.append("negative_risk_member_missing")
            if relation_books:
                relation_skew = (
                    max(item.exchange_at for item in relation_books)
                    - min(item.exchange_at for item in relation_books)
                ).total_seconds() * 1000
                if relation_skew > self.max_frame_skew_ms:
                    reasons.append("negative_risk_frame_skew")
                if any(
                    (current - item.received_at).total_seconds() * 1000 > self.stale_after_ms
                    for item in relation_books
                ):
                    reasons.append("negative_risk_member_stale")
            if any(
                not gap.resolved and gap.token_id in relation_token_ids
                for gap in self.gaps
            ):
                reasons.append("negative_risk_member_gap")
        return MarketFrame(
            market_id=market_id, condition_id=market.identity.condition_id,
            frame_at=current, cursor=self.cursor,
            status="fail_closed" if reasons else "ready",
            reason_codes=sorted(set(reasons)), tokens=books,
            relation_ids=relation_ids, relation_tokens=relation_books,
        )

    def checkpoint_payload(self) -> LiveCheckpoint:
        state = {
            "cursor": self.cursor, "catalog_revision": self.catalog_revision,
            "books": {key: value.model_dump(mode="json") for key, value in sorted(self.books.items())},
            "unresolved_gaps": [gap.model_dump(mode="json") for gap in self.gaps if not gap.resolved],
        }
        checkpoint = LiveCheckpoint(
            **state, created_at=self.now_provider(), state_sha256=content_sha256(state)
        )
        return checkpoint

    def checkpoint(self) -> LiveCheckpoint:
        checkpoint = self.checkpoint_payload()
        _atomic_write(self.checkpoint_path, canonical_json(checkpoint.model_dump(mode="json")))
        return checkpoint

    def recover(self) -> None:
        if self.catalog_path.exists():
            payload = json.loads(self.catalog_path.read_text(encoding="utf-8"), parse_float=str, parse_int=str)
            markets = [LiveMarket.model_validate(item) for item in payload.get("markets") or []]
            observed_revision = content_sha256([
                market.model_dump(mode="json")
                for market in sorted(markets, key=lambda item: item.identity.market_id)
            ])
            if observed_revision != payload.get("catalog_revision"):
                raise RuntimeError("live catalog integrity failed")
            self.catalog = {item.identity.market_id: item for item in markets}
            self.token_to_market = {
                outcome.token_id: market.identity.market_id
                for market in markets if market.active and not market.closed
                for outcome in market.identity.outcomes
            }
            self.catalog_revision = payload.get("catalog_revision")
            self.catalog_source = payload.get("catalog_source")
            if self.catalog_source:
                self.raw_catalog_path = Path(
                    self.catalog_source["raw_path"]
                ).resolve()
                if not self.raw_catalog_path.is_relative_to(self.raw_catalog_root):
                    raise RuntimeError("live raw catalog escapes local evidence root")
                if (
                    not self.raw_catalog_path.exists()
                    or hashlib.sha256(self.raw_catalog_path.read_bytes()).hexdigest()
                    != self.catalog_source.get("raw_payload_sha256")
                ):
                    raise RuntimeError("live raw catalog integrity failed")
        if self.checkpoint_path.exists():
            checkpoint = LiveCheckpoint.model_validate_json(self.checkpoint_path.read_text())
            state = {
                "cursor": checkpoint.cursor, "catalog_revision": checkpoint.catalog_revision,
                "books": {key: value.model_dump(mode="json") for key, value in sorted(checkpoint.books.items())},
                "unresolved_gaps": [item.model_dump(mode="json") for item in checkpoint.unresolved_gaps],
            }
            if content_sha256(state) != checkpoint.state_sha256:
                raise RuntimeError("live checkpoint integrity failed")
            self.cursor = checkpoint.cursor
            self.books = checkpoint.books
            self.gaps = checkpoint.unresolved_gaps
        if self.event_path.exists():
            checkpoint_cursor = self.cursor
            for line in self.event_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                event = LiveEventEnvelope.model_validate_json(line)
                self.seen_raw_hashes.add(event.raw_payload_sha256)
                self.events.append(event)
                if (
                    event.cursor > checkpoint_cursor and event.applied
                    and event.token_id and event.event_type in {
                        "book", "price_change", "best_bid_ask",
                        "last_trade_price", "tick_size_change",
                    }
                ):
                    self.books[event.token_id] = LiveBook.model_validate(
                        event.canonical_payload
                    )
                self.cursor = max(self.cursor, event.cursor)

    def events_after(self, cursor: int, limit: int) -> tuple[list[LiveEventEnvelope], bool]:
        if self.events and cursor < self.events[0].cursor - 1:
            raise RuntimeError("resume_cursor_expired")
        selected = [event for event in self.events if event.cursor > cursor]
        return selected[:limit], len(selected) > limit

    def health(self) -> LiveHealth:
        active_market_ids = sorted(set(self.token_to_market.values()))
        frames = [self.frame(market_id) for market_id in active_market_ids]
        latest = max((book.received_at for book in self.books.values()), default=None)
        now = self.now_provider()
        lag = int((now - latest).total_seconds() * 1000) if latest else None
        unresolved = sum(not gap.resolved for gap in self.gaps)
        ready = sum(frame.status == "ready" for frame in frames)
        status = (
            "empty" if not active_market_ids
            else "ready" if ready == len(frames)
            else "degraded"
        )
        return LiveHealth(
            status=status, catalog_revision=self.catalog_revision,
            active_market_count=len(active_market_ids),
            subscribed_token_count=len(self.token_to_market),
            book_token_count=len(self.books), ready_market_count=ready,
            unresolved_gap_count=unresolved, latest_cursor=self.cursor,
            latest_received_at=latest, lag_ms=lag,
        )


class DataApiPublicNormalizer:
    """Decimal-safe public wallet facts; never claims orders or real-world identity."""

    @staticmethod
    def normalize(kind: str, rows: list[dict[str, Any]], observed_at: datetime) -> list[dict[str, Any]]:
        if kind not in PUBLIC_DATA_KINDS:
            raise ValueError("unsupported Data API public fact kind")
        normalized = []
        expanded = []
        for row in rows:
            if kind in {"positions", "holders"} and (row.get(kind) or row.get(kind.rstrip("s"))):
                children = row.get(kind) or row.get(kind.rstrip("s")) or []
                for child in children:
                    expanded.append({**child, "token": row.get("token")})
            else:
                expanded.append(row)
        decimal_fields = {
            "size", "price", "amount", "avgPrice", "currPrice", "curPrice",
            "currentValue", "cashPnl", "totalBought", "realizedPnl", "totalPnl", "usdcSize",
        }
        for row in expanded:
            values = {}
            for name in decimal_fields:
                if row.get(name) is not None:
                    values[name] = decimal_text(row[name], f"data_api.{name}")
            profile = {
                key: row.get(key) for key in (
                    "name", "pseudonym", "bio", "profileImage", "profileImageOptimized",
                    "displayUsernamePublic", "verified",
                ) if row.get(key) is not None
            }
            canonical = {
                "schema_version": "marketcow.polymarket.public-fact.v1",
                "fact_type": kind.rstrip("s"),
                "wallet": str(row.get("proxyWallet") or row.get("wallet") or "").lower(),
                "condition_id": str(row.get("conditionId") or row.get("condition_id") or ""),
                "token_id": str(row.get("asset") or row.get("token") or ""),
                "outcome": str(row.get("outcome") or ""),
                "side": str(row.get("side") or "").upper() or None,
                "timestamp": (
                    _instant(row["timestamp"]).isoformat() if row.get("timestamp") is not None else None
                ),
                "transaction_hash": str(row.get("transactionHash") or "").lower() or None,
                "decimal_values": values,
                "public_profile_provenance": profile,
                "source": "polymarket_data_api",
                "observed_at": observed_at.isoformat(),
                "semantic_boundary": (
                    "public trade/activity/position/holder fact; not an unfilled-order owner "
                    "and not a verified real-world identity"
                ),
            }
            normalized.append({
                "contract_version": CONTRACT_VERSION,
                "schema_version": "marketcow.polymarket.public-fact-envelope.v1",
                "fact_id": content_sha256({
                    "kind": kind, "canonical_payload": canonical,
                    "raw_payload_sha256": content_sha256(row),
                }),
                "canonical_payload": canonical,
                "canonical_payload_sha256": content_sha256(canonical),
                "raw_payload": row,
                "raw_payload_sha256": content_sha256(row),
            })
        return normalized


class DataApiPublicClient:
    base_url = "https://data-api.polymarket.com"

    def __init__(self, *, requester: Callable[..., Any] = requests.get, timeout: float = 20):
        self.requester = requester
        self.timeout = timeout

    def fetch(self, kind: str, *, params: dict[str, Any]) -> list[dict[str, Any]]:
        if kind not in PUBLIC_DATA_KINDS:
            raise ValueError("unsupported Data API endpoint")
        endpoint = "v1/market-positions" if kind == "positions" and params.get("market") else kind
        response = self.requester(
            f"{self.base_url}/{endpoint}", params=params, timeout=self.timeout,
            headers={"Accept": "application/json", "User-Agent": "MarketCow/0.2"},
        )
        response.raise_for_status()
        payload = json.loads(response.text, parse_float=str, parse_int=str)
        if not isinstance(payload, list):
            raise RuntimeError("Data API public response must be a list")
        return payload


class PolymarketLiveCollector:
    """Official public collector with sharding, heartbeats, and snapshot recovery."""

    endpoint = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(
        self,
        store: LiveStateStore,
        catalog: GammaKeysetCatalog,
        books: ClobBooksClient,
        *,
        connector: Callable[..., Any] = websockets.connect,
        shard_size: int = 500,
        heartbeat_seconds: float = 10,
        reconnect_seconds: float = 1,
    ):
        self.store = store
        self.catalog_client = catalog
        self.books_client = books
        self.connector = connector
        self.planner = SubscriptionPlanner(shard_size)
        self.heartbeat_seconds = max(0.01, heartbeat_seconds)
        self.reconnect_seconds = max(0.0, reconnect_seconds)
        self.sockets: list[Any] = []
        self.socket_tokens: dict[Any, set[str]] = {}

    def refresh_catalog(self) -> dict[str, Any]:
        rows, evidence = self.catalog_client.fetch_all()
        markets = GammaLiveNormalizer.normalize(rows, self.store.now_provider())
        update = self.store.replace_catalog(markets, rows)
        return {**evidence, **update, "active_token_count": len(self.store.token_to_market)}

    async def bootstrap_books(self, reason: str = "startup") -> str:
        recovery_id = self.store.mark_recovery_started(reason)
        rows = await asyncio.to_thread(
            self.books_client.fetch, sorted(self.store.token_to_market)
        )
        self.store.recover_from_books(rows, recovery_id)
        return recovery_id

    async def update_subscriptions(self) -> list[dict[str, Any]]:
        desired = set(self.store.token_to_market)
        messages = self.planner.update_messages(desired)
        if not self.sockets:
            return messages
        removed = set().union(*(self.socket_tokens.values() or [set()])) - desired
        for socket in self.sockets:
            owned_removed = sorted(self.socket_tokens.get(socket, set()) & removed)
            for shard in self.planner.shards(owned_removed):
                await socket.send(json.dumps({
                    "assets_ids": shard, "operation": "unsubscribe",
                }, separators=(",", ":")))
            self.socket_tokens.setdefault(socket, set()).difference_update(owned_removed)
        owned = set().union(*(self.socket_tokens.values() or [set()]))
        for token_id in sorted(desired - owned):
            socket = min(self.sockets, key=lambda item: len(self.socket_tokens.get(item, set())))
            await socket.send(json.dumps({
                "assets_ids": [token_id], "operation": "subscribe",
                "custom_feature_enabled": True,
            }, separators=(",", ":")))
            self.socket_tokens.setdefault(socket, set()).add(token_id)
        if messages:
            self.store._emit(
                "subscription_change", {"messages": messages}, {}, applied=True
            )
        return messages

    async def _consume(self, shard: list[str], *, message_limit: int | None = None) -> None:
        async with self.connector(self.endpoint) as socket:
            self.sockets.append(socket)
            self.socket_tokens[socket] = set(shard)
            try:
                await socket.send(json.dumps({
                    "assets_ids": shard, "type": "market",
                    "custom_feature_enabled": True,
                }, separators=(",", ":")))
                consumed = 0
                while message_limit is None or consumed < message_limit:
                    try:
                        message = await asyncio.wait_for(
                            socket.recv(), timeout=self.heartbeat_seconds
                        )
                    except asyncio.TimeoutError:
                        await socket.send("PING")
                        continue
                    if message == "PONG":
                        continue
                    payload = json.loads(message, parse_float=str, parse_int=str)
                    for item in payload if isinstance(payload, list) else [payload]:
                        self.store.apply_websocket(item)
                        consumed += 1
                        if str(item.get("event_type") or item.get("type") or "") in {
                            "new_market", "market_resolved",
                        }:
                            await asyncio.to_thread(self.refresh_catalog)
                            await self.update_subscriptions()
            finally:
                self.sockets.remove(socket)
                self.socket_tokens.pop(socket, None)

    async def run_once(self, *, message_limit: int | None = None) -> None:
        shards = self.planner.shards(self.store.token_to_market)
        if not shards:
            raise RuntimeError("live collector has no active tokens")
        await asyncio.gather(*(
            self._consume(shard, message_limit=message_limit) for shard in shards
        ))

    async def run(self, *, max_connections: int | None = None) -> None:
        attempts = 0
        while max_connections is None or attempts < max_connections:
            attempts += 1
            if attempts > 1:
                await self.bootstrap_books(f"websocket_reconnect:{attempts}")
            try:
                await self.run_once()
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                if max_connections is not None and attempts >= max_connections:
                    raise
                await asyncio.sleep(self.reconnect_seconds)


def atomic_write_public_facts(root: Path, kind: str, rows: list[dict[str, Any]]) -> Path:
    if kind not in PUBLIC_DATA_KINDS:
        raise ValueError("unsupported public fact kind")
    root = root.resolve()
    folder = root / "public-data" / kind
    folder.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".partial-", dir=folder)
    path = folder / f"{content_sha256(rows)}.json"
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(canonical_json(rows))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return path
