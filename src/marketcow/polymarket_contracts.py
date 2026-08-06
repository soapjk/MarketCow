from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from decimal import (
    ROUND_DOWN,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    Decimal,
    InvalidOperation,
)
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


CONTRACT_VERSION = "marketcow.prediction_market.v1"
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def content_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def decimal_text(value: Any, field: str, *, allow_zero: bool = True) -> str:
    if isinstance(value, float):
        raise ValueError(f"{field} must not pass through binary float")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a decimal string") from exc
    if not number.is_finite() or number < 0 or (not allow_zero and number == 0):
        raise ValueError(f"{field} must be a finite positive decimal")
    return format(number, "f")


class SourceRevision(BaseModel):
    source: Literal[
        "polymarket_gamma", "polymarket_clob", "polymarket_data_api",
        "polymarket_subgraph", "polygon_logs", "polymarket_websocket",
        "polymarket_docs", "polymarket_sdk", "huggingface_fixed_revision",
        "polymarket_fee_module",
    ]
    revision: str = Field(min_length=1, max_length=200)
    source_url: str = Field(min_length=1, max_length=2000)
    observed_at: datetime
    ingested_at: datetime
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    raw_path: str = Field(min_length=1)
    license: str | None = None


class OutcomeToken(BaseModel):
    token_id: str = Field(min_length=1, max_length=200)
    outcome: str = Field(min_length=1, max_length=200)
    instrument_id: str = Field(min_length=1, max_length=300)


class PredictionMarketIdentity(BaseModel):
    contract_version: Literal["marketcow.prediction_market.v1"] = CONTRACT_VERSION
    platform: Literal["polymarket"] = "polymarket"
    event_id: str = Field(min_length=1, max_length=200)
    market_id: str = Field(min_length=1, max_length=200)
    condition_id: str = Field(min_length=1, max_length=200)
    slug: str = Field(min_length=1, max_length=500)
    outcomes: list[OutcomeToken]
    neg_risk: bool = False
    neg_risk_market_id: str | None = None

    @model_validator(mode="after")
    def binary_and_reversible(self):
        if len(self.outcomes) != 2:
            raise ValueError("certifiable binary markets require exactly two outcomes")
        token_ids = [item.token_id for item in self.outcomes]
        instruments = [item.instrument_id for item in self.outcomes]
        if len(set(token_ids)) != 2 or len(set(instruments)) != 2:
            raise ValueError("outcome token identities must be unique")
        if self.neg_risk and not self.neg_risk_market_id:
            raise ValueError("negative-risk markets require neg_risk_market_id")
        return self


class MarketLifecycleRevision(BaseModel):
    contract_version: Literal["marketcow.prediction_market.v1"] = CONTRACT_VERSION
    identity: PredictionMarketIdentity
    revision_id: str = Field(min_length=1, max_length=200)
    valid_from: datetime
    valid_to: datetime | None = None
    state: Literal["discovered", "active", "closed", "resolved", "invalid"]
    resolution: str | None = None
    resolution_source: str | None = None
    tick_size: str
    minimum_order_size: str
    maker_fee_bps: str
    taker_fee_bps: str
    source: SourceRevision

    @model_validator(mode="after")
    def decimal_rules_and_window(self):
        self.tick_size = decimal_text(self.tick_size, "tick_size", allow_zero=False)
        self.minimum_order_size = decimal_text(
            self.minimum_order_size, "minimum_order_size", allow_zero=False
        )
        self.maker_fee_bps = decimal_text(self.maker_fee_bps, "maker_fee_bps")
        self.taker_fee_bps = decimal_text(self.taker_fee_bps, "taker_fee_bps")
        if self.valid_to is not None and self.valid_from >= self.valid_to:
            raise ValueError("lifecycle revision window must be ordered")
        return self


class GapEntry(BaseModel):
    code: Literal[
        "sequence_gap", "out_of_order", "duplicate", "hash_mismatch",
        "missing_snapshot", "coverage_gap", "source_mismatch",
    ]
    token_id: str | None = None
    expected: str | None = None
    observed: str | None = None
    event_at: datetime | None = None
    detected_at: datetime
    resolved: bool = False
    resolution: str | None = None


class DatasetPart(BaseModel):
    table: Literal["catalog", "lifecycle", "books", "trades", "onchain"]
    path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    rows: int = Field(ge=0)
    byte_size: int = Field(ge=0)
    start: datetime | None = None
    end: datetime | None = None


class ReplayContract(BaseModel):
    schema_version: Literal["marketcow.polymarket.book-replay.v1"] = (
        "marketcow.polymarket.book-replay.v1"
    )
    mode: Literal["snapshot_only", "snapshot_delta"]
    ordering: list[Literal[
        "exchange_at", "received_at", "book_epoch", "sequence", "record_id"
    ]] = Field(min_length=5, max_length=5)
    update_semantics: Literal["absolute_size"] = "absolute_size"
    sequence_semantics: Literal["source", "deterministic_normalized"]
    delta_supported: bool
    cancellation_supported: bool
    queue_position_supported: bool
    depth: str = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def replay_claims_are_consistent(self):
        if self.mode == "snapshot_only" and (
            self.delta_supported or self.cancellation_supported
            or self.queue_position_supported
        ):
            raise ValueError("snapshot-only replay cannot claim delta/cancel/queue semantics")
        if self.mode == "snapshot_delta" and not self.delta_supported:
            raise ValueError("snapshot-delta replay must support deltas")
        if self.ordering != [
            "exchange_at", "received_at", "book_epoch", "sequence", "record_id"
        ]:
            raise ValueError("book replay ordering must be deterministic and complete")
        return self


class RuleFact(BaseModel):
    fact_id: str = Field(min_length=1, max_length=300)
    rule_version: str = Field(min_length=1, max_length=100)
    fact_type: Literal[
        "binary_settlement", "standard_negative_risk", "settlement_currency",
        "price_increment", "size_increment", "minimum_order_size",
    ]
    value: dict[str, Any]
    provenance: SourceRevision
    valid_from: datetime
    valid_to: datetime | None = None

    @model_validator(mode="after")
    def valid_interval_is_ordered(self):
        if self.valid_to is not None and self.valid_from >= self.valid_to:
            raise ValueError("rule fact validity interval must be ordered")
        return self


class StructuralRelation(BaseModel):
    relation_id: str = Field(min_length=1, max_length=300)
    relation_version: str = Field(min_length=1, max_length=100)
    relation_type: Literal["binary_complements", "standard_negative_risk"]
    members: list[str] = Field(min_length=2)
    convertible: bool
    provenance: SourceRevision
    valid_from: datetime
    valid_to: datetime | None = None

    @model_validator(mode="after")
    def relation_is_explicit(self):
        if len(set(self.members)) != len(self.members):
            raise ValueError("structural relation members must be unique")
        if self.relation_type == "binary_complements" and len(self.members) != 2:
            raise ValueError("binary complement relation requires exactly two members")
        if self.valid_to is not None and self.valid_from >= self.valid_to:
            raise ValueError("structural relation validity interval must be ordered")
        return self


class FeeSchedule(BaseModel):
    schedule_id: str = Field(min_length=1, max_length=300)
    schedule_version: str = Field(min_length=1, max_length=100)
    currency: str = Field(min_length=1, max_length=50)
    maker_rate: str
    taker_rate: str
    formula: str = Field(min_length=1, max_length=500)
    exponent: str
    quantum: str
    rounding_mode: Literal[
        "ROUND_DOWN", "ROUND_HALF_EVEN", "ROUND_HALF_UP", "UNSPECIFIED"
    ]
    tie_semantics: Literal[
        "toward_zero", "ties_to_even", "ties_away_from_zero", "unspecified"
    ]
    calculation_status: Literal["executable_pnl", "informational_only"]
    effective_from: datetime
    effective_to: datetime | None = None
    provenance: list[SourceRevision] = Field(min_length=1)

    @model_validator(mode="after")
    def complete_schedule(self):
        self.maker_rate = decimal_text(self.maker_rate, "maker_rate")
        self.taker_rate = decimal_text(self.taker_rate, "taker_rate")
        self.exponent = decimal_text(self.exponent, "fee_exponent")
        self.quantum = decimal_text(self.quantum, "fee_quantum", allow_zero=False)
        expected_ties = {
            "ROUND_DOWN": "toward_zero",
            "ROUND_HALF_EVEN": "ties_to_even",
            "ROUND_HALF_UP": "ties_away_from_zero",
            "UNSPECIFIED": "unspecified",
        }
        if self.tie_semantics != expected_ties[self.rounding_mode]:
            raise ValueError("fee rounding mode and tie semantics disagree")
        if (
            self.calculation_status == "executable_pnl"
            and self.rounding_mode == "UNSPECIFIED"
        ):
            raise ValueError("executable PnL requires a deterministic fee rounding mode")
        if self.effective_to is not None and self.effective_from >= self.effective_to:
            raise ValueError("fee schedule interval must be ordered")
        return self


def quantize_fee_amount(raw_amount: Any, schedule: FeeSchedule) -> str:
    """Apply an explicitly certified fee quantum; ambiguous schedules fail closed."""
    if schedule.rounding_mode == "UNSPECIFIED":
        raise ValueError("fee rounding is not certified for executable PnL")
    amount = Decimal(decimal_text(raw_amount, "fee_amount"))
    quantum = Decimal(schedule.quantum)
    rounding = {
        "ROUND_DOWN": ROUND_DOWN,
        "ROUND_HALF_EVEN": ROUND_HALF_EVEN,
        "ROUND_HALF_UP": ROUND_HALF_UP,
    }[schedule.rounding_mode]
    units = (amount / quantum).quantize(Decimal("1"), rounding=rounding)
    return format(units * quantum, "f")


class MarketBootstrap(BaseModel):
    identity: PredictionMarketIdentity
    question: str = Field(min_length=1, max_length=2000)
    title: str = Field(min_length=1, max_length=2000)
    settlement_currency: str = Field(min_length=1, max_length=50)
    activation_at: datetime
    expiration_at: datetime
    price_increment: str
    size_increment: str
    minimum_order_size: str
    accepting_orders: bool
    lifecycle_state: Literal["discovered", "active", "closed", "resolved", "invalid"]
    resolution: str | None = None
    external_ids: dict[str, str] = Field(default_factory=dict)
    relations: list[StructuralRelation] = Field(min_length=1)
    rule_facts: list[RuleFact] = Field(min_length=1)
    fee_schedule: FeeSchedule

    @model_validator(mode="after")
    def nautilus_fields_are_complete(self):
        self.price_increment = decimal_text(
            self.price_increment, "price_increment", allow_zero=False
        )
        self.size_increment = decimal_text(
            self.size_increment, "size_increment", allow_zero=False
        )
        self.minimum_order_size = decimal_text(
            self.minimum_order_size, "minimum_order_size", allow_zero=False
        )
        if self.activation_at >= self.expiration_at:
            raise ValueError("market activation must precede expiration")
        if self.lifecycle_state == "resolved" and not self.resolution:
            raise ValueError("resolved bootstrap markets require a resolution")
        instruments = {item.instrument_id for item in self.identity.outcomes}
        relation_types = {item.relation_type for item in self.relations}
        if "binary_complements" not in relation_types:
            raise ValueError("binary bootstrap requires an explicit complement relation")
        if self.identity.neg_risk and "standard_negative_risk" not in relation_types:
            raise ValueError("negative-risk identity requires an explicit relation")
        if not self.identity.neg_risk and "standard_negative_risk" in relation_types:
            raise ValueError("standard negative risk must come from source metadata")
        for relation in self.relations:
            if not set(relation.members) <= instruments:
                raise ValueError("relation members must map to canonical instruments")
        required_facts = {
            "binary_settlement", "settlement_currency", "price_increment",
            "size_increment", "minimum_order_size",
        }
        fact_types = {item.fact_type for item in self.rule_facts}
        if not required_facts <= fact_types:
            raise ValueError("typed bootstrap is missing required rule facts")
        if self.fee_schedule.currency != self.settlement_currency:
            raise ValueError("fee and settlement currencies must match")
        return self


class PredictionMarketBootstrap(BaseModel):
    contract_version: Literal["marketcow.prediction_market.v1"] = CONTRACT_VERSION
    schema_version: Literal["marketcow.polymarket.bootstrap.v1"] = (
        "marketcow.polymarket.bootstrap.v1"
    )
    dataset_id: str = Field(min_length=1, max_length=300)
    manifest_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    bootstrap_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    intended_use: Literal[
        "nautilus_snapshot_replay", "nautilus_full_l2_replay",
        "official_onchain_reconciliation",
    ]
    replay: ReplayContract
    markets: list[MarketBootstrap] = Field(min_length=1)

    @model_validator(mode="after")
    def bootstrap_is_reversible_and_bound(self):
        market_ids = [item.identity.market_id for item in self.markets]
        condition_ids = [item.identity.condition_id for item in self.markets]
        instrument_ids = [
            outcome.instrument_id
            for market in self.markets for outcome in market.identity.outcomes
        ]
        if len(set(market_ids)) != len(market_ids):
            raise ValueError("bootstrap market identities must be unique")
        if len(set(condition_ids)) != len(condition_ids):
            raise ValueError("bootstrap condition identities must be unique")
        if len(set(instrument_ids)) != len(instrument_ids):
            raise ValueError("bootstrap instrument identities must be unique")
        return self


def bootstrap_identity(payload: dict[str, Any]) -> str:
    body = dict(payload)
    body.pop("manifest_id", None)
    body.pop("bootstrap_id", None)
    return content_sha256(body)


class CoverageFacts(BaseModel):
    market_count: int = Field(ge=0)
    token_count: int = Field(ge=0)
    start: datetime | None = None
    end: datetime | None = None
    event_count: int = Field(ge=0)
    trade_count: int = Field(ge=0)
    onchain_count: int = Field(ge=0)
    unresolved_gap_count: int = Field(ge=0)


class CertificationCheck(BaseModel):
    name: str
    passed: bool
    details: dict[str, Any] = Field(default_factory=dict)


class PredictionMarketManifest(BaseModel):
    contract_version: Literal["marketcow.prediction_market.v1"] = CONTRACT_VERSION
    manifest_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    dataset_id: str = Field(min_length=1, max_length=300)
    revision: str = Field(min_length=1, max_length=200)
    intended_use: Literal[
        "nautilus_snapshot_replay", "nautilus_full_l2_replay",
        "official_onchain_reconciliation",
    ]
    replay: ReplayContract
    bootstrap_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    required_parts: list[Literal[
        "catalog", "lifecycle", "books", "trades", "onchain"
    ]] = Field(min_length=1)
    status: Literal["draft", "certified", "rejected"]
    created_at: datetime
    identities: list[PredictionMarketIdentity]
    sources: list[SourceRevision]
    parts: list[DatasetPart]
    coverage: CoverageFacts
    gap_ledger: list[GapEntry]
    checks: list[CertificationCheck] = Field(default_factory=list)
    supersedes: str | None = None
    attestation_sha256: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )

    @model_validator(mode="after")
    def certification_is_evidence_backed(self):
        if self.status == "certified":
            if not self.checks or not all(item.passed for item in self.checks):
                raise ValueError("certified manifests require all checks to pass")
            if self.coverage.unresolved_gap_count:
                raise ValueError("certified manifests cannot contain unresolved gaps")
            if not self.attestation_sha256:
                raise ValueError("certified manifests require an attestation")
        return self


def manifest_identity(payload: dict[str, Any]) -> str:
    body = dict(payload)
    body.pop("manifest_id", None)
    body.pop("attestation_sha256", None)
    return content_sha256(body)
