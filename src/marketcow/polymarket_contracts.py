from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
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
        "huggingface_fixed_revision",
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
    maker_fee_bps: str = "0"
    taker_fee_bps: str = "0"
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
