from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


SCHEMA_VERSION = 1
DECIMAL_PATTERN = r"^-?(?:0|[1-9]\d*)(?:\.\d+)?$"
INSTRUMENT_ID_PATTERN = r"^[A-Z0-9][A-Z0-9.-]{0,31}\.[A-Z0-9]{4}$"


def _utc(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = SCHEMA_VERSION


class InstrumentContract(ContractModel):
    instrument_id: str = Field(pattern=INSTRUMENT_ID_PATTERN)
    symbol: str = Field(min_length=1, max_length=32)
    market: Literal["US", "HK", "CN"]
    mic: str = Field(pattern=r"^[A-Z0-9]{4}$")
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    price_precision: int = Field(ge=0, le=18)
    size_precision: int = Field(ge=0, le=18)
    tick_size: str = Field(pattern=DECIMAL_PATTERN)
    lot_size: str = Field(pattern=DECIMAL_PATTERN)
    provider_symbols: Dict[str, str] = Field(min_length=1)
    broker_symbols: Dict[str, str] = Field(default_factory=dict)

    @field_validator("tick_size", "lot_size")
    @classmethod
    def positive_decimal(cls, value: str) -> str:
        if Decimal(value) <= 0:
            raise ValueError("precision increments must be positive")
        return value


class QualityContract(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Literal["live", "delayed", "stale", "degraded", "historical"]
    delayed: bool
    stale: bool
    degraded: bool


class EventEnvelope(ContractModel):
    stream_id: str = Field(min_length=1)
    sequence: int = Field(ge=1)
    event_type: Literal[
        "quote", "trade", "order_book_snapshot", "order_book_delta",
        "bar", "stream_status", "heartbeat", "error",
    ]
    instrument_id: Optional[str] = Field(default=None, pattern=INSTRUMENT_ID_PATTERN)
    source: str = Field(min_length=1)
    ts_event: str
    ts_ingest: str
    ts_publish: str
    quality: QualityContract
    payload: Dict[str, Any]

    @field_validator("ts_event", "ts_ingest", "ts_publish")
    @classmethod
    def utc_timestamp(cls, value: str) -> str:
        return _utc(value)


class QuotePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    bid_price: str = Field(pattern=DECIMAL_PATTERN)
    ask_price: str = Field(pattern=DECIMAL_PATTERN)
    bid_size: str = Field(pattern=DECIMAL_PATTERN)
    ask_size: str = Field(pattern=DECIMAL_PATTERN)


class TradePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    price: str = Field(pattern=DECIMAL_PATTERN)
    size: str = Field(pattern=DECIMAL_PATTERN)
    trade_id: Optional[str] = None


class BookLevel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    price: str = Field(pattern=DECIMAL_PATTERN)
    size: str = Field(pattern=DECIMAL_PATTERN)
    order_count: Optional[int] = Field(default=None, ge=0)


class OrderBookSnapshotPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    depth: int = Field(ge=1)
    bids: list[BookLevel]
    asks: list[BookLevel]


class OrderBookDeltaPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    side: Literal["bid", "ask"]
    action: Literal["add", "update", "delete"]
    level: BookLevel


class StreamStatusPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    state: Literal[
        "connecting", "live", "stale", "degraded", "recovering", "closed"
    ]
    reason_code: Optional[str] = None
    last_sequence: Optional[int] = Field(default=None, ge=0)
    resume_supported: bool


class HeartbeatPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    last_sequence: int = Field(ge=0)
    server_time: str

    @field_validator("server_time")
    @classmethod
    def utc_timestamp(cls, value: str) -> str:
        return _utc(value)


class ErrorPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: str = Field(min_length=1)
    message: str = Field(min_length=1)
    retryable: bool


class BarPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    interval: str
    adjustment: Literal["raw", "adjusted"]
    window_start: str
    window_end: str
    open: str = Field(pattern=DECIMAL_PATTERN)
    high: str = Field(pattern=DECIMAL_PATTERN)
    low: str = Field(pattern=DECIMAL_PATTERN)
    close: str = Field(pattern=DECIMAL_PATTERN)
    volume: str = Field(pattern=DECIMAL_PATTERN)

    @field_validator("window_start", "window_end")
    @classmethod
    def utc_timestamp(cls, value: str) -> str:
        return _utc(value)


class HistoricalManifest(ContractModel):
    dataset_id: str
    snapshot_id: str
    canonical_version: str
    instruments: list[str]
    interval: str
    adjustment: Literal["raw", "adjusted"]
    start: str
    end: str
    row_count: int = Field(ge=0)
    content_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


CONTRACT_SCHEMAS = {
    "instrument": InstrumentContract,
    "event": EventEnvelope,
    "quote": QuotePayload,
    "trade": TradePayload,
    "order_book_snapshot": OrderBookSnapshotPayload,
    "order_book_delta": OrderBookDeltaPayload,
    "bar": BarPayload,
    "stream_status": StreamStatusPayload,
    "heartbeat": HeartbeatPayload,
    "error": ErrorPayload,
}


def canonical_hash(value: Any) -> str:
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        default=str,
    ).encode()
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def validate_instrument_identity(instrument: InstrumentContract) -> None:
    symbol, mic = instrument.instrument_id.rsplit(".", 1)
    if symbol != instrument.symbol or mic != instrument.mic:
        raise ValueError("instrument_id must equal symbol.MIC")
    mappings = list(instrument.provider_symbols.items()) + list(
        instrument.broker_symbols.items()
    )
    if any(not name.strip() or not value.strip() for name, value in mappings):
        raise ValueError("symbol mappings must use non-empty names and values")
