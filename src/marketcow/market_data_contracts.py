from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Annotated, Any, Dict, Literal, Optional, Union

from pydantic import (
    BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator,
)


SCHEMA_VERSION = 1
DECIMAL_PATTERN = r"^-?(?:0|[1-9]\d*)(?:\.\d+)?$"
INSTRUMENT_ID_PATTERN = r"^[A-Z0-9][A-Z0-9.-]{0,31}\.[A-Z0-9]{4}$"
Interval = Literal["1-MINUTE", "5-MINUTE", "15-MINUTE", "30-MINUTE", "1-HOUR", "1-DAY"]
DecimalString = Annotated[str, Field(pattern=DECIMAL_PATTERN)]


def utc(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def number(value: str) -> Decimal:
    return Decimal(value)


def instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ContractModel(StrictModel):
    schema_version: Literal[1] = SCHEMA_VERSION


class InstrumentContract(ContractModel):
    instrument_id: str = Field(pattern=INSTRUMENT_ID_PATTERN)
    instrument_type: Literal["equity"]
    asset_class: Literal["equity"]
    symbol: str = Field(min_length=1, max_length=32)
    market: Literal["US", "HK", "CN"]
    mic: str = Field(pattern=r"^[A-Z0-9]{4}$")
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    price_precision: int = Field(ge=0, le=18)
    size_precision: Literal[0]
    tick_size: DecimalString
    size_increment: Literal["1"]
    lot_size: DecimalString
    ts_event: str
    ts_init: str
    provider_symbols: Dict[str, str] = Field(min_length=1)
    broker_symbols: Dict[str, str] = Field(default_factory=dict)

    @field_validator("tick_size", "lot_size")
    @classmethod
    def positive_decimal(cls, value: str) -> str:
        if number(value) <= 0:
            raise ValueError("precision increments must be positive")
        return value

    @field_validator("ts_event", "ts_init")
    @classmethod
    def timestamps(cls, value: str) -> str:
        return utc(value)

    @model_validator(mode="after")
    def ordered_timestamps(self):
        if instant(self.ts_event) > instant(self.ts_init):
            raise ValueError("instrument ts_event must not be after ts_init")
        return self


class InstrumentRecord(InstrumentContract):
    content_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    updated_at: str

    @field_validator("updated_at")
    @classmethod
    def timestamp(cls, value: str) -> str:
        return utc(value)


class QualityContract(StrictModel):
    status: Literal["live", "delayed", "stale", "degraded", "historical"]
    delayed: bool
    stale: bool
    degraded: bool

    @model_validator(mode="after")
    def consistent(self):
        expected = {
            "live": (False, False, False),
            "historical": (False, False, False),
            "delayed": (True, False, False),
            "stale": (False, True, False),
            "degraded": (False, False, True),
        }
        actual = (self.delayed, self.stale, self.degraded)
        if actual != expected[self.status]:
            raise ValueError("quality status and flags are inconsistent")
        return self


class QuotePayload(StrictModel):
    bid_price: DecimalString
    ask_price: DecimalString
    bid_size: DecimalString
    ask_size: DecimalString

    @model_validator(mode="after")
    def market(self):
        if min(map(number, (self.bid_price, self.ask_price))) <= 0:
            raise ValueError("quote prices must be positive")
        if min(map(number, (self.bid_size, self.ask_size))) < 0:
            raise ValueError("quote sizes must be non-negative")
        if number(self.bid_price) > number(self.ask_price):
            raise ValueError("crossed markets are not valid in v1")
        return self


class TradePayload(StrictModel):
    price: DecimalString
    size: DecimalString
    trade_id: str = Field(min_length=1)
    aggressor_side: Literal["BUYER", "SELLER", "NO_AGGRESSOR"]

    @model_validator(mode="after")
    def positive(self):
        if number(self.price) <= 0 or number(self.size) <= 0:
            raise ValueError("trade price and size must be positive")
        return self


class BookLevel(StrictModel):
    price: DecimalString
    size: DecimalString
    order_id: Literal["0"] = "0"

    @model_validator(mode="after")
    def valid(self):
        if number(self.price) <= 0 or number(self.size) < 0:
            raise ValueError("book price must be positive and size non-negative")
        return self


class OrderBookSnapshotPayload(StrictModel):
    book_type: Literal["L1_MBP"]
    depth: Literal[1]
    baseline_sequence: int = Field(ge=0)
    bids: list[BookLevel] = Field(max_length=1)
    asks: list[BookLevel] = Field(max_length=1)


class OrderBookDeltaPayload(StrictModel):
    book_type: Literal["L1_MBP"]
    baseline_sequence: int = Field(ge=0)
    side: Literal["bid", "ask"]
    action: Literal["add", "update", "delete"]
    level: BookLevel


class BarPayload(StrictModel):
    interval: Interval
    adjustment: Literal["raw", "adjusted"]
    price_type: Literal["LAST", "BID", "ASK", "MID"]
    aggregation_source: Literal["EXTERNAL"]
    window_start: str
    window_end: str
    open: DecimalString
    high: DecimalString
    low: DecimalString
    close: DecimalString
    volume: DecimalString

    @field_validator("window_start", "window_end")
    @classmethod
    def timestamps(cls, value: str) -> str:
        return utc(value)

    @model_validator(mode="after")
    def valid(self):
        if instant(self.window_start) >= instant(self.window_end):
            raise ValueError("bar window_start must be before window_end")
        values = list(map(number, (self.open, self.high, self.low, self.close)))
        if min(values) <= 0 or number(self.high) != max(values):
            raise ValueError("bar OHLC is invalid")
        if number(self.low) != min(values) or number(self.volume) < 0:
            raise ValueError("bar OHLC/volume is invalid")
        return self


class StreamStatusPayload(StrictModel):
    state: Literal["connecting", "live", "stale", "degraded", "recovering", "closed"]
    reason_code: Optional[str] = None
    last_sequence: Optional[int] = Field(default=None, ge=0)
    resume_supported: bool


class HeartbeatPayload(StrictModel):
    last_sequence: int = Field(ge=0)
    server_time: str

    @field_validator("server_time")
    @classmethod
    def timestamp(cls, value: str) -> str:
        return utc(value)


class ErrorPayload(StrictModel):
    code: str = Field(min_length=1)
    message: str = Field(min_length=1)
    retryable: bool


class EnvelopeBase(ContractModel):
    stream_id: str = Field(min_length=1)
    sequence: int = Field(ge=1)
    source: str = Field(min_length=1)
    ts_event: str
    ts_ingest: str
    ts_publish: str
    quality: QualityContract

    @field_validator("ts_event", "ts_ingest", "ts_publish")
    @classmethod
    def timestamps(cls, value: str) -> str:
        return utc(value)

    @model_validator(mode="after")
    def ordered(self):
        if not (
            instant(self.ts_event)
            <= instant(self.ts_ingest)
            <= instant(self.ts_publish)
        ):
            raise ValueError("event timestamps must satisfy event <= ingest <= publish")
        return self


class InstrumentEventBase(EnvelopeBase):
    instrument_id: str = Field(pattern=INSTRUMENT_ID_PATTERN)


class QuoteEvent(InstrumentEventBase):
    event_type: Literal["quote"]
    payload: QuotePayload


class TradeEvent(InstrumentEventBase):
    event_type: Literal["trade"]
    payload: TradePayload


class BookSnapshotEvent(InstrumentEventBase):
    event_type: Literal["order_book_snapshot"]
    payload: OrderBookSnapshotPayload


class BookDeltaEvent(InstrumentEventBase):
    event_type: Literal["order_book_delta"]
    payload: OrderBookDeltaPayload


class BarEvent(InstrumentEventBase):
    event_type: Literal["bar"]
    payload: BarPayload


class StatusEvent(EnvelopeBase):
    event_type: Literal["stream_status"]
    instrument_id: Optional[str] = Field(default=None, pattern=INSTRUMENT_ID_PATTERN)
    payload: StreamStatusPayload


class HeartbeatEvent(EnvelopeBase):
    event_type: Literal["heartbeat"]
    instrument_id: None = None
    payload: HeartbeatPayload


class ErrorEvent(EnvelopeBase):
    event_type: Literal["error"]
    instrument_id: Optional[str] = Field(default=None, pattern=INSTRUMENT_ID_PATTERN)
    payload: ErrorPayload


StreamEvent = Annotated[
    Union[
        QuoteEvent, TradeEvent, BookSnapshotEvent, BookDeltaEvent, BarEvent,
        StatusEvent, HeartbeatEvent, ErrorEvent,
    ],
    Field(discriminator="event_type"),
]
STREAM_EVENT_ADAPTER = TypeAdapter(StreamEvent)


class HistoricalManifest(ContractModel):
    dataset_id: str
    snapshot_id: str
    canonical_version: str
    instruments: list[str]
    interval: Interval
    adjustment: Literal["raw", "adjusted"]
    start: str
    end: str
    end_inclusive: Literal[True] = True
    row_count: int = Field(ge=0)
    content_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @field_validator("start", "end")
    @classmethod
    def timestamps(cls, value: str) -> str:
        return utc(value)

    @model_validator(mode="after")
    def ordered_range(self):
        if instant(self.start) > instant(self.end):
            raise ValueError("manifest start must not be after end")
        return self


class HistoricalBar(ContractModel):
    instrument_id: str = Field(pattern=INSTRUMENT_ID_PATTERN)
    interval: Interval
    adjustment: Literal["raw", "adjusted"]
    price_type: Literal["LAST"]
    aggregation_source: Literal["EXTERNAL"]
    window_start: str
    window_end: str
    ts_event: str
    ts_init: str
    open: DecimalString
    high: DecimalString
    low: DecimalString
    close: DecimalString
    volume: DecimalString
    selected_source: str
    quality_status: str
    row_version: str

    @field_validator("window_start", "window_end", "ts_event", "ts_init")
    @classmethod
    def timestamps(cls, value: str) -> str:
        return utc(value)

    @model_validator(mode="after")
    def valid(self):
        BarPayload(
            interval=self.interval, adjustment=self.adjustment, price_type="LAST",
            aggregation_source="EXTERNAL", window_start=self.window_start,
            window_end=self.window_end, open=self.open, high=self.high, low=self.low,
            close=self.close, volume=self.volume,
        )
        if self.ts_event != self.window_end:
            raise ValueError("historical bar ts_event must equal window_end")
        if instant(self.ts_init) < instant(self.ts_event):
            raise ValueError("historical bar ts_init must not precede ts_event")
        return self


class CanonicalBarPage(ContractModel):
    manifest: HistoricalManifest
    count: int = Field(ge=0)
    bars: list[HistoricalBar]
    page_size: int = Field(ge=1, le=5000)
    next_cursor: Optional[str]
    truncated: bool
    provenance: Dict[str, str]

    @model_validator(mode="after")
    def consistent(self):
        if self.count != len(self.bars):
            raise ValueError("page count must equal bars length")
        if self.count > self.page_size:
            raise ValueError("page count must not exceed page_size")
        if self.truncated != (self.next_cursor is not None):
            raise ValueError("truncated must match next_cursor presence")
        previous = None
        for bar in self.bars:
            if bar.instrument_id not in self.manifest.instruments:
                raise ValueError("bar instrument is outside manifest")
            if (
                bar.interval != self.manifest.interval
                or bar.adjustment != self.manifest.adjustment
            ):
                raise ValueError("bar interval/adjustment must match manifest")
            bar_start = instant(bar.window_start)
            if not instant(self.manifest.start) <= bar_start <= instant(self.manifest.end):
                raise ValueError("bar window_start is outside manifest range")
            position = (bar_start, bar.instrument_id)
            if previous is not None and position <= previous:
                raise ValueError("bars must be strictly ordered without duplicates")
            previous = position
        return self


CONTRACT_SCHEMAS: Dict[str, Any] = {
    "instrument": InstrumentContract,
    "instrument_record": InstrumentRecord,
    "event": STREAM_EVENT_ADAPTER,
    "quote": QuotePayload,
    "trade": TradePayload,
    "order_book_snapshot": OrderBookSnapshotPayload,
    "order_book_delta": OrderBookDeltaPayload,
    "bar": BarPayload,
    "stream_status": StreamStatusPayload,
    "heartbeat": HeartbeatPayload,
    "error": ErrorPayload,
    "historical_manifest": HistoricalManifest,
    "historical_bar": HistoricalBar,
    "canonical_bar_page": CanonicalBarPage,
}


def canonical_hash(value: Any) -> str:
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str,
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
