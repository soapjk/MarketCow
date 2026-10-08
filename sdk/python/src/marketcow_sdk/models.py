from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class SdkModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class HealthResponse(SdkModel):
    status: str
    version: str | None = None
    profile: str | None = None
    storage_health: dict[str, Any] | None = None


class Instrument(SdkModel):
    instrument_id: str
    symbol: str | None = None
    mic: str | None = None
    market: str | None = None
    currency: str | None = None
    provider_symbols: dict[str, str] = Field(default_factory=dict)


class InstrumentSearchResult(SdkModel):
    count: int
    items: list[Instrument]


class InstrumentResolveItem(SdkModel):
    namespace: str
    external_symbol: str
    status: Literal["resolved", "error"]
    instrument_id: str | None = None
    symbol: str | None = None
    mic: str | None = None
    market: str | None = None
    currency: str | None = None
    source: str | None = None
    source_exchange: str | None = None
    observed_at: datetime | None = None
    resolution: Literal["registry", "upstream"] | None = None
    error: dict[str, Any] | None = None


class InstrumentResolveBatch(SdkModel):
    namespace: str
    count: int
    resolved_count: int
    error_count: int
    items: list[InstrumentResolveItem]


class Quote(SdkModel):
    instrument_id: str | None = None
    symbol: str
    market: str | None = None
    price: Decimal
    previous_close: Decimal | None = None
    change: Decimal | None = None
    change_pct: Decimal | None = None
    source: str
    quote_at: datetime | None = None
    observed_at: datetime | None = None
    ingested_at: datetime | None = None
    quality_status: str | None = None
    cached: bool = False
    stale: bool = False
    cache_status: str | None = None


class QuoteBatch(SdkModel):
    count: int
    items: list[Quote]
    errors: list[dict[str, Any]] = Field(default_factory=list)
    routing: dict[str, Any] | None = None


class HistoricalManifest(SdkModel):
    adjustment_contract_version: int = 1
    dataset_id: str
    snapshot_id: str
    canonical_version: str
    instruments: list[str]
    interval: str
    adjustment: Literal["raw", "qfq", "hfq"]
    start: datetime
    end: datetime
    row_count: int
    content_hash: str


class HistoricalBar(SdkModel):
    instrument_id: str
    interval: str
    adjustment: Literal["raw", "qfq", "hfq"]
    window_start: datetime
    window_end: datetime
    ts_event: datetime
    ts_init: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    selected_source: str
    quality_status: str
    row_version: str


class CanonicalBarPage(SdkModel):
    manifest: HistoricalManifest
    count: int
    bars: list[HistoricalBar]
    page_size: int
    next_cursor: str | None
    truncated: bool
    provenance: dict[str, str]


class MarketBarResult(SdkModel):
    count: int | None = None
    items: list[dict[str, Any]] = Field(default_factory=list)


class FxError(SdkModel):
    currency: str
    code: str
    message: str


class FxResponse(SdkModel):
    base: str
    rates: dict[str, Decimal]
    source: str
    source_urls: dict[str, str] = Field(alias="sourceUrls")
    as_of: datetime = Field(alias="asOf")
    fetched_at: datetime = Field(alias="fetchedAt")
    ingested_at: datetime = Field(alias="ingestedAt")
    cached: bool
    stale: bool
    cache_status: str = Field(alias="cacheStatus")
    errors: list[FxError]


class DocumentResponse(SdkModel):
    """Typed envelope for evolving dividend and exposure-fact documents."""
