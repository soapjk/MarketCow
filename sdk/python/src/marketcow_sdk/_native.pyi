from collections.abc import Awaitable

from ._facts import (
    FactBtcObservation,
    FactBtcPriceFrame,
    FactBtcTrade,
    FactFactsFinalityResponse,
    FactFactsReadingState,
    FactFactsReferenceResponse,
    FactMarketDeliveryBudget,
    FactMarketMarketBinding,
    FactMarketMarketFrame,
    FactMarketMarketKey,
    FactMarketMarketStreams,
    FactMarketRulesRecord,
    FactMarketTokenMetadata,
    FactWireHistoryObservationPage,
    FactWireHistoryPage,
    FactWireHistoryReleaseReceipt,
)

class NativeError(Exception):
    code: str
    retryable: bool
    page: FactWireHistoryPage | None

class ConnectionEvent:
    kind: str
    reason: str
    retry_after_ms: int
    available: bool

class MarketDirectory:
    fact: FactMarketMarketStreams
    delivery_budget: FactMarketDeliveryBudget | None
    schema_version: str

class Market:
    @staticmethod
    def from_key(
        *, market_id: str, condition_id: str, window_id: str, start_at: str, end_at: str,
        up_token_id: str, down_token_id: str, rules_id: str, rules_revision: int,
    ) -> Market: ...
    @staticmethod
    def from_binding(binding: FactMarketMarketBinding) -> Market: ...
    @staticmethod
    def from_proto_bytes(data: bytes) -> Market: ...
    def proto_bytes(self) -> bytes: ...
    read_only: bool
    binding: FactMarketMarketBinding
    market_id: str | None
    condition_id: str | None
    window_id: str | None
    start_at: str | None
    end_at: str | None
    up_token_id: str | None
    down_token_id: str | None
    rules_id: str | None
    rules_revision: int | None
    role: str
    stream_id: str

class Rules:
    def proto_bytes(self) -> bytes: ...
    @staticmethod
    def from_proto_bytes(data: bytes) -> Rules: ...
    fact: FactMarketRulesRecord
    rules_id: str | None
    revision: int | None
    market_id: str
    description: str
    resolution_source: str
    verification: str | None
    quote_currency: str | None
    price_method: str | None
    averaging_seconds: int | None
    opening_selection: str | None
    closing_selection: str | None
    comparison: str | None
    equality_outcome: str | None

class ReferenceReading:
    fact: FactFactsReadingState
    available: bool
    reason: str | None
    price: str | None
    purpose: str | None
    quote_currency: str | None
    price_method: str | None
    boundary_time_ms: int | None
    supplier_report_time_ms: int | None
    covered_ms: int | None
    baseline_id: str | None

class Reference:
    def proto_bytes(self) -> bytes: ...
    @staticmethod
    def from_proto_bytes(data: bytes) -> Reference: ...
    fact: FactFactsReferenceResponse
    official_opening: ReferenceReading | None
    binance_opening: ReferenceReading | None
    binance_current: ReferenceReading | None
    binance_is_settlement_source: bool
    mapped_official_scale_price: str | None

class Finality:
    def proto_bytes(self) -> bytes: ...
    @staticmethod
    def from_proto_bytes(data: bytes) -> Finality: ...
    fact: FactFactsFinalityResponse
    status: str
    reason: str | None
    policy: str
    durable: bool
    settlement_import_allowed: bool
    valid_provider_count: int
    denominator: str | None
    payouts: list[tuple[str, str]]

class Evidence:
    schema_version: str
    received_at_ns: int | None
    source_generation: str | None
    evidence_id: str
    acquisition: str
    upstream_url: str
    content_type: str
    raw_body: bytes

class MarketEvent:
    binding: FactMarketMarketBinding | None
    key: FactMarketMarketKey | None
    frame: FactMarketMarketFrame | None
    kind: str
    available: bool
    market_id: str | None
    state_sequence: int | None
    stream_id: str | None
    reason: str | None
    up_bid: tuple[str, str] | None
    up_ask: tuple[str, str] | None
    down_bid: tuple[str, str] | None
    down_ask: tuple[str, str] | None
    changes: list[tuple[str, str, str, str]]

class NativeMarketStream:
    def snapshot(self, depth: int | None = None) -> MarketSnapshot | None: ...
    def snapshot_async(self, depth: int | None = None) -> Awaitable[MarketSnapshot | None]: ...
    def next(self) -> MarketEvent | None: ...
    def next_async(self) -> Awaitable[MarketEvent | None]: ...
    def close(self) -> None: ...
    def close_async(self) -> Awaitable[None]: ...

class Trade:
    fact: FactBtcTrade
    @property
    def trade_id(self) -> int: ...
    @property
    def trade_time_ms(self) -> int: ...
    @property
    def event_time_ms(self) -> int | None: ...
    @property
    def price(self) -> str: ...
    @property
    def quantity(self) -> str: ...
    @property
    def buyer_is_maker(self) -> bool | None: ...
    @property
    def received_at_ns(self) -> int | None: ...
    @property
    def evidence_id(self) -> str | None: ...

class Observation:
    fact: FactBtcObservation
    received_at_ns: int | None
    evidence_id: str | None
    @property
    def source_time_ms(self) -> int: ...
    @property
    def price(self) -> str: ...
    @property
    def source_sequence(self) -> int | None: ...
    @property
    def row_index(self) -> int | None: ...

class PriceEvent:
    frame: FactBtcPriceFrame
    phase: int | None
    @property
    def kind(self) -> str: ...
    @property
    def available(self) -> bool: ...
    @property
    def source(self) -> int: ...
    @property
    def instrument(self) -> str: ...
    @property
    def stream_id(self) -> str: ...
    @property
    def stream_sequence(self) -> int: ...
    @property
    def source_sequence(self) -> int | None: ...
    @property
    def published_at_ns(self) -> int: ...
    @property
    def signed_age_ms(self) -> int | None: ...
    @property
    def trade(self) -> Trade | None: ...
    @property
    def trades(self) -> list[Trade]: ...
    @property
    def observations(self) -> list[Observation]: ...
    @property
    def reason(self) -> str | None: ...

class HistoryPage:
    fact: FactWireHistoryPage
    coverage_verified: bool
    coverage_first_time_ms: int | None
    coverage_last_time_ms: int | None
    database_revision: int
    start_time_ms: int | None
    through_trade_id: int | None
    segment_complete: bool
    segment_index: int
    database_state: str | None
    database_reason: str | None
    @property
    def session_id(self) -> str: ...
    @property
    def page_index(self) -> int: ...
    @property
    def page_count(self) -> int: ...
    @property
    def complete(self) -> bool: ...
    @property
    def total_count(self) -> int: ...
    @property
    def predecessor(self) -> Trade | None: ...
    @property
    def trades(self) -> list[Trade]: ...

class NativePriceStream:
    def next(self) -> PriceEvent | ConnectionEvent | None: ...
    def next_async(self) -> Awaitable[PriceEvent | ConnectionEvent | None]: ...
    def close(self) -> None: ...
    def close_async(self) -> Awaitable[None]: ...

class NativeHistory:
    def defer_release(self) -> None: ...
    def lifecycle(self) -> HistoryLifecycle: ...
    def lifecycle_async(self) -> Awaitable[HistoryLifecycle]: ...
    def save_checkpoint(self, path: str) -> None: ...
    def save_checkpoint_async(self, path: str) -> Awaitable[None]: ...
    def next(self) -> HistoryPage | None: ...
    def next_async(self) -> Awaitable[HistoryPage | None]: ...
    def close(self) -> None: ...
    def close_async(self) -> Awaitable[None]: ...

class NativeClient:
    def market_directory(self) -> MarketDirectory: ...
    def market_directory_async(self) -> Awaitable[MarketDirectory]: ...
    def __init__(self, endpoint: str, api_key: str | None = None) -> None: ...
    @property
    def closed(self) -> bool: ...
    def close(self) -> None: ...
    def close_async(self) -> Awaitable[None]: ...
    def connect(self) -> BtcCapabilities: ...
    def connect_async(self) -> Awaitable[BtcCapabilities]: ...
    def usage(self) -> Usage: ...
    def server_usage(self) -> ServerUsage: ...
    def server_usage_async(self) -> Awaitable[ServerUsage]: ...
    def latest_price(self, source: int) -> PriceEvent: ...
    def latest_price_async(self, source: int) -> Awaitable[PriceEvent]: ...
    def terms(self, market: Market) -> PublicTerms: ...
    def terms_async(self, market: Market) -> Awaitable[PublicTerms]: ...
    def official_book(self, market: Market, token_id: str) -> OfficialBook: ...
    def official_book_async(self, market: Market, token_id: str) -> Awaitable[OfficialBook]: ...
    def official_btc_trade(self, source: int) -> OfficialBtcTrade: ...
    def official_btc_trade_async(self, source: int) -> Awaitable[OfficialBtcTrade]: ...
    def markets(self) -> list[Market]: ...
    def markets_async(self) -> Awaitable[list[Market]]: ...
    def subscribe_market(self, market: Market) -> NativeMarketStream: ...
    def subscribe_market_async(self, market: Market) -> Awaitable[NativeMarketStream]: ...
    def follow_market(self) -> NativeMarketStream: ...
    def rules(self, id: str, revision: int) -> Rules: ...
    def rules_async(self, id: str, revision: int) -> Awaitable[Rules]: ...
    def reference(self, market: Market) -> Reference: ...
    def reference_async(self, market: Market) -> Awaitable[Reference]: ...
    def finality(self, market: Market) -> Finality: ...
    def finality_async(self, market: Market) -> Awaitable[Finality]: ...
    def evidence(self, id: str) -> Evidence: ...
    def evidence_async(self, id: str) -> Awaitable[Evidence]: ...
    def subscribe(self, source: int) -> NativePriceStream: ...
    def subscribe_async(self, source: int) -> Awaitable[NativePriceStream]: ...
    def trade_history(
        self, start_time_ms: int, through_trade_id: int, limit: int = 1000
    ) -> NativeHistory: ...
    def resume_history(self, path: str) -> NativeHistory: ...
    def resume_history_async(self, path: str) -> Awaitable[NativeHistory]: ...
    def observation_history(
        self, source: int, start_time_ms: int, end_time_ms: int, limit: int = 1000
    ) -> NativeObservationHistory: ...
    def observation_range(
        self, source: int, start_time_ms: int, end_time_ms: int, limit: int
    ) -> NativeObservationHistory: ...
    def observation_range_async(
        self, source: int, start_time_ms: int, end_time_ms: int, limit: int
    ) -> Awaitable[NativeObservationHistory]: ...
    def btc_capabilities(self) -> BtcCapabilities: ...
    def finalized_bars(self, interval_ms: int) -> FinalizedWindow: ...
    def subscribe_bars(self, interval_ms: int) -> NativeBars: ...
    def finalized_bars_async(self, interval_ms: int) -> Awaitable[FinalizedWindow]: ...
    def btc_capabilities_async(self) -> Awaitable[BtcCapabilities]: ...
    def btc_health(self) -> BtcHealth: ...
    def btc_health_async(self) -> Awaitable[BtcHealth]: ...
    def time_history(
        self, start_time_ms: int, end_time_ms: int | None = None, limit: int = 1000
    ) -> NativeHistory: ...
    def time_history_async(
        self, start_time_ms: int, end_time_ms: int | None = None, limit: int = 1000
    ) -> Awaitable[NativeHistory]: ...

class ServerUsage:
    customer_id: str
    policy_revision: int
    window_started_at_unix_seconds: int
    response_payload_bytes: int
    requests: int
    maximum_response_payload_bytes: int
    quota_window_seconds: int

class Usage:
    http_requests: int
    http_response_bytes: int
    price_message_bytes: int
    market_message_bytes: int
    queued_event_bytes: int

class InstrumentFacts:
    price_increment: str | None
    minimum_order_size: str | None
    size_increment: str | None
    price_unit: str
    size_unit: str
    missing_fields: list[str]

class FeeFacts:
    model: str
    currency: str | None
    maker_rate: str | None
    taker_rate: str | None
    exponent: int | None
    taker_only: bool | None
    quantum: str | None
    rounding_mode: str | None
    effective_at_ns: int | None
    maker_base_fee_bps: str | None
    taker_base_fee_bps: str | None
    missing_fields: list[str]
    applicability: str

class PublicOrderTerms:
    taker_order_delay_enabled: bool | None
    marketable_hold_ms: int | None
    missing_fields: list[str]
    documentation_evidence_ids: list[str]

class ExplicitLocalPolicy:
    policy_id: str | None
    revision: int | None
    origin: str
    purpose: str
    fee_currency: str | None
    fee_quantum: str | None
    rounding_mode: str | None
    size_increment: str | None
    fee_upper_bound_method: str | None

class PublicTerms:
    request_id: str
    source: str
    source_url: str
    observed_at_ns: int | None
    instrument: InstrumentFacts | None
    fees: FeeFacts | None
    order_execution: PublicOrderTerms | None
    explicit_policies: list[ExplicitLocalPolicy]
    unavailable_capabilities: list[str]

class UpstreamExchange:
    url: str
    status: int | None
    content_type: str | None
    retry_after: str | None
    started_at_ns: int | None
    received_at_ns: int | None
    duration_us: int | None
    body_complete: bool

class OfficialBtcTrade:
    request_id: str
    source: int
    instrument_id: str
    acquisition: str
    cache_used: bool
    ws_dependency: bool
    latest_trade: Trade | None
    signed_age_ms_at_receive: int | None
    upstream: UpstreamExchange | None
    evidence_id: str | None

class OfficialBook:
    request_id: str
    condition_id: str | None
    token_id: str | None
    supplier_timestamp_ms: int | None
    bids: list[tuple[str, str]]
    asks: list[tuple[str, str]]
    tick_size: str | None
    min_order_size: str | None
    neg_risk: bool | None
    last_trade_price: str | None
    acquisition: str
    cache_used: bool
    ws_dependency: bool
    upstream: UpstreamExchange | None
    evidence_id: str | None

class FinalizedBar:
    open_time_ms: int
    close_time_ms: int
    open: str
    high: str
    low: str
    close: str
    base_volume: str
    quote_volume: str
    trade_count: int
    taker_buy_base_volume: str
    taker_buy_quote_volume: str
    source_event_time_ms: int | None
    available_at: str | None
    provenance_kind: str | None

class FinalizedWindow:
    after_open_ms: int | None
    gap: bool
    ready: bool
    reason: str | None
    interval_ms: int
    stream_id: str
    checkpoint_sequence: int
    durable_sequence: int
    expected_last_open_ms: int
    bars: list[FinalizedBar]

class NativeBars:
    def next(self) -> FinalizedWindow | None: ...
    def next_async(self) -> Awaitable[FinalizedWindow | None]: ...
    def close(self) -> None: ...
    def close_async(self) -> Awaitable[None]: ...

class SourceCapability:
    source: int
    instrument_id: str
    price_stream: bool
    indexed_history: bool
    collector_present: bool

class BtcCapabilities:
    sources: list[SourceCapability]
    history_page_limit: int
    history_maximum_pages: int
    history_maximum_rows: int
    history_maximum_bytes: int
    history_maximum_span_ms: int
    history_session_ttl_seconds: int
    history_explicit_release: bool
    history_request_identity: bool

class WriterHealth:
    state: str
    reason: str | None
    last_success_sequence: int | None
    last_success_source_time_ms: int | None
    failed_at_ns: int | None
    dropped_records: int

class SourceHealth:
    source: int
    instrument_id: str
    phase: str
    reason: str | None
    stream_id: str | None
    stream_sequence: int | None
    source_sequence: int | None
    latest_source_time_ms: int | None
    signed_source_age_ms: int | None
    database: WriterHealth | None
    archive: WriterHealth | None

class BtcHealth:
    reported_at_ns: int
    sources: list[SourceHealth]

class ObservationRecord:
    observation_id: int
    source_generation: str
    observation: Observation | None

class ObservationPage:
    fact: FactWireHistoryObservationPage
    segment_complete: bool
    segment_index: int
    observations: list[ObservationRecord]
    complete: bool
    coverage_verified: bool
    reason: str | None
    source: int | None
    start_time_ms: int | None
    end_time_ms: int | None
    frozen_high_observation_id: int
    session_id: str
    database_state: str | None

class NativeObservationHistory:
    def next(self) -> ObservationPage | None: ...
    def next_async(self) -> Awaitable[ObservationPage | None]: ...
    def close(self) -> None: ...
    def close_async(self) -> Awaitable[None]: ...

class BookSnapshot:
    metadata: FactMarketTokenMetadata
    token_id: str
    outcome: str
    book_revision: int
    data_valid: bool
    tick_size: str | None
    quote_source_at: str | None
    received_at: str | None
    invalid_reason: str | None
    confirmation_source_at: str | None
    truncated: bool
    bids: list[tuple[str, str]]
    asks: list[tuple[str, str]]

class MarketSnapshot:
    key: FactMarketMarketKey
    market_id: str
    stream_id: str
    state_sequence: int | None
    source_status: str
    reason: str | None
    published_at: str
    books: list[BookSnapshot]

class HistoryLifecycle:
    session_id: str | None
    retrieval_complete: bool
    released: bool
    release_error: str | None
    release_receipt: FactWireHistoryReleaseReceipt | None
