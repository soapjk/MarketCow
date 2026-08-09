from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import os
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, Literal, Optional
from zoneinfo import ZoneInfo

from fastapi import (
    FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect,
)
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from starlette.responses import (
    FileResponse, HTMLResponse, JSONResponse, Response, StreamingResponse,
)
from starlette.staticfiles import StaticFiles

from . import __version__
from .config import Settings
from .dividends import normalize_dividend_symbol
from .market_bar_cursor import decode_cursor, encode_cursor, load_or_create_secret
from .normalize import normalize_as_of, normalize_report_period
from .service import FundamentalService
from .telemetry import sanitize_text, telemetry_call
from .health import HealthEvaluator
from .history_jobs import HistoryJobManager
from .history_reconciliation import HistoryReconciler
from .history_consistency import HistoryConsistencyAuditor
from .csv_import import (
    CsvImportRequest as CsvImportDeclaration,
    CsvSchemaProfile,
    InstrumentMapping,
)
from .csv_import_service import CsvImportService, create_csv_import_service
from .csv_import_jobs import _safe_error_message
from .csv_import_inference import infer_csv_semantics
from .instruments import canonical_instrument
from .provider_routing import ProviderNotSupported, ProviderRoutingError
from .market_data_contracts import (
    CanonicalBarPage,
    CONTRACT_SCHEMAS,
    HistoricalBar,
    HistoricalManifest,
    InstrumentContract,
    InstrumentRecord,
    SequenceWatermark,
    StreamError,
    StreamHeartbeat,
    SubscriptionAck,
    canonical_hash,
    validate_instrument_identity,
    CLIENT_COMMAND_ADAPTER,
    STREAM_EVENT_ADAPTER,
)
from .realtime import LongPortRealtimeProvider, RealtimeHub
from .hyperliquid_realtime import (
    HyperliquidRealtimeProvider,
    RoutingRealtimeProvider,
)
from .providers.longport_quote import LongPortError
from .providers.yahoo_fx import FxRateError, SUPPORTED_FX_CURRENCIES
from .polymarket_history import PublishedPredictionMarketStore
from .polymarket_contracts import (
    PredictionMarketBootstrap,
    PredictionMarketManifest,
)
from .polymarket_live import (
    LiveBootstrapResponse,
    LiveCheckpoint,
    LiveEventPage,
    LiveGapPage,
    LiveReadHealth,
    LiveSnapshotPage,
    PolymarketLiveReadError,
    PolymarketLiveReadStore,
    LiveStateStore,
    PUBLIC_DATA_KINDS,
    PublicDataPage,
)
from .dashboard_registry import load_dashboard_registry, registry_document
from .admin_control import AdminAuditService
from .http_metrics import RequestMetrics, RequestMetricsMiddleware
from .admin_events import ALLOWED_EVENT_TYPES, AdminEventHub, encode_sse
from .admin_auth import (
    CSRF_COOKIE, SESSION_COOKIE, AdminAuth, AdminSecurityMiddleware,
)
from .mcp_server import (
    SUPPORTED_PROTOCOL_VERSIONS,
    MarketCowClient,
    McpServer,
)


MCP_MAX_REQUEST_BYTES = 1024 * 1024


def normalize_quote_symbol(value: str) -> str:
    return canonical_instrument(value).instrument_id


class TushareRequest(BaseModel):
    params: Dict[str, Any] = Field(default_factory=dict)
    fields: str = ""


class TushareRealtimeRequest(BaseModel):
    ts_code: str


class ProviderPolicy(BaseModel):
    provider: Optional[str] = None
    allow_fallback: bool = False


class CrossMarketPairRequest(BaseModel):
    derivative_instrument_id: str
    underlying_instrument_id: str


class CrossMarketQuery(BaseModel):
    pairs: list[CrossMarketPairRequest] = Field(min_length=1, max_length=50)
    book_depth: int = Field(ge=1, le=20)
    max_age_ms: int = Field(ge=10, le=60_000)
    max_skew_ms: int = Field(ge=0, le=10_000)

    @model_validator(mode="after")
    def supported_depth_and_unique_pairs(self):
        if self.book_depth not in {1, 5, 10, 20}:
            raise ValueError("book_depth must be 1, 5, 10 or 20")
        identities = [
            (pair.derivative_instrument_id, pair.underlying_instrument_id)
            for pair in self.pairs
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("cross-market pairs must be unique")
        return self


class QuoteQuery(ProviderPolicy):
    symbols: list[str] = Field(min_length=1, max_length=20)
    refresh: bool = False


class FxErrorItem(BaseModel):
    currency: str
    code: Literal["provider_unavailable", "no_data", "stale_data"]
    message: str


class FxHttpErrorDetail(BaseModel):
    code: Literal[
        "invalid_currency", "provider_unavailable", "no_data", "stale_data"
    ]
    currency: Optional[str] = None
    message: str


class FxHttpErrorResponse(BaseModel):
    detail: FxHttpErrorDetail


class FxResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    base: str
    rates: Dict[str, float]
    source: str
    source_urls: Dict[str, str] = Field(alias="sourceUrls")
    as_of: str = Field(alias="asOf")
    fetched_at: str = Field(alias="fetchedAt")
    ingested_at: str = Field(alias="ingestedAt")
    cached: bool
    stale: bool
    cache_status: Literal["refreshed", "hit", "stale_if_error"] = Field(
        alias="cacheStatus"
    )
    cache_ttl_seconds: float = Field(alias="cacheTtlSeconds")
    stale_max_seconds: float = Field(alias="staleMaxSeconds")
    errors: list[FxErrorItem]


class InstrumentResolveBatchRequest(BaseModel):
    namespace: str = Field(
        min_length=3, max_length=80,
        pattern=r"^(?:provider|broker):[A-Za-z0-9_-]+$",
    )
    symbols: list[str] = Field(min_length=1, max_length=20)

    @model_validator(mode="after")
    def non_empty_symbols(self):
        if any(not str(symbol or "").strip() for symbol in self.symbols):
            raise ValueError("symbols must not contain empty values")
        return self


class InstrumentResolveError(BaseModel):
    code: Literal["not_found", "ambiguous", "provider_unavailable"]
    message: str


class InstrumentResolveItem(BaseModel):
    namespace: str
    external_symbol: str
    status: Literal["resolved", "error"]
    instrument_id: Optional[str] = None
    symbol: Optional[str] = None
    mic: Optional[str] = None
    market: Optional[str] = None
    currency: Optional[str] = None
    source: Optional[str] = None
    source_exchange: Optional[str] = None
    observed_at: Optional[str] = None
    resolution: Optional[Literal["registry", "upstream"]] = None
    error: Optional[InstrumentResolveError] = None


class InstrumentResolveBatchResponse(BaseModel):
    namespace: str
    count: int
    resolved_count: int
    error_count: int
    items: list[InstrumentResolveItem]


class MarketBarQuery(ProviderPolicy):
    symbols: list[str] = Field(min_length=1, max_length=20)
    range: str = "1y"
    interval: str = "1d"
    adjustment: str = Field(default="qfq", pattern="^(raw|qfq|hfq)$")
    refresh: bool = True
    limit: int = Field(default=500, ge=1, le=5000)


class HistoryJobRequest(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=100)
    provider: str = Field(min_length=1)
    range: str = Field(min_length=1)
    range_start: Optional[datetime] = None
    range_end: Optional[datetime] = None
    interval: str = Field(min_length=1)
    adjustment: str = Field(pattern="^(raw|qfq|hfq)$")
    allow_fallback: bool
    max_concurrency: int = Field(ge=1, le=16)
    max_attempts: int = Field(ge=1, le=10)
    retry_backoff_seconds: float = Field(ge=0, le=60)
    retry_max_backoff_seconds: float = Field(ge=0, le=600)
    retry_jitter_seconds: float = Field(ge=0, le=60)
    retry_budget_seconds: float = Field(ge=0, le=86400)
    canonical_wait_seconds: float = Field(ge=0, le=60)
    idempotency_key: str = Field(min_length=8, max_length=200)

    @model_validator(mode="after")
    def validate_symbols_and_provider(self):
        if (self.range_start is None) != (self.range_end is None):
            raise ValueError("range_start and range_end must be provided together")
        if self.range_start is not None and self.range_end is not None:
            if (
                self.range_start.tzinfo is None
                or self.range_end.tzinfo is None
                or self.range_start >= self.range_end
            ):
                raise ValueError(
                    "history date range must be ordered and timezone-aware"
                )
            self.range = "custom"
        normalized = [
            canonical_instrument(symbol).instrument_id for symbol in self.symbols
        ]
        if any(not symbol for symbol in normalized):
            raise ValueError("symbols must not contain empty values")
        if len(set(normalized)) != len(normalized):
            raise ValueError("symbols must be unique after normalization")
        self.symbols = normalized
        if self.provider == "yahoo_chart":
            self.provider = "yahoo"
        if self.provider not in {"yahoo", "tushare", "hyperliquid"}:
            raise ValueError("unsupported history provider")
        for symbol in normalized:
            instrument = canonical_instrument(symbol)
            if self.provider == "tushare":
                instrument.provider_symbol("provider:tushare")
            elif self.provider == "yahoo":
                instrument.provider_symbol("provider:yahoo")
            elif instrument.mic != "HYPL":
                raise ValueError("Hyperliquid history requires a HYPL instrument")
        return self


class HistoryReconcileRequest(BaseModel):
    dry_run: bool


class AdminSessionRequest(BaseModel):
    token: str = Field(default="", max_length=500)
    username: str = Field(default="", max_length=64)
    password: str = Field(default="", max_length=128)

    @model_validator(mode="after")
    def valid_login_method(self):
        has_token = bool(self.token)
        has_credentials = bool(self.username and self.password)
        if has_token == has_credentials:
            raise ValueError("provide either token or username and password")
        return self


class DividendAnnouncementInput(BaseModel):
    symbol: str
    fiscal_year: int = Field(ge=1990, le=2100)
    amount_per_share: str
    currency: str
    announcement_date: str
    expected_payment_date: Optional[str] = None
    record_date: Optional[str] = None
    ex_date: Optional[str] = None
    payment_date: Optional[str] = None
    date_evidence: Dict[str, Any] = Field(default_factory=dict)
    confirmation_status: str
    event_status: str = "active"
    source_type: str
    source_name: str = ""
    source_url: str = ""
    source_document_id: str = ""
    observed_at: Optional[str] = None
    raw_artifact_id: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)


class DividendIngestRequest(BaseModel):
    announcements: list[DividendAnnouncementInput] = Field(min_length=1, max_length=500)


class DividendQuery(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=50)
    fiscal_year: int = Field(ge=1991, le=2100)


class FundDividendProvenance(BaseModel):
    official: bool
    confirmation_status: Optional[str] = None
    raw_artifact_id: Optional[str] = None
    declared_amount: Optional[str] = None
    declared_unit_count: Optional[str] = None
    declared_unit: Optional[str] = None


class FundDividendEvent(BaseModel):
    event_id: str
    symbol: str
    instrument_id: str
    fund_name: Optional[str] = None
    announcement_date: Optional[str] = None
    record_date: Optional[str] = None
    ex_date: Optional[str] = None
    payment_date: Optional[str] = None
    amount_per_unit: str
    currency: str
    dividend_type: str
    event_status: str
    source_url: Optional[str] = None
    source_category: str
    source_name: Optional[str] = None
    source_document_id: Optional[str] = None
    observed_at: Optional[str] = None
    ingested_at: Optional[str] = None
    provenance: FundDividendProvenance


class FundDividendHistoryResponse(BaseModel):
    schema_version: str
    symbol: str
    instrument_id: str
    fund_name: Optional[str] = None
    asset_type: str
    status: Literal["complete", "incomplete", "no_dividends"]
    date_from: str
    date_to: str
    date_basis: Literal["payment_date"]
    event_count: int
    events: list[FundDividendEvent]
    aggregate: Dict[str, Any]
    coverage: Dict[str, Any]
    freshness: Dict[str, Any]


class FundDividendErrorDetail(BaseModel):
    code: Literal[
        "invalid_request", "unsupported_asset_type", "provider_unavailable"
    ]
    message: str


class FundDividendHttpError(BaseModel):
    detail: FundDividendErrorDetail


class CsvSchemaProfileInput(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    version: str = Field(min_length=1, max_length=50)
    columns: Dict[str, str]
    timezone_name: str = Field(min_length=1, max_length=100)
    timestamp_format: str = Field(default="iso8601", min_length=1, max_length=100)
    encoding: str = Field(default="utf-8-sig", min_length=1, max_length=50)
    delimiter: str = Field(default=",", min_length=1, max_length=1)
    fixed_external_symbol: Optional[str] = None
    allow_extra_columns: bool = True
    defaults: Dict[str, str] = Field(default_factory=dict)
    price_multiplier: str = "1"
    volume_multiplier: str = "1"
    amount_multiplier: str = "1"
    price_precision: Optional[int] = Field(default=None, ge=0, le=18)
    volume_precision: Optional[int] = Field(default=None, ge=0, le=18)
    amount_precision: Optional[int] = Field(default=None, ge=0, le=18)
    trading_sessions: list[Dict[str, Any]] = Field(default_factory=list)


class CsvInstrumentMappingInput(BaseModel):
    namespace: str = Field(min_length=3, max_length=100)
    symbols: Dict[str, str] = Field(min_length=1)


class CsvImportDeclarationInput(BaseModel):
    source: str = Field(min_length=1, max_length=100)
    interval: str = Field(min_length=1, max_length=10)
    adjustment: str = Field(pattern="^(raw|qfq|hfq)$")
    profile: CsvSchemaProfileInput
    instruments: CsvInstrumentMappingInput
    created_by: str = Field(default="api-operator", min_length=1, max_length=200)
    source_proof: str = Field(
        default="operator-declared", min_length=1, max_length=1000
    )
    retention_policy: str = Field(
        default="retain-until-explicit-deletion", min_length=1, max_length=200
    )

    def declaration(self) -> CsvImportDeclaration:
        return CsvImportDeclaration(
            source=self.source,
            interval=self.interval,
            adjustment=self.adjustment,
            profile=CsvSchemaProfile(**self.profile.model_dump()),
            instruments=InstrumentMapping(
                self.instruments.namespace, self.instruments.symbols
            ),
            created_by=self.created_by,
            source_proof=self.source_proof,
            retention_policy=self.retention_policy,
        )


class CsvImportSourceInput(BaseModel):
    path: Optional[str] = Field(default=None, min_length=1, max_length=4096)
    upload_id: Optional[str] = Field(
        default=None, pattern=r"^[0-9a-f]{32}$"
    )

    @model_validator(mode="after")
    def one_csv_source(self):
        if bool(self.path) == bool(self.upload_id):
            raise ValueError("provide exactly one of path or upload_id")
        return self


class CsvImportDryRunInput(CsvImportSourceInput):
    declaration: CsvImportDeclarationInput
    max_error_samples: int = Field(default=100, ge=0, le=1000)


class CsvImportCreateInput(CsvImportSourceInput):
    declaration: CsvImportDeclarationInput
    idempotency_key: str = Field(min_length=8, max_length=200)
    chunk_rows: int = Field(default=100000, ge=1000, le=1000000)
    max_attempts: int = Field(default=3, ge=1, le=10)


class CsvImportInferenceInput(BaseModel):
    upload_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    instrument_id: str = Field(min_length=6, max_length=40)
    columns: Dict[str, str]
    timestamp_format: str = Field(default="iso8601", min_length=1, max_length=100)
    encoding: str = Field(default="utf-8-sig", min_length=1, max_length=50)
    delimiter: str = Field(default=",", min_length=1, max_length=1)


class CsvImportRetryInput(BaseModel):
    idempotency_key: str = Field(min_length=8, max_length=200)


def create_app(
    settings: Optional[Settings] = None,
    service: Optional[FundamentalService] = None,
    now_provider: Optional[Callable[[], datetime]] = None,
    realtime_hub: Optional[RealtimeHub] = None,
    mcp_server: Optional[McpServer] = None,
) -> FastAPI:
    settings = settings or Settings.from_env()
    service = service or FundamentalService(settings)
    app = FastAPI(title="MarketCow", version=__version__)
    admin_auth = AdminAuth(
        settings.admin_auth_required,
        settings.admin_tokens_json,
        settings.admin_session_seconds,
        users_json=settings.admin_users_json,
        service_accounts_json=settings.service_accounts_json,
    )
    app.add_middleware(
        AdminSecurityMiddleware,
        auth=admin_auth,
        commands_enabled=settings.admin_commands_enabled,
    )
    app.state.admin_auth = admin_auth
    admin_events = AdminEventHub(
        replay_capacity=min(settings.realtime_replay_capacity, 100000),
        subscriber_capacity=min(settings.realtime_queue_capacity, 10000),
        heartbeat_seconds=max(1.0, settings.realtime_heartbeat_seconds),
        max_subscribers=settings.admin_live_max_connections,
    )
    request_metrics = RequestMetrics()
    app.add_middleware(
        RequestMetricsMiddleware,
        metrics=request_metrics,
        event_sink=admin_events.request_completed,
    )
    app.state.request_metrics = request_metrics
    app.state.admin_events = admin_events
    app.state.service = service
    polymarket_store = PublishedPredictionMarketStore(
        settings.storage_root / "prediction-markets" / "polymarket"
    )
    app.state.polymarket_store = polymarket_store
    polymarket_live = LiveStateStore(
        settings.storage_root / "prediction-markets" / "polymarket-live"
    )
    app.state.polymarket_live = polymarket_live
    polymarket_live_read = PolymarketLiveReadStore(polymarket_live.root)
    app.state.polymarket_live_read = polymarket_live_read
    history_repository = getattr(service, "metadata_repository", None)
    history_manager = None
    if history_repository is not None and all(hasattr(history_repository, name) for name in (
        "get_or_create_history_job", "upsert_history_job", "upsert_history_item",
        "get_history_job",
        "list_history_jobs", "list_recoverable_history_jobs", "list_history_items",
        "claim_history_item", "renew_history_item_lease",
        "finish_claimed_history_item", "release_history_item_lease",
        "upsert_history_shard", "list_history_shards",
        "upsert_history_canonical_check",
        "list_pending_history_canonical_checks",
        "list_history_canonical_checks",
    )):
        history_manager = HistoryJobManager(
            service, history_repository,
            max_workers=getattr(settings, "history_job_max_workers", 4),
            lease_seconds=getattr(settings, "history_job_lease_seconds", 30),
        )
    app.state.history_job_manager = history_manager
    history_reconciler = None
    history_consistency_auditor = None
    market_bar_repository = getattr(service, "market_bar_repository", None)
    artifact_store = getattr(service, "artifact_store", None)
    if history_repository is not None and market_bar_repository is not None:
        if all(hasattr(history_repository, name) for name in (
            "reconcile_history_shard", "reconcile_history_item",
            "list_history_shards", "list_history_items", "get_history_job",
        )) and hasattr(market_bar_repository, "get_raw_ingestion_receipt"):
            history_reconciler = HistoryReconciler(
                history_repository, market_bar_repository,
                getattr(service, "telemetry", None),
            )
        if (
            artifact_store is not None
            and hasattr(history_repository, "list_all_history_shards")
            and hasattr(market_bar_repository, "list_raw_ingestion_receipts")
            and hasattr(artifact_store, "list_artifacts")
        ):
            history_consistency_auditor = HistoryConsistencyAuditor(
                history_repository, market_bar_repository, artifact_store
            )
    app.state.history_reconciler = history_reconciler
    app.state.history_consistency_auditor = history_consistency_auditor
    csv_import_service = None
    if (
        history_repository is not None
        and market_bar_repository is not None
        and artifact_store is not None
        and all(hasattr(history_repository, name) for name in (
            "get_or_create_csv_import_job", "get_csv_import_job",
            "list_csv_import_jobs", "list_recoverable_csv_import_jobs",
            "list_csv_import_shards", "claim_csv_import_shard",
            "renew_csv_import_shard_lease",
            "finish_claimed_csv_import_shard", "update_csv_import_job",
            "request_cancel_csv_import_job",
            "cancel_unclaimed_csv_import_shards",
        ))
        and all(hasattr(market_bar_repository, name) for name in (
            "upsert_price_bars", "get_raw_ingestion_receipt",
            "get_canonical_ingestion_coverage",
        ))
    ):
        csv_import_service = create_csv_import_service(settings, service)
    app.state.csv_import_service = csv_import_service
    clock = now_provider or (lambda: datetime.now(timezone.utc))
    longport_realtime = LongPortRealtimeProvider(
        settings.longport_app_key, settings.longport_app_secret,
        settings.longport_access_token,
        enable_overnight=settings.longport_enable_overnight,
    )
    provider = RoutingRealtimeProvider(
        longport_realtime,
        HyperliquidRealtimeProvider(settings.hyperliquid_base_url),
    )
    metadata_repository = getattr(service, "metadata_repository", None)
    instrument_lookup = (
        metadata_repository.get_instrument
        if metadata_repository is not None
        else lambda _instrument_id: None
    )

    async def persist_realtime_bar(event: dict[str, Any]) -> None:
        instrument = instrument_lookup(event["instrument_id"])
        if instrument is None:
            raise ValueError("realtime bar instrument is unavailable")
        payload = event["payload"]
        bar = {
            "bar_at": payload["window_start"],
            "open": float(payload["open"]), "high": float(payload["high"]),
            "low": float(payload["low"]), "close": float(payload["close"]),
            "volume": float(payload["volume"]), "amount": None,
            "observed_at": payload["window_end"],
        }
        await asyncio.to_thread(
            service.market_bar_repository.upsert_price_bars,
            instrument["symbol"], "1m", "raw", event["source"],
            clock().astimezone(timezone.utc).isoformat(), [bar],
            {"stream_id": app.state.realtime_hub.stream_id},
        )

    hub = realtime_hub or RealtimeHub(
        instrument_lookup, provider,
        queue_capacity=settings.realtime_queue_capacity,
        replay_capacity=settings.realtime_replay_capacity,
        clock=clock, persist_bar=persist_realtime_bar,
    )
    app.state.realtime_hub = hub
    app.state.dashboard_registry = load_dashboard_registry(settings.dashboard_registry_json)
    admin_audit = AdminAuditService(metadata_repository)
    app.state.admin_audit = admin_audit
    owned_mcp_client = None
    if settings.mcp_enabled:
        if mcp_server is None:
            runtime_host = os.environ.get("MARKETCOW_RUNTIME_HOST", settings.host)
            if runtime_host in {"0.0.0.0", "::"}:
                runtime_host = "127.0.0.1"
            if ":" in runtime_host and not runtime_host.startswith("["):
                runtime_host = f"[{runtime_host}]"
            runtime_port = int(os.environ.get(
                "MARKETCOW_RUNTIME_PORT", str(settings.port)
            ))
            owned_mcp_client = MarketCowClient(
                base_url=f"http://{runtime_host}:{runtime_port}"
            )
            mcp_server = McpServer(owned_mcp_client)
        app.state.mcp_server = mcp_server
    else:
        app.state.mcp_server = None

    def authenticated_actor(request: Request) -> str:
        identity = request.scope.get("admin_identity")
        return getattr(identity, "actor", "local-development")

    async def shutdown() -> None:
        try:
            await hub.close()
        finally:
            if history_manager is not None:
                history_manager.close()
            if csv_import_service is not None:
                csv_import_service.close()
            if owned_mcp_client is not None:
                owned_mcp_client.close()
            service.close()

    app.add_event_handler("shutdown", shutdown)
    health_evaluator = HealthEvaluator(wall_clock=clock)

    @app.websocket("/v1/market-data/stream")
    async def market_data_stream(websocket: WebSocket):
        def error_frame(message: Any, code: str, detail: str, retryable: bool = False):
            return StreamError(
                type="error", request_id=(
                    message.get("request_id") if isinstance(message, dict) else None
                ),
                stream_id=hub.stream_id, code=code,
                message=detail[:300], retryable=retryable,
            ).model_dump(mode="json")

        def validate_outbound(frame: dict[str, Any]) -> dict[str, Any]:
            if "event_type" in frame:
                return STREAM_EVENT_ADAPTER.validate_python(frame).model_dump(mode="json")
            models = {
                "ack": SubscriptionAck,
                "heartbeat": StreamHeartbeat,
                "error": StreamError,
                "sequence_watermark": SequenceWatermark,
            }
            return models[frame["type"]].model_validate(frame).model_dump(mode="json")

        await websocket.accept()
        client = hub.new_client()
        receive = asyncio.create_task(websocket.receive_json())
        outgoing = asyncio.create_task(client.queue.get())
        try:
            while True:
                if client.closed_reason is not None:
                    await websocket.close(code=1013, reason=client.closed_reason)
                    return
                done, _pending = await asyncio.wait(
                    {receive, outgoing},
                    timeout=settings.realtime_heartbeat_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    await websocket.send_json(validate_outbound(hub.heartbeat()))
                    continue
                if outgoing in done:
                    await websocket.send_json(validate_outbound(outgoing.result()))
                    outgoing = asyncio.create_task(client.queue.get())
                if receive not in done:
                    continue
                try:
                    message = receive.result()
                except WebSocketDisconnect:
                    raise
                except Exception as exc:
                    await websocket.send_json(validate_outbound(error_frame(
                        None, "invalid_json", f"invalid JSON frame: {type(exc).__name__}"
                    )))
                    receive = asyncio.create_task(websocket.receive_json())
                    continue
                receive = asyncio.create_task(websocket.receive_json())
                try:
                    command = CLIENT_COMMAND_ADAPTER.validate_python(message)
                    response = (
                        await hub.subscribe(client, command)
                        if command.type == "subscribe"
                        else await hub.unsubscribe(client, command)
                    )
                except (ValidationError, ValueError, RuntimeError, LongPortError) as exc:
                    code = "invalid_request"
                    if str(exc) == "gap_unrecoverable":
                        code = "gap_unrecoverable"
                    elif str(exc) == "replay_too_large":
                        code = "replay_too_large"
                    elif isinstance(exc, LongPortError):
                        code = "provider_unavailable"
                    response = error_frame(
                        message, code, str(exc), code == "provider_unavailable"
                    )
                await websocket.send_json(validate_outbound(response))
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        finally:
            for task in (receive, outgoing):
                task.cancel()
            await asyncio.gather(receive, outgoing, return_exceptions=True)
            await hub.remove_client(client)

    def storage_health() -> Dict[str, Any]:
        resources = getattr(service, "online_resources", None)
        try:
            snapshot = resources.health_snapshot() if resources is not None else None
        except Exception:
            snapshot = None
        return health_evaluator.evaluate(snapshot)

    def database_identifier() -> str:
        return (
            f"postgresql://{sanitize_text(settings.postgres_schema)}+"
            f"clickhouse://{sanitize_text(settings.clickhouse_database)}"
        )

    if settings.mcp_enabled:
        @app.post("/mcp", include_in_schema=False)
        async def mcp_endpoint(request: Request):
            if request.headers.get("origin"):
                return JSONResponse(
                    {"error": "browser origins are not accepted by the MCP endpoint"},
                    status_code=403,
                )
            content_type = request.headers.get("content-type", "").split(";", 1)[0]
            if content_type.lower() != "application/json":
                return JSONResponse(
                    {"error": "Content-Type must be application/json"},
                    status_code=415,
                )
            content_length = request.headers.get("content-length")
            if content_length:
                try:
                    if int(content_length) > MCP_MAX_REQUEST_BYTES:
                        return JSONResponse(
                            {"error": "MCP request exceeds 1 MiB"},
                            status_code=413,
                        )
                except ValueError:
                    return JSONResponse(
                        {"error": "invalid Content-Length"},
                        status_code=400,
                    )
            protocol = request.headers.get("mcp-protocol-version")
            if protocol and protocol not in SUPPORTED_PROTOCOL_VERSIONS:
                return JSONResponse(
                    {"error": "unsupported MCP protocol version"},
                    status_code=400,
                )
            body = await request.body()
            if len(body) > MCP_MAX_REQUEST_BYTES:
                return JSONResponse(
                    {"error": "MCP request exceeds 1 MiB"},
                    status_code=413,
                )
            result = await asyncio.to_thread(
                app.state.mcp_server.handle_json, body
            )
            if result is None:
                return Response(status_code=202)
            return JSONResponse(result)

    def cache_metadata(
        bars: list[Dict[str, Any]], fallback_ingested_at: Any = None,
        reason: str = "",
    ) -> Dict[str, Any]:
        served = clock()
        if served.tzinfo is None:
            served = served.replace(tzinfo=timezone.utc)
        served = served.astimezone(timezone.utc)
        candidates = [row.get("ingested_at") for row in bars if row.get("ingested_at")]
        if fallback_ingested_at:
            candidates.append(fallback_ingested_at)
        newest = None
        for value in candidates:
            parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
                str(value).replace("Z", "+00:00")
            )
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            parsed = parsed.astimezone(timezone.utc)
            newest = parsed if newest is None or parsed > newest else newest
        age = None if newest is None else max(0.0, (served - newest).total_seconds())
        status = "empty" if not bars else (
            "fresh" if age is not None and
            age <= settings.market_bar_cache_freshness_seconds else "stale"
        )
        result: Dict[str, Any] = {
            "cache_status": status,
            "newest_ingested_at": None if newest is None else newest.isoformat(),
            "cache_age_seconds": age,
            "served_at": served.isoformat(),
            "cache_freshness_seconds": settings.market_bar_cache_freshness_seconds,
        }
        if reason:
            result["cache_reason"] = reason[:1000]
        repository = getattr(service, "market_bar_repository", None)
        telemetry = getattr(repository, "telemetry", None)
        if telemetry is not None:
            telemetry_call(
                telemetry, "safe",
                "histogram", "cache_age_seconds", 0.0 if age is None else age,
                status=status,
            )
        return result

    def parse_as_of(value: str) -> str:
        if not value:
            return ""
        try:
            return normalize_as_of(value)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    def calendar_range(
        date_from: str, date_to: str, days: int = 30, include_past: bool = False
    ) -> tuple[str, str]:
        today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        try:
            start = date.fromisoformat(date_from) if date_from else today
            end = date.fromisoformat(date_to) if date_to else today + timedelta(days=days)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="calendar dates must use YYYY-MM-DD") from exc
        if start > end:
            raise HTTPException(status_code=400, detail="from must be on or before to")
        if not include_past and start < today:
            start = today
        return start.isoformat(), end.isoformat()

    @app.get("/v1/health")
    def health():
        return {
            "status": "ok",
            "version": __version__,
            "profile": settings.profile,
            "database": database_identifier(),
            "metadata_backend": "postgresql",
            "mcp": {
                "enabled": settings.mcp_enabled,
                "endpoint": "/mcp" if settings.mcp_enabled else None,
            },
            "storage_health": storage_health(),
        }

    @app.post("/v1/auth/session")
    def create_admin_session(credentials: AdminSessionRequest):
        try:
            if credentials.token:
                session_id, identity = admin_auth.login(credentials.token)
            else:
                session_id, identity = admin_auth.login_credentials(
                    credentials.username, credentials.password
                )
        except PermissionError as exc:
            raise HTTPException(
                status_code=401, detail={"code": "invalid_credentials"}
            ) from exc
        response = JSONResponse({
            "authenticated": True, "actor": identity.actor, "role": identity.role,
        })
        response.set_cookie(
            SESSION_COOKIE, session_id, max_age=settings.admin_session_seconds,
            httponly=True, secure=settings.admin_cookie_secure,
            samesite="strict", path="/",
        )
        response.set_cookie(
            CSRF_COOKIE, identity.csrf, max_age=settings.admin_session_seconds,
            httponly=False, secure=settings.admin_cookie_secure,
            samesite="strict", path="/",
        )
        return response

    @app.get("/v1/auth/session")
    def get_admin_session(request: Request):
        identity = admin_auth.authenticate(request.headers, request.cookies)
        if identity is None:
            raise HTTPException(
                status_code=401, detail={"code": "authentication_required"}
            )
        return {
            "authenticated": True, "actor": identity.actor, "role": identity.role,
        }

    @app.delete("/v1/auth/session")
    def delete_admin_session(request: Request):
        identity = admin_auth.authenticate(request.headers, request.cookies)
        if identity is not None and not admin_auth.csrf_valid(identity, request.headers):
            raise HTTPException(status_code=403, detail={"code": "csrf_validation_failed"})
        admin_auth.logout(request.cookies.get(SESSION_COOKIE, ""))
        response = Response(status_code=204)
        response.delete_cookie(SESSION_COOKIE, path="/")
        response.delete_cookie(CSRF_COOKIE, path="/")
        return response

    @app.get("/metrics", include_in_schema=False)
    def prometheus_metrics():
        return Response(
            request_metrics.render(),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    @app.get("/v1/admin/events")
    async def admin_event_stream(
        types: str = Query("request.summary", max_length=300),
        after_sequence: int = Query(0, ge=0),
        last_event_id: str = Header("", alias="Last-Event-ID", max_length=30),
    ):
        if not settings.admin_live_enabled:
            raise HTTPException(
                status_code=503, detail={"code": "admin_live_disabled"}
            )
        if admin_events.subscriber_count >= admin_events.max_subscribers:
            raise HTTPException(
                status_code=503, detail={"code": "admin_live_connection_limit"}
            )
        if last_event_id:
            try:
                after_sequence = max(after_sequence, int(last_event_id))
            except ValueError as exc:
                raise HTTPException(
                    status_code=400, detail="Last-Event-ID must be an integer"
                ) from exc
        selected = tuple(item.strip() for item in types.split(",") if item.strip())
        subscribable = ALLOWED_EVENT_TYPES - {"stream.heartbeat", "stream.gap"}
        if not selected or not set(selected) <= subscribable:
            raise HTTPException(
                status_code=400, detail="admin event subscription is invalid"
            )

        async def events():
            yield b"retry: 1000\n\n"
            async for event in admin_events.stream(after_sequence, selected):
                yield encode_sse(event)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-store",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/v1/admin/dashboards")
    def admin_dashboards():
        values = app.state.dashboard_registry if settings.admin_grafana_enabled else ()
        return registry_document(values)

    @app.get("/v1/admin/capabilities")
    def admin_capabilities():
        return {
            "schema": "marketcow.admin-capabilities.v1",
            "features": {
                "frontend": settings.admin_frontend_enabled,
                "grafana": settings.admin_grafana_enabled,
                "commands": settings.admin_commands_enabled,
                "live": settings.admin_live_enabled,
            },
        }

    @app.get("/v1/admin/overview")
    def admin_overview():
        providers = (
            service.metadata_repository.provider_health()
            if getattr(service, "metadata_repository", None) is not None
            else []
        )
        jobs = history_manager.list(10) if history_manager is not None else []
        return {
            "schema": "marketcow.admin-overview.v1",
            "generated_at": clock().astimezone(timezone.utc).isoformat(),
            "service": {
                "status": "ok", "version": __version__, "profile": settings.profile,
            },
            "storage": storage_health(),
            "providers": {
                "total": len(providers),
                "healthy": sum(str(item.get("status", "")).lower() == "ok" for item in providers),
                "items": providers[:20],
            },
            "history_jobs": {"items": jobs},
        }

    @app.get("/v1/admin/providers")
    def admin_providers(
        status: str = Query("", pattern="^(|ok|error)$"),
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0, le=10000),
    ):
        rows = list(service.metadata_repository.provider_health())
        normalized = []
        for row in rows:
            item = dict(row)
            item["status"] = str(item.get("status", "")).lower()
            item["configured"] = None
            item.pop("credentials", None)
            normalized.append(item)
        if status:
            normalized = [item for item in normalized if item["status"] == status]
        page = normalized[offset:offset + limit]
        return {
            "schema": "marketcow.admin-providers.v1",
            "items": page,
            "page": {
                "limit": limit, "offset": offset, "returned": len(page),
                "total": len(normalized),
            },
        }

    @app.get("/v1/admin/audit")
    def admin_audit_events(
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0, le=10000),
        action: str = Query("", max_length=120),
        outcome: str = Query("", pattern="^(|accepted|succeeded|rejected|failed)$"),
    ):
        return admin_audit.list(
            limit=limit, offset=offset, action=action, outcome=outcome
        )

    @app.get("/v1/readiness")
    def readiness():
        result = storage_health()
        if not result["ready"]:
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=503, content=result)
        return result

    def instrument_record(row):
        def decode_database_value(value):
            if isinstance(value, bytes):
                return value.decode("utf-8")
            if isinstance(value, dict):
                return {
                    decode_database_value(key): decode_database_value(item)
                    for key, item in value.items()
                }
            if isinstance(value, (list, tuple)):
                return [decode_database_value(item) for item in value]
            return value

        payload = {
            field: decode_database_value(row[field])
            for field in InstrumentRecord.model_fields
        }
        for field in ("tick_size", "size_increment", "lot_size"):
            payload[field] = format(Decimal(str(payload[field])), "f")
        for field in ("ts_event", "ts_init", "updated_at"):
            if isinstance(payload[field], datetime):
                payload[field] = payload[field].isoformat()
        return InstrumentRecord.model_validate(payload).model_dump(mode="json")

    @app.get("/v1/schemas/{contract_name}")
    def contract_schema(contract_name: str):
        model = CONTRACT_SCHEMAS.get(contract_name)
        if model is None:
            raise HTTPException(status_code=404, detail={
                "code": "unknown_contract", "contract_name": contract_name,
            })
        return {
            "schema_version": 1, "contract_name": contract_name,
            "json_schema": (
                model.json_schema() if hasattr(model, "json_schema")
                else model.model_json_schema()
            ),
        }

    @app.post("/v1/admin/instruments/hyperliquid/refresh")
    def refresh_hyperliquid_instruments():
        try:
            return service.refresh_hyperliquid_instruments()
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail={"code": "hyperliquid_refresh_failed", "message": sanitize_text(exc)},
            ) from exc

    @app.get("/v1/hyperliquid/{symbol}/funding-history")
    def hyperliquid_funding_history(symbol: str, start: str, end: str):
        try:
            start_at = datetime.fromisoformat(start.replace("Z", "+00:00"))
            end_at = datetime.fromisoformat(end.replace("Z", "+00:00"))
            return service.refresh_hyperliquid_funding(
                normalize_quote_symbol(symbol), start_at, end_at
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail={
                    "code": "hyperliquid_funding_failed",
                    "message": sanitize_text(exc),
                },
            ) from exc

    @app.put("/v1/admin/instruments/{instrument_id}")
    def upsert_instrument(instrument_id: str, request: InstrumentContract):
        try:
            if request.instrument_id != instrument_id:
                raise ValueError("path and payload instrument_id must match")
            validate_instrument_identity(request)
            payload = request.model_dump(mode="json")
            row = {
                **payload, "content_hash": canonical_hash(payload),
                "updated_at": clock().astimezone(timezone.utc).isoformat(),
            }
            saved = service.metadata_repository.upsert_instrument(row)
            return InstrumentRecord.model_validate({
                **payload, "content_hash": saved["content_hash"],
                "updated_at": row["updated_at"],
            }).model_dump(mode="json")
        except ValueError as exc:
            raise HTTPException(status_code=409, detail={
                "code": "instrument_conflict", "message": str(exc),
            }) from exc

    # Register literal instrument subpaths before the catch-all instrument id.
    # Starlette resolves routes in declaration order.
    @app.get("/v1/instruments/search")
    def instrument_search(q: str, limit: int = Query(12, ge=1, le=30)):
        query = q.strip()
        if not query:
            return {"count": 0, "items": []}
        try:
            items = service.search_instruments(query, limit)
            return {"count": len(items), "items": items}
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get(
        "/v1/prediction-markets/polymarket/datasets/{dataset_id}/manifest",
        response_model=PredictionMarketManifest,
        summary="Read a certified Polymarket dataset manifest",
    )
    def polymarket_certified_manifest(dataset_id: str):
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,200}", dataset_id):
            raise HTTPException(status_code=400, detail={
                "code": "invalid_dataset_id",
                "message": "dataset_id must be a safe stable identifier",
            })
        try:
            return polymarket_store.manifest(dataset_id).model_dump(mode="json")
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail={
                "code": "certified_dataset_not_found",
                "dataset_id": dataset_id,
            }) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail={
                "code": "certified_dataset_integrity_failed",
                "message": str(exc),
            }) from exc

    def _raise_polymarket_read_error(exc: PolymarketLiveReadError) -> None:
        raise HTTPException(status_code=exc.status_code, detail={
            "code": exc.code,
            "message": str(exc),
        }) from exc

    def _require_polymarket_scope(market_id: list[str] | None) -> list[str]:
        if not market_id:
            raise HTTPException(status_code=409, detail={
                "code": "polymarket_full_universe_disabled",
                "message": "Use one or more market_id values for bounded reads",
            })
        return market_id

    @app.get(
        "/v1/prediction-markets/polymarket/datasets/{dataset_id}/bootstrap",
        response_model=PredictionMarketBootstrap,
        summary="Read typed bootstrap for a certified Polymarket dataset",
    )
    def polymarket_certified_bootstrap(dataset_id: str):
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,200}", dataset_id):
            raise HTTPException(status_code=400, detail={
                "code": "invalid_dataset_id",
                "message": "dataset_id must be a safe stable identifier",
            })
        try:
            return polymarket_store.bootstrap(dataset_id).model_dump(mode="json")
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail={
                "code": "certified_dataset_bootstrap_not_found",
                "dataset_id": dataset_id,
            }) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail={
                "code": "certified_dataset_integrity_failed",
                "message": str(exc),
            }) from exc

    @app.get(
        "/v1/prediction-markets/polymarket/datasets/{dataset_id}/parts/{table}",
        response_class=FileResponse,
        summary="Read one immutable certified Polymarket Parquet part",
    )
    def polymarket_certified_part(dataset_id: str, table: str):
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,200}", dataset_id):
            raise HTTPException(status_code=400, detail={
                "code": "invalid_dataset_id",
            })
        if table not in {"catalog", "lifecycle", "books", "trades", "onchain"}:
            raise HTTPException(status_code=400, detail={
                "code": "invalid_dataset_table",
            })
        try:
            path = polymarket_store.part(dataset_id, table)
            return FileResponse(
                path, media_type="application/vnd.apache.parquet",
                filename=path.name,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail={
                "code": "certified_dataset_part_not_found",
                "dataset_id": dataset_id, "table": table,
            }) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail={
                "code": "certified_dataset_integrity_failed",
                "message": str(exc),
            }) from exc

    @app.get(
        "/v1/prediction-markets/polymarket/live/bootstrap",
        response_model=LiveBootstrapResponse,
        summary="Read the provider-neutral Polymarket live catalog and resume contract",
    )
    def polymarket_live_bootstrap(
        market_id: list[str] | None = Query(default=None),
    ):
        try:
            return polymarket_live_read.bootstrap(
                _require_polymarket_scope(market_id)
            ).model_dump(mode="json")
        except PolymarketLiveReadError as exc:
            _raise_polymarket_read_error(exc)

    @app.get(
        "/v1/prediction-markets/polymarket/live/snapshot",
        response_model=LiveSnapshotPage,
        summary="Read consistent fail-closed market frames",
    )
    def polymarket_live_snapshot(
        market_id: list[str] | None = Query(default=None),
    ):
        _require_polymarket_scope(market_id)
        raise HTTPException(status_code=503, detail={
            "code": "polymarket_latest_state_index_unavailable",
            "message": "Scoped snapshots require the durable latest-state index",
        })

    @app.get(
        "/v1/prediction-markets/polymarket/live/events",
        response_model=LiveEventPage,
        summary="Resume provider-neutral Polymarket live events after a cursor",
    )
    def polymarket_live_events(
        after_cursor: int = Query(0, ge=0),
        limit: int = Query(1000, ge=1, le=10000),
        market_id: list[str] | None = Query(default=None),
    ):
        _require_polymarket_scope(market_id)
        raise HTTPException(status_code=503, detail={
            "code": "polymarket_event_index_unavailable",
            "message": "Scoped event resume requires the durable event-offset index",
        })

    @app.get(
        "/v1/prediction-markets/polymarket/live/checkpoint",
        response_model=LiveCheckpoint,
        summary="Read a content-addressed Polymarket live checkpoint",
    )
    def polymarket_live_checkpoint():
        raise HTTPException(status_code=503, detail={
            "code": "polymarket_latest_state_index_unavailable",
            "message": "Checkpoint reads require the durable latest-state index",
        })

    @app.get(
        "/v1/prediction-markets/polymarket/live/health",
        response_model=LiveReadHealth,
        summary="Read live source coverage, lag, and gap health",
    )
    def polymarket_live_health():
        return polymarket_live_read.health().model_dump(mode="json")

    @app.get(
        "/v1/prediction-markets/polymarket/live/gaps",
        response_model=LiveGapPage,
        summary="Read the Polymarket live gap ledger",
    )
    def polymarket_live_gaps(unresolved_only: bool = True):
        raise HTTPException(status_code=503, detail={
            "code": "polymarket_latest_state_index_unavailable",
            "message": "Gap reads require the durable latest-state index",
        })

    @app.get(
        "/v1/prediction-markets/polymarket/live/public-data/{kind}",
        response_model=PublicDataPage,
        summary="Read the latest locally captured public Data API facts",
    )
    def polymarket_live_public_data(kind: str):
        if kind not in PUBLIC_DATA_KINDS:
            raise HTTPException(status_code=400, detail={
                "code": "invalid_polymarket_public_data_kind",
            })
        folder = polymarket_live.root / "public-data" / kind
        paths = sorted(folder.glob("*.json"), key=lambda item: item.stat().st_mtime)
        if not paths:
            raise HTTPException(status_code=404, detail={
                "code": "polymarket_public_data_not_captured",
                "kind": kind,
            })
        try:
            body = paths[-1].read_bytes()
            if hashlib.sha256(body).hexdigest() != paths[-1].stem:
                raise ValueError("public Data API fact hash mismatch")
            items = json.loads(body, parse_float=str, parse_int=str)
        except (OSError, ValueError) as exc:
            raise HTTPException(status_code=409, detail={
                "code": "polymarket_public_data_integrity_failed",
            }) from exc
        return {
            "schema_version": "marketcow.polymarket.public-data-page.v1",
            "kind": kind,
            "count": len(items),
            "items": items,
        }

    @app.get("/v1/instruments/{instrument_id}")
    def get_instrument(instrument_id: str):
        row = service.metadata_repository.get_instrument(instrument_id)
        if row is None:
            raise HTTPException(status_code=404, detail={
                "code": "instrument_not_found", "instrument_id": instrument_id,
            })
        return instrument_record(row)

    @app.get("/v1/admin/instruments/{symbol}/coverage")
    def admin_instrument_coverage(symbol: str):
        try:
            normalized = normalize_quote_symbol(symbol)
            rows = service.market_bar_repository.get_symbol_coverage(normalized)
            return {
                "schema": "marketcow.instrument-coverage.v1",
                "symbol": normalized,
                "items": rows,
                "summary": {
                    "layers": sorted({row["layer"] for row in rows}),
                    "intervals": sorted({row["interval"] for row in rows}),
                    "rows": sum(int(row["row_count"]) for row in rows),
                    "sources": sorted({
                        source for row in rows for source in row["sources"]
                    }),
                },
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v1/instruments:resolve")
    def resolve_instrument(namespace: str, external_symbol: str):
        row = service.metadata_repository.find_instrument_by_mapping(
            namespace, external_symbol
        )
        if row is None:
            raise HTTPException(status_code=404, detail={
                "code": "instrument_mapping_not_found",
                "namespace": namespace, "external_symbol": external_symbol,
            })
        return instrument_record(row)

    @app.post(
        "/v1/instruments:resolve/query",
        response_model=InstrumentResolveBatchResponse,
    )
    def resolve_instruments_query(request: InstrumentResolveBatchRequest):
        return service.resolve_instruments_batch(request.namespace, request.symbols)

    @app.get("/v1/instrument-relationships")
    def instrument_relationship(
        derivative_instrument_id: str, underlying_instrument_id: str,
    ):
        try:
            return service.get_instrument_relationship(
                derivative_instrument_id, underlying_instrument_id
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail={
                "code": "instrument_relationship_unavailable",
                "message": str(exc),
            }) from exc

    @app.post("/v1/cross-market/snapshots/query")
    def cross_market_snapshots(request: CrossMarketQuery):
        def load(pair: CrossMarketPairRequest) -> Dict[str, Any]:
            try:
                data = service.get_cross_market_snapshot(
                    pair.derivative_instrument_id,
                    pair.underlying_instrument_id,
                    depth=request.book_depth,
                    max_age_ms=request.max_age_ms,
                    max_skew_ms=request.max_skew_ms,
                )
                return {
                    "derivative_instrument_id": pair.derivative_instrument_id,
                    "underlying_instrument_id": pair.underlying_instrument_id,
                    "status": "available", "data": data, "error": None,
                }
            except ValueError as exc:
                return {
                    "derivative_instrument_id": pair.derivative_instrument_id,
                    "underlying_instrument_id": pair.underlying_instrument_id,
                    "status": "unavailable", "data": None,
                    "error": {"code": "invalid_pair", "message": str(exc)},
                }
            except Exception as exc:
                return {
                    "derivative_instrument_id": pair.derivative_instrument_id,
                    "underlying_instrument_id": pair.underlying_instrument_id,
                    "status": "error", "data": None,
                    "error": {
                        "code": "provider_unavailable",
                        "message": sanitize_text(exc),
                    },
                }

        with ThreadPoolExecutor(max_workers=min(8, len(request.pairs))) as executor:
            futures = [executor.submit(load, pair) for pair in request.pairs]
            items = [future.result() for future in futures]
        return {
            "schema_version": "cross-market-query-v1",
            "count": len(items), "items": items,
        }

    @app.get("/v1/canonical-bars/{instrument_id}")
    def canonical_bars_v1(
        instrument_id: str,
        start: str,
        end: str,
        interval: str,
        adjustment: str = Query(pattern="^(raw|qfq|hfq)$"),
        page_size: int = Query(ge=1, le=5000),
        cursor: Optional[str] = None,
    ):
        instrument = service.metadata_repository.get_instrument(instrument_id)
        if instrument is None:
            raise HTTPException(status_code=404, detail={
                "code": "instrument_not_found", "instrument_id": instrument_id,
            })
        instrument = instrument_record(instrument)
        try:
            start_at = datetime.fromisoformat(start.replace("Z", "+00:00"))
            end_at = datetime.fromisoformat(end.replace("Z", "+00:00"))
            if start_at.tzinfo is None or end_at.tzinfo is None or start_at > end_at:
                raise ValueError("start/end must be ordered timezone-aware timestamps")
            start_utc = start_at.astimezone(timezone.utc).isoformat()
            end_utc = end_at.astimezone(timezone.utc).isoformat()
            interval_map = {
                "1-MINUTE": ("1m", 60), "5-MINUTE": ("5m", 300),
                "15-MINUTE": ("15m", 900), "30-MINUTE": ("30m", 1800),
                "1-HOUR": ("1h", 3600), "1-DAY": ("1d", 86400),
            }
            if interval not in interval_map:
                raise ValueError("interval is not supported by schema v1")
            storage_interval, interval_seconds = interval_map[interval]
            storage_symbol = (
                instrument_id
                if instrument["market"] == "CRYPTO"
                else instrument["symbol"]
            )
            identity = service.market_bar_repository.get_canonical_dataset_identity(
                storage_symbol, storage_interval, adjustment, start_utc, end_utc
            )
            binding = {
                "instrument_id": instrument_id, "start": start_utc, "end": end_utc,
                "interval": interval, "adjustment": adjustment,
                "page_size": page_size, "snapshot_id": identity["snapshot_id"],
            }
            secret = load_or_create_secret(
                settings.market_bar_cursor_secret, settings.storage_root
            )
            now_epoch = int(clock().timestamp())
            after = None if cursor is None else decode_cursor(
                cursor, binding, now_epoch,
                settings.market_bar_cursor_ttl_seconds, secret,
            )
            if after is not None and not isinstance(after, int):
                raise ValueError("canonical cursor position is invalid")
            rows, has_more = service.market_bar_repository.get_price_bars_page(
                storage_symbol, storage_interval, adjustment, start_utc, end_utc,
                page_size, after,
            )
            bars = []
            for row in rows:
                source_payload = row.get("source_payload") or {}
                window_start = datetime.fromisoformat(
                    str(row["bar_at"]).replace("Z", "+00:00")
                ).astimezone(timezone.utc)
                window_end = window_start + timedelta(seconds=interval_seconds)
                bars.append(HistoricalBar(
                    instrument_id=instrument_id, interval=interval,
                    adjustment=adjustment, price_type="LAST",
                    aggregation_source="EXTERNAL",
                    window_start=window_start.isoformat(),
                    window_end=window_end.isoformat(),
                    ts_event=window_end.isoformat(), ts_init=row["ingested_at"],
                    open=format(Decimal(str(row["open"])), "f"),
                    high=format(Decimal(str(row["high"])), "f"),
                    low=format(Decimal(str(row["low"])), "f"),
                    close=format(Decimal(str(row["close"])), "f"),
                    volume=format(Decimal(str(row["volume"])), "f"),
                    factor_applicability=(
                        row.get("factor_applicability") or None
                    ),
                    corporate_action_factor=row.get("corporate_action_factor"),
                    applied_adjustment_multiplier=row.get(
                        "applied_adjustment_multiplier"
                    ),
                    adjustment_reference_date=row.get(
                        "adjustment_reference_date"
                    ),
                    reference_factor=row.get("reference_factor"),
                    factor_source=row.get("factor_source"),
                    factor_artifact_id=row.get("factor_artifact_id"),
                    factor_as_of=row.get("factor_as_of"),
                    selected_source=(
                        row.get("selected_source") or row.get("source")
                    ),
                    quality_status=(
                        row.get("quality_status")
                        or source_payload.get("quality_status")
                    ),
                    row_version=str(
                        row.get("version") or source_payload.get("version")
                    ),
                ))
            confirmed = service.market_bar_repository.get_canonical_dataset_identity(
                storage_symbol, storage_interval, adjustment, start_utc, end_utc
            )
            if confirmed != identity:
                raise HTTPException(status_code=409, detail={
                    "code": "canonical_snapshot_changed",
                    "message": "canonical data changed during page read; restart query",
                })
            next_cursor = None
            if has_more and rows:
                next_cursor = encode_cursor(
                    binding, int(rows[-1]["timestamp"]), now_epoch, secret
                )
            manifest = HistoricalManifest(
                dataset_id=canonical_hash({
                    "instrument_id": instrument_id, "interval": interval,
                    "adjustment": adjustment, "start": start_utc, "end": end_utc,
                })[7:31],
                snapshot_id=identity["snapshot_id"],
                canonical_version=identity["canonical_version"],
                instruments=[instrument_id], interval=interval,
                adjustment=adjustment, start=start_utc, end=end_utc,
                row_count=identity["row_count"],
                content_hash=identity["content_hash"],
            )
            return CanonicalBarPage(
                manifest=manifest, count=len(bars), bars=bars, page_size=page_size,
                next_cursor=next_cursor, truncated=has_more,
                provenance={"layer": "canonical", "backend": "clickhouse"},
            ).model_dump(mode="json")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={
                "code": "invalid_canonical_query", "message": str(exc),
            }) from exc

    def provider_http_error(exc: ProviderRoutingError) -> HTTPException:
        status = 422 if isinstance(exc, ProviderNotSupported) else 503
        return HTTPException(status_code=status, detail=exc.detail())

    @app.post("/v1/tushare/realtime-quote", deprecated=True)
    def tushare_realtime_quote(request: TushareRealtimeRequest):
        try:
            items = service.tushare_realtime_quote(request.ts_code)
            return {"count": len(items), "items": items, "source": "tushare_realtime"}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/v1/tushare/{api_name}", deprecated=True)
    def tushare_call(api_name: str, request: TushareRequest):
        try:
            return service.call_tushare(api_name, request.params, request.fields)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/economic-calendar")
    def economic_calendar(
        country: str = "US",
        date_from: str = Query("", alias="from"),
        date_to: str = Query("", alias="to"),
        impact: str = "",
        limit: int = Query(50, ge=1, le=500),
        include_past: bool = False,
    ):
        start, end = calendar_range(date_from, date_to, include_past=include_past)
        events = service.metadata_repository.get_economic_calendar(start, end, country, impact, limit)
        return {
            "count": len(events), "from": start, "to": end,
            "filter_timezone": "Asia/Shanghai", "past_events_excluded": not include_past,
            "events": events,
        }

    @app.get("/v1/economic-indicators")
    def economic_indicators(
        country: str = "US", source: str = "", limit: int = Query(50, ge=1, le=500)
    ):
        indicators = service.metadata_repository.get_economic_indicators(country, source, limit)
        return {"count": len(indicators), "indicators": indicators}

    @app.get("/v1/earnings-calendar")
    def earnings_calendar(
        market: str = "",
        symbols: str = "",
        date_from: str = Query("", alias="from"),
        date_to: str = Query("", alias="to"),
        limit: int = Query(50, ge=1, le=500),
        include_past: bool = False,
    ):
        start, end = calendar_range(date_from, date_to, include_past=include_past)
        requested = [item.strip().upper() for item in symbols.split(",") if item.strip()]
        events = service.metadata_repository.get_earnings_calendar(start, end, market, requested, limit)
        return {
            "count": len(events), "from": start, "to": end,
            "filter_timezone": "Asia/Shanghai", "past_events_excluded": not include_past,
            "events": events,
        }

    @app.get("/v1/dividends/{symbol}")
    def dividends(symbol: str, fiscal_year: int = Query(ge=1991, le=2100)):
        try:
            return service.get_dividends(symbol, fiscal_year)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get(
        "/v1/funds/{symbol}/dividends",
        response_model=FundDividendHistoryResponse,
        responses={
            400: {
                "model": FundDividendHttpError,
                "description": "invalid_symbol_or_date_range",
            },
            422: {
                "model": FundDividendHttpError,
                "description": "unsupported_asset_type",
            },
            502: {
                "model": FundDividendHttpError,
                "description": "provider_unavailable",
            },
        },
    )
    def fund_dividend_history_api(
        symbol: str,
        date_from: str = Query(alias="from"),
        date_to: str = Query(alias="to"),
        refresh: bool = True,
    ):
        try:
            return service.get_fund_dividend_history(
                symbol, date_from, date_to, refresh=refresh
            )
        except ValueError as exc:
            message = str(exc)
            code = (
                "unsupported_asset_type"
                if "not recognized as a fund or ETF" in message
                else "invalid_request"
            )
            status_code = 422 if code == "unsupported_asset_type" else 400
            raise HTTPException(
                status_code=status_code,
                detail={"code": code, "message": message},
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail={"code": "provider_unavailable", "message": str(exc)},
            ) from exc

    @app.post("/v1/dividends/query")
    def dividends_query(request: DividendQuery):
        requested: list[tuple[str, Optional[str], Optional[str]]] = []
        unique_symbols: list[str] = []
        for symbol in request.symbols:
            try:
                normalized = normalize_dividend_symbol(symbol)
                requested.append((symbol, normalized, None))
                if normalized not in unique_symbols:
                    unique_symbols.append(normalized)
            except Exception as exc:
                requested.append((symbol, None, str(exc)))

        workers = max(1, min(settings.dividend_batch_workers, len(unique_symbols)))
        executor = ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="dividend-query"
        )
        futures = {
            executor.submit(service.get_dividends, symbol, request.fiscal_year): symbol
            for symbol in unique_symbols
        }
        done, unfinished = wait(
            futures, timeout=settings.dividend_batch_timeout_seconds
        )
        results: Dict[str, Dict[str, Any]] = {}
        for future in done:
            symbol = futures[future]
            try:
                results[symbol] = {"data": future.result(), "error": None}
            except Exception as exc:
                results[symbol] = {"data": None, "error": str(exc)}
        for future in unfinished:
            symbol = futures[future]
            future.cancel()
            results[symbol] = {
                "data": None,
                "error": (
                    "dividend query exceeded "
                    f"{settings.dividend_batch_timeout_seconds:g}s batch timeout"
                ),
            }
        executor.shutdown(wait=False, cancel_futures=True)

        items = []
        for original, normalized, normalization_error in requested:
            if normalization_error is not None:
                items.append({
                    "requested_symbol": original,
                    "symbol": None,
                    "status": "error",
                    "cache_status": None,
                    "error": normalization_error,
                    "data": None,
                })
                continue
            outcome = results[normalized]
            data, error = outcome["data"], outcome["error"]
            if error is not None:
                status, cache_status = "error", None
            else:
                cache_status = str(data.get("data_status") or "fresh")
                if cache_status in {"refreshing", "stale"}:
                    status = cache_status
                elif int(data.get("announced_count") or 0) > 0:
                    status = "available"
                else:
                    status = "unavailable"
            items.append({
                "requested_symbol": original,
                "symbol": normalized,
                "status": status,
                "cache_status": cache_status,
                "error": error,
                "data": data,
            })
        return {
            "fiscal_year": request.fiscal_year,
            "requested_count": len(request.symbols),
            "completed_count": sum(item["status"] != "error" for item in items),
            "items": items,
        }

    @app.post("/v1/admin/dividends/ingest")
    def ingest_dividends(request: DividendIngestRequest):
        try:
            return service.ingest_dividend_announcements([
                item.model_dump() for item in request.announcements
            ])
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v1/admin/dividends/{symbol}/refresh")
    def refresh_dividends(
        symbol: str, fiscal_year: int = Query(ge=1991, le=2100)
    ):
        try:
            return service.refresh_dividends(symbol, fiscal_year)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/v1/admin/dividends/{symbol}/discover")
    def discover_dividends(
        symbol: str, fiscal_year: int = Query(ge=1991, le=2100)
    ):
        try:
            return service.discover_dividends(symbol, fiscal_year)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/snapshot")
    def data_snapshot(limit: int = Query(50, ge=1, le=500), days: int = Query(30, ge=1, le=120)):
        start, end = calendar_range("", "", days=days)
        result = service.calendar_snapshot(start, end, limit)
        result.update({"from": start, "to": end, "past_events_excluded": True})
        return result

    @app.get(
        "/v1/fx",
        response_model=FxResponse,
        response_model_by_alias=True,
        responses={
            422: {"model": FxHttpErrorResponse},
            503: {"model": FxHttpErrorResponse},
        },
        summary="Get auditable foreign-exchange rates",
    )
    def fx_rates(
        base: str = Query(..., min_length=3, max_length=3),
        symbols: str = Query(
            ...,
            min_length=3,
            description="Comma-separated target currencies, for example CNY,HKD",
        ),
        refresh: bool = False,
    ):
        targets = [
            item.strip().upper() for item in symbols.split(",") if item.strip()
        ]
        normalized_base = base.strip().upper()
        if (
            normalized_base not in SUPPORTED_FX_CURRENCIES
            or not targets
            or any(item not in SUPPORTED_FX_CURRENCIES for item in targets)
            or len(targets) != len(set(targets))
        ):
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "invalid_currency",
                    "message": (
                        "base and unique symbols must use USD, CNY or HKD"
                    ),
                },
            )
        try:
            return service.get_fx_rates(
                normalized_base, targets, refresh=refresh
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=422,
                detail={"code": "invalid_currency", "message": str(exc)},
            ) from exc
        except FxRateError as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "code": exc.code,
                    "currency": exc.currency,
                    "message": str(exc),
                },
            ) from exc

    @app.post("/v1/admin/economic-calendar/refresh")
    def refresh_economic_calendar(
        country: str = "US",
        date_from: str = Query("", alias="from"),
        date_to: str = Query("", alias="to"),
        days: int = Query(30, ge=1, le=120),
    ):
        start, end = calendar_range(date_from, date_to, days=days, include_past=True)
        try:
            return service.refresh_economic_calendar(start, end, country)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/v1/admin/economic-indicators/refresh")
    def refresh_economic_indicators():
        try:
            return service.refresh_economic_indicators()
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/v1/admin/earnings-calendar/refresh")
    def refresh_earnings_calendar(
        market: str = "",
        symbols: str = "",
        date_from: str = Query("", alias="from"),
        date_to: str = Query("", alias="to"),
        days: int = Query(30, ge=1, le=120),
    ):
        start, end = calendar_range(date_from, date_to, days=days, include_past=True)
        requested = [item.strip().upper() for item in symbols.split(",") if item.strip()]
        try:
            return service.refresh_earnings_calendar(start, end, market, requested)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/quotes")
    def quotes(
        symbols: str, refresh: bool = False, provider: Optional[str] = None,
        allow_fallback: bool = False,
    ):
        requested = [item.strip() for item in symbols.split(",") if item.strip()]
        if not requested:
            raise HTTPException(status_code=400, detail="symbols is required")
        if len(requested) > 20:
            raise HTTPException(status_code=400, detail="at most 20 symbols per request")
        if provider and not refresh:
            raise HTTPException(
                status_code=400,
                detail={"code": "provider_requires_refresh",
                        "message": "provider selection requires refresh=true"},
            )
        normalized_symbols, normalization_errors = [], []
        for symbol in requested:
            try:
                normalized_symbols.append(normalize_quote_symbol(symbol))
            except Exception as exc:
                normalization_errors.append({
                    "symbol": symbol, "status": "unavailable", "error": str(exc),
                })
        by_symbol, errors = {}, list(normalization_errors)
        batch_method = getattr(service, "refresh_quotes_batch", None)
        if refresh and provider and callable(batch_method) and normalized_symbols:
            try:
                batch = batch_method(normalized_symbols, provider, allow_fallback)
            except ProviderNotSupported as exc:
                raise provider_http_error(exc) from exc
            except Exception as exc:
                batch = []
                errors.extend({
                    "symbol": symbol, "status": "unavailable", "error": str(exc),
                } for symbol in normalized_symbols)
            if batch is not None:
                by_symbol.update({row["symbol"]: row for row in batch})
                items = [by_symbol[symbol] for symbol in normalized_symbols if symbol in by_symbol]
                result = {"count": len(items), "items": items, "errors": errors}
                result["routing"] = {
                    "provider_requested": provider, "allow_fallback": allow_fallback
                }
                return result
        workers = max(1, min(settings.quote_refresh_workers, len(normalized_symbols)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    service.get_quote, normalized, refresh, provider, allow_fallback,
                ): normalized
                for normalized in normalized_symbols
            }
            for future in as_completed(futures):
                normalized = futures[future]
                try:
                    by_symbol[normalized] = future.result()
                except ProviderNotSupported as exc:
                    raise provider_http_error(exc) from exc
                except Exception as exc:
                    errors.append({
                        "symbol": normalized, "status": "unavailable", "error": str(exc),
                    })
        items = [by_symbol[symbol] for symbol in normalized_symbols if symbol in by_symbol]
        result = {"count": len(items), "items": items, "errors": errors}
        if provider or allow_fallback:
            result["routing"] = {
                "provider_requested": provider, "allow_fallback": allow_fallback
            }
        return result

    @app.post("/v1/quotes/query")
    def quotes_query(request: QuoteQuery):
        return quotes(
            ",".join(request.symbols), refresh=request.refresh,
            provider=request.provider, allow_fallback=request.allow_fallback,
        )

    @app.get("/v1/quotes/{symbol}/history")
    def quote_history(
        symbol: str,
        range_: str = Query("1y", alias="range"),
        interval: str = "1d",
        adjustment: str = Query("qfq", pattern="^(raw|qfq|hfq)$"),
        refresh: bool = True,
        limit: int = Query(500, ge=1, le=5000),
        start: Optional[str] = None,
        end: Optional[str] = None,
        page_size: Optional[int] = Query(None, ge=1, le=5000),
        cursor: Optional[str] = None,
        provider: Optional[str] = None,
        allow_fallback: bool = False,
    ):
        try:
            normalized = normalize_quote_symbol(symbol)
            if (start is None) != (end is None):
                raise ValueError("history range requires both start and end")
            if cursor is not None and page_size is None:
                raise ValueError("history cursor requires page_size")
            if page_size is not None and (start is None or end is None):
                raise ValueError("history pagination requires start and end")
            if start is not None and end is not None:
                start_at = datetime.fromisoformat(start.replace("Z", "+00:00"))
                end_at = datetime.fromisoformat(end.replace("Z", "+00:00"))
                if start_at.tzinfo is None or end_at.tzinfo is None:
                    raise ValueError("history range timestamps must include a timezone")
                start_at = datetime.fromtimestamp(
                    int(start_at.timestamp()), timezone.utc
                )
                end_at = datetime.fromtimestamp(int(end_at.timestamp()), timezone.utc)
                if start_at > end_at:
                    raise ValueError("history range start must not be after end")
                if page_size is not None:
                    query_binding = {
                        "symbol": normalized, "interval": interval,
                        "adjustment": adjustment, "start": start_at.isoformat(),
                        "end": end_at.isoformat(), "page_size": page_size,
                    }
                    cursor_secret = load_or_create_secret(
                        settings.market_bar_cursor_secret, settings.storage_root
                    )
                    cursor_now = clock()
                    if cursor_now.tzinfo is None:
                        cursor_now = cursor_now.replace(tzinfo=timezone.utc)
                    now_epoch = int(cursor_now.timestamp())
                    after = None if cursor is None else decode_cursor(
                        cursor, query_binding, now_epoch,
                        settings.market_bar_cursor_ttl_seconds,
                        cursor_secret,
                    )
                    if after is not None and not isinstance(after, int):
                        raise ValueError("invalid history cursor position")
                    if after is not None and not (
                        int(start_at.timestamp()) <= after <= int(end_at.timestamp())
                    ):
                        raise ValueError("cursor position is outside the query range")
                    bars, has_more = service.market_bar_repository.get_price_bars_page(
                        normalized, interval, adjustment, start_at.isoformat(),
                        end_at.isoformat(), page_size, after,
                    )
                    next_cursor = None
                    if has_more and bars:
                        next_cursor = encode_cursor(
                            query_binding, int(bars[-1]["timestamp"]), now_epoch,
                            cursor_secret,
                        )
                    return {
                        "symbol": normalized, "interval": interval,
                        "adjustment": adjustment, "count": len(bars), "bars": bars,
                        "cached": True, "start": start_at.isoformat(),
                        "end": end_at.isoformat(), "truncated": has_more,
                        "page_size": page_size, "next_cursor": next_cursor,
                        **cache_metadata(bars),
                    }
                bars, truncated = service.market_bar_repository.get_price_bars_range(
                    normalized, interval, adjustment, start, end, limit
                )
                return {
                    "symbol": normalized, "interval": interval,
                    "adjustment": adjustment, "count": len(bars), "bars": bars,
                    "cached": True, "start": start, "end": end,
                    "truncated": truncated,
                    **cache_metadata(bars),
                }
            if refresh:
                try:
                    if provider or allow_fallback:
                        result = service.refresh_quote_history(
                            normalized, range_, interval, adjustment,
                            provider=provider, allow_fallback=allow_fallback,
                        )
                    else:
                        result = service.refresh_quote_history(
                            normalized, range_, interval, adjustment
                        )
                except Exception as error:
                    bars = service.market_bar_repository.get_price_bars(
                        normalized, interval, adjustment, limit
                    )
                    if not bars:
                        raise
                    return {
                        "symbol": normalized, "interval": interval,
                        "adjustment": adjustment, "count": len(bars), "bars": bars,
                        "cached": True, "cache_degraded": True,
                        **cache_metadata(bars, reason=str(error)),
                    }
                result["bars"] = result["bars"][-limit:]
                result["count"] = len(result["bars"])
                result.setdefault("cached", False)
                result.update(cache_metadata(
                    result["bars"], result.get("ingested_at") or result.get("observed_at")
                ))
                return result
            bars = service.market_bar_repository.get_price_bars(normalized, interval, adjustment, limit)
            return {"symbol": normalized, "interval": interval, "adjustment": adjustment,
                    "count": len(bars), "bars": bars, "cached": True,
                    **cache_metadata(bars)}
        except ProviderRoutingError as exc:
            raise provider_http_error(exc) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/v1/market-bars/query")
    def market_bars_query(request: MarketBarQuery):
        items, errors = [], []
        for symbol in request.symbols:
            try:
                items.append(quote_history(
                    symbol, range_=request.range, interval=request.interval,
                    adjustment=request.adjustment, refresh=request.refresh,
                    limit=request.limit, start=None, end=None, page_size=None, cursor=None,
                    provider=request.provider,
                    allow_fallback=request.allow_fallback,
                ))
            except HTTPException as exc:
                if exc.status_code == 422:
                    raise
                errors.append({"symbol": symbol, "status": exc.status_code, "error": exc.detail})
        return {
            "count": len(items), "items": items, "errors": errors,
            "routing": {"provider_requested": request.provider,
                        "allow_fallback": request.allow_fallback},
        }

    @app.get("/v1/quotes/cross-section")
    def quote_cross_section(
        bar_at: str,
        interval: str = "1d",
        adjustment: str = "qfq",
        limit: int = 500,
        symbols: Optional[str] = None,
        page_size: Optional[int] = Query(None, ge=1, le=5000),
        cursor: Optional[str] = None,
    ):
        try:
            if adjustment not in {"raw", "qfq", "hfq"}:
                raise ValueError("adjustment must be raw, qfq or hfq")
            if not 1 <= limit <= 5000:
                raise ValueError("cross-section limit must be between 1 and 5000")
            if cursor is not None and page_size is None:
                raise ValueError("cross-section cursor requires page_size")
            point = datetime.fromisoformat(bar_at.replace("Z", "+00:00"))
            if point.tzinfo is None:
                raise ValueError("cross-section bar_at must include a timezone")
            normalized_bar_at = datetime.fromtimestamp(
                int(point.timestamp()), ZoneInfo("UTC")
            ).isoformat()
            symbol_filter = None
            if symbols is not None:
                symbol_filter = sorted({value.strip() for value in symbols.split(",")
                                        if value.strip()})
                if len(symbol_filter) > 5000:
                    raise ValueError(
                        "cross-section symbols must contain at most 5000 values"
                    )
            if page_size is not None:
                query_binding = {
                    "interval": interval, "adjustment": adjustment,
                    "bar_at": normalized_bar_at, "symbols": symbol_filter,
                    "page_size": page_size,
                }
                cursor_secret = load_or_create_secret(
                    settings.market_bar_cursor_secret, settings.storage_root
                )
                cursor_now = clock()
                if cursor_now.tzinfo is None:
                    cursor_now = cursor_now.replace(tzinfo=timezone.utc)
                now_epoch = int(cursor_now.timestamp())
                after = None if cursor is None else decode_cursor(
                    cursor, query_binding, now_epoch,
                    settings.market_bar_cursor_ttl_seconds, cursor_secret,
                )
                if after is not None and not isinstance(after, str):
                    raise ValueError("invalid cross-section cursor position")
                bars, has_more = (
                    service.market_bar_repository.get_price_bars_cross_section_page(
                        interval, adjustment, normalized_bar_at, page_size,
                        symbol_filter, after,
                    )
                )
                next_cursor = None
                if has_more and bars:
                    next_cursor = encode_cursor(
                        query_binding, bars[-1]["symbol"], now_epoch, cursor_secret
                    )
                return {
                    "bar_at": normalized_bar_at, "interval": interval,
                    "adjustment": adjustment, "count": len(bars), "bars": bars,
                    "cached": True, "truncated": has_more,
                    "page_size": page_size, "next_cursor": next_cursor,
                    **cache_metadata(bars),
                }
            bars, truncated = service.market_bar_repository.get_price_bars_cross_section(
                interval, adjustment, normalized_bar_at, limit, symbol_filter
            )
            return {
                "bar_at": normalized_bar_at, "interval": interval,
                "adjustment": adjustment,
                "count": len(bars), "bars": bars, "cached": True,
                "truncated": truncated,
                **cache_metadata(bars),
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/quotes/cross-section/matrix")
    def quote_cross_section_matrix(
        bar_ats: str,
        symbols: str,
        interval: str = "1d",
        adjustment: str = "qfq",
        page_size: int = Query(500, ge=1, le=5000),
        cursor: Optional[str] = None,
    ):
        try:
            if adjustment not in {"raw", "qfq", "hfq"}:
                raise ValueError("adjustment must be raw, qfq or hfq")
            normalized_points = set()
            for value in (item.strip() for item in bar_ats.split(",")):
                if not value:
                    continue
                point = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if point.tzinfo is None:
                    raise ValueError("matrix bar_ats must include a timezone")
                normalized_points.add(datetime.fromtimestamp(
                    int(point.timestamp()), ZoneInfo("UTC")
                ).isoformat())
            normalized_bar_ats = sorted(normalized_points)
            symbol_filter = sorted({
                value.strip() for value in symbols.split(",") if value.strip()
            })
            if not 1 <= len(normalized_bar_ats) <= 100:
                raise ValueError("matrix bar_ats must contain between 1 and 100 values")
            if not 1 <= len(symbol_filter) <= 1000:
                raise ValueError("matrix symbols must contain between 1 and 1000 values")
            matrix_cells = len(normalized_bar_ats) * len(symbol_filter)
            if matrix_cells > 100_000:
                raise ValueError("matrix request must contain at most 100000 cells")
            query_binding = {
                "interval": interval, "adjustment": adjustment,
                "bar_ats": normalized_bar_ats, "symbols": symbol_filter,
                "page_size": page_size,
            }
            cursor_secret = load_or_create_secret(
                settings.market_bar_cursor_secret, settings.storage_root
            )
            cursor_now = clock()
            if cursor_now.tzinfo is None:
                cursor_now = cursor_now.replace(tzinfo=timezone.utc)
            now_epoch = int(cursor_now.timestamp())
            decoded_after = None if cursor is None else decode_cursor(
                cursor, query_binding, now_epoch,
                settings.market_bar_cursor_ttl_seconds, cursor_secret,
            )
            after = None
            if decoded_after is not None:
                if not isinstance(decoded_after, list):
                    raise ValueError("invalid matrix cursor position")
                after = (decoded_after[0], decoded_after[1])
            bars, has_more = service.market_bar_repository.get_price_bars_matrix_page(
                interval, adjustment, normalized_bar_ats, symbol_filter,
                page_size, after,
            )
            next_cursor = None
            if has_more and bars:
                next_cursor = encode_cursor(
                    query_binding,
                    [int(bars[-1]["timestamp"]), bars[-1]["symbol"]],
                    now_epoch, cursor_secret,
                )
            return {
                "bar_ats": normalized_bar_ats, "symbols": symbol_filter,
                "interval": interval, "adjustment": adjustment,
                "matrix_cells": matrix_cells, "count": len(bars), "bars": bars,
                "cached": True, "truncated": has_more,
                "page_size": page_size, "next_cursor": next_cursor,
                **cache_metadata(bars),
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/quotes/cross-section/as-of")
    def quote_cross_section_as_of(
        as_of: str,
        symbols: str,
        interval: str = "1d",
        adjustment: str = "qfq",
        max_lookback_seconds: int = Query(86400, ge=1, le=31_536_000),
        page_size: int = Query(500, ge=1, le=1000),
        cursor: Optional[str] = None,
    ):
        try:
            if adjustment not in {"raw", "qfq", "hfq"}:
                raise ValueError("adjustment must be raw, qfq or hfq")
            point = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            if point.tzinfo is None:
                raise ValueError("as_of must include a timezone")
            normalized_as_of = datetime.fromtimestamp(
                int(point.timestamp()), ZoneInfo("UTC")
            ).isoformat()
            symbol_filter = sorted({
                value.strip() for value in symbols.split(",") if value.strip()
            })
            if not 1 <= len(symbol_filter) <= 1000:
                raise ValueError("as-of symbols must contain between 1 and 1000 values")
            query_binding = {
                "interval": interval, "adjustment": adjustment,
                "as_of": normalized_as_of,
                "max_lookback_seconds": max_lookback_seconds,
                "symbols": symbol_filter, "page_size": page_size,
            }
            cursor_secret = load_or_create_secret(
                settings.market_bar_cursor_secret, settings.storage_root
            )
            cursor_now = clock()
            if cursor_now.tzinfo is None:
                cursor_now = cursor_now.replace(tzinfo=timezone.utc)
            now_epoch = int(cursor_now.timestamp())
            after = None if cursor is None else decode_cursor(
                cursor, query_binding, now_epoch,
                settings.market_bar_cursor_ttl_seconds, cursor_secret,
            )
            if after is not None and not isinstance(after, str):
                raise ValueError("invalid as-of cursor position")
            bars, has_more = service.market_bar_repository.get_price_bars_as_of_page(
                interval, adjustment, normalized_as_of, max_lookback_seconds,
                symbol_filter, page_size, after,
            )
            next_cursor = None
            if has_more and bars:
                next_cursor = encode_cursor(
                    query_binding, bars[-1]["symbol"], now_epoch, cursor_secret
                )
            return {
                "as_of": normalized_as_of, "interval": interval,
                "adjustment": adjustment,
                "max_lookback_seconds": max_lookback_seconds,
                "symbols": symbol_filter, "count": len(bars), "bars": bars,
                "cached": True, "truncated": has_more,
                "page_size": page_size, "next_cursor": next_cursor,
                "max_staleness_seconds": max(
                    (row["staleness_seconds"] for row in bars), default=None
                ),
                **cache_metadata(bars),
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/quotes/{symbol}/as-of")
    def quote_as_of(
        symbol: str,
        as_of: str,
        interval: str = "1d",
        adjustment: str = "qfq",
        max_lookback_seconds: int = Query(86400, ge=1, le=31_536_000),
    ):
        try:
            if adjustment not in {"raw", "qfq", "hfq"}:
                raise ValueError("adjustment must be raw, qfq or hfq")
            normalized = normalize_quote_symbol(symbol)
            point = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            if point.tzinfo is None:
                raise ValueError("as_of must include a timezone")
            normalized_as_of = datetime.fromtimestamp(
                int(point.timestamp()), ZoneInfo("UTC")
            ).isoformat()
            row = service.market_bar_repository.get_price_bar_as_of(
                normalized, interval, adjustment, normalized_as_of,
                max_lookback_seconds,
            )
            bars = [] if row is None else [row]
            return {
                "symbol": normalized, "as_of": normalized_as_of,
                "interval": interval, "adjustment": adjustment,
                "max_lookback_seconds": max_lookback_seconds,
                "count": len(bars), "bar": row, "cached": True,
                "max_staleness_seconds": (
                    None if row is None else row["staleness_seconds"]
                ),
                **cache_metadata(bars),
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/quotes/{symbol}/raw-history")
    def quote_raw_history(
        symbol: str,
        start: str,
        end: str,
        interval: str = "1d",
        adjustment: str = "raw",
        limit: int = 500,
        sources: Optional[str] = None,
        page_size: Optional[int] = Query(None, ge=1, le=5000),
        cursor: Optional[str] = None,
    ):
        try:
            if adjustment not in {"raw", "qfq", "hfq"}:
                raise ValueError("adjustment must be raw, qfq or hfq")
            if not 1 <= limit <= 5000:
                raise ValueError("raw history limit must be between 1 and 5000")
            if cursor is not None and page_size is None:
                raise ValueError("raw history cursor requires page_size")
            normalized = normalize_quote_symbol(symbol)
            start_at = datetime.fromisoformat(start.replace("Z", "+00:00"))
            end_at = datetime.fromisoformat(end.replace("Z", "+00:00"))
            if start_at.tzinfo is None or end_at.tzinfo is None:
                raise ValueError("history range timestamps must include a timezone")
            normalized_start = datetime.fromtimestamp(
                int(start_at.timestamp()), ZoneInfo("UTC")
            ).isoformat()
            normalized_end = datetime.fromtimestamp(
                int(end_at.timestamp()), ZoneInfo("UTC")
            ).isoformat()
            source_filter = None
            if sources is not None:
                source_filter = sorted({value.strip() for value in sources.split(",")
                                        if value.strip()})
                if len(source_filter) > 100:
                    raise ValueError("raw history sources must contain at most 100 values")
            if page_size is not None:
                query_binding = {
                    "symbol": normalized, "interval": interval,
                    "adjustment": adjustment, "start": normalized_start,
                    "end": normalized_end, "sources": source_filter,
                    "page_size": page_size,
                }
                cursor_secret = load_or_create_secret(
                    settings.market_bar_cursor_secret, settings.storage_root
                )
                cursor_now = clock()
                if cursor_now.tzinfo is None:
                    cursor_now = cursor_now.replace(tzinfo=timezone.utc)
                now_epoch = int(cursor_now.timestamp())
                decoded_after = None if cursor is None else decode_cursor(
                    cursor, query_binding, now_epoch,
                    settings.market_bar_cursor_ttl_seconds, cursor_secret,
                )
                after = None
                if decoded_after is not None:
                    if not isinstance(decoded_after, list):
                        raise ValueError("invalid raw history cursor position")
                    after = (decoded_after[0], decoded_after[1])
                    if not (int(start_at.timestamp()) <= after[0]
                            <= int(end_at.timestamp())):
                        raise ValueError("cursor position is outside the query range")
                bars, has_more = service.market_bar_repository.get_raw_price_bars_page(
                    normalized, interval, adjustment, normalized_start, normalized_end,
                    page_size, source_filter, after,
                )
                next_cursor = None
                if has_more and bars:
                    next_cursor = encode_cursor(
                        query_binding,
                        [int(bars[-1]["timestamp"]), bars[-1]["source"]],
                        now_epoch, cursor_secret,
                    )
                return {
                    "symbol": normalized, "interval": interval,
                    "adjustment": adjustment, "start": normalized_start,
                    "end": normalized_end, "count": len(bars), "bars": bars,
                    "cached": True, "truncated": has_more,
                    "page_size": page_size, "next_cursor": next_cursor,
                    **cache_metadata(bars),
                }
            bars, truncated = service.market_bar_repository.get_raw_price_bars_range(
                normalized, interval, adjustment, normalized_start, normalized_end,
                limit, source_filter,
            )
            return {
                "symbol": normalized, "interval": interval,
                "adjustment": adjustment, "start": normalized_start,
                "end": normalized_end, "count": len(bars), "bars": bars,
                "cached": True, "truncated": truncated,
                **cache_metadata(bars),
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/quotes/{symbol}")
    def quote(
        symbol: str, refresh: bool = False, provider: Optional[str] = None,
        allow_fallback: bool = False,
    ):
        try:
            normalized = normalize_quote_symbol(symbol)
            if provider and not refresh:
                raise HTTPException(
                    status_code=400,
                    detail={"code": "provider_requires_refresh",
                            "message": "provider selection requires refresh=true"},
                )
            return service.get_quote(
                normalized, force_refresh=refresh, provider=provider,
                allow_fallback=allow_fallback,
            )
        except HTTPException:
            raise
        except ProviderRoutingError as exc:
            raise provider_http_error(exc) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail={
                    "symbol": symbol,
                    "status": "unavailable",
                    "error": str(exc),
                },
            ) from exc

    @app.get("/v1/exposure-facts/{symbol}")
    def exposure_facts(symbol: str, refresh: bool = False):
        """Auditable issuer/fund facts; no theme, factor or LLM inference."""
        try:
            return service.exposure_facts_service.get(symbol, refresh=refresh)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"code": "invalid_symbol", "message": str(exc)},
            ) from exc

    @app.get("/v1/quotes/{symbol}/spread")
    def quote_spread(symbol: str):
        try:
            return service.get_quote_spread(symbol)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail={"symbol": symbol, "status": "unavailable"},
            ) from exc

    @app.get("/v1/fundamentals")
    def fundamentals(
        limit: int = Query(100, ge=1, le=5000),
        offset: int = Query(0, ge=0),
        symbol: str = "",
        report_period: str = "",
        industry: str = "",
        min_roe: Optional[float] = None,
        max_pe: Optional[float] = None,
        active_only: bool = True,
        as_of: str = "",
    ):
        if report_period:
            report_period = normalize_report_period(report_period)
        if as_of:
            as_of = parse_as_of(as_of)
        rows = service.fundamental_repository.query_fundamentals(
            limit=limit,
            offset=offset,
            symbol="".join(ch for ch in symbol if ch.isdigit()).zfill(6) if symbol else "",
            report_period=report_period,
            industry=industry,
            min_roe=min_roe,
            max_pe=max_pe,
            active_only=active_only,
            as_of=as_of,
        )
        return {"count": len(rows), "limit": limit, "offset": offset, "as_of": as_of or None, "point_in_time": bool(as_of), "items": rows}

    @app.get("/v1/fundamentals/{symbol}")
    def fundamental(symbol: str, report_period: str = "", as_of: str = ""):
        code = "".join(ch for ch in symbol if ch.isdigit()).zfill(6)
        rows = service.fundamental_repository.query_fundamentals(
            limit=1,
            symbol=code,
            report_period=normalize_report_period(report_period) if report_period else "",
            active_only=False,
            as_of=parse_as_of(as_of),
        )
        if not rows:
            raise HTTPException(status_code=404, detail="fundamental data not found")
        return rows[0]

    @app.get("/v1/financials/{symbol}/statements")
    def statements(
        symbol: str,
        statement: str = Query("", pattern="^(|income|balance|cashflow)$"),
        limit_periods: int = Query(20, ge=1, le=200),
        as_of: str = "",
    ):
        code = "".join(ch for ch in symbol if ch.isdigit()).zfill(6)
        normalized_as_of = parse_as_of(as_of)
        rows = service.fundamental_repository.get_statement_rows(code, statement, limit_periods, normalized_as_of)
        return {"symbol": code, "count": len(rows), "as_of": normalized_as_of or None, "point_in_time": bool(normalized_as_of), "items": rows}

    @app.post("/v1/admin/fundamentals/refresh")
    def refresh_fundamentals(report_period: str = "", include_valuation: bool = True):
        try:
            return service.refresh_market_fundamentals(report_period, include_valuation)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/v1/admin/financials/{symbol}/refresh")
    def refresh_statements(symbol: str):
        try:
            return service.refresh_company_statements(symbol)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/admin/jobs")
    def jobs(limit: int = Query(20, ge=1, le=200)):
        return {"items": service.metadata_repository.latest_runs(limit)}

    def require_csv_import_service() -> CsvImportService:
        value = app.state.csv_import_service
        if value is None:
            raise HTTPException(
                status_code=503, detail="CSV import service unavailable"
            )
        return value

    def public_csv_import_job(job: Dict[str, Any]) -> Dict[str, Any]:
        public = {
            key: value for key, value in job.items()
            if key not in {"storage_path", "request_json"}
        }
        if public.get("error_message"):
            public["error_message"] = _safe_error_message(
                public["error_message"]
            )
        public["shards"] = [{
            **shard,
            "error_message": (
                _safe_error_message(shard["error_message"])
                if shard.get("error_message") else shard.get("error_message")
            ),
        } for shard in public.get("shards") or ()]
        return public

    csv_upload_root = settings.storage_root / "csv-import-uploads"

    def csv_upload_path(upload_id: str) -> Path:
        if re.fullmatch(r"[0-9a-f]{32}", upload_id) is None:
            raise HTTPException(status_code=400, detail="invalid CSV upload id")
        path = csv_upload_root / f"{upload_id}.csv"
        if not path.is_file():
            raise HTTPException(status_code=404, detail="CSV upload not found")
        return path

    def csv_input_path(request: CsvImportSourceInput) -> Path | str:
        if request.upload_id:
            return csv_upload_path(request.upload_id)
        return request.path or ""

    def inspect_uploaded_csv(path: Path) -> tuple[list[str], str]:
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as stream:
            sample = stream.read(65536)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        except csv.Error:
            dialect = csv.excel
        header = next(csv.reader(sample.splitlines(), dialect), [])
        return [value.strip() for value in header], dialect.delimiter

    @app.post("/v1/admin/csv-imports/upload", status_code=201)
    async def upload_csv_import(
        request: Request,
        filename: str = Query(..., min_length=1, max_length=255),
    ):
        safe_name = Path(filename).name
        if (
            safe_name != filename
            or not safe_name.lower().endswith(".csv")
            or any(ord(character) < 32 for character in safe_name)
        ):
            raise HTTPException(
                status_code=400, detail="filename must be a plain .csv filename"
            )
        content_type = request.headers.get(
            "content-type", ""
        ).split(";", 1)[0].lower()
        if content_type not in {
            "text/csv", "application/csv", "application/octet-stream",
        }:
            raise HTTPException(
                status_code=415, detail="CSV upload content type is unsupported"
            )
        declared_size = request.headers.get("content-length")
        if declared_size:
            try:
                if int(declared_size) > settings.csv_import_max_file_bytes:
                    raise HTTPException(
                        status_code=413,
                        detail="CSV file exceeds the configured size limit",
                    )
            except ValueError as exc:
                raise HTTPException(
                    status_code=400, detail="invalid Content-Length"
                ) from exc

        upload_id = uuid.uuid4().hex
        csv_upload_root.mkdir(parents=True, exist_ok=True)
        destination = csv_upload_root / f"{upload_id}.csv"
        partial = csv_upload_root / f".{upload_id}.part"
        digest = hashlib.sha256()
        byte_size = 0
        try:
            with partial.open("xb") as stream:
                async for chunk in request.stream():
                    if not chunk:
                        continue
                    byte_size += len(chunk)
                    if byte_size > settings.csv_import_max_file_bytes:
                        raise HTTPException(
                            status_code=413,
                            detail="CSV file exceeds the configured size limit",
                        )
                    digest.update(chunk)
                    stream.write(chunk)
                stream.flush()
                os.fsync(stream.fileno())
            if byte_size == 0:
                raise HTTPException(
                    status_code=400, detail="CSV file must not be empty"
                )
            os.replace(partial, destination)
        except BaseException:
            partial.unlink(missing_ok=True)
            raise
        columns, delimiter = inspect_uploaded_csv(destination)
        return {
            "upload_id": upload_id,
            "filename": safe_name,
            "byte_size": byte_size,
            "sha256": digest.hexdigest(),
            "columns": columns,
            "delimiter": delimiter,
        }

    @app.post("/v1/admin/csv-imports/dry-run")
    def dry_run_csv_import(request: CsvImportDryRunInput):
        try:
            return require_csv_import_service().dry_run(
                csv_input_path(request), request.declaration.declaration(),
                max_error_samples=request.max_error_samples,
            )
        except (ValueError, OSError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v1/admin/csv-imports/infer")
    def infer_csv_import(request: CsvImportInferenceInput):
        try:
            instrument = canonical_instrument(request.instrument_id)
            return infer_csv_semantics(
                csv_upload_path(request.upload_id),
                mic=instrument.mic,
                columns=request.columns,
                timestamp_format=request.timestamp_format,
                encoding=request.encoding,
                delimiter=request.delimiter,
            )
        except (ValueError, OSError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v1/admin/csv-imports")
    def create_csv_import(request: CsvImportCreateInput):
        path = csv_input_path(request)
        try:
            job, created = require_csv_import_service().create_import(
                path, request.declaration.declaration(),
                idempotency_key=request.idempotency_key,
                chunk_rows=request.chunk_rows,
                max_attempts=request.max_attempts,
            )
            if request.upload_id:
                Path(path).unlink(missing_ok=True)
            return {"created": created, "job": public_csv_import_job(job)}
        except (ValueError, OSError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v1/admin/csv-imports")
    def list_csv_imports(
        limit: int = Query(50, ge=1, le=500),
        manifest_id: str = Query("", max_length=64),
    ):
        rows = [
            public_csv_import_job(row)
            for row in require_csv_import_service().list(limit, manifest_id)
        ]
        return {"count": len(rows), "items": rows}

    @app.get("/v1/admin/csv-imports/{job_id}")
    def get_csv_import(job_id: str):
        job = require_csv_import_service().get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="CSV import job not found")
        return public_csv_import_job(job)

    @app.post("/v1/admin/csv-imports/{job_id}/cancel")
    def cancel_csv_import(job_id: str):
        job = require_csv_import_service().cancel(job_id)
        if job is None:
            current = require_csv_import_service().get(job_id)
            if current is None:
                raise HTTPException(
                    status_code=404, detail="CSV import job not found"
                )
            raise HTTPException(
                status_code=409,
                detail=f"CSV import job is already {current['status']}",
            )
        return public_csv_import_job(job)

    @app.post("/v1/admin/csv-imports/{job_id}/retry")
    def retry_csv_import(job_id: str, request: CsvImportRetryInput):
        try:
            job, created = require_csv_import_service().retry(
                job_id, idempotency_key=request.idempotency_key
            )
            return {"created": created, "job": public_csv_import_job(job)}
        except ValueError as exc:
            status = 404 if "not found" in str(exc) else 409
            raise HTTPException(status_code=status, detail=str(exc)) from exc

    @app.get("/v1/admin/csv-imports/{job_id}/manifest")
    def get_csv_import_manifest(job_id: str):
        manifest = require_csv_import_service().manifest(job_id)
        if manifest is None:
            raise HTTPException(status_code=404, detail="CSV import job not found")
        return manifest

    @app.get("/v1/admin/csv-imports/{job_id}/quality-report")
    def get_csv_import_quality_report(job_id: str):
        service_value = require_csv_import_service()
        if service_value.get(job_id) is None:
            raise HTTPException(status_code=404, detail="CSV import job not found")
        report = service_value.quality_report(job_id)
        if report is None:
            raise HTTPException(
                status_code=409, detail="CSV import quality report is not ready"
            )
        return report

    @app.get("/v1/admin/csv-imports/{job_id}/errors")
    def get_csv_import_errors(job_id: str):
        errors = require_csv_import_service().errors(job_id)
        if errors is None:
            raise HTTPException(status_code=404, detail="CSV import job not found")
        return {"count": len(errors), "items": errors}

    def require_history_manager() -> HistoryJobManager:
        if history_manager is None:
            raise HTTPException(status_code=503, detail="history job store unavailable")
        return history_manager

    @app.post("/v1/admin/history-jobs")
    def create_history_job(
        request: HistoryJobRequest,
        http_request: Request,
        x_request_id: str = Header("", max_length=120),
    ):
        payload = request.model_dump()
        actor = authenticated_actor(http_request)
        try:
            job, created = require_history_manager().create(payload)
        except Exception as exc:
            admin_audit.append(
                actor=actor, action="history_job.create",
                target=request.idempotency_key, outcome="failed",
                parameters={"symbols": request.symbols, "provider": request.provider},
                request_id=x_request_id, detail=str(exc),
            )
            raise
        admin_audit.append(
            actor=actor, action="history_job.create",
            target=job["job_id"], outcome="accepted" if created else "succeeded",
            parameters={
                "symbols": request.symbols, "provider": request.provider,
                "idempotency_key": request.idempotency_key,
            },
            request_id=x_request_id, detail="created" if created else "idempotent replay",
        )
        return JSONResponse(
            status_code=202,
            content={"job_id": job["job_id"], "created": created, "job": job},
        )

    @app.get("/v1/admin/history-jobs")
    def list_history_jobs(
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0, le=10000),
        status: str = Query("", max_length=40),
    ):
        values = require_history_manager().list(200)
        if status:
            values = [item for item in values if item.get("status") == status]
        page = values[offset:offset + limit]
        return {
            "items": page,
            "page": {
                "limit": limit, "offset": offset, "returned": len(page),
                "total": len(values),
            },
        }

    @app.get("/v1/admin/history-jobs/{job_id}")
    def get_history_job(job_id: str):
        try:
            return require_history_manager().detail(job_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="history job not found") from exc

    @app.post("/v1/admin/history-jobs/{job_id}/cancel")
    def cancel_history_job(
        job_id: str,
        request: Request,
        x_request_id: str = Header("", max_length=120),
    ):
        actor = authenticated_actor(request)
        try:
            result = require_history_manager().cancel(job_id)
        except KeyError as exc:
            admin_audit.append(
                actor=actor, action="history_job.cancel", target=job_id,
                outcome="rejected", request_id=x_request_id, detail="not found",
            )
            raise HTTPException(status_code=404, detail="history job not found") from exc
        admin_audit.append(
            actor=actor, action="history_job.cancel", target=job_id,
            outcome="succeeded", request_id=x_request_id,
        )
        return result

    @app.post("/v1/admin/history-jobs/{job_id}/retry-failed")
    def retry_history_job(
        job_id: str,
        request: Request,
        x_request_id: str = Header("", max_length=120),
    ):
        actor = authenticated_actor(request)
        try:
            result = require_history_manager().retry_failed(job_id)
        except KeyError as exc:
            admin_audit.append(
                actor=actor, action="history_job.retry_failed", target=job_id,
                outcome="rejected", request_id=x_request_id, detail="not found",
            )
            raise HTTPException(status_code=404, detail="history job not found") from exc
        except ValueError as exc:
            admin_audit.append(
                actor=actor, action="history_job.retry_failed", target=job_id,
                outcome="rejected", request_id=x_request_id, detail=str(exc),
            )
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        admin_audit.append(
            actor=actor, action="history_job.retry_failed", target=job_id,
            outcome="succeeded", request_id=x_request_id,
        )
        return result

    @app.post("/v1/admin/history-jobs/{job_id}/reconcile")
    def reconcile_history_job(job_id: str, request: HistoryReconcileRequest):
        if history_reconciler is None:
            raise HTTPException(
                status_code=503, detail="history reconciler unavailable"
            )
        try:
            return history_reconciler.reconcile(
                job_id, dry_run=request.dry_run
            )
        except KeyError as exc:
            raise HTTPException(
                status_code=404, detail="history job not found"
            ) from exc

    @app.get("/v1/admin/history-consistency")
    def audit_history_consistency(
        limit: int = Query(10000, ge=1, le=100000),
    ):
        if history_consistency_auditor is None:
            raise HTTPException(
                status_code=503,
                detail="history consistency auditor unavailable",
            )
        return history_consistency_auditor.audit(limit)

    @app.get("/v1/admin/history-health")
    def history_health():
        return require_history_manager().health_snapshot()

    @app.get("/v1/admin/history-jobs-ui", response_class=HTMLResponse)
    def history_jobs_ui():
        return """<!doctype html><html><head><meta charset="utf-8">
<title>MarketCow History Jobs</title><style>
body{font:14px system-ui;margin:24px;background:#f6f7f9;color:#17202a}
table{border-collapse:collapse;width:100%;background:white;margin:12px 0}
th,td{padding:8px;border:1px solid #dfe3e8;text-align:left}
progress{width:180px} pre{white-space:pre-wrap}
button.cancel{margin-left:12px;padding:6px 10px;border:1px solid #b42318;
border-radius:4px;background:#fff;color:#b42318;cursor:pointer}
button.cancel:disabled{cursor:wait;opacity:.55}
#notice{min-height:20px;color:#344054}</style></head><body>
<h1>History fetch jobs</h1><div id="notice" role="status" aria-live="polite"></div>
<div id="jobs">Loading…</div><script>
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;',
 '>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const cancelable=s=>s==='queued'||s==='running';
async function cancelJob(button){
 const id=button.dataset.job;
 if(!confirm(`Cancel history job ${id}? Completed data will be kept.`))return;
 button.disabled=true;document.getElementById('notice').textContent=`Canceling ${id}…`;
 try{const r=await fetch(`/v1/admin/history-jobs/${encodeURIComponent(id)}/cancel`,
  {method:'POST'});
  const d=await r.json();if(!r.ok)throw new Error(d.detail||`HTTP ${r.status}`);
  document.getElementById('notice').textContent=`Cancel requested for ${id}.`;
  await load();
 }catch(e){document.getElementById('notice').textContent=
  `Could not cancel ${id}: ${e.message}`;button.disabled=false}
}
async function load(){const r=await fetch('/v1/admin/history-jobs?limit=50');
const d=await r.json();document.getElementById('jobs').innerHTML=d.items.map(j=>
`<section><h2>${esc(j.job_id)} — ${esc(j.status)}
${cancelable(j.status)?`<button class="cancel" data-job="${esc(j.job_id)}"
type="button">Cancel job</button>`:''}</h2>
<progress max="100" value="${j.progress_percent}"></progress> ${j.progress_percent}%
<p>${j.completed_symbols}/${j.total_symbols}; fetched ${j.rows_fetched};
persisted ${j.rows_persisted}; shards ${j.completed_shards}/${j.total_shards};
splits ${j.coverage_split_events}; unproven ${j.coverage_unproven_shards};
updated ${esc(j.updated_at)}</p><table><tr>
<th>Symbol</th><th>Status</th><th>Provider/source</th><th>Attempt</th>
<th>Rows</th><th>Canonical</th><th>Owner / lease</th><th>Heartbeat</th>
<th>Takeovers</th><th>Error</th></tr>${j.items.map(i=>`<tr>
<td>${esc(i.symbol)}</td><td>${esc(i.status)}</td>
<td>${esc(i.provider)}/${esc(i.source)}</td><td>${i.attempt}</td>
<td>${i.rows_fetched}/${i.rows_persisted}</td><td>${esc(i.canonical_status)}</td>
<td>${esc(i.owner_id)} / ${esc(i.lease_active?'active':i.lease_expires_at)}</td>
<td>${esc(i.heartbeat_at)}</td><td>${esc(i.takeover_count)}</td>
<td>${esc(i.error_code)} ${esc(i.error_message)}</td></tr>`).join('')}</table></section>`
).join('')||'No jobs'}
document.getElementById('jobs').addEventListener('click',e=>{
 const button=e.target.closest('button.cancel');if(button)cancelJob(button)});
load();setInterval(load,2000);</script></body></html>"""

    @app.get("/v1/admin/csv-imports-ui", response_class=HTMLResponse)
    def csv_imports_ui():
        return """<!doctype html><html><head><meta charset="utf-8">
<title>MarketCow CSV Imports</title><style>
body{font:14px system-ui;margin:24px;background:#f6f7f9;color:#17202a}
form,section{background:white;padding:16px;margin:12px 0;border:1px solid #dfe3e8}
label{display:block;margin:8px 0}input,select,textarea{width:100%;max-width:900px}
textarea{height:90px;font-family:monospace}button{margin:8px 8px 8px 0;padding:7px 12px}
table{border-collapse:collapse;width:100%}th,td{padding:7px;border:1px solid #ddd}
pre{white-space:pre-wrap;max-height:360px;overflow:auto}.danger{color:#b42318}</style>
</head><body><h1>CSV history imports</h1>
<p>Only server-local files under MARKETCOW_ALLOWED_ROOT are accepted. Run dry-run first.</p>
<form id="form"><label>CSV path<input id="path" required></label>
<label>Source<input id="source" value="purchased_vendor" required></label>
<label>Namespace<input id="namespace" value="provider:purchased_vendor" required></label>
<label>Instrument mappings (provider symbol → SYMBOL.MIC)
<textarea id="mappings">{"AAPL.US":"AAPL.XNAS"}</textarea></label>
<label>Columns (canonical → CSV column)
<textarea id="columns">{"symbol":"symbol","timestamp":"timestamp","open":"open","high":"high","low":"low","close":"close","volume":"volume"}</textarea></label>
<label>Timezone<input id="timezone" value="America/New_York"></label>
<label>Timestamp format<input id="timestampFormat" value="iso8601"></label>
<label>Interval<select id="interval"><option>1m</option><option>5m</option>
<option>15m</option><option>1h</option><option>1d</option></select></label>
<label>Adjustment<select id="adjustment"><option>raw</option>
<option>qfq</option><option>hfq</option></select></label>
<label>Idempotency key<input id="key" value="csv-import-"></label>
<button type="button" id="dry">Dry-run</button>
<button type="button" id="start">Start import</button></form>
<pre id="result" role="status"></pre><section><h2>Jobs</h2><div id="jobs"></div></section>
<script>
const $=id=>document.getElementById(id),esc=s=>String(s??'').replace(/[&<>"']/g,
c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function payload(){return{path:$('path').value,declaration:{
source:$('source').value,interval:$('interval').value,
adjustment:$('adjustment').value,profile:{name:$('source').value,version:'1',
columns:JSON.parse($('columns').value),timezone_name:$('timezone').value,
timestamp_format:$('timestampFormat').value,encoding:'utf-8-sig',delimiter:','},
instruments:{namespace:$('namespace').value,
symbols:JSON.parse($('mappings').value)}}}}
async function call(url,body){const r=await fetch(url,{method:'POST',
headers:{'content-type':'application/json'},body:JSON.stringify(body)});
const d=await r.json();if(!r.ok)throw new Error(JSON.stringify(d.detail));return d}
$('dry').onclick=async()=>{try{$('result').textContent=JSON.stringify(
await call('/v1/admin/csv-imports/dry-run',payload()),null,2)}
catch(e){$('result').textContent=e.message}}
$('start').onclick=async()=>{if(!confirm('Start durable CSV import?'))return;
try{const p=payload();p.idempotency_key=$('key').value;p.chunk_rows=100000;
p.max_attempts=3;$('result').textContent=JSON.stringify(
await call('/v1/admin/csv-imports',p),null,2);await load()}
catch(e){$('result').textContent=e.message}}
async function cancelJob(id){if(!confirm(`Cancel CSV import ${id}?`))return;
try{await call(`/v1/admin/csv-imports/${encodeURIComponent(id)}/cancel`,{});await load()}
catch(e){$('result').textContent=e.message}}
async function retryJob(id){if(!confirm(`Retry CSV import ${id} using the same immutable manifest?`))return;
try{const key=`retry-${id}-${Date.now()}`;$('result').textContent=JSON.stringify(
await call(`/v1/admin/csv-imports/${encodeURIComponent(id)}/retry`,
{idempotency_key:key}),null,2);await load()}
catch(e){$('result').textContent=e.message}}
async function load(){const r=await fetch('/v1/admin/csv-imports?limit=50');
const d=await r.json();$('jobs').innerHTML=`<table><tr><th>Job</th><th>Status</th>
<th>Rows read/written</th><th>Quality</th><th>Updated</th><th>Evidence</th>
<th>Action</th></tr>`+
d.items.map(j=>`<tr><td>${esc(j.job_id)}</td><td>${esc(j.status)}</td>
<td><progress max="100" value="${j.progress_percent||0}"></progress>
 ${j.progress_percent||0}% — ${j.rows_read}/${j.rows_written}</td>
<td>${esc(j.quality_report_json?.status||'pending')}</td><td>${esc(j.updated_at)}</td>
<td><a href="/v1/admin/csv-imports/${encodeURIComponent(j.job_id)}/manifest">Manifest</a>
 ${j.quality_report_json?`<a href="/v1/admin/csv-imports/${encodeURIComponent(j.job_id)}/quality-report">Quality</a>`:''}
 <a href="/v1/admin/csv-imports/${encodeURIComponent(j.job_id)}/errors">Errors</a></td>
<td>${['queued','running','cancel_requested'].includes(j.status)?
`<button onclick="cancelJob('${esc(j.job_id)}')">Cancel</button>`:
['failed','canceled'].includes(j.status)?
`<button onclick="retryJob('${esc(j.job_id)}')">Retry</button>`:''}</td></tr>`).join('')
+'</table>'}load();setInterval(load,2000)</script></body></html>"""

    @app.get("/v1/admin/artifacts")
    def artifacts(dataset: str = "", limit: int = Query(100, ge=1, le=1000)):
        rows = service.artifact_store.list_artifacts(dataset, limit)
        return {"count": len(rows), "items": rows}

    @app.post("/v1/admin/baostock/{symbol}/refresh")
    def refresh_baostock(symbol: str, report_period: str):
        try:
            return service.refresh_baostock(symbol, report_period)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/v1/admin/tdx/financials/sync")
    def sync_tdx_financials(
        limit_periods: int = Query(12, ge=1, le=40),
        report_periods: str = "",
    ):
        periods = [item.strip() for item in report_periods.split(",") if item.strip()]
        try:
            return service.sync_tdx_financials(limit_periods, periods or None)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.get("/v1/sources/tdx/coverage")
    def tdx_coverage():
        return {"periods": service.fundamental_repository.tdx_coverage()}

    @app.get("/v1/validation/{symbol}")
    def validation(symbol: str, report_period: str):
        try:
            return service.validate_company(symbol, report_period)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/v1/fundamentals/{symbol}/history")
    def fundamental_history(
        symbol: str,
        annual_only: bool = False,
        limit: int = Query(40, ge=1, le=100),
        as_of: str = "",
    ):
        code = "".join(ch for ch in symbol if ch.isdigit()).zfill(6)
        normalized_as_of = parse_as_of(as_of)
        rows = service.fundamental_repository.get_tdx_history(code, annual_only, limit, normalized_as_of)
        return {"symbol": code, "count": len(rows), "as_of": normalized_as_of or None, "point_in_time": bool(normalized_as_of), "items": rows}

    @app.get("/v1/sources/health")
    def source_health():
        return {"items": service.metadata_repository.provider_health()}

    @app.get("/v1/validation/{symbol}/results")
    def validation_results(symbol: str, report_period: str):
        code = "".join(ch for ch in symbol if ch.isdigit()).zfill(6)
        period = normalize_report_period(report_period)
        rows = service.fundamental_repository.get_validation_results(code, period)
        return {"symbol": code, "report_period": period, "count": len(rows), "items": rows}

    @app.post("/v1/admin/validation/rebuild")
    def rebuild_validation(report_period: str):
        try:
            return service.rebuild_cached_validation(report_period)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/v1/admin/funnel/metrics/rebuild")
    def rebuild_funnel_metrics():
        return service.rebuild_funnel_metrics()

    @app.get("/v1/funnel/metrics")
    def funnel_metrics(
        limit: int = Query(100, ge=1, le=5000),
        offset: int = Query(0, ge=0),
        min_roe_median: Optional[float] = None,
        min_revenue_cagr: Optional[float] = None,
        min_profit_cagr: Optional[float] = None,
        max_pe: Optional[float] = None,
        max_debt_ratio: Optional[float] = None,
        min_annual_periods: int = Query(0, ge=0, le=20),
        active_only: bool = True,
        as_of: str = "",
    ):
        normalized_as_of = parse_as_of(as_of)
        rows = service.fundamental_repository.query_funnel_metrics(
            limit=limit,
            offset=offset,
            min_roe_median=min_roe_median,
            min_revenue_cagr=min_revenue_cagr,
            min_profit_cagr=min_profit_cagr,
            max_pe=max_pe,
            max_debt_ratio=max_debt_ratio,
            min_annual_periods=min_annual_periods,
            active_only=active_only,
            as_of=normalized_as_of,
        )
        return {"count": len(rows), "limit": limit, "offset": offset, "as_of": normalized_as_of or None, "point_in_time": bool(normalized_as_of), "items": rows}

    admin_dist = Path(__file__).resolve().parents[2] / "web" / "dist"
    if settings.admin_frontend_enabled and (admin_dist / "index.html").is_file():
        app.mount(
            "/admin",
            StaticFiles(directory=admin_dist, html=True),
            name="admin-frontend",
        )

    return app
