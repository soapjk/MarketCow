from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, Optional
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, ValidationError

from . import __version__
from .config import Settings
from .market_bar_cursor import decode_cursor, encode_cursor, load_or_create_secret
from .normalize import normalize_as_of, normalize_report_period
from .providers.yahoo_quote import normalize_yahoo_symbol
from .providers.eastmoney_realtime import normalize_a_symbol
from .service import FundamentalService
from .telemetry import sanitize_text, telemetry_call
from .health import HealthEvaluator
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
from .providers.longport_quote import LongPortError


class TushareRequest(BaseModel):
    params: Dict[str, Any] = Field(default_factory=dict)
    fields: str = ""


class TushareRealtimeRequest(BaseModel):
    ts_code: str


class ProviderPolicy(BaseModel):
    provider: Optional[str] = None
    allow_fallback: bool = False


class QuoteQuery(ProviderPolicy):
    symbols: list[str] = Field(min_length=1, max_length=20)
    refresh: bool = False


class MarketBarQuery(ProviderPolicy):
    symbols: list[str] = Field(min_length=1, max_length=20)
    range: str = "1y"
    interval: str = "1d"
    adjustment: str = Field(default="adjusted", pattern="^(adjusted|raw)$")
    refresh: bool = True
    limit: int = Field(default=500, ge=1, le=5000)


class DividendAnnouncementInput(BaseModel):
    symbol: str
    fiscal_year: int = Field(ge=1990, le=2100)
    amount_per_share: str
    currency: str
    announcement_date: str
    expected_payment_date: Optional[str] = None
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


def create_app(
    settings: Optional[Settings] = None,
    service: Optional[FundamentalService] = None,
    now_provider: Optional[Callable[[], datetime]] = None,
    realtime_hub: Optional[RealtimeHub] = None,
) -> FastAPI:
    settings = settings or Settings.from_env()
    service = service or FundamentalService(settings)
    app = FastAPI(title="MarketCow", version=__version__)
    app.state.service = service
    clock = now_provider or (lambda: datetime.now(timezone.utc))
    provider = LongPortRealtimeProvider(
        settings.longport_app_key, settings.longport_app_secret,
        settings.longport_access_token,
        enable_overnight=settings.longport_enable_overnight,
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

    async def shutdown() -> None:
        try:
            await hub.close()
        finally:
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
            "storage_health": storage_health(),
        }

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

    @app.get("/v1/instruments/{instrument_id}")
    def get_instrument(instrument_id: str):
        row = service.metadata_repository.get_instrument(instrument_id)
        if row is None:
            raise HTTPException(status_code=404, detail={
                "code": "instrument_not_found", "instrument_id": instrument_id,
            })
        return instrument_record(row)

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

    @app.get("/v1/canonical-bars/{instrument_id}")
    def canonical_bars_v1(
        instrument_id: str,
        start: str,
        end: str,
        interval: str,
        adjustment: str = Query(pattern="^(raw|adjusted)$"),
        page_size: int = Query(ge=1, le=5000),
        cursor: Optional[str] = None,
    ):
        instrument = service.metadata_repository.get_instrument(instrument_id)
        if instrument is None:
            raise HTTPException(status_code=404, detail={
                "code": "instrument_not_found", "instrument_id": instrument_id,
            })
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
            identity = service.market_bar_repository.get_canonical_dataset_identity(
                instrument["symbol"], storage_interval, adjustment, start_utc, end_utc
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
                instrument["symbol"], storage_interval, adjustment, start_utc, end_utc,
                page_size, after,
            )
            bars = []
            for row in rows:
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
                    selected_source=row["selected_source"],
                    quality_status=row["quality_status"],
                    row_version=str(row["version"]),
                ))
            confirmed = service.market_bar_repository.get_canonical_dataset_identity(
                instrument["symbol"], storage_interval, adjustment, start_utc, end_utc
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
                try:
                    normalized = normalize_a_symbol(symbol)
                except ValueError:
                    normalized, _ = normalize_yahoo_symbol(symbol)
                normalized_symbols.append(normalized)
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

    @app.get("/v1/quotes/{symbol}/history")
    def quote_history(
        symbol: str,
        range_: str = Query("1y", alias="range"),
        interval: str = "1d",
        adjustment: str = Query("adjusted", pattern="^(adjusted|raw)$"),
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
            if interval in {"1m", "5m", "15m", "30m", "60m", "1h"}:
                try:
                    normalized = normalize_a_symbol(symbol)
                except ValueError:
                    normalized, _ = normalize_yahoo_symbol(symbol)
            else:
                normalized, _ = normalize_yahoo_symbol(symbol)
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
        adjustment: str = "adjusted",
        limit: int = 500,
        symbols: Optional[str] = None,
        page_size: Optional[int] = Query(None, ge=1, le=5000),
        cursor: Optional[str] = None,
    ):
        try:
            if adjustment not in {"adjusted", "raw"}:
                raise ValueError("adjustment must be adjusted or raw")
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
        adjustment: str = "adjusted",
        page_size: int = Query(500, ge=1, le=5000),
        cursor: Optional[str] = None,
    ):
        try:
            if adjustment not in {"adjusted", "raw"}:
                raise ValueError("adjustment must be adjusted or raw")
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
        adjustment: str = "adjusted",
        max_lookback_seconds: int = Query(86400, ge=1, le=31_536_000),
        page_size: int = Query(500, ge=1, le=1000),
        cursor: Optional[str] = None,
    ):
        try:
            if adjustment not in {"adjusted", "raw"}:
                raise ValueError("adjustment must be adjusted or raw")
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
        adjustment: str = "adjusted",
        max_lookback_seconds: int = Query(86400, ge=1, le=31_536_000),
    ):
        try:
            if adjustment not in {"adjusted", "raw"}:
                raise ValueError("adjustment must be adjusted or raw")
            try:
                normalized = normalize_a_symbol(symbol)
            except ValueError:
                normalized, _ = normalize_yahoo_symbol(symbol)
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
            if adjustment not in {"adjusted", "raw"}:
                raise ValueError("adjustment must be adjusted or raw")
            if not 1 <= limit <= 5000:
                raise ValueError("raw history limit must be between 1 and 5000")
            if cursor is not None and page_size is None:
                raise ValueError("raw history cursor requires page_size")
            try:
                normalized = normalize_a_symbol(symbol)
            except ValueError:
                normalized, _ = normalize_yahoo_symbol(symbol)
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
            try:
                normalized = normalize_a_symbol(symbol)
            except ValueError:
                normalized, _ = normalize_yahoo_symbol(symbol)
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

    return app
