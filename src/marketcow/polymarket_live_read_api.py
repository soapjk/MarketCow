from __future__ import annotations

import asyncio
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Callable, NoReturn, TypeVar

from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from starlette.responses import FileResponse, Response
from .polymarket_stream_metrics import StreamMetrics

from .polymarket_live import (
    PUBLIC_DATA_KINDS,
    CandidateSnapshot,
    LiveBootstrapResponse,
    LiveCheckpoint,
    LiveEventPage,
    LiveFullSyncResponse,
    LiveGapPage,
    LiveReadHealth,
    LiveSnapshotPage,
    PolymarketLiveReadError,
    PolymarketLiveReadStore,
    PublicDataPage,
)
from .polymarket_live_stream import (
    PolymarketLiveProjection,
    PolymarketLiveStreamClient,
)
from .polymarket_events_observability import PolymarketReadTraceMiddleware
from .polymarket_configured_scope import PolymarketConfiguredScope
from .polymarket_discovery import (
    DEFAULT_MAXIMUM_FULL_SYNC_BYTES,
    DISCOVERY_EVENT_SCHEMA_VERSION,
    DiscoveryFullSync,
    LifecycleHistoryPage,
    PolymarketDiscoveryStore,
    install_discovery_openapi_extension,
)

T = TypeVar("T")


async def _send_live_message(websocket, message, metrics=None):
    try:
        if metrics is None:
            await websocket.send_json(message)
        else:
            started = time.perf_counter()
            # Match Starlette send_json text encoding exactly.
            encoded = json.dumps(message, separators=(',', ':'), ensure_ascii=False)
            metrics.record('json_encode', time.perf_counter() - started)
            started = time.perf_counter()
            await websocket.send_text(encoded)
            metrics.record('socket_send', time.perf_counter() - started)
        # ASGI sends may complete synchronously while the socket is writable.
        # Give source ingestion a turn even during an uninterrupted replay.
        started = time.perf_counter()
        await asyncio.sleep(0)
        if metrics is not None:
            metrics.record('send_yield', time.perf_counter() - started)
        return True
    except WebSocketDisconnect:
        return False
    except RuntimeError as exc:
        # Uvicorn can have already closed on a keepalive failure. Do not mask
        # unrelated application/runtime exceptions or send a second error.
        if "Unexpected ASGI message 'websocket.send', after sending 'websocket.close'" in str(exc):
            return False
        raise


def create_polymarket_live_read_app(
    *,
    root: Path,
    discovery_root: Path,
    stable_snapshot_max_book_age_seconds: float,
    stable_read_wait_seconds: float,
    stable_read_poll_seconds: float,
    executor_workers: int,
    discovery_depth_notionals: tuple[str, ...],
    discovery_maximum_book_age_ms: int,
    discovery_maximum_full_sync_bytes: int = DEFAULT_MAXIMUM_FULL_SYNC_BYTES,
    live_stream_uri: str = "",
    live_stream_replay_capacity: int = 10_000,
    consumer_maximum_book_age_seconds: float | None = None,
    minimum_delivery_headroom_seconds: float = 0.0,
    configured_scope_path: Path | None = None,
) -> FastAPI:
    if not root.is_absolute():
        raise ValueError("Polymarket live read root must be absolute")
    if not discovery_root.is_absolute():
        raise ValueError("Polymarket discovery root must be absolute")
    if stable_snapshot_max_book_age_seconds <= 0:
        raise ValueError("maximum book age must be positive")
    if stable_read_wait_seconds <= 0:
        raise ValueError("stable read wait must be positive")
    if stable_read_poll_seconds <= 0:
        raise ValueError("stable read poll must be positive")
    if executor_workers < 2:
        raise ValueError("at least two read executor workers are required")

    reader = PolymarketLiveReadStore(
        root,
        stable_snapshot_max_book_age_seconds=(
            stable_snapshot_max_book_age_seconds
        ),
        stable_read_wait_seconds=stable_read_wait_seconds,
        stable_read_poll_seconds=stable_read_poll_seconds,
        consumer_maximum_book_age_seconds=(
            consumer_maximum_book_age_seconds
            if consumer_maximum_book_age_seconds is not None
            else stable_snapshot_max_book_age_seconds
        ),
        minimum_delivery_headroom_seconds=minimum_delivery_headroom_seconds,
    )
    discovery_reader = PolymarketLiveReadStore(
        discovery_root,
        stable_snapshot_max_book_age_seconds=(
            stable_snapshot_max_book_age_seconds
        ),
        stable_read_wait_seconds=stable_read_wait_seconds,
        stable_read_poll_seconds=stable_read_poll_seconds,
        consumer_maximum_book_age_seconds=(
            consumer_maximum_book_age_seconds
            if consumer_maximum_book_age_seconds is not None
            else stable_snapshot_max_book_age_seconds
        ),
        minimum_delivery_headroom_seconds=minimum_delivery_headroom_seconds,
    )
    discovery = PolymarketDiscoveryStore(
        discovery_reader,
        depth_notionals=discovery_depth_notionals,
        maximum_book_age_ms=discovery_maximum_book_age_ms,
        maximum_full_sync_bytes=discovery_maximum_full_sync_bytes,
    )
    executor = ThreadPoolExecutor(
        max_workers=executor_workers,
        thread_name_prefix="marketcow-polymarket-live-read",
    )
    projection = PolymarketLiveProjection(
        replay_capacity=live_stream_replay_capacity
    )
    stream_client = (
        PolymarketLiveStreamClient(live_stream_uri, projection)
        if live_stream_uri else None
    )
    stream_stop = asyncio.Event()
    stream_task: asyncio.Task[None] | None = None
    loop_monitor_task: asyncio.Task[None] | None = None

    async def monitor_event_loop() -> None:
        interval = 0.05
        loop = asyncio.get_running_loop()
        expected = loop.time() + interval
        while not stream_stop.is_set():
            await asyncio.sleep(interval)
            observed = loop.time()
            projection.observe_event_loop_stall((observed - expected) * 1000)
            expected = observed + interval

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        nonlocal stream_task, loop_monitor_task
        discovery.start_background_materialization()
        if stream_client is not None:
            stream_task = asyncio.create_task(
                stream_client.run(stream_stop),
                name="polymarket-live-stream-client",
            )
            loop_monitor_task = asyncio.create_task(
                monitor_event_loop(), name="polymarket-event-loop-stall-monitor"
            )
        try:
            yield
        finally:
            stream_stop.set()
            discovery.stop_background_materialization()
            if stream_task is not None:
                stream_task.cancel()
                await asyncio.gather(stream_task, return_exceptions=True)
            if loop_monitor_task is not None:
                loop_monitor_task.cancel()
                await asyncio.gather(loop_monitor_task, return_exceptions=True)
            executor.shutdown(wait=True, cancel_futures=True)

    app = FastAPI(
        title="MarketCow Polymarket Live Read API",
        lifespan=lifespan,
    )
    app.add_middleware(PolymarketReadTraceMiddleware)
    app.state.polymarket_live_read = reader
    app.state.polymarket_live_read_executor = executor
    app.state.polymarket_live_projection = projection
    app.state.polymarket_discovery = discovery
    app.state.polymarket_discovery_read = discovery_reader

    def require_scope_identity(requested_scope_id: str | None) -> None:
        current_scope_id = projection.scope_id or reader.scope_id
        if requested_scope_id and requested_scope_id != current_scope_id:
            raise PolymarketLiveReadError(
                "polymarket_scope_retired",
                "Requested scope is no longer active; discover scope_id and re-bootstrap",
                410,
            )

    def load_configured_scope() -> PolymarketConfiguredScope:
        if configured_scope_path is None:
            raise ValueError("configured scope is unavailable")
        path = configured_scope_path.resolve(strict=True)
        if not path.is_relative_to(root.resolve()):
            raise ValueError("configured scope escapes live root")
        raw = path.read_bytes()
        scope = PolymarketConfiguredScope.model_validate_json(raw)
        runtime = json.loads((root / "scope-runtime.json").read_bytes())
        catalog = json.loads((root / "catalog.json").read_bytes())
        if (runtime["manifest_sha256"] != hashlib.sha256(raw).hexdigest()
                or runtime["scope_id"] != scope.active_scope_id
                or catalog["catalog_revision"] != scope.catalog_revision):
            raise ValueError("configured scope binding differs")
        return scope

    def resolve_market_scope(
        market_id: list[str] | None,
        scope_id: str | None,
    ) -> list[str]:
        require_scope_identity(scope_id)
        if market_id:
            # Caller-selected reads retain the public 100-market request cap.
            return PolymarketLiveReadStore._scope(market_id)
        if scope_id is None or configured_scope_path is None:
            return _require_scope(market_id)
        configured = load_configured_scope()
        if scope_id != configured.active_scope_id:
            raise PolymarketLiveReadError(
                "polymarket_scope_retired",
                "Requested scope is no longer active; discover scope_id and re-bootstrap",
                410,
            )
        # The configured scope is already content-addressed and bounded to 250.
        return PolymarketLiveReadStore._scope(
            (item.market_id for item in configured.configured_markets),
            maximum_markets=250,
        )

    @app.get("/v1/prediction-markets/polymarket/live/scope")
    async def scope_discovery():
        if configured_scope_path is not None:
            try:
                scope = load_configured_scope()
                return scope.model_dump(mode="json")
            except (OSError, ValueError, KeyError) as error:
                # Scope failure does not prevent Discovery and other modules serving.
                raise HTTPException(status_code=409, detail={
                    "code": "polymarket_configured_scope_invalid",
                    "message": str(error), "retryable": False,
                }) from error
        return {
            "schema_version": "marketcow.polymarket.scope-discovery.v1",
            "active_scope_id": projection.scope_id or reader.scope_id,
            "scope_status": "active" if (projection.scope_id or reader.scope_id) else "unscoped",
        }

    async def run_read(
        action: Callable[..., T],
        *args: object,
        **kwargs: object,
    ) -> T:
        return await asyncio.get_running_loop().run_in_executor(
            executor,
            partial(action, *args, **kwargs),
        )

    async def run_hot_json(
        action: Callable[..., T], scope: list[str], trace: dict[str, object],
    ) -> tuple[T, dict[str, float]]:
        submitted = time.perf_counter()
        phases: dict[str, float] = {}

        def execute() -> T:
            phases["executor_queue_ms"] = (
                time.perf_counter() - submitted
            ) * 1000
            return action(reader, scope, _phase_ms=phases)

        try:
            result = await run_read(execute)
        finally:
            trace.update(phases)
        return result, phases

    @app.get(
        "/v1/prediction-markets/polymarket/live/bootstrap",
        response_model=LiveBootstrapResponse,
    )
    async def bootstrap(
        request: Request,
        market_id: list[str] | None = Query(default=None),
        scope_id: str | None = Query(default=None),
    ):
        try:
            scope = resolve_market_scope(market_id, scope_id)
            if stream_client is not None:
                body, phases = await run_hot_json(
                    projection.bootstrap_json, scope,
                    request.scope["polymarket_read_trace"],
                )
            else:
                body = await run_read(reader.bootstrap_json, scope)
                phases = {"response_body_bytes": float(len(body))}
            return Response(
                content=body, media_type="application/json",
                headers={"Server-Timing": _server_timing(phases)},
            )
        except PolymarketLiveReadError as exc:
            _raise_read_error(exc)

    @app.get(
        "/v1/prediction-markets/polymarket/live/snapshot",
        response_model=LiveSnapshotPage,
    )
    async def snapshot(
        request: Request,
        market_id: list[str] | None = Query(default=None),
        scope_id: str | None = Query(default=None),
    ):
        try:
            scope = resolve_market_scope(market_id, scope_id)
            if stream_client is not None:
                body, phases = await run_hot_json(
                    projection.snapshot_json, scope,
                    request.scope["polymarket_read_trace"],
                )
            else:
                body = await run_read(reader.snapshot_json, scope)
                phases = {"response_body_bytes": float(len(body))}
            return Response(
                content=body, media_type="application/json",
                headers={"Server-Timing": _server_timing(phases)},
            )
        except PolymarketLiveReadError as exc:
            _raise_read_error(exc)

    @app.get(
        "/v1/prediction-markets/polymarket/live/full-sync",
        response_model=LiveFullSyncResponse,
    )
    async def full_sync(
        request: Request,
        market_id: list[str] | None = Query(default=None),
        scope_id: str | None = Query(default=None),
    ):
        try:
            scope = resolve_market_scope(market_id, scope_id)
            if stream_client is None:
                raise PolymarketLiveReadError(
                    "polymarket_live_stream_not_configured",
                    "Atomic full-sync requires the in-memory live projection",
                    503,
                )
            result, phases = await run_hot_json(
                projection.full_sync_json, scope,
                request.scope["polymarket_read_trace"],
            )
            body = result[0] if isinstance(result, tuple) else result
            return Response(
                content=body, media_type="application/json",
                headers={"Server-Timing": _server_timing(phases)},
            )
        except PolymarketLiveReadError as exc:
            _raise_read_error(exc)

    @app.get(
        "/v1/prediction-markets/polymarket/live/candidates",
        response_model=CandidateSnapshot,
    )
    async def candidates():
        try:
            path, metadata = await run_read(reader.candidate_snapshot_path)
            return FileResponse(
                path,
                media_type="application/json",
                headers={
                    "ETag": f'"{metadata["sha256"]}"',
                    "X-Polymarket-Catalog-Revision": str(
                        metadata["catalog_revision"]
                    ),
                    "X-Polymarket-Payload-SHA256": str(
                        metadata["payload_sha256"]
                    ),
                },
            )
        except PolymarketLiveReadError as exc:
            _raise_read_error(exc)

    @app.get(
        "/v1/prediction-markets/polymarket/live/discovery/full-sync",
        response_model=DiscoveryFullSync,
    )
    async def discovery_full_sync():
        try:
            return await run_read(discovery.full_sync)
        except PolymarketLiveReadError as exc:
            _raise_read_error(exc)

    @app.get("/v1/prediction-markets/polymarket/live/discovery/status")
    async def discovery_status():
        return discovery.materialization_status()

    @app.websocket(
        "/v1/prediction-markets/polymarket/live/discovery/stream"
    )
    async def discovery_stream(
        socket: WebSocket,
        after_cursor: int = Query(default=0, ge=0),
        projection_id: str = Query(..., pattern=r"^[0-9a-f]{64}$"),
    ):
        await socket.accept()
        cursor = after_cursor
        try:
            while True:
                try:
                    page = await run_read(
                        discovery.events_page, projection_id, cursor, 1000
                    )
                except PolymarketLiveReadError as exc:
                    await socket.send_json({
                        "schema_version": DISCOVERY_EVENT_SCHEMA_VERSION,
                        "type": "resync_required",
                        "cursor": cursor,
                        "reason": exc.code,
                    })
                    await socket.close(code=1012, reason="resync_required")
                    return
                if page.next_cursor != cursor or page.resync_required:
                    await socket.send_text(page.model_dump_json())
                    cursor = page.next_cursor
                if page.resync_required:
                    await socket.close(code=1012, reason="resync_required")
                    return
                if not page.has_more:
                    await asyncio.sleep(0.25)
        except WebSocketDisconnect:
            return

    @app.get(
        "/v1/prediction-markets/polymarket/history/lifecycle-events",
        response_model=LifecycleHistoryPage,
    )
    async def lifecycle_history(
        market_id: str | None = Query(default=None),
        start_at: datetime | None = Query(default=None),
        end_at: datetime | None = Query(default=None),
        after_cursor: int = Query(default=0, ge=0),
        limit: int = Query(default=1000, ge=1, le=10000),
    ):
        try:
            return await run_read(
                discovery.lifecycle_history,
                market_id=market_id,
                start_at=start_at,
                end_at=end_at,
                after_cursor=after_cursor,
                limit=limit,
            )
        except PolymarketLiveReadError as exc:
            _raise_read_error(exc)

    @app.get(
        "/v1/prediction-markets/polymarket/live/events",
        response_model=LiveEventPage,
    )
    async def events(
        after_cursor: int = Query(0, ge=0),
        limit: int = Query(1000, ge=1, le=10000),
        market_id: list[str] | None = Query(default=None),
        scope_id: str | None = Query(default=None),
    ):
        try:
            payload, _, _ = await run_read(
                projection.events_json if stream_client is not None else reader.events_json,
                resolve_market_scope(market_id, scope_id),
                after_cursor,
                limit,
            )
            return Response(content=payload, media_type="application/json")
        except PolymarketLiveReadError as exc:
            _raise_read_error(exc)

    @app.get(
        "/v1/prediction-markets/polymarket/live/checkpoint",
        response_model=LiveCheckpoint,
    )
    async def checkpoint(market_id: list[str] | None = Query(default=None)):
        try:
            return await run_read(reader.checkpoint, _require_scope(market_id))
        except PolymarketLiveReadError as exc:
            _raise_read_error(exc)

    @app.get(
        "/v1/prediction-markets/polymarket/live/health",
        response_model=LiveReadHealth,
    )
    async def health(
        request: Request,
        market_id: list[str] | None = Query(default=None),
        scope_id: str | None = Query(default=None),
    ):
        try:
            require_scope_identity(scope_id)
            if stream_client is not None:
                if market_id is not None:
                    health_scope = market_id
                elif configured_scope_path is not None:
                    configured = await scope_discovery()
                    health_scope = [item["market_id"] for item in configured["configured_markets"]]
                else:
                    health_scope = projection.market_ids()
                body, phases = await run_hot_json(
                    projection.health_json,
                    health_scope,
                    request.scope["polymarket_read_trace"],
                )
                return Response(
                    content=body,
                    media_type="application/json",
                    status_code=200,
                    headers={"Server-Timing": _server_timing(phases)},
                )
            return await run_read(reader.health)
        except PolymarketLiveReadError as exc:
            _raise_read_error(exc)

    @app.get(
        "/v1/prediction-markets/polymarket/live/gaps",
        response_model=LiveGapPage,
    )
    async def gaps(
        unresolved_only: bool = True,
        market_id: list[str] | None = Query(default=None),
    ):
        try:
            return await run_read(
                reader.gaps,
                _require_scope(market_id),
                unresolved_only=unresolved_only,
            )
        except PolymarketLiveReadError as exc:
            _raise_read_error(exc)

    @app.websocket("/v1/prediction-markets/polymarket/live/stream")
    async def live_stream(
        websocket: WebSocket,
        after_cursor: int = Query(0, ge=0),
        market_id: list[str] | None = Query(default=None),
        scope_id: str | None = Query(default=None),
    ):
        await websocket.accept()
        if stream_client is None:
            await websocket.send_json({
                "type": "error",
                "code": "polymarket_live_stream_not_configured",
                "retryable": False,
            })
            await websocket.close(code=1013)
            return
        try:
            scope = resolve_market_scope(market_id, scope_id)
            metrics = StreamMetrics('api_to_consumer')
            async for message in projection.stream_messages(scope, after_cursor, metrics=metrics):
                sending = time.perf_counter()
                if not await _send_live_message(websocket,message,metrics):
                    metrics.emit(message.get('cursor',after_cursor),force=True,error='downstream_closed')
                    return
                metrics.record('encode_send',time.perf_counter()-sending)
                metrics.emit(message.get('cursor',after_cursor))
        except PolymarketLiveReadError as exc:
            sent = await _send_live_message(websocket,{
                "type": "error", "code": exc.code, "message": str(exc),
                "retryable": exc.status_code == 503,
            })
            if sent:
                await websocket.close(code=1013)
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass

    @app.get(
        "/v1/prediction-markets/polymarket/live/public-data/{kind}",
        response_model=PublicDataPage,
    )
    async def public_data(kind: str):
        if kind not in PUBLIC_DATA_KINDS:
            raise HTTPException(
                status_code=400,
                detail={"code": "invalid_polymarket_public_data_kind"},
            )
        return await run_read(_read_public_data, reader.root, kind)

    install_discovery_openapi_extension(app)
    return app


def _require_scope(market_id: list[str] | None) -> list[str]:
    if not market_id:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "polymarket_full_universe_disabled",
                "message": "Use one or more market_id values for bounded reads",
            },
        )
    return market_id


def _raise_read_error(exc: PolymarketLiveReadError) -> NoReturn:
    headers = (
        {"Retry-After": "1"}
        if exc.code in {
            "polymarket_state_index_lagging",
            "polymarket_snapshot_freshness_budget_exhausted",
            "discovery_snapshot_materializing",
            "discovery_cross_revision_boundary",
        }
        else None
    )
    raise HTTPException(
        status_code=exc.status_code,
        detail={
            "code": exc.code,
            "message": str(exc),
            "retryable": exc.status_code == 503,
        },
        headers=headers,
    ) from exc


def _server_timing(phases: dict[str, float]) -> str:
    return ", ".join(
        f'{name.removesuffix("_ms")};dur={float(value):.3f}'
        for name, value in phases.items()
        if name.endswith("_ms")
    )


def _read_public_data(root: Path, kind: str) -> dict[str, object]:
    folder = root / "public-data" / kind
    paths = sorted(folder.glob("*.json"), key=lambda item: item.stat().st_mtime)
    if not paths:
        raise HTTPException(
            status_code=404,
            detail={"code": "polymarket_public_data_not_captured", "kind": kind},
        )
    try:
        body = paths[-1].read_bytes()
        if hashlib.sha256(body).hexdigest() != paths[-1].stem:
            raise ValueError("public Data API fact hash mismatch")
        items = json.loads(body, parse_float=str, parse_int=str)
    except (OSError, ValueError) as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "polymarket_public_data_integrity_failed"},
        ) from exc
    return {
        "schema_version": "marketcow.polymarket.public-data-page.v1",
        "kind": kind,
        "count": len(items),
        "items": items,
    }
