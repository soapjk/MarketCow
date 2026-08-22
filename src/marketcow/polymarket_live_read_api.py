from __future__ import annotations

import asyncio
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from typing import Callable, NoReturn, TypeVar

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from starlette.responses import FileResponse, Response

from .polymarket_live import (
    PUBLIC_DATA_KINDS,
    CandidateSnapshot,
    LiveBootstrapResponse,
    LiveCheckpoint,
    LiveEventPage,
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

T = TypeVar("T")


def create_polymarket_live_read_app(
    *,
    root: Path,
    stable_snapshot_max_book_age_seconds: float,
    stable_read_wait_seconds: float,
    stable_read_poll_seconds: float,
    executor_workers: int,
    live_stream_uri: str = "",
    live_stream_replay_capacity: int = 10_000,
) -> FastAPI:
    if not root.is_absolute():
        raise ValueError("Polymarket live read root must be absolute")
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

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        nonlocal stream_task
        if stream_client is not None:
            stream_task = asyncio.create_task(
                stream_client.run(stream_stop),
                name="polymarket-live-stream-client",
            )
        try:
            yield
        finally:
            stream_stop.set()
            if stream_task is not None:
                stream_task.cancel()
                await asyncio.gather(stream_task, return_exceptions=True)
            executor.shutdown(wait=True, cancel_futures=True)

    app = FastAPI(
        title="MarketCow Polymarket Live Read API",
        lifespan=lifespan,
    )
    app.state.polymarket_live_read = reader
    app.state.polymarket_live_read_executor = executor
    app.state.polymarket_live_projection = projection

    async def run_read(
        action: Callable[..., T],
        *args: object,
        **kwargs: object,
    ) -> T:
        return await asyncio.get_running_loop().run_in_executor(
            executor,
            partial(action, *args, **kwargs),
        )

    @app.get(
        "/v1/prediction-markets/polymarket/live/bootstrap",
        response_model=LiveBootstrapResponse,
    )
    async def bootstrap(market_id: list[str] | None = Query(default=None)):
        try:
            scope = _require_scope(market_id)
            if stream_client is not None:
                body = await run_read(projection.bootstrap_json, reader, scope)
            else:
                body = await run_read(reader.bootstrap_json, scope)
            return Response(content=body, media_type="application/json")
        except PolymarketLiveReadError as exc:
            _raise_read_error(exc)

    @app.get(
        "/v1/prediction-markets/polymarket/live/snapshot",
        response_model=LiveSnapshotPage,
    )
    async def snapshot(market_id: list[str] | None = Query(default=None)):
        try:
            scope = _require_scope(market_id)
            if stream_client is not None:
                body = await run_read(projection.snapshot_json, reader, scope)
            else:
                body = await run_read(reader.snapshot_json, scope)
            return Response(content=body, media_type="application/json")
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
        "/v1/prediction-markets/polymarket/live/events",
        response_model=LiveEventPage,
    )
    async def events(
        after_cursor: int = Query(0, ge=0),
        limit: int = Query(1000, ge=1, le=10000),
        market_id: list[str] | None = Query(default=None),
    ):
        try:
            payload, _, _ = await run_read(
                projection.events_json if stream_client is not None else reader.events_json,
                _require_scope(market_id),
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
    async def health():
        try:
            if stream_client is not None:
                return projection.health(reader)
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
        queue = projection.subscribe(capacity=2048)
        cursor = after_cursor
        try:
            scope = _require_scope(market_id)
            while True:
                page = projection.events_after(scope, cursor, 1000)
                for event in page.items:
                    await websocket.send_json({
                        "type": "event", "cursor": event.cursor,
                        "event": event.model_dump(mode="json"),
                    })
                    cursor = event.cursor
                if not page.has_more:
                    break
            await websocket.send_json({
                "type": "ready", "cursor": projection.latest_cursor,
            })
            while True:
                event = await queue.get()
                if event.cursor <= cursor:
                    continue
                if event.market_id is not None and event.market_id not in scope:
                    continue
                await websocket.send_json({
                    "type": "event", "cursor": event.cursor,
                    "event": event.model_dump(mode="json"),
                })
                cursor = event.cursor
        except PolymarketLiveReadError as exc:
            await websocket.send_json({
                "type": "error", "code": exc.code, "message": str(exc),
                "retryable": exc.status_code == 503,
            })
            await websocket.close(code=1013)
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        finally:
            projection.unsubscribe(queue)

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
        if exc.code == "polymarket_state_index_lagging"
        else None
    )
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": str(exc)},
        headers=headers,
    ) from exc


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
