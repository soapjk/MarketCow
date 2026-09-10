"""Read-only bounded facade for fixed durable BTC fact-log watermarks."""
import asyncio
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect

from .btc_fact_log import FactLog, read_page
from .btc_hourly_dataset import canonical, timestamp


def create_read_app(root: Path, *, maximum_records: int, maximum_bytes: int,
                    live_log: FactLog | None = None, subscriber_count: int = 256,
                    subscriber_bytes: int = 1048576, send_timeout: float = 5.0) -> FastAPI:
    if maximum_records <= 0 or maximum_bytes <= 0:
        raise ValueError("invalid_read_budget")
    app = FastAPI(title="MarketCow BTC research read-only")
    if subscriber_count <= 0 or subscriber_bytes <= 0 or send_timeout <= 0:
        raise ValueError("invalid_stream_budget")

    @app.get("/v1/btc-research/status")
    def status():
        if live_log is None:
            raise HTTPException(503, "live_capture_not_attached")
        return live_log.status()

    @app.websocket("/v1/btc-research/stream")
    async def stream(socket: WebSocket):
        if live_log is None:
            await socket.close(code=1013)
            return
        subscriber = None
        receive = None
        try:
            subscriber = live_log.subscribe(count=subscriber_count, size=subscriber_bytes)
            await socket.accept()
            # This is a future-only raw feed, never a book-ready or replay baseline.
            await asyncio.wait_for(socket.send_json({
                "schema_version": "marketcow.btc-research.stream-start.v1",
                "epoch": live_log.epoch, "replay": False,
                "continuity": "unverified", "durability": "asynchronous",
            }), send_timeout)
            receive = asyncio.create_task(socket.receive())
            while True:
                if receive.done():
                    # Read-only stream: close on disconnect or any application input.
                    await receive
                    break
                raw = subscriber.receive()
                if raw is None:
                    await asyncio.sleep(.005)
                    continue
                await asyncio.wait_for(socket.send_text(raw.decode("utf-8")), send_timeout)
        except (WebSocketDisconnect, RuntimeError, TimeoutError):
            pass
        finally:
            if receive is not None:
                receive.cancel()
                await asyncio.gather(receive, return_exceptions=True)
            if subscriber is not None:
                live_log.unsubscribe(subscriber)
            try:
                await asyncio.wait_for(socket.close(code=1013), send_timeout)
            except (RuntimeError, WebSocketDisconnect, TimeoutError):
                pass

    @app.get("/v1/btc-research/facts")
    def facts(after: int = Query(ge=0), through: int = Query(ge=0),
              limit: int = Query(gt=0), asof: str | None = None,
              mode: str = "observed"):
        if limit > maximum_records or mode not in {"observed", "event_time_research"}:
            raise HTTPException(400, "invalid_budget_or_mode")
        try:
            cutoff: datetime | None = timestamp(asof) if asof else None
            page = read_page(root, after=after, through=through, limit=limit, maximum_bytes=maximum_bytes)
            selected = []
            for row in page:
                fact = row["fact"]
                if mode == "observed":
                    observed = fact.get("first_received_at")
                else:
                    observed = fact.get("event_at")
                    if observed is None and type(fact.get("exchange_at_ms")) is int:
                        from datetime import timezone
                        observed = datetime.fromtimestamp(fact["exchange_at_ms"] / 1000, timezone.utc).isoformat()
                if observed is not None and (cutoff is None or timestamp(observed) <= cutoff):
                    selected.append(row)
            # Advance over inspected rows, including those excluded by as-of.
            next_after = page[-1]["sequence"] if page else after
            response = {"schema_version": "marketcow.btc-research.page.v1", "through": through,
                    "after": after, "next_after": next_after, "scanned": len(page),
                    "records": selected, "asof_mode": mode,
                    "limitation": "event_time_is_not_historical_availability" if mode != "observed" else None}
            if len(canonical(response)) > maximum_bytes:
                raise HTTPException(413, "response_size_exceeded")
            return response
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    return app
