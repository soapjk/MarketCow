"""Actual local HTTP/WS transport with explicitly synthetic Rust-shaped frames."""
import asyncio
import socket
import threading
import time

import pytest
import uvicorn
from fastapi import FastAPI, WebSocket

from marketcow.universe_live_probe import probe_live, strict_json


@pytest.mark.parametrize("mode", ["ok", "wrong_instance", "bad_event", "error"])
def test_actual_http_ws_boundary(mode):
    app = FastAPI()
    prefix = "/v1/prediction-markets/polymarket/live"

    @app.get(prefix+"/full-sync")
    def full_sync():
        return {"schema_version": "marketcow.polymarket.live-full-sync.v1", "scope_id": "scope",
                "catalog_revision": "catalog", "cursor": 10, "stream_instance_id": "instance",
                "bootstrap": {"markets": [{"identity": {"market_id": "1"}}]}}

    @app.websocket(prefix+"/stream")
    async def stream(ws: WebSocket):
        await ws.accept()
        if mode == "error":
            await ws.send_json({"type": "error", "code": "source_disconnected"})
        else:
            await ws.send_json({"type": "event", "cursor": 12,
                                "event": {"cursor": 11 if mode == "bad_event" else 12}})
            await ws.send_json({"type": "ready", "cursor": 14, "confirmation_sequence": 0,
                                "confirmation_books": [],
                                "stream_instance_id": "wrong" if mode == "wrong_instance" else "instance"})
        await ws.close()

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    endpoint = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error", access_log=False, lifespan="off"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]})
    thread.start()
    try:
        deadline = time.monotonic()+3
        while not server.started and time.monotonic() < deadline:
            time.sleep(.01)
        assert server.started
        async def probe():
            return await probe_live(endpoint, scope_id="scope", catalog_revision="catalog", market_ids=("1",),
                                    timeout_seconds=2, full_sync_bytes=10000, frame_bytes=10000,
                                    maximum_frames=5, maximum_stream_bytes=50000)
        if mode == "ok":
            result = asyncio.run(probe())
            assert (result.baseline_cursor, result.ready_cursor, result.stream_instance_id) == (10, 14, "instance")
        else:
            with pytest.raises(ValueError):
                asyncio.run(probe())
    finally:
        server.should_exit = True
        thread.join(5)
        listener.close()
        assert not thread.is_alive()


def test_strict_wire_json():
    for raw in ('{"cursor":1,"cursor":2}', '{"value":NaN}'):
        with pytest.raises(ValueError):
            strict_json(raw)
