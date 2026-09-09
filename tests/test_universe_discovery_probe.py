"""Real local transport with synthetic Discovery frames; no U1 success claim."""
import asyncio
import socket
import threading
import time

import pytest
import uvicorn
from fastapi import FastAPI, WebSocket

from marketcow.universe_discovery_probe import probe_discovery


@pytest.mark.parametrize("mode", ["partial", "new_projection", "wrong_projection", "gap", "resync"])
def test_discovery_protocol(mode):
    app = FastAPI()
    prefix = "/v1/prediction-markets/polymarket/live/discovery"
    binding = {"projection_id": "a"*64, "universe_revision": "u", "catalog_revision": "c"}

    @app.get(prefix+"/full-sync")
    def fullsync():
        return {**binding, "schema_version": "marketcow.polymarket.discovery.v3", "ready": False,
                "unresolved_gap_count": 1, "boundary_cursor": 10, "markets": [{"market_id": "1"}]}

    @app.websocket(prefix+"/stream")
    async def stream(ws: WebSocket):
        await ws.accept()
        await ws.send_json({**binding, "projection_id": "x" if mode == "wrong_projection" else binding["projection_id"],
                            "schema_version": "marketcow.polymarket.discovery-events.v3",
                            "after_cursor": 9 if mode == "gap" else 10, "next_cursor": 12,
                            "boundary_cursor": 12, "resync_required": mode == "resync", "items": []})
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
        async def run():
            expected = dict(binding, projection_id=None) if mode == "new_projection" else binding
            return await probe_discovery(endpoint, **expected, market_ids=("1",), timeout_seconds=2,
                                         full_sync_bytes=10000, frame_bytes=10000, maximum_frames=3,
                                         maximum_stream_bytes=30000)
        if mode in ("partial", "new_projection"):
            result = asyncio.run(run())
            assert result.identity_kind == "discovery_projection"
            assert result.ready_cursor == 12
            assert result.stream_instance_id == binding["projection_id"]
        else:
            with pytest.raises(ValueError):
                asyncio.run(run())
    finally:
        server.should_exit = True
        thread.join(5)
        listener.close()
        assert not thread.is_alive()
