import asyncio

import httpx
import pytest

from marketcow import btc_polymarket_capture as module


def execute(monkeypatch, *, ready=True, source_timeout=False):
    client = httpx.AsyncClient
    monkeypatch.setattr(module.httpx, "AsyncClient", lambda **kwargs: client(
        **kwargs, transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"scope_id": "scope"}))))

    class Boundary:
        def __init__(self, *args):
            self.instance, self.cursor, self.ready = "instance", 1, False

        def consume(self, raw):
            self.ready = True
            return "ready"

    class Socket:
        def __init__(self):
            self.first = True
            self.closed = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            self.closed = True

        async def recv(self):
            if self.first and ready:
                self.first = False
                return '{"type":"ready"}'
            if source_timeout:
                raise TimeoutError("independent source timeout")
            await asyncio.sleep(10)

    socket = Socket()
    monkeypatch.setattr(module, "CaptureBoundary", Boundary)
    monkeypatch.setattr(module, "connect", lambda *args, **kwargs: socket)
    facts = []
    async def run():
        return await module.capture("http://127.0.0.1", "scope", [{}], facts.append,
            seconds=.05, maximum_frames=10, maximum_total_bytes=8192,
            maximum_frame_bytes=4096, maximum_fullsync_bytes=4096)
    return run, facts, socket


def test_ready_then_normal_duration_is_success(monkeypatch):
    run, facts, socket = execute(monkeypatch)
    report = asyncio.run(run())
    assert report["ready_observed"] and report["stop_reason"] == "observation_window_complete"
    assert facts[-1]["stop_reason"] == report["stop_reason"] and socket.closed


def test_no_ready_at_deadline_is_failure(monkeypatch):
    run, facts, socket = execute(monkeypatch, ready=False)
    with pytest.raises(TimeoutError):
        asyncio.run(run())
    assert facts[-1]["stop_reason"] == "ready_deadline" and socket.closed


def test_independent_source_timeout_after_ready_still_fails(monkeypatch):
    run, facts, socket = execute(monkeypatch, source_timeout=True)
    with pytest.raises(TimeoutError):
        asyncio.run(run())
    assert facts[-1]["stop_reason"] == "source_timeout" and socket.closed
