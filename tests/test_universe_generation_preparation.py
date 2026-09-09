"""Prepared state is gated by probe and stopped writer, never process-active."""
import asyncio
from pathlib import Path

import pytest

from marketcow import universe_generation_preparation as preparation
from marketcow.universe_generation import GenerationStore
from marketcow.universe_runtime_supervisor import RuntimeBoundary
from marketcow.universe_systemd import LiveUnitBinding, UnitArtifact


@pytest.mark.parametrize("mode", ["success", "probe_failure", "stop_failure"])
def test_preparation_registers_only_after_real_adapter_boundary(tmp_path, monkeypatch, mode):
    artifact = UnitArtifact("marketcow-test.service", Path("/synthetic/unit"), "a"*64)
    binding = LiveUnitBinding("unregistered", "scope", artifact, artifact, artifact,
        "http://127.0.0.1:18899", "http://127.0.0.1:8793", 1000, 1000, 10, 10000)
    calls = []
    class Units:
        async def stop(self, _):
            calls.append("stop")
            return {"Result": "exit-code" if mode == "stop_failure" else "success", "ExecMainStatus": "1" if mode == "stop_failure" else "0"}
    class Runtime:
        def __init__(self, *_): self.boundary = None
        async def prepare(self, _):
            calls.append("prepare")
            if mode == "probe_failure": raise ValueError("synthetic probe failed")
            self.boundary = RuntimeBoundary("observed-instance", 123, 125, "http://127.0.0.1:18899")
    monkeypatch.setattr(preparation, "SystemdLiveRuntime", Runtime)
    store = GenerationStore(tmp_path/"generations.sqlite", maximum_generations=3, maximum_record_bytes=10000)
    try:
        async def run():
            return await preparation.preheat_and_register(store, Units(), binding, pool="live", selection_id="test",
                catalog_revision="a"*64, market_ids=("1",), dependency_market_ids=(), protected_market_ids=(),
                parent_selection_id="parent", expires_ms=1000, now_ms=lambda: 10, operation_timeout_seconds=1)
        if mode == "success":
            generation, registered_binding, boundary = asyncio.run(run())
            assert generation.stream_instance_id == "observed-instance" and generation.baseline_cursor == 123
            assert registered_binding.generation_id == generation.generation_id
            assert boundary.ready_cursor == 125
            assert store.db.execute("SELECT COUNT(*) FROM generations").fetchone() == (1,)
        else:
            with pytest.raises(ValueError): asyncio.run(run())
            assert store.db.execute("SELECT COUNT(*) FROM generations").fetchone() == (0,)
        assert calls == ["prepare", "stop"]
        assert store.desired("live") is None and store.applied("live") is None
    finally:
        store.close()
