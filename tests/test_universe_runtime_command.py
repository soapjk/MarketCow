"""Real owner/store orchestration with explicitly synthetic runtime probes."""
import asyncio
import json

import pytest

from marketcow import universe_runtime_command as command
from marketcow.universe_generation import Generation, GenerationStore
from marketcow.universe_owner import SupervisorOwner
from marketcow.universe_phase1 import selection_sha256
from marketcow.universe_runtime_supervisor import RuntimeBoundary


def test_command_holds_owner_until_durable_receipt(tmp_path, monkeypatch):
    db = tmp_path / "generations.sqlite"
    generation = Generation(pool="discovery", selection_id="test", catalog_revision="a"*64,
        artifact_sha256="b"*64, market_ids=("1",), dependency_market_ids=(), protected_market_ids=(),
        parent_selection_id=None, stream_instance_id="projection", baseline_cursor=1)
    store = GenerationStore(db, maximum_generations=3, maximum_record_bytes=10000)
    gid = store.register(generation, expires_ms=10**15, now_ms=1)
    store.select("discovery", gid, expected=None, protected_market_ids=(), now_ms=1)
    store.close()
    config = dict(pool="discovery", store_path=str(db), owner_lock=str(tmp_path/"owner.lock"),
        maximum_generations=3, maximum_record_bytes=10000, release_root=str(tmp_path),
        user_unit_root=str(tmp_path), command_timeout_seconds=2, operation_timeout_seconds=3,
        binding=dict(generation_id=gid, scope_id="unused", projection_id="projection", universe_revision="c"*64,
            preheat_endpoint="http://127.0.0.1:18899", public_endpoint="http://127.0.0.1:8795",
            full_sync_bytes=10000, frame_bytes=10000, maximum_frames=5, maximum_stream_bytes=50000,
            **{k:dict(name="marketcow-test.service", path=str(tmp_path/k), sha256="b"*64)
               for k in ("preheat", "candidate", "incumbent")}))
    path = tmp_path/"config.json"
    path.write_text(json.dumps(config))
    calls = []
    class Runtime:
        def __init__(self, *_):
            pass
        async def prepare(self, *_):
            calls.append("prepare")
        async def publish(self, *_):
            calls.append("publish")
        async def probe(self, *_):
            with pytest.raises(RuntimeError, match="already running"):
                with SupervisorOwner(tmp_path/"owner.lock"):
                    pass
            return RuntimeBoundary("projection", 1, 2, "http://127.0.0.1:8795", "discovery_projection")
    monkeypatch.setattr(command, "SystemdDiscoveryRuntime", Runtime)
    receipt = asyncio.run(command.execute(path, selection_sha256(config), "apply"))
    assert receipt["status"] == "runtime_applied"
    assert calls == ["prepare", "publish"]
    assert asyncio.run(command.execute(path, selection_sha256(config), "reconcile")) == receipt
    assert calls == ["prepare", "publish"]  # reconcile never restarts anything
    with pytest.raises(ValueError, match="hash mismatch"):
        asyncio.run(command.execute(path, "0"*64, "apply"))
    with SupervisorOwner(tmp_path/"owner.lock"):
        pass
