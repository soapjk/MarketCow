"""Real durable CAS and owner lock with explicitly synthetic runtime effects."""
import asyncio
import hashlib
import json

import httpx
import pytest

from marketcow import universe_generation_operations as ops
from marketcow.universe_control_http import Caller, PREFIX, create_control_app
from marketcow.universe_generation import Generation, GenerationStore
from marketcow.universe_phase1 import selection_sha256
from marketcow.universe_runtime_supervisor import RuntimeBoundary


def test_registered_operation_returns_only_verified_receipt(tmp_path, monkeypatch):
    db = tmp_path/"generations.sqlite"
    owner = tmp_path/"owner.lock"
    generation = Generation(pool="discovery", selection_id="synthetic", catalog_revision="a"*64,
        artifact_sha256="b"*64, market_ids=("1",), dependency_market_ids=(), protected_market_ids=(),
        parent_selection_id=None, stream_instance_id="old", baseline_cursor=100)
    store = GenerationStore(db, maximum_generations=3, maximum_record_bytes=10000)
    gid = store.register(generation, expires_ms=10**15, now_ms=1); store.close()
    config = dict(pool="discovery", store_path=str(db), owner_lock=str(owner), maximum_generations=3,
        maximum_record_bytes=10000, release_root=str(tmp_path), user_unit_root=str(tmp_path),
        command_timeout_seconds=2, operation_timeout_seconds=3,
        binding=dict(generation_id=gid, scope_id="unused", projection_id="old", universe_revision="c"*64,
            preheat_endpoint="http://127.0.0.1:18900", public_endpoint="http://127.0.0.1:8795",
            full_sync_bytes=10000, frame_bytes=10000, maximum_frames=5, maximum_stream_bytes=10000,
            **{k: dict(name="marketcow-test.service", path=str(tmp_path/k), sha256="b"*64)
               for k in ("preheat", "candidate", "incumbent")}))
    path = tmp_path/"operation.json"; path.write_text(json.dumps(config))
    operations = ops.GenerationOperations(store_path=str(db), owner_lock=str(owner), maximum_generations=3,
        maximum_record_bytes=10000, registrations={gid: dict(path=str(path), sha256=selection_sha256(config))})
    calls = []
    class Runtime:
        def __init__(self, *_): pass
        def _check(self, value): assert value == generation
        async def prepare(self, *_): calls.append("prepare")
        async def publish(self, *_): calls.append("publish")
        async def probe(self, *_):
            return RuntimeBoundary("new", 1, 3, "http://127.0.0.1:8795", "discovery_projection")
    monkeypatch.setattr(ops, "SystemdDiscoveryRuntime", Runtime)
    request = ops.ApplyGeneration(schema_version="marketcow.polymarket.generation-apply.v1", operation="activate",
        pool="discovery", generation_id=gid, expected=None, protected_market_ids=[])
    receipt = operations.apply(request)
    assert receipt["status"] == "runtime_applied" and receipt["baseline_cursor"] == 1
    assert operations.status("discovery")["applied"] == receipt
    with pytest.raises(ValueError, match="incumbent_conflict"):
        operations.apply(request)  # Never silently repeat a deployment on retry.
    assert calls == ["prepare", "publish"]
    request.operation = "reconcile"
    request.expected = ops.ExpectedGeneration(generation_id=gid, epoch=1)
    assert operations.apply(request) == receipt
    assert calls == ["prepare", "publish"]


def test_runtime_routes_require_separate_scope_and_explicit_mount():
    class Control:
        profile = {"catalog": {"concurrent_readers": 2}}
    class Operations:
        calls = 0
        def status(self, pool): return {"pool": pool, "applied": None}
        def apply(self, request):
            self.calls += 1
            return {"status": "synthetic_runtime_receipt", "pool": request.pool}
    operations = Operations()
    secret = "synthetic-only"
    caller = Caller("test", hashlib.sha256(secret.encode()).hexdigest(), frozenset({"catalog.read", "runtime.read"}))
    with pytest.raises(ValueError, match="scoped caller"):
        create_control_app(Control(), callers=(caller,), body_timeout_seconds=1)
    app = create_control_app(Control(), callers=(caller,), body_timeout_seconds=1, runtime_operations=operations)
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 1234)), base_url="http://localhost") as client:
            assert (await client.get(PREFIX+"/generations/status?pool=live")).status_code == 401
            client.headers["authorization"] = "Bearer "+secret
            assert (await client.get(PREFIX+"/generations/status?pool=live")).status_code == 200
            assert (await client.post(PREFIX+"/generations/apply", json={})).status_code == 403
    asyncio.run(run())
    assert operations.calls == 0


def test_request_does_not_accept_client_paths_or_duplicate_keys():
    raw = dict(schema_version="marketcow.polymarket.generation-apply.v1", operation="activate",
        pool="discovery", generation_id="a"*64, expected=None, protected_market_ids=[])
    assert ops.parse_operation(json.dumps(raw)).generation_id == "a"*64
    with pytest.raises(ValueError):
        ops.parse_operation(json.dumps(dict(raw, executable="/tmp/client-command")))
    with pytest.raises(ValueError, match="duplicate"):
        ops.parse_operation('{"pool":"live","pool":"discovery"}')
