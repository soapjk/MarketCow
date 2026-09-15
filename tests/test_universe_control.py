"""Synthetic metadata application tests; network arm uses real loopback HTTP."""
import copy
import hashlib
import json
import secrets
import socket
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn

from marketcow.universe_control import ControlError, UniverseControl
from marketcow.universe_control_http import Caller, PREFIX, create_control_app
from marketcow.universe_phase1 import DiscoverySelectionRequest, selection_sha256


FIXTURES = Path(__file__).parent / "contracts/phase1"
NOW = 1788769800000


class SyntheticSource:
    def __init__(self):
        self.rows = [json.loads((FIXTURES / f"catalog-page-{i}.json").read_bytes())["records"][0]
                     for i in (1, 2)]
        self.revision = "a" * 64
        self.count = len(self.rows)
        self.source = {"name": "synthetic", "observed_at": "2026-09-07T08:29:59.000Z"}
        self.coverage = {"predicate": "synthetic-only", "complete": True, "incomplete_reasons": []}

    def after(self, mid):
        yield from (copy.deepcopy(row) for row in self.rows if mid is None or row["market_id"] > mid)

    def get(self, mid):
        return next((copy.deepcopy(row) for row in self.rows if row["market_id"] == mid), None)


@pytest.fixture
def control(tmp_path):
    profile = json.loads((FIXTURES / "profile.json").read_bytes())
    clock = [NOW, 100.0]
    c = UniverseControl(SyntheticSource(), profile, tmp_path / "admit.sqlite",
                        expected_active_selection_id=None, wall_ms=lambda: clock[0], monotonic=lambda: clock[1])
    return c, clock


def request(control, ids=None):
    raw = json.loads((FIXTURES / "selection-request.json").read_bytes())
    raw["selection"]["resource_profile_sha256"] = control.profile_hash
    if ids is not None:
        raw["selection"]["market_ids"] = ids
    raw["selection_sha256"] = selection_sha256(raw["selection"])
    return DiscoverySelectionRequest.model_validate(raw)


def test_snapshot_traversal_binding_and_incumbent_preserved(control):
    c, _ = control
    snap = json.loads(c.snapshot(1))
    first = c.page(snap["snapshot_id"], snap["first_page_token"], 1)
    assert c.page(snap["snapshot_id"], snap["first_page_token"], 1) == first
    p1 = json.loads(first)
    p2 = json.loads(c.page(snap["snapshot_id"], p1["next_page_token"], 1))
    assert [p1["records"][0]["market_id"], p2["records"][0]["market_id"]] == ["1", "2"]
    assert p2["end_of_snapshot"] and p2["next_page_token"] is None
    with pytest.raises(ControlError, match="invalid_page_token"):
        c.page(snap["snapshot_id"], snap["first_page_token"], 2)
    old = c.source
    c.source = SyntheticSource()
    c.source.revision = "d"*64
    assert json.loads(c.page(snap["snapshot_id"], snap["first_page_token"], 1))["catalog_revision"] == old.revision
    assert c.incumbent is None


def test_snapshot_capacity_and_expiry_survive_wall_regression(control):
    c, clock = control
    snap = json.loads(c.snapshot(1))
    c.snapshot(2)
    with pytest.raises(ControlError, match="resource_unavailable"):
        c.snapshot(2)
    clock[0] -= 1000
    clock[1] += 901
    with pytest.raises(ControlError, match="catalog_snapshot_expired"):
        c.page(snap["snapshot_id"], snap["first_page_token"], 1)


def test_admission_dependencies_retry_and_no_activation(control):
    c, _ = control
    req = request(c, ["1"])
    body = c.admit("caller", req)
    response = json.loads(body)
    assert response["requested_market_ids"] == ["1"]
    assert response["dependency_market_ids"] == ["2"]
    assert response["dependency_token_ids"] == ["t2"]
    assert response["capacity_estimate"]["dependency_markets"] == 1
    assert c.incumbent is None
    # Retrying must not reinterpret admission against a changed source.
    c.source.revision = "d"*64
    assert c.admit("caller", req) == body
    with pytest.raises(ControlError, match="catalog_revision_mismatch"):
        c.admit("other", req)


def test_admission_capacity_protection_unknown(control):
    c, _ = control
    c.profile["admission"]["max_total_tokens"] = 1
    with pytest.raises(ControlError, match="capacity_exceeded"):
        c.admit("caller", request(c))
    with pytest.raises(ControlError, match="unknown_market_id"):
        c.admit("other", request(c, ["unknown"]))


def test_permanent_rejection_replay_does_not_reinterpret(control):
    c, _ = control
    req = request(c)
    c.source.revision = "d"*64
    with pytest.raises(ControlError, match="catalog_revision_mismatch"):
        c.admit("caller", req)
    c.source.revision = "a"*64
    with pytest.raises(ControlError, match="catalog_revision_mismatch"):
        c.admit("caller", req)
    assert json.loads(c.admit("other", req))["status"] == "admitted"


def test_byte_bound_tampered_token_and_source_revision(control):
    c, _ = control
    snap = json.loads(c.snapshot(1))
    with pytest.raises(ControlError, match="invalid_page_token"):
        c.page(snap["snapshot_id"], snap["first_page_token"] + "x", 1)
    c.profile["catalog"]["max_response_bytes"] = 400
    with pytest.raises(ControlError, match="response_size_exceeded"):
        c.page(snap["snapshot_id"], snap["first_page_token"], 1)


def test_admission_uses_actual_runtime_incumbent_and_preserves_retry(control):
    c, _ = control
    active = ["legacy"]
    c.incumbent_provider = lambda: active[0]
    req = request(c)
    with pytest.raises(ControlError, match="incumbent_conflict"):
        c.admit("old", req)
    raw = req.model_dump()
    raw["selection"]["expected_active_selection_id"] = "legacy"
    raw["selection_sha256"] = selection_sha256(raw["selection"])
    req = DiscoverySelectionRequest.model_validate(raw)
    original = c.admit("first", req)
    active[0] = "actual-next"
    assert c.admit("first", req) == original
    with pytest.raises(ControlError, match="incumbent_conflict"):
        c.admit("second", req)
    def unavailable():
        raise OSError("synthetic runtime unavailable")
    c.incumbent_provider = unavailable
    with pytest.raises(ControlError, match="runtime_state_requires_reconciliation"):
        c.admit("third", req)
    active[0] = "legacy"
    c.incumbent_provider = lambda: active[0]
    assert json.loads(c.admit("third", req))["status"] == "admitted"


def test_hot_routes_auth_strict_json_and_not_cold_activation(control):
    from fastapi.testclient import TestClient
    c, _ = control
    calls = []
    class Hot:
        def status(self, pool):
            calls.append(("status", pool)); return {"pool": pool, "actual": {"revision": 7},
                "bounded_inventory": "x"*70000}
        def prepare_live(self, caller, payload):
            calls.append((caller, payload)); return {"publication_applied": False}
        def activate(self, caller, payload):
            raise RuntimeError("synthetic uncertain receipt")
        def retire(self, caller, payload):
            return {"resources_released": False}
        def acquisition_lease_status(self, pool):
            calls.append(("lease_status", pool)); return {"source_ready": False}
        def acquisition_lease(self, caller, payload, action):
            calls.append((action, caller, payload)); return {"source_ready": action != "release"}
    secret = "synthetic-hot-route-token"
    app = create_control_app(c, callers=(Caller("test", hashlib.sha256(secret.encode()).hexdigest(),
        frozenset({"hot.read", "hot.prepare", "hot.activate", "hot.retire", "acquisition.lease"})),), body_timeout_seconds=2, hot_operations=Hot())
    with TestClient(app, client=("127.0.0.1", 45000)) as client:
        root = PREFIX+"/hot-scopes"
        assert client.get(root+"/status?pool=live").status_code == 401
        client.headers["Authorization"] = "Bearer "+secret
        assert client.get(root+"/status?pool=live").json()["actual"]["revision"] == 7
        assert client.post(root+"/live/prepare", content='{"x":1,"x":2}').status_code == 400
        assert client.post(root+"/live/prepare", content='{"x":NaN}').status_code == 400
        assert client.post(root+"/live/prepare", json={"synthetic": True}).json()["publication_applied"] is False
        failed = client.post(root+"/activate", json={"synthetic": True})
        assert failed.status_code == 503 and failed.json()["reconcile_required"] is True
        assert failed.json()["retryable"] is False
        assert client.post(PREFIX+"/generations/apply", json={}).status_code == 404
        assert client.post(PREFIX+"/discovery-selections/admit", json={}).status_code == 403
        lease=PREFIX+"/acquisition-leases"
        assert client.get(lease+"/status?pool=live").json()["source_ready"] is False
        body={"schema_version":"marketcow.acquisition-lease.v1","pool":"live",
            "expected_scope_id":"scope","expected_revision":7,"lease_id":"paper","ttl_seconds":30,"market_ids":["1"]}
        assert client.post(lease+"/acquire",json=body).json()["source_ready"] is True
        assert client.post(lease+"/renew",json={k:v for k,v in body.items() if k!="market_ids"}).status_code==200
        released={k:v for k,v in body.items() if k not in {"market_ids","ttl_seconds"}}
        assert client.post(lease+"/release",json=released).json()["source_ready"] is False
    assert len(calls) == 6
    with pytest.raises(ValueError, match="mutually exclusive"):
        create_control_app(c, callers=(), body_timeout_seconds=2, hot_operations=Hot(), runtime_operations=object())


def test_preparation_requires_existing_same_caller_admission(control):
    c, clock = control
    req = request(c)
    with pytest.raises(ControlError, match="admission_not_found"):
        c.admitted_for_preparation("test", req, "0"*64)
    body = c.admit("test", req)
    digest = hashlib.sha256(body).hexdigest()
    assert c.admitted_for_preparation("test", req, digest) == json.loads(body)
    with pytest.raises(ControlError, match="admission_not_found"):
        c.admitted_for_preparation("other", req, digest)
    with pytest.raises(ControlError, match="hash_mismatch"):
        c.admitted_for_preparation("test", req, "0"*64)
    assert c.incumbent is None
    clock[0] += 900000
    with pytest.raises(ControlError, match="request_expired"):
        c.admitted_for_preparation("test", req, digest)


def test_real_loopback_http_auth_snapshot_admit_cleanup(control):
    c, _ = control
    # Ephemeral synthetic secret: never a real operator credential.
    secret = secrets.token_hex(32)
    app = create_control_app(c, callers=(Caller("test", hashlib.sha256(secret.encode()).hexdigest(),
                                               frozenset({"catalog.read", "discovery.admit"})),),
                             body_timeout_seconds=2)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, access_log=False, log_level="error", lifespan="off"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]})
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while not server.started and thread.is_alive() and time.monotonic() < deadline:
            time.sleep(.01)
        assert server.started
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=3, trust_env=False) as client:
            path = PREFIX + "/catalog/snapshot"
            assert client.get(path, params={"page_size": 1}).status_code == 401
            client.headers["Authorization"] = f"Bearer {secret}"
            response = client.get(path, params={"page_size": 1})
            assert response.status_code == 200
            snap = response.json()
            page = client.get(PREFIX + "/catalog/page", params={"snapshot_id": snap["snapshot_id"],
                "page_token": snap["first_page_token"], "limit": 1})
            assert page.status_code == 200 and page.json()["records"][0]["market_id"] == "1"
            payload = request(c).model_dump()
            first = client.post(PREFIX + "/discovery-selections/admit", json=payload)
            assert first.status_code == 200
            retry = client.post(PREFIX + "/discovery-selections/admit", json=payload)
            assert retry.content == first.content
            assert client.get(path+"?page_size=1&page_size=2").status_code == 400
            assert c.incumbent is None
    finally:
        server.should_exit = True
        thread.join(5)
        listener.close()
    assert not thread.is_alive()
