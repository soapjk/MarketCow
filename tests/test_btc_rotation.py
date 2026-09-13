import asyncio
import hashlib
import json

import pytest

from marketcow.btc_rotation import (
    HotHttpOperations,
    discovery_baseline,
    rotate_and_capture,
    rotate_discovery_and_live,
)


BUDGETS = dict(seconds=10, maximum_frames=10, maximum_total_bytes=65536,
               maximum_frame_bytes=8192, maximum_fullsync_bytes=32768)
REQUEST = dict(schema_version="marketcow.hot-live-prepare.v1", market_ids=["1"],
               expected_scope_id="old", expected_revision=1, protected_market_ids=[])


class Operations:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def prepare_live(self, caller, request):
        self.calls.append("prepare")
        return dict(schema_version="marketcow.hot-scope-prepared.v1", pool="live",
                    publication_applied=False, requested_market_ids=["1"],
                    expected=dict(expected_scope_id="old", expected_revision=1),
                    acquisition=dict(all_installed=True), candidate_id="candidate", selection_id="selection")

    def activate(self, caller, request):
        self.calls.append("activate")
        if self.fail:
            raise TimeoutError("lost receipt")
        assert request["expected_revision"] == 1
        return dict(schema_version="marketcow.hot-scope-activated.v1",
                    candidate_id="candidate", selection_id="selection", new_full_sync_required=True,
                    actual=dict(scope_id="new", revision=2, stream_instance_id="instance"))


async def captured(endpoint, scope, bindings, publish, **kwargs):
    assert scope == "new"
    return dict(scope_id=scope, ready_observed=True, stream_instance_id="instance", last_cursor=3)


def run(root, operations, capture=captured, budgets=None):
    return asyncio.run(rotate_and_capture(operations, "caller", REQUEST, [{"market_id": "1"}],
                                         "http://127.0.0.1:8793", lambda _: None, root,
                                         capture_budgets=BUDGETS if budgets is None else budgets,
                                         capture_function=capture))


def test_prepare_activate_new_baseline_and_receipt(tmp_path):
    operations = Operations()
    report = run(tmp_path / "attempt", operations)
    assert operations.calls == ["prepare", "activate"]
    assert report["stage"] == "capture_complete"
    assert report["execution_ready"] is False
    assert json.loads((tmp_path / "attempt/receipt.json").read_bytes()) == report


def test_uncertain_activation_is_not_repeated_on_reopen(tmp_path):
    operations = Operations(fail=True)
    root = tmp_path / "attempt"
    with pytest.raises(TimeoutError):
        run(root, operations)
    report = json.loads((root / "receipt.json").read_bytes())
    assert report["stage"] == "requires_reconciliation" and report["failed_stage"] == "activating"
    with pytest.raises(FileExistsError):
        run(root, operations)
    assert operations.calls == ["prepare", "activate"]


def test_old_baseline_after_publication_is_rejected_without_rollback(tmp_path):
    async def wrong(*args, **kwargs):
        return dict(scope_id="old", ready_observed=True)
    operations = Operations()
    with pytest.raises(ValueError, match="new_baseline"):
        run(tmp_path / "attempt", operations, capture=wrong)
    report = json.loads((tmp_path / "attempt/receipt.json").read_bytes())
    assert report["stage"] == "capture_failed"
    assert report["activated"]["actual"]["scope_id"] == "new"
    assert operations.calls == ["prepare", "activate"]


def test_invalid_budget_never_prepares(tmp_path):
    operations = Operations()
    with pytest.raises(ValueError, match="budget"):
        run(tmp_path / "attempt", operations, budgets={})
    assert not operations.calls and not (tmp_path / "attempt").exists()


def test_existing_http_routes_and_no_redirect_retry():
    import httpx
    calls = []
    def handler(request):
        calls.append(request.url.path)
        assert request.headers["authorization"] == "Bearer synthetic"
        return httpx.Response(302, headers={"location": "http://elsewhere.invalid"})
    operations = HotHttpOperations("http://127.0.0.1:18898", "synthetic", timeout=1,
                                   maximum_bytes=1024, transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(RuntimeError, match="reconciliation"):
            operations.prepare_live("caller", REQUEST)
        assert calls == ["/v1/prediction-markets/polymarket/hot-scopes/live/prepare"]
    finally:
        operations.close()


def test_acquisition_lease_http_contract_uses_dedicated_routes():
    import httpx
    seen=[]
    def handler(request):
        seen.append((request.method,request.url.path,request.content))
        return httpx.Response(200,json={"schema_version":"marketcow.acquisition-lease-status.v1","source_ready":False})
    operations=HotHttpOperations("http://127.0.0.1:18898","synthetic",timeout=1,maximum_bytes=65536,
                                 transport=httpx.MockTransport(handler))
    try:
        status=operations.acquisition_lease_status("live")
        common=dict(pool="live",lease_id="paper",expected_scope_id="scope",expected_revision=2)
        operations.acquire_acquisition(**common,ttl_seconds=30,market_ids=["1","2"])
        operations.renew_acquisition(**common,ttl_seconds=30)
        operations.release_acquisition(**common)
        assert status["source_ready"] is False
        assert [path for _,path,_ in seen]==[
            "/v1/prediction-markets/polymarket/acquisition-leases/status",
            "/v1/prediction-markets/polymarket/acquisition-leases/acquire",
            "/v1/prediction-markets/polymarket/acquisition-leases/renew",
            "/v1/prediction-markets/polymarket/acquisition-leases/release"]
    finally: operations.close()


DISCOVERY = {
    "schema_version": "tradude.marketcow.discovery-selection-request.v1",
    "request_id": "1789038000000:" + "a" * 32,
    "created_at": "2026-09-10T11:00:00.000Z", "expires_at": "2026-09-10T11:15:00.000Z",
    "selection": {"catalog_revision": "c" * 64, "market_ids": ["1", "2"],
                  "tradude_policy_version": "btc-hour-v1", "expected_active_selection_id": "old-selection",
                  "protected_markets": [], "resource_profile_id": "profile", "resource_profile_sha256": "p" * 64},
    "selection_sha256": "s" * 64,
}
LIVE = {"schema_version": "marketcow.hot-live-prepare.v1", "catalog_revision": "c" * 64,
        "parent_selection_id": "s" * 64, "market_ids": ["2"], "protected_market_ids": [],
        "protected_exceptions": [], "policy_version": "btc-hour-v1", "expires_ms": 1789038900000,
        "expected_scope_id": "live-old", "expected_revision": 4}


class ParentOperations:
    def __init__(self, fail_at=None):
        self.calls, self.fail_at = [], fail_at

    def admit_discovery(self, request):
        self.calls.append("admit")
        return ({"schema_version": "marketcow.polymarket.discovery-admission.v1", "status": "admitted",
                 "selection_sha256": "s" * 64, "requested_market_ids": ["1", "2"]}, "a" * 64)

    def prepare_discovery(self, caller, request, response_sha):
        self.calls.append("prepare_discovery")
        return prepared("discovery", ["1", "2"], "discovery-candidate", "discovery-old", 3, "s" * 64)

    def prepare_live(self, caller, request):
        self.calls.append("prepare_live")
        return prepared("live", ["2"], "live-candidate", "live-old", 4, "l" * 64)

    def activate(self, caller, request):
        pool = request["pool"]
        self.calls.append("activate_" + pool)
        if self.fail_at == pool:
            raise TimeoutError("uncertain")
        actual = ({"projection_id": "discovery-new", "revision": 4, "universe_revision": "universe-new"}
                  if pool == "discovery" else
                  {"scope_id": "live-new", "revision": 5, "stream_instance_id": "live-instance"})
        return {"schema_version": "marketcow.hot-scope-activated.v1", "candidate_id": request["candidate_id"],
                "selection_id": "s" * 64 if pool == "discovery" else "l" * 64,
                "actual": actual, "new_full_sync_required": True}


def prepared(pool, ids, candidate, incumbent, revision, selection):
    return {"schema_version": "marketcow.hot-scope-prepared.v1", "pool": pool,
            "candidate_id": candidate, "selection_id": selection, "requested_market_ids": ids,
            "expected": {"expected_scope_id": incumbent, "expected_revision": revision},
            "acquisition": {"all_installed": True}, "publication_applied": False}


async def discovery_captured(endpoint, activated, required, **budget):
    assert required == ["2"] and activated["actual"]["projection_id"] == "discovery-new"
    return {"projection_id": "discovery-new", "universe_revision": "universe-new", "market_count": 2}


async def live_captured(endpoint, scope, bindings, publish, **budget):
    assert scope == "live-new" and bindings == [{"market_id": "2"}]
    return {"scope_id": scope, "stream_instance_id": "live-instance", "ready_observed": True}


def test_parent_discovery_then_live_complete_chain(tmp_path):
    operations = ParentOperations()
    report = asyncio.run(rotate_discovery_and_live(operations, "caller", DISCOVERY, LIVE,
        [{"market_id": "2"}], "http://127.0.0.1:8795", "http://127.0.0.1:8793", lambda _: None,
        tmp_path / "attempt", discovery_budget={"maximum_bytes": 8_000_000, "timeout": 10},
        capture_budgets=BUDGETS, discovery_capture=discovery_captured, live_capture=live_captured))
    assert report["stage"] == "capture_complete" and report["execution_ready"] is False
    assert operations.calls == ["admit", "prepare_discovery", "activate_discovery", "prepare_live", "activate_live"]
    assert json.loads((tmp_path / "attempt/receipt.json").read_bytes()) == report


def test_parent_activation_uncertain_stops_before_live(tmp_path):
    operations = ParentOperations(fail_at="discovery")
    with pytest.raises(TimeoutError):
        asyncio.run(rotate_discovery_and_live(operations, "caller", DISCOVERY, LIVE, [{"market_id": "2"}],
            "http://127.0.0.1:8795", "http://127.0.0.1:8793", lambda _: None, tmp_path / "attempt",
            discovery_budget={"maximum_bytes": 8_000_000, "timeout": 10}, capture_budgets=BUDGETS,
            discovery_capture=discovery_captured, live_capture=live_captured))
    stored = json.loads((tmp_path / "attempt/receipt.json").read_bytes())
    assert stored["stage"] == "requires_reconciliation"
    assert stored["failed_stage"] == "activating_discovery"
    assert operations.calls == ["admit", "prepare_discovery", "activate_discovery"]


def test_discovery_baseline_binds_generation_and_membership():
    import httpx
    body = json.dumps({"projection_id": "projection", "universe_revision": "universe",
                       "boundary_cursor": 9, "markets": [{"market_id": "2"}]}).encode()
    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    value = asyncio.run(discovery_baseline("http://127.0.0.1:8795",
        {"actual": {"projection_id": "projection", "universe_revision": "universe"}}, ["2"],
        maximum_bytes=10000, timeout=1, transport=transport))
    assert value["market_count"] == 1 and value["raw_sha256"] == hashlib.sha256(body).hexdigest()
