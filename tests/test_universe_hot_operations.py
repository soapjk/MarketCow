"""Synthetic Rust receipts with real local SQLite; not a live acquisition test."""
import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from marketcow.polymarket_contracts import content_sha256
from marketcow.universe_hot_operations import HotScopeOperations


class Runtime:
    def __init__(self, pool):
        self.pool = pool
        self.actual = dict(revision=1, catalog_revision="a"*64, retirement_submitted=0,
                           retirement_persisted=0, acquisition={"retiring_shards": 0})
        self.actual.update(scope_id="old-live") if pool == "live" else self.actual.update(
            projection_id="old-discovery", universe_revision="legacy")
        self.commands = []
        self.fail = None

    def status(self):
        return copy.deepcopy(self.actual)

    def request(self, command):
        self.commands.append(copy.deepcopy(command))
        if self.fail == command["operation"]:
            raise RuntimeError("synthetic lost receipt")
        key = "scope_id" if self.pool == "live" else "projection_id"
        if command["expected_scope_id"] != self.actual[key] or command["expected_revision"] != self.actual["revision"]:
            raise ValueError("scope revision conflict")
        if command["operation"] == "prepare_acquisition":
            assert content_sha256(command["records"]) == command["evidence_sha256"]
            return dict(acquisition_installed=True, publication_applied=False)
        if command["operation"] == "prepare_publication":
            return dict(publication_applied=False, actual=self.status())
        if command["operation"] == "retire_acquisition":
            self.actual["retirement_submitted"] += 1
            return dict(retirement_queued=True)
        assert command["operation"] == "publish_scope"
        config = command["config"]
        self.actual[key] = config["active_scope_id" if self.pool == "live" else "projection_id"]
        if self.pool == "discovery":
            self.actual["universe_revision"] = config["universe_revision"]
        self.actual["revision"] += 1
        return dict(publication_applied=True, actual=self.status())


def candidate(pool="discovery", name="new", ids=("1",)):
    result = dict(schema_version="marketcow.hot-scope-candidate.v1", pool=pool,
        catalog_revision="a"*64, selection_sha256=hashlib.sha256(name.encode()).hexdigest(),
        parent_selection_id="legacy-selection" if pool == "live" else None,
        requested_market_ids=list(ids), acquisition_market_ids=list(ids), protected_market_ids=[],
        records=[{"identity": {"market_id": mid}} for mid in ids], missing_dependency_market_ids=[],
        config={"active_scope_id": name} if pool == "live" else {"projection_id": name, "universe_revision": name})
    result["candidate_id"] = content_sha256(result)
    return result


@pytest.fixture
def operations(tmp_path):
    runtimes = {p: Runtime(p) for p in ("live", "discovery")}
    config = {k: 100000 for k in ("maximum_artifact_bytes", "maximum_row_bytes", "maximum_source_bytes",
        "maximum_relation_members", "maximum_metadata_tokens", "maximum_dependency_markets", "maximum_book_age_ms")}
    config.update(store_path=str(tmp_path/"hot.sqlite"), owner_lock=str(tmp_path/"owner.lock"),
        source_root=str(tmp_path), maximum_candidates=4, candidate_ttl_seconds=900,
        depth_quantities=["10"], legacy_discovery_universe="legacy", legacy_selection_id="legacy-selection",
        runtimes={p: dict(socket_path=str(tmp_path/p), maximum_bytes=100000, timeout_seconds=2) for p in runtimes})
    control = SimpleNamespace(source=SimpleNamespace(revision="a"*64), wall_ms=lambda: 1000)
    ops = HotScopeOperations(control, config, client_factory=lambda **kw: runtimes[kw["socket_path"].name])
    return ops, runtimes


def activation(c, expected="old-discovery", revision=1):
    return dict(schema_version="marketcow.hot-scope-activate.v1", pool=c["pool"], candidate_id=c["candidate_id"],
        expected_scope_id=expected, expected_revision=revision, protected_market_ids=[])


def test_prepare_does_not_publish_and_cas_reads_runtime(operations):
    ops, runtimes = operations
    c = candidate()
    result = ops._prepare("caller", c, 2000)
    assert result["publication_applied"] is False
    assert ops.current_discovery_selection() == "legacy-selection"
    assert ops._prepare("caller", c, 2000) == result
    assert len(runtimes["discovery"].commands) == 2
    with pytest.raises(ValueError, match="revision conflict"):
        ops.activate("caller", activation(c, revision=2))
    assert ops.status("discovery")["actual"]["projection_id"] == "old-discovery"
    result = ops.activate("caller", activation(c))
    assert result["new_full_sync_required"] is True
    assert ops.current_discovery_selection() == c["selection_sha256"]
    # Reopening the controller reconciles actual Rust identity, not old config.
    restored = HotScopeOperations(ops.control, ops.config, client_factory=lambda **kw: runtimes[kw["socket_path"].name])
    assert restored.current_discovery_selection() == c["selection_sha256"]


def test_shared_hot_response_vectors_match_status_and_reconcile(operations):
    vectors = json.loads((Path(__file__).parent/"contracts/hot-scope/responses.json").read_bytes())
    ops, runtimes = operations
    for pool in ("live", "discovery"):
        runtimes[pool].actual = copy.deepcopy(vectors["actual_"+pool])
        assert ops.status(pool) == vectors["status_"+pool]
    c = candidate()
    prepared = ops._prepare("caller", c, 2000)
    reconcile = dict(schema_version="marketcow.hot-scope-reconcile.v1", pool="discovery",
        candidate_id=c["candidate_id"], expected_scope_id="old-discovery", expected_revision=1)
    assert ops.reconcile("caller", reconcile) == prepared
    activated = ops.activate("caller", activation(c))
    assert set(activated) == {"schema_version", "candidate_id", "selection_id", "actual", "new_full_sync_required"}
    assert activated["actual"] == runtimes["discovery"].status()
    reconciled = ops.reconcile("caller", reconcile)
    assert reconciled == dict(schema_version="marketcow.hot-scope-reconciled.v1", stage="active",
        actual=activated["actual"], candidate_id=c["candidate_id"], selection_id=c["selection_sha256"],
        publication_applied_by_this_operation=False)


def test_lost_prepare_receipt_reserves_candidate_and_preserves_incumbent(operations):
    ops, runtimes = operations
    runtimes["discovery"].fail = "prepare_acquisition"
    with pytest.raises(RuntimeError, match="lost receipt"):
        ops._prepare("caller", candidate(), 2000)
    with pytest.raises(RuntimeError, match="reconciliation"):
        ops._prepare("caller", candidate(), 2000)
    assert len(runtimes["discovery"].commands) == 1
    assert ops.current_discovery_selection() == "legacy-selection"


def test_retirement_waits_for_durable_and_socket_receipts(operations):
    ops, runtimes = operations
    old, new = candidate(ids=("1", "2")), candidate(name="next", ids=("2", "3"))
    ops._prepare("caller", old, 2000)
    ops.activate("caller", activation(old))
    ops._prepare("caller", new, 2000)
    ops.activate("caller", activation(new, expected="new", revision=2))
    body = dict(schema_version="marketcow.hot-scope-retire.v1", pool="discovery", candidate_id=old["candidate_id"])
    assert ops.retire("caller", body)["resources_released"] is False
    assert runtimes["discovery"].commands[-1]["market_ids"] == ["1"]
    assert ops.retire("caller", body)["resources_released"] is False
    runtimes["discovery"].actual.update(retirement_persisted=1, acquisition={"retiring_shards": 1})
    assert ops.retire("caller", body)["resources_released"] is False
    runtimes["discovery"].actual["acquisition"]["retiring_shards"] = 0
    assert ops.retire("caller", body)["resources_released"] is True
    assert len(ops.status("discovery")["candidates"]) == 1


def test_foreign_caller_expiry_protection_and_tamper(operations):
    ops, _ = operations
    c = candidate()
    ops._prepare("caller", c, 2000)
    with pytest.raises(ValueError):
        ops.activate("other", activation(c))
    wrong = activation(c); wrong["protected_market_ids"] = ["outside"]
    with pytest.raises(ValueError, match="protected"):
        ops.activate("caller", wrong)
    ops.control.wall_ms = lambda: 2000
    with pytest.raises(ValueError, match="expired"):
        ops.activate("caller", activation(c))
    with ops._db() as db:
        db.execute("UPDATE hot_candidates SET body=?", (json.dumps({}).encode(),))
    with pytest.raises(ValueError, match="hash"):
        ops.status("discovery")


def test_metadata_chunks_respect_complete_wire_budget(operations):
    from marketcow.polymarket_contracts import canonical_json
    ops, _ = operations
    ops.config["runtimes"]["discovery"]["maximum_bytes"] = 1024
    c = candidate(ids=tuple(str(i) for i in range(30)))
    commands = list(ops._acquisition_commands(c, dict(expected_scope_id="old", expected_revision=1)))
    assert len(commands) > 1
    assert all(len(canonical_json(command)) <= 1024 for command in commands)
    assert [r for command in commands for r in command["records"]] == c["records"]
    assert sorted(mid for command in commands for mid in command["acquisition_market_ids"]) == sorted(c["acquisition_market_ids"])
    c["records"][0]["oversize"] = "x"*1024
    with pytest.raises(ValueError, match="single metadata"):
        list(ops._acquisition_commands(c, dict(expected_scope_id="old", expected_revision=1)))


def test_explicit_reconcile_repairs_lost_prepare_and_never_republishes(operations):
    ops, runtimes = operations
    runtime = runtimes["discovery"]
    c = candidate()
    runtime.fail = "prepare_acquisition"
    with pytest.raises(RuntimeError):
        ops._prepare("caller", c, 2000)
    runtime.fail = None
    body = dict(schema_version="marketcow.hot-scope-reconcile.v1", pool="discovery", candidate_id=c["candidate_id"],
                expected_scope_id="old-discovery", expected_revision=1)
    assert ops.reconcile("caller", body)["publication_applied"] is False
    ops.activate("caller", activation(c))
    count = len(runtime.commands)
    assert ops.reconcile("caller", body)["stage"] == "active"
    assert len(runtime.commands) == count


def test_legacy_collection_protects_runtime_references_and_pending_candidate(operations):
    ops, runtimes = operations
    runtime = runtimes["discovery"]
    runtime.actual.update(source_readable=True, admitted_market_ids=["1", "2", "3"], referenced_market_ids=["1"])
    ops._prepare("caller", candidate(ids=("2",)), 2000)
    body = dict(schema_version="marketcow.hot-scope-collect.v1", pool="discovery", expected_scope_id="old-discovery", expected_revision=1)
    result = ops.collect("caller", body)
    assert result["retirement"]["removed_market_ids"] == ["3"]
    assert result["resources_released"] is False
    runtime.actual.update(admitted_market_ids=["1", "2"], retirement_persisted=1)
    assert ops.collect("caller", body)["resources_released"] is True
    assert ops.collect("caller", body)["removed_market_ids"] == []


def test_pending_collection_fences_new_preparation(operations):
    ops, runtimes = operations
    runtime = runtimes["discovery"]
    runtime.actual.update(source_readable=True, admitted_market_ids=["1", "2"], referenced_market_ids=["1"])
    body = dict(schema_version="marketcow.hot-scope-collect.v1", pool="discovery", expected_scope_id="old-discovery", expected_revision=1)
    ops.collect("caller", body)
    with pytest.raises(RuntimeError, match="pending cleanup"):
        ops._prepare("caller", candidate(ids=("2",)), 2000)
    runtime.actual.update(admitted_market_ids=["1"], retirement_persisted=1)
    assert ops.collect("caller", body)["resources_released"]
    assert not ops._prepare("caller", candidate(ids=("2",)), 2000)["publication_applied"]


def test_candidate_retirement_after_collection_does_not_remove_unknown_ids(operations):
    ops, runtimes = operations
    c = candidate()
    ops._prepare("caller", c, 2000)
    runtimes["discovery"].actual["admitted_market_ids"] = []
    before = len(runtimes["discovery"].commands)
    assert ops.retire("caller", dict(schema_version="marketcow.hot-scope-retire.v1", pool="discovery",
        candidate_id=c["candidate_id"]))["resources_released"]
    assert len(runtimes["discovery"].commands) == before


def test_retirement_intent_survives_lost_reply_and_runtime_restart(operations):
    ops, runtimes = operations
    runtime = runtimes["discovery"]
    c = candidate()
    ops._prepare("caller", c, 2000)
    runtime.actual.update(admitted_market_ids=["1"], stream_instance_id="first")
    runtime.fail = "retire_acquisition"
    body = dict(schema_version="marketcow.hot-scope-retire.v1", pool="discovery", candidate_id=c["candidate_id"])
    with pytest.raises(RuntimeError, match="lost receipt"):
        ops.retire("caller", body)
    runtime.fail = None
    assert not ops.retire("caller", body)["resources_released"]
    assert runtime.actual["retirement_submitted"] == 1
    # Startup completed the durable intent, then reset process-local tickets.
    runtime.actual.update(admitted_market_ids=[], stream_instance_id="second", retirement_submitted=0)
    assert ops.retire("caller", body)["resources_released"]
