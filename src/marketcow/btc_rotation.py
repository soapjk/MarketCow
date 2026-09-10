"""Reviewed-hour orchestration using existing HotScopeOperations and Rust reads.

No alternate scope publisher. Lost mutation receipts require reconciliation;
reopening an attempt never implicitly repeats a mutation. The caller supplies
the existing authenticated control object and explicitly approved parent pool.
"""
import asyncio
import hashlib
import os
from pathlib import Path
from urllib.parse import urlsplit

from .btc_hourly_dataset import canonical
from .btc_polymarket_capture import capture
from .universe_live_probe import strict_json


class HotHttpOperations:
    """Existing authenticated management routes, with bounded response bodies."""
    def __init__(self, endpoint, bearer, *, timeout, maximum_bytes, transport=None):
        import httpx
        url = urlsplit(endpoint)
        if (url.scheme not in ("http", "https") or not url.hostname or url.username or url.password
                or url.query or url.fragment
                or (url.scheme == "http" and url.hostname not in ("127.0.0.1", "::1", "localhost"))):
            raise ValueError("private_or_tls_management_required")
        if not bearer or "\n" in bearer or "\r" in bearer or not 0 < timeout <= 300 or not 0 < maximum_bytes <= 1048576:
            raise ValueError("explicit_control_credentials_and_budget_required")
        self.endpoint, self.maximum = endpoint.rstrip("/"), maximum_bytes
        self.client = httpx.Client(timeout=timeout, follow_redirects=False, trust_env=False,
                                   headers={"Authorization": "Bearer " + bearer}, transport=transport)

    def _request(self, method, route, body=None):
        if method not in ("GET", "POST"):
            raise ValueError("unsupported_control_method")
        content = None if body is None else canonical(body)
        if content is not None and len(content) > 65536:
            raise ValueError("control_request_capacity")
        headers = {} if content is None else {"Content-Type": "application/json"}
        with self.client.stream(method, self.endpoint + "/v1/prediction-markets/polymarket/" + route,
                                content=content, headers=headers) as response:
            raw = bytearray()
            for chunk in response.iter_bytes(chunk_size=16384):
                if len(raw) + len(chunk) > self.maximum:
                    raise ValueError("control_response_capacity")
                raw.extend(chunk)
            if response.status_code != 200:
                raise RuntimeError("control_response_requires_reconciliation")
        value = strict_json(raw)
        return value, hashlib.sha256(raw).hexdigest()

    def _post(self, route, body):
        raw = canonical(body)
        if len(raw) > 65536:
            raise ValueError("control_request_capacity")
        return self._request("POST", "hot-scopes/" + route, body)[0]

    def admit_discovery(self, request):
        return self._request("POST", "discovery-selections/admit", request)

    def prepare_discovery(self, caller, request, admission_response_sha256):
        return self._request("POST", "hot-scopes/discovery/prepare", {
            "schema_version": "marketcow.polymarket.discovery-prepare.v1",
            "admission_request": request,
            "admission_response_sha256": admission_response_sha256,
        })[0]

    def status(self, pool):
        if pool not in ("discovery", "live"):
            raise ValueError("invalid_hot_pool")
        return self._request("GET", "hot-scopes/status?pool=" + pool)[0]

    def prepare_live(self, caller, request):
        return self._post("live/prepare", request)

    def activate(self, caller, request):
        return self._post("activate", request)

    def close(self):
        self.client.close()


def receipt(path, value):
    raw = canonical(value)
    if len(raw) > 1024 * 1024:
        raise ValueError("rotation_receipt_capacity")
    temporary = path.with_suffix(".pending")
    with temporary.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


async def discovery_baseline(endpoint, expected, required_market_ids, *, maximum_bytes,
                             timeout, transport=None):
    """Read one post-activation baseline; no stream or implicit retries."""
    import httpx
    if not 0 < maximum_bytes <= 256 * 1024 * 1024 or not 0 < timeout <= 300:
        raise ValueError("invalid_discovery_baseline_budget")
    url = urlsplit(endpoint)
    if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password:
        raise ValueError("invalid_discovery_endpoint")
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False, trust_env=False,
                                 transport=transport) as client:
        async with client.stream("GET", endpoint.rstrip("/") +
                                 "/v1/prediction-markets/polymarket/live/discovery/full-sync") as response:
            response.raise_for_status()
            raw = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=65536):
                if len(raw) + len(chunk) > maximum_bytes:
                    raise ValueError("discovery_baseline_capacity")
                raw.extend(chunk)
    value = strict_json(raw)
    actual = expected["actual"]
    if (value.get("projection_id") != actual.get("projection_id")
            or value.get("universe_revision") != actual.get("universe_revision")):
        raise ValueError("discovery_baseline_generation_mismatch")
    identities = [row.get("market_id") for row in value.get("markets", [])]
    if len(identities) != len(set(identities)) or not set(required_market_ids) <= set(identities):
        raise ValueError("discovery_baseline_membership_mismatch")
    return {"projection_id": value["projection_id"], "universe_revision": value["universe_revision"],
            "boundary_cursor": value.get("boundary_cursor"), "market_count": len(identities),
            "required_market_ids": sorted(required_market_ids), "raw_bytes": len(raw),
            "raw_sha256": hashlib.sha256(raw).hexdigest()}


async def rotate_discovery_and_live(operations, caller, discovery_request, live_request,
                                    bindings, discovery_endpoint, live_endpoint, publish,
                                    attempt_root: Path, *, discovery_budget, capture_budgets,
                                    discovery_capture=discovery_baseline,
                                    live_capture=capture):
    """One durable Discovery-parent then Live-child activation transaction plan.

    Publication operations are individually atomic in Rust. The receipt makes the
    cross-pool boundary explicit: a lost mutation response is reconciled, never
    replayed automatically. This function never chooses or fills market IDs.
    """
    if (set(discovery_budget) != {"maximum_bytes", "timeout"}
            or any(type(v) is not int or v <= 0 for v in discovery_budget.values())):
        raise ValueError("invalid_discovery_capture_budget")
    d = discovery_request.model_dump(mode="json") if hasattr(discovery_request, "model_dump") else discovery_request
    if (d.get("schema_version") != "tradude.marketcow.discovery-selection-request.v1"
            or d.get("selection", {}).get("market_ids") != sorted(d.get("selection", {}).get("market_ids", []))
            or not d.get("selection", {}).get("market_ids")):
        raise ValueError("invalid_discovery_selection_request")
    parent = d["selection_sha256"]
    if (live_request.get("parent_selection_id") != parent
            or not set(live_request.get("market_ids", ())) <= set(d["selection"]["market_ids"])):
        raise ValueError("live_parent_membership_mismatch")
    ids = [binding["market_id"] for binding in bindings]
    if sorted(ids) != live_request.get("market_ids"):
        raise ValueError("live_binding_membership_mismatch")
    attempt_root.mkdir(parents=True, exist_ok=False)
    path = attempt_root / "receipt.json"
    report = {"schema_version": "marketcow.btc-hour.parent-live-rotation.v1",
              "stage": "admitting_discovery", "discovery_request_sha256": hashlib.sha256(canonical(d)).hexdigest(),
              "live_request_sha256": hashlib.sha256(canonical(live_request)).hexdigest(),
              "discovery": {}, "live": {}, "error": None, "execution_ready": False}
    await asyncio.to_thread(receipt, path, report)
    try:
        admitted, admitted_sha = await asyncio.to_thread(operations.admit_discovery, d)
        if (admitted.get("schema_version") != "marketcow.polymarket.discovery-admission.v1"
                or admitted.get("status") != "admitted" or admitted.get("selection_sha256") != parent
                or admitted.get("requested_market_ids") != d["selection"]["market_ids"]):
            raise ValueError("invalid_discovery_admission")
        report["discovery"].update(admission=admitted, admission_response_sha256=admitted_sha)
        report["stage"] = "preparing_discovery"
        await asyncio.to_thread(receipt, path, report)
        prepared = await asyncio.to_thread(operations.prepare_discovery, caller, d, admitted_sha)
        _validate_prepared(prepared, "discovery", d["selection"]["market_ids"])
        report["discovery"]["prepared"] = prepared
        report["stage"] = "activating_discovery"
        await asyncio.to_thread(receipt, path, report)
        activated = await asyncio.to_thread(operations.activate, caller, _activation(prepared, d["selection"]["protected_markets"]))
        _validate_activated(activated, prepared)
        report["discovery"]["activated"] = activated
        report["stage"] = "synchronizing_discovery"
        await asyncio.to_thread(receipt, path, report)
        baseline = await discovery_capture(discovery_endpoint, activated, live_request["market_ids"], **discovery_budget)
        report["discovery"]["baseline"] = baseline
        report["stage"] = "preparing_live"
        await asyncio.to_thread(receipt, path, report)
        prepared_live = await asyncio.to_thread(operations.prepare_live, caller, live_request)
        _validate_prepared(prepared_live, "live", live_request["market_ids"])
        report["live"]["prepared"] = prepared_live
        report["stage"] = "activating_live"
        await asyncio.to_thread(receipt, path, report)
        activated_live = await asyncio.to_thread(operations.activate, caller,
            _activation(prepared_live, [{"market_id": mid} for mid in live_request["protected_market_ids"]]))
        _validate_activated(activated_live, prepared_live)
        report["live"]["activated"] = activated_live
        report["stage"] = "synchronizing_live"
        await asyncio.to_thread(receipt, path, report)
        actual = activated_live["actual"]
        captured = await live_capture(live_endpoint, actual["scope_id"], bindings, publish, **capture_budgets)
        if captured.get("scope_id") != actual["scope_id"] or captured.get("ready_observed") is not True:
            raise ValueError("new_live_baseline_not_ready")
        if actual.get("stream_instance_id") is not None and captured.get("stream_instance_id") != actual["stream_instance_id"]:
            raise ValueError("new_live_baseline_instance_mismatch")
        report["live"]["capture"] = captured
        report["stage"] = "capture_complete"
    except Exception as exc:
        report.update(error=type(exc).__name__, failed_stage=report["stage"], stage="requires_reconciliation")
        await asyncio.to_thread(receipt, path, report)
        raise
    await asyncio.to_thread(receipt, path, report)
    return report


def _validate_prepared(value, pool, market_ids):
    if (value.get("schema_version") != "marketcow.hot-scope-prepared.v1" or value.get("pool") != pool
            or value.get("publication_applied") is not False or value.get("requested_market_ids") != market_ids
            or value.get("acquisition", {}).get("all_installed") is not True):
        raise ValueError("invalid_" + pool + "_preparation_receipt")


def _activation(prepared, protected):
    ids = sorted(row["market_id"] for row in protected)
    return {"schema_version": "marketcow.hot-scope-activate.v1", "pool": prepared["pool"],
            "candidate_id": prepared["candidate_id"], "expected_scope_id": prepared["expected"]["expected_scope_id"],
            "expected_revision": prepared["expected"]["expected_revision"], "protected_market_ids": ids}


def _validate_activated(value, prepared):
    if (value.get("schema_version") != "marketcow.hot-scope-activated.v1"
            or value.get("candidate_id") != prepared.get("candidate_id")
            or value.get("selection_id") != prepared.get("selection_id")
            or value.get("new_full_sync_required") is not True):
        raise ValueError("invalid_activation_receipt")


async def rotate_and_capture(operations, caller, request, bindings, endpoint, publish,
                             attempt_root, *, capture_budgets, capture_function=capture):
    ids = [b["market_id"] for b in bindings]
    required = {"seconds", "maximum_frames", "maximum_total_bytes", "maximum_frame_bytes", "maximum_fullsync_bytes"}
    if (set(capture_budgets) != required or any(type(v) is not int or v <= 0 for v in capture_budgets.values())
            or capture_budgets["seconds"] > 1800):
        raise ValueError("invalid_capture_budget")
    url = urlsplit(endpoint)
    if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise ValueError("invalid_endpoint")
    if (not 1 <= len(ids) <= 3 or len(set(ids)) != len(ids)
            or sorted(ids) != request.get("market_ids")
            or request.get("schema_version") != "marketcow.hot-live-prepare.v1"):
        raise ValueError("rotation_binding_membership")
    # A distinct immutable attempt is a deliberate operator action, not a retry.
    attempt_root.mkdir(parents=True, exist_ok=False)
    path = attempt_root / "receipt.json"
    report = dict(schema_version="marketcow.btc-hour.rotation.v1", stage="preparing",
                  request_sha256=hashlib.sha256(canonical(request)).hexdigest(),
                  requested_market_ids=sorted(ids), prepared=None, activated=None,
                  capture=None, error=None, execution_ready=False)
    await asyncio.to_thread(receipt, path, report)
    try:
        prepared = await asyncio.to_thread(operations.prepare_live, caller, request)
        if (prepared.get("schema_version") != "marketcow.hot-scope-prepared.v1"
                or prepared.get("pool") != "live" or prepared.get("publication_applied") is not False
                or prepared.get("requested_market_ids") != sorted(ids)
                or prepared.get("expected") != {"expected_scope_id": request["expected_scope_id"],
                                               "expected_revision": request["expected_revision"]}
                or prepared.get("acquisition", {}).get("all_installed") is not True):
            raise ValueError("invalid_preparation_receipt")
        report.update(prepared=prepared, stage="activating")
        # This durable intent precedes publication, but is not an applied receipt.
        await asyncio.to_thread(receipt, path, report)
        command = dict(schema_version="marketcow.hot-scope-activate.v1", pool="live",
                       candidate_id=prepared["candidate_id"],
                       expected_scope_id=request["expected_scope_id"],
                       expected_revision=request["expected_revision"],
                       protected_market_ids=request["protected_market_ids"])
        activated = await asyncio.to_thread(operations.activate, caller, command)
        if (activated.get("schema_version") != "marketcow.hot-scope-activated.v1"
                or activated.get("candidate_id") != prepared["candidate_id"]
                or activated.get("selection_id") != prepared["selection_id"]
                or activated.get("new_full_sync_required") is not True):
            raise ValueError("invalid_activation_receipt")
        actual = activated["actual"]
        if (not isinstance(actual.get("scope_id"), str) or not actual["scope_id"]
                or type(actual.get("revision")) is not int
                or actual["revision"] <= request["expected_revision"]):
            raise ValueError("invalid_actual_generation")
        report.update(activated=activated, stage="synchronizing")
        await asyncio.to_thread(receipt, path, report)
        result = await capture_function(endpoint, actual["scope_id"], bindings, publish, **capture_budgets)
        if result.get("scope_id") != actual["scope_id"] or result.get("ready_observed") is not True:
            raise ValueError("new_baseline_not_ready")
        expected_instance = actual.get("stream_instance_id")
        if expected_instance is not None and result.get("stream_instance_id") != expected_instance:
            raise ValueError("new_baseline_instance_mismatch")
        report.update(capture=result, stage="capture_complete")
    except Exception as exc:
        report.update(error=type(exc).__name__, failed_stage=report["stage"],
                      stage="requires_reconciliation" if report["stage"] in ("preparing", "activating") else "capture_failed")
        await asyncio.to_thread(receipt, path, report)
        raise
    await asyncio.to_thread(receipt, path, report)
    return report
