import base64
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "history_probe", Path(__file__).parents[1] / "scripts/probe_marketcow_history.py")
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def fixture():
    raw = b"error code: 1010\n"
    request = {"token_id": "1", "start_ts": 1, "end_ts": 2, "fidelity_minutes": 60}
    return request, {
        "schema_version": "marketcow.polymarket.price-history-evidence.v1",
        "request": request.copy(), "raw_complete": True, "upstream_status": 403,
        "raw_base64": base64.b64encode(raw).decode(), "raw_bytes": len(raw),
        "raw_sha256": hashlib.sha256(raw).hexdigest()}


def test_preserves_upstream_rejection_not_empty_history():
    request, evidence = fixture()
    parsed, raw = probe.verify_evidence(json.dumps(evidence), request)
    assert parsed["upstream_status"] == 403
    assert raw == b"error code: 1010\n"


@pytest.mark.parametrize("field,value", [
    ("raw_complete", False), ("raw_sha256", "0" * 64), ("raw_bytes", 0),
    ("request", {}), ("raw_base64", "!not-base64")])
def test_rejects_bad_evidence(field, value):
    request, evidence = fixture()
    evidence[field] = value
    with pytest.raises(ValueError):
        probe.verify_evidence(json.dumps(evidence), request)
