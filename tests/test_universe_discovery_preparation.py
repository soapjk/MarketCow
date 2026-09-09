"""HTTP orchestration uses synthetic admission/runtime; no U1 deployment."""
import asyncio
import hashlib
import json
from pathlib import Path

import httpx
import pytest

from marketcow.universe_control_http import Caller, PREFIX, create_control_app
from marketcow.universe_discovery_preparation import parse_preparation_request


def fixture_request():
    return json.loads((Path(__file__).parent/"contracts/phase1/selection-request.json").read_bytes())


def test_preparation_wire_is_strict_and_preserves_original_admission():
    request = fixture_request()
    body = dict(schema_version="marketcow.polymarket.discovery-prepare.v1", admission_request=request,
                admission_response_sha256="a"*64)
    parsed, digest = parse_preparation_request(json.dumps(body).encode(), 1048576)
    assert parsed.request_id == request["request_id"] and digest == "a"*64
    with pytest.raises(ValueError):
        parse_preparation_request(json.dumps(dict(body, activate=True)).encode(), 1048576)
    with pytest.raises(ValueError):
        parse_preparation_request(json.dumps(body).encode(), 10)
    with pytest.raises(ValueError, match="duplicate"):
        parse_preparation_request(b'{"schema_version":"x","schema_version":"y"}', 1048576)


def test_prepare_scope_does_not_grant_activate_or_accept_client_paths():
    class Control:
        profile = {"catalog": {"concurrent_readers": 2}, "admission": {"max_request_bytes": 1048576}}
    class Preparation:
        calls = []
        def prepare(self, caller, request, digest):
            self.calls.append((caller, request.request_id, digest))
            return {"status": "prepared", "active": False}
    preparation = Preparation()
    secret = "synthetic-test-only"
    caller = Caller("test", hashlib.sha256(secret.encode()).hexdigest(), frozenset({"discovery.prepare"}))
    app = create_control_app(Control(), callers=(caller,), body_timeout_seconds=1, discovery_preparation=preparation)
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 1234)), base_url="http://localhost") as client:
            path = PREFIX+"/discovery-selections/prepare"
            assert (await client.post(path, json={})).status_code == 401
            client.headers["authorization"] = "Bearer "+secret
            request = dict(schema_version="marketcow.polymarket.discovery-prepare.v1", admission_request=fixture_request(),
                           admission_response_sha256="a"*64)
            response = await client.post(path, json=request)
            assert response.status_code == 200 and response.json() == {"status": "prepared", "active": False}
            assert (await client.post(path, json=dict(request, root="/client/root"))).status_code == 400
            assert (await client.post(PREFIX+"/generations/apply", json={})).status_code == 404
    asyncio.run(run())
    assert len(preparation.calls) == 1
