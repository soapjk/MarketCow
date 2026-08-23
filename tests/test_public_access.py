from __future__ import annotations

import base64
import hashlib
import hmac
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from tests.test_market_data_api import Service


NOW = 1_800_000_000
INVESTRACE_KEY = "investrace-test-signing-key-000000000000000000"
MARKETCOW_KEY = "marketcow-test-signing-key-00000000000000000000"


def _encode(value: object) -> str:
    raw = json.dumps(value, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def jwt_token(
    *, issuer: str, audience: str, scope: str, key: str, client_id: str = "llmay",
    expires_at: int = NOW + 300, overrides: dict[str, object] | None = None,
    remove_claim: str = "",
) -> str:
    header = {"alg": "HS256", "typ": "JWT", "kid": "active"}
    claims = {
        "iss": issuer,
        "aud": audience,
        "sub": client_id,
        "client_id": client_id,
        "scope": scope,
        "iat": NOW - 10,
        "exp": expires_at,
        "jti": "test-token-id",
    }
    claims.update(overrides or {})
    if remove_claim:
        claims.pop(remove_claim, None)
    signing_input = f"{_encode(header)}.{_encode(claims)}"
    signature = hmac.new(key.encode(), signing_input.encode(), hashlib.sha256).digest()
    return signing_input + "." + base64.urlsafe_b64encode(signature).decode().rstrip("=")


class PublicAccessApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = tempfile.TemporaryDirectory()
        root = Path(self.folder.name) / "runtime-test"
        base = Settings(
            raw_path=root / "raw", storage_root=root, allowed_root=root.parent,
            postgres_dsn="postgresql://u:p@127.0.0.1/marketcow_test",
            clickhouse_password="secret", profile="test", port=8793,
            postgres_schema="marketcow_test", clickhouse_database="marketcow_test",
            clickhouse_spool_path=root / "spool",
        )
        self.settings = replace(
            base,
            public_read_enabled=True,
            public_read_allowlist=("/v1/health", "/v1/instruments/{instrument_id}"),
            public_rate_limit_requests=2,
            public_rate_limit_window_seconds=60,
            public_audit_path=root / "audit" / "public-access.jsonl",
            investrace_jwt_issuer="https://auth.investrace.test",
            investrace_jwt_audience="marketcow-public",
            investrace_jwt_scope="marketcow:read",
            investrace_jwt_keys_json=json.dumps({"active": INVESTRACE_KEY}),
            marketcow_jwt_issuer="marketcow",
            marketcow_jwt_audience="llmay",
            marketcow_jwt_scope="llmay:read",
            marketcow_jwt_keys_json=json.dumps({"active": MARKETCOW_KEY}),
        )
        self.now = lambda: datetime.fromtimestamp(NOW, timezone.utc)

    def tearDown(self) -> None:
        self.folder.cleanup()

    def headers(self, **changes: str) -> dict[str, str]:
        values = {
            "Authorization": "Bearer " + jwt_token(
                issuer="https://auth.investrace.test", audience="marketcow-public",
                scope="marketcow:read", key=INVESTRACE_KEY,
            ),
            "X-MarketCow-JWT": jwt_token(
                issuer="marketcow", audience="llmay", scope="llmay:read",
                key=MARKETCOW_KEY,
            ),
            "X-Request-ID": "trace-public-0001",
        }
        values.update(changes)
        return values

    def test_normal_m2m_chain_reaches_allowlisted_read_route(self) -> None:
        with TestClient(create_app(self.settings, Service(), self.now)) as client:
            response = client.get("/public/v1/health", headers=self.headers())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")
        self.assertEqual(response.headers["x-request-id"], "trace-public-0001")
        self.assertEqual(response.headers["x-ratelimit-remaining"], "1")

    def test_missing_invalid_and_expired_access_tokens_are_rejected(self) -> None:
        cases = [
            ({"Authorization": ""}, "missing_access_token"),
            ({"Authorization": "Bearer not-a-jwt"}, "invalid_access_token"),
            ({"Authorization": "Bearer " + jwt_token(
                issuer="https://auth.investrace.test", audience="marketcow-public",
                scope="marketcow:read", key=INVESTRACE_KEY, expires_at=NOW - 31,
            )}, "expired_access_token"),
        ]
        for changes, expected in cases:
            with self.subTest(expected=expected):
                with TestClient(create_app(self.settings, Service(), self.now)) as client:
                    response = client.get("/public/v1/health", headers=self.headers(**changes))
                self.assertEqual(response.status_code, 401)
                self.assertEqual(response.json()["detail"]["code"], expected)

    def test_non_allowlisted_routes_and_all_writes_are_rejected(self) -> None:
        with TestClient(create_app(self.settings, Service(), self.now)) as client:
            route = client.get("/public/v1/readiness", headers=self.headers())
            write = client.post("/public/v1/health", headers=self.headers())
        self.assertEqual(route.status_code, 403)
        self.assertEqual(route.json()["detail"]["code"], "route_not_allowlisted")
        self.assertEqual(write.status_code, 405)
        self.assertEqual(write.json()["detail"]["code"], "read_only_method_required")

    def test_rate_limit_returns_explicit_contract(self) -> None:
        with TestClient(create_app(self.settings, Service(), self.now)) as client:
            first = client.get("/public/v1/health", headers=self.headers())
            second = client.get("/public/v1/health", headers=self.headers())
            limited = client.get("/public/v1/health", headers=self.headers())
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(limited.status_code, 429)
        self.assertEqual(limited.json()["detail"]["code"], "rate_limit_exceeded")
        self.assertEqual(limited.headers["retry-after"], "60")
        self.assertEqual(limited.headers["x-ratelimit-limit"], "2")

    def test_marketcow_jwt_signature_and_claim_failures_are_rejected(self) -> None:
        bad_tokens = {
            "signature": jwt_token(
                issuer="marketcow", audience="llmay", scope="llmay:read",
                key="wrong-signing-key-000000000000000000000000000",
            ),
            "issuer": jwt_token(
                issuer="another-issuer", audience="llmay", scope="llmay:read",
                key=MARKETCOW_KEY,
            ),
            "audience": jwt_token(
                issuer="marketcow", audience="another-client", scope="llmay:read",
                key=MARKETCOW_KEY,
            ),
            "expired": jwt_token(
                issuer="marketcow", audience="llmay", scope="llmay:read",
                key=MARKETCOW_KEY, expires_at=NOW - 31,
            ),
            "missing_claim": jwt_token(
                issuer="marketcow", audience="llmay", scope="llmay:read",
                key=MARKETCOW_KEY, remove_claim="jti",
            ),
        }
        for name, token in bad_tokens.items():
            with self.subTest(name=name):
                with TestClient(create_app(self.settings, Service(), self.now)) as client:
                    response = client.get(
                        "/public/v1/health",
                        headers=self.headers(**{"X-MarketCow-JWT": token}),
                    )
                self.assertEqual(response.status_code, 401)
                expected = "expired_marketcow_jwt" if name == "expired" else "invalid_marketcow_jwt"
                self.assertEqual(response.json()["detail"]["code"], expected)

    def test_tokens_for_different_clients_are_rejected(self) -> None:
        token = jwt_token(
            issuer="marketcow", audience="llmay", scope="llmay:read",
            key=MARKETCOW_KEY, client_id="another-client",
        )
        with TestClient(create_app(self.settings, Service(), self.now)) as client:
            response = client.get(
                "/public/v1/health", headers=self.headers(**{"X-MarketCow-JWT": token})
            )
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["detail"]["code"], "token_client_mismatch")

    def test_audit_has_required_fields_and_never_contains_tokens(self) -> None:
        headers = self.headers()
        with TestClient(create_app(self.settings, Service(), self.now)) as client:
            accepted = client.get("/public/v1/health?secret=query", headers=headers)
            rejected = client.get(
                "/public/v1/health", headers={"Authorization": "Bearer secret-token-value"}
            )
            events = client.app.state.public_audit.events()
        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(rejected.status_code, 401)
        self.assertEqual(events[0], {
            "schema": "marketcow.public-access-audit.v1",
            "occurred_at": "2027-01-15T08:00:00+00:00",
            "caller": "llmay",
            "method": "GET",
            "path": "/v1/health",
            "status": 200,
            "result": "allowed",
            "trace_id": "trace-public-0001",
        })
        audit_text = self.settings.public_audit_path.read_text()
        self.assertNotIn(headers["Authorization"], audit_text)
        self.assertNotIn(headers["X-MarketCow-JWT"], audit_text)
        self.assertNotIn("secret=query", audit_text)
        self.assertNotIn("secret-token-value", audit_text)


if __name__ == "__main__":
    unittest.main()
