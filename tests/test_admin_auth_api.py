from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.admin_auth import generate_service_api_key, hash_admin_password
from marketcow.config import Settings
from tests.test_market_data_api import Service


class AdminAuthApiTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        root = Path(self.folder.name) / "runtime"
        base = Settings(
            raw_path=root / "raw", storage_root=root, allowed_root=root.parent,
            postgres_dsn="postgresql://u:p@127.0.0.1/test",
            clickhouse_password="secret", profile="test", port=8793,
            postgres_schema="test", clickhouse_database="test",
            clickhouse_spool_path=root / "spool",
        )
        self.settings = replace(
            base,
            admin_auth_required=True,
            admin_tokens_json=(
                '{"viewer-token-123456789":"viewer",'
                '"operator-token-123456":"operator"}'
            ),
            admin_users_json=(
                '{"admin":{"role":"admin","password_hash":"'
                + hash_admin_password(
                    "correct horse battery staple", salt=b"0123456789abcdef"
                )
                + '"}}'
            ),
        )
        self.service_api_key, service_key_hash = generate_service_api_key(
            "history-worker"
        )
        self.settings = replace(
            self.settings,
            service_accounts_json=(
                '{"history-worker":{"role":"operator","key_hash":"'
                + service_key_hash
                + '","scopes":["history:read","history:write"],"enabled":true}}'
            ),
        )

    def tearDown(self):
        self.folder.cleanup()

    def test_admin_reads_require_login_and_security_headers_are_present(self):
        with TestClient(create_app(self.settings, Service())) as client:
            unauthorized = client.get("/v1/admin/dashboards")
            self.assertEqual(unauthorized.status_code, 401)
            self.assertEqual(unauthorized.headers["x-content-type-options"], "nosniff")
            login = client.post(
                "/v1/auth/session", json={"token": "viewer-token-123456789"}
            )
            self.assertEqual(login.status_code, 200)
            self.assertIn("HttpOnly", login.headers["set-cookie"])
            self.assertEqual(login.headers["x-content-type-options"], "nosniff")
            allowed = client.get("/v1/admin/dashboards")
            self.assertEqual(allowed.status_code, 200)
            self.assertEqual(allowed.headers["x-frame-options"], "DENY")

    def test_roles_csrf_and_bearer_behavior(self):
        with TestClient(create_app(self.settings, Service())) as viewer:
            viewer.post(
                "/v1/auth/session", json={"token": "viewer-token-123456789"}
            )
            forbidden = viewer.post("/v1/admin/history-jobs/example/cancel")
            self.assertEqual(forbidden.status_code, 403)
            self.assertEqual(forbidden.json()["detail"]["code"], "insufficient_role")

        with TestClient(create_app(self.settings, Service())) as operator:
            operator.post(
                "/v1/auth/session", json={"token": "operator-token-123456"}
            )
            csrf_failure = operator.post("/v1/admin/history-jobs/example/cancel")
            self.assertEqual(csrf_failure.status_code, 403)
            self.assertEqual(csrf_failure.json()["detail"]["code"], "csrf_validation_failed")
            csrf = operator.cookies.get("marketcow_csrf")
            accepted_by_security = operator.post(
                "/v1/admin/history-jobs/example/cancel",
                headers={"X-CSRF-Token": csrf},
            )
            self.assertNotEqual(accepted_by_security.json()["detail"]["code"]
                                if isinstance(accepted_by_security.json().get("detail"), dict)
                                else "", "csrf_validation_failed")

        with TestClient(create_app(self.settings, Service())) as bearer:
            response = bearer.post(
                "/v1/admin/history-jobs/example/cancel",
                headers={"Authorization": "Bearer operator-token-123456"},
            )
            self.assertNotEqual(response.status_code, 401)
            self.assertNotEqual(response.json()["detail"]["code"]
                                if isinstance(response.json().get("detail"), dict)
                                else "", "csrf_validation_failed")

    def test_command_feature_flag_fails_closed(self):
        settings = replace(self.settings, admin_commands_enabled=False)
        with TestClient(create_app(settings, Service())) as client:
            response = client.post(
                "/v1/admin/history-jobs/example/cancel",
                headers={"Authorization": "Bearer operator-token-123456"},
            )
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json()["detail"]["code"], "admin_commands_disabled")

    def test_username_and_password_login(self):
        with TestClient(create_app(self.settings, Service())) as client:
            login = client.post("/v1/auth/session", json={
                "username": "admin",
                "password": "correct horse battery staple",
            })
            self.assertEqual(login.status_code, 200)
            self.assertEqual(login.json()["actor"], "admin")
            self.assertEqual(login.json()["role"], "admin")
            rejected = TestClient(create_app(self.settings, Service())).post(
                "/v1/auth/session",
                json={"username": "admin", "password": "wrong password"},
            )
            self.assertEqual(rejected.status_code, 401)

    def test_service_account_is_limited_to_history_job_routes(self):
        headers = {"Authorization": f"Bearer {self.service_api_key}"}
        with TestClient(create_app(self.settings, Service())) as client:
            history = client.get("/v1/admin/history-jobs?limit=1", headers=headers)
            self.assertNotEqual(history.status_code, 401)
            self.assertNotEqual(history.status_code, 403)
            unrelated = client.get("/v1/admin/dashboards", headers=headers)
            self.assertEqual(unrelated.status_code, 403)
            self.assertEqual(
                unrelated.json()["detail"],
                {"code": "insufficient_scope", "required": "admin:read"},
            )


if __name__ == "__main__":
    unittest.main()
