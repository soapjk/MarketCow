from __future__ import annotations

import unittest

from marketcow.admin_auth import (
    AdminAuth, AdminSecurityMiddleware, Identity, hash_admin_password,
    generate_service_api_key, hash_service_api_key, load_admin_tokens,
    load_admin_users, load_service_accounts, verify_admin_password,
)


class Clock:
    def __init__(self):
        self.value = 1000

    def __call__(self):
        return self.value


class AdminAuthTest(unittest.TestCase):
    def test_tokens_sessions_roles_and_csrf(self):
        clock = Clock()
        auth = AdminAuth(
            True,
            '{"viewer-token-123456789":"viewer","operator-token-123456":"operator"}',
            session_seconds=300,
            clock=clock,
        )
        session_id, viewer = auth.login("viewer-token-123456789")
        authenticated = auth.authenticate({}, {"marketcow_admin_session": session_id})
        self.assertEqual(authenticated, viewer)
        self.assertTrue(auth.permits(viewer, "viewer"))
        self.assertFalse(auth.permits(viewer, "operator"))
        self.assertFalse(auth.csrf_valid(viewer, {}))
        self.assertTrue(auth.csrf_valid(viewer, {"x-csrf-token": viewer.csrf}))
        bearer = auth.authenticate(
            {"authorization": "Bearer operator-token-123456"}, {}
        )
        self.assertEqual(bearer.role, "operator")
        self.assertTrue(auth.csrf_valid(bearer, {}))
        clock.value += 301
        self.assertIsNone(auth.authenticate({}, {"marketcow_admin_session": session_id}))

    def test_disabled_auth_is_explicit_local_admin(self):
        auth = AdminAuth(False)
        self.assertEqual(auth.authenticate({}, {}), Identity(
            actor="local-development", role="admin"
        ))

    def test_invalid_configuration_and_login_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "at least one"):
            AdminAuth(True)
        with self.assertRaisesRegex(ValueError, "length"):
            load_admin_tokens('{"short":"admin"}')
        auth = AdminAuth(True, '{"valid-token-123456":"admin"}')
        with self.assertRaises(PermissionError):
            auth.login("wrong-token-123456")

    def test_password_users_are_hashed_and_create_named_sessions(self):
        password_hash = hash_admin_password(
            "correct horse battery staple", salt=b"0123456789abcdef"
        )
        users_json = (
            '{"admin":{"role":"admin","password_hash":"' + password_hash + '"}}'
        )
        auth = AdminAuth(True, users_json=users_json)
        session_id, identity = auth.login_credentials(
            "admin", "correct horse battery staple"
        )
        self.assertEqual(identity.actor, "admin")
        self.assertEqual(identity.role, "admin")
        self.assertEqual(
            auth.authenticate({}, {"marketcow_admin_session": session_id}), identity
        )
        self.assertTrue(verify_admin_password(
            "correct horse battery staple", password_hash
        ))
        self.assertFalse(verify_admin_password("wrong password", password_hash))
        with self.assertRaises(PermissionError):
            auth.login_credentials("admin", "wrong password")
        with self.assertRaises(PermissionError):
            auth.login_credentials("missing", "correct horse battery staple")
        with self.assertRaisesRegex(ValueError, "credentials"):
            load_admin_users(
                '{"admin":{"role":"admin","password_hash":"not-a-hash"}}'
            )

    def test_command_feature_flag_is_explicit(self):
        middleware = AdminSecurityMiddleware(
            object(), AdminAuth(False), commands_enabled=False
        )
        self.assertFalse(middleware.commands_enabled)

    def test_service_accounts_use_hashed_keys_and_scoped_identities(self):
        api_key, key_hash = generate_service_api_key("history-worker")
        self.assertTrue(api_key.startswith("mcsa.history-worker."))
        self.assertEqual(hash_service_api_key(api_key), key_hash)
        declaration = (
            '{"history-worker":{"role":"operator","key_hash":"' + key_hash
            + '","scopes":["history:read","history:write"],"enabled":true}}'
        )
        accounts = load_service_accounts(declaration)
        self.assertNotIn(api_key, declaration)
        self.assertEqual(accounts["history-worker"].key_hash, key_hash)
        auth = AdminAuth(True, service_accounts_json=declaration)
        identity = auth.authenticate(
            {"authorization": f"Bearer {api_key}"}, {}
        )
        self.assertEqual(identity.actor, "service:history-worker")
        self.assertTrue(auth.permits_scope(identity, "history:write"))
        self.assertFalse(auth.permits_scope(identity, "admin:read"))
        self.assertIsNone(auth.authenticate(
            {"authorization": "Bearer mcsa.history-worker.invalid-secret-value"}, {}
        ))

    def test_disabled_and_invalid_service_accounts_fail_closed(self):
        api_key, key_hash = generate_service_api_key("history-worker")
        declaration = (
            '{"history-worker":{"role":"operator","key_hash":"' + key_hash
            + '","scopes":["history:read"],"enabled":false}}'
        )
        auth = AdminAuth(True, service_accounts_json=declaration)
        self.assertIsNone(auth.authenticate(
            {"authorization": f"Bearer {api_key}"}, {}
        ))
        with self.assertRaisesRegex(ValueError, "scopes"):
            load_service_accounts(declaration.replace("history:read", "unknown"))


if __name__ == "__main__":
    unittest.main()
