from __future__ import annotations

import unittest

from marketcow.admin_auth import (
    AdminAuth, AdminSecurityMiddleware, Identity, load_admin_tokens,
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

    def test_command_feature_flag_is_explicit(self):
        middleware = AdminSecurityMiddleware(
            object(), AdminAuth(False), commands_enabled=False
        )
        self.assertFalse(middleware.commands_enabled)


if __name__ == "__main__":
    unittest.main()
