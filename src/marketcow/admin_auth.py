from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import secrets
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Mapping

from starlette.responses import JSONResponse


SESSION_COOKIE = "marketcow_admin_session"
CSRF_COOKIE = "marketcow_csrf"
ROLES = {"viewer": 1, "operator": 2, "admin": 3}
MAX_SESSIONS = 128
_USERNAME = re.compile(r"^[A-Za-z0-9_.-]{3,64}$")
_SCRYPT_N = 16384
_SCRYPT_R = 8
_SCRYPT_P = 1


@dataclass(frozen=True)
class Identity:
    actor: str
    role: str
    csrf: str = ""
    cookie_session: bool = False


@dataclass
class _Session:
    identity: Identity
    expires_at: float


@dataclass(frozen=True)
class AdminUser:
    role: str
    password_hash: str


def load_admin_tokens(raw: str) -> dict[str, str]:
    if not raw.strip():
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("admin tokens must be valid JSON") from exc
    if not isinstance(value, Mapping) or not 1 <= len(value) <= 20:
        raise ValueError("admin tokens must be an object with 1 to 20 entries")
    result = {}
    for token, role in value.items():
        token = str(token)
        role = str(role).strip().lower()
        if len(token) < 16 or len(token) > 500:
            raise ValueError("admin token length is invalid")
        if role not in ROLES:
            raise ValueError("admin token role is invalid")
        result[token] = role
    return result


def hash_admin_password(password: str, salt: bytes | None = None) -> str:
    if not 8 <= len(password) <= 128:
        raise ValueError("admin password length is invalid")
    salt = salt or secrets.token_bytes(16)
    if len(salt) != 16:
        raise ValueError("admin password salt is invalid")
    digest = hashlib.scrypt(
        password.encode(),
        salt=salt,
        n=_SCRYPT_N,
        r=_SCRYPT_R,
        p=_SCRYPT_P,
        dklen=32,
    )
    encoded_salt = base64.urlsafe_b64encode(salt).decode().rstrip("=")
    encoded_digest = base64.urlsafe_b64encode(digest).decode().rstrip("=")
    return (
        f"scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}"
        f"${encoded_salt}${encoded_digest}"
    )


def _decode_password_hash(encoded: str) -> tuple[bytes, bytes] | None:
    try:
        algorithm, n, r, p, encoded_salt, encoded_digest = encoded.split("$")
        if (
            algorithm != "scrypt"
            or (int(n), int(r), int(p)) != (_SCRYPT_N, _SCRYPT_R, _SCRYPT_P)
        ):
            return None
        salt = base64.urlsafe_b64decode(encoded_salt + "==")
        expected = base64.urlsafe_b64decode(encoded_digest + "==")
        if len(salt) != 16 or len(expected) != 32:
            return None
        return salt, expected
    except (ValueError, TypeError):
        return None


def verify_admin_password(password: str, encoded: str) -> bool:
    decoded = _decode_password_hash(encoded)
    if decoded is None:
        return False
    salt, expected = decoded
    try:
        actual = hashlib.scrypt(
            password.encode(),
            salt=salt,
            n=_SCRYPT_N,
            r=_SCRYPT_R,
            p=_SCRYPT_P,
            dklen=32,
        )
        return hmac.compare_digest(actual, expected)
    except (ValueError, TypeError):
        return False


def load_admin_users(raw: str) -> dict[str, AdminUser]:
    if not raw.strip():
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("admin users must be valid JSON") from exc
    if not isinstance(value, Mapping) or not 1 <= len(value) <= 20:
        raise ValueError("admin users must be an object with 1 to 20 entries")
    result = {}
    for username, declaration in value.items():
        username = str(username).strip()
        if not _USERNAME.fullmatch(username) or not isinstance(declaration, Mapping):
            raise ValueError("admin user declaration is invalid")
        role = str(declaration.get("role", "")).strip().lower()
        password_hash = str(declaration.get("password_hash", "")).strip()
        if role not in ROLES or _decode_password_hash(password_hash) is None:
            raise ValueError("admin user credentials are invalid")
        result[username] = AdminUser(role=role, password_hash=password_hash)
    return result


_DUMMY_PASSWORD_HASH = hash_admin_password(
    "marketcow-invalid-login", salt=b"\0" * 16
)


class AdminAuth:
    """Local bearer/bootstrap token auth with bounded HttpOnly sessions."""

    def __init__(
        self, required: bool, tokens_json: str = "", session_seconds: int = 28800,
        clock: Any = None, users_json: str = "",
    ) -> None:
        if not 300 <= session_seconds <= 2592000:
            raise ValueError("admin session duration is invalid")
        self.required = bool(required)
        self.tokens = load_admin_tokens(tokens_json)
        self.users = load_admin_users(users_json)
        if self.required and not (self.tokens or self.users):
            raise ValueError(
                "admin authentication requires at least one token or user"
            )
        self.session_seconds = session_seconds
        self.clock = clock or time.time
        self._sessions: OrderedDict[str, _Session] = OrderedDict()
        self._lock = threading.RLock()

    def login(self, token: str) -> tuple[str, Identity]:
        role = None
        for candidate, candidate_role in self.tokens.items():
            if hmac.compare_digest(token, candidate):
                role = candidate_role
        if role is None:
            raise PermissionError("invalid administration token")
        return self._create_session(f"local-{role}", role)

    def login_credentials(self, username: str, password: str) -> tuple[str, Identity]:
        normalized = username.strip()
        user = self.users.get(normalized)
        password_hash = user.password_hash if user else _DUMMY_PASSWORD_HASH
        valid = verify_admin_password(password, password_hash)
        if user is None or not valid:
            raise PermissionError("invalid administration credentials")
        return self._create_session(normalized, user.role)

    def _create_session(self, actor: str, role: str) -> tuple[str, Identity]:
        session_id = secrets.token_urlsafe(32)
        csrf = secrets.token_urlsafe(24)
        identity = Identity(
            actor=actor, role=role, csrf=csrf, cookie_session=True
        )
        with self._lock:
            self._expire()
            while len(self._sessions) >= MAX_SESSIONS:
                self._sessions.popitem(last=False)
            self._sessions[session_id] = _Session(
                identity=identity, expires_at=self.clock() + self.session_seconds
            )
        return session_id, identity

    def logout(self, session_id: str) -> None:
        with self._lock:
            self._sessions.pop(session_id, None)

    def authenticate(
        self, headers: Mapping[str, str], cookies: Mapping[str, str]
    ) -> Identity | None:
        if not self.required:
            return Identity(actor="local-development", role="admin")
        authorization = headers.get("authorization", "")
        if authorization.startswith("Bearer "):
            token = authorization[7:]
            for candidate, role in self.tokens.items():
                if hmac.compare_digest(token, candidate):
                    return Identity(actor=f"token-{role}", role=role)
            return None
        session_id = cookies.get(SESSION_COOKIE, "")
        if not session_id:
            return None
        with self._lock:
            self._expire()
            session = self._sessions.get(session_id)
            if session is None:
                return None
            self._sessions.move_to_end(session_id)
            return session.identity

    def _expire(self) -> None:
        now = self.clock()
        expired = [
            session_id for session_id, session in self._sessions.items()
            if session.expires_at <= now
        ]
        for session_id in expired:
            self._sessions.pop(session_id, None)

    @staticmethod
    def permits(identity: Identity, role: str) -> bool:
        return ROLES.get(identity.role, 0) >= ROLES[role]

    @staticmethod
    def csrf_valid(identity: Identity, headers: Mapping[str, str]) -> bool:
        if not identity.cookie_session:
            return True
        supplied = headers.get("x-csrf-token", "")
        return bool(supplied and hmac.compare_digest(supplied, identity.csrf))


class AdminSecurityMiddleware:
    def __init__(
        self, app: Any, auth: AdminAuth, commands_enabled: bool = True
    ) -> None:
        self.app = app
        self.auth = auth
        self.commands_enabled = commands_enabled

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        protected = path.startswith("/v1/admin/")
        if protected:
            headers = {
                key.decode("latin-1").lower(): value.decode("latin-1")
                for key, value in scope.get("headers", ())
            }
            cookies = _parse_cookies(headers.get("cookie", ""))
            identity = self.auth.authenticate(headers, cookies)
            if identity is None:
                await JSONResponse(
                    {"detail": {"code": "authentication_required"}},
                    status_code=401,
                    headers=_security_headers(),
                )(scope, receive, send)
                return
            required_role = "viewer" if scope.get("method") in {"GET", "HEAD"} else "operator"
            if required_role == "operator" and not self.commands_enabled:
                await JSONResponse(
                    {"detail": {"code": "admin_commands_disabled"}},
                    status_code=503,
                    headers=_security_headers(),
                )(scope, receive, send)
                return
            if not self.auth.permits(identity, required_role):
                await JSONResponse(
                    {"detail": {"code": "insufficient_role", "required": required_role}},
                    status_code=403,
                    headers=_security_headers(),
                )(scope, receive, send)
                return
            if scope.get("method") not in {"GET", "HEAD", "OPTIONS"} and not self.auth.csrf_valid(
                identity, headers
            ):
                await JSONResponse(
                    {"detail": {"code": "csrf_validation_failed"}},
                    status_code=403,
                    headers=_security_headers(),
                )(scope, receive, send)
                return
            scope["admin_identity"] = identity

        async def secure_send(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.extend(
                    (key.encode(), value.encode())
                    for key, value in _security_headers().items()
                )
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, secure_send)


def _parse_cookies(value: str) -> dict[str, str]:
    result = {}
    for item in value.split(";"):
        if "=" not in item:
            continue
        key, raw = item.split("=", 1)
        result[key.strip()] = raw.strip()
    return result


def _security_headers() -> dict[str, str]:
    return {
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "same-origin",
        "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
        "X-Frame-Options": "DENY",
        "Content-Security-Policy": (
            "default-src 'self'; frame-src http://127.0.0.1:3001; "
            "style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'"
        ),
    }
