from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from starlette.responses import JSONResponse


PUBLIC_PREFIX = "/public"
READ_METHODS = frozenset({"GET", "HEAD"})
_SAFE_PATH = re.compile(r"^/v1(?:/[A-Za-z0-9._~:{}-]+)+$")
_TEMPLATE_PARAMETER = re.compile(r"\{[A-Za-z][A-Za-z0-9_]*\}")
_TRACE_ID = re.compile(r"^[A-Za-z0-9._-]{8,128}$")
_SCOPE_SPLIT = re.compile(r"\s+")


class PublicAccessConfigurationError(ValueError):
    pass


class JwtValidationError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _json_without_duplicates(raw: bytes) -> Any:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise JwtValidationError("duplicate_json_member")
            result[key] = value
        return result

    try:
        return json.loads(raw, object_pairs_hook=pairs)
    except JwtValidationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JwtValidationError("malformed_json") from exc


def _decode_segment(value: str) -> bytes:
    if not value or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise JwtValidationError("malformed_token")
    try:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except ValueError as exc:
        raise JwtValidationError("malformed_token") from exc


def _scopes(value: Any) -> frozenset[str]:
    if isinstance(value, str):
        scopes = frozenset(item for item in _SCOPE_SPLIT.split(value.strip()) if item)
    elif isinstance(value, list) and all(isinstance(item, str) for item in value):
        scopes = frozenset(value)
    else:
        raise JwtValidationError("invalid_scope_claim")
    if not scopes or any(len(item) > 120 for item in scopes):
        raise JwtValidationError("invalid_scope_claim")
    return scopes


@dataclass(frozen=True)
class JwtPolicy:
    issuer: str
    audience: str
    required_scope: str
    keys: Mapping[str, str]
    leeway_seconds: int = 30
    max_lifetime_seconds: int = 3600

    def validate(self) -> None:
        if not self.issuer or not self.audience or not self.required_scope:
            raise PublicAccessConfigurationError("JWT issuer, audience and scope are required")
        if not 0 <= self.leeway_seconds <= 300:
            raise PublicAccessConfigurationError("JWT leeway must be between 0 and 300 seconds")
        if not 60 <= self.max_lifetime_seconds <= 86400:
            raise PublicAccessConfigurationError("JWT maximum lifetime is invalid")
        if not 1 <= len(self.keys) <= 10:
            raise PublicAccessConfigurationError("JWT key set must contain 1 to 10 keys")
        for kid, secret in self.keys.items():
            if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", kid) or len(secret.encode()) < 32:
                raise PublicAccessConfigurationError("JWT key declaration is invalid")


@dataclass(frozen=True)
class VerifiedJwt:
    subject: str
    client_id: str
    token_id: str
    claims: Mapping[str, Any]


class JwtVerifier:
    """Strict HS256 verifier with key rotation and OAuth-oriented claims checks."""

    def __init__(self, policy: JwtPolicy, clock: Callable[[], float] | None = None) -> None:
        policy.validate()
        self.policy = policy
        self.clock = clock or time.time

    def verify(self, token: str) -> VerifiedJwt:
        if not isinstance(token, str) or len(token) > 8192:
            raise JwtValidationError("malformed_token")
        parts = token.split(".")
        if len(parts) != 3:
            raise JwtValidationError("malformed_token")
        encoded_header, encoded_payload, encoded_signature = parts
        header = _json_without_duplicates(_decode_segment(encoded_header))
        claims = _json_without_duplicates(_decode_segment(encoded_payload))
        if not isinstance(header, dict) or not isinstance(claims, dict):
            raise JwtValidationError("malformed_token")
        if header.get("alg") != "HS256" or header.get("typ") not in {None, "JWT", "at+jwt"}:
            raise JwtValidationError("unsupported_token_algorithm")
        if set(header) - {"alg", "typ", "kid"}:
            raise JwtValidationError("unsupported_token_header")
        kid = header.get("kid")
        if not isinstance(kid, str) or kid not in self.policy.keys:
            raise JwtValidationError("unknown_signing_key")
        supplied_signature = _decode_segment(encoded_signature)
        expected_signature = hmac.new(
            self.policy.keys[kid].encode(),
            f"{encoded_header}.{encoded_payload}".encode("ascii"),
            hashlib.sha256,
        ).digest()
        if not hmac.compare_digest(supplied_signature, expected_signature):
            raise JwtValidationError("invalid_signature")

        required = {"iss", "aud", "sub", "client_id", "exp", "iat", "jti", "scope"}
        if any(name not in claims for name in required):
            raise JwtValidationError("missing_required_claim")
        if claims["iss"] != self.policy.issuer:
            raise JwtValidationError("invalid_issuer")
        audience = claims["aud"]
        audiences = {audience} if isinstance(audience, str) else set(audience) if (
            isinstance(audience, list) and all(isinstance(item, str) for item in audience)
        ) else set()
        if self.policy.audience not in audiences:
            raise JwtValidationError("invalid_audience")

        now = self.clock()
        exp = claims["exp"]
        issued_at = claims["iat"]
        not_before = claims.get("nbf", issued_at)
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in (
            exp, issued_at, not_before
        )):
            raise JwtValidationError("invalid_time_claim")
        if exp <= now - self.policy.leeway_seconds:
            raise JwtValidationError("expired_token")
        if issued_at > now + self.policy.leeway_seconds or not_before > now + self.policy.leeway_seconds:
            raise JwtValidationError("token_not_yet_valid")
        if exp <= issued_at or exp - issued_at > self.policy.max_lifetime_seconds:
            raise JwtValidationError("invalid_token_lifetime")

        subject = claims["sub"]
        client_id = claims["client_id"]
        token_id = claims["jti"]
        if any(not isinstance(value, str) or not 1 <= len(value) <= 200 for value in (
            subject, client_id, token_id
        )):
            raise JwtValidationError("invalid_identity_claim")
        if subject != client_id:
            raise JwtValidationError("subject_client_mismatch")
        if self.policy.required_scope not in _scopes(claims["scope"]):
            raise JwtValidationError("insufficient_scope")
        return VerifiedJwt(subject=subject, client_id=client_id, token_id=token_id, claims=claims)


def parse_key_set(raw: str) -> dict[str, str]:
    if not raw.strip():
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PublicAccessConfigurationError("JWT keys must be valid JSON") from exc
    if not isinstance(value, dict):
        raise PublicAccessConfigurationError("JWT keys must be a JSON object")
    return {str(kid): str(secret) for kid, secret in value.items()}


def compile_allowlist(templates: Sequence[str]) -> tuple[re.Pattern[str], ...]:
    if not 1 <= len(templates) <= 100 or len(set(templates)) != len(templates):
        raise PublicAccessConfigurationError("public allowlist must be non-empty and unique")
    compiled = []
    for template in templates:
        if (
            not isinstance(template, str)
            or not _SAFE_PATH.fullmatch(template)
            or template.startswith("/v1/admin/")
            or template.startswith("/v1/auth/")
            or "//" in template
        ):
            raise PublicAccessConfigurationError("public allowlist path is invalid")
        cursor = 0
        expression = ""
        for match in _TEMPLATE_PARAMETER.finditer(template):
            expression += re.escape(template[cursor:match.start()]) + r"[^/]+"
            cursor = match.end()
        expression += re.escape(template[cursor:])
        compiled.append(re.compile(f"^{expression}$"))
    return tuple(compiled)


@dataclass(frozen=True)
class PublicAccessConfig:
    enabled: bool
    allowlist: tuple[str, ...]
    access_token_policy: JwtPolicy
    marketcow_token_policy: JwtPolicy
    rate_limit_requests: int = 60
    rate_limit_window_seconds: int = 60

    def validate(self) -> None:
        if not self.enabled:
            return
        compile_allowlist(self.allowlist)
        self.access_token_policy.validate()
        self.marketcow_token_policy.validate()
        if not 1 <= self.rate_limit_requests <= 100000:
            raise PublicAccessConfigurationError("public rate limit request count is invalid")
        if not 1 <= self.rate_limit_window_seconds <= 3600:
            raise PublicAccessConfigurationError("public rate limit window is invalid")


class FixedWindowRateLimiter:
    def __init__(self, limit: int, window_seconds: int, clock: Callable[[], float] | None = None) -> None:
        self.limit = limit
        self.window_seconds = window_seconds
        self.clock = clock or time.time
        self._windows: dict[str, tuple[int, int]] = {}
        self._lock = threading.Lock()

    def consume(self, key: str) -> tuple[bool, int, int]:
        now = int(self.clock())
        window = now // self.window_seconds
        with self._lock:
            current_window, count = self._windows.get(key, (window, 0))
            if current_window != window:
                current_window, count = window, 0
            count += 1
            self._windows[key] = (current_window, count)
            if len(self._windows) > 10000:
                self._windows = {
                    identity: value for identity, value in self._windows.items()
                    if value[0] >= window - 1
                }
        reset = (window + 1) * self.window_seconds
        return count <= self.limit, max(0, self.limit - count), reset


class PublicAuditLogger:
    """Append-only JSONL audit. Events contain no headers, query strings, or tokens."""

    def __init__(self, path: Path | None, clock: Callable[[], float] | None = None) -> None:
        self.path = path
        self.clock = clock or time.time
        self._events: deque[dict[str, Any]] = deque(maxlen=500)
        self._lock = threading.Lock()

    def append(
        self, *, caller: str, method: str, path: str, status: int,
        result: str, trace_id: str,
    ) -> None:
        event = {
            "schema": "marketcow.public-access-audit.v1",
            "occurred_at": datetime.fromtimestamp(self.clock(), timezone.utc).isoformat(),
            "caller": caller[:200] if caller else "unknown",
            "method": method,
            "path": path[:500],
            "status": int(status),
            "result": result[:120],
            "trace_id": trace_id,
        }
        encoded = json.dumps(event, ensure_ascii=True, separators=(",", ":")) + "\n"
        with self._lock:
            self._events.append(event)
            if self.path is not None:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                descriptor = os.open(self.path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
                try:
                    os.write(descriptor, encoded.encode("utf-8"))
                finally:
                    os.close(descriptor)

    def events(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._events)


class PublicReadOnlyMiddleware:
    def __init__(
        self, app: Any, config: PublicAccessConfig, audit: PublicAuditLogger,
        clock: Callable[[], float] | None = None,
    ) -> None:
        config.validate()
        self.app = app
        self.config = config
        self.audit = audit
        self.clock = clock or time.time
        self.allowlist = compile_allowlist(config.allowlist) if config.enabled else ()
        self.access_tokens = JwtVerifier(config.access_token_policy, self.clock) if config.enabled else None
        self.marketcow_tokens = JwtVerifier(config.marketcow_token_policy, self.clock) if config.enabled else None
        self.rate_limiter = FixedWindowRateLimiter(
            config.rate_limit_requests, config.rate_limit_window_seconds, self.clock
        )

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http" or not scope.get("path", "").startswith(PUBLIC_PREFIX + "/"):
            await self.app(scope, receive, send)
            return
        trace_id = self._trace_id(scope)
        public_path = scope.get("path", "")
        method = scope.get("method", "").upper()
        internal_path = public_path[len(PUBLIC_PREFIX):]
        caller = "unknown"

        async def reject(status: int, code: str, extra_headers: Mapping[str, str] | None = None) -> None:
            self._audit(caller, method, internal_path, status, code, trace_id)
            headers = {"X-Request-ID": trace_id, **(extra_headers or {})}
            if status == 401:
                headers["WWW-Authenticate"] = 'Bearer realm="marketcow-public", error="invalid_token"'
                headers["Cache-Control"] = "no-store"
            await JSONResponse(
                {"detail": {"code": code, "request_id": trace_id}},
                status_code=status,
                headers=headers,
            )(scope, receive, send)

        if not self.config.enabled:
            await reject(404, "public_access_disabled")
            return
        if method not in READ_METHODS:
            await reject(405, "read_only_method_required", {"Allow": "GET, HEAD"})
            return
        if not any(pattern.fullmatch(internal_path) for pattern in self.allowlist):
            await reject(403, "route_not_allowlisted")
            return
        headers = self._headers(scope)
        authorization = headers.get("authorization", "")
        if not authorization.startswith("Bearer ") or not authorization[7:]:
            await reject(401, "missing_access_token")
            return
        try:
            access_identity = self.access_tokens.verify(authorization[7:])  # type: ignore[union-attr]
        except JwtValidationError as exc:
            code = "expired_access_token" if exc.code == "expired_token" else "invalid_access_token"
            await reject(401, code)
            return
        caller = access_identity.client_id
        secondary = headers.get("x-marketcow-jwt", "")
        if not secondary:
            await reject(401, "missing_marketcow_jwt")
            return
        try:
            marketcow_identity = self.marketcow_tokens.verify(secondary)  # type: ignore[union-attr]
        except JwtValidationError as exc:
            code = "expired_marketcow_jwt" if exc.code == "expired_token" else "invalid_marketcow_jwt"
            await reject(401, code)
            return
        if marketcow_identity.client_id != caller:
            await reject(403, "token_client_mismatch")
            return
        allowed, remaining, reset = self.rate_limiter.consume(caller)
        rate_headers = {
            "X-RateLimit-Limit": str(self.config.rate_limit_requests),
            "X-RateLimit-Remaining": str(remaining),
            "X-RateLimit-Reset": str(reset),
        }
        if not allowed:
            retry_after = max(1, reset - int(self.clock()))
            await reject(429, "rate_limit_exceeded", {**rate_headers, "Retry-After": str(retry_after)})
            return

        status = 500

        async def traced_send(message: dict[str, Any]) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message["status"])
                response_headers = list(message.get("headers", []))
                response_headers.extend(
                    (key.encode("latin-1"), value.encode("latin-1"))
                    for key, value in {"X-Request-ID": trace_id, **rate_headers}.items()
                )
                message["headers"] = response_headers
            await send(message)

        rewritten = dict(scope)
        rewritten["path"] = internal_path
        rewritten["raw_path"] = internal_path.encode("ascii")
        rewritten["public_identity"] = access_identity
        try:
            await self.app(rewritten, receive, traced_send)
        finally:
            self._audit(caller, method, internal_path, status, "allowed", trace_id)

    def _audit(self, caller: str, method: str, path: str, status: int, result: str, trace_id: str) -> None:
        self.audit.append(
            caller=caller, method=method, path=path, status=status,
            result=result, trace_id=trace_id,
        )

    @staticmethod
    def _headers(scope: Mapping[str, Any]) -> dict[str, str]:
        return {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", ())
        }

    @classmethod
    def _trace_id(cls, scope: Mapping[str, Any]) -> str:
        supplied = cls._headers(scope).get("x-request-id", "")
        return supplied if _TRACE_ID.fullmatch(supplied) else uuid.uuid4().hex
