from __future__ import annotations

import base64
import fcntl
import hashlib
import hmac
import json
import os
import secrets
import tempfile
from pathlib import Path
from typing import Any, Dict


CURSOR_VERSION = 1
MAX_CURSOR_LENGTH = 2048
REJECTED_SECRETS = {
    "marketcow-local-cursor-secret",
    "replace-with-a-local-development-secret",
    "change-me",
    "changeme",
}


def validate_explicit_secret(secret: str) -> str:
    value = secret.strip()
    if value.lower() in REJECTED_SECRETS or "placeholder" in value.lower():
        raise ValueError("market bar cursor secret must not use a known placeholder")
    if len(value.encode("utf-8")) < 32:
        raise ValueError("market bar cursor secret must contain at least 32 bytes")
    return value


def load_or_create_secret(explicit: str, storage_root: Path) -> str:
    if explicit.strip():
        return validate_explicit_secret(explicit)
    root = storage_root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    key_path = root / ".market-bar-cursor.key"
    if key_path.resolve(strict=False) != key_path:
        raise ValueError("market bar cursor key must stay within storage root")
    lock_path = root / ".market-bar-cursor.lock"
    try:
        with lock_path.open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            if key_path.exists():
                return validate_explicit_secret(key_path.read_text(encoding="utf-8"))
            generated = secrets.token_urlsafe(32)
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", dir=root, prefix=".cursor-key-", delete=False
            ) as temporary:
                temporary.write(generated)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            os.chmod(temporary_path, 0o600)
            os.replace(temporary_path, key_path)
            directory = os.open(root, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            return validate_explicit_secret(generated)
    except OSError as error:
        raise ValueError("market bar cursor key is unavailable") from error


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def encode_cursor(query: Dict[str, Any], after: int, issued_at: int, secret: str) -> str:
    payload = json.dumps(
        {"v": CURSOR_VERSION, "q": query, "after": after, "iat": issued_at},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).digest()
    return _encode(payload) + "." + _encode(signature)


def decode_cursor(
    token: str, query: Dict[str, Any], now: int, ttl_seconds: int, secret: str,
) -> int:
    if not token or len(token) > MAX_CURSOR_LENGTH:
        raise ValueError("invalid cursor length")
    try:
        encoded_payload, encoded_signature = token.split(".", 1)
        payload = _decode(encoded_payload)
        signature = _decode(encoded_signature)
        expected = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).digest()
        if not hmac.compare_digest(signature, expected):
            raise ValueError("cursor integrity check failed")
        decoded = json.loads(payload)
    except ValueError:
        raise
    except Exception as error:
        raise ValueError("invalid cursor") from error
    if not isinstance(decoded, dict):
        raise ValueError("invalid cursor payload")
    if decoded.get("v") != CURSOR_VERSION:
        raise ValueError("unsupported cursor version")
    if decoded.get("q") != query:
        raise ValueError("cursor does not match this query")
    issued_at = decoded.get("iat")
    after = decoded.get("after")
    if not isinstance(issued_at, int) or not isinstance(after, int):
        raise ValueError("invalid cursor payload")
    if issued_at > now + 30:
        raise ValueError("cursor issued_at is in the future")
    if now - issued_at > ttl_seconds:
        raise ValueError("cursor has expired")
    return after
