from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Any, Dict


CURSOR_VERSION = 1


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
