"""Pure phase-1 catalog/selection admission contracts.

This module deliberately contains no HTTP routes or activation side effects.  It
is shared by the eventual catalog read and admission gateways so validation and
canonical hashing cannot drift between transports.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


SCHEMA_VERSION = "tradude.marketcow.discovery-selection-request.v1"
ERROR_SCHEMA_VERSION = "marketcow.catalog-selection-error.v1"
_REQUEST_ID = re.compile(r"^(\d{13}):([0-9a-f]{32})$")


class RequestValidationError(ValueError):
    pass


def canonical_json_bytes(value: Any) -> bytes:
    """Canonical JSON used by selection_sha256 (no whitespace or float values)."""
    def check(item):
        if item is None or type(item) in (bool, int):
            return
        if type(item) is str and item.isascii():
            return
        if type(item) is list:
            for child in item:
                check(child)
            return
        if type(item) is dict and all(type(key) is str and key.isascii() for key in item):
            for child in item.values():
                check(child)
            return
        raise ValueError("canonical selection requires ASCII JSON without floats")
    check(value)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                      sort_keys=True, allow_nan=False).encode("utf-8")


def parse_request_bytes(body: bytes, *, maximum_bytes: int) -> "DiscoverySelectionRequest":
    if type(maximum_bytes) is not int or maximum_bytes <= 0 or len(body) > maximum_bytes:
        raise ValueError("request byte budget exceeded")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError("nonfinite JSON constant")

    payload = json.loads(body.decode("utf-8", errors="strict"),
                         object_pairs_hook=pairs, parse_constant=invalid_constant)
    canonical_json_bytes(payload)
    if isinstance(payload, dict) and isinstance(payload.get("selection"), dict):
        selection = payload["selection"]
        ids = selection.get("market_ids")
        if isinstance(ids, list) and all(isinstance(mid, str) for mid in ids) and len(ids) != len(set(ids)):
            raise RequestValidationError("duplicate_market_id")
        if payload.get("selection_sha256") != selection_sha256(selection):
            raise RequestValidationError("hash_mismatch")
    return DiscoverySelectionRequest.model_validate(payload)


def selection_sha256(selection: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(selection)).hexdigest()


def _parse_millis(value: str) -> int:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", value):
        raise ValueError("timestamps must use UTC millisecond RFC3339")
    parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    delta = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000


class ProtectedMarket(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    market_id: str = Field(min_length=1, pattern=r"^[\x21-\x7e]+$")
    reasons: list[str]

    @model_validator(mode="after")
    def validate_reasons(self) -> "ProtectedMarket":
        if not self.reasons or len(set(self.reasons)) != len(self.reasons):
            raise ValueError("protected reasons must be non-empty and unique")
        if self.reasons != sorted(self.reasons):
            raise ValueError("protected reasons must be sorted")
        if any(not reason or not reason.isascii() for reason in self.reasons):
            raise ValueError("protected reasons must be nonempty ASCII")
        return self


class DiscoverySelection(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    catalog_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    market_ids: list[str]
    tradude_policy_version: str = Field(min_length=1)
    expected_active_selection_id: str | None
    protected_markets: list[ProtectedMarket]
    resource_profile_id: str = Field(min_length=1)
    resource_profile_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_ids(self) -> "DiscoverySelection":
        if not self.market_ids or len(set(self.market_ids)) != len(self.market_ids):
            raise ValueError("market_ids must be non-empty and unique")
        if self.market_ids != sorted(self.market_ids):
            raise ValueError("market_ids must be lexicographically sorted")
        if any(not mid or not mid.isascii() for mid in self.market_ids):
            raise ValueError("market IDs must be nonempty ASCII")
        if self.expected_active_selection_id == "":
            raise ValueError("incumbent identity must be nonempty or null")
        protected = [item.market_id for item in self.protected_markets]
        if protected != sorted(protected) or len(set(protected)) != len(protected):
            raise ValueError("protected_markets must be sorted and unique")
        if not set(protected).issubset(self.market_ids):
            raise ValueError("protected market is outside selection")
        return self


class DiscoverySelectionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: Literal["tradude.marketcow.discovery-selection-request.v1"]
    request_id: str
    created_at: str
    expires_at: str
    selection: DiscoverySelection
    selection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_identity(self) -> "DiscoverySelectionRequest":
        match = _REQUEST_ID.fullmatch(self.request_id)
        if not match:
            raise ValueError("request_id must be <unix_ms>:<32 lowercase hex nonce>")
        created_ms = _parse_millis(self.created_at)
        expires_ms = _parse_millis(self.expires_at)
        if created_ms != int(match.group(1)):
            raise ValueError("request_id timestamp does not match created_at")
        if expires_ms <= created_ms:
            raise ValueError("expires_at must be after created_at")
        if selection_sha256(self.selection.model_dump(mode="json")) != self.selection_sha256:
            raise ValueError("selection_sha256 mismatch")
        return self

    def request_digest(self) -> str:
        payload = self.model_dump(mode="json")
        return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class AdmissionError(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: str = ERROR_SCHEMA_VERSION
    code: str
    retryable: bool
    request_id: str | None
    details: dict[str, Any]
