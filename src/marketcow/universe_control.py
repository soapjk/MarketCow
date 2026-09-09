"""Bounded phase-1 control-plane application, independent of realtime services.

No activation, collector, account or trading API is imported. CatalogSource must
be a verified immutable generation of phase-1 records, not a mutable live view.
The HTTP adapter is deliberately not wired into any existing service launcher.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol

from marketcow.universe_admission_store import AdmissionStore, StoreError, StoreLimits
from marketcow.universe_phase1 import DiscoverySelectionRequest, selection_sha256


def wire_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False,
                      sort_keys=True, separators=(",", ":")).encode("utf-8")


def instant(ms: int) -> str:
    seconds, millis = divmod(ms, 1000)
    return datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + f".{millis:03d}Z"


class ControlError(ValueError):
    def __init__(self, code: str, status: int, *, retryable: bool = False, details: dict | None = None):
        self.code, self.status, self.retryable = code, status, retryable
        self.details = {} if details is None else details
        super().__init__(code)


class CatalogSource(Protocol):
    revision: str
    count: int
    source: dict
    coverage: dict

    def after(self, market_id: str | None) -> Iterable[dict]: ...
    def get(self, market_id: str) -> dict | None: ...


@dataclass(frozen=True)
class Snapshot:
    source: CatalogSource
    page_size: int
    expires_ms: int
    deadline: float


class UniverseControl:
    def __init__(self, source: CatalogSource, profile: dict, store_path: Path, *,
                 expected_active_selection_id: str | None,
                 incumbent_guard=None, incumbent_provider=None, catalog_provider=None,
                 wall_ms=lambda: time.time_ns() // 1_000_000,
                 monotonic=time.monotonic):
        # Profile values are mandatory; no hidden capacity defaults.
        if (set(profile) != {"schema_version", "profile_id", "catalog", "admission"}
                or profile["schema_version"] != "marketcow.polymarket.phase1-resource-profile.v1"
                or not isinstance(profile["profile_id"], str) or not profile["profile_id"].isascii()
                or not profile["profile_id"]):
            raise ValueError("invalid resource profile schema")
        if expected_active_selection_id is not None and (
            type(expected_active_selection_id) is not str or not expected_active_selection_id
        ):
            raise ValueError("explicit incumbent identity required")
        for group, names in {
            "catalog": ("max_page_records", "max_response_bytes", "snapshot_ttl_seconds",
                        "retained_snapshots", "concurrent_readers"),
            "admission": ("admission_ttl_seconds", "idempotency_retention_seconds", "max_dependency_markets",
                          "max_idempotency_entries", "max_request_bytes", "max_response_bytes",
                          "max_requested_markets", "max_total_tokens"),
        }.items():
            if set(profile[group]) != set(names) or any(
                type(profile[group][name]) is not int or profile[group][name] <= 0 for name in names
            ):
                raise ValueError("invalid explicit resource profile")
        self.profile = json.loads(wire_bytes(profile))
        self.profile_hash = selection_sha256(self.profile)
        a = self.profile["admission"]
        self.store_limits = StoreLimits(a["admission_ttl_seconds"]*1000,
                                       a["idempotency_retention_seconds"]*1000,
                                       a["max_idempotency_entries"], a["max_response_bytes"])
        self.source, self.store_path = source, store_path
        self.incumbent = expected_active_selection_id
        self.incumbent_guard = incumbent_guard
        self.incumbent_provider = incumbent_provider
        self.catalog_provider = catalog_provider
        self.wall_ms, self.monotonic = wall_ms, monotonic
        self.lock = threading.RLock()
        self.slots = threading.BoundedSemaphore(self.profile["catalog"]["concurrent_readers"])
        self.snapshots: dict[str, Snapshot] = {}
        self.token_key = secrets.token_bytes(32)

    def _bounded(self, payload: dict, maximum: int) -> bytes:
        body = wire_bytes(payload)
        if len(body) > maximum:
            raise ControlError("response_size_exceeded", 413)
        return body

    def _token(self, sid: str, after: str | None, size: int) -> str:
        raw = wire_bytes([sid, after, size])
        return base64.urlsafe_b64encode(raw).decode().rstrip("=") + "." + hmac.new(
            self.token_key, raw, hashlib.sha256).hexdigest()

    def snapshot(self, page_size: int, *, include_change_sequence: bool = False) -> bytes:
        c = self.profile["catalog"]
        if type(page_size) is not int or not 1 <= page_size <= c["max_page_records"]:
            raise ControlError("invalid_schema", 400)
        with self.lock:
            now, mono = self.wall_ms(), self.monotonic()
            self.snapshots = {key: item for key, item in self.snapshots.items()
                              if item.deadline > mono and item.expires_ms > now}
            if len(self.snapshots) >= c["retained_snapshots"]:
                raise ControlError("resource_unavailable", 429, retryable=True)
            sid = secrets.token_hex(16)
            source = self.catalog_provider.current() if self.catalog_provider else self.source
            grace = getattr(source, 'reader_grace_seconds', None)
            if grace is not None and grace < c['snapshot_ttl_seconds']:
                raise ControlError('catalog_source_invalid', 503)
            expires = now + c["snapshot_ttl_seconds"]*1000
            payload = {
                "schema_version": "marketcow.polymarket.catalog-snapshot.v1",
                "snapshot_id": sid, "catalog_revision": source.revision,
                "captured_at": instant(now), "expires_at": instant(expires), "page_size": page_size,
                "source": source.source, "coverage": source.coverage, "unique_count": source.count,
                "first_page_token": self._token(sid, None, page_size) if source.count else None,
            }
            if include_change_sequence:
                if self.catalog_provider is None:
                    raise ControlError('catalog_changes_unavailable', 503)
                payload.update(schema_version="marketcow.polymarket.catalog-snapshot.v2",
                               change_sequence=source.change_sequence)
            result = self._bounded(payload, c["max_response_bytes"])
            self.snapshots[sid] = Snapshot(source, page_size, expires, mono+c["snapshot_ttl_seconds"])
            return result

    def changes(self, after_sequence: int, limit: int) -> bytes:
        if self.catalog_provider is None:
            raise ControlError('catalog_changes_unavailable', 503)
        if type(limit) is not int or not 1 <= limit <= self.profile['catalog']['max_page_records']:
            raise ControlError('invalid_schema', 400)
        return self.catalog_provider.current().changes(after_sequence, limit, self.profile['catalog']['max_response_bytes'])

    def catalog_status(self) -> bytes:
        if self.catalog_provider is None:
            raise ControlError('catalog_changes_unavailable', 503)
        return self._bounded(self.catalog_provider.status(), self.profile['catalog']['max_response_bytes'])

    def page(self, sid: str, token: str, limit: int) -> bytes:
        if len(token) > 8192:
            raise ControlError("invalid_page_token", 400)
        with self.lock:
            snap = self.snapshots.get(sid)
            if snap is None or snap.expires_ms <= self.wall_ms() or snap.deadline <= self.monotonic():
                raise ControlError("catalog_snapshot_expired", 410)
        try:
            encoded, signature = token.split(".")
            raw = base64.b64decode(encoded + "="*((-len(encoded)) % 4), altchars=b"-_", validate=True)
            if not hmac.compare_digest(hmac.new(self.token_key, raw, hashlib.sha256).hexdigest(), signature):
                raise ValueError("signature")
            tsid, after, size = json.loads(raw)
            if tsid != sid or size != snap.page_size or type(limit) is not int or limit != size:
                raise ValueError("binding")
        except (ValueError, TypeError):
            raise ControlError("invalid_page_token", 400) from None
        if not self.slots.acquire(blocking=False):
            raise ControlError("resource_unavailable", 429, retryable=True)
        try:
            result = {"schema_version": "marketcow.polymarket.catalog-page.v1", "snapshot_id": sid,
                      "catalog_revision": snap.source.revision, "page_token": token, "records": [],
                      "next_page_token": None, "end_of_snapshot": True}
            last = after
            iterator = iter(snap.source.after(after))
            try:
                for record in iterator:
                    mid = record["market_id"]
                    if last is not None and mid <= last:
                        raise ControlError("catalog_source_invalid", 503)
                    # Reserve the actual next token bytes, never only record bytes.
                    candidate = dict(result, records=result["records"] + [record],
                                     next_page_token=self._token(sid, mid, size), end_of_snapshot=False)
                    if len(result["records"]) == size or len(wire_bytes(candidate)) > self.profile["catalog"]["max_response_bytes"]:
                        if not result["records"]:
                            raise ControlError("response_size_exceeded", 413)
                        result.update(next_page_token=self._token(sid, last, size), end_of_snapshot=False)
                        break
                    result["records"].append(record)
                    last = mid
            finally:
                close = getattr(iterator, "close", None)
                if close:
                    close()
            if snap.expires_ms <= self.wall_ms() or snap.deadline <= self.monotonic():
                raise ControlError("catalog_snapshot_expired", 410)
            return self._bounded(result, self.profile["catalog"]["max_response_bytes"])
        finally:
            self.slots.release()

    def admit(self, caller: str, request: DiscoverySelectionRequest) -> bytes:
        db = AdmissionStore(self.store_path, self.store_limits)
        try:
            cached = db.record(caller, request, None, now_ms=self.wall_ms())
            if cached is not None:
                return self._stored_response(cached)
            try:
                body = self._evaluate(request)
            except ControlError as error:
                # Store permanent business rejection too; same ID must never
                # change from rejection to admission after the catalog changes.
                if error.retryable or error.status >= 500:
                    raise
                body = wire_bytes({"schema_version": "marketcow.catalog-selection-error.v1",
                                   "code": error.code, "retryable": False,
                                   "request_id": request.request_id, "details": error.details})
            return self._stored_response(db.record(caller, request, body, now_ms=self.wall_ms()))
        except StoreError as error:
            code = str(error)
            status = {"clock_regression": 503, "request_expired": 410,
                      "idempotency_conflict": 409, "resource_unavailable": 429,
                      "store_corruption": 503, "response_size_exceeded": 413}.get(code, 400)
            raise ControlError(code, status, retryable=code == "resource_unavailable") from error
        finally:
            db.close()

    @staticmethod
    def _stored_response(body: bytes) -> bytes:
        value = json.loads(body)
        if value["schema_version"] == "marketcow.catalog-selection-error.v1":
            code = value["code"]
            status = {"resource_profile_mismatch": 409, "catalog_revision_mismatch": 409,
                      "incumbent_conflict": 409, "capacity_exceeded": 422,
                      "unknown_market_id": 422, "response_size_exceeded": 413}[code]
            raise ControlError(code, status, details=value["details"])
        return body

    def _evaluate(self, request: DiscoverySelectionRequest) -> bytes:
        s = request.selection
        with self.lock:
            source = self.catalog_provider.current() if self.catalog_provider else self.source
            incumbent = self.incumbent
        if self.incumbent_provider is not None:
            try:
                incumbent = self.incumbent_provider()
                if not isinstance(incumbent, str) or not incumbent:
                    raise ValueError("unbound runtime incumbent")
            except (ValueError, OSError, RuntimeError, KeyError):
                raise ControlError("runtime_state_requires_reconciliation", 503) from None
        elif self.incumbent_guard is not None:
            try:
                self.incumbent_guard()
            except (ValueError, OSError, KeyError):
                raise ControlError("incumbent_conflict", 409) from None
        if s.resource_profile_id != self.profile["profile_id"] or s.resource_profile_sha256 != self.profile_hash:
            raise ControlError("resource_profile_mismatch", 409)
        if s.catalog_revision != source.revision:
            raise ControlError("catalog_revision_mismatch", 409)
        if s.expected_active_selection_id != incumbent:
            raise ControlError("incumbent_conflict", 409)
        a = self.profile["admission"]
        if len(s.market_ids) > a["max_requested_markets"]:
            raise ControlError("capacity_exceeded", 422)
        requested = set(s.market_ids)
        visited, tokens, missing = set(), set(), set()
        pending = set(s.market_ids)
        while pending:
            mid = pending.pop()
            if mid in visited:
                continue
            visited.add(mid)
            record = source.get(mid)
            if record is None:
                missing.add(mid)
                continue
            tokens.update(item["token_id"] for item in record["outcomes"])
            if len(tokens) > a["max_total_tokens"]:
                raise ControlError("capacity_exceeded", 422)
            for relation in record["relations"]:
                for member in relation["member_market_ids"]:
                    if member not in visited:
                        pending.add(member)
                # Bound traversal including queued identities, not just visited.
                if len((visited | pending) - requested) > a["max_dependency_markets"]:
                    raise ControlError("capacity_exceeded", 422)
        if missing:
            raise ControlError("unknown_market_id", 422, details={
                "missing_market_ids": sorted(missing),
                "market_results": [{"market_id": mid, "status": "rejected" if mid in missing else "accepted",
                                    "reason_codes": ["unknown_market_id"] if mid in missing else []}
                                   for mid in s.market_ids],
            })
        dependencies = sorted(visited-requested)
        requested_tokens = {item["token_id"] for mid in s.market_ids for item in source.get(mid)["outcomes"]}
        return self._bounded({
            "schema_version": "marketcow.polymarket.discovery-admission.v1",
            "request_id": request.request_id, "selection_sha256": request.selection_sha256,
            "admission_id": hashlib.sha256(("admission:"+request.request_digest()).encode()).hexdigest(),
            "catalog_revision": source.revision, "status": "admitted", "requested_market_ids": s.market_ids,
            "dependency_market_ids": dependencies, "dependency_token_ids": sorted(tokens-requested_tokens),
            "market_results": [{"market_id": mid, "status": "accepted", "reason_codes": []} for mid in s.market_ids],
            "protected_conflicts": [], "capacity_estimate": {"requested_markets": len(requested),
                "dependency_markets": len(dependencies), "total_tokens": len(tokens), "estimated_bytes": None},
            "resource_profile_id": s.resource_profile_id, "resource_profile_sha256": s.resource_profile_sha256,
            "expires_at": request.expires_at,
        }, a["max_response_bytes"])

    def admitted_for_preparation(self, caller: str, request: DiscoverySelectionRequest,
                                response_sha256: str) -> dict:
        """Resolve only a previously admitted, same-caller, unexpired request.

        Never evaluates a missing admission or installs a generation. This is
        the trusted boundary for a separate bounded candidate-preparation worker.
        Caller-supplied IDs and purported receipt JSON cannot self-certify it.
        """
        if not isinstance(response_sha256, str) or len(response_sha256) != 64:
            raise ControlError("hash_mismatch", 400)
        db = AdmissionStore(self.store_path, self.store_limits)
        try:
            body = db.record(caller, request, None, now_ms=self.wall_ms())
            if body is None:
                raise ControlError("admission_not_found", 409)
            self._stored_response(body)
            if hashlib.sha256(body).hexdigest() != response_sha256:
                raise ControlError("hash_mismatch", 400)
            value = json.loads(body)
            if (value.get("status") != "admitted" or value["selection_sha256"] != request.selection_sha256
                    or value["requested_market_ids"] != request.selection.market_ids):
                raise ControlError("admission_binding_mismatch", 409)
            # Re-check structural/current binding without generating another
            # admission or changing the original response/expiry.
            current = json.loads(self._evaluate(request))
            for field in ("catalog_revision", "resource_profile_id", "resource_profile_sha256",
                          "requested_market_ids", "dependency_market_ids", "dependency_token_ids"):
                if current[field] != value[field]:
                    raise ControlError("admission_binding_mismatch", 409)
            return value
        except StoreError as error:
            code = str(error)
            status = {"clock_regression": 503, "request_expired": 410, "idempotency_conflict": 409}.get(code, 503)
            raise ControlError(code, status) from error
        finally:
            db.close()
