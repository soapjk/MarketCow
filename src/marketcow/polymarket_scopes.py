from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable

from .polymarket_contracts import canonical_json


class ScopeTransitionError(RuntimeError):
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def scope_content_id(manifest: dict[str, Any]) -> str:
    content = dict(manifest)
    content.pop("scope_id", None)
    return hashlib.sha256(canonical_json(content)).hexdigest()


def _atomic_create(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != body:
            raise ScopeTransitionError(
                "polymarket_scope_immutable",
                f"Immutable scope artifact already differs: {path}",
            )
        return
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _atomic_replace(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = canonical_json(document)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


def write_scope_runtime(
    root: Path, *, scope_id: str, manifest_sha256: str,
) -> dict[str, Any]:
    if not all(
        len(value) == 64 and all(character in "0123456789abcdef" for character in value)
        for value in (scope_id, manifest_sha256)
    ):
        raise ValueError("scope runtime identifiers must be lowercase SHA-256 values")
    descriptor = {
        "schema_version": "marketcow.polymarket.scope-runtime.v1",
        "scope_id": scope_id,
        "manifest_sha256": manifest_sha256,
        "real_order_submission_enabled": False,
    }
    _atomic_create(root.resolve() / "scope-runtime.json", canonical_json(descriptor))
    return descriptor


def build_replacement_scope_manifest(
    current_manifest: dict[str, Any],
    candidate_snapshot: dict[str, Any],
    *,
    generated_at_ns: int,
) -> dict[str, Any]:
    """Deterministically keep eligible members and fill vacancies to exact 100."""
    if candidate_snapshot.get("schema") != "tradude.prediction_market.scope_candidates.v1":
        raise ScopeTransitionError(
            "polymarket_candidate_snapshot_invalid", "unsupported candidate snapshot schema",
        )
    candidates = []
    for row in candidate_snapshot.get("markets") or []:
        if not isinstance(row, dict):
            continue
        if not all((
            row.get("active") is True,
            row.get("accepting_orders") is True,
            row.get("closed") is False,
            row.get("binary_yes_no") is True,
            row.get("rules_complete") is True,
            isinstance(row.get("market_id"), str),
            isinstance(row.get("end_at_ns"), int),
            row.get("end_at_ns") > generated_at_ns,
            isinstance(row.get("yes_outcome_id"), str),
            isinstance(row.get("no_outcome_id"), str),
            row.get("yes_outcome_id") != row.get("no_outcome_id"),
        )):
            continue
        try:
            liquidity = Decimal(str(row.get("liquidity_num") or "0"))
        except InvalidOperation:
            continue
        candidates.append((row, liquidity))
    by_id = {row["market_id"]: (row, liquidity) for row, liquidity in candidates}
    retained = [
        str(market_id) for market_id in current_manifest.get("market_ids") or []
        if str(market_id) in by_id
    ]
    replacements = sorted(
        (
            (row, liquidity) for row, liquidity in candidates
            if row["market_id"] not in retained
        ),
        key=lambda item: (
            -item[1], -int(item[0]["end_at_ns"]), str(item[0]["market_id"]),
        ),
    )
    market_ids = retained + [
        row["market_id"] for row, _ in replacements[: 100 - len(retained)]
    ]
    if len(market_ids) != 100 or len(set(market_ids)) != 100:
        raise ScopeTransitionError(
            "polymarket_replacement_scope_insufficient",
            "candidate snapshot cannot produce exact 100 eligible markets",
        )
    manifest = {
        "schema": "tradude.prediction_market.scope_manifest.v1",
        "catalog_revision": candidate_snapshot.get("catalog_revision"),
        "candidate_snapshot_id": candidate_snapshot.get("snapshot_id"),
        "generated_at_ns": generated_at_ns,
        "market_ids": market_ids,
        "replaces_scope_id": current_manifest.get("scope_id"),
    }
    manifest["scope_id"] = scope_content_id(manifest)
    return manifest


class PolymarketScopeRegistry:
    """Immutable candidate artifacts plus one atomically replaced active pointer."""

    def __init__(
        self,
        root: Path,
        *,
        now_provider: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.root = root.resolve()
        self.scopes_root = self.root / "scopes"
        self.active_path = self.root / "active-scope.json"
        self.audit_path = self.root / "scope-transitions.jsonl"
        self.now_provider = now_provider

    def prepare(self, manifest: dict[str, Any]) -> dict[str, Any]:
        scope_id = str(manifest.get("scope_id") or "")
        if scope_id != scope_content_id(manifest):
            raise ScopeTransitionError(
                "polymarket_scope_content_mismatch",
                "scope_id does not match canonical manifest contents",
            )
        market_ids = [str(item) for item in manifest.get("market_ids") or []]
        if len(market_ids) != 100 or len(set(market_ids)) != 100:
            raise ScopeTransitionError(
                "polymarket_scope_not_exact",
                "candidate scope must contain exactly 100 unique markets",
            )
        body = canonical_json(manifest)
        path = self.scopes_root / scope_id / "manifest.json"
        _atomic_create(path, body)
        descriptor = {
            "schema_version": "marketcow.polymarket.scope-candidate.v1",
            "scope_id": scope_id,
            "manifest_sha256": hashlib.sha256(body).hexdigest(),
            "market_count": 100,
            "real_order_submission_enabled": False,
        }
        _atomic_create(
            self.scopes_root / scope_id / "candidate.json",
            canonical_json(descriptor),
        )
        return descriptor

    @staticmethod
    def _validate_endpoint_observation(observation: dict[str, Any]) -> None:
        expected = {
            "http_status": 200,
            "status": "index_ready",
            "market_count": 100,
            "book_count": 200,
            "complete_market_count": 100,
            "tick_consistent_token_count": 200,
            "gap_count": 0,
            "disconnect_count": 0,
        }
        mismatches = {
            key: (value, observation.get(key))
            for key, value in expected.items()
            if observation.get(key) != value
        }
        cursors = observation.get("cursors") or []
        if (
            mismatches
            or len(cursors) < 2
            or any(not isinstance(item, int) for item in cursors)
            or cursors[-1] <= cursors[0]
        ):
            raise ScopeTransitionError(
                "polymarket_candidate_acceptance_failed",
                f"candidate endpoint acceptance failed: {mismatches or 'cursor_not_advancing'}",
            )

    def accept(self, scope_id: str, observations: dict[str, Any]) -> dict[str, Any]:
        candidate_path = self.scopes_root / scope_id / "candidate.json"
        if not candidate_path.is_file():
            raise ScopeTransitionError(
                "polymarket_scope_candidate_not_found", "candidate is not prepared",
            )
        evidence = observations.get("activation_evidence") or observations
        endpoints = evidence.get("endpoints") or {}
        if set(endpoints) != {"8790", "8791"}:
            raise ScopeTransitionError(
                "polymarket_candidate_acceptance_failed",
                "candidate acceptance requires independent 8790 and 8791 evidence",
            )
        for observation in endpoints.values():
            self._validate_endpoint_observation(observation)
        if evidence.get("real_order_submission_enabled") is not False:
            raise ScopeTransitionError(
                "polymarket_real_orders_not_disabled",
                "candidate acceptance requires real order submission disabled",
            )
        accepted = {
            "schema_version": "marketcow.polymarket.scope-acceptance.v1",
            "scope_id": scope_id,
            "accepted_at": self.now_provider().isoformat(),
            "observations": observations,
            "evidence_sha256": hashlib.sha256(
                canonical_json(observations)
            ).hexdigest(),
        }
        _atomic_create(
            self.scopes_root / scope_id / "acceptance.json",
            canonical_json(accepted),
        )
        return accepted

    def active(self) -> dict[str, Any] | None:
        if not self.active_path.is_file():
            return None
        return json.loads(self.active_path.read_text(encoding="utf-8"))

    def activate(self, scope_id: str, *, grace_seconds: int = 300) -> dict[str, Any]:
        if grace_seconds < 0:
            raise ValueError("grace_seconds cannot be negative")
        if not (self.scopes_root / scope_id / "acceptance.json").is_file():
            raise ScopeTransitionError(
                "polymarket_candidate_not_accepted",
                "candidate must pass both endpoints before activation",
            )
        previous = self.active()
        now = self.now_provider()
        pointer = {
            "schema_version": "marketcow.polymarket.active-scope.v1",
            "active_scope_id": scope_id,
            "previous_scope_id": (
                previous.get("active_scope_id") if previous else None
            ),
            "switched_at": now.isoformat(),
            "previous_grace_until": (
                (now + timedelta(seconds=grace_seconds)).isoformat()
                if previous else None
            ),
            "real_order_submission_enabled": False,
        }
        _atomic_replace(self.active_path, pointer)
        self._audit("activate", pointer)
        return pointer

    def rollback(self, *, grace_seconds: int = 300) -> dict[str, Any]:
        current = self.active()
        target = current.get("previous_scope_id") if current else None
        if not target:
            raise ScopeTransitionError(
                "polymarket_scope_rollback_unavailable",
                "active scope has no accepted predecessor",
            )
        return self.activate(str(target), grace_seconds=grace_seconds)

    def resolve(self, scope_id: str, market_id: str) -> dict[str, Any]:
        active = self.active()
        if active is None:
            raise ScopeTransitionError(
                "polymarket_scope_not_configured", "active scope is not configured",
            )
        status = "active"
        if scope_id != active["active_scope_id"]:
            if scope_id != active.get("previous_scope_id"):
                raise ScopeTransitionError(
                    "polymarket_scope_not_found", "scope_id is unknown",
                )
            grace_until = datetime.fromisoformat(active["previous_grace_until"])
            if self.now_provider() > grace_until:
                raise ScopeTransitionError(
                    "polymarket_scope_retired",
                    "scope grace period elapsed; discover active_scope_id and re-bootstrap",
                )
            status = "grace"
        manifest = json.loads(
            (self.scopes_root / scope_id / "manifest.json").read_text(encoding="utf-8")
        )
        if market_id not in manifest["market_ids"]:
            raise ScopeTransitionError(
                "polymarket_market_outside_scope",
                "market_id is not a member of the addressed immutable scope",
            )
        return {
            "scope_id": scope_id,
            "scope_status": status,
            "active_scope_id": active["active_scope_id"],
            "market_id": market_id,
        }

    def _audit(self, action: str, payload: dict[str, Any]) -> None:
        record = canonical_json({
            "action": action,
            "at": self.now_provider().isoformat(),
            "payload": payload,
            "payload_sha256": hashlib.sha256(canonical_json(payload)).hexdigest(),
        }) + b"\n"
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        with self.audit_path.open("ab") as stream:
            stream.write(record)
            stream.flush()
            os.fsync(stream.fileno())
