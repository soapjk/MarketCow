#!/usr/bin/env python3
"""Warm and activate one fail-closed Tradude-owned exact Scope selection.

This command is intentionally one-shot. A service manager or scheduler may invoke it periodically;
an advisory lock prevents overlapping refreshes. Tradude owns membership selection; MarketCow owns
data validation, warmup, and atomic publication.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import requests

from scripts.migration.build_polymarket_dynamic_universe import build_dynamic_universe
from scripts.migration.build_polymarket_rust_scope import write_atomic_json
from scripts.migration.fetch_polymarket_dynamic_candidate_books import (
    build_candidate_book_snapshot,
)


SCHEMA_VERSION = "marketcow.polymarket.universe-auto-refresh.v2"
CONFIG_SCHEMA_VERSION = "marketcow.polymarket.universe-auto-refresh-config.v3"
ACTIVATION_SCHEMA_VERSION = "marketcow.polymarket.scope-activation.v1"
SCOPE_SCHEMA_VERSION = "marketcow.polymarket.scope-discovery.v5"


@dataclass(frozen=True)
class RefreshConfig:
    service_url: str
    candidate_manifest: Path
    catalog_manifest: Path
    startup_scope: Path
    scope_registry_root: Path
    work_root: Path
    audit_result: Path
    target_market_count: int
    minimum_market_count: int
    clob_books_endpoint: str = "https://clob.polymarket.com/books"
    request_timeout_seconds: float = 60.0
    retry_seconds: int = 60


def _load_config(path: Path) -> RefreshConfig:
    payload = json.loads(path.resolve(strict=True).read_bytes())
    expected = {
        "schema_version", "service_url", "candidate_manifest", "catalog_manifest",
        "startup_scope", "scope_registry_root", "work_root", "audit_result",
        "target_market_count", "minimum_market_count",
        "clob_books_endpoint", "request_timeout_seconds", "retry_seconds",
    }
    if not isinstance(payload, dict) or set(payload) - expected:
        raise ValueError("refresh configuration contains unknown fields")
    if payload.get("schema_version") != CONFIG_SCHEMA_VERSION:
        raise ValueError("refresh configuration schema is unsupported")
    parsed = urlparse(payload.get("service_url", ""))
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("refresh activation is restricted to a loopback HTTP service")
    return RefreshConfig(
        service_url=payload["service_url"].rstrip("/"),
        candidate_manifest=Path(payload["candidate_manifest"]),
        catalog_manifest=Path(payload["catalog_manifest"]),
        startup_scope=Path(payload["startup_scope"]),
        scope_registry_root=Path(payload["scope_registry_root"]),
        work_root=Path(payload["work_root"]),
        audit_result=Path(payload["audit_result"]),
        target_market_count=int(payload["target_market_count"]),
        minimum_market_count=int(payload["minimum_market_count"]),
        clob_books_endpoint=payload.get("clob_books_endpoint", "https://clob.polymarket.com/books"),
        request_timeout_seconds=float(payload.get("request_timeout_seconds", 60)),
        retry_seconds=int(payload.get("retry_seconds", 60)),
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@dataclass(frozen=True)
class CatalogBoundary:
    revision: str
    catalog_index: Path
    catalog: Path


def _catalog_boundary(config: RefreshConfig) -> CatalogBoundary:
    manifest_path = config.catalog_manifest.resolve(strict=True)
    payload = json.loads(manifest_path.read_bytes())
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != "marketcow.polymarket.live.v2"
    ):
        raise ValueError("current catalog manifest is unsupported")
    revision = payload.get("catalog_revision")
    normalized = payload.get("normalized_catalog")
    index = payload.get("catalog_index")
    if (
        not isinstance(revision, str)
        or len(revision) != 64
        or any(value not in "0123456789abcdef" for value in revision)
        or not isinstance(normalized, dict)
        or not isinstance(index, dict)
        or normalized.get("format") != "canonical_jsonl"
        or index.get("format") != "sqlite-offset-v1"
        or normalized.get("catalog_revision", revision) != revision
        or index.get("catalog_revision") != revision
    ):
        raise ValueError("current catalog manifest boundary is incoherent")
    catalog = Path(str(normalized.get("path") or "")).resolve(strict=True)
    catalog_index = Path(str(index.get("path") or "")).resolve(strict=True)
    if _sha256(catalog) != normalized.get("sha256"):
        raise ValueError("current normalized catalog hash mismatch")
    if _sha256(catalog_index) != index.get("sha256"):
        raise ValueError("current catalog index hash mismatch")
    return CatalogBoundary(revision, catalog_index, catalog)


def _selection_relation_registry(
    selection_path: Path,
    boundary: CatalogBoundary,
    output_path: Path,
) -> Path:
    """Build the exact relation registry from one immutable selection boundary."""
    selection = json.loads(selection_path.read_bytes())
    if selection.get("schema") != "tradude.prediction_market.scope_selection.v2":
        raise ValueError("candidate manifest must be a Tradude scope selection v2")
    if selection.get("catalog_revision") != boundary.revision:
        raise ValueError("selection and current catalog revisions differ")
    market_ids = selection.get("market_ids")
    relations = selection.get("relations")
    if not isinstance(market_ids, list) or not isinstance(relations, list):
        raise ValueError("selection markets/relations are invalid")
    import sqlite3

    placeholders = ",".join("?" for _ in market_ids)
    uri = f"{boundary.catalog_index.as_uri()}?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as connection:
        rows = connection.execute(
            f"SELECT market_id, byte_offset, byte_length FROM markets "
            f"WHERE market_id IN ({placeholders}) ORDER BY market_id",
            market_ids,
        ).fetchall()
    market_outcomes: dict[str, dict[str, dict[str, Any]]] = {}
    with boundary.catalog.open("rb") as catalog:
        for market_id, offset, length in rows:
            catalog.seek(offset)
            row = json.loads(catalog.read(length))
            outcomes = (row.get("identity") or {}).get("outcomes") or []
            market_outcomes[str(market_id)] = {
                str(item.get("outcome", "")).casefold(): item for item in outcomes
            }
    if set(market_outcomes) != set(map(str, market_ids)):
        raise ValueError("selection relation registry cannot resolve every market")
    groups = []
    for relation in relations:
        if not isinstance(relation, dict) or relation.get("complete") is not True:
            raise ValueError("selection contains an incomplete relation")
        member_ids = list(map(str, relation.get("member_market_ids") or []))
        if (
            not member_ids
            or int(relation.get("expected_member_count", -1)) != len(member_ids)
            or int(relation.get("actual_member_count", -1)) != len(member_ids)
            or set(member_ids) - set(market_outcomes)
        ):
            raise ValueError("selection relation membership is invalid")
        members = []
        for market_id in member_ids:
            outcomes = market_outcomes[market_id]
            yes = outcomes.get("yes")
            no = outcomes.get("no")
            if yes is None or no is None:
                raise ValueError("selection relation member is not binary Yes/No")
            members.append({
                "market_id": market_id,
                "yes_outcome_id": yes["instrument_id"],
                "no_outcome_id": no["instrument_id"],
            })
        material = {
            "relation_id": relation.get("relation_id"),
            "members": members,
        }
        groups.append({
            **material,
            "relation_revision": hashlib.sha256(
                json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
            "complete": True,
        })
    registry = {
        "schema_version": "marketcow.polymarket.selection-relation-registry.v1",
        "catalog_revision": boundary.revision,
        "negative_risk_groups": groups,
    }
    write_atomic_json(output_path, registry)
    return output_path


def _canonical_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("universe identity timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _identity_set(
    values: list[dict[str, Any]],
) -> set[tuple[str, str, tuple[str, ...], datetime]]:
    return {
        (
            value["market_id"],
            value["condition_id"],
            tuple(sorted(value["token_ids"])),
            _canonical_timestamp(value["end_at"]),
        )
        for value in values
    }


def _get_json(session: Any, url: str, timeout: float) -> tuple[int, dict[str, Any]]:
    response = session.get(url, timeout=timeout)
    payload = response.json(parse_float=str, parse_int=str)
    return response.status_code, payload


def _validate_live_boundary(scope: dict[str, Any], full_sync: dict[str, Any]) -> None:
    universe = full_sync.get("universe") or {}
    snapshot = full_sync.get("snapshot", {})
    checks = {
        "scope_schema": scope.get("schema_version") == SCOPE_SCHEMA_VERSION,
        "scope_ready": scope.get("ready") is True and scope.get("scope_status") == "ready",
        "scope_identity": full_sync.get("scope_id") == scope.get("active_scope_id"),
        "universe_identity": full_sync.get("universe_id") == scope.get("universe_id"),
        "generation": int(full_sync.get("universe_generation", -1))
        == int(scope.get("generation", -2))
        and int(universe.get("generation", -3)) == int(scope.get("generation", -2)),
        "active_identities": _identity_set(universe.get("active_markets", []))
        == _identity_set(scope.get("active_markets", [])),
        "book_count": len(snapshot.get("books", [])) == int(scope.get("token_count", -1)),
        "market_count": len(snapshot.get("markets", [])) == int(scope.get("market_count", -1)),
        "gap_free": not snapshot.get("unresolved_gaps"),
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(
            "live scope/full-sync boundary is not atomically ready: "
            f"failed={failed} "
            f"scope_ready={scope.get('ready')} generation={scope.get('generation')}/"
            f"{full_sync.get('universe_generation')} markets="
            f"{len(full_sync.get('snapshot', {}).get('markets', []))}/"
            f"{scope.get('market_count')} books="
            f"{len(full_sync.get('snapshot', {}).get('books', []))}/"
            f"{scope.get('token_count')}"
        )


def _validate_scope_identity(scope: dict[str, Any]) -> None:
    """Validate the durable universe identity even when its live books are fail-closed.

    A one-sided active book must make `/full-sync` unavailable, but the MarketCow validation and
    publication controller still needs the last atomically published membership to validate its
    replacement generation against a fresh, independent CLOB snapshot. The public `/scope` identity
    is sufficient for that purpose as long as every count and stable identity is internally
    consistent; no unready book state is reused.
    """
    active = scope.get("active_markets")
    if not isinstance(active, list):
        raise RuntimeError("live scope active_markets is unavailable")
    configured = scope.get("configured_markets")
    if not isinstance(configured, list):
        raise RuntimeError("live scope configured_markets is unavailable")
    try:
        generation = int(scope.get("generation", 0))
        market_count = int(scope.get("market_count", -1))
        token_count = int(scope.get("token_count", -1))
        configured_market_count = int(scope.get("configured_market_count", -1))
        configured_token_count = int(scope.get("configured_token_count", -1))
        target_count = int(scope.get("target_market_count", -1))
        minimum_count = int(scope.get("minimum_market_count", -1))
    except (TypeError, ValueError) as error:
        raise RuntimeError("live scope counts are invalid") from error
    identities = _identity_set(active)
    tokens = [token for identity in active for token in identity.get("token_ids", [])]
    configured_identities = _identity_set(configured)
    configured_tokens = [
        token for identity in configured for token in identity.get("token_ids", [])
    ]
    active_ids = {identity[0] for identity in identities}
    configured_ids = {identity[0] for identity in configured_identities}
    quarantined_ids = set(map(str, scope.get("quarantined_market_ids") or []))
    ready = scope.get("ready") is True
    checks = {
        "schema": scope.get("schema_version") == SCOPE_SCHEMA_VERSION,
        "scope_identity": scope.get("active_scope_id") == scope.get("universe_id"),
        "universe_id": isinstance(scope.get("universe_id"), str)
        and len(scope["universe_id"]) == 64
        and all(value in "0123456789abcdefABCDEF" for value in scope["universe_id"]),
        "generation": generation > 0,
        "status": scope.get("scope_status") == ("ready" if ready else "unready"),
        "market_count": market_count == len(active) == len(identities),
        "token_count": token_count == len(tokens) == 2 * market_count,
        "unique_tokens": len(tokens) == len(set(tokens)),
        "configured_market_count": configured_market_count
        == len(configured)
        == len(configured_identities),
        "configured_token_count": configured_token_count
        == len(configured_tokens)
        == 2 * configured_market_count,
        "configured_unique_tokens": len(configured_tokens)
        == len(set(configured_tokens)),
        "active_subset": active_ids <= configured_ids,
        "quarantine_partition": quarantined_ids == configured_ids - active_ids,
        "capacity": 0
        < minimum_count
        <= configured_market_count
        <= target_count
        <= 250,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(f"live scope identity is invalid: failed={failed}")


def _read_ready_boundary(
    config: RefreshConfig,
    session: Any,
    *,
    allow_unready_identity: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Take a bounded coherent read while allowing a short transport recovery barrier.

    When `allow_unready_identity` is true, a strictly valid `/scope` with `ready=false` can seed
    only the previous membership identity. Candidate books are always fetched independently and
    candidate activation still has to return `activated_ready` before publication.
    """
    last_error: Exception | None = None
    for attempt in range(10):
        scope_status, scope = _get_json(
            session,
            f"{config.service_url}/v1/prediction-markets/polymarket/live/scope",
            config.request_timeout_seconds,
        )
        try:
            if scope_status != 200:
                raise RuntimeError(f"live scope is unavailable: HTTP {scope_status}")
            _validate_scope_identity(scope)
            if allow_unready_identity and scope.get("ready") is not True:
                return scope, {}
        except RuntimeError as error:
            last_error = error
            if attempt < 9:
                time.sleep(0.25)
            continue
        full_status, full_sync = _get_json(
            session,
            f"{config.service_url}/v1/prediction-markets/polymarket/live/full-sync",
            config.request_timeout_seconds,
        )
        try:
            if scope_status != 200 or full_status != 200:
                raise RuntimeError(
                    f"live service is not ready: scope={scope_status} full_sync={full_status}"
                )
            _validate_live_boundary(scope, full_sync)
            return scope, full_sync
        except RuntimeError as error:
            last_error = error
            if attempt < 9:
                time.sleep(0.25)
    raise last_error or RuntimeError("live boundary unavailable")


def _configured_market_identities(
    config: RefreshConfig, session: Any, scope: dict[str, Any]
) -> list[dict[str, Any]]:
    """Reconstruct the configured generation, not only its currently executable subset.

    Scope v5 publishes the complete configured identity separately from its currently executable
    subset. Universe delta validation is relative to that complete generation; no quarantined book
    state is reused for opportunities or candidate activation.
    """
    del config, session
    identities = [dict(value) for value in scope.get("configured_markets") or []]
    if len(_identity_set(identities)) != len(identities):
        raise RuntimeError("configured market identities are duplicated")
    return identities


def _activate(
    config: RefreshConfig,
    session: Any,
    token: str,
    universe_id: str,
    candidate_path: Path,
) -> dict[str, Any]:
    digest = _sha256(candidate_path)
    registered = config.scope_registry_root / f"{universe_id}.json"
    previous = registered.read_bytes() if registered.exists() else None
    config.scope_registry_root.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=registered.parent, prefix=".auto-refresh-", delete=False) as stream:
        temporary = Path(stream.name)
        stream.write(candidate_path.read_bytes())
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, registered)
    try:
        response = session.post(
            f"{config.service_url}/v1/admin/polymarket/scope:activate",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "schema_version": ACTIVATION_SCHEMA_VERSION,
                "scope_id": universe_id,
                "scope_file_sha256": digest,
            },
            timeout=config.request_timeout_seconds,
        )
        payload = response.json(parse_float=str, parse_int=str)
        if response.status_code != 200 or payload.get("status") != "activated_ready":
            raise RuntimeError(f"activation failed closed: HTTP {response.status_code} {payload}")
        startup_temporary = config.startup_scope.with_name(
            f".{config.startup_scope.name}.{os.getpid()}.tmp"
        )
        config.startup_scope.parent.mkdir(parents=True, exist_ok=True)
        try:
            with startup_temporary.open("xb") as stream:
                stream.write(candidate_path.read_bytes())
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(startup_temporary, config.startup_scope)
        finally:
            startup_temporary.unlink(missing_ok=True)
        return payload
    except Exception:
        # A timeout is ambiguous. Only restore the registered artifact if the service demonstrably
        # remained on the preceding digest. A completed activation persists its own immutable copy.
        try:
            _, probe = _get_json(
                session,
                f"{config.service_url}/v1/prediction-markets/polymarket/live/scope",
                min(config.request_timeout_seconds, 5),
            )
            activated = probe.get("scope_file_sha256") == digest
        except Exception:
            activated = False
        if not activated:
            if previous is None:
                registered.unlink(missing_ok=True)
            else:
                registered.write_bytes(previous)
        raise


def refresh_once(
    config: RefreshConfig,
    *,
    session: Any = requests,
    book_snapshot_builder: Callable[..., dict[str, Any]] = build_candidate_book_snapshot,
    universe_builder: Callable[..., dict[str, Any]] = build_dynamic_universe,
) -> dict[str, Any]:
    started = datetime.now(timezone.utc)
    token = os.environ.get("MARKETCOW_RUST_ADMIN_TOKEN", "")
    if not token:
        raise RuntimeError("MARKETCOW_RUST_ADMIN_TOKEN is required")
    scope, _ = _read_ready_boundary(config, session, allow_unready_identity=True)
    recovery_from_unready = scope.get("ready") is not True
    universe_id = scope["universe_id"]
    current_generation = int(scope["generation"])
    current_identities = _configured_market_identities(config, session, scope)
    next_generation = current_generation + 1
    config.work_root.mkdir(parents=True, exist_ok=True)
    boundary = _catalog_boundary(config)
    relation_registry = _selection_relation_registry(
        config.candidate_manifest,
        boundary,
        config.work_root / f"generation-{next_generation:020d}-relations.json",
    )
    books_path = config.work_root / f"generation-{next_generation:020d}-candidate-books.json"
    candidate_path = config.work_root / f"generation-{next_generation:020d}-candidate.json"
    books = book_snapshot_builder(
        config.candidate_manifest,
        boundary.catalog_index,
        endpoint=config.clob_books_endpoint,
        timeout_seconds=config.request_timeout_seconds,
    )
    write_atomic_json(books_path, books)
    candidate = universe_builder(
        config.candidate_manifest,
        boundary.catalog_index,
        boundary.catalog,
        relation_registry,
        books_path,
        universe_id=universe_id,
        generation=next_generation,
        target_market_count=config.target_market_count,
        minimum_market_count=config.minimum_market_count,
        previous_market_ids=[value["market_id"] for value in current_identities],
        previous_market_identities=current_identities,
        retry_seconds=config.retry_seconds,
    )
    write_atomic_json(candidate_path, candidate)
    changed = _identity_set(current_identities) != _identity_set(candidate["universe"]["active_markets"])
    latest_scope, _ = _read_ready_boundary(
        config, session, allow_unready_identity=True
    )
    latest_identities = _configured_market_identities(config, session, latest_scope)
    if (
        latest_scope.get("universe_id") != universe_id
        or int(latest_scope.get("generation", -1)) != current_generation
        or _identity_set(latest_identities) != _identity_set(current_identities)
    ):
        raise RuntimeError("live universe changed while candidate was being built")
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "started_at": started.isoformat().replace("+00:00", "Z"),
        "completed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "status": "no_change" if not changed else "candidate_validated",
        "service_url": config.service_url,
        "universe_id": universe_id,
        "previous_generation": current_generation,
        "candidate_generation": next_generation,
        "target_market_count": config.target_market_count,
        "minimum_market_count": config.minimum_market_count,
        "active_market_count": len(candidate["universe"]["active_markets"]),
        "excluded_market_count": len(candidate["universe"]["excluded_markets"]),
        "added_markets": candidate["universe"]["added_markets"],
        "removed_markets": candidate["universe"]["removed_markets"],
        "recovery_from_unready": recovery_from_unready,
        "candidate_path": str(candidate_path.resolve()),
        "candidate_sha256": _sha256(candidate_path),
        "book_snapshot_path": str(books_path.resolve()),
        "book_snapshot_sha256": _sha256(books_path),
        "scope_selection_owner": "tradude",
        "marketcow_computes_market_ranking": False,
    }
    if not changed:
        return result
    receipt = _activate(config, session, token, universe_id, candidate_path)
    post_scope, post_full = _read_ready_boundary(config, session)
    if int(post_scope.get("generation", -1)) != next_generation:
        raise RuntimeError("activated generation identity mismatch")
    result.update(status="activated_ready", completed_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), activation_receipt=receipt)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--follow", action="store_true")
    parser.add_argument("--poll-seconds", type=float)
    arguments = parser.parse_args()
    if arguments.follow != (arguments.poll_seconds is not None):
        parser.error("--follow and --poll-seconds must be supplied together")
    if arguments.poll_seconds is not None and arguments.poll_seconds <= 0:
        parser.error("--poll-seconds must be positive")
    config = _load_config(arguments.config)
    config.work_root.mkdir(parents=True, exist_ok=True)
    lock_path = config.work_root / "auto-refresh.lock"
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            write_atomic_json(config.audit_result, {
                "schema_version": SCHEMA_VERSION,
                "status": "skipped_overlap",
                "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            })
            return 0
        last_completed_digest: str | None = None
        while True:
            try:
                digest = _sha256(config.candidate_manifest)
                if digest != last_completed_digest:
                    result = refresh_once(config)
                    write_atomic_json(config.audit_result, result)
                    print(json.dumps(result, sort_keys=True), flush=True)
                    last_completed_digest = digest
            except Exception as error:
                write_atomic_json(config.audit_result, {
                    "schema_version": SCHEMA_VERSION,
                    "status": "failed_closed",
                    "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "error_type": type(error).__name__,
                    "error": str(error),
                })
                if not arguments.follow:
                    raise
            if not arguments.follow:
                return 0
            time.sleep(arguments.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
