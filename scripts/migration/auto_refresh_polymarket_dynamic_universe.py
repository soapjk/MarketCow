#!/usr/bin/env python3
"""Run one fail-closed MarketCow-owned dynamic-universe refresh transaction.

This command is intentionally one-shot. A service manager or scheduler may invoke it periodically;
an advisory lock prevents overlapping refreshes. Tradude is never involved in discovery, activation,
or MarketCow lifecycle management.
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


SCHEMA_VERSION = "marketcow.polymarket.universe-auto-refresh.v1"
CONFIG_SCHEMA_VERSION = "marketcow.polymarket.universe-auto-refresh-config.v1"
ACTIVATION_SCHEMA_VERSION = "marketcow.polymarket.scope-activation.v1"
SCOPE_SCHEMA_VERSION = "marketcow.polymarket.scope-discovery.v3"


@dataclass(frozen=True)
class RefreshConfig:
    service_url: str
    candidate_manifest: Path
    catalog_index: Path
    catalog: Path
    fee_registry: Path
    scope_registry_root: Path
    work_root: Path
    audit_result: Path
    target_market_count: int
    minimum_market_count: int
    maximum_capital_lock_seconds: int
    clob_books_endpoint: str = "https://clob.polymarket.com/books"
    request_timeout_seconds: float = 60.0
    retry_seconds: int = 60


def _load_config(path: Path) -> RefreshConfig:
    payload = json.loads(path.resolve(strict=True).read_bytes())
    expected = {
        "schema_version", "service_url", "candidate_manifest", "catalog_index", "catalog",
        "fee_registry", "scope_registry_root", "work_root", "audit_result",
        "target_market_count", "minimum_market_count", "maximum_capital_lock_seconds",
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
        catalog_index=Path(payload["catalog_index"]),
        catalog=Path(payload["catalog"]),
        fee_registry=Path(payload["fee_registry"]),
        scope_registry_root=Path(payload["scope_registry_root"]),
        work_root=Path(payload["work_root"]),
        audit_result=Path(payload["audit_result"]),
        target_market_count=int(payload["target_market_count"]),
        minimum_market_count=int(payload["minimum_market_count"]),
        maximum_capital_lock_seconds=int(payload["maximum_capital_lock_seconds"]),
        clob_books_endpoint=payload.get("clob_books_endpoint", "https://clob.polymarket.com/books"),
        request_timeout_seconds=float(payload.get("request_timeout_seconds", 60)),
        retry_seconds=int(payload.get("retry_seconds", 60)),
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _identity_set(values: list[dict[str, Any]]) -> set[tuple[str, str, tuple[str, ...], str]]:
    return {
        (value["market_id"], value["condition_id"], tuple(value["token_ids"]), value["end_at"])
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


def _read_ready_boundary(config: RefreshConfig, session: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """Take a bounded coherent read while allowing a short transport recovery barrier."""
    last_error: Exception | None = None
    for attempt in range(10):
        scope_status, scope = _get_json(
            session,
            f"{config.service_url}/v1/prediction-markets/polymarket/live/scope",
            config.request_timeout_seconds,
        )
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
    scope, full_sync = _read_ready_boundary(config, session)
    universe_id = scope["universe_id"]
    current_generation = int(scope["generation"])
    current_identities = scope["active_markets"]
    next_generation = current_generation + 1
    config.work_root.mkdir(parents=True, exist_ok=True)
    books_path = config.work_root / f"generation-{next_generation:020d}-candidate-books.json"
    candidate_path = config.work_root / f"generation-{next_generation:020d}-candidate.json"
    books = book_snapshot_builder(
        config.candidate_manifest,
        config.catalog_index,
        endpoint=config.clob_books_endpoint,
        timeout_seconds=config.request_timeout_seconds,
    )
    write_atomic_json(books_path, books)
    candidate = universe_builder(
        config.candidate_manifest,
        config.catalog_index,
        config.catalog,
        config.fee_registry,
        books_path,
        universe_id=universe_id,
        generation=next_generation,
        target_market_count=config.target_market_count,
        minimum_market_count=config.minimum_market_count,
        maximum_capital_lock_seconds=config.maximum_capital_lock_seconds,
        previous_market_ids=[value["market_id"] for value in current_identities],
        retry_seconds=config.retry_seconds,
    )
    write_atomic_json(candidate_path, candidate)
    changed = _identity_set(current_identities) != _identity_set(candidate["universe"]["active_markets"])
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
        "maximum_capital_lock_seconds": config.maximum_capital_lock_seconds,
        "active_market_count": len(candidate["universe"]["active_markets"]),
        "excluded_market_count": len(candidate["universe"]["excluded_markets"]),
        "added_markets": candidate["universe"]["added_markets"],
        "removed_markets": candidate["universe"]["removed_markets"],
        "candidate_path": str(candidate_path.resolve()),
        "candidate_sha256": _sha256(candidate_path),
        "book_snapshot_path": str(books_path.resolve()),
        "book_snapshot_sha256": _sha256(books_path),
        "real_order_submission_enabled": False,
        "tradude_manages_marketcow_lifecycle": False,
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
    arguments = parser.parse_args()
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
        try:
            result = refresh_once(config)
        except Exception as error:
            write_atomic_json(config.audit_result, {
                "schema_version": SCHEMA_VERSION,
                "status": "failed_closed",
                "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "error_type": type(error).__name__,
                "error": str(error),
                "real_order_submission_enabled": False,
            })
            raise
        write_atomic_json(config.audit_result, result)
        print(json.dumps(result, sort_keys=True))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
