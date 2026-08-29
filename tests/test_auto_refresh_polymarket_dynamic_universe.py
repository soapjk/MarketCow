from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.migration.auto_refresh_polymarket_dynamic_universe import (
    CONFIG_SCHEMA_VERSION,
    RefreshConfig,
    _load_config,
    refresh_once,
)


IDENTITY = {
    "market_id": "1",
    "condition_id": "0x" + "1" * 64,
    "token_ids": ["11", "12"],
    "end_at": "2026-08-30T00:00:00Z",
}


class Response:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self, **_kwargs):
        return self._payload


class Session:
    def __init__(self, *, changed: bool = False):
        self.changed = changed
        self.posts: list[dict] = []
        self.generation = 1

    def _scope(self) -> dict:
        identity = {**IDENTITY, "market_id": "2"} if self.generation == 2 else IDENTITY
        return {
            "schema_version": "marketcow.polymarket.scope-discovery.v3",
            "ready": True,
            "scope_status": "ready",
            "active_scope_id": "a" * 64,
            "universe_id": "a" * 64,
            "generation": self.generation,
            "market_count": 1,
            "token_count": 2,
            "active_markets": [identity],
            "scope_file_sha256": "b" * 64,
        }

    def _full(self) -> dict:
        scope = self._scope()
        return {
            "scope_id": scope["active_scope_id"],
            "universe_id": scope["universe_id"],
            "universe_generation": scope["generation"],
            "universe": {"generation": scope["generation"], "active_markets": scope["active_markets"]},
            "snapshot": {"books": [{}, {}], "markets": [{}], "unresolved_gaps": []},
        }

    def get(self, url, **_kwargs):
        return Response(200, self._scope() if url.endswith("/scope") else self._full())

    def post(self, _url, **kwargs):
        self.posts.append(kwargs)
        self.generation = 2
        return Response(200, {
            "status": "activated_ready",
            "active_generation": 2,
            "real_order_submission_enabled": False,
        })


def _config(tmp_path: Path) -> RefreshConfig:
    for name in ("candidates.json", "catalog.sqlite3", "catalog.jsonl", "fees.json"):
        (tmp_path / name).write_text("{}")
    registry = tmp_path / "registry"
    registry.mkdir()
    (registry / f"{'a' * 64}.json").write_text("{\"old\":true}")
    return RefreshConfig(
        service_url="http://127.0.0.1:18872",
        candidate_manifest=tmp_path / "candidates.json",
        catalog_index=tmp_path / "catalog.sqlite3",
        catalog=tmp_path / "catalog.jsonl",
        fee_registry=tmp_path / "fees.json",
        scope_registry_root=registry,
        work_root=tmp_path / "work",
        audit_result=tmp_path / "audit.json",
        target_market_count=1,
        minimum_market_count=1,
        maximum_capital_lock_seconds=2_592_000,
    )


def _books(*_args, **_kwargs):
    return {"books": [], "schema_version": "test"}


def _candidate(identity: dict) -> dict:
    return {
        "schema_version": "marketcow.polymarket.rust-live-scope.v4",
        "scope_id": "a" * 64,
        "market_count": 1,
        "token_count": 2,
        "market_ids": [identity["market_id"]],
        "token_ids": identity["token_ids"],
        "catalog_revision": "c" * 64,
        "catalog_frame": {"markets": [], "negative_risk_relations": []},
        "universe": {
            "universe_id": "a" * 64,
            "generation": 2,
            "active_markets": [identity],
            "excluded_markets": [],
            "added_markets": [] if identity["market_id"] == "1" else ["2"],
            "removed_markets": [] if identity["market_id"] == "1" else ["1"],
        },
        "initial_book_frames": [],
    }


def test_no_change_is_idempotent_and_does_not_activate(tmp_path, monkeypatch):
    monkeypatch.setenv("MARKETCOW_RUST_ADMIN_TOKEN", "secret")
    session = Session()
    result = refresh_once(
        _config(tmp_path), session=session, book_snapshot_builder=_books,
        universe_builder=lambda *_args, **_kwargs: _candidate(IDENTITY),
    )
    assert result["status"] == "no_change"
    assert result["candidate_generation"] == 2
    assert session.posts == []


def test_outcome_token_order_is_not_a_universe_identity_change(tmp_path, monkeypatch):
    monkeypatch.setenv("MARKETCOW_RUST_ADMIN_TOKEN", "secret")
    session = Session()
    reordered = {**IDENTITY, "token_ids": list(reversed(IDENTITY["token_ids"]))}
    result = refresh_once(
        _config(tmp_path), session=session, book_snapshot_builder=_books,
        universe_builder=lambda *_args, **_kwargs: _candidate(reordered),
    )
    assert result["status"] == "no_change"
    assert session.posts == []


def test_changed_membership_registers_and_atomically_activates(tmp_path, monkeypatch):
    monkeypatch.setenv("MARKETCOW_RUST_ADMIN_TOKEN", "secret")
    replacement = {**IDENTITY, "market_id": "2"}
    session = Session(changed=True)
    config = _config(tmp_path)
    result = refresh_once(
        config, session=session, book_snapshot_builder=_books,
        universe_builder=lambda *_args, **_kwargs: _candidate(replacement),
    )
    assert result["status"] == "activated_ready"
    assert result["added_markets"] == ["2"]
    assert result["removed_markets"] == ["1"]
    assert len(session.posts) == 1
    request = session.posts[0]
    assert request["headers"] == {"Authorization": "Bearer secret"}
    assert request["json"]["scope_file_sha256"] == result["candidate_sha256"]
    registered = json.loads((config.scope_registry_root / f"{'a' * 64}.json").read_text())
    assert registered["universe"]["generation"] == 2


def test_missing_admin_token_fails_before_external_read(tmp_path, monkeypatch):
    monkeypatch.delenv("MARKETCOW_RUST_ADMIN_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="ADMIN_TOKEN"):
        refresh_once(_config(tmp_path), session=Session())


def test_config_rejects_non_loopback_activation(tmp_path):
    payload = {
        "schema_version": CONFIG_SCHEMA_VERSION,
        "service_url": "https://example.com",
        "candidate_manifest": "a",
        "catalog_index": "b",
        "catalog": "c",
        "fee_registry": "d",
        "scope_registry_root": "e",
        "work_root": "f",
        "audit_result": "g",
        "target_market_count": 100,
        "minimum_market_count": 80,
        "maximum_capital_lock_seconds": 2_592_000,
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="loopback"):
        _load_config(path)
