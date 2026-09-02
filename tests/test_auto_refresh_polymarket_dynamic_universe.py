from __future__ import annotations

import json
import hashlib
import sqlite3
from pathlib import Path

import pytest

from scripts.migration.auto_refresh_polymarket_dynamic_universe import (
    CONFIG_SCHEMA_VERSION,
    RefreshConfig,
    _load_config,
    _validate_scope_identity,
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
    def __init__(self, *, changed: bool = False, ready: bool = True):
        self.changed = changed
        self.ready = ready
        self.posts: list[dict] = []
        self.generation = 1

    def _scope(self) -> dict:
        identity = {**IDENTITY, "market_id": "2"} if self.generation == 2 else IDENTITY
        return {
            "schema_version": "marketcow.polymarket.scope-discovery.v5",
            "ready": self.ready,
            "scope_status": "ready" if self.ready else "unready",
            "active_scope_id": "a" * 64,
            "universe_id": "a" * 64,
            "generation": self.generation,
            "market_count": 1,
            "token_count": 2,
            "configured_market_count": 1,
            "configured_token_count": 2,
            "active_markets": [identity],
            "configured_markets": [identity],
            "quarantined_market_ids": [],
            "scope_file_sha256": "b" * 64,
            "target_market_count": 1,
            "minimum_market_count": 1,
            "real_order_submission_enabled": False,
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
        if "/live/markets/" in url and url.endswith("/snapshot"):
            market_id = url.rsplit("/", 2)[-2]
            return Response(200, {
                "schema_version": "marketcow.polymarket.market-snapshot.v1",
                "scan_universe_membership": True,
                "stable_identity_for_position_monitoring": True,
                "usable_for_new_opportunities": False,
                "market": {
                    "market_id": market_id,
                    "condition_id": "0x" + "3" * 64,
                    "outcomes": [
                        {"token_id": "31", "outcome": "Yes", "instrument_id": "YES"},
                        {"token_id": "32", "outcome": "No", "instrument_id": "NO"},
                    ],
                    "instrument_facts": {"end_at": "2026-08-31T00:00:00Z"},
                },
            })
        if url.endswith("/scope"):
            return Response(200, self._scope())
        if not self.ready:
            return Response(503, {"detail": {"code": "polymarket_projection_unready_or_stale"}})
        return Response(200, self._full())

    def post(self, _url, **kwargs):
        self.posts.append(kwargs)
        self.generation = 2
        self.ready = True
        return Response(200, {
            "status": "activated_ready",
            "active_generation": 2,
            "real_order_submission_enabled": False,
        })


def _config(tmp_path: Path) -> RefreshConfig:
    revision = "c" * 64
    candidates = tmp_path / "candidates.json"
    candidates.write_text(json.dumps({
        "schema": "tradude.prediction_market.scope_selection.v2",
        "catalog_revision": revision,
        "market_ids": ["1", "2", "3", "retired-bad-market"],
        "relations": [],
    }))
    catalog = tmp_path / "catalog.jsonl"
    offsets = []
    with catalog.open("wb") as stream:
        for market_id in ("1", "2", "3", "retired-bad-market"):
            row = json.dumps({
                "identity": {
                    "market_id": market_id,
                    "outcomes": [
                        {"outcome": "Yes", "instrument_id": f"POLY:c:{market_id}1"},
                        {"outcome": "No", "instrument_id": f"POLY:c:{market_id}2"},
                    ],
                },
            }, separators=(",", ":")).encode() + b"\n"
            offset = stream.tell()
            stream.write(row)
            offsets.append((market_id, offset, len(row)))
    catalog_index = tmp_path / "catalog.sqlite3"
    with sqlite3.connect(catalog_index) as connection:
        connection.execute(
            "CREATE TABLE markets (market_id TEXT PRIMARY KEY, byte_offset INTEGER, byte_length INTEGER)"
        )
        connection.executemany("INSERT INTO markets VALUES (?, ?, ?)", offsets)
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    catalog_manifest = tmp_path / "catalog-manifest.json"
    catalog_manifest.write_text(json.dumps({
        "schema_version": "marketcow.polymarket.live.v2",
        "catalog_revision": revision,
        "normalized_catalog": {
            "format": "canonical_jsonl",
            "path": str(catalog),
            "sha256": digest(catalog),
        },
        "catalog_index": {
            "format": "sqlite-offset-v1",
            "catalog_revision": revision,
            "path": str(catalog_index),
            "sha256": digest(catalog_index),
        },
    }))
    registry = tmp_path / "registry"
    registry.mkdir()
    (registry / f"{'a' * 64}.json").write_text("{\"old\":true}")
    return RefreshConfig(
        service_url="http://127.0.0.1:18872",
        candidate_manifest=candidates,
        catalog_manifest=catalog_manifest,
        startup_scope=tmp_path / "active-scope.json",
        scope_registry_root=registry,
        work_root=tmp_path / "work",
        audit_result=tmp_path / "audit.json",
        target_market_count=1,
        minimum_market_count=1,
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


def test_unready_scope_below_minimum_preserves_valid_configured_identity():
    scope = Session(ready=False)._scope()
    scope.update(
        market_count=0,
        token_count=0,
        active_markets=[],
        quarantined_market_ids=["1"],
    )

    _validate_scope_identity(scope)


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


def test_equivalent_rfc3339_precision_is_not_an_identity_change(tmp_path, monkeypatch):
    monkeypatch.setenv("MARKETCOW_RUST_ADMIN_TOKEN", "secret")
    session = Session()
    equivalent = {**IDENTITY, "end_at": "2026-08-30T00:00:00.000000Z"}
    result = refresh_once(
        _config(tmp_path), session=session, book_snapshot_builder=_books,
        universe_builder=lambda *_args, **_kwargs: _candidate(equivalent),
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
    assert json.loads(config.startup_scope.read_text())["universe"]["generation"] == 2


def test_below_target_quarantine_view_is_automatically_replenished(tmp_path, monkeypatch):
    monkeypatch.setenv("MARKETCOW_RUST_ADMIN_TOKEN", "secret")
    replacement = {
        **IDENTITY,
        "market_id": "2",
        "condition_id": "0x" + "2" * 64,
        "token_ids": ["21", "22"],
    }
    retired = {
        **IDENTITY,
        "market_id": "retired-bad-market",
        "condition_id": "0x" + "3" * 64,
        "token_ids": ["31", "32"],
        "end_at": "2026-08-31T00:00:00Z",
    }

    class PartialSession(Session):
        expanded = False

        def _scope(self) -> dict:
            scope = super()._scope()
            active = [IDENTITY, replacement] if self.expanded else [IDENTITY]
            configured = active if self.expanded else [IDENTITY, retired]
            scope.update({
                "market_count": len(active),
                "token_count": 2 * len(active),
                "active_markets": active,
                "active_market_ids": [item["market_id"] for item in active],
                "configured_market_count": len(configured),
                "configured_token_count": 2 * len(configured),
                "configured_markets": configured,
                "quarantined_market_ids": [] if self.expanded else ["retired-bad-market"],
                "target_market_count": 2,
                "minimum_market_count": 1,
            })
            return scope

        def _full(self) -> dict:
            scope = self._scope()
            return {
                "scope_id": scope["active_scope_id"],
                "universe_id": scope["universe_id"],
                "universe_generation": scope["generation"],
                "universe": {
                    "generation": scope["generation"],
                    "active_markets": scope["active_markets"],
                },
                "snapshot": {
                    "books": [{} for _ in range(scope["token_count"])],
                    "markets": [{} for _ in range(scope["market_count"])],
                    "unresolved_gaps": [],
                },
            }

        def post(self, url, **kwargs):
            response = super().post(url, **kwargs)
            self.expanded = True
            return response

    candidate = _candidate(IDENTITY)
    candidate.update({
        "market_count": 2,
        "token_count": 4,
        "market_ids": ["1", "2"],
        "token_ids": ["11", "12", "21", "22"],
    })
    candidate["universe"].update({
        "active_markets": [IDENTITY, replacement],
        "added_markets": ["2"],
        "removed_markets": ["retired-bad-market"],
    })
    config = _config(tmp_path)
    config = RefreshConfig(**{**config.__dict__, "target_market_count": 2})
    session = PartialSession(changed=True)
    result = refresh_once(
        config,
        session=session,
        book_snapshot_builder=_books,
        universe_builder=lambda *_args, **_kwargs: candidate,
    )
    assert result["status"] == "activated_ready"
    assert result["added_markets"] == ["2"]
    assert result["removed_markets"] == ["retired-bad-market"]
    assert len(session.posts) == 1


def test_concurrent_quarantine_does_not_mix_or_invalidate_configured_generation(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("MARKETCOW_RUST_ADMIN_TOKEN", "secret")
    second = {
        **IDENTITY,
        "market_id": "2",
        "condition_id": "0x" + "2" * 64,
        "token_ids": ["21", "22"],
    }
    replacement = {
        **IDENTITY,
        "market_id": "3",
        "condition_id": "0x" + "3" * 64,
        "token_ids": ["31", "32"],
    }

    class ConcurrentQuarantineSession(Session):
        quarantined_during_build = False

        def _scope(self) -> dict:
            scope = super()._scope()
            if self.generation == 2:
                active = [IDENTITY, replacement]
                quarantined = []
            elif self.quarantined_during_build:
                active = [IDENTITY]
                quarantined = ["2"]
            else:
                active = [IDENTITY, second]
                quarantined = []
            configured = (
                [IDENTITY, replacement]
                if self.generation == 2
                else [IDENTITY, second]
            )
            scope.update({
                "market_count": len(active),
                "token_count": len(active) * 2,
                "active_markets": active,
                "active_market_ids": [value["market_id"] for value in active],
                "configured_market_count": len(configured),
                "configured_token_count": len(configured) * 2,
                "configured_markets": configured,
                "quarantined_market_ids": quarantined,
                "target_market_count": 2,
                "minimum_market_count": 1,
            })
            return scope

        def _full(self) -> dict:
            scope = self._scope()
            return {
                "scope_id": scope["active_scope_id"],
                "universe_id": scope["universe_id"],
                "universe_generation": scope["generation"],
                "universe": {
                    "generation": scope["generation"],
                    "active_markets": scope["active_markets"],
                },
                "snapshot": {
                    "books": [{} for _ in range(scope["token_count"])],
                    "markets": [{} for _ in range(scope["market_count"])],
                    "unresolved_gaps": [],
                },
            }

        def get(self, url, **kwargs):
            if "/live/markets/2/snapshot" in url:
                return Response(200, {
                    "scan_universe_membership": True,
                    "stable_identity_for_position_monitoring": True,
                    "market": {
                        "market_id": "2",
                        "condition_id": second["condition_id"],
                        "outcomes": [
                            {"token_id": "21", "outcome": "Yes"},
                            {"token_id": "22", "outcome": "No"},
                        ],
                        "instrument_facts": {"end_at": second["end_at"]},
                    },
                })
            return super().get(url, **kwargs)

    candidate = _candidate(IDENTITY)
    candidate.update({
        "market_count": 2,
        "token_count": 4,
        "market_ids": ["1", "3"],
        "token_ids": ["11", "12", "31", "32"],
    })
    candidate["universe"].update({
        "active_markets": [IDENTITY, replacement],
        "added_markets": ["3"],
        "removed_markets": ["2"],
    })
    session = ConcurrentQuarantineSession(changed=True)

    def build_candidate(*_args, **_kwargs):
        session.quarantined_during_build = True
        return candidate

    config = _config(tmp_path)
    config = RefreshConfig(**{**config.__dict__, "target_market_count": 2})
    result = refresh_once(
        config,
        session=session,
        book_snapshot_builder=_books,
        universe_builder=build_candidate,
    )
    assert result["status"] == "activated_ready"
    assert result["added_markets"] == ["3"]
    assert result["removed_markets"] == ["2"]
    assert len(session.posts) == 1


def test_unready_live_books_can_be_isolated_by_fresh_candidate_activation(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("MARKETCOW_RUST_ADMIN_TOKEN", "secret")
    replacement = {**IDENTITY, "market_id": "2"}
    session = Session(changed=True, ready=False)
    result = refresh_once(
        _config(tmp_path), session=session, book_snapshot_builder=_books,
        universe_builder=lambda *_args, **_kwargs: _candidate(replacement),
    )
    assert result["status"] == "activated_ready"
    assert result["recovery_from_unready"] is True
    assert result["removed_markets"] == ["1"]
    assert result["added_markets"] == ["2"]
    assert len(session.posts) == 1


def test_missing_admin_token_fails_before_external_read(tmp_path, monkeypatch):
    monkeypatch.delenv("MARKETCOW_RUST_ADMIN_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="ADMIN_TOKEN"):
        refresh_once(_config(tmp_path), session=Session())


def test_config_rejects_non_loopback_activation(tmp_path):
    payload = {
        "schema_version": CONFIG_SCHEMA_VERSION,
        "service_url": "https://example.com",
        "candidate_manifest": "a",
        "catalog_manifest": "b",
        "startup_scope": "c",
        "scope_registry_root": "e",
        "work_root": "f",
        "audit_result": "g",
        "target_market_count": 100,
        "minimum_market_count": 80,
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="loopback"):
        _load_config(path)
