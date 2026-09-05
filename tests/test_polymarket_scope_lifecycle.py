from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from fastapi.testclient import TestClient

from marketcow.polymarket_live import (
    ClobBooksClient,
    ClobBooksCoverageError,
    GammaLiveNormalizer,
    LiveStateStore,
    PolymarketLiveCollector,
    PolymarketLiveReadStore,
    PolymarketOpenPositionError,
    PolymarketOpenPositionGuard,
)
from marketcow.polymarket_live_stream import PolymarketLiveProjection, STREAM_SCHEMA
from marketcow.polymarket_live_read_api import create_polymarket_live_read_app
from marketcow.polymarket_scopes import (
    PolymarketScopeRegistry,
    ScopeTransitionError,
    build_explicit_scope_manifest,
    scope_content_id,
    write_scope_runtime,
)
from tests.test_polymarket_live import NOW, Response, gamma_row, snapshot


class CatalogRows(list):
    def cleanup(self) -> None:
        pass


class CatalogClient:
    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.exact_requests: list[set[str]] = []

    def fetch_all(self):
        return CatalogRows(self.rows), {"source": "test"}

    def fetch_market_ids(self, market_ids):
        requested = set(market_ids)
        self.exact_requests.append(requested)
        return CatalogRows([
            row for row in self.rows if str(row.get("id")) in requested
        ]), {"source": "test-exact"}


def terminal_row(market_id: str, condition: str, tokens: tuple[str, str]) -> dict:
    row = gamma_row(market_id, condition, tokens)
    row.update({
        "active": False,
        "closed": True,
        "acceptingOrders": False,
        "resolution": "Yes",
    })
    return row


def acceptance_observation(*, cursor_start: int = 10, cursor_end: int = 11) -> dict:
    return {
        "http_status": 200,
        "status": "index_ready",
        "market_count": 100,
        "book_count": 200,
        "complete_market_count": 100,
        "tick_consistent_token_count": 200,
        "gap_count": 0,
        "disconnect_count": 0,
        "cursors": [cursor_start, cursor_end],
    }


def exact_manifest(prefix: str) -> dict:
    manifest = {
        "schema": "tradude.prediction_market.scope_manifest.v1",
        "catalog_revision": f"catalog-{prefix}",
        "candidate_snapshot_id": f"candidate-{prefix}",
        "market_ids": [f"{prefix}-{index:03d}" for index in range(100)],
    }
    manifest["scope_id"] = scope_content_id(manifest)
    return manifest


def test_clob_omission_is_typed_and_preserves_missing_token_ids() -> None:
    client = ClobBooksClient(
        requester=lambda *_args, **_kwargs: Response([]),
        max_retries_per_batch=0,
    )
    with pytest.raises(ClobBooksCoverageError) as raised:
        client.fetch_stream(["a", "b"], require_complete_batches=True)
    assert raised.value.missing_token_ids == ("a", "b")


def test_elapsed_gamma_end_time_is_not_fabricated_terminal_evidence() -> None:
    row = gamma_row()
    row["endDate"] = "2026-08-01T00:00:00Z"
    market = GammaLiveNormalizer.normalize([row], NOW)[0]
    assert market.lifecycle_state == "active"
    assert market.active is True
    assert market.accepting_orders is True
    assert market.end_at == datetime(2026, 8, 1, tzinfo=timezone.utc)
    assert market.terminal_at is None
    assert market.lifecycle_source is None
    assert market.lifecycle_evidence_sha256 is None


def test_startup_refreshes_but_does_not_close_elapsed_market_without_source_fact() -> None:
    with TemporaryDirectory() as folder:
        earlier = NOW - timedelta(hours=2)
        row = gamma_row("m1", "0x" + "1" * 64, ("yes-1", "no-1"))
        row["endDate"] = (NOW - timedelta(hours=1)).isoformat()
        store = LiveStateStore(Path(folder), now_provider=lambda: NOW)
        store.replace_catalog(GammaLiveNormalizer.normalize([row], earlier), [row])
        catalog = CatalogClient([row])
        collector = PolymarketLiveCollector(
            store,
            catalog,
            ClobBooksClient(requester=lambda *_args, **_kwargs: Response([])),
        )

        reconciled = collector.reconcile_elapsed_markets(
            reason="startup:elapsed_end"
        )

        assert reconciled == set()
        assert catalog.exact_requests == [{"m1"}]
        assert store.catalog["m1"].lifecycle_state == "active"
        assert store.catalog["m1"].terminal_at is None
        assert store.catalog["m1"].lifecycle_source is None
        assert "yes-1" in store.token_to_market


def test_terminal_omission_is_isolated_and_retains_audit_and_last_books() -> None:
    with TemporaryDirectory() as folder:
        root = Path(folder)
        active = gamma_row("m1", "0x" + "1" * 64, ("yes-1", "no-1"))
        expiring = gamma_row("m2", "0x" + "2" * 64, ("yes-2", "no-2"))
        observed = [
            active,
            terminal_row("m2", "0x" + "2" * 64, ("yes-2", "no-2")),
        ]
        store = LiveStateStore(root, now_provider=lambda: NOW)
        store.replace_catalog(
            GammaLiveNormalizer.normalize([active, expiring], NOW),
            [active, expiring],
        )
        for token, bid, ask in (
            ("yes-1", "0.40", "0.42"), ("no-1", "0.58", "0.60"),
            ("yes-2", "0.70", "0.72"), ("no-2", "0.28", "0.30"),
        ):
            store.apply_snapshot(snapshot(token, bid, ask), received_at=NOW)

        def books_request(_url, **kwargs):
            requested = {item["token_id"] for item in kwargs["json"]}
            rows = []
            if "yes-1" in requested:
                rows.append(snapshot("yes-1", "0.40", "0.42"))
            if "no-1" in requested:
                rows.append(snapshot("no-1", "0.58", "0.60"))
            return Response(rows)

        collector = PolymarketLiveCollector(
            store,
            CatalogClient(observed),
            ClobBooksClient(requester=books_request, max_retries_per_batch=0),
            publish_checkpoints=False,
        )
        asyncio.run(collector.refresh_books())

        terminal = store.catalog["m2"]
        assert terminal.lifecycle_state == "resolved"
        assert terminal.terminal_at == NOW
        assert terminal.lifecycle_source == "polymarket_gamma"
        assert terminal.lifecycle_evidence_sha256
        assert set(store.token_to_market) == {"yes-1", "no-1"}
        assert {"yes-2", "no-2"}.issubset(store.books)
        assert store.frame("m1").status == "ready"
        assert store.frame("m2").status == "terminal"
        assert not [gap for gap in store.gaps if not gap.resolved]
        assert any(event.event_type == "market_terminal" for event in store.events)


def test_active_clob_omission_remains_a_real_data_failure() -> None:
    with TemporaryDirectory() as folder:
        row = gamma_row()
        store = LiveStateStore(Path(folder), now_provider=lambda: NOW)
        store.replace_catalog(GammaLiveNormalizer.normalize([row], NOW), [row])
        collector = PolymarketLiveCollector(
            store,
            CatalogClient([row]),
            ClobBooksClient(
                requester=lambda *_args, **_kwargs: Response([]),
                max_retries_per_batch=0,
            ),
            publish_checkpoints=False,
        )
        with pytest.raises(ClobBooksCoverageError):
            asyncio.run(collector.bootstrap_books())
        assert store.catalog["m1"].lifecycle_state == "active"
        assert set(store.token_to_market) == {"yes-1", "no-1"}
        assert not any(event.event_type == "market_terminal" for event in store.events)


def test_periodic_omission_does_not_start_unbounded_gamma_reconciliation() -> None:
    with TemporaryDirectory() as folder:
        row = gamma_row()
        catalog = CatalogClient([row])
        store = LiveStateStore(Path(folder), now_provider=lambda: NOW)
        store.replace_catalog(GammaLiveNormalizer.normalize([row], NOW), [row])
        collector = PolymarketLiveCollector(
            store,
            catalog,
            ClobBooksClient(
                requester=lambda *_args, **_kwargs: Response([]),
                max_retries_per_batch=0,
            ),
            publish_checkpoints=False,
        )

        cursor = store.cursor
        asyncio.run(collector.refresh_books(
            reconcile_terminal_omissions=False,
        ))

        assert catalog.exact_requests == []
        assert store.cursor == cursor
        assert not [gap for gap in store.gaps if not gap.resolved]


def test_hot_health_reports_terminal_without_exact_ready_or_global_failure() -> None:
    with TemporaryDirectory() as folder:
        root = Path(folder)
        first = gamma_row("m1", "0x" + "1" * 64, ("yes-1", "no-1"))
        second = terminal_row("m2", "0x" + "2" * 64, ("yes-2", "no-2"))
        store = LiveStateStore(root, now_provider=lambda: NOW)
        store.replace_catalog(
            GammaLiveNormalizer.normalize([first, second], NOW), [first, second],
        )
        store.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        store.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
        projection = PolymarketLiveProjection()
        projection.install_state({
            "schema_version": STREAM_SCHEMA,
            "type": "state",
            "catalog_revision": store.catalog_revision,
            "scope_id": "a" * 64,
            "catalog_source": store.catalog_source,
            "latest_cursor": store.cursor,
            "persisted_cursor": store.cursor,
            "markets": [item.model_dump(mode="json") for item in store.catalog.values()],
            "books": [item.model_dump(mode="json") for item in store.books.values()],
            "gaps": [],
        })
        projection.mark_ready({"latest_cursor": store.cursor})
        reader = PolymarketLiveReadStore(
            root,
            now_provider=lambda: NOW,
            stable_snapshot_max_book_age_seconds=5,
            consumer_maximum_book_age_seconds=5,
        )
        health = projection.health(reader, ["m1", "m2"])
        assert health.status == "degraded"
        assert health.scope_status == "terminal_degraded"
        assert health.scope_id == "a" * 64
        assert health.active_market_count == 1
        assert health.terminal_market_count == 1
        assert health.complete_market_count == 1
        assert health.missing_market_count == 0
        assert health.status != "index_ready"


def test_pretrade_guard_rejects_terminal_and_stale_books_with_audit() -> None:
    with TemporaryDirectory() as folder:
        root = Path(folder)
        terminal = GammaLiveNormalizer.normalize([
            terminal_row("m1", "0x" + "1" * 64, ("yes-1", "no-1"))
        ], NOW)[0]
        store = LiveStateStore(root, now_provider=lambda: NOW)
        store._recovered = True
        store.catalog = {"m1": terminal}
        frame = store.frame("m1")
        guard = PolymarketOpenPositionGuard(
            root / "pretrade-audit.jsonl", now_provider=lambda: NOW,
        )
        with pytest.raises(PolymarketOpenPositionError) as raised:
            guard.require_eligible(terminal, frame, request_id="request-1")
        assert raised.value.code == "polymarket_open_position_market_resolved"
        audit = json.loads((root / "pretrade-audit.jsonl").read_text())
        assert audit["decision_code"] == raised.value.code
        assert audit["audit_id"] == raised.value.audit_id

        active_row = gamma_row("m2", "0x" + "2" * 64, ("yes-2", "no-2"))
        active = GammaLiveNormalizer.normalize([active_row], NOW)[0]
        store.catalog = {"m2": active}
        stale_frame = store.frame("m2")
        with pytest.raises(PolymarketOpenPositionError) as stale:
            guard.require_eligible(active, stale_frame, request_id="request-2")
        assert stale.value.code == "polymarket_open_position_fresh_book_required"

        closed_row = gamma_row("m3", "0x" + "3" * 64, ("yes-3", "no-3"))
        closed_row.update({"active": False, "closed": True, "acceptingOrders": False})
        closed = GammaLiveNormalizer.normalize([closed_row], NOW)[0]
        store.catalog = {"m3": closed}
        with pytest.raises(PolymarketOpenPositionError) as terminal:
            guard.require_eligible(
                closed, store.frame("m3"), request_id="request-3",
            )
        assert terminal.value.code == "polymarket_open_position_market_terminal"


def test_partial_active_refresh_failure_preserves_last_state_and_fails_closed() -> None:
    with TemporaryDirectory() as folder:
        current = [NOW]
        first = gamma_row("m1", "0x" + "1" * 64, ("yes-1", "no-1"))
        second = gamma_row("m2", "0x" + "2" * 64, ("yes-2", "no-2"))
        store = LiveStateStore(
            Path(folder), now_provider=lambda: current[0],
        )
        store.replace_catalog(
            GammaLiveNormalizer.normalize([first, second], NOW), [first, second],
        )
        for token, bid, ask in (
            ("yes-1", "0.40", "0.42"), ("no-1", "0.58", "0.60"),
            ("yes-2", "0.70", "0.72"), ("no-2", "0.28", "0.30"),
        ):
            store.apply_snapshot(snapshot(token, bid, ask), received_at=NOW)
        previous_checksums = {
            token: book.state_checksum for token, book in store.books.items()
        }
        current[0] += timedelta(seconds=10)

        def partial(_url, **kwargs):
            requested = {item["token_id"] for item in kwargs["json"]}
            return Response([
                snapshot(token, "0.40", "0.42")
                for token in sorted(requested & {"yes-1", "no-1"})
            ])

        collector = PolymarketLiveCollector(
            store,
            CatalogClient([first, second]),
            ClobBooksClient(requester=partial, max_retries_per_batch=0),
            publish_checkpoints=False,
        )
        with pytest.raises(ClobBooksCoverageError):
            asyncio.run(collector.refresh_books())
        assert {
            token: book.state_checksum for token, book in store.books.items()
        } == previous_checksums
        assert all(
            market.lifecycle_state == "active" for market in store.catalog.values()
        )
        assert not any(event.event_type == "market_terminal" for event in store.events)


def test_candidate_requires_both_exact_advancing_endpoints_before_atomic_switch() -> None:
    with TemporaryDirectory() as folder:
        now = [datetime(2026, 8, 27, tzinfo=timezone.utc)]
        registry = PolymarketScopeRegistry(
            Path(folder), now_provider=lambda: now[0],
        )
        first, second = exact_manifest("a"), exact_manifest("b")
        registry.prepare(first)
        registry.prepare(second)
        failed = {
            "endpoints": {
                "8790": acceptance_observation(),
                "8791": acceptance_observation(cursor_start=10, cursor_end=10),
            },
        }
        with pytest.raises(ScopeTransitionError) as raised:
            registry.accept(second["scope_id"], failed)
        assert raised.value.code == "polymarket_candidate_acceptance_failed"
        with pytest.raises(ScopeTransitionError) as unaccepted:
            registry.activate(second["scope_id"])
        assert unaccepted.value.code == "polymarket_candidate_not_accepted"

        evidence = {
            "endpoints": {
                "8790": acceptance_observation(),
                "8791": acceptance_observation(
                    cursor_start=20, cursor_end=21,
                ),
            },
        }
        registry.accept(first["scope_id"], evidence)
        registry.accept(second["scope_id"], evidence)
        registry.activate(first["scope_id"])
        pointer = registry.activate(second["scope_id"], grace_seconds=60)
        assert pointer["active_scope_id"] == second["scope_id"]
        assert pointer["previous_scope_id"] == first["scope_id"]
        assert registry.resolve(first["scope_id"], "a-000")["scope_status"] == "grace"

        rolled_back = registry.rollback(grace_seconds=60)
        assert rolled_back["active_scope_id"] == first["scope_id"]
        assert registry.active() == rolled_back
        now[0] += timedelta(seconds=61)
        with pytest.raises(ScopeTransitionError) as retired:
            registry.resolve(second["scope_id"], "b-000")
        assert retired.value.code == "polymarket_scope_retired"


def test_scope_generation_preserves_tradude_explicit_order_without_ranking() -> None:
    selection = {
        "schema": "tradude.prediction_market.scope_selection.v2",
        "discovery_snapshot_id": "a" * 64,
        "catalog_revision": "b" * 64,
        "selection_evidence_sha256": "c" * 64,
        "replaces_scope_id": "f" * 64,
        "market_ids": [f"m{index:03d}" for index in reversed(range(100))],
        "relations": [],
    }
    replacement = build_explicit_scope_manifest(
        selection, generated_at_ns=1_000_000_000,
    )
    assert len(replacement["market_ids"]) == 100
    assert replacement["market_ids"] == selection["market_ids"]
    assert replacement["scope_id"] == scope_content_id(replacement)
    assert replacement["replaces_scope_id"] == selection["replaces_scope_id"]


def test_explicit_scope_rejects_partial_relation_members() -> None:
    selection = {
        "schema": "tradude.prediction_market.scope_selection.v2",
        "discovery_snapshot_id": "a" * 64,
        "catalog_revision": "b" * 64,
        "selection_evidence_sha256": "c" * 64,
        "replaces_scope_id": "f" * 64,
        "market_ids": ["m1", "m2"],
        "relations": [{
            "relation_id": "neg-risk:g1",
            "member_market_ids": ["m1"],
            "expected_member_count": 2,
            "actual_member_count": 1,
            "complete": False,
        }],
    }
    with pytest.raises(ScopeTransitionError) as raised:
        build_explicit_scope_manifest(selection, generated_at_ns=1)
    assert raised.value.code == "polymarket_scope_relation_incomplete"


def test_prepared_scope_artifacts_are_immutable() -> None:
    with TemporaryDirectory() as folder:
        registry = PolymarketScopeRegistry(Path(folder))
        manifest = exact_manifest("a")
        registry.prepare(manifest)
        changed = dict(manifest)
        changed["market_ids"] = list(manifest["market_ids"][:-1]) + ["other"]
        with pytest.raises(ScopeTransitionError) as raised:
            registry.prepare(changed)
        assert raised.value.code == "polymarket_scope_content_mismatch"


def test_scope_discovery_and_stale_scope_id_return_stable_410() -> None:
    with TemporaryDirectory() as folder:
        root = Path(folder)
        write_scope_runtime(
            root, scope_id="a" * 64, manifest_sha256="b" * 64,
        )
        app = create_polymarket_live_read_app(
            root=root,
            discovery_root=root,
            stable_snapshot_max_book_age_seconds=5,
            stable_read_wait_seconds=1,
            stable_read_poll_seconds=0.01,
            executor_workers=2,
            discovery_depth_notionals=("10", "50", "100", "500"),
            discovery_maximum_book_age_ms=5000,
        )
        with TestClient(app) as client:
            discovery = client.get(
                "/v1/prediction-markets/polymarket/live/scope"
            )
            retired = client.get(
                "/v1/prediction-markets/polymarket/live/bootstrap",
                params={"market_id": "cached-market", "scope_id": "c" * 64},
            )
        assert discovery.status_code == 200
        assert discovery.json()["active_scope_id"] == "a" * 64
        assert retired.status_code == 410
        assert retired.json()["detail"]["code"] == "polymarket_scope_retired"
