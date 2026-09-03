from __future__ import annotations

import unittest
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from marketcow.polymarket_discovery import PolymarketDiscoveryStore
from marketcow.polymarket_live import (
    GammaLiveNormalizer,
    GammaRealtimeUniversePolicy,
    LiveStateStore,
)
from marketcow.polymarket_live_read_api import create_polymarket_live_read_app
from tests.test_polymarket_live import gamma_row, snapshot


NOW = datetime(2026, 8, 15, 10, 0, tzinfo=timezone.utc)


class PolymarketDiscoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name) / "live"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def writer(self, rows: list[dict]) -> LiveStateStore:
        writer = LiveStateStore(self.root, now_provider=lambda: NOW)
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        for row in rows:
            yes, no = row["clobTokenIds"].strip("[]").replace('"', "").split(",")
            writer.apply_snapshot(
                snapshot(yes.strip(), "0.40", "0.42"), received_at=NOW
            )
            writer.apply_snapshot(
                snapshot(no.strip(), "0.58", "0.60"), received_at=NOW
            )
        return writer

    def app(self):
        app = create_polymarket_live_read_app(
            root=self.root,
            discovery_root=self.root,
            stable_snapshot_max_book_age_seconds=5,
            stable_read_wait_seconds=1,
            stable_read_poll_seconds=0.01,
            executor_workers=4,
            discovery_depth_notionals=("1", "10"),
            discovery_maximum_book_age_ms=5000,
        )
        app.state.polymarket_live_read.now_provider = lambda: NOW
        return app

    def ready_snapshot(self, client: TestClient, **params):
        for _ in range(200):
            response = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/snapshot",
                params=params,
            )
            if response.status_code == 200:
                return response
            self.assertEqual(response.status_code, 503, response.text)
            time.sleep(0.01)
        self.fail("discovery snapshot did not finish materializing")

    def test_materialization_exposes_only_bounded_realtime_universe(self):
        rows = [
            gamma_row(
                f"m{index}", f"0x{index:064x}",
                (f"yes-{index}", f"no-{index}"),
            )
            for index in range(1, 4)
        ]
        for index, row in enumerate(rows, start=1):
            row.update({
                "enableOrderBook": True,
                "volume24hrClob": str(index),
                "liquidityClob": str(index * 10),
            })
        writer = LiveStateStore(self.root, now_provider=lambda: NOW)
        markets = GammaLiveNormalizer.normalize(rows, NOW)
        writer.replace_catalog(markets, rows)
        universe = GammaRealtimeUniversePolicy(2).select(
            markets,
            rows,
            catalog_revision=str(writer.catalog_revision),
        )
        writer.replace_realtime_universe(universe)
        for index in (2, 3):
            writer.apply_snapshot(
                snapshot(f"yes-{index}", "0.40", "0.42"),
                received_at=NOW,
            )
            writer.apply_snapshot(
                snapshot(f"no-{index}", "0.58", "0.60"),
                received_at=NOW,
            )

        with TestClient(self.app()) as client:
            body = self.ready_snapshot(client, limit=100).json()
            replacement = GammaRealtimeUniversePolicy(1).select(
                markets,
                rows,
                catalog_revision=str(writer.catalog_revision),
            )
            writer.replace_realtime_universe(replacement)
            expired = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/snapshot",
                params={
                    "snapshot_id": body["snapshot_id"],
                    "page_size": 100,
                },
            )
            replacement_body = self.ready_snapshot(client, limit=100).json()

        self.assertEqual(len(writer.catalog), 3)
        self.assertEqual(body["active_market_count"], 2)
        self.assertEqual(
            {item["market_id"] for item in body["items"]},
            {"m2", "m3"},
        )
        self.assertEqual(expired.status_code, 410)
        self.assertEqual(replacement_body["active_market_count"], 1)
        self.assertEqual(replacement_body["items"][0]["market_id"], "m3")

    def test_explicit_depth_configuration_is_mandatory_and_ordered(self):
        rows = [gamma_row()]
        self.writer(rows)
        app = self.app()
        reader = app.state.polymarket_live_read
        with self.assertRaisesRegex(ValueError, "explicit depth"):
            PolymarketDiscoveryStore(
                reader, depth_notionals=(), maximum_book_age_ms=5000
            )
        with self.assertRaisesRegex(ValueError, "strictly increasing"):
            PolymarketDiscoveryStore(
                reader,
                depth_notionals=("50", "10"),
                maximum_book_age_ms=5000,
            )

    def test_initial_materialization_is_single_flight_and_never_blocks_health(self):
        self.writer([gamma_row()])
        app = self.app()
        discovery = app.state.polymarket_discovery
        original = discovery._full_materialize
        started = threading.Event()
        release = threading.Event()
        calls = 0

        def blocked_materialization():
            nonlocal calls
            calls += 1
            started.set()
            release.wait(5)
            return original()

        discovery._full_materialize = blocked_materialization
        with TestClient(app) as client:
            self.assertTrue(started.wait(1))
            requested_at = time.monotonic()
            responses = [
                client.get(
                    "/v1/prediction-markets/polymarket/live/discovery/snapshot",
                    params={"page_size": 1},
                )
                for _ in range(5)
            ]
            self.assertLess(time.monotonic() - requested_at, 1)
            self.assertTrue(all(response.status_code == 503 for response in responses))
            self.assertTrue(all(
                response.headers.get("retry-after") == "1"
                for response in responses
            ))
            self.assertTrue(all(
                response.json()["detail"]["code"]
                == "discovery_snapshot_materializing"
                for response in responses
            ))
            health_started = time.monotonic()
            health = client.get(
                "/v1/prediction-markets/polymarket/live/health"
            )
            self.assertLess(time.monotonic() - health_started, 1)
            self.assertEqual(health.status_code, 200, health.text)
            self.assertEqual(calls, 1)
            release.set()
            for _ in range(100):
                ready = client.get(
                    "/v1/prediction-markets/polymarket/live/discovery/snapshot",
                    params={"page_size": 1},
                )
                if ready.status_code == 200:
                    break
                time.sleep(0.01)
            self.assertEqual(ready.status_code, 200, ready.text)
            boundary = discovery.boundary(ready.json()["snapshot_id"])
            self.assertFalse(hasattr(boundary, "quotes"))

    def test_published_boundary_is_restored_without_a_request_time_rebuild(self):
        self.writer([gamma_row()])
        with TestClient(self.app()) as first_client:
            first = self.ready_snapshot(first_client, page_size=1).json()

        restored_app = self.app()
        restored = restored_app.state.polymarket_discovery
        self.assertEqual(
            restored.materialization_status()["snapshot_id"], first["snapshot_id"]
        )
        with TestClient(restored_app) as client:
            response = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/snapshot",
                params={"page_size": 1},
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["snapshot_id"], first["snapshot_id"])

    def test_materialization_lock_is_shared_across_api_instances(self):
        self.writer([gamma_row()])
        first = self.app().state.polymarket_discovery
        second = self.app().state.polymarket_discovery
        original_first = first._full_materialize
        original_second = second._full_materialize
        started = threading.Event()
        release = threading.Event()
        second_full_builds = 0

        def blocked_first():
            started.set()
            release.wait(5)
            return original_first()

        def counted_second():
            nonlocal second_full_builds
            second_full_builds += 1
            return original_second()

        first._full_materialize = blocked_first
        second._full_materialize = counted_second
        first.start_background_materialization()
        self.assertTrue(started.wait(1))
        second.start_background_materialization()
        time.sleep(0.05)
        self.assertEqual(second_full_builds, 0)
        release.set()
        try:
            for _ in range(200):
                first_snapshot = first.materialization_status()["snapshot_id"]
                second_snapshot = second.materialization_status()["snapshot_id"]
                if first_snapshot and second_snapshot == first_snapshot:
                    break
                time.sleep(0.01)
            self.assertIsNotNone(first_snapshot)
            self.assertEqual(second_snapshot, first_snapshot)
            self.assertEqual(second_full_builds, 0)
        finally:
            first.stop_background_materialization()
            second.stop_background_materialization()

    def test_snapshot_exceeds_100_and_pages_remain_on_one_atomic_boundary(self):
        rows = [
            gamma_row(
                f"m{index:03d}",
                "0x" + f"{index + 1:064x}",
                (f"yes-{index:03d}", f"no-{index:03d}"),
            )
            for index in range(101)
        ]
        writer = self.writer(rows)
        with TestClient(self.app()) as client:
            first = self.ready_snapshot(client, page_size=60)
            self.assertEqual(first.status_code, 200, first.text)
            first_body = first.json()
            self.assertEqual(first_body["active_market_count"], 101)
            self.assertEqual(first_body["page_count"], 60)
            def all_keys(value):
                if isinstance(value, dict):
                    return set(value) | {
                        key
                        for child in value.values()
                        for key in all_keys(child)
                    }
                if isinstance(value, list):
                    return {key for child in value for key in all_keys(child)}
                return set()

            self.assertTrue(
                {"edge", "expected_profit", "apy", "score", "rank"}.isdisjoint(
                    {key.lower() for key in all_keys(first_body)}
                )
            )
            snapshot_id = first_body["snapshot_id"]
            boundary_cursor = first_body["boundary_cursor"]
            self.assertTrue(all(
                item["cursor"] == boundary_cursor
                for item in first_body["items"]
            ))
            old_revision = next(
                item["book_revision"] for item in first_body["items"]
                if item["market_id"] == "m000"
            )

            writer.apply_snapshot(
                snapshot("yes-000", "0.41", "0.43", "1785739201000"),
                received_at=NOW + timedelta(milliseconds=1),
            )
            second = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/snapshot",
                params={
                    "snapshot_id": snapshot_id,
                    "page_cursor": first_body["next_page_cursor"],
                    "page_size": 60,
                },
            )
            self.assertEqual(second.status_code, 200, second.text)
            self.assertEqual(second.json()["snapshot_id"], snapshot_id)
            self.assertEqual(second.json()["boundary_cursor"], boundary_cursor)
            self.assertEqual(second.json()["page_count"], 41)
            self.assertTrue(all(
                item["cursor"] == boundary_cursor
                for item in second.json()["items"]
            ))

            immutable_first = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/snapshot",
                params={"snapshot_id": snapshot_id, "page_size": 60},
            ).json()
            self.assertEqual(
                next(
                    item["book_revision"] for item in immutable_first["items"]
                    if item["market_id"] == "m000"
                ),
                old_revision,
            )
            for _ in range(200):
                replacement_response = client.get(
                    "/v1/prediction-markets/polymarket/live/discovery/snapshot",
                    params={"page_size": 60},
                )
                replacement = replacement_response.json()
                if replacement.get("snapshot_id") != snapshot_id:
                    break
                time.sleep(0.01)
            self.assertNotEqual(replacement["snapshot_id"], snapshot_id)
            replacement_boundary = client.app.state.polymarket_discovery.boundary(
                replacement["snapshot_id"]
            )
            with sqlite3.connect(replacement_boundary.database_path) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM quote_versions"
                    ).fetchone()[0],
                    102,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM quote_versions WHERE market_id='m000'"
                    ).fetchone()[0],
                    2,
                )

            discovery = client.app.state.polymarket_discovery
            discovery.stop_background_materialization()
            original_materialize_once = discovery.materialize_once
            materialization_calls = []
            discovery.materialize_once = lambda: materialization_calls.append(True)
            try:
                cached_page = client.get(
                    "/v1/prediction-markets/polymarket/live/discovery/snapshot",
                    params={
                        "snapshot_id": replacement["snapshot_id"],
                        "page_size": 1,
                    },
                )
            finally:
                discovery.materialize_once = original_materialize_once
            self.assertEqual(cached_page.status_code, 200, cached_page.text)
            self.assertEqual(materialization_calls, [])

    def test_non_yes_no_market_is_materialized_and_fails_closed_per_market(self):
        row = gamma_row(tokens=("team-a-token", "team-b-token"))
        row["outcomes"] = '["Team A","Team B"]'
        self.writer([row])

        with TestClient(self.app()) as client:
            response = self.ready_snapshot(client, page_size=1)

        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["active_market_count"], 1)
        quote = body["items"][0]
        self.assertIsNone(quote["yes_token_id"])
        self.assertIsNone(quote["no_token_id"])
        self.assertEqual(quote["book_status"], "missing_outcome_identity")
        self.assertEqual(
            {item["outcome"] for item in quote["outcomes"]},
            {"Team A", "Team B"},
        )
        self.assertTrue(
            {"yes_token_id", "no_token_id"}.issubset(quote["missing_fields"])
        )

    def test_quote_events_resume_after_snapshot_and_metadata_does_not_invent_resolution(self):
        writer = self.writer([gamma_row()])
        with TestClient(self.app()) as client:
            snap = self.ready_snapshot(client).json()
            metadata = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/metadata",
                params={"snapshot_id": snap["snapshot_id"]},
            )
            self.assertEqual(metadata.status_code, 200, metadata.text)
            fact = metadata.json()["items"][0]
            self.assertIsNone(fact["resolved_at"])
            self.assertIsNone(fact["redeemable_at"])
            self.assertIn("resolved_at", fact["missing_fields"])
            self.assertNotEqual(fact["event_end_at"], fact["resolved_at"])

            writer.apply_snapshot(
                snapshot("yes-1", "0.41", "0.43", "1785739201000"),
                received_at=NOW + timedelta(milliseconds=1),
            )
            events = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/events",
                params={"after_cursor": snap["boundary_cursor"]},
            )
            self.assertEqual(events.status_code, 200, events.text)
            body = events.json()
            self.assertFalse(body["resync_required"])
            self.assertEqual(len(body["items"]), 1)
            self.assertEqual(body["items"][0]["event_type"], "quote_changed")
            self.assertEqual(body["items"][0]["token_id"], "yes-1")
            self.assertEqual(body["items"][0]["quote"]["outcome"], "YES")

    def test_negative_risk_relation_is_complete_and_quoted_atomically(self):
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("yes-1", "no-1"), neg_risk=True),
            gamma_row("m2", "0x" + "2" * 64, ("yes-2", "no-2"), neg_risk=True),
        ]
        self.writer(rows)
        with TestClient(self.app()) as client:
            snap = self.ready_snapshot(client).json()
            relation = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/relations/neg-risk:neg-group",
                params={"snapshot_id": snap["snapshot_id"]},
            )
        self.assertEqual(relation.status_code, 200, relation.text)
        body = relation.json()
        self.assertTrue(body["complete"])
        self.assertEqual(body["expected_member_count"], 2)
        self.assertEqual(body["actual_member_count"], 2)
        self.assertEqual(len(body["quotes"]), 2)

    def test_cursor_expiry_and_websocket_resync_are_explicit(self):
        writer = self.writer([gamma_row()])
        with sqlite3.connect(writer.state_index.path) as connection:
            connection.execute("DELETE FROM event_offsets WHERE cursor < 3")
            connection.commit()
        with TestClient(self.app()) as client:
            expired = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/events",
                params={"after_cursor": 1},
            )
            self.assertEqual(expired.status_code, 410)
            self.assertEqual(
                expired.json()["detail"]["code"], "discovery_cursor_expired"
            )
            with client.websocket_connect(
                "/v1/prediction-markets/polymarket/live/discovery/stream?after_cursor=1"
            ) as socket:
                frame = socket.receive_json()
                self.assertEqual(frame["type"], "resync_required")
                self.assertEqual(frame["reason"], "discovery_cursor_expired")

    def test_lifecycle_history_only_emits_source_timestamped_facts(self):
        row = gamma_row()
        row.update({
            "active": False,
            "acceptingOrders": False,
            "closed": True,
            "resolution": "Yes",
            "resolutionProposedAt": "2026-08-15T09:00:00Z",
            "challengeDeadlineAt": "2026-08-16T09:00:00Z",
            "disputedAt": "2026-08-15T09:30:00Z",
            "resolvedAt": "2026-08-15T10:00:00Z",
            "redeemable": True,
            "redeemableAt": "2026-08-15T10:05:00Z",
            "closedAt": "2026-08-15T08:55:00Z",
            "resolutionEventId": "uma-event-1",
        })
        writer = LiveStateStore(self.root, now_provider=lambda: NOW)
        normalized = GammaLiveNormalizer.normalize([row], NOW)
        writer.replace_catalog(normalized, [row])
        with TestClient(self.app()) as client:
            history = client.get(
                "/v1/prediction-markets/polymarket/history/lifecycle-events",
                params={"market_id": "m1"},
            )
        self.assertEqual(history.status_code, 200, history.text)
        items = history.json()["items"]
        self.assertEqual(
            {item["event_type"] for item in items},
            {
                "market_closed",
                "resolution_proposed",
                "resolution_disputed",
                "market_resolved",
                "redemption_available",
            },
        )
        self.assertTrue(all(item["source_event_id"] == "uma-event-1" for item in items))
        self.assertEqual(
            next(item for item in items if item["event_type"] == "market_resolved")[
                "source_observed_at"
            ],
            "2026-08-15T10:00:00Z",
        )

    def test_openapi_publishes_v2_discovery_and_history_contracts(self):
        self.writer([gamma_row()])
        with TestClient(self.app()) as client:
            openapi = client.get("/openapi.json").json()
        for path in (
            "/v1/prediction-markets/polymarket/live/discovery/snapshot",
            "/v1/prediction-markets/polymarket/live/discovery/events",
            "/v1/prediction-markets/polymarket/live/discovery/metadata",
            "/v1/prediction-markets/polymarket/live/discovery/relations/{relation_id}",
            "/v1/prediction-markets/polymarket/history/lifecycle-events",
        ):
            self.assertIn(path, openapi["paths"])
        schemas = openapi["components"]["schemas"]
        self.assertIn("DiscoverySnapshotPage", schemas)
        self.assertIn("LifecycleHistoryPage", schemas)
        self.assertIn(
            "/v1/prediction-markets/polymarket/live/discovery/stream",
            openapi["x-websocket-paths"],
        )


if __name__ == "__main__":
    unittest.main()
