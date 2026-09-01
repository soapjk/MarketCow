from __future__ import annotations

import unittest
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from marketcow.polymarket_discovery import PolymarketDiscoveryStore
from marketcow.polymarket_live import GammaLiveNormalizer, LiveStateStore
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
            first = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/snapshot",
                params={"page_size": 60},
            )
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
            replacement = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/snapshot",
                params={"page_size": 60},
            ).json()
            self.assertNotEqual(replacement["snapshot_id"], snapshot_id)

    def test_quote_events_resume_after_snapshot_and_metadata_does_not_invent_resolution(self):
        writer = self.writer([gamma_row()])
        with TestClient(self.app()) as client:
            snap = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/snapshot"
            ).json()
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
            snap = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/snapshot"
            ).json()
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
