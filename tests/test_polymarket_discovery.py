from __future__ import annotations

import unittest
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from marketcow.polymarket_discovery import (
    DEFAULT_MAXIMUM_FULL_SYNC_BYTES,
    PolymarketDiscoveryStore,
)
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
            discovery_maximum_full_sync_bytes=DEFAULT_MAXIMUM_FULL_SYNC_BYTES,
        )
        app.state.polymarket_live_read.now_provider = lambda: NOW
        return app

    def ready_full_sync(self, client: TestClient):
        for _ in range(200):
            response = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/full-sync",
            )
            if response.status_code == 200:
                return response
            self.assertEqual(response.status_code, 503, response.text)
            time.sleep(0.01)
        self.fail("discovery full-sync did not finish materializing")

    def test_missing_discovery_startup_data_is_isolated_from_live_api(self):
        self.writer([gamma_row()])
        app = create_polymarket_live_read_app(
            root=self.root,
            discovery_root=self.root / "missing-discovery",
            stable_snapshot_max_book_age_seconds=5,
            stable_read_wait_seconds=1,
            stable_read_poll_seconds=0.01,
            executor_workers=4,
            discovery_depth_notionals=("1", "10"),
            discovery_maximum_book_age_ms=5000,
        )
        app.state.polymarket_live_read.now_provider = lambda: NOW

        with TestClient(app) as client:
            live = client.get(
                "/v1/prediction-markets/polymarket/live/health"
            )
            discovery = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/status"
            )
            full_sync = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/full-sync"
            )

        self.assertEqual(live.status_code, 200, live.text)
        self.assertEqual(discovery.status_code, 200, discovery.text)
        self.assertEqual(discovery.json()["state"], "failed")
        self.assertFalse(discovery.json()["ready"])
        self.assertIn("catalog is not configured", discovery.json()["error"])
        self.assertEqual(full_sync.status_code, 503)

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
            body = self.ready_full_sync(client).json()
            replacement = GammaRealtimeUniversePolicy(1).select(
                markets,
                rows,
                catalog_revision=str(writer.catalog_revision),
            )
            writer.replace_realtime_universe(replacement)
            replacement_body = self.ready_full_sync(client).json()
            frame = client.app.state.polymarket_discovery.events_page(
                body["projection_id"], body["boundary_cursor"], 1000
            )

        self.assertEqual(len(writer.catalog), 3)
        self.assertEqual(len(body["markets"]), 2)
        self.assertEqual(
            {item["market_id"] for item in body["markets"]},
            {"m2", "m3"},
        )
        self.assertNotEqual(
            replacement_body["projection_id"], body["projection_id"]
        )
        self.assertEqual(len(replacement_body["markets"]), 1)
        self.assertEqual(replacement_body["markets"][0]["market_id"], "m3")
        self.assertTrue(frame.resync_required)
        self.assertEqual(len(frame.items), 1)
        self.assertEqual(frame.items[0].type, "universe_changed")
        self.assertEqual(
            frame.items[0].payload.universe_revision,
            replacement_body["universe_revision"],
        )
        self.assertEqual(
            set(frame.items[0].model_dump(mode="json")),
            {"type", "payload"},
        )

    def test_explicit_depth_configuration_is_mandatory_and_ordered(self):
        rows = [gamma_row()]
        self.writer(rows)
        app = self.app()
        reader = app.state.polymarket_live_read
        with self.assertRaisesRegex(ValueError, "explicit depth"):
            PolymarketDiscoveryStore(
                reader, depth_notionals=(), maximum_book_age_ms=5000,
                maximum_full_sync_bytes=DEFAULT_MAXIMUM_FULL_SYNC_BYTES,
            )
        with self.assertRaisesRegex(ValueError, "strictly increasing"):
            PolymarketDiscoveryStore(
                reader,
                depth_notionals=("50", "10"),
                maximum_book_age_ms=5000,
                maximum_full_sync_bytes=DEFAULT_MAXIMUM_FULL_SYNC_BYTES,
            )

    def test_full_sync_byte_limit_returns_413_without_pagination(self):
        self.writer([gamma_row()])
        with TestClient(self.app()) as client:
            self.ready_full_sync(client)
            client.app.state.polymarket_discovery.maximum_full_sync_bytes = 1
            response = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/full-sync"
            )
        self.assertEqual(response.status_code, 413, response.text)
        self.assertEqual(
            response.json()["detail"]["code"],
            "discovery_full_sync_too_large",
        )

    def test_unresolved_gap_forces_full_sync_and_status_fail_closed(self):
        writer = self.writer([gamma_row()])
        with TestClient(self.app()) as client:
            baseline = self.ready_full_sync(client).json()
            writer.apply_websocket(
                {
                    "event_type": "last_trade_price",
                    "asset_id": "yes-1",
                    "timestamp": "1785739199000",
                    "price": "0.39",
                },
                received_at=NOW + timedelta(milliseconds=1),
            )
            discovery = client.app.state.polymarket_discovery
            discovery.materialize_once()
            full_sync = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/full-sync"
            ).json()
            status = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/status"
            ).json()
        self.assertEqual(full_sync["projection_id"], baseline["projection_id"])
        self.assertFalse(full_sync["ready"])
        self.assertGreater(full_sync["unresolved_gap_count"], 0)
        self.assertEqual(
            full_sync["fail_closed_reason"], "discovery_unresolved_gaps"
        )
        self.assertFalse(status["ready"])
        self.assertEqual(status["unresolved_gap_count"], full_sync["unresolved_gap_count"])
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
                    "/v1/prediction-markets/polymarket/live/discovery/full-sync",
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
                    "/v1/prediction-markets/polymarket/live/discovery/full-sync",
                )
                if ready.status_code == 200:
                    break
                time.sleep(0.01)
            self.assertEqual(ready.status_code, 200, ready.text)
            boundary = discovery.boundary(None)
            self.assertEqual(
                boundary.projection_id, ready.json()["projection_id"]
            )
            self.assertFalse(hasattr(boundary, "quotes"))

    def test_published_boundary_is_restored_without_a_request_time_rebuild(self):
        self.writer([gamma_row()])
        with TestClient(self.app()) as first_client:
            first = self.ready_full_sync(first_client).json()

        restored_app = self.app()
        restored = restored_app.state.polymarket_discovery
        self.assertEqual(
            restored.materialization_status()["projection_id"],
            first["projection_id"],
        )
        with TestClient(restored_app) as client:
            response = client.get(
                "/v1/prediction-markets/polymarket/live/discovery/full-sync",
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["projection_id"], first["projection_id"])

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

    def test_full_sync_exceeds_100_and_remains_one_atomic_boundary(self):
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
            first = self.ready_full_sync(client)
            self.assertEqual(first.status_code, 200, first.text)
            first_body = first.json()
            self.assertEqual(len(first_body["markets"]), 101)
            self.assertEqual(first_body["schema_version"], "marketcow.polymarket.discovery.v3")
            self.assertTrue(first_body["ready"])
            self.assertIsNone(first_body["fail_closed_reason"])
            self.assertEqual(first_body["unresolved_gap_count"], 0)
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
            projection_id = first_body["projection_id"]
            boundary_cursor = first_body["boundary_cursor"]
            self.assertTrue(all(
                item["cursor"] == boundary_cursor
                for item in first_body["markets"]
            ))
            old_revision = next(
                item["book_revision"] for item in first_body["markets"]
                if item["market_id"] == "m000"
            )

            writer.apply_snapshot(
                snapshot("yes-000", "0.41", "0.43", "1785739201000"),
                received_at=NOW + timedelta(milliseconds=1),
            )
            for _ in range(200):
                replacement_response = client.get(
                    "/v1/prediction-markets/polymarket/live/discovery/full-sync",
                )
                replacement = replacement_response.json()
                if replacement.get("boundary_cursor") != boundary_cursor:
                    break
                time.sleep(0.01)
            self.assertGreater(replacement["boundary_cursor"], boundary_cursor)
            self.assertEqual(replacement["projection_id"], projection_id)
            self.assertNotEqual(
                next(
                    item["book_revision"] for item in replacement["markets"]
                    if item["market_id"] == "m000"
                ),
                old_revision,
            )
            replacement_boundary = client.app.state.polymarket_discovery.boundary(None)
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

    def test_non_yes_no_market_is_materialized_and_fails_closed_per_market(self):
        row = gamma_row(tokens=("team-a-token", "team-b-token"))
        row["outcomes"] = '["Team A","Team B"]'
        self.writer([row])

        with TestClient(self.app()) as client:
            response = self.ready_full_sync(client)

        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(len(body["markets"]), 1)
        quote = body["markets"][0]
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

    def test_market_delta_contains_latest_complete_market_payload(self):
        writer = self.writer([gamma_row()])
        with TestClient(self.app()) as client:
            baseline = self.ready_full_sync(client).json()
            discovery = client.app.state.polymarket_discovery
            # The assertion below deliberately drives one synchronous
            # materialization. Stop the normal background owner first so it
            # cannot consume the two events between the write and that call.
            discovery.stop_background_materialization()

            writer.apply_snapshot(
                snapshot("yes-1", "0.41", "0.43", "1785739201000"),
                received_at=NOW + timedelta(milliseconds=1),
            )
            writer.apply_snapshot(
                snapshot("no-1", "0.57", "0.59", "1785739202000"),
                received_at=NOW + timedelta(milliseconds=2),
            )
            discovery.materialize_once()
            frame = discovery.events_page(
                baseline["projection_id"], baseline["boundary_cursor"], 1000
            )
        self.assertFalse(frame.resync_required)
        self.assertEqual(frame.next_cursor, frame.after_cursor + 1)
        self.assertGreater(frame.boundary_cursor, frame.next_cursor)
        self.assertEqual(len(frame.items), 1)
        self.assertEqual(frame.items[0].type, "market_update")
        self.assertEqual(frame.items[0].payload.market_id, "m1")
        self.assertEqual(frame.items[0].payload.cursor, frame.next_cursor)
        self.assertEqual(len(frame.items[0].payload.outcomes), 2)
        self.assertEqual(
            set(frame.items[0].model_dump(mode="json")),
            {"type", "payload"},
        )
        second = discovery.events_page(
            baseline["projection_id"], frame.next_cursor, 1000
        )
        self.assertEqual(second.next_cursor, second.after_cursor + 1)
        self.assertEqual(second.items[0].payload.cursor, second.next_cursor)

    def bounded_source(self, floor):
        """Synthetic migration fixture; never used for authoritative data."""
        with sqlite3.connect(self.root / "indexes/latest-state.sqlite3") as db:
            db.execute("CREATE TABLE recent_events(cursor INTEGER PRIMARY KEY,payload BLOB,sha256 TEXT)")
            with (self.root / "events.jsonl").open("rb") as stream:
                for cursor, offset, size, sha in db.execute(
                    "SELECT cursor,byte_offset,byte_length,line_sha256 FROM event_offsets WHERE cursor>?", (floor,)
                ).fetchall():
                    stream.seek(offset)
                    db.execute("INSERT INTO recent_events VALUES(?,?,?)", (cursor, stream.read(size), sha))
            size = db.execute("SELECT SUM(length(payload)) FROM recent_events").fetchone()[0]
            db.executemany("INSERT OR REPLACE INTO metadata VALUES(?,?)", [
                ("bounded_history_bytes", str(64 * 1024 * 1024)),
                ("history_floor_cursor", str(floor)), ("recent_event_bytes", str(size)),
            ])
            db.execute("DELETE FROM event_offsets")
        (self.root / "events.jsonl").unlink()

    def test_bounded_source_incremental_without_jsonl(self):
        writer = self.writer([gamma_row()])
        with TestClient(self.app()) as client:
            baseline = self.ready_full_sync(client).json()
            discovery = client.app.state.polymarket_discovery
            discovery.stop_background_materialization()
            writer.apply_snapshot(snapshot("yes-1", "0.41", "0.43", "1785739201000"), received_at=NOW)
            self.bounded_source(baseline["boundary_cursor"])
            discovery.materialize_once()
            frame = discovery.events_page(baseline["projection_id"], baseline["boundary_cursor"], 100)
            self.assertFalse(frame.resync_required)
            self.assertEqual(frame.next_cursor, baseline["boundary_cursor"] + 1)
            self.assertEqual(frame.items[0].payload.cursor, frame.next_cursor)

    def test_bounded_source_expired_materializer_rebuilds_current_state(self):
        writer = self.writer([gamma_row()])
        with TestClient(self.app()) as client:
            baseline = self.ready_full_sync(client).json()
            discovery = client.app.state.polymarket_discovery
            discovery.stop_background_materialization()
            writer.apply_snapshot(snapshot("yes-1", "0.41", "0.43", "1785739201000"), received_at=NOW)
            writer.apply_snapshot(snapshot("no-1", "0.57", "0.59", "1785739202000"), received_at=NOW)
            self.bounded_source(baseline["boundary_cursor"] + 1)
            discovery.materialize_once()
            current = self.ready_full_sync(client).json()
            self.assertEqual(current["boundary_cursor"], baseline["boundary_cursor"] + 2)
            self.assertTrue(discovery.events_page(baseline["projection_id"], baseline["boundary_cursor"], 100).resync_required)

    def test_market_settlement_is_optional_and_never_inferred(self):
        without_settlement = gamma_row()
        with_settlement = gamma_row(
            "m2", "0x" + "2" * 64, ("yes-2", "no-2")
        )
        with_settlement.update({
            "rules": "Resolves YES if the event occurs.",
            "resolutionSource": "official-results",
            "redeemable": True,
            "redeemableAt": "2026-08-04T12:34:56.123456Z",
        })
        self.writer([without_settlement, with_settlement])

        with TestClient(self.app()) as client:
            markets = {
                market["market_id"]: market
                for market in self.ready_full_sync(client).json()["markets"]
            }

        self.assertIsNone(markets["m1"]["settlement"])
        settlement = markets["m2"]["settlement"]
        self.assertEqual(settlement["resolution_source"], "official-results")
        self.assertRegex(settlement["rules_revision"], r"^[0-9a-f]{64}$")
        self.assertTrue(settlement["redeemable"])
        self.assertEqual(settlement["redeemable_at_ns"], 1785846896123456000)
        self.assertRegex(settlement["evidence_sha256"], r"^[0-9a-f]{64}$")

    def test_negative_risk_relation_is_complete_and_quoted_atomically(self):
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("yes-1", "no-1"), neg_risk=True),
            gamma_row("m2", "0x" + "2" * 64, ("yes-2", "no-2"), neg_risk=True),
        ]
        self.writer(rows)
        with TestClient(self.app()) as client:
            full_sync = self.ready_full_sync(client).json()
        body = full_sync["relations"][0]
        self.assertTrue(body["complete"])
        self.assertEqual(body["expected_member_count"], 2)
        self.assertEqual(body["actual_member_count"], 2)
        self.assertNotIn("quotes", body)
        self.assertNotIn("schema_version", body)

    def test_catalog_relation_change_emits_relation_update_payload(self):
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("yes-1", "no-1"), neg_risk=True),
            gamma_row("m2", "0x" + "2" * 64, ("yes-2", "no-2"), neg_risk=True),
        ]
        writer = self.writer(rows)
        with TestClient(self.app()) as client:
            baseline = self.ready_full_sync(client).json()
            writer._emit(
                "catalog_revision",
                {
                    "catalog_revision": writer.catalog_revision,
                    "token_changes": {},
                    "relation_changes": {
                        "added_relation_ids": [],
                        "removed_relation_ids": [],
                        "changed_relation_ids": ["neg-risk:neg-group"],
                    },
                },
                {},
                applied=True,
            )
            discovery = client.app.state.polymarket_discovery
            discovery.materialize_once()
            frame = discovery.events_page(
                baseline["projection_id"], baseline["boundary_cursor"], 1000
            )
        self.assertFalse(frame.resync_required)
        self.assertEqual(frame.next_cursor, frame.after_cursor + 1)
        self.assertEqual(len(frame.items), 1)
        self.assertEqual(frame.items[0].type, "relation_update")
        self.assertEqual(
            frame.items[0].payload.relation_id, "neg-risk:neg-group"
        )
        self.assertEqual(
            set(frame.items[0].model_dump(mode="json")),
            {"type", "payload"},
        )

    def test_cursor_expiry_and_websocket_resync_are_explicit(self):
        writer = self.writer([gamma_row()])
        with TestClient(self.app()) as client:
            baseline = self.ready_full_sync(client).json()
            after_cursor = baseline["boundary_cursor"]
            writer.apply_snapshot(
                snapshot("yes-1", "0.41", "0.43", "1785739201000"),
                received_at=NOW + timedelta(milliseconds=1),
            )
            writer.apply_snapshot(
                snapshot("no-1", "0.57", "0.59", "1785739202000"),
                received_at=NOW + timedelta(milliseconds=2),
            )
            with sqlite3.connect(writer.state_index.path) as connection:
                connection.execute(
                    "DELETE FROM event_offsets WHERE cursor=?", (after_cursor + 1,)
                )
                connection.commit()
            frame = client.app.state.polymarket_discovery.events_page(
                baseline["projection_id"], after_cursor, 1000
            )
            self.assertTrue(frame.resync_required)
            self.assertEqual(frame.next_cursor, after_cursor)
            self.assertEqual(frame.items, [])
            with client.websocket_connect(
                "/v1/prediction-markets/polymarket/live/discovery/stream"
                f"?after_cursor={after_cursor}"
                f"&projection_id={baseline['projection_id']}"
            ) as socket:
                wire = socket.receive_json()
                self.assertTrue(wire["resync_required"])
                self.assertEqual(wire["schema_version"], "marketcow.polymarket.discovery-events.v3")

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

    def test_openapi_publishes_only_v3_discovery_and_history_contracts(self):
        self.writer([gamma_row()])
        with TestClient(self.app()) as client:
            openapi = client.get("/openapi.json").json()
        for path in (
            "/v1/prediction-markets/polymarket/live/discovery/full-sync",
            "/v1/prediction-markets/polymarket/history/lifecycle-events",
        ):
            self.assertIn(path, openapi["paths"])
        for removed in (
            "/v1/prediction-markets/polymarket/live/discovery/snapshot",
            "/v1/prediction-markets/polymarket/live/discovery/events",
            "/v1/prediction-markets/polymarket/live/discovery/metadata",
            "/v1/prediction-markets/polymarket/live/discovery/relations/{relation_id}",
        ):
            self.assertNotIn(removed, openapi["paths"])
        schemas = openapi["components"]["schemas"]
        self.assertIn("DiscoveryFullSync", schemas)
        self.assertIn("DiscoveryDeltaFrame", schemas)
        self.assertIn("DiscoverySettlement", schemas)
        self.assertNotIn("DiscoveryMetadataPage", schemas)
        self.assertIn("LifecycleHistoryPage", schemas)
        delta_items = schemas["DiscoveryDeltaFrame"]["properties"]["items"][
            "items"
        ]
        self.assertEqual(delta_items["discriminator"]["propertyName"], "type")
        self.assertEqual(
            set(delta_items["discriminator"]["mapping"]),
            {"market_update", "relation_update", "universe_changed"},
        )
        for schema_name in (
            "DiscoveryMarketUpdateItem",
            "DiscoveryRelationUpdateItem",
            "DiscoveryUniverseChangedItem",
        ):
            self.assertEqual(
                set(schemas[schema_name]["required"]),
                {"payload"},
            )
            self.assertEqual(
                set(schemas[schema_name]["properties"]),
                {"type", "payload"},
            )
        self.assertIn(
            "/v1/prediction-markets/polymarket/live/discovery/stream",
            openapi["x-websocket-paths"],
        )
        stream = openapi["x-websocket-paths"][
            "/v1/prediction-markets/polymarket/live/discovery/stream"
        ]
        self.assertEqual(
            stream["schema_version"],
            "marketcow.polymarket.discovery-events.v3",
        )
        self.assertIn("projection_id", stream["query_parameters"])


if __name__ == "__main__":
    unittest.main()
