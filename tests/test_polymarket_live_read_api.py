from __future__ import annotations

import asyncio
import json
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from fastapi.testclient import TestClient

from marketcow.polymarket_live import (
    GammaLiveNormalizer,
    LiveStateStore,
)
from marketcow.polymarket_live_read_api import create_polymarket_live_read_app
from tests.test_polymarket_live import gamma_row, snapshot


NOW = datetime(2026, 8, 15, 10, 0, tzinfo=timezone.utc)


class PolymarketLiveReadApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name) / "live"
        writer = LiveStateStore(self.root, now_provider=lambda: NOW)
        self.writer = writer
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        writer.apply_snapshot(
            snapshot("yes-1", "0.40", "0.42"), received_at=NOW
        )
        writer.apply_snapshot(
            snapshot("no-1", "0.58", "0.60"), received_at=NOW
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def app(self):
        app = create_polymarket_live_read_app(
            root=self.root,
            stable_snapshot_max_book_age_seconds=3.5,
            stable_read_wait_seconds=6,
            stable_read_poll_seconds=0.025,
            executor_workers=4,
        )
        app.state.polymarket_live_read.now_provider = lambda: NOW
        return app

    def test_contract_matches_bounded_live_read_routes(self):
        with TestClient(self.app()) as client:
            health = client.get(
                "/v1/prediction-markets/polymarket/live/health"
            )
            bootstrap = client.get(
                "/v1/prediction-markets/polymarket/live/bootstrap?market_id=m1"
            )
            full = client.get(
                "/v1/prediction-markets/polymarket/live/bootstrap"
            )
            frame = client.get(
                "/v1/prediction-markets/polymarket/live/snapshot?market_id=m1"
            )
            events = client.get(
                "/v1/prediction-markets/polymarket/live/events"
                "?after_cursor=0&market_id=m1"
            )
            gaps = client.get(
                "/v1/prediction-markets/polymarket/live/gaps"
                "?unresolved_only=false&market_id=m1"
            )

        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["status"], "index_ready")
        self.assertEqual(bootstrap.status_code, 200)
        self.assertEqual(bootstrap.json()["markets"][0]["identity"]["market_id"], "m1")
        self.assertEqual(full.status_code, 409)
        self.assertEqual(
            full.json()["detail"]["code"], "polymarket_full_universe_disabled"
        )
        self.assertEqual(frame.status_code, 200)
        self.assertEqual(frame.json()["items"][0]["status"], "ready")
        self.assertEqual(events.status_code, 200)
        self.assertEqual(len(events.json()["items"]), 2)
        self.assertEqual(gaps.status_code, 200)
        self.assertEqual(gaps.json()["count"], 0)

    def test_health_runs_while_snapshot_worker_is_blocked(self):
        app = self.app()
        reader = app.state.polymarket_live_read
        original_snapshot = reader.snapshot_json
        snapshot_started = threading.Event()
        release_snapshot = threading.Event()

        def blocked_snapshot(market_ids):
            snapshot_started.set()
            release_snapshot.wait(timeout=5)
            return original_snapshot(market_ids)

        with patch.object(reader, "snapshot_json", side_effect=blocked_snapshot):
            with TestClient(app) as client:
                result: list[object] = []
                worker = threading.Thread(
                    target=lambda: result.append(
                        client.get(
                            "/v1/prediction-markets/polymarket/live/snapshot"
                            "?market_id=m1"
                        )
                    )
                )
                worker.start()
                self.assertTrue(snapshot_started.wait(timeout=2))
                started = time.monotonic()
                health = client.get(
                    "/v1/prediction-markets/polymarket/live/health"
                )
                elapsed = time.monotonic() - started
                release_snapshot.set()
                worker.join(timeout=5)

        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["status"], "index_ready")
        self.assertLess(elapsed, 1)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].status_code, 200)

    def test_stream_health_returns_503_when_projected_books_are_stale(self):
        app = create_polymarket_live_read_app(
            root=self.root,
            stable_snapshot_max_book_age_seconds=3.5,
            stable_read_wait_seconds=6,
            stable_read_poll_seconds=0.025,
            executor_workers=4,
            live_stream_uri="ws://127.0.0.1:1",
        )
        reader = app.state.polymarket_live_read
        reader.now_provider = lambda: NOW + timedelta(seconds=4)
        projection = app.state.polymarket_live_projection
        projection.install_state({
            "schema_version": "marketcow.polymarket.live-stream.v1",
            "type": "state",
            "catalog_revision": self.writer.catalog_revision,
            "catalog_source": self.writer.catalog_source,
            "latest_cursor": self.writer.cursor,
            "persisted_cursor": self.writer.cursor,
            "active_recovery_id": None,
            "markets": [
                market.model_dump(mode="json")
                for market in self.writer.catalog.values()
            ],
            "books": [
                book.model_dump(mode="json")
                for book in self.writer.books.values()
            ],
            "gaps": [],
        })
        projection.mark_ready({"latest_cursor": self.writer.cursor})

        client = TestClient(app)
        response = client.get(
            "/v1/prediction-markets/polymarket/live/health"
        )
        app.state.polymarket_live_read_executor.shutdown(
            wait=True, cancel_futures=True
        )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json()["detail"]["code"],
            "polymarket_snapshot_freshness_budget_exhausted",
        )
        self.assertTrue(response.json()["detail"]["retryable"])

    def test_slow_snapshot_client_write_does_not_block_projection_ingestion(self):
        app = create_polymarket_live_read_app(
            root=self.root,
            stable_snapshot_max_book_age_seconds=5,
            stable_read_wait_seconds=6,
            stable_read_poll_seconds=0.025,
            executor_workers=4,
            live_stream_uri="ws://127.0.0.1:1",
        )
        reader = app.state.polymarket_live_read
        reader.now_provider = lambda: NOW
        projection = app.state.polymarket_live_projection
        projection.install_state({
            "schema_version": "marketcow.polymarket.live-stream.v1",
            "type": "state",
            "catalog_revision": self.writer.catalog_revision,
            "catalog_source": self.writer.catalog_source,
            "latest_cursor": self.writer.cursor,
            "persisted_cursor": self.writer.cursor,
            "active_recovery_id": None,
            "markets": [
                market.model_dump(mode="json")
                for market in self.writer.catalog.values()
            ],
            "books": [
                book.model_dump(mode="json")
                for book in self.writer.books.values()
            ],
            "gaps": [],
        })
        projection.mark_ready({"latest_cursor": self.writer.cursor})

        async def exercise():
            body_write_started = asyncio.Event()
            release_body_write = asyncio.Event()
            messages = []

            async def receive():
                return {"type": "http.request", "body": b"", "more_body": False}

            async def slow_send(message):
                messages.append(message)
                if message["type"] == "http.response.body":
                    body_write_started.set()
                    await release_body_write.wait()

            request = asyncio.create_task(app({
                "type": "http",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": "/v1/prediction-markets/polymarket/live/snapshot",
                "raw_path": b"/v1/prediction-markets/polymarket/live/snapshot",
                "query_string": b"market_id=m1",
                "root_path": "",
                "headers": [],
                "client": ("127.0.0.1", 1),
                "server": ("127.0.0.1", 8790),
            }, receive, slow_send))
            await asyncio.wait_for(body_write_started.wait(), timeout=2)
            previous = projection.latest_cursor
            event = await asyncio.to_thread(
                self.writer.apply_snapshot,
                snapshot("yes-1", "0.39", "0.41", "1785739201000"),
                received_at=NOW,
            )
            started = time.perf_counter()
            projection.apply_live(event.model_dump(mode="json"))
            elapsed = time.perf_counter() - started
            self.assertEqual(projection.latest_cursor, previous + 1)
            release_body_write.set()
            await asyncio.wait_for(request, timeout=2)
            return elapsed, messages

        try:
            elapsed, messages = asyncio.run(exercise())
        finally:
            app.state.polymarket_live_read_executor.shutdown(
                wait=True, cancel_futures=True
            )

        self.assertLess(elapsed, 0.1)
        self.assertEqual(messages[0]["status"], 200)
        self.assertTrue(any(
            message["type"] == "http.response.body" for message in messages
        ))

    def test_atomic_full_sync_contract_and_server_timing(self):
        app = create_polymarket_live_read_app(
            root=self.root,
            stable_snapshot_max_book_age_seconds=5,
            stable_read_wait_seconds=6,
            stable_read_poll_seconds=0.025,
            executor_workers=4,
            live_stream_uri="ws://127.0.0.1:1",
            consumer_maximum_book_age_seconds=5,
            minimum_delivery_headroom_seconds=1,
        )
        reader = app.state.polymarket_live_read
        reader.now_provider = lambda: NOW + timedelta(seconds=1)
        projection = app.state.polymarket_live_projection
        projection.install_state({
            "schema_version": "marketcow.polymarket.live-stream.v1",
            "type": "state",
            "catalog_revision": self.writer.catalog_revision,
            "catalog_source": self.writer.catalog_source,
            "latest_cursor": self.writer.cursor,
            "persisted_cursor": self.writer.cursor,
            "active_recovery_id": None,
            "markets": [
                market.model_dump(mode="json")
                for market in self.writer.catalog.values()
            ],
            "books": [
                book.model_dump(mode="json")
                for book in self.writer.books.values()
            ],
            "gaps": [],
        })
        projection.mark_ready({"latest_cursor": self.writer.cursor})

        client = TestClient(app)
        with patch(
            "marketcow.polymarket_events_observability.LOGGER.info"
        ) as log_info:
            response = client.get(
                "/v1/prediction-markets/polymarket/live/full-sync?market_id=m1"
            )
        app.state.polymarket_live_read_executor.shutdown(
            wait=True, cancel_futures=True,
        )

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        boundary = (
            payload["catalog_revision"], payload["cursor"],
            payload["projection_generation"], payload["scope_market_ids"],
            payload["freshness_checked_at"],
        )
        for name in ("health", "bootstrap", "snapshot"):
            component = payload[name]
            self.assertEqual(component["catalog_revision"], boundary[0])
            self.assertEqual(
                component.get("latest_cursor", component.get("cursor")),
                boundary[1],
            )
            self.assertEqual(component["projection_generation"], boundary[2])
            self.assertEqual(component["scope_market_ids"], boundary[3])
            self.assertEqual(component["freshness_checked_at"], boundary[4])
        frame = payload["snapshot"]["items"][0]
        self.assertNotIn("tokens", frame)
        self.assertNotIn("relation_tokens", frame)
        self.assertEqual(
            set(payload["snapshot"]["books"]), set(frame["token_ids"]),
        )
        self.assertEqual(payload["maximum_book_age_ms"], 1000)
        self.assertGreater(payload["freshness_budget_remaining_ms"], 1000)
        timing = response.headers["server-timing"]
        for phase in (
            "executor_queue", "lock_wait", "projection_copy", "frame_build",
            "json_serialize", "freshness_check",
        ):
            self.assertIn(phase, timing)
        trace = json.loads(next(
            call.args[1] for call in log_info.call_args_list
            if call.args[0] == "polymarket_scoped_read %s"
        ))
        self.assertEqual(trace["status"], 200)
        self.assertEqual(trace["response_body_bytes"], len(response.content))
        self.assertGreaterEqual(trace["asgi_response_write_ms"], 0)
        for phase in (
            "scope_count", "executor_queue_ms", "lock_wait_ms",
            "projection_copy_ms", "frame_build_ms", "json_serialize_ms",
            "freshness_check_ms", "maximum_book_age_ms",
            "freshness_headroom_ms", "cursor",
        ):
            self.assertIn(phase, trace)

    def test_full_sync_rejects_insufficient_delivery_headroom(self):
        app = create_polymarket_live_read_app(
            root=self.root,
            stable_snapshot_max_book_age_seconds=5,
            stable_read_wait_seconds=6,
            stable_read_poll_seconds=0.025,
            executor_workers=4,
            live_stream_uri="ws://127.0.0.1:1",
            consumer_maximum_book_age_seconds=5,
            minimum_delivery_headroom_seconds=1,
        )
        reader = app.state.polymarket_live_read
        reader.now_provider = lambda: NOW + timedelta(seconds=4.2)
        projection = app.state.polymarket_live_projection
        projection.install_state({
            "schema_version": "marketcow.polymarket.live-stream.v1",
            "type": "state",
            "catalog_revision": self.writer.catalog_revision,
            "catalog_source": self.writer.catalog_source,
            "latest_cursor": self.writer.cursor,
            "persisted_cursor": self.writer.cursor,
            "active_recovery_id": None,
            "markets": [
                market.model_dump(mode="json")
                for market in self.writer.catalog.values()
            ],
            "books": [
                book.model_dump(mode="json")
                for book in self.writer.books.values()
            ],
            "gaps": [],
        })
        projection.mark_ready({"latest_cursor": self.writer.cursor})

        response = TestClient(app).get(
            "/v1/prediction-markets/polymarket/live/full-sync?market_id=m1"
        )
        app.state.polymarket_live_read_executor.shutdown(
            wait=True, cancel_futures=True,
        )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json()["detail"]["code"],
            "polymarket_snapshot_freshness_budget_exhausted",
        )
        self.assertTrue(response.json()["detail"]["retryable"])


if __name__ == "__main__":
    unittest.main()
