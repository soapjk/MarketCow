from __future__ import annotations

import asyncio
import socket
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from marketcow.polymarket_live import (
    GammaLiveNormalizer,
    LiveStateStore,
    PolymarketLiveReadStore,
)
from marketcow.polymarket_live import content_sha256, live_event_identity
from marketcow.polymarket_live_stream import (
    PolymarketLiveProjection,
    PolymarketLiveStreamClient,
    PolymarketLiveStreamServer,
)
from tests.test_polymarket_live import gamma_row, snapshot


NOW = datetime(2026, 8, 22, 11, 0, tzinfo=timezone.utc)


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def populated_store(root: Path) -> LiveStateStore:
    store = LiveStateStore(root, now_provider=lambda: NOW)
    rows = [gamma_row()]
    store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
    store.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
    store.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
    return store


class PolymarketAsyncPersistenceTest(unittest.TestCase):
    def test_live_sink_precedes_blocked_persistence_and_durable_tail_catches_up(self):
        with TemporaryDirectory() as temporary:
            store = populated_store(Path(temporary) / "live")
            previous_cursor = store.cursor
            previous_size = store.event_path.stat().st_size
            persisted_started = threading.Event()
            release_persistence = threading.Event()
            published: list[int] = []
            original = store._persist_records

            def blocked(records):
                persisted_started.set()
                release_persistence.wait(timeout=5)
                return original(records)

            store.live_event_sink = lambda event: published.append(event.cursor)
            with patch.object(store, "_persist_records", side_effect=blocked):
                store.enable_async_persistence()
                started = time.perf_counter()
                emitted = store.apply_snapshot(
                    snapshot("yes-1", "0.39", "0.41", "1785739210000"),
                    received_at=NOW,
                )
                elapsed = time.perf_counter() - started

                self.assertIsNotNone(emitted)
                self.assertEqual(published, [previous_cursor + 1])
                self.assertLess(elapsed, 0.1)
                self.assertTrue(persisted_started.wait(timeout=2))
                self.assertEqual(store.event_path.stat().st_size, previous_size)
                self.assertEqual(store.persisted_cursor, previous_cursor)

                # A persistence worker can own the file/index publication lock
                # for an arbitrarily slow fsync/SQLite commit.  Subsequent live
                # events must still publish without trying to acquire it.
                with patch(
                    "marketcow.polymarket_live._publication_lock",
                    side_effect=AssertionError("disk lock entered hot path"),
                ):
                    second_started = time.perf_counter()
                    second = store.apply_snapshot(
                        snapshot("no-1", "0.57", "0.59", "1785739211000"),
                        received_at=NOW,
                    )
                    second_elapsed = time.perf_counter() - second_started
                self.assertIsNotNone(second)
                self.assertEqual(published, [previous_cursor + 1, previous_cursor + 2])
                self.assertLess(second_elapsed, 0.1)

                release_persistence.set()
                store.flush_async_persistence(
                    target_cursor=previous_cursor + 2, timeout=5
                )
                store.close_async_persistence(timeout=5)

            self.assertGreater(store.event_path.stat().st_size, previous_size)
            self.assertEqual(store.persisted_cursor, previous_cursor + 2)
            recovered = LiveStateStore(store.root, now_provider=lambda: NOW)
            recovered.recover()
            self.assertEqual(recovered.cursor, previous_cursor + 2)
            self.assertEqual(
                [event.cursor for event in recovered.events][-3:],
                [previous_cursor, previous_cursor + 1, previous_cursor + 2],
            )


class PolymarketLiveStreamTest(unittest.IsolatedAsyncioTestCase):
    async def test_loopback_stream_builds_projection_and_advances_without_sqlite(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        store = populated_store(Path(temporary.name) / "live")
        store.enable_async_persistence()
        port = free_port()
        server = PolymarketLiveStreamServer(
            store, port=port, replay_capacity=100
        )
        projection = PolymarketLiveProjection(replay_capacity=100)
        client = PolymarketLiveStreamClient(
            f"ws://127.0.0.1:{port}", projection, reconnect_seconds=0.01
        )
        stop = asyncio.Event()
        await server.start()
        task = asyncio.create_task(client.run(stop))
        try:
            for _ in range(200):
                if projection.ready:
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(projection.ready)
            self.assertEqual(projection.latest_cursor, store.cursor)
            after = projection.latest_cursor

            await asyncio.to_thread(
                store.apply_snapshot,
                snapshot("yes-1", "0.39", "0.41", "1785739210000"),
                received_at=NOW,
            )
            for _ in range(200):
                if projection.latest_cursor == store.cursor:
                    break
                await asyncio.sleep(0.01)

            self.assertEqual(projection.latest_cursor, after + 1)
            page = projection.events_after(["m1"], after, 1000)
            self.assertEqual([event.cursor for event in page.items], [after + 1])
            self.assertEqual(page.next_cursor, after + 1)
            self.assertFalse(page.has_more)
            health = projection.health()
            self.assertEqual(health.status, "index_ready")
            self.assertTrue(health.latest_state_ready)
            self.assertEqual(health.book_complete_market_count, 1)
            reader = PolymarketLiveReadStore(
                store.root,
                now_provider=lambda: NOW,
                stable_snapshot_max_book_age_seconds=4.9,
            )
            with patch.object(
                reader, "bootstrap", side_effect=AssertionError("disk hot read")
            ):
                bootstrap = projection.bootstrap(reader, ["m1"])
                frame = projection.snapshot(reader, ["m1"])
            self.assertEqual(bootstrap.cursor, projection.latest_cursor)
            self.assertEqual(frame.items[0].status, "ready")
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await server.close()
            await asyncio.to_thread(store.close_async_persistence, timeout=5)

    async def test_100_market_memory_page_is_ordered_bounded_and_has_no_disk_phase(self):
        projection = PolymarketLiveProjection(replay_capacity=2000)
        projection.install_state({
            "schema_version": "marketcow.polymarket.live-stream.v1",
            "type": "state",
            "catalog_revision": None,
            "catalog_source": None,
            "latest_cursor": 0,
            "persisted_cursor": 0,
            "active_recovery_id": None,
            "markets": [],
            "books": [],
            "gaps": [],
        })
        projection.mark_ready({"latest_cursor": 0})
        with TemporaryDirectory() as temporary:
            template = populated_store(Path(temporary) / "template").events[-1]
            for cursor in range(1, 1001):
                market_id = f"m{cursor % 100}"
                raw = {"sequence": cursor}
                event = template.model_copy(update={
                    "cursor": cursor,
                    "event_id": "0" * 64,
                    "market_id": market_id,
                    "raw_payload": raw,
                    "raw_payload_sha256": content_sha256(raw),
                })
                event.event_id = live_event_identity(event)
                projection.apply_live(event.model_dump(mode="json"))

        phases: dict[str, float] = {}
        started = time.perf_counter()
        payload, observed, page = projection.events_json(
            [f"m{index}" for index in range(100)],
            0,
            1000,
            _phase_ms=phases,
        )
        elapsed = time.perf_counter() - started

        self.assertLess(elapsed, 0.5)
        self.assertEqual(observed["sqlite_query_ms"], 0)
        self.assertGreater(observed["memory_projection_ms"], 0)
        self.assertLessEqual(len(payload), 1_048_576 + 4096)
        self.assertEqual(
            [event.cursor for event in page.items],
            sorted(event.cursor for event in page.items),
        )
        self.assertTrue(page.has_more)

    async def test_cursor_gap_fails_closed_without_advancing_projection(self):
        projection = PolymarketLiveProjection(replay_capacity=10)
        projection.install_state({
            "schema_version": "marketcow.polymarket.live-stream.v1",
            "type": "state",
            "catalog_revision": None,
            "catalog_source": None,
            "latest_cursor": 0,
            "persisted_cursor": 0,
            "active_recovery_id": None,
            "markets": [],
            "books": [],
            "gaps": [],
        })
        projection.mark_ready({"latest_cursor": 0})
        with TemporaryDirectory() as temporary:
            event = populated_store(Path(temporary) / "template").events[-1]
            event = event.model_copy(update={
                "cursor": 2, "event_id": "0" * 64,
            })
            event.event_id = live_event_identity(event)
            with self.assertRaisesRegex(ValueError, "cursor gap"):
                projection.apply_live(event.model_dump(mode="json"))
        self.assertFalse(projection.ready)
        self.assertEqual(projection.latest_cursor, 0)

    async def test_persistence_watermark_may_lead_local_websocket_decode(self):
        projection = PolymarketLiveProjection(replay_capacity=10)
        projection.install_state({
            "schema_version": "marketcow.polymarket.live-stream.v1",
            "type": "state",
            "catalog_revision": None,
            "catalog_source": None,
            "latest_cursor": 10,
            "persisted_cursor": 10,
            "active_recovery_id": None,
            "markets": [], "books": [], "gaps": [],
        })
        projection.mark_ready({"latest_cursor": 10})
        projection.update_persistence(12, queue_depth=0)
        self.assertEqual(projection.watermarks()["persisted_cursor"], 12)
        self.assertEqual(projection.watermarks()["persistence_lag_events"], 0)


if __name__ == "__main__":
    unittest.main()
