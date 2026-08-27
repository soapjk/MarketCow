from __future__ import annotations

import asyncio
import json
import socket
import sqlite3
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from marketcow.polymarket_live import (
    GammaLiveNormalizer,
    LiveBook,
    LiveMarket,
    LiveSnapshotPage,
    LiveStateStore,
    PolymarketLiveReadError,
    PolymarketLiveReadStore,
    _durable_event_log_tail,
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

    def test_broken_derived_index_does_not_stop_authoritative_log_appends(self):
        with TemporaryDirectory() as temporary:
            store = populated_store(Path(temporary) / "live")
            previous_cursor = store.cursor
            previous_size = store.event_path.stat().st_size
            store.enable_async_persistence()
            with patch.object(
                store.state_index, "batch",
                side_effect=sqlite3.DatabaseError("database disk image is malformed"),
            ):
                first = store.apply_snapshot(
                    snapshot("yes-1", "0.39", "0.41", "1785739210000"),
                    received_at=NOW,
                )
                store.flush_async_persistence(
                    target_cursor=previous_cursor + 1, timeout=5,
                )
                for _ in range(100):
                    if store.derived_index_error:
                        break
                    time.sleep(0.01)
            self.assertIsNotNone(first)
            self.assertIsNone(store.persistence_error)
            self.assertIn("database disk image is malformed", store.derived_index_error)
            self.assertFalse(store._state_index_available)

            second = store.apply_snapshot(
                snapshot("no-1", "0.57", "0.59", "1785739211000"),
                received_at=NOW,
            )
            store.flush_async_persistence(
                target_cursor=previous_cursor + 2, timeout=5,
            )
            store.close_async_persistence(timeout=5)

            self.assertIsNotNone(second)
            self.assertGreater(store.event_path.stat().st_size, previous_size)
            tail, size = _durable_event_log_tail(store.event_path)
            self.assertIsNotNone(tail)
            self.assertEqual(tail.cursor, previous_cursor + 2)
            self.assertEqual(size, store.event_path.stat().st_size)

    def test_slow_derived_index_cannot_delay_authoritative_log_fsync(self):
        with TemporaryDirectory() as temporary:
            store = populated_store(Path(temporary) / "live")
            previous_cursor = store.cursor
            index_started = threading.Event()
            release_index = threading.Event()
            original = store._persist_index_records

            def blocked(records):
                index_started.set()
                release_index.wait(timeout=5)
                return original(records)

            with patch.object(store, "_persist_index_records", side_effect=blocked):
                store.enable_async_persistence()
                emitted = store.apply_snapshot(
                    snapshot("yes-1", "0.39", "0.41", "1785739210000"),
                    received_at=NOW,
                )
                self.assertIsNotNone(emitted)
                self.assertTrue(index_started.wait(timeout=2))
                started = time.perf_counter()
                store.flush_async_persistence(
                    target_cursor=previous_cursor + 1, timeout=2,
                )
                self.assertLess(time.perf_counter() - started, 0.5)
                tail, _size = _durable_event_log_tail(store.event_path)
                self.assertIsNotNone(tail)
                self.assertEqual(tail.cursor, previous_cursor + 1)
                release_index.set()
                store.close_async_persistence(timeout=5)


class PolymarketLiveStreamTest(unittest.IsolatedAsyncioTestCase):
    async def test_loopback_stream_builds_projection_and_advances_without_sqlite(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        store = populated_store(Path(temporary.name) / "live")
        store.derived_index_error = "DatabaseError:isolated derived index"
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

            empty_ask = snapshot(
                "yes-1", "0.39", "0.41", "1785739211000",
            )
            empty_ask["asks"] = []
            await asyncio.to_thread(
                store.apply_snapshot, empty_ask, received_at=NOW,
            )
            for _ in range(200):
                if projection.latest_cursor == store.cursor:
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(projection.latest_cursor, after + 2)

            health = projection.health(
                PolymarketLiveReadStore(
                    store.root,
                    now_provider=lambda: NOW,
                    stable_snapshot_max_book_age_seconds=5,
                ),
                ["m1"],
            )
            self.assertEqual(health.status, "index_ready")
            self.assertEqual(
                health.derived_index_error,
                "DatabaseError:isolated derived index",
            )
            self.assertTrue(health.latest_state_ready)
            self.assertEqual(health.book_complete_market_count, 1)
            self.assertEqual(projection._books["yes-1"].asks, [])
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

            reader.now_provider = lambda: NOW + timedelta(seconds=5)
            with self.assertRaises(PolymarketLiveReadError) as stale_health:
                projection.health(reader, ["m1"])
            self.assertEqual(
                stale_health.exception.code,
                "polymarket_snapshot_freshness_budget_exhausted",
            )
            for action in (
                lambda: projection.bootstrap_json(reader, ["m1"]),
                lambda: projection.snapshot_json(reader, ["m1"]),
            ):
                with self.assertRaises(PolymarketLiveReadError) as stale:
                    action()
                self.assertEqual(
                    stale.exception.code,
                    "polymarket_snapshot_freshness_budget_exhausted",
                )
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await server.close()
            await asyncio.to_thread(store.close_async_persistence, timeout=5)

    async def test_100_market_frames_drop_resolved_gap_history_from_hot_projection(self):
        rows = [
            gamma_row(
                f"m{index}",
                condition_id="0x" + f"{index + 1:064x}",
                tokens=(f"yes-{index}", f"no-{index}"),
            )
            for index in range(100)
        ]
        markets = GammaLiveNormalizer.normalize(rows, NOW)
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        template_store = populated_store(Path(temporary.name) / "template")
        templates = list(template_store.books.values())
        reader = PolymarketLiveReadStore(
            Path(temporary.name) / "read-root",
            now_provider=lambda: NOW,
            stable_snapshot_max_book_age_seconds=5,
        )
        books = []
        for index, market in enumerate(markets):
            for offset, outcome in enumerate(market.identity.outcomes):
                books.append(templates[offset].model_copy(update={
                    "token_id": outcome.token_id,
                    "condition_id": market.identity.condition_id,
                }).model_dump(mode="json"))
        resolved_gaps = [
            {
                "code": "coverage_gap",
                "token_id": f"historic-{index}",
                "detected_at": NOW.isoformat(),
                "resolved": True,
                "resolution": "snapshot_recovered",
            }
            for index in range(5_000)
        ]
        projection = PolymarketLiveProjection(replay_capacity=100)
        projection.install_state({
            "schema_version": "marketcow.polymarket.live-stream.v1",
            "type": "state",
            "catalog_revision": "a" * 64,
            "catalog_source": {"source": "test"},
            "latest_cursor": 0,
            "persisted_cursor": 0,
            "active_recovery_id": None,
            "markets": [market.model_dump(mode="json") for market in markets],
            "books": books,
            "gaps": resolved_gaps,
        })
        projection.mark_ready({"latest_cursor": 0})
        scope = [f"m{index}" for index in range(100)]

        started = time.perf_counter()
        with patch(
            "sqlite3.connect", side_effect=AssertionError("SQLite hot query")
        ):
            bootstrap = projection.bootstrap_json(reader, scope)
            snapshot_body = projection.snapshot_json(reader, scope)
        elapsed = time.perf_counter() - started

        self.assertEqual(projection.health(reader, scope).unresolved_gap_count, 0)
        self.assertEqual(projection._gaps, [])
        self.assertGreater(len(bootstrap), 100_000)
        self.assertGreater(len(snapshot_body), 100_000)
        self.assertLess(elapsed, 0.75)

        reader.now_provider = lambda: NOW + timedelta(seconds=5)
        with self.assertRaises(PolymarketLiveReadError) as stale_health:
            projection.health(reader, scope)
        self.assertEqual(
            stale_health.exception.code,
            "polymarket_snapshot_freshness_budget_exhausted",
        )

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

    async def test_event_page_sizing_does_not_hold_ingestion_lock(self):
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
            template = populated_store(Path(temporary) / "template").events[-1]
        first = template.model_copy(update={
            "cursor": 1,
            "event_id": "0" * 64,
            "market_id": "m1",
        })
        first.event_id = live_event_identity(first)
        projection.apply_live(first.model_dump(mode="json"))
        second = first.model_copy(update={
            "cursor": 2,
            "event_id": "0" * 64,
        })
        second.event_id = live_event_identity(second)

        sizing_started = threading.Event()
        release_sizing = threading.Event()
        original = type(first).model_dump_json

        def blocked_sizing(event, *args, **kwargs):
            sizing_started.set()
            release_sizing.wait(timeout=5)
            return original(event, *args, **kwargs)

        result: list[object] = []
        with patch.object(type(first), "model_dump_json", blocked_sizing):
            worker = threading.Thread(
                target=lambda: result.append(
                    projection.events_after(["m1"], 0, 1000)
                )
            )
            worker.start()
            self.assertTrue(sizing_started.wait(timeout=2))
            started = time.perf_counter()
            projection.apply_live(second.model_dump(mode="json"))
            elapsed = time.perf_counter() - started
            release_sizing.set()
            worker.join(timeout=5)

        self.assertLess(elapsed, 0.1)
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(result), 1)
        self.assertEqual(projection.latest_cursor, 2)

    async def test_event_burst_advances_during_two_concurrent_snapshot_serializations(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        store = populated_store(Path(temporary.name) / "live")
        projection = PolymarketLiveProjection(replay_capacity=500)
        projection.install_state({
            "schema_version": "marketcow.polymarket.live-stream.v1",
            "type": "state",
            "catalog_revision": store.catalog_revision,
            "catalog_source": store.catalog_source,
            "latest_cursor": store.cursor,
            "persisted_cursor": store.cursor,
            "active_recovery_id": None,
            "markets": [
                market.model_dump(mode="json")
                for market in store.catalog.values()
            ],
            "books": [book.model_dump(mode="json") for book in store.books.values()],
            "gaps": [],
        })
        projection.mark_ready({"latest_cursor": store.cursor})
        reader = PolymarketLiveReadStore(
            store.root,
            now_provider=lambda: NOW,
            stable_snapshot_max_book_age_seconds=5,
        )
        serialization_started = threading.Barrier(3)
        release_serialization = threading.Event()
        original = LiveSnapshotPage.model_dump_json

        def blocked_serialization(page, *args, **kwargs):
            serialization_started.wait(timeout=5)
            release_serialization.wait(timeout=5)
            return original(page, *args, **kwargs)

        results: list[bytes] = []
        with patch.object(
            LiveSnapshotPage, "model_dump_json", blocked_serialization
        ):
            workers = [
                threading.Thread(
                    target=lambda: results.append(
                        projection.snapshot_json(reader, ["m1"])
                    )
                )
                for _ in range(2)
            ]
            for worker in workers:
                worker.start()
            await asyncio.to_thread(serialization_started.wait, 5)

            template = store.events[-1]
            started = time.perf_counter()
            for offset in range(1, 101):
                event = template.model_copy(update={
                    "cursor": projection.latest_cursor + 1,
                    "event_id": "0" * 64,
                    "event_type": "last_trade_price",
                    "canonical_payload": template.canonical_payload,
                })
                event.event_id = live_event_identity(event)
                projection.apply_live(event.model_dump(mode="json"))
            elapsed = time.perf_counter() - started
            release_serialization.set()
            for worker in workers:
                worker.join(timeout=5)

        self.assertEqual(len(results), 2)
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        self.assertEqual(projection.latest_cursor, store.cursor + 100)
        self.assertLess(elapsed, 0.5)

    def test_broad_scope_capture_reuses_immutable_projection_models(self):
        with TemporaryDirectory() as temporary:
            store = populated_store(Path(temporary) / "live")
            projection = PolymarketLiveProjection(replay_capacity=100)
            projection.install_state({
                "schema_version": "marketcow.polymarket.live-stream.v1",
                "type": "state",
                "catalog_revision": store.catalog_revision,
                "catalog_source": store.catalog_source,
                "latest_cursor": store.cursor,
                "persisted_cursor": store.cursor,
                "active_recovery_id": None,
                "markets": [
                    market.model_dump(mode="json")
                    for market in store.catalog.values()
                ],
                "books": [
                    book.model_dump(mode="json")
                    for book in store.books.values()
                ],
                "gaps": [],
            })
            projection.mark_ready({"latest_cursor": store.cursor})
            reader = PolymarketLiveReadStore(
                store.root,
                now_provider=lambda: NOW,
                stable_snapshot_max_book_age_seconds=5,
            )

            with (
                patch.object(
                    LiveMarket,
                    "model_copy",
                    side_effect=AssertionError("market copied under capture lock"),
                ),
                patch.object(
                    LiveBook,
                    "model_copy",
                    side_effect=AssertionError("book copied under capture lock"),
                ),
            ):
                body = projection.snapshot_json(reader, ["m1"])

            payload = json.loads(body)
            self.assertEqual(payload["cursor"], store.cursor)
            self.assertEqual(set(payload["books"]), {"yes-1", "no-1"})

    def test_full_sync_scoped_health_covers_relation_books_without_duplicates(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name) / "relation"
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("a", "b"), neg_risk=True),
            gamma_row("m2", "0x" + "2" * 64, ("c", "d"), neg_risk=True),
        ]
        store = LiveStateStore(root, now_provider=lambda: NOW)
        store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        for token, bid, ask in (
            ("a", "0.40", "0.42"), ("b", "0.58", "0.60"),
            ("c", "0.30", "0.32"), ("d", "0.68", "0.70"),
        ):
            store.apply_snapshot(snapshot(token, bid, ask), received_at=NOW)
        projection = PolymarketLiveProjection(replay_capacity=100)
        projection.install_state({
            "schema_version": "marketcow.polymarket.live-stream.v1",
            "type": "state",
            "catalog_revision": store.catalog_revision,
            "catalog_source": store.catalog_source,
            "latest_cursor": store.cursor,
            "persisted_cursor": store.cursor,
            "active_recovery_id": None,
            "markets": [
                market.model_dump(mode="json") for market in store.catalog.values()
            ],
            "books": [book.model_dump(mode="json") for book in store.books.values()],
            "gaps": [],
        })
        projection.mark_ready({"latest_cursor": store.cursor})
        reader = PolymarketLiveReadStore(
            root,
            now_provider=lambda: NOW + timedelta(seconds=1),
            stable_snapshot_max_book_age_seconds=5,
            consumer_maximum_book_age_seconds=5,
            minimum_delivery_headroom_seconds=1,
        )

        body, phases, full_sync = projection.full_sync_json(reader, ["m1"])

        frame = full_sync.snapshot.items[0]
        self.assertEqual(set(frame.token_ids), {"a", "b"})
        self.assertEqual(set(frame.relation_token_ids), {"a", "c"})
        self.assertEqual(set(full_sync.snapshot.books), {"a", "b", "c"})
        self.assertEqual(full_sync.health.token_count, 3)
        self.assertEqual(full_sync.health.book_token_count, 3)
        payload = json.loads(body)
        self.assertEqual(len(payload["snapshot"]["books"]), 3)
        self.assertNotIn("tokens", payload["snapshot"]["items"][0])
        self.assertEqual(phases["scope_count"], 1)

    def test_dynamic_tick_full_sync_fails_mixed_then_publishes_audited_boundary(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name) / "dynamic-tick"
        store = populated_store(root)
        projection = PolymarketLiveProjection(replay_capacity=100)
        projection.install_state({
            "schema_version": "marketcow.polymarket.live-stream.v1",
            "type": "state",
            "catalog_revision": store.catalog_revision,
            "catalog_source": store.catalog_source,
            "latest_cursor": store.cursor,
            "persisted_cursor": store.cursor,
            "active_recovery_id": None,
            "markets": [
                market.model_dump(mode="json") for market in store.catalog.values()
            ],
            "books": [book.model_dump(mode="json") for book in store.books.values()],
            "gaps": [],
        })
        projection.mark_ready({"latest_cursor": store.cursor})
        reader = PolymarketLiveReadStore(
            root,
            now_provider=lambda: NOW,
            stable_snapshot_max_book_age_seconds=5,
            consumer_maximum_book_age_seconds=5,
            minimum_delivery_headroom_seconds=1,
        )
        original_revision = store.catalog["m1"].rules.instrument.revision

        yes_event = store.apply_websocket({
            "event_type": "tick_size_change",
            "asset_id": "yes-1",
            "new_tick_size": "0.001",
            "timestamp": "1785739201000",
        }, received_at=NOW)[0]
        projection.apply_live(yes_event.model_dump(mode="json"))

        with self.assertRaises(PolymarketLiveReadError) as mixed:
            projection.full_sync_json(reader, ["m1"])
        self.assertEqual(
            mixed.exception.code,
            "polymarket_instrument_book_binding_incomplete",
        )

        no_event = store.apply_websocket({
            "event_type": "tick_size_change",
            "asset_id": "no-1",
            "new_tick_size": "0.001",
            "timestamp": "1785739201001",
        }, received_at=NOW)[0]
        projection.apply_live(no_event.model_dump(mode="json"))
        _, _, full_sync = projection.full_sync_json(reader, ["m1"])

        market = full_sync.bootstrap.markets[0]
        instrument = market.rules.instrument
        books = full_sync.snapshot.books
        self.assertEqual(instrument.price_increment, "0.001")
        self.assertEqual({book.tick_size for book in books.values()}, {"0.001"})
        self.assertNotEqual(instrument.revision, original_revision)
        provenance = instrument.provenance[-1]
        self.assertEqual(provenance.source, "polymarket_clob")
        self.assertEqual(provenance.boundary_cursor, no_event.cursor)
        self.assertEqual(
            provenance.projection_generation,
            full_sync.projection_generation,
        )
        self.assertEqual(
            {book.tick_version for book in books.values()},
            {provenance.tick_version},
        )
        self.assertEqual(set(provenance.token_ids), set(books))
        self.assertEqual(store.events[-1].cursor, provenance.boundary_cursor)

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

    async def test_future_persistence_watermark_waits_for_atomic_projection(self):
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
        first = projection.watermarks()
        self.assertEqual(first["published_cursor"], 10)
        self.assertEqual(first["persisted_cursor"], 10)
        self.assertEqual(
            first["persistence_lag_events"],
            first["published_cursor"] - first["persisted_cursor"],
        )

        with TemporaryDirectory() as temporary:
            template = populated_store(Path(temporary) / "template").events[-1]
        for cursor in (11, 12):
            event = template.model_copy(update={
                "cursor": cursor,
                "event_id": "0" * 64,
                "event_type": "last_trade_price",
            })
            event.event_id = live_event_identity(event)
            projection.apply_live(event.model_dump(mode="json"))
            watermarks = projection.watermarks()
            self.assertEqual(watermarks["published_cursor"], cursor)
            self.assertEqual(watermarks["persisted_cursor"], cursor)
            self.assertEqual(
                watermarks["persistence_lag_events"],
                watermarks["published_cursor"] - watermarks["persisted_cursor"],
            )

    def test_scoped_health_never_combines_future_persistence_generation(self):
        with TemporaryDirectory() as temporary:
            store = populated_store(Path(temporary) / "live")
            projection = PolymarketLiveProjection(replay_capacity=100)
            projection.install_state({
                "schema_version": "marketcow.polymarket.live-stream.v1",
                "type": "state",
                "catalog_revision": store.catalog_revision,
                "catalog_source": store.catalog_source,
                "latest_cursor": store.cursor,
                "persisted_cursor": store.cursor,
                "active_recovery_id": None,
                "markets": [
                    market.model_dump(mode="json")
                    for market in store.catalog.values()
                ],
                "books": [
                    book.model_dump(mode="json")
                    for book in store.books.values()
                ],
                "gaps": [],
            })
            projection.mark_ready({"latest_cursor": store.cursor})
            projection.update_persistence(store.cursor + 2, queue_depth=0)
            reader = PolymarketLiveReadStore(
                store.root,
                now_provider=lambda: NOW,
                stable_snapshot_max_book_age_seconds=5,
                consumer_maximum_book_age_seconds=5,
                minimum_delivery_headroom_seconds=1,
            )

            for _ in range(200):
                health = projection.health(reader, ["m1"])
                self.assertLessEqual(health.persisted_cursor, health.latest_cursor)
                self.assertEqual(
                    health.persistence_lag_events,
                    health.latest_cursor - health.persisted_cursor,
                )


if __name__ == "__main__":
    unittest.main()
