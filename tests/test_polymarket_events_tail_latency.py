from __future__ import annotations

import asyncio
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.polymarket_live import (
    GammaLiveNormalizer,
    LiveStateStore,
    PolymarketLiveReadStore,
)
from tests.test_market_data_api import Service
from tests.test_polymarket_live import gamma_row, snapshot


NOW = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
EVENTS_PATH = "/v1/prediction-markets/polymarket/live/events"


class PolymarketEventsTailLatencyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        settings = Settings(
            raw_path=self.root / "raw",
            storage_root=self.root,
            allowed_root=self.root.parent,
            postgres_dsn="postgresql://u:p@127.0.0.1/test",
            clickhouse_password="x",
            profile="test",
            port=8793,
            postgres_schema="test",
            clickhouse_database="test",
            clickhouse_spool_path=self.root / "spool",
        )
        self.app = create_app(settings, Service())
        self.writer = self.app.state.polymarket_live
        self.writer.now_provider = lambda: NOW
        self.reader = self.app.state.polymarket_live_read
        self.reader.now_provider = lambda: NOW
        rows = [gamma_row()]
        self.writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        self.writer.apply_snapshot(
            snapshot("yes-1", "0.40", "0.42"), received_at=NOW,
        )
        self.writer.apply_snapshot(
            snapshot("no-1", "0.58", "0.60"), received_at=NOW,
        )

    def tearDown(self) -> None:
        self.app.state.polymarket_event_executor.shutdown(
            wait=True, cancel_futures=True,
        )
        self.app.state.polymarket_frame_executor.shutdown(
            wait=True, cancel_futures=True,
        )
        self.temporary.cleanup()

    def test_shared_api_serializes_on_dedicated_executor_and_exports_phases(self):
        threads: list[str] = []
        original = self.reader.events_json

        def observed(*args, **kwargs):
            threads.append(threading.current_thread().name)
            return original(*args, **kwargs)

        with (
            patch.object(self.reader, "events_json", side_effect=observed),
            self.assertLogs("uvicorn.error", level="INFO") as logs,
        ):
            client = TestClient(self.app)
            first = client.get(
                f"{EVENTS_PATH}?market_id=m1&after_cursor=0&limit=1000"
            )
            second = client.get(
                f"{EVENTS_PATH}?market_id=m1"
                f"&after_cursor={first.json()['next_cursor']}&limit=1000"
            )
            metrics = client.get("/metrics").text

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(
            [item["cursor"] for item in first.json()["items"]], [2, 3],
        )
        self.assertEqual(second.json()["items"], [])
        self.assertEqual(second.json()["next_cursor"], first.json()["next_cursor"])
        self.assertTrue(all(
            name.startswith("marketcow-polymarket-events") for name in threads
        ))
        for phase in (
            "executor_queue", "scope_bootstrap", "stable_boundary_wait",
            "sqlite_query", "model_construction", "json_serialization",
        ):
            self.assertIn(f"{phase};dur=", first.headers["server-timing"])
            self.assertIn(f'phase="{phase}"', metrics)
        self.assertIn('phase="response_write"', metrics)
        self.assertIn('phase="total"', metrics)
        self.assertEqual(
            first.headers["content-length"], str(len(first.content)),
        )
        self.assertIn(
            "marketcow_polymarket_events_response_bytes_total "
            f"{len(first.content) + len(second.content)}",
            metrics,
        )
        self.assertEqual(len(logs.records), 2)
        trace = logs.records[0].getMessage()
        for field in (
            '"timestamp":', '"after_cursor":0', '"market_count":1',
            '"response_bytes":', '"response_write_ms":',
        ):
            self.assertIn(field, trace)

    def test_blocked_event_workers_do_not_delay_health_bootstrap_or_snapshot(self):
        original = self.reader.events_json
        started = 0
        started_lock = threading.Lock()
        all_workers_started = threading.Event()
        release = threading.Event()

        def blocked(*args, **kwargs):
            nonlocal started
            with started_lock:
                started += 1
                if started == 4:
                    all_workers_started.set()
            release.wait(timeout=5)
            return original(*args, **kwargs)

        with patch.object(self.reader, "events_json", side_effect=blocked):
            async def exercise():
                transport = httpx.ASGITransport(app=self.app)
                async with httpx.AsyncClient(
                    transport=transport,
                    base_url="http://testserver",
                ) as client:
                    event_tasks = [
                        asyncio.create_task(client.get(
                            f"{EVENTS_PATH}"
                            "?market_id=m1&after_cursor=0&limit=1000"
                        ))
                        for _ in range(4)
                    ]
                    try:
                        self.assertTrue(await asyncio.to_thread(
                            all_workers_started.wait, 2
                        ))
                        began = time.perf_counter()
                        health = await client.get(
                            "/v1/prediction-markets/polymarket/live/health"
                        )
                        bootstrap = await client.get(
                            "/v1/prediction-markets/polymarket/live/bootstrap"
                            "?market_id=m1"
                        )
                        frame = await client.get(
                            "/v1/prediction-markets/polymarket/live/snapshot"
                            "?market_id=m1"
                        )
                        elapsed = time.perf_counter() - began
                    finally:
                        release.set()
                    responses = await asyncio.wait_for(
                        asyncio.gather(*event_tasks), timeout=10
                    )
                    return health, bootstrap, frame, elapsed, responses

            health, bootstrap, frame, concurrent_elapsed, event_responses = (
                asyncio.run(exercise())
            )

        self.assertLess(concurrent_elapsed, 1)
        self.assertEqual(health.status_code, 200)
        self.assertEqual(bootstrap.status_code, 200)
        self.assertEqual(frame.status_code, 200)
        self.assertEqual(frame.json()["items"][0]["status"], "ready")
        self.assertEqual(len(event_responses), 4)
        self.assertTrue(all(response.status_code == 200 for response in event_responses))

    def test_bootstrap_and_snapshot_construct_and_serialize_on_frame_executor(self):
        observed_threads: list[str] = []
        original_bootstrap = self.reader.bootstrap_json
        original_snapshot = self.reader.snapshot_json

        def observed(action):
            def wrapper(*args, **kwargs):
                observed_threads.append(threading.current_thread().name)
                return action(*args, **kwargs)

            return wrapper

        with (
            patch.object(
                self.reader,
                "bootstrap_json",
                side_effect=observed(original_bootstrap),
            ),
            patch.object(
                self.reader,
                "snapshot_json",
                side_effect=observed(original_snapshot),
            ),
        ):
            client = TestClient(self.app)
            bootstrap = client.get(
                "/v1/prediction-markets/polymarket/live/bootstrap?market_id=m1"
            )
            frame = client.get(
                "/v1/prediction-markets/polymarket/live/snapshot?market_id=m1"
            )

        self.assertEqual(bootstrap.status_code, 200)
        self.assertEqual(frame.status_code, 200)
        self.assertEqual(len(observed_threads), 2)
        self.assertTrue(all(
            name.startswith("marketcow-polymarket-frames")
            for name in observed_threads
        ))

    def test_broad_scope_uses_cursor_order_without_temp_market_range_merge(self):
        root = self.root / "broad-cursor-plan"
        rows = [
            gamma_row(
                f"m{index}",
                condition_id="0x" + f"{index + 1:064x}",
                tokens=(f"yes-{index}", f"no-{index}"),
            )
            for index in range(8)
        ]
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        reader = PolymarketLiveReadStore(root, now_provider=lambda: NOW)
        connection = reader._state_reader_connection(reader._state_path())
        statements: list[str] = []
        connection.set_trace_callback(statements.append)

        payload, phases, page = reader.events_json(
            [f"m{index}" for index in range(8)], 0, 1000,
        )

        self.assertEqual(page.items, [])
        self.assertEqual(page.next_cursor, 0)
        self.assertGreater(len(payload), 0)
        self.assertIn("sqlite_query_ms", phases)
        self.assertTrue(any(
            "FROM event_offsets NOT INDEXED" in statement
            for statement in statements
        ))


if __name__ == "__main__":
    unittest.main()
