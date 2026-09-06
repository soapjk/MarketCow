import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import threading
import time
import unittest
from unittest.mock import patch

from marketcow.polymarket_live import (
    GammaLiveNormalizer,
    LiveBook,
    LiveFullSyncResponse,
    LiveStateStore,
    PolymarketLiveReadError,
    PolymarketLiveReadStore,
)
from marketcow.polymarket_live_stream import PolymarketLiveProjection


def gamma_row() -> dict:
    return {
        "id": "m1",
        "conditionId": "0x" + "1" * 64,
        "slug": "market-m1",
        "question": "Will m1 happen?",
        "title": "Market m1",
        "active": True,
        "closed": False,
        "acceptingOrders": True,
        "startDate": "2026-08-01T00:00:00Z",
        "endDate": "2026-09-01T00:00:00Z",
        "clobTokenIds": '["yes-1","no-1"]',
        "outcomes": '["Yes","No"]',
        "orderPriceMinTickSize": "0.01",
        "orderMinSize": "1",
        "feesEnabled": True,
        "fee_schedule": {
            "version": "gamma-test-fee-v1",
            "currency": "USDC",
            "maker_rate": "0",
            "taker_rate": "0.02",
            "formula": "fee = C * rate * p * (1 - p)",
            "exponent": "1",
            "quantum": "0.00001",
            "effectiveFrom": "2026-08-01T00:00:00Z",
        },
        "negRisk": False,
        "updatedAt": "2026-08-03T03:59:00Z",
        "events": [{"id": "event-1"}],
    }


def snapshot(token: str, bid: str, ask: str) -> dict:
    return {
        "event_type": "book",
        "asset_id": token,
        "timestamp": "1785739200000",
        "hash": f"hash-{token}",
        "tick_size": "0.01",
        "min_order_size": "1",
        "bids": [{"price": bid, "size": "10"}],
        "asks": [{"price": ask, "size": "11"}],
        "last_trade_price": bid,
    }


class PolymarketLiveProjectionFreshnessTest(unittest.TestCase):
    def populated_projection(self, root: Path, recovery_id: str | None):
        now = datetime(2026, 9, 6, tzinfo=timezone.utc)
        store = LiveStateStore(root, now_provider=lambda: now)
        rows = [gamma_row()]
        store.replace_catalog(GammaLiveNormalizer.normalize(rows, now), rows)
        store.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=now)
        store.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=now)
        projection = PolymarketLiveProjection(replay_capacity=100)
        projection.install_state({
            "schema_version": "marketcow.polymarket.live-stream.v1",
            "type": "state",
            "catalog_revision": store.catalog_revision,
            "catalog_source": store.catalog_source,
            "latest_cursor": store.cursor,
            "persisted_cursor": store.cursor,
            "active_recovery_id": recovery_id,
            "markets": [
                market.model_dump(mode="json")
                for market in store.catalog.values()
            ],
            "books": [
                book.model_dump(mode="json") for book in store.books.values()
            ],
            "gaps": [],
        })
        projection.mark_ready({"latest_cursor": store.cursor})
        return now, store, projection

    def test_bounded_wait_observes_authoritative_gap_recovery(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "bounded-hot-recovery"
            now, store, projection = self.populated_projection(
                root, "recovery-in-progress",
            )
            reader = PolymarketLiveReadStore(
                root,
                now_provider=lambda: now + timedelta(seconds=1),
                stable_read_wait_seconds=0.5,
                stable_read_poll_seconds=0.005,
                stable_snapshot_max_book_age_seconds=5,
                consumer_maximum_book_age_seconds=5,
                minimum_delivery_headroom_seconds=1,
            )

            def complete_recovery() -> None:
                time.sleep(0.05)
                with projection._lock:
                    projection._active_recovery_id = None

            worker = threading.Thread(target=complete_recovery)
            worker.start()
            try:
                body, phases, full_sync = projection.full_sync_json(
                    reader, ["m1"],
                )
            finally:
                worker.join(timeout=1)

            self.assertEqual(json.loads(body)["cursor"], store.cursor)
            self.assertFalse(full_sync.health.latest_state_ready)
            self.assertIn("recovery_in_progress", full_sync.snapshot.items[0].reason_codes)
            self.assertEqual(phases["stable_wait_attempts"], 0)
            _, _, recovered = projection.full_sync_json(reader, ["m1"])
            self.assertTrue(recovered.health.latest_state_ready)

    def test_bounded_wait_keeps_persistent_recovery_fail_closed(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "bounded-hot-failure"
            now, _, projection = self.populated_projection(
                root, "recovery-still-in-progress",
            )
            reader = PolymarketLiveReadStore(
                root,
                now_provider=lambda: now + timedelta(seconds=1),
                stable_read_wait_seconds=0.03,
                stable_read_poll_seconds=0.005,
                stable_snapshot_max_book_age_seconds=5,
                consumer_maximum_book_age_seconds=5,
                minimum_delivery_headroom_seconds=1,
            )

            started = time.perf_counter()
            _, _, result = projection.full_sync_json(reader, ["m1"])
            self.assertEqual(result.snapshot.items[0].status, "fail_closed")
            self.assertEqual(result.snapshot.items[0].decision_owner, "consumer")
            self.assertIsNone(result.snapshot.items[0].open_position_allowed)

    def test_full_sync_reserves_for_second_serialization_scheduler_jitter(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "bounded-serialization"
            now, _, projection = self.populated_projection(root, None)
            reader = PolymarketLiveReadStore(
                root,
                now_provider=lambda: now + timedelta(seconds=1),
                stable_read_wait_seconds=0.1,
                stable_read_poll_seconds=0.005,
                stable_snapshot_max_book_age_seconds=5,
                consumer_maximum_book_age_seconds=5,
                minimum_delivery_headroom_seconds=0,
            )
            original = LiveFullSyncResponse.model_dump_json
            calls = 0

            def jittered(model, *args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 2:
                    time.sleep(0.01)
                return original(model, *args, **kwargs)

            with patch.object(
                LiveFullSyncResponse, "model_dump_json", jittered,
            ):
                _, phases, full_sync = projection.full_sync_json(
                    reader, ["m1"],
                )

            self.assertEqual(calls, 1)
            self.assertIsNone(full_sync.freshness_budget_remaining_ms)
            self.assertEqual(full_sync.freshness_policy, "consumer_decides")

    def test_trade_event_does_not_roll_back_confirmed_book_receipt(self):
        now = datetime(2026, 9, 6, tzinfo=timezone.utc)
        payload = {
            "token_id": "11",
            "condition_id": "condition",
            "book_epoch": "epoch",
            "sequence": 1,
            "exchange_at": now.isoformat(),
            "received_at": now.isoformat(),
            "tick_version": "tick-v1",
            "tick_size": "0.01",
            "bids": [{"price": "0.4", "size": "10"}],
            "asks": [{"price": "0.6", "size": "10"}],
            "last_trade_price": None,
            "state_checksum": "0" * 64,
            "source_hash": "1" * 64,
        }
        projection = PolymarketLiveProjection()
        confirmed = LiveBook.model_validate({
            **payload,
            "received_at": (now + timedelta(seconds=10)).isoformat(),
        })
        projection._books[confirmed.token_id] = confirmed
        event = SimpleNamespace(
            applied=True,
            event_type="last_trade_price",
            token_id="11",
            market_id=None,
            canonical_payload={**payload, "last_trade_price": "0.5"},
            gaps=[],
        )

        projection._apply_event_state(event)

        applied = projection._books["11"]
        self.assertEqual(applied.last_trade_price, "0.5")
        self.assertEqual(applied.received_at, confirmed.received_at)


if __name__ == "__main__":
    unittest.main()
