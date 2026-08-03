from __future__ import annotations

import asyncio
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.polymarket_live import (
    ClobBooksClient,
    DataApiPublicClient,
    DataApiPublicNormalizer,
    GammaKeysetCatalog,
    GammaLiveNormalizer,
    LiveStateStore,
    PolymarketLiveCollector,
    SubscriptionPlanner,
    atomic_write_public_facts,
)
from tests.test_market_data_api import Service


NOW = datetime(2026, 8, 3, 4, 0, tzinfo=timezone.utc)


def gamma_row(
    market_id: str = "m1",
    condition_id: str = "0x" + "1" * 64,
    tokens: tuple[str, str] = ("yes-1", "no-1"),
    *,
    neg_risk: bool = False,
) -> dict:
    return {
        "id": market_id,
        "conditionId": condition_id,
        "slug": f"market-{market_id}",
        "question": f"Will {market_id} happen?",
        "title": f"Market {market_id}",
        "active": True,
        "closed": False,
        "acceptingOrders": True,
        "startDate": "2026-08-01T00:00:00Z",
        "endDate": "2026-09-01T00:00:00Z",
        "clobTokenIds": json.dumps(list(tokens)),
        "outcomes": '["Yes","No"]',
        "orderPriceMinTickSize": "0.01",
        "orderMinSize": "1",
        "feesEnabled": True,
        "fee_schedule": {"maker_fee_bps": "0", "taker_fee_bps": "20"},
        "negRisk": neg_risk,
        "negRiskMarketID": "neg-group" if neg_risk else None,
        "updatedAt": "2026-08-03T03:59:00Z",
        "events": [{"id": "event-1"}],
    }


def snapshot(token: str, bid: str, ask: str, timestamp: str = "1785739200000") -> dict:
    return {
        "event_type": "book",
        "asset_id": token,
        "timestamp": timestamp,
        "hash": f"hash-{token}-{timestamp}",
        "tick_size": "0.01",
        "min_order_size": "1",
        "bids": [{"price": bid, "size": "10"}],
        "asks": [{"price": ask, "size": "11"}],
        "last_trade_price": bid,
    }


class Response:
    def __init__(self, payload, status=200, headers=None):
        self.text = json.dumps(payload)
        self.status_code = status
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class PolymarketLiveTest(unittest.TestCase):
    def setUp(self):
        self.folder = TemporaryDirectory()
        self.root = Path(self.folder.name)

    def tearDown(self):
        self.folder.cleanup()

    def store(self, rows=None, *, capacity=100_000, now=NOW):
        store = LiveStateStore(
            self.root / "live", replay_capacity=capacity,
            now_provider=lambda: now,
        )
        raw_rows = rows or [gamma_row()]
        store.replace_catalog(GammaLiveNormalizer.normalize(raw_rows, NOW), raw_rows)
        return store

    def test_gamma_keyset_complete_pagination_and_rate_limit_backoff(self):
        calls, sleeps = [], []
        responses = [
            Response({}, 429, {"Retry-After": "0"}),
            Response({"markets": [gamma_row()], "next_cursor": "cursor-2"}),
            Response({"markets": [gamma_row("m2", "0x" + "2" * 64, ("yes-2", "no-2"))]}),
        ]

        def requester(url, **kwargs):
            calls.append((url, kwargs["params"]))
            return responses.pop(0)

        rows, evidence = GammaKeysetCatalog(
            requester=requester, sleeper=sleeps.append
        ).fetch_all()
        self.assertEqual(len(rows), 2)
        self.assertEqual(evidence["pages"], 2)
        self.assertTrue(evidence["complete"])
        self.assertNotIn("offset", calls[-1][1])
        self.assertEqual(calls[-1][1]["after_cursor"], "cursor-2")
        self.assertEqual(sleeps, [0.0])

    def test_gamma_cursor_loop_fails_instead_of_publishing_partial_catalog(self):
        def requester(_url, **_kwargs):
            return Response({"markets": [gamma_row()], "next_cursor": "same"})

        with self.assertRaisesRegex(RuntimeError, "cursor loop"):
            GammaKeysetCatalog(requester=requester).fetch_all()

    def test_gamma_normalization_relations_and_metadata_versions(self):
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("a", "b"), neg_risk=True),
            gamma_row("m2", "0x" + "2" * 64, ("c", "d"), neg_risk=True),
        ]
        markets = GammaLiveNormalizer.normalize(rows, NOW)
        relation = next(
            item for item in markets[0].relations
            if item.relation_type == "standard_negative_risk"
        )
        self.assertEqual(len(relation.members), 4)
        self.assertTrue(markets[0].rules.rules_complete)
        self.assertTrue(markets[0].rules.fee_complete)
        changed = [dict(rows[0]), rows[1]]
        changed[0]["updatedAt"] = "2026-08-03T04:01:00Z"
        self.assertNotEqual(
            markets[0].metadata_revision,
            GammaLiveNormalizer.normalize(changed, NOW)[0].metadata_revision,
        )
        current_fee = gamma_row("m3", "0x" + "3" * 64, ("e", "f"))
        current_fee["fee_schedule"] = {
            "rate": "0.04", "exponent": "1", "rebateRate": "0.25",
            "takerOnly": True,
        }
        current_fee.pop("takerBaseFee", None)
        market = GammaLiveNormalizer.normalize([current_fee], NOW)[0]
        self.assertTrue(market.rules.fee_complete)
        self.assertEqual(market.rules.fee_rate, "0.04")
        self.assertEqual(market.rules.fee_rounding_mode, "UNSPECIFIED")
        self.assertEqual(market.rules.fee_calculation_status, "informational_only")

    def test_catalog_raw_revisions_are_immutable_and_verified_on_restart(self):
        rows = [gamma_row()]
        store = self.store(rows)
        first = store.raw_catalog_path
        changed = [dict(rows[0], updatedAt="2026-08-03T04:01:00Z")]
        store.replace_catalog(GammaLiveNormalizer.normalize(changed, NOW), changed)
        second = store.raw_catalog_path
        self.assertNotEqual(first, second)
        self.assertTrue(first.exists())
        self.assertTrue(second.exists())
        second.write_bytes(second.read_bytes() + b" ")
        with self.assertRaisesRegex(RuntimeError, "raw catalog integrity"):
            LiveStateStore(self.root / "live", now_provider=lambda: NOW)

    def test_subscription_sharding_and_dynamic_diff(self):
        planner = SubscriptionPlanner(shard_size=2)
        initial = planner.initial_messages(["d", "a", "c", "b"])
        self.assertEqual([len(item["assets_ids"]) for item in initial], [2, 2])
        self.assertTrue(all(item["custom_feature_enabled"] for item in initial))
        updates = planner.update_messages(["b", "c", "e"])
        self.assertEqual(updates[0]["operation"], "subscribe")
        self.assertEqual(updates[0]["assets_ids"], ["e"])
        self.assertEqual(updates[1]["operation"], "unsubscribe")
        self.assertEqual(updates[1]["assets_ids"], ["a", "d"])

    def test_books_bootstrap_delta_checksum_and_two_token_frame(self):
        store = self.store()
        store.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        store.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
        envelope = store.apply_websocket({
            "event_type": "price_change",
            "market": "0x" + "1" * 64,
            "timestamp": "1785739201000",
            "price_changes": [{
                "asset_id": "yes-1", "side": "BUY", "price": "0.40", "size": "12",
                "best_bid": "0.40", "best_ask": "0.42",
            }],
        }, received_at=NOW + timedelta(seconds=1))[0]
        self.assertTrue(envelope.applied)
        self.assertEqual(store.books["yes-1"].bids[0]["size"], "12")
        self.assertEqual(len(store.books["yes-1"].state_checksum), 64)
        frame = store.frame("m1", now=NOW + timedelta(seconds=1))
        self.assertEqual(frame.status, "ready")
        self.assertEqual(len(frame.tokens), 2)
        self.assertNotEqual(frame.tokens[0].book_epoch, "")

    def test_duplicate_out_of_order_and_invalid_book_fail_closed(self):
        store = self.store()
        store.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        raw = {
            "event_type": "last_trade_price", "asset_id": "yes-1",
            "timestamp": "1785739201000", "price": "0.41", "size": "2", "side": "BUY",
        }
        self.assertEqual(len(store.apply_websocket(raw, received_at=NOW)), 1)
        self.assertEqual(store.apply_websocket(raw, received_at=NOW), [])
        out_of_order = dict(raw, timestamp="1785739199000", price="0.39")
        rejected = store.apply_websocket(out_of_order, received_at=NOW)[0]
        self.assertFalse(rejected.applied)
        self.assertEqual(rejected.fail_closed_reason, "out_of_order")
        crossed = {
            "event_type": "price_change", "timestamp": "1785739202000",
            "price_changes": [{
                "asset_id": "yes-1", "side": "BUY", "price": "0.43", "size": "1",
            }],
        }
        rejected = store.apply_websocket(crossed, received_at=NOW)[0]
        self.assertFalse(rejected.applied)
        self.assertEqual(rejected.fail_closed_reason, "invalid_book_update")
        self.assertGreaterEqual(sum(not item.resolved for item in store.gaps), 2)

    def test_market_resolved_updates_lifecycle_and_is_retained_after_active_refresh(self):
        store = self.store()
        event = store.apply_websocket({
            "event_type": "market_resolved",
            "market": "0x" + "1" * 64,
            "winning_asset_id": "yes-1",
            "winning_outcome": "Yes",
            "timestamp": "1785739201000",
        }, received_at=NOW)[0]
        self.assertTrue(event.applied)
        self.assertEqual(store.catalog["m1"].lifecycle_state, "resolved")
        self.assertEqual(store.catalog["m1"].resolution, "Yes")
        self.assertEqual(store.token_to_market, {})
        store.replace_catalog([], [])
        self.assertIn("m1", store.catalog)
        self.assertEqual(store.catalog["m1"].lifecycle_state, "resolved")

    def test_disconnect_recovery_new_epoch_checkpoint_and_restart(self):
        store = self.store()
        store.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        store.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
        old_epoch = store.books["yes-1"].book_epoch
        recovery_id = store.mark_recovery_started("disconnect")
        rows = [
            snapshot("yes-1", "0.39", "0.41", "1785739203000"),
            snapshot("no-1", "0.59", "0.61", "1785739203000"),
        ]
        store.recover_from_books(rows, recovery_id)
        self.assertNotEqual(store.books["yes-1"].book_epoch, old_epoch)
        self.assertEqual(sum(not item.resolved for item in store.gaps), 0)
        checkpoint = store.checkpoint()
        store.apply_websocket({
            "event_type": "price_change", "timestamp": "1785739204000",
            "price_changes": [{
                "asset_id": "yes-1", "side": "BUY", "price": "0.39", "size": "19",
            }],
        }, received_at=NOW)
        restarted = LiveStateStore(self.root / "live", now_provider=lambda: NOW)
        self.assertGreater(restarted.cursor, checkpoint.cursor)
        self.assertEqual(
            restarted.books["yes-1"].state_checksum,
            store.books["yes-1"].state_checksum,
        )
        self.assertEqual(restarted.books["yes-1"].bids[0]["size"], "19")

    def test_negative_risk_frame_requires_all_relation_members(self):
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("a", "b"), neg_risk=True),
            gamma_row("m2", "0x" + "2" * 64, ("c", "d"), neg_risk=True),
        ]
        store = self.store(rows)
        for token, bid, ask in (
            ("a", "0.40", "0.42"), ("b", "0.58", "0.60"),
        ):
            store.apply_snapshot(snapshot(token, bid, ask), received_at=NOW)
        frame = store.frame("m1", now=NOW)
        self.assertEqual(frame.status, "fail_closed")
        self.assertIn("negative_risk_member_missing", frame.reason_codes)
        for token, bid, ask in (
            ("c", "0.30", "0.32"), ("d", "0.68", "0.70"),
        ):
            store.apply_snapshot(snapshot(token, bid, ask), received_at=NOW)
        frame = store.frame("m1", now=NOW)
        self.assertEqual(frame.status, "ready")
        self.assertEqual(len(frame.relation_tokens), 4)

    def test_resume_cursor_expiry_and_long_stability_bound(self):
        store = self.store(capacity=10)
        store.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        for index in range(250):
            store.apply_websocket({
                "event_type": "last_trade_price", "asset_id": "yes-1",
                "timestamp": str(1785739201000 + index),
                "price": "0.41", "size": str(index + 1), "side": "BUY",
            }, received_at=NOW)
        with self.assertRaisesRegex(RuntimeError, "resume_cursor_expired"):
            store.events_after(0, 10)
        items, has_more = store.events_after(store.cursor - 5, 3)
        self.assertEqual(len(items), 3)
        self.assertTrue(has_more)
        self.assertEqual(len(store.events), 10)

    def test_full_market_catalog_and_subscription_load_shape(self):
        rows = [
            gamma_row(
                f"m{index}", f"0x{index:064x}",
                (f"yes-{index}", f"no-{index}"),
            )
            for index in range(1000)
        ]
        store = self.store(rows)
        self.assertEqual(len(store.catalog), 1000)
        self.assertEqual(len(store.token_to_market), 2000)
        shards = SubscriptionPlanner(shard_size=500).initial_messages(
            store.token_to_market
        )
        self.assertEqual(len(shards), 4)
        self.assertTrue(all(len(item["assets_ids"]) == 500 for item in shards))

    def test_live_module_has_no_commercial_or_trial_provider_dependency(self):
        body = Path("src/marketcow/polymarket_live.py").read_text(
            encoding="utf-8"
        ).lower()
        for prohibited in ("pmdata", "domeapi", "polymarketdata"):
            self.assertNotIn(prohibited, body)

    def test_public_data_decimal_profile_and_semantic_boundary(self):
        rows = [{
            "proxyWallet": "0xABC", "asset": "yes-1",
            "conditionId": "0x" + "1" * 64, "side": "BUY",
            "price": "0.41", "size": "3.50", "timestamp": "1785739200",
            "transactionHash": "0xDEF", "outcome": "Yes",
            "name": "public profile", "pseudonym": "anon",
        }]
        normalized = DataApiPublicNormalizer.normalize("trades", rows, NOW)
        canonical = normalized[0]["canonical_payload"]
        self.assertEqual(canonical["decimal_values"]["size"], "3.50")
        self.assertEqual(canonical["wallet"], "0xabc")
        self.assertIn("not a verified real-world identity", canonical["semantic_boundary"])
        self.assertEqual(len(normalized[0]["canonical_payload_sha256"]), 64)
        self.assertEqual(len(normalized[0]["raw_payload_sha256"]), 64)
        with self.assertRaisesRegex(ValueError, "binary float"):
            DataApiPublicNormalizer.normalize("trades", [{**rows[0], "price": 0.41}], NOW)

    def test_public_data_client_routes_only_documented_free_endpoints(self):
        calls = []

        def requester(url, **kwargs):
            calls.append((url, kwargs["params"]))
            return Response([])

        client = DataApiPublicClient(requester=requester)
        client.fetch("trades", params={"limit": 10})
        client.fetch("activity", params={"user": "0xabc"})
        client.fetch("positions", params={"market": "0xmarket"})
        client.fetch("holders", params={"market": ["0xmarket"]})
        self.assertEqual(
            [item[0] for item in calls],
            [
                "https://data-api.polymarket.com/trades",
                "https://data-api.polymarket.com/activity",
                "https://data-api.polymarket.com/v1/market-positions",
                "https://data-api.polymarket.com/holders",
            ],
        )
        with self.assertRaisesRegex(ValueError, "unsupported"):
            client.fetch("orders", params={})

    def test_clob_books_batching(self):
        batches = []

        def requester(url, **kwargs):
            batches.append(kwargs["json"])
            return Response([
                snapshot(item["token_id"], "0.40", "0.42")
                for item in kwargs["json"]
            ])

        rows = ClobBooksClient(requester=requester, batch_size=2).fetch(["a", "b", "c"])
        self.assertEqual(len(rows), 3)
        self.assertEqual([len(batch) for batch in batches], [2, 1])

    def test_api_contract_openapi_events_frames_health_and_public_facts(self):
        settings = Settings(
            raw_path=self.root / "raw", storage_root=self.root, allowed_root=self.root.parent,
            postgres_dsn="postgresql://u:p@127.0.0.1/test", clickhouse_password="x",
            profile="test", port=8793, postgres_schema="test", clickhouse_database="test",
            clickhouse_spool_path=self.root / "spool",
        )
        app = create_app(settings, Service())
        store = app.state.polymarket_live
        store.now_provider = lambda: NOW
        rows = [gamma_row()]
        store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        store.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        store.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
        facts = DataApiPublicNormalizer.normalize("trades", [{
            "proxyWallet": "0xabc", "asset": "yes-1", "price": "0.4", "size": "1",
            "timestamp": "1785739200", "transactionHash": "0x1",
        }], NOW)
        facts_path = atomic_write_public_facts(store.root, "trades", facts)
        client = TestClient(app)
        bootstrap = client.get("/v1/prediction-markets/polymarket/live/bootstrap")
        frame = client.get("/v1/prediction-markets/polymarket/live/snapshot")
        events = client.get("/v1/prediction-markets/polymarket/live/events?after_cursor=0")
        health = client.get("/v1/prediction-markets/polymarket/live/health")
        public = client.get("/v1/prediction-markets/polymarket/live/public-data/trades")
        openapi = client.get("/openapi.json").json()
        self.assertEqual(bootstrap.status_code, 200)
        self.assertEqual(frame.json()["items"][0]["status"], "ready")
        self.assertGreater(len(events.json()["items"]), 0)
        self.assertEqual(health.json()["source_policy"], "official_free_only")
        self.assertEqual(public.json()["count"], 1)
        for path in (
            "/v1/prediction-markets/polymarket/live/bootstrap",
            "/v1/prediction-markets/polymarket/live/snapshot",
            "/v1/prediction-markets/polymarket/live/events",
            "/v1/prediction-markets/polymarket/live/checkpoint",
            "/v1/prediction-markets/polymarket/live/health",
            "/v1/prediction-markets/polymarket/live/gaps",
            "/v1/prediction-markets/polymarket/live/public-data/{kind}",
        ):
            self.assertIn(path, openapi["paths"])
            response_schema = openapi["paths"][path]["get"]["responses"]["200"][
                "content"
            ]
            self.assertIn("application/json", response_schema)
            self.assertTrue(response_schema["application/json"]["schema"])
        facts_path.write_bytes(facts_path.read_bytes() + b" ")
        tampered = client.get(
            "/v1/prediction-markets/polymarket/live/public-data/trades"
        )
        self.assertEqual(tampered.status_code, 409)
        self.assertEqual(
            tampered.json()["detail"]["code"],
            "polymarket_public_data_integrity_failed",
        )


class FakeSocket:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent = []

    async def send(self, value):
        self.sent.append(value)

    async def recv(self):
        value = self.messages.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


class SocketContext:
    def __init__(self, socket):
        self.socket = socket

    async def __aenter__(self):
        return self.socket

    async def __aexit__(self, *_):
        return False


class PolymarketLiveCollectorTest(unittest.TestCase):
    def test_public_websocket_subscription_ping_and_message(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            store = LiveStateStore(root, now_provider=lambda: NOW)
            rows = [gamma_row()]
            store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
            socket = FakeSocket([
                asyncio.TimeoutError(),
                json.dumps(snapshot("yes-1", "0.40", "0.42")),
            ])
            collector = PolymarketLiveCollector(
                store,
                GammaKeysetCatalog(requester=lambda *_args, **_kwargs: None),
                ClobBooksClient(requester=lambda *_args, **_kwargs: None),
                connector=lambda _url: SocketContext(socket),
                heartbeat_seconds=0.01,
            )
            asyncio.run(collector._consume(["yes-1"], message_limit=1))
            subscription = json.loads(socket.sent[0])
            self.assertEqual(subscription["type"], "market")
            self.assertTrue(subscription["custom_feature_enabled"])
            self.assertIn("PING", socket.sent)
            self.assertIn("yes-1", store.books)


if __name__ == "__main__":
    unittest.main()
