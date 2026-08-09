from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

import marketcow.polymarket_live as polymarket_live_module
from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.polymarket_contracts import content_sha256
from marketcow.polymarket_live import (
    ClobBooksClient,
    DataApiPublicClient,
    DataApiPublicNormalizer,
    GammaKeysetCatalog,
    GammaLiveNormalizer,
    LiveStateStore,
    PolymarketLiveReadError,
    PolymarketLiveReadStore,
    PolymarketLiveCollector,
    SubscriptionPlanner,
    atomic_write_public_facts,
    build_live_catalog_index,
    build_live_state_index,
    load_scoped_live_store,
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
        "negRisk": neg_risk,
        "negRiskMarketID": "neg-group" if neg_risk else None,
        "groupItemTitle": f"Outcome {market_id}" if neg_risk else None,
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
        self.addCleanup(rows.cleanup)
        self.assertEqual(len(rows), 2)
        self.assertEqual(evidence["pages"], 2)
        self.assertTrue(evidence["complete"])
        self.assertNotIn("offset", calls[-1][1])
        self.assertEqual(calls[-1][1]["after_cursor"], "cursor-2")
        self.assertEqual(sleeps, [0.0])
        self.assertEqual(evidence["retry_count"], 1)

    def test_gamma_keyset_traverses_beyond_1000_pages_until_terminal_cursor(self):
        page_count = 1005
        progress = []

        def requester(_url, **kwargs):
            cursor = kwargs["params"].get("after_cursor")
            page = int(cursor.split("-")[-1]) + 1 if cursor else 1
            payload = {
                "markets": [
                    {"id": f"m{page:04d}-{offset:03d}"}
                    for offset in range(100)
                ],
            }
            if page < page_count:
                payload["next_cursor"] = f"cursor-{page}"
            return Response(payload)

        rows, evidence = GammaKeysetCatalog(
            requester=requester, progress=progress.append,
            progress_every_pages=250,
        ).fetch_all()
        self.addCleanup(rows.cleanup)
        expected_market_count = page_count * 100
        self.assertEqual(len(rows), expected_market_count)
        self.assertEqual(sum(1 for _ in rows), expected_market_count)
        self.assertEqual(evidence["pages"], page_count)
        self.assertEqual(evidence["market_count"], expected_market_count)
        self.assertTrue(evidence["complete"])
        self.assertEqual(evidence["last_cursor"], "cursor-1004")
        self.assertEqual([item["pages"] for item in progress[:-1]], [250, 500, 750, 1000])
        self.assertTrue(progress[-1]["complete"])

    def test_gamma_keyset_empty_nonterminal_page_and_repeated_page_fail(self):
        with self.assertRaisesRegex(RuntimeError, "must be an object"):
            GammaKeysetCatalog(
                requester=lambda *_args, **_kwargs: Response([])
            ).fetch_all()
        with self.assertRaisesRegex(RuntimeError, "no progress"):
            GammaKeysetCatalog(requester=lambda *_args, **_kwargs: Response({
                "markets": [], "next_cursor": "cursor-1",
            })).fetch_all()

        responses = [
            Response({"markets": [gamma_row()], "next_cursor": "cursor-1"}),
            Response({"markets": [gamma_row()], "next_cursor": "cursor-2"}),
        ]
        with self.assertRaisesRegex(RuntimeError, "repeated a page"):
            GammaKeysetCatalog(
                requester=lambda *_args, **_kwargs: responses.pop(0)
            ).fetch_all()

    def test_gamma_keyset_retries_are_per_page_and_session_is_reused(self):
        class Session:
            def __init__(self):
                self.calls = 0

            def get(self, _url, **kwargs):
                self.calls += 1
                page = 1 if "after_cursor" not in kwargs["params"] else 2
                position = self.calls
                if position in {1, 4}:
                    return Response({}, 429, {"Retry-After": "0"})
                if position in {2, 5}:
                    return Response({}, 500, {"Retry-After": "0"})
                return Response({
                    "markets": [gamma_row(
                        f"m{page}", f"0x{page:064x}",
                        (f"yes-{page}", f"no-{page}"),
                    )],
                    **({"next_cursor": "cursor-1"} if page == 1 else {}),
                })

        session = Session()
        sleeps = []
        rows, evidence = GammaKeysetCatalog(
            requester=session.get, sleeper=sleeps.append,
        ).fetch_all()
        self.addCleanup(rows.cleanup)
        self.assertEqual(len(rows), 2)
        self.assertEqual(session.calls, 6)
        self.assertEqual(evidence["retry_count"], 4)
        self.assertEqual(sleeps, [0.0, 0.0, 0.0, 0.0])
        default_catalog = GammaKeysetCatalog()
        self.assertIs(default_catalog.requester.__self__, default_catalog.session)

    def test_gamma_cursor_loop_fails_instead_of_publishing_partial_catalog(self):
        def requester(_url, **_kwargs):
            return Response({"markets": [gamma_row()], "next_cursor": "same"})

        with self.assertRaisesRegex(RuntimeError, "cursor loop"):
            GammaKeysetCatalog(requester=requester).fetch_all()

    def test_failed_large_refresh_preserves_atomic_catalog_across_restart(self):
        store = self.store()
        revision = store.catalog_revision
        catalog_sha = content_sha256(json.loads(
            store.catalog_path.read_text(encoding="utf-8")
        ))
        responses = [
            Response({
                "markets": [gamma_row(
                    "m2", "0x" + "2" * 64, ("yes-2", "no-2"),
                )],
                "next_cursor": "cursor-1",
            }),
            Response({}, 500),
        ]
        collector = PolymarketLiveCollector(
            store,
            GammaKeysetCatalog(
                requester=lambda *_args, **_kwargs: responses.pop(0),
                max_retries_per_page=0,
            ),
            ClobBooksClient(requester=lambda *_args, **_kwargs: Response([])),
        )
        with self.assertRaises(RuntimeError):
            collector.refresh_catalog()
        self.assertEqual(store.catalog_revision, revision)
        self.assertEqual(
            content_sha256(json.loads(store.catalog_path.read_text(encoding="utf-8"))),
            catalog_sha,
        )
        restarted = LiveStateStore(self.root / "live", now_provider=lambda: NOW)
        restarted.recover()
        self.assertEqual(restarted.catalog_revision, revision)
        self.assertEqual(set(restarted.catalog), {"m1"})

    def test_catalog_publish_failure_does_not_mutate_live_in_memory_revision(self):
        store = self.store()
        revision = store.catalog_revision
        rows = [gamma_row("m2", "0x" + "2" * 64, ("yes-2", "no-2"))]
        markets = GammaLiveNormalizer.normalize(rows, NOW)
        with patch(
            "marketcow.polymarket_live._atomic_write_catalog",
            side_effect=OSError("simulated atomic catalog write failure"),
        ):
            with self.assertRaisesRegex(OSError, "simulated"):
                store.replace_catalog(markets, rows)
        self.assertEqual(store.catalog_revision, revision)
        self.assertEqual(set(store.catalog), {"m1"})
        self.assertEqual(set(store.token_to_market), {"yes-1", "no-1"})

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
        self.assertEqual(relation.members, sorted(["POLY:" + "0x" + "1" * 64 + ":a", "POLY:" + "0x" + "2" * 64 + ":c"]))
        self.assertEqual(len(relation.outcome_pairs), 2)
        self.assertEqual(
            {item.no_token_id for item in relation.outcome_pairs}, {"b", "d"},
        )
        second_relation = next(
            item for item in markets[1].relations
            if item.relation_type == "standard_negative_risk"
        )
        self.assertEqual(relation.revision, second_relation.revision)
        self.assertIsNot(
            relation.outcome_pairs[0], second_relation.outcome_pairs[0],
        )
        self.assertTrue(markets[0].rules.rules_complete)
        self.assertTrue(markets[0].rules.fee_schedule.complete)
        changed = [dict(rows[0]), rows[1]]
        changed[0]["updatedAt"] = "2026-08-03T04:01:00Z"
        self.assertNotEqual(
            markets[0].metadata_revision,
            GammaLiveNormalizer.normalize(changed, NOW)[0].metadata_revision,
        )
        current_fee = gamma_row("m3", "0x" + "3" * 64, ("e", "f"))
        current_fee["fee_schedule"] = {
            **current_fee["fee_schedule"], "taker_rate": "0.04",
        }
        current_fee.pop("takerBaseFee", None)
        market = GammaLiveNormalizer.normalize([current_fee], NOW)[0]
        self.assertTrue(market.rules.fee_schedule.complete)
        self.assertEqual(market.rules.fee_schedule.taker_rate, "0.04")
        self.assertEqual(market.rules.fee_schedule.rounding_mode, "UNSPECIFIED")
        self.assertEqual(
            market.rules.fee_schedule.calculation_status, "informational_only",
        )

    def test_negative_risk_relation_revision_scales_by_group_not_catalog(self):
        rows = [
            gamma_row(
                f"neg-{index}", f"0x{index + 10:064x}",
                (f"yes-{index}", f"no-{index}"), neg_risk=True,
            )
            for index in range(100)
        ]
        markets = GammaLiveNormalizer.normalize(rows, NOW)
        self.assertEqual(len(markets), 100)
        relations = [
            next(
                relation for relation in market.relations
                if relation.relation_type == "standard_negative_risk"
            )
            for market in markets
        ]
        self.assertEqual(len({item.revision for item in relations}), 1)
        self.assertTrue(all(len(item.outcome_pairs) == 100 for item in relations))
        self.assertTrue(all(item.complete for item in relations))

    def test_typed_nautilus_facts_have_explicit_source_revisions(self):
        market = GammaLiveNormalizer.normalize([gamma_row()], NOW)[0]
        instrument = market.rules.instrument
        fee = market.rules.fee_schedule
        self.assertTrue(instrument.complete)
        self.assertEqual(instrument.settlement_currency, "pUSD")
        self.assertEqual(instrument.price_increment, "0.01")
        self.assertEqual(instrument.size_increment, "0.01")
        self.assertEqual(instrument.minimum_order_size, "1")
        self.assertEqual(instrument.activation_at, datetime(2026, 8, 1, tzinfo=timezone.utc))
        self.assertEqual(instrument.expiration_at, datetime(2026, 9, 1, tzinfo=timezone.utc))
        self.assertEqual(
            {item.source for item in instrument.provenance},
            {"polymarket_gamma", "polymarket_docs", "polymarket_sdk"},
        )
        provenance = {item.source: item for item in instrument.provenance}
        self.assertEqual(
            provenance["polymarket_docs"].source_url,
            "https://docs.polymarket.com/concepts/pusd",
        )
        self.assertEqual(
            provenance["polymarket_sdk"].revision,
            "b076b04d61135657e25dccc1bbd6866a96bd8c6e",
        )
        self.assertTrue(fee.complete)
        self.assertEqual(fee.currency, "USDC")
        self.assertNotEqual(fee.currency, instrument.settlement_currency)
        self.assertEqual(fee.quantum, "0.00001")
        self.assertEqual(fee.rounding_mode, "UNSPECIFIED")
        self.assertEqual(fee.tie_semantics, "unspecified")
        self.assertEqual(fee.calculation_status, "informational_only")
        mapping = {
            outcome.instrument_id: (outcome.token_id, outcome.outcome)
            for outcome in market.identity.outcomes
        }
        self.assertEqual(len(mapping), 2)
        self.assertEqual({value[0] for value in mapping.values()}, {"yes-1", "no-1"})

    def test_missing_source_facts_remain_explicit_and_fail_closed(self):
        row = gamma_row()
        row.pop("startDate")
        row.pop("orderPriceMinTickSize")
        row["fee_schedule"].pop("taker_rate")
        market = GammaLiveNormalizer.normalize([row], NOW)[0]
        self.assertEqual(
            market.rules.instrument.missing_fields,
            ["activation_at", "price_increment"],
        )
        self.assertEqual(
            market.rules.fee_schedule.missing_fields, ["taker_rate"],
        )
        store = self.store([row])
        store.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        store.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
        frame = store.frame("m1", now=NOW)
        self.assertEqual(frame.status, "fail_closed")
        self.assertIn("instrument_facts_incomplete", frame.reason_codes)
        self.assertIn("fee_schedule_incomplete", frame.reason_codes)

    def test_invalid_instrument_and_fee_intervals_are_explicitly_incomplete(self):
        equal = gamma_row("equal", "0x" + "3" * 64, ("yes-e", "no-e"))
        equal["endDate"] = equal["startDate"]
        reversed_interval = gamma_row(
            "reversed", "0x" + "4" * 64, ("yes-r", "no-r"),
        )
        reversed_interval["startDate"] = "2026-09-02T00:00:00Z"
        fee_interval = gamma_row(
            "fee-interval", "0x" + "5" * 64, ("yes-f", "no-f"),
        )
        fee_interval["fee_schedule"] = {
            **fee_interval["fee_schedule"],
            "effectiveFrom": "2026-09-01T00:00:00Z",
            "effectiveTo": "2026-09-01T00:00:00Z",
        }

        markets = GammaLiveNormalizer.normalize(
            [equal, reversed_interval, fee_interval], NOW,
        )
        self.assertEqual(len(markets), 3)
        by_id = {item.identity.market_id: item for item in markets}
        for market_id in ("equal", "reversed"):
            instrument = by_id[market_id].rules.instrument
            self.assertFalse(instrument.complete)
            self.assertEqual(
                instrument.missing_fields, ["activation_expiration_interval"],
            )
            self.assertFalse(by_id[market_id].rules.rules_complete)
        fee = by_id["fee-interval"].rules.fee_schedule
        self.assertFalse(fee.complete)
        self.assertEqual(fee.missing_fields, ["effective_interval"])

        store = self.store([equal, reversed_interval, fee_interval])
        self.assertEqual(len(store.catalog), 3)
        self.assertFalse(store.catalog["equal"].rules.instrument.complete)
        self.assertFalse(store.catalog["fee-interval"].rules.fee_schedule.complete)

    def test_verified_gamma_spool_is_reused_after_publication_failure(self):
        spool_root = self.root / "verified-spool"
        calls = []

        def requester(_url, **_kwargs):
            calls.append(1)
            return Response({"markets": [gamma_row()]})

        store = LiveStateStore(self.root / "retry-live", now_provider=lambda: NOW)
        collector = PolymarketLiveCollector(
            store,
            GammaKeysetCatalog(requester=requester, spool_root=spool_root),
            ClobBooksClient(requester=lambda *_args, **_kwargs: Response([])),
        )
        with patch(
            "marketcow.polymarket_live._atomic_write_catalog",
            side_effect=OSError("simulated normalization/publication failure"),
        ):
            with self.assertRaisesRegex(OSError, "simulated"):
                collector.refresh_catalog()
        retry_manifest = spool_root / "gamma-keyset-verified-retry.json"
        self.assertTrue(retry_manifest.exists())
        self.assertEqual(len(calls), 1)

        reused = PolymarketLiveCollector(
            store,
            GammaKeysetCatalog(
                requester=lambda *_args, **_kwargs: self.fail(
                    "verified retry must not call Gamma"
                ),
                spool_root=spool_root,
            ),
            ClobBooksClient(requester=lambda *_args, **_kwargs: Response([])),
        ).refresh_catalog()
        self.assertTrue(reused["reused_verified_spool"])
        self.assertEqual(reused["normalized_market_count"], 1)
        self.assertFalse(retry_manifest.exists())

    def test_incomplete_verified_gamma_spool_is_never_reused(self):
        spool_root = self.root / "partial-spool"
        rows, _evidence = GammaKeysetCatalog(
            requester=lambda *_args, **_kwargs: Response({
                "markets": [gamma_row()],
            }),
            spool_root=spool_root,
        ).fetch_all()
        self.addCleanup(rows.cleanup)
        manifest_path = spool_root / "gamma-keyset-verified-retry.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["complete"] = False
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "not terminal and complete"):
            GammaKeysetCatalog(
                requester=lambda *_args, **_kwargs: self.fail(
                    "partial spool must fail before network fallback"
                ),
                spool_root=spool_root,
            ).fetch_all()

    def test_legacy_gamma_bps_is_exactly_normalized_with_official_fee_facts(self):
        row = gamma_row()
        row["fee_schedule"] = {
            "maker_fee_bps": "0", "taker_fee_bps": "20",
        }
        fee = GammaLiveNormalizer.normalize([row], NOW)[0].rules.fee_schedule
        self.assertTrue(fee.complete)
        self.assertEqual(fee.maker_rate, "0")
        self.assertEqual(fee.taker_rate, "0.002")
        self.assertEqual(fee.currency, "USDC")
        self.assertEqual(fee.exponent, "1")
        self.assertEqual(fee.quantum, "0.00001")
        self.assertEqual(
            {item.source for item in fee.provenance},
            {"polymarket_gamma", "polymarket_docs"},
        )

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
        restarted = LiveStateStore(self.root / "live", now_provider=lambda: NOW)
        with self.assertRaisesRegex(RuntimeError, "raw catalog integrity"):
            restarted.recover()

    def test_normalized_catalog_jsonl_is_hash_verified_on_restart(self):
        store = self.store()
        manifest = json.loads(store.catalog_path.read_text(encoding="utf-8"))
        normalized = Path(manifest["normalized_catalog"]["path"])
        normalized.write_bytes(normalized.read_bytes() + b"{}\n")
        restarted = LiveStateStore(self.root / "live", now_provider=lambda: NOW)
        with self.assertRaisesRegex(RuntimeError, "normalized live catalog integrity"):
            restarted.recover()

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

    def test_live_snapshot_levels_are_canonical_best_price_first(self):
        store = self.store()
        raw = snapshot("yes-1", "0.40", "0.42")
        raw["bids"] = [
            {"price": "0.39", "size": "9"},
            {"price": "0.41", "size": "11"},
            {"price": "0.40", "size": "10"},
        ]
        raw["asks"] = [
            {"price": "0.44", "size": "13"},
            {"price": "0.42", "size": "11"},
            {"price": "0.43", "size": "12"},
        ]

        event = store.apply_snapshot(raw, received_at=NOW)

        self.assertEqual(
            [level["price"] for level in event.canonical_payload["bids"]],
            ["0.41", "0.40", "0.39"],
        )
        self.assertEqual(
            [level["price"] for level in event.canonical_payload["asks"]],
            ["0.42", "0.43", "0.44"],
        )
        self.assertEqual(
            event.canonical_payload_sha256,
            content_sha256(event.canonical_payload),
        )

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
        self.assertTrue(all(
            relation.valid_to is not None
            for relation in store.catalog["m1"].relations
        ))
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
        restarted.recover()
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
        self.assertEqual(len(frame.relation_tokens), 2)
        self.assertEqual(
            {item.yes_token_id for item in frame.relation_pairs}, {"a", "c"},
        )

    def test_three_outcome_negative_risk_relation_is_yes_only_and_reversible(self):
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("a", "b"), neg_risk=True),
            gamma_row("m2", "0x" + "2" * 64, ("c", "d"), neg_risk=True),
            gamma_row("m3", "0x" + "3" * 64, ("e", "f"), neg_risk=True),
        ]
        markets = GammaLiveNormalizer.normalize(rows, NOW)
        relation = next(
            item for item in markets[0].relations
            if item.relation_type == "standard_negative_risk"
        )
        self.assertTrue(relation.complete)
        self.assertEqual(len(relation.members), 3)
        self.assertEqual(
            relation.members,
            sorted(item.yes_instrument_id for item in relation.outcome_pairs),
        )
        self.assertEqual(
            {item.yes_token_id for item in relation.outcome_pairs}, {"a", "c", "e"},
        )
        self.assertEqual(
            {item.no_token_id for item in relation.outcome_pairs}, {"b", "d", "f"},
        )
        store = self.store(rows)
        for token, bid, ask in (
            ("a", "0.20", "0.22"), ("b", "0.78", "0.80"),
            ("c", "0.30", "0.32"), ("d", "0.68", "0.70"),
            ("e", "0.40", "0.42"), ("f", "0.58", "0.60"),
        ):
            store.apply_snapshot(snapshot(token, bid, ask), received_at=NOW)
        frame = store.frame("m1", now=NOW)
        self.assertEqual(frame.status, "ready")
        self.assertEqual({item.token_id for item in frame.relation_tokens}, {"a", "c", "e"})
        self.assertEqual(len(frame.relation_pairs), 3)

    def test_ambiguous_negative_risk_pair_metadata_fails_closed(self):
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("a", "b"), neg_risk=True),
            gamma_row("m2", "0x" + "2" * 64, ("c", "d"), neg_risk=True),
            gamma_row("m3", "0x" + "3" * 64, ("e", "f"), neg_risk=True),
        ]
        rows[1].pop("groupItemTitle")
        store = self.store(rows)
        for token, bid, ask in (
            ("a", "0.20", "0.22"), ("b", "0.78", "0.80"),
            ("c", "0.30", "0.32"), ("d", "0.68", "0.70"),
            ("e", "0.40", "0.42"), ("f", "0.58", "0.60"),
        ):
            store.apply_snapshot(snapshot(token, bid, ask), received_at=NOW)
        frame = store.frame("m1", now=NOW)
        self.assertEqual(frame.status, "fail_closed")
        self.assertIn("negative_risk_relation_incomplete", frame.reason_codes)

    def test_negative_risk_member_business_fact_gap_fails_whole_group_closed(self):
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("a", "b"), neg_risk=True),
            gamma_row("m2", "0x" + "2" * 64, ("c", "d"), neg_risk=True),
            gamma_row("m3", "0x" + "3" * 64, ("e", "f"), neg_risk=True),
        ]
        rows[1].pop("orderMinSize")
        rows[2]["fee_schedule"].pop("taker_rate")
        store = self.store(rows)
        for token, bid, ask in (
            ("a", "0.20", "0.22"), ("b", "0.78", "0.80"),
            ("c", "0.30", "0.32"), ("d", "0.68", "0.70"),
            ("e", "0.40", "0.42"), ("f", "0.58", "0.60"),
        ):
            store.apply_snapshot(snapshot(token, bid, ask), received_at=NOW)
        frame = store.frame("m1", now=NOW)
        self.assertEqual(frame.status, "fail_closed")
        self.assertIn(
            "negative_risk_member_instrument_facts_incomplete",
            frame.reason_codes,
        )
        self.assertIn(
            "negative_risk_member_fee_schedule_incomplete", frame.reason_codes,
        )

    def test_full_catalog_refresh_expands_negative_risk_group_and_subscriptions(self):
        first = [
            gamma_row("m1", "0x" + "1" * 64, ("a", "b"), neg_risk=True),
            gamma_row("m2", "0x" + "2" * 64, ("c", "d"), neg_risk=True),
        ]
        store = self.store(first)
        old_revision = store.catalog_revision
        planner = SubscriptionPlanner()
        planner.initial_messages(store.token_to_market)
        expanded = [
            *first,
            gamma_row("m3", "0x" + "3" * 64, ("e", "f"), neg_risk=True),
        ]
        store.replace_catalog(GammaLiveNormalizer.normalize(expanded, NOW), expanded)
        messages = planner.update_messages(store.token_to_market)
        self.assertNotEqual(store.catalog_revision, old_revision)
        self.assertEqual(messages[0]["operation"], "subscribe")
        self.assertEqual(messages[0]["assets_ids"], ["e", "f"])
        relation = next(
            item for item in store.catalog["m1"].relations
            if item.relation_type == "standard_negative_risk"
        )
        self.assertEqual(len(relation.outcome_pairs), 3)
        expanded_relation_revision = relation.revision
        store.apply_websocket({
            "event_type": "market_resolved",
            "market": "0x" + "3" * 64,
            "winning_asset_id": "e",
            "winning_outcome": "Yes",
            "timestamp": "1785739201000",
        }, received_at=NOW)
        store.replace_catalog(GammaLiveNormalizer.normalize(first, NOW), first)
        self.assertNotIn("e", store.token_to_market)
        self.assertNotIn("f", store.token_to_market)
        relation = next(
            item for item in store.catalog["m1"].relations
            if item.relation_type == "standard_negative_risk"
        )
        self.assertEqual(len(relation.outcome_pairs), 2)
        self.assertNotEqual(relation.revision, expanded_relation_revision)

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

    def test_current_scale_token_planning_and_books_batches_remain_bounded(self):
        # Upper bound from the 2026-08-04 real traversal: 126,981 rows × 2 tokens.
        token_count = 253_962
        tokens = [f"token-{index:06d}" for index in range(token_count)]
        shards = SubscriptionPlanner(shard_size=500).shards(tokens)
        self.assertEqual(len(shards), 508)
        self.assertTrue(all(len(item) <= 500 for item in shards))
        groups = SubscriptionPlanner(shard_size=500).connection_groups(tokens, 32)
        self.assertEqual(len(groups), 32)
        self.assertEqual({token for group in groups for token in group}, set(tokens))
        self.assertLessEqual(max(map(len, groups)) - min(map(len, groups)), 500)
        batch_sizes = []

        def requester(_url, **kwargs):
            batch_sizes.append(len(kwargs["json"]))
            return Response([])

        rows = ClobBooksClient(requester=requester, batch_size=500).fetch(tokens)
        self.assertEqual(rows, [])
        self.assertEqual(len(batch_sizes), 508)
        self.assertEqual(sum(batch_sizes), token_count)
        self.assertTrue(all(size <= 500 for size in batch_sizes))

    def test_live_module_has_no_commercial_or_trial_provider_dependency(self):
        body = Path("src/marketcow/polymarket_live.py").read_text(
            encoding="utf-8"
        ).lower()
        for prohibited in ("pmdata", "domeapi", "polymarketdata"):
            self.assertNotIn(prohibited, body)

    def test_provider_neutral_consumer_fixture_covers_complete_resume_flow(self):
        fixture = json.loads(Path(
            "tests/fixtures/polymarket-live-provider-neutral-v2.json"
        ).read_text(encoding="utf-8"))
        self.assertEqual(fixture["schema_version"], "marketcow.polymarket.live.v2")
        ordinary, negative = fixture["bootstrap"]["markets"]
        self.assertEqual(len(ordinary["outcomes"]), 2)
        self.assertEqual(ordinary["settlement_currency"], "pUSD")
        self.assertEqual(ordinary["size_increment"], "0.01")
        self.assertEqual(ordinary["fee_schedule"]["quantum"], "0.00001")
        relation = negative["negative_risk_relation"]
        self.assertEqual(len(relation["members"]), 3)
        self.assertEqual(len(relation["outcome_pairs"]), 3)
        self.assertEqual(
            relation["members"],
            sorted(item["yes_instrument_id"] for item in relation["outcome_pairs"]),
        )
        self.assertEqual(
            fixture["snapshot"]["negative_risk_frame"]["pair_count"], 3,
        )
        self.assertEqual(fixture["events"]["request"]["after_cursor"], 9)
        self.assertEqual(
            fixture["checkpoint_resume"]["resume_request"]["after_cursor"], 11,
        )
        serialized = json.dumps(fixture, sort_keys=True).casefold()
        for provider_field in ("clobtokenids", "negriskmarketid", "groupitemtitle"):
            self.assertNotIn(provider_field, serialized)

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

    def test_clob_books_retries_and_reports_complete_coverage(self):
        responses = [
            Response({}, 429, {"Retry-After": "0"}),
            Response({}, 500, {"Retry-After": "0"}),
            Response([snapshot("a", "0.40", "0.42")]),
        ]
        sleeps = []
        progress = []
        client = ClobBooksClient(
            requester=lambda *_args, **_kwargs: responses.pop(0),
            sleeper=sleeps.append, progress=progress.append,
        )
        rows = client.fetch(["a"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(sleeps, [0.0, 0.0])
        self.assertEqual(client.last_evidence["retry_count"], 2)
        self.assertEqual(client.last_evidence["requested_token_count"], 1)
        self.assertEqual(client.last_evidence["received_book_count"], 1)
        self.assertTrue(progress[-1]["complete"])

    def test_partial_books_bootstrap_starts_degraded_and_keeps_ready_markets(self):
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("yes-1", "no-1")),
            gamma_row("m2", "0x" + "2" * 64, ("yes-2", "no-2")),
        ]
        store = self.store(rows)
        books = ClobBooksClient(requester=lambda *_args, **_kwargs: Response([
            snapshot("yes-1", "0.40", "0.42"),
            snapshot("no-1", "0.58", "0.60"),
        ]))
        collector = PolymarketLiveCollector(
            store,
            GammaKeysetCatalog(requester=lambda *_args, **_kwargs: Response({
                "markets": rows,
            })),
            books,
        )

        recovery_id = asyncio.run(collector.bootstrap_books())
        self.assertTrue(recovery_id)
        health = store.health()
        self.assertEqual(health.status, "degraded")
        self.assertEqual(health.book_token_count, 2)
        self.assertEqual(health.missing_book_token_count, 2)
        self.assertEqual(health.ready_market_count, 1)
        self.assertEqual(store.frame("m1", now=NOW).status, "ready")
        self.assertEqual(store.frame("m2", now=NOW).status, "fail_closed")
        self.assertEqual(books.last_evidence["missing_token_count"], 2)
        self.assertFalse(books.last_evidence["coverage_complete"])
        reader = PolymarketLiveReadStore(store.root, now_provider=lambda: NOW)
        self.assertEqual(reader.gaps(["m2"], unresolved_only=True).count, 2)
        self.assertEqual(reader.snapshot(["m2"]).items[0].status, "fail_closed")

    def test_invalid_rest_book_is_durable_gap_without_aborting_other_books(self):
        store = self.store()
        invalid = snapshot("yes-1", "0.40", "0.42")
        invalid["last_trade_price"] = ""
        recovery_id = store.mark_recovery_started("invalid_official_book")
        coverage = store.recover_from_books([
            invalid,
            snapshot("no-1", "0.58", "0.60"),
        ], recovery_id)
        self.assertEqual(coverage["recovered_token_count"], 1)
        self.assertEqual(coverage["invalid_book_token_count"], 1)
        self.assertEqual(coverage["missing_token_count"], 1)
        self.assertFalse(coverage["coverage_complete"])
        self.assertNotIn("yes-1", store.books)
        self.assertIn("no-1", store.books)
        invalid_events = [
            event for event in store.events
            if event.fail_closed_reason == "invalid_rest_book"
        ]
        self.assertEqual(len(invalid_events), 1)
        self.assertEqual(invalid_events[0].token_id, "yes-1")
        restarted = LiveStateStore(
            self.root / "live", now_provider=lambda: NOW,
        )
        restarted.recover()
        unresolved = [
            gap for gap in restarted.gaps
            if not gap.resolved and gap.token_id == "yes-1"
        ]
        self.assertTrue(any(gap.code == "source_mismatch" for gap in unresolved))
        self.assertEqual(restarted.frame("m1", now=NOW).status, "fail_closed")

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
        app.state.polymarket_live_read.now_provider = lambda: NOW
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
        bootstrap = client.get(
            "/v1/prediction-markets/polymarket/live/bootstrap?market_id=m1"
        )
        full = client.get("/v1/prediction-markets/polymarket/live/bootstrap")
        frame = client.get(
            "/v1/prediction-markets/polymarket/live/snapshot?market_id=m1"
        )
        events = client.get(
            "/v1/prediction-markets/polymarket/live/events?after_cursor=0&market_id=m1"
        )
        health = client.get("/v1/prediction-markets/polymarket/live/health")
        checkpoint = client.get(
            "/v1/prediction-markets/polymarket/live/checkpoint?market_id=m1"
        )
        gaps = client.get(
            "/v1/prediction-markets/polymarket/live/gaps?market_id=m1"
        )
        public = client.get("/v1/prediction-markets/polymarket/live/public-data/trades")
        openapi = client.get("/openapi.json").json()
        self.assertEqual(bootstrap.status_code, 200)
        self.assertEqual(bootstrap.json()["markets"][0]["identity"]["market_id"], "m1")
        self.assertEqual(full.json()["detail"]["code"], "polymarket_full_universe_disabled")
        self.assertEqual(frame.json()["items"][0]["status"], "ready")
        self.assertEqual(len(frame.json()["items"][0]["tokens"]), 2)
        self.assertEqual(len(events.json()["items"]), 2)
        self.assertEqual(health.json()["status"], "index_ready")
        self.assertEqual(len(checkpoint.json()["books"]), 2)
        self.assertEqual(gaps.json()["count"], 0)
        self.assertEqual(health.json()["source_policy"], "official_free_only")
        self.assertEqual(public.json()["count"], 1)
        for path in ("snapshot", "events", "checkpoint", "gaps"):
            unknown = client.get(
                f"/v1/prediction-markets/polymarket/live/{path}?market_id=unknown"
            )
            self.assertEqual(unknown.status_code, 404)
            self.assertEqual(
                unknown.json()["detail"]["code"],
                "polymarket_live_market_not_found",
            )
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
        schemas = openapi["components"]["schemas"]
        self.assertIn("LiveInstrumentFacts", schemas)
        self.assertIn("LiveFeeSchedule", schemas)
        self.assertIn("LiveOutcomePair", schemas)
        self.assertIn("relation_pairs", schemas["MarketFrame"]["properties"])
        self.assertIn("instrument_revision", schemas["MarketFrame"]["properties"])
        self.assertIn("fee_schedule_id", schemas["MarketFrame"]["properties"])
        facts_path.write_bytes(facts_path.read_bytes() + b" ")
        tampered = client.get(
            "/v1/prediction-markets/polymarket/live/public-data/trades"
        )
        self.assertEqual(tampered.status_code, 409)
        self.assertEqual(
            tampered.json()["detail"]["code"],
            "polymarket_public_data_integrity_failed",
        )

    def test_app_construction_defers_polymarket_full_recovery(self):
        settings = Settings(
            raw_path=self.root / "raw", storage_root=self.root,
            allowed_root=self.root.parent,
            postgres_dsn="postgresql://u:p@127.0.0.1/test",
            clickhouse_password="x", profile="test", port=8793,
            postgres_schema="test", clickhouse_database="test",
            clickhouse_spool_path=self.root / "spool",
        )
        writer = LiveStateStore(
            self.root / "prediction-markets" / "polymarket-live",
            now_provider=lambda: NOW,
        )
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)

        app = create_app(settings, Service())
        reader = app.state.polymarket_live
        reader.now_provider = lambda: NOW
        self.assertFalse(reader._recovered)
        self.assertEqual(reader.catalog, {})

        reader.recover()
        self.assertTrue(reader._recovered)
        self.assertEqual(set(reader.catalog), {"m1"})

    def test_catalog_offset_index_preserves_scope_order_and_row_integrity(self):
        root = self.root / "live"
        store = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row("m1"), gamma_row(
            "m2", condition_id="0x" + "2" * 64,
            tokens=("yes-2", "no-2"),
        )]
        store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        reader = PolymarketLiveReadStore(root)

        with patch.object(
            polymarket_live_module,
            "_file_sha256",
            wraps=polymarket_live_module._file_sha256,
        ) as digest:
            result = reader.bootstrap(["m2", "m1", "m2"])
            reader.bootstrap(["m1"])
        self.assertEqual(
            [market.identity.market_id for market in result.markets],
            ["m2", "m1"],
        )
        hashed_paths = [call.args[0] for call in digest.call_args_list]
        self.assertEqual(hashed_paths, [reader.catalog_index_root / (
            f"{result.catalog_revision}.sqlite3"
        )])
        self.assertNotIn(
            Path(json.loads(reader.catalog_path.read_text())["normalized_catalog"]["path"]),
            hashed_paths,
        )
        self.assertEqual(reader.health().status, "index_ready")

        manifest = json.loads(reader.catalog_path.read_text(encoding="utf-8"))
        normalized = Path(manifest["normalized_catalog"]["path"])
        body = normalized.read_bytes()
        normalized.write_bytes(body.replace(b'"market_id":"m1"', b'"market_id":"x1"'))
        with self.assertRaises(PolymarketLiveReadError) as raised:
            reader.bootstrap(["m1"])
        self.assertEqual(
            raised.exception.code, "polymarket_catalog_row_integrity_failed"
        )

    def test_catalog_index_tamper_and_legacy_catalog_fail_closed(self):
        root = self.root / "live"
        store = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        reader = PolymarketLiveReadStore(root)
        manifest = json.loads(reader.catalog_path.read_text(encoding="utf-8"))
        index = Path(manifest["catalog_index"]["path"])
        self.assertEqual(reader.health().status, "index_ready")
        index.write_bytes(index.read_bytes() + b"tampered")
        health = reader.health()
        self.assertEqual(health.status, "integrity_failed")
        self.assertIn("polymarket_catalog_integrity_failed", health.reason_codes)

        manifest.pop("catalog_index")
        reader.catalog_path.write_text(json.dumps(manifest), encoding="utf-8")
        self.assertEqual(reader.health().status, "legacy_unindexed")
        rebuilt = build_live_catalog_index(root)
        self.assertEqual(rebuilt["market_count"], 1)
        self.assertEqual(reader.health().status, "index_ready")

    def test_running_api_durable_tails_collector_writes_after_startup(self):
        settings = Settings(
            raw_path=self.root / "raw", storage_root=self.root,
            allowed_root=self.root.parent,
            postgres_dsn="postgresql://u:p@127.0.0.1/test",
            clickhouse_password="x", profile="test", port=8793,
            postgres_schema="test", clickhouse_database="test",
            clickhouse_spool_path=self.root / "spool",
        )
        app = create_app(settings, Service())
        app.state.polymarket_live.now_provider = lambda: NOW
        app.state.polymarket_live_read.now_provider = lambda: NOW
        client = TestClient(app)
        initial_health = client.get(
            "/v1/prediction-markets/polymarket/live/health"
        ).json()
        self.assertEqual(initial_health["status"], "not_configured")
        self.assertFalse(app.state.polymarket_live._recovered)

        writer = LiveStateStore(
            self.root / "prediction-markets" / "polymarket-live",
            now_provider=lambda: NOW,
        )
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        writer.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        writer.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)

        bootstrap = client.get(
            "/v1/prediction-markets/polymarket/live/bootstrap?market_id=m1"
        ).json()
        frame = client.get(
            "/v1/prediction-markets/polymarket/live/snapshot?market_id=m1"
        ).json()
        events = client.get(
            "/v1/prediction-markets/polymarket/live/events?after_cursor=0&market_id=m1"
        ).json()
        health = client.get(
            "/v1/prediction-markets/polymarket/live/health"
        ).json()
        self.assertEqual(len(bootstrap["markets"]), 1)
        self.assertEqual(frame["items"][0]["status"], "ready")
        self.assertEqual(len(frame["items"][0]["tokens"]), 2)
        self.assertEqual(events["next_cursor"], writer.cursor)
        self.assertEqual(health["market_count"], 1)
        self.assertEqual(health["token_count"], 2)
        self.assertEqual(health["status"], "index_ready")
        self.assertTrue(health["latest_state_ready"])
        self.assertFalse(app.state.polymarket_live._recovered)

    def test_state_index_lag_and_payload_tamper_fail_closed(self):
        root = self.root / "live"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        writer.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        writer.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
        reader = PolymarketLiveReadStore(root, now_provider=lambda: NOW)
        self.assertEqual(reader.snapshot(["m1"]).items[0].status, "ready")

        with sqlite3.connect(writer.state_index.path) as connection:
            connection.execute(
                "UPDATE books SET payload_json=? WHERE token_id='yes-1'", (b"{}",)
            )
            connection.commit()
        with self.assertRaises(PolymarketLiveReadError) as tampered:
            reader.snapshot(["m1"])
        self.assertEqual(tampered.exception.code, "polymarket_state_integrity_failed")

        writer.state_index.rebuild(
            event_path=writer.event_path, books=writer.books, gaps=writer.gaps,
            catalog_revision=writer.catalog_revision,
            token_to_market=writer.token_to_market,
        )
        with writer.event_path.open("ab") as stream:
            stream.write(b"partial-unindexed-event\n")
        with self.assertRaises(PolymarketLiveReadError) as lagging:
            reader.snapshot(["m1"])
        self.assertEqual(lagging.exception.code, "polymarket_state_index_lagging")

    def test_state_index_rebuild_restores_scoped_snapshot_and_event_offsets(self):
        root = self.root / "live"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        writer.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        writer.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
        writer.state_index.path.unlink()
        writer.state_index.manifest_path.unlink()

        rebuilt = build_live_state_index(root)
        self.assertEqual(rebuilt["latest_cursor"], 3)
        self.assertEqual(rebuilt["books_count"], 2)
        self.assertEqual(rebuilt["event_offsets_count"], 3)
        reader = PolymarketLiveReadStore(root, now_provider=lambda: NOW)
        self.assertEqual(reader.snapshot(["m1"]).items[0].status, "ready")
        page = reader.events_after(["m1"], 0, 10)
        self.assertEqual([event.cursor for event in page.items], [2, 3])

    def test_event_resume_before_catalog_transition_expires_explicitly(self):
        root = self.root / "live"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        first = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(first, NOW), first)
        writer.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        cursor = writer.cursor
        refreshed = [
            *first,
            gamma_row(
                "m2", condition_id="0x" + "2" * 64,
                tokens=("yes-2", "no-2"),
            ),
        ]
        writer.replace_catalog(
            GammaLiveNormalizer.normalize(refreshed, NOW), refreshed,
        )

        reader = PolymarketLiveReadStore(root, now_provider=lambda: NOW)
        with self.assertRaises(PolymarketLiveReadError) as expired:
            reader.events_after(["m1"], cursor, 10)
        self.assertEqual(expired.exception.code, "resume_cursor_expired")
        self.assertEqual(expired.exception.status_code, 409)

    def test_state_index_rebuild_does_not_run_full_store_recovery(self):
        root = self.root / "live"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        writer.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        writer.checkpoint()
        refreshed_rows = [
            gamma_row(),
            gamma_row(
                "m2", condition_id="0x" + "2" * 64,
                tokens=("yes-2", "no-2"),
            ),
        ]
        writer.replace_catalog(
            GammaLiveNormalizer.normalize(refreshed_rows, NOW), refreshed_rows,
        )

        with (
            patch.object(
                LiveStateStore, "recover",
                side_effect=AssertionError("full recovery must not run"),
            ),
            patch.object(
                LiveStateStore, "_load_catalog",
                side_effect=AssertionError("catalog models must not be loaded"),
            ),
            patch.object(
                LiveStateStore, "_read_all_events",
                side_effect=AssertionError("events must not be buffered"),
            ),
        ):
            rebuilt = build_live_state_index(root)

        self.assertEqual(rebuilt["latest_cursor"], 3)
        self.assertEqual(rebuilt["books_count"], 1)
        self.assertEqual(rebuilt["event_offsets_count"], 3)

    def test_scoped_writer_hydrates_from_indexes_without_full_recovery(self):
        root = self.root / "live"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        writer.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        writer.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)

        with patch.object(
            LiveStateStore, "_recover_unlocked",
            side_effect=AssertionError("scoped writer must not run full recovery"),
        ):
            scoped = load_scoped_live_store(
                root, ["m1"], now_provider=lambda: NOW,
            )
            event = scoped.apply_snapshot(
                snapshot("yes-1", "0.39", "0.41", "1785739203000"),
                received_at=NOW,
            )

        self.assertEqual(set(scoped.catalog), {"m1"})
        self.assertEqual(set(scoped.token_to_market), {"yes-1", "no-1"})
        self.assertEqual(event.cursor, 4)
        self.assertEqual(
            PolymarketLiveReadStore(root, now_provider=lambda: NOW)
            .snapshot(["m1"]).items[0].cursor,
            4,
        )

    def test_state_index_rebuild_resumes_from_committed_event_boundary(self):
        root = self.root / "live"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        writer.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        writer.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
        published_inode = writer.state_index.path.stat().st_ino
        published_manifest = writer.state_index.manifest_path.read_bytes()
        original_commit = (
            polymarket_live_module.LiveStateIndex._commit_rebuild_batch
        )

        def interrupt_after_first_event(connection, values):
            original_commit(connection, values)
            if (
                values.get("latest_cursor") == 1
                and "build_status" not in values
            ):
                raise RuntimeError("simulated rebuild interruption")

        with (
            patch.object(
                polymarket_live_module.LiveStateIndex,
                "rebuild_batch_size", 1,
            ),
            patch.object(
                polymarket_live_module.LiveStateIndex,
                "_commit_rebuild_batch",
                side_effect=interrupt_after_first_event,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated"):
                build_live_state_index(root)

        self.assertEqual(writer.state_index.path.stat().st_ino, published_inode)
        self.assertEqual(
            writer.state_index.manifest_path.read_bytes(), published_manifest,
        )
        self.assertEqual(
            PolymarketLiveReadStore(root, now_provider=lambda: NOW)
            .snapshot(["m1"]).items[0].status,
            "ready",
        )
        partial = writer.state_index.path.with_name(
            f".{writer.state_index.path.name}.rebuild"
        )
        with sqlite3.connect(partial) as connection:
            metadata = dict(connection.execute("SELECT key, value FROM metadata"))
        self.assertEqual(metadata["latest_cursor"], "1")
        self.assertEqual(metadata["build_status"], "in_progress")

        with patch.object(
            LiveStateStore, "_validate_event",
            wraps=LiveStateStore._validate_event,
        ) as validate_event:
            rebuilt = build_live_state_index(root)
        self.assertEqual(validate_event.call_count, 2)
        self.assertEqual(rebuilt["latest_cursor"], 3)
        self.assertFalse(partial.exists())
        self.assertEqual(
            PolymarketLiveReadStore(root, now_provider=lambda: NOW)
            .snapshot(["m1"]).items[0].status,
            "ready",
        )

    def test_state_index_checkpoint_adds_conservative_unlogged_gaps(self):
        root = self.root / "live"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        writer.apply_websocket({
            "event_type": "last_trade_price",
            "timestamp": "1785739201000",
            "asset_id": "yes-1",
            "price": "0.40",
            "size": "12",
        }, received_at=NOW)
        writer.gaps.extend([
            writer.gaps[-1].model_copy(deep=True),
            writer.gaps[-1].model_copy(
                deep=True, update={"token_id": "no-1"},
            ),
        ])
        writer.checkpoint()

        rebuilt = build_live_state_index(root)

        self.assertEqual(rebuilt["gaps_count"], 2)
        self.assertEqual(
            PolymarketLiveReadStore(root, now_provider=lambda: NOW)
            .gaps(["m1"], unresolved_only=True).count,
            2,
        )

    def test_scoped_reads_are_bounded_to_one_hundred_markets(self):
        root = self.root / "live"
        rows = [
            gamma_row(
                f"m{index}",
                condition_id="0x" + f"{index + 1:064x}",
                tokens=(f"yes-{index}", f"no-{index}"),
            )
            for index in range(100)
        ]
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        reader = PolymarketLiveReadStore(root, now_provider=lambda: NOW)
        market_ids = [f"m{index}" for index in range(100)]
        page = reader.snapshot(market_ids)
        self.assertEqual(page.count, 100)
        self.assertEqual([item.market_id for item in page.items], market_ids)
        self.assertTrue(all(item.status == "fail_closed" for item in page.items))
        with self.assertRaises(PolymarketLiveReadError) as raised:
            reader.bootstrap([*market_ids, "overflow"])
        self.assertEqual(raised.exception.code, "polymarket_scope_too_large")

    def test_event_and_state_index_publish_as_one_reader_visible_boundary(self):
        root = self.root / "live"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        writer.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        writer.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
        reader = PolymarketLiveReadStore(root, now_provider=lambda: NOW)
        entered = threading.Event()
        release = threading.Event()
        original_append = writer.state_index.append

        def delayed_append(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(timeout=2))
            return original_append(*args, **kwargs)

        error = []

        def update_book():
            try:
                writer.apply_websocket({
                    "event_type": "price_change",
                    "timestamp": "1785739201000",
                    "price_changes": [{
                        "asset_id": "yes-1", "side": "BUY",
                        "price": "0.40", "size": "12",
                    }],
                }, received_at=NOW)
            except Exception as exc:  # pragma: no cover - asserted below
                error.append(exc)

        with patch.object(writer.state_index, "append", side_effect=delayed_append):
            worker = threading.Thread(target=update_book)
            worker.start()
            self.assertTrue(entered.wait(timeout=2))
            pages = []
            read_errors = []

            def read_snapshot():
                try:
                    pages.append(reader.snapshot(["m1"]))
                except Exception as exc:  # pragma: no cover - asserted below
                    read_errors.append(exc)

            read_worker = threading.Thread(target=read_snapshot)
            read_worker.start()
            read_worker.join(timeout=0.1)
            self.assertTrue(read_worker.is_alive())
            release.set()
            worker.join(timeout=2)
            read_worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        self.assertFalse(read_worker.is_alive())
        self.assertEqual(error, [])
        self.assertEqual(read_errors, [])
        self.assertEqual(pages[0].cursor, writer.cursor)
        self.assertEqual(pages[0].items[0].status, "ready")

    def test_post_checkpoint_invalid_gap_survives_restart_until_rest_recovery(self):
        store = self.store()
        store.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        store.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
        store.checkpoint()
        store.apply_websocket({
            "event_type": "price_change", "timestamp": "1785739202000",
            "price_changes": [{
                "asset_id": "yes-1", "side": "BUY",
                "price": "0.43", "size": "1",
            }],
        }, received_at=NOW)
        self.assertEqual(store.frame("m1", now=NOW).status, "fail_closed")
        self.assertEqual(sum(not item.resolved for item in store.gaps), 1)

        restarted = LiveStateStore(
            self.root / "live", now_provider=lambda: NOW
        )
        restarted.recover()
        self.assertEqual(sum(not item.resolved for item in restarted.gaps), 1)
        frame = restarted.frame("m1", now=NOW)
        self.assertEqual(frame.status, "fail_closed")
        self.assertIn("unresolved_gap", frame.reason_codes)
        indexed_reader = PolymarketLiveReadStore(
            self.root / "live", now_provider=lambda: NOW
        )
        self.assertEqual(indexed_reader.gaps(["m1"], unresolved_only=True).count, 1)
        self.assertEqual(
            indexed_reader.snapshot(["m1"]).items[0].status, "fail_closed"
        )

        recovery_id = restarted.mark_recovery_started("integrity_recovery")
        self.assertEqual(indexed_reader.health().status, "degraded")
        self.assertIn(
            "recovery_in_progress",
            indexed_reader.snapshot(["m1"]).items[0].reason_codes,
        )
        restarted.recover_from_books([
            snapshot("yes-1", "0.39", "0.41", "1785739203000"),
            snapshot("no-1", "0.59", "0.61", "1785739203000"),
        ], recovery_id)
        self.assertEqual(sum(not item.resolved for item in restarted.gaps), 0)
        self.assertEqual(restarted.frame("m1", now=NOW).status, "ready")
        self.assertEqual(indexed_reader.gaps(["m1"], unresolved_only=True).count, 0)
        self.assertEqual(indexed_reader.snapshot(["m1"]).items[0].status, "ready")

    def test_failed_event_types_are_durable_after_checkpoint(self):
        cases = {
            "missing_snapshot": {
                "event_type": "last_trade_price", "asset_id": "yes-1",
                "timestamp": "1785739201000", "price": "0.41", "size": "1",
            },
            "out_of_order": {
                "event_type": "last_trade_price", "asset_id": "yes-1",
                "timestamp": "1785739199000", "price": "0.41", "size": "1",
            },
        }
        for name, event in cases.items():
            with self.subTest(name=name), TemporaryDirectory() as folder:
                root = Path(folder)
                store = LiveStateStore(root, now_provider=lambda: NOW)
                rows = [gamma_row()]
                store.replace_catalog(
                    GammaLiveNormalizer.normalize(rows, NOW), rows
                )
                if name == "out_of_order":
                    store.apply_snapshot(
                        snapshot("yes-1", "0.40", "0.42"), received_at=NOW
                    )
                store.checkpoint()
                store.apply_websocket(event, received_at=NOW)
                restarted = LiveStateStore(root, now_provider=lambda: NOW)
                restarted.recover()
                unresolved = [gap for gap in restarted.gaps if not gap.resolved]
                self.assertEqual(len(unresolved), 1)
                self.assertEqual(unresolved[0].code, name)

    def test_recovery_rejects_tampered_event_identity_and_payload_hashes(self):
        mutations = {
            "canonical": lambda event: event["canonical_payload"].update(
                {"tampered": True}
            ),
            "raw": lambda event: event["raw_payload"].update({"tampered": True}),
            "event_id": lambda event: event.update({"event_id": "0" * 64}),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), TemporaryDirectory() as folder:
                root = Path(folder)
                store = LiveStateStore(root, now_provider=lambda: NOW)
                rows = [gamma_row()]
                store.replace_catalog(
                    GammaLiveNormalizer.normalize(rows, NOW), rows
                )
                lines = store.event_path.read_text(encoding="utf-8").splitlines()
                event = json.loads(lines[0])
                mutate(event)
                lines[0] = json.dumps(event, separators=(",", ":"))
                store.event_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
                restarted = LiveStateStore(root, now_provider=lambda: NOW)
                with self.assertRaisesRegex(RuntimeError, "hash mismatch|identity mismatch"):
                    restarted.recover()

    def test_self_consistent_but_event_divergent_checkpoint_is_rejected(self):
        store = self.store()
        store.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        store.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
        store.checkpoint()
        payload = json.loads(store.checkpoint_path.read_text(encoding="utf-8"))
        payload["books"]["yes-1"]["last_trade_price"] = "0.99"
        state = {
            "cursor": payload["cursor"],
            "catalog_revision": payload["catalog_revision"],
            "books": payload["books"],
            "unresolved_gaps": payload["unresolved_gaps"],
        }
        payload["state_sha256"] = content_sha256(state)
        store.checkpoint_path.write_text(
            json.dumps(payload, separators=(",", ":")), encoding="utf-8"
        )
        restarted = LiveStateStore(self.root / "live", now_provider=lambda: NOW)
        with self.assertRaisesRegex(RuntimeError, "books disagree"):
            restarted.recover()


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
    def test_pinned_scope_does_not_refresh_full_catalog_on_lifecycle_event(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            store = LiveStateStore(root, now_provider=lambda: NOW)
            rows = [gamma_row()]
            store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
            socket = FakeSocket([json.dumps({
                "event_type": "new_market",
                "market": "0x" + "2" * 64,
                "timestamp": "1785739201000",
            })])
            collector = PolymarketLiveCollector(
                store,
                GammaKeysetCatalog(requester=lambda *_args, **_kwargs: None),
                ClobBooksClient(requester=lambda *_args, **_kwargs: None),
                connector=lambda _url: SocketContext(socket),
                catalog_refresh_on_lifecycle_events=False,
            )
            refreshes = []
            collector.refresh_catalog = lambda: refreshes.append(True)

            asyncio.run(collector._consume(["yes-1"], message_limit=1))

            self.assertEqual(refreshes, [])
            self.assertEqual(set(store.token_to_market), {"yes-1", "no-1"})

    def test_periodic_snapshot_refresh_keeps_quiet_scope_fresh(self):
        with TemporaryDirectory() as folder:
            collector = PolymarketLiveCollector(
                LiveStateStore(Path(folder), now_provider=lambda: NOW),
                GammaKeysetCatalog(
                    requester=lambda *_args, **_kwargs: Response({"markets": []})
                ),
                ClobBooksClient(
                    requester=lambda *_args, **_kwargs: Response([])
                ),
                snapshot_refresh_seconds=0.1,
            )
            reasons = []

            async def refresh(reason="startup"):
                reasons.append(reason)
                return "recovery"

            collector.refresh_books = refresh

            async def scenario():
                task = asyncio.create_task(
                    collector._refresh_snapshots_periodically()
                )
                await asyncio.sleep(0.23)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

            asyncio.run(scenario())
            self.assertGreaterEqual(len(reasons), 2)
            self.assertEqual(set(reasons), {"periodic_snapshot_refresh"})

    def test_large_subscription_group_uses_bounded_connections_and_messages(self):
        with TemporaryDirectory() as folder:
            socket = FakeSocket([])
            collector = PolymarketLiveCollector(
                LiveStateStore(Path(folder), now_provider=lambda: NOW),
                GammaKeysetCatalog(requester=lambda *_args, **_kwargs: None),
                ClobBooksClient(requester=lambda *_args, **_kwargs: None),
                connector=lambda _url: SocketContext(socket),
                shard_size=500,
                max_websocket_connections=32,
            )
            tokens = [f"token-{index}" for index in range(1001)]
            asyncio.run(collector._consume(tokens, message_limit=0))
            messages = [json.loads(item) for item in socket.sent]
            self.assertEqual(len(messages), 3)
            self.assertEqual(messages[0]["type"], "market")
            self.assertTrue(all(len(item["assets_ids"]) <= 500 for item in messages))
            self.assertEqual(
                {token for item in messages for token in item["assets_ids"]},
                set(tokens),
            )

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
