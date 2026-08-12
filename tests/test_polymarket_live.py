from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
import unittest
import requests
from unittest.mock import patch
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

import marketcow.polymarket_live as polymarket_live_module
from scripts.run_polymarket_scoped_manifest import (
    validate_snapshot_refresh_seconds,
)
from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.polymarket_contracts import content_sha256
from marketcow.polymarket_live import (
    CandidateSnapshot,
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
    build_live_candidate_snapshot,
    build_live_state_index,
    catch_up_live_state_index,
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

    def test_periodic_snapshot_mode_audits_stale_events_as_resolved(self):
        store = self.store()
        store.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        rejected = store.apply_websocket(
            {
                "event_type": "last_trade_price",
                "asset_id": "yes-1",
                "timestamp": "1785739199000",
                "price": "0.39",
            },
            received_at=NOW,
            stale_events_are_resolved=True,
        )[0]

        self.assertFalse(rejected.applied)
        self.assertEqual(rejected.fail_closed_reason, "out_of_order")
        self.assertTrue(rejected.gaps[0].resolved)
        self.assertEqual(
            rejected.gaps[0].resolution, "superseded_by_newer_snapshot"
        )
        self.assertEqual(sum(not item.resolved for item in store.gaps), 0)

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

    def test_negative_risk_frame_skew_uses_observation_not_last_exchange_change(self):
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("a", "b"), neg_risk=True),
            gamma_row("m2", "0x" + "2" * 64, ("c", "d"), neg_risk=True),
            gamma_row("m3", "0x" + "3" * 64, ("e", "f"), neg_risk=True),
        ]
        store = LiveStateStore(
            self.root / "negative-risk-observation-skew",
            now_provider=lambda: NOW,
            stale_after_ms=60_000,
        )
        store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        for token, bid, ask, timestamp in (
            ("a", "0.20", "0.22", "1785739200000"),
            ("b", "0.78", "0.80", "1785739200000"),
            ("c", "0.30", "0.32", "1785739210000"),
            ("d", "0.68", "0.70", "1785739210000"),
            ("e", "0.40", "0.42", "1785739220000"),
            ("f", "0.58", "0.60", "1785739220000"),
        ):
            store.apply_snapshot(
                snapshot(token, bid, ask, timestamp), received_at=NOW,
            )

        self.assertEqual(store.frame("m1", now=NOW).status, "ready")

        delayed = NOW + timedelta(seconds=6)
        for token, bid, ask in (
            ("c", "0.30", "0.32"), ("d", "0.68", "0.70"),
        ):
            store.apply_snapshot(
                snapshot(token, bid, ask, "1785739211000"),
                received_at=delayed,
            )
        frame = store.frame("m1", now=delayed)
        self.assertEqual(frame.status, "fail_closed")
        self.assertIn("negative_risk_frame_skew", frame.reason_codes)

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

    def test_clob_books_retries_transient_transport_failure(self):
        responses = [
            requests.Timeout("slow upstream"),
            Response([snapshot("a", "0.40", "0.42")]),
        ]

        def requester(*_args, **_kwargs):
            result = responses.pop(0)
            if isinstance(result, BaseException):
                raise result
            return result

        client = ClobBooksClient(
            requester=requester, max_retries_per_batch=1,
        )

        self.assertEqual(len(client.fetch(["a"])), 1)
        self.assertEqual(client.last_evidence["retry_count"], 1)

    def test_clob_books_complete_batch_retries_only_omitted_tokens(self):
        requests_seen = []
        responses = [
            Response([snapshot("a", "0.40", "0.42")]),
            Response([snapshot("b", "0.58", "0.60")]),
        ]

        def requester(*_args, **kwargs):
            requests_seen.append([item["token_id"] for item in kwargs["json"]])
            return responses.pop(0)

        client = ClobBooksClient(
            requester=requester, max_retries_per_batch=2,
        )
        consumed = []
        rows = client.fetch_stream(
            ["a", "b"],
            batch_consumer=consumed.extend,
            require_complete_batches=True,
        )

        self.assertEqual(rows, [])
        self.assertEqual([row["asset_id"] for row in consumed], ["a", "b"])
        self.assertEqual(requests_seen, [["a", "b"], ["b"]])
        self.assertEqual(client.last_evidence["received_book_count"], 2)
        self.assertEqual(client.last_evidence["retry_count"], 1)

    def test_clob_books_complete_batch_fails_after_omission_retries(self):
        client = ClobBooksClient(
            requester=lambda *_args, **_kwargs: Response([]),
            max_retries_per_batch=1,
        )

        with self.assertRaisesRegex(RuntimeError, "omitted requested tokens"):
            client.fetch_stream(["a"], require_complete_batches=True)

    def test_partial_books_bootstrap_does_not_publish_incomplete_batch(self):
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("yes-1", "no-1")),
            gamma_row("m2", "0x" + "2" * 64, ("yes-2", "no-2")),
        ]
        store = self.store(rows)
        books = ClobBooksClient(
            requester=lambda *_args, **_kwargs: Response([
                snapshot("yes-1", "0.40", "0.42"),
                snapshot("no-1", "0.58", "0.60"),
            ]),
            max_retries_per_batch=0,
        )
        collector = PolymarketLiveCollector(
            store,
            GammaKeysetCatalog(requester=lambda *_args, **_kwargs: Response({
                "markets": rows,
            })),
            books,
        )

        with self.assertRaisesRegex(RuntimeError, "omitted requested tokens"):
            asyncio.run(collector.bootstrap_books())

        health = store.health()
        self.assertEqual(health.status, "degraded")
        self.assertEqual(health.book_token_count, 0)
        self.assertEqual(health.missing_book_token_count, 4)
        self.assertEqual(health.ready_market_count, 0)
        self.assertIsNotNone(store.active_recovery_id)
        self.assertEqual(store.frame("m1", now=NOW).status, "fail_closed")
        self.assertEqual(store.frame("m2", now=NOW).status, "fail_closed")

    def test_bootstrap_retry_after_incomplete_batch_converges_recovery(self):
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("yes-1", "no-1")),
            gamma_row("m2", "0x" + "2" * 64, ("yes-2", "no-2")),
        ]
        store = self.store(rows)
        partial = ClobBooksClient(
            requester=lambda *_args, **_kwargs: Response([
                snapshot("yes-1", "0.40", "0.42"),
                snapshot("no-1", "0.58", "0.60"),
            ]),
            max_retries_per_batch=0,
        )
        collector = PolymarketLiveCollector(
            store,
            GammaKeysetCatalog(requester=lambda *_args, **_kwargs: Response({
                "markets": rows,
            })),
            partial,
        )

        with self.assertRaisesRegex(RuntimeError, "omitted requested tokens"):
            asyncio.run(collector.bootstrap_books())
        self.assertEqual(store.books, {})
        self.assertIsNotNone(store.active_recovery_id)

        collector.books_client = ClobBooksClient(
            requester=lambda *_args, **_kwargs: Response([
                snapshot("yes-1", "0.40", "0.42"),
                snapshot("no-1", "0.58", "0.60"),
                snapshot("yes-2", "0.30", "0.32"),
                snapshot("no-2", "0.68", "0.70"),
            ]),
            max_retries_per_batch=0,
        )
        recovery_id = asyncio.run(collector.bootstrap_books("startup_retry"))

        self.assertTrue(recovery_id)
        health = store.health()
        self.assertEqual(health.book_token_count, 4)
        self.assertEqual(health.missing_book_token_count, 0)
        self.assertEqual(health.ready_market_count, 2)
        self.assertIsNone(store.active_recovery_id)
        self.assertEqual(store.frame("m1", now=NOW).status, "ready")
        self.assertEqual(store.frame("m2", now=NOW).status, "ready")

    def test_empty_rest_last_trade_price_is_normalized_as_unavailable(self):
        store = self.store()
        without_trade = snapshot("yes-1", "0.40", "0.42")
        without_trade["last_trade_price"] = ""
        recovery_id = store.mark_recovery_started("official_book_without_trade")
        coverage = store.recover_from_books([
            without_trade,
            snapshot("no-1", "0.58", "0.60"),
        ], recovery_id)
        self.assertEqual(coverage["recovered_token_count"], 2)
        self.assertEqual(coverage["invalid_book_token_count"], 0)
        self.assertEqual(coverage["missing_token_count"], 0)
        self.assertTrue(coverage["coverage_complete"])
        self.assertIsNone(store.books["yes-1"].last_trade_price)
        self.assertIn("no-1", store.books)
        restarted = LiveStateStore(
            self.root / "live", now_provider=lambda: NOW,
        )
        restarted.recover()
        self.assertIsNone(restarted.books["yes-1"].last_trade_price)
        self.assertEqual(restarted.frame("m1", now=NOW).status, "ready")

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
            "/v1/prediction-markets/polymarket/live/candidates",
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
        self.assertIn("CandidateSnapshot", schemas)
        self.assertIn("CandidateNegativeRiskRelation", schemas)
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

    def test_candidate_snapshot_is_atomic_deterministic_and_fail_closed(self):
        root = self.root / "prediction-markets" / "polymarket-live"
        store = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [
            gamma_row("m1", "0x" + "1" * 64, ("a", "b"), neg_risk=True),
            gamma_row("m2", "0x" + "2" * 64, ("c", "d"), neg_risk=True),
            gamma_row("m3", "0x" + "3" * 64, ("e", "f"), neg_risk=True),
        ]
        rows[0]["liquidityNum"] = "100.2300"
        rows[1]["liquidityNum"] = None
        rows[2]["outcomes"] = '["Maybe","No"]'
        markets = GammaLiveNormalizer.normalize(rows, NOW)
        store.replace_catalog(markets, rows)
        manifest = json.loads(store.catalog_path.read_text(encoding="utf-8"))
        metadata = manifest["candidate_snapshot"]
        path = Path(metadata["path"])
        initial = path.read_bytes()
        candidate = CandidateSnapshot.model_validate_json(initial)

        self.assertEqual(candidate.catalog_revision, store.catalog_revision)
        self.assertEqual(candidate.payload.candidate_count, 3)
        by_id = {item.market_id: item for item in candidate.payload.candidates}
        self.assertEqual(by_id["m1"].liquidity_num, "100.2300")
        self.assertIsNone(by_id["m2"].liquidity_num)
        self.assertFalse(by_id["m3"].binary_yes_no)
        relation = candidate.payload.relations[0]
        self.assertFalse(relation.complete)
        self.assertEqual(relation.expected_member_count, 3)
        self.assertEqual(relation.actual_member_count, 2)
        self.assertIn("member_count_mismatch", relation.reason_codes)
        self.assertIn("source_member_not_binary_yes_no", relation.reason_codes)

        rebuilt = build_live_candidate_snapshot(root)
        self.assertEqual(rebuilt["sha256"], metadata["sha256"])
        self.assertEqual(path.read_bytes(), initial)

        settings = Settings(
            raw_path=self.root / "raw", storage_root=self.root,
            allowed_root=self.root.parent,
            postgres_dsn="postgresql://u:p@127.0.0.1/test",
            clickhouse_password="x", profile="test", port=8793,
            postgres_schema="test", clickhouse_database="test",
            clickhouse_spool_path=self.root / "spool",
        )
        client = TestClient(create_app(settings, Service()))
        first = client.get(
            "/v1/prediction-markets/polymarket/live/candidates"
        )
        second = client.get(
            "/v1/prediction-markets/polymarket/live/candidates"
        )
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.content, initial)
        self.assertEqual(second.content, initial)
        self.assertEqual(first.headers["etag"], f'"{metadata["sha256"]}"')
        self.assertEqual(
            first.headers["x-polymarket-payload-sha256"],
            candidate.payload_sha256,
        )
        path.write_bytes(initial + b" ")
        tampered = client.get(
            "/v1/prediction-markets/polymarket/live/candidates"
        )
        self.assertEqual(tampered.status_code, 409)
        self.assertEqual(
            tampered.json()["detail"]["code"],
            "polymarket_candidate_snapshot_integrity_failed",
        )

    def test_candidate_snapshot_supports_exact_50_and_100_relation_atomic_scopes(self):
        root = self.root / "candidate-selector"
        store = LiveStateStore(root, now_provider=lambda: NOW)
        rows = []
        for index in range(105):
            market_id = f"m{index:03d}"
            row = gamma_row(
                market_id,
                "0x" + f"{index + 1:064x}",
                (f"yes-{index}", f"no-{index}"),
                neg_risk=index < 5,
            )
            row["liquidityNum"] = f"{1000 - index}.0000"
            rows.append(row)
        store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        metadata = json.loads(store.catalog_path.read_text())[
            "candidate_snapshot"
        ]
        snapshot = CandidateSnapshot.model_validate_json(
            Path(metadata["path"]).read_bytes()
        )
        relation = snapshot.payload.relations[0]
        self.assertTrue(relation.complete)
        relation_ids = {item.market_id for item in relation.members}
        eligible = [
            item for item in snapshot.payload.candidates
            if item.active and item.accepting_orders and not item.closed
            and item.rules_complete and item.binary_yes_no
        ]
        eligible.sort(
            key=lambda item: Decimal(item.liquidity_num or "-1"), reverse=True,
        )
        for limit in (50, 100):
            selected = list(relation_ids)
            selected.extend(
                item.market_id for item in eligible
                if item.market_id not in relation_ids
            )
            selected = selected[:limit]
            self.assertEqual(len(selected), limit)
            self.assertTrue(relation_ids.issubset(selected))

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
        self.assertEqual(health["book_token_count"], 2)
        self.assertEqual(health["book_complete_market_count"], 1)
        self.assertEqual(health["unresolved_gap_count"], 0)
        self.assertEqual(health["status"], "index_ready")
        self.assertTrue(health["latest_state_ready"])
        self.assertFalse(app.state.polymarket_live._recovered)

    def test_live_health_uses_atomic_manifest_summary_without_state_scan(self):
        root = self.root / "live-health-summary"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        writer.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        writer.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
        reader = PolymarketLiveReadStore(root, now_provider=lambda: NOW)
        reader._manifest_binding()

        with patch.object(
            reader, "_state_binding", side_effect=AssertionError("state scan")
        ):
            health = reader.health()

        self.assertEqual(health.status, "index_ready")
        self.assertEqual(health.latest_cursor, writer.cursor)
        self.assertEqual(health.book_token_count, 2)
        self.assertEqual(health.book_complete_market_count, 1)
        self.assertEqual(health.unresolved_gap_count, 0)

    def test_bootstrap_binds_dynamic_clob_tick_to_instrument_facts(self):
        root = self.root / "live-dynamic-tick"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        for token, bid, ask in (
            ("yes-1", "0.400", "0.420"),
            ("no-1", "0.580", "0.600"),
        ):
            raw = snapshot(token, bid, ask)
            raw["tick_size"] = "0.001"
            writer.apply_snapshot(raw, received_at=NOW)

        reader = PolymarketLiveReadStore(root, now_provider=lambda: NOW)
        bootstrap = reader.bootstrap(["m1"])
        page = reader.snapshot(["m1"])

        self.assertEqual(bootstrap.cursor, writer.cursor)
        self.assertEqual(
            bootstrap.markets[0].rules.instrument.price_increment,
            "0.001",
        )
        self.assertEqual(
            {book.tick_size for book in page.items[0].tokens},
            {"0.001"},
        )
        self.assertEqual(
            bootstrap.markets[0].rules.instrument.price_increment,
            page.items[0].tokens[0].tick_size,
        )

    def test_raw_hash_deduplication_memory_is_bounded_by_replay_capacity(self):
        store = LiveStateStore(self.root / "bounded-hashes", replay_capacity=2)

        for raw_hash in ("one", "two", "three"):
            store._remember_raw_hash(raw_hash)

        self.assertEqual(store.seen_raw_hashes, {"two", "three"})
        self.assertEqual(list(store._seen_raw_hash_order), ["two", "three"])

    def test_api_startup_projects_polymarket_health_for_grafana(self):
        class ObservingMetadata:
            def __init__(self):
                self.observations = []

            def get_instrument(self, _instrument_id):
                return None

            def upsert_prediction_market_live_observation(self, row):
                self.observations.append(dict(row))
                return row

        settings = Settings(
            raw_path=self.root / "raw", storage_root=self.root,
            allowed_root=self.root.parent,
            postgres_dsn="postgresql://u:p@127.0.0.1/test",
            clickhouse_password="x", profile="test", port=8793,
            postgres_schema="test", clickhouse_database="test",
            clickhouse_spool_path=self.root / "spool",
        )
        service = Service()
        service.metadata_repository = ObservingMetadata()
        app = create_app(settings, service, now_provider=lambda: NOW)

        with TestClient(app):
            self.assertGreaterEqual(
                len(service.metadata_repository.observations), 1
            )
            observation = service.metadata_repository.observations[0]
            self.assertEqual(observation["status"], "not_configured")
            self.assertEqual(observation["market_count"], 0)
            self.assertEqual(observation["observed_at"], NOW.isoformat())

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

        tail_root = self.root / "tail-live"
        tail_writer = LiveStateStore(tail_root, now_provider=lambda: NOW)
        tail_writer.replace_catalog(
            GammaLiveNormalizer.normalize(rows, NOW), rows,
        )
        tail_writer.apply_snapshot(
            snapshot("yes-1", "0.40", "0.42"), received_at=NOW,
        )
        tail_writer.apply_snapshot(
            snapshot("no-1", "0.58", "0.60"), received_at=NOW,
        )
        tail_reader = PolymarketLiveReadStore(tail_root, now_provider=lambda: NOW)
        with tail_writer.event_path.open("ab") as stream:
            stream.write(b"partial-unindexed-event\n")
        self.assertEqual(tail_reader.snapshot(["m1"]).items[0].status, "ready")
        with self.assertRaises(ValueError):
            catch_up_live_state_index(tail_root)

    def test_live_state_writer_keeps_wal_open_across_hot_batches(self):
        root = self.root / "persistent-wal-live"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        writer.apply_snapshot(
            snapshot("yes-1", "0.40", "0.42"), received_at=NOW,
        )
        connection = writer.state_index._writer
        self.assertIsNotNone(connection)
        self.assertEqual(
            connection.execute("PRAGMA journal_mode").fetchone()[0], "wal",
        )

        writer.apply_snapshot(
            snapshot("no-1", "0.58", "0.60"), received_at=NOW,
        )

        self.assertIs(writer.state_index._writer, connection)
        reader = PolymarketLiveReadStore(root, now_provider=lambda: NOW)
        self.assertEqual(reader.snapshot(["m1"]).items[0].status, "ready")
        reader_connection = reader._state_reader.binding[2]
        self.assertEqual(reader.snapshot(["m1"]).items[0].status, "ready")
        self.assertIs(reader._state_reader.binding[2], reader_connection)
        self.assertEqual(
            reader_connection.execute("PRAGMA query_only").fetchone()[0], 1,
        )
        writer.state_index.close()
        self.assertIsNone(writer.state_index._writer)

    def test_scoped_snapshot_reads_one_atomic_live_state_boundary(self):
        root = self.root / "single-state-read-live"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        writer.apply_snapshot(
            snapshot("yes-1", "0.40", "0.42"), received_at=NOW,
        )
        writer.apply_snapshot(
            snapshot("no-1", "0.58", "0.60"), received_at=NOW,
        )
        reader = PolymarketLiveReadStore(root, now_provider=lambda: NOW)

        with patch.object(
            reader, "_state_snapshot", wraps=reader._state_snapshot,
        ) as state_snapshot:
            result = reader.snapshot(["m1"])

        self.assertEqual(result.items[0].status, "ready")
        self.assertEqual(state_snapshot.call_count, 1)

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

    def test_scoped_event_pages_are_bounded_without_skipping_cursors(self):
        root = self.root / "bounded-live-events"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        with (
            polymarket_live_module._publication_lock(root, exclusive=True),
            writer.state_index.batch(),
            writer.durable_event_batch(),
        ):
            for index in range(300):
                writer._emit(
                    "price_change", {"index": index}, {"index": index},
                    applied=True, market_id="m1", _publication_locked=True,
                )

        reader = PolymarketLiveReadStore(root, now_provider=lambda: NOW)
        first = reader.events_after(["m1"], 0, 1_000)
        second = reader.events_after(["m1"], first.next_cursor, 1_000)

        self.assertEqual(len(first.items), reader.max_event_page_items)
        self.assertTrue(first.has_more)
        self.assertEqual(len(second.items), 50)
        self.assertFalse(second.has_more)
        cursors = [event.cursor for event in [*first.items, *second.items]]
        self.assertEqual(cursors, list(range(2, 302)))

    def test_scoped_event_byte_budget_keeps_cursor_resume_lossless(self):
        root = self.root / "byte-bounded-live-events"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        with (
            polymarket_live_module._publication_lock(root, exclusive=True),
            writer.state_index.batch(),
            writer.durable_event_batch(),
        ):
            for index in range(3):
                writer._emit(
                    "price_change", {"index": index}, {"index": index},
                    applied=True, market_id="m1", _publication_locked=True,
                )

        reader = PolymarketLiveReadStore(root, now_provider=lambda: NOW)
        reader.max_event_page_bytes = 1
        cursors = []
        after_cursor = 0
        while True:
            page = reader.events_after(["m1"], after_cursor, 1_000)
            cursors.extend(event.cursor for event in page.items)
            after_cursor = page.next_cursor
            if not page.has_more:
                break

        self.assertEqual(cursors, [2, 3, 4])

    def test_scoped_writer_catches_up_event_tail_left_by_interrupted_publish(self):
        root = self.root / "live"
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        rows = [gamma_row()]
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        writer.apply_snapshot(snapshot("yes-1", "0.40", "0.42"), received_at=NOW)
        writer.apply_snapshot(snapshot("no-1", "0.58", "0.60"), received_at=NOW)
        with (
            patch.object(
                writer.state_index, "append",
                side_effect=RuntimeError("interrupted after durable append"),
            ),
            self.assertRaisesRegex(RuntimeError, "interrupted after durable append"),
        ):
            writer.mark_recovery_started("restart-boundary")

        recovered = catch_up_live_state_index(root)
        self.assertEqual(recovered["previous_cursor"], 3)
        self.assertEqual(recovered["latest_cursor"], 4)
        self.assertEqual(recovered["replayed_events"], 1)
        self.assertEqual(catch_up_live_state_index(root)["replayed_events"], 0)
        scoped = load_scoped_live_store(root, ["m1"], now_provider=lambda: NOW)
        self.assertEqual(scoped.cursor, 4)
        self.assertIsNotNone(scoped.active_recovery_id)

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

    def test_reader_uses_last_indexed_prefix_during_event_publication(self):
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
            self.assertFalse(read_worker.is_alive())
            self.assertEqual(read_errors, [])
            self.assertEqual(pages[0].cursor, writer.cursor - 1)
            release.set()
            worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(error, [])
        self.assertEqual(pages[0].items[0].status, "ready")
        self.assertEqual(reader.snapshot(["m1"]).cursor, writer.cursor)

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
            self.root / "live", now_provider=lambda: NOW,
            stable_read_wait_seconds=0,
        )
        self.assertEqual(indexed_reader.gaps(["m1"], unresolved_only=True).count, 1)
        with self.assertRaises(PolymarketLiveReadError) as unresolved:
            indexed_reader.snapshot(["m1"])
        self.assertEqual(
            unresolved.exception.code, "polymarket_state_index_lagging"
        )

        recovery_id = restarted.mark_recovery_started("integrity_recovery")
        self.assertEqual(indexed_reader.health().status, "degraded")
        with self.assertRaises(PolymarketLiveReadError) as recovering:
            indexed_reader.snapshot(["m1"])
        self.assertEqual(
            recovering.exception.code, "polymarket_state_index_lagging"
        )
        restarted.recover_from_books([
            snapshot("yes-1", "0.39", "0.41", "1785739203000"),
            snapshot("no-1", "0.59", "0.61", "1785739203000"),
        ], recovery_id)
        self.assertEqual(sum(not item.resolved for item in restarted.gaps), 0)
        self.assertEqual(restarted.frame("m1", now=NOW).status, "ready")
        self.assertEqual(indexed_reader.gaps(["m1"], unresolved_only=True).count, 0)
        self.assertEqual(indexed_reader.snapshot(["m1"]).items[0].status, "ready")

    def test_snapshot_waits_for_one_complete_relation_recovery_boundary(self):
        root = self.root / "relation-recovery-boundary"
        rows = [
            gamma_row(
                market_id,
                condition_id="0x" + f"{index:064x}",
                tokens=(f"yes-{index}", f"no-{index}"),
                neg_risk=True,
            )
            for index, market_id in enumerate(("m1", "m2", "m3"), start=1)
        ]
        writer = LiveStateStore(root, now_provider=lambda: NOW)
        writer.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
        books = [
            snapshot(token_id, bid, ask)
            for index in range(1, 4)
            for token_id, bid, ask in (
                (f"yes-{index}", "0.40", "0.42"),
                (f"no-{index}", "0.58", "0.60"),
            )
        ]
        for book in books:
            writer.apply_snapshot(book, received_at=NOW)
        writer.apply_websocket({
            "event_type": "price_change",
            "timestamp": "1785739201000",
            "price_changes": [{
                "asset_id": "yes-3", "side": "BUY",
                "price": "0.43", "size": "1",
            }],
        }, received_at=NOW)
        writer.state_index._publish_manifest()

        fail_fast = PolymarketLiveReadStore(
            root, now_provider=lambda: NOW, stable_read_wait_seconds=0,
        )
        with self.assertRaises(PolymarketLiveReadError) as incomplete:
            fail_fast.snapshot(["m1", "m2", "m3"])
        self.assertEqual(
            incomplete.exception.code, "polymarket_state_index_lagging"
        )
        health = fail_fast.health()
        self.assertEqual(health.status, "degraded")
        self.assertEqual(health.reason_codes, ["unresolved_gap"])

        reader = PolymarketLiveReadStore(
            root,
            now_provider=lambda: NOW,
            stable_read_wait_seconds=0.5,
            stable_read_poll_seconds=0.01,
        )
        sleep_entered = threading.Event()
        release_sleep = threading.Event()
        real_sleep = polymarket_live_module.time.sleep

        def blocked_poll(seconds):
            sleep_entered.set()
            self.assertTrue(release_sleep.wait(timeout=2))
            real_sleep(seconds)

        pages, errors = [], []

        def read_snapshot():
            try:
                pages.append(reader.snapshot(["m1", "m2", "m3"]))
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        with patch.object(
            polymarket_live_module.time, "sleep", side_effect=blocked_poll,
        ):
            read_worker = threading.Thread(target=read_snapshot)
            read_worker.start()
            self.assertTrue(sleep_entered.wait(timeout=2))
            recovery_id = writer.mark_recovery_started("relation_gap_recovery")
            writer.recover_from_books(books, recovery_id)
            release_sleep.set()
            read_worker.join(timeout=2)

        self.assertFalse(read_worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0].cursor, writer.cursor)
        self.assertEqual(pages[0].count, 3)
        self.assertTrue(all(item.status == "ready" for item in pages[0].items))
        self.assertEqual(fail_fast.health().status, "index_ready")

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
    def test_exact_scope_refresh_interval_rejects_event_amplifying_values(self):
        self.assertEqual(validate_snapshot_refresh_seconds(2), 2)
        self.assertEqual(validate_snapshot_refresh_seconds(1), 1)
        for unsafe in (0.1, 0.99, 2.01, 5):
            with self.subTest(unsafe=unsafe):
                with self.assertRaisesRegex(ValueError, "between 1 and 2"):
                    validate_snapshot_refresh_seconds(unsafe)

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

            async def refresh(reason="startup", **_partition):
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

    def test_periodic_snapshot_refresh_does_not_add_interval_after_slow_fetch(self):
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
            starts = []

            async def slow_refresh(reason="startup", **_partition):
                starts.append(time.monotonic())
                await asyncio.sleep(0.15)
                return "recovery"

            collector.refresh_books = slow_refresh

            async def scenario():
                task = asyncio.create_task(
                    collector._refresh_snapshots_periodically()
                )
                while len(starts) < 2:
                    await asyncio.sleep(0.01)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

            asyncio.run(scenario())

            # A completion-relative scheduler would add another 100ms and
            # start the second request after roughly 250ms.
            self.assertLess(starts[1] - starts[0], 0.21)

    def test_scoped_collector_never_overwrites_global_checkpoint(self):
        with TemporaryDirectory() as folder:
            rows = [gamma_row()]
            store = LiveStateStore(Path(folder), now_provider=lambda: NOW)
            store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
            collector = PolymarketLiveCollector(
                store,
                GammaKeysetCatalog(requester=lambda *_args, **_kwargs: None),
                ClobBooksClient(
                    requester=lambda *_args, **_kwargs: Response([
                        snapshot("yes-1", "0.40", "0.42"),
                        snapshot("no-1", "0.58", "0.60"),
                    ])
                ),
                publish_checkpoints=False,
            )

            asyncio.run(collector.bootstrap_books())

            self.assertFalse(store.checkpoint_path.exists())
            self.assertEqual(set(store.books), {"yes-1", "no-1"})

    def test_periodic_refresh_coalesces_recent_books_but_keeps_coverage(self):
        with TemporaryDirectory() as folder:
            rows = [gamma_row()]
            store = LiveStateStore(Path(folder), now_provider=lambda: NOW)
            store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
            store.apply_snapshot(
                snapshot("yes-1", "0.40", "0.42"), received_at=NOW
            )
            store.apply_snapshot(
                snapshot("no-1", "0.58", "0.60"), received_at=NOW
            )
            cursor = store.cursor
            requests = []

            def requester(_url, **kwargs):
                requests.append(kwargs["json"])
                return Response([
                    snapshot("yes-1", "0.40", "0.42"),
                    snapshot("no-1", "0.58", "0.60"),
                ])

            collector = PolymarketLiveCollector(
                store,
                GammaKeysetCatalog(requester=lambda *_args, **_kwargs: None),
                ClobBooksClient(requester=requester),
                publish_checkpoints=False,
                minimum_snapshot_refresh_age_seconds=2,
            )

            asyncio.run(collector.refresh_books())

            self.assertEqual(store.cursor, cursor)
            self.assertEqual(requests, [])

    def test_periodic_refresh_requests_only_stale_market_pairs(self):
        with TemporaryDirectory() as folder:
            current = [NOW]
            rows = [gamma_row()]
            store = LiveStateStore(
                Path(folder), now_provider=lambda: current[0],
            )
            store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
            store.apply_snapshot(
                snapshot("yes-1", "0.40", "0.42"), received_at=NOW
            )
            store.apply_snapshot(
                snapshot("no-1", "0.58", "0.60"), received_at=NOW
            )
            cursor_before_refresh = store.cursor
            requested = []

            def requester(_url, **kwargs):
                requested.extend(item["token_id"] for item in kwargs["json"])
                return Response([
                    snapshot("yes-1", "0.40", "0.42"),
                    snapshot("no-1", "0.58", "0.60"),
                ])

            collector = PolymarketLiveCollector(
                store,
                GammaKeysetCatalog(requester=lambda *_args, **_kwargs: None),
                ClobBooksClient(requester=requester),
                publish_checkpoints=False,
                minimum_snapshot_refresh_age_seconds=1,
            )
            current[0] = NOW + timedelta(seconds=2)
            event_inode = store.event_path.stat().st_ino
            event_fsyncs = []
            real_fsync = polymarket_live_module.os.fsync

            def track_fsync(file_descriptor):
                if polymarket_live_module.os.fstat(file_descriptor).st_ino == event_inode:
                    event_fsyncs.append(file_descriptor)
                return real_fsync(file_descriptor)

            with patch.object(
                polymarket_live_module.os,
                "fsync",
                side_effect=track_fsync,
            ):
                asyncio.run(collector.refresh_books())

            self.assertEqual(set(requested), {"yes-1", "no-1"})
            self.assertEqual(len(event_fsyncs), 0)
            self.assertEqual(store.cursor, cursor_before_refresh)
            refreshed = PolymarketLiveReadStore(
                Path(folder), now_provider=lambda: current[0],
            ).snapshot(["m1"])
            self.assertEqual(
                {book.received_at for book in refreshed.items[0].tokens},
                {current[0]},
            )

    def test_periodic_refresh_partitions_market_pairs_without_splitting(self):
        with TemporaryDirectory() as folder:
            current = [NOW]
            rows = [
                gamma_row(
                    market_id=f"m{index}",
                    tokens=(f"yes-{index}", f"no-{index}"),
                )
                for index in range(4)
            ]
            store = LiveStateStore(
                Path(folder), now_provider=lambda: current[0],
            )
            markets = GammaLiveNormalizer.normalize(rows, NOW)
            store.replace_catalog(markets, rows)
            requested = []

            def requester(_url, **kwargs):
                tokens = [item["token_id"] for item in kwargs["json"]]
                requested.append(tokens)
                return Response([
                    snapshot(token, "0.40", "0.42") for token in tokens
                ])

            collector = PolymarketLiveCollector(
                store,
                GammaKeysetCatalog(requester=lambda *_args, **_kwargs: None),
                ClobBooksClient(requester=requester),
                publish_checkpoints=False,
            )

            asyncio.run(collector.refresh_books(
                partition_index=1, partition_count=2,
            ))

            selected_markets = {
                store.token_to_market[token]
                for batch in requested for token in batch
            }
            self.assertEqual(
                selected_markets,
                set(sorted(store.catalog)[1::2]),
            )
            self.assertTrue(all(
                {
                    token
                    for token, market_id in store.token_to_market.items()
                    if market_id == selected
                } <= set(requested[0])
                for selected in selected_markets
            ))

    def test_periodic_refresh_keeps_negative_risk_group_in_one_transaction(self):
        with TemporaryDirectory() as folder:
            current = [NOW]
            rows = [
                gamma_row(
                    f"m{index}", f"0x{index + 1:064x}",
                    (f"yes-{index}", f"no-{index}"), neg_risk=True,
                )
                for index in range(3)
            ] + [gamma_row(
                "standalone", "0x" + "f" * 64,
                ("yes-standalone", "no-standalone"),
            )]
            store = LiveStateStore(
                Path(folder), now_provider=lambda: current[0],
            )
            store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
            for market in store.catalog.values():
                for outcome in market.identity.outcomes:
                    store.apply_snapshot(
                        snapshot(outcome.token_id, "0.40", "0.60"),
                        received_at=NOW,
                    )
            requested = []

            def requester(_url, **kwargs):
                token_ids = [item["token_id"] for item in kwargs["json"]]
                requested.append(token_ids)
                return Response([
                    snapshot(token_id, "0.40", "0.60")
                    for token_id in token_ids
                ])

            collector = PolymarketLiveCollector(
                store,
                GammaKeysetCatalog(requester=lambda *_args, **_kwargs: None),
                ClobBooksClient(requester=requester),
                publish_checkpoints=False,
                minimum_snapshot_refresh_age_seconds=1,
            )
            current[0] = NOW + timedelta(seconds=2)

            for partition_index in range(4):
                asyncio.run(collector.refresh_books(
                    partition_index=partition_index, partition_count=4,
                ))

            relation_tokens = {
                f"{side}-{index}"
                for index in range(3) for side in ("yes", "no")
            }
            relation_batches = [
                set(batch) for batch in requested
                if relation_tokens.intersection(batch)
            ]
            self.assertEqual(relation_batches, [relation_tokens])
            relation_received_at = {
                store.books[token_id].received_at for token_id in relation_tokens
            }
            self.assertEqual(relation_received_at, {current[0]})
            self.assertEqual(store.frame("m0", now=current[0]).status, "ready")

    def test_snapshot_refresh_partition_rejects_invalid_bounds(self):
        with TemporaryDirectory() as folder:
            collector = PolymarketLiveCollector(
                LiveStateStore(Path(folder), now_provider=lambda: NOW),
                GammaKeysetCatalog(requester=lambda *_args, **_kwargs: None),
                ClobBooksClient(requester=lambda *_args, **_kwargs: None),
            )

            with self.assertRaisesRegex(ValueError, "partition"):
                asyncio.run(collector.refresh_books(
                    partition_index=2, partition_count=2,
                ))

    def test_periodic_snapshot_refresh_recovers_after_transient_failure(self):
        with TemporaryDirectory() as folder:
            collector = PolymarketLiveCollector(
                LiveStateStore(Path(folder), now_provider=lambda: NOW),
                GammaKeysetCatalog(
                    requester=lambda *_args, **_kwargs: Response({"markets": []})
                ),
                ClobBooksClient(
                    requester=lambda *_args, **_kwargs: Response([])
                ),
                snapshot_refresh_seconds=0.01,
            )
            attempts = []

            async def refresh(reason="startup", **_partition):
                attempts.append(reason)
                if len(attempts) == 1:
                    raise RuntimeError("transient CLOB failure")
                return "recovery"

            collector.refresh_books = refresh

            async def scenario():
                task = asyncio.create_task(
                    collector._refresh_snapshots_periodically()
                )
                while len(attempts) < 2:
                    await asyncio.sleep(0.01)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

            with self.assertLogs(
                "marketcow.polymarket_live", level="ERROR"
            ) as captured:
                asyncio.run(scenario())

            self.assertEqual(
                attempts,
                ["periodic_snapshot_refresh", "periodic_snapshot_refresh"],
            )
            self.assertIn("periodic_snapshot_refresh_failed", captured.output[0])

    def test_websocket_reconnect_retries_transient_book_recovery_failure(self):
        with TemporaryDirectory() as folder:
            collector = PolymarketLiveCollector(
                LiveStateStore(Path(folder), now_provider=lambda: NOW),
                GammaKeysetCatalog(
                    requester=lambda *_args, **_kwargs: Response({"markets": []})
                ),
                ClobBooksClient(
                    requester=lambda *_args, **_kwargs: Response([])
                ),
                reconnect_seconds=0,
            )
            connection_attempts = []
            recovery_attempts = []

            async def run_once():
                connection_attempts.append(len(connection_attempts) + 1)
                if len(connection_attempts) == 1:
                    raise ConnectionError("simulated websocket disconnect")

            async def bootstrap(reason="startup"):
                recovery_attempts.append(reason)
                if len(recovery_attempts) == 1:
                    raise TimeoutError("simulated CLOB connect timeout")
                return "recovered"

            collector.run_once = run_once
            collector.bootstrap_books = bootstrap

            with self.assertLogs(
                "marketcow.polymarket_live", level="ERROR"
            ) as captured:
                asyncio.run(collector.run(max_connections=3))

            self.assertEqual(connection_attempts, [1, 2])
            self.assertEqual(
                recovery_attempts,
                ["websocket_reconnect:2", "websocket_reconnect:3"],
            )
            self.assertIn(
                "websocket_reconnect_recovery_failed", captured.output[0]
            )

    def test_websocket_publication_does_not_starve_periodic_refresh(self):
        with TemporaryDirectory() as folder:
            rows = [gamma_row()]
            selected_market = GammaLiveNormalizer.normalize(rows, NOW)[0]
            socket = FakeSocket([json.dumps({
                "event_type": "book",
                "asset_id": "yes-1",
                "market": selected_market.identity.condition_id,
                "timestamp": "1785739201000",
                "tick_size": "0.01",
                "bids": [{"price": "0.4", "size": "2"}],
                "asks": [{"price": "0.6", "size": "2"}],
            })])
            store = LiveStateStore(Path(folder), now_provider=lambda: NOW)
            store.replace_catalog([selected_market], rows)
            collector = PolymarketLiveCollector(
                store,
                GammaKeysetCatalog(requester=lambda *_args, **_kwargs: None),
                ClobBooksClient(requester=lambda *_args, **_kwargs: None),
                connector=lambda _url: SocketContext(socket),
                snapshot_refresh_seconds=0.01,
            )
            applying = threading.Event()
            release = threading.Event()
            original_apply = store.apply_websocket

            def slow_apply(item, **_kwargs):
                applying.set()
                release.wait(timeout=1)
                return original_apply(item)

            store.apply_websocket = slow_apply
            refreshed_while_applying = []

            async def refresh(reason="startup", **_partition):
                refreshed_while_applying.append(applying.is_set())
                release.set()
                return "recovery"

            collector.refresh_books = refresh

            async def scenario():
                periodic = asyncio.create_task(
                    collector._refresh_snapshots_periodically()
                )
                await collector._consume(["yes-1"], message_limit=1)
                periodic.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await periodic

            asyncio.run(scenario())

            self.assertEqual(refreshed_while_applying, [True])

    def test_websocket_publication_does_not_wait_for_rest_idle(self):
        with TemporaryDirectory() as folder:
            collector = PolymarketLiveCollector(
                LiveStateStore(Path(folder), now_provider=lambda: NOW),
                GammaKeysetCatalog(requester=lambda *_args, **_kwargs: None),
                ClobBooksClient(requester=lambda *_args, **_kwargs: None),
            )
            collector.store.apply_websocket = lambda _item, **_kwargs: None
            applied = threading.Event()
            thread = threading.Thread(
                target=lambda: (
                    collector._apply_websocket({"event_type": "unknown"}),
                    applied.set(),
                )
            )
            thread.start()
            self.assertTrue(applied.wait(timeout=0.05))
            thread.join(timeout=1)

    def test_websocket_batch_uses_one_durable_flush_and_contiguous_cursors(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            store = LiveStateStore(root, now_provider=lambda: NOW)
            rows = [gamma_row()]
            store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
            store.apply_snapshot(
                snapshot("yes-1", "0.40", "0.42"), received_at=NOW,
            )
            store.apply_snapshot(
                snapshot("no-1", "0.58", "0.60"), received_at=NOW,
            )
            collector = PolymarketLiveCollector(
                store,
                GammaKeysetCatalog(requester=lambda *_args, **_kwargs: None),
                ClobBooksClient(requester=lambda *_args, **_kwargs: None),
                publish_checkpoints=False,
            )
            cursor = store.cursor
            event_inode = store.event_path.stat().st_ino
            event_fsyncs = []
            real_fsync = polymarket_live_module.os.fsync

            def track_fsync(file_descriptor):
                if polymarket_live_module.os.fstat(file_descriptor).st_ino == event_inode:
                    event_fsyncs.append(file_descriptor)
                return real_fsync(file_descriptor)

            changes = [
                {
                    "event_type": "price_change",
                    "timestamp": "1785739201000",
                    "price_changes": [{
                        "asset_id": token_id, "side": "BUY",
                        "price": "0.40", "size": size,
                    }],
                }
                for token_id, size in (("yes-1", "12"), ("no-1", "13"))
            ]
            with patch.object(
                polymarket_live_module.os, "fsync", side_effect=track_fsync,
            ):
                collector._apply_websocket_batch(changes)

            self.assertEqual(len(event_fsyncs), 1)
            page = PolymarketLiveReadStore(
                root, now_provider=lambda: NOW,
            ).events_after(["m1"], cursor, 10)
            self.assertEqual(
                [event.cursor for event in page.items], [cursor + 1, cursor + 2],
            )

    def test_hot_batches_throttle_manifest_fsync_without_hiding_recovery(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            store = LiveStateStore(root, now_provider=lambda: NOW)
            rows = [gamma_row()]
            store.replace_catalog(GammaLiveNormalizer.normalize(rows, NOW), rows)
            collector = PolymarketLiveCollector(
                store,
                GammaKeysetCatalog(requester=lambda *_args, **_kwargs: None),
                ClobBooksClient(requester=lambda *_args, **_kwargs: None),
                publish_checkpoints=False,
            )
            writes = []
            real_atomic_write = polymarket_live_module._atomic_write

            def track_write(path, body):
                if path == store.state_index.manifest_path:
                    writes.append(bytes(body))
                return real_atomic_write(path, body)

            with patch.object(
                polymarket_live_module, "_atomic_write", side_effect=track_write,
            ):
                collector._apply_websocket_batch([{
                    "event_type": "book",
                    **snapshot("yes-1", "0.40", "0.42"),
                }])
                collector._apply_websocket_batch([{
                    "event_type": "book",
                    **snapshot("no-1", "0.58", "0.60"),
                }])
                with store.state_index.batch(), store.durable_event_batch():
                    store.mark_recovery_started("test")

            # The two adjacent market-data batches share the existing manifest
            # publication window; recovery state is always forced immediately.
            self.assertEqual(len(writes), 1)
            health = PolymarketLiveReadStore(root).health()
            self.assertEqual(health.status, "degraded")
            self.assertIn("recovery_in_progress", health.reason_codes)

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
