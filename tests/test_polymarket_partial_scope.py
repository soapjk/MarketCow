"""Delivery is scope-complete; market quality and strategy decisions are local."""
import asyncio
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from marketcow.polymarket_live import GammaLiveNormalizer, PolymarketLiveReadStore
from tests.test_polymarket_live_projection_freshness import (
    PolymarketLiveProjectionFreshnessTest, gamma_row,
)


class PartialScopeTests(unittest.TestCase):
    def test_dependency_metadata_is_bound_without_changing_selected_identity(self):
        from marketcow.polymarket_live import content_sha256
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            now, _, projection = PolymarketLiveProjectionFreshnessTest().populated_projection(root, None)
            first = gamma_row()
            first.update(negRisk=True, negRiskMarketID="group", groupItemTitle="First")
            second = dict(first, id="m2", conditionId="0x" + "2" * 64,
                          clobTokenIds='["yes-2","no-2"]', groupItemTitle="Second")
            markets = GammaLiveNormalizer.normalize([first, second], now)
            projection._markets = {m.identity.market_id: m for m in markets}
            reader = PolymarketLiveReadStore(root, now_provider=lambda: now)
            result = projection.full_sync_json(reader, ["m1"])[2]
            bootstrap = result.bootstrap
            self.assertEqual([m.identity.market_id for m in bootstrap.markets], ["m1"])
            self.assertEqual([m.identity.market_id for m in bootstrap.dependency_markets], ["m2"])
            binding = {"catalog_revision": result.catalog_revision, "cursor": result.cursor,
                       "projection_generation": result.projection_generation, "scope_id": result.scope_id,
                       "markets": [m.model_dump(mode="json") for m in bootstrap.dependency_markets],
                       "missing_market_ids": []}
            self.assertEqual(bootstrap.dependency_metadata_sha256, content_sha256(binding))
            del projection._markets["m2"]
            result = projection.full_sync_json(reader, ["m1"])[2]
            self.assertEqual(result.bootstrap.missing_dependency_market_ids, ["m2"])
            self.assertEqual(len(result.snapshot.items), 1)

    def test_250_delivers_missing_recovering_and_old_books_without_global_gate(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            now, _, projection = PolymarketLiveProjectionFreshnessTest().populated_projection(root, None)
            templates = list(projection._books.values())
            projection._markets.clear()
            projection._books.clear()
            ids = [str(i) for i in range(250)]
            for i, mid in enumerate(ids):
                row = gamma_row()
                row.update(id=mid, conditionId="0x" + f"{i+1:064x}",
                           clobTokenIds=f'["{mid}-yes","{mid}-no"]')
                market = GammaLiveNormalizer.normalize([row], now)[0]
                projection._markets[mid] = market
                if i >= 6:
                    for template, outcome in zip(templates, market.identity.outcomes):
                        projection._books[outcome.token_id] = template.model_copy(update={
                            "token_id": outcome.token_id,
                            "condition_id": market.identity.condition_id,
                        })
            projection._token_recoveries["6-yes"] = ("pending", None)
            reader = PolymarketLiveReadStore(root, now_provider=lambda: now + timedelta(seconds=44),
                consumer_maximum_book_age_seconds=5, stable_snapshot_max_book_age_seconds=5)
            _, phases, result = projection.full_sync_json(reader, ids)
            self.assertEqual(len(result.snapshot.items), 250)
            self.assertEqual(result.scope_market_ids, ids)
            by_id = {frame.market_id: frame for frame in result.snapshot.items}
            self.assertEqual(by_id["0"].missing_token_ids, ["0-no", "0-yes"])
            self.assertEqual(by_id["6"].recovering_token_ids, ["6-yes"])
            self.assertEqual(by_id["7"].status, "ready")
            self.assertEqual(by_id["7"].maximum_book_age_ms, 44000)
            self.assertIsNone(by_id["7"].open_position_allowed)
            self.assertEqual(result.freshness_policy, "consumer_decides")
            self.assertEqual(phases["stable_wait_attempts"], 0)
            self.assertEqual(result.bootstrap.cursor, result.snapshot.cursor)

    def test_confirmations_are_pushed_without_advancing_event_cursor(self):
        async def run(root):
            now, _, projection = PolymarketLiveProjectionFreshnessTest().populated_projection(root, None)
            cursor = projection.latest_cursor
            stream = projection.stream_messages(["m1"], cursor)
            self.assertEqual((await anext(stream))["type"], "ready")
            old = projection._books["yes-1"]
            confirmed = old.model_copy(update={"received_at": now + timedelta(seconds=2),
                "confirmed_at": now + timedelta(seconds=2), "confirmation_source": "polymarket_rest",
                "confirmation_evidence_sha256": "a" * 64})
            projection.confirm_validated_book(confirmed)
            frame = await asyncio.wait_for(anext(stream), 1)
            self.assertEqual(frame["type"], "book_confirmations")
            self.assertEqual(frame["cursor"], cursor)
            self.assertEqual(frame["books"][0]["confirmation_source"], "polymarket_rest")
            self.assertEqual(projection.latest_cursor, cursor)
            self.assertEqual(projection._books["yes-1"].book_received_at, old.received_at)
            projection.confirm_validated_book(old)
            self.assertEqual(projection._books["yes-1"].received_at, confirmed.received_at)
            await stream.aclose()
            self.assertFalse(projection._subscribers)
        with TemporaryDirectory() as tmp:
            asyncio.run(run(Path(tmp)))

    def test_wrong_version_confirmation_is_local_and_reconnect_carries_baseline(self):
        async def run(root):
            now, _, projection = PolymarketLiveProjectionFreshnessTest().populated_projection(root, None)
            original = projection._books["yes-1"]
            update = original.model_copy(update={
                "received_at": now + timedelta(seconds=2),
                "confirmed_at": now + timedelta(seconds=2),
                "confirmation_source": "polymarket_rest",
                "confirmation_evidence_sha256": "b" * 64,
            })
            for field, value in (("book_epoch", "wrong"), ("sequence", 999),
                                 ("condition_id", "wrong"), ("bids", [])):
                projection.confirm_validated_book(update.model_copy(update={field: value}))
                self.assertEqual(projection._books["yes-1"], original)
                self.assertTrue(projection._ready)
            projection.confirm_validated_book(update)
            stream = projection.stream_messages(["m1"], projection.latest_cursor)
            ready = await anext(stream)
            self.assertEqual(ready["confirmation_sequence"], 1)
            self.assertEqual(ready["confirmation_books"][0]["token_id"], "yes-1")
            self.assertEqual(ready["stream_instance_id"], projection._stream_instance_id)
            await stream.aclose()
        with TemporaryDirectory() as tmp:
            asyncio.run(run(Path(tmp)))
