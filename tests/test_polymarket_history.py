from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from marketcow.polymarket_contracts import (
    OutcomeToken,
    PredictionMarketIdentity,
    SourceRevision,
)
from marketcow.polymarket_history import (
    PolymarketWebSocketRecorder,
    PredictionMarketCertifier,
    PredictionMarketMaterializer,
    PublishedPredictionMarketStore,
    table_records_from_recorder,
)


NOW = datetime(2026, 8, 2, 8, 0, tzinfo=timezone.utc)


def identity():
    return PredictionMarketIdentity(
        event_id="event-1", market_id="market-1", condition_id="condition-1",
        slug="will-x", outcomes=[
            OutcomeToken(token_id="yes", outcome="Yes",
                         instrument_id="POLY:condition-1:yes"),
            OutcomeToken(token_id="no", outcome="No",
                         instrument_id="POLY:condition-1:no"),
        ],
    )


def snapshot(token, sequence, bid, ask, event_id):
    return {
        "event_id": event_id, "type": "book", "token_id": token,
        "sequence": sequence, "exchange_ts": "2026-08-02T08:00:00Z",
        "received_ts": "2026-08-02T08:00:01Z", "tick_size": "0.01",
        "bids": [{"price": bid, "size": "10.00"}],
        "asks": [{"price": ask, "size": "12.00"}], "hash": f"provider-{event_id}",
    }


def source(path):
    return SourceRevision(
        source="polymarket_websocket", revision="recorder-v1",
        source_url="wss://ws-subscriptions-clob.polymarket.com/ws/market",
        observed_at=NOW, ingested_at=NOW, payload_sha256="2" * 64,
        raw_path=str(path),
    )


class PolymarketHistoryTest(unittest.TestCase):
    def test_append_replay_checkpoint_duplicate_gap_and_snapshot_recovery(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            recorder = PolymarketWebSocketRecorder(
                root, identity(), checkpoint_every=2, now_provider=lambda: NOW
            )
            self.assertEqual(recorder.append(snapshot("yes", 1, "0.40", "0.42", "s1"))["status"], "applied")
            self.assertEqual(recorder.append(snapshot("no", 1, "0.58", "0.60", "s2"))["status"], "applied")
            duplicate = recorder.append(snapshot("yes", 1, "0.40", "0.42", "s1"))
            self.assertEqual(duplicate["status"], "duplicate")
            gap = recorder.append({
                "event_id": "d3", "type": "price_change", "token_id": "yes",
                "sequence": 3, "exchange_ts": "2026-08-02T08:00:02Z",
                "received_ts": "2026-08-02T08:00:03Z",
                "side": "buy", "price": "0.41", "size": "5",
            })
            self.assertEqual(gap["status"], "gap")
            recovered = recorder.append(snapshot("yes", 4, "0.41", "0.43", "s4"))
            self.assertEqual(recovered["status"], "applied")
            checkpoint = recorder.checkpoint()

            replayed = PolymarketWebSocketRecorder(
                root, identity(), checkpoint_every=2, now_provider=lambda: NOW
            )

            self.assertEqual(replayed.states, recorder.states)
            self.assertEqual(
                checkpoint["state_hashes"]["yes"], replayed.checkpoint()["state_hashes"]["yes"]
            )
            self.assertTrue(any(gap.code == "duplicate" for gap in replayed.gaps))
            self.assertTrue(any(gap.code == "sequence_gap" and gap.resolved
                                for gap in recorder.gaps))

    def test_decimal_tick_and_non_crossed_invariants_are_enforced(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            recorder = PolymarketWebSocketRecorder(root, identity())
            with self.assertRaisesRegex(ValueError, "crossed"):
                recorder.append(snapshot("yes", 1, "0.50", "0.50", "crossed"))
            bad_tick = snapshot("no", 1, "0.581", "0.60", "bad-tick")
            with self.assertRaisesRegex(ValueError, "tick aligned"):
                recorder.append(bad_tick)
            floating = snapshot("yes", 2, "0.40", "0.42", "float")
            floating["bids"][0]["price"] = 0.4
            with self.assertRaisesRegex(ValueError, "binary float"):
                recorder.append(floating)
            recovered = PolymarketWebSocketRecorder(root, identity())
            self.assertEqual(recovered.states, {})
            self.assertEqual(len(recovered.events()), 2)
            self.assertTrue(all(not item["accepted"] for item in recovered.events()))

    def test_immutable_parquet_manifest_reconciliation_certification_and_publish(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            recorder = PolymarketWebSocketRecorder(root / "recording", identity())
            recorder.append(snapshot("yes", 1, "0.40", "0.42", "yes-snapshot"))
            recorder.append(snapshot("no", 1, "0.58", "0.60", "no-snapshot"))
            recorder.append({
                "event_id": "trade-1", "type": "last_trade_price",
                "token_id": "yes", "exchange_ts": "2026-08-02T08:01:00Z",
                "received_ts": "2026-08-02T08:01:01Z",
                "price": "0.42", "size": "3.500",
            })
            official = [{
                "record_type": "trade", "trade_id": "official-1",
                "market_id": "market-1", "condition_id": "condition-1",
                "token_id": "yes", "timestamp": "2026-08-02T08:01:00Z",
                "price": "0.42", "size": "3.500",
                "transaction_hash": "0xabc", "source": "polymarket_data_api",
            }]
            onchain = [{
                "record_type": "fill", "market_id": "market-1",
                "condition_id": "condition-1", "token_id": "yes",
                "timestamp": "2026-08-02T08:01:01Z", "price": "0.42",
                "size": "3.500", "transaction_hash": "0xabc",
                "source": "polygon_logs",
            }]
            tables = table_records_from_recorder(recorder)
            tables["trades"] = official
            tables["onchain"] = onchain
            materializer = PredictionMarketMaterializer(root / "published", now_provider=lambda: NOW)
            draft = materializer.materialize(
                "polymarket-binary-sample", "revision-1", [identity()],
                [source(recorder.raw_path)], tables, recorder.gaps,
            )

            certified = PredictionMarketCertifier(materializer).certify(
                draft, book_states=recorder.states,
                official_trades=official, onchain_trades=onchain,
            )
            store = PublishedPredictionMarketStore(root / "published")
            store.publish(certified)

            self.assertEqual(certified.status, "certified")
            self.assertTrue(certified.attestation_sha256)
            self.assertTrue(all(check.passed for check in certified.checks))
            self.assertEqual(store.manifest(certified.dataset_id).manifest_id,
                             certified.manifest_id)
            book_path = store.part(certified.dataset_id, "books")
            self.assertEqual(pq.read_table(book_path).num_rows, 2)
            first_bytes = book_path.read_bytes()
            second = materializer.materialize(
                "another-dataset", "revision-1", [identity()],
                [source(recorder.raw_path)], tables, recorder.gaps,
            )
            self.assertEqual(next(p for p in second.parts if p.table == "books").path,
                             str(book_path))
            self.assertEqual(book_path.read_bytes(), first_bytes)

    def test_certification_rejects_missing_onchain_trade(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            recorder = PolymarketWebSocketRecorder(root / "recording", identity())
            recorder.append(snapshot("yes", 1, "0.40", "0.42", "s1"))
            recorder.append(snapshot("no", 1, "0.58", "0.60", "s2"))
            official = [{
                "token_id": "yes", "price": "0.42", "size": "1",
                "transaction_hash": "0xmissing",
                "timestamp": "2026-08-02T08:01:00Z", "record_type": "trade",
            }]
            materializer = PredictionMarketMaterializer(root / "published", now_provider=lambda: NOW)
            draft = materializer.materialize(
                "rejected-sample", "revision-1", [identity()],
                [source(recorder.raw_path)], {
                    **table_records_from_recorder(recorder), "trades": official,
                }, recorder.gaps,
            )

            rejected = PredictionMarketCertifier(materializer).certify(
                draft, book_states=recorder.states,
                official_trades=official, onchain_trades=[],
            )

            self.assertEqual(rejected.status, "rejected")
            self.assertFalse(next(
                item for item in rejected.checks
                if item.name == "official_onchain_trade_reconciliation"
            ).passed)


if __name__ == "__main__":
    unittest.main()
