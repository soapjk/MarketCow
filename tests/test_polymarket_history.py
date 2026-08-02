from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq
from pydantic import ValidationError

from marketcow.polymarket_contracts import (
    FeeSchedule,
    MarketBootstrap,
    OutcomeToken,
    PredictionMarketIdentity,
    ReplayContract,
    RuleFact,
    SourceRevision,
    StructuralRelation,
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


def replay():
    return ReplayContract(
        mode="snapshot_only",
        ordering=[
            "exchange_at", "received_at", "book_epoch", "sequence", "record_id"
        ],
        sequence_semantics="source", delta_supported=False,
        cancellation_supported=False, queue_position_supported=False,
        depth="full source snapshot",
    )


def bootstrap_market(path="/tmp/source", market_identity=None):
    evidence = source(path)
    selected_identity = market_identity or identity()
    instrument_ids = [item.instrument_id for item in selected_identity.outcomes]
    return MarketBootstrap(
        identity=selected_identity, question="Will X?", title="Will X?",
        settlement_currency="USDC.e", activation_at=NOW,
        expiration_at=datetime(2026, 8, 3, tzinfo=timezone.utc),
        price_increment="0.01", size_increment="0.01",
        minimum_order_size="5", accepting_orders=False,
        lifecycle_state="resolved", resolution="Yes",
        external_ids={"condition_id": "condition-1"},
        relations=[StructuralRelation(
            relation_id="binary-1", relation_version="1",
            relation_type="binary_complements", members=instrument_ids,
            convertible=True, provenance=evidence, valid_from=NOW,
        )],
        rule_facts=[RuleFact(
            fact_id=f"rule-{index}", rule_version="1", fact_type=fact_type,
            value=value, provenance=evidence, valid_from=NOW,
        ) for index, (fact_type, value) in enumerate((
            ("binary_settlement", {"winner_payout": "1", "loser_payout": "0"}),
            ("settlement_currency", {"currency": "USDC.e"}),
            ("price_increment", {"increment": "0.01"}),
            ("size_increment", {"increment": "0.01"}),
            ("minimum_order_size", {"size": "5"}),
        ), start=1)],
        fee_schedule=FeeSchedule(
            schedule_id="fee-1", schedule_version="1", currency="USDC.e",
            maker_rate="0", taker_rate="0.07",
            formula="C * taker_rate * p * (1-p)", exponent="1",
            quantum="0.00001", rounding_mode="half_up",
            effective_from=NOW, provenance=[evidence],
        ),
    )


def metadata_tables(market_identity=None):
    selected = market_identity or identity()
    base = {
        "market_id": selected.market_id,
        "condition_id": selected.condition_id,
        "timestamp": NOW.isoformat(),
        "source": "polymarket_gamma",
    }
    return {
        "catalog": [{**base, "record_type": "catalog"}],
        "lifecycle": [{**base, "record_type": "resolved"}],
    }


class PolymarketHistoryTest(unittest.TestCase):
    def test_bootstrap_requires_explicit_fee_schedule(self):
        payload = bootstrap_market().model_dump(mode="json")
        payload.pop("fee_schedule")
        with self.assertRaises(ValidationError):
            MarketBootstrap.model_validate(payload)

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
            tables.update(metadata_tables())
            tables["trades"] = official
            tables["onchain"] = onchain
            materializer = PredictionMarketMaterializer(root / "published", now_provider=lambda: NOW)
            draft = materializer.materialize(
                "polymarket-binary-sample", "revision-1", [identity()],
                [source(recorder.raw_path)], tables, recorder.gaps,
                intended_use="official_onchain_reconciliation", replay=replay(),
                bootstrap_markets=[bootstrap_market(recorder.raw_path)],
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
                intended_use="official_onchain_reconciliation", replay=replay(),
                bootstrap_markets=[bootstrap_market(recorder.raw_path)],
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
                    **table_records_from_recorder(recorder), **metadata_tables(),
                    "trades": official,
                }, recorder.gaps,
                intended_use="official_onchain_reconciliation", replay=replay(),
                bootstrap_markets=[bootstrap_market(recorder.raw_path)],
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

    def test_certification_rejects_missing_typed_bootstrap(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            recorder = PolymarketWebSocketRecorder(root / "recording", identity())
            recorder.append(snapshot("yes", 1, "0.40", "0.42", "s1"))
            recorder.append(snapshot("no", 1, "0.58", "0.60", "s2"))
            trade = [{
                "token_id": "yes", "price": "0.42", "size": "1",
                "transaction_hash": "", "timestamp": NOW.isoformat(),
                "record_type": "trade",
            }]
            materializer = PredictionMarketMaterializer(root / "published")
            tables = {**table_records_from_recorder(recorder), **metadata_tables(),
                      "trades": trade}
            draft = materializer.materialize(
                "missing-bootstrap", "r1", [identity()], [source(recorder.raw_path)],
                tables, recorder.gaps, intended_use="nautilus_snapshot_replay",
                replay=replay(), bootstrap_markets=[bootstrap_market(recorder.raw_path)],
            )
            definition = (
                materializer.root / "bootstrap-definitions"
                / f"{draft.bootstrap_id}.json"
            )
            definition.unlink()

            rejected = PredictionMarketCertifier(materializer).certify(
                draft, book_states=recorder.states,
                official_trades=trade, onchain_trades=[],
            )

            self.assertEqual(rejected.status, "rejected")
            self.assertFalse(next(
                check for check in rejected.checks
                if check.name == "typed_bootstrap_complete"
            ).passed)


if __name__ == "__main__":
    unittest.main()
