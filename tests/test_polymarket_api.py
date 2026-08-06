from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
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
from tests.test_market_data_api import Service
from tests.test_polymarket_history import bootstrap_market, metadata_tables, replay


NOW = datetime(2026, 8, 2, 9, 0, tzinfo=timezone.utc)


class PolymarketApiTest(unittest.TestCase):
    def setUp(self):
        self.folder = TemporaryDirectory()
        root = Path(self.folder.name)
        self.settings = Settings(
            raw_path=root / "raw", storage_root=root, allowed_root=root.parent,
            postgres_dsn="postgresql://u:p@127.0.0.1/marketcow_test",
            clickhouse_password="x", profile="test", port=8793,
            postgres_schema="marketcow_test", clickhouse_database="marketcow_test",
            clickhouse_spool_path=root / "spool",
        )
        self.client = TestClient(create_app(self.settings, Service()))

    def tearDown(self):
        self.folder.cleanup()

    def test_only_certified_manifest_and_immutable_parquet_are_public(self):
        identity = PredictionMarketIdentity(
            event_id="e", market_id="m", condition_id="c", slug="will-x",
            outcomes=[
                OutcomeToken(token_id="yes", outcome="Yes",
                             instrument_id="POLY:c:yes"),
                OutcomeToken(token_id="no", outcome="No",
                             instrument_id="POLY:c:no"),
            ],
        )
        record_root = self.settings.storage_root / "recording"
        recorder = PolymarketWebSocketRecorder(record_root, identity)
        for token, bid, ask in (("yes", "0.40", "0.42"), ("no", "0.58", "0.60")):
            recorder.append({
                "event_id": f"snapshot-{token}", "type": "book",
                "token_id": token, "sequence": 1,
                "exchange_ts": "2026-08-02T09:00:00Z",
                "received_ts": "2026-08-02T09:00:01Z", "tick_size": "0.01",
                "bids": [{"price": bid, "size": "1"}],
                "asks": [{"price": ask, "size": "1"}],
            })
        official = [{
            "record_type": "trade", "trade_id": "t", "token_id": "yes",
            "timestamp": "2026-08-02T09:00:02Z", "price": "0.41", "size": "1",
            "transaction_hash": "0x1", "source": "polymarket_data_api",
        }]
        onchain = [{
            **official[0], "record_type": "fill", "source": "polygon_logs",
        }]
        root = self.settings.storage_root / "prediction-markets" / "polymarket"
        materializer = PredictionMarketMaterializer(root, now_provider=lambda: NOW)
        tables = table_records_from_recorder(recorder)
        tables.update(metadata_tables(identity))
        tables.update({"trades": official, "onchain": onchain})
        source = SourceRevision(
            source="polymarket_websocket", revision="r1",
            source_url="wss://ws-subscriptions-clob.polymarket.com/ws/market",
            observed_at=NOW, ingested_at=NOW, payload_sha256="3" * 64,
            raw_path=str(recorder.raw_path),
        )
        draft = materializer.materialize(
            "tradude-sample", "r1", [identity], [source], tables, recorder.gaps,
            intended_use="official_onchain_reconciliation", replay=replay(),
            bootstrap_markets=[bootstrap_market(recorder.raw_path, identity)],
        )
        hidden = self.client.get(
            "/v1/prediction-markets/polymarket/datasets/tradude-sample/manifest"
        )
        hidden_bootstrap = self.client.get(
            "/v1/prediction-markets/polymarket/datasets/tradude-sample/bootstrap"
        )
        self.assertEqual(hidden.status_code, 404)
        self.assertEqual(hidden_bootstrap.status_code, 404)
        certified = PredictionMarketCertifier(materializer).certify(
            draft, book_states=recorder.states,
            official_trades=official, onchain_trades=onchain,
        )
        PublishedPredictionMarketStore(root).publish(certified)

        manifest = self.client.get(
            "/v1/prediction-markets/polymarket/datasets/tradude-sample/manifest"
        )
        part = self.client.get(
            "/v1/prediction-markets/polymarket/datasets/tradude-sample/parts/books"
        )
        openapi = self.client.get("/openapi.json").json()
        bootstrap = self.client.get(
            "/v1/prediction-markets/polymarket/datasets/tradude-sample/bootstrap"
        )

        self.assertEqual(manifest.status_code, 200)
        self.assertEqual(manifest.json()["status"], "certified")
        self.assertEqual(part.status_code, 200)
        self.assertEqual(bootstrap.status_code, 200)
        self.assertEqual(
            bootstrap.json()["manifest_id"], manifest.json()["manifest_id"]
        )
        self.assertEqual(part.headers["content-type"], "application/vnd.apache.parquet")
        self.assertIn(
            "/v1/prediction-markets/polymarket/datasets/{dataset_id}/manifest",
            openapi["paths"],
        )
        self.assertIn(
            "/v1/prediction-markets/polymarket/datasets/{dataset_id}/bootstrap",
            openapi["paths"],
        )

        manifest_path = root / "manifests" / f"{certified.manifest_id}.json"
        original_manifest = manifest_path.read_bytes()
        tampered_manifest = json.loads(original_manifest)
        tampered_manifest["status"] = "draft"
        manifest_path.write_text(json.dumps(tampered_manifest), encoding="utf-8")
        response = self.client.get(
            "/v1/prediction-markets/polymarket/datasets/tradude-sample/manifest"
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(
            response.json()["detail"]["code"], "certified_dataset_integrity_failed"
        )
        manifest_path.write_bytes(original_manifest)

        bootstrap_path = root / "bootstraps" / f"{certified.manifest_id}.json"
        original_bootstrap = bootstrap_path.read_bytes()
        tampered_bootstrap = json.loads(original_bootstrap)
        tampered_bootstrap["dataset_id"] = "tampered"
        bootstrap_path.write_text(json.dumps(tampered_bootstrap), encoding="utf-8")
        response = self.client.get(
            "/v1/prediction-markets/polymarket/datasets/tradude-sample/bootstrap"
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(
            response.json()["detail"]["code"], "certified_dataset_integrity_failed"
        )
        bootstrap_path.write_bytes(original_bootstrap)

        books_part = next(item for item in certified.parts if item.table == "books")
        books_path = Path(books_part.path)
        original_books = books_path.read_bytes()
        books_path.write_bytes(original_books + b"tampered")
        response = self.client.get(
            "/v1/prediction-markets/polymarket/datasets/tradude-sample/parts/books"
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(
            response.json()["detail"]["code"], "certified_dataset_integrity_failed"
        )
        books_path.write_bytes(original_books)


if __name__ == "__main__":
    unittest.main()
