from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.market_data_contracts import InstrumentRecord


class Metadata:
    def __init__(self):
        self.rows = {}

    def upsert_instrument(self, row):
        self.rows[row["instrument_id"]] = row
        return row

    def get_instrument(self, instrument_id):
        return self.rows.get(instrument_id)

    def find_instrument_by_mapping(self, namespace, external_symbol):
        for row in self.rows.values():
            kind, name = namespace.split(":", 1)
            if row[f"{kind}_symbols"].get(name) == external_symbol:
                return row
        return None


class Bars:
    def __init__(self):
        self.revision = "snapshot-a"
        self.revise_during_read = False

    def get_canonical_dataset_identity(self, *_args):
        return {
            "snapshot_id": self.revision, "canonical_version": "17",
            "row_count": 2, "content_hash": "sha256:" + "a" * 64,
        }

    def get_price_bars_page(
        self, _symbol, _interval, _adjustment, _start, _end, page_size, after
    ):
        rows = [
            {
                "timestamp": 100, "bar_at": "2026-07-23T01:00:00+00:00",
                "open": 1.1, "high": 1.2, "low": 1.0, "close": 1.15,
                "volume": 10.0, "selected_source": "longport",
                "quality_status": "ok", "version": 17,
                "ingested_at": "2026-07-23T01:01:01Z",
            },
            {
                "timestamp": 200, "bar_at": "2026-07-23T01:01:00+00:00",
                "open": 1.15, "high": 1.3, "low": 1.1, "close": 1.2,
                "volume": 11.0, "selected_source": "longport",
                "quality_status": "ok", "version": 17,
                "ingested_at": "2026-07-23T01:02:01Z",
            },
        ]
        selected = [row for row in rows if after is None or row["timestamp"] > after]
        if self.revise_during_read:
            self.revision = "snapshot-during-read"
        return selected[:page_size], len(selected) > page_size


class Service:
    def __init__(self):
        self.metadata_repository = Metadata()
        self.market_bar_repository = Bars()
        self.online_resources = None

    def close(self):
        pass


class MarketDataApiTest(unittest.TestCase):
    def setUp(self):
        self.folder = TemporaryDirectory()
        root = Path(self.folder.name) / "test"
        self.settings = Settings(
            raw_path=root / "raw", storage_root=root, allowed_root=root.parent,
            postgres_dsn="postgresql://u:p@127.0.0.1/marketcow_test",
            clickhouse_password="x", profile="test", port=8793,
            postgres_schema="marketcow_test", clickhouse_database="marketcow_test",
            clickhouse_spool_path=root / "spool",
        )
        self.service = Service()
        self.client = TestClient(create_app(self.settings, self.service))
        self.instrument = {
            "schema_version": 1, "instrument_id": "AAPL.XNAS", "symbol": "AAPL",
            "instrument_type": "equity", "asset_class": "equity",
            "market": "US", "mic": "XNAS", "currency": "USD",
            "price_precision": 2, "size_precision": 0,
            "tick_size": "0.01", "size_increment": "1", "lot_size": "1",
            "ts_event": "2026-07-23T00:00:00Z",
            "ts_init": "2026-07-23T00:00:01Z",
            "provider_symbols": {"longport": "AAPL.US"},
            "broker_symbols": {"longport": "AAPL.US"},
        }

    def tearDown(self):
        self.folder.cleanup()

    def test_instrument_registration_query_and_resolution(self):
        saved = self.client.put("/v1/admin/instruments/AAPL.XNAS", json=self.instrument)
        self.assertEqual(saved.status_code, 200)
        record_schema = self.client.get("/v1/schemas/instrument_record").json()[
            "json_schema"
        ]
        self.assertIn("content_hash", record_schema["properties"])
        self.assertIn("updated_at", record_schema["properties"])
        InstrumentRecord.model_validate(saved.json())
        fetched = self.client.get("/v1/instruments/AAPL.XNAS").json()
        InstrumentRecord.model_validate(fetched)
        self.assertEqual(fetched["mic"], "XNAS")
        resolved = self.client.get(
            "/v1/instruments:resolve",
            params={"namespace": "provider:longport", "external_symbol": "AAPL.US"},
        )
        InstrumentRecord.model_validate(resolved.json())
        self.assertEqual(resolved.json()["instrument_id"], "AAPL.XNAS")

    def test_schema_is_machine_readable(self):
        response = self.client.get("/v1/schemas/instrument")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["schema_version"], 1)
        self.assertIn("required", response.json()["json_schema"])
        event = self.client.get("/v1/schemas/event").json()["json_schema"]
        self.assertIn("oneOf", event)
        for name in (
            "historical_manifest", "historical_bar", "canonical_bar_page",
            "subscribe", "unsubscribe", "subscription_ack",
            "stream_heartbeat", "stream_error", "sequence_watermark",
        ):
            self.assertEqual(self.client.get(f"/v1/schemas/{name}").status_code, 200)

    def test_history_manifest_pagination_is_snapshot_bound(self):
        self.client.put("/v1/admin/instruments/AAPL.XNAS", json=self.instrument)
        params = {
            "start": "2026-07-23T01:00:00Z",
            "end": "2026-07-23T01:02:00Z",
            "interval": "1-MINUTE", "adjustment": "raw", "page_size": 1,
        }
        first = self.client.get("/v1/canonical-bars/AAPL.XNAS", params=params)
        self.assertEqual(first.status_code, 200, first.text)
        payload = first.json()
        self.assertEqual(payload["bars"][0]["open"], "1.1")
        self.assertEqual(payload["manifest"]["snapshot_id"], "snapshot-a")
        second = self.client.get(
            "/v1/canonical-bars/AAPL.XNAS",
            params={**params, "cursor": payload["next_cursor"]},
        )
        self.assertEqual(second.status_code, 200, second.text)
        self.assertEqual(
            second.json()["bars"][0]["window_start"], "2026-07-23T01:01:00Z"
        )
        self.service.market_bar_repository.revision = "snapshot-b"
        changed = self.client.get(
            "/v1/canonical-bars/AAPL.XNAS",
            params={**params, "cursor": payload["next_cursor"]},
        )
        self.assertEqual(changed.status_code, 400)

    def test_history_rejects_revision_during_current_page_read(self):
        self.client.put("/v1/admin/instruments/AAPL.XNAS", json=self.instrument)
        self.service.market_bar_repository.revise_during_read = True
        response = self.client.get("/v1/canonical-bars/AAPL.XNAS", params={
            "start": "2026-07-23T01:00:00Z",
            "end": "2026-07-23T01:02:00Z",
            "interval": "1-MINUTE", "adjustment": "raw", "page_size": 1,
        })
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(response.json()["detail"]["code"], "canonical_snapshot_changed")


if __name__ == "__main__":
    unittest.main()
