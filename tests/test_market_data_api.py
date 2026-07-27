from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.config import Settings
from marketcow.market_data_contracts import InstrumentRecord
from marketcow.providers.yahoo_fx import FxRateError


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

    def provider_health(self):
        return [{
            "provider": "longport", "status": "ok",
            "last_attempt_at": "2026-07-25T00:00:00Z",
            "last_success_at": "2026-07-25T00:00:00Z",
            "last_error": "", "consecutive_failures": 0,
        }]


class Bars:
    def __init__(self):
        self.revision = "snapshot-a"
        self.revise_during_read = False
        self.symbols = []

    def get_canonical_dataset_identity(self, symbol, *_args):
        self.symbols.append(symbol)
        return {
            "snapshot_id": self.revision, "canonical_version": "17",
            "row_count": 2, "content_hash": "sha256:" + "a" * 64,
        }

    def get_price_bars_page(
        self, symbol, _interval, _adjustment, _start, _end, page_size, after
    ):
        self.symbols.append(symbol)
        rows = [
            {
                "timestamp": 100, "bar_at": "2026-07-23T01:00:00+00:00",
                "open": 1.1, "high": 1.2, "low": 1.0, "close": 1.15,
                "volume": 10.0, "selected_source": "longport",
                "quality_status": "ok", "version": 17,
                "ingested_at": "2026-07-23T02:01:01Z",
            },
            {
                "timestamp": 200, "bar_at": "2026-07-23T01:01:00+00:00",
                "open": 1.15, "high": 1.3, "low": 1.1, "close": 1.2,
                "volume": 11.0, "selected_source": "longport",
                "quality_status": "ok", "version": 17,
                "ingested_at": "2026-07-23T02:02:01Z",
            },
        ]
        selected = [row for row in rows if after is None or row["timestamp"] > after]
        if self.revise_during_read:
            self.revision = "snapshot-during-read"
        return selected[:page_size], len(selected) > page_size

    def get_symbol_coverage(self, symbol):
        return [{
            "layer": "canonical", "interval": "1d", "adjustment": "raw",
            "first_bar": "2026-01-01T00:00:00+00:00",
            "last_bar": "2026-07-25T00:00:00+00:00",
            "row_count": 100, "sources": ["longport"],
        }]


class Service:
    def __init__(self):
        self.metadata_repository = Metadata()
        self.market_bar_repository = Bars()
        self.online_resources = None
        self.search_results = {}
        self.search_calls = []
        self.fx_error = None

    def close(self):
        pass

    def search_instruments(self, query, limit):
        self.search_calls.append((query, limit))
        return self.search_results.get(query, [])[:limit]

    def resolve_instruments_batch(self, namespace, symbols):
        items = [{
            "namespace": namespace,
            "external_symbol": symbol,
            "status": "resolved",
            "instrument_id": "MU.XNAS",
            "symbol": "MU",
            "mic": "XNAS",
            "market": "US",
            "currency": "USD",
            "source": "longport.static_info",
            "source_exchange": "NASD",
            "observed_at": "2026-07-27T00:00:00Z",
            "resolution": "upstream",
        } for symbol in symbols]
        return {
            "namespace": namespace,
            "count": len(items),
            "resolved_count": len(items),
            "error_count": 0,
            "items": items,
        }

    def get_fx_rates(self, base, symbols, *, refresh=False):
        if self.fx_error is not None:
            raise self.fx_error
        return {
            "base": base,
            "rates": {base: 1.0, "CNY": 7.2, "HKD": 7.8},
            "source": "yahoo_chart",
            "source_urls": {
                "CNY": "https://example/CNY=X",
                "HKD": "https://example/HKD=X",
            },
            "as_of": "2026-07-27T12:00:00+00:00",
            "fetched_at": "2026-07-27T12:01:00+00:00",
            "ingested_at": "2026-07-27T12:01:00+00:00",
            "cached": not refresh,
            "stale": False,
            "cache_status": "hit" if not refresh else "refreshed",
            "cache_ttl_seconds": 900,
            "stale_max_seconds": 86400,
            "errors": [],
        }


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

    def test_batch_instrument_resolution_is_public_and_machine_readable(self):
        response = self.client.post("/v1/instruments:resolve/query", json={
            "namespace": "provider:longport",
            "symbols": ["MU.US"],
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["items"][0], {
            "namespace": "provider:longport",
            "external_symbol": "MU.US",
            "status": "resolved",
            "instrument_id": "MU.XNAS",
            "symbol": "MU",
            "mic": "XNAS",
            "market": "US",
            "currency": "USD",
            "source": "longport.static_info",
            "source_exchange": "NASD",
            "observed_at": "2026-07-27T00:00:00Z",
            "resolution": "upstream",
            "error": None,
        })
        openapi = self.client.get("/openapi.json").json()
        operation = openapi["paths"]["/v1/instruments:resolve/query"]["post"]
        self.assertIn("InstrumentResolveBatchRequest", str(operation))
        self.assertIn("InstrumentResolveBatchResponse", str(operation))

    def test_fx_contract_is_public_compatible_and_machine_readable(self):
        response = self.client.get(
            "/v1/fx",
            params={"base": "USD", "symbols": "CNY,HKD"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["base"], "USD")
        self.assertEqual(response.json()["rates"], {
            "USD": 1.0, "CNY": 7.2, "HKD": 7.8,
        })
        self.assertEqual(response.json()["source"], "yahoo_chart")
        self.assertEqual(
            response.json()["asOf"], "2026-07-27T12:00:00+00:00"
        )
        self.assertIn("fetchedAt", response.json())
        self.assertIn("ingestedAt", response.json())
        self.assertTrue(response.json()["cached"])
        self.assertFalse(response.json()["stale"])
        operation = self.client.get("/openapi.json").json()["paths"][
            "/v1/fx"
        ]["get"]
        self.assertIn("FxResponse", str(operation))
        self.assertIn("FxHttpErrorResponse", str(operation["responses"]["503"]))

    def test_fx_validation_and_provider_errors_are_structured(self):
        invalid = self.client.get(
            "/v1/fx", params={"base": "EUR", "symbols": "CNY"}
        )
        self.assertEqual(invalid.status_code, 422)
        self.assertEqual(invalid.json()["detail"]["code"], "invalid_currency")

        self.service.fx_error = FxRateError(
            "provider_unavailable", "offline", currency="CNY"
        )
        unavailable = self.client.get(
            "/v1/fx", params={"base": "USD", "symbols": "CNY"}
        )
        self.assertEqual(unavailable.status_code, 503)
        self.assertEqual(
            unavailable.json()["detail"],
            {
                "code": "provider_unavailable",
                "currency": "CNY",
                "message": "offline",
            },
        )

    def test_convertible_bond_registration_uses_fixed_income_asset_class(self):
        convertible_bond = {
            **self.instrument,
            "instrument_id": "118074.XSHG",
            "symbol": "118074",
            "instrument_type": "convertible_bond",
            "asset_class": "fixed_income",
            "market": "CN",
            "mic": "XSHG",
            "currency": "CNY",
            "price_precision": 3,
            "tick_size": "0.001",
            "lot_size": "10",
            "provider_symbols": {
                "tushare": "118074.SH",
                "eastmoney": "118074.SH",
            },
            "broker_symbols": {},
        }

        response = self.client.put(
            "/v1/admin/instruments/118074.XSHG",
            json=convertible_bond,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["instrument_type"], "convertible_bond")
        self.assertEqual(response.json()["asset_class"], "fixed_income")

    def test_administration_read_models_are_versioned_and_paginated(self):
        dashboards = self.client.get("/v1/admin/dashboards")
        self.assertEqual(dashboards.status_code, 200)
        self.assertEqual(dashboards.json()["schema"], "marketcow.dashboard-registry.v1")
        capabilities = self.client.get("/v1/admin/capabilities").json()
        self.assertEqual(capabilities["schema"], "marketcow.admin-capabilities.v1")
        self.assertTrue(all(capabilities["features"].values()))

        overview = self.client.get("/v1/admin/overview")
        self.assertEqual(overview.status_code, 200)
        self.assertEqual(overview.json()["schema"], "marketcow.admin-overview.v1")
        self.assertEqual(overview.json()["providers"]["healthy"], 1)

        providers = self.client.get(
            "/v1/admin/providers", params={"limit": 1, "offset": 0, "status": "ok"}
        )
        self.assertEqual(providers.status_code, 200)
        self.assertEqual(providers.json()["page"]["total"], 1)
        self.assertNotIn("credentials", providers.json()["items"][0])

        audit = self.client.get("/v1/admin/audit")
        self.assertEqual(audit.status_code, 200)
        self.assertEqual(audit.json()["schema"], "marketcow.admin-audit.v1")

        metrics = self.client.get("/metrics")
        self.assertEqual(metrics.status_code, 200)
        self.assertIn("marketcow_http_requests_total", metrics.text)
        self.assertIn('route="/v1/admin/overview"', metrics.text)

        coverage = self.client.get("/v1/admin/instruments/AAPL.XNAS/coverage")
        self.assertEqual(coverage.status_code, 200)
        self.assertEqual(coverage.json()["schema"], "marketcow.instrument-coverage.v1")
        self.assertEqual(coverage.json()["summary"]["rows"], 100)
        self.assertEqual(coverage.json()["summary"]["sources"], ["longport"])

    def test_crypto_canonical_storage_uses_venue_qualified_symbol(self):
        crypto = {
            **self.instrument,
            "instrument_id": "BTC-PERP.HYPL", "symbol": "BTC-PERP",
            "instrument_type": "crypto_perpetual", "asset_class": "crypto",
            "market": "CRYPTO", "mic": "HYPL", "currency": "USDC",
            "price_precision": 1, "size_precision": 5,
            "tick_size": "0.1", "size_increment": "0.00001",
            "lot_size": "0.00001",
            "provider_symbols": {"hyperliquid": "BTC"}, "broker_symbols": {},
        }
        self.assertEqual(
            self.client.put(
                "/v1/admin/instruments/BTC-PERP.HYPL", json=crypto
            ).status_code,
            200,
        )
        response = self.client.get(
            "/v1/canonical-bars/BTC-PERP.HYPL",
            params={
                "start": "2026-07-23T00:00:00Z",
                "end": "2026-07-24T00:00:00Z",
                "interval": "1-HOUR", "adjustment": "raw", "page_size": 1,
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.service.market_bar_repository.symbols)
        self.assertEqual(
            set(self.service.market_bar_repository.symbols),
            {"BTC-PERP.HYPL"},
        )

    def test_literal_instrument_search_precedes_dynamic_instrument_route(self):
        self.service.search_results["信维通信"] = [{
            "symbol": "300136.SZ",
            "name": "信维通信",
            "market": "CN",
            "exchange": "SZ",
            "currency": "CNY",
            "source": "fixture",
        }]

        found = self.client.get(
            "/v1/instruments/search", params={"q": "信维通信", "limit": 5}
        )
        missing = self.client.get(
            "/v1/instruments/search", params={"q": "不存在的标的", "limit": 5}
        )
        dynamic = self.client.get("/v1/instruments/AAPL.XNAS")

        self.assertEqual(found.status_code, 200)
        self.assertEqual(found.json()["items"][0]["symbol"], "300136.SZ")
        self.assertEqual(found.json()["items"][0]["name"], "信维通信")
        self.assertEqual(missing.status_code, 200)
        self.assertEqual(missing.json(), {"count": 0, "items": []})
        self.assertEqual(dynamic.status_code, 404)
        self.assertEqual(dynamic.json()["detail"]["instrument_id"], "AAPL.XNAS")

    def test_instrument_response_decodes_postgres_text_bytes(self):
        response = self.client.put(
            "/v1/admin/instruments/AAPL.XNAS", json=self.instrument
        )
        self.assertEqual(response.status_code, 200)
        row = self.service.metadata_repository.rows["AAPL.XNAS"]
        for field in (
            "instrument_id", "instrument_type", "asset_class", "symbol",
            "market", "mic", "currency", "content_hash",
        ):
            row[field] = row[field].encode()
        row["provider_symbols"] = {b"longport": b"AAPL.US"}
        row["broker_symbols"] = {b"longport": b"AAPL.US"}

        fetched = self.client.get("/v1/instruments/AAPL.XNAS")

        self.assertEqual(fetched.status_code, 200)
        record = InstrumentRecord.model_validate(fetched.json())
        self.assertEqual(record.market, "US")
        self.assertEqual(record.provider_symbols["longport"], "AAPL.US")

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
        self.service.metadata_repository.rows["AAPL.XNAS"]["symbol"] = b"AAPL"
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

    def test_history_empty_dataset_returns_contract_valid_manifest(self):
        class EmptyBars:
            def get_canonical_dataset_identity(self, *_args):
                return {
                    "snapshot_id": "empty-snapshot",
                    "canonical_version": "0",
                    "row_count": 0,
                    "content_hash": "sha256:" + "0" * 64,
                }

            def get_price_bars_page(self, *_args):
                return [], False

        self.client.put("/v1/admin/instruments/AAPL.XNAS", json=self.instrument)
        self.service.market_bar_repository = EmptyBars()
        response = self.client.get("/v1/canonical-bars/AAPL.XNAS", params={
            "start": "2026-07-01T00:00:00Z",
            "end": "2026-07-23T23:59:59Z",
            "interval": "1-DAY", "adjustment": "raw", "page_size": 1000,
        })

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["manifest"]["row_count"], 0)
        self.assertEqual(payload["manifest"]["snapshot_id"], "empty-snapshot")
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["bars"], [])
        self.assertIsNone(payload["next_cursor"])
        self.assertFalse(payload["truncated"])

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
