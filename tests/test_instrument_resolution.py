from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from marketcow.service import FundamentalService


class Metadata:
    def __init__(self):
        self.rows = {}
        self.mappings = {}
        self.health = []

    def find_instrument_by_mapping(self, namespace, external_symbol):
        instrument_id = self.mappings.get((namespace, external_symbol))
        return None if instrument_id is None else self.rows[instrument_id]

    def get_instrument(self, instrument_id):
        return self.rows.get(instrument_id)

    def upsert_instrument(self, row):
        self.rows[row["instrument_id"]] = row
        for provider, symbol in row["provider_symbols"].items():
            self.mappings[(f"provider:{provider}", symbol)] = row["instrument_id"]
        return row

    def record_provider_health(self, provider, success, attempted_at, error=""):
        self.health.append((provider, success, error))


class ByteReturningMetadata(Metadata):
    def upsert_instrument(self, row):
        saved = super().upsert_instrument(row)
        return {
            **saved,
            "instrument_id": saved["instrument_id"].encode(),
            "currency": saved["currency"].encode(),
        }


class Provider:
    configured = True

    def __init__(self):
        self.calls = []

    def resolve_instruments(self, symbols):
        self.calls.append(list(symbols))
        result = []
        for symbol in symbols:
            if symbol == "MISSING.US":
                result.append({
                    "external_symbol": symbol,
                    "status": "error",
                    "error": {"code": "not_found", "message": "missing"},
                })
            else:
                result.append({
                    "external_symbol": symbol,
                    "status": "resolved",
                    "instrument_id": "MU.XNAS",
                    "symbol": "MU",
                    "mic": "XNAS",
                    "market": "US",
                    "currency": "USD",
                    "lot_size": 1,
                    "name": "Micron",
                    "source": "longport.static_info",
                    "source_exchange": "NASD",
                })
        return result


class InstrumentResolutionServiceTest(unittest.TestCase):
    def setUp(self):
        self.metadata = Metadata()
        self.provider = Provider()
        self.service = FundamentalService.__new__(FundamentalService)
        self.service.metadata_repository = self.metadata
        self.service.longport_quote_provider = self.provider

    def test_upstream_result_is_persisted_and_second_read_hits_registry(self):
        first = self.service.resolve_instruments_batch(
            "provider:longport", ["MU.US", "MISSING.US"]
        )

        self.assertEqual(
            [item["status"] for item in first["items"]], ["resolved", "error"]
        )
        self.assertEqual(first["items"][0]["instrument_id"], "MU.XNAS")
        self.assertEqual(first["items"][1]["error"]["code"], "not_found")
        self.assertEqual(self.metadata.mappings[
            ("provider:longport", "MU.US")
        ], "MU.XNAS")

        second = self.service.resolve_instruments_batch(
            "provider:longport", ["MU.US"]
        )

        self.assertEqual(second["items"][0]["resolution"], "registry")
        self.assertEqual(self.provider.calls, [["MU.US", "MISSING.US"]])

    def test_unconfigured_provider_is_structured_per_item(self):
        self.provider.configured = False

        result = self.service.resolve_instruments_batch(
            "provider:longport", ["MU.US", "PDD.US"]
        )

        self.assertEqual(result["resolved_count"], 0)
        self.assertEqual(
            [item["error"]["code"] for item in result["items"]],
            ["provider_unavailable", "provider_unavailable"],
        )

    def test_unsupported_namespace_does_not_guess(self):
        result = self.service.resolve_instruments_batch(
            "provider:unknown", ["MU"]
        )

        self.assertEqual(
            result["items"][0]["error"]["code"], "provider_unavailable"
        )
        self.assertEqual(self.provider.calls, [])

    def test_postgres_byte_values_are_decoded_on_write_and_registry_hit(self):
        self.metadata = ByteReturningMetadata()
        self.service.metadata_repository = self.metadata

        first = self.service.resolve_instruments_batch(
            "provider:longport", ["MU.US"]
        )
        self.assertEqual(first["items"][0]["instrument_id"], "MU.XNAS")

        row = self.metadata.rows["MU.XNAS"]
        row["instrument_id"] = b"MU.XNAS"
        row["currency"] = b"USD"
        row["tick_size"] = Decimal("0.01")
        row["size_increment"] = Decimal("1")
        row["lot_size"] = Decimal("1")
        row["ts_event"] = datetime(2026, 7, 27, 7, 30, tzinfo=timezone.utc)
        row["ts_init"] = datetime(2026, 7, 27, 7, 30, tzinfo=timezone.utc)
        row["updated_at"] = datetime(
            2026, 7, 27, 7, 30, tzinfo=timezone.utc
        )
        row["provider_symbols"] = {b"longport": b"MU.US"}
        second = self.service.resolve_instruments_batch(
            "provider:longport", ["MU.US"]
        )

        self.assertEqual(second["items"][0]["instrument_id"], "MU.XNAS")
        self.assertEqual(second["items"][0]["currency"], "USD")
        self.assertEqual(second["items"][0]["resolution"], "registry")

        existing = self.service.resolve_instruments_batch(
            "provider:longport", ["MICRON.US"]
        )
        self.assertEqual(existing["items"][0]["instrument_id"], "MU.XNAS")
        self.assertEqual(existing["items"][0]["resolution"], "upstream")


if __name__ == "__main__":
    unittest.main()
