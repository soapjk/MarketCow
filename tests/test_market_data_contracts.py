from __future__ import annotations

import unittest

from pydantic import ValidationError

from marketcow.market_data_contracts import (
    BarPayload,
    EventEnvelope,
    InstrumentContract,
    canonical_hash,
    validate_instrument_identity,
)


def instrument(**updates):
    payload = {
        "schema_version": 1, "instrument_id": "AAPL.XNAS", "symbol": "AAPL",
        "market": "US", "mic": "XNAS", "currency": "USD",
        "price_precision": 2, "size_precision": 0,
        "tick_size": "0.01", "lot_size": "1",
        "provider_symbols": {"longport": "AAPL.US", "yahoo": "AAPL"},
        "broker_symbols": {"longport": "AAPL.US"},
    }
    payload.update(updates)
    return payload


class MarketDataContractTest(unittest.TestCase):
    def test_instrument_has_no_implicit_critical_defaults(self):
        for field in (
            "mic", "currency", "price_precision", "size_precision",
            "tick_size", "lot_size",
        ):
            payload = instrument()
            payload.pop(field)
            with self.subTest(field=field), self.assertRaises(ValidationError):
                InstrumentContract.model_validate(payload)

    def test_identity_and_mapping_are_provider_neutral(self):
        value = InstrumentContract.model_validate(instrument())
        validate_instrument_identity(value)
        self.assertNotIn("LONGPORT", value.instrument_id)
        with self.assertRaisesRegex(ValueError, "symbol.MIC"):
            validate_instrument_identity(InstrumentContract.model_validate(
                instrument(instrument_id="MSFT.XNAS")
            ))

    def test_financial_values_reject_json_numbers(self):
        with self.assertRaises(ValidationError):
            BarPayload.model_validate({
                "interval": "1-MINUTE", "adjustment": "raw",
                "window_start": "2026-07-23T01:00:00Z",
                "window_end": "2026-07-23T01:01:00Z",
                "open": 1.1, "high": "1.2", "low": "1.0",
                "close": "1.1", "volume": "10",
            })

    def test_event_timestamps_require_utc_aware_values(self):
        payload = {
            "stream_id": "s", "sequence": 1, "event_type": "quote",
            "instrument_id": "AAPL.XNAS", "source": "longport",
            "ts_event": "2026-07-23T01:00:00Z",
            "ts_ingest": "2026-07-23T01:00:00.001Z",
            "ts_publish": "2026-07-23T01:00:00.002Z",
            "quality": {
                "status": "live", "delayed": False,
                "stale": False, "degraded": False,
            },
            "payload": {},
        }
        event = EventEnvelope.model_validate(payload)
        self.assertTrue(event.ts_event.endswith("Z"))
        payload["ts_event"] = "2026-07-23T01:00:00"
        with self.assertRaises(ValidationError):
            EventEnvelope.model_validate(payload)

    def test_content_identity_is_order_independent(self):
        self.assertEqual(canonical_hash({"a": 1, "b": 2}),
                         canonical_hash({"b": 2, "a": 1}))


if __name__ == "__main__":
    unittest.main()
