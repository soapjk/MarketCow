from __future__ import annotations

import unittest

from pydantic import ValidationError

from marketcow.market_data_contracts import (
    BarPayload,
    CanonicalBarPage,
    HistoricalBar,
    HistoricalManifest,
    InstrumentContract,
    OrderBookSnapshotPayload,
    QualityContract,
    STREAM_EVENT_ADAPTER,
    canonical_hash,
    validate_instrument_identity,
)


def instrument(**updates):
    payload = {
        "schema_version": 1, "instrument_id": "AAPL.XNAS", "symbol": "AAPL",
        "instrument_type": "equity", "asset_class": "equity",
        "market": "US", "mic": "XNAS", "currency": "USD",
        "price_precision": 2, "size_precision": 0,
        "tick_size": "0.01", "size_increment": "1", "lot_size": "1",
        "ts_event": "2026-07-23T00:00:00Z",
        "ts_init": "2026-07-23T00:00:01Z",
        "provider_symbols": {"longport": "AAPL.US", "yahoo": "AAPL"},
        "broker_symbols": {"longport": "AAPL.US"},
    }
    payload.update(updates)
    return payload


class MarketDataContractTest(unittest.TestCase):
    def test_instrument_has_no_implicit_critical_defaults(self):
        for field in (
            "mic", "currency", "price_precision", "size_precision",
            "instrument_type", "asset_class", "tick_size", "size_increment",
            "lot_size", "ts_event", "ts_init",
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
                "price_type": "LAST", "aggregation_source": "EXTERNAL",
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
            "payload": {
                "bid_price": "100", "ask_price": "101",
                "bid_size": "10", "ask_size": "11",
            },
        }
        event = STREAM_EVENT_ADAPTER.validate_python(payload)
        self.assertTrue(event.ts_event.endswith("Z"))
        payload["ts_event"] = "2026-07-23T01:00:00"
        with self.assertRaises(ValidationError):
            STREAM_EVENT_ADAPTER.validate_python(payload)

    def test_event_type_discriminates_and_instrument_rules_are_enforced(self):
        payload = {
            "schema_version": 1, "stream_id": "s", "sequence": 1,
            "event_type": "trade", "instrument_id": "AAPL.XNAS",
            "source": "longport", "ts_event": "2026-07-23T01:00:00Z",
            "ts_ingest": "2026-07-23T01:00:00.001Z",
            "ts_publish": "2026-07-23T01:00:00.002Z",
            "quality": {
                "status": "live", "delayed": False,
                "stale": False, "degraded": False,
            },
            "payload": {
                "price": "100", "size": "2", "trade_id": "trade-1",
                "aggressor_side": "NO_AGGRESSOR",
            },
        }
        self.assertEqual(
            STREAM_EVENT_ADAPTER.validate_python(payload).payload.trade_id, "trade-1"
        )
        with self.assertRaises(ValidationError):
            STREAM_EVENT_ADAPTER.validate_python({
                **payload, "payload": {
                    "bid_price": "100", "ask_price": "101",
                    "bid_size": "1", "ask_size": "1",
                },
            })
        with self.assertRaises(ValidationError):
            STREAM_EVENT_ADAPTER.validate_python({
                **payload, "event_type": "heartbeat", "instrument_id": "AAPL.XNAS",
                "payload": {
                    "last_sequence": 1, "server_time": "2026-07-23T01:00:01Z",
                },
            })

    def test_cross_field_market_data_invariants(self):
        base = {
            "interval": "1-MINUTE", "adjustment": "raw",
            "price_type": "LAST", "aggregation_source": "EXTERNAL",
            "window_start": "2026-07-23T01:00:00Z",
            "window_end": "2026-07-23T01:01:00Z",
            "open": "10", "high": "11", "low": "9",
            "close": "10.5", "volume": "10",
        }
        BarPayload.model_validate(base)
        for update in (
            {"high": "10"}, {"low": "10.1"}, {"volume": "-1"},
            {"window_end": "2026-07-23T01:00:00Z"},
            {
                "window_start": "2026-07-23T01:00:00.100000Z",
                "window_end": "2026-07-23T01:00:00Z",
            },
            {"interval": "2-MINUTE"},
        ):
            with self.subTest(update=update), self.assertRaises(ValidationError):
                BarPayload.model_validate({**base, **update})
        with self.assertRaises(ValidationError):
            QualityContract.model_validate({
                "status": "delayed", "delayed": True,
                "stale": True, "degraded": False,
            })

    def test_l1_snapshot_rejects_more_than_one_level_per_side(self):
        level = {"price": "10", "size": "1", "order_id": "0"}
        OrderBookSnapshotPayload.model_validate({
            "book_type": "L1_MBP", "depth": 1, "baseline_sequence": 1,
            "bids": [], "asks": [],
        })
        with self.assertRaises(ValidationError):
            OrderBookSnapshotPayload.model_validate({
                "book_type": "L1_MBP", "depth": 1, "baseline_sequence": 1,
                "bids": [level, level], "asks": [],
            })

    def test_canonical_page_rejects_manifest_mismatches(self):
        manifest = HistoricalManifest(
            dataset_id="d", snapshot_id="s", canonical_version="999",
            instruments=["AAPL.XNAS"], interval="1-MINUTE", adjustment="raw",
            start="2026-07-23T01:00:00Z", end="2026-07-23T02:00:00Z",
            row_count=2, content_hash="sha256:" + "a" * 64,
        )
        bar = HistoricalBar(
            instrument_id="AAPL.XNAS", interval="1-MINUTE", adjustment="raw",
            price_type="LAST", aggregation_source="EXTERNAL",
            window_start="2026-07-23T01:00:00Z",
            window_end="2026-07-23T01:01:00Z",
            ts_event="2026-07-23T01:01:00Z", ts_init="2026-07-23T01:01:01Z",
            open="10", high="11", low="9", close="10", volume="1",
            selected_source="longport", quality_status="ok", row_version="17",
        )
        CanonicalBarPage(
            manifest=manifest, count=1, bars=[bar], page_size=1,
            next_cursor=None, truncated=False, provenance={"layer": "canonical"},
        )
        bad_bar = bar.model_copy(update={
            "interval": "5-MINUTE", "adjustment": "adjusted",
            "window_start": "2026-07-23T03:00:00Z",
        })
        with self.assertRaises(ValidationError):
            CanonicalBarPage(
                manifest=manifest, count=1, bars=[bad_bar], page_size=1,
                next_cursor=None, truncated=False,
                provenance={"layer": "canonical"},
            )
        with self.assertRaises(ValidationError):
            CanonicalBarPage(
                manifest=manifest, count=2, bars=[bar, bar], page_size=1,
                next_cursor=None, truncated=False,
                provenance={"layer": "canonical"},
            )

    def test_content_identity_is_order_independent(self):
        self.assertEqual(canonical_hash({"a": 1, "b": 2}),
                         canonical_hash({"b": 2, "a": 1}))


if __name__ == "__main__":
    unittest.main()
