from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

from marketcow.providers.alpaca_sip import (
    AlpacaBarsRequest,
    AlpacaBudgets,
    AlpacaCredentials,
    AlpacaSipError,
    AlpacaSipProvider,
)
from marketcow.price_adjustment import PriceAdjustmentContract


NOW = datetime(2026, 9, 22, 0, 0, tzinfo=timezone.utc)


def bar(symbol: str, day: int, close: str = "101") -> dict:
    return {
        "symbol": symbol,
        "t": f"2024-01-{day:02d}T05:00:00Z",
        "o": "100", "h": "102", "l": "99", "c": close,
        "v": "1000", "n": 50, "vw": "100.5",
    }


class FakeResponse:
    def __init__(self, status: int, payload: dict | bytes):
        self.status_code = status
        self.content = (
            payload if isinstance(payload, bytes)
            else json.dumps(payload, separators=(",", ":")).encode()
        )
        self.closed = False

    def iter_content(self, chunk_size=65536):
        for offset in range(0, len(self.content), chunk_size):
            yield self.content[offset:offset + chunk_size]

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.trust_env = None

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.responses:
            raise AssertionError("unexpected request")
        return self.responses.pop(0)


def request(symbols=("AAPL", "QQQ"), adjustment="raw"):
    return AlpacaBarsRequest(
        symbols=tuple(sorted(symbols)),
        start=datetime(2024, 1, 2, tzinfo=timezone.utc),
        end=datetime(2024, 1, 3, 23, 59, 59, tzinfo=timezone.utc),
        timeframe="1Day", feed="sip", adjustment=adjustment,
        asof=date(2025, 12, 31), sort="asc", limit=2,
    )


def provider(responses, **kwargs):
    return AlpacaSipProvider(
        AlpacaCredentials("key-visible-only-to-header", "secret-visible-only-to-header"),
        session=FakeSession(responses),
        clock=lambda: NOW,
        sleeper=lambda _seconds: None,
        random_value=lambda: 0,
        budgets=kwargs.pop("budgets", AlpacaBudgets(
            maximum_requests=20, maximum_bytes=1024 * 1024,
            maximum_retries=2, timeout_seconds=5, total_seconds=30,
            page_limit=2,
        )),
        **kwargs,
    )


class AlpacaSipContractTest(unittest.TestCase):
    def test_request_is_explicit_sip_with_asof_and_delay(self):
        item = request()
        params = item.params()
        self.assertEqual(params, {
            "symbols": "AAPL,QQQ", "start": "2024-01-02T00:00:00Z",
            "end": "2024-01-03T23:59:59Z", "timeframe": "1Day",
            "feed": "sip", "adjustment": "raw", "asof": "2025-12-31",
            "sort": "asc", "limit": "2",
        })
        item.validate_delay(NOW)
        recent = AlpacaBarsRequest(
            symbols=("AAPL",), start=datetime(2026, 9, 21, tzinfo=timezone.utc),
            end=datetime(2026, 9, 21, 23, 50, tzinfo=timezone.utc),
        )
        with self.assertRaisesRegex(ValueError, "15 minutes"):
            recent.validate_delay(datetime(2026, 9, 22, tzinfo=timezone.utc))
        with self.assertRaisesRegex(ValueError, "feed=sip"):
            AlpacaBarsRequest(
                symbols=("AAPL",), start=item.start, end=item.end, feed="iex"
            )

    def test_multi_symbol_pagination_preserves_identity_and_order(self):
        first = {"bars": {"AAPL": [bar("AAPL", 2), bar("AAPL", 3)]},
                 "next_page_token": "opaque-next"}
        second = {"bars": {"QQQ": [bar("QQQ", 2), bar("QQQ", 3)]},
                  "next_page_token": None}
        single = {"bars": {"AAPL": [bar("AAPL", 2), bar("AAPL", 3)]}}
        client = provider([FakeResponse(200, single)])
        result = client.fetch_history_window(
            "AAPL.XNAS", request(("AAPL",)).start,
            request(("AAPL",)).end, "1d", "raw",
        )
        # Single-symbol service path is intentionally distinct from the multi-symbol
        # capture exercised below.
        self.assertEqual(result["source"], "alpaca_historical_sip")
        PriceAdjustmentContract.model_validate({
            key: result["bars"][0].get(key) for key in (
                "factor_applicability", "corporate_action_factor",
                "applied_adjustment_multiplier", "adjustment_reference_date",
                "reference_factor", "factor_source", "factor_artifact_id",
                "factor_as_of",
            )
        } | {"adjustment": "raw"})

        client = provider([
            FakeResponse(200, first), FakeResponse(200, second),
            FakeResponse(200, {"corporate_actions": {}, "next_page_token": None}),
        ])
        with tempfile.TemporaryDirectory() as folder:
            manifest = client.capture_dataset(
                Path(folder) / "run", (request(),),
                corporate_start=date(2024, 1, 2), corporate_end=date(2024, 1, 3),
                provider_neutral_symbols={"AAPL": "AAPL.XNAS", "QQQ": "QQQ.ARCX"},
            )
        self.assertTrue(manifest["complete"])

    def test_feed_drift_duplicate_and_out_of_order_fail_closed(self):
        item = request(("AAPL",))
        drift = {"feed": "iex", "bars": {"AAPL": [bar("AAPL", 2)]}}
        with self.assertRaisesRegex(AlpacaSipError, "feed drifted"):
            provider([FakeResponse(200, drift)]).fetch_history_window(
                "AAPL.XNAS", item.start, item.end, "1d", "raw"
            )
        disorder = {"bars": {"AAPL": [bar("AAPL", 3), bar("AAPL", 2)]}}
        with self.assertRaisesRegex(AlpacaSipError, "out of order"):
            provider([FakeResponse(200, disorder)]).fetch_history_window(
                "AAPL.XNAS", item.start, item.end, "1d", "raw"
            )

    def test_adjustment_drift_and_repeated_page_token_fail_closed(self):
        item = request(("AAPL",))
        drift = {
            "feed": "sip", "adjustment": "split",
            "bars": {"AAPL": [bar("AAPL", 2)]},
        }
        with self.assertRaisesRegex(AlpacaSipError, "adjustment identity drifted"):
            provider([FakeResponse(200, drift)]).fetch_history_window(
                "AAPL.XNAS", item.start, item.end, "1d", "raw"
            )

        repeated = {
            "feed": "sip", "adjustment": "raw",
            "bars": {"AAPL": [bar("AAPL", 2)]},
            "next_page_token": "same-token",
        }
        repeated_again = {
            "feed": "sip", "adjustment": "raw",
            "bars": {"AAPL": [bar("AAPL", 3)]},
            "next_page_token": "same-token",
        }
        with self.assertRaisesRegex(AlpacaSipError, "repeated a pagination token"):
            provider([
                FakeResponse(200, repeated), FakeResponse(200, repeated_again),
            ]).fetch_history_window(
                "AAPL.XNAS", item.start, item.end, "1d", "raw"
            )

    def test_retry_is_bounded_and_errors_do_not_leak_credentials(self):
        item = request(("AAPL",))
        client = provider([
            FakeResponse(429, {"message": "rate limited"}),
            FakeResponse(500, {"message": "server"}),
            FakeResponse(403, {"message": "denied"}),
        ])
        with self.assertRaises(AlpacaSipError) as caught:
            client.fetch_history_window("AAPL.XNAS", item.start, item.end, "1d", "raw")
        text = str(caught.exception)
        self.assertNotIn("key-visible", text)
        self.assertNotIn("secret-visible", text)
        self.assertEqual(len(client.session.calls), 3)

    def test_response_byte_budget_stops_before_publish(self):
        item = request(("AAPL",))
        response = FakeResponse(200, {"bars": {"AAPL": [bar("AAPL", 2)]}})
        client = provider([response], budgets=AlpacaBudgets(
            maximum_requests=2, maximum_bytes=10, maximum_retries=0,
            timeout_seconds=1, total_seconds=2, page_limit=2,
        ))
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "run"
            with self.assertRaisesRegex(AlpacaSipError, "byte budget"):
                client.capture_dataset(
                    target, (item,), corporate_start=date(2024, 1, 2),
                    corporate_end=date(2024, 1, 3),
                    provider_neutral_symbols={"AAPL": "AAPL.XNAS"},
                )
            self.assertFalse((target / "manifest.json").exists())
            self.assertTrue((target / "failure.json").exists())
            self.assertTrue(response.closed)

    def test_invalid_json_is_preserved_as_raw_evidence_without_publication(self):
        item = request(("AAPL",))
        raw = b'{"bars":{"AAPL":['
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "run"
            with self.assertRaisesRegex(AlpacaSipError, "invalid JSON"):
                provider([FakeResponse(200, raw)]).capture_dataset(
                    target, (item,), corporate_start=date(2024, 1, 2),
                    corporate_end=date(2024, 1, 3),
                    provider_neutral_symbols={"AAPL": "AAPL.XNAS"},
                )
            evidence = list((target / "raw").glob("bars-raw-*.json"))
            self.assertEqual(len(evidence), 1)
            self.assertEqual(evidence[0].read_bytes(), raw)
            failure = json.loads((target / "failure.json").read_text())
            self.assertFalse(failure["complete"])
            self.assertEqual(failure["pages"][0]["sha256"], hashlib.sha256(raw).hexdigest())
            self.assertFalse((target / "manifest.json").exists())

    def test_capture_refuses_to_overwrite_an_existing_run(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "run"
            target.mkdir()
            marker = target / "keep"
            marker.write_text("unchanged")
            with self.assertRaises(FileExistsError):
                provider([]).capture_dataset(
                    target, (request(("AAPL",)),),
                    corporate_start=date(2024, 1, 2),
                    corporate_end=date(2024, 1, 3),
                    provider_neutral_symbols={"AAPL": "AAPL.XNAS"},
                )
            self.assertEqual(marker.read_text(), "unchanged")

    def test_complete_dataset_keeps_raw_and_adjusted_views_separate(self):
        responses = [
            FakeResponse(200, {"bars": {"AAPL": [bar("AAPL", 2), bar("AAPL", 3)]}}),
            FakeResponse(200, {"bars": {"AAPL": [bar("AAPL", 2), bar("AAPL", 3)]}}),
            FakeResponse(200, {
                "corporate_actions": {"forward_splits": [{
                    "symbol": "AAPL", "process_date": "2024-01-03", "id": "x"
                }]},
                "next_page_token": None,
            }),
        ]
        client = provider(responses)
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "run"
            manifest = client.capture_dataset(
                target, (request(("AAPL",), "raw"), request(("AAPL",), "split")),
                corporate_start=date(2024, 1, 2), corporate_end=date(2024, 1, 3),
                provider_neutral_symbols={"AAPL": "AAPL.XNAS"},
            )
            stored = json.loads((target / "manifest.json").read_text())
            lines = [json.loads(line) for line in (
                target / "normalized" / "bars.jsonl"
            ).read_text().splitlines()]
            self.assertEqual({row["adjustment"] for row in lines}, {"raw", "split"})
            self.assertEqual(stored["usage"]["requests"], 3)
            self.assertTrue(stored["coverage"]["formal_interval_assessable"])
            for page in stored["pages"]:
                raw = Path(page["path"]).read_bytes()
                self.assertEqual(hashlib.sha256(raw).hexdigest(), page["sha256"])
                self.assertNotIn("key-visible-only-to-header", raw.decode())
                self.assertNotIn("secret-visible-only-to-header", raw.decode())
            self.assertEqual(manifest["manifest"]["sha256"], hashlib.sha256(
                (target / "manifest.json").read_bytes()
            ).hexdigest())

    def test_missing_symbol_is_unassessable_not_filled(self):
        responses = [
            FakeResponse(200, {"bars": {"AAPL": [bar("AAPL", 2), bar("AAPL", 3)]}}),
            FakeResponse(200, {"corporate_actions": {}, "next_page_token": None}),
        ]
        with tempfile.TemporaryDirectory() as folder:
            manifest = provider(responses).capture_dataset(
                Path(folder) / "run", (request(),),
                corporate_start=date(2024, 1, 2), corporate_end=date(2024, 1, 3),
                provider_neutral_symbols={"AAPL": "AAPL.XNAS", "QQQ": "QQQ.ARCX"},
            )
        self.assertFalse(manifest["coverage"]["formal_interval_assessable"])
        self.assertEqual(manifest["coverage"]["symbols"]["QQQ"]["count"], 0)

    def test_credentials_require_0600_and_environment_is_not_persisted(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "alpaca.json"
            path.write_text(json.dumps({"key_id": "k", "secret_key": "s"}))
            os.chmod(path, 0o644)
            with self.assertRaisesRegex(AlpacaSipError, "0600"):
                AlpacaCredentials.load(path)
            os.chmod(path, 0o600)
            self.assertEqual(AlpacaCredentials.load(path).key_id, "k")
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(AlpacaSipError, "not configured"):
                AlpacaCredentials.load()


if __name__ == "__main__":
    unittest.main()
