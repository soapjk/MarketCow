from __future__ import annotations

import hashlib
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from marketcow.polymarket_contracts import SourceRevision, decimal_text
from marketcow.polymarket_sources import (
    ClobPriceHistoryAdapter,
    DataApiTradesAdapter,
    FetchedPayload,
    FixedRevisionDatasetAdapter,
    GammaCatalogNormalizer,
    LocalFirstRawCache,
    SourceRequest,
    TradeTruthNormalizer,
    assert_free_source_policy,
)


NOW = datetime(2026, 8, 2, 7, 0, tzinfo=timezone.utc)


class Adapter:
    def __init__(self, body=b'{"price": "0.42"}'):
        self.body = body
        self.calls = 0

    def fetch(self, _request):
        self.calls += 1
        return FetchedPayload(
            self.body, NOW, "2026-08-01T00:00:00Z", "2026-08-03T00:00:00Z"
        )


class Response:
    def __init__(self, content):
        self.content = content

    def raise_for_status(self):
        pass


class JsonResponse(Response):
    def __init__(self, payload):
        self.text = json.dumps(payload)
        self.content = self.text.encode()
        self.headers = {}


class PolymarketSourcesTest(unittest.TestCase):
    def test_local_first_cache_fetches_once_and_verifies_immutable_hash(self):
        with TemporaryDirectory() as folder:
            cache = LocalFirstRawCache(Path(folder), now_provider=lambda: NOW)
            adapter = Adapter()
            request = SourceRequest(
                dataset_key="gamma-catalog", source="polymarket_gamma",
                source_url="https://gamma-api.polymarket.com/markets",
                revision="response-etag-1",
                required_start="2026-08-01T00:00:00Z",
                required_end="2026-08-02T00:00:00Z",
            )

            first = cache.get(request, adapter)
            second = cache.get(request, adapter)

            self.assertFalse(first.local_hit)
            self.assertTrue(second.local_hit)
            self.assertEqual(adapter.calls, 1)
            self.assertEqual(first.body, second.body)
            Path(first.revision.raw_path).write_bytes(b"tampered")
            with self.assertRaisesRegex(RuntimeError, "hash mismatch"):
                cache.get(request, adapter)

    def test_coverage_shortfall_fails_instead_of_fabricating_history(self):
        with TemporaryDirectory() as folder:
            cache = LocalFirstRawCache(Path(folder), now_provider=lambda: NOW)
            request = SourceRequest(
                dataset_key="trade-window", source="polymarket_data_api",
                source_url="https://data-api.polymarket.com/trades",
                revision="request-v1", required_start="2026-07-01T00:00:00Z",
            )
            with self.assertRaisesRegex(RuntimeError, "required_start"):
                cache.get(request, Adapter())

    def test_fixed_dataset_requires_reviewed_source_revision_license_and_hash(self):
        revision = "a" * 40
        body = b"real public dataset bytes"
        request = SourceRequest(
            dataset_key="kinzikdza/polymarket-updown-microstructure",
            source="huggingface_fixed_revision",
            source_url=(
                "https://huggingface.co/datasets/kinzikdza/"
                f"polymarket-updown-microstructure/resolve/{revision}/part.parquet"
            ),
            revision=revision, license="apache-2.0",
            expected_sha256=hashlib.sha256(body).hexdigest(),
        )
        adapter = FixedRevisionDatasetAdapter(
            requester=lambda *_args, **_kwargs: Response(body)
        )

        fetched = adapter.fetch(request)

        self.assertEqual(fetched.body, body)
        with self.assertRaisesRegex(ValueError, "reviewed"):
            adapter.fetch(SourceRequest(
                **{**request.__dict__, "dataset_key": "unknown/paid-dataset"}
            ))
        with self.assertRaisesRegex(ValueError, "license"):
            adapter.fetch(SourceRequest(
                **{**request.__dict__, "license": "proprietary-trial"}
            ))

    def test_paid_and_trial_sources_are_explicitly_prohibited(self):
        for name in ("PMData", "Dome API", "PolymarketData"):
            with self.assertRaisesRegex(ValueError, "prohibited"):
                assert_free_source_policy(name)

    def test_gamma_catalog_preserves_binary_identity_rules_fees_and_revision(self):
        raw_path = "/tmp/gamma.json"
        source = SourceRevision(
            source="polymarket_gamma", revision="etag-1",
            source_url="https://gamma-api.polymarket.com/markets",
            observed_at=NOW, ingested_at=NOW,
            payload_sha256="1" * 64, raw_path=raw_path,
        )
        rows = [{
            "id": "market-1", "conditionId": "condition-1", "slug": "will-x",
            "clobTokenIds": json.dumps(["yes-token", "no-token"]),
            "outcomes": json.dumps(["Yes", "No"]),
            "minimumTickSize": "0.01", "minimumOrderSize": "5",
            "fees": {"makerFeeBps": "0", "takerFeeBps": "20"},
            "closed": True, "resolution": "Yes",
            "resolutionSource": "https://example/resolution",
        }]

        identities, revisions = GammaCatalogNormalizer.normalize(rows, source)

        self.assertEqual(len(identities[0].outcomes), 2)
        self.assertEqual(identities[0].outcomes[0].instrument_id,
                         "POLY:condition-1:yes-token")
        self.assertEqual(revisions[0].state, "resolved")
        self.assertEqual(revisions[0].taker_fee_bps, "20")
        with self.assertRaisesRegex(ValueError, "tick"):
            GammaCatalogNormalizer.normalize([
                {key: value for key, value in rows[0].items()
                 if key != "minimumTickSize"}
            ], source)

    def test_trade_and_onchain_truth_keep_decimal_strings_and_transaction_key(self):
        trade = TradeTruthNormalizer.data_api([{
            "id": "t1", "conditionId": "c", "asset": "yes-token",
            "side": "BUY", "price": "0.42", "size": "10.500",
            "timestamp": "2026-08-02T07:00:00Z", "transactionHash": "0xABC",
        }])[0]
        fill = TradeTruthNormalizer.onchain([{
            "type": "fill", "condition_id": "c", "token_id": "yes-token",
            "price": "0.42", "size": "10.500",
            "block_time": "2026-08-02T07:00:01Z", "transaction_hash": "0xABC",
        }])[0]

        self.assertEqual(trade["price"], fill["price"])
        self.assertEqual(trade["size"], "10.500")
        self.assertEqual(trade["transaction_hash"], "0xabc")
        with self.assertRaisesRegex(ValueError, "binary float"):
            decimal_text(0.42, "price")

    def test_official_history_adapters_report_coverage_and_pagination_ceiling(self):
        clob = ClobPriceHistoryAdapter(
            requester=lambda *_args, **_kwargs: JsonResponse({
                "yes": {"history": [
                    {"t": "2026-08-01T00:00:00Z", "p": "0.4"},
                    {"t": "2026-08-02T00:00:00Z", "p": "0.5"},
                ]}
            })
        )
        request = SourceRequest(
            dataset_key="clob-price-window", source="polymarket_clob",
            source_url="https://clob.polymarket.com/prices-history",
            revision="request-v1", parameters={"token_ids": ["yes", "no"]},
            required_start="2026-08-01T00:00:00+00:00",
            required_end="2026-08-02T00:00:00+00:00",
        )
        fetched = clob.fetch(request)
        self.assertEqual(fetched.coverage_start, "2026-08-01T00:00:00+00:00")
        self.assertEqual(fetched.coverage_end, "2026-08-02T00:00:00+00:00")

        trades = DataApiTradesAdapter(
            requester=lambda *_args, **_kwargs: JsonResponse([{
                "timestamp": "2026-08-01T00:00:00Z",
            }])
        )
        trade_request = SourceRequest(
            dataset_key="trades", source="polymarket_data_api",
            source_url="https://data-api.polymarket.com/trades",
            revision="request-v1", parameters={"limit": 1, "offset": 9999},
            required_start="2026-08-01T00:00:00+00:00",
            required_end="2026-08-01T00:00:00+00:00",
        )
        with self.assertRaisesRegex(RuntimeError, "narrow"):
            trades.fetch(trade_request)


if __name__ == "__main__":
    unittest.main()
