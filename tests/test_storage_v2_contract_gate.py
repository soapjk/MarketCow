import random
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from marketcow.api import create_app
from marketcow.clickhouse_shadow import ShadowMarketBarRepository
from marketcow.config import Settings
from marketcow.contract_gate import (
    CONTRACT_MATRIX,
    LEGACY_PAYLOAD_PATHS,
    MAX_MISMATCHES,
    ROUTING_DIAGNOSTIC_PATHS,
    assert_contract_equal,
    compare_contract,
)
from marketcow.storage import Warehouse


NOW = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


def bar(timestamp, close, amount=None):
    return {
        "timestamp": timestamp,
        "bar_at": datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
        "open": close - 1, "high": close + 1, "low": close - 2, "close": close,
        "raw_close": close, "adjustment_factor": 1.0, "volume": None,
        "amount": amount,
    }


class CanonicalProxy:
    """ClickHouse-shaped facade used to exercise routing independently of a server."""
    def __init__(self, warehouse, fail=False):
        self.warehouse = warehouse
        self.fail = fail

    def _call(self, name, *args):
        if self.fail:
            raise ConnectionError("fixture backend unavailable: token=REDACTED")
        return getattr(self.warehouse, name)(*args)

    def get_canonical_price_bars(self, *args):
        return self._call("get_price_bars", *args)

    def get_canonical_price_bars_range(self, *args):
        return self._call("get_price_bars_range", *args)

    def get_canonical_price_bars_page(self, *args):
        return self._call("get_price_bars_page", *args)

    def get_canonical_price_bars_cross_section(self, *args):
        return self._call("get_price_bars_cross_section", *args)

    def get_canonical_price_bars_cross_section_page(self, *args):
        return self._call("get_price_bars_cross_section_page", *args)

    def get_canonical_price_bars_matrix_page(self, *args):
        return self._call("get_price_bars_matrix_page", *args)

    def get_canonical_price_bar_as_of(self, *args):
        return self._call("get_price_bar_as_of", *args)

    def get_canonical_price_bars_as_of_page(self, *args):
        return self._call("get_price_bars_as_of_page", *args)

    def get_raw_price_bars_range(self, *args):
        return self._call("get_raw_price_bars_range", *args)

    def get_raw_price_bars_page(self, *args):
        return self._call("get_raw_price_bars_page", *args)


class Service:
    def __init__(self, repository):
        self.market_bar_repository = repository

    def close(self):
        pass


class StorageV2ContractGateTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        root = Path(self.folder.name)
        self.warehouse = Warehouse(root / "warehouse.duckdb")
        self.settings = Settings(
            root / "warehouse.duckdb", root / "raw", storage_root=root / "development",
            market_bar_cursor_secret="contract-gate-secret-1234567890-abcdef",
            market_bar_cache_freshness_seconds=300,
        )
        # Deliberately shuffled, duplicated, offset-formatted and conflicting input.
        rows = [bar(300, 30.25), bar(100, 10.125, 101.25), bar(200, 20.5)]
        self.warehouse.upsert_price_bars(
            "AAA", "1m", "raw", "yahoo_chart", "2026-07-20T19:58:00+08:00", rows,
            {"observed_at": "2026-07-20T11:57:00.123Z", "raw_artifact_id": "raw-y"},
        )
        self.warehouse.upsert_price_bars(
            "AAA", "1m", "raw", "tushare", "2026-07-20T11:56:00Z",
            [bar(200, 19.75), bar(100, 9.875)],
            {"observed_at": "2026-07-20T19:55:00+08:00", "raw_artifact_id": "raw-t"},
        )
        # Older late arrival must not replace the logical version.
        self.warehouse.upsert_price_bars(
            "AAA", "1m", "raw", "tushare", "2026-07-20T11:50:00Z", [bar(200, 1.0)]
        )
        self.warehouse.upsert_price_bars(
            "BBB", "1m", "raw", "fixture", "2026-07-20T11:58:30Z", [bar(200, 40.0)]
        )

    def tearDown(self):
        self.folder.cleanup()

    def adapter(self, fail=False):
        writer = SimpleNamespace(
            repository=CanonicalProxy(self.warehouse, fail),
            spool=SimpleNamespace(diagnostics=lambda: {}), close=lambda: None,
        )
        return ShadowMarketBarRepository(
            self.warehouse, writer, canonical_reads_enabled=True, raw_reads_enabled=True
        )

    def test_contract_matrix_is_complete_and_allowlist_is_explicit(self):
        self.assertEqual(set(CONTRACT_MATRIX), {
            "recent", "range", "canonical_page", "exact_cross_section_page",
            "matrix", "raw_range", "raw_page", "single_as_of",
            "cross_section_as_of",
        })
        self.assertEqual(ROUTING_DIAGNOSTIC_PATHS, {
            "$.backend", "$.attempted_backend", "$.fallback", "$.error",
            "$.diagnostics.backend", "$.diagnostics.attempted_backend",
            "$.diagnostics.fallback", "$.diagnostics.error",
        })

    def test_allowlist_is_path_limited_and_never_hides_bar_data(self):
        legal = compare_contract(
            {"backend": "duckdb", "diagnostics": {"error": "left"}, "bars": []},
            {"backend": "clickhouse", "diagnostics": {"error": "right"}, "bars": []},
        )
        self.assertEqual(legal["status"], "ok")
        for field in ("backend", "error", "fallback", "attempted_backend"):
            report = compare_contract(
                {"bars": [{field: "data-a"}]}, {"bars": [{field: "data-b"}]}
            )
            self.assertEqual(report["status"], "mismatch", field)
            self.assertEqual(report["mismatches"][0]["path"], f"$.bars[0].{field}")
        self.assertEqual(compare_contract(
            {"metadata": {"source_payload": {"private": "a"}}},
            {"metadata": {"source_payload": {"private": "b"}}},
            LEGACY_PAYLOAD_PATHS,
        )["status"], "mismatch")
        self.assertEqual(compare_contract(
            {"bars": [{"source_payload": {"private": "a"}}]},
            {"bars": [{"source_payload": {"private": "b"}}]},
            LEGACY_PAYLOAD_PATHS,
        )["status"], "ok")

    def test_all_repository_contracts_match_success_and_fallback(self):
        direct, success, fallback = self.warehouse, self.adapter(), self.adapter(True)
        cases = {
            "recent": ("get_price_bars", ("AAA", "1m", "raw", 2)),
            "range": ("get_price_bars_range", ("AAA", "1m", "raw", "1970-01-01T00:01:40Z", "1970-01-01T00:05:00Z", 2)),
            "canonical_page": ("get_price_bars_page", ("AAA", "1m", "raw", "1970-01-01T00:01:40Z", "1970-01-01T00:05:00Z", 2, None)),
            "exact_cross_section_page": ("get_price_bars_cross_section_page", ("1m", "raw", "1970-01-01T00:03:20Z", 2, ["BBB", "AAA"], None)),
            "matrix": ("get_price_bars_matrix_page", ("1m", "raw", ["1970-01-01T00:01:40Z", "1970-01-01T00:03:20Z"], ["AAA", "BBB"], 3, None)),
            "raw_range": ("get_raw_price_bars_range", ("AAA", "1m", "raw", "1970-01-01T00:01:40Z", "1970-01-01T00:05:00Z", 20, None)),
            "raw_page": ("get_raw_price_bars_page", ("AAA", "1m", "raw", "1970-01-01T00:01:40Z", "1970-01-01T00:05:00Z", 2, None, None)),
            "single_as_of": ("get_price_bar_as_of", ("AAA", "1m", "raw", "1970-01-01T00:04:10Z", 100)),
            "cross_section_as_of": ("get_price_bars_as_of_page", ("1m", "raw", "1970-01-01T00:04:10Z", 100, ["AAA", "BBB"], 2, None)),
        }
        for label, (method, arguments) in cases.items():
            expected = getattr(direct, method)(*arguments)
            assert_contract_equal(expected, getattr(success, method)(*arguments), label)
            assert_contract_equal(expected, getattr(fallback, method)(*arguments), label + " fallback")

    def test_api_contract_snapshots_match_all_read_routes(self):
        paths = [
            "/v1/quotes/AAA/history?interval=1m&adjustment=raw&refresh=false&limit=2",
            "/v1/quotes/AAA/history?interval=1m&adjustment=raw&start=1970-01-01T00:01:40Z&end=1970-01-01T00:05:00Z&limit=2",
            "/v1/quotes/AAA/history?interval=1m&adjustment=raw&start=1970-01-01T00:01:40Z&end=1970-01-01T00:05:00Z&page_size=2",
            "/v1/quotes/cross-section?interval=1m&adjustment=raw&bar_at=1970-01-01T00:03:20Z&symbols=AAA,BBB&page_size=2",
            "/v1/quotes/cross-section/matrix?interval=1m&adjustment=raw&bar_ats=1970-01-01T00:01:40Z,1970-01-01T00:03:20Z&symbols=AAA,BBB&page_size=3",
            "/v1/quotes/AAA/raw-history?interval=1m&adjustment=raw&start=1970-01-01T00:01:40Z&end=1970-01-01T00:05:00Z&page_size=2",
            "/v1/quotes/AAA/as-of?interval=1m&adjustment=raw&as_of=1970-01-01T00:04:10Z&max_lookback_seconds=100",
            "/v1/quotes/cross-section/as-of?interval=1m&adjustment=raw&as_of=1970-01-01T00:04:10Z&max_lookback_seconds=100&symbols=AAA,BBB&page_size=2",
        ]
        clients = [
            TestClient(create_app(self.settings, Service(repository), lambda: NOW))
            for repository in (self.warehouse, self.adapter(), self.adapter(True))
        ]
        try:
            for path in paths:
                responses = [client.get(path) for client in clients]
                self.assertTrue(all(response.status_code == 200 for response in responses), path)
                expected = responses[0].json()
                for response in responses[1:]:
                    assert_contract_equal(expected, response.json(), path)
            empty = clients[0].get(
                "/v1/quotes/MISSING/history?refresh=false&start=1970-01-01T00:00:00Z&end=1970-01-01T00:01:00Z"
            ).json()
            self.assertEqual((empty["bars"], empty["cache_status"]), ([], "empty"))
            invalid = [client.get("/v1/quotes/AAA/as-of?as_of=not-a-time") for client in clients]
            self.assertTrue(all(response.status_code == 400 for response in invalid))
            self.assertTrue(all(set(response.json()) == {"detail"} for response in invalid))
        finally:
            for client in clients:
                client.close()

    def test_seeded_property_normalization_and_bounded_report(self):
        randomizer = random.Random(16016)
        for _ in range(200):
            value = randomizer.uniform(-1e6, 1e6)
            left = {"value": Decimal(str(value)), "at": datetime(2026, 7, 20, tzinfo=timezone.utc)}
            right = {"value": float(value), "at": "2026-07-20T00:00:00+00:00"}
            assert_contract_equal(left, right, "generated normalization")
        report = compare_contract(list(range(100)), list(reversed(range(100))))
        self.assertEqual(report["mismatch_count"], MAX_MISMATCHES)
        self.assertTrue(report["truncated"])
        self.assertLess(len(str(report)), 20000)


if __name__ == "__main__":
    unittest.main()
