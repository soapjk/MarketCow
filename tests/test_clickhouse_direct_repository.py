import ast
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import threading
import unittest

from marketcow.clickhouse_repositories import (
    ClickHouseDatabase,
    ClickHouseMarketBarRepository,
    ClickHouseRepositoryError,
    canonical_json_value,
)
from marketcow.repositories import MarketBarRepository


SOURCE = Path(__file__).resolve().parents[1] / "src" / "marketcow"


def _module_name(path: Path) -> str:
    relative = path.relative_to(SOURCE).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(("marketcow", *parts))


def _imports_for(path: Path) -> set[str]:
    owner = _module_name(path)
    package = owner.split(".")[:-1]
    imports: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package[: len(package) - node.level + 1]
                prefix = ".".join((*base, node.module or ""))
            else:
                prefix = node.module or ""
            imports.add(prefix.rstrip("."))
    return imports


def _forbidden_paths(
    graph: dict[str, set[str]], entrypoint: str, forbidden: set[str]
) -> set[tuple[str, ...]]:
    violations: set[tuple[str, ...]] = set()
    pending = [(entrypoint,)]
    visited: set[str] = set()
    while pending:
        path = pending.pop()
        owner = path[-1]
        if owner in visited:
            continue
        visited.add(owner)
        for imported in graph.get(owner, set()):
            candidate = (*path, imported)
            if imported in forbidden or any(
                imported.startswith(f"{item}.") for item in forbidden
            ):
                violations.add(candidate)
            elif imported in graph and imported not in path:
                pending.append(candidate)
    return violations


class _FailingClient:
    def query(self, *_args, **_kwargs):
        raise RuntimeError("password=secret " + "x" * 10000)


class _InsertClient:
    def __init__(self):
        self.rows = []
        self.insert_calls = []

    def query(self, _statement, **_kwargs):
        if not self.rows:
            return type("Result", (), {"result_rows": [["", 0]]})()
        payload_index = ClickHouseMarketBarRepository.QUOTE_COLUMNS.index("payload_json")
        version_index = ClickHouseMarketBarRepository.QUOTE_COLUMNS.index("content_version")
        latest = max(self.rows, key=lambda row: row[version_index])
        return type("Result", (), {
            "result_rows": [[latest[payload_index], latest[version_index]]]
        })()

    def insert(self, _table, rows, **_kwargs):
        self.insert_calls.append((_table, rows, _kwargs))
        self.rows.extend(rows)


class _InsertDatabase:
    def __init__(self):
        self.operation_lock = threading.RLock()
        self.client = _InsertClient()

    def _require_client(self):
        return self.client


class ClickHouseDirectRepositoryPolicyTest(unittest.TestCase):
    def test_range_time_accepts_database_datetime_values(self):
        point = datetime(2026, 7, 25, 7, 21, 24, tzinfo=timezone.utc)

        self.assertEqual(
            ClickHouseMarketBarRepository._range_time(point, "start"),
            point,
        )

    def test_history_ingestion_identity_is_clickhouse_deduplication_token(self):
        database = _InsertDatabase()
        repository = ClickHouseMarketBarRepository(database)
        provenance = {
            "market": "US", "observed_at": "2026-01-01T00:00:00+00:00",
            "raw_artifact_id": "artifact",
            "ingestion_id": "stable-history-ingestion",
        }
        bars = [{
            "bar_at": "2026-01-01T00:00:00+00:00",
            "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1,
        }]

        repository.upsert_price_bars(
            "AAPL", "1d", "raw", "yahoo",
            "2026-01-02T00:00:00+00:00", bars, provenance,
        )

        settings = database.client.insert_calls[0][2]["settings"]
        self.assertEqual(
            settings["insert_deduplication_token"],
            "stable-history-ingestion",
        )
        ingestion_index = repository.RAW_COLUMNS.index("ingestion_id")
        self.assertEqual(
            database.client.insert_calls[0][1][0][ingestion_index],
            "stable-history-ingestion",
        )

    def test_adjustment_factor_preserves_precision_and_ingestion_identity(self):
        database = _InsertDatabase()
        repository = ClickHouseMarketBarRepository(database)

        count = repository.upsert_adjustment_factors(
            "600519.XSHG", "tushare",
            "2026-07-25T01:02:03+00:00",
            [{
                "trade_date": "2026-07-24",
                "adjustment_factor": "12.345678901234567890",
            }],
            {
                "raw_artifact_id": "factor-artifact",
                "ingestion_id": "history-shard-a",
            },
        )

        self.assertEqual(count, 1)
        table, rows, kwargs = database.client.insert_calls[0]
        self.assertEqual(table, "market_adjustment_factor")
        factor_index = repository.ADJUSTMENT_FACTOR_COLUMNS.index(
            "adjustment_factor"
        )
        ingestion_index = repository.ADJUSTMENT_FACTOR_COLUMNS.index(
            "ingestion_id"
        )
        self.assertEqual(
            rows[0][factor_index], Decimal("12.345678901234567890")
        )
        self.assertEqual(rows[0][ingestion_index], "history-shard-a")
        self.assertEqual(
            kwargs["settings"]["insert_deduplication_token"],
            "history-shard-a",
        )

    def test_raw_ingestion_receipt_is_read_by_stable_identity(self):
        class Result:
            result_rows = [[2, 1000, 2000, "artifact-a"]]

        repository = object.__new__(ClickHouseMarketBarRepository)
        calls = []
        repository._query = lambda statement, parameters: (
            calls.append((statement, parameters)) or Result()
        )

        receipt = repository.get_raw_ingestion_receipt("ingestion-a")

        self.assertEqual(receipt["row_count"], 2)
        self.assertEqual(receipt["first_bar_at_ms"], 1000)
        self.assertEqual(receipt["raw_artifact_id"], "artifact-a")
        self.assertEqual(calls[0][1], {"ingestion_id": "ingestion-a"})

        Result.result_rows = [[0, None, None, None]]
        self.assertIsNone(repository.get_raw_ingestion_receipt("missing"))

    def test_lists_raw_ingestion_receipts_for_consistency_audit(self):
        class Result:
            result_rows = [["a", 2, 1000, 2000, "artifact-a"]]

        repository = object.__new__(ClickHouseMarketBarRepository)
        repository._query = lambda *_args, **_kwargs: Result()

        rows = repository.list_raw_ingestion_receipts(10)

        self.assertEqual(rows, [{
            "ingestion_id": "a", "row_count": 2,
            "first_bar_at_ms": 1000, "last_bar_at_ms": 2000,
            "raw_artifact_id": "artifact-a",
        }])

    def test_canonical_ingestion_coverage_joins_exact_raw_keys(self):
        class Result:
            result_rows = [[3, 3, 1000, 3000, 1, 2]]

        calls = []
        repository = object.__new__(ClickHouseMarketBarRepository)
        repository._query = lambda statement, parameters: (
            calls.append((statement, parameters)) or Result()
        )

        result = repository.get_canonical_ingestion_coverage(["b", "a", "a"])

        self.assertEqual(result, {
            "ingestion_ids": ["a", "b"], "raw_rows": 3,
            "canonical_rows": 3, "first_bar_at_ms": 1000,
            "last_bar_at_ms": 3000,
            "canonical_invalid_ohlc_rows": 1,
            "canonical_abnormal_price_rows": 2,
        })
        self.assertIn("SELECT DISTINCT symbol,interval,adjustment,bar_time", calls[0][0])
        self.assertIn("market_bar_canonical AS c FINAL", calls[0][0])
        self.assertEqual(calls[0][1], {"ingestion_ids": ["a", "b"]})

    def test_canonical_ingestion_quality_is_grouped_for_shard_diagnostics(self):
        class Result:
            result_rows = [["a", 2, 1, 1], ["b", 1, 1, 0]]

        calls = []
        repository = object.__new__(ClickHouseMarketBarRepository)
        repository._query = lambda statement, parameters: (
            calls.append((statement, parameters)) or Result()
        )
        result = repository.get_canonical_ingestion_quality(["b", "a"])
        self.assertEqual(result[0], {
            "ingestion_id": "a", "raw_rows": 2, "canonical_rows": 1,
            "canonical_invalid_ohlc_rows": 1,
        })
        self.assertIn("GROUP BY r.ingestion_id", calls[0][0])
        self.assertIn("market_bar_canonical AS c FINAL", calls[0][0])

    def test_canonical_json_normalizes_bytes_decimal_and_datetime(self):
        timestamp = datetime(2026, 7, 23, 1, 2, 3, 456000, timezone.utc)
        normalized = canonical_json_value([
            b"longport", Decimal("10.5000"), timestamp, None,
        ])

        self.assertEqual(normalized, [
            "longport", "10.5000", "2026-07-23T01:02:03.456000+00:00", None,
        ])
        self.assertEqual(
            canonical_json_value(datetime(2026, 7, 23, 1, 2, 3)),
            "2026-07-23T01:02:03+00:00",
        )
        with self.assertRaises(UnicodeDecodeError):
            canonical_json_value(b"\xff")

    def test_canonical_identity_is_stable_for_driver_value_types_and_empty_data(self):
        class Result:
            result_rows = [(
                1000, Decimal("10.50"), b"11", "9", "10.5", "10.5", "1",
                "100", "1000", b"longport", 1, b"single_source",
                datetime(2026, 7, 23, 1, tzinfo=timezone.utc),
                "17", 1100, 1200, b"artifact-a",
            )]

        repository = object.__new__(ClickHouseMarketBarRepository)
        repository._query = lambda *_args, **_kwargs: Result()
        first = repository.get_canonical_dataset_identity(
            b"QQQ", b"1d", b"raw",
            "2026-07-01T00:00:00Z", "2026-07-23T23:59:59Z",
        )
        second = repository.get_canonical_dataset_identity(
            b"QQQ", b"1d", b"raw",
            "2026-07-01T00:00:00Z", "2026-07-23T23:59:59Z",
        )
        self.assertEqual(first, second)
        self.assertEqual(first["symbol"], "QQQ")
        self.assertEqual(first["row_count"], 1)
        self.assertRegex(first["content_hash"], r"^sha256:[0-9a-f]{64}$")
        self.assertEqual(len(first["snapshot_id"]), 32)

        Result.result_rows = []
        empty_first = repository.get_canonical_dataset_identity(
            "QQQ", "1d", "raw",
            "2026-07-01T00:00:00Z", "2026-07-23T23:59:59Z",
        )
        empty_second = repository.get_canonical_dataset_identity(
            "QQQ", "1d", "raw",
            "2026-07-01T00:00:00Z", "2026-07-23T23:59:59Z",
        )
        self.assertEqual(empty_first, empty_second)
        self.assertEqual(empty_first["row_count"], 0)
        self.assertEqual(empty_first["canonical_version"], "0")
        self.assertNotEqual(empty_first["content_hash"], first["content_hash"])

    def test_canonical_identity_hashes_complete_ordered_rows(self):
        class Result:
            result_rows = [
                (
                    1000, "10", "11", "9", "10.5", "10.5", "1", "100",
                    "1000", "longport", 1, "single_source", "fingerprint-a",
                    "17", 1100, 1200, "artifact-a",
                ),
            ]

        repository = object.__new__(ClickHouseMarketBarRepository)
        repository._query = lambda *_args, **_kwargs: Result()
        first = repository.get_canonical_dataset_identity(
            "AAPL", "1m", "raw",
            "2026-07-23T00:00:00Z", "2026-07-23T01:00:00Z",
        )
        Result.result_rows[0] = (*Result.result_rows[0][:-1], "artifact-b")
        second = repository.get_canonical_dataset_identity(
            "AAPL", "1m", "raw",
            "2026-07-23T00:00:00Z", "2026-07-23T01:00:00Z",
        )
        self.assertNotEqual(first["content_hash"], second["content_hash"])
        self.assertEqual(first["canonical_version"], "1200")

    def test_quote_version_reserves_high_bits_for_ingestion_time(self):
        database = _InsertDatabase()
        repository = ClickHouseMarketBarRepository(database)
        repository.upsert_quote({
            "symbol": "AAPL", "source": "fixture", "price": 1,
            "observed_at": "2026-07-22T00:00:00+00:00",
            "ingested_at": "2026-07-22T00:00:01+00:00",
        })
        repository.upsert_quote({
            "symbol": "AAPL", "source": "fixture", "price": 2,
            "observed_at": "2026-07-22T00:00:00+00:00",
            "ingested_at": "2026-07-22T00:00:02+00:00",
        })
        rank_index = repository.QUOTE_COLUMNS.index("content_rank")
        version_index = repository.QUOTE_COLUMNS.index("content_version")
        self.assertTrue(all(len(row[rank_index]) == 52 for row in database.client.rows))
        self.assertLess(
            database.client.rows[0][version_index],
            database.client.rows[1][version_index],
        )
        repository.upsert_quote({
            "symbol": "AAPL", "source": "fixture", "price": 2,
            "observed_at": "2026-07-22T00:00:00+00:00",
            "ingested_at": "2026-07-22T00:00:02+00:00",
        })
        self.assertEqual(len(database.client.rows), 2)

    def test_direct_repository_satisfies_complete_contract(self):
        repository = ClickHouseMarketBarRepository(
            ClickHouseDatabase("127.0.0.1", 8123, "marketcow_test")
        )
        self.assertIsInstance(repository, MarketBarRepository)
        required = {
            name for name, value in MarketBarRepository.__dict__.items()
            if callable(value) and not name.startswith("_")
        }
        self.assertEqual(required - set(dir(repository)), set())

    def test_direct_call_chain_has_no_duckdb_or_offline_dependency(self):
        graph = {_module_name(path): _imports_for(path) for path in SOURCE.rglob("*.py")}
        forbidden = {
            "duckdb", "marketcow.storage", "marketcow.duckdb_repositories",
            "marketcow.clickhouse_shadow", "marketcow.local_backfill",
            "marketcow.restore_bundle",
        }
        violations = _forbidden_paths(
            graph, "marketcow.clickhouse_repositories", forbidden
        )
        self.assertEqual(violations, set(), sorted(violations))
        source = (SOURCE / "clickhouse_repositories.py").read_text()
        self.assertNotIn(" OFFSET ", source.upper())

    def test_writer_builder_scheduler_chains_have_no_duckdb_dependency(self):
        graph = {_module_name(path): _imports_for(path) for path in SOURCE.rglob("*.py")}
        forbidden = {
            "duckdb", "marketcow.storage", "marketcow.duckdb_repositories",
            "marketcow.clickhouse_shadow", "marketcow.local_backfill",
            "marketcow.restore_bundle",
        }
        for entrypoint in (
            "marketcow.clickhouse_writer", "marketcow.clickhouse_canonical",
            "marketcow.clickhouse_scheduler",
        ):
            with self.subTest(entrypoint=entrypoint):
                violations = _forbidden_paths(graph, entrypoint, forbidden)
                self.assertEqual(violations, set(), sorted(violations))

    def test_transitive_gate_reports_complete_reachable_path(self):
        graph = {
            "marketcow.clickhouse_repositories": {"marketcow.bridge"},
            "marketcow.bridge": {"marketcow.second"},
            "marketcow.second": {"duckdb.engine"},
        }
        self.assertEqual(
            _forbidden_paths(
                graph, "marketcow.clickhouse_repositories", {"duckdb"}
            ),
            {(
                "marketcow.clickhouse_repositories", "marketcow.bridge",
                "marketcow.second", "duckdb.engine",
            )},
        )

    def test_backend_errors_are_bounded_and_do_not_fallback(self):
        database = ClickHouseDatabase("127.0.0.1", 8123, "marketcow_test")
        database.client = _FailingClient()
        repository = ClickHouseMarketBarRepository(database)
        with self.assertRaises(ClickHouseRepositoryError) as raised:
            repository.get_latest_quotes(["FAIL"])
        message = str(raised.exception)
        self.assertLessEqual(len(message), 100)
        self.assertNotIn("secret", message)
        self.assertNotIn("duckdb", message.lower())


if __name__ == "__main__":
    unittest.main()
