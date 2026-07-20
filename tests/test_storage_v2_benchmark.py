import json
import os
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from marketcow.local_benchmark import (
    BENCHMARK_VERSION, OPERATIONS, BenchmarkInputs, BenchmarkPlan,
    LocalStorageBenchmark,
)


class Clock:
    def __init__(self, step=.01):
        self.value = 0.0
        self.step = step

    def __call__(self):
        value = self.value
        self.value += self.step
        return value


class StorageV2BenchmarkTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name)
        self.state = self.root / "benchmark-test"
        self.plan = BenchmarkPlan(10, 20, 240, 2, 3, max_peak_memory_mb=8192)

    def tearDown(self):
        self.folder.cleanup()

    def operations(self):
        rows = self.plan.sample_raw_rows
        values = {}
        for name in OPERATIONS:
            result = {"rows": rows, "logical": {"operation": name, "rows": rows}}
            if name == "raw_write":
                result["bytes"] = rows * 24
            if name == "archive":
                result.update({"bytes": rows * 12, "uncompressed_bytes": rows * 40})
            if name in {"page_first", "page_deep"}:
                result["query_plan"] = "ReadFromMergeTree Filter bar_time > cursor"
                result["query_sql"] = (
                    "SELECT bars ORDER BY bar_time LIMIT 101" if name == "page_first" else
                    "SELECT bars WHERE bar_time > cursor ORDER BY bar_time LIMIT 101"
                )
            if name == "merge_probe":
                result.update({"total_bytes": 1_000_000, "free_bytes": 400_000,
                               "merge_backlog": 2})
            values[name] = lambda result=result: dict(result)
        return values

    def benchmark(self, **overrides):
        values = dict(
            root=self.state, plan=self.plan, operations=self.operations(), profile="test",
            allowed_root=self.root, component_versions={"clickhouse": "25.8"},
            clock=Clock(.01),
        )
        values.update(overrides)
        return LocalStorageBenchmark(BenchmarkInputs(**values))

    def test_report_has_percentiles_capacity_slo_and_is_reproducible(self):
        report = self.benchmark().run()
        self.assertEqual(report["version"], BENCHMARK_VERSION)
        self.assertEqual(report["status"], "passed")
        self.assertTrue(all(report["checks"].values()))
        self.assertEqual(report["plan"]["sample_raw_rows"], 96000)
        self.assertEqual(report["plan"]["model_raw_rows"], 6_600_000_000)
        self.assertAlmostEqual(report["capacity"]["observed_clickhouse_free_ratio"], .4)
        self.assertGreater(
            report["capacity"]["model_required_disk_bytes_with_30pct_free"],
            report["capacity"]["model_online_bytes"],
        )
        for observation in report["observations"].values():
            self.assertEqual(set(observation["latency_seconds"]), {"p50", "p95", "p99"})
        persisted = json.loads(self.benchmark().report_path.read_text())
        self.assertEqual(persisted["status"], "passed")

    def test_offset_unstable_results_and_failed_slo_are_fail_closed(self):
        operations = self.operations()
        operations["page_deep"] = lambda: {
            "rows": 10, "logical": {"rows": 10},
            "query_plan": "ReadFromMergeTree", "query_sql": "SELECT bars OFFSET 10000",
        }
        with self.assertRaisesRegex(RuntimeError, "no-OFFSET"):
            self.benchmark(operations=operations).run()

        calls = {"value": 0}
        operations = self.operations()

        def unstable():
            calls["value"] += 1
            return {"rows": 10, "logical": {"generation": calls["value"]}}

        operations["query_warm"] = unstable
        with self.assertRaisesRegex(RuntimeError, "checksum is unstable"):
            self.benchmark(operations=operations).run()

        operations = self.operations()
        operations["merge_probe"] = lambda: {
            "rows": 1, "logical": {"probe": "full"}, "total_bytes": 100,
            "free_bytes": 20, "merge_backlog": 101,
        }
        with self.assertRaisesRegex(RuntimeError, "SLO failed"):
            self.benchmark(operations=operations).run()
        report = json.loads(self.benchmark(operations=operations).report_path.read_text())
        self.assertFalse(report["checks"]["clickhouse_free_reserve"])
        self.assertFalse(report["checks"]["merge_backlog"])

    def test_isolation_plan_and_operation_bounds(self):
        with self.assertRaisesRegex(ValueError, "development/test-only"):
            self.benchmark(profile="production")
        with self.assertRaisesRegex(ValueError, "bounded row"):
            LocalStorageBenchmark(BenchmarkInputs(
                self.root / "large-test", BenchmarkPlan(10000, 250, 1440, 8),
                self.operations(), "test", self.root,
            ))
        incomplete = self.operations()
        incomplete.pop("restore")
        with self.assertRaisesRegex(ValueError, "operations mismatch"):
            self.benchmark(operations=incomplete)


@unittest.skipUnless(
    os.getenv("MARKETCOW_TEST_CLICKHOUSE_HOST"),
    "set MARKETCOW_TEST_CLICKHOUSE_HOST for real Storage V2 benchmark",
)
class StorageV2BenchmarkIntegrationTest(unittest.TestCase):
    def test_disposable_clickhouse_parquet_spool_benchmark(self):
        import clickhouse_connect

        from marketcow.clickhouse_canonical import CanonicalMarketBarBuilder
        from marketcow.clickhouse_repositories import (
            ClickHouseDatabase, ClickHouseMarketBarRepository,
        )
        from marketcow.clickhouse_writer import (
            LocalClickHouseSpool, ReliableClickHouseWriter, normalize_bar,
        )
        from marketcow.cold_archive import ParquetColdArchive
        from marketcow.storage import Warehouse

        class FailingRepository:
            def insert_raw_bars(self, _rows, batch_id=""):
                raise ConnectionError("benchmark outage")

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            database_name = "benchmark_" + uuid.uuid4().hex[:10] + "_test"
            database = ClickHouseDatabase(
                os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"],
                int(os.getenv("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123")), database_name,
                os.getenv("MARKETCOW_TEST_CLICKHOUSE_USERNAME", "default"),
                os.getenv("MARKETCOW_TEST_CLICKHOUSE_PASSWORD", ""),
            )
            database.open()
            repository = ClickHouseMarketBarRepository(database)
            spool = LocalClickHouseSpool(root / "benchmark-test/spool", root)
            writer = ReliableClickHouseWriter(repository, spool, 1000)
            builder = CanonicalMarketBarBuilder(repository, writer)
            warehouse = Warehouse(root / "source-development/warehouse/market.duckdb")
            plan = BenchmarkPlan(1, 20, 240, 2, 3, max_peak_memory_mb=8192)
            start = datetime(2026, 7, 1, tzinfo=timezone.utc)
            rows = []
            for symbol_index in range(plan.symbols):
                symbol = f"B{symbol_index:04d}"
                for source_index, source in enumerate(("fixture", "secondary")):
                    source_bars = []
                    for day in range(plan.trading_days):
                        for minute in range(plan.bars_per_day):
                            moment = start + timedelta(days=day, minutes=minute)
                            close = float(symbol_index + source_index + day + minute / 1000)
                            bar = {
                                "symbol": symbol, "market": "US", "interval": "1m",
                                "adjustment": "raw", "bar_time": moment.isoformat(),
                                "open": close, "high": close + 1, "low": close - 1,
                                "close": close, "raw_close": close,
                                "adjustment_factor": 1, "volume": 100, "amount": 1000,
                                "source": source, "source_sequence": f"{day}:{minute}",
                                "observed_at": moment.isoformat(),
                                "ingested_at": (moment + timedelta(seconds=1)).isoformat(),
                                "raw_artifact_id": f"artifact-{source}",
                            }
                            rows.append(normalize_bar("raw", bar))
                            source_bars.append({
                                "timestamp": int(moment.timestamp()),
                                "bar_at": moment.isoformat(), "open": close,
                                "high": close + 1, "low": close - 1, "close": close,
                                "raw_close": close, "adjustment_factor": 1,
                                "volume": 100, "amount": 1000,
                            })
                    warehouse.upsert_price_bars(
                        symbol, "1m", "raw", source,
                        (start + timedelta(days=1)).isoformat(), source_bars,
                        {"observed_at": start.isoformat(),
                         "raw_artifact_id": f"artifact-{source}"},
                    )
            archive = ParquetColdArchive(
                warehouse.path, root / "source-development/archive",
                root / "source-development",
            )
            artifact = {"path": None}
            replay_run = {"value": 0}

            def raw_write():
                outcome = writer.write("raw", rows)
                stored = database.client.query(
                    "SELECT sum(bytes_on_disk) FROM system.parts WHERE active "
                    "AND database=currentDatabase() AND table='market_bar_raw'"
                ).result_rows[0][0] or 1
                return {"rows": len(rows), "bytes": int(stored),
                        "logical": {"rows": len(rows), "spooled": outcome["spooled"]}}

            def rebuild():
                total = 0
                for symbol_index in range(plan.symbols):
                    result = builder.rebuild(
                        f"B{symbol_index:04d}", "1m", "raw", start.isoformat(),
                        (start + timedelta(days=19, minutes=239)).isoformat(), 50000,
                    )
                    total += result["scanned_groups"]
                return {"rows": total, "logical": {"rows": total}}

            def query(after=None):
                result = repository.get_canonical_price_bars_page(
                    "B0000", "1m", "raw", start.isoformat(),
                    (start + timedelta(days=19, minutes=239)).isoformat(), 100, after,
                )
                return result

            def query_operation(after=None):
                result = query(after)
                return {"rows": len(result[0]),
                        "logical": {"bars": result[0], "more": result[1]}}

            def plan_query(after=None):
                predicate = "" if after is None else "AND bar_time > toDateTime64('2026-07-01 01:00:00',3,'UTC')"
                sql = (
                    "SELECT * FROM market_bar_canonical FINAL WHERE symbol='B0000' "
                    "AND interval='1m' AND adjustment='raw' " + predicate +
                    " ORDER BY bar_time LIMIT 101"
                )
                explanation = database.client.query(
                    "EXPLAIN " + sql
                ).result_rows
                result = query_operation(after)
                result["query_plan"] = " ".join(row[0] for row in explanation)
                result["query_sql"] = sql
                return result

            def archive_operation():
                result = archive.export_partition("US", "1m", "fixture", 2026, 7)
                artifact["path"] = Path(result["artifact_path"])
                manifest = json.loads((artifact["path"] / "manifest.json").read_text())
                return {"rows": result["row_count"], "bytes": manifest["parquet_bytes"],
                        "uncompressed_bytes": manifest["logical_json_bytes"],
                        "logical": {"artifact_id": result["artifact_id"],
                                    "rows": result["row_count"]}}

            def restore_operation():
                if artifact["path"] is None:
                    archive_operation()
                restored = archive.read_for_backfill(artifact["path"])
                return {"rows": len(restored),
                        "logical": {"rows": len(restored),
                                    "first": restored[0]["symbol"]}}

            def replay_operation():
                replay_run["value"] += 1
                local_spool = LocalClickHouseSpool(
                    root / f"replay-{replay_run['value']}-test", root,
                )
                failed = ReliableClickHouseWriter(FailingRepository(), local_spool, 1000)
                sample = rows[:1000]
                failed.write("raw", sample)
                recovered = ReliableClickHouseWriter(repository, local_spool, 1000).replay(10)
                return {"rows": len(sample),
                        "logical": {"rows": len(sample),
                                    "replayed": recovered["replayed"]}}

            def concurrent_operation():
                def isolated_query(_index):
                    client = database._connect(database_name)
                    try:
                        return client.query(
                            "SELECT count() FROM market_bar_canonical FINAL WHERE "
                            "symbol='B0000' AND interval='1m' AND adjustment='raw'"
                        ).result_rows[0][0]
                    finally:
                        client.close()

                with ThreadPoolExecutor(max_workers=4) as executor:
                    results = list(executor.map(isolated_query, range(4)))
                return {"rows": sum(results), "logical": {"counts": results}}

            def merge_probe():
                disk = database.client.query(
                    "SELECT total_space, free_space FROM system.disks LIMIT 1"
                ).result_rows[0]
                merges = database.client.query(
                    "SELECT count() FROM system.merges WHERE database=currentDatabase()"
                ).result_rows[0][0]
                return {"rows": 1, "total_bytes": disk[0], "free_bytes": disk[1],
                        "merge_backlog": merges, "logical": {"probe": "merge_disk"}}

            operations = {
                "raw_write": raw_write, "canonical_rebuild": rebuild,
                "query_warm": query_operation, "query_cold": query_operation,
                "page_first": lambda: plan_query(None),
                "page_deep": lambda: plan_query(int((start + timedelta(minutes=60)).timestamp())),
                "archive": archive_operation, "restore": restore_operation,
                "spool_recovery": replay_operation,
                "concurrent_query": concurrent_operation, "merge_probe": merge_probe,
            }
            try:
                report = LocalStorageBenchmark(BenchmarkInputs(
                    root / "benchmark-test", plan, operations, "test", root,
                    {"clickhouse": database.diagnostics()["version"], "duckdb": "local"},
                )).run()
                self.assertEqual(report["status"], "passed")
                self.assertEqual(report["plan"]["sample_raw_rows"], len(rows))
                self.assertTrue(all(report["checks"].values()))
            finally:
                database.close()
                bootstrap = clickhouse_connect.get_client(
                    host=os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"],
                    port=int(os.getenv("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123")),
                    username=os.getenv("MARKETCOW_TEST_CLICKHOUSE_USERNAME", "default"),
                    password=os.getenv("MARKETCOW_TEST_CLICKHOUSE_PASSWORD", ""),
                )
                bootstrap.command(f"DROP DATABASE IF EXISTS `{database_name}`")
                bootstrap.close()


if __name__ == "__main__":
    unittest.main()
