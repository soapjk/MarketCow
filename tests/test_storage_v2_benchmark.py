import json
import os
import shutil
import tempfile
import threading
import time
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
            result = {"rows": rows, "verification": {
                "expected_rows": rows, "actual_rows": rows,
                "expected_checksum": name, "actual_checksum": name,
            }}
            if name == "raw_write":
                result["bytes"] = rows * 24
            if name == "archive":
                result.update({"bytes": rows * 12, "uncompressed_bytes": rows * 40})
            if name in {"page_first", "page_deep"}:
                result["query_plan"] = "ReadFromMergeTree Filter bar_time > cursor"
                result["query_sql"] = (
                    "SELECT bars ORDER BY bar_time LIMIT 101" if name == "page_first" else
                    "SELECT bars WHERE bar_time > '2026-07-15 01:59:00.000' "
                    "ORDER BY bar_time LIMIT 101"
                )
                result["cursor_depth"] = 0 if name == "page_first" else 4600
                result["query_after"] = None if name == "page_first" else 1784075940
                result["explain_after"] = None if name == "page_first" else 1784075940
                result["depth_after"] = None if name == "page_first" else 1784075940
                result["cursor_predicate"] = (
                    "" if name == "page_first" else "2026-07-15 01:59:00.000"
                )
            if name == "query_warm":
                result["path_kind"] = "warm_existing_session"
            if name == "query_cold":
                result["path_kind"] = "new_connection"
            if name == "merge_probe":
                result.update({"total_bytes": 1_000_000, "free_bytes": 400_000,
                               "merge_backlog": 2})
            values[name] = lambda _run, result=result: dict(result)
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

    def test_capacity_uses_all_run_bytes_over_all_run_rows(self):
        operations = self.operations()
        run_rows = (100, 250, 625)

        def raw_write(run_index):
            rows = run_rows[run_index]
            return {"rows": rows, "bytes": rows * 24, "verification": {
                "expected_rows": rows, "actual_rows": rows,
                "expected_checksum": f"raw-{run_index}",
                "actual_checksum": f"raw-{run_index}",
            }}

        operations["raw_write"] = raw_write
        report = self.benchmark(operations=operations).run()
        self.assertEqual(report["capacity"]["bytes_per_raw_row"], 24)
        self.assertEqual(report["capacity"]["measured_raw_rows"], sum(run_rows))
        self.assertEqual(report["capacity"]["measured_raw_bytes"], sum(run_rows) * 24)

    def test_offset_target_mismatch_and_failed_slo_are_fail_closed(self):
        operations = self.operations()
        operations["page_deep"] = lambda _run: {
            "rows": 10, "verification": {"expected_rows": 10, "actual_rows": 10,
                "expected_checksum": "x", "actual_checksum": "x"},
            "query_plan": "ReadFromMergeTree", "query_sql": "SELECT bars OFFSET 10000",
        }
        with self.assertRaisesRegex(RuntimeError, "no-OFFSET"):
            self.benchmark(operations=operations).run()

        operations = self.operations()
        deep = operations["page_deep"](0)
        deep["explain_after"] += 60
        operations["page_deep"] = lambda _run: dict(deep)
        with self.assertRaisesRegex(RuntimeError, "depth cursor are not bound"):
            self.benchmark(operations=operations).run()

        operations = self.operations()
        deep = operations["page_deep"](0)
        deep["depth_after"] += 60
        operations["page_deep"] = lambda _run: dict(deep)
        with self.assertRaisesRegex(RuntimeError, "depth cursor are not bound"):
            self.benchmark(operations=operations).run()

        operations = self.operations()
        deep = operations["page_deep"](0)
        deep["cursor_predicate"] = "2026-07-01 01:00:00.000"
        operations["page_deep"] = lambda _run: dict(deep)
        with self.assertRaisesRegex(RuntimeError, "depth cursor are not bound"):
            self.benchmark(operations=operations).run()

        operations = self.operations()
        operations["query_warm"] = lambda _run: {
            "rows": 10, "path_kind": "warm_existing_session", "verification": {
                "expected_rows": 10, "actual_rows": 9,
                "expected_checksum": "expected", "actual_checksum": "wrong",
            },
        }
        with self.assertRaisesRegex(RuntimeError, "target verification mismatch"):
            self.benchmark(operations=operations).run()

        operations = self.operations()
        operations["merge_probe"] = lambda _run: {
            "rows": 1, "total_bytes": 100, "free_bytes": 20, "merge_backlog": 101,
            "verification": {"expected_rows": 1, "actual_rows": 1,
                "expected_checksum": "probe", "actual_checksum": "probe"},
        }
        with self.assertRaisesRegex(RuntimeError, "SLO failed"):
            self.benchmark(operations=operations).run()
        report = json.loads(self.benchmark(operations=operations).report_path.read_text())
        self.assertFalse(report["checks"]["clickhouse_free_reserve"])
        self.assertFalse(report["checks"]["merge_backlog"])

    def test_transient_thread_peak_is_observed(self):
        operations = self.operations()

        def transient(_run):
            workers = [threading.Thread(target=lambda: time.sleep(.03)) for _ in range(4)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
            return {"rows": 1, "verification": {
                "expected_rows": 1, "actual_rows": 1,
                "expected_checksum": "threads", "actual_checksum": "threads",
            }}

        operations["concurrent_query"] = transient
        strict = BenchmarkPlan(10, 20, 240, 2, 3, max_threads=3,
                               max_peak_memory_mb=8192)
        with self.assertRaisesRegex(RuntimeError, "thread_bound"):
            self.benchmark(operations=operations, plan=strict).run()

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
        from marketcow.contract_gate import normalize_contract_value
        from marketcow.local_backup import _hash, _json
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
            rows_by_run = []
            for run_index in range(plan.runs):
                run_rows = []
                run_start = start + timedelta(days=run_index * 31)
                for symbol_index in range(plan.symbols):
                    symbol = f"B{symbol_index:04d}"
                    for source_index, source in enumerate(("tushare", "yahoo")):
                        source_bars = []
                        for day in range(plan.trading_days):
                            for minute in range(plan.bars_per_day):
                                moment = run_start + timedelta(days=day, minutes=minute)
                                close = float(symbol_index + source_index + day + minute / 1000)
                                bar = {
                                    "symbol": symbol, "market": "US", "interval": "1m",
                                    "adjustment": "raw", "bar_time": moment.isoformat(),
                                    "open": close, "high": close + 1, "low": close - 1,
                                    "close": close, "raw_close": close,
                                    "adjustment_factor": 1, "volume": 100, "amount": 1000,
                                    "source": source,
                                    "source_sequence": str(int(moment.timestamp())),
                                    "observed_at": run_start.isoformat(),
                                    "ingested_at": (
                                        run_start + timedelta(days=20)
                                    ).isoformat(),
                                    "raw_artifact_id": f"artifact-{source}",
                                }
                                run_rows.append(normalize_bar("raw", bar))
                                source_bars.append({
                                    "timestamp": int(moment.timestamp()),
                                    "bar_at": moment.isoformat(), "open": close,
                                    "high": close + 1, "low": close - 1, "close": close,
                                    "raw_close": close, "adjustment_factor": 1,
                                    "volume": 100, "amount": 1000,
                                })
                        warehouse.upsert_price_bars(
                            symbol, "1m", "raw", source,
                            (run_start + timedelta(days=20)).isoformat(), source_bars,
                            {"observed_at": run_start.isoformat(),
                             "raw_artifact_id": f"artifact-{source}"},
                        )
                rows_by_run.append(run_rows)
            archive = ParquetColdArchive(
                warehouse.path, root / "source-development/archive",
                root / "source-development",
            )
            artifacts = {}
            canonical_expected = {}
            replay_run = {"value": 0}

            def verification(rows, checksum):
                return {"expected_rows": rows, "actual_rows": rows,
                        "expected_checksum": checksum, "actual_checksum": checksum}

            def verified_value(value):
                """Normalize target reads using the contract's bar-only legacy allowance."""
                normalized = normalize_contract_value(value)
                if isinstance(normalized, list):
                    return [verified_value(item) for item in normalized]
                if isinstance(normalized, dict):
                    return {
                        key: verified_value(item)
                        for key, item in normalized.items()
                        if not (key == "source_payload" and
                                "timestamp" in normalized and "source" in normalized)
                    }
                return normalized

            def raw_read_value(rows):
                mapped = []
                for row in rows:
                    moment = datetime.fromisoformat(
                        str(row["bar_time"]).replace("Z", "+00:00")
                    ).astimezone(timezone.utc)
                    mapped.append({
                        key: row.get(key) for key in (
                            "symbol", "interval", "adjustment", "open", "high", "low",
                            "close", "raw_close", "adjustment_factor", "volume", "amount",
                            "source", "source_sequence", "observed_at", "ingested_at",
                            "raw_artifact_id",
                        )
                    } | {"timestamp": int(moment.timestamp()),
                         "bar_at": moment.isoformat()})
                return verified_value(mapped)

            def raw_write(run_index):
                run_rows = rows_by_run[run_index]
                run_start = start + timedelta(days=run_index * 31)
                outcome = writer.write("raw", run_rows)
                expected_rows, actual_rows = [], []
                for source in ("tushare", "yahoo"):
                    expected_rows.extend(warehouse.get_raw_price_bars_range(
                        "B0000", "1m", "raw", run_start.isoformat(),
                        (run_start + timedelta(days=19, minutes=239)).isoformat(),
                        5000, [source],
                    )[0])
                    actual_rows.extend(repository.get_raw_price_bars_range(
                        "B0000", "1m", "raw", run_start.isoformat(),
                        (run_start + timedelta(days=19, minutes=239)).isoformat(),
                        5000, [source],
                    )[0])
                stored = database.client.query(
                    "SELECT sum(bytes_on_disk) FROM system.parts WHERE active "
                    "AND database=currentDatabase() AND table='market_bar_raw' "
                    "AND partition={partition:String}",
                    parameters={"partition": run_start.strftime("%Y%m")},
                ).result_rows[0][0] or 1
                return {"rows": len(run_rows), "bytes": int(stored),
                        "verification": {"expected_rows": len(expected_rows),
                            "actual_rows": len(actual_rows),
                            "expected_checksum": _hash(_json(
                                verified_value(expected_rows))),
                            "actual_checksum": _hash(_json(
                                verified_value(actual_rows)))},
                        "spooled": outcome["spooled"]}

            def rebuild(run_index):
                total = 0
                run_start = start + timedelta(days=run_index * 31)
                for symbol_index in range(plan.symbols):
                    result = builder.rebuild(
                        f"B{symbol_index:04d}", "1m", "raw", run_start.isoformat(),
                        (run_start + timedelta(days=19, minutes=239)).isoformat(), 50000,
                    )
                    total += result["scanned_groups"]
                expected = builder.build_rows(rows_by_run[run_index], [])[0]
                canonical_expected[run_index] = expected
                actual = repository.query_range(
                    "canonical", "B0000", "1m", "raw", run_start.isoformat(),
                    (run_start + timedelta(days=19, minutes=239)).isoformat(), 5000,
                )[0]
                return {"rows": total, "verification": {
                    "expected_rows": len(expected), "actual_rows": len(actual),
                    "expected_checksum": _hash(_json(verified_value(expected))),
                    "actual_checksum": _hash(_json(verified_value(actual))),
                }}

            def query(after=None):
                result = repository.get_canonical_price_bars_page(
                    "B0000", "1m", "raw", start.isoformat(),
                    (start + timedelta(days=19, minutes=239)).isoformat(), 100, after,
                )
                return result

            def query_operation(_run_index=0, after=None):
                result = query(after)
                expected_rows = repository._map_canonical_rows(canonical_expected[0])
                if after is not None:
                    expected_rows = [
                        row for row in expected_rows if row["timestamp"] > int(after)
                    ]
                expected = (expected_rows[:100], len(expected_rows) > 100)
                return {"rows": len(result[0]),
                        "path_kind": "warm_existing_session",
                        "verification": {"expected_rows": len(expected[0]),
                            "actual_rows": len(result[0]),
                            "expected_checksum": _hash(_json(verified_value(expected))),
                            "actual_checksum": _hash(_json(verified_value(result)))}}

            def cold_query(_run_index):
                client = database._connect(database_name)
                try:
                    count = client.query(
                        "SELECT count() FROM market_bar_canonical FINAL WHERE "
                        "symbol='B0000' AND interval='1m' AND adjustment='raw'"
                    ).result_rows[0][0]
                finally:
                    client.close()
                expected = plan.trading_days * plan.bars_per_day * plan.runs
                checksum = f"cold-count-{expected}"
                return {"rows": count, "path_kind": "new_connection",
                        "verification": {"expected_rows": expected, "actual_rows": count,
                            "expected_checksum": checksum,
                            "actual_checksum": f"cold-count-{count}"}}

            def plan_query(_run_index, after=None):
                predicate = ""
                cursor_depth = 0
                if after is not None:
                    after_at = datetime.fromtimestamp(int(after), timezone.utc)
                    predicate = (
                        "AND bar_time > toDateTime64('" +
                        after_at.strftime("%Y-%m-%d %H:%M:%S.000") + "',3,'UTC')"
                    )
                    cursor_depth = int(database.client.query(
                        "SELECT count() FROM market_bar_canonical FINAL WHERE "
                        "symbol='B0000' AND interval='1m' AND adjustment='raw' "
                        "AND bar_time >= {start:DateTime64(3)} "
                        "AND bar_time <= {after:DateTime64(3)}",
                        parameters={"start": start, "after": after_at},
                    ).result_rows[0][0])
                sql = (
                    "SELECT * FROM market_bar_canonical FINAL WHERE symbol='B0000' "
                    "AND interval='1m' AND adjustment='raw' " + predicate +
                    " ORDER BY bar_time LIMIT 101"
                )
                explanation = database.client.query(
                    "EXPLAIN " + sql
                ).result_rows
                result = query_operation(0, after)
                result["query_plan"] = " ".join(row[0] for row in explanation)
                result["query_sql"] = sql
                result["cursor_depth"] = cursor_depth
                result["query_after"] = after
                result["explain_after"] = after
                result["depth_after"] = after
                result["cursor_predicate"] = (
                    "" if after is None else
                    after_at.strftime("%Y-%m-%d %H:%M:%S.000")
                )
                return result

            def archive_operation(run_index):
                run_start = start + timedelta(days=run_index * 31)
                result = archive.export_partition(
                    "US", "1m", "tushare", run_start.year, run_start.month,
                )
                artifacts[run_index] = Path(result["artifact_path"])
                manifest = json.loads((artifacts[run_index] / "manifest.json").read_text())
                verified = archive.verify(artifacts[run_index])
                return {"rows": result["row_count"], "bytes": manifest["parquet_bytes"],
                        "uncompressed_bytes": manifest["logical_json_bytes"],
                        "verification": {"expected_rows": manifest["row_count"],
                            "actual_rows": verified["row_count"],
                            "expected_checksum": manifest["logical_checksum"],
                            "actual_checksum": verified["logical_checksum"]}}

            def restore_operation(run_index):
                if run_index not in artifacts:
                    archive_operation(run_index)
                restored = archive.read_for_backfill(artifacts[run_index])
                queried = archive.query(artifacts[run_index])
                expected_checksum = _hash(_json(queried))
                return {"rows": len(restored),
                        "verification": {"expected_rows": len(queried),
                            "actual_rows": len(restored),
                            "expected_checksum": expected_checksum,
                            "actual_checksum": _hash(_json(restored))}}

            def replay_operation(run_index):
                replay_run["value"] += 1
                local_spool = LocalClickHouseSpool(
                    root / f"replay-{replay_run['value']}-test", root,
                )
                failed = ReliableClickHouseWriter(FailingRepository(), local_spool, 1000)
                sample = []
                for row in rows_by_run[run_index][:1000]:
                    changed = dict(row)
                    changed.update({
                        "symbol": f"SPOOL{run_index}",
                        "source": f"spool-{run_index}",
                        "raw_artifact_id": f"spool-artifact-{run_index}",
                    })
                    sample.append(normalize_bar("raw", changed))
                failed.write("raw", sample)
                recovered = ReliableClickHouseWriter(repository, local_spool, 1000).replay(10)
                run_start = start + timedelta(days=run_index * 31)
                actual = repository.get_raw_price_bars_range(
                    f"SPOOL{run_index}", "1m", "raw", run_start.isoformat(),
                    (run_start + timedelta(days=4, minutes=39)).isoformat(),
                    5000, [f"spool-{run_index}"],
                )[0]
                return {"rows": len(sample),
                        "verification": {"expected_rows": len(sample),
                            "actual_rows": len(actual),
                            "expected_checksum": _hash(_json(raw_read_value(sample))),
                            "actual_checksum": _hash(_json(verified_value(actual))),
                        }, "replayed": recovered["replayed"]}

            def concurrent_operation(_run_index):
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
                expected_each = plan.trading_days * plan.bars_per_day * plan.runs
                expected = [expected_each] * 4
                return {"rows": sum(results),
                        "verification": {"expected_rows": sum(expected),
                            "actual_rows": sum(results),
                            "expected_checksum": _hash(_json(expected)),
                            "actual_checksum": _hash(_json(results))}}

            def merge_probe(_run_index):
                disk = database.client.query(
                    "SELECT total_space, free_space FROM system.disks LIMIT 1"
                ).result_rows[0]
                merges = database.client.query(
                    "SELECT count() FROM system.merges WHERE database=currentDatabase()"
                ).result_rows[0][0]
                return {"rows": 1, "total_bytes": disk[0], "free_bytes": disk[1],
                        "merge_backlog": merges,
                        "verification": verification(1, "merge-disk")}

            operations = {
                "raw_write": raw_write, "canonical_rebuild": rebuild,
                "query_warm": query_operation, "query_cold": cold_query,
                "page_first": lambda run_index: plan_query(run_index, None),
                "page_deep": lambda run_index: plan_query(
                    run_index, int((start + timedelta(days=19, minutes=39)).timestamp())
                ),
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
                self.assertEqual(report["plan"]["sample_raw_rows"], len(rows_by_run[0]))
                self.assertTrue(all(report["checks"].values()))
                if export_root := os.getenv("MARKETCOW_READINESS_EVIDENCE_ROOT"):
                    target = Path(export_root) / "SV2-023"
                    target.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(root / "benchmark-test/storage-v2-benchmark.json",
                                 target / "report.json")
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
