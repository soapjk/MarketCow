import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

from marketcow.clickhouse_canonical import CanonicalMarketBarBuilder
from marketcow.clickhouse_writer import LocalClickHouseSpool, ReliableClickHouseWriter


def raw(source="sina", close=10.0, observed="2026-07-20T01:00:01Z",
        ingested="2026-07-20T01:00:02Z", bar_time="2026-07-20T01:00:00Z"):
    return {
        "symbol": "0700.HK", "market": "HK", "interval": "1m",
        "adjustment": "raw", "bar_time": bar_time, "open": 9.0, "high": 11.0,
        "low": 8.0, "close": close, "volume": 100.0, "amount": 1000.0,
        "source": source, "source_sequence": "1", "observed_at": observed,
        "ingested_at": ingested, "raw_artifact_id": f"artifact-{source}",
    }


class FakeRepository:
    def __init__(self, raw_rows=None, canonical_rows=None, fail=False):
        self.raw_rows = raw_rows or []
        self.canonical_rows = canonical_rows or []
        self.fail = fail
        self.inserted = []

    def query_range(self, dataset, symbol, interval, adjustment, start, end, limit):
        rows = self.raw_rows if dataset == "raw" else self.canonical_rows
        return rows[:limit], len(rows) > limit

    def insert_canonical_bars(self, rows, batch_id=""):
        if self.fail:
            raise ConnectionError("fixture unavailable")
        self.inserted.extend(rows)
        return len(rows)

    def insert_raw_bars(self, rows, batch_id=""):
        return len(rows)


class CanonicalMarketBarBuilderTest(unittest.TestCase):
    def builder(self, repository, folder):
        writer = ReliableClickHouseWriter(
            repository, LocalClickHouseSpool(Path(folder) / "spool", Path(folder)), 1000
        )
        return CanonicalMarketBarBuilder(repository, writer, ("tushare", "sina"))

    def test_deterministic_priority_quality_tolerance_and_difference(self):
        with tempfile.TemporaryDirectory() as folder:
            builder = self.builder(FakeRepository(), folder)
            rows = [raw("sina", 10.0), raw("tushare", 10.000000001)]
            built, qualities, sources = builder.build_rows(rows, [])
            self.assertEqual(built[0]["selected_source"], "tushare")
            self.assertEqual(built[0]["source_count"], 2)
            self.assertEqual(built[0]["quality_status"], "multi_source_consistent")
            self.assertEqual(qualities["multi_source_consistent"], 1)
            self.assertEqual(sources["tushare"], 1)
            reversed_built, _, _ = builder.build_rows(list(reversed(rows)), [])
            self.assertEqual(built, reversed_built)
            different, _, _ = builder.build_rows(
                [raw("sina", 10.0), raw("tushare", 10.1)], []
            )
            self.assertEqual(different[0]["quality_status"],
                             "multi_source_ohlcva_difference")

    def test_stable_tie_break_and_monotonic_content_version(self):
        with tempfile.TemporaryDirectory() as folder:
            builder = self.builder(FakeRepository(), folder)
            first, _, _ = builder.build_rows([raw("unknown-z"), raw("unknown-a")], [])
            self.assertEqual(first[0]["selected_source"], "unknown-a")
            same, _, _ = builder.build_rows(
                [raw("unknown-z"), raw("unknown-a")], [deepcopy(first[0])]
            )
            self.assertEqual(same[0]["version"], 1)
            changed, _, _ = builder.build_rows(
                [raw("unknown-z"), raw("unknown-a", 10.2)], [deepcopy(first[0])]
            )
            self.assertEqual(changed[0]["version"], 2)

    def test_bounded_rebuild_spools_failure_and_replays(self):
        with tempfile.TemporaryDirectory() as folder:
            repository = FakeRepository([raw()], fail=True)
            builder = self.builder(repository, folder)
            result = builder.rebuild(
                "0700.HK", "1m", "raw", "2026-07-20T01:00:00Z",
                "2026-07-20T01:01:00Z", 10,
            )
            self.assertEqual(result["status"], "spooled")
            self.assertEqual(result["spooled"], 1)
            repository.fail = False
            replayed = builder.writer.replay()
            self.assertEqual({key: replayed[key] for key in ("attempted", "replayed", "failed")},
                             {"attempted": 1, "replayed": 1, "failed": 0})
            repository.raw_rows = [raw(bar_time=f"2026-07-20T01:00:{i:02d}Z")
                                   for i in range(3)]
            truncated = builder.rebuild(
                "0700.HK", "1m", "raw", "2026-07-20T01:00:00Z",
                "2026-07-20T01:01:00Z", 2,
            )
            self.assertEqual(truncated["status"], "truncated")
            self.assertEqual(truncated["written"], 0)

    def test_invalid_range_is_structured_error(self):
        with tempfile.TemporaryDirectory() as folder:
            builder = self.builder(FakeRepository(), folder)
            result = builder.rebuild(
                "0700.HK", "1m", "raw", "2026-07-21T00:00:00Z",
                "2026-07-20T00:00:00Z",
            )
            self.assertEqual(result["status"], "error")


if __name__ == "__main__":
    unittest.main()
