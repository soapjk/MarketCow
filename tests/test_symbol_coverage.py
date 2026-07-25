from __future__ import annotations

import unittest
from datetime import datetime, timezone

from marketcow.clickhouse_repositories import ClickHouseMarketBarRepository


class Result:
    column_names = [
        "layer", "interval", "adjustment", "first_bar", "last_bar",
        "row_count", "sources",
    ]
    result_rows = [[
        "canonical", "1d", "raw",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 7, 25, tzinfo=timezone.utc),
        123, ["yahoo", "longport"],
    ]]


class SymbolCoverageTest(unittest.TestCase):
    def test_coverage_query_is_parameterized_and_normalized(self):
        repository = ClickHouseMarketBarRepository(None)
        captured = {}

        def query(statement, parameters=None):
            captured["statement"] = statement
            captured["parameters"] = parameters
            return Result()

        repository._query = query
        rows = repository.get_symbol_coverage("AAPL")
        self.assertEqual(captured["parameters"], {"symbol": "AAPL"})
        self.assertIn("FINAL", captured["statement"])
        self.assertEqual(rows[0]["row_count"], 123)
        self.assertEqual(rows[0]["sources"], ["longport", "yahoo"])
        self.assertEqual(rows[0]["first_bar"], "2026-01-01T00:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
