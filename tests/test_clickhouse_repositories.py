import os
import unittest
import uuid

import clickhouse_connect

from marketcow.clickhouse_repositories import (
    ClickHouseDatabase,
    ClickHouseMarketBarRepository,
)


class ClickHouseDatabaseBoundaryTest(unittest.TestCase):
    def test_only_simple_isolated_local_database_is_allowed(self):
        with self.assertRaisesRegex(ValueError, "simple identifier"):
            ClickHouseDatabase("127.0.0.1", 8123, "bad-name_test")
        with self.assertRaisesRegex(ValueError, "loopback"):
            ClickHouseDatabase("clickhouse.example.com", 8123, "marketcow_test")
        with self.assertRaisesRegex(ValueError, "must end"):
            ClickHouseDatabase("127.0.0.1", 8123, "marketcow_production")

    def test_closed_database_rejects_operations(self):
        database = ClickHouseDatabase("127.0.0.1", 8123, "marketcow_test")
        with self.assertRaisesRegex(RuntimeError, "not open"):
            database.migrate()


@unittest.skipUnless(
    os.getenv("MARKETCOW_TEST_CLICKHOUSE_HOST"),
    "set MARKETCOW_TEST_CLICKHOUSE_HOST to run ClickHouse integration tests",
)
class ClickHouseRepositoryIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.host = os.environ["MARKETCOW_TEST_CLICKHOUSE_HOST"]
        cls.port = int(os.getenv("MARKETCOW_TEST_CLICKHOUSE_PORT", "8123"))
        cls.username = os.getenv("MARKETCOW_TEST_CLICKHOUSE_USERNAME", "default")
        cls.password = os.getenv("MARKETCOW_TEST_CLICKHOUSE_PASSWORD", "")
        cls.database_name = "marketcow_" + uuid.uuid4().hex[:12] + "_test"
        cls.database = ClickHouseDatabase(
            cls.host, cls.port, cls.database_name, cls.username, cls.password
        )
        cls.database.open()
        cls.repository = ClickHouseMarketBarRepository(cls.database)

    @classmethod
    def tearDownClass(cls):
        cls.database.close()
        client = clickhouse_connect.get_client(
            host=cls.host, port=cls.port, username=cls.username,
            password=cls.password, database="default",
        )
        try:
            client.command(f"DROP DATABASE IF EXISTS `{cls.database_name}`")
        finally:
            client.close()

    def test_migration_is_idempotent_and_diagnostics_are_healthy(self):
        self.database.migrate()
        diagnostics = self.database.diagnostics()
        self.assertEqual(diagnostics["status"], "ok")
        self.assertEqual(diagnostics["database"], self.database_name)
        self.assertTrue({"schema_migrations", "market_bar_raw",
                         "market_bar_canonical"}.issubset(diagnostics["tables"]))
        versions = self.database.client.query(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).result_rows
        self.assertEqual(versions, [(1,)])

    def test_raw_and_canonical_round_trip_with_replacing_keys(self):
        raw = {
            "symbol": "600519.SH", "market": "CN", "interval": "1m",
            "adjustment": "raw", "bar_time": "2026-07-20T01:31:00Z",
            "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0,
            "volume": 1000.0, "amount": 101000.0, "source": "fixture",
            "source_sequence": "1", "observed_at": "2026-07-20T01:31:01Z",
            "ingested_at": "2026-07-20T01:31:02Z", "raw_artifact_id": "artifact-1",
        }
        self.assertEqual(self.repository.insert_raw_bars([raw]), 1)
        self.assertEqual(self.repository.insert_raw_bars([raw]), 1)
        rows = self.repository.query_raw_bars("600519.SH")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source"], "fixture")
        canonical = {
            **{key: raw[key] for key in ["symbol", "market", "interval", "adjustment",
                "bar_time", "open", "high", "low", "close", "volume", "amount",
                "observed_at", "ingested_at", "raw_artifact_id"]},
            "selected_source": "fixture", "source_count": 1,
            "quality_status": "single_source", "version": 1,
            "updated_at": "2026-07-20T01:31:03Z",
        }
        self.assertEqual(self.repository.insert_canonical_bars([canonical]), 1)
        count = self.database.client.query(
            "SELECT count() FROM market_bar_canonical FINAL"
        ).result_rows[0][0]
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
