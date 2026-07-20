import os
import unittest
import uuid

import psycopg

from marketcow.postgres_repositories import (
    PostgresDatabase,
    PostgresFundamentalRepository,
    PostgresMetadataRepository,
)


@unittest.skipUnless(
    os.getenv("MARKETCOW_TEST_POSTGRES_DSN"),
    "set MARKETCOW_TEST_POSTGRES_DSN to run PostgreSQL integration tests",
)
class PostgresRepositoryIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dsn = os.environ["MARKETCOW_TEST_POSTGRES_DSN"]
        cls.schema = "marketcow_" + uuid.uuid4().hex[:12] + "_test"
        cls.database = PostgresDatabase(cls.dsn, cls.schema, min_size=1, max_size=2)
        cls.database.open()
        cls.repository = PostgresMetadataRepository(cls.database)
        cls.fundamentals = PostgresFundamentalRepository(cls.database)

    @classmethod
    def tearDownClass(cls):
        cls.database.close()
        with psycopg.connect(cls.dsn, autocommit=True) as connection:
            connection.execute(f'DROP SCHEMA IF EXISTS "{cls.schema}" CASCADE')

    def test_migrations_control_plane_and_artifact_manifest(self):
        with self.database.connection() as connection:
            versions = connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall()
        self.assertEqual([row["version"] for row in versions], [1, 2])

        run = ["run-1", "fixture", "running", None, "2026-07-20T00:00:00+00:00", None, 0, None]
        self.repository.save_run(run)
        run[2], run[5], run[6] = "success", "2026-07-20T00:00:01+00:00", 3
        self.repository.save_run(run)
        self.assertEqual(self.repository.latest_runs(1)[0]["status"], "success")

        self.repository.record_provider_health("fixture", False, run[5], "timeout")
        self.repository.record_provider_health("fixture", True, run[5])
        health = self.repository.provider_health()[0]
        self.assertEqual(health["status"], "healthy")
        self.assertEqual(health["consecutive_failures"], 0)

        artifact = {
            "artifact_id": "artifact-1", "dataset": "fixture", "source": "test",
            "source_url": "https://example.test", "observed_at": run[5],
            "ingested_at": run[5], "raw_response_locator": "payload",
            "storage_path": "/tmp/fixture.json", "sha256": "abc", "byte_size": 3,
            "metadata_json": '{"report_period":"20260331"}',
        }
        self.repository.save_artifact(artifact)
        saved = self.repository.latest_artifact("fixture", "report_period", "20260331")
        self.assertEqual(saved["artifact_id"], "artifact-1")

    def test_calendar_round_trip(self):
        row = {
            "event_id": "event-1", "country": "US", "event_date": "2026-07-21",
            "event_time": "08:30:00", "event_name": "Fixture", "impact": "High",
            "source": "test", "observed_at": "2026-07-20T00:00:00+00:00",
            "ingested_at": "2026-07-20T00:00:01+00:00", "payload": {"value": 1},
        }
        self.assertEqual(self.repository.upsert_economic_calendar([row]), 1)
        events = self.repository.get_economic_calendar(
            "2026-07-20", "2026-07-22", "US", "High", 10
        )
        self.assertEqual(events[0]["event_id"], "event-1")
        self.assertEqual(events[0]["payload_json"], {"value": 1})

    def test_fundamental_history_and_strict_point_in_time_queries(self):
        base = {
            "instrument_id": "CN.XSHG.600298", "symbol": "600298", "exchange": "XSHG",
            "name": "Fixture", "is_active": True, "report_period": "20260331",
            "published_at": "2026-04-30", "industry": "Food", "roe_weighted": 8.5,
            "pe_dynamic": 20.0, "source": "fixture", "observed_at": "2026-05-01",
            "ingested_at": "2026-05-01", "fetched_at": "2026-05-01", "price": 10.0,
        }
        self.fundamentals.replace_fundamentals("20260331", [base])
        revised = {**base, "price": 20.0, "observed_at": "2026-07-01", "ingested_at": "2026-07-01", "fetched_at": "2026-07-01"}
        self.fundamentals.replace_fundamentals("20260331", [revised])

        current = self.fundamentals.query_fundamentals(symbol="600298", limit=1)
        point_in_time = self.fundamentals.query_fundamentals(
            symbol="600298", as_of="2026-06-01", limit=1
        )
        unavailable = self.fundamentals.query_fundamentals(
            symbol="600298", as_of="2026-04-29", limit=1
        )
        self.assertEqual(current[0]["price"], 20.0)
        self.assertEqual(point_in_time[0]["price"], 10.0)
        self.assertEqual(unavailable, [])

    def test_financial_statement_jsonb_round_trip_and_as_of(self):
        row = {
            "instrument_id": "CN.XSHG.600298", "symbol": "600298",
            "statement": "income", "report_date": "2026-03-31",
            "published_at": "2026-04-30", "source": "fixture",
            "payload": {"revenue": 123.0}, "fetched_at": "2026-05-01",
            "observed_at": "2026-05-01", "ingested_at": "2026-05-01",
        }
        self.fundamentals.replace_statement_rows("600298", "income", [row])
        available = self.fundamentals.get_statement_rows(
            "600298", "income", 10, "2026-06-01"
        )
        unavailable = self.fundamentals.get_statement_rows(
            "600298", "income", 10, "2026-04-29"
        )
        self.assertEqual(available[0]["payload"], {"revenue": 123.0})
        self.assertEqual(unavailable, [])


if __name__ == "__main__":
    unittest.main()
