import os
import unittest
import uuid

import psycopg

from marketcow.postgres_repositories import PostgresDatabase, PostgresMetadataRepository


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
        self.assertEqual([row["version"] for row in versions], [1])

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


if __name__ == "__main__":
    unittest.main()
