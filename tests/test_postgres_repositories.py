import os
import unittest
import uuid
import hashlib
from concurrent.futures import ThreadPoolExecutor

import psycopg
import json

from marketcow.postgres_repositories import (
    PostgresDatabase,
    PostgresFundamentalRepository,
    PostgresMetadataRepository,
)
from marketcow.backup_bundle import BackupComponent
from marketcow.postgres_migrations import (
    POSTGRES_MIGRATIONS,
    POSTGRES_TRANSACTION_DOMAINS,
)
from marketcow.repositories import ControlPlaneRepository


class PostgresDomainInventoryTest(unittest.TestCase):
    def test_bg003_inventory_is_explicit_and_complete(self):
        self.assertEqual(len(POSTGRES_TRANSACTION_DOMAINS), 21)
        self.assertEqual(len(set(POSTGRES_TRANSACTION_DOMAINS)), 21)
        self.assertEqual(
            POSTGRES_TRANSACTION_DOMAINS[-2:],
            ("runtime_config_version", "migration_checkpoint"),
        )


@unittest.skipUnless(
    os.getenv("MARKETCOW_TEST_POSTGRES_DSN"),
    "set MARKETCOW_TEST_POSTGRES_DSN to run PostgreSQL integration tests",
)
class PostgresRepositoryIntegrationTest(unittest.TestCase):
    def test_backup_component_extracts_real_postgres_schema(self):
        component = BackupComponent.postgresql(
            self.database, "2026-07-20T00:00:00Z"
        )
        payload = json.loads(component.files["logical.json"])
        self.assertIn("schema_migrations", payload)
        self.assertGreater(component.watermark["table_count"], 0)

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

    def _create_v4_schema(self, schema, invalid_table=""):
        with psycopg.connect(self.dsn, autocommit=True) as connection:
            connection.execute(f'CREATE SCHEMA "{schema}"')
            connection.execute(f'SET search_path TO "{schema}", public')
            connection.execute("""
                CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY, description TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            for version, description, statement in POSTGRES_MIGRATIONS[:4]:
                connection.execute(statement)
                connection.execute(
                    "INSERT INTO schema_migrations(version, description) VALUES (%s, %s)",
                    (version, description),
                )
            if invalid_table == "ingestion_runs":
                connection.execute(
                    "INSERT INTO ingestion_runs "
                    "(run_id,job_name,status,started_at,row_count) "
                    "VALUES ('bad','fixture','failed','2026-07-20',-1)"
                )
            elif invalid_table == "provider_health":
                connection.execute(
                    "INSERT INTO provider_health "
                    "(provider,status,last_attempt_at,consecutive_failures) "
                    "VALUES ('bad','unhealthy','2026-07-20',-1)"
                )
            elif invalid_table == "raw_artifact_manifest":
                connection.execute(
                    "INSERT INTO raw_artifact_manifest "
                    "(artifact_id,dataset,source,observed_at,ingested_at,storage_path,sha256,byte_size) "
                    "VALUES ('bad','fixture','fixture','2026-07-20','2026-07-20','bad','bad',-1)"
                )

    def test_migrations_control_plane_and_artifact_manifest(self):
        self.assertIsInstance(self.repository, ControlPlaneRepository)
        self.database.migrate()
        with self.database.connection() as connection:
            versions = connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall()
        self.assertEqual([row["version"] for row in versions], [1, 2, 3, 4, 5])

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

    def test_runtime_config_pit_and_checkpoint_compare_and_swap(self):
        def config(version, observed, value, payload=None):
            payload = payload or {"metadata_backend": "postgres", "value": value}
            digest = hashlib.sha256(json.dumps(
                payload, sort_keys=True, separators=(",", ":")
            ).encode()).hexdigest()
            return {
                "config_id": "runtime", "version": version,
                "profile": "test", "schema_version": "marketcow.runtime-config.v1",
                "config_json": payload, "config_sha256": digest,
                "observed_at": observed, "actor": "test",
            }

        first = self.repository.save_runtime_config_version(
            config(1, "2026-07-20T00:00:00Z", "first")
        )
        repeated = self.repository.save_runtime_config_version(
            config(1, "2026-07-20T00:00:00Z", "first")
        )
        self.repository.save_runtime_config_version(
            config(2, "2026-07-21T00:00:00Z", "second")
        )
        self.assertEqual(first["version"], repeated["version"])
        equivalent = config(
            1, "2026-07-20T00:00:00Z", "first",
            {"value": "first", "metadata_backend": "postgres"},
        )
        self.assertEqual(
            self.repository.save_runtime_config_version(equivalent)["version"], 1
        )
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.repository.save_runtime_config_version({
                **config(3, "2026-07-22T00:00:00Z", "third"),
                "config_sha256": "0" * 64,
            })
        first_hash = first["config_sha256"]
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.repository.save_runtime_config_version({
                **config(3, "2026-07-22T00:00:00Z", "different"),
                "config_sha256": first_hash,
            })
        self.assertEqual(self.repository.get_runtime_config_version(
            "runtime", "2026-07-20T12:00:00Z"
        )["config_json"]["value"], "first")
        self.assertEqual(self.repository.get_runtime_config_version(
            "runtime"
        )["config_json"]["value"], "second")

        checkpoint = {
            "run_id": "migration-1", "domain": "fundamental_snapshot", "shard": "a",
            "status": "running", "source_watermark": "10", "target_watermark": "9",
            "cursor_json": {"after": "600001"}, "evidence_json": {"rows": 10},
            "updated_at": "2026-07-21T00:00:00Z",
        }
        created = self.repository.upsert_migration_checkpoint(checkpoint)
        self.assertEqual(created["revision"], 1)
        updated = self.repository.upsert_migration_checkpoint(
            {**checkpoint, "status": "completed", "target_watermark": "10"},
            expected_revision=1,
        )
        self.assertEqual(updated["revision"], 2)
        self.assertEqual(updated["cursor_json"], {"after": "600001"})
        with self.assertRaisesRegex(RuntimeError, "revision conflict"):
            self.repository.upsert_migration_checkpoint(
                {**checkpoint, "status": "failed"}, expected_revision=1
            )
        self.assertEqual(self.repository.get_migration_checkpoint(
            "migration-1", "fundamental_snapshot", "a"
        )["status"], "completed")

        concurrent = {**checkpoint, "run_id": "migration-concurrent"}
        self.repository.upsert_migration_checkpoint(concurrent)
        def advance(status):
            try:
                return self.repository.upsert_migration_checkpoint(
                    {**concurrent, "status": status}, expected_revision=1
                )["status"]
            except RuntimeError:
                return "conflict"
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(advance, ("completed", "failed")))
        self.assertEqual(outcomes.count("conflict"), 1)
        self.assertEqual(self.repository.get_migration_checkpoint(
            "migration-concurrent", "fundamental_snapshot", "a"
        )["revision"], 2)

    def test_upgrade_from_migration_four_and_repeat_migrate(self):
        schema = "marketcow_upgrade_" + uuid.uuid4().hex[:10] + "_test"
        try:
            self._create_v4_schema(schema)
            upgraded = PostgresDatabase(self.dsn, schema, min_size=1, max_size=2)
            upgraded.open()
            upgraded.migrate()
            with upgraded.connection() as connection:
                versions = [row["version"] for row in connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall()]
                tables = {row["table_name"] for row in connection.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema=%s",
                    (schema,),
                ).fetchall()}
            self.assertEqual(versions, [1, 2, 3, 4, 5])
            self.assertTrue(set(POSTGRES_TRANSACTION_DOMAINS).issubset(tables))
            upgraded.close()
        finally:
            with psycopg.connect(self.dsn, autocommit=True) as connection:
                connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

    def test_upgrade_from_migration_four_rejects_invalid_history(self):
        for invalid_table in (
            "ingestion_runs", "provider_health", "raw_artifact_manifest"
        ):
            with self.subTest(table=invalid_table):
                schema = "marketcow_invalid_" + uuid.uuid4().hex[:10] + "_test"
                database = None
                try:
                    self._create_v4_schema(schema, invalid_table)
                    database = PostgresDatabase(self.dsn, schema, min_size=1, max_size=2)
                    with self.assertRaises(psycopg.errors.CheckViolation):
                        database.open()
                    with psycopg.connect(self.dsn) as connection:
                        versions = [row[0] for row in connection.execute(
                            f'SELECT version FROM "{schema}".schema_migrations ORDER BY version'
                        ).fetchall()]
                    self.assertEqual(versions, [1, 2, 3, 4])
                finally:
                    if database is not None:
                        database.close()
                    with psycopg.connect(self.dsn, autocommit=True) as connection:
                        connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

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

    def test_indicator_earnings_and_tushare_json_round_trip(self):
        observed = "2026-07-20T00:00:00Z"
        self.repository.upsert_economic_indicators([{
            "indicator_id": "us-cpi", "country": "US", "name": "CPI",
            "source": "fixture", "period": "2026-06", "value": 2.5,
            "latest_date": "2026-07-20", "observed_at": observed,
            "ingested_at": observed, "payload": {"nested": {"unit": "%"}},
        }])
        indicator = self.repository.get_economic_indicators("US", "fixture", 1)[0]
        self.assertEqual(indicator["payload_json"], {"nested": {"unit": "%"}})

        self.repository.upsert_earnings_calendar([{
            "event_id": "earnings-1", "market": "CN", "symbol": "600298",
            "report_date": "2026-08-01", "source": "fixture",
            "observed_at": observed, "ingested_at": observed,
            "payload": {"forecast": {"eps": "1.20"}},
        }])
        earnings = self.repository.get_earnings_calendar(
            "2026-08-01", "2026-08-01", "CN", ["600298"], 1
        )[0]
        self.assertEqual(earnings["payload_json"]["forecast"]["eps"], "1.20")

        request = {
            "request_id": "tushare-1", "api_name": "daily_basic",
            "params": {"trade_date": "20260720"}, "response_fields": ["ts_code"],
            "response_code": 0, "source": "tushare", "observed_at": observed,
            "ingested_at": observed,
        }
        self.assertEqual(self.repository.save_tushare_response(
            request, [{"ts_code": "600298.SH", "trade_date": "20260720", "pe": 12.3}]
        ), 1)
        response = self.repository.get_tushare_response("tushare-1")
        self.assertEqual(response["params_json"], {"trade_date": "20260720"})
        self.assertEqual(response["rows"][0]["payload_json"]["pe"], 12.3)

    def test_database_constraints_reject_invalid_control_values(self):
        with self.assertRaises(psycopg.errors.CheckViolation):
            with self.database.connection() as connection:
                connection.execute(
                    "INSERT INTO ingestion_runs "
                    "(run_id,job_name,status,started_at,row_count) VALUES (%s,%s,%s,%s,%s)",
                    ("negative", "fixture", "failed", "2026-07-20", -1),
                )
        with self.assertRaises(psycopg.errors.CheckViolation):
            self.repository.upsert_migration_checkpoint({
                "run_id": "bad-status", "domain": "fixture", "status": "unknown",
                "updated_at": "2026-07-20T00:00:00Z",
            })

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

    def test_baostock_and_tdx_history_round_trip(self):
        now = "2026-05-01"
        self.fundamentals.upsert_baostock({
            "symbol": "600298", "report_period": "20260331",
            "published_at": "2026-04-30", "trade_date": "2026-04-30",
            "pe_ttm": 20.5, "roe_avg": 8.0, "payload": {"provider": "baostock"},
            "observed_at": now, "ingested_at": now, "fetched_at": now,
        })
        baostock = self.fundamentals.get_baostock("600298", "20260331")
        self.assertEqual(baostock["payload"], {"provider": "baostock"})

        first = {
            "symbol": "600298", "report_period": "20251231",
            "published_at": "2026-04-30", "roe_weighted": 15.0,
            "revenue_ttm": 100.0, "net_profit_parent_ttm": 10.0,
            "source": "tdx", "observed_at": now, "ingested_at": now,
            "fetched_at": now,
        }
        self.fundamentals.replace_tdx_period("20251231", [first])
        revised = {
            **first, "roe_weighted": 16.0, "observed_at": "2026-07-01",
            "ingested_at": "2026-07-01", "fetched_at": "2026-07-01",
        }
        self.fundamentals.replace_tdx_period("20251231", [revised])
        current = self.fundamentals.get_tdx("600298", "20251231")
        pit = self.fundamentals.get_tdx_history(
            "600298", annual_only=True, as_of="2026-06-01"
        )
        self.assertEqual(current["roe_weighted"], 16.0)
        self.assertEqual(pit[0]["roe_weighted"], 15.0)
        self.assertEqual(self.fundamentals.tdx_coverage()[0]["row_count"], 1)

    def test_validation_upsert_and_rebuild(self):
        key = {
            "symbol": "600519", "report_period": "20260331", "metric": "fixture",
            "source_a": "a", "source_b": "b", "value_a": 10.0, "value_b": 10.1,
            "difference_pct": 1.0, "status": "consistent", "observed_at": "2026-05-01",
        }
        self.assertEqual(self.fundamentals.save_validation_results([key]), 1)
        self.fundamentals.save_validation_results([{**key, "value_b": 12.0,
            "difference_pct": 20.0, "status": "difference_over_1pct",
            "observed_at": "2026-05-02"}])
        saved = self.fundamentals.get_validation_results("600519", "20260331")
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]["value_b"], 12.0)
        self.fundamentals.replace_fundamentals("20260331", [{
            "symbol": "600519", "report_period": "20260331", "name": "Fixture",
            "is_active": True, "published_at": "2026-04-30", "roe_weighted": 10.0,
            "revenue": 100.0, "net_profit": 10.0, "total_assets": 200.0,
            "total_liabilities": 80.0, "operating_cashflow": 12.0,
            "observed_at": "2026-05-01", "ingested_at": "2026-05-01",
            "fetched_at": "2026-05-01",
        }])
        self.fundamentals.upsert_baostock({"symbol": "600519",
            "report_period": "20260331", "roe_avg": 10.0,
            "observed_at": "2026-05-01", "ingested_at": "2026-05-01"})
        self.fundamentals.replace_tdx_period("20260331", [{
            "symbol": "600519", "report_period": "20260331", "roe_weighted": 10.0,
            "revenue": 101.0, "net_profit_parent": 10.0, "total_assets": 200.0,
            "total_liabilities": 80.0, "operating_cashflow": 12.0,
            "observed_at": "2026-05-01", "ingested_at": "2026-05-01"}])
        self.assertEqual(self.fundamentals.rebuild_validation_results(
            "20260331", "2026-05-03"), 8)
        rebuilt = self.fundamentals.get_validation_results("600519", "20260331")
        self.assertEqual(sum(row["metric"] != "fixture" for row in rebuilt), 7)

    def test_funnel_rebuild_filters_and_point_in_time_query(self):
        observed = "2026-05-01"
        self.fundamentals.replace_fundamentals("20260331", [{
            "symbol": "000001", "report_period": "20260331", "name": "Fixture Bank",
            "is_active": True, "published_at": "2026-04-30", "pe_dynamic": 8.0,
            "pb": 1.0, "observed_at": observed, "ingested_at": observed,
            "fetched_at": observed}])
        for period, roe, revenue, profit in [("20231231", 12.0, 100.0, 10.0),
            ("20241231", 14.0, 110.0, 11.0), ("20251231", 16.0, 121.0, 12.1)]:
            self.fundamentals.replace_tdx_period(period, [{
                "symbol": "000001", "report_period": period,
                "published_at": "2026-04-30", "roe_weighted": roe,
                "revenue_ttm": revenue, "net_profit_parent_ttm": profit,
                "net_profit_parent": profit, "total_assets": 200.0,
                "total_liabilities": 80.0, "cash": 40.0,
                "operating_cashflow": 15.0, "capex": 5.0,
                "observed_at": observed, "ingested_at": observed, "fetched_at": observed}])
        self.fundamentals.upsert_baostock({"symbol": "000001",
            "report_period": "20260331", "trade_date": "2026-04-30",
            "pe_ttm": 7.5, "pb_mrq": 0.9, "observed_at": observed,
            "ingested_at": observed, "fetched_at": observed})
        self.assertEqual(self.fundamentals.rebuild_funnel_metrics("2026-05-02"), 1)
        rows = self.fundamentals.query_funnel_metrics(min_roe_median=13,
            min_revenue_cagr=9, max_pe=8, max_debt_ratio=50, min_annual_periods=3)
        self.assertEqual(rows[0]["symbol"], "000001")
        self.assertAlmostEqual(rows[0]["roe_annual_median"], 14.0)
        self.assertEqual(rows[0]["quality_status"], "multi_source_pending_validation")
        pit_symbols = {row["symbol"] for row in self.fundamentals.query_funnel_metrics(
            as_of="2026-06-01")}
        self.assertIn("000001", pit_symbols)

    def test_z_all_eighteen_transaction_domains_are_nonempty(self):
        """The integration class exercises every authoritative BG-003 domain."""
        with self.database.connection() as connection:
            counts = {
                table: connection.execute(
                    f'SELECT count(*) AS count FROM "{table}"'
                ).fetchone()["count"]
                for table in POSTGRES_TRANSACTION_DOMAINS
            }
        self.assertEqual(set(counts), set(POSTGRES_TRANSACTION_DOMAINS))
        self.assertFalse({table: count for table, count in counts.items() if count < 1})


if __name__ == "__main__":
    unittest.main()
