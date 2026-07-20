from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .postgres_migrations import POSTGRES_MIGRATIONS
from .domain_columns import FUNDAMENTAL_COLUMNS, TDX_COLUMNS


class PostgresDatabase:
    """Development-only PostgreSQL connection and schema migration boundary."""

    def __init__(
        self, dsn: str, schema: str, min_size: int = 1, max_size: int = 4,
        connect_timeout: float = 2.0, read_timeout: float = 5.0,
    ) -> None:
        if not 0.1 <= connect_timeout <= 30 or not 0.1 <= read_timeout <= 60:
            raise ValueError("PostgreSQL timeout bounds are invalid")
        self.schema = schema
        self.connect_timeout = float(connect_timeout)
        self.read_timeout = float(read_timeout)
        self.pool = ConnectionPool(
            make_conninfo(dsn, connect_timeout=max(1, int(connect_timeout + .999))),
            min_size=min_size,
            max_size=max_size,
            open=False,
            kwargs={"row_factory": dict_row},
        )

    def health_probe(self) -> bool:
        """Synchronously borrow a connection and execute a bounded read-only probe."""
        with self.pool.connection(timeout=self.connect_timeout) as connection:
            connection.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (f"{max(1, int(self.read_timeout * 1000))}ms",),
            )
            row = connection.execute("SELECT 1 AS probe_value").fetchone()
            return bool(row and row["probe_value"] == 1)

    def open(self) -> None:
        self.pool.open(wait=True)
        self.migrate()

    def close(self) -> None:
        self.pool.close()

    @contextmanager
    def connection(self) -> Iterator[Any]:
        with self.pool.connection() as connection:
            connection.execute(
                sql.SQL("SET LOCAL search_path TO {}, public").format(
                    sql.Identifier(self.schema)
                )
            )
            yield connection

    def migrate(self) -> None:
        with self.pool.connection() as connection:
            connection.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(self.schema)
                )
            )
            connection.execute(
                sql.SQL("SET LOCAL search_path TO {}, public").format(
                    sql.Identifier(self.schema)
                )
            )
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                ("marketcow:migrations:" + self.schema,),
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY, description TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            applied = {
                row["version"]
                for row in connection.execute(
                    "SELECT version FROM schema_migrations"
                ).fetchall()
            }
            for version, description, statement in POSTGRES_MIGRATIONS:
                if version in applied:
                    continue
                connection.execute(statement)
                connection.execute(
                    "INSERT INTO schema_migrations (version, description) VALUES (%s, %s)",
                    (version, description),
                )


class _PostgresControlPlaneRepository:
    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True)

    def save_run(self, row: Iterable[Any]) -> None:
        values = list(row)
        with self.database.connection() as connection:
            connection.execute(
                """
                INSERT INTO ingestion_runs
                    (run_id, job_name, status, report_period, started_at, finished_at, row_count, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    status = EXCLUDED.status, finished_at = EXCLUDED.finished_at,
                    row_count = EXCLUDED.row_count, error = EXCLUDED.error
                """,
                values,
            )

    def latest_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self.database.connection() as connection:
            return list(connection.execute(
                "SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT %s", (limit,)
            ).fetchall())

    def record_provider_health(
        self, provider: str, success: bool, attempted_at: str, error: str = ""
    ) -> None:
        with self.database.connection() as connection:
            connection.execute(
                """
                INSERT INTO provider_health
                    (provider, status, last_attempt_at, last_success_at, last_error, consecutive_failures)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (provider) DO UPDATE SET
                    status = EXCLUDED.status,
                    last_attempt_at = EXCLUDED.last_attempt_at,
                    last_success_at = CASE WHEN EXCLUDED.status = 'healthy'
                        THEN EXCLUDED.last_success_at ELSE provider_health.last_success_at END,
                    last_error = EXCLUDED.last_error,
                    consecutive_failures = CASE WHEN EXCLUDED.status = 'healthy' THEN 0
                        ELSE provider_health.consecutive_failures + 1 END
                """,
                (
                    provider, "healthy" if success else "unhealthy", attempted_at,
                    attempted_at if success else None, None if success else error,
                    0 if success else 1,
                ),
            )

    def provider_health(self) -> List[Dict[str, Any]]:
        with self.database.connection() as connection:
            return list(connection.execute(
                "SELECT * FROM provider_health ORDER BY provider"
            ).fetchall())

    def save_runtime_config_version(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Append an immutable, content-addressed V2 runtime configuration version."""
        config = row.get("config_json", row.get("config", {}))
        canonical = json.dumps(
            config, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        expected_sha256 = hashlib.sha256(canonical).hexdigest()
        if row.get("config_sha256") != expected_sha256:
            raise ValueError("runtime configuration content checksum mismatch")
        with self.database.connection() as connection:
            return connection.execute(
                """
                INSERT INTO runtime_config_version
                    (config_id, version, profile, schema_version, config_json,
                     config_sha256, observed_at, actor)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (config_id, config_sha256) DO UPDATE SET
                    config_sha256 = EXCLUDED.config_sha256
                RETURNING *
                """,
                (
                    row["config_id"], row["version"], row["profile"],
                    row["schema_version"], Jsonb(config), expected_sha256,
                    row["observed_at"], row["actor"],
                ),
            ).fetchone()

    def get_runtime_config_version(
        self, config_id: str, as_of: str = ""
    ) -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM runtime_config_version WHERE config_id = %s"
        params: List[Any] = [config_id]
        if as_of:
            query += " AND observed_at <= %s"
            params.append(as_of)
        query += " ORDER BY observed_at DESC, version DESC LIMIT 1"
        with self.database.connection() as connection:
            return connection.execute(query, params).fetchone()

    def upsert_migration_checkpoint(
        self, row: Dict[str, Any], expected_revision: int = 0
    ) -> Dict[str, Any]:
        """Create or compare-and-swap a durable migration checkpoint."""
        with self.database.connection() as connection:
            if expected_revision == 0:
                saved = connection.execute(
                    """
                    INSERT INTO migration_checkpoint
                        (run_id, domain, shard, revision, status, source_watermark,
                         target_watermark, cursor_json, evidence_json, error, updated_at)
                    VALUES (%s, %s, %s, 1, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, domain, shard) DO NOTHING
                    RETURNING *
                    """,
                    (
                        row["run_id"], row["domain"], row.get("shard", ""),
                        row["status"], row.get("source_watermark"),
                        row.get("target_watermark"), Jsonb(row.get("cursor_json", {})),
                        Jsonb(row.get("evidence_json", {})), row.get("error"),
                        row["updated_at"],
                    ),
                ).fetchone()
            else:
                saved = connection.execute(
                    """
                    UPDATE migration_checkpoint SET
                        revision = revision + 1, status = %s, source_watermark = %s,
                        target_watermark = %s, cursor_json = %s, evidence_json = %s,
                        error = %s, updated_at = %s
                    WHERE run_id = %s AND domain = %s AND shard = %s
                      AND revision = %s
                    RETURNING *
                    """,
                    (
                        row["status"], row.get("source_watermark"),
                        row.get("target_watermark"), Jsonb(row.get("cursor_json", {})),
                        Jsonb(row.get("evidence_json", {})), row.get("error"),
                        row["updated_at"], row["run_id"], row["domain"],
                        row.get("shard", ""), expected_revision,
                    ),
                ).fetchone()
            if saved is None:
                raise RuntimeError("migration checkpoint revision conflict")
            return saved

    def get_migration_checkpoint(
        self, run_id: str, domain: str, shard: str = ""
    ) -> Optional[Dict[str, Any]]:
        with self.database.connection() as connection:
            return connection.execute(
                "SELECT * FROM migration_checkpoint WHERE run_id = %s "
                "AND domain = %s AND shard = %s",
                (run_id, domain, shard),
            ).fetchone()

    def save_artifact(self, row: Dict[str, Any]) -> None:
        columns = [
            "artifact_id", "dataset", "source", "source_url", "observed_at",
            "ingested_at", "raw_response_locator", "storage_path", "sha256",
            "byte_size", "metadata_json",
        ]
        values = [row.get(column) for column in columns]
        values[-1] = self._json(row.get("metadata_json") if not isinstance(row.get("metadata_json"), str) else json.loads(row["metadata_json"]))
        with self.database.connection() as connection:
            connection.execute(
                """
                INSERT INTO raw_artifact_manifest
                    (artifact_id, dataset, source, source_url, observed_at, ingested_at,
                     raw_response_locator, storage_path, sha256, byte_size, metadata_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (artifact_id) DO NOTHING
                """,
                values,
            )

    def save_artifacts(self, rows: List[Dict[str, Any]]) -> int:
        for row in rows:
            self.save_artifact(row)
        return len(rows)

    def artifact_paths(self) -> set[str]:
        with self.database.connection() as connection:
            return {row["storage_path"] for row in connection.execute(
                "SELECT storage_path FROM raw_artifact_manifest"
            ).fetchall()}

    def list_artifacts(self, dataset: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        query = "SELECT * FROM raw_artifact_manifest"
        params: List[Any] = []
        if dataset:
            query += " WHERE dataset = %s"
            params.append(dataset)
        query += " ORDER BY ingested_at DESC LIMIT %s"
        params.append(limit)
        with self.database.connection() as connection:
            return list(connection.execute(query, params).fetchall())


class PostgresRepository(_PostgresControlPlaneRepository):
    """Core PostgreSQL fundamentals with immutable point-in-time history."""

    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    def replace_fundamentals(self, report_period: str, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            with self.database.connection() as connection:
                connection.execute(
                    "DELETE FROM fundamental_snapshot WHERE report_period = %s",
                    (report_period,),
                )
            return 0
        columns = list(FUNDAMENTAL_COLUMNS)
        insert = sql.SQL("INSERT INTO fundamental_snapshot ({}) VALUES ({})").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )
        history_columns = columns + ["version_id"]
        insert_history = sql.SQL(
            "INSERT INTO fundamental_snapshot_history ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, history_columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in history_columns),
        )
        with self.database.connection() as connection:
            connection.execute(
                "DELETE FROM fundamental_snapshot WHERE report_period = %s",
                (report_period,),
            )
            for row in rows:
                values = [row.get(column) for column in columns]
                connection.execute(insert, values)
                connection.execute(insert_history, values + [uuid.uuid4().hex])
        return len(rows)

    def query_fundamentals(
        self,
        limit: int = 100,
        offset: int = 0,
        symbol: str = "",
        report_period: str = "",
        industry: str = "",
        min_roe: Optional[float] = None,
        max_pe: Optional[float] = None,
        active_only: bool = True,
        as_of: str = "",
    ) -> List[Dict[str, Any]]:
        where: List[str] = []
        params: List[Any] = []
        table = "fundamental_snapshot"
        if as_of:
            table = "fundamental_snapshot_history"
            where.extend([
                "published_at IS NOT NULL AND published_at <= %s",
                "CAST(COALESCE(observed_at, ingested_at, fetched_at) AS DATE) <= CAST(%s AS DATE)",
            ])
            params.extend([as_of, as_of])
        if active_only:
            where.append("is_active IS TRUE")
        if symbol:
            where.append("symbol = %s")
            params.append(symbol)
        if report_period:
            where.append("report_period = %s")
            params.append(report_period)
        elif as_of:
            where.append(
                "report_period = (SELECT MAX(fs2.report_period) "
                "FROM fundamental_snapshot_history fs2 "
                f"WHERE fs2.symbol = {table}.symbol AND fs2.published_at IS NOT NULL "
                "AND fs2.published_at <= %s "
                "AND CAST(COALESCE(fs2.observed_at, fs2.ingested_at, fs2.fetched_at) AS DATE) "
                "<= CAST(%s AS DATE))"
            )
            params.extend([as_of, as_of])
        elif symbol:
            where.append(
                "report_period = (SELECT MAX(fs2.report_period) FROM fundamental_snapshot fs2 "
                "WHERE fs2.symbol = fundamental_snapshot.symbol)"
            )
        else:
            where.append("report_period = (SELECT MAX(report_period) FROM fundamental_snapshot)")
        if industry:
            where.append("industry = %s")
            params.append(industry)
        if min_roe is not None:
            where.append("roe_weighted >= %s")
            params.append(min_roe)
        if max_pe is not None:
            where.append("pe_dynamic > 0 AND pe_dynamic <= %s")
            params.append(max_pe)
        selected = ", ".join(FUNDAMENTAL_COLUMNS)
        if as_of:
            query = (
                f"SELECT {selected} FROM (SELECT *, ROW_NUMBER() OVER ("
                "PARTITION BY symbol, report_period ORDER BY "
                "COALESCE(ingested_at, fetched_at) DESC, version_id DESC) AS revision_rank "
                f"FROM {table} WHERE {' AND '.join(where)}) revisions "
                "WHERE revision_rank = 1 ORDER BY symbol LIMIT %s OFFSET %s"
            )
        else:
            query = f"SELECT {selected} FROM {table}"
            if where:
                query += " WHERE " + " AND ".join(where)
            query += " ORDER BY symbol LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.database.connection() as connection:
            return list(connection.execute(query, params).fetchall())

    def replace_statement_rows(
        self, symbol: str, statement: str, rows: List[Dict[str, Any]]
    ) -> int:
        columns = [
            "instrument_id", "symbol", "statement", "report_date", "published_at",
            "source", "payload_json", "fetched_at", "source_url", "observed_at",
            "ingested_at", "raw_response_locator", "raw_path", "raw_artifact_id",
        ]
        statement_sql = sql.SQL(
            "INSERT INTO financial_statement_rows ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )
        with self.database.connection() as connection:
            connection.execute(
                "DELETE FROM financial_statement_rows WHERE symbol = %s AND statement = %s",
                (symbol, statement),
            )
            for row in rows:
                values = [
                    Jsonb(row.get("payload", {})) if column == "payload_json"
                    else row.get(column)
                    for column in columns
                ]
                connection.execute(statement_sql, values)
        return len(rows)

    def get_statement_rows(
        self, symbol: str, statement: str = "", limit_periods: int = 20,
        as_of: str = "",
    ) -> List[Dict[str, Any]]:
        where, params = ["symbol = %s"], [symbol]
        if statement:
            where.append("statement = %s")
            params.append(statement)
        if as_of:
            where.extend([
                "published_at IS NOT NULL AND published_at <= %s",
                "CAST(COALESCE(observed_at, ingested_at, fetched_at) AS DATE) <= CAST(%s AS DATE)",
            ])
            params.extend([as_of, as_of])
        params.append(limit_periods)
        with self.database.connection() as connection:
            rows = list(connection.execute(
                "SELECT * FROM financial_statement_rows WHERE " + " AND ".join(where)
                + " ORDER BY report_date DESC, statement LIMIT %s",
                params,
            ).fetchall())
        for row in rows:
            row["payload"] = row.pop("payload_json")
        return rows

    def upsert_baostock(self, row: Dict[str, Any]) -> None:
        columns = [
            "symbol", "report_period", "published_at", "trade_date", "close",
            "pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm", "trade_status", "is_st",
            "roe_avg", "net_margin", "gross_margin", "net_profit_all", "eps_ttm",
            "total_share", "current_ratio", "quick_ratio", "liability_to_asset",
            "asset_turnover", "inventory_turnover", "net_profit_yoy", "equity_yoy",
            "asset_yoy", "cfo_to_revenue", "cfo_to_net_profit", "dupont_roe",
            "payload_json", "fetched_at", "source", "source_url", "observed_at",
            "ingested_at", "raw_response_locator", "raw_path", "raw_artifact_id",
        ]
        assignments = [column for column in columns if column not in {"symbol", "report_period"}]
        statement = sql.SQL(
            "INSERT INTO baostock_snapshot ({columns}) VALUES ({values}) "
            "ON CONFLICT (symbol, report_period) DO UPDATE SET {assignments}"
        ).format(
            columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
            values=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            assignments=sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(column), sql.Identifier(column))
                for column in assignments
            ),
        )
        values = [
            Jsonb(row.get("payload", row.get("payload_json", {})))
            if column == "payload_json" else row.get(column)
            for column in columns
        ]
        with self.database.connection() as connection:
            connection.execute(statement, values)

    def get_baostock(self, symbol: str, report_period: str) -> Optional[Dict[str, Any]]:
        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT * FROM baostock_snapshot WHERE symbol = %s AND report_period = %s",
                (symbol, report_period),
            ).fetchone()
        if row is not None:
            row["payload"] = row.pop("payload_json")
        return row

    def replace_tdx_period(self, report_period: str, rows: List[Dict[str, Any]]) -> int:
        columns = list(TDX_COLUMNS)
        insert = sql.SQL("INSERT INTO tdx_financial_snapshot ({}) VALUES ({})").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )
        history_columns = columns + ["version_id"]
        insert_history = sql.SQL(
            "INSERT INTO tdx_financial_snapshot_history ({}) VALUES ({})"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, history_columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in history_columns),
        )
        with self.database.connection() as connection:
            connection.execute(
                "DELETE FROM tdx_financial_snapshot WHERE report_period = %s",
                (report_period,),
            )
            for row in rows:
                values = [row.get(column) for column in columns]
                connection.execute(insert, values)
                connection.execute(insert_history, values + [uuid.uuid4().hex])
        return len(rows)

    def get_tdx(self, symbol: str, report_period: str) -> Optional[Dict[str, Any]]:
        with self.database.connection() as connection:
            return connection.execute(
                "SELECT * FROM tdx_financial_snapshot WHERE symbol = %s AND report_period = %s",
                (symbol, report_period),
            ).fetchone()

    def tdx_coverage(self) -> List[Dict[str, Any]]:
        with self.database.connection() as connection:
            return list(connection.execute(
                """
                SELECT report_period, COUNT(*) AS row_count,
                       MIN(published_at) AS min_published_at,
                       MAX(published_at) AS max_published_at, MAX(fetched_at) AS fetched_at
                FROM tdx_financial_snapshot
                GROUP BY report_period ORDER BY report_period DESC
                """
            ).fetchall())

    def get_tdx_history(
        self, symbol: str, annual_only: bool = False, limit: int = 40,
        as_of: str = "",
    ) -> List[Dict[str, Any]]:
        where, params = ["symbol = %s"], [symbol]
        if annual_only:
            where.append("RIGHT(report_period, 4) = '1231'")
        table = "tdx_financial_snapshot"
        if as_of:
            table = "tdx_financial_snapshot_history"
            where.extend([
                "published_at IS NOT NULL AND published_at <= %s",
                "CAST(COALESCE(observed_at, ingested_at, fetched_at) AS DATE) <= CAST(%s AS DATE)",
            ])
            params.extend([as_of, as_of])
        params.append(limit)
        selected = ", ".join(TDX_COLUMNS)
        if as_of:
            query = (
                f"SELECT {selected} FROM (SELECT *, ROW_NUMBER() OVER ("
                "PARTITION BY symbol, report_period ORDER BY "
                "COALESCE(ingested_at, fetched_at) DESC, version_id DESC) AS revision_rank "
                f"FROM {table} WHERE {' AND '.join(where)}) revisions "
                "WHERE revision_rank = 1 ORDER BY report_period DESC LIMIT %s"
            )
        else:
            query = (
                f"SELECT {selected} FROM {table} WHERE {' AND '.join(where)} "
                "ORDER BY report_period DESC LIMIT %s"
            )
        with self.database.connection() as connection:
            return list(connection.execute(query, params).fetchall())

    def save_validation_results(self, rows: List[Dict[str, Any]]) -> int:
        columns = [
            "symbol", "report_period", "metric", "source_a", "source_b",
            "value_a", "value_b", "difference_pct", "status", "observed_at",
        ]
        statement = sql.SQL(
            "INSERT INTO validation_result ({}) VALUES ({}) "
            "ON CONFLICT (symbol, report_period, metric, source_a, source_b) "
            "DO UPDATE SET value_a = EXCLUDED.value_a, value_b = EXCLUDED.value_b, "
            "difference_pct = EXCLUDED.difference_pct, status = EXCLUDED.status, "
            "observed_at = EXCLUDED.observed_at"
        ).format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )
        with self.database.connection() as connection:
            for row in rows:
                connection.execute(statement, [row.get(column) for column in columns])
        return len(rows)

    def get_validation_results(
        self, symbol: str, report_period: str
    ) -> List[Dict[str, Any]]:
        with self.database.connection() as connection:
            return list(connection.execute(
                "SELECT * FROM validation_result WHERE symbol = %s "
                "AND report_period = %s ORDER BY metric, source_a, source_b",
                (symbol, report_period),
            ).fetchall())

    def rebuild_validation_results(self, report_period: str, observed_at: str) -> int:
        pairs = [
            ("roe_eastmoney_vs_baostock", "eastmoney", "baostock", "f.roe_weighted", "b.roe_avg"),
            ("roe_eastmoney_vs_tdx", "eastmoney", "tdx", "f.roe_weighted", "t.roe_weighted"),
            ("revenue_eastmoney_vs_tdx", "eastmoney", "tdx", "f.revenue", "t.revenue"),
            ("net_profit_eastmoney_vs_tdx", "eastmoney", "tdx", "f.net_profit", "t.net_profit_parent"),
            ("assets_eastmoney_vs_tdx", "eastmoney", "tdx", "f.total_assets", "t.total_assets"),
            ("liabilities_eastmoney_vs_tdx", "eastmoney", "tdx", "f.total_liabilities", "t.total_liabilities"),
            ("ocf_eastmoney_vs_tdx", "eastmoney", "tdx", "f.operating_cashflow", "t.operating_cashflow"),
        ]
        selects, params = [], []
        for metric, source_a, source_b, value_a, value_b in pairs:
            selects.append(
                "SELECT f.symbol, f.report_period, %s metric, %s source_a, %s source_b, "
                f"{value_a} value_a, {value_b} value_b FROM fundamental_snapshot f "
                "LEFT JOIN tdx_financial_snapshot t USING (symbol, report_period) "
                "LEFT JOIN baostock_snapshot b USING (symbol, report_period) "
                "WHERE f.report_period = %s"
            )
            params.extend([metric, source_a, source_b, report_period])
        params.append(observed_at)
        with self.database.connection() as connection:
            connection.execute(
                """
                INSERT INTO validation_result
                    (symbol, report_period, metric, source_a, source_b, value_a,
                     value_b, difference_pct, status, observed_at)
                WITH pairs AS (""" + " UNION ALL ".join(selects) + """), scored AS (
                    SELECT *, CASE WHEN value_a IS NULL OR value_b IS NULL OR value_a = 0
                        THEN NULL ELSE ABS(value_a-value_b)/ABS(value_a)*100 END difference_pct
                    FROM pairs
                ) SELECT symbol, report_period, metric, source_a, source_b, value_a,
                    value_b, ROUND(difference_pct::numeric, 4)::double precision,
                    CASE WHEN difference_pct IS NULL THEN 'missing_comparison'
                         WHEN difference_pct <= 1 THEN 'consistent'
                         ELSE 'difference_over_1pct' END, %s FROM scored
                ON CONFLICT (symbol, report_period, metric, source_a, source_b)
                DO UPDATE SET value_a=EXCLUDED.value_a, value_b=EXCLUDED.value_b,
                    difference_pct=EXCLUDED.difference_pct, status=EXCLUDED.status,
                    observed_at=EXCLUDED.observed_at
                """,
                params,
            )
            return connection.execute(
                "SELECT COUNT(*) count FROM validation_result WHERE report_period = %s",
                (report_period,),
            ).fetchone()["count"]

    @staticmethod
    def _funnel_select(source_ctes: str, rebuilt_expression: str) -> str:
        return source_ctes + """
        , annual AS (SELECT * FROM t_versions WHERE RIGHT(report_period, 4)='1231'),
        latest_annual AS (SELECT DISTINCT ON (symbol) * FROM annual ORDER BY symbol, report_period DESC),
        annual_agg AS (
            SELECT symbol,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY roe_weighted) roe_annual_median,
              MIN(roe_weighted) roe_annual_min, COUNT(*) annual_period_count,
              MIN(report_period) history_start_period, MAX(report_period) history_end_period,
              (ARRAY_AGG(revenue_ttm ORDER BY report_period)
                FILTER (WHERE revenue_ttm IS NOT NULL))[1] revenue_first,
              (ARRAY_AGG(revenue_ttm ORDER BY report_period DESC)
                FILTER (WHERE revenue_ttm IS NOT NULL))[1] revenue_last,
              (ARRAY_AGG(net_profit_parent_ttm ORDER BY report_period)
                FILTER (WHERE net_profit_parent_ttm IS NOT NULL))[1] profit_first,
              (ARRAY_AGG(net_profit_parent_ttm ORDER BY report_period DESC)
                FILTER (WHERE net_profit_parent_ttm IS NOT NULL))[1] profit_last,
              SUM(operating_cashflow) operating_cashflow_sum,
              SUM(COALESCE(net_profit_parent,net_profit_parent_ttm)) net_profit_parent_sum,
              LEFT(MAX(report_period),4)::integer-LEFT(MIN(report_period),4)::integer year_span
            FROM annual GROUP BY symbol
        ), validation_agg AS (
            SELECT symbol, report_period,
              COUNT(*) FILTER (WHERE status='consistent') consistent_count,
              COUNT(*) FILTER (WHERE status='difference_over_1pct') difference_count
            FROM validation_versions GROUP BY symbol, report_period
        ) SELECT f.symbol,f.name,f.is_active,t.report_period latest_report_period,t.published_at,
          b.pe_ttm,f.pe_dynamic,COALESCE(b.pe_ttm,f.pe_dynamic) pe_for_filter,
          CASE WHEN b.pe_ttm IS NOT NULL THEN 'baostock_pe_ttm' ELSE 'eastmoney_pe_dynamic' END pe_source,
          COALESCE(b.pb_mrq,f.pb) pb,
          CASE WHEN b.pb_mrq IS NOT NULL THEN 'baostock_pb_mrq' ELSE 'eastmoney_pb' END pb_source,
          t.roe_weighted roe_latest,a.roe_annual_median,a.roe_annual_min,
          COALESCE(a.annual_period_count,0) annual_period_count,t.revenue_ttm,t.net_profit_parent_ttm,
          CASE WHEN a.year_span>0 AND a.revenue_first>0 AND a.revenue_last>0
            THEN (POWER(a.revenue_last/a.revenue_first,1.0/a.year_span)-1)*100 END revenue_cagr_pct,
          CASE WHEN a.year_span>0 AND a.profit_first>0 AND a.profit_last>0
            THEN (POWER(a.profit_last/a.profit_first,1.0/a.year_span)-1)*100 END net_profit_cagr_pct,
          CASE WHEN t.total_assets>0 THEN t.total_liabilities/t.total_assets*100 END debt_ratio_latest,
          CASE WHEN t.total_assets>0 THEN t.cash/t.total_assets*100 END cash_to_assets_pct,
          t.operating_cashflow-t.capex fcf_latest_period,la.report_period latest_annual_period,
          la.operating_cashflow operating_cashflow_latest_annual,
          la.operating_cashflow-la.capex fcf_latest_annual,
          CASE WHEN COALESCE(la.net_profit_parent,la.net_profit_parent_ttm)!=0
            THEN la.operating_cashflow/COALESCE(la.net_profit_parent,la.net_profit_parent_ttm)*100 END ocf_to_net_profit_latest_annual_pct,
          CASE WHEN a.net_profit_parent_sum!=0 THEN a.operating_cashflow_sum/a.net_profit_parent_sum*100 END ocf_to_net_profit_annual_sum_pct,
          a.history_start_period,a.history_end_period,
          CASE WHEN t.symbol IS NULL THEN 'missing_tdx_latest'
            WHEN COALESCE(v.difference_count,0)>0 THEN 'cross_source_difference_over_1pct'
            WHEN COALESCE(a.annual_period_count,0)<3 THEN 'insufficient_annual_history'
            WHEN b.symbol IS NULL THEN 'history_ready_valuation_single_source'
            WHEN COALESCE(v.consistent_count,0)>0 THEN 'multi_source_verified'
            ELSE 'multi_source_pending_validation' END quality_status,
          """ + rebuilt_expression + """ rebuilt_at
        FROM current_f f LEFT JOIN latest_tdx t USING(symbol)
        LEFT JOIN annual_agg a USING(symbol) LEFT JOIN latest_annual la USING(symbol)
        LEFT JOIN latest_bao b USING(symbol)
        LEFT JOIN validation_agg v ON v.symbol=t.symbol AND v.report_period=t.report_period
        """

    def rebuild_funnel_metrics(self, rebuilt_at: str) -> int:
        sources = """
        WITH current_f AS (SELECT DISTINCT ON (symbol) * FROM fundamental_snapshot ORDER BY symbol,report_period DESC),
        t_versions AS (SELECT * FROM tdx_financial_snapshot),
        latest_tdx AS (SELECT DISTINCT ON (symbol) * FROM t_versions ORDER BY symbol,report_period DESC),
        latest_bao AS (SELECT DISTINCT ON (symbol) * FROM baostock_snapshot ORDER BY symbol,trade_date DESC,report_period DESC),
        validation_versions AS (SELECT * FROM validation_result)
        """
        select = self._funnel_select(sources, "%s")
        with self.database.connection() as connection:
            connection.execute("DELETE FROM funnel_metrics")
            connection.execute("INSERT INTO funnel_metrics " + select, (rebuilt_at,))
            return connection.execute("SELECT COUNT(*) count FROM funnel_metrics").fetchone()["count"]

    def query_funnel_metrics(
        self, limit: int = 100, offset: int = 0,
        min_roe_median: Optional[float] = None,
        min_revenue_cagr: Optional[float] = None,
        min_profit_cagr: Optional[float] = None, max_pe: Optional[float] = None,
        max_debt_ratio: Optional[float] = None, min_annual_periods: int = 0,
        active_only: bool = True, as_of: str = "",
    ) -> List[Dict[str, Any]]:
        params: List[Any] = []
        query = "SELECT * FROM funnel_metrics"
        if as_of:
            sources = """
            WITH cutoff AS (SELECT %s::date as_of),
            f_ranked AS (SELECT f.*,ROW_NUMBER() OVER(PARTITION BY symbol,report_period ORDER BY COALESCE(ingested_at,fetched_at) DESC,version_id DESC) revision_rank FROM fundamental_snapshot_history f,cutoff c WHERE published_at IS NOT NULL AND published_at::date<=c.as_of AND COALESCE(observed_at,ingested_at,fetched_at)::date<=c.as_of),
            f_versions AS (SELECT * FROM f_ranked WHERE revision_rank=1),
            current_f AS (SELECT DISTINCT ON(symbol) * FROM f_versions ORDER BY symbol,report_period DESC),
            t_ranked AS (SELECT t.*,ROW_NUMBER() OVER(PARTITION BY symbol,report_period ORDER BY COALESCE(ingested_at,fetched_at) DESC,version_id DESC) revision_rank FROM tdx_financial_snapshot_history t,cutoff c WHERE published_at IS NOT NULL AND published_at::date<=c.as_of AND COALESCE(observed_at,ingested_at,fetched_at)::date<=c.as_of),
            t_versions AS (SELECT * FROM t_ranked WHERE revision_rank=1),
            latest_tdx AS (SELECT DISTINCT ON(symbol) * FROM t_versions ORDER BY symbol,report_period DESC),
            latest_bao AS (SELECT DISTINCT ON(symbol) b.* FROM baostock_snapshot b,cutoff c WHERE COALESCE(observed_at,ingested_at,fetched_at)::date<=c.as_of ORDER BY symbol,trade_date DESC,report_period DESC),
            validation_versions AS (SELECT v.* FROM validation_result v,cutoff c WHERE observed_at::date<=c.as_of)
            """
            query = self._funnel_select(sources, "%s")
            params.extend([as_of, as_of])
        where = []
        if active_only:
            where.append("is_active IS TRUE")
        for value, clause in [
            (min_roe_median, "roe_annual_median >= %s"),
            (min_revenue_cagr, "revenue_cagr_pct >= %s"),
            (min_profit_cagr, "net_profit_cagr_pct >= %s"),
            (max_pe, "pe_for_filter > 0 AND pe_for_filter <= %s"),
            (max_debt_ratio, "debt_ratio_latest <= %s"),
        ]:
            if value is not None:
                where.append(clause)
                params.append(value)
        if min_annual_periods:
            where.append("annual_period_count >= %s")
            params.append(min_annual_periods)
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY roe_annual_median DESC NULLS LAST,symbol LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        with self.database.connection() as connection:
            return list(connection.execute(query, params).fetchall())

    def latest_artifact(
        self, dataset: str, metadata_key: str = "", metadata_value: str = ""
    ) -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM raw_artifact_manifest WHERE dataset = %s"
        params: List[Any] = [dataset]
        if metadata_key:
            query += " AND metadata_json ->> %s = %s"
            params.extend([metadata_key, metadata_value])
        query += " ORDER BY ingested_at DESC LIMIT 1"
        with self.database.connection() as connection:
            return connection.execute(query, params).fetchone()

    def save_tushare_response(
        self, request: Dict[str, Any], rows: List[Dict[str, Any]]
    ) -> int:
        with self.database.connection() as connection:
            connection.execute(
                """
                INSERT INTO tushare_request
                    (request_id, api_name, params_json, requested_fields,
                     response_fields_json, response_code, response_message, row_count,
                     source, source_url, observed_at, ingested_at, raw_path, raw_artifact_id)
                VALUES (%s, %s, %s::jsonb, %s, %s::jsonb, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s)
                ON CONFLICT (request_id) DO NOTHING
                """,
                (
                    request["request_id"], request["api_name"], self._json(request.get("params")),
                    request.get("requested_fields"), self._json(request.get("response_fields", [])),
                    request.get("response_code"), request.get("response_message"), len(rows),
                    request.get("source"), request.get("source_url"), request.get("observed_at"),
                    request.get("ingested_at"), request.get("raw_path"), request.get("raw_artifact_id"),
                ),
            )
            for index, row in enumerate(rows):
                connection.execute(
                    """
                    INSERT INTO tushare_data_row
                        (request_id, row_index, api_name, symbol, data_date, source,
                         source_url, observed_at, ingested_at, payload_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (request_id, row_index) DO NOTHING
                    """,
                    (
                        request["request_id"], index, request["api_name"],
                        row.get("ts_code") or row.get("symbol"),
                        row.get("trade_date") or row.get("ann_date") or row.get("date"),
                        request.get("source"), request.get("source_url"),
                        request.get("observed_at"), request.get("ingested_at"), self._json(row),
                    ),
                )
        return len(rows)

    def get_tushare_response(self, request_id: str) -> Optional[Dict[str, Any]]:
        with self.database.connection() as connection:
            request = connection.execute(
                "SELECT * FROM tushare_request WHERE request_id = %s", (request_id,)
            ).fetchone()
            if request is None:
                return None
            rows = list(connection.execute(
                "SELECT * FROM tushare_data_row WHERE request_id = %s ORDER BY row_index",
                (request_id,),
            ).fetchall())
            return {**request, "rows": rows}

    def _upsert_rows(
        self, table: str, key: str, columns: Sequence[str], rows: List[Dict[str, Any]]
    ) -> int:
        if not rows:
            return 0
        assignments = [column for column in columns if column != key]
        statement = sql.SQL(
            "INSERT INTO {table} ({columns}) VALUES ({values}) "
            "ON CONFLICT ({key}) DO UPDATE SET {assignments}"
        ).format(
            table=sql.Identifier(table),
            columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
            values=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            key=sql.Identifier(key),
            assignments=sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(column), sql.Identifier(column))
                for column in assignments
            ),
        )
        with self.database.connection() as connection:
            for row in rows:
                values = []
                for column in columns:
                    value = row.get(column)
                    if column == "payload_json":
                        value = Jsonb(value if value is not None else row.get("payload", {}))
                    values.append(value)
                connection.execute(statement, values)
        return len(rows)

    def upsert_economic_calendar(self, rows: List[Dict[str, Any]]) -> int:
        columns = [
            "event_id", "country", "event_date", "event_time", "timezone", "scheduled_at",
            "event_name", "impact", "actual", "estimate", "previous", "unit", "source",
            "source_url", "observed_at", "ingested_at", "raw_response_locator", "raw_path",
            "raw_artifact_id", "payload_json",
        ]
        return self._upsert_rows("economic_calendar_event", "event_id", columns, rows)

    def get_economic_calendar(
        self, date_from: str, date_to: str, country: str = "US",
        impact: str = "", limit: int = 50,
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM economic_calendar_event WHERE event_date BETWEEN %s AND %s"
        params: List[Any] = [date_from, date_to]
        if country:
            query += " AND country = %s"
            params.append(country)
        if impact:
            query += " AND impact = %s"
            params.append(impact)
        query += " ORDER BY event_date, event_time LIMIT %s"
        params.append(limit)
        with self.database.connection() as connection:
            return list(connection.execute(query, params).fetchall())

    def upsert_economic_indicators(self, rows: List[Dict[str, Any]]) -> int:
        columns = [
            "indicator_id", "country", "name", "source", "source_series_id", "period",
            "value", "previous_value", "change_value", "change_pct", "unit", "frequency",
            "latest_date", "source_url", "observed_at", "ingested_at",
            "raw_response_locator", "raw_path", "raw_artifact_id", "payload_json",
        ]
        return self._upsert_rows("economic_indicator_latest", "indicator_id", columns, rows)

    def get_economic_indicators(
        self, country: str = "US", source: str = "", limit: int = 50
    ) -> List[Dict[str, Any]]:
        query, params = "SELECT * FROM economic_indicator_latest WHERE 1=1", []
        if country:
            query += " AND country = %s"
            params.append(country)
        if source:
            query += " AND source = %s"
            params.append(source)
        query += " ORDER BY latest_date DESC, name LIMIT %s"
        params.append(limit)
        with self.database.connection() as connection:
            return list(connection.execute(query, params).fetchall())

    def upsert_earnings_calendar(self, rows: List[Dict[str, Any]]) -> int:
        columns = [
            "event_id", "market", "symbol", "name", "report_date", "report_time", "timezone",
            "scheduled_at", "fiscal_period", "eps_forecast", "previous_eps", "source",
            "source_url", "observed_at", "ingested_at", "raw_response_locator", "raw_path",
            "raw_artifact_id", "payload_json",
        ]
        return self._upsert_rows("earnings_calendar_event", "event_id", columns, rows)

    def get_earnings_calendar(
        self, date_from: str, date_to: str, market: str = "",
        symbols: Optional[Sequence[str]] = None, limit: int = 50,
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM earnings_calendar_event WHERE report_date BETWEEN %s AND %s"
        params: List[Any] = [date_from, date_to]
        if market:
            query += " AND market = %s"
            params.append(market)
        if symbols:
            query += " AND symbol = ANY(%s)"
            params.append(list(symbols))
        query += " ORDER BY report_date, symbol LIMIT %s"
        params.append(limit)
        with self.database.connection() as connection:
            return list(connection.execute(query, params).fetchall())


PostgresMetadataRepository = PostgresRepository
PostgresFundamentalRepository = PostgresRepository
