from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .postgres_migrations import POSTGRES_MIGRATIONS
from .storage import FUNDAMENTAL_COLUMNS, TDX_COLUMNS


class PostgresDatabase:
    """Development-only PostgreSQL connection and schema migration boundary."""

    def __init__(
        self, dsn: str, schema: str, min_size: int = 1, max_size: int = 4
    ) -> None:
        self.schema = schema
        self.pool = ConnectionPool(
            dsn,
            min_size=min_size,
            max_size=max_size,
            open=False,
            kwargs={"row_factory": dict_row},
        )

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
