from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import duckdb
import pandas as pd


FUNDAMENTAL_COLUMNS = [
    "instrument_id", "symbol", "exchange", "name", "is_active", "report_period",
    "published_at", "valuation_as_of", "price", "change_pct", "pe_dynamic",
    "pb", "total_market_cap", "float_market_cap", "roe_weighted", "eps",
    "revenue", "revenue_yoy", "revenue_qoq", "net_profit", "net_profit_yoy",
    "net_profit_qoq", "book_value_per_share", "ocf_per_share", "gross_margin",
    "industry", "cash", "accounts_receivable", "inventory", "total_assets",
    "total_assets_yoy", "accounts_payable", "advance_receipts",
    "total_liabilities", "total_liabilities_yoy", "debt_ratio", "total_equity",
    "operating_cost", "sales_expense", "admin_expense", "financial_expense",
    "total_operating_expense", "operating_profit", "total_profit", "net_cashflow",
    "net_cashflow_yoy", "operating_cashflow", "investing_cashflow",
    "financing_cashflow", "source", "source_url", "observed_at", "ingested_at",
    "raw_response_locator", "raw_path", "raw_artifact_id", "quality_status", "fetched_at",
]

PROVENANCE_COLUMNS = [
    "source", "source_url", "observed_at", "ingested_at",
    "raw_response_locator", "raw_path", "raw_artifact_id",
]

TDX_COLUMNS = [
    "symbol", "report_period", "published_at", "roe_weighted", "eps",
    "eps_adjusted", "book_value_per_share", "ocf_per_share", "cash",
    "accounts_receivable", "inventory", "total_assets", "total_liabilities",
    "total_equity", "revenue", "revenue_ttm", "net_profit_parent",
    "net_profit_parent_ttm", "operating_cashflow", "capex", "source_file",
    "source", "source_url", "observed_at", "ingested_at", "raw_response_locator",
    "raw_path", "raw_artifact_id", "fetched_at",
]


class Warehouse:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.init_schema()

    def connect(self):
        return duckdb.connect(str(self.path))

    def init_schema(self) -> None:
        with self._lock, self.connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS fundamental_snapshot (
                    instrument_id VARCHAR, symbol VARCHAR, exchange VARCHAR, name VARCHAR,
                    is_active BOOLEAN,
                    report_period VARCHAR, published_at VARCHAR, valuation_as_of VARCHAR,
                    price DOUBLE, change_pct DOUBLE, pe_dynamic DOUBLE, pb DOUBLE,
                    total_market_cap DOUBLE, float_market_cap DOUBLE, roe_weighted DOUBLE,
                    eps DOUBLE, revenue DOUBLE, revenue_yoy DOUBLE, revenue_qoq DOUBLE,
                    net_profit DOUBLE, net_profit_yoy DOUBLE, net_profit_qoq DOUBLE,
                    book_value_per_share DOUBLE, ocf_per_share DOUBLE, gross_margin DOUBLE,
                    industry VARCHAR, cash DOUBLE, accounts_receivable DOUBLE, inventory DOUBLE,
                    total_assets DOUBLE, total_assets_yoy DOUBLE, accounts_payable DOUBLE,
                    advance_receipts DOUBLE, total_liabilities DOUBLE,
                    total_liabilities_yoy DOUBLE, debt_ratio DOUBLE, total_equity DOUBLE,
                    operating_cost DOUBLE, sales_expense DOUBLE, admin_expense DOUBLE,
                    financial_expense DOUBLE, total_operating_expense DOUBLE,
                    operating_profit DOUBLE, total_profit DOUBLE, net_cashflow DOUBLE,
                    net_cashflow_yoy DOUBLE, operating_cashflow DOUBLE,
                    investing_cashflow DOUBLE, financing_cashflow DOUBLE, source VARCHAR,
                    quality_status VARCHAR, fetched_at VARCHAR,
                    PRIMARY KEY (symbol, report_period)
                )
                """
            )
            con.execute(
                "ALTER TABLE fundamental_snapshot ADD COLUMN IF NOT EXISTS is_active BOOLEAN"
            )
            for name in ("source_url", "observed_at", "ingested_at", "raw_response_locator", "raw_path", "raw_artifact_id"):
                con.execute("ALTER TABLE fundamental_snapshot ADD COLUMN IF NOT EXISTS {0} VARCHAR".format(name))
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS financial_statement_rows (
                    instrument_id VARCHAR, symbol VARCHAR, statement VARCHAR,
                    report_date VARCHAR, published_at VARCHAR, source VARCHAR,
                    payload_json VARCHAR, fetched_at VARCHAR,
                    PRIMARY KEY (symbol, statement, report_date, source)
                )
                """
            )
            for name in ("source_url", "observed_at", "ingested_at", "raw_response_locator", "raw_path", "raw_artifact_id"):
                con.execute("ALTER TABLE financial_statement_rows ADD COLUMN IF NOT EXISTS {0} VARCHAR".format(name))
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS ingestion_runs (
                    run_id VARCHAR PRIMARY KEY, job_name VARCHAR, status VARCHAR,
                    report_period VARCHAR, started_at VARCHAR, finished_at VARCHAR,
                    row_count BIGINT, error VARCHAR
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS baostock_snapshot (
                    symbol VARCHAR, report_period VARCHAR, published_at VARCHAR,
                    trade_date VARCHAR, close DOUBLE, pe_ttm DOUBLE, pb_mrq DOUBLE,
                    ps_ttm DOUBLE, pcf_ncf_ttm DOUBLE, trade_status INTEGER, is_st BOOLEAN,
                    roe_avg DOUBLE, net_margin DOUBLE, gross_margin DOUBLE,
                    net_profit_all DOUBLE, eps_ttm DOUBLE, total_share DOUBLE,
                    current_ratio DOUBLE, quick_ratio DOUBLE, liability_to_asset DOUBLE,
                    asset_turnover DOUBLE, inventory_turnover DOUBLE, net_profit_yoy DOUBLE,
                    equity_yoy DOUBLE, asset_yoy DOUBLE, cfo_to_revenue DOUBLE,
                    cfo_to_net_profit DOUBLE, dupont_roe DOUBLE, payload_json VARCHAR,
                    fetched_at VARCHAR, PRIMARY KEY (symbol, report_period)
                )
                """
            )
            for name in ("source", "source_url", "observed_at", "ingested_at", "raw_response_locator", "raw_path", "raw_artifact_id"):
                con.execute("ALTER TABLE baostock_snapshot ADD COLUMN IF NOT EXISTS {0} VARCHAR".format(name))
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS tdx_financial_snapshot (
                    symbol VARCHAR, report_period VARCHAR, published_at VARCHAR,
                    roe_weighted DOUBLE, eps DOUBLE, eps_adjusted DOUBLE,
                    book_value_per_share DOUBLE, ocf_per_share DOUBLE, cash DOUBLE,
                    accounts_receivable DOUBLE, inventory DOUBLE, total_assets DOUBLE,
                    total_liabilities DOUBLE, total_equity DOUBLE, revenue DOUBLE,
                    revenue_ttm DOUBLE, net_profit_parent DOUBLE,
                    net_profit_parent_ttm DOUBLE, operating_cashflow DOUBLE, capex DOUBLE,
                    source_file VARCHAR, fetched_at VARCHAR,
                    PRIMARY KEY (symbol, report_period)
                )
                """
            )
            for name in ("source", "source_url", "observed_at", "ingested_at", "raw_response_locator", "raw_path", "raw_artifact_id"):
                con.execute("ALTER TABLE tdx_financial_snapshot ADD COLUMN IF NOT EXISTS {0} VARCHAR".format(name))
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS funnel_metrics (
                    symbol VARCHAR PRIMARY KEY, name VARCHAR, is_active BOOLEAN,
                    latest_report_period VARCHAR, published_at VARCHAR,
                    pe_ttm DOUBLE, pe_dynamic DOUBLE, pe_for_filter DOUBLE,
                    pe_source VARCHAR, pb DOUBLE, pb_source VARCHAR,
                    roe_latest DOUBLE, roe_annual_median DOUBLE, roe_annual_min DOUBLE,
                    annual_period_count INTEGER, revenue_ttm DOUBLE,
                    net_profit_parent_ttm DOUBLE, revenue_cagr_pct DOUBLE,
                    net_profit_cagr_pct DOUBLE, debt_ratio_latest DOUBLE,
                    cash_to_assets_pct DOUBLE, fcf_latest_period DOUBLE,
                    latest_annual_period VARCHAR,
                    operating_cashflow_latest_annual DOUBLE,
                    fcf_latest_annual DOUBLE,
                    ocf_to_net_profit_latest_annual_pct DOUBLE,
                    ocf_to_net_profit_annual_sum_pct DOUBLE,
                    history_start_period VARCHAR, history_end_period VARCHAR,
                    quality_status VARCHAR, rebuilt_at VARCHAR
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY, description VARCHAR, applied_at VARCHAR
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_artifact_manifest (
                    artifact_id VARCHAR PRIMARY KEY, dataset VARCHAR, source VARCHAR,
                    source_url VARCHAR, observed_at VARCHAR, ingested_at VARCHAR,
                    raw_response_locator VARCHAR, storage_path VARCHAR, sha256 VARCHAR,
                    byte_size BIGINT, metadata_json VARCHAR
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS validation_result (
                    symbol VARCHAR, report_period VARCHAR, metric VARCHAR,
                    source_a VARCHAR, source_b VARCHAR, value_a DOUBLE, value_b DOUBLE,
                    difference_pct DOUBLE, status VARCHAR, observed_at VARCHAR,
                    PRIMARY KEY (symbol, report_period, metric, source_a, source_b)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS provider_health (
                    provider VARCHAR PRIMARY KEY, status VARCHAR, last_attempt_at VARCHAR,
                    last_success_at VARCHAR, last_error VARCHAR, consecutive_failures INTEGER
                )
                """
            )
            con.execute(
                "CREATE TABLE IF NOT EXISTS fundamental_snapshot_history AS SELECT *, CAST(NULL AS VARCHAR) AS version_id FROM fundamental_snapshot WHERE FALSE"
            )
            con.execute(
                "CREATE TABLE IF NOT EXISTS tdx_financial_snapshot_history AS SELECT *, CAST(NULL AS VARCHAR) AS version_id FROM tdx_financial_snapshot WHERE FALSE"
            )
            con.execute(
                "UPDATE fundamental_snapshot SET observed_at = COALESCE(observed_at, fetched_at), ingested_at = COALESCE(ingested_at, fetched_at)"
            )
            con.execute(
                "UPDATE tdx_financial_snapshot SET source = COALESCE(source, 'tdx_financial_via_mootdx'), observed_at = COALESCE(observed_at, fetched_at), ingested_at = COALESCE(ingested_at, fetched_at), raw_response_locator = COALESCE(raw_response_locator, source_file)"
            )
            con.execute(
                """
                INSERT INTO fundamental_snapshot_history
                SELECT *, md5(symbol || ':' || report_period || ':' || COALESCE(ingested_at, fetched_at, 'legacy'))
                FROM fundamental_snapshot f
                WHERE NOT EXISTS (
                    SELECT 1 FROM fundamental_snapshot_history h
                    WHERE h.symbol = f.symbol AND h.report_period = f.report_period
                      AND COALESCE(h.ingested_at, h.fetched_at, '') = COALESCE(f.ingested_at, f.fetched_at, '')
                )
                """
            )
            con.execute(
                """
                INSERT INTO tdx_financial_snapshot_history
                SELECT *, md5(symbol || ':' || report_period || ':' || COALESCE(ingested_at, fetched_at, 'legacy'))
                FROM tdx_financial_snapshot t
                WHERE NOT EXISTS (
                    SELECT 1 FROM tdx_financial_snapshot_history h
                    WHERE h.symbol = t.symbol AND h.report_period = t.report_period
                      AND COALESCE(h.ingested_at, h.fetched_at, '') = COALESCE(t.ingested_at, t.fetched_at, '')
                )
                """
            )
            con.execute(
                "INSERT OR IGNORE INTO schema_migrations VALUES (2, 'provenance, immutable history, PIT and validation', CAST(CURRENT_TIMESTAMP AS VARCHAR))"
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS market_quote_latest (
                    symbol VARCHAR PRIMARY KEY, instrument_id VARCHAR, name VARCHAR,
                    market VARCHAR, exchange VARCHAR, currency VARCHAR, price DOUBLE,
                    previous_close DOUBLE, change DOUBLE, change_pct DOUBLE,
                    session VARCHAR, quote_at VARCHAR, observed_at VARCHAR,
                    ingested_at VARCHAR, source VARCHAR, source_url VARCHAR,
                    raw_path VARCHAR, payload_json VARCHAR
                )
                """
            )
            for name in ("raw_response_locator", "raw_artifact_id"):
                con.execute("ALTER TABLE market_quote_latest ADD COLUMN IF NOT EXISTS {0} VARCHAR".format(name))
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS market_quote_observation (
                    symbol VARCHAR, observed_at VARCHAR, source VARCHAR,
                    price DOUBLE, quote_at VARCHAR, session VARCHAR,
                    payload_json VARCHAR,
                    PRIMARY KEY (symbol, observed_at, source)
                )
                """
            )
            for name in ("ingested_at", "source_url", "raw_response_locator", "raw_path", "raw_artifact_id"):
                con.execute("ALTER TABLE market_quote_observation ADD COLUMN IF NOT EXISTS {0} VARCHAR".format(name))
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS market_price_bar (
                    symbol VARCHAR, interval VARCHAR, adjustment VARCHAR,
                    timestamp BIGINT, bar_at VARCHAR, open DOUBLE, high DOUBLE,
                    low DOUBLE, close DOUBLE, raw_close DOUBLE,
                    adjustment_factor DOUBLE, volume DOUBLE, source VARCHAR,
                    ingested_at VARCHAR,
                    PRIMARY KEY (symbol, interval, adjustment, timestamp, source)
                )
                """
            )
            for name in ("source_url", "observed_at", "raw_response_locator", "raw_path", "raw_artifact_id"):
                con.execute("ALTER TABLE market_price_bar ADD COLUMN IF NOT EXISTS {0} VARCHAR".format(name))
            con.execute("ALTER TABLE market_price_bar ADD COLUMN IF NOT EXISTS amount DOUBLE")
            con.execute("ALTER TABLE market_price_bar ADD COLUMN IF NOT EXISTS payload_json VARCHAR")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS economic_calendar_event (
                    event_id VARCHAR PRIMARY KEY, country VARCHAR, event_date VARCHAR,
                    event_time VARCHAR, timezone VARCHAR, scheduled_at VARCHAR,
                    event_name VARCHAR, impact VARCHAR, actual VARCHAR, estimate VARCHAR,
                    previous VARCHAR, unit VARCHAR, source VARCHAR, source_url VARCHAR,
                    observed_at VARCHAR, ingested_at VARCHAR, raw_response_locator VARCHAR,
                    raw_path VARCHAR, raw_artifact_id VARCHAR, payload_json VARCHAR
                )
                """
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_economic_calendar_range ON economic_calendar_event(event_date, country)"
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS economic_indicator_latest (
                    indicator_id VARCHAR PRIMARY KEY, country VARCHAR, name VARCHAR,
                    source VARCHAR, source_series_id VARCHAR, period VARCHAR,
                    value DOUBLE, previous_value DOUBLE, change_value DOUBLE,
                    change_pct DOUBLE, unit VARCHAR, frequency VARCHAR,
                    latest_date VARCHAR, source_url VARCHAR, observed_at VARCHAR,
                    ingested_at VARCHAR, raw_response_locator VARCHAR, raw_path VARCHAR,
                    raw_artifact_id VARCHAR, payload_json VARCHAR
                )
                """
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_economic_indicator_country ON economic_indicator_latest(country, latest_date)"
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS earnings_calendar_event (
                    event_id VARCHAR PRIMARY KEY, market VARCHAR, symbol VARCHAR,
                    name VARCHAR, report_date VARCHAR, report_time VARCHAR,
                    timezone VARCHAR, scheduled_at VARCHAR, fiscal_period VARCHAR,
                    eps_forecast VARCHAR, previous_eps VARCHAR, source VARCHAR,
                    source_url VARCHAR, observed_at VARCHAR, ingested_at VARCHAR,
                    raw_response_locator VARCHAR, raw_path VARCHAR,
                    raw_artifact_id VARCHAR, payload_json VARCHAR
                )
                """
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_earnings_calendar_range ON earnings_calendar_event(report_date, market, symbol)"
            )
            con.execute(
                "INSERT OR IGNORE INTO schema_migrations VALUES (3, 'economic and earnings calendar datasets', CAST(CURRENT_TIMESTAMP AS VARCHAR))"
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS tushare_request (
                    request_id VARCHAR PRIMARY KEY, api_name VARCHAR, params_json VARCHAR,
                    requested_fields VARCHAR, response_fields_json VARCHAR,
                    response_code INTEGER, response_message VARCHAR, row_count BIGINT,
                    source VARCHAR, source_url VARCHAR, observed_at VARCHAR,
                    ingested_at VARCHAR, raw_path VARCHAR, raw_artifact_id VARCHAR
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS tushare_data_row (
                    request_id VARCHAR, row_index BIGINT, api_name VARCHAR,
                    symbol VARCHAR, data_date VARCHAR, source VARCHAR,
                    source_url VARCHAR, observed_at VARCHAR, ingested_at VARCHAR,
                    payload_json VARCHAR,
                    PRIMARY KEY (request_id, row_index)
                )
                """
            )
            con.execute("CREATE INDEX IF NOT EXISTS idx_tushare_rows_api ON tushare_data_row(api_name, symbol, data_date)")
            con.execute(
                "INSERT OR IGNORE INTO schema_migrations VALUES (4, 'generic Tushare request and full-field row storage', CAST(CURRENT_TIMESTAMP AS VARCHAR))"
            )
            con.execute(
                """
                INSERT OR IGNORE INTO provider_health
                SELECT source, 'healthy', MAX(ingested_at), MAX(ingested_at), NULL, 0
                FROM market_quote_latest WHERE source IS NOT NULL GROUP BY source
                """
            )
            con.execute(
                """
                INSERT OR IGNORE INTO provider_health
                SELECT 'akshare_eastmoney_financials', 'healthy', MAX(fetched_at), MAX(fetched_at), NULL, 0
                FROM fundamental_snapshot HAVING COUNT(*) > 0
                """
            )
            con.execute(
                """
                INSERT OR IGNORE INTO provider_health
                SELECT 'tdx_financial_via_mootdx', 'healthy', MAX(fetched_at), MAX(fetched_at), NULL, 0
                FROM tdx_financial_snapshot HAVING COUNT(*) > 0
                """
            )

    def replace_fundamentals(self, report_period: str, rows: List[Dict[str, Any]]) -> int:
        frame = pd.DataFrame(rows, columns=FUNDAMENTAL_COLUMNS)
        history = frame.copy()
        history["version_id"] = [uuid.uuid4().hex for _ in range(len(history))]
        with self._lock, self.connect() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute("DELETE FROM fundamental_snapshot WHERE report_period = ?", [report_period])
                con.register("incoming_fundamentals", frame)
                columns = ",".join(FUNDAMENTAL_COLUMNS)
                con.execute(
                    "INSERT INTO fundamental_snapshot ({0}) SELECT {0} FROM incoming_fundamentals".format(columns)
                )
                con.register("incoming_fundamentals_history", history)
                history_columns = columns + ",version_id"
                con.execute(
                    "INSERT INTO fundamental_snapshot_history ({0}) SELECT {0} FROM incoming_fundamentals_history".format(history_columns)
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
        return len(rows)

    def replace_statement_rows(self, symbol: str, statement: str, rows: List[Dict[str, Any]]) -> int:
        with self._lock, self.connect() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(
                    "DELETE FROM financial_statement_rows WHERE symbol = ? AND statement = ?",
                    [symbol, statement],
                )
                con.executemany(
                    """
                    INSERT INTO financial_statement_rows (
                        instrument_id, symbol, statement, report_date, published_at,
                        source, payload_json, fetched_at, source_url, observed_at,
                        ingested_at, raw_response_locator, raw_path, raw_artifact_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        [
                            row["instrument_id"], row["symbol"], row["statement"],
                            row["report_date"], row.get("published_at"), row["source"],
                            json.dumps(row["payload"], ensure_ascii=False, allow_nan=False),
                            row["fetched_at"], row.get("source_url"), row.get("observed_at"),
                            row.get("ingested_at"), row.get("raw_response_locator"),
                            row.get("raw_path"), row.get("raw_artifact_id"),
                        ]
                        for row in rows
                    ],
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
        return len(rows)

    @staticmethod
    def _rows(con, sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
        result = con.execute(sql, params)
        columns = [item[0] for item in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

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
                "published_at IS NOT NULL AND published_at <= ?",
                "CAST(COALESCE(observed_at, ingested_at, fetched_at) AS DATE) <= CAST(? AS DATE)",
            ])
            params.extend([as_of, as_of])
        if active_only:
            where.append("is_active IS TRUE")
        if symbol:
            where.append("symbol = ?")
            params.append(symbol)
        if report_period:
            where.append("report_period = ?")
            params.append(report_period)
        elif as_of:
            where.append(
                "report_period = (SELECT MAX(fs2.report_period) FROM fundamental_snapshot_history fs2 WHERE fs2.symbol = {0}.symbol AND fs2.published_at IS NOT NULL AND fs2.published_at <= ? AND CAST(COALESCE(fs2.observed_at, fs2.ingested_at, fs2.fetched_at) AS DATE) <= CAST(? AS DATE))".format(table)
            )
            params.extend([as_of, as_of])
        elif symbol:
            where.append(
                "report_period = (SELECT MAX(fs2.report_period) FROM fundamental_snapshot fs2 WHERE fs2.symbol = fundamental_snapshot.symbol)"
            )
        else:
            where.append("report_period = (SELECT MAX(report_period) FROM fundamental_snapshot)")
        if industry:
            where.append("industry = ?")
            params.append(industry)
        if min_roe is not None:
            where.append("roe_weighted >= ?")
            params.append(min_roe)
        if max_pe is not None:
            where.append("pe_dynamic > 0 AND pe_dynamic <= ?")
            params.append(max_pe)
        selected = "* EXCLUDE (version_id)" if as_of else "*"
        sql = "SELECT {0} FROM {1}".format(selected, table)
        if where:
            sql += " WHERE " + " AND ".join(where)
        if as_of:
            sql += " QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, report_period ORDER BY COALESCE(ingested_at, fetched_at) DESC, version_id DESC) = 1"
        sql += " ORDER BY symbol LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        with self._lock, self.connect() as con:
            return self._rows(con, sql, params)

    def count_fundamentals(self, report_period: str = "") -> int:
        with self._lock, self.connect() as con:
            if report_period:
                return int(con.execute(
                    "SELECT COUNT(*) FROM fundamental_snapshot WHERE report_period = ?", [report_period]
                ).fetchone()[0])
            return int(con.execute("SELECT COUNT(*) FROM fundamental_snapshot").fetchone()[0])

    def get_statement_rows(
        self, symbol: str, statement: str = "", limit_periods: int = 20, as_of: str = ""
    ) -> List[Dict[str, Any]]:
        where = ["symbol = ?"]
        params: List[Any] = [symbol]
        if statement:
            where.append("statement = ?")
            params.append(statement)
        if as_of:
            where.extend([
                "published_at IS NOT NULL AND published_at <= ?",
                "CAST(COALESCE(observed_at, ingested_at, fetched_at) AS DATE) <= CAST(? AS DATE)",
            ])
            params.extend([as_of, as_of])
        params.append(limit_periods)
        sql = (
            "SELECT * FROM financial_statement_rows WHERE " + " AND ".join(where)
            + " ORDER BY report_date DESC, statement LIMIT ?"
        )
        with self._lock, self.connect() as con:
            rows = self._rows(con, sql, params)
        for row in rows:
            row["payload"] = json.loads(row.pop("payload_json"))
        return rows

    def save_run(self, row: Iterable[Any]) -> None:
        with self._lock, self.connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO ingestion_runs (run_id, job_name, status, report_period, started_at, finished_at, row_count, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                list(row),
            )

    def save_artifact(self, row: Dict[str, Any]) -> None:
        columns = ["artifact_id", "dataset", "source", "source_url", "observed_at", "ingested_at", "raw_response_locator", "storage_path", "sha256", "byte_size", "metadata_json"]
        with self._lock, self.connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO raw_artifact_manifest ({0}) VALUES ({1})".format(
                    ",".join(columns), ",".join("?" for _ in columns)
                ),
                [row.get(column) for column in columns],
            )

    def save_artifacts(self, rows: List[Dict[str, Any]]) -> int:
        columns = ["artifact_id", "dataset", "source", "source_url", "observed_at", "ingested_at", "raw_response_locator", "storage_path", "sha256", "byte_size", "metadata_json"]
        if rows:
            with self._lock, self.connect() as con:
                con.executemany(
                    "INSERT OR IGNORE INTO raw_artifact_manifest ({0}) VALUES ({1})".format(
                        ",".join(columns), ",".join("?" for _ in columns)
                    ),
                    [[row.get(column) for column in columns] for row in rows],
                )
        return len(rows)

    def artifact_paths(self) -> set[str]:
        with self._lock, self.connect() as con:
            return {row[0] for row in con.execute("SELECT storage_path FROM raw_artifact_manifest").fetchall()}

    def list_artifacts(self, dataset: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM raw_artifact_manifest"
        params: List[Any] = []
        if dataset:
            sql += " WHERE dataset = ?"
            params.append(dataset)
        sql += " ORDER BY ingested_at DESC LIMIT ?"
        params.append(limit)
        with self._lock, self.connect() as con:
            return self._rows(con, sql, params)

    def latest_artifact(self, dataset: str, metadata_key: str = "", metadata_value: str = "") -> Optional[Dict[str, Any]]:
        sql = "SELECT * FROM raw_artifact_manifest WHERE dataset = ?"
        params: List[Any] = [dataset]
        if metadata_key:
            sql += " AND json_extract_string(metadata_json, ?) = ?"
            params.extend(["$." + metadata_key, metadata_value])
        sql += " ORDER BY ingested_at DESC LIMIT 1"
        with self._lock, self.connect() as con:
            rows = self._rows(con, sql, params)
        return rows[0] if rows else None

    def save_validation_results(self, rows: List[Dict[str, Any]]) -> int:
        columns = ["symbol", "report_period", "metric", "source_a", "source_b", "value_a", "value_b", "difference_pct", "status", "observed_at"]
        if rows:
            with self._lock, self.connect() as con:
                con.executemany(
                    "INSERT OR REPLACE INTO validation_result ({0}) VALUES ({1})".format(
                        ",".join(columns), ",".join("?" for _ in columns)
                    ),
                    [[row.get(column) for column in columns] for row in rows],
                )
        return len(rows)

    def get_validation_results(self, symbol: str, report_period: str) -> List[Dict[str, Any]]:
        with self._lock, self.connect() as con:
            return self._rows(
                con,
                "SELECT * FROM validation_result WHERE symbol = ? AND report_period = ? ORDER BY metric, source_a, source_b",
                [symbol, report_period],
            )

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
        selects = []
        params: List[Any] = []
        for metric, source_a, source_b, value_a, value_b in pairs:
            selects.append(
                "SELECT f.symbol, f.report_period, ? AS metric, ? AS source_a, ? AS source_b, {0} AS value_a, {1} AS value_b FROM fundamental_snapshot f LEFT JOIN tdx_financial_snapshot t USING (symbol, report_period) LEFT JOIN baostock_snapshot b USING (symbol, report_period) WHERE f.report_period = ?".format(value_a, value_b)
            )
            params.extend([metric, source_a, source_b, report_period])
        sql = " UNION ALL ".join(selects)
        params.append(observed_at)
        with self._lock, self.connect() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO validation_result
                WITH pairs AS ({0}), scored AS (
                    SELECT *,
                           CASE WHEN value_a IS NULL OR value_b IS NULL OR value_a = 0 THEN NULL
                                ELSE ABS(value_a - value_b) / ABS(value_a) * 100 END AS difference_pct
                    FROM pairs
                )
                SELECT symbol, report_period, metric, source_a, source_b, value_a, value_b,
                       ROUND(difference_pct, 4),
                       CASE WHEN difference_pct IS NULL THEN 'missing_comparison'
                            WHEN difference_pct <= 1.0 THEN 'consistent'
                            ELSE 'difference_over_1pct' END,
                       ?
                FROM scored
                """.format(sql),
                params,
            )
            return int(con.execute("SELECT COUNT(*) FROM validation_result WHERE report_period = ?", [report_period]).fetchone()[0])

    def record_provider_health(self, provider: str, success: bool, attempted_at: str, error: str = "") -> None:
        with self._lock, self.connect() as con:
            previous = con.execute("SELECT last_success_at, consecutive_failures FROM provider_health WHERE provider = ?", [provider]).fetchone()
            last_success = attempted_at if success else (previous[0] if previous else None)
            failures = 0 if success else ((previous[1] if previous else 0) or 0) + 1
            con.execute(
                "INSERT OR REPLACE INTO provider_health VALUES (?, ?, ?, ?, ?, ?)",
                [provider, "healthy" if success else "degraded", attempted_at, last_success, None if success else error, failures],
            )

    def provider_health(self) -> List[Dict[str, Any]]:
        with self._lock, self.connect() as con:
            return self._rows(con, "SELECT * FROM provider_health ORDER BY provider", [])

    def upsert_quote(self, row: Dict[str, Any]) -> None:
        payload_json = json.dumps(row, ensure_ascii=False, allow_nan=False)
        values = [
            row.get("symbol"), row.get("instrument_id"), row.get("name"), row.get("market"),
            row.get("exchange"), row.get("currency"), row.get("price"), row.get("previous_close"),
            row.get("change"), row.get("change_pct"), row.get("session"), row.get("quote_at"),
            row.get("observed_at"), row.get("ingested_at"), row.get("source"),
            row.get("source_url"), row.get("raw_path"), payload_json,
        ]
        with self._lock, self.connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO market_quote_latest (symbol, instrument_id, name, market, exchange, currency, price, previous_close, change, change_pct, session, quote_at, observed_at, ingested_at, source, source_url, raw_path, payload_json, raw_response_locator, raw_artifact_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                values + [row.get("raw_response_locator"), row.get("raw_artifact_id")],
            )
            con.execute(
                "INSERT OR REPLACE INTO market_quote_observation (symbol, observed_at, source, price, quote_at, session, payload_json, ingested_at, source_url, raw_response_locator, raw_path, raw_artifact_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [row.get("symbol"), row.get("observed_at"), row.get("source"), row.get("price"), row.get("quote_at"), row.get("session"), payload_json, row.get("ingested_at"), row.get("source_url"), row.get("raw_response_locator"), row.get("raw_path"), row.get("raw_artifact_id")],
            )

    def get_latest_quotes(self, symbols: Sequence[str]) -> List[Dict[str, Any]]:
        if not symbols:
            return []
        placeholders = ",".join("?" for _ in symbols)
        with self._lock, self.connect() as con:
            rows = self._rows(con, "SELECT payload_json FROM market_quote_latest WHERE symbol IN ({0}) ORDER BY symbol".format(placeholders), list(symbols))
        return [json.loads(row["payload_json"]) for row in rows]

    def upsert_price_bars(self, symbol: str, interval: str, adjustment: str, source: str, ingested_at: str, bars: List[Dict[str, Any]], provenance: Optional[Dict[str, Any]] = None) -> int:
        provenance = provenance or {}
        values = [
            [symbol, interval, adjustment, bar.get("timestamp"), bar.get("bar_at"), bar.get("open"), bar.get("high"), bar.get("low"), bar.get("close"), bar.get("raw_close"), bar.get("adjustment_factor"), bar.get("volume"), source, ingested_at, provenance.get("source_url"), provenance.get("observed_at") or bar.get("bar_at"), provenance.get("raw_response_locator"), provenance.get("raw_path"), provenance.get("raw_artifact_id"), bar.get("amount"), json.dumps(bar.get("source_payload") or bar, ensure_ascii=False, allow_nan=False)]
            for bar in bars
        ]
        if values:
            with self._lock, self.connect() as con:
                con.executemany("INSERT OR REPLACE INTO market_price_bar (symbol, interval, adjustment, timestamp, bar_at, open, high, low, close, raw_close, adjustment_factor, volume, source, ingested_at, source_url, observed_at, raw_response_locator, raw_path, raw_artifact_id, amount, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values)
        return len(values)

    def save_tushare_response(
        self, request: Dict[str, Any], rows: List[Dict[str, Any]]
    ) -> int:
        date_keys = ("trade_time", "trade_date", "cal_date", "ann_date", "end_date", "date", "time")
        values = []
        for index, row in enumerate(rows):
            symbol = row.get("ts_code") or row.get("code") or row.get("symbol")
            data_date = next((row.get(key) for key in date_keys if row.get(key) not in (None, "")), None)
            values.append([
                request["request_id"], index, request["api_name"], symbol, data_date,
                request["source"], request["source_url"], request["observed_at"],
                request["ingested_at"], json.dumps(row, ensure_ascii=False, allow_nan=False),
            ])
        with self._lock, self.connect() as con:
            con.execute(
                "INSERT INTO tushare_request VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    request["request_id"], request["api_name"],
                    json.dumps(request.get("params") or {}, ensure_ascii=False, allow_nan=False),
                    request.get("requested_fields") or "",
                    json.dumps(request.get("response_fields") or [], ensure_ascii=False),
                    request.get("response_code"), request.get("response_message"), len(rows),
                    request["source"], request["source_url"], request["observed_at"],
                    request["ingested_at"], request.get("raw_path"), request.get("raw_artifact_id"),
                ],
            )
            if values:
                con.executemany(
                    "INSERT INTO tushare_data_row VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values
                )
        return len(rows)

    def get_price_bars(self, symbol: str, interval: str, adjustment: str, limit: int) -> List[Dict[str, Any]]:
        with self._lock, self.connect() as con:
            rows = self._rows(
                con,
                "SELECT symbol, interval, adjustment, timestamp, bar_at, open, high, low, close, raw_close, adjustment_factor, volume, amount, source, ingested_at, payload_json FROM market_price_bar WHERE symbol = ? AND interval = ? AND adjustment = ? ORDER BY timestamp DESC LIMIT ?",
                [symbol, interval, adjustment, limit],
            )
        result = []
        for row in reversed(rows):
            payload = row.pop("payload_json", None)
            row["source_payload"] = json.loads(payload) if payload else {}
            result.append(row)
        return result

    @staticmethod
    def _utc_range(start: str, end: str) -> tuple[datetime, datetime]:
        def parse(value: str) -> datetime:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("history range timestamps must include a timezone")
            return parsed.astimezone(timezone.utc)

        start_at, end_at = parse(start), parse(end)
        if start_at > end_at:
            raise ValueError("history range start must not be after end")
        return start_at, end_at

    @staticmethod
    def _utc_iso(value: Any) -> str:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()

    def get_price_bars_range(
        self, symbol: str, interval: str, adjustment: str,
        start: str, end: str, limit: int,
    ) -> tuple[List[Dict[str, Any]], bool]:
        if not 1 <= limit <= 5000:
            raise ValueError("history limit must be between 1 and 5000")
        start_at, end_at = self._utc_range(start, end)
        with self._lock, self.connect() as con:
            rows = self._rows(
                con,
                "SELECT symbol, interval, adjustment, timestamp, bar_at, open, high, "
                "low, close, raw_close, adjustment_factor, volume, amount, source, "
                "ingested_at, payload_json FROM market_price_bar WHERE symbol = ? "
                "AND interval = ? AND adjustment = ? AND timestamp >= ? AND timestamp <= ? "
                "ORDER BY timestamp ASC, source ASC LIMIT ?",
                [symbol, interval, adjustment, int(start_at.timestamp()),
                 int(end_at.timestamp()), limit + 1],
            )
        truncated = len(rows) > limit
        result = []
        for row in rows[:limit]:
            payload = row.pop("payload_json", None)
            row["timestamp"] = int(row["timestamp"])
            row["bar_at"] = self._utc_iso(row["bar_at"])
            row["ingested_at"] = self._utc_iso(row["ingested_at"])
            row["source_payload"] = json.loads(payload) if payload else {}
            result.append(row)
        return result, truncated

    def get_price_bars_cross_section(
        self, interval: str, adjustment: str, bar_at: str, limit: int,
        symbols: Optional[Sequence[str]] = None,
    ) -> tuple[List[Dict[str, Any]], bool]:
        if not 1 <= limit <= 5000:
            raise ValueError("cross-section limit must be between 1 and 5000")
        point, _ = self._utc_range(bar_at, bar_at)
        symbol_filter = None if symbols is None else sorted(set(symbols))
        if symbol_filter is not None and len(symbol_filter) > 5000:
            raise ValueError("cross-section symbols must contain at most 5000 values")
        if symbol_filter == []:
            return [], False
        filters = ""
        parameters: List[Any] = [interval, adjustment, int(point.timestamp())]
        if symbol_filter is not None:
            filters = " AND symbol IN (" + ",".join("?" for _ in symbol_filter) + ")"
            parameters.extend(symbol_filter)
        parameters.append(limit + 1)
        with self._lock, self.connect() as con:
            rows = self._rows(
                con,
                "SELECT symbol, interval, adjustment, timestamp, bar_at, open, high, "
                "low, close, raw_close, adjustment_factor, volume, amount, source, "
                "ingested_at, payload_json FROM (SELECT *, ROW_NUMBER() OVER "
                "(PARTITION BY symbol ORDER BY ingested_at DESC, source ASC) AS selected "
                "FROM market_price_bar WHERE interval = ? AND adjustment = ? "
                "AND timestamp = ?" + filters + ") WHERE selected = 1 "
                "ORDER BY symbol ASC LIMIT ?",
                parameters,
            )
        truncated = len(rows) > limit
        result = []
        for row in rows[:limit]:
            payload = row.pop("payload_json", None)
            row["timestamp"] = int(row["timestamp"])
            row["bar_at"] = self._utc_iso(row["bar_at"])
            row["ingested_at"] = self._utc_iso(row["ingested_at"])
            row["source_payload"] = json.loads(payload) if payload else {}
            result.append(row)
        return result, truncated

    def get_price_bars_for_reconciliation(
        self, symbol: str, interval: str, adjustment: str, source: str,
        timestamps: Sequence[int],
    ) -> List[Dict[str, Any]]:
        if not timestamps:
            return []
        placeholders = ",".join("?" for _ in timestamps)
        query = (
            "SELECT symbol, interval, adjustment, timestamp, bar_at, open, high, low, "
            "close, volume, amount, source, ingested_at, observed_at, raw_artifact_id "
            "FROM market_price_bar WHERE symbol=? AND interval=? AND adjustment=? "
            "AND source=? AND timestamp IN (" + placeholders + ") ORDER BY timestamp"
        )
        with self._lock, self.connect() as con:
            return self._rows(
                con, query, [symbol, interval, adjustment, source, *timestamps]
            )

    @staticmethod
    def _calendar_payload(row: Dict[str, Any]) -> Dict[str, Any]:
        payload = json.loads(row.pop("payload_json") or "{}")
        payload.update({key: value for key, value in row.items() if key not in payload})
        return payload

    @staticmethod
    def _serialized_rows(rows: List[Dict[str, Any]], columns: List[str]) -> List[List[Any]]:
        return [
            [
                row.get(column)
                if column != "payload_json"
                else json.dumps(row, ensure_ascii=False, allow_nan=False)
                for column in columns
            ]
            for row in rows
        ]

    def upsert_economic_calendar(self, rows: List[Dict[str, Any]]) -> int:
        columns = [
            "event_id", "country", "event_date", "event_time", "timezone", "scheduled_at",
            "event_name", "impact", "actual", "estimate", "previous", "unit", "source",
            "source_url", "observed_at", "ingested_at", "raw_response_locator", "raw_path",
            "raw_artifact_id", "payload_json",
        ]
        if rows:
            with self._lock, self.connect() as con:
                con.executemany(
                    "INSERT OR REPLACE INTO economic_calendar_event ({0}) VALUES ({1})".format(
                        ",".join(columns), ",".join("?" for _ in columns)
                    ),
                    self._serialized_rows(rows, columns),
                )
        return len(rows)

    def get_economic_calendar(
        self, date_from: str, date_to: str, country: str = "US", impact: str = "", limit: int = 50
    ) -> List[Dict[str, Any]]:
        where, params = ["event_date >= ?", "event_date <= ?"], [date_from, date_to]
        if country:
            where.append("country = ?")
            params.append(country.upper())
        if impact:
            where.append("LOWER(impact) = LOWER(?)")
            params.append(impact)
        params.append(limit)
        sql = (
            "SELECT * FROM economic_calendar_event WHERE " + " AND ".join(where)
            + " ORDER BY event_date, event_time, event_name LIMIT ?"
        )
        with self._lock, self.connect() as con:
            return [self._calendar_payload(row) for row in self._rows(con, sql, params)]

    def upsert_economic_indicators(self, rows: List[Dict[str, Any]]) -> int:
        columns = [
            "indicator_id", "country", "name", "source", "source_series_id", "period", "value",
            "previous_value", "change_value", "change_pct", "unit", "frequency", "latest_date",
            "source_url", "observed_at", "ingested_at", "raw_response_locator", "raw_path",
            "raw_artifact_id", "payload_json",
        ]
        if rows:
            with self._lock, self.connect() as con:
                con.executemany(
                    "INSERT OR REPLACE INTO economic_indicator_latest ({0}) VALUES ({1})".format(
                        ",".join(columns), ",".join("?" for _ in columns)
                    ),
                    self._serialized_rows(rows, columns),
                )
        return len(rows)

    def get_economic_indicators(self, country: str = "US", source: str = "", limit: int = 50) -> List[Dict[str, Any]]:
        where, params = [], []
        if country:
            where.append("country = ?")
            params.append(country.upper())
        if source:
            where.append("source = ?")
            params.append(source.lower())
        params.append(limit)
        sql = "SELECT * FROM economic_indicator_latest"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY latest_date DESC, name LIMIT ?"
        with self._lock, self.connect() as con:
            return [self._calendar_payload(row) for row in self._rows(con, sql, params)]

    def upsert_earnings_calendar(self, rows: List[Dict[str, Any]]) -> int:
        columns = [
            "event_id", "market", "symbol", "name", "report_date", "report_time", "timezone",
            "scheduled_at", "fiscal_period", "eps_forecast", "previous_eps", "source", "source_url",
            "observed_at", "ingested_at", "raw_response_locator", "raw_path", "raw_artifact_id", "payload_json",
        ]
        if rows:
            with self._lock, self.connect() as con:
                con.executemany(
                    "INSERT OR REPLACE INTO earnings_calendar_event ({0}) VALUES ({1})".format(
                        ",".join(columns), ",".join("?" for _ in columns)
                    ),
                    self._serialized_rows(rows, columns),
                )
        return len(rows)

    def get_earnings_calendar(
        self, date_from: str, date_to: str, market: str = "", symbols: Optional[Sequence[str]] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        where, params = ["report_date >= ?", "report_date <= ?"], [date_from, date_to]
        if market:
            where.append("market = ?")
            params.append(market.upper())
        requested = [item.upper() for item in symbols or [] if item]
        if requested:
            where.append("symbol IN ({0})".format(",".join("?" for _ in requested)))
            params.extend(requested)
        params.append(limit)
        sql = (
            "SELECT * FROM earnings_calendar_event WHERE " + " AND ".join(where)
            + " ORDER BY report_date, report_time, market, symbol LIMIT ?"
        )
        with self._lock, self.connect() as con:
            return [self._calendar_payload(row) for row in self._rows(con, sql, params)]

    def latest_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._lock, self.connect() as con:
            return self._rows(
                con,
                "SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT ?",
                [limit],
            )

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
        placeholders = ",".join("?" for _ in columns)
        with self._lock, self.connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO baostock_snapshot ({0}) VALUES ({1})".format(
                    ",".join(columns), placeholders
                ),
                [row.get(column) for column in columns],
            )

    def replace_tdx_period(self, report_period: str, rows: List[Dict[str, Any]]) -> int:
        columns = TDX_COLUMNS
        frame = pd.DataFrame(rows, columns=columns)
        history = frame.copy()
        history["version_id"] = [uuid.uuid4().hex for _ in range(len(history))]
        with self._lock, self.connect() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute("DELETE FROM tdx_financial_snapshot WHERE report_period = ?", [report_period])
                con.register("incoming_tdx", frame)
                names = ",".join(columns)
                con.execute(
                    "INSERT INTO tdx_financial_snapshot ({0}) SELECT {0} FROM incoming_tdx".format(names)
                )
                con.register("incoming_tdx_history", history)
                history_names = names + ",version_id"
                con.execute(
                    "INSERT INTO tdx_financial_snapshot_history ({0}) SELECT {0} FROM incoming_tdx_history".format(history_names)
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
        return len(rows)

    def get_baostock(self, symbol: str, report_period: str) -> Optional[Dict[str, Any]]:
        with self._lock, self.connect() as con:
            rows = self._rows(
                con,
                "SELECT * FROM baostock_snapshot WHERE symbol = ? AND report_period = ?",
                [symbol, report_period],
            )
        if rows and rows[0].get("payload_json"):
            rows[0]["payload"] = json.loads(rows[0].pop("payload_json"))
        return rows[0] if rows else None

    def get_tdx(self, symbol: str, report_period: str) -> Optional[Dict[str, Any]]:
        with self._lock, self.connect() as con:
            rows = self._rows(
                con,
                "SELECT * FROM tdx_financial_snapshot WHERE symbol = ? AND report_period = ?",
                [symbol, report_period],
            )
        return rows[0] if rows else None

    def tdx_coverage(self) -> List[Dict[str, Any]]:
        with self._lock, self.connect() as con:
            return self._rows(
                con,
                """
                SELECT report_period, COUNT(*) AS row_count, MIN(published_at) AS min_published_at,
                       MAX(published_at) AS max_published_at, MAX(fetched_at) AS fetched_at
                FROM tdx_financial_snapshot GROUP BY report_period ORDER BY report_period DESC
                """,
                [],
            )

    def get_tdx_history(self, symbol: str, annual_only: bool = False, limit: int = 40, as_of: str = "") -> List[Dict[str, Any]]:
        where = "symbol = ?"
        params: List[Any] = [symbol]
        if annual_only:
            where += " AND RIGHT(report_period, 4) = '1231'"
        table = "tdx_financial_snapshot"
        selected = "*"
        if as_of:
            table = "tdx_financial_snapshot_history"
            selected = "* EXCLUDE (version_id)"
            where += " AND published_at IS NOT NULL AND published_at <= ? AND CAST(COALESCE(observed_at, ingested_at, fetched_at) AS DATE) <= CAST(? AS DATE)"
            params.extend([as_of, as_of])
        params.append(limit)
        with self._lock, self.connect() as con:
            return self._rows(
                con,
                "SELECT {0} FROM {1} WHERE {2} {3} ORDER BY report_period DESC LIMIT ?".format(
                    selected,
                    table,
                    where,
                    "QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, report_period ORDER BY COALESCE(ingested_at, fetched_at) DESC, version_id DESC) = 1" if as_of else "",
                ),
                params,
            )

    def rebuild_funnel_metrics(self, rebuilt_at: str) -> int:
        with self._lock, self.connect() as con:
            con.execute(
                """
                CREATE OR REPLACE TABLE funnel_metrics AS
                WITH current_f AS (
                    SELECT * FROM fundamental_snapshot
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY report_period DESC) = 1
                ),
                latest_tdx AS (
                    SELECT * FROM tdx_financial_snapshot
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY report_period DESC) = 1
                ),
                latest_bao AS (
                    SELECT * FROM baostock_snapshot
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC, report_period DESC) = 1
                ),
                annual AS (
                    SELECT * FROM tdx_financial_snapshot WHERE RIGHT(report_period, 4) = '1231'
                ),
                latest_annual AS (
                    SELECT * FROM annual
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY report_period DESC) = 1
                ),
                annual_agg AS (
                    SELECT
                        symbol,
                        MEDIAN(roe_weighted) AS roe_annual_median,
                        MIN(roe_weighted) AS roe_annual_min,
                        COUNT(*) AS annual_period_count,
                        MIN(report_period) AS history_start_period,
                        MAX(report_period) AS history_end_period,
                        ARG_MIN(revenue_ttm, report_period) AS revenue_first,
                        ARG_MAX(revenue_ttm, report_period) AS revenue_last,
                        ARG_MIN(net_profit_parent_ttm, report_period) AS profit_first,
                        ARG_MAX(net_profit_parent_ttm, report_period) AS profit_last,
                        SUM(operating_cashflow) AS operating_cashflow_sum,
                        SUM(COALESCE(net_profit_parent, net_profit_parent_ttm)) AS net_profit_parent_sum,
                        CAST(LEFT(MAX(report_period), 4) AS INTEGER) - CAST(LEFT(MIN(report_period), 4) AS INTEGER) AS year_span
                    FROM annual GROUP BY symbol
                ),
                validation_agg AS (
                    SELECT symbol, report_period,
                           COUNT(*) FILTER (WHERE status = 'consistent') AS consistent_count,
                           COUNT(*) FILTER (WHERE status = 'difference_over_1pct') AS difference_count
                    FROM validation_result GROUP BY symbol, report_period
                )
                SELECT
                    f.symbol,
                    f.name,
                    f.is_active,
                    t.report_period AS latest_report_period,
                    t.published_at,
                    b.pe_ttm,
                    f.pe_dynamic,
                    COALESCE(b.pe_ttm, f.pe_dynamic) AS pe_for_filter,
                    CASE WHEN b.pe_ttm IS NOT NULL THEN 'baostock_pe_ttm' ELSE 'eastmoney_pe_dynamic' END AS pe_source,
                    COALESCE(b.pb_mrq, f.pb) AS pb,
                    CASE WHEN b.pb_mrq IS NOT NULL THEN 'baostock_pb_mrq' ELSE 'eastmoney_pb' END AS pb_source,
                    t.roe_weighted AS roe_latest,
                    a.roe_annual_median,
                    a.roe_annual_min,
                    COALESCE(a.annual_period_count, 0) AS annual_period_count,
                    t.revenue_ttm,
                    t.net_profit_parent_ttm,
                    CASE WHEN a.year_span > 0 AND a.revenue_first > 0 AND a.revenue_last > 0
                         THEN (POWER(a.revenue_last / a.revenue_first, 1.0 / a.year_span) - 1) * 100 END AS revenue_cagr_pct,
                    CASE WHEN a.year_span > 0 AND a.profit_first > 0 AND a.profit_last > 0
                         THEN (POWER(a.profit_last / a.profit_first, 1.0 / a.year_span) - 1) * 100 END AS net_profit_cagr_pct,
                    CASE WHEN t.total_assets > 0 THEN t.total_liabilities / t.total_assets * 100 END AS debt_ratio_latest,
                    CASE WHEN t.total_assets > 0 THEN t.cash / t.total_assets * 100 END AS cash_to_assets_pct,
                    t.operating_cashflow - t.capex AS fcf_latest_period,
                    la.report_period AS latest_annual_period,
                    la.operating_cashflow AS operating_cashflow_latest_annual,
                    la.operating_cashflow - la.capex AS fcf_latest_annual,
                    CASE WHEN COALESCE(la.net_profit_parent, la.net_profit_parent_ttm) != 0
                         THEN la.operating_cashflow / COALESCE(la.net_profit_parent, la.net_profit_parent_ttm) * 100 END AS ocf_to_net_profit_latest_annual_pct,
                    CASE WHEN a.net_profit_parent_sum != 0
                         THEN a.operating_cashflow_sum / a.net_profit_parent_sum * 100 END AS ocf_to_net_profit_annual_sum_pct,
                    a.history_start_period,
                    a.history_end_period,
                    CASE
                        WHEN t.symbol IS NULL THEN 'missing_tdx_latest'
                        WHEN COALESCE(v.difference_count, 0) > 0 THEN 'cross_source_difference_over_1pct'
                        WHEN COALESCE(a.annual_period_count, 0) < 3 THEN 'insufficient_annual_history'
                        WHEN b.symbol IS NULL THEN 'history_ready_valuation_single_source'
                        WHEN COALESCE(v.consistent_count, 0) > 0 THEN 'multi_source_verified'
                        ELSE 'multi_source_pending_validation'
                    END AS quality_status,
                    ? AS rebuilt_at
                FROM current_f f
                LEFT JOIN latest_tdx t USING (symbol)
                LEFT JOIN annual_agg a USING (symbol)
                LEFT JOIN latest_annual la USING (symbol)
                LEFT JOIN latest_bao b USING (symbol)
                LEFT JOIN validation_agg v ON v.symbol = t.symbol AND v.report_period = t.report_period
                """,
                [rebuilt_at],
            )
            return int(con.execute("SELECT COUNT(*) FROM funnel_metrics").fetchone()[0])

    def query_funnel_metrics(
        self,
        limit: int = 100,
        offset: int = 0,
        min_roe_median: Optional[float] = None,
        min_revenue_cagr: Optional[float] = None,
        min_profit_cagr: Optional[float] = None,
        max_pe: Optional[float] = None,
        max_debt_ratio: Optional[float] = None,
        min_annual_periods: int = 0,
        active_only: bool = True,
        as_of: str = "",
    ) -> List[Dict[str, Any]]:
        where: List[str] = []
        params: List[Any] = [as_of] if as_of else []
        if active_only:
            where.append("is_active IS TRUE")
        filters = [
            (min_roe_median, "roe_annual_median >= ?"),
            (min_revenue_cagr, "revenue_cagr_pct >= ?"),
            (min_profit_cagr, "net_profit_cagr_pct >= ?"),
            (max_pe, "pe_for_filter > 0 AND pe_for_filter <= ?"),
            (max_debt_ratio, "debt_ratio_latest <= ?"),
        ]
        for value, clause in filters:
            if value is not None:
                where.append(clause)
                params.append(value)
        if min_annual_periods:
            where.append("annual_period_count >= ?")
            params.append(min_annual_periods)
        sql = "SELECT * FROM funnel_metrics"
        if as_of:
            sql = """
            WITH cutoff AS (SELECT CAST(? AS DATE) AS as_of),
            f_versions AS (
                SELECT f.* FROM fundamental_snapshot_history f, cutoff c
                WHERE f.published_at IS NOT NULL AND CAST(f.published_at AS DATE) <= c.as_of
                  AND CAST(COALESCE(f.observed_at, f.ingested_at, f.fetched_at) AS DATE) <= c.as_of
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY f.symbol, f.report_period
                    ORDER BY COALESCE(f.ingested_at, f.fetched_at) DESC, f.version_id DESC
                ) = 1
            ),
            current_f AS (
                SELECT * FROM f_versions
                QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY report_period DESC) = 1
            ),
            t_versions AS (
                SELECT t.* FROM tdx_financial_snapshot_history t, cutoff c
                WHERE t.published_at IS NOT NULL AND CAST(t.published_at AS DATE) <= c.as_of
                  AND CAST(COALESCE(t.observed_at, t.ingested_at, t.fetched_at) AS DATE) <= c.as_of
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY t.symbol, t.report_period
                    ORDER BY COALESCE(t.ingested_at, t.fetched_at) DESC, t.version_id DESC
                ) = 1
            ),
            latest_tdx AS (
                SELECT * FROM t_versions
                QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY report_period DESC) = 1
            ),
            latest_bao AS (
                SELECT b.* FROM baostock_snapshot b, cutoff c
                WHERE CAST(COALESCE(b.observed_at, b.ingested_at, b.fetched_at) AS DATE) <= c.as_of
                QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC, report_period DESC) = 1
            ),
            annual AS (SELECT * FROM t_versions WHERE RIGHT(report_period, 4) = '1231'),
            latest_annual AS (
                SELECT * FROM annual
                QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY report_period DESC) = 1
            ),
            annual_agg AS (
                SELECT symbol, MEDIAN(roe_weighted) AS roe_annual_median,
                       MIN(roe_weighted) AS roe_annual_min, COUNT(*) AS annual_period_count,
                       MIN(report_period) AS history_start_period, MAX(report_period) AS history_end_period,
                       ARG_MIN(revenue_ttm, report_period) AS revenue_first,
                       ARG_MAX(revenue_ttm, report_period) AS revenue_last,
                       ARG_MIN(net_profit_parent_ttm, report_period) AS profit_first,
                       ARG_MAX(net_profit_parent_ttm, report_period) AS profit_last,
                       SUM(operating_cashflow) AS operating_cashflow_sum,
                       SUM(COALESCE(net_profit_parent, net_profit_parent_ttm)) AS net_profit_parent_sum,
                       CAST(LEFT(MAX(report_period), 4) AS INTEGER) - CAST(LEFT(MIN(report_period), 4) AS INTEGER) AS year_span
                FROM annual GROUP BY symbol
            ),
            validation_agg AS (
                SELECT v.symbol, v.report_period,
                       COUNT(*) FILTER (WHERE v.status = 'consistent') AS consistent_count,
                       COUNT(*) FILTER (WHERE v.status = 'difference_over_1pct') AS difference_count
                FROM validation_result v, cutoff c
                WHERE CAST(v.observed_at AS DATE) <= c.as_of GROUP BY v.symbol, v.report_period
            ),
            pit AS (
                SELECT f.symbol, f.name, f.is_active, t.report_period AS latest_report_period,
                       t.published_at, b.pe_ttm, f.pe_dynamic,
                       COALESCE(b.pe_ttm, f.pe_dynamic) AS pe_for_filter,
                       CASE WHEN b.pe_ttm IS NOT NULL THEN 'baostock_pe_ttm' ELSE 'eastmoney_pe_dynamic' END AS pe_source,
                       COALESCE(b.pb_mrq, f.pb) AS pb,
                       CASE WHEN b.pb_mrq IS NOT NULL THEN 'baostock_pb_mrq' ELSE 'eastmoney_pb' END AS pb_source,
                       t.roe_weighted AS roe_latest, a.roe_annual_median, a.roe_annual_min,
                       COALESCE(a.annual_period_count, 0) AS annual_period_count,
                       t.revenue_ttm, t.net_profit_parent_ttm,
                       CASE WHEN a.year_span > 0 AND a.revenue_first > 0 AND a.revenue_last > 0
                            THEN (POWER(a.revenue_last / a.revenue_first, 1.0 / a.year_span) - 1) * 100 END AS revenue_cagr_pct,
                       CASE WHEN a.year_span > 0 AND a.profit_first > 0 AND a.profit_last > 0
                            THEN (POWER(a.profit_last / a.profit_first, 1.0 / a.year_span) - 1) * 100 END AS net_profit_cagr_pct,
                       CASE WHEN t.total_assets > 0 THEN t.total_liabilities / t.total_assets * 100 END AS debt_ratio_latest,
                       CASE WHEN t.total_assets > 0 THEN t.cash / t.total_assets * 100 END AS cash_to_assets_pct,
                       t.operating_cashflow - t.capex AS fcf_latest_period,
                       la.report_period AS latest_annual_period,
                       la.operating_cashflow AS operating_cashflow_latest_annual,
                       la.operating_cashflow - la.capex AS fcf_latest_annual,
                       CASE WHEN COALESCE(la.net_profit_parent, la.net_profit_parent_ttm) != 0
                            THEN la.operating_cashflow / COALESCE(la.net_profit_parent, la.net_profit_parent_ttm) * 100 END AS ocf_to_net_profit_latest_annual_pct,
                       CASE WHEN a.net_profit_parent_sum != 0
                            THEN a.operating_cashflow_sum / a.net_profit_parent_sum * 100 END AS ocf_to_net_profit_annual_sum_pct,
                       a.history_start_period, a.history_end_period,
                       CASE WHEN t.symbol IS NULL THEN 'missing_tdx_latest'
                            WHEN COALESCE(v.difference_count, 0) > 0 THEN 'cross_source_difference_over_1pct'
                            WHEN COALESCE(a.annual_period_count, 0) < 3 THEN 'insufficient_annual_history'
                            WHEN b.symbol IS NULL THEN 'history_ready_valuation_single_source'
                            WHEN COALESCE(v.consistent_count, 0) > 0 THEN 'multi_source_verified'
                            ELSE 'multi_source_pending_validation' END AS quality_status,
                       CAST(c.as_of AS VARCHAR) AS rebuilt_at
                FROM current_f f CROSS JOIN cutoff c
                LEFT JOIN latest_tdx t USING (symbol)
                LEFT JOIN annual_agg a USING (symbol)
                LEFT JOIN latest_annual la USING (symbol)
                LEFT JOIN latest_bao b USING (symbol)
                LEFT JOIN validation_agg v ON v.symbol = t.symbol AND v.report_period = t.report_period
            ) SELECT * FROM pit
            """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY roe_annual_median DESC NULLS LAST, symbol LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        with self._lock, self.connect() as con:
            return self._rows(con, sql, params)
