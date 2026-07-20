from __future__ import annotations


POSTGRES_MIGRATIONS = [
    (
        1,
        "control plane, calendars, Tushare rows, and artifact manifests",
        """
        CREATE TABLE IF NOT EXISTS ingestion_runs (
            run_id TEXT PRIMARY KEY, job_name TEXT NOT NULL, status TEXT NOT NULL,
            report_period TEXT, started_at TEXT NOT NULL, finished_at TEXT,
            row_count BIGINT NOT NULL DEFAULT 0, error TEXT
        );
        CREATE TABLE IF NOT EXISTS provider_health (
            provider TEXT PRIMARY KEY, status TEXT NOT NULL, last_attempt_at TEXT NOT NULL,
            last_success_at TEXT, last_error TEXT, consecutive_failures INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS raw_artifact_manifest (
            artifact_id TEXT PRIMARY KEY, dataset TEXT NOT NULL, source TEXT NOT NULL,
            source_url TEXT, observed_at TEXT NOT NULL, ingested_at TEXT NOT NULL,
            raw_response_locator TEXT, storage_path TEXT NOT NULL, sha256 TEXT NOT NULL,
            byte_size BIGINT NOT NULL, metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
        );
        CREATE INDEX IF NOT EXISTS raw_artifact_dataset_ingested_idx
            ON raw_artifact_manifest (dataset, ingested_at DESC);
        CREATE TABLE IF NOT EXISTS economic_calendar_event (
            event_id TEXT PRIMARY KEY, country TEXT, event_date TEXT, event_time TEXT,
            timezone TEXT, scheduled_at TEXT, event_name TEXT, impact TEXT, actual TEXT,
            estimate TEXT, previous TEXT, unit TEXT, source TEXT, source_url TEXT,
            observed_at TEXT, ingested_at TEXT, raw_response_locator TEXT, raw_path TEXT,
            raw_artifact_id TEXT, payload_json JSONB NOT NULL DEFAULT '{}'::jsonb
        );
        CREATE INDEX IF NOT EXISTS economic_calendar_range_idx
            ON economic_calendar_event (event_date, country);
        CREATE TABLE IF NOT EXISTS economic_indicator_latest (
            indicator_id TEXT PRIMARY KEY, country TEXT, name TEXT, source TEXT,
            source_series_id TEXT, period TEXT, value DOUBLE PRECISION,
            previous_value DOUBLE PRECISION, change_value DOUBLE PRECISION,
            change_pct DOUBLE PRECISION, unit TEXT, frequency TEXT, latest_date TEXT,
            source_url TEXT, observed_at TEXT, ingested_at TEXT,
            raw_response_locator TEXT, raw_path TEXT, raw_artifact_id TEXT,
            payload_json JSONB NOT NULL DEFAULT '{}'::jsonb
        );
        CREATE TABLE IF NOT EXISTS earnings_calendar_event (
            event_id TEXT PRIMARY KEY, market TEXT, symbol TEXT, name TEXT,
            report_date TEXT, report_time TEXT, timezone TEXT, scheduled_at TEXT,
            fiscal_period TEXT, eps_forecast TEXT, previous_eps TEXT, source TEXT,
            source_url TEXT, observed_at TEXT, ingested_at TEXT,
            raw_response_locator TEXT, raw_path TEXT, raw_artifact_id TEXT,
            payload_json JSONB NOT NULL DEFAULT '{}'::jsonb
        );
        CREATE INDEX IF NOT EXISTS earnings_calendar_range_idx
            ON earnings_calendar_event (report_date, market, symbol);
        CREATE TABLE IF NOT EXISTS tushare_request (
            request_id TEXT PRIMARY KEY, api_name TEXT NOT NULL, params_json JSONB NOT NULL,
            requested_fields TEXT, response_fields_json JSONB NOT NULL,
            response_code INTEGER, response_message TEXT, row_count BIGINT NOT NULL,
            source TEXT, source_url TEXT, observed_at TEXT, ingested_at TEXT,
            raw_path TEXT, raw_artifact_id TEXT
        );
        CREATE TABLE IF NOT EXISTS tushare_data_row (
            request_id TEXT NOT NULL REFERENCES tushare_request(request_id) ON DELETE CASCADE,
            row_index BIGINT NOT NULL, api_name TEXT NOT NULL, symbol TEXT, data_date TEXT,
            source TEXT, source_url TEXT, observed_at TEXT, ingested_at TEXT,
            payload_json JSONB NOT NULL, PRIMARY KEY (request_id, row_index)
        );
        CREATE INDEX IF NOT EXISTS tushare_rows_api_idx
            ON tushare_data_row (api_name, symbol, data_date);
        """,
    ),
    (
        2,
        "fundamental snapshots, immutable history, and financial statement rows",
        """
        CREATE TABLE IF NOT EXISTS fundamental_snapshot (
            instrument_id TEXT, symbol TEXT NOT NULL, exchange TEXT, name TEXT,
            is_active BOOLEAN, report_period TEXT NOT NULL, published_at TEXT,
            valuation_as_of TEXT, price DOUBLE PRECISION, change_pct DOUBLE PRECISION,
            pe_dynamic DOUBLE PRECISION, pb DOUBLE PRECISION,
            total_market_cap DOUBLE PRECISION, float_market_cap DOUBLE PRECISION,
            roe_weighted DOUBLE PRECISION, eps DOUBLE PRECISION, revenue DOUBLE PRECISION,
            revenue_yoy DOUBLE PRECISION, revenue_qoq DOUBLE PRECISION,
            net_profit DOUBLE PRECISION, net_profit_yoy DOUBLE PRECISION,
            net_profit_qoq DOUBLE PRECISION, book_value_per_share DOUBLE PRECISION,
            ocf_per_share DOUBLE PRECISION, gross_margin DOUBLE PRECISION, industry TEXT,
            cash DOUBLE PRECISION, accounts_receivable DOUBLE PRECISION,
            inventory DOUBLE PRECISION, total_assets DOUBLE PRECISION,
            total_assets_yoy DOUBLE PRECISION, accounts_payable DOUBLE PRECISION,
            advance_receipts DOUBLE PRECISION, total_liabilities DOUBLE PRECISION,
            total_liabilities_yoy DOUBLE PRECISION, debt_ratio DOUBLE PRECISION,
            total_equity DOUBLE PRECISION, operating_cost DOUBLE PRECISION,
            sales_expense DOUBLE PRECISION, admin_expense DOUBLE PRECISION,
            financial_expense DOUBLE PRECISION, total_operating_expense DOUBLE PRECISION,
            operating_profit DOUBLE PRECISION, total_profit DOUBLE PRECISION,
            net_cashflow DOUBLE PRECISION, net_cashflow_yoy DOUBLE PRECISION,
            operating_cashflow DOUBLE PRECISION, investing_cashflow DOUBLE PRECISION,
            financing_cashflow DOUBLE PRECISION, source TEXT, source_url TEXT,
            observed_at TEXT, ingested_at TEXT, raw_response_locator TEXT, raw_path TEXT,
            raw_artifact_id TEXT, quality_status TEXT, fetched_at TEXT,
            PRIMARY KEY (symbol, report_period)
        );
        CREATE INDEX IF NOT EXISTS fundamental_snapshot_period_idx
            ON fundamental_snapshot (report_period, symbol);
        CREATE TABLE IF NOT EXISTS fundamental_snapshot_history (
            LIKE fundamental_snapshot INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING IDENTITY
        );
        ALTER TABLE fundamental_snapshot_history ADD COLUMN IF NOT EXISTS version_id TEXT;
        CREATE UNIQUE INDEX IF NOT EXISTS fundamental_history_version_idx
            ON fundamental_snapshot_history (version_id);
        CREATE INDEX IF NOT EXISTS fundamental_history_pit_idx
            ON fundamental_snapshot_history (symbol, report_period, published_at, ingested_at);
        CREATE TABLE IF NOT EXISTS financial_statement_rows (
            instrument_id TEXT, symbol TEXT NOT NULL, statement TEXT NOT NULL,
            report_date TEXT NOT NULL, published_at TEXT, source TEXT NOT NULL,
            payload_json JSONB NOT NULL, fetched_at TEXT, source_url TEXT,
            observed_at TEXT, ingested_at TEXT, raw_response_locator TEXT,
            raw_path TEXT, raw_artifact_id TEXT,
            PRIMARY KEY (symbol, statement, report_date, source)
        );
        CREATE INDEX IF NOT EXISTS financial_statement_pit_idx
            ON financial_statement_rows (symbol, report_date, published_at, ingested_at);
        """,
    ),
    (
        3,
        "BaoStock snapshots and immutable TDX financial history",
        """
        CREATE TABLE IF NOT EXISTS baostock_snapshot (
            symbol TEXT NOT NULL, report_period TEXT NOT NULL, published_at TEXT,
            trade_date TEXT, close DOUBLE PRECISION, pe_ttm DOUBLE PRECISION,
            pb_mrq DOUBLE PRECISION, ps_ttm DOUBLE PRECISION, pcf_ncf_ttm DOUBLE PRECISION,
            trade_status INTEGER, is_st BOOLEAN, roe_avg DOUBLE PRECISION,
            net_margin DOUBLE PRECISION, gross_margin DOUBLE PRECISION,
            net_profit_all DOUBLE PRECISION, eps_ttm DOUBLE PRECISION,
            total_share DOUBLE PRECISION, current_ratio DOUBLE PRECISION,
            quick_ratio DOUBLE PRECISION, liability_to_asset DOUBLE PRECISION,
            asset_turnover DOUBLE PRECISION, inventory_turnover DOUBLE PRECISION,
            net_profit_yoy DOUBLE PRECISION, equity_yoy DOUBLE PRECISION,
            asset_yoy DOUBLE PRECISION, cfo_to_revenue DOUBLE PRECISION,
            cfo_to_net_profit DOUBLE PRECISION, dupont_roe DOUBLE PRECISION,
            payload_json JSONB NOT NULL DEFAULT '{}'::jsonb, fetched_at TEXT,
            source TEXT, source_url TEXT, observed_at TEXT, ingested_at TEXT,
            raw_response_locator TEXT, raw_path TEXT, raw_artifact_id TEXT,
            PRIMARY KEY (symbol, report_period)
        );
        CREATE TABLE IF NOT EXISTS tdx_financial_snapshot (
            symbol TEXT NOT NULL, report_period TEXT NOT NULL, published_at TEXT,
            roe_weighted DOUBLE PRECISION, eps DOUBLE PRECISION,
            eps_adjusted DOUBLE PRECISION, book_value_per_share DOUBLE PRECISION,
            ocf_per_share DOUBLE PRECISION, cash DOUBLE PRECISION,
            accounts_receivable DOUBLE PRECISION, inventory DOUBLE PRECISION,
            total_assets DOUBLE PRECISION, total_liabilities DOUBLE PRECISION,
            total_equity DOUBLE PRECISION, revenue DOUBLE PRECISION,
            revenue_ttm DOUBLE PRECISION, net_profit_parent DOUBLE PRECISION,
            net_profit_parent_ttm DOUBLE PRECISION, operating_cashflow DOUBLE PRECISION,
            capex DOUBLE PRECISION, source_file TEXT, source TEXT, source_url TEXT,
            observed_at TEXT, ingested_at TEXT, raw_response_locator TEXT,
            raw_path TEXT, raw_artifact_id TEXT, fetched_at TEXT,
            PRIMARY KEY (symbol, report_period)
        );
        CREATE TABLE IF NOT EXISTS tdx_financial_snapshot_history (
            LIKE tdx_financial_snapshot INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING IDENTITY
        );
        ALTER TABLE tdx_financial_snapshot_history ADD COLUMN IF NOT EXISTS version_id TEXT;
        CREATE UNIQUE INDEX IF NOT EXISTS tdx_history_version_idx
            ON tdx_financial_snapshot_history (version_id);
        CREATE INDEX IF NOT EXISTS tdx_history_pit_idx
            ON tdx_financial_snapshot_history
                (symbol, report_period, published_at, ingested_at);
        """,
    ),
]
