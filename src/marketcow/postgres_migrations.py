from __future__ import annotations


# Authoritative BG-003 inventory. The first eighteen domains are compatible with the
# The final two domains hold runtime configuration and migration checkpoints.
POSTGRES_TRANSACTION_DOMAINS = (
    "ingestion_runs",
    "provider_health",
    "raw_artifact_manifest",
    "economic_calendar_event",
    "economic_indicator_latest",
    "earnings_calendar_event",
    "tushare_request",
    "tushare_data_row",
    "fundamental_snapshot",
    "fundamental_snapshot_history",
    "financial_statement_rows",
    "baostock_snapshot",
    "tdx_financial_snapshot",
    "tdx_financial_snapshot_history",
    "validation_result",
    "funnel_metrics",
    "dividend_announcement",
    "dividend_refresh_state",
    "instrument_master",
    "runtime_config_version",
    "migration_checkpoint",
)


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
    (
        4,
        "cross-source validation results and fundamental funnel metrics",
        """
        CREATE TABLE IF NOT EXISTS validation_result (
            symbol TEXT NOT NULL, report_period TEXT NOT NULL, metric TEXT NOT NULL,
            source_a TEXT NOT NULL, source_b TEXT NOT NULL,
            value_a DOUBLE PRECISION, value_b DOUBLE PRECISION,
            difference_pct DOUBLE PRECISION, status TEXT NOT NULL, observed_at TEXT,
            PRIMARY KEY (symbol, report_period, metric, source_a, source_b)
        );
        CREATE INDEX IF NOT EXISTS validation_result_period_idx
            ON validation_result (report_period, symbol);
        CREATE TABLE IF NOT EXISTS funnel_metrics (
            symbol TEXT PRIMARY KEY, name TEXT, is_active BOOLEAN,
            latest_report_period TEXT, published_at TEXT,
            pe_ttm DOUBLE PRECISION, pe_dynamic DOUBLE PRECISION,
            pe_for_filter DOUBLE PRECISION, pe_source TEXT,
            pb DOUBLE PRECISION, pb_source TEXT, roe_latest DOUBLE PRECISION,
            roe_annual_median DOUBLE PRECISION, roe_annual_min DOUBLE PRECISION,
            annual_period_count BIGINT NOT NULL DEFAULT 0,
            revenue_ttm DOUBLE PRECISION, net_profit_parent_ttm DOUBLE PRECISION,
            revenue_cagr_pct DOUBLE PRECISION, net_profit_cagr_pct DOUBLE PRECISION,
            debt_ratio_latest DOUBLE PRECISION, cash_to_assets_pct DOUBLE PRECISION,
            fcf_latest_period DOUBLE PRECISION, latest_annual_period TEXT,
            operating_cashflow_latest_annual DOUBLE PRECISION,
            fcf_latest_annual DOUBLE PRECISION,
            ocf_to_net_profit_latest_annual_pct DOUBLE PRECISION,
            ocf_to_net_profit_annual_sum_pct DOUBLE PRECISION,
            history_start_period TEXT, history_end_period TEXT,
            quality_status TEXT NOT NULL, rebuilt_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS funnel_metrics_filter_idx
            ON funnel_metrics (is_active, roe_annual_median DESC, symbol);
        """,
    ),
    (
        5,
        "current runtime configuration versions and migration checkpoints",
        """
        ALTER TABLE ingestion_runs
            ADD CONSTRAINT ingestion_runs_row_count_nonnegative
            CHECK (row_count >= 0) NOT VALID;
        ALTER TABLE provider_health
            ADD CONSTRAINT provider_health_failures_nonnegative
            CHECK (consecutive_failures >= 0) NOT VALID;
        ALTER TABLE raw_artifact_manifest
            ADD CONSTRAINT raw_artifact_manifest_byte_size_nonnegative
            CHECK (byte_size >= 0) NOT VALID;
        ALTER TABLE ingestion_runs
            VALIDATE CONSTRAINT ingestion_runs_row_count_nonnegative;
        ALTER TABLE provider_health
            VALIDATE CONSTRAINT provider_health_failures_nonnegative;
        ALTER TABLE raw_artifact_manifest
            VALIDATE CONSTRAINT raw_artifact_manifest_byte_size_nonnegative;

        CREATE TABLE IF NOT EXISTS runtime_config_version (
            config_id TEXT NOT NULL,
            version BIGINT NOT NULL CHECK (version > 0),
            profile TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            config_json JSONB NOT NULL,
            config_sha256 TEXT NOT NULL CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
            observed_at TIMESTAMPTZ NOT NULL,
            actor TEXT NOT NULL,
            PRIMARY KEY (config_id, version),
            UNIQUE (config_id, config_sha256)
        );
        CREATE INDEX IF NOT EXISTS runtime_config_version_pit_idx
            ON runtime_config_version (config_id, observed_at DESC, version DESC);

        CREATE TABLE IF NOT EXISTS migration_checkpoint (
            run_id TEXT NOT NULL,
            domain TEXT NOT NULL,
            shard TEXT NOT NULL DEFAULT '',
            revision BIGINT NOT NULL CHECK (revision > 0),
            status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
            source_watermark TEXT,
            target_watermark TEXT,
            cursor_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            error TEXT,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (run_id, domain, shard)
        );
        CREATE INDEX IF NOT EXISTS migration_checkpoint_status_idx
            ON migration_checkpoint (status, updated_at, run_id, domain, shard);
        """,
    ),
    (
        6,
        "source-qualified dividend announcements",
        """
        CREATE TABLE IF NOT EXISTS dividend_announcement (
            dividend_id TEXT PRIMARY KEY,
            instrument_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            market TEXT NOT NULL CHECK (market IN ('CN', 'HK', 'US')),
            exchange TEXT NOT NULL,
            fiscal_year INTEGER NOT NULL CHECK (fiscal_year BETWEEN 1990 AND 2100),
            amount_per_share NUMERIC NOT NULL CHECK (amount_per_share >= 0),
            currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
            announcement_date DATE NOT NULL,
            expected_payment_date DATE,
            confirmation_status TEXT NOT NULL
                CHECK (confirmation_status IN ('confirmed', 'unverified')),
            event_status TEXT NOT NULL CHECK (event_status IN ('active', 'cancelled')),
            source_type TEXT NOT NULL CHECK (source_type IN (
                'fund_manager', 'issuer_announcement', 'exchange_announcement',
                'ir_filing', 'regulatory_filing', 'third_party'
            )),
            source_priority INTEGER NOT NULL CHECK (source_priority BETWEEN 1 AND 9),
            source_name TEXT,
            source_url TEXT,
            source_document_id TEXT,
            observed_at TIMESTAMPTZ NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            raw_artifact_id TEXT,
            payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            CONSTRAINT dividend_third_party_not_confirmed CHECK (
                source_type <> 'third_party' OR confirmation_status <> 'confirmed'
            ),
            CONSTRAINT dividend_confirmed_has_evidence CHECK (
                confirmation_status <> 'confirmed'
                OR (source_url <> '' AND source_document_id <> '')
            )
        );
        CREATE INDEX IF NOT EXISTS dividend_symbol_year_idx
            ON dividend_announcement (symbol, fiscal_year, announcement_date);
        """,
    ),
    (
        7,
        "durable dividend refresh state",
        """
        CREATE TABLE IF NOT EXISTS dividend_refresh_state (
            symbol TEXT NOT NULL,
            fiscal_year INTEGER NOT NULL CHECK (fiscal_year BETWEEN 1990 AND 2100),
            status TEXT NOT NULL CHECK (status IN ('refreshing', 'success', 'failed')),
            last_attempt_at TIMESTAMPTZ NOT NULL,
            last_success_at TIMESTAMPTZ,
            last_error TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (symbol, fiscal_year)
        );
        CREATE INDEX IF NOT EXISTS dividend_refresh_state_success_idx
            ON dividend_refresh_state (last_success_at DESC);
        """,
    ),
    (
        8,
        "version dividend refresh strategies",
        """
        ALTER TABLE dividend_refresh_state
            ADD COLUMN IF NOT EXISTS strategy_version TEXT NOT NULL
            DEFAULT 'official-pdf-v1';
        """,
    ),
    (
        9,
        "dividend entitlement and actual payment dates",
        """
        ALTER TABLE dividend_announcement
            ADD COLUMN IF NOT EXISTS record_date DATE,
            ADD COLUMN IF NOT EXISTS ex_date DATE,
            ADD COLUMN IF NOT EXISTS payment_date DATE,
            ADD COLUMN IF NOT EXISTS date_evidence_json JSONB
                NOT NULL DEFAULT '{}'::jsonb;
        """,
    ),
    (
        10,
        "versioned dividend cache completion states",
        """
        ALTER TABLE dividend_refresh_state
            DROP CONSTRAINT IF EXISTS dividend_refresh_state_status_check;
        ALTER TABLE dividend_refresh_state
            ADD COLUMN IF NOT EXISTS cache_schema_version TEXT NOT NULL
                DEFAULT 'dividend-cache-v1',
            ADD COLUMN IF NOT EXISTS parser_version TEXT NOT NULL
                DEFAULT 'official-pdf-v1',
            ADD COLUMN IF NOT EXISTS query_source TEXT NOT NULL DEFAULT '',
            ADD COLUMN IF NOT EXISTS result_count INTEGER
                CHECK (result_count IS NULL OR result_count >= 0),
            ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
        UPDATE dividend_refresh_state
        SET status = CASE
                WHEN status = 'success' THEN 'success_empty'
                WHEN status = 'failed' THEN 'failed_source'
                ELSE status
            END,
            cache_schema_version = 'dividend-cache-v1',
            parser_version = strategy_version
        WHERE status IN ('success', 'failed');
        ALTER TABLE dividend_refresh_state
            ADD CONSTRAINT dividend_refresh_state_status_check CHECK (
                status IN (
                    'refreshing', 'success_data', 'success_empty',
                    'failed_source', 'failed_rate_limited',
                    'failed_timeout', 'failed_parse'
                )
            );
        """,
    ),
    (
        11,
        "provider-neutral instrument master",
        """
        CREATE TABLE IF NOT EXISTS instrument_master (
            instrument_id TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL CHECK (schema_version = 1),
            instrument_type TEXT NOT NULL CHECK (instrument_type = 'equity'),
            asset_class TEXT NOT NULL CHECK (asset_class = 'equity'),
            symbol TEXT NOT NULL,
            market TEXT NOT NULL CHECK (market IN ('US', 'HK', 'CN')),
            mic TEXT NOT NULL CHECK (mic ~ '^[A-Z0-9]{4}$'),
            currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
            price_precision INTEGER NOT NULL CHECK (price_precision BETWEEN 0 AND 18),
            size_precision INTEGER NOT NULL CHECK (size_precision = 0),
            tick_size NUMERIC NOT NULL CHECK (tick_size > 0),
            size_increment NUMERIC NOT NULL CHECK (size_increment = 1),
            lot_size NUMERIC NOT NULL CHECK (lot_size > 0),
            ts_event TIMESTAMPTZ NOT NULL,
            ts_init TIMESTAMPTZ NOT NULL,
            provider_symbols JSONB NOT NULL,
            broker_symbols JSONB NOT NULL,
            content_hash TEXT NOT NULL CHECK (content_hash ~ '^sha256:[0-9a-f]{64}$'),
            updated_at TIMESTAMPTZ NOT NULL,
            UNIQUE (symbol, mic)
        );
        CREATE TABLE IF NOT EXISTS instrument_symbol_mapping (
            namespace TEXT NOT NULL,
            external_symbol TEXT NOT NULL,
            instrument_id TEXT NOT NULL REFERENCES instrument_master(instrument_id)
                ON DELETE CASCADE,
            PRIMARY KEY (namespace, external_symbol)
        );
        """,
    ),
]
