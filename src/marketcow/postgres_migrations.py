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
]
