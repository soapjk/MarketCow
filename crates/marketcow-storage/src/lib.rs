//! Narrow repository ports and an explicit workload router.
//! Realtime projection is always memory-owned and cannot route to SQLite or a remote DB.

use chrono::{DateTime, Utc};
use rusqlite::OptionalExtension;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::Mutex};
use thiserror::Error;

pub const PROVIDER_JOB_MIGRATION_VERSION: &str = "rust-provider-job-v1";
pub const PROVIDER_JOB_MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS provider_job (
    job_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'pending', 'claimed', 'running', 'succeeded',
        'failed_retryable', 'failed_terminal', 'canceled'
    )),
    revision BIGINT NOT NULL CHECK (revision > 0),
    deadline TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS provider_job_recovery_idx
    ON provider_job (status, deadline, created_at, job_id)
    WHERE status IN ('pending', 'claimed', 'running', 'failed_retryable');
"#;

const RUST_MIGRATION_TABLE_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS marketcow_rust_migration (
    version TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL,
    binary_commit TEXT NOT NULL,
    safe_forward BOOLEAN NOT NULL
);
"#;
const PROVIDER_JOB_MIGRATION_LOCK: i64 = 0x4d_43_4a_4f_42;

pub const CLICKHOUSE_QUOTE_MIGRATION_VERSION: &str = "rust-quote-latest-v1";
pub const CLICKHOUSE_QUOTE_MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS market_quote_latest (
    symbol String, payload_json String,
    observed_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'),
    source LowCardinality(String), content_rank String, content_version UInt256
) ENGINE = ReplacingMergeTree(content_version)
ORDER BY symbol
"#;
const CLICKHOUSE_RUST_MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS marketcow_rust_clickhouse_migration (
    version String, checksum String,
    applied_at DateTime64(3, 'UTC'), binary_commit String, safe_forward Bool
) ENGINE = ReplacingMergeTree(applied_at)
ORDER BY version
"#;

pub struct ClickHouseConfig {
    url: String,
    database: String,
    username: String,
    password: String,
}

impl ClickHouseConfig {
    pub fn new(
        url: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        let url = url.into();
        let parsed = url::Url::parse(&url).map_err(|_| RepositoryError::InvalidInput)?;
        if !matches!(parsed.scheme(), "http" | "https")
            || !parsed.host_str().is_some_and(|host| {
                host == "localhost"
                    || host
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())
            })
        {
            return Err(RepositoryError::InvalidInput);
        }
        let database = database.into();
        if !database.ends_with("_production")
            && !database.ends_with("_development")
            && !database.ends_with("_test")
            || database.is_empty()
            || !database.bytes().enumerate().all(|(index, byte)| {
                byte == b'_'
                    || byte.is_ascii_alphanumeric() && (index > 0 || byte.is_ascii_alphabetic())
            })
        {
            return Err(RepositoryError::InvalidInput);
        }
        Ok(Self {
            url,
            database,
            username: username.into(),
            password: password.into(),
        })
    }
}

#[derive(clickhouse::Row, Serialize)]
struct ClickHouseQuoteRow {
    symbol: String,
    payload_json: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    observed_at: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    ingested_at: DateTime<Utc>,
    source: String,
    content_rank: String,
    content_version: clickhouse::types::UInt256,
}

#[derive(clickhouse::Row, Deserialize)]
struct ClickHouseCurrentQuote {
    payload_json: String,
    content_version: clickhouse::types::UInt256,
}

#[derive(clickhouse::Row, Deserialize)]
struct ClickHousePayloadOnly {
    payload_json: String,
}

#[derive(clickhouse::Row, Deserialize)]
struct ClickHouseMigrationChecksum {
    checksum: String,
}

#[derive(clickhouse::Row, Serialize)]
struct ClickHouseMigrationRow {
    version: String,
    checksum: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    applied_at: DateTime<Utc>,
    binary_commit: String,
    safe_forward: bool,
}

/// Official typed ClickHouse client for the existing `market_quote_latest` contract. The payload
/// keeps decimal values as JSON strings and the deterministic UInt256 version matches Python's
/// `raw_content_version` bit layout.
pub struct ClickHouseQuoteRepository {
    client: clickhouse::Client,
    operation_lock: tokio::sync::Mutex<()>,
}

impl ClickHouseQuoteRepository {
    pub fn new(config: ClickHouseConfig) -> Self {
        let client = clickhouse::Client::default()
            .with_url(config.url)
            .with_database(config.database)
            .with_user(config.username)
            .with_password(config.password)
            .with_setting("max_execution_time", "5")
            .with_setting("async_insert", "0");
        Self {
            client,
            operation_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub async fn health_probe(&self) -> Result<(), RepositoryError> {
        self.client
            .query("SELECT 1")
            .execute()
            .await
            .map_err(|_| RepositoryError::Unavailable)
    }

    pub async fn migrate_safe_forward(&self, binary_commit: &str) -> Result<(), RepositoryError> {
        if binary_commit.is_empty() || binary_commit.len() > 128 {
            return Err(RepositoryError::InvalidInput);
        }
        let _guard = self.operation_lock.lock().await;
        let checksum = hex::encode(Sha256::digest(CLICKHOUSE_QUOTE_MIGRATION_SQL.as_bytes()));
        self.client
            .query(CLICKHOUSE_RUST_MIGRATION_SQL)
            .execute()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        let existing = self
            .client
            .query(
                "SELECT argMax(checksum, applied_at) AS checksum \
                 FROM marketcow_rust_clickhouse_migration WHERE version = ?",
            )
            .bind(CLICKHOUSE_QUOTE_MIGRATION_VERSION)
            .fetch_one::<ClickHouseMigrationChecksum>()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        if !existing.checksum.is_empty() {
            return if existing.checksum == checksum {
                Ok(())
            } else {
                Err(RepositoryError::MigrationChecksumMismatch)
            };
        }
        self.client
            .query(CLICKHOUSE_QUOTE_MIGRATION_SQL)
            .execute()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        let mut insert = self
            .client
            .insert::<ClickHouseMigrationRow>("marketcow_rust_clickhouse_migration")
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        insert
            .write(&ClickHouseMigrationRow {
                version: CLICKHOUSE_QUOTE_MIGRATION_VERSION.into(),
                checksum,
                applied_at: Utc::now(),
                binary_commit: binary_commit.into(),
                safe_forward: true,
            })
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        insert.end().await.map_err(|_| RepositoryError::Unavailable)
    }

    pub async fn upsert_quote(
        &self,
        quote: &QuoteRecord,
        ingested_at: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        validate_quote(quote)?;
        let _guard = self.operation_lock.lock().await;
        let payload_json =
            serde_json::to_string(quote).map_err(|_| RepositoryError::InvalidInput)?;
        let digest = Sha256::digest(payload_json.as_bytes());
        let content_rank = hex::encode(&digest[..26]);
        let computed = clickhouse_content_version(ingested_at, &content_rank)?;
        let current = self
            .client
            .query(
                "SELECT argMax(payload_json, content_version) AS payload_json, \
                 max(content_version) AS content_version \
                 FROM market_quote_latest WHERE symbol = ?",
            )
            .bind(&quote.instrument_id)
            .fetch_one::<ClickHouseCurrentQuote>()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        if current.payload_json == payload_json {
            return Ok(false);
        }
        let version = if computed > current.content_version {
            computed
        } else {
            increment_uint256(current.content_version).ok_or(RepositoryError::InvalidInput)?
        };
        let mut insert = self
            .client
            .insert::<ClickHouseQuoteRow>("market_quote_latest")
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        insert
            .write(&ClickHouseQuoteRow {
                symbol: quote.instrument_id.clone(),
                payload_json,
                observed_at: quote.observed_at,
                ingested_at,
                source: quote.source.clone(),
                content_rank,
                content_version: version,
            })
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        insert
            .end()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        Ok(true)
    }

    pub async fn latest(
        &self,
        instrument_id: &str,
    ) -> Result<Option<QuoteRecord>, RepositoryError> {
        if instrument_id.is_empty() {
            return Err(RepositoryError::InvalidInput);
        }
        let row = self
            .client
            .query(
                "SELECT argMax(payload_json, content_version) AS payload_json \
                 FROM market_quote_latest FINAL WHERE symbol = ?",
            )
            .bind(instrument_id)
            .fetch_one::<ClickHousePayloadOnly>()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        if row.payload_json.is_empty() {
            Ok(None)
        } else {
            serde_json::from_str(&row.payload_json)
                .map(Some)
                .map_err(|_| RepositoryError::Unavailable)
        }
    }
}

fn validate_quote(quote: &QuoteRecord) -> Result<(), RepositoryError> {
    if quote.instrument_id.is_empty()
        || quote.currency.is_empty()
        || quote.source.is_empty()
        || quote.raw_sha256.len() != 64
        || !quote
            .raw_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || quote.bid > quote.ask
    {
        return Err(RepositoryError::InvalidInput);
    }
    Ok(())
}

fn clickhouse_content_version(
    ingested_at: DateTime<Utc>,
    content_rank: &str,
) -> Result<clickhouse::types::UInt256, RepositoryError> {
    if content_rank.len() != 52 || !content_rank.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(RepositoryError::InvalidInput);
    }
    let epoch_millis = ingested_at.timestamp_millis();
    if !(0..(1_i64 << 48)).contains(&epoch_millis) {
        return Err(RepositoryError::InvalidInput);
    }
    let rank = hex::decode(content_rank).map_err(|_| RepositoryError::InvalidInput)?;
    let mut little_endian = [0_u8; 32];
    for (target, source) in little_endian[..26].iter_mut().zip(rank.iter().rev()) {
        *target = *source;
    }
    let timestamp = (epoch_millis as u64).to_le_bytes();
    little_endian[26..].copy_from_slice(&timestamp[..6]);
    Ok(clickhouse::types::UInt256::from_le_bytes(little_endian))
}

fn increment_uint256(value: clickhouse::types::UInt256) -> Option<clickhouse::types::UInt256> {
    let mut bytes = value.to_le_bytes();
    for byte in &mut bytes {
        let (next, overflow) = byte.overflowing_add(1);
        *byte = next;
        if !overflow {
            return Some(clickhouse::types::UInt256::from_le_bytes(bytes));
        }
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    MemoryProjection,
    PostgreSql,
    ClickHouse,
    SqliteDerived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Workload {
    RealtimeProjection,
    Instrument,
    Job,
    Audit,
    ArtifactManifest,
    Fundamental,
    MarketBar,
    QuoteHistory,
    MigrationCheckpoint,
    DerivedOfflineIndex,
}

pub fn backend_for(workload: Workload) -> Backend {
    match workload {
        Workload::RealtimeProjection => Backend::MemoryProjection,
        Workload::Instrument
        | Workload::Job
        | Workload::Audit
        | Workload::ArtifactManifest
        | Workload::Fundamental
        | Workload::MigrationCheckpoint => Backend::PostgreSql,
        Workload::MarketBar | Workload::QuoteHistory => Backend::ClickHouse,
        Workload::DerivedOfflineIndex => Backend::SqliteDerived,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuoteRecord {
    pub instrument_id: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub bid: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub ask: Decimal,
    pub currency: String,
    pub scale: u32,
    pub observed_at: DateTime<Utc>,
    pub source: String,
    pub raw_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyKey(pub String);

pub trait InstrumentRepository: Send + Sync {
    fn exists(&self, instrument_id: &str) -> Result<bool, RepositoryError>;
}
pub trait JobRepository: Send + Sync {
    fn create_once(&self, key: &IdempotencyKey) -> Result<String, RepositoryError>;
}
pub trait AuditRepository: Send + Sync {
    fn append(&self, actor: &str, action: &str) -> Result<(), RepositoryError>;
}
pub trait ArtifactManifestRepository: Send + Sync {
    fn register_verified(&self, sha256: &str, size: u64) -> Result<(), RepositoryError>;
}
pub trait FundamentalRepository: Send + Sync {}
pub trait MarketBarRepository: Send + Sync {}
pub trait QuoteRepository: Send + Sync {
    fn latest(&self, instrument_id: &str) -> Result<Option<QuoteRecord>, RepositoryError>;
}
pub trait MigrationCheckpointRepository: Send + Sync {}
pub trait DerivedIndexRepository: Send + Sync {
    fn rebuild(&self, persisted_cursor: u64) -> Result<(), RepositoryError>;
}

/// Disposable SQLite index for local/offline tools. It deliberately does not implement
/// `QuoteRepository`, so realtime reads cannot be wired to it by satisfying that port.
pub struct SqliteDerivedIndexRepository {
    connection: Mutex<rusqlite::Connection>,
}

impl SqliteDerivedIndexRepository {
    pub fn open(path: &Path) -> Result<Self, RepositoryError> {
        if !path.is_absolute() || path == Path::new("/") {
            return Err(RepositoryError::InvalidInput);
        }
        let connection =
            rusqlite::Connection::open(path).map_err(|_| RepositoryError::Unavailable)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 CREATE TABLE IF NOT EXISTS derived_index_metadata (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    persisted_cursor INTEGER NOT NULL CHECK (persisted_cursor >= 0),
                    rebuilt_at TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS derived_quote_index (
                    instrument_id TEXT PRIMARY KEY,
                    bid TEXT NOT NULL,
                    ask TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    scale INTEGER NOT NULL CHECK (scale >= 0),
                    observed_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    raw_sha256 TEXT NOT NULL
                 );",
            )
            .map_err(|_| RepositoryError::Unavailable)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn put_quote(&self, quote: &QuoteRecord) -> Result<(), RepositoryError> {
        if quote.instrument_id.is_empty()
            || quote.currency.is_empty()
            || quote.source.is_empty()
            || quote.raw_sha256.len() != 64
            || quote.bid > quote.ask
        {
            return Err(RepositoryError::InvalidInput);
        }
        self.connection
            .lock()
            .map_err(|_| RepositoryError::Unavailable)?
            .execute(
                "INSERT INTO derived_quote_index
                 (instrument_id,bid,ask,currency,scale,observed_at,source,raw_sha256)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
                 ON CONFLICT(instrument_id) DO UPDATE SET
                 bid=excluded.bid,ask=excluded.ask,currency=excluded.currency,scale=excluded.scale,
                 observed_at=excluded.observed_at,source=excluded.source,raw_sha256=excluded.raw_sha256",
                rusqlite::params![
                    quote.instrument_id,
                    quote.bid.to_string(),
                    quote.ask.to_string(),
                    quote.currency,
                    quote.scale,
                    quote.observed_at.to_rfc3339(),
                    quote.source,
                    quote.raw_sha256,
                ],
            )
            .map_err(|_| RepositoryError::Unavailable)?;
        Ok(())
    }

    pub fn quote(&self, instrument_id: &str) -> Result<Option<QuoteRecord>, RepositoryError> {
        if instrument_id.is_empty() {
            return Err(RepositoryError::InvalidInput);
        }
        let connection = self
            .connection
            .lock()
            .map_err(|_| RepositoryError::Unavailable)?;
        let mut statement = connection
            .prepare(
                "SELECT bid,ask,currency,scale,observed_at,source,raw_sha256
                 FROM derived_quote_index WHERE instrument_id=?1",
            )
            .map_err(|_| RepositoryError::Unavailable)?;
        let mut rows = statement
            .query([instrument_id])
            .map_err(|_| RepositoryError::Unavailable)?;
        let Some(row) = rows.next().map_err(|_| RepositoryError::Unavailable)? else {
            return Ok(None);
        };
        let bid: String = row.get(0).map_err(|_| RepositoryError::Unavailable)?;
        let ask: String = row.get(1).map_err(|_| RepositoryError::Unavailable)?;
        let observed_at: String = row.get(4).map_err(|_| RepositoryError::Unavailable)?;
        Ok(Some(QuoteRecord {
            instrument_id: instrument_id.into(),
            bid: bid.parse().map_err(|_| RepositoryError::Unavailable)?,
            ask: ask.parse().map_err(|_| RepositoryError::Unavailable)?,
            currency: row.get(2).map_err(|_| RepositoryError::Unavailable)?,
            scale: row.get(3).map_err(|_| RepositoryError::Unavailable)?,
            observed_at: DateTime::parse_from_rfc3339(&observed_at)
                .map_err(|_| RepositoryError::Unavailable)?
                .with_timezone(&Utc),
            source: row.get(5).map_err(|_| RepositoryError::Unavailable)?,
            raw_sha256: row.get(6).map_err(|_| RepositoryError::Unavailable)?,
        }))
    }

    pub fn persisted_cursor(&self) -> Result<Option<u64>, RepositoryError> {
        let value = self
            .connection
            .lock()
            .map_err(|_| RepositoryError::Unavailable)?
            .query_row(
                "SELECT persisted_cursor FROM derived_index_metadata WHERE singleton=1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(|_| RepositoryError::Unavailable)?;
        value
            .map(|cursor| u64::try_from(cursor).map_err(|_| RepositoryError::Unavailable))
            .transpose()
    }
}

impl DerivedIndexRepository for SqliteDerivedIndexRepository {
    fn rebuild(&self, persisted_cursor: u64) -> Result<(), RepositoryError> {
        let cursor = i64::try_from(persisted_cursor).map_err(|_| RepositoryError::InvalidInput)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RepositoryError::Unavailable)?;
        let transaction = connection
            .transaction()
            .map_err(|_| RepositoryError::Unavailable)?;
        transaction
            .execute("DELETE FROM derived_quote_index", [])
            .map_err(|_| RepositoryError::Unavailable)?;
        transaction
            .execute(
                "INSERT INTO derived_index_metadata(singleton,persisted_cursor,rebuilt_at)
                 VALUES(1,?1,?2) ON CONFLICT(singleton) DO UPDATE SET
                 persisted_cursor=excluded.persisted_cursor,rebuilt_at=excluded.rebuilt_at",
                rusqlite::params![cursor, Utc::now().to_rfc3339()],
            )
            .map_err(|_| RepositoryError::Unavailable)?;
        transaction
            .commit()
            .map_err(|_| RepositoryError::Unavailable)
    }
}

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("repository timeout")]
    Timeout,
    #[error("idempotency conflict")]
    IdempotencyConflict,
    #[error("invalid repository input")]
    InvalidInput,
    #[error("repository unavailable")]
    Unavailable,
    #[error("repository revision conflict")]
    RevisionConflict,
    #[error("migration checksum mismatch")]
    MigrationChecksumMismatch,
}

pub struct PostgresJobRepository {
    client: tokio::sync::Mutex<tokio_postgres::Client>,
}

impl PostgresJobRepository {
    pub async fn connect(dsn: &str) -> Result<Self, RepositoryError> {
        if dsn.trim().is_empty() {
            return Err(RepositoryError::InvalidInput);
        }
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(error = %error, "postgres_job_repository_connection_failed");
            }
        });
        Ok(Self {
            client: tokio::sync::Mutex::new(client),
        })
    }

    pub async fn migrate_safe_forward(&self, binary_commit: &str) -> Result<(), RepositoryError> {
        if binary_commit.is_empty() || binary_commit.len() > 128 {
            return Err(RepositoryError::InvalidInput);
        }
        let checksum = hex::encode(Sha256::digest(PROVIDER_JOB_MIGRATION_SQL.as_bytes()));
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock($1)",
                &[&PROVIDER_JOB_MIGRATION_LOCK],
            )
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        transaction
            .batch_execute(RUST_MIGRATION_TABLE_SQL)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        let existing = transaction
            .query_opt(
                "SELECT checksum FROM marketcow_rust_migration WHERE version = $1",
                &[&PROVIDER_JOB_MIGRATION_VERSION],
            )
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        if let Some(row) = existing {
            let recorded: String = row.get(0);
            if recorded != checksum {
                return Err(RepositoryError::MigrationChecksumMismatch);
            }
        } else {
            transaction
                .batch_execute(PROVIDER_JOB_MIGRATION_SQL)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            transaction
                .execute(
                    "INSERT INTO marketcow_rust_migration \
                     (version, checksum, applied_at, binary_commit, safe_forward) \
                     VALUES ($1, $2, NOW(), $3, TRUE)",
                    &[&PROVIDER_JOB_MIGRATION_VERSION, &checksum, &binary_commit],
                )
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| RepositoryError::Unavailable)
    }

    pub async fn insert_or_get(
        &self,
        job: &marketcow_jobs::ProviderJob,
    ) -> Result<(marketcow_jobs::ProviderJob, bool), RepositoryError> {
        validate_job_for_storage(job)?;
        let payload = serde_json::to_value(job).map_err(|_| RepositoryError::InvalidInput)?;
        let status = job_status_text(job.status);
        let revision = i64::try_from(job.revision).map_err(|_| RepositoryError::InvalidInput)?;
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        let inserted = transaction
            .execute(
                "INSERT INTO provider_job \
                 (job_id, idempotency_key, job_type, status, revision, deadline, payload, created_at, updated_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8) \
                 ON CONFLICT (idempotency_key) DO NOTHING",
                &[
                    &job.job_id,
                    &job.idempotency_key,
                    &job.job_type,
                    &status,
                    &revision,
                    &job.deadline,
                    &payload,
                    &job.created_at,
                ],
            )
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        let row = transaction
            .query_one(
                "SELECT payload FROM provider_job WHERE idempotency_key = $1 FOR SHARE",
                &[&job.idempotency_key],
            )
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        let stored = decode_job(row.get(0))?;
        transaction
            .commit()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        if stored.job_type != job.job_type
            || stored.request_schema != job.request_schema
            || stored.request_sha256 != job.request_sha256
        {
            return Err(RepositoryError::IdempotencyConflict);
        }
        Ok((stored, inserted == 1))
    }

    pub async fn compare_and_swap(
        &self,
        job: &marketcow_jobs::ProviderJob,
        expected_revision: u64,
    ) -> Result<(), RepositoryError> {
        validate_job_for_storage(job)?;
        if job.revision != expected_revision.saturating_add(1) {
            return Err(RepositoryError::InvalidInput);
        }
        let payload = serde_json::to_value(job).map_err(|_| RepositoryError::InvalidInput)?;
        let status = job_status_text(job.status);
        let revision = i64::try_from(job.revision).map_err(|_| RepositoryError::InvalidInput)?;
        let expected =
            i64::try_from(expected_revision).map_err(|_| RepositoryError::InvalidInput)?;
        let affected = self
            .client
            .lock()
            .await
            .execute(
                "UPDATE provider_job SET status=$2, revision=$3, deadline=$4, payload=$5, updated_at=NOW() \
                 WHERE job_id=$1 AND revision=$6",
                &[&job.job_id, &status, &revision, &job.deadline, &payload, &expected],
            )
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        if affected == 1 {
            Ok(())
        } else {
            Err(RepositoryError::RevisionConflict)
        }
    }

    pub async fn compare_and_swap_many(
        &self,
        updates: &[(marketcow_jobs::ProviderJob, u64)],
    ) -> Result<(), RepositoryError> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut ordered = updates.to_vec();
        ordered.sort_by(|left, right| left.0.job_id.cmp(&right.0.job_id));
        for (index, (job, expected_revision)) in ordered.iter().enumerate() {
            validate_job_for_storage(job)?;
            if job.revision != expected_revision.saturating_add(1)
                || index > 0 && ordered[index - 1].0.job_id == job.job_id
            {
                return Err(RepositoryError::InvalidInput);
            }
        }
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        for (job, expected_revision) in ordered {
            let payload = serde_json::to_value(&job).map_err(|_| RepositoryError::InvalidInput)?;
            let status = job_status_text(job.status);
            let revision =
                i64::try_from(job.revision).map_err(|_| RepositoryError::InvalidInput)?;
            let expected =
                i64::try_from(expected_revision).map_err(|_| RepositoryError::InvalidInput)?;
            let affected = transaction
                .execute(
                    "UPDATE provider_job SET status=$2, revision=$3, deadline=$4, payload=$5, updated_at=NOW() \
                     WHERE job_id=$1 AND revision=$6",
                    &[&job.job_id, &status, &revision, &job.deadline, &payload, &expected],
                )
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            if affected != 1 {
                return Err(RepositoryError::RevisionConflict);
            }
        }
        transaction
            .commit()
            .await
            .map_err(|_| RepositoryError::Unavailable)
    }

    pub async fn get(
        &self,
        job_id: &str,
    ) -> Result<Option<marketcow_jobs::ProviderJob>, RepositoryError> {
        if job_id.is_empty() {
            return Err(RepositoryError::InvalidInput);
        }
        self.client
            .lock()
            .await
            .query_opt(
                "SELECT payload FROM provider_job WHERE job_id=$1",
                &[&job_id],
            )
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .map(|row| decode_job(row.get(0)))
            .transpose()
    }

    pub async fn list_recoverable(
        &self,
        limit: i64,
    ) -> Result<Vec<marketcow_jobs::ProviderJob>, RepositoryError> {
        if !(1..=10_000).contains(&limit) {
            return Err(RepositoryError::InvalidInput);
        }
        self.client
            .lock()
            .await
            .query(
                "SELECT payload FROM provider_job \
                 WHERE status IN ('pending','claimed','running','failed_retryable') \
                 ORDER BY created_at, job_id LIMIT $1",
                &[&limit],
            )
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .into_iter()
            .map(|row| decode_job(row.get(0)))
            .collect()
    }

    pub async fn list_all(
        &self,
        limit: i64,
    ) -> Result<Vec<marketcow_jobs::ProviderJob>, RepositoryError> {
        if !(1..=10_001).contains(&limit) {
            return Err(RepositoryError::InvalidInput);
        }
        self.client
            .lock()
            .await
            .query(
                "SELECT payload FROM provider_job ORDER BY created_at, job_id LIMIT $1",
                &[&limit],
            )
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .into_iter()
            .map(|row| decode_job(row.get(0)))
            .collect()
    }
}

fn decode_job(value: serde_json::Value) -> Result<marketcow_jobs::ProviderJob, RepositoryError> {
    serde_json::from_value(value).map_err(|_| RepositoryError::Unavailable)
}

fn validate_job_for_storage(job: &marketcow_jobs::ProviderJob) -> Result<(), RepositoryError> {
    if job.job_id.is_empty()
        || job.idempotency_key.is_empty()
        || job.job_type.is_empty()
        || job.revision == 0
        || job.request_sha256.len() != 64
    {
        return Err(RepositoryError::InvalidInput);
    }
    Ok(())
}

fn job_status_text(status: marketcow_jobs::JobStatus) -> &'static str {
    match status {
        marketcow_jobs::JobStatus::Pending => "pending",
        marketcow_jobs::JobStatus::Claimed => "claimed",
        marketcow_jobs::JobStatus::Running => "running",
        marketcow_jobs::JobStatus::Succeeded => "succeeded",
        marketcow_jobs::JobStatus::FailedRetryable => "failed_retryable",
        marketcow_jobs::JobStatus::FailedTerminal => "failed_terminal",
        marketcow_jobs::JobStatus::Canceled => "canceled",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn every_workload_has_one_owner_and_hot_path_is_memory_only() {
        assert_eq!(
            backend_for(Workload::RealtimeProjection),
            Backend::MemoryProjection
        );
        assert_ne!(
            backend_for(Workload::RealtimeProjection),
            Backend::SqliteDerived
        );
        assert_eq!(
            backend_for(Workload::DerivedOfflineIndex),
            Backend::SqliteDerived
        );
        assert_eq!(backend_for(Workload::Instrument), Backend::PostgreSql);
        assert_eq!(backend_for(Workload::MarketBar), Backend::ClickHouse);
    }

    #[test]
    fn quote_decimal_json_round_trip_preserves_precision_and_currency() {
        let record = QuoteRecord {
            instrument_id: "POLY:condition:yes".into(),
            bid: "0.100000000000000001".parse().unwrap(),
            ask: "0.200000000000000002".parse().unwrap(),
            currency: "USDC".into(),
            scale: 18,
            observed_at: Utc::now(),
            source: "polymarket".into(),
            raw_sha256: "a".repeat(64),
        };
        let value = serde_json::to_value(&record).unwrap();
        assert_eq!(value["bid"], "0.100000000000000001");
        assert_eq!(value["currency"], "USDC");
        assert_eq!(
            serde_json::from_value::<QuoteRecord>(value).unwrap(),
            record
        );
    }

    #[test]
    fn postgres_job_migration_is_versioned_fenced_and_recoverable() {
        assert_eq!(PROVIDER_JOB_MIGRATION_VERSION, "rust-provider-job-v1");
        assert!(PROVIDER_JOB_MIGRATION_SQL.contains("idempotency_key TEXT NOT NULL UNIQUE"));
        assert!(PROVIDER_JOB_MIGRATION_SQL.contains("revision BIGINT NOT NULL"));
        assert!(PROVIDER_JOB_MIGRATION_SQL.contains("provider_job_recovery_idx"));
        assert!(PROVIDER_JOB_MIGRATION_SQL.contains("failed_retryable"));
        assert!(RUST_MIGRATION_TABLE_SQL.contains("checksum TEXT NOT NULL"));
        assert!(RUST_MIGRATION_TABLE_SQL.contains("binary_commit TEXT NOT NULL"));
        assert!(RUST_MIGRATION_TABLE_SQL.contains("safe_forward BOOLEAN NOT NULL"));
        assert_eq!(
            hex::encode(Sha256::digest(PROVIDER_JOB_MIGRATION_SQL.as_bytes())).len(),
            64
        );
    }

    #[test]
    fn sqlite_index_is_disposable_exact_and_rebuilt_from_authoritative_cursor() {
        let directory = tempfile::tempdir().unwrap();
        assert!(matches!(
            SqliteDerivedIndexRepository::open(Path::new("relative.sqlite")),
            Err(RepositoryError::InvalidInput)
        ));
        let repository =
            SqliteDerivedIndexRepository::open(&directory.path().join("derived.sqlite")).unwrap();
        let quote = QuoteRecord {
            instrument_id: "POLY:condition:yes".into(),
            bid: "0.100000000000000001".parse().unwrap(),
            ask: "0.200000000000000002".parse().unwrap(),
            currency: "USDC".into(),
            scale: 18,
            observed_at: Utc::now(),
            source: "polymarket".into(),
            raw_sha256: "a".repeat(64),
        };
        repository.put_quote(&quote).unwrap();
        assert_eq!(
            repository.quote(&quote.instrument_id).unwrap(),
            Some(quote.clone())
        );
        assert_eq!(repository.persisted_cursor().unwrap(), None);

        repository.rebuild(42).unwrap();
        assert_eq!(repository.persisted_cursor().unwrap(), Some(42));
        assert_eq!(repository.quote(&quote.instrument_id).unwrap(), None);

        let mut crossed = quote;
        crossed.bid = "0.3".parse().unwrap();
        assert!(matches!(
            repository.put_quote(&crossed),
            Err(RepositoryError::InvalidInput)
        ));
    }

    #[test]
    fn clickhouse_config_and_uint256_version_match_python_contract() {
        assert!(
            ClickHouseConfig::new("http://127.0.0.1:8123", "marketcow_test", "default", "").is_ok()
        );
        assert!(matches!(
            ClickHouseConfig::new("http://example.com:8123", "marketcow_test", "default", ""),
            Err(RepositoryError::InvalidInput)
        ));
        assert!(matches!(
            ClickHouseConfig::new("http://127.0.0.1:8123", "marketcow", "default", ""),
            Err(RepositoryError::InvalidInput)
        ));
        let at = DateTime::parse_from_rfc3339("2026-07-22T00:00:02Z")
            .unwrap()
            .with_timezone(&Utc);
        let version = clickhouse_content_version(at, &"ab".repeat(26)).unwrap();
        assert_eq!(
            version.to_string(),
            "734174110961207714005784373225550623274284546974944289204332051210571590571"
        );
        assert_eq!(
            increment_uint256(version).unwrap().to_string(),
            "734174110961207714005784373225550623274284546974944289204332051210571590572"
        );
        assert!(increment_uint256(clickhouse::types::UInt256::MAX).is_none());
        assert!(CLICKHOUSE_QUOTE_MIGRATION_SQL.contains("ReplacingMergeTree(content_version)"));
        assert!(CLICKHOUSE_RUST_MIGRATION_SQL.contains("binary_commit String"));
    }

    #[tokio::test]
    #[ignore = "requires MARKETCOW_TEST_CLICKHOUSE_URL and MARKETCOW_TEST_CLICKHOUSE_DATABASE"]
    async fn clickhouse_quote_round_trip_when_test_endpoint_is_configured() {
        let config = ClickHouseConfig::new(
            std::env::var("MARKETCOW_TEST_CLICKHOUSE_URL").expect("test ClickHouse URL"),
            std::env::var("MARKETCOW_TEST_CLICKHOUSE_DATABASE").expect("test ClickHouse database"),
            std::env::var("MARKETCOW_TEST_CLICKHOUSE_USERNAME")
                .unwrap_or_else(|_| "default".into()),
            std::env::var("MARKETCOW_TEST_CLICKHOUSE_PASSWORD").unwrap_or_default(),
        )
        .unwrap();
        let repository = ClickHouseQuoteRepository::new(config);
        repository.health_probe().await.unwrap();
        repository
            .migrate_safe_forward("test-binary-commit")
            .await
            .unwrap();
        let quote = QuoteRecord {
            instrument_id: format!("CLICKHOUSE.TEST.{}", uuid::Uuid::new_v4()),
            bid: "0.100000000000000001".parse().unwrap(),
            ask: "0.200000000000000002".parse().unwrap(),
            currency: "USDC".into(),
            scale: 18,
            observed_at: Utc::now(),
            source: "integration-test".into(),
            raw_sha256: "a".repeat(64),
        };
        assert!(repository.upsert_quote(&quote, Utc::now()).await.unwrap());
        assert!(!repository.upsert_quote(&quote, Utc::now()).await.unwrap());
        assert_eq!(
            repository.latest(&quote.instrument_id).await.unwrap(),
            Some(quote)
        );
    }

    #[tokio::test]
    #[ignore = "requires MARKETCOW_TEST_POSTGRES_DSN"]
    async fn postgres_job_repository_round_trip_when_test_dsn_is_configured() {
        let dsn = std::env::var("MARKETCOW_TEST_POSTGRES_DSN")
            .expect("set MARKETCOW_TEST_POSTGRES_DSN for the ignored integration test");
        let repository = PostgresJobRepository::connect(&dsn).await.unwrap();
        repository
            .migrate_safe_forward("test-binary-commit")
            .await
            .unwrap();
        let now = Utc::now();
        let mut engine = marketcow_jobs::JobEngine::default();
        let job = engine
            .submit(
                marketcow_jobs::SubmitJob {
                    idempotency_key: format!("rust-pg-test-{}", uuid::Uuid::new_v4()),
                    job_type: "provider.history".into(),
                    request_schema: "marketcow.provider.history.v1".into(),
                    request: serde_json::json!({"symbol":"AAPL.XNAS"}),
                    deadline: now + Duration::minutes(5),
                    max_attempts: 2,
                    audit_actor: "integration-test".into(),
                },
                now,
            )
            .unwrap()
            .clone();
        let (stored, created) = repository.insert_or_get(&job).await.unwrap();
        assert!(created);
        assert_eq!(stored, job);
        let (same, created_again) = repository.insert_or_get(&job).await.unwrap();
        assert!(!created_again);
        assert_eq!(same.job_id, job.job_id);

        let claimed = engine
            .claim(&job.job_id, "worker", Duration::seconds(30), now)
            .unwrap()
            .clone();
        repository.compare_and_swap(&claimed, 1).await.unwrap();
        assert_eq!(repository.get(&job.job_id).await.unwrap(), Some(claimed));
        let lease_token = engine
            .get(&job.job_id)
            .unwrap()
            .lease_token
            .clone()
            .unwrap();
        let running = engine
            .start(&job.job_id, &lease_token, now)
            .unwrap()
            .clone();
        repository
            .compare_and_swap_many(&[(running.clone(), 2)])
            .await
            .unwrap();
        assert_eq!(repository.get(&job.job_id).await.unwrap(), Some(running));
        assert!(
            repository
                .list_recoverable(100)
                .await
                .unwrap()
                .iter()
                .any(|candidate| candidate.job_id == job.job_id)
        );
        assert!(
            repository
                .list_all(100)
                .await
                .unwrap()
                .iter()
                .any(|candidate| candidate.job_id == job.job_id)
        );
    }
}
