//! Narrow repository ports and an explicit workload router.
//! Realtime projection is always memory-owned and cannot route to SQLite or a remote DB.

use chrono::{DateTime, Utc};
use rusqlite::OptionalExtension;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    sync::Mutex,
};
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

pub const ARTIFACT_MANIFEST_MIGRATION_VERSION: &str = "rust-artifact-manifest-v1";
pub const ARTIFACT_MANIFEST_MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS raw_artifact_manifest (
    artifact_id TEXT PRIMARY KEY,
    dataset TEXT NOT NULL,
    source TEXT NOT NULL,
    source_url TEXT,
    observed_at TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    raw_response_locator TEXT,
    storage_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    byte_size BIGINT NOT NULL CHECK (byte_size >= 0),
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS raw_artifact_dataset_ingested_idx
    ON raw_artifact_manifest (dataset, ingested_at DESC);
"#;

pub const INSTRUMENT_MASTER_MIGRATION_VERSION: &str = "rust-instrument-master-v1";
pub const INSTRUMENT_MASTER_MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS instrument_master (
    instrument_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    instrument_type TEXT NOT NULL CHECK (instrument_type IN (
        'equity', 'convertible_bond', 'crypto_spot', 'crypto_perpetual',
        'equity_perpetual', 'index_perpetual', 'hip3_perpetual'
    )),
    asset_class TEXT NOT NULL CHECK (asset_class IN (
        'equity', 'fixed_income', 'crypto', 'equity_derivative',
        'index_derivative', 'other_derivative'
    )),
    symbol TEXT NOT NULL,
    market TEXT NOT NULL CHECK (market IN ('US', 'HK', 'CN', 'CRYPTO')),
    mic TEXT NOT NULL CHECK (mic ~ '^[A-Z0-9]{4}$'),
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z0-9]{2,12}$'),
    price_precision INTEGER NOT NULL CHECK (price_precision BETWEEN 0 AND 18),
    size_precision INTEGER NOT NULL CHECK (size_precision BETWEEN 0 AND 18),
    tick_size NUMERIC NOT NULL CHECK (tick_size > 0),
    size_increment NUMERIC NOT NULL CHECK (size_increment > 0),
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
CREATE INDEX IF NOT EXISTS instrument_symbol_mapping_instrument_idx
    ON instrument_symbol_mapping (instrument_id);
"#;
const INSTRUMENT_MASTER_MIGRATION_LOCK: i64 = 0x4d_43_49_4e_53;

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
    latest_content_version: clickhouse::types::UInt256,
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
                 max(content_version) AS latest_content_version \
                 FROM market_quote_latest WHERE symbol = ?",
            )
            .bind(&quote.instrument_id)
            .fetch_one::<ClickHouseCurrentQuote>()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        if current.payload_json == payload_json {
            return Ok(false);
        }
        let version = if computed > current.latest_content_version {
            computed
        } else {
            increment_uint256(current.latest_content_version)
                .ok_or(RepositoryError::InvalidInput)?
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactManifestRecord {
    pub artifact_id: String,
    pub dataset: String,
    pub revision: String,
    pub source: String,
    pub source_url: Option<String>,
    pub observed_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub raw_response_locator: Option<String>,
    pub storage_path: String,
    pub relative_path: String,
    pub sha256: String,
    pub byte_size: u64,
    pub media_type: String,
    pub metadata_json: serde_json::Value,
}

/// Verifies a worker-owned staging file, then atomically promotes it into the Rust-owned,
/// content-addressed Artifact store. An existing identical target makes retries idempotent.
pub struct WorkerArtifactPromotion<'a> {
    pub staging_root: &'a Path,
    pub artifact_root: &'a Path,
    pub job_id: &'a str,
    pub dataset: &'a str,
    pub revision: &'a str,
    pub source: &'a str,
    pub result: &'a marketcow_jobs::StagedResult,
    pub ingested_at: DateTime<Utc>,
}

pub fn promote_worker_artifact(
    promotion: WorkerArtifactPromotion<'_>,
) -> Result<ArtifactManifestRecord, RepositoryError> {
    let WorkerArtifactPromotion {
        staging_root,
        artifact_root,
        job_id,
        dataset,
        revision,
        source,
        result,
        ingested_at,
    } = promotion;
    if !staging_root.is_absolute()
        || !artifact_root.is_absolute()
        || staging_root == Path::new("/")
        || artifact_root == Path::new("/")
        || !valid_path_segment(job_id, 128)
        || !valid_path_segment(dataset, 128)
        || !valid_path_segment(revision, 192)
        || source.is_empty()
        || source.len() > 256
        || result.relative_path.is_empty()
        || result.media_type.is_empty()
        || result.media_type.len() > 255
        || result
            .media_type
            .bytes()
            .any(|byte| byte.is_ascii_control())
        || result.sha256.len() != 64
        || !result.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(RepositoryError::InvalidInput);
    }
    let relative_staging = Path::new(&result.relative_path);
    if relative_staging.is_absolute()
        || relative_staging.components().count() != 1
        || !relative_staging
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
    {
        return Err(RepositoryError::InvalidInput);
    }
    let sha256 = result.sha256.to_ascii_lowercase();
    let relative_path = PathBuf::from(dataset)
        .join(revision)
        .join(&sha256[..2])
        .join(&sha256);
    let target = artifact_root.join(&relative_path);
    let target_parent = target.parent().ok_or(RepositoryError::InvalidInput)?;
    fs::create_dir_all(target_parent).map_err(|_| RepositoryError::Unavailable)?;
    if target.exists() {
        verify_file_identity(&target, &sha256, result.size_bytes)?;
    } else {
        let task_root = staging_root.join(job_id);
        let candidate = task_root.join(relative_staging);
        let metadata =
            fs::symlink_metadata(&candidate).map_err(|_| RepositoryError::Unavailable)?;
        if !metadata.file_type().is_file() || metadata.len() != result.size_bytes {
            return Err(RepositoryError::ArtifactVerificationFailed);
        }
        let canonical_task =
            fs::canonicalize(&task_root).map_err(|_| RepositoryError::Unavailable)?;
        let canonical_candidate =
            fs::canonicalize(&candidate).map_err(|_| RepositoryError::Unavailable)?;
        if canonical_candidate.parent() != Some(canonical_task.as_path()) {
            return Err(RepositoryError::ArtifactVerificationFailed);
        }
        verify_file_identity(&canonical_candidate, &sha256, result.size_bytes)?;
        File::open(&canonical_candidate)
            .and_then(|file| file.sync_all())
            .map_err(|_| RepositoryError::Unavailable)?;
        fs::rename(&canonical_candidate, &target).map_err(|_| RepositoryError::Unavailable)?;
        File::open(target_parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| RepositoryError::Unavailable)?;
    }
    let artifact_id = hex::encode(Sha256::digest(
        format!("{dataset}\0{revision}\0{sha256}").as_bytes(),
    ));
    Ok(ArtifactManifestRecord {
        artifact_id,
        dataset: dataset.into(),
        revision: revision.into(),
        source: source.into(),
        source_url: None,
        observed_at: ingested_at,
        ingested_at,
        raw_response_locator: Some(format!(
            "worker-staging://{job_id}/{}",
            result.relative_path
        )),
        storage_path: target.to_string_lossy().into_owned(),
        relative_path: relative_path.to_string_lossy().into_owned(),
        sha256,
        byte_size: result.size_bytes,
        media_type: result.media_type.clone(),
        metadata_json: serde_json::json!({
            "job_id": job_id,
            "request_schema": revision,
            "media_type": result.media_type,
            "relative_path": relative_path,
            "registered_by": "marketcowd"
        }),
    })
}

fn valid_path_segment(value: &str, maximum_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        && value != "."
        && value != ".."
}

fn verify_file_identity(
    path: &Path,
    expected_sha256: &str,
    expected_size: u64,
) -> Result<(), RepositoryError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| RepositoryError::Unavailable)?;
    if !metadata.file_type().is_file() || metadata.len() != expected_size {
        return Err(RepositoryError::ArtifactVerificationFailed);
    }
    let mut file = File::open(path).map_err(|_| RepositoryError::Unavailable)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| RepositoryError::Unavailable)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    if hex::encode(hasher.finalize()) != expected_sha256.to_ascii_lowercase() {
        return Err(RepositoryError::ArtifactVerificationFailed);
    }
    Ok(())
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
    fn register_verified(&self, record: &ArtifactManifestRecord) -> Result<(), RepositoryError>;
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
    #[error("artifact verification failed")]
    ArtifactVerificationFailed,
    #[error("instrument identity or symbol mapping conflict")]
    InstrumentConflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentRecord {
    pub schema_version: u8,
    pub instrument_id: String,
    pub instrument_type: String,
    pub asset_class: String,
    pub symbol: String,
    pub market: String,
    pub mic: String,
    pub currency: String,
    pub price_precision: u8,
    pub size_precision: u8,
    #[serde(with = "rust_decimal::serde::str")]
    pub tick_size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size_increment: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub lot_size: Decimal,
    pub ts_event: DateTime<Utc>,
    pub ts_init: DateTime<Utc>,
    pub provider_symbols: std::collections::BTreeMap<String, String>,
    pub broker_symbols: std::collections::BTreeMap<String, String>,
    pub content_hash: String,
    pub updated_at: DateTime<Utc>,
}

impl InstrumentRecord {
    pub fn validate(&self) -> Result<(), RepositoryError> {
        validate_instrument(self)
    }
}

pub struct PostgresInstrumentRepository {
    client: tokio::sync::Mutex<tokio_postgres::Client>,
}

impl PostgresInstrumentRepository {
    pub async fn connect(dsn: &str) -> Result<Self, RepositoryError> {
        if dsn.trim().is_empty() {
            return Err(RepositoryError::InvalidInput);
        }
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(error = %error, "postgres_instrument_repository_connection_failed");
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
        let checksum = hex::encode(Sha256::digest(INSTRUMENT_MASTER_MIGRATION_SQL.as_bytes()));
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock($1)",
                &[&INSTRUMENT_MASTER_MIGRATION_LOCK],
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
                &[&INSTRUMENT_MASTER_MIGRATION_VERSION],
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
                .batch_execute(INSTRUMENT_MASTER_MIGRATION_SQL)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            transaction
                .execute(
                    "INSERT INTO marketcow_rust_migration \
                     (version, checksum, applied_at, binary_commit, safe_forward) \
                     VALUES ($1, $2, NOW(), $3, TRUE)",
                    &[
                        &INSTRUMENT_MASTER_MIGRATION_VERSION,
                        &checksum,
                        &binary_commit,
                    ],
                )
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| RepositoryError::Unavailable)
    }

    pub async fn upsert(&self, instrument: &InstrumentRecord) -> Result<(), RepositoryError> {
        validate_instrument(instrument)?;
        let provider_symbols = serde_json::to_value(&instrument.provider_symbols)
            .map_err(|_| RepositoryError::InvalidInput)?;
        let broker_symbols = serde_json::to_value(&instrument.broker_symbols)
            .map_err(|_| RepositoryError::InvalidInput)?;
        let mappings = instrument
            .provider_symbols
            .iter()
            .map(|(provider, symbol)| (format!("provider:{provider}"), symbol))
            .chain(
                instrument
                    .broker_symbols
                    .iter()
                    .map(|(broker, symbol)| (format!("broker:{broker}"), symbol)),
            )
            .collect::<Vec<_>>();
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        for (namespace, external_symbol) in &mappings {
            let conflict = transaction
                .query_opt(
                    "SELECT instrument_id FROM instrument_symbol_mapping \
                     WHERE namespace = $1 AND external_symbol = $2",
                    &[namespace, external_symbol],
                )
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            if conflict.is_some_and(|row| row.get::<_, String>(0) != instrument.instrument_id) {
                return Err(RepositoryError::InstrumentConflict);
            }
        }
        transaction
            .execute(
                "INSERT INTO instrument_master \
                 (instrument_id,schema_version,instrument_type,asset_class,symbol,market,mic,\
                  currency,price_precision,size_precision,tick_size,size_increment,lot_size,\
                  ts_event,ts_init,provider_symbols,broker_symbols,content_hash,updated_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::TEXT::NUMERIC,\
                         $12::TEXT::NUMERIC,$13::TEXT::NUMERIC,$14,$15,$16,$17,$18,$19) \
                 ON CONFLICT (instrument_id) DO UPDATE SET \
                  schema_version=EXCLUDED.schema_version,instrument_type=EXCLUDED.instrument_type,\
                  asset_class=EXCLUDED.asset_class,symbol=EXCLUDED.symbol,market=EXCLUDED.market,\
                  mic=EXCLUDED.mic,currency=EXCLUDED.currency,\
                  price_precision=EXCLUDED.price_precision,size_precision=EXCLUDED.size_precision,\
                  tick_size=EXCLUDED.tick_size,size_increment=EXCLUDED.size_increment,\
                  lot_size=EXCLUDED.lot_size,ts_event=EXCLUDED.ts_event,ts_init=EXCLUDED.ts_init,\
                  provider_symbols=EXCLUDED.provider_symbols,broker_symbols=EXCLUDED.broker_symbols,\
                  content_hash=EXCLUDED.content_hash,updated_at=EXCLUDED.updated_at",
                &[
                    &instrument.instrument_id,
                    &i32::from(instrument.schema_version),
                    &instrument.instrument_type,
                    &instrument.asset_class,
                    &instrument.symbol,
                    &instrument.market,
                    &instrument.mic,
                    &instrument.currency,
                    &i32::from(instrument.price_precision),
                    &i32::from(instrument.size_precision),
                    &instrument.tick_size.to_string(),
                    &instrument.size_increment.to_string(),
                    &instrument.lot_size.to_string(),
                    &instrument.ts_event,
                    &instrument.ts_init,
                    &provider_symbols,
                    &broker_symbols,
                    &instrument.content_hash,
                    &instrument.updated_at,
                ],
            )
            .await
            .map_err(map_instrument_write_error)?;
        transaction
            .execute(
                "DELETE FROM instrument_symbol_mapping WHERE instrument_id = $1",
                &[&instrument.instrument_id],
            )
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        for (namespace, external_symbol) in mappings {
            transaction
                .execute(
                    "INSERT INTO instrument_symbol_mapping \
                     (namespace,external_symbol,instrument_id) VALUES ($1,$2,$3)",
                    &[&namespace, external_symbol, &instrument.instrument_id],
                )
                .await
                .map_err(map_instrument_write_error)?;
        }
        transaction
            .commit()
            .await
            .map_err(map_instrument_write_error)?;
        Ok(())
    }

    pub async fn get(
        &self,
        instrument_id: &str,
    ) -> Result<Option<InstrumentRecord>, RepositoryError> {
        if !valid_instrument_id(instrument_id) {
            return Err(RepositoryError::InvalidInput);
        }
        let query = instrument_select("WHERE instrument_id = $1");
        let client = self.client.lock().await;
        client
            .query_opt(&query, &[&instrument_id])
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .map(decode_instrument)
            .transpose()
    }

    pub async fn resolve(
        &self,
        namespace: &str,
        external_symbol: &str,
    ) -> Result<Option<InstrumentRecord>, RepositoryError> {
        if !valid_namespace(namespace) || external_symbol.is_empty() || external_symbol.len() > 128
        {
            return Err(RepositoryError::InvalidInput);
        }
        let query = instrument_select(
            "JOIN instrument_symbol_mapping m USING (instrument_id) \
             WHERE m.namespace = $1 AND m.external_symbol = $2",
        );
        let client = self.client.lock().await;
        client
            .query_opt(&query, &[&namespace, &external_symbol])
            .await
            .map_err(|_| RepositoryError::Unavailable)?
            .map(decode_instrument)
            .transpose()
    }
}

fn instrument_select(suffix: &str) -> String {
    format!(
        "SELECT instrument_id,schema_version,instrument_type,asset_class,symbol,market,mic,\
         currency,price_precision,size_precision,tick_size::TEXT,size_increment::TEXT,\
         lot_size::TEXT,ts_event,ts_init,provider_symbols,broker_symbols,content_hash,updated_at \
         FROM instrument_master {suffix}"
    )
}

fn decode_instrument(row: tokio_postgres::Row) -> Result<InstrumentRecord, RepositoryError> {
    let provider_symbols =
        serde_json::from_value(row.get(15)).map_err(|_| RepositoryError::Unavailable)?;
    let broker_symbols =
        serde_json::from_value(row.get(16)).map_err(|_| RepositoryError::Unavailable)?;
    let instrument = InstrumentRecord {
        instrument_id: row.get(0),
        schema_version: u8::try_from(row.get::<_, i32>(1))
            .map_err(|_| RepositoryError::Unavailable)?,
        instrument_type: row.get(2),
        asset_class: row.get(3),
        symbol: row.get(4),
        market: row.get(5),
        mic: row.get(6),
        currency: row.get(7),
        price_precision: u8::try_from(row.get::<_, i32>(8))
            .map_err(|_| RepositoryError::Unavailable)?,
        size_precision: u8::try_from(row.get::<_, i32>(9))
            .map_err(|_| RepositoryError::Unavailable)?,
        tick_size: row
            .get::<_, String>(10)
            .parse()
            .map_err(|_| RepositoryError::Unavailable)?,
        size_increment: row
            .get::<_, String>(11)
            .parse()
            .map_err(|_| RepositoryError::Unavailable)?,
        lot_size: row
            .get::<_, String>(12)
            .parse()
            .map_err(|_| RepositoryError::Unavailable)?,
        ts_event: row.get(13),
        ts_init: row.get(14),
        provider_symbols,
        broker_symbols,
        content_hash: row.get(17),
        updated_at: row.get(18),
    };
    validate_instrument(&instrument).map_err(|_| RepositoryError::Unavailable)?;
    Ok(instrument)
}

fn map_instrument_write_error(error: tokio_postgres::Error) -> RepositoryError {
    if error.as_db_error().is_some_and(|database| {
        database.code() == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION
    }) {
        RepositoryError::InstrumentConflict
    } else {
        RepositoryError::Unavailable
    }
}

fn validate_instrument(instrument: &InstrumentRecord) -> Result<(), RepositoryError> {
    const TYPES: &[&str] = &[
        "equity",
        "convertible_bond",
        "crypto_spot",
        "crypto_perpetual",
        "equity_perpetual",
        "index_perpetual",
        "hip3_perpetual",
    ];
    const ASSET_CLASSES: &[&str] = &[
        "equity",
        "fixed_income",
        "crypto",
        "equity_derivative",
        "index_derivative",
        "other_derivative",
    ];
    if instrument.schema_version != 1
        || !valid_instrument_id(&instrument.instrument_id)
        || instrument.instrument_id != format!("{}.{}", instrument.symbol, instrument.mic)
        || !TYPES.contains(&instrument.instrument_type.as_str())
        || !ASSET_CLASSES.contains(&instrument.asset_class.as_str())
        || !matches!(instrument.market.as_str(), "US" | "HK" | "CN" | "CRYPTO")
        || instrument.mic.len() != 4
        || !instrument
            .mic
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        || !(2..=12).contains(&instrument.currency.len())
        || !instrument
            .currency
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        || instrument.price_precision > 18
        || instrument.size_precision > 18
        || instrument.tick_size <= Decimal::ZERO
        || instrument.size_increment <= Decimal::ZERO
        || instrument.lot_size <= Decimal::ZERO
        || instrument.ts_event > instrument.ts_init
        || instrument.provider_symbols.is_empty()
        || !valid_symbol_map(&instrument.provider_symbols)
        || !valid_symbol_map(&instrument.broker_symbols)
        || instrument.content_hash.len() != 71
        || !instrument.content_hash.starts_with("sha256:")
        || !instrument.content_hash[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(RepositoryError::InvalidInput);
    }
    Ok(())
}

fn valid_instrument_id(value: &str) -> bool {
    let Some((symbol, mic)) = value.rsplit_once('.') else {
        return false;
    };
    !symbol.is_empty()
        && symbol.len() <= 32
        && symbol.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_uppercase()
                || byte.is_ascii_digit()
                || index > 0 && matches!(byte, b'.' | b'-')
        })
        && mic.len() == 4
        && mic
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

fn valid_namespace(value: &str) -> bool {
    let Some((kind, name)) = value.split_once(':') else {
        return false;
    };
    matches!(kind, "provider" | "broker")
        && !name.is_empty()
        && name.len() <= 64
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn valid_symbol_map(symbols: &std::collections::BTreeMap<String, String>) -> bool {
    symbols.iter().all(|(namespace, symbol)| {
        valid_namespace(&format!("provider:{namespace}"))
            && !symbol.is_empty()
            && symbol.len() <= 128
    })
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
        let artifact_checksum =
            hex::encode(Sha256::digest(ARTIFACT_MANIFEST_MIGRATION_SQL.as_bytes()));
        let artifact_existing = transaction
            .query_opt(
                "SELECT checksum FROM marketcow_rust_migration WHERE version = $1",
                &[&ARTIFACT_MANIFEST_MIGRATION_VERSION],
            )
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        if let Some(row) = artifact_existing {
            let recorded: String = row.get(0);
            if recorded != artifact_checksum {
                return Err(RepositoryError::MigrationChecksumMismatch);
            }
        } else {
            transaction
                .batch_execute(ARTIFACT_MANIFEST_MIGRATION_SQL)
                .await
                .map_err(|_| RepositoryError::Unavailable)?;
            transaction
                .execute(
                    "INSERT INTO marketcow_rust_migration \
                     (version, checksum, applied_at, binary_commit, safe_forward) \
                     VALUES ($1, $2, NOW(), $3, TRUE)",
                    &[
                        &ARTIFACT_MANIFEST_MIGRATION_VERSION,
                        &artifact_checksum,
                        &binary_commit,
                    ],
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

    /// Atomically records a verified Artifact manifest and advances the owning job. A filesystem
    /// body may already have been promoted, but it is not authoritative or discoverable until
    /// this transaction commits.
    pub async fn compare_and_swap_with_artifact(
        &self,
        job: &marketcow_jobs::ProviderJob,
        expected_revision: u64,
        artifact: &ArtifactManifestRecord,
    ) -> Result<(), RepositoryError> {
        validate_job_for_storage(job)?;
        validate_artifact_manifest(artifact)?;
        if job.revision != expected_revision.saturating_add(1)
            || job.status != marketcow_jobs::JobStatus::Succeeded
            || job.result.as_ref().is_none_or(|result| {
                result.sha256.to_ascii_lowercase() != artifact.sha256
                    || result.size_bytes != artifact.byte_size
                    || result.media_type != artifact.media_type
                    || result.relative_path != artifact.relative_path
            })
        {
            return Err(RepositoryError::InvalidInput);
        }
        let payload = serde_json::to_value(job).map_err(|_| RepositoryError::InvalidInput)?;
        let status = job_status_text(job.status);
        let revision = i64::try_from(job.revision).map_err(|_| RepositoryError::InvalidInput)?;
        let expected =
            i64::try_from(expected_revision).map_err(|_| RepositoryError::InvalidInput)?;
        let byte_size =
            i64::try_from(artifact.byte_size).map_err(|_| RepositoryError::InvalidInput)?;
        let observed_at = artifact.observed_at.to_rfc3339();
        let ingested_at = artifact.ingested_at.to_rfc3339();
        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        transaction
            .execute(
                "INSERT INTO raw_artifact_manifest \
                 (artifact_id,dataset,source,source_url,observed_at,ingested_at, \
                  raw_response_locator,storage_path,sha256,byte_size,metadata_json) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
                 ON CONFLICT (artifact_id) DO NOTHING",
                &[
                    &artifact.artifact_id,
                    &artifact.dataset,
                    &artifact.source,
                    &artifact.source_url,
                    &observed_at,
                    &ingested_at,
                    &artifact.raw_response_locator,
                    &artifact.storage_path,
                    &artifact.sha256,
                    &byte_size,
                    &artifact.metadata_json,
                ],
            )
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        let stored = transaction
            .query_one(
                "SELECT dataset,source,storage_path,sha256,byte_size,metadata_json \
                 FROM raw_artifact_manifest WHERE artifact_id=$1 FOR SHARE",
                &[&artifact.artifact_id],
            )
            .await
            .map_err(|_| RepositoryError::Unavailable)?;
        let stored_size: i64 = stored.get(4);
        let stored_metadata: serde_json::Value = stored.get(5);
        if stored.get::<_, String>(0) != artifact.dataset
            || stored.get::<_, String>(1) != artifact.source
            || stored.get::<_, String>(2) != artifact.storage_path
            || stored.get::<_, String>(3) != artifact.sha256
            || stored_size != byte_size
            || stored_metadata != artifact.metadata_json
        {
            return Err(RepositoryError::IdempotencyConflict);
        }
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
        transaction
            .commit()
            .await
            .map_err(|_| RepositoryError::Unavailable)
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

fn validate_artifact_manifest(artifact: &ArtifactManifestRecord) -> Result<(), RepositoryError> {
    if artifact.artifact_id.len() != 64
        || !artifact
            .artifact_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || !valid_path_segment(&artifact.dataset, 128)
        || !valid_path_segment(&artifact.revision, 192)
        || artifact.source.is_empty()
        || artifact.source.len() > 256
        || artifact.storage_path.is_empty()
        || !Path::new(&artifact.storage_path).is_absolute()
        || artifact.relative_path.is_empty()
        || Path::new(&artifact.relative_path).is_absolute()
        || artifact.sha256.len() != 64
        || !artifact.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        || artifact.media_type.is_empty()
        || !artifact.metadata_json.is_object()
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

    fn instrument_fixture(symbol: &str, provider_symbol: &str) -> InstrumentRecord {
        InstrumentRecord {
            schema_version: 1,
            instrument_id: format!("{symbol}.XNAS"),
            instrument_type: "equity".into(),
            asset_class: "equity".into(),
            symbol: symbol.into(),
            market: "US".into(),
            mic: "XNAS".into(),
            currency: "USD".into(),
            price_precision: 8,
            size_precision: 0,
            tick_size: "0.00000001".parse().unwrap(),
            size_increment: "1".parse().unwrap(),
            lot_size: "1".parse().unwrap(),
            ts_event: DateTime::parse_from_rfc3339("2026-08-28T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            ts_init: DateTime::parse_from_rfc3339("2026-08-28T00:00:01Z")
                .unwrap()
                .with_timezone(&Utc),
            provider_symbols: std::collections::BTreeMap::from([(
                "longport".into(),
                provider_symbol.into(),
            )]),
            broker_symbols: std::collections::BTreeMap::new(),
            content_hash: format!("sha256:{}", "a".repeat(64)),
            updated_at: DateTime::parse_from_rfc3339("2026-08-28T00:00:02Z")
                .unwrap()
                .with_timezone(&Utc),
        }
    }

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
        assert_eq!(
            ARTIFACT_MANIFEST_MIGRATION_VERSION,
            "rust-artifact-manifest-v1"
        );
        assert!(ARTIFACT_MANIFEST_MIGRATION_SQL.contains("raw_artifact_manifest"));
        assert!(ARTIFACT_MANIFEST_MIGRATION_SQL.contains("byte_size >= 0"));
    }

    #[test]
    fn instrument_contract_is_exact_validated_and_migration_compatible() {
        let instrument = instrument_fixture("RUSTTEST", "RUSTTEST.US");
        validate_instrument(&instrument).unwrap();
        let value = serde_json::to_value(&instrument).unwrap();
        assert_eq!(value["tick_size"], "0.00000001");
        assert_eq!(value["currency"], "USD");
        assert_eq!(
            serde_json::from_value::<InstrumentRecord>(value).unwrap(),
            instrument
        );
        let mut invalid = instrument.clone();
        invalid.ts_event = invalid.ts_init + Duration::seconds(1);
        assert!(matches!(
            validate_instrument(&invalid),
            Err(RepositoryError::InvalidInput)
        ));
        let mut invalid = instrument;
        invalid
            .provider_symbols
            .insert("Bad Namespace".into(), "x".into());
        assert!(matches!(
            validate_instrument(&invalid),
            Err(RepositoryError::InvalidInput)
        ));
        assert!(INSTRUMENT_MASTER_MIGRATION_SQL.contains("provider_symbols JSONB NOT NULL"));
        assert!(INSTRUMENT_MASTER_MIGRATION_SQL.contains("UNIQUE (symbol, mic)"));
        assert_eq!(
            INSTRUMENT_MASTER_MIGRATION_VERSION,
            "rust-instrument-master-v1"
        );
    }

    #[test]
    fn worker_artifact_promotion_is_verified_content_addressed_and_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let staging_root = directory.path().join("staging");
        let artifact_root = directory.path().join("artifacts");
        let task_root = staging_root.join("job-1");
        fs::create_dir_all(&task_root).unwrap();
        let body = br#"{"price":"0.100000000000000001"}"#;
        let sha256 = hex::encode(Sha256::digest(body));
        fs::write(task_root.join("result.json"), body).unwrap();
        let result = marketcow_jobs::StagedResult {
            relative_path: "result.json".into(),
            sha256: sha256.clone(),
            size_bytes: body.len() as u64,
            media_type: "application/json".into(),
        };
        let at = Utc::now();
        let first = promote_worker_artifact(WorkerArtifactPromotion {
            staging_root: &staging_root,
            artifact_root: &artifact_root,
            job_id: "job-1",
            dataset: "provider.history",
            revision: "marketcow.provider.history.v1",
            source: "worker:test",
            result: &result,
            ingested_at: at,
        })
        .unwrap();
        assert_eq!(first.sha256, sha256);
        assert_eq!(first.byte_size, body.len() as u64);
        assert!(Path::new(&first.storage_path).is_file());
        assert_eq!(fs::read(&first.storage_path).unwrap(), body);
        assert_eq!(first.metadata_json["registered_by"], "marketcowd");

        // A retry succeeds after the staging file was consumed by the atomic rename.
        assert!(!task_root.join("result.json").exists());
        let second = promote_worker_artifact(WorkerArtifactPromotion {
            staging_root: &staging_root,
            artifact_root: &artifact_root,
            job_id: "job-1",
            dataset: "provider.history",
            revision: "marketcow.provider.history.v1",
            source: "worker:test",
            result: &result,
            ingested_at: at,
        })
        .unwrap();
        assert_eq!(second, first);
    }

    #[test]
    fn worker_artifact_promotion_rejects_escape_and_hash_mismatch() {
        let directory = tempfile::tempdir().unwrap();
        let staging_root = directory.path().join("staging");
        let artifact_root = directory.path().join("artifacts");
        let task_root = staging_root.join("job-1");
        fs::create_dir_all(&task_root).unwrap();
        fs::write(task_root.join("result.json"), b"body").unwrap();
        let mismatch = marketcow_jobs::StagedResult {
            relative_path: "result.json".into(),
            sha256: "a".repeat(64),
            size_bytes: 4,
            media_type: "application/json".into(),
        };
        assert!(matches!(
            promote_worker_artifact(WorkerArtifactPromotion {
                staging_root: &staging_root,
                artifact_root: &artifact_root,
                job_id: "job-1",
                dataset: "provider.history",
                revision: "marketcow.provider.history.v1",
                source: "worker:test",
                result: &mismatch,
                ingested_at: Utc::now(),
            }),
            Err(RepositoryError::ArtifactVerificationFailed)
        ));
        let escape = marketcow_jobs::StagedResult {
            relative_path: "../outside".into(),
            sha256: "a".repeat(64),
            size_bytes: 4,
            media_type: "application/json".into(),
        };
        assert!(matches!(
            promote_worker_artifact(WorkerArtifactPromotion {
                staging_root: &staging_root,
                artifact_root: &artifact_root,
                job_id: "job-1",
                dataset: "provider.history",
                revision: "marketcow.provider.history.v1",
                source: "worker:test",
                result: &escape,
                ingested_at: Utc::now(),
            }),
            Err(RepositoryError::InvalidInput)
        ));
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
        let artifact_sha256 = "d".repeat(64);
        let artifact_relative_path =
            format!("provider.history/marketcow.provider.history.v1/dd/{artifact_sha256}");
        let completed = engine
            .succeed(
                &job.job_id,
                &lease_token,
                marketcow_jobs::StagedResult {
                    relative_path: artifact_relative_path.clone(),
                    sha256: artifact_sha256.clone(),
                    size_bytes: 4,
                    media_type: "application/json".into(),
                },
                now + Duration::seconds(1),
            )
            .unwrap()
            .clone();
        let artifact = ArtifactManifestRecord {
            artifact_id: hex::encode(Sha256::digest(
                format!("{}\0{}", job.job_id, artifact_sha256).as_bytes(),
            )),
            dataset: "provider.history".into(),
            revision: "marketcow.provider.history.v1".into(),
            source: "python-worker:integration-test".into(),
            source_url: None,
            observed_at: now,
            ingested_at: now,
            raw_response_locator: Some(format!("worker-staging://{}/result.json", job.job_id)),
            storage_path: format!("/tmp/marketcow-test-artifacts/{artifact_relative_path}"),
            relative_path: artifact_relative_path,
            sha256: artifact_sha256,
            byte_size: 4,
            media_type: "application/json".into(),
            metadata_json: serde_json::json!({
                "job_id": job.job_id,
                "request_schema": "marketcow.provider.history.v1",
                "media_type": "application/json",
                "registered_by": "marketcowd"
            }),
        };
        repository
            .compare_and_swap_with_artifact(&completed, 3, &artifact)
            .await
            .unwrap();
        assert_eq!(repository.get(&job.job_id).await.unwrap(), Some(completed));
    }

    #[tokio::test]
    #[ignore = "requires MARKETCOW_TEST_POSTGRES_DSN"]
    async fn postgres_instrument_repository_round_trip_when_test_dsn_is_configured() {
        let dsn = std::env::var("MARKETCOW_TEST_POSTGRES_DSN")
            .expect("set MARKETCOW_TEST_POSTGRES_DSN for the ignored integration test");
        let repository = PostgresInstrumentRepository::connect(&dsn).await.unwrap();
        repository
            .migrate_safe_forward("test-binary-commit")
            .await
            .unwrap();
        repository
            .migrate_safe_forward("test-binary-commit")
            .await
            .unwrap();
        let suffix = uuid::Uuid::new_v4().simple().to_string()[..8].to_ascii_uppercase();
        let symbol = format!("R{suffix}");
        let provider_symbol = format!("{symbol}.US");
        let mut instrument = instrument_fixture(&symbol, &provider_symbol);
        repository.upsert(&instrument).await.unwrap();
        assert_eq!(
            repository.get(&instrument.instrument_id).await.unwrap(),
            Some(instrument.clone())
        );
        assert_eq!(
            repository
                .resolve("provider:longport", &provider_symbol)
                .await
                .unwrap(),
            Some(instrument.clone())
        );

        let replacement_symbol = format!("{symbol}.REVISED.US");
        instrument
            .provider_symbols
            .insert("longport".into(), replacement_symbol.clone());
        instrument.content_hash = format!("sha256:{}", "b".repeat(64));
        repository.upsert(&instrument).await.unwrap();
        assert_eq!(
            repository
                .resolve("provider:longport", &provider_symbol)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            repository
                .resolve("provider:longport", &replacement_symbol)
                .await
                .unwrap(),
            Some(instrument.clone())
        );

        let other_symbol = format!("X{suffix}");
        let conflicting = instrument_fixture(&other_symbol, &replacement_symbol);
        assert!(matches!(
            repository.upsert(&conflicting).await,
            Err(RepositoryError::InstrumentConflict)
        ));
    }
}
