use anyhow::{Context, Result, bail};
use arc_swap::ArcSwap;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{
        DefaultBodyLimit, Extension, Path as AxumPath, Query, Request, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket, close_code},
    },
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{Timelike, Utc};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    env,
    fs::{self, OpenOptions},
    io::Write,
    net::SocketAddr,
    os::fd::AsRawFd,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    process::Command as ProcessCommand,
    signal,
    sync::{Mutex as AsyncMutex, Notify, broadcast},
    task::JoinSet,
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const STREAM_CHANNEL_CAPACITY: usize = 256;
const STREAM_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const STREAM_CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);
const MAX_WORKER_RESULT_BYTES: u64 = 16 * 1024 * 1024;
const SEC_DIVIDEND_TASK: &str = "transform.sec_dividend_filing";
const SEC_DIVIDEND_REQUEST_SCHEMA: &str = "marketcow.worker.transform.sec-dividend-filing.v1";
const SEC_DIVIDEND_RESULT_SCHEMA: &str = "marketcow.worker.transform.sec-dividend-filing-result.v1";
const CSV_INFERENCE_TASK: &str = "transform.csv_inference";
const CSV_INFERENCE_REQUEST_SCHEMA: &str = "marketcow.worker.transform.csv-inference.v1";
const CSV_INFERENCE_RESULT_SCHEMA: &str = "marketcow.worker.transform.csv-inference-result.v1";
const LONGPORT_RESOLVE_TASK: &str = "provider.longport.resolve_instruments";
const LONGPORT_RESOLVE_REQUEST_SCHEMA: &str = "marketcow.worker.provider.longport-resolve.v1";
const LONGPORT_RESOLVE_RESULT_SCHEMA: &str = "marketcow.worker.provider.longport-resolve-result.v1";
const DOMAIN_OWNERSHIP_REGISTRY: &[u8] =
    include_bytes!("../../../docs/architecture/migration/domain-ownership.yaml");
const MIGRATION_CHECKPOINT_DOMAINS: &[&str] = &[
    "instrument_master",
    "runtime_config",
    "provider_jobs",
    "admin_audit",
    "artifact_manifest",
    "polymarket_realtime",
    "authoritative_wal",
    "postgresql",
    "clickhouse",
];

#[derive(Parser)]
#[command(name = "marketcow", version, about = "MarketCow Rust shadow platform")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve,
    Doctor,
    Migrate {
        #[arg(long)]
        dry_run: bool,
    },
    Wal {
        #[command(subcommand)]
        command: WalCommand,
    },
    Replay {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        checkpoint: bool,
    },
    ShadowSoak {
        #[arg(long, default_value_t = 2700)]
        duration_seconds: u64,
        #[arg(long, default_value_t = 1000)]
        interval_millis: u64,
        #[arg(long)]
        output: PathBuf,
    },
}

#[derive(Subcommand)]
enum WalCommand {
    Verify { path: PathBuf },
}

#[derive(Clone, Serialize)]
struct Config {
    profile: String,
    bind: SocketAddr,
    storage_root: PathBuf,
    wal_root: PathBuf,
    worker_socket: PathBuf,
    scope_id: String,
    real_order_submission_enabled: bool,
    shadow_mode: bool,
    maximum_book_age_ms: u64,
    legacy_mcp_url: Option<String>,
    python_workers: PythonWorkerConfig,
}

#[derive(Clone, Serialize)]
struct PythonWorkerConfig {
    executable: Option<PathBuf>,
    script: Option<PathBuf>,
    revision: String,
    pool_size: usize,
    max_restarts: usize,
    restart_window_seconds: u64,
    restart_backoff_millis: u64,
    memory_limit_mib: u64,
    cpu_limit_seconds: u64,
    dispatch_policies: BTreeMap<String, marketcow_jobs::DispatchPolicy>,
    #[serde(skip_serializing)]
    secret_references: BTreeMap<String, PathBuf>,
}

impl PythonWorkerConfig {
    fn load() -> Result<Self> {
        let executable = optional_absolute_env("MARKETCOW_PYTHON_WORKER_EXECUTABLE")?;
        let script = optional_absolute_env("MARKETCOW_PYTHON_WORKER_SCRIPT")?;
        if executable.is_some() != script.is_some() {
            bail!(
                "MARKETCOW_PYTHON_WORKER_EXECUTABLE and MARKETCOW_PYTHON_WORKER_SCRIPT must be configured together"
            );
        }
        let enabled = executable.is_some();
        let pool_size = env::var("MARKETCOW_PYTHON_WORKER_POOL_SIZE")
            .unwrap_or_else(|_| if enabled { "2" } else { "0" }.into())
            .parse::<usize>()?;
        if enabled && !(1..=16).contains(&pool_size) {
            bail!("enabled Python worker pool size must be between 1 and 16");
        }
        if !enabled && pool_size != 0 {
            bail!("Python worker pool size must be zero when the worker is disabled");
        }
        let revision =
            env::var("MARKETCOW_PYTHON_WORKER_REVISION").unwrap_or_else(|_| "development".into());
        if revision.is_empty() || revision.len() > 256 {
            bail!("Python worker revision must contain 1 to 256 bytes");
        }
        let max_restarts = env::var("MARKETCOW_PYTHON_WORKER_MAX_RESTARTS")
            .unwrap_or_else(|_| "3".into())
            .parse::<usize>()?;
        if !(1..=20).contains(&max_restarts) {
            bail!("Python worker max restarts must be between 1 and 20");
        }
        let restart_window_seconds = env::var("MARKETCOW_PYTHON_WORKER_RESTART_WINDOW_SECONDS")
            .unwrap_or_else(|_| "60".into())
            .parse::<u64>()?;
        if !(10..=3600).contains(&restart_window_seconds) {
            bail!("Python worker restart window must be between 10 and 3600 seconds");
        }
        let restart_backoff_millis = env::var("MARKETCOW_PYTHON_WORKER_RESTART_BACKOFF_MILLIS")
            .unwrap_or_else(|_| "1000".into())
            .parse::<u64>()?;
        if !(100..=60_000).contains(&restart_backoff_millis) {
            bail!("Python worker restart backoff must be between 100 and 60000 milliseconds");
        }
        let memory_limit_mib = env::var("MARKETCOW_PYTHON_WORKER_MEMORY_LIMIT_MIB")
            .unwrap_or_else(|_| "2048".into())
            .parse::<u64>()?;
        if !(128..=16_384).contains(&memory_limit_mib) {
            bail!("Python worker memory limit must be between 128 and 16384 MiB");
        }
        let cpu_limit_seconds = env::var("MARKETCOW_PYTHON_WORKER_CPU_LIMIT_SECONDS")
            .unwrap_or_else(|_| "900".into())
            .parse::<u64>()?;
        if !(1..=86_400).contains(&cpu_limit_seconds) {
            bail!("Python worker CPU limit must be between 1 and 86400 seconds");
        }
        let dispatch_policies = match env::var("MARKETCOW_PYTHON_DISPATCH_POLICIES_JSON") {
            Ok(raw) => {
                serde_json::from_str::<BTreeMap<String, marketcow_jobs::DispatchPolicy>>(&raw)
                    .context("Python dispatch policy JSON is invalid")?
            }
            Err(env::VarError::NotPresent) => default_dispatch_policies(),
            Err(error) => return Err(error.into()),
        };
        validate_dispatch_policies(&dispatch_policies)?;
        let secret_references = match env::var("MARKETCOW_PYTHON_SECRET_REFERENCES_JSON") {
            Ok(raw) => serde_json::from_str::<BTreeMap<String, PathBuf>>(&raw)
                .context("Python secret reference JSON is invalid")?,
            Err(env::VarError::NotPresent) => BTreeMap::new(),
            Err(error) => return Err(error.into()),
        };
        validate_secret_reference_config(&secret_references, &dispatch_policies)?;
        if enabled && pool_size < dispatch_policies.len() {
            bail!("Python worker pool must provide at least one process per capability");
        }
        Ok(Self {
            executable,
            script,
            revision,
            pool_size,
            max_restarts,
            restart_window_seconds,
            restart_backoff_millis,
            memory_limit_mib,
            cpu_limit_seconds,
            dispatch_policies,
            secret_references,
        })
    }

    #[cfg(test)]
    fn disabled() -> Self {
        Self {
            executable: None,
            script: None,
            revision: "test".into(),
            pool_size: 0,
            max_restarts: 3,
            restart_window_seconds: 60,
            restart_backoff_millis: 1000,
            memory_limit_mib: 2048,
            cpu_limit_seconds: 900,
            dispatch_policies: default_dispatch_policies(),
            secret_references: BTreeMap::new(),
        }
    }

    fn enabled(&self) -> bool {
        self.executable.is_some()
    }
}

fn default_dispatch_policies() -> BTreeMap<String, marketcow_jobs::DispatchPolicy> {
    BTreeMap::from([
        (
            SEC_DIVIDEND_TASK.into(),
            marketcow_jobs::DispatchPolicy {
                max_in_flight: 1,
                minimum_interval_millis: 1_000,
            },
        ),
        (
            CSV_INFERENCE_TASK.into(),
            marketcow_jobs::DispatchPolicy {
                max_in_flight: 2,
                minimum_interval_millis: 0,
            },
        ),
    ])
}

fn validate_dispatch_policies(
    policies: &BTreeMap<String, marketcow_jobs::DispatchPolicy>,
) -> Result<()> {
    if policies.is_empty() || policies.len() > 64 {
        bail!("Python dispatch policy count must be between 1 and 64");
    }
    for (capability, policy) in policies {
        if capability.is_empty()
            || capability.len() > 128
            || !capability
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            bail!("Python dispatch policy capability is invalid");
        }
        if !matches!(
            capability.as_str(),
            SEC_DIVIDEND_TASK | CSV_INFERENCE_TASK | LONGPORT_RESOLVE_TASK
        ) {
            bail!("Python dispatch policy capability has no registered handler");
        }
        policy
            .validate()
            .map_err(|error| anyhow::anyhow!("Python dispatch policy is invalid: {error}"))?;
    }
    Ok(())
}

fn validate_secret_reference_config(
    references: &BTreeMap<String, PathBuf>,
    policies: &BTreeMap<String, marketcow_jobs::DispatchPolicy>,
) -> Result<()> {
    for (capability, path) in references {
        if !policies.contains_key(capability) {
            bail!("Python secret reference capability has no dispatch policy");
        }
        if !path.is_absolute() {
            bail!("Python secret reference path must be absolute");
        }
    }
    Ok(())
}

impl Config {
    fn load() -> Result<Self> {
        let profile = env::var("MARKETCOW_RUST_PROFILE").unwrap_or_else(|_| "development".into());
        let bind: SocketAddr = env::var("MARKETCOW_RUST_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8870".into())
            .parse()?;
        let storage_root = absolute_env("MARKETCOW_RUST_STORAGE_ROOT")?;
        let wal_root = storage_root.join("wal");
        let worker_socket = storage_root.join("worker.sock");
        let scope_id =
            env::var("MARKETCOW_RUST_SCOPE_ID").unwrap_or_else(|_| "shadow-unscoped".into());
        let shadow_mode = env::var("MARKETCOW_RUST_SHADOW").as_deref() != Ok("false");
        let orders = env::var("MARKETCOW_REAL_ORDER_SUBMISSION_ENABLED").as_deref() == Ok("true");
        let maximum_book_age_ms = env::var("MARKETCOW_RUST_MAX_BOOK_AGE_MS")
            .unwrap_or_else(|_| "30000".into())
            .parse::<u64>()?;
        let legacy_mcp_url = match env::var("MARKETCOW_LEGACY_MCP_URL") {
            Ok(value) => {
                validate_legacy_mcp_url(&value, bind)?;
                Some(value)
            }
            Err(env::VarError::NotPresent) => None,
            Err(error) => return Err(error.into()),
        };
        let python_workers = PythonWorkerConfig::load()?;
        if orders {
            bail!("real order submission is prohibited by the migration safety gate");
        }
        if profile == "production" && bind.port() != 8790 {
            bail!("production must bind port 8790");
        }
        if profile != "production" && bind.port() == 8790 {
            bail!("non-production may not bind production port 8790");
        }
        if !bind.ip().is_loopback()
            && env::var("MARKETCOW_ALLOW_PUBLIC_BIND").as_deref() != Ok("confirmed")
        {
            bail!("non-loopback bind requires explicit MARKETCOW_ALLOW_PUBLIC_BIND=confirmed");
        }
        if !shadow_mode {
            bail!("writer/cutover mode is not enabled in this work item");
        }
        if maximum_book_age_ms == 0 {
            bail!("MARKETCOW_RUST_MAX_BOOK_AGE_MS must be positive");
        }
        Ok(Self {
            profile,
            bind,
            storage_root,
            wal_root,
            worker_socket,
            scope_id,
            real_order_submission_enabled: false,
            shadow_mode,
            maximum_book_age_ms,
            legacy_mcp_url,
            python_workers,
        })
    }
}

fn validate_legacy_mcp_url(value: &str, public_bind: SocketAddr) -> Result<()> {
    let parsed = url::Url::parse(value).context("MARKETCOW_LEGACY_MCP_URL must be a URL")?;
    let host = parsed
        .host_str()
        .and_then(|host| host.parse::<std::net::IpAddr>().ok())
        .filter(std::net::IpAddr::is_loopback)
        .context("legacy MCP proxy must use an explicit loopback IP address")?;
    let port = parsed
        .port()
        .context("legacy MCP proxy URL must include an explicit port")?;
    if parsed.scheme() != "http"
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.path() != "/mcp"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        bail!("legacy MCP proxy URL must be an uncredentialed http://loopback:port/mcp URL");
    }
    if host == public_bind.ip() && port == public_bind.port() {
        bail!("legacy MCP proxy URL cannot point to the Rust public listener");
    }
    Ok(())
}

fn optional_absolute_env(name: &str) -> Result<Option<PathBuf>> {
    match env::var(name) {
        Ok(value) => {
            let path = PathBuf::from(value);
            if !path.is_absolute() {
                bail!("{name} must be absolute");
            }
            Ok(Some(path))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn absolute_env(name: &str) -> Result<PathBuf> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        bail!("{name} must be absolute");
    }
    Ok(path)
}

#[derive(Clone)]
struct AppState {
    config: Config,
    audit: Arc<AuditCoordinator>,
    metrics: Arc<Metrics>,
    projection: Arc<ArcSwap<marketcow_core::Projection>>,
    recent_events: Arc<ArcSwap<Vec<marketcow_core::PersistedEvent>>>,
    runtime: Arc<AsyncMutex<marketcow_runtime::PolymarketRuntime>>,
    jobs: Arc<DurableJobCoordinator>,
    instruments: Arc<InstrumentCoordinator>,
    control_plane: Arc<ControlPlaneCoordinator>,
    stream: broadcast::Sender<marketcow_core::PersistedEvent>,
    worker_status: Arc<PythonWorkerStatus>,
    legacy_mcp: Option<LegacyMcpProxy>,
}

#[derive(Clone)]
struct LegacyMcpProxy {
    client: reqwest::Client,
    url: String,
}

impl LegacyMcpProxy {
    fn new(url: String) -> Result<Self> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(20))
            .build()?;
        Ok(Self { client, url })
    }

    async fn request(&self, message: &serde_json::Value) -> Result<Option<serde_json::Value>> {
        let mut response = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .header(
                "mcp-protocol-version",
                marketcow_contracts::MCP_LATEST_PROTOCOL_VERSION,
            )
            .json(message)
            .send()
            .await
            .context("legacy MCP request failed")?;
        if response.status() == reqwest::StatusCode::ACCEPTED {
            return Ok(None);
        }
        if response.status() != reqwest::StatusCode::OK {
            bail!("legacy MCP returned a non-success status");
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .unwrap_or("");
        if !content_type.eq_ignore_ascii_case("application/json") {
            bail!("legacy MCP returned an invalid media type");
        }
        if response
            .content_length()
            .is_some_and(|length| length > marketcow_contracts::MCP_MAX_REQUEST_BYTES as u64)
        {
            bail!("legacy MCP response exceeds 1 MiB");
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if body.len().saturating_add(chunk.len()) > marketcow_contracts::MCP_MAX_REQUEST_BYTES {
                bail!("legacy MCP response exceeds 1 MiB");
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Some(
            serde_json::from_slice(&body).context("legacy MCP returned invalid JSON")?,
        ))
    }
}

#[derive(Default)]
struct PythonWorkerStatus {
    live: AtomicU64,
    restarts: AtomicU64,
    budget_exhaustions: AtomicU64,
    memory_limit_kills: AtomicU64,
    memory_monitor_failures: AtomicU64,
}

#[derive(Default)]
struct Metrics {
    requests: AtomicU64,
    errors: AtomicU64,
    disconnects: AtomicU64,
    persistence_latency_us: AtomicU64,
    publication_latency_us: AtomicU64,
    stream_clients: AtomicU64,
    stream_disconnects: AtomicU64,
    stream_slow_consumer_disconnects: AtomicU64,
}

#[derive(Debug, thiserror::Error)]
enum DurableJobError {
    #[error(transparent)]
    Engine(#[from] marketcow_jobs::JobEngineError),
    #[error("authoritative job persistence failed: {0}")]
    Persistence(#[from] marketcow_storage::RepositoryError),
}

struct DurableJobCoordinator {
    engine: AsyncMutex<marketcow_jobs::JobEngine>,
    state_changed: Notify,
    repository: Option<Arc<marketcow_storage::PostgresJobRepository>>,
    memory_artifacts: AsyncMutex<BTreeMap<String, marketcow_storage::ArtifactManifestRecord>>,
    dispatch_policies: BTreeMap<String, marketcow_jobs::DispatchPolicy>,
}

struct InstrumentCoordinator {
    repository: Option<Arc<marketcow_storage::PostgresInstrumentRepository>>,
    #[cfg(test)]
    memory: AsyncMutex<BTreeMap<String, marketcow_storage::InstrumentRecord>>,
    #[cfg(test)]
    memory_enabled: bool,
}

struct ControlPlaneCoordinator {
    repository: Option<Arc<marketcow_storage::PostgresControlPlaneRepository>>,
    config_revision: String,
    #[cfg(test)]
    checkpoints: AsyncMutex<
        BTreeMap<(String, String, String), marketcow_storage::MigrationCheckpointRecord>,
    >,
    #[cfg(test)]
    memory_enabled: bool,
}

impl ControlPlaneCoordinator {
    async fn open(config: &Config) -> Result<Self> {
        let config_json = serde_json::to_value(config)?;
        let config_revision = calculated_config_revision(config)?;
        let config_sha256 = config_revision
            .strip_prefix("sha256:")
            .expect("calculated revision is sha256-prefixed")
            .to_owned();
        let dsn = env::var("MARKETCOW_POSTGRES_DSN").ok();
        let Some(dsn) = dsn else {
            if config.profile == "production" {
                bail!("MARKETCOW_POSTGRES_DSN is required in production");
            }
            return Ok(Self {
                repository: None,
                config_revision,
                #[cfg(test)]
                checkpoints: AsyncMutex::new(BTreeMap::new()),
                #[cfg(test)]
                memory_enabled: false,
            });
        };
        let binary_commit = env::var("MARKETCOW_BINARY_COMMIT").unwrap_or_default();
        if binary_commit.is_empty() {
            bail!("MARKETCOW_BINARY_COMMIT is required when PostgreSQL control plane is enabled");
        }
        let repository = Arc::new(
            marketcow_storage::PostgresControlPlaneRepository::connect(&dsn)
                .await
                .map_err(|error| anyhow::anyhow!(error))?,
        );
        repository
            .migrate_safe_forward(&binary_commit)
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
        let config_id = "marketcowd-runtime";
        let latest = repository
            .get_runtime_config_version(config_id, None)
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
        if latest
            .as_ref()
            .is_none_or(|record| record.config_sha256 != config_sha256)
        {
            let version = latest
                .as_ref()
                .map_or(1, |record| record.version.saturating_add(1));
            if version == u64::MAX {
                bail!("runtime configuration version space is exhausted");
            }
            repository
                .save_runtime_config_version(&marketcow_storage::RuntimeConfigVersionRecord {
                    config_id: config_id.into(),
                    version,
                    profile: config.profile.clone(),
                    schema_version: "marketcow.runtime-config.v1".into(),
                    config_json,
                    config_sha256,
                    observed_at: Utc::now(),
                    actor: "marketcowd-startup".into(),
                })
                .await
                .map_err(|error| anyhow::anyhow!(error))?;
        }
        Ok(Self {
            repository: Some(repository),
            config_revision,
            #[cfg(test)]
            checkpoints: AsyncMutex::new(BTreeMap::new()),
            #[cfg(test)]
            memory_enabled: false,
        })
    }

    #[cfg(test)]
    fn memory(config_revision: &str) -> Self {
        Self {
            repository: None,
            config_revision: config_revision.into(),
            checkpoints: AsyncMutex::new(BTreeMap::new()),
            memory_enabled: true,
        }
    }

    fn persistence_enabled(&self) -> bool {
        self.repository.is_some()
    }

    async fn get_checkpoint(
        &self,
        run_id: &str,
        domain: &str,
        shard: &str,
    ) -> std::result::Result<
        Option<marketcow_storage::MigrationCheckpointRecord>,
        marketcow_storage::RepositoryError,
    > {
        if let Some(repository) = &self.repository {
            return repository
                .get_migration_checkpoint(run_id, domain, shard)
                .await;
        }
        #[cfg(test)]
        if self.memory_enabled {
            return Ok(self
                .checkpoints
                .lock()
                .await
                .get(&(run_id.into(), domain.into(), shard.into()))
                .cloned());
        }
        Err(marketcow_storage::RepositoryError::Unavailable)
    }

    async fn upsert_checkpoint(
        &self,
        record: &marketcow_storage::MigrationCheckpointRecord,
        expected_revision: u64,
    ) -> std::result::Result<
        marketcow_storage::MigrationCheckpointRecord,
        marketcow_storage::RepositoryError,
    > {
        record.validate()?;
        if let Some(repository) = &self.repository {
            return repository
                .upsert_migration_checkpoint(record, expected_revision)
                .await;
        }
        #[cfg(test)]
        if self.memory_enabled {
            let key = (
                record.run_id.clone(),
                record.domain.clone(),
                record.shard.clone(),
            );
            let mut checkpoints = self.checkpoints.lock().await;
            let revision = checkpoints.get(&key).map_or(0, |current| current.revision);
            if revision != expected_revision {
                return Err(marketcow_storage::RepositoryError::RevisionConflict);
            }
            let mut stored = record.clone();
            stored.revision = expected_revision
                .checked_add(1)
                .ok_or(marketcow_storage::RepositoryError::InvalidInput)?;
            checkpoints.insert(key, stored.clone());
            return Ok(stored);
        }
        Err(marketcow_storage::RepositoryError::Unavailable)
    }
}

fn calculated_config_revision(config: &Config) -> Result<String> {
    let config_json = serde_json::to_value(config)?;
    let canonical = serde_json::to_vec(&config_json)?;
    Ok(format!(
        "sha256:{}",
        hex::encode(Sha256::digest(&canonical))
    ))
}

impl InstrumentCoordinator {
    async fn open(profile: &str) -> Result<Self> {
        let dsn = env::var("MARKETCOW_POSTGRES_DSN").ok();
        let Some(dsn) = dsn else {
            if profile == "production" {
                bail!("MARKETCOW_POSTGRES_DSN is required in production");
            }
            return Ok(Self {
                repository: None,
                #[cfg(test)]
                memory: AsyncMutex::new(BTreeMap::new()),
                #[cfg(test)]
                memory_enabled: false,
            });
        };
        let binary_commit = env::var("MARKETCOW_BINARY_COMMIT").unwrap_or_default();
        if binary_commit.is_empty() {
            bail!("MARKETCOW_BINARY_COMMIT is required when PostgreSQL instruments are enabled");
        }
        let repository = Arc::new(
            marketcow_storage::PostgresInstrumentRepository::connect(&dsn)
                .await
                .map_err(|error| anyhow::anyhow!(error))?,
        );
        repository
            .migrate_safe_forward(&binary_commit)
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
        Ok(Self {
            repository: Some(repository),
            #[cfg(test)]
            memory: AsyncMutex::new(BTreeMap::new()),
            #[cfg(test)]
            memory_enabled: false,
        })
    }

    #[cfg(test)]
    fn memory() -> Self {
        Self {
            repository: None,
            memory: AsyncMutex::new(BTreeMap::new()),
            memory_enabled: true,
        }
    }

    fn persistence_enabled(&self) -> bool {
        self.repository.is_some()
    }

    async fn get(
        &self,
        instrument_id: &str,
    ) -> Result<Option<marketcow_storage::InstrumentRecord>, marketcow_storage::RepositoryError>
    {
        if let Some(repository) = &self.repository {
            return repository.get(instrument_id).await;
        }
        #[cfg(test)]
        if self.memory_enabled {
            return Ok(self.memory.lock().await.get(instrument_id).cloned());
        }
        Err(marketcow_storage::RepositoryError::Unavailable)
    }

    async fn resolve(
        &self,
        namespace: &str,
        external_symbol: &str,
    ) -> Result<Option<marketcow_storage::InstrumentRecord>, marketcow_storage::RepositoryError>
    {
        if let Some(repository) = &self.repository {
            return repository.resolve(namespace, external_symbol).await;
        }
        #[cfg(test)]
        if self.memory_enabled {
            let (kind, name) = namespace
                .split_once(':')
                .ok_or(marketcow_storage::RepositoryError::InvalidInput)?;
            if !matches!(kind, "provider" | "broker")
                || name.is_empty()
                || external_symbol.is_empty()
            {
                return Err(marketcow_storage::RepositoryError::InvalidInput);
            }
            return Ok(self.memory.lock().await.values().find_map(|record| {
                let mappings = match kind {
                    "provider" => &record.provider_symbols,
                    "broker" => &record.broker_symbols,
                    _ => unreachable!("mapping kind was validated"),
                };
                (mappings.get(name).map(String::as_str) == Some(external_symbol))
                    .then(|| record.clone())
            }));
        }
        Err(marketcow_storage::RepositoryError::Unavailable)
    }

    async fn upsert(
        &self,
        record: &marketcow_storage::InstrumentRecord,
    ) -> Result<(), marketcow_storage::RepositoryError> {
        record.validate()?;
        if let Some(repository) = &self.repository {
            return repository.upsert(record).await;
        }
        #[cfg(test)]
        if self.memory_enabled {
            let records = self.memory.lock().await;
            for existing in records.values() {
                if existing.instrument_id == record.instrument_id {
                    continue;
                }
                let provider_conflict = record
                    .provider_symbols
                    .iter()
                    .any(|(name, symbol)| existing.provider_symbols.get(name) == Some(symbol));
                let broker_conflict = record
                    .broker_symbols
                    .iter()
                    .any(|(name, symbol)| existing.broker_symbols.get(name) == Some(symbol));
                if provider_conflict || broker_conflict {
                    return Err(marketcow_storage::RepositoryError::InstrumentConflict);
                }
            }
            drop(records);
            self.memory
                .lock()
                .await
                .insert(record.instrument_id.clone(), record.clone());
            return Ok(());
        }
        Err(marketcow_storage::RepositoryError::Unavailable)
    }

    #[cfg(test)]
    async fn insert_fixture(&self, record: marketcow_storage::InstrumentRecord) {
        self.memory
            .lock()
            .await
            .insert(record.instrument_id.clone(), record);
    }
}

impl DurableJobCoordinator {
    #[cfg(test)]
    fn memory() -> Self {
        Self {
            engine: AsyncMutex::new(marketcow_jobs::JobEngine::default()),
            state_changed: Notify::new(),
            repository: None,
            memory_artifacts: AsyncMutex::new(BTreeMap::new()),
            dispatch_policies: BTreeMap::new(),
        }
    }

    fn memory_with_policies(
        dispatch_policies: BTreeMap<String, marketcow_jobs::DispatchPolicy>,
    ) -> Self {
        Self {
            engine: AsyncMutex::new(marketcow_jobs::JobEngine::default()),
            state_changed: Notify::new(),
            repository: None,
            memory_artifacts: AsyncMutex::new(BTreeMap::new()),
            dispatch_policies,
        }
    }

    async fn open(
        profile: &str,
        dispatch_policies: BTreeMap<String, marketcow_jobs::DispatchPolicy>,
    ) -> Result<Self> {
        let dsn = env::var("MARKETCOW_POSTGRES_DSN").ok();
        let Some(dsn) = dsn else {
            if profile == "production" {
                bail!("MARKETCOW_POSTGRES_DSN is required in production");
            }
            return Ok(Self::memory_with_policies(dispatch_policies));
        };
        let binary_commit = env::var("MARKETCOW_BINARY_COMMIT").unwrap_or_default();
        if binary_commit.is_empty() {
            bail!("MARKETCOW_BINARY_COMMIT is required when PostgreSQL jobs are enabled");
        }
        let repository = Arc::new(
            marketcow_storage::PostgresJobRepository::connect(&dsn)
                .await
                .map_err(|error| anyhow::anyhow!(error))?,
        );
        repository
            .migrate_safe_forward(&binary_commit)
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
        let jobs = repository
            .list_all(10_001)
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
        if jobs.len() > 10_000 {
            bail!("provider job recovery exceeds the bounded 10000-row startup limit");
        }
        let engine = marketcow_jobs::JobEngine::recover(jobs)
            .map_err(|error| anyhow::anyhow!("provider job recovery failed: {error}"))?;
        Ok(Self {
            engine: AsyncMutex::new(engine),
            state_changed: Notify::new(),
            repository: Some(repository),
            memory_artifacts: AsyncMutex::new(BTreeMap::new()),
            dispatch_policies,
        })
    }

    fn persistence_enabled(&self) -> bool {
        self.repository.is_some()
    }

    async fn submit(
        &self,
        input: marketcow_jobs::SubmitJob,
        now: chrono::DateTime<Utc>,
    ) -> std::result::Result<marketcow_jobs::ProviderJob, DurableJobError> {
        let mut engine = self.engine.lock().await;
        let before = engine.clone();
        let candidate = engine.submit(input, now)?.clone();
        let Some(repository) = &self.repository else {
            return Ok(candidate);
        };
        match repository.insert_or_get(&candidate).await {
            Ok((stored, _)) => {
                if let Err(error) = engine.replace_recovered(stored.clone()) {
                    *engine = before;
                    return Err(error.into());
                }
                Ok(stored)
            }
            Err(error) => {
                *engine = before;
                Err(error.into())
            }
        }
    }

    async fn get(&self, job_id: &str) -> Option<marketcow_jobs::ProviderJob> {
        self.engine.lock().await.get(job_id).cloned()
    }

    async fn wait_terminal(
        &self,
        job_id: &str,
        deadline: chrono::DateTime<Utc>,
    ) -> std::result::Result<marketcow_jobs::ProviderJob, DurableJobError> {
        loop {
            let notified = self.state_changed.notified();
            let job = self
                .get(job_id)
                .await
                .ok_or(marketcow_jobs::JobEngineError::NotFound)?;
            if job.status.terminal() {
                return Ok(job);
            }
            let remaining = deadline.signed_duration_since(Utc::now());
            if remaining <= chrono::Duration::zero() {
                return Err(marketcow_storage::RepositoryError::Timeout.into());
            }
            let timeout = remaining
                .to_std()
                .map_err(|_| marketcow_storage::RepositoryError::Timeout)?;
            if tokio::time::timeout(timeout, notified).await.is_err() {
                return Err(marketcow_storage::RepositoryError::Timeout.into());
            }
        }
    }

    async fn cancel(
        &self,
        job_id: &str,
        actor: &str,
        now: chrono::DateTime<Utc>,
    ) -> std::result::Result<marketcow_jobs::ProviderJob, DurableJobError> {
        let mut engine = self.engine.lock().await;
        let before = engine.clone();
        let updated = engine.cancel(job_id, actor, now)?.clone();
        self.persist_single_or_rollback(&mut engine, before, &updated)
            .await?;
        self.state_changed.notify_waiters();
        Ok(updated)
    }

    async fn reconcile_and_claim(
        &self,
        worker_id: &str,
        capabilities: &[String],
        lease_duration: chrono::Duration,
        now: chrono::DateTime<Utc>,
    ) -> std::result::Result<Option<marketcow_jobs::ProviderJob>, DurableJobError> {
        let mut engine = self.engine.lock().await;
        let before_expire = engine.clone();
        engine.expire_leases(now);
        self.persist_changes_or_rollback(&mut engine, before_expire)
            .await?;

        let before_requeue = engine.clone();
        engine.requeue_retryable(now);
        self.persist_changes_or_rollback(&mut engine, before_requeue)
            .await?;

        let before_claim = engine.clone();
        let claimed = if self.dispatch_policies.is_empty() {
            engine.claim_next(worker_id, capabilities, lease_duration, now)?
        } else {
            engine.claim_next_with_policies(
                worker_id,
                capabilities,
                &self.dispatch_policies,
                lease_duration,
                now,
            )?
        }
        .cloned();
        if let Some(job) = &claimed {
            self.persist_single_or_rollback(&mut engine, before_claim, job)
                .await?;
        }
        Ok(claimed)
    }

    async fn start(
        &self,
        job_id: &str,
        lease_token: &str,
        now: chrono::DateTime<Utc>,
    ) -> std::result::Result<marketcow_jobs::ProviderJob, DurableJobError> {
        let mut engine = self.engine.lock().await;
        let before = engine.clone();
        let updated = engine.start(job_id, lease_token, now)?.clone();
        self.persist_single_or_rollback(&mut engine, before, &updated)
            .await?;
        Ok(updated)
    }

    async fn authorize_result(
        &self,
        job_id: &str,
        lease_token: &str,
        now: chrono::DateTime<Utc>,
    ) -> std::result::Result<marketcow_jobs::ProviderJob, DurableJobError> {
        Ok(self
            .engine
            .lock()
            .await
            .authorize_result(job_id, lease_token, now)?
            .clone())
    }

    async fn succeed_with_artifact(
        &self,
        job_id: &str,
        lease_token: &str,
        result: marketcow_jobs::StagedResult,
        artifact: marketcow_storage::ArtifactManifestRecord,
        now: chrono::DateTime<Utc>,
    ) -> std::result::Result<marketcow_jobs::ProviderJob, DurableJobError> {
        if result.sha256.to_ascii_lowercase() != artifact.sha256
            || result.size_bytes != artifact.byte_size
            || result.media_type != artifact.media_type
            || result.relative_path != artifact.relative_path
        {
            return Err(marketcow_storage::RepositoryError::InvalidInput.into());
        }
        let mut engine = self.engine.lock().await;
        let current = engine
            .get(job_id)
            .ok_or(marketcow_jobs::JobEngineError::NotFound)?;
        if artifact.dataset != current.job_type || artifact.revision != current.request_schema {
            return Err(marketcow_storage::RepositoryError::InvalidInput.into());
        }
        let before = engine.clone();
        let updated = engine.succeed(job_id, lease_token, result, now)?.clone();
        let expected_revision = before
            .get(&updated.job_id)
            .map(|job| job.revision)
            .ok_or(marketcow_jobs::JobEngineError::NotFound)?;
        if let Some(repository) = &self.repository {
            if let Err(error) = repository
                .compare_and_swap_with_artifact(&updated, expected_revision, &artifact)
                .await
            {
                *engine = before;
                return Err(error.into());
            }
        } else {
            let mut manifests = self.memory_artifacts.lock().await;
            if manifests
                .get(&artifact.artifact_id)
                .is_some_and(|stored| stored != &artifact)
            {
                *engine = before;
                return Err(marketcow_storage::RepositoryError::IdempotencyConflict.into());
            }
            manifests.insert(artifact.artifact_id.clone(), artifact);
        }
        self.state_changed.notify_waiters();
        Ok(updated)
    }

    async fn fail(
        &self,
        job_id: &str,
        lease_token: &str,
        error: marketcow_jobs::JobError,
        retryable: bool,
        now: chrono::DateTime<Utc>,
    ) -> std::result::Result<marketcow_jobs::ProviderJob, DurableJobError> {
        let mut engine = self.engine.lock().await;
        let before = engine.clone();
        let updated = engine
            .fail(job_id, lease_token, error, retryable, now)?
            .clone();
        self.persist_single_or_rollback(&mut engine, before, &updated)
            .await?;
        self.state_changed.notify_waiters();
        Ok(updated)
    }

    async fn persist_single_or_rollback(
        &self,
        engine: &mut marketcow_jobs::JobEngine,
        before: marketcow_jobs::JobEngine,
        updated: &marketcow_jobs::ProviderJob,
    ) -> std::result::Result<(), DurableJobError> {
        let Some(repository) = &self.repository else {
            return Ok(());
        };
        let expected_revision = before
            .get(&updated.job_id)
            .map(|job| job.revision)
            .ok_or(marketcow_jobs::JobEngineError::NotFound)?;
        if let Err(error) = repository
            .compare_and_swap(updated, expected_revision)
            .await
        {
            *engine = before;
            return Err(error.into());
        }
        Ok(())
    }

    async fn persist_changes_or_rollback(
        &self,
        engine: &mut marketcow_jobs::JobEngine,
        before: marketcow_jobs::JobEngine,
    ) -> std::result::Result<(), DurableJobError> {
        let Some(repository) = &self.repository else {
            return Ok(());
        };
        let updates = engine
            .snapshot()
            .into_iter()
            .filter_map(|job| {
                before.get(&job.job_id).and_then(|previous| {
                    (previous.revision != job.revision).then_some((job, previous.revision))
                })
            })
            .collect::<Vec<_>>();
        if let Err(error) = repository.compare_and_swap_many(&updates).await {
            *engine = before;
            return Err(error.into());
        }
        Ok(())
    }
}

struct AuditLog {
    file: Mutex<std::fs::File>,
    path: PathBuf,
}
impl AuditLog {
    fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Mutex::new(file),
            path: path.to_owned(),
        })
    }
    fn record(&self, value: &serde_json::Value, durable: bool) -> Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("local audit lock is poisoned"))?;
        let line = serde_json::to_vec(value)?;
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        if durable {
            file.sync_data()?;
        } else {
            file.flush()?;
        }
        Ok(())
    }
}

struct AuditCoordinator {
    local: AuditLog,
    repository: Option<Arc<marketcow_storage::PostgresAuditRepository>>,
}

impl AuditCoordinator {
    async fn open(config: &Config) -> Result<Self> {
        let local = AuditLog::open(&config.storage_root.join("audit.jsonl"))?;
        let Some(dsn) = env::var("MARKETCOW_POSTGRES_DSN").ok() else {
            if config.profile == "production" {
                bail!("MARKETCOW_POSTGRES_DSN is required in production");
            }
            return Ok(Self {
                local,
                repository: None,
            });
        };
        let binary_commit = env::var("MARKETCOW_BINARY_COMMIT").unwrap_or_default();
        if binary_commit.is_empty() {
            bail!("MARKETCOW_BINARY_COMMIT is required when PostgreSQL audit is enabled");
        }
        let repository = Arc::new(
            marketcow_storage::PostgresAuditRepository::connect(&dsn)
                .await
                .map_err(|error| anyhow::anyhow!(error))?,
        );
        repository
            .migrate_safe_forward(&binary_commit)
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
        Ok(Self {
            local,
            repository: Some(repository),
        })
    }

    #[cfg(test)]
    fn memory(path: &Path) -> Self {
        Self {
            local: AuditLog::open(path).expect("test audit log opens"),
            repository: None,
        }
    }

    fn persistence_enabled(&self) -> bool {
        self.repository.is_some()
    }

    fn record_request(&self, value: &serde_json::Value) -> Result<()> {
        self.local.record(value, false)
    }

    async fn list_admin(
        &self,
        limit: i64,
        offset: i64,
        action: Option<&str>,
        outcome: Option<&str>,
    ) -> std::result::Result<
        (Vec<marketcow_storage::AdminAuditRecord>, bool),
        marketcow_storage::RepositoryError,
    > {
        if let Some(repository) = &self.repository {
            return repository
                .list(limit, offset, action, outcome)
                .await
                .map(|records| (records, true));
        }
        if !(1..=200).contains(&limit)
            || !(0..=10_000).contains(&offset)
            || action.is_some_and(|value| {
                value.is_empty() || value.len() > 120 || value.chars().any(char::is_control)
            })
            || outcome.is_some_and(|value| {
                !matches!(value, "accepted" | "succeeded" | "rejected" | "failed")
            })
        {
            return Err(marketcow_storage::RepositoryError::InvalidInput);
        }
        let bytes = fs::read(&self.local.path)
            .map_err(|_| marketcow_storage::RepositoryError::Unavailable)?;
        let mut records = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .filter_map(|line| serde_json::from_slice(line).ok())
            .filter(|record: &marketcow_storage::AdminAuditRecord| {
                action.is_none_or(|value| record.action == value)
                    && outcome.is_none_or(|value| record.outcome == value)
            })
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .occurred_at
                .cmp(&left.occurred_at)
                .then_with(|| right.audit_id.cmp(&left.audit_id))
        });
        let offset = usize::try_from(offset)
            .map_err(|_| marketcow_storage::RepositoryError::InvalidInput)?;
        let limit =
            usize::try_from(limit).map_err(|_| marketcow_storage::RepositoryError::InvalidInput)?;
        Ok((
            records.into_iter().skip(offset).take(limit).collect(),
            false,
        ))
    }

    async fn record_admin(&self, record: &marketcow_storage::AdminAuditRecord) -> Result<()> {
        self.local.record(&serde_json::to_value(record)?, true)?;
        if let Some(repository) = &self.repository {
            repository
                .append(record)
                .await
                .map_err(|error| anyhow::anyhow!(error))?;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    match Cli::parse().command {
        Command::Doctor => {
            let config = Config::load()?;
            preflight(&config)?;
            println!("{}", serde_json::to_string_pretty(&config)?);
        }
        Command::Migrate { dry_run } => {
            let config = Config::load()?;
            preflight(&config)?;
            if !dry_run {
                bail!("only safe dry-run migrations exist in the shadow milestone");
            }
            println!("{{\"status\":\"dry_run_ok\",\"destructive\":false}}");
        }
        Command::Wal {
            command: WalCommand::Verify { path },
        } => {
            let events = marketcow_core::SegmentedWal::verify(path)?;
            println!(
                "{}",
                json!({"status":"ok","records":events.len(),"last_cursor":events.last().map(|x|x.event.cursor)})
            );
        }
        Command::Replay { input, checkpoint } => {
            let config = Config::load()?;
            preflight(&config)?;
            let result = replay_file(&config, &input, checkpoint)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Command::ShadowSoak {
            duration_seconds,
            interval_millis,
            output,
        } => {
            let config = Config::load()?;
            preflight(&config)?;
            let result =
                run_headless_shadow_soak(&config, duration_seconds, interval_millis, &output)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            if result["passed"] != true {
                bail!("headless shadow soak gate failed");
            }
        }
        Command::Serve => serve().await?,
    }
    Ok(())
}

fn preflight(config: &Config) -> Result<()> {
    fs::create_dir_all(&config.wal_root)?;
    let canonical = fs::canonicalize(&config.storage_root).context("storage root must exist")?;
    if canonical == Path::new("/") {
        bail!("storage root cannot be filesystem root");
    }
    let probe = config.storage_root.join(".preflight-write");
    fs::write(&probe, b"shadow")?;
    fs::remove_file(probe)?;
    if let (Some(executable), Some(script)) = (
        &config.python_workers.executable,
        &config.python_workers.script,
    ) {
        let executable_metadata = fs::metadata(executable).with_context(|| {
            format!("worker executable is unavailable: {}", executable.display())
        })?;
        if !executable_metadata.is_file() || executable_metadata.permissions().mode() & 0o111 == 0 {
            bail!("worker executable must be an executable regular file");
        }
        if !fs::metadata(script)
            .with_context(|| format!("worker script is unavailable: {}", script.display()))?
            .is_file()
        {
            bail!("worker script must be a regular file");
        }
        if !Path::new("/bin/ps").is_file() {
            bail!("worker RSS monitor requires /bin/ps");
        }
    }
    for (capability, path) in &config.python_workers.secret_references {
        open_worker_secret(path).with_context(|| {
            format!("worker secret reference is invalid for capability {capability}")
        })?;
    }
    Ok(())
}

fn open_worker_secret(path: &Path) -> Result<std::fs::File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .context("worker secret reference is unavailable")?;
    let metadata = file
        .metadata()
        .context("worker secret metadata is unavailable")?;
    if !metadata.is_file() {
        bail!("worker secret reference must be a regular file");
    }
    // SAFETY: geteuid has no preconditions and does not mutate process state.
    if metadata.uid() != unsafe { libc::geteuid() } {
        bail!("worker secret reference must be owned by the MarketCow user");
    }
    if metadata.mode() & 0o077 != 0 {
        bail!("worker secret reference must not grant group or other permissions");
    }
    if metadata.len() == 0 || metadata.len() > 64 * 1024 {
        bail!("worker secret reference size must be between 1 and 65536 bytes");
    }
    Ok(file)
}

fn sanitize_worker_command(
    command: &mut ProcessCommand,
    config: &PythonWorkerConfig,
    socket: &Path,
    capability: &str,
) {
    command
        .env_clear()
        .env("PYTHONNOUSERSITE", "1")
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env("LC_ALL", "C.UTF-8")
        .arg(config.script.as_ref().expect("enabled worker has script"))
        .arg("--socket")
        .arg(socket)
        .arg("--revision")
        .arg(&config.revision)
        .arg("--capability")
        .arg(capability)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    apply_worker_resource_limits(command, config);
}

fn attach_worker_secret(
    command: &mut ProcessCommand,
    config: &PythonWorkerConfig,
    capability: &str,
) -> Result<()> {
    let Some(path) = config.secret_references.get(capability) else {
        return Ok(());
    };
    let secret_file = open_worker_secret(path)?;
    command.env("MARKETCOW_PROVIDER_SECRET_FD", "3");
    // SAFETY: the callback invokes only async-signal-safe fcntl/dup2 operations after fork. The
    // captured File keeps the source descriptor open until spawning completes; its content and
    // path are never placed in the child environment or command line.
    unsafe {
        command.as_std_mut().pre_exec(move || {
            let source = secret_file.as_raw_fd();
            if source == 3 {
                if libc::fcntl(source, libc::F_SETFD, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
            } else if libc::dup2(source, 3) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}

fn apply_worker_resource_limits(command: &mut ProcessCommand, config: &PythonWorkerConfig) {
    #[cfg(target_os = "linux")]
    let memory_limit_mib = config.memory_limit_mib;
    let cpu_seconds = config.cpu_limit_seconds as libc::rlim_t;
    // SAFETY: the callback only invokes async-signal-safe setrlimit calls with Copy values and
    // constructs an OS error on failure. It does not access shared process state after fork.
    unsafe {
        command.as_std_mut().pre_exec(move || {
            #[cfg(target_os = "linux")]
            set_process_limit(
                libc::RLIMIT_AS as libc::c_int,
                config_memory_bytes(memory_limit_mib)?,
            )?;
            set_process_limit(libc::RLIMIT_CPU as libc::c_int, cpu_seconds)?;
            set_process_limit(libc::RLIMIT_NOFILE as libc::c_int, 256)?;
            set_process_limit(libc::RLIMIT_CORE as libc::c_int, 0)?;
            Ok(())
        });
    }
}

#[cfg(target_os = "linux")]
fn config_memory_bytes(memory_limit_mib: u64) -> std::io::Result<libc::rlim_t> {
    memory_limit_mib
        .checked_mul(1024 * 1024)
        .map(|value| value as libc::rlim_t)
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "memory limit overflow")
        })
}

fn set_process_limit(resource: libc::c_int, value: libc::rlim_t) -> std::io::Result<()> {
    let limit = libc::rlimit {
        rlim_cur: value,
        rlim_max: value,
    };
    // SAFETY: `limit` points to a fully initialized rlimit value for the duration of the call.
    if unsafe { libc::setrlimit(resource as _, &limit) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn worker_command(
    config: &PythonWorkerConfig,
    socket: &Path,
    capability: &str,
) -> Result<ProcessCommand> {
    let mut command = ProcessCommand::new(
        config
            .executable
            .as_ref()
            .expect("enabled worker has executable"),
    );
    sanitize_worker_command(&mut command, config, socket, capability);
    attach_worker_secret(&mut command, config, capability)?;
    Ok(command)
}

fn consume_restart_budget(
    restart_times: &mut VecDeque<Instant>,
    now: Instant,
    window: Duration,
    max_restarts: usize,
) -> Option<Duration> {
    while restart_times
        .front()
        .is_some_and(|at| now.duration_since(*at) >= window)
    {
        restart_times.pop_front();
    }
    if restart_times.len() >= max_restarts {
        return Some(window.saturating_sub(
            now.duration_since(*restart_times.front().expect("restart budget is nonempty")),
        ));
    }
    restart_times.push_back(now);
    None
}

#[derive(Debug)]
enum WorkerExit {
    Exited(ExitStatus),
    MemoryLimitExceeded { resident_kib: u64, limit_kib: u64 },
}

async fn resident_memory_kib(pid: u32) -> std::io::Result<u64> {
    let output = ProcessCommand::new("/bin/ps")
        .env_clear()
        .arg("-o")
        .arg("rss=")
        .arg("-p")
        .arg(pid.to_string())
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await?;
    if !output.status.success() {
        return Err(std::io::Error::other("worker RSS probe failed"));
    }
    std::str::from_utf8(&output.stdout)
        .map_err(|_| std::io::Error::other("worker RSS probe was not UTF-8"))?
        .trim()
        .parse::<u64>()
        .map_err(|_| std::io::Error::other("worker RSS probe was not numeric"))
}

async fn wait_for_worker(
    child: &mut tokio::process::Child,
    memory_limit_mib: u64,
) -> std::io::Result<WorkerExit> {
    let pid = child
        .id()
        .ok_or_else(|| std::io::Error::other("worker PID unavailable"))?;
    let limit_kib = memory_limit_mib
        .checked_mul(1024)
        .ok_or_else(|| std::io::Error::other("worker memory limit overflow"))?;
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(WorkerExit::Exited(status));
        }
        interval.tick().await;
        match resident_memory_kib(pid).await {
            Ok(resident_kib) if resident_kib > limit_kib => {
                child.kill().await?;
                return Ok(WorkerExit::MemoryLimitExceeded {
                    resident_kib,
                    limit_kib,
                });
            }
            Ok(_) => {}
            Err(error) => {
                if let Some(status) = child.try_wait()? {
                    return Ok(WorkerExit::Exited(status));
                }
                child.kill().await?;
                return Err(error);
            }
        }
    }
}

async fn supervise_worker_slot(
    slot: usize,
    capability: String,
    config: PythonWorkerConfig,
    socket: PathBuf,
    status: Arc<PythonWorkerStatus>,
) {
    let window = Duration::from_secs(config.restart_window_seconds);
    let backoff = Duration::from_millis(config.restart_backoff_millis);
    let mut restart_times = VecDeque::new();
    let mut first_spawn = true;
    loop {
        if !first_spawn {
            let now = Instant::now();
            if let Some(wait) =
                consume_restart_budget(&mut restart_times, now, window, config.max_restarts)
            {
                status.budget_exhaustions.fetch_add(1, Ordering::Relaxed);
                warn!(
                    slot,
                    wait_ms = wait.as_millis(),
                    "python_worker_restart_budget_exhausted"
                );
                tokio::time::sleep(wait).await;
                continue;
            }
            status.restarts.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(backoff).await;
        }
        first_spawn = false;
        let mut command = match worker_command(&config, &socket, &capability) {
            Ok(command) => command,
            Err(error) => {
                warn!(slot, capability, error=%error, "python_worker_command_failed_closed");
                continue;
            }
        };
        match command.spawn() {
            Ok(mut child) => {
                status.live.fetch_add(1, Ordering::Relaxed);
                info!(slot, capability, pid=?child.id(), "python_worker_started");
                let result = wait_for_worker(&mut child, config.memory_limit_mib).await;
                status.live.fetch_sub(1, Ordering::Relaxed);
                match result {
                    Ok(WorkerExit::Exited(exit_status)) => {
                        warn!(slot, capability, status=?exit_status, "python_worker_exited");
                    }
                    Ok(WorkerExit::MemoryLimitExceeded {
                        resident_kib,
                        limit_kib,
                    }) => {
                        status.memory_limit_kills.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            slot,
                            capability,
                            resident_kib,
                            limit_kib,
                            "python_worker_memory_limit_exceeded"
                        );
                    }
                    Err(error) => {
                        status
                            .memory_monitor_failures
                            .fetch_add(1, Ordering::Relaxed);
                        warn!(slot, capability, error=%error, "python_worker_memory_monitor_failed_closed");
                    }
                }
            }
            Err(error) => warn!(slot, capability, error=%error, "python_worker_spawn_failed"),
        }
    }
}

async fn supervise_worker_pool(
    config: PythonWorkerConfig,
    socket: PathBuf,
    status: Arc<PythonWorkerStatus>,
) {
    for _ in 0..200 {
        if fs::metadata(&socket).is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if fs::metadata(&socket).is_err() {
        warn!(path=%socket.display(), "python_worker_socket_startup_timeout");
        return;
    }
    let mut slots = JoinSet::new();
    let capabilities = config.dispatch_policies.keys().cloned().collect::<Vec<_>>();
    for slot in 0..config.pool_size {
        slots.spawn(supervise_worker_slot(
            slot,
            capabilities[slot % capabilities.len()].clone(),
            config.clone(),
            socket.clone(),
            status.clone(),
        ));
    }
    while slots.join_next().await.is_some() {}
}

async fn serve() -> Result<()> {
    let config = Config::load()?;
    preflight(&config)?;
    let audit = Arc::new(AuditCoordinator::open(&config).await?);
    let control_plane = Arc::new(ControlPlaneCoordinator::open(&config).await?);
    let runtime = marketcow_runtime::PolymarketRuntime::open(runtime_config(
        &config,
        &control_plane.config_revision,
    ))?;
    let projection = runtime.projection();
    let recent_events = runtime.recent_events().to_vec();
    let jobs = Arc::new(
        DurableJobCoordinator::open(
            &config.profile,
            config.python_workers.dispatch_policies.clone(),
        )
        .await?,
    );
    let instruments = Arc::new(InstrumentCoordinator::open(&config.profile).await?);
    let legacy_mcp = config
        .legacy_mcp_url
        .clone()
        .map(LegacyMcpProxy::new)
        .transpose()?;
    let (stream, _) = broadcast::channel(STREAM_CHANNEL_CAPACITY);
    let state = AppState {
        config: config.clone(),
        audit,
        metrics: Arc::new(Metrics::default()),
        projection: Arc::new(ArcSwap::from(projection)),
        recent_events: Arc::new(ArcSwap::from_pointee(recent_events)),
        runtime: Arc::new(AsyncMutex::new(runtime)),
        jobs,
        instruments,
        control_plane,
        stream,
        worker_status: Arc::new(PythonWorkerStatus::default()),
        legacy_mcp,
    };
    let worker_path = config.worker_socket.clone();
    let worker_jobs = state.jobs.clone();
    let worker_staging_root = config.storage_root.join("worker-staging");
    let worker_artifact_root = config.storage_root.join("artifacts");
    let worker = tokio::spawn(async move {
        worker_server(
            worker_path,
            worker_staging_root,
            worker_artifact_root,
            worker_jobs,
        )
        .await
    });
    let worker_pool = config.python_workers.enabled().then(|| {
        tokio::spawn(supervise_worker_pool(
            config.python_workers.clone(),
            config.worker_socket.clone(),
            state.worker_status.clone(),
        ))
    });
    let app = app(state);
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    info!(bind=%config.bind, shadow=true, "marketcowd_ready");
    let server_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await;
    if let Some(worker_pool) = worker_pool {
        worker_pool.abort();
        let _ = worker_pool.await;
    }
    worker.abort();
    let _ = worker.await;
    let _ = fs::remove_file(&config.worker_socket);
    info!("marketcowd_stopped");
    server_result?;
    Ok(())
}

fn runtime_config(config: &Config, config_revision: &str) -> marketcow_runtime::RuntimeConfig {
    marketcow_runtime::RuntimeConfig {
        root: config.storage_root.join("polymarket"),
        scope_id: config.scope_id.clone(),
        config_revision: config_revision.into(),
        wal_segment_bytes: 256 * 1024 * 1024,
        recent_event_capacity: 10_000,
    }
}

#[derive(Debug, Deserialize)]
struct ReplayInput {
    received_at: chrono::DateTime<Utc>,
    raw_payload: serde_json::Value,
}

fn replay_file(config: &Config, input: &Path, checkpoint: bool) -> Result<serde_json::Value> {
    use std::io::{BufRead, BufReader};

    let revision = calculated_config_revision(config)?;
    let mut runtime =
        marketcow_runtime::PolymarketRuntime::open(runtime_config(config, &revision))?;
    let mut input_count = 0_u64;
    let mut event_count = 0_u64;
    let mut rejected_count = 0_u64;
    for line in BufReader::new(std::fs::File::open(input)?).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value: ReplayInput = serde_json::from_str(&line)?;
        input_count += 1;
        for outcome in runtime.apply_raw(value.raw_payload, value.received_at)? {
            event_count += 1;
            if !outcome.persisted.applied {
                rejected_count += 1;
            }
        }
    }
    let manifest = checkpoint.then(|| runtime.checkpoint()).transpose()?;
    let projection = runtime.projection();
    Ok(json!({
        "status":"replay_complete",
        "input_frames":input_count,
        "canonical_events":event_count,
        "rejected_events":rejected_count,
        "published_cursor":projection.cursor,
        "persisted_cursor":projection.persisted_cursor,
        "projection_sha256":projection.hash(),
        "ready":projection.ready,
        "checkpoint_cursor":manifest.map(|value|value.current.cursor),
        "real_order_submission_enabled":false
    }))
}

fn run_headless_shadow_soak(
    config: &Config,
    duration_seconds: u64,
    interval_millis: u64,
    output: &Path,
) -> Result<serde_json::Value> {
    if duration_seconds == 0 || interval_millis == 0 || !output.is_absolute() {
        bail!("soak duration/interval must be positive and output must be absolute");
    }
    let binary_commit = env::var("MARKETCOW_BINARY_COMMIT")
        .context("MARKETCOW_BINARY_COMMIT is required for attributable soak evidence")?;
    if binary_commit.is_empty() || binary_commit.len() > 128 {
        bail!("MARKETCOW_BINARY_COMMIT must be present and at most 128 characters");
    }
    let binary_path = env::current_exe()?.canonicalize()?;
    let binary_sha256 = hex::encode(Sha256::digest(fs::read(&binary_path)?));
    let revision = calculated_config_revision(config)?;
    let mut runtime =
        marketcow_runtime::PolymarketRuntime::open(runtime_config(config, &revision))?;
    let started_at = Utc::now();
    let started = std::time::Instant::now();
    let deadline = started + std::time::Duration::from_secs(duration_seconds);
    let mut sequence = 0_u64;
    let mut event_count = 0_u64;
    let mut checkpoint_count = 0_u64;
    let mut rejected_count = 0_u64;
    let mut maximum_book_age_ms = 0_u64;
    let mut maximum_persistence_latency_us = 0_u64;
    let mut maximum_publication_latency_us = 0_u64;
    let mut apply_latency_us = Vec::new();
    let mut next_checkpoint = started + std::time::Duration::from_secs(60);

    for (token_id, bid, ask) in [
        ("binary-yes", "0.40", "0.42"),
        ("binary-no", "0.58", "0.60"),
    ] {
        let now = Utc::now();
        let raw = json!({
            "event_type":"book", "asset_id":token_id, "timestamp":now,
            "tick_size":"0.01", "bids":[{"price":bid,"size":"10"}],
            "asks":[{"price":ask,"size":"11"}],
            "hash":format!("headless-{token_id}-{}", now.timestamp_nanos_opt().unwrap_or_default())
        });
        for outcome in runtime.apply_raw(raw, now)? {
            event_count += 1;
            if !outcome.persisted.applied {
                rejected_count += 1;
            }
            maximum_persistence_latency_us =
                maximum_persistence_latency_us.max(outcome.persistence_latency_us);
            maximum_publication_latency_us =
                maximum_publication_latency_us.max(outcome.publication_latency_us);
        }
    }

    while std::time::Instant::now() < deadline {
        let now = Utc::now();
        let size = (10 + sequence % 20).to_string();
        let apply_started = std::time::Instant::now();
        let outcomes = runtime.apply_raw(
            json!({
                "event_type":"price_change", "timestamp":now,
                "price_changes":[
                    {"asset_id":"binary-yes","side":"BUY","price":"0.40","size":size.clone()},
                    {"asset_id":"binary-no","side":"SELL","price":"0.60","size":size}
                ]
            }),
            now,
        )?;
        apply_latency_us.push(apply_started.elapsed().as_micros() as u64);
        for outcome in outcomes {
            event_count += 1;
            if !outcome.persisted.applied {
                rejected_count += 1;
            }
            maximum_persistence_latency_us =
                maximum_persistence_latency_us.max(outcome.persistence_latency_us);
            maximum_publication_latency_us =
                maximum_publication_latency_us.max(outcome.publication_latency_us);
        }
        let projection = runtime.projection();
        maximum_book_age_ms = maximum_book_age_ms.max(
            projection
                .books
                .values()
                .filter_map(|book| book.source_observed_at)
                .map(|observed| {
                    Utc::now()
                        .signed_duration_since(observed)
                        .num_milliseconds()
                        .max(0) as u64
                })
                .max()
                .unwrap_or(u64::MAX),
        );
        if std::time::Instant::now() >= next_checkpoint {
            runtime.checkpoint()?;
            checkpoint_count += 1;
            next_checkpoint += std::time::Duration::from_secs(60);
        }
        sequence += 1;
        std::thread::sleep(std::time::Duration::from_millis(interval_millis));
    }
    runtime.checkpoint()?;
    checkpoint_count += 1;
    let projection = runtime.projection();
    let elapsed_seconds = started.elapsed().as_secs_f64();
    apply_latency_us.sort_unstable();
    let percentile = |ratio: f64| -> u64 {
        if apply_latency_us.is_empty() {
            return u64::MAX;
        }
        let index = ((apply_latency_us.len() as f64 * ratio).ceil() as usize)
            .saturating_sub(1)
            .min(apply_latency_us.len() - 1);
        apply_latency_us[index]
    };
    let max_rss_kb = maximum_resident_set_kb();
    let gate_verdicts = json!({
        "duration_reached":elapsed_seconds >= duration_seconds as f64,
        "rejected_events_zero":rejected_count == 0,
        "projection_ready":projection.ready,
        "unresolved_gap_count_zero":projection.unresolved_gaps.is_empty(),
        "published_cursor_equals_persisted_cursor":projection.cursor == projection.persisted_cursor,
        "maximum_book_age_within_limit":maximum_book_age_ms
            <= interval_millis.saturating_mul(2).max(5_000),
        "apply_latency_p99_us_lte_50000":percentile(0.99) <= 50_000,
        "maximum_persistence_latency_us_lte_50000":maximum_persistence_latency_us <= 50_000,
        "maximum_publication_latency_us_lte_5000":maximum_publication_latency_us <= 5_000,
        "real_orders_disabled":!config.real_order_submission_enabled,
        "tradude_does_not_manage_marketcow":true
    });
    let passed = gate_verdicts
        .as_object()
        .expect("gate verdicts are an object")
        .values()
        .all(|value| value == &serde_json::Value::Bool(true));
    let result = json!({
        "schema_version":"marketcow.headless-shadow-soak.v2",
        "binary_commit":binary_commit,
        "binary_path":binary_path,
        "binary_sha256":binary_sha256,
        "storage_root":config.storage_root,
        "started_at":started_at,
        "finished_at":Utc::now(),
        "requested_duration_seconds":duration_seconds,
        "elapsed_seconds":elapsed_seconds,
        "samples":apply_latency_us.len(),
        "canonical_events":event_count,
        "rejected_events":rejected_count,
        "checkpoint_count":checkpoint_count,
        "published_cursor":projection.cursor,
        "persisted_cursor":projection.persisted_cursor,
        "unresolved_gap_count":projection.unresolved_gaps.len(),
        "book_count":projection.books.len(),
        "maximum_book_age_ms":maximum_book_age_ms,
        "ingress_queue_depth":0,
        "disconnect_count":0,
        "apply_latency_us":{"p50":percentile(0.50),"p95":percentile(0.95),"p99":percentile(0.99),
            "max":apply_latency_us.last().copied().unwrap_or(u64::MAX)},
        "maximum_persistence_latency_us":maximum_persistence_latency_us,
        "maximum_publication_latency_us":maximum_publication_latency_us,
        "max_rss_kb":max_rss_kb,
        "gate_verdicts":gate_verdicts,
        "real_order_submission_enabled":false,
        "tradude_manages_marketcow":false,
        "passed":passed
    });
    let temporary = output.with_extension("tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(&result)?)?;
    std::fs::File::open(&temporary)?.sync_all()?;
    fs::rename(&temporary, output)?;
    std::fs::File::open(output.parent().context("soak output needs a parent")?)?.sync_all()?;
    Ok(result)
}

fn maximum_resident_set_kb() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided rusage on a zero return code.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the successful getrusage call above initialized usage.
    let bytes_or_kb = unsafe { usage.assume_init() }.ru_maxrss;
    #[cfg(target_os = "macos")]
    let kilobytes = bytes_or_kb / 1024;
    #[cfg(not(target_os = "macos"))]
    let kilobytes = bytes_or_kb;
    u64::try_from(kilobytes).ok()
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/readiness", get(readiness))
        .route("/v1/prediction-markets/polymarket/live/scope", get(scope))
        .route(
            "/v1/prediction-markets/polymarket/live/snapshot",
            get(live_snapshot),
        )
        .route(
            "/v1/prediction-markets/polymarket/live/events",
            get(live_events),
        )
        .route(
            "/v1/prediction-markets/polymarket/live/checkpoint",
            get(live_checkpoint),
        )
        .route(
            "/v1/prediction-markets/polymarket/live/full-sync",
            get(live_full_sync),
        )
        .route("/v1/market-data/stream", get(market_data_stream))
        .route("/v1/instruments:resolve", get(resolve_instrument))
        .route(
            "/v1/instruments:resolve/query",
            post(resolve_instruments_batch),
        )
        .route("/v1/instruments/{instrument_id}", get(get_instrument))
        .route("/mcp", post(mcp))
        .route("/metrics", get(metrics))
        .route("/v1/admin/migration", get(admin_migration))
        .route(
            "/v1/admin/migration/checkpoints/{run_id}/{domain}/{shard}",
            get(admin_get_migration_checkpoint).put(admin_put_migration_checkpoint),
        )
        .route("/v1/admin/audit", get(admin_audit_events))
        .route(
            "/v1/admin/polymarket/checkpoint",
            axum::routing::post(admin_checkpoint),
        )
        .route(
            "/v1/admin/polymarket/shadow-ingest",
            axum::routing::post(admin_shadow_ingest),
        )
        .route("/v1/admin/jobs", axum::routing::post(admin_submit_job))
        .route(
            "/v1/admin/instruments/{instrument_id}",
            axum::routing::put(admin_upsert_instrument),
        )
        .route(
            "/v1/admin/jobs/{job_id}",
            get(admin_get_job).post(admin_cancel_job),
        )
        .layer(DefaultBodyLimit::max(1_048_576))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            request_boundary,
        ))
        .with_state(state)
}

fn health_payload(state: &AppState) -> serde_json::Value {
    let configured_workers = state.config.python_workers.pool_size as u64;
    let live_workers = state.worker_status.live.load(Ordering::Relaxed);
    let worker_health = if configured_workers == 0 {
        "disabled_optional"
    } else if live_workers == configured_workers {
        "healthy"
    } else {
        "degraded"
    };
    json!({
        "status":"healthy", "service":"marketcowd", "profile":state.config.profile,
        "shadow_mode":true, "real_order_submission_enabled":false,
        "mcp":{
            "enabled":true,
            "endpoint":"/mcp",
            "native_tools":2,
            "legacy_proxy_configured":state.legacy_mcp.is_some()
        },
        "components":{
            "api":"healthy","wal":"healthy","python_workers":worker_health,
            "job_persistence":if state.jobs.persistence_enabled() { "healthy" } else { "degraded_development_only" },
            "instrument_persistence":if state.instruments.persistence_enabled() { "healthy" } else { "degraded_development_only" },
            "control_plane_persistence":if state.control_plane.persistence_enabled() { "healthy" } else { "degraded_development_only" },
            "audit_persistence":if state.audit.persistence_enabled() { "healthy" } else { "degraded_development_only" }
        },
        "config_revision":state.control_plane.config_revision,
        "python_worker_pool":{
            "configured":configured_workers,
            "live":live_workers,
            "restarts":state.worker_status.restarts.load(Ordering::Relaxed),
            "restart_budget_exhaustions":state.worker_status.budget_exhaustions.load(Ordering::Relaxed),
            "memory_limit_kills":state.worker_status.memory_limit_kills.load(Ordering::Relaxed),
            "memory_monitor_failures":state.worker_status.memory_monitor_failures.load(Ordering::Relaxed),
            "memory_limit_mib":state.config.python_workers.memory_limit_mib,
            "cpu_limit_seconds":state.config.python_workers.cpu_limit_seconds,
            "dispatch_policies":&state.config.python_workers.dispatch_policies
        }
    })
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(health_payload(&state))
}

async fn instrument_lookup(
    state: &AppState,
    instrument_id: &str,
) -> Result<marketcow_storage::InstrumentRecord, (StatusCode, serde_json::Value)> {
    match state.instruments.get(instrument_id).await {
        Ok(Some(record)) => Ok(record),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            json!({"code":"instrument_not_found","instrument_id":instrument_id}),
        )),
        Err(marketcow_storage::RepositoryError::InvalidInput) => Err((
            StatusCode::BAD_REQUEST,
            json!({"code":"invalid_instrument_id","instrument_id":instrument_id}),
        )),
        Err(_) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"code":"instrument_repository_unavailable"}),
        )),
    }
}

async fn get_instrument(
    State(state): State<AppState>,
    AxumPath(instrument_id): AxumPath<String>,
) -> Response {
    match instrument_lookup(&state, &instrument_id).await {
        Ok(record) => Json(record).into_response(),
        Err((status, detail)) => (status, Json(json!({"detail":detail}))).into_response(),
    }
}

#[derive(Deserialize)]
struct ResolveInstrumentQuery {
    namespace: String,
    external_symbol: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolveInstrumentBatchRequest {
    namespace: String,
    symbols: Vec<String>,
}

fn instrument_resolution_error_item(
    namespace: &str,
    external_symbol: &str,
    code: &str,
    message: &str,
) -> serde_json::Value {
    json!({
        "namespace":namespace,
        "external_symbol":external_symbol,
        "status":"error",
        "instrument_id":null,
        "symbol":null,
        "mic":null,
        "market":null,
        "currency":null,
        "source":null,
        "source_exchange":null,
        "observed_at":null,
        "resolution":null,
        "error":{"code":code,"message":message}
    })
}

fn instrument_resolution_registry_item(
    namespace: &str,
    external_symbol: &str,
    record: &marketcow_storage::InstrumentRecord,
) -> serde_json::Value {
    json!({
        "namespace":namespace,
        "external_symbol":external_symbol,
        "status":"resolved",
        "instrument_id":record.instrument_id,
        "symbol":record.symbol,
        "mic":record.mic,
        "market":record.market,
        "currency":record.currency,
        "source":"instrument_mapping_registry",
        "source_exchange":null,
        "observed_at":record.updated_at,
        "resolution":"registry",
        "error":null
    })
}

fn instrument_resolution_response(namespace: &str, items: Vec<serde_json::Value>) -> Response {
    let resolved_count = items
        .iter()
        .filter(|item| item["status"] == "resolved")
        .count();
    Json(json!({
        "namespace":namespace,
        "count":items.len(),
        "resolved_count":resolved_count,
        "error_count":items.len() - resolved_count,
        "items":items
    }))
    .into_response()
}

async fn resolve_instruments_batch(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    Json(request): Json<ResolveInstrumentBatchRequest>,
) -> Response {
    let namespace = request.namespace.trim().to_ascii_lowercase();
    let valid_namespace = namespace.split_once(':').is_some_and(|(kind, name)| {
        matches!(kind, "provider" | "broker")
            && !name.is_empty()
            && name.len() <= 64
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    });
    let symbols = request
        .symbols
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase().replace(' ', ""))
        .collect::<Vec<_>>();
    if !valid_namespace
        || !(1..=20).contains(&symbols.len())
        || symbols
            .iter()
            .any(|symbol| symbol.is_empty() || symbol.len() > 128)
    {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_instrument_resolution_request",
            false,
            &request_id,
        );
    }

    let mut items = vec![serde_json::Value::Null; symbols.len()];
    let mut missing = Vec::new();
    for (position, symbol) in symbols.iter().enumerate() {
        match state.instruments.resolve(&namespace, symbol).await {
            Ok(Some(record)) => {
                items[position] = instrument_resolution_registry_item(&namespace, symbol, &record);
            }
            Ok(None) => missing.push(position),
            Err(marketcow_storage::RepositoryError::InvalidInput) => {
                return error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "invalid_instrument_resolution_request",
                    false,
                    &request_id,
                );
            }
            Err(_) => {
                return instrument_resolution_response(
                    &namespace,
                    symbols
                        .iter()
                        .map(|symbol| {
                            instrument_resolution_error_item(
                                &namespace,
                                symbol,
                                "provider_unavailable",
                                "instrument registry is unavailable",
                            )
                        })
                        .collect(),
                );
            }
        }
    }
    if missing.is_empty() {
        return instrument_resolution_response(&namespace, items);
    }
    if namespace != "provider:longport" {
        for position in missing {
            items[position] = instrument_resolution_error_item(
                &namespace,
                &symbols[position],
                "provider_unavailable",
                &format!("dynamic instrument resolution is unavailable for {namespace}"),
            );
        }
        return instrument_resolution_response(&namespace, items);
    }
    if !state
        .config
        .python_workers
        .dispatch_policies
        .contains_key(LONGPORT_RESOLVE_TASK)
        || !state
            .config
            .python_workers
            .secret_references
            .contains_key(LONGPORT_RESOLVE_TASK)
        || state.worker_status.live.load(Ordering::Relaxed) == 0
    {
        for position in missing {
            items[position] = instrument_resolution_error_item(
                &namespace,
                &symbols[position],
                "provider_unavailable",
                "LongPort credentials are not configured",
            );
        }
        return instrument_resolution_response(&namespace, items);
    }

    let missing_symbols = missing
        .iter()
        .map(|position| symbols[*position].clone())
        .collect::<Vec<_>>();
    let now = Utc::now();
    let deadline = now + chrono::Duration::seconds(20);
    let job = match state
        .jobs
        .submit(
            marketcow_jobs::SubmitJob {
                idempotency_key: format!("instrument-resolve:{request_id}"),
                job_type: LONGPORT_RESOLVE_TASK.into(),
                request_schema: LONGPORT_RESOLVE_REQUEST_SCHEMA.into(),
                request: json!({"namespace":namespace,"symbols":missing_symbols}),
                deadline,
                max_attempts: 1,
                audit_actor: format!("public_request:{request_id}"),
            },
            now,
        )
        .await
    {
        Ok(job) => job,
        Err(_) => {
            for position in missing {
                items[position] = instrument_resolution_error_item(
                    &namespace,
                    &symbols[position],
                    "provider_unavailable",
                    "LongPort resolution dispatch failed",
                );
            }
            return instrument_resolution_response(&namespace, items);
        }
    };
    let terminal = state.jobs.wait_terminal(&job.job_id, deadline).await;
    let worker_items = terminal
        .ok()
        .filter(|job| job.status == marketcow_jobs::JobStatus::Succeeded)
        .and_then(|job| read_longport_worker_result(&state.config, &job).ok());
    let Some((observed_at, worker_items)) = worker_items else {
        for position in missing {
            items[position] = instrument_resolution_error_item(
                &namespace,
                &symbols[position],
                "provider_unavailable",
                "LongPort resolution worker is unavailable",
            );
        }
        return instrument_resolution_response(&namespace, items);
    };
    if worker_items.len() != missing.len() {
        for position in missing {
            items[position] = instrument_resolution_error_item(
                &namespace,
                &symbols[position],
                "provider_unavailable",
                "LongPort returned an incomplete resolution batch",
            );
        }
        return instrument_resolution_response(&namespace, items);
    }
    for (position, worker_item) in missing.into_iter().zip(worker_items) {
        items[position] = persist_longport_resolution(
            &state,
            &namespace,
            &symbols[position],
            observed_at,
            worker_item,
        )
        .await;
    }
    instrument_resolution_response(&namespace, items)
}

fn instrument_schema_version() -> u8 {
    1
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstrumentInput {
    #[serde(default = "instrument_schema_version")]
    schema_version: u8,
    instrument_id: String,
    instrument_type: String,
    asset_class: String,
    symbol: String,
    market: String,
    mic: String,
    currency: String,
    price_precision: u8,
    size_precision: u8,
    #[serde(with = "rust_decimal::serde::str")]
    tick_size: rust_decimal::Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    size_increment: rust_decimal::Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    lot_size: rust_decimal::Decimal,
    ts_event: chrono::DateTime<Utc>,
    ts_init: chrono::DateTime<Utc>,
    provider_symbols: BTreeMap<String, String>,
    #[serde(default)]
    broker_symbols: BTreeMap<String, String>,
}

impl InstrumentInput {
    fn into_record(self, content_hash: String) -> marketcow_storage::InstrumentRecord {
        marketcow_storage::InstrumentRecord {
            schema_version: self.schema_version,
            instrument_id: self.instrument_id,
            instrument_type: self.instrument_type,
            asset_class: self.asset_class,
            symbol: self.symbol,
            market: self.market,
            mic: self.mic,
            currency: self.currency,
            price_precision: self.price_precision,
            size_precision: self.size_precision,
            tick_size: self.tick_size,
            size_increment: self.size_increment,
            lot_size: self.lot_size,
            ts_event: self.ts_event,
            ts_init: self.ts_init,
            provider_symbols: self.provider_symbols,
            broker_symbols: self.broker_symbols,
            content_hash,
            updated_at: Utc::now(),
        }
    }
}

fn instrument_record_input(record: marketcow_storage::InstrumentRecord) -> InstrumentInput {
    InstrumentInput {
        schema_version: record.schema_version,
        instrument_id: record.instrument_id,
        instrument_type: record.instrument_type,
        asset_class: record.asset_class,
        symbol: record.symbol,
        market: record.market,
        mic: record.mic,
        currency: record.currency,
        price_precision: record.price_precision,
        size_precision: record.size_precision,
        tick_size: record.tick_size,
        size_increment: record.size_increment,
        lot_size: record.lot_size,
        ts_event: record.ts_event,
        ts_init: record.ts_init,
        provider_symbols: record.provider_symbols,
        broker_symbols: record.broker_symbols,
    }
}

fn read_longport_worker_result(
    config: &Config,
    job: &marketcow_jobs::ProviderJob,
) -> Result<(chrono::DateTime<Utc>, Vec<serde_json::Value>)> {
    let result = job.result.as_ref().context("worker job result is absent")?;
    if result.sha256.len() != 64
        || !result
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("worker result identity is invalid");
    }
    let expected_relative_path = PathBuf::from(&job.job_type)
        .join(&job.request_schema)
        .join(&result.sha256[..2])
        .join(&result.sha256);
    if Path::new(&result.relative_path) != expected_relative_path {
        bail!("worker result content-addressed path is invalid");
    }
    let path = config
        .storage_root
        .join("artifacts")
        .join(expected_relative_path);
    validate_worker_result_artifact(
        &job.job_type,
        &job.request_schema,
        &path,
        result.size_bytes,
        &result.media_type,
    )?;
    let bytes = fs::read(path)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    validate_longport_resolve_result(&value)?;
    let observed_at = chrono::DateTime::parse_from_rfc3339(
        value["observed_at"]
            .as_str()
            .context("worker observed_at is absent")?,
    )?
    .with_timezone(&Utc);
    let items = value["items"]
        .as_array()
        .context("worker items are absent")?
        .clone();
    Ok((observed_at, items))
}

async fn persist_longport_resolution(
    state: &AppState,
    namespace: &str,
    external_symbol: &str,
    observed_at: chrono::DateTime<Utc>,
    worker_item: serde_json::Value,
) -> serde_json::Value {
    if worker_item["external_symbol"].as_str() != Some(external_symbol) {
        return instrument_resolution_error_item(
            namespace,
            external_symbol,
            "provider_unavailable",
            "LongPort resolution order or identity changed",
        );
    }
    if worker_item["status"] == "error" {
        return instrument_resolution_error_item(
            namespace,
            external_symbol,
            worker_item["error"]["code"]
                .as_str()
                .unwrap_or("provider_unavailable"),
            worker_item["error"]["message"]
                .as_str()
                .unwrap_or("LongPort resolution failed"),
        );
    }
    let instrument_id = worker_item["instrument_id"].as_str().unwrap_or_default();
    let source_exchange = worker_item["source_exchange"].as_str().unwrap_or_default();
    let existing = match state.instruments.get(instrument_id).await {
        Ok(record) => record,
        Err(_) => {
            return instrument_resolution_error_item(
                namespace,
                external_symbol,
                "provider_unavailable",
                "instrument registry is unavailable",
            );
        }
    };
    let mut input = if let Some(record) = existing {
        instrument_record_input(record)
    } else {
        let Ok(lot_size) = worker_item["lot_size"]
            .as_str()
            .unwrap_or_default()
            .parse::<rust_decimal::Decimal>()
        else {
            return instrument_resolution_error_item(
                namespace,
                external_symbol,
                "ambiguous",
                "LongPort lot size is invalid",
            );
        };
        InstrumentInput {
            schema_version: 1,
            instrument_id: instrument_id.into(),
            instrument_type: "equity".into(),
            asset_class: "equity".into(),
            symbol: worker_item["symbol"].as_str().unwrap_or_default().into(),
            market: worker_item["market"].as_str().unwrap_or_default().into(),
            mic: worker_item["mic"].as_str().unwrap_or_default().into(),
            currency: worker_item["currency"].as_str().unwrap_or_default().into(),
            price_precision: 2,
            size_precision: 0,
            tick_size: rust_decimal::Decimal::new(1, 2),
            size_increment: rust_decimal::Decimal::ONE,
            lot_size,
            ts_event: observed_at,
            ts_init: observed_at,
            provider_symbols: BTreeMap::new(),
            broker_symbols: BTreeMap::new(),
        }
    };
    input
        .provider_symbols
        .insert("longport".into(), external_symbol.into());
    let payload = serde_json::to_value(&input).expect("typed instrument input serializes");
    let record = input.into_record(python_canonical_hash(&payload));
    match state.instruments.upsert(&record).await {
        Ok(()) => json!({
            "namespace":namespace,
            "external_symbol":external_symbol,
            "status":"resolved",
            "instrument_id":record.instrument_id,
            "symbol":record.symbol,
            "mic":record.mic,
            "market":record.market,
            "currency":record.currency,
            "source":"longport.static_info",
            "source_exchange":source_exchange,
            "observed_at":observed_at,
            "resolution":"upstream",
            "error":null
        }),
        Err(
            marketcow_storage::RepositoryError::InvalidInput
            | marketcow_storage::RepositoryError::InstrumentConflict,
        ) => instrument_resolution_error_item(
            namespace,
            external_symbol,
            "ambiguous",
            "LongPort metadata conflicts with the instrument registry",
        ),
        Err(_) => instrument_resolution_error_item(
            namespace,
            external_symbol,
            "provider_unavailable",
            "instrument registry write failed",
        ),
    }
}

fn python_canonical_hash(value: &serde_json::Value) -> String {
    let canonical = python_canonical_json(value);
    format!(
        "sha256:{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    )
}

fn python_canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".into(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => python_ascii_json_string(value),
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(python_canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        serde_json::Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            format!(
                "{{{}}}",
                keys.iter()
                    .map(|key| format!(
                        "{}:{}",
                        python_ascii_json_string(key),
                        python_canonical_json(&values[*key])
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
    }
}

fn python_ascii_json_string(value: &str) -> String {
    let json = serde_json::to_string(value).expect("string serializes");
    let mut output = String::with_capacity(json.len());
    for character in json.chars() {
        if character.is_ascii() {
            output.push(character);
        } else {
            let codepoint = character as u32;
            if codepoint <= 0xffff {
                output.push_str(&format!("\\u{codepoint:04x}"));
            } else {
                let adjusted = codepoint - 0x1_0000;
                let high = 0xd800 + (adjusted >> 10);
                let low = 0xdc00 + (adjusted & 0x3ff);
                output.push_str(&format!("\\u{high:04x}\\u{low:04x}"));
            }
        }
    }
    output
}

async fn resolve_instrument(
    State(state): State<AppState>,
    Query(query): Query<ResolveInstrumentQuery>,
) -> Response {
    match state
        .instruments
        .resolve(&query.namespace, &query.external_symbol)
        .await
    {
        Ok(Some(record)) => Json(record).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"detail":{
                "code":"instrument_mapping_not_found",
                "namespace":query.namespace,
                "external_symbol":query.external_symbol
            }})),
        )
            .into_response(),
        Err(marketcow_storage::RepositoryError::InvalidInput) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"detail":{"code":"invalid_instrument_mapping"}})),
        )
            .into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"detail":{"code":"instrument_repository_unavailable"}})),
        )
            .into_response(),
    }
}

async fn admin_upsert_instrument(
    State(state): State<AppState>,
    AxumPath(instrument_id): AxumPath<String>,
    Json(input): Json<InstrumentInput>,
) -> Response {
    if input.instrument_id != instrument_id {
        return (
            StatusCode::CONFLICT,
            Json(json!({"detail":{
                "code":"instrument_conflict",
                "message":"path and payload instrument_id must match"
            }})),
        )
            .into_response();
    }
    let payload = serde_json::to_value(&input).expect("InstrumentInput serializes");
    let record = input.into_record(python_canonical_hash(&payload));
    match state.instruments.upsert(&record).await {
        Ok(()) => Json(record).into_response(),
        Err(
            marketcow_storage::RepositoryError::InvalidInput
            | marketcow_storage::RepositoryError::InstrumentConflict,
        ) => (
            StatusCode::CONFLICT,
            Json(json!({"detail":{
                "code":"instrument_conflict",
                "message":"instrument identity or symbol mapping conflict"
            }})),
        )
            .into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"detail":{"code":"instrument_repository_unavailable"}})),
        )
            .into_response(),
    }
}

async fn mcp(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    if headers.contains_key("origin") {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error":"browser origins are not accepted by the MCP endpoint"})),
        )
            .into_response();
    }
    let content_type = headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .unwrap_or("");
    if !content_type.eq_ignore_ascii_case("application/json") {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(json!({"error":"Content-Type must be application/json"})),
        )
            .into_response();
    }
    if let Some(protocol) = headers
        .get("mcp-protocol-version")
        .and_then(|value| value.to_str().ok())
        && !marketcow_contracts::MCP_SUPPORTED_PROTOCOL_VERSIONS.contains(&protocol)
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"unsupported MCP protocol version"})),
        )
            .into_response();
    }
    if body.len() > marketcow_contracts::MCP_MAX_REQUEST_BYTES {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({"error":"MCP request exceeds 1 MiB"})),
        )
            .into_response();
    }
    let message = match serde_json::from_slice(&body) {
        Ok(message) => message,
        Err(_) => {
            return Json(mcp_error(serde_json::Value::Null, -32700, "Parse error")).into_response();
        }
    };
    match mcp_handle_message(&state, message).await {
        Some(response) => Json(response).into_response(),
        None => StatusCode::ACCEPTED.into_response(),
    }
}

async fn mcp_handle_message(
    state: &AppState,
    message: serde_json::Value,
) -> Option<serde_json::Value> {
    if let Some(messages) = message.as_array() {
        if messages.is_empty() || messages.len() > marketcow_contracts::MCP_MAX_BATCH_MESSAGES {
            return Some(mcp_error(
                serde_json::Value::Null,
                -32600,
                if messages.is_empty() {
                    "Invalid Request"
                } else {
                    "Batch exceeds 100 messages"
                },
            ));
        }
        let mut responses = Vec::with_capacity(messages.len());
        for message in messages {
            if let Some(response) = mcp_handle_request(state, message).await {
                responses.push(response);
            }
        }
        return (!responses.is_empty()).then_some(serde_json::Value::Array(responses));
    }
    mcp_handle_request(state, &message).await
}

async fn mcp_handle_request(
    state: &AppState,
    message: &serde_json::Value,
) -> Option<serde_json::Value> {
    let Some(request) = message.as_object() else {
        return Some(mcp_error(
            serde_json::Value::Null,
            -32600,
            "Invalid Request",
        ));
    };
    let is_notification = !request.contains_key("id");
    let request_id = request
        .get("id")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let method = request.get("method").and_then(|value| value.as_str());
    if request.get("jsonrpc").and_then(|value| value.as_str()) != Some("2.0") || method.is_none() {
        return (!is_notification).then(|| mcp_error(request_id, -32600, "Invalid Request"));
    }
    let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
    if !params.is_object() {
        return (!is_notification).then(|| mcp_error(request_id, -32602, "Invalid params"));
    }
    let method = method.expect("method was validated");
    if is_notification {
        return None;
    }
    let result = match method {
        "initialize" => {
            let requested = params
                .get("protocolVersion")
                .and_then(|value| value.as_str());
            let protocol = requested
                .filter(|version| {
                    marketcow_contracts::MCP_SUPPORTED_PROTOCOL_VERSIONS.contains(version)
                })
                .unwrap_or(marketcow_contracts::MCP_LATEST_PROTOCOL_VERSION);
            json!({
                "protocolVersion":protocol,
                "capabilities":{"tools":{"listChanged":false}},
                "serverInfo":{"name":"marketcow","title":"MarketCow Financial Data","version":env!("CARGO_PKG_VERSION")},
                "instructions":"All tools are read-only and use cached MarketCow data. Search instruments first when venue identity is ambiguous. Treat provider timestamps, provenance, and quality fields as part of the analytical evidence."
            })
        }
        "ping" => json!({}),
        "tools/list" => return Some(mcp_tools_list(state, request_id).await),
        "tools/call" => return Some(mcp_tool_call(state, request_id, &params, message).await),
        _ => return Some(mcp_error(request_id, -32601, "Method not found")),
    };
    Some(json!({"jsonrpc":"2.0","id":request_id,"result":result}))
}

async fn mcp_tools_list(state: &AppState, request_id: serde_json::Value) -> serde_json::Value {
    let native_health = marketcow_contracts::mcp_service_health_tool_definition();
    let native_instrument = marketcow_contracts::mcp_get_instrument_tool_definition();
    let Some(proxy) = &state.legacy_mcp else {
        return mcp_result(
            request_id,
            json!({"tools":[native_health,native_instrument]}),
        );
    };
    let proxy_request = json!({
        "jsonrpc":"2.0",
        "id":request_id,
        "method":"tools/list",
        "params":{}
    });
    let response = match proxy.request(&proxy_request).await {
        Ok(Some(response)) => response,
        Ok(None) | Err(_) => {
            return mcp_error(request_id, -32603, "Legacy MCP proxy unavailable");
        }
    };
    let Some(tools) = response
        .pointer("/result/tools")
        .and_then(serde_json::Value::as_array)
    else {
        return mcp_error(request_id, -32603, "Legacy MCP tools contract is invalid");
    };
    let expected = marketcow_contracts::MCP_LEGACY_TOOL_NAMES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let observed = tools
        .iter()
        .filter_map(|tool| tool.get("name").and_then(serde_json::Value::as_str))
        .collect::<BTreeSet<_>>();
    let safe = tools.iter().all(|tool| {
        tool.pointer("/annotations/readOnlyHint") == Some(&serde_json::Value::Bool(true))
            && tool.pointer("/annotations/destructiveHint") == Some(&serde_json::Value::Bool(false))
    });
    if observed != expected || tools.len() != expected.len() || !safe {
        return mcp_error(request_id, -32603, "Legacy MCP tools contract is invalid");
    }
    let merged = tools
        .iter()
        .map(
            |tool| match tool.get("name").and_then(serde_json::Value::as_str) {
                Some("service_health") => native_health.clone(),
                Some("get_instrument") => native_instrument.clone(),
                _ => tool.clone(),
            },
        )
        .collect::<Vec<_>>();
    mcp_result(request_id, json!({"tools":merged}))
}

async fn mcp_tool_call(
    state: &AppState,
    request_id: serde_json::Value,
    params: &serde_json::Value,
    message: &serde_json::Value,
) -> serde_json::Value {
    let name = params.get("name").and_then(|value| value.as_str());
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if name.is_none() || !arguments.is_object() {
        return mcp_error(request_id, -32602, "Invalid tool call parameters");
    }
    if name == Some("get_instrument") {
        let arguments = arguments.as_object().expect("arguments were validated");
        let mut unexpected = arguments
            .keys()
            .filter(|key| key.as_str() != "instrument_id")
            .cloned()
            .collect::<Vec<_>>();
        unexpected.sort();
        if !unexpected.is_empty() {
            let error = json!({
                "error":"invalid_tool_input",
                "detail":format!("unexpected argument(s): {}", unexpected.join(", "))
            });
            return mcp_result(request_id, mcp_tool_result(error, true));
        }
        let Some(instrument_id) = arguments
            .get("instrument_id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            let detail = if arguments.contains_key("instrument_id") {
                "instrument_id must be a non-empty string"
            } else {
                "missing required argument(s): instrument_id"
            };
            let error = json!({"error":"invalid_tool_input","detail":detail});
            return mcp_result(request_id, mcp_tool_result(error, true));
        };
        return match instrument_lookup(state, instrument_id).await {
            Ok(record) => mcp_result(request_id, mcp_serialized_tool_result(&record)),
            Err((status, detail)) => {
                let error = json!({
                    "error":"marketcow_api_error",
                    "detail":{"status_code":status.as_u16(),"detail":detail}
                });
                mcp_result(request_id, mcp_tool_result(error, true))
            }
        };
    }
    if name != Some("service_health") {
        if !marketcow_contracts::MCP_LEGACY_TOOL_NAMES.contains(&name.expect("name was validated"))
        {
            return mcp_error(
                request_id,
                -32602,
                &format!("Unknown tool: {}", name.expect("name was validated")),
            );
        }
        let Some(proxy) = &state.legacy_mcp else {
            return mcp_error(request_id, -32602, "Tool is not migrated or configured");
        };
        return match proxy.request(message).await {
            Ok(Some(response)) if valid_legacy_mcp_response(&response, &request_id) => response,
            Ok(Some(_)) => mcp_error(request_id, -32603, "Legacy MCP response is invalid"),
            Ok(None) | Err(_) => mcp_error(request_id, -32603, "Legacy MCP proxy unavailable"),
        };
    }
    if !arguments
        .as_object()
        .expect("arguments were validated")
        .is_empty()
    {
        let error = json!({"error":"invalid_tool_input","detail":"unexpected argument(s)"});
        return mcp_result(request_id, mcp_tool_result(error, true));
    }
    mcp_result(request_id, mcp_tool_result(health_payload(state), false))
}

fn valid_legacy_mcp_response(response: &serde_json::Value, request_id: &serde_json::Value) -> bool {
    response.get("jsonrpc").and_then(serde_json::Value::as_str) == Some("2.0")
        && response.get("id") == Some(request_id)
        && (response.get("error").is_some()
            || response.get("result").is_some_and(|result| {
                result
                    .get("content")
                    .is_some_and(serde_json::Value::is_array)
                    && result.get("structuredContent").is_some()
                    && result
                        .get("isError")
                        .is_some_and(serde_json::Value::is_boolean)
            }))
}

fn mcp_tool_result(payload: serde_json::Value, is_error: bool) -> serde_json::Value {
    json!({
        "content":[{"type":"text","text":serde_json::to_string(&payload).expect("JSON value serializes")}],
        "structuredContent":payload,
        "isError":is_error
    })
}

fn mcp_serialized_tool_result<T: Serialize>(payload: &T) -> serde_json::Value {
    json!({
        "content":[{"type":"text","text":serde_json::to_string(payload).expect("tool payload serializes")}],
        "structuredContent":serde_json::to_value(payload).expect("tool payload serializes"),
        "isError":false
    })
}

fn mcp_result(request_id: serde_json::Value, result: serde_json::Value) -> serde_json::Value {
    json!({"jsonrpc":"2.0","id":request_id,"result":result})
}

fn mcp_error(request_id: serde_json::Value, code: i64, message: &str) -> serde_json::Value {
    json!({"jsonrpc":"2.0","id":request_id,"error":{"code":code,"message":message}})
}

async fn readiness(State(state): State<AppState>) -> Response {
    let projection = state.projection.load_full();
    let fresh = projection_fresh(&state.config, &projection);
    let ready = projection.ready && fresh;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "ready":ready, "mode":"shadow", "scope_id":state.config.scope_id,
            "writer_enabled":false, "real_order_submission_enabled":false,
            "fail_closed_reason":if fresh { projection.fail_closed_reason.clone() } else { Some("book_stale".into()) },
            "ownership_registry":"docs/architecture/migration/domain-ownership.yaml"
        })),
    )
        .into_response()
}

async fn scope(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(
        serde_json::to_value(marketcow_contracts::ScopeDiscovery::shadow(
            state.config.scope_id,
        ))
        .expect("scope contract is serializable"),
    )
}

async fn live_snapshot(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
) -> Response {
    let projection = state.projection.load_full();
    if !projection.ready || !projection_fresh(&state.config, &projection) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_projection_unready_or_stale",
            true,
            &request_id,
        );
    }
    Json(marketcow_api::snapshot(&projection)).into_response()
}

#[derive(Debug, Deserialize)]
struct EventQuery {
    #[serde(default)]
    after_cursor: u64,
    #[serde(default = "default_event_limit")]
    limit: usize,
}

fn default_event_limit() -> usize {
    1_000
}

async fn live_events(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    Query(query): Query<EventQuery>,
) -> Response {
    let projection = state.projection.load_full();
    if !projection.ready || !projection_fresh(&state.config, &projection) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_projection_unready_or_stale",
            true,
            &request_id,
        );
    }
    let records = state.recent_events.load_full();
    match marketcow_api::events_page(&records, query.after_cursor, query.limit) {
        Ok(response) => Json(response).into_response(),
        Err(marketcow_api::ReadApiError::InvalidLimit) => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_event_page_limit",
            false,
            &request_id,
        ),
        Err(marketcow_api::ReadApiError::CursorExpired { .. }) => {
            error(StatusCode::GONE, "event_cursor_expired", true, &request_id)
        }
        Err(marketcow_api::ReadApiError::Serialization) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "event_serialization_failed",
            true,
            &request_id,
        ),
    }
}

async fn live_checkpoint(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
) -> Response {
    let projection = state.projection.load_full();
    if !projection.ready || !projection_fresh(&state.config, &projection) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_projection_unready_or_stale",
            true,
            &request_id,
        );
    }
    Json(marketcow_api::checkpoint(&projection)).into_response()
}

async fn live_full_sync(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
) -> Response {
    let projection = state.projection.load_full();
    if !projection.ready || !projection_fresh(&state.config, &projection) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_projection_unready_or_stale",
            true,
            &request_id,
        );
    }
    Json(marketcow_api::full_sync(&projection)).into_response()
}

#[derive(Debug, Deserialize)]
struct StreamQuery {
    after_cursor: Option<u64>,
}

async fn market_data_stream(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    Query(query): Query<StreamQuery>,
) -> Response {
    let projection = state.projection.load_full();
    if !projection.ready || !projection_fresh(&state.config, &projection) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_projection_unready_or_stale",
            true,
            &request_id,
        );
    }
    if query
        .after_cursor
        .is_some_and(|cursor| cursor > projection.cursor)
    {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "stream_cursor_ahead_of_projection",
            false,
            &request_id,
        );
    }
    // Subscribe before capturing the immutable replay views. Events visible in both are skipped by
    // cursor, while an event landing between the reads remains in at least one source.
    let receiver = state.stream.subscribe();
    let records = state.recent_events.load_full();
    let projection = state.projection.load_full();
    if !projection.ready || !projection_fresh(&state.config, &projection) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_projection_unready_or_stale",
            true,
            &request_id,
        );
    }
    ws.on_upgrade(move |socket| {
        serve_market_data_stream(
            socket,
            state,
            receiver,
            projection,
            records,
            query.after_cursor,
        )
    })
}

struct StreamClientGuard(Arc<Metrics>);

impl Drop for StreamClientGuard {
    fn drop(&mut self) {
        self.0.stream_clients.fetch_sub(1, Ordering::Relaxed);
        self.0.stream_disconnects.fetch_add(1, Ordering::Relaxed);
    }
}

async fn serve_market_data_stream(
    mut socket: WebSocket,
    state: AppState,
    mut receiver: broadcast::Receiver<marketcow_core::PersistedEvent>,
    projection: Arc<marketcow_core::Projection>,
    records: Arc<Vec<marketcow_core::PersistedEvent>>,
    after_cursor: Option<u64>,
) {
    state.metrics.stream_clients.fetch_add(1, Ordering::Relaxed);
    let _guard = StreamClientGuard(state.metrics.clone());
    let boundary = projection.cursor;
    let mut last_cursor = after_cursor.unwrap_or(boundary);

    if let Some(resume_cursor) = after_cursor {
        let earliest = records.first().map(|record| record.event.cursor);
        if resume_cursor < boundary
            && earliest.is_some_and(|cursor| cursor > resume_cursor.saturating_add(1))
        {
            let frame = marketcow_api::stream_resync_required(
                &state.config.scope_id,
                resume_cursor,
                "event_cursor_expired",
            );
            let _ = send_stream_frame(&mut socket, &frame).await;
            close_stream(&mut socket, close_code::POLICY, "full_sync_required").await;
            return;
        }
        if !deliver_stream_frame(
            &mut socket,
            &state,
            &marketcow_api::stream_subscription(&projection, true),
        )
        .await
        {
            return;
        }
        for record in records
            .iter()
            .filter(|record| record.event.cursor > resume_cursor && record.event.cursor <= boundary)
        {
            if record.event.cursor != last_cursor.saturating_add(1) {
                send_resync_and_close(&mut socket, &state, last_cursor, "stream_replay_gap").await;
                return;
            }
            let Ok(frame) = marketcow_api::stream_event(&state.config.scope_id, record) else {
                close_stream(&mut socket, close_code::ERROR, "serialization_failed").await;
                return;
            };
            if !deliver_stream_frame(&mut socket, &state, &frame).await {
                return;
            }
            last_cursor = record.event.cursor;
        }
        if last_cursor != boundary {
            send_resync_and_close(&mut socket, &state, last_cursor, "stream_replay_incomplete")
                .await;
            return;
        }
    } else if !deliver_stream_frame(
        &mut socket,
        &state,
        &marketcow_api::stream_subscription(&projection, false),
    )
    .await
    {
        return;
    }

    let mut freshness_check = tokio::time::interval(std::time::Duration::from_secs(1));
    freshness_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        enum Next {
            Client(Option<Result<Message, axum::Error>>),
            Event(Box<Result<marketcow_core::PersistedEvent, broadcast::error::RecvError>>),
            FreshnessCheck,
        }
        let next = tokio::select! {
            message = socket.recv() => Next::Client(message),
            event = receiver.recv() => Next::Event(Box::new(event)),
            _ = freshness_check.tick() => Next::FreshnessCheck,
        };
        match next {
            Next::Client(Some(Ok(Message::Close(_))) | None | Some(Err(_))) => return,
            Next::Client(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
            Next::Client(Some(Ok(Message::Text(_) | Message::Binary(_)))) => {
                close_stream(&mut socket, close_code::UNSUPPORTED, "server_push_only").await;
                return;
            }
            Next::Event(event) => match *event {
                Ok(record) if record.event.cursor <= last_cursor => {}
                Ok(record) => {
                    if record.event.cursor != last_cursor.saturating_add(1) {
                        send_resync_and_close(&mut socket, &state, last_cursor, "stream_live_gap")
                            .await;
                        return;
                    }
                    let Ok(frame) = marketcow_api::stream_event(&state.config.scope_id, &record)
                    else {
                        close_stream(&mut socket, close_code::ERROR, "serialization_failed").await;
                        return;
                    };
                    if !deliver_stream_frame(&mut socket, &state, &frame).await {
                        return;
                    }
                    last_cursor = record.event.cursor;
                    let current = state.projection.load_full();
                    if !current.ready || !projection_fresh(&state.config, &current) {
                        send_resync_and_close(
                            &mut socket,
                            &state,
                            last_cursor,
                            "projection_unready_or_stale",
                        )
                        .await;
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    state
                        .metrics
                        .stream_slow_consumer_disconnects
                        .fetch_add(1, Ordering::Relaxed);
                    send_resync_and_close(&mut socket, &state, last_cursor, "slow_consumer_lagged")
                        .await;
                    return;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    close_stream(&mut socket, close_code::RESTART, "server_shutdown").await;
                    return;
                }
            },
            Next::FreshnessCheck => {
                let current = state.projection.load_full();
                if !current.ready || !projection_fresh(&state.config, &current) {
                    send_resync_and_close(
                        &mut socket,
                        &state,
                        last_cursor,
                        "projection_unready_or_stale",
                    )
                    .await;
                    return;
                }
            }
        }
    }
}

enum StreamSendError {
    Timeout,
    TransportOrSerialization,
}

async fn send_stream_frame(
    socket: &mut WebSocket,
    frame: &marketcow_api::StreamFrame,
) -> std::result::Result<(), StreamSendError> {
    let payload =
        serde_json::to_string(frame).map_err(|_| StreamSendError::TransportOrSerialization)?;
    match tokio::time::timeout(
        STREAM_SEND_TIMEOUT,
        socket.send(Message::Text(payload.into())),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(StreamSendError::TransportOrSerialization),
        Err(_) => Err(StreamSendError::Timeout),
    }
}

async fn deliver_stream_frame(
    socket: &mut WebSocket,
    state: &AppState,
    frame: &marketcow_api::StreamFrame,
) -> bool {
    match send_stream_frame(socket, frame).await {
        Ok(()) => true,
        Err(StreamSendError::TransportOrSerialization) => false,
        Err(StreamSendError::Timeout) => {
            state
                .metrics
                .stream_slow_consumer_disconnects
                .fetch_add(1, Ordering::Relaxed);
            close_stream(socket, close_code::AGAIN, "slow_consumer").await;
            false
        }
    }
}

async fn send_resync_and_close(
    socket: &mut WebSocket,
    state: &AppState,
    cursor: u64,
    reason: &str,
) {
    let frame = marketcow_api::stream_resync_required(&state.config.scope_id, cursor, reason);
    let _ = send_stream_frame(socket, &frame).await;
    close_stream(socket, close_code::AGAIN, "full_sync_required").await;
}

async fn close_stream(socket: &mut WebSocket, code: u16, reason: &'static str) {
    let close = socket.send(Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    })));
    let _ = tokio::time::timeout(STREAM_CLOSE_TIMEOUT, close).await;
}

#[cfg(test)]
fn bootstrap_projection(scope_id: String) -> marketcow_core::Projection {
    marketcow_core::Projection::bootstrap(scope_id)
}

async fn admin_migration(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({
        "schema":"marketcow.migration-control.v1",
        "phase":"shadow",
        "cutover_allowed":false,
        "checkpoint_persistence":if state.control_plane.persistence_enabled() {
            "healthy"
        } else {
            "degraded_development_only"
        },
        "ownership_registry":{
            "schema":"marketcow.domain-ownership.v1",
            "revision":"phase0-shadow",
            "sha256":hex::encode(Sha256::digest(DOMAIN_OWNERSHIP_REGISTRY))
        },
        "real_order_submission_enabled":false,
        "tradude_may_manage_marketcow":false
    }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MigrationCheckpointInput {
    expected_revision: u64,
    status: String,
    #[serde(default)]
    source_watermark: Option<String>,
    #[serde(default)]
    target_watermark: Option<String>,
    #[serde(default = "empty_json_object")]
    cursor_json: serde_json::Value,
    #[serde(default = "empty_json_object")]
    evidence_json: serde_json::Value,
    #[serde(default)]
    error: Option<String>,
}

fn empty_json_object() -> serde_json::Value {
    json!({})
}

fn valid_migration_checkpoint_domain(domain: &str) -> bool {
    MIGRATION_CHECKPOINT_DOMAINS.contains(&domain)
}

async fn admin_get_migration_checkpoint(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    AxumPath((run_id, domain, shard)): AxumPath<(String, String, String)>,
) -> Response {
    if !valid_migration_checkpoint_domain(&domain) {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_migration_checkpoint_domain",
            false,
            &request_id,
        );
    }
    match state
        .control_plane
        .get_checkpoint(&run_id, &domain, &shard)
        .await
    {
        Ok(Some(record)) => Json(json!({
            "schema":"marketcow.migration-checkpoint.v1",
            "checkpoint":record,
            "cutover_allowed":false,
            "real_order_submission_enabled":false
        }))
        .into_response(),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            "migration_checkpoint_not_found",
            false,
            &request_id,
        ),
        Err(repository_error) => {
            warn!(error=%repository_error, request_id, "migration_checkpoint_read_failed_closed");
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "control_plane_unavailable",
                true,
                &request_id,
            )
        }
    }
}

async fn admin_put_migration_checkpoint(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    AxumPath((run_id, domain, shard)): AxumPath<(String, String, String)>,
    Json(input): Json<MigrationCheckpointInput>,
) -> Response {
    if !valid_migration_checkpoint_domain(&domain) {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_migration_checkpoint_domain",
            false,
            &request_id,
        );
    }
    let record = marketcow_storage::MigrationCheckpointRecord {
        run_id,
        domain,
        shard,
        revision: input.expected_revision,
        status: input.status,
        source_watermark: input.source_watermark,
        target_watermark: input.target_watermark,
        cursor_json: input.cursor_json,
        evidence_json: input.evidence_json,
        error: input.error,
        updated_at: Utc::now(),
    };
    if record.validate().is_err() {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_migration_checkpoint",
            false,
            &request_id,
        );
    }
    match state
        .control_plane
        .upsert_checkpoint(&record, input.expected_revision)
        .await
    {
        Ok(stored) => Json(json!({
            "schema":"marketcow.migration-checkpoint.v1",
            "checkpoint":stored,
            "cutover_allowed":false,
            "real_order_submission_enabled":false
        }))
        .into_response(),
        Err(marketcow_storage::RepositoryError::RevisionConflict) => error(
            StatusCode::CONFLICT,
            "migration_checkpoint_revision_conflict",
            false,
            &request_id,
        ),
        Err(marketcow_storage::RepositoryError::InvalidInput) => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_migration_checkpoint",
            false,
            &request_id,
        ),
        Err(repository_error) => {
            warn!(error=%repository_error, request_id, "migration_checkpoint_write_failed_closed");
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "control_plane_unavailable",
                true,
                &request_id,
            )
        }
    }
}

#[derive(Debug, Deserialize)]
struct AdminAuditQuery {
    #[serde(default = "default_audit_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
    #[serde(default)]
    action: String,
    #[serde(default)]
    outcome: String,
}

fn default_audit_limit() -> i64 {
    50
}

async fn admin_audit_events(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    Query(query): Query<AdminAuditQuery>,
) -> Response {
    let action = (!query.action.is_empty()).then_some(query.action.as_str());
    let outcome = (!query.outcome.is_empty()).then_some(query.outcome.as_str());
    match state
        .audit
        .list_admin(query.limit, query.offset, action, outcome)
        .await
    {
        Ok((items, durable)) => Json(json!({
            "schema":"marketcow.admin-audit.v1",
            "page":{
                "limit":query.limit,
                "offset":query.offset,
                "returned":items.len()
            },
            "items":items,
            "durable":durable
        }))
        .into_response(),
        Err(marketcow_storage::RepositoryError::InvalidInput) => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_audit_query",
            false,
            &request_id,
        ),
        Err(repository_error) => {
            warn!(error=%repository_error, request_id, "admin_audit_query_failed_closed");
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "audit_unavailable",
                true,
                &request_id,
            )
        }
    }
}

#[derive(Debug, Deserialize)]
struct SubmitProviderJobRequest {
    job_type: String,
    request_schema: String,
    request: serde_json::Value,
    deadline: chrono::DateTime<Utc>,
    #[serde(default = "default_max_job_attempts")]
    max_attempts: u32,
}

fn default_max_job_attempts() -> u32 {
    3
}

async fn admin_submit_job(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    headers: HeaderMap,
    Json(request): Json<SubmitProviderJobRequest>,
) -> Response {
    let Some(idempotency_key) = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 256)
    else {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "idempotency_key_required",
            false,
            &request_id,
        );
    };
    match state
        .jobs
        .submit(
            marketcow_jobs::SubmitJob {
                idempotency_key: idempotency_key.into(),
                job_type: request.job_type,
                request_schema: request.request_schema,
                request: request.request,
                deadline: request.deadline,
                max_attempts: request.max_attempts,
                audit_actor: format!("admin_request:{request_id}"),
            },
            Utc::now(),
        )
        .await
    {
        Ok(job) => Json(admin_job_view(&job)).into_response(),
        Err(DurableJobError::Engine(marketcow_jobs::JobEngineError::IdempotencyConflict)) => error(
            StatusCode::CONFLICT,
            "idempotency_conflict",
            false,
            &request_id,
        ),
        Err(DurableJobError::Persistence(_)) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "job_persistence_unavailable",
            true,
            &request_id,
        ),
        Err(DurableJobError::Engine(_)) => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_job_request",
            false,
            &request_id,
        ),
    }
}

async fn admin_get_job(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    AxumPath(job_id): AxumPath<String>,
) -> Response {
    match state.jobs.get(&job_id).await {
        Some(job) => Json(admin_job_view(&job)).into_response(),
        None => error(StatusCode::NOT_FOUND, "job_not_found", false, &request_id),
    }
}

async fn admin_cancel_job(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    AxumPath(job_id): AxumPath<String>,
) -> Response {
    match state
        .jobs
        .cancel(&job_id, &format!("admin_request:{request_id}"), Utc::now())
        .await
    {
        Ok(job) => Json(admin_job_view(&job)).into_response(),
        Err(DurableJobError::Engine(marketcow_jobs::JobEngineError::NotFound)) => {
            error(StatusCode::NOT_FOUND, "job_not_found", false, &request_id)
        }
        Err(DurableJobError::Engine(marketcow_jobs::JobEngineError::InvalidTransition)) => error(
            StatusCode::CONFLICT,
            "job_terminal_or_transition_conflict",
            false,
            &request_id,
        ),
        Err(DurableJobError::Persistence(_)) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "job_persistence_unavailable",
            true,
            &request_id,
        ),
        Err(DurableJobError::Engine(_)) => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_job_cancel",
            false,
            &request_id,
        ),
    }
}

fn admin_job_view(job: &marketcow_jobs::ProviderJob) -> serde_json::Value {
    json!({
        "schema_version":job.schema_version,
        "job_id":job.job_id,
        "idempotency_key":job.idempotency_key,
        "job_type":job.job_type,
        "request_schema":job.request_schema,
        "request_sha256":job.request_sha256,
        "status":job.status,
        "revision":job.revision,
        "owner_id":job.owner_id,
        "lease_active":job.lease_token.is_some(),
        "lease_expires_at":job.lease_expires_at,
        "deadline":job.deadline,
        "attempt":job.attempt,
        "max_attempts":job.max_attempts,
        "created_at":job.created_at,
        "started_at":job.started_at,
        "finished_at":job.finished_at,
        "error":job.error,
        "result":job.result,
        "audit_actor":job.audit_actor,
        "real_order_submission_enabled":false
    })
}

async fn admin_checkpoint(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
) -> Response {
    let mut runtime = state.runtime.lock().await;
    match runtime.checkpoint() {
        Ok(manifest) => {
            state.projection.store(runtime.projection());
            state
                .recent_events
                .store(Arc::new(runtime.recent_events().to_vec()));
            Json(json!({
                "status":"checkpoint_written",
                "cursor":manifest.current.cursor,
                "projection_sha256":manifest.current.projection_sha256,
                "real_order_submission_enabled":false
            }))
            .into_response()
        }
        Err(error_value) => {
            warn!(error=%error_value, "checkpoint_failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "checkpoint_failed",
                true,
                &request_id,
            )
        }
    }
}

#[derive(Debug, Deserialize)]
struct ShadowIngestRequest {
    raw_payload: serde_json::Value,
    received_at: Option<chrono::DateTime<Utc>>,
}

async fn admin_shadow_ingest(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    Json(request): Json<ShadowIngestRequest>,
) -> Response {
    if !state.config.shadow_mode || state.config.real_order_submission_enabled {
        return error(
            StatusCode::FORBIDDEN,
            "shadow_ingest_disabled",
            false,
            &request_id,
        );
    }
    let received_at = request.received_at.unwrap_or_else(Utc::now);
    let mut runtime = state.runtime.lock().await;
    match runtime.apply_raw(request.raw_payload, received_at) {
        Ok(outcomes) => {
            let rejected = outcomes
                .iter()
                .filter(|outcome| !outcome.persisted.applied)
                .count();
            if let Some(maximum) = outcomes
                .iter()
                .map(|value| value.persistence_latency_us)
                .max()
            {
                state
                    .metrics
                    .persistence_latency_us
                    .fetch_max(maximum, Ordering::Relaxed);
            }
            if let Some(maximum) = outcomes
                .iter()
                .map(|value| value.publication_latency_us)
                .max()
            {
                state
                    .metrics
                    .publication_latency_us
                    .fetch_max(maximum, Ordering::Relaxed);
            }
            state.projection.store(runtime.projection());
            state
                .recent_events
                .store(Arc::new(runtime.recent_events().to_vec()));
            let projection = runtime.projection();
            // Publication is deliberately last: every delivered frame is already WAL-persisted
            // and visible through the immutable projection/replay views.
            for outcome in &outcomes {
                let _ = state.stream.send(outcome.persisted.clone());
            }
            Json(json!({
                "status":"shadow_ingested",
                "events":outcomes.len(),
                "rejected":rejected,
                "published_cursor":projection.cursor,
                "persisted_cursor":projection.persisted_cursor,
                "ready":projection.ready,
                "real_order_submission_enabled":false
            }))
            .into_response()
        }
        Err(error_value) => {
            warn!(error=%error_value, "shadow_ingest_rejected");
            error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "shadow_ingest_rejected",
                false,
                &request_id,
            )
        }
    }
}

fn projection_maximum_book_age_ms(projection: &marketcow_core::Projection) -> u64 {
    projection
        .books
        .values()
        .filter_map(|book| book.source_observed_at)
        .map(|observed_at| {
            Utc::now()
                .signed_duration_since(observed_at)
                .num_milliseconds()
                .max(0) as u64
        })
        .max()
        .unwrap_or(u64::MAX)
}

fn projection_fresh(config: &Config, projection: &marketcow_core::Projection) -> bool {
    projection_maximum_book_age_ms(projection) <= config.maximum_book_age_ms
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let projection = state.projection.load_full();
    let maximum_book_age_ms = projection_maximum_book_age_ms(&projection);
    (
        [("content-type", "text/plain; version=0.0.4")],
        format!(
            "# TYPE marketcow_http_requests_total counter\nmarketcow_http_requests_total {}\n\
             # TYPE marketcow_http_errors_total counter\nmarketcow_http_errors_total {}\n\
             marketcow_real_order_submission_enabled 0\nmarketcow_shadow_mode 1\n\
             marketcow_projection_published_cursor {}\nmarketcow_projection_persisted_cursor {}\n\
             marketcow_unresolved_gaps {}\nmarketcow_book_count {}\nmarketcow_maximum_book_age_ms {}\n\
             marketcow_disconnects_total {}\nmarketcow_ingress_queue_depth 0\n\
             marketcow_stream_clients {}\nmarketcow_stream_disconnects_total {}\n\
             marketcow_stream_channel_depth {}\n\
             marketcow_stream_channel_capacity {}\nmarketcow_stream_slow_consumer_disconnects_total {}\n\
             marketcow_persistence_latency_us {}\nmarketcow_publication_latency_us {}\n\
             marketcow_python_workers_configured {}\nmarketcow_python_workers_live {}\n\
             marketcow_python_worker_restarts_total {}\nmarketcow_python_worker_restart_budget_exhaustions_total {}\n\
             marketcow_python_worker_memory_limit_kills_total {}\nmarketcow_python_worker_memory_monitor_failures_total {}\n\
             marketcow_python_worker_memory_limit_mib {}\nmarketcow_python_worker_cpu_limit_seconds {}\n",
            state.metrics.requests.load(Ordering::Relaxed),
            state.metrics.errors.load(Ordering::Relaxed),
            projection.cursor,
            projection.persisted_cursor,
            projection.unresolved_gaps.len(),
            projection.books.len(),
            maximum_book_age_ms,
            state.metrics.disconnects.load(Ordering::Relaxed),
            state.metrics.stream_clients.load(Ordering::Relaxed),
            state.metrics.stream_disconnects.load(Ordering::Relaxed),
            state.stream.len(),
            STREAM_CHANNEL_CAPACITY,
            state
                .metrics
                .stream_slow_consumer_disconnects
                .load(Ordering::Relaxed),
            state.metrics.persistence_latency_us.load(Ordering::Relaxed),
            state.metrics.publication_latency_us.load(Ordering::Relaxed),
            state.config.python_workers.pool_size,
            state.worker_status.live.load(Ordering::Relaxed),
            state.worker_status.restarts.load(Ordering::Relaxed),
            state
                .worker_status
                .budget_exhaustions
                .load(Ordering::Relaxed),
            state
                .worker_status
                .memory_limit_kills
                .load(Ordering::Relaxed),
            state
                .worker_status
                .memory_monitor_failures
                .load(Ordering::Relaxed),
            state.config.python_workers.memory_limit_mib,
            state.config.python_workers.cpu_limit_seconds,
        ),
    )
}

async fn request_boundary(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|x| x.to_str().ok())
        .filter(|x| {
            !x.is_empty()
                && x.len() <= 128
                && x.bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        })
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let path = request.uri().path().to_owned();
    let method = request.method().as_str().to_owned();
    let is_admin = path.starts_with("/v1/admin/");
    if is_admin {
        let expected = env::var("MARKETCOW_RUST_ADMIN_TOKEN").unwrap_or_default();
        let supplied = request
            .headers()
            .get("authorization")
            .and_then(|x| x.to_str().ok())
            .and_then(|x| x.strip_prefix("Bearer "))
            .unwrap_or("");
        if expected.is_empty() || !constant_time_equal(expected.as_bytes(), supplied.as_bytes()) {
            let rejected = admin_audit_record(
                &request_id,
                "anonymous",
                &path,
                "rejected",
                json!({"method":method,"stage":"authentication"}),
                "authentication_required",
            );
            if let Err(audit_error) = state.audit.record_admin(&rejected).await {
                warn!(error=%audit_error, request_id, "admin_audit_failed_closed");
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "audit_unavailable",
                    true,
                    &request_id,
                );
            }
            return error(
                StatusCode::UNAUTHORIZED,
                "authentication_required",
                false,
                &request_id,
            );
        }
        let accepted = admin_audit_record(
            &request_id,
            "admin:bearer",
            &path,
            "accepted",
            json!({"method":method,"stage":"request_boundary"}),
            "",
        );
        if let Err(audit_error) = state.audit.record_admin(&accepted).await {
            warn!(error=%audit_error, request_id, "admin_audit_failed_closed");
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "audit_unavailable",
                true,
                &request_id,
            );
        }
    }
    request.extensions_mut().insert(request_id.clone());
    let started = std::time::Instant::now();
    let mut response = next.run(request).await;
    let handler_status = response.status();
    if is_admin {
        let outcome = if handler_status.is_server_error() {
            "failed"
        } else if handler_status.is_client_error() {
            "rejected"
        } else {
            "succeeded"
        };
        let completed = admin_audit_record(
            &request_id,
            "admin:bearer",
            &path,
            outcome,
            json!({
                "method":method,
                "stage":"handler_result",
                "status":handler_status.as_u16(),
                "elapsed_us":started.elapsed().as_micros()
            }),
            "",
        );
        if let Err(audit_error) = state.audit.record_admin(&completed).await {
            warn!(error=%audit_error, request_id, "admin_audit_outcome_failed_closed");
            response = error(
                StatusCode::SERVICE_UNAVAILABLE,
                "audit_unavailable",
                true,
                &request_id,
            );
        }
    } else if let Err(audit_error) = state.audit.record_request(&json!({
        "at":Utc::now(),"request_id":request_id,"path":path,
        "status":handler_status.as_u16(),"elapsed_us":started.elapsed().as_micros()
    })) {
        warn!(error=%audit_error, request_id, "request_audit_failed_closed");
        response = error(
            StatusCode::SERVICE_UNAVAILABLE,
            "audit_unavailable",
            true,
            &request_id,
        );
    }
    state.metrics.requests.fetch_add(1, Ordering::Relaxed);
    if response.status().is_client_error() || response.status().is_server_error() {
        state.metrics.errors.fetch_add(1, Ordering::Relaxed);
    }
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

fn admin_audit_record(
    request_id: &str,
    actor: &str,
    target: &str,
    outcome: &str,
    parameters_json: serde_json::Value,
    detail: &str,
) -> marketcow_storage::AdminAuditRecord {
    let now = Utc::now();
    let occurred_at = now
        .with_nanosecond(now.nanosecond() / 1_000 * 1_000)
        .expect("microsecond precision is a valid timestamp");
    marketcow_storage::AdminAuditRecord {
        audit_id: format!("audit-{}", Uuid::new_v4()),
        schema_version: "marketcow.admin-audit.v1".into(),
        occurred_at,
        actor: actor.into(),
        action: "http.admin.request".into(),
        target: target.into(),
        outcome: outcome.into(),
        request_id: request_id.into(),
        parameters_json,
        detail: detail.into(),
    }
}

fn constant_time_equal(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    let length = a.len().max(b.len());
    for i in 0..length {
        diff |= usize::from(*a.get(i).unwrap_or(&0) ^ *b.get(i).unwrap_or(&0));
    }
    diff == 0
}

fn error(status: StatusCode, code: &str, retryable: bool, request_id: &str) -> Response {
    let envelope = marketcow_contracts::MachineErrorEnvelope {
        detail: marketcow_contracts::MachineErrorDetail {
            code: code.into(),
            message: code.replace('_', " "),
            retryable,
            request_id: request_id.into(),
        },
    };
    (
        status,
        Json(serde_json::to_value(envelope).expect("error contract is serializable")),
    )
        .into_response()
}

async fn shutdown() {
    let ctrl_c = async { signal::ctrl_c().await.expect("ctrl-c handler") };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .unwrap()
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
    warn!("shutdown_draining");
}

fn validate_worker_result_artifact(
    job_type: &str,
    request_schema: &str,
    path: &Path,
    size_bytes: u64,
    media_type: &str,
) -> Result<()> {
    if media_type != "application/json" || size_bytes == 0 || size_bytes > MAX_WORKER_RESULT_BYTES {
        bail!("worker result media type or size is invalid");
    }
    let bytes = fs::read(path)?;
    if bytes.len() as u64 != size_bytes {
        bail!("worker result size changed before contract validation");
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    match (job_type, request_schema) {
        (SEC_DIVIDEND_TASK, SEC_DIVIDEND_REQUEST_SCHEMA) => validate_sec_dividend_result(&value),
        (CSV_INFERENCE_TASK, CSV_INFERENCE_REQUEST_SCHEMA) => validate_csv_inference_result(&value),
        (LONGPORT_RESOLVE_TASK, LONGPORT_RESOLVE_REQUEST_SCHEMA) => {
            validate_longport_resolve_result(&value)
        }
        _ => bail!("worker result contract is not registered in Rust"),
    }
}

fn exact_object_keys(object: &serde_json::Map<String, serde_json::Value>, keys: &[&str]) -> bool {
    object.len() == keys.len() && keys.iter().all(|key| object.contains_key(*key))
}

fn nonempty_bounded_string(value: &serde_json::Value, maximum_bytes: usize) -> Option<&str> {
    value
        .as_str()
        .filter(|text| !text.is_empty() && text.len() <= maximum_bytes)
}

fn valid_iso_date(value: &serde_json::Value, optional: bool) -> bool {
    if optional && value.is_null() {
        return true;
    }
    value
        .as_str()
        .is_some_and(|text| chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").is_ok())
}

fn validate_sec_dividend_result(value: &serde_json::Value) -> Result<()> {
    let object = value
        .as_object()
        .context("SEC worker result must be an object")?;
    if !exact_object_keys(object, &["schema_version", "rows"])
        || object["schema_version"] != SEC_DIVIDEND_RESULT_SCHEMA
    {
        bail!("SEC worker result schema is invalid");
    }
    let rows = object["rows"]
        .as_array()
        .context("SEC worker rows must be an array")?;
    if rows.len() > 10_000 {
        bail!("SEC worker row count exceeds limit");
    }
    let keys = [
        "symbol",
        "fiscal_year",
        "amount_per_share",
        "currency",
        "announcement_date",
        "record_date",
        "ex_date",
        "payment_date",
        "expected_payment_date",
        "confirmation_status",
        "source_type",
        "source_name",
        "source_url",
        "source_document_id",
    ];
    for row in rows {
        let row = row
            .as_object()
            .context("SEC worker row must be an object")?;
        if !exact_object_keys(row, &keys) {
            bail!("SEC worker row shape is invalid");
        }
        let amount_text = nonempty_bounded_string(&row["amount_per_share"], 128)
            .context("SEC amount must be an exact decimal string")?;
        if !amount_text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.')
        {
            bail!("SEC amount uses unsupported numeric notation");
        }
        let amount = amount_text
            .parse::<rust_decimal::Decimal>()
            .context("SEC amount is not a decimal")?;
        if amount <= rust_decimal::Decimal::ZERO || amount.scale() > 18 {
            bail!("SEC amount precision or sign is invalid");
        }
        if nonempty_bounded_string(&row["symbol"], 128).is_none()
            || row["fiscal_year"]
                .as_u64()
                .is_none_or(|year| !(1900..=3000).contains(&year))
            || row["currency"] != "USD"
            || !valid_iso_date(&row["announcement_date"], false)
            || !valid_iso_date(&row["record_date"], true)
            || !valid_iso_date(&row["ex_date"], true)
            || !valid_iso_date(&row["payment_date"], false)
            || !valid_iso_date(&row["expected_payment_date"], false)
            || row["confirmation_status"] != "confirmed"
            || row["source_type"] != "regulatory_filing"
            || row["source_name"] != "SEC EDGAR"
            || nonempty_bounded_string(&row["source_url"], 4096).is_none()
            || nonempty_bounded_string(&row["source_document_id"], 512).is_none()
        {
            bail!("SEC worker financial or provenance field is invalid");
        }
    }
    Ok(())
}

fn validate_csv_inference_result(value: &serde_json::Value) -> Result<()> {
    let object = value
        .as_object()
        .context("CSV worker result must be an object")?;
    let keys = [
        "schema_version",
        "source",
        "observed_at",
        "content_sha256",
        "delimiter",
        "columns",
        "rows",
        "row_count",
    ];
    if !exact_object_keys(object, &keys)
        || object["schema_version"] != CSV_INFERENCE_RESULT_SCHEMA
        || nonempty_bounded_string(&object["source"], 2048).is_none()
        || object["observed_at"]
            .as_str()
            .is_none_or(|text| chrono::DateTime::parse_from_rfc3339(text).is_err())
        || object["content_sha256"]
            .as_str()
            .is_none_or(|hash| hash.len() != 64 || hex::decode(hash).is_err())
        || object["delimiter"]
            .as_str()
            .is_none_or(|delimiter| !matches!(delimiter, "," | "\t" | ";" | "|"))
    {
        bail!("CSV worker schema or provenance is invalid");
    }
    let columns = object["columns"]
        .as_array()
        .context("CSV columns must be an array")?;
    if columns.is_empty() || columns.len() > 256 {
        bail!("CSV column count is invalid");
    }
    let mut unique = BTreeSet::new();
    for column in columns {
        let column = nonempty_bounded_string(column, 512).context("CSV header is invalid")?;
        if !unique.insert(column) {
            bail!("CSV headers must be unique");
        }
    }
    let rows = object["rows"]
        .as_array()
        .context("CSV rows must be an array")?;
    if rows.len() > 10_000 || object["row_count"].as_u64() != Some(rows.len() as u64) {
        bail!("CSV row count is invalid");
    }
    for row in rows {
        let row = row.as_array().context("CSV row must be an array")?;
        if row.len() != columns.len()
            || row
                .iter()
                .any(|field| field.as_str().is_none_or(|text| text.len() > 1_048_576))
        {
            bail!("CSV row width or field type is invalid");
        }
    }
    Ok(())
}

fn validate_longport_resolve_result(value: &serde_json::Value) -> Result<()> {
    let object = value
        .as_object()
        .context("LongPort worker result must be an object")?;
    if !exact_object_keys(
        object,
        &["schema_version", "namespace", "observed_at", "items"],
    ) || object["schema_version"] != LONGPORT_RESOLVE_RESULT_SCHEMA
        || object["namespace"] != "provider:longport"
        || object["observed_at"]
            .as_str()
            .is_none_or(|text| chrono::DateTime::parse_from_rfc3339(text).is_err())
    {
        bail!("LongPort worker result envelope is invalid");
    }
    let items = object["items"]
        .as_array()
        .context("LongPort worker items must be an array")?;
    if items.is_empty() || items.len() > 20 {
        bail!("LongPort worker result count is invalid");
    }
    for item in items {
        let item = item
            .as_object()
            .context("LongPort worker item must be an object")?;
        let external_symbol = item
            .get("external_symbol")
            .and_then(|value| nonempty_bounded_string(value, 128))
            .context("LongPort external symbol is invalid")?;
        if external_symbol != external_symbol.trim().to_ascii_uppercase().replace(' ', "") {
            bail!("LongPort external symbol is not canonical");
        }
        match item.get("status").and_then(serde_json::Value::as_str) {
            Some("error") => {
                if !exact_object_keys(item, &["external_symbol", "status", "error"]) {
                    bail!("LongPort error item contains unexpected fields");
                }
                let error = item["error"]
                    .as_object()
                    .context("LongPort item error must be an object")?;
                if !exact_object_keys(error, &["code", "message"])
                    || error["code"].as_str().is_none_or(|code| {
                        !matches!(code, "not_found" | "ambiguous" | "provider_unavailable")
                    })
                    || nonempty_bounded_string(&error["message"], 1000).is_none()
                {
                    bail!("LongPort item error is invalid");
                }
            }
            Some("resolved") => {
                let keys = [
                    "external_symbol",
                    "status",
                    "instrument_id",
                    "symbol",
                    "mic",
                    "market",
                    "currency",
                    "lot_size",
                    "source",
                    "source_exchange",
                ];
                if !exact_object_keys(item, &keys)
                    || nonempty_bounded_string(&item["instrument_id"], 128).is_none()
                    || nonempty_bounded_string(&item["symbol"], 64).is_none()
                    || nonempty_bounded_string(&item["mic"], 4).is_none()
                    || item["mic"].as_str().is_none_or(|mic| {
                        !mic.bytes()
                            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
                    })
                    || item["market"]
                        .as_str()
                        .is_none_or(|market| !matches!(market, "US" | "CN" | "HK"))
                    || nonempty_bounded_string(&item["currency"], 8).is_none()
                    || item["source"] != "longport.static_info"
                    || nonempty_bounded_string(&item["source_exchange"], 64).is_none()
                {
                    bail!("LongPort resolved item identity or provenance is invalid");
                }
                let lot_size = item["lot_size"]
                    .as_str()
                    .context("LongPort lot size must be an exact decimal string")?
                    .parse::<rust_decimal::Decimal>()
                    .context("LongPort lot size is invalid")?;
                if lot_size <= rust_decimal::Decimal::ZERO || lot_size.scale() != 0 {
                    bail!("LongPort lot size must be a positive integer decimal");
                }
            }
            _ => bail!("LongPort worker item status is invalid"),
        }
    }
    Ok(())
}

async fn worker_server(
    path: PathBuf,
    staging_root: PathBuf,
    artifact_root: PathBuf,
    jobs: Arc<DurableJobCoordinator>,
) -> Result<()> {
    let _ = fs::remove_file(&path);
    fs::create_dir_all(&staging_root)?;
    fs::set_permissions(&staging_root, fs::Permissions::from_mode(0o700))?;
    let listener = UnixListener::bind(&path)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    loop {
        let (stream, _) = listener.accept().await?;
        let connection_jobs = jobs.clone();
        let connection_staging_root = staging_root.clone();
        let connection_artifact_root = artifact_root.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_worker(
                stream,
                connection_staging_root,
                connection_artifact_root,
                connection_jobs,
            )
            .await
            {
                warn!(%error, "worker_connection_rejected");
            }
        });
    }
}

async fn handle_worker(
    mut stream: UnixStream,
    staging_root: PathBuf,
    artifact_root: PathBuf,
    jobs: Arc<DurableJobCoordinator>,
) -> Result<()> {
    use marketcow_contracts::{WORKER_PROTOCOL_VERSION, WorkerFrame, WorkerMessage};

    let hello = tokio::time::timeout(std::time::Duration::from_secs(10), read_frame(&mut stream))
        .await
        .context("worker handshake timeout")??;
    let (worker_id, worker_revision, nonce, capabilities) = match hello.message {
        WorkerMessage::Hello {
            worker_id,
            worker_revision,
            nonce,
            capabilities,
        } if hello.protocol_version == WORKER_PROTOCOL_VERSION
            && !worker_id.is_empty()
            && worker_id.len() <= 128
            && !worker_revision.is_empty()
            && worker_revision.len() <= 256
            && !nonce.is_empty()
            && nonce.len() <= 128
            && capabilities.len() <= 64
            && capabilities
                .iter()
                .all(|capability| !capability.is_empty() && capability.len() <= 128) =>
        {
            (worker_id, worker_revision, nonce, capabilities)
        }
        _ => bail!("invalid worker handshake"),
    };
    info!(worker_id, worker_revision, "worker_handshake_accepted");
    write_frame(
        &mut stream,
        &WorkerFrame::new(
            hello.message_id,
            WorkerMessage::HelloAck {
                daemon_revision: env!("CARGO_PKG_VERSION").into(),
                nonce,
                maximum_frame_bytes: marketcow_contracts::MAX_WORKER_FRAME_BYTES,
            },
        ),
    )
    .await?;

    loop {
        let input =
            match tokio::time::timeout(std::time::Duration::from_secs(30), read_frame(&mut stream))
                .await
            {
                Ok(Ok(input)) => input,
                Ok(Err(error)) => return Err(error),
                Err(_) => bail!("worker idle timeout"),
            };
        if input.protocol_version != WORKER_PROTOCOL_VERSION || input.message_id.is_empty() {
            bail!("worker protocol mismatch");
        }
        let response = match input.message {
            WorkerMessage::Poll => {
                let now = Utc::now();
                let claimed = jobs
                    .reconcile_and_claim(
                        &worker_id,
                        &capabilities,
                        chrono::Duration::seconds(60),
                        now,
                    )
                    .await?;
                match claimed {
                    Some(job) => {
                        let task_staging = staging_root.join(&job.job_id);
                        fs::create_dir_all(&task_staging)?;
                        fs::set_permissions(&task_staging, fs::Permissions::from_mode(0o700))?;
                        WorkerMessage::Task {
                            job_id: job.job_id,
                            lease_token: job.lease_token.context("claimed job lacks lease")?,
                            deadline: job.deadline,
                            job_type: job.job_type,
                            request_schema: job.request_schema,
                            request_sha256: job.request_sha256,
                            request: job.request,
                            staging_path: task_staging.to_string_lossy().into_owned(),
                        }
                    }
                    None => WorkerMessage::NoWork,
                }
            }
            WorkerMessage::Start {
                job_id,
                lease_token,
            } => {
                let job = jobs.start(&job_id, &lease_token, Utc::now()).await?;
                worker_job_state(&job)
            }
            WorkerMessage::Complete {
                job_id,
                lease_token,
                relative_path,
                sha256,
                size_bytes,
                media_type,
            } => {
                let result = marketcow_jobs::StagedResult {
                    relative_path,
                    sha256,
                    size_bytes,
                    media_type,
                };
                let now = Utc::now();
                let authorized = jobs.authorize_result(&job_id, &lease_token, now).await?;
                let promotion_staging_root = staging_root.clone();
                let promotion_artifact_root = artifact_root.clone();
                let promotion_job_id = job_id.clone();
                let promotion_dataset = authorized.job_type.clone();
                let promotion_revision = authorized.request_schema.clone();
                let promotion_source = format!("python-worker:{worker_id}");
                let promotion_result = result.clone();
                let artifact = tokio::task::spawn_blocking(move || -> Result<_> {
                    let artifact = marketcow_storage::promote_worker_artifact(
                        marketcow_storage::WorkerArtifactPromotion {
                            staging_root: &promotion_staging_root,
                            artifact_root: &promotion_artifact_root,
                            job_id: &promotion_job_id,
                            dataset: &promotion_dataset,
                            revision: &promotion_revision,
                            source: &promotion_source,
                            result: &promotion_result,
                            ingested_at: now,
                        },
                    )?;
                    validate_worker_result_artifact(
                        &promotion_dataset,
                        &promotion_revision,
                        Path::new(&artifact.storage_path),
                        artifact.byte_size,
                        &artifact.media_type,
                    )?;
                    Ok(artifact)
                })
                .await
                .context("artifact promotion task failed")??;
                let promoted_result = marketcow_jobs::StagedResult {
                    relative_path: artifact.relative_path.clone(),
                    sha256: artifact.sha256.clone(),
                    size_bytes: artifact.byte_size,
                    media_type: artifact.media_type.clone(),
                };
                let job = jobs
                    .succeed_with_artifact(
                        &job_id,
                        &lease_token,
                        promoted_result,
                        artifact,
                        Utc::now(),
                    )
                    .await?;
                worker_job_state(&job)
            }
            WorkerMessage::Fail {
                job_id,
                lease_token,
                code,
                classification,
                redacted_message,
                retryable,
            } => {
                let job = jobs
                    .fail(
                        &job_id,
                        &lease_token,
                        marketcow_jobs::JobError {
                            code,
                            classification,
                            redacted_message,
                        },
                        retryable,
                        Utc::now(),
                    )
                    .await?;
                worker_job_state(&job)
            }
            _ => WorkerMessage::Error {
                code: "worker_message_not_allowed".into(),
                retryable: false,
            },
        };
        write_frame(&mut stream, &WorkerFrame::new(input.message_id, response)).await?;
    }
}

fn worker_job_state(job: &marketcow_jobs::ProviderJob) -> marketcow_contracts::WorkerMessage {
    marketcow_contracts::WorkerMessage::JobState {
        job_id: job.job_id.clone(),
        status: serde_json::to_value(job.status)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_else(|| "unknown".into()),
        revision: job.revision,
    }
}

async fn read_frame(stream: &mut UnixStream) -> Result<marketcow_contracts::WorkerFrame> {
    let length = stream.read_u32().await? as usize;
    if length == 0 || length > marketcow_contracts::MAX_WORKER_FRAME_BYTES {
        bail!("invalid worker frame length");
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
async fn write_frame(
    stream: &mut UnixStream,
    value: &marketcow_contracts::WorkerFrame,
) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.is_empty() || bytes.len() > marketcow_contracts::MAX_WORKER_FRAME_BYTES {
        bail!("invalid worker response frame length");
    }
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use futures_util::StreamExt;
    use tempfile::tempdir;
    use tower::ServiceExt;

    fn test_state() -> (tempfile::TempDir, AppState) {
        let dir = tempdir().unwrap();
        let audit = Arc::new(AuditCoordinator::memory(&dir.path().join("audit.jsonl")));
        let config = Config {
            profile: "test".into(),
            bind: "127.0.0.1:8870".parse().unwrap(),
            storage_root: dir.path().into(),
            wal_root: dir.path().join("wal"),
            worker_socket: dir.path().join("worker.sock"),
            scope_id: "s".into(),
            real_order_submission_enabled: false,
            shadow_mode: true,
            maximum_book_age_ms: 30_000,
            legacy_mcp_url: None,
            python_workers: PythonWorkerConfig::disabled(),
        };
        let runtime =
            marketcow_runtime::PolymarketRuntime::open(marketcow_runtime::RuntimeConfig {
                root: dir.path().join("polymarket"),
                scope_id: "s".into(),
                config_revision: "test-config-v1".into(),
                wal_segment_bytes: 1_024,
                recent_event_capacity: 100,
            })
            .unwrap();
        let (stream, _) = broadcast::channel(STREAM_CHANNEL_CAPACITY);
        (
            dir,
            AppState {
                config,
                audit,
                metrics: Arc::new(Metrics::default()),
                projection: Arc::new(ArcSwap::from_pointee(bootstrap_projection("s".into()))),
                recent_events: Arc::new(ArcSwap::from_pointee(Vec::new())),
                runtime: Arc::new(AsyncMutex::new(runtime)),
                jobs: Arc::new(DurableJobCoordinator::memory()),
                instruments: Arc::new(InstrumentCoordinator::memory()),
                control_plane: Arc::new(ControlPlaneCoordinator::memory("test-config-v1")),
                stream,
                worker_status: Arc::new(PythonWorkerStatus::default()),
                legacy_mcp: None,
            },
        )
    }

    fn instrument_fixture() -> marketcow_storage::InstrumentRecord {
        marketcow_storage::InstrumentRecord {
            schema_version: 1,
            instrument_id: "AAPL.XNAS".into(),
            instrument_type: "equity".into(),
            asset_class: "equity".into(),
            symbol: "AAPL".into(),
            market: "US".into(),
            mic: "XNAS".into(),
            currency: "USD".into(),
            price_precision: 4,
            size_precision: 8,
            tick_size: "0.0100".parse().unwrap(),
            size_increment: "0.00000001".parse().unwrap(),
            lot_size: "1".parse().unwrap(),
            ts_event: chrono::DateTime::parse_from_rfc3339("2026-08-28T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            ts_init: chrono::DateTime::parse_from_rfc3339("2026-08-28T00:00:01Z")
                .unwrap()
                .with_timezone(&Utc),
            provider_symbols: BTreeMap::from([("longport".into(), "AAPL.US".into())]),
            broker_symbols: BTreeMap::from([("ibkr".into(), "AAPL".into())]),
            content_hash: format!("sha256:{}", "a".repeat(64)),
            updated_at: chrono::DateTime::parse_from_rfc3339("2026-08-28T00:00:02Z")
                .unwrap()
                .with_timezone(&Utc),
        }
    }

    fn instrument_input_fixture() -> InstrumentInput {
        InstrumentInput {
            schema_version: 1,
            instrument_id: "AAPL.XNAS".into(),
            instrument_type: "equity".into(),
            asset_class: "equity".into(),
            symbol: "AAPL".into(),
            market: "US".into(),
            mic: "XNAS".into(),
            currency: "USD".into(),
            price_precision: 4,
            size_precision: 8,
            tick_size: "0.0100".parse().unwrap(),
            size_increment: "0.00000001".parse().unwrap(),
            lot_size: "1".parse().unwrap(),
            ts_event: chrono::DateTime::parse_from_rfc3339("2026-08-28T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            ts_init: chrono::DateTime::parse_from_rfc3339("2026-08-28T00:00:01Z")
                .unwrap()
                .with_timezone(&Utc),
            provider_symbols: BTreeMap::from([("longport".into(), "苹果😀.US".into())]),
            broker_symbols: BTreeMap::from([("ibkr".into(), "AAPL".into())]),
        }
    }

    #[test]
    fn runtime_config_revision_is_deterministic_sensitive_and_secret_free() {
        let (_dir, state) = test_state();
        let config = state.config;
        let revision = calculated_config_revision(&config).unwrap();
        assert!(revision.starts_with("sha256:"));
        assert_eq!(revision.len(), 71);
        assert_eq!(calculated_config_revision(&config).unwrap(), revision);

        let mut changed = config.clone();
        changed.scope_id = "different-scope".into();
        assert_ne!(calculated_config_revision(&changed).unwrap(), revision);

        let mut secret_reference = config;
        secret_reference.python_workers.secret_references.insert(
            SEC_DIVIDEND_TASK.into(),
            PathBuf::from("/private/provider-secret"),
        );
        assert_eq!(
            calculated_config_revision(&secret_reference).unwrap(),
            revision
        );
    }

    #[test]
    fn rust_instrument_canonical_hash_matches_python_ascii_json() {
        let input = instrument_input_fixture();
        let payload = serde_json::to_value(input).unwrap();
        assert_eq!(
            python_canonical_hash(&payload),
            "sha256:3f9bc2d717069d9ed054df3affc0d51d1b9db736bb52d69bc18b1e6a60fd7c59"
        );
        assert_eq!(
            python_canonical_hash(&json!({"b":"😀","a":"苹果","n":1})),
            "sha256:4edca75104bc677b750ffe0eabd0763dccbeacd9e38f7b67157591261fde598b"
        );
    }

    #[tokio::test]
    async fn admin_instrument_upsert_is_validated_hashed_and_queryable() {
        let (_dir, state) = test_state();
        let response = admin_upsert_instrument(
            State(state.clone()),
            AxumPath("AAPL.XNAS".into()),
            Json(instrument_input_fixture()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let saved: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(
            saved["content_hash"],
            "sha256:3f9bc2d717069d9ed054df3affc0d51d1b9db736bb52d69bc18b1e6a60fd7c59"
        );
        assert_eq!(saved["tick_size"], "0.0100");
        assert!(saved["updated_at"].as_str().unwrap().ends_with('Z'));

        let fetched = state.instruments.get("AAPL.XNAS").await.unwrap().unwrap();
        assert_eq!(
            fetched.content_hash,
            saved["content_hash"].as_str().unwrap()
        );
        assert_eq!(
            state
                .instruments
                .resolve("provider:longport", "苹果😀.US")
                .await
                .unwrap()
                .unwrap()
                .instrument_id,
            "AAPL.XNAS"
        );

        let conflict = admin_upsert_instrument(
            State(state),
            AxumPath("MSFT.XNAS".into()),
            Json(instrument_input_fixture()),
        )
        .await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn native_instrument_http_and_mcp_share_the_exact_record() {
        let (_dir, state) = test_state();
        let fixture = instrument_fixture();
        state.instruments.insert_fixture(fixture.clone()).await;

        let http = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/v1/instruments/AAPL.XNAS")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(http.status(), StatusCode::OK);
        let http: serde_json::Value =
            serde_json::from_slice(&to_bytes(http.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(http, serde_json::to_value(&fixture).unwrap());
        assert_eq!(http["tick_size"], "0.0100");

        let resolved = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri(
                        "/v1/instruments:resolve?namespace=provider%3Alongport&external_symbol=AAPL.US",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resolved.status(), StatusCode::OK);
        let resolved: serde_json::Value =
            serde_json::from_slice(&to_bytes(resolved.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(resolved, http);

        let unresolved = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri(
                        "/v1/instruments:resolve?namespace=provider%3Alongport&external_symbol=MSFT.US",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unresolved.status(), StatusCode::NOT_FOUND);
        let unresolved: serde_json::Value =
            serde_json::from_slice(&to_bytes(unresolved.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(
            unresolved["detail"],
            json!({
                "code":"instrument_mapping_not_found",
                "namespace":"provider:longport",
                "external_symbol":"MSFT.US"
            })
        );

        let mcp = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":21,"method":"tools/call","params":{"name":"get_instrument","arguments":{"instrument_id":" AAPL.XNAS "}}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let mcp: serde_json::Value =
            serde_json::from_slice(&to_bytes(mcp.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(mcp["result"]["isError"], false);
        assert_eq!(mcp["result"]["structuredContent"], http);

        let missing = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/v1/instruments/MSFT.XNAS")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let missing: serde_json::Value =
            serde_json::from_slice(&to_bytes(missing.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(missing["detail"]["code"], "instrument_not_found");

        let invalid = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":22,"method":"tools/call","params":{"name":"get_instrument","arguments":{"instrument_id":"AAPL.XNAS","extra":true}}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let invalid: serde_json::Value =
            serde_json::from_slice(&to_bytes(invalid.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(invalid["result"]["isError"], true);
        assert_eq!(
            invalid["result"]["structuredContent"]["error"],
            "invalid_tool_input"
        );
    }

    #[tokio::test]
    async fn batch_instrument_resolution_preserves_order_and_fails_closed_without_worker() {
        let (_dir, state) = test_state();
        state.instruments.insert_fixture(instrument_fixture()).await;
        let response = resolve_instruments_batch(
            State(state),
            Extension("batch-registry-1".into()),
            Json(ResolveInstrumentBatchRequest {
                namespace: "provider:longport".into(),
                symbols: vec![" AAPL.US ".into(), "missing.us".into()],
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(body["count"], 2);
        assert_eq!(body["resolved_count"], 1);
        assert_eq!(body["error_count"], 1);
        assert_eq!(body["items"][0]["external_symbol"], "AAPL.US");
        assert_eq!(body["items"][0]["instrument_id"], "AAPL.XNAS");
        assert_eq!(body["items"][0]["resolution"], "registry");
        assert_eq!(body["items"][1]["external_symbol"], "MISSING.US");
        assert_eq!(body["items"][1]["error"]["code"], "provider_unavailable");
    }

    #[tokio::test]
    async fn batch_instrument_resolution_waits_for_verified_worker_artifact_and_persists_mapping() {
        let (dir, mut state) = test_state();
        let policy = marketcow_jobs::DispatchPolicy {
            max_in_flight: 1,
            minimum_interval_millis: 0,
        };
        state.config.python_workers.dispatch_policies =
            BTreeMap::from([(LONGPORT_RESOLVE_TASK.into(), policy)]);
        let secret = dir.path().join("longport-secret.json");
        fs::write(
            &secret,
            br#"{"app_key":"fixture","app_secret":"fixture","access_token":"fixture"}"#,
        )
        .unwrap();
        state
            .config
            .python_workers
            .secret_references
            .insert(LONGPORT_RESOLVE_TASK.into(), secret);
        state.worker_status.live.store(1, Ordering::Relaxed);
        state.jobs = Arc::new(DurableJobCoordinator::memory_with_policies(BTreeMap::from(
            [(LONGPORT_RESOLVE_TASK.into(), policy)],
        )));

        let endpoint_state = state.clone();
        let endpoint = tokio::spawn(async move {
            resolve_instruments_batch(
                State(endpoint_state),
                Extension("batch-worker-1".into()),
                Json(ResolveInstrumentBatchRequest {
                    namespace: "provider:longport".into(),
                    symbols: vec!["MU.US".into(), "MISSING.US".into()],
                }),
            )
            .await
        });
        let capabilities = vec![LONGPORT_RESOLVE_TASK.into()];
        let claimed = loop {
            if let Some(job) = state
                .jobs
                .reconcile_and_claim(
                    "python-longport-test",
                    &capabilities,
                    chrono::Duration::minutes(1),
                    Utc::now(),
                )
                .await
                .unwrap()
            {
                break job;
            }
            tokio::task::yield_now().await;
        };
        assert_eq!(claimed.job_type, LONGPORT_RESOLVE_TASK);
        assert_eq!(
            claimed.request,
            json!({"namespace":"provider:longport","symbols":["MU.US","MISSING.US"]})
        );
        let lease = claimed.lease_token.clone().unwrap();
        state
            .jobs
            .start(&claimed.job_id, &lease, Utc::now())
            .await
            .unwrap();
        let payload = json!({
            "schema_version":LONGPORT_RESOLVE_RESULT_SCHEMA,
            "namespace":"provider:longport",
            "observed_at":"2026-08-28T00:00:00Z",
            "items":[
                {"external_symbol":"MU.US","status":"resolved","instrument_id":"MU.XNAS",
                 "symbol":"MU","mic":"XNAS","market":"US","currency":"USD",
                 "lot_size":"1","source":"longport.static_info","source_exchange":"NASD"},
                {"external_symbol":"MISSING.US","status":"error",
                 "error":{"code":"not_found","message":"fixture missing"}}
            ]
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let sha256 = hex::encode(Sha256::digest(&bytes));
        let staging_root = dir.path().join("worker-staging");
        let task_root = staging_root.join(&claimed.job_id);
        fs::create_dir_all(&task_root).unwrap();
        fs::write(task_root.join("result.json"), &bytes).unwrap();
        let result = marketcow_jobs::StagedResult {
            relative_path: "result.json".into(),
            sha256,
            size_bytes: bytes.len() as u64,
            media_type: "application/json".into(),
        };
        let artifact = marketcow_storage::promote_worker_artifact(
            marketcow_storage::WorkerArtifactPromotion {
                staging_root: &staging_root,
                artifact_root: &dir.path().join("artifacts"),
                job_id: &claimed.job_id,
                dataset: LONGPORT_RESOLVE_TASK,
                revision: LONGPORT_RESOLVE_REQUEST_SCHEMA,
                source: "python-worker:test",
                result: &result,
                ingested_at: Utc::now(),
            },
        )
        .unwrap();
        let promoted_result = marketcow_jobs::StagedResult {
            relative_path: artifact.relative_path.clone(),
            sha256: artifact.sha256.clone(),
            size_bytes: artifact.byte_size,
            media_type: artifact.media_type.clone(),
        };
        state
            .jobs
            .succeed_with_artifact(
                &claimed.job_id,
                &lease,
                promoted_result,
                artifact,
                Utc::now(),
            )
            .await
            .unwrap();

        let response = endpoint.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(body["resolved_count"], 1);
        assert_eq!(body["items"][0]["instrument_id"], "MU.XNAS");
        assert_eq!(body["items"][0]["resolution"], "upstream");
        assert_eq!(body["items"][1]["error"]["code"], "not_found");
        assert_eq!(
            state
                .instruments
                .resolve("provider:longport", "MU.US")
                .await
                .unwrap()
                .unwrap()
                .instrument_id,
            "MU.XNAS"
        );
    }

    async fn legacy_mcp_fixture(Json(message): Json<serde_json::Value>) -> Json<serde_json::Value> {
        let id = message
            .get("id")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        match message.get("method").and_then(serde_json::Value::as_str) {
            Some("tools/list") => Json(json!({
                "jsonrpc":"2.0",
                "id":id,
                "result":{"tools":marketcow_contracts::MCP_LEGACY_TOOL_NAMES.iter().map(|name| json!({
                    "name":name,
                    "description":format!("fixture {name}"),
                    "inputSchema":{"type":"object","properties":{},"required":[],"additionalProperties":false},
                    "annotations":{"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}
                })).collect::<Vec<_>>()}
            })),
            Some("tools/call") => {
                let name = message
                    .pointer("/params/name")
                    .and_then(serde_json::Value::as_str);
                let payload = json!({"via":"legacy_fixture","tool":name});
                Json(mcp_result(id, mcp_tool_result(payload, false)))
            }
            _ => Json(mcp_error(id, -32601, "Method not found")),
        }
    }
    #[tokio::test]
    async fn public_health_and_scope_preserve_safety_flags() {
        let (_dir, state) = test_state();
        let response = app(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["real_order_submission_enabled"],
            false
        );
    }

    #[tokio::test]
    async fn rust_mcp_transport_is_golden_read_only_and_fails_closed() {
        let (_dir, state) = test_state();
        let initialize = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(initialize.status(), StatusCode::OK);
        let initialized: serde_json::Value =
            serde_json::from_slice(&to_bytes(initialize.into_body(), 16_384).await.unwrap())
                .unwrap();
        assert_eq!(initialized["result"]["protocolVersion"], "2025-06-18");

        let list = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .header("mcp-protocol-version", "2025-11-25")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let listed: serde_json::Value =
            serde_json::from_slice(&to_bytes(list.into_body(), 16_384).await.unwrap()).unwrap();
        let health_golden: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/mcp-service-health-tool-v1.json"
        ))
        .unwrap();
        let instrument_golden: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/mcp-get-instrument-tool-v1.json"
        ))
        .unwrap();
        assert_eq!(
            listed["result"]["tools"],
            json!([health_golden, instrument_golden])
        );

        let call = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"service_health","arguments":{}}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let called: serde_json::Value =
            serde_json::from_slice(&to_bytes(call.into_body(), 65_536).await.unwrap()).unwrap();
        assert_eq!(called["result"]["isError"], false);
        assert_eq!(
            called["result"]["structuredContent"]["real_order_submission_enabled"],
            false
        );
        assert_eq!(
            called["result"]["structuredContent"]["mcp"]["endpoint"],
            "/mcp"
        );

        for request in [
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("content-type", "application/json")
                .header("origin", "https://attacker.example")
                .body(Body::from(r#"{"jsonrpc":"2.0","id":4,"method":"ping"}"#))
                .unwrap(),
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("content-type", "application/json")
                .header("mcp-protocol-version", "unknown")
                .body(Body::from(r#"{"jsonrpc":"2.0","id":5,"method":"ping"}"#))
                .unwrap(),
        ] {
            let response = app(state.clone()).oneshot(request).await.unwrap();
            assert!(matches!(
                response.status(),
                StatusCode::FORBIDDEN | StatusCode::BAD_REQUEST
            ));
        }
        let wrong_media = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "text/plain")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_media.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let parse_error = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from("{"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let parse_error: serde_json::Value =
            serde_json::from_slice(&to_bytes(parse_error.into_body(), 4096).await.unwrap())
                .unwrap();
        assert_eq!(parse_error["error"]["code"], -32700);

        let empty_batch = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from("[]"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let empty_batch: serde_json::Value =
            serde_json::from_slice(&to_bytes(empty_batch.into_body(), 4096).await.unwrap())
                .unwrap();
        assert_eq!(empty_batch["error"]["code"], -32600);

        let oversized = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(vec![
                        b' ';
                        marketcow_contracts::MCP_MAX_REQUEST_BYTES
                            + 1
                    ]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let notification = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(notification.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn rust_mcp_initialize_works_over_a_real_tcp_listener() {
        let (_dir, state) = test_state();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app(state)).await.unwrap();
        });
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"unknown"}}"#;
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!(
                    "POST /mcp HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        let payload = response.split("\r\n\r\n").nth(1).unwrap();
        let payload: serde_json::Value = serde_json::from_str(payload).unwrap();
        assert_eq!(
            payload["result"]["protocolVersion"],
            marketcow_contracts::MCP_LATEST_PROTOCOL_VERSION
        );
        assert_eq!(payload["result"]["serverInfo"]["name"], "marketcow");
        server.abort();
    }

    #[tokio::test]
    async fn rust_mcp_staged_proxy_is_loopback_bounded_and_preserves_tool_results() {
        let legacy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let legacy_address = legacy_listener.local_addr().unwrap();
        let legacy_server = tokio::spawn(async move {
            axum::serve(
                legacy_listener,
                Router::new().route("/mcp", post(legacy_mcp_fixture)),
            )
            .await
            .unwrap();
        });
        let (_dir, mut state) = test_state();
        state.legacy_mcp =
            Some(LegacyMcpProxy::new(format!("http://{legacy_address}/mcp")).unwrap());

        let list = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":7,"method":"tools/list","params":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let listed: serde_json::Value =
            serde_json::from_slice(&to_bytes(list.into_body(), 65_536).await.unwrap()).unwrap();
        assert_eq!(listed["result"]["tools"].as_array().unwrap().len(), 14);
        assert_eq!(
            listed["result"]["tools"][0],
            marketcow_contracts::mcp_service_health_tool_definition()
        );
        assert_eq!(
            listed["result"]["tools"][2],
            marketcow_contracts::mcp_get_instrument_tool_definition()
        );

        let call = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"get_quotes","arguments":{"symbols":["AAPL.XNAS"]}}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let called: serde_json::Value =
            serde_json::from_slice(&to_bytes(call.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(called["id"], 8);
        assert_eq!(called["result"]["isError"], false);
        assert_eq!(
            called["result"]["structuredContent"]["via"],
            "legacy_fixture"
        );
        assert_eq!(called["result"]["structuredContent"]["tool"], "get_quotes");

        state.instruments = Arc::new(InstrumentCoordinator {
            repository: None,
            memory: AsyncMutex::new(BTreeMap::new()),
            memory_enabled: false,
        });
        let native_failure = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"get_instrument","arguments":{"instrument_id":"AAPL.XNAS"}}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let native_failure: serde_json::Value =
            serde_json::from_slice(&to_bytes(native_failure.into_body(), 16_384).await.unwrap())
                .unwrap();
        assert_eq!(native_failure["result"]["isError"], true);
        assert_eq!(
            native_failure["result"]["structuredContent"]["error"],
            "marketcow_api_error"
        );
        assert_eq!(
            native_failure["result"]["structuredContent"]["detail"]["status_code"],
            503
        );
        assert!(
            native_failure["result"]["structuredContent"]
                .get("via")
                .is_none()
        );

        assert!(
            validate_legacy_mcp_url(
                "https://127.0.0.1:8791/mcp",
                "127.0.0.1:8790".parse().unwrap()
            )
            .is_err()
        );
        assert!(
            validate_legacy_mcp_url(
                "http://example.com:8791/mcp",
                "127.0.0.1:8790".parse().unwrap()
            )
            .is_err()
        );
        assert!(
            validate_legacy_mcp_url(
                "http://127.0.0.1:8790/mcp",
                "127.0.0.1:8790".parse().unwrap()
            )
            .is_err()
        );
        legacy_server.abort();
    }

    #[tokio::test]
    async fn artifact_manifest_and_job_success_are_one_coordinator_transition() {
        let jobs = DurableJobCoordinator::memory();
        let now = Utc::now();
        let submitted = jobs
            .submit(
                marketcow_jobs::SubmitJob {
                    idempotency_key: "artifact-transition-1".into(),
                    job_type: "provider.history".into(),
                    request_schema: "marketcow.provider.history.v1".into(),
                    request: json!({"symbol":"AAPL.XNAS"}),
                    deadline: now + chrono::Duration::minutes(5),
                    max_attempts: 2,
                    audit_actor: "test".into(),
                },
                now,
            )
            .await
            .unwrap();
        let claimed = jobs
            .reconcile_and_claim(
                "worker",
                &["provider.history".into()],
                chrono::Duration::seconds(60),
                now,
            )
            .await
            .unwrap()
            .unwrap();
        let lease_token = claimed.lease_token.unwrap();
        jobs.start(&submitted.job_id, &lease_token, now)
            .await
            .unwrap();
        jobs.authorize_result(&submitted.job_id, &lease_token, now)
            .await
            .unwrap();
        let relative_path = format!(
            "provider.history/marketcow.provider.history.v1/aa/{}",
            "a".repeat(64)
        );
        let result = marketcow_jobs::StagedResult {
            relative_path: relative_path.clone(),
            sha256: "a".repeat(64),
            size_bytes: 4,
            media_type: "application/json".into(),
        };
        let artifact = marketcow_storage::ArtifactManifestRecord {
            artifact_id: "b".repeat(64),
            dataset: "provider.history".into(),
            revision: "marketcow.provider.history.v1".into(),
            source: "python-worker:test".into(),
            source_url: None,
            observed_at: now,
            ingested_at: now,
            raw_response_locator: Some(format!(
                "worker-staging://{}/result.json",
                submitted.job_id
            )),
            storage_path: format!("/tmp/artifacts/{relative_path}"),
            relative_path,
            sha256: "a".repeat(64),
            byte_size: 4,
            media_type: "application/json".into(),
            metadata_json: json!({"job_id":submitted.job_id}),
        };
        let mut mismatched = artifact.clone();
        mismatched.byte_size = 5;
        assert!(
            jobs.succeed_with_artifact(
                &submitted.job_id,
                &lease_token,
                result.clone(),
                mismatched,
                now,
            )
            .await
            .is_err()
        );
        assert_eq!(
            jobs.get(&submitted.job_id).await.unwrap().status,
            marketcow_jobs::JobStatus::Running
        );
        let mut wrong_contract = artifact.clone();
        wrong_contract.dataset = "provider.other".into();
        assert!(
            jobs.succeed_with_artifact(
                &submitted.job_id,
                &lease_token,
                result.clone(),
                wrong_contract,
                now,
            )
            .await
            .is_err()
        );
        assert_eq!(
            jobs.get(&submitted.job_id).await.unwrap().status,
            marketcow_jobs::JobStatus::Running
        );
        let completed = jobs
            .succeed_with_artifact(&submitted.job_id, &lease_token, result, artifact, now)
            .await
            .unwrap();
        assert_eq!(completed.status, marketcow_jobs::JobStatus::Succeeded);
        assert_eq!(jobs.memory_artifacts.lock().await.len(), 1);
    }

    #[test]
    fn rust_validates_worker_result_schema_before_registration() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("result.json");
        let csv_result = json!({
            "schema_version":CSV_INFERENCE_RESULT_SCHEMA,
            "source":"fixture://dividend.csv",
            "observed_at":"2026-08-28T00:00:00+00:00",
            "content_sha256":"a".repeat(64),
            "delimiter":",",
            "columns":["symbol","amount","currency"],
            "rows":[["AAPL.XNAS","0.250000000000000001","USD"]],
            "row_count":1
        });
        let bytes = serde_json::to_vec(&csv_result).unwrap();
        fs::write(&path, &bytes).unwrap();
        validate_worker_result_artifact(
            CSV_INFERENCE_TASK,
            CSV_INFERENCE_REQUEST_SCHEMA,
            &path,
            bytes.len() as u64,
            "application/json",
        )
        .unwrap();
        assert!(
            validate_worker_result_artifact(
                "provider.unknown",
                "marketcow.worker.unknown.v1",
                &path,
                bytes.len() as u64,
                "application/json",
            )
            .is_err()
        );

        let mut mismatched = csv_result;
        mismatched["row_count"] = json!(2);
        let bytes = serde_json::to_vec(&mismatched).unwrap();
        fs::write(&path, &bytes).unwrap();
        assert!(
            validate_worker_result_artifact(
                CSV_INFERENCE_TASK,
                CSV_INFERENCE_REQUEST_SCHEMA,
                &path,
                bytes.len() as u64,
                "application/json",
            )
            .is_err()
        );
    }

    #[test]
    fn rust_requires_exact_decimal_strings_and_sec_provenance() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("result.json");
        let mut result = json!({
            "schema_version":SEC_DIVIDEND_RESULT_SCHEMA,
            "rows":[{
                "symbol":"AAPL.XNAS",
                "fiscal_year":2026,
                "amount_per_share":"0.250000000000000001",
                "currency":"USD",
                "announcement_date":"2026-08-28",
                "record_date":"2026-09-10",
                "ex_date":null,
                "payment_date":"2026-09-30",
                "expected_payment_date":"2026-09-30",
                "confirmation_status":"confirmed",
                "source_type":"regulatory_filing",
                "source_name":"SEC EDGAR",
                "source_url":"https://www.sec.gov/Archives/fixture.htm",
                "source_document_id":"0000000000-26-000001#0"
            }]
        });
        let bytes = serde_json::to_vec(&result).unwrap();
        fs::write(&path, &bytes).unwrap();
        validate_worker_result_artifact(
            SEC_DIVIDEND_TASK,
            SEC_DIVIDEND_REQUEST_SCHEMA,
            &path,
            bytes.len() as u64,
            "application/json",
        )
        .unwrap();

        result["rows"][0]["amount_per_share"] = json!(0.25_f64);
        let bytes = serde_json::to_vec(&result).unwrap();
        fs::write(&path, &bytes).unwrap();
        assert!(
            validate_worker_result_artifact(
                SEC_DIVIDEND_TASK,
                SEC_DIVIDEND_REQUEST_SCHEMA,
                &path,
                bytes.len() as u64,
                "application/json",
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn uds_worker_handshake_claim_start_and_verified_completion() {
        use marketcow_contracts::{WorkerFrame, WorkerMessage};

        let dir = tempdir().unwrap();
        let socket = dir.path().join("worker.sock");
        let staging = dir.path().join("staging");
        let now = Utc::now();
        let mut engine = marketcow_jobs::JobEngine::default();
        let job_id = engine
            .submit(
                marketcow_jobs::SubmitJob {
                    idempotency_key: "worker-flow-1".into(),
                    job_type: CSV_INFERENCE_TASK.into(),
                    request_schema: CSV_INFERENCE_REQUEST_SCHEMA.into(),
                    request: json!({"content":"symbol,amount\nAAPL.XNAS,0.125\n","source":"fixture://input.csv","observed_at":"2026-08-28T00:00:00Z"}),
                    deadline: now + chrono::Duration::minutes(5),
                    max_attempts: 2,
                    audit_actor: "test".into(),
                },
                now,
            )
            .unwrap()
            .job_id
            .clone();
        let jobs = Arc::new(DurableJobCoordinator {
            engine: AsyncMutex::new(engine),
            state_changed: Notify::new(),
            repository: None,
            memory_artifacts: AsyncMutex::new(BTreeMap::new()),
            dispatch_policies: default_dispatch_policies(),
        });
        let server_jobs = jobs.clone();
        let server_socket = socket.clone();
        let server_staging = staging.clone();
        let artifact_root = dir.path().join("artifacts");
        let server_artifacts = artifact_root.clone();
        let server = tokio::spawn(async move {
            worker_server(server_socket, server_staging, server_artifacts, server_jobs).await
        });
        let mut stream = None;
        for _ in 0..100 {
            assert!(!server.is_finished(), "worker server exited during startup");
            match UnixStream::connect(&socket).await {
                Ok(connected) => {
                    stream = Some(connected);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
            }
        }
        let mut stream = stream.expect("worker socket should become available");
        assert_eq!(
            fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
            0o600
        );

        write_frame(
            &mut stream,
            &WorkerFrame::new(
                "hello-1",
                WorkerMessage::Hello {
                    worker_id: "python-test".into(),
                    worker_revision: "test-revision".into(),
                    nonce: "nonce-1".into(),
                    capabilities: vec![CSV_INFERENCE_TASK.into()],
                },
            ),
        )
        .await
        .unwrap();
        let ack = read_frame(&mut stream).await.unwrap();
        assert!(matches!(
            ack.message,
            WorkerMessage::HelloAck { nonce, .. } if nonce == "nonce-1"
        ));

        write_frame(
            &mut stream,
            &WorkerFrame::new("poll-1", WorkerMessage::Poll),
        )
        .await
        .unwrap();
        let task = read_frame(&mut stream).await.unwrap();
        let (lease_token, staging_path) = match task.message {
            WorkerMessage::Task {
                job_id: assigned,
                lease_token,
                staging_path,
                request_sha256,
                ..
            } => {
                assert_eq!(assigned, job_id);
                assert_eq!(request_sha256.len(), 64);
                (lease_token, staging_path)
            }
            other => panic!("expected worker task, got {other:?}"),
        };

        write_frame(
            &mut stream,
            &WorkerFrame::new(
                "start-1",
                WorkerMessage::Start {
                    job_id: job_id.clone(),
                    lease_token: lease_token.clone(),
                },
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            read_frame(&mut stream).await.unwrap().message,
            WorkerMessage::JobState { ref status, .. } if status == "running"
        ));

        let result_bytes = serde_json::to_vec(&json!({
            "schema_version":CSV_INFERENCE_RESULT_SCHEMA,
            "source":"fixture://input.csv",
            "observed_at":"2026-08-28T00:00:00+00:00",
            "content_sha256":"a".repeat(64),
            "delimiter":",",
            "columns":["symbol","amount"],
            "rows":[["AAPL.XNAS","0.125"]],
            "row_count":1
        }))
        .unwrap();
        let result_path = PathBuf::from(staging_path).join("result.json");
        fs::write(&result_path, &result_bytes).unwrap();
        let result_sha256 = hex::encode(Sha256::digest(&result_bytes));
        write_frame(
            &mut stream,
            &WorkerFrame::new(
                "complete-1",
                WorkerMessage::Complete {
                    job_id: job_id.clone(),
                    lease_token,
                    relative_path: "result.json".into(),
                    sha256: result_sha256,
                    size_bytes: result_bytes.len() as u64,
                    media_type: "application/json".into(),
                },
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            read_frame(&mut stream).await.unwrap().message,
            WorkerMessage::JobState { ref status, .. } if status == "succeeded"
        ));
        let completed = jobs.get(&job_id).await.unwrap();
        assert_eq!(completed.status, marketcow_jobs::JobStatus::Succeeded);
        let completed_result = completed.result.unwrap();
        assert_ne!(completed_result.relative_path, "result.json");
        assert_eq!(
            fs::read(artifact_root.join(&completed_result.relative_path)).unwrap(),
            result_bytes
        );
        assert_eq!(jobs.memory_artifacts.lock().await.len(), 1);
        server.abort();
    }

    #[tokio::test]
    async fn admin_job_submission_is_idempotent_and_never_exposes_lease_token() {
        let (_dir, state) = test_state();
        let mut headers = HeaderMap::new();
        headers.insert("idempotency-key", HeaderValue::from_static("idem-admin-1"));
        let request = || SubmitProviderJobRequest {
            job_type: "provider.history".into(),
            request_schema: "marketcow.provider.history.v1".into(),
            request: json!({"symbol":"AAPL.XNAS"}),
            deadline: Utc::now() + chrono::Duration::minutes(5),
            max_attempts: 2,
        };
        let first = admin_submit_job(
            State(state.clone()),
            Extension("request-1".into()),
            headers.clone(),
            Json(request()),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = to_bytes(first.into_body(), 16_384).await.unwrap();
        let first_value: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert!(first_value.get("lease_token").is_none());
        assert_eq!(first_value["lease_active"], false);
        assert_eq!(first_value["real_order_submission_enabled"], false);

        let second = admin_submit_job(
            State(state.clone()),
            Extension("request-2".into()),
            headers.clone(),
            Json(request()),
        )
        .await;
        let second_body = to_bytes(second.into_body(), 16_384).await.unwrap();
        let second_value: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(first_value["job_id"], second_value["job_id"]);

        let conflict = admin_submit_job(
            State(state),
            Extension("request-3".into()),
            headers,
            Json(SubmitProviderJobRequest {
                request: json!({"symbol":"MSFT.XNAS"}),
                ..request()
            }),
        )
        .await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
    }
    #[tokio::test]
    async fn admin_is_fail_closed_without_token() {
        let (dir, state) = test_state();
        let response = app(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/migration")
                    .header("x-request-id", "audit-rejection-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let events = fs::read_to_string(dir.path().join("audit.jsonl")).unwrap();
        let events = events
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["schema_version"], "marketcow.admin-audit.v1");
        assert_eq!(events[0]["request_id"], "audit-rejection-1");
        assert_eq!(events[0]["action"], "http.admin.request");
        assert_eq!(events[0]["outcome"], "rejected");
        assert_eq!(events[0]["parameters_json"]["stage"], "authentication");
    }

    #[tokio::test]
    async fn admin_audit_read_model_is_versioned_filtered_and_bounded() {
        let (_dir, state) = test_state();
        let event = admin_audit_record(
            "request-audit-list-1",
            "admin:test",
            "/v1/admin/instruments/AAPL.XNAS",
            "succeeded",
            json!({"method":"PUT"}),
            "",
        );
        state.audit.record_admin(&event).await.unwrap();
        let response = admin_audit_events(
            State(state.clone()),
            Extension("query-1".into()),
            Query(AdminAuditQuery {
                limit: 10,
                offset: 0,
                action: "http.admin.request".into(),
                outcome: "succeeded".into(),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(body["schema"], "marketcow.admin-audit.v1");
        assert_eq!(body["durable"], false);
        assert_eq!(body["page"], json!({"limit":10,"offset":0,"returned":1}));
        assert_eq!(body["items"][0], serde_json::to_value(event).unwrap());

        let invalid = admin_audit_events(
            State(state),
            Extension("query-2".into()),
            Query(AdminAuditQuery {
                limit: 201,
                offset: 0,
                action: String::new(),
                outcome: String::new(),
            }),
        )
        .await;
        assert_eq!(invalid.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn shadow_migration_checkpoint_is_cas_fenced_and_never_enables_cutover() {
        let (_dir, state) = test_state();
        let migration = admin_migration(State(state.clone())).await.0;
        assert_eq!(migration["schema"], "marketcow.migration-control.v1");
        assert_eq!(migration["cutover_allowed"], false);
        assert_eq!(migration["real_order_submission_enabled"], false);
        assert_eq!(migration["tradude_may_manage_marketcow"], false);
        assert_eq!(
            migration["ownership_registry"]["sha256"],
            "c71864af6bc9227cf166d42dc3963da12261b8fe80cb1c1ce581f2695714bf5e"
        );

        let path = AxumPath((
            "shadow-instrument-1".into(),
            "instrument_master".into(),
            "all".into(),
        ));
        let created = admin_put_migration_checkpoint(
            State(state.clone()),
            Extension("checkpoint-create".into()),
            path,
            Json(MigrationCheckpointInput {
                expected_revision: 0,
                status: "running".into(),
                source_watermark: Some("python:100".into()),
                target_watermark: Some("rust:99".into()),
                cursor_json: json!({"after":"AAPL.XNAS"}),
                evidence_json: json!({"diff_count":0}),
                error: None,
            }),
        )
        .await;
        assert_eq!(created.status(), StatusCode::OK);
        let created: serde_json::Value =
            serde_json::from_slice(&to_bytes(created.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(created["checkpoint"]["revision"], 1);
        assert_eq!(created["cutover_allowed"], false);

        let completed_input = || MigrationCheckpointInput {
            expected_revision: 1,
            status: "completed".into(),
            source_watermark: Some("python:100".into()),
            target_watermark: Some("rust:100".into()),
            cursor_json: json!({"after":"AAPL.XNAS"}),
            evidence_json: json!({"diff_count":0,"shadow_only":true}),
            error: None,
        };
        let completed = admin_put_migration_checkpoint(
            State(state.clone()),
            Extension("checkpoint-complete".into()),
            AxumPath((
                "shadow-instrument-1".into(),
                "instrument_master".into(),
                "all".into(),
            )),
            Json(completed_input()),
        )
        .await;
        assert_eq!(completed.status(), StatusCode::OK);
        let completed: serde_json::Value =
            serde_json::from_slice(&to_bytes(completed.into_body(), 16_384).await.unwrap())
                .unwrap();
        assert_eq!(completed["checkpoint"]["revision"], 2);
        assert_eq!(completed["checkpoint"]["status"], "completed");

        let stale = admin_put_migration_checkpoint(
            State(state.clone()),
            Extension("checkpoint-stale".into()),
            AxumPath((
                "shadow-instrument-1".into(),
                "instrument_master".into(),
                "all".into(),
            )),
            Json(completed_input()),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::CONFLICT);

        let fetched = admin_get_migration_checkpoint(
            State(state),
            Extension("checkpoint-get".into()),
            AxumPath((
                "shadow-instrument-1".into(),
                "instrument_master".into(),
                "all".into(),
            )),
        )
        .await;
        assert_eq!(fetched.status(), StatusCode::OK);
        let fetched: serde_json::Value =
            serde_json::from_slice(&to_bytes(fetched.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(fetched["checkpoint"], completed["checkpoint"]);
        assert_eq!(fetched["cutover_allowed"], false);
    }

    #[tokio::test]
    async fn realtime_routes_fail_closed_until_projection_is_ready() {
        let (_dir, state) = test_state();
        for uri in [
            "/v1/readiness",
            "/v1/prediction-markets/polymarket/live/snapshot",
            "/v1/prediction-markets/polymarket/live/events",
            "/v1/prediction-markets/polymarket/live/checkpoint",
            "/v1/prediction-markets/polymarket/live/full-sync",
        ] {
            let response = app(state.clone())
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{uri}");
        }
    }

    #[tokio::test]
    async fn ready_projection_serves_snapshot_checkpoint_and_bounded_events() {
        let (_dir, state) = test_state();
        let mut projection = bootstrap_projection("s".into());
        projection.generation = 1;
        projection.cursor = 1;
        projection.persisted_cursor = 1;
        projection.ready = true;
        projection.fail_closed_reason = None;
        projection.books.insert(
            "yes".into(),
            marketcow_core::Book {
                bids: [(
                    marketcow_core::Price::parse("0.4").unwrap(),
                    "2".parse().unwrap(),
                )]
                .into(),
                asks: [(
                    marketcow_core::Price::parse("0.6").unwrap(),
                    "3".parse().unwrap(),
                )]
                .into(),
                tick_size: Some(marketcow_core::Price::parse_tick("0.01").unwrap()),
                tick_version: "tick-v1".into(),
                source_observed_at: Some(Utc::now()),
                last_trade_price: None,
                last_trade_observed_at: None,
            },
        );
        state.projection.store(Arc::new(projection));
        for uri in [
            "/v1/readiness",
            "/v1/prediction-markets/polymarket/live/snapshot",
            "/v1/prediction-markets/polymarket/live/events?after_cursor=1&limit=10",
            "/v1/prediction-markets/polymarket/live/checkpoint",
            "/v1/prediction-markets/polymarket/live/full-sync",
        ] {
            let response = app(state.clone())
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
        }
        let invalid = app(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/prediction-markets/polymarket/live/events?limit=1001")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn shadow_ingest_updates_durable_projection_and_latency_metrics() {
        let (_dir, state) = test_state();
        let now = Utc::now();
        let response = admin_shadow_ingest(
            State(state.clone()),
            Extension("request-1".into()),
            Json(ShadowIngestRequest {
                received_at: Some(now),
                raw_payload: json!({
                    "event_type":"book", "asset_id":"yes",
                    "timestamp":now.to_rfc3339(), "tick_size":"0.01",
                    "bids":[{"price":"0.40","size":"10"}],
                    "asks":[{"price":"0.60","size":"11"}]
                }),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.projection.load().cursor, 1);
        assert!(state.projection.load().ready);
        assert_eq!(state.recent_events.load().len(), 1);

        let metrics_response = metrics(State(state)).await.into_response();
        let body = to_bytes(metrics_response.into_body(), 16_384)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("marketcow_projection_published_cursor 1"));
        assert!(body.contains("marketcow_projection_persisted_cursor 1"));
        assert!(body.contains("marketcow_ingress_queue_depth 0"));
        assert!(body.contains("marketcow_persistence_latency_us "));
        assert!(body.contains("marketcow_publication_latency_us "));
    }

    #[tokio::test]
    async fn websocket_full_sync_live_publish_and_cursor_resume_are_contiguous() {
        let (_dir, state) = test_state();
        let first_at = Utc::now();
        let first = admin_shadow_ingest(
            State(state.clone()),
            Extension("seed-1".into()),
            Json(ShadowIngestRequest {
                received_at: Some(first_at),
                raw_payload: json!({
                    "event_type":"book", "asset_id":"yes",
                    "timestamp":first_at.to_rfc3339(), "tick_size":"0.01",
                    "bids":[{"price":"0.40","size":"10"}],
                    "asks":[{"price":"0.60","size":"11"}]
                }),
            }),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app(server_state)).await.unwrap();
        });
        let (mut client, _) =
            tokio_tungstenite::connect_async(format!("ws://{address}/v1/market-data/stream"))
                .await
                .unwrap();
        let subscription = next_stream_frame(&mut client).await;
        assert_eq!(subscription.cursor, 1);
        assert!(matches!(
            subscription.payload,
            marketcow_api::StreamPayload::Subscription {
                ref mode,
                boundary_cursor: 1,
                full_sync: Some(_),
            } if mode == "full_sync"
        ));

        let second_at = first_at + chrono::Duration::milliseconds(1);
        let second = admin_shadow_ingest(
            State(state.clone()),
            Extension("seed-2".into()),
            Json(ShadowIngestRequest {
                received_at: Some(second_at),
                raw_payload: json!({
                    "event_type":"book", "asset_id":"yes",
                    "timestamp":second_at.to_rfc3339(), "tick_size":"0.01",
                    "bids":[{"price":"0.41","size":"10"}],
                    "asks":[{"price":"0.59","size":"11"}]
                }),
            }),
        )
        .await;
        assert_eq!(second.status(), StatusCode::OK);
        let live = next_stream_frame(&mut client).await;
        assert_eq!(live.cursor, 2);
        assert!(matches!(
            live.payload,
            marketcow_api::StreamPayload::Event { ref event } if event.cursor == 2
        ));
        client.close(None).await.unwrap();

        let (mut resumed, _) = tokio_tungstenite::connect_async(format!(
            "ws://{address}/v1/market-data/stream?after_cursor=1"
        ))
        .await
        .unwrap();
        let resume_ack = next_stream_frame(&mut resumed).await;
        assert!(matches!(
            resume_ack.payload,
            marketcow_api::StreamPayload::Subscription {
                ref mode,
                boundary_cursor: 2,
                full_sync: None,
            } if mode == "resume"
        ));
        let replayed = next_stream_frame(&mut resumed).await;
        assert_eq!(replayed.cursor, 2);
        assert!(matches!(
            replayed.payload,
            marketcow_api::StreamPayload::Event { ref event } if event.cursor == 2
        ));
        let mut unavailable = (*state.projection.load_full()).clone();
        unavailable.ready = false;
        unavailable.fail_closed_reason = Some("test_gap".into());
        state.projection.store(Arc::new(unavailable));
        let resync = next_stream_frame(&mut resumed).await;
        assert!(matches!(
            resync.payload,
            marketcow_api::StreamPayload::ResyncRequired {
                ref reason,
                retryable: true,
            } if reason == "projection_unready_or_stale"
        ));
        let close = tokio::time::timeout(std::time::Duration::from_secs(2), resumed.next())
            .await
            .expect("stream close timed out")
            .expect("stream ended without close frame")
            .expect("stream transport failed");
        assert!(matches!(
            close,
            tokio_tungstenite::tungstenite::Message::Close(Some(frame))
                if u16::from(frame.code) == close_code::AGAIN
        ));
        server.abort();
    }

    async fn next_stream_frame<S>(
        client: &mut tokio_tungstenite::WebSocketStream<S>,
    ) -> marketcow_api::StreamFrame
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let message = tokio::time::timeout(std::time::Duration::from_secs(2), client.next())
            .await
            .expect("stream frame timed out")
            .expect("stream closed")
            .expect("stream transport failed");
        let text = message.into_text().expect("expected text frame");
        serde_json::from_str(&text).unwrap()
    }

    #[tokio::test]
    async fn stale_projection_is_not_ready_even_when_book_state_is_valid() {
        let (_dir, state) = test_state();
        let mut projection = bootstrap_projection("s".into());
        projection.ready = true;
        projection.fail_closed_reason = None;
        projection.books.insert(
            "yes".into(),
            marketcow_core::Book {
                bids: [(
                    marketcow_core::Price::parse("0.4").unwrap(),
                    "1".parse().unwrap(),
                )]
                .into(),
                asks: [(
                    marketcow_core::Price::parse("0.6").unwrap(),
                    "1".parse().unwrap(),
                )]
                .into(),
                tick_size: Some(marketcow_core::Price::parse_tick("0.01").unwrap()),
                tick_version: "tick-v1".into(),
                source_observed_at: Some(Utc::now() - chrono::Duration::seconds(31)),
                last_trade_price: None,
                last_trade_observed_at: None,
            },
        );
        state.projection.store(Arc::new(projection));
        let response = app(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/readiness")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    #[test]
    fn equality_does_not_accept_prefixes() {
        assert!(constant_time_equal(b"abc", b"abc"));
        assert!(!constant_time_equal(b"abc", b"ab"));
    }

    #[tokio::test]
    async fn supervised_worker_command_clears_credentials_and_applies_resource_limits() {
        let dir = tempdir().unwrap();
        let script = dir.path().join("credential-check.sh");
        let secret = dir.path().join("provider-secret");
        fs::write(&secret, b"capability-secret\n").unwrap();
        fs::set_permissions(&secret, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(
            &script,
            b"#!/bin/sh\nif [ \"${MARKETCOW_POSTGRES_DSN+x}\" = x ] || [ \"${MARKETCOW_CLICKHOUSE_DSN+x}\" = x ] || [ \"${MARKETCOW_RUST_ADMIN_TOKEN+x}\" = x ]; then exit 42; fi\n[ \"${MARKETCOW_PROVIDER_SECRET_FD:-}\" = 3 ] || exit 47\nIFS= read -r provider_secret <&3 || exit 48\n[ \"$provider_secret\" = capability-secret ] || exit 49\n[ \"$(ulimit -t)\" = 7 ] || exit 43\n[ \"$(ulimit -n)\" = 256 ] || exit 45\n[ \"$(ulimit -c)\" = 0 ] || exit 46\nexit 0\n",
        )
        .unwrap();
        let config = PythonWorkerConfig {
            executable: Some(PathBuf::from("/bin/sh")),
            script: Some(script),
            revision: "credential-test".into(),
            pool_size: 1,
            max_restarts: 3,
            restart_window_seconds: 60,
            restart_backoff_millis: 100,
            memory_limit_mib: 512,
            cpu_limit_seconds: 7,
            dispatch_policies: default_dispatch_policies(),
            secret_references: BTreeMap::from([(CSV_INFERENCE_TASK.into(), secret)]),
        };
        let serialized = serde_json::to_string(&config).unwrap();
        assert!(!serialized.contains("provider-secret"));
        assert!(!serialized.contains("secret_references"));
        let mut command = ProcessCommand::new("/bin/sh");
        command
            .env("MARKETCOW_POSTGRES_DSN", "must-not-leak")
            .env("MARKETCOW_CLICKHOUSE_DSN", "must-not-leak")
            .env("MARKETCOW_RUST_ADMIN_TOKEN", "must-not-leak");
        sanitize_worker_command(
            &mut command,
            &config,
            &dir.path().join("worker.sock"),
            CSV_INFERENCE_TASK,
        );
        attach_worker_secret(&mut command, &config, CSV_INFERENCE_TASK).unwrap();
        assert!(command.status().await.unwrap().success());
    }

    #[test]
    fn worker_secret_reference_rejects_weak_permissions_and_symlinks() {
        let dir = tempdir().unwrap();
        let secret = dir.path().join("provider-secret");
        fs::write(&secret, b"secret").unwrap();
        fs::set_permissions(&secret, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(open_worker_secret(&secret).is_err());

        fs::set_permissions(&secret, fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join("provider-secret-link");
        std::os::unix::fs::symlink(&secret, &link).unwrap();
        assert!(open_worker_secret(&link).is_err());
    }

    #[tokio::test]
    async fn worker_memory_monitor_kills_and_reaps_over_limit_process() {
        let mut child = ProcessCommand::new("/bin/sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let outcome = wait_for_worker(&mut child, 0).await.unwrap();
        assert!(matches!(
            outcome,
            WorkerExit::MemoryLimitExceeded {
                resident_kib: 1..,
                limit_kib: 0
            }
        ));
        assert!(child.try_wait().unwrap().is_some());
    }

    #[test]
    fn worker_restart_budget_is_bounded_and_recovers_after_window() {
        let start = Instant::now();
        let window = Duration::from_secs(60);
        let mut restarts = VecDeque::new();
        assert_eq!(
            consume_restart_budget(&mut restarts, start, window, 2),
            None
        );
        assert_eq!(
            consume_restart_budget(&mut restarts, start + Duration::from_secs(1), window, 2),
            None
        );
        assert_eq!(
            consume_restart_budget(&mut restarts, start + Duration::from_secs(2), window, 2),
            Some(Duration::from_secs(58))
        );
        assert_eq!(
            consume_restart_budget(&mut restarts, start + window, window, 2),
            None
        );
    }

    #[test]
    fn offline_replay_seeds_durable_ready_state_without_orders() {
        let dir = tempdir().unwrap();
        let config = Config {
            profile: "test".into(),
            bind: "127.0.0.1:8870".parse().unwrap(),
            storage_root: dir.path().into(),
            wal_root: dir.path().join("wal"),
            worker_socket: dir.path().join("worker.sock"),
            scope_id: "shadow-seed".into(),
            real_order_submission_enabled: false,
            shadow_mode: true,
            maximum_book_age_ms: 30_000,
            legacy_mcp_url: None,
            python_workers: PythonWorkerConfig::disabled(),
        };
        preflight(&config).unwrap();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/polymarket-rust-shadow-seed.jsonl");
        let result = replay_file(&config, &fixture, true).unwrap();
        assert_eq!(result["input_frames"], 3);
        assert_eq!(result["canonical_events"], 4);
        assert_eq!(result["rejected_events"], 0);
        assert_eq!(result["ready"], true);
        assert_eq!(result["real_order_submission_enabled"], false);
        let revision = calculated_config_revision(&config).unwrap();
        let recovered =
            marketcow_runtime::PolymarketRuntime::open(runtime_config(&config, &revision)).unwrap();
        assert_eq!(recovered.projection().cursor, 4);
        assert!(recovered.projection().ready);
    }
}
