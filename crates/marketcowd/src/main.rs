use anyhow::{Context, Result, bail};
use arc_swap::ArcSwap;
use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Extension, Path as AxumPath, Query, Request, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket, close_code},
    },
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::Utc;
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
    os::unix::fs::PermissionsExt,
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
    sync::{Mutex as AsyncMutex, broadcast},
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

#[derive(Clone, Debug, Serialize)]
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
    python_workers: PythonWorkerConfig,
}

#[derive(Clone, Debug, Serialize)]
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
        policy
            .validate()
            .map_err(|error| anyhow::anyhow!("Python dispatch policy is invalid: {error}"))?;
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
            python_workers,
        })
    }
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
    audit: Arc<AuditLog>,
    metrics: Arc<Metrics>,
    projection: Arc<ArcSwap<marketcow_core::Projection>>,
    recent_events: Arc<ArcSwap<Vec<marketcow_core::PersistedEvent>>>,
    runtime: Arc<AsyncMutex<marketcow_runtime::PolymarketRuntime>>,
    jobs: Arc<DurableJobCoordinator>,
    stream: broadcast::Sender<marketcow_core::PersistedEvent>,
    worker_status: Arc<PythonWorkerStatus>,
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
    repository: Option<Arc<marketcow_storage::PostgresJobRepository>>,
    memory_artifacts: AsyncMutex<BTreeMap<String, marketcow_storage::ArtifactManifestRecord>>,
    dispatch_policies: BTreeMap<String, marketcow_jobs::DispatchPolicy>,
}

impl DurableJobCoordinator {
    #[cfg(test)]
    fn memory() -> Self {
        Self {
            engine: AsyncMutex::new(marketcow_jobs::JobEngine::default()),
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
}
impl AuditLog {
    fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }
    fn record(&self, value: serde_json::Value) {
        if let Ok(mut file) = self.file.lock()
            && let Ok(line) = serde_json::to_vec(&value)
        {
            let _ = file.write_all(&line);
            let _ = file.write_all(b"\n");
        }
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
    Ok(())
}

fn sanitize_worker_command(
    command: &mut ProcessCommand,
    config: &PythonWorkerConfig,
    socket: &Path,
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
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    apply_worker_resource_limits(command, config);
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

fn worker_command(config: &PythonWorkerConfig, socket: &Path) -> ProcessCommand {
    let mut command = ProcessCommand::new(
        config
            .executable
            .as_ref()
            .expect("enabled worker has executable"),
    );
    sanitize_worker_command(&mut command, config, socket);
    command
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
        let mut command = worker_command(&config, &socket);
        match command.spawn() {
            Ok(mut child) => {
                status.live.fetch_add(1, Ordering::Relaxed);
                info!(slot, pid=?child.id(), "python_worker_started");
                let result = wait_for_worker(&mut child, config.memory_limit_mib).await;
                status.live.fetch_sub(1, Ordering::Relaxed);
                match result {
                    Ok(WorkerExit::Exited(exit_status)) => {
                        warn!(slot, status=?exit_status, "python_worker_exited");
                    }
                    Ok(WorkerExit::MemoryLimitExceeded {
                        resident_kib,
                        limit_kib,
                    }) => {
                        status.memory_limit_kills.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            slot,
                            resident_kib, limit_kib, "python_worker_memory_limit_exceeded"
                        );
                    }
                    Err(error) => {
                        status
                            .memory_monitor_failures
                            .fetch_add(1, Ordering::Relaxed);
                        warn!(slot, error=%error, "python_worker_memory_monitor_failed_closed");
                    }
                }
            }
            Err(error) => warn!(slot, error=%error, "python_worker_spawn_failed"),
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
    for slot in 0..config.pool_size {
        slots.spawn(supervise_worker_slot(
            slot,
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
    let audit = Arc::new(AuditLog::open(&config.storage_root.join("audit.jsonl"))?);
    let runtime = marketcow_runtime::PolymarketRuntime::open(runtime_config(&config))?;
    let projection = runtime.projection();
    let recent_events = runtime.recent_events().to_vec();
    let jobs = Arc::new(
        DurableJobCoordinator::open(
            &config.profile,
            config.python_workers.dispatch_policies.clone(),
        )
        .await?,
    );
    let (stream, _) = broadcast::channel(STREAM_CHANNEL_CAPACITY);
    let state = AppState {
        config: config.clone(),
        audit,
        metrics: Arc::new(Metrics::default()),
        projection: Arc::new(ArcSwap::from(projection)),
        recent_events: Arc::new(ArcSwap::from_pointee(recent_events)),
        runtime: Arc::new(AsyncMutex::new(runtime)),
        jobs,
        stream,
        worker_status: Arc::new(PythonWorkerStatus::default()),
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

fn runtime_config(config: &Config) -> marketcow_runtime::RuntimeConfig {
    marketcow_runtime::RuntimeConfig {
        root: config.storage_root.join("polymarket"),
        scope_id: config.scope_id.clone(),
        config_revision: "marketcowd-config-v1".into(),
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

    let mut runtime = marketcow_runtime::PolymarketRuntime::open(runtime_config(config))?;
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
    let mut runtime = marketcow_runtime::PolymarketRuntime::open(runtime_config(config))?;
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
        .route("/metrics", get(metrics))
        .route("/v1/admin/migration", get(admin_migration))
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

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    let configured_workers = state.config.python_workers.pool_size as u64;
    let live_workers = state.worker_status.live.load(Ordering::Relaxed);
    let worker_health = if configured_workers == 0 {
        "disabled_optional"
    } else if live_workers == configured_workers {
        "healthy"
    } else {
        "degraded"
    };
    Json(json!({
        "status":"healthy", "service":"marketcowd", "profile":state.config.profile,
        "shadow_mode":true, "real_order_submission_enabled":false,
        "components":{
            "api":"healthy","wal":"healthy","python_workers":worker_health,
            "job_persistence":if state.jobs.persistence_enabled() { "healthy" } else { "degraded_development_only" }
        },
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
    }))
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

async fn admin_migration() -> Json<serde_json::Value> {
    Json(json!({
        "phase":"shadow", "cutover_allowed":false, "tradude_may_manage_marketcow":false
    }))
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
        .filter(|x| x.len() <= 128)
        .map(str::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let path = request.uri().path().to_owned();
    if path.starts_with("/v1/admin/") {
        let expected = env::var("MARKETCOW_RUST_ADMIN_TOKEN").unwrap_or_default();
        let supplied = request
            .headers()
            .get("authorization")
            .and_then(|x| x.to_str().ok())
            .and_then(|x| x.strip_prefix("Bearer "))
            .unwrap_or("");
        if expected.is_empty() || !constant_time_equal(expected.as_bytes(), supplied.as_bytes()) {
            return error(
                StatusCode::UNAUTHORIZED,
                "authentication_required",
                false,
                &request_id,
            );
        }
    }
    request.extensions_mut().insert(request_id.clone());
    let started = std::time::Instant::now();
    let mut response = next.run(request).await;
    state.metrics.requests.fetch_add(1, Ordering::Relaxed);
    if response.status().is_client_error() || response.status().is_server_error() {
        state.metrics.errors.fetch_add(1, Ordering::Relaxed);
    }
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", value);
    }
    state
        .audit
        .record(json!({"at":Utc::now(),"request_id":request_id,"path":path,
        "status":response.status().as_u16(),"elapsed_us":started.elapsed().as_micros()}));
    response
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
        let audit = Arc::new(AuditLog::open(&dir.path().join("audit.jsonl")).unwrap());
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
                stream,
                worker_status: Arc::new(PythonWorkerStatus::default()),
            },
        )
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
        let (_dir, state) = test_state();
        let response = app(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/admin/migration")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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
        fs::write(
            &script,
            b"#!/bin/sh\nif [ \"${MARKETCOW_POSTGRES_DSN+x}\" = x ] || [ \"${MARKETCOW_CLICKHOUSE_DSN+x}\" = x ] || [ \"${MARKETCOW_RUST_ADMIN_TOKEN+x}\" = x ]; then exit 42; fi\n[ \"$(ulimit -t)\" = 7 ] || exit 43\n[ \"$(ulimit -n)\" = 256 ] || exit 45\n[ \"$(ulimit -c)\" = 0 ] || exit 46\nexit 0\n",
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
        };
        let mut command = ProcessCommand::new("/bin/sh");
        command
            .env("MARKETCOW_POSTGRES_DSN", "must-not-leak")
            .env("MARKETCOW_CLICKHOUSE_DSN", "must-not-leak")
            .env("MARKETCOW_RUST_ADMIN_TOKEN", "must-not-leak");
        sanitize_worker_command(&mut command, &config, &dir.path().join("worker.sock"));
        assert!(command.status().await.unwrap().success());
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
        let recovered =
            marketcow_runtime::PolymarketRuntime::open(runtime_config(&config)).unwrap();
        assert_eq!(recovered.projection().cursor, 4);
        assert!(recovered.projection().ready);
    }
}
