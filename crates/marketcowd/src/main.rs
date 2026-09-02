use anyhow::{Context, Result, bail};
use arc_swap::{ArcSwap, ArcSwapOption};
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
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    env,
    fs::{self, File, OpenOptions},
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
    sync::{Mutex as AsyncMutex, Notify, broadcast, mpsc, watch},
    task::JoinSet,
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

// The public broadcast ring stores full audited events, including raw venue evidence. Keep roughly
// one minute at the observed 100-market rate; a lag beyond this verified in-memory window replays
// from the runtime journal or requires full-sync instead of allowing unbounded payload retention.
const STREAM_CHANNEL_CAPACITY: usize = 4_096;
// A 200-token scope may receive a full-book burst plus incremental traffic during bootstrap. The
// transport awaits this bounded queue, applying TCP backpressure without dropping or reconnecting
// merely because durable storage is briefly slower than the venue.
const POLYMARKET_TRANSPORT_CHANNEL_CAPACITY: usize = 2_048;
const POLYMARKET_RECENT_EVENT_CAPACITY: usize = 5_000;
const POLYMARKET_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);
const POLYMARKET_TRANSPORT_RESTART_DELAY: Duration = Duration::from_secs(1);
const POLYMARKET_TRANSPORT_SCOPE_SWITCH_STOP_TIMEOUT: Duration = Duration::from_secs(2);
const POLYMARKET_BOOK_REFRESH_MAX_INTERVAL: Duration = Duration::from_secs(5 * 60);
const POLYMARKET_BOOK_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);
const POLYMARKET_BOOK_REFRESH_MAX_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;
const HYPERLIQUID_TRANSPORT_CHANNEL_CAPACITY: usize = 64;
const HYPERLIQUID_GATEWAY_CHANNEL_CAPACITY: usize = 4_096;
const HYPERLIQUID_PUBLIC_CHANNEL_CAPACITY: usize = 4_096;
const HYPERLIQUID_REPLAY_CAPACITY: usize = 10_000;
const HYPERLIQUID_WAL_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;
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
    Verify {
        path: PathBuf,
    },
    AnchorLegacyCheckpoint {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        scope_id: String,
        #[arg(long)]
        expected_manifest_sha256: String,
        #[arg(long, default_value_t = 256 * 1024 * 1024)]
        wal_segment_bytes: u64,
    },
}

#[derive(Clone, Serialize)]
struct Config {
    profile: String,
    role: String,
    bind: SocketAddr,
    storage_root: PathBuf,
    wal_root: PathBuf,
    worker_socket: PathBuf,
    scope_id: String,
    real_order_submission_enabled: bool,
    shadow_mode: bool,
    maximum_book_age_ms: u64,
    legacy_mcp_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    polymarket_live: Option<PolymarketLiveConfig>,
    hyperliquid_shadow: Option<HyperliquidShadowConfig>,
    python_workers: PythonWorkerConfig,
}

#[derive(Clone, Serialize)]
struct PolymarketLiveConfig {
    scope_id: String,
    token_ids: Vec<String>,
    market_ids: Vec<String>,
    market_count: Option<usize>,
    catalog_revision: Option<String>,
    catalog_frame: Option<serde_json::Value>,
    scope_file_sha256: Option<String>,
    #[serde(skip_serializing)]
    scope_file_path: Option<PathBuf>,
    source_manifest_sha256: Option<String>,
    catalog_index_sha256: Option<String>,
    catalog_sha256: Option<String>,
    registry_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    universe: Option<PolymarketUniverseContract>,
    #[serde(skip_serializing)]
    initial_book_frames: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct PolymarketLiveScopeFile {
    schema_version: String,
    scope_id: String,
    market_count: usize,
    token_count: usize,
    market_ids: Vec<String>,
    token_ids: Vec<String>,
    catalog_revision: String,
    catalog_frame: serde_json::Value,
    source: PolymarketLiveScopeSource,
    #[serde(default)]
    universe: Option<PolymarketUniverseContract>,
    #[serde(default)]
    initial_book_frames: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PolymarketUniverseContract {
    schema_version: String,
    universe_id: String,
    generation: u64,
    target_market_count: usize,
    minimum_market_count: usize,
    filters: PolymarketUniverseFilters,
    active_markets: Vec<PolymarketUniverseMarket>,
    added_markets: Vec<String>,
    removed_markets: Vec<String>,
    added_market_identities: Vec<PolymarketUniverseMarket>,
    removed_market_identities: Vec<PolymarketUniverseMarket>,
    excluded_markets: Vec<PolymarketUniverseExclusion>,
    validated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PolymarketUniverseFilters {
    require_two_sided_books: bool,
    require_complete_instrument_facts: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PolymarketUniverseMarket {
    market_id: String,
    condition_id: String,
    token_ids: Vec<String>,
    end_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PolymarketUniverseExclusion {
    market_id: String,
    reason_code: String,
    retryable: bool,
    retry_after: Option<DateTime<Utc>>,
    observed_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct PolymarketLiveScopeSource {
    manifest_sha256: String,
    catalog_index_sha256: String,
    catalog_sha256: String,
    registry_sha256: String,
}

#[derive(Deserialize)]
struct PolymarketCatalogFrame {
    event_type: String,
    catalog_revision: String,
    markets: Vec<marketcow_core::MarketRecord>,
    #[serde(default)]
    negative_risk_relations: Vec<marketcow_core::NegativeRiskRelation>,
}

#[derive(Clone, Serialize)]
struct HyperliquidShadowConfig {
    instruments: BTreeMap<String, String>,
    maximum_source_delay_millis: i64,
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
        let role = env::var("MARKETCOW_RUST_ROLE").unwrap_or_else(|_| "public_gateway".into());
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
        let polymarket_live = load_polymarket_live_config(&scope_id, &storage_root)?;
        let hyperliquid_shadow = load_hyperliquid_shadow_config()?;
        let python_workers = PythonWorkerConfig::load()?;
        if orders {
            bail!("real order submission is prohibited by the migration safety gate");
        }
        if !matches!(role.as_str(), "public_gateway" | "polymarket_data_plane") {
            bail!("MARKETCOW_RUST_ROLE must be public_gateway or polymarket_data_plane");
        }
        if profile == "production" && role == "public_gateway" && bind.port() != 8790 {
            bail!("production public gateway must bind port 8790");
        }
        if profile == "production" && role == "polymarket_data_plane" && bind.port() == 8790 {
            bail!("production Polymarket data plane must use an internal non-public port");
        }
        if profile != "production" && bind.port() == 8790 {
            bail!("non-production may not bind production port 8790");
        }
        if !bind.ip().is_loopback()
            && env::var("MARKETCOW_ALLOW_PUBLIC_BIND").as_deref() != Ok("confirmed")
        {
            bail!("non-loopback bind requires explicit MARKETCOW_ALLOW_PUBLIC_BIND=confirmed");
        }
        if !shadow_mode && role != "polymarket_data_plane" {
            bail!("authoritative mode is restricted to the Polymarket data-plane role");
        }
        if role == "polymarket_data_plane" && polymarket_live.is_none() {
            bail!("Polymarket data-plane role requires live configuration");
        }
        if role == "polymarket_data_plane"
            && (legacy_mcp_url.is_some() || hyperliquid_shadow.is_some())
        {
            bail!("Polymarket data plane cannot own legacy MCP or Hyperliquid transports");
        }
        if maximum_book_age_ms == 0 {
            bail!("MARKETCOW_RUST_MAX_BOOK_AGE_MS must be positive");
        }
        Ok(Self {
            profile,
            role,
            bind,
            storage_root,
            wal_root,
            worker_socket,
            scope_id,
            real_order_submission_enabled: false,
            shadow_mode,
            maximum_book_age_ms,
            legacy_mcp_url,
            polymarket_live,
            hyperliquid_shadow,
            python_workers,
        })
    }
}

fn load_polymarket_live_config(
    expected_scope_id: &str,
    storage_root: &Path,
) -> Result<Option<PolymarketLiveConfig>> {
    let configured = load_polymarket_live_config_from_env(expected_scope_id)?;
    if configured.is_none() {
        return Ok(None);
    }
    if env::var("MARKETCOW_POLYMARKET_IGNORE_PERSISTED_UNIVERSE").as_deref() == Ok("true") {
        return Ok(configured);
    }
    let persisted_path = storage_root
        .join("polymarket-universe-control")
        .join("active-scope.json");
    if !persisted_path.exists() {
        return Ok(configured);
    }
    let persisted =
        load_polymarket_scope_file(expected_scope_id, &persisted_path.to_string_lossy())?
            .context("persisted Polymarket universe is empty")?;
    let persisted_generation = persisted
        .universe
        .as_ref()
        .context("persisted Polymarket activation is not a dynamic universe")?
        .generation;
    let configured_generation = configured
        .as_ref()
        .and_then(|live| live.universe.as_ref())
        .map_or(0, |universe| universe.generation);
    if persisted_generation < configured_generation {
        bail!("persisted Polymarket universe generation regressed behind configured generation");
    }
    Ok(Some(persisted))
}

fn load_polymarket_live_config_from_env(
    expected_scope_id: &str,
) -> Result<Option<PolymarketLiveConfig>> {
    if env::var("MARKETCOW_POLYMARKET_LIVE_ENABLED").as_deref() != Ok("true") {
        return Ok(None);
    }
    let encoded = env::var("MARKETCOW_POLYMARKET_TOKEN_IDS_JSON").ok();
    let scope_path = env::var("MARKETCOW_POLYMARKET_SCOPE_FILE").ok();
    match (encoded, scope_path) {
        (Some(_), Some(_)) => bail!(
            "Polymarket live requires exactly one of MARKETCOW_POLYMARKET_TOKEN_IDS_JSON or MARKETCOW_POLYMARKET_SCOPE_FILE"
        ),
        (Some(encoded), None) => {
            let token_ids: Vec<String> = serde_json::from_str(&encoded)
                .context("MARKETCOW_POLYMARKET_TOKEN_IDS_JSON must be a JSON array")?;
            Ok(Some(PolymarketLiveConfig {
                scope_id: expected_scope_id.to_owned(),
                token_ids: validate_polymarket_token_ids(token_ids)?,
                market_ids: Vec::new(),
                market_count: None,
                catalog_revision: None,
                catalog_frame: None,
                scope_file_sha256: None,
                scope_file_path: None,
                source_manifest_sha256: None,
                catalog_index_sha256: None,
                catalog_sha256: None,
                registry_sha256: None,
                universe: None,
                initial_book_frames: Vec::new(),
            }))
        }
        (None, Some(scope_path)) => load_polymarket_scope_file(expected_scope_id, &scope_path),
        (None, None) => bail!(
            "enabled Polymarket live requires MARKETCOW_POLYMARKET_TOKEN_IDS_JSON or MARKETCOW_POLYMARKET_SCOPE_FILE"
        ),
    }
}

fn validate_polymarket_token_ids(token_ids: Vec<String>) -> Result<Vec<String>> {
    let unique = token_ids.iter().cloned().collect::<BTreeSet<_>>();
    if token_ids.is_empty()
        || token_ids.len() > 500
        || unique.len() != token_ids.len()
        || token_ids.iter().any(|token| {
            token.is_empty() || token.len() > 128 || !token.bytes().all(|b| b.is_ascii_digit())
        })
    {
        bail!("Polymarket token scope must contain 1 to 500 unique decimal token identifiers");
    }
    Ok(unique.into_iter().collect())
}

fn load_polymarket_scope_file(
    expected_scope_id: &str,
    scope_path: &str,
) -> Result<Option<PolymarketLiveConfig>> {
    load_polymarket_scope_file_at(expected_scope_id, scope_path, Utc::now())
}

fn load_polymarket_scope_file_at(
    expected_scope_id: &str,
    scope_path: &str,
    activated_at: chrono::DateTime<Utc>,
) -> Result<Option<PolymarketLiveConfig>> {
    let path = Path::new(scope_path);
    if !path.is_absolute() {
        bail!("MARKETCOW_POLYMARKET_SCOPE_FILE must be an absolute path");
    }
    let encoded = fs::read(path).context("failed to read MARKETCOW_POLYMARKET_SCOPE_FILE")?;
    let scope_file_sha256 = hex::encode(Sha256::digest(&encoded));
    let scope: PolymarketLiveScopeFile = serde_json::from_slice(&encoded)
        .context("MARKETCOW_POLYMARKET_SCOPE_FILE must be valid JSON")?;
    let dynamic_universe = scope.schema_version == "marketcow.polymarket.rust-live-scope.v4";
    if !dynamic_universe && scope.schema_version != "marketcow.polymarket.rust-live-scope.v3" {
        bail!("unsupported Polymarket Rust live scope schema");
    }
    if scope.scope_id != expected_scope_id {
        bail!("Polymarket Rust live scope does not match MARKETCOW_RUST_SCOPE_ID");
    }
    let markets = scope.market_ids.iter().cloned().collect::<BTreeSet<_>>();
    if scope.market_count == 0
        || scope.market_count > 250
        || scope.market_count != scope.market_ids.len()
        || markets.len() != scope.market_ids.len()
        || scope.market_ids.iter().any(|market| {
            market.is_empty() || market.len() > 32 || !market.bytes().all(|b| b.is_ascii_digit())
        })
    {
        bail!("Polymarket Rust live scope market identity/count is invalid");
    }
    if scope.token_count != scope.token_ids.len()
        || scope.token_count != scope.market_count.saturating_mul(2)
    {
        bail!("Polymarket Rust live scope must contain exactly two tokens per market");
    }
    let token_ids = validate_polymarket_token_ids(scope.token_ids)?;
    let catalog: PolymarketCatalogFrame = serde_json::from_value(scope.catalog_frame.clone())
        .context("Polymarket scope catalog_frame is invalid")?;
    let catalog_markets = catalog
        .markets
        .iter()
        .map(|market| market.market_id.clone())
        .collect::<BTreeSet<_>>();
    let catalog_tokens = catalog
        .markets
        .iter()
        .flat_map(|market| {
            market
                .outcomes
                .iter()
                .map(|outcome| outcome.token_id.clone())
        })
        .collect::<BTreeSet<_>>();
    if catalog.event_type != "catalog_revision"
        || catalog.catalog_revision != scope.catalog_revision
        || catalog_markets != markets
        || catalog_tokens != token_ids.iter().cloned().collect()
        || catalog.markets.iter().any(|market| {
            market.instrument_facts.as_ref().is_none_or(|facts| {
                facts
                    .fee_schedule
                    .as_ref()
                    .is_none_or(|schedule| !schedule.is_complete())
            })
        })
    {
        bail!("Polymarket scope catalog frame does not exactly cover the declared scope");
    }
    let expired_or_inactive = catalog
        .markets
        .iter()
        .filter(|market| {
            market.lifecycle_state != marketcow_core::MarketLifecycleState::Active
                || (!dynamic_universe
                    && market
                        .instrument_facts
                        .as_ref()
                        .is_none_or(|facts| facts.end_at <= activated_at))
        })
        .map(|market| market.market_id.clone())
        .collect::<Vec<_>>();
    if !expired_or_inactive.is_empty() {
        bail!(
            "Polymarket live scope contains expired or inactive markets: {}",
            expired_or_inactive.join(",")
        );
    }
    let relation_markets = catalog
        .negative_risk_relations
        .iter()
        .flat_map(|relation| relation.member_market_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    if !relation_markets.is_subset(&markets) {
        bail!("Polymarket scope relation crosses the declared scope boundary");
    }
    if dynamic_universe {
        let universe = scope
            .universe
            .as_ref()
            .context("dynamic Polymarket scope requires universe contract")?;
        validate_polymarket_universe_contract(
            universe,
            &scope.scope_id,
            &catalog,
            &token_ids,
            &scope.initial_book_frames,
            activated_at,
        )?;
    } else if scope.universe.is_some() || !scope.initial_book_frames.is_empty() {
        bail!("legacy Polymarket scope cannot carry dynamic universe fields");
    }
    for digest in [
        &scope.source.manifest_sha256,
        &scope.source.catalog_index_sha256,
        &scope.source.catalog_sha256,
        &scope.source.registry_sha256,
    ] {
        if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
            bail!("Polymarket Rust live scope source hashes must be SHA-256 hex digests");
        }
    }
    Ok(Some(PolymarketLiveConfig {
        scope_id: scope.scope_id,
        token_ids,
        market_ids: markets.into_iter().collect(),
        market_count: Some(scope.market_count),
        catalog_revision: Some(scope.catalog_revision),
        catalog_frame: Some(scope.catalog_frame),
        scope_file_sha256: Some(scope_file_sha256),
        scope_file_path: Some(path.to_path_buf()),
        source_manifest_sha256: Some(scope.source.manifest_sha256.to_ascii_lowercase()),
        catalog_index_sha256: Some(scope.source.catalog_index_sha256.to_ascii_lowercase()),
        catalog_sha256: Some(scope.source.catalog_sha256.to_ascii_lowercase()),
        registry_sha256: Some(scope.source.registry_sha256.to_ascii_lowercase()),
        universe: scope.universe,
        initial_book_frames: scope.initial_book_frames,
    }))
}

fn validate_polymarket_universe_contract(
    universe: &PolymarketUniverseContract,
    scope_id: &str,
    catalog: &PolymarketCatalogFrame,
    token_ids: &[String],
    initial_book_frames: &[serde_json::Value],
    activated_at: DateTime<Utc>,
) -> Result<()> {
    if universe.schema_version != "marketcow.polymarket.universe.v2"
        || universe.universe_id != scope_id
        || universe.universe_id.len() != 64
        || !universe
            .universe_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || universe.generation == 0
        || universe.minimum_market_count == 0
        || universe.minimum_market_count > universe.target_market_count
        || universe.target_market_count > 250
        || universe.active_markets.len() < universe.minimum_market_count
        || universe.active_markets.len() > universe.target_market_count
        || !universe.filters.require_two_sided_books
        || !universe.filters.require_complete_instrument_facts
        || universe.validated_at > activated_at
    {
        bail!("dynamic Polymarket universe contract is invalid");
    }
    let catalog_by_market = catalog
        .markets
        .iter()
        .map(|market| (market.market_id.as_str(), market))
        .collect::<BTreeMap<_, _>>();
    let active_ids = universe
        .active_markets
        .iter()
        .map(|market| market.market_id.clone())
        .collect::<BTreeSet<_>>();
    if active_ids.len() != universe.active_markets.len()
        || active_ids
            != catalog_by_market
                .keys()
                .map(|value| (*value).to_owned())
                .collect()
    {
        bail!("dynamic universe active markets do not exactly match its catalog");
    }
    for active in &universe.active_markets {
        let market = catalog_by_market[active.market_id.as_str()];
        let expected_tokens = market
            .outcomes
            .iter()
            .map(|outcome| outcome.token_id.clone())
            .collect::<BTreeSet<_>>();
        let declared_tokens = active.token_ids.iter().cloned().collect::<BTreeSet<_>>();
        let facts = market
            .instrument_facts
            .as_ref()
            .context("dynamic universe market lacks instrument facts")?;
        if active.condition_id != market.condition_id
            || active.token_ids.len() != 2
            || declared_tokens != expected_tokens
            || active.end_at != facts.end_at
            || active.end_at <= universe.validated_at
        {
            bail!("dynamic universe market identity or lifecycle boundary is invalid");
        }
    }
    let added = universe
        .added_markets
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let removed = universe
        .removed_markets
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if added.len() != universe.added_markets.len()
        || removed.len() != universe.removed_markets.len()
        || !added.is_subset(&active_ids)
        || !added.is_disjoint(&removed)
    {
        bail!("dynamic universe membership delta is invalid");
    }
    let added_identities = universe
        .added_market_identities
        .iter()
        .map(|market| market.market_id.clone())
        .collect::<BTreeSet<_>>();
    let removed_identities = universe
        .removed_market_identities
        .iter()
        .map(|market| market.market_id.clone())
        .collect::<BTreeSet<_>>();
    if added_identities != added
        || removed_identities != removed
        || universe.added_market_identities.iter().any(|identity| {
            universe
                .active_markets
                .iter()
                .find(|active| active.market_id == identity.market_id)
                != Some(identity)
        })
        || universe.removed_market_identities.iter().any(|identity| {
            identity.condition_id.is_empty()
                || identity.token_ids.len() != 2
                || identity.token_ids.iter().collect::<BTreeSet<_>>().len() != 2
        })
    {
        bail!("dynamic universe membership identities are incomplete");
    }
    const EXCLUSION_REASONS: &[&str] = &[
        "market_expired",
        "market_not_found",
        "one_sided_book",
        "token_missing",
        "book_missing",
        "instrument_facts_missing",
        "instrument_facts_invalid",
        "fee_facts_unavailable",
    ];
    let mut excluded_ids = BTreeSet::new();
    for excluded in &universe.excluded_markets {
        if excluded.market_id.is_empty()
            || active_ids.contains(&excluded.market_id)
            || !excluded_ids.insert(excluded.market_id.clone())
            || !EXCLUSION_REASONS.contains(&excluded.reason_code.as_str())
            || excluded.retryable != excluded.retry_after.is_some()
            || excluded
                .retry_after
                .is_some_and(|retry_after| retry_after <= excluded.observed_at)
        {
            bail!("dynamic universe exclusion is invalid");
        }
    }
    let expected_tokens = token_ids.iter().cloned().collect::<BTreeSet<_>>();
    let mut book_tokens = BTreeSet::new();
    for frame in initial_book_frames {
        let token_id = frame
            .get("asset_id")
            .or_else(|| frame.get("token_id"))
            .and_then(serde_json::Value::as_str)
            .context("dynamic universe initial book lacks token identity")?;
        let bids = frame.get("bids").and_then(serde_json::Value::as_array);
        let asks = frame.get("asks").and_then(serde_json::Value::as_array);
        if frame.get("event_type").and_then(serde_json::Value::as_str) != Some("book")
            || frame
                .get("tick_size")
                .and_then(serde_json::Value::as_str)
                .is_none()
            || bids.is_none_or(Vec::is_empty)
            || asks.is_none_or(Vec::is_empty)
            || !book_tokens.insert(token_id.to_owned())
        {
            bail!("dynamic universe initial books must be unique and two-sided");
        }
    }
    if book_tokens != expected_tokens {
        bail!("dynamic universe initial books do not exactly cover active tokens");
    }
    Ok(())
}

fn load_hyperliquid_shadow_config() -> Result<Option<HyperliquidShadowConfig>> {
    if env::var("MARKETCOW_HYPERLIQUID_SHADOW_ENABLED").as_deref() != Ok("true") {
        return Ok(None);
    }
    let encoded = env::var("MARKETCOW_HYPERLIQUID_INSTRUMENTS_JSON")
        .context("enabled Hyperliquid shadow requires MARKETCOW_HYPERLIQUID_INSTRUMENTS_JSON")?;
    let instruments: BTreeMap<String, String> = serde_json::from_str(&encoded)
        .context("MARKETCOW_HYPERLIQUID_INSTRUMENTS_JSON must be a JSON object")?;
    let maximum_source_delay_millis = env::var("MARKETCOW_HYPERLIQUID_MAX_SOURCE_DELAY_MILLIS")
        .unwrap_or_else(|_| "30000".into())
        .parse::<i64>()?;
    // Both constructors validate duplicate normalized coins, identifiers, and delay bounds. Build
    // the subscription set here so an invalid scope fails before any public listener is bound.
    marketcow_realtime::HyperliquidNormalizer::new(
        instruments.clone(),
        maximum_source_delay_millis,
    )?;
    marketcow_realtime::hyperliquid_subscriptions(
        &instruments,
        &BTreeSet::from([
            marketcow_realtime::DataType::Quote,
            marketcow_realtime::DataType::OrderBook,
            marketcow_realtime::DataType::Trade,
            marketcow_realtime::DataType::AssetContext,
        ]),
    )?;
    Ok(Some(HyperliquidShadowConfig {
        instruments,
        maximum_source_delay_millis,
    }))
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
    runtime: Arc<AsyncMutex<marketcow_runtime::PolymarketRuntime>>,
    active_polymarket_scope: Arc<ArcSwapOption<PolymarketLiveConfig>>,
    polymarket_book_validations: Arc<ArcSwap<PolymarketBookValidationProjection>>,
    polymarket_scope_switch: Option<mpsc::Sender<PolymarketScopeSwitchRequest>>,
    jobs: Arc<DurableJobCoordinator>,
    instruments: Arc<InstrumentCoordinator>,
    market_data: Arc<MarketDataCoordinator>,
    canonical_cursor: Arc<CanonicalCursorSigner>,
    control_plane: Arc<ControlPlaneCoordinator>,
    stream: broadcast::Sender<marketcow_core::PersistedEvent>,
    hyperliquid_shadow: Option<marketcow_realtime::RealtimeHubReader>,
    hyperliquid_stream: Option<broadcast::Sender<marketcow_realtime::StreamEvent>>,
    hyperliquid_queues: Option<HyperliquidQueueProbe>,
    worker_status: Arc<PythonWorkerStatus>,
    legacy_mcp: Option<LegacyMcpProxy>,
}

#[derive(Clone, Default)]
struct PolymarketBookValidationProjection {
    scope_id: String,
    verified_at: BTreeMap<String, DateTime<Utc>>,
}

struct PolymarketBookValidationSummary {
    matched_books: usize,
    mismatched_books: usize,
    missing_books: usize,
    quarantine_token_ids: Vec<String>,
}

struct PolymarketBookRefreshBatch {
    frames: Vec<marketcow_polymarket::RawTransportFrame>,
    missing_token_ids: Vec<String>,
}

struct PolymarketScopeSwitchRequest {
    live: PolymarketLiveConfig,
    response: tokio::sync::oneshot::Sender<Result<PolymarketScopeSwitchReceipt, String>>,
}

#[derive(Serialize)]
struct PolymarketScopeSwitchReceipt {
    previous_scope_id: String,
    active_scope_id: String,
    boundary_cursor: u64,
    scope_file_sha256: String,
    universe_id: Option<String>,
    previous_generation: Option<u64>,
    active_generation: Option<u64>,
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
    polymarket_market_quarantines: AtomicU64,
    polymarket_market_recovery_started: AtomicU64,
    polymarket_market_recovered: AtomicU64,
    polymarket_wal_replay_attempts: AtomicU64,
    polymarket_wal_replay_successes: AtomicU64,
    polymarket_wal_replay_failures: AtomicU64,
    polymarket_global_resyncs: AtomicU64,
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

struct MarketDataCoordinator {
    repository: Option<Arc<marketcow_storage::ClickHouseQuoteRepository>>,
    #[cfg(test)]
    memory_quotes: AsyncMutex<BTreeMap<String, serde_json::Value>>,
    #[cfg(test)]
    memory_bars: AsyncMutex<Vec<marketcow_storage::CanonicalBarRecord>>,
    #[cfg(test)]
    memory_enabled: bool,
}

impl MarketDataCoordinator {
    async fn open(profile: &str) -> Result<Self> {
        let Some(database) = env::var("MARKETCOW_CLICKHOUSE_DATABASE").ok() else {
            if profile == "production" {
                bail!("MARKETCOW_CLICKHOUSE_DATABASE is required in production");
            }
            return Ok(Self {
                repository: None,
                #[cfg(test)]
                memory_quotes: AsyncMutex::new(BTreeMap::new()),
                #[cfg(test)]
                memory_bars: AsyncMutex::new(Vec::new()),
                #[cfg(test)]
                memory_enabled: false,
            });
        };
        let host = env::var("MARKETCOW_CLICKHOUSE_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        let port = env::var("MARKETCOW_CLICKHOUSE_PORT")
            .unwrap_or_else(|_| "8123".into())
            .parse::<u16>()?;
        let scheme = if env::var("MARKETCOW_CLICKHOUSE_SECURE").as_deref() == Ok("true") {
            "https"
        } else {
            "http"
        };
        let config = marketcow_storage::ClickHouseConfig::new(
            format!("{scheme}://{host}:{port}"),
            database,
            env::var("MARKETCOW_CLICKHOUSE_USERNAME").unwrap_or_else(|_| "default".into()),
            env::var("MARKETCOW_CLICKHOUSE_PASSWORD").unwrap_or_default(),
        )
        .map_err(|error| anyhow::anyhow!(error))?;
        let repository = Arc::new(marketcow_storage::ClickHouseQuoteRepository::new(config));
        repository
            .health_probe()
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
        let binary_commit = env::var("MARKETCOW_BINARY_COMMIT").unwrap_or_default();
        if binary_commit.is_empty() {
            bail!("MARKETCOW_BINARY_COMMIT is required when ClickHouse market data is enabled");
        }
        repository
            .migrate_safe_forward(&binary_commit)
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
        Ok(Self {
            repository: Some(repository),
            #[cfg(test)]
            memory_quotes: AsyncMutex::new(BTreeMap::new()),
            #[cfg(test)]
            memory_bars: AsyncMutex::new(Vec::new()),
            #[cfg(test)]
            memory_enabled: false,
        })
    }

    #[cfg(test)]
    fn memory() -> Self {
        Self {
            repository: None,
            memory_quotes: AsyncMutex::new(BTreeMap::new()),
            memory_bars: AsyncMutex::new(Vec::new()),
            memory_enabled: true,
        }
    }

    fn persistence_enabled(&self) -> bool {
        self.repository.is_some()
    }

    async fn latest_payloads(
        &self,
        symbols: &[String],
    ) -> std::result::Result<BTreeMap<String, serde_json::Value>, marketcow_storage::RepositoryError>
    {
        if let Some(repository) = &self.repository {
            return repository.latest_payloads(symbols).await;
        }
        #[cfg(test)]
        if self.memory_enabled {
            let memory = self.memory_quotes.lock().await;
            return Ok(symbols
                .iter()
                .filter_map(|symbol| memory.get(symbol).cloned().map(|row| (symbol.clone(), row)))
                .collect());
        }
        Err(marketcow_storage::RepositoryError::Unavailable)
    }

    async fn canonical_page(
        &self,
        query: &marketcow_storage::CanonicalPageQuery,
    ) -> std::result::Result<
        (Vec<marketcow_storage::CanonicalBarRecord>, bool),
        marketcow_storage::RepositoryError,
    > {
        if let Some(repository) = &self.repository {
            return repository.canonical_page(query).await;
        }
        #[cfg(test)]
        if self.memory_enabled {
            let mut rows = self
                .memory_bars
                .lock()
                .await
                .iter()
                .filter(|record| {
                    record.symbol == query.symbol
                        && record.interval == query.interval
                        && record.adjustment == query.adjustment
                        && record.bar_time >= query.start
                        && record.bar_time <= query.end
                        && query.after.is_none_or(|after| record.bar_time > after)
                })
                .cloned()
                .collect::<Vec<_>>();
            rows.sort_by_key(|record| record.bar_time);
            let has_more = rows.len() > query.page_size as usize;
            rows.truncate(query.page_size as usize);
            return Ok((rows, has_more));
        }
        Err(marketcow_storage::RepositoryError::Unavailable)
    }

    async fn canonical_identity(
        &self,
        query: &marketcow_storage::CanonicalPageQuery,
    ) -> std::result::Result<
        marketcow_storage::CanonicalDatasetIdentity,
        marketcow_storage::RepositoryError,
    > {
        if let Some(repository) = &self.repository {
            return repository.canonical_identity(query).await;
        }
        #[cfg(test)]
        if self.memory_enabled {
            let mut rows = self
                .memory_bars
                .lock()
                .await
                .iter()
                .filter(|record| {
                    record.symbol == query.symbol
                        && record.interval == query.interval
                        && record.adjustment == query.adjustment
                        && record.bar_time >= query.start
                        && record.bar_time <= query.end
                })
                .cloned()
                .collect::<Vec<_>>();
            rows.sort_by_key(|record| record.bar_time);
            let max_ingested_millis = rows
                .iter()
                .map(|record| record.ingested_at.timestamp_millis())
                .max()
                .unwrap_or(0);
            let content_hash = format!(
                "sha256:{}",
                hex::encode(Sha256::digest(
                    serde_json::to_vec(&rows)
                        .map_err(|_| marketcow_storage::RepositoryError::Unavailable)?
                ))
            );
            let identity_payload = serde_json::to_vec(&json!({
                "symbol":query.symbol,
                "interval":query.interval,
                "adjustment":query.adjustment,
                "start":query.start,
                "end":query.end,
                "row_count":rows.len(),
                "max_ingested_millis":max_ingested_millis,
                "content_hash":content_hash
            }))
            .map_err(|_| marketcow_storage::RepositoryError::Unavailable)?;
            let snapshot = hex::encode(Sha256::digest(identity_payload));
            return Ok(marketcow_storage::CanonicalDatasetIdentity {
                symbol: query.symbol.clone(),
                interval: query.interval.clone(),
                adjustment: query.adjustment.clone(),
                start: query.start,
                end: query.end,
                row_count: rows.len() as u64,
                max_ingested_millis,
                content_hash,
                canonical_version: max_ingested_millis.to_string(),
                snapshot_id: snapshot[..32].into(),
            });
        }
        Err(marketcow_storage::RepositoryError::Unavailable)
    }

    #[cfg(test)]
    async fn put_memory(&self, symbol: &str, payload: serde_json::Value) {
        self.memory_quotes
            .lock()
            .await
            .insert(symbol.into(), payload);
    }

    #[cfg(test)]
    async fn put_canonical_memory(&self, record: marketcow_storage::CanonicalBarRecord) {
        self.memory_bars.lock().await.push(record);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CanonicalCursorBinding {
    instrument_id: String,
    start: String,
    end: String,
    interval: String,
    adjustment: String,
    page_size: u32,
    snapshot_id: String,
}

#[derive(Serialize, Deserialize)]
struct CanonicalCursorPayload {
    v: u8,
    q: CanonicalCursorBinding,
    after_ms: i64,
    iat: i64,
}

struct CanonicalCursorSigner {
    secret: Vec<u8>,
    ttl_seconds: i64,
}

impl CanonicalCursorSigner {
    fn open(storage_root: &Path) -> Result<Self> {
        let ttl_seconds = env::var("MARKETCOW_MARKET_BAR_CURSOR_TTL_SECONDS")
            .unwrap_or_else(|_| "3600".into())
            .parse::<i64>()?;
        if !(60..=86_400).contains(&ttl_seconds) {
            bail!("MARKETCOW_MARKET_BAR_CURSOR_TTL_SECONDS must be between 60 and 86400");
        }
        let secret = match env::var("MARKETCOW_MARKET_BAR_CURSOR_SECRET") {
            Ok(secret) => validate_cursor_secret(secret.as_bytes())?,
            Err(env::VarError::NotPresent) => {
                let path = storage_root.join(".market-bar-cursor.key");
                match fs::symlink_metadata(&path) {
                    Ok(metadata) => {
                        if !metadata.file_type().is_file() || metadata.mode() & 0o777 != 0o600 {
                            bail!("market bar cursor key must be a mode-0600 regular file");
                        }
                        validate_cursor_secret(&fs::read(path)?)?
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        let generated =
                            format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
                        let mut file = OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .mode(0o600)
                            .open(&path)?;
                        file.write_all(generated.as_bytes())?;
                        file.sync_all()?;
                        File::open(storage_root)?.sync_all()?;
                        generated.into_bytes()
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            secret,
            ttl_seconds,
        })
    }

    fn encode(&self, binding: CanonicalCursorBinding, after_ms: i64, now: i64) -> Result<String> {
        let payload = serde_json::to_vec(&CanonicalCursorPayload {
            v: 1,
            q: binding,
            after_ms,
            iat: now,
        })?;
        let signature = hmac_sha256(&self.secret, &payload);
        Ok(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(signature)
        ))
    }

    fn decode(&self, token: &str, expected: &CanonicalCursorBinding, now: i64) -> Result<i64> {
        if token.is_empty() || token.len() > 2_048 {
            bail!("invalid cursor length");
        }
        let (payload, signature) = token.split_once('.').context("invalid canonical cursor")?;
        let payload = URL_SAFE_NO_PAD.decode(payload)?;
        let signature = URL_SAFE_NO_PAD.decode(signature)?;
        let expected_signature = hmac_sha256(&self.secret, &payload);
        if !constant_time_equal(&signature, &expected_signature) {
            bail!("cursor integrity check failed");
        }
        let decoded: CanonicalCursorPayload = serde_json::from_slice(&payload)?;
        if decoded.v != 1 || &decoded.q != expected {
            bail!("cursor does not match this query");
        }
        if decoded.iat > now + 30 || now.saturating_sub(decoded.iat) > self.ttl_seconds {
            bail!("cursor has expired or was issued in the future");
        }
        Ok(decoded.after_ms)
    }
}

fn validate_cursor_secret(secret: &[u8]) -> Result<Vec<u8>> {
    let secret = secret.trim_ascii();
    let lowered = String::from_utf8_lossy(secret).to_ascii_lowercase();
    if secret.len() < 32
        || matches!(
            lowered.as_str(),
            "marketcow-local-cursor-secret"
                | "replace-with-a-local-development-secret"
                | "change-me"
                | "changeme"
        )
        || lowered.contains("placeholder")
    {
        bail!("market bar cursor secret must contain at least 32 non-placeholder bytes");
    }
    Ok(secret.to_vec())
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut block = [0_u8; 64];
    if key.len() > block.len() {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner_pad = [0x36_u8; 64];
    let mut outer_pad = [0x5c_u8; 64];
    for index in 0..64 {
        inner_pad[index] ^= block[index];
        outer_pad[index] ^= block[index];
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(message);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner);
    outer.finalize().into()
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

    fn record_lifecycle(
        &self,
        domain: &str,
        transition: &str,
        details: serde_json::Value,
    ) -> Result<()> {
        self.local.record(
            &json!({
                "schema_version":"marketcow.lifecycle-audit.v1",
                "audit_id":format!("audit-{}", Uuid::new_v4()),
                "occurred_at":Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true),
                "domain":domain,
                "transition":transition,
                "details":details,
                "real_order_submission_enabled":false
            }),
            true,
        )
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
        Command::Wal { command } => match command {
            WalCommand::Verify { path } => {
                let events = marketcow_core::SegmentedWal::verify(path)?;
                println!(
                    "{}",
                    json!({"status":"ok","records":events.len(),"last_cursor":events.last().map(|x|x.event.cursor)})
                );
            }
            WalCommand::AnchorLegacyCheckpoint {
                root,
                scope_id,
                expected_manifest_sha256,
                wal_segment_bytes,
            } => {
                let manifest = marketcow_runtime::anchor_verified_legacy_checkpoint(
                    marketcow_runtime::RuntimeConfig {
                        root,
                        scope_id,
                        config_revision: "legacy-checkpoint-anchor-migration-v1".into(),
                        wal_segment_bytes,
                        recent_event_capacity: 1,
                    },
                    &expected_manifest_sha256,
                )?;
                println!("{}", serde_json::to_string_pretty(&manifest)?);
            }
        },
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

struct HyperliquidShadowRuntime {
    reader: marketcow_realtime::RealtimeHubReader,
    stream: broadcast::Sender<marketcow_realtime::StreamEvent>,
    queues: HyperliquidQueueProbe,
    transport: tokio::task::JoinHandle<()>,
    owner: tokio::task::JoinHandle<()>,
    gateway: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct HyperliquidQueueProbe {
    transport: mpsc::Sender<marketcow_realtime::TransportOutput>,
    gateway: mpsc::Sender<marketcow_realtime::StreamEvent>,
}

impl HyperliquidQueueProbe {
    fn transport_depth(&self) -> usize {
        self.transport.max_capacity() - self.transport.capacity()
    }

    fn gateway_depth(&self) -> usize {
        self.gateway.max_capacity() - self.gateway.capacity()
    }
}

fn start_hyperliquid_shadow(
    config: &Config,
    config_revision: &str,
    audit: Arc<AuditCoordinator>,
    shutdown: watch::Receiver<bool>,
) -> Result<Option<HyperliquidShadowRuntime>> {
    let Some(shadow) = &config.hyperliquid_shadow else {
        return Ok(None);
    };
    let data_types = BTreeSet::from([
        marketcow_realtime::DataType::Quote,
        marketcow_realtime::DataType::OrderBook,
        marketcow_realtime::DataType::Trade,
        marketcow_realtime::DataType::AssetContext,
    ]);
    let normalizer = marketcow_realtime::HyperliquidNormalizer::new(
        shadow.instruments.clone(),
        shadow.maximum_source_delay_millis,
    )?;
    let subscriptions =
        marketcow_realtime::hyperliquid_subscriptions(&shadow.instruments, &data_types)?;
    let mut hub = marketcow_realtime::DurableRealtimeHub::open(
        config.storage_root.join("hyperliquid"),
        "hyperliquid-main",
        config_revision,
        HYPERLIQUID_WAL_SEGMENT_BYTES,
        HYPERLIQUID_REPLAY_CAPACITY,
    )?;
    let reader = hub.reader();
    let recovered = reader.snapshot();
    audit.record_lifecycle(
        "hyperliquid",
        "starting",
        json!({
            "stream_id":recovered.stream_id,
            "config_revision":recovered.config_revision,
            "recovered_public_sequence":recovered.public_sequence,
            "recovered_wal_cursor":recovered.wal_cursor
        }),
    )?;
    let (transport_tx, mut transport_rx) = mpsc::channel(HYPERLIQUID_TRANSPORT_CHANNEL_CAPACITY);
    let (gateway_tx, mut gateway_rx) = mpsc::channel(HYPERLIQUID_GATEWAY_CHANNEL_CAPACITY);
    let queues = HyperliquidQueueProbe {
        transport: transport_tx.clone(),
        gateway: gateway_tx.clone(),
    };
    let (public_stream, _) = broadcast::channel(HYPERLIQUID_PUBLIC_CHANNEL_CAPACITY);
    let terminal_error = Arc::new(Mutex::new(None::<String>));
    let terminal_for_transport = terminal_error.clone();
    let audit_for_transport = audit.clone();
    let transport = tokio::spawn(async move {
        if let Err(error) = marketcow_realtime::run_hyperliquid_transport(
            marketcow_realtime::HyperliquidTransportConfig::production(),
            normalizer,
            subscriptions,
            transport_tx,
            shutdown,
        )
        .await
        {
            warn!(error=%error, "hyperliquid_transport_failed_closed");
            let _ = audit_for_transport.record_lifecycle(
                "hyperliquid",
                "failed_closed",
                json!({"component":"transport","reason":error.reason_code()}),
            );
            *terminal_for_transport
                .lock()
                .expect("terminal mutex poisoned") =
                Some(format!("transport_{}", error.reason_code()));
        }
    });
    let audit_for_owner = audit.clone();
    let owner = tokio::task::spawn_blocking(move || {
        let mut published_since_checkpoint = 0_usize;
        while let Some(output) = transport_rx.blocking_recv() {
            let lifecycle_transition = match &output {
                marketcow_realtime::TransportOutput::Connected { attempt } => {
                    Some(("connected", json!({"attempt":attempt})))
                }
                marketcow_realtime::TransportOutput::SubscriptionsReady { attempt } => {
                    Some(("ready", json!({"attempt":attempt})))
                }
                marketcow_realtime::TransportOutput::Degraded {
                    attempt,
                    reason,
                    retryable,
                } => Some((
                    "degraded",
                    json!({"attempt":attempt,"reason":reason,"retryable":retryable}),
                )),
                marketcow_realtime::TransportOutput::Events { .. } => None,
            };
            match hub.ingest(output, &gateway_tx) {
                Ok(published) => {
                    if let Some((transition, details)) = lifecycle_transition
                        && audit_for_owner
                            .record_lifecycle("hyperliquid", transition, details)
                            .is_err()
                    {
                        hub.fail_closed("lifecycle_audit_failed");
                        return;
                    }
                    published_since_checkpoint += published;
                    if published_since_checkpoint >= 1_000 {
                        if let Err(error) = hub.checkpoint() {
                            warn!(error=%error, "hyperliquid_checkpoint_failed_closed");
                            let _ = audit_for_owner.record_lifecycle(
                                "hyperliquid",
                                "failed_closed",
                                json!({"component":"checkpoint","reason":error.to_string()}),
                            );
                            hub.fail_closed("checkpoint_failed");
                            return;
                        }
                        published_since_checkpoint = 0;
                    }
                }
                Err(error) => {
                    warn!(error=%error, "hyperliquid_owner_failed_closed");
                    let _ = audit_for_owner.record_lifecycle(
                        "hyperliquid",
                        "failed_closed",
                        json!({"component":"owner","reason":error.to_string()}),
                    );
                    return;
                }
            }
        }
        if let Some(reason) = terminal_error
            .lock()
            .expect("terminal mutex poisoned")
            .take()
        {
            hub.fail_closed(&reason);
        } else if let Err(error) = hub.checkpoint() {
            warn!(error=%error, "hyperliquid_shutdown_checkpoint_failed_closed");
            let _ = audit_for_owner.record_lifecycle(
                "hyperliquid",
                "failed_closed",
                json!({"component":"shutdown_checkpoint","reason":error.to_string()}),
            );
            hub.fail_closed("shutdown_checkpoint_failed");
        } else {
            let _ = audit_for_owner.record_lifecycle(
                "hyperliquid",
                "stopped",
                json!({"reason":"service_shutdown","checkpointed":true}),
            );
        }
    });
    let public_stream_for_gateway = public_stream.clone();
    let gateway = tokio::spawn(async move {
        while let Some(event) = gateway_rx.recv().await {
            let _ = public_stream_for_gateway.send(event);
        }
    });
    Ok(Some(HyperliquidShadowRuntime {
        reader,
        stream: public_stream,
        queues,
        transport,
        owner,
        gateway,
    }))
}

async fn stop_polymarket_transport_for_scope_switch(
    shutdown: &watch::Sender<bool>,
    transport: &mut tokio::task::JoinHandle<
        std::result::Result<(), marketcow_polymarket::TransportError>,
    >,
) {
    let _ = shutdown.send(true);
    if tokio::time::timeout(
        POLYMARKET_TRANSPORT_SCOPE_SWITCH_STOP_TIMEOUT,
        &mut *transport,
    )
    .await
    .is_err()
    {
        // The ingress task does not own the WAL or projection writer. Once its bounded sender is
        // stopped, aborting a stuck upstream close handshake cannot create a partial durable
        // event. This keeps the already-prepared atomic generation swap bounded.
        transport.abort();
        let _ = transport.await;
        warn!(
            timeout_ms = POLYMARKET_TRANSPORT_SCOPE_SWITCH_STOP_TIMEOUT.as_millis(),
            "polymarket_transport_scope_switch_forced_abort"
        );
    }
}

fn start_polymarket_live(
    state: AppState,
    mut shutdown: watch::Receiver<bool>,
    mut scope_switches: mpsc::Receiver<PolymarketScopeSwitchRequest>,
) -> Option<tokio::task::JoinHandle<()>> {
    let mut live = state
        .active_polymarket_scope
        .load_full()
        .as_deref()
        .cloned()?;
    Some(tokio::spawn(async move {
        let refresh_client = match reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(POLYMARKET_BOOK_REFRESH_TIMEOUT)
            .https_only(true)
            .user_agent("MarketCow-Rust/Polymarket-authoritative-book-refresh")
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                warn!(error=%error, "polymarket_book_refresh_client_failed_closed");
                return;
            }
        };
        'service: loop {
            let (sender, mut receiver) = mpsc::channel(POLYMARKET_TRANSPORT_CHANNEL_CAPACITY);
            let (transport_shutdown_tx, transport_shutdown_rx) = watch::channel(false);
            let transport_tokens = live.token_ids.clone();
            let mut transport = tokio::spawn(async move {
                marketcow_polymarket::run_polymarket_transport(
                    marketcow_polymarket::PolymarketTransportConfig::production(),
                    transport_tokens,
                    sender,
                    transport_shutdown_rx,
                )
                .await
            });
            let mut events_since_checkpoint = 0_usize;
            let mut checkpoint_tick = tokio::time::interval(POLYMARKET_CHECKPOINT_INTERVAL);
            checkpoint_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            checkpoint_tick.tick().await;
            let refresh_interval =
                polymarket_book_refresh_interval(state.config.maximum_book_age_ms);
            let mut refresh_tick = tokio::time::interval(refresh_interval);
            refresh_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // The first authoritative validation is intentionally immediate.  A checkpoint may
            // contain one stale token even though the remaining universe is healthy; waiting a
            // full refresh interval would unnecessarily keep the public API globally unready.
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        let _ = transport_shutdown_tx.send(true);
                        let _ = (&mut transport).await;
                        let _ = checkpoint_polymarket_runtime(&state).await;
                        if changed.is_err() || *shutdown.borrow() {
                            break 'service;
                        }
                    }
                    request = scope_switches.recv() => {
                        let Some(request) = request else {
                            let _ = transport_shutdown_tx.send(true);
                            let _ = (&mut transport).await;
                            let _ = checkpoint_polymarket_runtime(&state).await;
                            break 'service;
                        };
                        let PolymarketScopeSwitchRequest {
                            live: requested_live,
                            response,
                        } = request;
                        // Keep the old transport connected and its bounded ingress queue alive
                        // while the candidate is prepared off the async request workers. This
                        // owner intentionally pauses application at a precise cursor boundary;
                        // consumers keep the last verified projection, and no old-generation
                        // frames can race the candidate. Only a validated candidate reaches the
                        // bounded transport stop and atomic writer/projection swap.
                        let candidate = prepare_polymarket_scope_candidate(
                            &state,
                            &live,
                            &requested_live,
                        )
                        .await;
                        let result = match candidate {
                            Ok(candidate_runtime) => {
                                stop_polymarket_transport_for_scope_switch(
                                    &transport_shutdown_tx,
                                    &mut transport,
                                )
                                .await;
                                commit_polymarket_scope_switch(
                                    &state,
                                    &live,
                                    requested_live,
                                    candidate_runtime,
                                )
                                .await
                            }
                            Err(error) => Err(error),
                        };
                        match result {
                            Ok((activated, receipt)) => {
                                live = activated;
                                info!(scope_id=%live.scope_id, boundary_cursor=receipt.boundary_cursor, "polymarket_scope_activated");
                                let _ = response.send(Ok(receipt));
                                continue 'service;
                            }
                            Err(error) => {
                                let _ = response.send(Err(error));
                                match wait_for_polymarket_retry_or_scope_recovery(
                                    &state,
                                    &live,
                                    &mut shutdown,
                                    &mut scope_switches,
                                    POLYMARKET_TRANSPORT_RESTART_DELAY,
                                )
                                .await
                                {
                                    Some(activated) => {
                                        live = activated;
                                        continue 'service;
                                    }
                                    None => break 'service,
                                }
                            }
                        }
                    }
                    frames = receiver.recv() => {
                        let Some(frames) = frames else {
                            match (&mut transport).await {
                                Ok(Ok(())) => warn!("polymarket_transport_ended_without_shutdown"),
                                Ok(Err(error)) => warn!(
                                    reason=error.reason_code(),
                                    error=%error,
                                    "polymarket_transport_restart_cycle"
                                ),
                                Err(error) => warn!(error=%error, "polymarket_transport_task_failed"),
                            }
                            let _ = checkpoint_polymarket_runtime(&state).await;
                            match wait_for_polymarket_retry_or_scope_recovery(
                                &state,
                                &live,
                                &mut shutdown,
                                &mut scope_switches,
                                POLYMARKET_TRANSPORT_RESTART_DELAY,
                            )
                            .await
                            {
                                Some(activated) => {
                                    live = activated;
                                    continue 'service;
                                }
                                None => break 'service,
                            }
                        };
                        match apply_polymarket_transport_frames(&state, frames).await {
                            Ok(events) => {
                                events_since_checkpoint += events;
                                if events_since_checkpoint >= 50_000 {
                                    if let Err(error) = checkpoint_polymarket_runtime(&state).await {
                                        warn!(error=%error, "polymarket_live_checkpoint_failed_closed");
                                        let _ = transport_shutdown_tx.send(true);
                                        let _ = (&mut transport).await;
                                        break 'service;
                                    }
                                    events_since_checkpoint = 0;
                                }
                            }
                            Err(error) => {
                                warn!(error=%error, "polymarket_live_owner_failed_closed");
                                let _ = transport_shutdown_tx.send(true);
                                let _ = (&mut transport).await;
                                let _ = checkpoint_polymarket_runtime(&state).await;
                                match wait_for_polymarket_retry_or_scope_recovery(
                                    &state,
                                    &live,
                                    &mut shutdown,
                                    &mut scope_switches,
                                    POLYMARKET_TRANSPORT_RESTART_DELAY,
                                )
                                .await
                                {
                                    Some(activated) => {
                                        live = activated;
                                        continue 'service;
                                    }
                                    None => break 'service,
                                }
                            }
                        }
                    }
                    _ = checkpoint_tick.tick() => {
                        if events_since_checkpoint > 0 {
                            if let Err(error) = checkpoint_polymarket_runtime(&state).await {
                                warn!(error=%error, "polymarket_live_checkpoint_failed_closed");
                                let _ = transport_shutdown_tx.send(true);
                                let _ = (&mut transport).await;
                                break 'service;
                            }
                            events_since_checkpoint = 0;
                        }
                    }
                    _ = refresh_tick.tick() => {
                        match fetch_polymarket_book_refreshes(&refresh_client, &live.token_ids).await {
                            Ok(batch) => {
                                match recover_quarantined_polymarket_markets(&state, &live, &batch.frames).await {
                                    Ok(recovery) => {
                                        events_since_checkpoint += recovery.applied_events;
                                        if recovery.attempted > 0 {
                                            info!(
                                                attempted_markets=recovery.attempted,
                                                recovered_markets=recovery.recovered,
                                                rejected_markets=recovery.rejected,
                                                "polymarket_market_recovery_snapshot_processed"
                                            );
                                        }
                                    }
                                    Err(error) => {
                                        // A durability or publication failure is global. Invalid
                                        // market snapshots are returned as rejected local attempts
                                        // and never reach this branch.
                                        warn!(error=%error, "polymarket_market_recovery_failed_closed");
                                        let _ = transport_shutdown_tx.send(true);
                                        let _ = (&mut transport).await;
                                        let _ = checkpoint_polymarket_runtime(&state).await;
                                        break 'service;
                                    }
                                }
                                match validate_polymarket_book_refreshes(
                                    &state,
                                    &live,
                                    &batch.frames,
                                    &batch.missing_token_ids,
                                ).await {
                                    Ok(summary) => {
                                        info!(
                                            matched_books=summary.matched_books,
                                            mismatched_books=summary.mismatched_books,
                                            missing_books=summary.missing_books,
                                            quarantine_token_count=summary.quarantine_token_ids.len(),
                                            interval_ms=refresh_interval.as_millis(),
                                            "polymarket_authoritative_books_validated"
                                        );
                                        if !summary.quarantine_token_ids.is_empty() {
                                            match quarantine_polymarket_tokens_from_authoritative_refresh(
                                                &state,
                                                &summary.quarantine_token_ids,
                                            ).await {
                                                Ok(events) => {
                                                    events_since_checkpoint += events;
                                                    match recover_quarantined_polymarket_markets(
                                                        &state,
                                                        &live,
                                                        &batch.frames,
                                                    ).await {
                                                        Ok(recovery) => {
                                                            events_since_checkpoint += recovery.applied_events;
                                                            info!(
                                                                attempted_markets=recovery.attempted,
                                                                recovered_markets=recovery.recovered,
                                                                rejected_markets=recovery.rejected,
                                                                "polymarket_token_local_recovery_processed"
                                                            );
                                                        }
                                                        Err(error) => {
                                                            warn!(error=%error, "polymarket_token_local_recovery_failed_closed");
                                                            let _ = transport_shutdown_tx.send(true);
                                                            let _ = (&mut transport).await;
                                                            let _ = checkpoint_polymarket_runtime(&state).await;
                                                            break 'service;
                                                        }
                                                    }
                                                }
                                                Err(error) => {
                                                    warn!(error=%error, "polymarket_token_quarantine_failed_closed");
                                                    let _ = transport_shutdown_tx.send(true);
                                                    let _ = (&mut transport).await;
                                                    let _ = checkpoint_polymarket_runtime(&state).await;
                                                    break 'service;
                                                }
                                            }
                                        }
                                    }
                                    Err(error) => {
                                        // Healthy books remain read-only freshness observations.
                                        // Only explicitly quarantined markets may use the atomic
                                        // two-token repair path above.
                                        warn!(error=%error, "polymarket_book_validation_retryable_failure");
                                    }
                                }
                            },
                            Err(error) => {
                                // A transient HTTP failure never fabricates freshness. The last
                                // verified books remain published and the existing age gate will
                                // fail closed if retries cannot refresh them before expiry.
                                warn!(error=%error, "polymarket_book_refresh_retryable_failure");
                            }
                        }
                    }
                }
            }
        }
    }))
}

fn polymarket_book_refresh_interval(maximum_book_age_ms: u64) -> Duration {
    let quarter_age_ms = maximum_book_age_ms
        .saturating_add(3)
        .saturating_div(4)
        .max(1);
    Duration::from_millis(quarter_age_ms).min(POLYMARKET_BOOK_REFRESH_MAX_INTERVAL)
}

async fn fetch_polymarket_book_refreshes(
    client: &reqwest::Client,
    token_ids: &[String],
) -> Result<PolymarketBookRefreshBatch> {
    if token_ids.is_empty() || token_ids.len() > 500 {
        bail!("Polymarket book refresh token scope is invalid");
    }
    let request = token_ids
        .iter()
        .map(|token_id| json!({"token_id":token_id}))
        .collect::<Vec<_>>();
    let response = client
        .post(marketcow_polymarket::CLOB_BOOK_SNAPSHOT_SOURCE_URL)
        .json(&request)
        .send()
        .await?
        .error_for_status()?;
    if response
        .content_length()
        .is_some_and(|length| length > POLYMARKET_BOOK_REFRESH_MAX_RESPONSE_BYTES)
    {
        bail!("Polymarket book refresh response exceeds the bounded limit");
    }
    let bytes = response.bytes().await?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > POLYMARKET_BOOK_REFRESH_MAX_RESPONSE_BYTES {
        bail!("Polymarket book refresh response exceeds the bounded limit");
    }
    validate_polymarket_book_refresh_response(
        serde_json::from_slice(&bytes)?,
        token_ids,
        Utc::now(),
    )
}

fn validate_polymarket_book_refresh_response(
    payload: serde_json::Value,
    token_ids: &[String],
    received_at: DateTime<Utc>,
) -> Result<PolymarketBookRefreshBatch> {
    let requested = token_ids.iter().cloned().collect::<BTreeSet<_>>();
    if requested.len() != token_ids.len()
        || requested.iter().any(|token| {
            token.is_empty()
                || token.len() > 128
                || !token.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        bail!("Polymarket book refresh token scope is invalid");
    }
    let books = payload
        .as_array()
        .context("Polymarket book refresh response must be an array")?;
    let mut validated = BTreeMap::new();
    for value in books {
        let mut book = value
            .as_object()
            .cloned()
            .context("Polymarket book refresh item must be an object")?;
        let token_id = book
            .get("asset_id")
            .and_then(serde_json::Value::as_str)
            .context("Polymarket book refresh asset_id must be a string")?
            .to_owned();
        if !requested.contains(&token_id) || validated.contains_key(&token_id) {
            bail!("Polymarket book refresh returned unexpected or duplicate token");
        }
        let timestamp = book
            .get("timestamp")
            .and_then(serde_json::Value::as_str)
            .context("Polymarket book refresh timestamp must be an exact string")?;
        if timestamp.is_empty() || !timestamp.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!("Polymarket book refresh timestamp is invalid");
        }
        validate_polymarket_book_refresh_decimal(book.get("tick_size"), "tick_size")?;
        for side in ["bids", "asks"] {
            let levels = book
                .get(side)
                .and_then(serde_json::Value::as_array)
                .with_context(|| format!("Polymarket book refresh {side} must be an array"))?;
            for level in levels {
                let level = level
                    .as_object()
                    .context("Polymarket book refresh level must be an object")?;
                validate_polymarket_book_refresh_decimal(level.get("price"), "price")?;
                validate_polymarket_book_refresh_decimal(level.get("size"), "size")?;
            }
        }
        book.insert(
            "event_type".into(),
            serde_json::Value::String("book".into()),
        );
        validated.insert(
            token_id,
            marketcow_polymarket::RawTransportFrame {
                raw_payload: serde_json::Value::Object(book),
                received_at,
            },
        );
    }
    let returned = validated.keys().cloned().collect::<BTreeSet<_>>();
    Ok(PolymarketBookRefreshBatch {
        frames: validated.into_values().collect(),
        missing_token_ids: requested.difference(&returned).cloned().collect(),
    })
}

fn validate_polymarket_book_refresh_decimal(
    value: Option<&serde_json::Value>,
    field: &'static str,
) -> Result<()> {
    let text = value
        .and_then(serde_json::Value::as_str)
        .with_context(|| format!("Polymarket book refresh {field} must be an exact string"))?;
    let decimal = text
        .parse::<rust_decimal::Decimal>()
        .with_context(|| format!("Polymarket book refresh {field} is invalid"))?;
    if decimal <= rust_decimal::Decimal::ZERO {
        bail!("Polymarket book refresh {field} must be positive");
    }
    Ok(())
}

async fn commit_polymarket_scope_switch(
    state: &AppState,
    current: &PolymarketLiveConfig,
    mut activated: PolymarketLiveConfig,
    candidate_runtime: marketcow_runtime::PolymarketRuntime,
) -> Result<(PolymarketLiveConfig, PolymarketScopeSwitchReceipt), String> {
    let previous_cursor = state.projection.load().cursor;
    let same_scope = current.scope_id == activated.scope_id;
    if let Some(universe) = activated.universe.as_ref() {
        let candidate = candidate_runtime.projection();
        if candidate.cursor <= previous_cursor
            || !candidate.ready
            || !candidate.instrument_ticks_consistent()
            || !projection_matches_polymarket_scope(&activated, &candidate, true)
        {
            return Err("universe_candidate_not_atomic_ready".into());
        }
        if universe.generation
            != current
                .universe
                .as_ref()
                .map_or(0, |value| value.generation)
                .saturating_add(1)
        {
            return Err("universe_generation_not_monotonic".into());
        }
    }
    checkpoint_polymarket_runtime(state)
        .await
        .map_err(|error| format!("old_scope_checkpoint_failed:{error}"))?;
    if let Some(universe) = activated.universe.clone() {
        activated.scope_file_path = Some(
            persist_active_polymarket_universe(state, &activated)
                .map_err(|error| format!("universe_activation_persist_failed:{error}"))?,
        );
        state
            .audit
            .record_lifecycle(
                "polymarket_universe",
                "generation_prepared",
                json!({
                    "universe_id":universe.universe_id,
                    "generation":universe.generation,
                    "added_markets":universe.added_markets,
                    "removed_markets":universe.removed_markets,
                    "scope_file_sha256":activated.scope_file_sha256,
                    "real_order_submission_enabled":false
                }),
            )
            .map_err(|error| format!("universe_activation_audit_failed:{error}"))?;
    }
    let previous_scope_id = current.scope_id.clone();
    let mut runtime = state.runtime.lock().await;
    *runtime = candidate_runtime;
    let catalog_changes = if same_scope {
        clone_polymarket_recent_events(&runtime)
            .into_iter()
            .filter(|record| {
                record.event.cursor > previous_cursor
                    && matches!(
                        &record.event.kind,
                        marketcow_core::EventKind::CatalogSnapshot { .. }
                    )
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let activated_projection = runtime.projection();
    drop(runtime);
    state.projection.store(activated_projection);
    state
        .active_polymarket_scope
        .store(Some(Arc::new(activated.clone())));
    reset_polymarket_book_validations(state, &activated.scope_id);
    if let Some(universe) = activated.universe.as_ref()
        && let Err(error) = state.audit.record_lifecycle(
            "polymarket_universe",
            "generation_activated",
            json!({
                "universe_id":universe.universe_id,
                "generation":universe.generation,
                "boundary_cursor":state.projection.load().cursor,
                "real_order_submission_enabled":false
            }),
        )
    {
        warn!(error=%error, "polymarket_universe_activation_audit_failed");
    }
    // A same-scope artifact refresh (for example, a fee schedule revision) preserves the cursor
    // domain. Publish its catalog event after the atomic projection swap so existing consumers
    // either apply the new generation or fail closed on a cursor gap. Different scope IDs are
    // handled by the existing `scope_changed` resync path.
    for record in catalog_changes {
        let _ = state.stream.send(record);
    }
    let boundary_cursor = state.projection.load().cursor;
    let receipt = PolymarketScopeSwitchReceipt {
        previous_scope_id,
        active_scope_id: activated.scope_id.clone(),
        boundary_cursor,
        scope_file_sha256: activated.scope_file_sha256.clone().unwrap_or_default(),
        universe_id: activated
            .universe
            .as_ref()
            .map(|value| value.universe_id.clone()),
        previous_generation: current.universe.as_ref().map(|value| value.generation),
        active_generation: activated.universe.as_ref().map(|value| value.generation),
    };
    Ok((activated, receipt))
}

fn persist_active_polymarket_universe(
    state: &AppState,
    activated: &PolymarketLiveConfig,
) -> Result<PathBuf> {
    let source = activated
        .scope_file_path
        .as_ref()
        .context("dynamic universe artifact source path is missing")?;
    let bytes = fs::read(source).context("failed to read dynamic universe artifact")?;
    let digest = hex::encode(Sha256::digest(&bytes));
    if activated
        .scope_file_sha256
        .as_ref()
        .is_none_or(|expected| !digest.eq_ignore_ascii_case(expected))
    {
        bail!("dynamic universe artifact hash changed before activation");
    }
    let directory = state
        .config
        .storage_root
        .join("polymarket-universe-control");
    fs::create_dir_all(&directory)?;
    let destination = directory.join("active-scope.json");
    let temporary = directory.join(format!(".active-scope.{}.tmp", Uuid::new_v4()));
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary)?;
    output.write_all(&bytes)?;
    output.sync_all()?;
    drop(output);
    fs::rename(&temporary, &destination)?;
    File::open(&directory)?.sync_all()?;
    Ok(destination)
}

fn projection_matches_polymarket_scope(
    live: &PolymarketLiveConfig,
    projection: &marketcow_core::Projection,
    require_books: bool,
) -> bool {
    projection_catalog_identity_matches_scope(live, projection)
        && projection.markets.values().all(|market| {
            market.instrument_facts.as_ref().is_some_and(|facts| {
                facts
                    .fee_schedule
                    .as_ref()
                    .is_some_and(marketcow_core::MarketFeeSchedule::is_complete)
            })
        })
        && (!require_books
            || projection.books.keys().cloned().collect::<BTreeSet<_>>()
                == live.token_ids.iter().cloned().collect())
}

fn projection_catalog_identity_matches_scope(
    live: &PolymarketLiveConfig,
    projection: &marketcow_core::Projection,
) -> bool {
    let expected_markets = live.market_ids.iter().cloned().collect::<BTreeSet<_>>();
    let expected_tokens = live.token_ids.iter().cloned().collect::<BTreeSet<_>>();
    let actual_markets = projection.markets.keys().cloned().collect::<BTreeSet<_>>();
    let catalog_tokens = projection
        .markets
        .values()
        .flat_map(|market| {
            market
                .outcomes
                .iter()
                .map(|outcome| outcome.token_id.clone())
        })
        .collect::<BTreeSet<_>>();
    live.catalog_revision.as_ref() == projection.catalog_revision.as_ref()
        && expected_markets == actual_markets
        && expected_tokens == catalog_tokens
}

fn seed_polymarket_scope_catalog(
    runtime: &mut marketcow_runtime::PolymarketRuntime,
    live: Option<&PolymarketLiveConfig>,
) -> Result<()> {
    let Some(live) = live else {
        return Ok(());
    };
    let Some(catalog_frame) = &live.catalog_frame else {
        return Ok(());
    };
    let projection = runtime.projection();
    let require_books = live.universe.is_some();
    if projection_matches_polymarket_scope(live, &projection, require_books) {
        return Ok(());
    }
    if projection.catalog_revision.is_some()
        && !projection_catalog_identity_matches_scope(live, &projection)
        && live.universe.is_none()
    {
        bail!("persisted Polymarket catalog does not match configured exact scope");
    }
    if live.universe.is_none() || !projection_catalog_identity_matches_scope(live, &projection) {
        runtime.apply_raw(catalog_frame.clone(), Utc::now())?;
    }
    for frame in &live.initial_book_frames {
        runtime.apply_raw(frame.clone(), Utc::now())?;
    }
    let projection = runtime.projection();
    if !projection_matches_polymarket_scope(live, &projection, require_books) {
        bail!("Polymarket catalog seed failed exact-scope validation");
    }
    runtime.checkpoint()?;
    Ok(())
}

async fn apply_polymarket_transport_frames(
    state: &AppState,
    frames: Vec<marketcow_polymarket::RawTransportFrame>,
) -> Result<usize> {
    if frames.is_empty() {
        bail!("Polymarket transport emitted an empty frame batch");
    }
    let mut runtime = state.runtime.lock().await;
    let mut published = Vec::new();
    let mut failure = None;
    for frame in frames {
        match runtime.apply_raw(frame.raw_payload, frame.received_at) {
            Ok(outcomes) => published.extend(outcomes),
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    state.projection.store(runtime.projection());
    drop(runtime);
    publish_polymarket_outcomes(state, &published);
    let recovery_records = published
        .iter()
        .filter(|outcome| {
            !matches!(
                outcome.persisted.event.kind,
                marketcow_core::EventKind::SourceGap { .. }
            ) && (outcome.persisted.fail_closed_reason.is_some()
                || !outcome.persisted.applied && outcome.persisted.market.is_none())
        })
        .collect::<Vec<_>>();
    if !recovery_records.is_empty() {
        // An incremental projection rejection cannot heal through further deltas. Force a
        // controlled reconnect so the venue supplies authoritative full books. A SourceGap is
        // itself the first half of that recovery protocol and must be followed by those books on
        // the same connection rather than recursively restarting at the boundary.
        let reason_codes = recovery_records
            .iter()
            .map(|outcome| {
                outcome
                    .persisted
                    .fail_closed_reason
                    .as_deref()
                    .unwrap_or("unapplied_without_reason")
                    .to_owned()
            })
            .collect::<BTreeSet<_>>();
        let affected_token_ids = recovery_records
            .iter()
            .map(|outcome| outcome.persisted.event.kind.token_id().to_owned())
            .collect::<BTreeSet<_>>();
        let projection = state.projection.load();
        let affected_market_ids = projection
            .markets
            .values()
            .filter(|market| {
                market
                    .outcomes
                    .iter()
                    .any(|outcome| affected_token_ids.contains(&outcome.token_id))
            })
            .map(|market| market.market_id.clone())
            .collect::<BTreeSet<_>>();
        let maximum_observed_source_delay_ms = recovery_records
            .iter()
            .map(|outcome| {
                outcome
                    .persisted
                    .event
                    .received_at
                    .signed_duration_since(outcome.persisted.event.source_observed_at)
                    .num_milliseconds()
                    .max(0)
            })
            .max()
            .unwrap_or_default();
        let first_cursor = recovery_records
            .first()
            .map(|outcome| outcome.persisted.event.cursor)
            .unwrap_or_default();
        let last_cursor = recovery_records
            .last()
            .map(|outcome| outcome.persisted.event.cursor)
            .unwrap_or(first_cursor);
        let recovery_scope = if reason_codes.iter().all(|reason| {
            matches!(
                reason.as_str(),
                "source_data_delayed" | "source_data_missing"
            )
        }) {
            "transport_connection"
        } else {
            "projection"
        };
        let details = json!({
            "scope_id":projection.scope_id,
            "recovery_scope":recovery_scope,
            "reason_codes":reason_codes,
            "affected_token_ids":affected_token_ids,
            "affected_token_count":affected_token_ids.len(),
            "affected_market_ids":affected_market_ids,
            "affected_market_count":affected_market_ids.len(),
            "maximum_observed_source_delay_ms":maximum_observed_source_delay_ms,
            "first_cursor":first_cursor,
            "last_cursor":last_cursor,
            "action":"same_scope_authoritative_websocket_reconnect",
            "periodic_http_snapshot_used_for_repair":false,
            "real_order_submission_enabled":false
        });
        if let Err(error) = state.audit.record_lifecycle(
            "polymarket_transport",
            "recovery_required",
            details.clone(),
        ) {
            warn!(error=%error, "polymarket_transport_recovery_audit_failed");
        }
        bail!(
            "polymarket_projection_recovery_required:{}",
            serde_json::to_string(&details)?
        );
    }
    if let Some(error) = failure {
        return Err(error.into());
    }
    Ok(published.len())
}

fn publish_polymarket_outcomes(state: &AppState, published: &[marketcow_core::ApplyOutcome]) {
    for outcome in published {
        let _ = state.stream.send(outcome.persisted.clone());
    }
    for outcome in published.iter().filter(|outcome| {
        outcome
            .persisted
            .market
            .as_ref()
            .is_some_and(|market| market.transition.is_some())
    }) {
        let market = outcome.persisted.market.as_ref().expect("filtered market");
        match market.transition {
            Some(marketcow_core::MarketTransitionKind::Quarantined) => {
                state
                    .metrics
                    .polymarket_market_quarantines
                    .fetch_add(1, Ordering::Relaxed);
            }
            Some(marketcow_core::MarketTransitionKind::RecoveryStarted) => {
                state
                    .metrics
                    .polymarket_market_recovery_started
                    .fetch_add(1, Ordering::Relaxed);
            }
            Some(marketcow_core::MarketTransitionKind::Recovered) => {
                state
                    .metrics
                    .polymarket_market_recovered
                    .fetch_add(1, Ordering::Relaxed);
            }
            None => unreachable!("filtered transition"),
        }
        if let Err(error) = state.audit.record_lifecycle(
            "polymarket_market_projection",
            match market.transition {
                Some(marketcow_core::MarketTransitionKind::Quarantined) => "market_quarantined",
                Some(marketcow_core::MarketTransitionKind::RecoveryStarted) => {
                    "market_recovery_started"
                }
                Some(marketcow_core::MarketTransitionKind::Recovered) => "market_recovered",
                None => unreachable!("filtered transition"),
            },
            json!({
                "scope_id":outcome.projection.scope_id,
                "global_cursor":outcome.persisted.event.cursor,
                "market_id":market.market_id,
                "market_sequence":market.market_sequence,
                "projection_generation":market.projection_generation,
                "catalog_revision":market.catalog_revision,
                "event_revision":market.event_revision,
                "projection_status":market.projection_status,
                "reason_code":market.reason_code,
                "retryable":true,
                "full_sync_required":false,
                "real_order_submission_enabled":false
            }),
        ) {
            warn!(error=%error, "polymarket_market_transition_audit_failed");
        }
    }
}

async fn validate_polymarket_book_refreshes(
    state: &AppState,
    live: &PolymarketLiveConfig,
    frames: &[marketcow_polymarket::RawTransportFrame],
    missing_token_ids: &[String],
) -> Result<PolymarketBookValidationSummary> {
    if frames.is_empty() && missing_token_ids.is_empty() {
        bail!("Polymarket authoritative book validation emitted an empty batch");
    }
    let runtime = state.runtime.lock().await;
    if runtime.projection().scope_id != live.scope_id {
        bail!("Polymarket book validation crossed a scope boundary");
    }
    let mut validations = Vec::with_capacity(frames.len());
    for frame in frames {
        validations.push(
            runtime.validate_full_book_observation(frame.raw_payload.clone(), frame.received_at)?,
        );
    }
    let projection_cursor = runtime.projection().cursor;
    let observed = validations
        .iter()
        .map(|validation| validation.token_id.clone())
        .collect::<BTreeSet<_>>();
    let mut quarantine_token_ids = validations
        .iter()
        .filter(|validation| !validation.matches_projection)
        .filter(|validation| {
            runtime
                .projection()
                .books
                .get(&validation.token_id)
                .and_then(|book| book.source_observed_at)
                .is_none_or(|observed_at| {
                    validation
                        .received_at
                        .signed_duration_since(observed_at)
                        .num_milliseconds()
                        .max(0) as u64
                        >= state.config.maximum_book_age_ms
                })
        })
        .map(|validation| validation.token_id.clone())
        .collect::<BTreeSet<_>>();
    let requested = live.token_ids.iter().cloned().collect::<BTreeSet<_>>();
    let missing = missing_token_ids.iter().cloned().collect::<BTreeSet<_>>();
    if missing.len() != missing_token_ids.len()
        || !observed.is_disjoint(&missing)
        || observed.union(&missing).cloned().collect::<BTreeSet<_>>() != requested
    {
        bail!("Polymarket authoritative book validation missing-token scope is invalid");
    }
    let previous_validations = state.polymarket_book_validations.load_full();
    let previous_verified_at = (previous_validations.scope_id == live.scope_id)
        .then_some(&previous_validations.verified_at);
    let now = Utc::now();
    let missing_quarantine_token_ids = missing
        .iter()
        .filter(|token_id| {
            let source_observed_at = runtime
                .projection()
                .books
                .get(*token_id)
                .and_then(|book| book.source_observed_at);
            let previously_verified_at = previous_verified_at
                .and_then(|verified| verified.get(*token_id))
                .copied();
            source_observed_at
                .into_iter()
                .chain(previously_verified_at)
                .max()
                .is_none_or(|observed_at| {
                    now.signed_duration_since(observed_at)
                        .num_milliseconds()
                        .max(0) as u64
                        >= state.config.maximum_book_age_ms
                })
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    quarantine_token_ids.extend(missing_quarantine_token_ids.iter().cloned());
    let quarantine_token_ids = quarantine_token_ids.into_iter().collect::<Vec<_>>();
    drop(runtime);
    if state
        .active_polymarket_scope
        .load_full()
        .is_none_or(|active| active.scope_id != live.scope_id)
    {
        bail!("Polymarket book validation completed after a scope transition");
    }
    let mut verified_at = validations
        .iter()
        .filter(|validation| validation.matches_projection)
        .map(|validation| (validation.token_id.clone(), validation.received_at))
        .collect::<BTreeMap<_, _>>();
    if let Some(previous) = previous_verified_at {
        verified_at.extend(
            missing
                .difference(&missing_quarantine_token_ids)
                .filter_map(|token_id| {
                    previous
                        .get(token_id)
                        .copied()
                        .map(|verified_at| (token_id.clone(), verified_at))
                }),
        );
    }
    let matched_books = validations
        .iter()
        .filter(|validation| validation.matches_projection)
        .count();
    let mismatched_books = validations
        .iter()
        .filter(|validation| !validation.matches_projection)
        .count();
    let evidence_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&json!({
        "validations":validations,
        "missing_token_ids":missing,
    }))?));
    state.audit.record_lifecycle(
        "polymarket_book_validation",
        "observed",
        json!({
            "scope_id":live.scope_id,
            "generation":live.universe.as_ref().map(|universe| universe.generation),
            "projection_cursor":projection_cursor,
            "observed_books":validations.len(),
            "matched_books":matched_books,
            "mismatched_books":mismatched_books,
            "missing_books":missing.len(),
            "missing_token_ids":missing,
            "quarantine_token_ids":quarantine_token_ids,
            "evidence_sha256":evidence_sha256,
            "projection_mutated":false,
            "public_cursor_advanced":false,
            "real_order_submission_enabled":false
        }),
    )?;
    state
        .polymarket_book_validations
        .store(Arc::new(PolymarketBookValidationProjection {
            scope_id: live.scope_id.clone(),
            verified_at,
        }));
    Ok(PolymarketBookValidationSummary {
        matched_books,
        mismatched_books,
        missing_books: missing.len(),
        quarantine_token_ids,
    })
}

async fn quarantine_polymarket_tokens_from_authoritative_refresh(
    state: &AppState,
    token_ids: &[String],
) -> Result<usize> {
    let projection = state.projection.load_full();
    let frames = token_ids
        .iter()
        .filter(|token_id| {
            projection.market_id_for_token(token_id).is_some()
                && !projection.quarantined_token_ids.contains(*token_id)
        })
        .map(|token_id| {
            let received_at = Utc::now();
            marketcow_polymarket::RawTransportFrame {
                raw_payload: json!({
                    "event_type":"source_gap",
                    "asset_id":token_id,
                    "reason":"authoritative_book_unavailable_after_freshness_deadline",
                    "timestamp":received_at.timestamp_millis().to_string(),
                }),
                received_at,
            }
        })
        .collect::<Vec<_>>();
    drop(projection);
    if frames.is_empty() {
        return Ok(0);
    }
    apply_polymarket_transport_frames(state, frames).await
}

#[derive(Default)]
struct PolymarketMarketRecoverySummary {
    attempted: usize,
    recovered: usize,
    rejected: usize,
    applied_events: usize,
}

async fn recover_quarantined_polymarket_markets(
    state: &AppState,
    live: &PolymarketLiveConfig,
    frames: &[marketcow_polymarket::RawTransportFrame],
) -> Result<PolymarketMarketRecoverySummary> {
    let mut by_token = BTreeMap::new();
    for frame in frames {
        let token_id = frame
            .raw_payload
            .get("asset_id")
            .or_else(|| frame.raw_payload.get("token_id"))
            .and_then(serde_json::Value::as_str)
            .context("authoritative recovery book is missing token identity")?;
        if by_token.insert(token_id.to_owned(), frame).is_some() {
            bail!("authoritative recovery response contains a duplicate token");
        }
    }

    let mut runtime = state.runtime.lock().await;
    if runtime.projection().scope_id != live.scope_id {
        bail!("Polymarket market recovery crossed a scope boundary");
    }
    let projection = runtime.projection();
    let quarantined = projection.quarantined_market_ids();
    let recovery_markets = projection
        .markets
        .values()
        .filter(|market| quarantined.contains(&market.market_id))
        .map(|market| {
            (
                market.market_id.clone(),
                market
                    .outcomes
                    .iter()
                    .map(|outcome| outcome.token_id.clone())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    drop(projection);

    let mut summary = PolymarketMarketRecoverySummary::default();
    let mut published = Vec::new();
    for (market_id, token_ids) in recovery_markets {
        summary.attempted += 1;
        let Some(pair) = token_ids
            .iter()
            .map(|token_id| by_token.get(token_id).copied())
            .collect::<Option<Vec<_>>>()
        else {
            summary.rejected += 1;
            continue;
        };
        let received_at = pair
            .iter()
            .map(|frame| frame.received_at)
            .max()
            .unwrap_or_else(Utc::now);
        match runtime.apply_market_recovery_snapshot(
            pair.iter().map(|frame| frame.raw_payload.clone()).collect(),
            received_at,
        ) {
            Ok(outcomes) => {
                summary.applied_events += outcomes.len();
                if outcomes
                    .last()
                    .is_some_and(|outcome| outcome.projection.market_is_public(&market_id))
                {
                    summary.recovered += 1;
                } else {
                    summary.rejected += 1;
                }
                published.extend(outcomes);
            }
            Err(marketcow_runtime::RuntimeError::InvalidMarketRecovery) => {
                summary.rejected += 1;
            }
            Err(error) => return Err(error.into()),
        }
    }
    state.projection.store(runtime.projection());
    drop(runtime);
    publish_polymarket_outcomes(state, &published);
    if summary.attempted > 0 {
        state.audit.record_lifecycle(
            "polymarket_market_recovery",
            "atomic_snapshot_batch",
            json!({
                "scope_id":live.scope_id,
                "attempted_markets":summary.attempted,
                "recovered_markets":summary.recovered,
                "rejected_markets":summary.rejected,
                "source":"polymarket_clob_http",
                "atomic_two_token_boundary":true,
                "healthy_markets_unchanged":true,
                "real_order_submission_enabled":false
            }),
        )?;
    }
    Ok(summary)
}

fn reset_polymarket_book_validations(state: &AppState, scope_id: &str) {
    state
        .polymarket_book_validations
        .store(Arc::new(PolymarketBookValidationProjection {
            scope_id: scope_id.into(),
            verified_at: BTreeMap::new(),
        }));
}

fn projection_maximum_effective_book_age_ms(
    state: &AppState,
    projection: &marketcow_core::Projection,
) -> u64 {
    let validations = state.polymarket_book_validations.load();
    let active_token_ids = projection.active_token_ids();
    projection
        .books
        .iter()
        .filter(|(token_id, _)| active_token_ids.contains(*token_id))
        .map(|(token_id, book)| {
            let validated_at = (validations.scope_id == projection.scope_id)
                .then(|| validations.verified_at.get(token_id).copied())
                .flatten();
            let observed_at = match (book.source_observed_at, validated_at) {
                (Some(causal), Some(validated)) => Some(causal.max(validated)),
                (causal, validated) => causal.or(validated),
            };
            observed_at.map_or(u64::MAX, |observed_at| {
                Utc::now()
                    .signed_duration_since(observed_at)
                    .num_milliseconds()
                    .max(0) as u64
            })
        })
        .max()
        .unwrap_or(u64::MAX)
}

fn projection_fresh(state: &AppState, projection: &marketcow_core::Projection) -> bool {
    projection_maximum_effective_book_age_ms(state, projection) <= state.config.maximum_book_age_ms
}

fn polymarket_projection_ready(state: &AppState, projection: &marketcow_core::Projection) -> bool {
    let active_market_count = projection.active_market_ids().len();
    projection.ready
        && projection.instrument_ticks_consistent()
        && projection.active_market_books_two_sided()
        && projection_fresh(state, projection)
        && state
            .active_polymarket_scope
            .load()
            .as_ref()
            .is_none_or(|live| {
                let capacity_ready = live
                    .universe
                    .as_ref()
                    .is_none_or(|universe| active_market_count >= universe.minimum_market_count);
                capacity_ready
                    && (live.catalog_frame.is_none()
                        || projection_matches_polymarket_scope(live, projection, true))
            })
}

async fn wait_for_polymarket_retry_or_scope_recovery(
    state: &AppState,
    current: &PolymarketLiveConfig,
    shutdown: &mut watch::Receiver<bool>,
    scope_switches: &mut mpsc::Receiver<PolymarketScopeSwitchRequest>,
    retry_delay: Duration,
) -> Option<PolymarketLiveConfig> {
    let retry = tokio::time::sleep(retry_delay);
    tokio::pin!(retry);
    loop {
        tokio::select! {
            _ = &mut retry => {
                warn!(
                    scope_id=%current.scope_id,
                    retry_delay_ms=retry_delay.as_millis(),
                    "polymarket_transport_retrying_same_scope"
                );
                return Some(current.clone());
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return None;
                }
            }
            request = scope_switches.recv() => {
                let PolymarketScopeSwitchRequest {
                    live,
                    response,
                } = request?;
                let candidate = prepare_polymarket_scope_candidate(state, current, &live).await;
                match match candidate {
                    Ok(runtime) => {
                        commit_polymarket_scope_switch(state, current, live, runtime).await
                    }
                    Err(error) => Err(error),
                } {
                    Ok((activated, receipt)) => {
                        info!(scope_id=%activated.scope_id, boundary_cursor=receipt.boundary_cursor, "polymarket_scope_recovered");
                        let _ = response.send(Ok(receipt));
                        return Some(activated);
                    }
                    Err(error) => {
                        let _ = response.send(Err(error));
                    }
                }
            }
        }
    }
}

async fn checkpoint_polymarket_runtime(state: &AppState) -> Result<()> {
    let runtime = state.runtime.clone();
    let projection = tokio::task::spawn_blocking(move || {
        let mut runtime = runtime.blocking_lock();
        runtime.checkpoint()?;
        Ok::<_, marketcow_runtime::RuntimeError>(runtime.projection())
    })
    .await
    .map_err(|error| anyhow::anyhow!("polymarket checkpoint task failed: {error}"))??;
    state.projection.store(projection);
    Ok(())
}

// The pending bounded-buffer runtime returns VecDeque while the committed baseline returns a
// slice. Iteration keeps this live-owner commit source-compatible with both representations.
#[allow(clippy::iter_cloned_collect)]
fn clone_polymarket_recent_events(
    runtime: &marketcow_runtime::PolymarketRuntime,
) -> Vec<marketcow_core::PersistedEvent> {
    runtime.recent_events().iter().cloned().collect()
}

fn expired_dynamic_scope_tokens(config: &Config, observed_at: DateTime<Utc>) -> Vec<String> {
    config
        .polymarket_live
        .as_ref()
        .and_then(|live| live.universe.as_ref())
        .into_iter()
        .flat_map(|universe| &universe.active_markets)
        .filter(|market| market.end_at <= observed_at)
        .flat_map(|market| market.token_ids.iter().cloned())
        .collect()
}

async fn serve() -> Result<()> {
    let config = Config::load()?;
    preflight(&config)?;
    let audit = Arc::new(AuditCoordinator::open(&config).await?);
    let control_plane = Arc::new(ControlPlaneCoordinator::open(&config).await?);
    let mut runtime = marketcow_runtime::PolymarketRuntime::open(runtime_config(
        &config,
        &control_plane.config_revision,
    ))?;
    seed_polymarket_scope_catalog(&mut runtime, config.polymarket_live.as_ref())?;
    let projection = runtime.projection();
    let jobs = Arc::new(
        DurableJobCoordinator::open(
            &config.profile,
            config.python_workers.dispatch_policies.clone(),
        )
        .await?,
    );
    let instruments = Arc::new(InstrumentCoordinator::open(&config.profile).await?);
    let market_data = Arc::new(MarketDataCoordinator::open(&config.profile).await?);
    let canonical_cursor = Arc::new(CanonicalCursorSigner::open(&config.storage_root)?);
    let (service_shutdown_tx, service_shutdown_rx) = watch::channel(false);
    let hyperliquid = start_hyperliquid_shadow(
        &config,
        &control_plane.config_revision,
        audit.clone(),
        service_shutdown_rx.clone(),
    )?;
    let legacy_mcp = config
        .legacy_mcp_url
        .clone()
        .map(LegacyMcpProxy::new)
        .transpose()?;
    let (stream, _) = broadcast::channel(STREAM_CHANNEL_CAPACITY);
    // The single live owner serializes candidate preparation and the final swap. Retain at most
    // one queued follow-up request; a larger generation backlog would be stale by construction.
    let (polymarket_scope_switch_tx, polymarket_scope_switch_rx) = mpsc::channel(1);
    let active_polymarket_scope = Arc::new(ArcSwapOption::from(
        config.polymarket_live.clone().map(Arc::new),
    ));
    let initial_validation_scope = config
        .polymarket_live
        .as_ref()
        .map_or_else(|| config.scope_id.clone(), |live| live.scope_id.clone());
    let state = AppState {
        config: config.clone(),
        audit,
        metrics: Arc::new(Metrics::default()),
        projection: Arc::new(ArcSwap::from(projection)),
        runtime: Arc::new(AsyncMutex::new(runtime)),
        active_polymarket_scope,
        polymarket_book_validations: Arc::new(ArcSwap::from_pointee(
            PolymarketBookValidationProjection {
                scope_id: initial_validation_scope,
                verified_at: BTreeMap::new(),
            },
        )),
        polymarket_scope_switch: config
            .polymarket_live
            .as_ref()
            .map(|_| polymarket_scope_switch_tx),
        jobs,
        instruments,
        market_data,
        canonical_cursor,
        control_plane,
        stream,
        hyperliquid_shadow: hyperliquid.as_ref().map(|runtime| runtime.reader.clone()),
        hyperliquid_stream: hyperliquid.as_ref().map(|runtime| runtime.stream.clone()),
        hyperliquid_queues: hyperliquid.as_ref().map(|runtime| runtime.queues.clone()),
        worker_status: Arc::new(PythonWorkerStatus::default()),
        legacy_mcp,
    };
    let expired_tokens = expired_dynamic_scope_tokens(&config, Utc::now());
    if !expired_tokens.is_empty() {
        let quarantined =
            quarantine_polymarket_tokens_from_authoritative_refresh(&state, &expired_tokens)
                .await?;
        warn!(
            expired_token_count = expired_tokens.len(),
            quarantined_event_count = quarantined,
            "polymarket_expired_startup_members_quarantined"
        );
    }
    let polymarket_live = start_polymarket_live(
        state.clone(),
        service_shutdown_rx,
        polymarket_scope_switch_rx,
    );
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
    info!(bind=%config.bind, role=%config.role, shadow=config.shadow_mode, "marketcowd_ready");
    let shutdown_sender = service_shutdown_tx.clone();
    let server_result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown().await;
            let _ = shutdown_sender.send(true);
        })
        .await;
    let _ = service_shutdown_tx.send(true);
    if let Some(worker_pool) = worker_pool {
        worker_pool.abort();
        let _ = worker_pool.await;
    }
    worker.abort();
    let _ = worker.await;
    let _ = fs::remove_file(&config.worker_socket);
    if let Some(polymarket_live) = polymarket_live {
        let _ = polymarket_live.await;
    }
    if let Some(runtime) = hyperliquid {
        let HyperliquidShadowRuntime {
            reader: _,
            stream: _,
            queues,
            transport,
            owner,
            gateway,
        } = runtime;
        drop(queues);
        let _ = transport.await;
        let _ = owner.await;
        let _ = gateway.await;
    }
    info!("marketcowd_stopped");
    server_result?;
    Ok(())
}

fn runtime_config(config: &Config, config_revision: &str) -> marketcow_runtime::RuntimeConfig {
    let root = if let Some(universe) = config
        .polymarket_live
        .as_ref()
        .and_then(|live| live.universe.as_ref())
    {
        config
            .storage_root
            .join("polymarket-universes")
            .join(&universe.universe_id)
            .join(format!("generation-{:020}", universe.generation))
    } else if config
        .polymarket_live
        .as_ref()
        .is_some_and(|live| live.scope_file_sha256.is_some())
    {
        config
            .storage_root
            .join("polymarket-scopes")
            .join(&config.scope_id)
    } else {
        config.storage_root.join("polymarket")
    };
    marketcow_runtime::RuntimeConfig {
        root,
        scope_id: config.scope_id.clone(),
        config_revision: config_revision.into(),
        wal_segment_bytes: 256 * 1024 * 1024,
        recent_event_capacity: POLYMARKET_RECENT_EVENT_CAPACITY,
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
    let mut persistence_latency_us = Vec::new();
    let mut publication_latency_us = Vec::new();
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
        let outcomes = runtime.apply_raw(raw, now)?;
        if let Some(maximum) = outcomes
            .iter()
            .map(|outcome| outcome.persistence_latency_us)
            .max()
        {
            persistence_latency_us.push(maximum);
        }
        if let Some(maximum) = outcomes
            .iter()
            .map(|outcome| outcome.publication_latency_us)
            .max()
        {
            publication_latency_us.push(maximum);
        }
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
        if let Some(maximum) = outcomes
            .iter()
            .map(|outcome| outcome.persistence_latency_us)
            .max()
        {
            persistence_latency_us.push(maximum);
        }
        if let Some(maximum) = outcomes
            .iter()
            .map(|outcome| outcome.publication_latency_us)
            .max()
        {
            publication_latency_us.push(maximum);
        }
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
    persistence_latency_us.sort_unstable();
    publication_latency_us.sort_unstable();
    let percentile = |values: &[u64], ratio: f64| -> u64 {
        if values.is_empty() {
            return u64::MAX;
        }
        let index = ((values.len() as f64 * ratio).ceil() as usize)
            .saturating_sub(1)
            .min(values.len() - 1);
        values[index]
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
        "apply_latency_p99_us_lte_20000":percentile(&apply_latency_us, 0.99) <= 20_000,
        "wal_persistence_latency_p99_us_lte_20000":
            percentile(&persistence_latency_us, 0.99) <= 20_000,
        "projection_publication_latency_p99_us_lte_5000":
            percentile(&publication_latency_us, 0.99) <= 5_000,
        "real_orders_disabled":!config.real_order_submission_enabled,
        "tradude_does_not_manage_marketcow":true
    });
    let passed = gate_verdicts
        .as_object()
        .expect("gate verdicts are an object")
        .values()
        .all(|value| value == &serde_json::Value::Bool(true));
    let result = json!({
        "schema_version":"marketcow.headless-shadow-soak.v3",
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
        "apply_latency_us":{"p50":percentile(&apply_latency_us, 0.50),
            "p95":percentile(&apply_latency_us, 0.95),"p99":percentile(&apply_latency_us, 0.99),
            "max":apply_latency_us.last().copied().unwrap_or(u64::MAX)},
        "persistence_latency_us":{"p50":percentile(&persistence_latency_us, 0.50),
            "p95":percentile(&persistence_latency_us, 0.95),
            "p99":percentile(&persistence_latency_us, 0.99),
            "max":persistence_latency_us.last().copied().unwrap_or(u64::MAX)},
        "publication_latency_us":{"p50":percentile(&publication_latency_us, 0.50),
            "p95":percentile(&publication_latency_us, 0.95),
            "p99":percentile(&publication_latency_us, 0.99),
            "max":publication_latency_us.last().copied().unwrap_or(u64::MAX)},
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
        .route(
            "/v1/prediction-markets/polymarket/live/health",
            get(polymarket_live_health),
        )
        .route("/v1/prediction-markets/polymarket/live/scope", get(scope))
        .route(
            "/v1/prediction-markets/polymarket/live/snapshot",
            get(live_snapshot),
        )
        .route(
            "/v1/prediction-markets/polymarket/live/markets/{market_id}/snapshot",
            get(live_market_snapshot),
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
        .route(
            "/v1/market-data/providers/hyperliquid/shadow/snapshot",
            get(hyperliquid_shadow_snapshot),
        )
        .route(
            "/v1/market-data/providers/hyperliquid/shadow/events",
            get(hyperliquid_shadow_events),
        )
        .route(
            "/v1/market-data/providers/hyperliquid/shadow/stream",
            get(hyperliquid_shadow_stream),
        )
        .route("/v1/instruments:resolve", get(resolve_instrument))
        .route(
            "/v1/instruments:resolve/query",
            post(resolve_instruments_batch),
        )
        .route("/v1/instruments/{instrument_id}", get(get_instrument))
        .route("/v1/quotes/query", post(quotes_query))
        .route("/v1/canonical-bars/{instrument_id}", get(canonical_bars))
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
            "/v1/admin/polymarket/scope:activate",
            axum::routing::post(admin_activate_polymarket_scope),
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
    let hyperliquid_projection = state
        .hyperliquid_shadow
        .as_ref()
        .map(marketcow_realtime::RealtimeHubReader::snapshot);
    let hyperliquid_ready = hyperliquid_projection.as_ref().is_none_or(|projection| {
        matches!(
            projection.health,
            marketcow_realtime::RealtimeHubHealth::Ready { .. }
        )
    });
    let projection = state.projection.load_full();
    let polymarket_ready = polymarket_projection_ready(state, &projection);
    json!({
        "status":if hyperliquid_ready && polymarket_ready { "healthy" } else { "degraded" }, "service":"marketcowd", "profile":state.config.profile,
        "role":state.config.role,
        "shadow_mode":state.config.shadow_mode, "real_order_submission_enabled":false,
        "mcp":{
            "enabled":true,
            "endpoint":"/mcp",
            "native_tools":4,
            "legacy_proxy_configured":state.legacy_mcp.is_some()
        },
        "components":{
            "api":"healthy","wal":"healthy","python_workers":worker_health,
            "hyperliquid_shadow":hyperliquid_projection.as_ref().map_or_else(
                || json!({"enabled":false,"status":"disabled_optional"}),
                |projection| json!({
                    "enabled":true,
                    "health":projection.health,
                    "stream_id":projection.stream_id,
                    "public_sequence":projection.public_sequence,
                    "wal_cursor":projection.wal_cursor,
                    "transport_queue_depth":state.hyperliquid_queues.as_ref().map_or(0, HyperliquidQueueProbe::transport_depth),
                    "gateway_queue_depth":state.hyperliquid_queues.as_ref().map_or(0, HyperliquidQueueProbe::gateway_depth),
                    "public_channel_depth":state.hyperliquid_stream.as_ref().map_or(0, broadcast::Sender::len)
                })
            ),
            "job_persistence":if state.jobs.persistence_enabled() { "healthy" } else { "degraded_development_only" },
            "instrument_persistence":if state.instruments.persistence_enabled() { "healthy" } else { "degraded_development_only" },
            "market_data_persistence":if state.market_data.persistence_enabled() { "healthy" } else { "degraded_development_only" },
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

async fn polymarket_live_health(State(state): State<AppState>) -> Response {
    let projection = state.projection.load_full();
    let ready = polymarket_projection_ready(&state, &projection);
    let active_market_ids = projection.active_market_ids();
    let active_token_ids = projection.active_token_ids();
    let book_token_count = active_token_ids
        .iter()
        .filter(|token_id| projection.books.contains_key(*token_id))
        .count();
    let maximum_book_age_ms = projection_maximum_effective_book_age_ms(&state, &projection);
    let reasons = if ready {
        Vec::<String>::new()
    } else {
        vec![
            projection
                .fail_closed_reason
                .clone()
                .unwrap_or_else(|| "polymarket_projection_unready_or_stale".into()),
        ]
    };
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "schema_version":"marketcow.polymarket.live-read-health.v1",
            "status":if ready { "index_ready" } else { "degraded" },
            "catalog_revision":state.active_polymarket_scope.load().as_ref()
                .and_then(|value| value.catalog_revision.clone()),
            "catalog_index_ready":true,
            "latest_state_ready":ready,
            "market_count":active_market_ids.len(),
            "token_count":active_token_ids.len(),
            "book_token_count":book_token_count,
            "book_complete_market_count":active_market_ids.len()
                .saturating_sub(projection.quarantined_market_ids().len()),
            "active_market_count":active_market_ids.len(),
            "terminal_market_count":0,
            "complete_market_count":if ready { active_market_ids.len() } else { 0 },
            "missing_market_count":if ready { 0 } else { active_market_ids.len() },
            "scope_status":if ready { "exact_ready" } else { "data_degraded" },
            "scope_id":projection.scope_id,
            "unresolved_gap_count":projection.unresolved_gaps.len(),
            "latest_cursor":projection.cursor,
            "persisted_cursor":projection.persisted_cursor,
            "persistence_lag_events":projection.cursor.saturating_sub(projection.persisted_cursor),
            "persistence_queue_depth":0,
            "derived_index_error":null,
            "live_stream_connected":true,
            "live_stream_disconnect_count":state.metrics.disconnects.load(Ordering::Relaxed),
            "event_loop_stall_max_ms":0,
            "events_read_source":"memory_projection",
            "realtime_sqlite_query_ms":0,
            "reason_codes":reasons,
            "source_policy":"official_free_only",
            "projection_generation":projection.generation,
            "scope_market_ids":active_market_ids,
            "freshness_checked_at":Utc::now(),
            "oldest_book_received_at":null,
            "maximum_book_age_ms":maximum_book_age_ms,
            "authoritative_owner":"marketcowd",
        })),
    )
        .into_response()
}

async fn hyperliquid_shadow_snapshot(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
) -> Response {
    match &state.hyperliquid_shadow {
        Some(reader) => Json(reader.snapshot()).into_response(),
        None => error(
            StatusCode::NOT_FOUND,
            "hyperliquid_shadow_disabled",
            false,
            &request_id,
        ),
    }
}

#[derive(Debug, Deserialize)]
struct HyperliquidEventQuery {
    after_sequence: Option<u64>,
    #[serde(default = "default_event_limit")]
    limit: usize,
    instruments: Option<String>,
    data_types: Option<String>,
}

fn hyperliquid_filter(
    state: &AppState,
    query: &HyperliquidEventQuery,
) -> Result<marketcow_realtime::SubscriptionFilter, &'static str> {
    let configured = state
        .config
        .hyperliquid_shadow
        .as_ref()
        .ok_or("hyperliquid_shadow_disabled")?;
    let instruments = match &query.instruments {
        None => configured.instruments.keys().cloned().collect(),
        Some(value) => {
            if value.is_empty() || value.len() > 16_384 {
                return Err("invalid_hyperliquid_instruments");
            }
            let instruments = value.split(',').map(str::to_owned).collect::<BTreeSet<_>>();
            if instruments.is_empty()
                || instruments.len() > 1_000
                || instruments
                    .iter()
                    .any(|instrument| !configured.instruments.contains_key(instrument))
            {
                return Err("invalid_hyperliquid_instruments");
            }
            instruments
        }
    };
    let all_data_types = BTreeSet::from([
        marketcow_realtime::DataType::Quote,
        marketcow_realtime::DataType::Trade,
        marketcow_realtime::DataType::OrderBook,
        marketcow_realtime::DataType::AssetContext,
    ]);
    let data_types = match &query.data_types {
        None => all_data_types,
        Some(value) => {
            if value.is_empty() || value.len() > 256 {
                return Err("invalid_hyperliquid_data_types");
            }
            let parsed = value
                .split(',')
                .map(|name| match name {
                    "quote" => Ok(marketcow_realtime::DataType::Quote),
                    "trade" => Ok(marketcow_realtime::DataType::Trade),
                    "order_book" => Ok(marketcow_realtime::DataType::OrderBook),
                    "asset_context" => Ok(marketcow_realtime::DataType::AssetContext),
                    _ => Err("invalid_hyperliquid_data_types"),
                })
                .collect::<Result<BTreeSet<_>, _>>()?;
            if parsed.is_empty() {
                return Err("invalid_hyperliquid_data_types");
            }
            parsed
        }
    };
    Ok(marketcow_realtime::SubscriptionFilter {
        instruments,
        data_types,
    })
}

fn hyperliquid_ready_snapshot(
    state: &AppState,
) -> Result<Arc<marketcow_realtime::RealtimeHubProjection>, &'static str> {
    let snapshot = state
        .hyperliquid_shadow
        .as_ref()
        .ok_or("hyperliquid_shadow_disabled")?
        .snapshot();
    if !matches!(
        snapshot.health,
        marketcow_realtime::RealtimeHubHealth::Ready { .. }
    ) {
        return Err("hyperliquid_shadow_unready");
    }
    Ok(snapshot)
}

fn hyperliquid_query_error(code: &'static str, request_id: &str) -> Response {
    let status = if code == "hyperliquid_shadow_disabled" {
        StatusCode::NOT_FOUND
    } else if code == "hyperliquid_shadow_unready" {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::UNPROCESSABLE_ENTITY
    };
    error(status, code, status.is_server_error(), request_id)
}

async fn hyperliquid_shadow_events(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    Query(query): Query<HyperliquidEventQuery>,
) -> Response {
    if !(1..=1_000).contains(&query.limit) {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_event_page_limit",
            false,
            &request_id,
        );
    }
    let filter = match hyperliquid_filter(&state, &query) {
        Ok(filter) => filter,
        Err(code) => return hyperliquid_query_error(code, &request_id),
    };
    let snapshot = match hyperliquid_ready_snapshot(&state) {
        Ok(snapshot) => snapshot,
        Err(code) => return hyperliquid_query_error(code, &request_id),
    };
    let after = query.after_sequence.unwrap_or(0);
    match snapshot.replay_after(&snapshot.stream_id, after, &filter) {
        Ok(mut frames) => {
            frames.truncate(query.limit);
            let next_sequence = frames.last().map_or(after, |frame| match frame {
                marketcow_realtime::ReplayFrame::Event { stream } => stream.sequence,
                marketcow_realtime::ReplayFrame::SequenceWatermark { sequence, .. } => *sequence,
            });
            Json(json!({
                "schema_version":"marketcow.realtime.events-page.v1",
                "stream_id":snapshot.stream_id,
                "current_sequence":snapshot.public_sequence,
                "next_sequence":next_sequence,
                "frames":frames,
                "real_order_submission_enabled":false
            }))
            .into_response()
        }
        Err(marketcow_realtime::RealtimeError::GapUnrecoverable { .. }) => error(
            StatusCode::GONE,
            "hyperliquid_cursor_expired",
            true,
            &request_id,
        ),
        Err(marketcow_realtime::RealtimeError::CursorAhead { .. }) => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "hyperliquid_cursor_ahead",
            false,
            &request_id,
        ),
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "hyperliquid_replay_failed_closed",
            true,
            &request_id,
        ),
    }
}

async fn hyperliquid_shadow_stream(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    Query(query): Query<HyperliquidEventQuery>,
) -> Response {
    hyperliquid_stream_response(ws, state, request_id, query).await
}

async fn hyperliquid_stream_response(
    ws: WebSocketUpgrade,
    state: AppState,
    request_id: String,
    query: HyperliquidEventQuery,
) -> Response {
    let filter = match hyperliquid_filter(&state, &query) {
        Ok(filter) => filter,
        Err(code) => return hyperliquid_query_error(code, &request_id),
    };
    let Some(stream) = &state.hyperliquid_stream else {
        return hyperliquid_query_error("hyperliquid_shadow_disabled", &request_id);
    };
    let receiver = stream.subscribe();
    let snapshot = match hyperliquid_ready_snapshot(&state) {
        Ok(snapshot) => snapshot,
        Err(code) => return hyperliquid_query_error(code, &request_id),
    };
    let after = query.after_sequence.unwrap_or(snapshot.public_sequence);
    let replay = match snapshot.replay_after(&snapshot.stream_id, after, &filter) {
        Ok(replay) => replay,
        Err(marketcow_realtime::RealtimeError::GapUnrecoverable { .. }) => {
            return error(
                StatusCode::GONE,
                "hyperliquid_cursor_expired",
                true,
                &request_id,
            );
        }
        Err(marketcow_realtime::RealtimeError::CursorAhead { .. }) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "hyperliquid_cursor_ahead",
                false,
                &request_id,
            );
        }
        Err(_) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "hyperliquid_replay_failed_closed",
                true,
                &request_id,
            );
        }
    };
    ws.on_upgrade(move |socket| {
        serve_hyperliquid_shadow_stream(socket, state, receiver, snapshot, replay, filter, after)
    })
}

async fn send_hyperliquid_frame(socket: &mut WebSocket, value: &serde_json::Value) -> bool {
    let Ok(encoded) = serde_json::to_string(value) else {
        return false;
    };
    matches!(
        tokio::time::timeout(
            STREAM_SEND_TIMEOUT,
            socket.send(Message::Text(encoded.into()))
        )
        .await,
        Ok(Ok(()))
    )
}

fn realtime_frame_json(frame: marketcow_realtime::ReplayFrame) -> serde_json::Value {
    serde_json::to_value(frame).expect("realtime frame is serializable")
}

async fn serve_hyperliquid_shadow_stream(
    mut socket: WebSocket,
    state: AppState,
    mut receiver: broadcast::Receiver<marketcow_realtime::StreamEvent>,
    snapshot: Arc<marketcow_realtime::RealtimeHubProjection>,
    replay: Vec<marketcow_realtime::ReplayFrame>,
    filter: marketcow_realtime::SubscriptionFilter,
    after: u64,
) {
    if !send_hyperliquid_frame(
        &mut socket,
        &json!({
            "type":"subscription",
            "schema_version":"marketcow.realtime.subscription.v1",
            "provider":"hyperliquid",
            "stream_id":snapshot.stream_id,
            "current_sequence":snapshot.public_sequence,
            "resumed":after < snapshot.public_sequence,
            "real_order_submission_enabled":false
        }),
    )
    .await
    {
        return;
    }
    let mut last_sequence = after;
    for frame in replay {
        last_sequence = match &frame {
            marketcow_realtime::ReplayFrame::Event { stream } => stream.sequence,
            marketcow_realtime::ReplayFrame::SequenceWatermark { sequence, .. } => *sequence,
        };
        if !send_hyperliquid_frame(&mut socket, &realtime_frame_json(frame)).await {
            return;
        }
    }
    let mut health_check = tokio::time::interval(Duration::from_secs(1));
    health_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            message = socket.recv() => match message {
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                Some(Ok(Message::Text(_) | Message::Binary(_))) => {
                    close_stream(&mut socket, close_code::UNSUPPORTED, "server_push_only").await;
                    return;
                }
            },
            event = receiver.recv() => match event {
                Ok(event) if event.sequence <= last_sequence => {}
                Ok(event) if event.sequence == last_sequence.saturating_add(1) => {
                    last_sequence = event.sequence;
                    let frame = if filter.matches(&event.event) {
                        marketcow_realtime::ReplayFrame::Event { stream: Box::new(event) }
                    } else {
                        marketcow_realtime::ReplayFrame::SequenceWatermark {
                            stream_id: event.stream_id,
                            sequence: event.sequence,
                        }
                    };
                    if !send_hyperliquid_frame(&mut socket, &realtime_frame_json(frame)).await {
                        return;
                    }
                }
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                    let _ = send_hyperliquid_frame(&mut socket, &json!({
                        "type":"resync_required","reason":"hyperliquid_stream_gap",
                        "last_sequence":last_sequence
                    })).await;
                    close_stream(&mut socket, close_code::POLICY, "full_sync_required").await;
                    return;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    close_stream(&mut socket, close_code::RESTART, "server_shutdown").await;
                    return;
                }
            },
            _ = health_check.tick() => {
                if hyperliquid_ready_snapshot(&state).is_err() {
                    let _ = send_hyperliquid_frame(&mut socket, &json!({
                        "type":"resync_required","reason":"hyperliquid_shadow_unready",
                        "last_sequence":last_sequence
                    })).await;
                    close_stream(&mut socket, close_code::POLICY, "full_sync_required").await;
                    return;
                }
            }
        }
    }
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
#[serde(deny_unknown_fields)]
struct QuoteQueryRequest {
    symbols: Vec<String>,
    #[serde(default)]
    refresh: bool,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    allow_fallback: bool,
}

async fn quotes_query(
    State(state): State<AppState>,
    Json(request): Json<QuoteQueryRequest>,
) -> Response {
    if request.symbols.is_empty() || request.symbols.len() > 20 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"detail":{"code":"invalid_symbols","message":"symbols must contain between 1 and 20 values"}})),
        )
            .into_response();
    }
    if request.refresh || request.provider.is_some() || request.allow_fallback {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"detail":{
                "code":"cached_quotes_only",
                "message":"the Rust quote boundary does not call upstream providers"
            }})),
        )
            .into_response();
    }
    let mut symbols = Vec::with_capacity(request.symbols.len());
    for raw in request.symbols {
        let symbol = raw.trim().to_ascii_uppercase();
        if symbol.is_empty()
            || symbol.len() > 64
            || symbol
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"detail":{"code":"invalid_quote_symbol","symbol":raw}})),
            )
                .into_response();
        }
        symbols.push(symbol);
    }
    let unique = symbols
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let payloads = match state.market_data.latest_payloads(&unique).await {
        Ok(payloads) => payloads,
        Err(marketcow_storage::RepositoryError::InvalidInput) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"detail":{"code":"invalid_quote_query"}})),
            )
                .into_response();
        }
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"detail":{"code":"quote_repository_unavailable"}})),
            )
                .into_response();
        }
    };
    let mut items = Vec::new();
    let mut errors = Vec::new();
    for symbol in symbols {
        if let Some(payload) = payloads.get(&symbol) {
            items.push(payload.clone());
        } else {
            errors.push(json!({
                "symbol":symbol,
                "status":"unavailable",
                "error":"cached quote not found"
            }));
        }
    }
    Json(json!({"count":items.len(),"items":items,"errors":errors})).into_response()
}

#[derive(Deserialize)]
struct CanonicalBarsQuery {
    start: String,
    end: String,
    interval: String,
    adjustment: String,
    page_size: u32,
    cursor: Option<String>,
}

fn canonical_query_error(message: impl Into<String>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"detail":{
            "code":"invalid_canonical_query",
            "message":message.into()
        }})),
    )
        .into_response()
}

fn canonical_time(value: &chrono::DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Millis, true)
}

async fn canonical_bars(
    State(state): State<AppState>,
    AxumPath(instrument_id): AxumPath<String>,
    Query(query): Query<CanonicalBarsQuery>,
) -> Response {
    let instrument = match instrument_lookup(&state, &instrument_id).await {
        Ok(instrument) => instrument,
        Err((status, detail)) => {
            return (status, Json(json!({"detail":detail}))).into_response();
        }
    };
    let start = match DateTime::parse_from_rfc3339(&query.start) {
        Ok(value) => value.with_timezone(&Utc),
        Err(_) => {
            return canonical_query_error("start must be a timezone-aware RFC 3339 timestamp");
        }
    };
    let end = match DateTime::parse_from_rfc3339(&query.end) {
        Ok(value) => value.with_timezone(&Utc),
        Err(_) => return canonical_query_error("end must be a timezone-aware RFC 3339 timestamp"),
    };
    if start > end || !(1..=1_000).contains(&query.page_size) {
        return canonical_query_error("start/end must be ordered and page_size must be 1..1000");
    }
    let (storage_interval, interval_seconds) = match query.interval.as_str() {
        "1-MINUTE" => ("1m", 60),
        "5-MINUTE" => ("5m", 300),
        "15-MINUTE" => ("15m", 900),
        "30-MINUTE" => ("30m", 1_800),
        "1-HOUR" => ("1h", 3_600),
        "1-DAY" => ("1d", 86_400),
        _ => return canonical_query_error("interval is not supported by schema v1"),
    };
    if !matches!(query.adjustment.as_str(), "raw" | "qfq" | "hfq") {
        return canonical_query_error("adjustment must be raw, qfq or hfq");
    }
    let storage_symbol = if instrument.market == "CRYPTO" {
        instrument.instrument_id.clone()
    } else {
        instrument.symbol.clone()
    };
    let mut storage_query = marketcow_storage::CanonicalPageQuery {
        symbol: storage_symbol,
        interval: storage_interval.into(),
        adjustment: query.adjustment.clone(),
        start,
        end,
        page_size: query.page_size,
        after: None,
    };
    let identity = match state.market_data.canonical_identity(&storage_query).await {
        Ok(identity) => identity,
        Err(marketcow_storage::RepositoryError::InvalidInput) => {
            return canonical_query_error("invalid canonical storage query");
        }
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"detail":{"code":"canonical_repository_unavailable"}})),
            )
                .into_response();
        }
    };
    let canonical_instrument_id = instrument.instrument_id.clone();
    let binding = CanonicalCursorBinding {
        instrument_id: canonical_instrument_id.clone(),
        start: canonical_time(&start),
        end: canonical_time(&end),
        interval: query.interval.clone(),
        adjustment: query.adjustment.clone(),
        page_size: query.page_size,
        snapshot_id: identity.snapshot_id.clone(),
    };
    let now = Utc::now().timestamp();
    if let Some(cursor) = query.cursor.as_deref() {
        let after_ms = match state.canonical_cursor.decode(cursor, &binding, now) {
            Ok(after_ms) => after_ms,
            Err(error) => return canonical_query_error(error.to_string()),
        };
        let Some(after) = DateTime::from_timestamp_millis(after_ms) else {
            return canonical_query_error("canonical cursor position is invalid");
        };
        if after < start || after > end {
            return canonical_query_error("canonical cursor position is outside the query range");
        }
        storage_query.after = Some(after);
    }
    let (records, has_more) = match state.market_data.canonical_page(&storage_query).await {
        Ok(result) => result,
        Err(marketcow_storage::RepositoryError::InvalidInput) => {
            return canonical_query_error("invalid canonical storage page");
        }
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"detail":{"code":"canonical_repository_unavailable"}})),
            )
                .into_response();
        }
    };
    let confirmed = match state.market_data.canonical_identity(&storage_query).await {
        Ok(identity) => identity,
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"detail":{"code":"canonical_repository_unavailable"}})),
            )
                .into_response();
        }
    };
    if confirmed != identity {
        return (
            StatusCode::CONFLICT,
            Json(json!({"detail":{
                "code":"canonical_snapshot_changed",
                "message":"canonical data changed during page read; restart query"
            }})),
        )
            .into_response();
    }
    let bars = records
        .iter()
        .map(|record| {
            let window_end = record.bar_time + chrono::Duration::seconds(interval_seconds);
            json!({
                "schema_version":1,
                "instrument_id":canonical_instrument_id,
                "interval":query.interval,
                "adjustment":query.adjustment,
                "price_type":"LAST",
                "aggregation_source":"EXTERNAL",
                "window_start":canonical_time(&record.bar_time),
                "window_end":canonical_time(&window_end),
                "ts_event":canonical_time(&window_end),
                "ts_init":canonical_time(&record.ingested_at),
                "open":record.open,
                "high":record.high,
                "low":record.low,
                "close":record.close,
                "volume":record.volume,
                "factor_applicability":record.factor_applicability,
                "corporate_action_factor":record.corporate_action_factor,
                "applied_adjustment_multiplier":record.applied_adjustment_multiplier,
                "adjustment_reference_date":record.adjustment_reference_date,
                "reference_factor":record.reference_factor,
                "factor_source":record.factor_source,
                "factor_artifact_id":record.factor_artifact_id,
                "factor_as_of":record.factor_as_of.map(|value| canonical_time(&value)),
                "selected_source":record.selected_source,
                "quality_status":record.quality_status,
                "row_version":record.version.to_string()
            })
        })
        .collect::<Vec<_>>();
    let next_cursor = if has_more {
        records.last().and_then(|record| {
            state
                .canonical_cursor
                .encode(binding, record.bar_time.timestamp_millis(), now)
                .ok()
        })
    } else {
        None
    };
    if has_more && next_cursor.is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"detail":{"code":"canonical_cursor_unavailable"}})),
        )
            .into_response();
    }
    let dataset_payload = serde_json::to_vec(&json!({
        "instrument_id":canonical_instrument_id,
        "interval":query.interval,
        "adjustment":query.adjustment,
        "start":canonical_time(&start),
        "end":canonical_time(&end)
    }))
    .expect("canonical dataset identity serializes");
    let dataset_hash = hex::encode(Sha256::digest(dataset_payload));
    Json(json!({
        "schema_version":1,
        "manifest":{
            "schema_version":1,
            "adjustment_contract_version":1,
            "dataset_id":&dataset_hash[..24],
            "snapshot_id":identity.snapshot_id,
            "canonical_version":identity.canonical_version,
            "instruments":[canonical_instrument_id],
            "interval":query.interval,
            "adjustment":query.adjustment,
            "start":canonical_time(&start),
            "end":canonical_time(&end),
            "end_inclusive":true,
            "row_count":identity.row_count,
            "content_hash":identity.content_hash
        },
        "count":bars.len(),
        "bars":bars,
        "page_size":query.page_size,
        "next_cursor":next_cursor,
        "truncated":has_more,
        "provenance":{"layer":"canonical","backend":"clickhouse"}
    }))
    .into_response()
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
    let native_quotes = marketcow_contracts::mcp_get_quotes_tool_definition();
    let native_canonical = marketcow_contracts::mcp_get_canonical_bars_tool_definition();
    let Some(proxy) = &state.legacy_mcp else {
        return mcp_result(
            request_id,
            json!({"tools":[native_health,native_instrument,native_quotes,native_canonical]}),
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
                Some("get_quotes") => native_quotes.clone(),
                Some("get_canonical_bars") => native_canonical.clone(),
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
    if name == Some("get_quotes") {
        let arguments = arguments.as_object().expect("arguments were validated");
        let mut unexpected = arguments
            .keys()
            .filter(|key| key.as_str() != "symbols")
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
        let Some(values) = arguments
            .get("symbols")
            .and_then(serde_json::Value::as_array)
        else {
            let detail = if arguments.contains_key("symbols") {
                "symbols must be an array containing between 1 and 20 non-empty strings"
            } else {
                "missing required argument(s): symbols"
            };
            return mcp_result(
                request_id,
                mcp_tool_result(json!({"error":"invalid_tool_input","detail":detail}), true),
            );
        };
        if values.is_empty() || values.len() > 20 {
            return mcp_result(
                request_id,
                mcp_tool_result(
                    json!({
                        "error":"invalid_tool_input",
                        "detail":"symbols must be an array containing between 1 and 20 non-empty strings"
                    }),
                    true,
                ),
            );
        }
        let mut symbols = Vec::with_capacity(values.len());
        for value in values {
            let Some(raw) = value.as_str() else {
                return mcp_result(
                    request_id,
                    mcp_tool_result(
                        json!({
                            "error":"invalid_tool_input",
                            "detail":"symbols must be an array containing between 1 and 20 non-empty strings"
                        }),
                        true,
                    ),
                );
            };
            let symbol = raw.trim().to_ascii_uppercase();
            if symbol.is_empty()
                || symbol.len() > 64
                || symbol
                    .bytes()
                    .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
            {
                return mcp_result(
                    request_id,
                    mcp_tool_result(
                        json!({
                            "error":"invalid_tool_input",
                            "detail":"symbols must be an array containing between 1 and 20 non-empty strings"
                        }),
                        true,
                    ),
                );
            }
            symbols.push(symbol);
        }
        let unique = symbols
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        return match state.market_data.latest_payloads(&unique).await {
            Ok(payloads) => {
                let mut items = Vec::new();
                let mut errors = Vec::new();
                for symbol in symbols {
                    if let Some(payload) = payloads.get(&symbol) {
                        items.push(payload.clone());
                    } else {
                        errors.push(json!({"symbol":symbol,"status":"unavailable","error":"cached quote not found"}));
                    }
                }
                mcp_result(
                    request_id,
                    mcp_tool_result(
                        json!({
                            "count":items.len(),"items":items,"errors":errors
                        }),
                        false,
                    ),
                )
            }
            Err(_) => mcp_result(
                request_id,
                mcp_tool_result(
                    json!({
                        "error":"marketcow_api_error",
                        "detail":{"status_code":503,"detail":{"code":"quote_repository_unavailable"}}
                    }),
                    true,
                ),
            ),
        };
    }
    if name == Some("get_canonical_bars") {
        let arguments = arguments.as_object().expect("arguments were validated");
        let allowed = BTreeSet::from([
            "instrument_id",
            "start",
            "end",
            "interval",
            "adjustment",
            "page_size",
            "cursor",
        ]);
        let mut unexpected = arguments
            .keys()
            .filter(|key| !allowed.contains(key.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        unexpected.sort();
        if !unexpected.is_empty() {
            return mcp_result(
                request_id,
                mcp_tool_result(
                    json!({"error":"invalid_tool_input","detail":format!(
                        "unexpected argument(s): {}", unexpected.join(", ")
                    )}),
                    true,
                ),
            );
        }
        let required_string = |name: &str| {
            arguments
                .get(name)
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        };
        let Some(instrument_id) = required_string("instrument_id") else {
            return mcp_result(
                request_id,
                mcp_tool_result(
                    json!({
                        "error":"invalid_tool_input","detail":"missing or invalid instrument_id"
                    }),
                    true,
                ),
            );
        };
        let Some(start) = required_string("start") else {
            return mcp_result(
                request_id,
                mcp_tool_result(
                    json!({
                        "error":"invalid_tool_input","detail":"missing or invalid start"
                    }),
                    true,
                ),
            );
        };
        let Some(end) = required_string("end") else {
            return mcp_result(
                request_id,
                mcp_tool_result(
                    json!({
                        "error":"invalid_tool_input","detail":"missing or invalid end"
                    }),
                    true,
                ),
            );
        };
        let optional_string = |name: &str, default: &str| {
            arguments
                .get(name)
                .map(|value| value.as_str().map(str::to_owned))
                .unwrap_or_else(|| Some(default.into()))
        };
        let Some(interval) = optional_string("interval", "1-DAY") else {
            return mcp_result(
                request_id,
                mcp_tool_result(
                    json!({
                        "error":"invalid_tool_input","detail":"interval must be a string"
                    }),
                    true,
                ),
            );
        };
        let Some(adjustment) = optional_string("adjustment", "qfq") else {
            return mcp_result(
                request_id,
                mcp_tool_result(
                    json!({
                        "error":"invalid_tool_input","detail":"adjustment must be a string"
                    }),
                    true,
                ),
            );
        };
        let page_size = match arguments.get("page_size") {
            Some(value) => match value.as_u64().and_then(|value| u32::try_from(value).ok()) {
                Some(value) if (1..=1_000).contains(&value) => value,
                _ => {
                    return mcp_result(
                        request_id,
                        mcp_tool_result(
                            json!({
                                "error":"invalid_tool_input","detail":"page_size must be between 1 and 1000"
                            }),
                            true,
                        ),
                    );
                }
            },
            None => 500,
        };
        let cursor = match arguments.get("cursor") {
            Some(value) => match value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                Some(value) => Some(value.to_owned()),
                None => {
                    return mcp_result(
                        request_id,
                        mcp_tool_result(
                            json!({
                                "error":"invalid_tool_input","detail":"cursor must be a non-empty string"
                            }),
                            true,
                        ),
                    );
                }
            },
            None => None,
        };
        let response = canonical_bars(
            State(state.clone()),
            AxumPath(instrument_id),
            Query(CanonicalBarsQuery {
                start,
                end,
                interval,
                adjustment,
                page_size,
                cursor,
            }),
        )
        .await;
        let status = response.status();
        let body = match axum::body::to_bytes(response.into_body(), 2 * 1024 * 1024).await {
            Ok(body) => body,
            Err(_) => {
                return mcp_result(
                    request_id,
                    mcp_tool_result(
                        json!({
                            "error":"marketcow_api_error",
                            "detail":{"status_code":500,"detail":{"code":"canonical_response_unavailable"}}
                        }),
                        true,
                    ),
                );
            }
        };
        let payload = serde_json::from_slice::<serde_json::Value>(&body)
            .unwrap_or_else(|_| json!({"detail":{"code":"canonical_response_invalid"}}));
        return if status.is_success() {
            mcp_result(request_id, mcp_tool_result(payload, false))
        } else {
            mcp_result(
                request_id,
                mcp_tool_result(
                    json!({
                        "error":"marketcow_api_error",
                        "detail":{"status_code":status.as_u16(),"detail":payload["detail"]}
                    }),
                    true,
                ),
            )
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
    let fresh = projection_fresh(&state, &projection);
    let hyperliquid_ready = state.hyperliquid_shadow.as_ref().is_none_or(|reader| {
        matches!(
            reader.snapshot().health,
            marketcow_realtime::RealtimeHubHealth::Ready { .. }
        )
    });
    let polymarket_ready = polymarket_projection_ready(&state, &projection);
    let ready = polymarket_ready && hyperliquid_ready;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({
            "ready":ready,
            "mode":if state.config.shadow_mode { "shadow" } else { "authoritative" },
            "scope_id":projection.scope_id,
            "writer_enabled":!state.config.shadow_mode, "real_order_submission_enabled":false,
            "fail_closed_reason":if !fresh {
                Some("book_stale".to_string())
            } else if !polymarket_ready {
                projection.fail_closed_reason.clone().or_else(|| Some("polymarket_projection_unready".into()))
            } else if !hyperliquid_ready {
                Some("hyperliquid_shadow_unready".into())
            } else {
                None
            },
            "hyperliquid_shadow_enabled":state.hyperliquid_shadow.is_some(),
            "hyperliquid_shadow_ready":hyperliquid_ready,
            "ownership_registry":"docs/architecture/migration/domain-ownership.yaml"
        })),
    )
        .into_response()
}

async fn scope(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
) -> Response {
    let Some((projection, live)) = polymarket_atomic_view(&state) else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_generation_transition_in_progress",
            true,
            &request_id,
        );
    };
    let ready = polymarket_projection_ready(&state, &projection);
    let universe = live.as_ref().and_then(|value| value.universe.clone());
    let active_market_ids = projection.active_market_ids();
    let active_token_ids = projection.active_token_ids();
    let configured_market_ids = projection.configured_market_ids();
    let configured_token_ids = projection.configured_token_ids();
    let available_token_ids = projection.available_token_ids();
    let unavailable_token_ids = projection.unavailable_token_ids();
    let quarantined_market_ids = projection.quarantined_market_ids();
    let active_markets = universe.as_ref().map(|value| {
        value
            .active_markets
            .iter()
            .filter(|market| active_market_ids.contains(&market.market_id))
            .cloned()
            .collect::<Vec<_>>()
    });
    let effective_exclusions = universe
        .as_ref()
        .map(|value| effective_polymarket_exclusions(value, &projection));
    let active_market_count = active_markets.as_ref().map_or_else(
        || live.as_ref().and_then(|value| value.market_count),
        |value| Some(value.len()),
    );
    let active_token_count = active_markets.as_ref().map_or_else(
        || live.as_ref().map(|value| value.token_ids.len()),
        |value| Some(value.len() * 2),
    );
    Json(json!({
        "schema_version":if universe.is_some() {
            "marketcow.polymarket.scope-discovery.v5"
        } else {
            "marketcow.polymarket.scope-discovery.v2"
        },
        "active_scope_id":projection.scope_id,
        "universe_id":universe.as_ref().map(|value| value.universe_id.clone()),
        "generation":universe.as_ref().map(|value| value.generation),
        "scope_status":if ready { "ready" } else { "unready" },
        "mode":if state.config.shadow_mode { "shadow" } else { "authoritative" },
        "ready":ready,
        "market_count":active_market_count,
        "token_count":active_token_count,
        "configured_market_count":configured_market_ids.len(),
        "configured_token_count":configured_token_ids.len(),
        "available_token_count":available_token_ids.len(),
        "unavailable_token_count":unavailable_token_ids.len(),
        "catalog_revision":live.as_ref().and_then(|value| value.catalog_revision.clone()),
        "scope_file_sha256":live.as_ref().and_then(|value| value.scope_file_sha256.clone()),
        "boundary_cursor":projection.cursor,
        "target_market_count":universe.as_ref().map(|value| value.target_market_count),
        "minimum_market_count":universe.as_ref().map(|value| value.minimum_market_count),
        "active_markets":active_markets,
        "active_market_ids":active_market_ids,
        "tradable_market_ids":projection.active_market_ids(),
        "tradable_token_ids":active_token_ids,
        "configured_markets":universe.as_ref().map(|value| value.active_markets.clone()),
        "configured_market_ids":configured_market_ids,
        "configured_token_ids":configured_token_ids,
        "available_token_ids":available_token_ids,
        "unavailable_token_ids":unavailable_token_ids,
        "unavailable_tokens":marketcow_api::unavailable_tokens(&projection),
        "availability_mask_semantics":"configured_token_ids_intersect_available_token_ids",
        "new_opportunity_requires_all_market_tokens_available":true,
        "quarantined_market_ids":quarantined_market_ids,
        "market_health":projection.market_health.values().cloned().collect::<Vec<_>>(),
        "added_markets":universe.as_ref().map(|value| value.added_markets.clone()),
        "removed_markets":universe.as_ref().map(|value| value.removed_markets.clone()),
        "excluded_markets":effective_exclusions,
        "filters":universe.as_ref().map(|value| value.filters.clone()),
        "real_order_submission_enabled":false,
    }))
    .into_response()
}

fn effective_polymarket_exclusions(
    universe: &PolymarketUniverseContract,
    projection: &marketcow_core::Projection,
) -> Vec<serde_json::Value> {
    let mut exclusions = universe
        .excluded_markets
        .iter()
        .map(|value| serde_json::to_value(value).expect("universe exclusion serializes"))
        .collect::<Vec<_>>();
    for market_id in projection.quarantined_market_ids() {
        let health = projection.market_health.get(&market_id);
        exclusions.push(json!({
            "market_id":market_id,
            "reason_code":health.and_then(|value| value.reason_code.clone()).unwrap_or_else(|| "market_quarantined".into()),
            "retryable":health.is_none_or(|value| value.retryable),
            "retry_after":health.and_then(|value| value.retry_after),
            "observed_at":health.and_then(|value| value.source_observed_at),
            "projection_status":health.map(|value| value.projection_status),
            "last_market_sequence":health.map(|value| value.last_market_sequence),
            "last_recovered_at":health.and_then(|value| value.last_recovered_at),
        }));
    }
    exclusions
}

fn effective_polymarket_universe(
    universe: &PolymarketUniverseContract,
    projection: &marketcow_core::Projection,
) -> serde_json::Value {
    let active_market_ids = projection.active_market_ids();
    let active_markets = universe
        .active_markets
        .iter()
        .filter(|market| active_market_ids.contains(&market.market_id))
        .cloned()
        .collect::<Vec<_>>();
    let mut payload = serde_json::to_value(universe).expect("universe serializes");
    let object = payload.as_object_mut().expect("universe is an object");
    object.insert(
        "schema_version".into(),
        json!("marketcow.polymarket.universe.v3"),
    );
    object.insert("active_markets".into(), json!(active_markets));
    object.insert("active_market_ids".into(), json!(active_market_ids));
    object.insert(
        "tradable_market_ids".into(),
        json!(projection.active_market_ids()),
    );
    object.insert(
        "tradable_token_ids".into(),
        json!(projection.active_token_ids()),
    );
    object.insert("configured_markets".into(), json!(universe.active_markets));
    object.insert(
        "configured_market_ids".into(),
        json!(projection.configured_market_ids()),
    );
    object.insert(
        "configured_token_ids".into(),
        json!(projection.configured_token_ids()),
    );
    object.insert(
        "available_token_ids".into(),
        json!(projection.available_token_ids()),
    );
    object.insert(
        "unavailable_token_ids".into(),
        json!(projection.unavailable_token_ids()),
    );
    object.insert(
        "unavailable_tokens".into(),
        json!(marketcow_api::unavailable_tokens(projection)),
    );
    object.insert(
        "availability_mask_semantics".into(),
        json!("configured_token_ids_intersect_available_token_ids"),
    );
    object.insert(
        "new_opportunity_requires_all_market_tokens_available".into(),
        json!(true),
    );
    object.insert(
        "quarantined_market_ids".into(),
        json!(projection.quarantined_market_ids()),
    );
    object.insert(
        "market_health".into(),
        json!(
            projection
                .market_health
                .values()
                .cloned()
                .collect::<Vec<_>>()
        ),
    );
    object.insert(
        "excluded_markets".into(),
        json!(effective_polymarket_exclusions(universe, projection)),
    );
    object.insert("projection_generation".into(), json!(projection.generation));
    object.insert("boundary_cursor".into(), json!(projection.cursor));
    payload
}

async fn live_snapshot(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
) -> Response {
    let projection = state.projection.load_full();
    if !polymarket_projection_ready(&state, &projection) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_projection_unready_or_stale",
            true,
            &request_id,
        );
    }
    Json(marketcow_api::snapshot(&projection)).into_response()
}

async fn live_market_snapshot(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    AxumPath(market_id): AxumPath<String>,
) -> Response {
    let Some((projection, _)) = polymarket_atomic_view(&state) else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_generation_transition_in_progress",
            true,
            &request_id,
        );
    };
    if projection.catalog_revision.is_none() {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_catalog_unavailable",
            true,
            &request_id,
        );
    }
    match marketcow_api::market_snapshot(&projection, &market_id) {
        Some(snapshot) => Json(snapshot).into_response(),
        None => error(
            StatusCode::NOT_FOUND,
            "polymarket_market_not_found",
            false,
            &request_id,
        ),
    }
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
    if !polymarket_projection_ready(&state, &projection) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_projection_unready_or_stale",
            true,
            &request_id,
        );
    }
    let records = {
        let runtime = state.runtime.lock().await;
        clone_polymarket_recent_events(&runtime)
    };
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
    if !polymarket_projection_ready(&state, &projection) {
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
    let Some((projection, live)) = polymarket_atomic_view(&state) else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_generation_transition_in_progress",
            true,
            &request_id,
        );
    };
    if !polymarket_projection_ready(&state, &projection) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_projection_unready_or_stale",
            true,
            &request_id,
        );
    }
    let mut payload = serde_json::to_value(marketcow_api::full_sync(&projection))
        .expect("full-sync response serializes");
    if let Some(universe) = live.as_ref().and_then(|value| value.universe.clone()) {
        let object = payload
            .as_object_mut()
            .expect("full-sync response is an object");
        object.insert(
            "universe_schema_version".into(),
            json!("marketcow.polymarket.universe.v3"),
        );
        object.insert("universe_id".into(), json!(universe.universe_id));
        object.insert("universe_generation".into(), json!(universe.generation));
        object.insert(
            "universe".into(),
            effective_polymarket_universe(&universe, &projection),
        );
    }
    Json(payload).into_response()
}

#[derive(Debug, Deserialize)]
struct StreamQuery {
    provider: Option<String>,
    after_cursor: Option<u64>,
    instruments: Option<String>,
    data_types: Option<String>,
}

async fn market_data_stream(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    Query(query): Query<StreamQuery>,
) -> Response {
    match query.provider.as_deref() {
        Some("hyperliquid") => {
            return hyperliquid_stream_response(
                ws,
                state,
                request_id,
                HyperliquidEventQuery {
                    after_sequence: query.after_cursor,
                    limit: default_event_limit(),
                    instruments: query.instruments,
                    data_types: query.data_types,
                },
            )
            .await;
        }
        None | Some("polymarket") => {}
        Some(_) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "unsupported_realtime_provider",
                false,
                &request_id,
            );
        }
    }
    if query.instruments.is_some() || query.data_types.is_some() {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "polymarket_stream_filter_unsupported",
            false,
            &request_id,
        );
    }
    let projection = state.projection.load_full();
    if !polymarket_projection_ready(&state, &projection) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "polymarket_projection_unready_or_stale",
            true,
            &request_id,
        );
    }
    // Subscribe before capturing the immutable replay views. Events visible in both are skipped by
    // cursor, while an event landing between the reads remains in at least one source.
    let receiver = state.stream.subscribe();
    // The single-writer lock is the atomic replay boundary. Capturing both views here avoids a
    // projection-new/replay-old window without copying the replay buffer on every hot-path event.
    let (projection, records) = {
        let runtime = state.runtime.lock().await;
        (
            runtime.projection(),
            Arc::new(clone_polymarket_recent_events(&runtime)),
        )
    };
    if !polymarket_projection_ready(&state, &projection) {
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
    let subscription_scope_id = projection.scope_id.clone();
    let subscription_universe = state
        .active_polymarket_scope
        .load_full()
        .and_then(|live| live.universe.clone());
    let mut last_cursor = after_cursor.unwrap_or(boundary);

    if let Some(resume_cursor) = after_cursor {
        if resume_cursor > boundary {
            // A clean lineage replacement can intentionally restart the local cursor while
            // retaining the stable scope/universe identity. An old consumer cursor is therefore
            // expired evidence from the retired lineage, not a malformed WebSocket request.
            let frame = marketcow_api::stream_resync_required(
                &subscription_scope_id,
                resume_cursor,
                "event_cursor_expired",
            );
            let _ = send_stream_frame(&mut socket, &frame).await;
            close_stream(&mut socket, close_code::POLICY, "full_sync_required").await;
            return;
        }
        let earliest = records.first().map(|record| record.event.cursor);
        if resume_cursor < boundary
            && earliest.is_some_and(|cursor| cursor > resume_cursor.saturating_add(1))
        {
            let frame = marketcow_api::stream_resync_required(
                &subscription_scope_id,
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
        let replay_window = match polymarket_replay_window(&records, resume_cursor, boundary) {
            Ok(window) => window,
            Err(reason) => {
                send_resync_and_close(&mut socket, &state, last_cursor, reason).await;
                return;
            }
        };
        for record in &records[replay_window] {
            if record.event.cursor <= resume_cursor {
                send_resync_and_close(&mut socket, &state, last_cursor, "stream_replay_gap").await;
                return;
            }
            if let Some(reason) = polymarket_stream_resync_reason(record) {
                send_resync_and_close(&mut socket, &state, last_cursor, reason).await;
                return;
            }
            let Ok(frame) = marketcow_api::stream_record(&subscription_scope_id, record) else {
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
                    let current = state.projection.load_full();
                    if let Some(frame) = polymarket_universe_change_frame(
                        &state,
                        subscription_universe.as_ref(),
                        current.cursor,
                    ) {
                        let _ = send_stream_frame(&mut socket, &frame).await;
                        close_stream(&mut socket, close_code::POLICY, "full_sync_required").await;
                        return;
                    }
                    if current.scope_id != subscription_scope_id {
                        send_resync_and_close(&mut socket, &state, last_cursor, "scope_changed")
                            .await;
                        return;
                    }
                    let records = if record.event.cursor == last_cursor.saturating_add(1) {
                        vec![record]
                    } else {
                        match polymarket_wal_replay_records(
                            &state,
                            last_cursor,
                            record.event.cursor,
                        )
                        .await
                        {
                            Ok(records) => records,
                            Err(reason) => {
                                send_resync_and_close(&mut socket, &state, last_cursor, reason)
                                    .await;
                                return;
                            }
                        }
                    };
                    match deliver_polymarket_records(
                        &mut socket,
                        &state,
                        &subscription_scope_id,
                        &records,
                        &mut last_cursor,
                    )
                    .await
                    {
                        StreamRecordDelivery::Delivered => {}
                        StreamRecordDelivery::ClientClosed => return,
                        StreamRecordDelivery::GlobalResync(reason) => {
                            send_resync_and_close(&mut socket, &state, last_cursor, reason).await;
                            return;
                        }
                    }
                    if !polymarket_projection_ready(&state, &current) {
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
                    let current = state.projection.load_full();
                    let records =
                        match polymarket_wal_replay_records(&state, last_cursor, current.cursor)
                            .await
                        {
                            Ok(records) => records,
                            Err(reason) => {
                                state
                                    .metrics
                                    .stream_slow_consumer_disconnects
                                    .fetch_add(1, Ordering::Relaxed);
                                send_resync_and_close(&mut socket, &state, last_cursor, reason)
                                    .await;
                                return;
                            }
                        };
                    match deliver_polymarket_records(
                        &mut socket,
                        &state,
                        &subscription_scope_id,
                        &records,
                        &mut last_cursor,
                    )
                    .await
                    {
                        StreamRecordDelivery::Delivered
                            if polymarket_projection_ready(&state, &current) => {}
                        StreamRecordDelivery::Delivered => {
                            send_resync_and_close(
                                &mut socket,
                                &state,
                                last_cursor,
                                "projection_unready_or_stale",
                            )
                            .await;
                            return;
                        }
                        StreamRecordDelivery::ClientClosed => return,
                        StreamRecordDelivery::GlobalResync(reason) => {
                            send_resync_and_close(&mut socket, &state, last_cursor, reason).await;
                            return;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    close_stream(&mut socket, close_code::RESTART, "server_shutdown").await;
                    return;
                }
            },
            Next::FreshnessCheck => {
                let current = state.projection.load_full();
                if let Some(frame) = polymarket_universe_change_frame(
                    &state,
                    subscription_universe.as_ref(),
                    current.cursor,
                ) {
                    let _ = send_stream_frame(&mut socket, &frame).await;
                    close_stream(&mut socket, close_code::POLICY, "full_sync_required").await;
                    return;
                }
                if current.scope_id != subscription_scope_id {
                    send_resync_and_close(&mut socket, &state, last_cursor, "scope_changed").await;
                    return;
                }
                if !polymarket_projection_ready(&state, &current) {
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

enum StreamRecordDelivery {
    Delivered,
    ClientClosed,
    GlobalResync(&'static str),
}

async fn polymarket_wal_replay_records(
    state: &AppState,
    after_cursor: u64,
    boundary: u64,
) -> std::result::Result<Vec<marketcow_core::PersistedEvent>, &'static str> {
    state
        .metrics
        .polymarket_wal_replay_attempts
        .fetch_add(1, Ordering::Relaxed);
    let runtime = state.runtime.lock().await;
    if runtime.projection().cursor < boundary {
        state
            .metrics
            .polymarket_wal_replay_failures
            .fetch_add(1, Ordering::Relaxed);
        return Err("stream_replay_incomplete");
    }
    // `recent_events` is populated only after the append-only WAL barrier and rebuilt from the
    // verified WAL on restart. It is therefore the bounded authoritative event-journal replay
    // window, not an uncommitted transport cache.
    let records = runtime.recent_events().iter().cloned().collect::<Vec<_>>();
    drop(runtime);
    let window = match polymarket_replay_window(&records, after_cursor, boundary) {
        Ok(window) => window,
        Err(reason) => {
            state
                .metrics
                .polymarket_wal_replay_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(reason);
        }
    };
    state
        .metrics
        .polymarket_wal_replay_successes
        .fetch_add(1, Ordering::Relaxed);
    Ok(records[window].to_vec())
}

async fn deliver_polymarket_records(
    socket: &mut WebSocket,
    state: &AppState,
    scope_id: &str,
    records: &[marketcow_core::PersistedEvent],
    last_cursor: &mut u64,
) -> StreamRecordDelivery {
    for record in records {
        if record.event.cursor != last_cursor.saturating_add(1) {
            return StreamRecordDelivery::GlobalResync("stream_replay_gap");
        }
        if let Some(reason) = polymarket_stream_resync_reason(record) {
            return StreamRecordDelivery::GlobalResync(reason);
        }
        let Ok(frame) = marketcow_api::stream_record(scope_id, record) else {
            close_stream(socket, close_code::ERROR, "serialization_failed").await;
            return StreamRecordDelivery::ClientClosed;
        };
        if !deliver_stream_frame(socket, state, &frame).await {
            return StreamRecordDelivery::ClientClosed;
        }
        *last_cursor = record.event.cursor;
    }
    StreamRecordDelivery::Delivered
}

fn polymarket_stream_resync_reason(
    record: &marketcow_core::PersistedEvent,
) -> Option<&'static str> {
    if record
        .market
        .as_ref()
        .is_some_and(|market| market.transition.is_some() && record.fail_closed_reason.is_none())
    {
        return None;
    }
    if !record.applied || record.fail_closed_reason.is_some() {
        return Some("upstream_projection_gap");
    }
    match &record.event.kind {
        marketcow_core::EventKind::CatalogSnapshot { .. }
        | marketcow_core::EventKind::NewMarket { .. }
        | marketcow_core::EventKind::MarketResolved { .. }
        | marketcow_core::EventKind::TickSizeChange { .. } => Some("instrument_facts_changed"),
        marketcow_core::EventKind::SourceGap { .. } if record.market.is_some() => None,
        marketcow_core::EventKind::SourceGap { .. } => Some("upstream_projection_gap"),
        marketcow_core::EventKind::FullBook { .. }
        | marketcow_core::EventKind::Delta { .. }
        | marketcow_core::EventKind::AtomicDelta { .. }
        | marketcow_core::EventKind::BestBidAsk { .. }
        | marketcow_core::EventKind::LastTradePrice { .. } => None,
    }
}

fn polymarket_replay_window(
    records: &[marketcow_core::PersistedEvent],
    resume_cursor: u64,
    boundary: u64,
) -> std::result::Result<std::ops::Range<usize>, &'static str> {
    if resume_cursor > boundary
        || records
            .windows(2)
            .any(|pair| pair[0].event.cursor >= pair[1].event.cursor)
    {
        return Err("stream_replay_gap");
    }
    let start = records.partition_point(|record| record.event.cursor <= resume_cursor);
    let end = records.partition_point(|record| record.event.cursor <= boundary);
    let mut expected = resume_cursor.saturating_add(1);
    for record in &records[start..end] {
        if record.event.cursor != expected {
            return Err("stream_replay_gap");
        }
        expected = expected.saturating_add(1);
    }
    if expected.saturating_sub(1) != boundary {
        return Err("stream_replay_incomplete");
    }
    Ok(start..end)
}

fn polymarket_universe_change_frame(
    state: &AppState,
    subscribed: Option<&PolymarketUniverseContract>,
    boundary_cursor: u64,
) -> Option<marketcow_api::StreamFrame> {
    let current = state
        .active_polymarket_scope
        .load_full()?
        .universe
        .clone()?;
    let old_generation = subscribed.map_or(0, |universe| universe.generation);
    if subscribed.is_some_and(|universe| {
        universe.universe_id == current.universe_id && universe.generation == current.generation
    }) {
        return None;
    }
    Some(marketcow_api::stream_universe_changed(
        &current.universe_id,
        boundary_cursor,
        current.universe_id.clone(),
        old_generation,
        current.generation,
        current.added_markets,
        current.removed_markets,
    ))
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
    state
        .metrics
        .polymarket_global_resyncs
        .fetch_add(1, Ordering::Relaxed);
    let active_scope_id = state.projection.load().scope_id.clone();
    let frame = marketcow_api::stream_resync_required(&active_scope_id, cursor, reason);
    let _ = send_stream_frame(socket, &frame).await;
    close_stream(socket, close_code::AGAIN, "full_sync_required").await;
}

#[derive(Deserialize)]
struct ActivatePolymarketScopeRequest {
    schema_version: String,
    scope_id: String,
    scope_file_sha256: String,
}

fn registered_polymarket_scope_path(scope_id: &str) -> Result<PathBuf> {
    if scope_id.len() != 64 || !scope_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("scope_id must be a SHA-256 hex digest");
    }
    let root = absolute_env("MARKETCOW_POLYMARKET_SCOPE_REGISTRY_ROOT")?;
    let canonical_root = fs::canonicalize(&root)
        .context("MARKETCOW_POLYMARKET_SCOPE_REGISTRY_ROOT is unavailable")?;
    let path = canonical_root.join(format!("{}.json", scope_id.to_ascii_lowercase()));
    let metadata =
        fs::symlink_metadata(&path).context("registered scope artifact is unavailable")?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("registered scope artifact must be a regular non-symlink file");
    }
    let canonical_path = fs::canonicalize(&path)?;
    if !canonical_path.starts_with(&canonical_root) {
        bail!("registered scope artifact escapes its registry root");
    }
    Ok(canonical_path)
}

fn runtime_config_for_dynamic_scope(
    config: &Config,
    config_revision: &str,
    scope_id: &str,
) -> marketcow_runtime::RuntimeConfig {
    marketcow_runtime::RuntimeConfig {
        root: config.storage_root.join("polymarket-scopes").join(scope_id),
        scope_id: scope_id.to_owned(),
        config_revision: config_revision.to_owned(),
        wal_segment_bytes: 256 * 1024 * 1024,
        recent_event_capacity: POLYMARKET_RECENT_EVENT_CAPACITY,
    }
}

fn validate_polymarket_universe_transition(
    current: &PolymarketLiveConfig,
    activated: &PolymarketLiveConfig,
) -> std::result::Result<(), String> {
    let Some(next) = activated.universe.as_ref() else {
        return Ok(());
    };
    if activated.scope_id != next.universe_id || current.scope_id != activated.scope_id {
        return Err("universe_identity_changed".into());
    }
    let previous_generation = current
        .universe
        .as_ref()
        .map_or(0, |universe| universe.generation);
    if next.generation != previous_generation.saturating_add(1) {
        return Err("universe_generation_not_monotonic".into());
    }
    let previous = current.market_ids.iter().cloned().collect::<BTreeSet<_>>();
    let active = activated
        .market_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let expected_added = active
        .difference(&previous)
        .cloned()
        .collect::<BTreeSet<_>>();
    let expected_removed = previous
        .difference(&active)
        .cloned()
        .collect::<BTreeSet<_>>();
    if next.added_markets.iter().cloned().collect::<BTreeSet<_>>() != expected_added
        || next
            .removed_markets
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            != expected_removed
    {
        return Err("universe_membership_delta_mismatch".into());
    }
    Ok(())
}

async fn prepare_polymarket_scope_candidate(
    state: &AppState,
    current: &PolymarketLiveConfig,
    activated: &PolymarketLiveConfig,
) -> std::result::Result<marketcow_runtime::PolymarketRuntime, String> {
    validate_polymarket_universe_transition(current, activated)?;
    let activated = activated.clone();
    let config = state.config.clone();
    let config_revision = state.control_plane.config_revision.clone();
    let active_runtime = state.runtime.clone();
    tokio::task::spawn_blocking(move || {
        let mut runtime = if let Some(universe) = activated.universe.as_ref() {
            let root = config
                .storage_root
                .join("polymarket-universes")
                .join(&universe.universe_id)
                .join(format!("generation-{:020}", universe.generation));
            let candidate_config = marketcow_runtime::RuntimeConfig {
                root,
                scope_id: activated.scope_id.clone(),
                config_revision: config_revision.clone(),
                wal_segment_bytes: 256 * 1024 * 1024,
                recent_event_capacity: POLYMARKET_RECENT_EVENT_CAPACITY,
            };
            active_runtime
                .blocking_lock()
                .fork_candidate(candidate_config)
                .map_err(|error| format!("universe_candidate_fork_failed:{error}"))?
        } else {
            marketcow_runtime::PolymarketRuntime::open(runtime_config_for_dynamic_scope(
                &config,
                &config_revision,
                &activated.scope_id,
            ))
            .map_err(|error| format!("scope_runtime_open_failed:{error}"))?
        };
        seed_polymarket_scope_catalog(&mut runtime, Some(&activated))
            .map_err(|error| format!("scope_catalog_seed_failed:{error}"))?;
        let projection = runtime.projection();
        if activated.universe.is_some()
            && (!projection.ready
                || !projection.instrument_ticks_consistent()
                || !projection_matches_polymarket_scope(&activated, &projection, true))
        {
            return Err("universe_candidate_not_ready".into());
        }
        runtime
            .checkpoint()
            .map_err(|error| format!("universe_candidate_checkpoint_failed:{error}"))?;
        Ok(runtime)
    })
    .await
    .map_err(|error| format!("universe_candidate_preparation_task_failed:{error}"))?
}

async fn admin_activate_polymarket_scope(
    State(state): State<AppState>,
    Extension(request_id): Extension<String>,
    Json(request): Json<ActivatePolymarketScopeRequest>,
) -> Response {
    if request.schema_version != "marketcow.polymarket.scope-activation.v1"
        || request.scope_file_sha256.len() != 64
        || !request
            .scope_file_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_scope_activation_request",
            false,
            &request_id,
        );
    }
    let Some(sender) = &state.polymarket_scope_switch else {
        return error(
            StatusCode::CONFLICT,
            "polymarket_live_scope_switch_disabled",
            false,
            &request_id,
        );
    };
    let path = match registered_polymarket_scope_path(&request.scope_id) {
        Ok(path) => path,
        Err(_) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "scope_artifact_not_registered",
                false,
                &request_id,
            );
        }
    };
    let live = match load_polymarket_scope_file(&request.scope_id, &path.to_string_lossy()) {
        Ok(Some(live))
            if live
                .scope_file_sha256
                .as_deref()
                .is_some_and(|digest| digest.eq_ignore_ascii_case(&request.scope_file_sha256)) =>
        {
            live
        }
        _ => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "scope_artifact_hash_or_contract_mismatch",
                false,
                &request_id,
            );
        }
    };
    if let Some(active) = state.active_polymarket_scope.load_full()
        && active.scope_id == live.scope_id
        && active.scope_file_sha256 == live.scope_file_sha256
    {
        return Json(json!({
            "schema_version":if active.universe.is_some() {
                "marketcow.polymarket.universe-activation-receipt.v1"
            } else {
                "marketcow.polymarket.scope-activation-receipt.v1"
            },
            "status":"already_active",
            "active_scope_id":active.scope_id,
            "boundary_cursor":state.projection.load().cursor,
            "scope_file_sha256":active.scope_file_sha256,
            "real_order_submission_enabled":false,
        }))
        .into_response();
    }
    let permit = match sender.clone().try_reserve_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return error(
                StatusCode::TOO_MANY_REQUESTS,
                "scope_switch_queue_full",
                true,
                &request_id,
            );
        }
    };
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    permit.send(PolymarketScopeSwitchRequest {
        live,
        response: response_tx,
    });
    match tokio::time::timeout(Duration::from_secs(30), response_rx).await {
        Ok(Ok(Ok(receipt))) => Json(json!({
            "schema_version":if receipt.active_generation.is_some() {
                "marketcow.polymarket.universe-activation-receipt.v1"
            } else {
                "marketcow.polymarket.scope-activation-receipt.v1"
            },
            "status":if receipt.active_generation.is_some() {
                "activated_ready"
            } else {
                "activated_unready"
            },
            "previous_scope_id":receipt.previous_scope_id,
            "active_scope_id":receipt.active_scope_id,
            "boundary_cursor":receipt.boundary_cursor,
            "scope_file_sha256":receipt.scope_file_sha256,
            "universe_id":receipt.universe_id,
            "previous_generation":receipt.previous_generation,
            "active_generation":receipt.active_generation,
            "full_sync_required":true,
            "real_order_submission_enabled":false,
        }))
        .into_response(),
        Ok(Ok(Err(_))) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "scope_switch_failed_closed",
            true,
            &request_id,
        ),
        _ => error(
            StatusCode::GATEWAY_TIMEOUT,
            "scope_switch_timeout",
            true,
            &request_id,
        ),
    }
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
        "phase":"polymarket_final_architecture",
        "cutover_allowed":true,
        "checkpoint_persistence":if state.control_plane.persistence_enabled() {
            "healthy"
        } else {
            "degraded_development_only"
        },
        "ownership_registry":{
            "schema":"marketcow.domain-ownership.v2",
            "revision":"polymarket-final-architecture",
            "sha256":hex::encode(Sha256::digest(DOMAIN_OWNERSHIP_REGISTRY))
        },
        "real_order_submission_enabled":false,
        "tradude_may_manage_marketcow":false,
        "tradude_may_select_scope":true
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
    if !state.config.shadow_mode
        || state.config.real_order_submission_enabled
        || state.config.polymarket_live.is_some()
    {
        return error(
            StatusCode::FORBIDDEN,
            "shadow_ingest_disabled",
            false,
            &request_id,
        );
    }
    let received_at = request.received_at.unwrap_or_else(Utc::now);
    let mut runtime = state.runtime.lock().await;
    let apply_started = Instant::now();
    match runtime.apply_raw(request.raw_payload, received_at) {
        Ok(outcomes) => {
            let apply_latency_us = apply_started.elapsed().as_micros() as u64;
            let rejected = outcomes
                .iter()
                .filter(|outcome| !outcome.persisted.applied)
                .count();
            let persistence_latency_us = outcomes
                .iter()
                .map(|value| value.persistence_latency_us)
                .max()
                .unwrap_or(0);
            if persistence_latency_us > 0 {
                state
                    .metrics
                    .persistence_latency_us
                    .fetch_max(persistence_latency_us, Ordering::Relaxed);
            }
            let publication_latency_us = outcomes
                .iter()
                .map(|value| value.publication_latency_us)
                .max()
                .unwrap_or(0);
            if publication_latency_us > 0 {
                state
                    .metrics
                    .publication_latency_us
                    .fetch_max(publication_latency_us, Ordering::Relaxed);
            }
            state.projection.store(runtime.projection());
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
                "apply_latency_us":apply_latency_us,
                "persistence_latency_us":persistence_latency_us,
                "publication_latency_us":publication_latency_us,
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

fn polymarket_atomic_view(
    state: &AppState,
) -> Option<(
    Arc<marketcow_core::Projection>,
    Option<Arc<PolymarketLiveConfig>>,
)> {
    for _ in 0..3 {
        let before = state.active_polymarket_scope.load_full();
        let projection = state.projection.load_full();
        let after = state.active_polymarket_scope.load_full();
        let same_config = match (&before, &after) {
            (Some(before), Some(after)) => Arc::ptr_eq(before, after),
            (None, None) => true,
            _ => false,
        };
        if same_config
            && after.as_ref().is_none_or(|live| {
                live.scope_id == projection.scope_id
                    && (live.catalog_frame.is_none()
                        || projection_catalog_identity_matches_scope(live, &projection))
            })
        {
            return Some((projection, after));
        }
    }
    None
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let projection = state.projection.load_full();
    let maximum_book_age_ms = projection_maximum_effective_book_age_ms(&state, &projection);
    let hyperliquid = state
        .hyperliquid_shadow
        .as_ref()
        .map(marketcow_realtime::RealtimeHubReader::snapshot);
    let hyperliquid_enabled = u8::from(hyperliquid.is_some());
    let hyperliquid_ready = u8::from(hyperliquid.as_ref().is_some_and(|projection| {
        matches!(
            projection.health,
            marketcow_realtime::RealtimeHubHealth::Ready { .. }
        )
    }));
    let hyperliquid_sequence = hyperliquid
        .as_ref()
        .map_or(0, |value| value.public_sequence);
    let hyperliquid_wal_cursor = hyperliquid.as_ref().map_or(0, |value| value.wal_cursor);
    let hyperliquid_transport_depth = state
        .hyperliquid_queues
        .as_ref()
        .map_or(0, HyperliquidQueueProbe::transport_depth);
    let hyperliquid_gateway_depth = state
        .hyperliquid_queues
        .as_ref()
        .map_or(0, HyperliquidQueueProbe::gateway_depth);
    let hyperliquid_public_depth = state
        .hyperliquid_stream
        .as_ref()
        .map_or(0, broadcast::Sender::len);
    (
        [("content-type", "text/plain; version=0.0.4")],
        format!(
            "# TYPE marketcow_http_requests_total counter\nmarketcow_http_requests_total {}\n\
             # TYPE marketcow_http_errors_total counter\nmarketcow_http_errors_total {}\n\
             marketcow_real_order_submission_enabled 0\nmarketcow_shadow_mode {}\n\
             marketcow_projection_published_cursor {}\nmarketcow_projection_persisted_cursor {}\n\
             marketcow_unresolved_gaps {}\nmarketcow_book_count {}\nmarketcow_maximum_book_age_ms {}\n\
             marketcow_disconnects_total {}\nmarketcow_ingress_queue_depth 0\n\
             marketcow_stream_clients {}\nmarketcow_stream_disconnects_total {}\n\
             marketcow_stream_channel_depth {}\n\
             marketcow_stream_channel_capacity {}\nmarketcow_stream_slow_consumer_disconnects_total {}\n\
             marketcow_persistence_latency_us {}\nmarketcow_publication_latency_us {}\n\
             marketcow_polymarket_active_markets {}\nmarketcow_polymarket_quarantined_markets {}\n\
             marketcow_polymarket_market_quarantines_total {}\nmarketcow_polymarket_market_recovery_started_total {}\n\
             marketcow_polymarket_market_recovered_total {}\nmarketcow_polymarket_wal_replay_attempts_total {}\n\
             marketcow_polymarket_wal_replay_successes_total {}\nmarketcow_polymarket_wal_replay_failures_total {}\n\
             marketcow_polymarket_global_resyncs_total {}\n\
             marketcow_python_workers_configured {}\nmarketcow_python_workers_live {}\n\
             marketcow_python_worker_restarts_total {}\nmarketcow_python_worker_restart_budget_exhaustions_total {}\n\
             marketcow_python_worker_memory_limit_kills_total {}\nmarketcow_python_worker_memory_monitor_failures_total {}\n\
             marketcow_python_worker_memory_limit_mib {}\nmarketcow_python_worker_cpu_limit_seconds {}\n\
             marketcow_hyperliquid_shadow_enabled {}\nmarketcow_hyperliquid_shadow_ready {}\n\
             marketcow_hyperliquid_public_sequence {}\nmarketcow_hyperliquid_wal_cursor {}\n\
             marketcow_hyperliquid_transport_queue_depth {}\nmarketcow_hyperliquid_gateway_queue_depth {}\n\
             marketcow_hyperliquid_public_channel_depth {}\n",
            state.metrics.requests.load(Ordering::Relaxed),
            state.metrics.errors.load(Ordering::Relaxed),
            u8::from(state.config.shadow_mode),
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
            projection.active_market_ids().len(),
            projection.quarantined_market_ids().len(),
            state
                .metrics
                .polymarket_market_quarantines
                .load(Ordering::Relaxed),
            state
                .metrics
                .polymarket_market_recovery_started
                .load(Ordering::Relaxed),
            state
                .metrics
                .polymarket_market_recovered
                .load(Ordering::Relaxed),
            state
                .metrics
                .polymarket_wal_replay_attempts
                .load(Ordering::Relaxed),
            state
                .metrics
                .polymarket_wal_replay_successes
                .load(Ordering::Relaxed),
            state
                .metrics
                .polymarket_wal_replay_failures
                .load(Ordering::Relaxed),
            state
                .metrics
                .polymarket_global_resyncs
                .load(Ordering::Relaxed),
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
            hyperliquid_enabled,
            hyperliquid_ready,
            hyperliquid_sequence,
            hyperliquid_wal_cursor,
            hyperliquid_transport_depth,
            hyperliquid_gateway_depth,
            hyperliquid_public_depth,
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
            role: "public_gateway".into(),
            bind: "127.0.0.1:8870".parse().unwrap(),
            storage_root: dir.path().into(),
            wal_root: dir.path().join("wal"),
            worker_socket: dir.path().join("worker.sock"),
            scope_id: "s".into(),
            real_order_submission_enabled: false,
            shadow_mode: true,
            maximum_book_age_ms: 30_000,
            legacy_mcp_url: None,
            polymarket_live: None,
            hyperliquid_shadow: None,
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
                runtime: Arc::new(AsyncMutex::new(runtime)),
                active_polymarket_scope: Arc::new(ArcSwapOption::empty()),
                polymarket_book_validations: Arc::new(ArcSwap::from_pointee(
                    PolymarketBookValidationProjection {
                        scope_id: "s".into(),
                        verified_at: BTreeMap::new(),
                    },
                )),
                polymarket_scope_switch: None,
                jobs: Arc::new(DurableJobCoordinator::memory()),
                instruments: Arc::new(InstrumentCoordinator::memory()),
                market_data: Arc::new(MarketDataCoordinator::memory()),
                canonical_cursor: Arc::new(CanonicalCursorSigner {
                    secret: b"test-canonical-cursor-secret-at-least-32-bytes".to_vec(),
                    ttl_seconds: 3_600,
                }),
                control_plane: Arc::new(ControlPlaneCoordinator::memory("test-config-v1")),
                stream,
                hyperliquid_shadow: None,
                hyperliquid_stream: None,
                hyperliquid_queues: None,
                worker_status: Arc::new(PythonWorkerStatus::default()),
                legacy_mcp: None,
            },
        )
    }

    #[test]
    fn polymarket_live_token_scope_is_decimal_unique_and_deterministic() {
        assert_eq!(
            validate_polymarket_token_ids(vec!["20".into(), "10".into()]).unwrap(),
            ["10", "20"]
        );
        assert!(validate_polymarket_token_ids(vec!["10".into(), "10".into()]).is_err());
        assert!(validate_polymarket_token_ids(vec!["not-a-token".into()]).is_err());
    }

    #[tokio::test]
    async fn delayed_polymarket_frame_records_machine_readable_connection_recovery() {
        let (dir, state) = test_state();
        let received_at = Utc::now();
        let observed_at = received_at - chrono::Duration::milliseconds(7_294);
        let error = apply_polymarket_transport_frames(
            &state,
            vec![marketcow_polymarket::RawTransportFrame {
                raw_payload: json!({
                    "event_type":"price_change",
                    "timestamp":observed_at.timestamp_millis().to_string(),
                    "price_changes":[
                        {
                            "asset_id":"20",
                            "side":"BUY",
                            "price":"0.40",
                            "size":"10",
                            "best_bid":"0.40",
                            "best_ask":"0.60"
                        },
                        {
                            "asset_id":"10",
                            "side":"SELL",
                            "price":"0.60",
                            "size":"11",
                            "best_bid":"0.40",
                            "best_ask":"0.60"
                        }
                    ]
                }),
                received_at,
            }],
        )
        .await
        .unwrap_err();
        let message = error.to_string();
        assert!(message.starts_with("polymarket_projection_recovery_required:"));
        assert!(message.contains("\"recovery_scope\":\"transport_connection\""));
        assert!(message.contains("\"reason_codes\":[\"source_data_delayed\"]"));
        assert!(message.contains("\"affected_token_ids\":[\"10\",\"20\"]"));
        assert!(message.contains("\"maximum_observed_source_delay_ms\":7294"));
        assert!(message.contains("\"periodic_http_snapshot_used_for_repair\":false"));

        let audit = fs::read_to_string(dir.path().join("audit.jsonl")).unwrap();
        let recovery = audit
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find(|record| {
                record["domain"] == "polymarket_transport"
                    && record["transition"] == "recovery_required"
            })
            .unwrap();
        assert_eq!(recovery["details"]["affected_token_count"], 2);
        assert_eq!(recovery["details"]["affected_market_count"], 0);
        assert_eq!(
            recovery["details"]["action"],
            "same_scope_authoritative_websocket_reconnect"
        );
        assert_eq!(recovery["real_order_submission_enabled"], false);
    }

    #[test]
    fn polymarket_live_scope_file_binds_counts_scope_and_source_hashes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("scope.json");
        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "schema_version":"marketcow.polymarket.rust-live-scope.v3",
                "scope_id":"scope-100",
                "market_count":2,
                "token_count":4,
                "market_ids":["2","1"],
                "token_ids":["40","10","30","20"],
                "catalog_revision":"catalog-v1",
                "catalog_frame":{
                    "event_type":"catalog_revision",
                    "catalog_revision":"catalog-v1",
                    "markets":[
                        test_scope_market("1", "10", "20"),
                        test_scope_market("2", "30", "40")
                    ],
                    "negative_risk_relations":[]
                },
                "source":{
                    "manifest_sha256":"a".repeat(64),
                    "catalog_index_sha256":"B".repeat(64),
                    "catalog_sha256":"c".repeat(64),
                    "registry_sha256":"d".repeat(64)
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let loaded = load_polymarket_scope_file("scope-100", path.to_str().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(loaded.market_count, Some(2));
        assert_eq!(loaded.token_ids, ["10", "20", "30", "40"]);
        assert_eq!(
            loaded.source_manifest_sha256.as_deref(),
            Some("a".repeat(64).as_str())
        );
        assert_eq!(
            loaded.catalog_index_sha256.as_deref(),
            Some("b".repeat(64).as_str())
        );
        assert_eq!(loaded.scope_file_sha256.as_deref().map(str::len), Some(64));
        let fee = &loaded.catalog_frame.as_ref().unwrap()["markets"][0]["instrument_facts"]["fee_schedule"];
        assert_eq!(fee["taker_rate"], "0.05");
        assert_eq!(fee["calculation_status"], "informational_only");
        assert_eq!(fee["revision"].as_str().map(str::len), Some(64));
        assert!(load_polymarket_scope_file("wrong-scope", path.to_str().unwrap()).is_err());
    }

    #[test]
    fn polymarket_live_scope_file_rejects_expired_active_market() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("expired-scope.json");
        let mut market = test_scope_market("1", "10", "20");
        market["instrument_facts"]["end_at"] = json!("2026-08-27T23:59:00Z");
        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "schema_version":"marketcow.polymarket.rust-live-scope.v3",
                "scope_id":"expired-scope",
                "market_count":1,
                "token_count":2,
                "market_ids":["1"],
                "token_ids":["10","20"],
                "catalog_revision":"catalog-v1",
                "catalog_frame":{
                    "event_type":"catalog_revision",
                    "catalog_revision":"catalog-v1",
                    "markets":[market],
                    "negative_risk_relations":[]
                },
                "source":{
                    "manifest_sha256":"a".repeat(64),
                    "catalog_index_sha256":"b".repeat(64),
                    "catalog_sha256":"c".repeat(64),
                    "registry_sha256":"d".repeat(64)
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let error = load_polymarket_scope_file_at(
            "expired-scope",
            path.to_str().unwrap(),
            "2026-08-29T00:00:00Z".parse().unwrap(),
        )
        .err()
        .expect("expired market must be rejected");
        assert!(error.to_string().contains("expired or inactive markets: 1"));
    }

    fn test_scope_market(market_id: &str, yes: &str, no: &str) -> serde_json::Value {
        let mut market = json!({
            "market_id":market_id,
            "condition_id":format!("condition-{market_id}"),
            "outcomes":[
                {"token_id":yes,"outcome":"Yes","instrument_id":format!("POLY:{market_id}:{yes}")},
                {"token_id":no,"outcome":"No","instrument_id":format!("POLY:{market_id}:{no}")}
            ],
            "negative_risk_group":null,
            "lifecycle_state":"active",
            "resolution":null,
            "metadata_revision":format!("metadata-{market_id}"),
            "observed_at":"2026-08-28T00:00:00Z",
            "terminal_at":null,
            "instrument_facts":{
                "price_increment":"0.01",
                "size_increment":"0.01",
                "minimum_order_size":"5",
                "settlement_currency":"pUSD",
                "start_at":"2026-01-01T00:00:00Z",
                "end_at":"2099-09-01T00:00:00Z",
                "revision":format!("instrument-{market_id}")
            }
        });
        market["instrument_facts"]["fee_schedule"] = test_fee_schedule("0.05", "e");
        market
    }

    fn test_fee_schedule(taker_rate: &str, revision_character: &str) -> serde_json::Value {
        json!({
            "schedule_id":"f".repeat(64),
            "revision":revision_character.repeat(64),
            "schedule_version":"polymarket-fees-v1",
            "currency":"USDC",
            "maker_rate":"0",
            "taker_rate":taker_rate,
            "formula_id":"polymarket_probability_fee.v1",
            "formula":"fee = C * feeRate * p * (1 - p)",
            "exponent":"1",
            "quantum":"0.00001",
            "rounding_mode":"UNSPECIFIED",
            "tie_semantics":"unspecified",
            "calculation_status":"informational_only",
            "effective_from":"2026-01-01T00:00:00Z",
            "effective_to":null,
            "observed_at":"2026-08-28T00:00:00Z",
            "provenance":[{
                "source":"polymarket_docs",
                "source_url":"https://docs.polymarket.com/trading/fees",
                "revision":"fees-docs-v1",
                "payload_sha256":"d".repeat(64),
                "observed_at":"2026-08-28T00:00:00Z",
                "field_paths":["fee_structure","fee_precision"]
            }]
        })
    }

    fn dynamic_live_scope(
        scope_id: &str,
        market_id: &str,
        yes: &str,
        no: &str,
    ) -> PolymarketLiveConfig {
        PolymarketLiveConfig {
            scope_id: scope_id.into(),
            token_ids: vec![no.into(), yes.into()],
            market_ids: vec![market_id.into()],
            market_count: Some(1),
            catalog_revision: Some(format!("catalog-{scope_id}")),
            catalog_frame: Some(json!({
                "event_type":"catalog_revision",
                "catalog_revision":format!("catalog-{scope_id}"),
                "markets":[test_scope_market(market_id, yes, no)],
                "negative_risk_relations":[]
            })),
            scope_file_sha256: Some("a".repeat(64)),
            scope_file_path: None,
            source_manifest_sha256: Some("b".repeat(64)),
            catalog_index_sha256: Some("c".repeat(64)),
            catalog_sha256: Some("d".repeat(64)),
            registry_sha256: Some("e".repeat(64)),
            universe: None,
            initial_book_frames: Vec::new(),
        }
    }

    fn dynamic_universe_live(
        universe_id: &str,
        generation: u64,
        market_id: &str,
        yes: &str,
        no: &str,
        previous_market: Option<&str>,
    ) -> PolymarketLiveConfig {
        let mut live = dynamic_live_scope(universe_id, market_id, yes, no);
        let today = Utc::now()
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let validated_at = today - chrono::Duration::days(1);
        let end_at = today + chrono::Duration::days(10);
        live.catalog_frame.as_mut().unwrap()["markets"][0]["instrument_facts"]["end_at"] =
            json!(end_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
        live.initial_book_frames = vec![yes, no]
            .into_iter()
            .map(|token| {
                json!({
                    "event_type":"book",
                    "asset_id":token,
                    "tick_size":"0.01",
                    "bids":[{"price":"0.40","size":"10"}],
                    "asks":[{"price":"0.60","size":"10"}],
                    "timestamp":validated_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                })
            })
            .collect();
        let active_identity = PolymarketUniverseMarket {
            market_id: market_id.into(),
            condition_id: format!("condition-{market_id}"),
            token_ids: vec![yes.into(), no.into()],
            end_at,
        };
        let removed_identities = previous_market
            .filter(|previous| *previous != market_id)
            .map_or_else(Vec::new, |previous| {
                vec![PolymarketUniverseMarket {
                    market_id: previous.into(),
                    condition_id: format!("condition-{previous}"),
                    token_ids: vec!["10".into(), "20".into()],
                    end_at,
                }]
            });
        live.universe = Some(PolymarketUniverseContract {
            schema_version: "marketcow.polymarket.universe.v2".into(),
            universe_id: universe_id.into(),
            generation,
            target_market_count: 1,
            minimum_market_count: 1,
            filters: PolymarketUniverseFilters {
                require_two_sided_books: true,
                require_complete_instrument_facts: true,
            },
            active_markets: vec![active_identity.clone()],
            added_markets: previous_market
                .is_none_or(|previous| previous != market_id)
                .then(|| market_id.into())
                .into_iter()
                .collect(),
            removed_markets: previous_market
                .filter(|previous| *previous != market_id)
                .map_or_else(Vec::new, |previous| vec![previous.into()]),
            added_market_identities: previous_market
                .is_none_or(|previous| previous != market_id)
                .then_some(active_identity)
                .into_iter()
                .collect(),
            removed_market_identities: removed_identities,
            excluded_markets: Vec::new(),
            validated_at,
        });
        live
    }

    fn two_market_dynamic_live() -> PolymarketLiveConfig {
        let active_markets = vec![
            PolymarketUniverseMarket {
                market_id: "1".into(),
                condition_id: "condition-1".into(),
                token_ids: vec!["10".into(), "20".into()],
                end_at: "2026-09-01T00:00:00Z".parse().unwrap(),
            },
            PolymarketUniverseMarket {
                market_id: "2".into(),
                condition_id: "condition-2".into(),
                token_ids: vec!["30".into(), "40".into()],
                end_at: "2026-09-01T00:00:00Z".parse().unwrap(),
            },
        ];
        let initial_book_frames = ["10", "20", "30", "40"]
            .into_iter()
            .map(|token| {
                json!({
                    "event_type":"book", "asset_id":token, "tick_size":"0.01",
                    "bids":[{"price":"0.40","size":"10"}],
                    "asks":[{"price":"0.60","size":"10"}],
                    "timestamp":"2026-08-30T00:00:00Z"
                })
            })
            .collect::<Vec<_>>();
        PolymarketLiveConfig {
            scope_id: "s".into(),
            token_ids: vec!["10".into(), "20".into(), "30".into(), "40".into()],
            market_ids: vec!["1".into(), "2".into()],
            market_count: Some(2),
            catalog_revision: Some("catalog-s".into()),
            catalog_frame: Some(json!({
                "event_type":"catalog_revision", "catalog_revision":"catalog-s",
                "markets":[test_scope_market("1", "10", "20"), test_scope_market("2", "30", "40")],
                "negative_risk_relations":[]
            })),
            scope_file_sha256: Some("a".repeat(64)),
            scope_file_path: None,
            source_manifest_sha256: Some("b".repeat(64)),
            catalog_index_sha256: Some("c".repeat(64)),
            catalog_sha256: Some("d".repeat(64)),
            registry_sha256: Some("e".repeat(64)),
            universe: Some(PolymarketUniverseContract {
                schema_version: "marketcow.polymarket.universe.v2".into(),
                universe_id: "s".into(),
                generation: 1,
                target_market_count: 2,
                minimum_market_count: 1,
                filters: PolymarketUniverseFilters {
                    require_two_sided_books: true,
                    require_complete_instrument_facts: true,
                },
                active_markets: active_markets.clone(),
                added_markets: vec!["1".into(), "2".into()],
                removed_markets: Vec::new(),
                added_market_identities: active_markets,
                removed_market_identities: Vec::new(),
                excluded_markets: Vec::new(),
                validated_at: "2026-08-30T00:00:00Z".parse().unwrap(),
            }),
            initial_book_frames,
        }
    }

    #[test]
    fn dynamic_universe_transition_is_monotonic_and_delta_exact() {
        let universe_id = "a".repeat(64);
        let current = dynamic_universe_live(&universe_id, 1, "1", "10", "20", None);
        let next = dynamic_universe_live(&universe_id, 2, "2", "30", "40", Some("1"));
        validate_polymarket_universe_transition(&current, &next).unwrap();

        let mut skipped = next.clone();
        skipped.universe.as_mut().unwrap().generation = 3;
        assert_eq!(
            validate_polymarket_universe_transition(&current, &skipped),
            Err("universe_generation_not_monotonic".into())
        );
        let mut mixed = next;
        mixed.universe.as_mut().unwrap().removed_markets.clear();
        assert_eq!(
            validate_polymarket_universe_transition(&current, &mixed),
            Err("universe_membership_delta_mismatch".into())
        );
    }

    #[tokio::test]
    async fn connection_boundary_stays_global_without_recursive_transport_restart() {
        let (_dir, state) = test_state();
        let live = two_market_dynamic_live();
        {
            let mut runtime = state.runtime.lock().await;
            seed_polymarket_scope_catalog(&mut runtime, Some(&live)).unwrap();
            state.projection.store(runtime.projection());
        }
        state.active_polymarket_scope.store(Some(Arc::new(live)));
        assert!(polymarket_projection_ready(
            &state,
            &state.projection.load_full()
        ));

        let mut published = state.stream.subscribe();
        let received_at = Utc::now();
        let applied = apply_polymarket_transport_frames(
            &state,
            vec![marketcow_polymarket::RawTransportFrame {
                raw_payload: json!({
                    "event_type":"source_gap", "asset_id":"10",
                    "reason":"upstream_connection_boundary", "attempt":2,
                    "timestamp":received_at.timestamp_millis().to_string()
                }),
                received_at,
            }],
        )
        .await
        .unwrap();
        assert_eq!(applied, 1);
        let record = published.recv().await.unwrap();
        assert!(record.market.is_none());
        assert_eq!(
            record.fail_closed_reason.as_deref(),
            Some("source_gap:upstream_connection_boundary")
        );
        assert!(!state.projection.load().ready);
        assert!(matches!(
            marketcow_api::stream_record("s", &record).unwrap().payload,
            marketcow_api::StreamPayload::Event { .. }
        ));
        assert_eq!(
            polymarket_stream_resync_reason(&record),
            Some("upstream_projection_gap")
        );
    }

    #[tokio::test]
    async fn stale_authoritative_mismatch_quarantines_only_the_affected_token() {
        let (_dir, state) = test_state();
        let live = two_market_dynamic_live();
        {
            let mut runtime = state.runtime.lock().await;
            seed_polymarket_scope_catalog(&mut runtime, Some(&live)).unwrap();
            let stale_at = Utc::now() - chrono::Duration::seconds(31);
            runtime
                .apply_raw(
                    json!({
                        "event_type":"book", "asset_id":"10", "tick_size":"0.01",
                        "bids":[{"price":"0.40","size":"10"}],
                        "asks":[{"price":"0.60","size":"10"}],
                        "timestamp":stale_at.timestamp_millis().to_string()
                    }),
                    stale_at,
                )
                .unwrap();
            state.projection.store(runtime.projection());
        }
        state
            .active_polymarket_scope
            .store(Some(Arc::new(live.clone())));

        let frames = ["10", "20", "30", "40"]
            .into_iter()
            .map(|token_id| {
                let received_at = Utc::now();
                marketcow_polymarket::RawTransportFrame {
                    raw_payload: json!({
                        "event_type":"book", "asset_id":token_id, "tick_size":"0.01",
                        "bids":[{"price":"0.40","size":"10"}],
                        "asks":[{
                            "price":"0.60",
                            "size":if token_id == "10" { "11" } else { "10" }
                        }],
                        "timestamp":received_at.timestamp_millis().to_string()
                    }),
                    received_at,
                }
            })
            .collect::<Vec<_>>();

        let validation = validate_polymarket_book_refreshes(&state, &live, &frames, &[])
            .await
            .unwrap();
        assert_eq!(validation.matched_books, 3);
        assert_eq!(validation.mismatched_books, 1);
        assert_eq!(validation.quarantine_token_ids, ["10"]);

        let persisted = quarantine_polymarket_tokens_from_authoritative_refresh(
            &state,
            &validation.quarantine_token_ids,
        )
        .await
        .unwrap();
        assert_eq!(persisted, 1);
        let projection = state.projection.load_full();
        assert!(polymarket_projection_ready(&state, &projection));
        assert_eq!(projection.active_market_ids(), BTreeSet::from(["2".into()]));
        assert_eq!(
            projection.unavailable_token_ids(),
            BTreeSet::from(["10".into()])
        );
        assert_eq!(
            projection.available_token_ids(),
            BTreeSet::from(["20".into(), "30".into(), "40".into()])
        );
        assert_eq!(
            projection.market_health["1"].reason_code.as_deref(),
            Some("source_gap:authoritative_book_unavailable_after_freshness_deadline")
        );
    }

    #[tokio::test]
    async fn stale_token_missing_from_partial_refresh_is_quarantined_locally() {
        let (_dir, state) = test_state();
        let live = two_market_dynamic_live();
        {
            let mut runtime = state.runtime.lock().await;
            seed_polymarket_scope_catalog(&mut runtime, Some(&live)).unwrap();
            let stale_at = Utc::now() - chrono::Duration::seconds(31);
            runtime
                .apply_raw(
                    json!({
                        "event_type":"book", "asset_id":"10", "tick_size":"0.01",
                        "bids":[{"price":"0.40","size":"10"}],
                        "asks":[{"price":"0.60","size":"10"}],
                        "timestamp":stale_at.timestamp_millis().to_string()
                    }),
                    stale_at,
                )
                .unwrap();
            state.projection.store(runtime.projection());
        }
        state
            .active_polymarket_scope
            .store(Some(Arc::new(live.clone())));
        state
            .polymarket_book_validations
            .store(Arc::new(PolymarketBookValidationProjection {
                scope_id: live.scope_id.clone(),
                verified_at: BTreeMap::from([("10".into(), Utc::now())]),
            }));

        let frames = ["20", "30", "40"]
            .into_iter()
            .map(|token_id| {
                let received_at = Utc::now();
                marketcow_polymarket::RawTransportFrame {
                    raw_payload: json!({
                        "event_type":"book", "asset_id":token_id, "tick_size":"0.01",
                        "bids":[{"price":"0.40","size":"10"}],
                        "asks":[{"price":"0.60","size":"10"}],
                        "timestamp":received_at.timestamp_millis().to_string()
                    }),
                    received_at,
                }
            })
            .collect::<Vec<_>>();
        let transient = validate_polymarket_book_refreshes(&state, &live, &frames, &["10".into()])
            .await
            .unwrap();
        assert!(transient.quarantine_token_ids.is_empty());
        assert!(
            state
                .polymarket_book_validations
                .load()
                .verified_at
                .contains_key("10")
        );

        state
            .polymarket_book_validations
            .store(Arc::new(PolymarketBookValidationProjection {
                scope_id: live.scope_id.clone(),
                verified_at: BTreeMap::from([(
                    "10".into(),
                    Utc::now() - chrono::Duration::seconds(31),
                )]),
            }));
        let validation = validate_polymarket_book_refreshes(&state, &live, &frames, &["10".into()])
            .await
            .unwrap();
        assert_eq!(validation.matched_books, 3);
        assert_eq!(validation.mismatched_books, 0);
        assert_eq!(validation.missing_books, 1);
        assert_eq!(validation.quarantine_token_ids, ["10"]);

        let persisted = quarantine_polymarket_tokens_from_authoritative_refresh(
            &state,
            &validation.quarantine_token_ids,
        )
        .await
        .unwrap();
        assert_eq!(persisted, 1);
        assert_eq!(
            state.projection.load().active_market_ids(),
            BTreeSet::from(["2".into()])
        );
    }

    #[tokio::test]
    async fn single_market_fault_is_local_and_recovers_from_two_token_atomic_snapshot() {
        let (_dir, state) = test_state();
        let live = two_market_dynamic_live();
        {
            let mut runtime = state.runtime.lock().await;
            seed_polymarket_scope_catalog(&mut runtime, Some(&live)).unwrap();
            state.projection.store(runtime.projection());
        }
        state
            .active_polymarket_scope
            .store(Some(Arc::new(live.clone())));
        assert!(polymarket_projection_ready(
            &state,
            &state.projection.load_full()
        ));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, app(server_state)).await.unwrap();
        });
        let (mut stream_client, _) =
            tokio_tungstenite::connect_async(format!("ws://{address}/v1/market-data/stream"))
                .await
                .unwrap();
        let subscription = next_stream_frame(&mut stream_client).await;
        let initial_boundary = subscription.cursor;
        assert!(matches!(
            subscription.payload,
            marketcow_api::StreamPayload::Subscription { .. }
        ));

        let mut published = state.stream.subscribe();
        let received_at = Utc::now();
        let applied = apply_polymarket_transport_frames(
            &state,
            vec![marketcow_polymarket::RawTransportFrame {
                raw_payload: json!({
                    "event_type":"source_gap", "asset_id":"10",
                    "reason":"injected_single_market_loss",
                    "timestamp":received_at.timestamp_millis().to_string()
                }),
                received_at,
            }],
        )
        .await
        .unwrap();
        assert_eq!(applied, 1);
        let quarantined = published.recv().await.unwrap();
        assert_eq!(
            quarantined
                .market
                .as_ref()
                .map(|market| market.market_id.as_str()),
            Some("1")
        );
        assert!(matches!(
            marketcow_api::stream_record("s", &quarantined).unwrap().payload,
            marketcow_api::StreamPayload::MarketQuarantined { ref control }
                if control.market_id == "1"
                    && control.affected_token_ids == ["10"]
                    && !control.full_sync_required
        ));
        let live_quarantine = next_stream_frame(&mut stream_client).await;
        assert_eq!(live_quarantine.cursor, initial_boundary + 1);
        assert!(matches!(
            live_quarantine.payload,
            marketcow_api::StreamPayload::MarketQuarantined { ref control }
                if control.market_id == "1"
        ));
        let projection = state.projection.load_full();
        assert!(polymarket_projection_ready(&state, &projection));
        assert_eq!(projection.active_market_ids(), BTreeSet::from(["2".into()]));
        assert!(projection.unresolved_gaps.is_empty());

        let scope_response = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/v1/prediction-markets/polymarket/live/scope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(scope_response.status(), StatusCode::OK);
        let scope: serde_json::Value = serde_json::from_slice(
            &to_bytes(scope_response.into_body(), 256 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            scope["schema_version"],
            "marketcow.polymarket.scope-discovery.v5"
        );
        assert_eq!(scope["market_count"], 1);
        assert_eq!(scope["token_count"], 2);
        assert_eq!(scope["configured_market_count"], 2);
        assert_eq!(scope["configured_token_count"], 4);
        assert_eq!(scope["available_token_count"], 3);
        assert_eq!(scope["unavailable_token_count"], 1);
        assert_eq!(scope["active_market_ids"], json!(["2"]));
        assert_eq!(scope["tradable_market_ids"], json!(["2"]));
        assert_eq!(scope["tradable_token_ids"], json!(["30", "40"]));
        assert_eq!(scope["configured_market_ids"], json!(["1", "2"]));
        assert_eq!(
            scope["configured_token_ids"],
            json!(["10", "20", "30", "40"])
        );
        assert_eq!(scope["available_token_ids"], json!(["20", "30", "40"]));
        assert_eq!(scope["unavailable_token_ids"], json!(["10"]));
        assert_eq!(scope["unavailable_tokens"][0]["token_id"], "10");
        assert_eq!(scope["unavailable_tokens"][0]["market_id"], "1");
        assert_eq!(
            scope["unavailable_tokens"][0]["availability_status"],
            "temporarily_unavailable"
        );
        assert_eq!(
            scope["unavailable_tokens"][0]["reason_code"],
            "source_gap:injected_single_market_loss"
        );
        assert!(scope["unavailable_tokens"][0]["unavailable_since"].is_string());
        assert_eq!(
            scope["unavailable_tokens"][0]["availability_revision"]
                .as_str()
                .map(str::len),
            Some(64)
        );
        assert_eq!(scope["quarantined_market_ids"], json!(["1"]));
        assert_eq!(scope["scope_file_sha256"].as_str().map(str::len), Some(64));
        for field in [
            "gap_from",
            "gap_to",
            "reason_code",
            "retry_after",
            "source_observed_at",
            "last_recovered_at",
            "catalog_revision",
            "last_event_revision",
        ] {
            assert!(scope["market_health"][0].get(field).is_some(), "{field}");
        }

        let full_response = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/v1/prediction-markets/polymarket/live/full-sync")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(full_response.status(), StatusCode::OK);
        let full: serde_json::Value = serde_json::from_slice(
            &to_bytes(full_response.into_body(), 512 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(full["snapshot"]["markets"].as_array().unwrap().len(), 1);
        assert_eq!(full["snapshot"]["books"].as_array().unwrap().len(), 2);
        assert_eq!(
            full["snapshot"]["configured_markets"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            full["snapshot"]["available_token_ids"],
            json!(["20", "30", "40"])
        );
        assert_eq!(full["snapshot"]["unavailable_token_ids"], json!(["10"]));
        assert_eq!(full["health"]["quarantined_market_count"], 1);

        let held_market_response = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/v1/prediction-markets/polymarket/live/markets/1/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(held_market_response.status(), StatusCode::OK);
        let held_market: serde_json::Value = serde_json::from_slice(
            &to_bytes(held_market_response.into_body(), 256 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(held_market["market"]["market_id"], "1");
        assert_eq!(held_market["books"].as_array().unwrap().len(), 2);
        assert_eq!(held_market["usable_for_new_opportunities"], false);
        assert_eq!(held_market["stable_identity_for_position_monitoring"], true);

        let healthy_event_at = Utc::now();
        apply_polymarket_transport_frames(
            &state,
            vec![marketcow_polymarket::RawTransportFrame {
                raw_payload: json!({
                    "event_type":"price_change",
                    "timestamp":healthy_event_at.timestamp_millis().to_string(),
                    "price_changes":[{
                        "asset_id":"30", "side":"BUY", "price":"0.39", "size":"1",
                        "best_bid":"0.40", "best_ask":"0.60"
                    }]
                }),
                received_at: healthy_event_at,
            }],
        )
        .await
        .unwrap();
        let healthy_record = published.recv().await.unwrap();
        assert_eq!(
            healthy_record
                .market
                .as_ref()
                .map(|market| market.market_id.as_str()),
            Some("2")
        );
        let live_healthy_event = next_stream_frame(&mut stream_client).await;
        assert_eq!(live_healthy_event.cursor, initial_boundary + 2);
        assert!(matches!(
            live_healthy_event.payload,
            marketcow_api::StreamPayload::Event { ref event }
                if event.market_id.as_deref() == Some("2") && event.market_sequence.is_some()
        ));

        let recovery_frames = ["10", "20"]
            .into_iter()
            .map(|token_id| {
                let received_at = Utc::now();
                marketcow_polymarket::RawTransportFrame {
                    raw_payload: json!({
                        "event_type":"book", "asset_id":token_id, "tick_size":"0.01",
                        "bids":[{"price":"0.40","size":"10"}],
                        "asks":[{"price":"0.60","size":"10"}],
                        "timestamp":received_at.timestamp_millis().to_string()
                    }),
                    received_at,
                }
            })
            .collect::<Vec<_>>();
        let recovery = recover_quarantined_polymarket_markets(&state, &live, &recovery_frames)
            .await
            .unwrap();
        assert_eq!(recovery.attempted, 1);
        assert_eq!(recovery.recovered, 1);
        assert_eq!(recovery.rejected, 0);
        let first_recovery = published.recv().await.unwrap();
        let second_recovery = published.recv().await.unwrap();
        assert!(matches!(
            marketcow_api::stream_record("s", &first_recovery)
                .unwrap()
                .payload,
            marketcow_api::StreamPayload::MarketRecoveryStarted { .. }
        ));
        assert!(matches!(
            marketcow_api::stream_record("s", &second_recovery).unwrap().payload,
            marketcow_api::StreamPayload::MarketRecovered { ref control }
                if control.market_snapshot_required && !control.full_sync_required
        ));
        assert!(matches!(
            next_stream_frame(&mut stream_client).await.payload,
            marketcow_api::StreamPayload::MarketRecoveryStarted { .. }
        ));
        assert!(matches!(
            next_stream_frame(&mut stream_client).await.payload,
            marketcow_api::StreamPayload::MarketRecovered { .. }
        ));
        assert_eq!(
            state.projection.load().active_market_ids(),
            BTreeSet::from(["1".into(), "2".into()])
        );

        let boundary = state.projection.load().cursor;
        let replay = polymarket_wal_replay_records(&state, boundary - 3, boundary)
            .await
            .unwrap();
        assert_eq!(replay.len(), 3);
        assert_eq!(replay.first().unwrap().event.cursor, boundary - 2);
        assert_eq!(replay.last().unwrap().event.cursor, boundary);

        let mut minimum_two = live;
        minimum_two.universe.as_mut().unwrap().minimum_market_count = 2;
        state
            .active_polymarket_scope
            .store(Some(Arc::new(minimum_two)));
        let below_minimum_at = Utc::now();
        apply_polymarket_transport_frames(
            &state,
            vec![marketcow_polymarket::RawTransportFrame {
                raw_payload: json!({
                    "event_type":"source_gap", "asset_id":"10",
                    "reason":"injected_below_minimum",
                    "timestamp":below_minimum_at.timestamp_millis().to_string()
                }),
                received_at: below_minimum_at,
            }],
        )
        .await
        .unwrap();
        assert!(!polymarket_projection_ready(
            &state,
            &state.projection.load_full()
        ));
        assert!(matches!(
            next_stream_frame(&mut stream_client).await.payload,
            marketcow_api::StreamPayload::MarketQuarantined { .. }
        ));
        assert!(matches!(
            next_stream_frame(&mut stream_client).await.payload,
            marketcow_api::StreamPayload::GlobalResyncRequired {
                ref reason,
                full_sync_required: true,
                ..
            } if reason == "projection_unready_or_stale"
        ));
        server.abort();
    }

    fn write_dynamic_universe_scope(path: &Path, live: &PolymarketLiveConfig) -> String {
        let payload = json!({
            "schema_version":"marketcow.polymarket.rust-live-scope.v4",
            "scope_id":live.scope_id,
            "market_count":live.market_count,
            "token_count":live.token_ids.len(),
            "market_ids":live.market_ids,
            "token_ids":live.token_ids,
            "catalog_revision":live.catalog_revision,
            "catalog_frame":live.catalog_frame,
            "universe":live.universe,
            "initial_book_frames":live.initial_book_frames,
            "source":{
                "manifest_sha256":"b".repeat(64),
                "catalog_index_sha256":"c".repeat(64),
                "catalog_sha256":"d".repeat(64),
                "registry_sha256":"e".repeat(64)
            }
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        fs::write(path, &bytes).unwrap();
        hex::encode(Sha256::digest(bytes))
    }

    #[test]
    fn dynamic_universe_scope_v4_is_strict_and_restart_pinned() {
        let (dir, mut state) = test_state();
        let universe_id = "c".repeat(64);
        let live = dynamic_universe_live(&universe_id, 1, "1", "10", "20", None);
        let activated_at = Utc::now();
        let source = dir.path().join("generation-1.json");
        let digest = write_dynamic_universe_scope(&source, &live);
        let loaded =
            load_polymarket_scope_file_at(&universe_id, &source.to_string_lossy(), activated_at)
                .unwrap()
                .unwrap();
        assert_eq!(loaded.scope_file_sha256.as_deref(), Some(digest.as_str()));
        assert_eq!(loaded.universe.as_ref().unwrap().generation, 1);
        assert_eq!(loaded.initial_book_frames.len(), 2);

        let restarted = load_polymarket_scope_file_at(
            &universe_id,
            &source.to_string_lossy(),
            activated_at + chrono::Duration::days(20),
        )
        .unwrap()
        .unwrap();
        let mut restart_config = state.config.clone();
        restart_config.polymarket_live = Some(restarted);
        assert_eq!(
            expired_dynamic_scope_tokens(
                &restart_config,
                activated_at + chrono::Duration::days(20),
            ),
            vec!["10", "20"],
        );

        state.config.scope_id = universe_id.clone();
        state.config.polymarket_live = Some(loaded.clone());
        let persisted = persist_active_polymarket_universe(&state, &loaded).unwrap();
        assert_eq!(fs::read(&persisted).unwrap(), fs::read(&source).unwrap());
        assert_eq!(
            runtime_config(&state.config, "config-v1").root,
            dir.path()
                .join("polymarket-universes")
                .join(&universe_id)
                .join("generation-00000000000000000001")
        );

        let mut invalid = live;
        invalid.initial_book_frames[0]["asks"] = json!([]);
        let invalid_path = dir.path().join("invalid-generation.json");
        write_dynamic_universe_scope(&invalid_path, &invalid);
        assert!(
            load_polymarket_scope_file_at(
                &universe_id,
                &invalid_path.to_string_lossy(),
                activated_at,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn dynamic_universe_candidate_isolated_ready_and_cursor_contiguous() {
        let (dir, state) = test_state();
        let universe_id = "b".repeat(64);
        let current = dynamic_universe_live(&universe_id, 1, "1", "10", "20", None);
        let mut runtime =
            marketcow_runtime::PolymarketRuntime::open(marketcow_runtime::RuntimeConfig {
                root: dir.path().join("active-universe"),
                scope_id: universe_id.clone(),
                config_revision: "test-config-v1".into(),
                wal_segment_bytes: 1_024,
                recent_event_capacity: 100,
            })
            .unwrap();
        seed_polymarket_scope_catalog(&mut runtime, Some(&current)).unwrap();
        let current_projection = runtime.projection();
        assert!(current_projection.ready);
        let current_cursor = current_projection.cursor;
        *state.runtime.lock().await = runtime;
        state.projection.store(current_projection);
        state
            .active_polymarket_scope
            .store(Some(Arc::new(current.clone())));

        let next = dynamic_universe_live(&universe_id, 2, "2", "30", "40", Some("1"));
        let candidate = prepare_polymarket_scope_candidate(&state, &current, &next)
            .await
            .unwrap();
        let candidate_projection = candidate.projection();
        assert!(candidate_projection.ready);
        assert!(candidate_projection.cursor > current_cursor);
        assert_eq!(
            candidate_projection.markets.keys().collect::<Vec<_>>(),
            vec!["2"]
        );
        assert_eq!(candidate_projection.books.len(), 2);
        assert!(
            dir.path()
                .join("polymarket-universes")
                .join(&universe_id)
                .join("generation-00000000000000000002")
                .join("checkpoint-manifest.json")
                .exists()
        );
        let expected_hash = candidate_projection.hash();
        let expected_cursor = candidate_projection.cursor;
        drop(candidate);
        let mut restart_config = state.config.clone();
        restart_config.scope_id = universe_id.clone();
        restart_config.polymarket_live = Some(next.clone());
        let recovered = marketcow_runtime::PolymarketRuntime::open(runtime_config(
            &restart_config,
            "test-config-v1",
        ))
        .unwrap();
        assert_eq!(recovered.projection().cursor, expected_cursor);
        assert_eq!(recovered.projection().hash(), expected_hash);
        state
            .active_polymarket_scope
            .store(Some(Arc::new(next.clone())));
        assert!(polymarket_atomic_view(&state).is_none());
        let mixed = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/v1/prediction-markets/polymarket/live/full-sync")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(mixed.status(), StatusCode::SERVICE_UNAVAILABLE);
        state
            .active_polymarket_scope
            .store(Some(Arc::new(current.clone())));
        state.projection.store(candidate_projection);
        state
            .active_polymarket_scope
            .store(Some(Arc::new(next.clone())));
        let control = polymarket_universe_change_frame(
            &state,
            current.universe.as_ref(),
            state.projection.load().cursor,
        )
        .expect("generation change must force an explicit resync control frame");
        assert!(matches!(
            control.payload,
            marketcow_api::StreamPayload::UniverseChanged {
                old_generation: 1,
                new_generation: 2,
                full_sync_required: true,
                ..
            }
        ));
        let scope_response = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/v1/prediction-markets/polymarket/live/scope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(scope_response.status(), StatusCode::OK);
        let scope_body: serde_json::Value =
            serde_json::from_slice(&to_bytes(scope_response.into_body(), 32_768).await.unwrap())
                .unwrap();
        assert_eq!(
            scope_body["schema_version"],
            "marketcow.polymarket.scope-discovery.v5"
        );
        assert_eq!(scope_body["universe_id"], universe_id);
        assert_eq!(scope_body["generation"], 2);
        assert_eq!(scope_body["active_markets"][0]["market_id"], "2");
        assert_eq!(scope_body["added_markets"], json!(["2"]));
        assert_eq!(scope_body["removed_markets"], json!(["1"]));

        let full_sync = app(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/prediction-markets/polymarket/live/full-sync")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(full_sync.status(), StatusCode::OK);
        let full_sync: serde_json::Value =
            serde_json::from_slice(&to_bytes(full_sync.into_body(), 128 * 1024).await.unwrap())
                .unwrap();
        assert_eq!(
            full_sync["universe_schema_version"],
            "marketcow.polymarket.universe.v3"
        );
        assert_eq!(full_sync["universe_generation"], 2);
        assert_eq!(
            full_sync["boundary_cursor"],
            full_sync["checkpoint"]["checkpoint_cursor"]
        );
        assert_eq!(
            full_sync["snapshot"]["markets"].as_array().unwrap().len(),
            1
        );
        assert_eq!(full_sync["snapshot"]["books"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn startup_reopens_hash_pinned_scope_from_dynamic_runtime_root() {
        let (_dir, state) = test_state();
        let mut config = state.config.clone();
        config.scope_id = "next-scope".into();
        config.polymarket_live = Some(dynamic_live_scope("next-scope", "2", "30", "40"));

        let runtime = runtime_config(&config, "test-config-v1");

        assert_eq!(
            runtime.root,
            config
                .storage_root
                .join("polymarket-scopes")
                .join("next-scope")
        );
        assert_eq!(runtime.scope_id, "next-scope");
    }

    #[test]
    fn same_catalog_revision_upgrades_legacy_projection_with_atomic_fee_facts() {
        let dir = tempdir().unwrap();
        let live = dynamic_live_scope("fee-upgrade-scope", "1", "10", "20");
        let mut runtime =
            marketcow_runtime::PolymarketRuntime::open(marketcow_runtime::RuntimeConfig {
                root: dir.path().join("fee-upgrade-scope"),
                scope_id: live.scope_id.clone(),
                config_revision: "test-config-v1".into(),
                wal_segment_bytes: 1_024,
                recent_event_capacity: 100,
            })
            .unwrap();
        let mut legacy = live.catalog_frame.clone().unwrap();
        legacy["markets"][0]["instrument_facts"]
            .as_object_mut()
            .unwrap()
            .remove("fee_schedule");
        runtime.apply_raw(legacy, Utc::now()).unwrap();
        runtime.checkpoint().unwrap();
        assert!(
            runtime.projection().markets["1"]
                .instrument_facts
                .as_ref()
                .unwrap()
                .fee_schedule
                .is_none()
        );

        seed_polymarket_scope_catalog(&mut runtime, Some(&live)).unwrap();
        let projection = runtime.projection();
        assert_eq!(projection.cursor, 2);
        assert!(
            projection.markets["1"]
                .instrument_facts
                .as_ref()
                .unwrap()
                .fee_schedule
                .as_ref()
                .is_some_and(marketcow_core::MarketFeeSchedule::is_complete)
        );
    }

    #[tokio::test]
    async fn dynamic_polymarket_scope_switch_is_atomic_unready_and_checkpointed() {
        let (dir, state) = test_state();
        let current = dynamic_live_scope("current-scope", "1", "10", "20");
        state
            .active_polymarket_scope
            .store(Some(Arc::new(current.clone())));
        let mut activated = dynamic_live_scope("next-scope", "2", "30", "40");
        let fee = &mut activated.catalog_frame.as_mut().unwrap()["markets"][0]["instrument_facts"]
            ["fee_schedule"];
        fee["taker_rate"] = json!("0.07");
        fee["revision"] = json!("c".repeat(64));
        let mut candidate =
            marketcow_runtime::PolymarketRuntime::open(marketcow_runtime::RuntimeConfig {
                root: dir.path().join("next-scope"),
                scope_id: activated.scope_id.clone(),
                config_revision: "test-config-v1".into(),
                wal_segment_bytes: 1_024,
                recent_event_capacity: 100,
            })
            .unwrap();
        seed_polymarket_scope_catalog(&mut candidate, Some(&activated)).unwrap();

        let (activated, receipt) =
            commit_polymarket_scope_switch(&state, &current, activated, candidate)
                .await
                .unwrap();
        let projection = state.projection.load_full();
        assert_eq!(receipt.previous_scope_id, "current-scope");
        assert_eq!(receipt.active_scope_id, "next-scope");
        assert_eq!(projection.scope_id, "next-scope");
        assert_eq!(
            projection.catalog_revision.as_deref(),
            Some("catalog-next-scope")
        );
        assert_eq!(projection.markets.len(), 1);
        let published_fee = projection.markets["2"]
            .instrument_facts
            .as_ref()
            .unwrap()
            .fee_schedule
            .as_ref()
            .unwrap();
        assert_eq!(published_fee.taker_rate.to_string(), "0.07");
        assert_eq!(published_fee.revision, "c".repeat(64));
        let catalog_event = {
            let runtime = state.runtime.lock().await;
            clone_polymarket_recent_events(&runtime)
        };
        assert!(matches!(
            &catalog_event[0].event.kind,
            marketcow_core::EventKind::CatalogSnapshot { .. }
        ));
        assert!(projection.books.is_empty());
        assert!(!polymarket_projection_ready(&state, &projection));
        assert_eq!(activated.scope_id, "next-scope");
        assert!(
            dir.path()
                .join("polymarket/checkpoint-manifest.json")
                .exists()
        );
    }

    #[tokio::test]
    async fn same_scope_fee_revision_is_atomically_published_as_catalog_change() {
        let (dir, state) = test_state();
        let scope_id = state.config.scope_id.clone();
        let current = dynamic_live_scope(&scope_id, "1", "10", "20");
        state
            .active_polymarket_scope
            .store(Some(Arc::new(current.clone())));
        let mut activated = current.clone();
        activated.scope_file_sha256 = Some("9".repeat(64));
        let fee = &mut activated.catalog_frame.as_mut().unwrap()["markets"][0]["instrument_facts"]
            ["fee_schedule"];
        fee["taker_rate"] = json!("0.07");
        fee["revision"] = json!("c".repeat(64));

        let mut candidate =
            marketcow_runtime::PolymarketRuntime::open(marketcow_runtime::RuntimeConfig {
                root: dir.path().join("same-scope-fee-refresh"),
                scope_id: scope_id.clone(),
                config_revision: "test-config-v1".into(),
                wal_segment_bytes: 1_024,
                recent_event_capacity: 100,
            })
            .unwrap();
        seed_polymarket_scope_catalog(&mut candidate, Some(&activated)).unwrap();
        let mut stream = state.stream.subscribe();

        commit_polymarket_scope_switch(&state, &current, activated, candidate)
            .await
            .unwrap();

        let event = stream.try_recv().expect("catalog change must be published");
        assert!(matches!(
            &event.event.kind,
            marketcow_core::EventKind::CatalogSnapshot { .. }
        ));
        assert_eq!(event.event.cursor, 1);
        let projection = state.projection.load_full();
        let schedule = projection.markets["1"]
            .instrument_facts
            .as_ref()
            .unwrap()
            .fee_schedule
            .as_ref()
            .unwrap();
        assert_eq!(schedule.taker_rate.to_string(), "0.07");
        assert_eq!(schedule.revision, "c".repeat(64));
        assert_eq!(projection.generation, event.event.cursor);
    }

    #[tokio::test]
    async fn exhausted_polymarket_transport_budget_retries_same_scope_without_admin_action() {
        let (_dir, state) = test_state();
        let current = dynamic_live_scope("current-scope", "1", "10", "20");
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (_scope_tx, mut scope_rx) = mpsc::channel(1);

        let recovered = wait_for_polymarket_retry_or_scope_recovery(
            &state,
            &current,
            &mut shutdown_rx,
            &mut scope_rx,
            Duration::from_millis(1),
        )
        .await
        .expect("the daemon must begin another bounded transport cycle");

        assert_eq!(recovered.scope_id, current.scope_id);
        assert_eq!(recovered.token_ids, current.token_ids);
        assert_eq!(recovered.scope_file_sha256, current.scope_file_sha256);
    }

    #[tokio::test]
    async fn scope_switch_bounds_a_stuck_upstream_close_handshake() {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let mut transport = tokio::spawn(async move {
            std::future::pending::<()>().await;
            Ok::<(), marketcow_polymarket::TransportError>(())
        });
        let started = std::time::Instant::now();

        stop_polymarket_transport_for_scope_switch(&shutdown_tx, &mut transport).await;

        assert!(transport.is_finished());
        assert!(
            started.elapsed()
                < POLYMARKET_TRANSPORT_SCOPE_SWITCH_STOP_TIMEOUT + Duration::from_secs(1)
        );
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

    fn canonical_bar_fixture(at: &str, version: u64) -> marketcow_storage::CanonicalBarRecord {
        let bar_time = DateTime::parse_from_rfc3339(at)
            .unwrap()
            .with_timezone(&Utc);
        marketcow_storage::CanonicalBarRecord {
            symbol: "AAPL".into(),
            interval: "1m".into(),
            adjustment: "raw".into(),
            bar_time,
            open: "10.1250".into(),
            high: "10.5000".into(),
            low: "10.0000".into(),
            close: "10.2500".into(),
            raw_close: None,
            adjustment_factor: None,
            factor_applicability: Some("applicable".into()),
            corporate_action_factor: Some("12.345678901234567890".into()),
            applied_adjustment_multiplier: Some("1.000000000000000000".into()),
            adjustment_reference_date: None,
            reference_factor: None,
            factor_source: Some("fixture".into()),
            factor_artifact_id: Some("factor-artifact".into()),
            factor_as_of: Some(bar_time + chrono::Duration::seconds(1)),
            volume: "100.12500000".into(),
            amount: None,
            selected_source: "fixture".into(),
            source_count: 1,
            quality_status: "single_source".into(),
            version,
            observed_at: bar_time + chrono::Duration::seconds(1),
            ingested_at: bar_time + chrono::Duration::seconds(61),
            raw_artifact_id: Some("raw-artifact".into()),
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
    async fn native_cached_quotes_preserve_order_decimal_strings_and_mcp_contract() {
        let (_dir, state) = test_state();
        state
            .market_data
            .put_memory(
                "AAPL.XNAS",
                json!({
                    "symbol":"AAPL.XNAS",
                    "bid_price":"213.8700",
                    "ask_price":"213.8800",
                    "bid_size":"100.00000000",
                    "ask_size":"90.00000000",
                    "currency":"USD",
                    "observed_at":"2026-08-28T00:00:00Z",
                    "source":"fixture"
                }),
            )
            .await;

        let http = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/quotes/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"symbols":[" aapl.xnas ","MSFT.XNAS","AAPL.XNAS"],"refresh":false,"provider":null,"allow_fallback":false}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(http.status(), StatusCode::OK);
        let http: serde_json::Value =
            serde_json::from_slice(&to_bytes(http.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(http["count"], 2);
        assert_eq!(http["items"][0]["bid_price"], "213.8700");
        assert_eq!(http["items"][1]["bid_size"], "100.00000000");
        assert_eq!(http["errors"][0]["symbol"], "MSFT.XNAS");

        let mcp = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":23,"method":"tools/call","params":{"name":"get_quotes","arguments":{"symbols":["AAPL.XNAS","MSFT.XNAS","AAPL.XNAS"]}}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let mcp: serde_json::Value =
            serde_json::from_slice(&to_bytes(mcp.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(mcp["result"]["isError"], false);
        assert_eq!(mcp["result"]["structuredContent"], http);

        let refresh = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/quotes/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"symbols":["AAPL.XNAS"],"refresh":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(refresh.status(), StatusCode::BAD_REQUEST);
        let refresh: serde_json::Value =
            serde_json::from_slice(&to_bytes(refresh.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(refresh["detail"]["code"], "cached_quotes_only");
    }

    #[tokio::test]
    async fn native_canonical_bars_are_snapshot_bound_signed_and_mcp_equal() {
        let (_dir, state) = test_state();
        state.instruments.insert_fixture(instrument_fixture()).await;
        state
            .market_data
            .put_canonical_memory(canonical_bar_fixture("2026-08-28T00:00:00Z", 1))
            .await;
        state
            .market_data
            .put_canonical_memory(canonical_bar_fixture("2026-08-28T00:01:00Z", 2))
            .await;
        let base = "/v1/canonical-bars/AAPL.XNAS?start=2026-08-28T00%3A00%3A00Z&end=2026-08-28T00%3A02%3A00Z&interval=1-MINUTE&adjustment=raw";
        let first = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri(format!("{base}&page_size=1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let first: serde_json::Value =
            serde_json::from_slice(&to_bytes(first.into_body(), 65_536).await.unwrap()).unwrap();
        assert_eq!(first["schema_version"], 1);
        assert_eq!(first["manifest"]["adjustment_contract_version"], 1);
        assert_eq!(first["manifest"]["row_count"], 2);
        assert_eq!(first["bars"][0]["open"], "10.1250");
        assert_eq!(
            first["bars"][0]["corporate_action_factor"],
            "12.345678901234567890"
        );
        assert_eq!(first["truncated"], true);
        let cursor = first["next_cursor"].as_str().unwrap();

        let second = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri(format!("{base}&page_size=1&cursor={cursor}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        let second: serde_json::Value =
            serde_json::from_slice(&to_bytes(second.into_body(), 65_536).await.unwrap()).unwrap();
        assert_eq!(second["count"], 1);
        assert_eq!(second["bars"][0]["row_version"], "2");
        assert_eq!(second["truncated"], false);

        let mut tampered = cursor.as_bytes().to_vec();
        tampered[0] = if tampered[0] == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(tampered).unwrap();
        let rejected = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri(format!("{base}&page_size=1&cursor={tampered}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);

        let http = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri(format!("{base}&page_size=2"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let http: serde_json::Value =
            serde_json::from_slice(&to_bytes(http.into_body(), 65_536).await.unwrap()).unwrap();
        let mcp = app(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"jsonrpc":"2.0","id":24,"method":"tools/call","params":{"name":"get_canonical_bars","arguments":{"instrument_id":"AAPL.XNAS","start":"2026-08-28T00:00:00Z","end":"2026-08-28T00:02:00Z","interval":"1-MINUTE","adjustment":"raw","page_size":2}}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let mcp: serde_json::Value =
            serde_json::from_slice(&to_bytes(mcp.into_body(), 65_536).await.unwrap()).unwrap();
        assert_eq!(mcp["result"]["isError"], false);
        assert_eq!(mcp["result"]["structuredContent"], http);

        let binding = CanonicalCursorBinding {
            instrument_id: "AAPL.XNAS".into(),
            start: "2026-08-28T00:00:00.000Z".into(),
            end: "2026-08-28T00:02:00.000Z".into(),
            interval: "1-MINUTE".into(),
            adjustment: "raw".into(),
            page_size: 1,
            snapshot_id: first["manifest"]["snapshot_id"].as_str().unwrap().into(),
        };
        let signer = CanonicalCursorSigner {
            secret: b"canonical-cursor-test-secret".to_vec(),
            ttl_seconds: 3_600,
        };
        let expired = signer.encode(binding.clone(), 1, 1).unwrap();
        assert!(signer.decode(&expired, &binding, 3_602).is_err());
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
    async fn enabled_hyperliquid_shadow_is_visible_and_fail_closed_while_starting() {
        let (dir, mut state) = test_state();
        let hub = marketcow_realtime::DurableRealtimeHub::open(
            dir.path().join("hyperliquid-shadow"),
            "hyperliquid-main",
            "test-config-v1",
            1_024,
            8,
        )
        .unwrap();
        state.hyperliquid_shadow = Some(hub.reader());
        state.config.hyperliquid_shadow = Some(HyperliquidShadowConfig {
            instruments: BTreeMap::from([("BTC-PERP.HYPL".into(), "BTC".into())]),
            maximum_source_delay_millis: 30_000,
        });

        let snapshot = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/v1/market-data/providers/hyperliquid/shadow/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(snapshot.status(), StatusCode::OK);
        let snapshot: serde_json::Value =
            serde_json::from_slice(&to_bytes(snapshot.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(snapshot["stream_id"], "hyperliquid-main");
        assert_eq!(snapshot["health"]["state"], "starting");

        let health = health_payload(&state);
        assert_eq!(health["status"], "degraded");
        assert_eq!(health["real_order_submission_enabled"], false);
        assert_eq!(health["components"]["hyperliquid_shadow"]["enabled"], true);
        let readiness = readiness(State(state.clone())).await;
        assert_eq!(readiness.status(), StatusCode::SERVICE_UNAVAILABLE);
        let metrics = metrics(State(state)).await.into_response();
        let metrics = String::from_utf8(
            to_bytes(metrics.into_body(), 16_384)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(metrics.contains("marketcow_hyperliquid_shadow_enabled 1"));
        assert!(metrics.contains("marketcow_hyperliquid_shadow_ready 0"));
    }

    #[test]
    fn realtime_lifecycle_audit_is_append_only_and_orders_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        let audit = AuditCoordinator::memory(&path);
        audit
            .record_lifecycle(
                "hyperliquid",
                "starting",
                json!({"recovered_public_sequence":7,"recovered_wal_cursor":7}),
            )
            .unwrap();
        audit
            .record_lifecycle("hyperliquid", "ready", json!({"attempt":1}))
            .unwrap();
        let records = fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["schema_version"], "marketcow.lifecycle-audit.v1");
        assert_eq!(records[0]["transition"], "starting");
        assert_eq!(records[1]["transition"], "ready");
        assert!(records.iter().all(|record| {
            record["domain"] == "hyperliquid"
                && record["real_order_submission_enabled"] == false
                && record["audit_id"]
                    .as_str()
                    .is_some_and(|value| value.starts_with("audit-"))
        }));
    }

    #[tokio::test]
    async fn hyperliquid_shadow_events_are_bounded_and_filtered_with_watermarks() {
        let (dir, mut state) = test_state();
        let mut hub = marketcow_realtime::DurableRealtimeHub::open(
            dir.path().join("hyperliquid-events"),
            "hyperliquid-main",
            "test-config-v1",
            1_024,
            8,
        )
        .unwrap();
        let (gateway, mut gateway_receiver) = mpsc::channel(4);
        hub.ingest(
            marketcow_realtime::TransportOutput::Connected { attempt: 1 },
            &gateway,
        )
        .unwrap();
        hub.ingest(
            marketcow_realtime::TransportOutput::SubscriptionsReady { attempt: 1 },
            &gateway,
        )
        .unwrap();
        let now = Utc::now();
        let event = marketcow_realtime::HyperliquidNormalizer::new(
            [("BTC-PERP.HYPL".into(), "BTC".into())],
            30_000,
        )
        .unwrap()
        .normalize(
            json!({"channel":"trades","data":[{
                "coin":"BTC","time":now.timestamp_millis(),"px":"65000.10",
                "sz":"0.001","side":"B","tid":42
            }]}),
            now,
        )
        .unwrap();
        hub.ingest(
            marketcow_realtime::TransportOutput::Events {
                attempt: 1,
                events: event,
            },
            &gateway,
        )
        .unwrap();
        let published = gateway_receiver.try_recv().unwrap();
        let (public_stream, _) = broadcast::channel(8);
        let _ = public_stream.send(published);
        state.hyperliquid_shadow = Some(hub.reader());
        state.hyperliquid_stream = Some(public_stream);
        state.config.hyperliquid_shadow = Some(HyperliquidShadowConfig {
            instruments: BTreeMap::from([("BTC-PERP.HYPL".into(), "BTC".into())]),
            maximum_source_delay_millis: 30_000,
        });

        let response = app(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/market-data/providers/hyperliquid/shadow/events?after_sequence=0&limit=8&instruments=BTC-PERP.HYPL&data_types=quote")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let page: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(page["schema_version"], "marketcow.realtime.events-page.v1");
        assert_eq!(page["current_sequence"], 1);
        assert_eq!(page["next_sequence"], 1);
        assert_eq!(page["frames"][0]["type"], "sequence_watermark");
        assert_eq!(page["real_order_submission_enabled"], false);
    }

    #[tokio::test]
    async fn unified_market_data_stream_dispatches_hyperliquid_without_breaking_default() {
        let (dir, mut state) = test_state();
        let mut hub = marketcow_realtime::DurableRealtimeHub::open(
            dir.path().join("hyperliquid-unified-stream"),
            "hyperliquid-main",
            "test-config-v1",
            1_024,
            8,
        )
        .unwrap();
        let (gateway, mut gateway_receiver) = mpsc::channel(4);
        hub.ingest(
            marketcow_realtime::TransportOutput::Connected { attempt: 1 },
            &gateway,
        )
        .unwrap();
        hub.ingest(
            marketcow_realtime::TransportOutput::SubscriptionsReady { attempt: 1 },
            &gateway,
        )
        .unwrap();
        let now = Utc::now();
        let events = marketcow_realtime::HyperliquidNormalizer::new(
            [("BTC-PERP.HYPL".into(), "BTC".into())],
            30_000,
        )
        .unwrap()
        .normalize(
            json!({"channel":"trades","data":[{
                "coin":"BTC","time":now.timestamp_millis(),"px":"65000.10",
                "sz":"0.001","side":"B","tid":43
            }]}),
            now,
        )
        .unwrap();
        hub.ingest(
            marketcow_realtime::TransportOutput::Events { attempt: 1, events },
            &gateway,
        )
        .unwrap();
        let published = gateway_receiver.try_recv().unwrap();
        let (public_stream, _) = broadcast::channel(8);
        let _ = public_stream.send(published);
        state.hyperliquid_shadow = Some(hub.reader());
        state.hyperliquid_stream = Some(public_stream);
        state.config.hyperliquid_shadow = Some(HyperliquidShadowConfig {
            instruments: BTreeMap::from([("BTC-PERP.HYPL".into(), "BTC".into())]),
            maximum_source_delay_millis: 30_000,
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app(state)).await.unwrap();
        });
        let (mut client, _) = tokio_tungstenite::connect_async(format!(
            "ws://{address}/v1/market-data/stream?provider=hyperliquid&after_cursor=0&instruments=BTC-PERP.HYPL&data_types=trade"
        ))
        .await
        .unwrap();
        let subscription = client.next().await.unwrap().unwrap().into_text().unwrap();
        let subscription: serde_json::Value = serde_json::from_str(&subscription).unwrap();
        assert_eq!(subscription["type"], "subscription");
        assert_eq!(subscription["provider"], "hyperliquid");
        assert_eq!(subscription["stream_id"], "hyperliquid-main");
        assert_eq!(subscription["real_order_submission_enabled"], false);
        let replay = client.next().await.unwrap().unwrap().into_text().unwrap();
        let replay: serde_json::Value = serde_json::from_str(&replay).unwrap();
        assert_eq!(replay["type"], "event");
        assert_eq!(replay["stream"]["sequence"], 1);
        assert_eq!(replay["stream"]["event"]["source"], "hyperliquid");
        client.close(None).await.unwrap();
        let unsupported = tokio_tungstenite::connect_async(format!(
            "ws://{address}/v1/market-data/stream?provider=unknown"
        ))
        .await
        .unwrap_err();
        assert!(matches!(
            unsupported,
            tokio_tungstenite::tungstenite::Error::Http(ref response)
                if response.status() == StatusCode::UNPROCESSABLE_ENTITY
        ));
        server.abort();
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
        let canonical_golden: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/mcp-get-canonical-bars-tool-v1.json"
        ))
        .unwrap();
        assert_eq!(
            listed["result"]["tools"],
            json!([
                health_golden,
                instrument_golden,
                marketcow_contracts::mcp_get_quotes_tool_definition(),
                canonical_golden
            ])
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
        state
            .market_data
            .put_memory(
                "AAPL.XNAS",
                json!({
                    "symbol":"AAPL.XNAS",
                    "bid_price":"213.8700",
                    "ask_price":"213.8800",
                    "currency":"USD",
                    "observed_at":"2026-08-28T00:00:00Z",
                    "source":"fixture"
                }),
            )
            .await;

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
        assert_eq!(
            listed["result"]["tools"][3],
            marketcow_contracts::mcp_get_quotes_tool_definition()
        );
        assert_eq!(
            listed["result"]["tools"][5],
            marketcow_contracts::mcp_get_canonical_bars_tool_definition()
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
        assert_eq!(called["result"]["structuredContent"]["count"], 1);
        assert_eq!(
            called["result"]["structuredContent"]["items"][0]["bid_price"],
            "213.8700"
        );
        assert_eq!(called["result"]["structuredContent"]["errors"], json!([]));

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
    async fn migration_checkpoint_is_cas_fenced_after_final_cutover() {
        let (_dir, state) = test_state();
        let migration = admin_migration(State(state.clone())).await.0;
        assert_eq!(migration["schema"], "marketcow.migration-control.v1");
        assert_eq!(migration["cutover_allowed"], true);
        assert_eq!(migration["real_order_submission_enabled"], false);
        assert_eq!(migration["tradude_may_manage_marketcow"], false);
        assert_eq!(
            migration["ownership_registry"]["sha256"],
            "7a7553c26b6d47a5f57a2f84dd5b0cdf96f872ed05ff44b0192df48c115a7250"
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
                authoritative_refresh_received_at: None,
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
        let response: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 16_384).await.unwrap()).unwrap();
        assert!(response["persistence_latency_us"].as_u64().is_some());
        assert!(response["publication_latency_us"].as_u64().is_some());
        assert!(response["apply_latency_us"].as_u64().is_some());
        assert_eq!(state.projection.load().cursor, 1);
        assert!(state.projection.load().ready);
        assert_eq!(state.runtime.lock().await.recent_events().len(), 1);

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

        let (mut retired_lineage, _) = tokio_tungstenite::connect_async(format!(
            "ws://{address}/v1/market-data/stream?after_cursor=999999"
        ))
        .await
        .unwrap();
        let retired_resync = next_stream_frame(&mut retired_lineage).await;
        assert!(matches!(
            retired_resync.payload,
            marketcow_api::StreamPayload::GlobalResyncRequired {
                ref reason,
                retryable: true,
                ..
            } if reason == "event_cursor_expired"
        ));
        let mut unavailable = (*state.projection.load_full()).clone();
        unavailable.ready = false;
        unavailable.fail_closed_reason = Some("test_gap".into());
        state.projection.store(Arc::new(unavailable));
        let resync = next_stream_frame(&mut resumed).await;
        assert!(matches!(
            resync.payload,
            marketcow_api::StreamPayload::GlobalResyncRequired {
                ref reason,
                retryable: true,
                ..
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

    #[tokio::test]
    async fn websocket_resume_window_never_returns_cursor_at_or_below_request() {
        let (_dir, state) = test_state();
        let first_at = Utc::now();
        for index in 0..3 {
            let observed_at = first_at + chrono::Duration::milliseconds(index);
            let response = admin_shadow_ingest(
                State(state.clone()),
                Extension(format!("resume-lower-bound-{index}")),
                Json(ShadowIngestRequest {
                    received_at: Some(observed_at),
                    raw_payload: json!({
                        "event_type":"book", "asset_id":"yes",
                        "timestamp":observed_at.to_rfc3339(), "tick_size":"0.01",
                        "bids":[{"price":format!("0.4{index}"),"size":"10"}],
                        "asks":[{"price":"0.60","size":"11"}]
                    }),
                }),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
        }
        let mut records = {
            let runtime = state.runtime.lock().await;
            clone_polymarket_recent_events(&runtime)
        };
        assert_eq!(records.len(), 3);
        records[0].event.cursor = 1_239_461;
        records[1].event.cursor = 1_239_471;
        records[2].event.cursor = 1_239_472;

        let requested = 1_239_470;
        let window = polymarket_replay_window(&records, requested, 1_239_472).unwrap();
        let replay = &records[window];
        assert_eq!(
            replay
                .iter()
                .map(|record| record.event.cursor)
                .collect::<Vec<_>>(),
            vec![1_239_471, 1_239_472]
        );
        assert!(replay.iter().all(|record| record.event.cursor > requested));

        records.swap(1, 2);
        assert_eq!(
            polymarket_replay_window(&records, requested, 1_239_472),
            Err("stream_replay_gap")
        );
    }

    #[tokio::test]
    async fn public_stream_converts_real_projection_failures_to_control_resync() {
        let (_dir, state) = test_state();
        let now = Utc::now();
        let response = admin_shadow_ingest(
            State(state.clone()),
            Extension("resync-reason-seed".into()),
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
        let mut applied = state.runtime.lock().await.recent_events()[0].clone();
        applied.applied = true;
        applied.fail_closed_reason = None;
        assert_eq!(polymarket_stream_resync_reason(&applied), None);

        let mut rejected = applied.clone();
        rejected.applied = false;
        rejected.fail_closed_reason = Some("crossed_or_locked_atomic_delta".into());
        assert_eq!(
            polymarket_stream_resync_reason(&rejected),
            Some("upstream_projection_gap")
        );

        let mut facts = applied;
        facts.event.kind = marketcow_core::EventKind::TickSizeChange {
            token_id: "yes".into(),
            old_tick_size: Some(marketcow_core::Price::parse_tick("0.01").unwrap()),
            new_tick_size: marketcow_core::Price::parse_tick("0.001").unwrap(),
            tick_version: "tick-v2".into(),
        };
        assert_eq!(
            polymarket_stream_resync_reason(&facts),
            Some("instrument_facts_changed")
        );
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
                authoritative_refresh_received_at: None,
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

    #[tokio::test]
    async fn recovered_ready_bit_cannot_expose_one_sided_dynamic_universe_book() {
        let (dir, state) = test_state();
        let universe_id = "f".repeat(64);
        let live = dynamic_universe_live(&universe_id, 1, "1", "10", "20", None);
        let mut runtime =
            marketcow_runtime::PolymarketRuntime::open(marketcow_runtime::RuntimeConfig {
                root: dir.path().join("one-sided-public-boundary"),
                scope_id: universe_id,
                config_revision: "test-config-v1".into(),
                wal_segment_bytes: 1_024,
                recent_event_capacity: 100,
            })
            .unwrap();
        seed_polymarket_scope_catalog(&mut runtime, Some(&live)).unwrap();
        let mut legacy_checkpoint = (*runtime.projection()).clone();
        legacy_checkpoint.books.get_mut("10").unwrap().asks.clear();
        legacy_checkpoint.ready = true;
        legacy_checkpoint.fail_closed_reason = None;
        state.projection.store(Arc::new(legacy_checkpoint));
        state.active_polymarket_scope.store(Some(Arc::new(live)));

        let scope = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/v1/prediction-markets/polymarket/live/scope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(scope.status(), StatusCode::OK);
        let scope: serde_json::Value =
            serde_json::from_slice(&to_bytes(scope.into_body(), 128 * 1024).await.unwrap())
                .unwrap();
        assert_eq!(scope["ready"], false);
        assert_eq!(scope["scope_status"], "unready");

        let full_sync = app(state)
            .oneshot(
                Request::builder()
                    .uri("/v1/prediction-markets/polymarket/live/full-sync")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(full_sync.status(), StatusCode::SERVICE_UNAVAILABLE);
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
            role: "public_gateway".into(),
            bind: "127.0.0.1:8870".parse().unwrap(),
            storage_root: dir.path().into(),
            wal_root: dir.path().join("wal"),
            worker_socket: dir.path().join("worker.sock"),
            scope_id: "shadow-seed".into(),
            real_order_submission_enabled: false,
            shadow_mode: true,
            maximum_book_age_ms: 30_000,
            legacy_mcp_url: None,
            polymarket_live: None,
            hyperliquid_shadow: None,
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

    #[test]
    fn authoritative_book_validation_is_bounded_well_inside_the_age_gate() {
        assert_eq!(
            polymarket_book_refresh_interval(3_600_000),
            Duration::from_secs(5 * 60)
        );
        assert_eq!(
            polymarket_book_refresh_interval(30_000),
            Duration::from_millis(7_500)
        );
        assert_eq!(
            polymarket_book_refresh_interval(1),
            Duration::from_millis(1)
        );
    }

    #[test]
    fn authoritative_book_validation_accepts_exact_partial_response() {
        let received_at = Utc::now();
        let tokens = vec!["2".to_string(), "1".to_string()];
        let book = |token: &str| {
            json!({
                "asset_id":token,
                "timestamp":"1788069600000",
                "tick_size":"0.01",
                "bids":[{"price":"0.40","size":"10"}],
                "asks":[{"price":"0.60","size":"11"}]
            })
        };
        let batch = validate_polymarket_book_refresh_response(
            json!([book("2"), book("1")]),
            &tokens,
            received_at,
        )
        .unwrap();
        assert_eq!(batch.frames.len(), 2);
        assert!(batch.missing_token_ids.is_empty());
        assert_eq!(batch.frames[0].raw_payload["asset_id"], "1");
        assert_eq!(batch.frames[1].raw_payload["asset_id"], "2");
        assert_eq!(batch.frames[0].raw_payload["event_type"], "book");
        assert_eq!(batch.frames[0].received_at, received_at);

        let partial =
            validate_polymarket_book_refresh_response(json!([book("1")]), &tokens, received_at)
                .unwrap();
        assert_eq!(partial.frames.len(), 1);
        assert_eq!(partial.missing_token_ids, ["2"]);
        let inexact = json!([{
            "asset_id":"1", "timestamp":"1788069600000", "tick_size":0.01,
            "bids":[{"price":"0.40","size":"10"}],
            "asks":[{"price":"0.60","size":"11"}]
        }, book("2")]);
        assert!(validate_polymarket_book_refresh_response(inexact, &tokens, received_at).is_err());
    }

    #[test]
    fn exact_validation_evidence_refreshes_age_without_mutating_projection_time() {
        let (_dir, state) = test_state();
        let causal_time = Utc::now() - chrono::Duration::minutes(2);
        let mut projection = bootstrap_projection("s".into());
        projection.books.insert(
            "token-1".into(),
            marketcow_core::Book {
                source_observed_at: Some(causal_time),
                ..Default::default()
            },
        );
        assert!(!projection_fresh(&state, &projection));

        state
            .polymarket_book_validations
            .store(Arc::new(PolymarketBookValidationProjection {
                scope_id: "s".into(),
                verified_at: BTreeMap::from([("token-1".into(), Utc::now())]),
            }));

        assert!(projection_fresh(&state, &projection));
        assert_eq!(
            projection.books["token-1"].source_observed_at,
            Some(causal_time)
        );
        assert_eq!(projection.cursor, 0);

        state
            .polymarket_book_validations
            .store(Arc::new(PolymarketBookValidationProjection {
                scope_id: "different-scope".into(),
                verified_at: BTreeMap::from([("token-1".into(), Utc::now())]),
            }));
        assert!(!projection_fresh(&state, &projection));
    }
}
