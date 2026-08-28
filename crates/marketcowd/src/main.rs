use anyhow::{Context, Result, bail};
use arc_swap::ArcSwap;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Extension, Query, Request, State},
    http::{HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::Utc;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    env,
    fs::{self, OpenOptions},
    io::Write,
    net::SocketAddr,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    signal,
    sync::Mutex as AsyncMutex,
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const PROTOCOL_VERSION: &str = "marketcow.worker.v1";

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
        })
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
}

#[derive(Default)]
struct Metrics {
    requests: AtomicU64,
    errors: AtomicU64,
    disconnects: AtomicU64,
    persistence_latency_us: AtomicU64,
    publication_latency_us: AtomicU64,
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
    Ok(())
}

async fn serve() -> Result<()> {
    let config = Config::load()?;
    preflight(&config)?;
    let audit = Arc::new(AuditLog::open(&config.storage_root.join("audit.jsonl"))?);
    let runtime = marketcow_runtime::PolymarketRuntime::open(runtime_config(&config))?;
    let projection = runtime.projection();
    let recent_events = runtime.recent_events().to_vec();
    let state = AppState {
        config: config.clone(),
        audit,
        metrics: Arc::new(Metrics::default()),
        projection: Arc::new(ArcSwap::from(projection)),
        recent_events: Arc::new(ArcSwap::from_pointee(recent_events)),
        runtime: Arc::new(AsyncMutex::new(runtime)),
    };
    let worker_path = config.worker_socket.clone();
    let worker = tokio::spawn(async move { worker_server(worker_path).await });
    let app = app(state);
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    info!(bind=%config.bind, shadow=true, "marketcowd_ready");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await?;
    worker.abort();
    let _ = fs::remove_file(&config.worker_socket);
    info!("marketcowd_stopped");
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
    let passed = elapsed_seconds >= duration_seconds as f64
        && rejected_count == 0
        && projection.ready
        && projection.unresolved_gaps.is_empty()
        && projection.cursor == projection.persisted_cursor
        && maximum_book_age_ms <= interval_millis.saturating_mul(2).max(5_000)
        && percentile(0.99) <= 50_000
        && maximum_persistence_latency_us <= 50_000
        && maximum_publication_latency_us <= 5_000
        && !config.real_order_submission_enabled;
    let result = json!({
        "schema_version":"marketcow.headless-shadow-soak.v1",
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
        .layer(DefaultBodyLimit::max(1_048_576))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            request_boundary,
        ))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({
        "status":"healthy", "service":"marketcowd", "profile":state.config.profile,
        "shadow_mode":true, "real_order_submission_enabled":false,
        "components":{"api":"healthy","wal":"healthy","python_workers":"degraded_optional"}
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

#[cfg(test)]
fn bootstrap_projection(scope_id: String) -> marketcow_core::Projection {
    marketcow_core::Projection::bootstrap(scope_id)
}

async fn admin_migration() -> Json<serde_json::Value> {
    Json(json!({
        "phase":"shadow", "cutover_allowed":false, "tradude_may_manage_marketcow":false
    }))
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
             marketcow_persistence_latency_us {}\nmarketcow_publication_latency_us {}\n",
            state.metrics.requests.load(Ordering::Relaxed),
            state.metrics.errors.load(Ordering::Relaxed),
            projection.cursor,
            projection.persisted_cursor,
            projection.unresolved_gaps.len(),
            projection.books.len(),
            maximum_book_age_ms,
            state.metrics.disconnects.load(Ordering::Relaxed),
            state.metrics.persistence_latency_us.load(Ordering::Relaxed),
            state.metrics.publication_latency_us.load(Ordering::Relaxed),
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

#[derive(Debug, Serialize, Deserialize)]
struct WorkerEnvelope {
    protocol_version: String,
    message_type: String,
    worker_revision: String,
    nonce: String,
}

async fn worker_server(path: PathBuf) -> Result<()> {
    let _ = fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    loop {
        let (mut stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            match read_frame(&mut stream).await {
                Ok(input)
                    if input.protocol_version == PROTOCOL_VERSION
                        && input.message_type == "hello" =>
                {
                    let response = WorkerEnvelope {
                        protocol_version: PROTOCOL_VERSION.into(),
                        message_type: "hello_ack".into(),
                        worker_revision: env!("CARGO_PKG_VERSION").into(),
                        nonce: input.nonce,
                    };
                    let _ = write_frame(&mut stream, &response).await;
                }
                Ok(_) => {
                    let _ = stream.shutdown().await;
                }
                Err(error) => warn!(%error, "worker_handshake_rejected"),
            }
        });
    }
}

async fn read_frame(stream: &mut UnixStream) -> Result<WorkerEnvelope> {
    let length = stream.read_u32().await? as usize;
    if length == 0 || length > 1_048_576 {
        bail!("invalid worker frame length");
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
async fn write_frame(stream: &mut UnixStream, value: &WorkerEnvelope) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
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
        (
            dir,
            AppState {
                config,
                audit,
                metrics: Arc::new(Metrics::default()),
                projection: Arc::new(ArcSwap::from_pointee(bootstrap_projection("s".into()))),
                recent_events: Arc::new(ArcSwap::from_pointee(Vec::new())),
                runtime: Arc::new(AsyncMutex::new(runtime)),
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
