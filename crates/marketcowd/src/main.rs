use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{Request, State},
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
        Ok(Self {
            profile,
            bind,
            storage_root,
            wal_root,
            worker_socket,
            scope_id,
            real_order_submission_enabled: false,
            shadow_mode,
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
}

#[derive(Default)]
struct Metrics {
    requests: AtomicU64,
    errors: AtomicU64,
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
    let state = AppState {
        config: config.clone(),
        audit,
        metrics: Arc::new(Metrics::default()),
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

fn app(state: AppState) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/readiness", get(readiness))
        .route("/v1/prediction-markets/polymarket/live/scope", get(scope))
        .route("/metrics", get(metrics))
        .route("/v1/admin/migration", get(admin_migration))
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

async fn readiness(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({
        "ready":true, "mode":"shadow", "scope_id":state.config.scope_id,
        "writer_enabled":false, "real_order_submission_enabled":false,
        "ownership_registry":"docs/architecture/migration/domain-ownership.yaml"
    }))
}

async fn scope(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({
        "schema_version":"marketcow.polymarket.scope-discovery.v1",
        "active_scope_id":state.config.scope_id, "scope_status":"shadow",
        "real_order_submission_enabled":false
    }))
}

async fn admin_migration() -> Json<serde_json::Value> {
    Json(json!({
        "phase":"shadow", "cutover_allowed":false, "tradude_may_manage_marketcow":false
    }))
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4")],
        format!(
            "# TYPE marketcow_http_requests_total counter\nmarketcow_http_requests_total {}\n\
             # TYPE marketcow_http_errors_total counter\nmarketcow_http_errors_total {}\n\
             marketcow_real_order_submission_enabled 0\nmarketcow_shadow_mode 1\n",
            state.metrics.requests.load(Ordering::Relaxed),
            state.metrics.errors.load(Ordering::Relaxed),
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
    (
        status,
        Json(
            json!({"detail":{"code":code,"message":code.replace('_'," "),
        "retryable":retryable,"request_id":request_id}}),
        ),
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
        };
        (
            dir,
            AppState {
                config,
                audit,
                metrics: Arc::new(Metrics::default()),
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
    #[test]
    fn equality_does_not_accept_prefixes() {
        assert!(constant_time_equal(b"abc", b"abc"));
        assert!(!constant_time_equal(b"abc", b"ab"));
    }
}
