//! Bounded original-trade history. Independent background IO; never gates live WS.
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

const SCHEMA: &str = "marketcow.binance-trade-history.v1";
const PAGE_BYTES: usize = 4 * 1024 * 1024;
const SESSION_BYTES: usize = 1024 * 1024 * 1024;
const MAX_TRADES: usize = 1_024_000;
const MAX_PAGES: usize = 1_024;
const ARCHIVE_SCAN_BYTES: usize = 1024 * 1024 * 1024;
const LINE_BYTES: usize = 2 * 1024 * 1024;
const TTL: Duration = Duration::from_secs(600);
const BUILD_TIME: Duration = Duration::from_secs(120);
const MAX_OPENING_EPOCHS: usize = 64;

#[path = "btc_trade_history_db.rs"]
mod db_pages;

fn opening_history_header(start_ms: i64, boundary_ms: i64) -> Vec<u8> {
    format!("marketcow.binance-opening-history.v1\n{start_ms}|{boundary_ms}\n").into_bytes()
}

fn opening_history_row(id: u64, time_ms: i64, price: rust_decimal::Decimal,
    quantity: rust_decimal::Decimal, maker: bool, raw_sha256: &str, epoch: &str) -> Vec<u8> {
    format!("{id}|{time_ms}|{}|{}|{maker}|{raw_sha256}|{epoch}\n",
        price.normalize(), quantity.normalize()).into_bytes()
}

#[derive(Clone)]
pub struct Service {
    root: PathBuf,
    owner: super::Service,
    slots: Arc<Semaphore>,
    page_slots: Arc<Semaphore>,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    db_sessions: Arc<Mutex<HashMap<String, Arc<db_pages::Session>>>>,
}
struct Session {
    created: Instant,
    start: i64,
    through: u64,
    limit: usize,
    pages: Vec<PathBuf>,
    // Frozen pages are temporary disk-backed data. Keep their directory alive
    // while an in-flight page response is reading it, even after TTL expiry.
    directory: Arc<tempfile::TempDir>,
}

struct FrozenPages {
    pages: Vec<PathBuf>,
    directory: Arc<tempfile::TempDir>,
}

fn remove_stale_sessions(root: &Path) -> Result<()> {
    let parent = root.join("history-sessions");
    let kind = match std::fs::symlink_metadata(&parent) {
        Ok(metadata) => metadata.file_type(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    ensure!(kind.is_dir() && !kind.is_symlink(), "history_session_path_invalid");
    for entry in std::fs::read_dir(&parent)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let kind = entry.file_type()?;
        if name.starts_with("history-session-") && kind.is_dir() && !kind.is_symlink() {
            std::fs::remove_dir_all(entry.path())?;
        }
    }
    Ok(())
}

fn freeze_pages(root: &Path, pages: Vec<Value>) -> Result<FrozenPages> {
    let parent = root.join("history-sessions");
    std::fs::create_dir_all(&parent)?;
    let kind = std::fs::symlink_metadata(&parent)?.file_type();
    ensure!(kind.is_dir() && !kind.is_symlink(), "history_session_path_invalid");
    let directory = Arc::new(tempfile::Builder::new()
        .prefix("history-session-")
        .tempdir_in(&parent)?);
    let mut paths = Vec::with_capacity(pages.len());
    let mut bytes = 0usize;
    for (index, page) in pages.into_iter().enumerate() {
        let encoded = serde_json::to_vec(&page)?;
        ensure!(encoded.len() <= PAGE_BYTES, "history_page_bytes");
        bytes = bytes.checked_add(encoded.len()).context("history_session_bytes")?;
        ensure!(bytes <= SESSION_BYTES, "history_session_bytes");
        let path = directory.path().join(format!("{index:04}.json"));
        std::fs::write(&path, encoded)?;
        paths.push(path);
    }
    Ok(FrozenPages { pages: paths, directory })
}

fn response_bytes(bytes: Vec<u8>) -> Response {
    (
        [(axum::http::header::CACHE_CONTROL, "no-store"),
         (axum::http::header::CONTENT_TYPE, "application/json")],
        bytes,
    ).into_response()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    source: String,
    start_time_ms: i64,
    through_trade_id: u64,
    #[serde(default = "default_limit")]
    limit: usize,
    page_token: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseRequest {
    source: String,
    session_id: String,
    start_time_ms: i64,
    through_trade_id: u64,
    limit: usize,
}
fn default_limit() -> usize {
    1000
}

impl Service {
    pub fn new(root: PathBuf, owner: super::Service) -> Self {
        let root = root.join("binance-trades");
        if let Err(error) = remove_stale_sessions(&root) {
            tracing::warn!(error = %error, "btc_history_stale_session_cleanup_failed");
        }
        Self {
            root,
            owner,
            // A 1 GiB encoded session can have a larger temporary typed
            // reduction footprint. Never build two such sessions together.
            slots: Arc::new(Semaphore::new(1)),
            page_slots: Arc::new(Semaphore::new(4)),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            db_sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub fn router(&self) -> Router {
        Router::new()
            .route("/v1/btc-research/price-history", get(read_db))
            .route("/v1/btc-research/price-history/session", axum::routing::delete(release_db))
            .route(
                "/v1/btc-research/price-history-contract",
                get(|| async {
                    response(
                        serde_json::from_str(include_str!("btc-trade-history-contract-v1.json"))
                            .unwrap(),
                    )
                }),
            )
            .with_state(self.clone())
    }
}

async fn release_db(State(service):State<Service>,
    query:Result<Query<ReleaseRequest>,axum::extract::rejection::QueryRejection>)->Response {
    let Ok(Query(q))=query else {return error(StatusCode::BAD_REQUEST,"invalid_request")};
    if q.source!="binance_spot_trade" || q.start_time_ms<=0 || q.through_trade_id==0
        || !(1..=1000).contains(&q.limit) || q.session_id.len()!=32
        || !q.session_id.bytes().all(|b|b.is_ascii_hexdigit()) {
        return error(StatusCode::BAD_REQUEST,"invalid_range_or_source");
    }
    let mut map=service.db_sessions.lock().unwrap();
    map.retain(|_,session|session.created.elapsed()<TTL);
    if let Some(session)=map.get(&q.session_id) {
        if session.start!=q.start_time_ms || session.through!=q.through_trade_id
            || session.limit!=q.limit {
            return error(StatusCode::CONFLICT,"session_range_mismatch");
        }
    }
    // Any in-flight page owns an Arc and finishes against the same frozen
    // metadata. Releasing the map entry never truncates a response in flight.
    let released=map.remove(&q.session_id).is_some();
    response(json!({"schema_version":"marketcow.binance-trade-history-release.v1",
        "source":"binance_spot_trade","session_id":q.session_id,
        "start_time_ms":q.start_time_ms,"through_trade_id":q.through_trade_id,
        "limit":q.limit,"released":released}))
}

async fn read_db(State(service):State<Service>,
    query:Result<Query<Request>,axum::extract::rejection::QueryRejection>)->Response {
    let Ok(Query(q))=query else {return error(StatusCode::BAD_REQUEST,"invalid_request")};
    if q.source!="binance_spot_trade" || q.start_time_ms<=0 || q.through_trade_id==0
        || !(1..=1000).contains(&q.limit) {
        return error(StatusCode::BAD_REQUEST,"invalid_range_or_source");
    }
    if let Some(token)=q.page_token {
        let Ok(_permit)=service.page_slots.try_acquire() else {
            return error(StatusCode::SERVICE_UNAVAILABLE,"history_capacity")
        };
        let Some((id,index))=token.split_once(':') else {
            return error(StatusCode::BAD_REQUEST,"invalid_page_token")
        };
        let Ok(index)=index.parse::<usize>() else {
            return error(StatusCode::BAD_REQUEST,"invalid_page_token")
        };
        let session={let mut map=service.db_sessions.lock().unwrap();
            map.retain(|_,s|s.created.elapsed()<TTL);
            map.get(id).cloned()};
        let Some(session)=session else {return error(StatusCode::GONE,"session_expired_or_unknown")};
        if session.start!=q.start_time_ms || session.through!=q.through_trade_id
            || session.limit!=q.limit {return error(StatusCode::CONFLICT,"session_range_mismatch")}
        let root=service.root.clone();
        return match tokio::task::spawn_blocking(move||db_pages::page(&root,&session,index)).await {
            Ok(Ok(page))=>response(page),
            Ok(Err(e))=>{let code=build_error_code(&e);(StatusCode::SERVICE_UNAVAILABLE,
                [(axum::http::header::CACHE_CONTROL,"no-store")],
                Json(json!({"schema_version":SCHEMA,"source":"binance_spot_trade","complete":false,
                    "code":code,"failure":build_failure(&code,&e),"execution_readiness":false}))).into_response()},
            Err(_)=>error(StatusCode::SERVICE_UNAVAILABLE,"history_database_worker_failed"),
        };
    }
    let reusable={let mut map=service.db_sessions.lock().unwrap();
        map.retain(|_,s|s.created.elapsed()<TTL);
        map.values().find(|s|s.start==q.start_time_ms && s.through==q.through_trade_id
            && s.limit==q.limit && s.reusable()).cloned()};
    if let Some(session)=reusable {
        let root=service.root.clone();
        return match tokio::task::spawn_blocking(move||db_pages::page(&root,&session,0)).await {
            Ok(Ok(page))=>response(page),
            Ok(Err(e))=>{let code=build_error_code(&e);(StatusCode::SERVICE_UNAVAILABLE,
                [(axum::http::header::CACHE_CONTROL,"no-store")],
                Json(json!({"schema_version":SCHEMA,"source":"binance_spot_trade","complete":false,
                    "code":code,"failure":build_failure(&code,&e),"execution_readiness":false}))).into_response()},
            Err(_)=>error(StatusCode::SERVICE_UNAVAILABLE,"history_database_worker_failed"),
        };
    }
    {let map=service.db_sessions.lock().unwrap();
        if map.len()>=4{return error(StatusCode::SERVICE_UNAVAILABLE,"session_capacity")}}
    let Ok(slot)=service.slots.clone().try_acquire_owned() else {
        return error(StatusCode::SERVICE_UNAVAILABLE,"history_capacity")};
    let seed=service.owner.history_seed().await;
    let archive_health=seed["archive_health"].clone();
    let root=service.root.clone();let start=q.start_time_ms;let through=q.through_trade_id;
    let limit=q.limit;let id=uuid::Uuid::new_v4().simple().to_string();
    let job=tokio::task::spawn_blocking(move||{
        let _permit=slot;
        let session=db_pages::freeze(&root,id,start,through,limit,archive_health)?;
        let first=db_pages::page(&root,&session,0)?;
        Ok::<_,anyhow::Error>((session,first))
    });
    let (session,first)=match tokio::time::timeout(BUILD_TIME,job).await {
        Ok(Ok(Ok(result)))=>result,
        Ok(Ok(Err(e)))=>{let code=build_error_code(&e);return (StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::CACHE_CONTROL,"no-store")],
            Json(json!({"schema_version":SCHEMA,"source":"binance_spot_trade","complete":false,
                "code":code,"failure":build_failure(&code,&e),"execution_readiness":false}))).into_response()},
        _=>return error(StatusCode::SERVICE_UNAVAILABLE,"history_deadline"),
    };
    let mut map=service.db_sessions.lock().unwrap();map.retain(|_,s|s.created.elapsed()<TTL);
    if map.len()>=4{return error(StatusCode::SERVICE_UNAVAILABLE,"session_capacity")}
    map.insert(session.id.clone(),Arc::new(session));
    response(first)
}
fn error(status: StatusCode, code: &str) -> Response {
    (
        status,
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(json!({
        "schema_version":SCHEMA,"source":"binance_spot_trade","complete":false,
        "code":code,"trades":[],"evidence":[],"predecessor":null,
        "next_page_token":null,"execution_readiness":false})),
    )
        .into_response()
}
fn response(value: Value) -> Response {
    (
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Json(value),
    )
        .into_response()
}
fn build_failure(code: &str, error: &anyhow::Error) -> Value {
    // A bounded history build can fail without any archive I/O failure.  In
    // particular, exhausting the trade/byte/deadline budget must not be
    // reported as a broken archive or index to a caller diagnosing coverage.
    let io = error.chain().find_map(|cause| cause.downcast_ref::<std::io::Error>());
    json!({
        "reason": code,
        "failed_at": chrono::Utc::now(),
        "os_error": io.and_then(|cause| cause.raw_os_error()),
        "detail": format!("{error:#}").chars().take(1024).collect::<String>(),
    })
}
fn build_error_code(error: &anyhow::Error) -> String {
    if let Some(io) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<std::io::Error>())
    {
        return match (io.raw_os_error(), io.kind()) {
            (Some(28), _) => "disk_full",
            (Some(13 | 1), _) => "permission_denied",
            (_, std::io::ErrorKind::TimedOut) => "history_io_timeout",
            _ => "history_io_or_index_failed",
        }
        .to_owned();
    }
    const KNOWN: &[&str] = &[
        "history_database_unavailable",
        "history_historical_database_invalid",
        "historical_live_trade_identity_conflict",
        "history_database_query_failed",
        "history_database_changed_during_session",
        "history_database_id_regression",
        "history_gap_capacity",
        "history_database_row_invalid",
        "supplier_time_regression",
        "database_original_row_missing",
        "database_original_row_invalid",
        "database_original_row_conflict",
        "database_raw_hash_invalid",
        "database_trade_identity_conflict",
        "history_archive_unavailable",
        "archive_file_capacity",
        "archive_selected_file_capacity",
        "archive_index_building_or_failed",
        "history_archive_index_unavailable",
        "archive_path_invalid",
        "archive_schema_invalid",
        "archive_raw_hash_invalid",
        "archive_record_invalid",
        "archive_raw_missing",
        "archive_raw_invalid",
        "archive_trade_time_invalid",
        "archive_event_time_invalid",
        "archive_price_invalid",
        "archive_quantity_invalid",
        "archive_maker_invalid",
        "raw_evidence_capacity",
        "archive_line_capacity",
        "archive_scan_capacity",
        "history_deadline",
        "history_session_bytes",
        "history_session_path_invalid",
        "history_trade_capacity",
        "history_page_capacity",
        "history_page_bytes",
        "history_span_capacity",
        "raw_evidence_missing",
        "owner_seed_invalid",
        "owner_raw_hash_invalid",
        "trade_identity_conflict",
    ];
    error
        .chain()
        .map(ToString::to_string)
        .find(|cause| KNOWN.contains(&cause.as_str()))
        .unwrap_or_else(|| "history_parse_or_validation_error".to_owned())
}
async fn read(
    State(service): State<Service>,
    query: Result<Query<Request>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Ok(Query(q)) = query else {
        return error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    if q.source != "binance_spot_trade"
        || q.start_time_ms <= 0
        || q.through_trade_id == 0
        || !(1..=1000).contains(&q.limit)
    {
        return error(StatusCode::BAD_REQUEST, "invalid_range_or_source");
    }
    if let Some(token) = &q.page_token {
        let Ok(_page_slot) = service.page_slots.try_acquire() else {
            return error(StatusCode::SERVICE_UNAVAILABLE, "history_capacity");
        };
        let Some((id, index)) = token.split_once(':') else {
            return error(StatusCode::BAD_REQUEST, "invalid_page_token");
        };
        let Ok(index) = index.parse::<usize>() else {
            return error(StatusCode::BAD_REQUEST, "invalid_page_token");
        };
        let selected = {
            let mut sessions = service.sessions.lock().unwrap();
            sessions.retain(|_, s| s.created.elapsed() < TTL);
            let Some(s) = sessions.get(id) else {
                return error(StatusCode::GONE, "session_expired_or_unknown");
            };
            if s.start != q.start_time_ms || s.through != q.through_trade_id || s.limit != q.limit {
                return error(StatusCode::CONFLICT, "session_range_mismatch");
            }
            let Some(path) = s.pages.get(index) else {
                return error(StatusCode::BAD_REQUEST, "page_out_of_range");
            };
            (path.clone(), s.directory.clone())
        };
        let _directory = selected.1;
        return match tokio::fs::read(&selected.0).await {
            Ok(bytes) => response_bytes(bytes),
            Err(_) => error(StatusCode::SERVICE_UNAVAILABLE, "session_page_read_failed"),
        };
    }
    {
        let mut sessions = service.sessions.lock().unwrap();
        sessions.retain(|_, s| s.created.elapsed() < TTL);
        if sessions.len() >= 4 {
            return error(StatusCode::SERVICE_UNAVAILABLE, "session_capacity");
        }
    }
    let Ok(slot) = service.slots.clone().try_acquire_owned() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "history_capacity");
    };
    // Only a bounded copy of the existing owner; scanning/reduction stays off live executor.
    let seed = service.owner.history_seed().await;
    let archive_health=seed["archive_health"].clone();
    let root = service.root.clone();
    let start = q.start_time_ms;
    let through = q.through_trade_id;
    let limit = q.limit;
    let id = uuid::Uuid::new_v4().simple().to_string();
    let session_id = id.clone();
    let job = tokio::task::spawn_blocking(move || {
        let _slot = slot;
        let pages = build(&root, &seed, start, through, limit, &session_id)?;
        let first = pages.first().context("history_empty_pages")?.clone();
        let frozen = freeze_pages(&root, pages)?;
        Ok::<_, anyhow::Error>((first, frozen))
    });
    let (first, frozen) = match tokio::time::timeout(BUILD_TIME, job).await {
        Ok(Ok(Ok(frozen))) => frozen,
        Ok(Ok(Err(e))) => {
            let code = build_error_code(&e);
            return (StatusCode::SERVICE_UNAVAILABLE,
                [(axum::http::header::CACHE_CONTROL,"no-store")],
                Json(json!({"schema_version":SCHEMA,"source":"binance_spot_trade","complete":false,"code":code,
                "failure":build_failure(&code,&e),"archive_health_at_freeze":archive_health,"trades":[],"evidence":[],"predecessor":null,"next_page_token":null,"execution_readiness":false}))).into_response();
        }
        _ => return (StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::CACHE_CONTROL,"no-store")],
            Json(json!({"schema_version":SCHEMA,"source":"binance_spot_trade","complete":false,
                "code":"history_deadline","failure":{"reason":"history_deadline","failed_at":chrono::Utc::now()},
                "archive_health_at_freeze":archive_health,"trades":[],"evidence":[],"predecessor":null,
                "next_page_token":null,"execution_readiness":false}))).into_response(),
    };
    tracing::info!(session_id=%id,start_time_ms=start,through_trade_id=through,
        pages=frozen.pages.len(),coverage_verified=first["coverage_verified"].as_bool().unwrap_or(false),
        "btc_trade_history_session_materialized");
    let mut sessions = service.sessions.lock().unwrap();
    sessions.retain(|_, s| s.created.elapsed() < TTL);
    if sessions.len() >= 4 {
        return error(StatusCode::SERVICE_UNAVAILABLE, "session_capacity");
    }
    sessions.insert(
        id,
        Session {
            created: Instant::now(),
            start,
            through,
            limit,
            pages: frozen.pages,
            directory: frozen.directory,
        },
    );
    response(first)
}

struct Collected {
    rows: BTreeMap<u64, Value>,
    evidence: HashMap<String, Value>,
    bytes: usize,
}
fn digest(raw: &[u8]) -> String {
    hex::encode(Sha256::digest(raw))
}
fn same_identity(a: &Value, b: &Value) -> bool {
    ["trade_id", "trade_time_ms", "buyer_is_maker"]
        .iter()
        .all(|k| a[*k] == b[*k])
        && ["price", "quantity"].iter().all(|k| {
            a[*k]
                .as_str()
                .and_then(|s| s.parse::<rust_decimal::Decimal>().ok())
                == b[*k]
                    .as_str()
                    .and_then(|s| s.parse::<rust_decimal::Decimal>().ok())
        })
        && (a["event_time_ms"].is_null()
            || b["event_time_ms"].is_null()
            || a["event_time_ms"] == b["event_time_ms"])
}
fn raw_rows(raw: &[u8]) -> Result<Vec<Value>> {
    let v: Value = serde_json::from_slice(raw).context("archive_raw_invalid")?;
    Ok(if let Value::Array(rows) = v {
        rows
    } else {
        vec![v]
    })
}
fn point(row: &Value, index: usize, reference: &str) -> Result<Option<Value>> {
    let ws = row["e"] == "trade" && row["s"] == "BTCUSDT";
    if !ws && !row.is_object() {
        return Ok(None);
    }
    let id = if ws { &row["t"] } else { &row["id"] };
    let Some(id) = id.as_u64() else {
        return Ok(None);
    };
    let t = if ws { &row["T"] } else { &row["time"] };
    let price = if ws { &row["p"] } else { &row["price"] };
    let qty = if ws { &row["q"] } else { &row["qty"] };
    let maker = if ws { &row["m"] } else { &row["isBuyerMaker"] };
    ensure!(
        t.as_i64().is_some_and(|v| v > 0),
        "archive_trade_time_invalid"
    );
    super::validate_decimal(price.as_str().context("archive_price_invalid")?)?;
    super::validate_decimal(qty.as_str().context("archive_quantity_invalid")?)?;
    ensure!(maker.is_boolean(), "archive_maker_invalid");
    if ws {
        ensure!(
            row["E"].as_i64().is_some_and(|v| v > 0),
            "archive_event_time_invalid"
        )
    }
    Ok(Some(
        json!({"trade_id":id,"trade_time_ms":t,"event_time_ms":if ws {row["E"].clone()}else{Value::Null},
        "price":price,"quantity":qty,"buyer_is_maker":maker,
        "evidence_ref":reference,"source_row_index":index}),
    ))
}
fn add_raw(
    c: &mut Collected,
    raw: &[u8],
    epoch: &Value,
    received: &Value,
    start: i64,
    through: u64,
) -> Result<()> {
    ensure!(raw.len() <= 1024 * 1024, "raw_evidence_capacity");
    let sha = digest(raw);
    let rows = raw_rows(raw)?;
    let mut kept = false;
    for (index, row) in rows.iter().enumerate() {
        let Some(p) = point(row, index, &sha)? else {
            continue;
        };
        let id = p["trade_id"].as_u64().unwrap();
        let t = p["trade_time_ms"].as_i64().unwrap();
        if id > through {
            continue;
        }
        if let Some(previous) = c.rows.get(&id) {
            ensure!(same_identity(previous, &p), "trade_identity_conflict");
            continue;
        }
        // Keep exactly the latest predecessor by ID; every interval point retained.
        if t < start {
            let priors = c
                .rows
                .iter()
                .filter(|(_, p)| p["trade_time_ms"].as_i64().unwrap() < start)
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            if priors.last().is_some_and(|prior| *prior >= id) {
                continue;
            }
            for prior in priors {
                c.rows.remove(&prior);
            }
        }
        c.bytes = c
            .bytes
            .checked_add(serde_json::to_vec(&p)?.len())
            .context("history_bytes")?;
        ensure!(c.bytes <= SESSION_BYTES, "history_session_bytes");
        c.rows.insert(id, p);
        kept = true;
        ensure!(c.rows.len() <= MAX_TRADES + 1, "history_trade_capacity");
    }
    if kept && !c.evidence.contains_key(&sha) {
        let ev = json!({"evidence_ref":sha,"raw_sha256":sha,"raw_base64":STANDARD.encode(raw),
            "raw_bytes":raw.len(),"source_epoch":epoch,"received_at":received,
            "acquisition":if rows.first().is_some_and(|r|r["e"]=="trade") {"ws_archive"} else {"official_recent_trades"},
            "upstream_url":if rows.first().is_some_and(|r|r["e"]=="trade") {super::WS_URL} else {super::REST_URL}});
        c.bytes = c
            .bytes
            .checked_add(serde_json::to_vec(&ev)?.len())
            .context("history_bytes")?;
        ensure!(c.bytes <= SESSION_BYTES, "history_session_bytes");
        c.evidence.insert(sha, ev);
    }
    Ok(())
}
fn decode_line(line: &[u8]) -> Result<(Vec<u8>, Value, Value)> {
    let v: Value = serde_json::from_slice(line).context("archive_record_invalid")?;
    ensure!(
        v["schema_version"] == "marketcow.binance-trade-raw.v1",
        "archive_schema_invalid"
    );
    let raw = STANDARD.decode(v["raw_base64"].as_str().context("archive_raw_missing")?)?;
    Ok((raw, v["source_epoch"].clone(), v["received_at"].clone()))
}
fn line(reader: &mut BufReader<File>) -> Result<Vec<u8>> {
    let mut b = Vec::new();
    reader
        .take((LINE_BYTES + 1) as u64)
        .read_until(b'\n', &mut b)?;
    ensure!(b.len() <= LINE_BYTES, "archive_line_capacity");
    Ok(b)
}
fn clock(line: &[u8]) -> Result<Option<i64>> {
    let (raw, _, _) = decode_line(line)?;
    Ok(raw_rows(&raw)?
        .iter()
        .filter_map(|r| r["T"].as_i64().or(r["time"].as_i64()))
        .max())
}
// Archive records append in reception order. Seek close to supplier-time lower
// boundary; the final strict ID/clock coverage checks reject any skipped prefix.
fn seek_start(
    reader: &mut BufReader<File>,
    size: u64,
    start: i64,
    deadline: Instant,
) -> Result<()> {
    let (mut low, mut high) = (0, size);
    while high.saturating_sub(low) > LINE_BYTES as u64 {
        ensure!(Instant::now() < deadline, "history_deadline");
        let mid = low + (high - low) / 2;
        reader.seek(SeekFrom::Start(mid))?;
        let _ = line(reader)?;
        let b = line(reader)?;
        if b.is_empty() || b.last() != Some(&b'\n') {
            high = mid;
            continue;
        }
        match clock(&b)? {
            Some(t) if t < start => low = mid,
            _ => high = mid,
        }
    }
    reader.seek(SeekFrom::Start(low.saturating_sub(LINE_BYTES as u64)))?;
    if low > LINE_BYTES as u64 {
        let _ = line(reader)?;
    }
    Ok(())
}
fn build(
    root: &Path,
    seed: &Value,
    start: i64,
    through: u64,
    limit: usize,
    id: &str,
) -> Result<Vec<Value>> {
    let deadline = Instant::now() + BUILD_TIME;
    let mut c = Collected {
        rows: BTreeMap::new(),
        evidence: HashMap::new(),
        bytes: 0,
    };
    // A normal business request never opens a JSONL segment or bridges an
    // asynchronous DB tail from live memory. Missing rows stay missing.
    let db_path=root.join("trades.sqlite3");
    ensure!(db_path.is_file(),"history_database_unavailable");
    let db=rusqlite::Connection::open_with_flags(&db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).context("history_database_unavailable")?;
    let database_health=super::observation_db::health(&db,"binance_spot_trade")?;
    for entry in super::observation_db::binance_evidence(&db,start,through,MAX_TRADES)
        .context("history_database_query_failed")? {
        ensure!(Instant::now() < deadline,"history_deadline");
        add_raw(&mut c,&entry.raw,&json!(entry.epoch),&json!(entry.received_at),start,through)?;
    }
    let mut pages=materialize(c, start, through, limit, id)?;
    let mut bytes=0;
    for page in &mut pages {
        page["database_health_at_freeze"]=database_health.clone();
        if page["code"]=="through_trade_missing"
            && database_health["last_source_sequence"].as_u64().unwrap_or(0)<through {
            page["code"]="database_write_lag".into();
        }
        page["archive_health_at_freeze"]=seed["archive_health"].clone();
        let size=serde_json::to_vec(page)?.len();ensure!(size<=PAGE_BYTES,"history_page_bytes");bytes+=size;
    }
    ensure!(bytes<=SESSION_BYTES,"history_session_bytes");
    Ok(pages)
}

pub(super) fn recover_opening(root: &Path, seed: &Value, boundary_ms: i64) -> Result<super::OpeningCandidate> {
    let start_ms = boundary_ms.checked_sub(60_000).context("history_opening_boundary_invalid")?;
    let retained = seed["trades"].as_array().context("history_owner_seed_invalid")?;
    let successor = retained.iter().find(|row| {
        row["trade_time_ms"].as_i64().is_some_and(|time| time >= boundary_ms)
    }).context("history_boundary_successor_missing")?;
    let through = successor["trade_id"].as_u64().context("history_successor_id_missing")?;
    let frozen=db_pages::freeze(root,"opening-recovery".to_owned(),start_ms,through,1000,
        seed["archive_health"].clone())?;
    let final_page=db_pages::page(root,&frozen,frozen.page_count()-1)?;
    ensure!(final_page["complete"] == true && final_page["coverage_verified"] == true,
        "history_coverage_gap: {}", final_page["code"]);
    let first_page=db_pages::page(root,&frozen,0)?;
    let predecessor = first_page["predecessor"].as_object()
        .context("history_boundary_predecessor_missing")?;
    let mut twap = super::TradeTwap::default();
    let mut digest = Sha256::new();
    digest.update(opening_history_header(start_ms, boundary_ms));
    let mut epochs = Vec::<String>::new();
    let mut boundary_seen = false;
    for page_index in 0..frozen.page_count() {
        let page=if page_index==0 {first_page.clone()}else{db_pages::page(root,&frozen,page_index)?};
        let evidence = page["evidence"].as_array().context("history_evidence_missing")?;
        let rows = page["trades"].as_array().context("history_trades_missing")?;
        for row in (if page_index == 0 {Some(&first_page["predecessor"])} else {None})
            .into_iter().chain(rows.iter())
        {
            let id = row["trade_id"].as_u64().context("history_trade_id_missing")?;
            let time = row["trade_time_ms"].as_i64().context("history_trade_time_missing")?;
            let price = rust_decimal::Decimal::from_str(
                row["price"].as_str().context("history_trade_price_missing")?)?;
            let quantity = rust_decimal::Decimal::from_str(
                row["quantity"].as_str().context("history_trade_quantity_missing")?)?;
            let maker = row["buyer_is_maker"].as_bool().context("history_trade_maker_missing")?;
            let reference = row["evidence_ref"].as_str().context("history_evidence_ref_missing")?;
            let source = evidence.iter().find(|entry| entry["evidence_ref"] == reference)
                .context("history_evidence_ref_unbound")?;
            let epoch = source["source_epoch"].as_str().context("history_source_epoch_missing")?;
            digest.update(opening_history_row(id,time,price,quantity,maker,reference,epoch));
            if time >= boundary_ms {
                let carried = twap.last_price.context("history_boundary_price_missing")?;
                twap.observe(boundary_ms, carried)?;
                boundary_seen = true;
                break;
            }
            if !epochs.iter().any(|seen| seen == epoch) {
                ensure!(epochs.len() < MAX_OPENING_EPOCHS, "history_opening_epoch_capacity");
                epochs.push(epoch.to_owned());
            }
            twap.observe(time, price)?;
        }
        if boundary_seen {break;}
    }
    ensure!(boundary_seen, "history_boundary_successor_missing");
    ensure!(predecessor.get("trade_time_ms").and_then(Value::as_i64).is_some_and(|time| time < start_ms),
        "history_boundary_predecessor_invalid");
    let price = twap.completed_at(boundary_ms).context("history_opening_coverage_incomplete")?;
    let seconds_sha256 = twap.seconds_sha256_at(boundary_ms)
        .context("history_opening_seconds_missing")?;
    let source_epoch = if epochs.len() == 1 {Some(epochs[0].clone())} else {None};
    Ok(super::OpeningCandidate {
        start_ms: boundary_ms,
        price,
        source_epoch,
        source_epochs: epochs,
        seconds_sha256,
        history_evidence_sha256: Some(hex::encode(digest.finalize())),
        evidence_kind: "verified_original_trade_history_price_seconds",
    })
}
fn materialize(
    c: Collected,
    start: i64,
    through: u64,
    limit: usize,
    id: &str,
) -> Result<Vec<Value>> {
    let predecessor = c
        .rows
        .values()
        .rfind(|p| p["trade_time_ms"].as_i64().unwrap() < start)
        .cloned();
    let rows = c
        .rows
        .values()
        .filter(|p| p["trade_time_ms"].as_i64().unwrap() >= start)
        .cloned()
        .collect::<Vec<_>>();
    let mut reason = None;
    if rows.is_empty() {
        reason = Some("range_has_no_trades");
    }
    if predecessor.is_none() {
        reason = Some("boundary_predecessor_missing")
    }
    if c.rows.last_key_value().map(|(i, _)| *i) != Some(through) {
        reason = Some("through_trade_missing")
    }
    let mut previous = None;
    let mut missing = Vec::new();
    let mut previous_t = None;
    for (&i, p) in &c.rows {
        let t = p["trade_time_ms"].as_i64().unwrap();
        if previous_t.is_some_and(|before| t < before) {
            reason = Some("supplier_time_regression")
        }
        if let Some(before) = previous {
            if i != before + 1 {
                missing.push(json!({"from_trade_id":before+1,"through_trade_id":i-1}));
                reason = Some("trade_id_gap");
            }
        }
        previous = Some(i);
        previous_t = Some(t);
    }
    if let Some(last) = rows.last() {
        ensure!(
            last["trade_time_ms"].as_i64().unwrap() - start <= 960000,
            "history_span_capacity"
        )
    }
    let total = rows.len().max(1).div_ceil(limit);
    ensure!(total <= MAX_PAGES, "history_page_capacity");
    let first_t = rows
        .first()
        .map(|p| p["trade_time_ms"].clone())
        .unwrap_or(Value::Null);
    let last_t = rows
        .last()
        .map(|p| p["trade_time_ms"].clone())
        .unwrap_or(Value::Null);
    let mut pages = Vec::new();
    let mut bytes = 0;
    for index in 0..total {
        let part = &rows[(index * limit).min(rows.len())..((index + 1) * limit).min(rows.len())];
        let pred = if index == 0 {
            predecessor.clone()
        } else {
            None
        };
        let mut refs = part
            .iter()
            .chain(pred.iter())
            .map(|p| p["evidence_ref"].as_str().unwrap())
            .collect::<Vec<_>>();
        refs.sort();
        refs.dedup();
        let evidence = refs
            .into_iter()
            .map(|r| c.evidence.get(r).cloned().context("raw_evidence_missing"))
            .collect::<Result<Vec<_>>>()?;
        let last = index + 1 == total;
        let page = json!({"schema_version":SCHEMA,"source":"binance_spot_trade","instrument_id":"BTCUSDT.BINANCE",
            "session_id":id,"start_time_ms":start,"through_trade_id":through,"limit":limit,
            "page_index":index,"page_count":total,"session_ttl_seconds":TTL.as_secs(),
            "predecessor":pred,"trades":part,"evidence":evidence,"next_page_token":if last{None}else{Some(format!("{id}:{}",index+1))},
            "complete":last&&reason.is_none(),"coverage_verified":reason.is_none(),"code":reason,
            "missing_ranges":missing,"coverage_first_T":first_t,"coverage_last_T":last_t,
            "total_trade_count":rows.len(),"time_unit":"integer_unix_milliseconds","fallback_used":false,
            "execution_readiness":false,"budgets":{"page_bytes":PAGE_BYTES,"session_bytes":SESSION_BYTES,"max_trades":MAX_TRADES,"max_pages":MAX_PAGES,"maximum_span_ms":960000}});
        let n = serde_json::to_vec(&page)?.len();
        ensure!(n <= PAGE_BYTES, "history_page_bytes");
        bytes += n;
        ensure!(bytes <= SESSION_BYTES, "history_session_bytes");
        pages.push(page);
    }
    Ok(pages)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn import_fixture(root:&Path) {
        let mut db=super::super::observation_db::open(&root.join("trades.sqlite3")).unwrap();
        let file=std::fs::File::open(root.join("raw-fixture.jsonl")).unwrap();
        for line in BufReader::new(file).lines() {
            let v:Value=serde_json::from_str(&line.unwrap()).unwrap();
            let raw=STANDARD.decode(v["raw_base64"].as_str().unwrap()).unwrap();
            super::super::observation_db::ingest_binance(&mut db,&raw,
                v["source_epoch"].as_str().unwrap_or("test-epoch"),"fixture-received").unwrap();
        }
    }

    #[test]
    fn history_failure_reason_matches_capacity_timeout_and_real_io() {
        let capacity = anyhow::anyhow!("history_trade_capacity");
        let code = build_error_code(&capacity);
        assert_eq!(code, "history_trade_capacity");
        assert_eq!(build_failure(&code, &capacity)["reason"], code);

        let deadline = anyhow::anyhow!("history_deadline");
        let code = build_error_code(&deadline);
        assert_eq!(code, "history_deadline");
        assert_eq!(build_failure(&code, &deadline)["reason"], code);

        let disk = anyhow::Error::new(std::io::Error::from_raw_os_error(28))
            .context("history_archive_index_unavailable");
        let code = build_error_code(&disk);
        assert_eq!(code, "disk_full");
        assert_eq!(build_failure(&code, &disk)["reason"], code);

        let io_timeout = anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "bounded read timed out",
        ));
        let code = build_error_code(&io_timeout);
        assert_eq!(code, "history_io_timeout");
        assert_eq!(build_failure(&code, &io_timeout)["reason"], code);
    }

    #[test]
    fn opening_recovery_integrates_verified_originals_across_real_epochs() {
        let root = tempfile::tempdir().unwrap();
        let rows = [
            (1, 239_000, "100", "epoch-old"),
            (2, 250_000, "100", "epoch-old"),
            (3, 270_000, "200", "epoch-old"),
            (4, 280_000, "200", "epoch-new"),
            (5, 300_100, "300", "epoch-successor"),
        ];
        let original = |id: u64, time: i64, price: &str| {
            serde_json::to_vec(&json!({"e":"trade","s":"BTCUSDT","t":id,
                "T":time,"E":time+1,"p":price,"q":"0.01","m":false})).unwrap()
        };
        let archive = rows.iter().map(|(id, time, price, epoch)| {
            let raw = original(*id, *time, price);
            let mut line = serde_json::to_vec(&json!({
                "schema_version":"marketcow.binance-trade-raw.v1",
                "source_epoch":epoch,"raw_sha256":digest(&raw),
                "raw_base64":STANDARD.encode(raw),
            })).unwrap();
            line.push(b'\n');
            line
        }).flatten().collect::<Vec<_>>();
        std::fs::write(root.path().join("raw-fixture.jsonl"), archive).unwrap();
        import_fixture(root.path());
        let live = rows[4..].iter().map(|(id, time, price, _)| {
            let raw = original(*id, *time, price);
            json!({"trade_id":id,"trade_time_ms":time,
                "source_raw_sha256":digest(&raw),"raw_frame_base64":STANDARD.encode(raw),
                "received_at":"diagnostic-only"})
        }).collect::<Vec<_>>();
        let seed = json!({"source_epoch":"epoch-successor","trades":live});
        let recovered = recover_opening(root.path(), &seed, 300_000).unwrap();
        assert_eq!(recovered.price, rust_decimal::Decimal::from(150));
        assert_eq!(recovered.source_epoch, None);
        assert_eq!(recovered.source_epochs, ["epoch-old", "epoch-new"]);
        assert!(recovered.history_evidence_sha256.is_some());
        assert_eq!(recovered.evidence_kind, "verified_original_trade_history_price_seconds");
    }

    #[test]
    fn opening_recovery_refuses_missing_predecessor() {
        let root = tempfile::tempdir().unwrap();
        let first_raw = raw(2, 250_000);
        let mut line = serde_json::to_vec(&json!({
            "schema_version":"marketcow.binance-trade-raw.v1",
            "source_epoch":"one","raw_sha256":digest(&first_raw),
            "raw_base64":STANDARD.encode(first_raw),
        })).unwrap();
        line.push(b'\n');
        std::fs::write(root.path().join("raw-fixture.jsonl"), line).unwrap();
        import_fixture(root.path());
        let successor = raw(3, 300_100);
        let mut db=super::super::observation_db::open(&root.path().join("trades.sqlite3")).unwrap();
        super::super::observation_db::ingest_binance(&mut db,&successor,"one","receipt").unwrap();
        let seed = json!({"source_epoch":"one","trades":[{
            "trade_id":3,"trade_time_ms":300_100,
            "source_raw_sha256":digest(&successor),
            "raw_frame_base64":STANDARD.encode(successor),
            "received_at":"diagnostic-only"}]});
        let error = recover_opening(root.path(), &seed, 300_000).unwrap_err();
        assert!(format!("{error:#}").contains("boundary_predecessor_missing"));
    }

    #[test]
    fn opening_recovery_refuses_missing_trade_id_inside_covered_minute() {
        let root = tempfile::tempdir().unwrap();
        let mut archive = Vec::new();
        for (id, time) in [(1, 239_000), (2, 250_000), (4, 280_000)] {
            let raw = raw(id, time);
            archive.extend(serde_json::to_vec(&json!({
                "schema_version":"marketcow.binance-trade-raw.v1",
                "source_epoch":"old","raw_sha256":digest(&raw),
                "raw_base64":STANDARD.encode(raw),
            })).unwrap());
            archive.push(b'\n');
        }
        std::fs::write(root.path().join("raw-fixture.jsonl"), archive).unwrap();
        import_fixture(root.path());
        let successor = raw(5, 300_100);
        let mut db=super::super::observation_db::open(&root.path().join("trades.sqlite3")).unwrap();
        super::super::observation_db::ingest_binance(&mut db,&successor,"new","receipt").unwrap();
        let seed = json!({"source_epoch":"new","trades":[{
            "trade_id":5,"trade_time_ms":300_100,
            "source_raw_sha256":digest(&successor),
            "raw_frame_base64":STANDARD.encode(successor),
        }]});
        let error = recover_opening(root.path(), &seed, 300_000).unwrap_err();
        assert!(format!("{error:#}").contains("trade_id_gap"));
    }

    #[tokio::test]
    async fn opening_v3_serializer_vector() {
        let root = tempfile::tempdir().unwrap();
        let observations = [
            (1u64, 239_000i64, "100.00", "11111111111111111111111111111111"),
            (2, 270_000, "200.00", "22222222222222222222222222222222"),
            (3, 300_100, "300.00", "33333333333333333333333333333333"),
        ];
        let mut archive = Vec::new();
        let mut source_frames = Vec::new();
        let mut history_preimage = opening_history_header(240_000, 300_000);
        let mut successor = Vec::new();
        for (id, time, price, epoch) in observations {
            let raw = serde_json::to_vec(&json!({"e":"trade","s":"BTCUSDT",
                "t":id,"T":time,"E":time+1,"p":price,"q":"0.0100","m":false})).unwrap();
            let raw_sha256 = digest(&raw);
            history_preimage.extend(opening_history_row(id,time,
                rust_decimal::Decimal::from_str(price).unwrap(),
                rust_decimal::Decimal::from_str("0.0100").unwrap(),
                false,&raw_sha256,epoch));
            source_frames.push(json!({"source_epoch":epoch,
                "raw_base64":STANDARD.encode(&raw),"raw_sha256":raw_sha256}));
            archive.extend(serde_json::to_vec(&json!({
                "schema_version":"marketcow.binance-trade-raw.v1",
                "source_epoch":epoch,"raw_sha256":raw_sha256,
                "raw_base64":STANDARD.encode(&raw),
            })).unwrap());
            archive.push(b'\n');
            if id == 3 { successor = raw; }
        }
        std::fs::write(root.path().join("raw-fixture.jsonl"), archive).unwrap();
        import_fixture(root.path());
        let seed = json!({"source_epoch":"33333333333333333333333333333333","trades":[{
            "trade_id":3,"trade_time_ms":300_100,
            "source_raw_sha256":digest(&successor),
            "raw_frame_base64":STANDARD.encode(&successor),
        }]});
        let candidate = recover_opening(root.path(), &seed, 300_000).unwrap();
        let identity = super::super::WindowIdentity {
            market_id:"600001".into(),
            condition_id:format!("0x{}", "aa".repeat(32)),
            window_id:"btc-updown-5m-300".into(),
            rule_sha256:"bb".repeat(32),
            start_ms:300_000,end_ms:600_000,
        };
        let binding_preimage = super::super::opening_binding_preimage(&identity,&candidate);
        let mut state = super::super::State::new();
        state.phase = "live";
        state.epoch = "44444444444444444444444444444444".into();
        state.opening_candidates.push_back(Arc::new(candidate.clone()));
        let service = super::super::tests::test_service(state);
        let fixed_time = chrono::DateTime::parse_from_rfc3339("2026-10-13T00:00:00Z")
            .unwrap().with_timezone(&chrono::Utc);
        let comparison = service.window_comparison_at(&identity, None, fixed_time).await;
        let comparison_bytes = serde_json::to_vec(&comparison).unwrap();
        let vector = json!({
            "schema_version":"marketcow.binance-opening-v3-vector.v1",
            "source_frames":source_frames,
            "history_preimage_base64":STANDARD.encode(&history_preimage),
            "history_sha256":digest(&history_preimage),
            "opening_price_usdt":candidate.price.normalize().to_string(),
            "opening_seconds_sha256":candidate.seconds_sha256,
            "opening_source_epoch":candidate.source_epoch,
            "opening_source_epochs":candidate.source_epochs,
            "opening_evidence_kind":candidate.evidence_kind,
            "binding_identity":{"market_id":identity.market_id,
                "condition_id":identity.condition_id,"window_id":identity.window_id,
                "rule_sha256":identity.rule_sha256,"start_ms":identity.start_ms,"end_ms":identity.end_ms},
            "binding_preimage_base64":STANDARD.encode(&binding_preimage),
            "binding_sha256":digest(&binding_preimage),
            "nested_comparison_raw_base64":STANDARD.encode(&comparison_bytes),
            "nested_comparison_raw_sha256":digest(&comparison_bytes),
        });
        assert_eq!(vector["history_sha256"],candidate.history_evidence_sha256.unwrap());
        let fixture: Value = serde_json::from_str(include_str!("btc-binance-opening-v3-vector.json")).unwrap();
        assert_eq!(vector, fixture);
    }
    fn raw(id: u64, t: i64) -> Vec<u8> {
        serde_json::to_vec(&json!({"e":"trade","s":"BTCUSDT","t":id,"T":t,"E":t+1,
            "p":"100.00","q":"0.01000000","m":false}))
        .unwrap()
    }
    fn collection(ids: &[(u64, i64)]) -> Collected {
        let mut c = Collected {
            rows: BTreeMap::new(),
            evidence: HashMap::new(),
            bytes: 0,
        };
        for &(id, t) in ids {
            add_raw(
                &mut c,
                &raw(id, t),
                &json!("source-a"),
                &json!("diagnostic-only"),
                1000,
                3,
            )
            .unwrap();
        }
        c
    }
    #[test]
    fn pages_only_last_complete_and_all_raw_reconstructible() {
        let pages = materialize(
            collection(&[(1, 900), (2, 1000), (3, 1100)]),
            1000,
            3,
            1,
            "fixed-session",
        )
        .unwrap();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0]["complete"], false);
        assert_eq!(pages[1]["complete"], true);
        assert_eq!(pages[0]["predecessor"]["trade_id"], 1);
        assert!(pages[1]["predecessor"].is_null());
        for page in &pages {
            for row in page["trades"].as_array().unwrap().iter().chain(
                page["predecessor"]
                    .as_object()
                    .map(|_| &page["predecessor"]),
            ) {
                let ev = page["evidence"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|ev| ev["evidence_ref"] == row["evidence_ref"])
                    .unwrap();
                let b = STANDARD.decode(ev["raw_base64"].as_str().unwrap()).unwrap();
                assert_eq!(ev["raw_sha256"], digest(&b));
                assert_eq!(
                    *row,
                    point(
                        &serde_json::from_slice::<Value>(&b).unwrap(),
                        0,
                        ev["evidence_ref"].as_str().unwrap()
                    )
                    .unwrap()
                    .unwrap()
                );
            }
        }
        if let Ok(path) = std::env::var("MARKETCOW_HISTORY_FIXTURE") {
            std::fs::write(path,serde_json::to_vec_pretty(&json!({"fixture_kind":"synthetic_formal_serializer",
                "schema_version":SCHEMA,"request":{"source":"binance_spot_trade","start_time_ms":1000,"through_trade_id":3,"limit":1},"pages":pages})).unwrap()).unwrap();
        }
    }
    #[test]
    fn missing_ids_and_missing_tail_never_claim_complete() {
        let gap = materialize(collection(&[(1, 900), (3, 1100)]), 1000, 3, 1000, "gap").unwrap();
        assert_eq!(gap[0]["complete"], false);
        assert_eq!(gap[0]["code"], "trade_id_gap");
        let tail = materialize(collection(&[(1, 900), (2, 1000)]), 1000, 3, 1000, "tail").unwrap();
        assert_eq!(tail[0]["complete"], false);
        assert_eq!(tail[0]["code"], "through_trade_missing");
    }
    #[test]
    fn predecessor_required_and_never_part_of_window_rows() {
        let p = materialize(
            collection(&[(2, 1000), (3, 1100)]),
            1000,
            3,
            1000,
            "no-prior",
        )
        .unwrap();
        assert_eq!(p[0]["complete"], false);
        assert_eq!(p[0]["code"], "boundary_predecessor_missing");
        let p = materialize(
            collection(&[(0, 800), (1, 900), (2, 1000), (3, 1100)]),
            1000,
            3,
            1000,
            "prior",
        )
        .unwrap();
        assert_eq!(p[0]["predecessor"]["trade_id"], 1);
        assert_eq!(p[0]["trades"].as_array().unwrap().len(), 2);
    }
    #[test]
    fn true_identity_overlap_is_deduplicated_conflict_rejected() {
        let mut c = collection(&[(1, 900), (2, 1000)]);
        add_raw(
            &mut c,
            &raw(2, 1000),
            &json!("other-epoch"),
            &Value::Null,
            1000,
            3,
        )
        .unwrap();
        assert_eq!(c.rows.len(), 2);
        let mut lexical: Value = serde_json::from_slice(&raw(2, 1000)).unwrap();
        lexical["p"] = json!("100.0");
        lexical["q"] = json!("0.01");
        add_raw(
            &mut c,
            &serde_json::to_vec(&lexical).unwrap(),
            &Value::Null,
            &Value::Null,
            1000,
            3,
        )
        .unwrap();
        assert_eq!(c.rows.len(), 2);
        let mut changed: Value = serde_json::from_slice(&raw(2, 1000)).unwrap();
        changed["q"] = json!("2");
        assert!(
            add_raw(
                &mut c,
                &serde_json::to_vec(&changed).unwrap(),
                &Value::Null,
                &Value::Null,
                1000,
                3
            )
            .is_err()
        );
    }
    #[test]
    fn archive_source_time_is_not_replaced_by_receipt() {
        let b = raw(1, 900);
        let mut v = json!({"schema_version":"marketcow.binance-trade-raw.v1",
            "raw_base64":STANDARD.encode(&b),"raw_sha256":digest(&b),"source_epoch":"old","received_at":"2099-01-01"});
        assert_eq!(clock(&serde_json::to_vec(&v).unwrap()).unwrap(), Some(900));
        v["raw_sha256"] = json!("bad");
        assert!(decode_line(&serde_json::to_vec(&v).unwrap()).is_ok());
        assert_eq!(clock(&serde_json::to_vec(&v).unwrap()).unwrap(), Some(900));
    }
    #[test]
    fn owner_original_raw_does_not_substitute_for_database_history() {
        let dir=tempfile::tempdir().unwrap();
        let root=dir.path();
        let lines = [(1, 900), (2, 1000)]
            .into_iter()
            .map(|(id, t)| {
                let b = raw(id, t);
                let mut line =
                    serde_json::to_vec(&json!({"schema_version":"marketcow.binance-trade-raw.v1",
                "source_epoch":"old","raw_sha256":digest(&b),"raw_base64":STANDARD.encode(b)}))
                    .unwrap();
                line.push(b'\n');
                line
            })
            .flatten()
            .collect::<Vec<_>>();
        std::fs::write(root.join("raw-test.jsonl"), lines).unwrap();
        let b = raw(3, 1100);
        let seed = json!({"source_epoch":"new","trades":[{"raw_frame_base64":STANDARD.encode(&b),"source_raw_sha256":digest(&b)}]});
        let error=build(root, &seed, 1000, 3, 1000, "no-fallback").unwrap_err();
        assert!(format!("{error:#}").contains("history_database_unavailable"));
    }
    #[test]
    fn suppliers_time_regression_and_page_capacity_fail_closed() {
        let p = materialize(
            collection(&[(1, 900), (2, 1100), (3, 1000)]),
            1000,
            3,
            1000,
            "bad-time",
        )
        .unwrap();
        assert_eq!(p[0]["complete"], false);
        assert_eq!(p[0]["code"], "supplier_time_regression");
        let mut c = collection(&[(1, 900), (2, 1000), (3, 1100)]);
        let reference = c.rows[&2]["evidence_ref"].as_str().unwrap().to_owned();
        c.evidence.get_mut(&reference).unwrap()["raw_base64"] = json!("A".repeat(PAGE_BYTES));
        assert!(materialize(c, 1000, 3, 1000, "too-large").is_err());
    }
    #[tokio::test]
    async fn real_route_frozen_pages_retries_range_and_expiry() {
        use tower::ServiceExt;
        let root =
            std::env::temp_dir().join(format!("marketcow-history-http-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(root.join("binance-trades")).unwrap();
        let mut db=super::super::observation_db::open(&root.join("binance-trades/trades.sqlite3")).unwrap();
        let mut state = super::super::State::new();
        for (id, t) in [(1, 900), (2, 1000), (3, 1100)] {
            let b = raw(id, t);
            super::super::observation_db::ingest_binance(&mut db,&b,"epoch-a","receipt").unwrap();
            let row = serde_json::from_slice(&b).unwrap();
            state
                .trades
                .push_back(super::super::ws_point(&row, &b, "local-diagnostic").unwrap());
        }
        let owner = super::super::tests::test_service(state);
        let service = Service::new(root.clone(), owner);
        let app = service.router();
        let prefix = "/v1/btc-research/price-history?source=binance_spot_trade&start_time_ms=1000&through_trade_id=3&limit=1";
        let call = |url: String| {
            axum::http::Request::builder()
                .uri(url)
                .body(axum::body::Body::empty())
                .unwrap()
        };
        let first = app.clone().oneshot(call(prefix.into())).await.unwrap();
        assert_eq!(first.status(), 200);
        let first: Value = serde_json::from_slice(
            &axum::body::to_bytes(first.into_body(), PAGE_BYTES)
                .await
                .unwrap(),
        )
        .unwrap();
        let url = format!(
            "{prefix}&page_token={}",
            first["next_page_token"].as_str().unwrap()
        );
        let bad = app
            .clone()
            .oneshot(call(
                url.replace("through_trade_id=3", "through_trade_id=4"),
            ))
            .await
            .unwrap();
        assert_eq!(bad.status(), 409);
        let final_page = app.clone().oneshot(call(url.clone())).await.unwrap();
        assert_eq!(final_page.status(), 200);
        let bytes = axum::body::to_bytes(final_page.into_body(), PAGE_BYTES)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap()["complete"],
            true
        );
        let retry = app.clone().oneshot(call(url.clone())).await.unwrap();
        assert_eq!(
            axum::body::to_bytes(retry.into_body(), PAGE_BYTES)
                .await
                .unwrap(),
            bytes
        );
        let repeated=app.clone().oneshot(call(prefix.into())).await.unwrap();
        let repeated:Value=serde_json::from_slice(&axum::body::to_bytes(
            repeated.into_body(),PAGE_BYTES).await.unwrap()).unwrap();
        assert_eq!(repeated["session_id"],first["session_id"]);
        assert_eq!(service.db_sessions.lock().unwrap().len(),1);
        let release_url=format!("/v1/btc-research/price-history/session?source=binance_spot_trade&session_id={}&start_time_ms=1000&through_trade_id=3&limit=1",
            first["session_id"].as_str().unwrap());
        let release=|url:String| axum::http::Request::builder().method("DELETE")
            .uri(url).body(axum::body::Body::empty()).unwrap();
        assert_eq!(app.clone().oneshot(release(release_url.replace(
            "through_trade_id=3","through_trade_id=4"))).await.unwrap().status(),409);
        let freed=app.clone().oneshot(release(release_url.clone())).await.unwrap();
        assert_eq!(freed.status(),200);
        let freed:Value=serde_json::from_slice(&axum::body::to_bytes(
            freed.into_body(),PAGE_BYTES).await.unwrap()).unwrap();
        assert_eq!(freed["released"],true);
        let repeated_release=app.clone().oneshot(release(release_url)).await.unwrap();
        let repeated_release:Value=serde_json::from_slice(&axum::body::to_bytes(
            repeated_release.into_body(),PAGE_BYTES).await.unwrap()).unwrap();
        assert_eq!(repeated_release["released"],false);
        assert_eq!(app.clone().oneshot(call(url.clone())).await.unwrap().status(),410);
        let fresh=app.clone().oneshot(call(prefix.into())).await.unwrap();
        let fresh:Value=serde_json::from_slice(&axum::body::to_bytes(
            fresh.into_body(),PAGE_BYTES).await.unwrap()).unwrap();
        assert_ne!(fresh["session_id"],first["session_id"]);
        for limit in 2..=4 {
            assert_eq!(app.clone().oneshot(call(prefix.replace("limit=1",
                &format!("limit={limit}")))).await.unwrap().status(),200);
        }
        assert_eq!(service.db_sessions.lock().unwrap().len(),4);
        assert_eq!(app.clone().oneshot(call(prefix.replace("limit=1","limit=5")))
            .await.unwrap().status(),503);
        let release_fresh=format!("/v1/btc-research/price-history/session?source=binance_spot_trade&session_id={}&start_time_ms=1000&through_trade_id=3&limit=1",
            fresh["session_id"].as_str().unwrap());
        assert_eq!(app.clone().oneshot(release(release_fresh)).await.unwrap().status(),200);
        assert_eq!(app.clone().oneshot(call(prefix.replace("limit=1","limit=5")))
            .await.unwrap().status(),200);
        service.db_sessions.lock().unwrap().clear();
        assert_eq!(app.oneshot(call(url)).await.unwrap().status(), 410);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}
