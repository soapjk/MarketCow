//! Explicit typed transport boundary. Router construction adds no upstream
//! connections and accepts only already-owned sources plus the indexed DB.
use super::{btc_typed::Published, btc_typed_history};
use anyhow::{Result, ensure};
use axum::{
    Router,
    body::Bytes,
    extract::{
        Path, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use marketcow_contracts::typed_v3::{self as contract, common, history};
use prost::Message as ProtoMessage;
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct Service {
    pub binance: crate::btc_trade::Service,
    pub chainlink_twap: Option<crate::btc_chainlink::Service>,
    pub history: Arc<btc_typed_history::Service>,
    pub database: PathBuf,
    pub clients: Arc<Semaphore>,
    pub requests: Arc<Semaphore>,
}
enum PriceSource {
    Binance(crate::btc_trade::Service),
    Chainlink(crate::btc_chainlink::Service),
}
impl PriceSource {
    async fn subscribe(
        &self,
    ) -> Result<(
        Arc<Published>,
        tokio::sync::broadcast::Receiver<Arc<Published>>,
    )> {
        match self {
            Self::Binance(owner) => owner.typed_snapshot_and_subscribe().await,
            Self::Chainlink(owner) => owner.typed_snapshot_and_subscribe().await,
        }
    }
}
pub(crate) fn failure(status: StatusCode, code: &str, retryable: bool) -> Response {
    let error = common::Error {
        code: code.into(),
        retryable,
        retry_after_ms: None,
        detail: None,
    };
    let mut response = encoded(&error, 64 * 1024);
    *response.status_mut() = status;
    response
}
pub(crate) fn encoded<T: ProtoMessage>(value: &T, limit: usize) -> Response {
    match contract::encode(value, limit) {
        Ok(bytes) => (
            [
                ("content-type", contract::CONTENT_TYPE),
                ("cache-control", "no-store"),
            ],
            bytes,
        )
            .into_response(),
        Err(_) => (
            StatusCode::PAYLOAD_TOO_LARGE,
            [
                ("content-type", contract::CONTENT_TYPE),
                ("cache-control", "no-store"),
            ],
            common::Error {
                code: "message_capacity".into(),
                retryable: false,
                retry_after_ms: None,
                detail: None,
            }
            .encode_to_vec(),
        )
            .into_response(),
    }
}
fn decode<T: ProtoMessage + Default>(headers: &HeaderMap, body: &[u8]) -> Result<T> {
    ensure!(
        headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|s| s.split(';').next() == Some(contract::CONTENT_TYPE)),
        "unsupported_content_type"
    );
    ensure!(body.len() <= 64 * 1024, "request_capacity");
    Ok(T::decode(body)?)
}
impl Service {
    /// These are new explicit routes for the coordinated migration. Mounting
    /// remains the collector's release decision; this module never falls back.
    pub fn router(self) -> Router {
        Router::new()
            .route("/v3/btc/capabilities", get(capabilities))
            .route("/v3/btc/health", get(health))
            .route("/v3/btc/price-stream", get(price_stream))
            .route("/v3/btc/trade-history", post(read_history))
            .route("/v3/btc/trade-history/resolve",post(resolve_range))
            .route(
                "/v3/btc/observation-history",
                post(read_observation_history),
            )
            .route("/v3/btc/observation-history/resolve",post(resolve_observation_range))
            .route("/v3/btc/trade-history/session", delete(release))
            .route("/v3/btc/observation-history/session", delete(release_observations))
            .route("/v3/btc/evidence/{evidence_id}", get(read_evidence))
            .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
            .with_state(self)
    }
}
async fn capabilities(State(service): State<Service>) -> Response {
    use contract::btc::{BtcCapabilities, SourceCapability};
    let entry = |source, instrument: &str, stream, present| SourceCapability {
        source, instrument_id: instrument.into(), price_stream: stream,
        indexed_history: true, collector_present: present,
    };
    encoded(&BtcCapabilities {
        schema_version: "marketcow.btc-capabilities.v3".into(),
        sources: vec![
            entry(common::Source::BinanceSpotTrade as i32, "BTCUSDT.BINANCE", true, true),
            entry(common::Source::ChainlinkBtcUsd as i32, "BTCUSD.CHAINLINK", false, false),
            entry(common::Source::ChainlinkBtcUsdTwap60 as i32, "BTCUSD.CHAINLINK.TWAP60",
                service.chainlink_twap.is_some(), service.chainlink_twap.is_some()),
        ], history_page_limit: 1000, history_maximum_pages: btc_typed_history::MAX_PAGES as u32,
        history_maximum_rows: btc_typed_history::MAX_ROWS,
        history_maximum_bytes: btc_typed_history::SESSION_BYTES as u64,
        history_maximum_span_ms: 960_000,
        history_session_ttl_seconds: btc_typed_history::TTL.as_secs() as u32,
        history_explicit_release: true, history_request_identity: true,
    }, 64 * 1024)
}
async fn health(State(service): State<Service>) -> Response {
    let Some(now) = chrono::Utc::now().timestamp_nanos_opt() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "clock_unavailable", true);
    };
    let mut sources = vec![service.binance.typed_source_health(now.div_euclid(1_000_000)).await];
    // No receiver is created, no full snapshot copied, and no DB/archive scan.
    for (source, instrument) in [
        (common::Source::ChainlinkBtcUsd, "BTCUSD.CHAINLINK"),
        (common::Source::ChainlinkBtcUsdTwap60, "BTCUSD.CHAINLINK.TWAP60"),
    ] {
        if source == common::Source::ChainlinkBtcUsdTwap60 {
            if let Some(owner) = &service.chainlink_twap {
                sources.push(owner.typed_source_health(now.div_euclid(1_000_000)).await);
                continue;
            }
        }
        sources.push(contract::btc::SourceHealth {
            source: source as i32, instrument_id: instrument.into(),
            phase: "not_collected".into(), reason: Some("source_not_collected".into()),
            ..Default::default()
        });
    }
    encoded(&contract::btc::BtcHealth {
        schema_version: "marketcow.btc-health.v3".into(), reported_at_ns: now, sources,
    }, 64 * 1024)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceQuery {
    source: String,
}
async fn price_stream(
    State(service): State<Service>,
    lease: Option<axum::Extension<crate::sdk_access::Lease>>,
    query: Result<Query<SourceQuery>, axum::extract::rejection::QueryRejection>,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
) -> Response {
    let Ok(ws) = ws else {
        return failure(StatusCode::BAD_REQUEST, "invalid_websocket_upgrade", false);
    };
    let Ok(Query(query)) = query else {
        return failure(StatusCode::BAD_REQUEST, "invalid_source_query", false);
    };
    let owner = match query.source.as_str() {
        "binance_spot_trade" => PriceSource::Binance(service.binance.clone()),
        "chainlink_twap_60s" => match service.chainlink_twap.clone() {
            Some(owner) => PriceSource::Chainlink(owner),
            None => return failure(StatusCode::NOT_IMPLEMENTED, "source_not_collected", false),
        },
        "chainlink_btc_usd" => {
            return failure(StatusCode::NOT_IMPLEMENTED, "source_not_collected", false);
        }
        _ => return failure(StatusCode::BAD_REQUEST, "unsupported_source", false),
    };
    if !headers
        .get("sec-websocket-protocol")
        .and_then(|h| h.to_str().ok())
        .is_some_and(|h| {
            h.split(',')
                .any(|s| s.trim() == contract::PRICE_SUBPROTOCOL)
        })
    {
        return failure(
            StatusCode::UPGRADE_REQUIRED,
            "typed_subprotocol_required",
            false,
        );
    }
    let Ok(permit) = service.clients.try_acquire_owned() else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "btc_price_stream_capacity",
            true,
        );
    };
    ws.protocols([contract::PRICE_SUBPROTOCOL])
        .max_frame_size(4096)
        .max_message_size(4096)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            let _ = stream(socket, owner, lease.map(|axum::Extension(v)|v)).await;
        })
}
async fn send(socket: &mut WebSocket, frame: &Published, lease: Option<&crate::sdk_access::Lease>) -> Result<()> {
    if let Some(lease)=lease {lease.charge(frame.bytes.len())?;}
    tokio::time::timeout(
        Duration::from_secs(5),
        socket.send(Message::Binary(frame.bytes.clone())),
    )
    .await??;
    Ok(())
}
async fn stream(mut socket: WebSocket, owner: PriceSource, lease:Option<crate::sdk_access::Lease>) -> Result<()> {
    let (snapshot, mut receiver) = owner.subscribe().await?;
    let mut serial = snapshot.frame.stream_sequence;
    send(&mut socket, &snapshot,lease.as_ref()).await?;
    let mut ping = tokio::time::interval(Duration::from_secs(15));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _=async {match &lease {Some(lease)=>lease.expired().await,None=>std::future::pending().await}}=>break,
            frame=receiver.recv()=>{
                let frame=frame?;
                if frame.frame.stream_sequence<=serial {continue;}
                ensure!(serial.checked_add(1)==Some(frame.frame.stream_sequence),"typed_stream_sequence_gap");
                send(&mut socket,&frame,lease.as_ref()).await?;serial=frame.frame.stream_sequence;
            },
            incoming=socket.recv()=>match incoming {
                Some(Ok(Message::Ping(payload)))=>{
                    tokio::time::timeout(Duration::from_secs(5),socket.send(Message::Pong(payload))).await??;
                },
                Some(Ok(Message::Pong(_)))=>{},
                _=>break,
            },
            _=ping.tick()=>{tokio::time::timeout(Duration::from_secs(5),socket.send(Message::Ping(Vec::new().into()))).await??;},
        }
    }
    Ok(())
}
fn worker_failure(error: &anyhow::Error) -> Response {
    let message = error.to_string();
    let (status, code, retry) = match message.as_str() {
        "session_capacity" | "session_customer_capacity" | "session_public_capacity" | "history_capacity" => {
            (StatusCode::SERVICE_UNAVAILABLE, message.as_str(), true)
        }
        "session_expired_or_unknown" => (StatusCode::GONE, message.as_str(), true),
        "session_invalidated" | "session_range_mismatch" | "session_request_mismatch" => {
            (StatusCode::CONFLICT, message.as_str(), true)
        }
        "typed_database_unavailable" | "database_write_lag" | "database_history_empty" => (StatusCode::SERVICE_UNAVAILABLE, message.as_str(), true),
        "history_boundary_gap" => (StatusCode::CONFLICT,message.as_str(),false),
        "session_owner_mismatch" => (StatusCode::FORBIDDEN,message.as_str(),false),
        "source_not_collected" => (StatusCode::NOT_IMPLEMENTED, message.as_str(), false),
        "history_deadline" => (StatusCode::GATEWAY_TIMEOUT, message.as_str(), true),
        "history_trade_capacity"
        | "history_observation_capacity"
        | "history_page_capacity"
        | "history_session_bytes"
        | "history_span_capacity"
        | "history_gap_capacity"
        | "history_page_bytes" => (StatusCode::PAYLOAD_TOO_LARGE, message.as_str(), false),
        "invalid_request_id" | "invalid history range"
        | "missing history range"
        | "invalid page token"
        | "invalid page index"
        | "missing page token"
        | "page_out_of_range"
        | "missing release range"
        | "release session identity" => (StatusCode::BAD_REQUEST, "invalid_request", false),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            "history_database_failed",
            true,
        ),
    };
    failure(status, code, retry)
}
async fn read_observation_history(
    State(service): State<Service>,
    lease: Option<axum::Extension<crate::sdk_access::Lease>>,
    headers: HeaderMap,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Response {
    let Ok(body) = body else {
        return failure(StatusCode::PAYLOAD_TOO_LARGE, "request_capacity", false);
    };
    let request = match decode::<history::ObservationPageRequest>(&headers, &body) {
        Ok(v) => v,
        Err(_) => return failure(StatusCode::BAD_REQUEST, "invalid_typed_request", false),
    };
    let Ok(permit) = service.requests.clone().try_acquire_owned() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "history_capacity", true);
    };
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        service.history.read_observations_for_owner(&request,lease.as_ref().map(|v|v.0.customer_id()))
    })
    .await
    {
        Ok(Ok(page)) => encoded(&page, btc_typed_history::PAGE_BYTES),
        Ok(Err(error)) => worker_failure(&error),
        Err(_) => failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "history_worker_failed",
            true,
        ),
    }
}
async fn resolve_observation_range(State(service):State<Service>,headers:HeaderMap,
    body:Result<Bytes,axum::extract::rejection::BytesRejection>)->Response {
    let Ok(body)=body else{return failure(StatusCode::PAYLOAD_TOO_LARGE,"request_capacity",false);};
    let request=match decode::<history::ObservationRangeQuery>(&headers,&body){Ok(v)=>v,
        Err(_)=>return failure(StatusCode::BAD_REQUEST,"invalid_typed_request",false)};
    let Ok(permit)=service.requests.clone().try_acquire_owned()else{return failure(StatusCode::SERVICE_UNAVAILABLE,"history_capacity",true);};
    match tokio::task::spawn_blocking(move||{let _permit=permit;service.history.resolve_observations(&request)}).await {
        Ok(Ok(value))=>encoded(&value,64*1024),Ok(Err(error))=>worker_failure(&error),
        Err(_)=>failure(StatusCode::SERVICE_UNAVAILABLE,"history_worker_failed",true),
    }
}
async fn resolve_range(State(service):State<Service>,headers:HeaderMap,
    body:Result<Bytes,axum::extract::rejection::BytesRejection>)->Response {
    let Ok(body)=body else {return failure(StatusCode::PAYLOAD_TOO_LARGE,"request_capacity",false);};
    let request=match decode::<history::TimeRangeQuery>(&headers,&body) {Ok(v)=>v,Err(_)=>return failure(StatusCode::BAD_REQUEST,"invalid_typed_request",false)};
    let Ok(permit)=service.requests.clone().try_acquire_owned() else {return failure(StatusCode::SERVICE_UNAVAILABLE,"history_capacity",true);};
    match tokio::task::spawn_blocking(move||{let _permit=permit;let (health,_)=service.binance.typed_health();service.history.resolve_range(&request,health)}).await {
        Ok(Ok(value))=>encoded(&value,64*1024),Ok(Err(error))=>worker_failure(&error),
        Err(_)=>failure(StatusCode::SERVICE_UNAVAILABLE,"history_worker_failed",true),
    }
}
async fn read_history(
    State(service): State<Service>,
    lease: Option<axum::Extension<crate::sdk_access::Lease>>,
    headers: HeaderMap,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Response {
    let Ok(body) = body else {
        return failure(StatusCode::PAYLOAD_TOO_LARGE, "request_capacity", false);
    };
    let request = match decode::<history::PageRequest>(&headers, &body) {
        Ok(request) => request,
        Err(_) => return failure(StatusCode::BAD_REQUEST, "invalid_typed_request", false),
    };
    let Ok(permit) = service.requests.try_acquire_owned() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "history_capacity", true);
    };
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if request.page_token.is_some() {
            service.history.read_for_owner(&request,lease.as_ref().map(|v|v.0.customer_id()))
        } else {
            let (database, archive) = service.binance.typed_health();
            service.history.create_for_owner(
                request
                    .range
                    .ok_or_else(|| anyhow::anyhow!("missing history range"))?,
                archive,
                database,
                request.request_id,
                lease.as_ref().map(|v|v.0.customer_id()),
            )
        }
    })
    .await;
    match result {
        Ok(Ok(page)) => encoded(&page, btc_typed_history::PAGE_BYTES),
        Ok(Err(error)) => worker_failure(&error),
        Err(_) => failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "history_worker_failed",
            true,
        ),
    }
}
async fn release(
    State(service): State<Service>,
    lease: Option<axum::Extension<crate::sdk_access::Lease>>,
    headers: HeaderMap,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Response {
    let Ok(body) = body else {
        return failure(StatusCode::PAYLOAD_TOO_LARGE, "request_capacity", false);
    };
    let request = match decode::<history::ReleaseRequest>(&headers, &body) {
        Ok(request) => request,
        Err(_) => return failure(StatusCode::BAD_REQUEST, "invalid_typed_request", false),
    };
    match service.history.release_for_owner(&request,lease.as_ref().map(|v|v.0.customer_id())) {
        Ok(receipt) => encoded(&receipt, 64 * 1024),
        Err(error) => worker_failure(&error),
    }
}
async fn release_observations(State(service): State<Service>, lease:Option<axum::Extension<crate::sdk_access::Lease>>,headers: HeaderMap,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>) -> Response {
    let Ok(body) = body else {
        return failure(StatusCode::PAYLOAD_TOO_LARGE, "request_capacity", false);
    };
    let request = match decode::<history::ObservationReleaseRequest>(&headers, &body) {
        Ok(v) => v,
        Err(_) => return failure(StatusCode::BAD_REQUEST, "invalid_typed_request", false),
    };
    match service.history.release_observations_for_owner(&request,lease.as_ref().map(|v|v.0.customer_id())) {
        Ok(receipt) => encoded(&receipt, 64 * 1024),
        Err(error) => worker_failure(&error),
    }
}
async fn read_evidence(State(service): State<Service>, Path(id): Path<String>) -> Response {
    if uuid::Uuid::parse_str(&id).is_err() {
        return failure(StatusCode::BAD_REQUEST, "invalid_evidence_id", false);
    }
    let Ok(permit) = service.requests.try_acquire_owned() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "evidence_capacity", true);
    };
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let db = super::btc_typed_store::open_reader(&service.database)?;
        if let Some(value) = super::btc_typed_store::read_evidence(&db, &id)? {
            return Ok(Some(value));
        }
        let path = service.history.observation_path()?;
        if !path.is_file() {
            return Ok(None);
        }
        let db = super::btc_typed_store::open_reader(&path)?;
        super::btc_typed_store::read_evidence(&db, &id)
    })
    .await
    {
        Ok(Ok(Some(evidence))) => encoded(&evidence, 2 * 1024 * 1024),
        Ok(Ok(None)) => failure(StatusCode::NOT_FOUND, "evidence_not_found", false),
        _ => failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "evidence_database_failed",
            true,
        ),
    }
}
