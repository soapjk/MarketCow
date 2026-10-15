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
            .route("/v3/btc/price-stream", get(price_stream))
            .route("/v3/btc/trade-history", post(read_history))
            .route(
                "/v3/btc/observation-history",
                post(read_observation_history),
            )
            .route("/v3/btc/trade-history/session", delete(release))
            .route("/v3/btc/evidence/{evidence_id}", get(read_evidence))
            .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
            .with_state(self)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceQuery {
    source: String,
}
async fn price_stream(
    State(service): State<Service>,
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
            let _ = stream(socket, owner).await;
        })
}
async fn send(socket: &mut WebSocket, frame: &Published) -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(5),
        socket.send(Message::Binary(frame.bytes.clone())),
    )
    .await??;
    Ok(())
}
async fn stream(mut socket: WebSocket, owner: PriceSource) -> Result<()> {
    let (snapshot, mut receiver) = owner.subscribe().await?;
    let mut serial = snapshot.frame.stream_sequence;
    send(&mut socket, &snapshot).await?;
    let mut ping = tokio::time::interval(Duration::from_secs(15));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            frame=receiver.recv()=>{
                let frame=frame?;
                if frame.frame.stream_sequence<=serial {continue;}
                ensure!(serial.checked_add(1)==Some(frame.frame.stream_sequence),"typed_stream_sequence_gap");
                send(&mut socket,&frame).await?;serial=frame.frame.stream_sequence;
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
        "session_capacity" | "history_capacity" => {
            (StatusCode::SERVICE_UNAVAILABLE, message.as_str(), true)
        }
        "session_expired_or_unknown" => (StatusCode::GONE, message.as_str(), true),
        "session_invalidated" | "session_range_mismatch" => {
            (StatusCode::CONFLICT, message.as_str(), true)
        }
        "typed_database_unavailable" => (StatusCode::SERVICE_UNAVAILABLE, message.as_str(), true),
        "source_not_collected" => (StatusCode::NOT_IMPLEMENTED, message.as_str(), false),
        "history_deadline" => (StatusCode::GATEWAY_TIMEOUT, message.as_str(), true),
        "history_trade_capacity"
        | "history_observation_capacity"
        | "history_page_capacity"
        | "history_session_bytes"
        | "history_span_capacity"
        | "history_gap_capacity"
        | "history_page_bytes" => (StatusCode::PAYLOAD_TOO_LARGE, message.as_str(), false),
        "invalid history range"
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
        service.history.read_observations(&request)
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
async fn read_history(
    State(service): State<Service>,
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
            service.history.read(&request)
        } else {
            let (database, archive) = service.binance.typed_health();
            service.history.create_with_health(
                request
                    .range
                    .ok_or_else(|| anyhow::anyhow!("missing history range"))?,
                archive,
                database,
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
    match service.history.release(&request) {
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
