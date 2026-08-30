//! Strict Polymarket CLOB raw-frame normalizer. Transport and cursor ownership stay outside this
//! crate; a frame is normalized without network, storage, or async-runtime dependencies.

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use marketcow_core::{
    CONTRACT_VERSION, CanonicalEvent, EventKind, Level, MarketRecord, NegativeRiskRelation, Price,
    Side, SideLevels, SourceEvidence,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, watch},
};
use tokio_tungstenite::tungstenite::Message;
use url::Url;

pub const NORMALIZER_VERSION: &str = "marketcow.polymarket.normalizer.v1";
pub const CLOB_MARKET_STREAM_SOURCE_URL: &str =
    "https://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Debug, Clone)]
pub struct PolymarketTransportConfig {
    endpoint: Url,
    connect_timeout: Duration,
    reconnect_delay: Duration,
    heartbeat_interval: Duration,
    maximum_reconnect_attempts: u32,
    maximum_frame_bytes: usize,
    https_proxy: Option<Url>,
}

impl PolymarketTransportConfig {
    pub fn production() -> Self {
        Self {
            endpoint: Url::parse("wss://ws-subscriptions-clob.polymarket.com/ws/market")
                .expect("fixed Polymarket endpoint is valid"),
            connect_timeout: Duration::from_secs(10),
            reconnect_delay: Duration::from_secs(1),
            heartbeat_interval: Duration::from_secs(10),
            maximum_reconnect_attempts: 32,
            maximum_frame_bytes: 8 * 1024 * 1024,
            https_proxy: std::env::var("HTTPS_PROXY")
                .or_else(|_| std::env::var("https_proxy"))
                .ok()
                .and_then(|value| Url::parse(&value).ok()),
        }
    }

    fn validate(&self) -> Result<(), TransportError> {
        let production = self.endpoint.scheme() == "wss"
            && self.endpoint.host_str() == Some("ws-subscriptions-clob.polymarket.com")
            && self.endpoint.port().is_none()
            && self.endpoint.path() == "/ws/market"
            && self.endpoint.query().is_none()
            && self.endpoint.fragment().is_none();
        #[cfg(test)]
        let test = self.endpoint.scheme() == "ws"
            && self
                .endpoint
                .host_str()
                .and_then(|host| host.parse::<std::net::IpAddr>().ok())
                .is_some_and(|address| address.is_loopback())
            && self.endpoint.port().is_some()
            && self.endpoint.path() == "/ws/market";
        #[cfg(not(test))]
        let test = false;
        let proxy_valid = self.https_proxy.as_ref().is_none_or(|proxy| {
            proxy.scheme() == "http"
                && proxy.host_str().is_some()
                && proxy.port_or_known_default().is_some()
                && proxy.username().is_empty()
                && proxy.password().is_none()
                && proxy.path() == "/"
                && proxy.query().is_none()
                && proxy.fragment().is_none()
        });
        if (!production && !test)
            || !proxy_valid
            || self.connect_timeout.is_zero()
            || self.reconnect_delay.is_zero()
            || self.heartbeat_interval.is_zero()
            || self.maximum_reconnect_attempts == 0
            || !(1..=8 * 1024 * 1024).contains(&self.maximum_frame_bytes)
        {
            return Err(TransportError::InvalidConfig);
        }
        Ok(())
    }

    #[cfg(test)]
    fn loopback(endpoint: Url) -> Self {
        Self {
            endpoint,
            connect_timeout: Duration::from_secs(2),
            reconnect_delay: Duration::from_millis(10),
            heartbeat_interval: Duration::from_millis(100),
            maximum_reconnect_attempts: 2,
            maximum_frame_bytes: 64 * 1024,
            https_proxy: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawTransportFrame {
    pub raw_payload: Value,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransportError {
    #[error("Polymarket transport configuration is invalid")]
    InvalidConfig,
    #[error("Polymarket WebSocket connect timed out")]
    ConnectTimeout,
    #[error("Polymarket HTTPS proxy connection failed")]
    ProxyConnectFailed,
    #[error("Polymarket HTTPS proxy rejected CONNECT")]
    ProxyConnectRejected,
    #[error("Polymarket WebSocket connection failed")]
    ConnectFailed,
    #[error("Polymarket WebSocket handshake was rejected")]
    HandshakeRejected,
    #[error("Polymarket WebSocket send failed")]
    SendFailed,
    #[error("Polymarket WebSocket read failed")]
    ReadFailed,
    #[error("Polymarket WebSocket frame is invalid")]
    InvalidFrame,
    #[error("Polymarket WebSocket frame exceeds the bounded limit")]
    FrameTooLarge,
    #[error("Polymarket transport output queue is full or closed")]
    Backpressure,
    #[error("Polymarket WebSocket reconnect budget was exhausted")]
    ReconnectExhausted,
}

impl TransportError {
    pub fn reason_code(&self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_config",
            Self::ConnectTimeout => "connect_timeout",
            Self::ProxyConnectFailed => "proxy_connect_failed",
            Self::ProxyConnectRejected => "proxy_connect_rejected",
            Self::ConnectFailed => "connect_failed",
            Self::HandshakeRejected => "handshake_rejected",
            Self::SendFailed => "send_failed",
            Self::ReadFailed => "read_failed",
            Self::InvalidFrame => "invalid_frame",
            Self::FrameTooLarge => "frame_too_large",
            Self::Backpressure => "strict_backpressure",
            Self::ReconnectExhausted => "reconnect_exhausted",
        }
    }
}

pub async fn run_polymarket_transport(
    config: PolymarketTransportConfig,
    token_ids: Vec<String>,
    output: mpsc::Sender<Vec<RawTransportFrame>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), TransportError> {
    config.validate()?;
    let tokens = token_ids.into_iter().collect::<BTreeSet<_>>();
    if tokens.is_empty()
        || tokens.len() > 500
        || tokens
            .iter()
            .any(|token| token.is_empty() || token.len() > 128)
    {
        return Err(TransportError::InvalidConfig);
    }
    for attempt in 1..=config.maximum_reconnect_attempts {
        if *shutdown.borrow() {
            return Ok(());
        }
        if !publish_connection_gaps(&tokens, attempt, &output, &mut shutdown).await? {
            return Ok(());
        }
        match run_polymarket_connection(&config, &tokens, &output, &mut shutdown).await {
            Ok(ConnectionEnd::Shutdown) => return Ok(()),
            Ok(ConnectionEnd::Disconnected) => {
                tracing::warn!(
                    attempt,
                    reason = "upstream_disconnected",
                    "polymarket_transport_retry"
                );
            }
            Err(TransportError::Backpressure) => return Err(TransportError::Backpressure),
            Err(error) if attempt == config.maximum_reconnect_attempts => {
                tracing::warn!(
                    attempt,
                    reason = error.reason_code(),
                    "polymarket_transport_exhausted"
                );
                return Err(error);
            }
            Err(error) => {
                tracing::warn!(
                    attempt,
                    reason = error.reason_code(),
                    "polymarket_transport_retry"
                );
            }
        }
        if attempt == config.maximum_reconnect_attempts {
            return Err(TransportError::ReconnectExhausted);
        }
        tokio::select! {
            _ = tokio::time::sleep(config.reconnect_delay) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
        }
    }
    Err(TransportError::ReconnectExhausted)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionEnd {
    Shutdown,
    Disconnected,
}

async fn publish_connection_gaps(
    tokens: &BTreeSet<String>,
    attempt: u32,
    output: &mpsc::Sender<Vec<RawTransportFrame>>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<bool, TransportError> {
    let received_at = Utc::now();
    let timestamp = received_at.timestamp_millis().to_string();
    let frames = tokens
        .iter()
        .map(|token_id| RawTransportFrame {
            raw_payload: serde_json::json!({
                "event_type":"source_gap", "asset_id":token_id,
                "reason":"upstream_connection_boundary", "attempt":attempt,
                "timestamp":timestamp,
            }),
            received_at,
        })
        .collect();
    send_bounded(output, frames, shutdown).await
}

async fn send_bounded(
    output: &mpsc::Sender<Vec<RawTransportFrame>>,
    frames: Vec<RawTransportFrame>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<bool, TransportError> {
    tokio::select! {
        permit = output.reserve() => {
            permit.map_err(|_| TransportError::Backpressure)?.send(frames);
            Ok(true)
        }
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                Ok(false)
            } else {
                Err(TransportError::Backpressure)
            }
        }
    }
}

async fn run_polymarket_connection(
    config: &PolymarketTransportConfig,
    tokens: &BTreeSet<String>,
    output: &mpsc::Sender<Vec<RawTransportFrame>>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<ConnectionEnd, TransportError> {
    let (mut socket, response) = connect_websocket(config).await?;
    if response.status() != 101 {
        return Err(TransportError::HandshakeRejected);
    }
    socket
        .send(Message::Text(
            serde_json::to_string(&serde_json::json!({
                // This transport owns an explicitly pinned token scope. Venue-wide lifecycle
                // announcements would introduce unrelated catalog gaps into that projection.
                "assets_ids":tokens, "type":"market", "custom_feature_enabled":false,
            }))
            .expect("subscription serializes")
            .into(),
        ))
        .await
        .map_err(|_| TransportError::SendFailed)?;
    loop {
        let heartbeat = tokio::time::sleep(config.heartbeat_interval);
        tokio::pin!(heartbeat);
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = socket.close(None).await;
                    return Ok(ConnectionEnd::Shutdown);
                }
            }
            message = socket.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    if text == "PONG" { continue; }
                    if text.len() > config.maximum_frame_bytes {
                        let _ = socket.close(None).await;
                        return Err(TransportError::FrameTooLarge);
                    }
                    let received_at = Utc::now();
                    let payload: Value = serde_json::from_str(&text)
                        .map_err(|_| TransportError::InvalidFrame)?;
                    let values = match payload {
                        Value::Array(values) => values,
                        value @ Value::Object(_) => vec![value],
                        _ => return Err(TransportError::InvalidFrame),
                    };
                    if values.is_empty() || values.iter().any(|value| !value.is_object()) {
                        return Err(TransportError::InvalidFrame);
                    }
                    let frames = values.into_iter().map(|raw_payload| RawTransportFrame {
                        raw_payload, received_at,
                    }).collect();
                    if !send_bounded(output, frames, shutdown).await? {
                        let _ = socket.close(None).await;
                        return Ok(ConnectionEnd::Shutdown);
                    }
                }
                Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await
                    .map_err(|_| TransportError::SendFailed)?,
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) | None => return Ok(ConnectionEnd::Disconnected),
                Some(Ok(Message::Binary(_) | Message::Frame(_))) => {
                    let _ = socket.close(None).await;
                    return Err(TransportError::InvalidFrame);
                }
                Some(Err(_)) => return Err(TransportError::ReadFailed),
            },
            _ = &mut heartbeat => socket.send(Message::Text("PING".into())).await
                .map_err(|_| TransportError::SendFailed)?,
        }
    }
}

async fn connect_websocket(
    config: &PolymarketTransportConfig,
) -> Result<
    (
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tokio_tungstenite::tungstenite::handshake::client::Response,
    ),
    TransportError,
> {
    static TLS_PROVIDER: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    TLS_PROVIDER.get_or_init(|| {
        // rustls 0.23 deliberately requires an explicit process-wide provider when downstream
        // feature unification cannot select exactly one. MarketCow standardizes on ring.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
    let host = config
        .endpoint
        .host_str()
        .ok_or(TransportError::InvalidConfig)?;
    let port = config
        .endpoint
        .port_or_known_default()
        .ok_or(TransportError::InvalidConfig)?;
    let connect = async {
        let stream = if let Some(proxy) = &config.https_proxy {
            connect_http_proxy(proxy, host, port).await?
        } else {
            connect_tcp(host, port)
                .await
                .map_err(|_| TransportError::ConnectFailed)?
        };
        stream
            .set_nodelay(true)
            .map_err(|_| TransportError::ConnectFailed)?;
        tokio_tungstenite::client_async_tls_with_config(
            config.endpoint.as_str(),
            stream,
            None,
            None,
        )
        .await
        .map_err(|_| TransportError::ConnectFailed)
    };
    tokio::time::timeout(config.connect_timeout, connect)
        .await
        .map_err(|_| TransportError::ConnectTimeout)?
}

async fn connect_tcp(host: &str, port: u16) -> std::io::Result<tokio::net::TcpStream> {
    let mut addresses = tokio::net::lookup_host((host, port))
        .await?
        .collect::<Vec<_>>();
    // Some hosts publish IPv6 without usable egress. Prefer IPv4, while retaining IPv6 fallback.
    addresses.sort_by_key(|address| u8::from(!address.is_ipv4()));
    let mut last_error = None;
    for address in addresses {
        match tokio::net::TcpStream::connect(address).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "host resolved to no addresses",
        )
    }))
}

async fn connect_http_proxy(
    proxy: &Url,
    target_host: &str,
    target_port: u16,
) -> Result<tokio::net::TcpStream, TransportError> {
    let proxy_host = proxy.host_str().ok_or(TransportError::ProxyConnectFailed)?;
    let proxy_port = proxy
        .port_or_known_default()
        .ok_or(TransportError::ProxyConnectFailed)?;
    let mut stream = connect_tcp(proxy_host, proxy_port)
        .await
        .map_err(|_| TransportError::ProxyConnectFailed)?;
    let authority = format!("{target_host}:{target_port}");
    let request = format!(
        "CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|_| TransportError::ProxyConnectFailed)?;
    let mut response = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 512];
    while !response.windows(4).any(|window| window == b"\r\n\r\n") {
        if response.len() >= 8 * 1024 {
            return Err(TransportError::ProxyConnectRejected);
        }
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|_| TransportError::ProxyConnectFailed)?;
        if read == 0 {
            return Err(TransportError::ProxyConnectFailed);
        }
        response.extend_from_slice(&chunk[..read]);
    }
    let status_line = response
        .split(|byte| *byte == b'\n')
        .next()
        .and_then(|line| std::str::from_utf8(line).ok())
        .unwrap_or_default();
    if !status_line.starts_with("HTTP/1.1 200 ") && !status_line.starts_with("HTTP/1.0 200 ") {
        return Err(TransportError::ProxyConnectRejected);
    }
    Ok(stream)
}

#[derive(Debug, Clone)]
pub struct NormalizerConfig {
    pub scope_id: String,
    pub config_revision: String,
    pub maximum_source_delay_ms: i64,
}

impl NormalizerConfig {
    pub fn new(scope_id: impl Into<String>, config_revision: impl Into<String>) -> Self {
        Self {
            scope_id: scope_id.into(),
            config_revision: config_revision.into(),
            maximum_source_delay_ms: 5_000,
        }
    }

    fn validate(&self) -> Result<(), NormalizeError> {
        if self.scope_id.is_empty()
            || self.config_revision.is_empty()
            || self.maximum_source_delay_ms < 0
        {
            return Err(NormalizeError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum NormalizeError {
    #[error("normalizer config is incomplete")]
    InvalidConfig,
    #[error("raw frame must be an object")]
    FrameNotObject,
    #[error("unsupported event type: {0}")]
    UnsupportedEventType(String),
    #[error("required field is missing: {0}")]
    MissingField(&'static str),
    #[error("timestamp is invalid")]
    InvalidTimestamp,
    #[error("side must be BUY/BID or SELL/ASK")]
    InvalidSide,
    #[error("book levels are invalid")]
    InvalidLevels,
    #[error("financial decimal is invalid")]
    InvalidDecimal,
    #[error("catalog payload is invalid")]
    InvalidCatalog,
}

struct FrameContext {
    raw_payload: Arc<Value>,
    raw_sha256: String,
    received_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
}

pub fn normalize_frame(
    config: &NormalizerConfig,
    raw_payload: Value,
    received_at: DateTime<Utc>,
    first_cursor: u64,
) -> Result<Vec<CanonicalEvent>, NormalizeError> {
    normalize_frame_with_book_tick(config, raw_payload, received_at, first_cursor, None)
}

pub fn normalize_frame_with_book_tick(
    config: &NormalizerConfig,
    raw_payload: Value,
    received_at: DateTime<Utc>,
    first_cursor: u64,
    verified_book_tick: Option<Price>,
) -> Result<Vec<CanonicalEvent>, NormalizeError> {
    config.validate()?;
    let raw = raw_payload
        .as_object()
        .ok_or(NormalizeError::FrameNotObject)?;
    let event_type = string_alias(raw, &["event_type", "type"])
        .ok_or(NormalizeError::MissingField("event_type"))?;
    let reported_at = parse_observed_at(raw.get("timestamp"), received_at)?;
    // A `book` frame is a complete state observation delivered at subscription/recovery time.
    // Its exchange timestamp can legitimately be old for a quiet market; freshness is therefore
    // anchored to receipt while the source timestamp remains preserved in the immutable raw frame.
    let observed_at = if event_type == "book" {
        received_at
    } else {
        reported_at
    };
    let raw_sha256 = content_sha256(&raw_payload)?;
    let context = FrameContext {
        raw_payload: Arc::new(raw_payload.clone()),
        raw_sha256,
        received_at,
        observed_at,
    };
    match event_type.as_str() {
        "catalog_revision" => {
            let catalog_revision = string_alias(raw, &["catalog_revision", "revision"])
                .filter(|value| !value.is_empty())
                .ok_or(NormalizeError::MissingField("catalog_revision"))?;
            let markets = serde_json::from_value::<Vec<MarketRecord>>(
                raw.get("markets")
                    .cloned()
                    .ok_or(NormalizeError::MissingField("markets"))?,
            )
            .map_err(|_| NormalizeError::InvalidCatalog)?;
            let negative_risk_relations = serde_json::from_value::<Vec<NegativeRiskRelation>>(
                raw.get("negative_risk_relations")
                    .cloned()
                    .unwrap_or_else(|| Value::Array(Vec::new())),
            )
            .map_err(|_| NormalizeError::InvalidCatalog)?;
            Ok(vec![build_event(
                config,
                &context,
                first_cursor,
                &catalog_revision,
                EventKind::CatalogSnapshot {
                    catalog_revision: catalog_revision.clone(),
                    markets,
                    negative_risk_relations,
                },
            )])
        }
        "new_market" => {
            let condition_id = string_alias(raw, &["condition_id", "market"])
                .filter(|value| !value.is_empty())
                .ok_or(NormalizeError::MissingField("condition_id"))?;
            Ok(vec![build_event(
                config,
                &context,
                first_cursor,
                &condition_id,
                EventKind::NewMarket {
                    condition_id: condition_id.clone(),
                },
            )])
        }
        "market_resolved" => {
            let condition_id = string_alias(raw, &["condition_id", "market"])
                .filter(|value| !value.is_empty())
                .ok_or(NormalizeError::MissingField("condition_id"))?;
            let winning_token_id = string_alias(raw, &["winning_asset_id", "winning_token_id"])
                .filter(|value| !value.is_empty());
            let winning_outcome =
                string_alias(raw, &["winning_outcome"]).filter(|value| !value.is_empty());
            Ok(vec![build_event(
                config,
                &context,
                first_cursor,
                &condition_id,
                EventKind::MarketResolved {
                    condition_id: condition_id.clone(),
                    winning_token_id,
                    winning_outcome,
                },
            )])
        }
        "source_gap" => {
            let token_id = token_id(raw)?;
            let reason = string_alias(raw, &["reason"])
                .filter(|value| !value.is_empty())
                .ok_or(NormalizeError::MissingField("reason"))?;
            Ok(vec![build_event(
                config,
                &context,
                first_cursor,
                &token_id,
                EventKind::SourceGap {
                    token_id: token_id.clone(),
                    reason,
                },
            )])
        }
        "book" => {
            let token_id = string_alias(raw, &["asset_id", "token_id"])
                .filter(|value| !value.is_empty())
                .ok_or(NormalizeError::MissingField("asset_id"))?;
            let tick_size = match value_text(raw.get("tick_size")) {
                Some(tick_text) => {
                    Price::parse_tick(&tick_text).map_err(|_| NormalizeError::InvalidDecimal)?
                }
                None => verified_book_tick.ok_or(NormalizeError::MissingField("tick_size"))?,
            };
            let bids = parse_levels(raw.get("bids"))?;
            let asks = parse_levels(raw.get("asks"))?;
            let tick_version =
                content_sha256(&serde_json::json!({"tick_size": tick_size.0.to_string()}))?;
            Ok(vec![build_event(
                config,
                &context,
                first_cursor,
                &token_id,
                EventKind::FullBook {
                    token_id: token_id.clone(),
                    bids,
                    asks,
                    tick_size,
                    tick_version,
                },
            )])
        }
        "price_change" => {
            let changes = raw
                .get("price_changes")
                .or_else(|| raw.get("changes"))
                .and_then(Value::as_array)
                .ok_or(NormalizeError::MissingField("price_changes"))?;
            if changes.is_empty() {
                return Err(NormalizeError::InvalidLevels);
            }
            let mut by_token: BTreeMap<String, Vec<SideLevels>> = BTreeMap::new();
            for change in changes {
                let item = change.as_object().ok_or(NormalizeError::InvalidLevels)?;
                let token_id = string_alias(item, &["asset_id", "token_id"])
                    .filter(|value| !value.is_empty())
                    .ok_or(NormalizeError::MissingField("asset_id"))?;
                let side = match string_alias(item, &["side"])
                    .unwrap_or_default()
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "BUY" | "BID" => Side::Bid,
                    "SELL" | "ASK" => Side::Ask,
                    _ => return Err(NormalizeError::InvalidSide),
                };
                let price =
                    value_text(item.get("price")).ok_or(NormalizeError::MissingField("price"))?;
                let size =
                    value_text(item.get("size")).ok_or(NormalizeError::MissingField("size"))?;
                let level =
                    Level::new(&price, &size).map_err(|_| NormalizeError::InvalidDecimal)?;
                by_token.entry(token_id).or_default().push(SideLevels {
                    side,
                    levels: vec![level],
                });
            }
            by_token
                .into_iter()
                .enumerate()
                .map(|(offset, (token_id, changes))| {
                    let cursor = first_cursor
                        .checked_add(offset as u64)
                        .ok_or(NormalizeError::InvalidConfig)?;
                    Ok(build_event(
                        config,
                        &context,
                        cursor,
                        &token_id,
                        EventKind::AtomicDelta {
                            token_id: token_id.clone(),
                            changes,
                        },
                    ))
                })
                .collect()
        }
        "best_bid_ask" => {
            let token_id = token_id(raw)?;
            if !raw.contains_key("best_bid") || !raw.contains_key("best_ask") {
                return Err(NormalizeError::MissingField("best_bid/best_ask"));
            }
            let best_bid = optional_price(raw.get("best_bid"))?;
            let best_ask = optional_price(raw.get("best_ask"))?;
            if best_bid
                .as_ref()
                .zip(best_ask.as_ref())
                .is_some_and(|(bid, ask)| bid >= ask)
            {
                return Err(NormalizeError::InvalidLevels);
            }
            Ok(vec![build_event(
                config,
                &context,
                first_cursor,
                &token_id,
                EventKind::BestBidAsk {
                    token_id: token_id.clone(),
                    best_bid,
                    best_ask,
                },
            )])
        }
        "last_trade_price" => {
            let token_id = token_id(raw)?;
            let price = required_price(raw.get("price"), "price")?;
            Ok(vec![build_event(
                config,
                &context,
                first_cursor,
                &token_id,
                EventKind::LastTradePrice {
                    token_id: token_id.clone(),
                    price,
                },
            )])
        }
        "tick_size_change" => {
            let token_id = token_id(raw)?;
            let new_tick_size = required_tick(raw.get("new_tick_size"), "new_tick_size")?;
            let old_tick_size = match raw.get("old_tick_size") {
                None | Some(Value::Null) => None,
                value => Some(required_tick(value, "old_tick_size")?),
            };
            let tick_version = content_sha256(&serde_json::json!({
                "old_tick_size": old_tick_size,
                "new_tick_size": new_tick_size,
                "source_revision": context.raw_sha256,
            }))?;
            Ok(vec![build_event(
                config,
                &context,
                first_cursor,
                &token_id,
                EventKind::TickSizeChange {
                    token_id: token_id.clone(),
                    old_tick_size,
                    new_tick_size,
                    tick_version,
                },
            )])
        }
        other => Err(NormalizeError::UnsupportedEventType(other.into())),
    }
}

fn token_id(raw: &serde_json::Map<String, Value>) -> Result<String, NormalizeError> {
    string_alias(raw, &["asset_id", "token_id"])
        .filter(|value| !value.is_empty())
        .ok_or(NormalizeError::MissingField("asset_id"))
}

fn required_price(value: Option<&Value>, field: &'static str) -> Result<Price, NormalizeError> {
    let text = value_text(value).ok_or(NormalizeError::MissingField(field))?;
    Price::parse(&text).map_err(|_| NormalizeError::InvalidDecimal)
}

fn required_tick(value: Option<&Value>, field: &'static str) -> Result<Price, NormalizeError> {
    let text = value_text(value).ok_or(NormalizeError::MissingField(field))?;
    Price::parse_tick(&text).map_err(|_| NormalizeError::InvalidDecimal)
}

fn optional_price(value: Option<&Value>) -> Result<Option<Price>, NormalizeError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        value => required_price(value, "best_bid/best_ask").map(Some),
    }
}

fn build_event(
    config: &NormalizerConfig,
    context: &FrameContext,
    cursor: u64,
    token_id: &str,
    kind: EventKind,
) -> CanonicalEvent {
    let event_id = hex::encode(Sha256::digest(format!(
        "{}\0{}\0{}\0{}",
        config.scope_id, NORMALIZER_VERSION, context.raw_sha256, token_id
    )));
    let delay_ms = context
        .received_at
        .signed_duration_since(context.observed_at)
        .num_milliseconds();
    CanonicalEvent {
        schema_version: CONTRACT_VERSION.into(),
        cursor,
        event_id,
        scope_id: config.scope_id.clone(),
        received_at: context.received_at,
        source_observed_at: context.observed_at,
        normalizer_version: NORMALIZER_VERSION.into(),
        config_revision: config.config_revision.clone(),
        source: SourceEvidence {
            source: "polymarket_clob_ws".into(),
            source_url: Some(CLOB_MARKET_STREAM_SOURCE_URL.into()),
            requested_at: context.received_at,
            responded_at: context.received_at,
            observed_at: context.observed_at,
            raw_sha256: context.raw_sha256.clone(),
            update_frequency: "realtime".into(),
            revision: context.raw_sha256.clone(),
            missing: false,
            delayed: delay_ms > config.maximum_source_delay_ms,
            duplicate: false,
            revised: false,
        },
        raw_payload: context.raw_payload.clone(),
        kind,
    }
}

/// Rebinds a byte-identical full book to a new recovery boundary. Callers must use this only when
/// the content-addressed identity is already known and the token currently has a durable source
/// gap. Normal repeated snapshots remain idempotent; an identical snapshot observed after a
/// connection boundary can still close that gap without weakening incremental-event deduplication.
pub fn bind_full_book_recovery_identity(event: &mut CanonicalEvent) -> bool {
    if !matches!(&event.kind, EventKind::FullBook { .. }) {
        return false;
    }
    event.event_id = hex::encode(Sha256::digest(format!(
        "{}\0full_book_recovery\0{}",
        event.event_id,
        event
            .received_at
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
    )));
    true
}

fn parse_levels(value: Option<&Value>) -> Result<Vec<Level>, NormalizeError> {
    value
        .and_then(Value::as_array)
        .ok_or(NormalizeError::InvalidLevels)?
        .iter()
        .map(|value| {
            let item = value.as_object().ok_or(NormalizeError::InvalidLevels)?;
            let price = value_text(item.get("price")).ok_or(NormalizeError::InvalidLevels)?;
            let size = value_text(item.get("size")).ok_or(NormalizeError::InvalidLevels)?;
            Level::new(&price, &size).map_err(|_| NormalizeError::InvalidDecimal)
        })
        .collect()
}

fn parse_observed_at(
    value: Option<&Value>,
    fallback: DateTime<Utc>,
) -> Result<DateTime<Utc>, NormalizeError> {
    let Some(value) = value else {
        return Ok(fallback);
    };
    if let Some(milliseconds) = value.as_i64().or_else(|| value.as_str()?.parse().ok()) {
        return DateTime::from_timestamp_millis(milliseconds)
            .ok_or(NormalizeError::InvalidTimestamp);
    }
    value
        .as_str()
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|value| value.with_timezone(&Utc))
        .ok_or(NormalizeError::InvalidTimestamp)
}

fn string_alias(map: &serde_json::Map<String, Value>, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| value_text(map.get(*name)))
}

fn value_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn content_sha256(value: &Value) -> Result<String, NormalizeError> {
    serde_json::to_vec(value)
        .map(|bytes| hex::encode(Sha256::digest(bytes)))
        .map_err(|_| NormalizeError::FrameNotObject)
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketcow_core::{DurableLog, PersistedEvent, SingleWriter};

    struct MemoryLog(Vec<PersistedEvent>);
    impl DurableLog for MemoryLog {
        fn append(&mut self, _: &CanonicalEvent) -> Result<(), marketcow_core::CoreError> {
            unreachable!("append_outcome is authoritative")
        }

        fn append_outcome(
            &mut self,
            value: &PersistedEvent,
        ) -> Result<(), marketcow_core::CoreError> {
            self.0.push(value.clone());
            Ok(())
        }
    }

    fn at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-08-03T04:00:03Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn config() -> NormalizerConfig {
        NormalizerConfig::new("scope", "config-v1")
    }

    fn snapshot(cursor: u64) -> CanonicalEvent {
        normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"book", "asset_id":"yes-1", "timestamp":"2026-08-03T04:00:00Z",
                "tick_size":"0.01", "bids":[{"price":"0.40","size":"10"}],
                "asks":[{"price":"0.42","size":"11"}], "hash":"source-hash"
            }),
            at(),
            cursor,
        )
        .unwrap()
        .remove(0)
    }

    #[test]
    fn snapshot_is_typed_exact_and_raw_evidence_is_verified_by_writer() {
        let normalized = snapshot(1);
        assert_eq!(normalized.source.raw_sha256.len(), 64);
        assert!(!normalized.source.delayed);
        let EventKind::FullBook {
            tick_size,
            tick_version,
            ..
        } = &normalized.kind
        else {
            panic!("expected full book")
        };
        assert_eq!(tick_size.0.to_string(), "0.01");
        assert_eq!(tick_version.len(), 64);
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        assert!(writer.apply(normalized).unwrap().persisted.applied);
        assert!(writer.projection().ready);
    }

    #[test]
    fn book_without_tick_requires_an_explicit_verified_fallback() {
        let raw = serde_json::json!({
            "event_type":"book", "asset_id":"yes-1", "timestamp":"2026-08-03T04:00:00Z",
            "bids":[{"price":"0.40","size":"10"}],
            "asks":[{"price":"0.42","size":"11"}]
        });
        assert_eq!(
            normalize_frame(&config(), raw.clone(), at(), 1),
            Err(NormalizeError::MissingField("tick_size"))
        );

        let event = normalize_frame_with_book_tick(
            &config(),
            raw,
            at(),
            1,
            Some(Price::parse_tick("0.01").unwrap()),
        )
        .unwrap()
        .remove(0);
        assert!(matches!(
            event.kind,
            EventKind::FullBook { ref tick_size, .. } if tick_size.0.to_string() == "0.01"
        ));
        assert!(event.raw_payload.get("tick_size").is_none());
    }

    #[test]
    fn same_token_multi_side_message_is_one_atomic_delta() {
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        writer.apply(snapshot(1)).unwrap();
        let raw = serde_json::json!({
            "event_type":"price_change", "timestamp":"2026-08-03T04:00:01Z",
            "price_changes":[
                {"asset_id":"yes-1","side":"BUY","price":"0.43","size":"1"},
                {"asset_id":"yes-1","side":"SELL","price":"0.42","size":"0"},
                {"asset_id":"yes-1","side":"SELL","price":"0.44","size":"2"}
            ]
        });
        let events = normalize_frame(&config(), raw, at(), 2).unwrap();
        assert_eq!(events.len(), 1);
        let outcome = writer.apply(events.into_iter().next().unwrap()).unwrap();
        assert!(outcome.persisted.applied);
        let book = &outcome.projection.books["yes-1"];
        assert_eq!(book.bids.last_key_value().unwrap().0.0.to_string(), "0.43");
        assert_eq!(book.asks.first_key_value().unwrap().0.0.to_string(), "0.44");
    }

    #[test]
    fn multi_token_message_has_deterministic_token_order_and_contiguous_cursors() {
        let raw = serde_json::json!({
            "event_type":"price_change", "timestamp":"2026-08-03T04:00:01Z",
            "price_changes":[
                {"asset_id":"z-token","side":"BUY","price":"0.40","size":"1"},
                {"asset_id":"a-token","side":"SELL","price":"0.60","size":"1"}
            ]
        });
        let events = normalize_frame(&config(), raw, at(), 9).unwrap();
        assert_eq!(
            events.iter().map(|event| event.cursor).collect::<Vec<_>>(),
            [9, 10]
        );
        let token_ids = events
            .iter()
            .map(|event| match &event.kind {
                EventKind::AtomicDelta { token_id, .. } => token_id.as_str(),
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        assert_eq!(token_ids, ["a-token", "z-token"]);
        assert!(Arc::ptr_eq(&events[0].raw_payload, &events[1].raw_payload));
    }

    #[test]
    fn invalid_or_unsupported_frames_fail_before_cursor_assignment() {
        assert_eq!(
            normalize_frame(
                &config(),
                serde_json::json!({"event_type":"price_change","price_changes":[]}),
                at(),
                1,
            ),
            Err(NormalizeError::InvalidLevels)
        );
        assert_eq!(
            normalize_frame(
                &config(),
                serde_json::json!({"event_type":"new_market"}),
                at(),
                1,
            ),
            Err(NormalizeError::MissingField("condition_id"))
        );
    }

    #[test]
    fn connection_gap_is_a_durable_fail_closed_event_until_full_book() {
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        let initial = snapshot(1);
        let initial_event_id = initial.event_id.clone();
        let initial_raw_sha256 = initial.source.raw_sha256.clone();
        let recovery_payload = initial.raw_payload.as_ref().clone();
        writer.apply(initial).unwrap();
        let gap = normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"source_gap", "asset_id":"yes-1",
                "reason":"upstream_connection_boundary", "timestamp":"1785729603000"
            }),
            at(),
            2,
        )
        .unwrap()
        .remove(0);
        let outcome = writer.apply(gap).unwrap();
        assert!(!outcome.persisted.applied);
        assert!(!outcome.projection.ready);
        assert!(outcome.projection.unresolved_gaps.contains("yes-1"));
        let recovered = normalize_frame(
            &config(),
            recovery_payload,
            at() + chrono::Duration::milliseconds(1),
            3,
        )
        .unwrap()
        .remove(0);
        assert_eq!(recovered.event_id, initial_event_id);
        let mut recovered = recovered;
        assert!(bind_full_book_recovery_identity(&mut recovered));
        assert_ne!(recovered.event_id, initial_event_id);
        assert_eq!(recovered.source.raw_sha256, initial_raw_sha256);
        assert!(writer.apply(recovered).unwrap().projection.ready);
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)]
    async fn rust_transport_subscribes_to_official_wire_shape_and_forwards_raw_book() {
        use tokio::net::TcpListener;
        use tokio_tungstenite::{
            accept_hdr_async,
            tungstenite::handshake::server::{Request, Response},
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_hdr_async(stream, |request: &Request, response: Response| {
                assert_eq!(request.uri().path(), "/ws/market");
                Ok(response)
            })
            .await
            .unwrap();
            let Message::Text(subscription) = socket.next().await.unwrap().unwrap() else {
                panic!("expected text subscription")
            };
            let subscription: Value = serde_json::from_str(&subscription).unwrap();
            assert_eq!(subscription["type"], "market");
            assert_eq!(subscription["assets_ids"], serde_json::json!(["yes-1"]));
            socket
                .send(Message::Text(
                    serde_json::json!([{
                        "event_type":"book", "asset_id":"yes-1", "timestamp":"1785729603000",
                        "tick_size":"0.01", "bids":[{"price":"0.40","size":"10"}],
                        "asks":[{"price":"0.42","size":"11"}]
                    }])
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let endpoint = Url::parse(&format!("ws://{address}/ws/market")).unwrap();
        let (sender, mut receiver) = mpsc::channel(4);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let transport = tokio::spawn(run_polymarket_transport(
            PolymarketTransportConfig::loopback(endpoint),
            vec!["yes-1".into()],
            sender,
            shutdown_rx,
        ));
        let boundary = receiver.recv().await.unwrap();
        assert_eq!(boundary.len(), 1);
        assert_eq!(boundary[0].raw_payload["event_type"], "source_gap");
        let books = receiver.recv().await.unwrap();
        assert_eq!(books.len(), 1);
        assert_eq!(books[0].raw_payload["event_type"], "book");
        shutdown_tx.send(true).unwrap();
        assert_eq!(transport.await.unwrap(), Ok(()));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bounded_transport_waits_for_capacity_and_shutdown_without_dropping() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender.send(Vec::new()).await.unwrap();
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let pending_sender = sender.clone();
        let pending = tokio::spawn(async move {
            send_bounded(
                &pending_sender,
                vec![RawTransportFrame {
                    raw_payload: serde_json::json!({"event_type":"test"}),
                    received_at: at(),
                }],
                &mut shutdown_rx,
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!pending.is_finished());
        receiver.recv().await.unwrap();
        assert_eq!(pending.await.unwrap(), Ok(true));
        assert_eq!(receiver.recv().await.unwrap().len(), 1);

        sender.send(Vec::new()).await.unwrap();
        let (shutdown_tx2, mut shutdown_rx2) = watch::channel(false);
        let pending_sender = sender.clone();
        let pending = tokio::spawn(async move {
            send_bounded(&pending_sender, Vec::new(), &mut shutdown_rx2).await
        });
        tokio::task::yield_now().await;
        shutdown_tx2.send(true).unwrap();
        assert_eq!(pending.await.unwrap(), Ok(false));
    }

    #[test]
    fn catalog_and_lifecycle_frames_are_typed_before_cursor_commit() {
        let catalog = normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"catalog_revision",
                "catalog_revision":"catalog-1",
                "markets":[{
                    "market_id":"m1", "condition_id":"condition-1",
                    "outcomes":[
                        {"token_id":"yes-1","outcome":"Yes","instrument_id":"POLY.M1.YES"},
                        {"token_id":"no-1","outcome":"No","instrument_id":"POLY.M1.NO"}
                    ],
                    "negative_risk_group":null, "lifecycle_state":"active",
                    "resolution":null, "metadata_revision":"metadata-1",
                    "observed_at":"2026-08-03T04:00:00Z", "terminal_at":null
                }],
                "negative_risk_relations":[]
            }),
            at(),
            1,
        )
        .unwrap()
        .remove(0);
        assert!(matches!(
            catalog.kind,
            EventKind::CatalogSnapshot { ref catalog_revision, .. }
                if catalog_revision == "catalog-1"
        ));

        let new_market = normalize_frame(
            &config(),
            serde_json::json!({"event_type":"new_market","market":"condition-2"}),
            at(),
            2,
        )
        .unwrap()
        .remove(0);
        assert!(matches!(
            new_market.kind,
            EventKind::NewMarket { ref condition_id } if condition_id == "condition-2"
        ));

        let resolved = normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"market_resolved", "market":"condition-1",
                "winning_asset_id":"yes-1", "winning_outcome":"Yes"
            }),
            at(),
            3,
        )
        .unwrap()
        .remove(0);
        assert!(matches!(
            resolved.kind,
            EventKind::MarketResolved {
                ref condition_id,
                winning_token_id: Some(ref token),
                winning_outcome: Some(ref outcome),
            } if condition_id == "condition-1" && token == "yes-1" && outcome == "Yes"
        ));
    }

    #[test]
    fn quote_trade_and_tick_events_preserve_exact_semantics() {
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        writer.apply(snapshot(1)).unwrap();

        let quote = normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"best_bid_ask", "asset_id":"yes-1",
                "timestamp":"2026-08-03T04:00:01Z", "best_bid":"0.40", "best_ask":"0.42"
            }),
            at(),
            2,
        )
        .unwrap()
        .remove(0);
        assert!(writer.apply(quote).unwrap().persisted.applied);

        let trade = normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"last_trade_price", "asset_id":"yes-1",
                "timestamp":"2026-08-03T04:00:02Z", "price":"0.41"
            }),
            at(),
            3,
        )
        .unwrap()
        .remove(0);
        assert!(writer.apply(trade).unwrap().persisted.applied);
        assert_eq!(
            writer.projection().books["yes-1"]
                .last_trade_price
                .as_ref()
                .unwrap()
                .0
                .to_string(),
            "0.41"
        );

        let tick = normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"tick_size_change", "asset_id":"yes-1",
                "timestamp":"2026-08-03T04:00:03Z", "old_tick_size":"0.01",
                "new_tick_size":"0.01"
            }),
            at(),
            4,
        )
        .unwrap()
        .remove(0);
        let EventKind::TickSizeChange { tick_version, .. } = &tick.kind else {
            panic!("expected tick change")
        };
        assert_eq!(tick_version.len(), 64);
        assert!(writer.apply(tick).unwrap().persisted.applied);
    }

    #[test]
    fn source_quote_or_tick_mismatch_opens_durable_gap() {
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        writer.apply(snapshot(1)).unwrap();
        let mismatch = normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"best_bid_ask", "asset_id":"yes-1",
                "best_bid":"0.39", "best_ask":"0.42"
            }),
            at(),
            2,
        )
        .unwrap()
        .remove(0);
        let outcome = writer.apply(mismatch).unwrap();
        assert!(!outcome.persisted.applied);
        assert_eq!(
            outcome.persisted.fail_closed_reason.as_deref(),
            Some("best_bid_ask_source_mismatch")
        );
        assert!(outcome.projection.unresolved_gaps.contains("yes-1"));

        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        writer.apply(snapshot(1)).unwrap();
        let tick_mismatch = normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"tick_size_change", "asset_id":"yes-1",
                "old_tick_size":"0.001", "new_tick_size":"0.01"
            }),
            at(),
            2,
        )
        .unwrap()
        .remove(0);
        let outcome = writer.apply(tick_mismatch).unwrap();
        assert!(!outcome.persisted.applied);
        assert_eq!(
            outcome.persisted.fail_closed_reason.as_deref(),
            Some("tick_size_source_mismatch")
        );
        assert_eq!(outcome.projection.persisted_cursor, 2);
    }

    #[test]
    fn raw_duplicate_has_stable_identity_independent_of_reserved_cursor() {
        let one = snapshot(1);
        let two = snapshot(2);
        assert_eq!(one.event_id, two.event_id);
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        writer.apply(one).unwrap();
        assert!(matches!(
            writer.apply(two),
            Err(marketcow_core::CoreError::DuplicateEvent(_))
        ));
        assert_eq!(writer.projection().cursor, 1);
    }

    #[test]
    fn delayed_incremental_source_is_durably_rejected_and_opens_recovery_gap() {
        let mut strict = config();
        strict.maximum_source_delay_ms = 1_000;
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        writer.apply(snapshot(1)).unwrap();
        let normalized = normalize_frame(
            &strict,
            serde_json::json!({
                "event_type":"last_trade_price", "asset_id":"yes-1",
                "timestamp":"2026-08-03T04:00:00Z", "price":"0.41"
            }),
            at(),
            2,
        )
        .unwrap()
        .remove(0);
        assert!(normalized.source.delayed);
        let outcome = writer.apply(normalized).unwrap();
        assert!(!outcome.persisted.applied);
        assert_eq!(
            outcome.persisted.fail_closed_reason.as_deref(),
            Some("source_data_delayed")
        );
        assert_eq!(outcome.projection.cursor, 2);
        assert_eq!(outcome.projection.persisted_cursor, 2);
        assert!(!outcome.projection.ready);
        assert!(outcome.projection.unresolved_gaps.contains("yes-1"));
    }

    #[test]
    fn quiet_market_full_book_freshness_is_anchored_to_receipt() {
        let mut strict = config();
        strict.maximum_source_delay_ms = 1_000;
        let normalized = normalize_frame(
            &strict,
            serde_json::json!({
                "event_type":"book", "asset_id":"yes-1", "timestamp":"2026-08-03T03:00:00Z",
                "tick_size":"0.01", "bids":[{"price":"0.40","size":"10"}],
                "asks":[{"price":"0.42","size":"11"}]
            }),
            at(),
            1,
        )
        .unwrap()
        .remove(0);
        assert!(!normalized.source.delayed);
        assert_eq!(normalized.source_observed_at, at());
        assert_eq!(normalized.raw_payload["timestamp"], "2026-08-03T03:00:00Z");
    }

    #[test]
    fn zero_tick_and_off_grid_price_are_rejected() {
        let zero_tick = serde_json::json!({
            "event_type":"book", "asset_id":"yes-1", "tick_size":"0",
            "bids":[], "asks":[]
        });
        assert_eq!(
            normalize_frame(&config(), zero_tick, at(), 1),
            Err(NormalizeError::InvalidDecimal)
        );

        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        let off_grid = normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"book", "asset_id":"yes-1", "tick_size":"0.01",
                "bids":[{"price":"0.405","size":"1"}],
                "asks":[{"price":"0.60","size":"1"}]
            }),
            at(),
            1,
        )
        .unwrap()
        .remove(0);
        let outcome = writer.apply(off_grid).unwrap();
        assert!(!outcome.persisted.applied);
        assert_eq!(
            outcome.persisted.fail_closed_reason.as_deref(),
            Some("invalid_tick_full_book")
        );
    }
}
