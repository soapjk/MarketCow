//! Strict Polymarket CLOB raw-frame normalizer. Transport and cursor ownership stay outside this
//! crate; a frame is normalized without network, storage, or async-runtime dependencies.

pub mod discovery_source;
pub mod diagnostic;
pub mod subscription;
mod transport_diagnostic;

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
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
};
use tokio_tungstenite::tungstenite::Message;
use url::Url;

pub const NORMALIZER_VERSION: &str = "marketcow.polymarket.normalizer.v2";
pub const CLOB_MARKET_STREAM_SOURCE_URL: &str =
    "https://ws-subscriptions-clob.polymarket.com/ws/market";
pub const CLOB_BOOK_SNAPSHOT_SOURCE_URL: &str = "https://clob.polymarket.com/books";

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

/// A socket-reader queue item holds an exact byte permit until the downstream
/// owner removes the item from the transport queue. This prevents a burst of
/// many small WS messages from being constrained by an arbitrary record count
/// while retaining a hard memory ceiling for maximum-sized messages.
pub struct RawTransportBatch {
    frames: Vec<RawTransportFrame>,
    source_tokens: BTreeSet<String>,
    queued_at: std::time::Instant,
    _bytes: OwnedSemaphorePermit,
    _shared_bytes: Option<OwnedSemaphorePermit>,
    _shared_slot: Option<OwnedSemaphorePermit>,
    declared_bytes: usize,
    metrics: Arc<TransportQueueMetrics>,
}

impl RawTransportBatch {
    pub fn source_tokens(&self) -> &BTreeSet<String> {
        &self.source_tokens
    }
    pub fn declared_bytes(&self) -> usize {
        self.declared_bytes
    }

    /// Time in this market's transport FIFO, excluding upstream socket read
    /// and actor admission. Read before moving the batch into its frames.
    pub fn queued_for(&self) -> Duration {
        self.queued_at.elapsed()
    }

    pub fn into_frames(mut self) -> Vec<RawTransportFrame> {
        std::mem::take(&mut self.frames)
    }
}

impl Drop for RawTransportBatch {
    fn drop(&mut self) {
        self.metrics
            .current_bytes
            .fetch_sub(self.declared_bytes, Ordering::Relaxed);
        self.metrics.current_batches.fetch_sub(1, Ordering::Relaxed);
    }
}

impl std::ops::Deref for RawTransportBatch {
    type Target = [RawTransportFrame];
    fn deref(&self) -> &Self::Target {
        &self.frames
    }
}

impl IntoIterator for RawTransportBatch {
    type Item = RawTransportFrame;
    type IntoIter = std::vec::IntoIter<RawTransportFrame>;
    fn into_iter(mut self) -> Self::IntoIter {
        std::mem::take(&mut self.frames).into_iter()
    }
}

#[derive(Default)]
struct TransportQueueMetrics {
    current_bytes: AtomicUsize,
    current_batches: AtomicUsize,
    high_bytes: AtomicUsize,
    high_batches: AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportQueueOccupancy {
    pub current_bytes: usize,
    pub current_batches: usize,
    pub high_bytes: usize,
    pub high_batches: usize,
}

#[derive(Clone)]
pub struct TransportOutput {
    sender: mpsc::Sender<RawTransportBatch>,
    bytes: Arc<Semaphore>,
    shared_bytes: Option<Arc<Semaphore>>,
    shared_slots: Option<Arc<Semaphore>>,
    ready: Option<TransportReadyHandle>,
    maximum_batch_bytes: usize,
    metrics: Arc<TransportQueueMetrics>,
}

/// At most one queued notification per live market FIFO. The receiver is also
/// explicitly bounded by the socket budget; failure is never a silent miss.
#[derive(Clone)]
pub struct TransportReadyHandle {
    id: u64,
    queued: Arc<std::sync::atomic::AtomicBool>,
    sender: mpsc::Sender<u64>,
}

impl TransportReadyHandle {
    pub fn signal(&self) -> Result<(), TransportError> {
        if !self.queued.swap(true, Ordering::AcqRel) {
            if self.sender.try_send(self.id).is_err() {
                self.queued.store(false, Ordering::Release);
                return Err(TransportError::Backpressure);
            }
        }
        Ok(())
    }

    pub fn consumed(&self) {
        self.queued.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct TransportBudget {
    bytes: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    maximum_batch_bytes: usize,
    metrics: Arc<TransportQueueMetrics>,
}

impl TransportBudget {
    /// A market has its own bounded FIFO and byte ceiling. Its permits also
    /// count against the collector-wide ceiling, so adding sockets cannot
    /// multiply the maximum retained transport payload.
    pub fn market_lane(
        &self,
        id: u64,
        ready_sender: mpsc::Sender<u64>,
        maximum_batches: usize,
        maximum_bytes: usize,
    ) -> Result<(TransportOutput, mpsc::Receiver<RawTransportBatch>, TransportReadyHandle), TransportError> {
        let (mut lane, receiver) = transport_output(
            maximum_batches,
            maximum_bytes,
            self.maximum_batch_bytes.min(maximum_bytes),
        )?;
        lane.shared_bytes = Some(self.bytes.clone());
        lane.shared_slots = Some(self.slots.clone());
        lane.metrics = self.metrics.clone();
        let ready = TransportReadyHandle {
            id, queued: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            sender: ready_sender,
        };
        lane.ready = Some(ready.clone());
        Ok((lane, receiver, ready))
    }
    pub fn take_occupancy(&self) -> TransportQueueOccupancy {
        take_transport_occupancy(&self.metrics)
    }
}

impl TransportOutput {
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    pub fn take_occupancy(&self) -> TransportQueueOccupancy {
        take_transport_occupancy(&self.metrics)
    }
}

fn take_transport_occupancy(metrics: &TransportQueueMetrics) -> TransportQueueOccupancy {
    let current_bytes = metrics.current_bytes.load(Ordering::Relaxed);
    let current_batches = metrics.current_batches.load(Ordering::Relaxed);
    let high_bytes = metrics.high_bytes.swap(current_bytes, Ordering::Relaxed);
    let high_batches = metrics.high_batches.swap(current_batches, Ordering::Relaxed);
    TransportQueueOccupancy {
        current_bytes,
        current_batches,
        high_bytes: high_bytes.max(current_bytes),
        high_batches: high_batches.max(current_batches),
    }
}

pub fn transport_budget(
    maximum_batches: usize,
    maximum_bytes: usize,
    maximum_batch_bytes: usize,
) -> Result<TransportBudget, TransportError> {
    validate_transport_limits(maximum_batches, maximum_bytes, maximum_batch_bytes)?;
    Ok(TransportBudget {
        bytes: Arc::new(Semaphore::new(maximum_bytes)),
        slots: Arc::new(Semaphore::new(maximum_batches)),
        maximum_batch_bytes,
        metrics: Arc::new(TransportQueueMetrics::default()),
    })
}

pub fn transport_output(
    maximum_batches: usize,
    maximum_bytes: usize,
    maximum_batch_bytes: usize,
) -> Result<(TransportOutput, mpsc::Receiver<RawTransportBatch>), TransportError> {
    validate_transport_limits(maximum_batches, maximum_bytes, maximum_batch_bytes)?;
    let (sender, receiver) = mpsc::channel(maximum_batches);
    let metrics = Arc::new(TransportQueueMetrics::default());
    Ok((
        TransportOutput {
            sender,
            bytes: Arc::new(Semaphore::new(maximum_bytes)),
            shared_bytes: None,
            shared_slots: None,
            ready: None,
            maximum_batch_bytes,
            metrics,
        },
        receiver,
    ))
}

fn validate_transport_limits(
    maximum_batches: usize,
    maximum_bytes: usize,
    maximum_batch_bytes: usize,
) -> Result<(), TransportError> {
    if maximum_batches == 0
        || maximum_bytes == 0
        || maximum_bytes > u32::MAX as usize
        || maximum_batch_bytes == 0
        || maximum_batch_bytes > maximum_bytes
    {
        return Err(TransportError::InvalidConfig);
    }
    Ok(())
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
    #[error("Polymarket subscription membership changed")]
    SubscriptionConflict,
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
            Self::SubscriptionConflict => "subscription_conflict",
        }
    }
}

pub async fn run_polymarket_transport(
    config: PolymarketTransportConfig,
    token_ids: Vec<String>,
    output: TransportOutput,
    shutdown: watch::Receiver<bool>,
) -> Result<(), TransportError> {
    run_transport(config, token_ids, output, shutdown, None).await
}

/// The owner admits metadata and installs new reducers before submitting an
/// update. Removing the final shard is done with its shutdown handle, not an
/// empty subscription. No consumer or venue message can call this control API.
pub async fn run_polymarket_transport_managed(
    config: PolymarketTransportConfig,
    token_ids: Vec<String>,
    output: TransportOutput,
    shutdown: watch::Receiver<bool>,
    updates: mpsc::Receiver<subscription::SubscriptionUpdate>,
) -> Result<(), TransportError> {
    run_transport(config, token_ids, output, shutdown, Some(updates)).await
}

async fn run_transport(
    config: PolymarketTransportConfig,
    token_ids: Vec<String>,
    output: TransportOutput,
    shutdown: watch::Receiver<bool>,
    updates: Option<mpsc::Receiver<subscription::SubscriptionUpdate>>,
) -> Result<(), TransportError> {
    let mut shutdown = shutdown;
    let mut updates = updates;
    config.validate()?;
    let count = token_ids.len();
    let mut tokens = token_ids.into_iter().collect::<BTreeSet<_>>();
    if tokens.len() != count {
        return Err(TransportError::InvalidConfig);
    }
    subscription::validate_tokens(&tokens)?;
    run_transport_attempts(&config, &mut tokens, &output, &mut shutdown, &mut updates).await
}

/// Service-owned shard supervision. Each retry group still has the configured
/// attempt/deadline bounds; a failed group cools down without killing other
/// shards. Current membership and the bounded control receiver survive it.
pub async fn supervise_polymarket_transport_managed(
    config: PolymarketTransportConfig,
    token_ids: Vec<String>,
    output: TransportOutput,
    mut shutdown: watch::Receiver<bool>,
    updates: mpsc::Receiver<subscription::SubscriptionUpdate>,
    cooldown: Duration,
) -> Result<(), TransportError> {
    config.validate()?;
    if cooldown.is_zero() || cooldown > Duration::from_secs(60) {
        return Err(TransportError::InvalidConfig);
    }
    let count = token_ids.len();
    let mut tokens: BTreeSet<_> = token_ids.into_iter().collect();
    if tokens.len() != count {
        return Err(TransportError::InvalidConfig);
    }
    subscription::validate_tokens(&tokens)?;
    let mut updates = Some(updates);
    loop {
        let result =
            run_transport_attempts(&config, &mut tokens, &output, &mut shutdown, &mut updates)
                .await;
        if *shutdown.borrow() || output.is_closed() {
            return Ok(());
        }
        // Output capacity errors remain a real shared-consumer failure, not an
        // excuse to drop accepted input or spin a reconnect loop.
        if matches!(result, Err(TransportError::Backpressure)) {
            return result;
        }
        crate::async_eprintln!("{}", serde_json::json!({
            "stage":"polymarket_shard_retry_group","token_count":tokens.len(),
            "result":format!("{result:?}"),
        }));
        tokio::select! {
            _=tokio::time::sleep(cooldown)=>{},
            _=shutdown.changed()=>return Ok(()),
        }
    }
}

async fn run_transport_attempts(
    config: &PolymarketTransportConfig,
    tokens: &mut BTreeSet<String>,
    output: &TransportOutput,
    shutdown: &mut watch::Receiver<bool>,
    updates: &mut Option<mpsc::Receiver<subscription::SubscriptionUpdate>>,
) -> Result<(), TransportError> {
    for attempt in 1..=config.maximum_reconnect_attempts {
        if *shutdown.borrow() {
            return Ok(());
        }
        match run_polymarket_connection(config, tokens, output, shutdown, updates).await {
            Ok(ConnectionEnd::Shutdown) => return Ok(()),
            Ok(ConnectionEnd::Disconnected {
                close_code,
                close_reason,
            }) => {
                if !publish_connection_gaps(
                    tokens,
                    attempt,
                    "upstream_close",
                    close_code,
                    close_reason.as_deref(),
                    output,
                    shutdown,
                )
                .await?
                {
                    return Ok(());
                }
                crate::async_eprintln!("{}", serde_json::json!({
                    "stage":"polymarket_transport_retry","attempt":attempt,
                    "close_code":close_code,"close_reason":close_reason,
                    "reason":"upstream_disconnected",
                }));
            }
            Err(TransportError::Backpressure) => return Err(TransportError::Backpressure),
            Err(error) if attempt == config.maximum_reconnect_attempts => {
                if !publish_connection_gaps(
                    tokens,
                    attempt,
                    error.reason_code(),
                    None,
                    None,
                    output,
                    shutdown,
                )
                .await?
                {
                    return Ok(());
                }
                crate::async_eprintln!("{}", serde_json::json!({
                    "stage":"polymarket_transport_exhausted","attempt":attempt,
                    "reason":error.reason_code(),
                }));
                return Err(error);
            }
            Err(error) => {
                if !publish_connection_gaps(
                    tokens,
                    attempt,
                    error.reason_code(),
                    None,
                    None,
                    output,
                    shutdown,
                )
                .await?
                {
                    return Ok(());
                }
                crate::async_eprintln!("{}", serde_json::json!({
                    "stage":"polymarket_transport_retry","attempt":attempt,
                    "reason":error.reason_code(),
                }));
            }
        }
        if attempt == config.maximum_reconnect_attempts {
            return Err(TransportError::ReconnectExhausted);
        }
        tokio::select! {
            _ = tokio::time::sleep(reconnect_backoff(config.reconnect_delay, attempt)) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
        }
    }
    Err(TransportError::ReconnectExhausted)
}

fn reconnect_backoff(base: Duration, attempt: u32) -> Duration {
    let multiplier = 1_u32 << attempt.saturating_sub(1).min(6);
    base.saturating_mul(multiplier).min(Duration::from_secs(60))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConnectionEnd {
    Shutdown,
    Disconnected {
        close_code: Option<u16>,
        close_reason: Option<String>,
    },
}

async fn publish_connection_gaps(
    tokens: &BTreeSet<String>,
    attempt: u32,
    transport_reason: &str,
    close_code: Option<u16>,
    close_reason: Option<&str>,
    output: &TransportOutput,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<bool, TransportError> {
    // A remove-first replacement can fail after the old pair was removed but
    // before the new pair was subscribed. There is no owned token to mark as
    // gapped; the control receipt carries the failed replacement instead.
    if tokens.is_empty() {
        return Ok(true);
    }
    let received_at = Utc::now();
    let timestamp = received_at.timestamp_millis().to_string();
    let frames = tokens
        .iter()
        .map(|token_id| RawTransportFrame {
            raw_payload: serde_json::json!({
                "event_type":"source_gap", "asset_id":token_id,
                "reason":"upstream_connection_boundary", "attempt":attempt,
                "transport_reason":transport_reason,
                "close_code":close_code,
                "close_reason":close_reason,
                "timestamp":timestamp,
            }),
            received_at,
        })
        .collect();
    let bytes = serde_json::to_vec(&frames)
        .map_err(|_| TransportError::InvalidFrame)?
        .len();
    send_bounded(output, frames, bytes, tokens, shutdown, None).await
}

async fn send_bounded(
    output: &TransportOutput,
    frames: Vec<RawTransportFrame>,
    bytes: usize,
    source_tokens: &BTreeSet<String>,
    shutdown: &mut watch::Receiver<bool>,
    mut timing: Option<&mut transport_diagnostic::SendTiming>,
) -> Result<bool, TransportError> {
    if bytes == 0 || bytes > output.maximum_batch_bytes || bytes > u32::MAX as usize {
        return Err(TransportError::FrameTooLarge);
    }
    let wait_started = std::time::Instant::now();
    let shared_slot_permit = if let Some(shared) = &output.shared_slots {
        Some(tokio::select! {
            permit = shared.clone().acquire_owned() =>
                permit.map_err(|_| TransportError::Backpressure)?,
            changed = shutdown.changed() => {
                return if changed.is_err() || *shutdown.borrow() {
                    Ok(false)
                } else {
                    Err(TransportError::Backpressure)
                };
            }
        })
    } else {
        None
    };
    if let Some(timing) = timing.as_deref_mut() {
        timing.shared_slot_wait = wait_started.elapsed();
    }
    let wait_started = std::time::Instant::now();
    let shared_byte_permit = if let Some(shared) = &output.shared_bytes {
        Some(tokio::select! {
            permit = shared.clone().acquire_many_owned(bytes as u32) =>
                permit.map_err(|_| TransportError::Backpressure)?,
            changed = shutdown.changed() => {
                return if changed.is_err() || *shutdown.borrow() {
                    Ok(false)
                } else {
                    Err(TransportError::Backpressure)
                };
            }
        })
    } else {
        None
    };
    if let Some(timing) = timing.as_deref_mut() {
        timing.shared_byte_wait = wait_started.elapsed();
    }
    let wait_started = std::time::Instant::now();
    let byte_permit = tokio::select! {
        permit = output.bytes.clone().acquire_many_owned(bytes as u32) =>
            permit.map_err(|_| TransportError::Backpressure)?,
        changed = shutdown.changed() => {
            return if changed.is_err() || *shutdown.borrow() {
                Ok(false)
            } else {
                Err(TransportError::Backpressure)
            };
        }
    };
    if let Some(timing) = timing.as_deref_mut() {
        timing.lane_byte_wait = wait_started.elapsed();
    }
    let wait_started = std::time::Instant::now();
    tokio::select! {
        permit = output.sender.reserve() => {
            let permit = permit.map_err(|_| TransportError::Backpressure)?;
            if let Some(timing) = timing.as_deref_mut() {
                timing.queue_slot_wait = wait_started.elapsed();
            }
            let current_bytes = output
                .metrics
                .current_bytes
                .fetch_add(bytes, Ordering::Relaxed)
                + bytes;
            let current_batches = output
                .metrics
                .current_batches
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            output
                .metrics
                .high_bytes
                .fetch_max(current_bytes, Ordering::Relaxed);
            output
                .metrics
                .high_batches
                .fetch_max(current_batches, Ordering::Relaxed);
            permit.send(RawTransportBatch {
                frames,
                source_tokens: source_tokens.clone(),
                queued_at: std::time::Instant::now(),
                _bytes: byte_permit,
                _shared_bytes: shared_byte_permit,
                _shared_slot: shared_slot_permit,
                declared_bytes: bytes,
                metrics: output.metrics.clone(),
            });
            if let Some(ready) = &output.ready { ready.signal()?; }
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
    tokens: &mut BTreeSet<String>,
    output: &TransportOutput,
    shutdown: &mut watch::Receiver<bool>,
    updates: &mut Option<mpsc::Receiver<subscription::SubscriptionUpdate>>,
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
    // A busy market must not postpone application heartbeats forever.  The
    // previous per-iteration sleep was recreated after every inbound quote,
    // so liquid streams never emitted PING and were eventually disconnected
    // by the upstream gateway.  Keep one independent periodic schedule whose
    // deadline is unaffected by market traffic.
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + config.heartbeat_interval,
        config.heartbeat_interval,
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut transport_diagnostic = transport_diagnostic::TransportDiagnostic::new(tokens);
    loop {
        let socket_poll_started = std::time::Instant::now();
        tokio::select! {
            update = async { match updates.as_mut() {
                Some(receiver) => receiver.recv().await,
                None => std::future::pending().await,
            }} => {
                let Some(update)=update else { *updates=None; continue; };
                let (added,removed)=match subscription::diff(tokens,&update) {
                    Ok(diff)=>diff,
                    Err(error)=>{let _=update.receipt.send(Err(error));continue;}
                };
                let operations = if update.remove_first {
                    [("unsubscribe", &removed), ("subscribe", &added)]
                } else {
                    [("subscribe", &added), ("unsubscribe", &removed)]
                };
                for (operation,ids) in operations {
                    if ids.is_empty() {continue;}
                    let message=Message::Text(serde_json::json!({"operation":operation,"assets_ids":ids}).to_string().into());
                    let sent=tokio::select! {
                        result=tokio::time::timeout(config.connect_timeout,socket.send(message)) => matches!(result,Ok(Ok(()))),
                        _=shutdown.changed()=>false,
                    };
                    if !sent {
                        // A timed-out write has uncertain delivery. Include all
                        // possibly subscribed tokens in recovery on reconnect.
                        tokens.extend(added.iter().cloned());
                        let _=update.receipt.send(Err(TransportError::SendFailed));
                        return Err(TransportError::SendFailed);
                    }
                    if operation=="subscribe" {tokens.extend(ids.iter().cloned());}
                    else {for id in ids {tokens.remove(id);}}
                }
                transport_diagnostic.set_tokens(tokens);
                let _=update.receipt.send(Ok(()));
            }
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
                    let wire_bytes = text.len();
                    let received_at = Utc::now();
                    let socket_next_poll = socket_poll_started.elapsed();
                    let decode_started = std::time::Instant::now();
                    let payload: Value = serde_json::from_str(&text)
                        .map_err(|_| TransportError::InvalidFrame)?;
                    let values = match payload {
                        Value::Array(values) => values,
                        value @ Value::Object(_) => vec![value],
                        _ => return Err(TransportError::InvalidFrame),
                    };
                    // The venue legitimately returns an empty snapshot array
                    // when none of the requested tokens currently has a book.
                    // Keep those identities waiting for a later book or the
                    // bounded recovery path; an empty batch is not transport
                    // corruption and must not invalidate the whole shard.
                    if values.is_empty() {
                        continue;
                    }
                    if values.iter().any(|value| !value.is_object()) {
                        return Err(TransportError::InvalidFrame);
                    }
                    let frames:Vec<_> = values.into_iter().filter(|value|subscription::owned_or_unclassified(value,tokens)).map(|raw_payload| RawTransportFrame {
                        raw_payload, received_at,
                    }).collect();
                    if frames.is_empty() {continue;}
                    transport_diagnostic.record_batch(&frames, wire_bytes,
                        socket_next_poll, decode_started.elapsed());
                    let mut send_timing = transport_diagnostic::SendTiming::default();
                    if !send_bounded(output, frames, wire_bytes, tokens, shutdown,
                        Some(&mut send_timing)).await? {
                        let _ = socket.close(None).await;
                        return Ok(ConnectionEnd::Shutdown);
                    }
                    transport_diagnostic.record_send(&send_timing);
                    transport_diagnostic.flush_if_due();
                }
                Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await
                    .map_err(|_| TransportError::SendFailed)?,
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(frame))) => return Ok(ConnectionEnd::Disconnected {
                    close_code: frame.as_ref().map(|frame| u16::from(frame.code)),
                    close_reason: frame.map(|frame| frame.reason.to_string()),
                }),
                None => return Ok(ConnectionEnd::Disconnected {
                    close_code: None,
                    close_reason: Some("stream_ended_without_close_frame".into()),
                }),
                Some(Ok(Message::Binary(_) | Message::Frame(_))) => {
                    let _ = socket.close(None).await;
                    return Err(TransportError::InvalidFrame);
                }
                Some(Err(error)) => {
                    // Keep the public fail-closed reason stable while making
                    // the transport failure class observable.  Never include
                    // request headers, proxy URLs or credentials in this log.
                    crate::async_eprintln!(
                        "{}",
                        serde_json::json!({
                            "stage":"polymarket_websocket_read_failed",
                            "error_class":websocket_read_error_class(&error),
                        })
                    );
                    return Err(TransportError::ReadFailed);
                },
            },
            _ = heartbeat.tick() => {
                transport_diagnostic.flush_if_due();
                socket.send(Message::Text("PING".into())).await
                    .map_err(|_| TransportError::SendFailed)?;
            },
        }
    }
}

fn websocket_read_error_class(
    error: &tokio_tungstenite::tungstenite::Error,
) -> String {
    use tokio_tungstenite::tungstenite::Error;
    match error {
        Error::ConnectionClosed => "connection_closed".into(),
        Error::AlreadyClosed => "already_closed".into(),
        Error::Io(error) => format!("io_{:?}", error.kind()).to_ascii_lowercase(),
        Error::Tls(_) => "tls".into(),
        Error::Capacity(_) => "capacity".into(),
        // ProtocolError is an enum without request data. Its variant is safe
        // and necessary to distinguish a peer omitting the close handshake
        // from an actually malformed WebSocket frame.
        Error::Protocol(error) => {
            format!("protocol_{error:?}").to_ascii_lowercase()
        }
        Error::WriteBufferFull(_) => "write_buffer_full".into(),
        Error::Utf8(_) => "utf8".into(),
        Error::AttackAttempt => "attack_attempt".into(),
        Error::Url(_) => "url".into(),
        Error::Http(_) => "http".into(),
        Error::HttpFormat(_) => "http_format".into(),
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

struct AtomicPriceChanges {
    changes: Vec<SideLevels>,
    bid_positions: BTreeMap<Price, usize>,
    ask_positions: BTreeMap<Price, usize>,
    best_bid: Option<Price>,
    best_ask: Option<Price>,
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
    // Supplier quote and trade time is never replaced by the local socket
    // receipt, including an old full book received during recovery. A missing
    // supplier clock is not a fresh quote. Internal lifecycle boundaries may
    // still use their explicit local boundary clock for ordering only.
    if matches!(event_type.as_str(),
        "book" | "price_change" | "best_bid_ask" | "last_trade_price" | "tick_size_change")
        && raw.get("timestamp").is_none() {
        return Err(NormalizeError::MissingField("timestamp"));
    }
    let observed_at = parse_observed_at(raw.get("timestamp"), received_at)?;
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
            let mut by_token: BTreeMap<String, AtomicPriceChanges> = BTreeMap::new();
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
                let best_bid_present = item.contains_key("best_bid");
                let best_ask_present = item.contains_key("best_ask");
                if best_bid_present != best_ask_present {
                    return Err(NormalizeError::MissingField("best_bid/best_ask"));
                }
                let best_bid = optional_price(item.get("best_bid"))?;
                let best_ask = optional_price(item.get("best_ask"))?;
                if best_bid
                    .as_ref()
                    .zip(best_ask.as_ref())
                    .is_some_and(|(bid, ask)| bid >= ask)
                {
                    return Err(NormalizeError::InvalidLevels);
                }
                let entry = by_token
                    .entry(token_id)
                    .or_insert_with(|| AtomicPriceChanges {
                        changes: Vec::new(),
                        bid_positions: BTreeMap::new(),
                        ask_positions: BTreeMap::new(),
                        best_bid: best_bid.clone(),
                        best_ask: best_ask.clone(),
                    });
                if entry.best_bid != best_bid || entry.best_ask != best_ask {
                    return Err(NormalizeError::InvalidLevels);
                }
                // All levels in one venue frame form one atomic observation.
                // Each size replaces its price level, so only the final write
                // to a token/side/price can affect the post-frame book. Keep
                // the raw frame unchanged for audit and validate every item
                // above before discarding an overwritten assignment.
                let positions = match side {
                    Side::Bid => &mut entry.bid_positions,
                    Side::Ask => &mut entry.ask_positions,
                };
                if let Some(&position) = positions.get(&level.price) {
                    entry.changes[position].levels[0] = level;
                } else {
                    positions.insert(level.price.clone(), entry.changes.len());
                    entry.changes.push(SideLevels {
                        side,
                        levels: vec![level],
                    });
                }
            }
            by_token
                .into_iter()
                .enumerate()
                .map(|(offset, (token_id, atomic))| {
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
                            changes: atomic.changes,
                            best_bid: atomic.best_bid,
                            best_ask: atomic.best_ask,
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

/// Rebinds an authoritative HTTP full-book snapshot to its observation boundary. This only marks
/// normalized evidence as HTTP-derived; callers decide whether it is a historical state event or
/// a read-only validation. The current live runtime never writes periodic observations into the
/// WS-owned projection or public cursor.
pub fn bind_full_book_refresh_identity(event: &mut CanonicalEvent) -> bool {
    if !matches!(&event.kind, EventKind::FullBook { .. }) {
        return false;
    }
    event.event_id = hex::encode(Sha256::digest(format!(
        "{}\0full_book_refresh\0{}",
        event.event_id,
        event
            .received_at
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
    )));
    event.source.source = "polymarket_clob_http".into();
    event.source.source_url = Some(CLOB_BOOK_SNAPSHOT_SOURCE_URL.into());
    event.source.update_frequency = "periodic_authoritative_refresh".into();
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

    #[test]
    fn reconnect_backoff_is_exponential_and_bounded() {
        let base = Duration::from_secs(1);
        assert_eq!(reconnect_backoff(base, 1), Duration::from_secs(1));
        assert_eq!(reconnect_backoff(base, 2), Duration::from_secs(2));
        assert_eq!(reconnect_backoff(base, 6), Duration::from_secs(32));
        assert_eq!(reconnect_backoff(base, 7), Duration::from_secs(60));
        assert_eq!(reconnect_backoff(base, 32), Duration::from_secs(60));
    }

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
    fn repeated_level_assignments_in_one_frame_keep_only_the_last_valid_value() {
        let raw = serde_json::json!({
            "event_type":"price_change", "timestamp":"2026-08-03T04:00:01Z",
            "price_changes":[
                {"asset_id":"yes-1","side":"BUY","price":"0.40","size":"1"},
                {"asset_id":"yes-1","side":"SELL","price":"0.44","size":"2"},
                {"asset_id":"yes-1","side":"BID","price":"0.40","size":"0"},
                {"asset_id":"yes-1","side":"BUY","price":"0.40","size":"3"}
            ]
        });
        let event = normalize_frame(&config(), raw.clone(), at(), 2)
            .unwrap()
            .remove(0);
        assert_eq!(*event.raw_payload, raw);
        let EventKind::AtomicDelta { changes, .. } = event.kind else {
            panic!("expected atomic delta")
        };
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].side, Side::Bid);
        assert_eq!(changes[0].levels[0].quantity.to_string(), "3");
        assert_eq!(changes[1].side, Side::Ask);
        assert_eq!(changes[1].levels[0].quantity.to_string(), "2");

        let invalid = serde_json::json!({
            "event_type":"price_change", "timestamp":"2026-08-03T04:00:01Z",
            "price_changes":[
                {"asset_id":"yes-1","side":"BUY","price":"0.40","size":"-1"},
                {"asset_id":"yes-1","side":"BUY","price":"0.40","size":"3"}
            ]
        });
        assert_eq!(
            normalize_frame(&config(), invalid, at(), 2),
            Err(NormalizeError::InvalidDecimal)
        );
    }

    #[test]
    fn atomic_price_change_uses_venue_bbo_to_remove_stale_crossing_levels() {
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        writer.apply(snapshot(1)).unwrap();
        let raw = serde_json::json!({
            "event_type":"price_change", "timestamp":"2026-08-03T04:00:01Z",
            "price_changes":[
                {"asset_id":"yes-1","side":"SELL","price":"0.39","size":"2",
                 "best_bid":"0.38","best_ask":"0.39"},
                {"asset_id":"yes-1","side":"BUY","price":"0.38","size":"1",
                 "best_bid":"0.38","best_ask":"0.39"}
            ]
        });
        let event = normalize_frame(&config(), raw, at(), 2).unwrap().remove(0);
        let EventKind::AtomicDelta {
            ref best_bid,
            ref best_ask,
            ..
        } = event.kind
        else {
            panic!("expected atomic delta")
        };
        assert_eq!(best_bid.as_ref().unwrap().0.to_string(), "0.38");
        assert_eq!(best_ask.as_ref().unwrap().0.to_string(), "0.39");

        let outcome = writer.apply(event).unwrap();
        assert!(outcome.persisted.applied);
        let book = &outcome.projection.books["yes-1"];
        assert_eq!(book.bids.last_key_value().unwrap().0.0.to_string(), "0.38");
        assert_eq!(book.asks.first_key_value().unwrap().0.0.to_string(), "0.39");
        assert!(!outcome.projection.unresolved_gaps.contains("yes-1"));
    }

    #[test]
    fn public_atomic_delta_materializes_bbo_pruning_for_deterministic_replay() {
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        writer.apply(snapshot(1)).unwrap();
        let preceding = writer.projection().books["yes-1"].clone();
        let raw = serde_json::json!({
            "event_type":"price_change", "timestamp":"2026-08-03T04:00:01Z",
            "price_changes":[
                {"asset_id":"yes-1","side":"BUY","price":"0.43","size":"1",
                 "best_bid":"0.43","best_ask":"0.44"},
                {"asset_id":"yes-1","side":"SELL","price":"0.44","size":"2",
                 "best_bid":"0.43","best_ask":"0.44"}
            ]
        });
        let event = normalize_frame(&config(), raw, at(), 2).unwrap().remove(0);

        let outcome = writer.apply(event).unwrap();
        assert!(outcome.persisted.applied);
        let EventKind::AtomicDelta { changes, .. } = &outcome.persisted.event.kind else {
            panic!("expected atomic delta")
        };
        assert!(changes.iter().any(|change| {
            change.side == Side::Ask
                && change
                    .levels
                    .iter()
                    .any(|level| level.price.0.to_string() == "0.42" && level.quantity.is_zero())
        }));

        // A consumer starting from the preceding full-sync and applying only the public canonical
        // changes must derive the exact same book as the authoritative server projection.
        let mut replay_bids = preceding.bids;
        let mut replay_asks = preceding.asks;
        for change in changes {
            let target = match change.side {
                Side::Bid => &mut replay_bids,
                Side::Ask => &mut replay_asks,
            };
            for level in &change.levels {
                if level.quantity.is_zero() {
                    target.remove(&level.price);
                } else {
                    target.insert(level.price.clone(), level.quantity);
                }
            }
        }
        let projected = &outcome.projection.books["yes-1"];
        assert_eq!(replay_bids, projected.bids);
        assert_eq!(replay_asks, projected.asks);
        assert!(replay_bids.last_key_value().unwrap().0 < replay_asks.first_key_value().unwrap().0);
    }

    #[test]
    fn atomic_price_change_treats_zero_and_one_bbo_as_empty_side_sentinels() {
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        writer.apply(snapshot(1)).unwrap();
        let event = normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"price_change", "timestamp":"2026-08-03T04:00:01Z",
                "price_changes":[
                    {"asset_id":"yes-1","side":"BUY","price":"0.40","size":"0",
                     "best_bid":"0","best_ask":"1"},
                    {"asset_id":"yes-1","side":"SELL","price":"0.42","size":"0",
                     "best_bid":"0","best_ask":"1"}
                ]
            }),
            at(),
            2,
        )
        .unwrap()
        .remove(0);
        let EventKind::AtomicDelta {
            ref best_bid,
            ref best_ask,
            ..
        } = event.kind
        else {
            panic!("expected atomic delta")
        };
        assert!(best_bid.as_ref().unwrap().is_zero());
        assert!(best_ask.as_ref().unwrap().is_one());

        let outcome = writer.apply(event).unwrap();
        assert!(outcome.persisted.applied);
        assert!(outcome.projection.books["yes-1"].bids.is_empty());
        assert!(outcome.projection.books["yes-1"].asks.is_empty());
        assert!(outcome.projection.ready);
    }

    #[test]
    fn legacy_atomic_delta_without_bbo_remains_replay_compatible() {
        let kind: EventKind = serde_json::from_value(serde_json::json!({
            "event_type":"atomic_delta",
            "token_id":"yes-1",
            "changes":[{"side":"bid","levels":[{"price":"0.40","quantity":"1"}]}]
        }))
        .unwrap();
        assert!(matches!(
            kind,
            EventKind::AtomicDelta {
                best_bid: None,
                best_ask: None,
                ..
            }
        ));
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
            socket.send(Message::Text("[]".into())).await.unwrap();
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
        let (sender, mut receiver) = transport_output(4, 65_536, 65_536).unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let transport = tokio::spawn(run_polymarket_transport(
            PolymarketTransportConfig::loopback(endpoint),
            vec!["yes-1".into()],
            sender,
            shutdown_rx,
        ));
        let books = receiver.recv().await.unwrap();
        assert_eq!(books.len(), 1);
        assert_eq!(books[0].raw_payload["event_type"], "book");
        shutdown_tx.send(true).unwrap();
        assert_eq!(transport.await.unwrap(), Ok(()));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn managed_transport_updates_same_socket_and_reconnects_current_membership() {
        managed_membership_roundtrip(false, false).await;
    }

    #[tokio::test]
    async fn supervised_shard_preserves_membership_when_retry_group_exhausts() {
        managed_membership_roundtrip(true, false).await;
    }

    #[tokio::test]
    async fn managed_transport_can_remove_before_add_without_transient_over_subscription() {
        managed_membership_roundtrip(false, true).await;
    }

    async fn managed_membership_roundtrip(supervised: bool, remove_first: bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = Url::parse(&format!(
                "ws://{}/ws/market",
                listener.local_addr().unwrap()
            ))
            .unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                let first = socket.next().await.unwrap().unwrap().into_text().unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(&first).unwrap()["assets_ids"],
                    serde_json::json!(["a", "b"])
                );
                socket
                    .send(Message::Text(
                        serde_json::json!({"event_type":"book","asset_id":"b"})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                let expected = if remove_first {
                    [
                        serde_json::json!({"operation":"unsubscribe","assets_ids":["a"]}),
                        serde_json::json!({"operation":"subscribe","assets_ids":["c"]}),
                    ]
                } else {
                    [
                        serde_json::json!({"operation":"subscribe","assets_ids":["c"]}),
                        serde_json::json!({"operation":"unsubscribe","assets_ids":["a"]}),
                    ]
                };
                for expected in expected {
                    let message = socket.next().await.unwrap().unwrap().into_text().unwrap();
                    assert_eq!(serde_json::from_str::<Value>(&message).unwrap(), expected);
                }
                // A frame already queued by the venue before unsubscribe is
                // no longer owned, and must not become a global routing gap.
                socket
                    .send(Message::Text(
                        serde_json::json!({"event_type":"book","asset_id":"a"})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                socket
                    .send(Message::Text(
                        serde_json::json!({"event_type":"book","asset_id":"c"})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                socket.close(None).await.unwrap();
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                let next = socket.next().await.unwrap().unwrap().into_text().unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(&next).unwrap()["assets_ids"],
                    serde_json::json!(["b", "c"])
                );
                socket
                    .send(Message::Text(
                        serde_json::json!({"event_type":"book","asset_id":"b"})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                while let Some(message) = socket.next().await {
                    match message.unwrap() {
                        Message::Close(_) => {
                            let _ = socket.flush().await;
                            break;
                        }
                        Message::Text(text) if text == "PING" => {
                            socket.send(Message::Text("PONG".into())).await.unwrap()
                        }
                        other => panic!("unexpected subscription/control: {other:?}"),
                    }
                }
            });
            let (sender, mut receiver) = transport_output(4, 65_536, 65_536).unwrap();
            let (stop, shutdown) = watch::channel(false);
            let (commands, updates) = subscription::subscription_commands();
            let worker = tokio::spawn(async move {
                let mut config = PolymarketTransportConfig::loopback(endpoint);
                if supervised {
                    config.maximum_reconnect_attempts = 1;
                    supervise_polymarket_transport_managed(
                        config,
                        vec!["a".into(), "b".into()],
                        sender,
                        shutdown,
                        updates,
                        Duration::from_millis(5),
                    )
                    .await
                } else {
                    run_polymarket_transport_managed(
                        config,
                        vec!["a".into(), "b".into()],
                        sender,
                        shutdown,
                        updates,
                    )
                    .await
                }
            });
            assert_eq!(
                receiver.recv().await.unwrap()[0].raw_payload["asset_id"],
                "b"
            );
            let (receipt, done) = tokio::sync::oneshot::channel();
            commands
                .send(subscription::SubscriptionUpdate {
                    expected_tokens: BTreeSet::from(["a".into(), "b".into()]),
                    token_ids: BTreeSet::from(["b".into(), "c".into()]),
                    remove_first,
                    receipt,
                })
                .await
                .unwrap();
            assert_eq!(done.await.unwrap(), Ok(()));
            assert_eq!(
                receiver.recv().await.unwrap()[0].raw_payload["asset_id"],
                "c"
            );
            let gaps = receiver.recv().await.unwrap();
            assert_eq!(
                gaps.iter()
                    .map(|f| f.raw_payload["asset_id"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                vec!["b", "c"]
            );
            assert!(
                gaps.iter()
                    .all(|f| f.raw_payload["event_type"] == "source_gap")
            );
            assert_eq!(
                receiver.recv().await.unwrap()[0].raw_payload["asset_id"],
                "b"
            );
            let (receipt, done) = tokio::sync::oneshot::channel();
            commands
                .send(subscription::SubscriptionUpdate {
                    expected_tokens: BTreeSet::from(["a".into(), "b".into()]),
                    token_ids: BTreeSet::from(["b".into(), "c".into()]),
                    remove_first,
                    receipt,
                })
                .await
                .unwrap();
            assert_eq!(
                done.await.unwrap(),
                Err(TransportError::SubscriptionConflict)
            );
            stop.send(true).unwrap();
            assert_eq!(worker.await.unwrap(), Ok(()));
            server.await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn bounded_transport_waits_for_capacity_and_shutdown_without_dropping() {
        let (sender, mut receiver) = transport_output(1, 64, 64).unwrap();
        let (_shutdown_tx, mut first_shutdown) = watch::channel(false);
        assert_eq!(
            send_bounded(
                &sender,
                vec![RawTransportFrame {
                    raw_payload: serde_json::json!({"event_type":"first"}),
                    received_at: at(),
                }],
                32,
                &BTreeSet::new(),
                &mut first_shutdown,
                None,
            )
            .await,
            Ok(true)
        );
        assert_eq!(
            sender.take_occupancy(),
            TransportQueueOccupancy {
                current_bytes: 32,
                current_batches: 1,
                high_bytes: 32,
                high_batches: 1,
            }
        );
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let pending_sender = sender.clone();
        let pending = tokio::spawn(async move {
            send_bounded(
                &pending_sender,
                vec![RawTransportFrame {
                    raw_payload: serde_json::json!({"event_type":"test"}),
                    received_at: at(),
                }],
                32,
                &BTreeSet::new(),
                &mut shutdown_rx,
                None,
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!pending.is_finished());
        receiver.recv().await.unwrap();
        assert_eq!(pending.await.unwrap(), Ok(true));
        assert_eq!(receiver.recv().await.unwrap().len(), 1);
        assert_eq!(sender.take_occupancy().current_bytes, 0);
        assert_eq!(sender.take_occupancy().current_batches, 0);

        let (_blocker_shutdown_tx, mut blocker_shutdown) = watch::channel(false);
        assert_eq!(
            send_bounded(
                &sender,
                vec![RawTransportFrame {
                    raw_payload: serde_json::json!({"event_type":"blocker"}),
                    received_at: at(),
                }],
                32,
                &BTreeSet::new(),
                &mut blocker_shutdown,
                None,
            )
            .await,
            Ok(true)
        );
        let (shutdown_tx2, mut shutdown_rx2) = watch::channel(false);
        let pending_sender = sender.clone();
        let pending = tokio::spawn(async move {
            send_bounded(
                &pending_sender,
                vec![RawTransportFrame {
                    raw_payload: serde_json::json!({"event_type":"never"}),
                    received_at: at(),
                }],
                32,
                &BTreeSet::new(),
                &mut shutdown_rx2,
                None,
            )
            .await
        });
        tokio::task::yield_now().await;
        shutdown_tx2.send(true).unwrap();
        assert_eq!(pending.await.unwrap(), Ok(false));
    }

    #[tokio::test]
    async fn busy_market_frames_do_not_starve_periodic_heartbeat() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut endpoint =
                Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
            endpoint.set_path("/ws/market");
            let (ping_seen, mut ping_received) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                let subscription = socket.next().await.unwrap().unwrap().into_text().unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(&subscription).unwrap()["assets_ids"],
                    serde_json::json!(["token"])
                );
                let mut ping_seen = Some(ping_seen);
                for sequence in 0..80 {
                    socket
                        .send(Message::Text(
                            serde_json::json!({
                                "event_type":"book", "asset_id":"token", "sequence":sequence
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .unwrap();
                    if let Ok(Some(Ok(Message::Text(text)))) =
                        tokio::time::timeout(Duration::from_millis(5), socket.next()).await
                        && text == "PING"
                    {
                        socket.send(Message::Text("PONG".into())).await.unwrap();
                        ping_seen.take().unwrap().send(()).unwrap();
                        break;
                    }
                }
                while let Some(message) = socket.next().await {
                    match message.unwrap() {
                        Message::Close(_) => break,
                        Message::Text(text) if text == "PING" => {
                            socket.send(Message::Text("PONG".into())).await.unwrap();
                        }
                        other => panic!("unexpected client frame: {other:?}"),
                    }
                }
            });
            let (sender, mut receiver) = transport_output(128, 1_048_576, 65_536).unwrap();
            let (stop, shutdown) = watch::channel(false);
            let mut worker = tokio::spawn(run_polymarket_transport(
                PolymarketTransportConfig::loopback(endpoint),
                vec!["token".into()],
                sender,
                shutdown,
            ));
            loop {
                tokio::select! {
                    result = &mut ping_received => { result.unwrap(); break; }
                    batch = receiver.recv() => { drop(batch.expect("transport output stopped")); }
                    result = &mut worker => { panic!("transport stopped before heartbeat: {:?}", result.unwrap()); }
                }
            }
            stop.send(true).unwrap();
            assert_eq!(worker.await.unwrap(), Ok(()));
            server.await.unwrap();
        })
        .await
        .unwrap();
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
    fn delayed_atomic_delta_older_than_recovery_book_is_an_auditable_noop() {
        let mut strict = config();
        strict.maximum_source_delay_ms = 1_000;
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        writer.apply(snapshot(1)).unwrap();
        let normalized = normalize_frame(
            &strict,
            serde_json::json!({
                "event_type":"price_change", "timestamp":"2026-08-03T03:59:59Z",
                "price_changes":[
                    {"asset_id":"yes-1","side":"BUY","price":"0.40","size":"0",
                     "best_bid":"0.40","best_ask":"0.42"}
                ]
            }),
            at(),
            2,
        )
        .unwrap()
        .remove(0);
        assert!(normalized.source.delayed);

        let outcome = writer.apply(normalized).unwrap();
        assert!(outcome.persisted.applied);
        assert_eq!(outcome.persisted.fail_closed_reason, None);
        assert_eq!(
            outcome.projection.books["yes-1"]
                .bids
                .last_key_value()
                .unwrap()
                .0
                .0
                .to_string(),
            "0.4"
        );
        assert!(outcome.projection.ready);
        assert!(!outcome.projection.unresolved_gaps.contains("yes-1"));
    }

    #[test]
    fn queued_non_delayed_atomic_delta_older_than_refresh_book_is_an_auditable_noop() {
        let mut tolerant = config();
        tolerant.maximum_source_delay_ms = 10_000;
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        let mut refreshed = snapshot(1);
        assert!(bind_full_book_refresh_identity(&mut refreshed));
        refreshed.received_at += chrono::Duration::seconds(1);
        writer.apply(refreshed).unwrap();
        let normalized = normalize_frame(
            &tolerant,
            serde_json::json!({
                "event_type":"price_change", "timestamp":"2026-08-03T04:00:02Z",
                "price_changes":[
                    {"asset_id":"yes-1","side":"BUY","price":"0.39","size":"10",
                     "best_bid":"0.39","best_ask":"0.42"}
                ]
            }),
            at(),
            2,
        )
        .unwrap()
        .remove(0);
        assert!(!normalized.source.delayed);
        assert!(
            normalized.source_observed_at
                < writer.projection().books["yes-1"]
                    .source_observed_at
                    .unwrap()
        );

        let outcome = writer.apply(normalized).unwrap();
        assert!(outcome.persisted.applied);
        assert_eq!(outcome.persisted.fail_closed_reason, None);
        assert_eq!(
            outcome.projection.books["yes-1"]
                .bids
                .last_key_value()
                .unwrap()
                .0
                .0
                .to_string(),
            "0.4"
        );
        assert!(outcome.projection.ready);
        assert!(!outcome.projection.unresolved_gaps.contains("yes-1"));
    }

    #[test]
    fn bbo_not_newer_than_authoritative_book_is_an_auditable_noop() {
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        writer.apply(snapshot(1)).unwrap();
        let normalized = normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"best_bid_ask", "asset_id":"yes-1",
                "timestamp":"2026-08-03T03:59:59Z", "best_bid":"0.39", "best_ask":"0.42"
            }),
            at(),
            2,
        )
        .unwrap()
        .remove(0);
        assert!(
            normalized.source_observed_at
                < writer.projection().books["yes-1"]
                    .source_observed_at
                    .unwrap()
        );
        let outcome = writer.apply(normalized).unwrap();
        assert!(outcome.persisted.applied);
        assert_eq!(outcome.persisted.fail_closed_reason, None);
        assert_eq!(
            outcome.projection.books["yes-1"]
                .bids
                .last_key_value()
                .unwrap()
                .0
                .0
                .to_string(),
            "0.4"
        );
        assert!(outcome.projection.ready);
        assert!(!outcome.projection.unresolved_gaps.contains("yes-1"));
    }

    #[test]
    fn network_delivered_atomic_delta_with_older_source_boundary_is_an_auditable_noop() {
        let mut tolerant = config();
        tolerant.maximum_source_delay_ms = 10_000;
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        let mut refreshed = snapshot(1);
        assert!(bind_full_book_refresh_identity(&mut refreshed));
        refreshed.received_at += chrono::Duration::seconds(1);
        refreshed.source_observed_at = refreshed.received_at;
        writer.apply(refreshed).unwrap();
        let mut normalized = normalize_frame(
            &tolerant,
            serde_json::json!({
                "event_type":"price_change", "timestamp":"2026-08-03T04:00:02Z",
                "price_changes":[
                    {"asset_id":"yes-1","side":"BUY","price":"0.39","size":"10",
                     "best_bid":"0.39","best_ask":"0.42"}
                ]
            }),
            at(),
            2,
        )
        .unwrap()
        .remove(0);
        normalized.received_at += chrono::Duration::seconds(2);
        assert!(!normalized.source.delayed);
        assert!(
            normalized.received_at
                > writer.projection().books["yes-1"]
                    .authoritative_refresh_received_at
                    .unwrap()
        );
        assert!(
            normalized.source_observed_at
                < writer.projection().books["yes-1"]
                    .source_observed_at
                    .unwrap()
        );

        let outcome = writer.apply(normalized).unwrap();
        assert!(outcome.persisted.applied);
        assert_eq!(outcome.persisted.fail_closed_reason, None);
        assert!(matches!(
            outcome.persisted.event.kind,
            EventKind::AtomicDelta { ref changes, .. } if changes.is_empty()
        ));
        assert_eq!(
            outcome.projection.books["yes-1"]
                .bids
                .last_key_value()
                .unwrap()
                .0
                .0
                .to_string(),
            "0.4"
        );
        assert!(outcome.projection.ready);
        assert!(!outcome.projection.unresolved_gaps.contains("yes-1"));
    }

    #[test]
    fn quiet_market_full_book_keeps_supplier_time_separate_from_receipt() {
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
        assert!(normalized.source.delayed);
        assert_eq!(normalized.source_observed_at,
            chrono::DateTime::parse_from_rfc3339("2026-08-03T03:00:00Z")
                .unwrap().with_timezone(&chrono::Utc));
        assert_eq!(normalized.received_at, at());
        assert_eq!(normalized.raw_payload["timestamp"], "2026-08-03T03:00:00Z");
    }

    #[test]
    fn supplier_quote_without_source_timestamp_cannot_inherit_receipt_time() {
        let raw = serde_json::json!({
            "event_type":"book", "asset_id":"yes-1", "tick_size":"0.01",
            "bids":[{"price":"0.40","size":"10"}],
            "asks":[{"price":"0.42","size":"11"}]
        });
        assert_eq!(normalize_frame(&config(), raw, at(), 1),
            Err(NormalizeError::MissingField("timestamp")));
    }

    #[test]
    fn zero_tick_and_off_grid_price_are_rejected() {
        let zero_tick = serde_json::json!({
            "event_type":"book", "asset_id":"yes-1", "timestamp":"2026-08-03T04:00:00Z", "tick_size":"0",
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
                "event_type":"book", "asset_id":"yes-1", "timestamp":"2026-08-03T04:00:00Z", "tick_size":"0.01",
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
