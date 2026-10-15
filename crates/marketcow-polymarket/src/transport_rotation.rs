//! A bounded, market-scoped rolling hot standby. Only the active connection
//! publishes; a third connection may replace the standby without waiting for
//! the active connection to become stale.
use super::*;
use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;

const PREPARE_AGE_MS: i64 = 1_200;
const SWITCH_AGE_MS: i64 = 1_500;
const PREPARE_DWELL: Duration = Duration::from_millis(200);
const SWITCH_DWELL: Duration = Duration::from_millis(500);
const STANDBY_BYTES: usize = 32 * 1024 * 1024;
const SOCKET_INGRESS_BYTES: usize = 32 * 1024 * 1024;
const SOCKET_INGRESS_MESSAGES: usize = 1024;
#[cfg(not(test))]
const STANDBY_REFRESH_BYTES: usize = 4 * 1024 * 1024;
#[cfg(test)]
const STANDBY_REFRESH_BYTES: usize = 1024;
const STANDBY_MAX_TENURE: Duration = Duration::from_secs(30);
static NEXT_ROTATION_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_SOCKET_PUMP_ID: AtomicU64 = AtomicU64::new(1);

fn rotation_log(rotation_id: u64, event: &str, details: serde_json::Value) {
    crate::async_eprintln!(
        "{}",
        serde_json::json!({
            "stage":"polymarket_ws_hot_rotation",
            "observed_at":Utc::now().to_rfc3339(),
            "rotation_id":rotation_id,
            "event":event,
            "details":details,
        })
    );
}

type MarketSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct SocketIngress {
    text: String,
    received_at: DateTime<Utc>,
    queued_at: Instant,
    poll_wait: Duration,
    _bytes: OwnedSemaphorePermit,
}

enum SocketCommand {
    Send(Message, oneshot::Sender<Result<(), TransportError>>),
    Close,
}

/// Each physical connection keeps reading even while the market owner waits
/// for downstream capacity or replays another connection's baseline. Queue
/// overflow is a broken source prefix, never permission to skip a frame.
struct SocketPump {
    id: String,
    ingress: mpsc::Receiver<SocketIngress>,
    commands: mpsc::Sender<SocketCommand>,
    task: JoinHandle<Result<ConnectionEnd, TransportError>>,
}

impl SocketPump {
    fn start(socket: MarketSocket, heartbeat_interval: Duration, rotation_id: u64) -> Self {
        let id = format!(
            "{rotation_id}-{}",
            NEXT_SOCKET_PUMP_ID.fetch_add(1, Ordering::Relaxed)
        );
        rotation_log(
            rotation_id,
            "socket_pump_started",
            serde_json::json!({
                "socket_id":id,"ingress_messages_limit":SOCKET_INGRESS_MESSAGES,
                "ingress_bytes_limit":SOCKET_INGRESS_BYTES,
            }),
        );
        let (ingress_tx, ingress) = mpsc::channel(SOCKET_INGRESS_MESSAGES);
        let (commands, mut command_rx) = mpsc::channel(8);
        let byte_budget = Arc::new(Semaphore::new(SOCKET_INGRESS_BYTES));
        let worker_id = id.clone();
        let task = tokio::spawn(async move {
            let mut socket = socket;
            let mut heartbeat = tokio::time::interval_at(
                tokio::time::Instant::now() + heartbeat_interval,
                heartbeat_interval,
            );
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let poll_started = Instant::now();
                tokio::select! {
                    command = command_rx.recv() => match command {
                        Some(SocketCommand::Send(message, reply)) => {
                            let result = socket.send(message).await.map_err(|_| TransportError::SendFailed);
                            let failed = result.is_err();
                            let _ = reply.send(result);
                            if failed { return Err(TransportError::SendFailed); }
                        }
                        Some(SocketCommand::Close) | None => {
                            let _ = socket.close(None).await;
                            return Ok(ConnectionEnd::Shutdown);
                        }
                    },
                    message = socket.next() => match message {
                        Some(Ok(Message::Text(text))) if text == "PONG" => {}
                        Some(Ok(Message::Text(text))) => {
                            let received_at = Utc::now();
                            let queued_at = Instant::now();
                            let poll_wait = poll_started.elapsed();
                            let wire_bytes = text.len();
                            if wire_bytes == 0 || wire_bytes > SOCKET_INGRESS_BYTES {
                                rotation_log(rotation_id, "socket_ingress_rejected",
                                    serde_json::json!({"socket_id":worker_id,
                                        "reason":"frame_byte_limit","wire_bytes":wire_bytes}));
                                return Err(TransportError::FrameTooLarge);
                            }
                            let permit = byte_budget.clone()
                                .try_acquire_many_owned(wire_bytes as u32)
                                .map_err(|_| {
                                    rotation_log(rotation_id, "socket_ingress_rejected",
                                        serde_json::json!({"socket_id":worker_id,
                                            "reason":"byte_budget","wire_bytes":wire_bytes}));
                                    TransportError::ReadFailed
                                })?;
                            if ingress_tx.try_send(SocketIngress {
                                text: text.to_string(), received_at,
                                queued_at, poll_wait, _bytes: permit,
                            }).is_err() {
                                rotation_log(rotation_id, "socket_ingress_rejected",
                                    serde_json::json!({"socket_id":worker_id,
                                        "reason":"message_budget","wire_bytes":wire_bytes}));
                                return Err(TransportError::ReadFailed);
                            }
                        }
                        Some(Ok(Message::Ping(data))) => socket.send(Message::Pong(data)).await
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
                        Some(Ok(Message::Binary(_) | Message::Frame(_))) => return Err(TransportError::InvalidFrame),
                        Some(Err(error)) => {
                            crate::async_eprintln!("{}", serde_json::json!({
                                "stage":"polymarket_websocket_read_failed",
                                "rotation_id":rotation_id,
                                "socket_id":worker_id,
                                "error_class":websocket_read_error_class(&error),
                            }));
                            return Err(TransportError::ReadFailed);
                        }
                    },
                    _ = heartbeat.tick() => socket.send(Message::Text("PING".into())).await
                        .map_err(|_| TransportError::SendFailed)?,
                }
            }
        });
        Self {
            id,
            ingress,
            commands,
            task,
        }
    }

    async fn send(&self, message: Message, timeout: Duration) -> Result<(), TransportError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        tokio::time::timeout(timeout, async {
            self.commands
                .send(SocketCommand::Send(message, reply_tx))
                .await
                .map_err(|_| TransportError::SendFailed)?;
            reply_rx.await.map_err(|_| TransportError::SendFailed)?
        })
        .await
        .map_err(|_| TransportError::SendFailed)?
    }

    async fn finish(&mut self) -> Result<ConnectionEnd, TransportError> {
        (&mut self.task)
            .await
            .map_err(|_| TransportError::ReadFailed)?
    }

    async fn close(&mut self) {
        let _ = self.commands.try_send(SocketCommand::Close);
        if tokio::time::timeout(Duration::from_millis(250), &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
        }
    }
}

impl Drop for SocketPump {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Standby {
    socket: Option<MarketSocket>,
    pump: Option<SocketPump>,
    ready_at: Instant,
    buffered: VecDeque<(Vec<RawTransportFrame>, usize)>,
    bytes: usize,
    max_ingress_wait: Duration,
    books: BTreeSet<String>,
    latest: BTreeMap<String, (i64, Instant)>,
}

struct StandbyReject {
    reason: &'static str,
    error: TransportError,
}

impl Standby {
    fn new(socket: MarketSocket) -> Self {
        Self {
            socket: Some(socket),
            pump: None,
            ready_at: Instant::now(),
            buffered: VecDeque::new(),
            bytes: 0,
            max_ingress_wait: Duration::ZERO,
            books: BTreeSet::new(),
            latest: BTreeMap::new(),
        }
    }

    fn retain(
        &mut self,
        frames: Vec<RawTransportFrame>,
        wire_bytes: usize,
        tokens: &BTreeSet<String>,
    ) -> Result<(), StandbyReject> {
        self.bytes = self
            .bytes
            .checked_add(wire_bytes)
            .filter(|bytes| *bytes <= STANDBY_BYTES)
            .ok_or(StandbyReject {
                reason: "standby_buffer_limit",
                error: TransportError::Backpressure,
            })?;
        let mut seen = self.books.clone();
        for frame in &frames {
            let raw = &frame.raw_payload;
            let kind = raw["event_type"].as_str();
            if kind == Some("book") {
                let token = raw["asset_id"].as_str().ok_or(StandbyReject {
                    reason: "book_without_asset_id",
                    error: TransportError::InvalidFrame,
                })?;
                if !tokens.contains(token) {
                    return Err(StandbyReject {
                        reason: "book_outside_subscription",
                        error: TransportError::InvalidFrame,
                    });
                }
                seen.insert(token.to_owned());
            } else if seen.len() != tokens.len() {
                // A delta before the complete market baseline cannot be
                // replayed atomically across both books. Retry this standby.
                return Err(StandbyReject {
                    reason: "delta_before_complete_baseline",
                    error: TransportError::InvalidFrame,
                });
            }
            record_quote_clock(raw, frame.received_at, &mut self.latest, tokens);
        }
        self.books = seen;
        self.buffered.push_back((frames, wire_bytes));
        Ok(())
    }

    fn fresh(&self, tokens: &BTreeSet<String>) -> bool {
        self.pump
            .as_ref()
            .is_some_and(|pump| !pump.task.is_finished())
            && self.books == *tokens
            && tokens.iter().all(|token| {
                self.latest.get(token).is_some_and(|(age, at)| {
                    age.saturating_add(at.elapsed().as_millis() as i64) < SWITCH_AGE_MS
                })
            })
    }

    fn switchable(&self, tokens: &BTreeSet<String>) -> bool {
        self.fresh(tokens)
            && self
                .pump
                .as_ref()
                .is_some_and(|pump| pump.ingress.is_empty())
    }

    fn refresh_reason(&self) -> Option<&'static str> {
        if self.bytes >= STANDBY_REFRESH_BYTES {
            Some("buffer_refresh_threshold")
        } else if self.ready_at.elapsed() >= STANDBY_MAX_TENURE {
            Some("standby_tenure_expired")
        } else {
            None
        }
    }
}

fn source_timestamp(value: &Value) -> Option<i64> {
    let millis = value["timestamp"].as_str()?.parse::<i64>().ok()?;
    (1_000_000_000_000..10_000_000_000_000)
        .contains(&millis)
        .then_some(millis)
}

fn record_quote_clock(
    raw: &Value,
    received_at: DateTime<Utc>,
    latest: &mut BTreeMap<String, (i64, Instant)>,
    tokens: &BTreeSet<String>,
) {
    let kind = raw["event_type"].as_str();
    if !matches!(kind, Some("book" | "price_change")) {
        return;
    }
    let Some(timestamp) = source_timestamp(raw) else {
        return;
    };
    let age = received_at.timestamp_millis().saturating_sub(timestamp);
    let now = Instant::now();
    if kind == Some("book") {
        if let Some(token) = raw["asset_id"].as_str().filter(|t| tokens.contains(*t)) {
            latest.insert(token.to_owned(), (age, now));
        }
    } else if let Some(changes) = raw["price_changes"].as_array() {
        for change in changes {
            if let Some(token) = change["asset_id"].as_str().filter(|t| tokens.contains(*t)) {
                latest.insert(token.to_owned(), (age, now));
            }
        }
    }
}

fn batch_age(frames: &[RawTransportFrame]) -> Option<i64> {
    frames
        .iter()
        .filter_map(|frame| {
            matches!(
                frame.raw_payload["event_type"].as_str(),
                Some("book" | "price_change")
            )
            .then(|| {
                source_timestamp(&frame.raw_payload).map(|timestamp| {
                    frame
                        .received_at
                        .timestamp_millis()
                        .saturating_sub(timestamp)
                })
            })
            .flatten()
        })
        .last()
}

fn decode_text(
    text: &str,
    tokens: &BTreeSet<String>,
    received_at: DateTime<Utc>,
    maximum_frame_bytes: usize,
) -> Result<Vec<RawTransportFrame>, TransportError> {
    if text.len() > maximum_frame_bytes {
        return Err(TransportError::FrameTooLarge);
    }
    let payload: Value = serde_json::from_str(text).map_err(|_| TransportError::InvalidFrame)?;
    let values = match payload {
        Value::Array(values) => values,
        value @ Value::Object(_) => vec![value],
        _ => return Err(TransportError::InvalidFrame),
    };
    if values.iter().any(|value| !value.is_object()
        || value["event_type"] == "marketcow_connection_checkpoint") {
        return Err(TransportError::InvalidFrame);
    }
    Ok(values
        .into_iter()
        .filter(|value| subscription::owned_or_unclassified(value, tokens))
        .map(|raw_payload| RawTransportFrame {
            raw_payload,
            received_at,
        })
        .collect())
}

async fn prepare_standby(
    config: PolymarketTransportConfig,
    tokens: BTreeSet<String>,
    rotation_id: u64,
    attempt: u64,
) -> Result<Standby, TransportError> {
    let (socket, response) = connect_websocket(&config).await?;
    if response.status() != 101 {
        return Err(TransportError::HandshakeRejected);
    }
    let mut standby = Standby::new(socket);
    standby
        .socket
        .as_mut()
        .expect("standby socket is owned during preparation")
        .send(Message::Text(
            serde_json::json!({
                "assets_ids":tokens,"type":"market","custom_feature_enabled":false,
            })
            .to_string()
            .into(),
        ))
        .await
        .map_err(|_| TransportError::SendFailed)?;
    let deadline = tokio::time::Instant::now() + config.connect_timeout;
    while standby.books != tokens {
        let message = tokio::time::timeout_at(
            deadline,
            standby
                .socket
                .as_mut()
                .expect("standby preparation socket")
                .next(),
        )
        .await
        .map_err(|_| TransportError::ConnectTimeout)?;
        match message {
            Some(Ok(Message::Text(text))) if text == "PONG" => {}
            Some(Ok(Message::Text(text))) => {
                let received_at = Utc::now();
                let frames = decode_text(&text, &tokens, received_at, config.maximum_frame_bytes)?;
                if !frames.is_empty() {
                    standby
                        .retain(frames, text.len(), &tokens)
                        .map_err(|rejection| {
                            rotation_log(
                                rotation_id,
                                "standby_seed_rejected",
                                serde_json::json!({
                                    "attempt":attempt,"reason":rejection.reason,
                                    "book_count":standby.books.len(),"required_books":tokens.len(),
                                    "buffered_bytes":standby.bytes,
                                }),
                            );
                            rejection.error
                        })?;
                }
            }
            Some(Ok(Message::Ping(data))) => standby
                .socket
                .as_mut()
                .expect("standby preparation socket")
                .send(Message::Pong(data))
                .await
                .map_err(|_| TransportError::SendFailed)?,
            Some(Ok(Message::Pong(_))) => {}
            _ => return Err(TransportError::ReadFailed),
        }
    }
    standby.ready_at = Instant::now();
    standby.pump = Some(SocketPump::start(
        standby.socket.take().expect("prepared standby socket"),
        config.heartbeat_interval,
        rotation_id,
    ));
    Ok(standby)
}

#[derive(Default)]
struct Rotation {
    prepare_since: Option<Instant>,
    high_since: Option<Instant>,
    prepare_confirmed: bool,
    high_confirmed: bool,
    may_prepare: bool,
}

impl Rotation {
    fn fresh() -> Self {
        Self {
            may_prepare: true,
            ..Self::default()
        }
    }

    /// A second real upstream observation is required after each dwell; a
    /// silent socket cannot make a threshold appear sustained.
    fn observe(&mut self, age: i64, now: Instant) -> bool {
        if age < PREPARE_AGE_MS {
            let cancel = self.prepare_since.is_some();
            *self = Self::fresh();
            return cancel;
        }
        if age < SWITCH_AGE_MS {
            if self.high_since.is_some() {
                // Falling below the switch threshold only cancels this
                // switch dwell. Preparation remains armed while the source
                // is still above the preparation threshold.
                self.high_since = None;
                self.high_confirmed = false;
                return true;
            }
            if self.may_prepare {
                self.prepare_since.get_or_insert(now);
                self.prepare_confirmed = self
                    .prepare_since
                    .is_some_and(|start| now.duration_since(start) >= PREPARE_DWELL);
            }
            return false;
        }
        if self.may_prepare {
            self.prepare_since.get_or_insert(now);
            self.prepare_confirmed = self
                .prepare_since
                .is_some_and(|start| now.duration_since(start) >= PREPARE_DWELL);
            self.high_since.get_or_insert(now);
            self.high_confirmed = self
                .high_since
                .is_some_and(|start| now.duration_since(start) >= SWITCH_DWELL);
        }
        false
    }
}

pub(super) async fn run_rotating_connection(
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
            serde_json::json!({
                "assets_ids":tokens,"type":"market","custom_feature_enabled":false,
            })
            .to_string()
            .into(),
        ))
        .await
        .map_err(|_| TransportError::SendFailed)?;

    let mut diagnostic = transport_diagnostic::TransportDiagnostic::new(tokens);
    let rotation_id = NEXT_ROTATION_ID.fetch_add(1, Ordering::Relaxed);
    let mut socket = SocketPump::start(socket, config.heartbeat_interval, rotation_id);
    rotation_log(
        rotation_id,
        "active_connected",
        serde_json::json!({
            "tokens":tokens,"proxy_enabled":config.https_proxy.is_some(),
            "connection_id":diagnostic.connection_id(),
            "socket_id":socket.id,
            "prepare_age_ms":PREPARE_AGE_MS,"switch_age_ms":SWITCH_AGE_MS,
            "prepare_dwell_ms":PREPARE_DWELL.as_millis(),
            "switch_dwell_ms":SWITCH_DWELL.as_millis(),
        }),
    );
    let mut control_tick = tokio::time::interval(Duration::from_millis(100));
    control_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut rotation = Rotation::fresh();
    let mut preparing: Option<JoinHandle<Result<Standby, TransportError>>>;
    let mut ready: Option<Standby> = None;
    let mut last_attempt = Instant::now();
    let mut last_active_age_ms = None;
    let mut last_active_observation_at: Option<Instant> = None;
    let mut attempt = 1_u64;
    let mut waiting_for_standby_logged = false;
    let mut preparing_replacement = false;
    rotation_log(
        rotation_id,
        "standby_prepare_started",
        serde_json::json!({
            "attempt":attempt,"token_count":tokens.len(),
            "active_age_ms":null,"replacing_existing":false,
            "reason":"initial_hot_standby","physical_connection_limit":3,
        }),
    );
    let cloned = config.clone();
    let subscribed = tokens.clone();
    preparing = Some(tokio::spawn(async move {
        prepare_standby(cloned, subscribed, rotation_id, attempt).await
    }));
    loop {
        let mut active_age = None;
        tokio::select! {
            _ = control_tick.tick() => diagnostic.flush_if_due(),
            update = async { match updates.as_mut() {
                Some(receiver) => receiver.recv().await,
                None => std::future::pending().await,
            }} => {
                let Some(update) = update else { *updates = None; continue; };
                rotation_log(rotation_id, "membership_changed", serde_json::json!({
                    "attempt":attempt,"preparing":preparing.is_some(),
                    "standby_ready":ready.is_some(),
                }));
                if let Some(job) = preparing.take() { job.abort(); }
                ready = None;
                preparing_replacement = false;
                rotation = Rotation::fresh();
                waiting_for_standby_logged = false;
                let (added, removed) = match subscription::diff(tokens, &update) {
                    Ok(diff) => diff,
                    Err(error) => { let _ = update.receipt.send(Err(error)); continue; }
                };
                let operations = if update.remove_first {
                    [("unsubscribe", &removed), ("subscribe", &added)]
                } else {
                    [("subscribe", &added), ("unsubscribe", &removed)]
                };
                for (operation, ids) in operations {
                    if ids.is_empty() { continue; }
                    let message = Message::Text(serde_json::json!({
                        "operation":operation,"assets_ids":ids,
                    }).to_string().into());
                    let sent = tokio::select! {
                        result=socket.send(message, config.connect_timeout) => result.is_ok(),
                        _=shutdown.changed()=>false,
                    };
                    if !sent {
                        tokens.extend(added.iter().cloned());
                        let _ = update.receipt.send(Err(TransportError::SendFailed));
                        return Err(TransportError::SendFailed);
                    }
                    if operation == "subscribe" { tokens.extend(ids.iter().cloned()); }
                    else { for id in ids { tokens.remove(id); } }
                }
                diagnostic.set_tokens(tokens);
                let _ = update.receipt.send(Ok(()));
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    rotation_log(rotation_id, "shutdown", serde_json::json!({"attempt":attempt}));
                    if let Some(job) = preparing.take() { job.abort(); }
                    socket.close().await;
                    return Ok(ConnectionEnd::Shutdown);
                }
            }
            message = socket.ingress.recv() => match message {
                Some(message) => {
                    diagnostic.record_socket_ingress_wait(message.queued_at.elapsed());
                    let decode_started = Instant::now();
                    let frames = decode_text(&message.text, tokens, message.received_at,
                        config.maximum_frame_bytes)?;
                    if frames.is_empty() { continue; }
                    diagnostic.record_batch(&frames, message.text.len(),
                        message.poll_wait, decode_started.elapsed());
                    active_age = batch_age(&frames);
                    if active_age.is_some() {
                        last_active_age_ms = active_age;
                        last_active_observation_at = Some(Instant::now());
                    }
                    let mut timing = transport_diagnostic::SendTiming::default();
                    let sent = tokio::select! {
                        result = send_bounded(output, frames, message.text.len(), tokens, shutdown,
                            Some(&mut timing)) => result?,
                        ended = &mut socket.task => {
                            rotation_log(rotation_id, "active_socket_stopped_while_publishing",
                                serde_json::json!({"socket_id":socket.id,"result":format!("{ended:?}")}));
                            return Err(TransportError::ReadFailed);
                        }
                    };
                    if !sent { return Ok(ConnectionEnd::Shutdown); }
                    diagnostic.record_send(&timing);
                    diagnostic.flush_if_due();
                }
                None => {
                    let end = socket.finish().await?;
                    rotation_log(rotation_id, "active_closed", serde_json::json!({
                        "attempt":attempt,"socket_id":socket.id,"close_code":match &end {
                            ConnectionEnd::Disconnected { close_code, .. } => *close_code,
                            ConnectionEnd::Shutdown => None,
                        },
                        "close_reason":match &end {
                            ConnectionEnd::Disconnected { close_reason, .. } => close_reason.as_deref(),
                            ConnectionEnd::Shutdown => Some("socket_pump_stopped"),
                        },
                    }));
                    return Ok(end);
                }
            },
            result = async { match preparing.as_mut() {
                Some(job) => Some(job.await), None => std::future::pending().await,
            }} => {
                preparing = None;
                match result {
                    Some(Ok(Ok(candidate))) => {
                        rotation_log(rotation_id, "standby_ready", serde_json::json!({
                            "attempt":attempt,"book_count":candidate.books.len(),
                            "socket_id":candidate.pump.as_ref().map(|pump| pump.id.as_str()),
                            "buffered_batches":candidate.buffered.len(),"buffered_bytes":candidate.bytes,
                            "fresh":candidate.fresh(tokens),
                            "replacing_existing":preparing_replacement,
                            "prepare_elapsed_ms":last_attempt.elapsed().as_millis(),
                        }));
                        if candidate.fresh(tokens) {
                            if let Some(old) = ready.replace(candidate) {
                                rotation_log(rotation_id, "standby_replaced", serde_json::json!({
                                    "attempt":attempt,"old_buffered_bytes":old.bytes,
                                    "old_tenure_ms":old.ready_at.elapsed().as_millis(),
                                }));
                            }
                            waiting_for_standby_logged = false;
                        } else {
                            rotation_log(rotation_id, "standby_discarded", serde_json::json!({
                                "attempt":attempt,"reason":"not_fresh_after_seed",
                            }));
                        }
                    }
                    Some(Ok(Err(error))) => rotation_log(rotation_id, "standby_prepare_failed",
                        serde_json::json!({"attempt":attempt,"reason":error.reason_code(),
                            "prepare_elapsed_ms":last_attempt.elapsed().as_millis()})),
                    Some(Err(error)) => rotation_log(rotation_id, "standby_prepare_failed",
                        serde_json::json!({"attempt":attempt,"reason":"task_failed",
                            "cancelled":error.is_cancelled(),
                            "prepare_elapsed_ms":last_attempt.elapsed().as_millis()})),
                    None => {}
                }
                preparing_replacement = false;
            }
            message = async { match ready.as_mut() {
                Some(candidate) => candidate.pump.as_mut().expect("ready standby pump").ingress.recv().await,
                None => std::future::pending().await,
            }} => {
                let Some(candidate) = ready.as_mut() else { continue; };
                match message {
                    Some(message) => {
                        candidate.max_ingress_wait = candidate.max_ingress_wait
                            .max(message.queued_at.elapsed());
                        match decode_text(&message.text, tokens, message.received_at,
                            config.maximum_frame_bytes) {
                            Ok(frames) if frames.is_empty() => {},
                            Ok(frames) => {
                                if let Err(error) = candidate.retain(frames, message.text.len(), tokens) {
                                    rotation_log(rotation_id, "standby_discarded", serde_json::json!({
                                        "attempt":attempt,"reason":error.reason,
                                        "error_code":error.error.reason_code(),
                                        "book_count":candidate.books.len(),
                                        "buffered_bytes":candidate.bytes,
                                    }));
                                    ready = None;
                                }
                            }
                            Err(error) => {
                                rotation_log(rotation_id, "standby_discarded", serde_json::json!({
                                    "attempt":attempt,"reason":error.reason_code(),
                                    "phase":"decode",
                                }));
                                ready = None;
                            }
                        }
                    }
                    None => {
                        let end = candidate.pump.as_mut().expect("ready standby pump").finish().await;
                        rotation_log(rotation_id, "standby_discarded", serde_json::json!({
                            "attempt":attempt,"reason":"socket_pump_stopped",
                            "result":format!("{end:?}"),
                        }));
                        ready = None;
                    }
                }
            }
        }

        if let Some(age) = active_age {
            let was_prepare_confirmed = rotation.prepare_confirmed;
            let was_high_confirmed = rotation.high_confirmed;
            if rotation.observe(age, Instant::now()) {
                rotation_log(
                    rotation_id,
                    "threshold_reset",
                    serde_json::json!({
                        "attempt":attempt,"active_age_ms":age,
                        "preparing":preparing.is_some(),"standby_ready":ready.is_some(),
                    }),
                );
                // A transient recovery cancels the switch decision, not the
                // independently healthy standby or its replacement attempt.
                waiting_for_standby_logged = false;
            }
            if !was_prepare_confirmed && rotation.prepare_confirmed {
                rotation_log(
                    rotation_id,
                    "prepare_threshold_confirmed",
                    serde_json::json!({
                        "attempt":attempt,"active_age_ms":age,
                    }),
                );
            }
            if !was_high_confirmed && rotation.high_confirmed {
                rotation_log(
                    rotation_id,
                    "switch_threshold_confirmed",
                    serde_json::json!({
                        "attempt":attempt,"active_age_ms":age,
                        "preparing":preparing.is_some(),"standby_ready":ready.is_some(),
                    }),
                );
            }
        }
        if rotation.high_confirmed {
            if ready
                .as_ref()
                .is_some_and(|candidate| candidate.switchable(tokens))
            {
                let mut candidate = ready.take().expect("checked standby");
                let switch_started = Instant::now();
                let candidate_ages_ms = tokens
                    .iter()
                    .map(|token| {
                        (
                            token.clone(),
                            candidate.latest.get(token).map(|(age, at)| {
                                age.saturating_add(at.elapsed().as_millis() as i64)
                            }),
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                rotation_log(
                    rotation_id,
                    "switch_started",
                    serde_json::json!({
                        "attempt":attempt,"active_age_ms":last_active_age_ms,
                        "old_connection_id":diagnostic.connection_id(),
                        "old_socket_id":socket.id,
                        "candidate_socket_id":candidate.pump.as_ref().map(|pump| pump.id.as_str()),
                        "candidate_ages_ms":candidate_ages_ms,
                        "buffered_batches":candidate.buffered.len(),"buffered_bytes":candidate.bytes,
                        "candidate_ingress_queue_max_ms":candidate.max_ingress_wait.as_millis(),
                        "candidate_pending_messages":candidate.pump.as_ref()
                            .map(|pump| pump.ingress.len()),
                    }),
                );
                // The active books remain published while the market owner
                // validates an isolated standby prefix. Never manufacture a
                // source gap merely because a physical socket changed.
                let source_frames = candidate.buffered.iter()
                    .flat_map(|(frames, _)| frames.iter())
                    .map(|frame| serde_json::json!({
                        "raw_payload":frame.raw_payload,
                        "received_at":frame.received_at,
                    })).collect::<Vec<_>>();
                let condition = source_frames.iter().find_map(|frame|
                    frame["raw_payload"]["market"].as_str()).ok_or(TransportError::InvalidFrame)?;
                let received_at = candidate.buffered.back()
                    .and_then(|(frames, _)| frames.last())
                    .ok_or(TransportError::InvalidFrame)?.received_at;
                let checkpoint = RawTransportFrame {
                    raw_payload:serde_json::json!({
                        "event_type":"marketcow_connection_checkpoint",
                        "schema_version":"marketcow.polymarket.connection-checkpoint.v1",
                        "market":condition,"token_ids":tokens,
                        "rotation_id":rotation_id,"attempt":attempt,
                        "connection_id":candidate.pump.as_ref().map(|pump|pump.id.as_str()),
                        "source_frames":source_frames,
                    }), received_at,
                };
                let checkpoint_bytes = serde_json::to_vec(&checkpoint)
                    .map_err(|_|TransportError::InvalidFrame)?.len();
                if source_frames.len() > crate::MAX_CONNECTION_CHECKPOINT_FRAMES
                    || checkpoint_bytes > output.maximum_batch_bytes.min(crate::MAX_CONNECTION_CHECKPOINT_BYTES) {
                    rotation_log(rotation_id,"switch_candidate_rejected",serde_json::json!({
                        "attempt":attempt,"reason":"checkpoint_byte_budget",
                        "bytes":checkpoint_bytes,"limit":output.maximum_batch_bytes}));
                    continue;
                }
                let (receipt_tx, receipt_rx) = oneshot::channel();
                if !send_bounded_with_receipt(output, vec![checkpoint], checkpoint_bytes,
                    tokens, shutdown, None, Some(receipt_tx)).await? {
                    return Ok(ConnectionEnd::Shutdown);
                }
                // No timeout followed by resuming A: an unobserved successful
                // commit must never be followed by old-connection deltas.
                let committed = tokio::select! {
                    receipt=receipt_rx => receipt.unwrap_or(false),
                    _=shutdown.changed()=>return Ok(ConnectionEnd::Shutdown),
                };
                if !committed {
                    rotation_log(rotation_id,"switch_candidate_rejected",serde_json::json!({
                        "attempt":attempt,"reason":"owner_checkpoint_not_committed"}));
                    continue;
                }
                if candidate
                    .pump
                    .as_ref()
                    .is_none_or(|pump| pump.task.is_finished())
                {
                    rotation_log(
                        rotation_id,
                        "switch_candidate_stopped_during_replay",
                        serde_json::json!({"attempt":attempt,
                            "candidate_socket_id":candidate.pump.as_ref().map(|pump| pump.id.as_str())}),
                    );
                    return Err(TransportError::ReadFailed);
                }
                socket.close().await;
                socket = candidate.pump.take().expect("ready standby pump");
                if let Some(job) = preparing.take() {
                    job.abort();
                }
                preparing_replacement = false;
                diagnostic = transport_diagnostic::TransportDiagnostic::new(tokens);
                rotation = Rotation::fresh();
                waiting_for_standby_logged = false;
                rotation_log(
                    rotation_id,
                    "switch_completed",
                    serde_json::json!({
                        "attempt":attempt,"token_count":tokens.len(),
                        "new_connection_id":diagnostic.connection_id(),
                        "new_socket_id":socket.id,
                        "switch_elapsed_ms":switch_started.elapsed().as_millis(),
                    }),
                );
            } else if ready.is_some() {
                let candidate = ready.as_ref().expect("checked standby");
                let candidate_ages_ms = tokens
                    .iter()
                    .map(|token| {
                        (
                            token.clone(),
                            candidate.latest.get(token).map(|(age, at)| {
                                age.saturating_add(at.elapsed().as_millis() as i64)
                            }),
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                if !waiting_for_standby_logged {
                    rotation_log(
                        rotation_id,
                        "switch_waiting_for_fresh_standby",
                        serde_json::json!({
                            "attempt":attempt,"book_count":candidate.books.len(),
                            "candidate_ages_ms":candidate_ages_ms,
                            "replacement_preparing":preparing.is_some(),
                        }),
                    );
                    waiting_for_standby_logged = true;
                }
            } else if !waiting_for_standby_logged {
                rotation_log(
                    rotation_id,
                    "switch_waiting_for_standby",
                    serde_json::json!({
                        "attempt":attempt,"preparing":preparing.is_some(),
                        "active_age_ms":last_active_age_ms,
                    }),
                );
                waiting_for_standby_logged = true;
            }
        }
        let refresh_reason = ready
            .as_ref()
            .and_then(Standby::refresh_reason)
            .filter(|reason| {
                *reason != "standby_tenure_expired"
                    || last_attempt.elapsed() >= Duration::from_secs(10)
            });
        let standby_lagging_active = ready.as_ref().is_some_and(|candidate| {
            !candidate.fresh(tokens)
                && last_active_age_ms.is_some_and(|age| age < PREPARE_AGE_MS)
                && last_active_observation_at
                    .is_some_and(|at| at.elapsed() < Duration::from_secs(2))
        });
        if preparing.is_none()
            && last_attempt.elapsed() >= Duration::from_secs(1)
            && (ready.is_none()
                || refresh_reason.is_some()
                || standby_lagging_active
                || (rotation.prepare_confirmed
                    && ready
                        .as_ref()
                        .is_some_and(|candidate| !candidate.fresh(tokens))))
        {
            let cloned = config.clone();
            let subscribed = tokens.clone();
            preparing_replacement = ready.is_some();
            attempt += 1;
            rotation_log(
                rotation_id,
                "standby_prepare_started",
                serde_json::json!({
                    "attempt":attempt,"token_count":subscribed.len(),
                    "active_age_ms":last_active_age_ms,
                    "replacing_existing":preparing_replacement,
                    "reason":refresh_reason.unwrap_or(if preparing_replacement {
                        if standby_lagging_active { "standby_lagging_active" } else { "stale_standby" }
                    } else {
                        "no_standby"
                    }),
                    "physical_connection_limit":3,
                }),
            );
            preparing = Some(tokio::spawn(async move {
                prepare_standby(cloned, subscribed, rotation_id, attempt).await
            }));
            last_attempt = Instant::now();
            waiting_for_standby_logged = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn separate_socket_pumps_receive_without_owner_draining_the_other() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!(
            "ws://{}/ws/market",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut active = accept_async(stream).await.unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let mut standby = accept_async(stream).await.unwrap();
            active
                .send(Message::Text("{\"slot\":\"active\"}".into()))
                .await
                .unwrap();
            standby
                .send(Message::Text("{\"slot\":\"standby\"}".into()))
                .await
                .unwrap();
            let command = active.next().await.unwrap().unwrap().into_text().unwrap();
            assert_eq!(command, "membership_update");
            tokio::time::sleep(Duration::from_millis(250)).await;
        });
        let config = PolymarketTransportConfig::loopback(endpoint);
        let (active_socket, _) = connect_websocket(&config).await.unwrap();
        let (standby_socket, _) = connect_websocket(&config).await.unwrap();
        let mut active = SocketPump::start(active_socket, config.heartbeat_interval, 1);
        let mut standby = SocketPump::start(standby_socket, config.heartbeat_interval, 2);
        active
            .send(
                Message::Text("membership_update".into()),
                Duration::from_secs(1),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let backup_frame = standby.ingress.try_recv().unwrap();
        assert!(backup_frame.queued_at.elapsed() >= Duration::from_millis(50));
        assert_eq!(backup_frame.text, "{\"slot\":\"standby\"}");
        assert_eq!(
            active.ingress.try_recv().unwrap().text,
            "{\"slot\":\"active\"}"
        );
        active.close().await;
        standby.close().await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn socket_pump_rejects_overflow_instead_of_skipping_a_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!(
            "ws://{}/ws/market",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            for index in 0..=SOCKET_INGRESS_MESSAGES {
                if socket
                    .send(Message::Text(index.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        let config = PolymarketTransportConfig::loopback(endpoint);
        let (socket, _) = connect_websocket(&config).await.unwrap();
        let mut pump = SocketPump::start(socket, config.heartbeat_interval, 3);
        tokio::time::timeout(Duration::from_secs(3), &mut pump.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(pump.ingress.len(), SOCKET_INGRESS_MESSAGES);
        server.await.unwrap();
    }

    #[test]
    fn preparation_and_switch_need_real_second_observations_and_rearm() {
        let t = Instant::now();
        let mut r = Rotation::fresh();
        assert!(!r.observe(1_201, t));
        assert!(!r.prepare_confirmed);
        assert!(!r.observe(1_300, t + Duration::from_millis(199)));
        assert!(!r.prepare_confirmed);
        assert!(!r.observe(1_300, t + Duration::from_millis(200)));
        assert!(r.prepare_confirmed);
        assert!(!r.observe(1_501, t + Duration::from_millis(250)));
        assert!(!r.high_confirmed);
        assert!(r.observe(1_499, t + Duration::from_millis(500)));
        assert!(r.prepare_confirmed);
        assert!(!r.observe(1_700, t + Duration::from_millis(900)));
        assert!(r.prepare_confirmed); // a 1499 dip must not disarm preparation
        assert!(r.observe(1_100, t + Duration::from_millis(901)));
        assert!(!r.observe(1_600, t + Duration::from_millis(1_000)));
        assert!(!r.observe(1_600, t + Duration::from_millis(1_499)));
        assert!(!r.high_confirmed);
        assert!(!r.observe(1_600, t + Duration::from_millis(1_500)));
        assert!(r.high_confirmed);
    }

    #[test]
    fn switch_rearms_after_brief_dip_without_falling_below_prepare_age() {
        let t = Instant::now();
        let mut r = Rotation::fresh();
        assert!(!r.observe(1_600, t));
        assert!(r.observe(1_400, t + Duration::from_millis(100)));
        assert!(!r.observe(1_600, t + Duration::from_millis(200)));
        assert!(!r.observe(1_600, t + Duration::from_millis(699)));
        assert!(!r.high_confirmed);
        assert!(!r.observe(1_600, t + Duration::from_millis(700)));
        assert!(r.high_confirmed);
    }

    #[test]
    fn standby_requires_both_complete_books_and_each_recent_quote_clock() {
        let tokens = BTreeSet::from(["up".to_owned(), "down".to_owned()]);
        let mut latest = BTreeMap::new();
        let at = Utc::now();
        let timestamp = at.timestamp_millis().to_string();
        for token in &tokens {
            record_quote_clock(
                &serde_json::json!({"event_type":"book",
                "asset_id":token,"timestamp":timestamp}),
                at,
                &mut latest,
                &tokens,
            );
        }
        assert_eq!(latest.len(), 2);
        assert!(latest.values().all(|(age, _)| *age < SWITCH_AGE_MS));
        assert_eq!(
            batch_age(&[RawTransportFrame {
                raw_payload: serde_json::json!({
            "event_type":"last_trade_price","timestamp":timestamp}),
                received_at: at
            }]),
            None
        );
    }

    #[tokio::test]
    async fn hot_cutover_requires_owner_ack_without_publishing_a_gap() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!(
            "ws://{}/ws/market",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let books = || {
            serde_json::json!([
            {"event_type":"book","market":"condition","asset_id":"down","timestamp":Utc::now().timestamp_millis().to_string()},
            {"event_type":"book","market":"condition","asset_id":"up","timestamp":Utc::now().timestamp_millis().to_string()},
        ]).to_string()
        };
        let delayed = |age: i64| {
            serde_json::json!({
                "event_type":"price_change",
                "timestamp":(Utc::now().timestamp_millis()-age).to_string(),
                "price_changes":[{"asset_id":"up"}],
            })
            .to_string()
        };
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut active = accept_async(stream).await.unwrap();
            let _ = active.next().await.unwrap().unwrap();
            active.send(Message::Text(books().into())).await.unwrap();
            tokio::time::sleep(Duration::from_millis(40)).await;
            active
                .send(Message::Text(delayed(1_300).into()))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(220)).await;
            active
                .send(Message::Text(delayed(1_300).into()))
                .await
                .unwrap();
            let (stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut standby = accept_async(stream).await.unwrap();
            let _ = standby.next().await.unwrap().unwrap();
            standby.send(Message::Text(books().into())).await.unwrap();
            active
                .send(Message::Text(delayed(1_600).into()))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(520)).await;
            active
                .send(Message::Text(delayed(1_600).into()))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        let (output, mut input) = transport_output(16, 1_048_576, 1_048_576).unwrap();
        let (stop, mut shutdown) = watch::channel(false);
        let mut config = PolymarketTransportConfig::loopback(endpoint);
        config.hot_rotation = true;
        let mut tokens = BTreeSet::from(["down".to_owned(), "up".to_owned()]);
        let mut updates = None;
        let worker = tokio::spawn(async move {
            run_rotating_connection(&config, &mut tokens, &output, &mut shutdown, &mut updates)
                .await
        });
        let mut observed = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !observed.iter().any(|kind| kind == "marketcow_connection_checkpoint") {
                let mut batch = input.recv().await.unwrap();
                let receipt = batch.take_handoff_receipt();
                for frame in batch {
                    let kind = frame.raw_payload["event_type"].as_str().unwrap();
                    if kind == "marketcow_connection_checkpoint" {
                        assert_eq!(frame.raw_payload["source_frames"].as_array().unwrap().len(), 2);
                        assert!(receipt.is_some());
                    }
                    observed.push(kind.to_owned());
                }
                if let Some(receipt) = receipt {
                    receipt.send(true).unwrap();
                }
            }
        })
        .await
        .unwrap();
        assert!(!observed.iter().any(|kind| kind == "source_gap"));
        stop.send(true).unwrap();
        assert_eq!(worker.await.unwrap(), Ok(ConnectionEnd::Shutdown));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn third_connection_replaces_hot_standby_before_cutover() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!(
            "ws://{}/ws/market",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut active = accept_async(stream).await.unwrap();
            let _ = active.next().await.unwrap().unwrap();
            let now = Utc::now().timestamp_millis().to_string();
            active
                .send(Message::Text(
                    serde_json::json!([
                        {"event_type":"book","market":"condition","asset_id":"down","timestamp":now},
                        {"event_type":"book","market":"condition","asset_id":"up","timestamp":now},
                    ])
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();

            let (stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut first_standby = accept_async(stream).await.unwrap();
            let _ = first_standby.next().await.unwrap().unwrap();
            let now = Utc::now().timestamp_millis().to_string();
            first_standby
                .send(Message::Text(
                    serde_json::json!([
                        {"event_type":"book","market":"condition","asset_id":"down","timestamp":now,
                            "padding":"x".repeat(1024),"origin":"first"},
                        {"event_type":"book","market":"condition","asset_id":"up","timestamp":now,
                            "origin":"first"},
                    ])
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();

            let (stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut replacement = accept_async(stream).await.unwrap();
            let _ = replacement.next().await.unwrap().unwrap();
            let now = Utc::now().timestamp_millis().to_string();
            replacement.send(Message::Text(serde_json::json!([
                {"event_type":"book","market":"condition","asset_id":"down","timestamp":now,"origin":"replacement"},
                {"event_type":"book","market":"condition","asset_id":"up","timestamp":now,"origin":"replacement"},
            ]).to_string().into())).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            let delayed = |age: i64| {
                serde_json::json!({
                    "event_type":"price_change",
                    "timestamp":(Utc::now().timestamp_millis()-age).to_string(),
                    "price_changes":[{"asset_id":"up"}],
                })
                .to_string()
            };
            active
                .send(Message::Text(delayed(1_600).into()))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(550)).await;
            active
                .send(Message::Text(delayed(1_600).into()))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        let (output, mut input) = transport_output(16, 1_048_576, 1_048_576).unwrap();
        let (stop, mut shutdown) = watch::channel(false);
        let mut config = PolymarketTransportConfig::loopback(endpoint);
        config.hot_rotation = true;
        let mut tokens = BTreeSet::from(["down".to_owned(), "up".to_owned()]);
        let mut updates = None;
        let worker = tokio::spawn(async move {
            run_rotating_connection(&config, &mut tokens, &output, &mut shutdown, &mut updates)
                .await
        });
        let mut origins = Vec::new();
        tokio::time::timeout(Duration::from_secs(4), async {
            while origins.len() < 2 {
                let mut batch = input.recv().await.unwrap();
                let receipt = batch.take_handoff_receipt();
                for frame in batch {
                    assert_ne!(frame.raw_payload["event_type"], "source_gap");
                    if frame.raw_payload["event_type"] == "marketcow_connection_checkpoint" {
                        for source in frame.raw_payload["source_frames"].as_array().unwrap() {
                            origins.push(source["raw_payload"]["origin"].as_str().unwrap().to_owned());
                        }
                    }
                }
                if let Some(receipt) = receipt {
                    receipt.send(true).unwrap();
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(origins, ["replacement", "replacement"]);
        stop.send(true).unwrap();
        assert_eq!(worker.await.unwrap(), Ok(ConnectionEnd::Shutdown));
        server.await.unwrap();
    }
}
