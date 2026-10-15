//! BTCUSDT raw trades, owned by the existing BTC research service.
//!
//! A recent-trades REST snapshot is joined to an already-open raw trade WS by
//! trade ID. Neither an unbridged REST result nor a live socket alone is ready.
//! Raw evidence is sent to a bounded, independent archive; disk never gates
//! publication.
use anyhow::{Context, Result, bail, ensure};
use base64::Engine;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    sync::{RwLock, broadcast, mpsc},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

const WS_URL: &str = "wss://data-stream.binance.vision/ws/btcusdt@trade";
const REST_URL: &str = "https://data-api.binance.vision/api/v3/trades?symbol=BTCUSDT&limit=1000";
const SCHEMA: &str = "marketcow.btc-price-stream.v2";
const INSTRUMENT: &str = "BTCUSDT.BINANCE";
const MAX_TRADES: usize = 4096;
const SNAPSHOT_TRADES: usize = 256;
const MAX_RAW_FRAME_BYTES: usize = 64 * 1024;
const MAX_REST_BYTES: usize = 1024 * 1024;
const INGRESS_RECORDS: usize = 4096;
const ARCHIVE_RECORDS: usize = 1024;
const ARCHIVE_BYTES: usize = 16 * 1024 * 1024;
const ARCHIVE_DISK_BYTES: usize = 512 * 1024 * 1024;
const SOURCE_IDLE: Duration = Duration::from_secs(30);
const TWAP_SECONDS: i64 = 60;
const TWAP_HISTORY_SECONDS: usize = 720;
const MAX_OPENING_WINDOWS: usize = 12;
const MARKET_WINDOW_MS: i64 = 300_000;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct WindowIdentity {
    pub market_id: String,
    pub condition_id: String,
    pub window_id: String,
    pub rule_sha256: String,
    pub start_ms: i64,
    pub end_ms: i64,
}

#[derive(Clone)]
struct OpeningCandidate {
    start_ms: i64,
    price: Decimal,
    source_epoch: String,
    seconds_sha256: String,
}

#[derive(Clone)]
struct BoundOpening {
    identity: WindowIdentity,
    candidate: OpeningCandidate,
    evidence_sha256: String,
}

#[derive(Clone)]
struct PriceSecond {
    start_ms: i64,
    price_milliseconds: Decimal,
    covered_ms: i64,
}

#[derive(Clone, Default)]
struct TradeTwap {
    seconds: VecDeque<PriceSecond>,
    last_time_ms: Option<i64>,
    last_price: Option<Decimal>,
}

impl TradeTwap {
    fn observe(&mut self, time_ms: i64, price: Decimal) -> Result<()> {
        ensure!(
            time_ms > 0 && price > Decimal::ZERO,
            "invalid BTC trade TWAP input"
        );
        if let (Some(last_time), Some(last_price)) = (self.last_time_ms, self.last_price) {
            ensure!(time_ms >= last_time, "BTC trade source time regressed");
            let mut cursor =
                last_time.max(time_ms.saturating_sub(TWAP_HISTORY_SECONDS as i64 * 1000));
            while cursor < time_ms {
                let second = cursor.div_euclid(1000) * 1000;
                let end = time_ms.min(second + 1000);
                if self
                    .seconds
                    .back()
                    .is_none_or(|entry| entry.start_ms != second)
                {
                    self.seconds.push_back(PriceSecond {
                        start_ms: second,
                        price_milliseconds: Decimal::ZERO,
                        covered_ms: 0,
                    });
                }
                let entry = self.seconds.back_mut().expect("TWAP second inserted");
                let increment = last_price
                    .checked_mul(Decimal::from(end - cursor))
                    .context("BTC trade TWAP area overflow")?;
                entry.price_milliseconds = entry
                    .price_milliseconds
                    .checked_add(increment)
                    .context("BTC trade TWAP area overflow")?;
                entry.covered_ms += end - cursor;
                cursor = end;
            }
        }
        self.last_time_ms = Some(time_ms);
        self.last_price = Some(price);
        while self.seconds.len() > TWAP_HISTORY_SECONDS {
            self.seconds.pop_front();
        }
        Ok(())
    }

    fn completed_at(&self, end_ms: i64) -> Option<Decimal> {
        if end_ms.rem_euclid(1000) != 0 || self.last_time_ms? < end_ms {
            return None;
        }
        let start_ms = end_ms.checked_sub(TWAP_SECONDS * 1000)?;
        let mut expected = start_ms;
        let mut area = Decimal::ZERO;
        for entry in self
            .seconds
            .iter()
            .filter(|entry| entry.start_ms >= start_ms && entry.start_ms < end_ms)
        {
            if entry.start_ms != expected || entry.covered_ms != 1000 {
                return None;
            }
            area = area.checked_add(entry.price_milliseconds)?;
            expected += 1000;
        }
        (expected == end_ms)
            .then_some(area)?
            .checked_div(Decimal::from(TWAP_SECONDS * 1000))
    }

    fn latest_completed_end_ms(&self) -> Option<i64> {
        Some(self.last_time_ms?.div_euclid(1000) * 1000)
    }

    fn seconds_sha256_at(&self, end_ms: i64) -> Option<String> {
        self.completed_at(end_ms)?;
        let start_ms = end_ms.checked_sub(TWAP_SECONDS * 1000)?;
        let mut hasher = Sha256::new();
        hasher.update(b"marketcow.binance-opening-seconds.v1\n");
        for second in self
            .seconds
            .iter()
            .filter(|entry| entry.start_ms >= start_ms && entry.start_ms < end_ms)
        {
            hasher.update(
                format!(
                    "{}|{}|{}\n",
                    second.start_ms,
                    second.price_milliseconds.normalize(),
                    second.covered_ms
                )
                .as_bytes(),
            );
        }
        Some(hex::encode(hasher.finalize()))
    }
}

enum Ingress {
    Trade(Vec<u8>, String),
    Heartbeat(String),
}

#[derive(Clone)]
pub struct Service {
    state: Arc<RwLock<State>>,
    live: broadcast::Sender<Arc<Value>>,
    archive: mpsc::Sender<Archive>,
    archive_bytes: Arc<AtomicUsize>,
    archive_healthy: Arc<AtomicBool>,
    archive_dropped: Arc<AtomicU64>,
}

struct State {
    epoch: String,
    phase: &'static str,
    reason: String,
    serial: u64,
    sequence: u64,
    last_id: Option<u64>,
    trades: VecDeque<Value>,
    twap: TradeTwap,
    opening_candidates: VecDeque<OpeningCandidate>,
    bound_openings: VecDeque<BoundOpening>,
    checked_opening_boundary_ms: Option<i64>,
    snapshot_raw_sha256: Option<String>,
    snapshot_raw_base64: Option<String>,
    snapshot_published_at: Option<String>,
    last_received_at: Option<String>,
    last_transport_at: Option<String>,
}

struct Archive {
    epoch: String,
    trade_id: Option<u64>,
    received_at: String,
    raw: Vec<u8>,
}

#[derive(Deserialize)]
struct WsTrade {
    e: String,
    #[serde(rename = "E")]
    event_time_ms: i64,
    s: String,
    t: u64,
    p: String,
    q: String,
    #[serde(rename = "T")]
    trade_time_ms: i64,
    m: bool,
}

#[derive(Deserialize)]
struct RestTrade {
    id: u64,
    price: String,
    qty: String,
    time: i64,
    #[serde(rename = "isBuyerMaker")]
    is_buyer_maker: bool,
}

impl Service {
    pub fn start(root: PathBuf) -> Self {
        let (live, _) = broadcast::channel(1024);
        let (archive, receiver) = mpsc::channel(ARCHIVE_RECORDS);
        let archive_bytes = Arc::new(AtomicUsize::new(0));
        let archive_healthy = Arc::new(AtomicBool::new(true));
        let service = Self {
            state: Arc::new(RwLock::new(State::new())),
            live,
            archive,
            archive_bytes: archive_bytes.clone(),
            archive_healthy: archive_healthy.clone(),
            archive_dropped: Arc::new(AtomicU64::new(0)),
        };
        tokio::spawn(archive_task(root, receiver, archive_bytes, archive_healthy));
        let owner = service.clone();
        tokio::spawn(async move { owner.run().await });
        service
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Value>> {
        self.live.subscribe()
    }

    pub async fn stream_snapshot(&self) -> Value {
        let state = self.state.read().await;
        if state.phase != "live" {
            return unavailable(&state, &state.reason);
        }
        self.snapshot_frame(&state)
    }

    pub async fn status(&self) -> Value {
        let state = self.state.read().await;
        json!({"schema_version":"marketcow.binance-trade-status.v1",
            "source":"binance_spot_trade","instrument_id":INSTRUMENT,
            "source_epoch":state.epoch,"phase":state.phase,"reason":state.reason,
            "stream_serial":state.serial,"message_sequence":state.sequence,
            "last_trade_id":state.last_id,"last_received_at":state.last_received_at,
            "last_transport_at":state.last_transport_at,
            "latest_trade_time_ms":state.trades.back()
                .and_then(|trade| trade["trade_time_ms"].as_i64()),
            "freshness_policy":"consumer_explicit_max_trade_age_ms_required",
            "retained_trade_count":state.trades.len(),"retained_limit":MAX_TRADES,
            "budgets":{"snapshot_trades":SNAPSHOT_TRADES,
                "retained_trades":MAX_TRADES,"ingress_records":INGRESS_RECORDS,
                "raw_ws_frame_bytes":MAX_RAW_FRAME_BYTES,
                "rest_snapshot_bytes":MAX_REST_BYTES,
                "consumer_broadcast_frames":1024,
                "archive_records":ARCHIVE_RECORDS,
                "archive_queue_bytes":ARCHIVE_BYTES,
                "archive_disk_bytes":ARCHIVE_DISK_BYTES,
                "source_idle_seconds":SOURCE_IDLE.as_secs()},
            "archive_queued_bytes":self.archive_bytes.load(Ordering::Relaxed),
            "archive_writer_healthy":self.archive_healthy.load(Ordering::Relaxed),
            "archive_dropped_records":self.archive_dropped.load(Ordering::Relaxed),
            "as_of":Utc::now().to_rfc3339()})
    }

    /// Same-source, same-method opening and current prices for a market window.
    /// This is a comparison signal, never a substitute for the venue reference.
    pub async fn window_comparison(
        &self,
        identity: &WindowIdentity,
        official_price: Option<Decimal>,
    ) -> Value {
        let mut state = self.state.write().await;
        let start_ms = identity.start_ms;
        let end_ms = identity.end_ms;
        let valid_window = end_ms > start_ms && start_ms.rem_euclid(MARKET_WINDOW_MS) == 0;
        // Bind an already verified opening to the exact market/rule. Neither
        // a later source epoch nor a current-price outage can rewrite it.
        if valid_window
            && !state
                .bound_openings
                .iter()
                .any(|entry| entry.identity == *identity)
            && !state
                .bound_openings
                .iter()
                .any(|entry| entry.identity.start_ms == start_ms)
        {
            if let Some(candidate) = state
                .opening_candidates
                .iter()
                .find(|entry| entry.start_ms == start_ms)
                .cloned()
            {
                let mut hasher = Sha256::new();
                hasher.update(b"marketcow.binance-opening-binding.v1\n");
                for part in [
                    &identity.market_id,
                    &identity.condition_id,
                    &identity.window_id,
                    &identity.rule_sha256,
                    &candidate.source_epoch,
                    &candidate.seconds_sha256,
                ] {
                    hasher.update(part.as_bytes());
                    hasher.update(b"\n");
                }
                hasher.update(
                    format!("{}|{}|{}", start_ms, end_ms, candidate.price.normalize()).as_bytes(),
                );
                state.bound_openings.push_back(BoundOpening {
                    identity: identity.clone(),
                    candidate,
                    evidence_sha256: hex::encode(hasher.finalize()),
                });
                while state.bound_openings.len() > MAX_OPENING_WINDOWS {
                    state.bound_openings.pop_front();
                }
            }
        }
        let bound = state
            .bound_openings
            .iter()
            .find(|entry| entry.identity == *identity);
        let opening = bound.map(|entry| entry.candidate.price);
        let latest_end_ms = state
            .twap
            .latest_completed_end_ms()
            .map(|latest| latest.min(end_ms));
        let opening_available = valid_window && opening.is_some();
        let current = if valid_window && state.phase == "live" {
            latest_end_ms
                .filter(|end| *end >= start_ms)
                .and_then(|end| state.twap.completed_at(end))
        } else {
            None
        };
        let current_available = current.is_some();
        let available = opening_available && current_available;
        let mapped = opening.zip(current).zip(official_price).and_then(
            |((opening, current), official_price)| {
                (opening > Decimal::ZERO)
                    .then_some(current)
                    .and_then(|current| official_price.checked_mul(current))
                    .and_then(|scaled| scaled.checked_div(opening))
            },
        );
        json!({"schema_version":"marketcow.btc5m.binance-comparison.v2",
            "source":"binance_spot_trade","instrument_id":INSTRUMENT,
            "source_epoch":state.epoch,"available":available,
            "reason":if available {"ready"} else if !opening_available {
                "complete_60s_coverage_unavailable"
            } else if state.phase != "live" {"trade_source_unavailable"}
                else {"current_60s_coverage_unavailable"},
            "opening_available":opening_available,
            "opening_reason":if opening_available {"ready"} else if !valid_window {
                "invalid_market_window"
            } else {"complete_60s_coverage_unavailable"},
            "current_available":current_available,
            "current_reason":if current_available {"ready"} else if !valid_window {
                "invalid_market_window"
            } else if state.phase != "live" {"trade_source_unavailable"}
                else {"current_60s_coverage_unavailable"},
            "method":"last_trade_price_time_weighted_over_preceding_60000ms",
            "quote_asset":"USDT","official_method_exactly_reproduced":false,
            "carry_between_trades":true,"window_start_ms":start_ms,
            "window_end_ms":end_ms,
            "opening_interval_start_ms":start_ms.saturating_sub(60_000),
            "opening_interval_end_ms":start_ms,
            "opening_price_usdt":opening.map(|value| value.normalize().to_string()),
            "opening_source_epoch":bound.map(|entry| entry.candidate.source_epoch.clone()),
            "opening_seconds_sha256":bound.map(|entry| entry.candidate.seconds_sha256.clone()),
            "opening_evidence_sha256":bound.map(|entry| entry.evidence_sha256.clone()),
            "opening_covered_ms":bound.map(|_| TWAP_SECONDS * 1000),
            "opening_evidence_kind":"complete_trade_price_seconds_bound_to_market_identity",
            "current_interval_end_ms":latest_end_ms.filter(|_| current_available),
            "current_price_usdt":current.map(|value| value.normalize().to_string()),
            "mapped_official_scale_price":mapped.map(|value| value.normalize().to_string()),
            "mapping_status":if official_price.is_some() {"official_reference_available"}
                else {"official_reference_unavailable"},
            "direction":opening.zip(current).map(|(opening,current)|
                if current >= opening {"up"} else {"down"}),
            "latest_trade_time_ms":state.twap.last_time_ms,
            "latest_trade_age_ms_at_emit":state.twap.last_time_ms
                .map(|at| Utc::now().timestamp_millis() - at),
            "latest_trade_id":state.last_id,
            "freshness_policy":"consumer_explicit_max_trade_age_ms_required",
            "snapshot_is_not_execution_readiness":true,
            "observed_at":Utc::now().to_rfc3339()})
    }

    fn snapshot_frame(&self, state: &State) -> Value {
        json!({"schema_version":SCHEMA,"type":"snapshot",
            "source":"binance_spot_trade","instrument_id":INSTRUMENT,
            "source_epoch":state.epoch,"stream_serial":state.serial,
            "message_sequence":state.sequence,"source_sequence":state.last_id,
            "source_sequence_scope":"BTCUSDT trade_id",
            "source_continuity":"contiguous_trade_id_since_rest_ws_join",
            "snapshot_kind":"rest_recent_trades_joined_to_ws",
            "snapshot_raw_sha256":state.snapshot_raw_sha256,
            "snapshot_raw_base64":state.snapshot_raw_base64,
            "trades":state.trades.iter().rev().take(SNAPSHOT_TRADES).cloned()
                .collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>(),
            "first_snapshot_trade_id":state.trades.iter().rev().take(SNAPSHOT_TRADES)
                .last().and_then(|trade| trade["trade_id"].as_u64()),
            "snapshot_truncated":state.trades.len()>SNAPSHOT_TRADES,
            "retained_trade_count":state.trades.len(),
            "retained_limit":MAX_TRADES,
            "freshness_policy":"consumer_explicit_max_trade_age_ms_required",
            "latest_trade_age_ms_at_emit":state.trades.back()
                .and_then(|trade| trade["trade_time_ms"].as_i64())
                .map(|at| Utc::now().timestamp_millis() - at),
            "last_received_at":state.last_received_at,
            "archive_diagnostics":{
                "writer_healthy":self.archive_healthy.load(Ordering::Relaxed),
                "dropped_records":self.archive_dropped.load(Ordering::Relaxed)},
            "snapshot_joined_at":state.snapshot_published_at,
            "response_at":Utc::now().to_rfc3339()})
    }

    async fn run(self) {
        let mut backoff_seconds = 1u64;
        loop {
            self.invalidate("source_reconnecting").await;
            if let Err(error) = self.run_once().await {
                tracing::warn!(error=%error, "btc_trade_source_unavailable");
                let had_live_join = self.state.read().await.phase == "live";
                let detail = format!("{error:#}").to_lowercase();
                let reason = if detail.contains("id gap")
                    || detail.contains("id discontinuity")
                    || detail.contains("overlap missing")
                {
                    "source_trade_id_gap"
                } else if detail.contains("ingress capacity") {
                    "source_ingress_dropped"
                } else if error.downcast_ref::<serde_json::Error>().is_some()
                    || detail.contains("identity differs")
                    || detail.contains("duplicate trade")
                    || detail.contains("invalid")
                    || detail.contains("unexpected")
                    || detail.contains("wrong event")
                    || detail.contains("too large")
                {
                    "source_bad_frame"
                } else {
                    "source_read_failed"
                };
                self.invalidate(reason).await;
                backoff_seconds = if had_live_join {
                    1
                } else {
                    (backoff_seconds * 2).min(30)
                };
            }
            tokio::time::sleep(Duration::from_secs(backoff_seconds)).await;
        }
    }

    async fn run_once(&self) -> Result<()> {
        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(MAX_RAW_FRAME_BYTES);
        ws_config.max_frame_size = Some(MAX_RAW_FRAME_BYTES);
        let (socket, _) = tokio::time::timeout(
            Duration::from_secs(15),
            connect_async_with_config(WS_URL, Some(ws_config), false),
        )
        .await
        .context("BTC trade WS connect timeout")??;
        let (mut writer, mut reader) = socket.split();
        let (tx, mut rx) = mpsc::channel::<Ingress>(INGRESS_RECORDS);
        let overflow = Arc::new(AtomicBool::new(false));
        let reader_overflow = overflow.clone();
        let reader_task: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            while let Some(message) = reader.next().await {
                let message = message?;
                match message {
                    Message::Text(text) => {
                        let raw = text.as_bytes();
                        ensure!(
                            raw.len() <= MAX_RAW_FRAME_BYTES,
                            "BTC trade raw frame too large"
                        );
                        if tx
                            .try_send(Ingress::Trade(raw.to_vec(), Utc::now().to_rfc3339()))
                            .is_err()
                        {
                            reader_overflow.store(true, Ordering::Release);
                            bail!("BTC trade ingress capacity exhausted");
                        }
                    }
                    Message::Binary(_) => bail!("BTC trade unexpected binary frame"),
                    Message::Ping(payload) => {
                        writer.send(Message::Pong(payload)).await?;
                        let _ = tx.try_send(Ingress::Heartbeat(Utc::now().to_rfc3339()));
                    }
                    Message::Pong(_) => {
                        let _ = tx.try_send(Ingress::Heartbeat(Utc::now().to_rfc3339()));
                    }
                    Message::Close(_) => bail!("BTC trade WS closed"),
                    _ => {}
                }
            }
            bail!("BTC trade WS ended")
        });
        let outcome = self.join_and_consume(&mut rx, &overflow).await;
        reader_task.abort();
        outcome
    }

    async fn join_and_consume(
        &self,
        rx: &mut mpsc::Receiver<Ingress>,
        overflow: &AtomicBool,
    ) -> Result<()> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()?;
        let mut response = client.get(REST_URL).send().await?.error_for_status()?;
        ensure!(
            response
                .content_length()
                .is_none_or(|len| len <= MAX_REST_BYTES as u64),
            "BTC recent trades response too large"
        );
        let mut rest_raw = Vec::with_capacity(128 * 1024);
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                rest_raw.len().saturating_add(chunk.len()) <= MAX_REST_BYTES,
                "BTC recent trades response too large"
            );
            rest_raw.extend_from_slice(&chunk);
        }
        let rest_received_at = Utc::now().to_rfc3339();
        let rows: Vec<RestTrade> = serde_json::from_slice(&rest_raw)?;
        ensure!(
            !rows.is_empty() && rows.len() <= 1000,
            "BTC recent trades empty/oversized"
        );
        let raw_sha = hex::encode(Sha256::digest(&rest_raw));
        let mut trades = VecDeque::with_capacity(MAX_TRADES);
        let mut previous: Option<u64> = None;
        for (index, row) in rows.iter().enumerate() {
            if let Some(id) = previous {
                ensure!(
                    Some(row.id) == id.checked_add(1),
                    "BTC recent trades ID discontinuity"
                );
            }
            previous = Some(row.id);
            trades.push_back(rest_point(row, index, &raw_sha, &rest_received_at)?);
        }
        let rest_last = previous.context("BTC recent trades lack last ID")?;
        let epoch = self.state.read().await.epoch.clone();
        self.archive_raw(Archive {
            epoch,
            trade_id: None,
            received_at: rest_received_at,
            raw: rest_raw.to_vec(),
        });
        // The socket was opened before REST. A matching WS trade proves the
        // bounded REST snapshot and the live stream overlap. No overlap means
        // no authoritative bridge, so never advertise a ready snapshot.
        let mut joined = false;
        let mut last_id = rest_last;
        loop {
            ensure!(
                !overflow.load(Ordering::Acquire),
                "BTC trade ingress overflow"
            );
            let ingress = tokio::time::timeout(SOURCE_IDLE, rx.recv())
                .await
                .context("BTC trade WS idle")?
                .context("BTC trade WS reader stopped")?;
            let (raw, received_at) = match ingress {
                Ingress::Trade(raw, received_at) => (raw, received_at),
                Ingress::Heartbeat(received_at) => {
                    self.state.write().await.last_transport_at = Some(received_at);
                    continue;
                }
            };
            let trade: WsTrade = match serde_json::from_slice(&raw) {
                Ok(trade) => trade,
                Err(error) => {
                    let epoch = self.state.read().await.epoch.clone();
                    self.archive_raw(Archive {
                        epoch,
                        trade_id: None,
                        received_at,
                        raw,
                    });
                    return Err(error.into());
                }
            };
            if trade.e != "trade" || trade.s != "BTCUSDT" {
                let epoch = self.state.read().await.epoch.clone();
                self.archive_raw(Archive {
                    epoch,
                    trade_id: Some(trade.t),
                    received_at,
                    raw,
                });
                bail!("BTC trade wrong event or symbol");
            }
            let point = match ws_point(&trade, &raw, &received_at) {
                Ok(point) => point,
                Err(error) => {
                    let epoch = self.state.read().await.epoch.clone();
                    self.archive_raw(Archive {
                        epoch,
                        trade_id: Some(trade.t),
                        received_at,
                        raw,
                    });
                    return Err(error);
                }
            };
            if !joined {
                self.state.write().await.last_transport_at = Some(received_at.clone());
                if trade.t > rest_last {
                    let epoch = self.state.read().await.epoch.clone();
                    self.archive_raw(Archive {
                        epoch,
                        trade_id: Some(trade.t),
                        received_at,
                        raw,
                    });
                    bail!("BTC trade REST/WS overlap missing");
                }
                if trade.t == rest_last {
                    ensure!(
                        same_trade(&point, trades.back().unwrap()),
                        "BTC trade REST/WS identity differs"
                    );
                    joined = true;
                    self.commit_snapshot(
                        std::mem::take(&mut trades),
                        raw_sha.clone(),
                        rest_raw.to_vec(),
                    )
                    .await?;
                }
                let epoch = self.state.read().await.epoch.clone();
                self.archive_raw(Archive {
                    epoch,
                    trade_id: Some(trade.t),
                    received_at,
                    raw,
                });
                continue;
            }
            if trade.t <= last_id {
                let mut state = self.state.write().await;
                state.last_transport_at = Some(received_at.clone());
                let prior = state
                    .trades
                    .iter()
                    .rev()
                    .find(|candidate| candidate["trade_id"].as_u64() == Some(trade.t))
                    .context("BTC duplicate trade outside retained identity window")?;
                ensure!(
                    same_trade(&point, prior),
                    "BTC duplicate trade identity changed"
                );
                let epoch = state.epoch.clone();
                drop(state);
                self.archive_raw(Archive {
                    epoch,
                    trade_id: Some(trade.t),
                    received_at,
                    raw,
                });
                continue;
            }
            if trade.t != last_id.checked_add(1).context("BTC trade ID overflow")? {
                let epoch = self.state.read().await.epoch.clone();
                self.archive_raw(Archive {
                    epoch,
                    trade_id: Some(trade.t),
                    received_at,
                    raw,
                });
                bail!("BTC trade ID gap");
            }
            last_id = trade.t;
            self.commit_trade(point, raw, received_at).await?;
        }
    }

    async fn commit_snapshot(
        &self,
        trades: VecDeque<Value>,
        raw_sha: String,
        raw: Vec<u8>,
    ) -> Result<()> {
        let mut twap = TradeTwap::default();
        for trade in &trades {
            twap.observe(
                trade["trade_time_ms"]
                    .as_i64()
                    .context("trade time missing")?,
                Decimal::from_str(trade["price"].as_str().context("trade price missing")?)?,
            )?;
        }
        let frame = {
            let mut state = self.state.write().await;
            state.phase = "live";
            state.reason.clear();
            state.last_id = trades.back().and_then(|v| v["trade_id"].as_u64());
            state.last_received_at = trades
                .back()
                .and_then(|v| v["received_at"].as_str())
                .map(str::to_owned);
            state.snapshot_raw_sha256 = Some(raw_sha);
            state.snapshot_raw_base64 = Some(base64::engine::general_purpose::STANDARD.encode(raw));
            state.snapshot_published_at = Some(Utc::now().to_rfc3339());
            state.trades = trades;
            state.twap = twap;
            state.capture_opening_boundary();
            state.serial += 1;
            state.sequence += 1;
            self.snapshot_frame(&state)
        };
        let _ = self.live.send(Arc::new(frame));
        Ok(())
    }

    async fn commit_trade(&self, point: Value, raw: Vec<u8>, received_at: String) -> Result<()> {
        let (frame, epoch, id) = {
            let mut state = self.state.write().await;
            ensure!(state.phase == "live", "BTC trade source not joined");
            let id = point["trade_id"].as_u64().context("BTC trade ID missing")?;
            state.twap.observe(
                point["trade_time_ms"]
                    .as_i64()
                    .context("trade time missing")?,
                Decimal::from_str(point["price"].as_str().context("trade price missing")?)?,
            )?;
            state.capture_opening_boundary();
            state.serial = state
                .serial
                .checked_add(1)
                .context("BTC trade serial overflow")?;
            state.sequence = state
                .sequence
                .checked_add(1)
                .context("BTC trade sequence overflow")?;
            state.last_id = Some(id);
            state.last_received_at = Some(received_at.clone());
            state.last_transport_at = Some(received_at.clone());
            state.trades.push_back(point.clone());
            while state.trades.len() > MAX_TRADES {
                state.trades.pop_front();
            }
            (
                json!({"schema_version":SCHEMA,"type":"trade",
                "source":"binance_spot_trade","instrument_id":INSTRUMENT,
                "source_epoch":state.epoch,"stream_serial":state.serial,
                "message_sequence":state.sequence,"source_sequence":id,
                "source_sequence_scope":"BTCUSDT trade_id",
                "source_continuity":"contiguous_trade_id_since_rest_ws_join",
                "trade":point,"received_at":received_at,
                "latest_trade_age_ms_at_emit":Utc::now().timestamp_millis()
                    - point["trade_time_ms"].as_i64().unwrap_or(0),
                "published_at":Utc::now().to_rfc3339()}),
                state.epoch.clone(),
                id,
            )
        };
        let _ = self.live.send(Arc::new(frame));
        self.archive_raw(Archive {
            epoch,
            trade_id: Some(id),
            received_at,
            raw,
        });
        Ok(())
    }

    async fn invalidate(&self, reason: &str) {
        let frame = {
            let mut state = self.state.write().await;
            state.epoch = uuid::Uuid::new_v4().simple().to_string();
            state.phase = "unavailable";
            state.reason = reason.to_owned();
            state.sequence = 0;
            state.serial = state.serial.saturating_add(1);
            state.last_id = None;
            state.trades.clear();
            state.twap = TradeTwap::default();
            state.checked_opening_boundary_ms = None;
            state.snapshot_raw_sha256 = None;
            state.snapshot_raw_base64 = None;
            state.snapshot_published_at = None;
            state.last_received_at = None;
            state.last_transport_at = None;
            unavailable(&state, reason)
        };
        let _ = self.live.send(Arc::new(frame));
    }

    fn archive_raw(&self, record: Archive) {
        let size = record.raw.len();
        if !self.archive_healthy.load(Ordering::Acquire) || size > ARCHIVE_BYTES {
            self.archive_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self
            .archive_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(size).filter(|next| *next <= ARCHIVE_BYTES)
            })
            .is_err()
        {
            self.archive_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self.archive.try_send(record).is_err() {
            self.archive_bytes.fetch_sub(size, Ordering::AcqRel);
            self.archive_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl State {
    fn capture_opening_boundary(&mut self) {
        let Some(last_time_ms) = self.twap.last_time_ms else {
            return;
        };
        let boundary_ms = last_time_ms.div_euclid(MARKET_WINDOW_MS) * MARKET_WINDOW_MS;
        if self.checked_opening_boundary_ms == Some(boundary_ms) {
            return;
        }
        self.checked_opening_boundary_ms = Some(boundary_ms);
        let (Some(price), Some(seconds_sha256)) = (
            self.twap.completed_at(boundary_ms),
            self.twap.seconds_sha256_at(boundary_ms),
        ) else {
            return;
        };
        if self
            .opening_candidates
            .iter()
            .any(|entry| entry.start_ms == boundary_ms)
        {
            return;
        }
        self.opening_candidates.push_back(OpeningCandidate {
            start_ms: boundary_ms,
            price,
            source_epoch: self.epoch.clone(),
            seconds_sha256,
        });
        while self.opening_candidates.len() > MAX_OPENING_WINDOWS {
            self.opening_candidates.pop_front();
        }
    }

    fn new() -> Self {
        Self {
            epoch: uuid::Uuid::new_v4().simple().to_string(),
            phase: "unavailable",
            reason: "bootstrap_not_started".into(),
            serial: 0,
            sequence: 0,
            last_id: None,
            trades: VecDeque::new(),
            twap: TradeTwap::default(),
            opening_candidates: VecDeque::new(),
            bound_openings: VecDeque::new(),
            checked_opening_boundary_ms: None,
            snapshot_raw_sha256: None,
            snapshot_raw_base64: None,
            snapshot_published_at: None,
            last_received_at: None,
            last_transport_at: None,
        }
    }
}

fn unavailable(state: &State, reason: &str) -> Value {
    json!({"schema_version":SCHEMA,"type":"unavailable",
        "source":"binance_spot_trade","instrument_id":INSTRUMENT,
        "source_epoch":state.epoch,"stream_serial":state.serial,
        "message_sequence":state.sequence,"source_sequence":Value::Null,
        "reason":reason,"observed_at":Utc::now().to_rfc3339()})
}

fn validate_decimal(value: &str) -> Result<()> {
    let decimal = Decimal::from_str(value)?;
    ensure!(decimal > Decimal::ZERO, "BTC trade nonpositive decimal");
    Ok(())
}

fn rest_point(row: &RestTrade, index: usize, raw_sha: &str, received_at: &str) -> Result<Value> {
    validate_decimal(&row.price)?;
    validate_decimal(&row.qty)?;
    ensure!(row.time > 0, "BTC trade invalid time");
    Ok(
        json!({"trade_id":row.id,"price":row.price,"quantity":row.qty,
        "trade_time_ms":row.time,"event_time_ms":Value::Null,
        "buyer_is_maker":row.is_buyer_maker,"received_at":received_at,
        "source":"binance_spot_rest_recent","source_raw_sha256":raw_sha,
        "source_row_index":index}),
    )
}

fn ws_point(row: &WsTrade, raw: &[u8], received_at: &str) -> Result<Value> {
    validate_decimal(&row.p)?;
    validate_decimal(&row.q)?;
    ensure!(
        row.trade_time_ms > 0 && row.event_time_ms > 0,
        "BTC trade invalid time"
    );
    Ok(json!({"trade_id":row.t,"price":row.p,"quantity":row.q,
        "trade_time_ms":row.trade_time_ms,"event_time_ms":row.event_time_ms,
        "buyer_is_maker":row.m,"received_at":received_at,
        "source":"binance_spot_ws_trade",
        "source_raw_sha256":hex::encode(Sha256::digest(raw)),
        "raw_frame_base64":base64::engine::general_purpose::STANDARD.encode(raw)}))
}

fn same_trade(a: &Value, b: &Value) -> bool {
    a["trade_id"] == b["trade_id"]
        && a["price"] == b["price"]
        && a["quantity"] == b["quantity"]
        && a["trade_time_ms"] == b["trade_time_ms"]
        && a["buyer_is_maker"] == b["buyer_is_maker"]
        && (a["event_time_ms"].is_null()
            || b["event_time_ms"].is_null()
            || a["event_time_ms"] == b["event_time_ms"])
}

async fn archive_task(
    root: PathBuf,
    mut rx: mpsc::Receiver<Archive>,
    bytes: Arc<AtomicUsize>,
    healthy: Arc<AtomicBool>,
) {
    let result = async {
        let root = root.join("binance-trades");
        tokio::fs::create_dir_all(&root).await?;
        let path = root.join(format!("raw-{}.jsonl", uuid::Uuid::new_v4().simple()));
        let mut writer = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .await?;
        let mut stored = 0usize;
        let mut since_sync = 0usize;
        while let Some(record) = rx.recv().await {
            let encoded = serde_json::to_vec(&json!({
                "schema_version":"marketcow.binance-trade-raw.v1",
                "source_epoch":record.epoch,"trade_id":record.trade_id,
                "received_at":record.received_at,
                "raw_sha256":hex::encode(Sha256::digest(&record.raw)),
                "raw_base64":base64::engine::general_purpose::STANDARD.encode(&record.raw)}))?;
            ensure!(
                stored.saturating_add(encoded.len() + 1) <= ARCHIVE_DISK_BYTES,
                "BTC trade archive capacity exhausted"
            );
            writer.write_all(&encoded).await?;
            writer.write_all(b"\n").await?;
            stored += encoded.len() + 1;
            since_sync += 1;
            if since_sync >= 4096 {
                writer.sync_data().await?;
                since_sync = 0;
            }
            bytes.fetch_sub(record.raw.len(), Ordering::AcqRel);
        }
        writer.sync_data().await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if let Err(error) = result {
        tracing::warn!(error=%error, "btc_trade_archive_unavailable");
        healthy.store(false, Ordering::Release);
        bytes.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn binance_window_remains_available_without_official_reference() {
        let mut state = State::new();
        state.phase = "live";
        state.twap.observe(240_000, Decimal::from(100)).unwrap();
        state.twap.observe(270_000, Decimal::from(200)).unwrap();
        state.twap.observe(300_000, Decimal::from(300)).unwrap();
        state.twap.observe(360_000, Decimal::from(400)).unwrap();
        state.capture_opening_boundary();
        let identity = WindowIdentity {
            market_id: "market-a".into(),
            condition_id: "condition-a".into(),
            window_id: "window-a".into(),
            rule_sha256: "rule-a".into(),
            start_ms: 300_000,
            end_ms: 600_000,
        };
        let (live, _) = broadcast::channel(1);
        let (archive, _) = mpsc::channel(1);
        let service = Service {
            state: Arc::new(RwLock::new(state)),
            live,
            archive,
            archive_bytes: Arc::new(AtomicUsize::new(0)),
            archive_healthy: Arc::new(AtomicBool::new(true)),
            archive_dropped: Arc::new(AtomicU64::new(0)),
        };
        let without = service.window_comparison(&identity, None).await;
        assert_eq!(without["available"], true);
        assert_eq!(without["opening_price_usdt"], "150");
        assert_eq!(without["current_price_usdt"], "300");
        assert_eq!(without["direction"], "up");
        assert!(without["mapped_official_scale_price"].is_null());
        assert_eq!(without["mapping_status"], "official_reference_unavailable");
        let with = service
            .window_comparison(&identity, Some(Decimal::from(1000)))
            .await;
        assert_eq!(with["mapped_official_scale_price"], "2000");

        service.invalidate("source_reconnecting").await;
        let reconnecting = service.window_comparison(&identity, None).await;
        assert_eq!(reconnecting["opening_available"], true);
        assert_eq!(reconnecting["opening_price_usdt"], "150");
        assert_eq!(reconnecting["current_available"], false);
        assert_eq!(reconnecting["current_reason"], "trade_source_unavailable");
        assert_eq!(
            reconnecting["opening_source_epoch"],
            without["opening_source_epoch"]
        );
        assert_eq!(
            reconnecting["opening_evidence_sha256"],
            without["opening_evidence_sha256"]
        );

        let different_rule = WindowIdentity {
            rule_sha256: "rule-b".into(),
            ..identity
        };
        let rebound = service.window_comparison(&different_rule, None).await;
        assert_eq!(rebound["opening_available"], false);
        assert!(rebound["opening_evidence_sha256"].is_null());
    }

    #[tokio::test]
    async fn incomplete_opening_coverage_is_not_fabricated() {
        let mut state = State::new();
        state.phase = "live";
        state.twap.observe(250_001, Decimal::from(100)).unwrap();
        state.twap.observe(300_000, Decimal::from(110)).unwrap();
        state.capture_opening_boundary();
        let identity = WindowIdentity {
            market_id: "market-a".into(),
            condition_id: "condition-a".into(),
            window_id: "window-a".into(),
            rule_sha256: "rule-a".into(),
            start_ms: 300_000,
            end_ms: 600_000,
        };
        let (live, _) = broadcast::channel(1);
        let (archive, _) = mpsc::channel(1);
        let service = Service {
            state: Arc::new(RwLock::new(state)),
            live,
            archive,
            archive_bytes: Arc::new(AtomicUsize::new(0)),
            archive_healthy: Arc::new(AtomicBool::new(true)),
            archive_dropped: Arc::new(AtomicU64::new(0)),
        };
        let result = service.window_comparison(&identity, None).await;
        assert_eq!(result["opening_available"], false);
        assert!(result["opening_price_usdt"].is_null());
        assert_eq!(
            result["opening_reason"],
            "complete_60s_coverage_unavailable"
        );
    }

    #[test]
    fn same_second_updates_use_elapsed_time_not_trade_count_or_volume() {
        let mut twap = TradeTwap::default();
        twap.observe(1_000, Decimal::from(100)).unwrap();
        twap.observe(31_000, Decimal::from(200)).unwrap();
        twap.observe(61_000, Decimal::from(300)).unwrap();
        assert_eq!(twap.completed_at(61_000), Some(Decimal::from(150)));
        twap.observe(121_000, Decimal::from(400)).unwrap();
        assert_eq!(twap.completed_at(121_000), Some(Decimal::from(300)));
    }

    #[test]
    fn opening_requires_every_millisecond_of_source_coverage() {
        let mut twap = TradeTwap::default();
        twap.observe(1_001, Decimal::from(100)).unwrap();
        twap.observe(61_000, Decimal::from(100)).unwrap();
        assert_eq!(twap.completed_at(61_000), None);
        twap.observe(62_000, Decimal::from(100)).unwrap();
        assert_eq!(twap.completed_at(62_000), Some(Decimal::from(100)));
    }

    #[test]
    fn multiple_trades_at_same_millisecond_use_last_price_for_next_interval() {
        let mut twap = TradeTwap::default();
        twap.observe(1_000, Decimal::from(100)).unwrap();
        twap.observe(1_000, Decimal::from(200)).unwrap();
        twap.observe(61_000, Decimal::from(300)).unwrap();
        assert_eq!(twap.completed_at(61_000), Some(Decimal::from(200)));
    }

    #[test]
    fn same_millisecond_raw_trades_remain_distinct_by_trade_id() {
        let received = "2026-10-08T00:00:00Z";
        let first: WsTrade = serde_json::from_slice(first_raw()).unwrap();
        let second: WsTrade = serde_json::from_slice(second_raw()).unwrap();
        let first_point = ws_point(&first, first_raw(), received).unwrap();
        let second_point = ws_point(&second, second_raw(), received).unwrap();
        assert_eq!(first_point["trade_time_ms"], second_point["trade_time_ms"]);
        assert_ne!(first_point["trade_id"], second_point["trade_id"]);
        assert!(!same_trade(&first_point, &second_point));
    }

    fn first_raw() -> &'static [u8] {
        br#"{"e":"trade","E":1791417600000,"s":"BTCUSDT","t":41,"p":"60000.01000000","q":"0.00100000","T":1791417600000,"m":true}"#
    }

    fn second_raw() -> &'static [u8] {
        br#"{"e":"trade","E":1791417600000,"s":"BTCUSDT","t":42,"p":"60000.02000000","q":"0.00200000","T":1791417600000,"m":false}"#
    }

    #[test]
    fn rest_and_ws_overlap_must_match_economic_identity() {
        let rest = RestTrade {
            id: 41,
            price: "60000.01000000".into(),
            qty: "0.00100000".into(),
            time: 1791417600000,
            is_buyer_maker: true,
        };
        let rest_point = rest_point(&rest, 0, "raw-sha", "2026-10-08T00:00:00Z").unwrap();
        let ws: WsTrade = serde_json::from_slice(first_raw()).unwrap();
        let ws_point = ws_point(&ws, first_raw(), "2026-10-08T00:00:00Z").unwrap();
        assert!(same_trade(&rest_point, &ws_point));
        let mut changed = ws_point;
        changed["quantity"] = json!("0.00300000");
        assert!(!same_trade(&rest_point, &changed));
    }
}
