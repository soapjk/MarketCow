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
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{broadcast, mpsc};
use crate::source_snapshot::Published;
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
const DATABASE_RECORDS: usize = 8192;
const ARCHIVE_BYTES: usize = 16 * 1024 * 1024;
const DATABASE_BYTES: usize = 16 * 1024 * 1024;
const SOURCE_IDLE: Duration = Duration::from_secs(30);
const TWAP_SECONDS: i64 = 60;
const TWAP_HISTORY_SECONDS: usize = 720;
const MAX_OPENING_WINDOWS: usize = 12;
const MARKET_WINDOW_MS: i64 = 300_000;

#[path = "btc_trade_rest.rs"]
pub mod official_rest;
#[path = "btc_trade_history.rs"]
pub mod history;
#[path = "btc_trade_archive.rs"]
pub mod archive;
#[path = "btc_observation_db.rs"]
pub mod observation_db;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct WindowIdentity {
    pub market_id: String,
    pub condition_id: String,
    pub window_id: String,
    pub rule_sha256: String,
    pub start_ms: i64,
    pub end_ms: i64,
}

#[derive(Clone, Debug)]
struct OpeningCandidate {
    start_ms: i64,
    price: Decimal,
    source_epoch: Option<String>,
    source_epochs: Vec<String>,
    seconds_sha256: String,
    history_evidence_sha256: Option<String>,
    evidence_kind: &'static str,
}

#[derive(Clone)]
struct BoundOpening {
    identity: WindowIdentity,
    candidate: Arc<OpeningCandidate>,
    evidence_sha256: String,
}

fn opening_binding_preimage(identity: &WindowIdentity, candidate: &OpeningCandidate) -> Vec<u8> {
    // v2 is deliberately different from the former live-only v1: an opening
    // reconstructed across source epochs cannot claim one fabricated epoch.
    let mut bytes = b"marketcow.binance-opening-binding.v2\n".to_vec();
    for part in [
        identity.market_id.as_str(),
        identity.condition_id.as_str(),
        identity.window_id.as_str(),
        identity.rule_sha256.as_str(),
        candidate.seconds_sha256.as_str(),
        candidate.evidence_kind,
    ] {
        bytes.extend_from_slice(part.as_bytes());
        bytes.push(b'\n');
    }
    for epoch in &candidate.source_epochs {
        bytes.extend_from_slice(b"epoch:");
        bytes.extend_from_slice(epoch.as_bytes());
        bytes.push(b'\n');
    }
    if let Some(history_hash) = &candidate.history_evidence_sha256 {
        bytes.extend_from_slice(b"history:");
        bytes.extend_from_slice(history_hash.as_bytes());
        bytes.push(b'\n');
    }
    bytes.extend_from_slice(format!("{}|{}|{}", identity.start_ms,
        identity.end_ms, candidate.price.normalize()).as_bytes());
    bytes
}

#[derive(Clone)]
struct PriceSecond {
    start_ms: i64,
    price_milliseconds: Decimal,
    covered_ms: i64,
}

#[derive(Clone, Default)]
struct TradeTwap {
    seconds: im::Vector<PriceSecond>,
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

fn failure_class(error: &anyhow::Error) -> &'static str {
    let text = format!("{error:#}").to_lowercase();
    if text.contains("ingress") { "ingress" }
    else if text.contains("id gap") || text.contains("id discontinuity") || text.contains("overlap") { "trade_id_continuity" }
    else if text.contains("bootstrap") { "rest_bootstrap" }
    else if text.contains("connect") || text.contains("tls") { "connect_tls" }
    else if text.contains("idle") { "idle" }
    else if text.contains("closed") || text.contains("ended") { "ws_close" }
    else if text.contains("parse") || text.contains("invalid") || text.contains("binary") || text.contains("wrong event") { "parse_or_bad_frame" }
    else { "ws_io_or_consumer" }
}

async fn finish_reader(outcome: Result<()>, reader: tokio::task::JoinHandle<Result<()>>) -> Result<()> {
    // A closed channel means the reader has dropped its sender. Await that
    // completed task instead of destroying the only concrete transport error.
    let channel_closed = outcome.as_ref().err().is_some_and(|e|
        format!("{e:#}").contains("reader stopped"));
    let aborted = !reader.is_finished() && !channel_closed;
    if aborted { reader.abort(); }
    match reader.await {
        Ok(Err(reader_error)) => match outcome {
            Err(consumer_error) => Err(reader_error.context(format!("BTC reader failure; consumer outcome: {consumer_error:#}"))),
            Ok(()) => Err(reader_error.context("BTC reader failure")),
        },
        Err(join_error) if !join_error.is_cancelled() =>
            Err(anyhow::anyhow!("BTC reader task failed: {join_error}; consumer outcome: {outcome:?}")),
        _ => outcome,
    }
}

#[derive(Clone)]
pub struct Service {
    state: Arc<Published<State>>,
    live: broadcast::Sender<Arc<TradeEvent>>,
    recovery: mpsc::Sender<RecoveryResult>,
    window_bindings: Arc<arc_swap::ArcSwap<BTreeMap<i64,BoundOpening>>>,
    archive: mpsc::Sender<Archive>,
    archive_bytes: Arc<AtomicUsize>,
    archive_healthy: Arc<AtomicBool>,
    archive_dropped: Arc<AtomicU64>,
    archive_status: Arc<std::sync::Mutex<Value>>,
    database: Option<mpsc::Sender<Archive>>,
    database_bytes: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct State {
    epoch: String,
    phase: &'static str,
    reason: String,
    serial: u64,
    sequence: u64,
    last_id: Option<u64>,
    trades: im::Vector<Arc<TradePoint>>,
    twap: TradeTwap,
    opening_candidates: VecDeque<Arc<OpeningCandidate>>,
    opening_recovery_status: BTreeMap<i64, String>,
    checked_opening_boundary_ms: Option<i64>,
    snapshot_raw_sha256: Option<String>,
    snapshot_raw_base64: Option<RawBytes>,
    snapshot_published_at: Option<String>,
    last_received_at: Option<String>,
    last_transport_at: Option<String>,
}

struct RecoveryResult {
    boundary: i64,
    result: Result<OpeningCandidate>,
}

/// Immutable event metadata. JSON encoding belongs to the socket consumer.
pub struct TradeEvent {
    snapshot: Option<Arc<State>>,
    header:TradeHeader,
    point: Option<Arc<TradePoint>>,
    kind: &'static str,
    emitted_at: chrono::DateTime<Utc>,
}
struct TradeHeader {
    epoch:String,serial:u64,sequence:u64,last_id:Option<u64>,received_at:Option<String>,reason:String,
}

#[derive(Clone, Serialize)]
struct TradePoint {
    trade_id:u64,trade_time_ms:i64,event_time_ms:Option<i64>,
    price:String,quantity:String,buyer_is_maker:bool,received_at:String,
    source:&'static str,source_raw_sha256:String,
    #[serde(skip_serializing_if="Option::is_none")]
    source_row_index:Option<usize>,
    #[serde(skip_serializing_if="Option::is_none")]
    raw_frame_base64:Option<RawBytes>,
    #[serde(skip)]
    numeric_price:Decimal,
}
#[derive(Clone)]
struct RawBytes(Arc<RawBytesInner>);
struct RawBytesInner {
    raw:Arc<[u8]>,
    encoded:std::sync::OnceLock<String>,
}
impl RawBytes {
    fn new(raw:Arc<[u8]>)->Self {
        Self(Arc::new(RawBytesInner {raw,encoded:std::sync::OnceLock::new()}))
    }
}
impl Serialize for RawBytes {
    fn serialize<S:Serializer>(&self,s:S)->std::result::Result<S::Ok,S::Error> {
        s.serialize_str(self.0.encoded.get_or_init(||
            base64::engine::general_purpose::STANDARD.encode(&self.0.raw)))
    }
}

#[derive(Clone)]
struct Archive {
    epoch: String,
    trade_id: Option<u64>,
    received_at: String,
    raw: Arc<[u8]>,
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
    pub(crate) async fn history_seed(&self) -> Value {
        let state = self.state.load();
        json!({"source_epoch":state.epoch,"trades":state.trades,
            "snapshot_raw_base64":state.snapshot_raw_base64,
            "snapshot_raw_sha256":state.snapshot_raw_sha256,
            "archive_health":self.archive_status.lock().unwrap().clone()})
    }
    pub fn start(root: PathBuf) -> Self {
        let history_root = root.join("binance-trades");
        let (live, _) = broadcast::channel(1024);
        let (archive, receiver) = mpsc::channel(ARCHIVE_RECORDS);
        let (database, db_receiver) = mpsc::channel(DATABASE_RECORDS);
        let archive_bytes = Arc::new(AtomicUsize::new(0));
        let database_bytes = Arc::new(AtomicUsize::new(0));
        let archive_healthy = Arc::new(AtomicBool::new(true));
        let (recovery, recovered) = mpsc::channel(4);
        let service = Self {
            state: Arc::new(Published::new(State::new())),
            recovery,
            window_bindings:Arc::new(arc_swap::ArcSwap::from_pointee(BTreeMap::new())),
            live,
            archive,
            archive_bytes: archive_bytes.clone(),
            archive_healthy: archive_healthy.clone(),
            archive_dropped: Arc::new(AtomicU64::new(0)),
            archive_status: Arc::new(std::sync::Mutex::new(json!({"state":"starting","retention":"permanent_segments"}))),
            database:Some(database),database_bytes:database_bytes.clone(),
        };
        let status=service.archive_status.clone();
        let db_status=status.clone();
        let db_root=root.clone();
        tokio::task::spawn_blocking(move || database_task(db_root,db_receiver,database_bytes,db_status));
        tokio::task::spawn_blocking(move || archive_task(root, receiver, archive_bytes, archive_healthy, status));
        let owner = service.clone();
        tokio::spawn(async move { owner.run(recovered).await });
        let recovery_owner = service.clone();
        tokio::spawn(async move { recovery_owner.recover_opening_loop(history_root).await });
        service
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<TradeEvent>> {
        self.live.subscribe()
    }

    pub async fn stream_snapshot(&self) -> Value {
        let state = self.state.load();
        if state.phase != "live" {
            return unavailable(&state, &state.reason);
        }
        self.snapshot_frame(&state)
    }

    pub async fn status(&self) -> Value {
        let state = self.state.load();
        json!({"schema_version":"marketcow.binance-trade-status.v1",
            "source":"binance_spot_trade","instrument_id":INSTRUMENT,
            "source_epoch":state.epoch,"phase":state.phase,"reason":state.reason,
            "stream_serial":state.serial,"message_sequence":state.sequence,
            "last_trade_id":state.last_id,"last_received_at":state.last_received_at,
            "last_transport_at":state.last_transport_at,
            "latest_trade_time_ms":state.trades.back()
                .map(|trade| trade.trade_time_ms),
            "freshness_policy":"consumer_explicit_max_trade_age_ms_required",
            "retained_trade_count":state.trades.len(),"retained_limit":MAX_TRADES,
            "archive_health":self.archive_status.lock().unwrap().clone(),
            "budgets":{"snapshot_trades":SNAPSHOT_TRADES,
                "retained_trades":MAX_TRADES,"ingress_records":INGRESS_RECORDS,
                "raw_ws_frame_bytes":MAX_RAW_FRAME_BYTES,
                "rest_snapshot_bytes":MAX_REST_BYTES,
                "consumer_broadcast_frames":1024,
                "archive_records":ARCHIVE_RECORDS,
                "database_records":DATABASE_RECORDS,
                "database_queue_bytes":DATABASE_BYTES,
                "archive_queue_bytes":ARCHIVE_BYTES,
                "archive_segment_bytes":archive::SEGMENT_BYTES,
                "archive_total_bytes_limit":Value::Null,
                "source_idle_seconds":SOURCE_IDLE.as_secs()},
            "archive_queued_bytes":self.archive_bytes.load(Ordering::Relaxed),
            "database_queued_bytes":self.database_bytes.load(Ordering::Relaxed),
            "archive_writer_healthy":self.archive_healthy.load(Ordering::Relaxed),
            "archive_dropped_records":self.archive_dropped.load(Ordering::Relaxed),
            "as_of":Utc::now().to_rfc3339()})
    }

    async fn recover_opening_loop(&self, root: PathBuf) {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        let mut last_attempt: Option<(i64, tokio::time::Instant)> = None;
        loop {
            interval.tick().await;
            let snapshot = self.state.load();
            let Some(time_ms) = snapshot.twap.last_time_ms else { continue };
            if snapshot.phase != "live" { continue }
            let boundary = time_ms.div_euclid(MARKET_WINDOW_MS) * MARKET_WINDOW_MS;
            if snapshot.opening_candidates.iter().any(|entry| entry.start_ms == boundary)
                || last_attempt.is_some_and(|(at, when)|
                    at == boundary && when.elapsed() < Duration::from_secs(30)) { continue }
            last_attempt = Some((boundary, tokio::time::Instant::now()));
            let seed = self.history_seed().await;
            let history_root = root.clone();
            let result = tokio::task::spawn_blocking(move ||
                history::recover_opening(&history_root, &seed, boundary)).await
                .unwrap_or_else(|error| Err(anyhow::anyhow!("history_recovery_worker_failed: {error}")));
            if self.recovery.send(RecoveryResult { boundary, result }).await.is_err() { return }
        }
    }

    fn apply_recovery(&self, state: &mut State, recovered: RecoveryResult) {
        let boundary = recovered.boundary;
        match recovered.result {
            Ok(candidate) => {
                if !state.opening_candidates.iter().any(|entry| entry.start_ms == boundary) {
                    state.opening_candidates.push_back(Arc::new(candidate));
                    while state.opening_candidates.len() > MAX_OPENING_WINDOWS {
                        state.opening_candidates.pop_front();
                    }
                }
                state.opening_recovery_status.remove(&boundary);
            }
            Err(error) => {
                let detail = format!("{error:#}");
                let code = detail.split(':').next().unwrap_or("history_recovery_unavailable");
                state.opening_recovery_status.insert(boundary,
                    if code.starts_with("history_") { code.to_owned() }
                    else { "history_recovery_unavailable".to_owned() });
            }
        }
        state.opening_recovery_status.retain(|start, _|
            *start >= boundary - MARKET_WINDOW_MS * MAX_OPENING_WINDOWS as i64);
        self.state.publish(state.clone());
    }
    /// This is a comparison signal, never a substitute for the venue reference.
    pub async fn window_comparison(
        &self,
        identity: &WindowIdentity,
        official_price: Option<Decimal>,
    ) -> Value {
        self.window_comparison_at(identity, official_price, Utc::now()).await
    }

    async fn window_comparison_at(
        &self,
        identity: &WindowIdentity,
        official_price: Option<Decimal>,
        now: chrono::DateTime<Utc>,
    ) -> Value {
        let state = self.state.load();
        let start_ms = identity.start_ms;
        let end_ms = identity.end_ms;
        let valid_window = end_ms > start_ms && start_ms.rem_euclid(MARKET_WINDOW_MS) == 0;
        // Binding immutable window facts is independent of live trade state.
        // CAS makes concurrent first readers agree without taking an owner lock.
        if valid_window {
            let existing = self.window_bindings.load_full();
            if !existing.contains_key(&start_ms) {
                if let Some(candidate) = state.opening_candidates.iter()
                    .find(|entry|entry.start_ms==start_ms).cloned() {
                    let binding = BoundOpening {identity:identity.clone(),
                        evidence_sha256:hex::encode(Sha256::digest(opening_binding_preimage(identity,&candidate))),
                        candidate};
                    self.window_bindings.rcu(|old| {
                        let mut next=(**old).clone();
                        next.entry(start_ms).or_insert_with(||binding.clone());
                        while next.len()>MAX_OPENING_WINDOWS {
                            let first=*next.keys().next().expect("nonempty bindings");
                            next.remove(&first);
                        }
                        next
                    });
                }
            }
        }
        let bindings = self.window_bindings.load_full();
        let bound = bindings.get(&start_ms).filter(|entry|entry.identity==*identity);
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
        json!({"schema_version":"marketcow.btc5m.binance-comparison.v3",
            "source":"binance_spot_trade","instrument_id":INSTRUMENT,
            "source_epoch":state.epoch,"available":available,
            "reason":if available {"ready"} else if !opening_available {
                "complete_60s_coverage_unavailable"
            } else if state.phase != "live" {"trade_source_unavailable"}
                else {"current_60s_coverage_unavailable"},
            "opening_available":opening_available,
            "opening_reason":if opening_available {"ready".to_owned()} else if !valid_window {
                "invalid_market_window".to_owned()
            } else {state.opening_recovery_status.get(&start_ms).cloned()
                .unwrap_or_else(||"complete_60s_coverage_unavailable".to_owned())},
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
            "opening_source_epoch":bound.and_then(|entry| entry.candidate.source_epoch.clone()),
            "opening_source_epochs":bound.map(|entry| entry.candidate.source_epochs.clone()),
            "opening_seconds_sha256":bound.map(|entry| entry.candidate.seconds_sha256.clone()),
            "opening_history_evidence_sha256":bound.and_then(|entry| entry.candidate.history_evidence_sha256.clone()),
            "opening_evidence_sha256":bound.map(|entry| entry.evidence_sha256.clone()),
            "opening_covered_ms":bound.map(|_| TWAP_SECONDS * 1000),
            "opening_evidence_kind":bound.map(|entry| entry.candidate.evidence_kind),
            "current_interval_end_ms":latest_end_ms.filter(|_| current_available),
            "current_price_usdt":current.map(|value| value.normalize().to_string()),
            "mapped_official_scale_price":mapped.map(|value| value.normalize().to_string()),
            "mapping_status":if official_price.is_some() {"official_reference_available"}
                else {"official_reference_unavailable"},
            "direction":opening.zip(current).map(|(opening,current)|
                if current >= opening {"up"} else {"down"}),
            "latest_trade_time_ms":state.twap.last_time_ms,
            "latest_trade_age_ms_at_emit":state.twap.last_time_ms
                .map(|at| now.timestamp_millis() - at),
            "latest_trade_id":state.last_id,
            "freshness_policy":"consumer_explicit_max_trade_age_ms_required",
            "snapshot_is_not_execution_readiness":true,
            "observed_at":now.to_rfc3339()})
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
                .last().map(|trade| trade.trade_id),
            "snapshot_truncated":state.trades.len()>SNAPSHOT_TRADES,
            "retained_trade_count":state.trades.len(),
            "retained_limit":MAX_TRADES,
            "freshness_policy":"consumer_explicit_max_trade_age_ms_required",
            "latest_trade_age_ms_at_emit":state.trades.back()
                .map(|trade| trade.trade_time_ms)
                .map(|at| Utc::now().timestamp_millis() - at),
            "last_received_at":state.last_received_at,
            "archive_diagnostics":{
                "writer_healthy":self.archive_healthy.load(Ordering::Relaxed),
                "dropped_records":self.archive_dropped.load(Ordering::Relaxed),
                "health":self.archive_status.lock().unwrap().clone()},
            "snapshot_joined_at":state.snapshot_published_at,
            "response_at":Utc::now().to_rfc3339()})
    }

    async fn run(self, mut recovered: mpsc::Receiver<RecoveryResult>) {
        let mut state = (*self.state.load()).clone();
        let mut backoff_seconds = 1u64;
        let mut attempt = 0u64;
        loop {
            attempt = attempt.saturating_add(1);
            self.invalidate(&mut state, "source_reconnecting").await;
            self.diagnostic("connecting", attempt, None).await;
            if let Err(error) = self.run_once(&mut state, &mut recovered).await {
                let had_live_join = state.phase == "live";
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
                self.diagnostic(reason, attempt, Some(&error)).await;
                self.invalidate(&mut state, reason).await;
                backoff_seconds = if had_live_join {
                    1
                } else {
                    (backoff_seconds * 2).min(30)
                };
                self.diagnostic("retry_scheduled", attempt,
                    Some(&anyhow::anyhow!("backoff_seconds={backoff_seconds}"))).await;
            }
            tokio::time::sleep(Duration::from_secs(backoff_seconds)).await;
        }
    }

    async fn diagnostic(&self, stage: &str, attempt: u64, error: Option<&anyhow::Error>) {
        let state = self.state.load();
        // Fixed public endpoints only; no credentials or raw supplier frames.
        // emit is the existing bounded non-blocking diagnostic queue, independent
        // of EnvFilter and of raw archive health.
        marketcow_polymarket::diagnostic::emit(json!({
            "stage":"btc_trade_source_transition", "transition":stage,
            "attempt":attempt,"observed_at":Utc::now(),"source_epoch":state.epoch,
            "phase":state.phase,"last_trade_id":state.last_id,"stream_serial":state.serial,
            "failure_class":error.map(failure_class),
            "error_chain":error.map(|e| format!("{e:#}").chars().take(1024).collect::<String>())
        }).to_string());
    }

    async fn run_once(&self, state: &mut State, recovered: &mut mpsc::Receiver<RecoveryResult>) -> Result<()> {
        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(MAX_RAW_FRAME_BYTES);
        ws_config.max_frame_size = Some(MAX_RAW_FRAME_BYTES);
        let (socket, _) = tokio::time::timeout(
            Duration::from_secs(15),
            connect_async_with_config(WS_URL, Some(ws_config), false),
        )
        .await
        .context("BTC trade WS connect timeout")?
        .context("BTC trade WS connect/TLS failed")?;
        let (mut writer, mut reader) = socket.split();
        let (tx, mut rx) = mpsc::channel::<Ingress>(INGRESS_RECORDS);
        let overflow = Arc::new(AtomicBool::new(false));
        let reader_overflow = overflow.clone();
        let reader_task: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            while let Some(message) = reader.next().await {
                let message = message.context("BTC trade WS receive failed")?;
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
                        writer.send(Message::Pong(payload)).await.context("BTC trade WS pong write failed")?;
                        let _ = tx.try_send(Ingress::Heartbeat(Utc::now().to_rfc3339()));
                    }
                    Message::Pong(_) => {
                        let _ = tx.try_send(Ingress::Heartbeat(Utc::now().to_rfc3339()));
                    }
                    Message::Close(close) => bail!("BTC trade WS closed: {:?}", close.map(|c| (c.code, c.reason.chars().take(256).collect::<String>()))),
                    _ => {}
                }
            }
            bail!("BTC trade WS ended")
        });
        let outcome = self.join_and_consume(&mut rx, &overflow, state, recovered).await;
        finish_reader(outcome, reader_task).await
    }

    async fn join_and_consume(
        &self,
        rx: &mut mpsc::Receiver<Ingress>,
        overflow: &AtomicBool,
        state: &mut State,
        recovered: &mut mpsc::Receiver<RecoveryResult>,
    ) -> Result<()> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()?;
        let mut response = client.get(REST_URL).send().await
            .context("BTC trade REST bootstrap request failed")?
            .error_for_status().context("BTC trade REST bootstrap HTTP failed")?;
        ensure!(
            response
                .content_length()
                .is_none_or(|len| len <= MAX_REST_BYTES as u64),
            "BTC recent trades response too large"
        );
        let mut rest_raw = Vec::with_capacity(128 * 1024);
        while let Some(chunk) = response.chunk().await.context("BTC trade REST bootstrap body failed")? {
            ensure!(
                rest_raw.len().saturating_add(chunk.len()) <= MAX_REST_BYTES,
                "BTC recent trades response too large"
            );
            rest_raw.extend_from_slice(&chunk);
        }
        let rest_received_at = Utc::now().to_rfc3339();
        let rows: Vec<RestTrade> = serde_json::from_slice(&rest_raw).context("BTC trade REST bootstrap parse failed")?;
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
        let epoch = state.epoch.clone();
        self.archive_raw(Archive {
            epoch,
            trade_id: None,
            received_at: rest_received_at,
            raw: Arc::from(rest_raw.as_slice()),
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
            // One bounded recovery result per iteration; trade ingress stays ordered.
            if let Ok(result) = recovered.try_recv() { self.apply_recovery(state, result); }
            let ingress = tokio::time::timeout(SOURCE_IDLE, rx.recv())
                .await
                .context("BTC trade WS idle")?
                .context("BTC trade WS reader stopped")?;
            let (raw, received_at) = match ingress {
                Ingress::Trade(raw, received_at) => (raw, received_at),
                Ingress::Heartbeat(received_at) => {
                    state.last_transport_at = Some(received_at);
                    continue;
                }
            };
            let trade: WsTrade = match serde_json::from_slice(&raw) {
                Ok(trade) => trade,
                Err(error) => {
                    let epoch = state.epoch.clone();
                    self.archive_raw(Archive {
                        epoch,
                        trade_id: None,
                        received_at,
                        raw:raw.into(),
                    });
                    return Err(anyhow::Error::from(error).context("BTC trade WS parse failed"));
                }
            };
            if trade.e != "trade" || trade.s != "BTCUSDT" {
                let epoch = state.epoch.clone();
                self.archive_raw(Archive {
                    epoch,
                    trade_id: Some(trade.t),
                    received_at,
                    raw:raw.into(),
                });
                bail!("BTC trade wrong event or symbol");
            }
            let point = match ws_point(&trade, &raw, &received_at) {
                Ok(point) => point,
                Err(error) => {
                    let epoch = state.epoch.clone();
                    self.archive_raw(Archive {
                        epoch,
                        trade_id: Some(trade.t),
                        received_at,
                        raw:raw.into(),
                    });
                    return Err(error);
                }
            };
            if !joined {
                state.last_transport_at = Some(received_at.clone());
                if trade.t > rest_last {
                    let epoch = state.epoch.clone();
                    self.archive_raw(Archive {
                        epoch,
                        trade_id: Some(trade.t),
                        received_at,
                        raw:raw.into(),
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
                        state,
                        std::mem::take(&mut trades),
                        raw_sha.clone(),
                        rest_raw.to_vec(),
                    )
                    .await?;
                }
                let epoch = state.epoch.clone();
                self.archive_raw(Archive {
                    epoch,
                    trade_id: Some(trade.t),
                    received_at,
                    raw:raw.into(),
                });
                continue;
            }
            if trade.t <= last_id {
                state.last_transport_at = Some(received_at.clone());
                let prior = state
                    .trades
                    .iter()
                    .rev()
                    .find(|candidate| candidate.trade_id == trade.t)
                    .context("BTC duplicate trade outside retained identity window")?;
                ensure!(
                    same_trade(&point, prior),
                    "BTC duplicate trade identity changed"
                );
                let epoch = state.epoch.clone();
                self.archive_raw(Archive {
                    epoch,
                    trade_id: Some(trade.t),
                    received_at,
                    raw:raw.into(),
                });
                continue;
            }
            if trade.t != last_id.checked_add(1).context("BTC trade ID overflow")? {
                let epoch = state.epoch.clone();
                self.archive_raw(Archive {
                    epoch,
                    trade_id: Some(trade.t),
                    received_at,
                    raw:raw.into(),
                });
                bail!("BTC trade ID gap: expected={} received={}", last_id.saturating_add(1), trade.t);
            }
            last_id = trade.t;
            self.commit_trade(state, point, raw, received_at).await?;
        }
    }

    async fn commit_snapshot(
        &self, state: &mut State, trades: VecDeque<Arc<TradePoint>>, raw_sha: String, raw: Vec<u8>,
    ) -> Result<()> {
        let mut twap = TradeTwap::default();
        for trade in &trades {
            twap.observe(trade.trade_time_ms, trade.numeric_price)?;
        }
        state.phase = "live";
        state.reason.clear();
        state.last_id = trades.back().map(|v|v.trade_id);
        state.last_received_at = trades.back().map(|v|v.received_at.clone());
        state.snapshot_raw_sha256 = Some(raw_sha);
        state.snapshot_raw_base64 = Some(RawBytes::new(raw.into()));
        state.snapshot_published_at = Some(Utc::now().to_rfc3339());
        state.trades = trades.into_iter().collect();
        state.twap = twap;
        state.capture_opening_boundary();
        state.serial = state.serial.checked_add(1).context("BTC trade serial overflow")?;
        state.sequence = state.sequence.checked_add(1).context("BTC trade sequence overflow")?;
        self.publish_event(state, None, "snapshot");
        self.diagnostic("live_snapshot_published", 0, None).await;
        Ok(())
    }

    async fn commit_trade(&self, state: &mut State, point: Arc<TradePoint>, raw: Vec<u8>, received_at: String) -> Result<()> {
        ensure!(state.phase == "live", "BTC trade source not joined");
        let id = point.trade_id;
        state.twap.observe(point.trade_time_ms, point.numeric_price)?;
        state.capture_opening_boundary();
        state.serial = state.serial.checked_add(1).context("BTC trade serial overflow")?;
        state.sequence = state.sequence.checked_add(1).context("BTC trade sequence overflow")?;
        state.last_id = Some(id);
        state.last_received_at = Some(received_at.clone());
        state.last_transport_at = Some(received_at.clone());
        state.trades.push_back(point.clone());
        while state.trades.len() > MAX_TRADES { state.trades.pop_front(); }
        self.publish_event(state, Some(point), "trade");
        self.archive_raw(Archive { epoch:state.epoch.clone(),trade_id:Some(id),received_at,raw:raw.into() });
        Ok(())
    }

    fn publish_event(&self, state: &State, point: Option<Arc<TradePoint>>, kind: &'static str) {
        self.state.publish(state.clone());
        let event = TradeEvent {snapshot:if kind=="snapshot" {Some(self.state.load())}else{None},
            header:TradeHeader {epoch:state.epoch.clone(),serial:state.serial,sequence:state.sequence,
                last_id:state.last_id,received_at:state.last_received_at.clone(),reason:state.reason.clone()},
            point,kind,emitted_at:Utc::now()};
        let _ = self.live.send(Arc::new(event));
    }

    pub fn event_frame(&self, event: &TradeEvent) -> Value {
        match event.kind {
            "snapshot" => self.snapshot_frame(event.snapshot.as_deref().expect("snapshot event state")),
            "unavailable" => json!({"schema_version":SCHEMA,"type":"unavailable",
                "source":"binance_spot_trade","instrument_id":INSTRUMENT,
                "source_epoch":event.header.epoch,"stream_serial":event.header.serial,
                "message_sequence":event.header.sequence,"source_sequence":Value::Null,
                "reason":event.header.reason,"observed_at":event.emitted_at.to_rfc3339()}),
            _ => {
                let header = &event.header;
                let point = event.point.as_ref().expect("trade event point");
                json!({"schema_version":SCHEMA,"type":"trade","source":"binance_spot_trade",
                    "instrument_id":INSTRUMENT,"source_epoch":header.epoch,
                    "stream_serial":header.serial,"message_sequence":header.sequence,
                    "source_sequence":header.last_id,"source_sequence_scope":"BTCUSDT trade_id",
                    "source_continuity":"contiguous_trade_id_since_rest_ws_join","trade":point,
                    "received_at":header.received_at,
                    "latest_trade_age_ms_at_emit":event.emitted_at.timestamp_millis()
                        - point.trade_time_ms,
                    "published_at":event.emitted_at.to_rfc3339()})
            }
        }
    }

    async fn invalidate(&self, state: &mut State, reason: &str) {
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
        self.publish_event(state, None, "unavailable");
    }

    fn archive_raw(&self, record: Archive) {
        self.queue_database(&record);
        let size = record.raw.len();
        if size > ARCHIVE_BYTES {
            self.archive_drop("archive_record_budget", &record);
            return;
        }
        if self
            .archive_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(size).filter(|next| *next <= ARCHIVE_BYTES)
            })
            .is_err()
        {
            self.archive_drop("archive_queue_bytes_exhausted", &record);
            return;
        }
        if let Err(error)=self.archive.try_send(record) {
            self.archive_bytes.fetch_sub(size, Ordering::AcqRel);
            let reason=if matches!(&error,mpsc::error::TrySendError::Full(_)){"archive_queue_records_exhausted"}else{"archive_worker_closed"};
            self.archive_drop(reason,&error.into_inner());
        }
    }
    fn queue_database(&self,record:&Archive) {
        let Some(sender)=&self.database else {return};
        let size=record.raw.len();
        if size>DATABASE_BYTES || self.database_bytes.fetch_update(Ordering::AcqRel,Ordering::Acquire,
            |used|used.checked_add(size).filter(|total|*total<=DATABASE_BYTES)).is_err() {
            self.database_drop("database_queue_bytes_exhausted",record);return;
        }
        if let Err(error)=sender.try_send(record.clone()) {
            self.database_bytes.fetch_sub(size,Ordering::AcqRel);
            self.database_drop(if matches!(error,mpsc::error::TrySendError::Full(_))
                {"database_queue_records_exhausted"}else{"database_worker_closed"},record);
        }
    }
    fn database_drop(&self,reason:&str,record:&Archive) {
        let mut status=self.archive_status.lock().unwrap();
        let count=status["database"]["dropped_records"].as_u64().unwrap_or(0)+1;
        status["database"]["dropped_records"]=count.into();
        status["database"]["last_drop_reason"]=reason.into();
        status["database"]["last_drop_trade_id"]=json!(record.trade_id);
        status["database"]["last_drop_at"]=json!(Utc::now());
        if count==1 || count.is_power_of_two() {marketcow_polymarket::diagnostic::emit(json!({
            "stage":"btc_observation_db_dropped","reason":reason,"trade_id":record.trade_id,
            "dropped_records":count,"observed_at":Utc::now()}).to_string());}
    }
    fn archive_drop(&self,reason:&str,record:&Archive) {
        let count=self.archive_dropped.fetch_add(1,Ordering::Relaxed)+1;
        let mut status=self.archive_status.lock().unwrap();
        status["dropped_records"]=count.into();
        status["last_drop_at"]=json!(Utc::now());status["last_drop_trade_id"]=json!(record.trade_id);
        status["last_drop_reason"]=reason.into();
        if count==1 || count.is_power_of_two() {
            marketcow_polymarket::diagnostic::emit(json!({"stage":"btc_archive_record_dropped","reason":reason,"observed_at":Utc::now(),"trade_id":record.trade_id,"health":*status}).to_string());
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
        self.opening_candidates.push_back(Arc::new(OpeningCandidate {
            start_ms: boundary_ms,
            price,
            source_epoch: Some(self.epoch.clone()),
            source_epochs: vec![self.epoch.clone()],
            seconds_sha256,
            history_evidence_sha256: None,
            evidence_kind: "complete_trade_price_seconds_bound_to_market_identity",
        }));
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
            trades: im::Vector::new(),
            twap: TradeTwap::default(),
            opening_candidates: VecDeque::new(),
            opening_recovery_status: BTreeMap::new(),
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

fn rest_point(row: &RestTrade, index: usize, raw_sha: &str, received_at: &str) -> Result<Arc<TradePoint>> {
    validate_decimal(&row.price)?;
    validate_decimal(&row.qty)?;
    ensure!(row.time > 0, "BTC trade invalid time");
    Ok(Arc::new(TradePoint {trade_id:row.id,price:row.price.clone(),quantity:row.qty.clone(),
        trade_time_ms:row.time,event_time_ms:None,buyer_is_maker:row.is_buyer_maker,
        received_at:received_at.into(),source:"binance_spot_rest_recent",
        source_raw_sha256:raw_sha.into(),source_row_index:Some(index),raw_frame_base64:None,
        numeric_price:row.price.parse()?}))
}

fn ws_point(row: &WsTrade, raw: &[u8], received_at: &str) -> Result<Arc<TradePoint>> {
    validate_decimal(&row.p)?;
    validate_decimal(&row.q)?;
    ensure!(row.trade_time_ms > 0 && row.event_time_ms > 0, "BTC trade invalid time");
    Ok(Arc::new(TradePoint {trade_id:row.t,price:row.p.clone(),quantity:row.q.clone(),
        trade_time_ms:row.trade_time_ms,event_time_ms:Some(row.event_time_ms),
        buyer_is_maker:row.m,received_at:received_at.into(),source:"binance_spot_ws_trade",
        source_raw_sha256:hex::encode(Sha256::digest(raw)),source_row_index:None,
        raw_frame_base64:Some(RawBytes::new(Arc::from(raw))),numeric_price:row.p.parse()?}))
}

fn same_trade(a: &TradePoint, b: &TradePoint) -> bool {
    a.trade_id == b.trade_id && a.price == b.price && a.quantity == b.quantity
        && a.trade_time_ms == b.trade_time_ms && a.buyer_is_maker == b.buyer_is_maker
        && (a.event_time_ms.is_none() || b.event_time_ms.is_none() || a.event_time_ms == b.event_time_ms)
}

fn database_task(root:PathBuf,mut rx:mpsc::Receiver<Archive>,bytes:Arc<AtomicUsize>,
    status:Arc<std::sync::Mutex<Value>>) {
    let path=root.join("binance-trades/trades.sqlite3");
    let mut db=observation_db::open(&path).ok();
    while let Some(first)=rx.blocking_recv() {
        let mut batch=vec![first];
        let mut batch_bytes=batch[0].raw.len();
        std::thread::sleep(Duration::from_millis(3));
        while batch.len()<64 && batch_bytes<256*1024 {
            match rx.try_recv() {
                Ok(next)=>{batch_bytes+=next.raw.len();batch.push(next)},
                Err(_)=>break,
            }
        }
        let last=batch.last().unwrap();
        if db.is_none(){db=observation_db::open(&path).ok();}
        let frames=batch.iter().map(|record|(record.raw.clone(),record.epoch.clone(),
            record.received_at.clone())).collect::<Vec<_>>();
        let result=db.as_mut().context("database_open_failed")
            .and_then(|db|observation_db::ingest_binance_batch(db,&frames));
        bytes.fetch_sub(batch_bytes,Ordering::AcqRel);
        match result {
            Ok(_) => {
                let mut health=status.lock().unwrap();
                health["database"]["state"]="writing".into();
                health["database"]["last_success_at"]=json!(Utc::now());
                health["database"]["last_success_trade_id"]=json!(last.trade_id);
                health["database"]["reason"]=Value::Null;
                health["database"]["failed_at"]=Value::Null;
            }
            Err(error) => {
                if let Some(open)=db.as_ref(){observation_db::mark_error(open,"binance_spot_trade",&format!("{error:#}"));}
                let mut health=status.lock().unwrap();
                health["database"]["state"]="failed_retrying".into();
                health["database"]["failed_at"]=json!(Utc::now());
                health["database"]["reason"]=format!("{error:#}").chars().take(256).collect::<String>().into();
                health["database"]["last_attempt_trade_id"]=json!(last.trade_id);
                health["database"]["failed_batch_records"]=json!(batch.len());
                marketcow_polymarket::diagnostic::emit(json!({"stage":"btc_observation_db_failed",
                    "health":health["database"]}).to_string());
                db=None;
                std::thread::sleep(Duration::from_secs(2));
            }
        }
    }
}

fn archive_task(
    root: PathBuf,
    mut rx: mpsc::Receiver<Archive>,
    bytes: Arc<AtomicUsize>,
    healthy: Arc<AtomicBool>,
    status: Arc<std::sync::Mutex<Value>>,
) {
    let root=root.join("binance-trades");
    if let Ok(file)=std::fs::File::open(root.join("archive-health.json")) {
        use std::io::Read;
        let mut raw=Vec::new();
        if file.take(65536).read_to_end(&mut raw).is_ok() {
            if let Ok(mut previous)=serde_json::from_slice::<Value>(&raw) {
                if previous.is_object(){previous["state"]="starting".into();*status.lock().unwrap()=previous;}
            }
        }
    }
    if let Ok(db)=archive::open_index(&root) {
        let _=db.execute("UPDATE segments SET closed=1 WHERE closed=0",[]);
        let _=db.execute("INSERT OR REPLACE INTO index_health VALUES ('ready','0')",[]);
    }
    // Index discovery runs once on its own blocking worker, not per request or
    // on the live owner. Existing files remain immutable.
    let index_root=root.clone();
    std::thread::spawn(move || {
        loop {
            let result=archive::open_index(&index_root).and_then(|db|archive::index_existing(&index_root,&db));
            match result {
                Ok(())=>{marketcow_polymarket::diagnostic::emit(json!({"stage":"btc_archive_index_ready","observed_at":Utc::now()}).to_string());break},
                Err(error)=>{marketcow_polymarket::diagnostic::emit(json!({"stage":"btc_archive_index_failed","failure":archive::failure(&error)}).to_string());std::thread::sleep(Duration::from_secs(10));}
            }
        }
    });
    let mut writer=None;
    let mut written_records=0u64;
    while let Some(record)=rx.blocking_recv() {
        let raw_size=record.raw.len();
        let result=(|| -> Result<()> {
            if writer.is_none() {
                writer=Some(archive::Writer::open(root.clone(),"raw",true,archive::SEGMENT_BYTES)?);
            }
            let encoded = serde_json::to_vec(&json!({
                "schema_version":"marketcow.binance-trade-raw.v1",
                "source_epoch":record.epoch,"trade_id":record.trade_id,
                "received_at":record.received_at,
                "raw_sha256":hex::encode(Sha256::digest(&record.raw)),
                "raw_base64":base64::engine::general_purpose::STANDARD.encode(&record.raw)}))?;
            let raw:Value=serde_json::from_slice(&record.raw)?;
            let last=raw.as_array().and_then(|r|r.last()).unwrap_or(&raw);
            let id=last["t"].as_u64().or_else(||last["id"].as_u64());
            let t=last["T"].as_i64().or_else(||last["time"].as_i64());
            writer.as_mut().unwrap().append(&encoded,id,t)?;
            written_records+=1;
            let was_healthy=healthy.swap(true,Ordering::AcqRel);
            let mut health=status.lock().unwrap();
            health["state"]="writing".into();health["last_success_at"]=json!(Utc::now());
            health["last_success_trade_id"]=json!(id);health["last_success_source_time_ms"]=json!(t);
            if !was_healthy {
                health["recovered_at"]=json!(Utc::now());
                marketcow_polymarket::diagnostic::emit(json!({"stage":"btc_archive_recovered","health":*health}).to_string());
            }
            let health_snapshot=health.clone();drop(health);
            if written_records%256==0 || !was_healthy {
                if let Err(error)=archive::save_health(&root,&health_snapshot) {marketcow_polymarket::diagnostic::emit(json!({"stage":"btc_archive_health_persist_failed","failure":archive::failure(&error)}).to_string());}
            }
            Ok(())
        })();
        bytes.fetch_sub(raw_size,Ordering::AcqRel);
        if let Err(error)=result {
            healthy.store(false,Ordering::Release);writer=None;
            let failure=archive::failure(&error);
            let mut health=status.lock().unwrap();health["state"]="failed_retrying".into();
            health["last_failure"]=failure.clone();health["failed_record_trade_id"]=json!(record.trade_id);
            if health["first_failure"].is_null(){health["first_failure"]=failure.clone();}
            health["failed_write_records"]=json!(health["failed_write_records"].as_u64().unwrap_or(0)+1);
            marketcow_polymarket::diagnostic::emit(json!({"stage":"btc_archive_failed","failure":failure,"health":*health}).to_string());
            let health_snapshot=health.clone();drop(health);
            let _=archive::save_health(&root,&health_snapshot);
            // Memory remains bounded; live drops are counted while archive IO
            // retries. Never truncate a partly written old segment.
            std::thread::sleep(Duration::from_secs(2));
        }
    }
    if let Some(writer)=writer {if let Err(error)=writer.finish(){healthy.store(false,Ordering::Release);marketcow_polymarket::diagnostic::emit(json!({"stage":"btc_archive_finish_failed","failure":archive::failure(&error)}).to_string());}}
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn test_service(state:State)->Service {
        let (live,_)=broadcast::channel(16);
        let (archive,_)=mpsc::channel(16);
        Service {state:Arc::new(Published::new(state)),live,archive,
            recovery:mpsc::channel(4).0,
            window_bindings:Arc::new(arc_swap::ArcSwap::from_pointee(BTreeMap::new())),
            archive_bytes:Arc::new(AtomicUsize::new(0)),archive_healthy:Arc::new(AtomicBool::new(true)),
            archive_dropped:Arc::new(AtomicU64::new(0)),archive_status:Arc::new(std::sync::Mutex::new(Value::Null)),
            database:None,database_bytes:Arc::new(AtomicUsize::new(0))}
    }

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
        let service = test_service(state);
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

        service.invalidate(&mut (*service.state.load()).clone(), "source_reconnecting").await;
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
        let service = test_service(state);
        let result = service.window_comparison(&identity, None).await;
        assert_eq!(result["opening_available"], false);
        assert!(result["opening_price_usdt"].is_null());
        assert_eq!(
            result["opening_reason"],
            "complete_60s_coverage_unavailable"
        );
    }

    #[tokio::test]
    async fn recovered_cross_epoch_opening_has_explicit_provenance_and_no_current_claim() {
        let mut state = State::new();
        state.phase = "live";
        state.opening_candidates.push_back(Arc::new(OpeningCandidate {
            start_ms: 300_000,
            price: Decimal::from(150),
            source_epoch: None,
            source_epochs: vec!["old-epoch".into(), "new-epoch".into()],
            seconds_sha256: "verified-seconds".into(),
            history_evidence_sha256: Some("verified-history".into()),
            evidence_kind: "verified_original_trade_history_price_seconds",
        }));
        let identity = WindowIdentity {
            market_id: "market-a".into(),
            condition_id: "condition-a".into(),
            window_id: "window-a".into(),
            rule_sha256: "rule-a".into(),
            start_ms: 300_000,
            end_ms: 600_000,
        };
        let service = test_service(state);
        let result = service.window_comparison(&identity, None).await;
        assert_eq!(result["schema_version"], "marketcow.btc5m.binance-comparison.v3");
        assert_eq!(result["opening_available"], true);
        assert_eq!(result["opening_price_usdt"], "150");
        assert_eq!(result["opening_source_epoch"], Value::Null);
        assert_eq!(result["opening_source_epochs"], json!(["old-epoch", "new-epoch"]));
        assert_eq!(result["opening_history_evidence_sha256"], "verified-history");
        assert_eq!(result["current_available"], false);
        assert_eq!(result["available"], false);
        assert!(result["mapped_official_scale_price"].is_null());
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
        assert_eq!(first_point.trade_time_ms, second_point.trade_time_ms);
        assert_ne!(first_point.trade_id, second_point.trade_id);
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
        Arc::make_mut(&mut changed).quantity = "0.00300000".into();
        assert!(!same_trade(&rest_point, &changed));
    }

    #[tokio::test]
    async fn reader_failure_survives_closed_ingress() {
        let task = tokio::spawn(async { bail!("BTC trade WS closed: code=1013"); });
        let error = finish_reader(Err(anyhow::anyhow!("BTC trade WS reader stopped")), task).await.unwrap_err();
        assert!(format!("{error:#}").contains("1013"));
        assert_eq!(failure_class(&error), "ws_close");
    }

    #[tokio::test]
    async fn consumer_failure_aborts_and_joins_reader() {
        let task = tokio::spawn(async { std::future::pending::<Result<()>>().await });
        let error = finish_reader(Err(anyhow::anyhow!("BTC trade ID gap: expected=5 received=7")), task).await.unwrap_err();
        assert_eq!(failure_class(&error), "trade_id_continuity");
        assert!(format!("{error:#}").contains("expected=5"));
    }

    #[tokio::test]
    async fn source_recovery_publishes_new_snapshot_on_same_subscription() {
        let service = test_service(State::new());
        let mut owner=(*service.state.load()).clone();
        let mut subscription=service.subscribe();
        service.invalidate(&mut owner, "source_read_failed").await;
        let unavailable=service.event_frame(&subscription.recv().await.unwrap());
        let trade:WsTrade=serde_json::from_slice(first_raw()).unwrap();
        let point=ws_point(&trade,first_raw(),"2026-10-08T00:00:00Z").unwrap();
        service.commit_snapshot(&mut owner, VecDeque::from([point]),"raw".into(),first_raw().to_vec()).await.unwrap();
        let snapshot=service.event_frame(&subscription.recv().await.unwrap());
        assert_eq!(snapshot["type"],"snapshot");
        assert_eq!(snapshot["source_epoch"],unavailable["source_epoch"]);
        assert_eq!(snapshot["stream_serial"].as_u64().unwrap(),unavailable["stream_serial"].as_u64().unwrap()+1);
    }
    #[tokio::test]
    async fn held_snapshot_never_prevents_next_trade_and_events_do_not_queue_snapshots() {
        let service=test_service(State::new());
        let mut owner=State::new();
        let a:WsTrade=serde_json::from_slice(first_raw()).unwrap();
        let b:WsTrade=serde_json::from_slice(second_raw()).unwrap();
        let first=ws_point(&a,first_raw(),"2026-10-08T00:00:00Z").unwrap();
        let second=ws_point(&b,second_raw(),"2026-10-08T00:00:00Z").unwrap();
        service.commit_snapshot(&mut owner,VecDeque::from([first]),"raw".into(),first_raw().to_vec()).await.unwrap();
        let held=service.state.load();
        let mut events=service.subscribe();
        service.commit_trade(&mut owner,second,second_raw().to_vec(),"2026-10-08T00:00:00Z".into()).await.unwrap();
        assert_eq!(held.last_id,Some(41));
        assert_eq!(service.state.load().last_id,Some(42));
        let event=events.recv().await.unwrap();
        assert!(event.snapshot.is_none());
        assert_eq!(service.event_frame(&event)["trade"]["trade_id"],42);
    }
    #[test]
    fn archive_io_failure_retries_without_changing_live() {
        let root=tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("binance-trades"),b"blocking file").unwrap();
        let (tx,rx)=mpsc::channel(4);
        let bytes=Arc::new(AtomicUsize::new(first_raw().len()*2));
        let healthy=Arc::new(AtomicBool::new(true));let status=Arc::new(std::sync::Mutex::new(json!({})));
        let dir=root.path().to_owned();let b=bytes.clone();let h=healthy.clone();let s=status.clone();
        let worker=std::thread::spawn(move || archive_task(dir,rx,b,h,s));
        tx.blocking_send(Archive{epoch:"test".into(),trade_id:Some(41),received_at:"diagnostic".into(),raw:first_raw().to_vec().into()}).unwrap();
        for _ in 0..100 {if !healthy.load(Ordering::Acquire){break}std::thread::sleep(Duration::from_millis(10));}
        assert_eq!(status.lock().unwrap()["state"],"failed_retrying");
        std::fs::remove_file(root.path().join("binance-trades")).unwrap();
        tx.blocking_send(Archive{epoch:"test".into(),trade_id:Some(41),received_at:"diagnostic".into(),raw:first_raw().to_vec().into()}).unwrap();
        drop(tx);worker.join().unwrap();assert!(healthy.load(Ordering::Acquire));
        assert_eq!(bytes.load(Ordering::Acquire),0);
        assert_eq!(status.lock().unwrap()["last_success_trade_id"],41);
        assert_eq!(status.lock().unwrap()["failed_write_records"],1);
    }
}
