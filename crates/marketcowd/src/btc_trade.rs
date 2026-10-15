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
use marketcow_contracts::typed_v3::{self as typed_contract, btc as typed};
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
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

const WS_URL: &str = "wss://data-stream.binance.vision/ws/btcusdt@trade";
const REST_URL: &str = "https://data-api.binance.vision/api/v3/trades?symbol=BTCUSDT&limit=1000";
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
#[path = "btc_trade_archive.rs"]
pub mod archive;

#[derive(Clone, Debug)]
struct OpeningCandidate {
    start_ms: i64,
    price: Decimal,
    source_epochs: Vec<String>,
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
    state: Arc<RwLock<State>>,

    typed_live: broadcast::Sender<Arc<crate::btc_typed::Published>>,
    producer_instance_id: String,
    archive: mpsc::Sender<Archive>,
    archive_bytes: Arc<AtomicUsize>,
    archive_healthy: Arc<AtomicBool>,
    archive_dropped: Arc<AtomicU64>,
    archive_status: Arc<std::sync::Mutex<Value>>,
    database: Option<mpsc::Sender<Archive>>,
    database_bytes: Arc<AtomicUsize>,
}

struct State {
    epoch: String,
    phase: &'static str,
    reason: String,
    serial: u64,
    sequence: u64,
    last_id: Option<u64>,
    trades: VecDeque<typed::Trade>,
    typed_generation: String,
    twap: TradeTwap,
    opening_candidates: VecDeque<OpeningCandidate>,
    opening_recovery_status: BTreeMap<i64, String>,
    checked_opening_boundary_ms: Option<i64>,

    last_received_at: Option<String>,
    last_transport_at: Option<String>,
}

#[derive(Clone)]
struct Archive {
    epoch: String,
    trade_id: Option<u64>,
    received_at: String,
    raw: Vec<u8>,
    trades: Vec<typed::Trade>,
}
impl Archive {
    fn owned_bytes(&self)->usize {
        std::mem::size_of::<Self>()+self.raw.capacity()+self.epoch.capacity()+self.received_at.capacity()
            +self.trades.capacity()*std::mem::size_of::<typed::Trade>()
            +self.trades.iter().map(|r|r.price.capacity()+r.quantity.capacity()+r.evidence.as_ref().map_or(0,|e|e.evidence_id.capacity())).sum::<usize>()
    }
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
        let history_root = root.join("binance-trades");

        let (typed_live, _) = broadcast::channel(1024);
        let (archive, receiver) = mpsc::channel(ARCHIVE_RECORDS);
        let (database, db_receiver) = mpsc::channel(DATABASE_RECORDS);
        let archive_bytes = Arc::new(AtomicUsize::new(0));
        let database_bytes = Arc::new(AtomicUsize::new(0));
        let archive_healthy = Arc::new(AtomicBool::new(true));
        let service = Self {
            state: Arc::new(RwLock::new(State::new())),

            typed_live,
            producer_instance_id: uuid::Uuid::new_v4().to_string(),
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
        tokio::spawn(async move { owner.run().await });
        let recovery_owner = service.clone();
        tokio::spawn(async move { recovery_owner.recover_opening_loop(history_root).await });
        service
    }

    pub fn typed_health(&self)->(Option<typed_contract::history::Health>,Option<typed_contract::history::Health>) {
        let Ok(status)=self.archive_status.lock() else{return (None,None)};
        let convert=|value:&Value|->Option<typed_contract::history::Health>{
            let state=value["state"].as_str()?;
            let at=|key:&str|value[key].as_str().and_then(|s|chrono::DateTime::parse_from_rfc3339(s).ok()).and_then(|t|t.timestamp_nanos_opt());
            let dropped=value["dropped_records"].as_u64().unwrap_or(0);
            Some(typed_contract::history::Health{state:if dropped>0&&state=="writing"{"writing_with_drops"}else{state}.into(),
                reason:value["reason"].as_str().or_else(||value["last_drop_reason"].as_str()).or_else(||value["last_failure"]["reason"].as_str()).map(str::to_owned),
                last_success_sequence:value["last_success_trade_id"].as_u64(),last_success_source_time_ms:value["last_success_source_time_ms"].as_i64(),
                failed_at_ns:at("failed_at").or_else(||at("last_drop_at")).or_else(||value["last_failure"]["failed_at"].as_str().and_then(|s|chrono::DateTime::parse_from_rfc3339(s).ok()).and_then(|t|t.timestamp_nanos_opt())),dropped_records:dropped})
        };
        (convert(&status["database"]),convert(&status))
    }
    pub async fn typed_snapshot_and_subscribe(&self) -> Result<(Arc<crate::btc_typed::Published>, broadcast::Receiver<Arc<crate::btc_typed::Published>>)> {
        let state=self.state.read().await;
        let receiver=self.typed_live.subscribe();
        Ok((crate::btc_typed::Published::new(self.typed_frame(&state,None)?)?,receiver))
    }
    fn typed_frame(&self,state:&State,payload:Option<typed::price_frame::Payload>)->Result<typed::PriceFrame> {
        let now=Utc::now().timestamp_nanos_opt().context("clock_unavailable")?;
        let payload=payload.unwrap_or_else(||if state.phase=="live" {
            let trades=state.trades.iter().skip(state.trades.len().saturating_sub(SNAPSHOT_TRADES)).cloned().collect::<Vec<_>>();
            typed::price_frame::Payload::Snapshot(typed::Snapshot{first_trade_id:trades.first().map(|r|r.trade_id),last_trade_id:trades.last().map(|r|r.trade_id),
                retained_count:state.trades.len() as u64,truncated:state.trades.len()>SNAPSHOT_TRADES,trades,observations:Vec::new()})
        }else{typed::price_frame::Payload::Status(typed::Status{phase:typed::Phase::Unavailable as i32,reason:Some(state.reason.clone())})});
        Ok(typed::PriceFrame{schema_version:typed_contract::PRICE_SCHEMA.into(),producer_instance_id:self.producer_instance_id.clone(),stream_id:state.typed_generation.clone(),
            stream_sequence:state.serial,source:typed_contract::common::Source::BinanceSpotTrade as i32,instrument_id:INSTRUMENT.into(),source_sequence:state.last_id,
            published_at_ns:now,latest_source_age_ms_at_emit:state.trades.back().map(|r|now.div_euclid(1_000_000)-r.trade_time_ms),payload:Some(payload)})
    }
    fn publish_typed(&self,state:&State,payload:Option<typed::price_frame::Payload>)->Result<()> {
        if self.typed_live.receiver_count()>0 {let _=self.typed_live.send(crate::btc_typed::Published::new(self.typed_frame(state,payload)?)?);}
        Ok(())
    }

    pub async fn status(&self) -> Value {
        let state = self.state.read().await;
        json!({"schema_version":"marketcow.binance-trade-status.v1",
            "source":"binance_spot_trade","instrument_id":INSTRUMENT,
            "source_epoch":state.epoch,"phase":state.phase,"reason":state.reason,
            "stream_serial":state.serial,"message_sequence":state.sequence,
            "last_trade_id":state.last_id,"last_received_at":state.last_received_at,
            "last_transport_at":state.last_transport_at,
            "latest_trade_time_ms":state.trades.back().map(|trade|trade.trade_time_ms),
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
    pub async fn native_readings(&self,key:&typed_contract::market::MarketKey)->Result<(typed_contract::facts::ReadingState,typed_contract::facts::ReadingState)> {
        use typed_contract::facts::{ReadingState,ReferenceReading};
        let start=chrono::DateTime::parse_from_rfc3339(&key.start_at)?.timestamp_millis();
        let end=chrono::DateTime::parse_from_rfc3339(&key.end_at)?.timestamp_millis();
        let state=self.state.read().await;
        let opening=state.opening_candidates.iter().find(|c|c.start_ms==start);
        let reading=|price:Decimal,at:i64,purpose:&str,generations:Vec<String>|ReferenceReading {
            purpose:purpose.into(),instrument_id:INSTRUMENT.into(),quote_currency:"USDT".into(),
            price_method:"last_trade_price_time_weighted_over_preceding_60000ms".into(),price:price.normalize().to_string(),
            boundary_time_ms:at,interval_start_ms:Some(at-60_000),interval_end_ms:Some(at),covered_ms:Some(60_000),
            selection_rule:"carry_last_trade_between_contiguous_trade_ids".into(),verification:"binance_proxy_not_official".into(),
            source_generations:generations,..Default::default()
        };
        let opening=match opening {
            Some(c)=>ReadingState{available:true,reason:None,reading:Some(reading(c.price,start,"binance_preopen_proxy",c.source_epochs.clone()))},
            None=>ReadingState{available:false,reason:Some(state.opening_recovery_status.get(&start).cloned().unwrap_or_else(||"complete_60s_coverage_unavailable".into())),reading:None},
        };
        let current=if state.phase=="live" {state.twap.latest_completed_end_ms().map(|t|t.min(end)).filter(|t|*t>=start)
            .and_then(|at|state.twap.completed_at(at).map(|price|reading(price,at,"current_proxy",vec![state.epoch.clone()]))) }else{None};
        let current=match current {
            Some(reading)=>ReadingState{available:true,reason:None,reading:Some(reading)},
            None=>ReadingState{available:false,reason:Some(if state.phase=="live"{"current_60s_coverage_unavailable"}else{"trade_source_unavailable"}.into()),reading:None},
        };
        Ok((opening,current))
    }

    async fn recover_opening_loop(&self, root: PathBuf) {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        let mut last_attempt: Option<(i64, tokio::time::Instant)> = None;
        loop {
            interval.tick().await;
            let boundary = {
                let state = self.state.read().await;
                if state.phase != "live" {
                    continue;
                }
                let Some(time_ms) = state.twap.last_time_ms else {
                    continue;
                };
                let boundary = time_ms.div_euclid(MARKET_WINDOW_MS) * MARKET_WINDOW_MS;
                if state.opening_candidates.iter().any(|entry| entry.start_ms == boundary) {
                    continue;
                }
                boundary
            };
            if last_attempt.is_some_and(|(at, when)| {
                at == boundary && when.elapsed() < Duration::from_secs(30)
            }) {
                continue;
            }
            last_attempt = Some((boundary, tokio::time::Instant::now()));
            self.state.write().await.opening_recovery_status.insert(
                boundary,
                "history_recovery_pending".to_owned(),
            );
            let through={let state=self.state.read().await;state.trades.iter().find(|row|row.trade_time_ms>=boundary).map(|row|row.trade_id)};
            let Some(through)=through else{continue};
            let root = root.clone();
            let recovered = tokio::task::spawn_blocking(move || {
                recover_typed_opening(&root,boundary,through)
            })
            .await;
            let mut state = self.state.write().await;
            match recovered {
                Ok(Ok(candidate)) => {
                    if !state
                        .opening_candidates
                        .iter()
                        .any(|entry| entry.start_ms == boundary)
                    {
                        marketcow_polymarket::diagnostic::emit(json!({
                            "stage":"btc_opening_history_recovered",
                            "window_start_ms":boundary,
                            "source_epochs":candidate.source_epochs,
                            "observed_at":Utc::now(),
                        }).to_string());
                        state.opening_candidates.push_back(candidate);
                        while state.opening_candidates.len() > MAX_OPENING_WINDOWS {
                            state.opening_candidates.pop_front();
                        }
                    }
                    state.opening_recovery_status.remove(&boundary);
                }
                other => {
                    let detail = match other {
                        Ok(Err(error)) => format!("{error:#}"),
                        Err(join_error) => format!("history_recovery_worker_failed: {join_error}"),
                        Ok(Ok(_)) => unreachable!(),
                    };
                    let reason = detail.split(':').next().unwrap_or("history_recovery_unavailable");
                    let reason = if reason.starts_with("history_") {
                        reason.to_owned()
                    } else {
                        "history_recovery_unavailable".to_owned()
                    };
                    state.opening_recovery_status.insert(boundary, reason.clone());
                    marketcow_polymarket::diagnostic::emit(json!({
                        "stage":"btc_opening_history_recovery_failed",
                        "window_start_ms":boundary,
                        "reason":reason,
                        "detail":detail.chars().take(512).collect::<String>(),
                        "observed_at":Utc::now(),
                    }).to_string());
                }
            }
            state.opening_recovery_status.retain(|start, _| {
                *start >= boundary - MARKET_WINDOW_MS * MAX_OPENING_WINDOWS as i64
            });
        }
    }

    async fn run(self) {
        let mut backoff_seconds = 1u64;
        let mut attempt = 0u64;
        loop {
            attempt = attempt.saturating_add(1);
            self.invalidate("source_reconnecting").await;
            self.diagnostic("connecting", attempt, None).await;
            if let Err(error) = self.run_once().await {
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
                self.diagnostic(reason, attempt, Some(&error)).await;
                self.invalidate(reason).await;
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
        let state = self.state.read().await;
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

    async fn run_once(&self) -> Result<()> {
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
        let outcome = self.join_and_consume(&mut rx, &overflow).await;
        finish_reader(outcome, reader_task).await
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
        let mut trades = VecDeque::with_capacity(MAX_TRADES);
        let mut previous: Option<u64> = None;
        for row in &rows {
            if let Some(id) = previous {
                ensure!(
                    Some(row.id) == id.checked_add(1),
                    "BTC recent trades ID discontinuity"
                );
            }
            previous = Some(row.id);
            trades.push_back(rest_point(row, &rest_received_at)?);
        }
        let rest_last = previous.context("BTC recent trades lack last ID")?;
        let epoch = self.state.read().await.epoch.clone();
        self.archive_raw(Archive {
            epoch,
            trade_id: None,
            received_at: rest_received_at,
            raw: rest_raw.to_vec(),
            trades: trades.iter().cloned().collect(),
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
                        trades: Vec::new(),
                    });
                    return Err(anyhow::Error::from(error).context("BTC trade WS parse failed"));
                }
            };
            if trade.e != "trade" || trade.s != "BTCUSDT" {
                let epoch = self.state.read().await.epoch.clone();
                self.archive_raw(Archive {
                    epoch,
                    trade_id: Some(trade.t),
                    received_at,
                    raw,
                    trades: Vec::new(),
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
                        trades: Vec::new(),
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
                        trades: vec![point],
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
                    )
                    .await?;
                }
                let epoch = self.state.read().await.epoch.clone();
                self.archive_raw(Archive {
                    epoch,
                    trade_id: Some(trade.t),
                    received_at,
                    raw,
                    trades: vec![point],
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
                    .find(|candidate| candidate.trade_id == trade.t)
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
                    trades: vec![point],
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
                    trades: vec![point],
                });
                bail!("BTC trade ID gap: expected={} received={}", last_id.saturating_add(1), trade.t);
            }
            last_id = trade.t;
            self.commit_trade(point, raw, received_at).await?;
        }
    }

    async fn commit_snapshot(
        &self,
        trades: VecDeque<typed::Trade>,
    ) -> Result<()> {
        let mut twap = TradeTwap::default();
        for trade in &trades {
            twap.observe(
                trade.trade_time_ms,
                Decimal::from_str(&trade.price)?,
            )?;
        }
        {
            let mut state = self.state.write().await;
            state.phase = "live";
            state.reason.clear();
            state.last_id = trades.back().map(|v|v.trade_id);
            state.last_received_at = trades
                .back()
                .and_then(|v|v.received_at_ns).map(|t|chrono::DateTime::from_timestamp_nanos(t).to_rfc3339());

            state.trades = trades;
            state.typed_generation=state.epoch.clone();
            state.twap = twap;
            state.capture_opening_boundary();
            state.serial += 1;
            state.sequence += 1;
            self.publish_typed(&state,None)?;
        }
        self.diagnostic("live_snapshot_published", 0, None).await;
        Ok(())
    }

    async fn commit_trade(&self, point: typed::Trade, raw: Vec<u8>, received_at: String) -> Result<()> {
        let (epoch, id) = {
            let mut state = self.state.write().await;
            ensure!(state.phase == "live", "BTC trade source not joined");
            let id = point.trade_id;
            state.twap.observe(
                point.trade_time_ms,
                Decimal::from_str(&point.price)?,
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
            self.publish_typed(&state,Some(typed::price_frame::Payload::Trade(typed::TradeDelta{trade:Some(point.clone())})))?;
            (state.epoch.clone(), id)
        };
        self.archive_raw(Archive {
            epoch,
            trade_id: Some(id),
            received_at,
            raw,
            trades: vec![point],
        });
        Ok(())
    }

    async fn invalidate(&self, reason: &str) {
        {
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

            state.last_received_at = None;
            state.last_transport_at = None;
            if let Err(error)=self.publish_typed(&state,None) {tracing::error!(%error,"btc_typed_status_failed");}
        }
    }

    fn archive_raw(&self, mut record: Archive) {
        self.queue_database(&record);
        record.trades=Vec::new(); // No duplicate typed payload in the raw archive queue.
        let size = record.owned_bytes();
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
        if record.trades.is_empty(){return;}
        let queued=record.clone();
        let size=queued.owned_bytes();
        if size>DATABASE_BYTES || self.database_bytes.fetch_update(Ordering::AcqRel,Ordering::Acquire,
            |used|used.checked_add(size).filter(|total|*total<=DATABASE_BYTES)).is_err() {
            self.database_drop("database_queue_bytes_exhausted",record);return;
        }
        if let Err(error)=sender.try_send(queued) {
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
        let Some(price) = self.twap.completed_at(boundary_ms) else {
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
            source_epochs: vec![self.epoch.clone()],
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
            typed_generation:uuid::Uuid::new_v4().to_string(),
            twap: TradeTwap::default(),
            opening_candidates: VecDeque::new(),
            opening_recovery_status: BTreeMap::new(),
            checked_opening_boundary_ms: None,

            last_received_at: None,
            last_transport_at: None,
        }
    }
}

fn validate_decimal(value: &str) -> Result<()> {
    let decimal = Decimal::from_str(value)?;
    ensure!(decimal > Decimal::ZERO, "BTC trade nonpositive decimal");
    Ok(())
}

fn rest_point(row: &RestTrade, received_at: &str) -> Result<typed::Trade> {
    validate_decimal(&row.price)?;
    validate_decimal(&row.qty)?;
    ensure!(row.time > 0, "BTC trade invalid time");
    Ok(typed::Trade{trade_id:row.id,trade_time_ms:row.time,event_time_ms:None,price:row.price.clone(),quantity:row.qty.clone(),
        buyer_is_maker:Some(row.is_buyer_maker),received_at_ns:chrono::DateTime::parse_from_rfc3339(received_at)?.timestamp_nanos_opt(),evidence:None})
}

fn ws_point(row: &WsTrade, _raw: &[u8], received_at: &str) -> Result<typed::Trade> {
    validate_decimal(&row.p)?;
    validate_decimal(&row.q)?;
    ensure!(
        row.trade_time_ms > 0 && row.event_time_ms > 0,
        "BTC trade invalid time"
    );
    Ok(typed::Trade{trade_id:row.t,trade_time_ms:row.trade_time_ms,event_time_ms:Some(row.event_time_ms),price:row.p.clone(),quantity:row.q.clone(),
        buyer_is_maker:Some(row.m),received_at_ns:chrono::DateTime::parse_from_rfc3339(received_at)?.timestamp_nanos_opt(),evidence:None})
}

fn same_trade(a: &typed::Trade, b: &typed::Trade) -> bool {
    a.trade_id==b.trade_id && a.trade_time_ms==b.trade_time_ms && a.buyer_is_maker==b.buyer_is_maker
        && Decimal::from_str(&a.price).ok()==Decimal::from_str(&b.price).ok()
        && Decimal::from_str(&a.quantity).ok()==Decimal::from_str(&b.quantity).ok()
        && (a.event_time_ms.is_none()||b.event_time_ms.is_none()||a.event_time_ms==b.event_time_ms)
}
fn recover_typed_opening(root:&std::path::Path,boundary:i64,through:u64)->Result<OpeningCandidate> {
    use rusqlite::{OptionalExtension,params};
    let start=boundary.checked_sub(60_000).context("history_opening_boundary_invalid")?;
    let mut db=crate::btc_typed_store::open_reader(&root.join("trades.sqlite3"))?;
    let tx=db.transaction()?;
    let predecessor:Option<i64>=tx.query_row("SELECT trade_id FROM typed_binance_trades WHERE trade_time_ms<? AND trade_id<=? ORDER BY trade_id DESC LIMIT 1",params![start,i64::try_from(through)?],|r|r.get(0)).optional()?;
    let first=u64::try_from(predecessor.context("history_boundary_predecessor_missing")?)?;
    let mut cursor=first;let mut previous=None;let mut previous_time=None;let mut count=0usize;
    let mut twap=TradeTwap::default();let mut generations=Vec::<String>::new();let mut boundary_seen=false;
    let deadline=std::time::Instant::now()+Duration::from_secs(120);
    while cursor<=through {
        ensure!(std::time::Instant::now()<deadline,"history_deadline");
        let rows=crate::btc_typed_store::read_trades_with_generations(&tx,cursor,through,1000)?;
        ensure!(!rows.is_empty(),"history_trade_id_gap");
        for (row,generation) in rows {
            ensure!(row.trade_id==cursor && previous.is_none_or(|p:u64|p.checked_add(1)==Some(row.trade_id)),"history_trade_id_gap");
            ensure!(previous_time.is_none_or(|t|row.trade_time_ms>=t),"history_supplier_time_regression");
            count+=1;ensure!(count<=1_024_000,"history_trade_capacity");
            if row.trade_time_ms>=boundary {
                let carried=twap.last_price.context("history_boundary_price_missing")?;
                twap.observe(boundary,carried)?;boundary_seen=true;break;
            }
            let generation=generation.context("history_source_generation_missing")?;
            if !generations.contains(&generation){ensure!(generations.len()<64,"history_opening_epoch_capacity");generations.push(generation);}
            twap.observe(row.trade_time_ms,Decimal::from_str(&row.price)?)?;
            previous=Some(row.trade_id);previous_time=Some(row.trade_time_ms);cursor=row.trade_id.checked_add(1).context("history_trade_id_range")?;
        }
        if boundary_seen{break;}
    }
    ensure!(boundary_seen,"history_boundary_successor_missing");
    let price=twap.completed_at(boundary).context("history_opening_coverage_incomplete")?;
    tx.commit()?;
    Ok(OpeningCandidate{start_ms:boundary,price,source_epochs:generations})
}

fn database_task(root:PathBuf,mut rx:mpsc::Receiver<Archive>,bytes:Arc<AtomicUsize>,
    status:Arc<std::sync::Mutex<Value>>) {
    let path=root.join("binance-trades/trades.sqlite3");
    let open=||crate::btc_typed_store::open(&path);
    let mut db=open().ok();
    while let Some(first)=rx.blocking_recv() {
        let mut batch=vec![first];
        let mut batch_bytes=batch[0].owned_bytes();
        std::thread::sleep(Duration::from_millis(3));
        while batch.len()<64 && batch_bytes<256*1024 {
            match rx.try_recv() {
                Ok(next)=>{batch_bytes+=next.owned_bytes();batch.push(next)},
                Err(_)=>break,
            }
        }
        let last=batch.last().unwrap();
        if db.is_none(){db=open().ok();}
        let result=db.as_mut().context("database_open_failed")
            .and_then(|db|->Result<usize>{
                let mut count=0;
                for record in &batch {
                    // Invalid supplier frames remain in the audit archive,
                    // never masquerade as market records or fail the writer.
                    if record.trades.is_empty() { continue; }
                    count+=crate::btc_typed_store::insert_trades(db,&record.trades,&crate::btc_typed_store::EvidenceInput {
                        raw:&record.raw,acquisition:if record.trade_id.is_some(){"ws_archive"}else{"official_recent_trades"},
                        upstream_url:if record.trade_id.is_some(){WS_URL}else{REST_URL},
                        received_at_ns:chrono::DateTime::parse_from_rfc3339(&record.received_at).ok().and_then(|t|t.timestamp_nanos_opt()),
                        source_generation:Some(&record.epoch),content_type:"application/json",
                    })?;
                }
                Ok(count)
            });
        bytes.fetch_sub(batch_bytes,Ordering::AcqRel);
        match result {
            Ok(_) => {
                let mut health=status.lock().unwrap();
                health["database"]["state"]="writing".into();
                health["database"]["last_success_at"]=json!(Utc::now());
                health["database"]["last_success_trade_id"]=json!(last.trades.last().map(|t|t.trade_id));
                health["database"]["last_success_source_time_ms"]=json!(last.trades.last().map(|t|t.trade_time_ms));
                health["database"]["reason"]=Value::Null;
                health["database"]["failed_at"]=Value::Null;
            }
            Err(error) => {
                let mut health=status.lock().unwrap();
                health["database"]["state"]="failed_retrying".into();
                health["database"]["failed_at"]=json!(Utc::now());
                health["database"]["reason"]=database_failure_code(&error).into();
                health["database"]["detail"]=format!("{error:#}").chars().take(256).collect::<String>().into();
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

fn database_failure_code(error:&anyhow::Error)->&'static str {
    use rusqlite::ErrorCode;
    for cause in error.chain() {
        if let Some(rusqlite::Error::SqliteFailure(code,_))=cause.downcast_ref::<rusqlite::Error>() {
            return match code.code {
                ErrorCode::DiskFull=>"database_disk_full",
                ErrorCode::PermissionDenied|ErrorCode::ReadOnly=>"database_permission_denied",
                ErrorCode::DatabaseBusy|ErrorCode::DatabaseLocked=>"database_busy",
                ErrorCode::CannotOpen=>"database_open_failed",
                ErrorCode::DatabaseCorrupt|ErrorCode::NotADatabase=>"database_corrupt",
                ErrorCode::SystemIoFailure=>"database_io_failed",
                _=>"database_write_failed",
            };
        }
    }
    if error.to_string().contains("identity conflict"){"database_identity_conflict"}else{"database_write_failed"}
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
        let raw_size=record.owned_bytes();
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

    fn publication_service() -> Service {
        Service {
            typed_live: broadcast::channel(1024).0,
            producer_instance_id: "test-producer".into(),
            state: Arc::new(RwLock::new(State::new())),
            archive: mpsc::channel(16).0,
            archive_bytes: Arc::new(AtomicUsize::new(0)),
            archive_healthy: Arc::new(AtomicBool::new(true)),
            archive_dropped: Arc::new(AtomicU64::new(0)),
            archive_status: Arc::new(std::sync::Mutex::new(Value::Null)),
            database: None,
            database_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[tokio::test]
    async fn native_recovery_uses_same_subscription_without_legacy_envelopes() {
        let service = publication_service();
        let (_, mut receiver) = service.typed_snapshot_and_subscribe().await.unwrap();
        service.invalidate("source_read_failed").await;
        assert!(matches!(receiver.recv().await.unwrap().frame.payload, Some(typed::price_frame::Payload::Status(_))));
        let row: WsTrade = serde_json::from_slice(first_raw()).unwrap();
        let point = ws_point(&row, first_raw(), "2026-10-08T00:00:00Z").unwrap();
        service.commit_snapshot(VecDeque::from([point])).await.unwrap();
        let snapshot = receiver.recv().await.unwrap();
        assert!(matches!(snapshot.frame.payload, Some(typed::price_frame::Payload::Snapshot(_))));
        let row: WsTrade = serde_json::from_slice(second_raw()).unwrap();
        let point = ws_point(&row, second_raw(), "2026-10-08T00:00:00Z").unwrap();
        service.commit_trade(point, second_raw().to_vec(), "2026-10-08T00:00:00Z".into()).await.unwrap();
        let delta = receiver.recv().await.unwrap();
        assert_eq!(delta.frame.stream_sequence, snapshot.frame.stream_sequence + 1);
        assert!(matches!(delta.frame.payload, Some(typed::price_frame::Payload::Trade(_))));
    }

    #[test]
    fn native_opening_recovery_is_database_only_and_rejects_real_gap() {
        let root = tempfile::tempdir().unwrap();
        let mut db = crate::btc_typed_store::open(&root.path().join("trades.sqlite3")).unwrap();
        let rows = [(1, 59_000, "100.00"), (2, 60_000, "120.00"), (3, 90_000, "140.00"), (4, 120_000, "900.00")]
            .into_iter().map(|(trade_id, trade_time_ms, price)| typed::Trade {
                trade_id, trade_time_ms, price: price.into(), quantity: "0.0100".into(),
                buyer_is_maker: Some(false), ..Default::default()
            }).collect::<Vec<_>>();
        crate::btc_typed_store::insert_trades(&mut db, &rows, &crate::btc_typed_store::EvidenceInput {
            raw: b"opaque audit evidence not parsed by business history", acquisition: "ws_archive",
            upstream_url: WS_URL, received_at_ns: None, source_generation: Some("source-a"), content_type: "application/json"
        }).unwrap();
        let opening = recover_typed_opening(root.path(), 120_000, 4).unwrap();
        assert_eq!(opening.price, Decimal::from(130));
        assert_eq!(opening.source_epochs, ["source-a"]);
        db.execute("DELETE FROM typed_binance_trades WHERE trade_id=3", []).unwrap();
        assert!(recover_typed_opening(root.path(), 120_000, 4).is_err());
    }

    #[tokio::test]
    async fn native_opening_is_independent_of_current_source_and_official_reference() {
        let service = publication_service();
        let key = typed_contract::market::MarketKey {
            start_at: "1970-01-01T00:05:00Z".into(),
            end_at: "1970-01-01T00:10:00Z".into(),
            ..Default::default()
        };
        assert!(!service.native_readings(&key).await.unwrap().0.available);
        {
            let mut state = service.state.write().await;
            state.phase = "live";
            state.twap.observe(240_000, Decimal::from(100)).unwrap();
            state.twap.observe(270_000, Decimal::from(200)).unwrap();
            state.twap.observe(300_000, Decimal::from(900)).unwrap();
            state.capture_opening_boundary();
        }
        let (opening, _) = service.native_readings(&key).await.unwrap();
        assert_eq!(opening.reading.unwrap().price, "150");
        service.invalidate("disconnected").await;
        let (opening, current) = service.native_readings(&key).await.unwrap();
        assert!(opening.available);
        assert!(!current.available);
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
        let rest_point = rest_point(&rest, "2026-10-08T00:00:00Z").unwrap();
        let ws: WsTrade = serde_json::from_slice(first_raw()).unwrap();
        let ws_point = ws_point(&ws, first_raw(), "2026-10-08T00:00:00Z").unwrap();
        assert!(same_trade(&rest_point, &ws_point));
        let mut changed = ws_point;
        changed.quantity = "0.00300000".into();
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

    #[test]
    fn archive_io_failure_retries_without_changing_live() {
        let root=tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("binance-trades"),b"blocking file").unwrap();
        let (tx,rx)=mpsc::channel(4);
        let record=||Archive{epoch:"test".into(),trade_id:Some(41),received_at:"diagnostic".into(),trades:Vec::new(),raw:first_raw().to_vec()};
        let bytes=Arc::new(AtomicUsize::new(record().owned_bytes()*2));
        let healthy=Arc::new(AtomicBool::new(true));let status=Arc::new(std::sync::Mutex::new(json!({})));
        let dir=root.path().to_owned();let b=bytes.clone();let h=healthy.clone();let s=status.clone();
        let worker=std::thread::spawn(move || archive_task(dir,rx,b,h,s));
        tx.blocking_send(record()).unwrap();
        for _ in 0..100 {if !healthy.load(Ordering::Acquire){break}std::thread::sleep(Duration::from_millis(10));}
        assert_eq!(status.lock().unwrap()["state"],"failed_retrying");
        std::fs::remove_file(root.path().join("binance-trades")).unwrap();
        tx.blocking_send(record()).unwrap();
        drop(tx);worker.join().unwrap();assert!(healthy.load(Ordering::Acquire));
        assert_eq!(bytes.load(Ordering::Acquire),0);
        assert_eq!(status.lock().unwrap()["last_success_trade_id"],41);
        assert_eq!(status.lock().unwrap()["failed_write_records"],1);
    }
}
