//! Native BTCUSDT finalized-minute and finalized-second feeds embedded in MarketCow.
//!
//! Publication is memory-first. A bounded independent writer advances the
//! durable watermark only after append+sync; disk IO is never awaited by the
//! Binance receive loop. Failures are isolated to this component.
use anyhow::{Context, Result, bail, ensure};
use axum::{
    Json, Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use base64::Engine;
use chrono::{SecondsFormat, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::VecDeque, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    fs::OpenOptions,
    io::AsyncWriteExt,
    sync::{RwLock, Semaphore, broadcast, mpsc},
};
use tokio_tungstenite::{connect_async, tungstenite::Message as UpstreamMessage};

const REST_URL: &str = "https://data-api.binance.vision/api/v3/klines";
const STEP_MS: i64 = 60_000;
const BAR_COUNT: usize = 60;
const WS_IDLE_TIMEOUT: Duration = Duration::from_secs(20);
const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_REPAIR_BARS: usize = 1_000;

#[derive(Clone, Copy)]
struct FeedSpec {
    interval: &'static str,
    ws_url: &'static str,
    step_ms: i64,
    bar_count: usize,
    window_schema: &'static str,
    fact_prefix: &'static str,
}

const MINUTE: FeedSpec = FeedSpec {
    interval: "1m",
    ws_url: "wss://data-stream.binance.vision/ws/btcusdt@kline_1m",
    step_ms: STEP_MS,
    bar_count: BAR_COUNT,
    window_schema: "marketcow.binance-finalized-1m-window.v1",
    fact_prefix: "btc_1m",
};
const SECOND: FeedSpec = FeedSpec {
    interval: "1s",
    ws_url: "wss://data-stream.binance.vision/ws/btcusdt@kline_1s",
    step_ms: 1_000,
    bar_count: 300,
    window_schema: "marketcow.binance-finalized-1s-window.v1",
    fact_prefix: "btc_1s",
};

#[derive(Clone)]
pub struct Config {
    pub root: PathBuf,
    pub archive_bytes: usize,
    pub queue_records: usize,
    pub queue_bytes: usize,
    pub response_bytes: usize,
    pub clients: usize,
    pub send_timeout: Duration,
}

impl Config {
    fn validate(&self) -> Result<()> {
        ensure!(
            (1..=1024).contains(&self.queue_records),
            "invalid BTC queue records"
        );
        ensure!(
            (512 * 1024..=64 * 1024 * 1024).contains(&self.queue_bytes),
            "BTC queue must accommodate a 300-bar checkpoint"
        );
        ensure!(
            (1024 * 1024..=16 * 1024 * 1024 * 1024usize).contains(&self.archive_bytes),
            "invalid BTC archive bytes"
        );
        ensure!(
            (512 * 1024..=8 * 1024 * 1024).contains(&self.response_bytes),
            "BTC response must accommodate a 300-bar window"
        );
        ensure!(
            (1..=32).contains(&self.clients) && !self.send_timeout.is_zero(),
            "invalid BTC stream limits"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub struct Service {
    minute: Arc<Shared>,
    second: Arc<Shared>,
}

struct Shared {
    state: RwLock<Runtime>,
    writer: mpsc::Sender<Persist>,
    stream: broadcast::Sender<Arc<String>>,
    clients: Arc<Semaphore>,
    config: Config,
    spec: FeedSpec,
}

#[derive(Clone)]
struct Persist {
    sequence: u64,
    bytes: Arc<Vec<u8>>,
    checkpoint: Option<Checkpoint>,
    reserved_bytes: usize,
}

#[derive(Clone)]
struct Checkpoint {
    sequence: u64,
    bars: Vec<Value>,
    sha256: String,
}

struct Runtime {
    epoch: String,
    phase: &'static str,
    reason: String,
    failure: Option<String>,
    published: u64,
    durable: u64,
    pending_records: usize,
    pending_bytes: usize,
    archive_bytes: usize,
    facts: VecDeque<(u64, Arc<Vec<u8>>, Value)>,
    bars: VecDeque<Value>,
    received_at: Option<String>,
    joined_at: Option<String>,
    raw_sha256: Option<String>,
    raw_sequence: Option<u64>,
    validated_sequence: Option<u64>,
    readiness_sequence: Option<u64>,
    durable_checkpoint: Option<Checkpoint>,
}

impl Runtime {
    fn new() -> Self {
        Self::with_epoch(uuid::Uuid::new_v4().simple().to_string())
    }

    fn with_epoch(epoch: String) -> Self {
        Self {
            epoch,
            phase: "not_started",
            reason: "bootstrap_not_started".into(),
            failure: None,
            published: 0,
            durable: 0,
            pending_records: 0,
            pending_bytes: 0,
            archive_bytes: 0,
            facts: VecDeque::new(),
            bars: VecDeque::new(),
            received_at: None,
            joined_at: None,
            raw_sha256: None,
            raw_sequence: None,
            validated_sequence: None,
            readiness_sequence: None,
            durable_checkpoint: None,
        }
    }
}

impl Service {
    pub async fn start(config: Config) -> Result<Self> {
        config.validate()?;
        tokio::fs::create_dir_all(&config.root).await?;
        tokio::fs::create_dir_all(config.root.join("finalized-1s")).await?;
        let epoch = uuid::Uuid::new_v4().simple().to_string();
        let minute = start_feed(config.clone(), MINUTE, epoch.clone()).await?;
        let second = start_feed(config, SECOND, epoch).await?;
        Ok(Self { minute, second })
    }

    pub fn router(&self) -> Router {
        let status_routes = Router::new()
            .route("/v1/btc-research/status", get(status))
            .with_state(Arc::new((self.minute.clone(), self.second.clone())));
        let minute_routes = Router::new()
            .route("/v1/btc-research/bootstrap", get(bootstrap))
            .route("/v1/btc-research/finalized-1m-window", get(window))
            .route("/v1/btc-research/facts", get(facts))
            .route("/v1/btc-research/stream", get(stream))
            .with_state(self.minute.clone());
        let second_routes = Router::new()
            .route("/v1/btc-research/finalized-1s-window", get(window))
            .route("/v1/btc-research/finalized-1s-facts", get(facts))
            .with_state(self.second.clone());
        status_routes.merge(minute_routes).merge(second_routes)
    }
}

async fn start_feed(config: Config, spec: FeedSpec, epoch: String) -> Result<Arc<Shared>> {
    let root = if spec.interval == "1s" {
        config.root.join("finalized-1s")
    } else {
        config.root.clone()
    };
    tokio::fs::create_dir_all(&root).await?;
    let state = Runtime::with_epoch(epoch);
    let path = root.join(format!("facts-{}.jsonl", state.epoch));
    let (writer, receiver) = mpsc::channel(config.queue_records);
    let (stream, _) = broadcast::channel(config.queue_records.max(16));
    let shared = Arc::new(Shared {
        state: RwLock::new(state),
        writer,
        stream,
        clients: Arc::new(Semaphore::new(config.clients)),
        config,
        spec,
    });
    tokio::spawn(writer_task(shared.clone(), receiver, path));
    tokio::spawn(acquisition_task(shared.clone()));
    Ok(shared)
}

async fn publish(
    shared: &Arc<Shared>,
    fact: Value,
    checkpoint: Option<(Vec<Value>, String)>,
) -> Result<u64> {
    let checkpoint_bytes = checkpoint
        .as_ref()
        .map(|(bars, _)| serde_json::to_vec(bars).map(|bytes| bytes.len()))
        .transpose()?
        .unwrap_or(0);
    let (sequence, bytes, wire) = {
        let mut state = shared.state.write().await;
        ensure!(state.failure.is_none(), "BTC component failed");
        let sequence = state
            .published
            .checked_add(1)
            .context("BTC sequence overflow")?;
        let wire = json!({"epoch":state.epoch,"sequence":sequence,"fact":fact});
        let mut encoded = serde_json::to_vec(&wire)?;
        encoded.push(b'\n');
        ensure!(
            encoded.len() + checkpoint_bytes <= shared.config.queue_bytes,
            "BTC fact exceeds queue byte budget"
        );
        ensure!(
            state.pending_records < shared.config.queue_records
                && state.pending_bytes + encoded.len() + checkpoint_bytes
                    <= shared.config.queue_bytes,
            "BTC persistence capacity"
        );
        ensure!(
            state.archive_bytes + state.pending_bytes + encoded.len()
                <= shared.config.archive_bytes,
            "BTC archive capacity"
        );
        let bytes = Arc::new(encoded);
        state.published = sequence;
        state.pending_records += 1;
        state.pending_bytes += bytes.len() + checkpoint_bytes;
        state.facts.push_back((sequence, bytes.clone(), wire));
        while state.facts.len() > 4096 {
            state.facts.pop_front();
        }
        (
            sequence,
            bytes,
            checkpoint.map(|(bars, sha256)| Checkpoint {
                sequence,
                bars,
                sha256,
            }),
        )
    };
    let text = Arc::new(String::from_utf8(bytes.as_ref().clone())?);
    let _ = shared.stream.send(text.clone());
    if shared
        .writer
        .try_send(Persist {
            sequence,
            bytes,
            checkpoint: wire,
            reserved_bytes: checkpoint_bytes + text.len(),
        })
        .is_err()
    {
        let mut state = shared.state.write().await;
        state.failure = Some("persistence_capacity".into());
        state.phase = "failed";
        state.reason = "persistence_capacity".into();
        bail!("BTC persistence capacity")
    }
    Ok(sequence)
}

async fn writer_task(shared: Arc<Shared>, mut receiver: mpsc::Receiver<Persist>, path: PathBuf) {
    let result: Result<()> = async {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await
            .with_context(|| format!("create {}", path.display()))?;
        while let Some(item) = receiver.recv().await {
            file.write_all(&item.bytes).await?;
            file.sync_data().await?;
            let mut state = shared.state.write().await;
            ensure!(
                item.sequence == state.durable + 1,
                "BTC durable sequence discontinuity"
            );
            state.durable = item.sequence;
            state.pending_records -= 1;
            state.pending_bytes -= item.reserved_bytes;
            state.archive_bytes += item.bytes.len();
            if let Some(checkpoint) = item.checkpoint {
                state.durable_checkpoint = Some(checkpoint);
            }
        }
        Ok(())
    }
    .await;
    if let Err(error) = result {
        let mut state = shared.state.write().await;
        state.failure = Some(format!("persistence_failed:{error:#}"));
        state.phase = "failed";
        state.reason = "persistence_failed".into();
    }
}

async fn acquisition_task(shared: Arc<Shared>) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        // The formal collector exports a deliberately Polymarket-only proxy.
        // Binance Vision is an independent official direct source and must not
        // be routed into that rejecting proxy policy.
        .no_proxy()
        .build()
    {
        Ok(v) => v,
        Err(e) => {
            fail(&shared, format!("client:{e}")).await;
            return;
        }
    };
    let mut delay = 1u64;
    loop {
        if shared.state.read().await.failure.is_some() {
            return;
        }
        match acquire_once(&shared, &client).await {
            Ok(()) => delay = 1,
            Err(error) => {
                {
                    let mut state = shared.state.write().await;
                    if state.raw_sequence.is_none() {
                        state.phase = "not_started";
                        state.reason = "bootstrap_retrying".into();
                    } else if state.phase != "failed" {
                        state.phase = "reconnecting";
                        state.reason = "live_reconnecting".into();
                    }
                    if state.failure.is_some() {
                        eprintln!(
                            "{}",
                            json!({"stage":"btc_research_terminal_failure","interval":shared.spec.interval,"at":Utc::now(),"error":format!("{error:#}")})
                        );
                        return;
                    }
                }
                eprintln!(
                    "{}",
                    json!({"stage":"btc_research_reconnect","interval":shared.spec.interval,"at":Utc::now(),"error":format!("{error:#}"),"delay_seconds":delay})
                );
                tokio::time::sleep(Duration::from_secs(delay)).await;
                delay = (delay * 2).min(30);
            }
        }
    }
}

async fn acquire_once(shared: &Arc<Shared>, client: &reqwest::Client) -> Result<()> {
    if shared.state.read().await.raw_sequence.is_none() {
        bootstrap_once(shared, client).await?;
    }
    repair_gap_once(shared, client).await?;
    let (socket, _) = tokio::time::timeout(WS_CONNECT_TIMEOUT, connect_async(shared.spec.ws_url))
        .await
        .context("Binance WS connect timeout")?
        .context("Binance WS connect")?;
    let (mut write, mut read) = socket.split();
    loop {
        let message = tokio::time::timeout(WS_IDLE_TIMEOUT, read.next())
            .await
            .context("Binance WS idle timeout")?
            .context("Binance WS ended")??;
        match message {
            UpstreamMessage::Text(text) => {
                observe_live_with_repair(shared, client, text.as_bytes()).await?
            }
            UpstreamMessage::Binary(bytes) => {
                observe_live_with_repair(shared, client, &bytes).await?
            }
            UpstreamMessage::Ping(payload) => {
                write.send(UpstreamMessage::Pong(payload)).await?;
            }
            UpstreamMessage::Pong(_) => {}
            UpstreamMessage::Close(_) => bail!("Binance WS closed"),
            _ => {}
        }
    }
}

async fn repair_gap_once(shared: &Arc<Shared>, client: &reqwest::Client) -> Result<()> {
    repair_gap_through(shared, client, expected_last_open_for(shared.spec.step_ms)).await
}

async fn repair_gap_through(
    shared: &Arc<Shared>,
    client: &reqwest::Client,
    target: i64,
) -> Result<()> {
    let max_pages = if shared.spec.interval == "1s" { 48 } else { 8 };
    for _ in 0..max_pages {
        let last = {
            let state = shared.state.read().await;
            state
                .bars
                .back()
                .and_then(|bar| bar["open_time_ms"].as_i64())
                .context("BTC recovery requires a bootstrap window")?
        };
        if last >= target {
            return Ok(());
        }
        let chunk_last = target.min(last + MAX_REPAIR_BARS as i64 * shared.spec.step_ms);
        repair_gap_page(shared, client, last, chunk_last).await?;
    }
    bail!("BTC recovery page budget exceeded")
}

async fn repair_gap_page(
    shared: &Arc<Shared>,
    client: &reqwest::Client,
    last: i64,
    expected: i64,
) -> Result<()> {
    {
        let state = shared.state.read().await;
        let current_last = state
            .bars
            .back()
            .and_then(|bar| bar["open_time_ms"].as_i64())
            .context("BTC recovery requires a bootstrap window")?;
        ensure!(
            current_last == last,
            "BTC recovery state changed before page"
        );
    }
    let missing = ((expected - last) / shared.spec.step_ms) as usize;
    ensure!(
        (1..=MAX_REPAIR_BARS).contains(&missing),
        "BTC recovery gap {missing} exceeds {MAX_REPAIR_BARS} bar budget"
    );
    {
        let mut state = shared.state.write().await;
        state.phase = "recovering";
        state.reason = "rest_gap_repair_fetching".into();
    }
    let first = last + shared.spec.step_ms;
    let end = expected + shared.spec.step_ms - 1;
    let params = [
        ("symbol", "BTCUSDT".to_string()),
        ("interval", shared.spec.interval.to_string()),
        ("startTime", first.to_string()),
        ("endTime", end.to_string()),
        ("limit", missing.to_string()),
    ];
    let response = client
        .get(REST_URL)
        .query(&params)
        .header("accept", "application/json")
        .header("user-agent", "MarketCow/native-btc-v1")
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::OK,
        "BTC recovery HTTP {}",
        response.status()
    );
    let raw = read_bounded(response, shared.config.response_bytes).await?;
    let received = timestamp();
    let raw_sha = sha(&raw);
    let rows: Vec<Value> = serde_json::from_slice(&raw).context("BTC recovery JSON")?;
    let repaired = validate_repair_rows_for(&rows, first, expected, shared.spec.step_ms)?;
    let observed_repaired = repaired
        .into_iter()
        .enumerate()
        .map(|(index, mut bar)| {
            let object = bar.as_object_mut().context("BTC recovery bar object")?;
            object.insert("available_at".into(), json!(received));
            object.insert("first_received_at".into(), json!(received));
            object.insert("historical_first_received_at".into(), Value::Null);
            object.insert(
                "source".into(),
                json!(if shared.spec.interval == "1s" {
                    "binance_global_spot_rest"
                } else {
                    "binance_global_spot_rest_gap_repair"
                }),
            );
            object.insert("source_raw_sha256".into(), json!(raw_sha));
            if shared.spec.interval == "1s" {
                rest_second_provenance(object, &received, index, "rest_backfill");
            }
            Ok(bar)
        })
        .collect::<Result<Vec<_>>>()?;
    let raw_fact = json!({
        "schema_version":"marketcow.binance-gap-repair-raw.v1",
        "event_type":format!("{}_gap_repair_raw",shared.spec.fact_prefix),
        "source":"binance_global_spot_rest",
        "instrument_id":"BTCUSDT.BINANCE",
        "request":{"url":REST_URL,"params":{"symbol":"BTCUSDT","interval":shared.spec.interval,"startTime":first.to_string(),"endTime":end.to_string(),"limit":missing.to_string()}},
        "http_status":200,
        "received_at":received,
        "first_received_at":received,
        "available_at":received,
        "historical_first_received_at":Value::Null,
        "raw_sha256":raw_sha,
        "raw_byte_size":raw.len(),
        "raw_base64":base64::engine::general_purpose::STANDARD.encode(&raw),
        "missing_bar_count":missing,
        "decision_boundary":"current_and_future_after_received_at_only"
    });
    let raw_sequence = publish(shared, raw_fact, None).await?;
    let (window, digest) = {
        let mut state = shared.state.write().await;
        let state_last = state
            .bars
            .back()
            .and_then(|bar| bar["open_time_ms"].as_i64())
            .context("BTC recovery state window")?;
        ensure!(state_last == last, "BTC recovery state changed");
        for bar in observed_repaired.iter().cloned() {
            state.bars.push_back(bar);
            while state.bars.len() > shared.spec.bar_count {
                state.bars.pop_front();
            }
        }
        state.received_at = Some(received.clone());
        let window = state.bars.iter().cloned().collect::<Vec<_>>();
        let digest = sha(&serde_json::to_vec(&window)?);
        (window, digest)
    };
    let repaired_fact = json!({
        "schema_version":"marketcow.binance-gap-repair.v1",
        "event_type":format!("{}_gap_repair",shared.spec.fact_prefix),
        "source":"binance_global_spot_rest",
        "instrument_id":"BTCUSDT.BINANCE",
        "first_open_ms":first,
        "last_open_ms":expected,
        "count":observed_repaired.len(),
        "received_at":received,
        "first_received_at":received,
        "available_at":received,
        "historical_first_received_at":Value::Null,
        "source_raw_sha256":raw_sha,
        "raw_fact_sequence":raw_sequence,
        "bars":observed_repaired,
        "ready":false,
        "reason":"awaiting_gap_repair_durability",
        "decision_boundary":"current_and_future_after_received_at_only"
    });
    let sequence = publish(shared, repaired_fact, Some((window, digest))).await?;
    let mut state = shared.state.write().await;
    finish_gap_repair(&mut state, sequence);
    Ok(())
}

fn finish_gap_repair(state: &mut Runtime, sequence: u64) {
    if state.joined_at.is_some() {
        state.phase = "joined";
        state.reason = "awaiting_gap_repair_durability".into();
        state.readiness_sequence = Some(sequence);
    } else {
        // A REST catch-up after bootstrap is not evidence of a live WS join.
        state.phase = "awaiting_live_finalized";
        state.reason = "awaiting_live_finalized_join".into();
        state.readiness_sequence = None;
    }
}

fn validate_repair_rows(rows: &[Value], first: i64, expected: i64) -> Result<Vec<Value>> {
    validate_repair_rows_for(rows, first, expected, STEP_MS)
}

fn validate_repair_rows_for(
    rows: &[Value],
    first: i64,
    expected: i64,
    step_ms: i64,
) -> Result<Vec<Value>> {
    let count = ((expected - first) / step_ms + 1) as usize;
    ensure!(rows.len() == count, "incomplete BTC recovery response");
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            validate_rest_bar_for(
                row,
                first + index as i64 * step_ms,
                expected + step_ms - 1,
                step_ms,
            )
        })
        .collect()
}

async fn bootstrap_once(shared: &Arc<Shared>, client: &reqwest::Client) -> Result<()> {
    {
        let mut state = shared.state.write().await;
        state.phase = "fetching";
        state.reason = "bootstrap_fetching".into();
    }
    let now = Utc::now().timestamp_millis();
    let last = now.div_euclid(shared.spec.step_ms) * shared.spec.step_ms - shared.spec.step_ms;
    let first = last - (shared.spec.bar_count as i64 - 1) * shared.spec.step_ms;
    let params = [
        ("symbol", "BTCUSDT".to_string()),
        ("interval", shared.spec.interval.to_string()),
        ("startTime", first.to_string()),
        ("endTime", (last + shared.spec.step_ms - 1).to_string()),
        ("limit", shared.spec.bar_count.to_string()),
    ];
    let response = client
        .get(REST_URL)
        .query(&params)
        .header("accept", "application/json")
        .header("user-agent", "MarketCow/native-btc-v1")
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::OK,
        "bootstrap HTTP {}",
        response.status()
    );
    let raw = read_bounded(response, shared.config.response_bytes).await?;
    let received = timestamp();
    let raw_sha = sha(&raw);
    let request_params = json!({"symbol":"BTCUSDT","interval":shared.spec.interval,"startTime":first.to_string(),"endTime":(last+shared.spec.step_ms-1).to_string(),"limit":shared.spec.bar_count.to_string()});
    let raw_fact = json!({"schema_version":"marketcow.binance-bootstrap-raw.v1","event_type":format!("{}_bootstrap_raw",shared.spec.fact_prefix),
        "source":"binance_global_spot_rest","instrument_id":"BTCUSDT.BINANCE","request":{"url":REST_URL,"params":request_params},
        "http_status":200,"received_at":received,"first_received_at":received,"available_at":received,"historical_first_received_at":Value::Null,
        "raw_sha256":raw_sha,"raw_byte_size":raw.len(),"raw_base64":base64::engine::general_purpose::STANDARD.encode(&raw),
        "decision_boundary":"current_and_future_after_received_at_only"});
    let raw_sequence = publish(shared, raw_fact, None).await?;
    let rows: Vec<Value> = serde_json::from_slice(&raw).context("bootstrap JSON")?;
    ensure!(rows.len() == shared.spec.bar_count, "incomplete bootstrap");
    let mut bars = VecDeque::new();
    for (index, row) in rows.iter().enumerate() {
        let mut bar = validate_rest_bar_for(
            row,
            first + index as i64 * shared.spec.step_ms,
            last + shared.spec.step_ms - 1,
            shared.spec.step_ms,
        )?;
        if shared.spec.interval == "1s" {
            let object = bar.as_object_mut().context("BTC bootstrap bar object")?;
            object.insert("available_at".into(), json!(received));
            object.insert("first_received_at".into(), json!(received));
            object.insert("source".into(), json!("binance_global_spot_rest"));
            object.insert("source_raw_sha256".into(), json!(raw_sha));
            rest_second_provenance(object, &received, index, "rest_bootstrap");
        }
        bars.push_back(bar);
    }
    let manifest = json!({"schema_version":"marketcow.binance-bootstrap-manifest.v1","event_type":format!("{}_bootstrap_validated",shared.spec.fact_prefix),
        "source":"binance_global_spot_rest","instrument_id":"BTCUSDT.BINANCE","interval":shared.spec.interval,"count":shared.spec.bar_count,
        "first_open_ms":first,"last_open_ms":last,"completed_through_ms":last+shared.spec.step_ms-1,"received_at":received,"first_received_at":received,
        "available_at":received,"historical_first_received_at":Value::Null,"raw_sha256":raw_sha,"raw_fact_sequence":raw_sequence,
        "continuous":true,"unfinished_rows":0,"gaps":[],"ready":false,"reason":"awaiting_live_finalized_join",
        "decision_boundary":"current_and_future_after_received_at_only","bars":bars});
    let validated_sequence = publish(shared, manifest, None).await?;
    let checkpoint_bars = bars
        .into_iter()
        .map(|mut bar| {
            let object = bar.as_object_mut().unwrap();
            object.insert("available_at".into(), json!(received));
            object.insert("first_received_at".into(), json!(received));
            object.insert("source".into(), json!("binance_global_spot_rest"));
            object.insert("source_raw_sha256".into(), json!(raw_sha));
            bar
        })
        .collect();
    let mut state = shared.state.write().await;
    state.phase = "awaiting_live_finalized";
    state.reason = "awaiting_live_finalized_join".into();
    state.bars = checkpoint_bars;
    state.received_at = Some(received);
    state.raw_sha256 = Some(raw_sha);
    state.raw_sequence = Some(raw_sequence);
    state.validated_sequence = Some(validated_sequence);
    Ok(())
}

async fn observe_live(shared: &Arc<Shared>, raw: &[u8]) -> Result<()> {
    observe_live_at(shared, raw, timestamp()).await
}

async fn observe_live_with_repair(
    shared: &Arc<Shared>,
    client: &reqwest::Client,
    raw: &[u8],
) -> Result<()> {
    ensure!(
        raw.len() <= shared.config.response_bytes,
        "WS message too large"
    );
    let value: Value = serde_json::from_slice(raw)?;
    if value["e"] == "kline" && value["k"]["i"] == shared.spec.interval && value["k"]["x"] == true {
        ensure!(
            value["s"] == "BTCUSDT" && value["k"]["s"] == "BTCUSDT",
            "live instrument mismatch"
        );
        let bar = validate_live_bar_for(&value, shared.spec.step_ms)?;
        let position = bar["open_time_ms"].as_i64().context("live open")?;
        let last = shared
            .state
            .read()
            .await
            .bars
            .back()
            .and_then(|bar| bar["open_time_ms"].as_i64())
            .context("live before bootstrap")?;
        if position > last + shared.spec.step_ms {
            repair_gap_through(shared, client, position - shared.spec.step_ms).await?;
        }
    }
    observe_live(shared, raw).await
}

async fn observe_live_at(shared: &Arc<Shared>, raw: &[u8], received: String) -> Result<()> {
    ensure!(
        raw.len() <= shared.config.response_bytes,
        "WS message too large"
    );
    let value: Value = serde_json::from_slice(raw)?;
    if value["e"] != "kline" || value["k"]["i"] != shared.spec.interval || value["k"]["x"] != true {
        return Ok(());
    }
    ensure!(
        value["s"] == "BTCUSDT" && value["k"]["s"] == "BTCUSDT",
        "live instrument mismatch"
    );
    let raw_sha = sha(raw);
    let mut bar = validate_live_bar_for(&value, shared.spec.step_ms)?;
    if shared.spec.interval == "1s" {
        second_base_volume(bar.as_object_mut().context("BTC live bar object")?);
    }
    let source_event_time_ms = if shared.spec.interval == "1s" {
        Some(
            value["E"]
                .as_i64()
                .context("missing Binance WS event time")?,
        )
    } else {
        None
    };
    let (relation, bars, digest, bootstrap_sha, first_join) = {
        let mut state = shared.state.write().await;
        ensure!(
            state.phase != "failed" && !state.bars.is_empty(),
            "live before bootstrap"
        );
        let position = bar["open_time_ms"].as_i64().context("live open")?;
        let last = state
            .bars
            .back()
            .and_then(|v| v["open_time_ms"].as_i64())
            .context("last open")?;
        let relation = if position == last {
            ensure!(
                same_bar(state.bars.back().unwrap(), &bar),
                "bootstrap live conflict"
            );
            let bootstrap_raw_sha256 = state.raw_sha256.clone();
            let current = state.bars.back_mut().unwrap().as_object_mut().unwrap();
            current.insert("available_at".into(), json!(received));
            current.insert("first_received_at".into(), json!(received));
            current.insert(
                "source".into(),
                json!(if shared.spec.interval == "1s" {
                    "binance_global_spot_websocket"
                } else {
                    "binance_global_spot_rest+websocket_confirmed"
                }),
            );
            current.insert("source_raw_sha256".into(), json!(raw_sha));
            current.insert("bootstrap_raw_sha256".into(), json!(bootstrap_raw_sha256));
            if let Some(source_at) = source_event_time_ms {
                ws_second_provenance(current, source_at);
            }
            "exact_overlap"
        } else if position == last + shared.spec.step_ms {
            let mut observed = bar.clone();
            let current = observed.as_object_mut().unwrap();
            current.insert("available_at".into(), json!(received));
            current.insert("first_received_at".into(), json!(received));
            current.insert("source".into(), json!("binance_global_spot_websocket"));
            current.insert("source_raw_sha256".into(), json!(raw_sha));
            if let Some(source_at) = source_event_time_ms {
                ws_second_provenance(current, source_at);
            }
            state.bars.push_back(observed);
            while state.bars.len() > shared.spec.bar_count {
                state.bars.pop_front();
            }
            "next_minute"
        } else if position < last {
            return Ok(());
        } else {
            state.phase = "failed";
            state.reason = "live_join_gap".into();
            state.failure = Some("live_join_gap".into());
            bail!("live join gap")
        };
        let first_join = state.phase != "joined";
        state.phase = "joined";
        state.reason = "awaiting_join_durability".into();
        if first_join {
            state.joined_at = Some(received.clone());
        }
        let bars = state.bars.iter().cloned().collect::<Vec<_>>();
        let digest = sha(&serde_json::to_vec(&bars)?);
        (relation, bars, digest, state.raw_sha256.clone(), first_join)
    };
    let join = json!({"schema_version":"marketcow.binance-bootstrap-join.v1","event_type":format!("{}_bootstrap_live_join",shared.spec.fact_prefix),
        "source":"binance_global_spot_websocket","instrument_id":"BTCUSDT.BINANCE","relation":relation,
        "open_time_ms":bar["open_time_ms"],"bar":bar,"first_received_at":received,"available_at":received,
        "source_raw_sha256":raw_sha,"bootstrap_raw_sha256":bootstrap_sha,"ready":true,"reason":"live_finalized_join_validated"});
    let sequence = publish(shared, join, Some((bars, digest))).await?;
    if first_join {
        shared.state.write().await.readiness_sequence = Some(sequence);
    }
    Ok(())
}

fn validate_rest_bar(row: &Value, expected: i64, completed: i64) -> Result<Value> {
    validate_rest_bar_for(row, expected, completed, STEP_MS)
}
fn validate_rest_bar_for(
    row: &Value,
    expected: i64,
    completed: i64,
    step_ms: i64,
) -> Result<Value> {
    let values = row.as_array().context("invalid kline shape")?;
    ensure!(values.len() == 12, "invalid kline shape");
    ensure!(
        values[0].as_i64() == Some(expected)
            && values[6].as_i64() == Some(expected + step_ms - 1)
            && expected + step_ms - 1 <= completed,
        "invalid kline window"
    );
    normalized(values)
}
fn validate_live_bar_for(value: &Value, step_ms: i64) -> Result<Value> {
    let k = &value["k"];
    let values = vec![
        k["t"].clone(),
        k["o"].clone(),
        k["h"].clone(),
        k["l"].clone(),
        k["c"].clone(),
        k["v"].clone(),
        k["T"].clone(),
        k["q"].clone(),
        k["n"].clone(),
        k["V"].clone(),
        k["Q"].clone(),
        k["B"].clone(),
    ];
    validate_rest_bar_for(
        &Value::Array(values),
        k["t"].as_i64().context("live open")?,
        k["T"].as_i64().context("live close")?,
        step_ms,
    )
}

fn second_base_volume(bar: &mut serde_json::Map<String, Value>) {
    if let Some(volume) = bar.remove("volume") {
        bar.insert("base_volume".into(), volume);
    }
}
fn rest_second_provenance(
    bar: &mut serde_json::Map<String, Value>,
    received: &str,
    index: usize,
    kind: &str,
) {
    second_base_volume(bar);
    bar.insert("final".into(), json!(true));
    bar.insert("source_event_time_ms".into(), Value::Null);
    bar.insert("source_response_received_at".into(), json!(received));
    bar.insert("source_row_index".into(), json!(index));
    bar.insert("provenance_kind".into(), json!(kind));
    bar.insert("historical_first_received_at".into(), Value::Null);
}

fn ws_second_provenance(bar: &mut serde_json::Map<String, Value>, source_at: i64) {
    second_base_volume(bar);
    bar.insert("final".into(), json!(true));
    bar.insert("source_event_time_ms".into(), json!(source_at));
    bar.insert("source_response_received_at".into(), Value::Null);
    bar.insert("source_row_index".into(), Value::Null);
    bar.insert("provenance_kind".into(), json!("live_final"));
    bar.insert("historical_first_received_at".into(), Value::Null);
}
fn normalized(v: &[Value]) -> Result<Value> {
    for i in [1usize, 2, 3, 4, 5, 7, 9, 10] {
        let s = v[i].as_str().context("decimal string")?;
        ensure!(
            s.parse::<rust_decimal::Decimal>().is_ok(),
            "invalid decimal"
        );
    }
    let open = v[1].as_str().unwrap().parse::<rust_decimal::Decimal>()?;
    let high = v[2].as_str().unwrap().parse::<rust_decimal::Decimal>()?;
    let low = v[3].as_str().unwrap().parse::<rust_decimal::Decimal>()?;
    let close = v[4].as_str().unwrap().parse::<rust_decimal::Decimal>()?;
    ensure!(
        low <= open && low <= close && high >= open && high >= close,
        "invalid OHLC"
    );
    let trades = v[8].as_u64().context("trade count")?;
    Ok(
        json!({"open_time_ms":v[0],"close_time_ms":v[6],"open":v[1],"high":v[2],"low":v[3],"close":v[4],
        "volume":v[5],"quote_volume":v[7],"trade_count":trades,"taker_buy_base_volume":v[9],"taker_buy_quote_volume":v[10]}),
    )
}
fn same_bar(a: &Value, b: &Value) -> bool {
    [
        "open_time_ms",
        "close_time_ms",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "base_volume",
        "quote_volume",
        "trade_count",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]
    .iter()
    .all(|key| a[*key] == b[*key])
}
fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
}
fn sha(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
async fn read_bounded(mut response: reqwest::Response, max_bytes: usize) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            chunk.len() <= max_bytes.saturating_sub(body.len()),
            "BTC source response too large"
        );
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
async fn fail(shared: &Arc<Shared>, reason: String) {
    let mut state = shared.state.write().await;
    state.phase = "failed";
    state.reason = "startup_failed".into();
    state.failure = Some(reason);
}

fn expected_last_open() -> i64 {
    expected_last_open_for(STEP_MS)
}
fn expected_last_open_for(step_ms: i64) -> i64 {
    Utc::now().timestamp_millis().div_euclid(step_ms) * step_ms - step_ms
}
fn bootstrap_json(state: &Runtime) -> Value {
    bootstrap_json_for(state, MINUTE)
}
fn bootstrap_json_for(state: &Runtime, spec: FeedSpec) -> Value {
    bootstrap_json_for_expected(state, spec, expected_last_open_for(spec.step_ms))
}
fn bootstrap_json_for_expected(state: &Runtime, spec: FeedSpec, expected: i64) -> Value {
    let current = state
        .durable_checkpoint
        .as_ref()
        .and_then(|c| c.bars.last())
        .and_then(|b| b["open_time_ms"].as_i64())
        == Some(expected);
    let ready = state.phase == "joined"
        && state.readiness_sequence.is_some_and(|v| v <= state.durable)
        && current;
    json!({"schema_version":"marketcow.binance-bootstrap-status.v1","epoch":state.epoch,"instrument_id":"BTCUSDT.BINANCE","interval":spec.interval,
        "phase":state.phase,"ready":ready,"reason":if ready{"ready"}else{&state.reason},"minimum_bars":spec.bar_count,
        "received_at":state.received_at,"available_at":state.received_at,"joined_at":state.joined_at,"historical_first_received_at":Value::Null,
        "raw_sha256":state.raw_sha256,"first_open_ms":state.bars.front().and_then(|v|v["open_time_ms"].as_i64()),
        "last_open_ms":state.bars.back().and_then(|v|v["open_time_ms"].as_i64()),"raw_sequence":state.raw_sequence,
        "validated_sequence":state.validated_sequence,"readiness_sequence":state.readiness_sequence,"durable_watermark":state.durable,
        "durable_checkpoint_sequence":state.durable_checkpoint.as_ref().map(|c|c.sequence),
        "durable_window_sha256":state.durable_checkpoint.as_ref().map(|c|&c.sha256),"durable_window_current":current,
        "failure":state.failure,"decision_boundary":"current_and_future_after_received_at_only"})
}

async fn status(State(feeds): State<Arc<(Arc<Shared>, Arc<Shared>)>>) -> Json<Value> {
    let s = feeds.0.state.read().await;
    let second = feeds.1.state.read().await;
    Json(status_json_for_expected(
        &s,
        &second,
        expected_last_open_for(MINUTE.step_ms),
        expected_last_open_for(SECOND.step_ms),
    ))
}
fn status_json_for_expected(
    minute: &Runtime,
    second: &Runtime,
    minute_expected: i64,
    second_expected: i64,
) -> Value {
    json!({"epoch":minute.epoch,"published":minute.published,"durable":minute.durable,"pending_count":minute.pending_records,"pending_bytes":minute.pending_bytes,"error":minute.failure,"stopped":false,"startup_validation":{"mode":"new_epoch","records_checked":0},"btc_1m_bootstrap":bootstrap_json_for_expected(minute,MINUTE,minute_expected),"btc_1s_bootstrap":bootstrap_json_for_expected(second,SECOND,second_expected)})
}
async fn bootstrap(State(shared): State<Arc<Shared>>) -> Json<Value> {
    let s = shared.state.read().await;
    Json(bootstrap_json(&s))
}
async fn window(State(shared): State<Arc<Shared>>) -> Response {
    let s = shared.state.read().await;
    let spec = shared.spec;
    let expected = expected_last_open_for(spec.step_ms);
    match window_json_for_expected(&s, spec, expected) {
        Some(value) => match serde_json::to_vec(&value) {
            Ok(bytes) if bytes.len() <= shared.config.response_bytes => Json(value).into_response(),
            Ok(_) => error(StatusCode::PAYLOAD_TOO_LARGE, "response_size_exceeded"),
            Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "encode_failed"),
        },
        None => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "durable_finalized_window_unavailable_or_stale",
        ),
    }
}
fn window_json_for_expected(s: &Runtime, spec: FeedSpec, expected: i64) -> Option<Value> {
    let Some(c) = &s.durable_checkpoint else {
        return None;
    };
    if !bootstrap_json_for_expected(s, spec, expected)["ready"]
        .as_bool()
        .unwrap_or(false)
        || c.bars.len() != spec.bar_count
        || c.bars.last().and_then(|b| b["open_time_ms"].as_i64()) != Some(expected)
    {
        return None;
    };
    Some(
        json!({"schema_version":spec.window_schema,"epoch":s.epoch,"instrument_id":"BTCUSDT.BINANCE","interval":spec.interval,"count":c.bars.len(),"first_open_ms":c.bars.first().unwrap()["open_time_ms"],"last_open_ms":c.bars.last().unwrap()["open_time_ms"],"expected_last_open_ms":expected,"latest_available_at":c.bars.last().unwrap()["available_at"],"checkpoint_sequence":c.sequence,"durable_watermark":s.durable,"window_sha256":c.sha256,"bootstrap_raw_sha256":s.raw_sha256,"bars":c.bars,"decision_boundary":"current_and_future_after_each_bar_available_at_only"}),
    )
}

#[derive(Deserialize)]
struct FactsQuery {
    after: u64,
    through: u64,
    limit: usize,
}
async fn facts(State(shared): State<Arc<Shared>>, Query(q): Query<FactsQuery>) -> Response {
    let s = shared.state.read().await;
    if q.after > q.through || q.through > s.durable || q.limit == 0 || q.limit > 4096 {
        return error(StatusCode::BAD_REQUEST, "invalid_budget_or_watermark");
    };
    let records = s
        .facts
        .iter()
        .filter(|(seq, _, _)| *seq > q.after && *seq <= q.through)
        .take(q.limit)
        .map(|(_, _, v)| v.clone())
        .collect::<Vec<_>>();
    if q.after < q.through && records.is_empty() {
        return error(StatusCode::GONE, "fact_retention_exceeded");
    };
    let next = records
        .last()
        .and_then(|v| v["sequence"].as_u64())
        .unwrap_or(q.after);
    let response = json!({"schema_version":"marketcow.btc-research.page.v1","through":q.through,"after":q.after,"next_after":next,"scanned":records.len(),"records":records,"asof_mode":"observed","limitation":Value::Null});
    match serde_json::to_vec(&response) {
        Ok(v) if v.len() <= shared.config.response_bytes => Json(response).into_response(),
        Ok(_) => error(StatusCode::PAYLOAD_TOO_LARGE, "response_size_exceeded"),
        Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "encode_failed"),
    }
}

async fn stream(State(shared): State<Arc<Shared>>, ws: WebSocketUpgrade) -> Response {
    let Ok(permit) = shared.clients.clone().try_acquire_owned() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "btc_stream_capacity");
    };
    ws.max_message_size(4096)
        .max_frame_size(4096)
        .on_upgrade(move |socket| stream_socket(shared, socket, permit))
}
async fn stream_socket(
    shared: Arc<Shared>,
    mut socket: WebSocket,
    _permit: tokio::sync::OwnedSemaphorePermit,
) {
    let mut receiver = shared.stream.subscribe();
    let hello = {
        let s = shared.state.read().await;
        json!({"schema_version":"marketcow.btc-research.stream-start.v1","epoch":s.epoch,"replay":false,"continuity":"unverified","durability":"asynchronous","btc_1m_bootstrap":bootstrap_json(&s)})
    };
    if send(&shared, &mut socket, serde_json::to_string(&hello).unwrap())
        .await
        .is_err()
    {
        return;
    }
    loop {
        tokio::select! {message=receiver.recv()=>match message{Ok(v)=>if send(&shared,&mut socket,v.as_ref().clone()).await.is_err(){break},Err(_)=>break},input=socket.recv()=>{if input.is_none(){break}else{break}}}
    }
    let _ = socket.close().await;
}
async fn send(shared: &Shared, socket: &mut WebSocket, text: String) -> Result<()> {
    tokio::time::timeout(
        shared.config.send_timeout,
        socket.send(Message::Text(text.into())),
    )
    .await
    .context("BTC send timeout")??;
    Ok(())
}
fn error(status: StatusCode, code: &str) -> Response {
    (
        status,
        Json(json!({"detail":{"code":code,"message":code,"retryable":status.is_server_error()}})),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    const SECOND_FIXTURE_FIRST_MS: i64 = 1_790_553_300_000;
    const SECOND_FIXTURE_REST_RECEIVED: &str = "2026-09-28T00:00:00.250000Z";
    const SECOND_FIXTURE_LIVE_RECEIVED: &str = "2026-09-28T00:00:00.500000Z";
    fn fixture(queue_records: usize) -> (Arc<Shared>, mpsc::Receiver<Persist>) {
        fixture_for(queue_records, MINUTE)
    }
    fn fixture_for(queue_records: usize, spec: FeedSpec) -> (Arc<Shared>, mpsc::Receiver<Persist>) {
        let (writer, receiver) = mpsc::channel(queue_records);
        let (stream, _) = broadcast::channel(16);
        (
            Arc::new(Shared {
                state: RwLock::new(Runtime::new()),
                writer,
                stream,
                clients: Arc::new(Semaphore::new(1)),
                config: Config {
                    root: PathBuf::from("unused"),
                    archive_bytes: 1024 * 1024,
                    queue_records,
                    queue_bytes: if spec.interval == "1s" {
                        1024 * 1024
                    } else {
                        65536
                    },
                    response_bytes: 65536,
                    clients: 1,
                    send_timeout: Duration::from_secs(1),
                },
                spec,
            }),
            receiver,
        )
    }
    #[test]
    fn validates_bar_and_rejects_gap_shape() {
        let row = json!([
            60000, "100", "102", "99", "101", "10", 119999, "1000", 12, "4", "400", "0"
        ]);
        let bar = validate_rest_bar(&row, 60000, 119999).unwrap();
        assert_eq!(bar["close"], "101");
        assert!(validate_rest_bar(&row, 120000, 179999).is_err());
    }
    #[test]
    fn bootstrap_status_is_not_ready_without_durable_join() {
        let state = Runtime::new();
        let value = bootstrap_json(&state);
        assert_eq!(value["ready"], false);
        assert_eq!(value["phase"], "not_started");
    }
    #[test]
    fn exact_bar_comparison_ignores_evidence() {
        let mut a = json!({"open_time_ms":1,"close_time_ms":2,"open":"1","high":"1","low":"1","close":"1","volume":"1","quote_volume":"1","trade_count":1,"taker_buy_base_volume":"1","taker_buy_quote_volume":"1"});
        let b = a.clone();
        a.as_object_mut()
            .unwrap()
            .insert("available_at".into(), json!("x"));
        assert!(same_bar(&a, &b));
    }

    #[test]
    fn recovery_rows_must_be_complete_and_contiguous() {
        let first = json!([
            60000, "100", "102", "99", "101", "10", 119999, "1000", 12, "4", "400", "0"
        ]);
        let second = json!([
            120000, "101", "103", "100", "102", "11", 179999, "1100", 13, "5", "500", "0"
        ]);
        let repaired =
            validate_repair_rows(&[first.clone(), second.clone()], 60000, 120000).unwrap();
        assert_eq!(repaired.len(), 2);
        assert_eq!(repaired[1]["open_time_ms"], 120000);
        assert!(validate_repair_rows(&[first.clone()], 60000, 120000).is_err());
        assert!(validate_repair_rows(&[first, second], 120000, 180000).is_err());
    }

    fn second_rows(first: i64) -> Vec<Value> {
        (0..SECOND.bar_count)
            .map(|index| {
                let open = first + index as i64 * SECOND.step_ms;
                json!([
                    open,
                    "100",
                    "102",
                    "99",
                    "101",
                    "10",
                    open + 999,
                    "1000",
                    12,
                    "4",
                    "400",
                    "0"
                ])
            })
            .collect()
    }

    fn second_window_fixture(first: i64) -> Vec<Value> {
        let received = SECOND_FIXTURE_REST_RECEIVED;
        second_rows(first)
            .iter()
            .enumerate()
            .map(|(index, row)| {
                let mut bar = validate_rest_bar_for(
                    row,
                    first + index as i64 * 1_000,
                    first + 299_999,
                    1_000,
                )
                .unwrap();
                let object = bar.as_object_mut().unwrap();
                object.insert("available_at".into(), json!(received));
                object.insert("first_received_at".into(), json!(received));
                object.insert("source".into(), json!("binance_global_spot_rest"));
                object.insert("source_raw_sha256".into(), json!("a".repeat(64)));
                rest_second_provenance(object, received, index, "rest_bootstrap");
                bar
            })
            .collect()
    }

    #[test]
    fn second_rest_rows_are_contiguous_final_and_have_no_invented_event_time() {
        let first = SECOND_FIXTURE_FIRST_MS;
        let rows = second_rows(first);
        let bars = second_window_fixture(first);
        assert_eq!(bars.len(), 300);
        assert_eq!(bars[0]["open_time_ms"], first);
        assert_eq!(bars[299]["close_time_ms"], first + 299_999);
        assert_eq!(bars[0]["source_event_time_ms"], Value::Null);
        assert_eq!(bars[0]["historical_first_received_at"], Value::Null);
        assert_eq!(
            bars[0]["source_response_received_at"],
            SECOND_FIXTURE_REST_RECEIVED
        );
        assert_eq!(bars[299]["source_row_index"], 299);
        assert_eq!(bars[299]["final"], true);
        assert_eq!(bars[0]["source"], "binance_global_spot_rest");
        assert_eq!(bars[0]["provenance_kind"], "rest_bootstrap");
        assert_eq!(bars[0]["base_volume"], "10");
        assert!(bars[0].get("volume").is_none());
        let rest_received_ms = chrono::DateTime::parse_from_rfc3339(SECOND_FIXTURE_REST_RECEIVED)
            .unwrap()
            .timestamp_millis();
        assert!(
            bars.iter()
                .all(|bar| bar["close_time_ms"].as_i64().unwrap() < rest_received_ms)
        );
        assert!(validate_repair_rows_for(&rows[..299], first, first + 299_000, 1_000).is_err());
        let mut duplicate = rows.clone();
        duplicate[5] = duplicate[4].clone();
        assert!(validate_repair_rows_for(&duplicate, first, first + 299_000, 1_000).is_err());
    }

    #[test]
    fn second_rest_backfill_keeps_source_identity_and_separate_provenance() {
        let first = SECOND_FIXTURE_FIRST_MS;
        let mut bar =
            validate_rest_bar_for(&second_rows(first)[0], first, first + 999, 1_000).unwrap();
        let object = bar.as_object_mut().unwrap();
        object.insert("source".into(), json!("binance_global_spot_rest"));
        rest_second_provenance(object, SECOND_FIXTURE_REST_RECEIVED, 0, "rest_backfill");
        assert_eq!(bar["source"], "binance_global_spot_rest");
        assert_eq!(bar["provenance_kind"], "rest_backfill");
        assert_eq!(bar["base_volume"], "10");
        assert_eq!(bar["source_event_time_ms"], Value::Null);
    }

    #[test]
    fn second_window_requires_durable_current_checkpoint_and_hashes_canonical_bars() {
        let first = SECOND_FIXTURE_FIRST_MS;
        let bars = second_window_fixture(first);
        let digest = sha(&serde_json::to_vec(&bars).unwrap());
        let mut state = Runtime::with_epoch("0123456789abcdef0123456789abcdef".into());
        state.phase = "joined";
        state.reason = "awaiting_join_durability".into();
        state.published = 3;
        state.readiness_sequence = Some(3);
        state.bars = bars.clone().into();
        state.durable_checkpoint = Some(Checkpoint {
            sequence: 3,
            bars,
            sha256: digest.clone(),
        });
        let expected = first + 299_000;
        assert!(window_json_for_expected(&state, SECOND, expected).is_none());
        state.durable = 3;
        let window = window_json_for_expected(&state, SECOND, expected).unwrap();
        assert_eq!(window["schema_version"], SECOND.window_schema);
        assert_eq!(window["count"], 300);
        assert_eq!(window["checkpoint_sequence"], 3);
        assert_eq!(window["window_sha256"], digest);
        assert_eq!(sha(&serde_json::to_vec(&window["bars"]).unwrap()), digest);
        assert!(window_json_for_expected(&state, SECOND, expected + 1_000).is_none());
        assert!(bootstrap_json_for_expected(&state, SECOND, expected + 1_000)["ready"] == false);
    }

    #[tokio::test]
    async fn fixed_second_serializer_vector() {
        let first = SECOND_FIXTURE_FIRST_MS;
        let last = first + 299_000;
        let (shared, mut disk) = fixture_for(2, SECOND);
        {
            let mut state = shared.state.write().await;
            state.epoch = "0123456789abcdef0123456789abcdef".into();
            state.bars = second_window_fixture(first).into();
            state.phase = "awaiting_live_finalized";
            state.reason = "awaiting_live_finalized_join".into();
            state.raw_sha256 = Some("a".repeat(64));
            state.raw_sequence = Some(1);
            state.validated_sequence = Some(2);
            state.published = 2;
            state.durable = 2;
            state.received_at = Some(SECOND_FIXTURE_REST_RECEIVED.into());
        }
        let event = json!({"e":"kline","E":last+1_025,"s":"BTCUSDT","k":{
            "s":"BTCUSDT","i":"1s","x":true,"t":last,"T":last+999,
            "o":"100","h":"102","l":"99","c":"101","v":"10","q":"1000",
            "n":12,"V":"4","Q":"400","B":"0"}});
        observe_live_at(
            &shared,
            &serde_json::to_vec(&event).unwrap(),
            SECOND_FIXTURE_LIVE_RECEIVED.into(),
        )
        .await
        .unwrap();
        let persisted = disk.recv().await.unwrap();
        assert_eq!(persisted.sequence, 3);
        let checkpoint = persisted.checkpoint.unwrap();
        {
            let mut state = shared.state.write().await;
            state.durable = 3;
            state.durable_checkpoint = Some(checkpoint);
        }
        let state = shared.state.read().await;
        let window = window_json_for_expected(&state, SECOND, last).unwrap();
        let minute = Runtime::with_epoch(state.epoch.clone());
        let status = status_json_for_expected(&minute, &state, first + 240_000, last);
        assert_eq!(window["epoch"], status["epoch"]);
        assert_eq!(window["epoch"], status["btc_1s_bootstrap"]["epoch"]);
        assert_eq!(status["btc_1s_bootstrap"]["ready"], true);
        assert_eq!(status["durable"], 0);
        assert_eq!(status["btc_1s_bootstrap"]["durable_watermark"], 3);
        assert_eq!(
            window["bars"][299]["source"],
            "binance_global_spot_websocket"
        );
        assert_eq!(window["bars"][299]["provenance_kind"], "live_final");
        assert!(window["bars"].as_array().unwrap().iter().all(|bar| {
            let available =
                chrono::DateTime::parse_from_rfc3339(bar["available_at"].as_str().unwrap())
                    .unwrap()
                    .timestamp_millis();
            bar["close_time_ms"].as_i64().unwrap() < available
        }));
        assert_eq!(
            window["window_sha256"],
            sha(&serde_json::to_vec(&window["bars"]).unwrap())
        );
        if let Some(path) = std::env::var_os("MARKETCOW_BTC_1S_FIXTURE") {
            std::fs::write(path, serde_json::to_vec_pretty(&window).unwrap()).unwrap();
        }
        if let Some(path) = std::env::var_os("MARKETCOW_BTC_1S_STATUS_FIXTURE") {
            std::fs::write(path, serde_json::to_vec_pretty(&status).unwrap()).unwrap();
        }
    }

    #[tokio::test]
    async fn finalized_second_ws_join_preserves_venue_event_time_and_durable_bar() {
        let first = SECOND_FIXTURE_FIRST_MS;
        let (shared, mut disk) = fixture_for(2, SECOND);
        {
            let mut state = shared.state.write().await;
            state.bars = second_window_fixture(first).into();
            state.phase = "awaiting_live_finalized";
            state.raw_sha256 = Some("b".repeat(64));
            state.raw_sequence = Some(1);
        }
        let last = first + 299_000;
        let event = json!({"e":"kline","E":last+1_025,"s":"BTCUSDT","k":{
            "s":"BTCUSDT","i":"1s","x":true,"t":last,"T":last+999,
            "o":"100","h":"102","l":"99","c":"101","v":"10","q":"1000",
            "n":12,"V":"4","Q":"400","B":"0"}});
        observe_live(&shared, &serde_json::to_vec(&event).unwrap())
            .await
            .unwrap();
        let persisted = disk.recv().await.unwrap();
        assert_eq!(persisted.sequence, 1);
        let checkpoint = persisted.checkpoint.unwrap();
        let joined = checkpoint.bars.last().unwrap();
        assert_eq!(joined["source"], "binance_global_spot_websocket");
        assert_eq!(joined["provenance_kind"], "live_final");
        assert_eq!(joined["base_volume"], "10");
        assert!(joined.get("volume").is_none());
        assert_eq!(joined["source_event_time_ms"], last + 1_025);
        assert_eq!(joined["source_response_received_at"], Value::Null);
        assert_eq!(joined["historical_first_received_at"], Value::Null);
        assert_eq!(joined["bootstrap_raw_sha256"], "b".repeat(64));
        assert_eq!(
            checkpoint.sha256,
            sha(&serde_json::to_vec(&checkpoint.bars).unwrap())
        );
        let state = shared.state.read().await;
        assert_eq!(state.phase, "joined");
        assert_eq!(state.readiness_sequence, Some(1));
        assert_eq!(state.durable, 0);
        assert!(window_json_for_expected(&state, SECOND, last).is_none());
    }

    #[tokio::test]
    async fn second_rest_catchup_waits_for_first_live_finalized_bar() {
        let first = SECOND_FIXTURE_FIRST_MS;
        let (shared, mut disk) = fixture_for(2, SECOND);
        let bootstrap_last = first + 299_000;
        let repaired_open = bootstrap_last + 1_000;
        let live_open = repaired_open + 1_000;
        let repaired_row = second_rows(repaired_open).remove(0);
        let mut repaired =
            validate_rest_bar_for(&repaired_row, repaired_open, repaired_open + 999, 1_000)
                .unwrap();
        let object = repaired.as_object_mut().unwrap();
        object.insert("available_at".into(), json!(SECOND_FIXTURE_REST_RECEIVED));
        object.insert(
            "first_received_at".into(),
            json!(SECOND_FIXTURE_REST_RECEIVED),
        );
        object.insert("source".into(), json!("binance_global_spot_rest"));
        object.insert("source_raw_sha256".into(), json!("c".repeat(64)));
        rest_second_provenance(object, SECOND_FIXTURE_REST_RECEIVED, 0, "rest_backfill");
        {
            let mut state = shared.state.write().await;
            state.bars = second_window_fixture(first).into();
            state.bars.pop_front();
            state.bars.push_back(repaired);
            state.phase = "recovering";
            state.raw_sha256 = Some("b".repeat(64));
            state.raw_sequence = Some(1);
            finish_gap_repair(&mut state, 2);
            assert_eq!(state.phase, "awaiting_live_finalized");
            assert_eq!(state.readiness_sequence, None);
        }
        let event = json!({"e":"kline","E":live_open+1_025,"s":"BTCUSDT","k":{
            "s":"BTCUSDT","i":"1s","x":true,"t":live_open,"T":live_open+999,
            "o":"100","h":"102","l":"99","c":"101","v":"10","q":"1000",
            "n":12,"V":"4","Q":"400","B":"0"}});
        observe_live_at(
            &shared,
            &serde_json::to_vec(&event).unwrap(),
            SECOND_FIXTURE_REST_RECEIVED.into(),
        )
        .await
        .unwrap();
        let item = disk.recv().await.unwrap();
        assert_eq!(item.sequence, 1);
        assert_eq!(
            item.checkpoint.unwrap().bars.last().unwrap()["open_time_ms"],
            live_open
        );
        let state = shared.state.read().await;
        assert_eq!(state.phase, "joined");
        assert_eq!(state.readiness_sequence, Some(1));
        assert!(state.joined_at.is_some());
    }

    #[tokio::test]
    async fn blocked_disk_does_not_block_memory_and_capacity_is_explicit() {
        let (shared, mut disk) = fixture(1);
        let mut subscriber = shared.stream.subscribe();
        let sequence = publish(&shared, json!({"event_type":"one"}), None)
            .await
            .unwrap();
        assert_eq!(sequence, 1);
        let state = shared.state.read().await;
        assert_eq!(
            (state.published, state.durable, state.pending_records),
            (1, 0, 1)
        );
        drop(state);
        assert!(subscriber.try_recv().unwrap().contains("event_type"));
        assert!(
            publish(&shared, json!({"event_type":"two"}), None)
                .await
                .unwrap_err()
                .to_string()
                .contains("capacity")
        );
        assert_eq!(shared.state.read().await.published, 1);
        let queued = disk.recv().await.unwrap();
        assert_eq!(queued.sequence, 1);
    }

    #[tokio::test]
    async fn facts_never_expose_beyond_durable_watermark() {
        let (shared, _disk) = fixture(2);
        publish(&shared, json!({"event_type":"one"}), None)
            .await
            .unwrap();
        let response = facts(
            State(shared.clone()),
            Query(FactsQuery {
                after: 0,
                through: 1,
                limit: 2,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        shared.state.write().await.durable = 1;
        let response = facts(
            State(shared),
            Query(FactsQuery {
                after: 0,
                through: 1,
                limit: 2,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[ignore = "bounded official Binance integration smoke"]
    async fn official_bootstrap_and_live_join_become_durable() {
        let root = tempfile::tempdir().unwrap();
        let service = Service::start(Config {
            root: root.path().to_path_buf(),
            archive_bytes: 8 * 1024 * 1024,
            queue_records: 16,
            queue_bytes: 2 * 1024 * 1024,
            response_bytes: 1024 * 1024,
            clients: 1,
            send_timeout: Duration::from_secs(2),
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                let state = service.minute.state.read().await;
                if bootstrap_json(&state)["ready"] == true {
                    assert_eq!(state.bars.len(), BAR_COUNT);
                    assert!(state.raw_sequence.is_some_and(|v| v <= state.durable));
                    assert!(state.validated_sequence.is_some_and(|v| v <= state.durable));
                    assert!(state.readiness_sequence.is_some_and(|v| v <= state.durable));
                    break;
                }
                assert!(state.failure.is_none(), "{:?}", state.failure);
                drop(state);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    #[ignore = "bounded official Binance REST recovery smoke"]
    async fn official_gap_repair_rebuilds_current_durable_window() {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            root: root.path().to_path_buf(),
            archive_bytes: 8 * 1024 * 1024,
            queue_records: 16,
            queue_bytes: 2 * 1024 * 1024,
            response_bytes: 1024 * 1024,
            clients: 1,
            send_timeout: Duration::from_secs(2),
        };
        let epoch = Runtime::new();
        let path = config.root.join(format!("facts-{}.jsonl", epoch.epoch));
        let (writer, receiver) = mpsc::channel(config.queue_records);
        let (stream, _) = broadcast::channel(16);
        let shared = Arc::new(Shared {
            state: RwLock::new(epoch),
            writer,
            stream,
            clients: Arc::new(Semaphore::new(1)),
            config,
            spec: MINUTE,
        });
        tokio::spawn(writer_task(shared.clone(), receiver, path));
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .unwrap();
        bootstrap_once(&shared, &client).await.unwrap();
        {
            let mut state = shared.state.write().await;
            state.bars.pop_back();
            state.bars.pop_back();
        }
        repair_gap_once(&shared, &client).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let state = shared.state.read().await;
                if state.durable == state.published {
                    assert_eq!(state.bars.len(), BAR_COUNT);
                    assert_eq!(
                        state.bars.back().unwrap()["open_time_ms"],
                        expected_last_open()
                    );
                    assert_eq!(
                        state
                            .durable_checkpoint
                            .as_ref()
                            .unwrap()
                            .bars
                            .last()
                            .unwrap()["open_time_ms"],
                        expected_last_open()
                    );
                    break;
                }
                drop(state);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
