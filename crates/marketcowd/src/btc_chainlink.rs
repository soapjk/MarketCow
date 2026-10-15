//! Authenticated Chainlink BTC/USD 60-second TWAP inside the BTC research owner.
//!
//! Credentials are optional and never logged. Without an explicitly configured
//! private credential file this component remains unavailable; Binance is not
//! substituted for Chainlink. The upstream connection has its own epoch and
//! bounded memory, and has no access to Polymarket order-book publication.
use anyhow::{Context, Result, ensure};
use base64::Engine;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{os::unix::fs::{MetadataExt, PermissionsExt}, path::PathBuf,
    sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}}, time::Duration};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::{broadcast, mpsc}};
use crate::source_snapshot::Published;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, client_async_tls, connect_async,
    tungstenite::{Message, http::Uri}};

const URL: &str = "wss://ws-live-v2.polymarket.com/ws";
const SCHEMA: &str = "marketcow.btc-price-window.v2";
const MAX_FRAME_BYTES: usize = 64 * 1024;
const MAX_POINTS: usize = 600;
const ARCHIVE_RECORDS: usize = 256;
const ARCHIVE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct Service {
    state: Arc<Published<State>>,
    live: broadcast::Sender<Arc<ChainlinkEvent>>,
    archive: mpsc::Sender<ArchiveRecord>,
    archive_bytes: Arc<AtomicUsize>,
    archive_available: Arc<AtomicBool>,
    archive_gaps: Arc<AtomicU64>,
    database: Option<mpsc::Sender<ArchiveRecord>>,
    database_bytes: Arc<AtomicUsize>,
    database_available: Arc<AtomicBool>,
    database_gaps: Arc<AtomicU64>,
}

#[derive(Clone)]
struct ArchiveRecord {
    epoch: String,
    sequence: u64,
    received_at: String,
    raw: Arc<[u8]>,
}

#[derive(Clone)]
struct State {
    phase: &'static str,
    epoch: String,
    stream_serial: u64,
    snapshot_sequence: Option<u64>,
    snapshot_received_at: Option<String>,
    last_seq: Option<u64>,
    last_frame_received_at: Option<String>,
    last_raw: Option<Arc<[u8]>>,
    points: im::Vector<Arc<ChainlinkPoint>>,
}

#[derive(Clone, Serialize)]
struct ChainlinkPoint {
    price:String,received_at:String,source_raw_sha256:String,
    source_sequence:u64,source_time_ms:i64,
}

pub struct ChainlinkEvent {
    snapshot_state: Option<Arc<State>>,
    epoch:String,phase:&'static str,serial:u64,sequence:Option<u64>,
    received_at:Option<String>,raw:Option<Arc<[u8]>>,point:Option<Arc<ChainlinkPoint>>,
    previous_source_time_ms: Option<i64>,
    snapshot: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Credentials {
    #[serde(rename = "apiKey")]
    api_key: String,
    secret: String,
    passphrase: String,
}

impl Service {
    pub fn start(root: PathBuf, credentials_path: Option<PathBuf>) -> Self {
        let (archive, receiver) = mpsc::channel(ARCHIVE_RECORDS);
        let (database, db_receiver) = mpsc::channel(ARCHIVE_RECORDS);
        let (live, _) = broadcast::channel(1024);
        let archive_bytes = Arc::new(AtomicUsize::new(0));
        let database_bytes = Arc::new(AtomicUsize::new(0));
        let archive_available = Arc::new(AtomicBool::new(true));
        let database_available=Arc::new(AtomicBool::new(true));
        let service = Self { state: Arc::new(Published::new(State {
            phase: "credentials_not_configured",
            epoch: uuid::Uuid::new_v4().simple().to_string(),
            stream_serial: 0,
            snapshot_sequence: None,
            snapshot_received_at: None,
            last_seq: None,
            last_frame_received_at: None,
            last_raw: None,
            points: im::Vector::new(),
        })), live, archive, archive_bytes: archive_bytes.clone(),
            archive_available: archive_available.clone(),
            archive_gaps: Arc::new(AtomicU64::new(0)),
            database:Some(database),database_bytes:database_bytes.clone(),
            database_available:database_available.clone(),database_gaps:Arc::new(AtomicU64::new(0)) };
        let db_root=root.clone();
        tokio::task::spawn_blocking(move || database_task(db_root,db_receiver,database_bytes,database_available));
        tokio::task::spawn_blocking(move || archive_task(root, receiver, archive_bytes, archive_available));
        if let Some(path) = credentials_path {
            let owner = service.clone();
            tokio::spawn(async move { owner.run(path).await });
        }
        service
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<ChainlinkEvent>> {
        self.live.subscribe()
    }

    pub async fn stream_snapshot(&self) -> Value {
        let state = self.state.load();
        if state.phase != "live" {
            return stream_unavailable(&state, state.phase);
        }
        let points = state.points.iter().rev().take(300).cloned()
            .collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>();
        let Some(latest) = points.last() else {
            return stream_unavailable(&state, "no_price_points");
        };
        // Staleness is a separate consumer gate, not a continuity break.
        // Keep the authenticated epoch so the next valid quote can advance it.
        let _ = latest;
        stream_snapshot(&state, points)
    }

    pub async fn window(&self) -> Result<Value> {
        let state = self.state.load();
        ensure!(state.phase == "live", "Chainlink feed {}", state.phase);
        let latest = state.points.back().context("Chainlink prices unavailable")?;
        let latest_ms = latest.source_time_ms;
        let response_at = Utc::now();
        let age = response_at.timestamp_millis() - latest_ms;
        ensure!((0..=3_000).contains(&age),
            "Chainlink prices stale");
        let points: Vec<Arc<ChainlinkPoint>> = state.points.iter()
            .rev().take(300).cloned().collect::<Vec<_>>()
            .into_iter().rev().collect();
        let first_ms = points.first().map(|p| p.source_time_ms)
            .context("Chainlink first timestamp missing")?;
        let intervals = points.windows(2).map(|pair| {
            pair[1].source_time_ms - pair[0].source_time_ms
        }).collect::<Vec<_>>();
        let longer_than_one_second = intervals.iter().filter(|gap| **gap > 1_000).count();
        let snapshot_sequence = state.snapshot_sequence
            .context("Chainlink authenticated snapshot missing")?;
        let last_sequence = state.last_seq.context("Chainlink sequence missing")?;
        Ok(json!({"schema_version":SCHEMA,"source":"chainlink_twap_60s",
            "instrument_id":"BTCUSD.CHAINLINK.TWAP60","epoch":state.epoch,
            "source_channel":"price.crypto.twap","source_symbol":"btcusd",
            "window_seconds":60,"first_source_time_ms":first_ms,
            "partial":points.len()<300,
            "quote_interval_diagnostics":{
                "cadence_requirement":"none",
                "intervals_over_1000_ms":longer_than_one_second,
                "max_interval_ms":intervals.iter().max(),
                "latest_interval_ms":intervals.last(),
            },
            "source_continuity":{
                "kind":"polybolt_authenticated_subscription",
                "status":"contiguous_since_snapshot",
                "epoch":state.epoch,
                "sequence_scope":"epoch_and_channel",
                "snapshot_sequence":snapshot_sequence,
                "last_sequence":last_sequence,
                "snapshot_received_at":state.snapshot_received_at,
                "last_frame_received_at":state.last_frame_received_at,
                "dropped_frames":0,
                "snapshot_point_sequence_semantics":"shared_frame_sequence",
            },
            "freshness":{
                "status":"fresh",
                "latest_source_time_ms":latest_ms,
                "latest_received_at":latest.received_at,
                "source_age_ms_at_response":age,
                "response_at":response_at.to_rfc3339(),
                "max_source_age_ms":3_000,
            },
            "archive_diagnostics":{
                "writer_healthy":self.archive_available.load(Ordering::Relaxed),
                "dropped_records":self.archive_gaps.load(Ordering::Relaxed),
            },
            "database_diagnostics":{
                "writer_healthy":self.database_available.load(Ordering::Relaxed),
                "dropped_records":self.database_gaps.load(Ordering::Relaxed),
                "queued_bytes":self.database_bytes.load(Ordering::Relaxed),
            },
            "points":points}))
    }

    pub async fn unavailable_status(&self) -> Value {
        let state = self.state.load();
        let latest = state.points.back();
        let reason = if state.phase == "live" {
            if latest.is_some() { "latest_point_stale_or_invalid" }
            else { "no_price_points" }
        } else {
            state.phase
        };
        json!({"schema_version":"marketcow.btc-price-window-unavailable.v2",
            "source":"chainlink_twap_60s","epoch":state.epoch,
            "status":"unavailable","reason":reason,
            "source_continuity_status":if state.phase == "live" {
                "contiguous_since_snapshot"
            } else { "awaiting_authenticated_snapshot" },
            "snapshot_sequence":state.snapshot_sequence,
            "last_sequence":state.last_seq,
            "latest_source_time_ms":latest.map(|point|point.source_time_ms),
            "latest_received_at":latest.map(|point|&point.received_at),
        })
    }

    async fn run(&self, path: PathBuf) {
        let mut state = (*self.state.load()).clone();
        let credentials = match read_credentials(&path).await {
            Ok(value) => value,
            Err(_) => {
                self.invalidate(&mut state, "credentials_invalid").await;
                return;
            }
        };
        let mut backoff = 1u64;
        loop {
            self.invalidate(&mut state, "connecting").await;
            match self.connect_once(&mut state, &credentials).await {
                Ok(()) => backoff = 1,
                Err(_) => backoff = (backoff * 2).min(30),
            }
            self.invalidate(&mut state, "disconnected").await;
            tokio::time::sleep(Duration::from_secs(backoff)).await;
        }
    }

    async fn invalidate(&self, state: &mut State, phase: &'static str) {
        state.phase = phase;
        state.stream_serial = state.stream_serial.saturating_add(1);
        state.epoch = uuid::Uuid::new_v4().simple().to_string();
        state.snapshot_sequence = None;
        state.snapshot_received_at = None;
        state.last_seq = None;
        state.last_frame_received_at = None;
        state.last_raw = None;
        state.points.clear();
        self.state.publish(state.clone());
        self.publish_event(state,None,false);
    }

    async fn connect_once(&self, state: &mut State, credentials: &Credentials) -> Result<()> {
        let mut stream = tokio::time::timeout(Duration::from_secs(10), connect_chainlink())
            .await??;
        stream.send(Message::Text(json!({"op":"auth","rid":"btc-twap-auth",
            "auth":{"apiKey":credentials.api_key,"secret":credentials.secret,
                "passphrase":credentials.passphrase}}).to_string().into())).await?;
        let auth = next_json(&mut stream).await?;
        ensure!(auth["op"] == "authed" && auth["rid"] == "btc-twap-auth",
            "Chainlink authentication not accepted");
        stream.send(Message::Text(json!({"op":"subscribe","rid":"btc-twap-sub",
            "subscriptions":[{"channel":"price.crypto.twap",
                "filter":{"symbol":"btcusd","window_seconds":60}}]})
            .to_string().into())).await?;
        let subscribed = next_json(&mut stream).await?;
        ensure!(subscribed["op"] == "subscribed"
            && subscribed["rid"] == "btc-twap-sub"
            && subscribed["channel"] == "price.crypto.twap",
            "Chainlink TWAP subscription not accepted");
        while let Some(frame) = tokio::time::timeout(Duration::from_secs(40), stream.next()).await? {
            let frame = frame?;
            match frame {
                Message::Text(text) => self.apply_frame(state, text.as_bytes()).await?,
                Message::Binary(bytes) => self.apply_frame(state, &bytes).await?,
                Message::Close(_) => break,
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
        Ok(())
    }

    async fn apply_frame(&self, state: &mut State, raw: &[u8]) -> Result<()> {
        let result = self.apply_frame_inner(state, raw).await;
        if let Err(error) = &result {
            // A bad sequence, dropped indication or malformed producer frame
            // immediately revokes the old epoch. A new authenticated snapshot
            // is required before any price window is served again.
            let reason = match error.to_string().as_str() {
                "Chainlink frames dropped" => "source_dropped",
                "Chainlink sequence gap" => "source_sequence_gap",
                _ => "source_bad_frame",
            };
            self.invalidate(state, reason).await;
        }
        result
    }

    async fn apply_frame_inner(&self, state: &mut State, raw: &[u8]) -> Result<()> {
        ensure!(raw.len() <= MAX_FRAME_BYTES, "Chainlink frame too large");
        let frame: Value = serde_json::from_slice(raw)?;
        ensure!(frame["channel"] == "price.crypto.twap"
            && frame["v"] == 1, "Chainlink channel changed");
        let dropped = match frame.get("dropped") {
            None | Some(Value::Null) | Some(Value::Bool(false)) => 0,
            Some(Value::Number(value)) => value.as_u64()
                .context("Chainlink dropped count invalid")?,
            _ => anyhow::bail!("Chainlink dropped indicator invalid"),
        };
        ensure!(dropped == 0, "Chainlink frames dropped");
        let seq = frame["seq"].as_u64().context("Chainlink sequence missing")?;
        let payload = &frame["payload"];
        ensure!(payload["symbol"] == "btcusd" && payload["source"] == "chainlink",
            "Chainlink producer changed");
        ensure!(payload["window_seconds"] == 60,
            "Chainlink TWAP window changed");
        let received_at = Utc::now().to_rfc3339();
        let raw_sha = hex::encode(Sha256::digest(raw));
        let previous_source_time_ms = state.points.back()
            .map(|point| point.source_time_ms);
        if let Some(previous) = state.last_seq {
            ensure!(Some(seq) == previous.checked_add(1), "Chainlink sequence gap");
            ensure!(frame["snapshot"] != true,
                "Chainlink unexpected snapshot within source epoch");
        } else {
            ensure!(frame["snapshot"] == true,
                "Chainlink connection has no initial snapshot");
        }
        if frame["snapshot"] == true {
            // Validate the complete authenticated snapshot before making it
            // visible. Every point in this one frame shares the same source seq.
            let mut snapshot_points = im::Vector::new();
            for point in payload["data"].as_array()
                .context("Chainlink snapshot data missing")? {
                push_point(&mut snapshot_points, point, seq, &received_at, &raw_sha)?;
            }
            ensure!(!snapshot_points.is_empty(), "Chainlink snapshot empty");
            state.points = snapshot_points;
            state.snapshot_sequence = Some(seq);
            state.snapshot_received_at = Some(received_at.clone());
        } else {
            push_point(&mut state.points, payload, seq, &received_at, &raw_sha)?;
        }
        state.last_seq = Some(seq);
        state.last_frame_received_at = Some(received_at.clone());
        state.last_raw = Some(Arc::from(raw));
        state.phase = "live";
        state.stream_serial = state.stream_serial.saturating_add(1);
        self.state.publish(state.clone());
        self.publish_event(state,previous_source_time_ms,frame["snapshot"]==true);
        let epoch = state.epoch.clone();
        self.archive_frame(ArchiveRecord { epoch, sequence: seq,
            received_at, raw: Arc::from(raw) });
        Ok(())
    }

    fn publish_event(&self,state:&State,previous_source_time_ms:Option<i64>,snapshot:bool) {
        let _=self.live.send(Arc::new(ChainlinkEvent {
            snapshot_state:if snapshot {Some(self.state.load())}else{None},
            epoch:state.epoch.clone(),phase:state.phase,serial:state.stream_serial,sequence:state.last_seq,
            received_at:state.last_frame_received_at.clone(),raw:state.last_raw.clone(),
            point:state.points.back().cloned(),previous_source_time_ms,snapshot}));
    }

    pub fn event_frame(&self, event: &ChainlinkEvent) -> Value {
        if event.phase != "live" { return json!({"schema_version":"marketcow.btc-price-stream.v2",
            "type":"unavailable","source":"chainlink_twap_60s","instrument_id":"BTCUSD.CHAINLINK.TWAP60",
            "source_epoch":event.epoch,"message_sequence":0,"stream_serial":event.serial,
            "source_sequence":null,"source_sequence_scope":"epoch_and_channel",
            "reason":event.phase,"observed_at":Utc::now().to_rfc3339()}); }
        let seq = event.sequence;
        let raw = event.raw.as_deref().unwrap_or_default();
        let raw_sha = event.point.as_ref().map(|point|point.source_raw_sha256.clone()).unwrap_or_default();
        let received_at = &event.received_at;
        let previous_source_time_ms = event.previous_source_time_ms;
        let event = if event.snapshot {
            let state=event.snapshot_state.as_deref().expect("Chainlink snapshot state");
            stream_snapshot(state, state.points.iter().rev().take(300).cloned()
                .collect::<Vec<_>>().into_iter().rev().collect())
        } else {
            json!({"schema_version":"marketcow.btc-price-stream.v2",
                "type":"point","source":"chainlink_twap_60s",
                "instrument_id":"BTCUSD.CHAINLINK.TWAP60",
                "source_epoch":event.epoch,"message_sequence":seq,
                "stream_serial":event.serial,
                "source_sequence":seq,"source_sequence_scope":"epoch_and_channel",
                "source_channel":"price.crypto.twap","source_symbol":"btcusd",
                "source_continuity":"contiguous_since_authenticated_snapshot",
                "point_action":"upsert_by_source_time_ms",
                "point":event.point,"raw_frame_sha256":raw_sha,
                "raw_evidence_scope":"this_source_frame",
                "raw_frame_base64":base64::engine::general_purpose::STANDARD.encode(raw),
                "received_at":received_at,
                "published_at":Utc::now().to_rfc3339(),
                "source_age_ms_at_emit":event.point.as_ref()
                    .map(|point| point.source_time_ms)
                    .map(|at| Utc::now().timestamp_millis() - at),
                "quote_interval_ms":previous_source_time_ms.and_then(|previous|
                    event.point.as_ref().map(|point|point.source_time_ms - previous)),
                "cadence_requirement":"none"})
        };

        event
    }

    fn archive_frame(&self, record: ArchiveRecord) {
        self.queue_database(&record);
        let bytes = record.raw.len();
        if bytes > ARCHIVE_BYTES {
            self.archive_drop("record_budget",record.sequence);
            return;
        }
        let reserved = self.archive_bytes.fetch_update(Ordering::AcqRel, Ordering::Acquire,
            |current| current.checked_add(bytes).filter(|total| *total <= ARCHIVE_BYTES));
        if reserved.is_err() {
            self.archive_drop("queue_bytes_exhausted",record.sequence);
            return;
        }
        if let Err(error)=self.archive.try_send(record) {
            self.archive_bytes.fetch_sub(bytes, Ordering::AcqRel);
            self.archive_drop("queue_records_or_worker_closed",error.into_inner().sequence);
        }
    }
    fn queue_database(&self,record:&ArchiveRecord) {
        let Some(sender)=&self.database else {return};
        let bytes=record.raw.len();
        let reserved=bytes<=ARCHIVE_BYTES && self.database_bytes.fetch_update(Ordering::AcqRel,Ordering::Acquire,
            |current|current.checked_add(bytes).filter(|total|*total<=ARCHIVE_BYTES)).is_ok();
        if !reserved {self.database_drop("queue_bytes_exhausted",record.sequence);return;}
        if let Err(error)=sender.try_send(record.clone()) {
            self.database_bytes.fetch_sub(bytes,Ordering::AcqRel);
            self.database_drop(if matches!(error,mpsc::error::TrySendError::Full(_))
                {"queue_records_exhausted"}else{"worker_closed"},record.sequence);
        }
    }
    fn database_drop(&self,reason:&str,sequence:u64) {
        let count=self.database_gaps.fetch_add(1,Ordering::Relaxed)+1;
        if count==1 || count.is_power_of_two(){marketcow_polymarket::diagnostic::emit(json!({
            "stage":"chainlink_observation_db_dropped","reason":reason,
            "source_sequence":sequence,"observed_at":Utc::now(),"dropped_records":count}).to_string());}
    }
    fn archive_drop(&self,reason:&str,sequence:u64) {
        let count=self.archive_gaps.fetch_add(1,Ordering::Relaxed)+1;
        if count==1 || count.is_power_of_two(){marketcow_polymarket::diagnostic::emit(json!({"stage":"chainlink_archive_record_dropped","reason":reason,"source_sequence":sequence,"observed_at":Utc::now(),"dropped_records":count}).to_string());}
    }
}

fn stream_unavailable(state: &State, reason: &str) -> Value {
    json!({"schema_version":"marketcow.btc-price-stream.v2",
        "type":"unavailable","source":"chainlink_twap_60s",
        "instrument_id":"BTCUSD.CHAINLINK.TWAP60",
        "source_epoch":state.epoch,"message_sequence":0,
        "stream_serial":state.stream_serial,
        "source_sequence":Value::Null,"source_sequence_scope":"epoch_and_channel",
        "reason":reason,"observed_at":Utc::now().to_rfc3339()})
}

fn stream_snapshot(state: &State, points: Vec<Arc<ChainlinkPoint>>) -> Value {
    let raw = state.last_raw.as_deref().unwrap_or_default();
    let points_sha256 = hex::encode(Sha256::digest(
        serde_json::to_vec(&points).expect("validated Chainlink points serialize")));
    let intervals = points.windows(2).map(|pair|
        pair[1].source_time_ms - pair[0].source_time_ms)
        .collect::<Vec<_>>();
    json!({"schema_version":"marketcow.btc-price-stream.v2",
        "type":"snapshot","source":"chainlink_twap_60s",
        "instrument_id":"BTCUSD.CHAINLINK.TWAP60",
        "source_epoch":state.epoch,"message_sequence":state.last_seq,
        "stream_serial":state.stream_serial,
        "source_sequence":state.last_seq,"source_sequence_scope":"epoch_and_channel",
        "source_channel":"price.crypto.twap","source_symbol":"btcusd",
        "source_continuity":"contiguous_since_authenticated_snapshot",
        "point_update_semantics":"upsert_by_source_time_ms",
        "snapshot_source_sequence":state.snapshot_sequence,
        "snapshot_received_at":state.snapshot_received_at,
        "snapshot_point_sequence_semantics":"shared_frame_sequence",
        "quote_interval_diagnostics":{"cadence_requirement":"none",
            "intervals_over_1000_ms":intervals.iter().filter(|v| **v > 1000).count(),
            "latest_interval_ms":intervals.last()},
        "latest_source_time_ms":points.last().map(|p| p.source_time_ms),
        "latest_received_at":points.last().map(|p| &p.received_at),
        "source_age_ms_at_emit":points.last()
            .map(|p| p.source_time_ms)
            .map(|at| Utc::now().timestamp_millis() - at),
        "max_source_age_ms":3000,"points":points,
        "points_sha256":points_sha256,
        "raw_evidence_scope":"latest_source_frame_only",
        "response_at":Utc::now().to_rfc3339(),
        "raw_frame_sha256":state.points.back().map(|point|&point.source_raw_sha256),
        "raw_frame_base64":base64::engine::general_purpose::STANDARD.encode(raw)})
}

// The U1 collector's existing Polymarket-only HTTPS proxy is a CONNECT proxy.
// tokio-tungstenite's connect_async opens a direct TCP socket and does not use
// HTTPS_PROXY, so establish the tunnel before its normal TLS/WebSocket handshake.
async fn connect_chainlink() -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let Ok(proxy) = std::env::var("HTTPS_PROXY") else {
        return Ok(connect_async(URL).await?.0);
    };
    let uri: Uri = proxy.parse().context("Chainlink proxy URI invalid")?;
    ensure!(uri.scheme_str() == Some("http") && uri.path() == "/"
        && matches!(uri.host(), Some("127.0.0.1" | "localhost")),
        "Chainlink proxy must be a loopback HTTP CONNECT proxy");
    let port = uri.port_u16().context("Chainlink proxy port missing")?;
    let mut socket = TcpStream::connect((uri.host().unwrap(), port)).await?;
    socket.write_all(b"CONNECT ws-live-v2.polymarket.com:443 HTTP/1.1\r\n\
        Host: ws-live-v2.polymarket.com:443\r\n\r\n").await?;
    let mut response = Vec::with_capacity(128);
    loop {
        ensure!(response.len() < 4096, "Chainlink proxy response too large");
        response.push(socket.read_u8().await?);
        if response.ends_with(b"\r\n\r\n") { break; }
    }
    ensure!(response.starts_with(b"HTTP/1.1 200 ")
        || response.starts_with(b"HTTP/1.0 200 "),
        "Chainlink proxy CONNECT rejected");
    Ok(client_async_tls(URL, socket).await?.0)
}

fn database_task(root:PathBuf,mut receiver:mpsc::Receiver<ArchiveRecord>,
    queued_bytes:Arc<AtomicUsize>,available:Arc<AtomicBool>) {
    let path=root.join("chainlink-twap/observations.sqlite3");
    let mut db=crate::btc_trade::observation_db::open(&path).ok();
    while let Some(record)=receiver.blocking_recv() {
        if db.is_none(){db=crate::btc_trade::observation_db::open(&path).ok();}
        let result=db.as_mut().context("chainlink_database_open_failed")
            .and_then(|db|crate::btc_trade::observation_db::ingest_chainlink(
                db,&record.raw,&record.epoch,&record.received_at));
        queued_bytes.fetch_sub(record.raw.len(),Ordering::AcqRel);
        match result {
            Ok(_)=>{if !available.swap(true,Ordering::AcqRel){marketcow_polymarket::diagnostic::emit(json!({
                "stage":"chainlink_observation_db_recovered","source_sequence":record.sequence,
                "observed_at":Utc::now()}).to_string());}},
            Err(error)=>{
                if let Some(open)=db.as_ref(){crate::btc_trade::observation_db::mark_error(open,
                    "chainlink_twap60",&format!("{error:#}"));}
                available.store(false,Ordering::Release);
                marketcow_polymarket::diagnostic::emit(json!({"stage":"chainlink_observation_db_failed",
                    "source_sequence":record.sequence,"failed_at":Utc::now(),
                    "reason":format!("{error:#}").chars().take(256).collect::<String>()}).to_string());
                db=None;
                std::thread::sleep(Duration::from_secs(2));
            }
        }
    }
}

fn archive_task(root: PathBuf, mut receiver: mpsc::Receiver<ArchiveRecord>,
    queued_bytes: Arc<AtomicUsize>, available: Arc<AtomicBool>) {
    let path = root.join("chainlink-twap");
    let mut writer=None;
    let mut last_success=Value::Null;
    while let Some(record)=receiver.blocking_recv() {
        let result=(||->Result<()> {
            if writer.is_none(){writer=Some(crate::btc_trade::archive::Writer::open(path.clone(),"facts",false,crate::btc_trade::archive::SEGMENT_BYTES)?);}
            let line = json!({"schema_version":"marketcow.chainlink-twap-raw.v1",
                "epoch":record.epoch,"source_sequence":record.sequence,
                "received_at":record.received_at,
                "raw_sha256":hex::encode(Sha256::digest(&record.raw)),
                "raw_base64":base64::engine::general_purpose::STANDARD.encode(&record.raw)});
            let encoded = serde_json::to_vec(&line)?;
            let raw:Value=serde_json::from_slice(&record.raw)?;
            let t=raw["payload"]["timestamp"].as_i64().or_else(||raw["payload"]["data"].as_array()?.last()?["timestamp"].as_i64());
            writer.as_mut().unwrap().append(&encoded,Some(record.sequence),t)?;
            last_success=json!({"source_epoch":record.epoch,"source_sequence":record.sequence,"source_time_ms":t,"archived_at":Utc::now()});
            if !available.swap(true,Ordering::AcqRel){marketcow_polymarket::diagnostic::emit(json!({"stage":"chainlink_archive_recovered","last_success":last_success}).to_string());}
            Ok(())
        })();
        queued_bytes.fetch_sub(record.raw.len(),Ordering::AcqRel);
        if let Err(error)=result {
            available.store(false,Ordering::Release);writer=None;
            marketcow_polymarket::diagnostic::emit(json!({"stage":"chainlink_archive_failed","failure":crate::btc_trade::archive::failure(&error),"last_success":last_success}).to_string());
            std::thread::sleep(Duration::from_secs(2));
        }
    }
    if let Some(writer)=writer {if let Err(error)=writer.finish(){available.store(false,Ordering::Release);marketcow_polymarket::diagnostic::emit(json!({"stage":"chainlink_archive_finish_failed","failure":crate::btc_trade::archive::failure(&error)}).to_string());}}
}

fn push_point(points: &mut im::Vector<Arc<ChainlinkPoint>>, raw: &Value, seq: u64,
    received_at: &str, raw_sha: &str) -> Result<()> {
    let time = raw["timestamp"].as_i64().context("Chainlink point timestamp")?;
    let price = raw["full_accuracy_value"].as_str()
        .context("Chainlink full accuracy price missing")?;
    ensure!(price.parse::<rust_decimal::Decimal>()? > rust_decimal::Decimal::ZERO,
        "Chainlink price invalid");
    if let Some(last) = points.back() {
        let previous = last.source_time_ms;
        ensure!(time >= previous, "Chainlink timestamp regressed");
        if time == previous {
            points.pop_back();
        }
    }
    points.push_back(Arc::new(ChainlinkPoint {source_time_ms:time,price:price.to_owned(),
        received_at:received_at.to_owned(),source_sequence:seq,source_raw_sha256:raw_sha.to_owned()}));
    while points.len() > MAX_POINTS { points.pop_front(); }
    Ok(())
}

async fn read_credentials(path: &PathBuf) -> Result<Credentials> {
    ensure!(path.is_absolute(), "credentials path must be absolute");
    let metadata = tokio::fs::symlink_metadata(path).await?;
    ensure!(metadata.is_file() && metadata.permissions().mode() & 0o077 == 0,
        "credentials file must be private");
    ensure!(metadata.uid() == unsafe { libc::geteuid() },
        "credentials file owner mismatch");
    ensure!(metadata.len() <= 4_096, "credentials file too large");
    let value: Credentials = serde_json::from_slice(&tokio::fs::read(path).await?)?;
    ensure!(!value.api_key.is_empty() && !value.secret.is_empty()
        && !value.passphrase.is_empty(), "credentials fields missing");
    Ok(value)
}

async fn next_json<S>(stream: &mut S) -> Result<Value>
where S: futures_util::Stream<Item = std::result::Result<Message,
    tokio_tungstenite::tungstenite::Error>> + Unpin {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await?.context("PolyBolt closed")??;
        match message {
            Message::Text(raw) => return Ok(serde_json::from_slice(raw.as_bytes())?),
            Message::Binary(raw) => return Ok(serde_json::from_slice(&raw)?),
            Message::Close(_) => anyhow::bail!("PolyBolt closed"),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_service(epoch:&str)->Service {
        let (live,_)=broadcast::channel(16);
        let (archive,mut archived)=mpsc::channel(16);
        tokio::spawn(async move {while archived.recv().await.is_some() {}});
        Service {state:Arc::new(Published::new(State {
            phase:"connecting",epoch:epoch.into(),stream_serial:0,snapshot_sequence:None,
            snapshot_received_at:None,last_seq:None,last_frame_received_at:None,last_raw:None,
            points:im::Vector::new()})),live,archive,
            archive_bytes:Arc::new(AtomicUsize::new(0)),archive_available:Arc::new(AtomicBool::new(true)),
            archive_gaps:Arc::new(AtomicU64::new(0)),database:None,database_bytes:Arc::new(AtomicUsize::new(0)),
            database_available:Arc::new(AtomicBool::new(true)),database_gaps:Arc::new(AtomicU64::new(0))}
    }

    #[tokio::test]
    async fn authenticated_twap_snapshot_then_ordered_update() {
        let service = test_service("fixed-epoch");
        let mut owner=(*service.state.load()).clone();
        let now = Utc::now().timestamp_millis();
        let snapshot = json!({"v":1,"channel":"price.crypto.twap","seq":1,
            "snapshot":true,"payload":{"symbol":"btcusd","source":"chainlink",
                "window_seconds":60,"data":[{"timestamp":now-1000,
                    "full_accuracy_value":"83512.123456789012345678"}]}});
        service.apply_frame(&mut owner, snapshot.to_string().as_bytes()).await.unwrap();
        let update = json!({"v":1,"channel":"price.crypto.twap","seq":2,
            "payload":{"symbol":"btcusd","source":"chainlink",
                "window_seconds":60,"timestamp":now,
                "full_accuracy_value":"83512.223456789012345678"}});
        service.apply_frame(&mut owner, update.to_string().as_bytes()).await.unwrap();
        let window = service.window().await.unwrap();
        assert_eq!(window["schema_version"], SCHEMA);
        assert_eq!(window["source"], "chainlink_twap_60s");
        assert_eq!(window["points"].as_array().unwrap().len(), 2);
        assert_eq!(window["points"][1]["price"], "83512.223456789012345678");
        assert_eq!(window["source_continuity"]["snapshot_sequence"], 1);
        assert_eq!(window["source_continuity"]["last_sequence"], 2);
        assert_eq!(window["quote_interval_diagnostics"]["intervals_over_1000_ms"], 0);
        assert_eq!(window["archive_diagnostics"]["dropped_records"], 0);
        assert!(service.apply_frame(&mut owner, update.to_string().as_bytes()).await.is_err());
        assert!(service.window().await.is_err());
        let skipped = json!({"v":1,"channel":"price.crypto.twap","seq":4,
            "payload":update["payload"]});
        assert!(service.apply_frame(&mut owner, skipped.to_string().as_bytes()).await.is_err());
        service.invalidate(&mut owner, "disconnected").await;
        assert!(service.window().await.is_err());
    }

    #[tokio::test]
    async fn snapshot_points_share_frame_sequence_and_sparse_quotes_are_not_transport_gaps() {
        let service = test_service("first-epoch");
        let mut owner=(*service.state.load()).clone();
        let now = Utc::now().timestamp_millis();
        let snapshot = json!({"v":1,"channel":"price.crypto.twap","seq":71,
            "snapshot":true,"dropped":false,
            "payload":{"symbol":"btcusd","source":"chainlink",
                "window_seconds":60,"data":[
                    {"timestamp":now-2_000,"full_accuracy_value":"83512.1"},
                    {"timestamp":now,"full_accuracy_value":"83512.2"}]}});
        service.apply_frame(&mut owner, snapshot.to_string().as_bytes()).await.unwrap();
        let window = service.window().await.unwrap();
        assert_eq!(window["points"][0]["source_sequence"], 71);
        assert_eq!(window["points"][1]["source_sequence"], 71);
        assert_eq!(window["source_continuity"]["status"], "contiguous_since_snapshot");
        assert_eq!(window["source_continuity"]["snapshot_sequence"], 71);
        assert_eq!(window["source_continuity"]["last_sequence"], 71);
        assert_eq!(window["quote_interval_diagnostics"]["intervals_over_1000_ms"], 1);
        assert_eq!(window["quote_interval_diagnostics"]["latest_interval_ms"], 2_000);
        assert_eq!(window["freshness"]["status"], "fresh");
        assert_eq!(window["archive_diagnostics"]["dropped_records"], 0);

        let second_snapshot = json!({"v":1,"channel":"price.crypto.twap","seq":72,
            "snapshot":true,"payload":snapshot["payload"]});
        assert!(service.apply_frame(&mut owner, second_snapshot.to_string().as_bytes()).await.is_err());
        assert!(service.window().await.is_err());
        let new_snapshot = json!({"v":1,"channel":"price.crypto.twap","seq":1,
            "snapshot":true,"payload":snapshot["payload"]});
        service.apply_frame(&mut owner, new_snapshot.to_string().as_bytes()).await.unwrap();
        let recovered = service.window().await.unwrap();
        assert_ne!(recovered["epoch"], window["epoch"]);
        assert_eq!(recovered["source_continuity"]["snapshot_sequence"], 1);
        assert_eq!(recovered["source_continuity"]["last_sequence"], 1);
        let dropped = json!({"v":1,"channel":"price.crypto.twap","seq":2,
            "dropped":true,"payload":{"symbol":"btcusd","source":"chainlink",
                "window_seconds":60,"timestamp":now,
                "full_accuracy_value":"83512.3"}});
        assert!(service.apply_frame(&mut owner, dropped.to_string().as_bytes()).await.is_err());
        assert!(service.window().await.is_err());
    }
}
