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
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use marketcow_contracts::typed_v3::{self as typed_contract,btc as typed};
use std::{collections::VecDeque, os::unix::fs::{MetadataExt, PermissionsExt}, path::PathBuf,
    sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}}, time::Duration};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::{RwLock, broadcast, mpsc}};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, client_async_tls, connect_async,
    tungstenite::{Message, http::Uri}};

const URL: &str = "wss://ws-live-v2.polymarket.com/ws";
const MAX_FRAME_BYTES: usize = 64 * 1024;
const MAX_POINTS: usize = 600;
const ARCHIVE_RECORDS: usize = 256;
const ARCHIVE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct Service {
    state: Arc<RwLock<State>>,
    typed_live:broadcast::Sender<Arc<crate::btc_typed::Published>>,
    producer_instance_id:String,
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
    raw: Vec<u8>,
    observations:Vec<typed::Observation>,
}
impl ArchiveRecord {
    fn owned_bytes(&self)->usize {
        std::mem::size_of::<Self>()+self.raw.capacity()+self.epoch.capacity()+self.received_at.capacity()
            +self.observations.capacity()*std::mem::size_of::<typed::Observation>()
            +self.observations.iter().map(|r|r.price.capacity()).sum::<usize>()
    }
}

struct State {
    phase: &'static str,
    epoch: String,
    typed_generation:String,
    stream_serial: u64,
    snapshot_sequence: Option<u64>,
    snapshot_received_at: Option<String>,
    last_seq: Option<u64>,
    last_frame_received_at: Option<String>,
    last_raw: Option<Vec<u8>>,
    points: VecDeque<typed::Observation>,
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
        let (typed_live, _) = broadcast::channel(1024);
        let archive_bytes = Arc::new(AtomicUsize::new(0));
        let database_bytes = Arc::new(AtomicUsize::new(0));
        let archive_available = Arc::new(AtomicBool::new(true));
        let database_available=Arc::new(AtomicBool::new(true));
        let service = Self { state: Arc::new(RwLock::new(State {
            phase: "credentials_not_configured",
            epoch: uuid::Uuid::new_v4().simple().to_string(),
            typed_generation:uuid::Uuid::new_v4().to_string(),
            stream_serial: 0,
            snapshot_sequence: None,
            snapshot_received_at: None,
            last_seq: None,
            last_frame_received_at: None,
            last_raw: None,
            points: VecDeque::new(),
        })), typed_live,producer_instance_id:uuid::Uuid::new_v4().to_string(), archive, archive_bytes: archive_bytes.clone(),
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

    pub async fn typed_snapshot_and_subscribe(&self)->Result<(Arc<crate::btc_typed::Published>,broadcast::Receiver<Arc<crate::btc_typed::Published>>)> {
        let state=self.state.read().await;
        let receiver=self.typed_live.subscribe();
        Ok((crate::btc_typed::Published::new(self.typed_frame(&state,None)?)?,receiver))
    }
    fn typed_frame(&self,state:&State,payload:Option<typed::price_frame::Payload>)->Result<typed::PriceFrame> {
        let now=Utc::now().timestamp_nanos_opt().context("clock_unavailable")?;
        let payload=if let Some(p)=payload {p}else if state.phase=="live" {
            let observations=state.points.iter().skip(state.points.len().saturating_sub(256)).cloned().collect::<Vec<_>>();
            typed::price_frame::Payload::Snapshot(typed::Snapshot{trades:Vec::new(),retained_count:state.points.len() as u64,truncated:state.points.len()>256,observations,first_trade_id:None,last_trade_id:None})
        }else{typed::price_frame::Payload::Status(typed::Status{phase:typed::Phase::Unavailable as i32,reason:Some(state.phase.into())})};
        Ok(typed::PriceFrame{schema_version:typed_contract::PRICE_SCHEMA.into(),producer_instance_id:self.producer_instance_id.clone(),stream_id:state.typed_generation.clone(),
            stream_sequence:state.stream_serial,source:typed_contract::common::Source::ChainlinkBtcUsdTwap60 as i32,instrument_id:"BTCUSD.CHAINLINK.TWAP60".into(),source_sequence:state.last_seq,
            published_at_ns:now,latest_source_age_ms_at_emit:state.points.back().map(|p|now.div_euclid(1_000_000)-p.source_time_ms),payload:Some(payload)})
    }
    fn publish_typed(&self,state:&State,payload:Option<typed::price_frame::Payload>)->Result<()> {
        if self.typed_live.receiver_count()>0 {let _=self.typed_live.send(crate::btc_typed::Published::new(self.typed_frame(state,payload)?)?);}
        Ok(())
    }

    async fn run(&self, path: PathBuf) {
        let credentials = match read_credentials(&path).await {
            Ok(value) => value,
            Err(_) => {
                self.invalidate("credentials_invalid").await;
                return;
            }
        };
        let mut backoff = 1u64;
        loop {
            self.invalidate("connecting").await;
            match self.connect_once(&credentials).await {
                Ok(()) => backoff = 1,
                Err(_) => backoff = (backoff * 2).min(30),
            }
            self.invalidate("disconnected").await;
            tokio::time::sleep(Duration::from_secs(backoff)).await;
        }
    }

    async fn invalidate(&self, phase: &'static str) {
        let mut state = self.state.write().await;
        state.phase = phase;
        state.stream_serial = state.stream_serial.saturating_add(1);
        state.epoch = uuid::Uuid::new_v4().simple().to_string();
        state.snapshot_sequence = None;
        state.snapshot_received_at = None;
        state.last_seq = None;
        state.last_frame_received_at = None;
        state.last_raw = None;
        state.points.clear();
        if let Err(error)=self.publish_typed(&state,None){tracing::error!(%error,"chainlink_typed_status_failed");}
    }

    async fn connect_once(&self, credentials: &Credentials) -> Result<()> {
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
                Message::Text(text) => self.apply_frame(text.as_bytes()).await?,
                Message::Binary(bytes) => self.apply_frame(&bytes).await?,
                Message::Close(_) => break,
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
        Ok(())
    }

    async fn apply_frame(&self, raw: &[u8]) -> Result<()> {
        let result = self.apply_frame_inner(raw).await;
        if let Err(error) = &result {
            // A bad sequence, dropped indication or malformed producer frame
            // immediately revokes the old epoch. A new authenticated snapshot
            // is required before any price window is served again.
            let reason = match error.to_string().as_str() {
                "Chainlink frames dropped" => "source_dropped",
                "Chainlink sequence gap" => "source_sequence_gap",
                _ => "source_bad_frame",
            };
            self.invalidate(reason).await;
        }
        result
    }

    async fn apply_frame_inner(&self, raw: &[u8]) -> Result<()> {
        ensure!(raw.len() <= MAX_FRAME_BYTES, "Chainlink frame too large");
        let received_at = Utc::now().to_rfc3339();
        let received_ns=chrono::DateTime::parse_from_rfc3339(&received_at)?.timestamp_nanos_opt().context("clock_unavailable")?;
        let parsed=crate::btc_typed_ingress::chainlink(raw,received_ns,typed_contract::common::Source::ChainlinkBtcUsdTwap60)?;
        let seq=parsed.rows.first().and_then(|r|r.source_sequence).context("Chainlink sequence missing")?;
        let snapshot=parsed.snapshot;
        let observations=parsed.rows;
        let mut state=self.state.write().await;
        let previous_source_time_ms=state.points.back().map(|r|r.source_time_ms);
        if let Some(previous)=state.last_seq {
            ensure!(Some(seq)==previous.checked_add(1),"Chainlink sequence gap");
            ensure!(!snapshot,"Chainlink unexpected snapshot within source epoch");
        }else{ensure!(snapshot,"Chainlink connection has no initial snapshot");}
        if !snapshot {ensure!(previous_source_time_ms.is_none_or(|t|observations[0].source_time_ms>=t),"Chainlink timestamp regressed");}
        if snapshot {
            state.points.clear();state.typed_generation=state.epoch.clone();
            state.snapshot_sequence=Some(seq);state.snapshot_received_at=Some(received_at.clone());
        }
        for row in &observations {
            state.points.push_back(row.clone());
            if state.points.len()>MAX_POINTS{state.points.pop_front();}
        }
        state.last_seq=Some(seq);state.last_frame_received_at=Some(received_at.clone());
        state.last_raw=Some(raw.to_vec());state.phase="live";
        state.stream_serial=state.stream_serial.checked_add(1).context("Chainlink stream sequence exhausted")?;
        self.publish_typed(&state,if snapshot{None}else{Some(typed::price_frame::Payload::Observations(typed::ObservationDelta{observations:observations.clone()}))})?;
        let epoch = state.epoch.clone();
        drop(state);
        self.archive_frame(ArchiveRecord { epoch, sequence: seq,
            received_at, raw: raw.to_vec(),observations });
        Ok(())
    }

    fn archive_frame(&self, mut record: ArchiveRecord) {
        self.queue_database(&record);
        record.observations=Vec::new();
        let bytes = record.owned_bytes();
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
        let queued=record.clone();
        let bytes=queued.owned_bytes();
        let reserved=bytes<=ARCHIVE_BYTES && self.database_bytes.fetch_update(Ordering::AcqRel,Ordering::Acquire,
            |current|current.checked_add(bytes).filter(|total|*total<=ARCHIVE_BYTES)).is_ok();
        if !reserved {self.database_drop("queue_bytes_exhausted",record.sequence);return;}
        if let Err(error)=sender.try_send(queued) {
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
    let open=||crate::btc_typed_store::open(&path);
    let mut db=open().ok();
    while let Some(record)=receiver.blocking_recv() {
        if db.is_none(){db=open().ok();}
        let result=db.as_mut().context("chainlink_database_open_failed")
            .and_then(|db|->Result<usize>{
                crate::btc_typed_store::insert_observations(db,typed_contract::common::Source::ChainlinkBtcUsdTwap60,&record.epoch,&record.observations,
                    &crate::btc_typed_store::EvidenceInput{raw:&record.raw,acquisition:"authenticated_chainlink_ws",upstream_url:URL,
                        received_at_ns:chrono::DateTime::parse_from_rfc3339(&record.received_at).ok().and_then(|t|t.timestamp_nanos_opt()),
                        source_generation:Some(&record.epoch),content_type:"application/json"})
            });
        queued_bytes.fetch_sub(record.owned_bytes(),Ordering::AcqRel);
        match result {
            Ok(_)=>{if !available.swap(true,Ordering::AcqRel){marketcow_polymarket::diagnostic::emit(json!({
                "stage":"chainlink_observation_db_recovered","source_sequence":record.sequence,
                "observed_at":Utc::now()}).to_string());}},
            Err(error)=>{
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
        queued_bytes.fetch_sub(record.owned_bytes(),Ordering::AcqRel);
        if let Err(error)=result {
            available.store(false,Ordering::Release);writer=None;
            marketcow_polymarket::diagnostic::emit(json!({"stage":"chainlink_archive_failed","failure":crate::btc_trade::archive::failure(&error),"last_success":last_success}).to_string());
            std::thread::sleep(Duration::from_secs(2));
        }
    }
    if let Some(writer)=writer {if let Err(error)=writer.finish(){available.store(false,Ordering::Release);marketcow_polymarket::diagnostic::emit(json!({"stage":"chainlink_archive_finish_failed","failure":crate::btc_trade::archive::failure(&error)}).to_string());}}
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

    fn service() -> Service {
        Service {
            state: Arc::new(RwLock::new(State {
                typed_generation: "generation-a".into(), phase: "connecting",
                epoch: "epoch-a".into(), stream_serial: 0, snapshot_sequence: None,
                snapshot_received_at: None, last_seq: None, last_frame_received_at: None,
                last_raw: None, points: VecDeque::new(),
            })),
            typed_live: broadcast::channel(16).0, producer_instance_id: "test-producer".into(),
            archive: mpsc::channel(16).0, archive_bytes: Arc::new(AtomicUsize::new(0)),
            archive_available: Arc::new(AtomicBool::new(true)), archive_gaps: Arc::new(AtomicU64::new(0)),
            database: None, database_bytes: Arc::new(AtomicUsize::new(0)),
            database_available: Arc::new(AtomicBool::new(true)), database_gaps: Arc::new(AtomicU64::new(0)),
        }
    }

    #[tokio::test]
    async fn native_snapshot_delta_gap_and_recovery_use_same_subscription() {
        let service = service();
        let (_, mut receiver) = service.typed_snapshot_and_subscribe().await.unwrap();
        let raw = br#"{"v":1,"channel":"price.crypto.twap","seq":71,"snapshot":true,"payload":{"symbol":"btcusd","source":"chainlink","window_seconds":60,"data":[{"timestamp":1000,"full_accuracy_value":"100.00"},{"timestamp":3000,"full_accuracy_value":"101.00"}]}}"#;
        service.apply_frame(raw).await.unwrap();
        let first = receiver.recv().await.unwrap();
        let Some(typed::price_frame::Payload::Snapshot(snapshot)) = &first.frame.payload else {panic!("snapshot")};
        assert_eq!(snapshot.observations.len(), 2);
        assert_eq!(snapshot.observations[0].source_sequence, Some(71));
        assert_eq!(snapshot.observations[1].row_index, Some(1));
        let raw = br#"{"v":1,"channel":"price.crypto.twap","seq":72,"payload":{"symbol":"btcusd","source":"chainlink","window_seconds":60,"timestamp":5000,"full_accuracy_value":"102.00"}}"#;
        service.apply_frame(raw).await.unwrap();
        let delta = receiver.recv().await.unwrap();
        assert!(matches!(delta.frame.payload, Some(typed::price_frame::Payload::Observations(_))));
        assert_eq!(delta.frame.stream_sequence, first.frame.stream_sequence + 1);
        assert!(service.apply_frame(raw).await.is_err());
        let status = receiver.recv().await.unwrap();
        assert!(matches!(status.frame.payload, Some(typed::price_frame::Payload::Status(_))));
        let recovery = br#"{"v":1,"channel":"price.crypto.twap","seq":1,"snapshot":true,"payload":{"symbol":"btcusd","source":"chainlink","window_seconds":60,"data":[{"timestamp":6000,"full_accuracy_value":"103.00"}]}}"#;
        service.apply_frame(recovery).await.unwrap();
        let recovered = receiver.recv().await.unwrap();
        assert!(matches!(recovered.frame.payload, Some(typed::price_frame::Payload::Snapshot(_))));
        assert_ne!(recovered.frame.stream_id, first.frame.stream_id);
    }
}
