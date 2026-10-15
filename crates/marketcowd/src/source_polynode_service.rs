//! One PN1 source owner and market-isolated hot observations. The source
//! never submits an order or calls the official CLOB during a candidate.
use crate::{
    source_btc5m_api::{Control, Profile, WindowRole},
    source_polynode_pn1::{Connection, DepthPayload, DepthReceipt, IngestOutcome},
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use marketcow_contracts::btc5m_market_local_v4::{self as contract, wire};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt,
    sync::{Semaphore, mpsc, watch},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as VendorError, Message as VendorMessage},
};

const RAW_FRAME_BYTES: usize = 2 * 1024 * 1024;
const FRAME_BYTES: usize = 4 * 1024 * 1024;
const SNAPSHOT_BYTES: usize = 8 * 1024 * 1024;
const REPLAY_FRAMES: usize = 1024;
const REPLAY_BYTES: usize = 32 * 1024 * 1024;
const CLIENTS: usize = 16;
const CLIENTS_PER_MARKET: usize = 8;
const KEY_BYTES: u64 = 512;
const ARCHIVE_QUEUE_RECORDS: usize = 128;
const ARCHIVE_QUEUE_BYTES: usize = 64 * 1024 * 1024;
const ARCHIVE_FILE_BYTES: usize = 256 * 1024 * 1024;
const ARCHIVE_TOTAL_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const WS_SEND_TIMEOUT: Duration = Duration::from_secs(5);
const WS_PAGE_FRAMES: usize = 32;
const PN1_RATE_LIMIT_RETRY_MIN: Duration = Duration::from_secs(60);
const PN1_RATE_LIMIT_RETRY_MAX: Duration = Duration::from_secs(900);

#[derive(Debug)]
struct Pn1HttpRejection {
    status: u16,
    retry_after: Option<Duration>,
}

impl std::fmt::Display for Pn1HttpRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "PN1 handshake HTTP {}", self.status)
    }
}

impl std::error::Error for Pn1HttpRejection {}

fn pn1_retry_after(value: &str) -> Option<Duration> {
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    chrono::DateTime::parse_from_rfc2822(value)
        .ok()
        .and_then(|deadline| {
            (deadline.with_timezone(&chrono::Utc) - chrono::Utc::now())
                .to_std()
                .ok()
        })
}

fn pn1_connect_error(error: VendorError) -> anyhow::Error {
    match error {
        VendorError::Http(response) => Pn1HttpRejection {
            status: response.status().as_u16(),
            retry_after: response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .and_then(pn1_retry_after),
        }
        .into(),
        // Other tungstenite errors may include the authenticated URL.
        _ => anyhow::anyhow!("PN1 connect failed"),
    }
}

fn now_ns() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?)
}

fn key_from_file(path: &PathBuf) -> Result<String> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.permissions().mode() & 0o077 == 0,
        "PN1 key must be a private regular file"
    );
    ensure!(
        (1..=KEY_BYTES).contains(&metadata.len()),
        "PN1 key byte budget"
    );
    let key = fs::read_to_string(path)?;
    let key = key.trim_end_matches(|character| character == '\r' || character == '\n');
    ensure!(
        (key.starts_with("pn_live_") || key.starts_with("qm_live_"))
            && key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "PN1 key syntax"
    );
    Ok(key.to_owned())
}

#[derive(Clone)]
struct MarketLane {
    state: Arc<Mutex<LaneState>>,
    changed: watch::Sender<u64>,
    clients: Arc<Semaphore>,
}

struct LaneState {
    identity: wire::MarketIdentity,
    outcomes: Vec<wire::OutcomeBinding>,
    source_epoch: String,
    serial: u64,
    floor: u64,
    books: BTreeMap<String, TokenMirror>,
    replay: VecDeque<(u64, Arc<Vec<u8>>)>,
    replay_bytes: usize,
    unavailable_reason: String,
    closed: bool,
}

#[derive(Clone)]
struct TokenMirror {
    bids: BTreeMap<String, String>,
    asks: BTreeMap<String, String>,
    sequence: u64,
    checksum: Vec<u8>,
    raw_frame_sha256: Vec<u8>,
    raw_frame: Arc<Vec<u8>>,
    provider_batch_ts_ms: Option<u64>,
    socket_received_at_unix_ns: u64,
}

impl TokenMirror {
    fn levels(
        levels: &BTreeMap<String, String>,
        descending: bool,
    ) -> Result<Vec<wire::DepthLevel>> {
        use std::str::FromStr;
        let mut values = levels
            .iter()
            .map(|(price, size)| {
                Ok((
                    rust_decimal::Decimal::from_str(price)?,
                    wire::DepthLevel {
                        price: price.clone(),
                        size: size.clone(),
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        values.sort_by(|left, right| {
            if descending {
                right.0.cmp(&left.0)
            } else {
                left.0.cmp(&right.0)
            }
        });
        Ok(values.into_iter().map(|(_, value)| value).collect())
    }

    fn wire(&self, token_id: &str) -> Result<wire::TokenBookState> {
        Ok(wire::TokenBookState {
            token_id: token_id.into(),
            provider_seq: self.sequence,
            pn1_checksum: self.checksum.clone(),
            bids: Self::levels(&self.bids, true)?,
            asks: Self::levels(&self.asks, false)?,
            raw_frame_sha256: self.raw_frame_sha256.clone(),
            raw_frame: self.raw_frame.as_ref().clone(),
            provider_batch_ts_ms: self.provider_batch_ts_ms,
            socket_received_at_unix_ns: self.socket_received_at_unix_ns,
            quote_source_at_ms: None,
        })
    }

    fn apply(
        &mut self,
        receipt: &DepthReceipt,
        raw_sha: &[u8],
        raw: &Arc<Vec<u8>>,
        received_at_ns: u64,
    ) -> Result<()> {
        let DepthPayload::Delta { prev_seq, changes } = &receipt.payload else {
            anyhow::bail!("PN1 mirror delta expected");
        };
        ensure!(
            self.sequence == *prev_seq && prev_seq.checked_add(1) == Some(receipt.provider_seq),
            "PN1 mirror sequence discontinuity"
        );
        for change in changes {
            let levels = match change.side {
                "B" => &mut self.bids,
                "A" => &mut self.asks,
                _ => anyhow::bail!("PN1 mirror side"),
            };
            if change.size == "0" {
                levels.remove(&change.price);
            } else {
                levels.insert(change.price.clone(), change.size.clone());
            }
        }
        self.sequence = receipt.provider_seq;
        self.checksum = hex::decode(&receipt.provider_checksum)?;
        self.raw_frame_sha256 = raw_sha.to_vec();
        self.raw_frame = raw.clone();
        self.provider_batch_ts_ms = receipt.provider_batch_time_ms;
        self.socket_received_at_unix_ns = received_at_ns;
        Ok(())
    }
}

impl MarketLane {
    fn from_binding(
        profile: &Profile,
        profile_epoch: &str,
        market: &crate::source_btc5m_api::MarketBinding,
    ) -> Self {
        let identity = wire::MarketIdentity {
            market_id: market.market_id.clone(),
            condition_id: market.condition_id.clone(),
            window_id: market.window_id.clone(),
            profile_id: profile.profile_id.clone(),
            profile_revision: profile.profile_revision,
            profile_epoch: profile_epoch.to_owned(),
            stream_epoch: uuid::Uuid::new_v4().simple().to_string(),
            role: match &market.role {
                WindowRole::Current => "current",
                WindowRole::Next => "next",
                WindowRole::Retiring => "retiring",
            }
            .into(),
            start_at: market.start_at.clone(),
            end_at: market.end_at.clone(),
            rule_sha256: market.rule_sha256.clone(),
        };
        let mut outcomes = market
            .outcomes
            .iter()
            .map(|outcome| wire::OutcomeBinding {
                outcome: outcome.outcome.clone(),
                token_id: outcome.token_id.clone(),
            })
            .collect::<Vec<_>>();
        outcomes.sort_by_key(|outcome| if outcome.outcome == "Up" { 0 } else { 1 });
        let state = LaneState {
            identity,
            outcomes,
            source_epoch: String::new(),
            serial: 0,
            floor: 0,
            books: BTreeMap::new(),
            replay: VecDeque::new(),
            replay_bytes: 0,
            unavailable_reason: "source_connecting".into(),
            closed: false,
        };
        let (changed, _) = watch::channel(0);
        Self {
            state: Arc::new(Mutex::new(state)),
            changed,
            clients: Arc::new(Semaphore::new(CLIENTS_PER_MARKET)),
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, LaneState>> {
        self.state
            .lock()
            .map_err(|_| anyhow::anyhow!("PN1 market lane poisoned"))
    }

    fn reset_source(&self, source_epoch: &str, reason: &str) -> Result<()> {
        let mut state = self.lock()?;
        state.source_epoch = source_epoch.to_owned();
        state.serial = 0;
        state.floor = 0;
        state.books.clear();
        state.replay.clear();
        state.replay_bytes = 0;
        state.unavailable_reason = reason.to_owned();
        let next = (*self.changed.borrow()).wrapping_add(1);
        self.changed.send_replace(next);
        Ok(())
    }

    fn close(&self) -> Result<()> {
        let mut state = self.lock()?;
        state.closed = true;
        state.unavailable_reason = "profile_changed".into();
        let next = (*self.changed.borrow()).wrapping_add(1);
        self.changed.send_replace(next);
        Ok(())
    }

    fn snapshot(&self) -> Result<wire::MarketSnapshot> {
        let (identity, outcomes, source_epoch, serial, floor, mirrors, unavailable_reason, closed) = {
            let state = self.lock()?;
            (
                state.identity.clone(),
                state.outcomes.clone(),
                state.source_epoch.clone(),
                state.serial,
                state.floor,
                state.books.clone(),
                state.unavailable_reason.clone(),
                state.closed,
            )
        };
        let ready = !closed
            && unavailable_reason.is_empty()
            && outcomes
                .iter()
                .all(|outcome| mirrors.contains_key(&outcome.token_id));
        let books = outcomes
            .iter()
            .filter_map(|outcome| {
                mirrors
                    .get(&outcome.token_id)
                    .map(|book| book.wire(&outcome.token_id))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(wire::MarketSnapshot {
            identity: Some(identity),
            outcomes,
            source_epoch,
            market_serial: serial,
            replay_floor_serial: Some(floor),
            books,
            ready,
            unavailable_reason: if ready {
                String::new()
            } else {
                unavailable_reason
            },
            snapshot_at_unix_ns: now_ns()?,
            execution_readiness: false,
        })
    }
}

impl LaneState {
    fn append_many(&mut self, bodies: Vec<wire::market_stream_frame::Body>) -> Result<()> {
        let mut prepared = Vec::with_capacity(bodies.len());
        let mut next_bytes = self.replay_bytes;
        for (index, body) in bodies.into_iter().enumerate() {
            let serial = self
                .serial
                .checked_add(u64::try_from(index + 1)?)
                .context("PN1 market serial overflow")?;
            let frame = wire::MarketStreamFrame {
                identity: Some(self.identity.clone()),
                source_epoch: self.source_epoch.clone(),
                market_serial: serial,
                body: Some(body),
            };
            let bytes = Arc::new(contract::encode_frame(&frame, FRAME_BYTES)?);
            next_bytes = next_bytes
                .checked_add(bytes.len())
                .context("PN1 replay byte overflow")?;
            prepared.push((serial, bytes));
        }
        if let Some((serial, _)) = prepared.last() {
            self.serial = *serial;
        }
        self.replay_bytes = next_bytes;
        self.replay.extend(prepared);
        while self.replay.len() > REPLAY_FRAMES || self.replay_bytes > REPLAY_BYTES {
            let (serial, old) = self.replay.pop_front().context("PN1 replay eviction")?;
            self.replay_bytes -= old.len();
            self.floor = serial;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Service {
    control: Control,
    lanes: Arc<RwLock<BTreeMap<String, MarketLane>>>,
    slots: Arc<Semaphore>,
    official_book_client: reqwest::Client,
    official_book_slots: Arc<Semaphore>,
    archive_tx: mpsc::Sender<RawRecord>,
    archive_queued_bytes: Arc<AtomicUsize>,
    archive_gaps: Arc<AtomicU64>,
    archive_available: Arc<AtomicBool>,
}

pub(crate) struct RawRecord {
    source_epoch: String,
    received_at_unix_ns: u64,
    raw_frame: String,
    raw_frame_sha256: String,
    charged_bytes: usize,
}

impl Service {
    pub fn new(control: Control) -> Result<(Self, mpsc::Receiver<RawRecord>)> {
        let official_book_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(2))
            .build()?;
        let (archive_tx, archive_rx) = mpsc::channel(ARCHIVE_QUEUE_RECORDS);
        Ok((
            Self {
                control,
                lanes: Arc::new(RwLock::new(BTreeMap::new())),
                slots: Arc::new(Semaphore::new(CLIENTS)),
                official_book_client,
                official_book_slots: Arc::new(Semaphore::new(32)),
                archive_tx,
                archive_queued_bytes: Arc::new(AtomicUsize::new(0)),
                archive_gaps: Arc::new(AtomicU64::new(0)),
                archive_available: Arc::new(AtomicBool::new(true)),
            },
            archive_rx,
        ))
    }

    fn lane(&self, market_id: &str) -> Result<Option<MarketLane>> {
        Ok(self
            .lanes
            .read()
            .map_err(|_| anyhow::anyhow!("PN1 registry poisoned"))?
            .get(market_id)
            .cloned())
    }

    fn active_identity(&self, identity: &wire::MarketIdentity) -> bool {
        self.control
            .profile_for_market_source()
            .is_ok_and(|(profile, epoch)| {
                epoch == identity.profile_epoch
                    && profile.profile_id == identity.profile_id
                    && profile.profile_revision == identity.profile_revision
                    && profile.markets.iter().any(|market| {
                        market.market_id == identity.market_id
                            && market.condition_id == identity.condition_id
                            && market.role != WindowRole::Retiring
                    })
            })
    }

    fn install_profile(&self, profile: &Profile, epoch: &str) -> Result<BTreeMap<String, String>> {
        let mut next = BTreeMap::new();
        let mut token_to_market = BTreeMap::new();
        let mut lanes = self
            .lanes
            .write()
            .map_err(|_| anyhow::anyhow!("PN1 registry poisoned"))?;
        for market in profile
            .markets
            .iter()
            .filter(|market| market.role != WindowRole::Retiring)
        {
            ensure!(
                market.outcomes.len() == 2
                    && market
                        .outcomes
                        .iter()
                        .any(|outcome| outcome.outcome == "Up")
                    && market
                        .outcomes
                        .iter()
                        .any(|outcome| outcome.outcome == "Down"),
                "PN1 market requires exact Up/Down token binding"
            );
            // A source socket reconnect changes source_epoch, not the market
            // stream identity. Only a published profile/rebuilt market lane
            // receives a new stream_epoch.
            let lane = if let Some(existing) = lanes.get(&market.market_id) {
                let state = existing.lock()?;
                let reuse = state.identity.profile_id == profile.profile_id
                    && state.identity.profile_revision == profile.profile_revision
                    && state.identity.profile_epoch == epoch
                    && state.identity.condition_id == market.condition_id
                    && state.identity.role
                        == match &market.role {
                            WindowRole::Current => "current",
                            WindowRole::Next => "next",
                            WindowRole::Retiring => "retiring",
                        }
                    && state
                        .outcomes
                        .iter()
                        .map(|outcome| (&outcome.outcome, &outcome.token_id))
                        .eq(market
                            .outcomes
                            .iter()
                            .map(|outcome| (&outcome.outcome, &outcome.token_id)));
                if reuse {
                    existing.clone()
                } else {
                    MarketLane::from_binding(profile, epoch, market)
                }
            } else {
                MarketLane::from_binding(profile, epoch, market)
            };
            for outcome in &market.outcomes {
                ensure!(
                    token_to_market
                        .insert(outcome.token_id.clone(), market.market_id.clone())
                        .is_none(),
                    "PN1 token has multiple active markets"
                );
            }
            next.insert(market.market_id.clone(), lane);
        }
        for (market_id, old) in lanes.iter() {
            if next
                .get(market_id)
                .is_none_or(|new| !Arc::ptr_eq(&old.state, &new.state))
            {
                old.close()?;
            }
        }
        *lanes = next;
        Ok(token_to_market)
    }

    fn reset_source(&self, epoch: &str, reason: &str) -> Result<()> {
        let lanes = self
            .lanes
            .read()
            .map_err(|_| anyhow::anyhow!("PN1 registry poisoned"))?;
        for lane in lanes.values() {
            lane.reset_source(epoch, reason)?;
        }
        Ok(())
    }

    fn publish(
        &self,
        source_epoch: &str,
        raw: &str,
        received_at_ns: u64,
        outcome: IngestOutcome,
        token_to_market: &BTreeMap<String, String>,
    ) -> Result<()> {
        let raw_sha = Sha256::digest(raw.as_bytes()).to_vec();
        let raw_bytes = Arc::new(raw.as_bytes().to_vec());
        let mut per_market = BTreeMap::<String, Vec<_>>::new();
        for receipt in outcome.verified_depth {
            let market = token_to_market
                .get(&receipt.token_id)
                .context("PN1 verified token not bound to market")?;
            per_market.entry(market.clone()).or_default().push(receipt);
        }
        let mut invalid_by_market = BTreeMap::<String, Vec<String>>::new();
        for token in outcome.invalidated_tokens {
            if let Some(market) = token_to_market.get(&token) {
                invalid_by_market
                    .entry(market.clone())
                    .or_default()
                    .push(token);
            }
        }
        for market_id in invalid_by_market.keys() {
            per_market.entry(market_id.clone()).or_default();
        }
        for (market_id, receipts) in per_market {
            let lane = self.lane(&market_id)?.context("PN1 market lane absent")?;
            let mut state = lane.lock()?;
            ensure!(
                state.source_epoch == source_epoch && !state.closed,
                "PN1 source epoch changed during publish"
            );
            let batch_time = receipts
                .first()
                .and_then(|receipt| receipt.provider_batch_time_ms);
            let mut books = state.books.clone();
            let mut unavailable_reason = state.unavailable_reason.clone();
            let mut updates = Vec::with_capacity(receipts.len());
            for receipt in &receipts {
                let body = match &receipt.payload {
                    DepthPayload::Anchor { bids, asks } => {
                        let mirror = TokenMirror {
                            bids: bids
                                .iter()
                                .map(|level| (level.price.clone(), level.size.clone()))
                                .collect(),
                            asks: asks
                                .iter()
                                .map(|level| (level.price.clone(), level.size.clone()))
                                .collect(),
                            sequence: receipt.provider_seq,
                            checksum: hex::decode(&receipt.provider_checksum)?,
                            raw_frame_sha256: raw_sha.clone(),
                            raw_frame: raw_bytes.clone(),
                            provider_batch_ts_ms: receipt.provider_batch_time_ms,
                            socket_received_at_unix_ns: received_at_ns,
                        };
                        books.insert(receipt.token_id.clone(), mirror);
                        wire::market_depth_item::Body::TokenAnchor(wire::TokenBookState {
                            token_id: receipt.token_id.clone(),
                            provider_seq: receipt.provider_seq,
                            pn1_checksum: hex::decode(&receipt.provider_checksum)?,
                            bids: bids
                                .iter()
                                .map(|level| wire::DepthLevel {
                                    price: level.price.clone(),
                                    size: level.size.clone(),
                                })
                                .collect(),
                            asks: asks
                                .iter()
                                .map(|level| wire::DepthLevel {
                                    price: level.price.clone(),
                                    size: level.size.clone(),
                                })
                                .collect(),
                            raw_frame_sha256: raw_sha.clone(),
                            raw_frame: Vec::new(),
                            provider_batch_ts_ms: receipt.provider_batch_time_ms,
                            socket_received_at_unix_ns: received_at_ns,
                            quote_source_at_ms: None,
                        })
                    }
                    DepthPayload::Delta { prev_seq, changes } => {
                        books
                            .get_mut(&receipt.token_id)
                            .context("PN1 mirror lacks verified anchor")?
                            .apply(receipt, &raw_sha, &raw_bytes, received_at_ns)?;
                        wire::market_depth_item::Body::Delta(wire::MarketDepthDelta {
                            token_id: receipt.token_id.clone(),
                            prev_provider_seq: *prev_seq,
                            provider_seq: receipt.provider_seq,
                            pn1_checksum: hex::decode(&receipt.provider_checksum)?,
                            changes: changes
                                .iter()
                                .map(|level| wire::DepthChange {
                                    side: level.side.into(),
                                    price: level.price.clone(),
                                    size: level.size.clone(),
                                })
                                .collect(),
                        })
                    }
                };
                updates.push(wire::MarketDepthItem { body: Some(body) });
            }
            let mut bodies = Vec::new();
            if !updates.is_empty() {
                bodies.push(wire::market_stream_frame::Body::DepthBatch(
                    wire::MarketDepthBatch {
                        updates,
                        raw_frame_sha256: raw_sha.clone(),
                        raw_frame: raw_bytes.as_ref().clone(),
                        provider_batch_ts_ms: batch_time,
                        socket_received_at_unix_ns: received_at_ns,
                        published_at_unix_ns: now_ns()?,
                        quote_source_at_ms: None,
                    },
                ));
            }
            // Commit the complete vendor message under one market lock. A
            // full-sync cannot observe half a batch with a final serial.
            if let Some(tokens) = invalid_by_market.remove(&market_id) {
                for token in &tokens {
                    books.remove(token);
                }
                unavailable_reason = "integrity_failed".into();
                bodies.push(wire::market_stream_frame::Body::Unavailable(
                    wire::MarketInvalidated {
                        token_ids: tokens,
                        reason: unavailable_reason.clone(),
                        requires_new_snapshot: true,
                    },
                ));
            }
            let ready = state
                .outcomes
                .iter()
                .all(|outcome| books.contains_key(&outcome.token_id));
            if ready && !unavailable_reason.is_empty() {
                unavailable_reason.clear();
                let verified_token_ids = state
                    .outcomes
                    .iter()
                    .map(|outcome| outcome.token_id.clone())
                    .collect();
                let market_serial = state
                    .serial
                    .checked_add(u64::try_from(bodies.len() + 1)?)
                    .context("PN1 ready serial overflow")?;
                bodies.push(wire::market_stream_frame::Body::Ready(wire::MarketReady {
                    market_serial,
                    verified_token_ids,
                    execution_readiness: false,
                }));
            }
            // Commit bytes and mirrored depth under the same market lock.
            // Encoding failure cannot leave an unannounced book mutation.
            state.append_many(bodies)?;
            state.books = books;
            state.unavailable_reason = unavailable_reason;
            let next = (*lane.changed.borrow()).wrapping_add(1);
            lane.changed.send_replace(next);
        }
        Ok(())
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route(
                "/v1/prediction-markets/polymarket/research/btc-5m/market-streams",
                get(http_streams),
            )
            .route(
                "/v1/prediction-markets/polymarket/research/btc-5m/market-full-sync",
                get(http_snapshot),
            )
            .route(
                "/v1/prediction-markets/polymarket/research/btc-5m/market-stream",
                get(http_stream),
            )
            .route(
                "/v1/prediction-markets/polymarket/research/btc-5m/official-book",
                get(http_official_book),
            )
            .with_state(self.clone())
    }

    pub fn start(
        &self,
        key_path: PathBuf,
        archive_root: PathBuf,
        archive_rx: mpsc::Receiver<RawRecord>,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let key = key_from_file(&key_path)?;
        let service = self.clone();
        Ok(tokio::spawn(async move {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let source = service.clone();
            let archive = service.clone();
            tokio::join!(
                source.run_forever(key),
                archive.run_archive(archive_root, archive_rx)
            );
        }))
    }

    async fn run_forever(&self, key: String) {
        let mut delay = Duration::from_millis(250);
        let mut rate_limit_delay = PN1_RATE_LIMIT_RETRY_MIN;
        loop {
            let result = self.run_connection(&key).await;
            if let Err(error) = result {
                // WebSocket errors may contain the authenticated URL. Never
                // format one into logs or any downstream diagnostic.
                let disconnected = uuid::Uuid::new_v4().simple().to_string();
                let rejection = error.downcast_ref::<Pn1HttpRejection>();
                let reason = match rejection {
                    Some(rejection) if rejection.status == 429 => "provider_http_429",
                    Some(rejection) if rejection.status == 401 || rejection.status == 403 => {
                        "provider_auth_rejected"
                    }
                    Some(_) => "provider_handshake_rejected",
                    None => "source_disconnected",
                };
                let _ = self.reset_source(&disconnected, reason);
                let retry = match rejection {
                    Some(rejection) if rejection.status == 429 => rejection
                        .retry_after
                        .unwrap_or(rate_limit_delay)
                        .max(rate_limit_delay)
                        .clamp(PN1_RATE_LIMIT_RETRY_MIN, PN1_RATE_LIMIT_RETRY_MAX),
                    _ => {
                        rate_limit_delay = PN1_RATE_LIMIT_RETRY_MIN;
                        delay
                    }
                };
                if rejection.is_some_and(|rejection| rejection.status == 429) {
                    rate_limit_delay = (retry * 2).min(PN1_RATE_LIMIT_RETRY_MAX);
                }
                self.wait_retry_with_profile_updates(retry, &disconnected, reason)
                    .await;
                delay = (delay * 2).min(Duration::from_secs(5));
            } else {
                delay = Duration::from_millis(250);
                rate_limit_delay = PN1_RATE_LIMIT_RETRY_MIN;
            }
        }
    }

    async fn wait_retry_with_profile_updates(
        &self,
        retry: Duration,
        source_epoch: &str,
        reason: &str,
    ) {
        let Ok((mut profile, mut profile_epoch, mut changed)) =
            self.control.profile_with_watch_for_market_source()
        else {
            tokio::time::sleep(retry).await;
            return;
        };
        // The profile may have advanced between the failed handshake and
        // subscribing to its watch. Bring the unavailable lanes to the
        // current identity before waiting for the next notification.
        if self.install_profile(&profile, &profile_epoch).is_ok() {
            let _ = self.reset_source(source_epoch, reason);
        }
        let timer = tokio::time::sleep(retry);
        tokio::pin!(timer);
        loop {
            tokio::select! {
                _ = &mut timer => break,
                update = changed.changed() => {
                    if update.is_err() {
                        break;
                    }
                    let Ok((next, next_epoch)) = self.control.profile_for_market_source() else {
                        continue;
                    };
                    if next_epoch != profile_epoch
                        || next.profile_revision != profile.profile_revision
                    {
                        if self.install_profile(&next, &next_epoch).is_ok() {
                            let _ = self.reset_source(source_epoch, reason);
                            profile = next;
                            profile_epoch = next_epoch;
                        }
                    }
                }
            }
        }
    }

    fn archive_raw(&self, source_epoch: &str, raw: &str, received_at_unix_ns: u64) {
        let bytes = raw.len();
        if !self.archive_available.load(Ordering::Relaxed)
            || bytes > RAW_FRAME_BYTES
            || self
                .archive_queued_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current
                        .checked_add(bytes)
                        .filter(|sum| *sum <= ARCHIVE_QUEUE_BYTES)
                })
                .is_err()
        {
            self.archive_gaps.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let record = RawRecord {
            source_epoch: source_epoch.into(),
            received_at_unix_ns,
            raw_frame: raw.into(),
            raw_frame_sha256: hex::encode(Sha256::digest(raw.as_bytes())),
            charged_bytes: bytes,
        };
        if self.archive_tx.try_send(record).is_err() {
            self.archive_queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
            self.archive_gaps.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn run_archive(&self, root: PathBuf, mut receiver: mpsc::Receiver<RawRecord>) {
        let result = async {
            tokio::fs::create_dir_all(&root).await?;
            let mut file: Option<tokio::fs::File> = None;
            let mut file_bytes = 0usize;
            prune_archive(&root, None).await?;
            while let Some(record) = receiver.recv().await {
                let charged = record.charged_bytes;
                let write_result = async {
                    let line = serde_json::to_vec(&json!({
                        "schema_version":"marketcow.polynode.pn1-raw.v1",
                        "source_epoch":record.source_epoch,
                        "received_at_unix_ns":record.received_at_unix_ns,
                        "raw_frame_sha256":record.raw_frame_sha256,
                        "raw_frame":record.raw_frame,
                    }))?;
                    if file.is_none()
                        || file_bytes.saturating_add(line.len() + 1) > ARCHIVE_FILE_BYTES
                    {
                        let path =
                            root.join(format!("raw-{}.jsonl", uuid::Uuid::new_v4().simple()));
                        file = Some(
                            tokio::fs::OpenOptions::new()
                                .create_new(true)
                                .write(true)
                                .mode(0o600)
                                .open(&path)
                                .await?,
                        );
                        file_bytes = 0;
                        prune_archive(&root, Some(&path)).await?;
                    }
                    let output = file.as_mut().context("PN1 archive file")?;
                    output.write_all(&line).await?;
                    output.write_all(b"\n").await?;
                    file_bytes += line.len() + 1;
                    Ok::<(), anyhow::Error>(())
                }
                .await;
                self.archive_queued_bytes
                    .fetch_sub(charged, Ordering::AcqRel);
                if write_result.is_err() {
                    self.archive_available.store(false, Ordering::Relaxed);
                    self.archive_gaps.fetch_add(1, Ordering::Relaxed);
                    // Keep draining charged reservations so a failed disk
                    // cannot backpressure or stall the live PN1 owner.
                    while let Some(pending) = receiver.recv().await {
                        self.archive_queued_bytes
                            .fetch_sub(pending.charged_bytes, Ordering::AcqRel);
                        self.archive_gaps.fetch_add(1, Ordering::Relaxed);
                    }
                    break;
                }
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if result.is_err() {
            self.archive_available.store(false, Ordering::Relaxed);
            while let Ok(pending) = receiver.try_recv() {
                self.archive_queued_bytes
                    .fetch_sub(pending.charged_bytes, Ordering::AcqRel);
                self.archive_gaps.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    async fn run_connection(&self, key: &str) -> Result<()> {
        let (profile, profile_epoch, mut profile_changed) =
            self.control.profile_with_watch_for_market_source()?;
        let token_to_market = self.install_profile(&profile, &profile_epoch)?;
        ensure!(!token_to_market.is_empty(), "PN1 active token set empty");
        let mut token_to_condition = BTreeMap::new();
        for market in profile
            .markets
            .iter()
            .filter(|market| market.role != WindowRole::Retiring)
        {
            for outcome in &market.outcomes {
                token_to_condition.insert(outcome.token_id.clone(), market.condition_id.clone());
            }
        }
        let source_epoch = uuid::Uuid::new_v4().simple().to_string();
        self.reset_source(&source_epoch, "awaiting_pn1_anchor")?;
        let mut parser = Connection::new(source_epoch.clone(), token_to_condition)?;
        let url = format!("wss://ob.polynode.dev/ws?key={key}&integrity=1");
        let (mut socket, _) = tokio::time::timeout(Duration::from_secs(10), connect_async(url))
            .await
            .map_err(|_| anyhow::anyhow!("PN1 connect timeout"))?
            .map_err(pn1_connect_error)?;
        let subscribe = json!({"action":"subscribe",
            "markets":token_to_market.keys().collect::<Vec<_>>()});
        tokio::time::timeout(
            Duration::from_secs(2),
            socket.send(VendorMessage::Text(subscribe.to_string().into())),
        )
        .await
        .map_err(|_| anyhow::anyhow!("PN1 subscribe send timeout"))?
        .map_err(|_| anyhow::anyhow!("PN1 subscribe send failed"))?;
        let mut ping = tokio::time::interval(Duration::from_secs(10));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_pong = tokio::time::Instant::now();
        let mut resync_sent = BTreeMap::<String, tokio::time::Instant>::new();
        let mut resync_pending = BTreeSet::<String>::new();
        let mut resync_retry_at = None::<tokio::time::Instant>;
        let mut retry_tick = tokio::time::interval(Duration::from_millis(100));
        retry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = profile_changed.changed() => {
                    changed.context("PN1 profile watch closed")?;
                    let (current, current_epoch) = self.control.profile_for_market_source()?;
                    if current_epoch != profile_epoch || current.profile_revision != profile.profile_revision {
                        return Ok(());
                    }
                },
                _ = ping.tick() => {
                    ensure!(last_pong.elapsed() < Duration::from_secs(30), "PN1 pong timeout");
                    tokio::time::timeout(Duration::from_secs(2),
                        socket.send(VendorMessage::Text(r#"{"action":"ping"}"#.into()))).await
                        .map_err(|_| anyhow::anyhow!("PN1 ping send timeout"))?
                        .map_err(|_| anyhow::anyhow!("PN1 ping send failed"))?;
                },
                _ = retry_tick.tick() => {
                    if resync_retry_at.is_some_and(|at| tokio::time::Instant::now() >= at) {
                        resync_retry_at = None;
                        if !resync_pending.is_empty() {
                            tokio::time::timeout(Duration::from_secs(2),
                                socket.send(VendorMessage::Text(json!({"action":"resync",
                                "markets":resync_pending.iter().collect::<Vec<_>>()})
                                .to_string().into()))).await
                                .map_err(|_| anyhow::anyhow!("PN1 resync retry timeout"))?
                                .map_err(|_| anyhow::anyhow!("PN1 resync retry send failed"))?;
                        }
                    }
                },
                message = socket.next() => match message {
                    Some(Ok(VendorMessage::Text(raw))) => {
                        let received_at_ns = now_ns()?;
                        ensure!(raw.len() <= RAW_FRAME_BYTES, "PN1 raw frame byte budget");
                        ensure!(!raw.contains(key), "PN1 credential echoed in source frame");
                        self.archive_raw(&source_epoch, raw.as_str(), received_at_ns);
                        let outcome = parser.ingest(raw.as_str(),
                            &chrono::Utc::now().to_rfc3339())?;
                        if outcome.controls.iter().any(|control| control == "pong") {
                            last_pong = tokio::time::Instant::now();
                        }
                        let invalidated = outcome.invalidated_tokens.clone();
                        for receipt in &outcome.verified_depth {
                            if receipt.is_anchor {
                                resync_pending.remove(&receipt.token_id);
                                resync_sent.remove(&receipt.token_id);
                            }
                        }
                        if let Some(delay_ms) = outcome.resync_retry_after_ms {
                            resync_retry_at = Some(tokio::time::Instant::now()
                                + Duration::from_millis(delay_ms));
                        }
                        let already_resyncing = outcome.controls.iter()
                            .any(|control| control == "resyncing");
                        self.publish(&source_epoch, raw.as_str(), received_at_ns,
                            outcome, &token_to_market)?;
                        let resync = invalidated.into_iter().filter(|token| {
                            resync_pending.insert(token.clone());
                            if already_resyncing { return false; }
                            let now = tokio::time::Instant::now();
                            if resync_sent.get(token).is_some_and(|previous|
                                now.duration_since(*previous) < Duration::from_secs(1)) {
                                resync_retry_at = Some(now + Duration::from_secs(1));
                                return false;
                            }
                            resync_sent.insert(token.clone(), now);
                            true
                        }).collect::<Vec<_>>();
                        if !resync.is_empty() {
                            tokio::time::timeout(Duration::from_secs(2),
                                socket.send(VendorMessage::Text(json!({"action":"resync",
                                "markets":resync}).to_string().into()))).await
                                .map_err(|_| anyhow::anyhow!("PN1 resync send timeout"))?
                                .map_err(|_| anyhow::anyhow!("PN1 resync send failed"))?;
                        }
                    },
                    Some(Ok(VendorMessage::Ping(payload))) => {
                        tokio::time::timeout(Duration::from_secs(2),
                            socket.send(VendorMessage::Pong(payload))).await
                            .map_err(|_| anyhow::anyhow!("PN1 pong send timeout"))?
                            .map_err(|_| anyhow::anyhow!("PN1 pong send failed"))?;
                    },
                    Some(Ok(VendorMessage::Pong(_))) => { last_pong = tokio::time::Instant::now(); },
                    Some(Ok(VendorMessage::Close(_))) | None => {
                        anyhow::bail!("PN1 source closed");
                    },
                    Some(Ok(_)) => anyhow::bail!("PN1 unsupported source frame"),
                    Some(Err(_)) => anyhow::bail!("PN1 source receive failed"),
                }
            }
        }
    }
}

async fn prune_archive(root: &PathBuf, active: Option<&PathBuf>) -> Result<()> {
    let mut entries = Vec::new();
    let mut listing = tokio::fs::read_dir(root).await?;
    let mut total = 0u64;
    while let Some(entry) = listing.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with("raw-")
            || !name.ends_with(".jsonl")
            || !entry.file_type().await?.is_file()
        {
            continue;
        }
        let metadata = entry.metadata().await?;
        total = total.saturating_add(metadata.len());
        entries.push((metadata.modified()?, entry.path(), metadata.len()));
    }
    entries.sort_by_key(|(modified, path, _)| (*modified, path.clone()));
    for (_, path, bytes) in entries {
        if total <= ARCHIVE_TOTAL_BYTES {
            break;
        }
        if active.is_some_and(|current| current == &path) {
            continue;
        }
        tokio::fs::remove_file(&path).await?;
        total = total.saturating_sub(bytes);
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotQuery {
    market_id: String,
    condition_id: String,
    profile_epoch: String,
    profile_revision: u64,
    stream_epoch: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamQuery {
    market_id: String,
    condition_id: String,
    profile_epoch: String,
    profile_revision: u64,
    stream_epoch: String,
    source_epoch: String,
    after_market_serial: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OfficialBookQuery {
    market_id: String,
    condition_id: String,
    token_id: String,
    rule_sha256: String,
    profile_epoch: String,
    profile_revision: u64,
    stream_epoch: String,
    source_epoch: String,
}

fn query_identity_matches(
    identity: &wire::MarketIdentity,
    market_id: &str,
    condition_id: &str,
    profile_epoch: &str,
    profile_revision: u64,
    stream_epoch: &str,
) -> bool {
    identity.market_id == market_id
        && identity.condition_id == condition_id
        && identity.profile_epoch == profile_epoch
        && identity.profile_revision == profile_revision
        && identity.stream_epoch == stream_epoch
}

fn api_error(status: StatusCode, code: &str, retryable: bool) -> Response {
    (status, Json(contract::market_error_json(code, retryable))).into_response()
}

fn protobuf_response<T: prost::Message>(value: &T, maximum_bytes: usize) -> Response {
    let Ok(bytes) = contract::encode_document(value, maximum_bytes) else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "response_byte_budget",
            true,
        );
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(contract::CONTENT_TYPE),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        "x-marketcow-schema",
        HeaderValue::from_static(contract::SCHEMA_VERSION),
    );
    (StatusCode::OK, headers, bytes).into_response()
}

async fn http_streams(State(service): State<Service>) -> Response {
    let Ok((profile, epoch)) = service.control.profile_for_market_source() else {
        return api_error(StatusCode::SERVICE_UNAVAILABLE, "profile_unavailable", true);
    };
    let Ok(lanes) = service.lanes.read() else {
        return api_error(StatusCode::SERVICE_UNAVAILABLE, "market_unavailable", true);
    };
    let mut markets = Vec::new();
    for binding in profile
        .markets
        .iter()
        .filter(|market| market.role != WindowRole::Retiring)
    {
        let Some(lane) = lanes.get(&binding.market_id) else {
            return api_error(StatusCode::SERVICE_UNAVAILABLE, "market_unavailable", true);
        };
        let Ok(state) = lane.lock() else {
            return api_error(StatusCode::SERVICE_UNAVAILABLE, "market_unavailable", true);
        };
        if state.identity.profile_epoch != epoch
            || state.identity.profile_revision != profile.profile_revision
            || state.identity.profile_id != profile.profile_id
        {
            return api_error(StatusCode::CONFLICT, "profile_changed", true);
        }
        let ready = !state.closed
            && state.unavailable_reason.is_empty()
            && state
                .outcomes
                .iter()
                .all(|outcome| state.books.contains_key(&outcome.token_id));
        markets.push(wire::MarketAvailability {
            identity: Some(state.identity.clone()),
            source_epoch: state.source_epoch.clone(),
            market_serial: state.serial,
            replay_floor_serial: state.floor,
            ready,
            unavailable_reason: if ready {
                String::new()
            } else {
                state.unavailable_reason.clone()
            },
            execution_readiness: false,
        });
    }
    drop(lanes);
    let Ok((latest, latest_epoch)) = service.control.profile_for_market_source() else {
        return api_error(StatusCode::SERVICE_UNAVAILABLE, "profile_unavailable", true);
    };
    if latest_epoch != epoch || latest.profile_revision != profile.profile_revision {
        return api_error(StatusCode::CONFLICT, "profile_changed", true);
    }
    let Ok(response_at_unix_ns) = now_ns() else {
        return api_error(StatusCode::SERVICE_UNAVAILABLE, "clock_unavailable", true);
    };
    protobuf_response(
        &wire::MarketStreams {
            profile_id: profile.profile_id.clone(),
            profile_revision: profile.profile_revision,
            profile_epoch: epoch,
            markets,
            response_at_unix_ns,
            raw_archive_gap_count: service.archive_gaps.load(Ordering::Relaxed),
            raw_archive_available: service.archive_available.load(Ordering::Relaxed),
        },
        SNAPSHOT_BYTES,
    )
}

async fn http_snapshot(
    State(service): State<Service>,
    query: std::result::Result<Query<SnapshotQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => return api_error(StatusCode::BAD_REQUEST, "invalid_query", false),
    };
    let Ok(Some(lane)) = service.lane(&query.market_id) else {
        return api_error(StatusCode::SERVICE_UNAVAILABLE, "market_unavailable", true);
    };
    let Ok(snapshot) = lane.snapshot() else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "snapshot_unavailable",
            true,
        );
    };
    let Some(identity) = snapshot.identity.as_ref() else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "snapshot_unavailable",
            true,
        );
    };
    if !query_identity_matches(
        identity,
        &query.market_id,
        &query.condition_id,
        &query.profile_epoch,
        query.profile_revision,
        &query.stream_epoch,
    ) || !service.active_identity(identity)
    {
        return api_error(StatusCode::CONFLICT, "identity_not_active", false);
    }
    protobuf_response(&snapshot, SNAPSHOT_BYTES)
}

async fn http_stream(
    State(service): State<Service>,
    query: std::result::Result<Query<StreamQuery>, axum::extract::rejection::QueryRejection>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => return api_error(StatusCode::BAD_REQUEST, "invalid_query", false),
    };
    let offered = headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|part| part.trim() == contract::WS_SUBPROTOCOL)
        });
    if !offered {
        return api_error(StatusCode::BAD_REQUEST, "subprotocol_required", false);
    }
    let Ok(permit) = service.slots.clone().try_acquire_owned() else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "market_stream_capacity",
            true,
        );
    };
    let Ok(Some(lane)) = service.lane(&query.market_id) else {
        return api_error(StatusCode::SERVICE_UNAVAILABLE, "market_unavailable", true);
    };
    let Ok(market_permit) = lane.clients.clone().try_acquire_owned() else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "market_stream_capacity",
            true,
        );
    };
    let Ok(state) = lane.lock() else {
        return api_error(StatusCode::SERVICE_UNAVAILABLE, "market_unavailable", true);
    };
    if !query_identity_matches(
        &state.identity,
        &query.market_id,
        &query.condition_id,
        &query.profile_epoch,
        query.profile_revision,
        &query.stream_epoch,
    ) || state.source_epoch != query.source_epoch
        || state.closed
        || !service.active_identity(&state.identity)
    {
        return api_error(StatusCode::CONFLICT, "identity_not_active", false);
    }
    if query.after_market_serial < state.floor {
        return api_error(StatusCode::GONE, "replay_expired", true);
    }
    if query.after_market_serial > state.serial {
        return api_error(StatusCode::BAD_REQUEST, "future_market_serial", false);
    }
    drop(state);
    upgrade
        .protocols([contract::WS_SUBPROTOCOL])
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            let _market_permit = market_permit;
            let _ = serve_stream(socket, lane, query).await;
        })
        .into_response()
}

async fn serve_stream(mut socket: WebSocket, lane: MarketLane, query: StreamQuery) -> Result<()> {
    let mut changed = lane.changed.subscribe();
    let mut cursor = query.after_market_serial;
    let mut ping = tokio::time::interval(Duration::from_secs(10));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let (page, close_reason) = {
            let state = lane.lock()?;
            if state.closed
                || state.source_epoch != query.source_epoch
                || !query_identity_matches(
                    &state.identity,
                    &query.market_id,
                    &query.condition_id,
                    &query.profile_epoch,
                    query.profile_revision,
                    &query.stream_epoch,
                )
            {
                (Vec::new(), Some("identity_changed"))
            } else if cursor < state.floor {
                (Vec::new(), Some("replay_expired"))
            } else {
                (
                    state
                        .replay
                        .iter()
                        .filter(|(serial, _)| *serial > cursor)
                        .take(WS_PAGE_FRAMES)
                        .cloned()
                        .collect::<Vec<_>>(),
                    None,
                )
            }
        };
        if let Some(reason) = close_reason {
            let _ = socket
                .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                    code: 1012,
                    reason: reason.into(),
                })))
                .await;
            return Ok(());
        }
        if !page.is_empty() {
            for (serial, bytes) in page {
                tokio::time::timeout(
                    WS_SEND_TIMEOUT,
                    socket.send(Message::Binary(bytes.as_ref().clone().into())),
                )
                .await??;
                cursor = serial;
                // One slow frame cannot postpone inbound control handling
                // until an entire high-throughput replay page is sent.
                tokio::select! {
                    biased;
                    message = socket.recv() => match message {
                        Some(Ok(Message::Close(_))) | None => return Ok(()),
                        Some(Err(_)) => return Ok(()),
                        Some(Ok(Message::Text(_) | Message::Binary(_))) => return Ok(()),
                        _ => {}
                    },
                    _ = ping.tick() => {
                        tokio::time::timeout(WS_SEND_TIMEOUT,
                            socket.send(Message::Ping(Vec::new().into()))).await??;
                    },
                    _ = tokio::task::yield_now() => {},
                }
            }
            continue;
        }
        tokio::select! {
            biased;
            message = socket.recv() => match message {
                Some(Ok(Message::Close(_))) | None => return Ok(()),
                Some(Err(_)) => return Ok(()),
                Some(Ok(Message::Text(_) | Message::Binary(_))) => return Ok(()),
                _ => {}
            },
            _ = ping.tick() => {
                tokio::time::timeout(WS_SEND_TIMEOUT, socket.send(Message::Ping(Vec::new().into()))).await??;
            },
            result = changed.changed() => { if result.is_err() { return Ok(()); } },
        }
    }
}

fn relay_header(headers: &mut HeaderMap, name: &'static str, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

fn relay_error(status: StatusCode, code: &str, retryable: bool, request_id: &str) -> Response {
    (
        status,
        Json(contract::official_relay_error_json(
            code, retryable, request_id,
        )),
    )
        .into_response()
}

async fn http_official_book(
    State(service): State<Service>,
    query: std::result::Result<Query<OfficialBookQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    const SCHEMA: &str = contract::OFFICIAL_RELAY_SCHEMA_VERSION;
    const BODY_BYTES: usize = 512 * 1024;
    let request_id = uuid::Uuid::new_v4().simple().to_string();
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => return relay_error(StatusCode::BAD_REQUEST, "invalid_query", false, &request_id),
    };
    if query.token_id.is_empty() || !query.token_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return relay_error(
            StatusCode::BAD_REQUEST,
            "invalid_token_id",
            false,
            &request_id,
        );
    }
    let Ok(_permit) = service.official_book_slots.clone().try_acquire_owned() else {
        return relay_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "relay_capacity",
            true,
            &request_id,
        );
    };
    let Ok(Some(lane)) = service.lane(&query.market_id) else {
        return relay_error(
            StatusCode::CONFLICT,
            "identity_not_active",
            false,
            &request_id,
        );
    };
    let identity = {
        let Ok(state) = lane.lock() else {
            return relay_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "relay_unavailable",
                true,
                &request_id,
            );
        };
        if !query_identity_matches(
            &state.identity,
            &query.market_id,
            &query.condition_id,
            &query.profile_epoch,
            query.profile_revision,
            &query.stream_epoch,
        ) || state.source_epoch != query.source_epoch
            || state.identity.rule_sha256 != query.rule_sha256
            || state.closed
            || !state
                .outcomes
                .iter()
                .any(|outcome| outcome.token_id == query.token_id)
            || !service.active_identity(&state.identity)
        {
            return relay_error(
                StatusCode::CONFLICT,
                "identity_not_active",
                false,
                &request_id,
            );
        }
        state.identity.clone()
    };
    let Ok(checked_at) = now_ns() else {
        return relay_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "clock_unavailable",
            true,
            &request_id,
        );
    };
    let started_at = checked_at;
    let started = tokio::time::Instant::now();
    let url = format!(
        "https://clob.polymarket.com/book?token_id={}",
        query.token_id
    );
    let response = match service.official_book_client.get(url).send().await {
        Ok(response) => response,
        Err(error) => {
            return relay_error(
                if error.is_timeout() {
                    StatusCode::GATEWAY_TIMEOUT
                } else {
                    StatusCode::BAD_GATEWAY
                },
                if error.is_timeout() {
                    "upstream_timeout"
                } else {
                    "upstream_transport_error"
                },
                true,
                &request_id,
            );
        }
    };
    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let retry_after = response
        .headers()
        .get(header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut response = response;
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if body.len().saturating_add(chunk.len()) <= BODY_BYTES => {
                body.extend_from_slice(&chunk)
            }
            Ok(Some(_)) => {
                return relay_error(
                    StatusCode::BAD_GATEWAY,
                    "upstream_body_too_large",
                    false,
                    &request_id,
                );
            }
            Ok(None) => break,
            Err(error) => {
                return relay_error(
                    if error.is_timeout() {
                        StatusCode::GATEWAY_TIMEOUT
                    } else {
                        StatusCode::BAD_GATEWAY
                    },
                    if error.is_timeout() {
                        "upstream_timeout"
                    } else {
                        "upstream_transport_error"
                    },
                    true,
                    &request_id,
                );
            }
        }
    }
    let Ok(received_at) = now_ns() else {
        return relay_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "clock_unavailable",
            true,
            &request_id,
        );
    };
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Some(value) = content_type.as_deref() {
        relay_header(&mut headers, "content-type", value);
    }
    if let Some(value) = retry_after.as_deref() {
        relay_header(&mut headers, "retry-after", value);
    }
    relay_header(&mut headers, "x-marketcow-relay-schema", SCHEMA);
    relay_header(&mut headers, "x-marketcow-relay-request-id", &request_id);
    relay_header(&mut headers, "x-marketcow-market-id", &identity.market_id);
    relay_header(
        &mut headers,
        "x-marketcow-condition-id",
        &identity.condition_id,
    );
    relay_header(&mut headers, "x-marketcow-token-id", &query.token_id);
    relay_header(
        &mut headers,
        "x-marketcow-rule-sha256",
        &identity.rule_sha256,
    );
    relay_header(
        &mut headers,
        "x-marketcow-profile-epoch",
        &identity.profile_epoch,
    );
    relay_header(
        &mut headers,
        "x-marketcow-profile-revision",
        &identity.profile_revision.to_string(),
    );
    relay_header(
        &mut headers,
        "x-marketcow-stream-epoch",
        &identity.stream_epoch,
    );
    relay_header(
        &mut headers,
        "x-marketcow-source-epoch",
        &query.source_epoch,
    );
    relay_header(
        &mut headers,
        "x-marketcow-identity-checked-at-unix-ns",
        &checked_at.to_string(),
    );
    relay_header(
        &mut headers,
        "x-marketcow-upstream-started-at-unix-ns",
        &started_at.to_string(),
    );
    relay_header(
        &mut headers,
        "x-marketcow-upstream-received-at-unix-ns",
        &received_at.to_string(),
    );
    relay_header(
        &mut headers,
        "x-marketcow-upstream-duration-us",
        &started.elapsed().as_micros().to_string(),
    );
    relay_header(
        &mut headers,
        "x-marketcow-upstream-status",
        &status.as_u16().to_string(),
    );
    relay_header(
        &mut headers,
        "x-marketcow-upstream-body-sha256",
        &hex::encode(Sha256::digest(&body)),
    );
    (status, headers, body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pn1_handshake_rejection_is_safe_and_rate_limited() {
        let response = axum::http::Response::builder()
            .status(429)
            .header("retry-after", "120")
            .body(Some(Vec::new()))
            .unwrap();
        let error = pn1_connect_error(VendorError::Http(Box::new(response)));
        let rejection = error.downcast_ref::<Pn1HttpRejection>().unwrap();
        assert_eq!(rejection.status, 429);
        assert_eq!(rejection.retry_after, Some(Duration::from_secs(120)));
        assert_eq!(error.to_string(), "PN1 handshake HTTP 429");
        assert_eq!(pn1_retry_after("invalid"), None);
    }

    fn lane_state() -> LaneState {
        LaneState {
            identity: wire::MarketIdentity {
                market_id: "5400001".into(),
                stream_epoch: "market-epoch".into(),
                ..Default::default()
            },
            outcomes: Vec::new(),
            source_epoch: "source-epoch".into(),
            serial: 0,
            floor: 0,
            books: BTreeMap::new(),
            replay: VecDeque::new(),
            replay_bytes: 0,
            unavailable_reason: "awaiting_pn1_anchor".into(),
            closed: false,
        }
    }

    #[test]
    fn market_frames_commit_as_one_serialized_batch() {
        let mut state = lane_state();
        state
            .append_many(vec![
                wire::market_stream_frame::Body::Unavailable(wire::MarketInvalidated {
                    token_ids: vec!["10001".into()],
                    reason: "sequence_gap".into(),
                    requires_new_snapshot: true,
                }),
                wire::market_stream_frame::Body::Ready(wire::MarketReady {
                    market_serial: 2,
                    verified_token_ids: vec!["10001".into()],
                    execution_readiness: false,
                }),
            ])
            .unwrap();
        assert_eq!(state.serial, 2);
        assert_eq!(state.replay.len(), 2);
        assert_eq!(state.replay[0].0, 1);
        assert_eq!(state.replay[1].0, 2);
    }

    #[test]
    fn oversized_second_frame_does_not_publish_first_frame() {
        let mut state = lane_state();
        let failed = state.append_many(vec![
            wire::market_stream_frame::Body::Unavailable(wire::MarketInvalidated {
                token_ids: vec!["10001".into()],
                reason: "sequence_gap".into(),
                requires_new_snapshot: true,
            }),
            wire::market_stream_frame::Body::DepthBatch(wire::MarketDepthBatch {
                raw_frame: vec![b'x'; FRAME_BYTES + 1],
                ..Default::default()
            }),
        ]);
        assert!(failed.is_err());
        assert_eq!(state.serial, 0);
        assert!(state.replay.is_empty());
        assert_eq!(state.replay_bytes, 0);
    }
}
