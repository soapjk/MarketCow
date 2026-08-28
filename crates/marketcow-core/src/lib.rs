//! Deterministic realtime domain core. It deliberately has no async runtime, HTTP or DB dependency.

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    },
};
use thiserror::Error;

pub const CONTRACT_VERSION: &str = "marketcow.polymarket.shadow.v1";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Price(#[serde(with = "rust_decimal::serde::str")] pub Decimal);

impl Price {
    pub fn parse(value: &str) -> Result<Self, CoreError> {
        let parsed =
            Decimal::from_str(value).map_err(|_| CoreError::InvalidDecimal(value.into()))?;
        if parsed < Decimal::ZERO || parsed > Decimal::ONE {
            return Err(CoreError::InvalidPrice(value.into()));
        }
        Ok(Self(parsed.normalize()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
}

impl Level {
    pub fn new(price: &str, quantity: &str) -> Result<Self, CoreError> {
        let quantity =
            Decimal::from_str(quantity).map_err(|_| CoreError::InvalidDecimal(quantity.into()))?;
        if quantity < Decimal::ZERO {
            return Err(CoreError::InvalidQuantity);
        }
        Ok(Self {
            price: Price::parse(price)?,
            quantity: quantity.normalize(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Book {
    pub bids: BTreeMap<Price, Decimal>,
    pub asks: BTreeMap<Price, Decimal>,
    pub tick_version: u64,
    pub source_observed_at: Option<DateTime<Utc>>,
}

impl Book {
    fn replace(&mut self, bids: &[Level], asks: &[Level], tick_version: u64, at: DateTime<Utc>) {
        self.bids = bids
            .iter()
            .filter(|x| !x.quantity.is_zero())
            .map(|x| (x.price.clone(), x.quantity))
            .collect();
        self.asks = asks
            .iter()
            .filter(|x| !x.quantity.is_zero())
            .map(|x| (x.price.clone(), x.quantity))
            .collect();
        self.tick_version = tick_version;
        self.source_observed_at = Some(at);
    }

    fn apply(&mut self, side: Side, levels: &[Level], at: DateTime<Utc>) {
        let target = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        for level in levels {
            if level.quantity.is_zero() {
                target.remove(&level.price);
            } else {
                target.insert(level.price.clone(), level.quantity);
            }
        }
        self.source_observed_at = Some(at);
    }

    pub fn crossed_or_locked(&self) -> bool {
        match (self.bids.last_key_value(), self.asks.first_key_value()) {
            (Some((bid, _)), Some((ask, _))) => bid >= ask,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum EventKind {
    FullBook {
        token_id: String,
        bids: Vec<Level>,
        asks: Vec<Level>,
        tick_version: u64,
    },
    Delta {
        token_id: String,
        side: Side,
        levels: Vec<Level>,
    },
    SourceGap {
        token_id: String,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceEvidence {
    pub source: String,
    pub source_url: Option<String>,
    pub requested_at: DateTime<Utc>,
    pub responded_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub raw_sha256: String,
    pub update_frequency: String,
    pub revision: String,
    pub missing: bool,
    pub delayed: bool,
    pub duplicate: bool,
    pub revised: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalEvent {
    pub schema_version: String,
    pub cursor: u64,
    pub event_id: String,
    pub scope_id: String,
    pub received_at: DateTime<Utc>,
    pub source_observed_at: DateTime<Utc>,
    pub normalizer_version: String,
    pub config_revision: String,
    pub source: SourceEvidence,
    pub raw_payload: serde_json::Value,
    pub kind: EventKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedEvent {
    pub event: CanonicalEvent,
    pub applied: bool,
    pub fail_closed_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ApplyOutcome {
    pub projection: Arc<Projection>,
    pub persisted: PersistedEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Projection {
    pub generation: u64,
    pub cursor: u64,
    pub persisted_cursor: u64,
    pub scope_id: String,
    pub books: BTreeMap<String, Book>,
    pub unresolved_gaps: BTreeSet<String>,
    pub recent_event_ids: VecDeque<String>,
    pub ready: bool,
    pub fail_closed_reason: Option<String>,
    pub published_at: DateTime<Utc>,
}

impl Projection {
    pub fn hash(&self) -> String {
        // Publication wall time is observability metadata, not canonical state.
        let value = serde_json::json!({
            "generation": self.generation,
            "cursor": self.cursor,
            "persisted_cursor": self.persisted_cursor,
            "scope_id": self.scope_id,
            "books": self.books,
            "unresolved_gaps": self.unresolved_gaps,
            "recent_event_ids": self.recent_event_ids,
            "ready": self.ready,
            "fail_closed_reason": self.fail_closed_reason,
        });
        let bytes = serde_json::to_vec(&value).expect("projection is serializable");
        hex::encode(Sha256::digest(bytes))
    }
}

pub trait DurableLog {
    fn append(&mut self, event: &CanonicalEvent) -> Result<(), CoreError>;

    fn append_outcome(&mut self, outcome: &PersistedEvent) -> Result<(), CoreError> {
        self.append(&outcome.event)
    }
}

/// Per-client bounded queue. A full queue closes only that consumer; it never blocks apply.
pub struct BoundedClientQueue<T> {
    sender: SyncSender<T>,
    capacity: usize,
}

/// Bounded ingress queue for the authoritative single writer. A rejected value is returned to
/// the caller so an upstream reconnect/recovery path cannot accidentally drop it.
pub struct BoundedApplyQueue<T> {
    sender: SyncSender<T>,
    capacity: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ApplyQueueError<T> {
    Full(T),
    Disconnected(T),
}

impl<T> BoundedApplyQueue<T> {
    pub fn new(capacity: usize) -> Result<(Self, Receiver<T>), CoreError> {
        if capacity == 0 {
            return Err(CoreError::InvalidQueueCapacity);
        }
        let (sender, receiver) = sync_channel(capacity);
        Ok((Self { sender, capacity }, receiver))
    }

    pub fn enqueue(&self, value: T) -> Result<(), ApplyQueueError<T>> {
        self.sender.try_send(value).map_err(|error| match error {
            TrySendError::Full(value) => ApplyQueueError::Full(value),
            TrySendError::Disconnected(value) => ApplyQueueError::Disconnected(value),
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<T> BoundedClientQueue<T> {
    pub fn new(capacity: usize) -> Result<(Self, Receiver<T>), CoreError> {
        if capacity == 0 {
            return Err(CoreError::InvalidQueueCapacity);
        }
        let (sender, receiver) = sync_channel(capacity);
        Ok((Self { sender, capacity }, receiver))
    }

    pub fn publish(&self, value: T) -> Result<(), CoreError> {
        self.sender.try_send(value).map_err(|error| match error {
            TrySendError::Full(_) => CoreError::SlowConsumer { close_code: 1013 },
            TrySendError::Disconnected(_) => CoreError::ConsumerDisconnected,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

pub struct SingleWriter<L> {
    current: ArcSwap<Projection>,
    log: L,
}

impl<L: DurableLog> SingleWriter<L> {
    pub fn new(scope_id: String, log: L) -> Self {
        Self {
            current: ArcSwap::from_pointee(Projection {
                generation: 0,
                cursor: 0,
                persisted_cursor: 0,
                scope_id,
                books: BTreeMap::new(),
                unresolved_gaps: BTreeSet::new(),
                recent_event_ids: VecDeque::new(),
                ready: false,
                fail_closed_reason: Some("bootstrap_required".into()),
                published_at: Utc::now(),
            }),
            log,
        }
    }

    pub fn projection(&self) -> Arc<Projection> {
        self.current.load_full()
    }

    pub fn resume(projection: Projection, log: L) -> Result<Self, CoreError> {
        if projection.cursor != projection.persisted_cursor {
            return Err(CoreError::PersistedWatermarkMismatch);
        }
        Ok(Self {
            current: ArcSwap::from_pointee(projection),
            log,
        })
    }

    /// Applies a candidate, validates it, durably appends, then atomically publishes it.
    pub fn apply(&mut self, event: CanonicalEvent) -> Result<ApplyOutcome, CoreError> {
        let previous = self.current.load_full();
        if event.schema_version != CONTRACT_VERSION {
            return Err(CoreError::SchemaMismatch);
        }
        if event.scope_id != previous.scope_id {
            return Err(CoreError::ScopeMismatch);
        }
        if event.normalizer_version.is_empty()
            || event.config_revision.is_empty()
            || event.source.responded_at < event.source.requested_at
            || event.source.raw_sha256.len() != 64
            || !event
                .source
                .raw_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || hex::encode(Sha256::digest(serde_json::to_vec(&event.raw_payload)?))
                != event.source.raw_sha256.to_ascii_lowercase()
        {
            return Err(CoreError::InvalidSourceEvidence);
        }
        if previous.recent_event_ids.contains(&event.event_id) {
            return Err(CoreError::DuplicateEvent(event.event_id));
        }
        if event.cursor != previous.cursor + 1 {
            return Err(CoreError::CursorGap {
                expected: previous.cursor + 1,
                actual: event.cursor,
            });
        }
        let mut next = (*previous).clone();
        next.generation += 1;
        next.cursor = event.cursor;
        next.published_at = Utc::now();
        let mut applied = true;
        let mut rejected = None;
        match &event.kind {
            EventKind::SourceGap { token_id, reason } => {
                next.unresolved_gaps.insert(token_id.clone());
                applied = false;
                rejected = Some(format!("source_gap:{reason}"));
            }
            EventKind::FullBook {
                token_id,
                bids,
                asks,
                tick_version,
            } => {
                let mut book = next.books.get(token_id).cloned().unwrap_or_default();
                book.replace(bids, asks, *tick_version, event.source_observed_at);
                if book.crossed_or_locked() {
                    next.unresolved_gaps.insert(token_id.clone());
                    applied = false;
                    rejected = Some("crossed_or_locked_full_book".into());
                } else {
                    next.books.insert(token_id.clone(), book);
                    next.unresolved_gaps.remove(token_id);
                }
            }
            EventKind::Delta {
                token_id,
                side,
                levels,
            } => {
                if next.unresolved_gaps.contains(token_id) || !next.books.contains_key(token_id) {
                    next.unresolved_gaps.insert(token_id.clone());
                    applied = false;
                    rejected = Some("full_book_recovery_required".into());
                } else {
                    let mut book = next.books[token_id].clone();
                    book.apply(*side, levels, event.source_observed_at);
                    if book.crossed_or_locked() {
                        next.unresolved_gaps.insert(token_id.clone());
                        applied = false;
                        rejected = Some("crossed_or_locked_delta".into());
                    } else {
                        next.books.insert(token_id.clone(), book);
                    }
                }
            }
        }
        next.ready = next.unresolved_gaps.is_empty() && !next.books.is_empty();
        next.fail_closed_reason = rejected
            .clone()
            .or_else(|| (!next.ready).then(|| "unresolved_gap".into()));
        let persisted = PersistedEvent {
            event: event.clone(),
            applied,
            fail_closed_reason: rejected,
        };
        self.log.append_outcome(&persisted)?;
        next.persisted_cursor = event.cursor;
        next.recent_event_ids.push_back(event.event_id);
        if next.recent_event_ids.len() > 10_000 {
            next.recent_event_ids.pop_front();
        }
        let published = Arc::new(next);
        self.current.store(published.clone());
        Ok(ApplyOutcome {
            projection: published,
            persisted,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WalRecord {
    cursor: u64,
    crc32c: u32,
    payload_sha256: String,
    payload: PersistedEvent,
}

pub struct SegmentedWal {
    root: PathBuf,
    stream_id: String,
    max_segment_bytes: u64,
    segment_first_cursor: Option<u64>,
    current_path: Option<PathBuf>,
    file: Option<File>,
    bytes: u64,
}

impl SegmentedWal {
    pub fn open(
        root: impl AsRef<Path>,
        stream_id: &str,
        max_segment_bytes: u64,
    ) -> Result<Self, CoreError> {
        let root = root.as_ref();
        if !root.is_absolute() {
            return Err(CoreError::PathMustBeAbsolute);
        }
        if max_segment_bytes < 1024 {
            return Err(CoreError::SegmentTooSmall);
        }
        fs::create_dir_all(root)?;
        let mut paths = wal_paths(root)?;
        let (segment_first_cursor, current_path, file, bytes) = match paths.pop() {
            Some(path) => {
                // Refuse to append to an unverified chain or a different stream.
                Self::verify(root)?;
                let header = read_wal_header(&path)?;
                if header.get("stream_id").and_then(|value| value.as_str()) != Some(stream_id) {
                    return Err(CoreError::WalStreamMismatch);
                }
                let first_cursor = header
                    .get("first_cursor")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or(CoreError::CorruptWal)?;
                let bytes = fs::metadata(&path)?.len();
                let file = OpenOptions::new().append(true).open(&path)?;
                (Some(first_cursor), Some(path), Some(file), bytes)
            }
            None => (None, None, None, 0),
        };
        Ok(Self {
            root: root.into(),
            stream_id: stream_id.into(),
            max_segment_bytes,
            segment_first_cursor,
            current_path,
            file,
            bytes,
        })
    }

    fn rotate(&mut self, cursor: u64) -> Result<(), CoreError> {
        if let Some(file) = &mut self.file {
            file.sync_all()?;
        }
        let previous_segment_sha256 = match &self.current_path {
            Some(path) => Some(hex::encode(Sha256::digest(fs::read(path)?))),
            None => None,
        };
        let path = self
            .root
            .join(format!("{}-{cursor:020}.wal", self.stream_id));
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        let header = serde_json::json!({"magic":"MCWAL1","wal_version":1,
            "stream_id":self.stream_id,"first_cursor":cursor,
            "previous_segment_sha256":previous_segment_sha256});
        let line = serde_json::to_vec(&header)?;
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        self.bytes = line.len() as u64 + 1;
        self.segment_first_cursor = Some(cursor);
        self.current_path = Some(path);
        self.file = Some(file);
        Ok(())
    }

    pub fn verify(root: impl AsRef<Path>) -> Result<Vec<PersistedEvent>, CoreError> {
        let paths = wal_paths(root.as_ref())?;
        let mut output = Vec::new();
        let mut previous_segment_sha256: Option<String> = None;
        let mut stream_id: Option<String> = None;
        for path in paths {
            let mut lines = BufReader::new(File::open(&path)?).lines();
            let header: serde_json::Value =
                serde_json::from_str(&lines.next().ok_or(CoreError::CorruptWal)??)?;
            if header.get("magic").and_then(|x| x.as_str()) != Some("MCWAL1") {
                return Err(CoreError::CorruptWal);
            }
            let actual_stream_id = header
                .get("stream_id")
                .and_then(serde_json::Value::as_str)
                .ok_or(CoreError::CorruptWal)?;
            if stream_id.get_or_insert_with(|| actual_stream_id.to_owned()) != actual_stream_id {
                return Err(CoreError::WalStreamMismatch);
            }
            let actual_previous = header
                .get("previous_segment_sha256")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            if actual_previous != previous_segment_sha256 {
                return Err(CoreError::CorruptWal);
            }
            for line in lines {
                let record: WalRecord = serde_json::from_str(&line?)?;
                let payload = serde_json::to_vec(&record.payload)?;
                if crc32c::crc32c(&payload) != record.crc32c
                    || hex::encode(Sha256::digest(&payload)) != record.payload_sha256
                {
                    return Err(CoreError::CorruptWal);
                }
                if output.last().map(|e: &PersistedEvent| e.event.cursor + 1) != Some(record.cursor)
                    && !output.is_empty()
                {
                    return Err(CoreError::CorruptWal);
                }
                output.push(record.payload);
            }
            previous_segment_sha256 = Some(hex::encode(Sha256::digest(fs::read(path)?)));
        }
        Ok(output)
    }
}

fn wal_paths(root: &Path) -> Result<Vec<PathBuf>, CoreError> {
    let mut paths: Vec<_> = fs::read_dir(root)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("wal"))
        .collect();
    paths.sort();
    Ok(paths)
}

fn read_wal_header(path: &Path) -> Result<serde_json::Value, CoreError> {
    let line = BufReader::new(File::open(path)?)
        .lines()
        .next()
        .ok_or(CoreError::CorruptWal)??;
    let header = serde_json::from_str(&line)?;
    Ok(header)
}

impl DurableLog for SegmentedWal {
    fn append(&mut self, event: &CanonicalEvent) -> Result<(), CoreError> {
        self.append_outcome(&PersistedEvent {
            event: event.clone(),
            applied: true,
            fail_closed_reason: None,
        })
    }

    fn append_outcome(&mut self, outcome: &PersistedEvent) -> Result<(), CoreError> {
        let payload = serde_json::to_vec(outcome)?;
        let record = WalRecord {
            cursor: outcome.event.cursor,
            crc32c: crc32c::crc32c(&payload),
            payload_sha256: hex::encode(Sha256::digest(&payload)),
            payload: outcome.clone(),
        };
        let line = serde_json::to_vec(&record)?;
        if self.file.is_none() || self.bytes + line.len() as u64 + 1 > self.max_segment_bytes {
            self.rotate(outcome.event.cursor)?;
        }
        let file = self.file.as_mut().expect("rotated");
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        self.bytes += line.len() as u64 + 1;
        Ok(())
    }
}

pub fn write_checkpoint(path: &Path, projection: &Projection) -> Result<String, CoreError> {
    if !path.is_absolute() {
        return Err(CoreError::PathMustBeAbsolute);
    }
    let bytes = serde_json::to_vec(projection)?;
    let hash = hex::encode(Sha256::digest(&bytes));
    let temp = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    File::open(path.parent().ok_or(CoreError::PathMustBeAbsolute)?)?.sync_all()?;
    Ok(hash)
}

pub fn read_checkpoint(path: &Path, expected_hash: &str) -> Result<Projection, CoreError> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    if hex::encode(Sha256::digest(&bytes)) != expected_hash {
        return Err(CoreError::CheckpointHash);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

/// Rebuilds canonical state strictly after a durable checkpoint and verifies that current code
/// derives the same apply/reject decision recorded in the append-only log.
pub fn replay_after_checkpoint(
    checkpoint: Projection,
    records: &[PersistedEvent],
) -> Result<Projection, CoreError> {
    struct ReplayLog;
    impl DurableLog for ReplayLog {
        fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
            Ok(())
        }
    }

    let checkpoint_cursor = checkpoint.cursor;
    let mut writer = SingleWriter::resume(checkpoint, ReplayLog)?;
    for persisted in records
        .iter()
        .filter(|record| record.event.cursor > checkpoint_cursor)
    {
        let replayed = writer.apply(persisted.event.clone())?.persisted;
        if replayed.applied != persisted.applied
            || replayed.fail_closed_reason != persisted.fail_closed_reason
        {
            return Err(CoreError::ReplayDivergence {
                cursor: persisted.event.cursor,
            });
        }
    }
    Ok((*writer.projection()).clone())
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("invalid decimal: {0}")]
    InvalidDecimal(String),
    #[error("price outside [0,1]: {0}")]
    InvalidPrice(String),
    #[error("quantity cannot be negative")]
    InvalidQuantity,
    #[error("schema mismatch")]
    SchemaMismatch,
    #[error("scope mismatch")]
    ScopeMismatch,
    #[error("source evidence is incomplete or invalid")]
    InvalidSourceEvidence,
    #[error("duplicate event: {0}")]
    DuplicateEvent(String),
    #[error("cursor gap expected {expected} got {actual}")]
    CursorGap { expected: u64, actual: u64 },
    #[error("crossed or locked book: {0}")]
    CrossedBook(String),
    #[error("full-book recovery required: {0}")]
    RecoveryRequired(String),
    #[error("path must be absolute")]
    PathMustBeAbsolute,
    #[error("WAL segment too small")]
    SegmentTooSmall,
    #[error("corrupt WAL")]
    CorruptWal,
    #[error("WAL contains a different stream")]
    WalStreamMismatch,
    #[error("checkpoint hash mismatch")]
    CheckpointHash,
    #[error("published and persisted watermark mismatch")]
    PersistedWatermarkMismatch,
    #[error("replay decision diverged at cursor {cursor}")]
    ReplayDivergence { cursor: u64 },
    #[error("queue capacity must be positive")]
    InvalidQueueCapacity,
    #[error("slow consumer must close with {close_code}")]
    SlowConsumer { close_code: u16 },
    #[error("consumer disconnected")]
    ConsumerDisconnected,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn event(cursor: u64, kind: EventKind) -> CanonicalEvent {
        let at = Utc::now();
        let raw_payload = serde_json::json!({"fixture_cursor": cursor});
        CanonicalEvent {
            schema_version: CONTRACT_VERSION.into(),
            cursor,
            event_id: format!("e-{cursor}"),
            scope_id: "scope".into(),
            received_at: at,
            source_observed_at: at,
            normalizer_version: "test-v1".into(),
            config_revision: "config-v1".into(),
            source: SourceEvidence {
                source: "polymarket-clob".into(),
                source_url: Some("https://clob.polymarket.com".into()),
                requested_at: at,
                responded_at: at,
                observed_at: at,
                raw_sha256: hex::encode(Sha256::digest(serde_json::to_vec(&raw_payload).unwrap())),
                update_frequency: "realtime".into(),
                revision: "fixture-v1".into(),
                missing: false,
                delayed: false,
                duplicate: false,
                revised: false,
            },
            raw_payload,
            kind,
        }
    }
    fn levels(price: &str, quantity: &str) -> Vec<Level> {
        vec![Level::new(price, quantity).unwrap()]
    }

    #[test]
    fn decimal_is_exact_and_rejects_float_semantics() {
        assert_eq!(
            Price::parse("0.100000000000000001").unwrap().0.to_string(),
            "0.100000000000000001"
        );
        assert!(Price::parse("1.0001").is_err());
        assert!(Level::new("0.1", "-1").is_err());
    }

    #[test]
    fn rejected_delta_is_persisted_and_full_book_recovers_next_cursor() {
        let dir = tempdir().unwrap();
        let wal = SegmentedWal::open(dir.path(), "test", 4096).unwrap();
        let mut state = SingleWriter::new("scope".into(), wal);
        state
            .apply(event(
                1,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.4", "1"),
                    asks: levels("0.6", "1"),
                    tick_version: 1,
                },
            ))
            .unwrap();
        let rejected = state
            .apply(event(
                2,
                EventKind::Delta {
                    token_id: "t".into(),
                    side: Side::Bid,
                    levels: levels("0.7", "1"),
                },
            ))
            .unwrap();
        assert!(!rejected.persisted.applied);
        assert_eq!(
            rejected.persisted.fail_closed_reason.as_deref(),
            Some("crossed_or_locked_delta")
        );
        assert_eq!(state.projection().cursor, 2);
        state
            .apply(event(
                3,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.3", "2"),
                    asks: levels("0.7", "2"),
                    tick_version: 2,
                },
            ))
            .unwrap();
        assert!(state.projection().ready);
        drop(state);
        let persisted = SegmentedWal::verify(dir.path()).unwrap();
        assert_eq!(persisted.len(), 3);
        assert!(!persisted[1].applied);
        assert_eq!(
            persisted[1].fail_closed_reason.as_deref(),
            Some("crossed_or_locked_delta")
        );
        assert!(persisted[2].applied);
    }

    #[test]
    fn wal_checkpoint_and_replay_are_deterministic() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        fs::create_dir(&wal_dir).unwrap();
        let wal = SegmentedWal::open(&wal_dir, "test", 1024).unwrap();
        let mut state = SingleWriter::new("scope".into(), wal);
        state
            .apply(event(
                1,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.1", "3"),
                    asks: levels("0.9", "3"),
                    tick_version: 1,
                },
            ))
            .unwrap();
        let checkpoint = dir.path().join("checkpoint.json");
        let hash = write_checkpoint(&checkpoint, &state.projection()).unwrap();
        assert_eq!(
            read_checkpoint(&checkpoint, &hash).unwrap().hash(),
            state.projection().hash()
        );
        let replay = SegmentedWal::verify(&wal_dir).unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].event.cursor, 1);
    }

    #[test]
    fn online_and_replay_projection_hashes_match() {
        struct MemoryLog(Vec<CanonicalEvent>);
        impl DurableLog for MemoryLog {
            fn append(&mut self, event: &CanonicalEvent) -> Result<(), CoreError> {
                self.0.push(event.clone());
                Ok(())
            }
        }
        let events = vec![
            event(
                1,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.2", "1.25"),
                    asks: levels("0.8", "3.5"),
                    tick_version: 1,
                },
            ),
            event(
                2,
                EventKind::Delta {
                    token_id: "t".into(),
                    side: Side::Bid,
                    levels: levels("0.3", "2.75"),
                },
            ),
            event(
                3,
                EventKind::SourceGap {
                    token_id: "t".into(),
                    reason: "disconnect".into(),
                },
            ),
            event(
                4,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.25", "5"),
                    asks: levels("0.75", "5"),
                    tick_version: 2,
                },
            ),
        ];
        let mut online = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        for value in events.clone() {
            online.apply(value).unwrap();
        }
        let mut replay = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        for value in events {
            replay.apply(value).unwrap();
        }
        assert_eq!(online.projection().hash(), replay.projection().hash());
    }

    #[test]
    fn persistence_failure_never_publishes_candidate() {
        struct FailedLog;
        impl DurableLog for FailedLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                Err(CoreError::CorruptWal)
            }
        }
        let mut state = SingleWriter::new("scope".into(), FailedLog);
        assert!(
            state
                .apply(event(
                    1,
                    EventKind::FullBook {
                        token_id: "t".into(),
                        bids: levels("0.2", "1"),
                        asks: levels("0.8", "1"),
                        tick_version: 1
                    }
                ))
                .is_err()
        );
        assert_eq!(state.projection().cursor, 0);
        assert_eq!(state.projection().persisted_cursor, 0);
    }

    #[test]
    fn slow_consumer_is_isolated_by_bounded_queue() {
        let (queue, receiver) = BoundedClientQueue::new(1).unwrap();
        assert_eq!(queue.capacity(), 1);
        queue.publish(1).unwrap();
        assert!(matches!(
            queue.publish(2),
            Err(CoreError::SlowConsumer { close_code: 1013 })
        ));
        assert_eq!(receiver.recv().unwrap(), 1);
    }

    #[test]
    fn ingress_backpressure_returns_the_unqueued_event_without_loss() {
        let (queue, receiver) = BoundedApplyQueue::new(1).unwrap();
        assert_eq!(queue.capacity(), 1);
        queue.enqueue("first").unwrap();
        assert_eq!(
            queue.enqueue("must-recover"),
            Err(ApplyQueueError::Full("must-recover"))
        );
        assert_eq!(receiver.recv().unwrap(), "first");
        drop(receiver);
        assert_eq!(
            queue.enqueue("disconnected"),
            Err(ApplyQueueError::Disconnected("disconnected"))
        );
    }

    #[test]
    fn duplicate_event_is_auditable_and_does_not_advance() {
        struct MemoryLog;
        impl DurableLog for MemoryLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                Ok(())
            }
        }
        let mut state = SingleWriter::new("scope".into(), MemoryLog);
        let first = event(
            1,
            EventKind::FullBook {
                token_id: "t".into(),
                bids: levels("0.2", "1"),
                asks: levels("0.8", "1"),
                tick_version: 1,
            },
        );
        state.apply(first.clone()).unwrap();
        let mut duplicate = first;
        duplicate.cursor = 2;
        assert!(matches!(
            state.apply(duplicate),
            Err(CoreError::DuplicateEvent(_))
        ));
        assert_eq!(state.projection().cursor, 1);
    }

    #[test]
    fn wal_chain_and_corruption_are_verified() {
        let dir = tempdir().unwrap();
        let mut wal = SegmentedWal::open(dir.path(), "chain", 1024).unwrap();
        for cursor in 1..=8 {
            wal.append(&event(
                cursor,
                EventKind::FullBook {
                    token_id: format!("token-{cursor}"),
                    bids: levels("0.2", "123456789.123456789"),
                    asks: levels("0.8", "987654321.987654321"),
                    tick_version: 1,
                },
            ))
            .unwrap();
        }
        drop(wal);
        assert_eq!(SegmentedWal::verify(dir.path()).unwrap().len(), 8);
        let mut paths: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        paths.sort();
        OpenOptions::new()
            .append(true)
            .open(paths.last().unwrap())
            .unwrap()
            .write_all(b"{corrupt}\n")
            .unwrap();
        assert!(SegmentedWal::verify(dir.path()).is_err());
    }

    #[test]
    fn wal_restart_preserves_segment_hash_chain() {
        let dir = tempdir().unwrap();
        let mut first = SegmentedWal::open(dir.path(), "restart", 1024).unwrap();
        first
            .append(&event(
                1,
                EventKind::SourceGap {
                    token_id: "t".into(),
                    reason: "disconnect".into(),
                },
            ))
            .unwrap();
        drop(first);

        let mut resumed = SegmentedWal::open(dir.path(), "restart", 1024).unwrap();
        resumed
            .append(&event(
                2,
                EventKind::SourceGap {
                    token_id: "t".into(),
                    reason: "reconnect_pending".into(),
                },
            ))
            .unwrap();
        drop(resumed);
        let records = SegmentedWal::verify(dir.path()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].event.cursor, 2);
    }

    #[test]
    fn checkpoint_replay_skips_boundary_and_detects_decision_divergence() {
        struct MemoryLog(Vec<PersistedEvent>);
        impl DurableLog for MemoryLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                unreachable!("append_outcome is used")
            }

            fn append_outcome(&mut self, outcome: &PersistedEvent) -> Result<(), CoreError> {
                self.0.push(outcome.clone());
                Ok(())
            }
        }

        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        let first = writer
            .apply(event(
                1,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.2", "1"),
                    asks: levels("0.8", "1"),
                    tick_version: 1,
                },
            ))
            .unwrap();
        let checkpoint = (*first.projection).clone();
        let rejected = writer
            .apply(event(
                2,
                EventKind::Delta {
                    token_id: "t".into(),
                    side: Side::Bid,
                    levels: levels("0.9", "1"),
                },
            ))
            .unwrap();
        let recovered = writer
            .apply(event(
                3,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.3", "2"),
                    asks: levels("0.7", "2"),
                    tick_version: 2,
                },
            ))
            .unwrap();
        let records = vec![first.persisted, rejected.persisted, recovered.persisted];

        let replayed = replay_after_checkpoint(checkpoint.clone(), &records).unwrap();
        assert_eq!(replayed.hash(), writer.projection().hash());
        assert_eq!(replayed.cursor, 3);

        let mut divergent = records;
        divergent[1].applied = true;
        assert!(matches!(
            replay_after_checkpoint(checkpoint, &divergent),
            Err(CoreError::ReplayDivergence { cursor: 2 })
        ));
    }

    #[test]
    fn checkpoint_corruption_is_rejected() {
        struct MemoryLog;
        impl DurableLog for MemoryLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                Ok(())
            }
        }
        let dir = tempdir().unwrap();
        let state = SingleWriter::new("scope".into(), MemoryLog);
        let path = dir.path().join("checkpoint.json");
        let hash = write_checkpoint(&path, &state.projection()).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"corrupt")
            .unwrap();
        assert!(matches!(
            read_checkpoint(&path, &hash),
            Err(CoreError::CheckpointHash)
        ));
    }
}
