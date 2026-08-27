//! Deterministic realtime domain core. It deliberately has no async runtime, HTTP or DB dependency.

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
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
pub struct CanonicalEvent {
    pub schema_version: String,
    pub cursor: u64,
    pub event_id: String,
    pub scope_id: String,
    pub source_observed_at: DateTime<Utc>,
    pub kind: EventKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Projection {
    pub generation: u64,
    pub cursor: u64,
    pub persisted_cursor: u64,
    pub scope_id: String,
    pub books: BTreeMap<String, Book>,
    pub unresolved_gaps: BTreeSet<String>,
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
            "ready": self.ready,
            "fail_closed_reason": self.fail_closed_reason,
        });
        let bytes = serde_json::to_vec(&value).expect("projection is serializable");
        hex::encode(Sha256::digest(bytes))
    }
}

pub trait DurableLog {
    fn append(&mut self, event: &CanonicalEvent) -> Result<(), CoreError>;
}

/// Per-client bounded queue. A full queue closes only that consumer; it never blocks apply.
pub struct BoundedClientQueue<T> {
    sender: SyncSender<T>,
    capacity: usize,
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
    pub fn apply(&mut self, event: CanonicalEvent) -> Result<Arc<Projection>, CoreError> {
        let previous = self.current.load_full();
        if event.schema_version != CONTRACT_VERSION {
            return Err(CoreError::SchemaMismatch);
        }
        if event.scope_id != previous.scope_id {
            return Err(CoreError::ScopeMismatch);
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
        match &event.kind {
            EventKind::SourceGap { token_id, .. } => {
                next.unresolved_gaps.insert(token_id.clone());
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
                    return Err(CoreError::CrossedBook(token_id.clone()));
                }
                next.books.insert(token_id.clone(), book);
                next.unresolved_gaps.remove(token_id);
            }
            EventKind::Delta {
                token_id,
                side,
                levels,
            } => {
                if next.unresolved_gaps.contains(token_id) {
                    return Err(CoreError::RecoveryRequired(token_id.clone()));
                }
                let book = next
                    .books
                    .get_mut(token_id)
                    .ok_or_else(|| CoreError::RecoveryRequired(token_id.clone()))?;
                book.apply(*side, levels, event.source_observed_at);
                if book.crossed_or_locked() {
                    return Err(CoreError::CrossedBook(token_id.clone()));
                }
            }
        }
        next.ready = next.unresolved_gaps.is_empty() && !next.books.is_empty();
        next.fail_closed_reason = (!next.ready).then(|| "unresolved_gap".into());
        self.log.append(&event)?;
        next.persisted_cursor = event.cursor;
        let published = Arc::new(next);
        self.current.store(published.clone());
        Ok(published)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WalRecord {
    cursor: u64,
    crc32c: u32,
    payload_sha256: String,
    payload: CanonicalEvent,
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
        Ok(Self {
            root: root.into(),
            stream_id: stream_id.into(),
            max_segment_bytes,
            segment_first_cursor: None,
            current_path: None,
            file: None,
            bytes: 0,
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

    pub fn verify(root: impl AsRef<Path>) -> Result<Vec<CanonicalEvent>, CoreError> {
        let mut paths: Vec<_> = fs::read_dir(root)?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("wal"))
            .collect();
        paths.sort();
        let mut output = Vec::new();
        let mut previous_segment_sha256: Option<String> = None;
        for path in paths {
            let mut lines = BufReader::new(File::open(&path)?).lines();
            let header: serde_json::Value =
                serde_json::from_str(&lines.next().ok_or(CoreError::CorruptWal)??)?;
            if header.get("magic").and_then(|x| x.as_str()) != Some("MCWAL1") {
                return Err(CoreError::CorruptWal);
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
                if output.last().map(|e: &CanonicalEvent| e.cursor + 1) != Some(record.cursor)
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

impl DurableLog for SegmentedWal {
    fn append(&mut self, event: &CanonicalEvent) -> Result<(), CoreError> {
        let payload = serde_json::to_vec(event)?;
        let record = WalRecord {
            cursor: event.cursor,
            crc32c: crc32c::crc32c(&payload),
            payload_sha256: hex::encode(Sha256::digest(&payload)),
            payload: event.clone(),
        };
        let line = serde_json::to_vec(&record)?;
        if self.file.is_none() || self.bytes + line.len() as u64 + 1 > self.max_segment_bytes {
            self.rotate(event.cursor)?;
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

pub trait InstrumentRepository: Send + Sync {}
pub trait JobRepository: Send + Sync {}
pub trait AuditRepository: Send + Sync {}
pub trait ArtifactManifestRepository: Send + Sync {}
pub trait FundamentalRepository: Send + Sync {}
pub trait MarketBarRepository: Send + Sync {}
pub trait QuoteRepository: Send + Sync {}
pub trait MigrationCheckpointRepository: Send + Sync {}

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
    #[error("checkpoint hash mismatch")]
    CheckpointHash,
    #[error("published and persisted watermark mismatch")]
    PersistedWatermarkMismatch,
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
        CanonicalEvent {
            schema_version: CONTRACT_VERSION.into(),
            cursor,
            event_id: format!("e-{cursor}"),
            scope_id: "scope".into(),
            source_observed_at: Utc::now(),
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
    fn gap_and_crossed_delta_fail_closed_without_advancing() {
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
        assert!(
            state
                .apply(event(
                    2,
                    EventKind::Delta {
                        token_id: "t".into(),
                        side: Side::Bid,
                        levels: levels("0.7", "1")
                    }
                ))
                .is_err()
        );
        assert_eq!(state.projection().cursor, 1);
        state
            .apply(event(
                2,
                EventKind::SourceGap {
                    token_id: "t".into(),
                    reason: "sequence".into(),
                },
            ))
            .unwrap();
        assert!(
            state
                .apply(event(
                    3,
                    EventKind::Delta {
                        token_id: "t".into(),
                        side: Side::Bid,
                        levels: levels("0.3", "1")
                    }
                ))
                .is_err()
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
        assert_eq!(replay[0].cursor, 1);
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
}
