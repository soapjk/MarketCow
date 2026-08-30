//! Polymarket runtime composition: normalization, the authoritative single writer, WAL recovery,
//! bounded replay exposure, and atomic checkpoint manifests.

use chrono::{DateTime, Utc};
use marketcow_core::{
    ApplyOutcome, EventKind, PersistedEvent, Projection, SegmentedWal, SingleWriter,
    WalCheckpointAnchor, read_checkpoint, replay_after_checkpoint, write_checkpoint,
};
use marketcow_polymarket::{
    NormalizeError, NormalizerConfig, bind_full_book_recovery_identity,
    bind_full_book_refresh_identity, normalize_frame_with_book_tick,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

const CHECKPOINT_MANIFEST_V1: &str = "marketcow.polymarket.checkpoint-manifest.v1";
pub const CHECKPOINT_MANIFEST_VERSION: &str = "marketcow.polymarket.checkpoint-manifest.v2";
const CANDIDATE_PREPARATION_TIMEOUT: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub root: PathBuf,
    pub scope_id: String,
    pub config_revision: String,
    pub wal_segment_bytes: u64,
    pub recent_event_capacity: usize,
}

impl RuntimeConfig {
    fn validate(&self) -> Result<(), RuntimeError> {
        if !self.root.is_absolute()
            || self.scope_id.is_empty()
            || self.config_revision.is_empty()
            || self.wal_segment_bytes < 1_024
            || self.recent_event_capacity == 0
        {
            return Err(RuntimeError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointReference {
    pub path: PathBuf,
    pub projection_sha256: String,
    pub cursor: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_anchor_segment_first_cursor: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_anchor_segment_header_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointManifest {
    pub schema_version: String,
    pub scope_id: String,
    pub current: CheckpointReference,
    pub previous: Option<CheckpointReference>,
    pub wal_last_cursor: u64,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_lineage: Option<CheckpointLineageReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointLineageReference {
    pub manifest_path: PathBuf,
    pub manifest_sha256: String,
    pub checkpoint_cursor: u64,
    pub checkpoint_sha256: String,
    pub wal_anchor_segment_first_cursor: u64,
    pub wal_anchor_segment_header_sha256: String,
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("runtime config is incomplete")]
    InvalidConfig,
    #[error("checkpoint manifest scope mismatch")]
    CheckpointScopeMismatch,
    #[error("checkpoint path escapes the runtime root")]
    CheckpointPathEscape,
    #[error("checkpoint cursor is not represented in the WAL")]
    CheckpointBeyondWal,
    #[error("all checkpoint candidates failed boundary verification")]
    NoValidCheckpoint,
    #[error("candidate physical copy failed byte verification")]
    PhysicalCopyMismatch,
    #[error("candidate preparation exceeded five-minute deadline")]
    CandidatePreparationTimeout,
    #[error("another Polymarket WAL writer already owns this generation")]
    WriterAlreadyActive,
    #[error("Polymarket writer lease is not a private regular file")]
    InsecureWriterLease,
    #[error(transparent)]
    Core(#[from] marketcow_core::CoreError),
    #[error(transparent)]
    Normalize(#[from] NormalizeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub struct PolymarketRuntime {
    config: RuntimeConfig,
    _writer_lock: File,
    writer: SingleWriter<SegmentedWal>,
    recent_events: VecDeque<PersistedEvent>,
    last_manifest: Option<CheckpointManifest>,
}

/// Read-only evidence produced by comparing one authoritative HTTP book observation with the
/// current WS-owned projection. Validation never advances the public cursor or writes the WAL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookObservationValidation {
    pub token_id: String,
    pub matches_projection: bool,
    pub observed_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub raw_sha256: String,
}

/// Upgrades a legacy checkpoint only after an older runtime completed full semantic recovery and
/// shut down cleanly. The caller pins the exact manifest bytes observed after shutdown. Requiring
/// the checkpoint cursor to equal the verified WAL tail prevents an anchor from being inserted
/// into the middle of a live history.
pub fn anchor_verified_legacy_checkpoint(
    config: RuntimeConfig,
    expected_manifest_sha256: &str,
) -> Result<CheckpointManifest, RuntimeError> {
    config.validate()?;
    let manifest_path = config.root.join("checkpoint-manifest.json");
    let manifest_bytes = fs::read(&manifest_path)?;
    if expected_manifest_sha256.len() != 64
        || !expected_manifest_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || hex::encode(Sha256::digest(&manifest_bytes))
            != expected_manifest_sha256.to_ascii_lowercase()
    {
        return Err(RuntimeError::PhysicalCopyMismatch);
    }
    let mut manifest: CheckpointManifest = serde_json::from_slice(&manifest_bytes)?;
    if manifest.schema_version != CHECKPOINT_MANIFEST_V1 || manifest.scope_id != config.scope_id {
        return Err(RuntimeError::CheckpointScopeMismatch);
    }
    let verified_wal = SegmentedWal::verify_with_anchors(config.root.join("wal"))?;
    let wal_last_cursor = verified_wal
        .records
        .last()
        .map_or(0, |record| record.event.cursor);
    if manifest.current.cursor != wal_last_cursor || manifest.wal_last_cursor != wal_last_cursor {
        return Err(RuntimeError::CheckpointBeyondWal);
    }
    load_checkpoint_candidate(&config, &manifest.current)?;
    let mut wal = SegmentedWal::open_after_verification(
        config.root.join("wal"),
        &config.scope_id,
        config.wal_segment_bytes,
    )?;
    let anchor =
        wal.anchor_checkpoint(manifest.current.cursor, &manifest.current.projection_sha256)?;
    manifest.schema_version = CHECKPOINT_MANIFEST_VERSION.into();
    manifest.current.wal_anchor_segment_first_cursor = Some(anchor.segment_first_cursor);
    manifest.current.wal_anchor_segment_header_sha256 = Some(anchor.segment_header_sha256);
    manifest.created_at = Utc::now();
    atomic_json_write(&manifest_path, &manifest)?;
    Ok(manifest)
}

impl PolymarketRuntime {
    pub fn open(config: RuntimeConfig) -> Result<Self, RuntimeError> {
        config.validate()?;
        fs::create_dir_all(&config.root)?;
        let writer_lock = acquire_writer_lock(&config.root.join("writer.lock"))?;
        fs::create_dir_all(config.root.join("wal"))?;
        fs::create_dir_all(config.root.join("checkpoints"))?;
        let verified_wal = SegmentedWal::verify_with_anchors(config.root.join("wal"))?;
        let manifest = load_manifest(&config)?;
        let recovered = recover_projection(
            &config,
            manifest.as_ref(),
            &verified_wal.records,
            &verified_wal.checkpoint_anchors,
        )?;
        let wal = SegmentedWal::open_after_verification(
            config.root.join("wal"),
            &config.scope_id,
            config.wal_segment_bytes,
        )?;
        let writer = SingleWriter::resume(recovered, wal)?;
        let start = verified_wal
            .records
            .len()
            .saturating_sub(config.recent_event_capacity);
        Ok(Self {
            config,
            _writer_lock: writer_lock,
            writer,
            recent_events: verified_wal.records[start..].iter().cloned().collect(),
            last_manifest: manifest,
        })
    }

    /// Forks an isolated generation from a WAL-anchored checkpoint. Historical WAL remains in the
    /// immutable parent generation and is content-addressed through `parent_lineage`; the child
    /// stores only its baseline checkpoint and post-switch WAL.
    pub fn fork_candidate(&mut self, config: RuntimeConfig) -> Result<Self, RuntimeError> {
        config.validate()?;
        if config.scope_id != self.config.scope_id {
            return Err(RuntimeError::CheckpointScopeMismatch);
        }
        if config.root.exists() && config.root.read_dir()?.next().is_some() {
            return Err(RuntimeError::InvalidConfig);
        }
        let deadline = Instant::now() + CANDIDATE_PREPARATION_TIMEOUT;
        let manifest = self.checkpoint()?;
        let projection = self.writer.projection();
        fs::create_dir_all(&config.root)?;
        let writer_lock = acquire_writer_lock(&config.root.join("writer.lock"))?;
        fs::create_dir_all(config.root.join("wal"))?;
        fs::create_dir_all(config.root.join("checkpoints"))?;
        fs::create_dir_all(config.root.join("lineage"))?;
        let parent_manifest_path = PathBuf::from("lineage/parent-checkpoint-manifest.json");
        copy_regular_file_verified(
            self.config.root.join("checkpoint-manifest.json"),
            config.root.join(&parent_manifest_path),
            deadline,
        )?;
        let parent_manifest_sha256 = hex::encode(Sha256::digest(fs::read(
            config.root.join(&parent_manifest_path),
        )?));
        let relative_checkpoint =
            PathBuf::from(format!("checkpoints/{:020}.json", projection.cursor));
        let projection_sha256 =
            write_checkpoint(&config.root.join(&relative_checkpoint), &projection)?;
        if projection_sha256 != manifest.current.projection_sha256 {
            return Err(RuntimeError::PhysicalCopyMismatch);
        }
        let mut wal = SegmentedWal::open(
            config.root.join("wal"),
            &config.scope_id,
            config.wal_segment_bytes,
        )?;
        let anchor = wal.anchor_checkpoint(projection.cursor, &projection_sha256)?;
        let candidate_manifest = CheckpointManifest {
            schema_version: CHECKPOINT_MANIFEST_VERSION.into(),
            scope_id: config.scope_id.clone(),
            current: CheckpointReference {
                path: relative_checkpoint,
                projection_sha256,
                cursor: projection.cursor,
                wal_anchor_segment_first_cursor: Some(anchor.segment_first_cursor),
                wal_anchor_segment_header_sha256: Some(anchor.segment_header_sha256),
            },
            previous: None,
            wal_last_cursor: projection.persisted_cursor,
            created_at: Utc::now(),
            parent_lineage: Some(CheckpointLineageReference {
                manifest_path: parent_manifest_path,
                manifest_sha256: parent_manifest_sha256,
                checkpoint_cursor: manifest.current.cursor,
                checkpoint_sha256: manifest.current.projection_sha256,
                wal_anchor_segment_first_cursor: manifest
                    .current
                    .wal_anchor_segment_first_cursor
                    .ok_or(RuntimeError::NoValidCheckpoint)?,
                wal_anchor_segment_header_sha256: manifest
                    .current
                    .wal_anchor_segment_header_sha256
                    .ok_or(RuntimeError::NoValidCheckpoint)?,
            }),
        };
        atomic_json_write(
            &config.root.join("checkpoint-manifest.json"),
            &candidate_manifest,
        )?;
        File::open(config.root.join("wal"))?.sync_all()?;
        File::open(config.root.join("checkpoints"))?.sync_all()?;
        File::open(config.root.join("lineage"))?.sync_all()?;
        File::open(&config.root)?.sync_all()?;
        ensure_candidate_deadline(deadline)?;
        let writer = SingleWriter::resume(projection.as_ref().clone(), wal)?;
        Ok(Self {
            config,
            _writer_lock: writer_lock,
            writer,
            recent_events: self.recent_events.clone(),
            last_manifest: Some(candidate_manifest),
        })
    }

    pub fn projection(&self) -> Arc<Projection> {
        self.writer.projection()
    }

    pub fn recent_events(&self) -> &VecDeque<PersistedEvent> {
        &self.recent_events
    }

    pub fn apply_raw(
        &mut self,
        raw_payload: Value,
        received_at: DateTime<Utc>,
    ) -> Result<Vec<ApplyOutcome>, RuntimeError> {
        let projection = self.writer.projection();
        let next_cursor = projection.cursor + 1;
        let normalizer = NormalizerConfig::new(
            self.config.scope_id.clone(),
            self.config.config_revision.clone(),
        );
        let verified_book_tick = verified_book_tick(&projection, &raw_payload);
        let mut events = normalize_frame_with_book_tick(
            &normalizer,
            raw_payload,
            received_at,
            next_cursor,
            verified_book_tick,
        )?;
        for event in &mut events {
            let duplicate = projection.recent_event_ids.contains(&event.event_id);
            let closes_durable_gap = match &event.kind {
                EventKind::FullBook { token_id, .. } => {
                    projection.unresolved_gaps.contains(token_id)
                }
                _ => false,
            };
            if duplicate && closes_durable_gap {
                // A quiet venue book may be byte-identical across reconnects. It is still a new
                // recovery barrier after a durable source gap, while ordinary repeated snapshots
                // and incremental events retain their content-addressed idempotency.
                bind_full_book_recovery_identity(event);
            }
        }
        events.retain(|event| !projection.recent_event_ids.contains(&event.event_id));
        for (offset, event) in events.iter_mut().enumerate() {
            event.cursor = next_cursor + u64::try_from(offset).expect("bounded frame offset");
        }
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let outcomes = self.writer.apply_batch(events)?;
        for outcome in &outcomes {
            self.recent_events.push_back(outcome.persisted.clone());
            if self.recent_events.len() > self.config.recent_event_capacity {
                self.recent_events.pop_front();
            }
        }
        Ok(outcomes)
    }

    /// Compares one authoritative HTTP book observation with the live WS projection without
    /// mutating the single writer. REST and WS do not expose a shared venue sequence, so applying
    /// this snapshot followed by queued WS deltas would invent an ordering that the venue did not
    /// provide. An exact match is freshness evidence only; a mismatch is diagnostic evidence and
    /// cannot refresh the book.
    pub fn validate_full_book_observation(
        &self,
        raw_payload: Value,
        received_at: DateTime<Utc>,
    ) -> Result<BookObservationValidation, RuntimeError> {
        let projection = self.writer.projection();
        let next_cursor = projection.cursor + 1;
        let normalizer = NormalizerConfig::new(
            self.config.scope_id.clone(),
            self.config.config_revision.clone(),
        );
        let verified_book_tick = verified_book_tick(&projection, &raw_payload);
        let mut events = normalize_frame_with_book_tick(
            &normalizer,
            raw_payload,
            received_at,
            next_cursor,
            verified_book_tick,
        )?;
        if events.len() != 1 || !bind_full_book_refresh_identity(&mut events[0]) {
            return Err(RuntimeError::Normalize(
                NormalizeError::UnsupportedEventType(
                    "periodic_refresh_requires_one_full_book".into(),
                ),
            ));
        }
        let event = events.pop().expect("one full-book event was checked");
        let EventKind::FullBook {
            token_id,
            bids,
            asks,
            tick_size,
            tick_version,
        } = &event.kind
        else {
            unreachable!("refresh identity is only bound to a full-book event");
        };
        let incoming_bids = bids
            .iter()
            .filter(|level| !level.quantity.is_zero())
            .map(|level| (level.price.clone(), level.quantity))
            .collect::<BTreeMap<_, _>>();
        let incoming_asks = asks
            .iter()
            .filter(|level| !level.quantity.is_zero())
            .map(|level| (level.price.clone(), level.quantity))
            .collect::<BTreeMap<_, _>>();
        let matches_projection = projection.books.get(token_id).is_some_and(|book| {
            book.bids == incoming_bids
                && book.asks == incoming_asks
                && book.tick_size.as_ref() == Some(tick_size)
                && book.tick_version == *tick_version
        });
        Ok(BookObservationValidation {
            token_id: token_id.clone(),
            matches_projection,
            observed_at: event.source_observed_at,
            received_at,
            raw_sha256: event.source.raw_sha256,
        })
    }

    pub fn checkpoint(&mut self) -> Result<CheckpointManifest, RuntimeError> {
        let projection = self.writer.projection();
        let relative_path = PathBuf::from(format!("checkpoints/{:020}.json", projection.cursor));
        let absolute_path = self.config.root.join(&relative_path);
        let projection_sha256 = write_checkpoint(&absolute_path, &projection)?;
        let wal_anchor = self
            .writer
            .durable_log_mut()
            .anchor_checkpoint(projection.cursor, &projection_sha256)?;
        let current = CheckpointReference {
            path: relative_path,
            projection_sha256,
            cursor: projection.cursor,
            wal_anchor_segment_first_cursor: Some(wal_anchor.segment_first_cursor),
            wal_anchor_segment_header_sha256: Some(wal_anchor.segment_header_sha256),
        };
        let previous = self.last_manifest.as_ref().and_then(|manifest| {
            if manifest.current.cursor != current.cursor {
                Some(manifest.current.clone())
            } else {
                manifest.previous.clone()
            }
        });
        let manifest = CheckpointManifest {
            schema_version: CHECKPOINT_MANIFEST_VERSION.into(),
            scope_id: self.config.scope_id.clone(),
            current,
            previous,
            wal_last_cursor: projection.persisted_cursor,
            created_at: Utc::now(),
            parent_lineage: self
                .last_manifest
                .as_ref()
                .and_then(|manifest| manifest.parent_lineage.clone()),
        };
        atomic_json_write(
            &self.config.root.join("checkpoint-manifest.json"),
            &manifest,
        )?;
        self.last_manifest = Some(manifest.clone());
        Ok(manifest)
    }
}

#[cfg(unix)]
impl Drop for PolymarketRuntime {
    fn drop(&mut self) {
        // Closing the descriptor also releases the lease. Explicit unlock makes the handoff
        // boundary visible and lets the replacement writer acquire immediately after drop.
        unsafe {
            libc::flock(self._writer_lock.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

fn acquire_writer_lock(path: &Path) -> Result<File, RuntimeError> {
    if path.exists() && fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(RuntimeError::InsecureWriterLease);
    }
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let file = options.open(path)?;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(RuntimeError::InsecureWriterLease);
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(RuntimeError::InsecureWriterLease);
    }
    #[cfg(unix)]
    {
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::WouldBlock {
                return Err(RuntimeError::WriterAlreadyActive);
            }
            return Err(RuntimeError::Io(error));
        }
    }
    Ok(file)
}

fn verified_book_tick(
    projection: &Projection,
    raw_payload: &Value,
) -> Option<marketcow_core::Price> {
    let raw = raw_payload.as_object()?;
    if raw
        .get("event_type")
        .or_else(|| raw.get("type"))
        .and_then(Value::as_str)
        != Some("book")
        || raw.get("tick_size").is_some_and(|value| !value.is_null())
    {
        return None;
    }
    let token_id = raw
        .get("asset_id")
        .or_else(|| raw.get("token_id"))
        .and_then(Value::as_str)?;
    projection
        .books
        .get(token_id)
        .and_then(|book| book.tick_size.clone())
        .or_else(|| {
            projection.markets.values().find_map(|market| {
                if market
                    .outcomes
                    .iter()
                    .any(|outcome| outcome.token_id == token_id)
                {
                    market
                        .instrument_facts
                        .as_ref()
                        .map(|facts| facts.price_increment.clone())
                } else {
                    None
                }
            })
        })
}

fn copy_regular_file_verified(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    deadline: Instant,
) -> Result<(), RuntimeError> {
    let source = source.as_ref();
    let destination = destination.as_ref();
    ensure_candidate_deadline(deadline)?;
    let mut source_file = File::open(source)?;
    let mut destination_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)?;
    let mut copy_buffer = vec![0_u8; 1024 * 1024];
    loop {
        ensure_candidate_deadline(deadline)?;
        let bytes = source_file.read(&mut copy_buffer)?;
        if bytes == 0 {
            break;
        }
        destination_file.write_all(&copy_buffer[..bytes])?;
    }
    destination_file.sync_all()?;
    if fs::metadata(source)?.len() != fs::metadata(destination)?.len() {
        return Err(RuntimeError::PhysicalCopyMismatch);
    }
    let mut left = File::open(source)?;
    let mut right = File::open(destination)?;
    let mut left_buffer = vec![0_u8; 1024 * 1024];
    let mut right_buffer = vec![0_u8; 1024 * 1024];
    loop {
        ensure_candidate_deadline(deadline)?;
        let left_bytes = left.read(&mut left_buffer)?;
        let right_bytes = right.read(&mut right_buffer)?;
        if left_bytes != right_bytes || left_buffer[..left_bytes] != right_buffer[..right_bytes] {
            return Err(RuntimeError::PhysicalCopyMismatch);
        }
        if left_bytes == 0 {
            return Ok(());
        }
    }
}

fn ensure_candidate_deadline(deadline: Instant) -> Result<(), RuntimeError> {
    if Instant::now() >= deadline {
        Err(RuntimeError::CandidatePreparationTimeout)
    } else {
        Ok(())
    }
}

fn load_manifest(config: &RuntimeConfig) -> Result<Option<CheckpointManifest>, RuntimeError> {
    let path = config.root.join("checkpoint-manifest.json");
    if !path.exists() {
        return Ok(None);
    }
    let manifest: CheckpointManifest = serde_json::from_slice(&fs::read(path)?)?;
    if !matches!(
        manifest.schema_version.as_str(),
        CHECKPOINT_MANIFEST_V1 | CHECKPOINT_MANIFEST_VERSION
    ) || manifest.scope_id != config.scope_id
    {
        return Err(RuntimeError::CheckpointScopeMismatch);
    }
    validate_parent_lineage(config, &manifest)?;
    Ok(Some(manifest))
}

fn validate_parent_lineage(
    config: &RuntimeConfig,
    manifest: &CheckpointManifest,
) -> Result<(), RuntimeError> {
    let Some(lineage) = &manifest.parent_lineage else {
        return Ok(());
    };
    if manifest.schema_version != CHECKPOINT_MANIFEST_VERSION
        || lineage.manifest_path.is_absolute()
        || lineage
            .manifest_path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(RuntimeError::CheckpointPathEscape);
    }
    let bytes = fs::read(config.root.join(&lineage.manifest_path))?;
    if hex::encode(Sha256::digest(&bytes)) != lineage.manifest_sha256 {
        return Err(RuntimeError::PhysicalCopyMismatch);
    }
    let parent: CheckpointManifest = serde_json::from_slice(&bytes)?;
    if parent.scope_id != manifest.scope_id
        || parent.current.cursor != lineage.checkpoint_cursor
        || parent.current.projection_sha256 != lineage.checkpoint_sha256
        || parent.current.wal_anchor_segment_first_cursor
            != Some(lineage.wal_anchor_segment_first_cursor)
        || parent.current.wal_anchor_segment_header_sha256.as_deref()
            != Some(lineage.wal_anchor_segment_header_sha256.as_str())
        || parent.current.cursor > manifest.current.cursor
    {
        return Err(RuntimeError::CheckpointScopeMismatch);
    }
    Ok(())
}

fn recover_projection(
    config: &RuntimeConfig,
    manifest: Option<&CheckpointManifest>,
    records: &[PersistedEvent],
    wal_anchors: &[WalCheckpointAnchor],
) -> Result<Projection, RuntimeError> {
    let last_wal_cursor = records.last().map_or(0, |record| record.event.cursor);
    if let Some(manifest) = manifest {
        for candidate in std::iter::once(&manifest.current).chain(manifest.previous.iter()) {
            let anchored = manifest.schema_version == CHECKPOINT_MANIFEST_VERSION
                && checkpoint_anchor_matches(candidate, wal_anchors);
            if candidate.cursor > last_wal_cursor && !anchored {
                continue;
            }
            if let Ok(checkpoint) = load_checkpoint_candidate(config, candidate)
                && checkpoint_boundary_matches(
                    config,
                    manifest.schema_version.as_str(),
                    candidate,
                    &checkpoint,
                    records,
                    wal_anchors,
                )?
            {
                return Ok(replay_after_checkpoint(checkpoint, records)?);
            }
        }
        // Keeping the full WAL permits a safe origin replay when both retained checkpoints fail.
        let origin_replay_available = records.first().map(|record| record.event.cursor) == Some(1)
            || records.is_empty() && manifest.current.cursor == 0;
        if !origin_replay_available {
            return Err(RuntimeError::NoValidCheckpoint);
        }
    }
    Ok(replay_after_checkpoint(
        Projection::bootstrap(config.scope_id.clone()),
        records,
    )?)
}

fn load_checkpoint_candidate(
    config: &RuntimeConfig,
    reference: &CheckpointReference,
) -> Result<Projection, RuntimeError> {
    if reference.path.is_absolute()
        || reference
            .path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(RuntimeError::CheckpointPathEscape);
    }
    let checkpoint = read_checkpoint(
        &config.root.join(&reference.path),
        &reference.projection_sha256,
    )?;
    if checkpoint.scope_id != config.scope_id || checkpoint.cursor != reference.cursor {
        return Err(RuntimeError::CheckpointScopeMismatch);
    }
    Ok(checkpoint)
}

fn checkpoint_boundary_matches(
    config: &RuntimeConfig,
    manifest_schema_version: &str,
    reference: &CheckpointReference,
    checkpoint: &Projection,
    records: &[PersistedEvent],
    wal_anchors: &[WalCheckpointAnchor],
) -> Result<bool, RuntimeError> {
    if manifest_schema_version == CHECKPOINT_MANIFEST_VERSION {
        return Ok(checkpoint_anchor_matches(reference, wal_anchors));
    }
    let boundary = records.partition_point(|record| record.event.cursor <= checkpoint.cursor);
    if checkpoint.cursor > 0
        && records
            .get(boundary.saturating_sub(1))
            .map(|record| record.event.cursor)
            != Some(checkpoint.cursor)
    {
        return Err(RuntimeError::CheckpointBeyondWal);
    }
    let rebuilt = replay_after_checkpoint(
        Projection::bootstrap(config.scope_id.clone()),
        &records[..boundary],
    )?;
    Ok(rebuilt.hash() == checkpoint.hash())
}

fn checkpoint_anchor_matches(
    reference: &CheckpointReference,
    wal_anchors: &[WalCheckpointAnchor],
) -> bool {
    let Some(segment_first_cursor) = reference.wal_anchor_segment_first_cursor else {
        return false;
    };
    let Some(segment_header_sha256) = &reference.wal_anchor_segment_header_sha256 else {
        return false;
    };
    wal_anchors.iter().any(|anchor| {
        anchor.checkpoint_cursor == reference.cursor
            && anchor.checkpoint_sha256 == reference.projection_sha256
            && anchor.segment_first_cursor == segment_first_cursor
            && anchor.segment_header_sha256 == *segment_header_sha256
    })
}

fn atomic_json_write(path: &Path, value: &impl Serialize) -> Result<(), RuntimeError> {
    let bytes = serde_json::to_vec(value)?;
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(path.parent().ok_or(RuntimeError::InvalidConfig)?)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn config(root: &Path) -> RuntimeConfig {
        RuntimeConfig {
            root: root.into(),
            scope_id: "scope".into(),
            config_revision: "config-v1".into(),
            wal_segment_bytes: 1_024,
            recent_event_capacity: 2,
        }
    }

    fn at(seconds: u32) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(&format!("2026-08-03T04:00:{seconds:02}Z"))
            .unwrap()
            .with_timezone(&Utc)
    }

    fn snapshot(token: &str, cursor_seconds: u32) -> Value {
        serde_json::json!({
            "event_type":"book", "asset_id":token,
            "timestamp":format!("2026-08-03T04:00:{cursor_seconds:02}Z"),
            "tick_size":"0.01", "bids":[{"price":"0.40","size":"10"}],
            "asks":[{"price":"0.60","size":"11"}]
        })
    }

    #[test]
    fn restart_recovers_checkpoint_then_only_post_boundary_records() {
        let dir = tempdir().unwrap();
        let mut runtime = PolymarketRuntime::open(config(dir.path())).unwrap();
        runtime.apply_raw(snapshot("yes", 0), at(0)).unwrap();
        let manifest = runtime.checkpoint().unwrap();
        assert_eq!(manifest.schema_version, CHECKPOINT_MANIFEST_VERSION);
        assert_eq!(manifest.current.wal_anchor_segment_first_cursor, Some(2));
        assert!(
            manifest
                .current
                .wal_anchor_segment_header_sha256
                .as_ref()
                .is_some_and(|hash| hash.len() == 64)
        );
        assert_eq!(runtime.projection().cursor, 1);
        runtime
            .apply_raw(
                serde_json::json!({
                    "event_type":"price_change", "timestamp":"2026-08-03T04:00:01Z",
                    "price_changes":[{"asset_id":"yes","side":"BUY","price":"0.41","size":"2"}]
                }),
                at(1),
            )
            .unwrap();
        let expected = runtime.projection().hash();
        drop(runtime);

        let recovered = PolymarketRuntime::open(config(dir.path())).unwrap();
        assert_eq!(recovered.projection().cursor, 2);
        assert_eq!(recovered.projection().hash(), expected);
        assert_eq!(recovered.recent_events().len(), 2);
    }

    #[test]
    fn repeated_upstream_snapshot_is_an_idempotent_noop() {
        let dir = tempdir().unwrap();
        let mut runtime = PolymarketRuntime::open(config(dir.path())).unwrap();
        let first = runtime.apply_raw(snapshot("yes", 0), at(0)).unwrap();
        assert_eq!(first.len(), 1);
        let expected_hash = runtime.projection().hash();

        let duplicate = runtime.apply_raw(snapshot("yes", 0), at(1)).unwrap();

        assert!(duplicate.is_empty());
        assert_eq!(runtime.projection().cursor, 1);
        assert_eq!(runtime.projection().hash(), expected_hash);
        assert_eq!(runtime.recent_events().len(), 1);
    }

    #[test]
    fn writer_lease_fences_overlapping_process_lifetimes() {
        let dir = tempdir().unwrap();
        let runtime = PolymarketRuntime::open(config(dir.path())).unwrap();

        assert!(matches!(
            PolymarketRuntime::open(config(dir.path())),
            Err(RuntimeError::WriterAlreadyActive)
        ));

        drop(runtime);
        assert!(PolymarketRuntime::open(config(dir.path())).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn writer_lease_rejects_symlink_substitution() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("target");
        fs::write(&target, b"not-a-lock").unwrap();
        std::os::unix::fs::symlink(&target, dir.path().join("writer.lock")).unwrap();

        assert!(matches!(
            PolymarketRuntime::open(config(dir.path())),
            Err(RuntimeError::InsecureWriterLease)
        ));
    }

    #[test]
    fn authoritative_observation_validates_without_mutating_the_ws_projection() {
        let dir = tempdir().unwrap();
        let mut runtime = PolymarketRuntime::open(config(dir.path())).unwrap();
        runtime.apply_raw(snapshot("yes", 0), at(0)).unwrap();
        let expected_hash = runtime.projection().hash();
        let expected_cursor = runtime.projection().cursor;
        let expected_recent_events = runtime.recent_events().len();

        let validation = runtime
            .validate_full_book_observation(snapshot("yes", 0), at(10))
            .unwrap();

        assert!(validation.matches_projection);
        assert_eq!(validation.token_id, "yes");
        assert_eq!(validation.received_at, at(10));
        assert_eq!(runtime.projection().cursor, expected_cursor);
        assert_eq!(runtime.projection().hash(), expected_hash);
        assert_eq!(runtime.recent_events().len(), expected_recent_events);
    }

    #[test]
    fn mismatched_authoritative_observation_is_evidence_without_mutation() {
        let dir = tempdir().unwrap();
        let mut runtime = PolymarketRuntime::open(config(dir.path())).unwrap();
        runtime.apply_raw(snapshot("yes", 0), at(0)).unwrap();
        let expected_hash = runtime.projection().hash();
        let expected_cursor = runtime.projection().cursor;
        let mut mismatched = snapshot("yes", 1);
        mismatched["bids"][0]["size"] = serde_json::Value::String("12".into());

        let validation = runtime
            .validate_full_book_observation(mismatched, at(10))
            .unwrap();

        assert!(!validation.matches_projection);
        assert_eq!(runtime.projection().cursor, expected_cursor);
        assert_eq!(runtime.projection().hash(), expected_hash);
        assert_eq!(runtime.recent_events().len(), 1);
    }

    #[test]
    fn ws_deltas_remain_the_only_causal_sequence_around_rest_validation() {
        let dir = tempdir().unwrap();
        let mut runtime = PolymarketRuntime::open(config(dir.path())).unwrap();
        runtime.apply_raw(snapshot("yes", 0), at(0)).unwrap();
        let cursor_before_validation = runtime.projection().cursor;

        let validation = runtime
            .validate_full_book_observation(snapshot("yes", 0), at(10))
            .unwrap();
        assert!(validation.matches_projection);
        assert_eq!(runtime.projection().cursor, cursor_before_validation);

        let first_delta = runtime
            .apply_raw(
                serde_json::json!({
                    "event_type":"price_change", "timestamp":"2026-08-03T04:00:11Z",
                    "price_changes":[{"asset_id":"yes","side":"BUY","price":"0.41","size":"2"}]
                }),
                at(11),
            )
            .unwrap();
        let second_delta = runtime
            .apply_raw(
                serde_json::json!({
                    "event_type":"price_change", "timestamp":"2026-08-03T04:00:12Z",
                    "price_changes":[{"asset_id":"yes","side":"BUY","price":"0.42","size":"3"}]
                }),
                at(12),
            )
            .unwrap();

        assert_eq!(
            first_delta[0].persisted.event.cursor,
            cursor_before_validation + 1
        );
        assert_eq!(
            second_delta[0].persisted.event.cursor,
            cursor_before_validation + 2
        );
        assert_eq!(runtime.projection().cursor, cursor_before_validation + 2);
        assert!(
            runtime.projection().books["yes"]
                .bids
                .contains_key(&marketcow_core::Price::parse("0.42").unwrap())
        );
    }

    #[test]
    fn identical_upstream_snapshot_closes_a_durable_connection_gap() {
        let dir = tempdir().unwrap();
        let mut runtime = PolymarketRuntime::open(config(dir.path())).unwrap();
        let first = runtime.apply_raw(snapshot("yes", 0), at(0)).unwrap();
        let first_event_id = first[0].persisted.event.event_id.clone();
        let first_raw_sha256 = first[0].persisted.event.source.raw_sha256.clone();

        runtime
            .apply_raw(
                serde_json::json!({
                    "event_type":"source_gap", "asset_id":"yes",
                    "reason":"upstream_connection_boundary", "timestamp":"2026-08-03T04:00:01Z"
                }),
                at(1),
            )
            .unwrap();
        assert!(!runtime.projection().ready);
        assert!(runtime.projection().unresolved_gaps.contains("yes"));

        let recovered = runtime.apply_raw(snapshot("yes", 0), at(2)).unwrap();

        assert_eq!(recovered.len(), 1);
        assert_ne!(recovered[0].persisted.event.event_id, first_event_id);
        assert_eq!(
            recovered[0].persisted.event.source.raw_sha256,
            first_raw_sha256
        );
        assert_eq!(runtime.projection().cursor, 3);
        assert!(runtime.projection().ready);
        assert!(!runtime.projection().unresolved_gaps.contains("yes"));
    }

    #[test]
    fn book_without_tick_inherits_only_the_verified_projection_tick() {
        let dir = tempdir().unwrap();
        let mut runtime = PolymarketRuntime::open(config(dir.path())).unwrap();
        runtime.apply_raw(snapshot("yes", 0), at(0)).unwrap();

        let outcomes = runtime
            .apply_raw(
                serde_json::json!({
                    "event_type":"book", "asset_id":"yes",
                    "timestamp":"2026-08-03T04:00:01Z",
                    "bids":[{"price":"0.41","size":"12"}],
                    "asks":[{"price":"0.61","size":"13"}]
                }),
                at(1),
            )
            .unwrap();

        assert_eq!(outcomes.len(), 1);
        assert_eq!(
            runtime.projection().books["yes"]
                .tick_size
                .as_ref()
                .unwrap()
                .0
                .to_string(),
            "0.01"
        );
        assert_eq!(runtime.projection().cursor, 2);
        assert!(
            runtime.recent_events()[1]
                .event
                .raw_payload
                .get("tick_size")
                .is_none()
        );
    }

    #[test]
    fn candidate_fork_uses_compact_lineage_and_recovers_post_switch_wal() {
        let source = tempdir().unwrap();
        let candidate = tempdir().unwrap();
        let mut runtime = PolymarketRuntime::open(config(source.path())).unwrap();
        for index in 0..3 {
            runtime
                .apply_raw(snapshot(&format!("token-{index}"), index), at(index))
                .unwrap();
            runtime.checkpoint().unwrap();
        }
        assert_eq!(
            fs::read_dir(source.path().join("checkpoints"))
                .unwrap()
                .count(),
            3
        );
        let expected = runtime.projection().hash();

        let mut fork = runtime.fork_candidate(config(candidate.path())).unwrap();

        assert_eq!(fork.projection().hash(), expected);
        assert_eq!(
            fs::read_dir(candidate.path().join("checkpoints"))
                .unwrap()
                .count(),
            1
        );
        let candidate_manifest: CheckpointManifest = serde_json::from_slice(
            &fs::read(candidate.path().join("checkpoint-manifest.json")).unwrap(),
        )
        .unwrap();
        let parent_manifest: CheckpointManifest = serde_json::from_slice(
            &fs::read(source.path().join("checkpoint-manifest.json")).unwrap(),
        )
        .unwrap();
        let lineage = candidate_manifest.parent_lineage.as_ref().unwrap();
        assert_eq!(lineage.checkpoint_cursor, 3);
        assert_eq!(
            lineage.checkpoint_sha256,
            parent_manifest.current.projection_sha256
        );
        assert_eq!(
            candidate_manifest.current.projection_sha256,
            lineage.checkpoint_sha256
        );
        assert_eq!(
            hex::encode(Sha256::digest(
                fs::read(candidate.path().join(&lineage.manifest_path)).unwrap()
            )),
            lineage.manifest_sha256
        );
        assert_eq!(
            SegmentedWal::verify(candidate.path().join("wal"))
                .unwrap()
                .len(),
            0
        );
        fork.apply_raw(snapshot("candidate-only", 3), at(3))
            .unwrap();
        assert_eq!(fork.projection().cursor, 4);
        assert_eq!(runtime.projection().cursor, 3);
        let candidate_records = SegmentedWal::verify(candidate.path().join("wal")).unwrap();
        assert_eq!(candidate_records.len(), 1);
        assert_eq!(candidate_records[0].event.cursor, 4);
        drop(fork);
        let recovered = PolymarketRuntime::open(config(candidate.path())).unwrap();
        assert_eq!(recovered.projection().cursor, 4);
        drop(recovered);

        fs::write(
            candidate.path().join(&lineage.manifest_path),
            b"tampered lineage",
        )
        .unwrap();
        assert!(matches!(
            PolymarketRuntime::open(config(candidate.path())),
            Err(RuntimeError::PhysicalCopyMismatch)
        ));
    }

    #[test]
    fn candidate_copy_fails_closed_after_deadline() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("source.wal");
        let destination = directory.path().join("candidate.wal");
        fs::write(&source, b"authoritative").unwrap();

        let error = copy_regular_file_verified(&source, &destination, Instant::now()).unwrap_err();

        assert!(matches!(error, RuntimeError::CandidatePreparationTimeout));
        assert!(!destination.exists());
    }

    #[test]
    fn corrupt_current_checkpoint_falls_back_to_previous_and_wal() {
        let dir = tempdir().unwrap();
        let mut runtime = PolymarketRuntime::open(config(dir.path())).unwrap();
        runtime.apply_raw(snapshot("yes", 0), at(0)).unwrap();
        let first = runtime.checkpoint().unwrap();
        runtime.apply_raw(snapshot("no", 1), at(1)).unwrap();
        let second = runtime.checkpoint().unwrap();
        assert_eq!(second.previous, Some(first.current));
        fs::write(dir.path().join(&second.current.path), b"corrupt").unwrap();
        let expected = runtime.projection().hash();
        drop(runtime);

        let recovered = PolymarketRuntime::open(config(dir.path())).unwrap();
        assert_eq!(recovered.projection().hash(), expected);
        assert_eq!(recovered.projection().cursor, 2);
    }

    #[test]
    fn self_consistent_but_event_divergent_checkpoint_is_rejected() {
        let dir = tempdir().unwrap();
        let mut runtime = PolymarketRuntime::open(config(dir.path())).unwrap();
        runtime.apply_raw(snapshot("yes", 0), at(0)).unwrap();
        let manifest = runtime.checkpoint().unwrap();
        let mut forged = (*runtime.projection()).clone();
        forged.books.clear();
        forged.ready = false;
        forged.fail_closed_reason = Some("forged".into());
        let forged_hash =
            write_checkpoint(&dir.path().join(&manifest.current.path), &forged).unwrap();
        let mut forged_manifest = manifest;
        forged_manifest.current.projection_sha256 = forged_hash;
        atomic_json_write(
            &dir.path().join("checkpoint-manifest.json"),
            &forged_manifest,
        )
        .unwrap();
        drop(runtime);

        let recovered = PolymarketRuntime::open(config(dir.path())).unwrap();
        assert!(recovered.projection().ready);
        assert!(recovered.projection().books.contains_key("yes"));
    }

    #[test]
    fn stopped_verified_legacy_checkpoint_is_anchored_without_consuming_cursor() {
        let dir = tempdir().unwrap();
        let config = config(dir.path());
        let mut runtime = PolymarketRuntime::open(config.clone()).unwrap();
        runtime.apply_raw(snapshot("yes", 0), at(0)).unwrap();
        let mut manifest = runtime.checkpoint().unwrap();
        drop(runtime);

        let anchor_path = dir
            .path()
            .join("wal")
            .join(format!("scope-{:020}.wal", manifest.current.cursor + 1));
        fs::remove_file(anchor_path).unwrap();
        manifest.schema_version = CHECKPOINT_MANIFEST_V1.into();
        manifest.current.wal_anchor_segment_first_cursor = None;
        manifest.current.wal_anchor_segment_header_sha256 = None;
        atomic_json_write(&dir.path().join("checkpoint-manifest.json"), &manifest).unwrap();
        let manifest_bytes = fs::read(dir.path().join("checkpoint-manifest.json")).unwrap();
        let expected_manifest_sha256 = hex::encode(Sha256::digest(&manifest_bytes));

        let upgraded =
            anchor_verified_legacy_checkpoint(config.clone(), &expected_manifest_sha256).unwrap();

        assert_eq!(upgraded.schema_version, CHECKPOINT_MANIFEST_VERSION);
        assert_eq!(upgraded.current.cursor, 1);
        assert_eq!(upgraded.current.wal_anchor_segment_first_cursor, Some(2));
        let recovered = PolymarketRuntime::open(config).unwrap();
        assert_eq!(recovered.projection().cursor, 1);
        assert!(recovered.projection().ready);
    }

    #[test]
    fn legacy_checkpoint_anchor_rejects_a_checkpoint_behind_the_wal_tail() {
        let dir = tempdir().unwrap();
        let config = config(dir.path());
        let mut runtime = PolymarketRuntime::open(config.clone()).unwrap();
        runtime.apply_raw(snapshot("yes", 0), at(0)).unwrap();
        let mut manifest = runtime.checkpoint().unwrap();
        runtime.apply_raw(snapshot("no", 1), at(1)).unwrap();
        drop(runtime);

        manifest.schema_version = CHECKPOINT_MANIFEST_V1.into();
        manifest.current.wal_anchor_segment_first_cursor = None;
        manifest.current.wal_anchor_segment_header_sha256 = None;
        atomic_json_write(&dir.path().join("checkpoint-manifest.json"), &manifest).unwrap();
        let manifest_bytes = fs::read(dir.path().join("checkpoint-manifest.json")).unwrap();
        let expected_manifest_sha256 = hex::encode(Sha256::digest(&manifest_bytes));

        assert!(matches!(
            anchor_verified_legacy_checkpoint(config, &expected_manifest_sha256),
            Err(RuntimeError::CheckpointBeyondWal)
        ));
    }

    #[test]
    fn recent_event_exposure_is_strictly_bounded() {
        let dir = tempdir().unwrap();
        let mut runtime = PolymarketRuntime::open(config(dir.path())).unwrap();
        for index in 0..3 {
            runtime
                .apply_raw(snapshot(&format!("token-{index}"), index), at(index))
                .unwrap();
        }
        assert_eq!(runtime.projection().cursor, 3);
        assert_eq!(runtime.recent_events().len(), 2);
        assert_eq!(runtime.recent_events()[0].event.cursor, 2);
    }
}
