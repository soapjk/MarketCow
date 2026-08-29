//! Polymarket runtime composition: normalization, the authoritative single writer, WAL recovery,
//! bounded replay exposure, and atomic checkpoint manifests.

use chrono::{DateTime, Utc};
use marketcow_core::{
    ApplyOutcome, EventKind, PersistedEvent, Projection, SegmentedWal, SingleWriter,
    read_checkpoint, replay_after_checkpoint, write_checkpoint,
};
use marketcow_polymarket::{
    NormalizeError, NormalizerConfig, bind_full_book_recovery_identity,
    normalize_frame_with_book_tick,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;

pub const CHECKPOINT_MANIFEST_VERSION: &str = "marketcow.polymarket.checkpoint-manifest.v1";
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointManifest {
    pub schema_version: String,
    pub scope_id: String,
    pub current: CheckpointReference,
    pub previous: Option<CheckpointReference>,
    pub wal_last_cursor: u64,
    pub created_at: DateTime<Utc>,
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
    writer: SingleWriter<SegmentedWal>,
    recent_events: VecDeque<PersistedEvent>,
    last_manifest: Option<CheckpointManifest>,
}

impl PolymarketRuntime {
    pub fn open(config: RuntimeConfig) -> Result<Self, RuntimeError> {
        config.validate()?;
        fs::create_dir_all(config.root.join("wal"))?;
        fs::create_dir_all(config.root.join("checkpoints"))?;
        let records = SegmentedWal::verify(config.root.join("wal"))?;
        let manifest = load_manifest(&config)?;
        let recovered = recover_projection(&config, manifest.as_ref(), &records)?;
        let wal = SegmentedWal::open(
            config.root.join("wal"),
            &config.scope_id,
            config.wal_segment_bytes,
        )?;
        let writer = SingleWriter::resume(recovered, wal)?;
        let start = records.len().saturating_sub(config.recent_event_capacity);
        Ok(Self {
            config,
            writer,
            recent_events: records[start..].iter().cloned().collect(),
            last_manifest: manifest,
        })
    }

    /// Copies the verified active WAL/checkpoint chain into an isolated candidate root. The copy
    /// is deliberately physical: candidate appends must never mutate a hard-linked active WAL.
    /// Callers stop ingress before invoking this method, then fully validate the fork before swap.
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
        fs::create_dir_all(config.root.join("wal"))?;
        fs::create_dir_all(config.root.join("checkpoints"))?;
        copy_regular_files_verified(
            &self.config.root.join("wal"),
            &config.root.join("wal"),
            deadline,
        )?;
        for reference in std::iter::once(&manifest.current).chain(manifest.previous.iter()) {
            copy_regular_file_verified(
                self.config.root.join(&reference.path),
                config.root.join(&reference.path),
                deadline,
            )?;
        }
        copy_regular_file_verified(
            self.config.root.join("checkpoint-manifest.json"),
            config.root.join("checkpoint-manifest.json"),
            deadline,
        )?;
        File::open(config.root.join("wal"))?.sync_all()?;
        File::open(config.root.join("checkpoints"))?.sync_all()?;
        File::open(&config.root)?.sync_all()?;
        let copied_checkpoint = load_checkpoint_candidate(&config, &manifest.current)?;
        if copied_checkpoint.hash() != projection.hash() {
            return Err(RuntimeError::PhysicalCopyMismatch);
        }
        let wal = SegmentedWal::open_verified_copy(
            config.root.join("wal"),
            &config.scope_id,
            config.wal_segment_bytes,
        )?;
        let writer = SingleWriter::resume(projection.as_ref().clone(), wal)?;
        Ok(Self {
            config,
            writer,
            recent_events: self.recent_events.clone(),
            last_manifest: Some(manifest),
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

    pub fn checkpoint(&mut self) -> Result<CheckpointManifest, RuntimeError> {
        let projection = self.writer.projection();
        let relative_path = PathBuf::from(format!("checkpoints/{:020}.json", projection.cursor));
        let absolute_path = self.config.root.join(&relative_path);
        let projection_sha256 = write_checkpoint(&absolute_path, &projection)?;
        let current = CheckpointReference {
            path: relative_path,
            projection_sha256,
            cursor: projection.cursor,
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
        };
        atomic_json_write(
            &self.config.root.join("checkpoint-manifest.json"),
            &manifest,
        )?;
        self.last_manifest = Some(manifest.clone());
        Ok(manifest)
    }
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

fn copy_regular_files_verified(
    source: &Path,
    destination: &Path,
    deadline: Instant,
) -> Result<(), RuntimeError> {
    for entry in fs::read_dir(source)? {
        ensure_candidate_deadline(deadline)?;
        let entry = entry?;
        let metadata = entry.file_type()?;
        if !metadata.is_file() || metadata.is_symlink() {
            return Err(RuntimeError::InvalidConfig);
        }
        copy_regular_file_verified(entry.path(), destination.join(entry.file_name()), deadline)?;
    }
    Ok(())
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
    if manifest.schema_version != CHECKPOINT_MANIFEST_VERSION
        || manifest.scope_id != config.scope_id
    {
        return Err(RuntimeError::CheckpointScopeMismatch);
    }
    Ok(Some(manifest))
}

fn recover_projection(
    config: &RuntimeConfig,
    manifest: Option<&CheckpointManifest>,
    records: &[PersistedEvent],
) -> Result<Projection, RuntimeError> {
    let last_wal_cursor = records.last().map_or(0, |record| record.event.cursor);
    if let Some(manifest) = manifest {
        for candidate in std::iter::once(&manifest.current).chain(manifest.previous.iter()) {
            if candidate.cursor > last_wal_cursor {
                continue;
            }
            if let Ok(checkpoint) = load_checkpoint_candidate(config, candidate)
                && checkpoint_boundary_matches(config, &checkpoint, records)?
            {
                return Ok(replay_after_checkpoint(checkpoint, records)?);
            }
        }
        // Keeping the full WAL permits a safe origin replay when both retained checkpoints fail.
        if records.first().map(|record| record.event.cursor) != Some(1) && !records.is_empty() {
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
    checkpoint: &Projection,
    records: &[PersistedEvent],
) -> Result<bool, RuntimeError> {
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
        runtime.checkpoint().unwrap();
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
    fn candidate_fork_reuses_verified_projection_and_retains_only_recovery_checkpoints() {
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
            2
        );
        fork.apply_raw(snapshot("candidate-only", 3), at(3))
            .unwrap();
        assert_eq!(fork.projection().cursor, 4);
        assert_eq!(runtime.projection().cursor, 3);
        drop(fork);
        let recovered = PolymarketRuntime::open(config(candidate.path())).unwrap();
        assert_eq!(recovered.projection().cursor, 4);
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
