//! Polymarket runtime composition: normalization, the authoritative single writer, WAL recovery,
//! bounded replay exposure, and atomic checkpoint manifests.

use chrono::{DateTime, Utc};
use marketcow_core::{
    ApplyOutcome, PersistedEvent, Projection, SegmentedWal, SingleWriter, read_checkpoint,
    replay_after_checkpoint, write_checkpoint,
};
use marketcow_polymarket::{NormalizeError, NormalizerConfig, normalize_frame};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;

pub const CHECKPOINT_MANIFEST_VERSION: &str = "marketcow.polymarket.checkpoint-manifest.v1";

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
    recent_events: Vec<PersistedEvent>,
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
            recent_events: records[start..].to_vec(),
            last_manifest: manifest,
        })
    }

    pub fn projection(&self) -> Arc<Projection> {
        self.writer.projection()
    }

    pub fn recent_events(&self) -> &[PersistedEvent] {
        &self.recent_events
    }

    pub fn apply_raw(
        &mut self,
        raw_payload: Value,
        received_at: DateTime<Utc>,
    ) -> Result<Vec<ApplyOutcome>, RuntimeError> {
        let next_cursor = self.writer.projection().cursor + 1;
        let normalizer = NormalizerConfig::new(
            self.config.scope_id.clone(),
            self.config.config_revision.clone(),
        );
        let mut events = normalize_frame(&normalizer, raw_payload, received_at, next_cursor)?;
        let projection = self.writer.projection();
        events.retain(|event| !projection.recent_event_ids.contains(&event.event_id));
        for (offset, event) in events.iter_mut().enumerate() {
            event.cursor = next_cursor + u64::try_from(offset).expect("bounded frame offset");
        }
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let outcomes = self.writer.apply_batch(events)?;
        for outcome in &outcomes {
            self.recent_events.push(outcome.persisted.clone());
            if self.recent_events.len() > self.config.recent_event_capacity {
                self.recent_events.remove(0);
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
        let previous = self
            .last_manifest
            .as_ref()
            .map(|manifest| manifest.current.clone())
            .filter(|reference| reference.cursor != current.cursor);
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
