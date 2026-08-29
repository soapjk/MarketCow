use super::{NormalizedProviderEvent, RealtimeError, SequencedReplay, StreamEvent};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    time::Instant,
};
use thiserror::Error;

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

pub const REALTIME_WAL_VERSION: &str = "marketcow.realtime.wal.v1";
pub const REALTIME_WAL_BATCH_VERSION: &str = "marketcow.realtime.wal-batch.v1";
pub const REALTIME_WAL_SEGMENT_VERSION: &str = "marketcow.realtime.wal-segment.v1";
pub const REALTIME_CHECKPOINT_VERSION: &str = "marketcow.realtime.checkpoint.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedRealtimeEvent {
    pub schema_version: String,
    pub wal_cursor: u64,
    pub public_sequence: Option<u64>,
    pub applied: bool,
    pub fail_closed_reason: Option<String>,
    pub event: NormalizedProviderEvent,
    pub previous_record_sha256: Option<String>,
    pub recorded_at: DateTime<Utc>,
    pub record_sha256: String,
    pub crc32c: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableApplyOutcome {
    pub persisted: PersistedRealtimeEvent,
    pub published: Option<StreamEvent>,
    pub persistence_latency_us: u64,
    pub publication_latency_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeCheckpoint {
    pub schema_version: String,
    pub stream_id: String,
    pub config_revision: String,
    pub wal_cursor: u64,
    pub public_sequence: u64,
    pub last_record_sha256: Option<String>,
    pub replay_capacity: usize,
    pub replay: Vec<StreamEvent>,
    pub created_at: DateTime<Utc>,
    pub state_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentHeader {
    schema_version: String,
    stream_id: String,
    config_revision: String,
    segment_id: u64,
    first_wal_cursor: u64,
    previous_segment_sha256: Option<String>,
    created_at: DateTime<Utc>,
    header_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalBatchRecord {
    schema_version: String,
    first_wal_cursor: u64,
    last_wal_cursor: u64,
    records: Vec<PersistedRealtimeEvent>,
    batch_sha256: String,
    crc32c: u32,
}

#[derive(Debug, Clone)]
struct PendingDecision {
    event: NormalizedProviderEvent,
    public_sequence: Option<u64>,
    applied: bool,
    fail_closed_reason: Option<String>,
}

#[derive(Serialize)]
struct RecordIntegrity<'a> {
    schema_version: &'a str,
    wal_cursor: u64,
    public_sequence: Option<u64>,
    applied: bool,
    fail_closed_reason: &'a Option<String>,
    event: &'a NormalizedProviderEvent,
    previous_record_sha256: &'a Option<String>,
    recorded_at: &'a DateTime<Utc>,
}

#[derive(Serialize)]
struct HeaderIntegrity<'a> {
    schema_version: &'a str,
    stream_id: &'a str,
    config_revision: &'a str,
    segment_id: u64,
    first_wal_cursor: u64,
    previous_segment_sha256: &'a Option<String>,
    created_at: &'a DateTime<Utc>,
}

#[derive(Serialize)]
struct BatchIntegrity<'a> {
    schema_version: &'a str,
    first_wal_cursor: u64,
    last_wal_cursor: u64,
    records: &'a [PersistedRealtimeEvent],
}

#[derive(Serialize)]
struct CheckpointIntegrity<'a> {
    schema_version: &'a str,
    stream_id: &'a str,
    config_revision: &'a str,
    wal_cursor: u64,
    public_sequence: u64,
    last_record_sha256: &'a Option<String>,
    replay_capacity: usize,
    replay: &'a [StreamEvent],
    created_at: &'a DateTime<Utc>,
}

#[derive(Debug)]
struct VerifiedWal {
    records: Vec<PersistedRealtimeEvent>,
    paths: Vec<PathBuf>,
    last_segment_id: u64,
}

#[derive(Debug)]
pub struct SegmentedRealtimeWal {
    root: PathBuf,
    stream_id: String,
    config_revision: String,
    max_segment_bytes: u64,
    _writer_lock: File,
    current_path: Option<PathBuf>,
    current_file: Option<File>,
    current_bytes: u64,
    current_segment_id: u64,
    records: Vec<PersistedRealtimeEvent>,
    applied_event_ids: BTreeSet<String>,
    poisoned: bool,
    #[cfg(test)]
    fail_next_append: bool,
}

impl SegmentedRealtimeWal {
    pub fn open(
        root: impl AsRef<Path>,
        stream_id: &str,
        config_revision: &str,
        max_segment_bytes: u64,
    ) -> Result<Self, DurabilityError> {
        let root = root.as_ref();
        validate_config(root, stream_id, config_revision, max_segment_bytes)?;
        create_private_directory(root)?;
        let writer_lock = acquire_writer_lock(&root.join("writer.lock"))?;
        let verified = verify_wal(root, Some((stream_id, config_revision)))?;
        let (current_path, current_file, current_bytes) = match verified.paths.last() {
            Some(path) => {
                let mut options = OpenOptions::new();
                options.append(true);
                #[cfg(unix)]
                options.custom_flags(libc::O_NOFOLLOW);
                (
                    Some(path.clone()),
                    Some(options.open(path)?),
                    fs::metadata(path)?.len(),
                )
            }
            None => (None, None, 0),
        };
        let applied_event_ids = verified
            .records
            .iter()
            .filter(|record| record.applied)
            .map(|record| record.event.event_id.clone())
            .collect();
        Ok(Self {
            root: root.to_path_buf(),
            stream_id: stream_id.to_owned(),
            config_revision: config_revision.to_owned(),
            max_segment_bytes,
            _writer_lock: writer_lock,
            current_path,
            current_file,
            current_bytes,
            current_segment_id: verified.last_segment_id,
            records: verified.records,
            applied_event_ids,
            poisoned: false,
            #[cfg(test)]
            fail_next_append: false,
        })
    }

    pub fn verify(
        root: impl AsRef<Path>,
        stream_id: &str,
        config_revision: &str,
    ) -> Result<Vec<PersistedRealtimeEvent>, DurabilityError> {
        Ok(verify_wal(root.as_ref(), Some((stream_id, config_revision)))?.records)
    }

    pub fn records(&self) -> &[PersistedRealtimeEvent] {
        &self.records
    }

    pub fn wal_cursor(&self) -> u64 {
        self.records.last().map_or(0, |record| record.wal_cursor)
    }

    fn append_decisions(
        &mut self,
        decisions: &[PendingDecision],
    ) -> Result<Vec<PersistedRealtimeEvent>, DurabilityError> {
        if self.poisoned {
            return Err(DurabilityError::WriterPoisoned);
        }
        if decisions.is_empty() {
            return Err(DurabilityError::EmptyBatch);
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_append) {
            self.poisoned = true;
            return Err(DurabilityError::InjectedAppendFailure);
        }
        let first_wal_cursor = self
            .wal_cursor()
            .checked_add(1)
            .ok_or(DurabilityError::CursorOverflow)?;
        let mut previous_record_sha256 = self
            .records
            .last()
            .map(|record| record.record_sha256.clone());
        let mut records = Vec::with_capacity(decisions.len());
        for (index, decision) in decisions.iter().enumerate() {
            if decision.applied != decision.public_sequence.is_some()
                || (decision.applied && decision.fail_closed_reason.is_some())
                || (!decision.applied
                    && decision.fail_closed_reason.as_deref() != Some("duplicate_event"))
            {
                return Err(DurabilityError::InvalidDecision);
            }
            let wal_cursor = first_wal_cursor
                .checked_add(u64::try_from(index).map_err(|_| DurabilityError::CursorOverflow)?)
                .ok_or(DurabilityError::CursorOverflow)?;
            let mut record = PersistedRealtimeEvent {
                schema_version: REALTIME_WAL_VERSION.into(),
                wal_cursor,
                public_sequence: decision.public_sequence,
                applied: decision.applied,
                fail_closed_reason: decision.fail_closed_reason.clone(),
                event: decision.event.clone(),
                previous_record_sha256,
                recorded_at: Utc::now(),
                record_sha256: String::new(),
                crc32c: 0,
            };
            let integrity = record_integrity_bytes(&record)?;
            record.record_sha256 = digest(&integrity);
            record.crc32c = crc32c::crc32c(&integrity);
            previous_record_sha256 = Some(record.record_sha256.clone());
            records.push(record);
        }
        let last_wal_cursor = records
            .last()
            .ok_or(DurabilityError::EmptyBatch)?
            .wal_cursor;
        let mut batch = WalBatchRecord {
            schema_version: REALTIME_WAL_BATCH_VERSION.into(),
            first_wal_cursor,
            last_wal_cursor,
            records,
            batch_sha256: String::new(),
            crc32c: 0,
        };
        let integrity = batch_integrity_bytes(&batch)?;
        batch.batch_sha256 = digest(&integrity);
        batch.crc32c = crc32c::crc32c(&integrity);
        let line = serde_json::to_vec(&batch)?;

        let result = (|| {
            self.ensure_segment(first_wal_cursor, line.len() as u64 + 1)?;
            let file = self
                .current_file
                .as_mut()
                .ok_or(DurabilityError::CorruptWal)?;
            file.write_all(&line)?;
            file.write_all(b"\n")?;
            file.sync_data()?;
            Ok::<(), DurabilityError>(())
        })();
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error);
        }

        self.current_bytes += line.len() as u64 + 1;
        for record in &batch.records {
            if record.applied {
                self.applied_event_ids.insert(record.event.event_id.clone());
            }
        }
        self.records.extend(batch.records.iter().cloned());
        Ok(batch.records)
    }

    fn ensure_segment(
        &mut self,
        first_wal_cursor: u64,
        record_bytes: u64,
    ) -> Result<(), DurabilityError> {
        if self.current_file.is_some()
            && self.current_bytes.saturating_add(record_bytes) <= self.max_segment_bytes
        {
            return Ok(());
        }
        if let Some(file) = &self.current_file {
            file.sync_all()?;
        }
        let previous_segment_sha256 = self
            .current_path
            .as_ref()
            .map(|path| fs::read(path).map(|bytes| digest(&bytes)))
            .transpose()?;
        let segment_id = self
            .current_segment_id
            .checked_add(1)
            .ok_or(DurabilityError::CursorOverflow)?;
        let path = self
            .root
            .join(segment_filename(segment_id, first_wal_cursor));
        let mut header = SegmentHeader {
            schema_version: REALTIME_WAL_SEGMENT_VERSION.into(),
            stream_id: self.stream_id.clone(),
            config_revision: self.config_revision.clone(),
            segment_id,
            first_wal_cursor,
            previous_segment_sha256,
            created_at: Utc::now(),
            header_sha256: String::new(),
        };
        header.header_sha256 = digest(&header_integrity_bytes(&header)?);
        let line = serde_json::to_vec(&header)?;
        let mut options = OpenOptions::new();
        options.create_new(true).append(true);
        #[cfg(unix)]
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        let mut file = options.open(&path)?;
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        self.current_path = Some(path);
        self.current_file = Some(file);
        self.current_bytes = line.len() as u64 + 1;
        self.current_segment_id = segment_id;
        Ok(())
    }

    #[cfg(test)]
    fn inject_append_failure(&mut self) {
        self.fail_next_append = true;
    }
}

#[cfg(unix)]
impl Drop for SegmentedRealtimeWal {
    fn drop(&mut self) {
        // The descriptor close also releases the advisory lock; the explicit unlock makes the
        // lifecycle obvious and allows a replacement writer to start immediately after drop.
        unsafe {
            libc::flock(self._writer_lock.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

#[derive(Debug)]
pub struct DurableRealtimeWriter {
    root: PathBuf,
    stream_id: String,
    config_revision: String,
    replay_capacity: usize,
    wal: SegmentedRealtimeWal,
    replay: SequencedReplay,
    recovered_checkpoint_cursor: Option<u64>,
    poisoned: bool,
}

impl DurableRealtimeWriter {
    pub fn open(
        root: impl AsRef<Path>,
        stream_id: &str,
        config_revision: &str,
        max_segment_bytes: u64,
        replay_capacity: usize,
    ) -> Result<Self, DurabilityError> {
        let root = root.as_ref();
        if replay_capacity == 0 {
            return Err(DurabilityError::InvalidConfig);
        }
        validate_root(root)?;
        create_private_directory(root)?;
        create_private_directory(&root.join("wal"))?;
        create_private_directory(&root.join("checkpoints"))?;
        let wal = SegmentedRealtimeWal::open(
            root.join("wal"),
            stream_id,
            config_revision,
            max_segment_bytes,
        )?;
        let (replay, recovered_checkpoint_cursor) = recover_replay(
            root,
            stream_id,
            config_revision,
            replay_capacity,
            wal.records(),
        )?;
        Ok(Self {
            root: root.to_path_buf(),
            stream_id: stream_id.to_owned(),
            config_revision: config_revision.to_owned(),
            replay_capacity,
            wal,
            replay,
            recovered_checkpoint_cursor,
            poisoned: false,
        })
    }

    pub fn apply(
        &mut self,
        event: NormalizedProviderEvent,
    ) -> Result<DurableApplyOutcome, DurabilityError> {
        self.apply_batch(vec![event])?
            .pop()
            .ok_or(DurabilityError::EmptyBatch)
    }

    pub fn apply_batch(
        &mut self,
        events: Vec<NormalizedProviderEvent>,
    ) -> Result<Vec<DurableApplyOutcome>, DurabilityError> {
        if self.poisoned {
            return Err(DurabilityError::WriterPoisoned);
        }
        if events.is_empty() {
            return Err(DurabilityError::EmptyBatch);
        }
        if events.iter().any(|event| !event.evidence_is_valid()) {
            return Err(DurabilityError::InvalidEventEvidence);
        }

        let mut seen = self.wal.applied_event_ids.clone();
        let mut next_sequence = self.replay.sequence();
        let mut decisions = Vec::with_capacity(events.len());
        for event in events {
            if seen.contains(&event.event_id) {
                decisions.push(PendingDecision {
                    event,
                    public_sequence: None,
                    applied: false,
                    fail_closed_reason: Some("duplicate_event".into()),
                });
            } else {
                next_sequence = next_sequence
                    .checked_add(1)
                    .ok_or(DurabilityError::CursorOverflow)?;
                seen.insert(event.event_id.clone());
                decisions.push(PendingDecision {
                    event,
                    public_sequence: Some(next_sequence),
                    applied: true,
                    fail_closed_reason: None,
                });
            }
        }

        // The whole normalized upstream frame is one WAL envelope and one sync_data barrier.
        let persistence_started = Instant::now();
        let persisted = self.wal.append_decisions(&decisions)?;
        let persistence_latency_us = elapsed_micros(persistence_started);
        let publication_started = Instant::now();
        let mut outcomes = Vec::with_capacity(persisted.len());
        for record in persisted {
            let published = if record.applied {
                let sequence = record
                    .public_sequence
                    .ok_or(DurabilityError::InvalidDecision)?;
                match self.replay.commit_persisted(record.event.clone(), sequence) {
                    Ok(event) => Some(event),
                    Err(error) => {
                        // The WAL is now ahead of memory. Stop this writer and require
                        // deterministic restart/replay rather than risking a second decision.
                        self.poisoned = true;
                        self.wal.poisoned = true;
                        return Err(DurabilityError::PublicationAfterPersistence(error));
                    }
                }
            } else {
                None
            };
            outcomes.push(DurableApplyOutcome {
                persisted: record,
                published,
                persistence_latency_us,
                publication_latency_us: 0,
            });
        }
        let publication_latency_us = elapsed_micros(publication_started);
        for outcome in &mut outcomes {
            outcome.publication_latency_us = publication_latency_us;
        }
        Ok(outcomes)
    }

    pub fn checkpoint(&self) -> Result<RealtimeCheckpoint, DurabilityError> {
        if self.poisoned || self.wal.poisoned {
            return Err(DurabilityError::WriterPoisoned);
        }
        let (_, _, public_sequence, replay) = self.replay.snapshot();
        let mut checkpoint = RealtimeCheckpoint {
            schema_version: REALTIME_CHECKPOINT_VERSION.into(),
            stream_id: self.stream_id.clone(),
            config_revision: self.config_revision.clone(),
            wal_cursor: self.wal.wal_cursor(),
            public_sequence,
            last_record_sha256: self
                .wal
                .records()
                .last()
                .map(|record| record.record_sha256.clone()),
            replay_capacity: self.replay_capacity,
            replay,
            created_at: Utc::now(),
            state_sha256: String::new(),
        };
        checkpoint.state_sha256 = digest(&checkpoint_integrity_bytes(&checkpoint)?);
        write_checkpoint(&self.root.join("checkpoints"), &checkpoint)?;
        Ok(checkpoint)
    }

    pub fn sequence(&self) -> u64 {
        self.replay.sequence()
    }

    pub fn wal_cursor(&self) -> u64 {
        self.wal.wal_cursor()
    }

    pub fn recovered_checkpoint_cursor(&self) -> Option<u64> {
        self.recovered_checkpoint_cursor
    }

    pub fn replay(&self) -> &SequencedReplay {
        &self.replay
    }

    pub(crate) fn replay_snapshot(&self) -> Vec<StreamEvent> {
        self.replay.snapshot().3
    }

    pub fn records(&self) -> &[PersistedRealtimeEvent] {
        self.wal.records()
    }
}

fn recover_replay(
    root: &Path,
    stream_id: &str,
    config_revision: &str,
    replay_capacity: usize,
    records: &[PersistedRealtimeEvent],
) -> Result<(SequencedReplay, Option<u64>), DurabilityError> {
    let checkpoint_root = root.join("checkpoints");
    let candidates = [
        checkpoint_root.join("current.json"),
        checkpoint_root.join("previous.json"),
    ];
    let mut selected = None;
    for path in candidates {
        if !path.exists() {
            continue;
        }
        if let Ok(checkpoint) = read_checkpoint(&path) {
            if checkpoint.stream_id != stream_id || checkpoint.config_revision != config_revision {
                return Err(DurabilityError::CheckpointWalDivergence);
            }
            if checkpoint.replay_capacity != replay_capacity {
                continue;
            }
            if checkpoint_matches(
                &checkpoint,
                stream_id,
                config_revision,
                replay_capacity,
                records,
            ) {
                selected = Some(checkpoint);
                break;
            }
            return Err(DurabilityError::CheckpointWalDivergence);
        }
    }

    let (mut replay, boundary, recovered_cursor) = match selected {
        Some(checkpoint) => {
            let wal_cursor = checkpoint.wal_cursor;
            (
                SequencedReplay::restore_tail(
                    checkpoint.stream_id,
                    checkpoint.replay_capacity,
                    checkpoint.public_sequence,
                    checkpoint.replay,
                )?,
                wal_cursor,
                Some(wal_cursor),
            )
        }
        None => (SequencedReplay::new(stream_id, replay_capacity)?, 0, None),
    };

    for record in records.iter().filter(|record| record.wal_cursor > boundary) {
        if record.applied {
            replay.commit_persisted(
                record.event.clone(),
                record.public_sequence.ok_or(DurabilityError::CorruptWal)?,
            )?;
        }
    }
    Ok((replay, recovered_cursor))
}

fn checkpoint_matches(
    checkpoint: &RealtimeCheckpoint,
    stream_id: &str,
    config_revision: &str,
    replay_capacity: usize,
    records: &[PersistedRealtimeEvent],
) -> bool {
    let integrity_matches = checkpoint_integrity_bytes(checkpoint)
        .map(|bytes| digest(&bytes) == checkpoint.state_sha256)
        .unwrap_or(false);
    if checkpoint.schema_version != REALTIME_CHECKPOINT_VERSION
        || checkpoint.stream_id != stream_id
        || checkpoint.config_revision != config_revision
        || checkpoint.replay_capacity != replay_capacity
        || !integrity_matches
    {
        return false;
    }
    let boundary = records.partition_point(|record| record.wal_cursor <= checkpoint.wal_cursor);
    if boundary != usize::try_from(checkpoint.wal_cursor).unwrap_or(usize::MAX) {
        return false;
    }
    let last_record_sha256 = records
        .get(boundary.saturating_sub(1))
        .map(|record| record.record_sha256.clone());
    if last_record_sha256 != checkpoint.last_record_sha256 {
        return false;
    }
    let applied = records[..boundary]
        .iter()
        .filter(|record| record.applied)
        .collect::<Vec<_>>();
    let expected_sequence = applied
        .last()
        .and_then(|record| record.public_sequence)
        .unwrap_or(0);
    let expected_replay = applied
        .iter()
        .rev()
        .take(replay_capacity)
        .rev()
        .map(|record| StreamEvent {
            stream_id: stream_id.to_owned(),
            sequence: record.public_sequence.expect("verified applied sequence"),
            event: record.event.clone(),
        })
        .collect::<Vec<_>>();
    checkpoint.public_sequence == expected_sequence && checkpoint.replay == expected_replay
}

fn write_checkpoint(root: &Path, checkpoint: &RealtimeCheckpoint) -> Result<(), DurabilityError> {
    create_private_directory(root)?;
    let current = root.join("current.json");
    let previous = root.join("previous.json");
    let temporary = root.join(format!(
        "current-{}-{}.tmp",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let bytes = serde_json::to_vec(checkpoint)?;
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let mut file = options.open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;

    // Only promote a structurally valid current checkpoint to previous. A corrupt current never
    // displaces the last known-readable fallback.
    if current.exists() && read_checkpoint(&current).is_ok() {
        fs::rename(&current, &previous)?;
    }
    fs::rename(&temporary, &current)?;
    File::open(root)?.sync_all()?;
    Ok(())
}

fn read_checkpoint(path: &Path) -> Result<RealtimeCheckpoint, DurabilityError> {
    verify_private_regular_file(path)?;
    let bytes = fs::read(path)?;
    let checkpoint: RealtimeCheckpoint = serde_json::from_slice(&bytes)?;
    let actual = digest(&checkpoint_integrity_bytes(&checkpoint)?);
    if actual != checkpoint.state_sha256 {
        return Err(DurabilityError::CorruptCheckpoint);
    }
    Ok(checkpoint)
}

fn verify_wal(
    root: &Path,
    expected_scope: Option<(&str, &str)>,
) -> Result<VerifiedWal, DurabilityError> {
    validate_root(root)?;
    let mut paths = fs::read_dir(root)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("wal"))
        .collect::<Vec<_>>();
    paths.sort();
    let mut records = Vec::new();
    let mut previous_segment_sha256 = None;
    let mut previous_record_sha256 = None;
    let mut applied_event_ids = BTreeSet::new();
    let mut expected_public_sequence = 0_u64;
    let mut stream_id: Option<String> = None;
    let mut config_revision: Option<String> = None;
    let mut last_segment_id = 0_u64;

    for path in &paths {
        verify_private_regular_file(path)?;
        let bytes = fs::read(path)?;
        if bytes.last() != Some(&b'\n') {
            return Err(DurabilityError::CorruptWal);
        }
        let lines = bytes.split(|byte| *byte == b'\n').collect::<Vec<_>>();
        if lines.len() < 2 || !lines.last().is_some_and(|line| line.is_empty()) {
            return Err(DurabilityError::CorruptWal);
        }
        let header: SegmentHeader = serde_json::from_slice(lines[0])?;
        if header.schema_version != REALTIME_WAL_SEGMENT_VERSION
            || digest(&header_integrity_bytes(&header)?) != header.header_sha256
            || header.segment_id != last_segment_id.saturating_add(1)
            || header.first_wal_cursor != records.len() as u64 + 1
            || header.previous_segment_sha256 != previous_segment_sha256
            || path.file_name().and_then(|value| value.to_str())
                != Some(segment_filename(header.segment_id, header.first_wal_cursor).as_str())
        {
            return Err(DurabilityError::CorruptWal);
        }
        if stream_id.get_or_insert_with(|| header.stream_id.clone()) != &header.stream_id
            || config_revision.get_or_insert_with(|| header.config_revision.clone())
                != &header.config_revision
        {
            return Err(DurabilityError::WalScopeMismatch);
        }
        if let Some((expected_stream, expected_revision)) = expected_scope
            && (header.stream_id != expected_stream || header.config_revision != expected_revision)
        {
            return Err(DurabilityError::WalScopeMismatch);
        }

        for line in &lines[1..lines.len() - 1] {
            if line.is_empty() {
                return Err(DurabilityError::CorruptWal);
            }
            let batch: WalBatchRecord = serde_json::from_slice(line)?;
            let batch_integrity = batch_integrity_bytes(&batch)?;
            if batch.schema_version != REALTIME_WAL_BATCH_VERSION
                || batch.records.is_empty()
                || batch.first_wal_cursor != records.len() as u64 + 1
                || batch.records.first().map(|record| record.wal_cursor)
                    != Some(batch.first_wal_cursor)
                || batch.records.last().map(|record| record.wal_cursor)
                    != Some(batch.last_wal_cursor)
                || batch.batch_sha256 != digest(&batch_integrity)
                || batch.crc32c != crc32c::crc32c(&batch_integrity)
            {
                return Err(DurabilityError::CorruptWal);
            }
            for record in batch.records {
                let integrity = record_integrity_bytes(&record)?;
                if record.schema_version != REALTIME_WAL_VERSION
                    || record.wal_cursor != records.len() as u64 + 1
                    || record.previous_record_sha256 != previous_record_sha256
                    || record.record_sha256 != digest(&integrity)
                    || record.crc32c != crc32c::crc32c(&integrity)
                    || !valid_event_evidence(&record.event)
                {
                    return Err(DurabilityError::CorruptWal);
                }
                if record.applied {
                    expected_public_sequence = expected_public_sequence
                        .checked_add(1)
                        .ok_or(DurabilityError::CursorOverflow)?;
                    if record.public_sequence != Some(expected_public_sequence)
                        || record.fail_closed_reason.is_some()
                        || !applied_event_ids.insert(record.event.event_id.clone())
                    {
                        return Err(DurabilityError::CorruptWal);
                    }
                } else if record.public_sequence.is_some()
                    || record.fail_closed_reason.as_deref() != Some("duplicate_event")
                    || !applied_event_ids.contains(&record.event.event_id)
                {
                    return Err(DurabilityError::CorruptWal);
                }
                previous_record_sha256 = Some(record.record_sha256.clone());
                records.push(record);
            }
        }
        last_segment_id = header.segment_id;
        previous_segment_sha256 = Some(digest(&bytes));
    }

    Ok(VerifiedWal {
        records,
        paths,
        last_segment_id,
    })
}

fn record_integrity_bytes(record: &PersistedRealtimeEvent) -> Result<Vec<u8>, DurabilityError> {
    Ok(serde_json::to_vec(&RecordIntegrity {
        schema_version: &record.schema_version,
        wal_cursor: record.wal_cursor,
        public_sequence: record.public_sequence,
        applied: record.applied,
        fail_closed_reason: &record.fail_closed_reason,
        event: &record.event,
        previous_record_sha256: &record.previous_record_sha256,
        recorded_at: &record.recorded_at,
    })?)
}

fn header_integrity_bytes(header: &SegmentHeader) -> Result<Vec<u8>, DurabilityError> {
    Ok(serde_json::to_vec(&HeaderIntegrity {
        schema_version: &header.schema_version,
        stream_id: &header.stream_id,
        config_revision: &header.config_revision,
        segment_id: header.segment_id,
        first_wal_cursor: header.first_wal_cursor,
        previous_segment_sha256: &header.previous_segment_sha256,
        created_at: &header.created_at,
    })?)
}

fn batch_integrity_bytes(batch: &WalBatchRecord) -> Result<Vec<u8>, DurabilityError> {
    Ok(serde_json::to_vec(&BatchIntegrity {
        schema_version: &batch.schema_version,
        first_wal_cursor: batch.first_wal_cursor,
        last_wal_cursor: batch.last_wal_cursor,
        records: &batch.records,
    })?)
}

fn checkpoint_integrity_bytes(checkpoint: &RealtimeCheckpoint) -> Result<Vec<u8>, DurabilityError> {
    Ok(serde_json::to_vec(&CheckpointIntegrity {
        schema_version: &checkpoint.schema_version,
        stream_id: &checkpoint.stream_id,
        config_revision: &checkpoint.config_revision,
        wal_cursor: checkpoint.wal_cursor,
        public_sequence: checkpoint.public_sequence,
        last_record_sha256: &checkpoint.last_record_sha256,
        replay_capacity: checkpoint.replay_capacity,
        replay: &checkpoint.replay,
        created_at: &checkpoint.created_at,
    })?)
}

fn valid_event_evidence(event: &NormalizedProviderEvent) -> bool {
    event.evidence_is_valid()
}

fn segment_filename(segment_id: u64, first_wal_cursor: u64) -> String {
    format!("segment-{segment_id:020}-{first_wal_cursor:020}.wal")
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn validate_config(
    root: &Path,
    stream_id: &str,
    config_revision: &str,
    max_segment_bytes: u64,
) -> Result<(), DurabilityError> {
    validate_root(root)?;
    if stream_id.is_empty() || config_revision.is_empty() || max_segment_bytes < 1_024 {
        return Err(DurabilityError::InvalidConfig);
    }
    Ok(())
}

fn validate_root(root: &Path) -> Result<(), DurabilityError> {
    if !root.is_absolute()
        || root == Path::new("/")
        || root
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(DurabilityError::UnsafeStorageRoot);
    }
    Ok(())
}

fn create_private_directory(path: &Path) -> Result<(), DurabilityError> {
    let existed = path.exists();
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(DurabilityError::InsecureStoragePermissions);
        }
        if existed {
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(DurabilityError::InsecureStoragePermissions);
            }
        } else {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}

fn verify_private_regular_file(path: &Path) -> Result<(), DurabilityError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(DurabilityError::InsecureStoragePermissions);
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(DurabilityError::InsecureStoragePermissions);
    }
    Ok(())
}

fn acquire_writer_lock(path: &Path) -> Result<File, DurabilityError> {
    if path.exists() && fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(DurabilityError::InsecureStoragePermissions);
    }
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let file = options.open(path)?;
    verify_private_regular_file(path)?;
    #[cfg(unix)]
    {
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::WouldBlock {
                return Err(DurabilityError::WriterAlreadyActive);
            }
            return Err(DurabilityError::Io(error));
        }
    }
    Ok(file)
}

#[derive(Debug, Error)]
pub enum DurabilityError {
    #[error("realtime durability configuration is invalid")]
    InvalidConfig,
    #[error("realtime storage root must be a dedicated absolute non-root path")]
    UnsafeStorageRoot,
    #[error("another realtime WAL writer already owns this stream")]
    WriterAlreadyActive,
    #[error("realtime WAL is corrupt or torn")]
    CorruptWal,
    #[error("realtime WAL stream or config revision does not match")]
    WalScopeMismatch,
    #[error("realtime checkpoint is corrupt")]
    CorruptCheckpoint,
    #[error("realtime checkpoint proves WAL rollback or state divergence")]
    CheckpointWalDivergence,
    #[error("realtime storage permissions or file type are unsafe")]
    InsecureStoragePermissions,
    #[error("realtime WAL cursor overflow")]
    CursorOverflow,
    #[error("realtime upstream frame cannot be empty")]
    EmptyBatch,
    #[error("realtime persisted decision is internally inconsistent")]
    InvalidDecision,
    #[error("realtime event identity or source evidence is invalid")]
    InvalidEventEvidence,
    #[error("realtime writer is poisoned and requires restart/replay")]
    WriterPoisoned,
    #[error("memory publication diverged after durable persistence: {0}")]
    PublicationAfterPersistence(RealtimeError),
    #[cfg(test)]
    #[error("injected authoritative append failure")]
    InjectedAppendFailure,
    #[error(transparent)]
    Realtime(#[from] RealtimeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HyperliquidNormalizer, ProviderPayload};
    use chrono::TimeZone;
    use serde_json::json;
    use tempfile::tempdir;

    fn event(sequence: i64, price: &str) -> NormalizedProviderEvent {
        let received = Utc.timestamp_millis_opt(1_700_000_001_000).unwrap();
        HyperliquidNormalizer::new([("BTC-PERP.HYPL".into(), "BTC".into())], 10_000)
            .unwrap()
            .normalize(
                json!({
                    "channel":"trades",
                    "data":[{"coin":"BTC","time":1_700_000_000_000_i64 + sequence,
                        "px":price,"sz":"0.10000000","side":"B","tid":sequence}]
                }),
                received,
            )
            .unwrap()
            .remove(0)
    }

    fn open(root: &Path) -> DurableRealtimeWriter {
        DurableRealtimeWriter::open(root, "hyperliquid-main", "config-v1", 1_024, 2).unwrap()
    }

    #[test]
    fn append_failure_never_publishes_or_consumes_sequence() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("runtime");
        let mut writer = open(&root);
        writer.wal.inject_append_failure();
        assert!(matches!(
            writer.apply(event(1, "65000.10000000")),
            Err(DurabilityError::InjectedAppendFailure)
        ));
        assert_eq!(writer.sequence(), 0);
        assert_eq!(writer.wal_cursor(), 0);
        drop(writer);

        let mut recovered = open(&root);
        let outcome = recovered.apply(event(1, "65000.10000000")).unwrap();
        assert_eq!(outcome.published.unwrap().sequence, 1);
    }

    #[test]
    fn duplicate_decisions_are_append_only_and_never_consume_public_sequence() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("runtime");
        let duplicate = event(1, "65000.10000000");
        let mut writer = open(&root);
        let first = writer.apply(duplicate.clone()).unwrap();
        let second = writer.apply(duplicate.clone()).unwrap();
        assert!(first.persisted.applied);
        assert_eq!(first.persisted.public_sequence, Some(1));
        assert!(!second.persisted.applied);
        assert_eq!(second.persisted.public_sequence, None);
        assert_eq!(
            second.persisted.fail_closed_reason.as_deref(),
            Some("duplicate_event")
        );
        assert!(second.published.is_none());
        assert_eq!(writer.sequence(), 1);
        assert_eq!(writer.wal_cursor(), 2);
        drop(writer);

        let mut recovered = open(&root);
        assert_eq!(recovered.sequence(), 1);
        let third = recovered.apply(duplicate).unwrap();
        assert!(!third.persisted.applied);
        assert_eq!(recovered.sequence(), 1);
        assert_eq!(recovered.wal_cursor(), 3);
    }

    #[test]
    fn one_upstream_frame_is_one_atomic_wal_batch_and_one_memory_generation() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("runtime");
        let first = event(1, "65000.10000000");
        let second = event(2, "65000.20000000");
        let mut writer =
            DurableRealtimeWriter::open(&root, "hyperliquid-main", "config-v1", 16 * 1_024, 4)
                .unwrap();
        let outcomes = writer
            .apply_batch(vec![first.clone(), second, first])
            .unwrap();
        assert_eq!(outcomes.len(), 3);
        assert_eq!(writer.sequence(), 2);
        assert_eq!(writer.wal_cursor(), 3);
        assert!(outcomes[0].published.is_some());
        assert!(outcomes[1].published.is_some());
        assert!(outcomes[2].published.is_none());

        let path = writer.wal.current_path.clone().unwrap();
        let lines = fs::read_to_string(path).unwrap();
        assert_eq!(
            lines.lines().count(),
            2,
            "one header plus one batch envelope"
        );
        let batch: WalBatchRecord = serde_json::from_str(lines.lines().nth(1).unwrap()).unwrap();
        assert_eq!(batch.schema_version, REALTIME_WAL_BATCH_VERSION);
        assert_eq!(batch.first_wal_cursor, 1);
        assert_eq!(batch.last_wal_cursor, 3);
        assert_eq!(batch.records.len(), 3);
    }

    #[test]
    fn segmented_chain_checkpoint_and_restart_replay_are_deterministic() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("runtime");
        let mut writer = open(&root);
        writer.apply(event(1, "65000.10000000")).unwrap();
        writer.apply(event(2, "65000.20000000")).unwrap();
        let checkpoint = writer.checkpoint().unwrap();
        writer.apply(event(3, "65000.30000000")).unwrap();
        assert_eq!(writer.sequence(), 3);
        assert_eq!(writer.records().len(), 3);
        assert!(
            fs::read_dir(root.join("wal"))
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| {
                    entry.path().extension().and_then(|value| value.to_str()) == Some("wal")
                })
                .count()
                >= 2
        );
        drop(writer);

        let recovered = open(&root);
        assert_eq!(
            recovered.recovered_checkpoint_cursor(),
            Some(checkpoint.wal_cursor)
        );
        assert_eq!(recovered.sequence(), 3);
        assert_eq!(recovered.wal_cursor(), 3);
        assert_eq!(recovered.records()[2].event, event(3, "65000.30000000"));
    }

    #[test]
    fn corrupt_current_checkpoint_falls_back_to_previous_then_wal_tail() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("runtime");
        let mut writer = open(&root);
        writer.apply(event(1, "65000.10000000")).unwrap();
        let first = writer.checkpoint().unwrap();
        writer.apply(event(2, "65000.20000000")).unwrap();
        writer.checkpoint().unwrap();
        writer.apply(event(3, "65000.30000000")).unwrap();
        drop(writer);
        fs::write(root.join("checkpoints/current.json"), b"corrupt").unwrap();

        let recovered = open(&root);
        assert_eq!(
            recovered.recovered_checkpoint_cursor(),
            Some(first.wal_cursor)
        );
        assert_eq!(recovered.sequence(), 3);
        assert_eq!(recovered.wal_cursor(), 3);
    }

    #[test]
    fn torn_wal_and_wrong_scope_fail_closed() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("runtime");
        let mut writer = open(&root);
        writer.apply(event(1, "65000.10000000")).unwrap();
        let last_path = writer.wal.current_path.clone().unwrap();
        drop(writer);
        OpenOptions::new()
            .append(true)
            .open(last_path)
            .unwrap()
            .write_all(b"{")
            .unwrap();
        assert!(matches!(
            open_result(&root),
            Err(DurabilityError::CorruptWal)
        ));

        let clean_root = directory.path().join("clean-runtime");
        let mut clean = open(&clean_root);
        clean.apply(event(1, "65000.10000000")).unwrap();
        drop(clean);
        assert!(matches!(
            DurableRealtimeWriter::open(&clean_root, "different-stream", "config-v1", 1_024, 2),
            Err(DurabilityError::WalScopeMismatch)
        ));
    }

    #[test]
    fn checkpoint_detects_cleanly_truncated_wal_tail() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("runtime");
        let mut writer = open(&root);
        writer.apply(event(1, "65000.10000000")).unwrap();
        writer.apply(event(2, "65000.20000000")).unwrap();
        writer.checkpoint().unwrap();
        let last_path = writer.wal.current_path.clone().unwrap();
        drop(writer);
        fs::remove_file(last_path).unwrap();

        assert!(matches!(
            open_result(&root),
            Err(DurabilityError::CheckpointWalDivergence)
        ));
    }

    #[test]
    fn exact_decimal_and_raw_evidence_survive_wal_round_trip() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("runtime");
        let original = event(1, "65000.12345678");
        let expected_raw_sha256 = original.raw_sha256.clone();
        let mut writer = open(&root);
        writer.apply(original.clone()).unwrap();
        drop(writer);
        let recovered = open(&root);
        let actual = &recovered.records()[0].event;
        assert_eq!(actual, &original);
        assert_eq!(actual.raw_sha256, expected_raw_sha256);
        let ProviderPayload::Trade { price, size, .. } = &actual.payload else {
            panic!("expected trade")
        };
        assert_eq!(price.0.to_string(), "65000.12345678");
        assert_eq!(size.0.to_string(), "0.1");
    }

    #[test]
    fn payload_tampering_is_rejected_before_authoritative_append() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("runtime");
        let mut forged = event(1, "65000.12345678");
        let ProviderPayload::Trade { price, .. } = &mut forged.payload else {
            panic!("expected trade")
        };
        *price = crate::ExactDecimal::positive(&json!("1.00000001")).unwrap();
        let mut writer = open(&root);
        assert!(matches!(
            writer.apply(forged),
            Err(DurabilityError::InvalidEventEvidence)
        ));
        assert_eq!(writer.sequence(), 0);
        assert_eq!(writer.wal_cursor(), 0);
    }

    #[test]
    fn advisory_lock_enforces_exactly_one_writer() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("runtime");
        let writer = open(&root);
        assert!(matches!(
            open_result(&root),
            Err(DurabilityError::WriterAlreadyActive)
        ));
        drop(writer);
        assert!(open_result(&root).is_ok());
    }

    fn open_result(root: &Path) -> Result<DurableRealtimeWriter, DurabilityError> {
        DurableRealtimeWriter::open(root, "hyperliquid-main", "config-v1", 1_024, 2)
    }
}
