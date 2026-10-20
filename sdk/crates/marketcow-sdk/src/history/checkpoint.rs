//! Opt-in small durable continuation, not a backup of application history.
//! Saving follows application consumption. A crash before saving can replay a
//! page; users still own their idempotent storage/transaction boundary.
use super::*;
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const SCHEMA: &str = "marketcow.sdk.trade-history-checkpoint.v1";
const MAX_BYTES: usize = 64 * 1024;
#[derive(Clone, PartialEq, Message)]
struct Checkpoint {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    endpoint: String,
    #[prost(message, optional, tag = "3")]
    range: Option<history::Range>,
    #[prost(string, tag = "4")]
    request_id: String,
    #[prost(string, optional, tag = "5")]
    session_id: Option<String>,
    #[prost(string, optional, tag = "6")]
    token: Option<String>,
    #[prost(uint32, optional, tag = "7")]
    page_count: Option<u32>,
    #[prost(uint64, optional, tag = "8")]
    total_count: Option<u64>,
    #[prost(uint64, optional, tag = "9")]
    revision: Option<u64>,
    #[prost(uint32, tag = "10")]
    next_index: u32,
    #[prost(uint64, tag = "11")]
    rows: u64,
    #[prost(uint64, tag = "12")]
    bytes: u64,
    #[prost(message, optional, tag = "13")]
    last: Option<btc::Trade>,
    #[prost(uint64, tag = "14")]
    deadline_unix_ms: u64,
    #[prost(bool, tag = "15")]
    complete: bool,
    #[prost(bool, tag = "16")]
    released: bool,
    #[prost(bool, optional, tag = "17")]
    release_on_complete: Option<bool>,
}
fn now_ms() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Protocol("local clock before epoch".into()))?
        .as_millis()
        .try_into()
        .map_err(|_| Error::Protocol("local clock overflow".into()))
}
fn read(path: &Path) -> Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path)?;
    require(
        metadata.is_file() && metadata.len() <= MAX_BYTES as u64,
        "checkpoint regular file/size",
    )?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    std::fs::File::open(path)?
        .take((MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    require(bytes.len() <= MAX_BYTES, "checkpoint grew beyond budget")?;
    Ok(bytes)
}
fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        require(
            metadata.is_file(),
            "checkpoint target is not a regular file",
        )?;
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|e| Error::Io(e.error))?;
    #[cfg(unix)]
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}
impl TradeHistory {
    /// Explicit asynchronous local persistence, no API key or raw payloads.
    /// Caller must save only after durably consuming the returned page. This
    /// does not transfer or recreate an expired/server-restarted session.
    pub async fn save_checkpoint(&self, path: impl Into<PathBuf>) -> Result<()> {
        require(
            !self.stopped || self.complete,
            "abandoned history cannot be checkpointed",
        )?;
        let deadline = self.expires.map_or(self.deadline, |t| t.min(self.deadline));
        let remaining = deadline
            .saturating_duration_since(Instant::now())
            .as_millis();
        let checkpoint = Checkpoint {
            schema: SCHEMA.into(),
            endpoint: self.client.0.endpoint.to_string(),
            range: Some(self.range.clone()),
            request_id: self.request_id.clone(),
            session_id: self.session_id.clone(),
            token: self.token.clone(),
            page_count: self.page_count,
            total_count: self.total_count,
            revision: self.revision,
            next_index: self.next_index,
            rows: self.rows,
            bytes: self.bytes,
            last: self.last.clone(),
            deadline_unix_ms: now_ms()?
                .checked_add(
                    remaining
                        .try_into()
                        .map_err(|_| Error::Limit("checkpoint deadline"))?,
                )
                .ok_or(Error::Limit("checkpoint deadline"))?,
            complete: self.complete,
            released: self.released,
            release_on_complete: Some(self.release_on_complete),
        };
        require(
            checkpoint.encoded_len() <= MAX_BYTES,
            "checkpoint byte budget",
        )?;
        let bytes = checkpoint.encode_to_vec();
        let path = path.into();
        tokio::task::spawn_blocking(move || write(&path, &bytes))
            .await
            .map_err(|_| Error::Protocol("checkpoint writer task".into()))?
    }
}
impl Client {
    /// Resume a saved fixed-ID job. A 410 is reported, never converted to a
    /// new freeze. The application's previously installed prefix is not saved.
    pub async fn resume_trade_history(&self, path: impl Into<PathBuf>) -> Result<TradeHistory> {
        let path = path.into();
        let bytes = tokio::task::spawn_blocking(move || read(&path))
            .await
            .map_err(|_| Error::Protocol("checkpoint reader task".into()))??;
        let checkpoint = Checkpoint::decode(bytes.as_slice())?;
        require(
            checkpoint.schema == SCHEMA
                && checkpoint.endpoint == self.0.endpoint.as_str()
                && uuid::Uuid::parse_str(&checkpoint.request_id).is_ok(),
            "checkpoint identity",
        )?;
        let range = checkpoint
            .range
            .ok_or_else(|| Error::Protocol("checkpoint range absent".into()))?;
        let mut owner =
            self.trade_history(range.start_time_ms, range.through_trade_id, range.limit)?;
        require(owner.range == range, "checkpoint source/instrument")?;
        let remaining = checkpoint.deadline_unix_ms.saturating_sub(now_ms()?);
        require(
            (remaining > 0 || checkpoint.complete)
                && remaining <= self.limits().history_timeout.as_millis() as u64,
            "checkpoint expired or local clock changed",
        )?;
        require(
            checkpoint.bytes <= self.limits().history_bytes
                && checkpoint.rows <= self.limits().history_rows
                && checkpoint.next_index <= self.limits().history_pages,
            "checkpoint resource budget",
        )?;
        if checkpoint.next_index == 0 {
            require(
                checkpoint.token.is_none()
                    && checkpoint.last.is_none()
                    && checkpoint.session_id.is_none()
                    && checkpoint.page_count.is_none()
                    && checkpoint.total_count.is_none()
                    && checkpoint.revision.is_none()
                    && checkpoint.rows == 0
                    && checkpoint.bytes == 0
                    && !checkpoint.complete
                    && !checkpoint.released,
                "checkpoint initial state",
            )?;
        } else {
            let count = checkpoint
                .page_count
                .ok_or_else(|| Error::Protocol("checkpoint page count absent".into()))?;
            let total = checkpoint
                .total_count
                .ok_or_else(|| Error::Protocol("checkpoint row count absent".into()))?;
            let id = checkpoint
                .session_id
                .as_deref()
                .ok_or_else(|| Error::Protocol("checkpoint session absent".into()))?;
            require(
                uuid::Uuid::parse_str(id).is_ok()
                    && count > 0
                    && count <= self.limits().history_pages
                    && checkpoint.next_index <= count
                    && checkpoint.complete == (checkpoint.next_index == count)
                    && checkpoint.revision.is_some_and(|r| r > 0)
                    && total <= self.limits().history_rows
                    && checkpoint.rows <= total,
                "checkpoint frozen metadata",
            )?;
            require(
                if checkpoint.complete {
                    checkpoint.token.is_none() && checkpoint.rows == total
                } else {
                    checkpoint.token.as_deref()
                        == Some(format!("{id}:{}", checkpoint.next_index).as_str())
                },
                "checkpoint page continuation",
            )?;
            let last = checkpoint
                .last
                .as_ref()
                .ok_or_else(|| Error::Protocol("checkpoint tail absent".into()))?;
            validate_trade(last)?;
            require(
                last.trade_id <= range.through_trade_id
                    && (!checkpoint.complete || last.trade_id == range.through_trade_id),
                "checkpoint tail identity",
            )?;
        }
        owner.request_id = checkpoint.request_id;
        owner.session_id = checkpoint.session_id;
        owner.token = checkpoint.token;
        owner.page_count = checkpoint.page_count;
        owner.total_count = checkpoint.total_count;
        owner.revision = checkpoint.revision;
        owner.next_index = checkpoint.next_index;
        owner.rows = checkpoint.rows;
        owner.bytes = checkpoint.bytes;
        owner.last = checkpoint.last;
        owner.deadline = Instant::now() + Duration::from_millis(remaining);
        owner.expires = Some(owner.deadline);
        owner.complete = checkpoint.complete;
        owner.stopped = checkpoint.complete;
        owner.released = checkpoint.released;
        owner.release_on_complete = checkpoint.release_on_complete.unwrap_or(true);
        if let Some(id) = &owner.session_id {
            if !owner.released {
                self.register_lease(crate::leases::Lease::Trades(history::ReleaseRequest {
                    session_id: id.clone(),
                    range: Some(owner.range.clone()),
                    request_id: Some(owner.request_id.clone()),
                }))?;
            }
        }
        Ok(owner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn checkpoint_keeps_request_identity_and_rejects_wrong_origin_or_expiry() {
        let client = Client::builder("http://127.0.0.1:8793").build().unwrap();
        let job = client.trade_history(1100, 3, 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("job.pb");
        job.save_checkpoint(&path).await.unwrap();
        let resumed = client.resume_trade_history(&path).await.unwrap();
        assert_eq!(resumed.request_id, job.request_id);
        let other = Client::builder("http://127.0.0.1:8794").build().unwrap();
        assert!(other.resume_trade_history(&path).await.is_err());
        let mut value = Checkpoint::decode(read(&path).unwrap().as_slice()).unwrap();
        value.deadline_unix_ms = 1;
        write(&path, &value.encode_to_vec()).unwrap();
        assert!(client.resume_trade_history(&path).await.is_err());
    }
}
