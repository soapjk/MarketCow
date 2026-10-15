//! Typed indexed history. Sessions retain page ranges only, never encoded
//! pages, evidence bodies, or a long-lived WAL read transaction.
use super::btc_typed_store as store;
use anyhow::{Context, Result, ensure};
use marketcow_contracts::typed_v3::{common, history};
use prost::Message;
use rusqlite::{OptionalExtension, params};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub const TTL: Duration = Duration::from_secs(600);
pub const MAX_ROWS: u64 = 1_024_000;
pub const MAX_PAGES: usize = 1024;
pub const PAGE_BYTES: usize = 4 * 1024 * 1024;
pub const SESSION_BYTES: usize = 1024 * 1024 * 1024;
pub const BUILD_TIME: Duration = Duration::from_secs(120);
struct PageRange {
    first: u64,
    last: u64,
    count: usize,
}
pub struct Session {
    id: String,
    range: history::Range,
    created: Instant,
    revision: u64,
    pages: Vec<PageRange>,
    predecessor: Option<u64>,
    count: u64,
    first_time: Option<i64>,
    last_time: Option<i64>,
    reason: Option<String>,
    missing: Vec<history::MissingRange>,
    database_health: Option<history::Health>,
    archive_health: Option<history::Health>,
}
pub struct Service {
    path: PathBuf,
    sessions: Mutex<BTreeMap<String, Arc<Session>>>,
    build: Mutex<()>,
    observations: Mutex<BTreeMap<String, Arc<ObservationSession>>>,
}
struct ObservationSession {
    id: String,
    range: history::ObservationRange,
    created: Instant,
    high: i64,
    first: i64,
    health: Option<history::Health>,
}
fn revision(db: &rusqlite::Connection, first: u64, through: u64) -> Result<u64> {
    let value: i64 = db.query_row(
        "SELECT COALESCE(MAX(revision),0)+1 FROM typed_trade_repairs WHERE trade_id>=? AND trade_id<=?",
        params![i64::try_from(first)?, i64::try_from(through)?],
        |r| r.get(0),
    )?;
    ensure!(value > 0, "invalid database revision");
    Ok(value as u64)
}
fn validate_range(range: &history::Range) -> Result<()> {
    ensure!(
        range.source == common::Source::BinanceSpotTrade as i32
            && range.instrument_id == "BTCUSDT.BINANCE"
            && range.start_time_ms > 0
            && range.through_trade_id > 0
            && (1..=1000).contains(&range.limit),
        "invalid history range"
    );
    i64::try_from(range.through_trade_id)?;
    Ok(())
}
fn freeze(
    path: &Path,
    range: history::Range,
    archive_health: Option<history::Health>,
    database_health: Option<history::Health>,
) -> Result<Session> {
    validate_range(&range)?;
    let mut db = store::open_reader(path)?;
    let deadline = Instant::now() + BUILD_TIME;
    let tx = db.transaction()?;
    let high: Option<i64> =
        tx.query_row("SELECT MAX(trade_id) FROM typed_binance_trades", [], |r| {
            r.get(0)
        })?;
    let predecessor: Option<i64> = tx
        .query_row(
            "SELECT trade_id FROM typed_binance_trades
      WHERE trade_time_ms<? AND trade_id<=? ORDER BY trade_id DESC LIMIT 1",
            params![range.start_time_ms, i64::try_from(range.through_trade_id)?],
            |r| r.get(0),
        )
        .optional()?;
    let revision = revision(&tx, predecessor.unwrap_or(0) as u64, range.through_trade_id)?;
    let skipped: bool = if let Some(before) = predecessor {
        tx.query_row(
            "SELECT EXISTS(
      SELECT 1 FROM typed_binance_trades WHERE trade_time_ms>=? AND trade_id<=?)",
            params![range.start_time_ms, before],
            |r| r.get(0),
        )?
    } else {
        false
    };
    let mut stmt = tx.prepare(
        "SELECT trade_id,trade_time_ms,length(price),length(quantity),
      COALESCE(length(evidence_id),0) FROM typed_binance_trades WHERE trade_id>?
      AND trade_id<=? ORDER BY trade_id",
    )?;
    let mut rows = stmt.query(params![
        predecessor.unwrap_or(0),
        i64::try_from(range.through_trade_id)?
    ])?;
    let mut reason = if skipped {
        Some("supplier_time_regression".into())
    } else {
        None
    };
    let mut missing = Vec::new();
    let mut previous = predecessor;
    let mut previous_time = None;
    let mut count = 0u64;
    let mut first_time = None;
    let mut last_time = None;
    let mut pages = Vec::new();
    let mut first = 0;
    let mut in_page = 0;
    let mut byte_upper = 0usize;
    while let Some(row) = rows.next()? {
        ensure!(Instant::now() < deadline, "history_deadline");
        let id: i64 = row.get(0)?;
        let time: i64 = row.get(1)?;
        ensure!(id > 0 && time > 0, "database invalid identity/time");
        let field_bytes = row.get::<_, i64>(2)? + row.get::<_, i64>(3)? + row.get::<_, i64>(4)?;
        ensure!(
            (0..=1024).contains(&field_bytes),
            "database invalid field budget"
        );
        // Deliberately conservative bound for all scalar tags, varints and
        // strings in Trade. No serialization or original-body read is needed.
        byte_upper = byte_upper
            .checked_add(field_bytes as usize + 192)
            .context("history byte overflow")?;
        ensure!(byte_upper <= SESSION_BYTES, "history_session_bytes");
        if previous.is_some_and(|p| id != p + 1) {
            ensure!(missing.len() < 1024, "history_gap_capacity");
            missing.push(history::MissingRange {
                first_trade_id: (previous.unwrap() + 1) as u64,
                last_trade_id: (id - 1) as u64,
            });
            reason = Some("trade_id_gap".into());
        }
        if time < range.start_time_ms || previous_time.is_some_and(|t| time < t) {
            reason = Some("supplier_time_regression".into());
        }
        if in_page == 0 {
            first = id as u64;
        }
        in_page += 1;
        count += 1;
        ensure!(count <= MAX_ROWS, "history_trade_capacity");
        first_time.get_or_insert(time);
        last_time = Some(time);
        previous = Some(id);
        previous_time = Some(time);
        if in_page == range.limit as usize {
            byte_upper = byte_upper
                .checked_add(64 * 1024)
                .context("history byte overflow")?;
            ensure!(byte_upper <= SESSION_BYTES, "history_session_bytes");
            pages.push(PageRange {
                first,
                last: id as u64,
                count: in_page,
            });
            in_page = 0;
            ensure!(pages.len() <= MAX_PAGES, "history_page_capacity");
        }
    }
    if in_page > 0 {
        byte_upper = byte_upper
            .checked_add(64 * 1024)
            .context("history byte overflow")?;
        ensure!(byte_upper <= SESSION_BYTES, "history_session_bytes");
        pages.push(PageRange {
            first,
            last: previous.unwrap() as u64,
            count: in_page,
        });
    }
    if pages.is_empty() {
        pages.push(PageRange {
            first: range.through_trade_id,
            last: range.through_trade_id,
            count: 0,
        });
        reason = Some("range_has_no_trades".into());
    }
    ensure!(pages.len() <= MAX_PAGES, "history_page_capacity");
    if predecessor.is_none() {
        reason = Some("boundary_predecessor_missing".into());
    }
    if previous.map(|id| id as u64) != Some(range.through_trade_id) {
        reason = Some(
            if high.is_none_or(|h| (h as u64) < range.through_trade_id) {
                "database_write_lag"
            } else {
                "through_trade_missing"
            }
            .into(),
        );
    }
    ensure!(
        last_time.is_none_or(|t| t - range.start_time_ms <= 960_000),
        "history_span_capacity"
    );
    drop(rows);
    drop(stmt);
    tx.commit()?;
    Ok(Session {
        id: uuid::Uuid::new_v4().to_string(),
        range,
        created: Instant::now(),
        revision,
        pages,
        predecessor: predecessor.map(|id| id as u64),
        count,
        first_time,
        last_time,
        reason,
        missing,
        // Coverage is already represented separately. Until the actual writer
        // health is supplied, absence means unknown, not fabricated healthy.
        database_health,
        archive_health,
    })
}
impl Service {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            sessions: Mutex::new(BTreeMap::new()),
            build: Mutex::new(()),
            observations: Mutex::new(BTreeMap::new()),
        }
    }
    pub fn observation_path(&self) -> Result<PathBuf> {
        Ok(self
            .path
            .parent()
            .and_then(Path::parent)
            .context("typed_database_unavailable")?
            .join("chainlink-twap/observations.sqlite3"))
    }
    pub fn read_observations(
        &self,
        request: &history::ObservationPageRequest,
    ) -> Result<history::ObservationPage> {
        use marketcow_contracts::typed_v3::btc;
        let range = request.range.as_ref().context("missing history range")?;
        let source = common::Source::try_from(range.source)?;
        ensure!(
            matches!(
                source,
                common::Source::ChainlinkBtcUsd | common::Source::ChainlinkBtcUsdTwap60
            ) && range.instrument_id
                == if source == common::Source::ChainlinkBtcUsd {
                    "BTCUSD.CHAINLINK"
                } else {
                    "BTCUSD.CHAINLINK.TWAP60"
                }
                && range.start_time_ms > 0
                && range.end_time_ms > range.start_time_ms
                && range.end_time_ms - range.start_time_ms <= 960_000
                && (1..=1000).contains(&range.limit),
            "invalid history range"
        );
        let path = self.observation_path()?;
        let mut db = store::open_reader(&path)?;
        let tx = db.transaction()?;
        let (session, after) = if let Some(token) = &request.page_token {
            let (id, after) = token.rsplit_once(':').context("invalid page token")?;
            let after: i64 = after.parse().context("invalid page index")?;
            let mut sessions = self
                .observations
                .lock()
                .map_err(|_| anyhow::anyhow!("session owner poisoned"))?;
            sessions.retain(|_, s| s.created.elapsed() < TTL);
            let session = sessions
                .get(id)
                .cloned()
                .context("session_expired_or_unknown")?;
            ensure!(&session.range == range, "session_range_mismatch");
            ensure!(
                after >= session.first.saturating_sub(1) && after <= session.high,
                "invalid page index"
            );
            (session, after)
        } else {
            let mut sessions = self
                .observations
                .lock()
                .map_err(|_| anyhow::anyhow!("session owner poisoned"))?;
            sessions.retain(|_, s| s.created.elapsed() < TTL);
            if let Some(session) = sessions.values().find(|s| &s.range == range).cloned() {
                let after = session.first.saturating_sub(1);
                (session, after)
            } else {
                ensure!(sessions.len() < 4, "session_capacity");
                let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM typed_chainlink_observations WHERE source=? LIMIT 1)",[range.source],|r|r.get(0))?;
                ensure!(exists, "source_not_collected");
                let (first,high,count,bytes):(Option<i64>,Option<i64>,i64,i64)=tx.query_row("SELECT MIN(observation_id),MAX(observation_id),COUNT(*),COALESCE(SUM(length(price)+length(source_generation)+length(evidence_id)+192),0) FROM typed_chainlink_observations WHERE source=? AND source_time_ms>=? AND source_time_ms<?",params![range.source,range.start_time_ms,range.end_time_ms],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
                ensure!(
                    count >= 0 && count as u64 <= MAX_ROWS,
                    "history_observation_capacity"
                );
                ensure!(
                    (count as usize).div_ceil(range.limit as usize) <= MAX_PAGES,
                    "history_page_capacity"
                );
                ensure!(
                    bytes >= 0 && bytes as usize <= SESSION_BYTES,
                    "history_session_bytes"
                );
                let first = first.unwrap_or(1);
                let high = high.unwrap_or(0);
                let session = Arc::new(ObservationSession {
                    id: uuid::Uuid::new_v4().to_string(),
                    range: range.clone(),
                    created: Instant::now(),
                    high,
                    first,
                    health: None,
                });
                sessions.insert(session.id.clone(), session.clone());
                (session, first - 1)
            }
        };
        let mut stmt=tx.prepare("SELECT observation_id,source_generation,source_sequence,row_index,source_time_ms,price,received_at_ns,evidence_id FROM typed_chainlink_observations INDEXED BY typed_chainlink_id WHERE source=? AND observation_id>? AND observation_id<=? AND source_time_ms>=? AND source_time_ms<? ORDER BY observation_id LIMIT ?")?;
        let mut rows = stmt.query(params![
            range.source,
            after,
            session.high,
            range.start_time_ms,
            range.end_time_ms,
            range.limit + 1
        ])?;
        let mut output = Vec::with_capacity(range.limit as usize + 1);
        while let Some(row) = rows.next()? {
            output.push(history::ObservationRecord {
                observation_id: u64::try_from(row.get::<_, i64>(0)?)?,
                source_generation: row.get(1)?,
                observation: Some(btc::Observation {
                    source_sequence: Some(row.get::<_, i64>(2)?.try_into()?),
                    row_index: Some(row.get(3)?),
                    source_time_ms: row.get(4)?,
                    price: row.get(5)?,
                    received_at_ns: row.get(6)?,
                    evidence: Some(common::EvidenceReference {
                        evidence_id: row.get(7)?,
                        row_index: Some(row.get(3)?),
                    }),
                }),
            });
        }
        drop(rows);
        drop(stmt);
        tx.commit()?;
        let more = output.len() > range.limit as usize;
        if more {
            output.pop();
        }
        let next = if more {
            Some(format!(
                "{}:{}",
                session.id,
                output
                    .last()
                    .context("observation page empty")?
                    .observation_id
            ))
        } else {
            None
        };
        let value = history::ObservationPage {
            schema_version: "marketcow.btc-observation-history.v3".into(),
            range: Some(range.clone()),
            observations: output,
            next_page_token: next,
            complete: !more,
            coverage_verified: false,
            reason: Some("observation_prefix_coverage_unverified".into()),
            frozen_high_observation_id: session.high.try_into()?,
            database_health_at_freeze: session.health.clone(),
        };
        ensure!(value.encoded_len() <= PAGE_BYTES, "history_page_bytes");
        Ok(value)
    }
    pub fn create(
        &self,
        range: history::Range,
        archive_health: Option<history::Health>,
    ) -> Result<history::Page> {
        self.create_with_health(range, archive_health, None)
    }
    pub fn create_with_health(
        &self,
        range: history::Range,
        archive_health: Option<history::Health>,
        database_health: Option<history::Health>,
    ) -> Result<history::Page> {
        validate_range(&range)?;
        // Session map is never held during DB work. One bounded build is
        // admitted immediately; callers do not queue behind a global wait.
        let reusable = {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|_| anyhow::anyhow!("session owner poisoned"))?;
            sessions.retain(|_, s| s.created.elapsed() < TTL);
            if let Some(s) = sessions.values().find(|s| s.range == range) {
                Some(s.clone())
            } else {
                ensure!(sessions.len() < 4, "session_capacity");
                None
            }
        };
        if let Some(session) = reusable {
            return page(&self.path, &session, 0);
        }
        let _build = self
            .build
            .try_lock()
            .map_err(|_| anyhow::anyhow!("history_capacity"))?;
        let session = Arc::new(freeze(&self.path, range, archive_health, database_health)?);
        let first = page(&self.path, &session, 0)?;
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("session owner poisoned"))?;
        sessions.retain(|_, s| s.created.elapsed() < TTL);
        ensure!(sessions.len() < 4, "session_capacity");
        sessions.insert(session.id.clone(), session);
        Ok(first)
    }
    pub fn read(&self, request: &history::PageRequest) -> Result<history::Page> {
        let range = request.range.as_ref().context("missing history range")?;
        validate_range(range)?;
        let token = request.page_token.as_ref().context("missing page token")?;
        let (id, index) = token.rsplit_once(':').context("invalid page token")?;
        let index: usize = index.parse().context("invalid page index")?;
        let session = {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|_| anyhow::anyhow!("session owner poisoned"))?;
            sessions.retain(|_, s| s.created.elapsed() < TTL);
            sessions.get(id).cloned()
        };
        let session = session.context("session_expired_or_unknown")?;
        ensure!(&session.range == range, "session_range_mismatch");
        page(&self.path, &session, index)
    }
    pub fn release(&self, request: &history::ReleaseRequest) -> Result<history::ReleaseReceipt> {
        let range = request.range.as_ref().context("missing release range")?;
        validate_range(range)?;
        ensure!(
            uuid::Uuid::parse_str(&request.session_id).is_ok(),
            "release session identity"
        );
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("session owner poisoned"))?;
        sessions.retain(|_, s| s.created.elapsed() < TTL);
        if let Some(s) = sessions.get(&request.session_id) {
            ensure!(&s.range == range, "session_range_mismatch");
        }
        let released = sessions.remove(&request.session_id).is_some();
        Ok(history::ReleaseReceipt {
            schema_version: "marketcow.btc-history-release.v3".into(),
            session_id: request.session_id.clone(),
            range: Some(range.clone()),
            released,
        })
    }
}
fn page(path: &Path, session: &Session, index: usize) -> Result<history::Page> {
    ensure!(
        session.created.elapsed() < TTL,
        "session_expired_or_unknown"
    );
    let selected = session.pages.get(index).context("page_out_of_range")?;
    let mut db = store::open_reader(path)?;
    let tx = db.transaction()?;
    ensure!(
        revision(
            &tx,
            session.predecessor.unwrap_or(0),
            session.range.through_trade_id
        )? == session.revision,
        "session_invalidated"
    );
    let trades = if selected.count == 0 {
        Vec::new()
    } else {
        store::read_trades(&tx, selected.first, selected.last, selected.count)?
    };
    ensure!(trades.len() == selected.count, "session_invalidated");
    let predecessor = if index == 0 {
        session
            .predecessor
            .map(|id| store::read_trade(&tx, id))
            .transpose()?
            .flatten()
    } else {
        None
    };
    if index == 0 {
        ensure!(
            predecessor.as_ref().map(|t| t.trade_id) == session.predecessor,
            "session_invalidated"
        );
    }
    tx.commit()?;
    let final_page = index + 1 == session.pages.len();
    let page = history::Page {
        schema_version: "marketcow.btc-trade-history.v3".into(),
        session_id: session.id.clone(),
        range: Some(session.range.clone()),
        page_index: index as u32,
        page_count: session.pages.len() as u32,
        session_ttl_seconds: 600,
        predecessor,
        trades,
        next_page_token: (!final_page).then(|| format!("{}:{}", session.id, index + 1)),
        complete: final_page && session.reason.is_none(),
        coverage_verified: session.reason.is_none(),
        reason: session.reason.clone(),
        missing_ranges: session.missing.clone(),
        coverage_first_time_ms: session.first_time,
        coverage_last_time_ms: session.last_time,
        total_trade_count: session.count,
        database_health_at_freeze: session.database_health.clone(),
        archive_health_at_freeze: session.archive_health.clone(),
        database_revision: session.revision,
    };
    ensure!(page.encoded_len() <= PAGE_BYTES, "history_page_bytes");
    Ok(page)
}

#[cfg(test)]
mod native_tests {
    use super::*;
    use marketcow_contracts::typed_v3::btc;
    fn row(id: u64) -> btc::Trade {
        btc::Trade {
            trade_id: id,
            trade_time_ms: 1000 + id as i64 * 100,
            price: "100.00".into(),
            quantity: "0.0100".into(),
            buyer_is_maker: Some(false),
            ..Default::default()
        }
    }
    fn input() -> store::EvidenceInput<'static> {
        store::EvidenceInput {
            raw: b"original supplier evidence",
            acquisition: "ws_archive",
            upstream_url: "wss://data-stream.binance.vision/ws/btcusdt@trade",
            received_at_ns: None,
            source_generation: Some("supplier-generation"),
            content_type: "application/json",
        }
    }
    fn setup() -> (
        tempfile::TempDir,
        rusqlite::Connection,
        Service,
        history::Range,
    ) {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("binance-trades/trades.sqlite3");
        let mut db = store::open(&path).unwrap();
        store::insert_trades(&mut db, &(1..=5).map(row).collect::<Vec<_>>(), &input()).unwrap();
        let service = Service::new(path);
        let range = history::Range {
            source: common::Source::BinanceSpotTrade as i32,
            instrument_id: "BTCUSDT.BINANCE".into(),
            start_time_ms: 1250,
            through_trade_id: 5,
            limit: 1,
        };
        (root, db, service, range)
    }
    #[test]
    fn native_pages_release_reuse_and_append_are_independent() {
        let (_root, mut db, service, range) = setup();
        let first = service.create(range.clone(), None).unwrap();
        assert_eq!(first.predecessor.as_ref().unwrap().trade_id, 2);
        assert_eq!(first.page_count, 3);
        assert!(!first.complete);
        assert_eq!(
            service.create(range.clone(), None).unwrap().session_id,
            first.session_id
        );
        store::insert_trades(&mut db, &[row(6)], &input()).unwrap();
        let second = service
            .read(&history::PageRequest {
                range: Some(range.clone()),
                page_token: first.next_page_token,
            })
            .unwrap();
        let last = service
            .read(&history::PageRequest {
                range: Some(range.clone()),
                page_token: second.next_page_token,
            })
            .unwrap();
        assert!(last.complete && last.coverage_verified);
        assert_eq!(last.trades[0].price, "100.00");
        let release = history::ReleaseRequest {
            session_id: first.session_id,
            range: Some(range),
        };
        assert!(service.release(&release).unwrap().released);
        assert!(!service.release(&release).unwrap().released);
    }
    #[test]
    fn native_incomplete_initial_response_retries_do_not_consume_new_slots() {
        let (_root, _db, service, mut range) = setup();
        range.through_trade_id = 8;
        let first = service.create(range.clone(), None).unwrap();
        assert!(!first.coverage_verified);
        for _ in 0..8 {
            assert_eq!(
                service.create(range.clone(), None).unwrap().session_id,
                first.session_id
            );
        }
        assert_eq!(service.sessions.lock().unwrap().len(), 1);
    }
    #[test]
    fn native_range_binding_and_in_range_evidence_revision_are_checked() {
        let (_root, mut db, service, range) = setup();
        let first = service.create(range.clone(), None).unwrap();
        let mut wrong = range.clone();
        wrong.limit = 2;
        assert_eq!(
            service
                .read(&history::PageRequest {
                    range: Some(wrong.clone()),
                    page_token: first.next_page_token.clone()
                })
                .unwrap_err()
                .to_string(),
            "session_range_mismatch"
        );
        assert!(
            service
                .release(&history::ReleaseRequest {
                    session_id: first.session_id,
                    range: Some(wrong)
                })
                .is_err()
        );
        let mut updated = row(3);
        updated.event_time_ms = Some(1301);
        store::insert_trades(&mut db, &[updated], &input()).unwrap();
        assert_eq!(
            service
                .read(&history::PageRequest {
                    range: Some(range),
                    page_token: first.next_page_token
                })
                .unwrap_err()
                .to_string(),
            "session_invalidated"
        );
    }
    #[test]
    fn native_observation_watermark_preserves_equal_time_facts() {
        let (root, _db, service, _range) = setup();
        let path = service.observation_path().unwrap();
        let mut db = store::open(&path).unwrap();
        let source = common::Source::ChainlinkBtcUsdTwap60;
        let rows = (0..2)
            .map(|index| btc::Observation {
                source_time_ms: 2000,
                price: "100.00".into(),
                source_sequence: Some(7),
                row_index: Some(index),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        store::insert_observations(&mut db, source, "generation-a", &rows, &input()).unwrap();
        let range = history::ObservationRange {
            source: source as i32,
            instrument_id: "BTCUSD.CHAINLINK.TWAP60".into(),
            start_time_ms: 1000,
            end_time_ms: 3000,
            limit: 1,
        };
        let first = service
            .read_observations(&history::ObservationPageRequest {
                range: Some(range.clone()),
                page_token: None,
            })
            .unwrap();
        let mut next = rows[0].clone();
        next.source_sequence = Some(8);
        next.source_time_ms = 2100;
        store::insert_observations(&mut db, source, "generation-a", &[next], &input()).unwrap();
        let last = service
            .read_observations(&history::ObservationPageRequest {
                range: Some(range),
                page_token: first.next_page_token,
            })
            .unwrap();
        assert!(last.complete && !last.coverage_verified);
        assert_eq!(
            last.observations[0].observation.as_ref().unwrap().row_index,
            Some(1)
        );
        assert_eq!(
            first.frozen_high_observation_id,
            last.frozen_high_observation_id
        );
        assert_eq!(
            root.path().join("chainlink-twap/observations.sqlite3"),
            path
        );
    }
}
