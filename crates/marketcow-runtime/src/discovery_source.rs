//! Bounded durable publisher for a prepared Discovery source generation.
//! JSONL is authoritative; SQLite and state-index.json publish only after fsync.
//! A crash leaving an unindexed tail fails closed on reopen, never truncates it.

use anyhow::{Context, Result, bail, ensure};
use marketcow_polymarket::discovery_source::canonical_hash;
use rusqlite::{Connection, OpenFlags, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

/// Hard parser/storage ceiling; managed collectors enforce an explicit lower budget.
pub const MAX_SOURCE_TOKEN_IDENTITIES: usize = 8192;
const MAX_SCOPED_TOKEN_IDENTITIES:usize=MAX_SOURCE_TOKEN_IDENTITIES;

pub fn is_token_recovery(event: &Value) -> bool {
    matches!(
        event["event_type"].as_str(),
        Some("recovery_started" | "recovery_completed")
    ) && event["canonical_payload"]["recovery_scope"] == "token"
}

/// Shared by the memory publisher and disk worker. A status transition cannot
/// clear another token or complete without a matching authoritative full snapshot.
pub fn apply_token_recovery(state: &mut BTreeMap<String, Value>, event: &Value) -> Result<()> {
    let raw = &event["raw_payload"];
    let full_snapshot = event["event_type"] == "book"
        && (raw["event_type"] == "book"
            || (raw.get("event_type").is_none()
                && raw["asset_id"] == event["token_id"]
                && raw["market"] == event["condition_id"]
                && raw["tick_size"].is_string()
                && raw["timestamp"].is_string()
                && raw["hash"].is_string()
                && raw["bids"].is_array()
                && raw["asks"].is_array()));
    if full_snapshot {
        if let Some(current) = state.get_mut(required(event, "token_id")?) {
            current["snapshot_cursor"] = event["cursor"].clone();
        }
    } else if is_token_recovery(event) {
        let token = required(event, "token_id")?;
        let payload = &event["canonical_payload"];
        if event["event_type"] == "recovery_started" {
            ensure!(
                state.contains_key(token) || state.len() < MAX_SOURCE_TOKEN_IDENTITIES,
                "token recovery capacity"
            );
            state.insert(
                token.into(),
                json!({"token_id":token,"market_id":event["market_id"],
                "condition_id":event["condition_id"],"recovery_id":payload["recovery_id"],
                "snapshot_cursor":null,"gap":event["gaps"][0]}),
            );
        } else {
            let current = state.get(token).context("completion without recovery")?;
            ensure!(
                current["recovery_id"] == payload["recovery_id"]
                    && current["snapshot_cursor"].is_u64()
                    && current["snapshot_cursor"] == payload["snapshot_cursor"],
                "completion without matching snapshot"
            );
            state.remove(token);
        }
    }
    Ok(())
}

pub fn load_token_recoveries(db: &Connection) -> Result<BTreeMap<String, Value>> {
    let exists: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='source_token_recoveries')",
        [],
        |r| r.get(0),
    )?;
    let mut result = BTreeMap::new();
    if !exists {
        return Ok(result);
    }
    let mut query = db.prepare(
        "SELECT token_id,payload_json,payload_sha256 FROM source_token_recoveries LIMIT 2049",
    )?;
    let mut rows = query.query([])?;
    while let Some(row) = rows.next()? {
        ensure!(result.len() < MAX_SOURCE_TOKEN_IDENTITIES, "stored recovery limit");
        let token: String = row.get(0)?;
        let body: Vec<u8> = row.get(1)?;
        let sha: String = row.get(2)?;
        ensure!(
            body.len() <= 65536 && hex::encode(Sha256::digest(&body)) == sha,
            "recovery checksum/cap"
        );
        let model: Value = serde_json::from_slice(&body)?;
        ensure!(
            !token.is_empty()
                && token.len() <= 128
                && model["token_id"] == token
                && model["gap"]["token_id"] == token
                && model["gap"]["resolved"] == false
                && model["gap"]["code"] == "coverage_gap"
                && model["gap"]["detected_at"].is_string()
                && model["market_id"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty())
                && model["condition_id"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty())
                && model["recovery_id"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty() && value.len() <= 128)
                && (model["snapshot_cursor"].is_null()
                    || model["snapshot_cursor"]
                        .as_u64()
                        .is_some_and(|cursor| cursor > 0)),
            "recovery identity"
        );
        result.insert(token, model);
    }
    Ok(result)
}

/// Pure validation shared by memory publication and the independent disk worker.
/// This function must never read SQLite, take file locks or perform filesystem I/O.
pub fn validate_source_events(
    events: &[Value],
    terminal: bool,
    cursor: u64,
    token_markets: &BTreeMap<String, String>,
    maximum_batch_bytes: usize,
) -> Result<Vec<Vec<u8>>> {
    ensure!(
        !events.is_empty() && events.len() <= 256,
        "batch must contain 1..256 events"
    );
    let mut grouped: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut lines = Vec::new();
    let mut total: usize = 0;
    let mut received: Option<String> = None;
    for (index, event) in events.iter().enumerate() {
        let recovery = is_token_recovery(event);
        ensure!(
            event["cursor"].as_u64() == cursor.checked_add(index as u64 + 1),
            "noncontiguous source cursor"
        );
        ensure!(
            (event["event_type"] == if terminal { "market_terminal" } else { "book" }
                || recovery && !terminal)
                && event["applied"] == true
                && (event["gaps"] == json!([]) || recovery)
                && event["fail_closed_reason"].is_null(),
            "not an applicable complete snapshot"
        );
        ensure!(
            event["schema_version"] == "marketcow.polymarket.live.v2",
            "wrong event schema"
        );
        let market = required(event, "market_id")?;
        if recovery {
            let token = required(event, "token_id")?;
            let p = &event["canonical_payload"];
            ensure!(
                token_markets.get(token).is_some_and(|m| m == market)
                    && p["token_id"] == token
                    && !required(p, "recovery_id")?.is_empty()
                    && event["condition_id"].is_string(),
                "recovery binding"
            );
            if event["event_type"] == "recovery_started" {
                let gaps = event["gaps"].as_array().context("recovery gaps")?;
                ensure!(
                    gaps.len() == 1
                        && gaps[0]["token_id"] == token
                        && gaps[0]["resolved"] == false
                        && gaps[0]["code"] == "coverage_gap"
                        && gaps[0]["detected_at"].is_string()
                        && !required(p, "reason")?.is_empty(),
                    "recovery must affect only own token"
                );
            } else {
                ensure!(
                    event["gaps"] == json!([])
                        && p["resolved_gap_token_ids"] == json!([token])
                        && p["snapshot_cursor"]
                            .as_u64()
                            .is_some_and(|c| c > 0 && c < event["cursor"].as_u64().unwrap()),
                    "recovery snapshot boundary"
                );
            }
        } else if terminal {
            let model = &event["canonical_payload"]["market"];
            let lifecycle_valid = (model["lifecycle_state"] == "closed"
                && model["resolution"].is_null())
                || (model["lifecycle_state"] == "resolved"
                    && model["resolution"]
                        .as_str()
                        .is_some_and(|value| !value.is_empty()));
            ensure!(
                events.len() == 1
                    && event["token_id"].is_null()
                    && model["identity"]["market_id"] == market
                    && model["identity"]["condition_id"] == event["condition_id"]
                    && lifecycle_valid
                    && model["closed"] == true
                    && model["accepting_orders"] == false
                    && model["lifecycle_source"] == "polymarket_gamma"
                    && model["terminal_at"].is_string()
                    && model["lifecycle_evidence_sha256"].is_string(),
                "invalid terminal evidence"
            );
            let outcomes = model["identity"]["outcomes"]
                .as_array()
                .context("missing terminal tokens")?;
            ensure!(outcomes.len() == 2, "terminal identity must be binary");
            let retired = event["canonical_payload"]["retired_token_ids"]
                .as_array()
                .context("missing retired terminal tokens")?;
            ensure!(retired.len() == 2, "terminal retirement must be binary");
            let retired: BTreeSet<_> = retired
                .iter()
                .map(|token| token.as_str().context("invalid retired token"))
                .collect::<Result<_>>()?;
            let outcome_tokens: BTreeSet<_> = outcomes
                .iter()
                .map(|outcome| required(outcome, "token_id"))
                .collect::<Result<_>>()?;
            ensure!(
                retired == outcome_tokens,
                "terminal retirement binding differs"
            );
            for outcome in outcomes {
                let token = required(outcome, "token_id")?;
                ensure!(
                    token_markets.get(token).is_some_and(|m| m == market),
                    "terminal token binding differs"
                );
            }
        } else {
            let token = required(event, "token_id")?;
            let observed = required(event, "received_at")?;
            if let Some(previous) = &received {
                ensure!(previous == observed, "mixed recovery observation boundary");
            } else {
                received = Some(observed.into());
            }
            ensure!(
                grouped
                    .entry(market.into())
                    .or_default()
                    .insert(token.into()),
                "duplicate token"
            );
            ensure!(
                token_markets
                    .get(token)
                    .is_some_and(|value| value == market),
                "token is outside prepared market identity"
            );
            ensure!(
                event["canonical_payload"]["token_id"] == token,
                "book token differs"
            );
        }
        ensure!(
            event["canonical_payload_sha256"] == canonical_hash(&event["canonical_payload"])
                && event["raw_payload_sha256"] == canonical_hash(&event["raw_payload"]),
            "payload checksum mismatch"
        );
        let mut identity = event.clone();
        identity
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("event is not object"))?
            .remove("event_id");
        ensure!(
            event["event_id"] == canonical_hash(&identity),
            "event identity mismatch"
        );
        let mut line = serde_json::to_vec(event)?;
        line.push(b'\n');
        total = total
            .checked_add(line.len())
            .ok_or_else(|| anyhow::anyhow!("batch length overflow"))?;
        ensure!(total <= maximum_batch_bytes, "batch byte budget exceeded");
        lines.push(line);
    }
    ensure!(
        grouped.values().all(|tokens| tokens.len() == 2)
            || events.iter().all(|event| {
                is_token_recovery(event)
                    || matches!(
                        event["raw_payload"]["event_type"].as_str(),
                        Some(
                            "book"
                                | "price_change"
                                | "tick_size_change"
                                | "last_trade_price"
                                | "best_bid_ask"
                        )
                    )
            }),
        "whole two-token market recovery required"
    );
    Ok(lines)
}

/// Immutable proof: external/untrusted events are validated once, then the exact
/// encoded lines are shared with the disk worker. No public mutation API.
pub struct ValidatedSourceBatch {
    events: Vec<Value>,
    lines: Vec<Vec<u8>>,
    terminal: bool,
    after: u64,
}
impl ValidatedSourceBatch {
    pub fn new(
        events: Vec<Value>,
        terminal: bool,
        after: u64,
        tokens: &BTreeMap<String, String>,
        limit: usize,
    ) -> Result<Self> {
        let lines = validate_source_events(&events, terminal, after, tokens, limit)?;
        Ok(Self {
            events,
            lines,
            terminal,
            after,
        })
    }
    pub fn events(&self) -> &[Value] {
        &self.events
    }
    pub fn encoded_bytes(&self) -> usize {
        self.lines.iter().map(Vec::len).sum()
    }
}

pub struct PreparedSourceWriter {
    recoveries: BTreeMap<String, Value>,
    root: PathBuf,
    database: Connection,
    log: Option<File>,
    _lease: File,
    maximum_batch_bytes: usize,
    poisoned: bool,
    history_limit: Option<usize>,
    // One small summary per prepared token, never event history or full books.
    health: BTreeMap<String, BookHealth>,
    token_markets: BTreeMap<String, String>,
}

struct BookHealth {
    market_id: String,
    received_at: chrono::DateTime<chrono::Utc>,
    complete: bool,
}

impl BookHealth {
    fn from_book(market_id: String, book: &Value) -> Result<Self> {
        Ok(Self {
            market_id,
            received_at: chrono::DateTime::parse_from_rfc3339(required(book, "received_at")?)?
                .with_timezone(&chrono::Utc),
            complete: ["bids", "asks"]
                .iter()
                .all(|side| book[*side].as_array().is_some_and(|v| !v.is_empty())),
        })
    }
}

fn metadata(db: &Connection) -> Result<BTreeMap<String, String>> {
    Ok(db
        .prepare("SELECT key,value FROM metadata")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?)
}

fn required<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing string {field}"))
}

impl PreparedSourceWriter {
    pub fn open(root: &Path, maximum_batch_bytes: usize) -> Result<Self> {
        ensure!(maximum_batch_bytes > 0, "batch byte budget is required");
        let root = fs::canonicalize(root)?;
        let lease = super::acquire_writer_lock(&root.join(".collector.lock"))?;
        let path = root.join("indexes/latest-state.sqlite3");
        ensure!(
            fs::canonicalize(&path)?.starts_with(&root),
            "index escapes generation"
        );
        let mut database = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        database.busy_timeout(std::time::Duration::from_secs(5))?;
        let values = metadata(&database)?;
        for key in [
            "schema_version",
            "unresolved_gap_count",
            "event_log_size",
            "latest_cursor",
            "catalog_revision",
            "active_recovery_id",
        ] {
            ensure!(
                values.contains_key(key),
                "missing required source metadata: {key}"
            );
        }
        ensure!(
            values["schema_version"] == "marketcow.polymarket.state-index.v1",
            "wrong source schema"
        );
        let mut recoveries = load_token_recoveries(&database)?;
        ensure!(
            values["unresolved_gap_count"].parse::<usize>()? == recoveries.len(),
            "source recovery required: unaccounted gaps"
        );
        let gaps: i64 =
            database.query_row("SELECT count(*) FROM gaps WHERE resolved=0", [], |r| {
                r.get(0)
            })?;
        ensure!(gaps == 0, "source gap ledger disagrees");
        database.execute_batch(
            "CREATE TABLE IF NOT EXISTS source_token_recoveries(\
                token_id TEXT PRIMARY KEY,payload_json BLOB NOT NULL,payload_sha256 TEXT NOT NULL);\
             CREATE TABLE IF NOT EXISTS market_lifecycle(\
                market_id TEXT PRIMARY KEY,cursor INTEGER NOT NULL,\
                payload_json BLOB NOT NULL,payload_sha256 TEXT NOT NULL)",
        )?;
        // A lifecycle overlay is an authoritative retirement boundary. Older
        // generations could commit the terminal row separately from the
        // preceding coverage recovery, leaving an audit-valid but no-longer
        // actionable recovery behind. Reconcile that state atomically at the
        // writer boundary; the event log and lifecycle row remain untouched.
        let terminal_markets: BTreeSet<String> = {
            let mut statement = database.prepare(
                "SELECT market_id,payload_json,payload_sha256 FROM market_lifecycle",
            )?;
            let mut rows = statement.query([])?;
            let mut result = BTreeSet::new();
            while let Some(row) = rows.next()? {
                let market: String = row.get(0)?;
                let body: Vec<u8> = row.get(1)?;
                let sha: String = row.get(2)?;
                ensure!(
                    hex::encode(Sha256::digest(&body)) == sha,
                    "terminal lifecycle hash mismatch"
                );
                let model: Value = serde_json::from_slice(&body)?;
                if matches!(model["lifecycle_state"].as_str(), Some("closed" | "resolved")) {
                    result.insert(market);
                }
            }
            result
        };
        if !terminal_markets.is_empty() {
            let stale: Vec<String> = recoveries
                .iter()
                .filter_map(|(token, recovery)| {
                    recovery["market_id"]
                        .as_str()
                        .filter(|market| terminal_markets.contains(*market))
                        .map(|_| token.clone())
                })
                .collect();
            if !stale.is_empty() {
                let tx = database.transaction()?;
                for token in &stale {
                    tx.execute("DELETE FROM source_token_recoveries WHERE token_id=?", [token])?;
                    recoveries.remove(token);
                }
                tx.execute(
                    "INSERT OR REPLACE INTO metadata(key,value) VALUES('unresolved_gap_count',?)",
                    [recoveries.len().to_string()],
                )?;
                tx.commit()?;
            }
        }
        let log_path = root.join("events.jsonl");
        let size: u64 = values["event_log_size"].parse()?;
        let cursor: u64 = values["latest_cursor"].parse()?;
        let history_limit = values
            .get("bounded_history_bytes")
            .map(|v| v.parse::<usize>())
            .transpose()?;
        let mut log = if history_limit.is_none() {
            ensure!(
                fs::canonicalize(&log_path)?.starts_with(&root),
                "event log escapes generation"
            );
            let file = OpenOptions::new().read(true).append(true).open(log_path)?;
            ensure!(
                file.metadata()?.len() == size,
                "unindexed or missing log tail; explicit recovery required"
            );
            Some(file)
        } else {
            None
        };
        if history_limit.is_some() {
            let pages: u64 = values
                .get("bounded_max_pages")
                .context("missing database page budget")?
                .parse()?;
            database.pragma_update(None, "max_page_count", i64::try_from(pages)?)?;
            let latest: Option<i64> =
                database.query_row("SELECT MAX(cursor) FROM recent_events", [], |r| r.get(0))?;
            if cursor == 0 {
                ensure!(latest.is_none() && size == 0
                    && values["recent_event_bytes"] == "0"
                    && values["history_floor_cursor"] == "0", "invalid empty bounded boundary");
                let books: i64 = database.query_row("SELECT COUNT(*) FROM books", [], |r| r.get(0))?;
                ensure!(books == 0 && recoveries.is_empty(), "empty boundary has source state");
            } else {
            ensure!(
                latest.map(|v| v as u64) == Some(cursor),
                "bounded history boundary mismatch"
            );
            let (body, sha): (Vec<u8>, String) = database.query_row(
                "SELECT payload,sha256 FROM recent_events WHERE cursor=?",
                [i64::try_from(cursor)?],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            ensure!(
                hex::encode(Sha256::digest(&body)) == sha,
                "bounded history checksum mismatch"
            );
            let (stored_bytes, first): (i64, i64) = database.query_row(
                "SELECT SUM(length(payload)),MIN(cursor) FROM recent_events",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            ensure!(
                stored_bytes.to_string() == values["recent_event_bytes"]
                    && first
                        .checked_sub(1)
                        .context("history floor overflow")?
                        .to_string()
                        == values["history_floor_cursor"],
                "bounded history accounting mismatch"
            );
            }
        } else if cursor > 0 {
            let log = log.as_mut().context("legacy log missing")?;
            let (offset, length, expected): (i64, i64, String) = database.query_row(
                "SELECT byte_offset,byte_length,line_sha256 FROM event_offsets WHERE cursor=?",
                [i64::try_from(cursor)?],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            let offset = u64::try_from(offset)?;
            let length = u64::try_from(length)?;
            ensure!(
                length <= maximum_batch_bytes as u64 && offset.checked_add(length) == Some(size),
                "invalid tail boundary"
            );
            log.seek(SeekFrom::Start(offset))?;
            let mut tail = vec![0; length as usize];
            log.read_exact(&mut tail)?;
            ensure!(
                hex::encode(Sha256::digest(&tail)) == expected,
                "tail checksum mismatch"
            );
        }
        database.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        let mut health = BTreeMap::new();
        {
            let mut statement =
                database.prepare("SELECT token_id,market_id,payload_json FROM books")?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let token: String = row.get(0)?;
                let market: String = row.get(1)?;
                let body: Vec<u8> = row.get(2)?;
                let book: Value = serde_json::from_slice(&body)?;
                health.insert(token, BookHealth::from_book(market, &book)?);
            }
        }
        let token_markets = health
            .iter()
            .map(|(token, summary)| (token.clone(), summary.market_id.clone()))
            .collect();
        // A killed publisher may leave an unpublished scratch manifest. Only
        // after validating the durable boundary, and while holding the writer
        // lease, retire this exact scratch file; never consume it as state.
        let pending = root.join("state-index.rust-pending.json");
        match fs::symlink_metadata(&pending) {
            Ok(info) => {
                ensure!(info.file_type().is_file(), "pending manifest is not a regular file");
                fs::remove_file(&pending)?;
                File::open(&root)?.sync_all()?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(Self {
            root,
            database,
            log,
            _lease: lease,
            maximum_batch_bytes,
            poisoned: false,
            history_limit,
            health,
            recoveries,
            token_markets,
        })
    }

    /// Explicit migration after the legacy tail has been verified. State tables
    /// are already complete at this boundary. Old JSONL is left untouched;
    /// disposal is a separate, verified maintenance operation.
    pub fn enable_bounded_history(&mut self, maximum_bytes: usize) -> Result<()> {
        ensure!(
            maximum_bytes >= self.maximum_batch_bytes,
            "history must fit one maximum batch"
        );
        if let Some(existing) = self.history_limit {
            ensure!(
                existing == maximum_bytes,
                "history budget differs; explicit migration required"
            );
            return Ok(());
        }
        let cursor = self.cursor()?;
        let tail = if cursor == 0 {
            let values = metadata(&self.database)?;
            let books: i64 = self.database.query_row("SELECT COUNT(*) FROM books", [], |r| r.get(0))?;
            let offsets: i64 = self.database.query_row("SELECT COUNT(*) FROM event_offsets", [], |r| r.get(0))?;
            ensure!(values["event_log_size"] == "0" && books == 0 && offsets == 0
                && self.recoveries.is_empty(), "empty migration has source state");
            None
        } else {
        let (offset, length, hash): (i64, i64, String) = self.database.query_row(
            "SELECT byte_offset,byte_length,line_sha256 FROM event_offsets WHERE cursor=?",
            [i64::try_from(cursor)?],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        let mut line = vec![0; usize::try_from(length)?];
        let log = self.log.as_mut().context("legacy migration log missing")?;
        log.seek(SeekFrom::Start(u64::try_from(offset)?))?;
        log.read_exact(&mut line)?;
        ensure!(
            hex::encode(Sha256::digest(&line)) == hash,
            "migration tail hash differs"
        );
            Some((line, hash, length))
        };
        let tx = self.database.transaction()?;
        tx.execute_batch("CREATE TABLE recent_events(cursor INTEGER PRIMARY KEY,payload BLOB NOT NULL,sha256 TEXT NOT NULL); DELETE FROM event_offsets;")?;
        let length = if let Some((line, hash, length)) = tail {
            tx.execute(
            "INSERT INTO recent_events VALUES(?,?,?)",
            params![i64::try_from(cursor)?, line, hash],
            )?;
            length
        } else { 0 };
        let page_size =
            usize::try_from(tx.query_row("PRAGMA page_size", [], |r| r.get::<_, i64>(0))?)?;
        let pages =
            usize::try_from(tx.query_row("PRAGMA page_count", [], |r| r.get::<_, i64>(0))?)?;
        let maximum_pages = pages
            .checked_add(
                maximum_bytes
                    .checked_mul(4)
                    .context("history budget overflow")?
                    / page_size
                    + 1024,
            )
            .context("page budget overflow")?;
        for (key, value) in [
            ("bounded_history_bytes", maximum_bytes.to_string()),
            ("bounded_max_pages", maximum_pages.to_string()),
            ("history_floor_cursor", cursor.saturating_sub(1).to_string()),
            ("recent_event_bytes", length.to_string()),
        ] {
            tx.execute(
                "INSERT OR REPLACE INTO metadata VALUES(?,?)",
                params![key, value],
            )?;
        }
        tx.commit()?;
        self.database
            .pragma_update(None, "max_page_count", i64::try_from(maximum_pages)?)?;
        self.history_limit = Some(maximum_bytes);
        self.log = None;
        Ok(())
    }

    /// Catalog identities only: never creates placeholder books or events.
    pub fn bind_catalog_tokens(&mut self, bindings: BTreeMap<String, String>) -> Result<()> {
        ensure!(
            bindings.len() <= MAX_SCOPED_TOKEN_IDENTITIES
                && self.token_markets.len().checked_add(bindings.keys()
                    .filter(|token| !self.token_markets.contains_key(*token)).count())
                    .is_some_and(|total|total <= MAX_SCOPED_TOKEN_IDENTITIES),
            "bounded scoped identity budget exceeded"
        );
        for (token, market) in &bindings {
            ensure!(
                !token.is_empty() && !market.is_empty(),
                "empty catalog identity"
            );
            if let Some(previous) = self.token_markets.get(token) {
                ensure!(previous == market, "catalog token identity changed");
            }
        }
        self.token_markets.extend(bindings);
        Ok(())
    }

    pub fn cursor(&self) -> Result<u64> {
        Ok(metadata(&self.database)?["latest_cursor"].parse()?)
    }

    /// Ordered scope retirement, not a venue resolution. Called only by the
    /// durability owner AFTER preceding batches and after old-scope leases
    /// expire. Recent event history remains unchanged; current unused books
    /// and token recovery state are released without inventing event cursors.
    pub fn retire_catalog_markets(&mut self, after:u64, markets:&BTreeSet<String>) -> Result<()> {
        ensure!(!self.poisoned && self.cursor()?==after,"retirement boundary differs");
        ensure!(!markets.is_empty() && markets.len()<=4096,"retirement market budget");
        let tokens:BTreeSet<String>=self.token_markets.iter().filter(|(_,market)|markets.contains(*market)).map(|(token,_)|token.clone()).collect();
        // The in-memory owner verified membership. Metadata-only dependencies
        // have no acquisition tokens; their lifecycle rows may still retire.
        let mut values=metadata(&self.database)?;
        let mut oldest=None;
        let mut complete=BTreeMap::<String,usize>::new();
        let mut count=0usize;
        for (token,health) in &self.health {
            if tokens.contains(token) {continue;}
            count+=1;
            oldest=Some(oldest.map_or(health.received_at,|at:chrono::DateTime<chrono::Utc>|at.min(health.received_at)));
            if health.complete {*complete.entry(health.market_id.clone()).or_default()+=1;}
        }
        values.insert("book_token_count".into(),count.to_string());
        values.insert("book_complete_market_count".into(),complete.values().filter(|n|**n==2).count().to_string());
        values.insert("oldest_book_received_at".into(),oldest.map_or_else(String::new,|at|at.to_rfc3339()));
        values.insert("unresolved_gap_count".into(),self.recoveries.keys().filter(|token|!tokens.contains(*token)).count().to_string());
        self.poisoned=true;
        let tx=self.database.transaction()?;
        for token in &tokens {
            tx.execute("DELETE FROM books WHERE token_id=?",[token])?;
            tx.execute("DELETE FROM book_confirmations WHERE token_id=?",[token])?;
            tx.execute("DELETE FROM source_token_recoveries WHERE token_id=?",[token])?;
        }
        for market in markets {
            tx.execute("DELETE FROM market_lifecycle WHERE market_id=?",[market])?;
        }
        for (key,value) in &values {tx.execute("INSERT OR REPLACE INTO metadata VALUES (?,?)",[key,value])?;}
        tx.commit()?;
        self.health.retain(|token,_|!tokens.contains(token));
        self.recoveries.retain(|token,_|!tokens.contains(token));
        self.token_markets.retain(|token,_|!tokens.contains(token));
        self.write_index_manifest(&values)?;
        self.poisoned=false;
        Ok(())
    }

    /// Commit only whole binary markets. Missing token snapshots never become a
    /// partial published recovery. All allocations are bounded by the input cap.
    pub fn append_market_batch(&mut self, events: &[Value]) -> Result<()> {
        self.append_source_events(events, false)
    }

    pub fn append_terminal(&mut self, event: &Value) -> Result<()> {
        self.append_source_events(std::slice::from_ref(event), true)
    }

    fn append_source_events(&mut self, events: &[Value], terminal: bool) -> Result<()> {
        ensure!(
            !self.poisoned,
            "writer requires restart/recovery after failed commit"
        );
        let cursor = self.cursor()?;
        let lines = validate_source_events(
            events,
            terminal,
            cursor,
            &self.token_markets,
            self.maximum_batch_bytes,
        )?;
        self.commit_encoded(events, &lines, terminal, cursor)
    }

    /// Coalesce already validated, contiguous batches on the disk worker only.
    /// The original JSONL bytes are retained; no publication waits for this commit.
    pub fn append_validated_group(&mut self, batches: &[&ValidatedSourceBatch]) -> Result<()> {
        let first = batches.first().context("empty persistence group")?;
        let mut after = first.after;
        let mut bytes = 0usize;
        let mut count = 0usize;
        for batch in batches {
            ensure!(
                batch.after == after && batch.terminal == first.terminal,
                "incompatible persistence group"
            );
            bytes = bytes
                .checked_add(batch.encoded_bytes())
                .context("group byte overflow")?;
            count += batch.events.len();
            ensure!(
                bytes <= self.maximum_batch_bytes && count <= 256,
                "persistence group cap"
            );
            after = batch.events.last().context("empty batch")?["cursor"]
                .as_u64()
                .context("cursor")?;
        }
        let merged = ValidatedSourceBatch {
            events: batches
                .iter()
                .flat_map(|b| b.events.iter().cloned())
                .collect(),
            lines: batches
                .iter()
                .flat_map(|b| b.lines.iter().cloned())
                .collect(),
            terminal: first.terminal,
            after: first.after,
        };
        self.append_validated(&merged)
    }

    pub fn append_validated(&mut self, batch: &ValidatedSourceBatch) -> Result<()> {
        ensure!(
            !self.poisoned && self.cursor()? == batch.after,
            "validated batch boundary differs"
        );
        ensure!(
            batch.encoded_bytes() <= self.maximum_batch_bytes,
            "writer byte cap"
        );
        // Proofs cannot authorize another writer's universe. Identity checks are
        // cheap; hashes and JSON encoding have already been verified exactly once.
        for event in &batch.events {
            let market = required(event, "market_id")?;
            if batch.terminal {
                for outcome in event["canonical_payload"]["market"]["identity"]["outcomes"]
                    .as_array()
                    .context("outcomes")?
                {
                    ensure!(
                        self.token_markets
                            .get(required(outcome, "token_id")?)
                            .is_some_and(|m| m == market),
                        "proof outside writer universe"
                    );
                }
            } else {
                ensure!(
                    self.token_markets
                        .get(required(event, "token_id")?)
                        .is_some_and(|m| m == market),
                    "proof outside writer universe"
                );
            }
        }
        self.commit_encoded(&batch.events, &batch.lines, batch.terminal, batch.after)
    }

    fn commit_encoded(
        &mut self,
        events: &[Value],
        lines: &[Vec<u8>],
        terminal: bool,
        cursor: u64,
    ) -> Result<()> {
        if let Some(limit) = self.history_limit {
            let wal_path = self.root.join("indexes/latest-state.sqlite3-wal");
            let wal_size = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
            if wal_size > limit as u64 {
                self.database.busy_timeout(std::time::Duration::ZERO)?;
                let checkpoint = self
                    .database
                    .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)");
                self.database
                    .busy_timeout(std::time::Duration::from_secs(5))?;
                checkpoint?;
            }
            let wal_size = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
            ensure!(
                wal_size <= limit.checked_mul(4).context("WAL budget overflow")? as u64,
                "bounded history WAL budget exhausted: reader/checkpoint maintenance required"
            );
        }
        let mut recoveries = self.recoveries.clone();
        if terminal {
            for token in events[0]["canonical_payload"]["retired_token_ids"]
                .as_array()
                .context("missing retired terminal tokens")?
            {
                recoveries.remove(token.as_str().context("invalid retired token")?);
            }
        }
        for event in events {
            apply_token_recovery(&mut recoveries, event)?;
        }
        // Conservatively poison until log, index and manifest have all committed.
        self.poisoned = true;
        let started = std::time::Instant::now();
        let _publication = loop {
            match super::acquire_writer_lock(&self.root.join(".publication.lock")) {
                Ok(lock) => break lock,
                Err(super::RuntimeError::WriterAlreadyActive)
                    if started.elapsed().as_secs() < 5 =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(error) => return Err(error.into()),
            }
        };
        let mut offset = match &self.log {
            Some(log) => log.metadata()?.len(),
            None => 0,
        };
        let tx = self.database.transaction()?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS market_lifecycle(market_id TEXT PRIMARY KEY, cursor INTEGER NOT NULL, payload_json BLOB NOT NULL, payload_sha256 TEXT NOT NULL)")?;
        let mut values = metadata(&tx)?;
        for (event, line) in events.iter().zip(lines) {
            if self.history_limit.is_none() {
                self.log
                    .as_mut()
                    .context("legacy log missing")?
                    .write_all(line)?;
            }
            let market = required(event, "market_id")?;
            let token = event["token_id"].as_str();
            let event_cursor = event["cursor"]
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("missing cursor"))?;
            if self.history_limit.is_some() {
                tx.execute(
                    "INSERT INTO recent_events VALUES(?,?,?)",
                    params![
                        i64::try_from(event_cursor)?,
                        line,
                        hex::encode(Sha256::digest(line))
                    ],
                )?;
            } else {
                tx.execute(
                    "INSERT INTO event_offsets VALUES (?,?,?,?,?,?,?)",
                    params![
                        i64::try_from(event_cursor)?,
                        i64::try_from(offset)?,
                        i64::try_from(line.len())?,
                        required(event, "event_id")?,
                        market,
                        token,
                        hex::encode(Sha256::digest(line))
                    ],
                )?;
            }
            if terminal {
                let model = serde_json::to_vec(&event["canonical_payload"]["market"])?;
                tx.execute(
                    "INSERT OR REPLACE INTO market_lifecycle VALUES (?,?,?,?)",
                    params![
                        market,
                        i64::try_from(event_cursor)?,
                        model,
                        hex::encode(Sha256::digest(&model))
                    ],
                )?;
                offset += line.len() as u64;
                continue;
            }
            if is_token_recovery(event) {
                offset += line.len() as u64;
                continue;
            }
            let token = token.context("missing book token")?;
            let book = serde_json::to_vec(&event["canonical_payload"])?;
            tx.execute(
                "INSERT INTO books(cursor,payload_json,payload_sha256,token_id,market_id) VALUES (?,?,?,?,?) ON CONFLICT(token_id) DO UPDATE SET cursor=excluded.cursor,payload_json=excluded.payload_json,payload_sha256=excluded.payload_sha256",
                params![
                    i64::try_from(event_cursor)?,
                    book,
                    hex::encode(Sha256::digest(&book)),
                    token,
                    market
                ],
            )?;
            tx.execute("DELETE FROM book_confirmations WHERE token_id=?", [token])?;
            self.health.insert(
                token.to_owned(),
                BookHealth::from_book(
                    required(event, "market_id")?.to_owned(),
                    &event["canonical_payload"],
                )?,
            );
            offset += line.len() as u64;
        }
        if self.history_limit.is_none() {
            self.log
                .as_ref()
                .context("legacy log missing")?
                .sync_all()?;
        }
        // The table is bounded by the prepared token universe and holds only
        // current attempts. Historical transitions remain in the event log.
        for token in self
            .recoveries
            .keys()
            .filter(|token| !recoveries.contains_key(*token))
        {
            tx.execute(
                "DELETE FROM source_token_recoveries WHERE token_id=?",
                [token],
            )?;
        }
        for (token, recovery) in &recoveries {
            if self.recoveries.get(token) == Some(recovery) {
                continue;
            }
            let body = serde_json::to_vec(recovery)?;
            tx.execute(
                "INSERT OR REPLACE INTO source_token_recoveries VALUES (?,?,?)",
                params![token, body, hex::encode(Sha256::digest(&body))],
            )?;
        }
        values.insert("unresolved_gap_count".into(), recoveries.len().to_string());
        values.insert(
            "latest_cursor".into(),
            (cursor + events.len() as u64).to_string(),
        );
        if let Some(limit) = self.history_limit {
            let mut bytes: usize = values
                .get("recent_event_bytes")
                .context("missing history byte accounting")?
                .parse()?;
            bytes = bytes
                .checked_add(lines.iter().map(Vec::len).sum())
                .context("history size overflow")?;
            while bytes > limit {
                let (at, size): (i64, i64) = tx.query_row(
                    "SELECT cursor,length(payload) FROM recent_events ORDER BY cursor LIMIT 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                tx.execute("DELETE FROM recent_events WHERE cursor=?", [at])?;
                bytes -= usize::try_from(size)?;
                values.insert("history_floor_cursor".into(), at.to_string());
            }
            values.insert("recent_event_bytes".into(), bytes.to_string());
        } else {
            values.insert("event_log_size".into(), offset.to_string());
        }
        // Compute health from actual stored books, not from an assumed universe size.
        let mut oldest: Option<chrono::DateTime<chrono::Utc>> = None;
        let mut complete: BTreeMap<String, usize> = BTreeMap::new();
        for summary in self.health.values() {
            let at = summary.received_at;
            oldest = Some(oldest.map_or(at, |previous| previous.min(at)));
            if summary.complete {
                *complete.entry(summary.market_id.clone()).or_default() += 1;
            }
        }
        values.insert("book_token_count".into(), self.health.len().to_string());
        values.insert(
            "book_complete_market_count".into(),
            complete.values().filter(|n| **n == 2).count().to_string(),
        );
        if let Some(at) = oldest {
            values.insert("oldest_book_received_at".into(), at.to_rfc3339());
        } else {
            // Cold start can publish recovery facts before its first book;
            // retiring the final market also legitimately leaves no books.
            // Match the empty index representation without inventing freshness.
            values.insert("oldest_book_received_at".into(), String::new());
        }
        for (key, value) in &values {
            tx.execute("INSERT OR REPLACE INTO metadata VALUES (?,?)", [key, value])?;
        }
        tx.commit()?;
        self.recoveries = recoveries;
        self.write_index_manifest(&values)?;
        self.poisoned = false;
        Ok(())
    }

    fn write_index_manifest(&self, values:&BTreeMap<String,String>)->Result<()> {
        let mut manifest = json!({"schema_version":"marketcow.polymarket.state-index.v1",
            "path":self.root.join("indexes/latest-state.sqlite3")});
        for key in [
            "catalog_revision",
            "latest_cursor",
            "active_recovery_id",
            "book_token_count",
            "book_complete_market_count",
            "unresolved_gap_count",
            "oldest_book_received_at",
        ] {
            manifest[key] = json!(values[key]);
        }
        let temporary = self.root.join("state-index.rust-pending.json");
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        output.write_all(&serde_json::to_vec(&manifest)?)?;
        output.sync_all()?;
        fs::rename(&temporary, self.root.join("state-index.json"))?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketcow_polymarket::discovery_source::{SnapshotBoundary, snapshot_event};
    #[test]
    fn terminal_commit_preserves_books_and_reopens_at_contiguous_boundary() {
        let temp = fixture();
        let mut writer = PreparedSourceWriter::open(temp.path(), 65536).unwrap();
        writer
            .append_market_batch(&[event("11", 1), event("12", 2)])
            .unwrap();
        let mut start = event("11", 3);
        start["event_type"] = json!("recovery_started");
        start["canonical_payload"] = json!({"recovery_scope":"token","token_id":"11",
            "recovery_id":"terminal-attempt","reason":"initial_snapshot_required"});
        start["gaps"] = json!([{"token_id":"11","code":"coverage_gap","resolved":false,
            "detected_at":"2026-01-01T00:00:00Z","event_at":null,"expected":null,
            "observed":null,"resolution":null}]);
        rehash(&mut start);
        writer.append_market_batch(&[start]).unwrap();
        let mut terminal = event("11", 4);
        terminal["event_type"] = json!("market_terminal");
        terminal["token_id"] = Value::Null;
        terminal["canonical_payload"] = json!({"retired_token_ids":["11","12"],"market":{
            "identity":{"market_id":"1","condition_id":"condition","outcomes":[{"token_id":"11"},{"token_id":"12"}]},
            "lifecycle_state":"closed","closed":true,"accepting_orders":false,"resolution":null,
            "lifecycle_source":"polymarket_gamma","terminal_at":"2026-01-01T00:00:00Z","lifecycle_evidence_sha256":"a".repeat(64)}});
        terminal["canonical_payload_sha256"] =
            json!(canonical_hash(&terminal["canonical_payload"]));
        terminal.as_object_mut().unwrap().remove("event_id");
        terminal["event_id"] = json!(canonical_hash(&terminal));
        let mut resolved = terminal.clone();
        resolved["canonical_payload"]["market"]["lifecycle_state"] = json!("resolved");
        resolved["canonical_payload"]["market"]["resolution"] = json!("No");
        resolved["canonical_payload_sha256"] =
            json!(canonical_hash(&resolved["canonical_payload"]));
        resolved.as_object_mut().unwrap().remove("event_id");
        resolved["event_id"] = json!(canonical_hash(&resolved));
        let tokens = [("11".into(), "1".into()), ("12".into(), "1".into())].into();
        assert!(validate_source_events(&[resolved], true, 3, &tokens, 65536).is_ok());
        writer.append_terminal(&terminal).unwrap();
        assert_eq!(writer.cursor().unwrap(), 4);
        assert!(writer.recoveries.is_empty());
        let n: i64 = writer
            .database
            .query_row("SELECT count(*) FROM books", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(
            writer
                .database
                .query_row(
                    "SELECT cursor FROM market_lifecycle WHERE market_id='1'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            4
        );
        drop(writer);
        let reopened = PreparedSourceWriter::open(temp.path(), 65536).unwrap();
        assert_eq!(reopened.cursor().unwrap(), 4);
        assert!(reopened.recoveries.is_empty());
    }

    #[test]
    fn immutable_proof_commits_exact_lines_and_rejects_replay() {
        let temp = fixture();
        let tokens = [("11".into(), "1".into()), ("12".into(), "1".into())].into();
        let proof = ValidatedSourceBatch::new(
            vec![event("11", 1), event("12", 2)],
            false,
            0,
            &tokens,
            65536,
        )
        .unwrap();
        let expected = proof.lines.concat();
        let mut writer = PreparedSourceWriter::open(temp.path(), 65536).unwrap();
        writer.append_validated(&proof).unwrap();
        assert!(writer.append_validated(&proof).is_err());
        assert_eq!(
            fs::read(temp.path().join("events.jsonl")).unwrap(),
            expected
        );
        drop(writer);
        assert_eq!(
            PreparedSourceWriter::open(temp.path(), 65536)
                .unwrap()
                .cursor()
                .unwrap(),
            2
        );
    }
    #[test]
    fn grouped_proofs_preserve_bytes_boundaries_and_recovery() {
        let temp = fixture();
        let tokens = [("11".into(), "1".into()), ("12".into(), "1".into())].into();
        let a = ValidatedSourceBatch::new(
            vec![event("11", 1), event("12", 2)],
            false,
            0,
            &tokens,
            65536,
        )
        .unwrap();
        let b = ValidatedSourceBatch::new(
            vec![event("11", 3), event("12", 4)],
            false,
            2,
            &tokens,
            65536,
        )
        .unwrap();
        let mut writer = PreparedSourceWriter::open(temp.path(), 65536).unwrap();
        assert!(writer.append_validated_group(&[&a, &a]).is_err());
        assert_eq!(writer.cursor().unwrap(), 0);
        writer.append_validated_group(&[&a, &b]).unwrap();
        assert_eq!(
            fs::read(temp.path().join("events.jsonl")).unwrap(),
            [a.lines.concat(), b.lines.concat()].concat()
        );
        drop(writer);
        assert_eq!(
            PreparedSourceWriter::open(temp.path(), 65536)
                .unwrap()
                .cursor()
                .unwrap(),
            4
        );
    }

    #[test]
    fn token_recovery_persists_reopens_and_requires_its_snapshot() {
        let temp = fixture();
        let tokens = [("11".into(), "1".into()), ("12".into(), "1".into())].into();
        let mut start = event("11", 3);
        start["event_type"] = json!("recovery_started");
        start["canonical_payload"] = json!({"recovery_scope":"token","token_id":"11","recovery_id":"attempt1","reason":"source_data_delayed"});
        start["gaps"] = json!([{"token_id":"11","code":"coverage_gap","resolved":false,
            "detected_at":"2026-01-01T00:00:00Z","event_at":null,"expected":null,"observed":null,"resolution":null}]);
        rehash(&mut start);
        let mut writer = PreparedSourceWriter::open(temp.path(), 65536).unwrap();
        writer
            .append_market_batch(&[event("11", 1), event("12", 2)])
            .unwrap();
        let proof = ValidatedSourceBatch::new(vec![start], false, 2, &tokens, 65536).unwrap();
        writer.append_validated(&proof).unwrap();
        drop(writer);
        let mut writer = PreparedSourceWriter::open(temp.path(), 65536).unwrap();
        assert_eq!(writer.recoveries.len(), 1);
        let mut done = event("11", 4);
        done["event_type"] = json!("recovery_completed");
        done["canonical_payload"] = json!({"recovery_scope":"token","token_id":"11","recovery_id":"attempt1","snapshot_cursor":2,"resolved_gap_token_ids":["11"]});
        rehash(&mut done);
        let proof =
            ValidatedSourceBatch::new(vec![done.clone()], false, 3, &tokens, 65536).unwrap();
        assert!(writer.append_validated(&proof).is_err());
        assert_eq!(writer.cursor().unwrap(), 3);
        let snapshot = event("11", 4);
        let sibling = event("12", 5);
        let proof =
            ValidatedSourceBatch::new(vec![snapshot, sibling], false, 3, &tokens, 65536).unwrap();
        writer.append_validated(&proof).unwrap();
        done["cursor"] = json!(6);
        done["canonical_payload"]["snapshot_cursor"] = json!(4);
        rehash(&mut done);
        let proof = ValidatedSourceBatch::new(vec![done], false, 5, &tokens, 65536).unwrap();
        writer.append_validated(&proof).unwrap();
        drop(writer);
        let writer = PreparedSourceWriter::open(temp.path(), 65536).unwrap();
        assert!(writer.recoveries.is_empty());
        assert_eq!(writer.cursor().unwrap(), 6);
    }
    #[test]
    fn empty_bounded_candidate_reopens_without_fabricated_event() {
        let root = fixture();
        let db = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        db.execute_batch("DELETE FROM books; UPDATE metadata SET value='' WHERE key='active_recovery_id';").unwrap();
        drop(db);
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        writer.enable_bounded_history(16384).unwrap();
        assert_eq!(writer.cursor().unwrap(), 0);
        assert_eq!(writer.database.query_row("SELECT COUNT(*) FROM recent_events", [], |r| r.get::<_, i64>(0)).unwrap(), 0);
        drop(writer);
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        writer.bind_catalog_tokens(BTreeMap::from([("11".into(),"1".into()),("12".into(),"1".into())])).unwrap();
        writer.append_market_batch(&[event("11", 1), event("12", 2)]).unwrap();
        drop(writer);
        assert_eq!(PreparedSourceWriter::open(root.path(), 16384).unwrap().cursor().unwrap(), 2);
    }

    #[test]
    fn empty_bounded_migration_rejects_unpublished_books() {
        let root = fixture(); // legacy fixture has books but cursor zero
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        assert!(writer.enable_bounded_history(16384).unwrap_err().to_string().contains("empty migration"));
    }

    #[test]
    fn cold_recovery_persists_before_first_book() {
        let root = fixture();
        let db = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        db.execute_batch("DELETE FROM books; UPDATE metadata SET value='' WHERE key='active_recovery_id';").unwrap();
        drop(db);
        let tokens = BTreeMap::from([("11".into(),"1".into()),("12".into(),"1".into())]);
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        writer.bind_catalog_tokens(tokens.clone()).unwrap();
        writer.enable_bounded_history(16384).unwrap();
        let mut start = event("11", 1);
        start["event_type"] = json!("recovery_started");
        start["canonical_payload"] = json!({"recovery_scope":"token","token_id":"11","recovery_id":"cold1","reason":"missing_tick"});
        start["gaps"] = json!([{"token_id":"11","code":"coverage_gap","resolved":false,
            "detected_at":"2026-01-01T00:00:00Z","event_at":null,"expected":null,"observed":null,"resolution":null}]);
        rehash(&mut start);
        let proof = ValidatedSourceBatch::new(vec![start], false, 0, &tokens, 16384).unwrap();
        writer.append_validated(&proof).unwrap();
        assert_eq!(writer.health.len(), 0);
        assert_eq!(metadata(&writer.database).unwrap()["oldest_book_received_at"], "");
        drop(writer);
        let writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        assert_eq!(writer.cursor().unwrap(), 1);
        assert_eq!(writer.recoveries.len(), 1);
        assert!(writer.health.is_empty());
    }

    fn rehash(event: &mut Value) {
        event["canonical_payload_sha256"] = json!(canonical_hash(&event["canonical_payload"]));
        event["raw_payload_sha256"] = json!(canonical_hash(&event["raw_payload"]));
        event.as_object_mut().unwrap().remove("event_id");
        event["event_id"] = json!(canonical_hash(event));
    }

    fn event(token: &str, cursor: u64) -> Value {
        snapshot_event(
            &json!({"asset_id":token,"market":"condition","tick_size":"0.01",
            "timestamp":"1700000000000","hash":"source-hash",
            "bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
            SnapshotBoundary {
                market_id: "1",
                condition_id: "condition",
                token_id: token,
                recovery_id: "recovery-test",
                cursor,
                received_at: chrono::DateTime::from_timestamp(1700000001, 0).unwrap(),
            },
        )
        .unwrap()
    }
    fn fixture() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("indexes")).unwrap();
        File::create(root.path().join("events.jsonl")).unwrap();
        let db = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        db.execute_batch("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE gaps(resolved INTEGER NOT NULL);
            CREATE TABLE books(token_id TEXT PRIMARY KEY,market_id TEXT,cursor INTEGER,payload_json BLOB,payload_sha256 TEXT);
            CREATE TABLE book_confirmations(token_id TEXT PRIMARY KEY);
            CREATE TABLE event_offsets(cursor INTEGER PRIMARY KEY,byte_offset INTEGER,byte_length INTEGER,event_id TEXT,market_id TEXT,token_id TEXT,line_sha256 TEXT);").unwrap();
        for (key, value) in [
            ("schema_version", "marketcow.polymarket.state-index.v1"),
            ("latest_cursor", "0"),
            ("event_log_size", "0"),
            ("unresolved_gap_count", "0"),
            ("catalog_revision", "catalog-test"),
            ("active_recovery_id", "recovery-test"),
        ] {
            db.execute("INSERT INTO metadata VALUES (?,?)", [key, value])
                .unwrap();
        }
        for token in ["11", "12"] {
            let book = event(token, 1)["canonical_payload"].clone();
            db.execute(
                "INSERT INTO books VALUES (?,?,0,?,?)",
                params![
                    token,
                    "1",
                    serde_json::to_vec(&book).unwrap(),
                    canonical_hash(&book)
                ],
            )
            .unwrap();
        }
        root
    }

    #[test]
    fn reopen_retires_recovery_for_terminal_lifecycle_atomically() {
        let root = fixture();
        let db_path = root.path().join("indexes/latest-state.sqlite3");
        let db = Connection::open(&db_path).unwrap();
        db.execute_batch(
            "CREATE TABLE source_token_recoveries(token_id TEXT PRIMARY KEY,payload_json BLOB NOT NULL,payload_sha256 TEXT NOT NULL);
             CREATE TABLE market_lifecycle(market_id TEXT PRIMARY KEY,cursor INTEGER NOT NULL,payload_json BLOB NOT NULL,payload_sha256 TEXT NOT NULL);",
        )
        .unwrap();
        let recovery = json!({"token_id":"11","market_id":"1","condition_id":"condition",
            "recovery_id":"old","snapshot_cursor":null,
            "gap":{"token_id":"11","code":"coverage_gap","resolved":false,
                "detected_at":"2026-01-01T00:00:00Z"}});
        let recovery_body = serde_json::to_vec(&recovery).unwrap();
        db.execute(
            "INSERT INTO source_token_recoveries VALUES (?,?,?)",
            params!["11", recovery_body, hex::encode(Sha256::digest(&recovery_body))],
        )
        .unwrap();
        let lifecycle = json!({"identity":{"market_id":"1"},"lifecycle_state":"resolved"});
        let lifecycle_body = serde_json::to_vec(&lifecycle).unwrap();
        db.execute(
            "INSERT INTO market_lifecycle VALUES (?,?,?,?)",
            params!["1", 0_i64, lifecycle_body, hex::encode(Sha256::digest(&lifecycle_body))],
        )
        .unwrap();
        db.execute(
            "UPDATE metadata SET value='1' WHERE key='unresolved_gap_count'",
            [],
        )
        .unwrap();
        drop(db);

        let writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        assert!(writer.recoveries.is_empty());
        assert_eq!(metadata(&writer.database).unwrap()["unresolved_gap_count"], "0");
        let count: i64 = writer
            .database
            .query_row("SELECT count(*) FROM source_token_recoveries", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn bounded_history_crash_child() {
        let Some(path) = std::env::var_os("MARKETCOW_CRASH_TEST_ROOT") else { return };
        let mut writer = PreparedSourceWriter::open(Path::new(&path), 16384).unwrap();
        for _ in 0..20000 {
            let at = writer.cursor().unwrap();
            writer.append_market_batch(&[event("11", at+1), event("12", at+2)]).unwrap();
        }
    }

    #[test]
    // Run separately: fork briefly inherits other parallel tests' flock FDs
    // until exec closes them, so it must not race their immediate reopen checks.
    #[ignore = "run explicitly with --ignored --test-threads=1"]
    fn bounded_history_sigkill_reopens_committed_boundary_and_continues() {
        let root = fixture();
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        writer.append_market_batch(&[event("11",1),event("12",2)]).unwrap();
        writer.enable_bounded_history(16384).unwrap();
        drop(writer);
        fs::remove_file(root.path().join("events.jsonl")).unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","discovery_source::tests::bounded_history_crash_child","--nocapture"])
            .env("MARKETCOW_CRASH_TEST_ROOT",root.path())
            .stdout(std::process::Stdio::null()).spawn().unwrap();
        let db = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        let deadline = std::time::Instant::now()+std::time::Duration::from_secs(5);
        let progressed = loop {
            let at: String = db.query_row("SELECT value FROM metadata WHERE key='latest_cursor'",[],|r|r.get(0)).unwrap();
            if at.parse::<u64>().unwrap() >= 10 { break true; }
            if std::time::Instant::now() >= deadline { break false; }
            std::thread::sleep(std::time::Duration::from_millis(1));
        };
        child.kill().unwrap(); child.wait().unwrap();
        assert!(progressed,"child never committed");
        assert_eq!(db.query_row("PRAGMA integrity_check",[],|r|r.get::<_,String>(0)).unwrap(),"ok");
        let mut reopened = PreparedSourceWriter::open(root.path(),16384).unwrap();
        let at = reopened.cursor().unwrap();
        assert_eq!(at % 2,0,"atomic pair was split");
        reopened.append_market_batch(&[event("11",at+1),event("12",at+2)]).unwrap();
        assert_eq!(reopened.cursor().unwrap(),at+2);
        assert!(!root.path().join("events.jsonl").exists());
    }

    #[test]
    fn bounded_history_partial_manifest_is_not_a_recovery_boundary() {
        let root = fixture();
        let mut writer = PreparedSourceWriter::open(root.path(),16384).unwrap();
        writer.append_market_batch(&[event("11",1),event("12",2)]).unwrap();
        writer.enable_bounded_history(16384).unwrap();
        drop(writer);
        fs::write(root.path().join("state-index.rust-pending.json"),b"{\"latest_cursor\":999").unwrap();
        let mut writer = PreparedSourceWriter::open(root.path(),16384).unwrap();
        assert_eq!(writer.cursor().unwrap(),2);
        assert!(!root.path().join("state-index.rust-pending.json").exists());
        writer.append_market_batch(&[event("11",3),event("12",4)]).unwrap();
        assert_eq!(writer.cursor().unwrap(),4);
    }

    #[test]
    fn bounded_history_failed_transaction_keeps_previous_boundary() {
        let root = fixture();
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        writer
            .append_market_batch(&[event("11", 1), event("12", 2)])
            .unwrap();
        writer.enable_bounded_history(16384).unwrap();
        writer.database.execute_batch("CREATE TRIGGER injected_failure BEFORE UPDATE ON books BEGIN SELECT RAISE(ABORT,'injected failure'); END;").unwrap();
        assert!(
            writer
                .append_market_batch(&[event("11", 3), event("12", 4)])
                .is_err()
        );
        assert_eq!(writer.cursor().unwrap(), 2);
        assert_eq!(
            writer
                .database
                .query_row("SELECT MAX(cursor) FROM recent_events", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        drop(writer);
        assert_eq!(
            PreparedSourceWriter::open(root.path(), 16384)
                .unwrap()
                .cursor()
                .unwrap(),
            2
        );
    }

    #[test]
    fn bounded_history_pinned_reader_hits_wal_budget_without_advancing() {
        let root = fixture();
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        writer
            .append_market_batch(&[event("11", 1), event("12", 2)])
            .unwrap();
        writer.enable_bounded_history(16384).unwrap();
        writer
            .database
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        let reader = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        reader.execute_batch("BEGIN; SELECT * FROM books;").unwrap();
        let mut stopped = false;
        for _ in 0..100 {
            let at = writer.cursor().unwrap();
            if let Err(error) =
                writer.append_market_batch(&[event("11", at + 1), event("12", at + 2)])
            {
                assert!(error.to_string().contains("WAL budget"), "{error}");
                assert_eq!(writer.cursor().unwrap(), at);
                stopped = true;
                break;
            }
        }
        assert!(stopped);
        reader.execute_batch("ROLLBACK").unwrap();
        let at = writer.cursor().unwrap();
        writer
            .append_market_batch(&[event("11", at + 1), event("12", at + 2)])
            .unwrap();
        assert_eq!(writer.cursor().unwrap(), at + 2);
    }

    #[test]
    fn bounded_history_keeps_latest_state_without_appending_legacy_log() {
        let root = fixture();
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        writer
            .append_market_batch(&[event("11", 1), event("12", 2)])
            .unwrap();
        let log_size = fs::metadata(root.path().join("events.jsonl"))
            .unwrap()
            .len();
        writer.enable_bounded_history(16384).unwrap();
        for after in (2..100).step_by(2) {
            writer
                .append_market_batch(&[event("11", after + 1), event("12", after + 2)])
                .unwrap();
        }
        let values = metadata(&writer.database).unwrap();
        assert!(values["recent_event_bytes"].parse::<usize>().unwrap() <= 16384);
        assert!(values["history_floor_cursor"].parse::<u64>().unwrap() > 2);
        assert_eq!(writer.cursor().unwrap(), 100);
        assert_eq!(
            fs::metadata(root.path().join("events.jsonl"))
                .unwrap()
                .len(),
            log_size
        );
        assert_eq!(
            writer
                .database
                .query_row("SELECT count(*) FROM books", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        drop(writer);
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        writer.enable_bounded_history(16384).unwrap();
        assert!(writer.enable_bounded_history(32768).is_err());
        fs::remove_file(root.path().join("events.jsonl")).unwrap();
        writer
            .append_market_batch(&[event("11", 101), event("12", 102)])
            .unwrap();
        assert_eq!(writer.cursor().unwrap(), 102);
        writer
            .database
            .execute(
                "UPDATE recent_events SET payload=x'00' WHERE cursor=102",
                [],
            )
            .unwrap();
        drop(writer);
        assert!(PreparedSourceWriter::open(root.path(), 16384).is_err());
    }

    #[test]
    fn commits_contiguous_pair_and_reopens_verified_tail() {
        let root = fixture();
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        assert_eq!(
            writer
                .database
                .query_row(
                    "SELECT count(*) FROM sqlite_master \
                     WHERE type='table' AND name='market_lifecycle'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
        assert!(PreparedSourceWriter::open(root.path(), 16384).is_err());
        writer
            .append_market_batch(&[event("11", 1), event("12", 2)])
            .unwrap();
        assert_eq!(writer.cursor().unwrap(), 2);
        let manifest: Value =
            serde_json::from_slice(&fs::read(root.path().join("state-index.json")).unwrap())
                .unwrap();
        assert_eq!(manifest["latest_cursor"], "2");
        assert_eq!(manifest["book_complete_market_count"], "1");
        drop(writer);
        assert_eq!(
            PreparedSourceWriter::open(root.path(), 16384)
                .unwrap()
                .cursor()
                .unwrap(),
            2
        );
    }
    #[test]
    fn health_summary_is_token_bounded_and_rebuilt_after_restart() {
        let root = fixture();
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        for cursor in [1, 3, 5] {
            writer
                .append_market_batch(&[event("11", cursor), event("12", cursor + 1)])
                .unwrap();
            assert_eq!(writer.health.len(), 2);
        }
        let before = metadata(&writer.database).unwrap();
        drop(writer);
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        assert_eq!(writer.health.len(), 2);
        writer
            .append_market_batch(&[event("11", 7), event("12", 8)])
            .unwrap();
        let after = metadata(&writer.database).unwrap();
        for key in [
            "book_token_count",
            "book_complete_market_count",
            "oldest_book_received_at",
        ] {
            assert_eq!(before[key], after[key]);
        }
        let incomplete = BookHealth::from_book(
            "1".into(),
            &json!({
                "received_at":"2023-11-14T22:13:21Z","bids":[],"asks":[]
            }),
        )
        .unwrap();
        assert!(!incomplete.complete);
        assert!(BookHealth::from_book("1".into(), &json!({"bids":[],"asks":[]})).is_err());
    }

    #[test]
    fn missing_catalog_token_is_created_only_with_complete_authoritative_pair() {
        let root = fixture();
        let db = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        db.execute("DELETE FROM books WHERE token_id='12'", [])
            .unwrap();
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        writer
            .bind_catalog_tokens(BTreeMap::from([("12".into(), "1".into())]))
            .unwrap();
        assert_eq!(
            db.query_row("SELECT count(*) FROM books", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert!(
            writer
                .bind_catalog_tokens(BTreeMap::from([("11".into(), "other".into())]))
                .is_err()
        );
        assert!(writer.append_market_batch(&[event("12", 1)]).is_err());
        writer
            .append_market_batch(&[event("11", 1), event("12", 2)])
            .unwrap();
        assert_eq!(
            db.query_row("SELECT count(*) FROM books", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        let oversized = (0..=MAX_SCOPED_TOKEN_IDENTITIES)
            .map(|index| (format!("token-{index}"), format!("market-{index}")))
            .collect();
        assert!(writer.bind_catalog_tokens(oversized).is_err());
    }

    #[test]
    fn repeated_small_bindings_cannot_bypass_total_token_limit() {
        let root=fixture();
        let mut writer=PreparedSourceWriter::open(root.path(),16384).unwrap();
        let remaining=MAX_SCOPED_TOKEN_IDENTITIES-writer.token_markets.len();
        let additions:BTreeMap<_,_>=(0..remaining).map(|n|(format!("extra-{n}"),"market".into())).collect();
        writer.bind_catalog_tokens(additions.clone()).unwrap();
        writer.bind_catalog_tokens(additions).unwrap(); // Same identities are idempotent.
        let before=writer.token_markets.clone();
        assert!(writer.bind_catalog_tokens(BTreeMap::from([("one-too-many".into(),"market".into())])).is_err());
        assert_eq!(writer.token_markets,before);
        assert_eq!(writer.cursor().unwrap(),0);
    }

    #[test]
    fn scope_retirement_releases_current_rows_preserves_history_and_reopens() {
        let root=fixture();
        let mut writer=PreparedSourceWriter::open(root.path(),16384).unwrap();
        writer.append_market_batch(&[event("11",1),event("12",2)]).unwrap();
        writer.enable_bounded_history(16384).unwrap();
        let before=writer.database.query_row("SELECT group_concat(payload_sha256) FROM books",[],|r|r.get::<_,String>(0)).unwrap();
        let history=writer.database.query_row("SELECT count(*) FROM recent_events",[],|r|r.get::<_,i64>(0)).unwrap();
        let markets=BTreeSet::from(["1".into()]);
        assert!(writer.retire_catalog_markets(1,&markets).is_err());
        assert_eq!(before,writer.database.query_row("SELECT group_concat(payload_sha256) FROM books",[],|r|r.get::<_,String>(0)).unwrap());
        writer.retire_catalog_markets(2,&markets).unwrap();
        assert_eq!(writer.cursor().unwrap(),2);
        assert_eq!(writer.database.query_row("SELECT count(*) FROM books",[],|r|r.get::<_,i64>(0)).unwrap(),0);
        assert_eq!(history,writer.database.query_row("SELECT count(*) FROM recent_events",[],|r|r.get::<_,i64>(0)).unwrap());
        assert!(writer.token_markets.is_empty());
        assert_eq!(metadata(&writer.database).unwrap()["book_token_count"],"0");
        drop(writer);
        let mut writer=PreparedSourceWriter::open(root.path(),16384).unwrap();
        assert_eq!(writer.cursor().unwrap(),2);
        writer.bind_catalog_tokens(BTreeMap::from([("11".into(),"1".into()),("12".into(),"1".into())])).unwrap();
        writer.append_market_batch(&[event("11",3),event("12",4)]).unwrap();
        assert_eq!(writer.cursor().unwrap(),4);
    }

    #[test]
    fn partial_market_bad_cursor_and_budget_never_append() {
        let root = fixture();
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        assert!(writer.append_market_batch(&[event("11", 1)]).is_err());
        assert!(
            writer
                .append_market_batch(&[event("11", 2), event("12", 3)])
                .is_err()
        );
        let mut tampered = event("12", 2);
        tampered["raw_payload"]["hash"] = json!("changed");
        assert!(
            writer
                .append_market_batch(&[event("11", 1), tampered])
                .is_err()
        );
        assert_eq!(writer.log.as_ref().unwrap().metadata().unwrap().len(), 0);
        drop(writer);
        let mut limited = PreparedSourceWriter::open(root.path(), 10).unwrap();
        assert!(
            limited
                .append_market_batch(&[event("11", 1), event("12", 2)])
                .is_err()
        );
        assert_eq!(limited.log.as_ref().unwrap().metadata().unwrap().len(), 0);
    }
    #[test]
    fn unindexed_tail_requires_recovery_without_truncation() {
        let root = fixture();
        fs::write(root.path().join("events.jsonl"), b"unindexed tail").unwrap();
        assert!(PreparedSourceWriter::open(root.path(), 16384).is_err());
        assert_eq!(
            fs::read(root.path().join("events.jsonl")).unwrap(),
            b"unindexed tail"
        );
    }

    #[test]
    fn corrupt_or_semantically_incomplete_recovery_state_never_reopens() {
        let root = fixture();
        drop(PreparedSourceWriter::open(root.path(), 65536).unwrap());
        let database = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        let invalid = serde_json::to_vec(&json!({"token_id":"11","market_id":"1","condition_id":"condition",
            "recovery_id":"","snapshot_cursor":null,"gap":{"token_id":"11","code":"coverage_gap","resolved":false,
                "detected_at":"2026-01-01T00:00:00Z"}})).unwrap();
        database
            .execute(
                "INSERT INTO source_token_recoveries VALUES (?,?,?)",
                params!["11", invalid, hex::encode(Sha256::digest(&invalid))],
            )
            .unwrap();
        drop(database);
        let error = match PreparedSourceWriter::open(root.path(), 65536) {
            Ok(_) => panic!("invalid recovery reopened"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("recovery identity"));
    }
}
