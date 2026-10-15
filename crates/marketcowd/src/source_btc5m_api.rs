//! Bounded BTC five-minute market-local read surface over MarketCow's
//! per-market publisher and replay lanes. It owns no upstream connection or
//! persistence path.
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use marketcow_polymarket::discovery_source::canonical_hash;
use marketcow_runtime::discovery_source::{DurableMarketPageBoundary, read_durable_market_page};
use marketcow_contracts::btc5m_market_local_v5::{self as market_proto, wire as proto};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex, RwLock, atomic::{AtomicU64, Ordering}},
    time::Duration,
};
use tokio::sync::{Semaphore, watch};

use crate::source_btc5m_proto as proto_convert;
use crate::source_publication::{Batch, MarketMemorySnapshot, MemoryReader, SourceBatchTiming};

const PROFILE_SCHEMA: &str = "marketcow.polymarket.btc-5m-profile.v1";
const STATUS_SCHEMA: &str = "marketcow.polymarket.btc-5m-status.v5";
const ERROR_SCHEMA: &str = "marketcow.polymarket.btc-5m-error.v1";
const MARKET_PROTO_SUBPROTOCOL: &str = "marketcow.btc5m.market-local.v5";
// Fixed v1 management-contract TTL, not an implicit runtime fallback.
const MANAGER_STALE_AFTER_SECONDS: i64 = 60;
const MAX_MARKETS: usize = 3;
const MAX_STREAM_CLIENTS: usize = 12;

fn market_stream_client_limit(total_clients: usize) -> usize {
    total_clients.div_ceil(MAX_MARKETS).max(1)
}
const MAX_TOKENS: usize = 6;
const TIMELINE_RECEIPT_BYTES: usize = 16 * 1024 * 1024;
const TIMELINE_RECEIPT_COUNT: usize = 16_384;
// This is a diagnostic threshold, not a readiness or trading freshness rule.
const SLOW_GROUP_THRESHOLD_US: i64 = 100_000;
const SLOW_GROUP_QUEUE: usize = 256;
// Bound independent confirmation control frames without delaying book groups.
const CONFIRMATION_MIN_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Default)]
struct TimelineArchive {
    entries: VecDeque<(String, String, String, u64, String, Value, usize)>,
    bytes: usize,
}
struct MarketTimelineGroup {
    profile_epoch: String,
    market_id: String,
    stream_epoch: String,
    market_cursor: u64,
    group: Arc<Batch>,
    group_commit_sent_at: DateTime<Utc>,
}
struct SlowMarketGroup {
    connection_id: String,
    profile_epoch: String,
    market_id: String,
    stream_epoch: String,
    market_cursor: u64,
    market_event_cursor: u64,
    event_id: String,
    event_count: usize,
    group_commit_sent_at: DateTime<Utc>,
    raw_last_to_send_us: i64,
    timing: SourceBatchTiming,
}

fn slow_group_elapsed_us(timing: &SourceBatchTiming, sent_at: DateTime<Utc>) -> Option<i64> {
    let elapsed = sent_at.signed_duration_since(timing.raw_batch_last_received_at)
        .num_microseconds()?;
    (elapsed >= SLOW_GROUP_THRESHOLD_US).then_some(elapsed)
}

fn slow_group_json(group: &SlowMarketGroup) -> Value {
    json!({
        "stage":"btc_5m_slow_market_group",
        "threshold_us":SLOW_GROUP_THRESHOLD_US,
        "connection_id":group.connection_id,
        "profile_epoch":group.profile_epoch,
        "market_id":group.market_id,
        "stream_epoch":group.stream_epoch,
        "market_cursor":group.market_cursor,
        "market_event_cursor":group.market_event_cursor,
        "event_id":group.event_id,
        "event_count":group.event_count,
        "raw_last_to_group_commit_sent_us":group.raw_last_to_send_us,
        "group_commit_sent_at":group.group_commit_sent_at,
        "source_batch_timing":source_timing_json(&group.timing),
        "clock_error_bound_ms":Value::Null,
    })
}
impl TimelineArchive {
    fn push(&mut self, epoch: &str, market_id: &str, stream_epoch: &str,
        event_cursor: u64, event_id: &str, receipt: Value) {
        let Ok(bytes) = serde_json::to_vec(&receipt).map(|v| v.len()) else { return; };
        if bytes > TIMELINE_RECEIPT_BYTES { return; }
        self.bytes += bytes;
        self.entries.push_back((epoch.to_owned(), market_id.to_owned(),
            stream_epoch.to_owned(), event_cursor, event_id.to_owned(), receipt, bytes));
        while self.entries.len() > TIMELINE_RECEIPT_COUNT || self.bytes > TIMELINE_RECEIPT_BYTES {
            if let Some((_, _, _, _, _, _, evicted_bytes)) = self.entries.pop_front() {
                self.bytes -= evicted_bytes;
            }
        }
    }
    fn lookup(&self, epoch: &str, market_id: &str, stream_epoch: &str,
        event_cursor: u64, event_id: &str) -> Vec<Value> {
        self.entries.iter().filter(|(e, m, s, c, id, _, _)|
            e == epoch && m == market_id && s == stream_epoch
                && *c == event_cursor && id == event_id
        ).take(8).map(|(_, _, _, _, _, receipt, _)| receipt.clone()).collect()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum WindowRole {
    Current,
    Next,
    Retiring,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct OutcomeBinding {
    pub outcome: String,
    pub token_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketBinding {
    pub role: WindowRole,
    pub window_id: String,
    pub start_at: String,
    pub end_at: String,
    pub market_id: String,
    pub condition_id: String,
    pub outcomes: [OutcomeBinding; 2],
    pub rule_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub schema_version: String,
    pub profile_id: String,
    pub profile_revision: u64,
    pub catalog_revision: String,
    pub markets: Vec<MarketBinding>,
    pub maximum_markets: usize,
    pub maximum_quote_age_ms: u64,
    pub maximum_tokens: usize,
}

fn hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl Profile {
    fn identity_value(&self) -> Value {
        json!({"profile_revision":self.profile_revision,"catalog_revision":self.catalog_revision,
            "markets":self.markets,"maximum_markets":self.maximum_markets,
            "maximum_quote_age_ms":self.maximum_quote_age_ms,
            "maximum_tokens":self.maximum_tokens})
    }
    pub fn expected_id(&self) -> String {
        canonical_hash(&self.identity_value())
    }
    pub fn document_sha256(&self) -> String {
        canonical_hash(&serde_json::to_value(self).expect("BTC 5m profile serializes"))
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == PROFILE_SCHEMA,
            "BTC 5m profile schema"
        );
        ensure!(
            self.profile_revision > 0 && hex64(&self.catalog_revision),
            "BTC 5m profile revision"
        );
        ensure!(
            self.maximum_markets == MAX_MARKETS && self.maximum_tokens == MAX_TOKENS,
            "BTC 5m fixed resource profile"
        );
        ensure!(
            (100..=60_000).contains(&self.maximum_quote_age_ms),
            "BTC 5m quote age budget"
        );
        ensure!(
            (1..=MAX_MARKETS).contains(&self.markets.len()),
            "BTC 5m market budget"
        );
        ensure!(
            self.profile_id == self.expected_id(),
            "BTC 5m profile identity"
        );
        let mut roles = BTreeSet::new();
        let mut markets = BTreeSet::new();
        let mut tokens = BTreeSet::new();
        let mut windows = BTreeSet::new();
        for market in &self.markets {
            ensure!(
                roles.insert(market.role.clone()),
                "duplicate BTC 5m window role"
            );
            ensure!(
                markets.insert(&market.market_id) && windows.insert(&market.window_id),
                "duplicate BTC 5m market/window"
            );
            ensure!(
                !market.market_id.is_empty()
                    && market.condition_id.len() == 66
                    && market.condition_id.starts_with("0x")
                    && hex64(&market.condition_id[2..])
                    && hex64(&market.rule_sha256),
                "BTC 5m market identity"
            );
            let start = DateTime::parse_from_rfc3339(&market.start_at)?;
            let end = DateTime::parse_from_rfc3339(&market.end_at)?;
            ensure!(
                end.signed_duration_since(start).num_seconds() == 300,
                "BTC 5m window must be exactly five minutes"
            );
            ensure!(
                market.outcomes[0].outcome == "Up" && market.outcomes[1].outcome == "Down",
                "BTC 5m ordered Up/Down identity"
            );
            ensure!(
                market
                    .outcomes
                    .iter()
                    .all(|o| !o.token_id.is_empty() && tokens.insert(&o.token_id)),
                "duplicate/empty BTC 5m token"
            );
        }
        let current = self
            .markets
            .iter()
            .find(|market| market.role == WindowRole::Current)
            .context("BTC 5m current window required")?;
        if let Some(next) = self
            .markets
            .iter()
            .find(|market| market.role == WindowRole::Next)
        {
            ensure!(
                next.start_at == current.end_at,
                "BTC 5m next window must be adjacent"
            );
        }
        if let Some(retiring) = self
            .markets
            .iter()
            .find(|market| market.role == WindowRole::Retiring)
        {
            ensure!(
                retiring.end_at == current.start_at,
                "BTC 5m retiring window must be adjacent"
            );
        }
        ensure!(
            roles.contains(&WindowRole::Current),
            "BTC 5m current window required"
        );
        ensure!(tokens.len() <= MAX_TOKENS, "BTC 5m token budget");
        Ok(())
    }
    /// Recheck the wall-clock-sensitive part of a profile at the actual
    /// control-plane mutation boundary. A manager-side review can become
    /// stale while discovery, preparation, or the control request is queued.
    pub fn validate_current_at(&self, now: DateTime<Utc>) -> Result<()> {
        self.validate()?;
        let current = self
            .window(WindowRole::Current)
            .context("BTC 5m current window")?;
        let start = DateTime::parse_from_rfc3339(&current.start_at)?.with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339(&current.end_at)?.with_timezone(&Utc);
        ensure!(
            start <= now && now < end,
            "BTC 5m profile current window is not active"
        );
        Ok(())
    }
    fn market_ids(&self) -> BTreeSet<&str> {
        self.markets.iter().map(|m| m.market_id.as_str()).collect()
    }
    fn window(&self, role: WindowRole) -> Option<&MarketBinding> {
        self.markets.iter().find(|market| market.role == role)
    }
    fn same_market_identity(left: &MarketBinding, right: &MarketBinding) -> bool {
        left.window_id == right.window_id
            && left.start_at == right.start_at
            && left.end_at == right.end_at
            && left.market_id == right.market_id
            && left.condition_id == right.condition_id
            && left.outcomes == right.outcomes
            && left.rule_sha256 == right.rule_sha256
    }
    /// A steady-state rotation reuses the already subscribed `next` as the
    /// new `current` and adds exactly one new `next`. A caller may retain the
    /// old `current` as `retiring`, but the autonomous manager intentionally
    /// uses the two-window form so a closed market never has to be
    /// re-subscribed after restart. Both forms are one-market replacements
    /// and cannot introduce an unrelated fourth market.
    pub fn validate_rotation_from(&self, incumbent: &Self) -> Result<()> {
        self.validate()?;
        incumbent.validate()?;
        ensure!(
            incumbent.markets.len() == self.markets.len()
                && matches!(self.markets.len(), 2 | MAX_MARKETS),
            "BTC 5m steady-state rotation requires equal two/three-window profiles"
        );
        let old_current = incumbent
            .window(WindowRole::Current)
            .context("incumbent current window")?;
        let old_next = incumbent
            .window(WindowRole::Next)
            .context("incumbent next window")?;
        let new_current = self
            .window(WindowRole::Current)
            .context("candidate current window")?;
        let new_next = self
            .window(WindowRole::Next)
            .context("candidate next window")?;
        if self.markets.len() == MAX_MARKETS {
            let new_retiring = self
                .window(WindowRole::Retiring)
                .context("candidate retiring window")?;
            ensure!(
                Self::same_market_identity(old_current, new_retiring),
                "BTC 5m candidate retiring must equal incumbent current"
            );
        } else {
            ensure!(
                self.window(WindowRole::Retiring).is_none()
                    && incumbent.window(WindowRole::Retiring).is_none(),
                "BTC 5m two-window rotation cannot retain retiring"
            );
        }
        ensure!(
            Self::same_market_identity(old_next, new_current),
            "BTC 5m candidate current must equal incumbent next"
        );
        ensure!(
            new_next.start_at == new_current.end_at
                && !incumbent.market_ids().contains(new_next.market_id.as_str()),
            "BTC 5m candidate must add one adjacent new next market"
        );
        Ok(())
    }
    pub fn market_ids_owned(&self) -> BTreeSet<String> {
        self.markets.iter().map(|m| m.market_id.clone()).collect()
    }
}

#[derive(Clone)]
pub struct Limits {
    pub live_backlog_groups: usize,
    pub live_backlog_age: Duration,
    pub snapshot_bytes: usize,
    pub frame_bytes: usize,
    pub replay_bytes: usize,
    pub clients: usize,
    pub send_timeout: Duration,
    pub snapshot_lease_capacity: usize,
}
impl Limits {
    fn validate(&self) -> Result<()> {
        ensure!((1..=4096).contains(&self.live_backlog_groups)
            && (10..=10_000).contains(&self.live_backlog_age.as_millis()),
            "BTC 5m live delivery budget");
        ensure!(
            (64 * 1024..=16 * 1024 * 1024).contains(&self.snapshot_bytes),
            "BTC 5m snapshot bytes"
        );
        ensure!(
            (4 * 1024..=2 * 1024 * 1024).contains(&self.frame_bytes),
            "BTC 5m frame bytes"
        );
        ensure!(
            (64 * 1024..=8 * 1024 * 1024).contains(&self.replay_bytes),
            "BTC 5m replay bytes"
        );
        ensure!(
            (1..=MAX_STREAM_CLIENTS).contains(&self.clients)
                && (1..=4).contains(&self.snapshot_lease_capacity)
                && !self.send_timeout.is_zero(),
            "BTC 5m client budget"
        );
        Ok(())
    }
}

fn instant(value: &Value, key: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(
        value[key]
            .as_str()
            .with_context(|| format!("missing {key}"))?,
    )?
    .with_timezone(&Utc))
}
fn levels<'a>(book: &'a Value, side: &str) -> Result<&'a Vec<Value>> {
    book[side]
        .as_array()
        .with_context(|| format!("missing {side}"))
}
fn best_price(book: &Value, side: &str, maximum: bool) -> Result<Option<String>> {
    let mut values = Vec::new();
    for level in levels(book, side)? {
        let raw = level["price"].as_str().context("book level price")?;
        values.push((raw.parse::<rust_decimal::Decimal>()?, raw.to_owned()));
    }
    values.sort_by_key(|v| v.0);
    Ok(if maximum {
        values.last()
    } else {
        values.first()
    }
    .map(|v| v.1.clone()))
}

#[derive(Clone)]
pub struct Api {
    reader: MemoryReader,
    control: Control,
    limits: Limits,
    archive_root: Option<PathBuf>,
    snapshots: Arc<Semaphore>,
    streams: Arc<Semaphore>,
    market_stream_slots: Arc<Mutex<BTreeMap<String, Arc<Semaphore>>>>,
    timeline: Arc<Mutex<TimelineArchive>>,
    timeline_tx: std::sync::mpsc::SyncSender<MarketTimelineGroup>,
    slow_group_tx: std::sync::mpsc::SyncSender<SlowMarketGroup>,
    slow_group_dropped: Arc<AtomicU64>,
    send_diagnostics: crate::source_market_send_diagnostic::Diagnostics,
}
#[derive(Clone)]
struct Active {
    profile: Arc<Profile>,
    epoch: String,
}
#[derive(Clone)]
pub struct Control {
    reader: MemoryReader,
    live: crate::source_market_live::Registry,
    process_instance_id: String,
    active: Arc<RwLock<Active>>,
    changed: watch::Sender<u64>,
    manager: Arc<RwLock<Option<Value>>>,
}
impl Control {
    pub fn live_identity(&self, binding: &MarketBinding) -> Result<(marketcow_contracts::btc5m_market_local_v6::wire::MarketKey,String)> {
        self.live.identity(binding)
    }
    pub fn profile_for_market_source(&self) -> Result<(Arc<Profile>, String)> {
        let active = self.active()?;
        Ok((active.profile, active.epoch))
    }

    pub fn profile_with_watch_for_market_source(&self)
        -> Result<(Arc<Profile>, String, watch::Receiver<u64>)> {
        let (active, changed) = self.active_with_watch()?;
        Ok((active.profile, active.epoch, changed))
    }

    fn active(&self) -> Result<Active> {
        Ok(self
            .active
            .read()
            .map_err(|_| anyhow::anyhow!("BTC 5m profile poisoned"))?
            .clone())
    }
    fn active_with_watch(&self) -> Result<(Active, watch::Receiver<u64>)> {
        loop {
            let receiver = self.changed.subscribe();
            let active = self.active()?;
            if *receiver.borrow() == active.profile.profile_revision {
                return Ok((active, receiver));
            }
        }
    }
    pub fn status(&self) -> Result<Value> {
        let active = self.active()?;
        Ok(json!({"profile_id":active.profile.profile_id,
            "profile_revision":active.profile.profile_revision,
            "profile_document_sha256":active.profile.document_sha256(),
            "profile":active.profile,"epoch":active.epoch,
            "manager":self.manager_status()?}))
    }
    pub fn set_manager_status(&self, status: Value) -> Result<()> {
        *self
            .manager
            .write()
            .map_err(|_| anyhow::anyhow!("BTC 5m manager status poisoned"))? = Some(status);
        Ok(())
    }
    /// A successful lease renewal is an actual manager update. Touch the
    /// existing status under the same lock so a concurrent fail-closed phase
    /// cannot be overwritten by an older cloned "running" status.
    pub fn note_manager_renewal_at(&self, at: DateTime<Utc>) -> Result<()> {
        let mut manager = self.manager.write()
            .map_err(|_| anyhow::anyhow!("BTC 5m manager status poisoned"))?;
        let status = manager.as_mut().context("BTC 5m manager status missing")?;
        ensure!(status["schema_version"] == "marketcow.polymarket.btc-5m-manager-status.v1",
            "BTC 5m manager status schema");
        status["updated_at"] = json!(at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
        Ok(())
    }
    pub fn manager_status(&self) -> Result<Value> {
        Ok(self
            .manager
            .read()
            .map_err(|_| anyhow::anyhow!("BTC 5m manager status poisoned"))?
            .clone()
            .unwrap_or(Value::Null))
    }
    pub fn referenced_markets(&self) -> Result<BTreeSet<String>> {
        Ok(self
            .active()?
            .profile
            .markets
            .iter()
            .map(|market| market.market_id.clone())
            .collect())
    }
    pub fn profile(&self) -> Result<Profile> {
        Ok((*self.active()?.profile).clone())
    }
    /// Publish only after the caller has prepared acquisition for every
    /// referenced market through the existing managed control plane.
    pub fn publish(
        &self,
        expected_profile_id: &str,
        expected_revision: u64,
        profile: Profile,
    ) -> Result<Value> {
        self.publish_inner(expected_profile_id, expected_revision, profile, true)
    }
    /// Re-establish a fresh profile after process restart. Acquisition leases
    /// are deliberately process-local, so the caller must first prove there is
    /// no incumbent manager lease and acquire the candidate's exact component
    /// subset. The acquisition owner derives the deduplicated shared union
    /// across this and every other consumer lease.
    pub fn publish_restart_reconciled(
        &self,
        expected_profile_id: &str,
        expected_revision: u64,
        profile: Profile,
    ) -> Result<Value> {
        self.publish_inner(expected_profile_id, expected_revision, profile, false)
    }
    fn publish_inner(
        &self,
        expected_profile_id: &str,
        expected_revision: u64,
        profile: Profile,
        require_adjacent_rotation: bool,
    ) -> Result<Value> {
        profile.validate()?;
        let installed = self.reader.installed_markets()?;
        for market in &profile.markets {
            let record = installed
                .get(&market.market_id)
                .context("BTC 5m market is not acquisition-prepared")?;
            ensure!(
                record["identity"]["condition_id"] == market.condition_id,
                "BTC 5m prepared condition differs"
            );
            let outcomes = record["identity"]["outcomes"]
                .as_array()
                .context("BTC 5m prepared outcomes")?;
            ensure!(
                outcomes.len() == 2
                    && outcomes[0]["outcome"] == "Up"
                    && outcomes[0]["token_id"] == market.outcomes[0].token_id
                    && outcomes[1]["outcome"] == "Down"
                    && outcomes[1]["token_id"] == market.outcomes[1].token_id,
                "BTC 5m prepared ordered outcomes differ"
            );
        }
        self.commit_profile_at(expected_profile_id,expected_revision,profile,
            require_adjacent_rotation,Utc::now())
    }
    /// Final in-process CAS. The real route supplies Utc::now() only after
    /// prepared market identity checks; a fixed clock lets the same mutation
    /// boundary be exercised over many deterministic UTC rotations offline.
    fn commit_profile_at(
        &self,
        expected_profile_id: &str,
        expected_revision: u64,
        profile: Profile,
        require_adjacent_rotation: bool,
        now: DateTime<Utc>,
    ) -> Result<Value> {
        let mut active = self
            .active
            .write()
            .map_err(|_| anyhow::anyhow!("BTC 5m profile poisoned"))?;
        ensure!(
            active.profile.profile_id == expected_profile_id
                && active.profile.profile_revision == expected_revision,
            "BTC 5m profile incumbent conflict"
        );
        ensure!(
            profile.profile_revision
                == expected_revision
                    .checked_add(1)
                    .context("revision overflow")?,
            "BTC 5m profile revision must advance once"
        );
        // This is the final mutation boundary, after acquisition work and
        // prepared-identity verification. It closes the remaining clock
        // race even if the outer control request crossed a UTC boundary.
        profile.validate_current_at(now)?;
        // Ordinary publication is only a steady-state, same-shape rotation.
        // A shape change would bypass next→current identity and the exact
        // one-market lease replacement; startup reconstitution has its own
        // explicitly reviewed publish_restart_reconciled entrypoint.
        if require_adjacent_rotation {
            profile.validate_rotation_from(&active.profile)?;
        }
        let previous = active.profile.profile_id.clone();
        let epoch = uuid::Uuid::new_v4().simple().to_string();
        *active = Active {
            profile: Arc::new(profile),
            epoch: epoch.clone(),
        };
        self.changed.send_replace(active.profile.profile_revision);
        Ok(
            json!({"previous_profile_id":previous,"profile_id":active.profile.profile_id,
            "profile_revision":active.profile.profile_revision,"epoch":epoch,
            "requires_new_full_sync":true}),
        )
    }
}
impl Api {
    pub fn router(
        reader: MemoryReader,
        profile: Profile,
        epoch: String,
        limits: Limits,
    ) -> Result<(Router, Control)> {
        Self::router_inner(reader, profile, epoch, limits, None)
    }
    /// The archive root must be the same prepared generation used by the
    /// managed writer. This route never establishes another source service.
    pub fn router_with_archive(
        reader: MemoryReader,
        profile: Profile,
        epoch: String,
        limits: Limits,
        archive_root: PathBuf,
    ) -> Result<(Router, Control)> {
        Self::router_inner(reader, profile, epoch, limits, Some(archive_root))
    }
    fn router_inner(
        reader: MemoryReader,
        profile: Profile,
        epoch: String,
        limits: Limits,
        archive_root: Option<PathBuf>,
    ) -> Result<(Router, Control)> {
        profile.validate()?;
        limits.validate()?;
        reader.configure_ordinary_handoff_frame_bytes(limits.frame_bytes)?;
        ensure!(!epoch.is_empty(), "BTC 5m epoch");
        let (changed, _) = watch::channel(profile.profile_revision);
        let control = Control {
            reader: reader.clone(),
            live: crate::source_market_live::Registry::default(),
            process_instance_id: uuid::Uuid::new_v4().simple().to_string(),
            active: Arc::new(RwLock::new(Active {
                profile: Arc::new(profile),
                epoch,
            })),
            changed,
            manager: Arc::new(RwLock::new(None)),
        };
        let timeline = Arc::new(Mutex::new(TimelineArchive::default()));
        // Diagnostic serialization and bounded retention run off the WS path.
        // A full diagnostics queue drops a receipt, never a market group.
        let (timeline_tx, timeline_rx) = std::sync::mpsc::sync_channel::<MarketTimelineGroup>(4);
        let timeline_archive = timeline.clone();
        std::thread::Builder::new().name("btc5m-market-timeline".into()).spawn(move || {
            while let Ok(sent) = timeline_rx.recv() {
                let timing = sent.group.timing_snapshot();
                for event in sent.group.validated.events() {
                    if let (Some(cursor), Some(event_id)) =
                        (event["cursor"].as_u64(), event["event_id"].as_str()) {
                        let receipt = json!({
                            "stage":"market_group_commit_sent",
                            "profile_epoch":&sent.profile_epoch,"market_id":&sent.market_id,
                            "stream_epoch":&sent.stream_epoch,
                            "market_cursor":sent.market_cursor,
                            "market_event_cursor":cursor,"event_id":event_id,
                            "group_commit_sent_at":&sent.group_commit_sent_at,
                            "source_batch_timing":timing.as_ref().map(source_timing_json),
                            "clock_error_bound_ms":Value::Null,
                        });
                        if let Ok(mut archive) = timeline_archive.lock() {
                            archive.push(&sent.profile_epoch, &sent.market_id,
                                &sent.stream_epoch, cursor, event_id, receipt);
                        }
                    }
                }
            }
        })?;
        let (slow_group_tx, slow_group_rx) =
            std::sync::mpsc::sync_channel::<SlowMarketGroup>(SLOW_GROUP_QUEUE);
        let slow_group_dropped = Arc::new(AtomicU64::new(0));
        let dropped_for_writer = slow_group_dropped.clone();
        std::thread::Builder::new().name("btc5m-slow-group-log".into()).spawn(move || {
            loop {
                match slow_group_rx.recv_timeout(Duration::from_secs(1)) {
                    Ok(group) => marketcow_polymarket::async_eprintln!("{}", slow_group_json(&group)),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {},
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
                let dropped = dropped_for_writer.swap(0, Ordering::Relaxed);
                if dropped > 0 {
                    marketcow_polymarket::async_eprintln!("{}", json!({
                        "stage":"btc_5m_slow_market_group_dropped",
                        "count":dropped,
                        "queue_capacity":SLOW_GROUP_QUEUE,
                    }));
                }
            }
        })?;
        let send_diagnostics=crate::source_market_send_diagnostic::Diagnostics::start(archive_root.clone());
        let state = Self {
            reader,
            control: control.clone(),
            archive_root,
            snapshots: Arc::new(Semaphore::new(limits.snapshot_lease_capacity)),
            streams: Arc::new(Semaphore::new(limits.clients)),
            market_stream_slots: Arc::new(Mutex::new(BTreeMap::new())),
            timeline,
            timeline_tx,
            slow_group_tx,
            slow_group_dropped,
            send_diagnostics,
            limits,
        };
        start_live_publication(state.clone());
        Ok((
            Router::new()
                .route(
                    "/v1/prediction-markets/polymarket/research/btc-5m/status",
                    get(http_status),
                )
                .route(
                    "/v1/prediction-markets/polymarket/research/btc-5m/management-health",
                    get(http_management_health),
                )
                // The official CLOB WebSocket remains an explicit observation
                // source. Its market-local v3 cursor and confirmation domain
                // must never be confused with the PN1 v4 market serial.
                .route(
                    "/v1/prediction-markets/polymarket/research/btc-5m/official-ws/market-streams",
                    get(http_live_market_streams),
                )
                .route(
                    "/v1/prediction-markets/polymarket/research/btc-5m/official-ws/market-full-sync",
                    get(http_live_market_full_sync),
                )
                .route(
                    "/v1/prediction-markets/polymarket/research/btc-5m/official-ws/market-stream",
                    get(http_live_market_stream),
                )
                .route(
                    "/v1/prediction-markets/polymarket/research/btc-5m/market-timeline",
                    get(http_timeline),
                )
                .route(
                    "/v1/prediction-markets/polymarket/research/btc-5m/market-replay",
                    get(http_market_replay),
                )
                .route(
                    "/v1/prediction-markets/polymarket/research/market-archive-replay",
                    get(http_market_archive_replay),
                )
                .with_state(state),
            control,
        ))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketFullSyncQuery {
    market_id: String,
}

fn live_response<T: prost::Message>(document: &T) -> Response {
    use marketcow_contracts::btc5m_market_local_v6 as live;
    match live::encode(document,crate::source_market_live::MAX_FRAME_BYTES) {
        Ok(bytes) => ([("content-type",live::CONTENT_TYPE),("cache-control","no-store")],bytes).into_response(),
        Err(_) => failure(StatusCode::PAYLOAD_TOO_LARGE,"btc_5m_live_frame_budget",false),
    }
}
async fn http_live_market_streams(State(api):State<Api>) -> Response {
    use marketcow_contracts::btc5m_market_local_v6::{self as live,wire};
    let document=(|| -> Result<wire::MarketStreams> {
        let active=api.control.active()?;
        api.control.live.retain(&active.profile.markets)?;
        let markets=active.profile.markets.iter().map(|binding| {
            let (key,stream_id)=api.control.live_identity(binding)?;
            Ok(wire::MarketBinding{key:Some(key),stream_id,role:match binding.role {
                WindowRole::Current=>"current",WindowRole::Next=>"next",WindowRole::Retiring=>"retiring",
            }.into()})
        }).collect::<Result<Vec<_>>>()?;
        ensure!(api.control.active()?.profile.document_sha256()==active.profile.document_sha256(),"discovery changed");
        Ok(wire::MarketStreams{schema_version:live::SCHEMA_VERSION.into(),markets,
            delivery_budget:Some(wire::DeliveryBudget {
                maximum_frame_bytes:crate::source_market_live::MAX_FRAME_BYTES as u64,
                maximum_state_bytes:crate::source_market_live::MAX_STATE_BYTES as u64,
                maximum_levels_per_side:crate::source_market_live::MAX_LEVELS as u64,
                maximum_changes:crate::source_market_live::MAX_CHANGES as u64,
            })})
    })();
    match document {Ok(value)=>live_response(&value),Err(_)=>failure(StatusCode::SERVICE_UNAVAILABLE,"btc_5m_live_discovery_unavailable",true)}
}

fn start_live_publication(api: Api) {
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(Duration::from_millis(10));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticks.tick().await;
            let Ok(active) = api.control.active() else {
                api.control.live.invalidate_all("profile_unavailable");
                continue;
            };
            if api.control.live.retain(&active.profile.markets).is_err() { continue }
            let mut jobs = tokio::task::JoinSet::new();
            for binding in active.profile.markets.iter().cloned() {
                let api = api.clone();
                jobs.spawn(async move {
                    match build_live_state(&api,binding.clone()).await {
                        Ok(state) => { let _ = api.control.live.publish(&binding,state); }
                        Err(error) => {
                            let _=api.control.live.invalidate_market(&binding,"publisher_failed");
                            marketcow_polymarket::async_eprintln!("{}",json!({
                                "stage":"market_live_publication_failed","market_id":binding.market_id,
                                "error":format!("{error:#}")}));
                        },
                    }
                });
            }
            // At most the three managed markets are in flight. Slow consumers
            // never create more builders or retain a backlog of snapshots.
            while jobs.join_next().await.is_some() {}
        }
    });
}

async fn capture_live_state(api:&Api,binding:MarketBinding) -> Result<Arc<crate::source_market_live::LiveState>> {
    api.control.live.load(&binding)
}

async fn build_live_state(api:&Api,binding:MarketBinding) -> Result<Arc<crate::source_market_live::LiveState>> {
    let api=api.clone();
    tokio::task::spawn_blocking(move || {
        let active=api.control.active()?;
        api.control.live.retain(&active.profile.markets)?;
        let lane=api.control.live.lane(&binding)?;
        let still_bound=active.profile.markets.iter().any(|b|
            crate::source_market_live::key(b).ok()==crate::source_market_live::key(&binding).ok());
        ensure!(still_bound,"immutable market key no longer active");
        let tokens=binding.outcomes.iter().map(|o|o.token_id.clone()).collect::<Vec<_>>();
        let ended=Utc::now()>=DateTime::parse_from_rfc3339(&binding.end_at)?.with_timezone(&Utc);
        let snapshot=api.reader.capture_market(&binding.market_id,&tokens).ok();
        let mut lane=lane.lock().map_err(|_|anyhow::anyhow!("live lane poisoned"))?;
        match lane.capture(snapshot.as_ref(),ended) {
            Ok(state)=>Ok(state),
            Err(error)=>{
                marketcow_polymarket::async_eprintln!("{}",json!({"stage":"market_live_state_rejected",
                    "market_id":binding.market_id,"error":format!("{error:#}")}));
                lane.reject(snapshot.as_ref(),ended,"state_budget_or_invalid")
            }
        }
    }).await?
}
async fn http_live_market_full_sync(State(api):State<Api>,Query(query):Query<MarketFullSyncQuery>) -> Response {
    let Ok(_permit)=api.snapshots.clone().try_acquire_owned() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE,"btc_5m_snapshot_capacity",true);
    };
    let binding=api.control.active().ok().and_then(|a|a.profile.markets.iter()
        .find(|b|b.market_id==query.market_id).cloned());
    let Some(binding)=binding else {return failure(StatusCode::NOT_FOUND,"btc_5m_market_not_in_profile",false);};
    match capture_live_state(&api,binding).await.and_then(|state|crate::source_market_live::frame(&state,None)) {
        Ok(document)=>live_response(&document),
        Err(_)=>failure(StatusCode::SERVICE_UNAVAILABLE,"btc_5m_live_snapshot_unavailable",true),
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveStreamQuery {market_id:String,stream_id:String}
async fn http_live_market_stream(State(api):State<Api>,Query(query):Query<LiveStreamQuery>,
    peer:Option<axum::Extension<axum::extract::ConnectInfo<std::net::SocketAddr>>>,
    headers:HeaderMap,ws:WebSocketUpgrade) -> Response {
    use marketcow_contracts::btc5m_market_local_v6 as live;
    if !headers.get(axum::http::header::SEC_WEBSOCKET_PROTOCOL).and_then(|h|h.to_str().ok())
        .is_some_and(|h|h.split(',').any(|s|s.trim()==live::SUBPROTOCOL)) {
        return failure(StatusCode::UPGRADE_REQUIRED,"btc_5m_proto_subprotocol_required",false);
    }
    let Ok(active)=api.control.active() else {return failure(StatusCode::SERVICE_UNAVAILABLE,"btc_5m_unavailable",true);};
    let Some(binding)=active.profile.markets.iter().find(|b|b.market_id==query.market_id).cloned() else {
        return failure(StatusCode::NOT_FOUND,"btc_5m_market_not_in_profile",false);
    };
    if api.control.live.retain(&active.profile.markets).is_err()
        || !api.control.live_identity(&binding).is_ok_and(|(_,id)|id==query.stream_id) {
        return failure(StatusCode::CONFLICT,"btc_5m_live_stream_changed",false);
    }
    let slots={
        let Ok(mut slots)=api.market_stream_slots.lock() else {return failure(StatusCode::SERVICE_UNAVAILABLE,"btc_5m_stream_capacity",true);};
        slots.retain(|id,_|active.profile.markets.iter().any(|b|&b.market_id==id));
        slots.entry(query.market_id.clone()).or_insert_with(||Arc::new(Semaphore::new(market_stream_client_limit(api.limits.clients)))).clone()
    };
    let Ok(market_permit)=slots.try_acquire_owned() else {return failure(StatusCode::SERVICE_UNAVAILABLE,"btc_5m_market_stream_capacity",true);};
    let Ok(permit)=api.streams.clone().try_acquire_owned() else {return failure(StatusCode::SERVICE_UNAVAILABLE,"btc_5m_stream_capacity",true);};
    ws.protocols([live::SUBPROTOCOL]).max_message_size(4096).max_frame_size(4096).on_upgrade(move |socket|async move {
        let (_permit,_market_permit)=(permit,market_permit);
        let mut socket=MarketClientIo::new(socket);
        socket.peer=peer.map(|axum::Extension(axum::extract::ConnectInfo(addr))|addr);
        socket.diagnostics=Some(api.send_diagnostics.clone());
        api.send_diagnostics.record(json!({"stage":"market_send_connection","observed_at":Utc::now(),
            "connection_id":socket.connection_id,"peer":socket.peer,"market_id":query.market_id,
            "stream_id":query.stream_id,"schema_version":live::SCHEMA_VERSION}));
        let result=async {
            let mut previous:Option<Arc<crate::source_market_live::LiveState>>=None;
            let mut changed=api.control.live.subscribe(&binding)?;
            changed.mark_changed();
            loop {
                tokio::select! {
                    message=socket.controls.recv()=> {if socket.handle_control(message,api.limits.send_timeout).await? {break;}}
                    notification=changed.changed()=>{
                        notification.context("public market retired")?;
                        let state=capture_live_state(&api,binding.clone()).await?;
                        ensure!(state.stream_id==query.stream_id,"public stream recreated");
                        if previous.as_ref().is_some_and(|old|old.sequence==state.sequence) {continue;}
                        // Net differences are always based on the last fully flushed state.
                        // Private source replacement chooses an ordinary snapshot, not a control protocol.
                        let frame=crate::source_market_live::frame(&state,previous.as_deref())?;
                        let bytes=live::encode(&frame,crate::source_market_live::MAX_FRAME_BYTES)?;
                        socket.in_flight_cursor=Some(state.sequence);
                        send_market_frame(&mut socket,&api,bytes).await?;
                        socket.last_complete_cursor=state.sequence;socket.in_flight_cursor=None;
                        let ended=state.snapshot.source_status=="ended";
                        previous=Some(state);
                        if ended {break;}
                    }
                }
            }
            Ok::<_,anyhow::Error>(())
        }.await;
        if let Err(error)=result {marketcow_polymarket::async_eprintln!("{}",json!({
            "stage":"btc_5m_live_stream_closed","observed_at":Utc::now(),"market_id":query.market_id,
            "stream_id":query.stream_id,"connection_id":socket.connection_id,"last_state_sequence":socket.last_complete_cursor,
            "error":format!("{error:#}")}));}
        let _=tokio::time::timeout(api.limits.send_timeout,socket.send(Message::Close(None))).await;
    })
}
fn proto_market_identity(active: &Active, binding: &MarketBinding,
    stream_epoch: Option<&str>) -> proto::MarketIdentity {
    proto::MarketIdentity {
        market_id: binding.market_id.clone(), condition_id: binding.condition_id.clone(),
        profile_id: active.profile.profile_id.clone(),
        profile_revision: active.profile.profile_revision,
        profile_epoch: active.epoch.clone(),
        stream_epoch: stream_epoch.map(str::to_owned),
    }
}
fn proto_market_binding(active: &Active, binding: &MarketBinding,
    stream_epoch: Option<&str>) -> Result<proto::MarketBinding> {
    Ok(proto::MarketBinding {
        identity: Some(proto_market_identity(active, binding, stream_epoch)),
        role: match &binding.role {
            WindowRole::Current => "current",
            WindowRole::Next => "next",
            WindowRole::Retiring => "retiring",
        }.into(),
        window_id: binding.window_id.clone(),
        start_at: binding.start_at.clone(), end_at: binding.end_at.clone(),
        outcomes: binding.outcomes.iter().map(|outcome| proto::OutcomeBinding {
            outcome: outcome.outcome.clone(), token_id: outcome.token_id.clone(),
        }).collect(),
        rule_sha256: proto_convert::hash32(&binding.rule_sha256)?,
        maximum_quote_age_ms: active.profile.maximum_quote_age_ms,
    })
}
fn proto_market_watermark(active: &Active, binding: &MarketBinding,
    snapshot: &MarketMemorySnapshot) -> proto::MarketWatermark {
    proto::MarketWatermark {
        identity: Some(proto_market_identity(active, binding,
            Some(&snapshot.watermark.stream_epoch))),
        published_market_cursor: Some(snapshot.watermark.published_cursor),
        replay_floor_market_cursor: Some(snapshot.watermark.replay_floor),
        last_market_event_cursor: Some(snapshot.last_market_event_cursor),
        confirmation_sequence: Some(snapshot.confirmation_sequence),
    }
}
fn market_replay_baseline_valid(snapshot: &MarketMemorySnapshot, token: &str,
    now: DateTime<Utc>) -> bool {
    if snapshot.unresolved_seed_gaps.contains(token) { return false; }
    if snapshot.capacity_fenced { return false; }
    let Some(book) = snapshot.books.get(token) else { return false; };
    if !instant(book, "exchange_at").is_ok_and(|source| source <= now) { return false; }
    if !instant(book, "received_at").is_ok_and(|received| received <= now) { return false; }
    if snapshot.recoveries.get(token).is_some_and(|recovery|
        recovery["snapshot_cursor"].as_u64() != snapshot.event_book_cursors.get(token).copied()) {
        return false;
    }
    match (best_price(book, "bids", true), best_price(book, "asks", false)) {
        (Ok(Some(bid)), Ok(Some(ask))) => {
            match (bid.parse::<rust_decimal::Decimal>(), ask.parse::<rust_decimal::Decimal>()) {
                (Ok(bid), Ok(ask)) => bid < ask,
                _ => false,
            }
        }
        (Ok(_), Ok(_)) => true,
        _ => false,
    }
}

/// Explain the exact quote gate without promoting a missing side, an old book,
/// or a recovery baseline to a tradable quote. This reads only the already
/// captured market lane and never performs I/O or refreshes book freshness.
fn market_token_quote_readiness(snapshot: &MarketMemorySnapshot, token: &str,
    now: &DateTime<Utc>, maximum_age_ms: u64) -> (bool, Value) {
    let mut reasons = Vec::new();
    if snapshot.capacity_fenced { reasons.push("market_capacity_fenced"); }
    if snapshot.unresolved_seed_gaps.contains(token) { reasons.push("source_gap"); }
    let book = snapshot.books.get(token);
    if book.is_none() { reasons.push("book_missing"); }
    // A replayable canonical baseline inside an active recovery is not an
    // execution-ready quote. Completion must clear the recovery barrier.
    if snapshot.recoveries.contains_key(token) { reasons.push("recovery_in_progress"); }
    let quote_source = book.and_then(|book| instant(book, "exchange_at").ok());
    let received = book.and_then(|book| instant(book, "received_at").ok());
    if book.is_some() && (quote_source.is_none() || received.is_none()) {
        reasons.push("book_clock_invalid");
    }
    let confirmation = market_token_confirmation(snapshot, token);
    let confirmation_source = confirmation.as_ref()
        .and_then(|fact| instant(fact, "confirmation_source_at").ok());
    let freshness = match (quote_source, confirmation_source) {
        (Some(source), Some(confirmed)) => Some(source.max(confirmed)),
        (Some(source), None) => Some(source),
        (None, _) => None,
    };
    let age_ms = freshness.as_ref().map(|at|
        (*now).clone().signed_duration_since((*at).clone()).num_milliseconds());
    if let Some(age_ms) = age_ms {
        if age_ms < 0 { reasons.push("book_clock_invalid"); }
        else if age_ms > maximum_age_ms as i64 { reasons.push("book_stale"); }
    }
    let bid_levels = book.and_then(|book| book["bids"].as_array()).map(Vec::len);
    let ask_levels = book.and_then(|book| book["asks"].as_array()).map(Vec::len);
    if let Some(book) = book {
        match (best_price(book, "bids", true), best_price(book, "asks", false)) {
            (Ok(Some(bid)), Ok(Some(ask))) => {
                match (bid.parse::<rust_decimal::Decimal>(), ask.parse::<rust_decimal::Decimal>()) {
                    (Ok(bid), Ok(ask)) if bid >= ask => reasons.push("crossed_book"),
                    (Ok(_), Ok(_)) => {},
                    _ => reasons.push("book_invalid"),
                }
            }
            (Ok(_), Ok(_)) => reasons.push("incomplete_book"),
            _ => reasons.push("book_invalid"),
        }
    }
    let ready = reasons.is_empty();
    (ready, json!({
        "token_id":token,
        "ready":ready,
        "reasons":reasons,
        "book_market_cursor":snapshot.market_book_cursors.get(token),
        "book_market_event_cursor":snapshot.event_book_cursors.get(token),
        "quote_source_at":book.and_then(|book| book["exchange_at"].as_str()),
        "book_received_at":book.and_then(|book| book["received_at"].as_str()),
        "confirmation_source_at":confirmation.as_ref()
            .and_then(|fact| fact["confirmation_source_at"].as_str()),
        "confirmation_received_at":confirmation.as_ref()
            .and_then(|fact| fact["confirmation_received_at"].as_str()),
        "effective_quote_source_at":freshness.map(|at| at.to_rfc3339()),
        "age_ms":age_ms,
        "bid_levels":bid_levels,
        "ask_levels":ask_levels,
    }))
}
fn proto_document_response<T: prost::Message>(document: &T, maximum_bytes: usize) -> Response {
    match market_proto::encode_document(document, maximum_bytes) {
        Ok(bytes) => ([(axum::http::header::CONTENT_TYPE, market_proto::CONTENT_TYPE)], bytes).into_response(),
        Err(_) => failure(StatusCode::PAYLOAD_TOO_LARGE, "btc_5m_proto_document_too_large", false),
    }
}
async fn http_market_full_sync(State(api): State<Api>, Query(query): Query<MarketFullSyncQuery>) -> Response {
    if query.market_id.is_empty() {
        return failure(StatusCode::BAD_REQUEST, "btc_5m_market_full_sync_query", false);
    }
    let Ok(permit) = api.snapshots.clone().try_acquire_owned() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_snapshot_capacity", true);
    };
    let Ok(active) = api.control.active() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    let Some(binding) = active.profile.markets.iter()
        .find(|market| market.market_id == query.market_id) else {
        return failure(StatusCode::NOT_FOUND, "btc_5m_market_not_in_profile", false);
    };
    let token_ids = binding.outcomes.iter().map(|outcome| outcome.token_id.clone()).collect::<Vec<_>>();
    let Ok(snapshot) = api.reader.capture_market(&query.market_id, &token_ids) else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_market_snapshot_unavailable", true);
    };
    let Ok(still_active) = api.control.active() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    if active.epoch != still_active.epoch
        || active.profile.profile_id != still_active.profile.profile_id
        || active.profile.profile_revision != still_active.profile.profile_revision {
        return failure(StatusCode::CONFLICT, "btc_5m_profile_changed", true);
    }
    let document = market_full_sync_document(&active, binding, &snapshot);
    drop(permit);
    match document {
        Ok(document) => proto_document_response(&document, api.limits.snapshot_bytes),
        Err(_) => failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_market_snapshot_invalid", true),
    }
}

fn market_full_sync_document(active: &Active, binding: &MarketBinding,
    snapshot: &MarketMemorySnapshot) -> Result<proto::MarketFullSync> {
    let books = binding.outcomes.iter().map(|outcome| -> Result<proto::BookBaseline> {
        let token = &outcome.token_id;
        let book = snapshot.books.get(token);
        let recovery = snapshot.recoveries.get(token).map(|value| -> Result<proto::RecoveryState> {
            Ok(proto::RecoveryState {
                token_id: token.clone(),
                recovery_id: required_recovery_string(value, "recovery_id")?.into(),
                gap_code: required_recovery_string(&value["gap"], "code")?.into(),
                gap_detected_at: required_recovery_string(&value["gap"], "detected_at")?.into(),
                snapshot_cursor: value["snapshot_cursor"].as_u64(),
            })
        }).transpose()?;
        Ok(proto::BookBaseline {
            outcome: outcome.outcome.clone(), token_id: token.clone(),
            replay_baseline_valid: Some(market_replay_baseline_valid(&snapshot, token, Utc::now())),
            book_market_cursor: snapshot.market_book_cursors.get(token).copied(),
            book_market_event_cursor: snapshot.event_book_cursors.get(token).copied(),
            canonical_book_sha256: book.map(|book| proto_convert::hash32(&canonical_hash(book.as_ref()))).transpose()?,
            canonical_book: book.map(|book| proto_convert::canonical_book(book.as_ref())).transpose()?,
            active_recovery: recovery,
            confirmation: market_token_confirmation(&snapshot, token)
                .map(|value| proto_convert::confirmation(&value)).transpose()?,
        })
    }).collect::<Result<Vec<_>>>();
    books.and_then(|books| Ok(proto::MarketFullSync {
        binding: Some(proto_market_binding(&active, binding, Some(&snapshot.watermark.stream_epoch))?),
        watermark: Some(proto_market_watermark(&active, binding, &snapshot)),
        books,
        market_unavailable_reason: snapshot.capacity_fenced.then(|| "market_persistence_capacity".into()),
        snapshot_is_not_execution_readiness: true,
    }))
}
fn required_recovery_string<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key].as_str().with_context(|| format!("missing recovery {key}"))
}
async fn http_market_streams(State(api): State<Api>) -> Response {
    let Ok(active) = api.control.active() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    let mut markets = Vec::new();
    let mut watermarks = Vec::new();
    for binding in &active.profile.markets {
        let watermark = match api.reader.market_stream_watermark(&binding.market_id) {
            Ok(value) => value,
            Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true),
        };
        let market = match proto_market_binding(&active, binding,
            watermark.as_ref().map(|value| value.stream_epoch.as_str())) {
            Ok(value) => value,
            Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_market_binding_invalid", true),
        };
        markets.push(market);
        if let Some(watermark) = watermark {
            let tokens = binding.outcomes.iter().map(|outcome| outcome.token_id.clone()).collect::<Vec<_>>();
            let snapshot = match api.reader.capture_market(&binding.market_id, &tokens) {
                Ok(value) if value.watermark.stream_epoch == watermark.stream_epoch => value,
                _ => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_market_snapshot_unavailable", true),
            };
            watermarks.push(proto_market_watermark(&active, binding, &snapshot));
        }
    }
    let Ok(still_active) = api.control.active() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    if active.epoch != still_active.epoch
        || active.profile.profile_id != still_active.profile.profile_id
        || active.profile.profile_revision != still_active.profile.profile_revision {
        return failure(StatusCode::CONFLICT, "btc_5m_profile_changed", true);
    }
    proto_document_response(&proto::MarketStreams {
        profile_id: active.profile.profile_id.clone(),
        profile_revision: active.profile.profile_revision,
        profile_epoch: active.epoch,
        markets, watermarks,
        live_delivery_budget:Some(proto::LiveDeliveryBudget {
            maximum_frame_bytes:api.limits.frame_bytes as u64,
            maximum_group_encoded_bytes:32*1024*1024,
            maximum_checkpoint_encoded_bytes:api.limits.snapshot_bytes as u64*3,
            backlog_groups:api.limits.live_backlog_groups as u64,
            backlog_age_ms:api.limits.live_backlog_age.as_millis() as u64,
            minimum_checkpoint_interval_ms:250,
            maximum_connection_checkpoint_frames:marketcow_polymarket::MAX_CONNECTION_CHECKPOINT_FRAMES as u64,
            maximum_connection_checkpoint_bytes:marketcow_polymarket::MAX_CONNECTION_CHECKPOINT_EVIDENCE_BYTES as u64,
            maximum_checkpoint_chunk_count:(api.limits.snapshot_bytes*3)
                .div_ceil(api.limits.frame_bytes-512) as u64,
        }),
    }, api.limits.snapshot_bytes)
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketReplayQuery {
    market_id: String,
    stream_epoch: String,
    after_market_cursor: u64,
    maximum_groups: usize,
}
fn archive_checkpoint_within_publication(
    before: &crate::source_publication::MarketStreamWatermark,
    after: &crate::source_publication::MarketStreamWatermark,
    archived_durable: u64,
) -> bool {
    before.market_id == after.market_id && before.stream_epoch == after.stream_epoch
        && before.published_cursor <= after.published_cursor
        && before.durable_cursor <= after.durable_cursor
        && archived_durable <= after.durable_cursor
        && after.durable_cursor <= after.published_cursor
}
async fn http_market_replay(State(api): State<Api>, Query(query): Query<MarketReplayQuery>) -> Response {
    if query.market_id.is_empty() || query.stream_epoch.is_empty()
        || !(1..=64).contains(&query.maximum_groups)
    {
        return failure(StatusCode::BAD_REQUEST, "btc_5m_market_replay_query", false);
    }
    let Ok(active) = api.control.active() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    if !active.profile.markets.iter().any(|market| market.market_id == query.market_id) {
        return failure(StatusCode::NOT_FOUND, "btc_5m_market_not_in_profile", false);
    }
    let watermark = match api.reader.market_stream_watermark(&query.market_id) {
        Ok(Some(value)) => value,
        Ok(None) => return failure(StatusCode::NOT_FOUND, "btc_5m_market_not_published", false),
        Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true),
    };
    if query.stream_epoch != watermark.stream_epoch {
        return failure(StatusCode::CONFLICT, "btc_5m_market_epoch_changed", false);
    }
    if query.after_market_cursor > watermark.durable_cursor {
        return failure(StatusCode::BAD_REQUEST, "btc_5m_market_future_cursor", false);
    }
    let Some(root) = api.archive_root.clone() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_market_archive_unavailable", true);
    };
    let Ok(permit) = api.snapshots.clone().try_acquire_owned() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_snapshot_capacity", true);
    };
    let maximum_bytes = api.limits.replay_bytes;
    let read_query = query.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        read_durable_market_page(&root, &read_query.market_id, &read_query.stream_epoch,
            read_query.after_market_cursor, read_query.maximum_groups, maximum_bytes)
    }).await;
    let page = match result {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => match error.downcast_ref::<DurableMarketPageBoundary>() {
            Some(DurableMarketPageBoundary::NotFound) =>
                return failure(StatusCode::NOT_FOUND, "btc_5m_market_archive_not_found", false),
            Some(DurableMarketPageBoundary::Expired { .. }) =>
                return failure(StatusCode::GONE, "btc_5m_market_replay_expired", false),
            Some(DurableMarketPageBoundary::Future { .. }) =>
                return failure(StatusCode::CONFLICT, "btc_5m_market_archive_behind", true),
            None => return failure(StatusCode::SERVICE_UNAVAILABLE,
                "btc_5m_market_archive_unavailable", true),
        },
        _ => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_market_archive_unavailable", true),
    };
    let Ok(still_active) = api.control.active() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    if still_active.epoch != active.epoch
        || still_active.profile.profile_id != active.profile.profile_id
        || still_active.profile.profile_revision != active.profile.profile_revision {
        return failure(StatusCode::CONFLICT, "btc_5m_profile_changed", true);
    }
    // Disk IO is asynchronous: a committed checkpoint may advance after the
    // first memory capture. Recheck the *same stream* after the read rather
    // than misclassifying legitimate writer progress against an old bound.
    let latest_watermark = match api.reader.market_stream_watermark(&query.market_id) {
        Ok(Some(value)) if value.stream_epoch == query.stream_epoch => value,
        Ok(_) => return failure(StatusCode::CONFLICT, "btc_5m_market_epoch_changed", false),
        Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true),
    };
    if !archive_checkpoint_within_publication(&watermark, &latest_watermark,
        page.checkpoint.durable_cursor) {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_market_archive_ahead_of_publisher", true);
    }
    let Some(binding) = active.profile.markets.iter()
        .find(|binding| binding.market_id == query.market_id) else {
        return failure(StatusCode::CONFLICT, "btc_5m_profile_changed", true);
    };
    let groups = match proto_convert::replay_groups(&page.groups) {
        Ok(groups) => groups,
        Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_market_replay_invalid", true),
    };
    proto_document_response(&proto::MarketReplayPage {
        market_id: page.checkpoint.market_id,
        stream_epoch: page.checkpoint.stream_epoch.clone(),
        active_identity: Some(proto_market_identity(&active, binding, Some(&page.checkpoint.stream_epoch))),
        after_market_cursor: Some(query.after_market_cursor),
        next_market_cursor: Some(page.next_cursor),
        replay_floor_market_cursor: Some(page.checkpoint.replay_floor),
        durable_market_cursor: Some(page.checkpoint.durable_cursor),
        published_market_cursor: Some(latest_watermark.published_cursor),
        caught_up: page.caught_up, archive_only: false, groups,
    }, api.limits.replay_bytes)
}
/// Cold, selective evidence remains addressable after a market leaves the hot
/// BTC profile or a process restart starts a new stream epoch. This endpoint
/// never asserts current-book availability or execution readiness.
async fn http_market_archive_replay(
    State(api): State<Api>, Query(query): Query<MarketReplayQuery>,
) -> Response {
    if query.market_id.is_empty() || query.stream_epoch.is_empty()
        || !(1..=64).contains(&query.maximum_groups)
    {
        return failure(StatusCode::BAD_REQUEST, "market_archive_replay_query", false);
    }
    let Some(root) = api.archive_root.clone() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "market_archive_unavailable", true);
    };
    let Ok(permit) = api.snapshots.clone().try_acquire_owned() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "market_archive_read_capacity", true);
    };
    let maximum_bytes = api.limits.replay_bytes;
    let read_query = query.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        read_durable_market_page(&root, &read_query.market_id, &read_query.stream_epoch,
            read_query.after_market_cursor, read_query.maximum_groups, maximum_bytes)
    }).await;
    let page = match result {
        Ok(Ok(page)) => page,
        Ok(Err(error)) => match error.downcast_ref::<DurableMarketPageBoundary>() {
            Some(DurableMarketPageBoundary::NotFound) =>
                return failure(StatusCode::NOT_FOUND, "market_archive_not_found", false),
            Some(DurableMarketPageBoundary::Expired { .. }) =>
                return failure(StatusCode::GONE, "market_archive_replay_expired", false),
            Some(DurableMarketPageBoundary::Future { .. }) =>
                return failure(StatusCode::CONFLICT, "market_archive_future_cursor", false),
            None => return failure(StatusCode::SERVICE_UNAVAILABLE, "market_archive_unavailable", true),
        },
        _ => return failure(StatusCode::SERVICE_UNAVAILABLE, "market_archive_unavailable", true),
    };
    let groups = match proto_convert::replay_groups(&page.groups) {
        Ok(groups) => groups,
        Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "market_archive_replay_invalid", true),
    };
    proto_document_response(&proto::MarketReplayPage {
        market_id: page.checkpoint.market_id,
        stream_epoch: page.checkpoint.stream_epoch,
        active_identity: None,
        after_market_cursor: Some(query.after_market_cursor),
        next_market_cursor: Some(page.next_cursor),
        replay_floor_market_cursor: Some(page.checkpoint.replay_floor),
        durable_market_cursor: Some(page.checkpoint.durable_cursor),
        published_market_cursor: None,
        caught_up: page.caught_up, archive_only: true, groups,
    }, api.limits.replay_bytes)
}
fn failure(status: StatusCode, code: &str, retryable: bool) -> Response {
    (
        status,
        Json(json!({
    "schema_version":ERROR_SCHEMA,"code":code,"retryable":retryable})),
    )
        .into_response()
}
async fn http_status(State(api): State<Api>) -> Response {
    let Ok(active) = api.control.active() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    let now = Utc::now();
    let mut windows = Vec::with_capacity(active.profile.markets.len());
    let mut current_ready = false;
    for binding in &active.profile.markets {
        let tokens = binding.outcomes.iter().map(|outcome| outcome.token_id.clone())
            .collect::<Vec<_>>();
        let snapshot = match api.reader.capture_market(&binding.market_id, &tokens) {
            Ok(snapshot) => snapshot,
            Err(_) => {
                windows.push(json!({"role":binding.role,"window_id":binding.window_id,
                    "start_at":binding.start_at,"end_at":binding.end_at,
                    "market_id":binding.market_id,"condition_id":binding.condition_id,
                    "stream_epoch":Value::Null,"published_cursor":Value::Null,
                    "confirmation_sequence":Value::Null,
                    "status":"unavailable","readiness_reasons":["market_capture_unavailable"],
                    "token_quotes":[]}));
                continue;
            }
        };
        let start = match DateTime::parse_from_rfc3339(&binding.start_at) {
            Ok(start) => start.with_timezone(&Utc),
            Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_profile_invalid", false),
        };
        let end = match DateTime::parse_from_rfc3339(&binding.end_at) {
            Ok(end) => end.with_timezone(&Utc),
            Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_profile_invalid", false),
        };
        let window_reason = match binding.role {
            WindowRole::Current if now < start => Some("window_not_started"),
            WindowRole::Current if now >= end => Some("window_ended"),
            WindowRole::Next if now >= end => Some("window_ended"),
            WindowRole::Retiring => Some("window_retiring"),
            _ => None,
        };
        let token_quotes = tokens.iter().map(|token|
        market_token_quote_readiness(&snapshot, token, &now, active.profile.maximum_quote_age_ms))
            .collect::<Vec<_>>();
        let books_ready = token_quotes.iter().all(|(ready, _)| *ready);
        let mut readiness_reasons = Vec::new();
        if let Some(reason) = window_reason { readiness_reasons.push(reason); }
        if !books_ready { readiness_reasons.push("token_quote_unavailable"); }
        let window_active = window_reason.is_none();
        let ready = window_active && books_ready;
        if binding.role == WindowRole::Current { current_ready = ready; }
        windows.push(json!({"role":binding.role,"window_id":binding.window_id,
            "start_at":binding.start_at,"end_at":binding.end_at,
            "market_id":binding.market_id,"condition_id":binding.condition_id,
            "stream_epoch":snapshot.watermark.stream_epoch,
            "published_cursor":snapshot.watermark.published_cursor,
            "confirmation_sequence":snapshot.confirmation_sequence,
            "status":if ready {"ready"} else {"unavailable"},
            "readiness_reasons":readiness_reasons,
            "token_quotes":token_quotes.into_iter().map(|(_, detail)| detail).collect::<Vec<_>>()}));
    }
    let Ok(still_active) = api.control.active() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    if active.epoch != still_active.epoch
        || active.profile.profile_id != still_active.profile.profile_id
        || active.profile.profile_revision != still_active.profile.profile_revision {
        return failure(StatusCode::CONFLICT, "btc_5m_profile_changed", true);
    }
    Json(json!({"schema_version":STATUS_SCHEMA,"profile_id":active.profile.profile_id,
        "profile_revision":active.profile.profile_revision,
        "profile_document_sha256":active.profile.document_sha256(),
        "profile":active.profile,"epoch":active.epoch,"observed_at":now.to_rfc3339(),
        "ready":current_ready,
        "reason":if current_ready {Value::Null} else {json!("current_market_not_ready")},
        "manager":api.control.manager_status().unwrap_or(Value::Null),
        "maximum_markets":MAX_MARKETS,"maximum_tokens":MAX_TOKENS,
        "cursor_semantics":"market_local_only_no_global_publisher_cursor",
        "windows":windows})).into_response()
}
/// Management visibility must not depend on a usable market-data capture.
/// HTTP 200 means the control plane answered, never that trading is ready.
fn management_health(
    process_instance_id: &str,
    active: Option<&Active>,
    manager: &Value,
    publication: Value,
    now: DateTime<Utc>,
) -> Value {
    let manager_updated_at = manager["updated_at"].as_str()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc));
    // This deadline belongs to the manager's last actual update, not this
    // HTTP read. Polling management health cannot extend data freshness.
    let manager_valid_until = manager_updated_at.map(|at| at + chrono::Duration::seconds(MANAGER_STALE_AFTER_SECONDS));
    let manager_stale = manager_valid_until.is_none_or(|until| now >= until);
    let source_ready = publication["source_ready"] == true;
    let persistence_failed = publication["persistence_state"] == "failed";
    let manager_blocked_code = ["capacity_exhausted", "lease_invalid", "recovery_in_progress"]
        .into_iter().find(|code| manager["blocked_reason_code"] == *code);
    let current_expired = active.is_some_and(|value| value.profile.markets.iter()
        .filter(|market| market.role == WindowRole::Current)
        .any(|market| DateTime::parse_from_rfc3339(&market.end_at)
            .is_ok_and(|end| now >= end.with_timezone(&Utc))));
    let mut blocked_reasons = Vec::new();
    if active.is_none() { blocked_reasons.push("profile_missing"); }
    if persistence_failed { blocked_reasons.push("persistence_failed"); }
    if manager.is_null() { blocked_reasons.push("manager_missing"); }
    else if manager_stale { blocked_reasons.push("manager_stale"); }
    if current_expired { blocked_reasons.push("current_window_expired"); }
    if !source_ready { blocked_reasons.push("source_not_ready"); }
    if let Some(code) = manager_blocked_code { blocked_reasons.push(code); }
    if matches!(manager["phase"].as_str(), Some("reconciling" | "discovering")) {
        blocked_reasons.push("manager_recovering");
    }
    if manager["phase"] == "fail_closed" { blocked_reasons.push("manager_failed"); }
    let windows = active.map(|value| value.profile.markets.iter()
        .filter(|market| matches!(market.role, WindowRole::Current | WindowRole::Next))
        .map(|market| {
            let expired = DateTime::parse_from_rfc3339(&market.end_at)
                .is_ok_and(|end| now >= end.with_timezone(&Utc));
            let reason = if expired { "window_expired" }
                else if persistence_failed { "persistence_failed" }
                else if manager.is_null() { "manager_missing" }
                else if manager_stale { "manager_stale" }
                else if !source_ready { "source_not_ready" }
                else if let Some(code) = manager_blocked_code { code }
                else if manager["phase"] == "fail_closed" { "manager_failed" }
                else if matches!(manager["phase"].as_str(), Some("reconciling" | "discovering")) { "manager_recovering" }
                else { "capture_required" };
            json!({"role":market.role,"window_id":market.window_id,
                "market_id":market.market_id,"start_at":market.start_at,
                "end_at":market.end_at,"profile_revision":value.profile.profile_revision,
                "ready":if reason == "capture_required" {Value::Null} else {json!(false)},
                "readiness_reason":reason})
        }).collect::<Vec<_>>()).unwrap_or_default();
    json!({
        "schema_version":"marketcow.btc-5m-management-health.v1",
        "as_of":now,
        "manager_updated_at":manager_updated_at,
        "valid_until":manager_valid_until,
        "manager_stale_after_seconds":MANAGER_STALE_AFTER_SECONDS,
        "process_state":"running",
        "data_state":if blocked_reasons.is_empty() {"capture_required"} else {"unavailable"},
        "entry_ready":Value::Null,
        "readiness_requires_status_and_full_sync":true,
        "generation":{"process_instance_id":process_instance_id,
            "profile_epoch":active.map(|value|value.epoch.as_str()),
            "profile_revision":active.map(|value|value.profile.profile_revision)},
        "profile_id":active.map(|value|value.profile.profile_id.as_str()),
        "profile_document_sha256":active.map(|value|value.profile.document_sha256()),
        "manager_phase":manager["phase"],
        "windows":windows,
        "blocked_reason":blocked_reasons.first(),
        "blocked_reasons":blocked_reasons,
        "resources":{"metadata_market_count":publication["metadata_market_count"],
            "hot_binding_tokens":{"used":publication["hot_binding_tokens"],
                "limit":publication["hot_token_limit"]},
            "wire_tokens":{"used":publication["wire_tokens"],
                "limit":publication["hot_token_limit"]},
            "wire_sockets":{"used":publication["wire_sockets"],
                "limit":publication["wire_socket_limit"]},
            "component_token_limit":active.map(|value|value.profile.maximum_tokens),
            "prepared_tokens":publication["prepared_tokens"],
            "active_tokens":publication["active_tokens"],
            "draining_tokens":publication["draining_tokens"],
            "phase_counts_authoritative":publication["phase_counts_authoritative"] == true
                && !manager_stale,
            "hot_binding_count_semantics":"unique union of publication-bound and wire-owned token ids, including draining; cold metadata excluded",
            "wire_token_count_semantics":"unique token ids in active or retiring source shard jobs",
            "shared_token_count_semantics":"deduplicated token-id union across live component leases",
            "phase_count_semantics":"mutually exclusive process-local binding phases; null on missing or inconsistent owner metrics; not a durable transaction ledger"},
        "publication":publication,
    })
}
async fn http_management_health(State(api): State<Api>) -> Response {
    let publication = match api.reader.management_status() {
        Ok(value) => value,
        Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_management_unavailable", true),
    };
    let profile = api.control.active().ok();
    let manager = api.control.manager_status().unwrap_or(Value::Null);
    Json(management_health(&api.control.process_instance_id,
        profile.as_ref(), &manager, publication, Utc::now())).into_response()
}
#[derive(Deserialize)]
struct TimelineQuery {
    profile_epoch: String,
    market_id: String,
    stream_epoch: String,
    market_event_cursor: u64,
    event_id: String,
}
async fn http_timeline(State(api): State<Api>, Query(q): Query<TimelineQuery>) -> Response {
    let Ok(_permit) = api.snapshots.clone().try_acquire_owned() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_snapshot_capacity", true);
    };
    let Ok(active) = api.control.active() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    if q.market_id.is_empty() || q.stream_epoch.is_empty()
        || q.market_event_cursor == 0 || !hex64(&q.event_id) {
        return failure(StatusCode::BAD_REQUEST, "btc_5m_timeline_identity", false);
    }
    if !active.profile.markets.iter().any(|market| market.market_id == q.market_id) {
        return failure(StatusCode::NOT_FOUND, "btc_5m_market_not_in_profile", false);
    }
    let replay_event = if q.profile_epoch == active.epoch {
        api.reader.market_timeline_event(&q.market_id, &q.stream_epoch,
            q.market_event_cursor)
    } else {
        Ok(None)
    };
    let publication = match replay_event {
        Ok(Some((event, timing))) if event["event_id"] == q.event_id => Some(json!({
            "market_id":event["market_id"], "token_id":event["token_id"],
            "book_epoch":event["book_epoch"], "sequence":event["sequence"],
            "market_event_cursor":event["cursor"], "event_id":event["event_id"],
            "event_received_at":event["received_at"],
            "canonical_book_received_at":event["canonical_payload"]["received_at"],
            "source_batch_timing":timing.as_ref().map(source_timing_json),
        })),
        Ok(Some(_)) => return failure(StatusCode::CONFLICT, "btc_5m_timeline_event_mismatch", false),
        Ok(None) => None,
        Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true),
    };
    let sent = match api.timeline.lock() {
        Ok(archive) => archive.lookup(&q.profile_epoch, &q.market_id,
            &q.stream_epoch, q.market_event_cursor, &q.event_id),
        Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true),
    };
    if publication.is_none() && sent.is_empty() {
        return failure(StatusCode::GONE, "btc_5m_timeline_evicted_or_unknown", false);
    }
    Json(json!({
        "schema_version":"marketcow.polymarket.btc-5m-market-timeline.v2",
        "profile_epoch":q.profile_epoch,"market_id":q.market_id,
        "stream_epoch":q.stream_epoch,"market_event_cursor":q.market_event_cursor,
        "event_id":q.event_id,
        "publication_if_still_in_replay":publication,
        "connection_sends":sent,
        "receipt_capacity":{"max_entries":TIMELINE_RECEIPT_COUNT,"max_bytes":TIMELINE_RECEIPT_BYTES},
        "clock_error_bound_ms":Value::Null,
    })).into_response()
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketStreamQuery {
    market_id: String,
    stream_epoch: String,
    profile_epoch: String,
    after_market_cursor: u64,
    after_confirmation_sequence: u64,
}

async fn http_market_stream(
    State(api): State<Api>, Query(query): Query<MarketStreamQuery>,
    peer: Option<axum::Extension<axum::extract::ConnectInfo<std::net::SocketAddr>>>,
    headers: HeaderMap, ws: WebSocketUpgrade,
) -> Response {
    if !headers.get(axum::http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(',').any(|offered| offered.trim() == MARKET_PROTO_SUBPROTOCOL)) {
        return failure(StatusCode::UPGRADE_REQUIRED, "btc_5m_proto_subprotocol_required", false);
    }
    if query.market_id.is_empty() || query.stream_epoch.is_empty() || query.profile_epoch.is_empty() {
        return failure(StatusCode::BAD_REQUEST, "btc_5m_market_stream_query", false);
    }
    let Ok((active, profile_changed)) = api.control.active_with_watch() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    if query.profile_epoch != active.epoch {
        return failure(StatusCode::CONFLICT, "btc_5m_profile_changed", true);
    }
    let Some(binding) = active.profile.markets.iter()
        .find(|market| market.market_id == query.market_id).cloned() else {
        return failure(StatusCode::NOT_FOUND, "btc_5m_market_not_in_profile", false);
    };
    let watermark = match api.reader.market_stream_watermark(&query.market_id) {
        Ok(Some(value)) => value,
        Ok(None) => return failure(StatusCode::NOT_FOUND, "btc_5m_market_not_published", false),
        Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true),
    };
    if watermark.stream_epoch != query.stream_epoch {
        return failure(StatusCode::CONFLICT, "btc_5m_market_epoch_changed", false);
    }
    if query.after_market_cursor > watermark.published_cursor {
        return failure(StatusCode::CONFLICT, "btc_5m_market_future_cursor", false);
    }
    let tokens = binding.outcomes.iter().map(|outcome| outcome.token_id.clone()).collect::<Vec<_>>();
    match api.reader.capture_market(&query.market_id, &tokens) {
        Ok(snapshot) if query.after_confirmation_sequence <= snapshot.confirmation_sequence => {},
        Ok(_) => return failure(StatusCode::BAD_REQUEST, "btc_5m_market_future_confirmation", false),
        Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true),
    }
    if query.after_market_cursor >= watermark.replay_floor {
        if let Err(error) = api.reader.replay_market(&query.market_id, &query.stream_epoch,
            query.after_market_cursor, 1, api.limits.replay_bytes) {
            // A bounded latest-state replacement is a v5 protocol operation,
            // including reconnects from an evicted applied prefix. Do not
            // reject that operation before the WS writer can send it.
            let expired = api.reader.market_stream_watermark(&query.market_id)
                .ok().flatten().is_some_and(|latest|
                    latest.stream_epoch == query.stream_epoch
                    && query.after_market_cursor < latest.replay_floor);
            if !expired && error.downcast_ref::<crate::source_market_replay::MarketReplayGroupBudget>().is_none() {
                return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_market_stream_unavailable", true);
            }
        }
    }
    // One market's slow or excessive consumers cannot occupy every WS slot
    // needed by current/next. The shared cap remains an absolute bound; each
    // market also has its own admission and send task.
    let market_slots = {
        let Ok(mut slots) = api.market_stream_slots.lock() else {
            return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_stream_capacity", true);
        };
        slots.retain(|market, _| active.profile.markets.iter()
            .any(|binding| &binding.market_id == market));
        slots.entry(query.market_id.clone()).or_insert_with(||
            Arc::new(Semaphore::new(market_stream_client_limit(api.limits.clients))))
            .clone()
    };
    let Ok(market_permit) = market_slots.try_acquire_owned() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_market_stream_capacity", true);
    };
    let Ok(permit) = api.streams.clone().try_acquire_owned() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_stream_capacity", true);
    };
    ws.protocols([MARKET_PROTO_SUBPROTOCOL]).max_message_size(4096).max_frame_size(4096)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            let _market_permit = market_permit;
            let mut socket = MarketClientIo::new(socket);
            socket.diagnostics=Some(api.send_diagnostics.clone());
            socket.peer=peer.map(|axum::Extension(axum::extract::ConnectInfo(addr))|addr);
            api.send_diagnostics.record(json!({"stage":"market_send_connection",
                "observed_at":Utc::now(),"connection_id":socket.connection_id,
                "peer":socket.peer,"market_id":query.market_id,
                "stream_epoch":query.stream_epoch,"profile_epoch":query.profile_epoch}));
            if let Err(error) = market_stream(&mut socket, &api, active,
                profile_changed, binding, query.clone()).await {
                marketcow_polymarket::async_eprintln!("{}", json!({"stage":"btc_5m_market_stream_closed",
                    "at":Utc::now(),"market_id":query.market_id,
                    "stream_epoch":query.stream_epoch,"initial_after_market_cursor":query.after_market_cursor,
                    "connection_id":socket.connection_id,
                    "last_complete_market_cursor":socket.last_complete_cursor,
                    "in_flight_market_cursor":socket.in_flight_cursor,
                    "error":format!("{error:#}")}));
            }
            // Release this selected market's and the shared WS permits even
            // when the peer cannot drain the closing handshake.
            let _ = tokio::time::timeout(api.limits.send_timeout,
                socket.send(Message::Close(None))).await;
        })
}

/// Split an encoded event bundle under the frame budget. A consumer stages
/// all parts before decoding events and validating the semantic group hash;
/// only the complete commit can advance its exclusive market cursor.
fn market_group_frames(active: &Active, query: &MarketStreamQuery,
    market_cursor: u64, group_sha256: &str, events: &[Value],
    frame_bytes: usize) -> Result<Vec<Vec<u8>>> {
    let binding = active.profile.markets.iter().find(|binding| binding.market_id == query.market_id)
        .context("market group not in active profile")?;
    let identity = proto_market_identity(active, binding, Some(&query.stream_epoch));
    proto_convert::encode_market_group(&identity, market_cursor, group_sha256, events, frame_bytes)
}

/// A bounded control reader runs even while the single data writer awaits
/// socket capacity. It never queues/forwards live market data or owns state.
struct MarketClientIo {
    connection_id: String,
    last_complete_cursor: u64,
    in_flight_cursor: Option<u64>,
    send_trace_budget: (std::time::Instant,u32),
    writer: futures_util::stream::SplitSink<WebSocket, Message>,
    controls: tokio::sync::mpsc::Receiver<std::result::Result<Message,String>>,
    reader: tokio::task::JoinHandle<()>,
    peer:Option<std::net::SocketAddr>,
    diagnostics:Option<crate::source_market_send_diagnostic::Diagnostics>,
    send_trace:SendTrace,
    send_window_started:std::time::Instant,
    send_histogram:[u64;8],
    send_count:u64,
    send_max_us:u64,
}
#[derive(Default,Serialize)]
struct SendTrace {
    phase:&'static str,
    ready_us:u64,
    start_send_us:u64,
    flush_us:u64,
    ready_pending:u64,
    flush_pending:u64,
    first_poll_delay_us:u64,
    // Includes legitimate IO wait; never label this scheduling latency.
    max_poll_interval_us:u64,
}
impl MarketClientIo {
    fn new(socket: WebSocket) -> Self {
        let (writer, mut reader) = socket.split();
        let (tx, controls) = tokio::sync::mpsc::channel(8);
        let task = tokio::spawn(async move {
            while let Some(message) = reader.next().await {
                let message = message.map_err(|error|error.to_string());
                if matches!(message, Ok(Message::Pong(_))) { continue; }
                let stop = matches!(message, Ok(Message::Close(_)) | Err(_));
                if tx.try_send(message).is_err() || stop { break; }
            }
        });
        Self {writer,controls,reader:task,
            connection_id:uuid::Uuid::new_v4().simple().to_string(),
            last_complete_cursor:0,in_flight_cursor:None,
            peer:None,diagnostics:None,send_trace:SendTrace::default(),
            send_window_started:std::time::Instant::now(),send_histogram:[0;8],
            send_count:0,send_max_us:0,
            send_trace_budget:(std::time::Instant::now(),0)}
    }
    async fn send(&mut self, message: Message) -> Result<()> {
        use futures_util::Sink;
        use std::{pin::Pin,task::Poll};
        let started=std::time::Instant::now();
        self.send_trace=SendTrace{phase:"poll_ready",..SendTrace::default()};
        let mut previous=None;
        futures_util::future::poll_fn(|cx| {
            let now=std::time::Instant::now();
            if let Some(old)=previous {
                self.send_trace.max_poll_interval_us=self.send_trace.max_poll_interval_us
                    .max(micros(now.duration_since(old)));
            } else {self.send_trace.first_poll_delay_us=micros(now.duration_since(started));}
            previous=Some(now);
            self.send_trace.ready_us=micros(now.duration_since(started));
            let result=Pin::new(&mut self.writer).poll_ready(cx);
            if matches!(result,Poll::Pending){self.send_trace.ready_pending+=1;}
            result
        }).await?;
        self.send_trace.phase="start_send";
        let writing=std::time::Instant::now();
        Pin::new(&mut self.writer).start_send(message)?;
        self.send_trace.start_send_us=micros(writing.elapsed());
        self.send_trace.phase="poll_flush";
        let flushing=std::time::Instant::now();
        previous=None;
        futures_util::future::poll_fn(|cx| {
            let now=std::time::Instant::now();
            if let Some(old)=previous {
                self.send_trace.max_poll_interval_us=self.send_trace.max_poll_interval_us
                    .max(micros(now.duration_since(old)));
            }
            previous=Some(now);
            self.send_trace.flush_us=micros(now.duration_since(flushing));
            let result=Pin::new(&mut self.writer).poll_flush(cx);
            if matches!(result,Poll::Pending){self.send_trace.flush_pending+=1;}
            result
        }).await?;
        self.send_trace.phase="completed";
        Ok(())
    }
    fn record_data_send(&mut self,bytes:usize,elapsed:Duration,result:&str) {
        let us=micros(elapsed);
        // Cancellation may occur between polls. Include the final pending
        // interval instead of reporting only time up to the last poll.
        if self.send_trace.phase=="poll_ready" {self.send_trace.ready_us=us;}
        if self.send_trace.phase=="poll_flush" {
            self.send_trace.flush_us=us.saturating_sub(self.send_trace.ready_us)
                .saturating_sub(self.send_trace.start_send_us);
        }
        let upper=[1_000,5_000,10_000,50_000,100_000,500_000,1_000_000];
        let bucket=upper.iter().position(|limit|us<=*limit).unwrap_or(7);
        self.send_histogram[bucket]+=1;self.send_count+=1;self.send_max_us=self.send_max_us.max(us);
        let Some(diagnostics)=&self.diagnostics else{return;};
        if us>=50_000 || result!="ok" {
            diagnostics.record(json!({"stage":"market_send_detail","observed_at":Utc::now(),
                "connection_id":self.connection_id,"peer":self.peer,
                "in_flight_market_cursor":self.in_flight_cursor,
                "last_complete_market_cursor":self.last_complete_cursor,
                "frame_bytes":bytes,"result":result,"elapsed_us":us,"trace":self.send_trace}));
        }
        if self.send_window_started.elapsed()>=Duration::from_secs(5) {
            diagnostics.record(json!({"stage":"market_send_window","observed_at":Utc::now(),
                "connection_id":self.connection_id,"peer":self.peer,
                "elapsed_us":micros(self.send_window_started.elapsed()),"count":self.send_count,
                "max_us":self.send_max_us,"bucket_counts":self.send_histogram,
                "bucket_upper_us":upper,"overflow_bucket":7}));
            self.send_window_started=std::time::Instant::now();self.send_histogram=[0;8];
            self.send_count=0;self.send_max_us=0;
        }
    }
    async fn handle_control(&mut self,
        message: Option<std::result::Result<Message,String>>, timeout: Duration) -> Result<bool> {
        match message {
            None | Some(Ok(Message::Close(_))) => Ok(true),
            Some(Ok(Message::Ping(bytes))) => {
                tokio::time::timeout(timeout,self.send(Message::Pong(bytes))).await??;
                Ok(false)
            }
            Some(Ok(Message::Pong(_))) => Ok(false),
            Some(Err(error)) => anyhow::bail!("market client receive: {error}"),
            _ => anyhow::bail!("unexpected market stream client message"),
        }
    }
}
impl Drop for MarketClientIo {
    fn drop(&mut self) {
        if let Some(diagnostics)=&self.diagnostics {
            diagnostics.record(json!({"stage":"market_send_connection_finished",
                "observed_at":Utc::now(),"connection_id":self.connection_id,"peer":self.peer,
                "last_complete_market_cursor":self.last_complete_cursor,
                "in_flight_market_cursor":self.in_flight_cursor,
                "tail_window_elapsed_us":micros(self.send_window_started.elapsed()),
                "tail_count":self.send_count,"tail_max_us":self.send_max_us,
                "tail_bucket_counts":self.send_histogram,
                "bucket_upper_us":[1000,5000,10000,50000,100000,500000,1000000],
                "overflow_bucket":7}));
        }
        self.reader.abort();
    }
}
async fn poll_live_client_control(socket: &mut MarketClientIo, timeout: Duration) -> Result<bool> {
    loop {
        match socket.controls.try_recv() {
            Ok(message) => if socket.handle_control(Some(message),timeout).await? { return Ok(true); },
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return Ok(false),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return Ok(true),
        }
    }
}

async fn send_market_frame(socket: &mut MarketClientIo, api: &Api, bytes: Vec<u8>) -> Result<DateTime<Utc>> {
    let count = bytes.len();
    let started = std::time::Instant::now();
    let result=tokio::time::timeout(api.limits.send_timeout,
        socket.send(Message::Binary(bytes.into()))).await;
    socket.record_data_send(count,started.elapsed(),match &result {
        Ok(Ok(()))=>"ok",Ok(Err(_))=>"write_error",Err(_)=>"deadline",
    });
    result.context("market stream send deadline")??;
    if socket.send_trace_budget.0.elapsed() >= Duration::from_secs(60) {
        socket.send_trace_budget = (std::time::Instant::now(),0);
    }
    if started.elapsed() >= Duration::from_millis(50) && socket.send_trace_budget.1 < 60 {
        socket.send_trace_budget.1 += 1;
        marketcow_polymarket::async_eprintln!("{}",json!({
            "stage":"market_stream_slow_send","observed_at":Utc::now(),
            "connection_id":socket.connection_id,"last_complete_market_cursor":socket.last_complete_cursor,
            "in_flight_market_cursor":socket.in_flight_cursor,"frame_bytes":count,
            "send_await_us":started.elapsed().as_micros(),
            "send_trace":socket.send_trace,
        }));
    }
    Ok(Utc::now())
}

fn micros(duration:Duration)->u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn proto_market_frame(active: &Active, binding: &MarketBinding,
    query: &MarketStreamQuery, body: proto::market_stream_frame::Body) -> proto::MarketStreamFrame {
    proto::MarketStreamFrame {
        identity: Some(proto_market_identity(active, binding, Some(&query.stream_epoch))),
        body: Some(body),
    }
}
async fn send_market_message(socket: &mut MarketClientIo, api: &Api,
    frame: proto::MarketStreamFrame) -> Result<()> {
    let bytes = market_proto::encode_frame(&frame, api.limits.frame_bytes)?;
    send_market_frame(socket, api, bytes).await?;
    Ok(())
}

fn market_token_confirmation(snapshot: &MarketMemorySnapshot, token: &str) -> Option<Value> {
    let fact = snapshot.confirmation_facts.get(token)?;
    let book = snapshot.books.get(token)?;
    let cursor = snapshot.market_book_cursors.get(token)?;
    let version = snapshot.market_confirmation_versions.get(token)?;
    if *version == 0 || *version > snapshot.confirmation_sequence { return None; }
    // Never attach an older same-depth confirmation to a newer book cursor.
    // In particular, last_trade can advance the canonical sequence without
    // changing the depth checksum.
    if fact["token_id"] != book["token_id"]
        || fact["condition_id"] != book["condition_id"]
        || fact["book_epoch"] != book["book_epoch"]
        || fact["sequence"] != book["sequence"]
        || fact["state_checksum"] != book["state_checksum"]
        || fact["book_quote_source_at"] != book["exchange_at"]
    { return None; }
    let quote_at = instant(book, "exchange_at").ok()?;
    let confirmation_at = instant(fact, "confirmation_source_at").ok()?;
    let receipt_at = instant(fact, "confirmation_received_at").ok()?;
    if confirmation_at <= quote_at || confirmation_at > receipt_at { return None; }
    Some(json!({"token_id":fact["token_id"],"condition_id":fact["condition_id"],
        "confirmation_sequence":version,
        "market_cursor":cursor,"book_epoch":fact["book_epoch"],
        "book_sequence":fact["sequence"],"state_checksum":fact["state_checksum"],
        "book_quote_source_at":fact["book_quote_source_at"],
        "confirmation_source_at":fact["confirmation_source_at"],
        "confirmation_received_at":fact["confirmation_received_at"],
        "confirmation_source":fact["confirmation_source"],
        "confirmation_evidence_sha256":fact["confirmation_evidence_sha256"]}))
}

fn market_confirmation_frame(active: &Active, binding: &MarketBinding,
    query: &MarketStreamQuery, snapshot: &MarketMemorySnapshot,
    sent_cursor: u64, after_confirmation: u64) -> Result<Option<proto::MarketStreamFrame>> {
    if snapshot.watermark.stream_epoch != query.stream_epoch
        || snapshot.confirmation_sequence <= after_confirmation { return Ok(None); }
    let confirmations = snapshot.confirmation_facts.keys()
        .filter(|token| snapshot.market_confirmation_versions.get(*token)
            .is_some_and(|version| *version > after_confirmation))
        .filter_map(|token| market_token_confirmation(snapshot, token))
        .map(|value| proto_convert::confirmation(&value)).collect::<Result<Vec<_>>>()?;
    if confirmations.iter().any(|fact|
        fact.book_market_cursor > sent_cursor) {
        return Ok(None);
    }
    Ok(Some(proto_market_frame(active, binding, query,
        proto::market_stream_frame::Body::BookConfirmations(proto::BookConfirmations {
            sent_market_cursor: Some(sent_cursor),
            confirmation_sequence: snapshot.confirmation_sequence,
            confirmations,
        }))))
}

fn market_ready_frame(active: &Active, binding: &MarketBinding,
    query: &MarketStreamQuery, snapshot: &MarketMemorySnapshot,
    sent_cursor: u64) -> Result<proto::MarketStreamFrame> {
    ensure!(snapshot.watermark.stream_epoch == query.stream_epoch
        && snapshot.watermark.published_cursor == sent_cursor,
        "market ready requires one caught-up snapshot");
    ensure!(sent_cursor == 0 || snapshot.last_market_event_cursor > 0,
        "market ready missing event cursor boundary");
    let confirmations = snapshot.confirmation_facts.keys()
        .filter_map(|token| market_token_confirmation(snapshot, token))
        .map(|value| proto_convert::confirmation(&value)).collect::<Result<Vec<_>>>()?;
    ensure!(confirmations.iter().all(|fact| fact.book_market_cursor <= sent_cursor),
        "ready confirmation ahead of sent book");
    Ok(proto_market_frame(active, binding, query,
        proto::market_stream_frame::Body::Ready(proto::MarketReady {
            market_cursor: Some(sent_cursor),
            last_market_event_cursor: Some(snapshot.last_market_event_cursor),
            confirmation_sequence: Some(snapshot.confirmation_sequence),
            confirmations, execution_readiness: Some(false),
        })))
}

fn live_checkpoint_frames(active: &Active, binding: &MarketBinding,
    query: &MarketStreamQuery, snapshot: &MarketMemorySnapshot,
    replaces_after: u64, reason: &str, limits: &Limits) -> Result<Vec<Vec<u8>>> {
    use prost::Message as _;
    ensure!(snapshot.watermark.stream_epoch == query.stream_epoch
        && snapshot.watermark.published_cursor > replaces_after,
        "live checkpoint epoch/prefix");
    ensure!(matches!(reason, "live_backlog" | "replay_expired"), "checkpoint reason");
    let document = market_full_sync_document(active, binding, snapshot)?;
    let books = binding.outcomes.iter().zip(document.books.iter()).map(|(outcome, baseline)| {
        let token = &outcome.token_id;
        let book = snapshot.books.get(token);
        json!({"token_id":token,"canonical_book":book.map(|book|book.as_ref()),
            "canonical_book_sha256":book.map(|book|canonical_hash(book.as_ref())),
            "book_market_cursor":snapshot.market_book_cursors.get(token),
            "book_market_event_cursor":snapshot.event_book_cursors.get(token),
            "replay_baseline_valid":baseline.replay_baseline_valid,
            "active_recovery":baseline.active_recovery.as_ref().map(|recovery|json!({
                "token_id":recovery.token_id,"recovery_id":recovery.recovery_id,
                "gap_code":recovery.gap_code,"gap_detected_at":recovery.gap_detected_at,
                "snapshot_cursor":recovery.snapshot_cursor})),
            "confirmation":market_token_confirmation(snapshot,token)})
    }).collect::<Vec<_>>();
    let preimage = json!({
        "schema_version":"marketcow.polymarket.live-checkpoint.v1",
        "market_id":query.market_id,"stream_epoch":query.stream_epoch,
        "profile_epoch":active.epoch,"profile_id":active.profile.profile_id,
        "profile_revision":active.profile.profile_revision,"binding":binding,
        "replaces_after_market_cursor":replaces_after,
        "market_cursor":snapshot.watermark.published_cursor,
        "last_market_event_cursor":snapshot.last_market_event_cursor,
        "confirmation_sequence":snapshot.confirmation_sequence,"reason":reason,"books":books,
    });
    let digest = proto_convert::hash32(&canonical_hash(&preimage))?;
    let checkpoint = proto::LiveCheckpoint { snapshot:Some(document),
        replaces_after_market_cursor:replaces_after,
        last_market_event_cursor:snapshot.last_market_event_cursor,
        reason:reason.into(),semantic_preimage_json:proto_convert::canonical_json_bytes(&preimage)?,
        semantic_sha256:digest.clone() };
    // Material and transport have explicit separate budgets. The preimage is
    // audit evidence embedded in this sole feed, not another live JSON feed.
    let encoded = checkpoint.encode_to_vec();
    ensure!(encoded.len() <= limits.snapshot_bytes.checked_mul(3)
        .context("checkpoint byte budget overflow")?, "checkpoint byte budget");
    let chunk_bytes = limits.frame_bytes.checked_sub(512).context("checkpoint frame budget")?;
    let checkpoint_id = hex::encode(&digest);
    let count = u32::try_from(encoded.len().div_ceil(chunk_bytes))?;
    let mut frames = Vec::with_capacity(count as usize + 2);
    frames.push(market_proto::encode_frame(&proto_market_frame(active,binding,query,
        proto::market_stream_frame::Body::CheckpointBegin(proto::CheckpointBegin {
            checkpoint_id:checkpoint_id.clone(),replaces_after_market_cursor:replaces_after,
            market_cursor:snapshot.watermark.published_cursor,chunk_count:count,
            encoded_bytes:encoded.len() as u64,semantic_sha256:digest.clone(),
        })), limits.frame_bytes)?);
    for (index, part) in encoded.chunks(chunk_bytes).enumerate() {
        frames.push(market_proto::encode_frame(&proto_market_frame(active,binding,query,
            proto::market_stream_frame::Body::CheckpointChunk(proto::CheckpointChunk {
                checkpoint_id:checkpoint_id.clone(),chunk_index:index as u32,
                encoded_checkpoint_part:part.to_vec(),
            })),limits.frame_bytes)?);
    }
    frames.push(market_proto::encode_frame(&proto_market_frame(active,binding,query,
        proto::market_stream_frame::Body::CheckpointCommit(proto::CheckpointCommit {
            checkpoint_id,market_cursor:snapshot.watermark.published_cursor,semantic_sha256:digest,
        })),limits.frame_bytes)?);
    Ok(frames)
}

async fn market_stream(socket: &mut MarketClientIo, api: &Api, active: Active,
    mut profile_changed: watch::Receiver<u64>, binding: MarketBinding,
    query: MarketStreamQuery) -> Result<()> {
    let mut changed = api.reader.subscribe_market(&query.market_id, &query.stream_epoch)?;
    let connection_id = socket.connection_id.clone();
    let mut cursor = query.after_market_cursor;
    socket.last_complete_cursor = cursor;
    let mut ready_sent = false;
    let mut last_confirmation = query.after_confirmation_sequence;
    let mut last_confirmation_sent_at = std::time::Instant::now() - CONFIRMATION_MIN_INTERVAL;
    let mut last_checkpoint_at = std::time::Instant::now() - Duration::from_secs(1);
    let tokens = binding.outcomes.iter().map(|outcome|outcome.token_id.clone()).collect::<Vec<_>>();
    // Freeze a causal ready prefix once. Requiring an empty live replay page
    // would make a liquid market unable to become ready while new groups keep
    // arriving. The initial snapshot's books/confirmations are valid exactly
    // at this complete group boundary; later groups follow only after ready.
    let initial_snapshot = api.reader.capture_market(&query.market_id, &tokens)?;
    ensure!(initial_snapshot.watermark.stream_epoch == query.stream_epoch,
        "market ready snapshot epoch changed");
    let ready_target = initial_snapshot.watermark.published_cursor;
    ensure!(cursor <= ready_target, "market ready snapshot precedes requested cursor");
    let mut ready_snapshot = Some(initial_snapshot);
    loop {
        if poll_live_client_control(socket, api.limits.send_timeout).await? { return Ok(()); }
        changed.borrow_and_update();
        if *profile_changed.borrow() != active.profile.profile_revision {
            let next = api.control.active()?;
            send_market_message(socket, api, proto_market_frame(&active, &binding, &query,
                proto::market_stream_frame::Body::ProfileChanged(proto::ProfileChanged {
                    after_market_cursor: Some(cursor),
                    next_profile_id: next.profile.profile_id.clone(),
                    next_profile_revision: next.profile.profile_revision,
                    next_profile_epoch: next.epoch,
                    requires_new_market_full_sync: true,
                }))).await?;
            return Ok(());
        }
        let mut fence = api.reader.capture_market(&query.market_id, &tokens)?;
        if fence.capacity_fenced {
            send_market_message(socket, api, proto_market_frame(&active, &binding, &query,
                proto::market_stream_frame::Body::Unavailable(proto::MarketUnavailable {
                    after_market_cursor: Some(cursor), reason: "market_persistence_capacity".into(),
                    requires_new_market_full_sync: true,
                }))).await?;
            return Ok(());
        }
        if !ready_sent && cursor == ready_target {
            let snapshot = ready_snapshot.take().context("market ready snapshot missing")?;
            if snapshot.capacity_fenced {
                send_market_message(socket, api, proto_market_frame(&active, &binding, &query,
                    proto::market_stream_frame::Body::Unavailable(proto::MarketUnavailable {
                        after_market_cursor: Some(cursor), reason: "market_persistence_capacity".into(),
                        requires_new_market_full_sync: true,
                    }))).await?;
                return Ok(());
            }
            send_market_message(socket, api,
                market_ready_frame(&active, &binding, &query, &snapshot, cursor)?).await?;
            ready_sent = true;
            last_confirmation = snapshot.confirmation_sequence;
        }
        let maximum_groups = if ready_sent { 16 } else {
            usize::try_from((ready_target - cursor).min(16))?
        };
        let mut expired = cursor < fence.watermark.replay_floor;
        let mut oversized_group = false;
        let group_backlog = fence.watermark.published_cursor.saturating_sub(cursor)
            > api.limits.live_backlog_groups as u64;
        let page = if expired { None } else {
            match api.reader.replay_market(&query.market_id, &query.stream_epoch,
                cursor, maximum_groups, api.limits.replay_bytes) {
                Ok(page) => Some(page),
                Err(error) => {
                    // Eviction can race the preceding capture. Recheck the
                    // actual floor, never classify every read error as expiry.
                    fence = api.reader.capture_market(&query.market_id,&tokens)?;
                    if cursor < fence.watermark.replay_floor {
                        expired = true;
                        None
                    } else if error.downcast_ref::<crate::source_market_replay::MarketReplayGroupBudget>().is_some() {
                        oversized_group = true;
                        None
                    } else { return Err(error); }
                }
            }
        };
        let aged_prefix = page.as_ref().and_then(|page|page.groups.first())
            .and_then(|group|group.payload.timing_snapshot())
            .and_then(|timing|timing.shared_replay_visible_mono)
            .is_some_and(|at|at.elapsed() >= api.limits.live_backlog_age);
        if (expired || oversized_group) && last_checkpoint_at.elapsed() < Duration::from_millis(250) {
            let wait = Duration::from_millis(250).saturating_sub(last_checkpoint_at.elapsed());
            tokio::select! {
                _=tokio::time::sleep(wait)=>{},
                result=profile_changed.changed()=>result.context("profile control closed during checkpoint pacing")?,
                message=socket.controls.recv()=> {
                    if socket.handle_control(message,api.limits.send_timeout).await? { return Ok(()); }
                }
            }
            continue;
        }
        if expired || oversized_group || ((group_backlog || aged_prefix)
            && last_checkpoint_at.elapsed() >= Duration::from_millis(250)) {
            let reason = if expired { "replay_expired" } else { "live_backlog" };
            let checkpoint_cursor = fence.watermark.published_cursor;
            let encoder_active = active.clone();
            let encoder_binding = binding.clone();
            let encoder_query = query.clone();
            let encoder_snapshot = fence.clone();
            let encoder_limits = api.limits.clone();
            // One bounded encoder job per admitted connection. Serialization
            // never executes on a socket-reader task or holds a replay lane.
            let frames = tokio::task::spawn_blocking(move || live_checkpoint_frames(
                &encoder_active,&encoder_binding,&encoder_query,&encoder_snapshot,
                cursor,reason,&encoder_limits)).await.context("checkpoint encoder stopped")??;
            if *profile_changed.borrow() != active.profile.profile_revision { continue; }
            socket.in_flight_cursor = Some(checkpoint_cursor);
            let sent_start = std::time::Instant::now();
            for frame in frames {
                if poll_live_client_control(socket,api.limits.send_timeout).await? { return Ok(()); }
                send_market_frame(socket,api,frame).await?;
            }
            // No intervening mutation/confirmation escapes this writer. Only
            // commit completes replacement; on partial send the consumer
            // discards it and reconnects from its last applied prefix.
            marketcow_polymarket::async_eprintln!("{}",json!({
                "stage":"market_live_checkpoint_sent","observed_at":Utc::now(),
                "connection_id":connection_id,"profile_epoch":active.epoch,
                "market_id":query.market_id,"stream_epoch":query.stream_epoch,
                "reason":reason,"replaces_after_market_cursor":cursor,
                "market_cursor":checkpoint_cursor,"send_elapsed_us":sent_start.elapsed().as_micros(),
            }));
            cursor = checkpoint_cursor;
            socket.last_complete_cursor = cursor;
            socket.in_flight_cursor = None;
            last_confirmation = fence.confirmation_sequence;
            last_checkpoint_at = std::time::Instant::now();
            if !ready_sent {
                send_market_message(socket,api,
                    market_ready_frame(&active,&binding,&query,&fence,cursor)?).await?;
                ready_sent = true;
                ready_snapshot = None;
            }
            continue;
        }
        let page = page.context("market replay page unavailable without checkpoint")?;
        for group in &page.groups {
            if *profile_changed.borrow() != active.profile.profile_revision {
                // Do not start another group under a superseded profile.
                break;
            }
            let encoder_active = active.clone();
            let encoder_query = query.clone();
            let encoder_group = group.clone();
            let frame_bytes = api.limits.frame_bytes;
            let frames = tokio::task::spawn_blocking(move || market_group_frames(
                &encoder_active,&encoder_query,encoder_group.market_cursor,
                &encoder_group.group_sha256,encoder_group.payload.validated.events(),frame_bytes))
                .await.context("market group encoder stopped")??;
            if *profile_changed.borrow() != active.profile.profile_revision { break; }
            socket.in_flight_cursor = Some(group.market_cursor);
            let mut group_sent_at = None;
            for frame in frames {
                if poll_live_client_control(socket, api.limits.send_timeout).await? { return Ok(()); }
                group_sent_at = Some(send_market_frame(socket, api, frame).await?);
            }
            // A partial begin/chunk transmission is never an applied group.
            // Only enqueue a commit-complete receipt, without serialization
            // or waiting on diagnostics from this WS path.
            if let Some(sent_at) = group_sent_at {
                if ready_sent
                    && let Some(timing) = group.payload.timing_snapshot()
                    && let Some(elapsed_us) = slow_group_elapsed_us(&timing, sent_at)
                    && let Some(event) = group.payload.validated.events().last()
                    && let (Some(market_event_cursor), Some(event_id)) =
                        (event["cursor"].as_u64(), event["event_id"].as_str())
                {
                    let slow = SlowMarketGroup {
                        connection_id: connection_id.clone(),
                        profile_epoch: active.epoch.clone(),
                        market_id: query.market_id.clone(),
                        stream_epoch: query.stream_epoch.clone(),
                        market_cursor: group.market_cursor,
                        market_event_cursor,
                        event_id: event_id.to_owned(),
                        event_count: group.payload.validated.events().len(),
                        group_commit_sent_at: sent_at,
                        raw_last_to_send_us: elapsed_us,
                        timing,
                    };
                    if api.slow_group_tx.try_send(slow).is_err() {
                        api.slow_group_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let _ = api.timeline_tx.try_send(MarketTimelineGroup {
                    profile_epoch:active.epoch.clone(), market_id:query.market_id.clone(),
                    stream_epoch:query.stream_epoch.clone(), market_cursor:group.market_cursor,
                    group:group.payload.clone(), group_commit_sent_at:sent_at,
                });
            }
            cursor = group.market_cursor;
            socket.last_complete_cursor = cursor;
            socket.in_flight_cursor = None;
        }
        if *profile_changed.borrow() == active.profile.profile_revision {
            ensure!(cursor == page.next_cursor, "market replay page boundary differs");
        }
        if !ready_sent && cursor == ready_target { continue; }
        let snapshot = api.reader.capture_market(&query.market_id, &tokens)?;
        if snapshot.capacity_fenced { continue; }
        let confirmation = if ready_sent {
            market_confirmation_frame(&active, &binding, &query, &snapshot, cursor, last_confirmation)?
        } else { None };
        let confirmation_wait = confirmation_wait(last_confirmation_sent_at, std::time::Instant::now());
        if let Some(frame) = confirmation.as_ref().filter(|_| confirmation_wait.is_zero()) {
            if *profile_changed.borrow() != active.profile.profile_revision { continue; }
            send_market_message(socket, api, frame.clone()).await?;
            last_confirmation = snapshot.confirmation_sequence;
            last_confirmation_sent_at = std::time::Instant::now();
        }
        // A liquid market may have another page ready on every iteration.
        // Confirmation evidence remains an independent, memory-only prefix.
        if !page.caught_up { continue; }
        tokio::select! {
            result=changed.changed()=>result.context("publication closed")?,
            result=profile_changed.changed()=>result.context("BTC 5m profile control closed")?,
            _=tokio::time::sleep(confirmation_wait), if confirmation.is_some() && !confirmation_wait.is_zero() => {},
            message=socket.controls.recv()=> {
                if socket.handle_control(message,api.limits.send_timeout).await? { return Ok(()); }
            }
        }
    }
}
fn monotonic_us(start: std::time::Instant, end: std::time::Instant) -> u64 {
    u64::try_from(end.saturating_duration_since(start).as_micros()).unwrap_or(u64::MAX)
}

fn source_timing_json(t: &SourceBatchTiming) -> Value {
    json!({
        "raw_batch_first_received_at":t.raw_batch_first_received_at,
        "raw_batch_last_received_at":t.raw_batch_last_received_at,
        "actor_admitted_at":t.actor_admitted_at,
        "actor_started_at":t.actor_started_at,
        "actor_completed_at":t.actor_completed_at,
        "publish_call_started_at":t.publish_call_started_at,
        "shared_replay_visible_at":t.shared_replay_visible_at,
        "durable_at_if_observed":t.durable_at,
        "actor_queue_wait_us":monotonic_us(t.actor_admitted_mono,t.actor_started_mono),
        "actor_execution_us":monotonic_us(t.actor_started_mono,t.actor_completed_mono),
        "actor_done_to_publish_call_us":monotonic_us(t.actor_completed_mono,t.publish_call_started_mono),
        "publish_call_to_replay_visible_us":t.shared_replay_visible_mono
            .map(|visible|monotonic_us(t.publish_call_started_mono,visible)),
    })
}

fn confirmation_wait(
    last_sent_at: std::time::Instant,
    now: std::time::Instant,
) -> Duration {
    CONFIRMATION_MIN_INTERVAL
        .saturating_sub(now.saturating_duration_since(last_sent_at))
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn v6_formal_routes_emit_ordinary_snapshot_and_preserve_role_identity() {
        use prost::Message as _;
        use futures_util::{SinkExt,StreamExt};
        use marketcow_contracts::btc5m_market_local_v6::{self as live,wire};
        use tokio_tungstenite::tungstenite::{client::IntoClientRequest,Message as ClientMessage};
        let publication=crate::source_publication::Publication::start(0,None,BTreeMap::new(),
            8,131072,65536,|_|Ok(())).unwrap();
        let (router,control)=Api::router(publication.reader(),profile(),"epoch".into(),Limits {
            live_backlog_groups:8,live_backlog_age:Duration::from_millis(100),snapshot_bytes:65536,
            frame_bytes:4096,replay_bytes:65536,clients:3,send_timeout:Duration::from_secs(1),snapshot_lease_capacity:1,
        }).unwrap();
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address=listener.local_addr().unwrap();
        let server=tokio::spawn(async move {axum::serve(listener,router).await.unwrap();});
        let root=format!("http://{address}/v1/prediction-markets/polymarket/research/btc-5m/official-ws");
        let http=reqwest::Client::new();
        let response=tokio::time::timeout(Duration::from_secs(2),async {
            loop {
                let response=http.get(format!("{root}/market-streams")).send().await.unwrap();
                if response.status()==StatusCode::OK {break response}
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.unwrap();
        assert_eq!(response.status(),StatusCode::OK);
        let discovery=wire::MarketStreams::decode(response.bytes().await.unwrap()).unwrap();
        assert_eq!(discovery.schema_version,live::SCHEMA_VERSION);
        assert_eq!(discovery.delivery_budget.unwrap().maximum_frame_bytes,8388608);
        let binding=&discovery.markets[0];let key=binding.key.as_ref().unwrap();
        let response=http.get(format!("{root}/market-full-sync?market_id={}",key.market_id)).send().await.unwrap();
        assert_eq!(response.headers()["cache-control"],"no-store");
        let snapshot=wire::MarketFrame::decode(response.bytes().await.unwrap()).unwrap();
        assert_eq!(snapshot.stream_id,binding.stream_id);
        assert!(matches!(snapshot.body,Some(wire::market_frame::Body::Snapshot(_))));
        let mut request=format!("ws://{address}/v1/prediction-markets/polymarket/research/btc-5m/official-ws/market-stream?market_id={}&stream_id={}",key.market_id,binding.stream_id).into_client_request().unwrap();
        request.headers_mut().insert("sec-websocket-protocol",live::SUBPROTOCOL.parse().unwrap());
        let (mut ws,response)=tokio_tungstenite::connect_async(request).await.unwrap();
        assert_eq!(response.headers()["sec-websocket-protocol"],live::SUBPROTOCOL);
        let message=tokio::time::timeout(Duration::from_secs(2),ws.next()).await.unwrap().unwrap().unwrap();
        let ClientMessage::Binary(bytes)=message else {panic!("first ordinary snapshot");};
        let first=wire::MarketFrame::decode(bytes).unwrap();
        assert_eq!(first.stream_id,binding.stream_id);
        assert!(matches!(first.body,Some(wire::market_frame::Body::Snapshot(_))));
        assert_eq!(control.live_identity(&control.profile().unwrap().markets[0]).unwrap().1,binding.stream_id);
        let _=ws.close(None).await;server.abort();publication.finish().await.unwrap();
    }
    #[test]
    fn live_checkpoint_serializer_has_atomic_segments_and_semantic_hash() {
        use prost::Message as _;
        use sha2::{Digest,Sha256};
        let active = Active {profile:Arc::new(profile()),epoch:"1".repeat(32)};
        let binding = &active.profile.markets[0];
        let query = MarketStreamQuery {market_id:binding.market_id.clone(),
            stream_epoch:"2".repeat(32),profile_epoch:active.epoch.clone(),
            after_market_cursor:4,after_confirmation_sequence:0};
        let mut books = BTreeMap::new();
        for outcome in &binding.outcomes {
            let bids = json!([{"price":"0.4","size":"10"}]);
            let asks = json!([{"price":"0.6","size":"10"}]);
            let checksum = canonical_hash(&json!({"token_id":outcome.token_id,
                "tick_size":"0.01","bids":bids,"asks":asks}));
            books.insert(outcome.token_id.clone(),Arc::new(json!({
                "token_id":outcome.token_id,"condition_id":binding.condition_id,
                "book_epoch":"3".repeat(64),"sequence":1,
                "sequence_semantics":"deterministic_normalized",
                "exchange_at":"2026-10-09T00:00:00Z","received_at":"2026-10-09T00:00:00.100000Z",
                "tick_version":canonical_hash(&json!({"tick_size":"0.01"})),
                "tick_size":"0.01","bids":bids,"asks":asks,"last_trade_price":null,
                "state_checksum":checksum,"source_hash":"venue-fixture"})));
        }
        let snapshot = MarketMemorySnapshot {
            watermark:crate::source_publication::MarketStreamWatermark {
                stream_epoch:query.stream_epoch.clone(),market_id:query.market_id.clone(),
                published_cursor:64,durable_cursor:0,replay_floor:32},
            last_market_event_cursor:129,market:None,books,
            physical_book_cursors:BTreeMap::new(),
            trade_source_times:BTreeMap::new(),
            event_book_cursors:[("10".into(),128),("20".into(),129)].into(),
            market_book_cursors:[("10".into(),64),("20".into(),64)].into(),
            recoveries:BTreeMap::new(),unresolved_seed_gaps:BTreeSet::new(),capacity_fenced:false,
            confirmation_sequence:0,confirmation_facts:BTreeMap::new(),
            market_confirmation_versions:BTreeMap::new(),
        };
        let limits = Limits {snapshot_bytes:65536,frame_bytes:4096,replay_bytes:65536,
            clients:1,send_timeout:Duration::from_secs(1),snapshot_lease_capacity:1,
            live_backlog_groups:64,live_backlog_age:Duration::from_millis(500)};
        let frames = live_checkpoint_frames(&active,binding,&query,&snapshot,4,"replay_expired",&limits).unwrap();
        let decoded = frames.iter().map(|bytes|proto::MarketStreamFrame::decode(bytes.as_slice()).unwrap())
            .collect::<Vec<_>>();
        let proto::market_stream_frame::Body::CheckpointBegin(begin) = decoded[0].body.as_ref().unwrap() else {panic!("begin")};
        let proto::market_stream_frame::Body::CheckpointCommit(commit) = decoded.last().unwrap().body.as_ref().unwrap() else {panic!("commit")};
        assert_eq!(begin.checkpoint_id,commit.checkpoint_id);
        assert_eq!(begin.semantic_sha256,commit.semantic_sha256);
        let mut encoded = Vec::new();
        for (index,frame) in decoded[1..decoded.len()-1].iter().enumerate() {
            let proto::market_stream_frame::Body::CheckpointChunk(chunk) = frame.body.as_ref().unwrap() else {panic!("chunk")};
            assert_eq!(chunk.checkpoint_id,begin.checkpoint_id);
            assert_eq!(chunk.chunk_index,index as u32);
            encoded.extend_from_slice(&chunk.encoded_checkpoint_part);
        }
        assert_eq!(encoded.len() as u64,begin.encoded_bytes);
        assert_eq!(decoded.len() as u32-2,begin.chunk_count);
        let checkpoint = proto::LiveCheckpoint::decode(encoded.as_slice()).unwrap();
        assert_eq!(Sha256::digest(&checkpoint.semantic_preimage_json).as_slice(),begin.semantic_sha256);
        assert_eq!(checkpoint.last_market_event_cursor,129);
        let restored = checkpoint.snapshot.unwrap();
        assert_eq!(restored.books.len(),2);
        assert_eq!(restored.watermark.unwrap().published_market_cursor,Some(64));
        assert!(restored.books.iter().all(|book|book.confirmation.is_none()));
        let vector = json!({
            "schema_version":"marketcow.polymarket.live-checkpoint-vector.v1",
            "frames_hex":frames.iter().map(hex::encode).collect::<Vec<_>>()
        });
        let fixed:Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-live-checkpoint-v5.json")).unwrap();
        assert_eq!(vector,fixed);
        println!("MARKETCOW_LIVE_CHECKPOINT_VECTOR={vector}");
        assert!(live_checkpoint_frames(&active,binding,&query,&snapshot,64,"live_backlog",&limits).is_err());
        let mut recovering = snapshot.clone();
        for (index,outcome) in binding.outcomes.iter().enumerate() {
            recovering.recoveries.insert(outcome.token_id.clone(),json!({
                "recovery_id":format!("recovery-{index}"),
                "snapshot_cursor":if index == 0 {Some(128_u64)} else {None},
                "gap":{"code":"coverage_gap","detected_at":"2026-10-09T00:00:01Z",
                    "diagnostic_only":"not carried by RecoveryState"},
                "extra_native_detail":"not a typed recovery commitment"}));
        }
        let frames = live_checkpoint_frames(&active,binding,&query,&recovering,4,"replay_expired",&limits).unwrap();
        let mut encoded = Vec::new();
        for bytes in &frames {
            let frame = proto::MarketStreamFrame::decode(bytes.as_slice()).unwrap();
            if let Some(proto::market_stream_frame::Body::CheckpointChunk(chunk)) = frame.body {
                encoded.extend_from_slice(&chunk.encoded_checkpoint_part);
            }
        }
        let checkpoint = proto::LiveCheckpoint::decode(encoded.as_slice()).unwrap();
        let semantic:Value = serde_json::from_slice(&checkpoint.semantic_preimage_json).unwrap();
        assert_eq!(Sha256::digest(&checkpoint.semantic_preimage_json).as_slice(),checkpoint.semantic_sha256);
        for (row,baseline) in semantic["books"].as_array().unwrap().iter()
            .zip(checkpoint.snapshot.as_ref().unwrap().books.iter()) {
            let recovery = baseline.active_recovery.as_ref().unwrap();
            assert_eq!(row["active_recovery"],json!({"token_id":recovery.token_id,
                "recovery_id":recovery.recovery_id,"gap_code":recovery.gap_code,
                "gap_detected_at":recovery.gap_detected_at,"snapshot_cursor":recovery.snapshot_cursor}));
            assert_eq!(row["replay_baseline_valid"],json!(baseline.replay_baseline_valid));
            assert_eq!(row["active_recovery"].as_object().unwrap().len(),5);
        }
        let vector = json!({"schema_version":"marketcow.polymarket.live-checkpoint-recovery-vector.v1",
            "semantic_preimage":semantic,"frames_hex":frames.iter().map(hex::encode).collect::<Vec<_>>()});
        let fixed:Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-live-checkpoint-recovery-v5.json")).unwrap();
        assert_eq!(vector,fixed);
        println!("MARKETCOW_LIVE_CHECKPOINT_RECOVERY_VECTOR={vector}");
    }

    #[test]
    fn twelve_stream_clients_allow_three_per_market_plus_one_spare() {
        let limits = Limits {
            snapshot_bytes: 65_536,
            frame_bytes: 65_536,
            replay_bytes: 65_536,
            clients: 12,
            send_timeout: Duration::from_secs(1),
            snapshot_lease_capacity: 1, live_backlog_groups:64, live_backlog_age:Duration::from_millis(500),
        };
        assert!(limits.validate().is_ok());
        assert_eq!(market_stream_client_limit(limits.clients), 4);
        assert_eq!(market_stream_client_limit(4), 2);
        assert!(Limits { clients: 13, ..limits }.validate().is_err());
    }

    #[test]
    fn slow_group_log_only_selects_large_source_to_send_delay() {
        let raw_at = chrono::Utc.with_ymd_and_hms(2026, 10, 5, 12, 0, 0).unwrap();
        let mono = std::time::Instant::now();
        let timing = SourceBatchTiming {
            raw_batch_first_received_at: raw_at,
            raw_batch_last_received_at: raw_at,
            actor_admitted_at: raw_at,
            actor_started_at: raw_at,
            actor_completed_at: raw_at,
            actor_admitted_mono: mono,
            actor_started_mono: mono,
            actor_completed_mono: mono,
            publish_call_started_at: raw_at,
            publish_call_started_mono: mono,
            shared_replay_visible_at: Some(raw_at),
            shared_replay_visible_mono: Some(mono),
            durable_at: None,
        };
        assert_eq!(slow_group_elapsed_us(&timing,
            raw_at + chrono::Duration::microseconds(SLOW_GROUP_THRESHOLD_US - 1)), None);
        let sent_at = raw_at + chrono::Duration::microseconds(SLOW_GROUP_THRESHOLD_US);
        assert_eq!(slow_group_elapsed_us(&timing, sent_at), Some(SLOW_GROUP_THRESHOLD_US));
        let log = slow_group_json(&SlowMarketGroup {
            connection_id:"connection".into(), profile_epoch:"profile".into(),
            market_id:"market".into(), stream_epoch:"stream".into(), market_cursor:7,
            market_event_cursor:9, event_id:"event".into(), event_count:2,
            group_commit_sent_at:sent_at, raw_last_to_send_us:SLOW_GROUP_THRESHOLD_US,
            timing,
        });
        assert_eq!(log["stage"], "btc_5m_slow_market_group");
        assert_eq!(log["raw_last_to_group_commit_sent_us"], SLOW_GROUP_THRESHOLD_US);
        assert_eq!(log["source_batch_timing"]["actor_queue_wait_us"], 0);
        assert!(log.get("raw_payload").is_none());
    }

    #[test]
    fn archive_read_rechecks_writer_progress_without_accepting_unknown_durability() {
        use crate::source_publication::MarketStreamWatermark;
        let before = MarketStreamWatermark { market_id:"1".into(), stream_epoch:"epoch-a".into(),
            published_cursor:12, durable_cursor:10, replay_floor:0,
        };
        let mut after = before.clone();
        assert!(!super::archive_checkpoint_within_publication(&before, &after, 12));
        after.durable_cursor = 12;
        assert!(super::archive_checkpoint_within_publication(&before, &after, 12));
        assert!(!super::archive_checkpoint_within_publication(&before, &after, 13));
        after.stream_epoch = "epoch-b".into();
        assert!(!super::archive_checkpoint_within_publication(&before, &after, 12));
        after = before.clone();
        after.durable_cursor = 9;
        assert!(!super::archive_checkpoint_within_publication(&before, &after, 9));
        after.durable_cursor = 13;
        assert!(!super::archive_checkpoint_within_publication(&before, &after, 13));
    }
    use super::*;
    use std::collections::BTreeMap;
    use crate::source_publication::Publication;
    use chrono::{SecondsFormat, TimeZone};
    use marketcow_polymarket::discovery_source::{SnapshotBoundary, snapshot_event};
    use marketcow_runtime::discovery_source::PreparedSourceWriter;
    use rusqlite::{Connection, params};
    use prost::Message as _;
    use tower::ServiceExt;
    #[tokio::test]
    async fn ordinary_latest_snapshot_keeps_same_socket_then_delivers_next_state() {
        use futures_util::{SinkExt,StreamExt};
        use tokio_tungstenite::tungstenite::{client::IntoClientRequest,Message as ClientMessage};
        use marketcow_contracts::btc5m_market_local_v6::{self as live,wire};
        let mut p = profile();
        let start=Utc::now().timestamp().div_euclid(300)*300;
        for b in &mut p.markets {
            let offset=match b.role {WindowRole::Current=>0,WindowRole::Next=>300,WindowRole::Retiring=>-300};
            let at=start+offset;
            b.start_at=chrono::DateTime::from_timestamp(at,0).unwrap().to_rfc3339();
            b.end_at=chrono::DateTime::from_timestamp(at+300,0).unwrap().to_rfc3339();
            b.window_id=format!("btc-updown-5m-{at}");
        }
        p.profile_id=p.expected_id();
        let binding = p.markets[0].clone();
        let seed = json!({"latest_cursor":0,"catalog_revision":"a".repeat(64),
            "gaps":[],"books":[],"markets":[market(&binding.market_id,
                &binding.condition_id,&binding.outcomes[0].token_id,&binding.outcomes[1].token_id)]});
        let tokens = binding.outcomes.iter().map(|outcome|
            (outcome.token_id.clone(),binding.market_id.clone())).collect();
        let mut publication = Publication::start(0,Some(seed),tokens,64,
            8*1024*1024,1024*1024,|_|Ok(())).unwrap();
        let publish_pair = |publication:&Publication, group:u64| {
            let received_at = Utc::now();
            let events = binding.outcomes.iter().enumerate().map(|(index,outcome)|
                snapshot_event(&json!({"asset_id":outcome.token_id,"market":binding.condition_id,
                    "tick_size":"0.01","timestamp":received_at.timestamp_millis().to_string(),
                    "hash":format!("source-{group}-{index}"),
                    "bids":[{"price":"0.4","size":group.to_string()}],
                    "asks":[{"price":"0.6","size":"10"}]}),SnapshotBoundary {
                        market_id:&binding.market_id,condition_id:&binding.condition_id,
                        token_id:&outcome.token_id,recovery_id:"loopback",cursor:group*2-1+index as u64,
                        received_at}).unwrap()).collect();
            publication.publish_native_events(events,None).unwrap();
        };
        for group in 1..=7 { publish_pair(&publication,group); }
        let (router,_) = Api::router(publication.reader(),p,"loopback-profile".into(),Limits {
            snapshot_bytes:65536,frame_bytes:4096,replay_bytes:65536,clients:2,
            send_timeout:Duration::from_secs(1),snapshot_lease_capacity:1,
            live_backlog_groups:2,live_backlog_age:Duration::from_secs(10)}).unwrap();
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address=listener.local_addr().unwrap();
        let server=tokio::spawn(async move {axum::serve(listener,router).await.unwrap()});
        let http=reqwest::Client::new();
        let discovery=tokio::time::timeout(Duration::from_secs(3),async {
            loop {
                let response=http.get(format!("http://{address}/v1/prediction-markets/polymarket/research/btc-5m/official-ws/market-streams")).send().await.unwrap();
                if response.status()==StatusCode::OK {
                    break wire::MarketStreams::decode(response.bytes().await.unwrap()).unwrap();
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.unwrap();
        let discovered=discovery.markets.iter().find(|m|m.key.as_ref().unwrap().market_id==binding.market_id).unwrap();
        let mut request=format!("ws://{address}/v1/prediction-markets/polymarket/research/btc-5m/official-ws/market-stream?market_id={}&stream_id={}",
            binding.market_id,discovered.stream_id).into_client_request().unwrap();
        request.headers_mut().insert("sec-websocket-protocol",live::SUBPROTOCOL.parse().unwrap());
        let (mut client,_)=tokio_tungstenite::connect_async(request).await.unwrap();
        let initial=tokio::time::timeout(Duration::from_secs(3),async {
            loop {
                if let ClientMessage::Binary(bytes)=client.next().await.unwrap().unwrap() {
                    break wire::MarketFrame::decode(bytes).unwrap();
                }
            }
        }).await.unwrap();
        let Some(wire::market_frame::Body::Snapshot(snapshot))=initial.body else {panic!("ordinary initial snapshot");};
        assert_eq!(snapshot.source_status,"available");
        assert!(snapshot.books.iter().all(|book|book.bids[0].size=="7"));
        publish_pair(&publication,8);
        client.send(ClientMessage::Ping(vec![7,8].into())).await.unwrap();
        let mut pong=false;
        let mut updated=false;
        tokio::time::timeout(Duration::from_secs(3),async {
            while !pong || !updated {
                match client.next().await.unwrap().unwrap() {
                    ClientMessage::Pong(bytes)=>{assert_eq!(bytes.as_ref(),[7,8]);pong=true;}
                    ClientMessage::Binary(bytes)=>{
                        let frame=wire::MarketFrame::decode(bytes).unwrap();
                        assert_eq!(frame.stream_id,discovered.stream_id);
                        if frame.state_sequence<=initial.state_sequence {continue}
                        updated=match frame.body.unwrap() {
                            wire::market_frame::Body::Snapshot(snapshot)=>
                                snapshot.books.iter().all(|book|book.bids.first().is_some_and(|b|b.size=="8")),
                            wire::market_frame::Body::PriceChange(delta)=>
                                delta.changes.iter().any(|change|change.side=="BUY" && change.size=="8"),
                        };
                    }
                    other=>panic!("unexpected ordinary socket frame {other:?}"),
                }
            }
        }).await.unwrap();
        client.close(None).await.unwrap();
        server.abort();
        publication.finish().await.unwrap();
    }
    #[tokio::test]
    async fn busy_market_replay_loop_still_replies_to_client_ping() {
        use futures_util::{SinkExt, StreamExt};
        async fn probe(ws: WebSocketUpgrade) -> impl IntoResponse {
            ws.on_upgrade(|socket| async move {
                let mut socket = MarketClientIo::new(socket);
                // Model a replay loop that always has another page: there is
                // no caught-up wait or blocking receive in this loop.
                loop {
                    match poll_live_client_control(&mut socket, Duration::from_secs(1)).await {
                        Ok(false) => tokio::task::yield_now().await,
                        _ => break,
                    }
                }
            })
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/probe", get(probe)))
                .await.unwrap();
        });
        let (mut client, _) = tokio_tungstenite::connect_async(
            format!("ws://{address}/probe")).await.unwrap();
        client.send(tokio_tungstenite::tungstenite::Message::Ping(vec![1, 2, 3].into()))
            .await.unwrap();
        let response = tokio::time::timeout(Duration::from_secs(2), client.next())
            .await.unwrap().unwrap().unwrap();
        assert!(matches!(response,
            tokio_tungstenite::tungstenite::Message::Pong(bytes) if bytes.as_ref() == [1, 2, 3]));
        client.close(None).await.unwrap();
        server.abort();
    }
    #[test]
    fn market_group_frames_split_without_splitting_an_event_or_advancing_early() {
        let active = Active {profile:Arc::new(two_market_reducer_profile()), epoch:"profile-epoch".into()};
        let market_id = active.profile.markets[0].market_id.clone();
        let query = MarketStreamQuery {market_id, stream_epoch:"stream-epoch".into(),
            profile_epoch:active.epoch.clone(), after_market_cursor:0, after_confirmation_sequence:0};
        let source = crate::source_websocket::btc5m_reducer_fixture_sequence().unwrap()[0].clone();
        let events = (0..10).map(|index| {
            let mut event = source.clone();
            event["cursor"] = json!(index + 1);
            event.as_object_mut().unwrap().remove("event_id");
            event["event_id"] = json!(canonical_hash(&event));
            event
        }).collect::<Vec<_>>();
        let frames = market_group_frames(&active, &query, 7, &"a".repeat(64), &events, 4096).unwrap();
        assert!(frames.len() > 3);
        assert!(frames.iter().all(|frame| frame.len() <= 4096));
        let parsed = frames.iter().map(|frame| proto::MarketStreamFrame::decode(frame.as_slice()).unwrap())
            .collect::<Vec<_>>();
        assert!(matches!(parsed.first().unwrap().body,
            Some(proto::market_stream_frame::Body::GroupBegin(_))));
        assert!(matches!(parsed.last().unwrap().body,
            Some(proto::market_stream_frame::Body::GroupCommit(_))));
        if let Some(proto::market_stream_frame::Body::GroupCommit(commit)) = &parsed.last().unwrap().body {
            assert_eq!(commit.next_after_market_cursor, 7);
        }
        let mut rebuilt = Vec::new();
        for (index, frame) in parsed[1..parsed.len()-1].iter().enumerate() {
            let Some(proto::market_stream_frame::Body::GroupChunk(chunk)) = &frame.body else {
                panic!("expected market group chunk")
            };
            assert_eq!(chunk.chunk_index, index as u32);
            assert_eq!(chunk.chunk_count, (parsed.len()-2) as u32);
            rebuilt.extend_from_slice(&chunk.encoded_events_part);
        }
        let rebuilt = proto::MarketEventBundle::decode(rebuilt.as_slice()).unwrap();
        assert_eq!(rebuilt.events, events.iter().map(proto_convert::market_event)
            .collect::<Result<Vec<_>>>().unwrap());
        let mut too_large = source;
        too_large["canonical_payload"]["source_hash"] = json!("x".repeat(5000));
        assert!(market_group_frames(&active, &query, 8, &"b".repeat(64),
            &[too_large], 4096).is_ok());
    }
    #[test]
    fn native_market_group_wire_uses_local_event_cursor() {
        let active = Active {profile:Arc::new(two_market_reducer_profile()), epoch:"profile-epoch".into()};
        let market_id = active.profile.markets[0].market_id.clone();
        let query = MarketStreamQuery {market_id:market_id.clone(),
            stream_epoch:"stream-epoch".into(), profile_epoch:active.epoch.clone(),
            after_market_cursor:0, after_confirmation_sequence:0};
        let source = crate::source_websocket::btc5m_reducer_fixture_sequence().unwrap()[0].clone();
        let after = source["cursor"].as_u64().unwrap() - 1;
        let tokens = active.profile.markets.iter().flat_map(|binding|
            binding.outcomes.iter().map(|outcome|
                (outcome.token_id.clone(), binding.market_id.clone())))
            .collect::<BTreeMap<_, _>>();
        let validated = marketcow_runtime::discovery_source::ValidatedSourceBatch::new_native(
            vec![source], false, after, &tokens, 65536).unwrap()
            .rebase_after(0, 65536).unwrap();
        let events = validated.events().to_vec();
        assert_eq!(events[0]["cursor"], 1);
        let group_sha256 = marketcow_runtime::discovery_source::market_group_hash(
            &market_id, &query.stream_epoch, 1, &events).unwrap();
        let frames = market_group_frames(&active, &query, 1, &group_sha256,
            &events, 65536).unwrap();
        let frames = frames.iter().map(|frame| proto::MarketStreamFrame::decode(frame.as_slice()).unwrap())
            .collect::<Vec<_>>();
        let Some(proto::market_stream_frame::Body::GroupChunk(chunk)) = &frames[1].body else {
            panic!("expected market group chunk")
        };
        let bundle = proto::MarketEventBundle::decode(chunk.encoded_events_part.as_slice()).unwrap();
        assert_eq!(bundle.events[0].market_event_cursor, 1);
        assert_eq!(bundle.events[0], proto_convert::market_event(&events[0]).unwrap());
        let Some(proto::market_stream_frame::Body::GroupCommit(commit)) = &frames[2].body else {
            panic!("expected market group commit")
        };
        assert_eq!(commit.next_after_market_cursor, 1);
    }
    #[tokio::test]
    async fn renewal_heartbeat_preserves_fail_closed_phase_and_error() {
        let publication = Publication::start(0, None, std::collections::BTreeMap::new(),
            8, 131072, 65536, |_| Ok(())).unwrap();
        let (_, control) = Api::router(publication.reader(), profile(), "epoch".into(), Limits {
            snapshot_bytes: 65536, frame_bytes: 65536, replay_bytes: 65536,
            clients: 1, send_timeout: Duration::from_secs(1), snapshot_lease_capacity: 1, live_backlog_groups:64, live_backlog_age:Duration::from_millis(500),
        }).unwrap();
        let first = Utc.with_ymd_and_hms(2026, 10, 1, 22, 40, 0).unwrap();
        let renewed = first + chrono::Duration::seconds(40);
        control.set_manager_status(json!({"schema_version":
            "marketcow.polymarket.btc-5m-manager-status.v1",
            "phase":"fail_closed","error":"discovery failed",
            "updated_at":first.to_rfc3339()})).unwrap();
        control.note_manager_renewal_at(renewed).unwrap();
        let status = control.manager_status().unwrap();
        assert_eq!(status["phase"], "fail_closed");
        assert_eq!(status["error"], "discovery failed");
        assert_eq!(status["updated_at"], renewed.to_rfc3339_opts(SecondsFormat::Millis, true));
        assert!(control.note_manager_renewal_at(first).is_ok());
        control.set_manager_status(json!({"schema_version":"wrong"})).unwrap();
        assert!(control.note_manager_renewal_at(renewed).is_err());
        publication.finish().await.unwrap();
    }
    #[test]
    fn management_contract_separates_capture_from_process_and_fences_stale_manager() {
        let now = Utc.with_ymd_and_hms(2026, 9, 26, 5, 31, 0).unwrap();
        let mut two_windows = profile();
        let mut next = two_windows.markets[0].clone();
        next.role = WindowRole::Next;
        next.window_id = "2026-09-26T05:35:00Z/2026-09-26T05:40:00Z".into();
        next.start_at = "2026-09-26T05:35:00Z".into();
        next.end_at = "2026-09-26T05:40:00Z".into();
        next.market_id = "4944031".into();
        two_windows.markets.push(next);
        let active = Active { profile: Arc::new(two_windows), epoch: "profile-epoch".into() };
        let publication = json!({"source_ready":true,"persistence_state":"running",
            "metadata_market_count":2048,"publication_binding_tokens":500,"hot_binding_tokens":500,
            "wire_tokens":4,"wire_sockets":1,"phase_counts_authoritative":false});
        let manager = json!({"phase":"running","updated_at":now.to_rfc3339()});
        let healthy = management_health("process-a", Some(&active), &manager,
            publication.clone(), now);
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-management-health-v1.json"
        )).unwrap();
        assert_eq!(healthy, fixture);
        assert_eq!(healthy["schema_version"], "marketcow.btc-5m-management-health.v1");
        assert_eq!(healthy["data_state"], "capture_required");
        assert_eq!(healthy["entry_ready"], Value::Null);
        assert_eq!(healthy["generation"]["process_instance_id"], "process-a");
        assert_eq!(healthy["generation"]["profile_epoch"], "profile-epoch");
        assert_eq!(healthy["windows"][0]["market_id"], "4944030");
        assert_eq!(healthy["windows"][1]["market_id"], "4944031");
        assert_eq!(healthy["windows"][1]["profile_revision"], 1);
        assert_eq!(healthy["windows"][0]["readiness_reason"], "capture_required");
        assert_eq!(healthy["resources"]["hot_binding_tokens"]["used"], 500);
        assert_eq!(healthy["resources"]["wire_tokens"]["used"], 4);
        assert_eq!(healthy["resources"]["phase_counts_authoritative"], false);
        assert_eq!(DateTime::parse_from_rfc3339(healthy["valid_until"].as_str().unwrap()).unwrap()
            .with_timezone(&Utc), now + chrono::Duration::seconds(60));

        let mut previously_authoritative = publication.clone();
        previously_authoritative["phase_counts_authoritative"] = json!(true);
        previously_authoritative["prepared_tokens"] = json!(496);
        previously_authoritative["active_tokens"] = json!(4);
        previously_authoritative["draining_tokens"] = json!(0);
        previously_authoritative["hot_token_limit"] = json!(4096);
        previously_authoritative["wire_socket_limit"] = json!(8);
        let healthy_authoritative = management_health("process-a", Some(&active), &manager,
            previously_authoritative.clone(), now);
        let healthy_fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-management-health-v1-healthy.json"
        )).unwrap();
        assert_eq!(healthy_authoritative, healthy_fixture);
        let stale = management_health("process-a", Some(&active), &manager,
            previously_authoritative, now + chrono::Duration::seconds(61));
        let stale_fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-management-health-v1-stale.json"
        )).unwrap();
        assert_eq!(stale, stale_fixture);
        assert_eq!(stale["blocked_reasons"], json!(["manager_stale"]));
        assert_eq!(stale["resources"]["phase_counts_authoritative"], false);
        assert_eq!(stale["valid_until"], healthy["valid_until"]);
        let no_capture = management_health("process-a", Some(&active), &manager,
            json!({"source_ready":false,"persistence_state":"running"}), now);
        assert_eq!(no_capture["blocked_reasons"], json!(["source_not_ready"]));
        assert_eq!(no_capture["windows"][0]["ready"], false);

        let lease = management_health("process-a", Some(&active),
            &json!({"phase":"fail_closed","updated_at":now.to_rfc3339(),
                "blocked_reason_code":"lease_invalid"}), publication.clone(), now);
        assert_eq!(lease["blocked_reasons"], json!(["lease_invalid","manager_failed"]));
        let capacity = management_health("process-a", Some(&active),
            &json!({"phase":"fail_closed","updated_at":now.to_rfc3339(),
                "blocked_reason_code":"capacity_exhausted"}), publication.clone(), now);
        assert_eq!(capacity["blocked_reasons"], json!(["capacity_exhausted","manager_failed"]));
        let recovering = management_health("process-a", Some(&active),
            &json!({"phase":"reconciling","updated_at":now.to_rfc3339()}), publication.clone(), now);
        assert_eq!(recovering["blocked_reasons"], json!(["manager_recovering"]));
        let no_manager = management_health("process-a", Some(&active), &Value::Null,
            publication, now);
        let missing_fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-management-health-v1-manager-missing.json"
        )).unwrap();
        assert_eq!(no_manager, missing_fixture);
        assert_eq!(no_manager["blocked_reasons"], json!(["manager_missing"]));
        assert_eq!(no_manager["manager_updated_at"], Value::Null);
        assert_eq!(no_manager["valid_until"], Value::Null);
        let no_timestamp = management_health("process-a", Some(&active),
            &json!({"phase":"running"}), json!({"source_ready":true}), now);
        assert_eq!(no_timestamp["valid_until"], Value::Null);
        assert_eq!(no_timestamp["blocked_reasons"], json!(["manager_stale"]));
        let later = now + chrono::Duration::minutes(5);
        let expired = management_health("process-a", Some(&active),
            &json!({"phase":"running","updated_at":later.to_rfc3339()}),
            json!({"source_ready":true,"persistence_state":"running"}), later);
        assert_eq!(expired["blocked_reasons"], json!(["current_window_expired"]));
        assert_eq!(expired["windows"][0]["readiness_reason"], "window_expired");
        let failed_disk = management_health("process-a", Some(&active), &manager,
            json!({"source_ready":true,"persistence_state":"failed"}), now);
        assert_eq!(failed_disk["blocked_reasons"], json!(["persistence_failed"]));
        let actual = std::collections::BTreeMap::from([
            ("running_without_capture", &healthy),
            ("source_paused", &no_capture),
            ("manager_exited_and_timeout_elapsed", &stale),
            ("manager_reconciling", &recovering),
            ("capacity_error", &capacity),
            ("lease_invalid", &lease),
            ("persistence_failed", &failed_disk),
            ("current_window_expired", &expired),
            ("manager_missing", &no_manager),
        ]);
        let cases: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-management-health-v1-cases.json"
        )).unwrap();
        for case in cases["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let result = actual[name];
            assert_eq!(result["blocked_reasons"], case["blocked_reasons"], "{name}");
            assert_eq!(result["data_state"], case["data_state"], "{name}");
            assert_eq!(result["windows"][0]["readiness_reason"],
                case["current_readiness_reason"], "{name}");
        }
    }
    fn market(id: &str, condition: &str, up: &str, down: &str) -> Value {
        json!({"identity":{"market_id":id,"condition_id":condition,
        "outcomes":[{"outcome":"Up","token_id":up},{"outcome":"Down","token_id":down}]},"active":true,"closed":false,"relations":[],
        "lifecycle_state":"active","rules":{"rules_complete":true,"instrument":{"complete":true,"price_increment":"0.01","revision":"i","provenance":[]},
        "fee_schedule":{"complete":true,"schedule_id":"f"}}})
    }
    fn book(token: &str, condition: &str, at: &str) -> Value {
        json!({"token_id":token,"condition_id":condition,"received_at":at,
        "exchange_at":at,"state_checksum":"b".repeat(64),"book_epoch":"e","bids":[{"price":"0.40","size":"10"}],
        "asks":[{"price":"0.60","size":"10"}],"last_trade_price":"0.50"})
    }
    #[test]
    fn market_snapshot_recovery_cannot_promote_an_old_book_to_replay_baseline() {
        let token = "101".to_owned();
        let now = Utc.with_ymd_and_hms(2026, 9, 26, 5, 30, 2).unwrap();
        let mut snapshot = MarketMemorySnapshot {
            watermark:crate::source_publication::MarketStreamWatermark {
                stream_epoch:"stream".into(),market_id:"4944030".into(),
                published_cursor:2,durable_cursor:1,replay_floor:0,
            },
            last_market_event_cursor:43,
            market:None,
            books:BTreeMap::from([(token.clone(), Arc::new(book(&token,"condition",
                "2026-09-26T05:30:01Z")))]),
            physical_book_cursors:BTreeMap::from([(token.clone(),43)]),
            trade_source_times:BTreeMap::new(),
            event_book_cursors:BTreeMap::from([(token.clone(),43)]),
            market_book_cursors:BTreeMap::from([(token.clone(),2)]),
            recoveries:BTreeMap::new(),unresolved_seed_gaps:BTreeSet::new(),capacity_fenced:false,
            confirmation_sequence:0, confirmation_facts:BTreeMap::new(),
            market_confirmation_versions:BTreeMap::new(),
        };
        assert!(market_replay_baseline_valid(&snapshot, &token, now));
        snapshot.recoveries.insert(token.clone(), json!({"recovery_id":"r",
            "snapshot_cursor":44}));
        assert!(!market_replay_baseline_valid(&snapshot, &token, now));
        snapshot.recoveries.get_mut(&token).unwrap()["snapshot_cursor"] = json!(43);
        assert!(market_replay_baseline_valid(&snapshot, &token, now));
        snapshot.physical_book_cursors.remove(&token);
        assert!(market_replay_baseline_valid(&snapshot, &token, now),
            "a native recovery baseline does not require an uncommitted archive position");
        snapshot.unresolved_seed_gaps.insert(token.clone());
        assert!(!market_replay_baseline_valid(&snapshot, &token, now));
    }
    #[test]
    fn market_quote_diagnostics_keep_one_sided_stale_and_recovery_distinct() {
        let token = "101".to_owned();
        let now = Utc.with_ymd_and_hms(2026, 9, 26, 5, 30, 2).unwrap();
        let mut snapshot = MarketMemorySnapshot {
            watermark:crate::source_publication::MarketStreamWatermark {
                stream_epoch:"stream".into(),market_id:"4944030".into(),
                published_cursor:2,durable_cursor:1,replay_floor:0,
            },
            last_market_event_cursor:43, market:None,
            books:BTreeMap::from([(token.clone(), Arc::new(book(&token,"condition",
                "2026-09-26T05:30:01Z")))]),
            physical_book_cursors:BTreeMap::new(),
            trade_source_times:BTreeMap::new(),
            event_book_cursors:BTreeMap::from([(token.clone(),43)]),
            market_book_cursors:BTreeMap::from([(token.clone(),2)]),
            recoveries:BTreeMap::new(),unresolved_seed_gaps:BTreeSet::new(),capacity_fenced:false,
            confirmation_sequence:0,confirmation_facts:BTreeMap::new(),
            market_confirmation_versions:BTreeMap::new(),
        };
        let (ready, detail) = market_token_quote_readiness(&snapshot, &token, &now, 5_000);
        assert!(ready);
        assert_eq!(detail["reasons"], json!([]));
        assert_eq!(detail["age_ms"], 1_000);

        let mut one_sided = book(&token, "condition", "2026-09-26T05:30:01Z");
        one_sided["bids"] = json!([]);
        snapshot.books.insert(token.clone(), Arc::new(one_sided));
        let (ready, detail) = market_token_quote_readiness(&snapshot, &token, &now, 5_000);
        assert!(!ready);
        assert_eq!(detail["reasons"], json!(["incomplete_book"]));
        assert_eq!(detail["bid_levels"], 0);
        assert_eq!(detail["ask_levels"], 1);

        let stale = book(&token, "condition", "2026-09-26T05:29:50Z");
        snapshot.books.insert(token.clone(), Arc::new(stale));
        let (_, detail) = market_token_quote_readiness(&snapshot, &token, &now, 5_000);
        assert_eq!(detail["reasons"], json!(["book_stale"]));
        snapshot.recoveries.insert(token.clone(), json!({"snapshot_cursor":44}));
        let (_, detail) = market_token_quote_readiness(&snapshot, &token, &now, 5_000);
        assert_eq!(detail["reasons"], json!(["recovery_in_progress","book_stale"]));
        snapshot.recoveries.get_mut(&token).unwrap()["snapshot_cursor"] = json!(43);
        let (_, detail) = market_token_quote_readiness(&snapshot, &token, &now, 5_000);
        assert_eq!(detail["reasons"], json!(["recovery_in_progress","book_stale"]));
        snapshot.unresolved_seed_gaps.insert(token.clone());
        let (_, detail) = market_token_quote_readiness(&snapshot, &token, &now, 5_000);
        assert_eq!(detail["reasons"], json!(["source_gap","recovery_in_progress","book_stale"]));
    }
    #[test]
    fn local_receipt_and_confirmation_receipt_never_refresh_supplier_quote_age() {
        let token = "101".to_owned();
        let now = Utc.with_ymd_and_hms(2026, 9, 26, 5, 30, 2).unwrap();
        let mut stale = book(&token, "condition", "2026-09-26T05:29:50Z");
        stale["received_at"] = json!("2026-09-26T05:30:01Z");
        stale["sequence"] = json!(1);
        let snapshot = MarketMemorySnapshot {
            watermark:crate::source_publication::MarketStreamWatermark {
                stream_epoch:"stream".into(),market_id:"4944030".into(),
                published_cursor:2,durable_cursor:1,replay_floor:0,
            },
            last_market_event_cursor:43, market:None,
            books:BTreeMap::from([(token.clone(), Arc::new(stale))]),
            physical_book_cursors:BTreeMap::new(),
            trade_source_times:BTreeMap::new(),
            event_book_cursors:BTreeMap::from([(token.clone(),43)]),
            market_book_cursors:BTreeMap::from([(token.clone(),2)]),
            recoveries:BTreeMap::new(),unresolved_seed_gaps:BTreeSet::new(),capacity_fenced:false,
            confirmation_sequence:1,
            confirmation_facts:BTreeMap::from([(token.clone(),Arc::new(json!({
                "token_id":token.clone(),"condition_id":"condition","book_epoch":"e",
                "sequence":1,"state_checksum":"b".repeat(64),
                "book_quote_source_at":"2026-09-26T05:29:50Z",
                "confirmation_source_at":"2026-09-26T05:29:51Z",
                "confirmation_received_at":"2026-09-26T05:30:02Z",
                "confirmation_source":"polymarket_websocket",
                "confirmation_evidence_sha256":"c".repeat(64),
            })))]),
            market_confirmation_versions:BTreeMap::from([(token.clone(),1)]),
        };
        let (ready, detail) = market_token_quote_readiness(&snapshot, &token, &now, 5_000);
        assert!(!ready);
        assert_eq!(detail["age_ms"], 11_000);
        assert_eq!(detail["reasons"], json!(["book_stale"]));
    }
    fn profile() -> Profile {
        let mut p = Profile {
            schema_version: PROFILE_SCHEMA.into(),
            profile_id: String::new(),
            profile_revision: 1,
            catalog_revision: "a".repeat(64),
            maximum_markets: 3,
            maximum_quote_age_ms: 5_000,
            maximum_tokens: 6,
            markets: vec![MarketBinding {
                role: WindowRole::Current,
                window_id: "2026-09-26T05:30:00Z/2026-09-26T05:35:00Z".into(),
                start_at: "2026-09-26T05:30:00Z".into(),
                end_at: "2026-09-26T05:35:00Z".into(),
                market_id: "4944030".into(),
                condition_id: format!("0x{}", "1".repeat(64)),
                outcomes: [
                    OutcomeBinding {
                        outcome: "Up".into(),
                        token_id: "10".into(),
                    },
                    OutcomeBinding {
                        outcome: "Down".into(),
                        token_id: "20".into(),
                    },
                ],
                rule_sha256: "c".repeat(64),
            }],
        };
        p.profile_id = p.expected_id();
        p
    }
    fn two_market_reducer_profile() -> Profile {
        let mut p = profile();
        p.markets[0].outcomes[0].token_id = "101".into();
        p.markets[0].outcomes[1].token_id = "102".into();
        p.markets.push(MarketBinding {
            role: WindowRole::Next,
            window_id: "2026-09-26T05:35:00Z/2026-09-26T05:40:00Z".into(),
            start_at: "2026-09-26T05:35:00Z".into(),
            end_at: "2026-09-26T05:40:00Z".into(),
            market_id: "4944031".into(),
            condition_id: format!("0x{}", "2".repeat(64)),
            outcomes: [
                OutcomeBinding { outcome: "Up".into(), token_id: "201".into() },
                OutcomeBinding { outcome: "Down".into(), token_id: "202".into() },
            ],
            rule_sha256: "d".repeat(64),
        });
        p.profile_id = p.expected_id();
        p
    }
    #[test]
    fn timeline_archive_retains_exact_recent_cursor_and_evicts_oldest() {
        let mut archive = TimelineArchive::default();
        for cursor in 1..=TIMELINE_RECEIPT_COUNT as u64 + 1 {
            archive.push("epoch-a", "market-a", "stream-a", cursor, "event-a", json!({
                "connection_id":"client-a","market_event_cursor":cursor,"send_outcome":"ok"
            }));
        }
        assert!(archive.lookup("epoch-a", "market-a", "stream-a", 1, "event-a").is_empty());
        assert_eq!(archive.lookup("epoch-a", "market-a", "stream-a", 2, "event-a").len(), 1);
        assert!(archive.lookup("epoch-b", "market-a", "stream-a", 2, "event-a").is_empty());
        assert!(archive.lookup("epoch-a", "market-b", "stream-a", 2, "event-a").is_empty());
        assert!(archive.lookup("epoch-a", "market-a", "stream-b", 2, "event-a").is_empty());
        assert!(archive.lookup("epoch-a", "market-a", "stream-a", 2, "event-b").is_empty());
        let newest = TIMELINE_RECEIPT_COUNT as u64 + 1;
        archive.push("epoch-a", "market-a", "stream-a", newest, "event-a",
            json!({"connection_id":"client-b"}));
        let receipts = archive.lookup("epoch-a", "market-a", "stream-a", newest, "event-a");
        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts[0]["connection_id"], "client-a");
        assert_eq!(receipts[1]["connection_id"], "client-b");
        assert!(archive.bytes <= TIMELINE_RECEIPT_BYTES);
    }
    #[tokio::test]
    async fn native_market_replay_expiry_is_local_to_one_market() {
        let profile = two_market_reducer_profile();
        let first = &profile.markets[0];
        let second = &profile.markets[1];
        let markets = profile.markets.iter().map(|binding| market(
            &binding.market_id, &binding.condition_id,
            &binding.outcomes[0].token_id, &binding.outcomes[1].token_id,
        )).collect::<Vec<_>>();
        let tokens = profile.markets.iter().flat_map(|binding| binding.outcomes.iter()
            .map(|outcome| (outcome.token_id.clone(), binding.market_id.clone())))
            .collect();
        let mut publication = Publication::start_managed_with_replay(42,
            Some(json!({"latest_cursor":42,"catalog_revision":profile.catalog_revision,
                "live_stream_connected":true,"gaps":[],"books":[],"markets":markets})),
            tokens, 1, 1024 * 1024, 2, 16 * 1024 * 1024, 512 * 1024,
            |_| Ok(())).unwrap();
        for (cursor, event) in crate::source_websocket::btc5m_reducer_fixture_sequence()
            .unwrap().iter().take(2).enumerate() {
            publication.publish_native_events(vec![event.clone()], None).unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let durable = publication.reader()
                        .market_stream_watermark(&first.market_id).unwrap().unwrap().durable_cursor;
                    if durable >= cursor as u64 + 1 { break; }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }).await.unwrap();
        }
        let second_events = second.outcomes.iter().enumerate().map(|(index, outcome)|
            snapshot_event(&json!({
                "asset_id":outcome.token_id,"market":second.condition_id,
                "tick_size":"0.01","timestamp":"1790399999000",
                "hash":format!("second-market-{index}"),
                "bids":[{"price":"0.4","size":"10"}],
                "asks":[{"price":"0.6","size":"10"}]}),
                SnapshotBoundary { market_id:&second.market_id,
                    condition_id:&second.condition_id,
                    token_id:&outcome.token_id,
                    recovery_id:"second-market",cursor:45 + index as u64,
                    received_at:Utc.with_ymd_and_hms(2026, 9, 26, 5, 30, 1).unwrap() })
        ).collect::<std::result::Result<Vec<_>, _>>().unwrap();
        publication.publish_native_events(second_events, None).unwrap();
        let reader = publication.reader();
        let first_watermark = reader.market_stream_watermark(&first.market_id)
            .unwrap().unwrap();
        let second_watermark = reader.market_stream_watermark(&second.market_id)
            .unwrap().unwrap();
        assert_eq!(first_watermark.replay_floor, 1);
        assert_eq!(second_watermark.replay_floor, 0);
        assert!(reader.replay_market(&first.market_id, &first_watermark.stream_epoch,
            0, 2, 512 * 1024).is_err());
        assert_eq!(reader.replay_market(&second.market_id, &second_watermark.stream_epoch,
            0, 2, 512 * 1024).unwrap().next_cursor, 1);
        publication.finish().await.unwrap();
    }
    #[tokio::test]
    async fn profile_rotation_is_cas_bound_and_requires_prepared_ordered_identity() {
        let seed = json!({"latest_cursor":0,"catalog_revision":"a".repeat(64),"gaps":[],"books":[],
            "markets":[market("4944030",&format!("0x{}", "1".repeat(64)),"10","20"),market("4944031",&format!("0x{}", "2".repeat(64)),"30","40")]});
        let publication = Publication::start(
            0,
            Some(seed),
            [
                ("10".into(), "4944030".into()),
                ("20".into(), "4944030".into()),
                ("30".into(), "4944031".into()),
                ("40".into(), "4944031".into()),
            ]
            .into(),
            8,
            131072,
            65536,
            |_| Ok(()),
        )
        .unwrap();
        let now_start = Utc::now().timestamp().div_euclid(300) * 300;
        let at = |seconds| {
            Utc.timestamp_opt(seconds, 0)
                .single()
                .unwrap()
                .to_rfc3339_opts(SecondsFormat::Secs, true)
        };
        let mut initial = profile();
        initial.markets[0].start_at = at(now_start - 300);
        initial.markets[0].end_at = at(now_start);
        initial.markets[0].window_id = format!(
            "{}/{}",
            initial.markets[0].start_at, initial.markets[0].end_at
        );
        initial.profile_id = initial.expected_id();
        let (_, control) = Api::router(
            publication.reader(),
            initial.clone(),
            "epoch-one".into(),
            Limits {
                snapshot_bytes: 65536,
                frame_bytes: 65536,
                replay_bytes: 65536,
                clients: 1,
                send_timeout: Duration::from_secs(1),
                snapshot_lease_capacity: 1, live_backlog_groups:64, live_backlog_age:Duration::from_millis(500),
            },
        )
        .unwrap();
        let mut next = initial.clone();
        next.profile_revision = 2;
        next.markets[0].role = WindowRole::Retiring;
        next.markets.push(MarketBinding {
            role: WindowRole::Current,
            window_id: format!("{}/{}", at(now_start), at(now_start + 300)),
            start_at: at(now_start),
            end_at: at(now_start + 300),
            market_id: "4944031".into(),
            condition_id: format!("0x{}", "2".repeat(64)),
            outcomes: [
                OutcomeBinding {
                    outcome: "Up".into(),
                    token_id: "30".into(),
                },
                OutcomeBinding {
                    outcome: "Down".into(),
                    token_id: "40".into(),
                },
            ],
            rule_sha256: "d".repeat(64),
        });
        next.profile_id = next.expected_id();
        assert!(
            control
                .publish("wrong", initial.profile_revision, next.clone())
                .is_err()
        );
        let mut expired = next.clone();
        expired.markets[0].start_at = at(now_start - 600);
        expired.markets[0].end_at = at(now_start - 300);
        expired.markets[0].window_id = format!(
            "{}/{}",
            expired.markets[0].start_at, expired.markets[0].end_at
        );
        expired.markets[1].start_at = at(now_start - 300);
        expired.markets[1].end_at = at(now_start);
        expired.markets[1].window_id = format!(
            "{}/{}",
            expired.markets[1].start_at, expired.markets[1].end_at
        );
        expired.profile_id = expired.expected_id();
        let error = control
            .publish(&initial.profile_id, initial.profile_revision, expired)
            .unwrap_err();
        assert!(error.to_string().contains("current window"));
        assert_eq!(control.profile().unwrap().profile_id, initial.profile_id);
        assert!(control.publish(&initial.profile_id,initial.profile_revision,next.clone()).is_err(),
            "ordinary 1→2 publication must not skip adjacent rotation validation");
        assert_eq!(control.profile().unwrap().profile_id,initial.profile_id);
        let receipt = control
            .publish_restart_reconciled(&initial.profile_id, initial.profile_revision, next.clone())
            .unwrap();
        assert_eq!(receipt["profile_id"], next.profile_id);
        assert_eq!(receipt["requires_new_full_sync"], true);
        assert_ne!(receipt["epoch"], "epoch-one");
        publication.finish().await.unwrap();
    }
    #[tokio::test]
    async fn two_window_publish_cas_rejects_unrelated_prepared_current() {
        let condition = |digit: char| format!("0x{}", digit.to_string().repeat(64));
        let seed = json!({"latest_cursor":0,"catalog_revision":"a".repeat(64),
            "gaps":[],"books":[],"markets":[
                market("4944030",&condition('1'),"10","20"),
                market("4944031",&condition('2'),"30","40"),
                market("4944032",&condition('3'),"50","60")]});
        let mut publication = Publication::start(0,Some(seed),[
            ("10".into(),"4944030".into()),("20".into(),"4944030".into()),
            ("30".into(),"4944031".into()),("40".into(),"4944031".into()),
            ("50".into(),"4944032".into()),("60".into(),"4944032".into()),
        ].into(),8,131072,65536,|_|Ok(())).unwrap();
        let start = Utc::now().timestamp().div_euclid(300)*300;
        let at = |seconds| Utc.timestamp_opt(seconds,0).single().unwrap()
            .to_rfc3339_opts(SecondsFormat::Secs,true);
        let binding = |id: &str, digit: char, up: &str, down: &str,
                       role: WindowRole, begins: i64| MarketBinding {
            role,window_id:format!("{}/{}",at(begins),at(begins+300)),
            start_at:at(begins),end_at:at(begins+300),market_id:id.into(),
            condition_id:condition(digit),outcomes:[
                OutcomeBinding{outcome:"Up".into(),token_id:up.into()},
                OutcomeBinding{outcome:"Down".into(),token_id:down.into()}],
            rule_sha256:"c".repeat(64),
        };
        let mut incumbent=profile();
        incumbent.markets=vec![
            binding("4944030",'1',"10","20",WindowRole::Current,start-300),
            binding("4944031",'2',"30","40",WindowRole::Next,start),
        ];
        incumbent.profile_id=incumbent.expected_id();
        let carried = &incumbent.markets[1];
        let received_at = Utc.timestamp_opt(Utc::now().timestamp(), 0).single().unwrap();
        let carried_events = carried.outcomes.iter().enumerate().map(|(index, outcome)|
            snapshot_event(&json!({"asset_id":outcome.token_id,
                "market":carried.condition_id,"tick_size":"0.01",
                "timestamp":received_at.timestamp_millis().to_string(),
                "hash":format!("carried-{index}"),
                "bids":[{"price":"0.4","size":"10"}],
                "asks":[{"price":"0.6","size":"10"}]}),
                SnapshotBoundary {market_id:&carried.market_id,
                    condition_id:&carried.condition_id,token_id:&outcome.token_id,
                    recovery_id:"carried-market",cursor:index as u64 + 1,received_at}).unwrap())
            .collect();
        publication.publish(carried_events, None).unwrap();
        let before = publication.reader().market_stream_watermark(&carried.market_id)
            .unwrap().unwrap();
        assert_eq!(before.published_cursor, 1);
        let (_,control)=Api::router(publication.reader(),incumbent.clone(),"old-epoch".into(),
            Limits{snapshot_bytes:65536,frame_bytes:65536,replay_bytes:65536,
                clients:1,send_timeout:Duration::from_secs(1),snapshot_lease_capacity:1,live_backlog_groups:64,live_backlog_age:Duration::from_millis(500)})
            .unwrap();
        let mut unrelated=incumbent.clone();
        unrelated.profile_revision+=1;
        unrelated.markets=vec![
            binding("4944030",'1',"10","20",WindowRole::Current,start),
            binding("4944032",'3',"50","60",WindowRole::Next,start+300),
        ];
        unrelated.profile_id=unrelated.expected_id();
        unrelated.validate_current_at(Utc::now()).unwrap();
        assert!(control.publish(&incumbent.profile_id,incumbent.profile_revision,
            unrelated).is_err(),"prepared identity is not a valid next→current rotation");
        assert_eq!(control.profile().unwrap().profile_id,incumbent.profile_id);
        let mut valid=incumbent.clone();
        valid.profile_revision+=1;
        valid.markets=vec![
            binding("4944031",'2',"30","40",WindowRole::Current,start),
            binding("4944032",'3',"50","60",WindowRole::Next,start+300),
        ];
        valid.profile_id=valid.expected_id();
        let mut expanded=valid.clone();
        expanded.markets.push(binding("4944030",'1',"10","20",
            WindowRole::Retiring,start-300));
        expanded.profile_id=expanded.expected_id();
        expanded.validate_current_at(Utc::now()).unwrap();
        assert!(control.publish(&incumbent.profile_id,incumbent.profile_revision,
            expanded.clone()).is_err(),"ordinary 2→3 cannot skip rotation validation");
        assert_eq!(control.profile().unwrap().profile_id,incumbent.profile_id);
        assert!(control.publish(&incumbent.profile_id,incumbent.profile_revision,
            valid.clone()).is_ok());
        let after = publication.reader().market_stream_watermark(&carried.market_id)
            .unwrap().unwrap();
        assert_eq!(after.stream_epoch, before.stream_epoch,
            "carried next market must retain its own stream generation");
        assert_eq!(after.published_cursor, before.published_cursor,
            "profile CAS cannot reset the carried market cursor");
        assert_eq!(control.profile().unwrap().market_ids_owned(), valid.market_ids_owned());
        let (_,three_window_control)=Api::router(publication.reader(),expanded.clone(),
            "three-window-epoch".into(),Limits{snapshot_bytes:65536,frame_bytes:65536,
                replay_bytes:65536,clients:1,send_timeout:Duration::from_secs(1),
                snapshot_lease_capacity:1,live_backlog_groups:64,live_backlog_age:Duration::from_millis(500)}).unwrap();
        let mut contracted=valid;
        contracted.profile_revision=expanded.profile_revision+1;
        contracted.profile_id=contracted.expected_id();
        assert!(three_window_control.publish(&expanded.profile_id,
            expanded.profile_revision,contracted).is_err(),
            "ordinary 3→2 cannot skip rotation validation");
        assert_eq!(three_window_control.profile().unwrap().profile_id,expanded.profile_id);
        publication.finish().await.unwrap();
    }
    #[tokio::test]
    async fn ten_thousand_two_window_profile_cas_rotations_keep_one_active_generation() {
        let condition=format!("0x{}","1".repeat(64));
        let seed=json!({"latest_cursor":0,"catalog_revision":"a".repeat(64),
            "gaps":[],"books":[],"markets":[market("m0",&condition,"0-up","0-down"),
                market("m1",&condition,"1-up","1-down")]});
        let publication=Publication::start(0,Some(seed),[
            ("0-up".into(),"m0".into()),("0-down".into(),"m0".into()),
            ("1-up".into(),"m1".into()),("1-down".into(),"m1".into()),
        ].into(),8,131072,65536,|_|Ok(())).unwrap();
        let base=1_800_000_000_i64.div_euclid(300)*300;
        let market_at=|number: usize,role: WindowRole,start: i64| {
            let at=|seconds| Utc.timestamp_opt(seconds,0).single().unwrap()
                .to_rfc3339_opts(SecondsFormat::Secs,true);
            MarketBinding {role,window_id:format!("{}/{}",at(start),at(start+300)),
                start_at:at(start),end_at:at(start+300),market_id:format!("m{number}"),
                condition_id:condition.clone(),outcomes:[
                    OutcomeBinding{outcome:"Up".into(),token_id:format!("{number}-up")},
                    OutcomeBinding{outcome:"Down".into(),token_id:format!("{number}-down")}],
                rule_sha256:"c".repeat(64)}
        };
        let mut incumbent=profile();
        incumbent.markets=vec![market_at(0,WindowRole::Current,base-300),
            market_at(1,WindowRole::Next,base)];
        incumbent.profile_id=incumbent.expected_id();
        let (_,control)=Api::router(publication.reader(),incumbent.clone(),"initial".into(),
            Limits{snapshot_bytes:65536,frame_bytes:65536,replay_bytes:65536,
                clients:1,send_timeout:Duration::from_secs(1),snapshot_lease_capacity:1,live_backlog_groups:64,live_backlog_age:Duration::from_millis(500)})
            .unwrap();
        for index in 0..10_000_usize {
            let now=Utc.timestamp_opt(base+index as i64*300+1,0).single().unwrap();
            let mut candidate=incumbent.clone();
            candidate.profile_revision+=1;
            candidate.markets=vec![
                market_at(index+1,WindowRole::Current,base+index as i64*300),
                market_at(index+2,WindowRole::Next,base+(index as i64+1)*300),
            ];
            candidate.profile_id=candidate.expected_id();
            if index==5_000 {
                let mut unrelated=candidate.clone();
                unrelated.markets[0]=market_at(index,WindowRole::Current,
                    base+index as i64*300);
                unrelated.profile_id=unrelated.expected_id();
                assert!(control.commit_profile_at(&incumbent.profile_id,
                    incumbent.profile_revision,unrelated,true,now).is_err());
                assert_eq!(control.profile().unwrap().profile_id,incumbent.profile_id);
            }
            let receipt=control.commit_profile_at(&incumbent.profile_id,
                incumbent.profile_revision,candidate.clone(),true,now).unwrap();
            assert_eq!(receipt["profile_revision"],candidate.profile_revision);
            assert_eq!(control.profile().unwrap().market_ids_owned().len(),2);
            incumbent=candidate;
        }
        assert_eq!(control.profile().unwrap().profile_revision,10_001);
        assert_eq!(control.profile().unwrap().market_ids_owned(),
            BTreeSet::from(["m10000".into(),"m10001".into()]));
        publication.finish().await.unwrap();
    }
    #[test]
    fn profile_rejects_swapped_outcomes_and_more_than_five_minutes() {
        let mut p = profile();
        p.markets[0].outcomes.swap(0, 1);
        p.profile_id = p.expected_id();
        assert!(p.validate().is_err());
        let mut p = profile();
        p.markets[0].end_at = "2026-09-26T05:36:00Z".into();
        p.profile_id = p.expected_id();
        assert!(p.validate().is_err());
    }
    #[test]
    fn current_window_is_rechecked_at_the_mutation_clock() {
        let p = profile();
        let inside = DateTime::parse_from_rfc3339("2026-09-26T05:34:59.999Z")
            .unwrap()
            .with_timezone(&Utc);
        let boundary = DateTime::parse_from_rfc3339("2026-09-26T05:35:00Z")
            .unwrap()
            .with_timezone(&Utc);
        p.validate_current_at(inside).unwrap();
        assert!(p.validate_current_at(boundary).is_err());
    }
    #[test]
    fn steady_state_rotation_reuses_next_and_never_invents_a_fourth_overlap() {
        let incumbent = two_market_reducer_profile();
        let old_current = incumbent.window(WindowRole::Current).unwrap().clone();
        let old_next = incumbent.window(WindowRole::Next).unwrap().clone();
        let mut candidate = incumbent.clone();
        candidate.profile_revision += 1;
        let mut current = old_next.clone();
        current.role = WindowRole::Current;
        let mut retiring = old_current.clone();
        retiring.role = WindowRole::Retiring;
        let next = MarketBinding {
            role: WindowRole::Next,
            window_id: "2026-09-26T05:40:00Z/2026-09-26T05:45:00Z".into(),
            start_at: "2026-09-26T05:40:00Z".into(),
            end_at: "2026-09-26T05:45:00Z".into(),
            market_id: "4944032".into(),
            condition_id: format!("0x{}", "4".repeat(64)),
            outcomes: [
                OutcomeBinding {
                    outcome: "Up".into(),
                    token_id: "401".into(),
                },
                OutcomeBinding {
                    outcome: "Down".into(),
                    token_id: "402".into(),
                },
            ],
            rule_sha256: canonical_hash(&json!({"fixture":"next-rule"})),
        };
        candidate.markets = vec![current, next, retiring];
        candidate.profile_id = candidate.expected_id();
        assert!(candidate.validate_rotation_from(&incumbent).is_err(),
            "ordinary 2→3 cannot bypass the equal-shape rotation contract");
        candidate.markets.pop();
        candidate.profile_id = candidate.expected_id();
        candidate.validate_rotation_from(&incumbent).unwrap();

        let mut unrelated = candidate;
        unrelated.markets[0].market_id = "unrelated".into();
        unrelated.profile_id = unrelated.expected_id();
        assert!(unrelated.validate_rotation_from(&incumbent).is_err());
    }
    #[test]
    fn autonomous_two_window_rotation_drops_expired_current_before_new_next() {
        let incumbent = two_market_reducer_profile();
        incumbent.validate().unwrap();
        let old_next = incumbent.window(WindowRole::Next).unwrap().clone();
        let mut current = old_next;
        current.role = WindowRole::Current;
        let next = MarketBinding {
            role: WindowRole::Next,
            window_id: "2026-09-26T05:40:00Z/2026-09-26T05:45:00Z".into(),
            start_at: "2026-09-26T05:40:00Z".into(),
            end_at: "2026-09-26T05:45:00Z".into(),
            market_id: "4944032".into(),
            condition_id: format!("0x{}", "4".repeat(64)),
            outcomes: [
                OutcomeBinding {
                    outcome: "Up".into(),
                    token_id: "401".into(),
                },
                OutcomeBinding {
                    outcome: "Down".into(),
                    token_id: "402".into(),
                },
            ],
            rule_sha256: canonical_hash(&json!({"fixture":"next-rule"})),
        };
        let mut candidate = incumbent.clone();
        candidate.profile_revision += 1;
        candidate.markets = vec![current, next];
        candidate.profile_id = candidate.expected_id();
        candidate.validate_rotation_from(&incumbent).unwrap();
        assert_eq!(
            incumbent
                .market_ids()
                .difference(&candidate.market_ids())
                .count(),
            1
        );
        assert_eq!(
            candidate
                .market_ids()
                .difference(&incumbent.market_ids())
                .count(),
            1
        );
    }
    #[tokio::test]
    async fn management_503_uses_versioned_error_fixture() {
        let response = failure(StatusCode::SERVICE_UNAVAILABLE,
            "btc_5m_management_unavailable", true);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = axum::body::to_bytes(response.into_body(), 65536).await.unwrap();
        let actual: Value = serde_json::from_slice(&bytes).unwrap();
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-management-health-v1-503.json"
        )).unwrap();
        assert_eq!(actual, fixture);
    }

    #[tokio::test]
    async fn market_replay_route_reads_writer_committed_market_only() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("indexes")).unwrap();
        std::fs::File::create(root.path().join("events.jsonl")).unwrap();
        let db = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        db.execute_batch("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE gaps(resolved INTEGER NOT NULL);
            CREATE TABLE books(token_id TEXT PRIMARY KEY,market_id TEXT,cursor INTEGER,payload_json BLOB,payload_sha256 TEXT);
            CREATE TABLE book_confirmations(token_id TEXT PRIMARY KEY);
            CREATE TABLE event_offsets(cursor INTEGER PRIMARY KEY,byte_offset INTEGER,byte_length INTEGER,event_id TEXT,market_id TEXT,token_id TEXT,line_sha256 TEXT);").unwrap();
        for (key, value) in [
            ("schema_version", "marketcow.polymarket.state-index.v1"),
            ("latest_cursor", "0"), ("event_log_size", "0"),
            ("unresolved_gap_count", "0"), ("catalog_revision", "catalog-test"),
            ("active_recovery_id", "recovery-test"),
        ] {
            db.execute("INSERT INTO metadata VALUES (?,?)", [key, value]).unwrap();
        }
        let profile = profile();
        let binding = profile.markets[0].clone();
        for outcome in &binding.outcomes {
            let model = book(&outcome.token_id, &binding.condition_id, "2026-09-26T05:30:01Z");
            db.execute("INSERT INTO books VALUES (?,?,0,?,?)",
                params![outcome.token_id, binding.market_id,
                    serde_json::to_vec(&model).unwrap(), canonical_hash(&model)]).unwrap();
        }
        drop(db);
        let writer = PreparedSourceWriter::open(root.path(), 65536).unwrap();
        let mut writer = writer;
        let tokens = binding.outcomes.iter().map(|outcome|
            (outcome.token_id.clone(), binding.market_id.clone())).collect();
        let seed = json!({"latest_cursor":0,"books":[],"markets":[market(
            &binding.market_id, &binding.condition_id,
            &binding.outcomes[0].token_id, &binding.outcomes[1].token_id)],"gaps":[],
            "schema_version":"marketcow.polymarket.live-stream.v1","type":"state"});
        let mut publication = Publication::start(0, Some(seed), tokens, 8, 131072, 65536,
            move |batches| writer.append_validated_group(
                &batches.iter().map(|batch| &batch.validated).collect::<Vec<_>>()))
            .unwrap();
        let received_at = Utc.with_ymd_and_hms(2026, 9, 26, 5, 30, 1).unwrap();
        let events = binding.outcomes.iter().enumerate().map(|(index, outcome)|
            snapshot_event(&json!({"asset_id":outcome.token_id,"market":binding.condition_id,
                "tick_size":"0.01","timestamp":"1790399999000","hash":"source",
                "bids":[{"price":"0.4","size":"10"}],
                "asks":[{"price":"0.6","size":"10"}]}),
                SnapshotBoundary {market_id:&binding.market_id,
                    condition_id:&binding.condition_id,token_id:&outcome.token_id,
                    recovery_id:"test",cursor:index as u64 + 1,received_at}).unwrap())
            .collect::<Vec<_>>();
        publication.publish_native_events(events, None).unwrap();
        let reader = publication.reader();
        publication.finish().await.unwrap();
        let watermark = reader.market_stream_watermark(&binding.market_id).unwrap().unwrap();
        assert_eq!((watermark.published_cursor, watermark.durable_cursor), (1, 1));
        let (router, control) = Api::router_with_archive(reader.clone(), profile.clone(),
            "profile-epoch".into(), Limits {
                snapshot_bytes:65536,frame_bytes:65536,replay_bytes:65536,
                clients:1,send_timeout:Duration::from_secs(1),snapshot_lease_capacity:1, live_backlog_groups:64, live_backlog_age:Duration::from_millis(500),
            }, root.path().to_path_buf()).unwrap();
        let status = router.clone().oneshot(axum::http::Request::builder()
            .uri("/v1/prediction-markets/polymarket/research/btc-5m/status")
            .body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let status: Value = serde_json::from_slice(&axum::body::to_bytes(
            status.into_body(), 65536).await.unwrap()).unwrap();
        assert_eq!(status["schema_version"], STATUS_SCHEMA);
        assert!(status["observed_at"].as_str().is_some());
        assert_eq!(status["windows"][0]["token_quotes"].as_array().unwrap().len(), 2);
        assert!(status["windows"][0]["readiness_reasons"].as_array().is_some());
        let streams = router.clone().oneshot(axum::http::Request::builder()
            .uri("/v1/prediction-markets/polymarket/research/btc-5m/market-streams")
            .body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(streams.status(), StatusCode::NOT_FOUND);
        let snapshot_path = format!(
            "/v1/prediction-markets/polymarket/research/btc-5m/market-full-sync?market_id={}",
            binding.market_id);
        let snapshot_response = router.clone().oneshot(axum::http::Request::builder()
            .uri(snapshot_path).body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(snapshot_response.status(), StatusCode::NOT_FOUND);
        use marketcow_contracts::btc5m_market_local_v6::{self as live,wire};
        let official_streams=tokio::time::timeout(Duration::from_secs(2),async {
            loop {
                let response=router.clone().oneshot(axum::http::Request::builder()
                    .uri("/v1/prediction-markets/polymarket/research/btc-5m/official-ws/market-streams")
                    .body(axum::body::Body::empty()).unwrap()).await.unwrap();
                if response.status()==StatusCode::OK {
                    break wire::MarketStreams::decode(axum::body::to_bytes(
                        response.into_body(),65536).await.unwrap()).unwrap();
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.unwrap();
        assert_eq!(official_streams.schema_version,live::SCHEMA_VERSION);
        let ordinary_binding=official_streams.markets.iter()
            .find(|m|m.key.as_ref().unwrap().market_id==binding.market_id).unwrap();
        let official_snapshot_path=format!(
            "/v1/prediction-markets/polymarket/research/btc-5m/official-ws/market-full-sync?market_id={}",
            binding.market_id);
        let official_snapshot=router.clone().oneshot(axum::http::Request::builder()
            .uri(official_snapshot_path).body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(official_snapshot.status(),StatusCode::OK);
        let official_snapshot=wire::MarketFrame::decode(axum::body::to_bytes(
            official_snapshot.into_body(),65536).await.unwrap()).unwrap();
        let Some(wire::market_frame::Body::Snapshot(books))=official_snapshot.body else {panic!("ordinary snapshot");};
        assert_eq!(books.books.len(),2);
        let relay=crate::source_official_ws_relay::router(control,reader.clone()).unwrap();
        let wrong_rule=format!(
            "/v1/prediction-markets/polymarket/research/btc-5m/official-ws/official-book?market_id={}&condition_id={}&token_id={}&rule_sha256={}&window_id={}&start_at={}&end_at={}&up_token_id={}&down_token_id={}&stream_id={}",
            binding.market_id,binding.condition_id,binding.outcomes[0].token_id,"f".repeat(64),
            binding.window_id,binding.start_at,binding.end_at,binding.outcomes[0].token_id,
            binding.outcomes[1].token_id,ordinary_binding.stream_id);
        let rejected=relay.oneshot(axum::http::Request::builder().uri(wrong_rule)
            .body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(rejected.status(),StatusCode::CONFLICT);
        let path = format!("/v1/prediction-markets/polymarket/research/btc-5m/market-replay?market_id={}&stream_epoch={}&after_market_cursor=0&maximum_groups=1",
            binding.market_id, watermark.stream_epoch);
        let response = router.clone().oneshot(axum::http::Request::builder().uri(path)
            .body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 65536).await.unwrap();
        let value = proto::MarketReplayPage::decode(body).unwrap();
        assert_eq!(value.next_market_cursor, Some(1));
        assert_eq!(value.groups[0].events.len(), 2);
        assert_eq!(value.groups[0].group_sha256.len(), 32);
        assert!(!value.archive_only);
        // A cold epoch must remain selectively readable even when the hot
        // profile has moved to another market. It does not expose readiness.
        let mut next_profile = profile;
        next_profile.markets[0].market_id = "other-hot-market".into();
        next_profile.profile_id = next_profile.expected_id();
        let (cold_router, _) = Api::router_with_archive(reader, next_profile,
            "next-profile-epoch".into(), Limits {
                snapshot_bytes:65536,frame_bytes:65536,replay_bytes:65536,
                clients:1,send_timeout:Duration::from_secs(1),snapshot_lease_capacity:1, live_backlog_groups:64, live_backlog_age:Duration::from_millis(500),
            }, root.path().to_path_buf()).unwrap();
        let cold_path = format!("/v1/prediction-markets/polymarket/research/market-archive-replay?market_id={}&stream_epoch={}&after_market_cursor=0&maximum_groups=1",
            binding.market_id, watermark.stream_epoch);
        let cold_response = cold_router.clone().oneshot(axum::http::Request::builder()
            .uri(cold_path).body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(cold_response.status(), StatusCode::OK);
        let cold = proto::MarketReplayPage::decode(axum::body::to_bytes(
            cold_response.into_body(), 65536).await.unwrap()).unwrap();
        assert!(cold.archive_only);
        assert!(cold.active_identity.is_none());
        assert_eq!(cold.groups, value.groups);
        let missing = cold_router.oneshot(axum::http::Request::builder()
            .uri("/v1/prediction-markets/polymarket/research/market-archive-replay?market_id=missing&stream_epoch=missing&after_market_cursor=0&maximum_groups=1")
            .body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        for (bad, expected) in [
            ("market_id=absent&stream_epoch=epoch&after_market_cursor=0&maximum_groups=1".to_owned(),
                StatusCode::NOT_FOUND),
            (format!("market_id={}&stream_epoch=wrong&after_market_cursor=0&maximum_groups=1", binding.market_id),
                StatusCode::CONFLICT),
        ] {
            let response = router.clone().oneshot(axum::http::Request::builder()
                .uri(format!("/v1/prediction-markets/polymarket/research/btc-5m/market-replay?{bad}"))
                .body(axum::body::Body::empty()).unwrap()).await.unwrap();
            assert_eq!(response.status(), expected);
        }
        let db = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        db.execute("UPDATE market_streams SET replay_floor=1 WHERE market_id=? AND stream_epoch=?",
            params![binding.market_id, watermark.stream_epoch]).unwrap();
        let expired_path = format!("/v1/prediction-markets/polymarket/research/btc-5m/market-replay?market_id={}&stream_epoch={}&after_market_cursor=0&maximum_groups=1",
            binding.market_id, watermark.stream_epoch);
        let expired = router.oneshot(axum::http::Request::builder().uri(expired_path)
            .body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(expired.status(), StatusCode::GONE);
    }
}
