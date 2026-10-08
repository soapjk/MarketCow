//! Memory-first publication. The disk worker never owns the publication lock during I/O.
use anyhow::{Context, Result, ensure};
use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    routing::get,
};
use chrono::{DateTime, SecondsFormat, Utc};
use marketcow_polymarket::discovery_source::{SnapshotBoundary, canonical_hash, prepare_snapshot};
#[cfg(test)]
use marketcow_runtime::discovery_source::validate_source_events;
use marketcow_runtime::discovery_source::{
    ValidatedSourceBatch, apply_token_recovery, is_token_recovery,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex, mpsc},
    thread,
};
use tokio::sync::{Semaphore, watch};
use crate::source_market_replay::{MarketPage, MarketReplay};
use crate::source_market_persistence::{
    Limits as MarketPersistenceLimits, MarketDrainTimeout, MarketPersistence,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const WS_STATE_CONFIRMATION_MIN_INTERVAL_MS: i64 = 1_000;
pub(crate) const BTC5M_TRACE_SAMPLE_MODULUS: u64 = 4096;
const MAX_NON_QUEUE_STATE_BYTES: usize = 512 * 1024 * 1024;

fn market_persistence_lane_limits(
    aggregate_batches: usize,
    aggregate_bytes: usize,
    batch_cap: usize,
) -> MarketPersistenceLimits {
    // The production 4 GiB / 16384-batch aggregate holds four full
    // 1 GiB / 4096-batch lanes. Smaller explicit test configurations retain
    // their historical half-aggregate limit, never exceeding the aggregate.
    MarketPersistenceLimits {
        batches: (aggregate_batches / 2).max(1).min(4096),
        bytes: (aggregate_bytes / 2).min(1024 * 1024 * 1024)
            .max(batch_cap).min(aggregate_bytes),
    }
}

pub(crate) fn btc5m_trace_sample(cursor: u64) -> bool {
    cursor != 0 && cursor % BTC5M_TRACE_SAMPLE_MODULUS == 0
}

fn advances_book_confirmation_fence(event_type: Option<&str>) -> bool {
    matches!(
        event_type,
        Some("book" | "price_change" | "tick_size_change")
    )
}

pub(super) fn validate_materialized_books(
    events: &[Value],
    materialized_books: &BTreeMap<u64, Value>,
) -> Result<()> {
    let expected: std::collections::BTreeSet<u64> = events
        .iter()
        .filter(|event| event["canonical_payload"]["payload_semantics"].is_string())
        .map(|event| event["cursor"].as_u64().context("compact event cursor"))
        .collect::<Result<_>>()?;
    ensure!(
        materialized_books
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            == expected,
        "materialized compact-book cursor set differs"
    );
    for event in events {
        let payload = &event["canonical_payload"];
        let Some(semantics) = payload["payload_semantics"].as_str() else {
            continue;
        };
        ensure!(
            matches!(
                semantics,
                "ordered_depth_delta_with_complete_post_state_commitment"
                    | "last_trade_with_unchanged_depth_commitment"
            ),
            "unknown compact payload semantics"
        );
        let cursor = event["cursor"].as_u64().context("compact event cursor")?;
        let book = materialized_books
            .get(&cursor)
            .context("compact event missing materialized book")?;
        ensure!(
            payload["canonical_book_sha256"]
                .as_str()
                .is_some_and(|value| value.len() == 64
                    && value
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())),
            "compact materialized book commitment invalid"
        );
        ensure!(
            payload["materialized_book_bytes"]
                .as_u64()
                .is_some_and(|n| n > 0),
            "compact materialized book size invalid"
        );
        for field in [
            "token_id",
            "condition_id",
            "book_epoch",
            "sequence",
            "sequence_semantics",
            "exchange_at",
            "received_at",
            "tick_version",
            "tick_size",
            "last_trade_price",
            "state_checksum",
            "source_hash",
        ] {
            ensure!(
                payload[field] == book[field],
                "compact post-state field differs: {field}"
            );
        }
        ensure!(
            book["bids"].is_array() && book["asks"].is_array(),
            "materialized depth missing"
        );
    }
    Ok(())
}

fn materialized_declared_bytes(events: &[Value]) -> Result<usize> {
    events.iter().try_fold(0usize, |total, event| {
        let bytes = event["canonical_payload"]["materialized_book_bytes"]
            .as_u64()
            .unwrap_or(0);
        total
            .checked_add(usize::try_from(bytes)?)
            .context("materialized byte overflow")
    })
}

enum ProjectionAction {
    Terminal { market_id: String, model: Arc<Value>, state_bytes: usize },
    Book {
        token_id: String,
        physical_cursor: u64,
        exchange_at: Option<DateTime<Utc>>,
        clear_confirmation: bool,
        model: Arc<Value>,
        state_bytes: usize,
    },
}

/// Immutable, market-local projection work prepared by the market actor.
/// Only the publisher's physical cursor is assigned later by the owner.
pub(super) struct PreparedProjection {
    actions: BTreeMap<u64, ProjectionAction>,
}

impl PreparedProjection {
    pub(super) fn retained_bytes(&self) -> usize {
        self.actions.values().map(|action| match action {
            ProjectionAction::Book { model, token_id, .. } =>
                marketcow_runtime::discovery_source::json_retained_bytes(model) + token_id.capacity() + 256,
            ProjectionAction::Terminal { model, market_id, .. } =>
                marketcow_runtime::discovery_source::json_retained_bytes(model) + market_id.capacity() + 256,
        }).sum()
    }
    pub(super) fn rebase(&mut self, after: u64) -> Result<()> {
        self.rebase_from(0, after)
    }

    pub(super) fn rebase_from(&mut self, old_after: u64, new_after: u64) -> Result<()> {
        self.actions = std::mem::take(&mut self.actions).into_iter().map(|(cursor, mut action)| {
            let cursor = new_after.checked_add(cursor.checked_sub(old_after)
                .context("projection cursor before actor boundary")?)
                .context("projection cursor overflow")?;
            if let ProjectionAction::Book { physical_cursor, .. } = &mut action {
                *physical_cursor = cursor;
            }
            Ok((cursor, action))
        }).collect::<Result<_>>()?;
        Ok(())
    }

    pub(super) fn take_for(&mut self, events: &[Value]) -> Result<Self> {
        let mut actions = BTreeMap::new();
        for event in events {
            let cursor = event["cursor"].as_u64().context("projection event cursor")?;
            if is_token_recovery(event) {
                ensure!(!self.actions.contains_key(&cursor), "recovery has projection action");
            } else {
                let action = self.actions.remove(&cursor)
                    .context("market projection action missing")?;
                ensure!(actions.insert(cursor, action).is_none(), "duplicate projection cursor");
            }
        }
        Ok(Self { actions })
    }

    pub(super) fn is_empty(&self) -> bool { self.actions.is_empty() }

    fn into_actions(self) -> Vec<ProjectionAction> {
        self.actions.into_values().collect()
    }
}

pub(super) fn prepare_market_projection(
    events: &[Value], materialized_books: &BTreeMap<u64, Value>,
) -> Result<PreparedProjection> {
    validate_materialized_books(events, materialized_books)?;
    let actions = prepare_projection_actions(events, materialized_books)?;
    let cursors = events.iter().filter(|event| !is_token_recovery(event))
        .map(|event| event["cursor"].as_u64().context("projection cursor"))
        .collect::<Result<Vec<_>>>()?;
    ensure!(cursors.len() == actions.len(), "projection action count differs");
    let actions: BTreeMap<_, _> = cursors.into_iter().zip(actions).collect();
    ensure!(actions.len() == events.iter().filter(|event| !is_token_recovery(event)).count(),
        "duplicate prepared projection cursor");
    Ok(PreparedProjection { actions })
}

/// Parse and clone immutable post-states before taking the shared publication
/// lock. The accepted-batch commit below is then infallible and cheap enough
/// to be one market-local visibility boundary.
fn prepare_projection_actions(
    events: &[Value], materialized_books: &BTreeMap<u64, Value>,
) -> Result<Vec<ProjectionAction>> {
    let mut actions = Vec::with_capacity(events.len());
    for event in events {
        if is_token_recovery(event) { continue; }
        if event["event_type"] == "market_terminal" {
            let model = &event["canonical_payload"]["market"];
            let state_bytes = match event["canonical_payload"]["materialized_book_bytes"].as_u64() {
                Some(declared) => usize::try_from(declared)?,
                None => serde_json::to_vec(model)?.len(),
            };
            actions.push(ProjectionAction::Terminal {
                market_id: event["market_id"].as_str().context("terminal market identity")?.to_owned(),
                model: Arc::new(model.clone()), state_bytes,
            });
            continue;
        }
        let token_id = event["token_id"].as_str().context("book token identity")?.to_owned();
        let physical_cursor = event["cursor"].as_u64().context("book event cursor")?;
        let clear_confirmation = advances_book_confirmation_fence(event["event_type"].as_str());
        let exchange_at = if clear_confirmation {
            Some(DateTime::parse_from_rfc3339(
                event["canonical_payload"]["exchange_at"].as_str()
                    .context("book event exchange_at")?
            )?.with_timezone(&Utc))
        } else { None };
        let model = materialized_books.get(&physical_cursor)
            .unwrap_or(&event["canonical_payload"]);
        let state_bytes = match event["canonical_payload"]["materialized_book_bytes"].as_u64() {
            Some(declared) => usize::try_from(declared)?,
            None => serde_json::to_vec(model)?.len(),
        };
        actions.push(ProjectionAction::Book {
            token_id, physical_cursor, exchange_at, clear_confirmation,
            model: Arc::new(model.clone()), state_bytes,
        });
    }
    Ok(actions)
}

pub struct Batch {
    pub validated: ValidatedSourceBatch,
    /// Complete post-state books keyed by publisher cursor for compact
    /// incremental facts. These immutable values update the memory projection
    /// and durable latest-state index, but are deliberately not copied into
    /// replay/history for every depth mutation.
    /// The disk worker takes this Arc exactly once. Replay retains only the
    /// compact validated facts after durability, while the publisher can use
    /// its own Arc clone concurrently without copying full depth.
    pub materialized_books: Mutex<Option<Arc<BTreeMap<u64, Value>>>>,
    pub evidence: Option<Value>,
    /// Explicit owner-admitted identities, ordered before these exact events
    /// on the existing disk worker. Never inferred from an untrusted event.
    pub catalog_bindings: BTreeMap<String, String>,
    // Diagnostic-only metadata. It is retained by the same bounded replay
    // batch and never changes event bytes, canonical hashes or cursor order.
    timing: Mutex<Option<SourceBatchTiming>>,
    bytes: usize,
    replay_retained_bytes: usize,
}

#[derive(Clone)]
pub struct SourceBatchTiming {
    pub raw_batch_first_received_at: DateTime<Utc>,
    pub raw_batch_last_received_at: DateTime<Utc>,
    pub actor_admitted_at: DateTime<Utc>,
    pub actor_started_at: DateTime<Utc>,
    pub actor_completed_at: DateTime<Utc>,
    pub actor_admitted_mono: std::time::Instant,
    pub actor_started_mono: std::time::Instant,
    pub actor_completed_mono: std::time::Instant,
    pub publish_call_started_at: DateTime<Utc>,
    pub publish_call_started_mono: std::time::Instant,
    pub shared_replay_visible_at: Option<DateTime<Utc>>,
    pub shared_replay_visible_mono: Option<std::time::Instant>,
    pub durable_at: Option<DateTime<Utc>>,
}

impl Batch {
    pub(super) fn retained_bytes(&self) -> usize { self.replay_retained_bytes }
    pub fn timing_snapshot(&self) -> Option<SourceBatchTiming> {
        self.timing.lock().ok().and_then(|timing| timing.clone())
    }
}
pub struct Retirement {
    pub ticket: u64,
    pub after: u64,
    pub markets: std::collections::BTreeSet<String>,
    market_epochs: BTreeMap<String, String>,
    live_only_markets: std::collections::BTreeSet<String>,
    /// Immutable catalog identity is committed to the cold index before hot
    /// bindings are removed. It is never reconstructed from a later book.
    pub cold_records: BTreeMap<String, Value>,
    pub catalog_bindings: BTreeMap<String, String>,
    bytes: usize,
}
enum QueuedWork {
    Events(Arc<Batch>),
    Retire(Retirement),
}
pub enum PersistenceWork<'a> {
    Events(&'a [Arc<Batch>]),
    Retire(&'a Retirement),
}
struct Memory {
    // A carried next market keeps its generation across profile rotation;
    // retiring and later re-admitting the same market creates a new epoch.
    market_stream_epochs: BTreeMap<String, String>,
    market_published_cursors: BTreeMap<String, u64>,
    market_durable_cursors: BTreeMap<String, u64>,
    market_confirmation_sequences: BTreeMap<String, u64>,
    market_confirmation_versions: BTreeMap<String, u64>,
    retiring_markets: BTreeSet<String>,
    retirement_submitted: u64,
    retirement_persisted: u64,
    acquisition_statistics: Option<(usize, usize, usize)>,
    acquisition_detail: Option<Value>,
    // Publication identity bindings are not the same thing as active wire
    // subscriptions. Keep both counts visible during hot/cold reconciliation.
    binding_token_ids: BTreeSet<String>,
    token_market_ids: BTreeMap<String, String>,
    wire_token_ids: Option<BTreeSet<String>>,
    draining_token_ids: Option<BTreeSet<String>>,
    recoveries: BTreeMap<String, Value>,
    source_ready: bool,
    resume_floor: u64,
    cursor: u64,
    persisted: u64,
    queued: usize,
    queued_bytes: usize,
    error: Option<String>,
    base: Option<Value>,
    books: BTreeMap<String, Arc<Value>>,
    book_cursors: BTreeMap<String, u64>,
    // A book can have a physical archive cursor without having crossed this
    // process incarnation's market-local publication boundary. Never seed a
    // market stream from such a pre-restart/pre-acquisition book.
    market_book_cursors: BTreeMap<String, u64>,
    // Venue time of the latest order-book mutation. `LiveBook.exchange_at`
    // also advances on last_trade_price, so it cannot fence REST book
    // confirmations without falsely treating an independent trade as a newer
    // book.
    book_exchanges: BTreeMap<String, chrono::DateTime<chrono::Utc>>,
    markets: BTreeMap<String, Arc<Value>>,
    // Authoritative lifecycle tombstones. This bounded set prevents any late
    // transport frame from resurrecting recovery/book state after terminal
    // publication, even if an unsubscribe receipt is still in flight.
    terminal_markets: std::collections::BTreeSet<String>,
    replay: VecDeque<Arc<Batch>>,
    replay_bytes: usize,
    snapshot_leases: BTreeMap<u64, std::time::Instant>,
    state_bytes: usize,
    state_sizes: BTreeMap<(bool, String), usize>,
    confirmation_cursor: u64,
    confirmations: BTreeMap<String, u64>,
    confirmation_facts: BTreeMap<String, Arc<Value>>,
}
pub struct Publication {
    memory: Arc<Mutex<Memory>>,
    market_health: Arc<MarketHealth>,
    changed: watch::Sender<u64>,
    market_replay: Arc<MarketReplay<Batch, Option<MarketReadState>>>,
    market_persistence: Arc<MarketPersistence<Batch>>,
    market_worker: Option<thread::JoinHandle<()>>,
    market_publishers: std::sync::RwLock<BTreeMap<String, Arc<MarketPublisher>>>,
    sender: Option<mpsc::Sender<QueuedWork>>,
    worker: Option<thread::JoinHandle<()>>,
    tokens: BTreeMap<String, String>,
    pending_bindings: BTreeMap<String, String>,
    retirement_enabled: bool,
    max_batches: usize,
    max_bytes: usize,
    replay_max_batches: usize,
    replay_max_bytes: usize,
    batch_cap: usize,
    hot_timing: PublicationTiming,
}

/// Cloneable selected-market hot path. Catalog admission creates the handle;
/// no book mutation takes a process-wide publication or metadata mutex.
pub(super) struct MarketPublisher {
    market_id: String,
    gate: Mutex<()>,
    replay: Arc<MarketReplay<Batch, Option<MarketReadState>>>,
    persistence: Arc<MarketPersistence<Batch>>,
    health: Arc<MarketHealth>,
    catalog_bindings: BTreeMap<String, String>,
    batch_cap: usize,
    confirmation_byte_limit: usize,
    // One-way for a stream epoch: after a disk admission gap, later groups
    // remain live-only. They cannot be appended behind a missing durable group.
    volatile: AtomicBool,
    volatile_groups: AtomicU64,
}

#[derive(Default)]
struct StageTiming {
    count: u64,
    total_us: u64,
    max_us: u64,
}

impl StageTiming {
    fn record(&mut self, elapsed: std::time::Duration) {
        let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.count = self.count.saturating_add(1);
        self.total_us = self.total_us.saturating_add(us);
        self.max_us = self.max_us.max(us);
    }

    fn as_json(&self) -> Value {
        json!({"count":self.count,"total_us":self.total_us,"max_us":self.max_us})
    }
}

#[derive(Default)]
struct PublicationTiming {
    validation: StageTiming,
    validated_events: u64,
    validated_bytes: u64,
    capacity_wait: StageTiming,
    memory_lock_wait: StageTiming,
    memory_lock_held: StageTiming,
}

/// One atomic memory boundary. Books/markets are immutable versions: holding a
/// view cannot block ingestion and later confirmations cannot mutate this view.
/// Callers must bound concurrent views and their lifetime independently.
#[derive(Clone)]
pub struct MemoryView {
    pub cursor: u64,
    pub persisted_cursor: u64,
    pub confirmation_sequence: u64,
    pub books: BTreeMap<String, Arc<Value>>,
    pub markets: BTreeMap<String, Arc<Value>>,
    pub recoveries: BTreeMap<String, Value>,
    pub book_cursors: BTreeMap<String, u64>,
    pub confirmation_versions: BTreeMap<String, u64>,
    pub confirmation_facts: BTreeMap<String, Arc<Value>>,
    pub base: Value,
    pub queued: usize,
}

/// Read-only shared handle. It cannot publish events or perform persistence.
#[derive(Clone)]
pub struct MemoryReader {
    memory: Arc<Mutex<Memory>>,
    market_health: Arc<MarketHealth>,
    changed: watch::Sender<u64>,
    market_replay: Arc<MarketReplay<Batch, Option<MarketReadState>>>,
    market_persistence: Arc<MarketPersistence<Batch>>,
}
struct MarketHealth {
    source_ready: AtomicBool,
    accepting_events: AtomicBool,
    persistence_failed: AtomicBool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketStreamWatermark {
    pub stream_epoch: String,
    pub market_id: String,
    pub published_cursor: u64,
    pub durable_cursor: u64,
    pub replay_floor: u64,
}
/// One selected market's books and replay boundary captured while holding only
/// its market lane. It does not clone unrelated markets' depth or claim that
/// the latest published books are already durable.
pub struct MarketMemorySnapshot {
    pub watermark: MarketStreamWatermark,
    /// Last event in the selected market's published stream, including
    /// recovery/terminal facts that do not replace either token's book.
    /// This is not the market-group resume cursor or a disk archive offset.
    pub last_market_event_cursor: u64,
    pub market: Option<Arc<Value>>,
    pub books: BTreeMap<String, Arc<Value>>,
    pub physical_book_cursors: BTreeMap<String, u64>,
    /// Cursor of the source event that installed this book. This is the
    /// recovery snapshot identity, not a physical archive offset and not the
    /// market group's resume cursor.
    pub event_book_cursors: BTreeMap<String, u64>,
    pub market_book_cursors: BTreeMap<String, u64>,
    pub recoveries: BTreeMap<String, Value>,
    pub unresolved_seed_gaps: BTreeSet<String>,
    pub capacity_fenced: bool,
    pub confirmation_sequence: u64,
    pub confirmation_facts: BTreeMap<String, Arc<Value>>,
    pub market_confirmation_versions: BTreeMap<String, u64>,
}

/// Selected-market read projection. Depth is the exact immutable Arc installed
/// by the reducer, not a second book state machine. BTC market readers never
/// take the discovery component's aggregate publication lock.
#[derive(Clone, Default)]
struct MarketReadState {
    token_ids: BTreeSet<String>,
    native_cursor_domain: bool,
    last_event_cursor: u64,
    market: Option<Arc<Value>>,
    books: BTreeMap<String, Arc<Value>>,
    book_exchanges: BTreeMap<String, DateTime<Utc>>,
    physical_book_cursors: BTreeMap<String, u64>,
    event_book_cursors: BTreeMap<String, u64>,
    market_book_cursors: BTreeMap<String, u64>,
    recoveries: BTreeMap<String, Value>,
    unresolved_seed_gaps: BTreeSet<String>,
    capacity_fenced: bool,
    confirmation_sequence: u64,
    confirmation_facts: BTreeMap<String, Arc<Value>>,
    market_confirmation_versions: BTreeMap<String, u64>,
}

impl MarketReadState {
    fn bootstrap(market: Arc<Value>, token_ids: BTreeSet<String>) -> Self {
        Self { token_ids, native_cursor_domain: true, market: Some(market),
            ..Self::default() }
    }
    /// Prepare a native market group's selected-lane projection from the
    /// actor's already validated immutable post-states. Cloning this small
    /// map only clones book Arcs, never full depth. The caller installs it
    /// with replay under the same market lane after writer admission.
    fn prepare_native_group(&self, market_id: &str, market_cursor: u64,
        events: &[Value], actions: Vec<ProjectionAction>) -> Result<Self> {
        ensure!(market_cursor > 0 && !events.is_empty(), "native market group identity");
        ensure!(events.iter().all(|event| event["market_id"] == market_id),
            "native projection crosses market boundary");
        let mut next = self.clone();
        if next.capacity_fenced {
            ensure!(events.iter().any(|event| event["event_type"] == "recovery_started")
                || !next.recoveries.is_empty(),
                "capacity-fenced market requires recovery barrier");
        }
        next.native_cursor_domain = true;
        let mut actions = actions.into_iter();
        for event in events {
            let cursor = event["cursor"].as_u64().context("native event cursor")?;
            ensure!(cursor == next.last_event_cursor.checked_add(1)
                .context("native event cursor overflow")?,
                "native market event cursor is not contiguous");
            next.last_event_cursor = cursor;
            apply_token_recovery(&mut next.recoveries, event)?;
            if is_token_recovery(event) {
                let token = event["token_id"].as_str().context("recovery token identity")?;
                ensure!(next.token_ids.contains(token), "recovery token outside market binding");
                next.confirmation_facts.remove(token);
                next.market_confirmation_versions.remove(token);
                continue;
            }
            let action = actions.next().context("native projection action missing")?;
            match action {
                ProjectionAction::Terminal { market_id: action_market, model, .. } => {
                    ensure!(action_market == market_id, "terminal projection market differs");
                    let retired = event["canonical_payload"]["retired_token_ids"]
                        .as_array().context("terminal retired tokens")?;
                    for token in retired {
                        let token = token.as_str().context("terminal token identity")?;
                        ensure!(next.token_ids.contains(token), "terminal token outside market binding");
                        next.books.remove(token);
                        next.market_book_cursors.remove(token);
                        next.physical_book_cursors.remove(token);
                        next.event_book_cursors.remove(token);
                        next.book_exchanges.remove(token);
                        next.recoveries.remove(token);
                        next.confirmation_facts.remove(token);
                        next.market_confirmation_versions.remove(token);
                    }
                    next.market = Some(model);
                }
                ProjectionAction::Book { token_id, physical_cursor, exchange_at,
                    clear_confirmation, model, .. } => {
                    ensure!(next.token_ids.contains(&token_id), "book token outside market binding");
                    ensure!(event["token_id"] == token_id && event["cursor"] == physical_cursor,
                        "native projection action identity differs");
                    // The native source cursor is a public event position; a
                    // physical archive position does not exist until the
                    // asynchronous writer commits. Never alias the two.
                    next.physical_book_cursors.remove(&token_id);
                    next.event_book_cursors.insert(token_id.clone(), physical_cursor);
                    next.market_book_cursors.insert(token_id.clone(), market_cursor);
                    if event["event_type"] == "book" {
                        next.unresolved_seed_gaps.remove(&token_id);
                    }
                    if let Some(exchange_at) = exchange_at {
                        next.book_exchanges.insert(token_id.clone(), exchange_at);
                    }
                    if clear_confirmation || next.books.get(&token_id).is_some_and(|old|
                        old["book_epoch"] != model["book_epoch"]
                            || old["sequence"] != model["sequence"])
                    {
                        next.confirmation_facts.remove(&token_id);
                        next.market_confirmation_versions.remove(&token_id);
                    }
                    next.books.insert(token_id, model);
                }
            }
        }
        ensure!(actions.next().is_none(), "native projection action surplus");
        if next.capacity_fenced && next.unresolved_seed_gaps.is_empty()
            && next.recoveries.is_empty()
            && next.token_ids.iter().all(|token| next.books.contains_key(token)) {
            next.capacity_fenced = false;
        }
        Ok(next)
    }

    fn project(memory: &Memory, market_id: &str, token_ids: BTreeSet<String>) -> Self {
        let mut state = Self { token_ids, market: memory.markets.get(market_id).cloned(),
            confirmation_sequence: memory.market_confirmation_sequences.get(market_id)
                .copied().unwrap_or(0), ..Self::default() };
        for token in &state.token_ids {
            if memory.base.as_ref().and_then(|base| base["gaps"].as_array())
                .is_some_and(|gaps| gaps.iter().any(|gap|
                    gap["token_id"] == token.as_str() && gap["resolved"] != true)) {
                state.unresolved_seed_gaps.insert(token.clone());
            }
            if let Some(recovery) = memory.recoveries.get(token) {
                state.recoveries.insert(token.clone(), recovery.clone());
            }
            let (Some(market_cursor), Some(physical_cursor), Some(book)) = (
                memory.market_book_cursors.get(token), memory.book_cursors.get(token),
                memory.books.get(token)) else { continue; };
            if *physical_cursor < memory.resume_floor { continue; }
            state.books.insert(token.clone(), book.clone());
            state.physical_book_cursors.insert(token.clone(), *physical_cursor);
            state.event_book_cursors.insert(token.clone(), *physical_cursor);
            state.market_book_cursors.insert(token.clone(), *market_cursor);
            if !state.recoveries.contains_key(token) && !state.unresolved_seed_gaps.contains(token) {
                if let Some(fact) = memory.confirmation_facts.get(token) {
                    if fact["book_epoch"] == book["book_epoch"] && fact["sequence"] == book["sequence"]
                        && fact["state_checksum"] == book["state_checksum"] {
                        state.confirmation_facts.insert(token.clone(), fact.clone());
                        if let Some(version) = memory.market_confirmation_versions.get(token) {
                            state.market_confirmation_versions.insert(token.clone(), *version);
                        }
                    }
                }
            }
        }
        state
    }
}
pub struct PublicReplay {
    pub batches: Vec<Arc<Batch>>,
    pub next: u64,
    pub boundary_cursor: u64,
    pub caught_up: bool,
    pub confirmation_sequence: u64,
    pub confirmation_books: Vec<Arc<Value>>,
}
impl MemoryReader {
    pub fn subscribe_market(&self, market_id: &str, stream_epoch: &str) -> Result<watch::Receiver<u64>> {
        self.market_replay.subscribe(market_id, stream_epoch)
    }
    pub fn capture_market(&self, market_id: &str, token_ids: &[String]) -> Result<MarketMemorySnapshot> {
        ensure!(!market_id.is_empty() && !token_ids.is_empty(), "market snapshot identity");
        ensure!(token_ids.iter().collect::<BTreeSet<_>>().len() == token_ids.len(),
            "duplicate market snapshot token");
        // Physical durability is a separate watermark. A failed writer must
        // not hide a still-live, market-local in-memory stream.
        ensure!(self.market_health.source_ready.load(Ordering::Acquire), "source unavailable");
        let snapshot = self.market_replay.read_state(market_id, |state, watermark| {
            let state = state.as_ref().context("market snapshot unavailable")?;
            ensure!(state.native_cursor_domain, "market snapshot uses obsolete cursor domain");
            ensure!(token_ids.iter().all(|token| state.token_ids.contains(token)),
                "market snapshot token binding differs");
            ensure!(state.last_event_cursor > 0
                && state.event_book_cursors.values().all(|cursor| *cursor <= state.last_event_cursor),
                "market snapshot event boundary differs");
            fn selected<T: Clone>(values: &BTreeMap<String, T>, tokens: &[String]) -> BTreeMap<String, T> {
                tokens.iter().filter_map(|token| values.get(token)
                    .map(|value| (token.clone(), value.clone()))).collect()
            }
            Ok(MarketMemorySnapshot {
                watermark: MarketStreamWatermark {
                    stream_epoch: watermark.stream_epoch, market_id: watermark.market_id,
                    published_cursor: watermark.published_cursor, durable_cursor: watermark.durable_cursor,
                    replay_floor: watermark.replay_floor,
                },
                last_market_event_cursor: state.last_event_cursor,
                market: state.market.clone(), books: selected(&state.books, token_ids),
                physical_book_cursors: selected(&state.physical_book_cursors, token_ids),
                event_book_cursors: selected(&state.event_book_cursors, token_ids),
                market_book_cursors: selected(&state.market_book_cursors, token_ids),
                recoveries: selected(&state.recoveries, token_ids),
                unresolved_seed_gaps: token_ids.iter().filter(|token|
                    state.unresolved_seed_gaps.contains(*token)).cloned().collect(),
                capacity_fenced: state.capacity_fenced,
                confirmation_sequence: state.confirmation_sequence,
                confirmation_facts: selected(&state.confirmation_facts, token_ids),
                market_confirmation_versions: selected(&state.market_confirmation_versions, token_ids),
            })
        })?;
        // A pause or disk failure racing the selected-lane read cannot pass
        // merely because the source was ready when the request began.
        ensure!(self.market_health.source_ready.load(Ordering::Acquire), "source unavailable");
        Ok(snapshot)
    }
    /// Replay one market's complete observation groups without traversing or
    /// retaining any unrelated market's events.
    pub fn replay_market(
        &self, market_id: &str, stream_epoch: &str, after_cursor: u64,
        maximum_groups: usize, maximum_bytes: usize,
    ) -> Result<MarketPage<Batch>> {
        ensure!(self.market_health.source_ready.load(Ordering::Acquire), "source unavailable");
        self.market_replay.read_state(market_id, |state, _| {
            ensure!(state.as_ref().is_some_and(|state| state.native_cursor_domain),
                "market replay uses obsolete cursor domain");
            Ok(())
        })?;
        self.market_replay.replay(
            market_id, stream_epoch, after_cursor, maximum_groups, maximum_bytes,
        )
    }
    pub fn market_stream_watermark(&self, market_id: &str) -> Result<Option<MarketStreamWatermark>> {
        let Some(_) = self.market_replay.watermark_optional(market_id)? else {
            return Ok(None);
        };
        self.market_replay.inspect_lane(market_id, |_, replay| Ok(Some(MarketStreamWatermark {
            stream_epoch: replay.stream_epoch.clone(),
            market_id: market_id.to_owned(),
            published_cursor: replay.published_cursor,
            durable_cursor: replay.durable_cursor,
            replay_floor: replay.replay_floor,
        })))
    }
    /// Bounded control-plane observation. This deliberately does not capture
    /// books and remains readable when the market-data snapshot is unavailable.
    pub fn management_status(&self) -> Result<Value> {
        let m = self.memory.lock().map_err(|_| anyhow::anyhow!("publication poisoned"))?;
        let queued = self.market_persistence.used();
        let (wire_tokens, wire_sockets, retiring_shards) = m.acquisition_statistics
            .map(|(tokens, sockets, retiring)| (Some(tokens), Some(sockets), Some(retiring)))
            .unwrap_or((None, None, None));
        let phases = m.wire_token_ids.as_ref().zip(m.draining_token_ids.as_ref())
            .filter(|(wire, draining)| draining.is_subset(wire))
            .map(|(wire, draining)| {
                let hot = m.binding_token_ids.union(wire).count();
                let prepared = m.binding_token_ids.difference(wire).count();
                let active = wire.difference(draining).count();
                (hot, prepared, active, draining.len())
            });
        Ok(json!({
            "schema_version":"marketcow.publication-management.v1",
            "source_ready":m.source_ready && m.error.is_none(),
            "persistence_state":if m.error.is_some() {"failed"} else {"running"},
            "metadata_market_count":m.markets.len(),
            "publication_binding_tokens":m.binding_token_ids.len(),
            "hot_binding_tokens":phases.map(|(hot,_,_,_)|hot),
            "wire_tokens":wire_tokens,
            "wire_sockets":wire_sockets,
            "retiring_shards":retiring_shards,
            "prepared_tokens":phases.map(|(_,prepared,_,_)|prepared),
            "active_tokens":phases.map(|(_,_,active,_)|active),
            "draining_tokens":phases.map(|(_,_,_,draining)|draining),
            "phase_counts_authoritative":phases.is_some(),
            "hot_token_limit":m.acquisition_detail.as_ref().and_then(|detail| detail["token_limit"].as_u64()),
            "wire_socket_limit":m.acquisition_detail.as_ref().and_then(|detail| detail["socket_limit"].as_u64()),
            "published_cursor":m.cursor,
            "durable_cursor":m.persisted,
            "persistence_queue_batches":m.queued.saturating_add(queued.batches),
            "persistence_queue_bytes":m.queued_bytes.saturating_add(queued.bytes),
            "retirement_submitted":m.retirement_submitted,
            "retirement_persisted":m.retirement_persisted,
        }))
    }
    /// Diagnostic lookup only. The same byte-bounded replay which serves WS
    /// also bounds how long an unpublished-to-client event can be inspected.
    pub fn timeline_event(&self, cursor: u64) -> Result<Option<(Value, Option<SourceBatchTiming>)>> {
        let memory = self.memory.lock().map_err(|_| anyhow::anyhow!("publication poisoned"))?;
        Ok(memory.replay.iter().find_map(|batch| {
            batch.validated.events().iter()
                .find(|event| event["cursor"].as_u64() == Some(cursor))
                .map(|event| (event.clone(), batch.timing_snapshot()))
        }))
    }
    pub fn market_timeline_event(&self, market: &str, stream_epoch: &str,
        event_cursor: u64) -> Result<Option<(Value, Option<SourceBatchTiming>)>> {
        ensure!(event_cursor > 0, "market timeline event cursor zero");
        self.market_replay.find_payload(market, stream_epoch, |batch| {
            batch.validated.events().iter()
                .find(|event| event["cursor"].as_u64() == Some(event_cursor))
                .map(|event| (event.clone(), batch.timing_snapshot()))
        })
    }
    /// Control-plane identity remains readable while acquisition is
    /// intentionally paused. This does not expose books, cursors or a usable
    /// market-data snapshot and therefore cannot bypass the readiness fence.
    pub fn installed_markets(&self) -> Result<BTreeMap<String, Arc<Value>>> {
        let memory = self
            .memory
            .lock()
            .map_err(|_| anyhow::anyhow!("publication poisoned"))?;
        ensure!(memory.error.is_none(), "publication persistence failed");
        Ok(memory.markets.clone())
    }
    pub fn acquisition_statistics(&self) -> Option<Value> {
        self.memory.lock().ok()?.acquisition_statistics.map(|(tokens,sockets,retiring)|
            json!({"tokens":tokens,"sockets":sockets,"retiring_shards":retiring}))
    }
    pub fn retirement_status(&self) -> Result<(u64, u64)> {
        let m = self
            .memory
            .lock()
            .map_err(|_| anyhow::anyhow!("publication poisoned"))?;
        ensure!(m.error.is_none(), "publication persistence failed");
        Ok((m.retirement_submitted, m.retirement_persisted))
    }
    pub fn capture(&self) -> Result<MemoryView> {
        MemoryView::capture(&self.memory.lock().unwrap())
    }
    /// Pin a full-sync boundary against count-based eviction until the
    /// existing response timeout. The global byte cap still wins: overflow
    /// expires the affected resume rather than blocking publication.
    pub fn capture_for_resume(
        &self,
        ttl: std::time::Duration,
        capacity: usize,
    ) -> Result<MemoryView> {
        ensure!(
            !ttl.is_zero() && (1..=4).contains(&capacity),
            "snapshot lease budget"
        );
        let mut m = self.memory.lock().unwrap();
        let now = std::time::Instant::now();
        m.snapshot_leases.retain(|_, until| *until > now);
        let view = MemoryView::capture(&m)?;
        ensure!(
            m.snapshot_leases.contains_key(&view.cursor) || m.snapshot_leases.len() < capacity,
            "snapshot resume capacity"
        );
        // Re-reading the same boundary cannot extend a lease indefinitely.
        m.snapshot_leases.entry(view.cursor).or_insert(now + ttl);
        Ok(view)
    }
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    /// A cheap writer watermark for live consumers. The durability worker
    /// updates this only after persistence succeeds and notifies `changed`.
    /// It deliberately does not capture or clone any book state.
    pub fn durable_cursor(&self) -> Result<u64> {
        let m = self.memory.lock().map_err(|_| anyhow::anyhow!("publication poisoned"))?;
        ensure!(m.error.is_none(), "persistence failed");
        Ok(m.persisted)
    }

    /// Constant-time diagnostics for a client that fell behind the shared
    /// replay. It does not extend retention or make an expired cursor usable.
    pub fn replay_window(&self, after: u64) -> Result<Value> {
        let m = self.memory.lock().map_err(|_| anyhow::anyhow!("publication poisoned"))?;
        Ok(Self::replay_window_locked(&m, after))
    }

    fn replay_window_locked(m: &Memory, after: u64) -> Value {
        let oldest = m.replay.front();
        let oldest_visible_at = oldest.and_then(|batch|
            batch.timing_snapshot().and_then(|timing| timing.shared_replay_visible_at));
        json!({"after_cursor":after,"current_cursor":m.cursor,
            "durable_cursor":m.persisted,"resume_floor":m.resume_floor,
            "oldest_cursor":oldest.and_then(|batch|batch.validated.events().first())
                .and_then(|event|event["cursor"].as_u64()),
            "oldest_replay_visible_at":oldest_visible_at,
            "oldest_replay_age_ms":oldest_visible_at.map(|at|
                (Utc::now()-at).num_milliseconds().max(0)),
            "retained_batches":m.replay.len(),"retained_bytes":m.replay_bytes})
    }

    /// Shared immutable batches, not per-client copies. Confirmation baseline
    /// is captured under the same lock only if this page reaches its boundary.
    pub fn replay(
        &self,
        after: u64,
        confirmation_after: u64,
        max_batches: usize,
        max_bytes: usize,
    ) -> Result<PublicReplay> {
        let m = self.memory.lock().map_err(|_| anyhow::anyhow!("publication poisoned"))?;
        Self::replay_locked(&m, after, confirmation_after, max_batches, max_bytes)
    }

    fn replay_locked(
        m: &Memory,
        after: u64,
        confirmation_after: u64,
        max_batches: usize,
        max_bytes: usize,
    ) -> Result<PublicReplay> {
        ensure!((1..=64).contains(&max_batches), "replay batch budget");
        ensure!(max_bytes > 0, "replay byte budget");
        ensure!(m.source_ready && m.error.is_none(), "source unavailable");
        ensure!(after <= m.cursor, "future cursor");
        ensure!(after >= m.resume_floor, "memory replay expired");
        let mut batches = Vec::new();
        let mut next = after;
        let mut bytes = 0usize;
        for batch in &m.replay {
            let end = batch.validated.events().last().context("empty batch")?["cursor"]
                .as_u64()
                .context("cursor")?;
            if end <= after {
                continue;
            }
            if batch.bytes > max_bytes.saturating_sub(bytes) {
                ensure!(
                    !batches.is_empty(),
                    "single batch exceeds public replay budget"
                );
                break;
            }
            let first = batch.validated.events().first().context("empty batch")?["cursor"]
                .as_u64()
                .context("cursor")?;
            ensure!(
                first <= next.checked_add(1).context("cursor overflow")?,
                "memory replay expired"
            );
            batches.push(batch.clone());
            bytes += batch.bytes;
            next = end;
            if batches.len() == max_batches {
                break;
            }
        }
        ensure!(after == m.cursor || next > after, "memory replay expired");
        let caught_up = next == m.cursor;
        let confirmation_books = if caught_up {
            m.confirmations
                .iter()
                .filter(|(_, seq)| **seq > confirmation_after)
                .filter_map(|(token, _)| m.confirmation_facts.get(token).cloned())
                .collect()
        } else {
            Vec::new()
        };
        Ok(PublicReplay {
            batches,
            next,
            boundary_cursor: m.cursor,
            caught_up,
            confirmation_books,
            confirmation_sequence: if caught_up {
                m.confirmation_cursor
            } else {
                confirmation_after
            },
        })
    }

    /// Include the cursor actually requested by this replay call, rather
    /// than the initial cursor of a long-lived WS connection, on failure.
    pub fn replay_with_diagnostics(
        &self,
        after: u64,
        confirmation_after: u64,
        max_batches: usize,
        max_bytes: usize,
    ) -> Result<PublicReplay> {
        let m = self.memory.lock().map_err(|_| anyhow::anyhow!("publication poisoned"))?;
        Self::replay_locked(&m, after, confirmation_after, max_batches, max_bytes)
            .with_context(|| format!("replay after cursor {after}; window={}",
                Self::replay_window_locked(&m, after)))
    }
}

impl MemoryView {
    fn capture(m: &Memory) -> Result<Self> {
        ensure!(m.source_ready, "upstream WebSocket recovery pending");
        ensure!(m.error.is_none(), "persistence failed");
        // A pause establishes a new acquisition generation.  Do not expose a
        // pre-pause book as fresh merely because another market has proved the
        // resumed source connection alive.  Markets that have not crossed the
        // resume floor are omitted and remain locally unavailable.
        let fresh = |token: &&String| {
            m.book_cursors
                .get(*token)
                .is_some_and(|cursor| *cursor >= m.resume_floor)
        };
        Ok(Self {
            cursor: m.cursor,
            persisted_cursor: m.persisted,
            confirmation_sequence: m.confirmation_cursor,
            books: m
                .books
                .iter()
                .filter(|(token, _)| fresh(token))
                .map(|(token, book)| (token.clone(), book.clone()))
                .collect(),
            markets: m.markets.clone(),
            recoveries: m.recoveries.clone(),
            book_cursors: m
                .book_cursors
                .iter()
                .filter(|(token, _)| fresh(token))
                .map(|(token, cursor)| (token.clone(), *cursor))
                .collect(),
            confirmation_versions: m
                .confirmations
                .iter()
                .filter(|(token, _)| fresh(token))
                .map(|(token, version)| (token.clone(), *version))
                .collect(),
            confirmation_facts: m
                .confirmation_facts
                .iter()
                .filter(|(token, _)| fresh(token))
                .map(|(token, fact)| (token.clone(), fact.clone()))
                .collect(),
            base: m.base.clone().context("missing stream seed")?,
            queued: m.queued,
        })
    }

    /// Legacy internal shape, built outside the ingestion lock. The public
    /// adapter consumes this same boundary without a network/SQLite bridge.
    fn into_source_state(self) -> Result<Value> {
        let mut state = self.base;
        state["latest_cursor"] = json!(self.cursor);
        state["persisted_cursor"] = json!(self.persisted_cursor);
        state["persistence_queue_depth"] = json!(self.queued);
        state["history_oldest_cursor"] =
            json!(self.cursor.checked_add(1).context("cursor overflow")?);
        state["books"] = json!(self.books.values().collect::<Vec<_>>());
        state["token_recoveries"] = json!(self.recoveries.values().collect::<Vec<_>>());
        let mut gaps = state["gaps"].as_array().context("base gaps")?.clone();
        gaps.extend(self.recoveries.values().map(|r| r["gap"].clone()));
        state["gaps"] = json!(gaps);
        state["markets"] = json!(self.markets.values().collect::<Vec<_>>());
        Ok(state)
    }
}
impl Publication {
    pub fn reader(&self) -> MemoryReader {
        MemoryReader {
            memory: self.memory.clone(),
            market_health: self.market_health.clone(),
            changed: self.changed.clone(),
            market_replay: self.market_replay.clone(),
            market_persistence: self.market_persistence.clone(),
        }
    }
    pub fn capture_view(&self) -> Result<MemoryView> {
        MemoryView::capture(&self.memory.lock().unwrap())
    }
    /// Internal shutdown audit only: source readiness is deliberately false
    /// after intake stops. This does not expose a usable consumer baseline.
    pub fn shutdown_recovery_facts(&self) -> Result<(BTreeMap<String, u64>, BTreeMap<String, Value>)> {
        ensure!(!self.market_health.persistence_failed.load(Ordering::Acquire),
            "publication persistence failed");
        let markets = self.memory.lock()
            .map_err(|_| anyhow::anyhow!("publication poisoned"))?
            .market_stream_epochs.keys().cloned().collect::<Vec<_>>();
        let mut event_cursors = BTreeMap::new();
        let mut facts = BTreeMap::new();
        for market in markets {
            let (cursor, recoveries) = self.market_replay.inspect_lane(&market, |state, _| {
                let state = state.as_ref().context("shutdown market state missing")?;
                Ok((state.last_event_cursor, state.recoveries.clone()))
            })?;
            event_cursors.insert(market, cursor);
            facts.extend(recoveries);
        }
        Ok((event_cursors, facts))
    }
    pub fn start(
        cursor: u64,
        base: Option<Value>,
        tokens: BTreeMap<String, String>,
        max_batches: usize,
        max_bytes: usize,
        batch_cap: usize,
        mut persist: impl FnMut(&[Arc<Batch>]) -> Result<()> + Send + 'static,
    ) -> Result<Self> {
        let mut publication = Self::start_managed(
            cursor,
            base,
            tokens,
            max_batches,
            max_bytes,
            batch_cap,
            move |work| match work {
                PersistenceWork::Events(batches) => persist(batches),
                PersistenceWork::Retire(_) => anyhow::bail!("retirement persistence not installed"),
            },
        )?;
        publication.retirement_enabled = false;
        Ok(publication)
    }

    pub fn start_managed(
        cursor: u64,
        base: Option<Value>,
        tokens: BTreeMap<String, String>,
        max_batches: usize,
        max_bytes: usize,
        batch_cap: usize,
        persist: impl FnMut(PersistenceWork<'_>) -> Result<()> + Send + 'static,
    ) -> Result<Self> {
        Self::start_managed_with_replay(
            cursor, base, tokens, max_batches, max_bytes, max_batches, max_bytes,
            batch_cap, persist,
        )
    }

    /// The persistence work queue and the in-memory WS replay retain different
    /// things and must have independent bounds. A slow client may need several
    /// seconds of replay even when the disk writer's queue is nearly empty.
    pub fn start_managed_with_replay(
        cursor: u64,
        mut base: Option<Value>,
        tokens: BTreeMap<String, String>,
        max_batches: usize,
        max_bytes: usize,
        replay_max_batches: usize,
        replay_max_bytes: usize,
        batch_cap: usize,
        persist: impl FnMut(PersistenceWork<'_>) -> Result<()> + Send + 'static,
    ) -> Result<Self> {
        ensure!(
            (1..=16384).contains(&max_batches)
                && max_bytes >= batch_cap
                && max_bytes <= 4 * 1024 * 1024 * 1024
                && batch_cap > 0
                && batch_cap <= 32 * 1024 * 1024,
            "explicit bounded queue limits required"
        );
        ensure!(
            (1..=65536).contains(&replay_max_batches)
                && replay_max_bytes >= batch_cap
                && replay_max_bytes <= 6 * 1024 * 1024 * 1024,
            "explicit bounded replay limits required"
        );
        let mut books = BTreeMap::new();
        let mut book_cursors = BTreeMap::new();
        let mut book_exchanges = BTreeMap::new();
        let mut markets: BTreeMap<String, Arc<Value>> = BTreeMap::new();
        let mut state_sizes = BTreeMap::new();
        if let Some(state) = &base {
            ensure!(
                state["latest_cursor"].as_u64() == Some(cursor),
                "startup boundary differs"
            );
            for book in state["books"].as_array().context("missing books")? {
                let token = book["token_id"].as_str().context("token")?.to_owned();
                state_sizes.insert((false, token.clone()), serde_json::to_vec(book)?.len());
                book_exchanges.insert(
                    token.clone(),
                    chrono::DateTime::parse_from_rfc3339(
                        book["exchange_at"].as_str().context("book exchange_at")?,
                    )?
                    .with_timezone(&chrono::Utc),
                );
                // The initial state is one atomic snapshot. Every included
                // book is causally safe at the snapshot's cursor.
                book_cursors.insert(token.clone(), cursor);
                books.insert(token, Arc::new(book.clone()));
            }
            for market in state["markets"].as_array().context("missing markets")? {
                state_sizes.insert(
                    (
                        true,
                        market["identity"]["market_id"]
                            .as_str()
                            .context("market")?
                            .into(),
                    ),
                    serde_json::to_vec(market)?.len(),
                );
                markets.insert(
                    market["identity"]["market_id"]
                        .as_str()
                        .context("market id")?
                        .into(),
                    Arc::new(market.clone()),
                );
            }
        }
        let state_bytes = base
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()?
            .map_or(0, |v| v.len());
        ensure!(
            state_bytes <= max_bytes.min(MAX_NON_QUEUE_STATE_BYTES),
            "initial memory state exceeds byte budget"
        );
        if let Some(state) = &mut base {
            state["books"] = json!([]);
            state["markets"] = json!([]);
        }
        let mut recoveries = BTreeMap::new();
        if let Some(items) = base.as_ref().and_then(|b| b["token_recoveries"].as_array()) {
            for item in items {
                let token = item["token_id"].as_str().context("recovery token")?;
                ensure!(
                    tokens.contains_key(token)
                        && recoveries.len()
                            < marketcow_runtime::discovery_source::MAX_SOURCE_TOKEN_IDENTITIES,
                    "recovery scope/cap"
                );
                recoveries.insert(token.into(), item.clone());
            }
        }
        if let Some(state) = &mut base {
            if let Some(gaps) = state["gaps"].as_array_mut() {
                gaps.retain(|gap| {
                    !gap["token_id"]
                        .as_str()
                        .is_some_and(|t| recoveries.contains_key(t))
                });
            }
            state["token_recoveries"] = json!([]);
        }
        let terminal_markets = markets
            .iter()
            .filter(|(_, market)| {
                matches!(
                    market["lifecycle_state"].as_str(),
                    Some("closed" | "resolved" | "invalid")
                )
            })
            .map(|(market, _)| market.clone())
            .collect();
        let memory = Arc::new(Mutex::new(Memory {
            market_stream_epochs: BTreeMap::new(),
            market_confirmation_sequences: BTreeMap::new(),
            market_confirmation_versions: BTreeMap::new(),
            market_published_cursors: BTreeMap::new(),
            market_durable_cursors: BTreeMap::new(),
            retiring_markets: BTreeSet::new(),
            retirement_submitted: 0,
            retirement_persisted: 0,
            acquisition_statistics: None,
            acquisition_detail: None,
            binding_token_ids: tokens.keys().cloned().collect(),
            token_market_ids: tokens.clone(),
            wire_token_ids: None,
            draining_token_ids: None,
            recoveries,
            source_ready: true,
            resume_floor: 0,
            cursor,
            persisted: cursor,
            queued: 0,
            queued_bytes: 0,
            error: None,
            base,
            books,
            book_cursors,
            market_book_cursors: BTreeMap::new(),
            book_exchanges,
            markets,
            terminal_markets,
            replay: VecDeque::new(),
            replay_bytes: 0,
            snapshot_leases: BTreeMap::new(),
            state_bytes,
            state_sizes,
            confirmation_cursor: 0,
            confirmations: BTreeMap::new(),
            confirmation_facts: BTreeMap::new(),
        }));
        // The physical archive has one writer identity. Market queues select
        // work fairly; the archive lock is acquired only on the disk worker,
        // never by a live market publisher.
        let persist: Arc<Mutex<Box<dyn for<'a> FnMut(PersistenceWork<'a>) -> Result<()> + Send>>> =
            Arc::new(Mutex::new(Box::new(persist)));
        let legacy_persist = persist.clone();
        let (sender, receiver) = mpsc::channel::<QueuedWork>();
        let (changed, _) = watch::channel(cursor);
        // Reserve one eighth for in-flight admission. The remaining bytes are
        // divided into whole market partitions before any publication; no
        // market can borrow another market's resumable history. Small offline
        // configurations reduce the slot count if one legal group would not
        // fit each slot. The production 2 GiB / 4-slot configuration yields
        // four independent 448 MiB replay lanes.
        let replay_headroom = (replay_max_bytes / 8)
            .min(replay_max_bytes.saturating_sub(batch_cap));
        let partitioned_bytes = replay_max_bytes - replay_headroom;
        // A blocked disk may fill the configured writer queue without
        // stopping memory publication. Do not split a tiny replay count into
        // so many lanes that one market cannot retain that in-flight prefix.
        // Parsed trees and materialized books can cost more than wire bytes;
        // small configured budgets may therefore support fewer partitions.
        let persistence_lane_limits = market_persistence_lane_limits(max_batches, max_bytes, batch_cap);
        let replay_market_slots = 4usize.min(replay_max_batches)
            .min((replay_max_batches / persistence_lane_limits.batches).max(1))
            .min((partitioned_bytes / (batch_cap * 4)).max(1));
        let replay_per_market_bytes = partitioned_bytes / replay_market_slots;
        let replay_per_market_groups = (replay_max_batches / replay_market_slots).max(1);
        // Tiny bounded fixtures deliberately exercise prefix rejection. In
        // production-sized queues the replay lane must hold one complete
        // undurable persistence lane, or the new disk capacity is unusable.
        if max_bytes >= 512 * 1024 * 1024 {
            ensure!(replay_per_market_bytes >= persistence_lane_limits.bytes,
                "market replay lane cannot retain its undurable persistence prefix");
        }
        let market_replay = Arc::new(MarketReplay::<Batch, Option<MarketReadState>>::new_partitioned_with_state(
            replay_per_market_groups, replay_per_market_bytes, replay_max_bytes,
            replay_market_slots,
        )?);
        marketcow_polymarket::async_eprintln!("{}", json!({"stage":"market_replay_limits",
            "market_slots":replay_market_slots,"groups_per_market":replay_per_market_groups,
            "bytes_per_market":replay_per_market_bytes,
            "total_bytes":replay_max_bytes,"admission_headroom_bytes":replay_headroom}));
        let market_persistence = Arc::new(MarketPersistence::new(MarketPersistenceLimits {
            batches: max_batches, bytes: max_bytes,
        })?);
        let replay_ack = market_replay.clone();
        let market_health = Arc::new(MarketHealth {
            source_ready: AtomicBool::new(true),
            accepting_events: AtomicBool::new(true),
            persistence_failed: AtomicBool::new(false),
        });
        let worker_health = market_health.clone();
        let retirement_queue = market_persistence.clone();
        let state = memory.clone();
        let notify = changed.clone();
        let worker = thread::Builder::new()
            .name("polymarket-durability".into())
            .spawn(move || {
                let mut pending = None;
                while let Some(work) = pending.take().or_else(|| receiver.recv().ok()) {
                    let batch=match work {
                        QueuedWork::Events(batch)=>batch,
                        QueuedWork::Retire(retirement)=>{
                            let result = (|| -> Result<()> {
                                for (market, epoch) in &retirement.market_epochs {
                                    if retirement_queue.is_registered(market)? {
                                        retirement_queue.wait_drained(market, epoch,
                                            std::time::Duration::from_secs(30))?;
                                    }
                                }
                                (*legacy_persist.lock().expect("physical writer poisoned"))
                                    (PersistenceWork::Retire(&retirement))
                            })();
                            let mut memory=state.lock().expect("publication mutex poisoned");
                            match result {
                                Ok(())=>{
                                    let released = (|| -> Result<()> {
                                        for market in &retirement.markets {
                                            if let Some(epoch) = retirement.market_epochs.get(market) {
                                                if retirement_queue.is_registered(market)? {
                                                    retirement_queue.finish_retire(market, epoch)?;
                                                }
                                            }
                                            if replay_ack.is_registered(market)? {
                                                let epoch = memory.market_stream_epochs.get(market)
                                                    .context("retired market epoch missing")?;
                                                replay_ack.retire_with_durability_gap(market, epoch,
                                                    retirement.live_only_markets.contains(market))?;
                                            }
                                            memory.market_published_cursors.remove(market);
                                            memory.market_durable_cursors.remove(market);
                                            memory.market_stream_epochs.remove(market);
                                            memory.market_confirmation_sequences.remove(market);
                                            memory.retiring_markets.remove(market);
                                        }
                                        Ok(())
                                    })();
                                    if let Err(error) = released {
                                        marketcow_polymarket::async_eprintln!("{}", json!({
                                            "stage":"publication_durability_failed",
                                            "worker":"retirement_release",
                                            "ticket":retirement.ticket,
                                            "markets":retirement.markets,
                                            "error":format!("{error:#}"),
                                        }));
                                        worker_health.persistence_failed.store(true, Ordering::Release);
                                        memory.error=Some(format!("{error:#}"));
                                        notify.send_modify(|n|*n=n.wrapping_add(1));
                                        let _=replay_ack.notify_all();
                                        break;
                                    }
                                    let Some((remaining, remaining_bytes)) = memory.queued
                                        .checked_sub(1).zip(memory.queued_bytes.checked_sub(retirement.bytes))
                                    else {
                                        marketcow_polymarket::async_eprintln!("{}", json!({
                                            "stage":"publication_durability_failed",
                                            "worker":"retirement_accounting",
                                            "ticket":retirement.ticket,
                                            "error":"retirement queue accounting underflow",
                                        }));
                                        worker_health.persistence_failed.store(true, Ordering::Release);
                                        memory.error = Some("retirement queue accounting underflow".into());
                                        notify.send_modify(|n| *n = n.wrapping_add(1));
                                        break;
                                    };
                                    memory.queued = remaining;
                                    memory.queued_bytes = remaining_bytes;
                                    memory.retirement_persisted=retirement.ticket;
                                },
                                Err(error)=>{
                                    if error.downcast_ref::<MarketDrainTimeout>().is_some() {
                                        if worker_health.persistence_failed.load(Ordering::Acquire) {
                                            // Another worker has already supplied the fatal
                                            // physical error. Do not spin a pending retirement
                                            // forever while shutdown joins this thread.
                                            break;
                                        }
                                        marketcow_polymarket::async_eprintln!("{}", json!({
                                            "stage":"retirement_drain_pending",
                                            "ticket":retirement.ticket,
                                            "market_count":retirement.markets.len(),
                                            "markets":retirement.markets.iter().take(4).collect::<Vec<_>>(),
                                            "error":format!("{error:#}"),
                                        }));
                                        // No physical retirement was attempted. Keep the
                                        // original reservation, fence and ticket. The fair
                                        // market writer may still drain; reusing this exact
                                        // immutable work preserves ordered crash recovery.
                                        pending = Some(QueuedWork::Retire(retirement));
                                        drop(memory);
                                        thread::sleep(std::time::Duration::from_millis(250));
                                        continue;
                                    }
                                    marketcow_polymarket::async_eprintln!("{}", json!({
                                        "stage":"publication_durability_failed",
                                        "worker":"retirement_writer",
                                        "ticket":retirement.ticket,
                                        "market_count":retirement.markets.len(),
                                        "markets":retirement.markets.iter().take(4).collect::<Vec<_>>(),
                                        "error":format!("{error:#}"),
                                    }));
                                    worker_health.persistence_failed.store(true, Ordering::Release);
                                    memory.error=Some(format!("{error:#}"));
                                    notify.send_modify(|n|*n=n.wrapping_add(1));
                                    let _=replay_ack.notify_all();
                                    break;
                                },
                            }
                            notify.send_modify(|n|*n=n.wrapping_add(1));
                            continue;
                        }
                    };
                    let terminal = batch.validated.events()[0]["event_type"] == "market_terminal";
                    let mut count = batch.validated.events().len();
                    let mut bytes = batch.bytes;
                    let mut group = vec![batch];
                    // Drain only available work, without a batching delay. Keep terminal
                    // evidence separate and count in-flight work against the same limits.
                    while !terminal && count < 256 {
                        let Ok(work) = receiver.try_recv() else { break };
                        let next=match work {
                            QueuedWork::Events(batch)=>batch,
                            other=>{pending=Some(other);break;},
                        };
                        if next.validated.events()[0]["event_type"] == "market_terminal"
                            || count + next.validated.events().len() > 256
                            || bytes + next.bytes > batch_cap
                        {
                            pending = Some(QueuedWork::Events(next));
                            break;
                        }
                        count += next.validated.events().len();
                        bytes += next.bytes;
                        group.push(next);
                    }
                    // Deliberately outside the memory mutex, including fsync and SQLite commit.
                    let first_cursor = group.first().unwrap().validated.events().first().unwrap()["cursor"].as_u64().unwrap();
                    let last_cursor = group.last().unwrap().validated.events().last().unwrap()["cursor"].as_u64().unwrap();
                    let disk_started=std::time::Instant::now();
                    let result = (*legacy_persist.lock().expect("physical writer poisoned"))
                        (PersistenceWork::Events(&group));
                    let disk_elapsed = disk_started.elapsed();
                    let durable_at = result.is_ok().then(Utc::now);
                    if let Some(at) = durable_at {
                        for batch in &group {
                            if let Some(timing) = batch.timing.lock().expect("batch timing poisoned").as_mut() {
                                timing.durable_at = Some(at);
                            }
                        }
                    }
                    marketcow_polymarket::async_eprintln!("{}",json!({"stage":"durability_worker",
                        "elapsed_us":disk_elapsed.as_micros(),"event_count":count,
                        "first_cursor":first_cursor,"last_cursor":last_cursor,
                        "persist_succeeded_at":durable_at,
                        "persist_succeeded":result.is_ok()}));
                    let mut memory = state.lock().expect("publication mutex poisoned");
                    match result {
                        Ok(()) => {
                            let updates = (|| -> Result<BTreeMap<String, u64>> {
                                let mut updates = BTreeMap::new();
                                for batch in &group {
                                    let (market, epoch, cursor) = batch.validated
                                        .single_market_group().context("durable market group missing")?;
                                    ensure!(memory.market_stream_epochs.get(market)
                                        .is_some_and(|current| current == epoch),
                                        "durable market epoch differs");
                                    let previous = updates.get(market).copied()
                                        .or_else(|| memory.market_durable_cursors.get(market).copied())
                                        .unwrap_or(0);
                                    ensure!(previous.checked_add(1) == Some(cursor),
                                        "durable market group gap");
                                    replay_ack.acknowledge_durable(market, epoch, cursor)?;
                                    updates.insert(market.to_owned(), cursor);
                                }
                                Ok(updates)
                            })();
                            let updates = match updates {
                                Ok(updates) => updates,
                                Err(error) => {
                                    worker_health.persistence_failed.store(true, Ordering::Release);
                                    memory.error = Some(format!("{error:#}"));
                                    notify.send_modify(|n| *n = n.wrapping_add(1));
                                    let _ = replay_ack.notify_all();
                                    break;
                                }
                            };
                            memory.market_durable_cursors.extend(updates);
                            memory.persisted = group.last().unwrap().validated.events().last().unwrap()["cursor"]
                                .as_u64()
                                .unwrap();
                            let Some((remaining, remaining_bytes)) = memory.queued
                                .checked_sub(group.len()).zip(memory.queued_bytes.checked_sub(bytes))
                            else {
                                worker_health.persistence_failed.store(true, Ordering::Release);
                                memory.error = Some("event queue accounting underflow".into());
                                notify.send_modify(|n| *n = n.wrapping_add(1));
                                break;
                            };
                            memory.queued = remaining;
                            memory.queued_bytes = remaining_bytes;
                        }
                        Err(error) => {
                            marketcow_polymarket::async_eprintln!("{}", json!({
                                "stage":"publication_durability_failed",
                                "worker":"legacy_event_writer",
                                "first_cursor":first_cursor,
                                "last_cursor":last_cursor,
                                "error":format!("{error:#}"),
                            }));
                            worker_health.persistence_failed.store(true, Ordering::Release);
                            memory.error = Some(format!("{error:#}"));
                            notify.send_modify(|n| *n = n.wrapping_add(1));
                            let _ = replay_ack.notify_all();
                            break;
                        }
                    }
                    notify.send_modify(|n| *n = n.wrapping_add(1));
                }
            })?;
        let fair_queue = market_persistence.clone();
        let fair_replay = market_replay.clone();
        let fair_health = market_health.clone();
        let fair_memory = memory.clone();
        let fair_notify = changed.clone();
        let market_worker = thread::Builder::new()
            .name("polymarket-market-durability".into())
            .spawn(move || {
                let result = fair_queue.run_writer(&fair_replay, |work| {
                    let group = [work.payload.clone()];
                    (*persist.lock().expect("physical writer poisoned"))
                        (PersistenceWork::Events(&group))?;
                    if let Some(timing) = work.payload.timing.lock()
                        .map_err(|_| anyhow::anyhow!("batch timing poisoned"))?.as_mut() {
                        timing.durable_at = Some(Utc::now());
                    }
                    Ok(())
                });
                if let Err(error) = result {
                    marketcow_polymarket::async_eprintln!("{}", json!({
                        "stage":"publication_durability_failed",
                        "worker":"market_writer",
                        "error":format!("{error:#}"),
                    }));
                    fair_health.persistence_failed.store(true, Ordering::Release);
                    if let Err(abandon_error) = fair_queue.abandon_pending() {
                        marketcow_polymarket::async_eprintln!("{}", json!({
                            "stage":"publication_durability_failed",
                            "worker":"market_writer_cleanup",
                            "error":format!("{abandon_error:#}"),
                        }));
                    }
                    if let Ok(mut memory) = fair_memory.lock() {
                        memory.error = Some(format!("{error:#}"));
                    }
                    fair_notify.send_modify(|n| *n = n.wrapping_add(1));
                    let _ = fair_replay.notify_all();
                }
            })?;
        let mut market_publishers = BTreeMap::new();
        {
            let mut control = memory.lock().map_err(|_| anyhow::anyhow!("publication poisoned"))?;
            for market in tokens.values().cloned().collect::<BTreeSet<_>>() {
                let metadata = control.markets.get(&market).cloned()
                    .context("configured market metadata missing")?;
                let epoch = uuid::Uuid::new_v4().simple().to_string();
                control.market_stream_epochs.insert(market.clone(), epoch.clone());
                market_replay.register(&market, &epoch)?;
                market_persistence.register(&market, &epoch,
                    market_persistence_lane_limits(max_batches, max_bytes, batch_cap))?;
                let bindings = tokens.iter().filter(|(_, owner)| *owner == &market)
                    .map(|(token, owner)| (token.clone(), owner.clone()))
                    .collect::<BTreeMap<_, _>>();
                market_replay.update_state(&market, |state| {
                    *state = Some(MarketReadState::bootstrap(metadata, bindings.keys().cloned().collect()));
                })?;
                market_publishers.insert(market.clone(), Arc::new(MarketPublisher {
                    market_id: market, gate: Mutex::new(()), replay: market_replay.clone(),
                    persistence: market_persistence.clone(), health: market_health.clone(),
                    catalog_bindings: bindings, batch_cap,
                    confirmation_byte_limit: max_bytes.min(MAX_NON_QUEUE_STATE_BYTES),
                    volatile: AtomicBool::new(false), volatile_groups: AtomicU64::new(0),
                }));
            }
        }
        Ok(Self {
            memory,
            market_health,
            changed,
            market_replay,
            market_persistence,
            market_worker: Some(market_worker),
            market_publishers: std::sync::RwLock::new(market_publishers),
            sender: Some(sender),
            worker: Some(worker),
            tokens,
            pending_bindings: BTreeMap::new(),
            retirement_enabled: true,
            max_batches,
            max_bytes,
            replay_max_batches,
            replay_max_bytes,
            batch_cap,
            hot_timing: PublicationTiming::default(),
        })
    }
    pub fn take_hot_path_timing(&mut self) -> Value {
        let timing = std::mem::take(&mut self.hot_timing);
        json!({
            "validation":timing.validation.as_json(),
            "validated_events":timing.validated_events,
            "validated_bytes":timing.validated_bytes,
            "capacity_wait":timing.capacity_wait.as_json(),
            "memory_lock_wait":timing.memory_lock_wait.as_json(),
            "memory_lock_held":timing.memory_lock_held.as_json(),
        })
    }
    pub fn cursor(&self) -> Result<u64> {
        let m = self.memory.lock().unwrap();
        ensure!(m.error.is_none(), "persistence failed: {:?}", m.error);
        Ok(m.cursor)
    }
    /// The acquisition owner must first expire all scopes referencing these
    /// markets and fence their actors/recovery jobs. This drops current state,
    /// not source history, and queues disk retirement without awaiting I/O.
    pub fn retire_catalog_markets(
        &mut self,
        markets: std::collections::BTreeSet<String>,
    ) -> Result<()> {
        self.retire_catalog_markets_inner(markets, false)
    }
    pub fn check_retirement(&mut self, markets: std::collections::BTreeSet<String>) -> Result<()> {
        self.retire_catalog_markets_inner(markets, true)
    }
    fn retire_catalog_markets_inner(
        &mut self,
        markets: std::collections::BTreeSet<String>,
        check_only: bool,
    ) -> Result<()> {
        ensure!(
            self.retirement_enabled,
            "retirement persistence not installed"
        );
        ensure!(
            !markets.is_empty() && markets.len() <= 4096,
            "retirement market capacity"
        );
        let tokens: std::collections::BTreeSet<String> = self
            .tokens
            .iter()
            .filter(|(_, id)| markets.contains(*id))
            .map(|(token, _)| token.clone())
            .collect();
        let mut m = self.memory.lock().unwrap();
        let cold_records = markets.iter().map(|id| {
            let record = m.markets.get(id).context("retirement cold catalog record missing")?;
            ensure!(serde_json::to_vec(record.as_ref())?.len() <= 256 * 1024,
                "retirement cold catalog record exceeds read budget");
            Ok((id.clone(), record.as_ref().clone()))
        }).collect::<Result<BTreeMap<_, _>>>()?;
        ensure!(
            markets
                .iter()
                .all(|id| m.markets.contains_key(id)
                    || self.tokens.values().any(|market| market == id)),
            "retired market not admitted"
        );
        ensure!(m.error.is_none(), "publication unavailable");
        ensure!(markets.is_disjoint(&m.retiring_markets), "market retirement already pending");
        let removed_keys: Vec<_> = m
            .state_sizes
            .keys()
            .filter(|(market, id)| {
                if *market {
                    markets.contains(id)
                } else {
                    tokens.contains(id)
                }
            })
            .cloned()
            .collect();
        let removed_bytes = removed_keys
            .iter()
            .map(|key| m.state_sizes[key])
            .sum::<usize>();
        let state_bytes = m
            .state_bytes
            .checked_sub(removed_bytes)
            .context("retirement size accounting")?;
        // Keep the owner registry fenced while capturing the irreversible
        // live-only suffix and admitting its immutable retirement work item.
        let mut owners = self.market_publishers.write()
            .map_err(|_| anyhow::anyhow!("market owner registry poisoned"))?;
        let live_only_markets = markets.iter().filter(|market|
            owners.get(*market).is_some_and(|owner| owner.volatile.load(Ordering::Acquire)))
            .cloned().collect::<std::collections::BTreeSet<_>>();
        let bytes = serde_json::to_vec(&json!({"after":m.cursor,"markets":markets,
            "cold_records":cold_records,"catalog_bindings":self.pending_bindings,
            "live_only_markets":live_only_markets}))?.len();
        let queued = m.queued.checked_add(1).context("retirement queue count overflow")?;
        let queued_bytes = m.queued_bytes.checked_add(bytes)
            .context("retirement queue byte overflow")?;
        ensure!(bytes <= self.batch_cap && queued <= self.max_batches
            && queued_bytes <= self.max_bytes, "retirement queue capacity");
        if check_only { return Ok(()); }
        let ticket = m.retirement_submitted.checked_add(1)
            .context("retirement ticket overflow")?;
        let market_epochs = markets.iter().filter_map(|market|
            m.market_stream_epochs.get(market).map(|epoch| (market.clone(), epoch.clone())))
            .collect::<BTreeMap<_, _>>();
        let sender = self.sender.as_ref().context("writer closed")?;
        let registered = market_epochs.iter().map(|(market, epoch)|
            Ok((market.clone(), epoch.clone(), self.market_persistence.is_registered(market)?)))
            .collect::<Result<Vec<_>>>()?;
        let mut fenced: Vec<(String, String)> = Vec::new();
        for (market, epoch, has_lane) in &registered {
            if *has_lane {
                if let Err(error) = self.market_persistence.begin_drain(market, epoch) {
                    for (market, epoch) in fenced {
                        let _ = self.market_persistence.cancel_unstarted_drain(&market, &epoch);
                    }
                    return Err(error);
                }
                fenced.push((market.clone(), epoch.clone()));
            }
        }
        // The writer may finish immediately after send. Its debit must see
        // the reservation even when a retirement has no replay lane.
        let old_queued = m.queued;
        let old_queued_bytes = m.queued_bytes;
        m.queued = queued;
        m.queued_bytes = queued_bytes;
        let sent = sender.send(QueuedWork::Retire(Retirement {
                ticket,
                after: m.cursor,
                markets: markets.clone(),
                market_epochs,
                live_only_markets,
                cold_records,
                catalog_bindings: self.pending_bindings.clone(),
                bytes,
            }));
        if let Err(error) = sent {
            m.queued = old_queued;
            m.queued_bytes = old_queued_bytes;
            for (market, epoch) in fenced {
                let _ = self.market_persistence.cancel_unstarted_drain(&market, &epoch);
            }
            return Err(error.into());
        }
        self.pending_bindings.clear();
        m.retirement_submitted = ticket;
        m.retiring_markets.extend(markets.iter().cloned());
        // The selected persistence lanes are already draining. An old actor
        // handle must not be reused if this market is admitted again with a
        // new stream epoch after the durable retirement receipt.
        for market in &markets {
            owners.remove(market);
        }
        drop(owners);
        for key in removed_keys {
            m.state_sizes.remove(&key);
        }
        for token in &tokens {
            m.books.remove(token);
            m.book_cursors.remove(token);
            m.market_book_cursors.remove(token);
            m.book_exchanges.remove(token);
            m.confirmations.remove(token);
            m.confirmation_facts.remove(token);
            m.market_confirmation_versions.remove(token);
            m.recoveries.remove(token);
            m.token_market_ids.remove(token);
            self.tokens.remove(token);
        }
        m.binding_token_ids = self.tokens.keys().cloned().collect();
        for market in &markets {
            m.markets.remove(market);
            m.terminal_markets.remove(market);
            self.market_replay.clear_state_after_retirement_enqueue(market);
        }
        if let Some(gaps) = m.base.as_mut().and_then(|base| base["gaps"].as_array_mut()) {
            gaps.retain(|gap| {
                !gap["token_id"].as_str().is_some_and(|t| tokens.contains(t))
                    && !gap["market_id"]
                        .as_str()
                        .is_some_and(|id| markets.contains(id))
            });
        }
        m.state_bytes = state_bytes;
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
        Ok(())
    }
    /// Admission is an in-memory control operation, not scope activation or
    /// proof of durable metadata. The owner supplies catalog-verified records;
    /// its persisted active plan must reconstruct them on restart. New books
    /// remain missing until authoritative source events arrive. Existing
    /// market/lifecycle records and confirmations are never overwritten here.
    pub fn admit_catalog_markets(
        &mut self,
        catalog_revision: &str,
        records: Vec<Value>,
        evidence_sha256: &str,
    ) -> Result<()> {
        let ids = records
            .iter()
            .map(|record| {
                record["identity"]["market_id"]
                    .as_str()
                    .map(str::to_owned)
                    .context("catalog admission market id")
            })
            .collect::<Result<std::collections::BTreeSet<_>>>()?;
        self.admit_catalog_markets_scoped(catalog_revision, records, evidence_sha256, &ids, 2048)
    }
    pub fn admit_catalog_markets_scoped(
        &mut self,
        catalog_revision: &str,
        records: Vec<Value>,
        evidence_sha256: &str,
        acquisition: &std::collections::BTreeSet<String>,
        maximum_tokens: usize,
    ) -> Result<()> {
        self.admit_catalog_markets_inner(
            catalog_revision,
            records,
            evidence_sha256,
            acquisition,
            maximum_tokens,
            false,
        )
    }
    pub fn check_catalog_admission(
        &mut self,
        catalog_revision: &str,
        records: Vec<Value>,
        evidence_sha256: &str,
        acquisition: &std::collections::BTreeSet<String>,
        maximum_tokens: usize,
    ) -> Result<()> {
        self.admit_catalog_markets_inner(
            catalog_revision,
            records,
            evidence_sha256,
            acquisition,
            maximum_tokens,
            true,
        )
    }
    fn admit_catalog_markets_inner(
        &mut self,
        catalog_revision: &str,
        records: Vec<Value>,
        evidence_sha256: &str,
        acquisition: &std::collections::BTreeSet<String>,
        maximum_tokens: usize,
        check_only: bool,
    ) -> Result<()> {
        ensure!(
            !records.is_empty() && records.len() <= 4096,
            "catalog admission count"
        );
        ensure!(
            (2..=marketcow_runtime::discovery_source::MAX_SOURCE_TOKEN_IDENTITIES)
                .contains(&maximum_tokens),
            "explicit acquisition token limit"
        );
        ensure!(
            canonical_hash(&json!(records)) == evidence_sha256,
            "catalog admission evidence hash"
        );
        let mut m = self.memory.lock().unwrap();
        ensure!(
            m.error.is_none() && self.sender.is_some(),
            "publication unavailable"
        );
        ensure!(
            m.base
                .as_ref()
                .is_some_and(|b| b["catalog_revision"] == catalog_revision),
            "catalog admission revision"
        );
        let mut additions = BTreeMap::new();
        let mut metadata_owners = BTreeMap::new();
        for (market, record) in &m.markets {
            for outcome in record["identity"]["outcomes"]
                .as_array()
                .context("existing metadata outcomes")?
            {
                let token = outcome["token_id"]
                    .as_str()
                    .context("existing metadata token")?;
                if let Some(previous) = metadata_owners.insert(token, market.as_str()) {
                    ensure!(previous == market, "ambiguous existing metadata ownership");
                }
            }
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut sizes = Vec::new();
        let mut state_bytes = m.state_bytes;
        for record in &records {
            let identity = &record["identity"];
            let market = identity["market_id"]
                .as_str()
                .filter(|s| !s.is_empty())
                .context("admitted market identity")?;
            ensure!(ids.insert(market), "duplicate admitted market");
            ensure!(!m.retiring_markets.contains(market), "market retirement pending");
            ensure!(
                identity["condition_id"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty()),
                "admitted condition"
            );
            let outcomes = identity["outcomes"]
                .as_array()
                .context("admitted outcomes")?;
            ensure!(outcomes.len() == 2, "admitted binary market");
            let mut pair = std::collections::BTreeSet::new();
            for outcome in outcomes {
                let token = outcome["token_id"]
                    .as_str()
                    .filter(|s| !s.is_empty() && s.len() <= 128)
                    .context("admitted token")?;
                ensure!(pair.insert(token), "duplicate admitted token");
                ensure!(
                    self.tokens.get(token).is_none_or(|id| id == market),
                    "admitted token ownership changed"
                );
                ensure!(
                    metadata_owners.get(token).is_none_or(|id| *id == market),
                    "admitted token conflicts with dependency metadata"
                );
                if let Some(id) = additions.insert(token.to_owned(), market.to_owned()) {
                    ensure!(id == market, "duplicate admitted token owner");
                }
            }
            if let Some(existing) = m.markets.get(market) {
                ensure!(
                    existing["identity"] == *identity,
                    "admitted market identity changed"
                );
                // Keep current typed lifecycle and metadata intact.
            } else {
                let size = serde_json::to_vec(record)?.len();
                state_bytes = state_bytes
                    .checked_add(size)
                    .context("admission size overflow")?;
                sizes.push((market.to_owned(), size, Arc::new(record.clone())));
            }
        }
        ensure!(
            acquisition.iter().all(|id| ids.contains(id.as_str())),
            "acquisition lacks submitted metadata"
        );
        additions.retain(|token, market| {
            acquisition.contains(market) && !self.tokens.contains_key(token)
        });
        // A token whose book has been fenced can still occupy a draining
        // wire slot. Count it until the owner observes the unsubscribe/exit
        // receipt; otherwise a fast prepare can overbook the global budget.
        let mut reserved = self.tokens.keys().cloned().collect::<BTreeSet<_>>();
        if let Some(draining) = &m.draining_token_ids {
            reserved.extend(draining.iter().cloned());
        }
        reserved.extend(additions.keys().cloned());
        ensure!(reserved.len() <= maximum_tokens, "admitted token capacity");
        ensure!(
            state_bytes <= self.max_bytes.min(MAX_NON_QUEUE_STATE_BYTES),
            "admitted metadata byte capacity"
        );
        let mut pending = self.pending_bindings.clone();
        pending.extend(additions.clone());
        ensure!(
            serde_json::to_vec(&pending)?.len() <= self.batch_cap,
            "admitted binding byte capacity"
        );
        if check_only {
            return Ok(());
        }
        // All validation/capacity checks precede mutation.
        for (market, size, record) in sizes {
            m.state_sizes.insert((true, market.clone()), size);
            m.markets.insert(market.clone(), record);
        }
        m.state_bytes = state_bytes;
        self.tokens.extend(additions);
        m.binding_token_ids = self.tokens.keys().cloned().collect();
        m.token_market_ids = self.tokens.clone();
        self.pending_bindings = pending;
        // Registration is a control-plane operation. Hot observations use
        // their selected lane, not this catalog/metadata lock.
        for market in acquisition {
            let epoch = m.market_stream_epochs.entry(market.clone())
                .or_insert_with(|| uuid::Uuid::new_v4().simple().to_string()).clone();
            self.market_replay.ensure_registered(market, &epoch)?;
            self.market_persistence.register(market, &epoch,
                market_persistence_lane_limits(self.max_batches, self.max_bytes, self.batch_cap))?;
            let record = m.markets.get(market).cloned()
                .context("acquired market metadata missing")?;
            let tokens = self.tokens.iter().filter(|(_, owner)| *owner == market)
                .map(|(token, _)| token.clone()).collect();
            self.market_replay.update_state(market, |state| {
                if state.is_none() {
                    *state = Some(MarketReadState::bootstrap(record.clone(), tokens));
                } else if let Some(current) = state.as_mut() {
                    current.market = Some(record.clone());
                }
            })?;
            let bindings = self.tokens.iter().filter(|(_, owner)| *owner == market)
                .map(|(token, owner)| (token.clone(), owner.clone())).collect();
            self.market_publishers.write()
                .map_err(|_| anyhow::anyhow!("market owner registry poisoned"))?
                .entry(market.clone()).or_insert_with(|| Arc::new(MarketPublisher {
                    market_id: market.clone(), gate: Mutex::new(()),
                    replay: self.market_replay.clone(),
                    persistence: self.market_persistence.clone(),
                    health: self.market_health.clone(), catalog_bindings: bindings,
                    batch_cap: self.batch_cap,
                    confirmation_byte_limit: self.max_bytes.min(MAX_NON_QUEUE_STATE_BYTES),
                    volatile: AtomicBool::new(false), volatile_groups: AtomicU64::new(0),
                }));
        }
        Ok(())
    }
    pub fn set_source_ready(&self, ready: bool) {
        if !ready { self.market_health.source_ready.store(false, Ordering::Release); }
        self.memory.lock().unwrap().source_ready = ready;
        if ready {
            self.market_health.accepting_events.store(true, Ordering::Release);
            self.market_health.source_ready.store(true, Ordering::Release);
        }
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
        let _ = self.market_replay.notify_all();
    }
    /// A valid lease may admit the first authoritative books before the
    /// public source-ready fence is opened. Public readers remain unavailable
    /// until fresh native market state has actually been published.
    pub fn resume_acquisition(&self) {
        self.market_health.accepting_events.store(true, Ordering::Release);
    }
    pub fn is_source_ready(&self) -> bool {
        self.memory.lock().unwrap().source_ready
    }
    pub fn fresh_acquisition_started(&self, tokens: &std::collections::BTreeSet<String>) -> bool {
        if self.market_health.persistence_failed.load(Ordering::Acquire) || tokens.is_empty() {
            return false;
        }
        tokens.iter().any(|token| self.tokens.get(token).is_some_and(|market| {
            self.market_replay.inspect_lane(market, |state, _| {
                let Some(state) = state.as_ref() else { return Ok(false); };
                Ok(state.native_cursor_domain && state.books.contains_key(token)
                    && !state.recoveries.contains_key(token)
                    && !state.unresolved_seed_gaps.contains(token))
            }).unwrap_or(false)
        }))
    }
    /// Intentionally stop acquisition without manufacturing an upstream gap.
    /// Existing consumers cannot resume across this boundary; the next lease
    /// must establish fresh books and obtain a new full-sync.
    pub fn pause_source(&self) -> Result<()> {
        self.market_health.accepting_events.store(false, Ordering::Release);
        self.market_health.source_ready.store(false, Ordering::Release);
        let mut memory = self
            .memory
            .lock()
            .map_err(|_| anyhow::anyhow!("publication poisoned"))?;
        memory.source_ready = false;
        memory.resume_floor = memory
            .cursor
            .checked_add(1)
            .context("resume floor overflow")?;
        memory.replay.clear();
        memory.replay_bytes = 0;
        memory.snapshot_leases.clear();
        drop(memory);
        self.market_replay.invalidate_all_with(|state| {
            if let Some(previous) = state.take() {
                if previous.native_cursor_domain {
                    let mut fresh = MarketReadState {
                        last_event_cursor: previous.last_event_cursor,
                        token_ids: previous.token_ids,
                        native_cursor_domain: true,
                        market: previous.market,
                        ..MarketReadState::default()
                    };
                    fresh.unresolved_seed_gaps = fresh.token_ids.clone();
                    *state = Some(fresh);
                }
            }
        })?;
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
        Ok(())
    }
    pub fn set_acquisition_statistics(&self, statistics: (usize, usize, usize)) {
        let mut memory = self.memory.lock().unwrap();
        memory.acquisition_statistics = Some(statistics);
        memory.acquisition_detail = None;
        memory.wire_token_ids = None;
        memory.draining_token_ids = None;
    }
    pub fn set_acquisition_metrics(&self, statistics: (usize, usize, usize), detail: Value,
        wire_tokens: BTreeSet<String>, draining_tokens: BTreeSet<String>) {
        let mut memory = self.memory.lock().unwrap();
        memory.acquisition_statistics = Some(statistics);
        memory.acquisition_detail = Some(detail);
        if statistics.0 == wire_tokens.len() && draining_tokens.is_subset(&wire_tokens) {
            memory.wire_token_ids = Some(wire_tokens);
            memory.draining_token_ids = Some(draining_tokens);
        } else {
            memory.wire_token_ids = None;
            memory.draining_token_ids = None;
        }
    }
    pub fn persisted_cursor(&self) -> u64 {
        self.memory.lock().unwrap().persisted
    }
    /// Accept freshness evidence from a new, validated WS observation whose
    /// fully reduced depth is byte-for-byte equivalent to the already public
    /// token state. This is deliberately memory-only: no causal state changed,
    /// so there is no event cursor or durable record to manufacture. Exact raw
    /// replays are rejected by the reducer before reaching this method.
    pub fn confirm_reduced_token(
        &self, market: &str, condition: &str, token: &str, state_checksum: &str,
        exchange_at: DateTime<Utc>, received_at: DateTime<Utc>, source_hash: &str,
    ) -> Result<bool> {
        self.market_publisher(market)?.confirm_reduced_token(
            market, condition, token, state_checksum, exchange_at, received_at, source_hash)
    }
}

impl MarketPublisher {
    pub(super) fn confirm_reduced_token(
        &self,
        market: &str,
        condition: &str,
        token: &str,
        state_checksum: &str,
        exchange_at: chrono::DateTime<chrono::Utc>,
        received_at: chrono::DateTime<chrono::Utc>,
        source_hash: &str,
    ) -> Result<bool> {
        let received_at = chrono::DateTime::from_timestamp_micros(received_at.timestamp_micros())
            .context("reduced confirmation receipt timestamp")?;
        let _exchange_at = chrono::DateTime::from_timestamp_micros(exchange_at.timestamp_micros())
            .context("reduced confirmation exchange timestamp")?;
        ensure!(
            market == self.market_id
                && self.catalog_bindings.get(token).is_some_and(|owner| owner == market),
            "reduced confirmation market differs"
        );
        self.replay.transition_state(market, |state| {
            let current_state = state.as_ref().context("market projection unavailable")?;
            ensure!(current_state.native_cursor_domain, "confirmation cursor domain differs");
            ensure!(!current_state.recoveries.contains_key(token)
                && !current_state.unresolved_seed_gaps.contains(token),
                "confirmation during market recovery");
            let current = current_state.books.get(token)
                .context("reduced confirmation book unavailable")?;
            ensure!(current["token_id"] == token && current["condition_id"] == condition,
                "reduced confirmation book identity differs");
            ensure!(current["state_checksum"] == state_checksum,
                "reduced confirmation state differs");
            let mut freshness_fence = chrono::DateTime::parse_from_rfc3339(
                current["received_at"].as_str().context("reduced confirmation book time")?)?
                .with_timezone(&Utc);
            if let Some(fact) = current_state.confirmation_facts.get(token) {
                freshness_fence = freshness_fence.max(DateTime::parse_from_rfc3339(
                    fact["confirmed_at"].as_str().context("previous confirmation time")?)?
                    .with_timezone(&Utc));
            }
            if received_at - freshness_fence < chrono::TimeDelta::milliseconds(
                WS_STATE_CONFIRMATION_MIN_INTERVAL_MS) {
                return Ok((None, false));
            }
            let fact = Arc::new(json!({
                "token_id":current["token_id"],"condition_id":current["condition_id"],
                "book_epoch":current["book_epoch"],"sequence":current["sequence"],
                "state_checksum":current["state_checksum"],
                "book_received_at":current["received_at"],
                "confirmed_at":received_at.to_rfc3339_opts(SecondsFormat::Micros, true),
                "confirmation_source":"polymarket_websocket_state_confirmation",
                "confirmation_evidence_sha256":source_hash,
            }));
            let mut next = current_state.clone();
            next.confirmation_sequence = next.confirmation_sequence.checked_add(1)
                .context("market confirmation sequence overflow")?;
            next.market_confirmation_versions.insert(token.to_owned(), next.confirmation_sequence);
            next.confirmation_facts.insert(token.to_owned(), fact);
            ensure!(serde_json::to_vec(&next.confirmation_facts)?.len() <= self.confirmation_byte_limit,
                "confirmation state byte budget exhausted");
            Ok((Some(Some(next)), true))
        })
    }
}

impl Publication {
    /// Confirm a complete market pair from an independent authoritative REST
    /// observation. Matching content refreshes only the hot projection and is
    /// broadcast without consuming a causal event cursor or touching disk.
    /// A concurrent newer WS update fences a stale REST response.
    pub fn confirm_market(
        &self,
        market: &str,
        condition: &str,
        tokens: &[String; 2],
        raw_books: &[Value],
        request_started_at: chrono::DateTime<chrono::Utc>,
        received_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<ConfirmationOutcome> {
        self.confirm_market_with_policy(
            market,
            condition,
            tokens,
            raw_books,
            request_started_at,
            received_at,
            true,
        )
    }

    /// Confirm a complete pair obtained from a fresh, bounded official WS
    /// subscription. Unlike REST, the first full WS book is an authoritative
    /// current baseline even when its venue timestamp predates the main
    /// stream's latest mutation. The local receipt fence still prevents a
    /// snapshot from replacing a concurrent main-stream update.
    pub fn confirm_targeted_market(
        &self,
        market: &str,
        condition: &str,
        tokens: &[String; 2],
        raw_books: &[Value],
        request_started_at: chrono::DateTime<chrono::Utc>,
        received_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<ConfirmationOutcome> {
        self.confirm_market_with_policy(
            market,
            condition,
            tokens,
            raw_books,
            request_started_at,
            received_at,
            false,
        )
    }

    fn confirm_market_with_policy(
        &self,
        market: &str,
        condition: &str,
        tokens: &[String; 2],
        raw_books: &[Value],
        request_started_at: chrono::DateTime<chrono::Utc>,
        received_at: chrono::DateTime<chrono::Utc>,
        require_monotonic_exchange: bool,
    ) -> Result<ConfirmationOutcome> {
        ensure!(
            raw_books.len() == 2,
            "confirmation requires complete market pair"
        );
        // Transport receipt timestamps use the platform clock's native
        // precision. The durable wire contract is microsecond-precise, so
        // canonicalize before using this as an explicit snapshot boundary.
        let received_at = chrono::DateTime::from_timestamp_micros(received_at.timestamp_micros())
            .context("confirmation receipt timestamp")?;
        let mut candidates = BTreeMap::new();
        let mut evidence_hashes = BTreeMap::new();
        for raw in raw_books {
            let token = raw["asset_id"].as_str().context("confirmation token")?;
            evidence_hashes.insert(token.to_owned(), canonical_hash(raw));
            ensure!(
                tokens.iter().any(|expected| expected == token),
                "unrequested confirmation token"
            );
            let candidate = prepare_snapshot(
                raw,
                SnapshotBoundary {
                    market_id: market,
                    condition_id: condition,
                    token_id: token,
                    recovery_id: "rest-freshness-confirmation",
                    cursor: 1,
                    received_at,
                },
            )?
            .finalize(1)?["canonical_payload"]
                .clone();
            ensure!(
                candidates.insert(token.to_owned(), candidate).is_none(),
                "duplicate confirmation token"
            );
        }
        ensure!(
            tokens.iter().all(|token| candidates.contains_key(token)),
            "confirmation pair incomplete"
        );
        self.market_replay.transition_state(market, |state| {
        let incumbent = state.as_ref().context("market confirmation projection unavailable")?;
        ensure!(incumbent.native_cursor_domain, "market confirmation cursor domain differs");
        let mut confirmed = Vec::new();
        let mut superseded = 0usize;
        for token in tokens {
            ensure!(incumbent.token_ids.contains(token)
                && !incumbent.recoveries.contains_key(token)
                && !incumbent.unresolved_seed_gaps.contains(token),
                "confirmation token outside ready market");
            let current = incumbent
                .books
                .get(token)
                .context("confirmation book unavailable")?;
            let current_received = chrono::DateTime::parse_from_rfc3339(
                current["received_at"]
                    .as_str()
                    .context("book received_at")?,
            )?
            .with_timezone(&chrono::Utc);
            // An identical REST/targeted-WS response may finish decoding after
            // a newer WS book or confirmation was already installed. Matching
            // price/size alone does not make its older receipt a fresh proof.
            // Never move either the original-book or confirmation time back.
            let mut freshness_fence = current_received;
            if let Some(fact) = incumbent.confirmation_facts.get(token) {
                if let Some(value) = fact["confirmed_at"].as_str() {
                    freshness_fence = freshness_fence.max(
                        chrono::DateTime::parse_from_rfc3339(value)?.with_timezone(&chrono::Utc),
                    );
                }
            }
            if received_at <= freshness_fence {
                superseded += 1;
                continue;
            }
            let candidate = &candidates[token];
            let current_book_exchange = incumbent
                .book_exchanges
                .get(token)
                .context("book exchange fence unavailable")?;
            let candidate_exchange = chrono::DateTime::parse_from_rfc3339(
                candidate["exchange_at"]
                    .as_str()
                    .context("confirmation exchange_at")?,
            )?
            .with_timezone(&chrono::Utc);
            // REST book confirmation proves the freshness of the price/size
            // book.  last_trade_price is a separate WS event stream and the
            // venue's REST snapshot can legitimately lag it while carrying an
            // identical current book.  Do not turn that independent lag into
            // a book mismatch or let it prevent a quiet book from being
            // freshness-confirmed; preserve the causally newer WS trade value.
            let book_differs = current["state_checksum"] != candidate["state_checksum"]
                || current["tick_size"] != candidate["tick_size"];
            if book_differs {
                if (require_monotonic_exchange && current_book_exchange > &candidate_exchange)
                    || freshness_fence > request_started_at
                {
                    // This token advanced while the pair request was in flight.
                    // Preserve its newer WS state, but continue: a quiet sibling
                    // whose content exactly matches the same authoritative REST
                    // response can still be freshness-confirmed independently.
                    superseded += 1;
                    continue;
                }
                // A non-racing divergence still requires an atomic two-token
                // replacement. Do not partially apply confirmations collected
                // earlier in this loop.
                return Ok((None, ConfirmationOutcome::Mismatch));
            }
            let mut next = current.as_ref().clone();
            if next["book_received_at"].is_null() {
                next["book_received_at"] = current["received_at"].clone();
            }
            next["confirmed_at"] = candidate["received_at"].clone();
            next["confirmation_source"] = json!(if require_monotonic_exchange {
                "polymarket_rest"
            } else {
                "polymarket_websocket"
            });
            next["confirmation_evidence_sha256"] = json!(evidence_hashes[token]);
            if current["last_trade_price"] == candidate["last_trade_price"] {
                next["exchange_at"] = candidate["exchange_at"].clone();
                next["source_hash"] = candidate["source_hash"].clone();
            }
            confirmed.push((token.clone(), next));
        }
        if confirmed.is_empty() {
            ensure!(
                superseded == tokens.len(),
                "confirmation outcome unavailable"
            );
            return Ok((None, ConfirmationOutcome::Superseded));
        }
        let mut next_state = incumbent.clone();
        for (token, book) in confirmed {
            let fact = json!({
                "token_id":book["token_id"],
                "condition_id":book["condition_id"],
                "book_epoch":book["book_epoch"],
                "sequence":book["sequence"],
                "state_checksum":book["state_checksum"],
                "book_received_at":book["received_at"],
                "confirmed_at":book["confirmed_at"],
                "confirmation_source":book["confirmation_source"],
                "confirmation_evidence_sha256":book["confirmation_evidence_sha256"]
            });
            let market_sequence = next_state.confirmation_sequence.checked_add(1)
                .context("market confirmation sequence overflow")?;
            next_state.confirmation_sequence = market_sequence;
            next_state.market_confirmation_versions.insert(token.clone(), market_sequence);
            next_state.confirmation_facts
                .insert(token.clone(), Arc::new(fact));
        }
        ensure!(serde_json::to_vec(&next_state.confirmation_facts)?.len()
            <= self.max_bytes.min(MAX_NON_QUEUE_STATE_BYTES),
            "confirmation state byte budget exhausted");
        Ok((Some(Some(next_state)), ConfirmationOutcome::Confirmed))
        })
    }
    /// Only capacity exhaustion waits. The normal memory publication path never
    /// waits for durability. One bounded incoming batch is retained while the
    /// transport's bounded channel propagates pressure upstream.
    pub async fn publish_with_capacity(&mut self, events: Vec<Value>) -> Result<()> {
        self.publish_with_capacity_and_materialized(events, BTreeMap::new(), None)
            .await
    }

    /// Terminal lifecycle evidence uses the same bounded asynchronous durability
    /// path as books. The evidence is committed atomically with its terminal event;
    /// normal memory publication still precedes disk I/O.
    pub async fn publish_with_capacity_and_evidence(
        &mut self,
        events: Vec<Value>,
        evidence: Option<Value>,
    ) -> Result<()> {
        self.publish_with_capacity_and_materialized(events, BTreeMap::new(), evidence)
            .await
    }

    pub async fn publish_with_capacity_and_materialized(
        &mut self,
        events: Vec<Value>,
        materialized_books: BTreeMap<u64, Value>,
        evidence: Option<Value>,
    ) -> Result<()> {
        self.publish_with_capacity_and_materialized_timed(
            events, materialized_books, evidence, None,
        ).await
    }

    pub async fn publish_with_capacity_and_materialized_timed(
        &mut self,
        events: Vec<Value>,
        materialized_books: BTreeMap<u64, Value>,
        evidence: Option<Value>,
        timing: Option<SourceBatchTiming>,
    ) -> Result<()> {
        self.publish_with_capacity_and_materialized_prepared(
            events, materialized_books, evidence, timing, None,
        ).await
    }

    pub(super) async fn publish_with_capacity_and_materialized_prepared(
        &mut self,
        events: Vec<Value>,
        materialized_books: BTreeMap<u64, Value>,
        evidence: Option<Value>,
        timing: Option<SourceBatchTiming>,
        prepared: Option<PreparedProjection>,
    ) -> Result<()> {
        let validation_started = std::time::Instant::now();
        let terminal = events
            .first()
            .is_some_and(|e| e["event_type"] == "market_terminal");
        let validated = ValidatedSourceBatch::new(
            events,
            terminal,
            self.cursor()?,
            &self.tokens,
            self.batch_cap,
        )?;
        self.hot_timing.validation.record(validation_started.elapsed());
        self.publish_actor_validated(validated, materialized_books, evidence, timing, prepared).await
    }

    pub(super) async fn publish_actor_validated(
        &mut self,
        validated: ValidatedSourceBatch,
        materialized_books: BTreeMap<u64, Value>,
        evidence: Option<Value>,
        timing: Option<SourceBatchTiming>,
        prepared: Option<PreparedProjection>,
    ) -> Result<()> {
        ensure!(validated.after_cursor() == self.cursor()?,
            "actor batch publisher cursor changed");
        let events = validated.events();
        let market = events.first().and_then(|event| event["market_id"].as_str())
            .context("actor batch market identity")?;
        ensure!(events.iter().all(|event| event["market_id"] == market),
            "actor batch crosses market boundary");
        for event in events {
            if let Some(token) = event["token_id"].as_str() {
                ensure!(self.tokens.get(token).is_some_and(|owner| owner == market),
                    "actor token binding changed before publication");
            } else if event["event_type"] == "market_terminal" {
                for token in event["canonical_payload"]["retired_token_ids"]
                    .as_array().context("terminal retired tokens")? {
                    let token = token.as_str().context("terminal retired token identity")?;
                    ensure!(self.tokens.get(token).is_some_and(|owner| owner == market),
                        "actor terminal binding changed before publication");
                }
            }
        }
        let bytes = self.batch_bytes(&validated, evidence.as_ref())?;
        ensure!(bytes <= self.batch_cap, "batch byte limit");
        self.hot_timing.validated_events = self.hot_timing.validated_events
            .saturating_add(validated.events().len() as u64);
        self.hot_timing.validated_bytes = self.hot_timing.validated_bytes
            .saturating_add(bytes as u64);
        let mut changed = self.changed.subscribe();
        let started = std::time::Instant::now();
        let mut waited = false;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                {
                    let m = self.memory.lock().unwrap();
                    ensure!(m.error.is_none(), "persistence failed: {:?}", m.error);
                    if m.queued < self.max_batches && m.queued_bytes + bytes <= self.max_bytes {
                        break;
                    }
                }
                waited = true;
                changed
                    .changed()
                    .await
                    .context("durability worker disconnected")?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("persistence capacity exhausted for 10 seconds")??;
        if waited {
            self.hot_timing.capacity_wait.record(started.elapsed());
            marketcow_polymarket::async_eprintln!(
                "{}",
                json!({"stage":"persistence_capacity_wait","elapsed_us":started.elapsed().as_micros(),"batch_bytes":bytes})
            );
        }
        self.publish_validated(validated, materialized_books, evidence, bytes, timing, prepared)
    }
}

impl MarketPublisher {
    /// The live market path binds actor-local event positions to this one
    /// market's event domain. Archive positions are assigned later by the
    /// fair disk worker and never rewrite the public bytes or group hash.
    /// Neither projection nor writer admission takes the shared Memory lock.
    pub(super) fn publish(
        &self,
        validated: ValidatedSourceBatch,
        materialized_books: BTreeMap<u64, Value>,
        mut prepared: PreparedProjection,
        evidence: Option<Value>,
        timing: Option<SourceBatchTiming>,
    ) -> Result<Arc<Batch>> {
        let actor_events = validated.events();
        let market = actor_events.first().and_then(|event| event["market_id"].as_str())
            .context("native actor market identity")?.to_owned();
        ensure!(actor_events.iter().all(|event| event["market_id"] == market),
            "native actor batch crosses markets");
        ensure!(market == self.market_id, "market owner identity differs");
        let _owner = self.gate.lock().map_err(|_| anyhow::anyhow!("market owner poisoned"))?;
        ensure!(self.health.accepting_events.load(Ordering::Acquire), "source acquisition paused");
        let (stream_epoch, next_group, event_after) = self.replay.inspect_lane(
            &market, |state, watermark| {
                let state = state.as_ref().context("market owner projection absent")?;
                ensure!(state.native_cursor_domain, "market owner cursor domain differs");
                Ok((watermark.stream_epoch.clone(),
                    watermark.published_cursor.checked_add(1).context("market group cursor overflow")?,
                    state.last_event_cursor))
            })?;
        let old_after = validated.after_cursor();
        let validated = validated.rebase_after(event_after, self.batch_cap)?;
        let materialized_books = materialized_books.into_iter().map(|(cursor, book)| {
            Ok((event_after.checked_add(cursor.checked_sub(old_after)
                .context("materialized cursor before actor boundary")?)
                .context("market materialized cursor overflow")?, book))
        }).collect::<Result<BTreeMap<_, _>>>()?;
        prepared.rebase_from(old_after, event_after)?;
        validate_materialized_books(validated.events(), &materialized_books)?;
        let validated = validated.with_native_market_group(&stream_epoch, next_group)?;
        let catalog_bindings = self.catalog_bindings.clone();
        let binding_bytes = serde_json::to_vec(&catalog_bindings)?.len();
        let evidence_bytes = evidence.as_ref().map(serde_json::to_vec)
            .transpose()?.map_or(0, |data| data.len());
        let materialized_bytes = materialized_declared_bytes(validated.events())?;
        let bytes = validated.encoded_bytes()
            .checked_add(materialized_bytes)
            .and_then(|size| size.checked_add(binding_bytes))
            .and_then(|size| size.checked_add(evidence_bytes))
            .context("native group byte overflow")?;
        ensure!(bytes <= self.batch_cap, "native group exceeds batch cap");
        let materialized_retained = materialized_books.values().try_fold(0usize,
            |total, book| total.checked_add(
                marketcow_runtime::discovery_source::json_retained_bytes(book)
                    .checked_add(std::mem::size_of::<(u64, Value)>() + 64)
                    .context("native book retained charge overflow")?
            ).context("native materialized retained charge overflow"))?;
        let durable_retained = validated.retained_bytes()
            .checked_add(std::mem::size_of::<Batch>() + 256)
            .and_then(|size| size.checked_add(catalog_bindings.iter().map(|(token, owner)|
                token.capacity() + owner.capacity() + 256).sum::<usize>()))
            .and_then(|size| size.checked_add(evidence.as_ref().map_or(0,
                marketcow_runtime::discovery_source::json_retained_bytes)))
            .context("native durable retained byte overflow")?.max(bytes);
        let retained = durable_retained.checked_add(materialized_retained)
            .context("native retained byte overflow")?;
        let batch = Arc::new(Batch {
            validated, materialized_books: Mutex::new(Some(Arc::new(materialized_books))),
            evidence, catalog_bindings, timing: Mutex::new(timing), bytes,
            replay_retained_bytes: retained,
        });
        let actions = prepared.into_actions();
        let was_volatile = self.volatile.load(Ordering::Acquire);
        self.persistence.publish_with_replay_prepared_charged(
            &self.replay, &market, &stream_epoch, next_group,
            batch.validated.events(), batch.clone(), bytes, retained,
            durable_retained, Some(&self.volatile),
            |state| {
                ensure!(self.health.accepting_events.load(Ordering::Acquire),
                    "source acquisition paused");
                state.as_ref().context("market projection absent")?
                    .prepare_native_group(&market, next_group, batch.validated.events(), actions)
            },
            |state, next| {
                *state = Some(next);
                if let Ok(mut timing) = batch.timing.lock() {
                    if let Some(timing) = timing.as_mut() {
                        timing.shared_replay_visible_at = Some(Utc::now());
                        timing.shared_replay_visible_mono = Some(std::time::Instant::now());
                    }
                }
            },
        )?;
        if self.volatile.load(Ordering::Acquire) {
            // No writer owns this group's post-state. The live projection has
            // already installed its Arc; retaining a second writer-only copy
            // would unnecessarily consume the bounded replay allowance.
            batch.materialized_books.lock()
                .map_err(|_| anyhow::anyhow!("materialized books poisoned"))?.take();
            let count = self.volatile_groups.fetch_add(1, Ordering::AcqRel) + 1;
            if !was_volatile || count % 256 == 0 {
                marketcow_polymarket::async_eprintln!("{}", json!({
                    "stage":"market_durability_gap", "market_id":market,
                    "stream_epoch":stream_epoch,"first_non_durable_cursor":
                        if was_volatile {Value::Null} else {json!(next_group)},
                    "live_only_groups":count,
                    "reason":if self.health.persistence_failed.load(Ordering::Acquire)
                        {"persistence_writer_failed"} else {"persistence_queue_capacity"},
                }));
            }
        }
        Ok(batch)
    }
}

impl Publication {
    pub(super) fn market_publisher(&self, market: &str) -> Result<Arc<MarketPublisher>> {
        self.market_publishers.read()
            .map_err(|_| anyhow::anyhow!("market owner registry poisoned"))?
            .get(market).cloned().context("market owner not admitted")
    }
    pub(super) fn next_market_event_cursor(&self, market: &str) -> Result<u64> {
        self.market_replay.inspect_lane(market, |state, _| {
            let state = state.as_ref().context("market event state missing")?;
            ensure!(state.native_cursor_domain, "market event cursor domain differs");
            state.last_event_cursor.checked_add(1).context("market event cursor overflow")
        })
    }

    /// A selected market whose bounded queue cannot accept even its gap
    /// marker becomes unavailable immediately. No other market is poisoned;
    /// the next authoritative pair must carry the recovery barrier after
    /// writer capacity returns. Existing clients are woken and closed rather
    /// than allowed to keep quoting the old books.
    pub(super) fn fence_market_capacity(&self, market: &str) -> Result<()> {
        ensure!(self.market_replay.is_registered(market)?,
            "market capacity fence has no registered stream");
        self.market_replay.inspect_lane(market, |state, _| {
            ensure!(state.is_some(), "market capacity fence has no projection");
            Ok(())
        })?;
        self.market_replay.update_state(market, |state| {
            if let Some(state) = state.as_mut() {
                state.capacity_fenced = true;
                state.unresolved_seed_gaps = state.token_ids.clone();
                state.confirmation_facts.clear();
                state.market_confirmation_versions.clear();
            }
        })
    }

    pub(super) fn publish_native_events(&self, events: Vec<Value>, evidence: Option<Value>) -> Result<()> {
        let first = events.first().context("empty native source group")?;
        let first_cursor = first["cursor"].as_u64().context("native event cursor")?;
        let after = first_cursor.checked_sub(1).context("native event cursor zero")?;
        let terminal = first["event_type"] == "market_terminal";
        let validated = ValidatedSourceBatch::new_native(
            events, terminal, after, &self.tokens, self.batch_cap)?;
        let prepared = prepare_market_projection(validated.events(), &BTreeMap::new())?;
        let market = validated.events()[0]["market_id"].as_str()
            .context("native event market identity")?;
        self.market_publisher(market)?.publish(
            validated, BTreeMap::new(), prepared, evidence, None)?;
        Ok(())
    }

    pub fn publish(&mut self, events: Vec<Value>, evidence: Option<Value>) -> Result<()> {
        let validation_started = std::time::Instant::now();
        let cursor = self.cursor()?;
        let terminal = events
            .first()
            .is_some_and(|e| e["event_type"] == "market_terminal");
        let validated =
            ValidatedSourceBatch::new(events, terminal, cursor, &self.tokens, self.batch_cap)?;
        let bytes = self.batch_bytes(&validated, evidence.as_ref())?;
        self.hot_timing.validation.record(validation_started.elapsed());
        self.hot_timing.validated_events = self.hot_timing.validated_events
            .saturating_add(validated.events().len() as u64);
        self.hot_timing.validated_bytes = self.hot_timing.validated_bytes
            .saturating_add(bytes as u64);
        self.publish_validated(validated, BTreeMap::new(), evidence, bytes, None, None)
    }
    fn batch_bytes(
        &self,
        validated: &ValidatedSourceBatch,
        evidence: Option<&Value>,
    ) -> Result<usize> {
        validated
            .encoded_bytes()
            .checked_add(if self.pending_bindings.is_empty() {
                0
            } else {
                serde_json::to_vec(&self.pending_bindings)?.len()
            })
            .context("binding byte overflow")?
            .checked_add(
                evidence
                    .map(serde_json::to_vec)
                    .transpose()?
                    .map_or(0, |value| value.len()),
            )
            .context("byte overflow")?
            .checked_add(materialized_declared_bytes(validated.events())?)
            .context("materialized byte overflow")
    }
    fn publish_validated(
        &mut self,
        validated: ValidatedSourceBatch,
        materialized_books: BTreeMap<u64, Value>,
        evidence: Option<Value>,
        bytes: usize,
        timing: Option<SourceBatchTiming>,
        prepared: Option<PreparedProjection>,
    ) -> Result<()> {
        let events = validated.events();
        let terminal = events
            .first()
            .is_some_and(|e| e["event_type"] == "market_terminal");
        ensure!(bytes <= self.batch_cap, "batch byte limit");
        // This only checks the immutable batch and its materialized post-state.
        // Keep it outside the shared publication lock so another market need
        // not wait for a full-depth book comparison.
        let projection_actions = if let Some(prepared) = prepared {
            prepared.into_actions()
        } else {
            validate_materialized_books(events, &materialized_books)?;
            prepare_projection_actions(events, &materialized_books)?
        };
        let materialized_books = Arc::new(materialized_books);
        let lock_started = std::time::Instant::now();
        let mut m = self.memory.lock().unwrap();
        self.hot_timing.memory_lock_wait.record(lock_started.elapsed());
        let held_started = std::time::Instant::now();
        ensure!(m.error.is_none(), "persistence failed: {:?}", m.error);
        if !terminal {
            ensure!(
                events.iter().all(|event| event["market_id"]
                    .as_str()
                    .is_some_and(|market| !m.terminal_markets.contains(market))),
                "event targets terminal market"
            );
        }
        let recovery_transition = terminal
            || events.iter().any(|event| {
                is_token_recovery(event)
                    || (event["event_type"] == "book"
                        && event["token_id"]
                            .as_str()
                            .is_some_and(|token| m.recoveries.contains_key(token)))
            });
        let recoveries = if recovery_transition {
            let mut recoveries = m.recoveries.clone();
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
            ensure!(
                serde_json::to_vec(&recoveries)?.len() <= self.batch_cap,
                "recovery state byte cap"
            );
            Some(recoveries)
        } else {
            None
        };
        // Limits include the batch currently being persisted, not just pending channel entries.
        ensure!(
            m.queued < self.max_batches && bytes <= self.max_bytes - m.queued_bytes,
            "persistence queue capacity exhausted; publication rejected without advancing cursor"
        );
        let mut state_bytes = m.state_bytes;
        let mut size_updates = BTreeMap::new();
        if m.base.is_some() {
            for action in &projection_actions {
                let (cache_key, new_size) = match action {
                    ProjectionAction::Terminal { market_id, state_bytes, .. } =>
                        ((true, market_id.clone()), *state_bytes),
                    ProjectionAction::Book { token_id, state_bytes, .. } =>
                        ((false, token_id.clone()), *state_bytes),
                };
                // An atomic group may update the same token more than once.
                // Account against the preceding update in this group, not
                // repeatedly against the pre-group projection.
                let old = size_updates.get(&cache_key).copied()
                    .or_else(|| m.state_sizes.get(&cache_key).copied()).unwrap_or(0);
                size_updates.insert(cache_key, new_size);
                state_bytes = state_bytes
                    .checked_sub(old)
                    .context("state accounting underflow")?
                    .checked_add(new_size)
                    .context("state bytes overflow")?;
            }
            ensure!(
                state_bytes <= self.max_bytes.min(MAX_NON_QUEUE_STATE_BYTES),
                "memory state byte budget exhausted"
                );
        }
        let market_id = events.first().and_then(|event| event["market_id"].as_str())
            .context("publication market identity")?.to_owned();
        ensure!(events.iter().all(|event| event["market_id"] == market_id),
            "publication batch crosses market boundary");
        let market_cursor = m.market_published_cursors.get(&market_id).copied().unwrap_or(0)
            .checked_add(1).context("market publication cursor overflow")?;
        let market_epoch = m.market_stream_epochs.entry(market_id.clone())
            .or_insert_with(|| uuid::Uuid::new_v4().simple().to_string()).clone();
        let validated = validated.with_market_group(&market_epoch, market_cursor)?;
        let replay_retained_bytes = validated.retained_bytes()
            .checked_add(std::mem::size_of::<Batch>() + 256)
            .and_then(|bytes| bytes.checked_add(evidence.as_ref().map_or(0,
                marketcow_runtime::discovery_source::json_retained_bytes)))
            .and_then(|bytes| bytes.checked_add(self.pending_bindings.iter().map(|(token, market)|
                token.capacity() + market.capacity() + 256).sum::<usize>()))
            .context("replay retained byte overflow")?.max(bytes);
        let batch = Arc::new(Batch {
            validated,
            materialized_books: Mutex::new(Some(materialized_books.clone())),
            evidence,
            catalog_bindings: self.pending_bindings.clone(),
            timing: Mutex::new(timing),
            bytes,
            replay_retained_bytes,
        });
        let sender = self.sender.as_ref().context("writer closed")?.clone();
        let changed = self.changed.clone();
        let replay_max_batches = self.replay_max_batches;
        let replay_max_bytes = self.replay_max_bytes;
        let market_id_for_commit = market_id.clone();
        self.market_replay.ensure_registered(&market_id, &market_epoch)?;
        let market_tokens = self.tokens.iter().filter(|(_, market)| *market == &market_id)
            .map(|(token, _)| token.clone()).collect();
        self.market_replay.publish_with_state_commit(
            &market_id, market_cursor, batch.validated.events(), batch.clone(), bytes, replay_retained_bytes,
            move |group| sender.send(QueuedWork::Events(group.payload.clone()))
                .map_err(Into::into),
            |market_state| {
                m.market_published_cursors.insert(market_id_for_commit, market_cursor);
                for action in projection_actions {
                    match action {
                        ProjectionAction::Terminal { market_id, model, .. } => {
                            m.terminal_markets.insert(market_id.clone());
                            if m.base.is_some() { m.markets.insert(market_id, model); }
                        }
                        ProjectionAction::Book { token_id, physical_cursor, exchange_at,
                            clear_confirmation, model, .. } => {
                            if m.base.is_none() { continue; }
                            m.book_cursors.insert(token_id.clone(), physical_cursor);
                            m.market_book_cursors.insert(token_id.clone(), market_cursor);
                            // Top quotes and trades do not both advance the
                            // venue book-confirmation fence.
                            if let Some(exchange_at) = exchange_at {
                                m.book_exchanges.insert(token_id.clone(), exchange_at);
                            }
                            if clear_confirmation {
                                m.confirmations.remove(&token_id);
                                m.confirmation_facts.remove(&token_id);
                                m.market_confirmation_versions.remove(&token_id);
                            }
                            m.books.insert(token_id, model);
                        }
                    }
                }
                m.state_bytes = state_bytes;
                if let Some(recoveries) = recoveries { m.recoveries = recoveries; }
                m.state_sizes.extend(size_updates);
                m.cursor = batch.validated.events().last().unwrap()["cursor"].as_u64().unwrap();
                m.queued += 1;
                m.queued_bytes += bytes;
                m.replay.push_back(batch.clone());
                m.replay_bytes += replay_retained_bytes;
                m.snapshot_leases.retain(|_, until| *until > std::time::Instant::now());
                while m.replay.len() > replay_max_batches || m.replay_bytes > replay_max_bytes {
                    let pinned = m.snapshot_leases.keys().next().is_some_and(|after| {
                        m.replay.front().and_then(|b| b.validated.events().last())
                            .and_then(|e| e["cursor"].as_u64()).is_some_and(|end| end > *after)
                    });
                    if pinned && m.replay_bytes <= replay_max_bytes { break; }
                    let old = m.replay.pop_front().unwrap();
                    m.replay_bytes -= old.replay_retained_bytes;
                }
                if let Some(timing) = batch.timing.lock().expect("batch timing poisoned").as_mut() {
                    timing.shared_replay_visible_mono = Some(std::time::Instant::now());
                    timing.shared_replay_visible_at = Some(Utc::now());
                }
                *market_state = Some(MarketReadState::project(&m, &market_id, market_tokens));
                changed.send_modify(|n| *n = n.wrapping_add(1));
            },
        )?;
        self.pending_bindings.clear();
        self.hot_timing.memory_lock_held.record(held_started.elapsed());
        Ok(())
    }
    pub async fn finish(mut self) -> Result<()> {
        self.market_persistence.close();
        if let Some(worker) = self.market_worker.take() {
            tokio::task::spawn_blocking(move || worker.join()
                .map_err(|_| anyhow::anyhow!("market disk worker panicked"))).await??;
        }
        self.sender.take();
        let worker = self.worker.take().unwrap();
        tokio::task::spawn_blocking(move || {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("disk worker panicked"))
        })
        .await??;
        let m = self.memory.lock().unwrap();
        ensure!(
            m.error.is_none() && m.persisted == m.cursor,
            "durability drain failed: {:?}",
            m.error
        );
        Ok(())
    }
    pub fn router(&self, frame_cap: usize, clients: usize) -> Result<Router> {
        ensure!(
            self.memory.lock().unwrap().base.is_some()
                && clients > 0
                && clients <= 4
                && frame_cap >= self.batch_cap * 2,
            "explicit stream seed/client/frame bounds required"
        );
        let stream = Arc::new(Stream {
            memory: self.memory.clone(),
            changed: self.changed.clone(),
            frame_cap,
            permits: Arc::new(Semaphore::new(clients)),
        });
        Ok(Router::new().route("/", get(upgrade)).with_state(stream))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmationOutcome {
    Confirmed,
    Superseded,
    Mismatch,
}
struct Stream {
    memory: Arc<Mutex<Memory>>,
    changed: watch::Sender<u64>,
    frame_cap: usize,
    permits: Arc<Semaphore>,
}
impl Stream {
    fn window(&self, after: u64) -> Value {
        let m = self.memory.lock().unwrap();
        json!({"after_cursor":after,"current_cursor":m.cursor,"persisted_cursor":m.persisted,
            "oldest_cursor":m.replay.front().map(|b|b.validated.events()[0]["cursor"].clone()),
            "retained_batches":m.replay.len(),"retained_bytes":m.replay_bytes,
            "persistence_queue_batches":m.queued,"persistence_queue_bytes":m.queued_bytes})
    }
    fn snapshot(&self) -> Result<(Value, u64, u64)> {
        let view = MemoryView::capture(&self.memory.lock().unwrap())?;
        let cursor = view.cursor;
        let confirmation = view.confirmation_sequence;
        Ok((view.into_source_state()?, cursor, confirmation))
    }
    #[cfg(test)]
    fn page(&self, after: u64, confirmation_after: u64) -> Result<(Value, u64, u64)> {
        self.page_with_priority(after, confirmation_after, false)
    }
    fn page_with_priority(
        &self,
        after: u64,
        confirmation_after: u64,
        prefer_events: bool,
    ) -> Result<(Value, u64, u64)> {
        let m = self.memory.lock().unwrap();
        ensure!(m.source_ready, "upstream WebSocket recovery pending");
        ensure!(m.error.is_none(), "persistence failed: {:?}", m.error);
        // A freshness confirmation has no causal cursor of its own, but its
        // book still does. Never deliver a confirmed book before the consumer
        // has applied that book's causal cursor. Keep confirmation order as a
        // prefix so advancing confirmation_after cannot skip a blocked item.
        let mut pending_confirmations: Vec<_> = m
            .confirmations
            .iter()
            .filter(|(_, cursor)| **cursor > confirmation_after)
            .collect();
        pending_confirmations.sort_by_key(|(_, cursor)| **cursor);
        let confirmations: Vec<_> = pending_confirmations
            .into_iter()
            .take_while(|(token, _)| {
                m.book_cursors
                    .get(*token)
                    .is_some_and(|cursor| *cursor <= after)
            })
            .collect();
        if let Some((_, next_confirmation)) = confirmations
            .last()
            .filter(|_| !(prefer_events && after < m.cursor))
        {
            let next_confirmation = **next_confirmation;
            return Ok((
                json!({"schema_version":"marketcow.polymarket.live-stream.v1",
                    "type":"book_confirmations",
                    "persisted_cursor":m.persisted,
                    "persistence_queue_depth":m.queued,
                    "persistence_error":null,"derived_index_error":null,
                    "books":confirmations.into_iter().map(|(token, _)| &m.confirmation_facts[token]).collect::<Vec<_>>() }),
                after,
                next_confirmation,
            ));
        }
        let mut events = Vec::new();
        let mut next = after;
        let mut bytes = 0;
        for batch in &m.replay {
            if batch.validated.events().last().unwrap()["cursor"]
                .as_u64()
                .unwrap()
                <= after
            {
                continue;
            }
            if bytes + batch.bytes > self.frame_cap / 2 {
                break;
            }
            for event in batch.validated.events() {
                let at = event["cursor"].as_u64().context("cursor")?;
                if at <= after {
                    continue;
                }
                ensure!(
                    Some(at) == next.checked_add(1),
                    "memory replay expired; full resync required"
                );
                events.push(event.clone());
                next = at;
            }
            bytes += batch.bytes;
        }
        ensure!(
            after == m.cursor || next > after,
            "memory replay expired; full resync required"
        );
        let mut frame = json!({"schema_version":"marketcow.polymarket.live-stream.v1", "type":"persistence",
            "persisted_cursor":m.persisted,"persistence_queue_depth":m.queued,"persistence_error":null,"derived_index_error":null});
        if !events.is_empty() {
            frame["type"] = json!("events");
            frame["events"] = json!(events);
        }
        Ok((frame, next, confirmation_after))
    }
}
async fn upgrade(
    State(state): State<Arc<Stream>>,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    ws.max_message_size(4096)
        .on_upgrade(move |socket| async move {
            let Ok(_permit) = state.permits.clone().try_acquire_owned() else {
                return;
            };
            if let Err(error) = serve(socket, state).await {
                marketcow_polymarket::async_eprintln!("memory stream closed: {error:#}");
            }
        })
}
async fn send(socket: &mut WebSocket, frame: Value, cap: usize) -> Result<(u128, u128, usize)> {
    let started = std::time::Instant::now();
    let body = serde_json::to_string(&frame)?;
    let encode_us = started.elapsed().as_micros();
    let bytes = body.len();
    ensure!(body.len() <= cap, "frame byte cap");
    let sending = std::time::Instant::now();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        socket.send(Message::Text(body.into())),
    )
    .await??;
    Ok((encode_us, sending.elapsed().as_micros(), bytes))
}
async fn serve(mut socket: WebSocket, stream: Arc<Stream>) -> Result<()> {
    let request = tokio::time::timeout(std::time::Duration::from_secs(5), socket.recv())
        .await?
        .context("subscribe required")??;
    let Message::Text(body) = request else {
        anyhow::bail!("subscribe required")
    };
    ensure!(
        serde_json::from_str::<Value>(&body)?["type"] == "subscribe",
        "subscribe required"
    );
    let mut changed = stream.changed.subscribe();
    let stream_id = format!(
        "{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let mut last_report = std::time::Instant::now();
    let mut frames = 0u64;
    let mut stage_us = [0u128; 3];
    let mut stage_max_us = [0u128; 3];
    let mut sent_bytes = 0usize;
    // Per-consumer alternation: a confirmation frame cannot starve queued events.
    let mut prefer_events = false;
    let (state, mut cursor, mut confirmation_cursor) = stream.snapshot()?;
    send(&mut socket, state, stream.frame_cap).await?;
    send(&mut socket,json!({"schema_version":"marketcow.polymarket.live-stream.v1","type":"ready","latest_cursor":cursor}),stream.frame_cap).await?;
    loop {
        let building = std::time::Instant::now();
        let (frame, next, next_confirmation) = match stream.page_with_priority(
            cursor,
            confirmation_cursor,
            prefer_events,
        ) {
            Ok(page) => page,
            Err(error) => {
                marketcow_polymarket::async_eprintln!(
                    "{}",
                    json!({"stage":"source_stream_error","at":chrono::Utc::now(),
                    "stream_id":stream_id,"window":stream.window(cursor),"error":format!("{error:#}")})
                );
                return Err(error);
            }
        };
        prefer_events = frame["type"] == "book_confirmations";
        let build_us = building.elapsed().as_micros();
        let (encode_us, send_us, bytes) = send(&mut socket, frame, stream.frame_cap).await?;
        frames += 1;
        sent_bytes += bytes;
        for (i, value) in [build_us, encode_us, send_us].into_iter().enumerate() {
            stage_us[i] += value;
            stage_max_us[i] = stage_max_us[i].max(value);
        }
        if last_report.elapsed() >= std::time::Duration::from_secs(5) {
            marketcow_polymarket::async_eprintln!(
                "{}",
                json!({"stage":"source_stream_stages","at":chrono::Utc::now(),
                "stream_id":stream_id,"window":stream.window(next),"frames":frames,"bytes":sent_bytes,
                "interval_ms":last_report.elapsed().as_millis(),"stage_order":["build","encode","send"],
                "sum_us":stage_us,"max_us":stage_max_us})
            );
            frames = 0;
            sent_bytes = 0;
            stage_us = [0; 3];
            stage_max_us = [0; 3];
            last_report = std::time::Instant::now();
        }
        let advanced = next > cursor;
        let confirmation_advanced = next_confirmation > confirmation_cursor;
        cursor = next;
        confirmation_cursor = next_confirmation;
        if advanced || confirmation_advanced {
            continue;
        }
        tokio::select! {
            value = changed.changed() => {value?;},
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Ping(body))) => socket.send(Message::Pong(body)).await?,
                Some(Ok(Message::Pong(_))) => {}, _ => return Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use marketcow_polymarket::discovery_source::{SnapshotBoundary, snapshot_event};
    use std::collections::BTreeSet;
    #[test]
    fn production_market_persistence_lanes_have_1_gib_and_4096_batches() {
        let aggregate = MarketPersistenceLimits {
            batches: 16384,
            bytes: 4 * 1024 * 1024 * 1024,
        };
        let lane = market_persistence_lane_limits(aggregate.batches, aggregate.bytes,
            16 * 1024 * 1024);
        assert_eq!(lane.batches, 4096);
        assert_eq!(lane.bytes, 1024 * 1024 * 1024);
        assert!(lane.batches * 4 <= aggregate.batches);
        assert!(lane.bytes * 4 <= aggregate.bytes);
    }

    // Tests of the legacy public queue deliberately use tiny disk limits to
    // exercise backpressure. Keep replay's retained-allocation budget
    // independent so those tests do not reject before reaching the disk gate.
    fn test_start(
        cursor: u64,
        base: Option<Value>,
        tokens: BTreeMap<String, String>,
        max_batches: usize,
        max_bytes: usize,
        batch_cap: usize,
        mut persist: impl FnMut(&[Arc<Batch>]) -> Result<()> + Send + 'static,
    ) -> Result<Publication> {
        test_start_with_replay(cursor, base, tokens, max_batches, max_bytes,
            1024, 16 * 1024 * 1024, batch_cap, move |batches| persist(batches))
    }
    fn test_start_with_replay(
        cursor: u64,
        base: Option<Value>,
        tokens: BTreeMap<String, String>,
        max_batches: usize,
        max_bytes: usize,
        replay_batches: usize,
        replay_bytes: usize,
        batch_cap: usize,
        mut persist: impl FnMut(&[Arc<Batch>]) -> Result<()> + Send + 'static,
    ) -> Result<Publication> {
        let mut publication = Publication::start_managed_with_replay(cursor, base, tokens,
            max_batches, max_bytes, replay_batches, replay_bytes, batch_cap,
            move |work| match work {
                PersistenceWork::Events(batches) => persist(batches),
                PersistenceWork::Retire(_) => anyhow::bail!("test writer has no retirement support"),
            })?;
        publication.retirement_enabled = false;
        Ok(publication)
    }
    #[tokio::test]
    async fn management_phase_counts_are_unique_and_fail_closed_when_metrics_missing() {
        let mut p = Publication::start_managed(0, Some(seed_with_market()), tokens(), 8, 131072, 65536,
            |_| Ok(())).unwrap();
        let reader = p.reader();
        let missing = reader.management_status().unwrap();
        assert_eq!(missing["phase_counts_authoritative"], false);
        assert_eq!(missing["publication_binding_tokens"], 2);
        p.set_acquisition_metrics((1, 1, 0), json!({"active_tokens":1,
            "draining_tokens":0,"token_limit":6,"socket_limit":2}),
            BTreeSet::from(["11".into()]), BTreeSet::new());
        let prepared = reader.management_status().unwrap();
        assert_eq!(prepared["prepared_tokens"], 1);
        assert_eq!(prepared["active_tokens"], 1);
        assert_eq!(prepared["draining_tokens"], 0);
        assert_eq!(prepared["phase_counts_authoritative"], true);
        p.set_acquisition_metrics((2, 1, 1), json!({"active_tokens":1,
            "draining_tokens":1,"token_limit":6,"socket_limit":2}),
            BTreeSet::from(["11".into(), "12".into()]), BTreeSet::from(["12".into()]));
        let draining = reader.management_status().unwrap();
        assert_eq!(draining["prepared_tokens"], 0);
        assert_eq!(draining["active_tokens"], 1);
        assert_eq!(draining["draining_tokens"], 1);
        assert_eq!(draining["phase_counts_authoritative"], true);
        p.retire_catalog_markets(BTreeSet::from(["1".into()])).unwrap();
        p.set_acquisition_metrics((2, 1, 1), json!({"active_tokens":0,
            "draining_tokens":2,"token_limit":6,"socket_limit":2}),
            BTreeSet::from(["11".into(), "12".into()]),
            BTreeSet::from(["11".into(), "12".into()]));
        let released_binding = reader.management_status().unwrap();
        assert_eq!(released_binding["publication_binding_tokens"], 0);
        assert_eq!(released_binding["hot_binding_tokens"], 2);
        assert_eq!(released_binding["draining_tokens"], 2);
        assert_eq!(released_binding["phase_counts_authoritative"], true);
        p.finish().await.unwrap();
    }
    #[test]
    fn trace_sample_is_bounded_and_cursor_stable() {
        assert!(!btc5m_trace_sample(0));
        assert!(!btc5m_trace_sample(BTC5M_TRACE_SAMPLE_MODULUS - 1));
        assert!(btc5m_trace_sample(BTC5M_TRACE_SAMPLE_MODULUS));
        assert!(btc5m_trace_sample(BTC5M_TRACE_SAMPLE_MODULUS * 2));
    }
    #[tokio::test]
    async fn timed_batch_survives_replay_and_records_visibility_and_durability() {
        let mut p =
            test_start(0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(())).unwrap();
        let admitted_mono = std::time::Instant::now();
        let timing = SourceBatchTiming {
            raw_batch_first_received_at: Utc::now(),
            raw_batch_last_received_at: Utc::now(),
            actor_admitted_at: Utc::now(),
            actor_started_at: Utc::now(),
            actor_completed_at: Utc::now(),
            actor_admitted_mono: admitted_mono,
            actor_started_mono: std::time::Instant::now(),
            actor_completed_mono: std::time::Instant::now(),
            publish_call_started_at: Utc::now(),
            publish_call_started_mono: std::time::Instant::now(),
            shared_replay_visible_at: None,
            shared_replay_visible_mono: None,
            durable_at: None,
        };
        p.publish_with_capacity_and_materialized_timed(
            events(0), BTreeMap::new(), None, Some(timing),
        ).await.unwrap();
        let batch = p.reader().replay(0, 0, 1, 131072).unwrap().batches.remove(0);
        let visible = batch.timing_snapshot().unwrap();
        assert!(visible.shared_replay_visible_at.is_some());
        assert!(visible.shared_replay_visible_mono.unwrap() >= visible.publish_call_started_mono);
        assert!(visible.actor_started_mono >= visible.actor_admitted_mono);
        assert!(visible.actor_completed_mono >= visible.actor_started_mono);
        p.finish().await.unwrap();
        assert!(batch.timing_snapshot().unwrap().durable_at.is_some());
    }
    fn events(after: u64) -> Vec<Value> {
        ["11","12"].iter().enumerate().map(|(i,token)| snapshot_event(
            &json!({"asset_id":token,"market":"condition","tick_size":"0.01","timestamp":"1700000000000",
                "hash":"source","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
            SnapshotBoundary {market_id:"1",condition_id:"condition",token_id:token,recovery_id:"test",
                cursor:after+i as u64+1, received_at:chrono::DateTime::from_timestamp(1700000001,0).unwrap()}).unwrap()).collect()
    }
    fn other_market_events(after: u64) -> Vec<Value> {
        ["21", "22"].iter().enumerate().map(|(i,token)| snapshot_event(
            &json!({"asset_id":token,"market":"condition2","tick_size":"0.01","timestamp":"1700000000000",
                "hash":"source","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
            SnapshotBoundary {market_id:"2",condition_id:"condition2",token_id:token,recovery_id:"test",
                cursor:after+i as u64+1, received_at:chrono::DateTime::from_timestamp(1700000001,0).unwrap()}).unwrap()).collect()
    }

    #[tokio::test]
    async fn projection_size_accounting_replaces_existing_token_sizes() {
        let mut p = test_start(
            0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(()),
        ).unwrap();
        let initial = p.memory.lock().unwrap().state_bytes;
        let first = events(0);
        let first_sizes = first.iter().map(|event|
            serde_json::to_vec(&event["canonical_payload"]).unwrap().len()
        ).collect::<Vec<_>>();
        p.publish(first, None).unwrap();
        assert_eq!(p.memory.lock().unwrap().state_bytes,
            initial + first_sizes.iter().sum::<usize>());
        let second = events(2);
        let second_sizes = second.iter().map(|event|
            serde_json::to_vec(&event["canonical_payload"]).unwrap().len()
        ).collect::<Vec<_>>();
        p.publish(second, None).unwrap();
        let memory = p.memory.lock().unwrap();
        assert_eq!(memory.state_sizes[&(false, "11".to_owned())], second_sizes[0]);
        assert_eq!(memory.state_sizes[&(false, "12".to_owned())], second_sizes[1]);
        assert_eq!(memory.state_bytes, initial + second_sizes.iter().sum::<usize>());
        drop(memory);
        p.finish().await.unwrap();
    }


    #[test]
    fn immutable_materialized_validation_does_not_wait_for_global_publication_lock() {
        let mut publication = test_start(
            0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(()),
        ).unwrap();
        let immutable = ValidatedSourceBatch::new(
            events(0), false, 0, &tokens(), 65536,
        ).unwrap();
        let bytes = immutable.encoded_bytes();
        let memory = publication.memory.clone();
        let (result_sender, result_receiver) = std::sync::mpsc::channel();
        let held = memory.lock().unwrap();
        let worker = std::thread::spawn(move || {
            let invalid = BTreeMap::from([(999, json!({}))]);
            let result = publication.publish_validated(
                immutable, invalid, None, bytes, None, None,
            );
            result_sender.send(result.map_err(|error| error.to_string())).unwrap();
        });
        let result = result_receiver.recv_timeout(std::time::Duration::from_millis(500));
        drop(held);
        worker.join().unwrap();
        assert!(result.unwrap().unwrap_err().contains("materialized compact-book cursor set differs"));
    }

    #[tokio::test]
    async fn actor_prepared_projection_rebases_and_preserves_ordered_commit() {
        let mut prepared = prepare_market_projection(&events(0), &BTreeMap::new()).unwrap();
        prepared.rebase(2).unwrap();
        let mut first = prepared.take_for(&events(2)[..1]).unwrap();
        assert!(prepared.take_for(&events(2)[..1]).is_err(),
            "a prepared action cannot be consumed twice");
        let second = prepared.take_for(&events(2)[1..]).unwrap();
        assert!(prepared.is_empty());
        match first.actions.remove(&3).unwrap() {
            ProjectionAction::Book { physical_cursor, token_id, .. } => {
                assert_eq!((physical_cursor, token_id.as_str()), (3, "11"));
            }
            _ => panic!("expected book projection"),
        }
        let mut publication = test_start(
            0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(()),
        ).unwrap();
        let complete = prepare_market_projection(&events(0), &BTreeMap::new()).unwrap();
        publication.publish_with_capacity_and_materialized_prepared(
            events(0), BTreeMap::new(), None, None, Some(complete),
        ).await.unwrap();
        assert_eq!(publication.memory.lock().unwrap().cursor, 2);
        assert_eq!(second.actions.len(), 1);
        publication.finish().await.unwrap();
    }

    #[tokio::test]
    async fn actor_proof_cannot_publish_after_owner_token_binding_changes() {
        let mut publication = test_start(
            0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(()),
        ).unwrap();
        let actor_events = events(0);
        let prepared = prepare_market_projection(&actor_events, &BTreeMap::new()).unwrap();
        let validated = ValidatedSourceBatch::new(
            actor_events, false, 0, &tokens(), 65536,
        ).unwrap();
        publication.tokens.insert("11".into(), "other-market".into());
        assert!(publication.publish_actor_validated(
            validated, BTreeMap::new(), None, None, Some(prepared),
        ).await.unwrap_err().to_string().contains("binding changed"));
        assert_eq!(publication.cursor().unwrap(), 0);
        publication.finish().await.unwrap();
    }

    #[tokio::test]
    async fn production_batches_commit_independent_market_stream_watermarks() {
        let observed = Arc::new(Mutex::new(Vec::<(String, String, u64)>::new()));
        let persisted = observed.clone();
        let identities = tokens().into_iter().chain([
            ("21".into(), "2".into()), ("22".into(), "2".into()),
        ]).collect();
        let mut p = test_start(0, Some(seed_with_second_market("21", "22")), identities,
            16, 1024 * 1024, 65536, move |batches| {
            for batch in batches {
                let (market, epoch, cursor) = batch.validated.single_market_group()
                    .context("market group proof")?;
                persisted.lock().unwrap().push((market.into(), epoch.into(), cursor));
            }
            Ok(())
        }).unwrap();
        let reader = p.reader();
        p.publish_native_events(events(0), None).unwrap();
        p.publish_native_events(other_market_events(2), None).unwrap();
        p.publish_native_events(events(4), None).unwrap();
        assert_eq!(reader.market_stream_watermark("1").unwrap().unwrap().published_cursor, 2);
        assert_eq!(reader.market_stream_watermark("2").unwrap().unwrap().published_cursor, 1);
        p.finish().await.unwrap();
        let a = reader.market_stream_watermark("1").unwrap().unwrap();
        let b = reader.market_stream_watermark("2").unwrap().unwrap();
        assert_eq!((a.published_cursor, a.durable_cursor), (2, 2));
        assert_eq!((b.published_cursor, b.durable_cursor), (1, 1));
        let held = reader.memory.lock().unwrap();
        let isolated = reader.clone();
        let epoch = a.stream_epoch.clone();
        let (done, read_result) = std::sync::mpsc::channel();
        let market_read = std::thread::spawn(move || {
            let watermark = isolated.market_stream_watermark("1").unwrap().unwrap();
            let page = isolated.replay_market("1", &epoch, 0, 2, 131072).unwrap();
            let snapshot = isolated.capture_market("1", &["11".into(), "12".into()]).unwrap();
            assert_eq!(snapshot.books.len(), 2);
            assert_eq!(snapshot.watermark.published_cursor, watermark.published_cursor);
            assert!(snapshot.market_book_cursors.values().all(|cursor| *cursor == 2));
            done.send((watermark.published_cursor, page.next_cursor)).unwrap();
        });
        assert_eq!(read_result.recv_timeout(std::time::Duration::from_millis(500)).unwrap(), (2, 2),
            "selected-market replay AND snapshot must not wait for the global projection lock");
        drop(held);
        market_read.join().unwrap();
        let a_tokens = vec!["11".to_owned(), "12".to_owned()];
        let b_tokens = vec!["21".to_owned(), "22".to_owned()];
        let a_snapshot = reader.capture_market("1", &a_tokens).unwrap();
        let b_snapshot = reader.capture_market("2", &b_tokens).unwrap();
        assert_eq!(a_snapshot.watermark, a);
        assert_eq!(b_snapshot.watermark, b);
        assert_eq!(a_snapshot.market_book_cursors.values().copied().collect::<Vec<_>>(), vec![2, 2]);
        assert_eq!(b_snapshot.market_book_cursors.values().copied().collect::<Vec<_>>(), vec![1, 1]);
        assert!(a_snapshot.books.keys().all(|token| a_tokens.contains(token)));
        assert!(b_snapshot.books.keys().all(|token| b_tokens.contains(token)));
        assert!(reader.capture_market("1", &b_tokens).err().unwrap().to_string()
            .contains("market snapshot token binding differs"));
        let a_page = reader.replay_market("1", &a.stream_epoch, 0, 2, 131072).unwrap();
        let b_page = reader.replay_market("2", &b.stream_epoch, 0, 2, 131072).unwrap();
        assert_eq!((a_page.groups.len(), b_page.groups.len()), (2, 1));
        assert_eq!(a_page.groups.iter().map(|group| group.market_cursor).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(a_page.groups[0].payload.validated.events()[0]["market_id"], "1");
        assert_eq!(b_page.groups[0].payload.validated.events()[0]["market_id"], "2");
        assert_eq!(a_page.durable_cursor, 2);
        assert_eq!(b_page.durable_cursor, 1);
        assert_ne!(a.stream_epoch, b.stream_epoch);
        assert_eq!(observed.lock().unwrap().iter().map(|(market, _, cursor)| (market.as_str(), *cursor)).collect::<Vec<_>>(),
            vec![("1", 1), ("2", 1), ("1", 2)]);
    }

    #[tokio::test]
    async fn full_market_writer_lane_keeps_live_books_without_false_durable_progress() {
        let (release, gate) = std::sync::mpsc::channel::<()>();
        let identities = BTreeMap::from([
            ("11".into(), "1".into()), ("12".into(), "1".into()),
            ("21".into(), "2".into()), ("22".into(), "2".into()),
        ]);
        let p = Publication::start_managed_with_replay(0,
            Some(seed_with_second_market("21", "22")), identities,
            2, 1024 * 1024, 64, 2 * 1024 * 1024, 65536, move |_| {
                gate.recv_timeout(std::time::Duration::from_secs(3))?;
                Ok(())
            }).unwrap();
        let reader = p.reader();
        p.publish_native_events(events(0), None).unwrap();
        p.publish_native_events(events(2), None).unwrap();
        p.publish_native_events(other_market_events(2), None).unwrap();
        p.publish_native_events(events(4), None).unwrap();
        let ids = vec!["11".to_owned(), "12".to_owned()];
        let live = reader.capture_market("1", &ids).unwrap();
        assert_eq!(live.watermark.published_cursor, 3);
        assert_eq!(live.watermark.durable_cursor, 0);
        assert!(!live.capacity_fenced);
        assert_eq!(live.books.len(), 2);
        release.send(()).unwrap();
        release.send(()).unwrap();
        p.finish().await.unwrap();
        let final_watermark = reader.market_stream_watermark("1").unwrap().unwrap();
        assert_eq!(final_watermark.durable_cursor, 1);
        assert_eq!(final_watermark.published_cursor, 3);
        let other = reader.market_stream_watermark("2").unwrap().unwrap();
        assert_eq!((other.published_cursor, other.durable_cursor), (1, 1));
    }
    #[tokio::test]
    async fn market_snapshot_never_rebinds_old_confirmation_after_last_trade() {
        let source = crate::source_websocket::btc5m_reducer_fixture_sequence().unwrap();
        let condition = format!("0x{}", "1".repeat(64));
        let tokens = BTreeMap::from([
            ("101".to_owned(), "4944030".to_owned()),
            ("102".to_owned(), "4944030".to_owned()),
        ]);
        let mut p = test_start(42, Some(json!({"latest_cursor":42,
            "live_stream_connected":true,"gaps":[],"books":[],"markets":[{
                "identity":{"market_id":"4944030","condition_id":condition,
                    "outcomes":[{"token_id":"101"},{"token_id":"102"}]}}]})),
            tokens, 16, 8 * 1024 * 1024, 512 * 1024, |_| Ok(())).unwrap();
        for event in &source[..3] { p.publish_native_events(vec![event.clone()], None).unwrap(); }
        let ids = vec!["101".to_owned(), "102".to_owned()];
        let before = p.reader().capture_market("4944030", &ids).unwrap();
        let at = chrono::DateTime::parse_from_rfc3339(
            before.books["101"]["received_at"].as_str().unwrap())
            .unwrap().with_timezone(&chrono::Utc) + chrono::TimeDelta::seconds(1);
        assert!(p.confirm_reduced_token("4944030", &condition, "101",
            before.books["101"]["state_checksum"].as_str().unwrap(),
            at, at, &"a".repeat(64)).unwrap());
        let confirmed = p.reader().capture_market("4944030", &ids).unwrap();
        assert_eq!(confirmed.confirmation_facts.len(), 1);
        assert_eq!(source[3]["raw_payload"]["event_type"], "last_trade_price");
        p.publish_native_events(vec![source[3].clone()], None).unwrap();
        let after = p.reader().capture_market("4944030", &ids).unwrap();
        assert_eq!(after.books["101"]["received_at"], before.books["101"]["received_at"],
            "trade must not refresh depth receive time");
        assert_eq!(after.confirmation_sequence, confirmed.confirmation_sequence,
            "trade must not manufacture a confirmation");
        for (token, fact) in &after.confirmation_facts {
            let book = &after.books[token];
            assert_eq!(fact["book_epoch"], book["book_epoch"]);
            assert_eq!(fact["sequence"], book["sequence"]);
            assert_eq!(fact["state_checksum"], book["state_checksum"]);
        }
        assert_eq!(after.books["101"]["sequence"].as_u64().unwrap(),
            confirmed.books["101"]["sequence"].as_u64().unwrap() + 1);
        assert!(!after.confirmation_facts.contains_key("101"));
        assert!(!after.market_confirmation_versions.contains_key("101"));
        // Even a retained stale fact must not escape the coherent capture.
        // This tests the second fence independently of mutation cleanup.
        p.memory.lock().unwrap().confirmation_facts.insert("101".into(),
            confirmed.confirmation_facts["101"].clone());
        assert!(p.reader().capture_market("4944030", &ids).unwrap()
            .confirmation_facts.is_empty());
        p.finish().await.unwrap();
    }
    #[test]
    fn native_market_projection_keeps_event_cursor_separate_from_archive_position() {
        let replay = MarketReplay::<usize, Option<MarketReadState>>::new_with_state(
            8, 131072, 262144).unwrap();
        for (market, tokens) in [("1", ["11", "12"]), ("2", ["21", "22"])] {
            replay.register(market, "epoch").unwrap();
            replay.update_state(market, |state| *state = Some(MarketReadState {
                token_ids: tokens.into_iter().map(str::to_owned).collect(),
                ..MarketReadState::default()
            })).unwrap();
        }
        for (market, cursor, events) in [("1", 1, events(0)),
            ("2", 1, other_market_events(0)), ("1", 2, events(2))] {
            let prepared = prepare_market_projection(&events, &BTreeMap::new()).unwrap();
            replay.publish_with_state_prepare(market, cursor, &events,
                Arc::new(cursor as usize), 1024, 2048,
                |state| state.as_ref().context("native state missing")?
                    .prepare_native_group(market, cursor, &events, prepared.into_actions()),
                |_| Ok(()), |state, next| *state = Some(next)).unwrap();
        }
        for (market, cursor, tokens) in [("1", 2, ["11", "12"]),
            ("2", 1, ["21", "22"])] {
            replay.read_state(market, |state, watermark| {
                let state = state.as_ref().unwrap();
                assert_eq!(watermark.published_cursor, cursor);
                assert_eq!(state.books.len(), 2);
                assert!(tokens.iter().all(|token| state.market_book_cursors[*token] == cursor));
                let expected_event_cursors = if market == "1" { [3, 4] } else { [1, 2] };
                assert_eq!(tokens.map(|token| state.event_book_cursors[token]),
                    expected_event_cursors);
                assert!(state.physical_book_cursors.is_empty(),
                    "unpersisted native books must not borrow physical archive positions");
                Ok(())
            }).unwrap();
        }
        replay.update_state("1", |state| {
            let state = state.as_mut().unwrap();
            state.confirmation_sequence = 1;
            state.confirmation_facts.insert("11".into(), Arc::new(json!({"sequence":1})));
            state.market_confirmation_versions.insert("11".into(), 1);
        }).unwrap();
        let next_events = events(4);
        let prepared = prepare_market_projection(&next_events, &BTreeMap::new()).unwrap();
        replay.publish_with_state_prepare("1", 3, &next_events, Arc::new(3), 1024, 2048,
            |state| state.as_ref().unwrap().prepare_native_group(
                "1", 3, &next_events, prepared.into_actions()),
            |_| Ok(()), |state, next| *state = Some(next)).unwrap();
        replay.read_state("1", |state, watermark| {
            let state = state.as_ref().unwrap();
            assert_eq!(watermark.published_cursor, 3);
            assert_eq!(state.last_event_cursor, 6);
            assert_eq!((state.event_book_cursors["11"], state.event_book_cursors["12"]), (5, 6));
            assert!(state.confirmation_facts.is_empty());
            assert!(state.market_confirmation_versions.is_empty());
            assert_eq!(state.confirmation_sequence, 1,
                "a book mutation invalidates facts without forging a new confirmation");
            Ok(())
        }).unwrap();
        for invalid_events in [events(4), events(8)] {
            let prepared = prepare_market_projection(&invalid_events, &BTreeMap::new()).unwrap();
            let error = replay.publish_with_state_prepare("1", 4, &invalid_events,
                Arc::new(4), 1024, 2048,
                |state| state.as_ref().unwrap().prepare_native_group(
                    "1", 4, &invalid_events, prepared.into_actions()),
                |_| panic!("noncontiguous event group reached writer admission"),
                |state, next| *state = Some(next)).err().unwrap();
            assert!(error.to_string().contains("native market event cursor is not contiguous"));
        }
        let wrong_token = other_market_events(2);
        let prepared = prepare_market_projection(&wrong_token, &BTreeMap::new()).unwrap();
        assert!(replay.publish_with_state_prepare("1", 4, &wrong_token,
            Arc::new(4), 1024, 2048,
            |state| state.as_ref().unwrap().prepare_native_group(
                "1", 4, &wrong_token, prepared.into_actions()),
            |_| panic!("invalid projection reached writer admission"),
            |state, next| *state = Some(next)).is_err());
        replay.read_state("1", |_, watermark| {
            assert_eq!(watermark.published_cursor, 3);
            Ok(())
        }).unwrap();
    }
    #[tokio::test]
    async fn market_confirmations_are_isolated_and_mutations_clear_old_facts() {
        let identities = tokens().into_iter().chain([
            ("21".into(), "2".into()), ("22".into(), "2".into()),
        ]).collect();
        let mut p = test_start(0, Some(seed_with_second_market("21", "22")), identities,
            8, 131072, 65536, |_| Ok(())).unwrap();
        p.publish_native_events(events(0), None).unwrap();
        p.publish_native_events(other_market_events(2), None).unwrap();
        let reader = p.reader();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while reader.market_stream_watermark("1").unwrap().unwrap().durable_cursor < 1
                || reader.market_stream_watermark("2").unwrap().unwrap().durable_cursor < 1 {
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        // Drain completion notifications before testing the selected-market
        // confirmation wakeup; durability is a separate notification source.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let a_tokens = vec!["11".to_owned(), "12".to_owned()];
        let b_tokens = vec!["21".to_owned(), "22".to_owned()];
        let a = reader.capture_market("1", &a_tokens).unwrap();
        let b = reader.capture_market("2", &b_tokens).unwrap();
        let mut a_watch = reader.subscribe_market("1", &a.watermark.stream_epoch).unwrap();
        let mut b_watch = reader.subscribe_market("2", &b.watermark.stream_epoch).unwrap();
        a_watch.borrow_and_update();
        b_watch.borrow_and_update();
        let at = chrono::DateTime::from_timestamp(1700000010, 0).unwrap();
        assert!(p.confirm_reduced_token("1", "condition", "11",
            a.books["11"]["state_checksum"].as_str().unwrap(), at, at, &"a".repeat(64)).unwrap());
        assert!(a_watch.has_changed().unwrap());
        assert!(!b_watch.has_changed().unwrap());
        let confirmed_a = reader.capture_market("1", &a_tokens).unwrap();
        let untouched_b = reader.capture_market("2", &b_tokens).unwrap();
        assert_eq!(confirmed_a.confirmation_sequence, 1);
        assert_eq!(confirmed_a.market_confirmation_versions["11"], 1);
        assert_eq!(untouched_b.confirmation_sequence, 0);
        assert!(untouched_b.confirmation_facts.is_empty());
        assert_eq!(untouched_b.books, b.books);
        assert_eq!(untouched_b.watermark, b.watermark);
        a_watch.borrow_and_update();
        assert!(p.confirm_reduced_token("2", "condition2", "21",
            b.books["21"]["state_checksum"].as_str().unwrap(), at, at, &"b".repeat(64)).unwrap());
        assert!(b_watch.has_changed().unwrap());
        assert!(!a_watch.has_changed().unwrap());
        assert_eq!(reader.capture_market("2", &b_tokens).unwrap().confirmation_sequence, 1,
            "market sequence must not inherit another market's confirmation count");
        p.publish_native_events(events(4), None).unwrap();
        let replaced_a = reader.capture_market("1", &a_tokens).unwrap();
        assert!(replaced_a.confirmation_facts.is_empty());
        assert!(replaced_a.market_confirmation_versions.is_empty());
        assert_eq!(replaced_a.confirmation_sequence, 1,
            "mutation does not manufacture a confirmation");
        assert_eq!(reader.capture_market("2", &b_tokens).unwrap().confirmation_facts.len(), 1);
        p.finish().await.unwrap();
    }
    fn terminal_event(cursor: u64) -> Value {
        let mut terminal = events(cursor - 1).remove(0);
        terminal["event_type"] = json!("market_terminal");
        terminal["token_id"] = Value::Null;
        terminal["canonical_payload"] = json!({"retired_token_ids":["11","12"],"market":{
            "identity":{"market_id":"1","condition_id":"condition","outcomes":[{"token_id":"11"},{"token_id":"12"}]},
            "lifecycle_state":"closed","closed":true,"accepting_orders":false,"resolution":null,
            "lifecycle_source":"polymarket_gamma","terminal_at":"2026-01-01T00:00:00Z",
            "lifecycle_evidence_sha256":"a".repeat(64)}});
        terminal["canonical_payload_sha256"] =
            json!(canonical_hash(&terminal["canonical_payload"]));
        terminal.as_object_mut().unwrap().remove("event_id");
        terminal["event_id"] = json!(canonical_hash(&terminal));
        terminal
    }
    fn tokens() -> BTreeMap<String, String> {
        [("11".into(), "1".into()), ("12".into(), "1".into())].into()
    }
    fn seed() -> Value {
        json!({"latest_cursor":0,"books":[],"markets":[{"identity":{"market_id":"1",
            "condition_id":"condition","outcomes":[{"token_id":"11"},{"token_id":"12"}]}}],
            "gaps":[],"schema_version":"marketcow.polymarket.live-stream.v1","type":"state"})
    }
    fn seed_with_second_market(first: &str, second: &str) -> Value {
        let mut state = seed();
        state["markets"].as_array_mut().unwrap().push(json!({"identity":{
            "market_id":"2","condition_id":"condition2",
            "outcomes":[{"token_id":first},{"token_id":second}]}}));
        state
    }
    fn seed_with_market() -> Value {
        let mut state = seed();
        state["markets"] = json!([{"identity":{"market_id":"1","condition_id":"c1",
            "outcomes":[{"token_id":"11"},{"token_id":"12"}]}}]);
        state
    }
    #[tokio::test]
    async fn oversized_cold_catalog_is_rejected_before_hot_release() {
        let mut base = seed();
        base["markets"] = json!([{"identity":{"market_id":"1","condition_id":"c",
            "outcomes":[{"token_id":"11"},{"token_id":"12"}]},
            "large_metadata":"x".repeat(256 * 1024)}]);
        let mut publication = Publication::start_managed(0, Some(base), tokens(),
            2, 1024 * 1024, 512 * 1024, |_| Ok(())).unwrap();
        let before = publication.capture_view().unwrap();
        assert!(publication.check_retirement(BTreeSet::from(["1".into()])).is_err());
        assert_eq!(publication.capture_view().unwrap().markets.len(), before.markets.len());
        assert_eq!(publication.tokens, tokens());
        assert_eq!(publication.reader().retirement_status().unwrap(), (0, 0));
        publication.finish().await.unwrap();
    }
    #[tokio::test]
    async fn metadata_only_retirement_reserves_queue_before_writer_debit() {
        let persisted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let receipt = persisted.clone();
        let mut publication = Publication::start_managed_with_replay(0,
            Some(seed_with_market()), BTreeMap::new(), 2, 131072, 8,
            512 * 1024, 65536, move |work| {
                match work {
                    PersistenceWork::Retire(retirement) => {
                        assert_eq!(retirement.markets, BTreeSet::from(["1".into()]));
                        assert!(retirement.market_epochs.is_empty());
                        receipt.fetch_add(1, Ordering::AcqRel);
                        Ok(())
                    }
                    PersistenceWork::Events(_) => anyhow::bail!("unexpected events"),
                }
            }).unwrap();
        publication.retire_catalog_markets(BTreeSet::from(["1".into()])).unwrap();
        let mut changed = publication.reader().subscribe();
        while publication.reader().retirement_status().unwrap().1 < 1 {
            changed.changed().await.unwrap();
        }
        assert_eq!(persisted.load(Ordering::Acquire), 1);
        let view = publication.capture_view().unwrap();
        assert_eq!(view.queued, 0);
        assert_eq!(publication.memory.lock().unwrap().queued_bytes, 0);
        assert!(view.markets.is_empty());
        publication.finish().await.unwrap();
    }
    #[tokio::test]
    async fn hot_path_timing_is_aggregated_and_reset_without_changing_publication() {
        let mut p =
            test_start(0, Some(seed()), tokens(), 4, 65536, 16384, |_| Ok(())).unwrap();
        p.publish(events(0), None).unwrap();
        let timing = p.take_hot_path_timing();
        assert_eq!(timing["validation"]["count"], 1);
        assert_eq!(timing["validated_events"], 2);
        assert!(timing["validated_bytes"].as_u64().unwrap() > 0);
        assert_eq!(timing["memory_lock_wait"]["count"], 1);
        assert_eq!(timing["memory_lock_held"]["count"], 1);
        assert_eq!(p.take_hot_path_timing()["validation"]["count"], 0);
        assert_eq!(p.cursor().unwrap(), 2);
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn async_publication_records_the_same_validation_and_commit_stages() {
        let mut p =
            test_start(0, Some(seed()), tokens(), 4, 65536, 16384, |_| Ok(())).unwrap();
        p.publish_with_capacity_and_materialized(events(0), BTreeMap::new(), None)
            .await
            .unwrap();
        let timing = p.take_hot_path_timing();
        assert_eq!(timing["validation"]["count"], 1);
        assert_eq!(timing["memory_lock_wait"]["count"], 1);
        assert_eq!(timing["memory_lock_held"]["count"], 1);
        assert_eq!(p.cursor().unwrap(), 2);
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn admitted_identity_is_ordered_on_disk_worker_and_survives_queue_rejection() {
        let (started, wait) = mpsc::channel();
        let (release, gate) = mpsc::channel();
        let mut bound = tokens();
        let mut cursor = 0;
        let mut base = seed();
        base["catalog_revision"] = json!("catalog");
        let mut p = test_start(0, Some(base), tokens(), 1, 65536, 16384, move |batches| {
            for batch in batches {
                bound.extend(batch.catalog_bindings.clone());
                validate_source_events(batch.validated.events(), false, cursor, &bound, 16384)?;
                if cursor == 0 {
                    started.send(()).unwrap();
                    gate.recv().unwrap();
                }
                cursor = batch.validated.events().last().unwrap()["cursor"]
                    .as_u64()
                    .unwrap();
            }
            Ok(())
        })
        .unwrap();
        p.publish(events(0), None).unwrap();
        wait.recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        let record = json!({"identity":{"market_id":"2","condition_id":"condition2","outcomes":[{"token_id":"21"},{"token_id":"22"}]}});
        let records = vec![record.clone()];
        let hash = canonical_hash(&json!(records));
        assert!(
            p.admit_catalog_markets("catalog", records.clone(), "wrong")
                .is_err()
        );
        assert!(!p.capture_view().unwrap().markets.contains_key("2"));
        p.admit_catalog_markets("catalog", records.clone(), &hash)
            .unwrap();
        p.admit_catalog_markets("catalog", records, &hash).unwrap();
        let view = p.capture_view().unwrap();
        assert!(view.markets.contains_key("2"));
        assert!(!view.books.contains_key("21"));
        assert_eq!(view.cursor, 2);
        assert_eq!(view.persisted_cursor, 0);
        let next = || {
            ["21","22"].iter().enumerate().map(|(i,token)|snapshot_event(
            &json!({"asset_id":token,"market":"condition2","tick_size":"0.01","timestamp":"1700000000000","hash":"source",
                "bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
            SnapshotBoundary{market_id:"2",condition_id:"condition2",token_id:token,recovery_id:"admitted",cursor:3+i as u64,
                received_at:chrono::DateTime::from_timestamp(1700000001,0).unwrap()}).unwrap()).collect()
        };
        assert!(p.publish(next(), None).is_err());
        assert_eq!(p.pending_bindings.len(), 2);
        assert_eq!(p.cursor().unwrap(), 2);
        release.send(()).unwrap();
        let mut changed = p.reader().subscribe();
        while p.persisted_cursor() < 2 {
            changed.changed().await.unwrap();
        }
        p.publish(next(), None).unwrap();
        assert!(p.pending_bindings.is_empty());
        assert_eq!(
            p.capture_view().unwrap().books["21"]["condition_id"],
            "condition2"
        );
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn scope_retirement_queues_after_books_without_waiting_for_disk() {
        let (started, wait) = mpsc::channel();
        let (release, gate) = mpsc::channel();
        let (observed, rows) = mpsc::channel();
        let mut state = seed_with_market();
        state["catalog_revision"] = json!("catalog");
        let retired_record = state["markets"][0].clone();
        let mut p =
            Publication::start_managed_with_replay(0, Some(state), tokens(), 2, 65536, 8, 512 * 1024, 16384, move |work| {
                match work {
                    PersistenceWork::Events(batches) => {
                        observed.send("events").unwrap();
                        assert_eq!(
                            batches.last().unwrap().validated.events().last().unwrap()["cursor"],
                            2
                        );
                        started.send(()).unwrap();
                        gate.recv().unwrap();
                    }
                    PersistenceWork::Retire(retirement) => {
                        assert_eq!(retirement.after, 2);
                        assert_eq!(
                            retirement.markets,
                            std::collections::BTreeSet::from(["1".into()])
                        );
                        observed.send("retired").unwrap();
                    }
                }
                Ok(())
            })
            .unwrap();
        p.publish(events(0), None).unwrap();
        wait.recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        p.retire_catalog_markets(std::collections::BTreeSet::from(["1".into()]))
            .unwrap();
        let view = p.capture_view().unwrap();
        assert_eq!(view.cursor, 2);
        assert_eq!(view.persisted_cursor, 0);
        assert!(view.books.is_empty());
        assert_eq!(view.queued, 2);
        assert!(p.tokens.is_empty());
        assert!(p.publish(events(2), None).is_err());
        let attempted = vec![retired_record];
        let attempted_sha = canonical_hash(&json!(attempted));
        let admission_error = p.check_catalog_admission("catalog", attempted,
            &attempted_sha, &BTreeSet::from(["1".into()]), 2).unwrap_err();
        assert!(admission_error.to_string().contains("market retirement pending"));
        let reader = p.reader();
        assert_eq!(reader.market_stream_watermark("1").unwrap().unwrap().published_cursor, 1);
        release.send(()).unwrap();
        p.finish().await.unwrap();
        assert!(reader.market_stream_watermark("1").unwrap().is_none());
        assert_eq!(reader.market_replay.total_bytes(), 0);
        assert_eq!(
            rows.into_iter().collect::<Vec<_>>(),
            vec!["events", "retired"]
        );
    }

    /// Exercises the production 30-second drain deadline, not a shortened
    /// test-only timeout. A backlog is pending work, never proof of disk
    /// corruption or permission to discard the journal ticket.
    #[tokio::test]
    #[ignore = "31-second physical-writer stall fault injection"]
    async fn retirement_timeout_retries_same_ticket_and_other_market_stays_live() {
        let (started, wait) = mpsc::channel();
        let (release, gate) = mpsc::channel();
        let first_write = AtomicBool::new(true);
        let identities = tokens().into_iter().chain([
            ("21".into(), "2".into()), ("22".into(), "2".into()),
        ]).collect();
        let mut p = Publication::start_managed_with_replay(
            0, Some(seed_with_second_market("21", "22")), identities,
            16, 1024 * 1024, 1024, 16 * 1024 * 1024, 65536,
            move |work| {
                if matches!(work, PersistenceWork::Events(_))
                    && first_write.swap(false, Ordering::AcqRel) {
                    started.send(()).unwrap();
                    let _ = gate.recv();
                }
                Ok(())
            }).unwrap();
        p.publish_native_events(events(0), None).unwrap();
        wait.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        p.retire_catalog_markets(BTreeSet::from(["1".into()])).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(31));
        {
            let m = p.memory.lock().unwrap();
            assert!(m.error.is_none(), "waiting for earlier writes must not poison publication");
            assert_eq!((m.retirement_submitted, m.retirement_persisted), (1, 0));
            assert!(m.retiring_markets.contains("1"));
        }
        p.publish_native_events(other_market_events(2), None).unwrap();
        let b = p.reader().market_stream_watermark("2").unwrap().unwrap();
        assert_eq!((b.published_cursor, b.durable_cursor), (1, 0));
        release.send(()).unwrap();
        let mut changed = p.changed.subscribe();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if p.memory.lock().unwrap().retirement_persisted == 1 { break; }
                changed.changed().await.unwrap();
            }
        }).await.unwrap();
        assert!(p.reader().market_stream_watermark("1").unwrap().is_none());
        assert!(p.reader().market_stream_watermark("2").unwrap().is_some());
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn readmitted_market_starts_new_stream_epoch_after_durable_retirement() {
        let mut state = seed_with_market();
        state["catalog_revision"] = json!("catalog");
        let record = state["markets"][0].clone();
        let mut p = Publication::start_managed(0, Some(state), tokens(), 8, 131072, 65536,
            |_| Ok(())).unwrap();
        p.publish(events(0), None).unwrap();
        let reader = p.reader();
        let old = reader.market_stream_watermark("1").unwrap().unwrap();
        p.retire_catalog_markets(BTreeSet::from(["1".into()])).unwrap();
        let mut changed = reader.subscribe();
        while reader.retirement_status().unwrap().1 < 1 {
            changed.changed().await.unwrap();
        }
        assert!(reader.market_stream_watermark("1").unwrap().is_none());
        let records = vec![record];
        let sha = canonical_hash(&json!(records));
        p.admit_catalog_markets_scoped("catalog", records, &sha,
            &BTreeSet::from(["1".into()]), 2).unwrap();
        p.publish(events(2), None).unwrap();
        let fresh = reader.market_stream_watermark("1").unwrap().unwrap();
        assert_ne!(fresh.stream_epoch, old.stream_epoch);
        assert_eq!(fresh.published_cursor, 1);
        assert!(reader.replay_market("1", &old.stream_epoch, 0, 1, 65536).is_err());
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn terminal_market_cannot_be_resurrected_by_late_book_or_gap() {
        let mut p =
            test_start(0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(())).unwrap();
        p.publish(events(0), None).unwrap();
        p.publish(vec![terminal_event(3)], None).unwrap();
        let view = p.capture_view().unwrap();
        assert_eq!(view.cursor, 3);
        assert_eq!(view.markets["1"]["lifecycle_state"], "closed");

        let error = p.publish(events(3), None).unwrap_err().to_string();
        assert!(error.contains("terminal market"), "{error}");

        let mut late_gap = events(3).remove(0);
        late_gap["event_type"] = json!("recovery_started");
        late_gap["canonical_payload"] = json!({"recovery_scope":"token","token_id":"11",
            "recovery_id":"late-gap","reason":"source_connection_gap"});
        late_gap["gaps"] = json!([{"token_id":"11","code":"coverage_gap","resolved":false,
            "detected_at":"2026-01-01T00:00:00Z","event_at":null,"expected":null,
            "observed":null,"resolution":null}]);
        late_gap["canonical_payload_sha256"] =
            json!(canonical_hash(&late_gap["canonical_payload"]));
        late_gap.as_object_mut().unwrap().remove("event_id");
        late_gap["event_id"] = json!(canonical_hash(&late_gap));
        let error = p.publish(vec![late_gap], None).unwrap_err().to_string();
        assert!(error.contains("terminal market"), "{error}");
        assert_eq!(p.capture_view().unwrap().cursor, 3);
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn dependency_metadata_does_not_implicitly_subscribe_or_spend_acquisition_budget() {
        let mut base = seed();
        base["catalog_revision"] = json!("catalog");
        let mut p =
            test_start(0, Some(base), tokens(), 2, 65536, 16384, |_| Ok(())).unwrap();
        let records:Vec<_>=["2","3"].into_iter().map(|id|json!({"identity":{"market_id":id,
            "condition_id":format!("c{id}"),"outcomes":[{"token_id":format!("{id}1")},{"token_id":format!("{id}2")}]}})).collect();
        let hash = canonical_hash(&json!(records));
        let selected = std::collections::BTreeSet::from(["2".into()]);
        assert!(
            p.admit_catalog_markets_scoped("catalog", records.clone(), &hash, &selected, 2)
                .is_err()
        );
        assert_eq!(p.capture_view().unwrap().markets.len(), 1);
        p.admit_catalog_markets_scoped("catalog", records.clone(), &hash, &selected, 4)
            .unwrap();
        assert_eq!(p.tokens.len(), 4);
        assert!(!p.tokens.contains_key("31"));
        let view = p.capture_view().unwrap();
        assert_eq!(view.markets.len(), 3);
        assert!(view.books.is_empty());
        // Later promotion binds the already admitted metadata without replacing it.
        let selected = std::collections::BTreeSet::from(["3".into()]);
        p.admit_catalog_markets_scoped("catalog", records, &hash, &selected, 6)
            .unwrap();
        assert_eq!(p.tokens.len(), 6);
        assert_eq!(p.cursor().unwrap(), 0);
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn catalog_admission_cannot_steal_unsubscribed_dependency_token() {
        let mut base = seed();
        base["catalog_revision"] = json!("catalog");
        base["markets"].as_array_mut().unwrap().push(json!({"identity":{
            "market_id":"dependency","outcomes":[{"token_id":"x"},{"token_id":"y"}]}}));
        let mut p =
            test_start(0, Some(base), tokens(), 2, 65536, 16384, |_| Ok(())).unwrap();
        let records = vec![
            json!({"identity":{"market_id":"new","condition_id":"new","outcomes":[{"token_id":"x"},{"token_id":"z"}]}}),
        ];
        let hash = canonical_hash(&json!(records));
        assert!(
            p.admit_catalog_markets("catalog", records, &hash)
                .unwrap_err()
                .to_string()
                .contains("dependency metadata")
        );
        assert_eq!(p.tokens, tokens());
        assert!(p.pending_bindings.is_empty());
        assert!(!p.capture_view().unwrap().markets.contains_key("new"));
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn expired_public_reader_does_not_poison_current_reader_or_publication() {
        // Exercises the exact MemoryReader used by both public Rust APIs, not
        // a synthetic socket queue. A separate HTTP/WS test is still required.
        let mut p =
            test_start_with_replay(0, Some(seed()), tokens(), 2, 65536,
                2, 512 * 1024, 16384, |_| Ok(())).unwrap();
        let fast = p.reader();
        let slow = p.reader();
        let mut fast_cursor = 0;
        let mut changed = fast.subscribe();
        for at in (0..20).step_by(2) {
            p.publish_with_capacity(events(at)).await.unwrap();
            let page = fast.replay(fast_cursor, 0, 64, 65536).unwrap();
            assert_eq!(page.next, at + 2);
            fast_cursor = page.next;
            while p.persisted_cursor() < at + 2 {
                changed.changed().await.unwrap();
            }
        }
        assert!(
            slow.replay(0, 0, 64, 65536)
                .err()
                .unwrap()
                .to_string()
                .contains("expired")
        );
        assert_eq!(fast.capture().unwrap().cursor, 20);
        assert_eq!(slow.capture().unwrap().cursor, 20);
        p.publish_with_capacity(events(20)).await.unwrap();
        assert_eq!(fast.replay(20, 0, 64, 65536).unwrap().next, 22);
        assert_eq!(slow.replay(20, 0, 64, 65536).unwrap().next, 22);
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn blocked_disk_does_not_block_publication_or_memory_stream() {
        let (started, wait) = mpsc::channel();
        let (release, gate) = mpsc::channel();
        let mut first = true;
        let mut p = Publication::start_managed_with_replay(0, Some(seed()), tokens(), 4, 65536, 4, 262144, 16384, move |_| {
            if first {
                first = false;
                started.send(()).unwrap();
                gate.recv_timeout(std::time::Duration::from_secs(3))?;
            }
            Ok(())
        })
        .unwrap();
        p.publish(events(0), None).unwrap();
        wait.recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        let start = std::time::Instant::now();
        p.publish(events(2), None).unwrap();
        let stream = Stream {
            memory: p.memory.clone(),
            changed: p.changed.clone(),
            frame_cap: 65536,
            permits: Arc::new(Semaphore::new(1)),
        };
        let (page, cursor, _) = stream.page(0, 0).unwrap();
        assert!(start.elapsed() < std::time::Duration::from_millis(100));
        assert_eq!(cursor, 4);
        assert_eq!(page["persisted_cursor"], 0);
        assert_eq!(page["events"].as_array().unwrap().len(), 4);
        let (snapshot, _, _) = stream.snapshot().unwrap();
        assert_eq!(snapshot["latest_cursor"], 4);
        assert_eq!(snapshot["persisted_cursor"], 0);
        release.send(()).unwrap();
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn queued_work_coalesces_after_blocked_disk_without_losing_cursors() {
        let (entered, wait) = mpsc::channel();
        let (release, gate) = mpsc::channel();
        let counts = Arc::new(Mutex::new(Vec::new()));
        let captured = counts.clone();
        let mut first = true;
        let mut p = test_start(0, Some(seed()), tokens(), 8, 131072, 65536, move |group| {
            captured.lock().unwrap().push(
                group
                    .iter()
                    .map(|b| b.validated.events().len())
                    .sum::<usize>(),
            );
            if first {
                first = false;
                entered.send(())?;
                gate.recv_timeout(std::time::Duration::from_secs(3))?;
            }
            Ok(())
        })
        .unwrap();
        p.publish(events(0), None).unwrap();
        wait.recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        for after in [2, 4, 6] {
            p.publish(events(after), None).unwrap();
        }
        assert_eq!(p.cursor().unwrap(), 8);
        assert_eq!(p.persisted_cursor(), 0);
        release.send(()).unwrap();
        let memory = p.memory.clone();
        p.finish().await.unwrap();
        assert_eq!(*counts.lock().unwrap(), vec![2, 6]);
        let m = memory.lock().unwrap();
        assert_eq!((m.persisted, m.queued, m.queued_bytes), (8, 0, 0));
    }
    #[tokio::test]
    async fn capacity_wait_resumes_without_dropping_or_advancing_early() {
        let (release, gate) = mpsc::channel();
        let mut first = true;
        let mut p = test_start(0, Some(seed()), tokens(), 1, 65536, 16384, move |_| {
            if first {
                first = false;
                gate.recv_timeout(std::time::Duration::from_secs(3))?;
            }
            Ok(())
        })
        .unwrap();
        p.publish_with_capacity(events(0)).await.unwrap();
        let memory = p.memory.clone();
        {
            let waiting = p.publish_with_capacity(events(2));
            tokio::pin!(waiting);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiting)
                    .await
                    .is_err()
            );
            assert_eq!(memory.lock().unwrap().cursor, 2);
            release.send(()).unwrap();
            waiting.await.unwrap();
        }
        assert_eq!(memory.lock().unwrap().cursor, 4);
        p.finish().await.unwrap();
        assert_eq!(memory.lock().unwrap().persisted, 4);
    }
    #[tokio::test]
    async fn token_gap_is_streamed_without_stopping_other_books_or_snapshot() {
        use marketcow_polymarket::discovery_source::canonical_hash;
        let mut p =
            test_start(0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(())).unwrap();
        p.publish(events(0), None).unwrap();
        let mut status = events(2).remove(0);
        status["event_type"] = json!("recovery_started");
        status["canonical_payload"] = json!({"recovery_scope":"token","token_id":"11","recovery_id":"attempt1","reason":"source_data_delayed"});
        status["gaps"] = json!([{"token_id":"11","code":"coverage_gap","resolved":false,
            "detected_at":"2026-01-01T00:00:00Z","event_at":null,"expected":null,"observed":null,"resolution":null}]);
        status["canonical_payload_sha256"] = json!(canonical_hash(&status["canonical_payload"]));
        status.as_object_mut().unwrap().remove("event_id");
        status["event_id"] = json!(canonical_hash(&status));
        p.publish_with_capacity(vec![status]).await.unwrap();
        let mut healthy = events(2).remove(1);
        healthy["raw_payload"]["event_type"] = json!("book");
        healthy["raw_payload_sha256"] = json!(canonical_hash(&healthy["raw_payload"]));
        healthy.as_object_mut().unwrap().remove("event_id");
        healthy["event_id"] = json!(canonical_hash(&healthy));
        p.publish_with_capacity(vec![healthy]).await.unwrap();
        let stream = Stream {
            memory: p.memory.clone(),
            changed: p.changed.clone(),
            frame_cap: 131072,
            permits: Arc::new(Semaphore::new(1)),
        };
        let (page, cursor, _) = stream.page(2, 0).unwrap();
        assert_eq!(cursor, 4);
        assert_eq!(page["events"][0]["event_type"], "recovery_started");
        assert_eq!(page["events"][1]["token_id"], "12");
        let (state, _, _) = stream.snapshot().unwrap();
        assert_eq!(state["gaps"].as_array().unwrap().len(), 1);
        assert_eq!(state["gaps"][0]["token_id"], "11");
        assert_eq!(state["books"].as_array().unwrap().len(), 2);
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn queue_limit_includes_inflight_and_does_not_advance_cursor() {
        let (release, gate) = mpsc::channel();
        let mut p = Publication::start_managed_with_replay(0, Some(seed()), tokens(), 1, 16384, 1, 512 * 1024, 16384, move |_| {
            gate.recv_timeout(std::time::Duration::from_secs(3))?;
            Ok(())
        })
        .unwrap();
        p.publish(events(0), None).unwrap();
        assert!(
            p.publish(events(2), None)
                .unwrap_err()
                .to_string()
                .contains("capacity")
        );
        assert_eq!(p.cursor().unwrap(), 2);
        let watermark = p.reader().market_stream_watermark("1").unwrap().unwrap();
        assert_eq!((watermark.published_cursor, watermark.durable_cursor), (1, 0));
        // Global batches cannot be replayed under a native market cursor.
        assert!(p.reader().replay_market("1", &watermark.stream_epoch, 0, 2, 16384).is_err());
        release.send(()).unwrap();
        let reader = p.reader();
        p.finish().await.unwrap();
        assert_eq!(reader.market_stream_watermark("1").unwrap().unwrap().durable_cursor, 1);
    }
    #[tokio::test]
    async fn disk_failure_is_visible_and_drain_fails() {
        let mut p = test_start(0, Some(seed()), tokens(), 4, 65536, 16384, |_| {
            anyhow::bail!("injected disk failure")
        })
        .unwrap();
        let mut changed = p.changed.subscribe();
        p.publish(events(0), None).unwrap();
        loop {
            if p.memory.lock().unwrap().error.is_some() {
                break;
            }
            changed.changed().await.unwrap();
        }
        assert!(p.cursor().is_err());
        assert!(p.publish(events(2), None).is_err());
        assert!(p.finish().await.is_err());
    }

    #[tokio::test]
    async fn native_market_remains_live_only_after_physical_writer_failure() {
        let p = test_start(0, Some(seed()), tokens(), 8, 1024 * 1024, 65536,
            |_| anyhow::bail!("injected physical write failure")).unwrap();
        let reader = p.reader();
        p.publish_native_events(events(0), None).unwrap();
        let mut changed = p.changed.subscribe();
        while p.memory.lock().unwrap().error.is_none() {
            changed.changed().await.unwrap();
        }
        let before = reader.market_stream_watermark("1").unwrap().unwrap();
        assert_eq!((before.published_cursor, before.durable_cursor), (1, 0));
        p.publish_native_events(events(2), None).unwrap();
        let after = reader.market_stream_watermark("1").unwrap().unwrap();
        assert_eq!((after.published_cursor, after.durable_cursor), (2, 0));
        assert_eq!(reader.replay_market("1", &after.stream_epoch, 0, 2, 65536)
            .unwrap().groups.len(), 2);
        assert_eq!(reader.capture_market("1", &["11".into(), "12".into()])
            .unwrap().watermark.durable_cursor, 0);
        assert!(p.finish().await.is_err(), "failed archive remains explicitly failed");
    }
    #[tokio::test]
    async fn byte_budget_rejects_before_item_budget() {
        let batch = events(0);
        let cap = validate_source_events(&batch, false, 0, &tokens(), 65536)
            .unwrap()
            .iter()
            .map(Vec::len)
            .sum::<usize>();
        let (release, gate) = mpsc::channel();
        let mut p = Publication::start_managed_with_replay(0, Some(seed()), tokens(), 100, cap, 100, 512 * 1024, cap, move |_| {
            gate.recv_timeout(std::time::Duration::from_secs(3))?;
            Ok(())
        })
        .unwrap();
        p.publish(batch, None).unwrap();
        assert!(
            p.publish(events(2), None)
                .unwrap_err()
                .to_string()
                .contains("capacity")
        );
        assert_eq!(p.cursor().unwrap(), 2);
        release.send(()).unwrap();
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn corrupt_batch_never_publishes_or_reaches_disk() {
        let mut p = test_start(0, Some(seed()), tokens(), 4, 65536, 16384, |_| {
            panic!("invalid event persisted")
        })
        .unwrap();
        let mut batch = events(0);
        batch[0]["cursor"] = json!(7);
        assert!(p.publish(batch, None).is_err());
        assert_eq!(p.cursor().unwrap(), 0);
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn bounded_disk_eviction_does_not_interrupt_connected_memory_ws() {
        use marketcow_runtime::discovery_source::PreparedSourceWriter;
        use rusqlite::{Connection, params};
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("indexes")).unwrap();
        std::fs::write(root.path().join("events.jsonl"), b"").unwrap();
        let db = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        db.execute_batch("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE gaps(resolved INTEGER NOT NULL);
            CREATE TABLE books(token_id TEXT PRIMARY KEY,market_id TEXT,cursor INTEGER,payload_json BLOB,payload_sha256 TEXT);
            CREATE TABLE book_confirmations(token_id TEXT PRIMARY KEY);
            CREATE TABLE event_offsets(cursor INTEGER PRIMARY KEY,byte_offset INTEGER,byte_length INTEGER,event_id TEXT,market_id TEXT,token_id TEXT,line_sha256 TEXT);").unwrap();
        for (k, v) in [
            ("schema_version", "marketcow.polymarket.state-index.v1"),
            ("latest_cursor", "0"),
            ("event_log_size", "0"),
            ("unresolved_gap_count", "0"),
            ("catalog_revision", "catalog"),
            ("active_recovery_id", ""),
        ] {
            db.execute("INSERT INTO metadata VALUES(?,?)", [k, v])
                .unwrap();
        }
        for event in events(0) {
            let book = &event["canonical_payload"];
            let bytes = serde_json::to_vec(book).unwrap();
            db.execute(
                "INSERT INTO books VALUES(?,?,0,?,?)",
                params![
                    event["token_id"].as_str().unwrap(),
                    event["market_id"].as_str().unwrap(),
                    bytes,
                    canonical_hash(book)
                ],
            )
            .unwrap();
        }
        let mut writer = PreparedSourceWriter::open(root.path(), 16384).unwrap();
        writer.append_market_batch(&events(0)).unwrap();
        writer.enable_bounded_history(16384).unwrap();
        std::fs::remove_file(root.path().join("events.jsonl")).unwrap();
        let mut base = seed();
        base["latest_cursor"] = json!(2);
        base["persisted_cursor"] = json!(2);
        base["books"] = json!(
            events(0)
                .into_iter()
                .map(|e| e["canonical_payload"].clone())
                .collect::<Vec<_>>()
        );
        let mut p = test_start(2, Some(base), tokens(), 8, 131072, 16384, move |batches| {
            writer.append_validated_group(&batches.iter().map(|b| &b.validated).collect::<Vec<_>>())
        })
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = p.router(131072, 1).unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/"))
            .await
            .unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            json!({"type":"subscribe"}).to_string().into(),
        ))
        .await
        .unwrap();
        for _ in 0..2 {
            ws.next().await.unwrap().unwrap();
        }
        let mut cursor = 2;
        for after in (2..102).step_by(2) {
            p.publish_with_capacity(events(after)).await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while cursor < after + 2 {
                    let frame: Value =
                        serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap())
                            .unwrap();
                    assert_ne!(frame["type"], "error");
                    if let Some(items) = frame["events"].as_array() {
                        for item in items {
                            cursor += 1;
                            assert_eq!(item["cursor"], cursor);
                        }
                    }
                }
            })
            .await
            .unwrap();
        }
        p.finish().await.unwrap();
        let floor: String = db
            .query_row(
                "SELECT value FROM metadata WHERE key='history_floor_cursor'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(floor.parse::<u64>().unwrap() > 2);
        assert_eq!(cursor, 102);
        assert!(!root.path().join("events.jsonl").exists());
        assert_eq!(
            PreparedSourceWriter::open(root.path(), 16384)
                .unwrap()
                .cursor()
                .unwrap(),
            102
        );
        ws.close(None).await.unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn raw_websocket_delivers_before_blocked_disk_completes() {
        let (release, gate) = mpsc::channel();
        let mut p = test_start(0, Some(seed()), tokens(), 4, 65536, 16384, move |_| {
            gate.recv_timeout(std::time::Duration::from_secs(3))?;
            Ok(())
        })
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = p.router(65536, 1).unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/"))
            .await
            .unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            json!({"type":"subscribe"}).to_string().into(),
        ))
        .await
        .unwrap();
        for _ in 0..2 {
            ws.next().await.unwrap().unwrap();
        }
        let start = std::time::Instant::now();
        p.publish(events(0), None).unwrap();
        let frame = tokio::time::timeout(std::time::Duration::from_millis(100), async {
            loop {
                let frame: Value =
                    serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap())
                        .unwrap();
                if frame["type"] == "events" {
                    break frame;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(frame["persisted_cursor"], 0);
        assert_eq!(frame["events"][0]["cursor"], 1);
        assert_eq!(frame["events"][1]["cursor"], 2);
        marketcow_polymarket::async_eprintln!(
            "blocked-disk raw WS latency_us={} published=2 persisted=0",
            start.elapsed().as_micros()
        );
        release.send(()).unwrap();
        p.finish().await.unwrap();
        task.abort();
    }
    #[tokio::test]
    async fn matching_rest_pair_refreshes_memory_without_cursor_or_disk_event() {
        let p = test_start(0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(())).unwrap();
        p.publish_native_events(events(0), None).unwrap();
        let ids = vec!["11".to_owned(), "12".to_owned()];
        let before = p.reader().capture_market("1", &ids).unwrap();
        let raw = ids.iter().map(|token| json!({"asset_id":token,"market":"condition",
            "tick_size":"0.01","timestamp":"1700000000000","hash":"confirm",
            "bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]})).collect::<Vec<_>>();
        let requested = chrono::DateTime::from_timestamp(1_700_000_002, 0).unwrap();
        let received = chrono::DateTime::from_timestamp(1_700_000_003, 0).unwrap();
        assert_eq!(p.confirm_market("1", "condition", &[ids[0].clone(), ids[1].clone()],
            &raw, requested, received).unwrap(), ConfirmationOutcome::Confirmed);
        let after = p.reader().capture_market("1", &ids).unwrap();
        assert_eq!(after.watermark.stream_epoch, before.watermark.stream_epoch);
        assert_eq!(after.watermark.published_cursor, before.watermark.published_cursor);
        assert_eq!(after.confirmation_sequence, before.confirmation_sequence + 2);
        assert_eq!(after.books, before.books);
        assert_eq!(after.confirmation_facts.len(), 2);
        assert_eq!(p.confirm_market("1", "condition", &[ids[0].clone(), ids[1].clone()],
            &raw, requested, received).unwrap(), ConfirmationOutcome::Superseded);
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn websocket_precision_is_canonicalized_for_confirmation_boundary() {
        let p = test_start(0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(())).unwrap();
        p.publish_native_events(events(0), None).unwrap();
        let ids = vec!["11".to_owned(), "12".to_owned()];
        let raw = ids.iter().map(|token| json!({"asset_id":token,"market":"condition",
            "tick_size":"0.01","timestamp":"1700000000000","hash":"confirm",
            "bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]})).collect::<Vec<_>>();
        let requested = chrono::DateTime::from_timestamp(1_700_000_002, 123_456_789).unwrap();
        let received = chrono::DateTime::from_timestamp(1_700_000_003, 987_654_321).unwrap();
        assert_eq!(p.confirm_market("1", "condition", &[ids[0].clone(), ids[1].clone()],
            &raw, requested, received).unwrap(), ConfirmationOutcome::Confirmed);
        let after = p.reader().capture_market("1", &ids).unwrap();
        assert_eq!(after.confirmation_facts["11"]["confirmed_at"],
            "2023-11-14T22:13:23.987654Z");
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn reduced_ws_confirmation_advances_only_memory_confirmation_fence() {
        let p = test_start(0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(())).unwrap();
        p.publish_native_events(events(0), None).unwrap();
        let ids = vec!["11".to_owned(), "12".to_owned()];
        let before = p.reader().capture_market("1", &ids).unwrap();
        let at = chrono::DateTime::from_timestamp(1_700_000_010, 123_456_789).unwrap();
        assert!(p.confirm_reduced_token("1", "condition", "11",
            before.books["11"]["state_checksum"].as_str().unwrap(), at, at,
            "fresh-observation-hash").unwrap());
        let after = p.reader().capture_market("1", &ids).unwrap();
        assert_eq!(after.watermark.stream_epoch, before.watermark.stream_epoch);
        assert_eq!(after.watermark.published_cursor, before.watermark.published_cursor);
        assert_eq!(after.books, before.books);
        assert_eq!(after.confirmation_sequence, before.confirmation_sequence + 1);
        assert_eq!(after.confirmation_facts["11"]["confirmed_at"],
            "2023-11-14T22:13:30.123456Z");
        assert_eq!(after.confirmation_facts["11"]["confirmation_source"],
            "polymarket_websocket_state_confirmation");
        p.finish().await.unwrap();
    }
    #[test]
    fn only_full_book_mutations_advance_the_confirmation_fence() {
        assert!(advances_book_confirmation_fence(Some("book")));
        assert!(advances_book_confirmation_fence(Some("price_change")));
        assert!(advances_book_confirmation_fence(Some("tick_size_change")));
        assert!(!advances_book_confirmation_fence(Some("best_bid_ask")));
        assert!(!advances_book_confirmation_fence(Some("last_trade_price")));
    }
    #[tokio::test]
    async fn newer_causal_book_state_invalidates_the_older_freshness_confirmation() {
        let p = test_start(0, Some(seed()), tokens(), 8, 1024 * 1024, 65536, |_| Ok(())).unwrap();
        p.publish_native_events(events(0), None).unwrap();
        let ids = vec!["11".to_owned(), "12".to_owned()];
        let before = p.reader().capture_market("1", &ids).unwrap();
        let at = chrono::DateTime::from_timestamp(1_700_000_010, 0).unwrap();
        assert!(p.confirm_reduced_token("1", "condition", "11",
            before.books["11"]["state_checksum"].as_str().unwrap(), at, at,
            "fresh-observation-hash").unwrap());
        assert_eq!(p.reader().capture_market("1", &ids).unwrap().confirmation_facts.len(), 1);
        p.publish_native_events(events(2), None).unwrap();
        let after = p.reader().capture_market("1", &ids).unwrap();
        assert!(after.confirmation_facts.is_empty());
        assert!(after.market_confirmation_versions.is_empty());
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn replay_eviction_requires_resync_instead_of_skipping() {
        let mut p =
            Publication::start_managed_with_replay(0, Some(seed()), tokens(), 1, 16384, 1, 512 * 1024, 16384, |_| Ok(())).unwrap();
        p.publish(events(0), None).unwrap();
        let mut changed = p.changed.subscribe();
        while p.persisted_cursor() < 2 {
            changed.changed().await.unwrap();
        }
        p.publish(events(2), None).unwrap();
        let stream = Stream {
            memory: p.memory.clone(),
            changed: p.changed.clone(),
            frame_cap: 65536,
            permits: Arc::new(Semaphore::new(1)),
        };
        assert!(
            stream
                .page(0, 0)
                .unwrap_err()
                .to_string()
                .contains("resync")
        );
        assert_eq!(stream.page(2, 0).unwrap().1, 4);
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn replay_failure_diagnostics_use_progressed_client_cursor() {
        let mut p = test_start_with_replay(
            0, Some(seed()), tokens(), 1, 65536,
            1, 512 * 1024, 16384, |_| Ok(()),
        ).unwrap();
        let reader = p.reader();
        let mut changed = reader.subscribe();
        p.publish(events(0), None).unwrap();
        while p.persisted_cursor() < 2 {
            changed.changed().await.unwrap();
        }
        // The connection started at zero, then actually applied through two.
        let progressed = reader.replay_with_diagnostics(0, 0, 64, 65536).unwrap().next;
        assert_eq!(progressed, 2);
        for at in [2, 4] {
            p.publish(events(at), None).unwrap();
            while p.persisted_cursor() < at + 2 {
                changed.changed().await.unwrap();
            }
        }
        let error = reader.replay_with_diagnostics(progressed, 0, 64, 65536)
            .err().expect("progressed client must expire");
        let detail = format!("{error:#}");
        assert!(detail.contains("replay after cursor 2;"));
        assert!(detail.contains("\"after_cursor\":2"));
        assert!(detail.contains("\"oldest_cursor\":5"));
        assert!(detail.contains("\"current_cursor\":6"));
        assert!(detail.contains("memory replay expired"));
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn replay_retention_is_independent_of_persistence_queue_capacity() {
        let mut p = Publication::start_managed_with_replay(
            0, Some(seed()), tokens(), 1, 16384, 8, 512 * 1024, 16384,
            |work| match work {
                PersistenceWork::Events(_) => Ok(()),
                PersistenceWork::Retire(_) => unreachable!(),
            },
        ).unwrap();
        let reader = p.reader();
        let mut changed = reader.subscribe();
        for at in (0..16).step_by(2) {
            p.publish(events(at), None).unwrap();
            while p.persisted_cursor() < at + 2 {
                changed.changed().await.unwrap();
            }
        }
        let page = reader.replay(0, 0, 64, 65536).unwrap();
        assert_eq!(page.next, 16);
        assert_eq!(page.batches.len(), 8);
        let window = reader.replay_window(0).unwrap();
        assert_eq!(window["retained_batches"], 8);
        assert_eq!(window["oldest_cursor"], 1);
        assert_eq!(window["current_cursor"], 16);
        let memory = p.memory.lock().unwrap();
        assert!(memory.replay_bytes <= 512 * 1024);
        assert_eq!(memory.queued, 0);
        drop(memory);
        p.publish(events(16), None).unwrap();
        assert!(reader.replay(0, 0, 64, 65536).is_err());
        assert_eq!(reader.replay(2, 0, 64, 65536).unwrap().next, 18);
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn high_churn_replay_remains_contiguous_for_a_slow_reader() {
        // The reader remains at zero while more batches than the persistence
        // queue can hold pass through the writer. It then catches up through
        // bounded 64-batch pages without skipping a cursor.
        let mut p = Publication::start_managed_with_replay(
            0, Some(seed()), tokens(), 1, 16384, 1024, 64 * 1024 * 1024, 16384,
            |work| match work {
                PersistenceWork::Events(_) => Ok(()),
                PersistenceWork::Retire(_) => unreachable!(),
            },
        ).unwrap();
        let reader = p.reader();
        let mut changed = reader.subscribe();
        for at in (0..1024).step_by(2) {
            p.publish(events(at), None).unwrap();
            while p.persisted_cursor() < at + 2 {
                changed.changed().await.unwrap();
            }
        }
        let mut cursor = 0;
        let mut pages = 0;
        while cursor < 1024 {
            let page = reader.replay(cursor, 0, 64, 1024 * 1024).unwrap();
            assert!(page.next > cursor);
            cursor = page.next;
            pages += 1;
        }
        assert_eq!(cursor, 1024);
        assert_eq!(pages, 8);
        assert!(p.memory.lock().unwrap().replay_bytes <= 64 * 1024 * 1024);
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn parsed_replay_budget_evicts_both_arc_owners_after_durability() {
        let mut p = Publication::start_managed_with_replay(
            0, Some(seed()), tokens(), 8, 131072, 100, 512 * 1024, 16384,
            |_| Ok(()),
        ).unwrap();
        let mut changed = p.changed.subscribe();
        p.publish(events(0), None).unwrap();
        let first = {
            let memory = p.memory.lock().unwrap();
            let batch = memory.replay.front().unwrap();
            assert!(batch.replay_retained_bytes > batch.bytes);
            Arc::downgrade(batch)
        };
        for after in (2..100).step_by(2) {
            while p.persisted_cursor() < after { changed.changed().await.unwrap(); }
            p.publish(events(after), None).unwrap();
            assert!(p.memory.lock().unwrap().replay_bytes <= 512 * 1024);
            assert!(p.market_replay.total_bytes() <= 512 * 1024);
        }
        while p.persisted_cursor() < 100 { changed.changed().await.unwrap(); }
        assert!(first.upgrade().is_none(), "neither replay nor writer may retain the evicted batch");
        assert!(p.reader().replay(0, 0, 64, 65536).is_err());
        assert_eq!(p.cursor().unwrap(), 100);
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn larger_replay_does_not_enlarge_blocked_persistence_queue() {
        let (started, wait) = mpsc::channel();
        let (release, gate) = mpsc::channel();
        let mut p = Publication::start_managed_with_replay(
            0, Some(seed()), tokens(), 1, 16384, 64, 131072, 16384,
            move |work| match work {
                PersistenceWork::Events(_) => {
                    started.send(()).unwrap();
                    gate.recv().unwrap();
                    Ok(())
                }
                PersistenceWork::Retire(_) => unreachable!(),
            },
        ).unwrap();
        p.publish(events(0), None).unwrap();
        wait.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert!(p.publish(events(2), None).is_err());
        assert_eq!(p.cursor().unwrap(), 2);
        release.send(()).unwrap();
        p.finish().await.unwrap();
    }

    #[test]
    fn independent_replay_limits_must_be_explicitly_bounded() {
        let invalid = Publication::start_managed_with_replay(
            0, Some(seed()), tokens(), 1, 16384, 0, 65536, 16384,
            |_| Ok(()),
        );
        assert!(invalid.is_err());
        let invalid = Publication::start_managed_with_replay(
            0, Some(seed()), tokens(), 1, 16384, 8, 16383, 16384,
            |_| Ok(()),
        );
        assert!(invalid.is_err());
    }

    #[tokio::test]
    async fn snapshot_lease_survives_count_eviction_then_expires_without_blocking_source() {
        let mut p =
            Publication::start_managed_with_replay(0, Some(seed()), tokens(), 1, 65536, 1, 262144, 16384, |_| Ok(())).unwrap();
        let reader = p.reader();
        assert_eq!(
            reader
                .capture_for_resume(std::time::Duration::from_secs(5), 1)
                .unwrap()
                .cursor,
            0
        );
        let mut changed = p.changed.subscribe();
        for at in [0, 2] {
            p.publish(events(at), None).unwrap();
            while p.persisted_cursor() < at + 2 {
                changed.changed().await.unwrap();
            }
        }
        assert_eq!(reader.replay(0, 0, 64, 65536).unwrap().next, 4);
        assert!(
            reader
                .capture_for_resume(std::time::Duration::from_secs(5), 1)
                .is_err()
        );
        p.memory
            .lock()
            .unwrap()
            .snapshot_leases
            .insert(0, std::time::Instant::now());
        p.publish(events(4), None).unwrap();
        assert!(reader.replay(0, 0, 64, 65536).is_err());
        assert_eq!(reader.replay(4, 0, 64, 65536).unwrap().next, 6);
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn snapshot_lease_never_exceeds_existing_byte_cap() {
        let mut p =
            Publication::start_managed_with_replay(0, Some(seed()), tokens(), 1, 16384, 1, 128 * 1024, 16384, |_| Ok(())).unwrap();
        let reader = p.reader();
        reader
            .capture_for_resume(std::time::Duration::from_secs(60), 1)
            .unwrap();
        let mut changed = p.changed.subscribe();
        for at in (0..24).step_by(2) {
            p.publish(events(at), None).unwrap();
            while p.persisted_cursor() < at + 2 {
                changed.changed().await.unwrap();
            }
            assert!(p.memory.lock().unwrap().replay_bytes <= 128 * 1024);
        }
        assert!(reader.replay(0, 0, 64, 16384).is_err());
        assert_eq!(reader.capture().unwrap().cursor, 24);
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_audit_does_not_reenable_public_baseline() {
        let p = test_start(0, Some(seed()), tokens(), 4, 65536, 16384, |_| Ok(())).unwrap();
        p.set_source_ready(false);
        assert!(p.capture_view().is_err());
        assert!(p.shutdown_recovery_facts().unwrap().0.values().all(|cursor| *cursor == 0));
        assert!(p.reader().capture().is_err());
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn intentional_pause_fences_old_resume_without_creating_a_gap() {
        let p =
            test_start(0, Some(seed()), tokens(), 4, 1024 * 1024, 512 * 1024, |_| Ok(())).unwrap();
        p.publish_native_events(events(0), None).unwrap();
        let reader = p.reader();
        let market = reader.market_stream_watermark("1").unwrap().unwrap();
        assert!(reader.replay_market("1", &market.stream_epoch, 0, 1, 16384).is_ok());
        assert!(p.fresh_acquisition_started(&BTreeSet::from(["11".into(), "12".into()])));
        p.pause_source().unwrap();
        assert!(!p.fresh_acquisition_started(&BTreeSet::from(["11".into(), "12".into()])));
        assert!(reader.installed_markets().is_ok());
        assert!(reader.replay_market("1", &market.stream_epoch, 0, 1, 16384).is_err());
        assert!(p.shutdown_recovery_facts().unwrap().1.is_empty());
        p.resume_acquisition();
        assert!(!p.is_source_ready());
        p.publish_native_events(events(2), None).unwrap();
        assert!(p.fresh_acquisition_started(&BTreeSet::from(["11".into(), "12".into()])));
        p.set_source_ready(true);
        assert!(reader.replay_market("1", &market.stream_epoch, 0, 1, 16384).is_err());
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn continuous_confirmations_cannot_starve_pending_events() {
        let mut bindings = tokens();
        bindings.insert("13".into(), "2".into());
        bindings.insert("14".into(), "2".into());
        let mut p =
            test_start(0, Some(seed_with_second_market("13", "14")), bindings, 8, 131072, 16384, |_| Ok(())).unwrap();
        p.publish(events(0), None).unwrap();
        let other = ["13","14"].iter().enumerate().map(|(i,token)| snapshot_event(
            &json!({"asset_id":token,"market":"condition2","tick_size":"0.01","timestamp":"1700000000000",
                "hash":"source","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
            SnapshotBoundary{market_id:"2",condition_id:"condition2",token_id:token,recovery_id:"test",
                cursor:3+i as u64,received_at:chrono::DateTime::from_timestamp(1700000001,0).unwrap()}).unwrap()).collect();
        p.publish(other, None).unwrap();
        // Synthetic confirmation on unchanged token12 is causally eligible at2,
        // while another market's atomic pair is pending. No live evidence claim.
        {
            let mut memory = p.memory.lock().unwrap();
            let fact = memory.books["12"].clone();
            memory.confirmations.insert("12".into(), 1);
            memory.confirmation_facts.insert("12".into(), fact);
        }
        let stream = Stream {
            memory: p.memory.clone(),
            changed: p.changed.clone(),
            frame_cap: 65536,
            permits: Arc::new(Semaphore::new(1)),
        };
        let (confirmation, after, c) = stream.page_with_priority(2, 0, false).unwrap();
        assert_eq!(confirmation["type"], "book_confirmations");
        assert_eq!(after, 2);
        {
            let mut memory = p.memory.lock().unwrap();
            let fact = memory.books["12"].clone();
            memory.confirmations.insert("12".into(), 2);
            memory.confirmation_facts.insert("12".into(), fact);
        }
        let (event, after, c2) = stream.page_with_priority(after, c, true).unwrap();
        assert_eq!(event["type"], "events");
        assert_eq!(after, 4);
        assert_eq!(c2, c);
        assert_eq!(
            stream.page_with_priority(after, c2, false).unwrap().0["type"],
            "book_confirmations"
        );
        let window = stream.window(2);
        assert_eq!(window["current_cursor"], 4);
        assert_eq!(window["retained_batches"], 2);
        p.finish().await.unwrap();
    }
}
