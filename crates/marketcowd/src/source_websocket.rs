//! Public Polymarket WS -> market-local actors -> market-local publication.
//! Physical archive positions and each market's published/durable watermarks
//! remain separate; actor reduction has no synchronous disk side effects.
use super::{Args, Plan};
use crate::source_publication::{ConfirmationOutcome, Publication};
use anyhow::{Context, Result, ensure};
use chrono::{SecondsFormat, Timelike, Utc};
use marketcow_core::{CanonicalEvent, CoreError, DurableLog, EventKind, Price, SingleWriter};
use marketcow_polymarket::discovery_source::{SnapshotBoundary, canonical_hash, snapshot_event};
use marketcow_polymarket::{
    NormalizerConfig, PolymarketTransportConfig, normalize_frame_with_book_tick,
    RawTransportBatch, RawTransportFrame, run_polymarket_transport, transport_budget,
    transport_output,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::sync::Arc;
#[path = "source_websocket_pipeline.rs"]
mod pipeline;

// Every market socket owns a 2048-item/32-MiB FIFO. All socket FIFOs also share
// these hard count/byte ceilings; more markets never multiply total retention.
const TRANSPORT_QUEUE_BATCHES: usize = 4096;
const TRANSPORT_QUEUE_BYTES: usize = 64 * 1024 * 1024;
const TRANSPORT_BATCH_BYTES: usize = 8 * 1024 * 1024;
const OWNER_COALESCE_BATCHES: usize = 64;
const OWNER_COALESCE_BYTES: usize = 4 * 1024 * 1024;
const OWNER_COALESCE_FRAMES: usize = 64;
// Receipt-time window, not a timer: an idle actor never waits for another
// venue frame. Control, top-of-book, recovery and trade boundaries flush it.
const DEPTH_CONFLATION_WINDOW_US: i64 = 2_000;

fn split_transport_control_boundaries(frames: Vec<RawTransportFrame>) -> Vec<Vec<RawTransportFrame>> {
    let mut segments = Vec::new();
    let mut current = Vec::new();
    let mut current_is_gap = None;
    for frame in frames {
        let is_gap = frame.raw_payload["event_type"] == "source_gap";
        if current.len() == OWNER_COALESCE_FRAMES
            || current_is_gap.is_some_and(|prior| prior != is_gap)
        {
            segments.push(std::mem::take(&mut current));
        }
        current_is_gap = Some(is_gap);
        current.push(frame);
    }
    if !current.is_empty() {
        segments.push(current);
    }
    segments
}

#[cfg(test)]
pub(crate) fn compact_btc5m_fixture_events(events: Vec<Value>) -> Result<Vec<Value>> {
    Ok(pipeline::compact_incremental_events(events)?.0)
}

struct MemoryReducer;
#[derive(Debug)]
struct ResyncRequired {
    token: String,
    observed: chrono::DateTime<Utc>,
    received: chrono::DateTime<Utc>,
}
impl std::fmt::Display for ResyncRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "source_data_delayed token={} observed={} received={}; fresh WS baseline required",
            self.token, self.observed, self.received
        )
    }
}
impl std::error::Error for ResyncRequired {}
impl DurableLog for MemoryReducer {
    fn append(&mut self, _: &CanonicalEvent) -> std::result::Result<(), CoreError> {
        Ok(())
    }
}
struct Adapter {
    config: NormalizerConfig,
    identities: BTreeMap<String, (String, String)>,
    ticks: BTreeMap<String, Price>,
    engines: BTreeMap<String, SingleWriter<MemoryReducer>>,
    epochs: BTreeMap<String, String>,
    sequences: BTreeMap<String, u64>,
    waiting: BTreeSet<String>,
    book_received: BTreeMap<String, chrono::DateTime<Utc>>,
    book_source: BTreeMap<String, chrono::DateTime<Utc>>,
    // The venue often repeats an already-applied price level with only a new
    // transport timestamp. That is not a new book state or freshness evidence.
    depth_checksums: BTreeMap<String, String>,
    // Complete Polymarket book frames carry this fact even though the generic
    // reducer represents later trade ticks as separate events.
    last_trades: BTreeMap<String, Price>,
    recoveries: BTreeMap<String, String>,
    // A new, validated venue observation can reduce to the exact depth that
    // is already public.  It is not a causal state mutation and therefore
    // must not consume a public cursor or enter durability, but it is valid
    // freshness evidence.  The market actor hands these receipts to the
    // publication owner after all state-mutating events from the same input.
    // Each receipt retains the exact book identity it observed; identical
    // depth after a later trade is not permission to confirm the newer book.
    confirmations: Vec<ReducedBookConfirmation>,
    // A conflated frame has one public identity for several immutable venue
    // frames. Retain their raw identities for the reducer's duplicate window;
    // an exact venue replay must not become a new freshness confirmation.
    conflated_raw_hashes: HashSet<String>,
    conflated_raw_order: VecDeque<String>,
}
#[derive(Debug)]
struct ReducedBookConfirmation {
    market_id: String,
    condition_id: String,
    token_id: String,
    book_epoch: String,
    book_sequence: u64,
    book_received_at: chrono::DateTime<Utc>,
    state_checksum: String,
    exchange_at: chrono::DateTime<Utc>,
    received_at: chrono::DateTime<Utc>,
    source_hash: String,
}
struct DepthConflationCandidate {
    market_id: String,
    condition_id: String,
    observed_at: chrono::DateTime<Utc>,
    tokens: BTreeMap<String, DepthTokenConflation>,
}
struct DepthTokenConflation {
    best_bid: Price,
    best_ask: Price,
    changes: BTreeMap<(String, String), Value>,
}
struct ScopeAcquisition {
    validate_only: bool,
    catalog_revision: String,
    records: Vec<Value>,
    evidence_sha256: String,
    acquisition_market_ids: BTreeSet<String>,
    retire_market_ids: BTreeSet<String>,
}
impl Adapter {
    /// The owner has already bound this exact identity to catalog metadata.
    /// New reducers start without tick/book/freshness state; snapshots must
    /// supply those facts. Retained pairs are never reset by admission.
    fn admit_market(&mut self, market: &super::Market) -> Result<bool> {
        ensure!(
            !market.market_id.is_empty()
                && !market.condition_id.is_empty()
                && market.token_ids[0] != market.token_ids[1]
                && market
                    .token_ids
                    .iter()
                    .all(|token| !token.is_empty() && token.len() <= 128),
            "market admission identity"
        );
        let expected = (market.market_id.clone(), market.condition_id.clone());
        let existing = market
            .token_ids
            .iter()
            .filter(|token| self.identities.contains_key(*token))
            .count();
        if existing > 0 {
            ensure!(
                existing == 2
                    && market
                        .token_ids
                        .iter()
                        .all(|token| self.identities.get(token) == Some(&expected)),
                "market admission ownership conflict"
            );
            return Ok(false);
        }
        ensure!(
            !self
                .identities
                .values()
                .any(|(id, _)| id == &market.market_id),
            "market admission changed token pair"
        );
        ensure!(
            self.identities.len() + 2
                <= marketcow_runtime::discovery_source::MAX_SOURCE_TOKEN_IDENTITIES,
            "market admission token capacity"
        );
        for token in &market.token_ids {
            self.identities.insert(token.clone(), expected.clone());
            self.waiting.insert(token.clone());
        }
        Ok(true)
    }
    fn market_actor(&self, market: &str) -> Self {
        let identities: BTreeMap<_, _> = self
            .identities
            .iter()
            .filter(|(_, (m, _))| m == market)
            .map(|(t, identity)| (t.clone(), identity.clone()))
            .collect();
        Self {
            config: self.config.clone(),
            ticks: self
                .ticks
                .iter()
                .filter(|(t, _)| identities.contains_key(*t))
                .map(|(t, v)| (t.clone(), v.clone()))
                .collect(),
            waiting: identities.keys().cloned().collect(),
            recoveries: self
                .recoveries
                .iter()
                .filter(|(t, _)| identities.contains_key(*t))
                .map(|(t, v)| (t.clone(), v.clone()))
                .collect(),
            identities,
            engines: BTreeMap::new(),
            epochs: BTreeMap::new(),
            sequences: BTreeMap::new(),
            book_received: BTreeMap::new(),
            book_source: BTreeMap::new(),
            depth_checksums: BTreeMap::new(),
            last_trades: BTreeMap::new(),
            confirmations: Vec::new(),
            conflated_raw_hashes: HashSet::new(),
            conflated_raw_order: VecDeque::new(),
        }
    }
    fn reset_connection(&mut self) {
        // Discard the entire connection's reducer state, including any partial
        // multi-token frame. Keep verified tick metadata, never stale books.
        self.waiting = self.identities.keys().cloned().collect();
        self.engines.clear();
        self.epochs.clear();
        self.sequences.clear();
        self.book_received.clear();
        self.book_source.clear();
        self.depth_checksums.clear();
        self.last_trades.clear();
        self.confirmations.clear();
        self.conflated_raw_hashes.clear();
        self.conflated_raw_order.clear();
    }
    fn new(plan: &Plan, seed: &Value) -> Result<Self> {
        let market_states: BTreeMap<_, _> = seed["markets"]
            .as_array()
            .context("market seed")?
            .iter()
            .map(|m| {
                Ok((
                    m["identity"]["market_id"]
                        .as_str()
                        .context("market id")?
                        .to_owned(),
                    m["lifecycle_state"]
                        .as_str()
                        .context("market lifecycle state")?
                        .to_owned(),
                ))
            })
            .collect::<Result<_>>()?;
        let terminal_tokens: BTreeSet<_> = seed["markets"]
            .as_array()
            .context("market seed")?
            .iter()
            .filter(|market| {
                market["lifecycle_state"] == "closed" || market["lifecycle_state"] == "resolved"
            })
            .flat_map(|market| {
                market["identity"]["outcomes"]
                    .as_array()
                    .into_iter()
                    .flatten()
            })
            .filter_map(|outcome| outcome["token_id"].as_str())
            .collect();
        let active: BTreeSet<_> = market_states
            .iter()
            .filter(|(_, state)| state.as_str() == "active")
            .map(|(market, _)| market.as_str())
            .collect();
        let mut identities = BTreeMap::new();
        for market in &plan.markets {
            if active.contains(market.market_id.as_str()) {
                for token in &market.token_ids {
                    identities.insert(
                        token.clone(),
                        (market.market_id.clone(), market.condition_id.clone()),
                    );
                }
            }
        }
        ensure!(
            !identities.is_empty()
                && identities.len()
                    <= marketcow_runtime::discovery_source::MAX_SOURCE_TOKEN_IDENTITIES,
            "bounded active WS universe required"
        );
        let mut ticks = BTreeMap::new();
        for book in seed["books"].as_array().context("book seed")? {
            let token = book["token_id"].as_str().context("token")?;
            if identities.contains_key(token) {
                ticks.insert(
                    token.into(),
                    Price::parse_tick(
                        book["tick_size"]
                            .as_str()
                            .context("verified tick metadata missing")?,
                    )?,
                );
            }
        }
        // A cold candidate can lack authoritative tick metadata. Keep those
        // tokens waiting; normalization still refuses a book without a verified
        // tick, and the existing token-local recovery/REST snapshot path supplies
        // it. Missing metadata must not prevent unrelated markets from starting.
        let recoveries = seed
            .get("token_recoveries")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|recovery| {
                let token = recovery["token_id"]
                    .as_str()
                    .context("recovery token")?
                    .to_owned();
                // A terminal lifecycle overlay is authoritative for the market
                // and retires its books. Older generations may still contain
                // the pre-terminal coverage recovery row; preserve that row in
                // durable history, but do not let it poison the active WS
                // identity set or force a recovery for a retired token.
                if !identities.contains_key(&token) {
                    let market = recovery["market_id"].as_str().context("recovery market")?;
                    ensure!(
                        market_states
                            .get(market)
                            .is_some_and(|state| state == "closed" || state == "resolved"),
                        "recovery token outside scope"
                    );
                    ensure!(
                        terminal_tokens.contains(token.as_str()),
                        "recovery token outside scope"
                    );
                    return Ok(None);
                }
                Ok(Some((
                    token,
                    recovery["recovery_id"]
                        .as_str()
                        .context("recovery id")?
                        .to_owned(),
                )))
            })
            .filter_map(|item| item.transpose())
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            config: NormalizerConfig::new(
                seed["scope_id"].as_str().context("scope id")?,
                &plan.catalog_revision,
            ),
            waiting: identities.keys().cloned().collect(),
            identities,
            ticks,
            engines: BTreeMap::new(),
            epochs: BTreeMap::new(),
            sequences: BTreeMap::new(),
            book_received: BTreeMap::new(),
            book_source: BTreeMap::new(),
            depth_checksums: BTreeMap::new(),
            last_trades: BTreeMap::new(),
            recoveries,
            confirmations: Vec::new(),
            conflated_raw_hashes: HashSet::new(),
            conflated_raw_order: VecDeque::new(),
        })
    }
    fn invalidate(
        &mut self,
        token: &str,
        reason: &str,
        received: chrono::DateTime<Utc>,
        cursor: u64,
    ) -> Result<Value> {
        let (market, condition) = self
            .identities
            .get(token)
            .context("unknown recovery token")?;
        let at = wire_time(received)?;
        let attempt = canonical_hash(&json!({"token_id":token,"reason":reason,
            "received_at":at,"publisher_cursor":cursor}));
        let payload = json!({"recovery_scope":"token","token_id":token,"recovery_id":attempt,"reason":reason});
        let gap = json!({"token_id":token,"code":"coverage_gap","resolved":false,
            "detected_at":at,"event_at":null,"expected":null,"observed":null,"resolution":null});
        let event = recovery_event(
            market,
            condition,
            token,
            cursor,
            &at,
            "recovery_started",
            payload,
            vec![gap],
        );
        self.recoveries.insert(token.into(), attempt);
        self.waiting.insert(token.into());
        self.engines.remove(token);
        self.epochs.remove(token);
        self.sequences.remove(token);
        self.book_received.remove(token);
        self.book_source.remove(token);
        self.depth_checksums.remove(token);
        self.last_trades.remove(token);
        Ok(event)
    }

    /// A reconnecting shard can report the same connection boundary more than
    /// once before its authoritative snapshot arrives. The first gap already
    /// invalidated the book; replacing its recovery identity would make the
    /// in-flight snapshot stale and create an unbounded recovery storm.
    fn invalidate_once(
        &mut self,
        token: &str,
        reason: &str,
        received: chrono::DateTime<Utc>,
        cursor: u64,
    ) -> Result<Option<Value>> {
        if self.recoveries.contains_key(token) {
            return Ok(None);
        }
        self.invalidate(token, reason, received, cursor).map(Some)
    }

    // This check is deliberately conservative. It normalizes every supplier
    // frame before considering a replacement and never conflates a top-level,
    // BBO or recovery observation. All tokens in one supplier frame remain
    // together; any uncertainty falls through
    // to the original one-frame reducer and its fail-closed behavior.
    fn depth_conflation_candidate(
        &self,
        frame: &RawTransportFrame,
        require_live_state: bool,
    ) -> Option<DepthConflationCandidate> {
        let raw = &frame.raw_payload;
        if raw["event_type"] != "price_change" || raw.get("marketcow_conflation").is_some() {
            return None;
        }
        let source_changes = raw["price_changes"].as_array()?;
        if source_changes.iter().any(|change| {
            let price = &change["price"];
            change["best_bid"].is_null() || change["best_ask"].is_null()
                || price == &change["best_bid"] || price == &change["best_ask"]
        }) {
            return None;
        }
        let normalized = normalize_frame_with_book_tick(
            &self.config, raw.clone(), frame.received_at, 1, None,
        ).ok()?;
        if normalized.is_empty() || normalized.iter().any(|event| event.source.delayed) {
            return None;
        }
        if require_live_state && normalized.iter().any(|event|
            self.conflated_raw_hashes.contains(&event.source.raw_sha256)) {
            return None;
        }
        let mut market_id = None;
        let mut condition_id = None;
        let mut tokens = BTreeMap::new();
        for event in &normalized {
            let EventKind::AtomicDelta {
                token_id, changes, best_bid: Some(best_bid), best_ask: Some(best_ask),
            } = &event.kind else { return None; };
            if require_live_state && self.waiting.contains(token_id) {
                return None;
            }
            let (market, condition) = self.identities.get(token_id)?;
            if raw["market"].as_str() != Some(condition.as_str()) || market.is_empty()
                || market_id.as_ref().is_some_and(|id| id != market)
                || condition_id.as_ref().is_some_and(|id| id != condition) {
                return None;
            }
            market_id = Some(market.clone());
            condition_id = Some(condition.clone());
            if require_live_state {
                let projection = self.engines.get(token_id)?.projection();
                let book = projection.books.get(token_id)?;
                if book.source_observed_at.is_some_and(|at| event.source_observed_at <= at) {
                    return None;
                }
                // Deep-level replacement is safe only when the reducer has
                // the same *previous* two-sided BBO as the supplier frame.
                // A one-sided book (or a changed top) must retain every raw
                // frame so downstream delta preimages remain reconstructible.
                if book.bids.keys().next_back() != Some(best_bid)
                    || book.asks.keys().next() != Some(best_ask) {
                    return None;
                }
            }
            let tick = self.ticks.get(token_id)?;
            let mut final_changes = BTreeMap::new();
            for change in changes {
                let side = match change.side {
                    marketcow_core::Side::Bid => "BUY",
                    marketcow_core::Side::Ask => "SELL",
                };
                for level in &change.levels {
                    let strictly_below_top = change.side == marketcow_core::Side::Bid
                        && level.price.0 < best_bid.0;
                    let strictly_above_top = change.side == marketcow_core::Side::Ask
                        && level.price.0 > best_ask.0;
                    if !(strictly_below_top || strictly_above_top)
                        || !(level.price.0 % tick.0).is_zero() {
                        return None;
                    }
                    let price = level.price.0.to_string();
                    final_changes.insert((side.to_owned(), price.clone()), json!({
                        "asset_id":token_id,
                        "side":side,
                        "price":price,
                        "size":level.quantity.to_string(),
                        "best_bid":best_bid.0.to_string(),
                        "best_ask":best_ask.0.to_string(),
                    }));
                }
            }
            if final_changes.is_empty() { return None; }
            tokens.insert(token_id.clone(), DepthTokenConflation {
                best_bid: best_bid.clone(), best_ask: best_ask.clone(), changes: final_changes,
            });
        }
        Some(DepthConflationCandidate {
            market_id: market_id?, condition_id: condition_id?,
            observed_at: normalized[0].source_observed_at, tokens,
        })
    }

    fn flush_depth_conflation(
        pending: &mut Vec<RawTransportFrame>,
        candidate: &mut Option<DepthConflationCandidate>,
        output: &mut Vec<RawTransportFrame>,
    ) {
        let Some(info) = candidate.take() else { return; };
        if pending.len() < 2 {
            output.append(pending);
            return;
        }
        // The aggregate carries the originals as immutable evidence. Never
        // turn an otherwise admissible input into an oversized public event.
        if serde_json::to_vec(&*pending).map_or(true, |bytes| bytes.len() > 4 * 1024 * 1024) {
            output.append(pending);
            return;
        }
        let first_received_at = pending[0].received_at;
        let last = pending.last().expect("nonempty conflation");
        let last_received_at = last.received_at;
        let timestamp = last.raw_payload.get("timestamp").cloned()
            .unwrap_or_else(|| json!(wire_time(last_received_at).expect("valid receipt")));
        let source_frames = std::mem::take(pending).into_iter().map(|frame| json!({
            "received_at":wire_time(frame.received_at).expect("valid receipt"),
            "raw_payload":frame.raw_payload,
        })).collect::<Vec<_>>();
        let source_frame_count = source_frames.len();
        output.push(RawTransportFrame {
            raw_payload: json!({
                "event_type":"price_change",
                "market":info.condition_id,
                "timestamp":timestamp,
                "price_changes":info.tokens.values().flat_map(|token|
                    token.changes.values().cloned()).collect::<Vec<_>>(),
                "marketcow_conflation":{
                    "schema_version":"marketcow.polymarket.depth-conflation.v2",
                    "market_id":info.market_id,
                    "token_ids":info.tokens.keys().collect::<Vec<_>>(),
                    "first_received_at":wire_time(first_received_at).expect("valid receipt"),
                    "last_received_at":wire_time(last_received_at).expect("valid receipt"),
                    "source_frame_count":source_frame_count,
                    "source_frames":source_frames,
                },
            }),
            received_at:last_received_at,
        });
    }

    fn conflate_depth_frames(&self, frames: Vec<RawTransportFrame>, require_live_state: bool) -> Vec<RawTransportFrame> {
        if frames.len() < 2 { return frames; }
        let mut output = Vec::with_capacity(frames.len());
        let mut pending: Vec<RawTransportFrame> = Vec::new();
        let mut candidate: Option<DepthConflationCandidate> = None;
        for frame in frames {
            let next = self.depth_conflation_candidate(&frame, require_live_state);
            let compatible = candidate.as_ref().zip(next.as_ref()).is_some_and(|(old, new)| {
                old.market_id == new.market_id
                    && old.condition_id == new.condition_id
                    && old.tokens.len() == new.tokens.len()
                    && old.tokens.iter().all(|(id, token)| new.tokens.get(id)
                        .is_some_and(|other| token.best_bid == other.best_bid
                            && token.best_ask == other.best_ask))
                    && new.observed_at >= old.observed_at
                    && frame.received_at >= pending[0].received_at
                    && (frame.received_at - pending[0].received_at).num_microseconds()
                        .is_some_and(|elapsed| elapsed <= DEPTH_CONFLATION_WINDOW_US)
                    && pending.len() < OWNER_COALESCE_FRAMES
                    && !pending.iter().any(|earlier: &RawTransportFrame|
                        earlier.raw_payload == frame.raw_payload)
            });
            if !pending.is_empty() && !compatible {
                Self::flush_depth_conflation(&mut pending, &mut candidate, &mut output);
            }
            if let Some(mut next) = next {
                if let Some(old) = candidate.as_mut() {
                    for (id, mut token) in next.tokens {
                        old.tokens.get_mut(&id).expect("compatible token set")
                            .changes.append(&mut token.changes);
                    }
                    old.observed_at = next.observed_at;
                } else {
                    candidate = Some(next);
                }
                pending.push(frame);
            } else {
                output.push(frame);
            }
        }
        if !pending.is_empty() {
            Self::flush_depth_conflation(&mut pending, &mut candidate, &mut output);
        }
        output
    }

    /// Preserve an upstream multi-token frame as an atomic unit. If validation
    /// fails after partially reducing it, invalidate every token in that frame,
    /// never unrelated markets. No partially reduced output escapes.
    fn apply_isolated(
        &mut self,
        raw: Value,
        received: chrono::DateTime<Utc>,
        after: u64,
    ) -> Result<Vec<Value>> {
        if raw["event_type"] == "price_change" && !self.conflated_raw_hashes.is_empty()
            && self.conflated_raw_hashes.contains(&canonical_hash(&raw))
        {
            return Ok(Vec::new());
        }
        let conflated_source_hashes = raw["marketcow_conflation"]["source_frames"]
            .as_array().map(|frames| frames.iter().filter_map(|frame|
                frame.get("raw_payload").map(canonical_hash)).collect::<Vec<_>>())
            .unwrap_or_default();
        let mut affected = BTreeSet::new();
        if let Some(token) = raw["asset_id"].as_str() {
            affected.insert(token.to_owned());
        }
        if let Some(changes) = raw["price_changes"].as_array() {
            for change in changes {
                affected.insert(
                    change["asset_id"]
                        .as_str()
                        .context("unidentifiable price change")?
                        .to_owned(),
                );
            }
        }
        if affected.is_empty()
            && let Some(condition) = raw["market"].as_str()
        {
            affected.extend(
                self.identities
                    .iter()
                    .filter(|(_, (_, c))| c == condition)
                    .map(|(t, _)| t.clone()),
            );
        }
        ensure!(
            affected.iter().all(|t| self.identities.contains_key(t)),
            "unsubscribed frame identity"
        );
        let source_gap = raw["event_type"] == "source_gap";
        let source_gap_diagnostic = source_gap.then(|| {
            json!({
                "transport_reason":raw["transport_reason"],
                "close_code":raw["close_code"],
                "close_reason":raw["close_reason"],
                "transport_attempt":raw["attempt"],
            })
        });
        let full = raw["event_type"] == "book";
        // A price-change array is one atomic market observation. If any member
        // lacks continuity, do not partially advance its healthy sibling.
        if !full && !source_gap && affected.iter().any(|token| self.waiting.contains(token)) {
            return Ok(Vec::new());
        }
        let result = if source_gap {
            Err(anyhow::anyhow!("source_connection_gap"))
        } else {
            self.apply(raw, received, after)
        };
        match result {
            Ok(mut events) => {
                for hash in conflated_source_hashes {
                    if self.conflated_raw_hashes.insert(hash.clone()) {
                        self.conflated_raw_order.push_back(hash);
                    }
                }
                while self.conflated_raw_order.len() > 10_000 {
                    if let Some(oldest) = self.conflated_raw_order.pop_front() {
                        self.conflated_raw_hashes.remove(&oldest);
                    }
                }
                if full && let Some(snapshot) = events.first() {
                    let token = snapshot["token_id"]
                        .as_str()
                        .context("snapshot token")?
                        .to_owned();
                    let snapshot_cursor = snapshot["cursor"].as_u64().context("snapshot cursor")?;
                    if let Some(attempt) = self.recoveries.remove(&token) {
                        let (market, condition) = &self.identities[&token];
                        let payload = json!({"recovery_scope":"token","token_id":token,"recovery_id":attempt,
                            "snapshot_cursor":snapshot_cursor,"resolved_gap_token_ids":[token]});
                        events.push(recovery_event(
                            market,
                            condition,
                            &token,
                            snapshot_cursor.checked_add(1).context("cursor overflow")?,
                            &wire_time(received)?,
                            "recovery_completed",
                            payload,
                            vec![],
                        ));
                    }
                }
                Ok(events)
            }
            Err(error) => {
                ensure!(
                    !affected.is_empty(),
                    "unidentifiable upstream failure: {error}"
                );
                let reason = if source_gap {
                    "source_connection_gap"
                } else if error.downcast_ref::<ResyncRequired>().is_some() {
                    "source_data_delayed"
                } else {
                    "source_validation_failed"
                };
                let invalidatable = affected
                    .iter()
                    .filter(|token| !source_gap || !self.recoveries.contains_key(*token))
                    .cloned()
                    .collect::<Vec<_>>();
                if invalidatable.is_empty() {
                    return Ok(Vec::new());
                }
                marketcow_polymarket::async_eprintln!(
                    "{}",
                    json!({"stage":"token_invalidation","tokens":invalidatable,"reason":reason,
                        "detail":error.to_string(),"source_gap":source_gap_diagnostic})
                );
                let mut invalidations = Vec::new();
                for token in &invalidatable {
                    let cursor = after
                        .checked_add(invalidations.len() as u64 + 1)
                        .context("cursor overflow")?;
                    let event = if source_gap {
                        self.invalidate_once(token, reason, received, cursor)?
                    } else {
                        Some(self.invalidate(token, reason, received, cursor)?)
                    };
                    if let Some(event) = event {
                        invalidations.push(event);
                    }
                }
                Ok(invalidations)
            }
        }
    }
    fn apply(
        &mut self,
        raw: Value,
        received: chrono::DateTime<Utc>,
        after: u64,
    ) -> Result<Vec<Value>> {
        if let Some(metadata) = raw.get("marketcow_conflation") {
            ensure!(metadata["schema_version"] == "marketcow.polymarket.depth-conflation.v2",
                "unsupported depth conflation schema");
            let frames = metadata["source_frames"].as_array()
                .context("conflation source frames missing")?;
            ensure!(frames.len() >= 2 && frames.len() <= OWNER_COALESCE_FRAMES
                && metadata["source_frame_count"].as_u64() == Some(frames.len() as u64),
                "conflation source frame count");
            let originals = frames.iter().map(|frame| {
                let raw_payload = frame.get("raw_payload").context("conflation raw frame")?.clone();
                let received_at = chrono::DateTime::parse_from_rfc3339(
                    frame["received_at"].as_str().context("conflation receipt")?)?
                    .with_timezone(&Utc);
                Ok(RawTransportFrame { raw_payload, received_at })
            }).collect::<Result<Vec<_>>>()?;
            // The aggregate was selected before this batch was applied. Earlier
            // frames in the same batch may already have advanced the book and
            // dedupe cache, so verify the immutable supplier preimage without
            // reapplying live-state eligibility against a later state.
            let rebuilt = self.conflate_depth_frames(originals, false);
            ensure!(rebuilt.len() == 1
                && wire_time(rebuilt[0].received_at)? == wire_time(received)?
                && rebuilt[0].raw_payload == raw,
                "conflation does not match validated supplier frames");
        }
        let kind = raw["event_type"]
            .as_str()
            .context("missing WS event_type")?
            .to_owned();
        let source_hash = raw["marketcow_conflation"]["source_frames"]
            .as_array()
            .and_then(|frames| frames.last())
            .and_then(|frame| frame.get("raw_payload"))
            .map(canonical_hash)
            .unwrap_or_else(|| canonical_hash(&raw));
        if kind == "source_gap" {
            let token = raw["asset_id"].as_str().context("gap token")?;
            ensure!(
                self.identities.contains_key(token),
                "unsubscribed gap token"
            );
            self.waiting.insert(token.into());
            self.engines.remove(token);
            return Ok(Vec::new());
        }
        if kind == "new_market" {
            return Ok(Vec::new());
        } // Frozen scope is changed only through its configured contract.
        ensure!(
            kind != "market_resolved",
            "market resolution observed; authoritative metadata recovery required"
        );
        let tick = raw["asset_id"]
            .as_str()
            .and_then(|t| self.ticks.get(t))
            .cloned();
        let normalized =
            normalize_frame_with_book_tick(&self.config, raw.clone(), received, 1, tick)?;
        let mut output = Vec::new();
        for mut event in normalized {
            let token = event.kind.token_id().to_owned();
            let (market, condition) = self
                .identities
                .get(&token)
                .context("unsubscribed WS token")?
                .clone();
            ensure!(
                raw["market"].as_str() == Some(condition.as_str()),
                "WS condition/catalog mismatch"
            );
            if self.waiting.contains(&token) && !matches!(event.kind, EventKind::FullBook { .. }) {
                continue;
            }
            let engine = self.engines.entry(token.clone()).or_insert_with(|| {
                SingleWriter::new_data_delivery(self.config.scope_id.clone(), MemoryReducer)
            });
            event.cursor = engine
                .projection()
                .cursor
                .checked_add(1)
                .context("reducer overflow")?;
            let full = matches!(event.kind, EventKind::FullBook { .. });
            let observed = event.source_observed_at;
            let applied = match engine.apply(event) {
                Ok(applied) => applied,
                // Native identity hashes the complete raw frame plus token and
                // scope. An exact replay within this connection is idempotent:
                // do not advance any cursor, sequence or freshness timestamp.
                Err(CoreError::DuplicateEvent(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            if !applied.persisted.applied
                && applied.persisted.fail_closed_reason.as_deref() == Some("source_data_delayed")
            {
                return Err(ResyncRequired {
                    token,
                    observed,
                    received,
                }
                .into());
            }
            ensure!(
                applied.persisted.applied,
                "WS book rejected: {:?}",
                applied.persisted.fail_closed_reason
            );
            let book = applied
                .projection
                .books
                .get(&token)
                .context("missing materialized WS book")?;
            if full {
                self.waiting.remove(&token);
                self.epochs.insert(token.clone(),canonical_hash(&json!({
                    "connection_snapshot_cursor":after.checked_add(output.len() as u64+1).context("cursor overflow")?,
                    "received_at":wire_time(received)?,"source_hash":canonical_hash(&raw),"token_id":token})));
                self.sequences.insert(token.clone(), 0);
            }
            let tick = book.tick_size.as_ref().context("missing tick")?;
            self.ticks.insert(token.clone(), tick.clone());
            if full {
                let last_trade = match raw.get("last_trade_price") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(value)) if value.is_empty() => None,
                    Some(Value::String(value)) => Some(Price::parse(value)?),
                    Some(Value::Number(value)) => Some(Price::parse(&value.to_string())?),
                    _ => anyhow::bail!("invalid book last_trade_price"),
                };
                ensure!(
                    last_trade
                        .as_ref()
                        .is_none_or(|price| (price.0 % tick.0).is_zero()),
                    "book last_trade_price is off tick"
                );
                if let Some(price) = last_trade {
                    self.last_trades.insert(token.clone(), price);
                } else {
                    self.last_trades.remove(&token);
                }
            } else if kind == "last_trade_price"
                && let Some(price) = &book.last_trade_price
            {
                self.last_trades.insert(token.clone(), price.clone());
            }
            let bids: Vec<_> = book
                .bids
                .iter()
                .rev()
                .map(|(p, q)| json!({"price":p.0.to_string(),"size":q.to_string()}))
                .collect();
            let asks: Vec<_> = book
                .asks
                .iter()
                .map(|(p, q)| json!({"price":p.0.to_string(),"size":q.to_string()}))
                .collect();
            let at = wire_time(received)?;
            let event_source = wire_time(observed)?;
            let checksum = canonical_hash(
                &json!({"token_id":token,"tick_size":tick.0.to_string(),"bids":bids,"asks":asks}),
            );
            if matches!(kind.as_str(), "price_change" | "best_bid_ask")
                && self
                    .depth_checksums
                    .get(&token)
                    .is_some_and(|previous| previous == &checksum)
            {
                // This is a new, non-replayed venue observation (the exact
                // replay path returned above) whose fully reduced depth is
                // already visible.  It confirms only this token in the hot
                // projection; it is not a state mutation and must not advance
                // the public sequence/cursor or durability watermark.
                self.confirmations.push(ReducedBookConfirmation {
                    market_id: market,
                    condition_id: condition,
                    book_epoch: self.epochs.get(&token)
                        .context("confirmation book epoch missing")?.clone(),
                    book_sequence: *self.sequences.get(&token)
                        .context("confirmation book sequence missing")?,
                    book_received_at: *self.book_received.get(&token)
                        .context("confirmation book receipt missing")?,
                    token_id: token,
                    state_checksum: checksum,
                    exchange_at: observed,
                    received_at: received,
                    source_hash: source_hash.clone(),
                });
                continue;
            }
            self.depth_checksums.insert(token.clone(), checksum.clone());
            if matches!(kind.as_str(), "book" | "price_change" | "tick_size_change") {
                // Local receipt is retained for diagnosis and ordering only.
                self.book_received.insert(token.clone(), received);
            }
            if matches!(kind.as_str(), "book" | "price_change") {
                self.book_source.insert(token.clone(), observed);
            }
            let quote_source = wire_time(*self.book_source.get(&token)
                .context("missing supplier quote time")?)?;
            let book_at = wire_time(
                *self
                    .book_received
                    .get(&token)
                    .context("missing book observation")?,
            )?;
            let sequence = {
                let sequence = self
                    .sequences
                    .get_mut(&token)
                    .context("delta without full-book epoch")?;
                *sequence = sequence.checked_add(1).context("sequence overflow")?;
                *sequence
            };
            let model = json!({"token_id":token,"condition_id":condition,"book_epoch":self.epochs[&token],
                "sequence":sequence,"sequence_semantics":"deterministic_normalized","exchange_at":quote_source,
                "received_at":book_at,"tick_version":book.tick_version,"tick_size":tick.0.to_string(),
                "bids":bids,"asks":asks,"last_trade_price":self.last_trades.get(&token).map(|p|p.0.to_string()),
                "state_checksum":checksum,"source_hash":source_hash});
            let mut envelope = json!({"contract_version":"marketcow.prediction_market.v1","schema_version":"marketcow.polymarket.live.v2",
                "cursor":after.checked_add(output.len() as u64+1).context("cursor overflow")?,"event_type":"book",
                "market_id":market,"condition_id":condition,"token_id":token,"book_epoch":self.epochs[&token],"sequence":sequence,
                "exchange_at":event_source,"received_at":at,"canonical_payload_sha256":canonical_hash(&model),"canonical_payload":model,
                "raw_payload_sha256":canonical_hash(&raw),"raw_payload":raw,"applied":true,"fail_closed_reason":null,"gaps":[]});
            if kind == "price_change" {
                let EventKind::AtomicDelta { changes, .. } = &applied.persisted.event.kind else {
                    anyhow::bail!("price change did not persist an atomic delta")
                };
                let replay_changes = changes
                    .iter()
                    .flat_map(|change| {
                        let side = match change.side {
                            marketcow_core::Side::Bid => "BUY",
                            marketcow_core::Side::Ask => "SELL",
                        };
                        let token = token.clone();
                        change.levels.iter().map(move |level| {
                            json!({"asset_id":token,"side":side,
                                "price":level.price.0.to_string(),
                                "size":level.quantity.to_string()})
                        })
                    })
                    .collect::<Vec<_>>();
                ensure!(
                    !replay_changes.is_empty(),
                    "mutating price change has no replay-complete delta"
                );
                // Private actor-to-publication handoff. compact_incremental_events
                // consumes and removes it before event identity/validation. Unlike
                // raw_payload.price_changes, this includes reducer-synthesized BBO
                // removals and therefore exactly replays the committed post-state.
                envelope["_replay_complete_delta"] = json!({"changes":replay_changes});
            }
            envelope["event_id"] = json!(canonical_hash(&envelope));
            output.push(envelope);
        }
        Ok(output)
    }
    fn apply_rest_recovery(
        &mut self,
        mut raw_books: Vec<Value>,
        received: chrono::DateTime<Utc>,
        after: u64,
        attempts: &BTreeMap<String, String>,
    ) -> Result<Vec<Value>> {
        if attempts
            .iter()
            .all(|(token, attempt)| self.recoveries.get(token) != Some(attempt))
        {
            return Ok(Vec::new()); // An earlier recovery source already won this fenced attempt.
        }
        ensure!(
            raw_books.len() == 2,
            "REST recovery requires a two-token market"
        );
        raw_books.sort_by(|left, right| left["asset_id"].as_str().cmp(&right["asset_id"].as_str()));
        let boundary = uuid::Uuid::new_v4().to_string();
        let mut output = Vec::new();
        let mut market_identity = None;
        for raw in raw_books {
            let token = raw["asset_id"]
                .as_str()
                .context("REST recovery token")?
                .to_owned();
            let (market, condition) = self
                .identities
                .get(&token)
                .context("unknown REST recovery token")?
                .clone();
            ensure!(
                raw["market"] == condition,
                "REST recovery identity mismatch"
            );
            if let Some(expected) = &market_identity {
                ensure!(expected == &market, "REST recovery crosses markets");
            }
            market_identity = Some(market.clone());
            let mut reducer_raw = raw.clone();
            reducer_raw["event_type"] = json!("book");
            ensure!(
                self.apply(reducer_raw, received, 0)?.len() == 1,
                "REST recovery did not seed one token book"
            );
            let attempt = attempts
                .get(&token)
                .filter(|attempt| self.recoveries.get(&token) == Some(*attempt));
            let recovery_id = attempt.map(String::as_str).unwrap_or(&boundary);
            let cursor = after
                .checked_add(output.len() as u64 + 1)
                .context("cursor overflow")?;
            let snapshot = snapshot_event(
                &raw,
                SnapshotBoundary {
                    market_id: &market,
                    condition_id: &condition,
                    token_id: &token,
                    recovery_id,
                    cursor,
                    received_at: received,
                },
            )?;
            self.epochs.insert(
                token.clone(),
                snapshot["canonical_payload"]["book_epoch"]
                    .as_str()
                    .context("REST epoch")?
                    .to_owned(),
            );
            self.sequences.insert(token.clone(), 1);
            output.push(snapshot);
            if let Some(attempt) = attempt.cloned() {
                self.recoveries.remove(&token);
                let payload = json!({"recovery_scope":"token","token_id":token,"recovery_id":attempt,
                    "snapshot_cursor":cursor,"resolved_gap_token_ids":[token]});
                output.push(recovery_event(
                    &market,
                    &condition,
                    &token,
                    after
                        .checked_add(output.len() as u64 + 1)
                        .context("cursor overflow")?,
                    &wire_time(received)?,
                    "recovery_completed",
                    payload,
                    vec![],
                ));
            }
        }
        Ok(output)
    }
}
fn recovery_event(
    market: &str,
    condition: &str,
    token: &str,
    cursor: u64,
    at: &str,
    kind: &str,
    payload: Value,
    gaps: Vec<Value>,
) -> Value {
    // Recovery is a producer control record, never a fabricated upstream book.
    let raw_source_event_type = if payload["reason"] == "source_connection_gap" {
        "source_gap"
    } else {
        kind
    };
    let raw = json!({"recovery_scope":"token","source_event_type":raw_source_event_type,
        "reason":payload.get("reason"),"recovery_id":payload["recovery_id"]});
    let mut event = json!({"contract_version":"marketcow.prediction_market.v1","schema_version":"marketcow.polymarket.live.v2",
        "cursor":cursor,"event_type":kind,"market_id":market,"condition_id":condition,"token_id":token,
        "book_epoch":null,"sequence":null,"exchange_at":at,"received_at":at,
        "canonical_payload_sha256":canonical_hash(&payload),"canonical_payload":payload,
        "raw_payload_sha256":canonical_hash(&raw),"raw_payload":raw,"applied":true,"fail_closed_reason":null,"gaps":gaps});
    event["event_id"] = json!(canonical_hash(&event));
    event
}
fn wire_time(value: chrono::DateTime<Utc>) -> Result<String> {
    let value = value
        .with_nanosecond(value.nanosecond() / 1000 * 1000)
        .context("timestamp")?;
    Ok(value.to_rfc3339_opts(
        if value.nanosecond() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        },
        true,
    ))
}

#[cfg(test)]
pub(crate) fn btc5m_reducer_fixture_sequence() -> Result<Vec<Value>> {
    use chrono::TimeZone;
    let condition = format!("0x{}", "1".repeat(64));
    let plan = Plan {
        schema_version: "fixture".into(),
        catalog_revision: "a".repeat(64),
        markets: vec![super::Market {
            market_id: "4944030".into(),
            condition_id: condition.clone(),
            token_ids: ["101".into(), "102".into()],
        }],
    };
    let seed = json!({"scope_id":"btc5m-fixture-scope",
        "markets":[{"identity":{"market_id":"4944030"},"lifecycle_state":"active"}],
        "books":[{"token_id":"101","tick_size":"0.01"},{"token_id":"102","tick_size":"0.01"}]});
    let mut adapter = Adapter::new(&plan, &seed)?;
    let at = |millis| Utc.timestamp_millis_opt(millis).single().unwrap();
    let mut events = Vec::new();
    events.extend(adapter.apply_isolated(
        json!({"event_type":"book","asset_id":"101",
        "market":condition,"timestamp":"1790400604000","hash":"fixture-book-1","tick_size":"0.01",
        "bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.60","size":"10"}],
        "last_trade_price":"0.50"}),
        at(1790400604010),
        42,
    )?);
    events.extend(adapter.apply_isolated(
        json!({"event_type":"book","asset_id":"102",
        "market":condition,"timestamp":"1790400604020","hash":"fixture-book-down-1","tick_size":"0.01",
        "bids":[{"price":"0.39","size":"8"}],"asks":[{"price":"0.61","size":"9"}],
        "last_trade_price":"0.50"}),
        at(1790400604030),
        43,
    )?);
    events.extend(adapter.apply_isolated(json!({"event_type":"price_change","market":condition,
        "timestamp":"1790400605000","price_changes":[{"asset_id":"101","side":"BUY",
        "price":"0.40","size":"0","best_bid":"0.41","best_ask":"0.60"},
        {"asset_id":"101","side":"BUY","price":"0.41","size":"8","best_bid":"0.41","best_ask":"0.60"}]}),
        at(1790400605010),44)?);
    events.extend(adapter.apply_isolated(
        json!({"event_type":"last_trade_price","asset_id":"101",
        "market":condition,"timestamp":"1790400606000","price":"0.52"}),
        at(1790400606010),
        45,
    )?);
    events.extend(adapter.apply_isolated(
        json!({"event_type":"source_gap","asset_id":"101",
        "transport_reason":"fixture_gap","close_code":null,"close_reason":null,"attempt":1}),
        at(1790400607000),
        46,
    )?);
    events.extend(adapter.apply_isolated(
        json!({"event_type":"book","asset_id":"101",
        "market":condition,"timestamp":"1790400608000","hash":"fixture-book-2","tick_size":"0.01",
        "bids":[{"price":"0.42","size":"9"}],"asks":[{"price":"0.58","size":"11"}],
        "last_trade_price":"0.52"}),
        at(1790400608010),
        47,
    )?);
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_coalescing_preserves_order_and_gap_boundaries() {
        let received_at = Utc::now();
        let frames = ["book", "price_change", "source_gap", "source_gap", "book"]
            .into_iter()
            .enumerate()
            .map(|(sequence, event_type)| RawTransportFrame {
                raw_payload: json!({"event_type":event_type,"sequence":sequence}),
                received_at,
            })
            .collect();
        let groups = split_transport_control_boundaries(frames);
        assert_eq!(groups.iter().map(Vec::len).collect::<Vec<_>>(), [2, 2, 1]);
        assert_eq!(groups.into_iter().flatten()
            .map(|frame| frame.raw_payload["sequence"].as_u64().unwrap())
            .collect::<Vec<_>>(), (0..5).collect::<Vec<_>>());
    }

    #[test]
    fn owner_coalescing_caps_actor_inputs() {
        let received_at = Utc::now();
        let frames = (0..130).map(|sequence| RawTransportFrame {
            raw_payload: json!({"event_type":"price_change","sequence":sequence}),
            received_at,
        }).collect();
        let groups = split_transport_control_boundaries(frames);
        assert_eq!(groups.iter().map(Vec::len).collect::<Vec<_>>(), [64, 64, 2]);
    }

    #[test]
    fn targeted_confirmation_retry_stays_inside_freshness_budget() {
        assert_eq!(
            targeted_confirmation_retry_delay(1),
            std::time::Duration::from_secs(1),
        );
        assert_eq!(
            targeted_confirmation_retry_delay(0),
            std::time::Duration::from_secs(1),
        );
        assert_eq!(recovery_retry_delay(1), std::time::Duration::from_secs(5));
        assert_eq!(recovery_retry_delay(2), std::time::Duration::from_secs(10));
        assert_eq!(recovery_retry_delay(9), std::time::Duration::from_secs(60));
        assert_eq!(audit_batch_offset(300, 0, 5), std::time::Duration::ZERO);
        assert_eq!(
            audit_batch_offset(300, 1, 5),
            std::time::Duration::from_secs(60)
        );
        assert_eq!(
            audit_batch_offset(300, 4, 5),
            std::time::Duration::from_secs(240)
        );
    }

    #[tokio::test]
    async fn targeted_snapshot_scope_is_nonempty_and_unique_before_network_io() {
        assert!(
            recover_snapshots(Vec::new())
                .await
                .unwrap_err()
                .to_string()
                .contains("empty")
        );
        assert!(
            recover_snapshots(vec!["11".into(), "11".into()])
                .await
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );
    }

    #[test]
    fn targeted_http_confirmation_is_complete_unique_and_book_typed() {
        let received = Utc::now();
        let expected = BTreeSet::from(["11".into(), "12".into()]);
        let book = |token: &str| {
            json!({"asset_id":token,"timestamp":"1","tick_size":"0.01",
                "bids":[{"price":"0.4","size":"1"}],
                "asks":[{"price":"0.6","size":"1"}]})
        };
        let frames =
            confirmation_frames(vec![book("12"), book("11")], &expected, received).unwrap();
        assert_eq!(frames.len(), 2);
        assert!(frames.iter().all(|frame| {
            frame.raw_payload["event_type"] == "book" && frame.received_at == received
        }));
        assert!(confirmation_frames(vec![book("11")], &expected, received).is_err());
        assert!(confirmation_frames(vec![book("11"), book("11")], &expected, received).is_err());
        assert!(confirmation_frames(vec![book("11"), book("13")], &expected, received).is_err());
    }

    #[test]
    fn lifecycle_lookup_uses_the_exact_failed_catalog_identity() {
        let market = catalog_market(&json!({
            "identity": {
                "market_id": "3418199",
                "condition_id": "0xcondition",
                "outcomes": [{"token_id": "yes"}, {"token_id": "no"}]
            }
        }))
        .unwrap();
        assert_eq!(market.market_id, "3418199");
        assert_eq!(market.condition_id, "0xcondition");
        assert_eq!(market.token_ids, ["yes", "no"]);
        assert!(
            catalog_market(&json!({
                "identity": {
                    "market_id": "1",
                    "condition_id": "c",
                    "outcomes": [{"token_id": "only-one"}]
                }
            }))
            .is_err()
        );
    }

    #[tokio::test]
    async fn disabled_confirmation_finishes_without_network_io() {
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let (_stop, shutdown) = tokio::sync::watch::channel(false);
        let (_membership_sender, membership) =
            tokio::sync::watch::channel(BTreeSet::from(["1".to_owned()]));
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            run_confirmations(
                vec![super::super::Market {
                    market_id: "1".into(),
                    condition_id: "condition".into(),
                    token_ids: ["11".into(), "12".into()],
                }],
                1,
                1,
                1,
                1,
                0,
                sender,
                shutdown,
                Arc::new(tokio::sync::Semaphore::new(1)),
                membership,
            ),
        )
        .await;
        assert_eq!(result.unwrap().unwrap(), ());
    }
    fn fixture() -> Adapter {
        let plan = Plan {
            schema_version: "test".into(),
            catalog_revision: "catalog".into(),
            markets: vec![super::super::Market {
                market_id: "1".into(),
                condition_id: "condition".into(),
                token_ids: ["11".into(), "12".into()],
            }],
        };
        Adapter::new(&plan,&json!({"scope_id":"scope","markets":[{"identity":{"market_id":"1"},"lifecycle_state":"active"}],"books":[{"token_id":"11","tick_size":"0.01"},{"token_id":"12","tick_size":"0.01"}]})).unwrap()
    }
    fn book(token: &str, now: chrono::DateTime<Utc>) -> Value {
        json!({"event_type":"book","asset_id":token,"market":"condition","timestamp":now.timestamp_millis().to_string(),"hash":"upstream","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]})
    }
    fn deep_change(size: &str, at: chrono::DateTime<Utc>) -> RawTransportFrame {
        RawTransportFrame {
            raw_payload: json!({"event_type":"price_change","market":"condition",
                "timestamp":at.timestamp_millis().to_string(),"price_changes":[{
                    "asset_id":"11","side":"BUY","price":"0.30","size":size,
                    "best_bid":"0.4","best_ask":"0.6"}]}),
            received_at: at,
        }
    }
    fn paired_deep_change(size: &str, at: chrono::DateTime<Utc>) -> RawTransportFrame {
        RawTransportFrame {
            raw_payload: json!({"event_type":"price_change","market":"condition",
                "timestamp":at.timestamp_millis().to_string(),"price_changes":[
                    {"asset_id":"11","side":"BUY","price":"0.30","size":size,
                        "best_bid":"0.4","best_ask":"0.6"},
                    {"asset_id":"12","side":"SELL","price":"0.70","size":size,
                        "best_bid":"0.4","best_ask":"0.6"}]}),
            received_at: at,
        }
    }

    #[test]
    fn paired_token_microburst_commits_both_books_and_preserves_source_identity() {
        let now = Utc::now().with_nanosecond(0).unwrap();
        let first = paired_deep_change("1", now + chrono::Duration::milliseconds(1));
        let last = paired_deep_change("2", now + chrono::Duration::milliseconds(2));
        let mut sequential = fixture();
        let mut conflated = fixture();
        for adapter in [&mut sequential, &mut conflated] {
            adapter.apply_isolated(book("11", now), now, 0).unwrap();
            adapter.apply_isolated(book("12", now), now, 1).unwrap();
        }
        sequential.apply_isolated(first.raw_payload.clone(), first.received_at, 2).unwrap();
        let expected = sequential.apply_isolated(last.raw_payload.clone(), last.received_at, 4).unwrap();
        assert_eq!(expected.len(), 2);
        let frames = conflated.conflate_depth_frames(vec![first.clone(), last.clone()], true);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].raw_payload["marketcow_conflation"]["token_ids"], json!(["11", "12"]));
        assert_eq!(frames[0].raw_payload["marketcow_conflation"]["source_frame_count"], 2);
        let actual = conflated.apply_isolated(frames[0].raw_payload.clone(), frames[0].received_at, 2).unwrap();
        assert_eq!(actual.len(), 2);
        for (actual, expected) in actual.iter().zip(&expected) {
            assert_eq!(actual["token_id"], expected["token_id"]);
            assert_eq!(actual["canonical_payload"]["bids"], expected["canonical_payload"]["bids"]);
            assert_eq!(actual["canonical_payload"]["asks"], expected["canonical_payload"]["asks"]);
            assert_eq!(actual["canonical_payload"]["state_checksum"],
                expected["canonical_payload"]["state_checksum"]);
        }
        let compact = compact_btc5m_fixture_events(actual).unwrap();
        assert_eq!(compact.len(), 2);
        for event in &compact {
            let typed = crate::source_btc5m_proto::market_event(event).unwrap();
            assert!(matches!(typed.payload,
                Some(marketcow_contracts::btc5m_market_local_v3::wire::market_event::Payload::BookDelta(_))));
        }
        assert!(conflated.apply_isolated(last.raw_payload, last.received_at, 4).unwrap().is_empty());
    }

    #[test]
    fn conflation_preimage_check_does_not_recheck_later_dedupe_state() {
        let now = Utc::now().with_nanosecond(0).unwrap();
        let first = paired_deep_change("1", now + chrono::Duration::milliseconds(1));
        let last = paired_deep_change("2", now + chrono::Duration::milliseconds(2));
        let mut adapter = fixture();
        adapter.apply_isolated(book("11", now), now, 0).unwrap();
        adapter.apply_isolated(book("12", now), now, 1).unwrap();
        let frames = adapter.conflate_depth_frames(vec![first.clone(), last], true);
        assert_eq!(frames.len(), 1);
        // Selection is performed for the whole input batch before reduction.
        // The live dedupe cache can advance before a selected aggregate is
        // applied; immutable-preimage verification must not depend on it.
        adapter.conflated_raw_hashes.insert(canonical_hash(&first.raw_payload));
        let applied = adapter.apply_isolated(frames[0].raw_payload.clone(), frames[0].received_at, 2).unwrap();
        assert_eq!(applied.len(), 2);
        assert!(!adapter.waiting.contains("11"));
        assert!(!adapter.waiting.contains("12"));
    }

    #[test]
    fn paired_token_microburst_flushes_when_one_token_bbo_changes() {
        let now = Utc::now().with_nanosecond(0).unwrap();
        let mut adapter = fixture();
        adapter.apply_isolated(book("11", now), now, 0).unwrap();
        adapter.apply_isolated(book("12", now), now, 1).unwrap();
        let first = paired_deep_change("1", now + chrono::Duration::milliseconds(1));
        let mut changed = paired_deep_change("2", now + chrono::Duration::milliseconds(2));
        changed.raw_payload["price_changes"][1]["best_bid"] = json!("0.5");
        let frames = adapter.conflate_depth_frames(vec![first, changed], true);
        assert_eq!(frames.len(), 2);
        assert!(frames.iter().all(|frame| frame.raw_payload.get("marketcow_conflation").is_none()));
    }

    #[test]
    fn depth_conflation_keeps_raw_frames_without_matching_previous_two_sided_bbo() {
        let now = Utc::now().with_nanosecond(0).unwrap();
        for previous_bids in [json!([]), json!([{"price":"0.35","size":"10"}])] {
            let mut adapter = fixture();
            let mut one_side_or_different_top = book("11", now);
            one_side_or_different_top["bids"] = previous_bids;
            adapter.apply_isolated(one_side_or_different_top, now, 0).unwrap();
            adapter.apply_isolated(book("12", now), now, 1).unwrap();
            let first = paired_deep_change("1", now + chrono::Duration::milliseconds(1));
            let second = paired_deep_change("2", now + chrono::Duration::milliseconds(2));
            let frames = adapter.conflate_depth_frames(vec![first.clone(), second.clone()], true);
            assert_eq!(frames.len(), 2);
            assert_eq!(frames[0].raw_payload, first.raw_payload);
            assert_eq!(frames[1].raw_payload, second.raw_payload);
        }
    }

    #[test]
    fn paired_token_microburst_keeps_distinct_levels_on_both_sides() {
        let now = Utc::now().with_nanosecond(0).unwrap();
        let first = paired_deep_change("1", now + chrono::Duration::milliseconds(1));
        let mut second = paired_deep_change("2", now + chrono::Duration::milliseconds(2));
        second.raw_payload["price_changes"][0]["price"] = json!("0.20");
        second.raw_payload["price_changes"][1]["price"] = json!("0.80");
        let mut sequential = fixture();
        let mut conflated = fixture();
        for adapter in [&mut sequential, &mut conflated] {
            adapter.apply_isolated(book("11", now), now, 0).unwrap();
            adapter.apply_isolated(book("12", now), now, 1).unwrap();
        }
        sequential.apply_isolated(first.raw_payload.clone(), first.received_at, 2).unwrap();
        let expected = sequential.apply_isolated(second.raw_payload.clone(), second.received_at, 4).unwrap();
        let frames = conflated.conflate_depth_frames(vec![first, second], true);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].raw_payload["price_changes"].as_array().unwrap().len(), 4);
        let actual = conflated.apply_isolated(frames[0].raw_payload.clone(), frames[0].received_at, 2).unwrap();
        assert_eq!(actual.len(), 2);
        for (actual, expected) in actual.iter().zip(&expected) {
            assert_eq!(actual["canonical_payload"]["state_checksum"],
                expected["canonical_payload"]["state_checksum"]);
        }
    }

    #[test]
    fn deep_microburst_preserves_final_book_and_exact_source_frames() {
        let now = Utc::now().with_nanosecond(0).unwrap();
        let first = deep_change("1", now + chrono::Duration::milliseconds(1));
        let last = deep_change("2", now + chrono::Duration::milliseconds(2));
        let mut sequential = fixture();
        let mut conflated = fixture();
        for adapter in [&mut sequential, &mut conflated] {
            adapter.apply_isolated(book("11", now), now, 0).unwrap();
        }
        sequential.apply_isolated(first.raw_payload.clone(), first.received_at, 1).unwrap();
        let expected = sequential.apply_isolated(last.raw_payload.clone(), last.received_at, 2).unwrap();
        let frames = conflated.conflate_depth_frames(vec![first.clone(), last.clone()], true);
        assert_eq!(frames.len(), 1);
        let originals = frames[0].raw_payload["marketcow_conflation"]["source_frames"]
            .as_array().unwrap();
        assert_eq!(originals.len(), 2);
        assert_eq!(frames[0].raw_payload["marketcow_conflation"]["source_frame_count"], 2);
        assert_eq!(originals[0]["raw_payload"], first.raw_payload);
        assert_eq!(originals[1]["raw_payload"], last.raw_payload);
        let actual = conflated.apply_isolated(frames[0].raw_payload.clone(), frames[0].received_at, 1).unwrap();
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0]["canonical_payload"]["bids"], expected[0]["canonical_payload"]["bids"]);
        assert_eq!(actual[0]["canonical_payload"]["asks"], expected[0]["canonical_payload"]["asks"]);
        assert_eq!(actual[0]["canonical_payload"]["state_checksum"], expected[0]["canonical_payload"]["state_checksum"]);
        let compact = compact_btc5m_fixture_events(actual.clone()).unwrap();
        let typed = crate::source_btc5m_proto::market_event(&compact[0]).unwrap();
        assert!(matches!(typed.payload,
            Some(marketcow_contracts::btc5m_market_local_v3::wire::market_event::Payload::BookDelta(_))));
        assert_eq!(typed.raw_payload_sha256,
            hex::decode(compact[0]["raw_payload_sha256"].as_str().unwrap()).unwrap());
        assert_eq!(typed.event_id,
            hex::decode(compact[0]["event_id"].as_str().unwrap()).unwrap());
        assert!(conflated.apply_isolated(last.raw_payload, last.received_at, 2).unwrap().is_empty(),
            "exact replay of an original frame cannot refresh the conflated book");
    }

    #[test]
    fn deep_microburst_flushes_on_bbo_control_and_receipt_window() {
        let now = Utc::now().with_nanosecond(0).unwrap();
        let mut adapter = fixture();
        adapter.apply_isolated(book("11", now), now, 0).unwrap();
        let first = deep_change("1", now + chrono::Duration::milliseconds(1));
        let second = deep_change("2", now + chrono::Duration::milliseconds(2));
        let late = deep_change("3", now + chrono::Duration::milliseconds(5));
        assert_eq!(adapter.conflate_depth_frames(vec![first.clone(), second.clone(), late], true).len(), 2);
        let control = RawTransportFrame {
            raw_payload: json!({"event_type":"last_trade_price","asset_id":"11",
                "market":"condition","timestamp":now.timestamp_millis().to_string(),"price":"0.5"}),
            received_at: now + chrono::Duration::milliseconds(2),
        };
        assert_eq!(adapter.conflate_depth_frames(vec![first, control, second], true).len(), 3);
    }

    #[test]
    fn deep_microburst_never_absorbs_top_level_or_invalid_frame() {
        let now = Utc::now().with_nanosecond(0).unwrap();
        let mut adapter = fixture();
        adapter.apply_isolated(book("11", now), now, 0).unwrap();
        let first = deep_change("1", now + chrono::Duration::milliseconds(1));
        let mut top = deep_change("9", now + chrono::Duration::milliseconds(2));
        top.raw_payload["price_changes"][0]["price"] = json!("0.4");
        let last = deep_change("2", now + chrono::Duration::milliseconds(3));
        let frames = adapter.conflate_depth_frames(vec![first.clone(), top, last.clone()], true);
        assert_eq!(frames.len(), 3);
        let mut invalid = deep_change("2", now + chrono::Duration::milliseconds(2));
        invalid.raw_payload["price_changes"][0]["size"] = json!("not-a-decimal");
        let frames = adapter.conflate_depth_frames(vec![first, invalid, last], true);
        assert_eq!(frames.len(), 3);
        assert!(frames.iter().all(|frame| frame.raw_payload.get("marketcow_conflation").is_none()));
    }

    #[test]
    fn bounded_deep_burst_has_same_final_depth_as_sequential_reduction() {
        let now = Utc::now().with_nanosecond(0).unwrap();
        let mut sequential = fixture();
        let mut conflated = fixture();
        for adapter in [&mut sequential, &mut conflated] {
            adapter.apply_isolated(book("11", now), now, 0).unwrap();
        }
        let frames = (0..64).map(|index| {
            deep_change(&(index + 1).to_string(),
                now + chrono::Duration::microseconds(1_000 + index * 20))
        }).collect::<Vec<_>>();
        let mut expected = Vec::new();
        for (index, frame) in frames.iter().enumerate() {
            expected = sequential.apply_isolated(frame.raw_payload.clone(),
                frame.received_at, 1 + index as u64).unwrap();
        }
        let actual_frames = conflated.conflate_depth_frames(frames, true);
        assert_eq!(actual_frames.len(), 1);
        let actual = conflated.apply_isolated(actual_frames[0].raw_payload.clone(),
            actual_frames[0].received_at, 1).unwrap();
        assert_eq!(actual[0]["canonical_payload"]["state_checksum"],
            expected[0]["canonical_payload"]["state_checksum"]);
        assert_eq!(actual[0]["canonical_payload"]["bids"],
            expected[0]["canonical_payload"]["bids"]);
    }
    #[test]
    fn public_replay_delta_contains_reducer_synthesized_bbo_removals() {
        let mut adapter = fixture();
        let now = Utc::now().with_nanosecond(0).unwrap();
        let mut snapshot = book("11", now);
        snapshot["bids"] = json!([
            {"price":"0.42","size":"3"},
            {"price":"0.40","size":"10"}
        ]);
        adapter.apply_isolated(snapshot, now, 0).unwrap();
        let raw = json!({"event_type":"price_change","market":"condition",
        "timestamp":now.timestamp_millis().to_string(),"price_changes":[
            {"asset_id":"11","side":"BUY","price":"0.40","size":"0",
             "best_bid":"0.41","best_ask":"0.60"},
            {"asset_id":"11","side":"BUY","price":"0.41","size":"8",
             "best_bid":"0.41","best_ask":"0.60"}
        ]});
        let events = adapter.apply_isolated(raw, now, 1).unwrap();
        assert_eq!(events.len(), 1);
        let changes = events[0]["_replay_complete_delta"]["changes"]
            .as_array()
            .unwrap();
        assert!(
            changes.iter().any(|row| {
                row["side"] == "BUY" && row["price"] == "0.42" && row["size"] == "0"
            })
        );
        assert!(
            events[0]["canonical_payload"]["bids"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row["price"] != "0.42")
        );
    }
    #[test]
    fn missing_initial_tick_is_local_until_authoritative_snapshot() {
        let plan = Plan {
            schema_version: "test".into(),
            catalog_revision: "catalog".into(),
            markets: vec![super::super::Market {
                market_id: "1".into(),
                condition_id: "condition".into(),
                token_ids: ["11".into(), "12".into()],
            }],
        };
        let mut a = Adapter::new(
            &plan,
            &json!({"scope_id":"scope",
            "markets":[{"identity":{"market_id":"1"},"lifecycle_state":"active"}],
            "books":[{"token_id":"12","tick_size":"0.01"}]}),
        )
        .unwrap();
        let now = Utc::now();
        let failed = a.apply_isolated(book("11", now), now, 0).unwrap();
        assert_eq!(failed[0]["event_type"], "recovery_started");
        assert!(!a.ticks.contains_key("11"));
        assert!(a.waiting.contains("11"));
        let healthy = a
            .apply_isolated(book("12", now), now, failed.len() as u64)
            .unwrap();
        assert_eq!(healthy[0]["event_type"], "book");
        assert!(!a.waiting.contains("12"));
        assert!(a.waiting.contains("11"));
        let mut authoritative = book("11", now);
        authoritative["tick_size"] = json!("0.01");
        let recovered = a
            .apply_isolated(authoritative, now, (failed.len() + healthy.len()) as u64)
            .unwrap();
        assert!(recovered.iter().any(|event| event["event_type"] == "book"));
        assert!(a.ticks.contains_key("11"));
        assert!(!a.waiting.contains("11"));
    }
    #[test]
    fn authoritative_snapshot_delta_and_reconnect_require_new_baseline() {
        let mut a = fixture();
        let now = Utc::now();
        let first = a.apply(book("11", now), now, 0).unwrap();
        assert_eq!(first[0]["cursor"], 1);
        assert!(!a.waiting.is_empty());
        a.apply(book("12", now), now, 1).unwrap();
        assert!(a.waiting.is_empty());
        let delta = json!({"event_type":"price_change","market":"condition","timestamp":now.timestamp_millis().to_string(),"price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"7","best_bid":"0.4","best_ask":"0.6"}]});
        let result = a.apply(delta.clone(), now, 2).unwrap();
        assert_eq!(result[0]["cursor"], 3);
        assert_eq!(result[0]["canonical_payload"]["bids"][0]["size"], "7");
        assert_eq!(result[0]["raw_payload"], delta);
        let tokens = [("11".into(), "1".into()), ("12".into(), "1".into())].into();
        marketcow_runtime::discovery_source::ValidatedSourceBatch::new(
            result, false, 2, &tokens, 65536,
        )
        .unwrap();
        a.apply(json!({"event_type":"source_gap","asset_id":"11"}), now, 3)
            .unwrap();
        assert!(!a.waiting.is_empty());
        assert!(a.apply(delta, now, 3).unwrap().is_empty());
        a.apply(book("11", now), now, 3).unwrap();
        assert!(a.waiting.is_empty());
    }

    #[test]
    fn repeated_connection_gap_keeps_one_recovery_identity() {
        let mut adapter = fixture();
        let now = Utc::now();
        adapter.apply_isolated(book("11", now), now, 0).unwrap();
        let first = adapter
            .apply_isolated(json!({"event_type":"source_gap","asset_id":"11"}), now, 1)
            .unwrap();
        assert_eq!(first.len(), 1);
        let recovery = adapter.recoveries["11"].clone();
        let duplicate = adapter
            .apply_isolated(json!({"event_type":"source_gap","asset_id":"11"}), now, 2)
            .unwrap();
        assert!(duplicate.is_empty());
        assert_eq!(adapter.recoveries["11"], recovery);
    }

    #[test]
    fn connection_gap_recovery_is_coalesced_by_market() {
        let mut adapter = fixture();
        let now = Utc::now();
        for (cursor, token) in [(0, "11"), (1, "12")] {
            adapter
                .apply_isolated(
                    json!({"event_type":"source_gap","asset_id":token}),
                    now,
                    cursor,
                )
                .unwrap();
        }
        let pending = pending_market_recoveries(&adapter);
        assert_eq!(pending.len(), 1);
        assert_eq!(
            pending["1"].keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["11", "12"]
        );
    }

    #[test]
    fn stale_recovery_for_terminal_market_is_not_an_active_scope_error() {
        let plan = Plan {
            schema_version: "test".into(),
            catalog_revision: "catalog".into(),
            markets: vec![
                super::super::Market {
                    market_id: "1".into(),
                    condition_id: "active-condition".into(),
                    token_ids: ["11".into(), "12".into()].into(),
                },
                super::super::Market {
                    market_id: "2".into(),
                    condition_id: "terminal-condition".into(),
                    token_ids: ["21".into(), "22".into()].into(),
                },
            ],
        };
        let seed = json!({
            "scope_id":"scope",
            "markets":[
                {"identity":{"market_id":"1"},"lifecycle_state":"active"},
                {"identity":{"market_id":"2","outcomes":[{"token_id":"21"},{"token_id":"22"}]},"lifecycle_state":"resolved"}
            ],
            "books":[
                {"token_id":"11","tick_size":"0.01"},
                {"token_id":"12","tick_size":"0.01"}
            ],
            "token_recoveries":[{"token_id":"21","market_id":"2","recovery_id":"old-terminal-recovery"}]
        });
        let adapter = Adapter::new(&plan, &seed).unwrap();
        assert_eq!(adapter.identities.len(), 2);
        assert!(adapter.recoveries.is_empty());
    }
    #[test]
    fn complete_book_last_trade_survives_the_ws_reducer_and_later_price_changes() {
        let mut a = fixture();
        let now = Utc::now();
        let now = now.with_nanosecond(now.nanosecond() / 1000 * 1000).unwrap();
        let mut snapshot = book("11", now);
        snapshot["tick_size"] = json!("0.01");
        snapshot["last_trade_price"] = json!("0.41");
        let rest = snapshot_event(
            &snapshot,
            SnapshotBoundary {
                market_id: "1",
                condition_id: "condition",
                token_id: "11",
                recovery_id: "comparison",
                cursor: 1,
                received_at: now,
            },
        )
        .unwrap();
        let first = a.apply(snapshot, now, 0).unwrap();
        assert_eq!(first[0]["canonical_payload"]["last_trade_price"], "0.41");
        assert_eq!(
            first[0]["canonical_payload"]["last_trade_price"],
            rest["canonical_payload"]["last_trade_price"]
        );
        assert_eq!(
            first[0]["canonical_payload"]["state_checksum"],
            rest["canonical_payload"]["state_checksum"]
        );
        let delta = json!({"event_type":"price_change","market":"condition",
            "timestamp":now.timestamp_millis().to_string(),
            "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"7",
                "best_bid":"0.4","best_ask":"0.6"}]});
        let second = a.apply(delta, now, 1).unwrap();
        assert_eq!(second[0]["canonical_payload"]["last_trade_price"], "0.41");

        let mut empty = fixture();
        let mut empty_snapshot = book("11", now);
        empty_snapshot["last_trade_price"] = json!("");
        let event = empty.apply(empty_snapshot, now, 0).unwrap();
        assert!(event[0]["canonical_payload"]["last_trade_price"].is_null());

        let mut invalid = fixture();
        let mut invalid_snapshot = book("11", now);
        invalid_snapshot["last_trade_price"] = json!("not-a-decimal");
        assert!(invalid.apply(invalid_snapshot, now, 0).is_err());
    }
    #[test]
    fn exact_replay_does_not_advance_or_refresh_and_reconnect_reseeds() {
        let mut a = fixture();
        let now = Utc::now();
        let raw = book("11", now);
        a.apply(raw.clone(), now, 0).unwrap();
        let epoch = a.epochs["11"].clone();
        let later = now + chrono::Duration::seconds(1);
        assert!(a.apply(raw.clone(), later, 1).unwrap().is_empty());
        assert_eq!(a.book_received["11"], now);
        assert_eq!(a.sequences["11"], 1);
        assert_eq!(a.engines["11"].projection().cursor, 1);
        assert_eq!(a.epochs["11"], epoch);
        a.apply(json!({"event_type":"source_gap","asset_id":"11"}), later, 1)
            .unwrap();
        let fresh = a.apply(raw, later, 1).unwrap();
        assert_eq!(fresh[0]["cursor"], 2);
        assert_ne!(a.epochs["11"], epoch);
    }
    #[test]
    fn repeated_level_with_new_transport_identity_becomes_memory_confirmation() {
        let mut a = fixture();
        let now = Utc::now();
        a.apply(book("11", now), now, 0).unwrap();
        let later = now + chrono::Duration::seconds(1);
        let unchanged = json!({"event_type":"price_change","market":"condition",
            "timestamp":later.timestamp_millis().to_string(),
            "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"10",
                "best_bid":"0.4","best_ask":"0.6"}]});
        assert!(a.apply(unchanged, later, 1).unwrap().is_empty());
        assert_eq!(a.book_received["11"], now);
        assert_eq!(a.sequences["11"], 1);
        assert_eq!(a.confirmations.len(), 1);
        let confirmation = &a.confirmations[0];
        assert_eq!(confirmation.token_id, "11");
        assert_eq!(confirmation.received_at, later);
        assert_eq!(confirmation.market_id, "1");

        let changed = json!({"event_type":"price_change","market":"condition",
            "timestamp":later.timestamp_millis().to_string(),
            "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"9",
                "best_bid":"0.4","best_ask":"0.6"}]});
        let output = a.apply(changed, later, 1).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(a.book_received["11"], later);
        assert_eq!(a.sequences["11"], 2);
        assert_eq!(a.confirmations.len(), 1);
    }
    #[test]
    fn matching_bbo_is_confirmation_only_and_does_not_create_a_wire_sequence_hole() {
        let mut a = fixture();
        let now = Utc::now();
        a.apply(book("11", now), now, 0).unwrap();
        let later = now + chrono::Duration::seconds(1);
        let bbo = json!({"event_type":"best_bid_ask","asset_id":"11",
            "market":"condition","timestamp":later.timestamp_millis().to_string(),
            "best_bid":"0.4","best_ask":"0.6"});
        assert!(a.apply(bbo, later, 1).unwrap().is_empty());
        assert_eq!(a.sequences["11"], 1);
        assert_eq!(a.book_received["11"], now);
        assert_eq!(a.confirmations.len(), 1);
        assert_eq!(a.confirmations[0].token_id, "11");

        let changed = json!({"event_type":"price_change","market":"condition",
            "timestamp":later.timestamp_millis().to_string(),
            "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"9",
                "best_bid":"0.4","best_ask":"0.6"}]});
        let output = a.apply(changed, later, 1).unwrap();
        assert_eq!(output[0]["sequence"], 2);
        assert_eq!(output[0]["canonical_payload"]["sequence"], 2);
    }
    #[test]
    fn tick_change_does_not_refresh_supplier_quote_clock() {
        let mut a = fixture();
        let now = Utc::now();
        let baseline = a.apply(book("11", now), now, 0).unwrap();
        let quote_source = baseline[0]["canonical_payload"]["exchange_at"].clone();
        let later = now + chrono::Duration::seconds(1);
        let tick = json!({"event_type":"tick_size_change","asset_id":"11",
            "market":"condition","timestamp":later.timestamp_millis().to_string(),
            "old_tick_size":"0.01","new_tick_size":"0.01"});
        let output = a.apply(tick, later, 1).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["raw_payload"]["event_type"], "tick_size_change");
        assert_eq!(output[0]["received_at"], wire_time(later).unwrap());
        assert_eq!(
            output[0]["canonical_payload"]["received_at"],
            output[0]["received_at"]
        );
        assert_eq!(a.book_received["11"], later);
        assert_eq!(a.sequences["11"], 2);
        assert_eq!(output[0]["canonical_payload"]["exchange_at"], quote_source);
    }

    #[test]
    fn trade_source_time_is_separate_from_the_unchanged_book_quote_time() {
        let mut a = fixture();
        let now = Utc::now();
        let baseline = a.apply(book("11", now), now, 0).unwrap();
        let quote_source = baseline[0]["canonical_payload"]["exchange_at"].clone();
        let later = now + chrono::Duration::seconds(1);
        let trade = json!({"event_type":"last_trade_price","asset_id":"11",
            "market":"condition","timestamp":later.timestamp_millis().to_string(),
            "price":"0.41"});
        let output = a.apply(trade, later, 1).unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["canonical_payload"]["exchange_at"], quote_source);
        assert_eq!(output[0]["exchange_at"],
            wire_time(chrono::DateTime::<Utc>::from_timestamp_millis(
                later.timestamp_millis()).unwrap()).unwrap());
        assert_eq!(output[0]["canonical_payload"]["last_trade_price"], "0.41");
    }
    #[test]
    fn delayed_delta_is_delivered_but_disconnect_still_requires_baseline() {
        let mut a = fixture();
        let now = Utc::now();
        a.apply(book("11", now), now, 0).unwrap();
        a.apply(book("12", now), now, 1).unwrap();
        let delta = json!({"event_type":"price_change","market":"condition",
            "timestamp":(now+chrono::Duration::milliseconds(1)).timestamp_millis().to_string(),
            "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"7","best_bid":"0.4","best_ask":"0.6"}]});
        let later = now + chrono::Duration::seconds(7);
        let output = a.apply(delta.clone(), later, 2).unwrap();
        assert_eq!(output[0]["applied"], true);
        assert_eq!(output[0]["raw_payload"], delta);
        assert!(a.recoveries.is_empty());
        a.reset_connection();
        assert_eq!(a.waiting.len(), 2);
        assert!(a.engines.is_empty());
        assert!(a.book_received.is_empty());
        // Old queued increments cannot establish the new connection baseline.
        assert!(a.apply(delta, later, 2).unwrap().is_empty());
        let recovered = a.apply(book("11", later), later, 2).unwrap();
        assert_eq!(recovered[0]["cursor"], 3);
        assert!(!a.waiting.is_empty());
        let recovered = a.apply(book("12", later), later, 3).unwrap();
        assert_eq!(recovered[0]["cursor"], 4);
        assert!(a.waiting.is_empty());
    }
    #[test]
    fn delayed_delivery_preserves_identity_checks() {
        let mut a = fixture();
        let now = Utc::now();
        for _ in 0..3 {
            a.apply(book("11", now), now, 0).unwrap();
            let delta = json!({"event_type":"price_change","market":"condition",
                "timestamp":(now+chrono::Duration::milliseconds(1)).timestamp_millis().to_string(),
                "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"7","best_bid":"0.4","best_ask":"0.6"}]});
            let output = a
                .apply(delta, now + chrono::Duration::seconds(7), 1)
                .unwrap();
            assert_eq!(output[0]["applied"], true);
            a.reset_connection();
            assert_eq!(a.waiting.len(), 2);
        }
        let mut invalid = book("11", now);
        invalid["market"] = json!("wrong");
        let error = a.apply(invalid, now, 0).unwrap_err();
        assert!(error.downcast_ref::<ResyncRequired>().is_none());
    }
    #[test]
    fn isolated_rejection_preserves_other_token_and_recovery_is_ordered() {
        let mut a = fixture();
        let now = Utc::now();
        a.apply(book("11", now), now, 0).unwrap();
        a.apply(book("12", now), now, 1).unwrap();
        let epoch = a.epochs["12"].clone();
        let delta = json!({"event_type":"price_change","market":"wrong-condition",
            "timestamp":(now+chrono::Duration::milliseconds(1)).timestamp_millis().to_string(),
            "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"7"}]});
        let later = now + chrono::Duration::seconds(7);
        let failed = a.apply_isolated(delta.clone(), later, 2).unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0]["event_type"], "recovery_started");
        assert_eq!(failed[0]["token_id"], "11");
        assert_eq!(a.waiting, BTreeSet::from(["11".into()]));
        assert_eq!(a.epochs["12"], epoch);
        assert!(a.apply_isolated(delta, later, 3).unwrap().is_empty());
        let healthy = a.apply_isolated(book("12", later), later, 3).unwrap();
        assert_eq!(healthy[0]["cursor"], 4);
        let recovered = a.apply_isolated(book("11", later), later, 4).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0]["cursor"], 5);
        assert_eq!(recovered[1]["cursor"], 6);
        assert_eq!(recovered[1]["canonical_payload"]["snapshot_cursor"], 5);
        assert_eq!(
            recovered[1]["canonical_payload"]["recovery_id"],
            failed[0]["canonical_payload"]["recovery_id"]
        );
        assert!(a.recoveries.is_empty());
        let tokens = [("11".into(), "1".into()), ("12".into(), "1".into())].into();
        marketcow_runtime::discovery_source::ValidatedSourceBatch::new(
            failed, false, 2, &tokens, 65536,
        )
        .unwrap();
        marketcow_runtime::discovery_source::ValidatedSourceBatch::new(
            recovered, false, 4, &tokens, 65536,
        )
        .unwrap();
    }

    #[test]
    fn exact_rest_book_can_win_fenced_recovery_without_fabricating_raw_ws() {
        let mut adapter = fixture();
        let now = Utc::now().with_nanosecond(0).unwrap();
        adapter.apply(book("11", now), now, 0).unwrap();
        let started = adapter.invalidate("11", "test_gap", now, 2).unwrap();
        let attempt = started["canonical_payload"]["recovery_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut raw = book("11", now);
        raw.as_object_mut().unwrap().remove("event_type");
        raw["tick_size"] = json!("0.01");
        let mut sibling = book("12", now);
        sibling.as_object_mut().unwrap().remove("event_type");
        sibling["tick_size"] = json!("0.01");
        let recovered = adapter
            .apply_rest_recovery(
                vec![raw.clone(), sibling],
                now,
                3,
                &BTreeMap::from([("11".into(), attempt.clone())]),
            )
            .unwrap();
        assert_eq!(recovered.len(), 3);
        assert_eq!(recovered[0]["event_type"], "book");
        assert_eq!(recovered[0]["raw_payload"], raw);
        assert!(recovered[0]["raw_payload"].get("event_type").is_none());
        assert_eq!(recovered[1]["event_type"], "recovery_completed");
        assert_eq!(recovered[1]["canonical_payload"]["snapshot_cursor"], 4);
        assert_eq!(recovered[2]["token_id"], "12");
        let bindings = [("11".into(), "1".into()), ("12".into(), "1".into())].into();
        marketcow_runtime::discovery_source::ValidatedSourceBatch::new(
            recovered, false, 3, &bindings, 65536,
        )
        .unwrap();
        assert!(adapter.recoveries.get("11").is_none());
        assert!(
            adapter
                .apply_rest_recovery(vec![], now, 6, &BTreeMap::from([("11".into(), attempt)]),)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn malformed_atomic_frame_invalidates_its_tokens_without_partial_output() {
        let mut a = fixture();
        let now = Utc::now();
        a.apply(book("11", now), now, 0).unwrap();
        a.apply(book("12", now), now, 1).unwrap();
        let raw = json!({"event_type":"price_change","market":"condition","timestamp":now.timestamp_millis().to_string(),
            "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"7"},
                {"asset_id":"12","side":"BUY","price":"broken","size":"8"}]});
        let events = a.apply_isolated(raw, now, 2).unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|e| e["event_type"] == "recovery_started"));
        assert_eq!(a.waiting.len(), 2);
        assert!(a.engines.is_empty());
    }

    #[test]
    fn wrong_condition_fails_closed_and_timestamp_is_canonical() {
        let mut a = fixture();
        let now = Utc::now();
        let mut raw = book("11", now);
        raw["market"] = json!("wrong");
        assert!(a.apply(raw, now, 0).is_err());
        assert_eq!(
            wire_time(chrono::DateTime::from_timestamp(1700000000, 0).unwrap()).unwrap(),
            "2023-11-14T22:13:20Z"
        );
    }
}
pub async fn run(
    args: &Args,
    plan: &Plan,
    seed: Value,
    publication: &mut Publication,
    acquisition: Option<
        tokio::sync::mpsc::Receiver<super::source_scope_control::AcquisitionRequest>,
    >,
) -> Result<()> {
    ensure!(
        args.cycles.is_none(),
        "--cycles is REST-only; use managed runtime limits for WebSocket"
    );
    let mut adapter = Adapter::new(plan, &seed)?;
    // A clean persisted boundary remains causally valid while the first live
    // connection establishes. Per-book freshness still fails closed until a
    // current WS snapshot or authoritative confirmation arrives. Creating a
    // durable gap for every token on every normal process start caused a
    // scope-wide recovery storm and made unrelated markets unavailable.
    // Actual disconnects are emitted by the transport, while pre-existing
    // durable recoveries retain their original IDs above and continue here.
    let confirmation_markets = plan
        .markets
        .iter()
        .filter(|market| {
            market
                .token_ids
                .iter()
                .all(|token| adapter.identities.contains_key(token))
        })
        .cloned()
        .collect();
    let lifecycle_markets = seed["markets"]
        .as_array()
        .context("market seed")?
        .iter()
        .map(|market| {
            Ok((
                market["identity"]["market_id"]
                    .as_str()
                    .context("seed market id")?
                    .to_owned(),
                market.clone(),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    run_connection(
        &mut adapter,
        publication,
        args.websocket_shard_tokens,
        args.websocket_recovery_concurrency,
        args.market_workers,
        args.websocket_confirmation_seconds,
        args.request_market_batch_size,
        args.concurrency,
        args.response_byte_limit,
        args.request_timeout_seconds,
        confirmation_markets,
        plan.markets.clone(),
        lifecycle_markets,
        acquisition,
        args.acquisition_token_budget.unwrap_or(2048),
        args.acquisition_socket_budget.unwrap_or(1024),
        args.acquisition_lease_required,
        args.acquisition_lease_capacity.unwrap_or(0),
        args.acquisition_lease_max_seconds.unwrap_or(0),
    )
    .await
}

/// A bounded, temporary subscription obtains one authoritative snapshot. Its
/// result carries the recovery attempt; the caller fences obsolete responses.
async fn recover_snapshots(
    tokens: Vec<String>,
) -> Result<Vec<marketcow_polymarket::RawTransportFrame>> {
    ensure!(!tokens.is_empty(), "target recovery token scope is empty");
    let expected: BTreeSet<_> = tokens.iter().cloned().collect();
    ensure!(
        expected.len() == tokens.len(),
        "duplicate target recovery token"
    );
    let (send, mut receive) = transport_output(8, TRANSPORT_BATCH_BYTES, TRANSPORT_BATCH_BYTES)?;
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let mut job = tokio::spawn(run_polymarket_transport(
        PolymarketTransportConfig::production(),
        tokens,
        send,
        shutdown,
    ));
    struct AbortOnDrop(tokio::task::AbortHandle);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _socket_guard = AbortOnDrop(job.abort_handle());
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut snapshots = BTreeMap::new();
        loop {
            tokio::select! {
                frames = receive.recv() => {
                    for frame in frames.context("target recovery closed")? {
                        let token = frame.raw_payload["asset_id"].as_str();
                        if frame.raw_payload["event_type"] == "book"
                            && token.is_some_and(|token| expected.contains(token))
                        {
                            snapshots.insert(token.unwrap().to_owned(), frame);
                            if snapshots.len() == expected.len() {
                                return Ok(snapshots.into_values().collect());
                            }
                        }
                    }
                }
                ended = &mut job => { ended??; anyhow::bail!("target recovery stopped"); }
            }
        }
    })
    .await
    .context("target recovery timeout")
    .and_then(|result| result);
    let _ = stop.send(true);
    // No detached socket after timeout, successful snapshot, or operator stop.
    if !job.is_finished() {
        job.abort();
        let _ = job.await;
    }
    result
}

/// Read one complete, bounded authoritative HTTP snapshot for an exact token
/// pair. This is used only for manager-requested freshness observations; gap
/// recovery continues to use the independently fenced WS recovery path.
async fn confirm_snapshots(
    client: reqwest::Client,
    tokens: Vec<String>,
    response_byte_limit: usize,
) -> Result<Vec<marketcow_polymarket::RawTransportFrame>> {
    ensure!(
        !tokens.is_empty(),
        "target confirmation token scope is empty"
    );
    let expected: BTreeSet<_> = tokens.iter().cloned().collect();
    ensure!(
        expected.len() == tokens.len(),
        "duplicate target confirmation token"
    );
    let request = tokens
        .iter()
        .map(|token| json!({"token_id":token}))
        .collect::<Vec<_>>();
    let mut response = client
        .post(marketcow_polymarket::CLOB_BOOK_SNAPSHOT_SOURCE_URL)
        .json(&request)
        .send()
        .await?
        .error_for_status()?;
    ensure!(
        response
            .content_length()
            .is_none_or(|bytes| bytes <= response_byte_limit as u64),
        "target confirmation response byte budget"
    );
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            body.len()
                .checked_add(chunk.len())
                .is_some_and(|bytes| bytes <= response_byte_limit),
            "target confirmation response byte budget"
        );
        body.extend_from_slice(&chunk);
    }
    let received_at = Utc::now();
    let values: Vec<Value> = serde_json::from_slice(&body)?;
    confirmation_frames(values, &expected, received_at)
}

fn confirmation_frames(
    values: Vec<Value>,
    expected: &BTreeSet<String>,
    received_at: chrono::DateTime<Utc>,
) -> Result<Vec<marketcow_polymarket::RawTransportFrame>> {
    ensure!(
        values.len() == expected.len(),
        "target confirmation incomplete response"
    );
    let mut frames = BTreeMap::new();
    for value in values {
        let mut book = value
            .as_object()
            .cloned()
            .context("target confirmation book object")?;
        let token = book
            .get("asset_id")
            .and_then(Value::as_str)
            .context("target confirmation token")?
            .to_owned();
        ensure!(
            expected.contains(&token) && !frames.contains_key(&token),
            "target confirmation unexpected or duplicate token"
        );
        book.insert("event_type".into(), Value::String("book".into()));
        frames.insert(
            token,
            marketcow_polymarket::RawTransportFrame {
                raw_payload: Value::Object(book),
                received_at,
            },
        );
    }
    ensure!(
        &frames.keys().cloned().collect::<BTreeSet<_>>() == expected,
        "target confirmation token set differs"
    );
    Ok(frames.into_values().collect())
}

type FreshnessResult = (
    super::Market,
    chrono::DateTime<Utc>,
    Result<Vec<marketcow_polymarket::RawTransportFrame>>,
);

type MarketRecoveryResult = (
    String,
    BTreeMap<String, String>,
    Result<Vec<marketcow_polymarket::RawTransportFrame>>,
);

fn catalog_market(record: &Value) -> Result<super::Market> {
    let identity = &record["identity"];
    let token_ids: Vec<String> = identity["outcomes"]
        .as_array()
        .context("lifecycle outcomes")?
        .iter()
        .map(|outcome| {
            outcome["token_id"]
                .as_str()
                .map(str::to_owned)
                .context("lifecycle token")
        })
        .collect::<Result<_>>()?;
    Ok(super::Market {
        market_id: identity["market_id"]
            .as_str()
            .context("lifecycle market id")?
            .to_owned(),
        condition_id: identity["condition_id"]
            .as_str()
            .context("lifecycle condition")?
            .to_owned(),
        token_ids: token_ids
            .try_into()
            .map_err(|_| anyhow::anyhow!("lifecycle market is not binary"))?,
    })
}

fn targeted_confirmation_retry_delay(confirmation_interval_seconds: u64) -> std::time::Duration {
    // A successful targeted snapshot refreshes the book at completion. Its
    // retry cooldown must remain inside the configured confirmation cadence
    // instead of consuming the entire five-second consumer freshness budget
    // before the next WS handshake even starts.
    std::time::Duration::from_secs(confirmation_interval_seconds.max(1))
}

fn recovery_retry_delay(failures: u32) -> std::time::Duration {
    let multiplier = 1_u64 << failures.saturating_sub(1).min(4);
    std::time::Duration::from_secs((5 * multiplier).min(60))
}

/// Fill the bounded targeted-confirmation worker set from a market-keyed
/// queue. The queue cannot exceed the explicit prepared scope because each
/// market owns at most one entry. Capacity pressure delays an item instead of
/// silently dropping it and letting an otherwise quiet book age out.
fn start_freshness_jobs(
    queued: &mut BTreeMap<String, super::Market>,
    pending: &mut BTreeSet<String>,
    jobs: &mut tokio::task::JoinSet<FreshnessResult>,
    maximum_jobs: usize,
) {
    while jobs.len() < maximum_jobs {
        let Some(market_id) = queued.keys().next().cloned() else {
            break;
        };
        let market = queued.remove(&market_id).expect("queued market exists");
        if !pending.insert(market_id) {
            continue;
        }
        let requested = Utc::now();
        jobs.spawn(async move {
            let result = recover_snapshots(market.token_ids.to_vec()).await;
            (market, requested, result)
        });
    }
}

/// Bound reconnect blast radius without splitting the two books of one market.
/// Keeping market pairs together preserves the market-atomic reducer contract
/// even when one upstream connection is recycled independently of its peers.
fn connection_shards(
    identities: &BTreeMap<String, (String, String)>,
    maximum_tokens: usize,
) -> Result<Vec<Vec<String>>> {
    ensure!(
        maximum_tokens >= 2,
        "WS shard must hold a complete market pair"
    );
    let mut by_market = BTreeMap::<String, Vec<String>>::new();
    for (token, (market, _)) in identities {
        by_market
            .entry(market.clone())
            .or_default()
            .push(token.clone());
    }
    ensure!(
        by_market.values().all(|tokens| tokens.len() == 2),
        "each WS market must contain exactly two token books"
    );
    let mut shards = Vec::new();
    let mut current = Vec::new();
    for mut pair in by_market.into_values() {
        pair.sort();
        if !current.is_empty() && current.len() + pair.len() > maximum_tokens {
            shards.push(std::mem::take(&mut current));
        }
        current.extend(pair);
    }
    if !current.is_empty() {
        shards.push(current);
    }
    Ok(shards)
}

fn eligible_markets(adapter: &Adapter, requested: &BTreeSet<String>) -> BTreeSet<String> {
    adapter
        .identities
        .values()
        .map(|(market, _)| market.clone())
        .filter(|market| requested.contains(market))
        .collect()
}
fn eligible_pairs(adapter: &Adapter, markets: &BTreeSet<String>) -> Result<Vec<[String; 2]>> {
    let mut pairs = BTreeMap::<String, Vec<String>>::new();
    for (token, (market, _)) in &adapter.identities {
        if markets.contains(market) {
            pairs.entry(market.clone()).or_default().push(token.clone());
        }
    }
    pairs
        .into_values()
        .map(|mut pair| {
            pair.sort();
            pair.try_into()
                .map_err(|_| anyhow::anyhow!("eligible acquisition market pair"))
        })
        .collect()
}
fn pause_acquisition(
    adapter: &mut Adapter,
    pipeline: &pipeline::Pipeline,
    publication: &Publication,
    jobs: &mut super::source_acquisition_shards::Shards,
) -> Result<()> {
    publication.pause_source()?;
    pipeline.pause_all(adapter)?;
    let tokens = jobs.tokens();
    if !tokens.is_empty() {
        let _ = jobs.request_retire(&tokens)?;
    }
    update_acquisition_metrics(publication, jobs);
    Ok(())
}
fn update_acquisition_metrics(
    publication: &Publication,
    jobs: &super::source_acquisition_shards::Shards,
) {
    publication.set_acquisition_metrics(jobs.statistics(), jobs.management_statistics(),
        jobs.tokens(), jobs.draining_tokens());
}
/// A scope retirement is an owner-local fence from the first irreversible
/// wire/actor action through both the writer receipt and wire release. Control
/// preflight is not a lease lock: only this serial owner can decide the final
/// lease state before beginning retirement.
#[derive(Default)]
struct ScopeRetirementFences(BTreeMap<String, ScopeRetirementFence>);
struct ScopeRetirementFence {
    tokens: BTreeSet<String>,
    ticket: Option<u64>,
}
impl ScopeRetirementFences {
    fn unqueued_markets(&self) -> BTreeSet<String> {
        self.0.iter().filter(|(_, fence)| fence.ticket.is_none())
            .map(|(id, _)| id.clone()).collect()
    }
    fn check_retire(&self, leases: &super::source_acquisition_lease::Leases,
        markets: &BTreeSet<String>) -> Result<()> {
        ensure!(markets.is_disjoint(&leases.market_ids()),
            "active acquisition lease protects retirement");
        ensure!(markets.iter().all(|market| !self.0.contains_key(market)),
            "scope retirement already draining");
        Ok(())
    }
    fn check_admission(&self, markets: &BTreeSet<String>) -> Result<()> {
        ensure!(markets.iter().all(|market| !self.0.contains_key(market)),
            "scope retirement draining; reacquire requires completed receipts");
        Ok(())
    }
    fn begin(&mut self, markets: &BTreeSet<String>,
        identities: &BTreeMap<String, (String, String)>) {
        for market in markets {
            let tokens = identities.iter().filter(|(_, (owner, _))| owner == market)
                .map(|(token, _)| token.clone()).collect();
            self.0.insert(market.clone(), ScopeRetirementFence { tokens, ticket: None });
        }
    }
    fn cancel_unstarted(&mut self, markets: &BTreeSet<String>) {
        for market in markets { self.0.remove(market); }
    }
    fn set_ticket(&mut self, markets: &BTreeSet<String>, ticket: u64) {
        for market in markets {
            self.0.get_mut(market).unwrap().ticket = Some(ticket);
        }
    }
    fn reconcile(&mut self, persisted_ticket: u64, wire_tokens: &BTreeSet<String>) {
        self.0.retain(|_, fence| !fence.ticket.is_some_and(|ticket|
            persisted_ticket >= ticket && fence.tokens.is_disjoint(wire_tokens)));
    }
}
fn lease_status(
    leases: &super::source_acquisition_lease::Leases,
    adapter: &Adapter,
    publication: &Publication,
    requested: &BTreeSet<String>,
    now: std::time::Instant,
) -> Value {
    let mut value = leases.status(&eligible_markets(adapter, requested), now);
    value["source_ready"] = json!(publication.is_source_ready());
    value
}

struct ConfirmationBatch {
    markets: Vec<super::Market>,
    request_started_at: chrono::DateTime<Utc>,
    result: Result<(Vec<Value>, chrono::DateTime<Utc>)>,
}

fn audit_batch_offset(period_seconds: u64, index: usize, batches: usize) -> std::time::Duration {
    if period_seconds == 0 || batches <= 1 || index == 0 {
        return std::time::Duration::ZERO;
    }
    let period_millis = u128::from(period_seconds) * 1_000;
    let offset_millis = period_millis * index as u128 / batches as u128;
    std::time::Duration::from_millis(offset_millis.min(u128::from(u64::MAX)) as u64)
}

async fn run_confirmations(
    markets: Vec<super::Market>,
    market_batch_size: usize,
    concurrency: usize,
    response_byte_limit: usize,
    request_timeout_seconds: u64,
    interval_seconds: u64,
    sender: tokio::sync::mpsc::Sender<ConfirmationBatch>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    network: Arc<tokio::sync::Semaphore>,
    membership: tokio::sync::watch::Receiver<BTreeSet<String>>,
) -> Result<()> {
    // Healthy WebSocket operation must not imply a parallel full-book REST
    // downloader. Zero is the production default and performs no network I/O.
    if interval_seconds == 0 {
        return Ok(());
    }
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(request_timeout_seconds))
        .build()?;
    ensure!(concurrency > 0, "confirmation concurrency");
    let mut jobs = tokio::task::JoinSet::new();
    let batch_count = markets.len().div_ceil(market_batch_size);
    for (batch_index, batch) in markets.chunks(market_batch_size).enumerate() {
        let batch = batch.to_vec();
        let client = client.clone();
        let network = network.clone();
        let sender = sender.clone();
        let mut shutdown = shutdown.clone();
        let mut membership = membership.clone();
        jobs.spawn(async move {
            let offset = audit_batch_offset(interval_seconds, batch_index, batch_count);
            if !offset.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(offset) => {},
                    value = shutdown.changed() => {
                        value?;
                        if *shutdown.borrow() { return Ok(()); }
                    },
                }
            }
            loop {
                if *shutdown.borrow() {
                    return Ok::<(), anyhow::Error>(());
                }
                let batch: Vec<_> = batch
                    .iter()
                    .filter(|market| membership.borrow().contains(&market.market_id))
                    .cloned()
                    .collect();
                if batch.is_empty() {
                    tokio::select! {
                        value=membership.changed()=>{value?;continue;},
                        value=shutdown.changed()=>{value?;if *shutdown.borrow(){return Ok(());}},
                    }
                    continue;
                }
                let cycle_started = tokio::time::Instant::now();
                let permit = tokio::select! {
                    permit = network.clone().acquire_owned() => permit?,
                    value = shutdown.changed() => {
                        value?;
                        if *shutdown.borrow() { return Ok(()); }
                        continue;
                    }
                };
                let request_started_at = Utc::now();
                let result = super::fetch(client.clone(), &batch, response_byte_limit).await;
                drop(permit);
                sender
                    .send(ConfirmationBatch {
                        markets: batch.clone(),
                        request_started_at,
                        result,
                    })
                    .await
                    .context("confirmation consumer stopped")?;
                tokio::select! {
                    _ = tokio::time::sleep_until(
                        cycle_started + std::time::Duration::from_secs(interval_seconds)
                    ) => {},
                    value = shutdown.changed() => {
                        value?;
                        if *shutdown.borrow() { return Ok(()); }
                    },
                }
            }
        });
    }
    drop(sender);
    while let Some(result) = jobs.join_next().await {
        result??;
    }
    Ok(())
}

fn pending_market_recoveries(adapter: &Adapter) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut pending = BTreeMap::<String, BTreeMap<String, String>>::new();
    for (token, attempt) in &adapter.recoveries {
        let Some((market_id, _)) = adapter.identities.get(token) else {
            continue;
        };
        pending
            .entry(market_id.clone())
            .or_default()
            .insert(token.clone(), attempt.clone());
    }
    pending
}

async fn handle_acquisition_socket_failure(
    adapter: &mut Adapter,
    publication: &Publication,
    pipeline: &pipeline::Pipeline,
    tokens: &BTreeSet<String>,
    reason: &str,
    acquisition_enabled: bool,
) -> Result<()> {
    let markets = tokens.iter().filter_map(|token|
        adapter.identities.get(token).map(|(market, _)| market.clone()))
        .collect::<BTreeSet<_>>();
    marketcow_polymarket::async_eprintln!("{}", json!({
        "stage":"market_acquisition_socket_restarted",
        "market_ids":&markets,"reason":reason,
    }));
    if acquisition_enabled {
        for market in markets {
            if pipeline.has_market(&market) {
                pipeline.invalidate_or_fence_market(adapter, publication, &market,
                    "market_socket_task_failed").await?;
            }
        }
    }
    Ok(())
}

async fn run_connection(
    adapter: &mut Adapter,
    publication: &mut Publication,
    websocket_shard_tokens: usize,
    websocket_recovery_concurrency: usize,
    market_workers: usize,
    websocket_confirmation_seconds: u64,
    request_market_batch_size: usize,
    network_concurrency: usize,
    response_byte_limit: usize,
    request_timeout_seconds: u64,
    mut confirmation_markets: Vec<super::Market>,
    lifecycle_candidates: Vec<super::Market>,
    mut lifecycle_markets: BTreeMap<String, Value>,
    mut acquisition: Option<
        tokio::sync::mpsc::Receiver<super::source_scope_control::AcquisitionRequest>,
    >,
    acquisition_token_budget: usize,
    acquisition_socket_budget: usize,
    acquisition_lease_required: bool,
    acquisition_lease_capacity: usize,
    acquisition_lease_max_seconds: u64,
) -> Result<()> {
    let (mut pipeline, mut completed_markets) =
        pipeline::Pipeline::start_with_workers(adapter, publication, market_workers)?;
    let mut acquisition_tokens: BTreeSet<_> = adapter.identities.keys().cloned().collect();
    let transport_budget = transport_budget(
        TRANSPORT_QUEUE_BATCHES,
        TRANSPORT_QUEUE_BYTES,
        TRANSPORT_BATCH_BYTES,
    )?;
    let transport_metrics = transport_budget.clone();
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let mut jobs = super::source_acquisition_shards::Shards::new(
        PolymarketTransportConfig::production(),
        transport_budget,
        acquisition_token_budget,
        acquisition_socket_budget,
        websocket_shard_tokens,
    )?;
    let mut initial_pairs = BTreeMap::<String, Vec<String>>::new();
    for (token, (market, _)) in &adapter.identities {
        initial_pairs
            .entry(market.clone())
            .or_default()
            .push(token.clone());
    }
    let initial_pairs: Vec<[String; 2]> = initial_pairs
        .into_values()
        .map(|pair| {
            pair.try_into()
                .map_err(|_| anyhow::anyhow!("initial acquisition market pair"))
        })
        .collect::<Result<_>>()?;
    let mut leases = super::source_acquisition_lease::Leases::new(
        acquisition_lease_required,
        acquisition_lease_capacity,
        std::time::Duration::from_secs(acquisition_lease_max_seconds),
    )?;
    let mut scope_retirements = ScopeRetirementFences::default();
    if leases.enabled() {
        jobs.add_pairs(&initial_pairs)?;
    } else {
        pause_acquisition(adapter, &pipeline, publication, &mut jobs)?;
    }
    update_acquisition_metrics(publication, &jobs);
    let (confirmation_sender, mut confirmation_receiver) =
        tokio::sync::mpsc::channel(network_concurrency);
    let confirmation_shutdown = shutdown.clone();
    let confirmation_network = Arc::new(tokio::sync::Semaphore::new(network_concurrency));
    let (confirmation_membership, confirmation_members) =
        tokio::sync::watch::channel(if leases.enabled() {
            confirmation_markets
                .iter()
                .map(|market| market.market_id.clone())
                .collect::<BTreeSet<_>>()
        } else {
            BTreeSet::new()
        });
    let mut added_confirmation_jobs = tokio::task::JoinSet::new();
    let confirmation_job = tokio::spawn(run_confirmations(
        confirmation_markets.clone(),
        request_market_batch_size,
        network_concurrency,
        response_byte_limit,
        request_timeout_seconds,
        websocket_confirmation_seconds,
        confirmation_sender.clone(),
        confirmation_shutdown,
        confirmation_network.clone(),
        confirmation_members.clone(),
    ));
    let lifecycle_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(request_timeout_seconds))
        .build()?;
    let lifecycle_network = Arc::new(tokio::sync::Semaphore::new(network_concurrency));
    let mut lifecycle_jobs = tokio::task::JoinSet::<(super::Market, Result<Option<Value>>)>::new();
    let mut lifecycle_pending = BTreeSet::new();
    // The durable lifecycle overlay is authoritative across process restarts.
    // Seed the runtime fence from it instead of waiting for another Gamma read.
    let mut lifecycle_terminal: BTreeSet<String> = lifecycle_markets
        .iter()
        .filter(|(_, market)| {
            matches!(
                market["lifecycle_state"].as_str(),
                Some("closed" | "resolved" | "invalid")
            )
        })
        .map(|(market, _)| market.clone())
        .collect();
    let mut lifecycle_retry_at = BTreeMap::<String, tokio::time::Instant>::new();
    // Terminal publication fences reducers immediately. Wire unsubscribe is
    // reconciled independently because its receipt can be delayed by another
    // bounded shard command. One entry per market keeps this state bounded.
    let mut terminal_unsubscribes = BTreeMap::<String, BTreeSet<String>>::new();
    let mut recovery_jobs = tokio::task::JoinSet::<MarketRecoveryResult>::new();
    let mut freshness_jobs = tokio::task::JoinSet::<FreshnessResult>::new();
    let mut manager_freshness_jobs = tokio::task::JoinSet::<FreshnessResult>::new();
    let mut recovering_markets = BTreeSet::new();
    let mut freshness_pending = BTreeSet::new();
    let mut manager_freshness_pending = BTreeSet::new();
    let mut freshness_queued = BTreeMap::<String, super::Market>::new();
    let mut freshness_retry_at = BTreeMap::<String, tokio::time::Instant>::new();
    let mut rest_submitted = BTreeMap::<String, String>::new();
    let mut recovery_retry_at = BTreeMap::<String, tokio::time::Instant>::new();
    let mut recovery_failures = BTreeMap::<String, u32>::new();
    let mut retry_tick = tokio::time::interval(std::time::Duration::from_secs(1));
    retry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut transport_metrics_at = tokio::time::Instant::now();
    let shutdown_requested = super::shutdown_signal();
    tokio::pin!(shutdown_requested);
    // A copied generation can already contain lifecycle-only terminal rows
    // from an earlier run. Re-observe only those bounded plan members whose
    // payout remained unresolved; never rescan the catalog.
    for market in if leases.enabled() {
        lifecycle_candidates
    } else {
        Vec::new()
    } {
        let seed_market = lifecycle_markets
            .get(&market.market_id)
            .context("lifecycle candidate missing from seed")?;
        let terminal = matches!(
            seed_market["lifecycle_state"].as_str(),
            Some("closed" | "resolved" | "invalid")
        );
        if terminal && seed_market["resolution"].is_null() {
            lifecycle_pending.insert(market.market_id.clone());
            let client = lifecycle_client.clone();
            let network = lifecycle_network.clone();
            lifecycle_jobs.spawn(async move {
                let _permit = network.acquire_owned().await;
                let result =
                    super::source_lifecycle::refresh(client, market.clone(), response_byte_limit)
                        .await;
                (market, result)
            });
        }
    }
    publication.set_source_ready(leases.enabled());
    let mut pending_transport: Option<RawTransportBatch> = None;
    let mut transport_dwell_started = std::time::Instant::now();
    let mut transport_dwell = BTreeMap::<String, (u64, u128, u64, u64)>::new();
    let result:Result<()>=async {
        loop {
            jobs.reap_finished()?;
            if let Some((tokens, reason)) = jobs.take_failure() {
                if pending_transport.as_ref().is_some_and(|batch|
                    batch.source_tokens() == &tokens) {
                    pending_transport = None;
                }
                handle_acquisition_socket_failure(adapter, publication, &pipeline,
                    &tokens, reason, leases.enabled()).await?;
                update_acquisition_metrics(publication, &jobs);
                continue;
            }
            // Keep control and shutdown branches selectable even while one
            // overflow batch is pending. Never take that batch inside the
            // select future: cancellation would silently drop it.
            let frames=tokio::select! {
                request=async {match acquisition.as_mut(){Some(rx)=>rx.recv().await,None=>std::future::pending().await}}=>{
                    let Some(request)=request else {acquisition=None;continue;};
                    let result=(||->Result<Value>{
                        use super::source_scope_control::AcquisitionAction;
                        let command_now=std::time::Instant::now();
                        scope_retirements.reconcile(publication.reader().retirement_status()?.1,&jobs.tokens());
                        if leases.required(){
                            let expired=leases.expire(command_now);let after=leases.market_ids();
                            if expired>0 {
                                confirmation_membership.send_replace(after.clone());
                            }
                            if after.is_empty()&&expired>0 {
                                pause_acquisition(adapter,&pipeline,publication,&mut jobs)?;
                            } else if !after.is_empty() {
                                // Reconcile orphaned wire ownership on every
                                // command, not only on the expiry edge. A busy
                                // shard command queue therefore cannot turn a
                                // failed first retirement attempt into an
                                // unbounded subscription leak.
                                let desired_tokens:BTreeSet<_>=eligible_pairs(adapter,&after)?.into_iter().flatten().collect();
                                let excess:BTreeSet<_>=jobs.tokens().difference(&desired_tokens).cloned().collect();
                                if !excess.is_empty(){let _=jobs.request_retire(&excess)?;}
                                update_acquisition_metrics(publication, &jobs);
                            }
                        }
                        let request=match request.action {
                            AcquisitionAction::LeaseStatus{eligible_market_ids}=>return Ok(
                                lease_status(&leases,adapter,publication,&eligible_market_ids,command_now)),
                            AcquisitionAction::RetirementStatus{market_ids}=>return Ok(json!({
                                "draining_market_ids":market_ids.into_iter()
                                    .filter(|id|scope_retirements.0.contains_key(id)).collect::<Vec<_>>()
                            })),
                            AcquisitionAction::AcquireLease{lease_id,ttl_seconds,market_ids,eligible_market_ids}=>{
                                scope_retirements.check_admission(&market_ids)?;
                                let now=std::time::Instant::now();let eligible=eligible_markets(adapter,&eligible_market_ids);
                                let was_enabled=leases.enabled();
                                leases.validate_acquire(&lease_id,std::time::Duration::from_secs(ttl_seconds),&market_ids,&eligible,now)?;
                                let before=leases.market_ids();
                                let mut after=before.clone();after.extend(market_ids.iter().cloned());
                                let added:BTreeSet<_>=after.difference(&before).cloned().collect();
                                let pairs=eligible_pairs(adapter,&added)?;jobs.validate_add_pairs(&pairs)?;
                                if !pairs.is_empty(){
                                    jobs.add_pairs(&pairs)?;
                                }
                                leases.acquire(lease_id,std::time::Duration::from_secs(ttl_seconds),market_ids,&eligible,now)?;
                                publication.resume_acquisition();
                                if after!=before {
                                    confirmation_membership.send_replace(after.clone());
                                    acquisition_tokens=jobs.tokens();
                                    update_acquisition_metrics(publication, &jobs);
                                }
                                if !was_enabled {
                                    for record in lifecycle_markets.values().filter(|record|matches!(record["lifecycle_state"].as_str(),
                                        Some("closed"|"resolved"|"invalid"))&&record["resolution"].is_null()) {
                                        let market=catalog_market(record)?;
                                        if !lifecycle_pending.insert(market.market_id.clone()){continue;}
                                        let client=lifecycle_client.clone();let network=lifecycle_network.clone();
                                        lifecycle_jobs.spawn(async move {let _permit=network.acquire_owned().await;
                                            let result=super::source_lifecycle::refresh(client,market.clone(),response_byte_limit).await;
                                            (market,result)});
                                    }
                                }
                                return Ok(lease_status(&leases,adapter,publication,&eligible_market_ids,now));
                            },
                            AcquisitionAction::RenewLease{lease_id,ttl_seconds}=>{
                                let now=std::time::Instant::now();leases.renew(&lease_id,std::time::Duration::from_secs(ttl_seconds),now)?;
                                let active=leases.market_ids();
                                return Ok(lease_status(&leases,adapter,publication,&active,now));
                            },
                            AcquisitionAction::RefreshLeaseMarkets{lease_id,market_ids}=>{
                                let now=std::time::Instant::now();
                                let owned=leases.lease_market_ids(&lease_id,now)?;
                                ensure!(!market_ids.is_empty()&&market_ids.is_subset(&owned),
                                    "freshness request must be a nonempty subset of its lease");
                                let mut accepted=Vec::new();
                                for market_id in &market_ids {
                                    let record=lifecycle_markets.get(market_id)
                                        .context("freshness market missing")?;
                                    let market=catalog_market(record)?;
                                    if !manager_freshness_pending.insert(market_id.clone()) { continue; }
                                    let requested=Utc::now();
                                    let client=lifecycle_client.clone();
                                    manager_freshness_jobs.spawn(async move {
                                        let result=confirm_snapshots(client,market.token_ids.to_vec(),response_byte_limit).await;
                                        (market,requested,result)
                                    });
                                    accepted.push(market_id.clone());
                                }
                                return Ok(json!({"schema_version":"marketcow.acquisition-freshness-request.v1",
                                    "status":if accepted.is_empty(){"coalesced"}else{"accepted"},
                                    "lease_id":lease_id,"market_ids":market_ids,
                                    "accepted_market_ids":accepted,
                                    "pending_markets":manager_freshness_pending.len()}));
                            },
                            AcquisitionAction::ReplaceLease{lease_id,ttl_seconds,expected_market_ids,market_ids,
                                eligible_market_ids,removed_token_ids,added_token_ids}=>{
                                scope_retirements.check_admission(&market_ids)?;
                                let now=std::time::Instant::now();
                                let expected=eligible_markets(adapter,&expected_market_ids);
                                let available=eligible_markets(adapter,&eligible_market_ids);
                                let desired=eligible_markets(adapter,&market_ids);
                                ensure!(expected==expected_market_ids&&desired==market_ids&&desired.is_subset(&available),
                                    "replacement lease contains unprepared market");
                                ensure!(expected.difference(&desired).count()==1&&desired.difference(&expected).count()==1,
                                    "replacement lease must rotate one market");
                                leases.validate_replace(&lease_id,std::time::Duration::from_secs(ttl_seconds),&expected,&desired,&available,now)?;
                                let union_before=leases.market_ids();
                                let union_after=leases.union_after_replacement(&lease_id,&desired)?;
                                let removed_union:BTreeSet<_>=union_before.difference(&union_after).cloned().collect();
                                let added_union:BTreeSet<_>=union_after.difference(&union_before).cloned().collect();
                                let removed_market=expected.difference(&desired).next().unwrap();
                                let added_market=desired.difference(&expected).next().unwrap();
                                ensure!(removed_token_ids.iter().all(|token|adapter.identities.get(token).is_some_and(|(market,_)|market==removed_market)),
                                    "replacement removed token binding differs");
                                ensure!(added_token_ids.iter().all(|token|adapter.identities.get(token).is_some_and(|(market,_)|market==added_market)),
                                    "replacement added token binding differs");
                                let _=jobs.poll_retirements()?;
                                if removed_union.len()==1&&added_union.len()==1 {
                                    if !jobs.request_pair_replacement(&removed_token_ids,&added_token_ids)? {
                                        return Ok(json!({"schema_version":"marketcow.acquisition-lease-replacement.v1",
                                            "status":"wire_membership_pending","lease_id":lease_id,
                                            "expected_market_ids":expected,"market_ids":desired,
                                            "wire_token_count":jobs.tokens().len(),"maximum_token_budget_preserved":true}));
                                    }
                                } else if removed_union.len()==1&&added_union.is_empty() {
                                    let tokens:BTreeSet<_>=removed_token_ids.iter().cloned().collect();
                                    if !jobs.request_retire(&tokens)? {
                                        return Ok(json!({"schema_version":"marketcow.acquisition-lease-replacement.v1",
                                            "status":"wire_membership_pending","lease_id":lease_id,
                                            "expected_market_ids":expected,"market_ids":desired,
                                            "wire_token_count":jobs.tokens().len(),"maximum_token_budget_preserved":true}));
                                    }
                                } else if removed_union.is_empty()&&added_union.len()==1 {
                                    let pairs=eligible_pairs(adapter,&added_union)?;jobs.validate_add_pairs(&pairs)?;jobs.add_pairs(&pairs)?;
                                } else {
                                    ensure!(removed_union.is_empty()&&added_union.is_empty(),"replacement shared union differs by more than one market");
                                }
                                leases.replace(&lease_id,std::time::Duration::from_secs(ttl_seconds),&expected,
                                    desired.clone(),&available,now)?;
                                confirmation_membership.send_replace(union_after.clone());
                                acquisition_tokens=jobs.tokens();
                                update_acquisition_metrics(publication, &jobs);
                                // A retained next window becomes current at the
                                // UTC boundary. Its last real mutation may be
                                // older than the execution freshness budget,
                                // while the newly added next pair also needs a
                                // current authoritative baseline. Reuse the
                                // existing bounded targeted-WS recovery path
                                // once per lease replacement; never turn this
                                // into periodic whole-scope polling.
                                for market_id in &desired {
                                    let record = lifecycle_markets.get(market_id)
                                        .context("replacement freshness market missing")?;
                                    freshness_queued.insert(
                                        market_id.clone(),
                                        catalog_market(record)?,
                                    );
                                }
                                start_freshness_jobs(
                                    &mut freshness_queued,
                                    &mut freshness_pending,
                                    &mut freshness_jobs,
                                    websocket_recovery_concurrency,
                                );
                                return Ok(json!({"schema_version":"marketcow.acquisition-lease-replacement.v1",
                                    "status":"complete","lease":lease_status(&leases,adapter,publication,&available,now),
                                    "wire_token_count":acquisition_tokens.len(),"maximum_token_budget_preserved":true}));
                            },
                            AcquisitionAction::ReleaseLease{lease_id}=>{
                                let now=std::time::Instant::now();
                                let before=leases.market_ids();let after=leases.union_after_release(&lease_id)?;
                                let removed:BTreeSet<_>=before.difference(&after).cloned().collect();
                                let removed_tokens:BTreeSet<_>=eligible_pairs(adapter,&removed)?.into_iter().flatten().collect();
                                let _=jobs.poll_retirements()?;
                                if !removed_tokens.is_empty()&&!jobs.request_retire(&removed_tokens)? {
                                    return Ok(json!({"schema_version":"marketcow.acquisition-lease-release.v1",
                                        "status":"wire_membership_pending","lease_id":lease_id,
                                        "market_ids":after,"wire_token_count":jobs.tokens().len()}));
                                }
                                leases.release(&lease_id)?;
                                if !leases.enabled(){
                                    confirmation_membership.send_replace(BTreeSet::new());
                                    pause_acquisition(adapter,&pipeline,publication,&mut jobs)?;
                                } else {
                                    confirmation_membership.send_replace(after.clone());
                                }
                                acquisition_tokens=jobs.tokens();update_acquisition_metrics(publication, &jobs);
                                return Ok(lease_status(&leases,adapter,publication,&after,now));
                            },
                            AcquisitionAction::Scope{validate_only,catalog_revision,records,evidence_sha256,
                                acquisition_market_ids,retire_market_ids}=>ScopeAcquisition{validate_only,catalog_revision,
                                    records,evidence_sha256,acquisition_market_ids,retire_market_ids},
                        };
                        if !request.retire_market_ids.is_empty() {
                            let removed=&request.retire_market_ids;
                            // The control worker may have persisted its intent
                            // before a competing lease reached this serial owner.
                            // Return an explicit pre-effect rejection so only
                            // this case may safely cancel that journal intent.
                            if !removed.is_disjoint(&leases.market_ids()) {
                                if request.validate_only {
                                    anyhow::bail!("active acquisition lease protects retirement");
                                }
                                return Ok(json!({"retirement_rejected_before_apply":"active_lease"}));
                            }
                            scope_retirements.check_retire(&leases,removed)?;
                            publication.check_retirement(removed.clone())?;
                            if request.validate_only{return Ok(json!({"retirement_validated":true,"publication_applied":false}));}
                            let retired_tokens:BTreeSet<_>=adapter.identities.iter().filter(|(_, (market,_))|removed.contains(market)).map(|(t,_)|t.clone()).collect();
                            scope_retirements.begin(removed,&adapter.identities);
                            let wire_retire_tokens:BTreeSet<_>=retired_tokens.intersection(&jobs.tokens()).cloned().collect();
                            if !wire_retire_tokens.is_empty(){
                                if let Err(error)=jobs.begin_retire(&wire_retire_tokens){
                                    scope_retirements.cancel_unstarted(removed);
                                    // begin_retire reserves every affected shard command slot
                                    // before sending or mutating any shard. A failure here is
                                    // therefore provably pre-effect, unlike a lost receipt after
                                    // the command was accepted. Tell the journal owner so it can
                                    // cancel only the matching durable intent.
                                    marketcow_polymarket::async_eprintln!("{}", json!({
                                        "stage":"scope_retirement_rejected_before_wire_apply",
                                        "error":error.to_string(),
                                    }));
                                    return Ok(json!({"retirement_rejected_before_apply":
                                        "wire_retirement_preflight"}));
                                }
                            }
                            for market in removed {
                                if pipeline.has_market(market){pipeline.remove_market(adapter,market)?;}
                                lifecycle_markets.remove(market);lifecycle_pending.remove(market);lifecycle_terminal.remove(market);
                                lifecycle_retry_at.remove(market);freshness_queued.remove(market);freshness_pending.remove(market);freshness_retry_at.remove(market);
                            }
                            confirmation_markets.retain(|m|!removed.contains(&m.market_id));
                            confirmation_membership.send_replace(confirmation_markets.iter().map(|m|m.market_id.clone()).collect());
                            acquisition_tokens=if leases.required(){jobs.tokens()}else{adapter.identities.keys().cloned().collect()};
                            let ticket = publication.reader().retirement_status()?.0
                                .checked_add(1).context("scope retirement ticket overflow")?;
                            publication.retire_catalog_markets(removed.clone())?;
                            scope_retirements.set_ticket(removed,ticket);
                            update_acquisition_metrics(publication, &jobs);
                            return Ok(json!({"retirement_queued":true,"retired_market_ids":removed,
                                "source_cursor":publication.cursor()?,"wire_unsubscribe_confirmed":false}));
                        }
                        ensure!(!request.records.is_empty()&&request.records.len()<=4096,"acquisition record budget");
                        let mut ids=BTreeSet::new();let mut added=vec![];
                        let subscribed=jobs.tokens();
                        for record in &request.records {
                            let identity=&record["identity"];
                            let id=identity["market_id"].as_str().context("acquisition market id")?;
                            ensure!(ids.insert(id.to_owned()),"duplicate acquisition market");
                            if record["lifecycle_state"]!="active" || !request.acquisition_market_ids.contains(id) {continue;}
                            let tokens:Vec<String>=identity["outcomes"].as_array().context("acquisition outcomes")?.iter()
                                .map(|outcome|outcome["token_id"].as_str().map(str::to_owned).context("acquisition token")).collect::<Result<_>>()?;
                            let market=super::Market{market_id:id.into(),condition_id:identity["condition_id"].as_str().context("acquisition condition")?.into(),
                                token_ids:tokens.try_into().map_err(|_|anyhow::anyhow!("acquisition binary pair"))?};
                            let actor_absent=market.token_ids.iter().all(|t|!adapter.identities.contains_key(t));
                            if !actor_absent {ensure!(market.token_ids.iter().all(|t|adapter.identities.get(t)==Some(&(market.market_id.clone(),market.condition_id.clone()))),
                                "retained acquisition identity differs");}
                            if market.token_ids.iter().all(|t|!subscribed.contains(t)){added.push(market);}
                            else {ensure!(!actor_absent&&market.token_ids.iter().all(|t|subscribed.contains(t)),"acquisition retirement still pending");}
                        }
                        scope_retirements.check_admission(&ids)?;
                        let leased_markets = leases.market_ids();
                        let wire_added:Vec<_>=added.iter()
                            .filter(|market|!leases.required()
                                || leased_markets.contains(&market.market_id))
                            .cloned().collect();
                        let wire_pairs:Vec<_>=wire_added.iter()
                            .map(|market|market.token_ids.clone()).collect();
                        jobs.validate_add_pairs(&wire_pairs)?;
                        if request.validate_only {
                            publication.check_catalog_admission(&request.catalog_revision,request.records.clone(),&request.evidence_sha256,&request.acquisition_market_ids,acquisition_token_budget)?;
                            return Ok(json!({"acquisition_validated":true,"publication_applied":false}));
                        }
                        publication.admit_catalog_markets_scoped(&request.catalog_revision,request.records.clone(),&request.evidence_sha256,
                            &request.acquisition_market_ids,acquisition_token_budget)?;
                        for market in &added {pipeline.install_admitted_market(adapter,publication,market)?;}
                        if leases.enabled(){jobs.add_pairs(&wire_pairs)?;}
                        update_acquisition_metrics(publication, &jobs);
                        acquisition_tokens=if leases.required(){jobs.tokens()}else{adapter.identities.keys().cloned().collect()};
                        for record in request.records {lifecycle_markets.insert(record["identity"]["market_id"].as_str().unwrap().to_owned(),record);}
                        let added_ids:Vec<_>=added.iter().map(|m|m.market_id.clone()).collect();
                        if !wire_added.is_empty()&&leases.enabled() {
                            confirmation_markets.extend(wire_added.clone());
                            confirmation_membership.send_replace(confirmation_markets.iter().map(|m|m.market_id.clone()).collect());
                            added_confirmation_jobs.spawn(run_confirmations(wire_added,request_market_batch_size,network_concurrency,
                                response_byte_limit,request_timeout_seconds,websocket_confirmation_seconds,
                                confirmation_sender.clone(),shutdown.clone(),confirmation_network.clone(),confirmation_members.clone()));
                        }
                        Ok(json!({"acquisition_installed":true,"added_market_ids":added_ids,
                            "requested_market_ids":ids,"source_cursor":publication.cursor()?,
                            "books_all_ready_required":false,"publication_applied":false}))
                    })();
                    let _=request.receipt.send(result);continue;
                },
                result=added_confirmation_jobs.join_next(),if !added_confirmation_jobs.is_empty()=>{
                    result.context("added confirmation task missing")???;
                    continue;
                },
                value=async {
                    if pending_transport.is_some() {
                        None
                    } else {
                        Some(jobs.next_batch_or_failure().await)
                    }
                }=>match value {
                    Some(result) => match result? {
                        super::source_acquisition_shards::AcquisitionEvent::Batch(batch) => batch,
                        super::source_acquisition_shards::AcquisitionEvent::ShardFailed { tokens, reason } => {
                            handle_acquisition_socket_failure(adapter, publication, &pipeline,
                                &tokens, reason, leases.enabled()).await?;
                            update_acquisition_metrics(publication, &jobs);
                            continue;
                        }
                    },
                    None => pending_transport.take().context("pending transport batch missing")?,
                },
                completed=completed_markets.recv()=>{
                    pipeline.publish(adapter, publication, completed.context("market workers stopped")?).await?;
                    continue;
                },
                confirmation=confirmation_receiver.recv()=>{
                    let confirmation = confirmation.context("REST confirmation stopped")?;
                    if !leases.enabled(){continue;}
                    match confirmation.result {
                        Err(error) => marketcow_polymarket::async_eprintln!("{}", json!({"stage":"rest_confirmation_request_failed","detail":error.to_string()})),
                        Ok((books, received_at)) => {
                            for market in confirmation.markets {
                                if !confirmation_membership.borrow().contains(&market.market_id){continue;}
                                for _ in 0..market_workers * 2 {
                                    match completed_markets.try_recv() {
                                        Ok(done) => pipeline.publish(adapter, publication, done).await?,
                                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => anyhow::bail!("market workers stopped"),
                                    }
                                }
                                tokio::task::yield_now().await;
                                let selected:Vec<_> = books.iter().filter(|book| market.token_ids.iter()
                                    .any(|token| book["asset_id"] == *token)).cloned().collect();
                                if selected.len() != 2 {
                                    marketcow_polymarket::async_eprintln!("{}",json!({"stage":"rest_confirmation_incomplete",
                                        "market_id":market.market_id,"returned_token_count":selected.len()}));
                                    let due = lifecycle_retry_at.get(&market.market_id)
                                        .is_none_or(|at| *at <= tokio::time::Instant::now());
                                    if !lifecycle_terminal.contains(&market.market_id)
                                        && !lifecycle_pending.contains(&market.market_id) && due
                                    {
                                        lifecycle_pending.insert(market.market_id.clone());
                                        let client = lifecycle_client.clone();
                                        let network = lifecycle_network.clone();
                                        lifecycle_jobs.spawn(async move {
                                            let _permit = network.acquire_owned().await;
                                            let result = super::source_lifecycle::refresh(
                                                client, market.clone(), response_byte_limit,
                                            ).await;
                                            (market, result)
                                        });
                                    }
                                    continue;
                                }
                                let pending:Vec<_> = market.token_ids.iter().filter_map(|token| adapter.recoveries.get(token)
                                    .map(|attempt|(token.clone(),attempt.clone()))).collect();
                                if !pending.is_empty() {
                                    if pending.iter().any(|(token,attempt)| rest_submitted.get(token) != Some(attempt)) {
                                        if pipeline.submit_rest_recovery(adapter, publication, selected, received_at,
                                            pending.clone()).await? {
                                            for (token,attempt) in pending { rest_submitted.insert(token, attempt); }
                                        }
                                    }
                                    continue;
                                }
                                match publication.confirm_market(&market.market_id, &market.condition_id, &market.token_ids,
                                    &selected, confirmation.request_started_at, received_at) {
                                    Ok(ConfirmationOutcome::Confirmed) => {},
                                    Ok(ConfirmationOutcome::Superseded) => {
                                        marketcow_polymarket::async_eprintln!("{}",json!({
                                            "stage":"rest_confirmation_superseded","market_id":market.market_id
                                        }));
                                        // REST and WS are independent official read
                                        // surfaces. If REST remains behind a newer WS
                                        // book, obtain a fresh complete pair from one
                                        // bounded temporary WS subscription instead of
                                        // aging out the market or overwriting newer state.
                                        let due = freshness_retry_at.get(&market.market_id)
                                            .is_none_or(|at| *at <= tokio::time::Instant::now());
                                        if !freshness_pending.contains(&market.market_id) && due {
                                            freshness_queued
                                                .insert(market.market_id.clone(), market.clone());
                                            start_freshness_jobs(
                                                &mut freshness_queued,
                                                &mut freshness_pending,
                                                &mut freshness_jobs,
                                                websocket_recovery_concurrency,
                                            );
                                        }
                                    },
                                    Ok(ConfirmationOutcome::Mismatch) => {
                                        marketcow_polymarket::async_eprintln!("{}",json!({"stage":"rest_confirmation_mismatch","market_id":market.market_id}));
                                        // REST and the main WS stream can expose different
                                        // current surfaces around the same update. REST is a
                                        // freshness witness, not permission to invalidate a
                                        // healthy WS-primary book. Resolve the divergence with
                                        // one bounded official WS full snapshot; only that
                                        // snapshot may atomically replace the pair.
                                        let due = freshness_retry_at.get(&market.market_id)
                                            .is_none_or(|at| *at <= tokio::time::Instant::now());
                                        if !freshness_pending.contains(&market.market_id) && due {
                                            freshness_queued
                                                .insert(market.market_id.clone(), market.clone());
                                            start_freshness_jobs(
                                                &mut freshness_queued,
                                                &mut freshness_pending,
                                                &mut freshness_jobs,
                                                websocket_recovery_concurrency,
                                            );
                                        }
                                    },
                                    Err(error) => marketcow_polymarket::async_eprintln!("{}",json!({"stage":"rest_confirmation_invalid","market_id":market.market_id,"detail":error.to_string()})),
                                }
                            }
                        }
                    }
                    continue;
                },
                completed=lifecycle_jobs.join_next(), if !lifecycle_jobs.is_empty()=>{
                    let (market, result) = completed.context("lifecycle task missing")??;
                    if !lifecycle_markets.contains_key(&market.market_id){continue;}
                    lifecycle_pending.remove(&market.market_id);
                    if !leases.enabled(){continue;}
                    lifecycle_retry_at.insert(
                        market.market_id.clone(),
                        tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                    );
                    match result {
                        Ok(Some(evidence)) => {
                            let catalog_market = lifecycle_markets.get(&market.market_id)
                                .context("lifecycle catalog market missing")?;
                            let event = super::source_lifecycle::terminal_event(
                                catalog_market,
                                &evidence,
                                publication.next_market_event_cursor(&market.market_id)?,
                            )?;
                            publication.publish_native_events(vec![event], Some(evidence.clone()))?;
                            lifecycle_terminal.insert(market.market_id.clone());
                            let retired_tokens = if pipeline.has_market(&market.market_id) {
                                pipeline
                                    .remove_market(adapter, &market.market_id)?
                                    .into_iter()
                                    .collect::<BTreeSet<_>>()
                            } else {
                                BTreeSet::new()
                            };
                            if !retired_tokens.is_empty() {
                                acquisition_tokens.retain(|token| !retired_tokens.contains(token));
                                terminal_unsubscribes
                                    .insert(market.market_id.clone(), retired_tokens.clone());
                                recovery_retry_at.remove(&market.market_id);
                                recovery_failures.remove(&market.market_id);
                                recovering_markets.remove(&market.market_id);
                                freshness_queued.remove(&market.market_id);
                                freshness_pending.remove(&market.market_id);
                                freshness_retry_at.remove(&market.market_id);
                                confirmation_markets
                                    .retain(|item| item.market_id != market.market_id);
                                confirmation_membership.send_replace(
                                    confirmation_markets
                                        .iter()
                                        .map(|item| item.market_id.clone())
                                        .collect(),
                                );
                                // Local ownership is already fenced. A busy
                                // command slot is item-local and retried by the
                                // scheduler; it must not fail the whole source.
                                if let Err(error) = jobs.request_retire(&retired_tokens) {
                                    marketcow_polymarket::async_eprintln!("{}", json!({
                                        "stage":"lifecycle_wire_retirement_retry",
                                        "market_id":market.market_id,
                                        "detail":error.to_string()
                                    }));
                                }
                            }
                            marketcow_polymarket::async_eprintln!("{}",json!({"stage":"lifecycle_terminal_published",
                                "market_id":market.market_id,
                                "evidence_sha256":evidence["raw_response_sha256"],
                                "retired_token_count":retired_tokens.len(),
                                "wire_unsubscribe_pending":!retired_tokens.is_empty()}));
                        }
                        Ok(None) => marketcow_polymarket::async_eprintln!("{}",json!({"stage":"lifecycle_not_terminal",
                            "market_id":market.market_id})),
                        Err(error) => marketcow_polymarket::async_eprintln!("{}",json!({"stage":"lifecycle_refresh_error",
                            "market_id":market.market_id,"detail":format!("{error:#}")})),
                    }
                    continue;
                },
                completed=recovery_jobs.join_next(), if !recovery_jobs.is_empty()=>{
                    let (market_id, attempts, result) = completed.context("recovery task missing")??;
                    recovering_markets.remove(&market_id);
                    if !leases.enabled(){continue;}
                    // A lifecycle result can retire a market while a bounded
                    // recovery socket is still in flight. Its eventual result
                    // must not recreate retry state or submit a late snapshot.
                    if lifecycle_terminal.contains(&market_id) {
                        recovery_failures.remove(&market_id);
                        recovery_retry_at.remove(&market_id);
                        continue;
                    }
                    match result {
                        Ok(frames) => {
                            recovery_failures.remove(&market_id);
                            recovery_retry_at.remove(&market_id);
                            let mut by_token = frames.into_iter().filter_map(|frame| {
                                let token = frame.raw_payload["asset_id"].as_str()?.to_owned();
                                Some((token, frame))
                            }).collect::<BTreeMap<_, _>>();
                            for (token, attempt) in attempts {
                                if adapter.recoveries.get(&token) != Some(&attempt) { continue; }
                                match by_token.remove(&token) {
                                    Some(frame) => pipeline.submit(
                                        adapter, publication, frame, Some((token, attempt)),
                                    ).await?,
                                    None => marketcow_polymarket::async_eprintln!("{}", json!({"stage":"market_recovery_incomplete",
                                        "market_id":market_id,"token_id":token})),
                                }
                            }
                        }
                        Err(error) => {
                            let failures = recovery_failures.entry(market_id.clone()).or_default();
                            *failures = failures.saturating_add(1);
                            recovery_retry_at.insert(
                                market_id.clone(),
                                tokio::time::Instant::now() + recovery_retry_delay(*failures),
                            );
                            marketcow_polymarket::async_eprintln!("{}", json!({"stage":"market_recovery_retry",
                                "market_id":market_id,"token_count":attempts.len(),
                                "consecutive_failures":*failures,"detail":error.to_string()}));
                            // A market that disappears from the official book
                            // stream may have become terminal. Confirmation
                            // polling can be disabled, so a failed bounded WS
                            // recovery must independently trigger one bounded
                            // lifecycle lookup. Otherwise an expired member can
                            // remain in recovery forever and keep opening retry
                            // sockets. This lookup is event-driven, coalesced by
                            // market, and observes only the failed identity.
                            let due = lifecycle_retry_at
                                .get(&market_id)
                                .is_none_or(|at| *at <= tokio::time::Instant::now());
                            if !lifecycle_terminal.contains(&market_id)
                                && !lifecycle_pending.contains(&market_id)
                                && due
                            {
                                if let Some(record) = lifecycle_markets.get(&market_id) {
                                    let market = catalog_market(record)?;
                                    lifecycle_pending.insert(market_id.clone());
                                    let client = lifecycle_client.clone();
                                    let network = lifecycle_network.clone();
                                    lifecycle_jobs.spawn(async move {
                                        let _permit = network.acquire_owned().await;
                                        let result = super::source_lifecycle::refresh(
                                            client,
                                            market.clone(),
                                            response_byte_limit,
                                        )
                                        .await;
                                        (market, result)
                                    });
                                }
                            }
                        }
                    }
                    continue;
                },
                completed=manager_freshness_jobs.join_next(), if !manager_freshness_jobs.is_empty()=>{
                    let (market,requested,result)=completed.context("manager freshness task missing")??;
                    manager_freshness_pending.remove(&market.market_id);
                    if !leases.enabled()
                        || lifecycle_terminal.contains(&market.market_id)
                        || !confirmation_membership.borrow().contains(&market.market_id)
                    { continue; }
                    match result {
                        Err(error)=>marketcow_polymarket::async_eprintln!("{}",json!({"stage":"targeted_http_confirmation_failed",
                            "market_id":market.market_id,"detail":error.to_string()})),
                        Ok(frames)=>{
                            let received=frames.iter().map(|frame|frame.received_at)
                                .max().context("targeted HTTP confirmation empty")?;
                            let books:Vec<_>=frames.into_iter().map(|frame|frame.raw_payload).collect();
                            if market.token_ids.iter().any(|token|adapter.recoveries.contains_key(token)) {
                                marketcow_polymarket::async_eprintln!("{}",json!({"stage":"targeted_http_confirmation_superseded_by_recovery",
                                    "market_id":market.market_id}));
                                continue;
                            }
                            match publication.confirm_targeted_market(&market.market_id,&market.condition_id,
                                &market.token_ids,&books,requested,received) {
                                Ok(ConfirmationOutcome::Confirmed)=>marketcow_polymarket::async_eprintln!("{}",json!({
                                    "stage":"targeted_http_confirmation","market_id":market.market_id})),
                                Ok(ConfirmationOutcome::Superseded)=>marketcow_polymarket::async_eprintln!("{}",json!({
                                    "stage":"targeted_http_confirmation_superseded","market_id":market.market_id})),
                                Ok(ConfirmationOutcome::Mismatch)=>{
                                    // The main WS stream remains the causal
                                    // state owner. A racing REST snapshot is
                                    // useful only when it exactly confirms the
                                    // visible state; publishing it as a full
                                    // replacement every cadence created
                                    // artificial invalidation/recovery churn.
                                    marketcow_polymarket::async_eprintln!("{}",json!({"stage":"targeted_http_confirmation_mismatch",
                                        "market_id":market.market_id}));
                                },
                                Err(error)=>marketcow_polymarket::async_eprintln!("{}",json!({"stage":"targeted_http_confirmation_invalid",
                                    "market_id":market.market_id,"detail":error.to_string()})),
                            }
                        }
                    }
                    continue;
                },
                completed=freshness_jobs.join_next(), if !freshness_jobs.is_empty()=>{
                    let (market, requested, result) = completed.context("freshness task missing")??;
                    freshness_pending.remove(&market.market_id);
                    if !leases.enabled(){continue;}
                    // The terminal transition removes confirmation membership,
                    // but an already running task may complete afterward.
                    // Fence it before it can confirm or replace retired state.
                    if lifecycle_terminal.contains(&market.market_id)
                        || !confirmation_membership.borrow().contains(&market.market_id)
                    {
                        freshness_retry_at.remove(&market.market_id);
                        continue;
                    }
                    freshness_retry_at.insert(
                        market.market_id.clone(),
                        tokio::time::Instant::now()
                            + targeted_confirmation_retry_delay(
                                websocket_confirmation_seconds,
                            ),
                    );
                    start_freshness_jobs(
                        &mut freshness_queued,
                        &mut freshness_pending,
                        &mut freshness_jobs,
                        websocket_recovery_concurrency,
                    );
                    match result {
                        Err(error) => marketcow_polymarket::async_eprintln!("{}",json!({
                            "stage":"targeted_ws_confirmation_failed",
                            "market_id":market.market_id,"detail":error.to_string()
                        })),
                        Ok(frames) => {
                            let received = frames.iter().map(|frame| frame.received_at)
                                .max().context("targeted WS confirmation empty")?;
                            let books:Vec<_> = frames.into_iter()
                                .map(|frame| frame.raw_payload).collect();
                            if market.token_ids.iter().any(|token| adapter.recoveries.contains_key(token)) {
                                marketcow_polymarket::async_eprintln!("{}",json!({"stage":"targeted_ws_confirmation_superseded_by_recovery",
                                    "market_id":market.market_id}));
                                continue;
                            }
                            match publication.confirm_targeted_market(
                                &market.market_id, &market.condition_id, &market.token_ids,
                                &books, requested, received,
                            ) {
                                Ok(ConfirmationOutcome::Confirmed) => marketcow_polymarket::async_eprintln!("{}",json!({
                                    "stage":"targeted_ws_confirmation","market_id":market.market_id
                                })),
                                Ok(ConfirmationOutcome::Superseded) => marketcow_polymarket::async_eprintln!("{}",json!({
                                    "stage":"targeted_ws_confirmation_superseded","market_id":market.market_id
                                })),
                                Ok(ConfirmationOutcome::Mismatch) => {
                                    marketcow_polymarket::async_eprintln!("{}",json!({"stage":"targeted_ws_confirmation_replacement",
                                        "market_id":market.market_id}));
                                    if let Err(error) = pipeline.submit_rest_replacement(
                                        adapter, publication, books, received, &market.market_id,
                                        "targeted_ws_confirmation_mismatch",
                                    ).await {
                                        // A malformed/capacity-rejected recovery for one
                                        // market is item-local. Keep other markets and
                                        // shards live; this market remains stale and is
                                        // retried after its cooldown.
                                        marketcow_polymarket::async_eprintln!("{}",json!({
                                            "stage":"targeted_ws_confirmation_replacement_failed",
                                            "market_id":market.market_id,"detail":error.to_string()
                                        }));
                                    }
                                }
                                Err(error) => marketcow_polymarket::async_eprintln!("{}",json!({
                                    "stage":"targeted_ws_confirmation_invalid",
                                    "market_id":market.market_id,"detail":error.to_string()
                                })),
                            }
                        }
                    }
                    continue;
                },
                _=retry_tick.tick()=>{
                    pipeline.restart_closed_actors(adapter, publication).await?;
                    let incomplete = scope_retirements.unqueued_markets();
                    if !incomplete.is_empty() {
                        // A prior owner command may have started wire retirement
                        // but hit a temporarily full durability queue. Retain
                        // the same journal intent/fence and finish that effect;
                        // never cancel or allocate a new transaction identity.
                        let result:Result<()> = (|| {
                            for market in &incomplete {
                                if pipeline.has_market(market) { pipeline.remove_market(adapter,market)?; }
                                lifecycle_markets.remove(market);lifecycle_pending.remove(market);
                                lifecycle_terminal.remove(market);lifecycle_retry_at.remove(market);
                                freshness_queued.remove(market);freshness_pending.remove(market);
                                freshness_retry_at.remove(market);
                            }
                            confirmation_markets.retain(|m|!incomplete.contains(&m.market_id));
                            confirmation_membership.send_replace(confirmation_markets.iter()
                                .map(|m|m.market_id.clone()).collect());
                            let ticket = publication.reader().retirement_status()?.0
                                .checked_add(1).context("scope retirement ticket overflow")?;
                            publication.retire_catalog_markets(incomplete.clone())?;
                            scope_retirements.set_ticket(&incomplete,ticket);
                            acquisition_tokens = if leases.required(){jobs.tokens()}
                                else{adapter.identities.keys().cloned().collect()};
                            update_acquisition_metrics(publication,&jobs);
                            Ok(())
                        })();
                        if let Err(error) = result {
                            marketcow_polymarket::async_eprintln!("{}",json!({"stage":"scope_retirement_resume_blocked",
                                "market_ids":incomplete,"detail":format!("{error:#}")}));
                        }
                    }
                    if transport_metrics_at.elapsed() >= std::time::Duration::from_secs(5) {
                        let sample_ms = transport_metrics_at.elapsed().as_millis();
                        let occupancy = transport_metrics.take_occupancy();
                        marketcow_polymarket::async_eprintln!("{}", json!({
                            "stage":"transport_queue",
                            "current_bytes":occupancy.current_bytes,
                            "current_batches":occupancy.current_batches,
                            "high_bytes":occupancy.high_bytes,
                            "high_batches":occupancy.high_batches,
                            "maximum_bytes":TRANSPORT_QUEUE_BYTES,
                            "maximum_batches":TRANSPORT_QUEUE_BATCHES,
                        }));
                        marketcow_polymarket::async_eprintln!("{}", json!({
                            "stage":"market_hot_path_timing",
                            "sample_ms":sample_ms,
                            "pipeline":pipeline.take_hot_path_timing(),
                            "publication":publication.take_hot_path_timing(),
                        }));
                        transport_metrics_at = tokio::time::Instant::now();
                    }
                    if leases.required()&&leases.expire(std::time::Instant::now())>0&&!leases.enabled(){
                        confirmation_membership.send_replace(BTreeSet::new());
                        pause_acquisition(adapter,&pipeline,publication,&mut jobs)?;
                        marketcow_polymarket::async_eprintln!("{}",json!({"stage":"acquisition_lease_expired","acquisition_enabled":false}));
                    }
                    if let Err(error)=jobs.poll_retirements(){marketcow_polymarket::async_eprintln!("{}",json!({"stage":"acquisition_retirement_error","error":error.to_string()}));}
                    if leases.enabled()&&jobs.tokens().is_empty(){
                        let active=leases.market_ids();
                        jobs.add_pairs(&eligible_pairs(adapter,&active)?)?;
                        publication.resume_acquisition();
                        confirmation_membership.send_replace(active);
                        acquisition_tokens=jobs.tokens();
                        update_acquisition_metrics(publication, &jobs);
                    }
                    let pending_terminal_markets = terminal_unsubscribes
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>();
                    for market_id in pending_terminal_markets {
                        let tokens = terminal_unsubscribes[&market_id].clone();
                        match jobs.request_retire(&tokens) {
                            Ok(true) => {
                                terminal_unsubscribes.remove(&market_id);
                                marketcow_polymarket::async_eprintln!("{}",json!({
                                    "stage":"lifecycle_wire_retirement_complete",
                                    "market_id":market_id,
                                    "token_count":tokens.len()
                                }));
                            }
                            Ok(false) => {}
                            Err(error) => marketcow_polymarket::async_eprintln!("{}",json!({
                                "stage":"lifecycle_wire_retirement_retry",
                                "market_id":market_id,
                                "detail":error.to_string()
                            })),
                        }
                    }
                    update_acquisition_metrics(publication, &jobs);
                    if leases.enabled()&&!publication.is_source_ready()&&publication.fresh_acquisition_started(&jobs.tokens()){
                        publication.set_source_ready(true);
                        marketcow_polymarket::async_eprintln!("{}",json!({"stage":"acquisition_lease_ready","source_cursor":publication.cursor()?}));
                    }
                    if !leases.enabled(){continue;}
                    rest_submitted.retain(|token, attempt| adapter.recoveries.get(token) == Some(attempt));
                    // Coalesce all pending token gaps for one market into one
                    // temporary subscription. Repeated shard boundaries cannot
                    // create a socket per token or reset an in-flight identity.
                    let pending = pending_market_recoveries(adapter);
                    recovery_retry_at.retain(|market_id, _| pending.contains_key(market_id));
                    recovery_failures.retain(|market_id, _| pending.contains_key(market_id));
                    for (market_id, attempts) in pending {
                        if recovery_jobs.len() >= websocket_recovery_concurrency { break; }
                        if recovering_markets.contains(&market_id)
                            || recovery_retry_at.get(&market_id)
                                .is_some_and(|at| *at > tokio::time::Instant::now()) { continue; }
                        let tokens = attempts.keys().cloned().collect();
                        recovering_markets.insert(market_id.clone());
                        recovery_jobs.spawn(async move {
                            let result = recover_snapshots(tokens).await;
                            (market_id, attempts, result)
                        });
                    }
                    start_freshness_jobs(
                        &mut freshness_queued,
                        &mut freshness_pending,
                        &mut freshness_jobs,
                        websocket_recovery_concurrency,
                    );
                    continue;
                },
                _=&mut shutdown_requested=>break,
            };
            if !leases.enabled(){continue;}
            let mut batches = vec![frames];
            let mut declared_bytes = batches[0].declared_bytes();
            while batches.len() < OWNER_COALESCE_BATCHES {
                let batch = match jobs.try_next_batch()? {
                    Some(batch) => batch,
                    None => break,
                };
                if declared_bytes.saturating_add(batch.declared_bytes()) > OWNER_COALESCE_BYTES {
                    pending_transport = Some(batch);
                    break;
                }
                declared_bytes += batch.declared_bytes();
                batches.push(batch);
            }
            let mut routed = std::collections::VecDeque::new();
            for batch in batches {
                let source_tokens = batch.source_tokens();
                if source_tokens.is_empty() { continue; }
                if !source_tokens.is_subset(&acquisition_tokens) { continue; }
                let markets = source_tokens.iter().filter_map(|token|
                    adapter.identities.get(token).map(|(market, _)| market.clone()))
                    .collect::<BTreeSet<_>>();
                if source_tokens.len() != 2 || markets.len() != 1
                    || source_tokens.iter().any(|token| !adapter.identities.contains_key(token))
                {
                    // A malformed socket-owned batch cannot make another
                    // market's owner loop exit. Fence every reviewed market
                    // touched by this batch and require a fresh baseline.
                    handle_acquisition_socket_failure(adapter, publication, &pipeline,
                        source_tokens, "market_socket_membership_mismatch", leases.enabled()).await?;
                    continue;
                }
                let market = markets.into_iter().next().expect("one reviewed socket market");
                let dwell_us = u64::try_from(batch.queued_for().as_micros()).unwrap_or(u64::MAX);
                let dwell = transport_dwell.entry(market.clone()).or_default();
                dwell.0 += 1;
                dwell.1 = dwell.1.saturating_add(u128::from(dwell_us));
                dwell.2 = dwell.2.max(dwell_us);
                dwell.3 += u64::from(dwell_us >= 100_000);
                let frames = batch.into_frames().into_iter().filter(|frame|
                    marketcow_polymarket::subscription::owned_or_unclassified(
                        &frame.raw_payload, &acquisition_tokens))
                    .collect();
                for segment in split_transport_control_boundaries(frames) {
                    routed.push_back((market.clone(), segment));
                }
            }
            while !routed.is_empty() {
                // Keep owner-visible completions flowing between bounded actor
                // inputs; each market actor still reduces its own FIFO serially.
                for _ in 0..market_workers * 4 {
                    match completed_markets.try_recv() {
                        Ok(done) => pipeline.publish(adapter, publication, done).await?,
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => anyhow::bail!("market workers stopped"),
                    }
                }
                let pass = routed.drain(..routed.len().min(16)).collect();
                pipeline.submit_routed_batches(adapter, publication, pass).await?;
            }
            if transport_dwell_started.elapsed() >= std::time::Duration::from_secs(5) {
                for (market_id, (count, total_us, max_us, over_100ms)) in
                    std::mem::take(&mut transport_dwell) {
                    marketcow_polymarket::async_eprintln!("{}", json!({
                        "stage":"market_transport_queue_dwell_window",
                        "market_id":market_id,
                        "count":count,
                        "mean_us":if count == 0 { 0 } else { (total_us / u128::from(count)) as u64 },
                        "max_us":max_us,
                        "over_100ms":over_100ms,
                        "window_elapsed_ms":transport_dwell_started.elapsed().as_millis(),
                    }));
                }
                transport_dwell_started = std::time::Instant::now();
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    }.await;
    publication.set_source_ready(false);
    let _ = stop.send(true);
    // Let recovery workers observe their deadline/stop instead of detaching
    // nested transport jobs. A drop guard below aborts any nested socket task.
    lifecycle_jobs.abort_all();
    while lifecycle_jobs.join_next().await.is_some() {}
    freshness_jobs.abort_all();
    while freshness_jobs.join_next().await.is_some() {}
    manager_freshness_jobs.abort_all();
    while manager_freshness_jobs.join_next().await.is_some() {}
    jobs.stop().await;
    added_confirmation_jobs.abort_all();
    while added_confirmation_jobs.join_next().await.is_some() {}
    // A confirmation mismatch submits its already-validated REST pair to the
    // market actor immediately. Apply those bounded in-flight recoveries before
    // closing; merely draining and dropping their results would persist a gap
    // that did not exist upstream at shutdown.
    let recovery_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while !adapter.recoveries.is_empty() {
        tokio::select! {
            completed = completed_markets.recv() => {
                let Some(completed) = completed else { break; };
                pipeline.publish(adapter, publication, completed).await?;
            }
            completed = recovery_jobs.join_next(), if !recovery_jobs.is_empty() => {
                let Some(completed) = completed else { continue; };
                let (market_id, attempts, result) = completed?;
                recovering_markets.remove(&market_id);
                match result {
                    Ok(frames) => {
                        let mut by_token = frames.into_iter().filter_map(|frame| {
                            let token = frame.raw_payload["asset_id"].as_str()?.to_owned();
                            Some((token, frame))
                        }).collect::<BTreeMap<_, _>>();
                        for (token, attempt) in attempts {
                            if adapter.recoveries.get(&token) != Some(&attempt) { continue; }
                            if let Some(frame) = by_token.remove(&token) {
                                pipeline.submit(adapter, publication, frame, Some((token, attempt))).await?;
                            }
                        }
                    }
                    Err(error) => marketcow_polymarket::async_eprintln!("{}", json!({
                        "stage":"shutdown_market_recovery_failed","market_id":market_id,
                        "token_count":attempts.len(),"detail":error.to_string()
                    })),
                }
            }
            _ = tokio::time::sleep_until(recovery_deadline) => break,
        }
    }
    recovery_jobs.abort_all();
    while recovery_jobs.join_next().await.is_some() {}
    pipeline
        .finish(adapter, publication, &mut completed_markets)
        .await?;
    // Missing books are valid local quality facts, including at shutdown.
    // Do not start fresh network recovery or require all markets healthy to
    // stop. Every outstanding recovery must already be published; the owner
    // subsequently drains the independent durability worker before exit.
    if !adapter.recoveries.is_empty() {
        let (event_cursors, facts) = publication.shutdown_recovery_facts()?;
        validate_shutdown_recovery_facts(&adapter.recoveries, &facts)?;
        marketcow_polymarket::async_eprintln!(
            "{}",
            json!({"stage":"shutdown_recoveries_preserved",
            "token_count":adapter.recoveries.len(),"market_event_cursors":event_cursors})
        );
    }
    confirmation_job.abort();
    let _ = confirmation_job.await;
    result
}

fn validate_shutdown_recovery_facts(
    expected: &BTreeMap<String, String>,
    published: &BTreeMap<String, Value>,
) -> Result<()> {
    for (token, attempt) in expected {
        ensure!(
            published
                .get(token)
                .is_some_and(|fact| fact["recovery_id"] == *attempt),
            "shutdown recovery not represented in published state"
        );
    }
    Ok(())
}

#[cfg(test)]
mod shard_tests {
    use super::*;

    #[test]
    fn ten_thousand_owner_retirement_fences_release_after_both_receipts() {
        let now = std::time::Instant::now();
        let ttl = std::time::Duration::from_secs(120);
        let mut leases = super::super::source_acquisition_lease::Leases::new(true, 2, ttl).unwrap();
        let mut fences = ScopeRetirementFences::default();
        for generation in 1..=10_000_u64 {
            let market_id = format!("market-{generation}");
            let markets = BTreeSet::from([market_id.clone()]);
            let up = format!("{market_id}-up");
            let down = format!("{market_id}-down");
            let tokens = BTreeSet::from([up.clone(), down.clone()]);
            let identities = BTreeMap::from([
                (up, (market_id.clone(), "condition".to_owned())),
                (down, (market_id, "condition".to_owned())),
            ]);

            leases.acquire("shared".into(), ttl, markets.clone(), &markets, now).unwrap();
            assert!(fences.check_retire(&leases, &markets).is_err());
            leases.release("shared").unwrap();
            fences.check_retire(&leases, &markets).unwrap();
            fences.begin(&markets, &identities);
            fences.set_ticket(&markets, generation);
            assert_eq!(fences.0.len(), 1);
            assert!(fences.check_admission(&markets).is_err());
            fences.reconcile(generation, &tokens);
            assert_eq!(fences.0.len(), 1, "wire receipt has not completed");
            fences.reconcile(generation - 1, &BTreeSet::new());
            assert_eq!(fences.0.len(), 1, "durability receipt has not completed");
            fences.reconcile(generation, &BTreeSet::new());
            assert!(fences.0.is_empty(), "retirement fences must not accumulate");
            fences.check_admission(&markets).unwrap();
        }
    }

    #[test]
    fn retirement_rechecks_owner_leases_and_fences_reacquire_until_both_receipts() {
        let now = std::time::Instant::now();
        let ttl = std::time::Duration::from_secs(120);
        let market = BTreeSet::from(["m1".to_owned()]);
        let mut leases = super::super::source_acquisition_lease::Leases::new(true, 2, ttl).unwrap();
        let mut fences = ScopeRetirementFences::default();
        // A control-side status query saw no lease; another consumer acquires
        // before the serial owner handles the actual retirement command.
        let _stale_query = leases.market_ids();
        leases.acquire("other".into(), ttl, market.clone(), &market, now).unwrap();
        assert!(fences.check_retire(&leases, &market).is_err());
        leases.release("other").unwrap();
        // Likewise, validate_only cannot reserve a lease-free instant.
        fences.check_retire(&leases, &market).unwrap();
        leases.acquire("other".into(), ttl, market.clone(), &market, now).unwrap();
        assert!(fences.check_retire(&leases, &market).is_err());
        leases.release("other").unwrap();

        let identities = BTreeMap::from([
            ("up".into(), ("m1".into(), "condition".into())),
            ("down".into(), ("m1".into(), "condition".into())),
        ]);
        fences.begin(&market, &identities);
        assert_eq!(fences.unqueued_markets(), market,
            "an unknown durability effect keeps the generation fenced for replay");
        fences.set_ticket(&market, 2);
        assert!(fences.unqueued_markets().is_empty());
        assert!(fences.check_admission(&market).is_err());
        let wire = BTreeSet::from(["up".into(), "down".into()]);
        fences.reconcile(2, &wire); // writer complete, wire still draining
        assert!(fences.check_admission(&market).is_err());
        fences.reconcile(1, &BTreeSet::new()); // wire complete, old writer receipt
        assert!(fences.check_admission(&market).is_err());
        fences.reconcile(2, &BTreeSet::new());
        fences.check_admission(&market).unwrap();
        // A delayed receipt for the previous generation cannot release the
        // newly fenced generation of the same market identity.
        fences.begin(&market, &identities);
        fences.set_ticket(&market, 3);
        fences.reconcile(2, &BTreeSet::new());
        assert!(fences.check_admission(&market).is_err());
    }

    #[test]
    fn shutdown_preserves_missing_book_recovery_and_rejects_unpublished_attempt() {
        let expected = BTreeMap::from([("token".into(), "attempt".into())]);
        let published = BTreeMap::from([(
            "token".into(),
            json!({"recovery_id":"attempt","gap":{"resolved":false}}),
        )]);
        validate_shutdown_recovery_facts(&expected, &published).unwrap();
        assert_eq!(published["token"]["gap"]["resolved"], false);
        assert!(validate_shutdown_recovery_facts(&expected, &BTreeMap::new()).is_err());
        let stale = BTreeMap::from([("token".into(), json!({"recovery_id":"old"}))]);
        assert!(validate_shutdown_recovery_facts(&expected, &stale).is_err());
    }

    #[test]
    fn connection_shards_are_bounded_and_never_split_market_pairs() {
        let identities = BTreeMap::from([
            ("12".into(), ("1".into(), "c1".into())),
            ("11".into(), ("1".into(), "c1".into())),
            ("22".into(), ("2".into(), "c2".into())),
            ("21".into(), ("2".into(), "c2".into())),
            ("32".into(), ("3".into(), "c3".into())),
            ("31".into(), ("3".into(), "c3".into())),
        ]);
        let shards = connection_shards(&identities, 5).unwrap();
        assert_eq!(shards, vec![vec!["11", "12", "21", "22"], vec!["31", "32"]]);
        for market in ["1", "2", "3"] {
            let pair: BTreeSet<_> = identities
                .iter()
                .filter(|(_, value)| value.0 == market)
                .map(|(token, _)| token.as_str())
                .collect();
            assert!(shards.iter().any(|shard| {
                pair.iter()
                    .all(|token| shard.iter().any(|item| item == token))
            }));
        }
        assert!(connection_shards(&identities, 1).is_err());
        let incomplete = BTreeMap::from([("11".into(), ("1".into(), "c1".into()))]);
        assert!(connection_shards(&incomplete, 2).is_err());
    }
}
