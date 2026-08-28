//! Deterministic realtime domain core. It deliberately has no async runtime, HTTP or DB dependency.

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc,
        mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    },
};
use thiserror::Error;

pub const CONTRACT_VERSION: &str = "marketcow.polymarket.shadow.v1";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Price(#[serde(with = "rust_decimal::serde::str")] pub Decimal);

impl Price {
    pub fn parse(value: &str) -> Result<Self, CoreError> {
        let parsed =
            Decimal::from_str(value).map_err(|_| CoreError::InvalidDecimal(value.into()))?;
        if parsed < Decimal::ZERO || parsed > Decimal::ONE {
            return Err(CoreError::InvalidPrice(value.into()));
        }
        Ok(Self(parsed.normalize()))
    }

    pub fn parse_tick(value: &str) -> Result<Self, CoreError> {
        let tick = Self::parse(value)?;
        if tick.0.is_zero() {
            return Err(CoreError::InvalidTickSize);
        }
        Ok(tick)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
}

impl Level {
    pub fn new(price: &str, quantity: &str) -> Result<Self, CoreError> {
        let quantity =
            Decimal::from_str(quantity).map_err(|_| CoreError::InvalidDecimal(quantity.into()))?;
        if quantity < Decimal::ZERO {
            return Err(CoreError::InvalidQuantity);
        }
        Ok(Self {
            price: Price::parse(price)?,
            quantity: quantity.normalize(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Book {
    pub bids: BTreeMap<Price, Decimal>,
    pub asks: BTreeMap<Price, Decimal>,
    pub tick_size: Option<Price>,
    pub tick_version: String,
    pub source_observed_at: Option<DateTime<Utc>>,
    pub last_trade_price: Option<Price>,
    pub last_trade_observed_at: Option<DateTime<Utc>>,
}

impl Book {
    fn levels_tick_aligned(tick_size: &Price, levels: &[Level]) -> bool {
        !tick_size.0.is_zero()
            && levels
                .iter()
                .all(|level| (level.price.0 % tick_size.0).is_zero())
    }

    fn replace(
        &mut self,
        bids: &[Level],
        asks: &[Level],
        tick_size: &Price,
        tick_version: &str,
        at: DateTime<Utc>,
    ) {
        self.bids = bids
            .iter()
            .filter(|x| !x.quantity.is_zero())
            .map(|x| (x.price.clone(), x.quantity))
            .collect();
        self.asks = asks
            .iter()
            .filter(|x| !x.quantity.is_zero())
            .map(|x| (x.price.clone(), x.quantity))
            .collect();
        self.tick_size = Some(tick_size.clone());
        self.tick_version = tick_version.into();
        self.source_observed_at = Some(at);
    }

    fn apply(&mut self, side: Side, levels: &[Level], at: DateTime<Utc>) {
        let target = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        for level in levels {
            if level.quantity.is_zero() {
                target.remove(&level.price);
            } else {
                target.insert(level.price.clone(), level.quantity);
            }
        }
        self.source_observed_at = Some(at);
    }

    fn apply_batch(&mut self, changes: &[SideLevels], at: DateTime<Utc>) {
        for change in changes {
            self.apply(change.side, &change.levels, at);
        }
    }

    pub fn crossed_or_locked(&self) -> bool {
        match (self.bids.last_key_value(), self.asks.first_key_value()) {
            (Some((bid, _)), Some((ask, _))) => bid >= ask,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SideLevels {
    pub side: Side,
    pub levels: Vec<Level>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketLifecycleState {
    Active,
    Closed,
    Resolved,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeToken {
    pub token_id: String,
    pub outcome: String,
    pub instrument_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketRecord {
    pub market_id: String,
    pub condition_id: String,
    pub outcomes: [OutcomeToken; 2],
    pub negative_risk_group: Option<String>,
    pub lifecycle_state: MarketLifecycleState,
    pub resolution: Option<String>,
    pub metadata_revision: String,
    pub observed_at: DateTime<Utc>,
    pub terminal_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NegativeRiskRelation {
    pub group_id: String,
    pub member_market_ids: BTreeSet<String>,
    pub yes_token_ids: BTreeSet<String>,
    pub revision: String,
    pub complete: bool,
    pub valid_to: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum EventKind {
    CatalogSnapshot {
        catalog_revision: String,
        markets: Vec<MarketRecord>,
        negative_risk_relations: Vec<NegativeRiskRelation>,
    },
    NewMarket {
        condition_id: String,
    },
    MarketResolved {
        condition_id: String,
        winning_token_id: Option<String>,
        winning_outcome: Option<String>,
    },
    FullBook {
        token_id: String,
        bids: Vec<Level>,
        asks: Vec<Level>,
        tick_size: Price,
        tick_version: String,
    },
    Delta {
        token_id: String,
        side: Side,
        levels: Vec<Level>,
    },
    AtomicDelta {
        token_id: String,
        changes: Vec<SideLevels>,
    },
    BestBidAsk {
        token_id: String,
        best_bid: Option<Price>,
        best_ask: Option<Price>,
    },
    LastTradePrice {
        token_id: String,
        price: Price,
    },
    TickSizeChange {
        token_id: String,
        old_tick_size: Option<Price>,
        new_tick_size: Price,
        tick_version: String,
    },
    SourceGap {
        token_id: String,
        reason: String,
    },
}

impl EventKind {
    pub fn token_id(&self) -> &str {
        match self {
            Self::CatalogSnapshot {
                catalog_revision, ..
            } => catalog_revision,
            Self::NewMarket { condition_id } | Self::MarketResolved { condition_id, .. } => {
                condition_id
            }
            Self::FullBook { token_id, .. }
            | Self::Delta { token_id, .. }
            | Self::AtomicDelta { token_id, .. }
            | Self::BestBidAsk { token_id, .. }
            | Self::LastTradePrice { token_id, .. }
            | Self::TickSizeChange { token_id, .. }
            | Self::SourceGap { token_id, .. } => token_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceEvidence {
    pub source: String,
    pub source_url: Option<String>,
    pub requested_at: DateTime<Utc>,
    pub responded_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub raw_sha256: String,
    pub update_frequency: String,
    pub revision: String,
    pub missing: bool,
    pub delayed: bool,
    pub duplicate: bool,
    pub revised: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalEvent {
    pub schema_version: String,
    pub cursor: u64,
    pub event_id: String,
    pub scope_id: String,
    pub received_at: DateTime<Utc>,
    pub source_observed_at: DateTime<Utc>,
    pub normalizer_version: String,
    pub config_revision: String,
    pub source: SourceEvidence,
    pub raw_payload: Arc<serde_json::Value>,
    pub kind: EventKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedEvent {
    pub event: CanonicalEvent,
    pub applied: bool,
    pub fail_closed_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ApplyOutcome {
    pub projection: Arc<Projection>,
    pub persisted: PersistedEvent,
    pub persistence_latency_us: u64,
    pub publication_latency_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Projection {
    pub generation: u64,
    pub cursor: u64,
    pub persisted_cursor: u64,
    pub scope_id: String,
    pub books: BTreeMap<String, Book>,
    #[serde(default)]
    pub catalog_revision: Option<String>,
    #[serde(default)]
    pub markets: BTreeMap<String, MarketRecord>,
    #[serde(default)]
    pub condition_to_market: BTreeMap<String, String>,
    #[serde(default)]
    pub negative_risk_relations: BTreeMap<String, NegativeRiskRelation>,
    pub unresolved_gaps: BTreeSet<String>,
    pub recent_event_ids: VecDeque<String>,
    pub ready: bool,
    pub fail_closed_reason: Option<String>,
    pub published_at: DateTime<Utc>,
}

impl Projection {
    pub fn bootstrap(scope_id: impl Into<String>) -> Self {
        Self {
            generation: 0,
            cursor: 0,
            persisted_cursor: 0,
            scope_id: scope_id.into(),
            books: BTreeMap::new(),
            catalog_revision: None,
            markets: BTreeMap::new(),
            condition_to_market: BTreeMap::new(),
            negative_risk_relations: BTreeMap::new(),
            unresolved_gaps: BTreeSet::new(),
            recent_event_ids: VecDeque::new(),
            ready: false,
            fail_closed_reason: Some("bootstrap_required".into()),
            published_at: Utc::now(),
        }
    }

    pub fn hash(&self) -> String {
        // Publication wall time is observability metadata, not canonical state.
        let value = serde_json::json!({
            "generation": self.generation,
            "cursor": self.cursor,
            "persisted_cursor": self.persisted_cursor,
            "scope_id": self.scope_id,
            "books": self.books,
            "catalog_revision": self.catalog_revision,
            "markets": self.markets,
            "condition_to_market": self.condition_to_market,
            "negative_risk_relations": self.negative_risk_relations,
            "unresolved_gaps": self.unresolved_gaps,
            "recent_event_ids": self.recent_event_ids,
            "ready": self.ready,
            "fail_closed_reason": self.fail_closed_reason,
        });
        let bytes = serde_json::to_vec(&value).expect("projection is serializable");
        hex::encode(Sha256::digest(bytes))
    }
}

type CatalogState = (
    BTreeMap<String, MarketRecord>,
    BTreeMap<String, String>,
    BTreeMap<String, NegativeRiskRelation>,
    BTreeSet<String>,
);

fn validated_catalog(
    markets: &[MarketRecord],
    relations: &[NegativeRiskRelation],
) -> Option<CatalogState> {
    let mut by_market = BTreeMap::new();
    let mut by_condition = BTreeMap::new();
    let mut active_tokens = BTreeSet::new();
    let mut expected_groups: BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)> =
        BTreeMap::new();
    let mut all_tokens = BTreeSet::new();
    let mut all_instruments = BTreeSet::new();
    for market in markets {
        let terminal = market.lifecycle_state != MarketLifecycleState::Active;
        let outcome_tokens = market
            .outcomes
            .iter()
            .map(|outcome| outcome.token_id.as_str())
            .collect::<BTreeSet<_>>();
        if market.market_id.is_empty()
            || market.condition_id.is_empty()
            || market.metadata_revision.is_empty()
            || terminal != market.terminal_at.is_some()
            || market.lifecycle_state == MarketLifecycleState::Resolved
                && market.resolution.as_deref().is_none_or(str::is_empty)
            || outcome_tokens.len() != 2
            || market.outcomes.iter().any(|outcome| {
                outcome.token_id.is_empty()
                    || outcome.outcome.is_empty()
                    || outcome.instrument_id.is_empty()
                    || !all_tokens.insert(outcome.token_id.clone())
                    || !all_instruments.insert(outcome.instrument_id.clone())
            })
            || by_condition
                .insert(market.condition_id.clone(), market.market_id.clone())
                .is_some()
            || by_market
                .insert(market.market_id.clone(), market.clone())
                .is_some()
        {
            return None;
        }
        if market.lifecycle_state == MarketLifecycleState::Active {
            active_tokens.extend(
                market
                    .outcomes
                    .iter()
                    .map(|outcome| outcome.token_id.clone()),
            );
        }
        if let Some(group_id) = &market.negative_risk_group {
            if group_id.is_empty() || market.lifecycle_state != MarketLifecycleState::Active {
                continue;
            }
            let yes = market
                .outcomes
                .iter()
                .filter(|outcome| outcome.outcome.eq_ignore_ascii_case("yes"))
                .collect::<Vec<_>>();
            if yes.len() != 1 {
                return None;
            }
            let expected = expected_groups.entry(group_id.clone()).or_default();
            expected.0.insert(market.market_id.clone());
            expected.1.insert(yes[0].token_id.clone());
        }
    }
    let mut by_group = BTreeMap::new();
    for relation in relations {
        if relation.group_id.is_empty()
            || relation.revision.is_empty()
            || !relation.complete
            || relation.valid_to.is_some()
            || relation.member_market_ids.len() < 2
            || by_group
                .insert(relation.group_id.clone(), relation.clone())
                .is_some()
        {
            return None;
        }
    }
    if expected_groups.len() != by_group.len()
        || expected_groups.iter().any(|(group_id, expected)| {
            by_group.get(group_id).is_none_or(|relation| {
                relation.member_market_ids != expected.0 || relation.yes_token_ids != expected.1
            })
        })
    {
        return None;
    }
    Some((by_market, by_condition, by_group, active_tokens))
}

pub trait DurableLog {
    fn append(&mut self, event: &CanonicalEvent) -> Result<(), CoreError>;

    fn append_outcome(&mut self, outcome: &PersistedEvent) -> Result<(), CoreError> {
        self.append(&outcome.event)
    }

    fn append_outcomes(&mut self, outcomes: &[PersistedEvent]) -> Result<(), CoreError> {
        for outcome in outcomes {
            self.append_outcome(outcome)?;
        }
        Ok(())
    }
}

/// Per-client bounded queue. A full queue closes only that consumer; it never blocks apply.
pub struct BoundedClientQueue<T> {
    sender: SyncSender<T>,
    capacity: usize,
}

/// Bounded ingress queue for the authoritative single writer. A rejected value is returned to
/// the caller so an upstream reconnect/recovery path cannot accidentally drop it.
pub struct BoundedApplyQueue<T> {
    sender: SyncSender<T>,
    capacity: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ApplyQueueError<T> {
    Full(T),
    Disconnected(T),
}

impl<T> BoundedApplyQueue<T> {
    pub fn new(capacity: usize) -> Result<(Self, Receiver<T>), CoreError> {
        if capacity == 0 {
            return Err(CoreError::InvalidQueueCapacity);
        }
        let (sender, receiver) = sync_channel(capacity);
        Ok((Self { sender, capacity }, receiver))
    }

    pub fn enqueue(&self, value: T) -> Result<(), ApplyQueueError<T>> {
        self.sender.try_send(value).map_err(|error| match error {
            TrySendError::Full(value) => ApplyQueueError::Full(value),
            TrySendError::Disconnected(value) => ApplyQueueError::Disconnected(value),
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<T> BoundedClientQueue<T> {
    pub fn new(capacity: usize) -> Result<(Self, Receiver<T>), CoreError> {
        if capacity == 0 {
            return Err(CoreError::InvalidQueueCapacity);
        }
        let (sender, receiver) = sync_channel(capacity);
        Ok((Self { sender, capacity }, receiver))
    }

    pub fn publish(&self, value: T) -> Result<(), CoreError> {
        self.sender.try_send(value).map_err(|error| match error {
            TrySendError::Full(_) => CoreError::SlowConsumer { close_code: 1013 },
            TrySendError::Disconnected(_) => CoreError::ConsumerDisconnected,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

pub struct SingleWriter<L> {
    current: ArcSwap<Projection>,
    log: L,
}

impl<L: DurableLog> SingleWriter<L> {
    pub fn new(scope_id: String, log: L) -> Self {
        Self {
            current: ArcSwap::from_pointee(Projection::bootstrap(scope_id)),
            log,
        }
    }

    pub fn projection(&self) -> Arc<Projection> {
        self.current.load_full()
    }

    pub fn resume(projection: Projection, log: L) -> Result<Self, CoreError> {
        if projection.cursor != projection.persisted_cursor {
            return Err(CoreError::PersistedWatermarkMismatch);
        }
        Ok(Self {
            current: ArcSwap::from_pointee(projection),
            log,
        })
    }

    /// Validates every logical event in one upstream frame against an isolated candidate, writes
    /// all WAL records with one durability barrier, then publishes only the final generation.
    pub fn apply_batch(
        &mut self,
        events: Vec<CanonicalEvent>,
    ) -> Result<Vec<ApplyOutcome>, CoreError> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let frame_raw_payload = events[0].raw_payload.clone();
        let frame_raw_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(
            frame_raw_payload.as_ref(),
        )?));
        if events.iter().any(|event| {
            event.raw_payload.as_ref() != frame_raw_payload.as_ref()
                || !event
                    .source
                    .raw_sha256
                    .eq_ignore_ascii_case(&frame_raw_sha256)
        }) {
            return Err(CoreError::InvalidSourceEvidence);
        }
        struct CandidateLog;
        impl DurableLog for CandidateLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                Ok(())
            }
        }
        let starting = (*self.current.load_full()).clone();
        let mut candidate = SingleWriter::resume(starting, CandidateLog)?;
        let mut persisted = Vec::with_capacity(events.len());
        for event in events {
            persisted.push(candidate.apply_inner(event, false)?.persisted);
        }
        let persistence_started = std::time::Instant::now();
        self.log.append_outcomes(&persisted)?;
        let persistence_latency_us = persistence_started.elapsed().as_micros() as u64;
        let final_projection = candidate.projection();
        let publication_started = std::time::Instant::now();
        self.current.store(final_projection.clone());
        let publication_latency_us = publication_started.elapsed().as_micros() as u64;
        Ok(persisted
            .into_iter()
            .map(|persisted| ApplyOutcome {
                projection: final_projection.clone(),
                persisted,
                persistence_latency_us,
                publication_latency_us,
            })
            .collect())
    }

    /// Applies a candidate, validates it, durably appends, then atomically publishes it.
    pub fn apply(&mut self, event: CanonicalEvent) -> Result<ApplyOutcome, CoreError> {
        self.apply_inner(event, true)
    }

    fn apply_inner(
        &mut self,
        event: CanonicalEvent,
        validate_raw_payload: bool,
    ) -> Result<ApplyOutcome, CoreError> {
        let previous = self.current.load_full();
        if event.schema_version != CONTRACT_VERSION {
            return Err(CoreError::SchemaMismatch);
        }
        if event.scope_id != previous.scope_id {
            return Err(CoreError::ScopeMismatch);
        }
        if event.normalizer_version.is_empty()
            || event.config_revision.is_empty()
            || event.source.responded_at < event.source.requested_at
            || event.source.raw_sha256.len() != 64
            || !event
                .source
                .raw_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || validate_raw_payload
                && hex::encode(Sha256::digest(serde_json::to_vec(&event.raw_payload)?))
                    != event.source.raw_sha256.to_ascii_lowercase()
        {
            return Err(CoreError::InvalidSourceEvidence);
        }
        if previous.recent_event_ids.contains(&event.event_id) {
            return Err(CoreError::DuplicateEvent(event.event_id));
        }
        if event.cursor != previous.cursor + 1 {
            return Err(CoreError::CursorGap {
                expected: previous.cursor + 1,
                actual: event.cursor,
            });
        }
        let mut next = (*previous).clone();
        next.generation += 1;
        next.cursor = event.cursor;
        next.published_at = Utc::now();
        let mut applied = true;
        let mut rejected = None;
        if event.source.missing || event.source.delayed {
            next.unresolved_gaps.insert(event.kind.token_id().into());
            applied = false;
            rejected = Some(if event.source.missing {
                "source_data_missing".into()
            } else {
                "source_data_delayed".into()
            });
        } else {
            match &event.kind {
                EventKind::CatalogSnapshot {
                    catalog_revision,
                    markets,
                    negative_risk_relations,
                } => {
                    let mut combined = markets.clone();
                    combined.extend(
                        next.markets
                            .values()
                            .filter(|existing| {
                                existing.lifecycle_state == MarketLifecycleState::Resolved
                                    && !markets
                                        .iter()
                                        .any(|market| market.market_id == existing.market_id)
                            })
                            .cloned(),
                    );
                    if catalog_revision.is_empty() {
                        next.unresolved_gaps.insert("catalog:invalid".into());
                        applied = false;
                        rejected = Some("invalid_catalog_snapshot".into());
                    } else if let Some((catalog, conditions, relations, active_tokens)) =
                        validated_catalog(&combined, negative_risk_relations)
                    {
                        next.catalog_revision = Some(catalog_revision.clone());
                        next.markets = catalog;
                        next.condition_to_market = conditions;
                        next.negative_risk_relations = relations;
                        next.books
                            .retain(|token_id, _| active_tokens.contains(token_id));
                        next.unresolved_gaps.retain(|gap| {
                            !gap.starts_with("catalog:") && !gap.starts_with("negative_risk:")
                        });
                    } else {
                        next.unresolved_gaps.insert("catalog:invalid".into());
                        applied = false;
                        rejected = Some("invalid_catalog_snapshot".into());
                    }
                }
                EventKind::NewMarket { condition_id } => {
                    next.unresolved_gaps
                        .insert(format!("catalog:{condition_id}"));
                    rejected = Some("catalog_refresh_required".into());
                }
                EventKind::MarketResolved {
                    condition_id,
                    winning_token_id,
                    winning_outcome,
                } => {
                    if let Some(market_id) = next.condition_to_market.get(condition_id).cloned() {
                        let market = next
                            .markets
                            .get(&market_id)
                            .expect("condition index is valid");
                        let winning_token_valid = winning_token_id.as_ref().is_none_or(|winner| {
                            market
                                .outcomes
                                .iter()
                                .any(|outcome| &outcome.token_id == winner)
                        });
                        let winning_outcome_valid = winning_outcome.as_ref().is_none_or(|winner| {
                            market
                                .outcomes
                                .iter()
                                .any(|outcome| &outcome.outcome == winner)
                        });
                        if !winning_token_valid || !winning_outcome_valid {
                            next.unresolved_gaps
                                .insert(format!("catalog:{condition_id}"));
                            applied = false;
                            rejected = Some("market_resolution_source_mismatch".into());
                        } else {
                            let market = next
                                .markets
                                .get_mut(&market_id)
                                .expect("condition index is valid");
                            market.lifecycle_state = MarketLifecycleState::Resolved;
                            market.resolution = winning_outcome
                                .clone()
                                .or_else(|| winning_token_id.clone())
                                .or_else(|| Some("resolved".into()));
                            market.terminal_at = Some(event.source_observed_at);
                            market.metadata_revision = hex::encode(Sha256::digest(format!(
                                "{}\0{}",
                                market.metadata_revision, event.source.raw_sha256
                            )));
                            let outcomes = market.outcomes.clone();
                            let group = market.negative_risk_group.clone();
                            for outcome in outcomes {
                                next.books.remove(&outcome.token_id);
                                next.unresolved_gaps.remove(&outcome.token_id);
                            }
                            if let Some(group_id) = group {
                                if let Some(relation) =
                                    next.negative_risk_relations.get_mut(&group_id)
                                {
                                    relation.complete = false;
                                    relation.valid_to = Some(event.source_observed_at);
                                    relation.revision = hex::encode(Sha256::digest(format!(
                                        "{}\0{}",
                                        relation.revision, event.source.raw_sha256
                                    )));
                                }
                                next.unresolved_gaps
                                    .insert(format!("negative_risk:{group_id}"));
                            }
                            next.unresolved_gaps
                                .insert(format!("catalog:{condition_id}"));
                        }
                    } else {
                        next.unresolved_gaps
                            .insert(format!("catalog:{condition_id}"));
                        applied = false;
                        rejected = Some("catalog_refresh_required".into());
                    }
                }
                EventKind::SourceGap { token_id, reason } => {
                    next.unresolved_gaps.insert(token_id.clone());
                    applied = false;
                    rejected = Some(format!("source_gap:{reason}"));
                }
                EventKind::FullBook {
                    token_id,
                    bids,
                    asks,
                    tick_size,
                    tick_version,
                } => {
                    let token_is_active = next.catalog_revision.is_none()
                        || next.markets.values().any(|market| {
                            market.lifecycle_state == MarketLifecycleState::Active
                                && market
                                    .outcomes
                                    .iter()
                                    .any(|outcome| &outcome.token_id == token_id)
                        });
                    let mut book = next.books.get(token_id).cloned().unwrap_or_default();
                    if !token_is_active {
                        next.unresolved_gaps.insert(token_id.clone());
                        applied = false;
                        rejected = Some("token_not_in_active_catalog".into());
                    } else if tick_version.is_empty()
                        || !Book::levels_tick_aligned(tick_size, bids)
                        || !Book::levels_tick_aligned(tick_size, asks)
                    {
                        next.unresolved_gaps.insert(token_id.clone());
                        applied = false;
                        rejected = Some("invalid_tick_full_book".into());
                    } else {
                        book.replace(
                            bids,
                            asks,
                            tick_size,
                            tick_version,
                            event.source_observed_at,
                        );
                        if book.crossed_or_locked() {
                            next.unresolved_gaps.insert(token_id.clone());
                            applied = false;
                            rejected = Some("crossed_or_locked_full_book".into());
                        } else {
                            next.books.insert(token_id.clone(), book);
                            next.unresolved_gaps.remove(token_id);
                        }
                    }
                }
                EventKind::Delta {
                    token_id,
                    side,
                    levels,
                } => {
                    if next.unresolved_gaps.contains(token_id) || !next.books.contains_key(token_id)
                    {
                        next.unresolved_gaps.insert(token_id.clone());
                        applied = false;
                        rejected = Some("full_book_recovery_required".into());
                    } else {
                        let mut book = next.books[token_id].clone();
                        let aligned = book
                            .tick_size
                            .as_ref()
                            .is_some_and(|tick| Book::levels_tick_aligned(tick, levels));
                        if !aligned {
                            next.unresolved_gaps.insert(token_id.clone());
                            applied = false;
                            rejected = Some("invalid_tick_delta".into());
                        } else {
                            book.apply(*side, levels, event.source_observed_at);
                        }
                        if applied {
                            if book.crossed_or_locked() {
                                next.unresolved_gaps.insert(token_id.clone());
                                applied = false;
                                rejected = Some("crossed_or_locked_delta".into());
                            } else {
                                next.books.insert(token_id.clone(), book);
                            }
                        }
                    }
                }
                EventKind::AtomicDelta { token_id, changes } => {
                    if changes.is_empty()
                        || next.unresolved_gaps.contains(token_id)
                        || !next.books.contains_key(token_id)
                    {
                        next.unresolved_gaps.insert(token_id.clone());
                        applied = false;
                        rejected = Some("full_book_recovery_required".into());
                    } else {
                        let mut book = next.books[token_id].clone();
                        let aligned = book.tick_size.as_ref().is_some_and(|tick| {
                            changes
                                .iter()
                                .all(|change| Book::levels_tick_aligned(tick, &change.levels))
                        });
                        if !aligned {
                            next.unresolved_gaps.insert(token_id.clone());
                            applied = false;
                            rejected = Some("invalid_tick_atomic_delta".into());
                        } else {
                            book.apply_batch(changes, event.source_observed_at);
                        }
                        if applied {
                            if book.crossed_or_locked() {
                                next.unresolved_gaps.insert(token_id.clone());
                                applied = false;
                                rejected = Some("crossed_or_locked_atomic_delta".into());
                            } else {
                                next.books.insert(token_id.clone(), book);
                            }
                        }
                    }
                }
                EventKind::BestBidAsk {
                    token_id,
                    best_bid,
                    best_ask,
                } => {
                    if next.unresolved_gaps.contains(token_id) || !next.books.contains_key(token_id)
                    {
                        next.unresolved_gaps.insert(token_id.clone());
                        applied = false;
                        rejected = Some("full_book_recovery_required".into());
                    } else {
                        let book = &mut next.books.get_mut(token_id).expect("book checked above");
                        let projected_bid = book.bids.last_key_value().map(|(price, _)| price);
                        let projected_ask = book.asks.first_key_value().map(|(price, _)| price);
                        if projected_bid != best_bid.as_ref() || projected_ask != best_ask.as_ref()
                        {
                            next.unresolved_gaps.insert(token_id.clone());
                            applied = false;
                            rejected = Some("best_bid_ask_source_mismatch".into());
                        } else {
                            book.source_observed_at = Some(event.source_observed_at);
                        }
                    }
                }
                EventKind::LastTradePrice { token_id, price } => {
                    if next.unresolved_gaps.contains(token_id) || !next.books.contains_key(token_id)
                    {
                        next.unresolved_gaps.insert(token_id.clone());
                        applied = false;
                        rejected = Some("full_book_recovery_required".into());
                    } else {
                        let book = &mut next.books.get_mut(token_id).expect("book checked above");
                        let aligned = book
                            .tick_size
                            .as_ref()
                            .is_some_and(|tick| (price.0 % tick.0).is_zero());
                        if !aligned {
                            next.unresolved_gaps.insert(token_id.clone());
                            applied = false;
                            rejected = Some("invalid_tick_last_trade".into());
                        } else {
                            book.last_trade_price = Some(price.clone());
                            book.last_trade_observed_at = Some(event.source_observed_at);
                        }
                    }
                }
                EventKind::TickSizeChange {
                    token_id,
                    old_tick_size,
                    new_tick_size,
                    tick_version,
                } => {
                    if tick_version.is_empty()
                        || next.unresolved_gaps.contains(token_id)
                        || !next.books.contains_key(token_id)
                    {
                        next.unresolved_gaps.insert(token_id.clone());
                        applied = false;
                        rejected = Some("full_book_recovery_required".into());
                    } else {
                        let book = &mut next.books.get_mut(token_id).expect("book checked above");
                        let old_matches = old_tick_size
                            .as_ref()
                            .is_none_or(|expected| book.tick_size.as_ref() == Some(expected));
                        let levels_align = book
                            .bids
                            .keys()
                            .chain(book.asks.keys())
                            .all(|price| (price.0 % new_tick_size.0).is_zero());
                        if !old_matches {
                            next.unresolved_gaps.insert(token_id.clone());
                            applied = false;
                            rejected = Some("tick_size_source_mismatch".into());
                        } else if !levels_align {
                            next.unresolved_gaps.insert(token_id.clone());
                            applied = false;
                            rejected = Some("tick_size_recovery_required".into());
                        } else {
                            book.tick_size = Some(new_tick_size.clone());
                            book.tick_version = tick_version.clone();
                            book.source_observed_at = Some(event.source_observed_at);
                        }
                    }
                }
            }
        }
        let catalog_books_complete = next.catalog_revision.is_none()
            || next
                .markets
                .values()
                .filter(|market| market.lifecycle_state == MarketLifecycleState::Active)
                .flat_map(|market| market.outcomes.iter())
                .all(|outcome| next.books.contains_key(&outcome.token_id));
        next.ready =
            next.unresolved_gaps.is_empty() && !next.books.is_empty() && catalog_books_complete;
        next.fail_closed_reason = rejected
            .clone()
            .or_else(|| (!next.ready).then(|| "unresolved_gap".into()));
        let persisted = PersistedEvent {
            event: event.clone(),
            applied,
            fail_closed_reason: rejected,
        };
        let persistence_started = std::time::Instant::now();
        self.log.append_outcome(&persisted)?;
        let persistence_latency_us = persistence_started.elapsed().as_micros() as u64;
        next.persisted_cursor = event.cursor;
        next.recent_event_ids.push_back(event.event_id);
        if next.recent_event_ids.len() > 10_000 {
            next.recent_event_ids.pop_front();
        }
        let published = Arc::new(next);
        let publication_started = std::time::Instant::now();
        self.current.store(published.clone());
        let publication_latency_us = publication_started.elapsed().as_micros() as u64;
        Ok(ApplyOutcome {
            projection: published,
            persisted,
            persistence_latency_us,
            publication_latency_us,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WalRecord {
    cursor: u64,
    crc32c: u32,
    payload_sha256: String,
    payload: PersistedEvent,
}

#[derive(Debug, Serialize, Deserialize)]
struct WalBatchRecord {
    #[serde(default = "wal_batch_v1")]
    batch_version: u8,
    first_cursor: u64,
    last_cursor: u64,
    crc32c: u32,
    payload_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    frame_raw_payload: Option<serde_json::Value>,
    payloads: Vec<PersistedEvent>,
}

const fn wal_batch_v1() -> u8 {
    1
}

fn wal_batch_integrity_bytes(record: &WalBatchRecord) -> Result<Vec<u8>, CoreError> {
    match (record.batch_version, record.frame_raw_payload.as_ref()) {
        (1, None) => Ok(serde_json::to_vec(&record.payloads)?),
        (2, Some(raw_payload)) => Ok(serde_json::to_vec(&(raw_payload, &record.payloads))?),
        _ => Err(CoreError::CorruptWal),
    }
}

pub struct SegmentedWal {
    root: PathBuf,
    stream_id: String,
    max_segment_bytes: u64,
    segment_first_cursor: Option<u64>,
    current_path: Option<PathBuf>,
    file: Option<File>,
    bytes: u64,
}

impl SegmentedWal {
    pub fn open(
        root: impl AsRef<Path>,
        stream_id: &str,
        max_segment_bytes: u64,
    ) -> Result<Self, CoreError> {
        let root = root.as_ref();
        if !root.is_absolute() {
            return Err(CoreError::PathMustBeAbsolute);
        }
        if max_segment_bytes < 1024 {
            return Err(CoreError::SegmentTooSmall);
        }
        fs::create_dir_all(root)?;
        let mut paths = wal_paths(root)?;
        let (segment_first_cursor, current_path, file, bytes) = match paths.pop() {
            Some(path) => {
                // Refuse to append to an unverified chain or a different stream.
                Self::verify(root)?;
                let header = read_wal_header(&path)?;
                if header.get("stream_id").and_then(|value| value.as_str()) != Some(stream_id) {
                    return Err(CoreError::WalStreamMismatch);
                }
                let first_cursor = header
                    .get("first_cursor")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or(CoreError::CorruptWal)?;
                let bytes = fs::metadata(&path)?.len();
                let file = OpenOptions::new().append(true).open(&path)?;
                (Some(first_cursor), Some(path), Some(file), bytes)
            }
            None => (None, None, None, 0),
        };
        Ok(Self {
            root: root.into(),
            stream_id: stream_id.into(),
            max_segment_bytes,
            segment_first_cursor,
            current_path,
            file,
            bytes,
        })
    }

    fn rotate(&mut self, cursor: u64) -> Result<(), CoreError> {
        if let Some(file) = &mut self.file {
            file.sync_all()?;
        }
        let previous_segment_sha256 = match &self.current_path {
            Some(path) => Some(hex::encode(Sha256::digest(fs::read(path)?))),
            None => None,
        };
        let path = self
            .root
            .join(format!("{}-{cursor:020}.wal", self.stream_id));
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        let header = serde_json::json!({"magic":"MCWAL1","wal_version":1,
            "stream_id":self.stream_id,"first_cursor":cursor,
            "previous_segment_sha256":previous_segment_sha256});
        let line = serde_json::to_vec(&header)?;
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        self.bytes = line.len() as u64 + 1;
        self.segment_first_cursor = Some(cursor);
        self.current_path = Some(path);
        self.file = Some(file);
        Ok(())
    }

    fn write_outcome_without_final_sync(
        &mut self,
        outcome: &PersistedEvent,
    ) -> Result<(), CoreError> {
        let payload = serde_json::to_vec(outcome)?;
        let record = WalRecord {
            cursor: outcome.event.cursor,
            crc32c: crc32c::crc32c(&payload),
            payload_sha256: hex::encode(Sha256::digest(&payload)),
            payload: outcome.clone(),
        };
        let line = serde_json::to_vec(&record)?;
        if self.file.is_none() || self.bytes + line.len() as u64 + 1 > self.max_segment_bytes {
            self.rotate(outcome.event.cursor)?;
        }
        let file = self.file.as_mut().expect("rotated");
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        self.bytes += line.len() as u64 + 1;
        Ok(())
    }

    pub fn verify(root: impl AsRef<Path>) -> Result<Vec<PersistedEvent>, CoreError> {
        let paths = wal_paths(root.as_ref())?;
        let mut output = Vec::new();
        let mut previous_segment_sha256: Option<String> = None;
        let mut stream_id: Option<String> = None;
        for path in paths {
            let mut lines = BufReader::new(File::open(&path)?).lines();
            let header: serde_json::Value =
                serde_json::from_str(&lines.next().ok_or(CoreError::CorruptWal)??)?;
            if header.get("magic").and_then(|x| x.as_str()) != Some("MCWAL1") {
                return Err(CoreError::CorruptWal);
            }
            let actual_stream_id = header
                .get("stream_id")
                .and_then(serde_json::Value::as_str)
                .ok_or(CoreError::CorruptWal)?;
            if stream_id.get_or_insert_with(|| actual_stream_id.to_owned()) != actual_stream_id {
                return Err(CoreError::WalStreamMismatch);
            }
            let actual_previous = header
                .get("previous_segment_sha256")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            if actual_previous != previous_segment_sha256 {
                return Err(CoreError::CorruptWal);
            }
            for line in lines {
                let value: serde_json::Value = serde_json::from_str(&line?)?;
                if value.get("payloads").is_some() {
                    let mut record: WalBatchRecord = serde_json::from_value(value)?;
                    let payload = wal_batch_integrity_bytes(&record)?;
                    if record.payloads.is_empty()
                        || record.payloads.first().map(|item| item.event.cursor)
                            != Some(record.first_cursor)
                        || record.payloads.last().map(|item| item.event.cursor)
                            != Some(record.last_cursor)
                        || crc32c::crc32c(&payload) != record.crc32c
                        || hex::encode(Sha256::digest(&payload)) != record.payload_sha256
                    {
                        return Err(CoreError::CorruptWal);
                    }
                    if let Some(raw_payload) = record.frame_raw_payload.take() {
                        let raw_payload = Arc::new(raw_payload);
                        let raw_sha256 =
                            hex::encode(Sha256::digest(serde_json::to_vec(raw_payload.as_ref())?));
                        if record
                            .payloads
                            .iter()
                            .any(|persisted| persisted.event.source.raw_sha256 != raw_sha256)
                        {
                            return Err(CoreError::CorruptWal);
                        }
                        for persisted in &mut record.payloads {
                            persisted.event.raw_payload = raw_payload.clone();
                        }
                    }
                    for persisted in record.payloads {
                        if output
                            .last()
                            .map(|item: &PersistedEvent| item.event.cursor + 1)
                            != Some(persisted.event.cursor)
                            && !output.is_empty()
                        {
                            return Err(CoreError::CorruptWal);
                        }
                        output.push(persisted);
                    }
                } else {
                    let record: WalRecord = serde_json::from_value(value)?;
                    let payload = serde_json::to_vec(&record.payload)?;
                    if crc32c::crc32c(&payload) != record.crc32c
                        || hex::encode(Sha256::digest(&payload)) != record.payload_sha256
                    {
                        return Err(CoreError::CorruptWal);
                    }
                    if output
                        .last()
                        .map(|item: &PersistedEvent| item.event.cursor + 1)
                        != Some(record.cursor)
                        && !output.is_empty()
                    {
                        return Err(CoreError::CorruptWal);
                    }
                    output.push(record.payload);
                }
            }
            previous_segment_sha256 = Some(hex::encode(Sha256::digest(fs::read(path)?)));
        }
        Ok(output)
    }
}

fn wal_paths(root: &Path) -> Result<Vec<PathBuf>, CoreError> {
    let mut paths: Vec<_> = fs::read_dir(root)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("wal"))
        .collect();
    paths.sort();
    Ok(paths)
}

fn read_wal_header(path: &Path) -> Result<serde_json::Value, CoreError> {
    let line = BufReader::new(File::open(path)?)
        .lines()
        .next()
        .ok_or(CoreError::CorruptWal)??;
    let header = serde_json::from_str(&line)?;
    Ok(header)
}

impl DurableLog for SegmentedWal {
    fn append(&mut self, event: &CanonicalEvent) -> Result<(), CoreError> {
        self.append_outcome(&PersistedEvent {
            event: event.clone(),
            applied: true,
            fail_closed_reason: None,
        })
    }

    fn append_outcome(&mut self, outcome: &PersistedEvent) -> Result<(), CoreError> {
        self.write_outcome_without_final_sync(outcome)?;
        let file = self.file.as_mut().expect("rotated");
        file.sync_data()?;
        Ok(())
    }
    fn append_outcomes(&mut self, outcomes: &[PersistedEvent]) -> Result<(), CoreError> {
        let Some(first) = outcomes.first() else {
            return Ok(());
        };
        if outcomes
            .windows(2)
            .any(|pair| pair[0].event.cursor + 1 != pair[1].event.cursor)
        {
            return Err(CoreError::CorruptWal);
        }
        let frame_raw_payload = outcomes[0].event.raw_payload.clone();
        let raw_sha256 = &outcomes[0].event.source.raw_sha256;
        if outcomes.iter().any(|outcome| {
            outcome.event.source.raw_sha256 != *raw_sha256
                || outcome.event.raw_payload.as_ref() != frame_raw_payload.as_ref()
        }) {
            return Err(CoreError::CorruptWal);
        }
        let mut payloads = outcomes.to_vec();
        for persisted in &mut payloads {
            persisted.event.raw_payload = Arc::new(serde_json::Value::Null);
        }
        let mut record = WalBatchRecord {
            batch_version: 2,
            first_cursor: first.event.cursor,
            last_cursor: outcomes.last().expect("non-empty").event.cursor,
            crc32c: 0,
            payload_sha256: String::new(),
            frame_raw_payload: Some((*frame_raw_payload).clone()),
            payloads,
        };
        let payload = wal_batch_integrity_bytes(&record)?;
        record.crc32c = crc32c::crc32c(&payload);
        record.payload_sha256 = hex::encode(Sha256::digest(&payload));
        let line = serde_json::to_vec(&record)?;
        if self.file.is_none() || self.bytes + line.len() as u64 + 1 > self.max_segment_bytes {
            self.rotate(first.event.cursor)?;
        }
        let file = self.file.as_mut().expect("rotated");
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        self.bytes += line.len() as u64 + 1;
        Ok(())
    }
}

pub fn write_checkpoint(path: &Path, projection: &Projection) -> Result<String, CoreError> {
    if !path.is_absolute() {
        return Err(CoreError::PathMustBeAbsolute);
    }
    let bytes = serde_json::to_vec(projection)?;
    let hash = hex::encode(Sha256::digest(&bytes));
    let temp = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    File::open(path.parent().ok_or(CoreError::PathMustBeAbsolute)?)?.sync_all()?;
    Ok(hash)
}

pub fn read_checkpoint(path: &Path, expected_hash: &str) -> Result<Projection, CoreError> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    if hex::encode(Sha256::digest(&bytes)) != expected_hash {
        return Err(CoreError::CheckpointHash);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

/// Rebuilds canonical state strictly after a durable checkpoint and verifies that current code
/// derives the same apply/reject decision recorded in the append-only log.
pub fn replay_after_checkpoint(
    checkpoint: Projection,
    records: &[PersistedEvent],
) -> Result<Projection, CoreError> {
    struct ReplayLog;
    impl DurableLog for ReplayLog {
        fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
            Ok(())
        }
    }

    let checkpoint_cursor = checkpoint.cursor;
    let mut writer = SingleWriter::resume(checkpoint, ReplayLog)?;
    for persisted in records
        .iter()
        .filter(|record| record.event.cursor > checkpoint_cursor)
    {
        let replayed = writer.apply(persisted.event.clone())?.persisted;
        if replayed.applied != persisted.applied
            || replayed.fail_closed_reason != persisted.fail_closed_reason
        {
            return Err(CoreError::ReplayDivergence {
                cursor: persisted.event.cursor,
            });
        }
    }
    Ok((*writer.projection()).clone())
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("invalid decimal: {0}")]
    InvalidDecimal(String),
    #[error("price outside [0,1]: {0}")]
    InvalidPrice(String),
    #[error("quantity cannot be negative")]
    InvalidQuantity,
    #[error("tick size must be positive")]
    InvalidTickSize,
    #[error("schema mismatch")]
    SchemaMismatch,
    #[error("scope mismatch")]
    ScopeMismatch,
    #[error("source evidence is incomplete or invalid")]
    InvalidSourceEvidence,
    #[error("duplicate event: {0}")]
    DuplicateEvent(String),
    #[error("cursor gap expected {expected} got {actual}")]
    CursorGap { expected: u64, actual: u64 },
    #[error("crossed or locked book: {0}")]
    CrossedBook(String),
    #[error("full-book recovery required: {0}")]
    RecoveryRequired(String),
    #[error("path must be absolute")]
    PathMustBeAbsolute,
    #[error("WAL segment too small")]
    SegmentTooSmall,
    #[error("corrupt WAL")]
    CorruptWal,
    #[error("WAL contains a different stream")]
    WalStreamMismatch,
    #[error("checkpoint hash mismatch")]
    CheckpointHash,
    #[error("published and persisted watermark mismatch")]
    PersistedWatermarkMismatch,
    #[error("replay decision diverged at cursor {cursor}")]
    ReplayDivergence { cursor: u64 },
    #[error("queue capacity must be positive")]
    InvalidQueueCapacity,
    #[error("slow consumer must close with {close_code}")]
    SlowConsumer { close_code: u16 },
    #[error("consumer disconnected")]
    ConsumerDisconnected,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn event(cursor: u64, kind: EventKind) -> CanonicalEvent {
        let at = Utc::now();
        let raw_payload = serde_json::json!({"fixture_cursor": cursor});
        CanonicalEvent {
            schema_version: CONTRACT_VERSION.into(),
            cursor,
            event_id: format!("e-{cursor}"),
            scope_id: "scope".into(),
            received_at: at,
            source_observed_at: at,
            normalizer_version: "test-v1".into(),
            config_revision: "config-v1".into(),
            source: SourceEvidence {
                source: "polymarket-clob".into(),
                source_url: Some("https://clob.polymarket.com".into()),
                requested_at: at,
                responded_at: at,
                observed_at: at,
                raw_sha256: hex::encode(Sha256::digest(serde_json::to_vec(&raw_payload).unwrap())),
                update_frequency: "realtime".into(),
                revision: "fixture-v1".into(),
                missing: false,
                delayed: false,
                duplicate: false,
                revised: false,
            },
            raw_payload: Arc::new(raw_payload),
            kind,
        }
    }
    fn levels(price: &str, quantity: &str) -> Vec<Level> {
        vec![Level::new(price, quantity).unwrap()]
    }

    #[test]
    fn decimal_is_exact_and_rejects_float_semantics() {
        assert_eq!(
            Price::parse("0.100000000000000001").unwrap().0.to_string(),
            "0.100000000000000001"
        );
        assert!(Price::parse("1.0001").is_err());
        assert!(Level::new("0.1", "-1").is_err());
    }

    #[test]
    fn rejected_delta_is_persisted_and_full_book_recovers_next_cursor() {
        let dir = tempdir().unwrap();
        let wal = SegmentedWal::open(dir.path(), "test", 4096).unwrap();
        let mut state = SingleWriter::new("scope".into(), wal);
        state
            .apply(event(
                1,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.4", "1"),
                    asks: levels("0.6", "1"),
                    tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "tick-v1".into(),
                },
            ))
            .unwrap();
        let rejected = state
            .apply(event(
                2,
                EventKind::Delta {
                    token_id: "t".into(),
                    side: Side::Bid,
                    levels: levels("0.7", "1"),
                },
            ))
            .unwrap();
        assert!(!rejected.persisted.applied);
        assert_eq!(
            rejected.persisted.fail_closed_reason.as_deref(),
            Some("crossed_or_locked_delta")
        );
        assert_eq!(state.projection().cursor, 2);
        state
            .apply(event(
                3,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.3", "2"),
                    asks: levels("0.7", "2"),
                    tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "tick-v2".into(),
                },
            ))
            .unwrap();
        assert!(state.projection().ready);
        drop(state);
        let persisted = SegmentedWal::verify(dir.path()).unwrap();
        assert_eq!(persisted.len(), 3);
        assert!(!persisted[1].applied);
        assert_eq!(
            persisted[1].fail_closed_reason.as_deref(),
            Some("crossed_or_locked_delta")
        );
        assert!(persisted[2].applied);
    }

    #[test]
    fn wal_checkpoint_and_replay_are_deterministic() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        fs::create_dir(&wal_dir).unwrap();
        let wal = SegmentedWal::open(&wal_dir, "test", 1024).unwrap();
        let mut state = SingleWriter::new("scope".into(), wal);
        state
            .apply(event(
                1,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.1", "3"),
                    asks: levels("0.9", "3"),
                    tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "tick-v1".into(),
                },
            ))
            .unwrap();
        let checkpoint = dir.path().join("checkpoint.json");
        let hash = write_checkpoint(&checkpoint, &state.projection()).unwrap();
        assert_eq!(
            read_checkpoint(&checkpoint, &hash).unwrap().hash(),
            state.projection().hash()
        );
        let replay = SegmentedWal::verify(&wal_dir).unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].event.cursor, 1);
    }

    #[test]
    fn online_and_replay_projection_hashes_match() {
        struct MemoryLog(Vec<CanonicalEvent>);
        impl DurableLog for MemoryLog {
            fn append(&mut self, event: &CanonicalEvent) -> Result<(), CoreError> {
                self.0.push(event.clone());
                Ok(())
            }
        }
        let events = vec![
            event(
                1,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.2", "1.25"),
                    asks: levels("0.8", "3.5"),
                    tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "tick-v1".into(),
                },
            ),
            event(
                2,
                EventKind::Delta {
                    token_id: "t".into(),
                    side: Side::Bid,
                    levels: levels("0.3", "2.75"),
                },
            ),
            event(
                3,
                EventKind::SourceGap {
                    token_id: "t".into(),
                    reason: "disconnect".into(),
                },
            ),
            event(
                4,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.25", "5"),
                    asks: levels("0.75", "5"),
                    tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "tick-v2".into(),
                },
            ),
        ];
        let mut online = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        for value in events.clone() {
            online.apply(value).unwrap();
        }
        let mut replay = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        for value in events {
            replay.apply(value).unwrap();
        }
        assert_eq!(online.projection().hash(), replay.projection().hash());
    }

    #[test]
    fn persistence_failure_never_publishes_candidate() {
        struct FailedLog;
        impl DurableLog for FailedLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                Err(CoreError::CorruptWal)
            }
        }
        let mut state = SingleWriter::new("scope".into(), FailedLog);
        assert!(
            state
                .apply(event(
                    1,
                    EventKind::FullBook {
                        token_id: "t".into(),
                        bids: levels("0.2", "1"),
                        asks: levels("0.8", "1"),
                        tick_size: Price::parse_tick("0.01").unwrap(),
                        tick_version: "tick-v1".into()
                    }
                ))
                .is_err()
        );
        assert_eq!(state.projection().cursor, 0);
        assert_eq!(state.projection().persisted_cursor, 0);
    }

    #[test]
    fn upstream_frame_group_commit_publishes_only_after_one_batch_barrier() {
        #[derive(Default)]
        struct BatchLog {
            batches: usize,
            records: Vec<PersistedEvent>,
        }
        impl DurableLog for BatchLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                unreachable!("batch append is required")
            }
            fn append_outcomes(&mut self, outcomes: &[PersistedEvent]) -> Result<(), CoreError> {
                self.batches += 1;
                self.records.extend_from_slice(outcomes);
                Ok(())
            }
        }
        let mut state = SingleWriter::new("scope".into(), BatchLog::default());
        let first = event(
            1,
            EventKind::FullBook {
                token_id: "t".into(),
                bids: levels("0.4", "1"),
                asks: levels("0.6", "1"),
                tick_size: Price::parse_tick("0.01").unwrap(),
                tick_version: "tick-v1".into(),
            },
        );
        let mut second = event(
            2,
            EventKind::Delta {
                token_id: "t".into(),
                side: Side::Bid,
                levels: levels("0.4", "2"),
            },
        );
        second.raw_payload = first.raw_payload.clone();
        second.source.raw_sha256 = first.source.raw_sha256.clone();
        let outcomes = state.apply_batch(vec![first, second]).unwrap();
        assert_eq!(outcomes.len(), 2);
        assert_eq!(state.log.batches, 1);
        assert_eq!(state.log.records.len(), 2);
        assert_eq!(state.projection().cursor, 2);
        assert_eq!(state.projection().persisted_cursor, 2);
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.projection.cursor == 2)
        );
    }

    #[test]
    fn failed_group_commit_publishes_none_of_the_frame() {
        struct FailingBatchLog;
        impl DurableLog for FailingBatchLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                unreachable!("batch append is required")
            }
            fn append_outcomes(&mut self, _: &[PersistedEvent]) -> Result<(), CoreError> {
                Err(CoreError::Io(std::io::Error::other("batch fsync failed")))
            }
        }
        let mut state = SingleWriter::new("scope".into(), FailingBatchLog);
        assert!(
            state
                .apply_batch(vec![event(
                    1,
                    EventKind::SourceGap {
                        token_id: "t".into(),
                        reason: "disconnect".into(),
                    },
                )])
                .is_err()
        );
        assert_eq!(state.projection().cursor, 0);
        assert_eq!(state.projection().persisted_cursor, 0);
    }

    #[test]
    fn grouped_frame_rejects_mixed_raw_evidence_before_persistence() {
        #[derive(Default)]
        struct BatchLog {
            writes: usize,
        }
        impl DurableLog for BatchLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                unreachable!("batch append is required")
            }
            fn append_outcomes(&mut self, _: &[PersistedEvent]) -> Result<(), CoreError> {
                self.writes += 1;
                Ok(())
            }
        }
        let mut state = SingleWriter::new("scope".into(), BatchLog::default());
        let result = state.apply_batch(vec![
            event(
                1,
                EventKind::SourceGap {
                    token_id: "a".into(),
                    reason: "fixture".into(),
                },
            ),
            event(
                2,
                EventKind::SourceGap {
                    token_id: "b".into(),
                    reason: "fixture".into(),
                },
            ),
        ]);
        assert!(matches!(result, Err(CoreError::InvalidSourceEvidence)));
        assert_eq!(state.log.writes, 0);
        assert_eq!(state.projection().cursor, 0);
    }

    #[test]
    fn segmented_wal_encodes_a_logical_frame_as_one_verified_batch_record() {
        let dir = tempdir().unwrap();
        let wal = SegmentedWal::open(dir.path(), "batch", 16 * 1024).unwrap();
        let mut state = SingleWriter::new("scope".into(), wal);
        let first = event(
            1,
            EventKind::SourceGap {
                token_id: "a".into(),
                reason: "fixture".into(),
            },
        );
        let mut second = event(
            2,
            EventKind::SourceGap {
                token_id: "b".into(),
                reason: "fixture".into(),
            },
        );
        second.raw_payload = first.raw_payload.clone();
        second.source.raw_sha256 = first.source.raw_sha256.clone();
        state.apply_batch(vec![first, second]).unwrap();
        drop(state);
        let path = wal_paths(dir.path()).unwrap().remove(0);
        assert_eq!(
            BufReader::new(File::open(&path).unwrap()).lines().count(),
            2
        );
        let record: serde_json::Value = serde_json::from_str(
            &BufReader::new(File::open(&path).unwrap())
                .lines()
                .nth(1)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(record["batch_version"], 2);
        assert!(record["frame_raw_payload"].is_object());
        assert!(
            record["payloads"]
                .as_array()
                .unwrap()
                .iter()
                .all(|payload| payload["event"]["raw_payload"].is_null())
        );
        let recovered = SegmentedWal::verify(dir.path()).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].event.cursor, 1);
        assert_eq!(recovered[1].event.cursor, 2);
        assert!(Arc::ptr_eq(
            &recovered[0].event.raw_payload,
            &recovered[1].event.raw_payload
        ));
    }

    #[test]
    fn torn_batch_record_fails_closed_without_partial_replay() {
        let dir = tempdir().unwrap();
        let wal = SegmentedWal::open(dir.path(), "batch", 16 * 1024).unwrap();
        let mut state = SingleWriter::new("scope".into(), wal);
        let first = event(
            1,
            EventKind::SourceGap {
                token_id: "a".into(),
                reason: "fixture".into(),
            },
        );
        let mut second = event(
            2,
            EventKind::SourceGap {
                token_id: "b".into(),
                reason: "fixture".into(),
            },
        );
        second.raw_payload = first.raw_payload.clone();
        second.source.raw_sha256 = first.source.raw_sha256.clone();
        state.apply_batch(vec![first, second]).unwrap();
        drop(state);
        let path = wal_paths(dir.path()).unwrap().remove(0);
        let bytes = fs::read(&path).unwrap();
        fs::write(&path, &bytes[..bytes.len() - 12]).unwrap();
        assert!(SegmentedWal::verify(dir.path()).is_err());
    }

    #[test]
    fn wal_batch_v1_remains_replay_compatible() {
        let dir = tempdir().unwrap();
        let mut wal = SegmentedWal::open(dir.path(), "batch-v1", 16 * 1024).unwrap();
        wal.rotate(1).unwrap();
        let payloads = vec![PersistedEvent {
            event: event(
                1,
                EventKind::SourceGap {
                    token_id: "legacy".into(),
                    reason: "fixture".into(),
                },
            ),
            applied: false,
            fail_closed_reason: Some("fixture".into()),
        }];
        let mut record = WalBatchRecord {
            batch_version: 1,
            first_cursor: 1,
            last_cursor: 1,
            crc32c: 0,
            payload_sha256: String::new(),
            frame_raw_payload: None,
            payloads,
        };
        let payload = wal_batch_integrity_bytes(&record).unwrap();
        record.crc32c = crc32c::crc32c(&payload);
        record.payload_sha256 = hex::encode(Sha256::digest(&payload));
        let line = serde_json::to_vec(&record).unwrap();
        wal.file.as_mut().unwrap().write_all(&line).unwrap();
        wal.file.as_mut().unwrap().write_all(b"\n").unwrap();
        wal.file.as_mut().unwrap().sync_data().unwrap();
        drop(wal);
        let recovered = SegmentedWal::verify(dir.path()).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].event.raw_payload["fixture_cursor"], 1);
    }

    #[test]
    fn slow_consumer_is_isolated_by_bounded_queue() {
        let (queue, receiver) = BoundedClientQueue::new(1).unwrap();
        assert_eq!(queue.capacity(), 1);
        queue.publish(1).unwrap();
        assert!(matches!(
            queue.publish(2),
            Err(CoreError::SlowConsumer { close_code: 1013 })
        ));
        assert_eq!(receiver.recv().unwrap(), 1);
    }

    #[test]
    fn ingress_backpressure_returns_the_unqueued_event_without_loss() {
        let (queue, receiver) = BoundedApplyQueue::new(1).unwrap();
        assert_eq!(queue.capacity(), 1);
        queue.enqueue("first").unwrap();
        assert_eq!(
            queue.enqueue("must-recover"),
            Err(ApplyQueueError::Full("must-recover"))
        );
        assert_eq!(receiver.recv().unwrap(), "first");
        drop(receiver);
        assert_eq!(
            queue.enqueue("disconnected"),
            Err(ApplyQueueError::Disconnected("disconnected"))
        );
    }

    #[test]
    fn duplicate_event_is_auditable_and_does_not_advance() {
        struct MemoryLog;
        impl DurableLog for MemoryLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                Ok(())
            }
        }
        let mut state = SingleWriter::new("scope".into(), MemoryLog);
        let first = event(
            1,
            EventKind::FullBook {
                token_id: "t".into(),
                bids: levels("0.2", "1"),
                asks: levels("0.8", "1"),
                tick_size: Price::parse_tick("0.01").unwrap(),
                tick_version: "tick-v1".into(),
            },
        );
        state.apply(first.clone()).unwrap();
        let mut duplicate = first;
        duplicate.cursor = 2;
        assert!(matches!(
            state.apply(duplicate),
            Err(CoreError::DuplicateEvent(_))
        ));
        assert_eq!(state.projection().cursor, 1);
    }

    #[test]
    fn wal_chain_and_corruption_are_verified() {
        let dir = tempdir().unwrap();
        let mut wal = SegmentedWal::open(dir.path(), "chain", 1024).unwrap();
        for cursor in 1..=8 {
            wal.append(&event(
                cursor,
                EventKind::FullBook {
                    token_id: format!("token-{cursor}"),
                    bids: levels("0.2", "123456789.123456789"),
                    asks: levels("0.8", "987654321.987654321"),
                    tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "tick-v1".into(),
                },
            ))
            .unwrap();
        }
        drop(wal);
        assert_eq!(SegmentedWal::verify(dir.path()).unwrap().len(), 8);
        let mut paths: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        paths.sort();
        OpenOptions::new()
            .append(true)
            .open(paths.last().unwrap())
            .unwrap()
            .write_all(b"{corrupt}\n")
            .unwrap();
        assert!(SegmentedWal::verify(dir.path()).is_err());
    }

    #[test]
    fn wal_restart_preserves_segment_hash_chain() {
        let dir = tempdir().unwrap();
        let mut first = SegmentedWal::open(dir.path(), "restart", 1024).unwrap();
        first
            .append(&event(
                1,
                EventKind::SourceGap {
                    token_id: "t".into(),
                    reason: "disconnect".into(),
                },
            ))
            .unwrap();
        drop(first);

        let mut resumed = SegmentedWal::open(dir.path(), "restart", 1024).unwrap();
        resumed
            .append(&event(
                2,
                EventKind::SourceGap {
                    token_id: "t".into(),
                    reason: "reconnect_pending".into(),
                },
            ))
            .unwrap();
        drop(resumed);
        let records = SegmentedWal::verify(dir.path()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].event.cursor, 2);
    }

    #[test]
    fn checkpoint_replay_skips_boundary_and_detects_decision_divergence() {
        struct MemoryLog(Vec<PersistedEvent>);
        impl DurableLog for MemoryLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                unreachable!("append_outcome is used")
            }

            fn append_outcome(&mut self, outcome: &PersistedEvent) -> Result<(), CoreError> {
                self.0.push(outcome.clone());
                Ok(())
            }
        }

        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        let first = writer
            .apply(event(
                1,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.2", "1"),
                    asks: levels("0.8", "1"),
                    tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "tick-v1".into(),
                },
            ))
            .unwrap();
        let checkpoint = (*first.projection).clone();
        let rejected = writer
            .apply(event(
                2,
                EventKind::Delta {
                    token_id: "t".into(),
                    side: Side::Bid,
                    levels: levels("0.9", "1"),
                },
            ))
            .unwrap();
        let recovered = writer
            .apply(event(
                3,
                EventKind::FullBook {
                    token_id: "t".into(),
                    bids: levels("0.3", "2"),
                    asks: levels("0.7", "2"),
                    tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "tick-v2".into(),
                },
            ))
            .unwrap();
        let records = vec![first.persisted, rejected.persisted, recovered.persisted];

        let replayed = replay_after_checkpoint(checkpoint.clone(), &records).unwrap();
        assert_eq!(replayed.hash(), writer.projection().hash());
        assert_eq!(replayed.cursor, 3);

        let mut divergent = records;
        divergent[1].applied = true;
        assert!(matches!(
            replay_after_checkpoint(checkpoint, &divergent),
            Err(CoreError::ReplayDivergence { cursor: 2 })
        ));
    }

    fn market(market_id: &str, condition_id: &str, negative_group: Option<&str>) -> MarketRecord {
        MarketRecord {
            market_id: market_id.into(),
            condition_id: condition_id.into(),
            outcomes: [
                OutcomeToken {
                    token_id: format!("{market_id}-yes"),
                    outcome: "Yes".into(),
                    instrument_id: format!("POLY.{market_id}.YES"),
                },
                OutcomeToken {
                    token_id: format!("{market_id}-no"),
                    outcome: "No".into(),
                    instrument_id: format!("POLY.{market_id}.NO"),
                },
            ],
            negative_risk_group: negative_group.map(str::to_owned),
            lifecycle_state: MarketLifecycleState::Active,
            resolution: None,
            metadata_revision: format!("revision-{market_id}"),
            observed_at: Utc::now(),
            terminal_at: None,
        }
    }

    #[test]
    fn catalog_requires_every_active_book_and_resolution_is_durably_fail_closed() {
        struct MemoryLog(Vec<PersistedEvent>);
        impl DurableLog for MemoryLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                unreachable!()
            }
            fn append_outcome(&mut self, event: &PersistedEvent) -> Result<(), CoreError> {
                self.0.push(event.clone());
                Ok(())
            }
        }
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        let market = market("m1", "condition-1", None);
        assert!(
            writer
                .apply(event(
                    1,
                    EventKind::CatalogSnapshot {
                        catalog_revision: "catalog-1".into(),
                        markets: vec![market],
                        negative_risk_relations: Vec::new(),
                    },
                ))
                .unwrap()
                .persisted
                .applied
        );
        for (cursor, token_id) in [(2, "m1-yes"), (3, "m1-no")] {
            let outcome = writer
                .apply(event(
                    cursor,
                    EventKind::FullBook {
                        token_id: token_id.into(),
                        bids: levels("0.4", "1"),
                        asks: levels("0.6", "1"),
                        tick_size: Price::parse_tick("0.01").unwrap(),
                        tick_version: "tick-v1".into(),
                    },
                ))
                .unwrap();
            assert_eq!(outcome.projection.ready, cursor == 3);
        }
        let resolved = writer
            .apply(event(
                4,
                EventKind::MarketResolved {
                    condition_id: "condition-1".into(),
                    winning_token_id: Some("m1-yes".into()),
                    winning_outcome: Some("Yes".into()),
                },
            ))
            .unwrap();
        assert!(resolved.persisted.applied);
        assert_eq!(resolved.persisted.fail_closed_reason.as_deref(), None);
        assert!(resolved.projection.books.is_empty());
        assert_eq!(
            resolved.projection.markets["m1"].lifecycle_state,
            MarketLifecycleState::Resolved
        );

        let refreshed = writer
            .apply(event(
                5,
                EventKind::CatalogSnapshot {
                    catalog_revision: "catalog-2".into(),
                    markets: Vec::new(),
                    negative_risk_relations: Vec::new(),
                },
            ))
            .unwrap();
        assert!(refreshed.persisted.applied);
        assert!(refreshed.projection.markets.contains_key("m1"));
        assert!(!refreshed.projection.ready);
    }

    #[test]
    fn negative_risk_catalog_requires_exact_complete_yes_membership() {
        struct MemoryLog;
        impl DurableLog for MemoryLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                Ok(())
            }
        }
        let markets = vec![
            market("m1", "condition-1", Some("group-1")),
            market("m2", "condition-2", Some("group-1")),
        ];
        let invalid_relation = NegativeRiskRelation {
            group_id: "group-1".into(),
            member_market_ids: ["m1".into(), "m2".into()].into(),
            yes_token_ids: ["m1-yes".into()].into(),
            revision: "relation-1".into(),
            complete: true,
            valid_to: None,
        };
        let mut writer = SingleWriter::new("scope".into(), MemoryLog);
        let rejected = writer
            .apply(event(
                1,
                EventKind::CatalogSnapshot {
                    catalog_revision: "catalog-invalid".into(),
                    markets,
                    negative_risk_relations: vec![invalid_relation],
                },
            ))
            .unwrap();
        assert!(!rejected.persisted.applied);
        assert_eq!(
            rejected.persisted.fail_closed_reason.as_deref(),
            Some("invalid_catalog_snapshot")
        );
        assert!(
            rejected
                .projection
                .unresolved_gaps
                .contains("catalog:invalid")
        );
    }

    #[test]
    fn checkpoint_corruption_is_rejected() {
        struct MemoryLog;
        impl DurableLog for MemoryLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                Ok(())
            }
        }
        let dir = tempdir().unwrap();
        let state = SingleWriter::new("scope".into(), MemoryLog);
        let path = dir.path().join("checkpoint.json");
        let hash = write_checkpoint(&path, &state.projection()).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"corrupt")
            .unwrap();
        assert!(matches!(
            read_checkpoint(&path, &hash),
            Err(CoreError::CheckpointHash)
        ));
    }
}
