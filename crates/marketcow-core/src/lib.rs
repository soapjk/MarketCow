//! Deterministic realtime domain core. It deliberately has no async runtime, HTTP or DB dependency.

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
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

    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    pub fn is_one(&self) -> bool {
        self.0 == Decimal::ONE
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authoritative_refresh_received_at: Option<DateTime<Utc>>,
    pub last_trade_price: Option<Price>,
    pub last_trade_observed_at: Option<DateTime<Utc>>,
}

impl Book {
    pub fn levels_tick_aligned(tick_size: &Price, levels: &[Level]) -> bool {
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
#[serde(transparent)]
pub struct Quantity(#[serde(with = "rust_decimal::serde::str")] pub Decimal);

impl Quantity {
    pub fn parse_positive(value: &str) -> Result<Self, CoreError> {
        let parsed =
            Decimal::from_str(value).map_err(|_| CoreError::InvalidDecimal(value.into()))?;
        if parsed <= Decimal::ZERO {
            return Err(CoreError::InvalidQuantity);
        }
        Ok(Self(parsed.normalize()))
    }
}

/// Immutable strategy facts sourced from the hash-pinned catalog. These values travel in the
/// same catalog event/cursor boundary as the market/outcome identity mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketInstrumentFacts {
    pub price_increment: Price,
    pub size_increment: Quantity,
    pub minimum_order_size: Quantity,
    pub settlement_currency: String,
    pub start_at: DateTime<Utc>,
    pub end_at: DateTime<Utc>,
    pub revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fee_schedule: Option<MarketFeeSchedule>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeCalculationStatus {
    Executable,
    InformationalOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketFeeProvenance {
    pub source: String,
    pub source_url: String,
    pub revision: String,
    pub payload_sha256: String,
    pub observed_at: DateTime<Utc>,
    pub field_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketFeeSchedule {
    pub schedule_id: String,
    pub revision: String,
    pub schedule_version: String,
    pub currency: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub maker_rate: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub taker_rate: Decimal,
    pub formula_id: String,
    pub formula: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub exponent: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantum: Decimal,
    pub rounding_mode: String,
    pub tie_semantics: String,
    pub calculation_status: FeeCalculationStatus,
    pub effective_from: DateTime<Utc>,
    pub effective_to: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
    pub provenance: Vec<MarketFeeProvenance>,
}

impl MarketFeeSchedule {
    pub fn is_complete(&self) -> bool {
        let digest =
            |value: &str| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
        digest(&self.schedule_id)
            && digest(&self.revision)
            && !self.schedule_version.is_empty()
            && !self.currency.is_empty()
            && self.maker_rate >= Decimal::ZERO
            && self.taker_rate >= Decimal::ZERO
            && !self.formula_id.is_empty()
            && !self.formula.is_empty()
            && self.exponent >= Decimal::ZERO
            && self.quantum > Decimal::ZERO
            && !self.rounding_mode.is_empty()
            && !self.tie_semantics.is_empty()
            && !(self.calculation_status == FeeCalculationStatus::Executable
                && (self.rounding_mode.eq_ignore_ascii_case("UNSPECIFIED")
                    || self.tie_semantics.eq_ignore_ascii_case("unspecified")))
            && self
                .effective_to
                .is_none_or(|effective_to| effective_to > self.effective_from)
            && !self.provenance.is_empty()
            && self.provenance.iter().all(|item| {
                !item.source.is_empty()
                    && !item.source_url.is_empty()
                    && !item.revision.is_empty()
                    && digest(&item.payload_sha256)
                    && !item.field_paths.is_empty()
                    && item.field_paths.iter().all(|path| !path.is_empty())
            })
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument_facts: Option<MarketInstrumentFacts>,
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
        /// Venue-reported top of book for the same atomic price-change boundary. These fields are
        /// internal recovery evidence: the public stream contract deliberately continues to
        /// expose only `changes`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        best_bid: Option<Price>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        best_ask: Option<Price>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market: Option<MarketEventMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketProjectionStatus {
    Recovering,
    Ready,
    TemporarilyUnavailable,
    Quarantined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketTransitionKind {
    Quarantined,
    RecoveryStarted,
    Recovered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketEventMetadata {
    pub market_id: String,
    pub market_sequence: u64,
    pub projection_generation: u64,
    pub catalog_revision: String,
    pub event_revision: String,
    pub projection_status: MarketProjectionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transition: Option<MarketTransitionKind>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketProjectionHealth {
    pub market_id: String,
    pub projection_status: MarketProjectionStatus,
    pub last_market_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gap_from: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gap_to: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_observed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_recovered_at: Option<DateTime<Utc>>,
    pub projection_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_revision: Option<String>,
}

impl MarketProjectionHealth {
    fn recovering(market_id: String, generation: u64, catalog_revision: Option<String>) -> Self {
        Self {
            market_id,
            projection_status: MarketProjectionStatus::Recovering,
            last_market_sequence: 0,
            gap_from: None,
            gap_to: None,
            reason_code: Some("initial_snapshot_required".into()),
            retryable: true,
            retry_after: None,
            source_observed_at: None,
            last_recovered_at: None,
            projection_generation: generation,
            catalog_revision,
            last_event_revision: None,
        }
    }
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
    #[serde(default)]
    pub monitoring_markets: BTreeMap<String, MarketRecord>,
    #[serde(default)]
    pub monitoring_books: BTreeMap<String, Book>,
    #[serde(default)]
    pub market_health: BTreeMap<String, MarketProjectionHealth>,
    #[serde(default)]
    pub quarantined_token_ids: BTreeSet<String>,
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
            monitoring_markets: BTreeMap::new(),
            monitoring_books: BTreeMap::new(),
            market_health: BTreeMap::new(),
            quarantined_token_ids: BTreeSet::new(),
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
            "monitoring_markets": self.monitoring_markets,
            "monitoring_books": self.monitoring_books,
            "market_health": self.market_health,
            "quarantined_token_ids": self.quarantined_token_ids,
            "unresolved_gaps": self.unresolved_gaps,
            "recent_event_ids": self.recent_event_ids,
            "ready": self.ready,
            "fail_closed_reason": self.fail_closed_reason,
        });
        let bytes = serde_json::to_vec(&value).expect("projection is serializable");
        hex::encode(Sha256::digest(bytes))
    }

    /// Verifies the public strategy tick invariant without mutating recovered state. This is used
    /// by the HTTP readiness boundary so a legacy checkpoint cannot briefly expose stale catalog
    /// facts before the next authoritative event reconciles it.
    pub fn instrument_ticks_consistent(&self) -> bool {
        self.catalog_revision.is_none()
            || self
                .markets
                .values()
                .filter(|market| market.lifecycle_state == MarketLifecycleState::Active)
                .filter(|market| self.market_is_public(&market.market_id))
                .filter_map(|market| {
                    market
                        .instrument_facts
                        .as_ref()
                        .map(|facts| (market, facts))
                })
                .all(|(market, facts)| {
                    market.outcomes.iter().all(|outcome| {
                        self.books.get(&outcome.token_id).is_some_and(|book| {
                            !book.tick_version.is_empty()
                                && book.tick_size.as_ref() == Some(&facts.price_increment)
                        })
                    })
                })
    }

    /// Verifies that every active catalog instrument still has an executable two-sided book.
    ///
    /// Dynamic-universe activation validates the initial books, but that qualification is not a
    /// lifetime property: a later authoritative full-book can legitimately remove the last level
    /// from one side.  A projection containing such a market must stop being public-ready until a
    /// fresh universe generation isolates/replaces it (or the same book becomes two-sided again).
    /// Keeping this check on the immutable projection also prevents an older checkpoint whose
    /// persisted `ready` bit predates this invariant from being exposed after restart.
    pub fn active_market_books_two_sided(&self) -> bool {
        self.catalog_revision.is_none()
            || self
                .markets
                .values()
                .filter(|market| market.lifecycle_state == MarketLifecycleState::Active)
                .filter(|market| self.market_is_public(&market.market_id))
                .flat_map(|market| market.outcomes.iter())
                .all(|outcome| {
                    self.books
                        .get(&outcome.token_id)
                        .is_some_and(|book| !book.bids.is_empty() && !book.asks.is_empty())
                })
    }

    pub fn market_id_for_token(&self, token_id: &str) -> Option<&str> {
        self.markets.values().find_map(|market| {
            market
                .outcomes
                .iter()
                .any(|outcome| outcome.token_id == token_id)
                .then_some(market.market_id.as_str())
        })
    }

    pub fn market_is_public(&self, market_id: &str) -> bool {
        self.market_health
            .get(market_id)
            .is_none_or(|health| health.projection_status == MarketProjectionStatus::Ready)
    }

    pub fn active_market_ids(&self) -> BTreeSet<String> {
        self.markets
            .values()
            .filter(|market| {
                market.lifecycle_state == MarketLifecycleState::Active
                    && self.market_is_public(&market.market_id)
            })
            .map(|market| market.market_id.clone())
            .collect()
    }

    pub fn quarantined_market_ids(&self) -> BTreeSet<String> {
        self.market_health
            .iter()
            .filter(|(market_id, health)| {
                self.markets
                    .get(*market_id)
                    .is_some_and(|market| market.lifecycle_state == MarketLifecycleState::Active)
                    && health.projection_status != MarketProjectionStatus::Ready
            })
            .map(|(market_id, _)| market_id.clone())
            .collect()
    }

    pub fn active_token_ids(&self) -> BTreeSet<String> {
        if self.catalog_revision.is_none() {
            return self.books.keys().cloned().collect();
        }
        self.markets
            .values()
            .filter(|market| {
                market.lifecycle_state == MarketLifecycleState::Active
                    && self.market_is_public(&market.market_id)
            })
            .flat_map(|market| {
                market
                    .outcomes
                    .iter()
                    .map(|outcome| outcome.token_id.clone())
            })
            .collect()
    }

    pub fn token_has_recovery_gap(&self, token_id: &str) -> bool {
        self.unresolved_gaps.contains(token_id) || self.quarantined_token_ids.contains(token_id)
    }
}

fn event_market_id(projection: &Projection, kind: &EventKind) -> Option<String> {
    match kind {
        EventKind::CatalogSnapshot { .. } | EventKind::NewMarket { .. } => None,
        EventKind::MarketResolved { condition_id, .. } => {
            projection.condition_to_market.get(condition_id).cloned()
        }
        EventKind::SourceGap { reason, .. }
            if matches!(reason.as_str(), "upstream_connection_boundary") =>
        {
            // The transport emits one token-shaped record per subscription at a connection
            // boundary, but the missing interval is connection-wide. Token shape is not proof
            // of market-local attribution, so this must remain a global fail-closed boundary.
            None
        }
        EventKind::FullBook { token_id, .. }
        | EventKind::Delta { token_id, .. }
        | EventKind::AtomicDelta { token_id, .. }
        | EventKind::BestBidAsk { token_id, .. }
        | EventKind::LastTradePrice { token_id, .. }
        | EventKind::TickSizeChange { token_id, .. }
        | EventKind::SourceGap { token_id, .. } => {
            projection.market_id_for_token(token_id).map(str::to_owned)
        }
    }
}

fn market_event_is_locally_recoverable(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::FullBook { .. }
            | EventKind::Delta { .. }
            | EventKind::AtomicDelta { .. }
            | EventKind::BestBidAsk { .. }
            | EventKind::LastTradePrice { .. }
            | EventKind::SourceGap { .. }
    )
}

fn market_books_are_executable(projection: &Projection, market_id: &str) -> bool {
    let Some(market) = projection.markets.get(market_id) else {
        return false;
    };
    if market.lifecycle_state != MarketLifecycleState::Active {
        return false;
    }
    if let Some(facts) = market.instrument_facts.as_ref()
        && (facts
            .fee_schedule
            .as_ref()
            .is_some_and(|schedule| !schedule.is_complete())
            || market.outcomes.iter().any(|outcome| {
                projection
                    .books
                    .get(&outcome.token_id)
                    .is_none_or(|book| book.tick_size.as_ref() != Some(&facts.price_increment))
            }))
    {
        return false;
    }
    if let Some(group_id) = market.negative_risk_group.as_ref()
        && projection
            .negative_risk_relations
            .get(group_id)
            .is_none_or(|relation| {
                !relation.complete
                    || relation.revision.is_empty()
                    || !relation.member_market_ids.contains(market_id)
            })
    {
        return false;
    }
    market.outcomes.iter().all(|outcome| {
        !projection.quarantined_token_ids.contains(&outcome.token_id)
            && projection.books.get(&outcome.token_id).is_some_and(|book| {
                !book.bids.is_empty()
                    && !book.asks.is_empty()
                    && book.tick_size.is_some()
                    && !book.tick_version.is_empty()
                    && !book.crossed_or_locked()
            })
    })
}

fn quarantine_market(
    projection: &mut Projection,
    market_id: &str,
    sequence: u64,
    reason: &str,
    event: &CanonicalEvent,
) {
    let token_ids = projection
        .markets
        .get(market_id)
        .map(|market| {
            market
                .outcomes
                .iter()
                .map(|outcome| outcome.token_id.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for token_id in token_ids {
        projection.unresolved_gaps.remove(&token_id);
        projection.quarantined_token_ids.insert(token_id);
    }
    let health = projection
        .market_health
        .entry(market_id.to_owned())
        .or_insert_with(|| {
            MarketProjectionHealth::recovering(
                market_id.to_owned(),
                projection.generation,
                projection.catalog_revision.clone(),
            )
        });
    health.projection_status = MarketProjectionStatus::Quarantined;
    health.gap_from.get_or_insert(sequence);
    health.gap_to = Some(sequence);
    health.reason_code = Some(reason.to_owned());
    health.retryable = true;
    health.retry_after = Some(event.received_at + chrono::Duration::seconds(1));
}

/// Reconciles the catalog's market-level executable price increment with the two authoritative
/// outcome-token books. Polymarket binary outcomes share one market tick, but the tick may change
/// after the hash-pinned catalog was produced. The reconciliation is all-or-nothing across the
/// active catalog: a missing book, missing revision, or disagreement between outcome tokens keeps
/// the projection fail-closed and leaves every catalog fact untouched.
fn reconcile_active_market_instrument_ticks(projection: &mut Projection) -> bool {
    if projection.catalog_revision.is_none() {
        return true;
    }

    let mut updates = Vec::new();
    for market in projection
        .markets
        .values()
        .filter(|market| market.lifecycle_state == MarketLifecycleState::Active)
    {
        let public_market = projection.market_is_public(&market.market_id);
        let Some(facts) = market.instrument_facts.as_ref() else {
            // Generic catalog users may not publish strategy facts. Exact Rust Polymarket scopes
            // separately require them at configuration and readiness boundaries.
            continue;
        };
        let mut common_tick: Option<Price> = None;
        let mut token_ticks = BTreeMap::new();
        let mut complete = true;
        for outcome in &market.outcomes {
            let Some(book) = projection.books.get(&outcome.token_id) else {
                complete = false;
                break;
            };
            let Some(tick_size) = book.tick_size.as_ref() else {
                complete = false;
                break;
            };
            if book.tick_version.is_empty()
                || common_tick
                    .as_ref()
                    .is_some_and(|expected| expected != tick_size)
            {
                complete = false;
                break;
            }
            common_tick.get_or_insert_with(|| tick_size.clone());
            token_ticks.insert(
                outcome.token_id.clone(),
                serde_json::json!({
                    "tick_size": tick_size,
                    "tick_version": book.tick_version,
                }),
            );
        }
        if !complete {
            if public_market {
                return false;
            }
            continue;
        }
        let Some(price_increment) = common_tick else {
            if public_market {
                return false;
            }
            continue;
        };
        // This revision is independently reproducible from the same immutable projection. It
        // binds every market fact plus both token-level tick revisions without relying on wall
        // time or mutation history.
        let revision_material = serde_json::json!({
            "schema_version": "marketcow.polymarket.instrument-facts-revision.v1",
            "market_id": market.market_id,
            "condition_id": market.condition_id,
            "price_increment": price_increment,
            "size_increment": facts.size_increment,
            "minimum_order_size": facts.minimum_order_size,
            "settlement_currency": facts.settlement_currency,
            "start_at": facts.start_at,
            "end_at": facts.end_at,
            "token_ticks": token_ticks,
        });
        let revision = hex::encode(Sha256::digest(
            serde_json::to_vec(&revision_material).expect("instrument facts are serializable"),
        ));
        updates.push((market.market_id.clone(), price_increment, revision));
    }

    for (market_id, price_increment, revision) in updates {
        let facts = projection
            .markets
            .get_mut(&market_id)
            .and_then(|market| market.instrument_facts.as_mut())
            .expect("active market facts were validated above");
        facts.price_increment = price_increment;
        facts.revision = revision;
    }
    true
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

    /// Grants the runtime access to durability operations that do not publish market state.
    /// This is intentionally mutable so an implementation can establish a durable checkpoint
    /// boundary before the checkpoint manifest becomes visible.
    pub fn durable_log_mut(&mut self) -> &mut L {
        &mut self.log
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
        let mut next = (*self.current.load_full()).clone();
        // A provider frame can contain many logical updates. Scan the bounded duplicate window
        // once instead of linearly scanning it for every event in the same batch.
        let mut event_ids = next
            .recent_event_ids
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        for event in &events {
            if !event_ids.insert(event.event_id.clone()) {
                return Err(CoreError::DuplicateEvent(event.event_id.clone()));
            }
        }
        let mut persisted = Vec::with_capacity(events.len());
        for event in events {
            let (updated, outcome) = Self::evaluate_event(next, event, false, false)?;
            next = updated;
            persisted.push(outcome);
        }
        let persistence_started = std::time::Instant::now();
        self.log.append_outcomes(&persisted)?;
        let persistence_latency_us = persistence_started.elapsed().as_micros() as u64;
        let final_projection = Arc::new(next);
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

    fn evaluate_event(
        mut next: Projection,
        event: CanonicalEvent,
        validate_raw_payload: bool,
        validate_duplicate: bool,
    ) -> Result<(Projection, PersistedEvent), CoreError> {
        if event.schema_version != CONTRACT_VERSION {
            return Err(CoreError::SchemaMismatch);
        }
        if event.scope_id != next.scope_id {
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
        if validate_duplicate && next.recent_event_ids.contains(&event.event_id) {
            return Err(CoreError::DuplicateEvent(event.event_id));
        }
        if event.cursor != next.cursor + 1 {
            return Err(CoreError::CursorGap {
                expected: next.cursor + 1,
                actual: event.cursor,
            });
        }
        next.generation += 1;
        next.cursor = event.cursor;
        next.published_at = Utc::now();
        let event_market_id = event_market_id(&next, &event.kind);
        if let Some(market_id) = event_market_id.as_ref()
            && !next.market_health.contains_key(market_id)
            && market_books_are_executable(&next, market_id)
        {
            let mut health = MarketProjectionHealth::recovering(
                market_id.clone(),
                next.generation,
                next.catalog_revision.clone(),
            );
            health.projection_status = MarketProjectionStatus::Ready;
            health.reason_code = None;
            health.retryable = false;
            next.market_health.insert(market_id.clone(), health);
        }
        let market_sequence = event_market_id.as_ref().map(|market_id| {
            next.market_health
                .get(market_id)
                .map_or(1, |health| health.last_market_sequence.saturating_add(1))
        });
        let previous_market_status = event_market_id
            .as_ref()
            .and_then(|market_id| next.market_health.get(market_id))
            .map(|health| health.projection_status);
        let mut applied = true;
        let mut rejected = None;
        let delayed_atomic_delta_superseded_by_book = event.source.delayed
            && event.normalizer_version == "marketcow.polymarket.normalizer.v2"
            && matches!(event.kind, EventKind::AtomicDelta { .. })
            && next
                .books
                .get(event.kind.token_id())
                .and_then(|book| book.source_observed_at)
                .is_some_and(|book_at| event.source_observed_at <= book_at);
        let queued_atomic_delta_superseded_by_refresh = event.normalizer_version
            == "marketcow.polymarket.normalizer.v2"
            && matches!(event.kind, EventKind::AtomicDelta { .. })
            && next.books.get(event.kind.token_id()).is_some_and(|book| {
                book.authoritative_refresh_received_at
                    .is_some_and(|refresh_at| {
                        event.received_at < refresh_at
                            || book
                                .source_observed_at
                                .is_some_and(|book_at| event.source_observed_at < book_at)
                    })
            });
        let atomic_delta_superseded_by_book =
            delayed_atomic_delta_superseded_by_book || queued_atomic_delta_superseded_by_refresh;
        if event.source.missing || event.source.delayed && !atomic_delta_superseded_by_book {
            next.unresolved_gaps.insert(event.kind.token_id().into());
            applied = false;
            rejected = Some(if event.source.missing {
                "source_data_missing".into()
            } else {
                "source_data_delayed".into()
            });
        } else if atomic_delta_superseded_by_book {
            // The frame is older than an already applied authoritative full book. This includes
            // WS frames queued while a periodic HTTP snapshot was in flight. Keep its durable
            // cursor/evidence, but do not mutate the newer projection or reopen the gap. The
            // public API exposes this as an explicit no-op delta.
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
                        // A dynamic-universe replacement removes a market from opportunity
                        // scanning, but that must not erase the stable identities and last
                        // authoritative books a position owner needs for settlement monitoring.
                        // These retained records never participate in public readiness or
                        // opportunity generation.
                        let next_catalog_ids = catalog.keys().cloned().collect::<BTreeSet<_>>();
                        let removed_active_markets = next
                            .markets
                            .values()
                            .filter(|market| {
                                market.lifecycle_state == MarketLifecycleState::Active
                                    && !next_catalog_ids.contains(&market.market_id)
                            })
                            .cloned()
                            .collect::<Vec<_>>();
                        for market in removed_active_markets {
                            for outcome in &market.outcomes {
                                if let Some(book) = next.books.get(&outcome.token_id) {
                                    next.monitoring_books
                                        .insert(outcome.token_id.clone(), book.clone());
                                }
                            }
                            let health = next
                                .market_health
                                .entry(market.market_id.clone())
                                .or_insert_with(|| {
                                    MarketProjectionHealth::recovering(
                                        market.market_id.clone(),
                                        next.generation,
                                        Some(catalog_revision.clone()),
                                    )
                                });
                            health.projection_status =
                                MarketProjectionStatus::TemporarilyUnavailable;
                            health.reason_code = Some("removed_from_scan_universe".into());
                            health.retryable = false;
                            health.retry_after = None;
                            health.projection_generation = next.generation;
                            health.catalog_revision = Some(catalog_revision.clone());
                            next.monitoring_markets
                                .insert(market.market_id.clone(), market);
                        }
                        // If a retained identity becomes active again, its new catalog facts and
                        // live books are authoritative. Drop only the stale monitoring copy.
                        for market in catalog.values() {
                            if let Some(retained) =
                                next.monitoring_markets.remove(&market.market_id)
                            {
                                for outcome in retained.outcomes {
                                    next.monitoring_books.remove(&outcome.token_id);
                                }
                            }
                        }
                        next.catalog_revision = Some(catalog_revision.clone());
                        next.markets = catalog;
                        next.condition_to_market = conditions;
                        next.negative_risk_relations = relations;
                        next.books
                            .retain(|token_id, _| active_tokens.contains(token_id));
                        next.quarantined_token_ids
                            .retain(|token_id| active_tokens.contains(token_id));
                        next.market_health.retain(|market_id, _| {
                            next.markets.get(market_id).is_some_and(|market| {
                                market.lifecycle_state == MarketLifecycleState::Active
                            }) || next.monitoring_markets.contains_key(market_id)
                        });
                        for market in next
                            .markets
                            .values()
                            .filter(|market| market.lifecycle_state == MarketLifecycleState::Active)
                        {
                            next.market_health
                                .entry(market.market_id.clone())
                                .or_insert_with(|| {
                                    MarketProjectionHealth::recovering(
                                        market.market_id.clone(),
                                        next.generation,
                                        next.catalog_revision.clone(),
                                    )
                                });
                        }
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
                        book.authoritative_refresh_received_at = (event.source.source
                            == "polymarket_clob_http")
                            .then_some(event.received_at);
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
                    if next.token_has_recovery_gap(token_id) || !next.books.contains_key(token_id) {
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
                EventKind::AtomicDelta {
                    token_id,
                    changes,
                    best_bid,
                    best_ask,
                } => {
                    if changes.is_empty()
                        || next.token_has_recovery_gap(token_id)
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
                            let corrected_bbo_semantics =
                                event.normalizer_version == "marketcow.polymarket.normalizer.v2";
                            let expected_bid = best_bid
                                .as_ref()
                                .filter(|price| !corrected_bbo_semantics || !price.is_zero());
                            let expected_ask = best_ask
                                .as_ref()
                                .filter(|price| !corrected_bbo_semantics || !price.is_one());
                            let bbo_observed = best_bid.is_some() || best_ask.is_some();
                            let invalid_source_quote = expected_bid
                                .zip(expected_ask)
                                .is_some_and(|(bid, ask)| bid >= ask);
                            if bbo_observed {
                                if let Some(expected) = expected_bid {
                                    // A venue BBO proves that any locally retained bid above it is
                                    // stale even when the incremental frame omitted that deletion.
                                    book.bids.retain(|price, _| price <= expected);
                                } else {
                                    // Polymarket reports an empty bid side as `best_bid=0`.
                                    book.bids.clear();
                                }
                                if let Some(expected) = expected_ask {
                                    // Likewise, asks below the venue BBO cannot remain in the atomic
                                    // post-frame projection.
                                    book.asks.retain(|price, _| price >= expected);
                                } else {
                                    // Polymarket reports an empty ask side as `best_ask=1`.
                                    book.asks.clear();
                                }
                            }
                            let projected_bid = book.bids.last_key_value().map(|(price, _)| price);
                            let projected_ask = book.asks.first_key_value().map(|(price, _)| price);
                            let source_quote_mismatch = bbo_observed
                                && (projected_bid != expected_bid || projected_ask != expected_ask);
                            if invalid_source_quote || source_quote_mismatch {
                                next.unresolved_gaps.insert(token_id.clone());
                                applied = false;
                                rejected = Some("best_bid_ask_source_mismatch".into());
                            } else if book.crossed_or_locked() {
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
                    if next.token_has_recovery_gap(token_id) || !next.books.contains_key(token_id) {
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
                    if next.token_has_recovery_gap(token_id) || !next.books.contains_key(token_id) {
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
                        || next.token_has_recovery_gap(token_id)
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
        let mut market_transition = None;
        let mut market_reason_code = None;
        if let (Some(market_id), Some(sequence), Some(reason)) = (
            event_market_id.as_deref(),
            market_sequence,
            rejected.clone(),
        ) && market_event_is_locally_recoverable(&event.kind)
        {
            quarantine_market(&mut next, market_id, sequence, &reason, &event);
            market_transition = Some(MarketTransitionKind::Quarantined);
            market_reason_code = Some(reason);
            // This is a durably recorded market fault, not a global projection fault. Consumers
            // receive a market control frame and may continue using independently healthy markets.
            rejected = None;
        }

        let instrument_ticks_consistent = reconcile_active_market_instrument_ticks(&mut next);
        if let (Some(market_id), Some(sequence)) = (event_market_id.as_deref(), market_sequence) {
            if let EventKind::FullBook { token_id, .. } = &event.kind
                && applied
                && rejected.is_none()
                && market_reason_code.is_none()
            {
                next.quarantined_token_ids.remove(token_id);
                if market_books_are_executable(&next, market_id) {
                    if previous_market_status != Some(MarketProjectionStatus::Ready) {
                        market_transition = Some(MarketTransitionKind::Recovered);
                    }
                    let generation = next.generation;
                    let catalog_revision = next.catalog_revision.clone();
                    let health = next
                        .market_health
                        .entry(market_id.to_owned())
                        .or_insert_with(|| {
                            MarketProjectionHealth::recovering(
                                market_id.to_owned(),
                                generation,
                                catalog_revision,
                            )
                        });
                    health.projection_status = MarketProjectionStatus::Ready;
                    health.gap_from = None;
                    health.gap_to = None;
                    health.reason_code = None;
                    health.retryable = false;
                    health.retry_after = None;
                    health.last_recovered_at = Some(event.received_at);
                } else if previous_market_status != Some(MarketProjectionStatus::Ready) {
                    market_transition = Some(MarketTransitionKind::RecoveryStarted);
                    market_reason_code = Some("atomic_market_snapshot_incomplete".into());
                    let generation = next.generation;
                    let catalog_revision = next.catalog_revision.clone();
                    let health = next
                        .market_health
                        .entry(market_id.to_owned())
                        .or_insert_with(|| {
                            MarketProjectionHealth::recovering(
                                market_id.to_owned(),
                                generation,
                                catalog_revision,
                            )
                        });
                    health.projection_status = MarketProjectionStatus::Recovering;
                    health.reason_code = market_reason_code.clone();
                }
            }
            if market_reason_code.is_none()
                && rejected.is_none()
                && previous_market_status == Some(MarketProjectionStatus::Ready)
                && market_event_is_locally_recoverable(&event.kind)
                && !market_books_are_executable(&next, market_id)
            {
                let reason = "market_book_not_executable";
                quarantine_market(&mut next, market_id, sequence, reason, &event);
                market_transition = Some(MarketTransitionKind::Quarantined);
                market_reason_code = Some(reason.into());
            }
        }

        let catalog_books_complete = next.catalog_revision.is_none()
            || next
                .markets
                .values()
                .filter(|market| market.lifecycle_state == MarketLifecycleState::Active)
                .filter(|market| next.market_is_public(&market.market_id))
                .flat_map(|market| market.outcomes.iter())
                .all(|outcome| next.books.contains_key(&outcome.token_id));
        let active_market_books_two_sided = next.active_market_books_two_sided();
        let usable_projection = if next.catalog_revision.is_none() {
            !next.books.is_empty()
        } else {
            !next.active_market_ids().is_empty()
        };
        next.ready = next.unresolved_gaps.is_empty()
            && usable_projection
            && catalog_books_complete
            && instrument_ticks_consistent
            && active_market_books_two_sided;
        next.fail_closed_reason = rejected
            .clone()
            .or_else(|| {
                (!instrument_ticks_consistent).then(|| "instrument_tick_inconsistent".into())
            })
            .or_else(|| (!active_market_books_two_sided).then(|| "one_sided_active_book".into()))
            .or_else(|| {
                (!next.ready).then(|| {
                    if next.unresolved_gaps.is_empty() {
                        "no_healthy_markets".into()
                    } else {
                        "unresolved_gap".into()
                    }
                })
            });
        let market_metadata =
            event_market_id
                .as_ref()
                .zip(market_sequence)
                .map(|(market_id, sequence)| {
                    let generation = next.generation;
                    let catalog_revision = next.catalog_revision.clone();
                    let health = next
                        .market_health
                        .entry(market_id.clone())
                        .or_insert_with(|| {
                            MarketProjectionHealth::recovering(
                                market_id.clone(),
                                generation,
                                catalog_revision.clone(),
                            )
                        });
                    health.last_market_sequence = sequence;
                    health.source_observed_at = Some(event.source_observed_at);
                    health.projection_generation = generation;
                    health.catalog_revision = catalog_revision.clone();
                    health.last_event_revision = Some(event.event_id.clone());
                    MarketEventMetadata {
                        market_id: market_id.clone(),
                        market_sequence: sequence,
                        projection_generation: generation,
                        catalog_revision: catalog_revision.unwrap_or_default(),
                        event_revision: event.event_id.clone(),
                        projection_status: health.projection_status,
                        reason_code: market_reason_code.clone(),
                        transition: market_transition,
                    }
                });
        let event_cursor = event.cursor;
        let persisted = PersistedEvent {
            event,
            applied,
            fail_closed_reason: rejected,
            market: market_metadata,
        };
        next.persisted_cursor = event_cursor;
        next.recent_event_ids
            .push_back(persisted.event.event_id.clone());
        if next.recent_event_ids.len() > 10_000 {
            next.recent_event_ids.pop_front();
        }
        Ok((next, persisted))
    }

    fn apply_inner(
        &mut self,
        event: CanonicalEvent,
        validate_raw_payload: bool,
    ) -> Result<ApplyOutcome, CoreError> {
        let previous = (*self.current.load_full()).clone();
        let (next, persisted) = Self::evaluate_event(previous, event, validate_raw_payload, true)?;
        let persistence_started = std::time::Instant::now();
        self.log.append_outcome(&persisted)?;
        let persistence_latency_us = persistence_started.elapsed().as_micros() as u64;
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

#[derive(Serialize)]
struct WalCanonicalEventV2<'a> {
    schema_version: &'a str,
    cursor: u64,
    event_id: &'a str,
    scope_id: &'a str,
    received_at: &'a DateTime<Utc>,
    source_observed_at: &'a DateTime<Utc>,
    normalizer_version: &'a str,
    config_revision: &'a str,
    source: &'a SourceEvidence,
    raw_payload: (),
    kind: &'a EventKind,
}

#[derive(Serialize)]
struct WalPersistedEventV2<'a> {
    event: WalCanonicalEventV2<'a>,
    applied: bool,
    fail_closed_reason: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    market: &'a Option<MarketEventMetadata>,
}

impl<'a> From<&'a PersistedEvent> for WalPersistedEventV2<'a> {
    fn from(persisted: &'a PersistedEvent) -> Self {
        let event = &persisted.event;
        Self {
            event: WalCanonicalEventV2 {
                schema_version: &event.schema_version,
                cursor: event.cursor,
                event_id: &event.event_id,
                scope_id: &event.scope_id,
                received_at: &event.received_at,
                source_observed_at: &event.source_observed_at,
                normalizer_version: &event.normalizer_version,
                config_revision: &event.config_revision,
                source: &event.source,
                raw_payload: (),
                kind: &event.kind,
            },
            applied: persisted.applied,
            fail_closed_reason: &persisted.fail_closed_reason,
            market: &persisted.market,
        }
    }
}

#[derive(Serialize)]
struct WalBatchRecordV2<'a> {
    batch_version: u8,
    first_cursor: u64,
    last_cursor: u64,
    crc32c: u32,
    payload_sha256: String,
    frame_raw_payload: &'a serde_json::Value,
    payloads: Vec<WalPersistedEventV2<'a>>,
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
    segment_has_records: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalCheckpointAnchor {
    pub checkpoint_cursor: u64,
    pub checkpoint_sha256: String,
    pub segment_first_cursor: u64,
    pub segment_header_sha256: String,
}

#[derive(Debug)]
pub struct VerifiedSegmentedWal {
    pub records: Vec<PersistedEvent>,
    pub checkpoint_anchors: Vec<WalCheckpointAnchor>,
}

impl SegmentedWal {
    pub fn open(
        root: impl AsRef<Path>,
        stream_id: &str,
        max_segment_bytes: u64,
    ) -> Result<Self, CoreError> {
        Self::open_inner(root.as_ref(), stream_id, max_segment_bytes, true)
    }

    /// Opens an exact, byte-verified physical copy of a WAL already owned by a live
    /// `SegmentedWal`. The caller must verify every copied regular file before calling this
    /// method. This avoids parsing the entire authoritative history a second time while
    /// preparing an isolated hot-switch candidate.
    pub fn open_verified_copy(
        root: impl AsRef<Path>,
        stream_id: &str,
        max_segment_bytes: u64,
    ) -> Result<Self, CoreError> {
        Self::open_after_verification(root, stream_id, max_segment_bytes)
    }

    /// Opens a WAL immediately after the caller verified the same root. This avoids a redundant
    /// full parse during single-threaded startup while retaining `open` as the safe default.
    pub fn open_after_verification(
        root: impl AsRef<Path>,
        stream_id: &str,
        max_segment_bytes: u64,
    ) -> Result<Self, CoreError> {
        Self::open_inner(root.as_ref(), stream_id, max_segment_bytes, false)
    }

    fn open_inner(
        root: &Path,
        stream_id: &str,
        max_segment_bytes: u64,
        verify_chain: bool,
    ) -> Result<Self, CoreError> {
        if !root.is_absolute() {
            return Err(CoreError::PathMustBeAbsolute);
        }
        if max_segment_bytes < 1024 {
            return Err(CoreError::SegmentTooSmall);
        }
        fs::create_dir_all(root)?;
        let mut paths = wal_paths(root)?;
        let (segment_first_cursor, current_path, file, bytes, segment_has_records) =
            match paths.pop() {
                Some(path) => {
                    // Refuse to append to an unverified chain or a different stream.
                    if verify_chain {
                        Self::verify(root)?;
                    }
                    let header = read_wal_header(&path)?;
                    if header.get("stream_id").and_then(|value| value.as_str()) != Some(stream_id) {
                        return Err(CoreError::WalStreamMismatch);
                    }
                    let first_cursor = header
                        .get("first_cursor")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or(CoreError::CorruptWal)?;
                    let bytes = fs::metadata(&path)?.len();
                    let header_bytes = serde_json::to_vec(&header)?.len() as u64 + 1;
                    let file = OpenOptions::new().append(true).open(&path)?;
                    (
                        Some(first_cursor),
                        Some(path),
                        Some(file),
                        bytes,
                        bytes > header_bytes,
                    )
                }
                None => (None, None, None, 0, false),
            };
        Ok(Self {
            root: root.into(),
            stream_id: stream_id.into(),
            max_segment_bytes,
            segment_first_cursor,
            current_path,
            file,
            bytes,
            segment_has_records,
        })
    }

    fn rotate(&mut self, cursor: u64) -> Result<(), CoreError> {
        self.rotate_with_checkpoint_anchor(cursor, None)?;
        Ok(())
    }

    fn rotate_with_checkpoint_anchor(
        &mut self,
        cursor: u64,
        checkpoint: Option<(u64, &str)>,
    ) -> Result<WalCheckpointAnchor, CoreError> {
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
        let mut header = serde_json::json!({"magic":"MCWAL1","wal_version":1,
            "stream_id":self.stream_id,"first_cursor":cursor,
            "previous_segment_sha256":previous_segment_sha256});
        if let Some((checkpoint_cursor, checkpoint_sha256)) = checkpoint {
            header["checkpoint_cursor"] = serde_json::json!(checkpoint_cursor);
            header["checkpoint_sha256"] = serde_json::json!(checkpoint_sha256);
        }
        let line = serde_json::to_vec(&header)?;
        let segment_header_sha256 = hex::encode(Sha256::digest(&line));
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        self.bytes = line.len() as u64 + 1;
        self.segment_first_cursor = Some(cursor);
        self.current_path = Some(path);
        self.file = Some(file);
        self.segment_has_records = false;
        Ok(WalCheckpointAnchor {
            checkpoint_cursor: checkpoint.map_or(0, |value| value.0),
            checkpoint_sha256: checkpoint.map_or_else(String::new, |value| value.1.into()),
            segment_first_cursor: cursor,
            segment_header_sha256,
        })
    }

    /// Starts the next WAL segment with a checkpoint hash in its immutable header. The anchor
    /// consumes no public cursor, so downstream market streams remain contiguous. Later segment
    /// hashes chain this header into the append-only WAL.
    pub fn anchor_checkpoint(
        &mut self,
        checkpoint_cursor: u64,
        checkpoint_sha256: &str,
    ) -> Result<WalCheckpointAnchor, CoreError> {
        if checkpoint_sha256.len() != 64
            || !checkpoint_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(CoreError::CheckpointHash);
        }
        let next_cursor = checkpoint_cursor
            .checked_add(1)
            .ok_or(CoreError::CorruptWal)?;
        if let Some(path) = &self.current_path {
            let header = read_wal_header(path)?;
            let bytes = serde_json::to_vec(&header)?;
            if header
                .get("first_cursor")
                .and_then(serde_json::Value::as_u64)
                == Some(next_cursor)
                && header
                    .get("checkpoint_cursor")
                    .and_then(serde_json::Value::as_u64)
                    == Some(checkpoint_cursor)
                && header
                    .get("checkpoint_sha256")
                    .and_then(serde_json::Value::as_str)
                    == Some(checkpoint_sha256)
                && fs::metadata(path)?.len() == u64::try_from(bytes.len() + 1).unwrap_or(u64::MAX)
            {
                return Ok(WalCheckpointAnchor {
                    checkpoint_cursor,
                    checkpoint_sha256: checkpoint_sha256.into(),
                    segment_first_cursor: next_cursor,
                    segment_header_sha256: hex::encode(Sha256::digest(bytes)),
                });
            }
        }
        self.rotate_with_checkpoint_anchor(
            next_cursor,
            Some((checkpoint_cursor, checkpoint_sha256)),
        )
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
        if self.file.is_none()
            || self.segment_has_records
                && self.bytes + line.len() as u64 + 1 > self.max_segment_bytes
        {
            self.rotate(outcome.event.cursor)?;
        }
        let file = self.file.as_mut().expect("rotated");
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        self.bytes += line.len() as u64 + 1;
        self.segment_has_records = true;
        Ok(())
    }

    pub fn verify(root: impl AsRef<Path>) -> Result<Vec<PersistedEvent>, CoreError> {
        Ok(Self::verify_with_anchors(root)?.records)
    }

    pub fn verify_with_anchors(root: impl AsRef<Path>) -> Result<VerifiedSegmentedWal, CoreError> {
        let paths = wal_paths(root.as_ref())?;
        let mut output = Vec::new();
        let mut checkpoint_anchors = Vec::new();
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
            match (
                header.get("checkpoint_cursor"),
                header.get("checkpoint_sha256"),
            ) {
                (Some(cursor), Some(checkpoint_sha256)) => {
                    let checkpoint_cursor = cursor.as_u64().ok_or(CoreError::CorruptWal)?;
                    let checkpoint_sha256 = checkpoint_sha256
                        .as_str()
                        .filter(|hash| {
                            hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
                        })
                        .ok_or(CoreError::CorruptWal)?;
                    let segment_first_cursor = header
                        .get("first_cursor")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or(CoreError::CorruptWal)?;
                    if checkpoint_cursor.checked_add(1) != Some(segment_first_cursor) {
                        return Err(CoreError::CorruptWal);
                    }
                    checkpoint_anchors.push(WalCheckpointAnchor {
                        checkpoint_cursor,
                        checkpoint_sha256: checkpoint_sha256.into(),
                        segment_first_cursor,
                        segment_header_sha256: hex::encode(Sha256::digest(serde_json::to_vec(
                            &header,
                        )?)),
                    });
                }
                (None, None) => {}
                _ => return Err(CoreError::CorruptWal),
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
        Ok(VerifiedSegmentedWal {
            records: output,
            checkpoint_anchors,
        })
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
            market: None,
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
        let frame_raw_payload = outcomes[0].event.raw_payload.as_ref();
        let raw_sha256 = &outcomes[0].event.source.raw_sha256;
        if outcomes.iter().any(|outcome| {
            outcome.event.source.raw_sha256 != *raw_sha256
                || outcome.event.raw_payload.as_ref() != frame_raw_payload
        }) {
            return Err(CoreError::CorruptWal);
        }
        let payloads = outcomes
            .iter()
            .map(WalPersistedEventV2::from)
            .collect::<Vec<_>>();
        let payload = serde_json::to_vec(&(frame_raw_payload, &payloads))?;
        let record = WalBatchRecordV2 {
            batch_version: 2,
            first_cursor: first.event.cursor,
            last_cursor: outcomes.last().expect("non-empty").event.cursor,
            crc32c: crc32c::crc32c(&payload),
            payload_sha256: hex::encode(Sha256::digest(&payload)),
            frame_raw_payload,
            payloads,
        };
        let line = serde_json::to_vec(&record)?;
        if self.file.is_none()
            || self.segment_has_records
                && self.bytes + line.len() as u64 + 1 > self.max_segment_bytes
        {
            self.rotate(first.event.cursor)?;
        }
        let file = self.file.as_mut().expect("rotated");
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        self.bytes += line.len() as u64 + 1;
        self.segment_has_records = true;
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
            || persisted.market.is_some() && replayed.market != persisted.market
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
    fn duplicate_inside_upstream_batch_is_rejected_before_the_wal_barrier() {
        #[derive(Default)]
        struct CountingLog(usize);
        impl DurableLog for CountingLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                unreachable!("batch append is required")
            }
            fn append_outcomes(&mut self, _: &[PersistedEvent]) -> Result<(), CoreError> {
                self.0 += 1;
                Ok(())
            }
        }
        let mut writer = SingleWriter::new("scope".into(), CountingLog::default());
        let original = event(
            1,
            EventKind::FullBook {
                token_id: "t".into(),
                bids: levels("0.4", "1"),
                asks: levels("0.6", "1"),
                tick_size: Price::parse_tick("0.01").unwrap(),
                tick_version: "tick-v1".into(),
            },
        );
        let duplicate = original.clone();
        assert!(matches!(
            writer.apply_batch(vec![original, duplicate]),
            Err(CoreError::DuplicateEvent(_))
        ));
        assert_eq!(writer.log.0, 0);
        assert_eq!(writer.projection().cursor, 0);
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
            market: None,
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
            instrument_facts: None,
        }
    }

    fn fee_schedule(taker_rate: &str) -> MarketFeeSchedule {
        MarketFeeSchedule {
            schedule_id: "a".repeat(64),
            revision: "b".repeat(64),
            schedule_version: "polymarket-fees-v1".into(),
            currency: "USDC".into(),
            maker_rate: Decimal::ZERO,
            taker_rate: Decimal::from_str(taker_rate).unwrap(),
            formula_id: "polymarket_probability_fee.v1".into(),
            formula: "fee = C * feeRate * p * (1 - p)".into(),
            exponent: Decimal::ONE,
            quantum: Decimal::from_str("0.00001").unwrap(),
            rounding_mode: "UNSPECIFIED".into(),
            tie_semantics: "unspecified".into(),
            calculation_status: FeeCalculationStatus::InformationalOnly,
            effective_from: "2026-01-01T00:00:00Z".parse().unwrap(),
            effective_to: None,
            observed_at: "2026-08-28T00:00:00Z".parse().unwrap(),
            provenance: vec![MarketFeeProvenance {
                source: "polymarket_docs".into(),
                source_url: "https://docs.polymarket.com/trading/fees".into(),
                revision: "fees-docs-v1".into(),
                payload_sha256: "c".repeat(64),
                observed_at: "2026-08-28T00:00:00Z".parse().unwrap(),
                field_paths: vec!["fee_structure".into(), "fee_precision".into()],
            }],
        }
    }

    #[test]
    fn fee_schedule_is_exact_auditable_and_fails_closed_on_ambiguity() {
        for rate in ["0", "0.05"] {
            let schedule = fee_schedule(rate);
            assert!(schedule.is_complete());
            let encoded = serde_json::to_value(&schedule).unwrap();
            assert_eq!(encoded["maker_rate"], "0");
            assert_eq!(encoded["taker_rate"], rate);
            assert_eq!(encoded["exponent"], "1");
            assert_eq!(encoded["quantum"], "0.00001");
            assert!(encoded["maker_rate"].is_string());
        }

        let mut invalid_interval = fee_schedule("0.05");
        invalid_interval.effective_to = Some(invalid_interval.effective_from);
        assert!(!invalid_interval.is_complete());

        let mut ambiguous_execution = fee_schedule("0.05");
        ambiguous_execution.calculation_status = FeeCalculationStatus::Executable;
        assert!(!ambiguous_execution.is_complete());

        let mut missing_provenance = fee_schedule("0");
        missing_provenance.provenance.clear();
        assert!(!missing_provenance.is_complete());
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
    fn universe_replacement_retains_removed_market_identity_for_position_monitoring() {
        struct MemoryLog;
        impl DurableLog for MemoryLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                unreachable!()
            }
            fn append_outcome(&mut self, _: &PersistedEvent) -> Result<(), CoreError> {
                Ok(())
            }
        }

        let mut writer = SingleWriter::new("scope".into(), MemoryLog);
        writer
            .apply(event(
                1,
                EventKind::CatalogSnapshot {
                    catalog_revision: "catalog-1".into(),
                    markets: vec![
                        market("m1", "condition-1", None),
                        market("m2", "condition-2", None),
                    ],
                    negative_risk_relations: Vec::new(),
                },
            ))
            .unwrap();
        for (cursor, token_id) in [(2, "m1-yes"), (3, "m1-no"), (4, "m2-yes"), (5, "m2-no")] {
            writer
                .apply(event(
                    cursor,
                    EventKind::FullBook {
                        token_id: token_id.into(),
                        bids: levels("0.4", "10"),
                        asks: levels("0.6", "10"),
                        tick_size: Price::parse_tick("0.01").unwrap(),
                        tick_version: "tick-v1".into(),
                    },
                ))
                .unwrap();
        }
        assert!(writer.projection().ready);

        let replaced = writer
            .apply(event(
                6,
                EventKind::CatalogSnapshot {
                    catalog_revision: "catalog-2".into(),
                    markets: vec![market("m2", "condition-2", None)],
                    negative_risk_relations: Vec::new(),
                },
            ))
            .unwrap();
        assert!(replaced.persisted.applied);
        assert!(replaced.projection.ready);
        assert_eq!(
            replaced.projection.active_market_ids(),
            BTreeSet::from(["m2".into()])
        );
        assert!(!replaced.projection.markets.contains_key("m1"));
        assert_eq!(
            replaced.projection.monitoring_markets["m1"].condition_id,
            "condition-1"
        );
        assert!(replaced.projection.monitoring_books.contains_key("m1-yes"));
        assert!(replaced.projection.monitoring_books.contains_key("m1-no"));
        assert_eq!(
            replaced.projection.market_health["m1"].projection_status,
            MarketProjectionStatus::TemporarilyUnavailable
        );
        assert_eq!(
            replaced.projection.market_health["m1"]
                .reason_code
                .as_deref(),
            Some("removed_from_scan_universe")
        );
    }

    #[test]
    fn active_catalog_book_becoming_one_sided_is_quarantined_until_atomic_market_recovery() {
        struct MemoryLog;
        impl DurableLog for MemoryLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                unreachable!()
            }
            fn append_outcome(&mut self, _: &PersistedEvent) -> Result<(), CoreError> {
                Ok(())
            }
        }

        let mut writer = SingleWriter::new("scope".into(), MemoryLog);
        writer
            .apply(event(
                1,
                EventKind::CatalogSnapshot {
                    catalog_revision: "catalog-1".into(),
                    markets: vec![market("m1", "condition-1", None)],
                    negative_risk_relations: Vec::new(),
                },
            ))
            .unwrap();
        for (cursor, token_id) in [(2, "m1-yes"), (3, "m1-no")] {
            writer
                .apply(event(
                    cursor,
                    EventKind::FullBook {
                        token_id: token_id.into(),
                        bids: levels("0.4", "10"),
                        asks: levels("0.6", "10"),
                        tick_size: Price::parse_tick("0.01").unwrap(),
                        tick_version: "tick-v1".into(),
                    },
                ))
                .unwrap();
        }
        assert!(writer.projection().ready);
        assert!(writer.projection().active_market_books_two_sided());

        let one_sided = writer
            .apply(event(
                4,
                EventKind::FullBook {
                    token_id: "m1-yes".into(),
                    bids: levels("0.4", "10"),
                    asks: Vec::new(),
                    tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "tick-v1".into(),
                },
            ))
            .unwrap();
        assert!(one_sided.persisted.applied);
        assert!(!one_sided.projection.ready);
        assert!(one_sided.projection.active_market_books_two_sided());
        assert_eq!(
            one_sided.persisted.market.as_ref().unwrap().transition,
            Some(MarketTransitionKind::Quarantined)
        );
        assert_eq!(
            one_sided.projection.quarantined_market_ids(),
            BTreeSet::from(["m1".to_owned()])
        );
        assert_eq!(
            one_sided.projection.fail_closed_reason.as_deref(),
            Some("no_healthy_markets")
        );

        let recovery_started = writer
            .apply(event(
                5,
                EventKind::FullBook {
                    token_id: "m1-yes".into(),
                    bids: levels("0.4", "10"),
                    asks: levels("0.6", "10"),
                    tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "tick-v1".into(),
                },
            ))
            .unwrap();
        assert_eq!(
            recovery_started
                .persisted
                .market
                .as_ref()
                .unwrap()
                .transition,
            Some(MarketTransitionKind::RecoveryStarted)
        );
        let recovered = writer
            .apply(event(
                6,
                EventKind::FullBook {
                    token_id: "m1-no".into(),
                    bids: levels("0.4", "10"),
                    asks: levels("0.6", "10"),
                    tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "tick-v1".into(),
                },
            ))
            .unwrap();
        assert!(recovered.projection.ready);
        assert_eq!(recovered.projection.fail_closed_reason, None);
        assert_eq!(
            recovered.persisted.market.as_ref().unwrap().transition,
            Some(MarketTransitionKind::Recovered)
        );
    }

    #[test]
    fn one_market_gap_does_not_stop_an_independent_healthy_market() {
        struct MemoryLog;
        impl DurableLog for MemoryLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                unreachable!()
            }
            fn append_outcome(&mut self, _: &PersistedEvent) -> Result<(), CoreError> {
                Ok(())
            }
        }

        let mut writer = SingleWriter::new("scope".into(), MemoryLog);
        writer
            .apply(event(
                1,
                EventKind::CatalogSnapshot {
                    catalog_revision: "catalog-1".into(),
                    markets: vec![
                        market("m1", "condition-1", None),
                        market("m2", "condition-2", None),
                    ],
                    negative_risk_relations: Vec::new(),
                },
            ))
            .unwrap();
        for (cursor, token_id) in [(2, "m1-yes"), (3, "m1-no"), (4, "m2-yes"), (5, "m2-no")] {
            writer
                .apply(event(
                    cursor,
                    EventKind::FullBook {
                        token_id: token_id.into(),
                        bids: levels("0.4", "10"),
                        asks: levels("0.6", "10"),
                        tick_size: Price::parse_tick("0.01").unwrap(),
                        tick_version: "tick-v1".into(),
                    },
                ))
                .unwrap();
        }
        assert!(writer.projection().ready);

        let quarantined = writer
            .apply(event(
                6,
                EventKind::SourceGap {
                    token_id: "m1-yes".into(),
                    reason: "injected_single_market_loss".into(),
                },
            ))
            .unwrap();
        assert!(!quarantined.persisted.applied);
        assert_eq!(quarantined.persisted.fail_closed_reason, None);
        assert!(quarantined.projection.unresolved_gaps.is_empty());
        assert!(quarantined.projection.ready);
        assert_eq!(
            quarantined.projection.active_market_ids(),
            BTreeSet::from(["m2".to_owned()])
        );
        assert_eq!(
            quarantined.persisted.market.as_ref().unwrap().transition,
            Some(MarketTransitionKind::Quarantined)
        );

        for (cursor, token_id) in [(7, "m1-yes"), (8, "m1-no")] {
            writer
                .apply(event(
                    cursor,
                    EventKind::FullBook {
                        token_id: token_id.into(),
                        bids: levels("0.4", "10"),
                        asks: levels("0.6", "10"),
                        tick_size: Price::parse_tick("0.01").unwrap(),
                        tick_version: "tick-v2".into(),
                    },
                ))
                .unwrap();
        }
        let recovered = writer.projection();
        assert!(recovered.ready);
        assert_eq!(
            recovered.active_market_ids(),
            BTreeSet::from(["m1".to_owned(), "m2".to_owned()])
        );
        assert!(recovered.quarantined_market_ids().is_empty());
    }

    #[test]
    fn unknown_gap_remains_a_global_fail_closed_boundary() {
        struct MemoryLog;
        impl DurableLog for MemoryLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                Ok(())
            }
        }
        let mut writer = SingleWriter::new("scope".into(), MemoryLog);
        writer
            .apply(event(
                1,
                EventKind::FullBook {
                    token_id: "known".into(),
                    bids: levels("0.4", "1"),
                    asks: levels("0.6", "1"),
                    tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "tick-v1".into(),
                },
            ))
            .unwrap();
        let failed = writer
            .apply(event(
                2,
                EventKind::SourceGap {
                    token_id: "unknown".into(),
                    reason: "unattributed".into(),
                },
            ))
            .unwrap();
        assert!(!failed.projection.ready);
        assert_eq!(
            failed.persisted.fail_closed_reason.as_deref(),
            Some("source_gap:unattributed")
        );
        assert!(failed.persisted.market.is_none());
    }

    #[test]
    fn connection_wide_gap_is_global_even_when_record_names_a_known_token() {
        struct MemoryLog;
        impl DurableLog for MemoryLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                unreachable!()
            }
            fn append_outcome(&mut self, _: &PersistedEvent) -> Result<(), CoreError> {
                Ok(())
            }
        }
        let mut writer = SingleWriter::new("scope".into(), MemoryLog);
        writer
            .apply(event(
                1,
                EventKind::CatalogSnapshot {
                    catalog_revision: "catalog-1".into(),
                    markets: vec![market("m1", "condition-1", None)],
                    negative_risk_relations: Vec::new(),
                },
            ))
            .unwrap();
        for (cursor, token_id) in [(2, "m1-yes"), (3, "m1-no")] {
            writer
                .apply(event(
                    cursor,
                    EventKind::FullBook {
                        token_id: token_id.into(),
                        bids: levels("0.4", "10"),
                        asks: levels("0.6", "10"),
                        tick_size: Price::parse_tick("0.01").unwrap(),
                        tick_version: "tick-v1".into(),
                    },
                ))
                .unwrap();
        }
        assert!(writer.projection().ready);

        let failed = writer
            .apply(event(
                4,
                EventKind::SourceGap {
                    token_id: "m1-yes".into(),
                    reason: "upstream_connection_boundary".into(),
                },
            ))
            .unwrap();
        assert!(!failed.projection.ready);
        assert!(failed.persisted.market.is_none());
        assert_eq!(
            failed.persisted.fail_closed_reason.as_deref(),
            Some("source_gap:upstream_connection_boundary")
        );
        assert!(failed.projection.quarantined_market_ids().is_empty());
    }

    #[test]
    fn market_instrument_tick_tracks_both_token_books_or_fails_closed() {
        #[derive(Default)]
        struct TickLog(Vec<PersistedEvent>);
        impl DurableLog for TickLog {
            fn append(&mut self, _: &CanonicalEvent) -> Result<(), CoreError> {
                unreachable!()
            }
            fn append_outcome(&mut self, event: &PersistedEvent) -> Result<(), CoreError> {
                self.0.push(event.clone());
                Ok(())
            }
        }
        let mut writer = SingleWriter::new("scope".into(), TickLog::default());
        let mut record = market("m1", "condition-1", None);
        record.instrument_facts = Some(MarketInstrumentFacts {
            price_increment: Price::parse_tick("0.01").unwrap(),
            size_increment: Quantity::parse_positive("0.01").unwrap(),
            minimum_order_size: Quantity::parse_positive("5").unwrap(),
            settlement_currency: "pUSD".into(),
            start_at: Utc::now(),
            end_at: Utc::now() + chrono::Duration::days(1),
            revision: "catalog-facts-v1".into(),
            fee_schedule: None,
        });
        writer
            .apply(event(
                1,
                EventKind::CatalogSnapshot {
                    catalog_revision: "catalog-1".into(),
                    markets: vec![record],
                    negative_risk_relations: Vec::new(),
                },
            ))
            .unwrap();

        let yes = writer
            .apply(event(
                2,
                EventKind::FullBook {
                    token_id: "m1-yes".into(),
                    bids: levels("0.4", "1"),
                    asks: levels("0.6", "1"),
                    tick_size: Price::parse_tick("0.001").unwrap(),
                    tick_version: "yes-tick-001".into(),
                },
            ))
            .unwrap();
        assert!(!yes.projection.ready);
        assert_eq!(
            yes.projection.fail_closed_reason.as_deref(),
            Some("no_healthy_markets")
        );
        assert_eq!(
            yes.projection.markets["m1"]
                .instrument_facts
                .as_ref()
                .unwrap()
                .price_increment,
            Price::parse_tick("0.01").unwrap()
        );

        let no = writer
            .apply(event(
                3,
                EventKind::FullBook {
                    token_id: "m1-no".into(),
                    bids: levels("0.4", "1"),
                    asks: levels("0.6", "1"),
                    tick_size: Price::parse_tick("0.001").unwrap(),
                    tick_version: "no-tick-001".into(),
                },
            ))
            .unwrap();
        assert!(no.projection.ready);
        let reconciled = no.projection.markets["m1"]
            .instrument_facts
            .as_ref()
            .unwrap();
        assert_eq!(
            reconciled.price_increment,
            Price::parse_tick("0.001").unwrap()
        );
        assert_eq!(reconciled.revision.len(), 64);
        assert_ne!(reconciled.revision, "catalog-facts-v1");
        let stable_revision = reconciled.revision.clone();

        let unchanged_tick = writer
            .apply(event(
                4,
                EventKind::Delta {
                    token_id: "m1-yes".into(),
                    side: Side::Bid,
                    levels: levels("0.4", "2"),
                },
            ))
            .unwrap();
        assert!(unchanged_tick.projection.ready);
        assert_eq!(
            unchanged_tick.projection.markets["m1"]
                .instrument_facts
                .as_ref()
                .unwrap()
                .revision,
            stable_revision
        );

        let one_token_changed = writer
            .apply(event(
                5,
                EventKind::TickSizeChange {
                    token_id: "m1-yes".into(),
                    old_tick_size: Some(Price::parse_tick("0.001").unwrap()),
                    new_tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "yes-tick-01".into(),
                },
            ))
            .unwrap();
        assert!(one_token_changed.persisted.applied);
        assert!(!one_token_changed.projection.ready);
        assert_eq!(
            one_token_changed.projection.fail_closed_reason.as_deref(),
            Some("instrument_tick_inconsistent")
        );
        assert_eq!(
            one_token_changed.projection.markets["m1"]
                .instrument_facts
                .as_ref()
                .unwrap()
                .price_increment,
            Price::parse_tick("0.001").unwrap()
        );

        let both_tokens_changed = writer
            .apply(event(
                6,
                EventKind::TickSizeChange {
                    token_id: "m1-no".into(),
                    old_tick_size: Some(Price::parse_tick("0.001").unwrap()),
                    new_tick_size: Price::parse_tick("0.01").unwrap(),
                    tick_version: "no-tick-01".into(),
                },
            ))
            .unwrap();
        assert!(both_tokens_changed.projection.ready);
        let updated = both_tokens_changed.projection.markets["m1"]
            .instrument_facts
            .as_ref()
            .unwrap();
        assert_eq!(updated.price_increment, Price::parse_tick("0.01").unwrap());
        assert_ne!(updated.revision, stable_revision);
        assert!(both_tokens_changed.projection.instrument_ticks_consistent());

        let mut legacy_checkpoint = (*both_tokens_changed.projection).clone();
        legacy_checkpoint
            .markets
            .get_mut("m1")
            .unwrap()
            .instrument_facts = Some(MarketInstrumentFacts {
            price_increment: Price::parse_tick("0.001").unwrap(),
            revision: "stale-checkpoint-revision".into(),
            ..updated.clone()
        });
        assert!(legacy_checkpoint.ready);
        assert!(!legacy_checkpoint.instrument_ticks_consistent());
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
