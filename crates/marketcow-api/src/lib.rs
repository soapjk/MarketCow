//! Pure read-model builders for the Rust public API. No socket, runtime, or database dependency.

use marketcow_contracts::{EventContractFields, LIVE_SCHEMA_VERSION};
use marketcow_core::{
    Book, EventKind, Level, MarketEventMetadata, MarketProjectionHealth, MarketTransitionKind,
    PersistedEvent, Projection,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::sync::Arc;
use thiserror::Error;

pub const MAX_EVENT_PAGE: usize = 1_000;
pub const STREAM_PROTOCOL_VERSION: &str = "marketcow.market-stream.v4";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorWatermarks {
    pub published_cursor: u64,
    pub persisted_cursor: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookView {
    pub token_id: String,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub tick_size: Option<marketcow_core::Price>,
    pub tick_version: String,
    pub source_observed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_trade_price: Option<marketcow_core::Price>,
    pub last_trade_observed_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Machine-readable token mask entry for a token-local projection fault.  The containing market
/// remains present in `configured_markets` so downstream position monitoring never loses stable
/// identities, while new-opportunity scanners intersect configured tokens with
/// `available_token_ids`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnavailableToken {
    pub token_id: String,
    pub market_id: String,
    pub availability_status: String,
    pub reason_code: String,
    pub retryable: bool,
    pub retry_after: Option<chrono::DateTime<chrono::Utc>>,
    pub unavailable_since: Option<chrono::DateTime<chrono::Utc>>,
    pub source_observed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_recovered_at: Option<chrono::DateTime<chrono::Utc>>,
    pub projection_generation: u64,
    pub catalog_revision: Option<String>,
    pub availability_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotResponse {
    pub schema_version: String,
    pub scope_id: String,
    pub generation: u64,
    pub watermarks: CursorWatermarks,
    pub status: String,
    pub fail_closed_reason: Option<String>,
    pub unresolved_gaps: Vec<String>,
    pub books: Vec<BookView>,
    pub catalog_revision: Option<String>,
    /// Tradable subset. Every outcome token has passed the availability mask.
    pub markets: Vec<marketcow_core::MarketRecord>,
    pub negative_risk_relations: Vec<marketcow_core::NegativeRiskRelation>,
    /// Full configured identity/facts set at this same atomic boundary, including quarantined
    /// markets.  Books for unavailable tokens are intentionally not published as tradable.
    pub configured_markets: Vec<marketcow_core::MarketRecord>,
    pub configured_negative_risk_relations: Vec<marketcow_core::NegativeRiskRelation>,
    pub configured_market_ids: Vec<String>,
    pub configured_token_ids: Vec<String>,
    pub available_token_ids: Vec<String>,
    pub unavailable_token_ids: Vec<String>,
    pub unavailable_tokens: Vec<UnavailableToken>,
    pub availability_mask_semantics: String,
    pub new_opportunity_requires_all_market_tokens_available: bool,
    pub tradable_market_ids: Vec<String>,
    pub tradable_token_ids: Vec<String>,
    pub active_market_ids: Vec<String>,
    pub quarantined_market_ids: Vec<String>,
    pub market_health: Vec<MarketProjectionHealth>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSnapshotResponse {
    pub schema_version: String,
    pub scope_id: String,
    pub projection_generation: u64,
    pub boundary_cursor: u64,
    pub catalog_revision: String,
    pub market_sequence_boundary: u64,
    pub market: marketcow_core::MarketRecord,
    pub books: Vec<BookView>,
    pub negative_risk_relation: Option<marketcow_core::NegativeRiskRelation>,
    pub health: MarketProjectionHealth,
    pub atomic: bool,
    pub scan_universe_membership: bool,
    pub usable_for_new_opportunities: bool,
    pub stable_identity_for_position_monitoring: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventsResponse {
    pub schema_version: String,
    pub after_cursor: u64,
    pub next_cursor: u64,
    pub has_more: bool,
    pub events: Vec<EventContractFields>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointResponse {
    pub schema_version: String,
    pub scope_id: String,
    pub checkpoint_cursor: u64,
    pub persisted_cursor: u64,
    pub state_sha256: String,
    pub unresolved_gaps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullSyncHealth {
    pub ready: bool,
    pub book_count: usize,
    pub active_market_count: usize,
    pub quarantined_market_count: usize,
    pub unresolved_gap_count: usize,
    pub fail_closed_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullSyncResponse {
    pub schema_version: String,
    pub scope_id: String,
    pub projection_generation: u64,
    pub boundary_cursor: u64,
    pub health: FullSyncHealth,
    pub snapshot: SnapshotResponse,
    pub checkpoint: CheckpointResponse,
}

/// Versioned downstream WebSocket contract. Every frame carries the authoritative event cursor;
/// clients must full-sync again after `resync_required` instead of guessing across a gap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamFrame {
    pub protocol_version: String,
    pub scope_id: String,
    pub cursor: u64,
    #[serde(flatten)]
    pub payload: StreamPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamPayload {
    Subscription {
        mode: String,
        boundary_cursor: u64,
        full_sync: Option<Box<FullSyncResponse>>,
    },
    Event {
        event: EventContractFields,
    },
    GlobalResyncRequired {
        reason: String,
        retryable: bool,
        full_sync_required: bool,
    },
    UniverseChanged {
        universe_id: String,
        old_generation: u64,
        new_generation: u64,
        added_markets: Vec<String>,
        removed_markets: Vec<String>,
        switch_boundary_cursor: u64,
        full_sync_required: bool,
    },
    MarketQuarantined {
        #[serde(flatten)]
        control: MarketControlFields,
    },
    MarketRecoveryStarted {
        #[serde(flatten)]
        control: MarketControlFields,
    },
    MarketRecovered {
        #[serde(flatten)]
        control: MarketControlFields,
    },
    MarketReplaced {
        #[serde(flatten)]
        control: MarketControlFields,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketControlFields {
    pub global_cursor: u64,
    pub market_id: String,
    pub market_sequence: u64,
    pub projection_generation: u64,
    pub catalog_revision: String,
    pub event_revision: String,
    pub old_generation: u64,
    pub new_generation: u64,
    pub affected_market_ids: Vec<String>,
    pub affected_token_ids: Vec<String>,
    pub added_market_ids: Vec<String>,
    pub removed_market_ids: Vec<String>,
    pub replacement_market_id: Option<String>,
    pub reason_code: Option<String>,
    pub recovery_boundary: u64,
    pub full_sync_required: bool,
    pub market_snapshot_required: bool,
}

pub fn stream_universe_changed(
    scope_id: &str,
    cursor: u64,
    universe_id: impl Into<String>,
    old_generation: u64,
    new_generation: u64,
    added_markets: Vec<String>,
    removed_markets: Vec<String>,
) -> StreamFrame {
    StreamFrame {
        protocol_version: STREAM_PROTOCOL_VERSION.into(),
        scope_id: scope_id.into(),
        cursor,
        payload: StreamPayload::UniverseChanged {
            universe_id: universe_id.into(),
            old_generation,
            new_generation,
            added_markets,
            removed_markets,
            switch_boundary_cursor: cursor,
            full_sync_required: true,
        },
    }
}

pub fn stream_subscription(projection: &Projection, resumed: bool) -> StreamFrame {
    StreamFrame {
        protocol_version: STREAM_PROTOCOL_VERSION.into(),
        scope_id: projection.scope_id.clone(),
        cursor: projection.cursor,
        payload: StreamPayload::Subscription {
            mode: if resumed { "resume" } else { "full_sync" }.into(),
            boundary_cursor: projection.cursor,
            full_sync: (!resumed).then(|| Box::new(full_sync(projection))),
        },
    }
}

pub fn stream_event(scope_id: &str, record: &PersistedEvent) -> Result<StreamFrame, ReadApiError> {
    Ok(StreamFrame {
        protocol_version: STREAM_PROTOCOL_VERSION.into(),
        scope_id: scope_id.into(),
        cursor: record.event.cursor,
        payload: StreamPayload::Event {
            event: event_contract(record)?,
        },
    })
}

pub fn stream_record(scope_id: &str, record: &PersistedEvent) -> Result<StreamFrame, ReadApiError> {
    let Some(market) = record.market.as_ref() else {
        return stream_event(scope_id, record);
    };
    let Some(transition) = market.transition else {
        return stream_event(scope_id, record);
    };
    let control = market_control(record, market);
    let payload = match transition {
        MarketTransitionKind::Quarantined => StreamPayload::MarketQuarantined { control },
        MarketTransitionKind::RecoveryStarted => StreamPayload::MarketRecoveryStarted { control },
        MarketTransitionKind::Recovered => StreamPayload::MarketRecovered { control },
    };
    Ok(StreamFrame {
        protocol_version: STREAM_PROTOCOL_VERSION.into(),
        scope_id: scope_id.into(),
        cursor: record.event.cursor,
        payload,
    })
}

fn market_control(record: &PersistedEvent, market: &MarketEventMetadata) -> MarketControlFields {
    let quarantined = market.transition == Some(MarketTransitionKind::Quarantined);
    let recovered = market.transition == Some(MarketTransitionKind::Recovered);
    MarketControlFields {
        global_cursor: record.event.cursor,
        market_id: market.market_id.clone(),
        market_sequence: market.market_sequence,
        projection_generation: market.projection_generation,
        catalog_revision: market.catalog_revision.clone(),
        event_revision: market.event_revision.clone(),
        old_generation: market.projection_generation.saturating_sub(1),
        new_generation: market.projection_generation,
        affected_market_ids: vec![market.market_id.clone()],
        affected_token_ids: vec![record.event.kind.token_id().to_owned()],
        added_market_ids: recovered
            .then(|| market.market_id.clone())
            .into_iter()
            .collect(),
        removed_market_ids: quarantined
            .then(|| market.market_id.clone())
            .into_iter()
            .collect(),
        replacement_market_id: None,
        reason_code: market.reason_code.clone(),
        recovery_boundary: record.event.cursor,
        full_sync_required: false,
        market_snapshot_required: recovered,
    }
}

pub fn stream_resync_required(
    scope_id: &str,
    cursor: u64,
    reason: impl Into<String>,
) -> StreamFrame {
    StreamFrame {
        protocol_version: STREAM_PROTOCOL_VERSION.into(),
        scope_id: scope_id.into(),
        cursor,
        payload: StreamPayload::GlobalResyncRequired {
            reason: reason.into(),
            retryable: true,
            full_sync_required: true,
        },
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReadApiError {
    #[error("event page limit must be between 1 and {MAX_EVENT_PAGE}")]
    InvalidLimit,
    #[error("cursor {after_cursor} expired; earliest available cursor is {earliest_cursor}")]
    CursorExpired {
        after_cursor: u64,
        earliest_cursor: u64,
    },
    #[error("event serialization failed")]
    Serialization,
}

pub fn snapshot(projection: &Projection) -> SnapshotResponse {
    let active_market_ids = projection.active_market_ids();
    let active_token_ids = projection.active_token_ids();
    let configured_market_ids = projection.configured_market_ids();
    let configured_token_ids = projection.configured_token_ids();
    let available_token_ids = projection.available_token_ids();
    let unavailable_token_ids = projection.unavailable_token_ids();
    let quarantined_market_ids = projection.quarantined_market_ids();
    let unavailable_tokens = unavailable_tokens(projection);
    SnapshotResponse {
        schema_version: LIVE_SCHEMA_VERSION.into(),
        scope_id: projection.scope_id.clone(),
        generation: projection.generation,
        watermarks: CursorWatermarks {
            published_cursor: projection.cursor,
            persisted_cursor: projection.persisted_cursor,
        },
        status: if projection.ready { "ready" } else { "unready" }.into(),
        fail_closed_reason: projection.fail_closed_reason.clone(),
        unresolved_gaps: projection.unresolved_gaps.iter().cloned().collect(),
        books: projection
            .books
            .iter()
            .filter(|(token_id, _)| active_token_ids.contains(*token_id))
            .map(|(token_id, book)| book_view(token_id, book))
            .collect(),
        catalog_revision: projection.catalog_revision.clone(),
        markets: projection
            .markets
            .iter()
            .filter(|(market_id, _)| active_market_ids.contains(*market_id))
            .map(|(_, market)| market.clone())
            .collect(),
        negative_risk_relations: projection
            .negative_risk_relations
            .values()
            .filter(|relation| relation.member_market_ids.is_subset(&active_market_ids))
            .cloned()
            .collect(),
        configured_markets: projection
            .markets
            .iter()
            .filter(|(market_id, _)| configured_market_ids.contains(*market_id))
            .map(|(_, market)| market.clone())
            .collect(),
        configured_negative_risk_relations: projection
            .negative_risk_relations
            .values()
            .cloned()
            .collect(),
        configured_market_ids: configured_market_ids.into_iter().collect(),
        configured_token_ids: configured_token_ids.into_iter().collect(),
        available_token_ids: available_token_ids.into_iter().collect(),
        unavailable_token_ids: unavailable_token_ids.into_iter().collect(),
        unavailable_tokens,
        availability_mask_semantics: "configured_token_ids_intersect_available_token_ids".into(),
        new_opportunity_requires_all_market_tokens_available: true,
        tradable_market_ids: active_market_ids.iter().cloned().collect(),
        tradable_token_ids: active_token_ids.iter().cloned().collect(),
        active_market_ids: active_market_ids.into_iter().collect(),
        quarantined_market_ids: quarantined_market_ids.into_iter().collect(),
        market_health: projection.market_health.values().cloned().collect(),
    }
}

pub fn unavailable_tokens(projection: &Projection) -> Vec<UnavailableToken> {
    projection
        .unavailable_token_ids()
        .into_iter()
        .filter_map(|token_id| {
            let market_id = projection.market_id_for_token(&token_id)?.to_owned();
            let health = projection.market_health.get(&market_id);
            let token_health = projection.token_health.get(&token_id);
            Some(UnavailableToken {
                token_id,
                market_id,
                availability_status: "temporarily_unavailable".into(),
                reason_code: token_health
                    .map(|value| value.reason_code.clone())
                    .or_else(|| health.and_then(|value| value.reason_code.clone()))
                    .unwrap_or_else(|| "token_quarantined".into()),
                retryable: token_health.map_or_else(
                    || health.is_none_or(|value| value.retryable),
                    |value| value.retryable,
                ),
                retry_after: token_health
                    .and_then(|value| value.retry_after)
                    .or_else(|| health.and_then(|value| value.retry_after)),
                unavailable_since: token_health.map(|value| value.unavailable_since),
                source_observed_at: token_health
                    .map(|value| value.source_observed_at)
                    .or_else(|| health.and_then(|value| value.source_observed_at)),
                last_recovered_at: health.and_then(|value| value.last_recovered_at),
                projection_generation: token_health.map_or_else(
                    || health.map_or(projection.generation, |value| value.projection_generation),
                    |value| value.projection_generation,
                ),
                catalog_revision: token_health
                    .and_then(|value| value.catalog_revision.clone())
                    .or_else(|| health.and_then(|value| value.catalog_revision.clone()))
                    .or_else(|| projection.catalog_revision.clone()),
                availability_revision: token_health
                    .map(|value| value.availability_revision.clone())
                    .or_else(|| health.and_then(|value| value.last_event_revision.clone())),
            })
        })
        .collect()
}

pub fn market_snapshot(projection: &Projection, market_id: &str) -> Option<MarketSnapshotResponse> {
    let scan_universe_membership = projection.markets.contains_key(market_id);
    let market = projection
        .markets
        .get(market_id)
        .or_else(|| projection.monitoring_markets.get(market_id))?
        .clone();
    let health = projection
        .market_health
        .get(market_id)
        .cloned()
        .unwrap_or_else(|| MarketProjectionHealth {
            market_id: market_id.into(),
            projection_status: marketcow_core::MarketProjectionStatus::Ready,
            last_market_sequence: 0,
            gap_from: None,
            gap_to: None,
            reason_code: None,
            retryable: false,
            retry_after: None,
            source_observed_at: None,
            last_recovered_at: None,
            projection_generation: projection.generation,
            catalog_revision: projection.catalog_revision.clone(),
            last_event_revision: None,
        });
    let books = market
        .outcomes
        .iter()
        .filter_map(|outcome| {
            projection
                .books
                .get(&outcome.token_id)
                .or_else(|| projection.monitoring_books.get(&outcome.token_id))
                .map(|book| book_view(&outcome.token_id, book))
        })
        .collect::<Vec<_>>();
    let negative_risk_relation = market
        .negative_risk_group
        .as_ref()
        .and_then(|group_id| projection.negative_risk_relations.get(group_id))
        .cloned();
    let usable_for_new_opportunities =
        scan_universe_membership && projection.market_is_public(market_id) && books.len() == 2;
    Some(MarketSnapshotResponse {
        schema_version: "marketcow.polymarket.market-snapshot.v1".into(),
        scope_id: projection.scope_id.clone(),
        projection_generation: projection.generation,
        boundary_cursor: projection.cursor,
        catalog_revision: projection.catalog_revision.clone().unwrap_or_default(),
        market_sequence_boundary: health.last_market_sequence,
        market,
        books,
        negative_risk_relation,
        health,
        atomic: true,
        scan_universe_membership,
        usable_for_new_opportunities,
        stable_identity_for_position_monitoring: true,
    })
}

fn book_view(token_id: &str, book: &Book) -> BookView {
    BookView {
        token_id: token_id.into(),
        bids: book
            .bids
            .iter()
            .rev()
            .map(|(price, quantity)| Level {
                price: price.clone(),
                quantity: *quantity,
            })
            .collect(),
        asks: book
            .asks
            .iter()
            .map(|(price, quantity)| Level {
                price: price.clone(),
                quantity: *quantity,
            })
            .collect(),
        tick_size: book.tick_size.clone(),
        tick_version: book.tick_version.clone(),
        source_observed_at: book.source_observed_at,
        last_trade_price: book.last_trade_price.clone(),
        last_trade_observed_at: book.last_trade_observed_at,
    }
}

pub fn events_page(
    records: &[PersistedEvent],
    after_cursor: u64,
    limit: usize,
) -> Result<EventsResponse, ReadApiError> {
    if limit == 0 || limit > MAX_EVENT_PAGE {
        return Err(ReadApiError::InvalidLimit);
    }
    if let (Some(first), Some(last)) = (records.first(), records.last())
        && after_cursor < last.event.cursor
        && first.event.cursor > after_cursor.saturating_add(1)
    {
        return Err(ReadApiError::CursorExpired {
            after_cursor,
            earliest_cursor: first.event.cursor,
        });
    }
    let available: Vec<_> = records
        .iter()
        .filter(|record| record.event.cursor > after_cursor)
        .take(limit + 1)
        .collect();
    let has_more = available.len() > limit;
    let selected = &available[..available.len().min(limit)];
    let events = selected
        .iter()
        .map(|record| event_contract(record))
        .collect::<Result<Vec<_>, _>>()?;
    let next_cursor = selected
        .last()
        .map_or(after_cursor, |record| record.event.cursor);
    Ok(EventsResponse {
        schema_version: LIVE_SCHEMA_VERSION.into(),
        after_cursor,
        next_cursor,
        has_more,
        events,
    })
}

pub fn event_contract(record: &PersistedEvent) -> Result<EventContractFields, ReadApiError> {
    // Last-trade and best-bid/ask observations are durably authoritative but do not change the
    // strategy book projection. Preserve their cursor and raw evidence while exposing an explicit
    // no-op atomic delta, so strict consumers can advance without treating metadata-only traffic
    // as an instrument-facts change.
    let (canonical_payload, public_event_type) = match &record.event.kind {
        EventKind::LastTradePrice { token_id, .. } => (
            serde_json::json!({
                "event_type":"atomic_delta", "token_id":token_id, "changes":[],
                "source_event_type":"last_trade_price"
            }),
            "delta",
        ),
        EventKind::BestBidAsk { token_id, .. } => (
            serde_json::json!({
                "event_type":"atomic_delta", "token_id":token_id, "changes":[],
                "source_event_type":"best_bid_ask"
            }),
            "delta",
        ),
        EventKind::Delta {
            token_id,
            side,
            levels,
        } => (
            serde_json::json!({
                "event_type":"atomic_delta", "token_id":token_id,
                "changes":[{"side":side,"levels":levels}],
                "source_event_type":"delta"
            }),
            "delta",
        ),
        EventKind::AtomicDelta { token_id, .. }
            if record.applied && record.event.source.delayed =>
        {
            (
                serde_json::json!({
                    "event_type":"atomic_delta", "token_id":token_id, "changes":[],
                    "source_event_type":"stale_delayed_atomic_delta"
                }),
                "delta",
            )
        }
        EventKind::AtomicDelta {
            token_id, changes, ..
        } => (
            serde_json::json!({
                "event_type":"atomic_delta", "token_id":token_id,
                "changes":changes, "source_event_type":"atomic_delta"
            }),
            "delta",
        ),
        kind => (
            serde_json::to_value(kind).map_err(|_| ReadApiError::Serialization)?,
            match kind {
                EventKind::CatalogSnapshot { .. } => "catalog_revision",
                EventKind::NewMarket { .. } => "new_market",
                EventKind::MarketResolved { .. } if record.applied => "market_terminal",
                EventKind::MarketResolved { .. } => "market_resolved",
                EventKind::FullBook { .. } => "full_book",
                EventKind::TickSizeChange { .. } => "tick_size_change",
                EventKind::SourceGap { .. } => "source_gap",
                EventKind::Delta { .. }
                | EventKind::AtomicDelta { .. }
                | EventKind::BestBidAsk { .. }
                | EventKind::LastTradePrice { .. } => unreachable!(),
            },
        ),
    };
    let canonical_bytes =
        serde_json::to_vec(&canonical_payload).map_err(|_| ReadApiError::Serialization)?;
    let gaps = record
        .fail_closed_reason
        .as_ref()
        .map(|reason| vec![serde_json::json!({"reason": reason})])
        .unwrap_or_default();
    Ok(EventContractFields {
        cursor: record.event.cursor,
        event_id: record.event.event_id.clone(),
        event_revision: record.market.as_ref().map_or_else(
            || record.event.event_id.clone(),
            |market| market.event_revision.clone(),
        ),
        market_id: record
            .market
            .as_ref()
            .map(|market| market.market_id.clone()),
        market_sequence: record.market.as_ref().map(|market| market.market_sequence),
        projection_generation: record
            .market
            .as_ref()
            .map_or(record.event.cursor, |market| market.projection_generation),
        catalog_revision: record
            .market
            .as_ref()
            .map_or_else(String::new, |market| market.catalog_revision.clone()),
        event_type: public_event_type.into(),
        canonical_payload,
        canonical_payload_sha256: hex::encode(Sha256::digest(canonical_bytes)),
        raw_payload: (*record.event.raw_payload).clone(),
        raw_payload_sha256: record.event.source.raw_sha256.clone(),
        applied: record.applied,
        fail_closed_reason: record.fail_closed_reason.clone(),
        gaps,
        received_at: record.event.received_at,
    })
}

pub fn checkpoint(projection: &Projection) -> CheckpointResponse {
    CheckpointResponse {
        schema_version: LIVE_SCHEMA_VERSION.into(),
        scope_id: projection.scope_id.clone(),
        checkpoint_cursor: projection.cursor,
        persisted_cursor: projection.persisted_cursor,
        state_sha256: projection.hash(),
        unresolved_gaps: projection.unresolved_gaps.iter().cloned().collect(),
    }
}

/// Builds every recovery view from one immutable projection reference. Callers must perform
/// freshness validation before invoking this function; no storage or second state read occurs.
pub fn full_sync(projection: &Projection) -> FullSyncResponse {
    let active_market_count = projection.active_market_ids().len();
    let quarantined_market_count = projection.quarantined_market_ids().len();
    let active_book_count = projection.active_token_ids().len();
    FullSyncResponse {
        schema_version: LIVE_SCHEMA_VERSION.into(),
        scope_id: projection.scope_id.clone(),
        projection_generation: projection.generation,
        boundary_cursor: projection.cursor,
        health: FullSyncHealth {
            ready: projection.ready,
            book_count: active_book_count,
            active_market_count,
            quarantined_market_count,
            unresolved_gap_count: projection.unresolved_gaps.len(),
            fail_closed_reason: projection.fail_closed_reason.clone(),
        },
        snapshot: snapshot(projection),
        checkpoint: checkpoint(projection),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use marketcow_core::{
        CONTRACT_VERSION, CanonicalEvent, DurableLog, EventKind, Level, SingleWriter,
        SourceEvidence,
    };

    struct MemoryLog;
    impl DurableLog for MemoryLog {
        fn append(&mut self, _: &CanonicalEvent) -> Result<(), marketcow_core::CoreError> {
            Ok(())
        }
    }

    fn event(cursor: u64, kind: EventKind) -> CanonicalEvent {
        let at = Utc
            .with_ymd_and_hms(2026, 8, 28, 0, 0, cursor as u32)
            .unwrap();
        let raw_payload = serde_json::json!({"cursor": cursor});
        let raw_payload_sha256 =
            hex::encode(Sha256::digest(serde_json::to_vec(&raw_payload).unwrap()));
        CanonicalEvent {
            schema_version: CONTRACT_VERSION.into(),
            cursor,
            event_id: format!("event-{cursor}"),
            scope_id: "scope".into(),
            received_at: at,
            source_observed_at: at,
            normalizer_version: "fixture-v1".into(),
            config_revision: "config-v1".into(),
            source: SourceEvidence {
                source: "polymarket-clob".into(),
                source_url: Some("https://clob.polymarket.com".into()),
                requested_at: at,
                responded_at: at,
                observed_at: at,
                raw_sha256: raw_payload_sha256,
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

    fn book(cursor: u64) -> CanonicalEvent {
        event(
            cursor,
            EventKind::FullBook {
                token_id: "yes".into(),
                bids: vec![Level::new("0.4", "2.50").unwrap()],
                asks: vec![Level::new("0.6", "3.25").unwrap()],
                tick_size: marketcow_core::Price::parse_tick("0.01").unwrap(),
                tick_version: "tick-v1".into(),
            },
        )
    }

    #[test]
    fn snapshot_and_checkpoint_expose_both_watermarks_and_exact_decimals() {
        let mut writer = SingleWriter::new("scope".into(), MemoryLog);
        writer.apply(book(1)).unwrap();
        let projection = writer.projection();
        let response = snapshot(&projection);
        assert_eq!(response.status, "ready");
        assert_eq!(response.watermarks.published_cursor, 1);
        assert_eq!(response.watermarks.persisted_cursor, 1);
        let json = serde_json::to_value(response).unwrap();
        assert_eq!(json["books"][0]["bids"][0]["price"], "0.4");
        assert_eq!(json["books"][0]["bids"][0]["quantity"], "2.5");
        let checkpoint = checkpoint(&projection);
        assert_eq!(checkpoint.checkpoint_cursor, 1);
        assert_eq!(checkpoint.state_sha256.len(), 64);
        let full = full_sync(&projection);
        assert_eq!(full.projection_generation, full.snapshot.generation);
        assert_eq!(
            full.boundary_cursor,
            full.snapshot.watermarks.published_cursor
        );
        assert_eq!(full.boundary_cursor, full.checkpoint.checkpoint_cursor);
        assert_eq!(full.health.book_count, 1);
        assert!(full.health.ready);
    }

    #[test]
    fn event_pages_are_bounded_contiguous_and_preserve_raw_evidence() {
        let records: Vec<_> = (1..=3)
            .map(|cursor| PersistedEvent {
                event: book(cursor),
                applied: true,
                fail_closed_reason: None,
                market: None,
            })
            .collect();
        let first = events_page(&records, 0, 2).unwrap();
        assert_eq!(first.next_cursor, 2);
        assert!(first.has_more);
        assert_eq!(
            first.events[0].raw_payload,
            serde_json::json!({"cursor": 1})
        );
        assert_eq!(first.events[0].canonical_payload_sha256.len(), 64);
        let second = events_page(&records, first.next_cursor, 2).unwrap();
        assert_eq!(second.next_cursor, 3);
        assert!(!second.has_more);
        assert!(matches!(
            events_page(&records[1..], 0, 2),
            Err(ReadApiError::CursorExpired {
                after_cursor: 0,
                earliest_cursor: 2
            })
        ));
        assert_eq!(events_page(&records, 3, 2).unwrap().next_cursor, 3);
        assert_eq!(events_page(&records, 0, 0), Err(ReadApiError::InvalidLimit));
    }

    #[test]
    fn metadata_only_market_events_advance_as_explicit_noop_deltas() {
        let record = PersistedEvent {
            event: event(
                2,
                EventKind::LastTradePrice {
                    token_id: "yes".into(),
                    price: marketcow_core::Price::parse("0.41").unwrap(),
                },
            ),
            applied: true,
            fail_closed_reason: None,
            market: None,
        };
        let contract = event_contract(&record).unwrap();
        assert_eq!(contract.event_type, "delta");
        assert_eq!(contract.canonical_payload["event_type"], "atomic_delta");
        assert_eq!(contract.canonical_payload["token_id"], "yes");
        assert_eq!(contract.canonical_payload["changes"], serde_json::json!([]));
        assert_eq!(
            contract.canonical_payload["source_event_type"],
            "last_trade_price"
        );
        assert_eq!(
            contract.canonical_payload_sha256,
            hex::encode(Sha256::digest(
                serde_json::to_vec(&contract.canonical_payload).unwrap()
            ))
        );
    }

    #[test]
    fn superseded_delayed_atomic_delta_is_exposed_as_an_explicit_noop() {
        let mut event = event(
            2,
            EventKind::AtomicDelta {
                token_id: "yes".into(),
                changes: vec![],
                best_bid: None,
                best_ask: None,
            },
        );
        event.source.delayed = true;
        let contract = event_contract(&PersistedEvent {
            event,
            applied: true,
            fail_closed_reason: None,
            market: None,
        })
        .unwrap();
        assert_eq!(contract.event_type, "delta");
        assert_eq!(contract.canonical_payload["changes"], serde_json::json!([]));
        assert_eq!(
            contract.canonical_payload["source_event_type"],
            "stale_delayed_atomic_delta"
        );
    }

    #[test]
    fn rejected_event_is_returned_as_auditable_gap() {
        let record = PersistedEvent {
            event: event(
                1,
                EventKind::SourceGap {
                    token_id: "yes".into(),
                    reason: "disconnect".into(),
                },
            ),
            applied: false,
            fail_closed_reason: Some("source_gap:disconnect".into()),
            market: None,
        };
        let response = events_page(&[record], 0, 1).unwrap();
        assert!(!response.events[0].applied);
        assert_eq!(
            response.events[0].gaps[0]["reason"],
            "source_gap:disconnect"
        );
    }

    #[test]
    fn stream_contract_distinguishes_full_sync_resume_and_resync() {
        let mut projection = Projection::bootstrap("scope-1");
        projection.cursor = 7;
        projection.persisted_cursor = 7;
        let initial = stream_subscription(&projection, false);
        assert_eq!(initial.protocol_version, STREAM_PROTOCOL_VERSION);
        assert!(matches!(
            initial.payload,
            StreamPayload::Subscription {
                ref mode,
                boundary_cursor: 7,
                full_sync: Some(_),
            } if mode == "full_sync"
        ));
        let resume = stream_subscription(&projection, true);
        assert!(matches!(
            resume.payload,
            StreamPayload::Subscription {
                ref mode,
                boundary_cursor: 7,
                full_sync: None,
            } if mode == "resume"
        ));
        assert!(matches!(
            stream_resync_required("scope-1", 6, "slow_consumer").payload,
            StreamPayload::GlobalResyncRequired {
                retryable: true,
                full_sync_required: true,
                ..
            }
        ));
    }

    #[test]
    fn universe_change_is_an_explicit_atomic_resync_boundary() {
        let frame = stream_universe_changed(
            "universe-a",
            42,
            "universe-a",
            7,
            8,
            vec!["new-market".into()],
            vec!["expired-market".into()],
        );
        assert_eq!(frame.cursor, 42);
        assert!(matches!(
            frame.payload,
            StreamPayload::UniverseChanged {
                old_generation: 7,
                new_generation: 8,
                switch_boundary_cursor: 42,
                full_sync_required: true,
                ..
            }
        ));
    }

    #[test]
    fn market_fault_and_recovery_are_cursor_advancing_local_controls() {
        let record = PersistedEvent {
            event: event(
                11,
                EventKind::SourceGap {
                    token_id: "yes".into(),
                    reason: "injected".into(),
                },
            ),
            applied: false,
            fail_closed_reason: None,
            market: Some(MarketEventMetadata {
                market_id: "m1".into(),
                market_sequence: 7,
                projection_generation: 11,
                catalog_revision: "catalog-1".into(),
                event_revision: "e".repeat(64),
                projection_status: marketcow_core::MarketProjectionStatus::Quarantined,
                reason_code: Some("source_gap:injected".into()),
                transition: Some(MarketTransitionKind::Quarantined),
            }),
        };
        let frame = stream_record("scope", &record).unwrap();
        assert_eq!(frame.cursor, 11);
        assert!(matches!(
            frame.payload,
            StreamPayload::MarketQuarantined {
                control: MarketControlFields {
                    global_cursor: 11,
                    market_sequence: 7,
                    full_sync_required: false,
                    market_snapshot_required: false,
                    ref market_id,
                    ..
                }
            } if market_id == "m1"
        ));
    }

    #[test]
    fn removed_scan_market_remains_queryable_for_position_monitoring_only() {
        let mut projection = Projection::bootstrap("scope");
        projection.cursor = 12;
        projection.persisted_cursor = 12;
        projection.catalog_revision = Some("catalog-2".into());
        projection.monitoring_markets.insert(
            "m1".into(),
            marketcow_core::MarketRecord {
                market_id: "m1".into(),
                condition_id: "condition-1".into(),
                outcomes: [
                    marketcow_core::OutcomeToken {
                        token_id: "m1-yes".into(),
                        outcome: "Yes".into(),
                        instrument_id: "POLY.m1.YES".into(),
                    },
                    marketcow_core::OutcomeToken {
                        token_id: "m1-no".into(),
                        outcome: "No".into(),
                        instrument_id: "POLY.m1.NO".into(),
                    },
                ],
                negative_risk_group: None,
                lifecycle_state: marketcow_core::MarketLifecycleState::Active,
                resolution: None,
                metadata_revision: "metadata-1".into(),
                observed_at: Utc::now(),
                terminal_at: None,
                instrument_facts: None,
            },
        );
        projection
            .monitoring_books
            .insert("m1-yes".into(), Book::default());
        projection
            .monitoring_books
            .insert("m1-no".into(), Book::default());
        projection.market_health.insert(
            "m1".into(),
            MarketProjectionHealth {
                market_id: "m1".into(),
                projection_status: marketcow_core::MarketProjectionStatus::TemporarilyUnavailable,
                last_market_sequence: 9,
                gap_from: None,
                gap_to: None,
                reason_code: Some("removed_from_scan_universe".into()),
                retryable: false,
                retry_after: None,
                source_observed_at: Some(Utc::now()),
                last_recovered_at: None,
                projection_generation: 12,
                catalog_revision: Some("catalog-2".into()),
                last_event_revision: Some("event-12".into()),
            },
        );

        let response = market_snapshot(&projection, "m1").unwrap();
        assert_eq!(response.market.condition_id, "condition-1");
        assert_eq!(response.books.len(), 2);
        assert!(!response.scan_universe_membership);
        assert!(!response.usable_for_new_opportunities);
        assert!(response.stable_identity_for_position_monitoring);
    }
}
