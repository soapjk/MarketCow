//! Pure read-model builders for the Rust public API. No socket, runtime, or database dependency.

use marketcow_contracts::{EventContractFields, LIVE_SCHEMA_VERSION};
use marketcow_core::{Book, EventKind, Level, PersistedEvent, Projection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const MAX_EVENT_PAGE: usize = 1_000;
pub const STREAM_PROTOCOL_VERSION: &str = "marketcow.market-stream.v1";

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
    pub markets: Vec<marketcow_core::MarketRecord>,
    pub negative_risk_relations: Vec<marketcow_core::NegativeRiskRelation>,
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
    ResyncRequired {
        reason: String,
        retryable: bool,
    },
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

pub fn stream_resync_required(
    scope_id: &str,
    cursor: u64,
    reason: impl Into<String>,
) -> StreamFrame {
    StreamFrame {
        protocol_version: STREAM_PROTOCOL_VERSION.into(),
        scope_id: scope_id.into(),
        cursor,
        payload: StreamPayload::ResyncRequired {
            reason: reason.into(),
            retryable: true,
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
            .map(|(token_id, book)| book_view(token_id, book))
            .collect(),
        catalog_revision: projection.catalog_revision.clone(),
        markets: projection.markets.values().cloned().collect(),
        negative_risk_relations: projection
            .negative_risk_relations
            .values()
            .cloned()
            .collect(),
    }
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
    let canonical_payload =
        serde_json::to_value(&record.event.kind).map_err(|_| ReadApiError::Serialization)?;
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
        event_type: match &record.event.kind {
            EventKind::CatalogSnapshot { .. } => "catalog_revision",
            EventKind::NewMarket { .. } => "new_market",
            EventKind::MarketResolved { .. } if record.applied => "market_terminal",
            EventKind::MarketResolved { .. } => "market_resolved",
            EventKind::FullBook { .. } => "full_book",
            EventKind::Delta { .. } => "delta",
            EventKind::AtomicDelta { .. } => "delta",
            EventKind::BestBidAsk { .. } => "best_bid_ask",
            EventKind::LastTradePrice { .. } => "last_trade_price",
            EventKind::TickSizeChange { .. } => "tick_size_change",
            EventKind::SourceGap { .. } => "source_gap",
        }
        .into(),
        canonical_payload,
        canonical_payload_sha256: hex::encode(Sha256::digest(canonical_bytes)),
        raw_payload: record.event.raw_payload.clone(),
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
    FullSyncResponse {
        schema_version: LIVE_SCHEMA_VERSION.into(),
        scope_id: projection.scope_id.clone(),
        projection_generation: projection.generation,
        boundary_cursor: projection.cursor,
        health: FullSyncHealth {
            ready: projection.ready,
            book_count: projection.books.len(),
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
            raw_payload,
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
            StreamPayload::ResyncRequired {
                retryable: true,
                ..
            }
        ));
    }
}
