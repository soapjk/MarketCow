//! Strict Polymarket CLOB raw-frame normalizer. Transport and cursor ownership stay outside this
//! crate; a frame is normalized without network, storage, or async-runtime dependencies.

use chrono::{DateTime, Utc};
use marketcow_core::{
    CONTRACT_VERSION, CanonicalEvent, EventKind, Level, Price, Side, SideLevels, SourceEvidence,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use thiserror::Error;

pub const NORMALIZER_VERSION: &str = "marketcow.polymarket.normalizer.v1";
pub const CLOB_MARKET_STREAM_SOURCE_URL: &str =
    "https://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Debug, Clone)]
pub struct NormalizerConfig {
    pub scope_id: String,
    pub config_revision: String,
    pub maximum_source_delay_ms: i64,
}

impl NormalizerConfig {
    pub fn new(scope_id: impl Into<String>, config_revision: impl Into<String>) -> Self {
        Self {
            scope_id: scope_id.into(),
            config_revision: config_revision.into(),
            maximum_source_delay_ms: 5_000,
        }
    }

    fn validate(&self) -> Result<(), NormalizeError> {
        if self.scope_id.is_empty()
            || self.config_revision.is_empty()
            || self.maximum_source_delay_ms < 0
        {
            return Err(NormalizeError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum NormalizeError {
    #[error("normalizer config is incomplete")]
    InvalidConfig,
    #[error("raw frame must be an object")]
    FrameNotObject,
    #[error("unsupported event type: {0}")]
    UnsupportedEventType(String),
    #[error("required field is missing: {0}")]
    MissingField(&'static str),
    #[error("timestamp is invalid")]
    InvalidTimestamp,
    #[error("side must be BUY/BID or SELL/ASK")]
    InvalidSide,
    #[error("book levels are invalid")]
    InvalidLevels,
    #[error("financial decimal is invalid")]
    InvalidDecimal,
}

struct FrameContext {
    raw_payload: Value,
    raw_sha256: String,
    received_at: DateTime<Utc>,
    observed_at: DateTime<Utc>,
}

pub fn normalize_frame(
    config: &NormalizerConfig,
    raw_payload: Value,
    received_at: DateTime<Utc>,
    first_cursor: u64,
) -> Result<Vec<CanonicalEvent>, NormalizeError> {
    config.validate()?;
    let raw = raw_payload
        .as_object()
        .ok_or(NormalizeError::FrameNotObject)?;
    let event_type = string_alias(raw, &["event_type", "type"])
        .ok_or(NormalizeError::MissingField("event_type"))?;
    let observed_at = parse_observed_at(raw.get("timestamp"), received_at)?;
    let raw_sha256 = content_sha256(&raw_payload)?;
    let context = FrameContext {
        raw_payload: raw_payload.clone(),
        raw_sha256,
        received_at,
        observed_at,
    };
    match event_type.as_str() {
        "book" => {
            let token_id = string_alias(raw, &["asset_id", "token_id"])
                .filter(|value| !value.is_empty())
                .ok_or(NormalizeError::MissingField("asset_id"))?;
            let tick_text = value_text(raw.get("tick_size"))
                .ok_or(NormalizeError::MissingField("tick_size"))?;
            let tick_size =
                Price::parse_tick(&tick_text).map_err(|_| NormalizeError::InvalidDecimal)?;
            let bids = parse_levels(raw.get("bids"))?;
            let asks = parse_levels(raw.get("asks"))?;
            let tick_version =
                content_sha256(&serde_json::json!({"tick_size": tick_size.0.to_string()}))?;
            Ok(vec![build_event(
                config,
                &context,
                first_cursor,
                &token_id,
                EventKind::FullBook {
                    token_id: token_id.clone(),
                    bids,
                    asks,
                    tick_size,
                    tick_version,
                },
            )])
        }
        "price_change" => {
            let changes = raw
                .get("price_changes")
                .or_else(|| raw.get("changes"))
                .and_then(Value::as_array)
                .ok_or(NormalizeError::MissingField("price_changes"))?;
            if changes.is_empty() {
                return Err(NormalizeError::InvalidLevels);
            }
            let mut by_token: BTreeMap<String, Vec<SideLevels>> = BTreeMap::new();
            for change in changes {
                let item = change.as_object().ok_or(NormalizeError::InvalidLevels)?;
                let token_id = string_alias(item, &["asset_id", "token_id"])
                    .filter(|value| !value.is_empty())
                    .ok_or(NormalizeError::MissingField("asset_id"))?;
                let side = match string_alias(item, &["side"])
                    .unwrap_or_default()
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "BUY" | "BID" => Side::Bid,
                    "SELL" | "ASK" => Side::Ask,
                    _ => return Err(NormalizeError::InvalidSide),
                };
                let price =
                    value_text(item.get("price")).ok_or(NormalizeError::MissingField("price"))?;
                let size =
                    value_text(item.get("size")).ok_or(NormalizeError::MissingField("size"))?;
                let level =
                    Level::new(&price, &size).map_err(|_| NormalizeError::InvalidDecimal)?;
                by_token.entry(token_id).or_default().push(SideLevels {
                    side,
                    levels: vec![level],
                });
            }
            by_token
                .into_iter()
                .enumerate()
                .map(|(offset, (token_id, changes))| {
                    let cursor = first_cursor
                        .checked_add(offset as u64)
                        .ok_or(NormalizeError::InvalidConfig)?;
                    Ok(build_event(
                        config,
                        &context,
                        cursor,
                        &token_id,
                        EventKind::AtomicDelta {
                            token_id: token_id.clone(),
                            changes,
                        },
                    ))
                })
                .collect()
        }
        other => Err(NormalizeError::UnsupportedEventType(other.into())),
    }
}

fn build_event(
    config: &NormalizerConfig,
    context: &FrameContext,
    cursor: u64,
    token_id: &str,
    kind: EventKind,
) -> CanonicalEvent {
    let event_id = hex::encode(Sha256::digest(format!(
        "{}\0{}\0{}\0{}",
        config.scope_id, NORMALIZER_VERSION, context.raw_sha256, token_id
    )));
    let delay_ms = context
        .received_at
        .signed_duration_since(context.observed_at)
        .num_milliseconds();
    CanonicalEvent {
        schema_version: CONTRACT_VERSION.into(),
        cursor,
        event_id,
        scope_id: config.scope_id.clone(),
        received_at: context.received_at,
        source_observed_at: context.observed_at,
        normalizer_version: NORMALIZER_VERSION.into(),
        config_revision: config.config_revision.clone(),
        source: SourceEvidence {
            source: "polymarket_clob_ws".into(),
            source_url: Some(CLOB_MARKET_STREAM_SOURCE_URL.into()),
            requested_at: context.received_at,
            responded_at: context.received_at,
            observed_at: context.observed_at,
            raw_sha256: context.raw_sha256.clone(),
            update_frequency: "realtime".into(),
            revision: context.raw_sha256.clone(),
            missing: false,
            delayed: delay_ms > config.maximum_source_delay_ms,
            duplicate: false,
            revised: false,
        },
        raw_payload: context.raw_payload.clone(),
        kind,
    }
}

fn parse_levels(value: Option<&Value>) -> Result<Vec<Level>, NormalizeError> {
    value
        .and_then(Value::as_array)
        .ok_or(NormalizeError::InvalidLevels)?
        .iter()
        .map(|value| {
            let item = value.as_object().ok_or(NormalizeError::InvalidLevels)?;
            let price = value_text(item.get("price")).ok_or(NormalizeError::InvalidLevels)?;
            let size = value_text(item.get("size")).ok_or(NormalizeError::InvalidLevels)?;
            Level::new(&price, &size).map_err(|_| NormalizeError::InvalidDecimal)
        })
        .collect()
}

fn parse_observed_at(
    value: Option<&Value>,
    fallback: DateTime<Utc>,
) -> Result<DateTime<Utc>, NormalizeError> {
    let Some(value) = value else {
        return Ok(fallback);
    };
    if let Some(milliseconds) = value.as_i64().or_else(|| value.as_str()?.parse().ok()) {
        return DateTime::from_timestamp_millis(milliseconds)
            .ok_or(NormalizeError::InvalidTimestamp);
    }
    value
        .as_str()
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|value| value.with_timezone(&Utc))
        .ok_or(NormalizeError::InvalidTimestamp)
}

fn string_alias(map: &serde_json::Map<String, Value>, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| value_text(map.get(*name)))
}

fn value_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn content_sha256(value: &Value) -> Result<String, NormalizeError> {
    serde_json::to_vec(value)
        .map(|bytes| hex::encode(Sha256::digest(bytes)))
        .map_err(|_| NormalizeError::FrameNotObject)
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketcow_core::{DurableLog, PersistedEvent, SingleWriter};

    struct MemoryLog(Vec<PersistedEvent>);
    impl DurableLog for MemoryLog {
        fn append(&mut self, _: &CanonicalEvent) -> Result<(), marketcow_core::CoreError> {
            unreachable!("append_outcome is authoritative")
        }

        fn append_outcome(
            &mut self,
            value: &PersistedEvent,
        ) -> Result<(), marketcow_core::CoreError> {
            self.0.push(value.clone());
            Ok(())
        }
    }

    fn at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-08-03T04:00:03Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn config() -> NormalizerConfig {
        NormalizerConfig::new("scope", "config-v1")
    }

    fn snapshot(cursor: u64) -> CanonicalEvent {
        normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"book", "asset_id":"yes-1", "timestamp":"2026-08-03T04:00:00Z",
                "tick_size":"0.01", "bids":[{"price":"0.40","size":"10"}],
                "asks":[{"price":"0.42","size":"11"}], "hash":"source-hash"
            }),
            at(),
            cursor,
        )
        .unwrap()
        .remove(0)
    }

    #[test]
    fn snapshot_is_typed_exact_and_raw_evidence_is_verified_by_writer() {
        let normalized = snapshot(1);
        assert_eq!(normalized.source.raw_sha256.len(), 64);
        assert!(!normalized.source.delayed);
        let EventKind::FullBook {
            tick_size,
            tick_version,
            ..
        } = &normalized.kind
        else {
            panic!("expected full book")
        };
        assert_eq!(tick_size.0.to_string(), "0.01");
        assert_eq!(tick_version.len(), 64);
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        assert!(writer.apply(normalized).unwrap().persisted.applied);
        assert!(writer.projection().ready);
    }

    #[test]
    fn same_token_multi_side_message_is_one_atomic_delta() {
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        writer.apply(snapshot(1)).unwrap();
        let raw = serde_json::json!({
            "event_type":"price_change", "timestamp":"2026-08-03T04:00:01Z",
            "price_changes":[
                {"asset_id":"yes-1","side":"BUY","price":"0.43","size":"1"},
                {"asset_id":"yes-1","side":"SELL","price":"0.42","size":"0"},
                {"asset_id":"yes-1","side":"SELL","price":"0.44","size":"2"}
            ]
        });
        let events = normalize_frame(&config(), raw, at(), 2).unwrap();
        assert_eq!(events.len(), 1);
        let outcome = writer.apply(events.into_iter().next().unwrap()).unwrap();
        assert!(outcome.persisted.applied);
        let book = &outcome.projection.books["yes-1"];
        assert_eq!(book.bids.last_key_value().unwrap().0.0.to_string(), "0.43");
        assert_eq!(book.asks.first_key_value().unwrap().0.0.to_string(), "0.44");
    }

    #[test]
    fn multi_token_message_has_deterministic_token_order_and_contiguous_cursors() {
        let raw = serde_json::json!({
            "event_type":"price_change", "timestamp":"2026-08-03T04:00:01Z",
            "price_changes":[
                {"asset_id":"z-token","side":"BUY","price":"0.40","size":"1"},
                {"asset_id":"a-token","side":"SELL","price":"0.60","size":"1"}
            ]
        });
        let events = normalize_frame(&config(), raw, at(), 9).unwrap();
        assert_eq!(
            events.iter().map(|event| event.cursor).collect::<Vec<_>>(),
            [9, 10]
        );
        let token_ids = events
            .iter()
            .map(|event| match &event.kind {
                EventKind::AtomicDelta { token_id, .. } => token_id.as_str(),
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        assert_eq!(token_ids, ["a-token", "z-token"]);
    }

    #[test]
    fn invalid_or_unsupported_frames_fail_before_cursor_assignment() {
        assert_eq!(
            normalize_frame(
                &config(),
                serde_json::json!({"event_type":"price_change","price_changes":[]}),
                at(),
                1,
            ),
            Err(NormalizeError::InvalidLevels)
        );
        assert_eq!(
            normalize_frame(
                &config(),
                serde_json::json!({"event_type":"last_trade_price"}),
                at(),
                1,
            ),
            Err(NormalizeError::UnsupportedEventType(
                "last_trade_price".into()
            ))
        );
    }

    #[test]
    fn raw_duplicate_has_stable_identity_independent_of_reserved_cursor() {
        let one = snapshot(1);
        let two = snapshot(2);
        assert_eq!(one.event_id, two.event_id);
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        writer.apply(one).unwrap();
        assert!(matches!(
            writer.apply(two),
            Err(marketcow_core::CoreError::DuplicateEvent(_))
        ));
        assert_eq!(writer.projection().cursor, 1);
    }

    #[test]
    fn delayed_source_is_durably_rejected_and_opens_recovery_gap() {
        let mut strict = config();
        strict.maximum_source_delay_ms = 1_000;
        let normalized = normalize_frame(
            &strict,
            serde_json::json!({
                "event_type":"book", "asset_id":"yes-1", "timestamp":"2026-08-03T04:00:00Z",
                "tick_size":"0.01", "bids":[{"price":"0.40","size":"10"}],
                "asks":[{"price":"0.42","size":"11"}]
            }),
            at(),
            1,
        )
        .unwrap()
        .remove(0);
        assert!(normalized.source.delayed);
        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        let outcome = writer.apply(normalized).unwrap();
        assert!(!outcome.persisted.applied);
        assert_eq!(
            outcome.persisted.fail_closed_reason.as_deref(),
            Some("source_data_delayed")
        );
        assert_eq!(outcome.projection.cursor, 1);
        assert_eq!(outcome.projection.persisted_cursor, 1);
        assert!(!outcome.projection.ready);
        assert!(outcome.projection.unresolved_gaps.contains("yes-1"));
    }

    #[test]
    fn zero_tick_and_off_grid_price_are_rejected() {
        let zero_tick = serde_json::json!({
            "event_type":"book", "asset_id":"yes-1", "tick_size":"0",
            "bids":[], "asks":[]
        });
        assert_eq!(
            normalize_frame(&config(), zero_tick, at(), 1),
            Err(NormalizeError::InvalidDecimal)
        );

        let mut writer = SingleWriter::new("scope".into(), MemoryLog(Vec::new()));
        let off_grid = normalize_frame(
            &config(),
            serde_json::json!({
                "event_type":"book", "asset_id":"yes-1", "tick_size":"0.01",
                "bids":[{"price":"0.405","size":"1"}],
                "asks":[{"price":"0.60","size":"1"}]
            }),
            at(),
            1,
        )
        .unwrap()
        .remove(0);
        let outcome = writer.apply(off_grid).unwrap();
        assert!(!outcome.persisted.applied);
        assert_eq!(
            outcome.persisted.fail_closed_reason.as_deref(),
            Some("invalid_tick_full_book")
        );
    }
}
