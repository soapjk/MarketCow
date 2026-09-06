//! Strict Rust producer codec for the existing Discovery live.v2 source log.
//! No network, implicit catalog defaults, replay cache, or cursor ownership here.

use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, str::FromStr};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("invalid discovery source snapshot: {0}")]
pub struct SourceError(pub String);

pub struct SnapshotBoundary<'a> {
    pub market_id: &'a str,
    pub condition_id: &'a str,
    pub token_id: &'a str,
    pub recovery_id: &'a str,
    pub cursor: u64,
    pub received_at: DateTime<Utc>,
}

pub fn canonical_hash(value: &Value) -> String {
    // This crate does not enable serde_json/preserve_order. Build every object
    // from sorted keys explicitly so callers cannot change the hash ordering.
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(values) => {
                let ordered: BTreeMap<_, _> =
                    values.iter().map(|(k, v)| (k.clone(), sorted(v))).collect();
                serde_json::to_value(ordered).expect("JSON object serialization")
            }
            Value::Array(values) => Value::Array(values.iter().map(sorted).collect()),
            _ => value.clone(),
        }
    }
    hex::encode(Sha256::digest(
        serde_json::to_vec(&sorted(value)).expect("JSON serialization"),
    ))
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str, SourceError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| SourceError(format!("missing or non-string {key}")))
}

fn decimal(text: &str) -> Result<Decimal, SourceError> {
    Decimal::from_str_exact(text)
        .map_err(|_| SourceError("decimal is invalid or exceeds exact precision".into()))
}

fn levels(raw: &Value, side: &str, tick: Decimal) -> Result<Value, SourceError> {
    let rows = raw
        .get(side)
        .and_then(Value::as_array)
        .ok_or_else(|| SourceError(format!("missing {side}")))?;
    let mut result = BTreeMap::new();
    for row in rows {
        let price = decimal(string(row, "price")?)?;
        let size = decimal(string(row, "size")?)?;
        if price < Decimal::ZERO
            || price > Decimal::ONE
            || size < Decimal::ZERO
            || price % tick != Decimal::ZERO
        {
            return Err(SourceError("invalid price, size or off-tick level".into()));
        }
        if result.insert(price, size).is_some() {
            return Err(SourceError("duplicate price level".into()));
        }
    }
    let mut rows: Vec<_> = result
        .into_iter()
        .map(|(price, size)| json!({"price":price.to_string(),"size":size.to_string()}))
        .collect();
    if side == "bids" {
        rows.reverse();
    }
    Ok(Value::Array(rows))
}

fn timestamp(value: DateTime<Utc>, z: bool) -> String {
    value.to_rfc3339_opts(
        if value.nanosecond() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        },
        z,
    )
}

/// Every successful call describes a new, explicitly named authoritative REST
/// recovery boundary. Both tokens must be validated by the caller before commit.
pub fn snapshot_event(raw: &Value, boundary: SnapshotBoundary<'_>) -> Result<Value, SourceError> {
    let cursor = boundary.cursor;
    prepare_snapshot(raw, boundary)?.finalize(cursor)
}

/// Cursor-free immutable draft. Expensive normalization is safe to run in parallel;
/// only the ordered publisher assigns the final cursor and event identity.
pub struct PreparedSnapshot {
    draft: Value,
}
impl PreparedSnapshot {
    pub fn finalize(mut self, cursor: u64) -> Result<Value, SourceError> {
        if cursor == 0 {
            return Err(SourceError("zero publication cursor".into()));
        }
        self.draft["cursor"] = json!(cursor);
        self.draft["event_id"] = json!(canonical_hash(&self.draft));
        Ok(self.draft)
    }
}
pub fn prepare_snapshot(
    raw: &Value,
    boundary: SnapshotBoundary<'_>,
) -> Result<PreparedSnapshot, SourceError> {
    if boundary.cursor == 0
        || boundary.recovery_id.is_empty()
        || boundary.market_id.is_empty()
        || boundary.received_at.nanosecond() % 1000 != 0
    {
        return Err(SourceError("invalid explicit boundary".into()));
    }
    if string(raw, "asset_id")? != boundary.token_id
        || string(raw, "market")? != boundary.condition_id
    {
        return Err(SourceError(
            "source token/condition differs from catalog".into(),
        ));
    }
    let tick = decimal(string(raw, "tick_size")?)?;
    if tick <= Decimal::ZERO || tick > Decimal::ONE {
        return Err(SourceError("invalid tick".into()));
    }
    let tick = tick.to_string();
    let bids = levels(raw, "bids", decimal(&tick)?)?;
    let asks = levels(raw, "asks", decimal(&tick)?)?;
    if let (Some(bid), Some(ask)) = (
        bids.as_array().and_then(|v| v.first()),
        asks.as_array().and_then(|v| v.first()),
    ) {
        if decimal(string(bid, "price")?)? >= decimal(string(ask, "price")?)? {
            return Err(SourceError("crossed or locked book".into()));
        }
    }
    let millis = i64::from_str(string(raw, "timestamp")?)
        .map_err(|_| SourceError("invalid source timestamp".into()))?;
    let exchange = DateTime::<Utc>::from_timestamp_millis(millis)
        .ok_or_else(|| SourceError("timestamp out of range".into()))?;
    let source_hash = string(raw, "hash")?;
    let last = match raw.get("last_trade_price") {
        None | Some(Value::Null) => Value::Null,
        Some(Value::String(text)) if text.is_empty() => Value::Null,
        Some(Value::String(text)) => {
            let value = decimal(text)?;
            if value < Decimal::ZERO || value > Decimal::ONE {
                return Err(SourceError("invalid last trade".into()));
            }
            json!(value.to_string())
        }
        _ => return Err(SourceError("invalid last trade type".into())),
    };
    let epoch = canonical_hash(
        &json!({"token_id":boundary.token_id,"recovery_id":boundary.recovery_id,
        "source_hash":source_hash,"exchange_at":timestamp(exchange, false)}),
    );
    let checksum = canonical_hash(
        &json!({"token_id":boundary.token_id,"tick_size":tick,"bids":bids,"asks":asks}),
    );
    let book = json!({"token_id":boundary.token_id,"condition_id":boundary.condition_id,
        "book_epoch":epoch,"sequence":1,"sequence_semantics":"deterministic_normalized",
        "exchange_at":timestamp(exchange, true),"received_at":timestamp(boundary.received_at, true),
        "tick_version":canonical_hash(&json!({"tick_size":tick})),"tick_size":tick,
        "bids":bids,"asks":asks,"last_trade_price":last,"state_checksum":checksum,"source_hash":source_hash});
    let event = json!({"contract_version":"marketcow.prediction_market.v1",
        "schema_version":"marketcow.polymarket.live.v2","event_type":"book",
        "market_id":boundary.market_id,"condition_id":boundary.condition_id,"token_id":boundary.token_id,
        "book_epoch":epoch,"sequence":1,"exchange_at":timestamp(exchange, true),
        "received_at":timestamp(boundary.received_at, true),"canonical_payload_sha256":canonical_hash(&book),
        "canonical_payload":book,"raw_payload_sha256":canonical_hash(raw),"raw_payload":raw,
        "applied":true,"fail_closed_reason":null,"gaps":[]});
    Ok(PreparedSnapshot { draft: event })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn raw() -> Value {
        json!({"asset_id":"123","market":"condition","tick_size":"0.01",
        "timestamp":"1700000000123","hash":"source-hash","bids":[{"price":"0.40","size":"10.0"}],
        "asks":[{"price":"0.60","size":"20.0"}]})
    }
    fn boundary() -> SnapshotBoundary<'static> {
        SnapshotBoundary {
            market_id: "1",
            condition_id: "condition",
            token_id: "123",
            recovery_id: "test-recovery",
            cursor: 42,
            received_at: DateTime::from_timestamp_millis(1700000001000).unwrap(),
        }
    }
    #[test]
    fn identity_and_point_in_time_are_bound() {
        let mut event = snapshot_event(&raw(), boundary()).unwrap();
        // Independently calculated with Python LiveBook/LiveEventEnvelope and
        // live_event_identity. Decimal scale and datetime formatting are wire facts.
        assert_eq!(
            event["canonical_payload_sha256"],
            "780f53dba8c54c2f5165ba0b4da9878be855292e427a43744690a33dad83cad3"
        );
        assert_eq!(
            event["event_id"],
            "dd717dad057b2e2e0226f5cc1124274c1a7b2961d1a5df60f54a593b15306dae"
        );
        let id = event.as_object_mut().unwrap().remove("event_id").unwrap();
        assert_eq!(id, canonical_hash(&event));
        assert_eq!(event["cursor"], 42);
        assert_eq!(
            event["canonical_payload"]["exchange_at"],
            "2023-11-14T22:13:20.123000Z"
        );
        assert_eq!(event["canonical_payload"]["bids"][0]["price"], "0.40");
    }

    #[test]
    fn prepared_snapshot_assigns_cursor_only_at_ordered_publication() {
        let prepared = prepare_snapshot(&raw(), boundary()).unwrap();
        assert!(prepared.draft.get("cursor").is_none());
        assert!(prepared.draft.get("event_id").is_none());
        assert_eq!(
            prepared.finalize(42).unwrap(),
            snapshot_event(&raw(), boundary()).unwrap()
        );
        let mut changed = boundary();
        changed.cursor = 99;
        assert_eq!(
            prepare_snapshot(&raw(), boundary())
                .unwrap()
                .finalize(99)
                .unwrap(),
            snapshot_event(&raw(), changed).unwrap()
        );
    }
    #[test]
    fn required_source_facts_cannot_use_defaults() {
        for field in [
            "asset_id",
            "market",
            "tick_size",
            "timestamp",
            "hash",
            "bids",
            "asks",
        ] {
            let mut value = raw();
            value.as_object_mut().unwrap().remove(field);
            assert!(snapshot_event(&value, boundary()).is_err(), "{field}");
        }
    }
    #[test]
    fn malformed_or_wrong_scope_books_fail_closed() {
        for (field, value) in [
            ("asset_id", json!("999")),
            ("market", json!("other")),
            ("tick_size", json!("0")),
            ("bids", json!([{"price":"0.405","size":"2"}])),
            ("asks", json!([{"price":"0.30","size":"2"}])),
        ] {
            let mut input = raw();
            input[field] = value;
            assert!(snapshot_event(&input, boundary()).is_err());
        }
    }
}
