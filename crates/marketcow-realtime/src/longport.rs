use super::{
    AggressorSide, BookLevel, EventQuality, ExactDecimal, NormalizedEventMetadata,
    NormalizedProviderEvent, ProviderPayload, RealtimeError,
};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub const LONGPORT_BRIDGE_VERSION: &str = "marketcow.longport.raw-push.v1";
pub const LONGPORT_NORMALIZER_VERSION: &str = "marketcow.longport.normalizer.v1";

#[derive(Debug, Clone)]
pub struct LongPortBridgeNormalizer {
    instrument_by_symbol: BTreeMap<String, String>,
    maximum_source_delay_millis: i64,
    enable_overnight: bool,
}

impl LongPortBridgeNormalizer {
    pub fn new(
        mappings: impl IntoIterator<Item = (String, String)>,
        maximum_source_delay_millis: i64,
        enable_overnight: bool,
    ) -> Result<Self, RealtimeError> {
        if maximum_source_delay_millis < 0 {
            return Err(RealtimeError::InvalidConfig);
        }
        let mut instrument_by_symbol = BTreeMap::new();
        for (instrument_id, symbol) in mappings {
            let symbol = symbol.trim().to_ascii_uppercase();
            if instrument_id.trim().is_empty()
                || !valid_longport_symbol(&symbol)
                || instrument_by_symbol.insert(symbol, instrument_id).is_some()
            {
                return Err(RealtimeError::InvalidConfig);
            }
        }
        if instrument_by_symbol.is_empty() {
            return Err(RealtimeError::InvalidConfig);
        }
        Ok(Self {
            instrument_by_symbol,
            maximum_source_delay_millis,
            enable_overnight,
        })
    }

    pub fn normalize(
        &self,
        raw: Value,
        received_at: DateTime<Utc>,
    ) -> Result<Vec<NormalizedProviderEvent>, RealtimeError> {
        let raw_sha256 = super::sha256(&raw);
        self.normalize_with_raw_sha256(raw, received_at, &raw_sha256)
    }

    pub fn normalize_with_raw_sha256(
        &self,
        raw: Value,
        received_at: DateTime<Utc>,
        raw_sha256: &str,
    ) -> Result<Vec<NormalizedProviderEvent>, RealtimeError> {
        if !super::valid_sha256_text(raw_sha256) {
            return Err(RealtimeError::InvalidRawEvidence);
        }
        let frame = raw.as_object().ok_or(RealtimeError::FrameNotObject)?;
        if required_string(frame, "schema_version")? != LONGPORT_BRIDGE_VERSION {
            return Err(RealtimeError::InvalidPayload);
        }
        let channel = required_string(frame, "channel")?;
        match channel {
            "depth" => self.normalize_depth(frame, received_at, raw_sha256),
            "market_state" => self.normalize_market_state(frame, received_at, raw_sha256),
            "trades" => self.normalize_trades(frame, received_at, raw_sha256),
            value => Err(RealtimeError::UnsupportedChannel(value.into())),
        }
    }

    fn normalize_depth(
        &self,
        frame: &Map<String, Value>,
        received_at: DateTime<Utc>,
        raw_sha256: &str,
    ) -> Result<Vec<NormalizedProviderEvent>, RealtimeError> {
        let (instrument_id, _) = self.instrument(frame)?;
        let observed_at = self.timestamp(frame, "observed_at", received_at)?;
        let bids = one_level(frame, "bids")?;
        let asks = one_level(frame, "asks")?;
        if bids
            .first()
            .zip(asks.first())
            .is_some_and(|(bid, ask)| bid.price.0 >= ask.price.0)
        {
            return Err(RealtimeError::CrossedBook);
        }
        let provider_sequence = optional_u64(frame, "provider_sequence")?;
        let mut events = vec![NormalizedProviderEvent::new(
            NormalizedEventMetadata {
                source: "longport",
                normalizer_version: LONGPORT_NORMALIZER_VERSION,
                instrument_id,
                source_observed_at: observed_at,
                received_at,
                raw_sha256,
                ordinal: 0,
                quality: EventQuality::live(),
            },
            ProviderPayload::OrderBookSnapshot {
                book_type: "L1_MBP".into(),
                depth: 1,
                baseline_sequence: 0,
                bids: bids.clone(),
                asks: asks.clone(),
                ts_event_source: "marketcow_observation".into(),
                provider_sequence,
            },
        )];
        if let Some((bid, ask)) = bids.first().zip(asks.first()) {
            events.push(NormalizedProviderEvent::new(
                NormalizedEventMetadata {
                    source: "longport",
                    normalizer_version: LONGPORT_NORMALIZER_VERSION,
                    instrument_id,
                    source_observed_at: observed_at,
                    received_at,
                    raw_sha256,
                    ordinal: 1,
                    quality: EventQuality::degraded(),
                },
                ProviderPayload::Quote {
                    bid_price: bid.price.clone(),
                    ask_price: ask.price.clone(),
                    bid_size: bid.size.clone(),
                    ask_size: ask.size.clone(),
                    ts_event_source: "marketcow_observation".into(),
                },
            ));
        }
        Ok(events)
    }

    fn normalize_market_state(
        &self,
        frame: &Map<String, Value>,
        received_at: DateTime<Utc>,
        raw_sha256: &str,
    ) -> Result<Vec<NormalizedProviderEvent>, RealtimeError> {
        let (instrument_id, _) = self.instrument(frame)?;
        let observed_at = self.timestamp(frame, "timestamp", received_at)?;
        let trade_status = longport_trade_status(required_string(frame, "trade_status")?);
        let session = longport_session(required_string(frame, "trade_session")?);
        let tradable = trade_status == "active" && !matches!(session, "closed" | "unknown");
        Ok(vec![NormalizedProviderEvent::new(
            NormalizedEventMetadata {
                source: "longport",
                normalizer_version: LONGPORT_NORMALIZER_VERSION,
                instrument_id,
                source_observed_at: observed_at,
                received_at,
                raw_sha256,
                ordinal: 0,
                quality: EventQuality::live(),
            },
            ProviderPayload::MarketState {
                trade_status: trade_status.into(),
                session: session.into(),
                tradable,
                provider_sequence: optional_u64(frame, "provider_sequence")?,
                ts_event_source: "provider".into(),
            },
        )])
    }

    fn normalize_trades(
        &self,
        frame: &Map<String, Value>,
        received_at: DateTime<Utc>,
        raw_sha256: &str,
    ) -> Result<Vec<NormalizedProviderEvent>, RealtimeError> {
        let (instrument_id, symbol) = self.instrument(frame)?;
        let trades = frame
            .get("trades")
            .and_then(Value::as_array)
            .filter(|trades| !trades.is_empty())
            .ok_or(RealtimeError::EmptyBatch)?;
        let mut events = Vec::with_capacity(trades.len());
        for (ordinal, value) in trades.iter().enumerate() {
            let trade = value.as_object().ok_or(RealtimeError::InvalidPayload)?;
            let observed_at = self.timestamp(trade, "timestamp", received_at)?;
            let price = ExactDecimal::positive(required(trade, "price")?)?;
            let size = ExactDecimal::positive(required(trade, "volume")?)?;
            let direction = required_string(trade, "direction")?;
            let session = longport_trade_session(required_string(trade, "trade_session")?);
            if session == "overnight" && !self.enable_overnight {
                events.push(NormalizedProviderEvent::new(
                    NormalizedEventMetadata {
                        source: "longport",
                        normalizer_version: LONGPORT_NORMALIZER_VERSION,
                        instrument_id,
                        source_observed_at: observed_at,
                        received_at,
                        raw_sha256,
                        ordinal,
                        quality: EventQuality::degraded(),
                    },
                    ProviderPayload::StreamStatus {
                        state: "degraded".into(),
                        reason_code: "overnight_trade_rejected".into(),
                        last_sequence: None,
                        resume_supported: true,
                    },
                ));
                continue;
            }
            let timestamp = super::python_iso(observed_at);
            let raw_trade = serde_json::json!({
                "symbol":symbol,
                "timestamp":timestamp,
                "price":price.0.to_string(),
                "volume":size.0.to_string(),
                "direction":direction,
                "index":ordinal,
            });
            let trade_id = format!("longport:{}", super::sha256(&raw_trade));
            let direction = direction.to_ascii_uppercase();
            let aggressor_side = if direction.contains("BUY") {
                AggressorSide::Buyer
            } else if direction.contains("SELL") {
                AggressorSide::Seller
            } else {
                AggressorSide::NoAggressor
            };
            events.push(NormalizedProviderEvent::new(
                NormalizedEventMetadata {
                    source: "longport",
                    normalizer_version: LONGPORT_NORMALIZER_VERSION,
                    instrument_id,
                    source_observed_at: observed_at,
                    received_at,
                    raw_sha256,
                    ordinal,
                    quality: EventQuality::live(),
                },
                ProviderPayload::Trade {
                    price,
                    size,
                    trade_id,
                    aggressor_side,
                    session: session.into(),
                },
            ));
        }
        Ok(events)
    }

    fn instrument<'a>(
        &'a self,
        frame: &'a Map<String, Value>,
    ) -> Result<(&'a str, &'a str), RealtimeError> {
        let symbol = required_string(frame, "symbol")?;
        if symbol != symbol.to_ascii_uppercase() || !valid_longport_symbol(symbol) {
            return Err(RealtimeError::InvalidPayload);
        }
        let instrument = self
            .instrument_by_symbol
            .get(symbol)
            .ok_or(RealtimeError::UnknownInstrument)?;
        Ok((instrument, symbol))
    }

    fn timestamp(
        &self,
        frame: &Map<String, Value>,
        field: &'static str,
        received_at: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, RealtimeError> {
        let value = required_string(frame, field)?;
        let observed_at = DateTime::parse_from_rfc3339(value)
            .map_err(|_| RealtimeError::InvalidTimestamp)?
            .with_timezone(&Utc);
        let delay = received_at
            .signed_duration_since(observed_at)
            .num_milliseconds();
        if delay < 0 || delay > self.maximum_source_delay_millis {
            return Err(RealtimeError::InvalidTimestamp);
        }
        Ok(observed_at)
    }
}

fn required<'a>(
    object: &'a Map<String, Value>,
    field: &'static str,
) -> Result<&'a Value, RealtimeError> {
    object.get(field).ok_or(RealtimeError::MissingField(field))
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    field: &'static str,
) -> Result<&'a str, RealtimeError> {
    required(object, field)?
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or(RealtimeError::InvalidPayload)
}

fn optional_u64(
    object: &Map<String, Value>,
    field: &'static str,
) -> Result<Option<u64>, RealtimeError> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or(RealtimeError::InvalidPayload),
    }
}

fn one_level(
    object: &Map<String, Value>,
    field: &'static str,
) -> Result<Vec<BookLevel>, RealtimeError> {
    let values = required(object, field)?
        .as_array()
        .filter(|values| values.len() <= 1)
        .ok_or(RealtimeError::InvalidPayload)?;
    values
        .iter()
        .map(|value| {
            let level = value.as_object().ok_or(RealtimeError::InvalidPayload)?;
            Ok(BookLevel {
                price: ExactDecimal::positive(required(level, "price")?)?,
                size: ExactDecimal::positive(required(level, "size")?)?,
                order_id: "0".into(),
                order_count: None,
            })
        })
        .collect()
}

fn valid_longport_symbol(value: &str) -> bool {
    value.len() >= 3
        && value.len() <= 64
        && value.contains('.')
        && value.bytes().all(|byte| {
            byte.is_ascii_uppercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
}

fn simplified(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(['_', ' '], "")
}

fn longport_trade_status(value: &str) -> &'static str {
    let value = simplified(value);
    if matches!(value.as_str(), "normal" | "0") || value.ends_with(".normal") {
        "active"
    } else if value.contains("delisted") || value.contains("expired") {
        "delisted"
    } else if value.contains("fuse") {
        "volatility_halt"
    } else if value.contains("tobeopened") || value.contains("preparelist") {
        "opening"
    } else if value.contains("halt") || value.contains("suspend") {
        "halted"
    } else {
        "unknown"
    }
}

fn longport_session(value: &str) -> &'static str {
    let value = simplified(value);
    if value.contains("overnight") {
        "overnight"
    } else if value.contains("pre") {
        "pre_market"
    } else if value.contains("post") {
        "post_market"
    } else if matches!(
        value.as_str(),
        "normal" | "normaltrade" | "regular" | "trading" | "0"
    ) {
        "regular"
    } else if matches!(value.as_str(), "closed" | "close") {
        "closed"
    } else {
        "unknown"
    }
}

fn longport_trade_session(value: &str) -> &'static str {
    let value = simplified(value);
    if value.contains("pre") {
        "pre_market"
    } else if value.contains("post") {
        "post_market"
    } else if value.contains("overnight") {
        "overnight"
    } else if matches!(value.as_str(), "" | "normal" | "regular") {
        "regular"
    } else {
        "unknown"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;
    use tempfile::tempdir;

    fn received() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 23, 1, 0, 1).unwrap()
    }

    fn normalizer(enable_overnight: bool) -> LongPortBridgeNormalizer {
        LongPortBridgeNormalizer::new(
            [("AAPL.XNAS".into(), "AAPL.US".into())],
            5_000,
            enable_overnight,
        )
        .unwrap()
    }

    #[test]
    fn shared_python_rust_bridge_golden_matches_legacy_public_events() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/longport-realtime-bridge-v1.json"
        ))
        .unwrap();
        let received_at = DateTime::parse_from_rfc3339(fixture["received_at"].as_str().unwrap())
            .unwrap()
            .with_timezone(&Utc);
        let mappings = fixture["mappings"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(instrument, symbol)| (instrument.clone(), symbol.as_str().unwrap().to_owned()));
        let normalizer = LongPortBridgeNormalizer::new(mappings, 5_000, false).unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let actual = normalizer
                .normalize(case["raw"].clone(), received_at)
                .unwrap()
                .iter()
                .map(NormalizedProviderEvent::public_contract_value)
                .collect::<Vec<_>>();
            assert_eq!(
                Value::Array(actual),
                case["expected"],
                "case {}",
                case["name"]
            );
        }
    }

    #[test]
    fn typed_depth_matches_legacy_l1_quote_contract_with_exact_decimals() {
        let events = normalizer(false)
            .normalize(
                json!({
                    "schema_version":LONGPORT_BRIDGE_VERSION,
                    "channel":"depth","symbol":"AAPL.US",
                    "observed_at":"2026-07-23T01:00:00Z","provider_sequence":10,
                    "bids":[{"price":"100.0000","size":"2"}],
                    "asks":[{"price":"101.0000","size":"3"}]
                }),
                received(),
            )
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].data_type(), super::super::DataType::OrderBook);
        assert_eq!(events[1].data_type(), super::super::DataType::Quote);
        assert!(events[1].quality.degraded);
        assert_eq!(
            events[1].public_contract_value()["payload"]["bid_price"],
            "100"
        );
    }

    #[test]
    fn market_state_and_trades_preserve_provider_time_session_and_identity() {
        let state = normalizer(false)
            .normalize(
                json!({
                    "schema_version":LONGPORT_BRIDGE_VERSION,
                    "channel":"market_state","symbol":"AAPL.US",
                    "timestamp":"2026-07-23T01:00:00Z","provider_sequence":11,
                    "trade_status":"Normal","trade_session":"Normal"
                }),
                received(),
            )
            .unwrap();
        let ProviderPayload::MarketState {
            trade_status,
            session,
            tradable,
            ..
        } = &state[0].payload
        else {
            panic!("expected market state")
        };
        assert_eq!(trade_status, "active");
        assert_eq!(session, "regular");
        assert!(*tradable);

        let trades = normalizer(false)
            .normalize(
                json!({
                    "schema_version":LONGPORT_BRIDGE_VERSION,
                    "channel":"trades","symbol":"AAPL.US","trades":[{
                        "timestamp":"2026-07-23T01:00:00Z","price":"100.5",
                        "volume":"4","direction":"Buy","trade_session":"Normal"
                    }]
                }),
                received(),
            )
            .unwrap();
        let ProviderPayload::Trade {
            trade_id,
            aggressor_side,
            session,
            ..
        } = &trades[0].payload
        else {
            panic!("expected trade")
        };
        assert!(trade_id.starts_with("longport:"));
        assert_eq!(*aggressor_side, AggressorSide::Buyer);
        assert_eq!(session, "regular");
        assert!(trades[0].evidence_is_valid());
    }

    #[test]
    fn overnight_policy_is_durable_status_instead_of_silent_drop() {
        let raw = json!({
            "schema_version":LONGPORT_BRIDGE_VERSION,
            "channel":"trades","symbol":"AAPL.US","trades":[{
                "timestamp":"2026-07-23T01:00:00Z","price":"100.5",
                "volume":"4","direction":"Buy","trade_session":"Overnight"
            }]
        });
        let rejected = normalizer(false)
            .normalize(raw.clone(), received())
            .unwrap();
        assert!(matches!(
            rejected[0].payload,
            ProviderPayload::StreamStatus { ref reason_code, .. }
                if reason_code == "overnight_trade_rejected"
        ));
        assert!(rejected[0].quality.degraded);
        let accepted = normalizer(true).normalize(raw, received()).unwrap();
        assert!(matches!(
            accepted[0].payload,
            ProviderPayload::Trade { ref session, .. } if session == "overnight"
        ));
    }

    #[test]
    fn bridge_rejects_numeric_money_unknown_symbol_and_stale_time() {
        let numeric = json!({
            "schema_version":LONGPORT_BRIDGE_VERSION,"channel":"depth",
            "symbol":"AAPL.US","observed_at":"2026-07-23T01:00:00Z",
            "bids":[{"price":100.0,"size":"2"}],"asks":[]
        });
        assert_eq!(
            normalizer(false)
                .normalize(numeric, received())
                .unwrap_err(),
            RealtimeError::FinancialDecimalMustBeString
        );
        let stale = json!({
            "schema_version":LONGPORT_BRIDGE_VERSION,"channel":"market_state",
            "symbol":"AAPL.US","timestamp":"2026-07-23T00:00:00Z",
            "trade_status":"Normal","trade_session":"Normal"
        });
        assert_eq!(
            normalizer(false).normalize(stale, received()).unwrap_err(),
            RealtimeError::InvalidTimestamp
        );
    }

    #[test]
    fn bridge_contract_is_provider_neutral_and_contains_no_credentials() {
        assert_eq!(
            super::super::REALTIME_CONTRACT_VERSION,
            "marketcow.realtime.provider-neutral.v1"
        );
        let debug = format!("{:?}", normalizer(false));
        assert!(!debug.contains("app_key"));
        assert!(!debug.contains("app_secret"));
        assert!(!debug.contains("access_token"));
    }

    #[test]
    fn typed_bridge_batch_uses_the_same_durable_hub_boundary() {
        let events = normalizer(false)
            .normalize(
                json!({
                    "schema_version":LONGPORT_BRIDGE_VERSION,
                    "channel":"depth","symbol":"AAPL.US",
                    "observed_at":"2026-07-23T01:00:00Z","provider_sequence":10,
                    "bids":[{"price":"100","size":"2"}],
                    "asks":[{"price":"101","size":"3"}]
                }),
                received(),
            )
            .unwrap();
        let directory = tempdir().unwrap();
        let root = directory.path().join("longport-runtime");
        let mut writer = super::super::DurableRealtimeWriter::open(
            &root,
            "longport-main",
            "config-v1",
            16 * 1_024,
            4,
        )
        .unwrap();
        let outcomes = writer.apply_batch(events).unwrap();
        assert_eq!(outcomes.len(), 2);
        assert_eq!(writer.sequence(), 2);
        writer.checkpoint().unwrap();
        drop(writer);

        let recovered = super::super::DurableRealtimeWriter::open(
            &root,
            "longport-main",
            "config-v1",
            16 * 1_024,
            4,
        )
        .unwrap();
        assert_eq!(recovered.sequence(), 2);
        assert_eq!(recovered.wal_cursor(), 2);
        assert_eq!(recovered.records()[0].event.source, "longport");
    }
}
