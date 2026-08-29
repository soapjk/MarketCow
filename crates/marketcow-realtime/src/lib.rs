//! Provider-neutral realtime normalization and replay primitives.
//!
//! This crate deliberately has no HTTP or database dependency. Venue adapters provide raw frames;
//! the Rust owner validates exact decimals and source time before its durable single writer assigns
//! the public stream sequence. The writer owns append, sync, replay, and publication ordering so
//! callers cannot publish before the authoritative append succeeds.

mod durability;
mod hub;
mod longport;
pub use durability::{
    DurabilityError, DurableApplyOutcome, DurableRealtimeWriter, PersistedRealtimeEvent,
    REALTIME_CHECKPOINT_VERSION, REALTIME_SPARSE_INDEX_VERSION, REALTIME_WAL_BATCH_VERSION,
    REALTIME_WAL_SEGMENT_VERSION, REALTIME_WAL_VERSION, RealtimeCheckpoint, RealtimeSparseIndex,
    SegmentedRealtimeWal, SparseCursorEntry,
};
pub use hub::{
    DurableRealtimeHub, REALTIME_HUB_PROJECTION_VERSION, RealtimeHubError, RealtimeHubHealth,
    RealtimeHubProjection, RealtimeHubReader,
};
pub use longport::{
    LONGPORT_BRIDGE_VERSION, LONGPORT_NORMALIZER_VERSION, LongPortBridgeNormalizer,
};

use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    str::FromStr,
    time::Duration,
};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use url::Url;

pub const REALTIME_CONTRACT_VERSION: &str = "marketcow.realtime.provider-neutral.v1";
pub const HYPERLIQUID_NORMALIZER_VERSION: &str = "marketcow.hyperliquid.normalizer.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExactDecimal(#[serde(with = "rust_decimal::serde::str")] pub Decimal);

impl ExactDecimal {
    pub fn positive(value: &Value) -> Result<Self, RealtimeError> {
        let text = value
            .as_str()
            .ok_or(RealtimeError::FinancialDecimalMustBeString)?;
        let decimal = Decimal::from_str(text).map_err(|_| RealtimeError::InvalidDecimal)?;
        if decimal <= Decimal::ZERO {
            return Err(RealtimeError::InvalidDecimal);
        }
        Ok(Self(decimal.normalize()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AggressorSide {
    Buyer,
    Seller,
    NoAggressor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventQuality {
    pub status: String,
    pub delayed: bool,
    pub stale: bool,
    pub degraded: bool,
}

impl EventQuality {
    fn live() -> Self {
        Self {
            status: "live".into(),
            delayed: false,
            stale: false,
            degraded: false,
        }
    }

    fn degraded() -> Self {
        Self {
            status: "degraded".into(),
            delayed: false,
            stale: false,
            degraded: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: ExactDecimal,
    pub size: ExactDecimal,
    pub order_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "payload", rename_all = "snake_case")]
pub enum ProviderPayload {
    OrderBookSnapshot {
        book_type: String,
        depth: u8,
        baseline_sequence: u64,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        ts_event_source: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        provider_sequence: Option<u64>,
    },
    Quote {
        bid_price: ExactDecimal,
        ask_price: ExactDecimal,
        bid_size: ExactDecimal,
        ask_size: ExactDecimal,
        ts_event_source: String,
    },
    Trade {
        price: ExactDecimal,
        size: ExactDecimal,
        trade_id: String,
        aggressor_side: AggressorSide,
        session: String,
    },
    AssetContext {
        mark_price: Option<ExactDecimal>,
        oracle_price: Option<ExactDecimal>,
        external_oracle_price: Option<ExactDecimal>,
        mid_price: Option<ExactDecimal>,
        funding_rate: Option<ExactDecimal>,
        open_interest: Option<ExactDecimal>,
        premium: Option<ExactDecimal>,
        market_status: String,
        oracle_status: String,
    },
    MarketState {
        trade_status: String,
        session: String,
        tradable: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        provider_sequence: Option<u64>,
        ts_event_source: String,
    },
    StreamStatus {
        state: String,
        reason_code: String,
        last_sequence: Option<u64>,
        resume_supported: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedProviderEvent {
    pub contract_version: String,
    pub normalizer_version: String,
    pub instrument_id: String,
    pub source: String,
    pub source_observed_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub raw_sha256: String,
    pub normalizer_ordinal: u64,
    pub event_id: String,
    pub quality: EventQuality,
    pub payload: ProviderPayload,
}

pub(crate) struct NormalizedEventMetadata<'a> {
    pub source: &'a str,
    pub normalizer_version: &'a str,
    pub instrument_id: &'a str,
    pub source_observed_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub raw_sha256: &'a str,
    pub ordinal: usize,
    pub quality: EventQuality,
}

impl NormalizedProviderEvent {
    fn new(metadata: NormalizedEventMetadata<'_>, payload: ProviderPayload) -> Self {
        let NormalizedEventMetadata {
            source,
            normalizer_version,
            instrument_id,
            source_observed_at,
            received_at,
            raw_sha256,
            ordinal,
            quality,
        } = metadata;
        let normalizer_ordinal = u64::try_from(ordinal).expect("event ordinal fits into u64");
        let event_id = sha256(&serde_json::json!({
            "normalizer": normalizer_version,
            "instrument_id": instrument_id,
            "source_observed_at": source_observed_at,
            "raw_sha256": raw_sha256,
            "ordinal": normalizer_ordinal,
            "payload": payload,
        }));
        Self {
            contract_version: REALTIME_CONTRACT_VERSION.into(),
            normalizer_version: normalizer_version.into(),
            instrument_id: instrument_id.into(),
            source: source.into(),
            source_observed_at,
            received_at,
            raw_sha256: raw_sha256.into(),
            normalizer_ordinal,
            event_id,
            quality,
            payload,
        }
    }

    pub(crate) fn evidence_is_valid(&self) -> bool {
        let known_normalizer_source = matches!(
            (self.normalizer_version.as_str(), self.source.as_str()),
            (HYPERLIQUID_NORMALIZER_VERSION, "hyperliquid")
                | (LONGPORT_NORMALIZER_VERSION, "longport")
        );
        self.contract_version == REALTIME_CONTRACT_VERSION
            && known_normalizer_source
            && !self.instrument_id.is_empty()
            && valid_sha256_text(&self.raw_sha256)
            && self.received_at >= self.source_observed_at
            && self.event_id
                == sha256(&serde_json::json!({
                    "normalizer": self.normalizer_version,
                    "instrument_id": self.instrument_id,
                    "source_observed_at": self.source_observed_at,
                    "raw_sha256": self.raw_sha256,
                    "ordinal": self.normalizer_ordinal,
                    "payload": self.payload,
                }))
            && self.quality_is_valid()
    }

    fn quality_is_valid(&self) -> bool {
        let live = self.quality == EventQuality::live();
        let degraded = self.quality == EventQuality::degraded();
        match (&self.source[..], &self.payload) {
            ("hyperliquid", _) => live,
            ("longport", ProviderPayload::Quote { .. }) => degraded,
            ("longport", ProviderPayload::StreamStatus { state, .. }) => {
                (state == "live" && live) || (state == "degraded" && degraded)
            }
            ("longport", _) => live,
            _ => false,
        }
    }

    pub fn data_type(&self) -> DataType {
        match self.payload {
            ProviderPayload::OrderBookSnapshot { .. } => DataType::OrderBook,
            ProviderPayload::Quote { .. } => DataType::Quote,
            ProviderPayload::Trade { .. } => DataType::Trade,
            ProviderPayload::AssetContext { .. } => DataType::AssetContext,
            ProviderPayload::MarketState { .. } => DataType::MarketState,
            ProviderPayload::StreamStatus { .. } => DataType::StreamStatus,
        }
    }

    /// Existing Python downstream event shape used by the cross-language golden comparator.
    /// Source evidence remains on the typed envelope and is not discarded by the Rust owner.
    pub fn public_contract_value(&self) -> Value {
        let (event_type, payload) = match &self.payload {
            ProviderPayload::OrderBookSnapshot { .. } => ("order_book_snapshot", &self.payload),
            ProviderPayload::Quote { .. } => ("quote", &self.payload),
            ProviderPayload::Trade { .. } => ("trade", &self.payload),
            ProviderPayload::AssetContext { .. } => ("asset_context", &self.payload),
            ProviderPayload::MarketState { .. } => ("market_state", &self.payload),
            ProviderPayload::StreamStatus { .. } => ("stream_status", &self.payload),
        };
        let serialized = serde_json::to_value(payload).expect("typed provider payload serializes");
        let payload = serialized
            .get("payload")
            .cloned()
            .expect("internally-tagged provider payload has content");
        let mut output = serde_json::json!({
            "event_type":event_type,
            "source":self.source,
            "ts_event":python_iso(self.source_observed_at),
            "payload":payload,
        });
        if !matches!(&self.payload, ProviderPayload::StreamStatus { .. }) {
            output
                .as_object_mut()
                .expect("public event is an object")
                .insert(
                    "instrument_id".into(),
                    Value::String(self.instrument_id.clone()),
                );
        }
        if self.quality.degraded {
            output
                .as_object_mut()
                .expect("public event is an object")
                .insert(
                    "quality".into(),
                    serde_json::to_value(&self.quality).expect("quality serializes"),
                );
        }
        output
    }
}

#[derive(Debug, Clone)]
pub struct HyperliquidNormalizer {
    instrument_by_coin: BTreeMap<String, String>,
    maximum_source_delay_millis: i64,
}

impl HyperliquidNormalizer {
    pub fn new(
        mappings: impl IntoIterator<Item = (String, String)>,
        maximum_source_delay_millis: i64,
    ) -> Result<Self, RealtimeError> {
        if maximum_source_delay_millis < 0 {
            return Err(RealtimeError::InvalidConfig);
        }
        let mut instrument_by_coin = BTreeMap::new();
        for (instrument_id, coin) in mappings {
            let normalized = coin.trim().to_ascii_uppercase();
            if instrument_id.trim().is_empty()
                || !valid_coin(&normalized)
                || instrument_by_coin
                    .insert(normalized, instrument_id)
                    .is_some()
            {
                return Err(RealtimeError::InvalidConfig);
            }
        }
        if instrument_by_coin.is_empty() {
            return Err(RealtimeError::InvalidConfig);
        }
        Ok(Self {
            instrument_by_coin,
            maximum_source_delay_millis,
        })
    }

    pub fn normalize(
        &self,
        raw: Value,
        received_at: DateTime<Utc>,
    ) -> Result<Vec<NormalizedProviderEvent>, RealtimeError> {
        let raw_sha256 = sha256(&raw);
        self.normalize_with_raw_sha256(raw, received_at, &raw_sha256)
    }

    /// Normalize a frame while retaining the SHA-256 of the exact UTF-8 WebSocket payload. The
    /// transport uses this path; tests and offline semantic fixtures may use `normalize`.
    pub fn normalize_with_raw_sha256(
        &self,
        raw: Value,
        received_at: DateTime<Utc>,
        raw_sha256: &str,
    ) -> Result<Vec<NormalizedProviderEvent>, RealtimeError> {
        if raw_sha256.len() != 64
            || !raw_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(RealtimeError::InvalidRawEvidence);
        }
        let object = raw.as_object().ok_or(RealtimeError::FrameNotObject)?;
        let channel = required_string(object, "channel")?;
        match channel {
            "l2Book" | "bbo" => self.normalize_book(object, received_at, raw_sha256),
            "trades" => self.normalize_trades(object, received_at, raw_sha256),
            "activeAssetCtx" => self.normalize_context(object, received_at, raw_sha256),
            value => Err(RealtimeError::UnsupportedChannel(value.into())),
        }
    }

    fn normalize_book(
        &self,
        frame: &Map<String, Value>,
        received_at: DateTime<Utc>,
        raw_sha256: &str,
    ) -> Result<Vec<NormalizedProviderEvent>, RealtimeError> {
        let data = object_field(frame, "data")?;
        let instrument_id = self.instrument(data)?;
        let observed_at = self.observed_at(data, received_at)?;
        let (bid_values, ask_values) = book_sides(data)?;
        let bids = parse_levels(&bid_values)?;
        let asks = parse_levels(&ask_values)?;
        if !strictly_descending(&bids) || !strictly_ascending(&asks) {
            return Err(RealtimeError::InvalidPayload);
        }
        if bids
            .first()
            .zip(asks.first())
            .is_some_and(|(bid, ask)| bid.price.0 >= ask.price.0)
        {
            return Err(RealtimeError::CrossedBook);
        }
        let maximum_depth = bids.len().max(asks.len());
        let depth = [1_u8, 5, 10, 20]
            .into_iter()
            .find(|candidate| maximum_depth <= usize::from(*candidate))
            .ok_or(RealtimeError::BookTooDeep)?;
        let book_type = if maximum_depth > 1 {
            "L2_MBP"
        } else {
            "L1_MBP"
        };
        let mut events = vec![NormalizedProviderEvent::new(
            NormalizedEventMetadata {
                source: "hyperliquid",
                normalizer_version: HYPERLIQUID_NORMALIZER_VERSION,
                instrument_id,
                source_observed_at: observed_at,
                received_at,
                raw_sha256,
                ordinal: 0,
                quality: EventQuality::live(),
            },
            ProviderPayload::OrderBookSnapshot {
                book_type: book_type.into(),
                depth,
                baseline_sequence: 0,
                bids: bids.clone(),
                asks: asks.clone(),
                ts_event_source: "provider".into(),
                provider_sequence: None,
            },
        )];
        if let Some((bid, ask)) = bids.first().zip(asks.first()) {
            events.push(NormalizedProviderEvent::new(
                NormalizedEventMetadata {
                    source: "hyperliquid",
                    normalizer_version: HYPERLIQUID_NORMALIZER_VERSION,
                    instrument_id,
                    source_observed_at: observed_at,
                    received_at,
                    raw_sha256,
                    ordinal: 1,
                    quality: EventQuality::live(),
                },
                ProviderPayload::Quote {
                    bid_price: bid.price.clone(),
                    ask_price: ask.price.clone(),
                    bid_size: bid.size.clone(),
                    ask_size: ask.size.clone(),
                    ts_event_source: "provider".into(),
                },
            ));
        }
        Ok(events)
    }

    fn normalize_trades(
        &self,
        frame: &Map<String, Value>,
        received_at: DateTime<Utc>,
        raw_sha256: &str,
    ) -> Result<Vec<NormalizedProviderEvent>, RealtimeError> {
        let trades = frame
            .get("data")
            .and_then(Value::as_array)
            .ok_or(RealtimeError::MissingField("data"))?;
        if trades.is_empty() {
            return Err(RealtimeError::EmptyBatch);
        }
        trades
            .iter()
            .enumerate()
            .map(|(ordinal, value)| {
                let trade = value.as_object().ok_or(RealtimeError::InvalidPayload)?;
                let instrument_id = self.instrument(trade)?;
                let observed_at = self.observed_at(trade, received_at)?;
                let trade_id = trade
                    .get("tid")
                    .or_else(|| trade.get("hash"))
                    .map(value_identifier)
                    .transpose()?
                    .unwrap_or_else(|| observed_at.timestamp_millis().to_string());
                if trade_id.is_empty() {
                    return Err(RealtimeError::InvalidPayload);
                }
                let aggressor_side = match required_string(trade, "side")?
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "B" => AggressorSide::Buyer,
                    "A" => AggressorSide::Seller,
                    _ => return Err(RealtimeError::InvalidPayload),
                };
                Ok(NormalizedProviderEvent::new(
                    NormalizedEventMetadata {
                        source: "hyperliquid",
                        normalizer_version: HYPERLIQUID_NORMALIZER_VERSION,
                        instrument_id,
                        source_observed_at: observed_at,
                        received_at,
                        raw_sha256,
                        ordinal,
                        quality: EventQuality::live(),
                    },
                    ProviderPayload::Trade {
                        price: ExactDecimal::positive(required(trade, "px")?)?,
                        size: ExactDecimal::positive(required(trade, "sz")?)?,
                        trade_id,
                        aggressor_side,
                        session: "regular".into(),
                    },
                ))
            })
            .collect()
    }

    fn normalize_context(
        &self,
        frame: &Map<String, Value>,
        received_at: DateTime<Utc>,
        raw_sha256: &str,
    ) -> Result<Vec<NormalizedProviderEvent>, RealtimeError> {
        let data = object_field(frame, "data")?;
        let instrument_id = self.instrument(data)?;
        let context = data.get("ctx").and_then(Value::as_object).unwrap_or(data);
        let observed_at = data
            .get("time")
            .or_else(|| context.get("time"))
            .map(|value| timestamp(value, received_at, self.maximum_source_delay_millis))
            .transpose()?
            .unwrap_or(received_at);
        let oracle = optional_positive(context.get("oraclePx"))?;
        let external = optional_positive(context.get("externalPerpPx"))?;
        let oracle_status = if external.is_some() {
            "external_live"
        } else if oracle.is_some() {
            "internal_only"
        } else {
            "unavailable"
        };
        Ok(vec![NormalizedProviderEvent::new(
            NormalizedEventMetadata {
                source: "hyperliquid",
                normalizer_version: HYPERLIQUID_NORMALIZER_VERSION,
                instrument_id,
                source_observed_at: observed_at,
                received_at,
                raw_sha256,
                ordinal: 0,
                quality: EventQuality::live(),
            },
            ProviderPayload::AssetContext {
                mark_price: optional_positive(context.get("markPx"))?,
                oracle_price: oracle,
                external_oracle_price: external,
                mid_price: optional_positive(context.get("midPx"))?,
                funding_rate: optional_decimal(context.get("funding"))?,
                open_interest: optional_positive(context.get("openInterest"))?,
                premium: optional_decimal(context.get("premium"))?,
                market_status: "active".into(),
                oracle_status: oracle_status.into(),
            },
        )])
    }

    fn instrument<'a>(&'a self, data: &Map<String, Value>) -> Result<&'a str, RealtimeError> {
        let coin = required_string(data, "coin")?.to_ascii_uppercase();
        self.instrument_by_coin
            .get(&coin)
            .map(String::as_str)
            .ok_or(RealtimeError::UnknownInstrument)
    }

    fn observed_at(
        &self,
        data: &Map<String, Value>,
        received_at: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, RealtimeError> {
        timestamp(
            required(data, "time")?,
            received_at,
            self.maximum_source_delay_millis,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum HyperliquidSubscriptionKind {
    L2Book,
    Trades,
    ActiveAssetCtx,
}

impl HyperliquidSubscriptionKind {
    fn as_wire(self) -> &'static str {
        match self {
            Self::L2Book => "l2Book",
            Self::Trades => "trades",
            Self::ActiveAssetCtx => "activeAssetCtx",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidSubscription {
    pub coin: String,
    pub kind: HyperliquidSubscriptionKind,
}

impl HyperliquidSubscription {
    fn wire_message(&self) -> Value {
        serde_json::json!({
            "method":"subscribe",
            "subscription":{"type":self.kind.as_wire(),"coin":self.coin}
        })
    }
}

pub fn hyperliquid_subscriptions(
    mappings: &BTreeMap<String, String>,
    data_types: &BTreeSet<DataType>,
) -> Result<Vec<HyperliquidSubscription>, RealtimeError> {
    if mappings.is_empty()
        || data_types.is_empty()
        || data_types.contains(&DataType::MarketState)
        || data_types.contains(&DataType::StreamStatus)
    {
        return Err(RealtimeError::InvalidConfig);
    }
    let wants_book =
        data_types.contains(&DataType::Quote) || data_types.contains(&DataType::OrderBook);
    let wants_trade = data_types.contains(&DataType::Trade);
    let wants_context = data_types.contains(&DataType::AssetContext);
    let mut unique = BTreeSet::new();
    let mut instrument_by_coin = BTreeMap::new();
    for (instrument, coin) in mappings {
        let coin = coin.trim().to_ascii_uppercase();
        if instrument.trim().is_empty()
            || !valid_coin(&coin)
            || instrument_by_coin
                .insert(coin.clone(), instrument.as_str())
                .is_some()
        {
            return Err(RealtimeError::InvalidConfig);
        }
        for (kind, wanted) in [
            (HyperliquidSubscriptionKind::L2Book, wants_book),
            (HyperliquidSubscriptionKind::Trades, wants_trade),
            (HyperliquidSubscriptionKind::ActiveAssetCtx, wants_context),
        ] {
            if wanted {
                unique.insert((coin.clone(), kind));
            }
        }
    }
    Ok(unique
        .into_iter()
        .map(|(coin, kind)| HyperliquidSubscription { coin, kind })
        .collect())
}

#[derive(Debug, Clone)]
pub struct HyperliquidTransportConfig {
    endpoint: Url,
    connect_timeout: Duration,
    reconnect_delay: Duration,
    heartbeat_interval: Duration,
    maximum_reconnect_attempts: u32,
    maximum_frame_bytes: usize,
}

impl HyperliquidTransportConfig {
    pub fn production() -> Self {
        Self {
            endpoint: Url::parse("wss://api.hyperliquid.xyz/ws")
                .expect("the fixed Hyperliquid endpoint is valid"),
            connect_timeout: Duration::from_secs(5),
            reconnect_delay: Duration::from_millis(500),
            heartbeat_interval: Duration::from_secs(30),
            maximum_reconnect_attempts: 8,
            maximum_frame_bytes: 1_048_576,
        }
    }

    pub fn validate(&self) -> Result<(), TransportError> {
        let valid_production = self.endpoint.scheme() == "wss"
            && self.endpoint.host_str() == Some("api.hyperliquid.xyz")
            && self.endpoint.port().is_none()
            && self.endpoint.path() == "/ws"
            && self.endpoint.query().is_none()
            && self.endpoint.fragment().is_none()
            && self.endpoint.username().is_empty()
            && self.endpoint.password().is_none();
        #[cfg(test)]
        let valid_test = self.endpoint.scheme() == "ws"
            && self
                .endpoint
                .host_str()
                .and_then(|host| host.parse::<std::net::IpAddr>().ok())
                .is_some_and(|address| address.is_loopback())
            && self.endpoint.port().is_some()
            && self.endpoint.path() == "/ws"
            && self.endpoint.query().is_none()
            && self.endpoint.fragment().is_none()
            && self.endpoint.username().is_empty()
            && self.endpoint.password().is_none();
        #[cfg(not(test))]
        let valid_test = false;
        if (!valid_production && !valid_test)
            || self.connect_timeout.is_zero()
            || self.reconnect_delay.is_zero()
            || self.heartbeat_interval.is_zero()
            || self.maximum_reconnect_attempts == 0
            || !(1..=1_048_576).contains(&self.maximum_frame_bytes)
        {
            return Err(TransportError::InvalidConfig);
        }
        Ok(())
    }

    #[cfg(test)]
    fn loopback(endpoint: Url) -> Self {
        Self {
            endpoint,
            connect_timeout: Duration::from_secs(2),
            reconnect_delay: Duration::from_millis(10),
            heartbeat_interval: Duration::from_secs(30),
            maximum_reconnect_attempts: 2,
            maximum_frame_bytes: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransportOutput {
    Connected {
        attempt: u32,
    },
    SubscriptionsReady {
        attempt: u32,
    },
    Events {
        attempt: u32,
        events: Vec<NormalizedProviderEvent>,
    },
    Degraded {
        attempt: u32,
        reason: String,
        retryable: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionEnd {
    Shutdown,
    Disconnected,
}

/// Own the public Hyperliquid WebSocket in Rust. Any unprocessable frame closes the connection;
/// no event is skipped and no unbounded queue is introduced. A supervisor performs only the
/// configured bounded number of reconnects and replays the deterministic subscription set.
pub async fn run_hyperliquid_transport(
    config: HyperliquidTransportConfig,
    normalizer: HyperliquidNormalizer,
    subscriptions: Vec<HyperliquidSubscription>,
    output: mpsc::Sender<TransportOutput>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), TransportError> {
    config.validate()?;
    if subscriptions.is_empty() || !valid_subscription_set(&subscriptions) {
        return Err(TransportError::InvalidConfig);
    }
    for attempt in 1..=config.maximum_reconnect_attempts {
        if *shutdown.borrow() {
            return Ok(());
        }
        match run_hyperliquid_connection(
            &config,
            &normalizer,
            &subscriptions,
            &output,
            &mut shutdown,
            attempt,
        )
        .await
        {
            Ok(ConnectionEnd::Shutdown) => return Ok(()),
            Ok(ConnectionEnd::Disconnected) => {
                try_output(
                    &output,
                    TransportOutput::Degraded {
                        attempt,
                        reason: "upstream_disconnected".into(),
                        retryable: true,
                    },
                )?;
            }
            Err(TransportError::Backpressure) => return Err(TransportError::Backpressure),
            Err(error) => {
                try_output(
                    &output,
                    TransportOutput::Degraded {
                        attempt,
                        reason: error.reason_code().into(),
                        retryable: attempt < config.maximum_reconnect_attempts,
                    },
                )?;
            }
        }
        if attempt == config.maximum_reconnect_attempts {
            return Err(TransportError::ReconnectExhausted);
        }
        tokio::select! {
            _ = tokio::time::sleep(config.reconnect_delay) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
    Err(TransportError::ReconnectExhausted)
}

async fn run_hyperliquid_connection(
    config: &HyperliquidTransportConfig,
    normalizer: &HyperliquidNormalizer,
    subscriptions: &[HyperliquidSubscription],
    output: &mpsc::Sender<TransportOutput>,
    shutdown: &mut watch::Receiver<bool>,
    attempt: u32,
) -> Result<ConnectionEnd, TransportError> {
    let connect = tokio_tungstenite::connect_async(config.endpoint.as_str());
    let (mut socket, response) = tokio::time::timeout(config.connect_timeout, connect)
        .await
        .map_err(|_| TransportError::ConnectTimeout)?
        .map_err(|_| TransportError::ConnectFailed)?;
    if response.status() != 101 {
        return Err(TransportError::HandshakeRejected);
    }
    for subscription in subscriptions {
        socket
            .send(Message::Text(
                serde_json::to_string(&subscription.wire_message())
                    .expect("subscription is serializable")
                    .into(),
            ))
            .await
            .map_err(|_| TransportError::SendFailed)?;
    }
    let mut pending_subscriptions = subscriptions
        .iter()
        .map(|subscription| (subscription.coin.clone(), subscription.kind))
        .collect::<BTreeSet<_>>();
    let mut subscriptions_ready_reported = false;
    try_output(output, TransportOutput::Connected { attempt })?;
    loop {
        let heartbeat = tokio::time::sleep(config.heartbeat_interval);
        tokio::pin!(heartbeat);
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = socket.close(None).await;
                    return Ok(ConnectionEnd::Shutdown);
                }
            }
            message = socket.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        if text.len() > config.maximum_frame_bytes {
                            let _ = socket.close(None).await;
                            return Err(TransportError::FrameTooLarge);
                        }
                        let received_at = Utc::now();
                        let raw_sha256 = hex::encode(Sha256::digest(text.as_bytes()));
                        let raw = serde_json::from_str::<Value>(&text)
                            .map_err(|_| TransportError::InvalidJson)?;
                        if handle_control_frame(&raw, &mut pending_subscriptions)? {
                            if pending_subscriptions.is_empty() && !subscriptions_ready_reported {
                                try_output(output, TransportOutput::SubscriptionsReady { attempt })?;
                                subscriptions_ready_reported = true;
                            }
                            continue;
                        }
                        let events = normalizer
                            .normalize_with_raw_sha256(raw, received_at, &raw_sha256)
                            .map_err(TransportError::Normalization)?;
                        try_output(output, TransportOutput::Events { attempt, events })?;
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        socket.send(Message::Pong(payload)).await
                            .map_err(|_| TransportError::SendFailed)?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => return Ok(ConnectionEnd::Disconnected),
                    Some(Ok(Message::Binary(_))) => {
                        let _ = socket.close(None).await;
                        return Err(TransportError::UnexpectedBinaryFrame);
                    }
                    Some(Ok(Message::Frame(_))) => {
                        let _ = socket.close(None).await;
                        return Err(TransportError::InvalidFrame);
                    }
                    Some(Err(_)) => return Err(TransportError::ReadFailed),
                }
            }
            _ = &mut heartbeat => {
                socket.send(Message::Text(r#"{"method":"ping"}"#.into())).await
                    .map_err(|_| TransportError::SendFailed)?;
            }
        }
    }
}

fn try_output(
    output: &mpsc::Sender<TransportOutput>,
    message: TransportOutput,
) -> Result<(), TransportError> {
    output.try_send(message).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => TransportError::Backpressure,
        mpsc::error::TrySendError::Closed(_) => TransportError::ConsumerClosed,
    })
}

fn valid_subscription_set(subscriptions: &[HyperliquidSubscription]) -> bool {
    let mut unique = BTreeSet::new();
    subscriptions.iter().all(|subscription| {
        valid_coin(&subscription.coin)
            && subscription.coin == subscription.coin.to_ascii_uppercase()
            && unique.insert((subscription.coin.clone(), subscription.kind))
    })
}

fn handle_control_frame(
    raw: &Value,
    pending: &mut BTreeSet<(String, HyperliquidSubscriptionKind)>,
) -> Result<bool, TransportError> {
    let object = raw.as_object().ok_or(TransportError::InvalidControlFrame)?;
    match object.get("channel").and_then(Value::as_str) {
        Some("pong") => Ok(true),
        Some("subscriptionResponse") => {
            let data = object
                .get("data")
                .and_then(Value::as_object)
                .ok_or(TransportError::InvalidControlFrame)?;
            let subscription = data
                .get("subscription")
                .and_then(Value::as_object)
                .unwrap_or(data);
            let coin = subscription
                .get("coin")
                .and_then(Value::as_str)
                .filter(|coin| valid_coin(coin) && *coin == coin.to_ascii_uppercase())
                .ok_or(TransportError::InvalidControlFrame)?;
            let kind = match subscription.get("type").and_then(Value::as_str) {
                Some("l2Book") => HyperliquidSubscriptionKind::L2Book,
                Some("trades") => HyperliquidSubscriptionKind::Trades,
                Some("activeAssetCtx") => HyperliquidSubscriptionKind::ActiveAssetCtx,
                _ => return Err(TransportError::InvalidControlFrame),
            };
            if !pending.remove(&(coin.into(), kind)) {
                return Err(TransportError::InvalidControlFrame);
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransportError {
    #[error("Hyperliquid transport configuration is invalid")]
    InvalidConfig,
    #[error("Hyperliquid connection timed out")]
    ConnectTimeout,
    #[error("Hyperliquid connection failed")]
    ConnectFailed,
    #[error("Hyperliquid handshake was rejected")]
    HandshakeRejected,
    #[error("Hyperliquid subscription send failed")]
    SendFailed,
    #[error("Hyperliquid read failed")]
    ReadFailed,
    #[error("Hyperliquid frame exceeds the configured bound")]
    FrameTooLarge,
    #[error("Hyperliquid returned invalid JSON")]
    InvalidJson,
    #[error("Hyperliquid returned an unexpected binary frame")]
    UnexpectedBinaryFrame,
    #[error("Hyperliquid returned an invalid frame")]
    InvalidFrame,
    #[error("Hyperliquid returned an invalid or duplicate control frame")]
    InvalidControlFrame,
    #[error("Hyperliquid normalization failed: {0}")]
    Normalization(RealtimeError),
    #[error("realtime consumer queue is full")]
    Backpressure,
    #[error("realtime consumer is closed")]
    ConsumerClosed,
    #[error("bounded Hyperliquid reconnect budget was exhausted")]
    ReconnectExhausted,
}

impl TransportError {
    pub fn reason_code(&self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_config",
            Self::ConnectTimeout => "connect_timeout",
            Self::ConnectFailed => "connect_failed",
            Self::HandshakeRejected => "handshake_rejected",
            Self::SendFailed => "subscription_send_failed",
            Self::ReadFailed => "read_failed",
            Self::FrameTooLarge => "frame_too_large",
            Self::InvalidJson => "invalid_json",
            Self::UnexpectedBinaryFrame => "unexpected_binary_frame",
            Self::InvalidFrame => "invalid_frame",
            Self::InvalidControlFrame => "invalid_control_frame",
            Self::Normalization(_) => "normalization_failed",
            Self::Backpressure => "backpressure",
            Self::ConsumerClosed => "consumer_closed",
            Self::ReconnectExhausted => "reconnect_exhausted",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Quote,
    Trade,
    OrderBook,
    AssetContext,
    MarketState,
    StreamStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEvent {
    pub stream_id: String,
    pub sequence: u64,
    pub event: NormalizedProviderEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReplayFrame {
    Event { stream: Box<StreamEvent> },
    SequenceWatermark { stream_id: String, sequence: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionFilter {
    pub instruments: BTreeSet<String>,
    pub data_types: BTreeSet<DataType>,
}

impl SubscriptionFilter {
    pub fn matches(&self, event: &NormalizedProviderEvent) -> bool {
        self.instruments.contains(&event.instrument_id)
            && (matches!(&event.payload, ProviderPayload::StreamStatus { .. })
                || self.data_types.contains(&event.data_type()))
    }
}

/// In-memory sequence and bounded replay state owned by `DurableRealtimeWriter`. Its mutation API
/// is crate-private so a caller cannot bypass the append-and-sync boundary. Rejected/duplicate
/// decisions never consume a public sequence.
#[derive(Debug)]
pub struct SequencedReplay {
    stream_id: String,
    replay_capacity: usize,
    sequence: u64,
    recent_event_ids: BTreeSet<String>,
    recent_event_order: VecDeque<String>,
    replay: VecDeque<StreamEvent>,
}

impl SequencedReplay {
    pub fn new(
        stream_id: impl Into<String>,
        replay_capacity: usize,
    ) -> Result<Self, RealtimeError> {
        let stream_id = stream_id.into();
        if stream_id.is_empty() || replay_capacity == 0 {
            return Err(RealtimeError::InvalidConfig);
        }
        Ok(Self {
            stream_id,
            replay_capacity,
            sequence: 0,
            recent_event_ids: BTreeSet::new(),
            recent_event_order: VecDeque::new(),
            replay: VecDeque::new(),
        })
    }

    pub(crate) fn commit_persisted(
        &mut self,
        event: NormalizedProviderEvent,
        sequence: u64,
    ) -> Result<StreamEvent, RealtimeError> {
        if self.recent_event_ids.contains(&event.event_id) {
            return Err(RealtimeError::DuplicateCommit);
        }
        let expected = self
            .sequence
            .checked_add(1)
            .ok_or(RealtimeError::SequenceOverflow)?;
        if sequence != expected {
            return Err(RealtimeError::SequenceMismatch {
                expected,
                actual: sequence,
            });
        }
        let stream = StreamEvent {
            stream_id: self.stream_id.clone(),
            sequence,
            event,
        };
        self.sequence = sequence;
        self.recent_event_ids.insert(stream.event.event_id.clone());
        self.recent_event_order
            .push_back(stream.event.event_id.clone());
        self.replay.push_back(stream.clone());
        while self.replay.len() > self.replay_capacity {
            self.replay.pop_front();
            if let Some(event_id) = self.recent_event_order.pop_front() {
                self.recent_event_ids.remove(&event_id);
            }
        }
        Ok(stream)
    }

    pub(crate) fn snapshot(&self) -> (String, usize, u64, Vec<StreamEvent>) {
        (
            self.stream_id.clone(),
            self.replay_capacity,
            self.sequence,
            self.replay.iter().cloned().collect(),
        )
    }

    pub(crate) fn restore_tail(
        stream_id: String,
        replay_capacity: usize,
        sequence: u64,
        events: Vec<StreamEvent>,
    ) -> Result<Self, RealtimeError> {
        if events.len() > replay_capacity
            || events
                .last()
                .map_or(sequence != 0, |event| event.sequence != sequence)
            || events.windows(2).any(|pair| {
                pair[0].sequence.checked_add(1) != Some(pair[1].sequence)
                    || pair[0].stream_id != stream_id
                    || pair[1].stream_id != stream_id
            })
            || events
                .first()
                .is_some_and(|event| event.stream_id != stream_id || event.sequence == 0)
        {
            return Err(RealtimeError::InvalidReplaySnapshot);
        }
        let recent_event_ids = events
            .iter()
            .map(|event| event.event.event_id.clone())
            .collect::<BTreeSet<_>>();
        if recent_event_ids.len() != events.len() {
            return Err(RealtimeError::InvalidReplaySnapshot);
        }
        Ok(Self {
            stream_id,
            replay_capacity,
            sequence,
            recent_event_order: events
                .iter()
                .map(|event| event.event.event_id.clone())
                .collect(),
            recent_event_ids,
            replay: events.into(),
        })
    }

    pub fn replay_after(
        &self,
        stream_id: &str,
        after: u64,
        filter: &SubscriptionFilter,
    ) -> Result<Vec<ReplayFrame>, RealtimeError> {
        if stream_id != self.stream_id {
            return Err(RealtimeError::StreamChanged);
        }
        if after > self.sequence {
            return Err(RealtimeError::CursorAhead {
                current_sequence: self.sequence,
            });
        }
        let earliest = self
            .replay
            .front()
            .map_or(self.sequence.saturating_add(1), |event| event.sequence);
        if after.saturating_add(1) < earliest {
            return Err(RealtimeError::GapUnrecoverable {
                earliest_sequence: earliest,
            });
        }
        Ok(self
            .replay
            .iter()
            .filter(|event| event.sequence > after)
            .map(|event| {
                if filter.matches(&event.event) {
                    ReplayFrame::Event {
                        stream: Box::new(event.clone()),
                    }
                } else {
                    ReplayFrame::SequenceWatermark {
                        stream_id: self.stream_id.clone(),
                        sequence: event.sequence,
                    }
                }
            })
            .collect())
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RealtimeError {
    #[error("realtime configuration is invalid")]
    InvalidConfig,
    #[error("raw frame must be an object")]
    FrameNotObject,
    #[error("unsupported Hyperliquid channel: {0}")]
    UnsupportedChannel(String),
    #[error("required field is missing: {0}")]
    MissingField(&'static str),
    #[error("provider payload is invalid")]
    InvalidPayload,
    #[error("raw frame evidence hash is invalid")]
    InvalidRawEvidence,
    #[error("financial decimals must be JSON strings")]
    FinancialDecimalMustBeString,
    #[error("financial decimal is invalid")]
    InvalidDecimal,
    #[error("source timestamp is invalid or outside the accepted delay window")]
    InvalidTimestamp,
    #[error("provider symbol has no canonical instrument mapping")]
    UnknownInstrument,
    #[error("order book is crossed or locked")]
    CrossedBook,
    #[error("order book exceeds the supported depth")]
    BookTooDeep,
    #[error("provider batch is empty")]
    EmptyBatch,
    #[error("stream identity changed; full sync is required")]
    StreamChanged,
    #[error("consumer cursor is ahead of current sequence {current_sequence}")]
    CursorAhead { current_sequence: u64 },
    #[error("replay gap is unrecoverable; earliest sequence is {earliest_sequence}")]
    GapUnrecoverable { earliest_sequence: u64 },
    #[error("stream sequence overflow")]
    SequenceOverflow,
    #[error("persisted event duplicates current replay state")]
    DuplicateCommit,
    #[error("persisted sequence mismatch: expected {expected}, got {actual}")]
    SequenceMismatch { expected: u64, actual: u64 },
    #[error("replay snapshot is invalid")]
    InvalidReplaySnapshot,
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

fn object_field<'a>(
    object: &'a Map<String, Value>,
    field: &'static str,
) -> Result<&'a Map<String, Value>, RealtimeError> {
    required(object, field)?
        .as_object()
        .ok_or(RealtimeError::InvalidPayload)
}

fn value_identifier(value: &Value) -> Result<String, RealtimeError> {
    match value {
        Value::String(value) if !value.is_empty() => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(RealtimeError::InvalidPayload),
    }
}

fn two_sides(value: &Value) -> Result<(&[Value], &[Value]), RealtimeError> {
    let sides = value.as_array().ok_or(RealtimeError::InvalidPayload)?;
    if sides.len() != 2 {
        return Err(RealtimeError::InvalidPayload);
    }
    Ok((
        sides[0].as_array().ok_or(RealtimeError::InvalidPayload)?,
        sides[1].as_array().ok_or(RealtimeError::InvalidPayload)?,
    ))
}

fn book_sides(data: &Map<String, Value>) -> Result<(Vec<Value>, Vec<Value>), RealtimeError> {
    if let Some(levels) = data.get("levels") {
        let (bids, asks) = two_sides(levels)?;
        return Ok((bids.to_vec(), asks.to_vec()));
    }
    let bbo = data
        .get("bbo")
        .and_then(Value::as_array)
        .filter(|sides| sides.len() == 2)
        .ok_or(RealtimeError::MissingField("levels/bbo"))?;
    let side = |value: &Value| match value {
        Value::Null => Ok(Vec::new()),
        Value::Object(_) => Ok(vec![value.clone()]),
        _ => Err(RealtimeError::InvalidPayload),
    };
    Ok((side(&bbo[0])?, side(&bbo[1])?))
}

fn valid_coin(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'@' | b':' | b'/' | b'-' | b'_' | b'.')
        })
}

fn parse_levels(values: &[Value]) -> Result<Vec<BookLevel>, RealtimeError> {
    if values.len() > 20 {
        return Err(RealtimeError::BookTooDeep);
    }
    let mut levels = Vec::with_capacity(values.len());
    let mut previous: Option<Decimal> = None;
    for value in values {
        let row = value.as_object().ok_or(RealtimeError::InvalidPayload)?;
        let price = ExactDecimal::positive(required(row, "px")?)?;
        let size = ExactDecimal::positive(required(row, "sz")?)?;
        if previous == Some(price.0) {
            return Err(RealtimeError::InvalidPayload);
        }
        previous = Some(price.0);
        let order_count = match row.get("n") {
            None | Some(Value::Null) => None,
            Some(value) => Some(
                value
                    .as_u64()
                    .filter(|count| *count > 0)
                    .ok_or(RealtimeError::InvalidPayload)?,
            ),
        };
        levels.push(BookLevel {
            price,
            size,
            order_id: "0".into(),
            order_count,
        });
    }
    Ok(levels)
}

fn strictly_descending(levels: &[BookLevel]) -> bool {
    levels
        .windows(2)
        .all(|pair| pair[0].price.0 > pair[1].price.0)
}

fn strictly_ascending(levels: &[BookLevel]) -> bool {
    levels
        .windows(2)
        .all(|pair| pair[0].price.0 < pair[1].price.0)
}

fn optional_positive(value: Option<&Value>) -> Result<Option<ExactDecimal>, RealtimeError> {
    value
        .filter(|value| !value.is_null())
        .map(ExactDecimal::positive)
        .transpose()
}

fn optional_decimal(value: Option<&Value>) -> Result<Option<ExactDecimal>, RealtimeError> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let text = value
        .as_str()
        .ok_or(RealtimeError::FinancialDecimalMustBeString)?;
    let decimal = Decimal::from_str(text).map_err(|_| RealtimeError::InvalidDecimal)?;
    Ok(Some(ExactDecimal(decimal.normalize())))
}

fn timestamp(
    value: &Value,
    received_at: DateTime<Utc>,
    maximum_delay: i64,
) -> Result<DateTime<Utc>, RealtimeError> {
    let milliseconds = value.as_i64().ok_or(RealtimeError::InvalidTimestamp)?;
    let observed_at = Utc
        .timestamp_millis_opt(milliseconds)
        .single()
        .ok_or(RealtimeError::InvalidTimestamp)?;
    let delay = received_at
        .signed_duration_since(observed_at)
        .num_milliseconds();
    if delay < 0 || delay > maximum_delay {
        return Err(RealtimeError::InvalidTimestamp);
    }
    Ok(observed_at)
}

fn sha256(value: &Value) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_vec(value).expect("JSON values are serializable"),
    ))
}

fn valid_sha256_text(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn python_iso(value: DateTime<Utc>) -> String {
    if value.timestamp_subsec_micros() == 0 {
        value.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    } else {
        value.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn received() -> DateTime<Utc> {
        Utc.timestamp_millis_opt(1_700_000_000_100).unwrap()
    }

    fn normalizer() -> HyperliquidNormalizer {
        HyperliquidNormalizer::new([("BTC-PERP.HYPL".into(), "BTC".into())], 1_000).unwrap()
    }

    #[test]
    fn python_hyperliquid_golden_is_normalized_with_exact_decimals() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/hyperliquid-realtime-normalizer-v1.json"
        ))
        .unwrap();
        let received_at = DateTime::parse_from_rfc3339(fixture["received_at"].as_str().unwrap())
            .unwrap()
            .with_timezone(&Utc);
        for case in fixture["cases"].as_array().unwrap() {
            let actual = normalizer()
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

        let book = normalizer()
            .normalize(
                json!({
                    "channel":"l2Book",
                    "data":{
                        "coin":"BTC","time":1700000000000_i64,
                        "levels":[
                            [{"px":"64999.5","sz":"1.2","n":2}],
                            [{"px":"65000.5","sz":"0.8","n":1}]
                        ]
                    }
                }),
                received(),
            )
            .unwrap();
        assert_eq!(book.len(), 2);
        assert_eq!(book[0].data_type(), DataType::OrderBook);
        assert_eq!(book[1].data_type(), DataType::Quote);
        let ProviderPayload::Quote {
            bid_price,
            ask_price,
            ..
        } = &book[1].payload
        else {
            panic!("expected quote")
        };
        assert_eq!(bid_price.0.to_string(), "64999.5");
        assert_eq!(ask_price.0.to_string(), "65000.5");
        let bbo = normalizer()
            .normalize(
                json!({
                    "channel":"bbo",
                    "data":{"coin":"BTC","time":1700000000000_i64,"bbo":[
                        {"px":"64999.5","sz":"1.2"},
                        {"px":"65000.5","sz":"0.8"}
                    ]}
                }),
                received(),
            )
            .unwrap();
        assert_eq!(bbo.len(), 2);
        assert_eq!(bbo[1].public_contract_value()["event_type"], "quote");

        let trade = normalizer()
            .normalize(
                json!({
                    "channel":"trades",
                    "data":[{"coin":"BTC","time":1700000000001_i64,
                        "px":"65000","sz":"0.1","side":"B","tid":42}]
                }),
                received(),
            )
            .unwrap();
        let ProviderPayload::Trade {
            trade_id,
            aggressor_side,
            ..
        } = &trade[0].payload
        else {
            panic!("expected trade")
        };
        assert_eq!(trade_id, "42");
        assert_eq!(*aggressor_side, AggressorSide::Buyer);

        let context = normalizer()
            .normalize(
                json!({
                    "channel":"activeAssetCtx",
                    "data":{"coin":"BTC","time":1700000000002_i64,"ctx":{
                        "markPx":"65001","oraclePx":"65000",
                        "funding":"0.00001","openInterest":"100"
                    }}
                }),
                received(),
            )
            .unwrap();
        let ProviderPayload::AssetContext {
            oracle_status,
            funding_rate,
            ..
        } = &context[0].payload
        else {
            panic!("expected context")
        };
        assert_eq!(oracle_status, "internal_only");
        assert_eq!(funding_rate.as_ref().unwrap().0.to_string(), "0.00001");
    }

    #[test]
    fn invalid_financial_or_identity_input_fails_closed() {
        let numeric_decimal = json!({
            "channel":"trades",
            "data":[{"coin":"BTC","time":1700000000001_i64,
                "px":65000.0,"sz":"0.1","side":"B","tid":42}]
        });
        assert_eq!(
            normalizer()
                .normalize(numeric_decimal, received())
                .unwrap_err(),
            RealtimeError::FinancialDecimalMustBeString
        );
        let stale = json!({
            "channel":"trades",
            "data":[{"coin":"BTC","time":1699999990000_i64,
                "px":"65000","sz":"0.1","side":"B","tid":42}]
        });
        assert_eq!(
            normalizer().normalize(stale, received()).unwrap_err(),
            RealtimeError::InvalidTimestamp
        );
        let unknown = json!({
            "channel":"trades",
            "data":[{"coin":"ETH","time":1700000000001_i64,
                "px":"3500","sz":"0.1","side":"B","tid":42}]
        });
        assert_eq!(
            normalizer().normalize(unknown, received()).unwrap_err(),
            RealtimeError::UnknownInstrument
        );
        assert_eq!(
            normalizer()
                .normalize_with_raw_sha256(json!({"channel":"pong"}), received(), "A")
                .unwrap_err(),
            RealtimeError::InvalidRawEvidence
        );
    }

    #[test]
    fn transport_supplied_hash_preserves_exact_wire_bytes() {
        let wire = r#"{ "channel": "trades", "data": [{"coin":"BTC","time":1700000000001,"px":"65000","sz":"0.1","side":"B","tid":42}] }"#;
        let expected = hex::encode(Sha256::digest(wire.as_bytes()));
        let event = normalizer()
            .normalize_with_raw_sha256(serde_json::from_str(wire).unwrap(), received(), &expected)
            .unwrap()
            .remove(0);
        assert_eq!(event.raw_sha256, expected);
        assert_ne!(
            event.raw_sha256,
            sha256(&serde_json::from_str(wire).unwrap())
        );
    }

    #[test]
    fn replay_state_rejects_duplicate_commit_without_consuming_sequence() {
        let event = normalizer()
            .normalize(
                json!({
                    "channel":"trades",
                    "data":[{"coin":"BTC","time":1700000000001_i64,
                        "px":"65000","sz":"0.1","side":"B","tid":42}]
                }),
                received(),
            )
            .unwrap()
            .remove(0);
        let mut replay = SequencedReplay::new("stream-1", 2).unwrap();
        assert_eq!(
            replay.commit_persisted(event.clone(), 1).unwrap().sequence,
            1
        );
        assert_eq!(
            replay.commit_persisted(event, 2).unwrap_err(),
            RealtimeError::DuplicateCommit
        );
        assert_eq!(replay.sequence(), 1);
    }

    #[test]
    fn filtered_replay_is_contiguous_and_expired_gap_is_explicit() {
        let mut replay = SequencedReplay::new("stream-1", 2).unwrap();
        let frames = [
            ("BTC", "65000", 1_700_000_000_001_i64),
            ("BTC", "65001", 1_700_000_000_002_i64),
            ("BTC", "65002", 1_700_000_000_003_i64),
        ];
        for (coin, price, time) in frames {
            let event = normalizer()
                .normalize(
                    json!({"channel":"trades","data":[{
                        "coin":coin,"time":time,"px":price,"sz":"0.1",
                        "side":"B","tid":time
                    }]}),
                    received(),
                )
                .unwrap()
                .remove(0);
            let sequence = replay.sequence().checked_add(1).unwrap();
            replay.commit_persisted(event, sequence).unwrap();
        }
        let filter = SubscriptionFilter {
            instruments: BTreeSet::from(["OTHER.HYPL".into()]),
            data_types: BTreeSet::from([DataType::Trade]),
        };
        assert_eq!(
            replay.replay_after("stream-1", 0, &filter).unwrap_err(),
            RealtimeError::GapUnrecoverable {
                earliest_sequence: 2
            }
        );
        let resumed = replay.replay_after("stream-1", 1, &filter).unwrap();
        assert_eq!(resumed.len(), 2);
        assert!(matches!(
            resumed[0],
            ReplayFrame::SequenceWatermark { sequence: 2, .. }
        ));
        assert_eq!(
            replay.replay_after("old-stream", 1, &filter).unwrap_err(),
            RealtimeError::StreamChanged
        );
    }

    #[test]
    fn subscriptions_are_deterministic_unambiguous_and_read_only() {
        let mappings = BTreeMap::from([
            ("BTC-PERP.HYPL".into(), "btc".into()),
            ("ETH-PERP.HYPL".into(), "ETH".into()),
        ]);
        let subscriptions = hyperliquid_subscriptions(
            &mappings,
            &BTreeSet::from([DataType::Quote, DataType::Trade, DataType::OrderBook]),
        )
        .unwrap();
        assert_eq!(subscriptions.len(), 4);
        assert_eq!(subscriptions[0].coin, "BTC");
        assert_eq!(subscriptions[0].kind, HyperliquidSubscriptionKind::L2Book);
        assert_eq!(subscriptions[1].kind, HyperliquidSubscriptionKind::Trades);
        assert_eq!(subscriptions[2].coin, "ETH");
        for subscription in subscriptions {
            let wire = subscription.wire_message();
            assert_eq!(wire["method"], "subscribe");
            assert!(wire.get("order").is_none());
            assert!(wire.get("signature").is_none());
        }
        assert!(HyperliquidTransportConfig::production().validate().is_ok());
        let mut invalid = HyperliquidTransportConfig::production();
        invalid.endpoint = Url::parse("wss://example.com/ws").unwrap();
        assert_eq!(
            invalid.validate().unwrap_err(),
            TransportError::InvalidConfig
        );
        assert_eq!(
            hyperliquid_subscriptions(
                &BTreeMap::from([
                    ("BTC-PERP.HYPL".into(), "BTC".into()),
                    ("BTC-SPOT.HYPL".into(), "btc".into()),
                ]),
                &BTreeSet::from([DataType::Trade]),
            )
            .unwrap_err(),
            RealtimeError::InvalidConfig
        );
    }

    #[tokio::test]
    async fn local_websocket_reconnects_resubscribes_and_fails_closed_on_bad_frame() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for attempt in 1..=2 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                let subscription = socket.next().await.unwrap().unwrap();
                let Message::Text(text) = subscription else {
                    panic!("expected text subscription")
                };
                let value: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(value["method"], "subscribe");
                assert_eq!(value["subscription"]["type"], "trades");
                assert_eq!(value["subscription"]["coin"], "BTC");
                socket
                    .send(Message::Text(
                        r#"{"channel":"subscriptionResponse","data":{"type":"trades","coin":"BTC"}}"#
                            .into(),
                    ))
                    .await
                    .unwrap();
                let price = if attempt == 1 {
                    Value::String("65000.1".into())
                } else {
                    serde_json::json!(65000.1)
                };
                socket
                    .send(Message::Text(
                        serde_json::to_string(&serde_json::json!({
                            "channel":"trades",
                            "data":[{"coin":"BTC","time":Utc::now().timestamp_millis(),
                                "px":price,"sz":"0.1","side":"B","tid":attempt}]
                        }))
                        .unwrap()
                        .into(),
                    ))
                    .await
                    .unwrap();
                if attempt == 1 {
                    socket.close(None).await.unwrap();
                } else {
                    let _ = socket.next().await;
                }
            }
        });
        let endpoint = Url::parse(&format!("ws://{address}/ws")).unwrap();
        let config = HyperliquidTransportConfig::loopback(endpoint);
        let normalizer =
            HyperliquidNormalizer::new([("BTC-PERP.HYPL".into(), "BTC".into())], 5_000).unwrap();
        let subscriptions = hyperliquid_subscriptions(
            &BTreeMap::from([("BTC-PERP.HYPL".into(), "BTC".into())]),
            &BTreeSet::from([DataType::Trade]),
        )
        .unwrap();
        let (sender, mut receiver) = mpsc::channel(16);
        let (_shutdown_sender, shutdown) = watch::channel(false);
        assert_eq!(
            run_hyperliquid_transport(config, normalizer, subscriptions, sender, shutdown)
                .await
                .unwrap_err(),
            TransportError::ReconnectExhausted
        );
        server.await.unwrap();
        let mut outputs = Vec::new();
        while let Ok(output) = receiver.try_recv() {
            outputs.push(output);
        }
        assert!(matches!(
            outputs[0],
            TransportOutput::Connected { attempt: 1 }
        ));
        assert!(matches!(
            outputs[1],
            TransportOutput::SubscriptionsReady { attempt: 1 }
        ));
        assert!(matches!(
            outputs[2],
            TransportOutput::Events { attempt: 1, .. }
        ));
        assert!(matches!(
            outputs[3],
            TransportOutput::Degraded {
                attempt: 1,
                ref reason,
                retryable: true
            } if reason == "upstream_disconnected"
        ));
        assert!(matches!(
            outputs[4],
            TransportOutput::Connected { attempt: 2 }
        ));
        assert!(matches!(
            outputs[5],
            TransportOutput::SubscriptionsReady { attempt: 2 }
        ));
        assert!(matches!(
            outputs[6],
            TransportOutput::Degraded {
                attempt: 2,
                ref reason,
                retryable: false
            } if reason == "normalization_failed"
        ));
    }

    #[tokio::test]
    async fn full_consumer_queue_stops_transport_without_reconnect_or_loss() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(
                    r#"{"channel":"subscriptionResponse","data":{"type":"trades","coin":"BTC"}}"#
                        .into(),
                ))
                .await
                .unwrap();
            let _ = socket.next().await;
        });
        let config = HyperliquidTransportConfig::loopback(
            Url::parse(&format!("ws://{address}/ws")).unwrap(),
        );
        let normalizer =
            HyperliquidNormalizer::new([("BTC-PERP.HYPL".into(), "BTC".into())], 5_000).unwrap();
        let subscriptions = hyperliquid_subscriptions(
            &BTreeMap::from([("BTC-PERP.HYPL".into(), "BTC".into())]),
            &BTreeSet::from([DataType::Trade]),
        )
        .unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        let (_shutdown_sender, shutdown) = watch::channel(false);
        assert_eq!(
            run_hyperliquid_transport(config, normalizer, subscriptions, sender, shutdown)
                .await
                .unwrap_err(),
            TransportError::Backpressure
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn application_heartbeat_matches_official_ping_pong_contract() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(
                    r#"{"channel":"subscriptionResponse","data":{"type":"trades","coin":"BTC"}}"#
                        .into(),
                ))
                .await
                .unwrap();
            let Message::Text(ping) = socket.next().await.unwrap().unwrap() else {
                panic!("expected application heartbeat")
            };
            assert_eq!(
                serde_json::from_str::<Value>(&ping).unwrap(),
                json!({"method":"ping"})
            );
            socket
                .send(Message::Text(r#"{"channel":"pong"}"#.into()))
                .await
                .unwrap();
            socket.close(None).await.unwrap();
        });
        let mut config = HyperliquidTransportConfig::loopback(
            Url::parse(&format!("ws://{address}/ws")).unwrap(),
        );
        config.maximum_reconnect_attempts = 1;
        config.heartbeat_interval = Duration::from_millis(10);
        let normalizer =
            HyperliquidNormalizer::new([("BTC-PERP.HYPL".into(), "BTC".into())], 5_000).unwrap();
        let subscriptions = hyperliquid_subscriptions(
            &BTreeMap::from([("BTC-PERP.HYPL".into(), "BTC".into())]),
            &BTreeSet::from([DataType::Trade]),
        )
        .unwrap();
        let (sender, _receiver) = mpsc::channel(8);
        let (_shutdown_sender, shutdown) = watch::channel(false);
        assert_eq!(
            run_hyperliquid_transport(config, normalizer, subscriptions, sender, shutdown)
                .await
                .unwrap_err(),
            TransportError::ReconnectExhausted
        );
        server.await.unwrap();
    }
}
