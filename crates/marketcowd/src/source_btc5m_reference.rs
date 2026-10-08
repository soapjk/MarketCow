//! Venue-published BTC five-minute opening reference, separate from chart prices.
//!
//! This is a bounded read surface inside the existing MarketCow process.  A
//! window reference is never inferred from a Binance candle or a nearby TWAP
//! tick.  The Polymarket window response is accepted only after the matching
//! Gamma market has declared the exact Chainlink 60-second TWAP rule.
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{StatusCode, header::RETRY_AFTER},
    response::IntoResponse,
    routing::get,
};
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

use crate::{
    btc_trade,
    source_btc5m_api::{Control, MarketBinding},
};
use marketcow_polymarket::discovery_source::canonical_hash;

const SCHEMA: &str = "marketcow.btc5m.reference-window.v1";
const RESOLUTION_SOURCE: &str = "https://data.chain.link/streams/btc-usd-twap-60s-streams";
const MAX_GAMMA_BYTES: usize = 512 * 1024;
const MAX_REFERENCE_BYTES: usize = 16 * 1024;

#[derive(Clone)]
struct Service {
    control: Control,
    trade: Option<btc_trade::Service>,
    client: reqwest::Client,
    // Only the two current profile markets can be requested. Holding this
    // independent lock over the bounded HTTP reads coalesces duplicate reads;
    // it never participates in market publication or WebSocket processing.
    cache: Arc<Mutex<BTreeMap<(String, String), Cached>>>,
}

struct Cached {
    valid_until: Instant,
    result: ReferenceResult<Value>,
}

type ReferenceResult<T> = std::result::Result<T, ReferenceFailure>;

#[derive(Clone, Debug)]
struct ReferenceFailure {
    code: &'static str,
    detail: String,
    retry_after: Option<Duration>,
    cache_for: Duration,
}

impl ReferenceFailure {
    fn new(code: &'static str, detail: impl Into<String>, cache_for: Duration) -> Self {
        Self {
            code,
            detail: detail.into(),
            retry_after: None,
            cache_for,
        }
    }

    fn body(&self) -> Value {
        json!({"available":false,"reason_code":self.code,"detail":self.detail,
            "retry_after_ms":self.retry_after.map(|delay| delay.as_millis() as u64)})
    }
}

impl fmt::Display for ReferenceFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReferenceQuery {
    market_id: String,
}

// Preserve the exact upstream JSON decimal token. Going through Value::Number
// would convert an official threshold to f64 before it can be audited.
#[derive(Deserialize)]
struct ExactReference {
    #[serde(rename = "openPrice")]
    open_price: Box<RawValue>,
}

#[derive(Deserialize)]
struct ExactGammaPrice {
    #[serde(rename = "eventMetadata")]
    event_metadata: Option<ExactGammaMetadata>,
}

#[derive(Deserialize)]
struct ExactGammaMetadata {
    #[serde(rename = "priceToBeat")]
    price_to_beat: Option<Box<RawValue>>,
}

pub fn router(control: Control, trade: Option<btc_trade::Service>) -> Result<Router> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()?;
    let service = Service {
        control,
        trade,
        client,
        cache: Arc::new(Mutex::new(BTreeMap::new())),
    };
    Ok(Router::new()
        .route(
            "/v1/prediction-markets/polymarket/research/btc-5m/reference",
            get(http_reference),
        )
        .route(
            "/v1/prediction-markets/polymarket/research/btc-5m/reference-comparison",
            get(http_comparison),
        )
        .route(
            "/v1/prediction-markets/polymarket/research/btc-5m/binance-comparison",
            get(http_binance_comparison),
        )
        .with_state(service))
}

async fn http_binance_comparison(
    State(service): State<Service>,
    Query(query): Query<ReferenceQuery>,
) -> impl IntoResponse {
    match service.binance_only(&query.market_id).await {
        Ok(value) => (StatusCode::OK, Json(value)),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(
                json!({"schema_version":"marketcow.btc5m.binance-reference-window.v1",
                "available":false,"market_id":query.market_id,
                "reason":format!("{error:#}")}),
            ),
        ),
    }
}

async fn http_comparison(
    State(service): State<Service>,
    Query(query): Query<ReferenceQuery>,
) -> impl IntoResponse {
    match service.comparison(&query.market_id).await {
        Ok(value) => (StatusCode::OK, Json(value)),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(
                json!({"schema_version":"marketcow.btc5m.reference-comparison.v2",
                "available":false,"market_id":query.market_id,
                "reason":format!("{error:#}")}),
            ),
        ),
    }
}

async fn http_reference(
    State(service): State<Service>,
    Query(query): Query<ReferenceQuery>,
) -> impl IntoResponse {
    match service.reference(&query.market_id).await {
        Ok(value) => (StatusCode::OK, Json(value)),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"schema_version":SCHEMA,"available":false,
                "market_id":query.market_id,"failure":error.body()})),
        ),
    }
}

impl Service {
    async fn binance_only(&self, market_id: &str) -> Result<Value> {
        let before = self.control.status()?;
        let profile = self.control.profile()?;
        ensure!(
            before["profile_id"] == profile.profile_id
                && before["profile_revision"] == profile.profile_revision,
            "BTC five-minute profile changed before Binance comparison read"
        );
        let binding = profile
            .markets
            .iter()
            .find(|market| market.market_id == market_id)
            .context("market is not in current BTC five-minute profile")?;
        let start_ms = DateTime::parse_from_rfc3339(&binding.start_at)?.timestamp_millis();
        let end_ms = DateTime::parse_from_rfc3339(&binding.end_at)?.timestamp_millis();
        let binance = match &self.trade {
            Some(trade) => trade.window_comparison(start_ms, end_ms, None).await,
            None => json!({"schema_version":"marketcow.btc5m.binance-comparison.v1",
                "source":"binance_spot_trade","available":false,
                "reason":"trade_source_not_configured"}),
        };
        self.require_same_profile(&before)?;
        Ok(
            json!({"schema_version":"marketcow.btc5m.binance-reference-window.v1",
            "market_id":market_id,"condition_id":binding.condition_id,
            "window_id":binding.window_id,"start_at":binding.start_at,
            "end_at":binding.end_at,"profile_epoch":before["epoch"],
            "profile_revision":profile.profile_revision,"rule_sha256":binding.rule_sha256,
            "binance_comparison":binance,"binance_is_settlement_source":false,
            "observed_at":Utc::now().to_rfc3339()}),
        )
    }

    async fn comparison(&self, market_id: &str) -> Result<Value> {
        let before = self.control.status()?;
        let profile = self.control.profile()?;
        ensure!(
            before["profile_id"] == profile.profile_id
                && before["profile_revision"] == profile.profile_revision,
            "BTC five-minute profile changed before comparison read"
        );
        let binding = profile
            .markets
            .iter()
            .find(|market| market.market_id == market_id)
            .context("market is not in current BTC five-minute profile")?;
        let start_ms = DateTime::parse_from_rfc3339(&binding.start_at)?.timestamp_millis();
        let end_ms = DateTime::parse_from_rfc3339(&binding.end_at)?.timestamp_millis();
        let official_future = self.reference(market_id);
        let binance_future = async {
            match &self.trade {
                Some(trade) => trade.window_comparison(start_ms, end_ms, None).await,
                None => json!({"schema_version":"marketcow.btc5m.binance-comparison.v1",
                    "source":"binance_spot_trade","available":false,
                    "reason":"trade_source_not_configured"}),
            }
        };
        let (official, mut binance) = tokio::join!(official_future, binance_future);
        let official_price = official
            .as_ref()
            .ok()
            .and_then(|value| value["price_to_beat"].as_str())
            .and_then(|value| value.parse::<rust_decimal::Decimal>().ok());
        add_official_mapping(&mut binance, official_price);
        self.require_same_profile(&before)?;
        let official = match official {
            Ok(value) => json!({"available":true,"price_to_beat":value["price_to_beat"],
                "source":value["source"],"verification":value["verification"],
                "reference_raw_sha256":value["reference_raw_sha256"],
                "gamma_raw_sha256":value["gamma_raw_sha256"],
                "observed_at":value["observed_at"]}),
            Err(error) => error.body(),
        };
        Ok(
            json!({"schema_version":"marketcow.btc5m.reference-comparison.v2",
            "market_id":market_id,"condition_id":binding.condition_id,
            "window_id":binding.window_id,"start_at":binding.start_at,
            "end_at":binding.end_at,"profile_epoch":before["epoch"],
            "profile_revision":profile.profile_revision,
            "rule_sha256":binding.rule_sha256,
            "official_reference":official,
            "binance_comparison":binance,
            "binance_is_settlement_source":false,
            "observed_at":Utc::now().to_rfc3339()}),
        )
    }

    async fn reference(&self, market_id: &str) -> ReferenceResult<Value> {
        let source_failure = |error: anyhow::Error| {
            ReferenceFailure::new(
                "source_profile_unavailable",
                format!("{error:#}"),
                Duration::ZERO,
            )
        };
        let before = self.control.status().map_err(source_failure)?;
        let epoch = before["epoch"].as_str().ok_or_else(|| {
            ReferenceFailure::new(
                "source_profile_unavailable",
                "profile epoch missing",
                Duration::ZERO,
            )
        })?;
        let profile = self.control.profile().map_err(source_failure)?;
        if before["profile_id"] != profile.profile_id
            || before["profile_revision"] != profile.profile_revision
        {
            return Err(ReferenceFailure::new(
                "profile_changed",
                "BTC five-minute profile changed before reference read",
                Duration::ZERO,
            ));
        }
        let binding = profile
            .markets
            .iter()
            .find(|market| market.market_id == market_id)
            .ok_or_else(|| {
                ReferenceFailure::new(
                    "market_not_in_profile",
                    "market is not in current BTC five-minute profile",
                    Duration::ZERO,
                )
            })?;
        let start = DateTime::parse_from_rfc3339(&binding.start_at).map_err(|error| {
            ReferenceFailure::new("source_profile_invalid", error.to_string(), Duration::ZERO)
        })?;
        if Utc::now() < start {
            return Err(ReferenceFailure::new(
                "window_not_started",
                "window has not started",
                Duration::ZERO,
            ));
        }
        let key = (epoch.to_owned(), market_id.to_owned());
        let mut cache = self.cache.lock().await;
        cache.retain(|(cached_epoch, _), _| cached_epoch == epoch);
        if let Some(cached) = cache.get(&key) {
            if Instant::now() < cached.valid_until {
                self.require_same_profile(&before).map_err(|error| {
                    ReferenceFailure::new("profile_changed", error.to_string(), Duration::ZERO)
                })?;
                return cached.result.clone();
            }
        }
        let result = self
            .fetch_window(
                binding,
                epoch,
                &profile.profile_id,
                profile.profile_revision,
            )
            .await;
        self.require_same_profile(&before).map_err(|error| {
            ReferenceFailure::new("profile_changed", error.to_string(), Duration::ZERO)
        })?;
        let lifetime = match &result {
            Ok(value) if value["verification"] == "gamma_matched" => 300,
            Ok(_) => 60,
            Err(_) => 0,
        };
        cache.insert(
            key,
            Cached {
                valid_until: Instant::now()
                    + result
                        .as_ref()
                        .map(|_| Duration::from_secs(lifetime))
                        .unwrap_or_else(|error| error.cache_for),
                result: result.clone(),
            },
        );
        result
    }

    fn require_same_profile(&self, before: &Value) -> Result<()> {
        let after = self.control.status()?;
        ensure!(
            after["epoch"] == before["epoch"]
                && after["profile_id"] == before["profile_id"]
                && after["profile_revision"] == before["profile_revision"],
            "BTC five-minute profile changed during reference read"
        );
        Ok(())
    }

    async fn fetch_window(
        &self,
        binding: &MarketBinding,
        epoch: &str,
        profile_id: &str,
        profile_revision: u64,
    ) -> ReferenceResult<Value> {
        let gamma_url = format!(
            "https://gamma-api.polymarket.com/events/slug/{}",
            binding.window_id
        );
        let gamma_raw = fetch_bounded(&self.client, &gamma_url, MAX_GAMMA_BYTES).await?;
        let parse_failure = |error: serde_json::Error| {
            ReferenceFailure::new(
                "invalid_upstream_payload",
                error.to_string(),
                Duration::from_secs(2),
            )
        };
        let identity_failure = |detail| {
            ReferenceFailure::new("identity_or_rule_mismatch", detail, Duration::from_secs(30))
        };
        let gamma: Value = serde_json::from_slice(&gamma_raw).map_err(parse_failure)?;
        if gamma["slug"] != binding.window_id {
            return Err(identity_failure("Gamma event slug mismatch"));
        }
        let market = gamma["markets"]
            .as_array()
            .ok_or_else(|| identity_failure("Gamma event markets missing"))?
            .iter()
            .find(|market| market["id"] == binding.market_id)
            .ok_or_else(|| identity_failure("Gamma event market identity missing"))?;
        if market["conditionId"] != binding.condition_id {
            return Err(identity_failure("Gamma condition identity mismatch"));
        }
        if market["eventStartTime"] != binding.start_at || market["endDate"] != binding.end_at {
            return Err(identity_failure("Gamma market window changed"));
        }
        if market["resolutionSource"] != RESOLUTION_SOURCE {
            return Err(identity_failure("Gamma resolution source changed"));
        }
        let description = market["description"]
            .as_str()
            .ok_or_else(|| identity_failure("Gamma market rules missing"))?;
        if canonical_hash(&json!({
            "description":description,"resolution_source":RESOLUTION_SOURCE,
            "start_at":binding.start_at,"end_at":binding.end_at,
            "outcomes":["Up","Down"],
        })) != binding.rule_sha256
        {
            return Err(identity_failure(
                "Gamma market rules differ from reviewed profile",
            ));
        }
        let config = &market["cryptoMarketConfig"];
        if !(config["id"] == "btc-5m-twap-60"
            && config["asset"] == "btc"
            && config["duration"] == "5m"
            && config["twapEnabled"] == true
            && config["twapLookbackSeconds"] == 60)
        {
            return Err(identity_failure(
                "Gamma Chainlink TWAP configuration changed",
            ));
        }

        let mut url = reqwest::Url::parse("https://polymarket.com/api/crypto/crypto-price")
            .map_err(|error| {
                ReferenceFailure::new("invalid_upstream_url", error.to_string(), Duration::ZERO)
            })?;
        url.query_pairs_mut()
            .append_pair("symbol", "BTC")
            .append_pair("eventStartTime", &binding.start_at)
            .append_pair("variant", "fiveminute")
            .append_pair("endDate", &binding.end_at)
            .append_pair("twapEnabled", "true")
            .append_pair("twapLookbackSeconds", "60");
        let reference_raw = fetch_bounded(&self.client, url.as_str(), MAX_REFERENCE_BYTES).await?;
        let reference: Value = serde_json::from_slice(&reference_raw).map_err(parse_failure)?;
        if reference.get("openPrice").is_none_or(Value::is_null) {
            return Err(ReferenceFailure::new(
                "official_not_published",
                "window openPrice has not been published",
                Duration::from_secs(2),
            ));
        }
        let exact: ExactReference =
            serde_json::from_slice(&reference_raw).map_err(parse_failure)?;
        let price: rust_decimal::Decimal = exact.open_price.get().parse().map_err(|error| {
            ReferenceFailure::new(
                "invalid_upstream_payload",
                format!("{error}"),
                Duration::from_secs(2),
            )
        })?;
        if price <= rust_decimal::Decimal::ZERO {
            return Err(ReferenceFailure::new(
                "invalid_upstream_payload",
                "window openPrice invalid",
                Duration::from_secs(2),
            ));
        }
        let exact_gamma: ExactGammaPrice =
            serde_json::from_slice(&gamma_raw).map_err(parse_failure)?;
        let verification = if let Some(gamma_price) = exact_gamma
            .event_metadata
            .and_then(|metadata| metadata.price_to_beat)
        {
            let expected: rust_decimal::Decimal = gamma_price.get().parse().map_err(|error| {
                ReferenceFailure::new(
                    "invalid_upstream_payload",
                    format!("{error}"),
                    Duration::from_secs(2),
                )
            })?;
            if price != expected {
                return Err(identity_failure(
                    "Gamma priceToBeat disagrees with window openPrice",
                ));
            }
            "gamma_matched"
        } else {
            "venue_window_bound"
        };
        Ok(json!({
            "schema_version":SCHEMA,"available":true,
            "market_id":binding.market_id,"condition_id":binding.condition_id,
            "window_id":binding.window_id,"start_at":binding.start_at,
            "end_at":binding.end_at,"profile_id":profile_id,
            "profile_revision":profile_revision,"profile_epoch":epoch,
            "rule_sha256":binding.rule_sha256,
            "resolution_source":RESOLUTION_SOURCE,
            "crypto_market_config":config,
            "price_to_beat":price.normalize().to_string(),
            "source":"polymarket_window_open_chainlink_twap60",
            "verification":verification,
            "gamma_url":gamma_url,"gamma_raw_sha256":hex::encode(Sha256::digest(&gamma_raw)),
            "reference_url":url.as_str(),
            "reference_raw_sha256":hex::encode(Sha256::digest(&reference_raw)),
            "reference_raw_base64":base64::engine::general_purpose::STANDARD.encode(&reference_raw),
            "source_response_timestamp_ms":reference["timestamp"],
            "observed_at":Utc::now().to_rfc3339(),
        }))
    }
}

fn add_official_mapping(binance: &mut Value, official_price: Option<rust_decimal::Decimal>) {
    let Some(price) = official_price else { return };
    let parsed = binance["opening_price_usdt"]
        .as_str()
        .and_then(|value| value.parse::<rust_decimal::Decimal>().ok())
        .zip(
            binance["current_price_usdt"]
                .as_str()
                .and_then(|value| value.parse::<rust_decimal::Decimal>().ok()),
        );
    let mapped = parsed.and_then(|(opening, current)| {
        (opening > rust_decimal::Decimal::ZERO)
            .then(|| {
                price
                    .checked_mul(current)
                    .and_then(|value| value.checked_div(opening))
            })
            .flatten()
    });
    binance["mapped_official_scale_price"] =
        mapped.map(|value| value.normalize().to_string()).into();
    binance["mapping_status"] = if mapped.is_some() {
        "official_reference_available"
    } else if binance["available"] == true {
        "scale_mapping_unavailable"
    } else {
        "binance_comparison_unavailable"
    }
    .into();
}

fn retry_after(response: &reqwest::Response) -> Duration {
    parse_retry_after(
        response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Utc::now(),
    )
}

fn parse_retry_after(raw: Option<&str>, now: DateTime<Utc>) -> Duration {
    const DEFAULT: Duration = Duration::from_secs(10);
    const MAX_RETRY: Duration = Duration::from_secs(3600);
    let Some(raw) = raw else { return DEFAULT };
    if let Ok(seconds) = raw.parse::<u64>() {
        return Duration::from_secs(seconds).min(MAX_RETRY);
    }
    chrono::DateTime::parse_from_rfc2822(raw)
        .map(|deadline| {
            (deadline.with_timezone(&Utc) - now)
                .to_std()
                .unwrap_or(Duration::ZERO)
                .min(MAX_RETRY)
        })
        .unwrap_or(DEFAULT)
}

async fn fetch_bounded(
    client: &reqwest::Client,
    url: &str,
    limit: usize,
) -> ReferenceResult<Vec<u8>> {
    let response = client.get(url).send().await.map_err(|error| {
        ReferenceFailure::new(
            "upstream_transport_error",
            error.to_string(),
            Duration::from_secs(1),
        )
    })?;
    let status = response.status();
    if status.as_u16() == 429 {
        let delay = retry_after(&response);
        return Err(ReferenceFailure {
            code: "upstream_rate_limited",
            detail: "upstream returned HTTP 429".to_owned(),
            retry_after: Some(delay),
            cache_for: delay,
        });
    }
    if status.as_u16() == 404 || status.as_u16() == 204 {
        return Err(ReferenceFailure::new(
            "official_not_published",
            format!("upstream returned HTTP {}", status.as_u16()),
            Duration::from_secs(2),
        ));
    }
    if !status.is_success() {
        return Err(ReferenceFailure::new(
            "upstream_http_error",
            format!("upstream returned HTTP {}", status.as_u16()),
            Duration::from_secs(2),
        ));
    }
    read_bounded(response, limit).await
}

async fn read_bounded(mut response: reqwest::Response, limit: usize) -> ReferenceResult<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(ReferenceFailure::new(
            "upstream_response_too_large",
            "upstream response exceeds byte limit",
            Duration::from_secs(2),
        ));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| {
        ReferenceFailure::new(
            "upstream_transport_error",
            error.to_string(),
            Duration::from_secs(1),
        )
    })? {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(ReferenceFailure::new(
                "upstream_response_too_large",
                "upstream response exceeds byte limit",
                Duration::from_secs(2),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_status_distinguishes_missing_binance_from_missing_official() {
        let mut binance = json!({"available":false,"opening_price_usdt":null,
            "current_price_usdt":null,"mapped_official_scale_price":null,
            "mapping_status":"official_reference_unavailable"});
        add_official_mapping(&mut binance, Some("83000".parse().unwrap()));
        assert_eq!(binance["mapping_status"], "binance_comparison_unavailable");
        assert!(binance["mapped_official_scale_price"].is_null());
        binance["available"] = true.into();
        binance["opening_price_usdt"] = "80000".into();
        binance["current_price_usdt"] = "81000".into();
        add_official_mapping(&mut binance, Some("83000".parse().unwrap()));
        assert_eq!(binance["mapping_status"], "official_reference_available");
        assert_eq!(binance["mapped_official_scale_price"], "84037.5");
    }

    #[test]
    fn official_failure_codes_and_retry_after_are_distinct() {
        let now = DateTime::parse_from_rfc3339("2026-10-08T03:50:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(parse_retry_after(Some("7"), now), Duration::from_secs(7));
        assert_eq!(
            parse_retry_after(Some("Thu, 08 Oct 2026 03:50:09 GMT"), now),
            Duration::from_secs(9)
        );
        assert_eq!(
            parse_retry_after(Some("7200"), now),
            Duration::from_secs(3600)
        );
        assert_eq!(parse_retry_after(None, now), Duration::from_secs(10));
        let pending = ReferenceFailure::new(
            "official_not_published",
            "window openPrice has not been published",
            Duration::from_secs(2),
        );
        assert_eq!(pending.body()["reason_code"], "official_not_published");
        assert!(pending.body()["retry_after_ms"].is_null());
        let limited = ReferenceFailure {
            code: "upstream_rate_limited",
            detail: "HTTP 429".to_owned(),
            retry_after: Some(Duration::from_secs(7)),
            cache_for: Duration::from_secs(7),
        };
        assert_eq!(limited.body()["retry_after_ms"], 7000);
    }

    #[test]
    fn official_decimal_tokens_keep_all_source_digits() {
        let raw = br#"{"openPrice":83512.123456789012345678,"timestamp":1790000000000}"#;
        let value: ExactReference = serde_json::from_slice(raw).unwrap();
        assert_eq!(value.open_price.get(), "83512.123456789012345678");
        let price: rust_decimal::Decimal = value.open_price.get().parse().unwrap();
        assert_eq!(price.to_string(), "83512.123456789012345678");

        let gamma = br#"{"eventMetadata":{"priceToBeat":83512.123456789012345678}}"#;
        let metadata: ExactGammaPrice = serde_json::from_slice(gamma).unwrap();
        let expected: rust_decimal::Decimal = metadata
            .event_metadata
            .unwrap()
            .price_to_beat
            .unwrap()
            .get()
            .parse()
            .unwrap();
        assert_eq!(price, expected);
    }
}
