//! Venue-published BTC five-minute opening reference, separate from chart prices.
//!
//! This is a bounded read surface inside the existing MarketCow process.  A
//! window reference is never inferred from a Binance candle or a nearby TWAP
//! tick.  The Polymarket window response is accepted only after the matching
//! Gamma market has declared the exact Chainlink 60-second TWAP rule.
use anyhow::{Context, Result};
use axum::{
    Router,
    extract::State,
    http::header::RETRY_AFTER,
    response::IntoResponse,
    routing::post,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use std::{
    collections::BTreeMap,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

use crate::{
    btc_trade,
    source_btc5m_api::Control,
};

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
    native_cache:Arc<Mutex<BTreeMap<Vec<u8>,(Instant,marketcow_contracts::typed_v3::facts::ReadingState)>>>,
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
        native_cache:Arc::new(Mutex::new(BTreeMap::new())),
    };
    Ok(Router::new()
        .route("/v1/facts/polymarket/reference",post(http_native_reference))
        .with_state(service)
        .layer(axum::extract::DefaultBodyLimit::max(64*1024)))
}
async fn http_native_reference(State(service):State<Service>,headers:axum::http::HeaderMap,
    body:std::result::Result<axum::body::Bytes,axum::extract::rejection::BytesRejection>)->axum::response::Response {
    use marketcow_contracts::typed_v3::{self as native,facts};use prost::Message;
    use crate::source_facts_rest::{Failed,response};
    let request=match crate::source_facts_rest::decode::<facts::MarketRequest>(&headers,body){Ok(v)=>v,Err(e)=>return e.into_response()};
    if crate::source_facts_typed::validate_request(&request).is_err(){return Failed::local(&request.request_id,"invalid_request",400).into_response();}
    let key=request.key.as_ref().unwrap().clone();let control=service.control.clone();let copied=key.clone();
    let rules=match tokio::task::spawn_blocking(move||control.validate_native_key(&copied)).await {
        Ok(Ok(v))=>v,_=>return Failed::local(&request.request_id,"rule_identity_mismatch",409).into_response(),
    };
    let mut cache=service.native_cache.lock().await;
    cache.retain(|_,(until,_)|*until>Instant::now());
    let encoded_key=key.encode_to_vec();
    let official=if let Some((_,reading))=cache.get(&encoded_key){reading.clone()}else {
        let fetched=async {
            let start=chrono::DateTime::parse_from_rfc3339(&key.start_at).map_err(|_|("invalid_window".to_owned(),Duration::from_secs(2)))?;
            if Utc::now()<start {return Err(("window_not_started".to_owned(),Duration::from_secs(2)));}
            let gamma_url=format!("https://gamma-api.polymarket.com/events/slug/{}",key.window_id);
            let gamma=fetch_bounded(&service.client,&gamma_url,MAX_GAMMA_BYTES).await.map_err(|e|(e.code.to_owned(),e.retry_after.unwrap_or(e.cache_for)))?;
            let mut url=reqwest::Url::parse("https://polymarket.com/api/crypto/crypto-price").map_err(|_|("invalid_upstream_url".to_owned(),Duration::from_secs(2)))?;
            url.query_pairs_mut().append_pair("symbol","BTC").append_pair("eventStartTime",&key.start_at)
                .append_pair("variant","fiveminute").append_pair("endDate",&key.end_at)
                .append_pair("twapEnabled","true").append_pair("twapLookbackSeconds","60");
            let raw=fetch_bounded(&service.client,url.as_str(),MAX_REFERENCE_BYTES).await.map_err(|e|(e.code.to_owned(),e.retry_after.unwrap_or(e.cache_for)))?;
            let req=request.clone();let rule=rules.clone();
            tokio::task::spawn_blocking(move||->Result<facts::ReadingState>{
                let identity=crate::source_facts_rest::provisional_identity();
                let meta=facts::FactMeta{identity:Some(identity),key:req.key.clone(),observed_at_ns:Utc::now().timestamp_nanos_opt().context("clock_unavailable")?,evidence:Vec::new()};
                let result=crate::source_facts_typed::official_reference(&req,meta,&rule,&gamma,&raw)?;
                result.official_opening.context("official_reference_missing")
            }).await.map_err(|_|("reference_worker_failed".to_owned(),Duration::from_secs(2)))?.map_err(|e|(e.to_string(),Duration::from_secs(2)))
        }.await;
        let (lifetime,state)=match fetched {
            Ok(state)=>(Duration::from_secs(300),state),
            Err((reason,delay))=>(delay.max(Duration::from_millis(100)).min(Duration::from_secs(300)),facts::ReadingState{available:false,reason:Some(reason),reading:None}),
        };
        if cache.len()>=6{if let Some(oldest)=cache.iter().min_by_key(|(_,v)|v.0).map(|(k,_)|k.clone()){cache.remove(&oldest);}}
        cache.insert(encoded_key.clone(),(Instant::now()+lifetime,state.clone()));state
    };
    drop(cache);
    let absent=||facts::ReadingState{available:false,reason:Some("trade_source_not_configured".into()),reading:None};
    let (opening,current)=match &service.trade {
        Some(trade)=>match trade.native_readings(&key).await{Ok(v)=>v,Err(_)=>return Failed::local(&request.request_id,"reference_unavailable",503).into_response()},
        None=>(absent(),absent()),
    };
    let control=service.control.clone();
    let request_id=request.request_id.clone();
    let result=tokio::task::spawn_blocking(move||->Result<facts::ReferenceResponse>{
        let mut states=[official,opening,current];
        for state in &mut states {if let Some(reading)=&mut state.reading {
            reading.identity=None;
            if reading.purpose!="current_proxy" {
                reading.baseline_id=Some(control.latch_native_baseline(&key,&reading.instrument_id,&reading.price_method,&reading.price)?);
            }
            reading.identity=Some(control.native_fact_record(&reading.purpose,&encoded_key,&reading.encode_to_vec())?);
        }}
        let [official,opening,current]=states;
        let mapped=official.reading.as_ref().zip(opening.reading.as_ref()).zip(current.reading.as_ref()).and_then(|((o,b),c)|{
            let o=o.price.parse::<rust_decimal::Decimal>().ok()?;let b=b.price.parse::<rust_decimal::Decimal>().ok()?;let c=c.price.parse::<rust_decimal::Decimal>().ok()?;
            o.checked_mul(c)?.checked_div(b).map(|v|v.normalize().to_string())
        });
        let mut document=facts::ReferenceResponse{schema_version:native::REFERENCE_SCHEMA.into(),request_id:String::new(),meta:None,
            official_opening:Some(official),binance_opening:Some(opening),binance_current:Some(current),binance_is_settlement_source:false,
            mapping_reason:Some(if mapped.is_some(){"official_reference_available"}else{"mapping_unavailable"}.into()),mapped_official_scale_price:mapped};
        let identity=control.native_fact_record("reference-comparison",&encoded_key,&document.encode_to_vec())?;
        document.request_id=request_id;document.meta=Some(facts::FactMeta{identity:Some(identity),key:Some(key),observed_at_ns:Utc::now().timestamp_nanos_opt().context("clock_unavailable")?,evidence:Vec::new()});
        Ok(document)
    }).await;
    match result{Ok(Ok(value))=>response(200,&value,"marketcow.facts.v1.ReferenceResponse",None),
        Ok(Err(error)) if error.to_string()=="baseline_price_conflict"=>Failed::local(&request.request_id,"baseline_price_conflict",409).into_response(),
        _=>Failed::local(&request.request_id,"database_unavailable",503).into_response()}
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
