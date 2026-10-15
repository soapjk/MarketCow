//! Public, read-only REST acquisition adapted to the original service clients.
//! Every call does one real upstream read, never a WebSocket cache lookup.
//! These optional data readouts are not a Tradude submission prerequisite:
//! Tradude owns direct pre-order reads, account access and all order actions.
use anyhow::{Result, ensure};
use marketcow_contracts::typed_v3::{self as contract, common, facts};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

pub const BINANCE_URL: &str =
    "https://data-api.binance.vision/api/v3/trades?symbol=BTCUSDT&limit=1";
pub struct Service {
    client: reqwest::Client,
    books: Arc<Semaphore>,
    btc: Arc<Semaphore>,
}
pub struct Failed {
    pub status: u16,
    pub failure: facts::Failure,
}
/// Adapter-local placeholder only. Handlers replace it with the durable fact
/// identity before any successful response; no request-ID row/fsync is needed.
pub fn provisional_identity() -> common::RecordIdentity {
    common::RecordIdentity {
        id: uuid::Uuid::new_v4().to_string(),
        revision: 1,
    }
}
pub fn response<T: prost::Message>(
    status: u16,
    value: &T,
    payload_type: &'static str,
    retry_after: Option<&str>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "content-type",
        axum::http::HeaderValue::from_static(contract::CONTENT_TYPE),
    );
    headers.insert(
        "cache-control",
        axum::http::HeaderValue::from_static("no-store"),
    );
    headers.insert(
        "x-marketcow-payload-type",
        axum::http::HeaderValue::from_static(payload_type),
    );
    if let Some(raw) = retry_after {
        if let Ok(value) = axum::http::HeaderValue::from_str(raw) {
            headers.insert("retry-after", value);
        }
    }
    match contract::encode(value, 4 * 1024 * 1024) {
        Ok(bytes) => (
            axum::http::StatusCode::from_u16(status).unwrap_or(axum::http::StatusCode::BAD_GATEWAY),
            headers,
            bytes,
        )
            .into_response(),
        Err(_) => Failed::local("", "response_capacity", 413).into_response(),
    }
}
impl Failed {
    pub fn local(request_id: &str, code: &str, status: u16) -> Self {
        failed(request_id, code, status, None, None)
    }
}
pub fn decode<T: prost::Message + Default>(
    headers: &axum::http::HeaderMap,
    body: std::result::Result<axum::body::Bytes, axum::extract::rejection::BytesRejection>,
) -> std::result::Result<T, Failed> {
    let body = body.map_err(|_| Failed::local("", "request_capacity", 413))?;
    if body.len() > 64 * 1024 {
        return Err(Failed::local("", "request_capacity", 413));
    }
    if headers.get("content-type").and_then(|h| h.to_str().ok()) != Some(contract::CONTENT_TYPE) {
        return Err(Failed::local("", "unsupported_content_type", 415));
    }
    T::decode(body).map_err(|_| Failed::local("", "invalid_request", 400))
}
impl axum::response::IntoResponse for Failed {
    fn into_response(self) -> axum::response::Response {
        response(
            self.status,
            &self.failure,
            "marketcow.facts.v1.Failure",
            self.failure
                .upstream
                .as_ref()
                .and_then(|u| u.retry_after.as_deref()),
        )
    }
}
fn now() -> Result<i64> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow::anyhow!("clock_unavailable"))
}
fn failed(
    request_id: &str,
    code: &str,
    status: u16,
    upstream: Option<facts::UpstreamExchange>,
    dispatch: Option<facts::DispatchBinding>,
) -> Failed {
    Failed {
        status,
        failure: facts::Failure {
            error: Some(common::Error {
                code: code.into(),
                retryable: status == 429 || status >= 500,
                retry_after_ms: None,
                detail: None,
            }),
            request_id: request_id.into(),
            upstream,
            dispatch,
        },
    }
}
impl Service {
    pub fn from_existing(client: reqwest::Client, slots: Arc<Semaphore>) -> Self {
        Self {
            client,
            books: slots.clone(),
            btc: slots,
        }
    }
    async fn exchange(
        &self,
        request_id: &str,
        url: &str,
        max: usize,
    ) -> std::result::Result<(facts::UpstreamExchange, Vec<u8>), Failed> {
        self.exchange_with(request_id, url, max, Duration::from_secs(2))
            .await
    }
    pub async fn exchange_with(
        &self,
        request_id: &str,
        url: &str,
        max: usize,
        deadline: Duration,
    ) -> std::result::Result<(facts::UpstreamExchange, Vec<u8>), Failed> {
        let mut exchange = facts::UpstreamExchange {
            url: url.into(),
            started_at_ns: Some(
                now().map_err(|_| failed(request_id, "clock_unavailable", 503, None, None))?,
            ),
            ..Default::default()
        };
        let started = Instant::now();
        let operation = async {
            let mut response = self.client.get(url).send().await.map_err(|e| {
                if e.is_timeout() {
                    "upstream_timeout"
                } else {
                    "upstream_transport_error"
                }
            })?;
            exchange.status = Some(response.status().as_u16() as u32);
            exchange.content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|h| h.to_str().ok())
                .map(str::to_owned);
            exchange.retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|h| h.to_str().ok())
                .map(str::to_owned);
            if response.content_length().is_some_and(|n| n > max as u64) {
                return Err("upstream_body_too_large");
            }
            let mut raw = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|e| {
                if e.is_timeout() {
                    "upstream_timeout"
                } else {
                    "upstream_body_failed"
                }
            })? {
                if chunk.len() > max - raw.len() {
                    return Err("upstream_body_too_large");
                }
                raw.extend_from_slice(&chunk);
            }
            Ok(raw)
        };
        let result = tokio::time::timeout(deadline, operation).await;
        exchange.duration_us = Some(started.elapsed().as_micros().min(u64::MAX as u128) as u64);
        let raw = match result {
            Ok(Ok(raw)) => raw,
            result => {
                let code = match result {
                    Ok(Err(code)) => code,
                    _ => "upstream_timeout",
                };
                return Err(failed(
                    request_id,
                    code,
                    if code == "upstream_timeout" { 504 } else { 502 },
                    Some(exchange),
                    None,
                ));
            }
        };
        exchange.body_complete = true;
        exchange.received_at_ns = now().ok();
        if exchange.status != Some(200) {
            let status = exchange.status.unwrap_or(502) as u16;
            let code = if status == 429 {
                "upstream_rate_limited"
            } else if status == 418 {
                "upstream_ip_banned"
            } else {
                "upstream_http_error"
            };
            exchange.raw_body = Some(raw);
            return Err(failed(
                request_id,
                code,
                if (400..600).contains(&status) {
                    status
                } else {
                    502
                },
                Some(exchange),
                None,
            ));
        }
        Ok((exchange, raw))
    }
    pub async fn book(
        &self,
        request: &facts::OfficialBookRequest,
        dispatch: facts::DispatchBinding,
        identity: common::RecordIdentity,
    ) -> std::result::Result<facts::OfficialBookResponse, Failed> {
        let validate = || -> Result<()> {
            ensure!(
                !request.request_id.is_empty() && request.request_id.len() <= 128,
                "invalid_request_id"
            );
            super::source_facts_typed::validate_identity(&identity)?;
            let key = request
                .key
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("missing_market_key"))?;
            super::source_facts_typed::validate_key(key)?;
            ensure!(
                request.representation == "typed" || request.representation == "raw",
                "invalid_representation"
            );
            ensure!(
                request.token_id == key.up_token_id || request.token_id == key.down_token_id,
                "invalid_token_id"
            );
            ensure!(
                dispatch.key.as_ref() == Some(key)
                    && dispatch.stream_id == request.stream_id
                    && !request.stream_id.is_empty()
                    && dispatch.token_id == request.token_id
                    && dispatch.identity_checked_at_ns > 0,
                "identity_not_active"
            );
            let at = chrono::Utc::now();
            ensure!(
                chrono::DateTime::parse_from_rfc3339(&key.start_at)? <= at
                    && at < chrono::DateTime::parse_from_rfc3339(&key.end_at)?,
                "identity_not_active"
            );
            Ok(())
        };
        validate().map_err(|e| {
            failed(
                &request.request_id,
                if e.to_string() == "identity_not_active" {
                    "identity_not_active"
                } else {
                    "invalid_request"
                },
                if e.to_string() == "identity_not_active" {
                    409
                } else {
                    400
                },
                None,
                Some(dispatch.clone()),
            )
        })?;
        let _slot = self.books.try_acquire().map_err(|_| {
            failed(
                &request.request_id,
                "relay_capacity",
                503,
                None,
                Some(dispatch.clone()),
            )
        })?;
        let url = format!(
            "https://clob.polymarket.com/book?token_id={}",
            request.token_id
        );
        let (mut upstream, raw) = self
            .exchange(&request.request_id, &url, 512 * 1024)
            .await
            .map_err(|mut e| {
                e.failure.dispatch = Some(dispatch.clone());
                e
            })?;
        let body = if request.representation == "raw" {
            facts::official_book_response::Body::RawBody(raw)
        } else {
            match super::source_facts_typed::official_book(
                &raw,
                request.key.as_ref().unwrap(),
                &request.token_id,
            ) {
                Ok(book) => facts::official_book_response::Body::Book(book),
                Err(_) => {
                    upstream.raw_body = Some(raw);
                    return Err(failed(
                        &request.request_id,
                        "upstream_invalid_book",
                        502,
                        Some(upstream),
                        Some(dispatch),
                    ));
                }
            }
        };
        Ok(facts::OfficialBookResponse {
            schema_version: contract::BOOK_REST_SCHEMA.into(),
            request_id: request.request_id.clone(),
            identity: Some(identity),
            dispatch: Some(dispatch),
            upstream: Some(upstream),
            acquisition: "official_rest".into(),
            cache_used: false,
            ws_dependency: false,
            body: Some(body),
            evidence: None,
        })
    }
    pub async fn btc(
        &self,
        request: &facts::BtcRestRequest,
        identity: common::RecordIdentity,
    ) -> std::result::Result<facts::BtcRestResponse, Failed> {
        self.btc_raw(request, identity)
            .await
            .map(|(message, _)| message)
    }
    pub async fn btc_raw(
        &self,
        request: &facts::BtcRestRequest,
        identity: common::RecordIdentity,
    ) -> std::result::Result<(facts::BtcRestResponse, Vec<u8>), Failed> {
        if !matches!(
            common::Source::try_from(request.source),
            Ok(common::Source::BinanceSpotTrade
                | common::Source::ChainlinkBtcUsd
                | common::Source::ChainlinkBtcUsdTwap60)
        ) {
            return Err(failed(
                &request.request_id,
                "invalid_request",
                400,
                None,
                None,
            ));
        }
        if request.source != common::Source::BinanceSpotTrade as i32 {
            return Err(failed(
                &request.request_id,
                "source_rest_not_supported",
                501,
                None,
                None,
            ));
        }
        if request.request_id.is_empty()
            || request.request_id.len() > 128
            || super::source_facts_typed::validate_identity(&identity).is_err()
        {
            return Err(failed(
                &request.request_id,
                "invalid_request",
                400,
                None,
                None,
            ));
        }
        let _slot = self
            .btc
            .try_acquire()
            .map_err(|_| failed(&request.request_id, "rest_capacity", 503, None, None))?;
        let (mut upstream, raw) = self
            .exchange(&request.request_id, BINANCE_URL, 64 * 1024)
            .await?;
        let received = upstream
            .received_at_ns
            .ok_or_else(|| failed(&request.request_id, "clock_unavailable", 503, None, None))?;
        let mut trades =
            super::btc_typed_ingress::binance_recent(&raw, received).map_err(|_| {
                upstream.raw_body = Some(raw.clone());
                failed(
                    &request.request_id,
                    "upstream_invalid_trade",
                    502,
                    Some(upstream.clone()),
                    None,
                )
            })?;
        if trades.len() != 1 {
            upstream.raw_body = Some(raw);
            return Err(failed(
                &request.request_id,
                "upstream_invalid_trade",
                502,
                Some(upstream),
                None,
            ));
        }
        let trade = trades.remove(0);
        Ok((
            facts::BtcRestResponse {
                schema_version: contract::BTC_REST_SCHEMA.into(),
                request_id: request.request_id.clone(),
                identity: Some(identity),
                source: request.source,
                instrument_id: "BTCUSDT.BINANCE".into(),
                acquisition: "official_rest".into(),
                cache_used: false,
                ws_dependency: false,
                upstream: Some(upstream),
                latest_source_age_ms_at_receive: Some(
                    received.div_euclid(1_000_000) - trade.trade_time_ms,
                ),
                latest_trade: Some(trade),
                evidence: None,
            },
            raw,
        ))
    }
}
