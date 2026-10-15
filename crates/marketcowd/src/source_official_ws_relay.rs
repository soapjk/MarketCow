//! Thin official CLOB /book relay for the official-WS market-local candidate.
//! This does not adjudicate a trade: the caller must fence its owner identity
//! again after the response and validate the returned book, age and depth.
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use crate::{
    source_btc5m_api::{Control, WindowRole},
    source_publication::MemoryReader,
};

const SCHEMA: &str = "marketcow.polymarket.official-ws-book-relay.v2";
const ERROR_SCHEMA: &str = "marketcow.polymarket.official-ws-book-relay-error.v2";
const MAX_BODY_BYTES: usize = 512 * 1024;
const MAX_CLIENTS: usize = 32;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct Relay {
    control: Control,
    client: reqwest::Client,
    slots: Arc<Semaphore>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BookQuery {
    market_id: String,
    condition_id: String,
    token_id: String,
    rule_sha256: String,
    window_id: String,
    start_at: String,
    end_at: String,
    up_token_id: String,
    down_token_id: String,
    stream_id: String,
}

fn now_ns() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
}

fn header_if_valid(headers: &mut HeaderMap, name: &'static str, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

fn failure(status: StatusCode, code: &str, retryable: bool, request_id: &str) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    (
        status,
        headers,
        Json(json!({
            "schema_version":ERROR_SCHEMA,
            "code":code,
            "retryable":retryable,
            "request_id":request_id,
        })),
    )
        .into_response()
}

pub fn router(control: Control, _reader: MemoryReader) -> Result<Router> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .timeout(REQUEST_TIMEOUT)
        .build()?;
    Ok(Router::new()
        .route(
            "/v1/prediction-markets/polymarket/research/btc-5m/official-ws/official-book",
            get(http_book),
        )
        .with_state(Relay {
            control,
            client,
            slots: Arc::new(Semaphore::new(MAX_CLIENTS)),
        }))
}

async fn http_book(
    State(relay): State<Relay>,
    query: std::result::Result<Query<BookQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let request_id = uuid::Uuid::new_v4().simple().to_string();
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => return failure(StatusCode::BAD_REQUEST, "invalid_query", false, &request_id),
    };
    if query.token_id.is_empty() || !query.token_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return failure(
            StatusCode::BAD_REQUEST,
            "invalid_token_id",
            false,
            &request_id,
        );
    }
    let Ok(_permit) = relay.slots.clone().try_acquire_owned() else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "relay_capacity",
            true,
            &request_id,
        );
    };
    let Ok((profile, _)) = relay.control.profile_for_market_source() else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "profile_unavailable",
            true,
            &request_id,
        );
    };
    let Some(market) = profile
        .markets
        .iter()
        .find(|market| market.market_id == query.market_id)
    else {
        return failure(
            StatusCode::CONFLICT,
            "identity_not_active",
            false,
            &request_id,
        );
    };
    let public_identity=relay.control.live_identity(market);
    if market.condition_id != query.condition_id
        || market.rule_sha256 != query.rule_sha256
        || !public_identity.as_ref().is_ok_and(|(key,id)|
            id==&query.stream_id && key.window_id==query.window_id
            && key.start_at==query.start_at && key.end_at==query.end_at
            && key.up_token_id==query.up_token_id && key.down_token_id==query.down_token_id)
        || matches!(&market.role, WindowRole::Retiring)
        || !chrono::DateTime::parse_from_rfc3339(&market.start_at).ok()
            .zip(chrono::DateTime::parse_from_rfc3339(&market.end_at).ok())
            .is_some_and(|(start,end)|start<=chrono::Utc::now() && chrono::Utc::now()<end)
        || !market
            .outcomes
            .iter()
            .any(|outcome| outcome.token_id == query.token_id)
    {
        return failure(
            StatusCode::CONFLICT,
            "identity_not_active",
            false,
            &request_id,
        );
    }
    let Some(checked_at) = now_ns() else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "clock_unavailable",
            true,
            &request_id,
        );
    };
    let Some(started_at) = now_ns() else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "clock_unavailable",
            true,
            &request_id,
        );
    };
    let started = Instant::now();
    let response = match relay
        .client
        .get(format!(
            "https://clob.polymarket.com/book?token_id={}",
            query.token_id,
        ))
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return failure(
                if error.is_timeout() {
                    StatusCode::GATEWAY_TIMEOUT
                } else {
                    StatusCode::BAD_GATEWAY
                },
                if error.is_timeout() {
                    "upstream_timeout"
                } else {
                    "upstream_transport_error"
                },
                true,
                &request_id,
            );
        }
    };
    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let retry_after = response
        .headers()
        .get(header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut response = response;
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if body.len().saturating_add(chunk.len()) <= MAX_BODY_BYTES => {
                body.extend_from_slice(&chunk)
            }
            Ok(Some(_)) => {
                return failure(
                    StatusCode::BAD_GATEWAY,
                    "upstream_body_too_large",
                    false,
                    &request_id,
                );
            }
            Ok(None) => break,
            Err(error) => {
                return failure(
                    if error.is_timeout() {
                        StatusCode::GATEWAY_TIMEOUT
                    } else {
                        StatusCode::BAD_GATEWAY
                    },
                    if error.is_timeout() {
                        "upstream_timeout"
                    } else {
                        "upstream_transport_error"
                    },
                    true,
                    &request_id,
                );
            }
        }
    }
    let Some(received_at) = now_ns() else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "clock_unavailable",
            true,
            &request_id,
        );
    };
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Some(value) = content_type.as_deref() {
        header_if_valid(&mut headers, "content-type", value);
    }
    if let Some(value) = retry_after.as_deref() {
        header_if_valid(&mut headers, "retry-after", value);
    }
    for (name, value) in [
        ("x-marketcow-relay-schema", SCHEMA),
        ("x-marketcow-candidate-source", "polymarket_official_ws"),
        ("x-marketcow-relay-request-id", &request_id),
        ("x-marketcow-market-id", &query.market_id),
        ("x-marketcow-condition-id", &query.condition_id),
        ("x-marketcow-token-id", &query.token_id),
        ("x-marketcow-rule-sha256", &query.rule_sha256),
        ("x-marketcow-window-id", &query.window_id),
        ("x-marketcow-window-start-at", &query.start_at),
        ("x-marketcow-window-end-at", &query.end_at),
        ("x-marketcow-up-token-id", &query.up_token_id),
        ("x-marketcow-down-token-id", &query.down_token_id),
        ("x-marketcow-stream-id", &query.stream_id),
    ] {
        header_if_valid(&mut headers, name, value);
    }
    for (name, value) in [
        (
            "x-marketcow-identity-checked-at-unix-ns",
            checked_at.to_string(),
        ),
        (
            "x-marketcow-upstream-started-at-unix-ns",
            started_at.to_string(),
        ),
        (
            "x-marketcow-upstream-received-at-unix-ns",
            received_at.to_string(),
        ),
        (
            "x-marketcow-upstream-duration-us",
            started.elapsed().as_micros().to_string(),
        ),
        ("x-marketcow-upstream-status", status.as_u16().to_string()),
        (
            "x-marketcow-upstream-body-sha256",
            hex::encode(Sha256::digest(&body)),
        ),
    ] {
        header_if_valid(&mut headers, name, &value);
    }
    (status, headers, body).into_response()
}
