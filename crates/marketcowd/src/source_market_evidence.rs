//! Raw rule/lifecycle facts; never certifies logical implication or redemption.
use anyhow::{ensure, Context, Result};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};
const CAP: usize = 262144;
pub fn project(id: &str, condition: &str, raw: &[u8], observed: &str) -> Result<Value> {
    ensure!(raw.len() <= CAP, "response_size_exceeded");
    chrono::DateTime::parse_from_rfc3339(observed)?;
    let v: Value = serde_json::from_slice(raw)?;
    ensure!(
        v["id"] == id && v["conditionId"] == condition,
        "identity_mismatch"
    );
    let array = |key: &str| -> Result<Vec<String>> {
        let values: Vec<String> =
            serde_json::from_str(v[key].as_str().context("identity_array_missing")?)?;
        ensure!(
            !values.is_empty() && values.iter().all(|s| !s.is_empty()),
            "identity_array_empty"
        );
        Ok(values)
    };
    let tokens = array("clobTokenIds")?;
    let outcomes = array("outcomes")?;
    ensure!(
        tokens.len() == outcomes.len() && tokens.len() <= 16,
        "outcome_binding"
    );
    for list in [&tokens, &outcomes] {
        ensure!(
            list.iter().collect::<std::collections::BTreeSet<_>>().len() == list.len(),
            "duplicate_identity"
        );
    }
    let hash = hex::encode(Sha256::digest(raw));
    let rules = json!({"question":v.get("question").filter(|v|v.is_string()),
        "description":v.get("description").filter(|v|v.is_string()),
        "resolution_source":v.get("resolutionSource").filter(|v|v.is_string()),
        "scheduled_end_at_source":v.get("endDate"),
        "observation_start":null,"source_unavailable_extension":null,
        "interpretation":"unreviewed_full_text","version_sha256":hash});
    let mut payout: Option<Vec<Value>> = None;
    if v["umaResolutionStatus"] == "resolved" {
        if let Ok(values) = array("outcomePrices") {
            if values.len() == tokens.len()
                && values.iter().all(|p| p == "0" || p == "1")
                && values.iter().filter(|p| p.as_str() == "1").count() == 1
            {
                payout = Some(
                    tokens
                        .iter()
                        .zip(&outcomes)
                        .zip(values)
                        .map(|((t, o), p)| json!({"token_id":t,"outcome":o,"reported_payout":p}))
                        .collect(),
                );
            }
        }
    }
    Ok(
        json!({"schema_version":"marketcow.polymarket.market-evidence.v1",
        "market_id":id,"condition_id":condition,
        "outcomes":tokens.iter().zip(&outcomes).map(|(t,o)|json!({"token_id":t,"outcome":o})).collect::<Vec<_>>(),
        "source":"polymarket_gamma","source_url":format!("https://gamma-api.polymarket.com/markets/{id}"),
        "observed_at":observed,"raw_complete":true,"raw_bytes":raw.len(),"raw_sha256":hash,"raw_base64":STANDARD.encode(raw),
        "rules":rules,"closed":v.get("closed").filter(|v|v.is_boolean()),
        "accepting_orders":v.get("acceptingOrders").filter(|v|v.is_boolean()),
        "settlement":{"source_reported_status":v.get("umaResolutionStatus"),"reported_payouts":payout,
            "finality":"unverified","finality_evidence":null,"redeemable":null,"label_available_at":null},
        "fee_facts":null,"execution_eligible":false,
        "missing_facts":["reviewed_rule_semantics","authoritative_finality","fee_execution_facts"]}),
    )
}
#[derive(Clone)]
struct Service {
    client: reqwest::Client,
    slots: Arc<Semaphore>,
    last: Arc<Mutex<Option<Instant>>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    market_id: String,
    expected_condition_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SlugRequest {
    slug: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionRequest {
    market_id: String,
    expected_condition_id: String,
    expected_up_token: String,
    expected_down_token: String,
}
pub fn slug_url(slug: &str) -> Result<String> {
    ensure!(
        slug.starts_with("bitcoin-up-or-down-")
            && slug.len() > 19
            && slug.len() <= 160
            && slug
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "invalid_hour_slug"
    );
    Ok(format!(
        "https://gamma-api.polymarket.com/markets/slug/{slug}"
    ))
}
pub fn project_slug(slug: &str, raw: &[u8], observed: &str) -> Result<Value> {
    let url = slug_url(slug)?;
    ensure!(raw.len() <= CAP, "response_size_exceeded");
    let source: Value = serde_json::from_slice(raw)?;
    ensure!(source["slug"] == slug, "slug_identity_mismatch");
    let id = source["id"].as_str().context("market_id_missing")?;
    let condition = source["conditionId"]
        .as_str()
        .context("condition_missing")?;
    ensure!(
        !id.is_empty() && id.len() <= 20 && id.bytes().all(|b| b.is_ascii_digit()),
        "invalid_market_id"
    );
    ensure!(
        condition.len() == 66
            && condition.starts_with("0x")
            && condition[2..].bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid_condition"
    );
    let mut result = project(id, condition, raw, observed)?;
    result["source_url"] = json!(url);
    // Same evidence schema: exact raw bytes contain the queried slug. No
    // inferred hour, rule approval, subscription or settlement authorization.
    Ok(result)
}
pub fn router() -> Result<Router> {
    Ok(Router::new()
        .route(
            "/v1/prediction-markets/polymarket/research/market-evidence",
            get(read),
        )
        .route(
            "/v1/prediction-markets/polymarket/research/btc-hour-evidence",
            get(read_slug),
        )
        .route(
            "/v1/prediction-markets/polymarket/research/market-execution-facts",
            get(read_execution),
        )
        .with_state(Service {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()?,
            slots: Arc::new(Semaphore::new(1)),
            last: Arc::new(Mutex::new(None)),
        }))
}
fn decimal(v: &Value, name: &str, allow_zero: bool) -> Result<String> {
    let text = if let Some(n) = v.as_number() {
        n.to_string()
    } else if let Some(s) = v.as_str() {
        s.to_string()
    } else {
        anyhow::bail!("{name}_missing")
    };
    let parsed = text
        .parse::<rust_decimal::Decimal>()
        .with_context(|| format!("{name}_invalid"))?;
    ensure!(
        parsed >= rust_decimal::Decimal::ZERO
            && (allow_zero || parsed > rust_decimal::Decimal::ZERO),
        "{name}_invalid"
    );
    Ok(parsed.normalize().to_string())
}
pub fn project_execution(
    market_id: &str,
    condition: &str,
    up: &str,
    down: &str,
    raw: &[u8],
    observed: &str,
) -> Result<Value> {
    ensure!(raw.len() <= CAP, "response_size_exceeded");
    chrono::DateTime::parse_from_rfc3339(observed)?;
    let v: Value = serde_json::from_slice(raw)?;
    ensure!(v["c"] == condition, "condition_identity_mismatch");
    let tokens = v["t"].as_array().context("tokens_missing")?;
    ensure!(
        tokens.len() == 2
            && tokens[0]["o"] == "Up"
            && tokens[0]["t"] == up
            && tokens[1]["o"] == "Down"
            && tokens[1]["t"] == down,
        "ordered_token_identity_mismatch"
    );
    let minimum_order_size = decimal(&v["mos"], "minimum_order_size", false)?;
    let price_increment = decimal(&v["mts"], "price_increment", false)?;
    let maker_base_fee = decimal(&v["mbf"], "maker_base_fee", true)?;
    let taker_base_fee = decimal(&v["tbf"], "taker_base_fee", true)?;
    let rate = decimal(&v["fd"]["r"], "fee_rate", true)?;
    let exponent = v["fd"]["e"].as_u64().context("fee_exponent_missing")?;
    ensure!(
        exponent > 0 && v["fd"]["to"].as_bool() == Some(true),
        "unsupported_fee_curve"
    );
    let hash = hex::encode(Sha256::digest(raw));
    let source_url = format!("https://clob.polymarket.com/clob-markets/{condition}");
    Ok(json!({
        "schema_version":"marketcow.polymarket.market-execution-facts.v1",
        "market_id":market_id,"condition_id":condition,
        "outcomes":[{"outcome":"Up","token_id":up},{"outcome":"Down","token_id":down}],
        "source":"polymarket_clob_v2","source_url":source_url,"observed_at":observed,
        "raw_complete":true,"raw_bytes":raw.len(),"raw_sha256":hash,"raw_base64":STANDARD.encode(raw),
        "instrument":{
            "price_increment":price_increment,"price_unit":"probability",
            "minimum_order_size":minimum_order_size,"size_unit":"shares","size_increment":null,
            "version_sha256":hash,"complete":false,"missing_fields":["size_increment"]
        },
        "fee_schedule":{
            "model":"clob_v2_dynamic","currency":"USDC","maker_rate":"0","taker_rate":rate,
            "formula":"fee = shares * rate * (price * (1 - price)) ^ exponent","exponent":exponent,
            "taker_only":true,"quantum":"0.00001","rounding_decimal_places":5,"rounding_mode":null,
            "effective_at":null,"maker_base_fee_bps":maker_base_fee,"taker_base_fee_bps":taker_base_fee,
            "version_sha256":hash,"complete":false,
            "missing_fields":["rounding_mode","effective_at"]
        },
        "paper_assumption":{
            "schema_version":"marketcow.polymarket.paper-execution-assumption.v1",
            "fee_currency_conversion":"1 pUSD = 1 USDC face value","fee_rounding_mode":"ROUND_UP",
            "size_increment":"0.000001","purpose":"conservative_simulation_only",
            "not_source_facts":["fee_currency_conversion","fee_rounding_mode","size_increment"]
        },
        "documentation":[
            "https://docs.polymarket.com/api-reference/markets/get-clob-market-info",
            "https://docs.polymarket.com/trading/fees",
            "https://docs.polymarket.com/v2-migration"
        ],
        "execution_eligible":false,"paper_simulation_eligible_with_explicit_assumption":true
    }))
}
async fn read_execution(
    State(s): State<Service>,
    query: Result<Query<ExecutionRequest>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Ok(Query(q)) = query else {
        return failure(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let market_identity =
        |v: &str| !v.is_empty() && v.len() <= 20 && v.bytes().all(|b| b.is_ascii_digit());
    let token_identity =
        |v: &str| !v.is_empty() && v.len() <= 78 && v.bytes().all(|b| b.is_ascii_digit());
    if !market_identity(&q.market_id)
        || !token_identity(&q.expected_up_token)
        || !token_identity(&q.expected_down_token)
        || q.expected_up_token == q.expected_down_token
        || q.expected_condition_id.len() != 66
        || !q.expected_condition_id.starts_with("0x")
        || !q.expected_condition_id[2..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit())
    {
        return failure(StatusCode::BAD_REQUEST, "invalid_identity");
    }
    let Ok(_slot) = s.slots.try_acquire() else {
        return failure(StatusCode::TOO_MANY_REQUESTS, "busy");
    };
    {
        let mut last = s.last.lock().await;
        if last.is_some_and(|t| t.elapsed() < Duration::from_secs(1)) {
            return failure(StatusCode::TOO_MANY_REQUESTS, "rate_limited");
        };
        *last = Some(Instant::now());
    }
    let url = format!(
        "https://clob.polymarket.com/clob-markets/{}",
        q.expected_condition_id
    );
    let operation = async {
        let mut response = s
            .client
            .get(&url)
            .send()
            .await
            .map_err(|_| "transport_failed")?;
        let status = response.status().as_u16();
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| "body_failed")? {
            if chunk.len() > CAP - body.len() {
                return Err("response_size_exceeded");
            };
            body.extend_from_slice(&chunk);
        }
        let observed = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        if status != 200 {
            return Ok(
                json!({"schema_version":"marketcow.polymarket.market-evidence-rejection.v1",
            "upstream_status":status,"source_url":url,"observed_at":observed,"raw_complete":true,
            "raw_bytes":body.len(),"raw_sha256":hex::encode(Sha256::digest(&body)),"raw_base64":STANDARD.encode(body)}),
            );
        }
        project_execution(
            &q.market_id,
            &q.expected_condition_id,
            &q.expected_up_token,
            &q.expected_down_token,
            &body,
            &observed,
        )
        .map_err(|_| "invalid_source_identity_or_schema")
    };
    match tokio::time::timeout(Duration::from_secs(15), operation).await {
        Ok(Ok(v)) => Json(v).into_response(),
        Ok(Err(e)) => failure(StatusCode::BAD_GATEWAY, e),
        Err(_) => failure(StatusCode::GATEWAY_TIMEOUT, "timeout"),
    }
}
async fn read_slug(
    State(s): State<Service>,
    query: Result<Query<SlugRequest>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Ok(Query(q)) = query else {
        return failure(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Ok(url) = slug_url(&q.slug) else {
        return failure(StatusCode::BAD_REQUEST, "invalid_hour_slug");
    };
    let Ok(_slot) = s.slots.try_acquire() else {
        return failure(StatusCode::TOO_MANY_REQUESTS, "busy");
    };
    {
        let mut last = s.last.lock().await;
        if last.is_some_and(|t| t.elapsed() < Duration::from_secs(1)) {
            return failure(StatusCode::TOO_MANY_REQUESTS, "rate_limited");
        };
        *last = Some(Instant::now());
    }
    let operation = async {
        let mut response = s
            .client
            .get(&url)
            .send()
            .await
            .map_err(|_| "transport_failed")?;
        let status = response.status().as_u16();
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| "body_failed")? {
            if chunk.len() > CAP - body.len() {
                return Err("response_size_exceeded");
            };
            body.extend_from_slice(&chunk);
        }
        let observed = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        if status != 200 {
            return Ok(
                json!({"schema_version":"marketcow.polymarket.market-evidence-rejection.v1",
            "upstream_status":status,"source_url":url,"observed_at":observed,"raw_complete":true,
            "raw_bytes":body.len(),"raw_sha256":hex::encode(Sha256::digest(&body)),"raw_base64":STANDARD.encode(body)}),
            );
        }
        project_slug(&q.slug, &body, &observed).map_err(|_| "invalid_source_identity_or_schema")
    };
    match tokio::time::timeout(Duration::from_secs(15), operation).await {
        Ok(Ok(v)) => Json(v).into_response(),
        Ok(Err(e)) => failure(StatusCode::BAD_GATEWAY, e),
        Err(_) => failure(StatusCode::GATEWAY_TIMEOUT, "timeout"),
    }
}
fn failure(s: StatusCode, code: &str) -> Response {
    (
        s,
        Json(json!({"schema_version":"marketcow.polymarket.market-evidence-error.v1","code":code})),
    )
        .into_response()
}
async fn read(
    State(s): State<Service>,
    query: Result<Query<Request>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Ok(Query(q)) = query else {
        return failure(StatusCode::BAD_REQUEST, "invalid_request");
    };
    if q.market_id.is_empty()
        || q.market_id.len() > 20
        || !q.market_id.bytes().all(|b| b.is_ascii_digit())
        || q.expected_condition_id.len() != 66
        || !q.expected_condition_id.starts_with("0x")
        || !q.expected_condition_id[2..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit())
    {
        return failure(StatusCode::BAD_REQUEST, "invalid_identity");
    }
    let Ok(_slot) = s.slots.try_acquire() else {
        return failure(StatusCode::TOO_MANY_REQUESTS, "busy");
    };
    {
        let mut last = s.last.lock().await;
        if last.is_some_and(|t| t.elapsed() < Duration::from_secs(1)) {
            return failure(StatusCode::TOO_MANY_REQUESTS, "rate_limited");
        };
        *last = Some(Instant::now());
    }
    let operation = async {
        let mut response = s
            .client
            .get(format!(
                "https://gamma-api.polymarket.com/markets/{}",
                q.market_id
            ))
            .send()
            .await
            .map_err(|_| "transport_failed")?;
        let status = response.status().as_u16();
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| "body_failed")? {
            if chunk.len() > CAP - body.len() {
                return Err("response_size_exceeded");
            };
            body.extend_from_slice(&chunk);
        }
        let observed = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        if status != 200 {
            return Ok(
                json!({"schema_version":"marketcow.polymarket.market-evidence-rejection.v1","upstream_status":status,"market_id":q.market_id,"observed_at":observed,"raw_complete":true,"raw_bytes":body.len(),"raw_sha256":hex::encode(Sha256::digest(&body)),"raw_base64":STANDARD.encode(body)}),
            );
        }
        project(&q.market_id, &q.expected_condition_id, &body, &observed)
            .map_err(|_| "invalid_source_identity_or_schema")
    };
    match tokio::time::timeout(Duration::from_secs(15), operation).await {
        Ok(Ok(v)) => Json(v).into_response(),
        Ok(Err(e)) => failure(StatusCode::BAD_GATEWAY, e),
        Err(_) => failure(StatusCode::GATEWAY_TIMEOUT, "timeout"),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn raw() -> Value {
        json!({"id":"1","conditionId":"c","clobTokenIds":"[\"10\",\"20\"]","outcomes":"[\"Yes\",\"No\"]","question":"synthetic","description":"synthetic full rules","closed":true,"outcomePrices":"[\"1\",\"0\"]"})
    }
    fn apply(v: &Value) -> Value {
        project(
            "1",
            "c",
            &serde_json::to_vec(v).unwrap(),
            "2026-09-09T00:00:00Z",
        )
        .unwrap()
    }
    #[test]
    fn closed_and_prices_not_final() {
        let p = apply(&raw());
        assert!(p["settlement"]["reported_payouts"].is_null());
        assert_eq!(p["settlement"]["finality"], "unverified");
    }
    #[test]
    fn resolved_still_not_finality() {
        let mut r = raw();
        r["umaResolutionStatus"] = json!("resolved");
        let p = apply(&r);
        assert_eq!(
            p["settlement"]["reported_payouts"][0]["reported_payout"],
            "1"
        );
        assert!(p["settlement"]["redeemable"].is_null());
    }
    #[test]
    fn identity_fails() {
        assert!(project(
            "2",
            "c",
            &serde_json::to_vec(&raw()).unwrap(),
            "2026-09-09T00:00:00Z"
        )
        .is_err());
    }
    #[test]
    fn missing_and_changed_rules() {
        let mut r = raw();
        let before = apply(&r);
        r.as_object_mut().unwrap().remove("description");
        let after = apply(&r);
        assert!(after["rules"]["description"].is_null());
        assert_ne!(
            before["rules"]["version_sha256"],
            after["rules"]["version_sha256"]
        );
    }
    #[test]
    fn slug_discovery_preserves_raw_and_never_approves() {
        let slug = "bitcoin-up-or-down-september-10-2026-5pm-et";
        let mut r = raw();
        r["slug"] = json!(slug);
        r["conditionId"] = json!(format!("0x{}", "a".repeat(64)));
        let bytes = serde_json::to_vec(&r).unwrap();
        let p = project_slug(slug, &bytes, "2026-09-10T00:00:00Z").unwrap();
        assert_eq!(p["raw_sha256"], hex::encode(Sha256::digest(&bytes)));
        assert_eq!(p["source_url"], slug_url(slug).unwrap());
        assert_eq!(p["execution_eligible"], false);
        assert!(project_slug("bitcoin-up-or-down-wrong", &bytes, "2026-09-10T00:00:00Z").is_err());
    }
    #[test]
    fn slug_path_injection_rejected() {
        for s in [
            "bitcoin-up-or-down-../x",
            "bitcoin-up-or-down-x?y=z",
            "https://evil.invalid",
            "bitcoin-up-or-down-%2f",
        ] {
            assert!(slug_url(s).is_err());
        }
    }
    fn execution_raw(condition: &str, up: &str, down: &str) -> Value {
        json!({
            "c":condition,"mos":"5","mts":"0.010","mbf":1000,"tbf":"1000",
            "fd":{"r":"0.070","e":1,"to":true},
            "t":[{"o":"Up","t":up},{"o":"Down","t":down}]
        })
    }
    #[test]
    fn execution_facts_preserve_source_and_separate_assumptions() {
        let condition = format!("0x{}", "a".repeat(64));
        let up = "10";
        let down = "20";
        let bytes = serde_json::to_vec(&execution_raw(&condition, up, down)).unwrap();
        let p = project_execution(
            "4463079",
            &condition,
            up,
            down,
            &bytes,
            "2026-09-13T06:19:59.961Z",
        )
        .unwrap();
        assert_eq!(p["raw_sha256"], hex::encode(Sha256::digest(&bytes)));
        assert_eq!(p["instrument"]["minimum_order_size"], "5");
        assert_eq!(p["instrument"]["price_increment"], "0.01");
        assert!(p["instrument"]["size_increment"].is_null());
        assert_eq!(p["fee_schedule"]["taker_rate"], "0.07");
        assert_eq!(p["fee_schedule"]["currency"], "USDC");
        assert!(p["fee_schedule"]["rounding_mode"].is_null());
        assert!(p["fee_schedule"]["effective_at"].is_null());
        assert_eq!(p["execution_eligible"], false);
        assert_eq!(
            p["paper_assumption"]["purpose"],
            "conservative_simulation_only"
        );
    }
    #[test]
    fn execution_facts_reject_wrong_identity_or_order() {
        let condition = format!("0x{}", "b".repeat(64));
        let bytes = serde_json::to_vec(&execution_raw(&condition, "10", "20")).unwrap();
        assert!(
            project_execution("1", &condition, "20", "10", &bytes, "2026-09-13T00:00:00Z").is_err()
        );
        assert!(project_execution(
            "1",
            &format!("0x{}", "c".repeat(64)),
            "10",
            "20",
            &bytes,
            "2026-09-13T00:00:00Z"
        )
        .is_err());
    }
    #[test]
    fn execution_facts_reject_missing_or_unsupported_spec() {
        let condition = format!("0x{}", "d".repeat(64));
        let mut raw = execution_raw(&condition, "10", "20");
        raw["mos"] = Value::Null;
        assert!(project_execution(
            "1",
            &condition,
            "10",
            "20",
            &serde_json::to_vec(&raw).unwrap(),
            "2026-09-13T00:00:00Z"
        )
        .is_err());
        let mut raw = execution_raw(&condition, "10", "20");
        raw["fd"]["to"] = json!(false);
        assert!(project_execution(
            "1",
            &condition,
            "10",
            "20",
            &serde_json::to_vec(&raw).unwrap(),
            "2026-09-13T00:00:00Z"
        )
        .is_err());
    }
}
