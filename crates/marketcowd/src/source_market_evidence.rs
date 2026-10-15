//! Raw rule/lifecycle facts; never certifies logical implication or redemption.
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get,post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
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
}
fn shared_service()->Result<Service> {
    static SHARED:std::sync::OnceLock<Service>=std::sync::OnceLock::new();
    if let Some(service)=SHARED.get(){return Ok(service.clone());}
    let service=Service {client:reqwest::Client::builder().timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none()).retry(reqwest::retry::never()).build()?,slots:Arc::new(Semaphore::new(1))};
    let _=SHARED.set(service);
    Ok(SHARED.get().context("facts_client_unavailable")?.clone())
}
pub fn native_router(control:crate::source_btc5m_api::Control)->Result<Router> {
    Ok(Router::new().route("/v1/facts/polymarket/execution",post(read_native_execution))
        .with_state((shared_service()?,control)).layer(axum::extract::DefaultBodyLimit::max(64*1024)))
}
async fn read_native_execution(State((service,control)):State<(Service,crate::source_btc5m_api::Control)>,headers:axum::http::HeaderMap,
    body:std::result::Result<axum::body::Bytes,axum::extract::rejection::BytesRejection>)->Response {
    use marketcow_contracts::typed_v3::facts;use prost::Message;
    use crate::source_facts_rest::{Failed,response};
    let request=match crate::source_facts_rest::decode::<facts::MarketRequest>(&headers,body){Ok(v)=>v,Err(e)=>return e.into_response()};
    if crate::source_facts_typed::validate_request(&request).is_err(){return Failed::local(&request.request_id,"invalid_request",400).into_response();}
    let cloned=control.clone();let key=request.key.as_ref().unwrap().clone();
    if !matches!(tokio::task::spawn_blocking(move||cloned.validate_native_key(&key)).await,Ok(Ok(_))) {
        return Failed::local(&request.request_id,"rule_identity_mismatch",409).into_response();
    }
    let Ok(_slot)=service.slots.try_acquire()else{return Failed::local(&request.request_id,"facts_capacity",503).into_response()};
    let adapter=crate::source_facts_rest::Service::from_existing(service.client.clone(),service.slots.clone());
    let url=format!("https://clob.polymarket.com/clob-markets/{}",request.key.as_ref().unwrap().condition_id);
    let (_,raw)=match adapter.exchange_with(&request.request_id,&url,CAP,Duration::from_secs(15)).await {Ok(v)=>v,Err(e)=>return e.into_response()};
    let copied=request.clone();
    let result=tokio::task::spawn_blocking(move||->Result<facts::ExecutionFactsResponse>{
        let key=copied.key.as_ref().context("missing_market_key")?;
        let identity=crate::source_facts_rest::provisional_identity();
        let meta=facts::FactMeta{identity:Some(identity),key:Some(key.clone()),observed_at_ns:chrono::Utc::now().timestamp_nanos_opt().context("clock_unavailable")?,evidence:Vec::new()};
        let mut value=crate::source_facts_typed::execution(&copied,meta,&raw)?;
        value.request_id.clear();value.meta=None;
        let identity=control.native_fact_record("execution",&key.encode_to_vec(),&value.encode_to_vec())?;
        value.request_id=copied.request_id;value.meta=Some(facts::FactMeta{identity:Some(identity),key:Some(key.clone()),observed_at_ns:chrono::Utc::now().timestamp_nanos_opt().context("clock_unavailable")?,evidence:Vec::new()});
        Ok(value)
    }).await;
    match result {Ok(Ok(v))=>response(200,&v,"marketcow.facts.v1.ExecutionFactsResponse",None),
        Ok(Err(e)) if e.to_string()=="unsupported_fee_model"=>Failed::local(&request.request_id,"unsupported_fee_model",501).into_response(),
        Ok(Err(e)) if e.to_string()=="upstream_identity_mismatch"=>Failed::local(&request.request_id,"upstream_identity_mismatch",409).into_response(),
        _=>Failed::local(&request.request_id,"invalid_upstream_payload",502).into_response()}
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
        .with_state(shared_service()?))
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
    let taker_delay_enabled = v["itode"].as_bool().context("taker_order_delay_missing")?;
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
        "order_execution":{
            "taker_order_delay_enabled":taker_delay_enabled,
            "marketable_order_hold_milliseconds":if taker_delay_enabled {Some(250_u64)} else {None},
            "semantics":"marketable orders are held before synchronous processing",
            "source_field":"itode","version_sha256":hash,
            "complete":taker_delay_enabled,
            "missing_fields":if taker_delay_enabled {Vec::<String>::new()} else {vec!["disabled_delay_duration".to_owned()]}
        },
        "fee_schedule":{
            "model":"clob_v2_dynamic","currency":"USDC","maker_rate":"0","taker_rate":rate,
            "formula":"fee = shares * rate * (price * (1 - price)) ^ exponent","exponent":exponent,
            "taker_only":true,"quantum":"0.00001","rounding_decimal_places":5,"rounding_mode":null,
            "effective_at":null,"maker_base_fee_bps":maker_base_fee,"taker_base_fee_bps":taker_base_fee,
            "applicable_parameter_source":"fd",
            "base_fee_fields_applicability":"preserved_source_fields_not_used_for_clob_v2_dynamic_fee_calculation",
            "applicability_basis":"CLOB V2 determines fees at match time and directs clients to fd.r/fd.e/fd.to",
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
        let retry_after = response.headers().get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()).map(str::to_owned);
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
            "upstream_status":status,"upstream_retry_after":retry_after,"source_url":url,"observed_at":observed,"raw_complete":true,
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
    let operation = async {
        let mut response = s
            .client
            .get(&url)
            .send()
            .await
            .map_err(|_| "transport_failed")?;
        let status = response.status().as_u16();
        let retry_after = response.headers().get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()).map(str::to_owned);
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
            "upstream_status":status,"upstream_retry_after":retry_after,"source_url":url,"observed_at":observed,"raw_complete":true,
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
        let retry_after = response.headers().get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()).map(str::to_owned);
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
                json!({"schema_version":"marketcow.polymarket.market-evidence-rejection.v1","upstream_status":status,"upstream_retry_after":retry_after,"market_id":q.market_id,"observed_at":observed,"raw_complete":true,"raw_bytes":body.len(),"raw_sha256":hex::encode(Sha256::digest(&body)),"raw_base64":STANDARD.encode(body)}),
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
    #[tokio::test]
    async fn sequential_read_surfaces_have_no_shared_interval_gate() {
        // Closed loopback proxy makes upstream attempts fail immediately;
        // no supplier network or timing sleeps are needed for this regression.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let service = Service {
            client: reqwest::Client::builder()
                .proxy(reqwest::Proxy::all(format!("http://{address}")).unwrap())
                .timeout(Duration::from_secs(1)).build().unwrap(),
            slots: Arc::new(Semaphore::new(1)),
        };
        let condition = format!("0x{}", "33".repeat(32));
        let first = read(State(service.clone()), Ok(Query(Request {
            market_id: "600001".into(), expected_condition_id: condition.clone(),
        }))).await;
        assert_eq!(first.status(), StatusCode::BAD_GATEWAY);
        let second = read_execution(State(service.clone()), Ok(Query(ExecutionRequest {
            market_id: "600001".into(), expected_condition_id: condition,
            expected_up_token: "10001".into(), expected_down_token: "10002".into(),
        }))).await;
        assert_eq!(second.status(), StatusCode::BAD_GATEWAY);
        let third = read_slug(State(service.clone()), Ok(Query(SlugRequest {
            slug: "bitcoin-up-or-down-october-10-2026-1am-et".into(),
        }))).await;
        assert_eq!(third.status(), StatusCode::BAD_GATEWAY);
        // Concurrency remains bounded and is not converted into a wait queue.
        let held = service.slots.acquire().await.unwrap();
        let busy = read(State(service.clone()), Ok(Query(Request {
            market_id: "600001".into(), expected_condition_id: format!("0x{}", "33".repeat(32)),
        }))).await;
        assert_eq!(busy.status(), StatusCode::TOO_MANY_REQUESTS);
        drop(held);
    }
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
        assert!(
            project(
                "2",
                "c",
                &serde_json::to_vec(&raw()).unwrap(),
                "2026-09-09T00:00:00Z"
            )
            .is_err()
        );
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
        "fd":{"r":"0.070","e":1,"to":true},"itode":true,
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
        assert_eq!(p["fee_schedule"]["applicable_parameter_source"], "fd");
        assert_eq!(p["fee_schedule"]["maker_base_fee_bps"], "1000");
        assert_eq!(p["fee_schedule"]["taker_base_fee_bps"], "1000");
        assert_eq!(p["fee_schedule"]["currency"], "USDC");
        assert_eq!(p["order_execution"]["taker_order_delay_enabled"], true);
        assert_eq!(
            p["order_execution"]["marketable_order_hold_milliseconds"],
            250
        );
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
        assert!(
            project_execution(
                "1",
                &format!("0x{}", "c".repeat(64)),
                "10",
                "20",
                &bytes,
                "2026-09-13T00:00:00Z"
            )
            .is_err()
        );
    }
    #[test]
    fn execution_facts_reject_missing_or_unsupported_spec() {
        let condition = format!("0x{}", "d".repeat(64));
        let mut raw = execution_raw(&condition, "10", "20");
        raw["mos"] = Value::Null;
        assert!(
            project_execution(
                "1",
                &condition,
                "10",
                "20",
                &serde_json::to_vec(&raw).unwrap(),
                "2026-09-13T00:00:00Z"
            )
            .is_err()
        );
        let mut raw = execution_raw(&condition, "10", "20");
        raw["fd"]["to"] = json!(false);
        assert!(
            project_execution(
                "1",
                &condition,
                "10",
                "20",
                &serde_json::to_vec(&raw).unwrap(),
                "2026-09-13T00:00:00Z"
            )
            .is_err()
        );
    }
}
