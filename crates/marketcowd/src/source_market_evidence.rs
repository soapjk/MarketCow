//! Raw rule/lifecycle facts; never certifies logical implication or redemption.
use anyhow::{ensure, Context, Result};
use axum::{extract::{Query, State}, http::StatusCode, response::{IntoResponse, Response}, routing::get, Json, Router};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::{Duration, Instant}};
use tokio::sync::{Mutex, Semaphore};
const CAP: usize=262144;
pub fn project(id:&str, condition:&str, raw:&[u8], observed:&str)->Result<Value> {
    ensure!(raw.len()<=CAP,"response_size_exceeded");
    chrono::DateTime::parse_from_rfc3339(observed)?;
    let v:Value=serde_json::from_slice(raw)?;
    ensure!(v["id"]==id && v["conditionId"]==condition,"identity_mismatch");
    let array=|key:&str|->Result<Vec<String>> {
        let values:Vec<String>=serde_json::from_str(v[key].as_str().context("identity_array_missing")?)?;
        ensure!(!values.is_empty() && values.iter().all(|s|!s.is_empty()),"identity_array_empty");Ok(values)
    };
    let tokens=array("clobTokenIds")?;let outcomes=array("outcomes")?;
    ensure!(tokens.len()==outcomes.len() && tokens.len()<=16,"outcome_binding");
    for list in [&tokens,&outcomes] {ensure!(list.iter().collect::<std::collections::BTreeSet<_>>().len()==list.len(),"duplicate_identity");}
    let hash=hex::encode(Sha256::digest(raw));
    let rules=json!({"question":v.get("question").filter(|v|v.is_string()),
        "description":v.get("description").filter(|v|v.is_string()),
        "resolution_source":v.get("resolutionSource").filter(|v|v.is_string()),
        "scheduled_end_at_source":v.get("endDate"),
        "observation_start":null,"source_unavailable_extension":null,
        "interpretation":"unreviewed_full_text","version_sha256":hash});
    let mut payout:Option<Vec<Value>>=None;
    if v["umaResolutionStatus"]=="resolved" {
        if let Ok(values)=array("outcomePrices") {
            if values.len()==tokens.len() && values.iter().all(|p|p=="0" || p=="1") && values.iter().filter(|p|p.as_str()=="1").count()==1 {
                payout=Some(tokens.iter().zip(&outcomes).zip(values).map(|((t,o),p)|json!({"token_id":t,"outcome":o,"reported_payout":p})).collect());
            }
        }
    }
    Ok(json!({"schema_version":"marketcow.polymarket.market-evidence.v1",
        "market_id":id,"condition_id":condition,
        "outcomes":tokens.iter().zip(&outcomes).map(|(t,o)|json!({"token_id":t,"outcome":o})).collect::<Vec<_>>(),
        "source":"polymarket_gamma","source_url":format!("https://gamma-api.polymarket.com/markets/{id}"),
        "observed_at":observed,"raw_complete":true,"raw_bytes":raw.len(),"raw_sha256":hash,"raw_base64":STANDARD.encode(raw),
        "rules":rules,"closed":v.get("closed").filter(|v|v.is_boolean()),
        "accepting_orders":v.get("acceptingOrders").filter(|v|v.is_boolean()),
        "settlement":{"source_reported_status":v.get("umaResolutionStatus"),"reported_payouts":payout,
            "finality":"unverified","finality_evidence":null,"redeemable":null,"label_available_at":null},
        "fee_facts":null,"execution_eligible":false,
        "missing_facts":["reviewed_rule_semantics","authoritative_finality","fee_execution_facts"]}))
}
#[derive(Clone)] struct Service {client:reqwest::Client, slots:Arc<Semaphore>,last:Arc<Mutex<Option<Instant>>>}
#[derive(Deserialize)] #[serde(deny_unknown_fields)]
struct Request {market_id:String,expected_condition_id:String}
pub fn router()->Result<Router> {
    Ok(Router::new().route("/v1/prediction-markets/polymarket/research/market-evidence",get(read))
        .with_state(Service{client:reqwest::Client::builder().timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none()).retry(reqwest::retry::never()).build()?,
            slots:Arc::new(Semaphore::new(1)),last:Arc::new(Mutex::new(None))}))
}
fn failure(s:StatusCode,code:&str)->Response {(s,Json(json!({"schema_version":"marketcow.polymarket.market-evidence-error.v1","code":code}))).into_response()}
async fn read(State(s):State<Service>,query:Result<Query<Request>,axum::extract::rejection::QueryRejection>)->Response {
    let Ok(Query(q))=query else {return failure(StatusCode::BAD_REQUEST,"invalid_request")};
    if q.market_id.is_empty() || q.market_id.len()>20 || !q.market_id.bytes().all(|b|b.is_ascii_digit())
        || q.expected_condition_id.len()!=66 || !q.expected_condition_id.starts_with("0x")
        || !q.expected_condition_id[2..].bytes().all(|b|b.is_ascii_hexdigit()) {
        return failure(StatusCode::BAD_REQUEST,"invalid_identity")
    }
    let Ok(_slot)=s.slots.try_acquire() else {return failure(StatusCode::TOO_MANY_REQUESTS,"busy")};
    {let mut last=s.last.lock().await;if last.is_some_and(|t|t.elapsed()<Duration::from_secs(1)){return failure(StatusCode::TOO_MANY_REQUESTS,"rate_limited")};*last=Some(Instant::now());}
    let operation=async {
        let mut response=s.client.get(format!("https://gamma-api.polymarket.com/markets/{}",q.market_id)).send().await.map_err(|_|"transport_failed")?;
        let status=response.status().as_u16();let mut body=Vec::new();
        while let Some(chunk)=response.chunk().await.map_err(|_|"body_failed")? {
            if chunk.len()>CAP-body.len(){return Err("response_size_exceeded")};body.extend_from_slice(&chunk);
        }
        let observed=chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis,true);
        if status!=200 {return Ok(json!({"schema_version":"marketcow.polymarket.market-evidence-rejection.v1","upstream_status":status,"market_id":q.market_id,"observed_at":observed,"raw_complete":true,"raw_bytes":body.len(),"raw_sha256":hex::encode(Sha256::digest(&body)),"raw_base64":STANDARD.encode(body)}))}
        project(&q.market_id,&q.expected_condition_id,&body,&observed).map_err(|_|"invalid_source_identity_or_schema")
    };
    match tokio::time::timeout(Duration::from_secs(15),operation).await {
        Ok(Ok(v))=>Json(v).into_response(),Ok(Err(e))=>failure(StatusCode::BAD_GATEWAY,e),Err(_)=>failure(StatusCode::GATEWAY_TIMEOUT,"timeout")
    }
}
#[cfg(test)] mod tests {
    use super::*;
    fn raw()->Value {json!({"id":"1","conditionId":"c","clobTokenIds":"[\"10\",\"20\"]","outcomes":"[\"Yes\",\"No\"]","question":"synthetic","description":"synthetic full rules","closed":true,"outcomePrices":"[\"1\",\"0\"]"})}
    fn apply(v:&Value)->Value {project("1","c",&serde_json::to_vec(v).unwrap(),"2026-09-09T00:00:00Z").unwrap()}
    #[test] fn closed_and_prices_not_final(){let p=apply(&raw());assert!(p["settlement"]["reported_payouts"].is_null());assert_eq!(p["settlement"]["finality"],"unverified");}
    #[test] fn resolved_still_not_finality(){let mut r=raw();r["umaResolutionStatus"]=json!("resolved");let p=apply(&r);assert_eq!(p["settlement"]["reported_payouts"][0]["reported_payout"],"1");assert!(p["settlement"]["redeemable"].is_null());}
    #[test] fn identity_fails(){assert!(project("2","c",&serde_json::to_vec(&raw()).unwrap(),"2026-09-09T00:00:00Z").is_err());}
    #[test] fn missing_and_changed_rules(){let mut r=raw();let before=apply(&r);r.as_object_mut().unwrap().remove("description");let after=apply(&r);assert!(after["rules"]["description"].is_null());assert_ne!(before["rules"]["version_sha256"],after["rules"]["version_sha256"]);}
}
