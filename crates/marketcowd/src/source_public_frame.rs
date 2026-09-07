//! Pure consumer-owned quality projection over one captured memory boundary.
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::str::FromStr;
use rust_decimal::Decimal;
use crate::source_publication::MemoryView;

fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key].as_str().with_context(|| format!("missing string {key}"))
}
fn list<'a>(value: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
    value[key].as_array().with_context(|| format!("missing array {key}"))
}
fn flag(value: &Value, key: &str) -> Result<bool> {
    value[key].as_bool().with_context(|| format!("missing boolean {key}"))
}
fn terminal(market: &Value) -> Result<bool> {
    Ok(matches!(text(market,"lifecycle_state")?, "closed"|"resolved"|"invalid"))
}

pub fn frame(view: &MemoryView, market_id: &str, now: DateTime<Utc>, generation: u64) -> Result<Value> {
    let market = view.markets.get(market_id).context("selected market metadata absent")?;
    let identity = &market["identity"];
    let instrument = &market["rules"]["instrument"];
    let fee = &market["rules"]["fee_schedule"];
    let relations = list(market,"relations")?;
    let relation_ids: Vec<_> = relations.iter().map(|r| text(r,"relation_id")).collect::<Result<_>>()?;
    let mut reasons: BTreeSet<String> = BTreeSet::new();
    let mut required = BTreeSet::new();
    let mut relation_tokens = BTreeSet::new();
    let mut pairs = Vec::new();
    let mut own_tokens = Vec::new();
    if terminal(market)? {
        reasons.insert(format!("market_{}", text(market,"lifecycle_state")?));
    } else {
        for outcome in list(identity,"outcomes")? {
            let token = text(outcome,"token_id")?.to_owned();
            required.insert(token.clone());
            if view.books.contains_key(&token) { own_tokens.push(token); }
        }
        if own_tokens.len() != 2 { reasons.insert("missing_outcome_book".into()); }
        let expected_tick = instrument.get("price_increment").context("price increment field absent")?
            .as_str().map(Decimal::from_str).transpose()?;
        let mut tick_versions = BTreeSet::new();
        let mut binding_invalid = own_tokens.len() != 2 || expected_tick.is_none();
        for token in &own_tokens {
            let book = &view.books[token];
            binding_invalid |= text(book,"token_id")? != token
                || text(book,"condition_id")? != text(identity,"condition_id")?
                || Some(Decimal::from_str(text(book,"tick_size")?)?) != expected_tick;
            tick_versions.insert(text(book,"tick_version")?);
        }
        if binding_invalid || tick_versions.len() != 1 {
            reasons.insert("polymarket_instrument_book_binding_incomplete".into());
        }
        let dynamic: Vec<_> = list(instrument,"provenance")?.iter()
            .filter(|p| p["source"] == "polymarket_clob").collect();
        let mut bound = false;
        for p in &dynamic {
            let token_set: BTreeSet<_> = list(p,"token_ids")?.iter()
                .map(|t| t.as_str().context("provenance token")).collect::<Result<_>>()?;
            bound |= p["tick_version"].as_str().is_some_and(|t|tick_versions.contains(t))
                && p["boundary_cursor"].as_u64().is_some_and(|c|c <= view.cursor)
                && p["projection_generation"].as_u64().is_some_and(|g|g <= generation)
                && token_set == required.iter().map(String::as_str).collect();
        }
        if !dynamic.is_empty() && !bound { reasons.insert("polymarket_instrument_book_binding_incomplete".into()); }
        if !flag(&market["rules"],"rules_complete")? { reasons.insert("instrument_facts_incomplete".into()); }
        if !flag(fee,"complete")? { reasons.insert("fee_schedule_incomplete".into()); }
        for relation in relations {
            if text(relation,"relation_type")? != "standard_negative_risk" { continue; }
            if !flag(relation,"complete")? { reasons.insert("negative_risk_relation_incomplete".into()); }
            for pair in list(relation,"outcome_pairs")? {
                let member = text(pair,"market_id")?;
                let token = text(pair,"yes_token_id")?.to_owned();
                match view.markets.get(member) {
                    None => {
                        reasons.insert("negative_risk_member_catalog_missing".into());
                        relation_tokens.insert(token.clone());
                        required.insert(token);
                    }
                    Some(meta) => {
                        if terminal(meta)? {
                            if text(meta,"lifecycle_state")? != "resolved" || meta["resolution"].is_null() {
                                reasons.insert("negative_risk_terminal_dependency_requires_resolution".into());
                            }
                        } else {
                            relation_tokens.insert(token.clone());
                            required.insert(token);
                        }
                        if !flag(&meta["rules"]["instrument"],"complete")? {
                            reasons.insert("negative_risk_member_instrument_facts_incomplete".into());
                        }
                        if !flag(&meta["rules"]["fee_schedule"],"complete")? {
                            reasons.insert("negative_risk_member_fee_schedule_incomplete".into());
                        }
                    }
                }
                pairs.push(pair.clone());
            }
        }
        if relation_tokens.iter().any(|t| !view.books.contains_key(t)) {
            reasons.insert("negative_risk_member_missing".into());
        }
        // Gaps are facts, never a reason to reject unrelated markets.
        let mut gaps: Vec<&Value> = list(&view.base,"gaps")?.iter().collect();
        gaps.extend(view.recoveries.values().map(|r| &r["gap"]));
        for gap in gaps {
            if flag(gap,"resolved")? { continue; }
            if let Some(token) = gap["token_id"].as_str() {
                if own_tokens.iter().any(|t| t == token) { reasons.insert("unresolved_gap".into()); }
                if relation_tokens.contains(token) { reasons.insert("negative_risk_member_gap".into()); }
            }
        }
    }
    let missing: Vec<_> = required.iter().filter(|t| !view.books.contains_key(*t)).cloned().collect();
    let recovering: Vec<_> = required.iter().filter(|t| view.recoveries.contains_key(*t)).cloned().collect();
    if !recovering.is_empty() { reasons.insert("recovery_in_progress".into()); }
    let mut oldest = None;
    for token in &required {
        if let Some(book) = view.books.get(token) {
            let observed = DateTime::parse_from_rfc3339(text(book,"received_at")?)?.with_timezone(&Utc);
            oldest = Some(oldest.map_or(observed, |old: DateTime<Utc>| old.min(observed)));
        }
    }
    let age = oldest.map(|at| ((now-at).num_microseconds().unwrap_or(i64::MAX) as f64 / 1000.0).max(0.0));
    let dependencies: BTreeSet<_> = pairs.iter().map(|p| text(p,"market_id"))
        .collect::<Result<BTreeSet<_>>>()?.into_iter().filter(|id| *id != market_id).collect();
    Ok(json!({
        "market_id":market_id,"condition_id":text(identity,"condition_id")?,
        "frame_at":now.to_rfc3339(),"cursor":view.cursor,
        "status":if terminal(market)? {"terminal"} else if reasons.is_empty() && missing.is_empty() {"ready"} else {"fail_closed"},
        "reason_codes":reasons,"token_ids":own_tokens,"relation_ids":relation_ids,
        "relation_token_ids":relation_tokens.iter().filter(|t|view.books.contains_key(*t)).collect::<Vec<_>>(),
        "relation_pairs":pairs,"instrument_revision":text(instrument,"revision")?,
        "fee_schedule_id":text(fee,"schedule_id")?,"open_position_allowed":null,
        "open_position_error_code":null,"decision_owner":"consumer",
        "required_token_ids":required,"missing_token_ids":missing,
        "recovering_token_ids":recovering,"dependency_market_ids":dependencies,
        "maximum_book_age_ms":age
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source_publication::Publication;

    /// Real captured input supplied externally; no network and no generated
    /// history. This checks binary ready/terminal frames only: old fixtures do
    /// not contain the complete gap/recovery or dependency metadata baseline.
    #[test]
    #[ignore = "requires explicit captured fixture path and SHA256"]
    fn captured_binary_frame_contract() {
        use sha2::{Digest,Sha256};
        use std::{collections::BTreeMap,sync::Arc};
        let path = std::env::var("MARKETCOW_PUBLIC_FIXTURE").unwrap();
        let expected = std::env::var("MARKETCOW_PUBLIC_FIXTURE_SHA256").unwrap();
        let bytes = std::fs::read(path).unwrap();
        assert_eq!(hex::encode(Sha256::digest(&bytes)),expected);
        let original: Value = serde_json::from_slice(&bytes).unwrap();
        let view = MemoryView {
            cursor:original["cursor"].as_u64().unwrap(),persisted_cursor:original["cursor"].as_u64().unwrap(),
            confirmation_sequence:original["confirmation_sequence"].as_u64().unwrap(),
            books:original["snapshot"]["books"].as_object().unwrap().iter().map(|(t,b)|(t.clone(),Arc::new(b.clone()))).collect(),
            markets:original["bootstrap"]["markets"].as_array().unwrap().iter().map(|m|(m["identity"]["market_id"].as_str().unwrap().to_owned(),Arc::new(m.clone()))).collect(),
            recoveries:BTreeMap::new(),book_cursors:BTreeMap::new(),confirmation_versions:BTreeMap::new(),
            base:json!({"gaps":[]}),queued:0,
        };
        let now = DateTime::parse_from_rfc3339(original["freshness_checked_at"].as_str().unwrap()).unwrap().with_timezone(&Utc);
        let mut checked = 0;
        for old in original["snapshot"]["items"].as_array().unwrap() {
            let id = old["market_id"].as_str().unwrap();
            if view.markets[id]["identity"]["neg_risk"] != false || old["status"] == "fail_closed" {continue;}
            let new = frame(&view,id,now,original["projection_generation"].as_u64().unwrap()).unwrap();
            for key in ["status","reason_codes","required_token_ids","missing_token_ids","recovering_token_ids","decision_owner","open_position_allowed","token_ids","relation_pairs"] {
                assert_eq!(new[key],old[key],"market {id} field {key}");
            }
            checked += 1;
        }
        assert!(checked > 0);
        eprintln!("captured binary/terminal contract checked {checked} markets (not complete scope audit)");
    }

    fn market(id: &str, tokens: [&str;2]) -> Value {
        json!({"identity":{"market_id":id,"condition_id":id,
            "outcomes":[{"token_id":tokens[0]},{"token_id":tokens[1]}]},
            "lifecycle_state":"active","active":true,"closed":false,"relations":[],
            "rules":{"rules_complete":true,"instrument":{"complete":true,
                "price_increment":"0.01","revision":"instrument","provenance":[]},
                "fee_schedule":{"complete":true,"schedule_id":"fee"}}})
    }
    fn book(token: &str, condition: &str) -> Value {
        json!({"token_id":token,"condition_id":condition,"tick_size":"0.01",
            "tick_version":"tick","exchange_at":"2026-01-01T00:00:00Z",
            "received_at":"2026-01-01T00:00:00Z"})
    }

    #[tokio::test]
    async fn missing_market_does_not_block_other_market_or_hide_old_age() {
        let mut incomplete = market("2",["c","d"]);
        incomplete["rules"]["instrument"]["price_increment"] = Value::Null;
        incomplete["rules"]["instrument"]["complete"] = json!(false);
        incomplete["rules"]["rules_complete"] = json!(false);
        let seed = json!({"latest_cursor":0,"catalog_revision":"a".repeat(64),"catalog_source":{},"gaps":[],"books":[book("a","1"),book("b","1")],
            "markets":[market("1",["a","b"]),incomplete]});
        let p = Publication::start(0,Some(seed),[("a".into(),"1".into()),("b".into(),"1".into()),
            ("c".into(),"2".into()),("d".into(),"2".into())].into(),8,131072,65536, |_|Ok(())).unwrap();
        let view = p.capture_view().unwrap();
        let now = DateTime::parse_from_rfc3339("2026-01-01T00:01:00Z").unwrap().with_timezone(&Utc);
        let ready = frame(&view,"1",now,1).unwrap();
        let missing = frame(&view,"2",now,1).unwrap();
        assert_eq!(ready["status"],"ready");
        assert_eq!(ready["maximum_book_age_ms"],60000.0);
        assert_eq!(ready["decision_owner"],"consumer");
        assert!(ready["open_position_allowed"].is_null());
        assert_eq!(missing["status"],"fail_closed");
        assert_eq!(missing["missing_token_ids"],json!(["c","d"]));
        assert_eq!(view.cursor,0);
        use crate::source_public_api::{ConfiguredScope, ConfiguredMarket, full_sync, encode_bounded, EncodeError};
        let mut scope = ConfiguredScope { schema_version:"marketcow.polymarket.scope-discovery.v1".into(),
            mode:"shadow".into(),catalog_revision:"a".repeat(64),active_scope_id:String::new(),configured_market_count:2,
            configured_markets:vec![ConfiguredMarket{market_id:"1".into(),condition_id:"1".into(),token_ids:["a".into(),"b".into()],end_at:now.to_rfc3339()},
                ConfiguredMarket{market_id:"2".into(),condition_id:"2".into(),token_ids:["c".into(),"d".into()],end_at:now.to_rfc3339()}] };
        scope.active_scope_id = marketcow_polymarket::discovery_source::canonical_hash(&json!({
            "catalog_revision":scope.catalog_revision,"configured_markets":scope.configured_markets,"mode":scope.mode}));
        let sync = full_sync(&view,&scope,1,"test-instance",now).unwrap();
        assert_eq!(sync["snapshot"]["count"],2);
        assert_eq!(sync["health"]["missing_market_count"],1);
        assert_eq!(sync["health"]["complete_market_count"],1);
        for component in ["health","bootstrap","snapshot"] {
            for key in ["catalog_revision","projection_generation","scope_market_ids","freshness_checked_at","scope_id"] {
                assert_eq!(sync[key],sync[component][key]);
            }
        }
        assert!(matches!(encode_bounded(&sync,1),Err(EncodeError::TooLarge)));
        assert_eq!(sync["cursor"],sync["snapshot"]["cursor"]);
        assert_eq!(sync["cursor"],sync["bootstrap"]["cursor"]);
        assert_eq!(sync["cursor"],sync["health"]["latest_cursor"]);
        use tower::ServiceExt;
        let path = format!("/v1/prediction-markets/polymarket/live/full-sync?scope_id={}",scope.active_scope_id);
        let limits = crate::source_public_api::StreamLimits {frame_bytes:131072,replay_bytes:131072,clients:2,send_timeout:std::time::Duration::from_secs(1)};
        let router = crate::source_public_api::PublicApi::router(p.reader(),scope.clone(),1,"test-instance".into(),131072,1,limits.clone()).unwrap();
        let response = router.oneshot(axum::http::Request::builder().uri(&path).body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(),axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(),131072).await.unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["scope_market_ids"],json!(["1","2"]));
        let router = crate::source_public_api::PublicApi::router(p.reader(),scope.clone(),1,"test-instance".into(),131072,1,limits.clone()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener,router).await.unwrap(); });
        let (mut ws,_) = tokio_tungstenite::connect_async(format!("ws://{address}/v1/prediction-markets/polymarket/live/stream?scope_id={}&after_cursor=0",scope.active_scope_id)).await.unwrap();
        use futures_util::{StreamExt,SinkExt};
        let message = tokio::time::timeout(std::time::Duration::from_secs(2),ws.next()).await.unwrap().unwrap().unwrap();
        let ready: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
        assert_eq!(ready["type"],"ready");
        assert_eq!(ready["cursor"],0);
        assert_eq!(ready["stream_instance_id"],"test-instance");
        assert_eq!(ready["confirmation_books"],json!([]));
        ws.send(tokio_tungstenite::tungstenite::Message::Ping(vec![1,2].into())).await.unwrap();
        let pong = tokio::time::timeout(std::time::Duration::from_secs(2),ws.next()).await.unwrap().unwrap().unwrap();
        assert!(pong.is_pong());
        ws.close(None).await.unwrap();
        server.abort();
        let _ = server.await;
        let router = crate::source_public_api::PublicApi::router(p.reader(),scope,1,"test-instance".into(),1,1,limits).unwrap();
        let response = router.oneshot(axum::http::Request::builder().uri(path).body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(),axum::http::StatusCode::PAYLOAD_TOO_LARGE);
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn terminal_keeps_identity_without_requiring_l2() {
        let mut m = market("1",["a","b"]);
        m["lifecycle_state"] = json!("closed");
        let p = Publication::start(0,Some(json!({"latest_cursor":0,"gaps":[],"books":[],"markets":[m]})),
            [("a".into(),"1".into()),("b".into(),"1".into())].into(),8,131072,65536, |_|Ok(())).unwrap();
        let result = frame(&p.capture_view().unwrap(),"1",Utc::now(),1).unwrap();
        assert_eq!(result["market_id"],"1");
        assert_eq!(result["status"],"terminal");
        assert_eq!(result["required_token_ids"],json!([]));
        assert_eq!(result["reason_codes"],json!(["market_closed"]));
        p.finish().await.unwrap();
    }
}
