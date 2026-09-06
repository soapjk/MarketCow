//! Discovery v3 quote projection from an explicit immutable source boundary.
//! No network, persistence or business defaults are used by this codec.
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use marketcow_polymarket::discovery_source::canonical_hash;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::{collections::{BTreeMap, BTreeSet}, str::FromStr, sync::Arc};

pub struct QuotePolicy {
    pub quantities: Vec<String>,
    pub maximum_book_age_ms: i64,
}
impl QuotePolicy {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.maximum_book_age_ms > 0, "explicit positive book age required");
        ensure!(!self.quantities.is_empty(), "explicit depth quantities required");
        let mut previous = Decimal::ZERO;
        for quantity in &self.quantities {
            let value = decimal(quantity)?;
            ensure!(value > previous, "depth quantities must be positive and increasing");
            previous = value;
        }
        Ok(())
    }
}
fn decimal(text: &str) -> Result<Decimal> { Ok(Decimal::from_str(text)?) }
fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key].as_str().with_context(||format!("required string {key}"))
}
fn instant(value: &Value, key: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(text(value,key)?)?.with_timezone(&Utc))
}
fn age(now: DateTime<Utc>, received: DateTime<Utc>) -> i64 {
    now.signed_duration_since(received).num_milliseconds().max(0)
}
fn wire_time(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(if value.timestamp_subsec_micros()==0 {
        chrono::SecondsFormat::Secs
    } else { chrono::SecondsFormat::Micros },true)
}
fn levels(book: &Value, side: &str, reverse: bool) -> Result<Vec<(Decimal,Decimal,String,String)>> {
    let mut result = Vec::new();
    for item in book[side].as_array().context("book levels")? {
        let price = text(item,"price")?; let size = text(item,"size")?;
        let p = decimal(price)?; let s = decimal(size)?;
        ensure!(p >= Decimal::ZERO && p <= Decimal::ONE && s >= Decimal::ZERO,"invalid level");
        result.push((p,s,price.into(),size.into()));
    }
    result.sort_by(|a,b| if reverse { b.0.cmp(&a.0) } else { a.0.cmp(&b.0) });
    Ok(result)
}
fn depth(levels: &[(Decimal,Decimal,String,String)], quantity: Decimal) -> Result<Option<String>> {
    let mut remaining = quantity; let mut total = Decimal::ZERO;
    for (price,size,_,_) in levels {
        let fill = remaining.min(*size);
        total = total.checked_add(fill.checked_mul(*price).context("depth multiplication overflow")?)
            .context("depth sum overflow")?;
        remaining = remaining.checked_sub(fill).context("depth subtraction overflow")?;
        if remaining.is_zero() { return Ok(Some(total.to_string())); }
    }
    Ok(None)
}
fn outcome(name: &str, token: &str, book: Option<&Value>, now: DateTime<Utc>, policy: &QuotePolicy) -> Result<Value> {
    let (bids,asks) = match book { Some(b)=>(levels(b,"bids",true)?,levels(b,"asks",false)?),None=>(vec![],vec![]) };
    let mut depths = Vec::new();
    for quantity in &policy.quantities {
        let quantity_decimal = decimal(quantity)?;
        let buy = depth(&asks,quantity_decimal)?; let sell = depth(&bids,quantity_decimal)?;
        depths.push(json!({"notional":quantity,"buy_cost_at_notional":buy,"sell_proceeds_at_notional":sell,
            "buy_status":if book.is_none(){"book_unavailable"}else if buy.is_some(){"complete"}else{"insufficient_depth"},
            "sell_status":if book.is_none(){"book_unavailable"}else if sell.is_some(){"complete"}else{"insufficient_depth"}}));
    }
    let received = book.map(|b|instant(b,"received_at")).transpose()?;
    Ok(json!({"outcome":name,"token_id":token,"best_bid":bids.first().map(|l|&l.2),
        "best_bid_size":bids.first().map(|l|&l.3),"best_ask":asks.first().map(|l|&l.2),
        "best_ask_size":asks.first().map(|l|&l.3),"last_trade_price":book.and_then(|b|b.get("last_trade_price")),
        "depth":depths,"book_observed_at":received.map(wire_time),"book_age_ms":received.map(|t|age(now,t)),
        "book_revision":book.map(|b|text(b,"state_checksum")).transpose()?}))
}

/// `settlement` is an already validated authoritative metadata fact or explicit
/// null. Lifecycle resolution by itself is never evidence of redeemability.
pub fn market_quote(market: &Value, books: &BTreeMap<String,Arc<Value>>, policy: &QuotePolicy,
    relation_complete: bool, has_gap: bool, catalog: &str, cursor: u64, now: DateTime<Utc>, settlement: &Value) -> Result<Value> {
    policy.validate()?;
    let identity = &market["identity"];
    let outcomes = identity["outcomes"].as_array().context("outcomes")?;
    ensure!(outcomes.len()==2,"exactly two outcomes required");
    let mut named = outcomes.iter().map(|o|Ok((text(o,"outcome")?.to_lowercase(),o))).collect::<Result<Vec<_>>>()?;
    let yes_no = named.iter().map(|(n,_)|n.as_str()).collect::<BTreeSet<_>>() == BTreeSet::from(["yes","no"]);
    if yes_no { named.sort_by_key(|(n,_)| n != "yes"); }
    let mut present = Vec::new(); let mut quotes = Vec::new(); let mut missing = BTreeSet::new(); let mut status = "ready";
    for (name, item) in &named {
        let token = text(item,"token_id")?;
        let book = books.get(token).map(Arc::as_ref);
        if let Some(book) = book { present.push((token,book)); }
        else { missing.insert(format!("book:{token}")); }
        quotes.push(outcome(&if yes_no{name.to_uppercase()}else{text(item,"outcome")?.into()},token,book,now,policy)?);
    }
    if present.len()!=2 {status="missing_book";}
    else if present.iter().any(|(_,b)| b["bids"].as_array().is_some_and(Vec::is_empty)||b["asks"].as_array().is_some_and(Vec::is_empty)) {
        status="incomplete_book";missing.insert("two_sided_book".into());
    } else if present.iter().map(|(_,b)|instant(b,"received_at")).collect::<Result<Vec<_>>>()?.iter().any(|t|age(now,*t)>policy.maximum_book_age_ms) {
        status="stale_book";missing.insert("fresh_book".into());
    }
    let instrument = &market["rules"]["instrument"];
    let tick = instrument.get("price_increment").context("tick field absent")?;
    let minimum = instrument.get("minimum_order_size").context("minimum field absent")?;
    if tick.is_null(){status="missing_tick";missing.insert("tick_size".into());}
    else {let t=decimal(tick.as_str().context("tick text")?)?;ensure!(t>Decimal::ZERO,"positive tick");
        if present.iter().any(|(_,b)| b["tick_size"]!=*tick){status="inconsistent_tick";missing.insert("atomic_tick_revision".into());}}
    if minimum.is_null(){status="missing_minimum_order_size";missing.insert("minimum_order_size".into());}
    else {ensure!(decimal(minimum.as_str().context("minimum text")?)?>Decimal::ZERO,"positive minimum");}
    let fee=&market["rules"]["fee_schedule"];
    let fee_complete=fee["complete"].as_bool().context("fee completeness")?;
    if !fee_complete {status="missing_fee_schedule";for f in fee["missing_fields"].as_array().context("fee missing fields")? {
        missing.insert(format!("fee_schedule:{}",f.as_str().context("fee missing field")?));}}
    let relation_id=market["relations"].as_array().context("relations")?.iter()
        .find(|r|r["relation_type"]=="standard_negative_risk").map(|r|text(r,"relation_id")).transpose()?;
    let negative=identity["neg_risk"].as_bool().context("negative risk")?;
    if negative && (relation_id.is_none()||!relation_complete){status="incomplete_relation";missing.insert("complete_negative_risk_relation".into());}
    if has_gap {status="source_gap";missing.insert("unresolved_source_gap".into());}
    if !yes_no {status="missing_outcome_identity";missing.extend(["yes_token_id".into(),"no_token_id".into()]);}
    ensure!(status=="ready" || !missing.is_empty(),"failed quote missing reason");
    let received=present.iter().map(|(_,b)|instant(b,"received_at")).collect::<Result<Vec<_>>>()?.into_iter().min();
    let checksums:BTreeMap<_,_>=present.iter().map(|(token,b)|Ok((*token,text(b,"state_checksum")?))).collect::<Result<_>>()?;
    let revision=if present.len()==2{Some(canonical_hash(&serde_json::to_value(checksums)?))}else{None};
    let mut result=json!({"market_id":text(identity,"market_id")?,"condition_id":text(identity,"condition_id")?,"event_id":text(identity,"event_id")?,
        "yes_token_id":if yes_no{Some(text(named[0].1,"token_id")?)}else{None},"no_token_id":if yes_no{Some(text(named[1].1,"token_id")?)}else{None},
        "negative_risk":negative,"negative_risk_relation_id":relation_id,"outcomes":quotes,"book_observed_at":received.map(wire_time),
        "book_age_ms":received.map(|t|age(now,t)),"book_status":status,"tick_size":tick,"minimum_order_size":minimum,
        "fee_schedule_id":if fee_complete{Some(text(fee,"schedule_id")?)}else{None},"missing_fields":missing,
        "metadata_revision":text(market,"metadata_revision")?,"book_revision":revision,"catalog_revision":catalog,"cursor":cursor,"settlement":settlement});
    for field in ["active","closed","accepting_orders"] {result[field]=json!(market[field].as_bool().with_context(||format!("missing {field}"))?);}
    for field in ["lifecycle_state","start_at","end_at"] {result[field]=market.get(field).with_context(||format!("missing {field}"))?.clone();}
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "explicit hash-bound Python oracle required"]
    fn captured_python_quote_parity() {
        use sha2::{Digest,Sha256};
        let raw=std::fs::read(std::env::var("DISCOVERY_QUOTE_GOLDEN").unwrap()).unwrap();
        assert_eq!(hex::encode(Sha256::digest(&raw)),std::env::var("DISCOVERY_QUOTE_GOLDEN_SHA256").unwrap());
        let data:Value=serde_json::from_slice(&raw).unwrap();
        let books=data["books"].as_object().unwrap().iter().map(|(k,v)|(k.clone(),Arc::new(v.clone()))).collect();
        let policy=QuotePolicy{quantities:serde_json::from_value(data["quantities"].clone()).unwrap(),maximum_book_age_ms:data["maximum_book_age_ms"].as_i64().unwrap()};
        for case in data["cases"].as_array().unwrap() {
            let actual=market_quote(&case["market"],&books,&policy,case["relation_complete"].as_bool().unwrap(),
                case["has_gap"].as_bool().unwrap(),data["catalog_revision"].as_str().unwrap(),data["cursor"].as_u64().unwrap(),
                instant(&data,"observed_at").unwrap(),&case["settlement"]).unwrap();
            assert_eq!(actual,case["expected"],"market {}",case["market"]["identity"]["market_id"]);
        }
        eprintln!("exact market quote comparisons: {}",data["cases"].as_array().unwrap().len());
    }
    #[test]
    fn explicit_depth_exact_decimal_and_insufficient_side() {
        let policy=QuotePolicy{quantities:vec!["5".into(),"10".into()],maximum_book_age_ms:5000};
        policy.validate().unwrap();
        let now=DateTime::from_timestamp(1700000000,0).unwrap();
        let book=json!({"received_at":now.to_rfc3339(),"state_checksum":"a".repeat(64),"last_trade_price":null,
            "bids":[{"price":"0.2","size":"3"},{"price":"0.3","size":"2"}],
            "asks":[{"price":"0.5","size":"5"}]});
        let quote=outcome("YES","t",Some(&book),now,&policy).unwrap();
        assert_eq!(quote["best_bid"],"0.3");assert_eq!(quote["depth"][0]["sell_proceeds_at_notional"],"1.2");
        assert_eq!(quote["depth"][0]["buy_cost_at_notional"],"2.5");
        assert_eq!(quote["depth"][1]["buy_status"],"insufficient_depth");
        let missing=outcome("YES","t",None,now,&policy).unwrap();assert_eq!(missing["depth"][0]["buy_status"],"book_unavailable");
        assert!(QuotePolicy{quantities:vec!["5".into(),"5.0".into()],maximum_book_age_ms:5000}.validate().is_err());
        assert!(QuotePolicy{quantities:vec![],maximum_book_age_ms:5000}.validate().is_err());
    }
}
