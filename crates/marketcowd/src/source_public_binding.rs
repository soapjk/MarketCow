//! Source-backed instrument tick binding, matching the public live contract.
use anyhow::{Context,Result};
use serde_json::{json,Value};
use std::{collections::BTreeMap,sync::Arc,str::FromStr};
use rust_decimal::Decimal;
use marketcow_polymarket::discovery_source::canonical_hash;

pub fn bind(market: &Arc<Value>, books: &BTreeMap<String,Arc<Value>>, cursor:u64,
    generation:u64, observed:&str) -> Result<Arc<Value>> {
    let mut tokens: Vec<_> = market["identity"]["outcomes"].as_array().context("outcomes")?.iter()
        .map(|o|o["token_id"].as_str().context("token")).collect::<Result<_>>()?;
    tokens.sort();
    if tokens.len()!=2 { anyhow::bail!("binary outcomes required"); }
    let (Some(a),Some(b))=(books.get(tokens[0]),books.get(tokens[1])) else { return Ok(market.clone()); };
    let tick = a["tick_size"].as_str().context("book tick")?;
    let tick_decimal=Decimal::from_str(tick)?;
    if tick_decimal!=Decimal::from_str(b["tick_size"].as_str().context("book tick")?)?
        || a["tick_version"]!=b["tick_version"] {return Ok(market.clone());}
    let tick_version=a["tick_version"].as_str().context("tick version")?;
    let instrument=&market["rules"]["instrument"];
    let provenance=instrument["provenance"].as_array().context("instrument provenance")?;
    let same=instrument["price_increment"].as_str().map(Decimal::from_str).transpose()?==Some(tick_decimal);
    let dynamic=provenance.iter().any(|p|p["source"]=="polymarket_clob");
    if same && (!dynamic || provenance.iter().any(|p|p["source"]=="polymarket_clob"
        && p["projection_generation"].as_u64().is_some() && p["tick_version"]==tick_version)) {return Ok(market.clone());}
    let boundary=json!({"binding":"polymarket_clob_book_tick_v2","market_id":market["identity"]["market_id"],
        "token_ids":tokens,"price_increment":tick,"tick_version":tick_version,"cursor":cursor,"projection_generation":generation});
    let revision=canonical_hash(&boundary);
    let mut evidence=boundary.clone();
    evidence["books"]=json!({tokens[0]:{"tick_size":a["tick_size"],"tick_version":a["tick_version"],"state_checksum":a["state_checksum"]},
        tokens[1]:{"tick_size":b["tick_size"],"tick_version":b["tick_version"],"state_checksum":b["state_checksum"]}});
    let mut next=market.as_ref().clone();
    next["rules"]["instrument"]["price_increment"]=json!(tick);
    next["rules"]["instrument"]["provenance"].as_array_mut().context("provenance")?.push(json!({
        "source":"polymarket_clob","revision":revision,"source_url":"wss://ws-subscriptions-clob.polymarket.com/ws/market",
        "observed_at":observed,"payload_sha256":canonical_hash(&evidence),
        "field_paths":["event_type","new_tick_size","asset_id","book.tick_size","book.tick_version"],
        "boundary_cursor":cursor,"projection_generation":generation,"tick_version":tick_version,"token_ids":tokens}));
    next["rules"]["instrument"]["revision"]=json!(canonical_hash(&json!({"previous_revision":instrument["revision"],
        "dynamic_tick_boundary":boundary,"provenance_revision":revision})));
    next["metadata_revision"]=json!(canonical_hash(&json!({"previous_revision":market["metadata_revision"],
        "instrument_revision":next["rules"]["instrument"]["revision"],"dynamic_tick_boundary":boundary})));
    Ok(Arc::new(next))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mixed_tick_preserves_prior_facts_then_complete_pair_binds_once() {
        let market=Arc::new(json!({"identity":{"market_id":"1","outcomes":[{"token_id":"a"},{"token_id":"b"}]},
            "metadata_revision":"old","rules":{"instrument":{"price_increment":"0.01","revision":"old-inst","provenance":[]}}}));
        let mut books:BTreeMap<_,_>=[("a".into(),Arc::new(json!({"tick_size":"0.001","tick_version":"new","state_checksum":"a"}))),
            ("b".into(),Arc::new(json!({"tick_size":"0.01","tick_version":"old","state_checksum":"b"})))].into();
        assert!(Arc::ptr_eq(&market,&bind(&market,&books,7,1,"2026-09-06T00:00:00Z").unwrap()));
        books.insert("b".into(),Arc::new(json!({"tick_size":"0.001","tick_version":"new","state_checksum":"b"})));
        let next=bind(&market,&books,8,1,"2026-09-06T00:00:00Z").unwrap();
        assert_eq!(market["rules"]["instrument"]["price_increment"],"0.01");
        assert_eq!(next["rules"]["instrument"]["price_increment"],"0.001");
        assert_eq!(next["rules"]["instrument"]["provenance"][0]["boundary_cursor"],8);
        assert!(Arc::ptr_eq(&next,&bind(&next,&books,9,1,"2026-09-06T00:00:01Z").unwrap()));
    }
}
