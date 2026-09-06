//! Per-consumer point-in-time projection over immutable native source events.
//! No shared Python/WS intermediary and no online SQLite reads.
use anyhow::{Context,Result,ensure};
use chrono::{DateTime,Utc};
use marketcow_runtime::discovery_source::{ValidatedSourceBatch,apply_token_recovery,is_token_recovery};
use serde_json::{Value,json};
use std::{collections::{BTreeMap,BTreeSet},sync::Arc};
use crate::{source_discovery_quote::{market_quote,QuotePolicy},source_discovery_history::DiscoveryHistory,source_publication::MemoryView};

fn relation_wire(relation:&Value)->Value {
    let mut wire=relation.clone();
    wire["schema_version"]=json!("marketcow.polymarket.discovery-relation.v3");
    wire
}

pub struct DiscoveryConfig {
    pub projection_id:String,
    pub catalog_revision:String,
    pub universe_revision:String,
    pub market_ids:Vec<String>,
    pub relations:Vec<Value>,
    pub settlements:BTreeMap<String,Value>,
    pub policy:QuotePolicy,
}
impl DiscoveryConfig {
    pub fn validate(&self,view:&MemoryView)->Result<()> {
        self.policy.validate()?;
        ensure!(view.base["catalog_revision"]==self.catalog_revision,"source catalog differs");
        ensure!(!self.market_ids.is_empty() && self.market_ids.len()<=1024,"explicit market universe bounds");
        let ids:BTreeSet<_>=self.market_ids.iter().collect();
        ensure!(ids.len()==self.market_ids.len(),"duplicate universe market");
        ensure!(ids==self.settlements.keys().collect(),"explicit settlement value required per market");
        for id in &self.market_ids {ensure!(view.markets.contains_key(id),"universe metadata absent: {id}");}
        let mut relations=BTreeSet::new();
        for relation in &self.relations {
            ensure!(relation["catalog_revision"]==self.catalog_revision,"relation catalog differs");
            ensure!(relations.insert(relation["relation_id"].as_str().context("relation id")?),"duplicate relation");
            let members=relation["members"].as_array().context("relation members")?;
            ensure!(relation["actual_member_count"].as_u64()==Some(members.len() as u64),"relation count differs");
            let declared=relation["member_market_ids"].as_array().context("relation market ids")?;
            ensure!(declared==&members.iter().map(|m|m["market_id"].clone()).collect::<Vec<_>>(),"relation identity differs");
            let reasons=relation["reason_codes"].as_array().context("relation reasons")?;
            let complete=relation["expected_member_count"].as_u64()==Some(members.len() as u64)&&members.len()>=2&&reasons.is_empty();
            ensure!(relation["complete"].as_bool()==Some(complete),"relation completeness differs");
        }
        Ok(())
    }
    fn quote(&self,view:&MemoryView,id:&str,now:DateTime<Utc>)->Result<Value> {
        let market=view.markets.get(id).context("metadata missing")?;
        let related=market["relations"].as_array().context("market relations")?;
        let complete=related.iter().filter(|r|r["relation_type"]=="standard_negative_risk")
            .all(|r|self.relations.iter().any(|fact|fact["relation_id"]==r["relation_id"]&&fact["complete"]==true));
        let tokens:BTreeSet<_>=market["identity"]["outcomes"].as_array().context("outcomes")?.iter()
            .map(|o|o["token_id"].as_str().context("token")).collect::<Result<_>>()?;
        let has_gap=view.recoveries.keys().any(|t|tokens.contains(t.as_str())) ||
            view.base["gaps"].as_array().context("source gaps")?.iter().any(|g|g["resolved"]==false &&
                (g["token_id"].as_str().is_some_and(|t|tokens.contains(t)) || g["market_id"]==*id || (g["token_id"].is_null()&&g["market_id"].is_null())));
        market_quote(market,&view.books,&self.policy,complete,has_gap,&self.catalog_revision,view.cursor,now,&self.settlements[id])
    }
    pub fn full_sync(&self,view:&MemoryView,observed:DateTime<Utc>)->Result<Value> {
        self.validate(view)?;
        let markets=self.market_ids.iter().map(|id|self.quote(view,id,observed)).collect::<Result<Vec<_>>>()?;
        let unresolved=view.recoveries.len()+view.base["gaps"].as_array().context("source gaps")?.iter().filter(|g|g["resolved"]==false).count();
        let ready=unresolved==0;
        let relations=self.relations.iter().map(relation_wire).collect::<Vec<_>>();
        Ok(json!({"schema_version":"marketcow.polymarket.discovery.v3","projection_id":self.projection_id,
            "catalog_revision":self.catalog_revision,"universe_revision":self.universe_revision,"boundary_cursor":view.cursor,
            "observed_at":observed,"ready":ready,"fail_closed_reason":if ready{None}else{Some("discovery_unresolved_gaps")},
            "unresolved_gap_count":unresolved,"depth_notionals":self.policy.quantities,"markets":markets,"relations":relations}))
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;
    #[test]
    fn relation_wire_adds_required_schema_without_rewriting_evidence() {
        let fact=json!({"relation_id":"r","evidence_sha256":"a".repeat(64),"members":[]});
        let mut wire=relation_wire(&fact);
        assert_eq!(wire.as_object_mut().unwrap().remove("schema_version"),Some(json!("marketcow.polymarket.discovery-relation.v3")));
        assert_eq!(wire,fact);
        assert!(fact.get("schema_version").is_none());
    }
}

pub struct DiscoveryConsumer {
    config:Arc<DiscoveryConfig>,
    view:MemoryView,
    history:DiscoveryHistory,
    maximum_state_bytes:usize,
    state_bytes:usize,
    book_bytes:BTreeMap<String,usize>,
    market_bytes:BTreeMap<String,usize>,
    recovery_bytes:BTreeMap<String,usize>,
    selected:BTreeSet<String>,
}
impl DiscoveryConsumer {
    pub fn new(config:Arc<DiscoveryConfig>,view:MemoryView,maximum_frame_bytes:usize,maximum_state_bytes:usize)->Result<Self> {
        config.validate(&view)?;
        let history=DiscoveryHistory::new(config.projection_id.clone(),config.catalog_revision.clone(),config.universe_revision.clone(),view.cursor,1,maximum_frame_bytes)?;
        let book_bytes=view.books.iter().map(|(id,b)|Ok((id.clone(),serde_json::to_vec(b)?.len()))).collect::<Result<BTreeMap<_,_>>>()?;
        let market_bytes=view.markets.iter().map(|(id,m)|Ok((id.clone(),serde_json::to_vec(m)?.len()))).collect::<Result<BTreeMap<_,_>>>()?;
        let recovery_bytes=view.recoveries.iter().map(|(id,m)|Ok((id.clone(),serde_json::to_vec(m)?.len()))).collect::<Result<BTreeMap<_,_>>>()?;
        let state_bytes=book_bytes.values().chain(market_bytes.values()).chain(recovery_bytes.values()).sum::<usize>()+serde_json::to_vec(&view.base)?.len();
        ensure!(state_bytes<=maximum_state_bytes,"consumer book state byte budget");
        let selected=config.market_ids.iter().cloned().collect();
        Ok(Self{config,view,history,maximum_state_bytes,state_bytes,book_bytes,market_bytes,recovery_bytes,selected})
    }
    pub fn cursor(&self)->u64 {self.view.cursor}
    /// Events must come from the native validated batch, not arbitrary client
    /// input. Errors terminate this consumer; no partial state is reused.
    pub fn apply_event(&mut self,batch:&ValidatedSourceBatch,index:usize)->Result<Option<Value>> {
            let event=batch.events().get(index).context("batch event index")?;
            let cursor=event["cursor"].as_u64().context("event cursor")?;
            if cursor<=self.view.cursor {return Ok(None);}
            ensure!(self.view.cursor.checked_add(1)==Some(cursor),"source event discontinuity");
            let id=event["market_id"].as_str().context("source market identity")?;
            let selected=self.selected.contains(id);
            if selected {
                let market=self.view.markets.get(id).context("source metadata absent")?;
                ensure!(market["identity"]["condition_id"]==event["condition_id"],"event condition differs");
                if !event["token_id"].is_null() {
                    ensure!(market["identity"]["outcomes"].as_array().context("outcomes")?.iter()
                        .any(|o|o["token_id"]==event["token_id"]),"event token differs");
                }
                apply_token_recovery(&mut self.view.recoveries,event)?;
                if let Some(token)=event["token_id"].as_str() {
                    let bytes=self.view.recoveries.get(token).map(serde_json::to_vec).transpose()?.map_or(0,|v|v.len());
                    let next=self.state_bytes.checked_sub(self.recovery_bytes.get(token).copied().unwrap_or(0)).context("recovery accounting")?
                        .checked_add(bytes).context("recovery overflow")?;
                    ensure!(next<=self.maximum_state_bytes,"consumer recovery byte budget");
                    if bytes==0 {self.recovery_bytes.remove(token);}else{self.recovery_bytes.insert(token.into(),bytes);}
                    self.state_bytes=next;
                }
                if !is_token_recovery(event) {
                    match event["event_type"].as_str() {
                        Some("book"|"price_change"|"last_trade_price"|"best_bid_ask"|"tick_size_change")=>{
                            let token=event["token_id"].as_str().context("book token")?;
                            let book=&event["canonical_payload"];
                            ensure!(book["token_id"]==token && book["condition_id"]==event["condition_id"],"book identity differs");
                            let bytes=serde_json::to_vec(book)?.len();
                            let next=self.state_bytes.checked_sub(self.book_bytes.get(token).copied().unwrap_or(0)).context("state accounting")?
                                .checked_add(bytes).context("state overflow")?;
                            ensure!(next<=self.maximum_state_bytes,"consumer state byte budget");
                            self.view.books.insert(token.into(),Arc::new(book.clone()));self.book_bytes.insert(token.into(),bytes);self.state_bytes=next;
                        },
                        Some("market_terminal"|"market_resolved")=>{
                            let next=&event["canonical_payload"]["market"];
                            ensure!(next["identity"]==market["identity"],"terminal identity differs");
                            let bytes=serde_json::to_vec(next)?.len();
                            let total=self.state_bytes.checked_sub(self.market_bytes[id]).context("metadata accounting")?
                                .checked_add(bytes).context("metadata overflow")?;
                            ensure!(total<=self.maximum_state_bytes,"consumer metadata byte budget");
                            self.state_bytes=total;self.market_bytes.insert(id.into(),bytes);
                            self.view.markets.insert(id.into(),Arc::new(next.clone()));
                        },
                        _=>anyhow::bail!("source schema/catalog change requires new full-sync"),
                    }
                }
            }
            let previous=self.view.cursor;self.view.cursor=cursor;
            let now=DateTime::parse_from_rfc3339(event["received_at"].as_str().context("event observed time")?)?.with_timezone(&Utc);
            let items=if selected {json!([{"type":"market_update","payload":self.config.quote(&self.view,id,now)?}])}else{json!([])};
            self.history.append(cursor,items)?;
            Ok(Some(self.history.frame(&self.config.projection_id,previous)))
    }
}
