//! Bounded point-in-time Discovery delta history. Payloads are frozen at the
//! event boundary, never reconstructed from a newer current-state quote.
use anyhow::{Context, Result, ensure};
use serde_json::{Value,json};
use std::{collections::VecDeque,sync::Arc};

pub struct DiscoveryHistory {
    projection: String,
    catalog: String,
    universe: String,
    cursor: u64,
    bytes: usize,
    maximum_events: usize,
    maximum_bytes: usize,
    entries: VecDeque<Entry>,
}
struct Entry { cursor: u64, items: Arc<Value>, bytes: usize }

impl DiscoveryHistory {
    pub fn new(projection: String,catalog: String,universe: String,cursor: u64,
        maximum_events: usize,maximum_bytes: usize) -> Result<Self> {
        for hash in [&projection,&catalog,&universe] {
            ensure!(hash.len()==64 && hash.bytes().all(|b|b.is_ascii_digit()||(b'a'..=b'f').contains(&b)),"invalid revision hash");
        }
        ensure!(maximum_events>0 && maximum_bytes>0,"explicit positive history budgets required");
        Ok(Self{projection,catalog,universe,cursor,bytes:0,maximum_events,maximum_bytes,entries:VecDeque::new()})
    }
    pub fn append(&mut self,cursor: u64,items: Value) -> Result<()> {
        ensure!(self.cursor.checked_add(1)==Some(cursor),"discovery event discontinuity");
        for item in items.as_array().context("typed delta array required")? {
            let map=item.as_object().context("typed delta object")?;
            ensure!(map.len()==2 && map.contains_key("type") && map.contains_key("payload"),"typed delta requires type/payload only");
            let payload=&item["payload"];
            ensure!(payload.is_object(),"payload object required");
            ensure!(payload["catalog_revision"]==self.catalog,"delta catalog binding");
            match item["type"].as_str() {
                Some("market_update") => {
                    ensure!(payload["cursor"].as_u64()==Some(cursor),"quote cursor must equal event boundary");
                    ensure!(payload["market_id"].as_str().is_some_and(|id|!id.is_empty()),"market identity");
                },
                Some("relation_update") => {
                    ensure!(payload["relation_id"].as_str().is_some_and(|id|!id.is_empty()),"relation identity");
                },
                _=>anyhow::bail!("universe/schema changes require a new projection baseline"),
            }
        }
        let bytes=serde_json::to_vec(&items)?.len();
        ensure!(bytes<=self.maximum_bytes,"single discovery delta exceeds history byte budget");
        // Validate and size everything first; failed appends leave the current
        // boundary and existing replay history unchanged.
        while !self.entries.is_empty() && (self.entries.len()>=self.maximum_events || self.bytes>self.maximum_bytes-bytes) {
            self.bytes-=self.entries.pop_front().unwrap().bytes;
        }
        self.entries.push_back(Entry{cursor,items:Arc::new(items),bytes});
        self.bytes+=bytes;self.cursor=cursor;
        Ok(())
    }
    pub fn frame(&self,projection: &str,after: u64) -> Value {
        let matching=projection==self.projection;
        let entry=after.checked_add(1).and_then(|next| {
            let first=self.entries.front()?.cursor;
            let offset=usize::try_from(next.checked_sub(first)?).ok()?;
            self.entries.get(offset)
        });
        let resync=!matching || after>self.cursor || (after<self.cursor && entry.is_none());
        let next=if resync {after}else{entry.map(|e|e.cursor).unwrap_or(after)};
        let items=if resync {json!([])}else{entry.map(|e|e.items.as_ref().clone()).unwrap_or_else(||json!([]))};
        json!({"schema_version":"marketcow.polymarket.discovery-events.v3","projection_id":projection,
            "catalog_revision":self.catalog,"universe_revision":self.universe,
            "after_cursor":after,"next_cursor":next,"boundary_cursor":self.cursor,
            "has_more":!resync && next<self.cursor,"resync_required":resync,"items":items})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn history(count:usize,bytes:usize)->DiscoveryHistory {
        DiscoveryHistory::new("a".repeat(64),"b".repeat(64),"c".repeat(64),10,count,bytes).unwrap()
    }
    // Synthetic minimal payloads isolate the history invariant; full quote
    // fields are covered by the separate captured 250-market codec oracle.
    fn item(cursor:u64)->Value {json!([{"type":"market_update","payload":{
        "market_id":"m","catalog_revision":"b".repeat(64),"cursor":cursor}}])}
    #[test]
    fn future_quote_rejected_and_point_in_time_payload_retained() {
        let mut h=history(2,4096);
        assert!(h.append(11,item(12)).is_err());assert_eq!(h.cursor,10);
        h.append(11,item(11)).unwrap();h.append(12,item(12)).unwrap();
        let first=h.frame(&"a".repeat(64),10);
        assert_eq!(first["next_cursor"],11);assert_eq!(first["items"][0]["payload"]["cursor"],11);
        assert_eq!(first["boundary_cursor"],12);assert_eq!(first["resync_required"],false);
        h.append(13,json!([])).unwrap();
        assert_eq!(h.frame(&"a".repeat(64),10)["resync_required"],true);
        let last=h.frame(&"a".repeat(64),12);
        assert_eq!(last["next_cursor"],13);assert_eq!(last["items"],json!([]));
        assert_eq!(h.frame(&"d".repeat(64),12)["resync_required"],true);
        assert_eq!(h.frame(&"a".repeat(64),14)["resync_required"],true);
    }
    #[test]
    fn byte_budget_and_failed_append_are_atomic() {
        let size=serde_json::to_vec(&item(11)).unwrap().len();
        let mut h=history(10,size);
        h.append(11,item(11)).unwrap();
        let old=h.frame(&"a".repeat(64),10);
        let mut oversized=item(12);oversized[0]["payload"]["extra"]=json!("oversized");
        assert!(h.append(12,oversized).is_err());assert_eq!(h.frame(&"a".repeat(64),10),old);
        h.append(12,item(12)).unwrap();assert_eq!(h.entries.len(),1);assert!(h.bytes<=size);
        assert!(h.append(14,item(14)).is_err());assert_eq!(h.cursor,12);
    }
}
