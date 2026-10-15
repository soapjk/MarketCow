//! Public market state, deliberately independent of upstream/profile epochs.
//! Captures existing immutable reducer books; never replays supplier frames.
use std::{collections::BTreeMap, sync::{Arc, Mutex}};
use anyhow::{Context, Result, ensure};
use arc_swap::ArcSwapOption;
use marketcow_contracts::btc5m_market_local_v6::{self as contract, wire};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use crate::{source_btc5m_api::MarketBinding, source_publication::MarketMemorySnapshot};

pub const MAX_LEVELS: usize = 10_000;
pub const MAX_CHANGES: usize = MAX_LEVELS * 4;
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_STATE_BYTES: usize = 8 * 1024 * 1024;

fn decimal(value: &str) -> Result<String> {
    let value: Decimal = value.parse().context("live decimal")?;
    ensure!(value >= Decimal::ZERO, "negative live decimal");
    Ok(value.normalize().to_string())
}
fn hash(value: &Value) -> Result<Vec<u8>> {
    Ok(Sha256::digest(crate::source_btc5m_proto::canonical_json_bytes(value)?).to_vec())
}
pub fn key(binding: &MarketBinding) -> Result<wire::MarketKey> {
    let up = binding.outcomes.iter().find(|x| x.outcome == "Up").context("Up binding")?;
    let down = binding.outcomes.iter().find(|x| x.outcome == "Down").context("Down binding")?;
    Ok(wire::MarketKey { market_id:binding.market_id.clone(), condition_id:binding.condition_id.clone(),
        window_id:binding.window_id.clone(), start_at:binding.start_at.clone(), end_at:binding.end_at.clone(),
        up_token_id:up.token_id.clone(), down_token_id:down.token_id.clone(),
        rule_sha256:crate::source_btc5m_proto::hash32(&binding.rule_sha256)? })
}
fn key_json(key: &wire::MarketKey) -> Value {
    json!({"market_id":key.market_id,"condition_id":key.condition_id,"window_id":key.window_id,
        "start_at":key.start_at,"end_at":key.end_at,"up_token_id":key.up_token_id,
        "down_token_id":key.down_token_id,"rule_sha256":hex::encode(&key.rule_sha256)})
}
fn levels(book: &Value, side: &str) -> Result<Vec<wire::PriceLevel>> {
    let rows = book[side].as_array().context("live depth")?;
    ensure!(rows.len() <= MAX_LEVELS, "live level budget");
    let mut previous = None;
    rows.iter().map(|row| {
        let price = decimal(row["price"].as_str().context("depth price")?)?;
        let size = decimal(row["size"].as_str().context("depth size")?)?;
        let p: Decimal = price.parse()?;
        ensure!(size != "0", "zero size in snapshot");
        if let Some(before) = previous {
            ensure!(if side == "bids" { before > p } else { before < p }, "depth order/duplicate");
        }
        previous = Some(p);
        Ok(wire::PriceLevel {price,size})
    }).collect()
}
fn levels_json(levels: &[wire::PriceLevel]) -> Value {
    Value::Array(levels.iter().map(|l|json!({"price":l.price,"size":l.size})).collect())
}
fn metadata_json(m: &wire::TokenMetadata) -> Value {
    json!({"token_id":m.token_id,"outcome":m.outcome,"tick_size":m.tick_size,
        "quote_source_at":m.quote_source_at,"last_trade":m.last_trade.as_ref().map(|t|
            json!({"price":t.price,"source_at":t.source_at})),
        "confirmation":m.confirmation.as_ref().map(|f|json!({"source_at":f.source_at,
            "received_at":f.received_at,"source":f.source,"evidence_sha256":hex::encode(&f.evidence_sha256),
            "bound_depth_sha256":hex::encode(&f.bound_depth_sha256),"bound_quote_source_at":f.bound_quote_source_at})),
        "data_valid":m.data_valid,"invalid_reason":m.invalid_reason,"received_at":m.received_at})
}
fn book_json(b: &wire::TokenBook) -> Value {
    json!({"metadata":b.metadata.as_ref().map(metadata_json),"bids":levels_json(&b.bids),"asks":levels_json(&b.asks)})
}
fn token(snapshot: Option<&MarketMemorySnapshot>, token: &str, outcome: &str) -> Result<wire::TokenBook> {
    let book = snapshot.and_then(|s|s.books.get(token));
    let mut metadata = wire::TokenMetadata {token_id:token.into(),outcome:outcome.into(),
        invalid_reason:Some("waiting_baseline".into()), ..Default::default()};
    let Some(book) = book else {return Ok(wire::TokenBook {metadata:Some(metadata),..Default::default()});};
    let snapshot = snapshot.context("book without snapshot")?;
    metadata.tick_size = Some(decimal(book["tick_size"].as_str().context("tick size")?)?);
    metadata.quote_source_at = book["exchange_at"].as_str().map(str::to_owned);
    if let Some(at) = &metadata.quote_source_at {
        chrono::DateTime::parse_from_rfc3339(at).context("supplier quote time")?;
    }
    metadata.received_at = book["received_at"].as_str().map(str::to_owned);
    metadata.last_trade = book["last_trade_price"].as_str().map(|p|
        decimal(p).map(|price|wire::Trade {price,source_at:snapshot.trade_source_times.get(token).cloned()})).transpose()?;
    let recovery_resolved = snapshot.recoveries.get(token).is_none_or(|r|
        r["snapshot_cursor"].as_u64().is_some_and(|c|snapshot.event_book_cursors.get(token)==Some(&c)));
    metadata.data_valid = !snapshot.capacity_fenced && !snapshot.unresolved_seed_gaps.contains(token)
        && recovery_resolved && metadata.quote_source_at.is_some();
    metadata.invalid_reason = (!metadata.data_valid).then(||"baseline_unavailable".into());
    let bids = levels(book,"bids")?;
    let asks = levels(book,"asks")?;
    if metadata.data_valid {
        if let Some(f) = snapshot.confirmation_facts.get(token) {
            if f["book_epoch"] == book["book_epoch"] && f["sequence"] == book["sequence"]
                && f["state_checksum"] == book["state_checksum"] && f["book_quote_source_at"] == book["exchange_at"] {
                let depth = Sha256::digest(crate::source_market_preimage::depth(
                    token,&bids,&asks,metadata.tick_size.as_deref())?).to_vec();
                metadata.confirmation = Some(wire::Confirmation {
                    source_at:f["confirmation_source_at"].as_str().context("confirmation source time")?.into(),
                    received_at:f["confirmation_received_at"].as_str().context("confirmation receipt")?.into(),
                    source:f["confirmation_source"].as_str().context("confirmation source")?.into(),
                    evidence_sha256:crate::source_btc5m_proto::hash32(f["confirmation_evidence_sha256"].as_str().context("confirmation SHA")?)?,
                    bound_depth_sha256:depth,bound_quote_source_at:metadata.quote_source_at.clone().context("quote time")?,
                });
                let fact = metadata.confirmation.as_ref().context("confirmation fact")?;
                let source_at = chrono::DateTime::parse_from_rfc3339(&fact.source_at)?;
                let quote_at = chrono::DateTime::parse_from_rfc3339(&fact.bound_quote_source_at)?;
                let received_at = chrono::DateTime::parse_from_rfc3339(&fact.received_at)?;
                ensure!(quote_at<=source_at && source_at<=received_at,"confirmation source clock binding");
            }
        }
    }
    Ok(wire::TokenBook {metadata:Some(metadata),bids,asks})
}

#[derive(Clone)]
pub struct LiveState {
    pub key: wire::MarketKey,
    pub stream_id: String,
    pub sequence: u64,
    pub snapshot: wire::Snapshot,
    pub state_sha256: Vec<u8>,
    pub upstream_incarnation: String,
}
pub struct Lane {
    pub key: wire::MarketKey,
    pub stream_id: String,
    state: Option<Arc<LiveState>>,
    last_input: Option<CaptureIdentity>,
}
#[derive(PartialEq)]
struct CaptureIdentity {
    epoch: String,
    cursor: u64,
    confirmations: u64,
    capacity_fenced: bool,
    ended: bool,
    recoveries: BTreeMap<String,Value>,
    unresolved_seed_gaps: std::collections::BTreeSet<String>,
}
fn capture_identity(snapshot: Option<&MarketMemorySnapshot>,ended:bool) -> Option<CaptureIdentity> {
    snapshot.map(|s|CaptureIdentity {epoch:s.watermark.stream_epoch.clone(),cursor:s.watermark.published_cursor,
        confirmations:s.confirmation_sequence,capacity_fenced:s.capacity_fenced,ended,
        recoveries:s.recoveries.clone(),unresolved_seed_gaps:s.unresolved_seed_gaps.clone()})
}
impl Lane {
    pub fn reject(&mut self,snapshot:Option<&MarketMemorySnapshot>,ended:bool,reason:&str) -> Result<Arc<LiveState>> {
        let state=self.invalidate(reason)?;
        // A rejected oversized/malformed post-state is not repeatedly rebuilt
        // by every downstream client. Retry only on a new producer revision.
        self.last_input=capture_identity(snapshot,ended);
        Ok(state)
    }
    pub fn invalidate(&mut self, reason: &str) -> Result<Arc<LiveState>> {
        if let Some(state) = &self.state {
            if state.snapshot.source_status=="unavailable" && state.snapshot.reason.as_deref()==Some(reason) {
                return Ok(state.clone());
            }
        }
        let mut state = (*self.capture(None,false)?).clone();
        state.sequence = state.sequence.checked_add(1).context("public sequence exhausted")?;
        state.snapshot.source_status = "unavailable".into();
        state.snapshot.reason = Some(reason.into());
        for book in &mut state.snapshot.books {
            if let Some(metadata) = &mut book.metadata {
                metadata.data_valid=false;
                metadata.invalid_reason=Some(reason.into());
            }
        }
        state.state_sha256=Sha256::digest(crate::source_market_preimage::state(
            &state.key,&state.stream_id,state.sequence,&state.snapshot)?).to_vec();
        let state=Arc::new(state);
        self.state=Some(state.clone());
        Ok(state)
    }
    pub fn capture(&mut self, snapshot: Option<&MarketMemorySnapshot>, ended: bool) -> Result<Arc<LiveState>> {
        let input = capture_identity(snapshot,ended);
        if input.is_some() && self.last_input==input {
            if let Some(state)=&self.state {return Ok(state.clone());}
        }
        let books = vec![token(snapshot,&self.key.up_token_id,"Up")?,token(snapshot,&self.key.down_token_id,"Down")?];
        let had_baseline = self.state.is_some();
        let status = if ended {"ended"} else if books.iter().all(|b|b.metadata.as_ref().is_some_and(|m|m.data_valid)) {
            "available"
        } else if snapshot.is_none() && !had_baseline {"waiting_baseline"} else {"unavailable"};
        let reason = (status != "available").then(||status.to_string());
        if let Some(current) = &self.state {if current.snapshot.books == books
            && current.snapshot.source_status == status && current.snapshot.reason == reason {
            self.last_input=input;
            return Ok(current.clone());
        }}
        let sequence = self.state.as_ref().map_or(Ok(1),|s|s.sequence.checked_add(1).context("public state sequence exhausted"))?;
        let public_snapshot = wire::Snapshot {books,source_status:status.into(),reason};
        let canonical = crate::source_market_preimage::state(&self.key,&self.stream_id,sequence,&public_snapshot)?;
        ensure!(canonical.len() <= MAX_STATE_BYTES,"live state bytes");
        let state = Arc::new(LiveState {key:self.key.clone(),stream_id:self.stream_id.clone(),sequence,
            snapshot:public_snapshot,state_sha256:Sha256::digest(&canonical).to_vec(),
            upstream_incarnation:snapshot.map(|s| format!("{}:{}:{}",s.watermark.stream_epoch,
                s.books.get(&self.key.up_token_id).map(|b|b["book_epoch"].to_string()).unwrap_or_default(),
                s.books.get(&self.key.down_token_id).map(|b|b["book_epoch"].to_string()).unwrap_or_default())).unwrap_or_default()});
        self.state = Some(state.clone());
        self.last_input=input;
        Ok(state)
    }
}
#[derive(Clone, Default)]
pub struct Registry(Arc<Mutex<BTreeMap<String,RegistryEntry>>>);
#[derive(Clone)]
struct RegistryEntry {
    key: wire::MarketKey,
    stream_id: String,
    lane: Arc<Mutex<Lane>>,
    latest: Arc<ArcSwapOption<LiveState>>,
    changed: tokio::sync::watch::Sender<u64>,
}
impl Registry {
    pub fn invalidate_market(&self,binding:&MarketBinding,reason:&str)->Result<()> {
        let entry={let directory=self.0.lock().map_err(|_|anyhow::anyhow!("live directory poisoned"))?;
            let entry=directory.get(&binding.market_id).context("public market retired")?;
            ensure!(entry.key==key(binding)?,"immutable public market changed");entry.clone()};
        let state=entry.lane.lock().map_err(|_|anyhow::anyhow!("live lane poisoned"))?.invalidate(reason)?;
        self.publish(binding,state)
    }
    pub fn subscribe(&self,binding:&MarketBinding)->Result<tokio::sync::watch::Receiver<u64>> {
        let directory=self.0.lock().map_err(|_|anyhow::anyhow!("live directory poisoned"))?;
        let entry=directory.get(&binding.market_id).context("public market preparing")?;
        ensure!(entry.key==key(binding)?,"immutable public market changed");
        Ok(entry.changed.subscribe())
    }
    pub fn invalidate_all(&self, reason:&str) {
        let entries = match self.0.lock() {
            Ok(directory)=>directory.values().cloned().collect::<Vec<_>>(),Err(_)=>return,
        };
        for entry in entries {
            if let Ok(mut lane)=entry.lane.lock() {
                if let Ok(state)=lane.invalidate(reason) {
                    entry.latest.store(Some(state));
                    entry.changed.send_modify(|n|*n=n.wrapping_add(1));
                }
            }
        }
    }
    pub fn load(&self, binding:&MarketBinding) -> Result<Arc<LiveState>> {
        let expected = key(binding)?;
        let latest = {
            let directory = self.0.lock().map_err(|_|anyhow::anyhow!("live directory poisoned"))?;
            let entry = directory.get(&binding.market_id).context("public market preparing")?;
            ensure!(entry.key == expected,"immutable public market changed");
            entry.latest.clone()
        };
        latest.load_full().context("public market preparing")
    }
    pub fn publish(&self, binding:&MarketBinding, state:Arc<LiveState>) -> Result<()> {
        let (latest,changed)={
            let directory = self.0.lock().map_err(|_|anyhow::anyhow!("live directory poisoned"))?;
            let entry = directory.get(&binding.market_id).context("public market retired")?;
            ensure!(entry.key == state.key && entry.stream_id == state.stream_id,"stale public publisher");
            (entry.latest.clone(),entry.changed.clone())
        };
        if latest.load_full().is_some_and(|old|old.sequence==state.sequence) {return Ok(())}
        latest.store(Some(state));
        changed.send_modify(|n|*n=n.wrapping_add(1));
        Ok(())
    }
    pub fn lane(&self,binding: &MarketBinding) -> Result<Arc<Mutex<Lane>>> {
        Ok(self.register(binding)?.lane)
    }
    pub fn identity(&self,binding:&MarketBinding) -> Result<(wire::MarketKey,String)> {
        let expected=key(binding)?;
        let directory=self.0.lock().map_err(|_|anyhow::anyhow!("live directory poisoned"))?;
        let entry=directory.get(&binding.market_id).context("public market preparing")?;
        ensure!(entry.key==expected,"immutable public market changed");
        Ok((entry.key.clone(),entry.stream_id.clone()))
    }
    fn register(&self,binding:&MarketBinding) -> Result<RegistryEntry> {
        let key = key(binding)?;
        let mut directory = self.0.lock().map_err(|_|anyhow::anyhow!("live directory poisoned"))?;
        if let Some(entry) = directory.get(&key.market_id) {
            if entry.key == key {
                // Immutable registry metadata avoids waiting for a selected
                // market's encoder while holding the all-market directory.
                return Ok(entry.clone());
            }
            // A genuinely changed immutable market key is a new public
            // incarnation. Existing sockets retain the old Arc and cannot
            // accidentally read books through the new binding.
            directory.remove(&key.market_id);
        }
        ensure!(directory.len() < 3,"live market directory budget");
        let stream_id=uuid::Uuid::new_v4().simple().to_string();
        let lane = Arc::new(Mutex::new(Lane {key:key.clone(),stream_id:stream_id.clone(),state:None,last_input:None}));
        let entry=RegistryEntry {key,stream_id,lane,latest:Arc::new(ArcSwapOption::empty()),
            changed:tokio::sync::watch::channel(0).0};
        directory.insert(entry.key.market_id.clone(),entry.clone());
        Ok(entry)
    }
    pub fn retain(&self, bindings: &[MarketBinding]) -> Result<()> {
        let keys = bindings.iter().map(key).collect::<Result<Vec<_>>>()?;
        let mut directory = self.0.lock().map_err(|_|anyhow::anyhow!("live directory poisoned"))?;
        directory.retain(|_,entry| keys.iter().any(|k|k == &entry.key));
        Ok(())
    }
}
pub fn frame(state: &LiveState, previous: Option<&LiveState>) -> Result<wire::MarketFrame> {
    // Ordinary snapshot after private connection replacement. The consumer
    // never sees or checks this private incarnation.
    let previous = previous.filter(|p|p.upstream_incarnation == state.upstream_incarnation
        && p.sequence.checked_add(1) == Some(state.sequence)
        && p.snapshot.source_status == "available" && state.snapshot.source_status == "available");
    let mut changes = Vec::new();
    if let Some(before) = previous {
        ensure!(before.key == state.key && before.stream_id == state.stream_id,"live delta identity");
        for (old,new) in before.snapshot.books.iter().zip(&state.snapshot.books) {
            let token = new.metadata.as_ref().context("book metadata")?.token_id.clone();
            for (side,old,new) in [("BUY",&old.bids,&new.bids),("SELL",&old.asks,&new.asks)] {
                let old = old.iter().map(|l|(l.price.as_str(),l.size.as_str())).collect::<BTreeMap<_,_>>();
                let new = new.iter().map(|l|(l.price.as_str(),l.size.as_str())).collect::<BTreeMap<_,_>>();
                for (price,size) in &new {if old.get(price) != Some(size) {
                    changes.push(wire::DepthChange {token_id:token.clone(),side:side.into(),price:(*price).into(),size:(*size).into()});
                }}
                for price in old.keys() {if !new.contains_key(price) {
                    changes.push(wire::DepthChange {token_id:token.clone(),side:side.into(),price:(*price).into(),size:"0".into()});
                }}
            }
        }
    }
    let body = if let Some(before) = previous.filter(|_|changes.len() <= MAX_CHANGES) {
        wire::market_frame::Body::PriceChange(wire::PriceChange {base_sequence:before.sequence,changes,
            post_metadata:state.snapshot.books.iter().filter_map(|b|b.metadata.clone()).collect(),
            source_status:state.snapshot.source_status.clone(),reason:state.snapshot.reason.clone()})
    } else {wire::market_frame::Body::Snapshot(state.snapshot.clone())};
    Ok(wire::MarketFrame {schema_version:contract::SCHEMA_VERSION.into(),key:Some(state.key.clone()),
        stream_id:state.stream_id.clone(),state_sequence:state.sequence,state_sha256:state.state_sha256.clone(),
        published_at:chrono::Utc::now().to_rfc3339(),body:Some(body)})
}

/// Offline deterministic materials emitted by the actual producer executable.
/// Intentionally not invoked at service startup or in the live data path.
pub fn serializer_vectors() -> Result<Value> {
    let key = wire::MarketKey {market_id:"600001".into(),condition_id:format!("0x{}", "33".repeat(32)),
        window_id:"btc-updown-5m-1791590400".into(),start_at:"2026-10-10T00:00:00Z".into(),
        end_at:"2026-10-10T00:05:00Z".into(),up_token_id:"10001".into(),
        down_token_id:"10002".into(),rule_sha256:vec![0x11;32]};
    let metadata = |token:&str,outcome:&str|wire::TokenMetadata {token_id:token.into(),outcome:outcome.into(),
        tick_size:Some("0.01".into()),quote_source_at:Some("2026-10-10T00:00:00.100Z".into()),
        received_at:Some("2026-10-10T00:00:00.150Z".into()),data_valid:true,..Default::default()};
    let level = |price:&str,size:&str|wire::PriceLevel {price:price.into(),size:size.into()};
    let mut snapshot = wire::Snapshot {books:vec![
        wire::TokenBook {metadata:Some(metadata("10001","Up")),
            bids:vec![level("0.4","12"),level("0.3","6")],asks:vec![level("0.6","10")]},
        // Fresh one-sided depth remains available as data.
        wire::TokenBook {metadata:Some(metadata("10002","Down")),
            bids:vec![],asks:vec![level("0.7","8")]}],source_status:"available".into(),reason:None};
    let make = |sequence,snapshot:wire::Snapshot,incarnation:&str| -> Result<LiveState> {
        let facts = json!({"books":snapshot.books.iter().map(book_json).collect::<Vec<_>>(),
            "source_status":snapshot.source_status,"reason":snapshot.reason});
        let preimage = json!({"schema_version":contract::SCHEMA_VERSION,"key":key_json(&key),
            "stream_id":"public-stream-600001","state_sequence":sequence,
            "books":facts["books"],"source_status":facts["source_status"],"reason":facts["reason"]});
        Ok(LiveState {key:key.clone(),stream_id:"public-stream-600001".into(),sequence,
            snapshot,state_sha256:hash(&preimage)?,upstream_incarnation:incarnation.into()})
    };
    let first = make(1,snapshot.clone(),"private-A")?;
    snapshot.books[0].bids=vec![level("0.4","5"),level("0.2","7")];
    snapshot.books[0].metadata.as_mut().context("fixture metadata")?.quote_source_at=
        Some("2026-10-10T00:00:00.200Z".into());
    let second = make(2,snapshot.clone(),"private-A")?;
    let depth = hash(&json!({"token_id":"10001","bids":levels_json(&snapshot.books[0].bids),
        "asks":levels_json(&snapshot.books[0].asks),"tick_size":"0.01"}))?;
    snapshot.books[0].metadata.as_mut().context("fixture metadata")?.confirmation=Some(wire::Confirmation {
        source_at:"2026-10-10T00:00:00.250Z".into(),received_at:"2026-10-10T00:00:00.300Z".into(),
        source:"polymarket_websocket".into(),evidence_sha256:vec![0x22;32],bound_depth_sha256:depth,
        bound_quote_source_at:"2026-10-10T00:00:00.200Z".into()});
    let confirmed = make(3,snapshot.clone(),"private-A")?;
    // A private switch has no switch marker or public identity change.
    snapshot.books[0].metadata.as_mut().context("fixture metadata")?.confirmation=None;
    let switched = make(9,snapshot.clone(),"private-B")?;
    snapshot.source_status="unavailable".into();snapshot.reason=Some("source_unavailable".into());
    for book in &mut snapshot.books {let m=book.metadata.as_mut().context("fixture metadata")?;
        m.data_valid=false;m.invalid_reason=Some("source_unavailable".into());m.confirmation=None;}
    let unavailable=make(10,snapshot.clone(),"private-B")?;
    snapshot.source_status="available".into();snapshot.reason=None;
    for book in &mut snapshot.books {let m=book.metadata.as_mut().context("fixture metadata")?;
        m.data_valid=true;m.invalid_reason=None;}
    let recovered=make(11,snapshot,"private-C")?;
    let mut frames=Vec::new();
    for (name,state,previous) in [("initial_snapshot",&first,None),
        ("absolute_price_change",&second,Some(&first)),("metadata_only_confirmation",&confirmed,Some(&second)),
        ("ordinary_replacement",&switched,Some(&confirmed)),("unavailable",&unavailable,Some(&switched)),
        ("recovered_snapshot",&recovered,Some(&unavailable))] {
        let mut message=frame(state,previous)?;
        message.published_at="2026-10-10T00:00:01Z".into();
        let bytes=contract::encode(&message,MAX_FRAME_BYTES)?;
        let preimage=json!({"schema_version":contract::SCHEMA_VERSION,"key":key_json(&state.key),
            "stream_id":state.stream_id,"state_sequence":state.sequence,
            "books":state.snapshot.books.iter().map(book_json).collect::<Vec<_>>(),
            "source_status":state.snapshot.source_status,"reason":state.snapshot.reason});
        frames.push(json!({"name":name,"protobuf_hex":hex::encode(&bytes),"protobuf_sha256":hex::encode(Sha256::digest(&bytes)),
            "state_preimage":preimage,"state_sha256":hex::encode(&state.state_sha256)}));
    }
    Ok(json!({"schema_version":contract::SCHEMA_VERSION,"subprotocol":contract::SUBPROTOCOL,
        "evidence_kind":"deterministic_synthetic_producer_serializer_not_live_observation","frames":frames}))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn state(sequence:u64,bids:&[(&str,&str)],incarnation:&str) -> LiveState {
        let metadata = |token:&str,outcome:&str|wire::TokenMetadata {token_id:token.into(),outcome:outcome.into(),
            tick_size:Some("0.01".into()),quote_source_at:Some("2026-10-10T00:00:00Z".into()),data_valid:true,
            ..Default::default()};
        LiveState {key:wire::MarketKey {market_id:"1".into(),condition_id:"condition".into(),
            up_token_id:"10".into(),down_token_id:"20".into(),..Default::default()},
            stream_id:"public".into(),sequence,state_sha256:vec![0;32],
            upstream_incarnation:incarnation.into(),snapshot:wire::Snapshot {source_status:"available".into(),reason:None,
                books:vec![wire::TokenBook {metadata:Some(metadata("10","Up")),
                    bids:bids.iter().map(|(p,s)|wire::PriceLevel {price:(*p).into(),size:(*s).into()}).collect(),asks:vec![]},
                    wire::TokenBook {metadata:Some(metadata("20","Down")),bids:vec![],asks:vec![]}]} }
    }
    #[test]
    fn ordinary_price_sets_absolute_size_and_removes_absent_level() {
        let before=state(1,&[("0.4","20"),("0.3","30")],"A");
        let after=state(2,&[("0.4","15"),("0.2","25")],"A");
        let update=frame(&after,Some(&before)).unwrap();
        let Some(wire::market_frame::Body::PriceChange(delta))=update.body else {panic!("ordinary delta");};
        assert_eq!(delta.base_sequence,1);
        assert!(delta.changes.iter().any(|c|c.price=="0.4" && c.size=="15"));
        assert!(delta.changes.iter().any(|c|c.price=="0.3" && c.size=="0"));
        assert_eq!(delta.post_metadata.len(),2);
    }
    #[test]
    fn skipped_versions_send_an_ordinary_snapshot() {
        let before=state(1,&[("0.4","20")],"A");
        let after=state(4,&[("0.4","10")],"A");
        assert!(matches!(frame(&after,Some(&before)).unwrap().body,
            Some(wire::market_frame::Body::Snapshot(_))));
    }
    #[test]
    fn typed_commitment_serializer_matches_original_contract() {
        let snapshot=state(1,&[("0.4","20")],"A");
        let expected=json!({"schema_version":contract::SCHEMA_VERSION,"key":key_json(&snapshot.key),
            "stream_id":snapshot.stream_id,"state_sequence":snapshot.sequence,
            "books":snapshot.snapshot.books.iter().map(book_json).collect::<Vec<_>>(),
            "source_status":snapshot.snapshot.source_status,"reason":snapshot.snapshot.reason});
        assert_eq!(crate::source_market_preimage::state(&snapshot.key,&snapshot.stream_id,
            snapshot.sequence,&snapshot.snapshot).unwrap(),
            crate::source_btc5m_proto::canonical_json_bytes(&expected).unwrap());
    }
    #[test]
    fn private_switch_is_only_an_ordinary_snapshot_with_same_public_identity() {
        let before=state(1,&[("0.4","20")],"A");
        let after=state(8,&[("0.4","15")],"B");
        let update=frame(&after,Some(&before)).unwrap();
        assert_eq!(update.stream_id,"public");
        assert_eq!(update.state_sequence,8);
        assert!(matches!(update.body,Some(wire::market_frame::Body::Snapshot(_))));
    }
    #[test]
    fn metadata_only_delta_explicitly_clears_old_facts() {
        let before=state(1,&[("0.4","20")],"A");
        let after=state(2,&[("0.4","20")],"A");
        let Some(wire::market_frame::Body::PriceChange(delta))=frame(&after,Some(&before)).unwrap().body else {panic!("delta");};
        assert!(delta.changes.is_empty());
        assert!(delta.post_metadata.iter().all(|m|m.last_trade.is_none() && m.confirmation.is_none()));
    }
    fn binding(id:&str) -> MarketBinding {
        use crate::source_btc5m_api::{OutcomeBinding,WindowRole};
        MarketBinding {role:WindowRole::Next,market_id:id.into(),condition_id:format!("condition-{id}"),
            window_id:"window".into(),start_at:"2026-10-10T00:00:00Z".into(),end_at:"2026-10-10T00:05:00Z".into(),
            outcomes:[OutcomeBinding {outcome:"Up".into(),token_id:format!("{id}1")},
                OutcomeBinding {outcome:"Down".into(),token_id:format!("{id}2")}],rule_sha256:"11".repeat(32)}
    }
    #[test]
    fn public_instance_survives_role_changes_and_directory_does_not_wait_for_other_lane() {
        let registry=Registry::default();
        let mut a=binding("1");
        let lane=registry.lane(&a).unwrap();
        let held=lane.lock().unwrap();
        a.role=crate::source_btc5m_api::WindowRole::Current;
        assert!(Arc::ptr_eq(&lane,&registry.lane(&a).unwrap()));
        assert_eq!(registry.identity(&a).unwrap().1,held.stream_id);
        let b=binding("2");
        assert!(registry.lane(&b).is_ok());
        registry.retain(&[a.clone(),b]).unwrap();
        drop(held);
        assert!(Arc::ptr_eq(&lane,&registry.lane(&a).unwrap()));
        a.rule_sha256="22".repeat(32);
        let replacement=registry.lane(&a).unwrap();
        assert!(!Arc::ptr_eq(&lane,&replacement));
        assert_ne!(lane.lock().unwrap().stream_id,replacement.lock().unwrap().stream_id);
    }
    #[test]
    fn unavailable_state_is_stable_and_does_not_keep_stale_confirmation() {
        let registry=Registry::default();
        let lane=registry.lane(&binding("1")).unwrap();
        let mut lane=lane.lock().unwrap();
        let first=lane.invalidate("source_unavailable").unwrap();
        let second=lane.invalidate("source_unavailable").unwrap();
        assert_eq!(first.sequence,second.sequence);
        assert!(second.snapshot.books.iter().all(|b|b.metadata.as_ref().is_some_and(|m|
            !m.data_valid && m.confirmation.is_none())));
    }
}
