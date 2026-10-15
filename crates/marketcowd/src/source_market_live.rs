//! Public market state, deliberately independent of upstream/profile epochs.
//! Captures existing immutable reducer books; never replays supplier frames.
use std::{collections::BTreeMap, sync::{Arc, Mutex}};
use anyhow::{Context, Result, ensure};
use marketcow_contracts::typed_v3::market as wire;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use crate::{source_btc5m_api::MarketBinding, source_publication::MarketMemorySnapshot};

pub const MAX_LEVELS: usize = 10_000;

fn decimal(value: &str) -> Result<String> {
    let value: Decimal = value.parse().context("live decimal")?;
    ensure!(value >= Decimal::ZERO, "negative live decimal");
    Ok(value.normalize().to_string())
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
fn native_token(snapshot:Option<&MarketMemorySnapshot>,token:&str,outcome:&str)->Result<marketcow_contracts::typed_v3::market::TokenBook> {
    use marketcow_contracts::typed_v3::market as native;
    let mut metadata=native::TokenMetadata{token_id:token.into(),outcome:outcome.into(),invalid_reason:Some("waiting_baseline".into()),..Default::default()};
    let Some((snapshot,book))=snapshot.and_then(|s|s.books.get(token).map(|b|(s,b))) else {
        return Ok(native::TokenBook{metadata:Some(metadata),bids:Vec::new(),asks:Vec::new()});
    };
    metadata.tick_size=Some(decimal(book["tick_size"].as_str().context("tick size")?)?);
    metadata.quote_source_at=book["exchange_at"].as_str().map(str::to_owned);
    metadata.received_at=book["received_at"].as_str().map(str::to_owned);
    metadata.last_trade=book["last_trade_price"].as_str().map(|p|decimal(p).map(|price|native::Trade{price,source_at:snapshot.trade_source_times.get(token).cloned()})).transpose()?;
    let recovery_resolved=snapshot.recoveries.get(token).is_none_or(|r|r["snapshot_cursor"].as_u64().is_some_and(|c|snapshot.event_book_cursors.get(token)==Some(&c)));
    metadata.data_valid=!snapshot.capacity_fenced && !snapshot.unresolved_seed_gaps.contains(token)&&recovery_resolved&&metadata.quote_source_at.is_some();
    metadata.invalid_reason=(!metadata.data_valid).then(||"baseline_unavailable".into());
    if metadata.data_valid {if let Some(f)=snapshot.confirmation_facts.get(token) {
        if f["book_epoch"]==book["book_epoch"] && f["sequence"]==book["sequence"] && f["book_quote_source_at"]==book["exchange_at"] {
            metadata.confirmation=Some(native::Confirmation{source_at:f["confirmation_source_at"].as_str().context("confirmation source")?.into(),
                received_at:f["confirmation_received_at"].as_str().context("confirmation receipt")?.into(),source:f["confirmation_source"].as_str().context("confirmation supplier")?.into(),
                bound_quote_source_at:metadata.quote_source_at.clone().context("quote source")?,bound_book_revision:0,evidence_id:None});
        }
    }}
    Ok(native::TokenBook{metadata:Some(metadata),bids:levels(book,"bids")?,asks:levels(book,"asks")?})
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
#[derive(Clone, Default)]
pub struct Registry(Arc<Mutex<BTreeMap<String,RegistryEntry>>>);
#[derive(Clone)]
struct RegistryEntry {
    key: wire::MarketKey,
    native:Arc<Mutex<Option<NativeLane>>>,
}
struct NativeLane {
    lane:crate::source_market_live_v7::Lane,
    last_input:Option<CaptureIdentity>,
    state:Option<Arc<crate::source_market_live_v7::State>>,
}
impl Registry {
    fn native_lane(&self,_binding:&MarketBinding,key:marketcow_contracts::typed_v3::market::MarketKey)->Result<Arc<Mutex<Option<NativeLane>>>> {
        let entry=self.register(key.clone())?;
        {
            let mut cached=entry.native.lock().map_err(|_|anyhow::anyhow!("native lane poisoned"))?;
            if cached.as_ref().is_none_or(|c|c.lane.key!=key) {
                *cached=Some(NativeLane{lane:crate::source_market_live_v7::Lane::new(key,uuid::Uuid::new_v4().to_string())?,last_input:None,state:None});
            }
        }
        Ok(entry.native)
    }
    pub fn native_identity(&self,binding:&MarketBinding,key:marketcow_contracts::typed_v3::market::MarketKey)->Result<(marketcow_contracts::typed_v3::market::MarketKey,String)> {
        let lane=self.native_lane(binding,key)?;
        let lane=lane.lock().map_err(|_|anyhow::anyhow!("native lane poisoned"))?;
        let lane=lane.as_ref().context("native lane missing")?;
        Ok((lane.lane.key.clone(),lane.lane.stream_id.clone()))
    }
    pub fn capture_native(&self,binding:&MarketBinding,key:marketcow_contracts::typed_v3::market::MarketKey,snapshot:Option<&MarketMemorySnapshot>,ended:bool)->Result<Arc<crate::source_market_live_v7::State>> {
        use marketcow_contracts::typed_v3::market as native;
        let cached=self.native_lane(binding,key)?;
        let mut cached=cached.lock().map_err(|_|anyhow::anyhow!("native lane poisoned"))?;
        let cached=cached.as_mut().context("native lane missing")?;
        let input=capture_identity(snapshot,ended);
        if cached.last_input==input {if let Some(state)=&cached.state{return Ok(state.clone());}}
        let key=&cached.lane.key;
        let make=||->Result<native::Snapshot>{
            let books=vec![native_token(snapshot,&key.up_token_id,"Up")?,native_token(snapshot,&key.down_token_id,"Down")?];
            let status=if ended{"ended"}else if books.iter().all(|b|b.metadata.as_ref().is_some_and(|m|m.data_valid)){"available"}else if snapshot.is_none()&&cached.state.is_none(){"waiting_baseline"}else{"unavailable"};
            Ok(native::Snapshot{books,source_status:status.into(),reason:(status!="available").then(||status.to_owned())})
        };
        let post=make();
        let scalar=|value:Option<&Value>|value.and_then(|v|v.as_str().map(str::to_owned).or_else(||v.as_u64().map(|n|n.to_string()))).unwrap_or_default();
        let generation=snapshot.map(|s|format!("{}:{}:{}",s.watermark.stream_epoch,
            scalar(s.books.get(&key.up_token_id).and_then(|b|b.get("book_epoch"))),scalar(s.books.get(&key.down_token_id).and_then(|b|b.get("book_epoch"))))).unwrap_or_default();
        let captured=post.and_then(|post|cached.lane.capture(post,generation.clone()));
        let state=match captured {
            Ok(state)=>Arc::new(state),
            Err(error)=>{
                // A rejected private source state is ordinary unavailable data,
                // not a reason to tear down a healthy downstream transport.
                let reason=if error.to_string().contains("capacity"){"source_state_capacity"}else{"source_state_invalid"};
                let books=vec![native_token(None,&cached.lane.key.up_token_id,"Up")?,native_token(None,&cached.lane.key.down_token_id,"Down")?];
                let state=cached.lane.capture(native::Snapshot{books,source_status:"unavailable".into(),reason:Some(reason.into())},generation)?;
                marketcow_polymarket::diagnostic::emit(json!({"stage":"native_market_state_rejected","market_id":binding.market_id,"reason":reason}).to_string());
                Arc::new(state)
            }
        };
        cached.last_input=input;cached.state=Some(state.clone());Ok(state)
    }
    fn register(&self,key:wire::MarketKey) -> Result<RegistryEntry> {
        let mut directory=self.0.lock().map_err(|_|anyhow::anyhow!("live directory poisoned"))?;
        if let Some(entry)=directory.get(&key.market_id) {
            if entry.key==key{return Ok(entry.clone());}
            directory.remove(&key.market_id);
        }
        ensure!(directory.len()<3,"live market directory budget");
        let entry=RegistryEntry{key,native:Arc::new(Mutex::new(None))};
        directory.insert(entry.key.market_id.clone(),entry.clone());
        Ok(entry)
    }
    pub fn retain(&self,bindings:&[MarketBinding])->Result<()> {
        let mut directory=self.0.lock().map_err(|_|anyhow::anyhow!("live directory poisoned"))?;
        directory.retain(|_,entry|bindings.iter().any(|b|{
            let k=&entry.key;
            b.market_id==k.market_id && b.condition_id==k.condition_id && b.window_id==k.window_id
                && b.start_at==k.start_at && b.end_at==k.end_at
                && b.outcomes.iter().any(|o|o.outcome=="Up" && o.token_id==k.up_token_id)
                && b.outcomes.iter().any(|o|o.outcome=="Down" && o.token_id==k.down_token_id)
        }));
        Ok(())
    }
}
