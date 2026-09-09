//! Discovery HTTP/WS directly over the collector's immutable memory reader.
use anyhow::{Context,Result,ensure};
use axum::{Router,extract::{State,Query,WebSocketUpgrade,ws::{Message,WebSocket}},routing::get,
    response::{Response,IntoResponse},http::{StatusCode,header},body::Bytes};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value,json};
use std::{sync::{Arc,Mutex},collections::VecDeque,time::Duration};
use tokio::sync::{Semaphore,OwnedSemaphorePermit};
use crate::{source_publication::{MemoryReader,MemoryView},source_public_api::encode_bounded,
    source_discovery_projection::{DiscoveryConfig,DiscoveryConsumer}};

#[derive(Clone)]
pub struct DiscoveryLimits {
    pub full_sync_bytes:usize,
    pub frame_bytes:usize,
    pub state_bytes:usize,
    pub replay_bytes:usize,
    pub clients:usize,
    pub cached_baselines:usize,
    pub send_timeout:Duration,
}
struct Api {
    reader:MemoryReader,config:Arc<DiscoveryConfig>,limits:DiscoveryLimits,
    baselines:Mutex<VecDeque<Arc<MemoryView>>>,snapshots:Arc<Semaphore>,clients:Arc<Semaphore>,
}
type ScopeState=Arc<crate::source_scope_registry::ScopeRegistry<Arc<Api>>>;
pub struct DiscoveryScopeControl {state:ScopeState}
impl DiscoveryScopeControl {
    pub fn referenced_markets(&self)->Result<std::collections::BTreeSet<String>> {
        let mut ids=std::collections::BTreeSet::new();
        for lease in self.state.readable(std::time::Instant::now())? {
            ids.extend(lease.value.config.market_ids.iter().cloned());
            for relation in &lease.value.config.relations {
                for member in relation["member_market_ids"].as_array().context("leased discovery relation")? {
                    ids.insert(member.as_str().context("leased dependency market")?.into());
                }
            }
        }
        Ok(ids)
    }
    pub fn status(&self)->Result<Value> {
        let active=self.state.active()?;
        let view=active.value.reader.capture().ok();
        let retirements=active.value.reader.retirement_status().ok();
        Ok(json!({"pool":"discovery","projection_id":active.id,"revision":active.revision,
            "catalog_revision":active.value.config.catalog_revision,"universe_revision":active.value.config.universe_revision,
            "market_count":active.value.config.market_ids.len(),"source_readable":view.is_some(),
            "source_cursor":view.as_ref().map(|v|v.cursor),"persisted_cursor":view.as_ref().map(|v|v.persisted_cursor),
            "retirement_submitted":retirements.map(|r|r.0),"retirement_persisted":retirements.map(|r|r.1),
            "admitted_market_ids":view.as_ref().map(|v|v.markets.keys().cloned().collect::<Vec<_>>()),
            "referenced_market_ids":self.referenced_markets()?,
            "acquisition":active.value.reader.acquisition_statistics()}))
    }
    pub fn prepare(&self,expected_projection:&str,expected_revision:u64,config:&DiscoveryConfig)->Result<()> {
        let old=self.state.active()?;
        ensure!(old.id==expected_projection && old.revision==expected_revision,"scope revision conflict");
        ensure!(config.catalog_revision==old.value.config.catalog_revision,"catalog changed");
        ensure!(config.projection_id!=old.id && config.universe_revision!=old.value.config.universe_revision,"new discovery range identities required");
        self.state.check_activation(expected_projection,expected_revision,&config.projection_id,std::time::Instant::now())?;
        config.full_sync(&old.value.reader.capture()?,Utc::now())?;
        Ok(())
    }
    pub fn activate(&self, expected_projection:&str, expected_revision:u64, config:Arc<DiscoveryConfig>)->Result<u64> {
        self.prepare(expected_projection,expected_revision,&config)?;
        let old=self.state.active()?;
        ensure!(config.catalog_revision==old.value.config.catalog_revision,"catalog changed");
        ensure!(config.projection_id!=old.value.config.projection_id && config.universe_revision!=old.value.config.universe_revision,
            "new discovery range identities required");
        let api=Arc::new(Api{reader:old.value.reader.clone(),config:config.clone(),limits:old.value.limits.clone(),
            baselines:Mutex::new(VecDeque::new()),snapshots:old.value.snapshots.clone(),clients:old.value.clients.clone()});
        Ok(self.state.activate(expected_projection,expected_revision,config.projection_id.clone(),api,std::time::Instant::now())?.revision)
    }
}
struct OwnedBytes {bytes:Vec<u8>,_permit:OwnedSemaphorePermit}
impl AsRef<[u8]> for OwnedBytes {fn as_ref(&self)->&[u8]{&self.bytes}}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Resume {projection_id:String,after_cursor:u64}

pub fn router(reader:MemoryReader,config:Arc<DiscoveryConfig>,limits:DiscoveryLimits)->Result<Router> {
    let grace=limits.send_timeout;
    Ok(router_managed(reader,config,limits,grace)?.0)
}
pub fn router_managed(reader:MemoryReader,config:Arc<DiscoveryConfig>,limits:DiscoveryLimits,grace:Duration)->Result<(Router,DiscoveryScopeControl)> {
    router_managed_at(reader,config,limits,grace,1)
}
pub fn router_managed_at(reader:MemoryReader,config:Arc<DiscoveryConfig>,limits:DiscoveryLimits,grace:Duration,revision:u64)->Result<(Router,DiscoveryScopeControl)> {
    ensure!((1..=4).contains(&limits.clients)&&(1..=4).contains(&limits.cached_baselines),"explicit Discovery client/baseline limits");
    ensure!(limits.full_sync_bytes>0&&limits.full_sync_bytes<=256*1024*1024&&limits.frame_bytes>0&&limits.frame_bytes<=16*1024*1024,
        "Discovery response byte limits");
    ensure!(limits.state_bytes>0&&limits.state_bytes<=256*1024*1024&&limits.replay_bytes>0&&limits.replay_bytes<=64*1024*1024&&!limits.send_timeout.is_zero(),
        "Discovery state/replay/time limits");
    let api=Arc::new(Api{reader,config,clients:Arc::new(Semaphore::new(limits.clients)),limits,
        baselines:Mutex::new(VecDeque::new()),snapshots:Arc::new(Semaphore::new(1))});
    let state=Arc::new(crate::source_scope_registry::ScopeRegistry::new(api.config.projection_id.clone(),revision,api,grace)?);
    let control=DiscoveryScopeControl{state:state.clone()};
    Ok((Router::new()
        .route("/v1/prediction-markets/polymarket/live/discovery/full-sync",get(full_sync))
        .route("/v1/prediction-markets/polymarket/live/discovery/status",get(status))
        .route("/v1/prediction-markets/polymarket/live/discovery/stream",get(upgrade)).with_state(state),control))
}
fn error(status:StatusCode,code:&str,retry:bool)->Response {
    (status,axum::Json(json!({"detail":{"code":code,"message":code,"retryable":retry}}))).into_response()
}
async fn full_sync(State(state):State<ScopeState>)->Response {
    let Ok(lease)=state.active() else{return error(StatusCode::SERVICE_UNAVAILABLE,"discovery_projection_unavailable",true)};
    let api=lease.value.clone();
    let Ok(permit)=api.snapshots.clone().try_acquire_owned() else {return error(StatusCode::SERVICE_UNAVAILABLE,"discovery_full_sync_capacity",true)};
    let result=tokio::task::spawn_blocking(move ||->Result<OwnedBytes>{
        let view=Arc::new(api.reader.capture_for_resume(api.limits.send_timeout,api.limits.cached_baselines)?);
        // Bound retained state, not just outgoing quote JSON. Each baseline
        // and each client may keep one immutable book/metadata version set.
        let size=view.books.values().map(|v|serde_json::to_vec(v).map(|v|v.len())).collect::<std::result::Result<Vec<_>,_>>()?.into_iter().sum::<usize>()
            +view.markets.values().map(|v|serde_json::to_vec(v).map(|v|v.len())).collect::<std::result::Result<Vec<_>,_>>()?.into_iter().sum::<usize>();
        ensure!(size<=api.limits.state_bytes,"Discovery baseline state budget");
        let body=api.config.full_sync(&view,Utc::now())?;
        let bytes=encode_bounded(&body,api.limits.full_sync_bytes)?;
        let mut cache=api.baselines.lock().unwrap();
        cache.retain(|prior|prior.cursor!=view.cursor);
        if cache.len()==api.limits.cached_baselines {cache.pop_front();}
        cache.push_back(view);
        Ok(OwnedBytes{bytes,_permit:permit})
    }).await;
    match result {
        Ok(Ok(bytes))=>([(header::CONTENT_TYPE,"application/json")],Bytes::from_owner(bytes)).into_response(),
        Ok(Err(e)) if e.downcast_ref::<crate::source_public_api::EncodeError>().is_some_and(|e|matches!(e,crate::source_public_api::EncodeError::TooLarge))=>
            error(StatusCode::PAYLOAD_TOO_LARGE,"discovery_full_sync_too_large",false),
        Ok(Err(e))=>{eprintln!("discovery full-sync unavailable: {e:#}");error(StatusCode::SERVICE_UNAVAILABLE,"discovery_projection_unavailable",true)},
        Err(e)=>{eprintln!("discovery snapshot worker failed: {e}");error(StatusCode::SERVICE_UNAVAILABLE,"discovery_projection_unavailable",true)},
    }
}
async fn status(State(state):State<ScopeState>)->Response {
    let Ok(lease)=state.active() else{return error(StatusCode::SERVICE_UNAVAILABLE,"discovery_projection_unavailable",true)};
    let api=lease.value.clone();
    match api.reader.capture() {
        Ok(view)=>{
            let gaps=view.base["gaps"].as_array().map(|g|view.recoveries.len()+g.iter().filter(|g|g["resolved"]==false).count());
            let valid=api.config.validate(&view).is_ok()&&gaps.is_some();let ready=valid&&gaps==Some(0);
            axum::Json(json!({"state":if ready{"ready"}else{"failed"},"ready":ready,
                "fail_closed_reason":if !valid{Some("discovery_projection_unavailable")}else if gaps.is_some_and(|g|g>0){Some("discovery_unresolved_gaps")}else{None},
                "unresolved_gap_count":gaps,"error":null,"snapshot_id":null,
                "projection_id":api.config.projection_id,"universe_revision":api.config.universe_revision,"boundary_cursor":view.cursor})).into_response()
        },
        Err(_)=>axum::Json(json!({"state":"unavailable","ready":false,"fail_closed_reason":"discovery_projection_unavailable",
            "unresolved_gap_count":null,"error":"source unavailable","snapshot_id":null,"projection_id":api.config.projection_id,
            "universe_revision":api.config.universe_revision,"boundary_cursor":null})).into_response(),
    }
}
async fn upgrade(State(state):State<ScopeState>,Query(query):Query<Resume>,ws:WebSocketUpgrade)->Response {
    let Ok(lease)=state.get(&query.projection_id,std::time::Instant::now()) else {
        return error(StatusCode::CONFLICT,"discovery_projection_changed",true);
    };
    let api=lease.value.clone();
    let Ok(permit)=api.clients.clone().try_acquire_owned() else {return error(StatusCode::SERVICE_UNAVAILABLE,"discovery_stream_capacity",true)};
    ws.max_message_size(4096).max_frame_size(4096).on_upgrade(move |mut socket|async move {
        let _permit=permit;
        let mut query=query;
        let result=tokio::select! {
            result=stream(&api,&mut socket,&mut query)=>result,
            _=lease.expired()=>Err(anyhow::anyhow!("discovery scope retired")),
        };
        if let Err(e)=result {
            eprintln!("discovery client requires resync: {e:#}");
            let _=send(&api,&mut socket,resync(&api,&query)).await;
        }
        let _=tokio::time::timeout(api.limits.send_timeout,socket.send(Message::Close(None))).await;
    })
}
fn resync(api:&Api,query:&Resume)->Value {
    let boundary=api.reader.capture().map(|v|v.cursor).unwrap_or(query.after_cursor);
    json!({"schema_version":"marketcow.polymarket.discovery-events.v3","projection_id":query.projection_id,
        "catalog_revision":api.config.catalog_revision,"universe_revision":api.config.universe_revision,
        "after_cursor":query.after_cursor,"next_cursor":query.after_cursor,"boundary_cursor":boundary,
        "has_more":false,"resync_required":true,"items":[]})
}
async fn send(api:&Api,socket:&mut WebSocket,frame:Value)->Result<()> {
    let raw=String::from_utf8(encode_bounded(&frame,api.limits.frame_bytes)?)?;
    tokio::time::timeout(api.limits.send_timeout,socket.send(Message::Text(raw.into()))).await??;Ok(())
}
async fn stream(api:&Api,socket:&mut WebSocket,query:&mut Resume)->Result<()> {
    ensure!(query.projection_id==api.config.projection_id,"projection changed");
    let baseline=api.baselines.lock().unwrap().iter().find(|v|v.cursor==query.after_cursor).cloned().context("full-sync baseline expired")?;
    let mut consumer=DiscoveryConsumer::new(api.config.clone(),baseline.as_ref().clone(),api.limits.frame_bytes,api.limits.state_bytes)?;
    drop(baseline);
    let mut changed=api.reader.subscribe();
    loop {
        changed.borrow_and_update();
        let page=api.reader.replay(consumer.cursor(),u64::MAX,64,api.limits.replay_bytes)?;
        for batch in &page.batches {
            if !crate::source_public_api::poll_replay_control(socket,api.limits.send_timeout).await? {return Ok(());}
            for index in 0..batch.validated.events().len() {
                if let Some(mut frame)=consumer.apply_event(&batch.validated,index)? {
                    frame["boundary_cursor"]=json!(page.boundary_cursor);
                    frame["has_more"]=json!(consumer.cursor()<page.boundary_cursor);
                    send(api,socket,frame).await?;
                    query.after_cursor=consumer.cursor();
                }
            }
            tokio::task::yield_now().await;
        }
        ensure!(consumer.cursor()==page.next,"discovery replay boundary mismatch");
        if !page.caught_up {continue;}
        tokio::select! {
            result=changed.changed()=>{result.context("publication closed")?;},
            incoming=socket.recv()=>match incoming {
                None|Some(Ok(Message::Close(_)))=>return Ok(()),
                Some(Ok(Message::Ping(bytes)))=>{tokio::time::timeout(api.limits.send_timeout,socket.send(Message::Pong(bytes))).await??;},
                Some(Ok(Message::Pong(_)))=>{},_=>anyhow::bail!("unexpected client input"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use tower::ServiceExt;
    use crate::{source_publication::Publication,source_discovery_quote::QuotePolicy};
    use std::collections::BTreeMap;
    fn config()->Arc<DiscoveryConfig> {
        Arc::new(DiscoveryConfig{projection_id:"a".repeat(64),catalog_revision:"b".repeat(64),universe_revision:"c".repeat(64),
            market_ids:vec!["1".into()],relations:vec![],settlements:BTreeMap::from([("1".into(),Value::Null)]),
            policy:QuotePolicy{quantities:vec!["10".into()],maximum_book_age_ms:5000}})
    }
    fn limits(bytes:usize)->DiscoveryLimits {DiscoveryLimits{full_sync_bytes:bytes,frame_bytes:65536,state_bytes:131072,
        replay_bytes:65536,clients:2,cached_baselines:2,send_timeout:Duration::from_secs(1)}}
    fn market()->Value {json!({"identity":{"market_id":"1","condition_id":"condition","event_id":"e","neg_risk":false,
        "outcomes":[{"outcome":"Yes","token_id":"11"},{"outcome":"No","token_id":"12"}]},
        "metadata_revision":"d".repeat(64),"active":true,"closed":false,"accepting_orders":true,"lifecycle_state":"active",
        "start_at":null,"end_at":null,"relations":[],"rules":{"instrument":{"price_increment":"0.01","minimum_order_size":"5"},
        "fee_schedule":{"complete":true,"schedule_id":"fee"}}})}
    #[tokio::test]
    async fn scope_change_keeps_listener_and_uses_native_resync() {
        let mut second=market();
        second["identity"]=json!({"market_id":"2","condition_id":"other","event_id":"e2","neg_risk":false,
            "outcomes":[{"outcome":"Yes","token_id":"21"},{"outcome":"No","token_id":"22"}]});
        let p=Publication::start(0,Some(json!({"latest_cursor":0,"books":[],"markets":[market(),second],
            "gaps":[],"catalog_revision":"b".repeat(64)})),
            BTreeMap::from([("11".into(),"1".into()),("12".into(),"1".into()),
                ("21".into(),"2".into()),("22".into(),"2".into())]),8,131072,65536, |_|Ok(())).unwrap();
        let (app,control)=router_managed(p.reader(),config(),limits(65536),Duration::from_millis(50)).unwrap();
        let request=||axum::http::Request::builder().uri("/v1/prediction-markets/polymarket/live/discovery/full-sync")
            .body(axum::body::Body::empty()).unwrap();
        let response=app.clone().oneshot(request()).await.unwrap();
        assert_eq!(response.status(),StatusCode::OK);
        drop(axum::body::to_bytes(response.into_body(),65536).await.unwrap());
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address=listener.local_addr().unwrap();
        let routed=app.clone();
        let server=tokio::spawn(async move{axum::serve(listener,routed).await.unwrap()});
        let (mut ws,_)=tokio_tungstenite::connect_async(format!("ws://{address}/v1/prediction-markets/polymarket/live/discovery/stream?projection_id={}&after_cursor=0","a".repeat(64))).await.unwrap();
        let next=Arc::new(DiscoveryConfig {projection_id:"e".repeat(64),catalog_revision:"b".repeat(64),
            universe_revision:"f".repeat(64),market_ids:vec!["2".into()],relations:vec![],
            settlements:BTreeMap::from([("2".into(),Value::Null)]),
            policy:QuotePolicy{quantities:vec!["10".into()],maximum_book_age_ms:5000}});
        assert_eq!(control.activate(&"a".repeat(64),1,next.clone()).unwrap(),2);
        assert_eq!(control.referenced_markets().unwrap(),std::collections::BTreeSet::from(["1".into(),"2".into()]));
        assert!(control.activate(&"a".repeat(64),1,next).is_err());
        let response=app.oneshot(request()).await.unwrap();
        assert_eq!(response.status(),StatusCode::OK);
        let body=axum::body::to_bytes(response.into_body(),65536).await.unwrap();
        let body:Value=serde_json::from_slice(&body).unwrap();
        assert_eq!(body["projection_id"],"e".repeat(64));
        assert_eq!(body["markets"].as_array().unwrap().len(),1);
        assert_eq!(body["markets"][0]["market_id"],"2");
        assert_eq!(body["markets"][0]["book_status"],"missing_book");
        let raw=tokio::time::timeout(Duration::from_secs(2),ws.next()).await.unwrap().unwrap().unwrap();
        let frame:Value=serde_json::from_str(raw.to_text().unwrap()).unwrap();
        assert_eq!(frame["projection_id"],"a".repeat(64));
        assert_eq!(frame["resync_required"],true);
        assert_eq!(frame["next_cursor"],0);
        assert_eq!(control.referenced_markets().unwrap(),std::collections::BTreeSet::from(["2".into()]));
        ws.close(None).await.unwrap();p.finish().await.unwrap();server.abort();
    }
    #[tokio::test]
    async fn http_baseline_and_native_ws_do_not_mix_future_book() {
        let mut p=Publication::start(0,Some(json!({"latest_cursor":0,"books":[],"markets":[market()],"gaps":[],"catalog_revision":"b".repeat(64)})),
            BTreeMap::from([("11".into(),"1".into()),("12".into(),"1".into())]),8,131072,65536, |_|Ok(())).unwrap();
        let app=router(p.reader(),config(),limits(65536)).unwrap();
        let response=app.clone().oneshot(axum::http::Request::builder()
            .uri("/v1/prediction-markets/polymarket/live/discovery/full-sync").body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(),StatusCode::OK);
        let body=axum::body::to_bytes(response.into_body(),65536).await.unwrap();
        let baseline:Value=serde_json::from_slice(&body).unwrap();drop(body);
        assert_eq!(baseline["boundary_cursor"],0);assert_eq!(baseline["markets"][0]["book_status"],"missing_book");
        let tiny=router(p.reader(),config(),limits(1)).unwrap().oneshot(axum::http::Request::builder()
            .uri("/v1/prediction-markets/polymarket/live/discovery/full-sync").body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(tiny.status(),StatusCode::PAYLOAD_TOO_LARGE);
        let events=["11","12"].iter().enumerate().map(|(index,token)|{
            marketcow_polymarket::discovery_source::snapshot_event(&json!({"asset_id":token,"market":"condition","tick_size":"0.01",
                "timestamp":"1700000000000","hash":"source","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
                marketcow_polymarket::discovery_source::SnapshotBoundary{market_id:"1",condition_id:"condition",token_id:token,
                    recovery_id:"test",cursor:index as u64+1,received_at:chrono::DateTime::from_timestamp(1700000001,0).unwrap()}).unwrap()
        }).collect();
        p.publish(events,None).unwrap();
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address=listener.local_addr().unwrap();let task=tokio::spawn(async move{axum::serve(listener,app).await.unwrap()});
        let (mut ws,_)=tokio_tungstenite::connect_async(format!("ws://{address}/v1/prediction-markets/polymarket/live/discovery/stream?projection_id={}&after_cursor=0","a".repeat(64))).await.unwrap();
        for cursor in 1..=2 {
            let raw=tokio::time::timeout(Duration::from_secs(2),ws.next()).await.unwrap().unwrap().unwrap();
            let frame:Value=serde_json::from_str(raw.to_text().unwrap()).unwrap();
            assert_eq!(frame["after_cursor"],cursor-1);assert_eq!(frame["next_cursor"],cursor);
            assert_eq!(frame["boundary_cursor"],2);assert_eq!(frame["resync_required"],false);
            let quote=&frame["items"][0]["payload"];
            assert_eq!(frame["items"][0]["type"],"market_update");assert_eq!(quote["cursor"],cursor);
            assert_eq!(quote["outcomes"][1]["best_ask"].is_null(),cursor==1);
        }
        ws.close(None).await.unwrap();p.finish().await.unwrap();task.abort();
    }
}
