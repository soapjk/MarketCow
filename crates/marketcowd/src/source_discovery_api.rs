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
struct OwnedBytes {bytes:Vec<u8>,_permit:OwnedSemaphorePermit}
impl AsRef<[u8]> for OwnedBytes {fn as_ref(&self)->&[u8]{&self.bytes}}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Resume {projection_id:String,after_cursor:u64}

pub fn router(reader:MemoryReader,config:Arc<DiscoveryConfig>,limits:DiscoveryLimits)->Result<Router> {
    ensure!((1..=4).contains(&limits.clients)&&(1..=4).contains(&limits.cached_baselines),"explicit Discovery client/baseline limits");
    ensure!(limits.full_sync_bytes>0&&limits.full_sync_bytes<=256*1024*1024&&limits.frame_bytes>0&&limits.frame_bytes<=16*1024*1024,
        "Discovery response byte limits");
    ensure!(limits.state_bytes>0&&limits.state_bytes<=256*1024*1024&&limits.replay_bytes>0&&limits.replay_bytes<=64*1024*1024&&!limits.send_timeout.is_zero(),
        "Discovery state/replay/time limits");
    let api=Arc::new(Api{reader,config,clients:Arc::new(Semaphore::new(limits.clients)),limits,
        baselines:Mutex::new(VecDeque::new()),snapshots:Arc::new(Semaphore::new(1))});
    Ok(Router::new()
        .route("/v1/prediction-markets/polymarket/live/discovery/full-sync",get(full_sync))
        .route("/v1/prediction-markets/polymarket/live/discovery/status",get(status))
        .route("/v1/prediction-markets/polymarket/live/discovery/stream",get(upgrade)).with_state(api))
}
fn error(status:StatusCode,code:&str,retry:bool)->Response {
    (status,axum::Json(json!({"detail":{"code":code,"message":code,"retryable":retry}}))).into_response()
}
async fn full_sync(State(api):State<Arc<Api>>)->Response {
    let Ok(permit)=api.snapshots.clone().try_acquire_owned() else {return error(StatusCode::SERVICE_UNAVAILABLE,"discovery_full_sync_capacity",true)};
    let result=tokio::task::spawn_blocking(move ||->Result<OwnedBytes>{
        let view=Arc::new(api.reader.capture()?);
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
async fn status(State(api):State<Arc<Api>>)->Response {
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
async fn upgrade(State(api):State<Arc<Api>>,Query(query):Query<Resume>,ws:WebSocketUpgrade)->Response {
    let Ok(permit)=api.clients.clone().try_acquire_owned() else {return error(StatusCode::SERVICE_UNAVAILABLE,"discovery_stream_capacity",true)};
    ws.max_message_size(4096).max_frame_size(4096).on_upgrade(move |mut socket|async move {
        let _permit=permit;
        let mut query=query;
        if let Err(e)=stream(&api,&mut socket,&mut query).await {
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
