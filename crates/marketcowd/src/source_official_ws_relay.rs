//! Public read-only official CLOB book acquisition. Tradude owns direct
//! pre-order reads; this optional public data route is not a trading gate.
use std::{sync::Arc, time::Duration};
use anyhow::Result;
use axum::{Router, extract::State, http::HeaderMap, response::{IntoResponse,Response}, routing::post};
use tokio::sync::Semaphore;
use crate::{source_btc5m_api::Control,source_publication::MemoryReader};
const MAX_CLIENTS:usize=32;
const REQUEST_TIMEOUT:Duration=Duration::from_secs(2);
#[derive(Clone)]
struct Relay {control:Control, client:reqwest::Client, slots:Arc<Semaphore>}

pub fn router(control: Control, _reader: MemoryReader) -> Result<Router> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .timeout(REQUEST_TIMEOUT)
        .build()?;
    Ok(Router::new()
        .route("/v1/facts/polymarket/official-book",post(http_native_book))
        .layer(axum::extract::DefaultBodyLimit::max(64*1024))
        .with_state(Relay {
            control,
            client,
            slots: Arc::new(Semaphore::new(MAX_CLIENTS)),
        }))
}
async fn http_native_book(State(relay):State<Relay>,headers:HeaderMap,
    body:std::result::Result<axum::body::Bytes,axum::extract::rejection::BytesRejection>)->Response {
    use marketcow_contracts::typed_v3::{self as native,facts};
    use prost::Message;
    use crate::source_facts_rest::{Failed,response};
    let Ok(body)=body else{return Failed::local("","request_capacity",413).into_response()};
    if body.len()>64*1024{return Failed::local("","request_capacity",413).into_response();}
    if headers.get("content-type").and_then(|h|h.to_str().ok())!=Some(native::CONTENT_TYPE){return Failed::local("","unsupported_content_type",415).into_response();}
    let request=match facts::OfficialBookRequest::decode(body){Ok(v)=>v,Err(_)=>return Failed::local("","invalid_request",400).into_response()};
    let dispatch=match relay.control.native_dispatch(&request){Ok(v)=>v,Err(_)=>return Failed::local(&request.request_id,"identity_not_active",409).into_response()};
    let identity=crate::source_facts_rest::provisional_identity();
    let client=crate::source_facts_rest::Service::from_existing(relay.client.clone(),relay.slots.clone());
    match client.book(&request,dispatch,identity).await {
        Ok(mut value)=>{
            value.identity=None;
            let control=relay.control.clone();let mut semantic=value.clone();semantic.request_id.clear();semantic.upstream=None;
            if let Some(dispatch)=&mut semantic.dispatch{dispatch.identity_checked_at_ns=0;}
            let encoded=semantic.encode_to_vec();
            let key=request.key.as_ref().map(|k|k.encode_to_vec()).unwrap_or_default();
            let namespace=format!("book-{}",request.token_id);
            match tokio::task::spawn_blocking(move||control.native_fact_record(&namespace,&key,&encoded)).await {
                Ok(Ok(identity))=>{value.identity=Some(identity);response(200,&value,"marketcow.facts.v1.OfficialBookResponse",None)},
                _=>Failed::local(&request.request_id,"database_unavailable",503).into_response(),
            }
        },
        Err(error)=>error.into_response(),
    }
}
