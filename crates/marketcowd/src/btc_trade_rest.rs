//! Independent official REST acquisition. Never reads or mutates the WS owner.
use anyhow::Result;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use std::{path::PathBuf, sync::{Arc, atomic::{AtomicU64, Ordering}}, time::Duration};
use tokio::{io::AsyncWriteExt, sync::{Semaphore, mpsc}};

const TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct Service {
    client: reqwest::Client,
    slots: Arc<Semaphore>,
    archive: mpsc::Sender<Vec<u8>>,
    archive_dropped: Arc<AtomicU64>,
    identity_database:PathBuf,
}

impl Service {
    pub fn start(root: PathBuf) -> Result<Self> {
        let identity_database=root.join("binance-trades/trades.sqlite3");
        let (archive, mut receiver) = mpsc::channel::<Vec<u8>>(128);
        let dropped = Arc::new(AtomicU64::new(0));
        let writer_dropped = dropped.clone();
        tokio::spawn(async move {
            let result = async {
                let root = root.join("binance-official-rest");
                tokio::fs::create_dir_all(&root).await?;
                let path = root.join(format!("requests-{}.jsonl", uuid::Uuid::new_v4().simple()));
                let mut file = tokio::fs::OpenOptions::new().create_new(true).write(true).open(path).await?;
                let mut written = 0usize;
                while let Some(record) = receiver.recv().await {
                    if written.saturating_add(record.len()+1) > 64*1024*1024 {
                        writer_dropped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    file.write_all(&record).await?;
                    file.write_all(b"\n").await?;
                    written += record.len()+1;
                }
                file.sync_data().await?;
                Ok::<(), anyhow::Error>(())
            }.await;
            if let Err(error) = result { tracing::warn!(%error, "binance_rest_archive_unavailable"); }
        });
        Ok(Self {client: reqwest::Client::builder().no_proxy()
            .timeout(TIMEOUT).redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never()).build()?,
            slots: Arc::new(Semaphore::new(8)), archive, archive_dropped: dropped,identity_database})
    }
    pub fn router(&self)->axum::Router {
        axum::Router::new().route("/v1/facts/btc/rest",axum::routing::post(read_native))
            .with_state(self.clone()).layer(axum::extract::DefaultBodyLimit::max(64*1024))
    }

}

async fn read_native(axum::extract::State(service):axum::extract::State<Service>,headers:axum::http::HeaderMap,
    body:std::result::Result<axum::body::Bytes,axum::extract::rejection::BytesRejection>)->Response {
    use marketcow_contracts::typed_v3::facts;use prost::Message;
    use crate::source_facts_rest::{Failed,response};
    let request=match crate::source_facts_rest::decode::<facts::BtcRestRequest>(&headers,body){Ok(v)=>v,Err(e)=>return e.into_response()};
    let identity=crate::source_facts_rest::provisional_identity();
    let adapter=crate::source_facts_rest::Service::from_existing(service.client.clone(),service.slots.clone());
    let (mut value,raw)=match adapter.btc_raw(&request,identity).await{Ok(v)=>v,Err(e)=>return e.into_response()};
    // Only the archival branch generates JSON. Business replies remain typed.
    if let Ok(raw_text)=std::str::from_utf8(&raw) {
        let archive=serde_json::to_vec(&json!({"schema_version":"marketcow.binance-rest-raw.v2","request_id":request.request_id,
            "raw_utf8":raw_text,"upstream_url":crate::source_facts_rest::BINANCE_URL,"observed_at":chrono::Utc::now()}));
        if let Ok(bytes)=archive {if service.archive.try_send(bytes).is_err(){service.archive_dropped.fetch_add(1,Ordering::Relaxed);}}
    }
    let path=service.identity_database.clone();let source=value.source;let mut payload=value.clone();payload.identity=None;payload.request_id.clear();
    payload.upstream=None;payload.latest_source_age_ms_at_receive=None;
    if let Some(trade)=&mut payload.latest_trade {trade.received_at_ns=None;}
    let saved=tokio::task::spawn_blocking(move||->Result<_>{
        let mut db=crate::btc_typed_store::open(&path)?;crate::source_facts_identity::initialize(&db)?;
        crate::source_facts_identity::record(&mut db,"btc-rest",&source.to_be_bytes(),&payload.encode_to_vec())
    }).await;
    match saved {Ok(Ok(identity))=>{value.identity=Some(identity);response(200,&value,"marketcow.facts.v1.BtcRestResponse",None)},
        _=>Failed::local(&request.request_id,"database_unavailable",503).into_response()}
}
