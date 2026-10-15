//! Independent official REST acquisition. Never reads or mutates the WS owner.
use anyhow::{Result, ensure};
use axum::{Json, http::{StatusCode, header, HeaderValue}, response::{IntoResponse, Response}};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::{Arc, atomic::{AtomicU64, Ordering}}, time::{Duration, Instant}};
use tokio::{io::AsyncWriteExt, sync::{Semaphore, mpsc}};

const URL: &str = "https://data-api.binance.vision/api/v3/trades?symbol=BTCUSDT&limit=1";
const BYTES: usize = 65536;
const TIMEOUT: Duration = Duration::from_secs(2);
const SCHEMA: &str = "marketcow.binance-rest-trade.v1";

#[derive(Clone)]
pub struct Service {
    client: reqwest::Client,
    slots: Arc<Semaphore>,
    archive: mpsc::Sender<Vec<u8>>,
    archive_dropped: Arc<AtomicU64>,
}

impl Service {
    pub fn start(root: PathBuf) -> Result<Self> {
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
            slots: Arc::new(Semaphore::new(8)), archive, archive_dropped: dropped})
    }

    pub async fn read(&self) -> Response {
        self.read_url(URL).await
    }

    async fn read_url(&self, url: &str) -> Response {
        let request_id = uuid::Uuid::new_v4().simple().to_string();
        let started = chrono::Utc::now().to_rfc3339();
        let timer = Instant::now();
        let Ok(_slot) = self.slots.try_acquire() else {
            return reply(StatusCode::SERVICE_UNAVAILABLE,
                error(&request_id, "rest_capacity", None, None), None);
        };
        let operation = async {
            let mut response = self.client.get(url).send().await
                .map_err(|error| if error.is_timeout() {"upstream_timeout"} else {"upstream_transport"})?;
            let status = response.status().as_u16();
            let retry = response.headers().get(header::RETRY_AFTER).cloned();
            let content_type = response.headers().get(header::CONTENT_TYPE)
                .and_then(|h| h.to_str().ok()).map(str::to_owned);
            if response.content_length().is_some_and(|n| n > BYTES as u64) {
                return Err("upstream_body_too_large");
            }
            let mut raw = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| "upstream_body")? {
                if chunk.len() > BYTES-raw.len() {return Err("upstream_body_too_large");}
                raw.extend_from_slice(&chunk);
            }
            Ok((status, retry, content_type, raw))
        };
        let (status, retry, content_type, raw) = match tokio::time::timeout(TIMEOUT, operation).await {
            Err(_) => return reply(StatusCode::GATEWAY_TIMEOUT, error(&request_id,"upstream_timeout",None,None),None),
            Ok(Err(code)) => return reply(if code == "upstream_timeout" {StatusCode::GATEWAY_TIMEOUT}
                else {StatusCode::BAD_GATEWAY}, error(&request_id,code,None,None),None),
            Ok(Ok(result)) => result,
        };
        let received = chrono::Utc::now();
        let mut value = if status == 200 {
            match project(&raw, &request_id, received.timestamp_millis()) {
                Ok(value) => value,
                Err(_) => error(&request_id,"upstream_invalid_trade",Some(status),None),
            }
        } else {error(&request_id, if status == 429 {"upstream_rate_limited"}
            else if status == 418 {"upstream_ip_banned"} else {"upstream_http"}, Some(status),
            retry.as_ref().and_then(|h| h.to_str().ok()))};
        value["upstream_url"] = json!(url);
        value["upstream_status"] = json!(status);
        value["upstream_content_type"] = json!(content_type);
        value["upstream_started_at"] = json!(started);
        value["upstream_received_at"] = json!(received.to_rfc3339());
        value["upstream_duration_us"] = json!(timer.elapsed().as_micros() as u64);
        value["response_at"] = json!(chrono::Utc::now().to_rfc3339());
        value["raw_sha256"] = json!(hex::encode(Sha256::digest(&raw)));
        value["raw_base64"] = json!(STANDARD.encode(&raw));
        value["raw_bytes"] = json!(raw.len());
        value["raw_complete"] = json!(true);
        // A bounded try_send only; archive health never gates acquisition.
        if let Ok(encoded) = serde_json::to_vec(&value) {
            if self.archive.try_send(encoded).is_err() {self.archive_dropped.fetch_add(1,Ordering::Relaxed);}
        }
        value["archive_dropped_total"] = json!(self.archive_dropped.load(Ordering::Relaxed));
        let http = if status != 200 {StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY)}
            else if value["available"] == true {StatusCode::OK} else {StatusCode::BAD_GATEWAY};
        reply(http,value,retry)
    }
}

fn error(request_id: &str, code: &str, status: Option<u16>, retry: Option<&str>) -> Value {
    json!({"schema_version":SCHEMA,"source":"binance_spot_trade","acquisition":"official_rest",
        "instrument_id":"BTCUSDT.BINANCE","request_id":request_id,"available":false,
        "code":code,"upstream_status":status,"retry_after":retry,"latest_trade":null,
        "cache_used":false,"ws_dependency":false,"execution_readiness":false})
}

fn project(raw: &[u8], request_id: &str, received_ms: i64) -> Result<Value> {
    ensure!(raw.len() <= BYTES, "body budget");
    let rows: Vec<Value> = serde_json::from_slice(raw)?;
    ensure!(rows.len() == 1, "expected latest recent trade");
    let row = &rows[0];
    let id = row["id"].as_u64().ok_or_else(||anyhow::anyhow!("trade id"))?;
    let at = row["time"].as_i64().ok_or_else(||anyhow::anyhow!("trade time"))?;
    ensure!(at > 0 && row["isBuyerMaker"].is_boolean(), "supplier fields");
    for field in ["price","qty"] {
        let value = row[field].as_str().ok_or_else(||anyhow::anyhow!("decimal string"))?;
        ensure!(value.parse::<rust_decimal::Decimal>()? > rust_decimal::Decimal::ZERO, "positive decimal");
    }
    Ok(json!({"schema_version":SCHEMA,"source":"binance_spot_trade","acquisition":"official_rest",
        "instrument_id":"BTCUSDT.BINANCE","supplier":"Binance Spot","request_id":request_id,
        "available":true,"price_semantics":"latest_trade_from_recent_trades_not_ticker_or_twap",
        "latest_trade":{"trade_id":id,"price":row["price"],"quantity":row["qty"],
            "trade_time_ms":at,"event_time_ms":null,"buyer_is_maker":row["isBuyerMaker"],"source_row_index":0},
        "latest_trade_age_ms_at_receive":received_ms-at,
        "time_semantics":{"trade_time_ms":"supplier_REST_time_unix_ms",
            "event_time_ms":"not_provided_by_REST","upstream_received_at":"local_diagnostic_only",
            "response_at":"local_diagnostic_only"},
        "continuity":"single_independent_observation_no_stream_prefix_claim",
        "freshness_policy":"consumer_explicit_max_trade_age_ms_required",
        "cache_used":false,"ws_dependency":false,"execution_readiness":false}))
}

fn reply(status: StatusCode, value: Value, retry: Option<HeaderValue>) -> Response {
    let mut response = (status,Json(value)).into_response();
    response.headers_mut().insert(header::CACHE_CONTROL,HeaderValue::from_static("no-store"));
    response.headers_mut().insert("x-marketcow-acquisition",HeaderValue::from_static("official_rest"));
    if let Some(retry) = retry {response.headers_mut().insert(header::RETRY_AFTER,retry);}
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn every_request_hits_upstream_without_ws_owner_or_cache() {
        use axum::{Router, routing::get};
        let count = Arc::new(AtomicU64::new(0));
        let upstream_count = count.clone();
        let app = Router::new().route("/",get(move || {
            let count = upstream_count.clone();
            async move {Json(json!([{"id":count.fetch_add(1,Ordering::Relaxed)+1,
                "price":"12345.67","qty":"0.01","time":1000,"isBuyerMaker":false}]))}
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/",listener.local_addr().unwrap());
        let server = tokio::spawn(async {axum::serve(listener,app).await.unwrap()});
        let (archive, mut receiver) = mpsc::channel(128);
        let service = Service {client:reqwest::Client::builder().no_proxy().build().unwrap(),
            slots:Arc::new(Semaphore::new(8)),archive,archive_dropped:Arc::new(AtomicU64::new(0))};
        for id in [1,2] {
            let response = service.read_url(&url).await;
            assert_eq!(response.status(),StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(),128*1024).await.unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["latest_trade"]["trade_id"],id);
            let raw = STANDARD.decode(body["raw_base64"].as_str().unwrap()).unwrap();
            assert_eq!(body["raw_sha256"],hex::encode(Sha256::digest(&raw)));
            assert!(body.get("source_epoch").is_none());
            assert!(receiver.try_recv().is_ok());
        }
        assert_eq!(count.load(Ordering::Relaxed),2);
        server.abort();
    }
    #[test]
    fn supplier_trade_time_is_not_receipt_and_has_no_ws_identity() {
        let raw = br#"[{"id":123,"price":"12345.6700","qty":"0.0100","time":1000,"isBuyerMaker":false}]"#;
        let result = project(raw,"request",2300).unwrap();
        assert_eq!(result["latest_trade"]["trade_time_ms"],1000);
        assert_eq!(result["latest_trade_age_ms_at_receive"],1300);
        assert_eq!(result["latest_trade"]["price"],"12345.6700");
        assert!(result.get("source_epoch").is_none());
        assert_eq!(result["ws_dependency"],false);
        assert!(project(br#"[{"id":123,"price":"1","qty":"1","isBuyerMaker":false}]"#,"r",2300).is_err());
    }
    #[test]
    fn upstream_rate_limit_remains_independent_and_preserves_retry_after() {
        let response = reply(StatusCode::TOO_MANY_REQUESTS,
            error("r","upstream_rate_limited",Some(429),Some("3")),Some(HeaderValue::from_static("3")));
        assert_eq!(response.status(),StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::RETRY_AFTER],"3");
        assert_eq!(response.headers()[header::CACHE_CONTROL],"no-store");
    }
}
