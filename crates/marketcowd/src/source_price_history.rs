//! Bounded research read-through. Independent of live publication and persistence.
use axum::{extract::{Query, State}, http::StatusCode, response::{IntoResponse, Response}, routing::get, Json, Router};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::{Duration, Instant}};
use tokio::sync::{Mutex, Semaphore};

const CAP: usize = 1024 * 1024;
const URL: &str = "https://clob.polymarket.com/prices-history";
#[derive(Clone)]
struct Service { client: reqwest::Client, slots: Arc<Semaphore>, last: Arc<Mutex<Option<Instant>>>, upstream: String }
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Request { token_id: String, start_ts: i64, end_ts: i64, fidelity_minutes: u32 }
impl Request {
    fn valid(&self, now: i64) -> bool {
        !self.token_id.is_empty() && self.token_id.len() <= 78
            && self.token_id.bytes().all(|c| c.is_ascii_digit())
            && self.start_ts >= 0 && self.end_ts > self.start_ts && self.end_ts <= now
            && self.end_ts - self.start_ts <= 7*86400
            && (1..=10080).contains(&self.fidelity_minutes)
    }
}
pub fn router() -> anyhow::Result<Router> {
    let client = reqwest::Client::builder().timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none()).retry(reqwest::retry::never()).build()?;
    Ok(Router::new().route("/v1/prediction-markets/polymarket/history/prices", get(history))
        .with_state(Service {client, slots: Arc::new(Semaphore::new(2)), last: Arc::new(Mutex::new(None)), upstream: URL.into()}))
}
fn error(status: StatusCode, code: &str) -> Response {
    (status, Json(serde_json::json!({"schema_version":"marketcow.polymarket.history-error.v1","code":code}))).into_response()
}
async fn history(State(service): State<Service>, query: Result<Query<Request>, axum::extract::rejection::QueryRejection>) -> Response {
    let Ok(Query(request)) = query else { return error(StatusCode::BAD_REQUEST,"invalid_request"); };
    if !request.valid(chrono::Utc::now().timestamp()) { return error(StatusCode::BAD_REQUEST,"invalid_request"); }
    let Ok(_permit) = service.slots.try_acquire() else { return error(StatusCode::TOO_MANY_REQUESTS,"resource_unavailable"); };
    {
        let mut last = service.last.lock().await;
        if last.is_some_and(|t| t.elapsed() < Duration::from_secs(1)) {
            return error(StatusCode::TOO_MANY_REQUESTS,"rate_limited");
        }
        *last = Some(Instant::now());
    }
    let started_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis,true);
    let work = async {
        let mut response = service.client.get(&service.upstream).query(&[
            ("market",request.token_id.clone()),("startTs",request.start_ts.to_string()),
            ("endTs",request.end_ts.to_string()),("fidelity",request.fidelity_minutes.to_string())
        ]).send().await.map_err(|_| "upstream_transport_failed")?;
        let status = response.status().as_u16();
        if response.content_length().is_some_and(|n| n > CAP as u64) { return Err("response_size_exceeded"); }
        let mut raw = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| "upstream_body_failed")? {
            if chunk.len() > CAP - raw.len() { return Err("response_size_exceeded"); }
            raw.extend_from_slice(&chunk);
        }
        Ok((status,raw))
    };
    let result = tokio::time::timeout(Duration::from_secs(15),work).await;
    match result {
        Ok(Ok((status,raw))) => Json(serde_json::json!({
            "schema_version":"marketcow.polymarket.price-history-evidence.v1",
            "request":request,"source_url":URL,"started_at":started_at,
            "received_at":chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis,true),
            "upstream_status":status,"raw_complete":true,"raw_bytes":raw.len(),
            "raw_sha256":hex::encode(Sha256::digest(&raw)),"raw_base64":STANDARD.encode(&raw)
        })).into_response(),
        Ok(Err(code)) => error(StatusCode::BAD_GATEWAY,code),
        Err(_) => error(StatusCode::GATEWAY_TIMEOUT,"upstream_timeout"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_past_window() {
        let mut r=Request{token_id:"123".into(),start_ts:1,end_ts:100,fidelity_minutes:60};
        assert!(r.valid(100)); assert!(!r.valid(99));
        r.end_ts=1; assert!(!r.valid(100));
        r.end_ts=604802; assert!(!r.valid(604802));
        r.end_ts=100; r.token_id="http://localhost".into(); assert!(!r.valid(100));
    }
    #[tokio::test]
    async fn invalid_request_never_fetches() {
        use tower::ServiceExt;
        let response=router().unwrap().oneshot(axum::http::Request::builder()
            .uri("/v1/prediction-markets/polymarket/history/prices?token_id=1&start_ts=10&end_ts=1&fidelity_minutes=1")
            .body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(),StatusCode::BAD_REQUEST);
    }
    #[tokio::test]
    async fn real_http_evidence_and_resource_limits() {
        use std::sync::atomic::{AtomicUsize,Ordering};
        let calls=Arc::new(AtomicUsize::new(0));
        let counter=calls.clone();
        let upstream=Router::new().route("/",get(move || {
            counter.fetch_add(1,Ordering::SeqCst);
            async { (StatusCode::FORBIDDEN,"error code: 1010\n") }
        }));
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr=listener.local_addr().unwrap();
        let task=tokio::spawn(async move {axum::serve(listener,upstream).await.unwrap()});
        let service=Service {client:reqwest::Client::builder().no_proxy().build().unwrap(),
            slots:Arc::new(Semaphore::new(1)),last:Arc::new(Mutex::new(None)),upstream:format!("http://{addr}/")};
        let request=|| Ok(Query(Request {token_id:"123".into(),start_ts:1,end_ts:100,fidelity_minutes:60}));
        let response=history(State(service.clone()),request()).await;
        assert_eq!(response.status(),StatusCode::OK);
        let bytes=axum::body::to_bytes(response.into_body(),2*CAP).await.unwrap();
        let value:serde_json::Value=serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["upstream_status"],403);
        assert_eq!(value["raw_sha256"],hex::encode(Sha256::digest(b"error code: 1010\n")));
        assert_eq!(history(State(service.clone()),request()).await.status(),StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(calls.load(Ordering::SeqCst),1);
        let _permit=service.slots.acquire().await.unwrap();
        assert_eq!(history(State(service.clone()),request()).await.status(),StatusCode::TOO_MANY_REQUESTS);
        task.abort(); let _=task.await;
    }
}
