//! Read-only cold catalog from the existing durable source index. A history
//! query never creates an acquisition lease or reactivates a token.
use anyhow::{Context, Result, ensure};
use axum::{Json, Router, extract::{Query, State}, http::StatusCode, response::{IntoResponse, Response}, routing::get};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Semaphore;

const MAX_RECORD_BYTES: usize = 256 * 1024;
const MAX_EVIDENCE_BYTES: usize = 8 * 1024 * 1024;
const MAX_CONCURRENT_READS: usize = 2;

#[derive(Clone)]
struct Reader {
    root: PathBuf,
    slots: Arc<Semaphore>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    market_id: String,
}

fn read(root: &PathBuf, id: &str) -> Result<Option<Value>> {
    ensure!(!id.is_empty() && id.len() <= 64 && id.bytes().all(|c| c.is_ascii_digit()),
        "invalid cold catalog market id");
    let db = Connection::open_with_flags(root.join("indexes/latest-state.sqlite3"),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
    let row: Option<(i64, Vec<u8>, String)> = db.query_row(
        "SELECT retired_after,payload_json,payload_sha256 FROM cold_market_catalog \
            WHERE market_id=? ORDER BY retired_after DESC,rowid DESC LIMIT 1",
        [id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).optional()?;
    row.map(|(after, bytes, sha)| {
        ensure!(bytes.len() <= MAX_RECORD_BYTES && after >= 0,
            "cold catalog record budget or cursor");
        ensure!(hex::encode(Sha256::digest(&bytes)) == sha,
            "cold catalog record hash mismatch");
        let record: Value = serde_json::from_slice(&bytes)?;
        ensure!(record["identity"]["market_id"] == id,
            "cold catalog record identity mismatch");
        Ok(json!({"schema_version":"marketcow.polymarket.cold-market.v1",
            "market_id":id,"retired_after_cursor":after,
            "catalog_record_sha256":sha,"catalog_record":record,
            "query_activated_acquisition":false}))
    }).transpose()
}

fn read_evidence(root: &PathBuf, id: &str) -> Result<Option<Value>> {
    ensure!(!id.is_empty() && id.len() <= 64 && id.bytes().all(|c| c.is_ascii_digit()),
        "invalid cold evidence market id");
    let db = Connection::open_with_flags(root.join("indexes/latest-state.sqlite3"),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
    let row: Option<(Vec<u8>, String)> = db.query_row(
        "SELECT payload_json,payload_sha256 FROM cold_market_evidence \
            WHERE market_id=? ORDER BY retired_after DESC LIMIT 1",
        [id], |row| Ok((row.get(0)?,row.get(1)?)),
    ).optional()?;
    row.map(|(bytes, sha)| {
        ensure!(bytes.len() <= MAX_EVIDENCE_BYTES,
            "cold evidence response byte budget");
        ensure!(hex::encode(Sha256::digest(&bytes)) == sha,
            "cold evidence hash mismatch");
        let evidence: Value = serde_json::from_slice(&bytes)?;
        ensure!(evidence["market_id"] == id,
            "cold evidence market identity mismatch");
        Ok(json!({"schema_version":"marketcow.polymarket.cold-evidence-read.v1",
            "market_id":id,"evidence_sha256":sha,"evidence":evidence,
            "query_activated_acquisition":false}))
    }).transpose()
}

async fn handle(State(state): State<Arc<Reader>>, Query(query): Query<Request>) -> Response {
    if query.market_id.is_empty() || query.market_id.len() > 64
        || !query.market_id.bytes().all(|c| c.is_ascii_digit()) {
        return (StatusCode::BAD_REQUEST, Json(json!({"detail":{"code":"invalid_market_id"}}))).into_response();
    }
    let Ok(permit) = state.slots.clone().try_acquire_owned() else {
        return (StatusCode::TOO_MANY_REQUESTS, Json(json!({"detail":{"code":"cold_read_busy"}}))).into_response();
    };
    let root = state.root.clone();
    let id = query.market_id;
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        read(&root, &id)
    }).await {
        Ok(Ok(Some(value))) => Json(value).into_response(),
        Ok(Ok(None)) => (StatusCode::NOT_FOUND, Json(json!({"detail":{"code":"cold_market_not_found"}}))).into_response(),
        Ok(Err(error)) => {
            eprintln!("{}", json!({"stage":"cold_market_read_failed","detail":format!("{error:#}")}));
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"detail":{"code":"cold_market_unavailable"}}))).into_response()
        }
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"detail":{"code":"cold_read_worker_failed"}}))).into_response(),
    }
}

async fn handle_evidence(State(state): State<Arc<Reader>>, Query(query): Query<Request>) -> Response {
    if query.market_id.is_empty() || query.market_id.len() > 64
        || !query.market_id.bytes().all(|c| c.is_ascii_digit()) {
        return (StatusCode::BAD_REQUEST, Json(json!({"detail":{"code":"invalid_market_id"}}))).into_response();
    }
    let Ok(permit) = state.slots.clone().try_acquire_owned() else {
        return (StatusCode::TOO_MANY_REQUESTS, Json(json!({"detail":{"code":"cold_read_busy"}}))).into_response();
    };
    let root = state.root.clone();
    let id = query.market_id;
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        read_evidence(&root, &id)
    }).await {
        Ok(Ok(Some(value))) => Json(value).into_response(),
        Ok(Ok(None)) => (StatusCode::NOT_FOUND, Json(json!({"detail":{"code":"cold_evidence_not_found"}}))).into_response(),
        Ok(Err(error)) => {
            eprintln!("{}", json!({"stage":"cold_evidence_read_failed","detail":format!("{error:#}")}));
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"detail":{"code":"cold_evidence_unavailable"}}))).into_response()
        }
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"detail":{"code":"cold_read_worker_failed"}}))).into_response(),
    }
}

pub fn router(root: PathBuf) -> Result<Router> {
    let root = root.canonicalize().context("cold catalog root")?;
    ensure!(root.is_absolute(), "cold catalog root must be absolute");
    Ok(Router::new().route(
        "/v1/prediction-markets/polymarket/history/cold-market", get(handle),
    ).route(
        "/v1/prediction-markets/polymarket/history/cold-evidence", get(handle_evidence),
    ).with_state(Arc::new(Reader { root, slots: Arc::new(Semaphore::new(MAX_CONCURRENT_READS)) })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    #[test]
    fn cold_history_is_hash_checked_and_never_creates_hot_state() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("indexes")).unwrap();
        let db = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        db.execute_batch("CREATE TABLE cold_market_catalog(\
            market_id TEXT NOT NULL,retired_after INTEGER NOT NULL,\
            payload_json BLOB NOT NULL,payload_sha256 TEXT NOT NULL,\
            PRIMARY KEY(market_id,payload_sha256));\
            CREATE TABLE cold_market_evidence(\
            market_id TEXT NOT NULL,retired_after INTEGER NOT NULL,\
            payload_json BLOB NOT NULL,payload_sha256 TEXT NOT NULL,\
            PRIMARY KEY(market_id,retired_after))").unwrap();
        let record = json!({"identity":{"market_id":"123","condition_id":"condition",
            "outcomes":[{"token_id":"1"},{"token_id":"2"}]}});
        let bytes = serde_json::to_vec(&record).unwrap();
        let sha = hex::encode(Sha256::digest(&bytes));
        db.execute("INSERT INTO cold_market_catalog VALUES (?,?,?,?)",
            rusqlite::params!["123", 42_i64, bytes, sha.clone()]).unwrap();
        let result = read(&root.path().to_path_buf(), "123").unwrap().unwrap();
        assert_eq!(result["catalog_record_sha256"], sha);
        assert_eq!(result["query_activated_acquisition"], false);
        assert!(read(&root.path().to_path_buf(), "124").unwrap().is_none());
        let evidence = json!({"market_id":"123","retired_after_cursor":42,"books":{},
            "recoveries":{},"lifecycle":null});
        let evidence_bytes = serde_json::to_vec(&evidence).unwrap();
        let evidence_hash = hex::encode(Sha256::digest(&evidence_bytes));
        db.execute("INSERT INTO cold_market_evidence VALUES (?,?,?,?)",
            rusqlite::params!["123",42_i64,evidence_bytes,evidence_hash.clone()]).unwrap();
        assert_eq!(read_evidence(&root.path().to_path_buf(),"123").unwrap().unwrap()
            ["evidence_sha256"],evidence_hash);
        assert!(read_evidence(&root.path().to_path_buf(),"124").unwrap().is_none());
        db.execute("UPDATE cold_market_evidence SET payload_sha256='bad' WHERE market_id='123'", [])
            .unwrap();
        assert!(read_evidence(&root.path().to_path_buf(),"123").is_err());
        db.execute("UPDATE cold_market_catalog SET payload_sha256='bad' WHERE market_id='123'", [])
            .unwrap();
        assert!(read(&root.path().to_path_buf(), "123").is_err());
    }

    #[tokio::test]
    async fn cold_market_and_evidence_loopback_fixture_is_read_only() {
        let root = tempfile::tempdir_in(std::env::temp_dir()).unwrap();
        std::fs::create_dir(root.path().join("indexes")).unwrap();
        let db = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        db.execute_batch("CREATE TABLE cold_market_catalog(\
            market_id TEXT NOT NULL,retired_after INTEGER NOT NULL,\
            payload_json BLOB NOT NULL,payload_sha256 TEXT NOT NULL,\
            PRIMARY KEY(market_id,payload_sha256));\
            CREATE TABLE cold_market_evidence(\
            market_id TEXT NOT NULL,retired_after INTEGER NOT NULL,\
            payload_json BLOB NOT NULL,payload_sha256 TEXT NOT NULL,\
            PRIMARY KEY(market_id,retired_after))").unwrap();
        let record = json!({"identity":{"market_id":"123","condition_id":"synthetic-condition",
            "outcomes":[{"token_id":"1231"},{"token_id":"1232"}]},
            "lifecycle_state":"resolved"});
        let evidence = json!({"market_id":"123","retired_after_cursor":42,
            "books":{"1231":{"best_bid":"0.41"}},"recoveries":{},
            "lifecycle":{"lifecycle_state":"resolved"}});
        for (table, payload) in [("cold_market_catalog",&record),
            ("cold_market_evidence",&evidence)] {
            let bytes = serde_json::to_vec(payload).unwrap();
            let sha = hex::encode(Sha256::digest(&bytes));
            db.execute(&format!("INSERT INTO {table} VALUES (?,?,?,?)"),
                rusqlite::params!["123",42_i64,bytes,sha]).unwrap();
        }
        drop(db);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}",listener.local_addr().unwrap());
        let app = router(root.path().to_path_buf()).unwrap();
        let service = tokio::spawn(async move { axum::serve(listener,app).await.unwrap(); });
        let client = reqwest::Client::new();
        let mut responses = BTreeMap::new();
        for name in ["cold-market","cold-evidence"] {
            let response = client.get(format!("{base}/v1/prediction-markets/polymarket/history/{name}?market_id=123"))
                .send().await.unwrap();
            assert_eq!(response.status(),reqwest::StatusCode::OK);
            let bytes = response.bytes().await.unwrap();
            assert!(bytes.len() < 4096);
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["market_id"],"123");
            assert_eq!(body["query_activated_acquisition"],false);
            responses.insert(name.to_owned(),(bytes,body));
        }
        assert_eq!(responses["cold-market"].1["catalog_record"],record);
        assert_eq!(responses["cold-evidence"].1["evidence"],evidence);
        assert_eq!(responses["cold-market"].1["catalog_record_sha256"],
            hex::encode(Sha256::digest(serde_json::to_vec(&record).unwrap())));
        assert_eq!(responses["cold-evidence"].1["evidence_sha256"],
            hex::encode(Sha256::digest(serde_json::to_vec(&evidence).unwrap())));
        let absent = client.get(format!("{base}/v1/prediction-markets/polymarket/history/cold-market?market_id=124"))
            .send().await.unwrap();
        assert_eq!(absent.status(),reqwest::StatusCode::NOT_FOUND);
        if let Ok(dir) = std::env::var("MARKETCOW_COLD_FIXTURE_DIR") {
            let dir = PathBuf::from(dir);
            assert!(dir.starts_with("/Volumes/T9/"),"fixture output must stay on T9");
            std::fs::create_dir_all(&dir).unwrap();
            for (name,(bytes,_)) in responses {
                std::fs::write(dir.join(format!("{name}.json")),&bytes).unwrap();
            }
        }
        service.abort();
        let _ = service.await;
    }
}
