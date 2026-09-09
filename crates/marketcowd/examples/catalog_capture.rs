//! Fresh catalog-only keyset capture. No reuse, book requests, selection or listener.
use anyhow::{Context, Result, ensure};
use clap::Parser;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};
#[derive(Parser)]
struct Args {
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    maximum_pages: usize,
    #[arg(long)]
    maximum_bytes: usize,
    #[arg(long)]
    maximum_page_bytes: usize,
    #[arg(long)]
    maximum_seconds: u64,
    #[arg(long)]
    request_seconds: u64,
    #[arg(long)]
    interval_millis: u64,
    #[arg(long)]
    closed: bool,
    #[arg(long)]
    market_ids: Option<PathBuf>,
}
async fn bounded_body(
    response: &mut reqwest::Response,
    maximum: usize,
) -> (Vec<u8>, bool, Option<String>) {
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let keep = chunk.len().min(maximum - body.len());
                body.extend_from_slice(&chunk[..keep]);
                if keep != chunk.len() {
                    return (body, true, None);
                }
            }
            Ok(None) => return (body, false, None),
            Err(error) => return (body, true, Some(format!("{error:#}"))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let a = Args::parse();
    ensure!(
        a.root.is_absolute()
            && a.maximum_pages > 0
            && a.maximum_bytes > 0
            && a.maximum_page_bytes > 0
            && a.maximum_seconds > 0
            && a.request_seconds > 0,
        "explicit positive budgets"
    );
    let ids: Vec<String> = if let Some(path) = &a.market_ids {
        ensure!(path.metadata()?.len() <= 65536, "identity file budget");
        let values: Vec<String> = serde_json::from_slice(&std::fs::read(path)?)?;
        ensure!(
            !values.is_empty() && values.len() <= a.maximum_pages,
            "identity request budget"
        );
        let mut unique = std::collections::HashSet::new();
        for id in &values {
            ensure!(
                !id.is_empty()
                    && id.bytes().all(|c| c.is_ascii_digit())
                    && unique.insert(id.clone()),
                "numeric unique ids required"
            );
        }
        values
    } else {
        vec![]
    };
    std::fs::create_dir(&a.root)?; // Existing capture cannot be resumed as new evidence.
    let started = chrono::Utc::now();
    let deadline = Instant::now() + Duration::from_secs(a.maximum_seconds);
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    let db = rusqlite::Connection::open(a.root.join("identity-check.sqlite"))?;
    db.execute_batch("PRAGMA cache_size=-8192; CREATE TABLE ids(id TEXT PRIMARY KEY); CREATE TABLE cursors(id TEXT PRIMARY KEY);")?;
    let mut cursor: Option<String> = None;
    let mut bytes = 0usize;
    let mut count = 0usize;
    let mut pages = 0usize;
    let mut log = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(a.root.join("pages.jsonl"))?;
    let result:Result<()> = async {
        loop {
            ensure!(pages<a.maximum_pages && bytes<a.maximum_bytes,"capture budget before request");
            let remaining=deadline.checked_duration_since(Instant::now()).filter(|d|!d.is_zero()).context("capture deadline")?;
            let mut query=vec![("limit","100".to_owned()),("closed",a.closed.to_string()),("order","id".to_owned()),("ascending","true".to_owned())];
            if let Some(c)=&cursor {query.push(("after_cursor",c.clone()));}
            let url=if ids.is_empty(){"https://gamma-api.polymarket.com/markets/keyset".to_owned()}else{query.clear();format!("https://gamma-api.polymarket.com/markets/{}",ids[pages])};
            let mut r=client.get(&url).query(&query)
                .timeout(remaining.min(Duration::from_secs(a.request_seconds))).send().await?;
            let status=r.status().as_u16();
            let (body,truncated,body_error)=bounded_body(&mut r,a.maximum_page_bytes.min(a.maximum_bytes-bytes)).await;
            bytes+=body.len();
            pages+=1;let name=format!("page-{pages:06}.json");
            std::fs::write(a.root.join(&name),&body)?;
            writeln!(log,"{}",json!({"page":pages,"file":name,"url":url,"params":query,"received_at":chrono::Utc::now(),"status":status,"raw_bytes":body.len(),"raw_sha256":hex::encode(Sha256::digest(&body)),"truncated":truncated,"body_error":body_error}))?;
            log.flush()?;
            ensure!(status==200 && !truncated,"upstream failure/truncation");
            let value:serde_json::Value=serde_json::from_slice(&body)?;
            let singleton=vec![value.clone()];
            let markets=if ids.is_empty(){value["markets"].as_array().context("markets array missing")?}else{
                ensure!(value["id"].as_str()==Some(ids[pages-1].as_str()),"requested identity mismatch"); &singleton
            };
            ensure!(markets.len()<=100,"unexpected page size");
            for m in markets {
                let id=m["id"].as_str().filter(|s|!s.is_empty()).context("market id")?;
                db.execute("INSERT INTO ids VALUES(?)",[id])?;count+=1;
            }
            if !ids.is_empty() {
                if pages==ids.len(){break;}
                tokio::time::sleep(Duration::from_millis(a.interval_millis)).await;
                continue;
            }
            cursor=match value.get("next_cursor") {None|Some(serde_json::Value::Null)=>None,Some(v)=>Some(v.as_str().filter(|s|!s.is_empty()).context("cursor invalid")?.to_owned())};
            if let Some(c)=&cursor {
                ensure!(!markets.is_empty(),"nonterminal empty page");
                db.execute("INSERT INTO cursors VALUES(?)",[c])?;
            } else {break;}
            tokio::time::sleep(Duration::from_millis(a.interval_millis)).await;
        }
        Ok(())
    }.await;
    let report = json!({"schema_version":"marketcow.catalog-capture.v1","capture_started_at":started,
        "capture_completed_at":chrono::Utc::now(),"closed_filter":a.closed,"complete":result.is_ok(),
        "pages":pages,"market_count":count,"retained_raw_bytes":bytes,"terminal_cursor":cursor,
        "requested_market_ids":ids,
        "budgets":{"maximum_pages":a.maximum_pages,"maximum_bytes":a.maximum_bytes,
            "maximum_page_bytes":a.maximum_page_bytes,"maximum_seconds":a.maximum_seconds,
            "request_seconds":a.request_seconds,"interval_millis":a.interval_millis},
        "source_atomic_snapshot":false,"transport_chunk_can_exceed_retained_cap":true,
        "error":result.as_ref().err().map(|e|format!("{e:#}"))});
    std::fs::write(
        a.root.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    log.sync_all()?;
    println!("{report}");
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    async fn response(wire: &'static [u8]) -> (reqwest::Response, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = [0; 4096];
            socket.read(&mut request).unwrap();
            socket.write_all(wire).unwrap();
        });
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(format!("http://{address}/"))
            .send()
            .await
            .unwrap();
        (response, worker)
    }
    #[tokio::test]
    async fn broken_body_retains_partial_evidence() {
        let (mut r, worker) =
            response(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{}")
                .await;
        let (body, truncated, error) = bounded_body(&mut r, 1024).await;
        assert_eq!(body, b"{}");
        assert!(truncated);
        assert!(error.is_some());
        worker.join().unwrap();
    }
    #[tokio::test]
    async fn byte_cap_retains_only_budget() {
        let (mut r, worker) =
            response(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n1234")
                .await;
        let (body, truncated, error) = bounded_body(&mut r, 2).await;
        assert_eq!(body, b"12");
        assert!(truncated);
        assert!(error.is_none());
        worker.join().unwrap();
    }
    #[tokio::test]
    async fn exact_budget_complete() {
        let (mut r, worker) =
            response(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await;
        let (body, truncated, error) = bounded_body(&mut r, 2).await;
        assert_eq!(body, b"{}");
        assert!(!truncated);
        assert!(error.is_none());
        worker.join().unwrap();
    }
}
