//! Catalog-only keyset capture with verified page-boundary resume.
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
    /// Reuse verified complete pages in this root; retry the incomplete page in full.
    #[arg(long)]
    resume: bool,
    /// Additional attempts per page (not unlimited process restarts).
    #[arg(long, default_value_t = 3)]
    maximum_retries: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_seconds: u64,
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

fn retryable(status: u16, body_error: bool) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504) || (status == 200 && body_error)
}

async fn fetch_page(
    a: &Args,
    client: &reqwest::Client,
    url: &str,
    query: &[(&str, String)],
    page: usize,
    deadline: Instant,
    remaining_bytes: usize,
    retry_bytes: &mut usize,
) -> Result<(u16, Vec<u8>, bool, Option<String>)> {
    for attempt in 0..=a.maximum_retries {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .context("capture deadline before request")?;
        let available = remaining_bytes
            .checked_sub(*retry_bytes)
            .filter(|n| *n > 0)
            .context("retry byte budget")?;
        let response = client
            .get(url)
            .query(query)
            .timeout(remaining.min(Duration::from_secs(a.request_seconds)))
            .send()
            .await;
        let (status, body, truncated, error, can_retry, retry_after) = match response {
            Ok(mut r) => {
                let status = r.status().as_u16();
                let retry_after = r
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| {
                        v.parse::<u64>().ok().or_else(|| {
                            chrono::DateTime::parse_from_rfc2822(v).ok().map(|t| {
                                (t.with_timezone(&chrono::Utc) - chrono::Utc::now())
                                    .num_seconds()
                                    .max(0) as u64
                            })
                        })
                    })
                    .unwrap_or(0);
                let (body, truncated, error) =
                    bounded_body(&mut r, a.maximum_page_bytes.min(available)).await;
                let can_retry =
                    retryable(status, error.is_some()) && !(truncated && error.is_none());
                (status, body, truncated, error, can_retry, retry_after)
            }
            Err(e) => {
                let can_retry = e.is_timeout() || e.is_connect() || e.is_body() || e.is_request();
                (0, vec![], true, Some(format!("{e:?}")), can_retry, 0)
            }
        };
        if status == 200 && !truncated {
            return Ok((status, body, truncated, error));
        }
        if !can_retry || attempt == a.maximum_retries {
            return Ok((status, body, truncated, error));
        }
        let name = format!(
            "retry-{page:06}-{}-{attempt}.json",
            chrono::Utc::now().timestamp_nanos_opt().context("clock")?
        );
        let mut raw = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(a.root.join(&name))?;
        raw.write_all(&body)?;
        raw.sync_all()?;
        *retry_bytes += body.len();
        let delay = a
            .retry_delay_seconds
            .saturating_mul(1u64 << attempt)
            .max(retry_after);
        let record = json!({"page":page,"attempt":attempt+1,"file":name,"status":status,"raw_bytes":body.len(),"raw_sha256":hex::encode(Sha256::digest(&body)),"error":error,"received_at":chrono::Utc::now(),"delay_seconds":delay});
        let mut log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(a.root.join("retries.jsonl"))?;
        writeln!(log, "{record}")?;
        log.sync_all()?;
        eprintln!("catalog_retry {record}");
        ensure!(
            deadline.saturating_duration_since(Instant::now()) > Duration::from_secs(delay),
            "capture deadline before retry wait"
        );
        tokio::time::sleep(Duration::from_secs(delay)).await;
    }
    unreachable!()
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
            Err(error) => return (body, true, Some(format!("{error:?}"))),
        }
    }
}

// The ledger, not a possibly half-updated identity database, is the checkpoint.
// A failing attempt is archived before replacing the active ledger. Repeating
// recovery after a crash is safe; no incomplete body is ever appended to.
fn recover(
    a: &Args,
    ids: &[String],
    db: &rusqlite::Connection,
) -> Result<(usize, usize, usize, Option<String>, bool)> {
    use std::io::{BufRead, Read};
    let ledger = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(a.root.join("pages.jsonl"))?;
    let mut reader = std::io::BufReader::new(ledger);
    let mut valid = Vec::new();
    let (mut pages, mut bytes, mut count) = (0usize, 0usize, 0usize);
    let mut cursor: Option<String> = None;
    let mut terminal = false;
    loop {
        let mut line = Vec::new();
        reader.by_ref().take(65537).read_until(b'\n', &mut line)?;
        if line.is_empty() {
            break;
        }
        ensure!(line.len() <= 65536, "ledger row budget");
        // Only a torn final ledger write may be discarded.
        if !line.ends_with(b"\n") {
            break;
        }
        let p: serde_json::Value = serde_json::from_slice(&line)?;
        ensure!(
            !terminal && pages < a.maximum_pages,
            "page after terminal/budget"
        );
        let name = format!("page-{:06}.json", pages + 1);
        ensure!(
            p["page"].as_u64() == Some((pages + 1) as u64) && p["file"] == name,
            "page sequence/path"
        );
        let mut params = vec![
            ("limit", "100".to_owned()),
            ("closed", a.closed.to_string()),
            ("order", "id".to_owned()),
            ("ascending", "true".to_owned()),
        ];
        if let Some(c) = &cursor {
            params.push(("after_cursor", c.clone()));
        }
        let url = if ids.is_empty() {
            "https://gamma-api.polymarket.com/markets/keyset".to_owned()
        } else {
            ensure!(pages < ids.len(), "identity page overflow");
            params.clear();
            format!("https://gamma-api.polymarket.com/markets/{}", ids[pages])
        };
        ensure!(
            p["url"] == url && p["params"] == json!(params),
            "resume request mismatch"
        );
        if p["status"] != 200 || p["truncated"] != false || !p["body_error"].is_null() {
            let mut rest = Vec::new();
            reader.take(1).read_to_end(&mut rest)?;
            ensure!(rest.is_empty(), "records after failed attempt");
            break;
        }
        let file = a.root.join(&name);
        ensure!(
            file.metadata()?.len() <= a.maximum_page_bytes as u64,
            "saved page budget"
        );
        let body = std::fs::read(file)?;
        ensure!(
            p["raw_bytes"].as_u64() == Some(body.len() as u64)
                && p["raw_sha256"] == hex::encode(Sha256::digest(&body)),
            "saved page hash/length mismatch"
        );
        bytes = bytes.checked_add(body.len()).context("byte overflow")?;
        ensure!(bytes <= a.maximum_bytes, "saved total byte budget");
        let v: serde_json::Value = serde_json::from_slice(&body)?;
        let singleton = vec![v.clone()];
        let markets = if ids.is_empty() {
            v["markets"].as_array().context("saved markets")?
        } else {
            ensure!(
                v["id"].as_str() == Some(ids[pages].as_str()),
                "saved identity mismatch"
            );
            &singleton
        };
        ensure!(markets.len() <= 100, "saved page size");
        for market in markets {
            let id = market["id"]
                .as_str()
                .filter(|s| !s.is_empty())
                .context("saved market id")?;
            db.execute("INSERT INTO ids VALUES(?)", [id])?;
            count += 1;
        }
        cursor = if ids.is_empty() {
            match v.get("next_cursor") {
                None | Some(serde_json::Value::Null) => None,
                Some(c) => Some(
                    c.as_str()
                        .filter(|s| !s.is_empty())
                        .context("saved cursor")?
                        .to_owned(),
                ),
            }
        } else {
            None
        };
        if let Some(c) = &cursor {
            ensure!(!markets.is_empty(), "saved empty nonterminal");
            db.execute("INSERT INTO cursors VALUES(?)", [c])?;
        }
        pages += 1;
        terminal = if ids.is_empty() {
            cursor.is_none()
        } else {
            pages == ids.len()
        };
        valid.extend_from_slice(&line);
    }
    let archive = a.root.join(format!(
        "resume-attempt-{}",
        chrono::Utc::now().timestamp_nanos_opt().context("clock")?
    ));
    std::fs::create_dir(&archive)?;
    std::fs::copy(a.root.join("pages.jsonl"), archive.join("pages.jsonl"))?;
    if a.root.join("report.json").exists() {
        std::fs::copy(a.root.join("report.json"), archive.join("report.json"))?;
    }
    let failed = format!("page-{:06}.json", pages + 1);
    if a.root.join(&failed).exists() {
        std::fs::copy(a.root.join(&failed), archive.join(&failed))?;
    }
    for entry in std::fs::read_dir(&archive)? {
        std::fs::File::open(entry?.path())?.sync_all()?;
    }
    std::fs::File::open(&archive)?.sync_all()?;
    let temporary = a.root.join("pages.resume.tmp");
    let mut out = std::fs::File::create(&temporary)?;
    out.write_all(&valid)?;
    out.sync_all()?;
    std::fs::rename(temporary, a.root.join("pages.jsonl"))?;
    std::fs::File::open(&a.root)?.sync_all()?;
    Ok((pages, bytes, count, cursor, terminal))
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
    ensure!(
        a.maximum_retries <= 10 && a.retry_delay_seconds > 0 && a.retry_delay_seconds <= 60,
        "bounded retry configuration"
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
    if !a.resume {
        std::fs::create_dir(&a.root)?;
    }
    // SQLite's OS-backed writer lock is released on process death.
    let owner = rusqlite::Connection::open(a.root.join("capture-owner.sqlite"))?;
    owner.busy_timeout(Duration::ZERO)?;
    owner.execute_batch("BEGIN IMMEDIATE")?;
    let metadata_path = a.root.join("capture.json");
    let metadata = if a.resume {
        let path = if metadata_path.exists() {
            metadata_path.clone()
        } else {
            a.root.join("report.json")
        };
        ensure!(path.metadata()?.len() <= 65536, "capture metadata budget");
        let value: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
        ensure!(
            value["closed_filter"] == a.closed && value["requested_market_ids"] == json!(ids),
            "resume configuration mismatch"
        );
        ensure!(
            value["capture_started_at"].as_str().is_some(),
            "missing original capture time"
        );
        let original =
            chrono::DateTime::parse_from_rfc3339(value["capture_started_at"].as_str().unwrap())?;
        ensure!(original <= chrono::Utc::now(), "capture clock regression");
        value
    } else {
        json!({"capture_started_at":chrono::Utc::now(), "closed_filter":a.closed,"requested_market_ids":ids})
    };
    let started = metadata["capture_started_at"].clone();
    let mut metadata_file = std::fs::File::create(a.root.join("capture.tmp"))?;
    metadata_file.write_all(&serde_json::to_vec(&metadata)?)?;
    metadata_file.sync_all()?;
    std::fs::rename(a.root.join("capture.tmp"), &metadata_path)?;
    let attempt_started = chrono::Utc::now();
    let deadline = Instant::now() + Duration::from_secs(a.maximum_seconds);
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    let db = rusqlite::Connection::open(a.root.join("identity-check.sqlite"))?;
    db.execute_batch("PRAGMA cache_size=-8192; BEGIN; CREATE TABLE IF NOT EXISTS ids(id TEXT PRIMARY KEY); CREATE TABLE IF NOT EXISTS cursors(id TEXT PRIMARY KEY); DELETE FROM ids; DELETE FROM cursors;")?;
    let (mut pages, mut bytes, mut count, mut cursor, terminal) = if a.resume {
        recover(&a, &ids, &db)?
    } else {
        (0, 0, 0, None, false)
    };
    db.execute_batch("COMMIT")?;
    let resumed_pages = pages;
    let mut retry_bytes = 0usize;
    let mut log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(a.root.join("pages.jsonl"))?;
    let result:Result<()> = async {
        if terminal { return Ok(()); }
        loop {
            ensure!(pages<a.maximum_pages && bytes<a.maximum_bytes,"capture budget before request");
            let mut query=vec![("limit","100".to_owned()),("closed",a.closed.to_string()),("order","id".to_owned()),("ascending","true".to_owned())];
            if let Some(c)=&cursor {query.push(("after_cursor",c.clone()));}
            let url=if ids.is_empty(){"https://gamma-api.polymarket.com/markets/keyset".to_owned()}else{query.clear();format!("https://gamma-api.polymarket.com/markets/{}",ids[pages])};
            let (status,body,truncated,body_error)=fetch_page(&a,&client,&url,&query,pages+1,deadline,a.maximum_bytes-bytes,&mut retry_bytes).await?;
            bytes+=body.len();
            pages+=1;let name=format!("page-{pages:06}.json");
            let mut page_file=std::fs::File::create(a.root.join(&name))?;
            page_file.write_all(&body)?;page_file.sync_all()?;
            writeln!(log,"{}",json!({"page":pages,"file":name,"url":url,"params":query,"received_at":chrono::Utc::now(),"status":status,"raw_bytes":body.len(),"raw_sha256":hex::encode(Sha256::digest(&body)),"truncated":truncated,"body_error":body_error}))?;
            log.sync_all()?;
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
        "resumed_verified_pages":resumed_pages,"attempt_started_at":attempt_started,
        "retry_retained_raw_bytes":retry_bytes,"maximum_retries":a.maximum_retries,"retry_delay_seconds":a.retry_delay_seconds,
        "budgets":{"maximum_pages":a.maximum_pages,"maximum_bytes":a.maximum_bytes,
            "maximum_page_bytes":a.maximum_page_bytes,"maximum_seconds":a.maximum_seconds,
            "request_seconds":a.request_seconds,"interval_millis":a.interval_millis},
        "source_atomic_snapshot":false,"transport_chunk_can_exceed_retained_cap":true,
        "error":result.as_ref().err().map(|e|format!("{e:#}"))});
    let mut report_file = std::fs::File::create(a.root.join("report.tmp"))?;
    report_file.write_all(&serde_json::to_vec_pretty(&report)?)?;
    report_file.sync_all()?;
    std::fs::rename(a.root.join("report.tmp"), a.root.join("report.json"))?;
    log.sync_all()?;
    println!("{report}");
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    fn retry_server(wires: Vec<&'static [u8]>) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/markets", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let worker = std::thread::spawn(move || {
            let mut requests = Vec::new();
            let end = Instant::now() + Duration::from_secs(3);
            for wire in wires {
                loop {
                    match listener.accept() {
                        Ok((mut socket, _)) => {
                            socket.set_nonblocking(false).unwrap();
                            socket
                                .set_read_timeout(Some(Duration::from_secs(1)))
                                .unwrap();
                            let mut buffer = [0; 4096];
                            let n = socket.read(&mut buffer).unwrap();
                            requests.push(
                                String::from_utf8_lossy(&buffer[..n])
                                    .lines()
                                    .next()
                                    .unwrap()
                                    .to_owned(),
                            );
                            socket.write_all(wire).unwrap();
                            break;
                        }
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < end =>
                        {
                            std::thread::sleep(Duration::from_millis(1))
                        }
                        _ => return requests,
                    }
                }
            }
            requests
        });
        (url, worker)
    }
    #[tokio::test]
    async fn transient_retry_preserves_cursor_and_evidence() {
        let (mut a, _) = fixture();
        a.retry_delay_seconds = 0;
        let (url, server) = retry_server(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 99\r\nConnection: close\r\n\r\nx",
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
        ]);
        let mut bytes = 0;
        let r = fetch_page(
            &a,
            &reqwest::Client::builder().no_proxy().build().unwrap(),
            &url,
            &[("after_cursor", "frozen".into())],
            1,
            Instant::now() + Duration::from_secs(5),
            100,
            &mut bytes,
        )
        .await
        .unwrap();
        assert_eq!(r.1, b"{}");
        assert_eq!(bytes, 1);
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
        assert_eq!(
            std::fs::read_to_string(a.root.join("retries.jsonl"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        std::fs::remove_dir_all(a.root).unwrap();
    }
    #[tokio::test]
    async fn retry_limit_and_auth_failure_stop() {
        for fatal in [false, true] {
            let (mut a, _) = fixture();
            a.maximum_retries = 2;
            a.retry_delay_seconds = 0;
            let wire: &'static [u8] = if fatal {
                b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            } else {
                b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            };
            let (url, server) = retry_server(vec![wire; if fatal { 1 } else { 3 }]);
            let r = fetch_page(
                &a,
                &reqwest::Client::builder().no_proxy().build().unwrap(),
                &url,
                &[],
                1,
                Instant::now() + Duration::from_secs(5),
                100,
                &mut 0,
            )
            .await
            .unwrap();
            assert_eq!(r.0, if fatal { 403 } else { 503 });
            assert_eq!(server.join().unwrap().len(), if fatal { 1 } else { 3 });
            std::fs::remove_dir_all(a.root).unwrap();
        }
    }
    #[tokio::test]
    async fn retry_deadline_and_byte_budget_prevent_next_request() {
        let (mut a, _) = fixture();
        a.retry_delay_seconds = 0;
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let error = fetch_page(
            &a,
            &client,
            "http://127.0.0.1:1/",
            &[],
            1,
            Instant::now(),
            100,
            &mut 0,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("deadline before request"));
        let (url, server) = retry_server(vec![
            b"HTTP/1.1 503 Unavailable\r\nContent-Length: 2\r\nConnection: close\r\n\r\nxx",
        ]);
        let error = fetch_page(
            &a,
            &client,
            &url,
            &[],
            1,
            Instant::now() + Duration::from_secs(5),
            2,
            &mut 0,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("retry byte budget"));
        assert_eq!(server.join().unwrap().len(), 1);
        std::fs::remove_dir_all(a.root).unwrap();
    }
    #[test]
    fn retry_status_classification() {
        for status in [408, 429, 500, 502, 503, 504] {
            assert!(retryable(status, false));
        }
        for status in [301, 400, 401, 403, 404, 410, 422] {
            assert!(!retryable(status, true));
        }
        assert!(retryable(200, true));
        assert!(!retryable(200, false));
    }
    #[tokio::test]
    async fn retry_after_does_not_extend_deadline_and_oversize_does_not_retry() {
        for oversize in [false, true] {
            let (mut a, _) = fixture();
            a.retry_delay_seconds = 0;
            a.maximum_page_bytes = 1;
            let wire: &'static [u8] = if oversize {
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nxx"
            } else {
                b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 60\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            };
            let (url, server) = retry_server(vec![wire]);
            let result = fetch_page(
                &a,
                &reqwest::Client::builder().no_proxy().build().unwrap(),
                &url,
                &[],
                1,
                Instant::now() + Duration::from_secs(2),
                100,
                &mut 0,
            )
            .await;
            if oversize {
                let r = result.unwrap();
                assert!(r.2);
                assert!(r.3.is_none());
            } else {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("deadline before retry wait")
                );
            }
            assert_eq!(server.join().unwrap().len(), 1);
            std::fs::remove_dir_all(a.root).unwrap();
        }
    }
    fn fixture() -> (Args, rusqlite::Connection) {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "catalog-resume-{}-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let a = Args {
            maximum_retries: 3,
            retry_delay_seconds: 2,
            root,
            resume: true,
            maximum_pages: 10,
            maximum_bytes: 100000,
            maximum_page_bytes: 10000,
            maximum_seconds: 10,
            request_seconds: 1,
            interval_millis: 0,
            closed: false,
            market_ids: None,
        };
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE ids(id TEXT PRIMARY KEY); CREATE TABLE cursors(id TEXT PRIMARY KEY);",
        )
        .unwrap();
        (a, db)
    }
    fn page(
        a: &Args,
        n: usize,
        previous: Option<&str>,
        id: &str,
        next: Option<&str>,
        broken: bool,
    ) {
        let body = if broken {
            b"partial".to_vec()
        } else {
            serde_json::to_vec(&json!({"markets":[{"id":id}],"next_cursor":next})).unwrap()
        };
        let name = format!("page-{n:06}.json");
        std::fs::write(a.root.join(&name), &body).unwrap();
        let mut params = vec![
            json!(["limit", "100"]),
            json!(["closed", "false"]),
            json!(["order", "id"]),
            json!(["ascending", "true"]),
        ];
        if let Some(c) = previous {
            params.push(json!(["after_cursor", c]));
        }
        let mut log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(a.root.join("pages.jsonl"))
            .unwrap();
        writeln!(log,"{}",json!({"page":n,"file":name,"url":"https://gamma-api.polymarket.com/markets/keyset","params":params,"status":200,"raw_bytes":body.len(),"raw_sha256":hex::encode(Sha256::digest(&body)),"truncated":broken,"body_error":null})).unwrap();
    }
    #[test]
    fn resume_failed_page_then_complete_without_duplicate_prefix() {
        let (a, db) = fixture();
        page(&a, 1, None, "1", Some("c1"), false);
        page(&a, 2, Some("c1"), "2", None, true);
        let r = recover(&a, &[], &db).unwrap();
        assert_eq!((r.0, r.2, r.3, r.4), (1, 1, Some("c1".into()), false));
        assert!(std::fs::read_dir(&a.root).unwrap().any(|p| {
            p.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("resume-attempt-")
        }));
        page(&a, 2, Some("c1"), "2", None, false);
        db.execute_batch("DELETE FROM ids; DELETE FROM cursors")
            .unwrap();
        let r = recover(&a, &[], &db).unwrap();
        assert_eq!((r.0, r.2, r.3, r.4), (2, 2, None, true));
        std::fs::remove_dir_all(&a.root).unwrap();
    }
    #[test]
    fn resume_rejects_tampering_without_rewriting_ledger() {
        let (a, db) = fixture();
        page(&a, 1, None, "1", Some("c1"), false);
        let before = std::fs::read(a.root.join("pages.jsonl")).unwrap();
        std::fs::write(a.root.join("page-000001.json"), b"{}").unwrap();
        assert!(recover(&a, &[], &db).is_err());
        assert_eq!(before, std::fs::read(a.root.join("pages.jsonl")).unwrap());
        std::fs::remove_dir_all(&a.root).unwrap();
    }
    #[test]
    fn resume_rejects_wrong_cursor_duplicates_and_filter() {
        for case in 0..3 {
            let (mut a, db) = fixture();
            page(&a, 1, None, "1", Some("c1"), false);
            page(
                &a,
                2,
                Some(if case == 0 { "wrong" } else { "c1" }),
                if case == 1 { "1" } else { "2" },
                None,
                false,
            );
            if case == 2 {
                a.closed = true;
            }
            assert!(recover(&a, &[], &db).is_err());
            std::fs::remove_dir_all(&a.root).unwrap();
        }
    }
    #[test]
    fn resume_recovers_torn_ledger_and_uncommitted_page() {
        let (a, db) = fixture();
        page(&a, 1, None, "1", Some("c1"), false);
        std::fs::OpenOptions::new()
            .append(true)
            .open(a.root.join("pages.jsonl"))
            .unwrap()
            .write_all(b"{\"page\":2")
            .unwrap();
        std::fs::write(a.root.join("page-000002.json"), b"partial").unwrap();
        assert_eq!(recover(&a, &[], &db).unwrap().0, 1);
        std::fs::remove_dir_all(&a.root).unwrap();
    }
    #[test]
    fn resume_enforces_saved_byte_budget_and_terminal_boundary() {
        for terminal in [false, true] {
            let (mut a, db) = fixture();
            page(&a, 1, None, "1", None, false);
            if terminal {
                page(&a, 2, None, "2", None, false);
            } else {
                a.maximum_bytes = 1;
            }
            assert!(recover(&a, &[], &db).is_err());
            std::fs::remove_dir_all(&a.root).unwrap();
        }
    }
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
