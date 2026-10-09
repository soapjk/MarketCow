//! Read-only PolyNode batch -> asynchronous official CLOB /books comparison.
//! A REST response is later than the WS frame: a mismatch is not proof of a bad feed.
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::{BTreeMap, BTreeSet}, env, fs::{self, File}, io::{BufWriter, Write}, os::unix::fs::PermissionsExt, path::PathBuf, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, task::JoinSet};
use tokio_tungstenite::{client_async_tls_with_config, connect_async, tungstenite::Message};

const KEY_FILE: &str = "/mnt/p44pro/marketcow-shadow-v3-runtime/linux/diagnostics/polynode-probe/api-key";
const MAX_IN_FLIGHT: usize = 16;
const MAX_ROWS: usize = 6000;
const MAX_REST_BODY: usize = 512 * 1024;
const MAX_OUTPUT_BYTES: usize = 512 * 1024 * 1024;

#[derive(Clone, Serialize)]
struct ChangedLevel { token: String, side: String, price: String, size: String }

#[derive(Serialize)]
struct LevelResult { token: String, side: String, price: String, polynode_size: String, rest_size: Option<String>, matches: bool }

#[derive(Serialize)]
struct Row {
    ordinal: u64,
    ws_received_at_ns: u128,
    polynode_batch_ts_ms: Option<u64>,
    polynode_raw_sha256: String,
    polynode_raw: String,
    tokens: Vec<String>,
    changed_levels: usize,
    rest_request_started_at_ns: Option<u128>,
    rest_response_received_at_ns: Option<u128>,
    rest_http_status: Option<u16>,
    rest_raw_sha256: Option<String>,
    rest_raw: Option<String>,
    rest_book_timestamps_ms: BTreeMap<String, Option<u64>>,
    levels: Vec<LevelResult>,
    outcome: String,
}

fn now_ns() -> u128 { SystemTime::now().duration_since(UNIX_EPOCH).expect("clock before epoch").as_nanos() }
fn sha(bytes: &[u8]) -> String { format!("{:x}", Sha256::digest(bytes)) }
fn norm(input: &str) -> String {
    let (whole, frac) = input.split_once('.').unwrap_or((input, ""));
    let whole = whole.trim_start_matches('0');
    let frac = frac.trim_end_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    if frac.is_empty() { whole.to_string() } else { format!("{whole}.{frac}") }
}
fn string_at<'a>(value: &'a Value, key: &str) -> Option<&'a str> { value.get(key)?.as_str() }
fn token_of(value: &Value) -> Option<String> { string_at(value, "asset_id").filter(|s| s.len() > 10 && s.bytes().all(|b| b.is_ascii_digit())).map(str::to_owned) }
fn add_level(levels: &mut BTreeMap<(String, String, String), ChangedLevel>, token: &str, side: &str, item: &Value) {
    if let (Some(price), Some(size)) = (string_at(item, "price"), string_at(item, "size")) {
        let entry = ChangedLevel { token: token.to_owned(), side: side.to_owned(), price: norm(price), size: norm(size) };
        levels.insert((entry.token.clone(), entry.side.clone(), entry.price.clone()), entry);
    }
}
fn collect_changes(value: &Value, known: &mut BTreeSet<String>) -> (BTreeSet<String>, Vec<ChangedLevel>) {
    let mut touched = BTreeSet::new();
    let mut levels = BTreeMap::new();
    let updates: Vec<&Value> = match value.get("type").and_then(Value::as_str) {
        Some("batch") => value.get("updates").and_then(Value::as_array).map(|a| a.iter().collect()).unwrap_or_default(),
        Some("snapshot_batch") => value.get("snapshots").and_then(Value::as_array).map(|a| a.iter().collect()).unwrap_or_default(),
        Some("book_snapshot") => vec![value],
        _ => Vec::new(),
    };
    for update in updates {
        match string_at(update, "type") {
            Some("price_change") => {
                if let Some(assets) = update.get("assets").and_then(Value::as_array) {
                    for asset in assets {
                        if let (Some(token), Some(side)) = (token_of(asset), string_at(asset, "side")) {
                            let side = match side { "BUY" => "bids", "SELL" => "asks", _ => continue };
                            known.insert(token.clone()); touched.insert(token.clone());
                            add_level(&mut levels, &token, side, asset);
                        }
                    }
                }
            }
            Some("book_update" | "book_snapshot") => {
                if let Some(token) = token_of(update) {
                    known.insert(token.clone()); touched.insert(token.clone());
                    for side in ["bids", "asks"] {
                        if let Some(items) = update.get(side).and_then(Value::as_array) {
                            for item in items { add_level(&mut levels, &token, side, item); }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    // Trade-only batches still generate a REST request for already discovered tokens.
    if touched.is_empty() { touched.extend(known.iter().cloned()); }
    (touched, levels.into_values().collect())
}
fn rest_level(book: &Value, side: &str, price: &str) -> Option<String> {
    book.get(side)?.as_array()?.iter().find_map(|row| {
        let p = string_at(row, "price")?;
        (norm(p) == price).then(|| string_at(row, "size").map(norm)).flatten()
    })
}
async fn fetch(client: reqwest::Client, mut row: Row, changes: Vec<ChangedLevel>) -> Row {
    row.rest_request_started_at_ns = Some(now_ns());
    let body: Vec<_> = row.tokens.iter().map(|t| json!({"token_id": t})).collect();
    let response = client.post("https://clob.polymarket.com/books").json(&body).send().await;
    let Ok(response) = response else { row.outcome = "rest_request_failed".into(); return row };
    row.rest_http_status = Some(response.status().as_u16());
    let bytes = match response.bytes().await { Ok(bytes) => bytes, Err(_) => { row.outcome = "rest_body_failed".into(); return row } };
    row.rest_response_received_at_ns = Some(now_ns());
    if bytes.len() > MAX_REST_BODY { row.outcome = "rest_body_limit".into(); return row; }
    row.rest_raw_sha256 = Some(sha(&bytes));
    row.rest_raw = Some(String::from_utf8_lossy(&bytes).into_owned());
    if row.rest_http_status != Some(200) { row.outcome = "rest_http_error".into(); return row; }
    let Ok(books) = serde_json::from_slice::<Vec<Value>>(&bytes) else { row.outcome = "rest_decode_failed".into(); return row };
    let indexed: BTreeMap<String, Value> = books.into_iter().filter_map(|book| token_of(&book).map(|t| (t, book))).collect();
    for token in &row.tokens {
        row.rest_book_timestamps_ms.insert(token.clone(), indexed.get(token).and_then(|book| book.get("timestamp")).and_then(|v| v.as_str().and_then(|s| s.parse().ok()).or_else(|| v.as_u64())));
    }
    for change in changes {
        let rest_size = indexed.get(&change.token).and_then(|book| rest_level(book, &change.side, &change.price));
        let matches = rest_size.as_deref().unwrap_or("0") == change.size;
        row.levels.push(LevelResult { token: change.token, side: change.side, price: change.price, polynode_size: change.size, rest_size, matches });
    }
    row.outcome = if indexed.len() < row.tokens.len() { "rest_missing_book".into() }
        else if row.levels.is_empty() { "no_depth_change".into() }
        else if row.levels.iter().all(|level| level.matches) { "all_changed_levels_match".into() }
        else { "changed_levels_differ".into() };
    row
}
async fn tunnel(proxy: &str, port: u16) -> Result<TcpStream, &'static str> {
    let mut stream = TcpStream::connect((proxy, port)).await.map_err(|_| "proxy_connect_failed")?;
    stream.write_all(b"CONNECT ob.polynode.dev:443 HTTP/1.1\r\nHost: ob.polynode.dev:443\r\n\r\n").await.map_err(|_| "proxy_write_failed")?;
    let mut header = Vec::new();
    loop {
        let mut one = [0]; stream.read_exact(&mut one).await.map_err(|_| "proxy_read_failed")?;
        header.push(one[0]);
        if header.ends_with(b"\r\n\r\n") { break; }
        if header.len() > 8192 { return Err("proxy_header_limit"); }
    }
    if !(header.starts_with(b"HTTP/1.1 200 ") || header.starts_with(b"HTTP/1.0 200 ")) { return Err("proxy_rejected"); }
    Ok(stream)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut output = None::<PathBuf>; let mut markets = Vec::new(); let mut duration = 300u64;
    let mut key_file = PathBuf::from(KEY_FILE); let mut proxy = "127.0.0.1".to_string(); let mut port = 17890u16;
    let mut ws_direct = false;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--ws-direct" { ws_direct = true; continue; }
        let value = args.next().ok_or("missing argument value")?;
        match arg.as_str() {
            "--output" => output = Some(value.into()), "--market" => markets.push(value),
            "--duration-seconds" => duration = value.parse()?, "--api-key-file" => key_file = value.into(),
            "--proxy-host" => proxy = value, "--proxy-port" => port = value.parse()?,
            _ => return Err("unknown argument".into()),
        }
    }
    if !(1..=600).contains(&duration) { return Err("duration must be 1..600 seconds".into()); }
    let output = output.ok_or("missing --output")?;
    if output.exists() { return Err("output already exists".into()); }
    let meta = fs::metadata(&key_file)?;
    if !meta.is_file() || meta.permissions().mode() & 0o077 != 0 || !(1..=512).contains(&meta.len()) { return Err("invalid key file metadata".into()); }
    let key = fs::read_to_string(key_file)?.trim().to_owned();
    if !(key.starts_with("pn_live_") || key.starts_with("qm_live_")) || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') { return Err("invalid key format".into()); }
    if markets.is_empty() {
        let start = (now_ns() / 1_000_000_000 / 300 * 300) as u64;
        markets = [start, start + 300].iter().map(|s| format!("btc-updown-5m-{s}")).collect();
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = reqwest::Client::builder().proxy(reqwest::Proxy::all(format!("http://{proxy}:{port}"))?).timeout(Duration::from_secs(10)).build()?;
    let url = format!("wss://ob.polynode.dev/ws?key={key}");
    // Do not format errors from this call: the authenticated URL may be embedded.
    let (mut ws, _) = if ws_direct {
        tokio::time::timeout(Duration::from_secs(10), connect_async(url)).await.map_err(|_| "ws_timeout")?.map_err(|_| "ws_connect_failed")?
    } else {
        let stream = tokio::time::timeout(Duration::from_secs(10), tunnel(&proxy, port)).await.map_err(|_| "proxy_timeout")??;
        tokio::time::timeout(Duration::from_secs(10), client_async_tls_with_config(url, stream, None, None)).await.map_err(|_| "ws_timeout")?.map_err(|_| "ws_connect_failed")?
    };
    ws.send(Message::Text(json!({"action":"subscribe","markets":markets}).to_string().into())).await.map_err(|_| "subscribe_failed")?;
    let started = now_ns(); let deadline = Instant::now() + Duration::from_secs(duration);
    let mut tasks = JoinSet::<Row>::new(); let mut rows = Vec::<Row>::new(); let mut known = BTreeSet::<String>::new();
    let mut ordinal = 0u64; let mut controls = 0u64; let mut skipped_capacity = 0u64; let mut skipped_no_tokens = 0u64;
    let mut ws_end = "duration_complete"; let mut output_bytes = 0usize;
    loop {
        tokio::select! {
            biased;
            frame = ws.next() => match frame {
                Some(Ok(Message::Text(raw))) => {
                    let received = now_ns();
                    let Ok(value) = serde_json::from_str::<Value>(&raw) else { controls += 1; continue; };
                    if !matches!(string_at(&value,"type"), Some("batch" | "snapshot_batch" | "book_snapshot")) { controls += 1; continue; }
                    ordinal += 1;
                    let (touched, changes) = collect_changes(&value, &mut known);
                    let mut row = Row { ordinal, ws_received_at_ns: received, polynode_batch_ts_ms: value.get("ts").and_then(Value::as_u64), polynode_raw_sha256: sha(raw.as_bytes()), polynode_raw: raw.to_string(), tokens: touched.into_iter().collect(), changed_levels: changes.len(), rest_request_started_at_ns: None, rest_response_received_at_ns: None, rest_http_status: None, rest_raw_sha256: None, rest_raw: None, rest_book_timestamps_ms: BTreeMap::new(), levels: Vec::new(), outcome: String::new() };
                    if raw.contains(&key) { ws_end = "secret_echo"; break; }
                    if row.tokens.is_empty() { row.outcome = "no_known_tokens".into(); skipped_no_tokens += 1; rows.push(row); }
                    else if tasks.len() >= MAX_IN_FLIGHT { row.outcome = "skipped_in_flight_limit".into(); skipped_capacity += 1; rows.push(row); }
                    else { tasks.spawn(fetch(client.clone(), row, changes)); }
                    if rows.len() + tasks.len() >= MAX_ROWS { ws_end = "row_limit"; break; }
                }
                Some(Ok(Message::Close(_))) => { ws_end = "server_closed"; break; }
                Some(Ok(_)) => { controls += 1; }
                Some(Err(_)) => { ws_end = "ws_read_failed"; break; }
                None => { ws_end = "ws_ended"; break; }
            },
            Some(result) = tasks.join_next(), if !tasks.is_empty() => { if let Ok(row) = result { rows.push(row); } },
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => break,
        }
    }
    let ended = now_ns();
    let _ = tokio::time::timeout(Duration::from_secs(1), ws.close(None)).await;
    let drain = Instant::now() + Duration::from_secs(12);
    while !tasks.is_empty() && Instant::now() < drain {
        if let Ok(Some(Ok(row))) = tokio::time::timeout(Duration::from_secs(1), tasks.join_next()).await { rows.push(row); }
    }
    let unfinished = tasks.len(); tasks.abort_all();
    rows.sort_by_key(|row| row.ordinal);
    let mut writer = BufWriter::new(File::options().create_new(true).write(true).open(&output)?);
    fs::set_permissions(&output, fs::Permissions::from_mode(0o600))?;
    for row in &rows {
        let line = serde_json::to_vec(row)?;
        output_bytes += line.len() + 1;
        if output_bytes > MAX_OUTPUT_BYTES { return Err("output byte limit exceeded".into()); }
        writer.write_all(&line)?; writer.write_all(b"\n")?;
    }
    writer.flush()?;
    println!("{}", json!({"markets":markets,"started_at_ns":started,"ended_at_ns":ended,"duration_seconds":(ended-started) as f64 / 1e9,"ws_end":ws_end,"batch_frames":ordinal,"recorded_rows":rows.len(),"skipped_in_flight":skipped_capacity,"skipped_no_tokens":skipped_no_tokens,"unfinished_rest":unfinished,"controls":controls,"output_bytes":output_bytes,"output":output}));
    Ok(())
}
