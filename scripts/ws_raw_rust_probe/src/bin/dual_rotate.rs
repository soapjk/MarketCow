//! Isolated, read-only experiment: overlap two upstream sockets and switch only
//! after the replacement has full books for both subscribed assets.
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::HashSet, env, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::mpsc};
use tokio_tungstenite::{client_async_tls_with_config, tungstenite::Message};

const HOST: &str = "ws-subscriptions-clob.polymarket.com";

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

async fn tunnel(host: &str, port: u16) -> Result<TcpStream, String> {
    let mut stream = TcpStream::connect((host, port)).await.map_err(|e| e.to_string())?;
    stream.set_nodelay(true).map_err(|e| e.to_string())?;
    stream.write_all(format!("CONNECT {HOST}:443 HTTP/1.1\r\nHost: {HOST}:443\r\n\r\n").as_bytes()).await.map_err(|e| e.to_string())?;
    let mut header = Vec::with_capacity(256);
    loop {
        let mut byte = [0];
        stream.read_exact(&mut byte).await.map_err(|e| e.to_string())?;
        header.push(byte[0]);
        if header.ends_with(b"\r\n\r\n") { break; }
        if header.len() >= 8192 { return Err("CONNECT header too large".into()); }
    }
    if !header.starts_with(b"HTTP/1.1 200 ") && !header.starts_with(b"HTTP/1.0 200 ") {
        return Err(format!("CONNECT rejected: {}", String::from_utf8_lossy(&header[..header.len().min(80)])));
    }
    Ok(stream)
}

enum Report {
    Connected(&'static str, i64),
    Seeded(&'static str, i64),
    Window(&'static str, i64, usize, usize, i64, i64, i64),
    Closed(&'static str, String),
}

fn report_frame(value: &Value, received_ms: i64, assets: &HashSet<String>, seeded: &mut HashSet<String>, ages: &mut Vec<i64>) {
    let events: Vec<&Value> = match value {
        Value::Array(items) => items.iter().collect(),
        Value::Object(_) => vec![value],
        _ => return,
    };
    for event in events {
        if let Some(asset) = event.get("asset_id").and_then(Value::as_str) {
            if !assets.contains(asset) { continue; }
            if event.get("event_type").and_then(Value::as_str) == Some("book") {
                seeded.insert(asset.to_string());
            }
        }
        if let Some(ts) = event.get("timestamp").and_then(Value::as_str).and_then(|s| s.parse::<i64>().ok()) {
            if (1_000_000_000_000..10_000_000_000_000).contains(&ts) {
                ages.push(received_ms - ts);
            }
        }
    }
}

async fn socket_task(name: &'static str, proxy_host: String, proxy_port: u16, assets: Vec<String>, tx: mpsc::UnboundedSender<Report>) {
    let result: Result<(), String> = async {
        let tcp = tokio::time::timeout(Duration::from_secs(10), tunnel(&proxy_host, proxy_port)).await.map_err(|_| "CONNECT timeout".to_string())??;
        let (mut ws, _) = tokio::time::timeout(Duration::from_secs(10), client_async_tls_with_config(format!("wss://{HOST}/ws/market"), tcp, None, None)).await.map_err(|_| "WS handshake timeout".to_string())?.map_err(|e| e.to_string())?;
        ws.send(Message::Text(json!({"assets_ids": assets, "type": "market"}).to_string().into())).await.map_err(|e| e.to_string())?;
        let _ = tx.send(Report::Connected(name, now_ms()));
        let wanted: HashSet<String> = assets.into_iter().collect();
        let mut seeded = HashSet::new();
        let mut emitted_seed = false;
        let mut ping = tokio::time::interval(Duration::from_secs(10));
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ping.tick().await;
        tick.tick().await;
        let mut ages = Vec::new();
        let mut frames = 0_usize;
        loop {
            tokio::select! {
                next = ws.next() => {
                    let raw = match next {
                        Some(Ok(Message::Text(s))) => s.to_string(),
                        Some(Ok(Message::Binary(b))) => String::from_utf8(b.to_vec()).map_err(|e| e.to_string())?,
                        Some(Ok(Message::Close(c))) => return Err(format!("close {c:?}")),
                        Some(Ok(_)) => continue,
                        Some(Err(e)) => return Err(format!("receive {e}")),
                        None => return Err("stream ended".into()),
                    };
                    let received_ms = now_ms();
                    frames += 1;
                    if let Ok(value) = serde_json::from_str::<Value>(&raw) {
                        report_frame(&value, received_ms, &wanted, &mut seeded, &mut ages);
                        if !emitted_seed && seeded.len() == wanted.len() {
                            emitted_seed = true;
                            let _ = tx.send(Report::Seeded(name, received_ms));
                        }
                    }
                }
                _ = ping.tick() => ws.send(Message::Text("PING".into())).await.map_err(|e| e.to_string())?,
                _ = tick.tick() => {
                    ages.sort_unstable();
                    let n = ages.len();
                    let (p50, p95, max) = if n == 0 { (0, 0, 0) } else { (ages[n / 2], ages[(n * 95 / 100).min(n - 1)], ages[n - 1]) };
                    let _ = tx.send(Report::Window(name, now_ms(), frames, n, p50, p95, max));
                    frames = 0;
                    ages.clear();
                }
            }
        }
    }.await;
    let _ = tx.send(Report::Closed(name, result.err().unwrap_or_else(|| "ended".into())));
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut assets = Vec::new();
    let mut proxy_host = "127.0.0.1".to_string();
    let mut proxy_port = 17890_u16;
    let mut overlap_after = 20_u64;
    let mut duration = 80_u64;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args.next().ok_or("missing argument value")?;
        match arg.as_str() {
            "--asset-id" => assets.push(value),
            "--proxy-host" => proxy_host = value,
            "--proxy-port" => proxy_port = value.parse()?,
            "--overlap-after-seconds" => overlap_after = value.parse()?,
            "--duration-seconds" => duration = value.parse()?,
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    if assets.len() != 2 || assets[0] == assets[1] || overlap_after == 0 || duration < overlap_after + 10 || duration > 300 {
        return Err("require two distinct assets, overlap>0, and overlap+10<=duration<=300".into());
    }
    let started = Instant::now();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let a = tokio::spawn(socket_task("A", proxy_host.clone(), proxy_port, assets.clone(), tx.clone()));
    let mut b = None;
    let mut switched = false;
    let mut a_seeded = false;
    println!("{}", json!({"kind":"start","at_ms":now_ms(),"overlap_after_seconds":overlap_after,"duration_seconds":duration,"assets":assets}));
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(started + Duration::from_secs(duration))) => break,
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(started + Duration::from_secs(overlap_after))), if b.is_none() => {
                b = Some(tokio::spawn(socket_task("B", proxy_host.clone(), proxy_port, assets.clone(), tx.clone())));
                println!("{}", json!({"kind":"overlap_start","at_ms":now_ms(),"elapsed_ms":started.elapsed().as_millis()}));
            }
            report = rx.recv() => match report {
                Some(Report::Connected(name, at_ms)) => println!("{}", json!({"kind":"connected","connection":name,"at_ms":at_ms,"elapsed_ms":started.elapsed().as_millis()})),
                Some(Report::Seeded(name, at_ms)) => {
                    println!("{}", json!({"kind":"both_books_seeded","connection":name,"at_ms":at_ms,"elapsed_ms":started.elapsed().as_millis()}));
                    if name == "A" { a_seeded = true; }
                    if name == "B" && a_seeded && !switched {
                        switched = true;
                        a.abort();
                        println!("{}", json!({"kind":"cutover","at_ms":now_ms(),"elapsed_ms":started.elapsed().as_millis(),"old":"A","new":"B","note":"read-only probe; no book state or downstream publication"}));
                    }
                }
                Some(Report::Window(name, at_ms, frames, events, p50, p95, max)) => println!("{}", json!({"kind":"window","connection":name,"at_ms":at_ms,"elapsed_ms":started.elapsed().as_millis(),"frames":frames,"timestamped_events":events,"age_p50_ms":p50,"age_p95_ms":p95,"age_max_ms":max})),
                Some(Report::Closed(name, detail)) => println!("{}", json!({"kind":"closed","connection":name,"at_ms":now_ms(),"detail":detail})),
                None => break,
            },
        }
    }
    a.abort();
    if let Some(task) = b { task.abort(); }
    println!("{}", json!({"kind":"end","at_ms":now_ms(),"elapsed_ms":started.elapsed().as_millis(),"switched":switched}));
    Ok(())
}
