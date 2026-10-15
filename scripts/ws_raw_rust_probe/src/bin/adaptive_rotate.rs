//! Read-only A/B upstream WebSocket rotation probe. It never publishes a book.
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::HashSet, env, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{client_async_tls_with_config, tungstenite::Message};

const HOST: &str = "ws-subscriptions-clob.polymarket.com";
const PREPARE_MS: i64 = 1200;
const SWITCH_MS: i64 = 1500;
const SWITCH_DWELL_MS: u64 = 500;

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

enum Event {
    Connected,
    Seeded { age_ms: Option<i64> },
    AgeSample { age_ms: i64 },
    Window { frames: usize, events: usize, p50_ms: Option<i64>, p95_ms: Option<i64>, max_ms: Option<i64>, last_ms: Option<i64> },
    Closed(String),
}

struct Report { slot: usize, generation: u64, at_ms: i64, event: Event }

fn emit(tx: &mpsc::UnboundedSender<Report>, slot: usize, generation: u64, event: Event) {
    let _ = tx.send(Report { slot, generation, at_ms: now_ms(), event });
}

fn inspect_frame(raw: &str, at_ms: i64, wanted: &HashSet<String>, books: &mut HashSet<String>, ages: &mut Vec<i64>) -> Option<i64> {
    let value: Value = serde_json::from_str(raw).ok()?;
    let mut last = None;
    let mut visit = |event: &Value| {
        let kind = event.get("event_type").and_then(Value::as_str);
        if kind == Some("book") {
            if let Some(asset) = event.get("asset_id").and_then(Value::as_str) {
                if wanted.contains(asset) { books.insert(asset.to_owned()); }
            }
        }
        if kind != Some("book") && kind != Some("price_change") { return; }
        if let Some(ts) = event.get("timestamp").and_then(Value::as_str).and_then(|s| s.parse::<i64>().ok()) {
            if (1_000_000_000_000..10_000_000_000_000).contains(&ts) {
                let age = at_ms - ts;
                ages.push(age);
                last = Some(age);
            }
        }
    };
    match &value {
        Value::Array(items) => { for event in items { visit(event); } }
        Value::Object(_) => visit(&value),
        _ => {},
    }
    last
}

async fn socket_task(slot: usize, generation: u64, proxy_host: String, proxy_port: u16, assets: Vec<String>, tx: mpsc::UnboundedSender<Report>) {
    let result: Result<(), String> = async {
        let tcp = tokio::time::timeout(Duration::from_secs(10), tunnel(&proxy_host, proxy_port)).await.map_err(|_| "CONNECT timeout".to_string())??;
        let (mut ws, _) = tokio::time::timeout(Duration::from_secs(10), client_async_tls_with_config(format!("wss://{HOST}/ws/market"), tcp, None, None)).await.map_err(|_| "WS handshake timeout".to_string())?.map_err(|e| e.to_string())?;
        ws.send(Message::Text(json!({"assets_ids": assets, "type": "market"}).to_string().into())).await.map_err(|e| e.to_string())?;
        emit(&tx, slot, generation, Event::Connected);
        let wanted: HashSet<String> = assets.into_iter().collect();
        let mut books = HashSet::new();
        let mut seeded = false;
        let mut ages = Vec::new();
        let mut last_age = None;
        let mut frames = 0;
        let mut ping = tokio::time::interval(Duration::from_secs(10));
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ping.tick().await;
        tick.tick().await;
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
                    frames += 1;
                    let at_ms = now_ms();
                    if let Some(age) = inspect_frame(&raw, at_ms, &wanted, &mut books, &mut ages) {
                        last_age = Some(age);
                        emit(&tx, slot, generation, Event::AgeSample { age_ms: age });
                    }
                    if !seeded && books.len() == wanted.len() {
                        seeded = true;
                        emit(&tx, slot, generation, Event::Seeded { age_ms: last_age });
                    }
                }
                _ = ping.tick() => ws.send(Message::Text("PING".into())).await.map_err(|e| e.to_string())?,
                _ = tick.tick() => {
                    ages.sort_unstable();
                    let n = ages.len();
                    let p50 = (n > 0).then(|| ages[n / 2]);
                    let p95 = (n > 0).then(|| ages[(n * 95 / 100).min(n - 1)]);
                    let max = ages.last().copied();
                    emit(&tx, slot, generation, Event::Window { frames, events: n, p50_ms: p50, p95_ms: p95, max_ms: max, last_ms: last_age });
                    frames = 0;
                    ages.clear();
                }
            }
        }
    }.await;
    emit(&tx, slot, generation, Event::Closed(result.err().unwrap_or_else(|| "ended".into())));
}

fn spawn_slot(slot: usize, generations: &mut [u64; 2], tasks: &mut [Option<JoinHandle<()>>; 2], seeded: &mut [bool; 2], ages: &mut [Option<i64>; 2], host: &str, port: u16, assets: &[String], tx: &mpsc::UnboundedSender<Report>, started: Instant) {
    generations[slot] += 1;
    seeded[slot] = false;
    ages[slot] = None;
    tasks[slot] = Some(tokio::spawn(socket_task(slot, generations[slot], host.to_owned(), port, assets.to_vec(), tx.clone())));
    println!("{}", json!({"kind":"prepare_connection","slot":if slot==0 {"A"} else {"B"},"generation":generations[slot],"elapsed_ms":started.elapsed().as_millis(),"at_ms":now_ms()}));
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut assets = Vec::new();
    let mut host = "127.0.0.1".to_owned();
    let mut port = 17890_u16;
    let mut duration = 300_u64;
    let mut start_at_ms = 0_i64;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args.next().ok_or("missing argument value")?;
        match arg.as_str() {
            "--asset-id" => assets.push(value),
            "--proxy-host" => host = value,
            "--proxy-port" => port = value.parse()?,
            "--duration-seconds" => duration = value.parse()?,
            "--start-at-ms" => start_at_ms = value.parse()?,
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    if assets.len() != 2 || assets[0] == assets[1] || duration != 300 {
        return Err("require two distinct asset IDs and exactly 300 seconds".into());
    }
    if start_at_ms > now_ms() { tokio::time::sleep(Duration::from_millis((start_at_ms - now_ms()) as u64)).await; }
    let started = Instant::now();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut tasks: [Option<JoinHandle<()>>; 2] = [None, None];
    let mut generations = [0_u64; 2];
    let mut seeded = [false; 2];
    let mut ages = [None, None];
    let mut active = 0_usize;
    let mut prepare_since: Option<Instant> = None;
    let mut high_since: Option<Instant> = None;
    let mut dwell_confirmed = false;
    let mut active_dead = false;
    let mut may_prepare = true;
    let mut last_age_at: [Option<Instant>; 2] = [None, None];
    let mut last_attempt = Instant::now() - Duration::from_secs(10);
    let mut switches = 0_usize;
    spawn_slot(0, &mut generations, &mut tasks, &mut seeded, &mut ages, &host, port, &assets, &tx, started);
    println!("{}", json!({"kind":"start","at_ms":now_ms(),"duration_seconds":duration,"prepare_age_ms":PREPARE_MS,"switch_age_ms":SWITCH_MS,"switch_dwell_ms":SWITCH_DWELL_MS,"assets":assets}));
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(started + Duration::from_secs(duration))) => break,
            report = rx.recv() => {
                let Some(report) = report else { break; };
                let slot = report.slot;
                if report.generation != generations[slot] { continue; }
                let label = if slot == 0 { "A" } else { "B" };
                match report.event {
                    Event::Connected => println!("{}", json!({"kind":"connected","slot":label,"generation":report.generation,"at_ms":report.at_ms,"elapsed_ms":started.elapsed().as_millis()})),
                    Event::Seeded { age_ms } => {
                        seeded[slot] = true;
                        ages[slot] = age_ms;
                        last_age_at[slot] = Some(Instant::now());
                        println!("{}", json!({"kind":"both_books_seeded","slot":label,"generation":report.generation,"age_ms":age_ms,"at_ms":report.at_ms,"elapsed_ms":started.elapsed().as_millis()}));
                    }
                    Event::AgeSample { age_ms } => {
                        ages[slot] = Some(age_ms);
                        last_age_at[slot] = Some(Instant::now());
                        if slot == active && seeded[slot] {
                            if age_ms < PREPARE_MS {
                                if prepare_since.is_some() {
                                    println!("{}", json!({"kind":"recovered","age_ms":age_ms,"at_ms":report.at_ms,"elapsed_ms":started.elapsed().as_millis()}));
                                    let standby = 1 - active;
                                    if let Some(task) = tasks[standby].take() { task.abort(); generations[standby] += 1; }
                                    seeded[standby] = false;
                                    ages[standby] = None;
                                    last_age_at[standby] = None;
                                }
                                prepare_since = None;
                                high_since = None;
                                dwell_confirmed = false;
                                may_prepare = true;
                            } else if age_ms < SWITCH_MS {
                                if high_since.is_some() {
                                    println!("{}", json!({"kind":"recovered","age_ms":age_ms,"at_ms":report.at_ms,"elapsed_ms":started.elapsed().as_millis()}));
                                    let standby = 1 - active;
                                    if let Some(task) = tasks[standby].take() { task.abort(); generations[standby] += 1; }
                                    seeded[standby] = false;
                                    ages[standby] = None;
                                    last_age_at[standby] = None;
                                    prepare_since = None;
                                    high_since = None;
                                    dwell_confirmed = false;
                                    may_prepare = false;
                                } else if may_prepare && prepare_since.is_none() {
                                    prepare_since = Some(Instant::now());
                                    println!("{}", json!({"kind":"prepare_threshold","age_ms":age_ms,"at_ms":report.at_ms,"elapsed_ms":started.elapsed().as_millis()}));
                                }
                            } else if may_prepare {
                                if prepare_since.is_none() {
                                    prepare_since = Some(Instant::now());
                                    println!("{}", json!({"kind":"prepare_threshold","age_ms":age_ms,"at_ms":report.at_ms,"elapsed_ms":started.elapsed().as_millis()}));
                                }
                                if high_since.is_none() {
                                    high_since = Some(Instant::now());
                                    println!("{}", json!({"kind":"high_threshold","age_ms":age_ms,"at_ms":report.at_ms,"elapsed_ms":started.elapsed().as_millis()}));
                                }
                                if !dwell_confirmed && high_since.is_some_and(|since| since.elapsed() >= Duration::from_millis(SWITCH_DWELL_MS)) {
                                    dwell_confirmed = true;
                                    println!("{}", json!({"kind":"high_sustained","age_ms":age_ms,"at_ms":report.at_ms,"elapsed_ms":started.elapsed().as_millis()}));
                                }
                            }
                        }
                    }
                    Event::Window { frames, events, p50_ms, p95_ms, max_ms, last_ms } => {
                        println!("{}", json!({"kind":"window","slot":label,"generation":report.generation,"active":slot==active,"at_ms":report.at_ms,"elapsed_ms":started.elapsed().as_millis(),"frames":frames,"timestamped_events":events,"age_p50_ms":p50_ms,"age_p95_ms":p95_ms,"age_max_ms":max_ms,"last_age_ms":last_ms}));
                    }
                    Event::Closed(detail) => {
                        tasks[slot] = None;
                        seeded[slot] = false;
                        println!("{}", json!({"kind":"closed","slot":label,"generation":report.generation,"active":slot==active,"detail":detail,"at_ms":report.at_ms,"elapsed_ms":started.elapsed().as_millis()}));
                        if slot == active { active_dead = true; prepare_since.get_or_insert_with(Instant::now); }
                    }
                }
            }
            _ = tick.tick() => {}
        }
        let standby = 1 - active;
        if (seeded[active] && prepare_since.is_some() || active_dead) && tasks[standby].is_none() && last_attempt.elapsed() >= Duration::from_secs(1) {
            spawn_slot(standby, &mut generations, &mut tasks, &mut seeded, &mut ages, &host, port, &assets, &tx, started);
            last_attempt = Instant::now();
        }
        if (dwell_confirmed || active_dead) && seeded[standby] {
            let standby_now_age = ages[standby].zip(last_age_at[standby])
                .map(|(age, at)| age.saturating_add(at.elapsed().as_millis() as i64));
            if standby_now_age.is_some_and(|age| age < SWITCH_MS) {
                let old = active;
                let wait_ms = high_since.map(|when| when.elapsed().as_millis());
                let prepare_ms = prepare_since.map(|when| when.elapsed().as_millis());
                if let Some(task) = tasks[old].take() { task.abort(); generations[old] += 1; }
                seeded[old] = false;
                ages[old] = None;
                last_age_at[old] = None;
                active = standby;
                switches += 1;
                println!("{}", json!({"kind":"cutover","from":if old==0 {"A"} else {"B"},"to":if active==0 {"A"} else {"B"},"new_generation":generations[active],"new_age_ms":standby_now_age,"wait_after_1500_ms":wait_ms,"elapsed_since_1200_ms":prepare_ms,"at_ms":now_ms(),"elapsed_ms":started.elapsed().as_millis()}));
                prepare_since = None;
                high_since = None;
                dwell_confirmed = false;
                active_dead = false;
                may_prepare = true;
            } else {
                println!("{}", json!({"kind":"standby_stale","slot":if standby==0 {"A"} else {"B"},"age_ms":standby_now_age,"at_ms":now_ms(),"elapsed_ms":started.elapsed().as_millis()}));
                if let Some(task) = tasks[standby].take() { task.abort(); }
                generations[standby] += 1;
                seeded[standby] = false;
                ages[standby] = None;
                last_age_at[standby] = None;
            }
        }
    }
    for task in tasks.into_iter().flatten() { task.abort(); }
    println!("{}", json!({"kind":"end","at_ms":now_ms(),"elapsed_ms":started.elapsed().as_millis(),"switches":switches}));
    Ok(())
}
