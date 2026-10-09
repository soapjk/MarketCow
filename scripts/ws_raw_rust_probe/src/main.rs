//! Capture raw market WebSocket messages in memory and write them only after reception ends.
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::{env, fs::OpenOptions, io::{BufWriter, Write}, path::PathBuf, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use tokio_tungstenite::{client_async_tls_with_config, tungstenite::Message};

const HOST: &str = "ws-subscriptions-clob.polymarket.com";

#[derive(Serialize)]
struct ReceivedMessage {
    raw_data: String,
    received_at_ns: u128,
}

fn now_ns() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock before epoch").as_nanos()
}

async fn tunnel(host: &str, port: u16) -> Result<TcpStream, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true)?;
    let request = format!("CONNECT {HOST}:443 HTTP/1.1\r\nHost: {HOST}:443\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut header = Vec::with_capacity(256);
    loop {
        let mut byte = [0];
        stream.read_exact(&mut byte).await?;
        header.push(byte[0]);
        if header.ends_with(b"\r\n\r\n") { break; }
        if header.len() >= 8192 { return Err("CONNECT header too large".into()); }
    }
    if !header.starts_with(b"HTTP/1.1 200 ") && !header.starts_with(b"HTTP/1.0 200 ") {
        return Err(format!("CONNECT rejected: {}", String::from_utf8_lossy(&header[..header.len().min(80)])).into());
    }
    Ok(stream)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut assets = Vec::new();
    let mut output: Option<PathBuf> = None;
    let mut proxy_host = String::from("127.0.0.1");
    let mut proxy_port = 17890_u16;
    let mut duration = 300_u64;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args.next().ok_or("missing argument value")?;
        match arg.as_str() {
            "--asset-id" => assets.push(value),
            "--output" => output = Some(value.into()),
            "--proxy-host" => proxy_host = value,
            "--proxy-port" => proxy_port = value.parse()?,
            "--duration-seconds" => duration = value.parse()?,
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    if assets.is_empty() || duration == 0 || duration > 300 { return Err("need assets and duration 1..300".into()); }
    let output = output.ok_or("missing --output")?;
    if output.exists() { return Err("output already exists".into()); }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let started_at_ns = now_ns();
    let mut messages = Vec::<ReceivedMessage>::new();
    let mut error: Option<String> = None;
    let capture = async {
        let stream = tokio::time::timeout(Duration::from_secs(10), tunnel(&proxy_host, proxy_port)).await??;
        let (mut ws, _) = tokio::time::timeout(
            Duration::from_secs(10),
            client_async_tls_with_config(format!("wss://{HOST}/ws/market"), stream, None, None),
        ).await??;
        let subscription = serde_json::json!({"assets_ids": assets, "type": "market"}).to_string();
        ws.send(Message::Text(subscription.into())).await?;
        let deadline = Instant::now() + Duration::from_secs(duration);
        let mut ping = tokio::time::interval(Duration::from_secs(10));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ping.tick().await;
        loop {
            tokio::select! {
                next = ws.next() => match next {
                    Some(Ok(Message::Text(raw))) => {
                        let received_at_ns = now_ns();
                        messages.push(ReceivedMessage { raw_data: raw.to_string(), received_at_ns });
                    }
                    Some(Ok(Message::Binary(raw))) => {
                        let received_at_ns = now_ns();
                        messages.push(ReceivedMessage { raw_data: raw.iter().map(|&b| char::from(b)).collect(), received_at_ns });
                    }
                    Some(Ok(Message::Close(frame))) => { error = Some(format!("close: {frame:?}")); break; }
                    Some(Ok(_)) => {},
                    Some(Err(err)) => { error = Some(format!("read: {err}")); break; }
                    None => { error = Some(String::from("stream ended")); break; }
                },
                _ = ping.tick() => ws.send(Message::Text("PING".into())).await?,
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => break,
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    if let Err(err) = capture.await { error = Some(err.to_string()); }
    let ended_at_ns = now_ns();
    let file = OpenOptions::new().write(true).create_new(true).open(&output)?;
    let mut writer = BufWriter::new(file);
    for message in &messages {
        serde_json::to_writer(&mut writer, message)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    println!("{}", serde_json::json!({"output": output, "messages": messages.len(), "started_at_ns": started_at_ns, "ended_at_ns": ended_at_ns, "error": error}));
    Ok(())
}
