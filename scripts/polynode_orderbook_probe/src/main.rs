//! Isolated, read-only capture of PolyNode's off-chain orderbook WebSocket.
//! The API key is read from a mode-0600 file and never printed or stored in captures.
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::{
    env,
    fs::{self, File},
    io::{BufWriter, Write},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async,
    tungstenite::Message,
};

const HOST: &str = "ob.polynode.dev";
const DEFAULT_KEY_FILE: &str = "/mnt/p44pro/marketcow-shadow-v3-runtime/linux/diagnostics/polynode-probe/api-key";
const MAX_CAPTURE_BYTES: usize = 512 * 1024 * 1024;

#[derive(Serialize)]
struct ReceivedMessage {
    received_at_ns: u128,
    raw_data: String,
}

fn now_ns() -> Result<u128, Box<dyn std::error::Error>> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())
}

fn usage() -> &'static str {
    "usage: polynode-orderbook-probe --output PATH [--market SLUG_OR_CONDITION_OR_TOKEN ...] [--duration-seconds 1..600] [--stop-after-first-data] [--api-key-file PATH] [--proxy-host HOST --proxy-port PORT]\nIf --market is omitted, subscribes to the current and next btc-updown-5m slugs."
}

fn key_from_file(path: &PathBuf) -> Result<String, Box<dyn std::error::Error>> {
    let meta = fs::metadata(path)?;
    if !meta.is_file() || meta.permissions().mode() & 0o077 != 0 {
        return Err("API key must be in a regular file readable only by its owner (mode 0600)".into());
    }
    if meta.len() == 0 || meta.len() > 512 {
        return Err("API key file must contain one nonempty short key".into());
    }
    let key = fs::read_to_string(path)?;
    let key = key.trim_end_matches(['\r', '\n']);
    if !(key.starts_with("pn_live_") || key.starts_with("qm_live_"))
        || !key.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err("API key file is not a valid Polynode key".into());
    }
    Ok(key.to_owned())
}

async fn proxy_tunnel(host: &str, port: u16) -> Result<TcpStream, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true)?;
    stream
        .write_all(format!("CONNECT {HOST}:443 HTTP/1.1\r\nHost: {HOST}:443\r\n\r\n").as_bytes())
        .await?;
    let mut header = Vec::with_capacity(256);
    loop {
        let mut byte = [0];
        stream.read_exact(&mut byte).await?;
        header.push(byte[0]);
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
        if header.len() > 8192 {
            return Err("proxy CONNECT response too large".into());
        }
    }
    if !header.starts_with(b"HTTP/1.1 200 ") && !header.starts_with(b"HTTP/1.0 200 ") {
        return Err("proxy CONNECT rejected".into());
    }
    Ok(stream)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut key_file = PathBuf::from(DEFAULT_KEY_FILE);
    let mut output: Option<PathBuf> = None;
    let mut markets = Vec::<String>::new();
    let mut duration = 300_u64;
    let mut stop_after_first_data = false;
    let mut proxy_host: Option<String> = None;
    let mut proxy_port: Option<u16> = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--help" {
            println!("{}", usage());
            return Ok(());
        }
        if arg == "--stop-after-first-data" {
            stop_after_first_data = true;
            continue;
        }
        let value = args.next().ok_or("missing argument value")?;
        match arg.as_str() {
            "--api-key-file" => key_file = value.into(),
            "--output" => output = Some(value.into()),
            "--market" => markets.push(value),
            "--duration-seconds" => duration = value.parse()?,
            "--proxy-host" => proxy_host = Some(value),
            "--proxy-port" => proxy_port = Some(value.parse()?),
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    if !(1..=600).contains(&duration) || proxy_host.is_some() != proxy_port.is_some() {
        return Err(usage().into());
    }
    let output = output.ok_or("missing --output")?;
    if output.exists() {
        return Err("output already exists".into());
    }
    let key = key_from_file(&key_file)?;
    if markets.is_empty() {
        let now_s = now_ns()? / 1_000_000_000;
        let window_s = now_s / 300 * 300;
        markets = [window_s, window_s + 300]
            .into_iter()
            .map(|start| format!("btc-updown-5m-{start}"))
            .collect();
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let url = format!("wss://{HOST}/ws?key={key}");
    let start_ns = now_ns()?;
    let mut captured = Vec::<ReceivedMessage>::new();
    let mut capture_bytes = 0_usize;
    let mut controls = 0_u64;
    let mut error: Option<&'static str> = None;
    let mut connect_http_status: Option<u16> = None;

    // Never format a WebSocket error: it may contain the authenticated request URL.
    let connection = if let (Some(host), Some(port)) = (proxy_host, proxy_port) {
        let tunnel = tokio::time::timeout(Duration::from_secs(10), proxy_tunnel(&host, port)).await;
        match tunnel {
            Ok(Ok(stream)) => tokio::time::timeout(
                Duration::from_secs(10),
                client_async_tls_with_config(url, stream, None, None),
            )
            .await
            .map(|result| result.map(|(ws, _)| ws).map_err(|error| match error {
                tokio_tungstenite::tungstenite::Error::Http(response) =>
                    Some(response.status().as_u16()),
                _ => None,
            }))
            .map_err(|_| ()),
            _ => Err(()),
        }
    } else {
        tokio::time::timeout(Duration::from_secs(10), connect_async(url))
            .await
            .map(|result| result.map(|(ws, _)| ws).map_err(|error| match error {
                tokio_tungstenite::tungstenite::Error::Http(response) =>
                    Some(response.status().as_u16()),
                _ => None,
            }))
            .map_err(|_| ())
    };

    match connection {
        Ok(Ok(mut ws)) => {
            let subscribe = serde_json::json!({"action":"subscribe","markets":markets});
            if ws.send(Message::Text(subscribe.to_string().into())).await.is_err() {
                error = Some("subscribe_send_failed");
            } else {
                let deadline = Instant::now() + Duration::from_secs(duration);
                loop {
                    tokio::select! {
                        result = ws.next() => match result {
                            Some(Ok(Message::Text(raw))) => {
                                let received_at_ns = now_ns()?;
                                let kind = serde_json::from_str::<serde_json::Value>(&raw)
                                    .ok().and_then(|value| value.get("type").and_then(|v| v.as_str()).map(str::to_owned));
                                if matches!(kind.as_deref(), Some("batch" | "snapshot_batch" | "book_snapshot" | "book_update")) {
                                    if raw.contains(&key) {
                                        error = Some("secret_echo_in_data_frame");
                                        break;
                                    }
                                    capture_bytes += raw.len();
                                    if capture_bytes > MAX_CAPTURE_BYTES {
                                        error = Some("capture_limit_reached");
                                        break;
                                    }
                                    captured.push(ReceivedMessage { received_at_ns, raw_data: raw.to_string() });
                                    if stop_after_first_data {
                                        break;
                                    }
                                } else {
                                    controls += 1;
                                }
                            }
                            Some(Ok(Message::Binary(_))) => { error = Some("unexpected_binary_frame"); break; }
                            Some(Ok(Message::Close(_))) => { error = Some("server_closed"); break; }
                            Some(Ok(_)) => { controls += 1; }
                            Some(Err(_)) => { error = Some("websocket_read_failed"); break; }
                            None => { error = Some("websocket_ended"); break; }
                        },
                        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => break,
                    }
                }
            }
            let _ = tokio::time::timeout(Duration::from_secs(1), ws.close(None)).await;
        }
        Ok(Err(status)) => {
            connect_http_status = status;
            error = Some("connect_http_rejected");
        }
        Err(()) => error = Some("connect_failed"),
    }
    let end_ns = now_ns()?;
    let mut writer = BufWriter::new(File::options().write(true).create_new(true).open(&output)?);
    for frame in &captured {
        serde_json::to_writer(&mut writer, frame)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    fs::set_permissions(&output, fs::Permissions::from_mode(0o600))?;
    println!("{}", serde_json::json!({
        "markets": markets,
        "started_at_ns": start_ns,
        "ended_at_ns": end_ns,
        "duration_seconds": (end_ns - start_ns) as f64 / 1e9,
        "captured_frames": captured.len(),
        "captured_raw_bytes": capture_bytes,
        "control_frames": controls,
        "output": output,
        "error": error,
        "connect_http_status": connect_http_status,
    }));
    Ok(())
}
