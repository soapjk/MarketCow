//! Explicit Binance-only transport. Never inherits the Polymarket proxy.
use anyhow::{Context, Result, ensure};
use std::time::Duration;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::protocol::WebSocketConfig};

fn proxy_uri(value: &str) -> Result<axum::http::Uri> {
    let uri: axum::http::Uri = value.parse().context("Binance proxy URI invalid")?;
    ensure!(uri.scheme_str() == Some("http") && uri.host() == Some("127.0.0.1")
        && uri.port_u16().is_some() && uri.path() == "/" && uri.query().is_none()
        && uri.authority().is_some_and(|a| !a.as_str().contains('@')),
        "Binance proxy must be an explicit loopback HTTP endpoint");
    Ok(uri)
}

fn configured_proxy() -> Result<Option<axum::http::Uri>> {
    match std::env::var("MARKETCOW_BINANCE_PROXY") {
        Ok(value) => Ok(Some(proxy_uri(&value)?)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub fn client(timeout: Duration) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().no_proxy().timeout(timeout)
        .redirect(reqwest::redirect::Policy::none()).retry(reqwest::retry::never());
    if let Some(proxy) = configured_proxy()? {
        builder = builder.proxy(reqwest::Proxy::all(proxy.to_string())?);
    }
    Ok(builder.build()?)
}

pub async fn connect(url: &str, config: Option<WebSocketConfig>)
    -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let Some(proxy) = configured_proxy()? else {
        return Ok(tokio_tungstenite::connect_async_with_config(url, config, false).await?.0);
    };
    let target: axum::http::Uri = url.parse()?;
    let host = target.host().context("Binance target host absent")?;
    ensure!(target.scheme_str() == Some("wss") && host == "data-stream.binance.vision",
        "Binance websocket target not allowed");
    let port = target.port_u16().unwrap_or(443);
    let mut stream = TcpStream::connect(("127.0.0.1", proxy.port_u16().unwrap())).await?;
    // The proxy resolves the official hostname; no local upstream DNS lookup.
    stream.write_all(format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n").as_bytes()).await?;
    let mut response = Vec::with_capacity(128);
    loop {
        ensure!(response.len() < 4096, "Binance proxy CONNECT headers too large");
        response.push(stream.read_u8().await.context("Binance proxy CONNECT ended")?);
        if response.ends_with(b"\r\n\r\n") { break; }
    }
    ensure!(response.starts_with(b"HTTP/1.1 200 ") || response.starts_with(b"HTTP/1.0 200 "),
        "Binance proxy CONNECT rejected");
    // Normal hostname certificate verification remains enabled.
    Ok(tokio_tungstenite::client_async_tls_with_config(url, stream, config, None).await?.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn proxy_is_explicit_loopback_only() {
        assert!(proxy_uri("http://127.0.0.1:17892").is_ok());
        for value in ["", "http://example.com:80", "https://127.0.0.1:17892", "http://127.0.0.1:17892/x",
            "http://127.0.0.1:17892/?x=1", "http://user:pass@127.0.0.1:17892"] {
            assert!(proxy_uri(value).is_err(), "{value}");
        }
    }
}
