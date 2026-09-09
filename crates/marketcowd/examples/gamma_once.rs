//! One read-only Gamma request; no service state or account access.
use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, time::{Duration, Instant}};

#[tokio::main]
async fn main() -> Result<()> {
    let output = PathBuf::from(std::env::args().nth(1).expect("new output directory required"));
    std::fs::create_dir(&output)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    let url = "https://gamma-api.polymarket.com/markets/1088482";
    let started_at = chrono::Utc::now();
    let started = Instant::now();
    let mut body = Vec::new();
    let mut report = serde_json::json!({"url":url,"started_at":started_at,
        "client":"reqwest","host":"local, not U1 collector",
        "requests_max":1,"response_cap":262144,"timeout_seconds":15,
        "proxy_policy":"reqwest system default","redirects":false,"retries":false,
        "source_sha256":hex::encode(Sha256::digest(include_bytes!("gamma_once.rs"))),
        "raw_complete":false});
    let result: Result<()> = async {
        let mut response = client.get(url).send().await?;
        report["status"] = response.status().as_u16().into();
        ensure!(response.content_length().is_none_or(|n|n<=262144), "response cap");
        while let Some(chunk) = response.chunk().await? {
            ensure!(body.len()+chunk.len()<=262144, "response cap");
            body.extend_from_slice(&chunk);
        }
        report["raw_complete"] = true.into();
        Ok(())
    }.await;
    if result.is_err() {
        // Do not expose transport errors containing proxy configuration.
        report["error"] = "request_or_response_failed".into();
    }
    report["received_at"] = chrono::Utc::now().to_rfc3339().into();
    report["elapsed_ms"] = (started.elapsed().as_millis() as u64).into();
    report["bytes"] = body.len().into();
    report["raw_sha256"] = hex::encode(Sha256::digest(&body)).into();
    if report["status"] == 200 && report["raw_complete"] == true {
        if let Ok(market) = serde_json::from_slice::<serde_json::Value>(&body) {
            report["market_id"] = market["id"].clone();
            report["condition_id"] = market["conditionId"].clone();
        }
    }
    std::fs::write(output.join("response.raw"), body)?;
    let serialized = serde_json::to_vec_pretty(&report)?;
    std::fs::write(output.join("report.json"), &serialized)?;
    println!("{}", String::from_utf8(serialized.clone())?);
    println!("report_sha256={}",hex::encode(Sha256::digest(serialized)));
    Ok(())
}
