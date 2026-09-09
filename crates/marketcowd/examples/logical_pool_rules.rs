//! Fixed metadata-selected in-scope research batch. Never changes a service.
#[allow(dead_code)]
#[path = "../src/source_market_evidence.rs"]
mod evidence;
use anyhow::{ensure, Result};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{path::PathBuf, time::{Duration, Instant}};

#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(std::env::args().nth(1).expect("new absolute output directory"));
    ensure!(root.is_absolute(), "absolute output required");
    std::fs::create_dir(&root)?;
    let client = reqwest::Client::builder().timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none()).retry(reqwest::retry::never()).build()?;
    let start = Instant::now();
    let mut total = 0usize;
    let mut records = vec![];
    for (id, condition) in [
        ("2587796", "0xe5c79f9cc0092a3c064fa990454baa851638d32a988e278a548ad284e55a31ac"),
        ("2587798", "0x268e6091245fda2b8f8974729a0db131777bb1e0596e3eb2133d681345228ee7"),
        ("2589855", "0xb64e4e073ef1242216ceeb1971e4fc5bc9094532558e89014b240992baac0c35"),
        ("2589856", "0x424a35de2ed7150339423462d110256fd1a40b6a5f07c0002433bc89201798d7"),
        ("2589857", "0xaa49a7412f36c5902e19bbee7570abd3ccc7b51d72c76c0f8526d9a1876d74eb"),
    ] {
        let remaining = Duration::from_secs(180).checked_sub(start.elapsed()).filter(|x| !x.is_zero()).ok_or_else(|| anyhow::anyhow!("total deadline"))?;
        ensure!(total < 2_097_152, "total byte budget");
        let url = format!("https://gamma-api.polymarket.com/markets/{id}");
        let mut response = client.get(&url).timeout(remaining.min(Duration::from_secs(15))).send().await?;
        let status = response.status().as_u16();
        let mut raw = vec![];
        let mut truncated = false;
        while let Some(chunk) = response.chunk().await? {
            let allowance = (262_144usize - raw.len()).min(2_097_152 - total);
            let n = allowance.min(chunk.len());
            raw.extend_from_slice(&chunk[..n]); total += n;
            if n != chunk.len() { truncated = true; break; }
        }
        let observed = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        std::fs::write(root.join(format!("{id}.raw")), &raw)?;
        records.push(json!({"market_id":id,"url":url,"status":status,"observed_at":observed,"raw_bytes":raw.len(),"raw_sha256":hex::encode(Sha256::digest(&raw)),"truncated":truncated}));
        std::fs::write(root.join("report.json"), serde_json::to_vec_pretty(&json!({"records":records,"retained_raw_bytes":total,"elapsed_seconds":start.elapsed().as_secs_f64(),"transport_chunk_may_exceed_retained_budget":true}))?)?;
        ensure!(!truncated && status == 200, "upstream failed or truncated; stopped, raw retained");
        let projected = evidence::project(id, condition, &raw, &observed)?;
        std::fs::write(root.join(format!("{id}.json")), serde_json::to_vec_pretty(&projected)?)?;
        println!("{id}: {status}, {} bytes", raw.len());
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}
