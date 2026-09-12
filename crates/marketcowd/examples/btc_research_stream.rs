//! Bounded direct Polymarket WS capture for at most three reviewed markets.
//!
//! This is an isolated research input, not a Live/Discovery scope mutation and
//! not a tradable book projection.  The upstream adapter owns reconnects and
//! emits explicit source-gap facts at every connection boundary.
use anyhow::{Context, Result, ensure};
use chrono::{SecondsFormat, Utc};
use marketcow_polymarket::{
    PolymarketTransportConfig, RawTransportFrame, run_polymarket_transport,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
};

const MAX_CONFIG_BYTES: usize = 64 * 1024;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Market {
    market_id: String,
    condition_id: String,
    token_ids: [String; 2],
    rule_evidence_path: PathBuf,
    rule_evidence_sha256: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    schema_version: String,
    markets: Vec<Market>,
    seconds: u64,
    maximum_batches: u64,
    maximum_frames: u64,
    maximum_total_bytes: u64,
    maximum_batch_bytes: u64,
    maximum_pending_batches: usize,
    maximum_pending_bytes: u32,
}

fn hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn validate(config: &Config) -> Result<Vec<String>> {
    ensure!(
        config.schema_version == "marketcow.btc-hour.rust-research-stream-config.v1",
        "config schema"
    );
    ensure!(
        (1..=3).contains(&config.markets.len()),
        "one to three reviewed markets required"
    );
    ensure!((1..=1800).contains(&config.seconds), "seconds budget");
    ensure!(
        (1..=1_000_000).contains(&config.maximum_batches),
        "batch budget"
    );
    ensure!(
        (1..=5_000_000).contains(&config.maximum_frames),
        "frame budget"
    );
    ensure!(
        (1..=4 * 1024 * 1024 * 1024).contains(&config.maximum_total_bytes),
        "total byte budget"
    );
    ensure!(
        (1..=8 * 1024 * 1024).contains(&config.maximum_batch_bytes),
        "batch byte budget"
    );
    ensure!(
        (1..=256).contains(&config.maximum_pending_batches),
        "pending batch budget"
    );
    ensure!(
        config.maximum_pending_bytes > 0
            && config.maximum_pending_bytes <= 512 * 1024 * 1024
            && u64::from(config.maximum_pending_bytes) >= config.maximum_batch_bytes,
        "pending byte budget"
    );
    let mut markets = BTreeSet::new();
    let mut conditions = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    for market in &config.markets {
        ensure!(
            !market.market_id.is_empty()
                && market.market_id.len() <= 64
                && market.market_id.is_ascii(),
            "market id"
        );
        ensure!(
            market.condition_id.len() == 66
                && market.condition_id.starts_with("0x")
                && market.condition_id[2..]
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit()),
            "condition id"
        );
        ensure!(hex64(&market.rule_evidence_sha256), "rule evidence sha");
        ensure!(
            markets.insert(&market.market_id) && conditions.insert(&market.condition_id),
            "duplicate identity"
        );
        for token in &market.token_ids {
            ensure!(
                !token.is_empty()
                    && token.len() <= 128
                    && token.bytes().all(|byte| byte.is_ascii_digit()),
                "token id"
            );
            ensure!(tokens.insert(token.clone()), "duplicate token");
        }
    }
    Ok(tokens.into_iter().collect())
}

fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(value)?)
}

async fn validate_evidence(markets: &[Market]) -> Result<()> {
    for market in markets {
        ensure!(
            market.rule_evidence_path.is_absolute(),
            "absolute evidence path required"
        );
        let raw = tokio::fs::read(&market.rule_evidence_path).await?;
        ensure!(raw.len() <= 256 * 1024, "rule evidence byte budget");
        ensure!(
            hex::encode(Sha256::digest(&raw)) == market.rule_evidence_sha256,
            "rule evidence hash changed"
        );
        let value: serde_json::Value = serde_json::from_slice(&raw)?;
        ensure!(
            value["schema_version"] == "marketcow.polymarket.market-evidence.v1"
                && value["market_id"] == market.market_id
                && value["condition_id"] == market.condition_id,
            "rule evidence identity"
        );
        let outcomes = value["outcomes"]
            .as_array()
            .context("rule evidence outcomes")?;
        let observed: BTreeSet<_> = outcomes
            .iter()
            .map(|outcome| outcome["token_id"].as_str().context("rule evidence token"))
            .collect::<Result<_>>()?;
        ensure!(
            observed == market.token_ids.iter().map(String::as_str).collect(),
            "rule evidence token binding"
        );
    }
    Ok(())
}

fn encode_frame(frame: &RawTransportFrame) -> Result<Vec<u8>> {
    let mut raw = canonical(&json!({
        "schema_version":"marketcow.btc-hour.rust-research-frame.v1",
        "received_at":frame.received_at.to_rfc3339_opts(SecondsFormat::Micros,true),
        "raw_payload":frame.raw_payload,
        "raw_wire_bytes_preserved":false,
        "limitation":"transport parsed JSON before this archive boundary",
    }))?;
    raw.push(b'\n');
    Ok(raw)
}

async fn run(config_path: &Path, output: &Path) -> Result<serde_json::Value> {
    ensure!(
        config_path.is_absolute() && output.is_absolute(),
        "absolute paths required"
    );
    ensure!(!output.exists(), "output already exists");
    let config_raw = tokio::fs::read(config_path).await?;
    ensure!(config_raw.len() <= MAX_CONFIG_BYTES, "config byte budget");
    let config: Config = serde_json::from_slice(&config_raw)?;
    let tokens = validate(&config)?;
    validate_evidence(&config.markets).await?;
    tokio::fs::create_dir_all(output).await?;
    let config_sha = hex::encode(Sha256::digest(&config_raw));
    tokio::fs::write(output.join("config.json"), &config_raw).await?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // One transport batch can wait while the application immediately publishes
    // into the separately byte-and-count bounded persistence queue below.
    let (sender, mut receiver) = mpsc::channel(1);
    let transport = tokio::spawn(run_polymarket_transport(
        PolymarketTransportConfig::production(),
        tokens,
        sender,
        shutdown_rx,
    ));
    let file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output.join("frames.jsonl"))
        .await?;
    struct PersistBatch {
        raw: Vec<u8>,
        _permit: OwnedSemaphorePermit,
    }
    let (persist_tx, mut persist_rx) =
        mpsc::channel::<PersistBatch>(config.maximum_pending_batches);
    let byte_budget = Arc::new(Semaphore::new(config.maximum_pending_bytes as usize));
    let writer = tokio::spawn(async move {
        let mut output = BufWriter::new(file);
        let mut digest = Sha256::new();
        let mut bytes = 0_u64;
        while let Some(batch) = persist_rx.recv().await {
            output.write_all(&batch.raw).await?;
            digest.update(&batch.raw);
            bytes += batch.raw.len() as u64;
        }
        output.flush().await?;
        output.get_ref().sync_all().await?;
        Result::<_>::Ok((bytes, hex::encode(digest.finalize())))
    });
    let deadline = tokio::time::sleep(Duration::from_secs(config.seconds));
    tokio::pin!(deadline);
    let mut frames = 0_u64;
    let mut batches = 0_u64;
    let mut bytes = 0_u64;
    let started_at = Utc::now();
    let mut stop_reason = "observation_window_complete";
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            maybe = receiver.recv() => {
                let Some(batch) = maybe else {
                    stop_reason = "transport_ended";
                    break;
                };
                let mut encoded = Vec::new();
                for frame in batch {
                    let raw = encode_frame(&frame)?;
                    ensure!(encoded.len().saturating_add(raw.len()) <= config.maximum_batch_bytes as usize,
                        "batch byte budget exceeded");
                    encoded.extend_from_slice(&raw);
                    frames += 1;
                    ensure!(frames <= config.maximum_frames, "frame budget exceeded");
                }
                batches += 1;
                bytes = bytes.saturating_add(encoded.len() as u64);
                ensure!(batches <= config.maximum_batches, "batch budget exceeded");
                ensure!(bytes <= config.maximum_total_bytes, "total byte budget exceeded");
                let permit = byte_budget.clone().acquire_many_owned(encoded.len() as u32).await
                    .context("persistence byte queue closed")?;
                persist_tx.send(PersistBatch { raw: encoded, _permit: permit }).await
                    .context("persistence count queue closed")?;
            }
        }
    }
    shutdown_tx.send(true).ok();
    let transport_result = tokio::time::timeout(Duration::from_secs(5), transport)
        .await
        .context("transport shutdown timeout")?
        .context("transport task")?;
    if let Err(error) = transport_result {
        stop_reason = error.reason_code();
    }
    drop(persist_tx);
    let (written_bytes, frame_sha) = writer.await.context("persistence task")??;
    ensure!(written_bytes == bytes, "archive size changed");
    let report = json!({
        "schema_version":"marketcow.btc-hour.rust-research-stream-report.v1",
        "status":if stop_reason == "observation_window_complete" {"complete"} else {"failed"},
        "stop_reason":stop_reason,
        "started_at":started_at.to_rfc3339_opts(SecondsFormat::Micros,true),
        "finished_at":Utc::now().to_rfc3339_opts(SecondsFormat::Micros,true),
        "markets":config.markets,
        "config_sha256":config_sha,
        "batches":batches,"frames":frames,"bytes":bytes,
        "frames_sha256":frame_sha,
        "persistence_queue":{"maximum_batches":config.maximum_pending_batches,
            "maximum_bytes":config.maximum_pending_bytes,"pending_batches":0,"pending_bytes":0},
        "raw_wire_bytes_preserved":false,
        "activation_performed":false,
        "account_or_scope_mutation":false,
    });
    tokio::fs::write(output.join("report.json"), canonical(&report)?).await?;
    Ok(report)
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments: Vec<String> = std::env::args().collect();
    ensure!(
        arguments.len() == 3,
        "usage: btc_research_stream CONFIG OUTPUT"
    );
    let report = run(Path::new(&arguments[1]), Path::new(&arguments[2])).await?;
    println!("{}", serde_json::to_string(&report)?);
    ensure!(report["status"] == "complete", "research stream failed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> Config {
        Config {
            schema_version: "marketcow.btc-hour.rust-research-stream-config.v1".into(),
            markets: vec![Market {
                market_id: "1".into(),
                condition_id: format!("0x{}", "a".repeat(64)),
                token_ids: ["10".into(), "20".into()],
                rule_evidence_path: PathBuf::from("/tmp/evidence.json"),
                rule_evidence_sha256: "b".repeat(64),
            }],
            seconds: 1,
            maximum_batches: 10,
            maximum_frames: 20,
            maximum_total_bytes: 1024,
            maximum_batch_bytes: 512,
            maximum_pending_batches: 2,
            maximum_pending_bytes: 1024,
        }
    }
    #[test]
    fn exact_identity_and_resource_bounds() {
        assert_eq!(validate(&config()).unwrap(), vec!["10", "20"]);
        let mut duplicate = config();
        duplicate.markets.push(Market {
            market_id: "2".into(),
            condition_id: format!("0x{}", "c".repeat(64)),
            token_ids: ["20".into(), "30".into()],
            rule_evidence_path: PathBuf::from("/tmp/evidence-2.json"),
            rule_evidence_sha256: "d".repeat(64),
        });
        assert!(
            validate(&duplicate)
                .unwrap_err()
                .to_string()
                .contains("duplicate token")
        );
        let mut oversized = config();
        oversized.maximum_pending_batches = 257;
        assert!(validate(&oversized).is_err());
    }
    #[test]
    fn archived_frame_marks_exact_wire_boundary_unknown() {
        let frame = RawTransportFrame {
            raw_payload: json!({"event_type":"book","asset_id":"10"}),
            received_at: Utc::now(),
        };
        let value: serde_json::Value =
            serde_json::from_slice(&encode_frame(&frame).unwrap()).unwrap();
        assert_eq!(value["raw_wire_bytes_preserved"], false);
        assert_eq!(value["raw_payload"]["asset_id"], "10");
    }

    #[tokio::test]
    async fn evidence_file_binds_market_condition_and_both_tokens() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("evidence.json");
        let raw = canonical(
            &json!({"schema_version":"marketcow.polymarket.market-evidence.v1",
            "market_id":"1","condition_id":format!("0x{}","a".repeat(64)),
            "outcomes":[{"token_id":"10"},{"token_id":"20"}]}),
        )
        .unwrap();
        std::fs::write(&path, &raw).unwrap();
        let mut value = config();
        value.markets[0].rule_evidence_path = path;
        value.markets[0].rule_evidence_sha256 = hex::encode(Sha256::digest(&raw));
        validate_evidence(&value.markets).await.unwrap();
        value.markets[0].token_ids[1] = "21".into();
        assert!(
            validate_evidence(&value.markets)
                .await
                .unwrap_err()
                .to_string()
                .contains("token binding")
        );
    }
}
