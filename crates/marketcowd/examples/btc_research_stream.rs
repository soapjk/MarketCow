//! Bounded direct Polymarket WS capture for at most three reviewed markets.
//!
//! This is an isolated research input, not a Live/Discovery scope mutation and
//! not a tradable book projection.  The upstream adapter owns reconnects and
//! emits explicit source-gap facts at every connection boundary.
use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, SecondsFormat, Utc};
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
const RESEARCH_ENDPOINT: &str = "http://192.168.124.3:8793";
const MAX_PACKAGE_AGE_SECONDS: i64 = 300;
const BINANCE_HOUR_RULE: &str = "This market will resolve to \"Up\" if the close price is greater than or equal to the open price for the BTC/USDT 1 hour candle that begins on the time and date specified in the title. Otherwise, this market will resolve to \"Down\".\n\nThe resolution source for this market is information from Binance, specifically the BTC/USDT pair (https://www.binance.com/en/trade/BTC_USDT). The close « C » and open « O » displayed at the top of the graph for the relevant \"1H\" candle will be used once the data for that candle is finalized.\n\nPlease note that this market is about the price according to Binance BTC/USDT, not according to other exchanges or trading pairs.";

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Market {
    market_id: String,
    condition_id: String,
    token_ids: [String; 2],
    rule_evidence_path: PathBuf,
    rule_evidence_sha256: String,
    rule_review_path: PathBuf,
    rule_review_sha256: String,
    binding_path: PathBuf,
    binding_sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PackageManifest {
    schema_version: String,
    created_at: String,
    audit_clock: String,
    endpoint: String,
    market_count: usize,
    token_count: usize,
    stream_config_path: PathBuf,
    stream_config_sha256: String,
    observation_seconds: u64,
    activation_performed: bool,
    subscription_started: bool,
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
        ensure!(hex64(&market.rule_review_sha256), "rule review sha");
        ensure!(hex64(&market.binding_sha256), "binding sha");
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
        ensure!(
            outcomes.len() == 2
                && outcomes[0]["token_id"] == market.token_ids[0]
                && outcomes[0]["outcome"] == "Up"
                && outcomes[1]["token_id"] == market.token_ids[1]
                && outcomes[1]["outcome"] == "Down",
            "ordered Up/Down evidence binding"
        );
        let source_raw = STANDARD.decode(value["raw_base64"].as_str().context("raw evidence")?)?;
        ensure!(source_raw.len() <= 256 * 1024, "raw evidence byte budget");
        ensure!(
            hex::encode(Sha256::digest(&source_raw))
                == value["raw_sha256"].as_str().context("raw evidence sha")?,
            "raw evidence hash"
        );
        let source: serde_json::Value = serde_json::from_slice(&source_raw)?;
        let source_tokens: Vec<String> = serde_json::from_str(
            source["clobTokenIds"]
                .as_str()
                .context("source token ids")?,
        )?;
        let source_outcomes: Vec<String> =
            serde_json::from_str(source["outcomes"].as_str().context("source outcomes")?)?;
        ensure!(
            source["id"] == market.market_id
                && source["conditionId"] == market.condition_id
                && source_tokens == market.token_ids
                && source_outcomes == ["Up", "Down"]
                && source["description"] == BINANCE_HOUR_RULE
                && source["resolutionSource"] == "https://www.binance.com/en/trade/BTC_USDT",
            "raw source rule and identity binding"
        );
    }
    Ok(())
}

fn timestamp(value: &str, name: &str) -> Result<DateTime<Utc>> {
    ensure!(value.ends_with('Z'), "{name} must use UTC Z");
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("invalid {name}"))?
        .with_timezone(&Utc))
}

async fn read_bound(path: &Path, expected_sha: &str) -> Result<(Vec<u8>, serde_json::Value)> {
    ensure!(path.is_absolute(), "bound path must be absolute");
    let raw = tokio::fs::read(path).await?;
    ensure!(raw.len() <= 256 * 1024, "bound file byte budget");
    ensure!(
        hex::encode(Sha256::digest(&raw)) == expected_sha,
        "bound file hash changed"
    );
    let value = serde_json::from_slice(&raw)?;
    Ok((raw, value))
}

async fn load_package(
    package: &Path,
    now: DateTime<Utc>,
) -> Result<(Config, Vec<u8>, String, Vec<u8>)> {
    ensure!(package.is_absolute(), "absolute package path required");
    let manifest_path = package.join("manifest.json");
    let manifest_raw = tokio::fs::read(&manifest_path).await?;
    ensure!(
        manifest_raw.len() <= MAX_CONFIG_BYTES,
        "manifest byte budget"
    );
    let manifest: PackageManifest = serde_json::from_slice(&manifest_raw)?;
    ensure!(
        manifest.schema_version == "marketcow.btc-hour.research-package.v1"
            && manifest.endpoint == RESEARCH_ENDPOINT
            && manifest.market_count == 3
            && manifest.token_count == 6
            && manifest.observation_seconds == 1800
            && !manifest.activation_performed
            && !manifest.subscription_started,
        "package manifest contract"
    );
    let audit = timestamp(&manifest.audit_clock, "audit_clock")?;
    let created = timestamp(&manifest.created_at, "created_at")?;
    ensure!(
        created >= audit && created - audit <= chrono::Duration::seconds(60),
        "package publication clock"
    );
    ensure!(
        now >= created && now - audit <= chrono::Duration::seconds(MAX_PACKAGE_AGE_SECONDS),
        "package expired or future"
    );
    let audit_hour = audit.timestamp().div_euclid(3600) * 3600;
    let now_hour = now.timestamp().div_euclid(3600) * 3600;
    ensure!(
        now_hour == audit_hour,
        "package UTC hour expired; rebuild package"
    );
    let config_path = package.join("stream-config.json");
    ensure!(
        manifest.stream_config_path == config_path,
        "manifest config path"
    );
    let config_raw = tokio::fs::read(&config_path).await?;
    ensure!(config_raw.len() <= MAX_CONFIG_BYTES, "config byte budget");
    let config_sha = hex::encode(Sha256::digest(&config_raw));
    ensure!(
        manifest.stream_config_sha256 == config_sha,
        "manifest config hash"
    );
    let config: Config = serde_json::from_slice(&config_raw)?;
    ensure!(
        config.markets.len() == 3
            && config.seconds == 1800
            && config.maximum_batches == 250_000
            && config.maximum_frames == 250_000
            && config.maximum_total_bytes == 512 * 1024 * 1024
            && config.maximum_batch_bytes == 8 * 1024 * 1024
            && config.maximum_pending_batches == 8
            && config.maximum_pending_bytes == 64 * 1024 * 1024,
        "fixed package resource profile"
    );
    for (index, market) in config.markets.iter().enumerate() {
        let root = package.join(&market.market_id);
        ensure!(
            market.rule_evidence_path == root.join("evidence.json")
                && market.rule_review_path == root.join("review.json")
                && market.binding_path == root.join("binding.json"),
            "package evidence paths"
        );
        let (_, evidence) =
            read_bound(&market.rule_evidence_path, &market.rule_evidence_sha256).await?;
        let (review_raw, review) =
            read_bound(&market.rule_review_path, &market.rule_review_sha256).await?;
        let (_, binding) = read_bound(&market.binding_path, &market.binding_sha256).await?;
        let start = timestamp(
            review["start_utc"].as_str().context("review start")?,
            "review start",
        )?;
        let end = timestamp(
            review["end_utc"].as_str().context("review end")?,
            "review end",
        )?;
        ensure!(
            start.timestamp() == audit_hour + index as i64 * 3600
                && end - start == chrono::Duration::hours(1),
            "reviewed hour window"
        );
        let source_raw = STANDARD.decode(
            evidence["raw_base64"]
                .as_str()
                .context("package raw evidence")?,
        )?;
        let source: serde_json::Value = serde_json::from_slice(&source_raw)?;
        ensure!(
            timestamp(
                source["eventStartTime"]
                    .as_str()
                    .context("source event start")?,
                "source event start"
            )? == start
                && timestamp(
                    source["endDate"].as_str().context("source end date")?,
                    "source end date"
                )? == end,
            "source and reviewed hour mismatch"
        );
        ensure!(
            review["schema_version"] == "marketcow.btc-hour.rule-review.v1"
                && review["market_id"] == market.market_id
                && review["status"] == "approved"
                && review["source_instrument"] == "BINANCE_SPOT:BTCUSDT"
                && review["comparison"] == "final_close_gte_open"
                && review["raw_sha256"] == evidence["raw_sha256"],
            "rule review binding"
        );
        ensure!(
            binding["schema_version"] == "marketcow.btc-hour.binding.v1"
                && binding["market_id"] == market.market_id
                && binding["condition_id"] == market.condition_id
                && binding["up_token"] == market.token_ids[0]
                && binding["down_token"] == market.token_ids[1]
                && binding["start_utc"] == review["start_utc"]
                && binding["end_utc"] == review["end_utc"]
                && binding["raw_sha256"] == evidence["raw_sha256"]
                && binding["review_sha256"] == hex::encode(Sha256::digest(&review_raw)),
            "reviewed binding identity"
        );
    }
    validate_evidence(&config.markets).await?;
    Ok((config, config_raw, config_sha, manifest_raw))
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

async fn run(package: &Path, output: &Path) -> Result<serde_json::Value> {
    ensure!(
        package.is_absolute() && output.is_absolute(),
        "absolute paths required"
    );
    ensure!(!output.exists(), "output already exists");
    let (config, config_raw, config_sha, manifest_raw) = load_package(package, Utc::now()).await?;
    let tokens = validate(&config)?;
    tokio::fs::create_dir_all(output).await?;
    tokio::fs::write(output.join("config.json"), &config_raw).await?;
    tokio::fs::write(output.join("package-manifest.json"), &manifest_raw).await?;
    let manifest_sha = hex::encode(Sha256::digest(&manifest_raw));

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
        "package_manifest_sha256":manifest_sha,
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
        "usage: btc_research_stream PACKAGE OUTPUT"
    );
    let report = run(Path::new(&arguments[1]), Path::new(&arguments[2])).await?;
    println!("{}", serde_json::to_string(&report)?);
    ensure!(report["status"] == "complete", "research stream failed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    fn config() -> Config {
        Config {
            schema_version: "marketcow.btc-hour.rust-research-stream-config.v1".into(),
            markets: vec![Market {
                market_id: "1".into(),
                condition_id: format!("0x{}", "a".repeat(64)),
                token_ids: ["10".into(), "20".into()],
                rule_evidence_path: PathBuf::from("/tmp/evidence.json"),
                rule_evidence_sha256: "b".repeat(64),
                rule_review_path: PathBuf::from("/tmp/review.json"),
                rule_review_sha256: "c".repeat(64),
                binding_path: PathBuf::from("/tmp/binding.json"),
                binding_sha256: "d".repeat(64),
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
            rule_review_path: PathBuf::from("/tmp/review-2.json"),
            rule_review_sha256: "e".repeat(64),
            binding_path: PathBuf::from("/tmp/binding-2.json"),
            binding_sha256: "f".repeat(64),
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
        let source_raw = canonical(&json!({
            "id":"1", "conditionId":format!("0x{}","a".repeat(64)),
            "clobTokenIds":"[\"10\",\"20\"]", "outcomes":"[\"Up\",\"Down\"]",
            "description":BINANCE_HOUR_RULE,
            "resolutionSource":"https://www.binance.com/en/trade/BTC_USDT"
        }))
        .unwrap();
        let raw = canonical(
            &json!({"schema_version":"marketcow.polymarket.market-evidence.v1",
            "market_id":"1","condition_id":format!("0x{}","a".repeat(64)),
            "raw_base64":STANDARD.encode(&source_raw),
            "raw_sha256":hex::encode(Sha256::digest(&source_raw)),
            "outcomes":[{"token_id":"10","outcome":"Up"},{"token_id":"20","outcome":"Down"}]}),
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
                .contains("ordered Up/Down")
        );
    }

    fn write_value(path: &Path, value: &serde_json::Value) -> Vec<u8> {
        let raw = canonical(value).unwrap();
        std::fs::write(path, &raw).unwrap();
        raw
    }

    fn package(audit: DateTime<Utc>) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        let mut markets = Vec::new();
        let hour = audit.timestamp().div_euclid(3600) * 3600;
        for index in 0..3 {
            let number = index + 1;
            let market_id = number.to_string();
            let condition_id = format!("0x{number:064x}");
            let token_ids = [(number * 10).to_string(), (number * 10 + 1).to_string()];
            let market_root = root.path().join(&market_id);
            std::fs::create_dir(&market_root).unwrap();
            let start = Utc.timestamp_opt(hour + index as i64 * 3600, 0).unwrap();
            let end = start + chrono::Duration::hours(1);
            let source_raw = canonical(&json!({
                "id":market_id, "conditionId":condition_id,
                "clobTokenIds":serde_json::to_string(&token_ids).unwrap(),
                "outcomes":"[\"Up\",\"Down\"]", "description":BINANCE_HOUR_RULE,
                "resolutionSource":"https://www.binance.com/en/trade/BTC_USDT",
                "eventStartTime":start.to_rfc3339_opts(SecondsFormat::Secs,true),
                "endDate":end.to_rfc3339_opts(SecondsFormat::Secs,true)
            }))
            .unwrap();
            let source_sha = hex::encode(Sha256::digest(&source_raw));
            let evidence_path = market_root.join("evidence.json");
            let evidence_raw = write_value(
                &evidence_path,
                &json!({
                    "schema_version":"marketcow.polymarket.market-evidence.v1",
                    "market_id":market_id,"condition_id":condition_id,
                    "raw_base64":STANDARD.encode(&source_raw),"raw_sha256":source_sha,
                    "outcomes":[{"token_id":token_ids[0],"outcome":"Up"},
                                {"token_id":token_ids[1],"outcome":"Down"}]
                }),
            );
            let review_path = market_root.join("review.json");
            let review_raw = write_value(
                &review_path,
                &json!({
                    "schema_version":"marketcow.btc-hour.rule-review.v1","market_id":market_id,
                    "raw_sha256":source_sha,"status":"approved",
                    "source_instrument":"BINANCE_SPOT:BTCUSDT","comparison":"final_close_gte_open",
                    "start_utc":start.to_rfc3339_opts(SecondsFormat::Secs,true),
                    "end_utc":end.to_rfc3339_opts(SecondsFormat::Secs,true)
                }),
            );
            let binding_path = market_root.join("binding.json");
            let binding_raw = write_value(
                &binding_path,
                &json!({
                    "schema_version":"marketcow.btc-hour.binding.v1","market_id":market_id,
                    "condition_id":condition_id,"up_token":token_ids[0],"down_token":token_ids[1],
                    "start_utc":start.to_rfc3339_opts(SecondsFormat::Secs,true),
                    "end_utc":end.to_rfc3339_opts(SecondsFormat::Secs,true),
                    "raw_sha256":source_sha,"review_sha256":hex::encode(Sha256::digest(&review_raw))
                }),
            );
            markets.push(json!({"market_id":market_id,"condition_id":condition_id,
                "token_ids":token_ids,"rule_evidence_path":evidence_path,
                "rule_evidence_sha256":hex::encode(Sha256::digest(&evidence_raw)),
                "rule_review_path":review_path,"rule_review_sha256":hex::encode(Sha256::digest(&review_raw)),
                "binding_path":binding_path,"binding_sha256":hex::encode(Sha256::digest(&binding_raw))}));
        }
        let config_path = root.path().join("stream-config.json");
        let config_raw = write_value(
            &config_path,
            &json!({
                "schema_version":"marketcow.btc-hour.rust-research-stream-config.v1","markets":markets,
                "seconds":1800,"maximum_batches":250000,"maximum_frames":250000,
                "maximum_total_bytes":536870912_u64,"maximum_batch_bytes":8388608,
                "maximum_pending_batches":8,"maximum_pending_bytes":67108864
            }),
        );
        write_value(
            &root.path().join("manifest.json"),
            &json!({
                "schema_version":"marketcow.btc-hour.research-package.v1",
                "created_at":audit.to_rfc3339_opts(SecondsFormat::Secs,true),
                "audit_clock":audit.to_rfc3339_opts(SecondsFormat::Secs,true),
                "endpoint":RESEARCH_ENDPOINT,"market_count":3,"token_count":6,
                "stream_config_path":config_path,
                "stream_config_sha256":hex::encode(Sha256::digest(&config_raw)),
                "observation_seconds":1800,"activation_performed":false,"subscription_started":false
            }),
        );
        root
    }

    #[tokio::test]
    async fn complete_package_is_required_and_expires() {
        let audit = Utc.with_ymd_and_hms(2026, 9, 12, 10, 58, 0).unwrap();
        let root = package(audit);
        let (config, _, _, _) = load_package(root.path(), audit + chrono::Duration::seconds(2))
            .await
            .unwrap();
        assert_eq!(config.markets.len(), 3);
        let expired = load_package(root.path(), audit + chrono::Duration::seconds(301))
            .await
            .unwrap_err();
        assert!(expired.to_string().contains("expired"));
        std::fs::remove_file(root.path().join("manifest.json")).unwrap();
        assert!(load_package(root.path(), Utc::now()).await.is_err());
    }

    #[tokio::test]
    async fn crossing_utc_hour_requires_fresh_package_even_within_five_minutes() {
        let audit = Utc.with_ymd_and_hms(2026, 9, 12, 10, 59, 58).unwrap();
        let root = package(audit);
        let error = load_package(
            root.path(),
            Utc.with_ymd_and_hms(2026, 9, 12, 11, 0, 2).unwrap(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("UTC hour expired"));
    }

    #[tokio::test]
    async fn ordered_outcome_binding_cannot_be_swapped() {
        let audit = Utc.with_ymd_and_hms(2026, 9, 12, 10, 58, 0).unwrap();
        let root = package(audit);
        let path = root.path().join("1/evidence.json");
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["outcomes"].as_array_mut().unwrap().swap(0, 1);
        let raw = write_value(&path, &value);
        let mut config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.path().join("stream-config.json")).unwrap())
                .unwrap();
        config["markets"][0]["rule_evidence_sha256"] = hex::encode(Sha256::digest(&raw)).into();
        let config_raw = write_value(&root.path().join("stream-config.json"), &config);
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.path().join("manifest.json")).unwrap())
                .unwrap();
        manifest["stream_config_sha256"] = hex::encode(Sha256::digest(&config_raw)).into();
        write_value(&root.path().join("manifest.json"), &manifest);
        let error = load_package(root.path(), audit + chrono::Duration::seconds(2))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("ordered Up/Down"));
    }
}
