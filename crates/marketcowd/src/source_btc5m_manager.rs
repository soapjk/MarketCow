//! Autonomous, bounded BTC five-minute market rotation inside the unified
//! MarketCow process.  It discovers and audits one new `next` market per UTC
//! boundary, prepares metadata without subscribing it, then uses the shared
//! acquisition lease/profile CAS path.  It never owns a supplier socket.
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use marketcow_polymarket::discovery_source::canonical_hash;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::source_btc5m_api::{MarketBinding, OutcomeBinding, Profile, WindowRole};

const RULE_DESCRIPTION_SHA256: &str =
    "485ceb1dabc4aa12fb42c76184563b7378f01e5de9ded73df049c0d191cd5ad1";
const RESOLUTION_SOURCE: &str = "https://data.chain.link/streams/btc-usd-twap-60s-streams";
const LEASE_ID: &str = "marketcow-btc5m-manager-v1";
const PROFILE_SCHEMA: &str = "marketcow.polymarket.btc-5m-profile.v1";
const STATUS_SCHEMA: &str = "marketcow.polymarket.btc-5m-manager-status.v1";
const BOOTSTRAP_PROFILE: &str = "last-profile.json";
const CURRENT_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
// One candidate review performs two ordered supplier reads because the CLOB
// condition is learned from Gamma. Each has a 15-second timeout and the
// contract deliberately spaces them by one second. `prereview` is the desired
// completed-review margin, not the time at which network I/O should begin.
const CANDIDATE_FETCH_BUDGET_SECONDS: u64 = 31;

#[derive(Clone)]
pub struct Config {
    pub root: PathBuf,
    pub control_socket: PathBuf,
    pub ttl: Duration,
    pub renew: Duration,
    pub prereview: Duration,
    pub control_timeout: Duration,
    pub response_bytes: usize,
    pub evidence_files: usize,
}

pub struct Bootstrap {
    pub profile: Profile,
    candidates: [Candidate; 2],
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.root.is_absolute() && self.control_socket.is_absolute(),
            "BTC 5m manager paths"
        );
        ensure!(
            self.ttl == Duration::from_secs(120)
                && self.renew == Duration::from_secs(40)
                && self.prereview == Duration::from_secs(30)
                && self.control_timeout == Duration::from_secs(10),
            "BTC 5m manager fixed timing profile"
        );
        ensure!(
            (64 * 1024..=1024 * 1024).contains(&self.response_bytes)
                && (288..=4032).contains(&self.evidence_files),
            "BTC 5m manager evidence budget"
        );
        Ok(())
    }
}

#[derive(Clone)]
struct Candidate {
    binding: MarketBinding,
    record: Value,
    evidence: Value,
}

fn at(seconds: i64) -> Result<DateTime<Utc>> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .context("BTC 5m timestamp")
}
fn window_start(now: DateTime<Utc>) -> i64 {
    now.timestamp().div_euclid(300) * 300
}
fn require_current_window(expected_start: i64, now: DateTime<Utc>) -> Result<()> {
    ensure!(
        window_start(now) == expected_start,
        "BTC 5m UTC boundary crossed during candidate preparation"
    );
    Ok(())
}
fn window_times(start: i64) -> Result<(String, String)> {
    Ok((
        at(start)?.to_rfc3339_opts(SecondsFormat::Secs, true),
        at(start + 300)?.to_rfc3339_opts(SecondsFormat::Secs, true),
    ))
}
fn parse_string_array(value: &Value, key: &str) -> Result<Vec<String>> {
    let result: Vec<String> = serde_json::from_str(
        value[key]
            .as_str()
            .with_context(|| format!("BTC 5m missing {key}"))?,
    )?;
    ensure!(
        !result.is_empty() && result.iter().all(|item| !item.is_empty()),
        "BTC 5m empty identity array"
    );
    Ok(result)
}
fn decimal(value: &Value, key: &str) -> Result<String> {
    let raw = value[key]
        .as_str()
        .map(str::to_owned)
        .or_else(|| value[key].as_number().map(ToString::to_string))
        .with_context(|| format!("BTC 5m missing {key}"))?;
    Ok(raw
        .parse::<rust_decimal::Decimal>()?
        .normalize()
        .to_string())
}

fn project_candidate(
    start: i64,
    gamma_raw: &[u8],
    execution_raw: &[u8],
    observed_at: &str,
    require_active: bool,
) -> Result<Candidate> {
    ensure!(
        gamma_raw.len() <= 262_144 && execution_raw.len() <= 262_144,
        "BTC 5m source response capacity"
    );
    DateTime::parse_from_rfc3339(observed_at)?;
    let gamma: Value = serde_json::from_slice(gamma_raw)?;
    let slug = format!("btc-updown-5m-{start}");
    let (start_at, end_at) = window_times(start)?;
    ensure!(gamma["slug"] == slug, "BTC 5m slug identity");
    ensure!(
        gamma["eventStartTime"] == start_at && gamma["endDate"] == end_at,
        "BTC 5m source window identity"
    );
    let description = gamma["description"]
        .as_str()
        .context("BTC 5m complete description")?;
    ensure!(
        hex::encode(Sha256::digest(description.as_bytes())) == RULE_DESCRIPTION_SHA256
            && gamma["resolutionSource"] == RESOLUTION_SOURCE,
        "BTC 5m reviewed Chainlink rule changed"
    );
    ensure!(
        gamma["enableOrderBook"] == true,
        "BTC 5m order book disabled"
    );
    if require_active {
        ensure!(
            gamma["active"] == true && gamma["closed"] == false && gamma["acceptingOrders"] == true,
            "BTC 5m market is not order-book active"
        );
    }
    let market_id = gamma["id"].as_str().context("BTC 5m market id")?;
    let condition = gamma["conditionId"].as_str().context("BTC 5m condition")?;
    ensure!(
        !market_id.is_empty()
            && market_id.bytes().all(|byte| byte.is_ascii_digit())
            && condition.len() == 66
            && condition.starts_with("0x")
            && condition[2..].bytes().all(|byte| byte.is_ascii_hexdigit()),
        "BTC 5m source identity"
    );
    let outcomes = parse_string_array(&gamma, "outcomes")?;
    let tokens = parse_string_array(&gamma, "clobTokenIds")?;
    ensure!(
        outcomes == ["Up", "Down"] && tokens.len() == 2 && tokens[0] != tokens[1],
        "BTC 5m ordered Up/Down tokens"
    );
    let execution: Value = serde_json::from_slice(execution_raw)?;
    ensure!(execution["c"] == condition, "BTC 5m execution condition");
    let execution_tokens = execution["t"]
        .as_array()
        .context("BTC 5m execution tokens")?;
    ensure!(
        execution_tokens.len() == 2
            && execution_tokens[0]["o"] == "Up"
            && execution_tokens[0]["t"] == tokens[0]
            && execution_tokens[1]["o"] == "Down"
            && execution_tokens[1]["t"] == tokens[1],
        "BTC 5m execution ordered identity"
    );
    let gamma_sha = hex::encode(Sha256::digest(gamma_raw));
    let execution_sha = hex::encode(Sha256::digest(execution_raw));
    let rule_sha = canonical_hash(&json!({"description":description,
        "resolution_source":RESOLUTION_SOURCE,"start_at":start_at,"end_at":end_at,
        "outcomes":["Up","Down"]}));
    let price_increment = decimal(&execution, "mts")?;
    let minimum_order_size = decimal(&execution, "mos")?;
    let event_id = gamma["events"]
        .as_array()
        .and_then(|events| events.first())
        .and_then(|event| event["id"].as_str())
        .unwrap_or("");
    let record = json!({
        "identity":{"market_id":market_id,"event_id":event_id,"condition_id":condition,
            "neg_risk":gamma.get("negRisk").and_then(Value::as_bool).unwrap_or(false),
            "outcomes":[{"outcome":"Up","token_id":tokens[0]},
                {"outcome":"Down","token_id":tokens[1]}]},
        "metadata_revision":gamma_sha,"active":gamma["active"],"closed":gamma["closed"],
        "accepting_orders":gamma["acceptingOrders"],
        "lifecycle_state":if gamma["closed"]==true {"closed"} else {"active"},
        "start_at":start_at,"end_at":end_at,"relations":[],
        "rules":{"rules_complete":true,
            "rule_version":rule_sha,
            "instrument":{"complete":false,"price_increment":price_increment,
                "minimum_order_size":minimum_order_size,"size_increment":null,
                "revision":execution_sha,"provenance":["polymarket_clob_v2"]},
            "fee_schedule":{"complete":false,"schedule_id":execution_sha,
                "source":"polymarket_clob_v2","raw_fee_rate":execution["fd"]["r"],
                "raw_fee_exponent":execution["fd"]["e"]}}
    });
    let binding = MarketBinding {
        role: WindowRole::Next,
        window_id: slug.clone(),
        start_at,
        end_at,
        market_id: market_id.to_owned(),
        condition_id: condition.to_owned(),
        outcomes: [
            OutcomeBinding {
                outcome: "Up".into(),
                token_id: tokens[0].clone(),
            },
            OutcomeBinding {
                outcome: "Down".into(),
                token_id: tokens[1].clone(),
            },
        ],
        rule_sha256: rule_sha.clone(),
    };
    Ok(Candidate {
        binding,
        record: record.clone(),
        evidence: json!({"schema_version":"marketcow.polymarket.btc-5m-manager-evidence.v1",
            "slug":slug,"observed_at":observed_at,
            "gamma":{"url":format!("https://gamma-api.polymarket.com/markets/slug/{slug}"),
                "bytes":gamma_raw.len(),"sha256":gamma_sha,"raw_base64":STANDARD.encode(gamma_raw)},
            "execution":{"url":format!("https://clob.polymarket.com/clob-markets/{condition}"),
                "bytes":execution_raw.len(),"sha256":execution_sha,"raw_base64":STANDARD.encode(execution_raw)},
            "rule_sha256":rule_sha,"record":record}),
    })
}

fn profile_from_candidates(
    catalog_revision: &str,
    revision: u64,
    start: i64,
    candidates: &[Candidate; 2],
) -> Result<Profile> {
    let mut markets = Vec::with_capacity(2);
    for (candidate, role, expected) in [
        (&candidates[0], WindowRole::Current, start),
        (&candidates[1], WindowRole::Next, start + 300),
    ] {
        ensure!(
            candidate.binding.start_at == window_times(expected)?.0,
            "BTC 5m candidate window ordering"
        );
        let mut binding = candidate.binding.clone();
        binding.role = role;
        markets.push(binding);
    }
    let mut profile = Profile {
        schema_version: PROFILE_SCHEMA.into(),
        profile_id: String::new(),
        profile_revision: revision,
        catalog_revision: catalog_revision.into(),
        markets,
        maximum_markets: 3,
        maximum_quote_age_ms: 5_000,
        maximum_tokens: 6,
    };
    profile.profile_id = profile.expected_id();
    profile.validate()?;
    Ok(profile)
}

async fn body(client: &reqwest::Client, url: &str, limit: usize) -> Result<Vec<u8>> {
    let mut response = client.get(url).send().await?;
    ensure!(response.status().as_u16() == 200, "BTC 5m upstream status");
    let mut result = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(chunk.len() <= limit - result.len(), "BTC 5m upstream bytes");
        result.extend_from_slice(&chunk);
    }
    Ok(result)
}
async fn fetch_candidate(
    client: &reqwest::Client,
    start: i64,
    maximum_bytes: usize,
    require_active: bool,
) -> Result<Candidate> {
    let slug = format!("btc-updown-5m-{start}");
    let gamma = body(
        client,
        &format!("https://gamma-api.polymarket.com/markets/slug/{slug}"),
        maximum_bytes,
    )
    .await?;
    let value: Value = serde_json::from_slice(&gamma)?;
    let condition = value["conditionId"]
        .as_str()
        .context("BTC 5m discovered condition")?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let execution = body(
        client,
        &format!("https://clob.polymarket.com/clob-markets/{condition}"),
        maximum_bytes,
    )
    .await?;
    project_candidate(
        start,
        &gamma,
        &execution,
        &Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        require_active,
    )
}

fn atomic_evidence(root: &Path, candidate: &Candidate, maximum_files: usize) -> Result<()> {
    fs::create_dir_all(root)?;
    fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
    let name = format!(
        "{}-{}.json",
        candidate.binding.start_at.replace([':', '-'], ""),
        candidate.evidence["gamma"]["sha256"]
            .as_str()
            .context("BTC 5m archived Gamma hash")?
            .get(..16)
            .context("BTC 5m archived Gamma hash length")?
    );
    let target = root.join(name);
    if !target.exists() {
        let bytes = serde_json::to_vec(&candidate.evidence)?;
        let temporary = root.join(format!(".{}.tmp", uuid::Uuid::new_v4().simple()));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &target)?;
        FileSync::directory(root)?;
    }
    let mut entries = fs::read_dir(root)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());
    while entries.len() > maximum_files {
        fs::remove_file(entries.remove(0).path())?;
    }
    Ok(())
}

fn atomic_profile(root: &Path, profile: &Profile) -> Result<()> {
    fs::create_dir_all(root)?;
    fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
    let bytes = serde_json::to_vec(profile)?;
    let target = root.join(BOOTSTRAP_PROFILE);
    let temporary = root.join(format!(".{}.tmp", uuid::Uuid::new_v4().simple()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, target)?;
    FileSync::directory(root)
}

pub fn load_persisted_profile(config: &Config, catalog_revision: &str) -> Result<Option<Profile>> {
    let path = config.root.join(BOOTSTRAP_PROFILE);
    if !path.exists() {
        return Ok(None);
    }
    ensure!(
        fs::metadata(&path)?.len() <= 1024 * 1024,
        "BTC 5m persisted profile bytes"
    );
    let profile: Profile = serde_json::from_slice(&fs::read(path)?)?;
    profile.validate()?;
    ensure!(
        profile.catalog_revision == catalog_revision,
        "BTC 5m persisted profile catalog revision"
    );
    Ok(Some(profile))
}

fn candidate_from_evidence(path: &Path, maximum_bytes: usize) -> Result<Candidate> {
    ensure!(
        fs::metadata(path)?.len() <= maximum_bytes.saturating_mul(4) as u64,
        "BTC 5m persisted evidence bytes"
    );
    let evidence: Value = serde_json::from_slice(&fs::read(path)?)?;
    ensure!(
        evidence["schema_version"] == "marketcow.polymarket.btc-5m-manager-evidence.v1",
        "BTC 5m persisted evidence schema"
    );
    let decode = |key: &str| -> Result<Vec<u8>> {
        let section = &evidence[key];
        let raw = STANDARD.decode(
            section["raw_base64"]
                .as_str()
                .with_context(|| format!("BTC 5m persisted {key} raw"))?,
        )?;
        ensure!(
            raw.len() == section["bytes"].as_u64().context("BTC 5m evidence bytes")? as usize
                && raw.len() <= maximum_bytes
                && hex::encode(Sha256::digest(&raw)) == section["sha256"],
            "BTC 5m persisted evidence content identity"
        );
        Ok(raw)
    };
    let slug = evidence["slug"]
        .as_str()
        .context("BTC 5m persisted evidence slug")?;
    let start = slug
        .strip_prefix("btc-updown-5m-")
        .context("BTC 5m persisted evidence slug prefix")?
        .parse()?;
    let candidate = project_candidate(
        start,
        &decode("gamma")?,
        &decode("execution")?,
        evidence["observed_at"]
            .as_str()
            .context("BTC 5m persisted evidence observation")?,
        true,
    )?;
    ensure!(
        candidate.evidence == evidence,
        "BTC 5m persisted evidence projection differs"
    );
    Ok(candidate)
}

fn load_persisted_candidates(config: &Config, profile: &Profile) -> Result<Option<[Candidate; 2]>> {
    profile.validate_current_at(Utc::now())?;
    let mut entries = fs::read_dir(&config.root)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.file_type().is_ok_and(|kind| kind.is_file())
                && entry.file_name() != BOOTSTRAP_PROFILE
        })
        .collect::<Vec<_>>();
    ensure!(
        entries.len() <= config.evidence_files,
        "BTC 5m persisted evidence file capacity"
    );
    entries.sort_by_key(|entry| entry.file_name());
    let mut matched = std::collections::BTreeMap::<String, Candidate>::new();
    for entry in entries {
        let candidate = match candidate_from_evidence(&entry.path(), config.response_bytes) {
            Ok(candidate) => candidate,
            Err(_) => continue,
        };
        if profile
            .markets
            .iter()
            .any(|binding| binding.market_id == candidate.binding.market_id)
        {
            matched.insert(candidate.binding.market_id.clone(), candidate);
        }
    }
    let mut ordered = Vec::with_capacity(2);
    for role in [WindowRole::Current, WindowRole::Next] {
        let binding = profile
            .markets
            .iter()
            .find(|binding| binding.role == role)
            .context("BTC 5m persisted profile window role")?;
        let Some(candidate) = matched.remove(&binding.market_id) else {
            return Ok(None);
        };
        ordered.push(candidate);
    }
    let candidates: [Candidate; 2] = ordered
        .try_into()
        .map_err(|_| anyhow::anyhow!("BTC 5m persisted candidate count"))?;
    let start = profile
        .markets
        .iter()
        .find(|market| market.role == WindowRole::Current)
        .and_then(|market| DateTime::parse_from_rfc3339(&market.start_at).ok())
        .context("BTC 5m persisted current start")?
        .timestamp();
    ensure!(
        profile_from_candidates(
            &profile.catalog_revision,
            profile.profile_revision,
            start,
            &candidates,
        )?
        .profile_id
            == profile.profile_id,
        "BTC 5m persisted candidates differ from profile"
    );
    Ok(Some(candidates))
}

fn load_persisted_window_candidates(config: &Config, start: i64) -> Result<Option<[Candidate; 2]>> {
    let expected = [window_times(start)?.0, window_times(start + 300)?.0];
    let mut entries = fs::read_dir(&config.root)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.file_type().is_ok_and(|kind| kind.is_file())
                && entry.file_name() != BOOTSTRAP_PROFILE
        })
        .collect::<Vec<_>>();
    ensure!(
        entries.len() <= config.evidence_files,
        "BTC 5m persisted evidence file capacity"
    );
    entries.sort_by_key(|entry| entry.file_name());
    let mut matched =
        std::collections::BTreeMap::<String, (chrono::DateTime<Utc>, Candidate)>::new();
    for entry in entries {
        let candidate = match candidate_from_evidence(&entry.path(), config.response_bytes) {
            Ok(candidate) => candidate,
            Err(_) => continue,
        };
        if !expected.contains(&candidate.binding.start_at) {
            continue;
        }
        let observed = chrono::DateTime::parse_from_rfc3339(
            candidate.evidence["observed_at"]
                .as_str()
                .context("BTC 5m persisted evidence observed_at")?,
        )?
        .with_timezone(&Utc);
        let replace = matched
            .get(&candidate.binding.start_at)
            .is_none_or(|(current, _)| observed > *current);
        if replace {
            matched.insert(candidate.binding.start_at.clone(), (observed, candidate));
        }
    }
    let mut ordered = Vec::with_capacity(2);
    for start_at in expected {
        let Some((_, candidate)) = matched.remove(&start_at) else {
            return Ok(None);
        };
        ordered.push(candidate);
    }
    Ok(Some(ordered.try_into().map_err(|_| {
        anyhow::anyhow!("BTC 5m persisted window candidate count")
    })?))
}
struct FileSync;
impl FileSync {
    fn directory(path: &Path) -> Result<()> {
        fs::File::open(path)?.sync_all()?;
        Ok(())
    }
}

async fn control(config: &Config, request: Value) -> Result<Value> {
    let bytes = serde_json::to_vec(&request)?;
    ensure!(
        bytes.len() <= config.response_bytes,
        "BTC 5m control request bytes"
    );
    let operation = async {
        let mut socket = tokio::net::UnixStream::connect(&config.control_socket).await?;
        socket.write_u32(bytes.len() as u32).await?;
        socket.write_all(&bytes).await?;
        let length = socket.read_u32().await? as usize;
        ensure!(
            length <= config.response_bytes,
            "BTC 5m control response bytes"
        );
        let mut response = vec![0; length];
        socket.read_exact(&mut response).await?;
        let value: Value = serde_json::from_slice(&response)?;
        ensure!(
            value["ok"] == true,
            "BTC 5m control rejected: {}",
            value["error"]
        );
        Ok::<_, anyhow::Error>(value["result"].clone())
    };
    tokio::time::timeout(config.control_timeout, operation)
        .await
        .context("BTC 5m control timeout")?
}
fn scope_identity(status: &Value) -> Result<(String, u64)> {
    let id = status
        .get("scope_id")
        .or_else(|| status.get("projection_id"))
        .and_then(Value::as_str)
        .context("BTC 5m control scope identity")?;
    Ok((
        id.to_owned(),
        status["revision"]
            .as_u64()
            .context("BTC 5m control scope revision")?,
    ))
}
async fn prepare(config: &Config, profile: &Profile, candidates: &[Candidate; 2]) -> Result<()> {
    let status = control(config, json!({"operation":"status"})).await?;
    let (scope_id, revision) = scope_identity(&status)?;
    let records = candidates
        .iter()
        .map(|candidate| candidate.record.clone())
        .collect::<Vec<_>>();
    let mut market_ids = profile.market_ids_owned().into_iter().collect::<Vec<_>>();
    market_ids.sort();
    control(
        config,
        json!({"operation":"prepare_acquisition","expected_scope_id":scope_id,
            "expected_revision":revision,"catalog_revision":profile.catalog_revision,
            "records":records,"evidence_sha256":canonical_hash(&json!(records)),
            "acquisition_market_ids":market_ids}),
    )
    .await?;
    Ok(())
}
async fn publish(config: &Config, incumbent: &Profile, profile: &Profile) -> Result<Value> {
    control(
        config,
        json!({"operation":"publish_btc5m_profile",
        "expected_profile_id":incumbent.profile_id,"expected_revision":incumbent.profile_revision,
        "lease_id":LEASE_ID,"lease_ttl_seconds":config.ttl.as_secs(),"profile":profile}),
    )
    .await
}
async fn renew(config: &Config) -> Result<Value> {
    let status = control(config, json!({"operation":"status"})).await?;
    let (scope_id, revision) = scope_identity(&status)?;
    control(
        config,
        json!({"operation":"renew_acquisition_lease","expected_scope_id":scope_id,
        "expected_revision":revision,"lease_id":LEASE_ID,"ttl_seconds":config.ttl.as_secs()}),
    )
    .await
}
async fn release(config: &Config) -> Result<Value> {
    let status = control(config, json!({"operation":"status"})).await?;
    let (scope_id, revision) = scope_identity(&status)?;
    control(
        config,
        json!({"operation":"release_acquisition_lease","expected_scope_id":scope_id,
        "expected_revision":revision,"lease_id":LEASE_ID}),
    )
    .await
}

/// The control worker selects only unreferenced hot admissions. Its owner
/// validates the lease union again after this request crosses the socket.
/// Unknown/unfinished effects retain the same durable retirement ticket and
/// are reconciled by subsequent control calls, never retried with a new ID.
async fn retire_unreferenced(config: &Config) -> Result<usize> {
    let candidates = control(config, json!({"operation":"retirement_candidates"})).await?;
    if !candidates["pending_ticket"].is_null() {
        return Ok(0);
    }
    let markets = candidates["market_ids"].as_array()
        .context("BTC 5m retirement candidate response")?;
    if markets.is_empty() {
        return Ok(0);
    }
    let status = control(config, json!({"operation":"status"})).await?;
    let (scope_id, revision) = scope_identity(&status)?;
    let receipt = control(config, json!({"operation":"retire_acquisition",
        "expected_scope_id":scope_id,"expected_revision":revision,
        "market_ids":markets})).await?;
    ensure!(receipt["retirement_queued"] == true,
        "BTC 5m retirement was not queued");
    Ok(markets.len())
}

async fn refresh_current(config: &Config) -> Result<Value> {
    control(
        config,
        json!({"operation":"refresh_btc5m_current","lease_id":LEASE_ID}),
    )
    .await
}

fn manager_lease_remaining_from(status: &Value) -> Result<Option<u64>> {
    let leases = status["leases"]
        .as_array()
        .context("BTC 5m acquisition lease status")?;
    let matching = leases
        .iter()
        .filter(|lease| lease["lease_id"] == LEASE_ID)
        .collect::<Vec<_>>();
    ensure!(matching.len() <= 1, "duplicate BTC 5m manager lease");
    matching
        .first()
        .map(|lease| {
            lease["remaining_milliseconds"]
                .as_u64()
                .context("BTC 5m lease remaining duration")
        })
        .transpose()
}
async fn manager_lease_remaining(config: &Config) -> Result<Option<u64>> {
    let status = control(config, json!({"operation":"acquisition_lease_status"})).await?;
    manager_lease_remaining_from(&status)
}

async fn renewal_worker(
    config: Config,
    control_view: Option<crate::source_btc5m_api::Control>,
    lease_healthy: tokio::sync::watch::Sender<bool>,
    period: Duration,
) {
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let healthy = match renew(&config).await {
            Ok(_) => true,
            Err(_) => manager_lease_remaining(&config)
                .await
                .ok()
                .flatten()
                .is_some_and(|remaining| remaining > period.as_millis() as u64),
        };
        lease_healthy.send_replace(healthy);
        if let Some(control_view) = &control_view {
            if healthy {
                if let Err(error) = control_view.note_manager_renewal_at(Utc::now()) {
                    eprintln!("{}", json!({"stage":"btc_5m_manager_heartbeat_failed",
                        "detail":error.to_string()}));
                }
            } else {
                let profile = control_view.profile().ok();
                let mut status = manager_status(
                    "fail_closed",
                    profile.as_ref(),
                    Some("BTC 5m lease renewal not durably confirmed"),
                );
                status["blocked_reason_code"] = json!("lease_invalid");
                let _ = control_view.set_manager_status(status);
            }
        }
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn discover_set(
    client: &reqwest::Client,
    config: &Config,
    start: i64,
) -> Result<[Candidate; 2]> {
    let mut values = Vec::new();
    for window in [start, start + 300] {
        let candidate = fetch_candidate(client, window, config.response_bytes, true).await?;
        let archived = candidate.clone();
        let root = config.root.clone();
        let retention = config.evidence_files;
        tokio::task::spawn_blocking(move || atomic_evidence(&root, &archived, retention)).await??;
        values.push(candidate);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    values
        .try_into()
        .map_err(|_| anyhow::anyhow!("BTC 5m candidate count"))
}

fn promote_reviewed(
    active: Option<[Candidate; 2]>,
    reviewed: Option<Candidate>,
    start: i64,
) -> Option<[Candidate; 2]> {
    let active = active?;
    let next = reviewed?;
    if next.binding.start_at == window_times(start + 300).ok()?.0
        && active[0].binding.start_at == window_times(start - 300).ok()?.0
        && active[1].binding.start_at == window_times(start).ok()?.0
    {
        Some([active[1].clone(), next])
    } else {
        None
    }
}

/// A missed boundary cannot be repaired by pretending the fresh current is
/// the incumbent next. The normal profile CAS remains strictly adjacent;
/// only an explicitly absent manager lease permits restart reconciliation.
fn missed_adjacent_window(incumbent: &Profile, start: i64) -> Result<bool> {
    let next = incumbent.markets.iter().find(|market| market.role == WindowRole::Next)
        .context("BTC 5m incumbent next")?;
    let next_start = DateTime::parse_from_rfc3339(&next.start_at)
        .context("BTC 5m incumbent next start")?.timestamp();
    ensure!(start >= next_start, "BTC 5m candidate precedes incumbent next");
    Ok(start > next_start)
}

fn prereview_due(seconds_into_window: u64, completed_margin: Duration) -> bool {
    let remaining = 300_u64.saturating_sub(seconds_into_window);
    remaining > completed_margin.as_secs()
        && remaining
            <= completed_margin
                .as_secs()
                .saturating_add(CANDIDATE_FETCH_BUDGET_SECONDS)
}

fn manager_status(phase: &str, profile: Option<&Profile>, error: Option<&str>) -> Value {
    json!({"schema_version":STATUS_SCHEMA,"owner":"same_process_unified_marketcow",
        "lease_id":LEASE_ID,"lease_ttl_seconds":120,"renew_interval_seconds":40,
        "prereview_seconds":30,"control_timeout_seconds":10,"phase":phase,
        "profile_id":profile.map(|value|value.profile_id.clone()),
        "profile_revision":profile.map(|value|value.profile_revision),
        "error":error,"updated_at":Utc::now().to_rfc3339_opts(SecondsFormat::Millis,true)})
}

fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?)
}

/// Create the first current+next profile before the shared HTTP router is
/// built. This is still the same collector process and the same audited
/// manager path; it removes any need to deploy a hand-maintained rotating
/// profile file.
pub async fn bootstrap(config: &Config, catalog_revision: &str) -> Result<Bootstrap> {
    config.validate()?;
    ensure!(
        catalog_revision.len() == 64
            && catalog_revision
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()),
        "BTC 5m bootstrap catalog revision"
    );
    let start = window_start(Utc::now());
    let candidates = discover_set(&client()?, config, start).await?;
    // Both bounded source requests can span a five-minute boundary. Never
    // materialize that reviewed-but-now-expired profile as the process seed.
    require_current_window(start, Utc::now())?;
    let profile = profile_from_candidates(catalog_revision, 1, start, &candidates)?;
    atomic_profile(&config.root, &profile)?;
    Ok(Bootstrap {
        profile,
        candidates,
    })
}

pub fn start(
    config: Config,
    control_view: crate::source_btc5m_api::Control,
    bootstrap: Option<Bootstrap>,
) -> Result<tokio::task::JoinHandle<()>> {
    config.validate()?;
    let client = client()?;
    let mut active_candidates = bootstrap.map(|value| value.candidates);
    if active_candidates.is_none() {
        match load_persisted_candidates(&config, &control_view.profile()?) {
            Ok(candidates) => active_candidates = candidates,
            Err(error) => eprintln!(
                "{}",
                json!({"stage":"btc_5m_persisted_candidates_rejected","detail":error.to_string()})
            ),
        }
    }
    Ok(tokio::spawn(async move {
        let initial_profile = control_view.profile().ok();
        let _ = control_view.set_manager_status(manager_status(
            "reconciling",
            initial_profile.as_ref(),
            None,
        ));
        let (lease_healthy, lease_health) = tokio::sync::watch::channel(false);
        let _renewal = AbortOnDrop(tokio::spawn(renewal_worker(
            config.clone(),
            Some(control_view.clone()),
            lease_healthy.clone(),
            config.renew,
        )));
        let _freshness = AbortOnDrop(tokio::spawn(freshness_worker(
            config.clone(),
            CURRENT_REFRESH_INTERVAL,
        )));
        let mut reviewed_next: Option<Candidate> = None;
        let mut prereview_attempted_for: Option<i64> = None;
        let mut retirement_polled_at = tokio::time::Instant::now()
            .checked_sub(Duration::from_secs(10)).unwrap_or_else(tokio::time::Instant::now);
        loop {
            let result: Result<()> = async {
                let now = Utc::now();
                let start = window_start(now);
                let incumbent = control_view.profile()?;
                let current_start = incumbent
                    .markets
                    .iter()
                    .find(|market| market.role == WindowRole::Current)
                    .and_then(|market| DateTime::parse_from_rfc3339(&market.start_at).ok())
                    .map(|value| value.timestamp());
                if current_start != Some(start) {
                    control_view.set_manager_status(manager_status(
                        "discovering",
                        Some(&incumbent),
                        None,
                    ))?;
                    let promoted =
                        promote_reviewed(active_candidates.take(), reviewed_next.take(), start);
                    let candidates = if let Some(candidates) = promoted {
                        candidates
                    } else {
                        let persisted_config = config.clone();
                        match tokio::task::spawn_blocking(move || {
                            load_persisted_window_candidates(&persisted_config, start)
                        })
                        .await??
                        {
                            Some(candidates) => candidates,
                            None => discover_set(&client, &config, start).await?,
                        }
                    };
                    let candidate = profile_from_candidates(
                        &incumbent.catalog_revision,
                        incumbent
                            .profile_revision
                            .checked_add(1)
                            .context("BTC 5m profile revision")?,
                        start,
                        &candidates,
                    )?;
                    require_current_window(start, Utc::now())?;
                    prepare(&config, &candidate, &candidates).await?;
                    require_current_window(start, Utc::now())?;
                    if missed_adjacent_window(&incumbent, start)? {
                        // The manager missed at least one full UTC window.
                        // Release only its own component lease and prove its
                        // absence before using the distinct restart-reconcile
                        // CAS. Every other consumer lease remains intact.
                        if manager_lease_remaining(&config).await?.is_some() {
                            let _ = release(&config).await?;
                        }
                        ensure!(manager_lease_remaining(&config).await?.is_none(),
                            "BTC 5m missed-window lease release pending");
                        lease_healthy.send_replace(false);
                        require_current_window(start, Utc::now())?;
                    }
                    let receipt = publish(&config, &incumbent, &candidate).await?;
                    ensure!(
                        receipt["profile_published"] != false,
                        "BTC 5m wire replacement pending"
                    );
                    control_view.set_manager_status(manager_status(
                        "running",
                        Some(&candidate),
                        None,
                    ))?;
                    atomic_profile(&config.root, &candidate)?;
                    lease_healthy.send_replace(true);
                    active_candidates = Some(candidates);
                    reviewed_next = None;
                    prereview_attempted_for = None;
                    return Ok(());
                }
                if active_candidates.is_none() {
                    let candidates = discover_set(&client, &config, start).await?;
                    let observed = profile_from_candidates(
                        &incumbent.catalog_revision,
                        incumbent.profile_revision,
                        start,
                        &candidates,
                    )?;
                    require_current_window(start, Utc::now())?;
                    let identity = |profile: &Profile| {
                        profile
                            .markets
                            .iter()
                            .map(|market| {
                                (
                                    market.role.clone(),
                                    market.market_id.clone(),
                                    market.condition_id.clone(),
                                    market.outcomes.clone(),
                                    market.rule_sha256.clone(),
                                )
                            })
                            .collect::<BTreeSet<_>>()
                    };
                    ensure!(
                        identity(&observed) == identity(&incumbent),
                        "BTC 5m running profile differs from fresh reviewed source"
                    );
                    active_candidates = Some(candidates);
                }
                if !*lease_health.borrow() {
                    match manager_lease_remaining(&config).await? {
                        Some(_) => {
                            renew(&config).await?;
                            lease_healthy.send_replace(true);
                        }
                        None => {
                            let candidates = active_candidates
                                .clone()
                                .context("BTC 5m restart candidates unavailable")?;
                            let candidate = profile_from_candidates(
                                &incumbent.catalog_revision,
                                incumbent
                                    .profile_revision
                                    .checked_add(1)
                                    .context("BTC 5m profile revision")?,
                                start,
                                &candidates,
                            )?;
                            require_current_window(start, Utc::now())?;
                            prepare(&config, &candidate, &candidates).await?;
                            require_current_window(start, Utc::now())?;
                            publish(&config, &incumbent, &candidate).await?;
                            atomic_profile(&config.root, &candidate)?;
                            lease_healthy.send_replace(true);
                        }
                    }
                    control_view.set_manager_status(manager_status(
                        "running",
                        Some(&incumbent),
                        None,
                    ))?;
                }
                let seconds = now.timestamp().rem_euclid(300) as u64;
                if prereview_due(seconds, config.prereview)
                    && reviewed_next.is_none()
                    && prereview_attempted_for != Some(start)
                {
                    // At most one bounded attempt per current window. A fast
                    // failure must not immediately start another 31-second
                    // request in the critical boundary interval.
                    prereview_attempted_for = Some(start);
                    let candidate =
                        fetch_candidate(&client, start + 600, config.response_bytes, true).await?;
                    require_current_window(start, Utc::now())?;
                    let archived = candidate.clone();
                    let root = config.root.clone();
                    let retention = config.evidence_files;
                    tokio::task::spawn_blocking(move || {
                        atomic_evidence(&root, &archived, retention)
                    })
                    .await??;
                    reviewed_next = Some(candidate);
                    control_view.set_manager_status(manager_status(
                        "next_reviewed",
                        Some(&incumbent),
                        None,
                    ))?;
                }
                Ok(())
            }
            .await;
            if let Err(error) = result {
                let rendered = format!("{error:#}");
                if rendered.contains("publish crossed boundary after acquisition") {
                    // A second bounded release is idempotent and also resolves
                    // an uncertain response from the control-side release.
                    let _ = release(&config).await;
                    lease_healthy.send_replace(false);
                    active_candidates = None;
                    reviewed_next = None;
                    prereview_attempted_for = None;
                }
                let profile = control_view.profile().ok();
                let _ = control_view.set_manager_status(manager_status(
                    "fail_closed",
                    profile.as_ref(),
                    Some(&rendered),
                ));
            }
            if retirement_polled_at.elapsed() >= Duration::from_secs(10) {
                retirement_polled_at = tokio::time::Instant::now();
                if let Err(error) = retire_unreferenced(&config).await {
                    eprintln!("{}", json!({"stage":"btc_5m_auto_hot_retirement_blocked",
                        "detail":format!("{error:#}")}));
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }))
}

async fn freshness_worker(config: Config, period: Duration) {
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        // A missing/rotating lease or a UTC-boundary profile race is expected
        // to reject fail-closed. The next bounded tick re-reads the current
        // profile; it never widens acquisition membership.
        let _ = refresh_current(&config).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    fn gamma(start: i64) -> Vec<u8> {
        let (start_at, end_at) = window_times(start).unwrap();
        serde_json::to_vec(&json!({"id":format!("{start}"),"conditionId":format!("0x{}", "a".repeat(64)),
            "slug":format!("btc-updown-5m-{start}"),"eventStartTime":start_at,"endDate":end_at,
            "description":"This market will resolve to \"Up\" if the time-weighted average price (TWAP) of Bitcoin, generated by Chainlink, of the time range specified in the title is greater than or equal to the price at the beginning of that range. Otherwise, it will resolve to \"Down\".\nThe resolution source for this market is information from Chainlink, specifically the BTC/USD TWAP data stream available at https://data.chain.link/streams/btc-usd-twap-60s-streams.\nPlease note that this market is about the price according to the TWAP Chainlink data stream for the asset pair BTC/USD, not according to any other sources or spot markets.",
            "resolutionSource":RESOLUTION_SOURCE,"active":true,"closed":false,"acceptingOrders":true,"enableOrderBook":true,
            "outcomes":"[\"Up\",\"Down\"]","clobTokenIds":format!("[\"{}1\",\"{}2\"]",start,start),"events":[{"id":"event"}]})).unwrap()
    }
    fn execution(start: i64) -> Vec<u8> {
        serde_json::to_vec(
            &json!({"c":format!("0x{}", "a".repeat(64)),"mts":"0.01","mos":"5",
            "t":[{"o":"Up","t":format!("{}1",start)},{"o":"Down","t":format!("{}2",start)}],
            "fd":{"r":"0.07","e":2}}),
        )
        .unwrap()
    }
    #[test]
    fn reviewed_rules_build_exact_current_and_next_profile() {
        let start = 1790400600;
        let values = [start, start + 300].map(|window| {
            project_candidate(
                window,
                &gamma(window),
                &execution(window),
                "2026-09-26T05:30:00Z",
                true,
            )
            .unwrap()
        });
        let profile = profile_from_candidates(&"b".repeat(64), 2, start, &values).unwrap();
        assert_eq!(profile.markets.len(), 2);
        assert_eq!(profile.markets[0].role, WindowRole::Current);
        assert_eq!(profile.markets[1].role, WindowRole::Next);
        assert_eq!(
            profile
                .markets
                .iter()
                .flat_map(|market| market.outcomes.iter())
                .map(|outcome| &outcome.token_id)
                .collect::<BTreeSet<_>>()
                .len(),
            4
        );
        // Six is a defensive wire/profile ceiling for readers that can expose
        // a retiring market. The autonomous manager's remove-first rotation
        // always publishes only current+next: two markets/four tokens.
        assert_eq!(profile.maximum_tokens, 6);
    }
    #[test]
    fn missed_windows_require_lease_absence_and_never_relax_rotation_cas() {
        let start = 1790400600;
        let build = |at, revision| {
            let values = [at, at + 300].map(|window| {
                project_candidate(window, &gamma(window), &execution(window),
                    "2026-09-26T05:30:00Z", true).unwrap()
            });
            profile_from_candidates(&"b".repeat(64), revision, at, &values).unwrap()
        };
        let incumbent = build(start, 1);
        let adjacent = build(start + 300, 2);
        let missed = build(start + 900, 2);
        assert!(!missed_adjacent_window(&incumbent, start + 300).unwrap());
        assert!(adjacent.validate_rotation_from(&incumbent).is_ok());
        assert!(missed_adjacent_window(&incumbent, start + 900).unwrap());
        assert!(missed.validate_rotation_from(&incumbent).is_err());
        assert!(missed_adjacent_window(&incumbent, start).is_err());
    }
    #[test]
    fn prereview_network_budget_finishes_before_requested_margin() {
        let margin = Duration::from_secs(30);
        assert!(!prereview_due(238, margin)); // 62 seconds remain.
        assert!(prereview_due(239, margin)); // 61 = 30 + 31 budget.
        assert!(prereview_due(269, margin));
        assert!(!prereview_due(270, margin)); // Critical 30-second margin.
        assert!(!prereview_due(299, margin));
    }
    #[test]
    fn changed_rule_or_window_fails_closed() {
        let start = 1790400600;
        let mut value: Value = serde_json::from_slice(&gamma(start)).unwrap();
        value["description"] = json!("changed");
        assert!(
            project_candidate(
                start,
                &serde_json::to_vec(&value).unwrap(),
                &execution(start),
                "2026-09-26T05:30:00Z",
                true,
            )
            .is_err()
        );
        let mut value: Value = serde_json::from_slice(&gamma(start)).unwrap();
        value["endDate"] = json!("2026-09-26T05:40:00Z");
        assert!(
            project_candidate(
                start,
                &serde_json::to_vec(&value).unwrap(),
                &execution(start),
                "2026-09-26T05:30:00Z",
                true,
            )
            .is_err()
        );
    }
    #[test]
    fn boundary_promotes_cached_current_and_next_without_a_fourth_market() {
        let old_start = 1790400600;
        let active = [old_start, old_start + 300].map(|window| {
            project_candidate(
                window,
                &gamma(window),
                &execution(window),
                "2026-09-26T05:30:00Z",
                true,
            )
            .unwrap()
        });
        let reviewed = project_candidate(
            old_start + 600,
            &gamma(old_start + 600),
            &execution(old_start + 600),
            "2026-09-26T05:34:30Z",
            true,
        )
        .unwrap();
        let reviewed_market_id = reviewed.binding.market_id.clone();
        let promoted = promote_reviewed(Some(active.clone()), Some(reviewed), old_start + 300)
            .expect("adjacent prereview promotes");
        assert_eq!(promoted[0].binding.market_id, active[1].binding.market_id);
        assert_eq!(promoted[1].binding.market_id, reviewed_market_id);
        assert_eq!(promoted.len(), 2);
        assert_eq!(
            promoted
                .iter()
                .flat_map(|candidate| candidate.binding.outcomes.iter())
                .count(),
            4
        );
        assert!(promote_reviewed(Some(active), None, old_start + 300).is_none());
    }
    #[test]
    fn rule_identity_is_stable_across_non_rule_gamma_updates() {
        let start = 1790400600;
        let first = project_candidate(
            start,
            &gamma(start),
            &execution(start),
            "2026-09-26T05:30:00Z",
            true,
        )
        .unwrap();
        let mut changed: Value = serde_json::from_slice(&gamma(start)).unwrap();
        changed["updatedAt"] = json!("2026-09-26T05:31:00Z");
        changed["liquidity"] = json!("999");
        let second = project_candidate(
            start,
            &serde_json::to_vec(&changed).unwrap(),
            &execution(start),
            "2026-09-26T05:31:00Z",
            true,
        )
        .unwrap();
        assert_eq!(first.binding.rule_sha256, second.binding.rule_sha256);
        assert_ne!(
            first.evidence["gamma"]["sha256"],
            second.evidence["gamma"]["sha256"]
        );
    }
    #[test]
    fn fixed_manager_timing_rejects_drift() {
        let mut config = Config {
            root: PathBuf::from("/tmp/btc5m-manager-test"),
            control_socket: PathBuf::from("/tmp/btc5m-manager.sock"),
            ttl: Duration::from_secs(120),
            renew: Duration::from_secs(40),
            prereview: Duration::from_secs(30),
            control_timeout: Duration::from_secs(10),
            response_bytes: 262_144,
            evidence_files: 576,
        };
        config.validate().unwrap();
        config.renew = Duration::from_secs(60);
        assert!(config.validate().is_err());
    }
    #[test]
    fn fixed_clock_rejects_candidate_after_utc_boundary() {
        let start = 1_790_400_600;
        require_current_window(start, at(start + 299).unwrap()).unwrap();
        assert!(require_current_window(start, at(start + 300).unwrap()).is_err());
    }
    #[test]
    fn bootstrap_post_discovery_clock_gate_rejects_expired_seed() {
        let start = 1_790_400_600;
        // This is the exact gate bootstrap invokes after all source responses
        // have arrived and before profile construction/persistence.
        assert!(require_current_window(start, at(start + 301).unwrap()).is_err());
    }
    #[tokio::test]
    async fn renewal_ticks_continue_while_discovery_path_is_blocked() {
        let directory =
            std::env::temp_dir().join(format!("mc-btc5m-renew-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let socket = directory.join("c");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let renewals = Arc::new(AtomicUsize::new(0));
        let observed = renewals.clone();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let observed = observed.clone();
                tokio::spawn(async move {
                    let length = stream.read_u32().await.unwrap() as usize;
                    let mut bytes = vec![0; length];
                    stream.read_exact(&mut bytes).await.unwrap();
                    let request: Value = serde_json::from_slice(&bytes).unwrap();
                    let result = match request["operation"].as_str().unwrap() {
                        "status" => json!({"scope_id":"scope","revision":1}),
                        "renew_acquisition_lease" => {
                            observed.fetch_add(1, Ordering::SeqCst);
                            json!({"status":"renewed"})
                        }
                        operation => panic!("unexpected operation {operation}"),
                    };
                    let response = serde_json::to_vec(&json!({"ok":true,"result":result})).unwrap();
                    stream.write_u32(response.len() as u32).await.unwrap();
                    stream.write_all(&response).await.unwrap();
                });
            }
        });
        let config = Config {
            root: directory.clone(),
            control_socket: socket,
            ttl: Duration::from_secs(120),
            renew: Duration::from_secs(40),
            prereview: Duration::from_secs(30),
            control_timeout: Duration::from_secs(10),
            response_bytes: 262_144,
            evidence_files: 576,
        };
        let (health_tx, health_rx) = tokio::sync::watch::channel(false);
        let worker = tokio::spawn(renewal_worker(
            config,
            None,
            health_tx,
            Duration::from_millis(10),
        ));
        // Represents a source prereview request that is slow or ultimately
        // fails. The lease worker must make progress independently.
        tokio::time::sleep(Duration::from_millis(75)).await;
        assert!(renewals.load(Ordering::SeqCst) >= 3);
        assert!(*health_rx.borrow());
        worker.abort();
        server.abort();
        fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn lease_status_distinguishes_absent_near_expiry_and_confirmed_renewal() {
        let status = json!({"leases":[
            {"lease_id":"other","remaining_milliseconds":100_000},
            {"lease_id":LEASE_ID,"remaining_milliseconds":80_000}
        ]});
        assert_eq!(manager_lease_remaining_from(&status).unwrap(), Some(80_000));
        assert_eq!(
            manager_lease_remaining_from(&json!({"leases":[]})).unwrap(),
            None
        );
        assert_eq!(
            manager_lease_remaining_from(&json!({"leases":[
                {"lease_id":LEASE_ID,"remaining_milliseconds":5_000}
            ]}))
            .unwrap(),
            Some(5_000)
        );
        assert!(
            manager_lease_remaining_from(&json!({"leases":[
                {"lease_id":LEASE_ID,"remaining_milliseconds":5_000},
                {"lease_id":LEASE_ID,"remaining_milliseconds":6_000}
            ]}))
            .is_err()
        );
    }
    #[test]
    fn persisted_profile_is_bounded_and_catalog_bound_for_restart() {
        let root = std::env::temp_dir().join(format!("mc-btc5m-{}", uuid::Uuid::new_v4()));
        let config = Config {
            root: root.clone(),
            control_socket: PathBuf::from("/tmp/btc5m-manager.sock"),
            ttl: Duration::from_secs(120),
            renew: Duration::from_secs(40),
            prereview: Duration::from_secs(30),
            control_timeout: Duration::from_secs(10),
            response_bytes: 262_144,
            evidence_files: 576,
        };
        let start = window_start(Utc::now());
        let candidates = [start, start + 300].map(|window| {
            project_candidate(
                window,
                &gamma(window),
                &execution(window),
                &Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
                true,
            )
            .unwrap()
        });
        let profile = profile_from_candidates(&"b".repeat(64), 1, start, &candidates).unwrap();
        for candidate in &candidates {
            atomic_evidence(&root, candidate, config.evidence_files).unwrap();
        }
        atomic_profile(&root, &profile).unwrap();
        let loaded = load_persisted_profile(&config, &"b".repeat(64))
            .unwrap()
            .unwrap();
        assert_eq!(loaded.profile_id, profile.profile_id);
        assert_eq!(
            fs::metadata(root.join(BOOTSTRAP_PROFILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let restored = load_persisted_candidates(&config, &loaded)
            .unwrap()
            .unwrap();
        assert_eq!(
            restored[0].binding.market_id,
            candidates[0].binding.market_id
        );
        assert_eq!(
            restored[1].binding.market_id,
            candidates[1].binding.market_id
        );
        let by_window = load_persisted_window_candidates(&config, start)
            .unwrap()
            .unwrap();
        assert_eq!(
            by_window[0].binding.start_at,
            candidates[0].binding.start_at
        );
        assert_eq!(
            by_window[1].binding.start_at,
            candidates[1].binding.start_at
        );
        assert!(load_persisted_profile(&config, &"c".repeat(64)).is_err());
        fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    #[ignore = "bounded official Gamma/CLOB rule-audit smoke"]
    async fn current_official_market_passes_reviewed_rule_contract() {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .unwrap();
        let start = window_start(Utc::now());
        let candidate = fetch_candidate(&client, start, 262_144, true)
            .await
            .unwrap();
        assert_eq!(candidate.binding.start_at, window_times(start).unwrap().0);
        assert_eq!(candidate.binding.outcomes[0].outcome, "Up");
        assert_eq!(candidate.binding.outcomes[1].outcome, "Down");
    }
}
