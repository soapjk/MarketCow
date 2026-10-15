//! Durable CTF finality: two bounded parallel reads, at least one verified result.
//!
//! RPC endpoints stay in mode-0600 profile files. Provider reads are bounded and
//! execute on the blocking pool, while publication is memory-first and disk
//! persistence advances a separate durable watermark.
use crate::source_finality_reader::{self as reader, Profile as ReaderProfile, TokenBinding};
use anyhow::{Context, Result, bail, ensure};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    io::Read,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    fs::OpenOptions,
    io::AsyncWriteExt,
    sync::{Mutex, RwLock, Semaphore, mpsc},
};
use url::Url;

const PROFILE_CAP: usize = 16 * 1024;
const TOKEN_CAP: usize = 4 * 1024;
const RECORD_CAP: usize = 1024 * 1024;
const VERIFIED_RESPONSE_CAP: usize = RECORD_CAP;
const RPC_RESPONSE_CAP: usize = 32 * 1024;
const RPC_TOTAL_CAP: usize = 128 * 1024;
const RPC_CALLS: usize = 11;
const OPERATOR_REGISTRY_SCHEMA: &str = "marketcow.rpc.operator-endpoint-registry.v1";
const OPERATOR_REGISTRY_REVISION: &str = "polygon-public-rpc-2026-09-25-r1";
const OPERATOR_ENDPOINT_VERIFIER: &str = "compiled-operator-endpoint-registry-v1";
const ENABLED_PROVIDER_IDENTITY_VERIFIERS: &[&str] = &[OPERATOR_ENDPOINT_VERIFIER];

#[derive(Clone)]
struct OperatorRegistryEntry {
    provider_id: &'static str,
    operator_id: &'static str,
    failure_domain: &'static str,
    endpoint_origin: &'static str,
    evidence_urls: [&'static str; 2],
}

const OPERATOR_REGISTRY: [OperatorRegistryEntry; 2] = [
    OperatorRegistryEntry {
        provider_id: "allnodes_publicnode_polygon_bor",
        operator_id: "allnodes",
        failure_domain: "operator_backend:allnodes_publicnode_polygon_bor",
        endpoint_origin: "https://polygon-bor-rpc.publicnode.com",
        evidence_urls: [
            "https://docs.polygon.technology/pos/reference/rpc-endpoints",
            "https://polygon.publicnode.com/",
        ],
    },
    OperatorRegistryEntry {
        provider_id: "tenderly_polygon_public_rpc",
        operator_id: "tenderly",
        failure_domain: "operator_backend:tenderly_polygon_public_rpc",
        endpoint_origin: "https://tenderly.rpc.polygon.community",
        evidence_urls: [
            "https://docs.polygon.technology/pos/reference/rpc-endpoints",
            "https://tenderly.co/blog/changelog/tenderly-is-a-polygon-public-rpc-provider/",
        ],
    },
];

#[derive(Clone)]
pub struct Config {
    pub root: PathBuf,
    pub profile_paths: [PathBuf; 2],
    pub caller_token_path: PathBuf,
    pub archive_bytes: usize,
    pub queue_records: usize,
    pub queue_bytes: usize,
    pub clients: usize,
    pub request_timeout: Duration,
    pub retry_after: Duration,
}

impl Config {
    fn validate(&self) -> Result<()> {
        ensure!(self.root.is_absolute(), "finality root must be absolute");
        ensure!(
            self.profile_paths.iter().all(|path| path.is_absolute()),
            "finality profiles must be absolute"
        );
        ensure!(
            self.caller_token_path.is_absolute(),
            "finality caller token path must be absolute"
        );
        ensure!(
            (2..=256).contains(&self.queue_records),
            "invalid finality queue records"
        );
        ensure!(
            (2 * RECORD_CAP..=64 * 1024 * 1024).contains(&self.queue_bytes),
            "invalid finality queue bytes"
        );
        ensure!(
            (8 * 1024 * 1024..=4 * 1024 * 1024 * 1024usize).contains(&self.archive_bytes),
            "invalid finality archive bytes"
        );
        ensure!((1..=8).contains(&self.clients), "invalid finality clients");
        ensure!(
            (Duration::from_secs(5)..=Duration::from_secs(120)).contains(&self.request_timeout),
            "invalid finality request timeout"
        );
        ensure!(
            (Duration::from_secs(10)..=Duration::from_secs(3600)).contains(&self.retry_after),
            "invalid finality retry interval"
        );
        Ok(())
    }
}

#[derive(Clone)]
struct Provider {
    provider_id: String,
    operator_id: String,
    failure_domain: String,
    profile_sha256: String,
    operator_registry_entry_sha256: String,
    endpoint: Url,
    proxy: Option<Url>,
    reader: ReaderProfile,
    collateral: String,
}

#[derive(Clone)]
pub struct Service {
    shared: Arc<Shared>,
}

struct Shared {
    state: RwLock<Runtime>,
    inflight: Mutex<HashSet<String>>,
    attempts: Mutex<HashMap<String, Instant>>,
    writer: mpsc::Sender<Persist>,
    clients: Arc<Semaphore>,
    providers: [Provider; 2],
    caller_token_sha256: [u8; 32],
    config: Config,
}

struct Runtime {
    epoch: String,
    published: u64,
    durable: u64,
    pending_records: usize,
    pending_bytes: usize,
    archive_bytes: usize,
    failure: Option<String>,
    first_observed: HashMap<String, String>,
    published_verified: HashMap<String, Value>,
    durable_verified: HashMap<String, Value>,
}

struct Persist {
    sequence: u64,
    bytes: Arc<Vec<u8>>,
    key: String,
    verified: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileWire {
    schema_version: String,
    provider_id: String,
    operator_id: String,
    failure_domain: String,
    rpc_endpoint: String,
    proxy_url: Option<String>,
    chain_id: String,
    contract: String,
    code_sha256: String,
    finality_policy: String,
    collateral: String,
    adapter_mode: String,
}

#[derive(Deserialize)]
struct FinalityQuery {
    market_id: String,
    condition_id: String,
    up_token: String,
    down_token: String,
}

struct InflightGuard {
    shared: Arc<Shared>,
    key: String,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        let shared = self.shared.clone();
        let key = self.key.clone();
        tokio::spawn(async move {
            shared.inflight.lock().await.remove(&key);
        });
    }
}

impl Service {
    pub async fn start(config: Config) -> Result<Self> {
        config.validate()?;
        let providers = [
            load_profile(&config.profile_paths[0])?,
            load_profile(&config.profile_paths[1])?,
        ];
        ensure!(
            providers[0].provider_id != providers[1].provider_id,
            "finality providers must be independent identities"
        );
        ensure!(
            providers[0].operator_id != providers[1].operator_id
                && providers[0].failure_domain != providers[1].failure_domain,
            "finality providers must have independent operators and failure domains"
        );
        ensure!(
            providers[0].reader.chain_id == providers[1].reader.chain_id
                && providers[0].reader.contract == providers[1].reader.contract
                && providers[0].reader.code_sha256 == providers[1].reader.code_sha256
                && providers[0].collateral == providers[1].collateral,
            "finality verification profiles disagree"
        );
        let caller_token_sha256 = load_caller_token(&config.caller_token_path)?;
        tokio::fs::create_dir_all(&config.root).await?;
        tokio::fs::set_permissions(&config.root, std::fs::Permissions::from_mode(0o700)).await?;
        let path = config.root.join("verified-final.jsonl");
        let runtime = recover(&path, config.archive_bytes)?;
        let (writer, receiver) = mpsc::channel(config.queue_records);
        let shared = Arc::new(Shared {
            state: RwLock::new(runtime),
            inflight: Mutex::new(HashSet::new()),
            attempts: Mutex::new(HashMap::new()),
            writer,
            clients: Arc::new(Semaphore::new(config.clients)),
            providers,
            caller_token_sha256,
            config,
        });
        tokio::spawn(writer_task(shared.clone(), receiver, path));
        Ok(Self { shared })
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route(
                "/v1/prediction-markets/polymarket/finality/status",
                get(status),
            )
            .route(
                "/v1/prediction-markets/polymarket/finality/verified",
                get(verified),
            )
            .with_state(self.shared.clone())
    }
}

fn operator_registry_entry_value(entry: &OperatorRegistryEntry) -> Value {
    json!({
        "schema_version":OPERATOR_REGISTRY_SCHEMA,
        "registry_revision":OPERATOR_REGISTRY_REVISION,
        "provider_id":entry.provider_id,
        "operator_id":entry.operator_id,
        "failure_domain":entry.failure_domain,
        "endpoint_origin":entry.endpoint_origin,
        "assurance_boundary":"operator-managed HTTPS endpoint; concrete upstream, cloud, region and physical host are not asserted",
        "evidence_urls":entry.evidence_urls
    })
}

fn operator_registry_entry(endpoint: &Url) -> Result<(&'static OperatorRegistryEntry, String)> {
    ensure!(
        endpoint.path() == "/" && endpoint.query().is_none() && endpoint.fragment().is_none(),
        "finality endpoint must be an exact registered origin"
    );
    let origin = endpoint.origin().ascii_serialization();
    let entry = OPERATOR_REGISTRY
        .iter()
        .find(|entry| entry.endpoint_origin == origin)
        .context("finality endpoint is not in operator registry")?;
    let digest = sha(&serde_json::to_vec(&operator_registry_entry_value(entry))?);
    Ok((entry, digest))
}

fn load_profile(path: &Path) -> Result<Provider> {
    let metadata = std::fs::metadata(path)?;
    ensure!(
        metadata.permissions().mode() & 0o077 == 0,
        "finality profile permissions"
    );
    let mut raw = Vec::new();
    std::fs::File::open(path)?
        .take((PROFILE_CAP + 1) as u64)
        .read_to_end(&mut raw)?;
    ensure!(raw.len() <= PROFILE_CAP, "finality profile capacity");
    let wire: ProfileWire = serde_json::from_slice(&raw)?;
    ensure!(
        wire.schema_version == "marketcow.polymarket.finality-rpc-profile.v1",
        "finality profile schema"
    );
    ensure!(
        !wire.provider_id.is_empty()
            && wire.provider_id.len() <= 64
            && wire
                .provider_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte)),
        "finality provider identity"
    );
    ensure!(
        !wire.operator_id.is_empty()
            && wire.operator_id.len() <= 128
            && !wire.failure_domain.is_empty()
            && wire.failure_domain.len() <= 128,
        "finality independence identity"
    );
    let endpoint = Url::parse(&wire.rpc_endpoint)?;
    ensure!(
        endpoint.scheme() == "https"
            && endpoint.username().is_empty()
            && endpoint.password().is_none(),
        "finality endpoint policy"
    );
    let (registry, operator_registry_entry_sha256) = operator_registry_entry(&endpoint)?;
    ensure!(
        wire.provider_id == registry.provider_id
            && wire.operator_id == registry.operator_id
            && wire.failure_domain == registry.failure_domain,
        "finality profile does not match operator registry"
    );
    let proxy = wire.proxy_url.map(|value| Url::parse(&value)).transpose()?;
    if let Some(proxy) = &proxy {
        ensure!(
            matches!(proxy.scheme(), "http" | "https")
                && proxy.username().is_empty()
                && proxy.password().is_none(),
            "finality proxy policy"
        );
    }
    ensure!(
        wire.finality_policy == "rpc_finalized_hash_pinned_v1",
        "finality policy"
    );
    ensure!(
        wire.adapter_mode == "standard_ctf_no_adapter",
        "unsupported finality adapter mode"
    );
    ensure!(hex_field(&wire.contract, 40), "finality contract identity");
    ensure!(
        hex_field(&wire.collateral, 40),
        "finality collateral identity"
    );
    ensure!(
        wire.code_sha256.len() == 64
            && wire
                .code_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()),
        "finality code identity"
    );
    Ok(Provider {
        provider_id: wire.provider_id,
        operator_id: wire.operator_id,
        failure_domain: wire.failure_domain,
        profile_sha256: sha(&raw),
        operator_registry_entry_sha256,
        endpoint,
        proxy,
        reader: ReaderProfile {
            chain_id: wire.chain_id,
            contract: wire.contract,
            code_sha256: wire.code_sha256,
            finality_policy: wire.finality_policy,
            maximum_calls: RPC_CALLS,
            maximum_bytes: RPC_TOTAL_CAP,
        },
        collateral: wire.collateral,
    })
}

fn load_caller_token(path: &Path) -> Result<[u8; 32]> {
    let metadata = std::fs::metadata(path)?;
    ensure!(
        metadata.permissions().mode() & 0o077 == 0,
        "finality caller token permissions"
    );
    let mut raw = Vec::new();
    std::fs::File::open(path)?
        .take((TOKEN_CAP + 1) as u64)
        .read_to_end(&mut raw)?;
    ensure!(raw.len() <= TOKEN_CAP, "finality caller token capacity");
    while raw.last().is_some_and(|byte| byte.is_ascii_whitespace()) {
        raw.pop();
    }
    ensure!(
        (32..=512).contains(&raw.len()) && raw.iter().all(|byte| byte.is_ascii_graphic()),
        "invalid finality caller token"
    );
    Ok(Sha256::digest(raw).into())
}

fn recover(path: &Path, archive_cap: usize) -> Result<Runtime> {
    let epoch = uuid::Uuid::new_v4().simple().to_string();
    let mut runtime = Runtime {
        epoch,
        published: 0,
        durable: 0,
        pending_records: 0,
        pending_bytes: 0,
        archive_bytes: 0,
        failure: None,
        first_observed: HashMap::new(),
        published_verified: HashMap::new(),
        durable_verified: HashMap::new(),
    };
    if !path.exists() {
        return Ok(runtime);
    }
    let raw = std::fs::read(path)?;
    ensure!(raw.len() <= archive_cap, "finality archive capacity");
    ensure!(
        raw.is_empty() || raw.ends_with(b"\n"),
        "partial finality archive"
    );
    for line in raw
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        ensure!(line.len() <= RECORD_CAP, "finality record capacity");
        let row: Value = serde_json::from_slice(line)?;
        ensure!(
            row["schema_version"] == "marketcow.polymarket.finality-log-record.v1",
            "finality log schema"
        );
        let sequence = row["sequence"].as_u64().context("finality sequence")?;
        ensure!(sequence == runtime.durable + 1, "finality archive sequence");
        let fact = row.get("fact").context("finality fact")?;
        ensure!(
            row["fact_sha256"] == sha(&serde_json::to_vec(fact)?),
            "finality fact digest"
        );
        let key = fact["binding_key"]
            .as_str()
            .context("finality binding key")?;
        let first = fact["first_observed_at"]
            .as_str()
            .context("finality first observed")?;
        match runtime.first_observed.get(key) {
            Some(existing) => ensure!(existing == first, "finality first observation changed"),
            None => {
                runtime
                    .first_observed
                    .insert(key.to_owned(), first.to_owned());
            }
        }
        if fact["status"] == "verified_final" {
            validate_verified_fact(fact)?;
            if let Some(existing) = runtime.durable_verified.get(key) {
                ensure!(same_verified(existing, fact), "finality payout changed");
            }
            runtime
                .durable_verified
                .insert(key.to_owned(), fact.clone());
            runtime
                .published_verified
                .insert(key.to_owned(), fact.clone());
        } else {
            ensure!(
                fact["schema_version"] == "marketcow.polymarket.finality-attempt.v1"
                    && fact["status"] == "pending"
                    && fact["settlement_import_allowed"] == false,
                "finality attempt schema"
            );
        }
        runtime.durable = sequence;
        runtime.published = sequence;
    }
    runtime.archive_bytes = raw.len();
    Ok(runtime)
}

async fn publish(shared: &Arc<Shared>, key: String, fact: Value) -> Result<u64> {
    let verified = (fact["status"] == "verified_final").then_some(fact.clone());
    let (sequence, bytes) = {
        let mut state = shared.state.write().await;
        ensure!(state.failure.is_none(), "finality persistence failed");
        let sequence = state
            .published
            .checked_add(1)
            .context("finality sequence overflow")?;
        let fact_sha256 = sha(&serde_json::to_vec(&fact)?);
        let wrapper = json!({
            "schema_version":"marketcow.polymarket.finality-log-record.v1",
            "sequence":sequence,
            "fact_sha256":fact_sha256,
            "fact":fact
        });
        let mut encoded = serde_json::to_vec(&wrapper)?;
        encoded.push(b'\n');
        ensure!(encoded.len() <= RECORD_CAP, "finality record too large");
        ensure!(
            state.pending_records < shared.config.queue_records
                && state.pending_bytes + encoded.len() <= shared.config.queue_bytes,
            "finality persistence capacity"
        );
        ensure!(
            state.archive_bytes + state.pending_bytes + encoded.len()
                <= shared.config.archive_bytes,
            "finality archive capacity"
        );
        state.published = sequence;
        state.pending_records += 1;
        state.pending_bytes += encoded.len();
        if let Some(value) = &verified {
            state.published_verified.insert(key.clone(), value.clone());
        }
        (sequence, Arc::new(encoded))
    };
    if shared
        .writer
        .try_send(Persist {
            sequence,
            bytes: bytes.clone(),
            key,
            verified,
        })
        .is_err()
    {
        let mut state = shared.state.write().await;
        state.failure = Some("finality_persistence_capacity".into());
        bail!("finality persistence capacity")
    }
    Ok(sequence)
}

async fn writer_task(shared: Arc<Shared>, mut receiver: mpsc::Receiver<Persist>, path: PathBuf) {
    let result: Result<()> = async {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .await?;
        while let Some(item) = receiver.recv().await {
            file.write_all(&item.bytes).await?;
            file.sync_data().await?;
            let mut state = shared.state.write().await;
            ensure!(
                item.sequence == state.durable + 1,
                "finality durable sequence"
            );
            state.durable = item.sequence;
            state.pending_records -= 1;
            state.pending_bytes -= item.bytes.len();
            state.archive_bytes += item.bytes.len();
            if let Some(value) = item.verified {
                state.durable_verified.insert(item.key, value);
            }
        }
        Ok(())
    }
    .await;
    if let Err(error) = result {
        shared.state.write().await.failure = Some(format!("finality_persistence_failed:{error:#}"));
    }
}

async fn status(State(shared): State<Arc<Shared>>, headers: HeaderMap) -> Response {
    if !authorized(&shared, &headers) {
        return failure(StatusCode::UNAUTHORIZED, "finality_unauthorized", None);
    }
    let state = shared.state.read().await;
    Json(json!({
        "schema_version":"marketcow.polymarket.finality-status.v1",
        "epoch":state.epoch,
        "published":state.published,
        "durable":state.durable,
        "pending_records":state.pending_records,
        "pending_bytes":state.pending_bytes,
        "verified_market_count":state.durable_verified.len(),
        "maximum_verified_response_bytes":VERIFIED_RESPONSE_CAP,
        "failure":state.failure,
        "provider_identity_gate":provider_identity_requirement(),
        "providers":shared.providers.iter().map(|provider|json!({
            "provider_id":provider.provider_id,
            "operator_id":provider.operator_id,
            "failure_domain":provider.failure_domain,
            "profile_sha256":provider.profile_sha256,
            "operator_registry_revision":OPERATOR_REGISTRY_REVISION,
            "operator_registry_entry_sha256":provider.operator_registry_entry_sha256,
            "identity_assurance":"registry_bound_https_operator_endpoint"
        })).collect::<Vec<_>>()
    }))
    .into_response()
}

async fn verified(
    State(shared): State<Arc<Shared>>,
    headers: HeaderMap,
    query: Result<Query<FinalityQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    if !authorized(&shared, &headers) {
        return failure(StatusCode::UNAUTHORIZED, "finality_unauthorized", None);
    }
    let Ok(Query(query)) = query else {
        return failure(StatusCode::BAD_REQUEST, "invalid_request", None);
    };
    let Ok((key, token_hex)) = validate_query(&query) else {
        return failure(StatusCode::BAD_REQUEST, "invalid_identity", None);
    };
    {
        let state = shared.state.read().await;
        if let Some(value) = state.durable_verified.get(&key).filter(|value|
            value["schema_version"] == "marketcow.polymarket.verified-final.v2") {
            if !matches!(
                serde_json::to_vec(value),
                Ok(encoded) if encoded.len() <= VERIFIED_RESPONSE_CAP
            ) {
                return failure(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "finality_response_capacity",
                    state.first_observed.get(&key).cloned(),
                );
            }
            return Json(value.clone()).into_response();
        }
        if state.failure.is_some() {
            return failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "finality_persistence_failed",
                state.first_observed.get(&key).cloned(),
            );
        }
    }
    let Ok(_permit) = shared.clients.clone().try_acquire_owned() else {
        return failure(StatusCode::TOO_MANY_REQUESTS, "finality_capacity", None);
    };
    {
        let mut inflight = shared.inflight.lock().await;
        if !inflight.insert(key.clone()) {
            return failure(StatusCode::CONFLICT, "finality_read_inflight", None);
        }
    }
    let _guard = InflightGuard {
        shared: shared.clone(),
        key: key.clone(),
    };
    {
        let mut attempts = shared.attempts.lock().await;
        if attempts
            .get(&key)
            .is_some_and(|previous| previous.elapsed() < shared.config.retry_after)
        {
            let first = shared.state.read().await.first_observed.get(&key).cloned();
            return failure(
                StatusCode::TOO_MANY_REQUESTS,
                "finality_retry_limited",
                first,
            );
        }
        attempts.insert(key.clone(), Instant::now());
    }
    let request_started_at = timestamp();
    let first_observed_at = {
        let mut state = shared.state.write().await;
        state
            .first_observed
            .entry(key.clone())
            .or_insert_with(|| request_started_at.clone())
            .clone()
    };
    let binding = TokenBinding {
        collateral: shared.providers[0].collateral.clone(),
        token_ids_hex: token_hex,
    };
    if shared.providers[0].collateral != shared.providers[1].collateral {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "finality_profile_disagreement",
            Some(first_observed_at),
        );
    }
    let left = observe_provider(
        shared.providers[0].clone(),
        query.condition_id.clone(),
        binding.clone(),
        shared.config.request_timeout,
    );
    let right = observe_provider(
        shared.providers[1].clone(),
        query.condition_id.clone(),
        binding,
        shared.config.request_timeout,
    );
    let (left, right) = tokio::join!(left, right);
    // Join both bounded reads before committing. No detached late result can
    // change an already published payout; failure of one provider is not a gate.
    let observations = [left.ok(), right.ok()].into_iter().flatten().collect::<Vec<_>>();
    if observations.is_empty() {
            let attempt_observed_at = timestamp();
            let fact = pending_fact(
                &query,
                &key,
                &first_observed_at,
                &attempt_observed_at,
                "provider_read_failed",
                observations,
            );
            if publish(&shared, key, fact).await.is_err() {
                return failure(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "finality_persistence_failed",
                    Some(first_observed_at),
                );
            }
            return failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "provider_read_failed",
                Some(first_observed_at),
            );
    }
    let fact = match quorum_fact(&query, &key, &first_observed_at, &observations) {
        Ok(value) => value,
        Err(code) => {
            let attempt_observed_at = timestamp();
            let fact = pending_fact(
                &query,
                &key,
                &first_observed_at,
                &attempt_observed_at,
                code,
                observations,
            );
            if publish(&shared, key, fact).await.is_err() {
                return failure(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "finality_persistence_failed",
                    Some(first_observed_at),
                );
            }
            return failure(if code == "finality_provider_disagreement" {
                StatusCode::CONFLICT
            } else { StatusCode::ACCEPTED }, code, Some(first_observed_at));
        }
    };
    match publish(&shared, key, fact).await {
        Ok(sequence) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "schema_version":"marketcow.polymarket.finality-pending.v1",
                "status":"awaiting_durability",
                "published_sequence":sequence,
                "first_observed_at":first_observed_at,
                "settlement_import_allowed":false
            })),
        )
            .into_response(),
        Err(_) => failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "finality_persistence_failed",
            Some(first_observed_at),
        ),
    }
}

fn authorized(shared: &Shared, headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(axum::http::header::AUTHORIZATION) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return false;
    };
    let candidate: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    candidate
        .iter()
        .zip(shared.caller_token_sha256.iter())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

async fn observe_provider(
    provider: Provider,
    condition: String,
    binding: TokenBinding,
    timeout: Duration,
) -> Result<Value> {
    tokio::task::spawn_blocking(move || {
        let mut builder = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .no_proxy();
        if let Some(proxy) = &provider.proxy {
            builder = builder.proxy(reqwest::Proxy::all(proxy.as_str())?);
        }
        let client = builder.build()?;
        let deadline = Instant::now() + timeout;
        let mut total = 0usize;
        let endpoint = provider.endpoint.clone();
        let observation = reader::read_bound(&provider.reader, &condition, &binding, |request| {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .context("RPC deadline")?;
            ensure!(total < RPC_TOTAL_CAP, "RPC response capacity");
            let response = client
                .post(endpoint.clone())
                .timeout(remaining.min(Duration::from_secs(10)))
                .json(request)
                .send()
                .map_err(|_| anyhow::anyhow!("RPC transport failed"))?;
            ensure!(response.status().is_success(), "RPC HTTP failure");
            let mut raw = Vec::new();
            response
                .take((RPC_RESPONSE_CAP + 1) as u64)
                .read_to_end(&mut raw)
                .map_err(|_| anyhow::anyhow!("RPC body failed"))?;
            ensure!(raw.len() <= RPC_RESPONSE_CAP, "RPC response capacity");
            total = total.checked_add(raw.len()).context("RPC byte overflow")?;
            ensure!(total <= RPC_TOTAL_CAP, "RPC total capacity");
            Ok(raw)
        })?;
        let observation_sha256 = sha(&serde_json::to_vec(&observation)?);
        let endpoint_origin = endpoint.origin().ascii_serialization();
        Ok(json!({
            "provider_id":provider.provider_id,
            "operator_id":provider.operator_id,
            "failure_domain":provider.failure_domain,
            "profile_sha256":provider.profile_sha256,
            "observation_sha256":observation_sha256,
            "provider_identity_evidence":{
                "schema_version":"marketcow.rpc.operator-endpoint-identity-evidence.v1",
                "assurance":"registry_bound_https_operator_endpoint",
                "verifier_id":OPERATOR_ENDPOINT_VERIFIER,
                "endpoint_origin":endpoint_origin,
                "profile_claims_authoritative":false,
                "provider_id":provider.provider_id,
                "operator_id":provider.operator_id,
                "failure_domain":provider.failure_domain,
                "operator_registry_schema":OPERATOR_REGISTRY_SCHEMA,
                "operator_registry_revision":OPERATOR_REGISTRY_REVISION,
                "operator_registry_entry_sha256":provider.operator_registry_entry_sha256,
                "observation_sha256":observation_sha256
            },
            "observation":observation
        }))
    })
    .await
    .context("finality provider task")?
}

fn provider_identity_requirement() -> Value {
    json!({
        "schema_version":"marketcow.rpc.provider-identity-requirement.v1",
        "verified_final_enabled":!ENABLED_PROVIDER_IDENTITY_VERIFIERS.is_empty(),
        "profile_claims_authoritative":false,
        "active_evidence_schema":"marketcow.rpc.operator-endpoint-identity-evidence.v1",
        "active_assurance":"registry_bound_https_operator_endpoint",
        "assurance_boundary":"operator-managed HTTPS endpoint; concrete upstream, cloud, region and physical host are not asserted",
        "runtime_checks":[
            "exact_registered_https_origin",
            "provider_id",
            "operator_id",
            "failure_domain",
            "operator_registry_revision",
            "operator_registry_entry_sha256",
            "observation_sha256"
        ],
        "stronger_optional_evidence_schema":"marketcow.rpc.signed-provider-upstream-evidence.v1",
        "enabled_verifiers":ENABLED_PROVIDER_IDENTITY_VERIFIERS
    })
}

fn provider_identity_is_runtime_verified(row: &Value) -> bool {
    let evidence = &row["provider_identity_evidence"];
    let Some(verifier_id) = evidence["verifier_id"].as_str() else {
        return false;
    };
    if verifier_id != OPERATOR_ENDPOINT_VERIFIER
        || !ENABLED_PROVIDER_IDENTITY_VERIFIERS.contains(&verifier_id)
        || evidence["schema_version"] != "marketcow.rpc.operator-endpoint-identity-evidence.v1"
        || evidence["assurance"] != "registry_bound_https_operator_endpoint"
        || evidence["profile_claims_authoritative"] != false
        || evidence["observation_sha256"] != row["observation_sha256"]
        || evidence["provider_id"] != row["provider_id"]
        || evidence["operator_id"] != row["operator_id"]
        || evidence["failure_domain"] != row["failure_domain"]
        || evidence["operator_registry_schema"] != OPERATOR_REGISTRY_SCHEMA
        || evidence["operator_registry_revision"] != OPERATOR_REGISTRY_REVISION
    {
        return false;
    }
    let Some(origin) = evidence["endpoint_origin"].as_str() else {
        return false;
    };
    let Ok(endpoint) = Url::parse(origin) else {
        return false;
    };
    let Ok((entry, digest)) = operator_registry_entry(&endpoint) else {
        return false;
    };
    evidence["provider_id"] == entry.provider_id
        && evidence["operator_id"] == entry.operator_id
        && evidence["failure_domain"] == entry.failure_domain
        && evidence["operator_registry_entry_sha256"] == digest
}

fn quorum_fact(
    query: &FinalityQuery,
    key: &str,
    first_observed_at: &str,
    observations: &[Value],
) -> std::result::Result<Value, &'static str> {
    if observations.is_empty() || observations.len() > 2
        || (observations.len() == 2 && observations[0]["provider_id"] == observations[1]["provider_id"]) {
        return Err("valid_provider_observation_required");
    }
    let mut last_invalid = "finality_observation_unresolved_or_invalid";
    let observations = observations.iter().filter(|row| {
        let value = &row["observation"];
        if value["schema_version"] != "marketcow.polymarket.ctf-observation.v1"
            || value["condition_id"] != query.condition_id
            || value["status"] != "resolved_unverified"
            || value["settlement_import_allowed"] != false
            || value["standard_ctf_token_binding_verified"] != true
        { return false; }
        if validate_observation(value).is_err()
            || row["observation_sha256"] != sha(&serde_json::to_vec(value).unwrap_or_default())
        { last_invalid = "finality_observation_invalid"; return false; }
        if !provider_identity_is_runtime_verified(row) {
            last_invalid = "two_gateway_matching_observations";
            return false;
        }
        true
    }).cloned().collect::<Vec<_>>();
    if observations.is_empty() { return Err(last_invalid); }
    let values = observations
        .iter()
        .map(|row| &row["observation"])
        .collect::<Vec<_>>();
    for value in &values {
        if value["schema_version"] != "marketcow.polymarket.ctf-observation.v1"
            || value["condition_id"] != query.condition_id
            || value["status"] != "resolved_unverified"
            || value["settlement_import_allowed"] != false
            || value["standard_ctf_token_binding_verified"] != true
        {
            return Err("finality_observation_unresolved_or_invalid");
        }
        validate_observation(value).map_err(|_| "finality_observation_invalid")?;
    }
    let agreement = |value: &Value| {
        json!({
            "chain_id":value["chain_id"],
            "ctf_address":value["ctf_address"],
            "collateral":value["collateral"],
            "payout_numerators":value["payout_numerators"],
            "payout_denominator":value["payout_denominator"],
            "bound_tokens":value["bound_tokens"]
        })
    };
    if values.len() == 2 && agreement(values[0]) != agreement(values[1]) {
        return Err("finality_provider_disagreement");
    }
    if !observations
        .iter()
        .all(provider_identity_is_runtime_verified)
    {
        return Err("two_gateway_matching_observations");
    }
    let payout_numerators = values[0]["payout_numerators"].clone();
    let payout_denominator = values[0]["payout_denominator"].clone();
    let provider_receipts = observations
        .iter()
        .map(|row| {
            json!({
                "provider_id":row["provider_id"],
                "operator_id":row["operator_id"],
                "failure_domain":row["failure_domain"],
                "profile_sha256":row["profile_sha256"],
                "observation_sha256":row["observation_sha256"],
                "provider_identity_evidence":row["provider_identity_evidence"],
                "block_hash":row["observation"]["block"]["hash"],
                "block_number":row["observation"]["block"]["number"],
                "observed_at":row["observation"]["observed_at"]
            })
        })
        .collect::<Vec<_>>();
    let verified_at = timestamp();
    validate_finality_timeline(first_observed_at, &verified_at, &observations)
        .map_err(|_| "finality_observation_time_invalid")?;
    Ok(json!({
        "schema_version":"marketcow.polymarket.verified-final.v2",
        "event_type":"polymarket_verified_final",
        "binding_key":key,
        "market_id":query.market_id,
        "condition_id":query.condition_id,
        "ordered_outcomes":[
            {"outcome":"Up","token_id":query.up_token,"payout_numerator":payout_numerators[0]},
            {"outcome":"Down","token_id":query.down_token,"payout_numerator":payout_numerators[1]}
        ],
        "payout_numerators":payout_numerators,
        "payout_denominator":payout_denominator,
        "chain_id":values[0]["chain_id"],
        "ctf_address":values[0]["ctf_address"],
        "collateral":values[0]["collateral"],
        "status":"verified_final",
        "finality_policy":"parallel_two_endpoints_at_least_one_valid_finalized_no_conflict_v2",
        "provider_reads_requested":2,
        "provider_reads_completed_before_decision":true,
        "valid_provider_count":observations.len(),
        "decision_strategy":"wait_both_bounded_reads_no_late_commit",
        "identity_assurance":"registry_bound_https_operator_endpoints",
        "identity_assurance_boundary":"concrete upstream, cloud, region and physical host are not asserted",
        "first_observed_at":first_observed_at,
        "verified_at":verified_at,
        "provider_receipts":provider_receipts,
        "provider_observations":observations,
        "settlement_import_allowed":true,
        "limitation":"standard CTF payouts verified; adapter redemption execution is outside this receipt"
    }))
}

fn validate_observation(value: &Value) -> Result<()> {
    let evidence = value["evidence"].as_array().context("finality evidence")?;
    ensure!(evidence.len() == RPC_CALLS, "finality evidence count");
    let expected_methods = [
        "eth_chainId",
        "eth_getBlockByNumber",
        "eth_getCode",
        "eth_call",
        "eth_call",
        "eth_call",
        "eth_call",
        "eth_call",
        "eth_call",
        "eth_call",
        "eth_call",
    ];
    let block_hash = value["block"]["hash"]
        .as_str()
        .context("finality block hash")?;
    let pinned = json!({"blockHash":block_hash,"requireCanonical":true});
    for (index, row) in evidence.iter().enumerate() {
        let raw = row["raw_utf8"].as_str().context("finality raw")?.as_bytes();
        ensure!(row["raw_bytes"] == raw.len(), "finality raw bytes");
        ensure!(row["raw_sha256"] == sha(raw), "finality raw digest");
        let response: Value = serde_json::from_slice(raw)?;
        ensure!(response["jsonrpc"] == "2.0", "finality RPC version");
        ensure!(response["id"] == index + 1, "finality RPC identity");
        ensure!(response.get("error").is_none(), "finality RPC error");
        ensure!(
            row["request"]["method"] == expected_methods[index],
            "finality RPC method sequence"
        );
        if index >= 2 {
            ensure!(
                row["request"]["params"][1] == pinned,
                "finality RPC not pinned"
            );
        }
    }
    let denominator = value["payout_denominator"]
        .as_str()
        .context("finality denominator")?
        .parse::<u128>()?;
    let numerators = value["payout_numerators"]
        .as_array()
        .context("finality numerators")?;
    ensure!(
        denominator > 0 && numerators.len() == 2,
        "finality payout shape"
    );
    let sum = numerators.iter().try_fold(0u128, |sum, value| {
        Ok::<_, anyhow::Error>(
            sum + value
                .as_str()
                .context("finality numerator")?
                .parse::<u128>()?,
        )
    })?;
    ensure!(sum == denominator, "finality payout sum");
    Ok(())
}

fn pending_fact(
    query: &FinalityQuery,
    key: &str,
    first_observed_at: &str,
    observed_at: &str,
    reason: &str,
    observations: Vec<Value>,
) -> Value {
    json!({
        "schema_version":"marketcow.polymarket.finality-attempt.v1",
        "event_type":"polymarket_finality_attempt",
        "binding_key":key,
        "market_id":query.market_id,
        "condition_id":query.condition_id,
        "ordered_outcomes":[{"outcome":"Up","token_id":query.up_token},{"outcome":"Down","token_id":query.down_token}],
        "status":"pending",
        "reason":reason,
        "evidence_class":if reason == "two_gateway_matching_observations" {
            "two_gateway_matching_observations"
        } else {
            "incomplete_finality_attempt"
        },
        "first_observed_at":first_observed_at,
        "observed_at":observed_at,
        "provider_observations":observations,
        "provider_identity_gate":provider_identity_requirement(),
        "settlement_import_allowed":false
    })
}

fn validate_verified_fact(value: &Value) -> Result<()> {
    // Existing immutable archive records are verified under their original
    // assurance, never relabelled or served as new v2 receipts.
    let historical = value["schema_version"] == "marketcow.polymarket.verified-final.v1";
    ensure!(
        (historical || value["schema_version"] == "marketcow.polymarket.verified-final.v2")
            && value["status"] == "verified_final"
            && value["finality_policy"]
                == if historical { "two_independent_operator_endpoints_finalized_receipts_v1" }
                else { "parallel_two_endpoints_at_least_one_valid_finalized_no_conflict_v2" }
            && value["identity_assurance"] == "registry_bound_https_operator_endpoints"
            && value["settlement_import_allowed"] == true,
        "verified final schema"
    );
    let observations = value["provider_observations"]
        .as_array()
        .context("verified observations")?;
    ensure!(if historical { observations.len() == 2 }
        else { (1..=2).contains(&observations.len())
            && value["valid_provider_count"] == observations.len()
            && value["provider_reads_requested"] == 2
            && value["provider_reads_completed_before_decision"] == true
            && value["decision_strategy"] == "wait_both_bounded_reads_no_late_commit" },
        "verified provider count/policy");
    validate_finality_timeline(
        value["first_observed_at"]
            .as_str()
            .context("verified first observation time")?,
        value["verified_at"]
            .as_str()
            .context("verified quorum time")?,
        observations,
    )?;
    for row in observations {
        ensure!(
            provider_identity_is_runtime_verified(row),
            "verified provider identity evidence"
        );
        let observation = &row["observation"];
        ensure!(
            row["observation_sha256"] == sha(&serde_json::to_vec(observation)?),
            "verified observation digest"
        );
        validate_observation(observation)?;
    }
    if !historical {
        let query = FinalityQuery {
            market_id: value["market_id"].as_str().context("market")?.to_owned(),
            condition_id: value["condition_id"].as_str().context("condition")?.to_owned(),
            up_token: value["ordered_outcomes"][0]["token_id"].as_str().context("up")?.to_owned(),
            down_token: value["ordered_outcomes"][1]["token_id"].as_str().context("down")?.to_owned(),
        };
        let regenerated = quorum_fact(&query, value["binding_key"].as_str().context("binding")?,
            value["first_observed_at"].as_str().context("first")?, observations)
            .map_err(|code| anyhow::anyhow!(code))?;
        ensure!(same_verified(value, &regenerated)
            && value["provider_receipts"] == regenerated["provider_receipts"],
            "verified payout/receipts mismatch");
    }
    Ok(())
}

fn validate_finality_timeline(
    first_observed_at: &str,
    verified_at: &str,
    observations: &[Value],
) -> Result<()> {
    let first = chrono::DateTime::parse_from_rfc3339(first_observed_at)?;
    let verified = chrono::DateTime::parse_from_rfc3339(verified_at)?;
    ensure!(first <= verified, "finality timeline inverted");
    for row in observations {
        let observed = chrono::DateTime::parse_from_rfc3339(
            row["observation"]["observed_at"]
                .as_str()
                .context("provider observation time")?,
        )?;
        ensure!(
            first <= observed && observed <= verified,
            "provider observation outside finality timeline"
        );
    }
    Ok(())
}

fn same_verified(left: &Value, right: &Value) -> bool {
    [
        "market_id",
        "condition_id",
        "ordered_outcomes",
        "payout_numerators",
        "payout_denominator",
        "chain_id",
        "ctf_address",
        "collateral",
    ]
    .iter()
    .all(|field| left[*field] == right[*field])
}

fn validate_query(query: &FinalityQuery) -> Result<(String, [String; 2])> {
    ensure!(
        !query.market_id.is_empty()
            && query.market_id.len() <= 20
            && query.market_id.bytes().all(|byte| byte.is_ascii_digit()),
        "market identity"
    );
    ensure!(hex_field(&query.condition_id, 64), "condition identity");
    ensure!(
        query.up_token != query.down_token,
        "duplicate outcome token"
    );
    let token_hex = [
        decimal_to_u256(&query.up_token)?,
        decimal_to_u256(&query.down_token)?,
    ];
    let key = format!(
        "{}|{}|{}|{}",
        query.market_id, query.condition_id, query.up_token, query.down_token
    );
    Ok((key, token_hex))
}

fn decimal_to_u256(value: &str) -> Result<String> {
    ensure!(
        !value.is_empty() && value.len() <= 78 && value.bytes().all(|byte| byte.is_ascii_digit()),
        "token decimal identity"
    );
    let mut bytes = [0u8; 32];
    for digit in value.bytes().map(|byte| byte - b'0') {
        let mut carry = digit as u16;
        for byte in bytes.iter_mut().rev() {
            let current = *byte as u16 * 10 + carry;
            *byte = current as u8;
            carry = current >> 8;
        }
        ensure!(carry == 0, "token exceeds uint256");
    }
    Ok(format!("0x{}", hex::encode(bytes)))
}

fn hex_field(value: &str, digits: usize) -> bool {
    value.len() == digits + 2
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn sha(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn failure(status: StatusCode, code: &str, first_observed_at: Option<String>) -> Response {
    (
        status,
        Json(json!({
            "schema_version":"marketcow.polymarket.finality-pending.v1",
            "status":"pending",
            "reason":code,
            "first_observed_at":first_observed_at,
            "settlement_import_allowed":false
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    fn query() -> FinalityQuery {
        FinalityQuery {
            market_id: "1".into(),
            condition_id: format!("0x{}", "a".repeat(64)),
            up_token: "10".into(),
            down_token: "20".into(),
        }
    }

    fn provider_observation(index: usize, query: &FinalityQuery, registry_bound: bool) -> Value {
        let registry = &OPERATOR_REGISTRY[index];
        let (_, token_ids_hex) = validate_query(query).unwrap();
        let profile = ReaderProfile {
            chain_id: "0x89".into(),
            contract: format!("0x{}", "1".repeat(40)),
            code_sha256: sha(&[1]),
            finality_policy: "rpc_finalized_hash_pinned_v1".into(),
            maximum_calls: RPC_CALLS,
            maximum_bytes: RPC_TOTAL_CAP,
        };
        let binding = TokenBinding {
            collateral: format!("0x{}", "3".repeat(40)),
            token_ids_hex,
        };
        let observation = reader::read_bound(&profile, &query.condition_id, &binding, |request| {
            let id = request["id"].as_u64().unwrap() as usize;
            let result = match id {
                1 => json!("0x89"),
                2 => json!({
                    "hash":format!("0x{}","2".repeat(64)),
                    "number":"0x10",
                    "timestamp":"0x20"
                }),
                3 => json!("0x01"),
                4 => json!(format!("0x{:064x}", 2)),
                5 | 6 => json!(format!("0x{:064x}", 1)),
                7 => json!(format!("0x{:064x}", 0)),
                8 | 10 => json!(format!("0x{}", "b".repeat(64))),
                9 => json!(binding.token_ids_hex[0]),
                11 => json!(binding.token_ids_hex[1]),
                _ => unreachable!(),
            };
            Ok(serde_json::to_vec(
                &json!({"jsonrpc":"2.0","id":id,"result":result}),
            )?)
        })
        .unwrap();
        let observation_sha256 = sha(&serde_json::to_vec(&observation).unwrap());
        let registry_entry_sha256 =
            sha(&serde_json::to_vec(&operator_registry_entry_value(registry)).unwrap());
        let provider_identity_evidence = if registry_bound {
            json!({
                "schema_version":"marketcow.rpc.operator-endpoint-identity-evidence.v1",
                "assurance":"registry_bound_https_operator_endpoint",
                "verifier_id":OPERATOR_ENDPOINT_VERIFIER,
                "endpoint_origin":registry.endpoint_origin,
                "profile_claims_authoritative":false,
                "provider_id":registry.provider_id,
                "operator_id":registry.operator_id,
                "failure_domain":registry.failure_domain,
                "operator_registry_schema":OPERATOR_REGISTRY_SCHEMA,
                "operator_registry_revision":OPERATOR_REGISTRY_REVISION,
                "operator_registry_entry_sha256":registry_entry_sha256,
                "observation_sha256":observation_sha256
            })
        } else {
            json!({
                "schema_version":"marketcow.rpc.gateway-identity-observation.v1",
                "assurance":"gateway_tls_origin_only",
                "endpoint_origin":registry.endpoint_origin,
                "profile_claims_authoritative":false,
                "observation_sha256":observation_sha256
            })
        };
        json!({
            "provider_id":registry.provider_id,
            "operator_id":registry.operator_id,
            "failure_domain":registry.failure_domain,
            "profile_sha256":sha(registry.provider_id.as_bytes()),
            "observation_sha256":observation_sha256,
            "provider_identity_evidence":provider_identity_evidence,
            "observation":observation
        })
    }

    fn write_private(path: &Path, value: &[u8]) {
        std::fs::write(path, value).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn service_config(root: &Path) -> Config {
        let profile_paths = [root.join("provider-a.json"), root.join("provider-b.json")];
        for (index, path) in profile_paths.iter().enumerate() {
            let registry = &OPERATOR_REGISTRY[index];
            let value = json!({
                "schema_version":"marketcow.polymarket.finality-rpc-profile.v1",
                "provider_id":registry.provider_id,
                "operator_id":registry.operator_id,
                "failure_domain":registry.failure_domain,
                "rpc_endpoint":registry.endpoint_origin,
                "proxy_url":Value::Null,
                "chain_id":"0x89",
                "contract":format!("0x{}","1".repeat(40)),
                "code_sha256":sha(&[1]),
                "finality_policy":"rpc_finalized_hash_pinned_v1",
                "collateral":format!("0x{}","3".repeat(40)),
                "adapter_mode":"standard_ctf_no_adapter"
            });
            write_private(path, &serde_json::to_vec(&value).unwrap());
        }
        let caller_token_path = root.join("caller.token");
        write_private(
            &caller_token_path,
            b"a-private-token-with-at-least-32-bytes",
        );
        Config {
            root: root.join("evidence"),
            profile_paths,
            caller_token_path,
            archive_bytes: 8 * 1024 * 1024,
            queue_records: 4,
            queue_bytes: 2 * RECORD_CAP,
            clients: 1,
            request_timeout: Duration::from_secs(5),
            retry_after: Duration::from_secs(10),
        }
    }

    #[test]
    fn decimal_token_conversion_is_exact_and_bounded() {
        assert_eq!(
            decimal_to_u256("1").unwrap(),
            format!("0x{}1", "0".repeat(63))
        );
        assert_eq!(
            decimal_to_u256(
                "115792089237316195423570985008687907853269984665640564039457584007913129639935"
            )
            .unwrap(),
            format!("0x{}", "f".repeat(64))
        );
        assert!(
            decimal_to_u256(
                "115792089237316195423570985008687907853269984665640564039457584007913129639936"
            )
            .is_err()
        );
    }

    #[test]
    fn query_binds_market_condition_and_ordered_tokens() {
        let query = query();
        let (key, tokens) = validate_query(&query).unwrap();
        assert!(key.starts_with("1|0x"));
        assert!(tokens[0].ends_with("0a") && tokens[1].ends_with("14"));
    }

    #[test]
    fn registered_operator_endpoints_promote_but_gateway_only_does_not() {
        let query = query();
        let key = validate_query(&query).unwrap().0;
        let left = provider_observation(0, &query, true);
        let mut right = provider_observation(1, &query, true);
        let verified = quorum_fact(
            &query,
            &key,
            "2026-01-01T00:00:00.000Z",
            &[left.clone(), right.clone()],
        )
        .unwrap();
        assert_eq!(verified["status"], "verified_final");
        assert_eq!(verified["settlement_import_allowed"], true);
        validate_verified_fact(&verified).unwrap();
        let first =
            chrono::DateTime::parse_from_rfc3339(verified["first_observed_at"].as_str().unwrap())
                .unwrap();
        let completed =
            chrono::DateTime::parse_from_rfc3339(verified["verified_at"].as_str().unwrap())
                .unwrap();
        assert!(first <= completed);
        assert!(
            verified["provider_observations"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| {
                    let observed = chrono::DateTime::parse_from_rfc3339(
                        row["observation"]["observed_at"].as_str().unwrap(),
                    )
                    .unwrap();
                    first <= observed && observed <= completed
                })
        );
        let mut inverted = verified.clone();
        inverted["verified_at"] = json!("2026-01-01T00:00:00.000Z");
        assert!(validate_verified_fact(&inverted).is_err());
        assert_eq!(
            verified["finality_policy"],
            "parallel_two_endpoints_at_least_one_valid_finalized_no_conflict_v2"
        );
        let gateway_left = provider_observation(0, &query, false);
        let gateway_right = provider_observation(1, &query, false);
        assert_eq!(
            quorum_fact(
                &query,
                &key,
                "2026-01-01T00:00:00.000Z",
                &[gateway_left, gateway_right],
            ),
            Err("two_gateway_matching_observations")
        );
        right["observation"]["payout_numerators"] = json!(["0", "1"]);
        right["observation_sha256"] = json!(sha(&serde_json::to_vec(&right["observation"]).unwrap()));
        right["provider_identity_evidence"]["observation_sha256"] = right["observation_sha256"].clone();
        assert_eq!(
            quorum_fact(&query, &key, "2026-01-01T00:00:00.000Z", &[left, right],),
            Err("finality_provider_disagreement")
        );
    }

    #[test]
    fn single_valid_finality_and_invalid_peer_do_not_require_quorum() {
        let query = query();
        let key = validate_query(&query).unwrap().0;
        let good = provider_observation(0, &query, true);
        let single = quorum_fact(&query, &key, "2026-01-01T00:00:00.000Z", &[good.clone()]).unwrap();
        assert_eq!(single["schema_version"], "marketcow.polymarket.verified-final.v2");
        assert_eq!(single["valid_provider_count"], 1);
        assert_eq!(single["provider_receipts"].as_array().unwrap().len(), 1);
        validate_verified_fact(&single).unwrap();
        let mut unresolved = provider_observation(1, &query, true);
        unresolved["observation"]["status"] = json!("unresolved");
        let fact = quorum_fact(&query, &key, "2026-01-01T00:00:00.000Z", &[good, unresolved]).unwrap();
        assert_eq!(fact["valid_provider_count"], 1);
        let mut tampered = fact;
        tampered["provider_receipts"][0]["block_hash"] = json!("forged");
        assert!(validate_verified_fact(&tampered).is_err());
    }

    #[test]
    fn profile_claims_and_forged_signed_shape_cannot_enable_quorum() {
        let query = query();
        let key = validate_query(&query).unwrap().0;
        let left = provider_observation(0, &query, false);
        let mut right = provider_observation(1, &query, false);
        let observation_sha256 = right["observation_sha256"].clone();
        right["provider_identity_evidence"] = json!({
            "schema_version":"marketcow.rpc.signed-provider-upstream-evidence.v1",
            "assurance":"cryptographically_verified_provider_upstream",
            "profile_claims_authoritative":false,
            "verifier_id":"unregistered-verifier",
            "provider_id":right["provider_id"],
            "upstream_id":"claimed-upstream",
            "operator_id":right["operator_id"],
            "failure_domain":right["failure_domain"],
            "observation_sha256":observation_sha256,
            "signing_key_registry_sha256":"a".repeat(64),
            "call_receipts":(0..RPC_CALLS).map(|_|json!({})).collect::<Vec<_>>()
        });
        assert_eq!(
            quorum_fact(&query, &key, "2026-01-01T00:00:00.000Z", &[left, right],),
            Err("two_gateway_matching_observations")
        );
    }

    #[test]
    fn legacy_verified_fact_with_gateway_only_identity_is_rejected() {
        let query = query();
        let observations = vec![
            provider_observation(0, &query, false),
            provider_observation(1, &query, false),
        ];
        let value = json!({
            "schema_version":"marketcow.polymarket.verified-final.v1",
            "status":"verified_final",
            "settlement_import_allowed":true,
            "provider_observations":observations
        });
        assert!(validate_verified_fact(&value).is_err());
    }

    #[test]
    fn profile_requires_private_permissions_and_hides_endpoint_from_projection() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("profile.json");
        let registry = &OPERATOR_REGISTRY[0];
        let mut value = json!({
            "schema_version":"marketcow.polymarket.finality-rpc-profile.v1",
            "provider_id":registry.provider_id,
            "operator_id":registry.operator_id,
            "failure_domain":registry.failure_domain,
            "rpc_endpoint":registry.endpoint_origin,
            "proxy_url":Value::Null,
            "chain_id":"0x89",
            "contract":format!("0x{}","a".repeat(40)),
            "code_sha256":"b".repeat(64),
            "finality_policy":"rpc_finalized_hash_pinned_v1",
            "collateral":format!("0x{}","c".repeat(40)),
            "adapter_mode":"standard_ctf_no_adapter"
        });
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let profile = load_profile(&path).unwrap();
        let projection =
            json!({"provider_id":profile.provider_id,"profile_sha256":profile.profile_sha256});
        assert!(!projection.to_string().contains("publicnode.com"));
        value["operator_id"] = json!("profile-self-claim");
        write_private(&path, &serde_json::to_vec(&value).unwrap());
        assert!(load_profile(&path).is_err());
        value["operator_id"] = json!(registry.operator_id);
        value["rpc_endpoint"] = json!(format!("{}/unregistered-path", registry.endpoint_origin));
        write_private(&path, &serde_json::to_vec(&value).unwrap());
        assert!(load_profile(&path).is_err());
        value["rpc_endpoint"] = json!(registry.endpoint_origin);
        write_private(&path, &serde_json::to_vec(&value).unwrap());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load_profile(&path).is_err());
    }

    #[tokio::test]
    async fn status_requires_scoped_caller_and_never_exposes_rpc_endpoint() {
        let root = tempfile::tempdir().unwrap();
        let service = Service::start(service_config(root.path())).await.unwrap();
        let request = || {
            axum::http::Request::builder()
                .uri("/v1/prediction-markets/polymarket/finality/status")
                .body(axum::body::Body::empty())
                .unwrap()
        };
        let denied = service.router().oneshot(request()).await.unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let authorized = service
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/prediction-markets/polymarket/finality/status")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        "Bearer a-private-token-with-at-least-32-bytes",
                    )
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authorized.status(), StatusCode::OK);
        let body = axum::body::to_bytes(authorized.into_body(), 64 * 1024)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body["maximum_verified_response_bytes"],
            VERIFIED_RESPONSE_CAP
        );
        assert_eq!(
            body["provider_identity_gate"]["verified_final_enabled"],
            true
        );
        assert_eq!(
            body["providers"][0]["identity_assurance"],
            "registry_bound_https_operator_endpoint"
        );
        assert!(!body.to_string().contains("publicnode.com"));
    }

    #[tokio::test]
    async fn gateway_observation_is_not_durable_until_writer_commits_and_recovery_checks_hash() {
        let root = tempfile::tempdir().unwrap();
        let config = service_config(root.path());
        let service = Service::start(config.clone()).await.unwrap();
        let query = query();
        let key = validate_query(&query).unwrap().0;
        let fact = pending_fact(
            &query,
            &key,
            "2026-01-01T00:00:00.000Z",
            "2026-01-01T00:00:01.000Z",
            "two_gateway_matching_observations",
            vec![
                provider_observation(0, &query, false),
                provider_observation(1, &query, false),
            ],
        );
        assert!(serde_json::to_vec(&fact).unwrap().len() <= VERIFIED_RESPONSE_CAP);
        let sequence = publish(&service.shared, key.clone(), fact).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if service.shared.state.read().await.durable == sequence {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let path = config.root.join("verified-final.jsonl");
        let recovered = recover(&path, config.archive_bytes).unwrap();
        assert_eq!(recovered.durable, 1);
        assert!(!recovered.durable_verified.contains_key(&key));
        let mut raw = std::fs::read(&path).unwrap();
        let index = raw.iter().position(|byte| *byte == b'2').unwrap();
        raw[index] = b'3';
        std::fs::write(&path, raw).unwrap();
        assert!(recover(&path, config.archive_bytes).is_err());
    }

    #[tokio::test]
    async fn registry_bound_verified_fact_becomes_durable_and_recovers() {
        let root = tempfile::tempdir().unwrap();
        let config = service_config(root.path());
        let service = Service::start(config.clone()).await.unwrap();
        let query = query();
        let key = validate_query(&query).unwrap().0;
        let fact = quorum_fact(
            &query,
            &key,
            "2026-01-01T00:00:00.000Z",
            &[
                provider_observation(0, &query, true),
                provider_observation(1, &query, true),
            ],
        )
        .unwrap();
        validate_verified_fact(&fact).unwrap();
        let sequence = publish(&service.shared, key.clone(), fact).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if service.shared.state.read().await.durable == sequence {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let recovered = recover(
            &config.root.join("verified-final.jsonl"),
            config.archive_bytes,
        )
        .unwrap();
        assert!(recovered.durable_verified.contains_key(&key));
    }
}
