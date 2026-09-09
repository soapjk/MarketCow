//! Observe-only bounded REST producer; no catalog traversal, account or order API.
use anyhow::{Context, Result, ensure};
use chrono::{Timelike, Utc};
use clap::{Parser, ValueEnum};
use marketcow_polymarket::discovery_source::{
    PreparedSnapshot, SnapshotBoundary, canonical_hash, prepare_snapshot,
};
use marketcow_runtime::discovery_source::PreparedSourceWriter;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fs, path::PathBuf, time::Duration};
use tokio::task::JoinSet;
#[allow(dead_code)]
#[path = "live_source_bridge.rs"]
mod durable_bootstrap;
mod source_dispatch;
mod source_lifecycle;
mod source_public_api;
mod source_price_history;
mod source_market_evidence;
mod source_scope_control;
mod source_scope_journal;
mod source_acquisition_shards;
mod source_rest_pool;
mod source_scope_registry;
mod source_public_frame;
mod source_public_binding;
mod source_discovery_quote;
mod source_discovery_history;
mod source_discovery_projection;
mod source_discovery_api;
mod source_discovery_startup;
mod source_publication;
mod source_websocket;

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
        tokio::select! {
            result = tokio::signal::ctrl_c() => { result.expect("SIGINT handler"); }
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.expect("shutdown handler");
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum InputMode {
    Websocket,
    RestPoll,
}

#[derive(Parser)]
struct Args {
    /// Upstream transport. Switching requires a controlled collector restart.
    #[arg(long, value_enum, default_value = "websocket")]
    input_mode: InputMode,
    /// CPU market actors; output reservations are bounded to twice this count.
    #[arg(long, default_value_t = 6)]
    market_workers: usize,
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    plan: PathBuf,
    #[arg(long)]
    plan_sha256: String,
    #[arg(long)]
    expected_market_count: usize,
    #[arg(long)]
    concurrency: usize,
    #[arg(long)]
    request_market_batch_size: usize,
    #[arg(long)]
    response_byte_limit: usize,
    #[arg(long)]
    batch_byte_limit: usize,
    #[arg(long)]
    persistence_queue_batches: usize,
    #[arg(long)]
    persistence_queue_bytes: usize,
    /// Explicit bounded durable history for prepared discovery or scoped sources.
    /// Readers must support recent_events; no JSONL append.
    #[arg(long)]
    bounded_history_bytes: Option<usize>,
    /// Maximum tokens per official upstream WS connection. Market pairs are
    /// never split across shards, so the minimum is two.
    #[arg(long, default_value_t = 50)]
    websocket_shard_tokens: usize,
    /// Bounded number of temporary authoritative snapshot subscriptions.
    #[arg(long, default_value_t = 8)]
    websocket_recovery_concurrency: usize,
    /// Periodic authoritative REST confirmation for quiet WS books. Matching
    /// snapshots refresh only memory freshness and never enter the event log.
    #[arg(long, default_value_t = 2)]
    websocket_confirmation_seconds: u64,
    #[arg(long, requires_all = ["dependency_plan", "live_frame_bytes", "live_maximum_clients"])]
    live_listen: Option<std::net::SocketAddr>,
    #[arg(long, requires = "live_listen")]
    live_frame_bytes: Option<usize>,
    #[arg(long, requires = "live_listen")]
    live_maximum_clients: Option<usize>,
    /// Direct Rust public read API; does not require the internal WS listener.
    #[arg(long, requires_all = ["configured_scope", "dependency_plan", "public_full_sync_bytes", "public_snapshot_concurrency", "public_frame_bytes", "public_replay_bytes", "public_maximum_clients", "public_send_timeout_seconds"])]
    public_listen: Option<std::net::SocketAddr>,
    #[arg(long, requires = "public_listen")]
    public_full_sync_bytes: Option<usize>,
    #[arg(long, requires = "public_listen")]
    public_snapshot_concurrency: Option<usize>,
    #[arg(long, requires = "public_listen")]
    public_frame_bytes: Option<usize>,
    #[arg(long, requires = "public_listen")]
    public_replay_bytes: Option<usize>,
    #[arg(long, requires = "public_listen")]
    public_maximum_clients: Option<usize>,
    #[arg(long, requires = "public_listen")]
    public_send_timeout_seconds: Option<u64>,
    /// Private publication-control socket. Does not prepare acquisition or expose a LAN manager.
    #[arg(long, requires_all=["scope_control_bytes", "scope_control_timeout_seconds", "scope_retire_grace_seconds", "acquisition_token_budget", "acquisition_socket_budget", "scope_state_file", "scope_state_bytes"])]
    scope_control_socket: Option<PathBuf>,
    #[arg(long,requires="scope_control_socket")]
    scope_state_file:Option<PathBuf>,
    #[arg(long,requires="scope_control_socket")]
    scope_state_bytes:Option<usize>,
    #[arg(long, requires="scope_control_socket")]
    scope_control_bytes: Option<usize>,
    #[arg(long, requires="scope_control_socket")]
    scope_control_timeout_seconds: Option<u64>,
    #[arg(long, requires="scope_control_socket")]
    scope_retire_grace_seconds: Option<u64>,
    /// Concurrent acquisition tokens, including preparation and retirement.
    #[arg(long, requires="scope_control_socket")]
    acquisition_token_budget:Option<usize>,
    #[arg(long, requires="scope_control_socket")]
    acquisition_socket_budget:Option<usize>,
    /// Independent direct Rust Discovery surface over this frozen universe.
    #[arg(long, conflicts_with_all=["configured_scope", "public_listen", "live_listen"], requires_all=["discovery_seed", "discovery_seed_sha256", "discovery_state_bytes", "discovery_full_sync_bytes", "discovery_frame_bytes", "discovery_replay_bytes", "discovery_clients", "discovery_baselines", "discovery_send_timeout_seconds"])]
    discovery_listen:Option<std::net::SocketAddr>,
    #[arg(long,requires="discovery_listen")]
    discovery_seed:Option<PathBuf>,
    #[arg(long,requires="discovery_listen")]
    discovery_seed_sha256:Option<String>,
    #[arg(long,requires="discovery_listen")]
    discovery_state_bytes:Option<usize>,
    #[arg(long,requires="discovery_listen")]
    discovery_full_sync_bytes:Option<usize>,
    #[arg(long,requires="discovery_listen")]
    discovery_frame_bytes:Option<usize>,
    #[arg(long,requires="discovery_listen")]
    discovery_replay_bytes:Option<usize>,
    #[arg(long,requires="discovery_listen")]
    discovery_clients:Option<usize>,
    #[arg(long,requires="discovery_listen")]
    discovery_baselines:Option<usize>,
    #[arg(long,requires="discovery_listen")]
    discovery_send_timeout_seconds:Option<u64>,
    #[arg(long)]
    poll_seconds: u64,
    #[arg(long)]
    request_timeout_seconds: u64,
    /// Finite verification runs only; omit for continuous collection.
    #[arg(long)]
    cycles: Option<u64>,
    /// Explicit scoped-source binding; never inferred from missing universe data.
    #[arg(long, requires = "configured_scope_sha256")]
    configured_scope: Option<PathBuf>,
    #[arg(long, requires = "configured_scope")]
    configured_scope_sha256: Option<String>,
    #[arg(long, requires_all = ["configured_scope", "dependency_plan_sha256"])]
    dependency_plan: Option<PathBuf>,
    #[arg(long, requires = "dependency_plan")]
    dependency_plan_sha256: Option<String>,
    /// Refresh only known failed market IDs; terminal evidence expires after this interval.
    #[arg(long, requires = "dependency_plan")]
    lifecycle_refresh_seconds: Option<u64>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Market {
    market_id: String,
    condition_id: String,
    token_ids: [String; 2],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    schema_version: String,
    catalog_revision: String,
    markets: Vec<Market>,
}

async fn fetch(
    client: reqwest::Client,
    markets: &[Market],
    limit: usize,
) -> Result<(Vec<Value>, chrono::DateTime<Utc>)> {
    let network_started = std::time::Instant::now();
    let request: Vec<_> = markets
        .iter()
        .flat_map(|market| &market.token_ids)
        .map(|token| json!({"token_id":token}))
        .collect();
    // The only network destination is the existing authoritative public read API.
    let mut response = client
        .post(marketcow_polymarket::CLOB_BOOK_SNAPSHOT_SOURCE_URL)
        .json(&request)
        .send()
        .await?
        .error_for_status()?;
    ensure!(
        response
            .content_length()
            .is_none_or(|bytes| bytes <= limit as u64),
        "source response exceeds byte budget"
    );
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            body.len()
                .checked_add(chunk.len())
                .is_some_and(|size| size <= limit),
            "source response exceeds byte budget"
        );
        body.extend_from_slice(&chunk);
    }
    let received = Utc::now();
    eprintln!(
        "{}",
        json!({"stage":"network_body","elapsed_us":network_started.elapsed().as_micros(),"response_bytes":body.len()})
    );
    let received = received
        .with_nanosecond(received.nanosecond() / 1000 * 1000)
        .context("invalid timestamp")?;
    let parse_started = std::time::Instant::now();
    let books: Vec<Value> =
        tokio::task::spawn_blocking(move || serde_json::from_slice(&body)).await??;
    eprintln!(
        "{}",
        json!({"stage":"response_decode","elapsed_us":parse_started.elapsed().as_micros()})
    );
    let returned: BTreeSet<_> = books.iter().map(|book| book["asset_id"].as_str()).collect();
    let expected: BTreeSet<_> = markets
        .iter()
        .flat_map(|market| &market.token_ids)
        .map(|token| Some(token.as_str()))
        .collect();
    ensure!(
        returned.len() == books.len() && returned.is_subset(&expected),
        "source returned duplicate or unrequested tokens"
    );
    Ok((books, received))
}

fn complete_market_books(market: &Market, books: &[Value]) -> Result<Vec<Value>> {
    let selected: Vec<_> = books
        .iter()
        .filter(|book| {
            market
                .token_ids
                .iter()
                .any(|token| book["asset_id"] == *token)
        })
        .cloned()
        .collect();
    let returned: BTreeSet<_> = selected
        .iter()
        .map(|book| book["asset_id"].as_str())
        .collect();
    ensure!(
        selected.len() == 2 && returned.len() == 2,
        "source did not return both token books: expected=2 returned={}",
        selected.len()
    );
    Ok(selected)
}

async fn fetch_identified(
    client: reqwest::Client,
    markets: Vec<Market>,
    limit: usize,
) -> Result<Vec<(String, Result<Vec<PreparedSnapshot>>)>> {
    let request_started = std::time::Instant::now();
    let fetched = fetch(client, &markets, limit).await;
    let request_us = request_started.elapsed().as_micros();
    tokio::task::spawn_blocking(move || {
    let normalize_started=std::time::Instant::now();
    let recovery=uuid::Uuid::new_v4().to_string();
    let result=markets
        .into_iter()
        .map(|market| {
            let id = market.market_id.clone();
            let result = match &fetched {
                Err(error) => Err(anyhow::anyhow!("{error:#}")),
                Ok((books, received)) => complete_market_books(&market, books).and_then(|selected| {
                    selected.iter().map(|raw| Ok(prepare_snapshot(raw,SnapshotBoundary {
                        market_id:&market.market_id,condition_id:&market.condition_id,
                        token_id:raw["asset_id"].as_str().context("token")?,recovery_id:&recovery,
                        cursor:1,received_at:*received,
                    })?)).collect()
                }),
            };
            (id, result)
        })
        .collect();
    eprintln!("{}",json!({"stage":"fetch_and_prepare","request_and_decode_us":request_us,"normalize_us":normalize_started.elapsed().as_micros()}));
    result
    }).await.context("snapshot preparation worker panicked")
}

enum PreparedMessage {
    Books {
        batch: usize,
        cycle: u64,
        elapsed_us: u128,
        values: Vec<(String, Result<Vec<PreparedSnapshot>>)>,
    },
    Terminal(Value),
}

fn validate_scope_binding(scope: &Value, plan: &Plan) -> Result<()> {
    ensure!(
        scope["schema_version"] == "marketcow.polymarket.scope-discovery.v1"
            && scope["mode"] == "shadow"
            && scope["catalog_revision"] == plan.catalog_revision,
        "scope contract/catalog differs"
    );
    let configured = scope["configured_markets"]
        .as_array()
        .context("missing configured markets")?;
    ensure!(
        scope["configured_market_count"].as_u64() == Some(configured.len() as u64)
            && configured.len() == plan.markets.len(),
        "configured scope count differs"
    );
    ensure!(
        scope["active_scope_id"]
            == canonical_hash(&json!({
                "catalog_revision": scope["catalog_revision"],
                "configured_markets": configured, "mode": "shadow"
            })),
        "scope content hash differs"
    );
    for market in &plan.markets {
        let matches: Vec<_> = configured
            .iter()
            .filter(|v| v["market_id"] == market.market_id)
            .collect();
        ensure!(
            matches.len() == 1,
            "plan market missing/duplicated in configured scope"
        );
        let entry = matches[0];
        ensure!(
            entry["condition_id"] == market.condition_id
                && entry["token_ids"] == json!(market.token_ids),
            "configured market identity differs"
        );
    }
    Ok(())
}

const MAX_DEPENDENCY_MARKETS: usize = 1024;
const MAX_DEPENDENCY_PLAN_BYTES: u64 = 32 * 1024 * 1024;

fn dependency_closure(configured: &[Market], raw: &Value) -> Result<Vec<Market>> {
    let values = raw.as_array().context("missing dependency markets")?;
    ensure!(
        values.len() <= MAX_DEPENDENCY_MARKETS,
        "dependency market cap"
    );
    let mut catalog = std::collections::BTreeMap::new();
    for value in values {
        let id = value["identity"]["market_id"]
            .as_str()
            .context("missing dependency identity")?;
        ensure!(
            catalog.insert(id, value).is_none(),
            "duplicate dependency identity"
        );
    }
    let mut required: BTreeSet<String> = configured.iter().map(|m| m.market_id.clone()).collect();
    let mut visited = BTreeSet::new();
    while let Some(id) = required.iter().find(|id| !visited.contains(*id)).cloned() {
        let value = catalog
            .get(id.as_str())
            .context("missing required relation dependency")?;
        for relation in value["relations"]
            .as_array()
            .context("missing explicit relations")?
        {
            for pair in relation["outcome_pairs"]
                .as_array()
                .context("missing relation outcome pairs")?
            {
                required.insert(
                    pair["market_id"]
                        .as_str()
                        .context("missing dependency id")?
                        .to_owned(),
                );
            }
        }
        ensure!(
            required.len() <= MAX_DEPENDENCY_MARKETS,
            "dependency closure cap"
        );
        visited.insert(id);
    }
    let mut out = Vec::new();
    let mut tokens = BTreeSet::new();
    for id in required {
        let identity = &catalog[id.as_str()]["identity"];
        let outcomes = identity["outcomes"]
            .as_array()
            .context("missing binary outcomes")?;
        ensure!(outcomes.len() == 2, "dependency not binary");
        let token_ids = [
            outcomes[0]["token_id"]
                .as_str()
                .context("missing token")?
                .to_owned(),
            outcomes[1]["token_id"]
                .as_str()
                .context("missing token")?
                .to_owned(),
        ];
        ensure!(
            token_ids.iter().all(|t| !t.is_empty()
                && t.bytes().all(|b| b.is_ascii_digit())
                && tokens.insert(t.clone())),
            "invalid or duplicate dependency token"
        );
        let market = Market {
            market_id: id,
            condition_id: identity["condition_id"]
                .as_str()
                .context("missing condition")?
                .to_owned(),
            token_ids,
        };
        if let Some(original) = configured.iter().find(|m| m.market_id == market.market_id) {
            ensure!(
                original.condition_id == market.condition_id
                    && original.token_ids == market.token_ids,
                "configured identity differs from dependency catalog"
            );
        }
        out.push(market);
    }
    Ok(out)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        (1..=32).contains(&args.concurrency)
            && (1..=8).contains(&args.market_workers)
            && (1..=20).contains(&args.request_market_batch_size)
            && (2..=500).contains(&args.websocket_shard_tokens)
            && (1..=32).contains(&args.websocket_recovery_concurrency)
            && (1..=300).contains(&args.websocket_confirmation_seconds)
            && args.poll_seconds > 0
            && args.request_timeout_seconds > 0,
        "explicit positive concurrency/timeout/poll configuration required"
    );
    ensure!(
        args.expected_market_count > 0 && args.response_byte_limit > 0 && args.batch_byte_limit > 0,
        "explicit universe count and byte budgets required"
    );
    ensure!(args.cycles != Some(0), "cycles must be positive");
    ensure!(
        args.lifecycle_refresh_seconds != Some(0),
        "lifecycle interval must be positive"
    );
    ensure!(
        fs::metadata(&args.plan)?.len() <= 32 * 1024 * 1024,
        "oversized startup plan"
    );
    let encoded = fs::read(&args.plan)?;
    ensure!(
        hex::encode(Sha256::digest(&encoded)) == args.plan_sha256,
        "startup plan hash mismatch"
    );
    let mut plan: Plan = serde_json::from_slice(&encoded)?;
    ensure!(
        plan.schema_version
            == if args.configured_scope.is_some() {
                "marketcow.polymarket.rust-scoped-source-plan.v1"
            } else {
                "marketcow.polymarket.rust-discovery-source-plan.v1"
            },
        "unsupported startup plan"
    );
    ensure!(
        plan.markets.len() == args.expected_market_count,
        "universe count differs from explicit configuration"
    );
    let mut markets = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    for market in &plan.markets {
        ensure!(
            !market.market_id.is_empty()
                && !market.condition_id.is_empty()
                && markets.insert(&market.market_id),
            "invalid or duplicate market"
        );
        for token in &market.token_ids {
            ensure!(
                !token.is_empty()
                    && token.bytes().all(|b| b.is_ascii_digit())
                    && tokens.insert(token),
                "invalid or duplicate token"
            );
        }
    }
    let manifest: Value = serde_json::from_slice(&fs::read(args.root.join("catalog.json"))?)?;
    ensure!(
        manifest["catalog_revision"] == plan.catalog_revision,
        "source catalog revision differs"
    );
    let mut public_scope = None;
    if let Some(path) = &args.configured_scope {
        ensure!(
            fs::canonicalize(path)?.starts_with(fs::canonicalize(&args.root)?),
            "configured scope escapes source root"
        );
        ensure!(
            fs::metadata(path)?.len() <= 1024 * 1024,
            "oversized configured scope"
        );
        let bytes = fs::read(path)?;
        let hash = hex::encode(Sha256::digest(&bytes));
        ensure!(
            Some(&hash) == args.configured_scope_sha256.as_ref(),
            "configured scope file hash differs"
        );
        let scope: Value = serde_json::from_slice(&bytes)?;
        let runtime: Value =
            serde_json::from_slice(&fs::read(args.root.join("scope-runtime.json"))?)?;
        ensure!(
            runtime["manifest_sha256"] == hash && runtime["scope_id"] == scope["active_scope_id"],
            "scope runtime binding differs"
        );
        validate_scope_binding(&scope, &plan)?;
        if args.public_listen.is_some() {
            let typed: source_public_api::ConfiguredScope = serde_json::from_slice(&bytes)?;
            typed.validate()?;
            public_scope = Some(typed);
        }
    } else {
        let declared: BTreeSet<_> = manifest["realtime_universe"]["market_ids"]
            .as_array()
            .context("missing explicit realtime universe")?
            .iter()
            .map(|v| v.as_str().context("invalid market id"))
            .collect::<Result<_>>()?;
        ensure!(
            declared == markets.iter().map(|s| s.as_str()).collect(),
            "plan differs from frozen universe"
        );
    }
    let mut writer = PreparedSourceWriter::open(&args.root, args.batch_byte_limit)?;
    let mut lifecycle_markets = std::collections::BTreeMap::new();
    if let Some(path) = &args.dependency_plan {
        ensure!(
            fs::metadata(path)?.len() <= MAX_DEPENDENCY_PLAN_BYTES,
            "dependency plan byte cap"
        );
        let bytes = fs::read(path)?;
        let hash = hex::encode(Sha256::digest(&bytes));
        ensure!(
            Some(&hash) == args.dependency_plan_sha256.as_ref(),
            "dependency plan hash differs"
        );
        let dependency: Value = serde_json::from_slice(&bytes)?;
        let runtime: Value =
            serde_json::from_slice(&fs::read(args.root.join("scope-runtime.json"))?)?;
        ensure!(
            dependency["schema_version"] == "marketcow.polymarket.live-bridge-plan.v1"
                && dependency["catalog_revision"] == plan.catalog_revision
                && dependency["scope_id"] == runtime["scope_id"]
                && dependency["catalog_manifest_sha256"]
                    == hex::encode(Sha256::digest(fs::read(args.root.join("catalog.json"))?)),
            "dependency plan source binding differs"
        );
        plan.markets = dependency_closure(&plan.markets, &dependency["markets"])?;
        for market in dependency["markets"]
            .as_array()
            .context("missing lifecycle catalog")?
        {
            lifecycle_markets.insert(
                market["identity"]["market_id"]
                    .as_str()
                    .context("missing market id")?
                    .to_owned(),
                market.clone(),
            );
        }
    }
    let mut scope_boot=if let Some(path)=&args.scope_state_file {
        source_scope_journal::read(path,args.scope_state_bytes.context("scope state bytes")?)?
    }else{None};
    let manifest_sha=hex::encode(Sha256::digest(fs::read(args.root.join("catalog.json"))?));
    if let Some(state)=scope_boot.as_mut() {
        ensure!(state.catalog_revision==plan.catalog_revision&&state.catalog_manifest_sha256==manifest_sha
            && state.pool==if args.discovery_listen.is_some(){"discovery"}else{"live"},"runtime source differs");
        let bindings=state.records.iter().chain(&state.retiring_records).filter(|(id,_)|
            state.acquisition_market_ids.contains(*id)||state.retiring_records.contains_key(*id))
            .flat_map(|(id,record)|record["identity"]["outcomes"].as_array().unwrap().iter()
                .map(move|outcome|(outcome["token_id"].as_str().unwrap().to_owned(),id.clone()))).collect();
        writer.bind_catalog_tokens(bindings)?;
        if !state.retiring_records.is_empty() {
            // Startup-only completion of a previously durable control intent.
            // Recent history is retained; no listener exists at this point.
            writer.retire_catalog_markets(writer.cursor()?,&state.retiring_records.keys().cloned().collect())?;
            state.retiring_records.clear();state.retirement_ticket=None;
        }
        plan.markets=state.markets()?;
        ensure!(plan.markets.len()*2<=args.acquisition_token_budget.context("runtime acquisition budget")?,"runtime acquisition capacity");
        lifecycle_markets=state.records.clone();
    }
    // Both cold Discovery and scoped Live writers require the verified plan
    // identity map. Existing books are not a substitute for catalog binding;
    // Discovery has no dependency-plan branch on a fresh generation.
    writer.bind_catalog_tokens(
        plan.markets
            .iter()
            .flat_map(|m| m.token_ids.iter().map(|t| (t.clone(), m.market_id.clone())))
            .collect(),
    )?;
    // Do not migrate durable history until the complete source binding is valid.
    if let Some(limit) = args.bounded_history_bytes {
        writer.enable_bounded_history(limit)?;
    }
    let cursor = writer.cursor()?;
    let mut discovery_config=None;
    let scope_revision=scope_boot.as_ref().map_or(1,|state|state.active_revision);
    let base = if let Some(state)=scope_boot.as_mut() {
        if state.pool=="live" {
            let scope:source_public_api::ConfiguredScope=serde_json::from_value(state.active_config.clone())?;
            scope.validate()?;public_scope=Some(scope);
        } else {
            let mut config:source_discovery_projection::DiscoveryConfig=serde_json::from_value(state.active_config.clone())?;
            config.projection_id=canonical_hash(&json!({"previous":config.projection_id,"instance":uuid::Uuid::new_v4().simple().to_string()}));
            state.active_config=serde_json::to_value(&config)?;discovery_config=Some(config);
        }
        Some(durable_bootstrap::bootstrap_managed(args.root.clone(),state.catalog_revision.clone(),manifest_sha.clone(),
            state.records.values().cloned().collect(),state.catalog_source.clone(),
            if state.pool=="live"{args.public_full_sync_bytes.context("public state bytes")?}else{args.discovery_state_bytes.context("Discovery state bytes")?},
            if state.pool=="live"{Some(public_scope.as_ref().context("managed Live scope")?.active_scope_id.clone())}else{None})?)
    } else if args.discovery_listen.is_some() {
        let (config,base)=source_discovery_startup::load(&args.root,args.discovery_seed.as_deref().context("Discovery seed")?,
            args.discovery_seed_sha256.as_deref().context("Discovery seed hash")?,args.discovery_state_bytes.context("Discovery state cap")?,&plan.markets)?;
        discovery_config=Some(config);Some(base)
    } else if args.live_listen.is_some() || args.public_listen.is_some() {
        Some(durable_bootstrap::bootstrap(
            args.root.clone(),
            args.dependency_plan
                .clone()
                .context("missing stream plan")?,
            args.dependency_plan_sha256
                .as_deref()
                .context("missing stream plan hash")?,
            if args.public_listen.is_some() { args.public_full_sync_bytes.context("public seed cap")? }
                else { args.live_frame_bytes.context("missing frame cap")? },
        )?)
    } else {
        None
    };
    let journal=if let Some(path)=&args.scope_state_file {
        let initial=if let Some(state)=scope_boot {state}else{
            let base=base.as_ref().context("managed source baseline required")?;
            source_scope_journal::State{
                schema_version:"marketcow.runtime-scope-state.v1".into(),catalog_revision:plan.catalog_revision.clone(),
                catalog_manifest_sha256:manifest_sha,pool:if args.discovery_listen.is_some(){"discovery"}else{"live"}.into(),
                active_config:if let Some(config)=&discovery_config{serde_json::to_value(config)?}else{serde_json::to_value(public_scope.as_ref().context("public config")?)?},
                active_revision:scope_revision,catalog_source:base["catalog_source"].clone(),
                records:base["markets"].as_array().context("managed metadata")?.iter().map(|record|Ok((record["identity"]["market_id"].as_str().context("managed market")?.into(),record.clone()))).collect::<Result<_>>()?,
                acquisition_market_ids:plan.markets.iter().map(|m|m.market_id.clone()).collect(),retiring_records:Default::default(),retirement_ticket:None,
            }
        };
        let mut journal=source_scope_journal::Journal::open(path.clone(),args.scope_state_bytes.context("scope state bytes")?,initial.clone())?;
        // Persist completed startup retirement and the new Discovery instance.
        journal.commit(initial)?;Some(journal)
    }else{None};
    let bindings = plan
        .markets
        .iter()
        .flat_map(|m| m.token_ids.iter().map(|t| (t.clone(), m.market_id.clone())))
        .collect();
    let persistence_root = args.root.clone();
    let persistence_catalog = plan.catalog_revision.clone();
    let ws_seed = base.clone();
    let mut publication = source_publication::Publication::start_managed(
        cursor,
        base,
        bindings,
        args.persistence_queue_batches,
        args.persistence_queue_bytes,
        args.batch_byte_limit,
        move |work| {
            let batches=match work {
                source_publication::PersistenceWork::Events(batches)=>batches,
                source_publication::PersistenceWork::Retire(retirement)=>{
                    writer.bind_catalog_tokens(retirement.catalog_bindings.clone())?;
                    return writer.retire_catalog_markets(retirement.after,&retirement.markets);
                }
            };
            for batch in batches {
                writer.bind_catalog_tokens(batch.catalog_bindings.clone())?;
                if let Some(evidence) = &batch.evidence {
                    source_lifecycle::persist(&persistence_root, &persistence_catalog, evidence)?;
                }
            }
            writer.append_validated_group(&batches.iter().map(|b| &b.validated).collect::<Vec<_>>())
        },
    )?;
    if args.input_mode == InputMode::Websocket {
        publication.set_source_ready(false);
    }
    let stream_task = if let Some(listen) = args.live_listen {
        ensure!(
            listen.ip().is_loopback(),
            "collector stream must be loopback"
        );
        let router = publication.router(
            args.live_frame_bytes.context("frame cap")?,
            args.live_maximum_clients.context("client cap")?,
        )?;
        let listener = tokio::net::TcpListener::bind(listen).await?;
        Some(tokio::spawn(
            async move { axum::serve(listener, router).await },
        ))
    } else {
        None
    };
    ensure!(args.scope_control_socket.is_none() || args.public_listen.is_some() || args.discovery_listen.is_some(),
        "scope control requires a public pool");
    let mut control_backend = None;
    let public_task = if let Some(listen) = args.public_listen {
        ensure!(listen.ip().is_loopback() || matches!(listen.ip(), std::net::IpAddr::V4(ip) if ip.is_private()),
            "public read API must bind explicit loopback or private LAN address");
        let (router,control) = source_public_api::PublicApi::router_managed(publication.reader(),public_scope.context("public scope")?,
            scope_revision,uuid::Uuid::new_v4().simple().to_string(),args.public_full_sync_bytes.context("public bytes")?,
            args.public_snapshot_concurrency.context("snapshot concurrency")?,source_public_api::StreamLimits {
                frame_bytes:args.public_frame_bytes.context("public frame bytes")?,
                replay_bytes:args.public_replay_bytes.context("public replay bytes")?,
                clients:args.public_maximum_clients.context("public clients")?,
                send_timeout:Duration::from_secs(args.public_send_timeout_seconds.context("send timeout")?),
            },Duration::from_secs(args.scope_retire_grace_seconds.unwrap_or(args.public_send_timeout_seconds.context("send timeout")?)))?;
        control_backend = Some(source_scope_control::Backend::Live(control));
        let listener = tokio::net::TcpListener::bind(listen).await?;
        Some(tokio::spawn(async move {axum::serve(listener,router).await}))
    } else {None};
    let discovery_task=if let Some(listen)=args.discovery_listen {
        ensure!(listen.ip().is_loopback() || matches!(listen.ip(),std::net::IpAddr::V4(ip) if ip.is_private()),"Discovery requires explicit loopback/private address");
        let (router,control)=source_discovery_api::router_managed_at(publication.reader(),std::sync::Arc::new(discovery_config.context("Discovery config")?),
            source_discovery_api::DiscoveryLimits{full_sync_bytes:args.discovery_full_sync_bytes.context("Discovery response cap")?,
                frame_bytes:args.discovery_frame_bytes.context("Discovery frame cap")?,state_bytes:args.discovery_state_bytes.context("Discovery state cap")?,
                replay_bytes:args.discovery_replay_bytes.context("Discovery replay cap")?,clients:args.discovery_clients.context("Discovery clients")?,
                cached_baselines:args.discovery_baselines.context("Discovery baselines")?,send_timeout:Duration::from_secs(args.discovery_send_timeout_seconds.context("Discovery send timeout")?)},
            Duration::from_secs(args.scope_retire_grace_seconds.unwrap_or(args.discovery_send_timeout_seconds.context("Discovery send timeout")?)),scope_revision)?;
        control_backend=Some(source_scope_control::Backend::Discovery(control));
        let listener=tokio::net::TcpListener::bind(listen).await?;
        Some(tokio::spawn(async move{axum::serve(listener,router).await}))
    }else{None};
    let (acquisition_sender,acquisition_receiver)=tokio::sync::mpsc::channel(1);
    let control_task = if let Some(path) = &args.scope_control_socket {
        Some(source_scope_control::start(path,args.scope_control_bytes.context("control bytes")?,
            Duration::from_secs(args.scope_control_timeout_seconds.context("control timeout")?),
            control_backend.context("control backend")?,
            Some(acquisition_sender),journal)?)
    } else {None};
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.request_timeout_seconds))
        .build()?;
    if args.input_mode == InputMode::Websocket {
        let result = source_websocket::run(
            &args,
            &plan,
            ws_seed.context(
                "WebSocket requires an explicit live dependency plan and seeded read stream",
            )?,
            &mut publication,
            if args.scope_control_socket.is_some(){Some(acquisition_receiver)}else{None},
        )
        .await;
        if let Some(task) = stream_task {
            task.abort();
        }
        if let Some(task) = public_task { task.abort(); }
        if let Some(task) = discovery_task { task.abort(); }
        if let Some(task) = control_task { task.abort(); let _ = task.await; }
        let drain = publication.finish().await;
        result?;
        drain?;
        return Ok(());
    }
    if args.scope_control_socket.is_some() {
        let result=source_rest_pool::run(&args,&plan,&mut publication,acquisition_receiver).await;
        if let Some(task)=stream_task{task.abort();}
        if let Some(task)=public_task{task.abort();}
        if let Some(task)=discovery_task{task.abort();}
        if let Some(task)=control_task{task.abort();let _=task.await;}
        let drain=publication.finish().await;result?;drain?;return Ok(());
    }
    // Managed shutdown completes the current durability cycle before releasing the lease.
    let stopping = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let signal_flag = stopping.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        signal_flag.store(true, std::sync::atomic::Ordering::Relaxed);
    });
    // Each fixed batch has its own deadline. A slow request never creates a
    // universe-wide round barrier. Channel reservations bound completed work.
    let (sender, mut receiver) = tokio::sync::mpsc::channel(args.concurrency);
    let network = std::sync::Arc::new(tokio::sync::Semaphore::new(args.concurrency));
    let mut workers = JoinSet::new();
    for (batch, markets) in plan
        .markets
        .chunks(args.request_market_batch_size)
        .enumerate()
    {
        let markets = markets.to_vec();
        let client = client.clone();
        let sender = sender.clone();
        let network = network.clone();
        let stopping = stopping.clone();
        let limit = args.response_byte_limit;
        let poll = args.poll_seconds;
        let cycles = args.cycles;
        let refresh = args.lifecycle_refresh_seconds;
        workers.spawn(async move {
            let mut cycle=0; let mut terminal=std::collections::BTreeMap::new();
            loop {
                if stopping.load(std::sync::atomic::Ordering::Relaxed) {break;}
                terminal.retain(|_,at: &mut tokio::time::Instant| refresh.is_some_and(|s| at.elapsed()<Duration::from_secs(s)));
                let active: Vec<_>=markets.iter().filter(|m|!terminal.contains_key(&m.market_id)).cloned().collect();
                let slot=sender.clone().reserve_owned().await?;
                let started=tokio::time::Instant::now();
                let values=if active.is_empty() {Vec::new()} else {
                    let _permit=network.acquire().await?;
                    fetch_identified(client.clone(),active,limit).await?
                };
                let failed: Vec<_>=values.iter().filter(|(_,v)|v.is_err()).map(|(id,_)|id.clone()).collect();
                cycle+=1;
                slot.send(PreparedMessage::Books {batch,cycle,elapsed_us:started.elapsed().as_micros(),values});
                // Lifecycle reads affect only this batch, never stall other batches.
                if refresh.is_some() {
                    for id in failed {
                        let market=markets.iter().find(|m|m.market_id==id).context("failed identity")?.clone();
                        let permit=network.acquire().await?;
                        let observation=source_lifecycle::refresh(client.clone(),market,limit).await;
                        drop(permit);
                        match observation {
                            Ok(Some(evidence)) => {
                                sender.send(PreparedMessage::Terminal(evidence)).await?;
                                terminal.insert(id,tokio::time::Instant::now());
                            },
                            Ok(None)=>{},
                            Err(error)=>eprintln!("{}",json!({"lifecycle_refresh_error":format!("{error:#}"),"market_id":id})),
                        }
                    }
                }
                if cycles.is_some_and(|n|cycle>=n) {break;}
                tokio::time::sleep_until(started+Duration::from_secs(poll)).await;
            }
            Ok::<(),anyhow::Error>(())
        });
    }
    drop(sender);
    let collection_result: Result<()> = async {
        loop {
            let message=tokio::select! {
                message=receiver.recv()=>match message {Some(value)=>value,None=>break},
                result=workers.join_next(), if !workers.is_empty()=>{result.context("worker disappeared")???;continue;}
            };
            let started=std::time::Instant::now();
            match message {
                PreparedMessage::Books {batch,cycle,elapsed_us,values} => {
                    let mut accepted=0; let mut rejected=0;
                    for (id,value) in values {
                        match value {
                            Ok(drafts) => {
                                let cursor=publication.cursor()?;
                                let events=drafts.into_iter().enumerate().map(|(i,d)| {
                                    Ok(d.finalize(cursor.checked_add(i as u64+1).context("cursor overflow")?)?)
                                }).collect::<Result<Vec<_>>>()?;
                                publication.publish(events,None)?; accepted+=1;
                            },
                            Err(error) => {rejected+=1; eprintln!("{}",json!({"market_id":id,"source_error":format!("{error:#}")}));}
                        }
                    }
                    println!("{}",json!({"batch":batch,"batch_cycle":cycle,"request_wait_and_prepare_us":elapsed_us,
                        "ordered_publication_us":started.elapsed().as_micros(),"accepted_markets":accepted,"rejected_markets":rejected,
                        "cursor":publication.cursor()?,"persisted_cursor":publication.persisted_cursor(),
                        "configured_plan_market_count":args.expected_market_count,"collection_market_count":plan.markets.len(),"trading_enabled":false}));
                },
                PreparedMessage::Terminal(evidence) => {
                    let id=evidence["market_id"].as_str().context("terminal market")?;
                    let event=source_lifecycle::terminal_event(lifecycle_markets.get(id).context("lifecycle identity")?,
                        &evidence,publication.cursor()?.checked_add(1).context("cursor overflow")?)?;
                    publication.publish(vec![event],Some(evidence))?;
                }
            }
        }
        while let Some(result)=workers.join_next().await {result??;}
        Ok(())
    }.await;
    workers.abort_all();
    if let Some(task) = public_task { task.abort(); }
    if let Some(task) = discovery_task { task.abort(); }
    if let Some(task) = control_task { task.abort(); let _ = task.await; }
    if let Some(task) = stream_task {
        task.abort();
    }
    // Even a rejected batch must drain previously accepted data before normal exit.
    // The stream closes first, so downstream never mistakes a stopped source for healthy.
    let drain_result = publication.finish().await;
    collection_result?;
    drain_result?;
    Ok(())
}

#[cfg(test)]
mod scope_tests {
    use super::*;

    #[test]
    fn upstream_defaults_to_websocket_with_explicit_rest_option() {
        use clap::CommandFactory;
        let command = Args::command();
        let mode = command
            .get_arguments()
            .find(|a| a.get_id() == "input_mode")
            .unwrap();
        assert_eq!(
            mode.get_default_values(),
            [std::ffi::OsString::from("websocket")]
        );
        assert_eq!(
            InputMode::from_str("rest-poll", false).unwrap(),
            InputMode::RestPoll
        );
        assert!(InputMode::from_str("auto", false).is_err());
        let defaults = Args::try_parse_from([
            "collector",
            "--root",
            "/tmp/root",
            "--plan",
            "/tmp/plan",
            "--plan-sha256",
            "hash",
            "--expected-market-count",
            "1",
            "--concurrency",
            "1",
            "--request-market-batch-size",
            "1",
            "--response-byte-limit",
            "1",
            "--batch-byte-limit",
            "1",
            "--persistence-queue-batches",
            "1",
            "--persistence-queue-bytes",
            "1",
            "--poll-seconds",
            "1",
            "--request-timeout-seconds",
            "1",
        ])
        .unwrap();
        assert_eq!(defaults.websocket_shard_tokens, 50);
        assert_eq!(defaults.websocket_recovery_concurrency, 8);
        assert_eq!(defaults.websocket_confirmation_seconds, 2);
    }

    #[test]
    fn hydration_adds_required_dependencies_not_unrelated_candidates() {
        let configured = vec![Market {
            market_id: "1".into(),
            condition_id: "c1".into(),
            token_ids: ["11".into(), "12".into()],
        }];
        let raw = json!([
            {"identity":{"market_id":"1","condition_id":"c1","outcomes":[{"token_id":"11"},{"token_id":"12"}]},"relations":[{"outcome_pairs":[{"market_id":"2"}]}]},
            {"identity":{"market_id":"2","condition_id":"c2","outcomes":[{"token_id":"21"},{"token_id":"22"}]},"relations":[]},
            {"identity":{"market_id":"3","condition_id":"c3","outcomes":[{"token_id":"31"},{"token_id":"32"}]},"relations":[]}
        ]);
        let hydrated = dependency_closure(&configured, &raw).unwrap();
        assert_eq!(
            hydrated
                .iter()
                .map(|m| m.market_id.as_str())
                .collect::<Vec<_>>(),
            vec!["1", "2"]
        );
        let mut missing = raw.clone();
        missing.as_array_mut().unwrap().remove(1);
        assert!(dependency_closure(&configured, &missing).is_err());
        let mut changed = raw.clone();
        changed[0]["identity"]["condition_id"] = json!("other");
        assert!(dependency_closure(&configured, &changed).is_err());
    }

    #[test]
    fn partial_batch_rejects_only_incomplete_market_without_placeholder() {
        let market = Market {
            market_id: "1".into(),
            condition_id: "c".into(),
            token_ids: ["11".into(), "12".into()],
        };
        assert!(complete_market_books(&market, &[json!({"asset_id":"11"})]).is_err());
        assert!(
            complete_market_books(
                &market,
                &[json!({"asset_id":"11"}), json!({"asset_id":"11"})]
            )
            .is_err()
        );
        let books = vec![
            json!({"asset_id":"11"}),
            json!({"asset_id":"12"}),
            json!({"asset_id":"21"}),
        ];
        assert_eq!(complete_market_books(&market, &books).unwrap().len(), 2);
        let other = Market {
            market_id: "2".into(),
            condition_id: "d".into(),
            token_ids: ["21".into(), "22".into()],
        };
        assert!(complete_market_books(&other, &books).is_err());
    }

    #[test]
    fn scoped_plan_requires_exact_content_and_token_binding() {
        let plan = Plan {
            schema_version: "marketcow.polymarket.rust-scoped-source-plan.v1".into(),
            catalog_revision: "a".repeat(64),
            markets: vec![Market {
                market_id: "1".into(),
                condition_id: "condition".into(),
                token_ids: ["11".into(), "12".into()],
            }],
        };
        let mut scope = json!({"schema_version":"marketcow.polymarket.scope-discovery.v1",
            "mode":"shadow","catalog_revision":plan.catalog_revision,"configured_market_count":1,
            "configured_markets":[{"market_id":"1","condition_id":"condition","token_ids":["11","12"],"end_at":"2027-01-01T00:00:00Z"}]});
        scope["active_scope_id"] = json!(canonical_hash(
            &json!({"catalog_revision":scope["catalog_revision"],
            "configured_markets":scope["configured_markets"],"mode":"shadow"})
        ));
        validate_scope_binding(&scope, &plan).unwrap();
        scope["configured_market_count"] = json!(2);
        assert!(validate_scope_binding(&scope, &plan).is_err());
        scope["configured_market_count"] = json!(1);
        scope["configured_markets"][0]["token_ids"] = json!(["12", "11"]);
        assert!(validate_scope_binding(&scope, &plan).is_err());
        scope["active_scope_id"] = json!(canonical_hash(
            &json!({"catalog_revision":scope["catalog_revision"],
            "configured_markets":scope["configured_markets"],"mode":"shadow"})
        ));
        assert!(validate_scope_binding(&scope, &plan).is_err());
    }
}
