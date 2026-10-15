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
use std::{collections::{BTreeMap, BTreeSet}, fs, path::PathBuf, time::Duration};
use tokio::task::JoinSet;
mod btc_research;
mod source_snapshot;
#[allow(dead_code)]
#[path = "live_source_bridge.rs"]
mod durable_bootstrap;
mod source_acquisition_lease;
mod source_acquisition_shards;
#[allow(dead_code)]
mod source_btc5m_api;
mod source_market_live;
mod source_market_live_v7;
mod source_facts_typed;
mod source_facts_identity;
mod source_facts_rest;
mod btc_typed;
mod btc_typed_store;
mod btc_typed_history;
mod source_typed_identity;
mod btc_typed_api;
mod btc_typed_ingress;
mod source_market_send_diagnostic;
mod source_btc5m_reference;
mod source_btc5m_proto;
mod source_btc5m_manager;
mod btc_chainlink;
mod btc_trade;
mod source_cold_catalog;
mod source_discovery_api;
mod source_discovery_history;
mod source_discovery_projection;
mod source_discovery_quote;
mod source_discovery_startup;
mod source_dispatch;
mod source_finality_reader;
mod source_finality_service;
mod source_lifecycle;
mod source_market_evidence;
mod source_market_replay;
mod source_market_persistence;
mod source_price_history;
#[allow(dead_code)]
mod source_polynode_pn1;
mod source_polynode_service;
mod source_official_ws_relay;
mod source_public_api;
mod source_public_binding;
mod source_public_frame;
mod source_publication;
mod source_rest_pool;
mod source_scope_control;
mod source_scope_journal;
mod source_scope_migration;
mod source_scope_registry;
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

fn websocket_seed(
    base: Option<Value>,
    discovery: Option<&source_discovery_projection::DiscoveryConfig>,
) -> Result<Option<Value>> {
    base.map(|mut seed| {
        if seed.get("scope_id").and_then(Value::as_str).is_none() {
            let universe_revision = &discovery
                .context("unscoped WebSocket seed requires Discovery identity")?
                .universe_revision;
            seed.as_object_mut()
                .context("WebSocket seed must be an object")?
                .insert("scope_id".into(), json!(universe_revision));
        }
        Ok(seed)
    })
    .transpose()
}

/// Keep the immutable cold catalog in the journal and metadata projection,
/// but never turn its old books or recovery rows into current hot evidence.
fn restrict_bootstrap_to_hot(mut base: Value, hot_tokens: &BTreeSet<String>) -> Result<Value> {
    for field in ["books", "gaps", "token_recoveries"] {
        let entries = base[field].as_array_mut().context("managed bootstrap entries")?;
        entries.retain(|entry| entry["token_id"].as_str()
            .is_some_and(|token| hot_tokens.contains(token)));
    }
    Ok(base)
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
    /// In-memory publication batches retained for bounded WS replay; separate
    /// from the asynchronous persistence work queue.
    #[arg(long, requires = "memory_replay_bytes")]
    memory_replay_batches: Option<usize>,
    #[arg(long, requires = "memory_replay_batches")]
    memory_replay_bytes: Option<usize>,
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
    /// Optional full-scope REST audit period. Zero disables polling while the
    /// WebSocket stream is healthy. A non-zero audit is deliberately limited
    /// to a five-minute-or-slower cadence; recovery remains event-driven.
    #[arg(long, default_value_t = 0)]
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
    #[arg(long, requires = "scope_control_socket")]
    scope_state_file: Option<PathBuf>,
    #[arg(long, requires = "scope_control_socket")]
    scope_state_bytes: Option<usize>,
    /// Offline, quiesced copy-only hot/cold migration; never starts a service.
    #[arg(long, requires_all=["scope_state_file","scope_state_bytes","offline_cold_state_sha256",
        "offline_cold_index_sha256","offline_cold_copy_budget_bytes"])]
    offline_cold_output: Option<PathBuf>,
    #[arg(long)]
    offline_cold_state_sha256: Option<String>,
    #[arg(long)]
    offline_cold_index_sha256: Option<String>,
    #[arg(long)]
    offline_cold_events_sha256: Option<String>,
    #[arg(long)]
    offline_cold_copy_budget_bytes: Option<u64>,
    #[arg(long, requires = "scope_control_socket")]
    scope_control_bytes: Option<usize>,
    #[arg(long, requires = "scope_control_socket")]
    scope_control_timeout_seconds: Option<u64>,
    #[arg(long, requires = "scope_control_socket")]
    scope_retire_grace_seconds: Option<u64>,
    /// Concurrent acquisition tokens, including preparation and retirement.
    #[arg(long, requires = "scope_control_socket")]
    acquisition_token_budget: Option<usize>,
    #[arg(long, requires = "scope_control_socket")]
    acquisition_socket_budget: Option<usize>,
    /// Keep APIs/control alive but acquire upstream sockets only while a
    /// bounded consumer lease exists. Leases are intentionally process-local.
    #[arg(long,requires_all=["scope_control_socket","acquisition_lease_capacity","acquisition_lease_max_seconds"])]
    acquisition_lease_required: bool,
    #[arg(long, requires = "acquisition_lease_required")]
    acquisition_lease_capacity: Option<usize>,
    #[arg(long, requires = "acquisition_lease_required")]
    acquisition_lease_max_seconds: Option<u64>,
    /// Independent direct Rust Discovery surface over this frozen universe.
    #[arg(long, conflicts_with_all=["configured_scope", "public_listen", "live_listen"], requires_all=["discovery_seed", "discovery_seed_sha256", "discovery_state_bytes", "discovery_full_sync_bytes", "discovery_frame_bytes", "discovery_replay_bytes", "discovery_clients", "discovery_baselines", "discovery_send_timeout_seconds"])]
    discovery_listen: Option<std::net::SocketAddr>,
    #[arg(long, requires = "discovery_listen")]
    discovery_seed: Option<PathBuf>,
    #[arg(long, requires = "discovery_listen")]
    discovery_seed_sha256: Option<String>,
    #[arg(long, requires = "discovery_listen")]
    discovery_state_bytes: Option<usize>,
    #[arg(long, requires = "discovery_listen")]
    discovery_full_sync_bytes: Option<usize>,
    #[arg(long, requires = "discovery_listen")]
    discovery_frame_bytes: Option<usize>,
    #[arg(long, requires = "discovery_listen")]
    discovery_replay_bytes: Option<usize>,
    #[arg(long, requires = "discovery_listen")]
    discovery_clients: Option<usize>,
    #[arg(long, requires = "discovery_listen")]
    discovery_baselines: Option<usize>,
    #[arg(long, requires = "discovery_listen")]
    discovery_send_timeout_seconds: Option<u64>,
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
    /// Native BTCUSDT bootstrap/realtime component on the same public listener.
    #[arg(long, requires_all=["public_listen","btc_research_archive_bytes","btc_research_queue_records","btc_research_queue_bytes","btc_research_response_bytes","btc_research_clients","btc_research_send_timeout_seconds"])]
    btc_research_root: Option<PathBuf>,
    #[arg(long, requires = "btc_research_root")]
    btc_research_archive_bytes: Option<usize>,
    #[arg(long, requires = "btc_research_root")]
    btc_research_queue_records: Option<usize>,
    #[arg(long, requires = "btc_research_root")]
    btc_research_queue_bytes: Option<usize>,
    #[arg(long, requires = "btc_research_root")]
    btc_research_response_bytes: Option<usize>,
    #[arg(long, requires = "btc_research_root")]
    btc_research_clients: Option<usize>,
    #[arg(long, requires = "btc_research_root")]
    btc_research_send_timeout_seconds: Option<u64>,
    /// Optional private CLOB API credential file for the Chainlink TWAP
    /// PolyBolt channel. Without it, Chainlink is explicitly unavailable.
    #[arg(long, requires = "btc_research_root")]
    btc_chainlink_credentials: Option<PathBuf>,
    /// Bounded BTC five-minute market projection over the same publication
    /// and listener. Acquisition membership is prepared through the existing
    /// managed control plane; this option never starts a sidecar source.
    #[arg(long, requires_all=["public_listen","btc_5m_profile_sha256","btc_5m_snapshot_bytes","btc_5m_frame_bytes","btc_5m_replay_bytes","btc_5m_clients","btc_5m_send_timeout_seconds","btc_5m_snapshot_lease_capacity"])]
    btc_5m_profile: Option<PathBuf>,
    #[arg(long, requires = "btc_5m_profile")]
    btc_5m_profile_sha256: Option<String>,
    /// Private PolyNode orderbook key. Enables the PN1 market-local v4 hot
    /// feed; never included in logs, profiles, snapshots or deployment receipts.
    #[arg(long, requires = "public_listen")]
    btc_polynode_key_file: Option<PathBuf>,
    #[arg(long)]
    btc_5m_snapshot_bytes: Option<usize>,
    #[arg(long)]
    btc_5m_frame_bytes: Option<usize>,
    #[arg(long)]
    btc_5m_replay_bytes: Option<usize>,
    /// Live consumers replace stale prefixes with an atomic current-state checkpoint.
    #[arg(long, default_value_t = 64)]
    btc_5m_live_backlog_groups: usize,
    #[arg(long, default_value_t = 500)]
    btc_5m_live_backlog_age_ms: u64,
    #[arg(long)]
    btc_5m_clients: Option<usize>,
    #[arg(long)]
    btc_5m_send_timeout_seconds: Option<u64>,
    #[arg(long)]
    btc_5m_snapshot_lease_capacity: Option<usize>,
    /// Same-process UTC rotation manager. It reuses the private scope control,
    /// shared acquisition lease and publication path; no sidecar/listener is
    /// created. The fixed timing values are intentionally explicit.
    #[arg(long, requires_all=["public_listen","scope_control_socket","btc_5m_snapshot_bytes","btc_5m_frame_bytes","btc_5m_replay_bytes","btc_5m_clients","btc_5m_send_timeout_seconds","btc_5m_snapshot_lease_capacity","btc_5m_manager_lease_ttl_seconds","btc_5m_manager_renew_seconds","btc_5m_manager_prereview_seconds","btc_5m_manager_control_timeout_seconds","btc_5m_manager_response_bytes","btc_5m_manager_evidence_files"])]
    btc_5m_manager_root: Option<PathBuf>,
    #[arg(long, requires = "btc_5m_manager_root")]
    btc_5m_manager_lease_ttl_seconds: Option<u64>,
    #[arg(long, requires = "btc_5m_manager_root")]
    btc_5m_manager_renew_seconds: Option<u64>,
    #[arg(long, requires = "btc_5m_manager_root")]
    btc_5m_manager_prereview_seconds: Option<u64>,
    #[arg(long, requires = "btc_5m_manager_root")]
    btc_5m_manager_control_timeout_seconds: Option<u64>,
    #[arg(long, requires = "btc_5m_manager_root")]
    btc_5m_manager_response_bytes: Option<usize>,
    #[arg(long, requires = "btc_5m_manager_root")]
    btc_5m_manager_evidence_files: Option<usize>,
    /// Durable two-provider CTF finality quorum on the unified public listener.
    #[arg(long, requires_all=["public_listen","finality_profile_a","finality_profile_b","finality_caller_token_file","finality_archive_bytes","finality_queue_records","finality_queue_bytes","finality_clients","finality_request_timeout_seconds","finality_retry_after_seconds"])]
    finality_root: Option<PathBuf>,
    #[arg(long, requires = "finality_root")]
    finality_profile_a: Option<PathBuf>,
    #[arg(long, requires = "finality_root")]
    finality_profile_b: Option<PathBuf>,
    #[arg(long, requires = "finality_root")]
    finality_caller_token_file: Option<PathBuf>,
    #[arg(long, requires = "finality_root")]
    finality_archive_bytes: Option<usize>,
    #[arg(long, requires = "finality_root")]
    finality_queue_records: Option<usize>,
    #[arg(long, requires = "finality_root")]
    finality_queue_bytes: Option<usize>,
    #[arg(long, requires = "finality_root")]
    finality_clients: Option<usize>,
    #[arg(long, requires = "finality_root")]
    finality_request_timeout_seconds: Option<u64>,
    #[arg(long, requires = "finality_root")]
    finality_retry_after_seconds: Option<u64>,
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
    marketcow_polymarket::async_eprintln!(
        "{}",
        json!({"stage":"network_body","elapsed_us":network_started.elapsed().as_micros(),"response_bytes":body.len()})
    );
    let received = received
        .with_nanosecond(received.nanosecond() / 1000 * 1000)
        .context("invalid timestamp")?;
    let parse_started = std::time::Instant::now();
    let books: Vec<Value> =
        tokio::task::spawn_blocking(move || serde_json::from_slice(&body)).await??;
    marketcow_polymarket::async_eprintln!(
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
    marketcow_polymarket::async_eprintln!("{}",json!({"stage":"fetch_and_prepare","request_and_decode_us":request_us,"normalize_us":normalize_started.elapsed().as_micros()}));
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

/// Finish a journaled retirement before any listener or supplier socket is
/// opened. A crash after the writer commits but before the journal settles
/// replays the same immutable cold record; the writer verifies its existing
/// hash and never replaces it with an empty post-retirement hot snapshot.
fn replay_pending_retirement(
    writer: &mut PreparedSourceWriter,
    state: &mut source_scope_journal::State,
) -> Result<()> {
    if state.retiring_records.is_empty() {
        return Ok(());
    }
    writer.retire_catalog_markets(
        writer.cursor()?,
        &state.retiring_records.keys().cloned().collect(),
        &state.retiring_records,
    )?;
    state.retiring_records.clear();
    state.retirement_ticket = None;
    state.retiring_acquisition_market_ids = None;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    marketcow_polymarket::diagnostic::install();
    let args = Args::parse();
    if let Some(output) = &args.offline_cold_output {
        let report = source_scope_migration::migrate(source_scope_migration::Inputs {
            source_root: &args.root,
            state_path: args.scope_state_file.as_deref().context("offline scope state")?,
            output_root: output,
            state_max_bytes: args.scope_state_bytes.context("offline state byte budget")?,
            batch_bytes: args.batch_byte_limit,
            expected_state_sha256: args.offline_cold_state_sha256.as_deref().context("offline state SHA")?,
            expected_index_sha256: args.offline_cold_index_sha256.as_deref().context("offline index SHA")?,
            expected_events_sha256: args.offline_cold_events_sha256.as_deref(),
            copy_budget_bytes: args.offline_cold_copy_budget_bytes.context("offline copy byte budget")?,
        })?;
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }
    ensure!(
        (1..=32).contains(&args.concurrency)
            && (1..=8).contains(&args.market_workers)
            && (1..=20).contains(&args.request_market_batch_size)
            && (2..=500).contains(&args.websocket_shard_tokens)
            && (1..=32).contains(&args.websocket_recovery_concurrency)
            && (args.websocket_confirmation_seconds == 0
                || (300..=3600).contains(&args.websocket_confirmation_seconds))
            && args.poll_seconds > 0
            && args.request_timeout_seconds > 0,
        "invalid concurrency/timeout/poll/audit configuration"
    );
    ensure!(
        args.expected_market_count > 0 && args.response_byte_limit > 0 && args.batch_byte_limit > 0,
        "explicit universe count and byte budgets required"
    );
    ensure!(args.cycles != Some(0), "cycles must be positive");
    ensure!(
        !args.acquisition_lease_required
            || (args.input_mode == InputMode::Websocket
                && args
                    .acquisition_lease_capacity
                    .is_some_and(|n| (1..=64).contains(&n))
                && args
                    .acquisition_lease_max_seconds
                    .is_some_and(|n| (1..=3600).contains(&n))),
        "on-demand acquisition requires bounded WebSocket leases"
    );
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
    let btc_5m_manager_config = if let Some(root) = &args.btc_5m_manager_root {
        fs::create_dir_all(root)?;
        let canonical_root = fs::canonicalize(root)?;
        ensure!(
            canonical_root.starts_with(fs::canonicalize(&args.root)?),
            "BTC 5m manager root escapes source root"
        );
        Some(source_btc5m_manager::Config {
            root: canonical_root,
            control_socket: args
                .scope_control_socket
                .clone()
                .context("BTC 5m manager control socket")?,
            ttl: Duration::from_secs(
                args.btc_5m_manager_lease_ttl_seconds
                    .context("BTC 5m manager lease TTL")?,
            ),
            renew: Duration::from_secs(
                args.btc_5m_manager_renew_seconds
                    .context("BTC 5m manager renewal")?,
            ),
            prereview: Duration::from_secs(
                args.btc_5m_manager_prereview_seconds
                    .context("BTC 5m manager prereview")?,
            ),
            control_timeout: Duration::from_secs(
                args.btc_5m_manager_control_timeout_seconds
                    .context("BTC 5m manager control timeout")?,
            ),
            response_bytes: args
                .btc_5m_manager_response_bytes
                .context("BTC 5m manager response bytes")?,
            evidence_files: args
                .btc_5m_manager_evidence_files
                .context("BTC 5m manager evidence files")?,
        })
    } else {
        None
    };
    let mut btc_5m_manager_bootstrap = None;
    let btc_5m_profile = if let Some(path) = &args.btc_5m_profile {
        ensure!(
            fs::canonicalize(path)?.starts_with(fs::canonicalize(&args.root)?),
            "BTC 5m profile escapes source root"
        );
        ensure!(
            fs::metadata(path)?.len() <= 1024 * 1024,
            "oversized BTC 5m profile"
        );
        let bytes = fs::read(path)?;
        ensure!(
            Some(&hex::encode(Sha256::digest(&bytes))) == args.btc_5m_profile_sha256.as_ref(),
            "BTC 5m profile file hash differs"
        );
        let profile: source_btc5m_api::Profile = serde_json::from_slice(&bytes)?;
        profile.validate()?;
        Some(profile)
    } else if let Some(config) = &btc_5m_manager_config {
        match source_btc5m_manager::load_persisted_profile(config, &plan.catalog_revision)? {
            Some(profile) => Some(profile),
            None => {
                // First activation performs one bounded current+next audit.
                // Every later restart loads the last validated profile before
                // networking, so a BTC supplier outage cannot prevent the
                // rest of the unified MarketCow service from starting.
                let bootstrap =
                    source_btc5m_manager::bootstrap(config, &plan.catalog_revision).await?;
                let profile = bootstrap.profile.clone();
                btc_5m_manager_bootstrap = Some(bootstrap);
                Some(profile)
            }
        }
    } else {
        None
    };
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
    let mut scope_boot = if let Some(path) = &args.scope_state_file {
        source_scope_journal::read(path, args.scope_state_bytes.context("scope state bytes")?)?
    } else {
        None
    };
    let manifest_sha = hex::encode(Sha256::digest(fs::read(args.root.join("catalog.json"))?));
    if let Some(state) = scope_boot.as_mut() {
        ensure!(
            state.catalog_revision == plan.catalog_revision
                && state.catalog_manifest_sha256 == manifest_sha
                && state.pool
                    == if args.discovery_listen.is_some() {
                        "discovery"
                    } else {
                        "live"
                    },
            "runtime source differs"
        );
        let restart_markets = state.restart_markets(args.acquisition_lease_required)?;
        let mut bindings: BTreeMap<String, String> = restart_markets
            .iter()
            .flat_map(|market| market.token_ids.iter().map(move |token| (token.clone(), market.market_id.clone())))
            .collect();
        for (id, record) in &state.retiring_records {
            // A pending durable retirement needs its writer identity, but is
            // never placed back into the active subscription plan.
            for outcome in record["identity"]["outcomes"].as_array().context("retiring outcomes")? {
                let token = outcome["token_id"].as_str().context("retiring token")?;
                ensure!(bindings.insert(token.to_owned(), id.clone()).is_none(), "retiring token overlaps active hot binding");
            }
        }
        writer.bind_catalog_tokens(bindings)?;
        replay_pending_retirement(&mut writer, state)?;
        plan.markets = restart_markets;
        ensure!(
            plan.markets.len() * 2
                <= args
                    .acquisition_token_budget
                    .context("runtime acquisition budget")?,
            "runtime acquisition capacity"
        );
        lifecycle_markets = state.records.clone();
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
    // The market-local feed has no JSONL/coupled-replay compatibility mode.
    // PN1 is an optional source: the official-WS market-local read surface
    // remains available when the private PN1 subscription is disabled.
    // Non-BTC discovery retains its independent archive policy.
    if btc_5m_profile.is_some() {
        ensure!(args.bounded_history_bytes.is_some(),
            "BTC 5m market-local archive requires bounded history");
        ensure!(args.memory_replay_batches.is_some() && args.memory_replay_bytes.is_some(),
            "BTC 5m market-local replay requires independent bounds");
    }
    // Do not migrate durable history until the complete source binding is valid.
    if let Some(limit) = args.bounded_history_bytes {
        writer.enable_bounded_history(limit)?;
    }
    let cursor = writer.cursor()?;
    let mut discovery_config = None;
    let scope_revision = scope_boot.as_ref().map_or(1, |state| state.active_revision);
    let base = if let Some(state) = scope_boot.as_mut() {
        if state.pool == "live" {
            let scope: source_public_api::ConfiguredScope =
                serde_json::from_value(state.active_config.clone())?;
            scope.validate()?;
            public_scope = Some(scope);
        } else {
            let mut config: source_discovery_projection::DiscoveryConfig =
                serde_json::from_value(state.active_config.clone())?;
            config.projection_id = canonical_hash(
                &json!({"previous":config.projection_id,"instance":uuid::Uuid::new_v4().simple().to_string()}),
            );
            state.active_config = serde_json::to_value(&config)?;
            discovery_config = Some(config);
        }
        let hot_tokens: BTreeSet<String> = plan.markets.iter()
            .flat_map(|market| market.token_ids.iter().cloned()).collect();
        Some(restrict_bootstrap_to_hot(durable_bootstrap::bootstrap_managed(
            args.root.clone(),
                state.catalog_revision.clone(),
                manifest_sha.clone(),
                state.records.values().cloned().collect(),
            state.catalog_source.clone(),
            if state.pool == "live" {
                args.public_full_sync_bytes.context("public state bytes")?
            } else {
                args.discovery_state_bytes
                    .context("Discovery state bytes")?
            },
            if state.pool == "live" {
                Some(
                    public_scope
                        .as_ref()
                        .context("managed Live scope")?
                        .active_scope_id
                        .clone(),
                )
            } else {
                None
            },
        )?, &hot_tokens)?)
    } else if args.discovery_listen.is_some() {
        let (config, base) = source_discovery_startup::load(
            &args.root,
            args.discovery_seed.as_deref().context("Discovery seed")?,
            args.discovery_seed_sha256
                .as_deref()
                .context("Discovery seed hash")?,
            args.discovery_state_bytes.context("Discovery state cap")?,
            &plan.markets,
        )?;
        discovery_config = Some(config);
        Some(base)
    } else if args.live_listen.is_some() || args.public_listen.is_some() {
        Some(durable_bootstrap::bootstrap(
            args.root.clone(),
            args.dependency_plan
                .clone()
                .context("missing stream plan")?,
            args.dependency_plan_sha256
                .as_deref()
                .context("missing stream plan hash")?,
            if args.public_listen.is_some() {
                args.public_full_sync_bytes.context("public seed cap")?
            } else {
                args.live_frame_bytes.context("missing frame cap")?
            },
        )?)
    } else {
        None
    };
    let journal = if let Some(path) = &args.scope_state_file {
        let initial = if let Some(state) = scope_boot {
            state
        } else {
            let base = base.as_ref().context("managed source baseline required")?;
            source_scope_journal::State {
                schema_version: "marketcow.runtime-scope-state.v1".into(),
                catalog_revision: plan.catalog_revision.clone(),
                catalog_manifest_sha256: manifest_sha,
                pool: if args.discovery_listen.is_some() {
                    "discovery"
                } else {
                    "live"
                }
                .into(),
                active_config: if let Some(config) = &discovery_config {
                    serde_json::to_value(config)?
                } else {
                    serde_json::to_value(public_scope.as_ref().context("public config")?)?
                },
                active_revision: scope_revision,
                catalog_source: base["catalog_source"].clone(),
                records: base["markets"]
                    .as_array()
                    .context("managed metadata")?
                    .iter()
                    .map(|record| {
                        Ok((
                            record["identity"]["market_id"]
                                .as_str()
                                .context("managed market")?
                                .into(),
                            record.clone(),
                        ))
                    })
                    .collect::<Result<_>>()?,
                acquisition_market_ids: plan.markets.iter().map(|m| m.market_id.clone()).collect(),
                retiring_records: Default::default(),
                retirement_ticket: None,
                retiring_acquisition_market_ids: Default::default(),
            }
        };
        let mut journal = source_scope_journal::Journal::open(
            path.clone(),
            args.scope_state_bytes.context("scope state bytes")?,
            initial.clone(),
        )?;
        // Persist completed startup retirement and the new Discovery instance.
        journal.commit(initial)?;
        Some(journal)
    } else {
        None
    };
    let bindings = plan
        .markets
        .iter()
        .flat_map(|m| m.token_ids.iter().map(|t| (t.clone(), m.market_id.clone())))
        .collect();
    let persistence_root = args.root.clone();
    let persistence_catalog = plan.catalog_revision.clone();
    // The normalizer requires a stable identity domain. Live already carries
    // its scope ID in the baseline; Discovery is intentionally unscoped, so
    // bind its in-memory WS events to the immutable universe revision instead.
    // This field is added only to the adapter seed and is not exposed as a
    // fabricated Live scope in the Discovery read contract.
    let ws_seed = websocket_seed(base.clone(), discovery_config.as_ref())?;
    let (replay_batches, replay_bytes, coupled_for_non_btc) = match
        (args.memory_replay_batches, args.memory_replay_bytes)
    {
        (Some(batches), Some(bytes)) => (batches, bytes, false),
        (None, None) => (args.persistence_queue_batches, args.persistence_queue_bytes, true),
        _ => anyhow::bail!("memory replay batch and byte bounds must be configured together"),
    };
    marketcow_polymarket::async_eprintln!("{}", json!({"stage":"publication_limits",
        "persistence_queue_batches":args.persistence_queue_batches,
        "persistence_queue_bytes":args.persistence_queue_bytes,
        "memory_replay_batches":replay_batches,
        "memory_replay_bytes":replay_bytes,
        "coupled_replay_limits_non_btc_only":coupled_for_non_btc}));
    let mut publication = source_publication::Publication::start_managed_with_replay(
        cursor,
        base,
        bindings,
        args.persistence_queue_batches,
        args.persistence_queue_bytes,
        replay_batches,
        replay_bytes,
        args.batch_byte_limit,
        move |work| {
            let batches = match work {
                source_publication::PersistenceWork::Events(batches) => batches,
                source_publication::PersistenceWork::Retire(retirement) => {
                    writer.bind_catalog_tokens(retirement.catalog_bindings.clone())?;
                    let after = writer.cursor()?;
                    return writer.retire_catalog_markets(
                        after, &retirement.markets, &retirement.cold_records);
                }
            };
            for batch in batches {
                writer.bind_catalog_tokens(batch.catalog_bindings.clone())?;
                if let Some(evidence) = &batch.evidence {
                    source_lifecycle::persist(&persistence_root, &persistence_catalog, evidence)?;
                }
            }
            let materialized = batches
                .iter()
                .map(|batch| {
                    Ok((
                        &batch.validated,
                        batch
                            .materialized_books
                            .lock()
                            .map_err(|_| anyhow::anyhow!("materialized books poisoned"))?
                            .take()
                            .context("materialized books already persisted")?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            writer.append_validated_group_with_books(
                &materialized
                    .iter()
                    .map(|(validated, books)| (*validated, books.as_ref()))
                    .collect::<Vec<_>>(),
            )
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
            async move { axum::serve(listener, router.into_make_service_with_connect_info::<std::net::SocketAddr>()).await },
        ))
    } else {
        None
    };
    ensure!(
        args.scope_control_socket.is_none()
            || args.public_listen.is_some()
            || args.discovery_listen.is_some(),
        "scope control requires a public pool"
    );
    let btc_service = if let Some(root) = args.btc_research_root.clone() {
        Some(
            btc_research::Service::start(btc_research::Config {
                root,
                archive_bytes: args
                    .btc_research_archive_bytes
                    .context("BTC archive bytes")?,
                queue_records: args
                    .btc_research_queue_records
                    .context("BTC queue records")?,
                queue_bytes: args.btc_research_queue_bytes.context("BTC queue bytes")?,
                response_bytes: args
                    .btc_research_response_bytes
                    .context("BTC response bytes")?,
                clients: args.btc_research_clients.context("BTC clients")?,
                send_timeout: Duration::from_secs(
                    args.btc_research_send_timeout_seconds
                        .context("BTC send timeout")?,
                ),
                chainlink_credentials: args.btc_chainlink_credentials.clone(),
            })
            .await?,
        )
    } else {
        None
    };
    let finality_service = if let Some(root) = args.finality_root.clone() {
        Some(
            source_finality_service::Service::start(source_finality_service::Config {
                root,
                profile_paths: [
                    args.finality_profile_a
                        .clone()
                        .context("finality profile A")?,
                    args.finality_profile_b
                        .clone()
                        .context("finality profile B")?,
                ],
                caller_token_path: args
                    .finality_caller_token_file
                    .clone()
                    .context("finality caller token file")?,
                archive_bytes: args
                    .finality_archive_bytes
                    .context("finality archive bytes")?,
                queue_records: args
                    .finality_queue_records
                    .context("finality queue records")?,
                queue_bytes: args.finality_queue_bytes.context("finality queue bytes")?,
                clients: args.finality_clients.context("finality clients")?,
                request_timeout: Duration::from_secs(
                    args.finality_request_timeout_seconds
                        .context("finality request timeout")?,
                ),
                retry_after: Duration::from_secs(
                    args.finality_retry_after_seconds
                        .context("finality retry interval")?,
                ),
            })
            .await?,
        )
    } else {
        None
    };
    let mut control_backend = None;
    let mut btc_5m_control = None;
    let mut polynode_task = None;
    let public_task = if let Some(listen) = args.public_listen {
        ensure!(
            listen.ip().is_loopback()
                || matches!(listen.ip(), std::net::IpAddr::V4(ip) if ip.is_private()),
            "public read API must bind explicit loopback or private LAN address"
        );
        let (mut router, control) = source_public_api::PublicApi::router_managed(
            publication.reader(),
            public_scope.context("public scope")?,
            scope_revision,
            uuid::Uuid::new_v4().simple().to_string(),
            args.public_full_sync_bytes.context("public bytes")?,
            args.public_snapshot_concurrency
                .context("snapshot concurrency")?,
            source_public_api::StreamLimits {
                frame_bytes: args.public_frame_bytes.context("public frame bytes")?,
                replay_bytes: args.public_replay_bytes.context("public replay bytes")?,
                clients: args.public_maximum_clients.context("public clients")?,
                send_timeout: Duration::from_secs(
                    args.public_send_timeout_seconds.context("send timeout")?,
                ),
            },
            Duration::from_secs(
                args.scope_retire_grace_seconds
                    .unwrap_or(args.public_send_timeout_seconds.context("send timeout")?),
            ),
        )?;
        if let Some(service) = &btc_service {
            router = router.merge(service.router());
        }
        if let Some(service) = &finality_service {
            router = router.merge(service.router());
        }
        router = router.merge(source_cold_catalog::router(args.root.clone())?);
        if let Some(profile) = btc_5m_profile {
            let (btc_router, control) = source_btc5m_api::Api::router_with_archive(
                publication.reader(),
                profile,
                uuid::Uuid::new_v4().simple().to_string(),
                source_btc5m_api::Limits {
                    live_backlog_groups: args.btc_5m_live_backlog_groups,
                    live_backlog_age: std::time::Duration::from_millis(args.btc_5m_live_backlog_age_ms),
                    snapshot_bytes: args
                        .btc_5m_snapshot_bytes
                        .context("BTC 5m snapshot bytes")?,
                    frame_bytes: args.btc_5m_frame_bytes.context("BTC 5m frame bytes")?,
                    replay_bytes: args.btc_5m_replay_bytes.context("BTC 5m replay bytes")?,
                    clients: args.btc_5m_clients.context("BTC 5m clients")?,
                    send_timeout: Duration::from_secs(
                        args.btc_5m_send_timeout_seconds
                            .context("BTC 5m send timeout")?,
                    ),
                    snapshot_lease_capacity: args
                        .btc_5m_snapshot_lease_capacity
                        .context("BTC 5m snapshot lease capacity")?,
                },
                args.root.clone(),
            )?;
            router = router.merge(source_btc5m_reference::router(
                control.clone(), btc_service.as_ref().map(|service| service.trade_source()),
            )?);
            router = router.merge(source_official_ws_relay::router(
                control.clone(), publication.reader(),
            )?);
            router=router.merge(source_market_evidence::native_router(control.clone())?);
            if let Some(service)=&finality_service {
                router=router.merge(service.native_router(control.clone()));
            }
            if let Some(key_path) = args.btc_polynode_key_file.clone() {
                let (pn1, archive_rx) = source_polynode_service::Service::new(control.clone())?;
                polynode_task = Some(pn1.start(key_path,
                    args.root.join("polynode-pn1"), archive_rx)?);
                router = router.merge(pn1.router());
            }
            btc_5m_control = Some(control);
            router = router.merge(btc_router);
        }
        control_backend = Some(source_scope_control::Backend::Live(control));
        let listener = tokio::net::TcpListener::bind(listen).await?;
        Some(tokio::spawn(
            async move { axum::serve(listener, router.into_make_service_with_connect_info::<std::net::SocketAddr>()).await },
        ))
    } else {
        None
    };
    let discovery_task = if let Some(listen) = args.discovery_listen {
        ensure!(
            listen.ip().is_loopback()
                || matches!(listen.ip(),std::net::IpAddr::V4(ip) if ip.is_private()),
            "Discovery requires explicit loopback/private address"
        );
        let (router, control) = source_discovery_api::router_managed_at(
            publication.reader(),
            std::sync::Arc::new(discovery_config.context("Discovery config")?),
            source_discovery_api::DiscoveryLimits {
                full_sync_bytes: args
                    .discovery_full_sync_bytes
                    .context("Discovery response cap")?,
                frame_bytes: args.discovery_frame_bytes.context("Discovery frame cap")?,
                state_bytes: args.discovery_state_bytes.context("Discovery state cap")?,
                replay_bytes: args
                    .discovery_replay_bytes
                    .context("Discovery replay cap")?,
                clients: args.discovery_clients.context("Discovery clients")?,
                cached_baselines: args.discovery_baselines.context("Discovery baselines")?,
                send_timeout: Duration::from_secs(
                    args.discovery_send_timeout_seconds
                        .context("Discovery send timeout")?,
                ),
            },
            Duration::from_secs(
                args.scope_retire_grace_seconds.unwrap_or(
                    args.discovery_send_timeout_seconds
                        .context("Discovery send timeout")?,
                ),
            ),
            scope_revision,
        )?;
        control_backend = Some(source_scope_control::Backend::Discovery(control));
        let listener = tokio::net::TcpListener::bind(listen).await?;
        Some(tokio::spawn(
            async move { axum::serve(listener, router).await },
        ))
    } else {
        None
    };
    let (acquisition_sender, acquisition_receiver) = tokio::sync::mpsc::channel(1);
    let btc_5m_manager_control = btc_5m_control.clone();
    let control_task = if let Some(path) = &args.scope_control_socket {
        Some(source_scope_control::start(
            path,
            args.scope_control_bytes.context("control bytes")?,
            Duration::from_secs(
                args.scope_control_timeout_seconds
                    .context("control timeout")?,
            ),
            control_backend.context("control backend")?,
            Some(acquisition_sender),
            btc_5m_control.clone(),
            journal,
        ).await?)
    } else {
        None
    };
    let btc_5m_manager_task = if let Some(config) = btc_5m_manager_config {
        Some(source_btc5m_manager::start(
            config,
            btc_5m_manager_control.context("BTC 5m manager projection")?,
            btc_5m_manager_bootstrap,
        )?)
    } else {
        None
    };
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
            if args.scope_control_socket.is_some() {
                Some(acquisition_receiver)
            } else {
                None
            },
        )
        .await;
        if let Some(task) = stream_task {
            task.abort();
        }
        if let Some(task) = public_task {
            task.abort();
        }
        if let Some(task) = discovery_task {
            task.abort();
        }
        if let Some(task) = btc_5m_manager_task {
            task.abort();
        }
        if let Some(task) = control_task {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = polynode_task.take() {
            task.abort();
            let _ = task.await;
        }
        let drain = publication.finish().await;
        result?;
        drain?;
        return Ok(());
    }
    if args.scope_control_socket.is_some() {
        let result =
            source_rest_pool::run(&args, &plan, &mut publication, acquisition_receiver).await;
        if let Some(task) = stream_task {
            task.abort();
        }
        if let Some(task) = public_task {
            task.abort();
        }
        if let Some(task) = discovery_task {
            task.abort();
        }
        if let Some(task) = btc_5m_manager_task {
            task.abort();
        }
        if let Some(task) = control_task {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = polynode_task.take() {
            task.abort();
            let _ = task.await;
        }
        let drain = publication.finish().await;
        result?;
        drain?;
        return Ok(());
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
                            Err(error)=>marketcow_polymarket::async_eprintln!("{}",json!({"lifecycle_refresh_error":format!("{error:#}"),"market_id":id})),
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
                            Err(error) => {rejected+=1; marketcow_polymarket::async_eprintln!("{}",json!({"market_id":id,"source_error":format!("{error:#}")}));}
                        }
                    }
                    marketcow_polymarket::async_eprintln!("{}",json!({"batch":batch,"batch_cycle":cycle,"request_wait_and_prepare_us":elapsed_us,
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
    if let Some(task) = public_task {
        task.abort();
    }
    if let Some(task) = discovery_task {
        task.abort();
    }
    if let Some(task) = control_task {
        task.abort();
        let _ = task.await;
    }
    if let Some(task) = stream_task {
        task.abort();
    }
    if let Some(task) = polynode_task.take() {
        task.abort();
        let _ = task.await;
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
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn process_kill_replays_retirement_intent_unknown_wire_and_writer_commit() {
        fn record(id: &str) -> Value {
            json!({"identity":{"market_id":id,"condition_id":format!("condition-{id}"),
                "outcomes":[{"token_id":format!("{id}1")},
                            {"token_id":format!("{id}2")} ]}})
        }
        fn initial() -> source_scope_journal::State {
            source_scope_journal::State {
                schema_version:"marketcow.runtime-scope-state.v1".into(),
                catalog_revision:"catalog".into(),
                catalog_manifest_sha256:"manifest".into(), pool:"live".into(),
                active_config:json!({"catalog_revision":"catalog",
                    "configured_markets":[{"market_id":"1"}]}),
                active_revision:1, catalog_source:Value::Null,
                records:BTreeMap::from([("1".into(),record("1")),
                    ("2".into(),record("2"))]),
                acquisition_market_ids:BTreeSet::from(["1".into(),"2".into()]),
                retiring_records:Default::default(),retirement_ticket:None,
                retiring_acquisition_market_ids:None,
            }
        }
        fn bindings() -> BTreeMap<String, String> {
            BTreeMap::from([("11".into(),"1".into()),("12".into(),"1".into()),
                ("21".into(),"2".into()),("22".into(),"2".into())])
        }
        if let Some(root) = std::env::var_os("MARKETCOW_CRASH_REPLAY_ROOT") {
            let root = PathBuf::from(root);
            let stage = std::env::var("MARKETCOW_CRASH_REPLAY_STAGE").unwrap();
            let mut journal = source_scope_journal::Journal::open(
                root.join("private/runtime-scope-state.json"),65536,initial()).unwrap();
            journal.retire(&BTreeSet::from(["2".into()]),1).unwrap();
            let _unanswered_wire = if stage == "wire_result_unknown" {
                let (mut pool, mut receiver) =
                    source_acquisition_shards::unanswered_retirement_fixture();
                assert!(!pool.request_retire(&BTreeSet::from(["21".into(),"22".into()])).unwrap());
                let command = receiver.try_recv().unwrap();
                assert_eq!(command.token_ids,BTreeSet::from(["11".into(),"12".into()]));
                // Deliberately retain the command and its receipt sender until
                // SIGKILL: the process never learns whether venue removal won.
                Some((pool, command))
            } else { None };
            if stage == "writer_committed" {
                let mut writer = PreparedSourceWriter::open(&root,16384).unwrap();
                writer.bind_catalog_tokens(bindings()).unwrap();
                replay_pending_retirement(&mut writer,&mut journal.state).unwrap();
                assert_eq!(writer.cold_market_record("2").unwrap(),Some(record("2")));
            } else {
                assert!(matches!(stage.as_str(),"intent_committed" | "wire_result_unknown"));
            }
            std::fs::write(root.join("kill-ready"),stage).unwrap();
            // The parent sends SIGKILL: no Rust destructors, journal settle or
            // graceful writer drain may run after the stage marker.
            loop { std::thread::sleep(std::time::Duration::from_secs(1)); }
        }
        for stage in ["intent_committed","wire_result_unknown","writer_committed"] {
            let root = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
            std::fs::create_dir(root.path().join("private")).unwrap();
            std::fs::set_permissions(root.path().join("private"),
                std::fs::Permissions::from_mode(0o700)).unwrap();
            std::fs::create_dir(root.path().join("indexes")).unwrap();
            std::fs::File::create(root.path().join("events.jsonl")).unwrap();
            let db = rusqlite::Connection::open(
                root.path().join("indexes/latest-state.sqlite3")).unwrap();
            db.execute_batch("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE gaps(resolved INTEGER NOT NULL);
                CREATE TABLE books(token_id TEXT PRIMARY KEY,market_id TEXT,cursor INTEGER,
                    payload_json BLOB,payload_sha256 TEXT);
                CREATE TABLE book_confirmations(token_id TEXT PRIMARY KEY);
                CREATE TABLE event_offsets(cursor INTEGER PRIMARY KEY,byte_offset INTEGER,
                    byte_length INTEGER,event_id TEXT,market_id TEXT,token_id TEXT,line_sha256 TEXT);").unwrap();
            for (key,value) in [("schema_version","marketcow.polymarket.state-index.v1"),
                ("unresolved_gap_count","0"),("event_log_size","0"),
                ("latest_cursor","0"),("catalog_revision","catalog"),
                ("active_recovery_id","recovery")] {
                db.execute("INSERT INTO metadata VALUES (?,?)",[key,value]).unwrap();
            }
            drop(db);
            let mut writer = PreparedSourceWriter::open(root.path(),16384).unwrap();
            writer.bind_catalog_tokens(bindings()).unwrap();
            drop(writer);
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("process_kill_replays_retirement_intent_unknown_wire_and_writer_commit")
                .env("MARKETCOW_CRASH_REPLAY_ROOT",root.path())
                .env("MARKETCOW_CRASH_REPLAY_STAGE",stage)
                .spawn().unwrap();
            let deadline = std::time::Instant::now()+std::time::Duration::from_secs(10);
            while !root.path().join("kill-ready").exists() {
                assert!(std::time::Instant::now()<deadline,
                    "child did not reach {stage}; status={:?}",child.try_wait().unwrap());
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            child.kill().unwrap();
            assert!(!child.wait().unwrap().success());
            let path = root.path().join("private/runtime-scope-state.json");
            let mut state = source_scope_journal::read(&path,65536).unwrap().unwrap();
            assert_eq!(state.retirement_ticket,Some(1));
            assert_eq!(state.retiring_records["2"],record("2"));
            assert_eq!(state.restart_markets(true).unwrap().len(),1,
                "a crashed retirement must not revive the old subscription");
            let mut writer = PreparedSourceWriter::open(root.path(),16384).unwrap();
            writer.bind_catalog_tokens(bindings()).unwrap();
            let before = writer.cold_market_evidence("2").unwrap();
            assert_eq!(before.is_some(),stage=="writer_committed");
            replay_pending_retirement(&mut writer,&mut state).unwrap();
            let after = writer.cold_market_evidence("2").unwrap();
            assert!(after.is_some());
            if let Some(before) = before { assert_eq!(after,Some(before)); }
            assert_eq!(writer.cold_market_record("2").unwrap(),Some(record("2")));
            let mut journal = source_scope_journal::Journal::open(path.clone(),65536,initial()).unwrap();
            journal.commit(state).unwrap();
            assert!(source_scope_journal::read(&path,65536).unwrap().unwrap()
                .retiring_records.is_empty());
        }
    }

    #[test]
    fn managed_restart_read_only_source_smoke_when_requested() {
        let Ok(root) = std::env::var("MARKETCOW_RESTART_SMOKE_ROOT") else { return };
        let Ok(state_path) = std::env::var("MARKETCOW_RESTART_SMOKE_STATE") else { return };
        let state = source_scope_journal::read(std::path::Path::new(&state_path), 134_217_728)
            .unwrap().unwrap();
        let hot = state.restart_markets(true).unwrap();
        let hot_tokens: BTreeSet<String> = hot.iter()
            .flat_map(|market| market.token_ids.iter().cloned()).collect();
        let base = durable_bootstrap::bootstrap_managed(
            root.into(), state.catalog_revision.clone(),
            state.catalog_manifest_sha256.clone(),
            state.records.values().cloned().collect(),
            state.catalog_source.clone(), 134_217_728,
            Some(state.active_config["active_scope_id"].as_str().unwrap().to_owned()),
        ).unwrap();
        let base = restrict_bootstrap_to_hot(base, &hot_tokens).unwrap();
        assert_eq!(base["markets"].as_array().unwrap().len(), state.records.len());
        assert!(base["books"].as_array().unwrap().iter().all(|book|
            hot_tokens.contains(book["token_id"].as_str().unwrap())));
        assert!(base["token_recoveries"].as_array().unwrap().iter().all(|recovery|
            hot_tokens.contains(recovery["token_id"].as_str().unwrap())));
    }

    #[test]
    fn managed_restart_keeps_cold_metadata_but_not_cold_book_freshness() {
        let hot = BTreeSet::from(["hot-up".to_owned(), "hot-down".to_owned()]);
        let base = json!({"markets":[{"identity":{"market_id":"hot"}},
            {"identity":{"market_id":"cold"}}],
            "books":[{"token_id":"hot-up"},{"token_id":"cold-up"}],
            "gaps":[{"token_id":"cold-up"}],
            "token_recoveries":[{"token_id":"cold-up"}]});
        let filtered = restrict_bootstrap_to_hot(base, &hot).unwrap();
        assert_eq!(filtered["markets"].as_array().unwrap().len(), 2);
        assert_eq!(filtered["books"], json!([{"token_id":"hot-up"}]));
        assert_eq!(filtered["gaps"], json!([]));
        assert_eq!(filtered["token_recoveries"], json!([]));
    }

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
            "--memory-replay-batches",
            "1",
            "--memory-replay-bytes",
            "1",
            "--poll-seconds",
            "1",
            "--request-timeout-seconds",
            "1",
        ])
        .unwrap();
        assert_eq!(defaults.websocket_shard_tokens, 50);
        assert_eq!(defaults.websocket_recovery_concurrency, 8);
        assert_eq!(defaults.websocket_confirmation_seconds, 0);
    }

    #[test]
    fn discovery_websocket_seed_uses_universe_revision_without_live_scope() {
        let config = source_discovery_projection::DiscoveryConfig {
            projection_id: "projection".into(),
            catalog_revision: "catalog".into(),
            universe_revision: "universe".into(),
            market_ids: vec!["1".into()],
            relations: vec![],
            settlements: Default::default(),
            policy: source_discovery_quote::QuotePolicy {
                quantities: vec!["1".into()],
                maximum_book_age_ms: 1_000,
            },
        };
        let seed = websocket_seed(Some(json!({"markets": [], "books": []})), Some(&config))
            .unwrap()
            .unwrap();
        assert_eq!(seed["scope_id"], "universe");

        let live = websocket_seed(
            Some(json!({"scope_id": "live-scope", "markets": [], "books": []})),
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(live["scope_id"], "live-scope");
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
