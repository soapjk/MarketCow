//! Direct public read surface primitives. No socket bridge or disk reads.
//!
//! Snapshot callers must acquire an immutable, causally consistent view first,
//! release the publication lock, and only then encode. A partial response must
//! never escape when its configured byte budget is exceeded.
use std::io::{self, Write};
use std::collections::BTreeSet;
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use marketcow_polymarket::discovery_source::canonical_hash;
use serde_json::{Value, json};
use chrono::{DateTime, Utc};
use crate::{source_publication::MemoryView, source_public_frame};
use std::sync::Arc;
use axum::{Router, extract::{State, Query}, response::{IntoResponse, Response}, routing::get, http::{StatusCode, header}};
use tokio::sync::Semaphore;
use axum::extract::ws::{WebSocketUpgrade, WebSocket, Message};
use std::time::Duration;

// Keep admission charged until the last response-byte owner is dropped, not
// merely until JSON encoding completes. A slow HTTP reader cannot accumulate
// arbitrarily many completed full-sync buffers behind the semaphore.
struct SnapshotBytes {
    bytes: Vec<u8>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl AsRef<[u8]> for SnapshotBytes {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}

#[derive(Clone)]
pub struct StreamLimits {
    pub frame_bytes: usize,
    pub replay_bytes: usize,
    pub clients: usize,
    pub send_timeout: Duration,
}

pub struct PublicApi {
    reader: crate::source_publication::MemoryReader,
    scope: ConfiguredScope,
    generation: u64,
    instance: String,
    full_sync_bytes: usize,
    snapshots: Arc<Semaphore>,
    streams: Arc<Semaphore>,
    limits: StreamLimits,
}

impl PublicApi {
    pub fn router(reader: crate::source_publication::MemoryReader, scope: ConfiguredScope,
        generation: u64, instance: String, full_sync_bytes: usize, concurrent_snapshots: usize, limits: StreamLimits) -> Result<Router> {
        scope.validate()?;
        ensure!(generation > 0 && !instance.is_empty(), "public identity required");
        ensure!(full_sync_bytes > 0 && full_sync_bytes <= 256*1024*1024
            && (1..=4).contains(&concurrent_snapshots), "explicit snapshot budgets");
        ensure!(limits.frame_bytes > 0 && limits.frame_bytes <= 64*1024*1024
            && limits.replay_bytes > 0 && limits.replay_bytes <= 64*1024*1024
            && (1..=16).contains(&limits.clients) && !limits.send_timeout.is_zero(), "explicit stream budgets");
        let state = Arc::new(Self {reader,scope,generation,instance,full_sync_bytes,
            snapshots:Arc::new(Semaphore::new(concurrent_snapshots)),
            streams:Arc::new(Semaphore::new(limits.clients)),limits});
        Ok(Router::new()
            .route("/v1/prediction-markets/polymarket/live/full-sync",get(http_full_sync))
            .route("/v1/prediction-markets/polymarket/live/scope",get(http_scope))
            .route("/v1/prediction-markets/polymarket/live/bootstrap",get(http_bootstrap))
            .route("/v1/prediction-markets/polymarket/live/snapshot",get(http_snapshot))
            .route("/v1/prediction-markets/polymarket/live/health",get(http_health))
            .route("/v1/prediction-markets/polymarket/live/stream",get(ws_upgrade))
            .with_state(state))
    }
}

fn failure(status: StatusCode, code: &str, retryable: bool) -> Response {
    (status, axum::Json(json!({"detail":{"code":code,"message":code,"retryable":retryable}}))).into_response()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeQuery { scope_id: String }
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamQuery { scope_id: String, after_cursor: u64 }

async fn ws_upgrade(State(api): State<Arc<PublicApi>>, Query(query): Query<StreamQuery>, ws: WebSocketUpgrade) -> Response {
    if query.scope_id != api.scope.active_scope_id {
        return failure(StatusCode::CONFLICT,"polymarket_scope_binding_mismatch",false);
    }
    let Ok(permit) = api.streams.clone().try_acquire_owned() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE,"polymarket_stream_capacity",true);
    };
    ws.max_message_size(4096).max_frame_size(4096).on_upgrade(move |mut socket|async move {
        let _permit = permit;
        if let Err(error) = public_stream(&mut socket,&api,query.after_cursor).await {
            eprintln!("public live stream closed: {error:#}");
            let _ = send_public(&mut socket,&api,json!({"type":"error","code":"polymarket_live_stream_disconnected",
                "message":"Source disconnected; re-establish snapshot and stream","retryable":true})).await;
        }
        let _ = tokio::time::timeout(api.limits.send_timeout,socket.send(Message::Close(None))).await;
    })
}

async fn send_public(socket: &mut WebSocket, api: &PublicApi, frame: Value) -> Result<()> {
    let bytes = encode_bounded(&frame,api.limits.frame_bytes)?;
    let body = String::from_utf8(bytes)?;
    tokio::time::timeout(api.limits.send_timeout,socket.send(Message::Text(body.into()))).await??;
    Ok(())
}

async fn public_stream(socket: &mut WebSocket, api: &PublicApi, mut cursor: u64) -> Result<()> {
    let mut changed = api.reader.subscribe();
    let view = api.reader.capture()?;
    ensure!(view.base["catalog_revision"] == api.scope.catalog_revision,"stream catalog mismatch");
    let mut markets: BTreeSet<String> = api.scope.configured_markets.iter().map(|m|m.market_id.clone()).collect();
    let mut tokens: BTreeSet<String> = api.scope.configured_markets.iter().flat_map(|m|m.token_ids.clone()).collect();
    for selected in &api.scope.configured_markets {
        let market = view.markets.get(&selected.market_id).context("scope metadata")?;
        for relation in market["relations"].as_array().context("relations")? {
            for pair in relation["outcome_pairs"].as_array().context("pairs")? {
                markets.insert(pair["market_id"].as_str().context("pair market")?.into());
                tokens.insert(pair["yes_token_id"].as_str().context("YES token")?.into());
                tokens.insert(pair["no_token_id"].as_str().context("NO token")?.into());
            }
        }
    }
    drop(view);
    let mut ready = false;
    let mut confirmation = 0;
    loop {
        changed.borrow_and_update();
        let page = api.reader.replay(cursor,confirmation,64,api.limits.replay_bytes)?;
        for batch in &page.batches {
            for event in batch.validated.events() {
                let at = event["cursor"].as_u64().context("event cursor")?;
                if at <= cursor { continue; }
                if event["market_id"].is_null() || event["market_id"].as_str().is_some_and(|m|markets.contains(m)) {
                    send_public(socket,api,json!({"type":"event","cursor":at,"event":event})).await?;
                }
                cursor = at;
            }
        }
        ensure!(cursor == page.next,"public replay boundary");
        if page.caught_up {
            let books: Vec<_> = page.confirmation_books.iter().filter(|b| b["token_id"].as_str().is_some_and(|t|tokens.contains(t))).collect();
            if !ready {
                send_public(socket,api,json!({"type":"ready","cursor":cursor,"stream_instance_id":api.instance,
                    "confirmation_sequence":page.confirmation_sequence,"confirmation_books":books})).await?;
                ready = true;
            } else if page.confirmation_sequence > confirmation && !books.is_empty() {
                send_public(socket,api,json!({"type":"book_confirmations","cursor":cursor,"stream_instance_id":api.instance,
                    "confirmation_sequence":page.confirmation_sequence,"books":books})).await?;
            }
            confirmation = page.confirmation_sequence;
        } else { continue; }
        tokio::select! {
            result = changed.changed() => {result.context("publication closed")?;},
            message = socket.recv() => {
                match message {
                    None | Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Ok(Message::Ping(bytes))) => {
                        tokio::time::timeout(api.limits.send_timeout,socket.send(Message::Pong(bytes))).await??;
                    },
                    Some(Ok(Message::Pong(_))) => {},
                    _ => anyhow::bail!("unexpected public client message"),
                }
            }
        }
    }
}

async fn http_scope(State(api): State<Arc<PublicApi>>) -> Response {
    axum::Json(api.scope.clone()).into_response()
}

async fn http_full_sync(State(api): State<Arc<PublicApi>>, Query(query): Query<ScopeQuery>) -> Response {
    scoped_response(api,query,None).await
}
async fn http_bootstrap(State(api): State<Arc<PublicApi>>, Query(query): Query<ScopeQuery>) -> Response {
    scoped_response(api,query,Some("bootstrap")).await
}
async fn http_snapshot(State(api): State<Arc<PublicApi>>, Query(query): Query<ScopeQuery>) -> Response {
    scoped_response(api,query,Some("snapshot")).await
}
async fn http_health(State(api): State<Arc<PublicApi>>) -> Response {
    let query = ScopeQuery{scope_id:api.scope.active_scope_id.clone()};
    scoped_response(api,query,Some("health")).await
}

async fn scoped_response(api: Arc<PublicApi>, query: ScopeQuery, component: Option<&'static str>) -> Response {
    if query.scope_id != api.scope.active_scope_id {
        return failure(StatusCode::CONFLICT,"polymarket_scope_binding_mismatch",false);
    }
    let Ok(permit) = api.snapshots.clone().try_acquire_owned() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE,"polymarket_snapshot_capacity",true);
    };
    // Bound CPU tasks before submission; permit lives until construction and
    // encoding finish even if the requesting connection is cancelled.
    let result = tokio::task::spawn_blocking(move || -> Result<SnapshotBytes> {
        let view = api.reader.capture()?;
        let mut model = full_sync(&view,&api.scope,api.generation,&api.instance,Utc::now())?;
        let model = match component {Some(key)=>model[key].take(),None=>model};
        Ok(SnapshotBytes{bytes:encode_bounded(&model,api.full_sync_bytes)?,_permit:permit})
    }).await;
    match result {
        Ok(Ok(bytes)) => ([(header::CONTENT_TYPE,"application/json")],axum::body::Bytes::from_owner(bytes)).into_response(),
        Ok(Err(error)) if error.downcast_ref::<EncodeError>().is_some_and(|e| matches!(e,EncodeError::TooLarge)) =>
            failure(StatusCode::PAYLOAD_TOO_LARGE,"polymarket_full_sync_too_large",false),
        _ => failure(StatusCode::SERVICE_UNAVAILABLE,"polymarket_live_snapshot_unavailable",true),
    }
}

/// Caller provides the generation/instance of this direct public projection,
/// never a previously running Python projection's identity.
pub fn full_sync(view: &MemoryView, scope: &ConfiguredScope, generation: u64,
    instance: &str, now: DateTime<Utc>) -> Result<Value> {
    scope.validate()?;
    ensure!(generation > 0 && !instance.is_empty(), "public projection identity");
    ensure!(view.base["catalog_revision"] == scope.catalog_revision, "scope catalog differs");
    ensure!(view.persisted_cursor <= view.cursor, "durable boundary ahead of published boundary");
    let mut bound = view.clone();
    for (id,market) in &view.markets {
        bound.markets.insert(id.clone(),crate::source_public_binding::bind(market,&view.books,view.cursor,generation,&now.to_rfc3339())?);
    }
    let view = &bound;
    let selected: Vec<_> = scope.configured_markets.iter().map(|m|m.market_id.clone()).collect();
    ensure!(!selected.is_empty(), "empty configured full-sync");
    let mut markets = Vec::new();
    let mut dependencies = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    let mut active_tokens = BTreeSet::new();
    for configured in &scope.configured_markets {
        let market = view.markets.get(&configured.market_id).context("configured metadata absent")?;
        let identity = &market["identity"];
        ensure!(identity["condition_id"] == configured.condition_id, "configured condition differs");
        let own: BTreeSet<_> = identity["outcomes"].as_array().context("market outcomes")?.iter()
            .map(|o|o["token_id"].as_str().context("outcome token")).collect::<Result<_>>()?;
        ensure!(own == configured.token_ids.iter().map(String::as_str).collect(), "configured tokens differ");
        tokens.extend(own.iter().map(|t|t.to_string()));
        if market["active"].as_bool().context("market active")? && !market["closed"].as_bool().context("market closed")? {
            active_tokens.extend(own.into_iter().map(str::to_owned));
        }
        for relation in market["relations"].as_array().context("market relations")? {
            for pair in relation["outcome_pairs"].as_array().context("relation pairs")? {
                let mid = pair["market_id"].as_str().context("pair market")?;
                if !selected.iter().any(|s|s == mid) { dependencies.insert(mid.to_owned()); }
                tokens.insert(pair["yes_token_id"].as_str().context("YES token")?.to_owned());
                tokens.insert(pair["no_token_id"].as_str().context("NO token")?.to_owned());
            }
        }
        markets.push(market.clone());
    }
    let dependency_markets: Vec<_> = dependencies.iter().filter_map(|m|view.markets.get(m)).cloned().collect();
    let missing_dependencies: Vec<_> = dependencies.iter().filter(|m|!view.markets.contains_key(*m)).cloned().collect();
    let books: std::collections::BTreeMap<_,_> = tokens.iter().filter_map(|t|view.books.get(t).map(|b|(t.clone(),b.clone()))).collect();
    let mut oldest = now;
    for book in books.values() {
        oldest = oldest.min(DateTime::parse_from_rfc3339(book["received_at"].as_str().context("book received")?)?.with_timezone(&Utc));
    }
    let age = ((now-oldest).num_microseconds().context("book age overflow")? as f64/1000.0).max(0.0);
    let frames: Vec<_> = selected.iter().map(|m|source_public_frame::frame(view,m,now,generation)).collect::<Result<_>>()?;
    let complete = frames.iter().filter(|f|f["status"] == "ready").count();
    let missing = frames.iter().filter(|f|f["status"] == "fail_closed").count();
    let terminal = frames.iter().filter(|f|f["status"] == "terminal").count();
    let active = markets.iter().filter(|m|m["lifecycle_state"] == "active").count();
    let typed_terminal = markets.iter().filter(|m|m["lifecycle_state"] != "active")
        .all(|m|m["lifecycle_state"] == "resolved" && !m["resolution"].is_null());
    let latest_ready = missing == 0 && typed_terminal;
    let mut reasons = BTreeSet::new();
    for frame in &frames { for r in frame["reason_codes"].as_array().context("frame reasons")? {
        reasons.insert(r.as_str().context("reason")?.to_owned());
    }}
    let mut gaps: Vec<_> = view.base["gaps"].as_array().context("source gaps")?.iter().collect();
    gaps.extend(view.recoveries.values().map(|r|&r["gap"]));
    let gap_count = gaps.into_iter().filter(|g|g["resolved"] == false
        && g["token_id"].as_str().is_some_and(|t|tokens.contains(t))).count();
    let common = json!({"catalog_revision":scope.catalog_revision,"projection_generation":generation,
        "scope_market_ids":selected,"freshness_checked_at":now.to_rfc3339(),"scope_id":scope.active_scope_id});
    let binding = json!({"catalog_revision":scope.catalog_revision,"cursor":view.cursor,
        "projection_generation":generation,"scope_id":scope.active_scope_id,
        "markets":dependency_markets,"missing_market_ids":missing_dependencies});
    let mut bootstrap = common.clone();
    bootstrap.as_object_mut().unwrap().extend(json!({
        "contract_version":"marketcow.prediction_market.v1","schema_version":"marketcow.polymarket.live-bootstrap.v2",
        "catalog_source":view.base.get("catalog_source").context("catalog source missing")?,
        "cursor":view.cursor,"markets":markets,"dependency_markets":dependency_markets,
        "missing_dependency_market_ids":missing_dependencies,"dependency_metadata_sha256":canonical_hash(&binding),
        "active_token_ids":active_tokens,"sequence_semantics":"deterministic_normalized","source_policy":"official_free_only",
        "recovery":{"bootstrap":"CLOB POST /books full snapshots","disconnect":"new book_epoch followed by full /books recovery",
            "resume":"in-memory live stream replay required before event resume"}
    }).as_object().unwrap().clone());
    let mut snapshot = common.clone();
    snapshot.as_object_mut().unwrap().extend(json!({"schema_version":"marketcow.polymarket.live-snapshot.v2",
        "cursor":view.cursor,"count":selected.len(),"books":books,"items":frames}).as_object().unwrap().clone());
    let mut health = common.clone();
    health.as_object_mut().unwrap().extend(json!({"schema_version":"marketcow.polymarket.live-read-health.v1",
        "status":if latest_ready {"index_ready"}else{"degraded"},"catalog_index_ready":true,"latest_state_ready":latest_ready,
        "market_count":selected.len(),"token_count":books.len(),"book_token_count":books.len(),
        "book_complete_market_count":complete,"active_market_count":active,"terminal_market_count":terminal,
        "complete_market_count":complete,"missing_market_count":missing,
        "scope_status":if active==selected.len() && complete==selected.len(){"exact_ready"}else if terminal>0 && missing==0{"terminal_degraded"}else{"data_degraded"},
        "unresolved_gap_count":gap_count,"latest_cursor":view.cursor,"persisted_cursor":view.persisted_cursor,
        "persistence_lag_events":view.cursor-view.persisted_cursor,"persistence_queue_depth":view.queued,
        "derived_index_error":null,"live_stream_connected":true,"live_stream_disconnect_count":0,
        "event_loop_stall_max_ms":0,"events_read_source":"memory_projection","realtime_sqlite_query_ms":0,
        "reason_codes":reasons,"oldest_book_received_at":oldest.to_rfc3339(),"maximum_book_age_ms":age,"source_policy":"official_free_only"
    }).as_object().unwrap().clone());
    let mut response = common;
    response.as_object_mut().unwrap().extend(json!({"schema_version":"marketcow.polymarket.live-full-sync.v1",
        "cursor":view.cursor,"oldest_book_received_at":oldest.to_rfc3339(),"maximum_book_age_ms":age,
        "consumer_maximum_book_age_ms":null,"minimum_delivery_headroom_ms":null,"freshness_budget_remaining_ms":null,
        "freshness_policy":"consumer_decides","stream_instance_id":instance,"confirmation_sequence":view.confirmation_sequence,
        "health":health,"bootstrap":bootstrap,"snapshot":snapshot}).as_object().unwrap().clone());
    Ok(response)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredMarket {
    pub market_id: String,
    pub condition_id: String,
    pub token_ids: [String; 2],
    pub end_at: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredScope {
    pub schema_version: String,
    pub mode: String,
    pub active_scope_id: String,
    pub catalog_revision: String,
    pub configured_market_count: usize,
    pub configured_markets: Vec<ConfiguredMarket>,
}

impl ConfiguredScope {
    /// Validate the immutable input; no discovery traversal or automatic
    /// selection is performed by the online server.
    pub fn validate(&self) -> Result<()> {
        ensure!(self.schema_version == "marketcow.polymarket.scope-discovery.v1", "scope schema");
        ensure!(self.mode == "shadow", "scope mode");
        ensure!(self.configured_market_count == self.configured_markets.len()
            && self.configured_market_count <= 250, "scope count");
        ensure!(self.catalog_revision.len() == 64 && self.catalog_revision.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)), "catalog revision");
        let mut ids = BTreeSet::new();
        for market in &self.configured_markets {
            ensure!(!market.market_id.is_empty() && !market.condition_id.is_empty()
                && ids.insert(&market.market_id), "scope market identity");
            ensure!(market.token_ids.iter().all(|t| !t.is_empty())
                && market.token_ids[0] != market.token_ids[1], "scope token identity");
            chrono::DateTime::parse_from_rfc3339(&market.end_at)?;
        }
        let expected = canonical_hash(&serde_json::json!({
            "catalog_revision": self.catalog_revision,
            "configured_markets": self.configured_markets,
            "mode": self.mode,
        }));
        ensure!(self.active_scope_id == expected, "scope content hash");
        Ok(())
    }
}

#[derive(Debug)]
pub enum EncodeError {
    InvalidLimit,
    TooLarge,
    Json(serde_json::Error),
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimit => f.write_str("explicit positive response limit required"),
            Self::TooLarge => f.write_str("response byte limit exceeded"),
            Self::Json(error) => write!(f, "response encoding: {error}"),
        }
    }
}
impl std::error::Error for EncodeError {}

struct LimitedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl Write for LimitedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("public response byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

/// Encodes atomically under an explicit payload limit; never allocates the
/// complete oversized JSON before checking its size. Not a process RSS cap.
pub fn encode_bounded(value: &impl serde::Serialize, limit: usize) -> Result<Vec<u8>, EncodeError> {
    if limit == 0 { return Err(EncodeError::InvalidLimit); }
    let mut output = LimitedBuffer { bytes: Vec::new(), limit, exceeded: false };
    let result = serde_json::to_writer(&mut output, value);
    if output.exceeded { return Err(EncodeError::TooLarge); }
    result.map_err(EncodeError::Json)?;
    Ok(output.bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn snapshot_admission_lives_until_last_wire_buffer_reference() {
        let capacity=Arc::new(Semaphore::new(1));
        let permit=capacity.clone().try_acquire_owned().unwrap();
        let bytes=axum::body::Bytes::from_owner(SnapshotBytes{bytes:vec![1,2,3],_permit:permit});
        let network_reference=bytes.clone();
        drop(bytes);
        assert!(capacity.clone().try_acquire_owned().is_err());
        assert_eq!(&network_reference[..],&[1,2,3]);
        drop(network_reference);
        assert!(capacity.try_acquire_owned().is_ok());
    }

    #[test]
    fn scope_binds_exact_content_and_rejects_unknown_fields() {
        let mut scope = ConfiguredScope {
            schema_version: "marketcow.polymarket.scope-discovery.v1".into(),
            mode: "shadow".into(), active_scope_id: String::new(),
            catalog_revision: "a".repeat(64), configured_market_count: 1,
            configured_markets: vec![ConfiguredMarket { market_id: "1".into(),
                condition_id: "condition".into(), token_ids: ["yes".into(), "no".into()],
                end_at: "2026-09-06T00:00:00Z".into() }],
        };
        scope.active_scope_id = canonical_hash(&serde_json::json!({
            "catalog_revision":scope.catalog_revision,"configured_markets":scope.configured_markets,"mode":scope.mode}));
        scope.validate().unwrap();
        let mut raw = serde_json::to_value(&scope).unwrap();
        raw["extra"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ConfiguredScope>(raw).is_err());
        scope.configured_markets[0].token_ids[1] = "other".into();
        assert!(scope.validate().is_err());
        scope.configured_markets[0].token_ids[1] = "yes".into();
        assert!(scope.validate().is_err());
    }

    #[test]
    fn exact_limit_and_utf8_bytes_are_enforced() {
        let value = serde_json::json!({"market":"市场"});
        let expected = serde_json::to_vec(&value).unwrap();
        assert_eq!(encode_bounded(&value, expected.len()).unwrap(), expected);
        assert!(matches!(encode_bounded(&value, expected.len()-1), Err(EncodeError::TooLarge)));
        assert!(matches!(encode_bounded(&value, 0), Err(EncodeError::InvalidLimit)));
    }

    #[test]
    fn failed_write_preserves_budget_and_never_returns_partial_response() {
        let mut output = LimitedBuffer { bytes: Vec::new(), limit: 4, exceeded: false };
        output.write_all(b"abc").unwrap();
        assert!(output.write_all(b"de").is_err());
        assert_eq!(output.bytes, b"abc");
        assert!(output.exceeded);
    }
}
