//! Bounded BTC five-minute research projection over the shared MarketCow
//! publication.  It owns no upstream connection or persistence path.
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use marketcow_polymarket::discovery_source::canonical_hash;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, VecDeque},
    sync::{Arc, Mutex, OnceLock, RwLock, atomic::{AtomicU64, Ordering}},
    time::Duration,
};
use tokio::sync::{Semaphore, watch};

use crate::source_public_api::encode_bounded;
use crate::source_publication::{MemoryReader, MemoryView, SourceBatchTiming};

const PROFILE_SCHEMA: &str = "marketcow.polymarket.btc-5m-profile.v1";
const STATUS_SCHEMA: &str = "marketcow.polymarket.btc-5m-status.v1";
const FULL_SYNC_SCHEMA: &str = "marketcow.polymarket.btc-5m-full-sync.v1";
const EVENT_SCHEMA: &str = "marketcow.polymarket.btc-5m-event.v1";
const COMPACT_EVENT_SCHEMA: &str = "marketcow.polymarket.btc-5m-event.v2";
const ERROR_SCHEMA: &str = "marketcow.polymarket.btc-5m-error.v1";
// Fixed v1 management-contract TTL, not an implicit runtime fallback.
const MANAGER_STALE_AFTER_SECONDS: i64 = 60;
const MAX_MARKETS: usize = 3;
const MAX_TOKENS: usize = 6;
const TIMELINE_RECEIPT_BYTES: usize = 16 * 1024 * 1024;
const TIMELINE_RECEIPT_COUNT: usize = 16_384;
// At most four writer-watermark control frames per second per connection.
// This does not delay book events or the writer's own fsync notification.
const DURABLE_WATERMARK_MIN_INTERVAL: Duration = Duration::from_millis(250);
static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct TimelineArchive {
    entries: VecDeque<(String, u64, String, Value, usize)>,
    bytes: usize,
}
impl TimelineArchive {
    fn push(&mut self, epoch: &str, cursor: u64, event_id: &str, receipt: Value) {
        let Ok(bytes) = serde_json::to_vec(&receipt).map(|v| v.len()) else { return; };
        if bytes > TIMELINE_RECEIPT_BYTES { return; }
        self.bytes += bytes;
        self.entries.push_back((epoch.to_owned(), cursor, event_id.to_owned(), receipt, bytes));
        while self.entries.len() > TIMELINE_RECEIPT_COUNT || self.bytes > TIMELINE_RECEIPT_BYTES {
            if let Some((_, _, _, _, evicted_bytes)) = self.entries.pop_front() {
                self.bytes -= evicted_bytes;
            }
        }
    }
    fn lookup(&self, epoch: &str, cursor: u64, event_id: &str) -> Vec<Value> {
        self.entries.iter().filter(|(e, c, id, _, _)|
            e == epoch && *c == cursor && id == event_id
        ).take(8).map(|(_, _, _, receipt, _)| receipt.clone()).collect()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum WindowRole {
    Current,
    Next,
    Retiring,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct OutcomeBinding {
    pub outcome: String,
    pub token_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketBinding {
    pub role: WindowRole,
    pub window_id: String,
    pub start_at: String,
    pub end_at: String,
    pub market_id: String,
    pub condition_id: String,
    pub outcomes: [OutcomeBinding; 2],
    pub rule_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub schema_version: String,
    pub profile_id: String,
    pub profile_revision: u64,
    pub catalog_revision: String,
    pub markets: Vec<MarketBinding>,
    pub maximum_markets: usize,
    pub maximum_quote_age_ms: u64,
    pub maximum_tokens: usize,
}

fn hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl Profile {
    fn identity_value(&self) -> Value {
        json!({"profile_revision":self.profile_revision,"catalog_revision":self.catalog_revision,
            "markets":self.markets,"maximum_markets":self.maximum_markets,
            "maximum_quote_age_ms":self.maximum_quote_age_ms,
            "maximum_tokens":self.maximum_tokens})
    }
    pub fn expected_id(&self) -> String {
        canonical_hash(&self.identity_value())
    }
    pub fn document_sha256(&self) -> String {
        canonical_hash(&serde_json::to_value(self).expect("BTC 5m profile serializes"))
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == PROFILE_SCHEMA,
            "BTC 5m profile schema"
        );
        ensure!(
            self.profile_revision > 0 && hex64(&self.catalog_revision),
            "BTC 5m profile revision"
        );
        ensure!(
            self.maximum_markets == MAX_MARKETS && self.maximum_tokens == MAX_TOKENS,
            "BTC 5m fixed resource profile"
        );
        ensure!(
            (100..=60_000).contains(&self.maximum_quote_age_ms),
            "BTC 5m quote age budget"
        );
        ensure!(
            (1..=MAX_MARKETS).contains(&self.markets.len()),
            "BTC 5m market budget"
        );
        ensure!(
            self.profile_id == self.expected_id(),
            "BTC 5m profile identity"
        );
        let mut roles = BTreeSet::new();
        let mut markets = BTreeSet::new();
        let mut tokens = BTreeSet::new();
        let mut windows = BTreeSet::new();
        for market in &self.markets {
            ensure!(
                roles.insert(market.role.clone()),
                "duplicate BTC 5m window role"
            );
            ensure!(
                markets.insert(&market.market_id) && windows.insert(&market.window_id),
                "duplicate BTC 5m market/window"
            );
            ensure!(
                !market.market_id.is_empty()
                    && market.condition_id.len() == 66
                    && market.condition_id.starts_with("0x")
                    && hex64(&market.condition_id[2..])
                    && hex64(&market.rule_sha256),
                "BTC 5m market identity"
            );
            let start = DateTime::parse_from_rfc3339(&market.start_at)?;
            let end = DateTime::parse_from_rfc3339(&market.end_at)?;
            ensure!(
                end.signed_duration_since(start).num_seconds() == 300,
                "BTC 5m window must be exactly five minutes"
            );
            ensure!(
                market.outcomes[0].outcome == "Up" && market.outcomes[1].outcome == "Down",
                "BTC 5m ordered Up/Down identity"
            );
            ensure!(
                market
                    .outcomes
                    .iter()
                    .all(|o| !o.token_id.is_empty() && tokens.insert(&o.token_id)),
                "duplicate/empty BTC 5m token"
            );
        }
        let current = self
            .markets
            .iter()
            .find(|market| market.role == WindowRole::Current)
            .context("BTC 5m current window required")?;
        if let Some(next) = self
            .markets
            .iter()
            .find(|market| market.role == WindowRole::Next)
        {
            ensure!(
                next.start_at == current.end_at,
                "BTC 5m next window must be adjacent"
            );
        }
        if let Some(retiring) = self
            .markets
            .iter()
            .find(|market| market.role == WindowRole::Retiring)
        {
            ensure!(
                retiring.end_at == current.start_at,
                "BTC 5m retiring window must be adjacent"
            );
        }
        ensure!(
            roles.contains(&WindowRole::Current),
            "BTC 5m current window required"
        );
        ensure!(tokens.len() <= MAX_TOKENS, "BTC 5m token budget");
        Ok(())
    }
    /// Recheck the wall-clock-sensitive part of a profile at the actual
    /// control-plane mutation boundary. A manager-side review can become
    /// stale while discovery, preparation, or the control request is queued.
    pub fn validate_current_at(&self, now: DateTime<Utc>) -> Result<()> {
        self.validate()?;
        let current = self
            .window(WindowRole::Current)
            .context("BTC 5m current window")?;
        let start = DateTime::parse_from_rfc3339(&current.start_at)?.with_timezone(&Utc);
        let end = DateTime::parse_from_rfc3339(&current.end_at)?.with_timezone(&Utc);
        ensure!(
            start <= now && now < end,
            "BTC 5m profile current window is not active"
        );
        Ok(())
    }
    fn market_ids(&self) -> BTreeSet<&str> {
        self.markets.iter().map(|m| m.market_id.as_str()).collect()
    }
    fn window(&self, role: WindowRole) -> Option<&MarketBinding> {
        self.markets.iter().find(|market| market.role == role)
    }
    fn same_market_identity(left: &MarketBinding, right: &MarketBinding) -> bool {
        left.window_id == right.window_id
            && left.start_at == right.start_at
            && left.end_at == right.end_at
            && left.market_id == right.market_id
            && left.condition_id == right.condition_id
            && left.outcomes == right.outcomes
            && left.rule_sha256 == right.rule_sha256
    }
    /// A steady-state rotation reuses the already subscribed `next` as the
    /// new `current` and adds exactly one new `next`. A caller may retain the
    /// old `current` as `retiring`, but the autonomous manager intentionally
    /// uses the two-window form so a closed market never has to be
    /// re-subscribed after restart. Both forms are one-market replacements
    /// and cannot introduce an unrelated fourth market.
    pub fn validate_rotation_from(&self, incumbent: &Self) -> Result<()> {
        self.validate()?;
        incumbent.validate()?;
        ensure!(
            incumbent.markets.len() == self.markets.len()
                && matches!(self.markets.len(), 2 | MAX_MARKETS),
            "BTC 5m steady-state rotation requires equal two/three-window profiles"
        );
        let old_current = incumbent
            .window(WindowRole::Current)
            .context("incumbent current window")?;
        let old_next = incumbent
            .window(WindowRole::Next)
            .context("incumbent next window")?;
        let new_current = self
            .window(WindowRole::Current)
            .context("candidate current window")?;
        let new_next = self
            .window(WindowRole::Next)
            .context("candidate next window")?;
        if self.markets.len() == MAX_MARKETS {
            let new_retiring = self
                .window(WindowRole::Retiring)
                .context("candidate retiring window")?;
            ensure!(
                Self::same_market_identity(old_current, new_retiring),
                "BTC 5m candidate retiring must equal incumbent current"
            );
        } else {
            ensure!(
                self.window(WindowRole::Retiring).is_none()
                    && incumbent.window(WindowRole::Retiring).is_none(),
                "BTC 5m two-window rotation cannot retain retiring"
            );
        }
        ensure!(
            Self::same_market_identity(old_next, new_current),
            "BTC 5m candidate current must equal incumbent next"
        );
        ensure!(
            new_next.start_at == new_current.end_at
                && !incumbent.market_ids().contains(new_next.market_id.as_str()),
            "BTC 5m candidate must add one adjacent new next market"
        );
        Ok(())
    }
    pub fn market_ids_owned(&self) -> BTreeSet<String> {
        self.markets.iter().map(|m| m.market_id.clone()).collect()
    }
    fn token_ids(&self) -> BTreeSet<&str> {
        self.markets
            .iter()
            .flat_map(|m| m.outcomes.iter().map(|o| o.token_id.as_str()))
            .collect()
    }
}

#[derive(Clone)]
pub struct Limits {
    pub snapshot_bytes: usize,
    pub frame_bytes: usize,
    pub replay_bytes: usize,
    pub clients: usize,
    pub send_timeout: Duration,
    pub snapshot_lease_capacity: usize,
}
impl Limits {
    fn validate(&self) -> Result<()> {
        ensure!(
            (64 * 1024..=16 * 1024 * 1024).contains(&self.snapshot_bytes),
            "BTC 5m snapshot bytes"
        );
        ensure!(
            (4 * 1024..=2 * 1024 * 1024).contains(&self.frame_bytes),
            "BTC 5m frame bytes"
        );
        ensure!(
            (64 * 1024..=8 * 1024 * 1024).contains(&self.replay_bytes),
            "BTC 5m replay bytes"
        );
        ensure!(
            (1..=8).contains(&self.clients)
                && (1..=4).contains(&self.snapshot_lease_capacity)
                && !self.send_timeout.is_zero(),
            "BTC 5m client budget"
        );
        Ok(())
    }
}

fn instant(value: &Value, key: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(
        value[key]
            .as_str()
            .with_context(|| format!("missing {key}"))?,
    )?
    .with_timezone(&Utc))
}
fn levels<'a>(book: &'a Value, side: &str) -> Result<&'a Vec<Value>> {
    book[side]
        .as_array()
        .with_context(|| format!("missing {side}"))
}
fn best_price(book: &Value, side: &str, maximum: bool) -> Result<Option<String>> {
    let mut values = Vec::new();
    for level in levels(book, side)? {
        let raw = level["price"].as_str().context("book level price")?;
        values.push((raw.parse::<rust_decimal::Decimal>()?, raw.to_owned()));
    }
    values.sort_by_key(|v| v.0);
    Ok(if maximum {
        values.last()
    } else {
        values.first()
    }
    .map(|v| v.1.clone()))
}

fn token_snapshot(
    view: &MemoryView,
    outcome: &OutcomeBinding,
    now: DateTime<Utc>,
    window_reason: Option<&str>,
    maximum_quote_age_ms: u64,
) -> Result<Value> {
    let token = &outcome.token_id;
    let recovery = view.recoveries.get(token);
    let unresolved_seed_gap = view.base["gaps"].as_array().is_some_and(|gaps| {
        gaps.iter()
            .any(|gap| gap["token_id"] == token.as_str() && gap["resolved"].as_bool() != Some(true))
    });
    let book = view.books.get(token);
    let confirmation = view.confirmation_facts.get(token);
    let mut reason: Option<&str> = None;
    if window_reason.is_some() {
        reason = window_reason;
    } else if recovery.is_some() || unresolved_seed_gap {
        reason = Some("source_gap");
    } else if book.is_none() {
        reason = Some("book_unavailable");
    }
    let (best_bid, best_ask, received, confirmed, exchange, crossed, two_sided) =
        if let Some(book) = book {
            let bid = best_price(book, "bids", true)?;
            let ask = best_price(book, "asks", false)?;
            let crossed = match (&bid, &ask) {
                (Some(b), Some(a)) => {
                    b.parse::<rust_decimal::Decimal>()? >= a.parse::<rust_decimal::Decimal>()?
                }
                _ => false,
            };
            (
                bid,
                ask,
                Some(instant(book, "received_at")?),
                confirmation
                    .and_then(|value| value["confirmed_at"].as_str())
                    .map(DateTime::parse_from_rfc3339)
                    .transpose()?
                    .map(|value| value.with_timezone(&Utc)),
                book["exchange_at"].as_str().map(str::to_owned),
                crossed,
                !levels(book, "bids")?.is_empty() && !levels(book, "asks")?.is_empty(),
            )
        } else {
            (None, None, None, None, None, false, false)
        };
    if reason.is_none() && crossed {
        reason = Some("crossed_book");
    }
    let freshness_at = match (received, confirmed) {
        (Some(book), Some(confirmation)) => Some(book.max(confirmation)),
        (book, None) => book,
        (None, confirmation) => confirmation,
    };
    let signed_age_ms = freshness_at.map(|at| (now - at).num_milliseconds());
    if reason.is_none() {
        if signed_age_ms.is_some_and(|age| age < 0) {
            reason = Some("future_received_at");
        } else if signed_age_ms.is_some_and(|age| age > maximum_quote_age_ms as i64) {
            reason = Some("quote_stale");
        }
    }
    if reason.is_none() && !two_sided {
        reason = Some("incomplete_book");
    }
    let available = reason.is_none();
    // Quote availability and replay continuity are separate facts.  A stale
    // but structurally valid canonical book is not tradable, yet it remains
    // the exact baseline for a later ordered delta.  During token recovery it
    // becomes a valid baseline only after the authoritative snapshot recorded
    // by the recovery state has actually replaced the pre-gap book.
    let recovery_snapshot_matches = recovery.is_some_and(|state| {
        state["snapshot_cursor"]
            .as_u64()
            .zip(view.book_cursors.get(token).copied())
            .is_some_and(|(snapshot, book_cursor)| snapshot == book_cursor)
    });
    let replay_baseline_valid = book.is_some()
        && !crossed
        && signed_age_ms.is_some_and(|age| age >= 0)
        && !unresolved_seed_gap
        && (recovery.is_none() || recovery_snapshot_matches);
    Ok(
        json!({"outcome":outcome.outcome,"token_id":token,"available":available,
        "replay_baseline_valid":replay_baseline_valid,
        "confirmation_sequence":view.confirmation_versions.get(token),
        "confirmed_at":confirmed.map(|value|value.to_rfc3339()),
        "confirmation_source":confirmation.and_then(|value|value.get("confirmation_source")).cloned(),
        "confirmation_evidence_sha256":confirmation.and_then(|value|value.get("confirmation_evidence_sha256")).cloned(),
        "recovery_id":recovery.and_then(|value|value["recovery_id"].as_str()),
        "recovery_snapshot_cursor":recovery.and_then(|value|value["snapshot_cursor"].as_u64()),
        "unavailable_reason":reason,"publisher_cursor":view.book_cursors.get(token),
        "source_event_cursor":book.and_then(|b|b.get("source_sequence")).cloned(),
        "book_epoch":book.and_then(|b|b.get("book_epoch")).cloned(),
        "state_checksum":book.and_then(|b|b.get("state_checksum")).cloned(),
        "canonical_book_sha256":book.map(|value|canonical_hash(value.as_ref())),
        "tick_version":book.and_then(|b|b.get("tick_version")).cloned(),
        "tick_size":book.and_then(|b|b.get("tick_size")).cloned(),
        "source_hash":book.and_then(|b|b.get("source_hash")).cloned(),
        "canonical_book":book.cloned(),
        "exchange_at":exchange,"received_at":received.map(|v|v.to_rfc3339()),
        "freshness_at":freshness_at.map(|v|v.to_rfc3339()),
        "quote_age_ms":signed_age_ms.filter(|age|*age>=0),
        "future_skew_ms":signed_age_ms.filter(|age|*age<0).map(|age|-age),
        "best_bid":best_bid,"best_ask":best_ask,
        "last_trade_price":book.and_then(|b|b.get("last_trade_price")).cloned(),
        "bids":if available {book.map(|b|b["bids"].clone())} else {None},
        "asks":if available {book.map(|b|b["asks"].clone())} else {None}}),
    )
}

pub fn full_sync(
    view: &MemoryView,
    profile: &Profile,
    epoch: &str,
    now: DateTime<Utc>,
) -> Result<Value> {
    profile.validate()?;
    ensure!(
        !epoch.is_empty() && view.persisted_cursor <= view.cursor,
        "BTC 5m publication identity"
    );
    ensure!(
        view.base["catalog_revision"] == profile.catalog_revision,
        "BTC 5m catalog revision"
    );
    let mut markets = Vec::new();
    let mut current_ready = false;
    let mut unavailable = 0usize;
    for configured in &profile.markets {
        let metadata = view
            .markets
            .get(&configured.market_id)
            .context("BTC 5m metadata absent")?;
        ensure!(
            metadata["identity"]["condition_id"] == configured.condition_id,
            "BTC 5m condition changed"
        );
        let identities = metadata["identity"]["outcomes"]
            .as_array()
            .context("BTC 5m outcomes")?;
        ensure!(
            identities.len() == 2
                && identities[0]["outcome"] == "Up"
                && identities[0]["token_id"] == configured.outcomes[0].token_id
                && identities[1]["outcome"] == "Down"
                && identities[1]["token_id"] == configured.outcomes[1].token_id,
            "BTC 5m ordered token binding changed"
        );
        let end = DateTime::parse_from_rfc3339(&configured.end_at)?.with_timezone(&Utc);
        let start = DateTime::parse_from_rfc3339(&configured.start_at)?.with_timezone(&Utc);
        let window_reason = match configured.role {
            WindowRole::Current if now < start || now >= end => Some("market_window_inactive"),
            WindowRole::Next if now >= end => Some("market_window_inactive"),
            WindowRole::Retiring if now >= end => Some("market_window_ended"),
            _ => None,
        };
        let terminated = window_reason == Some("market_window_ended");
        let tokens = configured
            .outcomes
            .iter()
            .map(|o| token_snapshot(view, o, now, window_reason, profile.maximum_quote_age_ms))
            .collect::<Result<Vec<_>>>()?;
        let ready = tokens.iter().all(|t| t["available"] == true) && !terminated;
        unavailable += tokens.iter().filter(|t| t["available"] == false).count();
        if configured.role == WindowRole::Current {
            current_ready = ready;
        }
        markets.push(json!({"role":configured.role,"window_id":configured.window_id,
            "start_at":configured.start_at,"end_at":configured.end_at,"market_id":configured.market_id,
            "condition_id":configured.condition_id,"rule_sha256":configured.rule_sha256,
            "status":if terminated{"book_terminated"}else if ready{"ready"}else{"unavailable"},
            "book_terminated":terminated,"settlement_status":"independent","cross_token_atomic":false,
            "tokens":tokens}));
    }
    Ok(
        json!({"schema_version":FULL_SYNC_SCHEMA,"profile_id":profile.profile_id,
        "profile_revision":profile.profile_revision,"profile_document_sha256":profile.document_sha256(),
        "profile":profile,"epoch":epoch,"publisher_cursor":view.cursor,
        "manager":null,
        "durable_cursor":view.persisted_cursor,"confirmation_sequence":view.confirmation_sequence,
        "ready":current_ready,
        "maximum_quote_age_ms":profile.maximum_quote_age_ms,
        "crossed_book_rule":"best_bid_greater_than_or_equal_to_best_ask_is_invalid",
        "acquisition_membership_semantics":"component_subset_leases_deduplicated_shared_union_before_profile_cas",
        "ready_barrier":"full_sync_cursor_then_stream_after_cursor_strictly_greater",
        "after_cursor_semantics":"exclusive","replay_expiry_recovery":"new_full_sync",
        "source_event_cursor_semantics":"upstream_optional_and_never_comparable_to_publisher_cursor",
        "transport_alive":view.base.get("live_stream_connected").cloned().unwrap_or(Value::Null),
        "heartbeat_and_quote_freshness_are_independent":true,"unavailable_token_count":unavailable,
        "observed_at":now.to_rfc3339(),"markets":markets}),
    )
}

pub fn status(
    view: &MemoryView,
    profile: &Profile,
    epoch: &str,
    now: DateTime<Utc>,
) -> Result<Value> {
    let sync = full_sync(view, profile, epoch, now)?;
    let windows = sync["markets"]
        .as_array()
        .context("BTC 5m markets")?
        .iter()
        .map(|m| {
            json!({
        "role":m["role"],"window_id":m["window_id"],"start_at":m["start_at"],"end_at":m["end_at"],
        "market_id":m["market_id"],"condition_id":m["condition_id"],"status":m["status"]})
        })
        .collect::<Vec<_>>();
    Ok(
        json!({"schema_version":STATUS_SCHEMA,"profile_id":profile.profile_id,
        "profile_revision":profile.profile_revision,"profile_document_sha256":profile.document_sha256(),
        "profile":profile,"epoch":epoch,"published_cursor":view.cursor,
        "manager":null,
        "durable_cursor":view.persisted_cursor,"confirmation_sequence":view.confirmation_sequence,
        "ready":sync["ready"],
        "reason":if sync["ready"]==true {Value::Null}else{json!("current_market_not_ready")},
        "transport_alive":sync["transport_alive"],"heartbeat_and_quote_freshness_are_independent":true,
        "maximum_markets":MAX_MARKETS,"maximum_tokens":MAX_TOKENS,"windows":windows}),
    )
}

fn event_frame(profile: &Profile, epoch: &str, event: &Value) -> Result<Option<Value>> {
    let cursor = event["cursor"].as_u64().context("publisher cursor")?;
    let market = event["market_id"].as_str();
    let token = event["token_id"].as_str();
    if !market.is_some_and(|m| profile.market_ids().contains(m))
        && !token.is_some_and(|t| profile.token_ids().contains(t))
    {
        return Ok(None);
    }
    let publication_type = event["event_type"]
        .as_str()
        .context("publication event type")?;
    let source_type = if publication_type == "book" {
        event["raw_payload"]["event_type"]
            .as_str()
            .context("raw source event type")?
    } else {
        event["raw_payload"]["source_event_type"]
            .as_str()
            .unwrap_or(publication_type)
    };
    let event_type = match (publication_type, source_type) {
        ("recovery_started", _) => "recovery_started",
        ("recovery_completed", _) => "recovery_completed",
        ("book", "book") => "book_snapshot",
        // Tick changes advance the canonical token sequence. Expose the
        // reducer's complete committed post-state so compact consumers do not
        // observe an unexplained sequence hole.
        ("book", "tick_size_change") => "book_snapshot",
        ("book", "price_change") => "book_change",
        ("book", "last_trade_price") => "last_trade",
        _ => return Ok(None),
    };
    let canonical = event
        .get("canonical_payload")
        .context("canonical payload")?;
    ensure!(
        !canonical["payload_semantics"].is_string(),
        "BTC 5m wire v1 cannot replay compact incremental facts; use wire_version=2 and a fresh full-sync"
    );
    let semantics = match source_type {
        "book" | "price_change" | "last_trade_price" | "tick_size_change" => {
            for field in [
                "token_id",
                "condition_id",
                "book_epoch",
                "sequence",
                "exchange_at",
                "received_at",
                "tick_version",
                "tick_size",
                "bids",
                "asks",
                "last_trade_price",
                "state_checksum",
                "source_hash",
            ] {
                ensure!(
                    canonical.get(field).is_some(),
                    "BTC 5m incomplete canonical book state"
                );
            }
            "complete_post_state"
        }
        _ => "control_record",
    };
    let invalidated = publication_type == "recovery_started";
    Ok(Some(
        json!({"schema_version":EVENT_SCHEMA,"type":event_type,"epoch":epoch,
        "publisher_cursor":cursor,"source_event_cursor":canonical.get("sequence").cloned(),
        "source_event_type":source_type,"market_id":event["market_id"],"token_id":event["token_id"],
        "availability":if invalidated{"unavailable"}else if source_type=="book"{"authoritative_snapshot"}else{"unchanged"},
        "payload_semantics":semantics,
        "payload":{"contract_version":event["contract_version"],"source_schema_version":event["schema_version"],
            "publication_event_type":publication_type,"raw_source_event_type":source_type,
            "event_id":event["event_id"],"book_epoch":event["book_epoch"],"sequence":event["sequence"],
            "exchange_at":event["exchange_at"],"received_at":event["received_at"],
            "canonical_payload_sha256":event["canonical_payload_sha256"],"canonical_payload":canonical,
            "raw_payload_sha256":event["raw_payload_sha256"],"applied":event["applied"],
            "fail_closed_reason":event["fail_closed_reason"],"gaps":event["gaps"]},
        "durability":"published_not_necessarily_durable"}),
    ))
}

/// Version 2 keeps authoritative snapshots complete, but represents ordinary
/// price/trade increments as the exact normalized mutation plus a commitment
/// to the complete post-state. A consumer starts from full-sync, applies the
/// ordered delta, then must reproduce both `state_checksum` and
/// `canonical_book_sha256`; a mismatch is a resync boundary, never permission
/// to keep an approximate book.
fn compact_event_frame(profile: &Profile, epoch: &str, event: &Value) -> Result<Option<Value>> {
    if event["canonical_payload"]["payload_semantics"].is_string() {
        let cursor = event["cursor"].as_u64().context("publisher cursor")?;
        let market = event["market_id"].as_str();
        let token = event["token_id"].as_str();
        if !market.is_some_and(|m| profile.market_ids().contains(m))
            && !token.is_some_and(|t| profile.token_ids().contains(t))
        {
            return Ok(None);
        }
        let token = token.context("BTC 5m compact token")?;
        let canonical = &event["canonical_payload"];
        let semantics = canonical["payload_semantics"]
            .as_str()
            .context("BTC 5m compact semantics")?;
        let (kind, source_type) = match semantics {
            "ordered_depth_delta_with_complete_post_state_commitment" => {
                ("book_change", "price_change")
            }
            "last_trade_with_unchanged_depth_commitment" => ("last_trade", "last_trade_price"),
            _ => anyhow::bail!("unknown BTC 5m compact semantics"),
        };
        let post_state = json!({
            "token_id":canonical["token_id"],"condition_id":canonical["condition_id"],
            "book_epoch":canonical["book_epoch"],"sequence":canonical["sequence"],
            "sequence_semantics":canonical["sequence_semantics"],
            "exchange_at":canonical["exchange_at"],"received_at":canonical["received_at"],
            "tick_version":canonical["tick_version"],"tick_size":canonical["tick_size"],
            "last_trade_price":canonical["last_trade_price"],
            "state_checksum":canonical["state_checksum"],"source_hash":canonical["source_hash"],
            "canonical_book_sha256":canonical["canonical_book_sha256"]
        });
        let mut payload = json!({
            "contract_version":event["contract_version"],
            "source_schema_version":event["schema_version"],
            "publication_event_type":event["event_type"],
            "raw_source_event_type":source_type,"event_id":event["event_id"],
            "book_epoch":event["book_epoch"],"sequence":event["sequence"],
            "exchange_at":event["exchange_at"],"received_at":event["received_at"],
            "canonical_payload_sha256":event["canonical_payload_sha256"],
            "raw_payload_sha256":event["raw_payload_sha256"],"applied":event["applied"],
            "fail_closed_reason":event["fail_closed_reason"],"gaps":event["gaps"],
            "post_state":post_state
        });
        if kind == "book_change" {
            payload["delta"] = canonical["delta"].clone();
        } else {
            payload["trade"] = canonical["trade"].clone();
        }
        return Ok(Some(json!({
            "schema_version":COMPACT_EVENT_SCHEMA,"type":kind,"epoch":epoch,
            "publisher_cursor":cursor,"source_event_cursor":canonical["sequence"],
            "source_event_type":source_type,"market_id":event["market_id"],
            "token_id":token,"availability":"unchanged","payload_semantics":semantics,
            "payload":payload,"durability":"published_not_necessarily_durable"
        })));
    }
    let Some(mut frame) = event_frame(profile, epoch, event)? else {
        return Ok(None);
    };
    frame["schema_version"] = json!(COMPACT_EVENT_SCHEMA);
    let kind = frame["type"]
        .as_str()
        .context("BTC 5m event type")?
        .to_owned();
    if !matches!(kind.as_str(), "book_change" | "last_trade") {
        return Ok(Some(frame));
    }
    let token = frame["token_id"]
        .as_str()
        .context("BTC 5m compact token")?
        .to_owned();
    let canonical = event["canonical_payload"].clone();
    let post_state = json!({
        "token_id":canonical["token_id"],"condition_id":canonical["condition_id"],
        "book_epoch":canonical["book_epoch"],"sequence":canonical["sequence"],
        "sequence_semantics":canonical["sequence_semantics"],
        "exchange_at":canonical["exchange_at"],"received_at":canonical["received_at"],
        "tick_version":canonical["tick_version"],"tick_size":canonical["tick_size"],
        "last_trade_price":canonical["last_trade_price"],
        "state_checksum":canonical["state_checksum"],"source_hash":canonical["source_hash"],
        "canonical_book_sha256":event["canonical_payload_sha256"]
    });
    frame["payload_semantics"] = json!(if kind == "book_change" {
        "ordered_depth_delta_with_complete_post_state_commitment"
    } else {
        "last_trade_with_unchanged_depth_commitment"
    });
    frame["payload"]
        .as_object_mut()
        .context("BTC 5m event payload")?
        .remove("canonical_payload");
    frame["payload"]["post_state"] = post_state;
    if kind == "book_change" {
        let changes = event["raw_payload"]["price_changes"]
            .as_array()
            .context("BTC 5m price changes")?
            .iter()
            .filter(|change| change["asset_id"] == token)
            .map(|change| {
                json!({"asset_id":change["asset_id"],"side":change["side"],
                    "price":change["price"],"size":change["size"]})
            })
            .collect::<Vec<_>>();
        ensure!(!changes.is_empty(), "BTC 5m token delta absent");
        frame["payload"]["delta"] = json!({"changes":changes});
    } else {
        frame["payload"]["trade"] = json!({
            "price":canonical["last_trade_price"],
            "exchange_at":event["exchange_at"],"received_at":event["received_at"]
        });
    }
    Ok(Some(frame))
}

fn confirmation_frame(
    profile: &Profile,
    epoch: &str,
    publisher_cursor: u64,
    confirmation_sequence: u64,
    books: &[Arc<Value>],
) -> Result<Option<Value>> {
    let allowed = profile.token_ids();
    let mut confirmations = Vec::new();
    for book in books {
        let token = book["token_id"]
            .as_str()
            .context("BTC 5m confirmation token")?;
        if !allowed.contains(token) {
            continue;
        }
        let confirmed_at = book["confirmed_at"]
            .as_str()
            .context("BTC 5m confirmation time")?;
        DateTime::parse_from_rfc3339(confirmed_at)?;
        confirmations.push(json!({
            "token_id":token,
            "condition_id":book["condition_id"],
            "book_epoch":book["book_epoch"],
            "book_sequence":book["sequence"],
            "state_checksum":book["state_checksum"],
            "book_received_at":book["book_received_at"],
            "confirmed_at":confirmed_at,
            "confirmation_source":book["confirmation_source"],
            "confirmation_evidence_sha256":book["confirmation_evidence_sha256"]
        }));
    }
    if confirmations.is_empty() {
        return Ok(None);
    }
    confirmations.sort_by(|left, right| left["token_id"].as_str().cmp(&right["token_id"].as_str()));
    Ok(Some(json!({
        "schema_version":COMPACT_EVENT_SCHEMA,
        "type":"book_confirmations",
        "epoch":epoch,
        "publisher_cursor":publisher_cursor,
        "confirmation_sequence":confirmation_sequence,
        "payload_semantics":"freshness_confirmation_without_depth_or_publisher_cursor_mutation",
        "confirmations":confirmations,
        "durability":"memory_confirmation_not_persisted"
    })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StreamQuery {
    epoch: String,
    after_cursor: u64,
    #[serde(default)]
    after_confirmation_sequence: Option<u64>,
    #[serde(default)]
    wire_version: Option<u8>,
}

#[derive(Clone)]
pub struct Api {
    reader: MemoryReader,
    control: Control,
    limits: Limits,
    snapshots: Arc<Semaphore>,
    streams: Arc<Semaphore>,
    timeline: Arc<Mutex<TimelineArchive>>,
}
#[derive(Clone)]
struct Active {
    profile: Arc<Profile>,
    epoch: String,
}
#[derive(Clone)]
pub struct Control {
    reader: MemoryReader,
    process_instance_id: String,
    active: Arc<RwLock<Active>>,
    changed: watch::Sender<u64>,
    manager: Arc<RwLock<Option<Value>>>,
}
impl Control {
    fn active(&self) -> Result<Active> {
        Ok(self
            .active
            .read()
            .map_err(|_| anyhow::anyhow!("BTC 5m profile poisoned"))?
            .clone())
    }
    fn active_with_watch(&self) -> Result<(Active, watch::Receiver<u64>)> {
        loop {
            let receiver = self.changed.subscribe();
            let active = self.active()?;
            if *receiver.borrow() == active.profile.profile_revision {
                return Ok((active, receiver));
            }
        }
    }
    pub fn status(&self) -> Result<Value> {
        let active = self.active()?;
        Ok(json!({"profile_id":active.profile.profile_id,
            "profile_revision":active.profile.profile_revision,
            "profile_document_sha256":active.profile.document_sha256(),
            "profile":active.profile,"epoch":active.epoch,
            "manager":self.manager_status()?}))
    }
    pub fn set_manager_status(&self, status: Value) -> Result<()> {
        *self
            .manager
            .write()
            .map_err(|_| anyhow::anyhow!("BTC 5m manager status poisoned"))? = Some(status);
        Ok(())
    }
    /// A successful lease renewal is an actual manager update. Touch the
    /// existing status under the same lock so a concurrent fail-closed phase
    /// cannot be overwritten by an older cloned "running" status.
    pub fn note_manager_renewal_at(&self, at: DateTime<Utc>) -> Result<()> {
        let mut manager = self.manager.write()
            .map_err(|_| anyhow::anyhow!("BTC 5m manager status poisoned"))?;
        let status = manager.as_mut().context("BTC 5m manager status missing")?;
        ensure!(status["schema_version"] == "marketcow.polymarket.btc-5m-manager-status.v1",
            "BTC 5m manager status schema");
        status["updated_at"] = json!(at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
        Ok(())
    }
    pub fn manager_status(&self) -> Result<Value> {
        Ok(self
            .manager
            .read()
            .map_err(|_| anyhow::anyhow!("BTC 5m manager status poisoned"))?
            .clone()
            .unwrap_or(Value::Null))
    }
    pub fn referenced_markets(&self) -> Result<BTreeSet<String>> {
        Ok(self
            .active()?
            .profile
            .markets
            .iter()
            .map(|market| market.market_id.clone())
            .collect())
    }
    pub fn profile(&self) -> Result<Profile> {
        Ok((*self.active()?.profile).clone())
    }
    /// Publish only after the caller has prepared acquisition for every
    /// referenced market through the existing managed control plane.
    pub fn publish(
        &self,
        expected_profile_id: &str,
        expected_revision: u64,
        profile: Profile,
    ) -> Result<Value> {
        self.publish_inner(expected_profile_id, expected_revision, profile, true)
    }
    /// Re-establish a fresh profile after process restart. Acquisition leases
    /// are deliberately process-local, so the caller must first prove there is
    /// no incumbent manager lease and acquire the candidate's exact component
    /// subset. The acquisition owner derives the deduplicated shared union
    /// across this and every other consumer lease.
    pub fn publish_restart_reconciled(
        &self,
        expected_profile_id: &str,
        expected_revision: u64,
        profile: Profile,
    ) -> Result<Value> {
        self.publish_inner(expected_profile_id, expected_revision, profile, false)
    }
    fn publish_inner(
        &self,
        expected_profile_id: &str,
        expected_revision: u64,
        profile: Profile,
        require_adjacent_rotation: bool,
    ) -> Result<Value> {
        profile.validate()?;
        let installed = self.reader.installed_markets()?;
        for market in &profile.markets {
            let record = installed
                .get(&market.market_id)
                .context("BTC 5m market is not acquisition-prepared")?;
            ensure!(
                record["identity"]["condition_id"] == market.condition_id,
                "BTC 5m prepared condition differs"
            );
            let outcomes = record["identity"]["outcomes"]
                .as_array()
                .context("BTC 5m prepared outcomes")?;
            ensure!(
                outcomes.len() == 2
                    && outcomes[0]["outcome"] == "Up"
                    && outcomes[0]["token_id"] == market.outcomes[0].token_id
                    && outcomes[1]["outcome"] == "Down"
                    && outcomes[1]["token_id"] == market.outcomes[1].token_id,
                "BTC 5m prepared ordered outcomes differ"
            );
        }
        self.commit_profile_at(expected_profile_id,expected_revision,profile,
            require_adjacent_rotation,Utc::now())
    }
    /// Final in-process CAS. The real route supplies Utc::now() only after
    /// prepared market identity checks; a fixed clock lets the same mutation
    /// boundary be exercised over many deterministic UTC rotations offline.
    fn commit_profile_at(
        &self,
        expected_profile_id: &str,
        expected_revision: u64,
        profile: Profile,
        require_adjacent_rotation: bool,
        now: DateTime<Utc>,
    ) -> Result<Value> {
        let mut active = self
            .active
            .write()
            .map_err(|_| anyhow::anyhow!("BTC 5m profile poisoned"))?;
        ensure!(
            active.profile.profile_id == expected_profile_id
                && active.profile.profile_revision == expected_revision,
            "BTC 5m profile incumbent conflict"
        );
        ensure!(
            profile.profile_revision
                == expected_revision
                    .checked_add(1)
                    .context("revision overflow")?,
            "BTC 5m profile revision must advance once"
        );
        // This is the final mutation boundary, after acquisition work and
        // prepared-identity verification. It closes the remaining clock
        // race even if the outer control request crossed a UTC boundary.
        profile.validate_current_at(now)?;
        // Ordinary publication is only a steady-state, same-shape rotation.
        // A shape change would bypass next→current identity and the exact
        // one-market lease replacement; startup reconstitution has its own
        // explicitly reviewed publish_restart_reconciled entrypoint.
        if require_adjacent_rotation {
            profile.validate_rotation_from(&active.profile)?;
        }
        let previous = active.profile.profile_id.clone();
        let epoch = uuid::Uuid::new_v4().simple().to_string();
        *active = Active {
            profile: Arc::new(profile),
            epoch: epoch.clone(),
        };
        self.changed.send_replace(active.profile.profile_revision);
        Ok(
            json!({"previous_profile_id":previous,"profile_id":active.profile.profile_id,
            "profile_revision":active.profile.profile_revision,"epoch":epoch,
            "requires_new_full_sync":true}),
        )
    }
}
impl Api {
    pub fn router(
        reader: MemoryReader,
        profile: Profile,
        epoch: String,
        limits: Limits,
    ) -> Result<(Router, Control)> {
        profile.validate()?;
        limits.validate()?;
        ensure!(!epoch.is_empty(), "BTC 5m epoch");
        let (changed, _) = watch::channel(profile.profile_revision);
        let control = Control {
            reader: reader.clone(),
            process_instance_id: uuid::Uuid::new_v4().simple().to_string(),
            active: Arc::new(RwLock::new(Active {
                profile: Arc::new(profile),
                epoch,
            })),
            changed,
            manager: Arc::new(RwLock::new(None)),
        };
        let state = Self {
            reader,
            control: control.clone(),
            snapshots: Arc::new(Semaphore::new(limits.snapshot_lease_capacity)),
            streams: Arc::new(Semaphore::new(limits.clients)),
            timeline: Arc::new(Mutex::new(TimelineArchive::default())),
            limits,
        };
        Ok((
            Router::new()
                .route(
                    "/v1/prediction-markets/polymarket/research/btc-5m/status",
                    get(http_status),
                )
                .route(
                    "/v1/prediction-markets/polymarket/research/btc-5m/management-health",
                    get(http_management_health),
                )
                .route(
                    "/v1/prediction-markets/polymarket/research/btc-5m/full-sync",
                    get(http_full_sync),
                )
                .route(
                    "/v1/prediction-markets/polymarket/research/btc-5m/stream",
                    get(http_stream),
                )
                .route(
                    "/v1/prediction-markets/polymarket/research/btc-5m/timeline",
                    get(http_timeline),
                )
                .with_state(state),
            control,
        ))
    }
}
fn failure(status: StatusCode, code: &str, retryable: bool) -> Response {
    (
        status,
        Json(json!({
    "schema_version":ERROR_SCHEMA,"code":code,"retryable":retryable})),
    )
        .into_response()
}
async fn http_status(State(api): State<Api>) -> Response {
    let Ok(active) = api.control.active() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    let Ok(view) = api.reader.capture() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    match status(&view, &active.profile, &active.epoch, Utc::now()) {
        Ok(mut v) => {
            v["manager"] = api.control.manager_status().unwrap_or(Value::Null);
            Json(v).into_response()
        }
        Err(_) => failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true),
    }
}
/// Management visibility must not depend on a usable market-data capture.
/// HTTP 200 means the control plane answered, never that trading is ready.
fn management_health(
    process_instance_id: &str,
    active: Option<&Active>,
    manager: &Value,
    publication: Value,
    now: DateTime<Utc>,
) -> Value {
    let manager_updated_at = manager["updated_at"].as_str()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc));
    // This deadline belongs to the manager's last actual update, not this
    // HTTP read. Polling management health cannot extend data freshness.
    let manager_valid_until = manager_updated_at.map(|at| at + chrono::Duration::seconds(MANAGER_STALE_AFTER_SECONDS));
    let manager_stale = manager_valid_until.is_none_or(|until| now >= until);
    let source_ready = publication["source_ready"] == true;
    let persistence_failed = publication["persistence_state"] == "failed";
    let manager_blocked_code = ["capacity_exhausted", "lease_invalid", "recovery_in_progress"]
        .into_iter().find(|code| manager["blocked_reason_code"] == *code);
    let current_expired = active.is_some_and(|value| value.profile.markets.iter()
        .filter(|market| market.role == WindowRole::Current)
        .any(|market| DateTime::parse_from_rfc3339(&market.end_at)
            .is_ok_and(|end| now >= end.with_timezone(&Utc))));
    let mut blocked_reasons = Vec::new();
    if active.is_none() { blocked_reasons.push("profile_missing"); }
    if persistence_failed { blocked_reasons.push("persistence_failed"); }
    if manager.is_null() { blocked_reasons.push("manager_missing"); }
    else if manager_stale { blocked_reasons.push("manager_stale"); }
    if current_expired { blocked_reasons.push("current_window_expired"); }
    if !source_ready { blocked_reasons.push("source_not_ready"); }
    if let Some(code) = manager_blocked_code { blocked_reasons.push(code); }
    if matches!(manager["phase"].as_str(), Some("reconciling" | "discovering")) {
        blocked_reasons.push("manager_recovering");
    }
    if manager["phase"] == "fail_closed" { blocked_reasons.push("manager_failed"); }
    let windows = active.map(|value| value.profile.markets.iter()
        .filter(|market| matches!(market.role, WindowRole::Current | WindowRole::Next))
        .map(|market| {
            let expired = DateTime::parse_from_rfc3339(&market.end_at)
                .is_ok_and(|end| now >= end.with_timezone(&Utc));
            let reason = if expired { "window_expired" }
                else if persistence_failed { "persistence_failed" }
                else if manager.is_null() { "manager_missing" }
                else if manager_stale { "manager_stale" }
                else if !source_ready { "source_not_ready" }
                else if let Some(code) = manager_blocked_code { code }
                else if manager["phase"] == "fail_closed" { "manager_failed" }
                else if matches!(manager["phase"].as_str(), Some("reconciling" | "discovering")) { "manager_recovering" }
                else { "capture_required" };
            json!({"role":market.role,"window_id":market.window_id,
                "market_id":market.market_id,"start_at":market.start_at,
                "end_at":market.end_at,"profile_revision":value.profile.profile_revision,
                "ready":if reason == "capture_required" {Value::Null} else {json!(false)},
                "readiness_reason":reason})
        }).collect::<Vec<_>>()).unwrap_or_default();
    json!({
        "schema_version":"marketcow.btc-5m-management-health.v1",
        "as_of":now,
        "manager_updated_at":manager_updated_at,
        "valid_until":manager_valid_until,
        "manager_stale_after_seconds":MANAGER_STALE_AFTER_SECONDS,
        "process_state":"running",
        "data_state":if blocked_reasons.is_empty() {"capture_required"} else {"unavailable"},
        "entry_ready":Value::Null,
        "readiness_requires_status_and_full_sync":true,
        "generation":{"process_instance_id":process_instance_id,
            "profile_epoch":active.map(|value|value.epoch.as_str()),
            "profile_revision":active.map(|value|value.profile.profile_revision)},
        "profile_id":active.map(|value|value.profile.profile_id.as_str()),
        "profile_document_sha256":active.map(|value|value.profile.document_sha256()),
        "manager_phase":manager["phase"],
        "windows":windows,
        "blocked_reason":blocked_reasons.first(),
        "blocked_reasons":blocked_reasons,
        "resources":{"metadata_market_count":publication["metadata_market_count"],
            "hot_binding_tokens":{"used":publication["hot_binding_tokens"],
                "limit":publication["hot_token_limit"]},
            "wire_tokens":{"used":publication["wire_tokens"],
                "limit":publication["hot_token_limit"]},
            "wire_sockets":{"used":publication["wire_sockets"],
                "limit":publication["wire_socket_limit"]},
            "component_token_limit":active.map(|value|value.profile.maximum_tokens),
            "prepared_tokens":publication["prepared_tokens"],
            "active_tokens":publication["active_tokens"],
            "draining_tokens":publication["draining_tokens"],
            "phase_counts_authoritative":publication["phase_counts_authoritative"] == true
                && !manager_stale,
            "hot_binding_count_semantics":"unique union of publication-bound and wire-owned token ids, including draining; cold metadata excluded",
            "wire_token_count_semantics":"unique token ids in active or retiring source shard jobs",
            "shared_token_count_semantics":"deduplicated token-id union across live component leases",
            "phase_count_semantics":"mutually exclusive process-local binding phases; null on missing or inconsistent owner metrics; not a durable transaction ledger"},
        "publication":publication,
    })
}
async fn http_management_health(State(api): State<Api>) -> Response {
    let publication = match api.reader.management_status() {
        Ok(value) => value,
        Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_management_unavailable", true),
    };
    let profile = api.control.active().ok();
    let manager = api.control.manager_status().unwrap_or(Value::Null);
    Json(management_health(&api.control.process_instance_id,
        profile.as_ref(), &manager, publication, Utc::now())).into_response()
}
async fn http_full_sync(State(api): State<Api>) -> Response {
    let Ok(permit) = api.snapshots.clone().try_acquire_owned() else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "btc_5m_snapshot_capacity",
            true,
        );
    };
    let Ok(active) = api.control.active() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    let result = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
        let _permit = permit;
        let view = api
            .reader
            .capture_for_resume(api.limits.send_timeout, api.limits.snapshot_lease_capacity)?;
        let mut value = full_sync(&view, &active.profile, &active.epoch, Utc::now())?;
        value["manager"] = api.control.manager_status()?;
        Ok(encode_bounded(&value, api.limits.snapshot_bytes)?)
    })
    .await;
    match result {
        Ok(Ok(v)) => ([(axum::http::header::CONTENT_TYPE, "application/json")], v).into_response(),
        Ok(Err(e))
            if e.downcast_ref::<crate::source_public_api::EncodeError>()
                .is_some() =>
        {
            failure(
                StatusCode::PAYLOAD_TOO_LARGE,
                "btc_5m_snapshot_too_large",
                false,
            )
        }
        _ => failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true),
    }
}
#[derive(Deserialize)]
struct TimelineQuery {
    epoch: String,
    publisher_cursor: u64,
    event_id: String,
}
async fn http_timeline(State(api): State<Api>, Query(q): Query<TimelineQuery>) -> Response {
    let Ok(_permit) = api.snapshots.clone().try_acquire_owned() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_snapshot_capacity", true);
    };
    let Ok(active) = api.control.active() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    if q.publisher_cursor == 0 || !hex64(&q.event_id) {
        return failure(StatusCode::BAD_REQUEST, "btc_5m_timeline_identity", false);
    }
    let replay_event = if q.epoch == active.epoch {
        api.reader.timeline_event(q.publisher_cursor)
    } else {
        Ok(None)
    };
    let publication = match replay_event {
        Ok(Some((event, timing))) if event["event_id"] == q.event_id => Some(json!({
            "market_id":event["market_id"], "token_id":event["token_id"],
            "book_epoch":event["book_epoch"], "sequence":event["sequence"],
            "publisher_cursor":event["cursor"], "event_id":event["event_id"],
            "event_received_at":event["received_at"],
            "canonical_book_received_at":event["canonical_payload"]["received_at"],
            "source_batch_timing":timing.as_ref().map(source_timing_json),
        })),
        Ok(Some(_)) => return failure(StatusCode::CONFLICT, "btc_5m_timeline_event_mismatch", false),
        Ok(None) => None,
        Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true),
    };
    let sent = match api.timeline.lock() {
        Ok(archive) => archive.lookup(&q.epoch, q.publisher_cursor, &q.event_id),
        Err(_) => return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true),
    };
    if publication.is_none() && sent.is_empty() {
        return failure(StatusCode::GONE, "btc_5m_timeline_evicted_or_unknown", false);
    }
    Json(json!({
        "schema_version":"marketcow.polymarket.btc-5m-timeline.v1",
        "epoch":q.epoch,"publisher_cursor":q.publisher_cursor,"event_id":q.event_id,
        "publication_if_still_in_replay":publication,
        "connection_sends":sent,
        "receipt_capacity":{"max_entries":TIMELINE_RECEIPT_COUNT,"max_bytes":TIMELINE_RECEIPT_BYTES},
        "clock_error_bound_ms":Value::Null,
    })).into_response()
}
async fn http_stream(
    State(api): State<Api>,
    Query(q): Query<StreamQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    let wire_version = q.wire_version.unwrap_or(1);
    if !matches!(wire_version, 1 | 2) {
        return failure(StatusCode::BAD_REQUEST, "btc_5m_wire_version", false);
    }
    let Ok((active, profile_changed)) = api.control.active_with_watch() else {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
    };
    if q.epoch != active.epoch {
        return failure(StatusCode::CONFLICT, "btc_5m_epoch_changed", false);
    }
    let current = match api.reader.capture() {
        Ok(view) => view,
        Err(_) => {
            return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
        }
    };
    if q.after_cursor > current.cursor
        || q.after_confirmation_sequence.unwrap_or(0) > current.confirmation_sequence
    {
        return failure(StatusCode::CONFLICT, "btc_5m_future_cursor", false);
    }
    match api.reader.replay(
        q.after_cursor,
        q.after_confirmation_sequence.unwrap_or(0),
        1,
        api.limits.replay_bytes,
    ) {
        Err(error) if error.to_string().contains("replay expired") => {
            return failure(StatusCode::GONE, "btc_5m_replay_expired", false);
        }
        Err(_) => {
            return failure(StatusCode::SERVICE_UNAVAILABLE, "btc_5m_unavailable", true);
        }
        Ok(_) => {}
    }
    let Ok(permit) = api.streams.clone().try_acquire_owned() else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "btc_5m_stream_capacity",
            true,
        );
    };
    let connection_id = format!(
        "{}:{}:{}",
        std::process::id(),
        active.epoch,
        NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed),
    );
    ws.max_message_size(4096)
        .max_frame_size(4096)
        .on_upgrade(move |mut socket| async move {
            let _permit = permit;
            if let Err(error) = stream(
                &mut socket,
                &api,
                active,
                profile_changed,
                q.after_cursor,
                q.after_confirmation_sequence.unwrap_or(0),
                wire_version,
                &connection_id,
            )
            .await
            {
                eprintln!("BTC 5m stream closed: {error:#}");
            }
            let _ = socket.send(Message::Close(None)).await;
        })
}
async fn send(socket: &mut WebSocket, api: &Api, value: Value) -> Result<()> {
    let body = String::from_utf8(encode_bounded(&value, api.limits.frame_bytes)?)?;
    tokio::time::timeout(
        api.limits.send_timeout,
        socket.send(Message::Text(body.into())),
    )
    .await??;
    Ok(())
}

fn monotonic_us(start: std::time::Instant, end: std::time::Instant) -> u64 {
    u64::try_from(end.saturating_duration_since(start).as_micros()).unwrap_or(u64::MAX)
}

fn source_timing_json(t: &SourceBatchTiming) -> Value {
    json!({
        "raw_batch_first_received_at":t.raw_batch_first_received_at,
        "raw_batch_last_received_at":t.raw_batch_last_received_at,
        "actor_admitted_at":t.actor_admitted_at,
        "actor_started_at":t.actor_started_at,
        "actor_completed_at":t.actor_completed_at,
        "publish_call_started_at":t.publish_call_started_at,
        "shared_replay_visible_at":t.shared_replay_visible_at,
        "durable_at_if_observed":t.durable_at,
        "actor_queue_wait_us":monotonic_us(t.actor_admitted_mono,t.actor_started_mono),
        "actor_execution_us":monotonic_us(t.actor_started_mono,t.actor_completed_mono),
        "actor_done_to_publish_call_us":monotonic_us(t.actor_completed_mono,t.publish_call_started_mono),
        "publish_call_to_replay_visible_us":t.shared_replay_visible_mono
            .map(|visible|monotonic_us(t.publish_call_started_mono,visible)),
    })
}

fn timeline_log_permitted(sampled: bool, anomalous: bool) -> bool {
    static BUDGET: OnceLock<Mutex<(std::time::Instant, u32, u32, u64)>> = OnceLock::new();
    let mut budget = BUDGET
        .get_or_init(|| Mutex::new((std::time::Instant::now(), 0, 0, 0)))
        .lock()
        .expect("BTC 5m timeline budget poisoned");
    if budget.0.elapsed() >= Duration::from_secs(60) {
        if budget.3 > 0 {
            eprintln!("{}", json!({"stage":"btc_5m_timeline_suppressed",
                "count":budget.3,"period_seconds":60}));
        }
        *budget = (std::time::Instant::now(), 0, 0, 0);
    }
    if sampled && budget.1 < 32 {
        budget.1 += 1;
        true
    } else if anomalous && budget.2 < 60 {
        budget.2 += 1;
        true
    } else {
        if sampled || anomalous {
            budget.3 = budget.3.saturating_add(1);
        }
        false
    }
}

async fn send_event(
    socket: &mut WebSocket,
    api: &Api,
    frame: Value,
    batch_timing: Option<&SourceBatchTiming>,
    connection_id: &str,
    replay_dequeued_at: DateTime<Utc>,
    replay_dequeued_mono: std::time::Instant,
) -> Result<()> {
    let event_received_at = frame["payload"]["received_at"]
        .as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc));
    let body = String::from_utf8(encode_bounded(&frame, api.limits.frame_bytes)?)?;
    let send_started_mono = std::time::Instant::now();
    let send_started_at = Utc::now();
    let result = tokio::time::timeout(
        api.limits.send_timeout,
        socket.send(Message::Text(body.into())),
    ).await;
    let send_completed_mono = std::time::Instant::now();
    let send_completed_at = Utc::now();
    let send_outcome = match &result {
        Ok(Ok(())) => "ok",
        Ok(Err(_)) => "socket_error",
        Err(_) => "timeout",
    };
    let sampled = frame["publisher_cursor"].as_u64()
        .is_some_and(crate::source_publication::btc5m_trace_sample);
    let anomalous = send_outcome != "ok"
        || event_received_at.is_some_and(|t| (send_completed_at - t).num_milliseconds() >= 1000)
        || monotonic_us(replay_dequeued_mono, send_completed_mono) >= 200_000;
    let source = batch_timing.map(|t| {
        let mut value = source_timing_json(t);
        value["replay_visible_to_ws_dequeue_us"] = json!(t.shared_replay_visible_mono
            .map(|visible|monotonic_us(visible,replay_dequeued_mono)));
        value["actor_admit_to_ws_send_complete_us"] =
            json!(monotonic_us(t.actor_admitted_mono,send_completed_mono));
        value
    });
    let receipt = json!({
            "stage":"btc_5m_event_timeline_v1",
            "trace_reason":if anomalous {"anomaly"} else if sampled {"cursor_sample"} else {"query_retained"},
            "connection_id":connection_id,
            "epoch":frame["epoch"],"market_id":frame["market_id"],
            "token_id":frame["token_id"],"publisher_cursor":frame["publisher_cursor"],
            "event_id":frame["payload"]["event_id"],
            "raw_payload_sha256":frame["payload"]["raw_payload_sha256"],
            "book_epoch":frame["payload"]["book_epoch"],
            "sequence":frame["payload"]["sequence"],
            "event_received_at":frame["payload"]["received_at"],
            "canonical_book_received_at":frame["payload"]["post_state"]["received_at"]
                .as_str().or_else(||frame["payload"]["canonical_payload"]["received_at"].as_str()),
            "source_batch_timing":source,
            "ws_queue_semantics":"shared_memory_replay_no_per_client_enqueue",
            "ws_replay_dequeued_at":replay_dequeued_at,
            "ws_send_started_at":send_started_at,
            "ws_send_completed_at":send_completed_at,
            "ws_dequeue_to_send_start_us":monotonic_us(replay_dequeued_mono,send_started_mono),
            "ws_send_call_us":monotonic_us(send_started_mono,send_completed_mono),
            "send_outcome":send_outcome,
            "source_clock_error_bound_ms":Value::Null,
            "durability":frame["durability"],
    });
    if let (Some(epoch), Some(cursor), Some(event_id)) = (
        frame["epoch"].as_str(), frame["publisher_cursor"].as_u64(),
        frame["payload"]["event_id"].as_str(),
    ) {
        api.timeline.lock().expect("BTC 5m timeline archive poisoned")
            .push(epoch, cursor, event_id, receipt.clone());
    }
    if timeline_log_permitted(sampled, anomalous) {
        eprintln!("{receipt}");
    }
    result??;
    Ok(())
}

/// The writer may be ahead of this client's replay. Never attest durability
/// for a cursor this connection has not yet sent, and never repeat/regress a
/// watermark. This frame carries no quote-freshness or new book evidence.
fn durable_watermark_target(
    sent_cursor: u64,
    writer_cursor: u64,
    last_sent_durable: u64,
) -> Option<u64> {
    let durable_cursor = writer_cursor.min(sent_cursor);
    (durable_cursor > last_sent_durable).then_some(durable_cursor)
}

fn durable_watermark_frame(
    active: &Active,
    sent_cursor: u64,
    durable_cursor: u64,
    observed_at: DateTime<Utc>,
) -> Value {
    json!({
        "schema_version":COMPACT_EVENT_SCHEMA,
        "type":"durable_watermark",
        "epoch":active.epoch,
        "profile_id":active.profile.profile_id,
        "profile_revision":active.profile.profile_revision,
        "publisher_cursor":sent_cursor,
        "durable_cursor":durable_cursor,
        "observed_at":observed_at,
        "causal_bound":"durable_cursor_not_after_sent_publisher_cursor",
    })
}

fn durable_watermark_wait(
    last_sent_at: std::time::Instant,
    now: std::time::Instant,
) -> Duration {
    DURABLE_WATERMARK_MIN_INTERVAL
        .saturating_sub(now.saturating_duration_since(last_sent_at))
}

async fn send_profile_changed(
    socket: &mut WebSocket,
    api: &Api,
    active: &Active,
    cursor: u64,
    event_schema: &str,
) -> Result<()> {
    let next = api.control.active()?;
    send(socket, api, json!({"schema_version":event_schema,"type":"profile_changed",
        "epoch":active.epoch,"publisher_cursor":cursor,
        "next_profile_id":next.profile.profile_id,
        "next_profile_revision":next.profile.profile_revision,"next_epoch":next.epoch,
        "requires_new_full_sync":true})).await
}
async fn stream(
    socket: &mut WebSocket,
    api: &Api,
    active: Active,
    mut profile_changed: watch::Receiver<u64>,
    mut cursor: u64,
    mut confirmation_cursor: u64,
    wire_version: u8,
    connection_id: &str,
) -> Result<()> {
    let event_schema = if wire_version == 2 {
        COMPACT_EVENT_SCHEMA
    } else {
        EVENT_SCHEMA
    };
    let mut changed = api.reader.subscribe();
    let mut ready_sent = false;
    let mut last_sent_durable = 0;
    let mut last_durable_sent_at = std::time::Instant::now();
    loop {
        changed.borrow_and_update();
        if *profile_changed.borrow() != active.profile.profile_revision {
            send_profile_changed(socket, api, &active, cursor, event_schema).await?;
            return Ok(());
        }
        let page = api
            .reader
            .replay(cursor, confirmation_cursor, 64, api.limits.replay_bytes)?;
        for batch in &page.batches {
            if *profile_changed.borrow() != active.profile.profile_revision {
                send_profile_changed(socket, api, &active, cursor, event_schema).await?;
                return Ok(());
            }
            let batch_timing = batch.timing_snapshot();
            for event in batch.validated.events() {
                let at = event["cursor"].as_u64().context("event cursor")?;
                if at <= cursor {
                    continue;
                }
                let replay_dequeued_mono = std::time::Instant::now();
                let replay_dequeued_at = Utc::now();
                let frame = if wire_version == 2 {
                    compact_event_frame(&active.profile, &active.epoch, event)?
                } else {
                    event_frame(&active.profile, &active.epoch, event)?
                };
                if let Some(frame) = frame {
                    send_event(socket, api, frame, batch_timing.as_ref(),
                        connection_id, replay_dequeued_at, replay_dequeued_mono).await?;
                }
                cursor = at;
            }
        }
        ensure!(cursor == page.next, "BTC 5m replay boundary");
        if page.caught_up && page.confirmation_sequence > confirmation_cursor {
            if wire_version == 2 {
                if let Some(frame) = confirmation_frame(
                    &active.profile,
                    &active.epoch,
                    cursor,
                    page.confirmation_sequence,
                    &page.confirmation_books,
                )? {
                    send(socket, api, frame).await?;
                }
            }
            confirmation_cursor = page.confirmation_sequence;
        }
        if page.caught_up && !ready_sent {
            let view = api.reader.capture()?;
            if view.cursor != cursor || view.confirmation_sequence != confirmation_cursor {
                continue;
            }
            ensure!(view.persisted_cursor <= cursor,
                "BTC 5m durable cursor exceeds ready replay boundary");
            if *profile_changed.borrow() != active.profile.profile_revision {
                send_profile_changed(socket, api, &active, cursor, event_schema).await?;
                return Ok(());
            }
            let sync = full_sync(&view, &active.profile, &active.epoch, Utc::now())?;
            send(
                socket,
                api,
                json!({"schema_version":event_schema,"type":"ready","epoch":active.epoch,
                "publisher_cursor":cursor,"durable_cursor":view.persisted_cursor,
                "confirmation_sequence":confirmation_cursor,"ready":sync["ready"],
                "barrier":"all_source_events_through_publisher_cursor_applied"}),
            )
            .await?;
            ready_sent = true;
            last_sent_durable = view.persisted_cursor;
            last_durable_sent_at = std::time::Instant::now();
        }
        // The durability worker notifies the same watch as publication after
        // fsync. A bounded timer also flushes progress withheld by the rate
        // limit when no further book or writer notification arrives.
        let pending_durable = if wire_version == 2 && ready_sent {
            durable_watermark_target(cursor, api.reader.durable_cursor()?,
                last_sent_durable)
        } else {
            None
        };
        let durable_wait = durable_watermark_wait(
            last_durable_sent_at, std::time::Instant::now());
        if durable_wait.is_zero() {
            if let Some(durable_cursor) = pending_durable {
                if *profile_changed.borrow() != active.profile.profile_revision {
                    send_profile_changed(socket, api, &active, cursor, event_schema).await?;
                    return Ok(());
                }
                send(socket, api, durable_watermark_frame(&active, cursor, durable_cursor,
                    Utc::now())).await?;
                last_sent_durable = durable_cursor;
                last_durable_sent_at = std::time::Instant::now();
                continue;
            }
        }
        if !page.caught_up {
            continue;
        }
        tokio::select! {r=changed.changed()=>r.context("publication closed")?,r=profile_changed.changed()=>{
            r.context("BTC 5m profile control closed")?;
            send_profile_changed(socket, api, &active, cursor, event_schema).await?;
            return Ok(());
        },_ = tokio::time::sleep(durable_wait), if pending_durable.is_some() => {
            // Re-read the writer watermark on the next loop: a prior value
            // may have advanced while the timer was waiting.
        },m=socket.recv()=>match m {
        None|Some(Ok(Message::Close(_)))=>return Ok(()),Some(Ok(Message::Ping(v)))=>socket.send(Message::Pong(v)).await?,
        Some(Ok(Message::Pong(_)))=>{},_=>anyhow::bail!("unexpected BTC 5m client message")}}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source_publication::Publication;
    use chrono::{SecondsFormat, TimeZone};
    use tower::ServiceExt;
    #[tokio::test]
    async fn renewal_heartbeat_preserves_fail_closed_phase_and_error() {
        let publication = Publication::start(0, None, std::collections::BTreeMap::new(),
            8, 131072, 65536, |_| Ok(())).unwrap();
        let (_, control) = Api::router(publication.reader(), profile(), "epoch".into(), Limits {
            snapshot_bytes: 65536, frame_bytes: 65536, replay_bytes: 65536,
            clients: 1, send_timeout: Duration::from_secs(1), snapshot_lease_capacity: 1,
        }).unwrap();
        let first = Utc.with_ymd_and_hms(2026, 10, 1, 22, 40, 0).unwrap();
        let renewed = first + chrono::Duration::seconds(40);
        control.set_manager_status(json!({"schema_version":
            "marketcow.polymarket.btc-5m-manager-status.v1",
            "phase":"fail_closed","error":"discovery failed",
            "updated_at":first.to_rfc3339()})).unwrap();
        control.note_manager_renewal_at(renewed).unwrap();
        let status = control.manager_status().unwrap();
        assert_eq!(status["phase"], "fail_closed");
        assert_eq!(status["error"], "discovery failed");
        assert_eq!(status["updated_at"], renewed.to_rfc3339_opts(SecondsFormat::Millis, true));
        assert!(control.note_manager_renewal_at(first).is_ok());
        control.set_manager_status(json!({"schema_version":"wrong"})).unwrap();
        assert!(control.note_manager_renewal_at(renewed).is_err());
        publication.finish().await.unwrap();
    }
    #[test]
    fn management_contract_separates_capture_from_process_and_fences_stale_manager() {
        let now = Utc.with_ymd_and_hms(2026, 9, 26, 5, 31, 0).unwrap();
        let mut two_windows = profile();
        let mut next = two_windows.markets[0].clone();
        next.role = WindowRole::Next;
        next.window_id = "2026-09-26T05:35:00Z/2026-09-26T05:40:00Z".into();
        next.start_at = "2026-09-26T05:35:00Z".into();
        next.end_at = "2026-09-26T05:40:00Z".into();
        next.market_id = "4944031".into();
        two_windows.markets.push(next);
        let active = Active { profile: Arc::new(two_windows), epoch: "profile-epoch".into() };
        let publication = json!({"source_ready":true,"persistence_state":"running",
            "metadata_market_count":2048,"publication_binding_tokens":500,"hot_binding_tokens":500,
            "wire_tokens":4,"wire_sockets":1,"phase_counts_authoritative":false});
        let manager = json!({"phase":"running","updated_at":now.to_rfc3339()});
        let healthy = management_health("process-a", Some(&active), &manager,
            publication.clone(), now);
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-management-health-v1.json"
        )).unwrap();
        assert_eq!(healthy, fixture);
        assert_eq!(healthy["schema_version"], "marketcow.btc-5m-management-health.v1");
        assert_eq!(healthy["data_state"], "capture_required");
        assert_eq!(healthy["entry_ready"], Value::Null);
        assert_eq!(healthy["generation"]["process_instance_id"], "process-a");
        assert_eq!(healthy["generation"]["profile_epoch"], "profile-epoch");
        assert_eq!(healthy["windows"][0]["market_id"], "4944030");
        assert_eq!(healthy["windows"][1]["market_id"], "4944031");
        assert_eq!(healthy["windows"][1]["profile_revision"], 1);
        assert_eq!(healthy["windows"][0]["readiness_reason"], "capture_required");
        assert_eq!(healthy["resources"]["hot_binding_tokens"]["used"], 500);
        assert_eq!(healthy["resources"]["wire_tokens"]["used"], 4);
        assert_eq!(healthy["resources"]["phase_counts_authoritative"], false);
        assert_eq!(DateTime::parse_from_rfc3339(healthy["valid_until"].as_str().unwrap()).unwrap()
            .with_timezone(&Utc), now + chrono::Duration::seconds(60));

        let mut previously_authoritative = publication.clone();
        previously_authoritative["phase_counts_authoritative"] = json!(true);
        previously_authoritative["prepared_tokens"] = json!(496);
        previously_authoritative["active_tokens"] = json!(4);
        previously_authoritative["draining_tokens"] = json!(0);
        previously_authoritative["hot_token_limit"] = json!(4096);
        previously_authoritative["wire_socket_limit"] = json!(8);
        let healthy_authoritative = management_health("process-a", Some(&active), &manager,
            previously_authoritative.clone(), now);
        let healthy_fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-management-health-v1-healthy.json"
        )).unwrap();
        assert_eq!(healthy_authoritative, healthy_fixture);
        let stale = management_health("process-a", Some(&active), &manager,
            previously_authoritative, now + chrono::Duration::seconds(61));
        let stale_fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-management-health-v1-stale.json"
        )).unwrap();
        assert_eq!(stale, stale_fixture);
        assert_eq!(stale["blocked_reasons"], json!(["manager_stale"]));
        assert_eq!(stale["resources"]["phase_counts_authoritative"], false);
        assert_eq!(stale["valid_until"], healthy["valid_until"]);
        let no_capture = management_health("process-a", Some(&active), &manager,
            json!({"source_ready":false,"persistence_state":"running"}), now);
        assert_eq!(no_capture["blocked_reasons"], json!(["source_not_ready"]));
        assert_eq!(no_capture["windows"][0]["ready"], false);

        let lease = management_health("process-a", Some(&active),
            &json!({"phase":"fail_closed","updated_at":now.to_rfc3339(),
                "blocked_reason_code":"lease_invalid"}), publication.clone(), now);
        assert_eq!(lease["blocked_reasons"], json!(["lease_invalid","manager_failed"]));
        let capacity = management_health("process-a", Some(&active),
            &json!({"phase":"fail_closed","updated_at":now.to_rfc3339(),
                "blocked_reason_code":"capacity_exhausted"}), publication.clone(), now);
        assert_eq!(capacity["blocked_reasons"], json!(["capacity_exhausted","manager_failed"]));
        let recovering = management_health("process-a", Some(&active),
            &json!({"phase":"reconciling","updated_at":now.to_rfc3339()}), publication.clone(), now);
        assert_eq!(recovering["blocked_reasons"], json!(["manager_recovering"]));
        let no_manager = management_health("process-a", Some(&active), &Value::Null,
            publication, now);
        let missing_fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-management-health-v1-manager-missing.json"
        )).unwrap();
        assert_eq!(no_manager, missing_fixture);
        assert_eq!(no_manager["blocked_reasons"], json!(["manager_missing"]));
        assert_eq!(no_manager["manager_updated_at"], Value::Null);
        assert_eq!(no_manager["valid_until"], Value::Null);
        let no_timestamp = management_health("process-a", Some(&active),
            &json!({"phase":"running"}), json!({"source_ready":true}), now);
        assert_eq!(no_timestamp["valid_until"], Value::Null);
        assert_eq!(no_timestamp["blocked_reasons"], json!(["manager_stale"]));
        let later = now + chrono::Duration::minutes(5);
        let expired = management_health("process-a", Some(&active),
            &json!({"phase":"running","updated_at":later.to_rfc3339()}),
            json!({"source_ready":true,"persistence_state":"running"}), later);
        assert_eq!(expired["blocked_reasons"], json!(["current_window_expired"]));
        assert_eq!(expired["windows"][0]["readiness_reason"], "window_expired");
        let failed_disk = management_health("process-a", Some(&active), &manager,
            json!({"source_ready":true,"persistence_state":"failed"}), now);
        assert_eq!(failed_disk["blocked_reasons"], json!(["persistence_failed"]));
        let actual = std::collections::BTreeMap::from([
            ("running_without_capture", &healthy),
            ("source_paused", &no_capture),
            ("manager_exited_and_timeout_elapsed", &stale),
            ("manager_reconciling", &recovering),
            ("capacity_error", &capacity),
            ("lease_invalid", &lease),
            ("persistence_failed", &failed_disk),
            ("current_window_expired", &expired),
            ("manager_missing", &no_manager),
        ]);
        let cases: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-management-health-v1-cases.json"
        )).unwrap();
        for case in cases["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let result = actual[name];
            assert_eq!(result["blocked_reasons"], case["blocked_reasons"], "{name}");
            assert_eq!(result["data_state"], case["data_state"], "{name}");
            assert_eq!(result["windows"][0]["readiness_reason"],
                case["current_readiness_reason"], "{name}");
        }
    }
    fn market(id: &str, condition: &str, up: &str, down: &str) -> Value {
        json!({"identity":{"market_id":id,"condition_id":condition,
        "outcomes":[{"outcome":"Up","token_id":up},{"outcome":"Down","token_id":down}]},"active":true,"closed":false,"relations":[],
        "lifecycle_state":"active","rules":{"rules_complete":true,"instrument":{"complete":true,"price_increment":"0.01","revision":"i","provenance":[]},
        "fee_schedule":{"complete":true,"schedule_id":"f"}}})
    }
    fn book(token: &str, condition: &str, at: &str) -> Value {
        json!({"token_id":token,"condition_id":condition,"received_at":at,
        "exchange_at":at,"state_checksum":"b".repeat(64),"book_epoch":"e","bids":[{"price":"0.40","size":"10"}],
        "asks":[{"price":"0.60","size":"10"}],"last_trade_price":"0.50"})
    }
    fn profile() -> Profile {
        let mut p = Profile {
            schema_version: PROFILE_SCHEMA.into(),
            profile_id: String::new(),
            profile_revision: 1,
            catalog_revision: "a".repeat(64),
            maximum_markets: 3,
            maximum_quote_age_ms: 5_000,
            maximum_tokens: 6,
            markets: vec![MarketBinding {
                role: WindowRole::Current,
                window_id: "2026-09-26T05:30:00Z/2026-09-26T05:35:00Z".into(),
                start_at: "2026-09-26T05:30:00Z".into(),
                end_at: "2026-09-26T05:35:00Z".into(),
                market_id: "4944030".into(),
                condition_id: format!("0x{}", "1".repeat(64)),
                outcomes: [
                    OutcomeBinding {
                        outcome: "Up".into(),
                        token_id: "10".into(),
                    },
                    OutcomeBinding {
                        outcome: "Down".into(),
                        token_id: "20".into(),
                    },
                ],
                rule_sha256: "c".repeat(64),
            }],
        };
        p.profile_id = p.expected_id();
        p
    }
    #[test]
    fn durable_watermark_is_monotonic_causally_capped_and_rate_limited() {
        let existing: Value = serde_json::from_slice(include_bytes!(
            "../../../tests/fixtures/btc-5m-wire-v2.json"
        )).unwrap();
        let active = Active { profile: Arc::new(serde_json::from_value(
            existing["profile"].clone()).unwrap()),
            epoch: "11111111111111111111111111111111".into() };
        // Writer progress without another book must still be deliverable.
        let first = durable_watermark_target(100, 80, 50).unwrap();
        assert_eq!(first, 80);
        let observed_at = Utc.with_ymd_and_hms(2026, 10, 3, 0, 0, 0).unwrap();
        let frame = durable_watermark_frame(&active, 100, first, observed_at);
        assert_eq!(frame["schema_version"], COMPACT_EVENT_SCHEMA);
        assert_eq!(frame["type"], "durable_watermark");
        assert_eq!(frame["epoch"], "11111111111111111111111111111111");
        assert_eq!(frame["profile_id"], active.profile.profile_id);
        assert_eq!(frame["profile_revision"], 1);
        assert_eq!(frame["publisher_cursor"], 100);
        assert_eq!(frame["durable_cursor"], 80);
        assert!(frame.get("book_received_at").is_none());
        assert!(serde_json::to_vec(&frame).unwrap().len() < 1024);
        let fixture: Value = serde_json::from_slice(include_bytes!(
            "../../../tests/fixtures/btc-5m-durable-watermark-v2.json"
        )).unwrap();
        assert_eq!(frame, fixture);

        // During segmented replay the writer can be much farther ahead;
        // this connection attests only the part it has already emitted.
        assert_eq!(durable_watermark_target(110, 500, 80), Some(110));
        assert_eq!(durable_watermark_target(130, 120, 110), Some(120));
        assert_eq!(durable_watermark_target(130, 119, 120), None);
        assert_eq!(durable_watermark_target(130, 120, 120), None);
        let at = std::time::Instant::now();
        assert_eq!(durable_watermark_wait(at, at + Duration::from_millis(100)),
            Duration::from_millis(150));
        assert!(durable_watermark_wait(at, at + Duration::from_millis(250)).is_zero());
    }
    #[tokio::test]
    async fn v2_stream_reports_later_fsync_without_another_book_event() {
        use futures_util::StreamExt;
        let fixture: Value = serde_json::from_slice(include_bytes!(
            "../../../tests/fixtures/btc-5m-wire-v2.json"
        )).unwrap();
        let profile: Profile = serde_json::from_value(fixture["profile"].clone()).unwrap();
        let markets = profile.markets.iter().map(|binding| market(
            &binding.market_id, &binding.condition_id,
            &binding.outcomes[0].token_id, &binding.outcomes[1].token_id,
        )).collect::<Vec<_>>();
        let tokens = profile.markets.iter().flat_map(|binding| binding.outcomes.iter()
            .map(|outcome| (outcome.token_id.clone(), binding.market_id.clone())))
            .collect();
        let (persist_entered, entered) = std::sync::mpsc::channel();
        let (release, persist_release) = std::sync::mpsc::channel();
        let mut publication = Publication::start(42,
            Some(json!({"latest_cursor":42,"catalog_revision":profile.catalog_revision,
                "live_stream_connected":true,"gaps":[],"books":[],"markets":markets})),
            tokens, 8, 1024 * 1024, 512 * 1024,
            move |_| {
                persist_entered.send(()).unwrap();
                persist_release.recv_timeout(Duration::from_secs(5))?;
                Ok(())
            }).unwrap();
        let (router, control) = Api::router(publication.reader(), profile,
            "epoch-durable".into(), Limits {
                snapshot_bytes: 1024 * 1024, frame_bytes: 1024 * 1024,
                replay_bytes: 512 * 1024, clients: 1,
                send_timeout: Duration::from_secs(1), snapshot_lease_capacity: 1,
            }).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (mut client, _) = tokio_tungstenite::connect_async(format!(
            "ws://{address}/v1/prediction-markets/polymarket/research/btc-5m/stream?epoch=epoch-durable&after_cursor=42&wire_version=2"
        )).await.unwrap();
        let next = |message: tokio_tungstenite::tungstenite::Message| -> Value {
            serde_json::from_str(message.to_text().unwrap()).unwrap()
        };
        let ready = next(tokio::time::timeout(Duration::from_secs(2), client.next())
            .await.unwrap().unwrap().unwrap());
        assert_eq!(ready["type"], "ready");
        assert_eq!(ready["publisher_cursor"], 42);
        assert_eq!(ready["durable_cursor"], 42);

        publication.publish(vec![fixture["source_events"][0].clone()], None).unwrap();
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        let book = next(tokio::time::timeout(Duration::from_secs(2), client.next())
            .await.unwrap().unwrap().unwrap());
        assert_eq!(book["type"], "book_snapshot");
        assert_eq!(book["publisher_cursor"], 43);
        assert!(tokio::time::timeout(Duration::from_millis(100), client.next()).await.is_err(),
            "must not attest cursor 43 before fsync");
        release.send(()).unwrap();
        let durable = next(tokio::time::timeout(Duration::from_secs(2), client.next())
            .await.unwrap().unwrap().unwrap());
        assert_eq!(durable["type"], "durable_watermark");
        assert_eq!(durable["epoch"], "epoch-durable");
        assert_eq!(durable["publisher_cursor"], 43);
        assert_eq!(durable["durable_cursor"], 43);
        assert!(tokio::time::timeout(Duration::from_millis(350), client.next()).await.is_err(),
            "unchanged watermark must not be repeated");
        let incumbent = control.profile().unwrap();
        let mut replacement = incumbent.clone();
        replacement.profile_revision += 1;
        replacement.profile_id = replacement.expected_id();
        control.commit_profile_at(&incumbent.profile_id, incumbent.profile_revision,
            replacement.clone(), false,
            Utc.with_ymd_and_hms(2026, 9, 26, 5, 30, 30).unwrap()).unwrap();
        let changed = next(tokio::time::timeout(Duration::from_secs(2), client.next())
            .await.unwrap().unwrap().unwrap());
        assert_eq!(changed["type"], "profile_changed");
        assert_eq!(changed["epoch"], "epoch-durable");
        assert_eq!(changed["next_profile_revision"], 2);
        assert_eq!(changed["next_profile_id"], replacement.profile_id);
        assert_eq!(changed["requires_new_full_sync"], true);
        drop(client);
        server.abort();
        publication.finish().await.unwrap();
    }
    #[test]
    fn timeline_archive_retains_exact_recent_cursor_and_evicts_oldest() {
        let mut archive = TimelineArchive::default();
        for cursor in 1..=TIMELINE_RECEIPT_COUNT as u64 + 1 {
            archive.push("epoch-a", cursor, "event-a", json!({
                "connection_id":"client-a","publisher_cursor":cursor,"send_outcome":"ok"
            }));
        }
        assert!(archive.lookup("epoch-a", 1, "event-a").is_empty());
        assert_eq!(archive.lookup("epoch-a", 2, "event-a").len(), 1);
        assert!(archive.lookup("epoch-b", 2, "event-a").is_empty());
        assert!(archive.lookup("epoch-a", 2, "event-b").is_empty());
        let newest = TIMELINE_RECEIPT_COUNT as u64 + 1;
        archive.push("epoch-a", newest, "event-a", json!({"connection_id":"client-b"}));
        let receipts = archive.lookup("epoch-a", newest, "event-a");
        assert_eq!(receipts.len(), 2);
        assert_eq!(receipts[0]["connection_id"], "client-a");
        assert_eq!(receipts[1]["connection_id"], "client-b");
        assert!(archive.bytes <= TIMELINE_RECEIPT_BYTES);
    }
    #[test]
    fn shared_wire_fixture_binds_profile_and_declared_budgets() {
        let fixture: Value = serde_json::from_slice(include_bytes!(
            "../../../tests/fixtures/btc-5m-wire-v1.json"
        ))
        .unwrap();
        let profile: Profile = serde_json::from_value(fixture["profile"].clone()).unwrap();
        profile.validate().unwrap();
        assert_eq!(fixture["limits"]["snapshot_bytes"], 8 * 1024 * 1024);
        assert_eq!(fixture["limits"]["frame_bytes"], 1024 * 1024);
        assert_eq!(fixture["limits"]["replay_bytes"], 4 * 1024 * 1024);
        assert_eq!(fixture["events"][0]["type"], "book_snapshot");
        assert_eq!(fixture["events"][1]["type"], "book_snapshot");
        assert_eq!(fixture["events"][2]["type"], "book_change");
        assert_eq!(fixture["events"][3]["type"], "last_trade");
        assert_eq!(fixture["events"][4]["type"], "recovery_started");
        assert_eq!(fixture["events"][5]["type"], "book_snapshot");
        assert_eq!(fixture["events"][6]["type"], "recovery_completed");
        assert_eq!(fixture["events"][7]["type"], "ready");
        assert_eq!(fixture["events"][8]["type"], "profile_changed");
        assert_eq!(
            fixture["status"]["profile_document_sha256"],
            profile.document_sha256()
        );
        assert_eq!(fixture["status"]["profile"], fixture["profile"]);
        assert_eq!(fixture["full_sync"]["after_cursor_semantics"], "exclusive");
        assert_eq!(
            fixture["full_sync"]["markets"][0]["tokens"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let schema: Value = serde_json::from_slice(include_bytes!(
            "../../../tests/fixtures/btc-5m-wire-v1.schema.json"
        ))
        .unwrap();
        assert_eq!(schema["$id"], "marketcow.polymarket.btc-5m-wire.v1");
    }
    #[tokio::test]
    async fn shared_event_samples_are_output_of_the_actual_serializer() {
        let fixture: Value = serde_json::from_slice(include_bytes!(
            "../../../tests/fixtures/btc-5m-wire-v1.json"
        ))
        .unwrap();
        let profile: Profile = serde_json::from_value(fixture["profile"].clone()).unwrap();
        let mut source = crate::source_websocket::btc5m_reducer_fixture_sequence().unwrap();
        for event in &mut source {
            if event
                .as_object_mut()
                .unwrap()
                .remove("_replay_complete_delta")
                .is_some()
            {
                event.as_object_mut().unwrap().remove("event_id");
                event["event_id"] = json!(canonical_hash(event));
            }
        }
        let projected = source
            .iter()
            .map(|event| {
                event_frame(&profile, "epoch-example", event)
                    .unwrap()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let markets = profile
            .markets
            .iter()
            .map(|binding| {
                market(
                    &binding.market_id,
                    &binding.condition_id,
                    &binding.outcomes[0].token_id,
                    &binding.outcomes[1].token_id,
                )
            })
            .collect::<Vec<_>>();
        let tokens = profile
            .markets
            .iter()
            .flat_map(|binding| {
                binding
                    .outcomes
                    .iter()
                    .map(|outcome| (outcome.token_id.clone(), binding.market_id.clone()))
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut publication = Publication::start(
            42,
            Some(
                json!({"latest_cursor":42,"catalog_revision":profile.catalog_revision,
                "live_stream_connected":true,"gaps":[],"books":[],"markets":markets}),
            ),
            tokens,
            8,
            1024 * 1024,
            512 * 1024,
            |_| Ok(()),
        )
        .unwrap();
        for event in &source {
            publication.publish(vec![event.clone()], None).unwrap();
        }
        for _ in 0..100 {
            if publication.persisted_cursor() == 49 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(publication.persisted_cursor(), 49);
        let now = DateTime::parse_from_rfc3339("2026-09-26T05:30:09Z")
            .unwrap()
            .with_timezone(&Utc);
        let view = publication.capture_view().unwrap();
        let generated_full_sync = full_sync(&view, &profile, "epoch-example", now).unwrap();
        let generated_status = status(&view, &profile, "epoch-example", now).unwrap();
        if let Ok(path) = std::env::var("MARKETCOW_BTC5M_FIXTURE_OUTPUT") {
            std::fs::write(
                path,
                serde_json::to_vec_pretty(&json!({"source_events":source,"events":projected,
                    "full_sync":generated_full_sync,"status":generated_status}))
                .unwrap(),
            )
            .unwrap();
            publication.finish().await.unwrap();
            return;
        }
        assert_eq!(fixture["source_events"], Value::Array(source.clone()));
        assert_eq!(
            fixture["events"].as_array().unwrap()[..projected.len()],
            projected
        );
        for event in &source {
            assert_eq!(
                event["canonical_payload_sha256"],
                canonical_hash(&event["canonical_payload"])
            );
            assert_eq!(
                event["raw_payload_sha256"],
                canonical_hash(&event["raw_payload"])
            );
            let mut identity = event.clone();
            identity.as_object_mut().unwrap().remove("event_id");
            assert_eq!(event["event_id"], canonical_hash(&identity));
            if event["event_type"] == "book" {
                let book = &event["canonical_payload"];
                assert_eq!(book["sequence_semantics"], "deterministic_normalized");
                assert_eq!(
                    book["state_checksum"],
                    canonical_hash(&json!({"token_id":book["token_id"],
                        "tick_size":book["tick_size"],"bids":book["bids"],"asks":book["asks"]}))
                );
            }
        }
        let recovery_book = &source[source.len() - 2];
        let completion = &source[source.len() - 1];
        assert_eq!(
            completion["canonical_payload"]["snapshot_cursor"],
            recovery_book["cursor"]
        );
        assert_eq!(
            fixture["full_sync"]["markets"][0]["tokens"][0]["state_checksum"],
            recovery_book["canonical_payload"]["state_checksum"]
        );
        assert_eq!(
            fixture["full_sync"]["markets"][0]["tokens"][0]["tick_size"],
            recovery_book["canonical_payload"]["tick_size"]
        );
        assert_eq!(fixture["full_sync"], generated_full_sync);
        assert_eq!(fixture["status"], generated_status);
        for market in fixture["full_sync"]["markets"].as_array().unwrap() {
            for token in market["tokens"].as_array().unwrap() {
                if let Some(book) = token["canonical_book"].as_object() {
                    assert_eq!(
                        token["canonical_book_sha256"],
                        canonical_hash(&Value::Object(book.clone()))
                    );
                } else {
                    assert!(token["canonical_book_sha256"].is_null());
                }
            }
        }
        publication.finish().await.unwrap();
    }

    #[test]
    fn compact_wire_keeps_snapshots_complete_and_commits_each_delta_post_state() {
        let fixture: Value = serde_json::from_slice(include_bytes!(
            "../../../tests/fixtures/btc-5m-wire-v1.json"
        ))
        .unwrap();
        let profile: Profile = serde_json::from_value(fixture["profile"].clone()).unwrap();
        let source = crate::source_websocket::compact_btc5m_fixture_events(
            crate::source_websocket::btc5m_reducer_fixture_sequence().unwrap(),
        )
        .unwrap();
        let mut projected = source
            .iter()
            .filter_map(|event| compact_event_frame(&profile, "epoch-example", event).unwrap())
            .collect::<Vec<_>>();
        let mut confirmed_book = source[source.len() - 2]["canonical_payload"].clone();
        confirmed_book["confirmed_at"] = json!("2026-09-26T05:30:08.500Z");
        confirmed_book["confirmation_source"] = json!("rest_book_confirmation");
        confirmed_book["confirmation_evidence_sha256"] = json!("e".repeat(64));
        confirmed_book["book_received_at"] = confirmed_book["received_at"].clone();
        confirmed_book
            .as_object_mut()
            .unwrap()
            .remove("received_at");
        projected.push(
            confirmation_frame(
                &profile,
                "epoch-example",
                49,
                1,
                &[Arc::new(confirmed_book)],
            )
            .unwrap()
            .unwrap(),
        );
        if let Ok(path) = std::env::var("MARKETCOW_BTC5M_V2_FIXTURE_OUTPUT") {
            std::fs::write(
                path,
                serde_json::to_vec_pretty(&json!({
                    "schema_version":"marketcow.polymarket.btc-5m-wire-fixture.v2",
                    "profile":profile,
                    "source_events":source,
                    "events":projected,
                    "replay_contract":{
                        "initial_state":"full_sync.canonical_book",
                        "after_cursor":"exclusive",
                        "price_change":"apply ordered payload.delta.changes then verify both post_state hashes",
                        "last_trade":"does not refresh canonical book received_at or quote freshness",
                        "hash_mismatch":"invalidate token and obtain a new full-sync"
                    }
                }))
                .unwrap(),
            )
            .unwrap();
            return;
        }
        let compact_fixture: Value = serde_json::from_slice(include_bytes!(
            "../../../tests/fixtures/btc-5m-wire-v2.json"
        ))
        .unwrap();
        assert_eq!(
            compact_fixture["profile"],
            serde_json::to_value(&profile).unwrap()
        );
        assert_eq!(
            compact_fixture["source_events"],
            Value::Array(source.clone())
        );
        assert_eq!(compact_fixture["events"], Value::Array(projected));
        let snapshot = compact_event_frame(&profile, "epoch-example", &source[0])
            .unwrap()
            .unwrap();
        assert_eq!(snapshot["type"], "book_snapshot");
        assert!(snapshot["payload"]["canonical_payload"]["bids"].is_array());

        let mut tick_change_source = source[0].clone();
        tick_change_source["raw_payload"]["event_type"] = json!("tick_size_change");
        let tick_change = compact_event_frame(&profile, "epoch-example", &tick_change_source)
            .unwrap()
            .unwrap();
        assert_eq!(tick_change["type"], "book_snapshot");
        assert_eq!(tick_change["source_event_type"], "tick_size_change");
        assert_eq!(tick_change["payload_semantics"], "complete_post_state");
        assert!(tick_change["payload"]["canonical_payload"]["bids"].is_array());

        let change_source = source
            .iter()
            .find(|event| event["raw_payload"]["event_type"] == "price_change")
            .unwrap();
        let compact = compact_event_frame(&profile, "epoch-example", change_source)
            .unwrap()
            .unwrap();
        assert_eq!(compact["schema_version"], COMPACT_EVENT_SCHEMA);
        assert_eq!(compact["type"], "book_change");
        assert!(compact["payload"].get("canonical_payload").is_none());
        assert_eq!(
            compact["payload"]["post_state"]["canonical_book_sha256"],
            change_source["canonical_payload"]["canonical_book_sha256"]
        );
        assert_eq!(
            compact["payload"]["post_state"]["state_checksum"],
            change_source["canonical_payload"]["state_checksum"]
        );
        assert!(
            compact["payload"]["delta"]["changes"]
                .as_array()
                .unwrap()
                .iter()
                .all(|change| change["asset_id"] == compact["token_id"])
        );
        assert!(compact["payload"]["post_state"].get("bids").is_none());
        assert!(compact["payload"]["post_state"].get("asks").is_none());

        let trade_source = source
            .iter()
            .find(|event| event["raw_payload"]["event_type"] == "last_trade_price")
            .unwrap();
        let trade = compact_event_frame(&profile, "epoch-example", trade_source)
            .unwrap()
            .unwrap();
        assert_eq!(trade["type"], "last_trade");
        assert_eq!(
            trade["payload"]["trade"]["price"],
            trade_source["canonical_payload"]["last_trade_price"]
        );
        assert!(trade["payload"]["post_state"]["state_checksum"].is_string());
    }
    #[tokio::test]
    async fn profile_rotation_is_cas_bound_and_requires_prepared_ordered_identity() {
        let seed = json!({"latest_cursor":0,"catalog_revision":"a".repeat(64),"gaps":[],"books":[],
            "markets":[market("4944030",&format!("0x{}", "1".repeat(64)),"10","20"),market("4944031",&format!("0x{}", "2".repeat(64)),"30","40")]});
        let publication = Publication::start(
            0,
            Some(seed),
            [
                ("10".into(), "4944030".into()),
                ("20".into(), "4944030".into()),
                ("30".into(), "4944031".into()),
                ("40".into(), "4944031".into()),
            ]
            .into(),
            8,
            131072,
            65536,
            |_| Ok(()),
        )
        .unwrap();
        let now_start = Utc::now().timestamp().div_euclid(300) * 300;
        let at = |seconds| {
            Utc.timestamp_opt(seconds, 0)
                .single()
                .unwrap()
                .to_rfc3339_opts(SecondsFormat::Secs, true)
        };
        let mut initial = profile();
        initial.markets[0].start_at = at(now_start - 300);
        initial.markets[0].end_at = at(now_start);
        initial.markets[0].window_id = format!(
            "{}/{}",
            initial.markets[0].start_at, initial.markets[0].end_at
        );
        initial.profile_id = initial.expected_id();
        let (_, control) = Api::router(
            publication.reader(),
            initial.clone(),
            "epoch-one".into(),
            Limits {
                snapshot_bytes: 65536,
                frame_bytes: 65536,
                replay_bytes: 65536,
                clients: 1,
                send_timeout: Duration::from_secs(1),
                snapshot_lease_capacity: 1,
            },
        )
        .unwrap();
        let mut next = initial.clone();
        next.profile_revision = 2;
        next.markets[0].role = WindowRole::Retiring;
        next.markets.push(MarketBinding {
            role: WindowRole::Current,
            window_id: format!("{}/{}", at(now_start), at(now_start + 300)),
            start_at: at(now_start),
            end_at: at(now_start + 300),
            market_id: "4944031".into(),
            condition_id: format!("0x{}", "2".repeat(64)),
            outcomes: [
                OutcomeBinding {
                    outcome: "Up".into(),
                    token_id: "30".into(),
                },
                OutcomeBinding {
                    outcome: "Down".into(),
                    token_id: "40".into(),
                },
            ],
            rule_sha256: "d".repeat(64),
        });
        next.profile_id = next.expected_id();
        assert!(
            control
                .publish("wrong", initial.profile_revision, next.clone())
                .is_err()
        );
        let mut expired = next.clone();
        expired.markets[0].start_at = at(now_start - 600);
        expired.markets[0].end_at = at(now_start - 300);
        expired.markets[0].window_id = format!(
            "{}/{}",
            expired.markets[0].start_at, expired.markets[0].end_at
        );
        expired.markets[1].start_at = at(now_start - 300);
        expired.markets[1].end_at = at(now_start);
        expired.markets[1].window_id = format!(
            "{}/{}",
            expired.markets[1].start_at, expired.markets[1].end_at
        );
        expired.profile_id = expired.expected_id();
        let error = control
            .publish(&initial.profile_id, initial.profile_revision, expired)
            .unwrap_err();
        assert!(error.to_string().contains("current window"));
        assert_eq!(control.profile().unwrap().profile_id, initial.profile_id);
        assert!(control.publish(&initial.profile_id,initial.profile_revision,next.clone()).is_err(),
            "ordinary 1→2 publication must not skip adjacent rotation validation");
        assert_eq!(control.profile().unwrap().profile_id,initial.profile_id);
        let receipt = control
            .publish_restart_reconciled(&initial.profile_id, initial.profile_revision, next.clone())
            .unwrap();
        assert_eq!(receipt["profile_id"], next.profile_id);
        assert_eq!(receipt["requires_new_full_sync"], true);
        assert_ne!(receipt["epoch"], "epoch-one");
        publication.finish().await.unwrap();
    }
    #[tokio::test]
    async fn two_window_publish_cas_rejects_unrelated_prepared_current() {
        let condition = |digit: char| format!("0x{}", digit.to_string().repeat(64));
        let seed = json!({"latest_cursor":0,"catalog_revision":"a".repeat(64),
            "gaps":[],"books":[],"markets":[
                market("4944030",&condition('1'),"10","20"),
                market("4944031",&condition('2'),"30","40"),
                market("4944032",&condition('3'),"50","60")]});
        let publication = Publication::start(0,Some(seed),[
            ("10".into(),"4944030".into()),("20".into(),"4944030".into()),
            ("30".into(),"4944031".into()),("40".into(),"4944031".into()),
            ("50".into(),"4944032".into()),("60".into(),"4944032".into()),
        ].into(),8,131072,65536,|_|Ok(())).unwrap();
        let start = Utc::now().timestamp().div_euclid(300)*300;
        let at = |seconds| Utc.timestamp_opt(seconds,0).single().unwrap()
            .to_rfc3339_opts(SecondsFormat::Secs,true);
        let binding = |id: &str, digit: char, up: &str, down: &str,
                       role: WindowRole, begins: i64| MarketBinding {
            role,window_id:format!("{}/{}",at(begins),at(begins+300)),
            start_at:at(begins),end_at:at(begins+300),market_id:id.into(),
            condition_id:condition(digit),outcomes:[
                OutcomeBinding{outcome:"Up".into(),token_id:up.into()},
                OutcomeBinding{outcome:"Down".into(),token_id:down.into()}],
            rule_sha256:"c".repeat(64),
        };
        let mut incumbent=profile();
        incumbent.markets=vec![
            binding("4944030",'1',"10","20",WindowRole::Current,start-300),
            binding("4944031",'2',"30","40",WindowRole::Next,start),
        ];
        incumbent.profile_id=incumbent.expected_id();
        let (_,control)=Api::router(publication.reader(),incumbent.clone(),"old-epoch".into(),
            Limits{snapshot_bytes:65536,frame_bytes:65536,replay_bytes:65536,
                clients:1,send_timeout:Duration::from_secs(1),snapshot_lease_capacity:1})
            .unwrap();
        let mut unrelated=incumbent.clone();
        unrelated.profile_revision+=1;
        unrelated.markets=vec![
            binding("4944030",'1',"10","20",WindowRole::Current,start),
            binding("4944032",'3',"50","60",WindowRole::Next,start+300),
        ];
        unrelated.profile_id=unrelated.expected_id();
        unrelated.validate_current_at(Utc::now()).unwrap();
        assert!(control.publish(&incumbent.profile_id,incumbent.profile_revision,
            unrelated).is_err(),"prepared identity is not a valid next→current rotation");
        assert_eq!(control.profile().unwrap().profile_id,incumbent.profile_id);
        let mut valid=incumbent.clone();
        valid.profile_revision+=1;
        valid.markets=vec![
            binding("4944031",'2',"30","40",WindowRole::Current,start),
            binding("4944032",'3',"50","60",WindowRole::Next,start+300),
        ];
        valid.profile_id=valid.expected_id();
        let mut expanded=valid.clone();
        expanded.markets.push(binding("4944030",'1',"10","20",
            WindowRole::Retiring,start-300));
        expanded.profile_id=expanded.expected_id();
        expanded.validate_current_at(Utc::now()).unwrap();
        assert!(control.publish(&incumbent.profile_id,incumbent.profile_revision,
            expanded.clone()).is_err(),"ordinary 2→3 cannot skip rotation validation");
        assert_eq!(control.profile().unwrap().profile_id,incumbent.profile_id);
        assert!(control.publish(&incumbent.profile_id,incumbent.profile_revision,
            valid.clone()).is_ok());
        let (_,three_window_control)=Api::router(publication.reader(),expanded.clone(),
            "three-window-epoch".into(),Limits{snapshot_bytes:65536,frame_bytes:65536,
                replay_bytes:65536,clients:1,send_timeout:Duration::from_secs(1),
                snapshot_lease_capacity:1}).unwrap();
        let mut contracted=valid;
        contracted.profile_revision=expanded.profile_revision+1;
        contracted.profile_id=contracted.expected_id();
        assert!(three_window_control.publish(&expanded.profile_id,
            expanded.profile_revision,contracted).is_err(),
            "ordinary 3→2 cannot skip rotation validation");
        assert_eq!(three_window_control.profile().unwrap().profile_id,expanded.profile_id);
        publication.finish().await.unwrap();
    }
    #[tokio::test]
    async fn ten_thousand_two_window_profile_cas_rotations_keep_one_active_generation() {
        let condition=format!("0x{}","1".repeat(64));
        let seed=json!({"latest_cursor":0,"catalog_revision":"a".repeat(64),
            "gaps":[],"books":[],"markets":[market("m0",&condition,"0-up","0-down"),
                market("m1",&condition,"1-up","1-down")]});
        let publication=Publication::start(0,Some(seed),[
            ("0-up".into(),"m0".into()),("0-down".into(),"m0".into()),
            ("1-up".into(),"m1".into()),("1-down".into(),"m1".into()),
        ].into(),8,131072,65536,|_|Ok(())).unwrap();
        let base=1_800_000_000_i64.div_euclid(300)*300;
        let market_at=|number: usize,role: WindowRole,start: i64| {
            let at=|seconds| Utc.timestamp_opt(seconds,0).single().unwrap()
                .to_rfc3339_opts(SecondsFormat::Secs,true);
            MarketBinding {role,window_id:format!("{}/{}",at(start),at(start+300)),
                start_at:at(start),end_at:at(start+300),market_id:format!("m{number}"),
                condition_id:condition.clone(),outcomes:[
                    OutcomeBinding{outcome:"Up".into(),token_id:format!("{number}-up")},
                    OutcomeBinding{outcome:"Down".into(),token_id:format!("{number}-down")}],
                rule_sha256:"c".repeat(64)}
        };
        let mut incumbent=profile();
        incumbent.markets=vec![market_at(0,WindowRole::Current,base-300),
            market_at(1,WindowRole::Next,base)];
        incumbent.profile_id=incumbent.expected_id();
        let (_,control)=Api::router(publication.reader(),incumbent.clone(),"initial".into(),
            Limits{snapshot_bytes:65536,frame_bytes:65536,replay_bytes:65536,
                clients:1,send_timeout:Duration::from_secs(1),snapshot_lease_capacity:1})
            .unwrap();
        for index in 0..10_000_usize {
            let now=Utc.timestamp_opt(base+index as i64*300+1,0).single().unwrap();
            let mut candidate=incumbent.clone();
            candidate.profile_revision+=1;
            candidate.markets=vec![
                market_at(index+1,WindowRole::Current,base+index as i64*300),
                market_at(index+2,WindowRole::Next,base+(index as i64+1)*300),
            ];
            candidate.profile_id=candidate.expected_id();
            if index==5_000 {
                let mut unrelated=candidate.clone();
                unrelated.markets[0]=market_at(index,WindowRole::Current,
                    base+index as i64*300);
                unrelated.profile_id=unrelated.expected_id();
                assert!(control.commit_profile_at(&incumbent.profile_id,
                    incumbent.profile_revision,unrelated,true,now).is_err());
                assert_eq!(control.profile().unwrap().profile_id,incumbent.profile_id);
            }
            let receipt=control.commit_profile_at(&incumbent.profile_id,
                incumbent.profile_revision,candidate.clone(),true,now).unwrap();
            assert_eq!(receipt["profile_revision"],candidate.profile_revision);
            assert_eq!(control.profile().unwrap().market_ids_owned().len(),2);
            incumbent=candidate;
        }
        assert_eq!(control.profile().unwrap().profile_revision,10_001);
        assert_eq!(control.profile().unwrap().market_ids_owned(),
            BTreeSet::from(["m10000".into(),"m10001".into()]));
        publication.finish().await.unwrap();
    }
    #[test]
    fn profile_rejects_swapped_outcomes_and_more_than_five_minutes() {
        let mut p = profile();
        p.markets[0].outcomes.swap(0, 1);
        p.profile_id = p.expected_id();
        assert!(p.validate().is_err());
        let mut p = profile();
        p.markets[0].end_at = "2026-09-26T05:36:00Z".into();
        p.profile_id = p.expected_id();
        assert!(p.validate().is_err());
    }
    #[test]
    fn current_window_is_rechecked_at_the_mutation_clock() {
        let p = profile();
        let inside = DateTime::parse_from_rfc3339("2026-09-26T05:34:59.999Z")
            .unwrap()
            .with_timezone(&Utc);
        let boundary = DateTime::parse_from_rfc3339("2026-09-26T05:35:00Z")
            .unwrap()
            .with_timezone(&Utc);
        p.validate_current_at(inside).unwrap();
        assert!(p.validate_current_at(boundary).is_err());
    }
    #[test]
    fn steady_state_rotation_reuses_next_and_never_invents_a_fourth_overlap() {
        let fixture: Value = serde_json::from_slice(include_bytes!(
            "../../../tests/fixtures/btc-5m-wire-v1.json"
        ))
        .unwrap();
        let incumbent: Profile = serde_json::from_value(fixture["profile"].clone()).unwrap();
        let old_current = incumbent.window(WindowRole::Current).unwrap().clone();
        let old_next = incumbent.window(WindowRole::Next).unwrap().clone();
        let mut candidate = incumbent.clone();
        candidate.profile_revision += 1;
        let mut current = old_next.clone();
        current.role = WindowRole::Current;
        let mut retiring = old_current.clone();
        retiring.role = WindowRole::Retiring;
        let next = MarketBinding {
            role: WindowRole::Next,
            window_id: "2026-09-26T05:40:00Z/2026-09-26T05:45:00Z".into(),
            start_at: "2026-09-26T05:40:00Z".into(),
            end_at: "2026-09-26T05:45:00Z".into(),
            market_id: "4944032".into(),
            condition_id: format!("0x{}", "4".repeat(64)),
            outcomes: [
                OutcomeBinding {
                    outcome: "Up".into(),
                    token_id: "401".into(),
                },
                OutcomeBinding {
                    outcome: "Down".into(),
                    token_id: "402".into(),
                },
            ],
            rule_sha256: canonical_hash(&json!({"fixture":"next-rule"})),
        };
        candidate.markets = vec![current, next, retiring];
        candidate.profile_id = candidate.expected_id();
        candidate.validate_rotation_from(&incumbent).unwrap();

        let mut unrelated = candidate;
        unrelated.markets[0].market_id = "unrelated".into();
        unrelated.profile_id = unrelated.expected_id();
        assert!(unrelated.validate_rotation_from(&incumbent).is_err());
    }
    #[test]
    fn autonomous_two_window_rotation_drops_expired_current_before_new_next() {
        let fixture: Value = serde_json::from_slice(include_bytes!(
            "../../../tests/fixtures/btc-5m-wire-v1.json"
        ))
        .unwrap();
        let mut incumbent: Profile = serde_json::from_value(fixture["profile"].clone()).unwrap();
        incumbent
            .markets
            .retain(|market| market.role != WindowRole::Retiring);
        incumbent.profile_id = incumbent.expected_id();
        incumbent.validate().unwrap();
        let old_next = incumbent.window(WindowRole::Next).unwrap().clone();
        let mut current = old_next;
        current.role = WindowRole::Current;
        let next = MarketBinding {
            role: WindowRole::Next,
            window_id: "2026-09-26T05:40:00Z/2026-09-26T05:45:00Z".into(),
            start_at: "2026-09-26T05:40:00Z".into(),
            end_at: "2026-09-26T05:45:00Z".into(),
            market_id: "4944032".into(),
            condition_id: format!("0x{}", "4".repeat(64)),
            outcomes: [
                OutcomeBinding {
                    outcome: "Up".into(),
                    token_id: "401".into(),
                },
                OutcomeBinding {
                    outcome: "Down".into(),
                    token_id: "402".into(),
                },
            ],
            rule_sha256: canonical_hash(&json!({"fixture":"next-rule"})),
        };
        let mut candidate = incumbent.clone();
        candidate.profile_revision += 1;
        candidate.markets = vec![current, next];
        candidate.profile_id = candidate.expected_id();
        candidate.validate_rotation_from(&incumbent).unwrap();
        assert_eq!(
            incumbent
                .market_ids()
                .difference(&candidate.market_ids())
                .count(),
            1
        );
        assert_eq!(
            candidate
                .market_ids()
                .difference(&incumbent.market_ids())
                .count(),
            1
        );
    }
    #[tokio::test]
    async fn full_sync_is_two_token_non_atomic_and_retiring_terminates_without_settlement() {
        let seed = json!({"latest_cursor":0,"catalog_revision":"a".repeat(64),
        "gaps":[],"books":[book("10",&format!("0x{}", "1".repeat(64)),"2026-09-26T05:30:01Z"),book("20",&format!("0x{}", "1".repeat(64)),"2026-09-26T05:30:02Z")],
        "markets":[market("4944030",&format!("0x{}", "1".repeat(64)),"10","20")]});
        let p = Publication::start(
            0,
            Some(seed),
            [
                ("10".into(), "4944030".into()),
                ("20".into(), "4944030".into()),
            ]
            .into(),
            8,
            131072,
            65536,
            |_| Ok(()),
        )
        .unwrap();
        let profile = profile();
        let value = full_sync(
            &p.capture_view().unwrap(),
            &profile,
            "epoch",
            DateTime::parse_from_rfc3339("2026-09-26T05:30:03Z")
                .unwrap()
                .with_timezone(&Utc),
        )
        .unwrap();
        assert_eq!(value["ready"], true);
        assert_eq!(value["markets"][0]["cross_token_atomic"], false);
        assert_eq!(value["markets"][0]["tokens"][1]["outcome"], "Down");
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn gap_only_invalidates_affected_token() {
        let mut seed = json!({"latest_cursor":0,"catalog_revision":"a".repeat(64),"gaps":[],
        "books":[book("10",&format!("0x{}", "1".repeat(64)),"2026-09-26T05:30:01Z"),book("20",&format!("0x{}", "1".repeat(64)),"2026-09-26T05:30:02Z")],
        "markets":[market("4944030",&format!("0x{}", "1".repeat(64)),"10","20")]});
        seed["gaps"] = json!([{"token_id":"10","resolved":false}]);
        let p = Publication::start(
            0,
            Some(seed),
            [
                ("10".into(), "4944030".into()),
                ("20".into(), "4944030".into()),
            ]
            .into(),
            8,
            131072,
            65536,
            |_| Ok(()),
        )
        .unwrap();
        let value = full_sync(
            &p.capture_view().unwrap(),
            &profile(),
            "epoch",
            DateTime::parse_from_rfc3339("2026-09-26T05:30:03Z")
                .unwrap()
                .with_timezone(&Utc),
        )
        .unwrap();
        assert_eq!(value["markets"][0]["tokens"][0]["available"], false);
        assert_eq!(
            value["markets"][0]["tokens"][0]["replay_baseline_valid"],
            false
        );
        assert_eq!(
            value["markets"][0]["tokens"][0]["unavailable_reason"],
            "source_gap"
        );
        assert_eq!(value["markets"][0]["tokens"][1]["available"], true);
        assert_eq!(
            value["markets"][0]["tokens"][1]["replay_baseline_valid"],
            true
        );
        assert_eq!(value["markets"][0]["tokens"][1]["token_id"], "20");
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn full_sync_binds_an_in_progress_recovery_to_its_authoritative_snapshot() {
        let fixture: Value = serde_json::from_slice(include_bytes!(
            "../../../tests/fixtures/btc-5m-wire-v1.json"
        ))
        .unwrap();
        let profile: Profile = serde_json::from_value(fixture["profile"].clone()).unwrap();
        let source = crate::source_websocket::btc5m_reducer_fixture_sequence().unwrap();
        let markets = profile
            .markets
            .iter()
            .map(|binding| {
                market(
                    &binding.market_id,
                    &binding.condition_id,
                    &binding.outcomes[0].token_id,
                    &binding.outcomes[1].token_id,
                )
            })
            .collect::<Vec<_>>();
        let tokens = profile
            .markets
            .iter()
            .flat_map(|binding| {
                binding
                    .outcomes
                    .iter()
                    .map(|outcome| (outcome.token_id.clone(), binding.market_id.clone()))
            })
            .collect();
        let mut publication = Publication::start(
            42,
            Some(
                json!({"latest_cursor":42,"catalog_revision":profile.catalog_revision,
                "live_stream_connected":true,"gaps":[],"books":[],"markets":markets}),
            ),
            tokens,
            8,
            1024 * 1024,
            512 * 1024,
            |_| Ok(()),
        )
        .unwrap();
        for event in &source[..5] {
            publication.publish(vec![event.clone()], None).unwrap();
        }
        // A new consumer may join after recovery_started but before the
        // authoritative snapshot. The baseline must carry the identity that
        // binds both subsequent frames, without making the old book usable.
        let before = full_sync(
            &publication.capture_view().unwrap(),
            &profile,
            "epoch-recovery",
            DateTime::parse_from_rfc3339("2026-09-26T05:30:08.500Z")
                .unwrap()
                .with_timezone(&Utc),
        )
        .unwrap();
        let recovering = &before["markets"][0]["tokens"][0];
        assert_eq!(recovering["available"], false);
        assert_eq!(recovering["unavailable_reason"], "source_gap");
        assert_eq!(recovering["replay_baseline_valid"], false);
        assert!(recovering["recovery_snapshot_cursor"].is_null());
        let recovery_id = recovering["recovery_id"].clone();
        assert!(recovery_id.as_str().is_some());
        publication.publish(vec![source[5].clone()], None).unwrap();
        let value = full_sync(
            &publication.capture_view().unwrap(),
            &profile,
            "epoch-recovery",
            DateTime::parse_from_rfc3339("2026-09-26T05:30:08.500Z")
                .unwrap()
                .with_timezone(&Utc),
        )
        .unwrap();
        let token = &value["markets"][0]["tokens"][0];
        assert_eq!(token["token_id"], "101");
        assert_eq!(token["replay_baseline_valid"], true);
        assert!(token["recovery_id"].as_str().is_some());
        assert_eq!(token["recovery_id"], recovery_id);
        assert_eq!(token["recovery_snapshot_cursor"], token["publisher_cursor"]);
        let completion = event_frame(&profile, "epoch-recovery", &source[6])
            .unwrap()
            .unwrap();
        assert_eq!(completion["type"], "recovery_completed");
        assert_eq!(completion["payload"]["canonical_payload"]["recovery_id"], recovery_id);
        assert_eq!(completion["payload"]["canonical_payload"]["snapshot_cursor"], token["recovery_snapshot_cursor"]);
        publication.publish(vec![source[6].clone()], None).unwrap();
        let finished = publication.capture_view().unwrap();
        assert!(!finished.recoveries.contains_key("101"));
        publication.finish().await.unwrap();
    }
    #[test]
    fn confirmation_frame_advances_only_the_independent_freshness_watermark() {
        let profile = profile();
        let condition = format!("0x{}", "1".repeat(64));
        let mut confirmed = book("10", &condition, "2026-09-26T05:30:01Z");
        confirmed["book_epoch"] = json!("epoch-book");
        confirmed["sequence"] = json!(7);
        confirmed["confirmed_at"] = json!("2026-09-26T05:30:08.500Z");
        confirmed["confirmation_source"] = json!("rest_book_confirmation");
        confirmed["confirmation_evidence_sha256"] = json!("e".repeat(64));
        confirmed["book_received_at"] = confirmed["received_at"].clone();
        confirmed.as_object_mut().unwrap().remove("received_at");
        let frame = confirmation_frame(
            &profile,
            "epoch-confirmation",
            49,
            3,
            &[Arc::new(confirmed)],
        )
        .unwrap()
        .unwrap();
        assert_eq!(frame["type"], "book_confirmations");
        assert_eq!(frame["publisher_cursor"], 49);
        assert_eq!(frame["confirmation_sequence"], 3);
        assert_eq!(
            frame["confirmations"][0]["book_received_at"],
            "2026-09-26T05:30:01Z"
        );
        assert_eq!(
            frame["confirmations"][0]["confirmed_at"],
            "2026-09-26T05:30:08.500Z"
        );
        assert!(frame.get("payload").is_none());
    }
    #[tokio::test]
    async fn expired_current_profile_fails_closed_even_with_fresh_books() {
        let seed = json!({"latest_cursor":0,"catalog_revision":"a".repeat(64),"gaps":[],
        "books":[book("10",&format!("0x{}", "1".repeat(64)),"2026-09-26T05:35:00Z"),book("20",&format!("0x{}", "1".repeat(64)),"2026-09-26T05:35:00Z")],
        "markets":[market("4944030",&format!("0x{}", "1".repeat(64)),"10","20")]});
        let p = Publication::start(
            0,
            Some(seed),
            [
                ("10".into(), "4944030".into()),
                ("20".into(), "4944030".into()),
            ]
            .into(),
            8,
            131072,
            65536,
            |_| Ok(()),
        )
        .unwrap();
        let value = full_sync(
            &p.capture_view().unwrap(),
            &profile(),
            "epoch",
            DateTime::parse_from_rfc3339("2026-09-26T05:35:01Z")
                .unwrap()
                .with_timezone(&Utc),
        )
        .unwrap();
        assert_eq!(value["ready"], false);
        assert_eq!(
            value["markets"][0]["tokens"][0]["unavailable_reason"],
            "market_window_inactive"
        );
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn future_or_locked_book_is_never_available() {
        let condition = format!("0x{}", "1".repeat(64));
        let future = book("10", &condition, "2026-09-26T05:30:04Z");
        let mut locked = book("20", &condition, "2026-09-26T05:30:02Z");
        locked["bids"] = json!([{"price":"0.60","size":"1"}]);
        locked["asks"] = json!([{"price":"0.60","size":"1"}]);
        let seed = json!({"latest_cursor":0,"catalog_revision":"a".repeat(64),"gaps":[],
            "books":[future,locked],
            "markets":[market("4944030",&condition,"10","20")]});
        let publication = Publication::start(
            0,
            Some(seed),
            [
                ("10".into(), "4944030".into()),
                ("20".into(), "4944030".into()),
            ]
            .into(),
            8,
            131072,
            65536,
            |_| Ok(()),
        )
        .unwrap();
        let value = full_sync(
            &publication.capture_view().unwrap(),
            &profile(),
            "epoch",
            DateTime::parse_from_rfc3339("2026-09-26T05:30:03Z")
                .unwrap()
                .with_timezone(&Utc),
        )
        .unwrap();
        assert_eq!(value["markets"][0]["tokens"][0]["available"], false);
        assert_eq!(
            value["markets"][0]["tokens"][0]["replay_baseline_valid"],
            false
        );
        assert_eq!(
            value["markets"][0]["tokens"][0]["unavailable_reason"],
            "future_received_at"
        );
        assert_eq!(
            value["markets"][0]["tokens"][1]["unavailable_reason"],
            "crossed_book"
        );
        assert_eq!(
            value["markets"][0]["tokens"][1]["replay_baseline_valid"],
            false
        );
        assert_eq!(
            value["crossed_book_rule"],
            "best_bid_greater_than_or_equal_to_best_ask_is_invalid"
        );
        publication.finish().await.unwrap();
    }
    #[tokio::test]
    async fn management_503_uses_versioned_error_fixture() {
        let response = failure(StatusCode::SERVICE_UNAVAILABLE,
            "btc_5m_management_unavailable", true);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = axum::body::to_bytes(response.into_body(), 65536).await.unwrap();
        let actual: Value = serde_json::from_slice(&bytes).unwrap();
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/btc-5m-management-health-v1-503.json"
        )).unwrap();
        assert_eq!(actual, fixture);
    }

    #[tokio::test]
    async fn routes_serve_bounded_same_process_status_and_full_sync() {
        let seed = json!({"latest_cursor":0,"catalog_revision":"a".repeat(64),"gaps":[],
        "books":[book("10",&format!("0x{}", "1".repeat(64)),"2026-09-26T05:30:01Z"),book("20",&format!("0x{}", "1".repeat(64)),"2026-09-26T05:30:02Z")],
        "markets":[market("4944030",&format!("0x{}", "1".repeat(64)),"10","20")]});
        let p = Publication::start(
            0,
            Some(seed),
            [
                ("10".into(), "4944030".into()),
                ("20".into(), "4944030".into()),
            ]
            .into(),
            8,
            131072,
            65536,
            |_| Ok(()),
        )
        .unwrap();
        let (router, _) = Api::router(
            p.reader(),
            profile(),
            "epoch-http".into(),
            Limits {
                snapshot_bytes: 65536,
                frame_bytes: 65536,
                replay_bytes: 65536,
                clients: 1,
                send_timeout: Duration::from_secs(1),
                snapshot_lease_capacity: 1,
            },
        )
        .unwrap();
        for path in [
            "/v1/prediction-markets/polymarket/research/btc-5m/status",
            "/v1/prediction-markets/polymarket/research/btc-5m/full-sync",
        ] {
            let response = router
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(path)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), 65536)
                .await
                .unwrap();
            assert!(bytes.len() < 65536);
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value["epoch"], "epoch-http");
        }
        let timeline_path = format!(
            "/v1/prediction-markets/polymarket/research/btc-5m/timeline?epoch=epoch-http&publisher_cursor=1&event_id={}",
            "a".repeat(64)
        );
        let timeline = router.clone().oneshot(axum::http::Request::builder()
            .uri(timeline_path).body(axum::body::Body::empty()).unwrap())
            .await.unwrap();
        assert_eq!(timeline.status(), StatusCode::GONE);
        p.pause_source().unwrap();
        let health = router.oneshot(axum::http::Request::builder()
            .uri("/v1/prediction-markets/polymarket/research/btc-5m/management-health")
            .body(axum::body::Body::empty()).unwrap()).await.unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(health.into_body(), 65536).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["data_state"], "unavailable");
        assert_eq!(value["entry_ready"], Value::Null);
        assert_eq!(value["publication"]["publication_binding_tokens"], 2);
        assert_eq!(value["publication"]["metadata_market_count"], 1);
        assert_eq!(value["publication"]["wire_tokens"], Value::Null);
        assert_eq!(value["resources"]["phase_counts_authoritative"], false);
        p.finish().await.unwrap();
    }
}
