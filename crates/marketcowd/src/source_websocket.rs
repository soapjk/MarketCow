//! Public Polymarket WS -> existing strict core -> live.v2 memory publication.
//! The internal core reducer has no durable side effects. Only Publication owns
//! public cursors/durable watermarks; reducer cursors never cross this boundary.
use super::{Args, Plan};
use crate::source_publication::{ConfirmationOutcome, Publication};
use anyhow::{Context, Result, ensure};
use chrono::{SecondsFormat, Timelike, Utc};
use marketcow_core::{CanonicalEvent, CoreError, DurableLog, EventKind, Price, SingleWriter};
use marketcow_polymarket::discovery_source::{SnapshotBoundary, canonical_hash, snapshot_event};
use marketcow_polymarket::{
    NormalizerConfig, PolymarketTransportConfig, normalize_frame_with_book_tick,
    run_polymarket_transport,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
#[path = "source_websocket_pipeline.rs"]
mod pipeline;

struct MemoryReducer;
#[derive(Debug)]
struct ResyncRequired {
    token: String,
    observed: chrono::DateTime<Utc>,
    received: chrono::DateTime<Utc>,
}
impl std::fmt::Display for ResyncRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "source_data_delayed token={} observed={} received={}; fresh WS baseline required",
            self.token, self.observed, self.received
        )
    }
}
impl std::error::Error for ResyncRequired {}
impl DurableLog for MemoryReducer {
    fn append(&mut self, _: &CanonicalEvent) -> std::result::Result<(), CoreError> {
        Ok(())
    }
}
struct Adapter {
    config: NormalizerConfig,
    identities: BTreeMap<String, (String, String)>,
    ticks: BTreeMap<String, Price>,
    engines: BTreeMap<String, SingleWriter<MemoryReducer>>,
    epochs: BTreeMap<String, String>,
    sequences: BTreeMap<String, u64>,
    waiting: BTreeSet<String>,
    book_received: BTreeMap<String, chrono::DateTime<Utc>>,
    // Complete Polymarket book frames carry this fact even though the generic
    // reducer represents later trade ticks as separate events.
    last_trades: BTreeMap<String, Price>,
    recoveries: BTreeMap<String, String>,
}
struct ScopeAcquisition {
    validate_only:bool,catalog_revision:String,records:Vec<Value>,evidence_sha256:String,
    acquisition_market_ids:BTreeSet<String>,retire_market_ids:BTreeSet<String>,
}
impl Adapter {
    /// The owner has already bound this exact identity to catalog metadata.
    /// New reducers start without tick/book/freshness state; snapshots must
    /// supply those facts. Retained pairs are never reset by admission.
    fn admit_market(&mut self, market:&super::Market)->Result<bool> {
        ensure!(!market.market_id.is_empty() && !market.condition_id.is_empty()
            && market.token_ids[0]!=market.token_ids[1]
            && market.token_ids.iter().all(|token|!token.is_empty()&&token.len()<=128),"market admission identity");
        let expected=(market.market_id.clone(),market.condition_id.clone());
        let existing=market.token_ids.iter().filter(|token|self.identities.contains_key(*token)).count();
        if existing>0 {
            ensure!(existing==2 && market.token_ids.iter().all(|token|self.identities.get(token)==Some(&expected)),"market admission ownership conflict");
            return Ok(false);
        }
        ensure!(!self.identities.values().any(|(id,_)|id==&market.market_id),"market admission changed token pair");
        ensure!(self.identities.len()+2<=marketcow_runtime::discovery_source::MAX_SOURCE_TOKEN_IDENTITIES,"market admission token capacity");
        for token in &market.token_ids {
            self.identities.insert(token.clone(),expected.clone());
            self.waiting.insert(token.clone());
        }
        Ok(true)
    }
    fn market_actor(&self, market: &str) -> Self {
        let identities: BTreeMap<_, _> = self
            .identities
            .iter()
            .filter(|(_, (m, _))| m == market)
            .map(|(t, identity)| (t.clone(), identity.clone()))
            .collect();
        Self {
            config: self.config.clone(),
            ticks: self
                .ticks
                .iter()
                .filter(|(t, _)| identities.contains_key(*t))
                .map(|(t, v)| (t.clone(), v.clone()))
                .collect(),
            waiting: identities.keys().cloned().collect(),
            recoveries: self
                .recoveries
                .iter()
                .filter(|(t, _)| identities.contains_key(*t))
                .map(|(t, v)| (t.clone(), v.clone()))
                .collect(),
            identities,
            engines: BTreeMap::new(),
            epochs: BTreeMap::new(),
            sequences: BTreeMap::new(),
            book_received: BTreeMap::new(),
            last_trades: BTreeMap::new(),
        }
    }
    fn reset_connection(&mut self) {
        // Discard the entire connection's reducer state, including any partial
        // multi-token frame. Keep verified tick metadata, never stale books.
        self.waiting = self.identities.keys().cloned().collect();
        self.engines.clear();
        self.epochs.clear();
        self.sequences.clear();
        self.book_received.clear();
        self.last_trades.clear();
    }
    fn new(plan: &Plan, seed: &Value) -> Result<Self> {
        let market_states: BTreeMap<_, _> = seed["markets"]
            .as_array()
            .context("market seed")?
            .iter()
            .map(|m| {
                Ok((
                    m["identity"]["market_id"]
                        .as_str()
                        .context("market id")?
                        .to_owned(),
                    m["lifecycle_state"]
                        .as_str()
                        .context("market lifecycle state")?
                        .to_owned(),
                ))
            })
            .collect::<Result<_>>()?;
        let terminal_tokens: BTreeSet<_> = seed["markets"]
            .as_array()
            .context("market seed")?
            .iter()
            .filter(|market| market["lifecycle_state"] == "closed" || market["lifecycle_state"] == "resolved")
            .flat_map(|market| market["identity"]["outcomes"].as_array().into_iter().flatten())
            .filter_map(|outcome| outcome["token_id"].as_str())
            .collect();
        let active: BTreeSet<_> = market_states
            .iter()
            .filter(|(_, state)| state.as_str() == "active")
            .map(|(market, _)| market.as_str())
            .collect();
        let mut identities = BTreeMap::new();
        for market in &plan.markets {
            if active.contains(market.market_id.as_str()) {
                for token in &market.token_ids {
                    identities.insert(
                        token.clone(),
                        (market.market_id.clone(), market.condition_id.clone()),
                    );
                }
            }
        }
        ensure!(
            !identities.is_empty() && identities.len() <= marketcow_runtime::discovery_source::MAX_SOURCE_TOKEN_IDENTITIES,
            "bounded active WS universe required"
        );
        let mut ticks = BTreeMap::new();
        for book in seed["books"].as_array().context("book seed")? {
            let token = book["token_id"].as_str().context("token")?;
            if identities.contains_key(token) {
                ticks.insert(
                    token.into(),
                    Price::parse_tick(
                        book["tick_size"]
                            .as_str()
                            .context("verified tick metadata missing")?,
                    )?,
                );
            }
        }
        // A cold candidate can lack authoritative tick metadata. Keep those
        // tokens waiting; normalization still refuses a book without a verified
        // tick, and the existing token-local recovery/REST snapshot path supplies
        // it. Missing metadata must not prevent unrelated markets from starting.
        let recoveries = seed
            .get("token_recoveries")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|recovery| {
                let token = recovery["token_id"]
                    .as_str()
                    .context("recovery token")?
                    .to_owned();
                // A terminal lifecycle overlay is authoritative for the market
                // and retires its books. Older generations may still contain
                // the pre-terminal coverage recovery row; preserve that row in
                // durable history, but do not let it poison the active WS
                // identity set or force a recovery for a retired token.
                if !identities.contains_key(&token) {
                    let market = recovery["market_id"]
                        .as_str()
                        .context("recovery market")?;
                    ensure!(
                        market_states
                            .get(market)
                            .is_some_and(|state| state == "closed" || state == "resolved"),
                        "recovery token outside scope"
                    );
                    ensure!(terminal_tokens.contains(token.as_str()), "recovery token outside scope");
                    return Ok(None);
                }
                Ok(Some((
                    token,
                    recovery["recovery_id"]
                        .as_str()
                        .context("recovery id")?
                        .to_owned(),
                )))
            })
            .filter_map(|item| item.transpose())
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            config: NormalizerConfig::new(
                seed["scope_id"].as_str().context("scope id")?,
                &plan.catalog_revision,
            ),
            waiting: identities.keys().cloned().collect(),
            identities,
            ticks,
            engines: BTreeMap::new(),
            epochs: BTreeMap::new(),
            sequences: BTreeMap::new(),
            book_received: BTreeMap::new(),
            last_trades: BTreeMap::new(),
            recoveries,
        })
    }
    fn invalidate(
        &mut self,
        token: &str,
        reason: &str,
        received: chrono::DateTime<Utc>,
        cursor: u64,
    ) -> Result<Value> {
        let (market, condition) = self
            .identities
            .get(token)
            .context("unknown recovery token")?;
        let attempt = uuid::Uuid::new_v4().to_string();
        let at = wire_time(received)?;
        let payload = json!({"recovery_scope":"token","token_id":token,"recovery_id":attempt,"reason":reason});
        let gap = json!({"token_id":token,"code":"coverage_gap","resolved":false,
            "detected_at":at,"event_at":null,"expected":null,"observed":null,"resolution":null});
        let event = recovery_event(
            market,
            condition,
            token,
            cursor,
            &at,
            "recovery_started",
            payload,
            vec![gap],
        );
        self.recoveries.insert(token.into(), attempt);
        self.waiting.insert(token.into());
        self.engines.remove(token);
        self.epochs.remove(token);
        self.sequences.remove(token);
        self.book_received.remove(token);
        self.last_trades.remove(token);
        Ok(event)
    }

    /// A reconnecting shard can report the same connection boundary more than
    /// once before its authoritative snapshot arrives. The first gap already
    /// invalidated the book; replacing its recovery identity would make the
    /// in-flight snapshot stale and create an unbounded recovery storm.
    fn invalidate_once(
        &mut self,
        token: &str,
        reason: &str,
        received: chrono::DateTime<Utc>,
        cursor: u64,
    ) -> Result<Option<Value>> {
        if self.recoveries.contains_key(token) {
            return Ok(None);
        }
        self.invalidate(token, reason, received, cursor).map(Some)
    }

    /// Preserve an upstream multi-token frame as an atomic unit. If validation
    /// fails after partially reducing it, invalidate every token in that frame,
    /// never unrelated markets. No partially reduced output escapes.
    fn apply_isolated(
        &mut self,
        raw: Value,
        received: chrono::DateTime<Utc>,
        after: u64,
    ) -> Result<Vec<Value>> {
        let mut affected = BTreeSet::new();
        if let Some(token) = raw["asset_id"].as_str() {
            affected.insert(token.to_owned());
        }
        if let Some(changes) = raw["price_changes"].as_array() {
            for change in changes {
                affected.insert(
                    change["asset_id"]
                        .as_str()
                        .context("unidentifiable price change")?
                        .to_owned(),
                );
            }
        }
        if affected.is_empty()
            && let Some(condition) = raw["market"].as_str()
        {
            affected.extend(
                self.identities
                    .iter()
                    .filter(|(_, (_, c))| c == condition)
                    .map(|(t, _)| t.clone()),
            );
        }
        ensure!(
            affected.iter().all(|t| self.identities.contains_key(t)),
            "unsubscribed frame identity"
        );
        let source_gap = raw["event_type"] == "source_gap";
        let source_gap_diagnostic = source_gap.then(|| json!({
            "transport_reason":raw["transport_reason"],
            "close_code":raw["close_code"],
            "close_reason":raw["close_reason"],
            "transport_attempt":raw["attempt"],
        }));
        let full = raw["event_type"] == "book";
        // A price-change array is one atomic market observation. If any member
        // lacks continuity, do not partially advance its healthy sibling.
        if !full && !source_gap && affected.iter().any(|token| self.waiting.contains(token)) {
            return Ok(Vec::new());
        }
        let result = if source_gap {
            Err(anyhow::anyhow!("source_connection_gap"))
        } else {
            self.apply(raw, received, after)
        };
        match result {
            Ok(mut events) => {
                if full && let Some(snapshot) = events.first() {
                    let token = snapshot["token_id"]
                        .as_str()
                        .context("snapshot token")?
                        .to_owned();
                    let snapshot_cursor = snapshot["cursor"].as_u64().context("snapshot cursor")?;
                    if let Some(attempt) = self.recoveries.remove(&token) {
                        let (market, condition) = &self.identities[&token];
                        let payload = json!({"recovery_scope":"token","token_id":token,"recovery_id":attempt,
                            "snapshot_cursor":snapshot_cursor,"resolved_gap_token_ids":[token]});
                        events.push(recovery_event(
                            market,
                            condition,
                            &token,
                            snapshot_cursor.checked_add(1).context("cursor overflow")?,
                            &wire_time(received)?,
                            "recovery_completed",
                            payload,
                            vec![],
                        ));
                    }
                }
                Ok(events)
            }
            Err(error) => {
                ensure!(
                    !affected.is_empty(),
                    "unidentifiable upstream failure: {error}"
                );
                let reason = if source_gap {
                    "source_connection_gap"
                } else if error.downcast_ref::<ResyncRequired>().is_some() {
                    "source_data_delayed"
                } else {
                    "source_validation_failed"
                };
                let invalidatable = affected.iter().filter(|token| {
                    !source_gap || !self.recoveries.contains_key(*token)
                }).cloned().collect::<Vec<_>>();
                if invalidatable.is_empty() {
                    return Ok(Vec::new());
                }
                eprintln!(
                    "{}",
                    json!({"stage":"token_invalidation","tokens":invalidatable,"reason":reason,
                        "detail":error.to_string(),"source_gap":source_gap_diagnostic})
                );
                let mut invalidations = Vec::new();
                for token in &invalidatable {
                    let cursor = after
                        .checked_add(invalidations.len() as u64 + 1)
                        .context("cursor overflow")?;
                    let event = if source_gap {
                        self.invalidate_once(token, reason, received, cursor)?
                    } else {
                        Some(self.invalidate(token, reason, received, cursor)?)
                    };
                    if let Some(event) = event {
                        invalidations.push(event);
                    }
                }
                Ok(invalidations)
            }
        }
    }
    fn apply(
        &mut self,
        raw: Value,
        received: chrono::DateTime<Utc>,
        after: u64,
    ) -> Result<Vec<Value>> {
        let kind = raw["event_type"]
            .as_str()
            .context("missing WS event_type")?
            .to_owned();
        if kind == "source_gap" {
            let token = raw["asset_id"].as_str().context("gap token")?;
            ensure!(
                self.identities.contains_key(token),
                "unsubscribed gap token"
            );
            self.waiting.insert(token.into());
            self.engines.remove(token);
            return Ok(Vec::new());
        }
        if kind == "new_market" {
            return Ok(Vec::new());
        } // Frozen scope is changed only through its configured contract.
        ensure!(
            kind != "market_resolved",
            "market resolution observed; authoritative metadata recovery required"
        );
        let tick = raw["asset_id"]
            .as_str()
            .and_then(|t| self.ticks.get(t))
            .cloned();
        let normalized =
            normalize_frame_with_book_tick(&self.config, raw.clone(), received, 1, tick)?;
        let mut output = Vec::new();
        for mut event in normalized {
            let token = event.kind.token_id().to_owned();
            let (market, condition) = self
                .identities
                .get(&token)
                .context("unsubscribed WS token")?
                .clone();
            ensure!(
                raw["market"].as_str() == Some(condition.as_str()),
                "WS condition/catalog mismatch"
            );
            if self.waiting.contains(&token) && !matches!(event.kind, EventKind::FullBook { .. }) {
                continue;
            }
            let engine = self.engines.entry(token.clone()).or_insert_with(|| {
                SingleWriter::new_data_delivery(self.config.scope_id.clone(), MemoryReducer)
            });
            event.cursor = engine
                .projection()
                .cursor
                .checked_add(1)
                .context("reducer overflow")?;
            let full = matches!(event.kind, EventKind::FullBook { .. });
            let observed = event.source_observed_at;
            let applied = match engine.apply(event) {
                Ok(applied) => applied,
                // Native identity hashes the complete raw frame plus token and
                // scope. An exact replay within this connection is idempotent:
                // do not advance any cursor, sequence or freshness timestamp.
                Err(CoreError::DuplicateEvent(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            if !applied.persisted.applied
                && applied.persisted.fail_closed_reason.as_deref() == Some("source_data_delayed")
            {
                return Err(ResyncRequired {
                    token,
                    observed,
                    received,
                }
                .into());
            }
            ensure!(
                applied.persisted.applied,
                "WS book rejected: {:?}",
                applied.persisted.fail_closed_reason
            );
            let book = applied
                .projection
                .books
                .get(&token)
                .context("missing materialized WS book")?;
            if full {
                self.waiting.remove(&token);
                self.epochs.insert(token.clone(),canonical_hash(&json!({"connection_snapshot":uuid::Uuid::new_v4().to_string(),"token_id":token})));
                self.sequences.insert(token.clone(), 0);
            }
            let sequence = self
                .sequences
                .get_mut(&token)
                .context("delta without full-book epoch")?;
            *sequence = sequence.checked_add(1).context("sequence overflow")?;
            let tick = book.tick_size.as_ref().context("missing tick")?;
            self.ticks.insert(token.clone(), tick.clone());
            if full {
                let last_trade = match raw.get("last_trade_price") {
                    None | Some(Value::Null) => None,
                    Some(Value::String(value)) if value.is_empty() => None,
                    Some(Value::String(value)) => Some(Price::parse(value)?),
                    Some(Value::Number(value)) => Some(Price::parse(&value.to_string())?),
                    _ => anyhow::bail!("invalid book last_trade_price"),
                };
                ensure!(
                    last_trade
                        .as_ref()
                        .is_none_or(|price| (price.0 % tick.0).is_zero()),
                    "book last_trade_price is off tick"
                );
                if let Some(price) = last_trade {
                    self.last_trades.insert(token.clone(), price);
                } else {
                    self.last_trades.remove(&token);
                }
            } else if kind == "last_trade_price"
                && let Some(price) = &book.last_trade_price
            {
                self.last_trades.insert(token.clone(), price.clone());
            }
            let bids: Vec<_> = book
                .bids
                .iter()
                .rev()
                .map(|(p, q)| json!({"price":p.0.to_string(),"size":q.to_string()}))
                .collect();
            let asks: Vec<_> = book
                .asks
                .iter()
                .map(|(p, q)| json!({"price":p.0.to_string(),"size":q.to_string()}))
                .collect();
            let at = wire_time(received)?;
            let exchange = wire_time(observed)?;
            if matches!(kind.as_str(), "book" | "price_change") {
                self.book_received.insert(token.clone(), received);
            }
            let book_at = wire_time(
                *self
                    .book_received
                    .get(&token)
                    .context("missing book observation")?,
            )?;
            let checksum = canonical_hash(
                &json!({"token_id":token,"tick_size":tick.0.to_string(),"bids":bids,"asks":asks}),
            );
            let model = json!({"token_id":token,"condition_id":condition,"book_epoch":self.epochs[&token],
                "sequence":sequence,"sequence_semantics":"deterministic_normalized","exchange_at":exchange,
                "received_at":book_at,"tick_version":book.tick_version,"tick_size":tick.0.to_string(),
                "bids":bids,"asks":asks,"last_trade_price":self.last_trades.get(&token).map(|p|p.0.to_string()),
                "state_checksum":checksum,"source_hash":canonical_hash(&raw)});
            let mut envelope = json!({"contract_version":"marketcow.prediction_market.v1","schema_version":"marketcow.polymarket.live.v2",
                "cursor":after.checked_add(output.len() as u64+1).context("cursor overflow")?,"event_type":"book",
                "market_id":market,"condition_id":condition,"token_id":token,"book_epoch":self.epochs[&token],"sequence":sequence,
                "exchange_at":exchange,"received_at":at,"canonical_payload_sha256":canonical_hash(&model),"canonical_payload":model,
                "raw_payload_sha256":canonical_hash(&raw),"raw_payload":raw,"applied":true,"fail_closed_reason":null,"gaps":[]});
            envelope["event_id"] = json!(canonical_hash(&envelope));
            output.push(envelope);
        }
        Ok(output)
    }
    fn apply_rest_recovery(
        &mut self,
        mut raw_books: Vec<Value>,
        received: chrono::DateTime<Utc>,
        after: u64,
        attempts: &BTreeMap<String, String>,
    ) -> Result<Vec<Value>> {
        if attempts
            .iter()
            .all(|(token, attempt)| self.recoveries.get(token) != Some(attempt))
        {
            return Ok(Vec::new()); // An earlier recovery source already won this fenced attempt.
        }
        ensure!(
            raw_books.len() == 2,
            "REST recovery requires a two-token market"
        );
        raw_books.sort_by(|left, right| left["asset_id"].as_str().cmp(&right["asset_id"].as_str()));
        let boundary = uuid::Uuid::new_v4().to_string();
        let mut output = Vec::new();
        let mut market_identity = None;
        for raw in raw_books {
            let token = raw["asset_id"]
                .as_str()
                .context("REST recovery token")?
                .to_owned();
            let (market, condition) = self
                .identities
                .get(&token)
                .context("unknown REST recovery token")?
                .clone();
            ensure!(
                raw["market"] == condition,
                "REST recovery identity mismatch"
            );
            if let Some(expected) = &market_identity {
                ensure!(expected == &market, "REST recovery crosses markets");
            }
            market_identity = Some(market.clone());
            let mut reducer_raw = raw.clone();
            reducer_raw["event_type"] = json!("book");
            ensure!(
                self.apply(reducer_raw, received, 0)?.len() == 1,
                "REST recovery did not seed one token book"
            );
            let attempt = attempts
                .get(&token)
                .filter(|attempt| self.recoveries.get(&token) == Some(*attempt));
            let recovery_id = attempt.map(String::as_str).unwrap_or(&boundary);
            let cursor = after
                .checked_add(output.len() as u64 + 1)
                .context("cursor overflow")?;
            let snapshot = snapshot_event(
                &raw,
                SnapshotBoundary {
                    market_id: &market,
                    condition_id: &condition,
                    token_id: &token,
                    recovery_id,
                    cursor,
                    received_at: received,
                },
            )?;
            self.epochs.insert(
                token.clone(),
                snapshot["canonical_payload"]["book_epoch"]
                    .as_str()
                    .context("REST epoch")?
                    .to_owned(),
            );
            self.sequences.insert(token.clone(), 1);
            output.push(snapshot);
            if let Some(attempt) = attempt.cloned() {
                self.recoveries.remove(&token);
                let payload = json!({"recovery_scope":"token","token_id":token,"recovery_id":attempt,
                    "snapshot_cursor":cursor,"resolved_gap_token_ids":[token]});
                output.push(recovery_event(
                    &market,
                    &condition,
                    &token,
                    after
                        .checked_add(output.len() as u64 + 1)
                        .context("cursor overflow")?,
                    &wire_time(received)?,
                    "recovery_completed",
                    payload,
                    vec![],
                ));
            }
        }
        Ok(output)
    }
}
fn recovery_event(
    market: &str,
    condition: &str,
    token: &str,
    cursor: u64,
    at: &str,
    kind: &str,
    payload: Value,
    gaps: Vec<Value>,
) -> Value {
    // Recovery is a producer control record, never a fabricated upstream book.
    let raw = json!({"recovery_scope":"token","reason":payload.get("reason"),"recovery_id":payload["recovery_id"]});
    let mut event = json!({"contract_version":"marketcow.prediction_market.v1","schema_version":"marketcow.polymarket.live.v2",
        "cursor":cursor,"event_type":kind,"market_id":market,"condition_id":condition,"token_id":token,
        "book_epoch":null,"sequence":null,"exchange_at":at,"received_at":at,
        "canonical_payload_sha256":canonical_hash(&payload),"canonical_payload":payload,
        "raw_payload_sha256":canonical_hash(&raw),"raw_payload":raw,"applied":true,"fail_closed_reason":null,"gaps":gaps});
    event["event_id"] = json!(canonical_hash(&event));
    event
}
fn wire_time(value: chrono::DateTime<Utc>) -> Result<String> {
    let value = value
        .with_nanosecond(value.nanosecond() / 1000 * 1000)
        .context("timestamp")?;
    Ok(value.to_rfc3339_opts(
        if value.nanosecond() == 0 {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        },
        true,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targeted_confirmation_retry_stays_inside_freshness_budget() {
        assert_eq!(
            targeted_confirmation_retry_delay(1),
            std::time::Duration::from_secs(1),
        );
        assert_eq!(
            targeted_confirmation_retry_delay(0),
            std::time::Duration::from_secs(1),
        );
        assert_eq!(recovery_retry_delay(1), std::time::Duration::from_secs(5));
        assert_eq!(recovery_retry_delay(2), std::time::Duration::from_secs(10));
        assert_eq!(recovery_retry_delay(9), std::time::Duration::from_secs(60));
        assert_eq!(audit_batch_offset(300, 0, 5), std::time::Duration::ZERO);
        assert_eq!(audit_batch_offset(300, 1, 5), std::time::Duration::from_secs(60));
        assert_eq!(audit_batch_offset(300, 4, 5), std::time::Duration::from_secs(240));
    }

    #[tokio::test]
    async fn targeted_snapshot_scope_is_nonempty_and_unique_before_network_io() {
        assert!(
            recover_snapshots(Vec::new())
                .await
                .unwrap_err()
                .to_string()
                .contains("empty")
        );
        assert!(
            recover_snapshots(vec!["11".into(), "11".into()])
                .await
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );
    }

    #[test]
    fn lifecycle_lookup_uses_the_exact_failed_catalog_identity() {
        let market = catalog_market(&json!({
            "identity": {
                "market_id": "3418199",
                "condition_id": "0xcondition",
                "outcomes": [{"token_id": "yes"}, {"token_id": "no"}]
            }
        }))
        .unwrap();
        assert_eq!(market.market_id, "3418199");
        assert_eq!(market.condition_id, "0xcondition");
        assert_eq!(market.token_ids, ["yes", "no"]);
        assert!(catalog_market(&json!({
            "identity": {
                "market_id": "1",
                "condition_id": "c",
                "outcomes": [{"token_id": "only-one"}]
            }
        }))
        .is_err());
    }

    #[tokio::test]
    async fn disabled_confirmation_finishes_without_network_io() {
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let (_stop, shutdown) = tokio::sync::watch::channel(false);
        let (_membership_sender, membership) = tokio::sync::watch::channel(
            BTreeSet::from(["1".to_owned()]),
        );
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            run_confirmations(
                vec![super::super::Market {
                    market_id: "1".into(),
                    condition_id: "condition".into(),
                    token_ids: ["11".into(), "12".into()],
                }],
                1,
                1,
                1,
                1,
                0,
                sender,
                shutdown,
                Arc::new(tokio::sync::Semaphore::new(1)),
                membership,
            ),
        )
        .await;
        assert_eq!(result.unwrap().unwrap(), ());
    }
    fn fixture() -> Adapter {
        let plan = Plan {
            schema_version: "test".into(),
            catalog_revision: "catalog".into(),
            markets: vec![super::super::Market {
                market_id: "1".into(),
                condition_id: "condition".into(),
                token_ids: ["11".into(), "12".into()],
            }],
        };
        Adapter::new(&plan,&json!({"scope_id":"scope","markets":[{"identity":{"market_id":"1"},"lifecycle_state":"active"}],"books":[{"token_id":"11","tick_size":"0.01"},{"token_id":"12","tick_size":"0.01"}]})).unwrap()
    }
    fn book(token: &str, now: chrono::DateTime<Utc>) -> Value {
        json!({"event_type":"book","asset_id":token,"market":"condition","timestamp":now.timestamp_millis().to_string(),"hash":"upstream","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]})
    }
    #[test]
    fn missing_initial_tick_is_local_until_authoritative_snapshot() {
        let plan = Plan { schema_version: "test".into(), catalog_revision: "catalog".into(),
            markets: vec![super::super::Market { market_id:"1".into(), condition_id:"condition".into(),
                token_ids:["11".into(),"12".into()] }] };
        let mut a = Adapter::new(&plan, &json!({"scope_id":"scope",
            "markets":[{"identity":{"market_id":"1"},"lifecycle_state":"active"}],
            "books":[{"token_id":"12","tick_size":"0.01"}]})).unwrap();
        let now = Utc::now();
        let failed = a.apply_isolated(book("11", now), now, 0).unwrap();
        assert_eq!(failed[0]["event_type"], "recovery_started");
        assert!(!a.ticks.contains_key("11"));
        assert!(a.waiting.contains("11"));
        let healthy = a.apply_isolated(book("12", now), now, failed.len() as u64).unwrap();
        assert_eq!(healthy[0]["event_type"], "book");
        assert!(!a.waiting.contains("12"));
        assert!(a.waiting.contains("11"));
        let mut authoritative = book("11", now);
        authoritative["tick_size"] = json!("0.01");
        let recovered = a.apply_isolated(authoritative, now, (failed.len()+healthy.len()) as u64).unwrap();
        assert!(recovered.iter().any(|event| event["event_type"] == "book"));
        assert!(a.ticks.contains_key("11"));
        assert!(!a.waiting.contains("11"));
    }
    #[test]
    fn authoritative_snapshot_delta_and_reconnect_require_new_baseline() {
        let mut a = fixture();
        let now = Utc::now();
        let first = a.apply(book("11", now), now, 0).unwrap();
        assert_eq!(first[0]["cursor"], 1);
        assert!(!a.waiting.is_empty());
        a.apply(book("12", now), now, 1).unwrap();
        assert!(a.waiting.is_empty());
        let delta = json!({"event_type":"price_change","market":"condition","timestamp":now.timestamp_millis().to_string(),"price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"7","best_bid":"0.4","best_ask":"0.6"}]});
        let result = a.apply(delta.clone(), now, 2).unwrap();
        assert_eq!(result[0]["cursor"], 3);
        assert_eq!(result[0]["canonical_payload"]["bids"][0]["size"], "7");
        assert_eq!(result[0]["raw_payload"], delta);
        let tokens = [("11".into(), "1".into()), ("12".into(), "1".into())].into();
        marketcow_runtime::discovery_source::ValidatedSourceBatch::new(
            result, false, 2, &tokens, 65536,
        )
        .unwrap();
        a.apply(json!({"event_type":"source_gap","asset_id":"11"}), now, 3)
            .unwrap();
        assert!(!a.waiting.is_empty());
        assert!(a.apply(delta, now, 3).unwrap().is_empty());
        a.apply(book("11", now), now, 3).unwrap();
        assert!(a.waiting.is_empty());
    }

    #[test]
    fn repeated_connection_gap_keeps_one_recovery_identity() {
        let mut adapter = fixture();
        let now = Utc::now();
        adapter.apply_isolated(book("11", now), now, 0).unwrap();
        let first = adapter
            .apply_isolated(
                json!({"event_type":"source_gap","asset_id":"11"}),
                now,
                1,
            )
            .unwrap();
        assert_eq!(first.len(), 1);
        let recovery = adapter.recoveries["11"].clone();
        let duplicate = adapter
            .apply_isolated(
                json!({"event_type":"source_gap","asset_id":"11"}),
                now,
                2,
            )
            .unwrap();
        assert!(duplicate.is_empty());
        assert_eq!(adapter.recoveries["11"], recovery);
    }

    #[test]
    fn connection_gap_recovery_is_coalesced_by_market() {
        let mut adapter = fixture();
        let now = Utc::now();
        for (cursor, token) in [(0, "11"), (1, "12")] {
            adapter
                .apply_isolated(
                    json!({"event_type":"source_gap","asset_id":token}),
                    now,
                    cursor,
                )
                .unwrap();
        }
        let pending = pending_market_recoveries(&adapter);
        assert_eq!(pending.len(), 1);
        assert_eq!(
            pending["1"].keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["11", "12"]
        );
    }

    #[test]
    fn stale_recovery_for_terminal_market_is_not_an_active_scope_error() {
        let plan = Plan {
            schema_version: "test".into(),
            catalog_revision: "catalog".into(),
            markets: vec![
                super::super::Market {
                    market_id: "1".into(),
                    condition_id: "active-condition".into(),
                    token_ids: ["11".into(), "12".into()].into(),
                },
                super::super::Market {
                    market_id: "2".into(),
                    condition_id: "terminal-condition".into(),
                    token_ids: ["21".into(), "22".into()].into(),
                },
            ],
        };
        let seed = json!({
            "scope_id":"scope",
            "markets":[
                {"identity":{"market_id":"1"},"lifecycle_state":"active"},
                {"identity":{"market_id":"2","outcomes":[{"token_id":"21"},{"token_id":"22"}]},"lifecycle_state":"resolved"}
            ],
            "books":[
                {"token_id":"11","tick_size":"0.01"},
                {"token_id":"12","tick_size":"0.01"}
            ],
            "token_recoveries":[{"token_id":"21","market_id":"2","recovery_id":"old-terminal-recovery"}]
        });
        let adapter = Adapter::new(&plan, &seed).unwrap();
        assert_eq!(adapter.identities.len(), 2);
        assert!(adapter.recoveries.is_empty());
    }
    #[test]
    fn complete_book_last_trade_survives_the_ws_reducer_and_later_price_changes() {
        let mut a = fixture();
        let now = Utc::now();
        let now = now.with_nanosecond(now.nanosecond() / 1000 * 1000).unwrap();
        let mut snapshot = book("11", now);
        snapshot["tick_size"] = json!("0.01");
        snapshot["last_trade_price"] = json!("0.41");
        let rest = snapshot_event(
            &snapshot,
            SnapshotBoundary {
                market_id: "1",
                condition_id: "condition",
                token_id: "11",
                recovery_id: "comparison",
                cursor: 1,
                received_at: now,
            },
        )
        .unwrap();
        let first = a.apply(snapshot, now, 0).unwrap();
        assert_eq!(first[0]["canonical_payload"]["last_trade_price"], "0.41");
        assert_eq!(
            first[0]["canonical_payload"]["last_trade_price"],
            rest["canonical_payload"]["last_trade_price"]
        );
        assert_eq!(
            first[0]["canonical_payload"]["state_checksum"],
            rest["canonical_payload"]["state_checksum"]
        );
        let delta = json!({"event_type":"price_change","market":"condition",
            "timestamp":now.timestamp_millis().to_string(),
            "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"7",
                "best_bid":"0.4","best_ask":"0.6"}]});
        let second = a.apply(delta, now, 1).unwrap();
        assert_eq!(second[0]["canonical_payload"]["last_trade_price"], "0.41");

        let mut empty = fixture();
        let mut empty_snapshot = book("11", now);
        empty_snapshot["last_trade_price"] = json!("");
        let event = empty.apply(empty_snapshot, now, 0).unwrap();
        assert!(event[0]["canonical_payload"]["last_trade_price"].is_null());

        let mut invalid = fixture();
        let mut invalid_snapshot = book("11", now);
        invalid_snapshot["last_trade_price"] = json!("not-a-decimal");
        assert!(invalid.apply(invalid_snapshot, now, 0).is_err());
    }
    #[test]
    fn exact_replay_does_not_advance_or_refresh_and_reconnect_reseeds() {
        let mut a = fixture();
        let now = Utc::now();
        let raw = book("11", now);
        a.apply(raw.clone(), now, 0).unwrap();
        let epoch = a.epochs["11"].clone();
        let later = now + chrono::Duration::seconds(1);
        assert!(a.apply(raw.clone(), later, 1).unwrap().is_empty());
        assert_eq!(a.book_received["11"], now);
        assert_eq!(a.sequences["11"], 1);
        assert_eq!(a.engines["11"].projection().cursor, 1);
        assert_eq!(a.epochs["11"], epoch);
        a.apply(json!({"event_type":"source_gap","asset_id":"11"}), later, 1)
            .unwrap();
        let fresh = a.apply(raw, later, 1).unwrap();
        assert_eq!(fresh[0]["cursor"], 2);
        assert_ne!(a.epochs["11"], epoch);
    }
    #[test]
    fn delayed_delta_is_delivered_but_disconnect_still_requires_baseline() {
        let mut a = fixture();
        let now = Utc::now();
        a.apply(book("11", now), now, 0).unwrap();
        a.apply(book("12", now), now, 1).unwrap();
        let delta = json!({"event_type":"price_change","market":"condition",
            "timestamp":(now+chrono::Duration::milliseconds(1)).timestamp_millis().to_string(),
            "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"7","best_bid":"0.4","best_ask":"0.6"}]});
        let later = now + chrono::Duration::seconds(7);
        let output = a.apply(delta.clone(), later, 2).unwrap();
        assert_eq!(output[0]["applied"], true);
        assert_eq!(output[0]["raw_payload"], delta);
        assert!(a.recoveries.is_empty());
        a.reset_connection();
        assert_eq!(a.waiting.len(), 2);
        assert!(a.engines.is_empty());
        assert!(a.book_received.is_empty());
        // Old queued increments cannot establish the new connection baseline.
        assert!(a.apply(delta, later, 2).unwrap().is_empty());
        let recovered = a.apply(book("11", later), later, 2).unwrap();
        assert_eq!(recovered[0]["cursor"], 3);
        assert!(!a.waiting.is_empty());
        let recovered = a.apply(book("12", later), later, 3).unwrap();
        assert_eq!(recovered[0]["cursor"], 4);
        assert!(a.waiting.is_empty());
    }
    #[test]
    fn delayed_delivery_preserves_identity_checks() {
        let mut a = fixture();
        let now = Utc::now();
        for _ in 0..3 {
            a.apply(book("11", now), now, 0).unwrap();
            let delta = json!({"event_type":"price_change","market":"condition",
                "timestamp":(now+chrono::Duration::milliseconds(1)).timestamp_millis().to_string(),
                "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"7","best_bid":"0.4","best_ask":"0.6"}]});
            let output = a
                .apply(delta, now + chrono::Duration::seconds(7), 1)
                .unwrap();
            assert_eq!(output[0]["applied"], true);
            a.reset_connection();
            assert_eq!(a.waiting.len(), 2);
        }
        let mut invalid = book("11", now);
        invalid["market"] = json!("wrong");
        let error = a.apply(invalid, now, 0).unwrap_err();
        assert!(error.downcast_ref::<ResyncRequired>().is_none());
    }
    #[test]
    fn isolated_rejection_preserves_other_token_and_recovery_is_ordered() {
        let mut a = fixture();
        let now = Utc::now();
        a.apply(book("11", now), now, 0).unwrap();
        a.apply(book("12", now), now, 1).unwrap();
        let epoch = a.epochs["12"].clone();
        let delta = json!({"event_type":"price_change","market":"wrong-condition",
            "timestamp":(now+chrono::Duration::milliseconds(1)).timestamp_millis().to_string(),
            "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"7"}]});
        let later = now + chrono::Duration::seconds(7);
        let failed = a.apply_isolated(delta.clone(), later, 2).unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0]["event_type"], "recovery_started");
        assert_eq!(failed[0]["token_id"], "11");
        assert_eq!(a.waiting, BTreeSet::from(["11".into()]));
        assert_eq!(a.epochs["12"], epoch);
        assert!(a.apply_isolated(delta, later, 3).unwrap().is_empty());
        let healthy = a.apply_isolated(book("12", later), later, 3).unwrap();
        assert_eq!(healthy[0]["cursor"], 4);
        let recovered = a.apply_isolated(book("11", later), later, 4).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0]["cursor"], 5);
        assert_eq!(recovered[1]["cursor"], 6);
        assert_eq!(recovered[1]["canonical_payload"]["snapshot_cursor"], 5);
        assert_eq!(
            recovered[1]["canonical_payload"]["recovery_id"],
            failed[0]["canonical_payload"]["recovery_id"]
        );
        assert!(a.recoveries.is_empty());
        let tokens = [("11".into(), "1".into()), ("12".into(), "1".into())].into();
        marketcow_runtime::discovery_source::ValidatedSourceBatch::new(
            failed, false, 2, &tokens, 65536,
        )
        .unwrap();
        marketcow_runtime::discovery_source::ValidatedSourceBatch::new(
            recovered, false, 4, &tokens, 65536,
        )
        .unwrap();
    }

    #[test]
    fn exact_rest_book_can_win_fenced_recovery_without_fabricating_raw_ws() {
        let mut adapter = fixture();
        let now = Utc::now().with_nanosecond(0).unwrap();
        adapter.apply(book("11", now), now, 0).unwrap();
        let started = adapter.invalidate("11", "test_gap", now, 2).unwrap();
        let attempt = started["canonical_payload"]["recovery_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut raw = book("11", now);
        raw.as_object_mut().unwrap().remove("event_type");
        raw["tick_size"] = json!("0.01");
        let mut sibling = book("12", now);
        sibling.as_object_mut().unwrap().remove("event_type");
        sibling["tick_size"] = json!("0.01");
        let recovered = adapter
            .apply_rest_recovery(
                vec![raw.clone(), sibling],
                now,
                3,
                &BTreeMap::from([("11".into(), attempt.clone())]),
            )
            .unwrap();
        assert_eq!(recovered.len(), 3);
        assert_eq!(recovered[0]["event_type"], "book");
        assert_eq!(recovered[0]["raw_payload"], raw);
        assert!(recovered[0]["raw_payload"].get("event_type").is_none());
        assert_eq!(recovered[1]["event_type"], "recovery_completed");
        assert_eq!(recovered[1]["canonical_payload"]["snapshot_cursor"], 4);
        assert_eq!(recovered[2]["token_id"], "12");
        let bindings = [("11".into(), "1".into()), ("12".into(), "1".into())].into();
        marketcow_runtime::discovery_source::ValidatedSourceBatch::new(
            recovered, false, 3, &bindings, 65536,
        )
        .unwrap();
        assert!(adapter.recoveries.get("11").is_none());
        assert!(
            adapter
                .apply_rest_recovery(vec![], now, 6, &BTreeMap::from([("11".into(), attempt)]),)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn malformed_atomic_frame_invalidates_its_tokens_without_partial_output() {
        let mut a = fixture();
        let now = Utc::now();
        a.apply(book("11", now), now, 0).unwrap();
        a.apply(book("12", now), now, 1).unwrap();
        let raw = json!({"event_type":"price_change","market":"condition","timestamp":now.timestamp_millis().to_string(),
            "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4","size":"7"},
                {"asset_id":"12","side":"BUY","price":"broken","size":"8"}]});
        let events = a.apply_isolated(raw, now, 2).unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|e| e["event_type"] == "recovery_started"));
        assert_eq!(a.waiting.len(), 2);
        assert!(a.engines.is_empty());
    }

    #[test]
    fn wrong_condition_fails_closed_and_timestamp_is_canonical() {
        let mut a = fixture();
        let now = Utc::now();
        let mut raw = book("11", now);
        raw["market"] = json!("wrong");
        assert!(a.apply(raw, now, 0).is_err());
        assert_eq!(
            wire_time(chrono::DateTime::from_timestamp(1700000000, 0).unwrap()).unwrap(),
            "2023-11-14T22:13:20Z"
        );
    }
}
pub async fn run(
    args: &Args,
    plan: &Plan,
    seed: Value,
    publication: &mut Publication,
    acquisition:Option<tokio::sync::mpsc::Receiver<super::source_scope_control::AcquisitionRequest>>,
) -> Result<()> {
    ensure!(
        args.cycles.is_none(),
        "--cycles is REST-only; use managed runtime limits for WebSocket"
    );
    let mut adapter = Adapter::new(plan, &seed)?;
    // A clean persisted boundary remains causally valid while the first live
    // connection establishes. Per-book freshness still fails closed until a
    // current WS snapshot or authoritative confirmation arrives. Creating a
    // durable gap for every token on every normal process start caused a
    // scope-wide recovery storm and made unrelated markets unavailable.
    // Actual disconnects are emitted by the transport, while pre-existing
    // durable recoveries retain their original IDs above and continue here.
    let confirmation_markets = plan
        .markets
        .iter()
        .filter(|market| {
            market
                .token_ids
                .iter()
                .all(|token| adapter.identities.contains_key(token))
        })
        .cloned()
        .collect();
    let lifecycle_markets = seed["markets"]
        .as_array()
        .context("market seed")?
        .iter()
        .map(|market| {
            Ok((
                market["identity"]["market_id"]
                    .as_str()
                    .context("seed market id")?
                    .to_owned(),
                market.clone(),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    run_connection(
        &mut adapter,
        publication,
        args.websocket_shard_tokens,
        args.websocket_recovery_concurrency,
        args.market_workers,
        args.websocket_confirmation_seconds,
        args.request_market_batch_size,
        args.concurrency,
        args.response_byte_limit,
        args.request_timeout_seconds,
        confirmation_markets,
        plan.markets.clone(),
        lifecycle_markets,
        acquisition,
        args.acquisition_token_budget.unwrap_or(2048),
        args.acquisition_socket_budget.unwrap_or(1024),
        args.acquisition_lease_required,
        args.acquisition_lease_capacity.unwrap_or(0),
        args.acquisition_lease_max_seconds.unwrap_or(0),
    )
    .await
}

/// A bounded, temporary subscription obtains one authoritative snapshot. Its
/// result carries the recovery attempt; the caller fences obsolete responses.
async fn recover_snapshots(
    tokens: Vec<String>,
) -> Result<Vec<marketcow_polymarket::RawTransportFrame>> {
    ensure!(!tokens.is_empty(), "target recovery token scope is empty");
    let expected: BTreeSet<_> = tokens.iter().cloned().collect();
    ensure!(
        expected.len() == tokens.len(),
        "duplicate target recovery token"
    );
    let (send, mut receive) = tokio::sync::mpsc::channel(1);
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let mut job = tokio::spawn(run_polymarket_transport(
        PolymarketTransportConfig::production(),
        tokens,
        send,
        shutdown,
    ));
    struct AbortOnDrop(tokio::task::AbortHandle);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _socket_guard = AbortOnDrop(job.abort_handle());
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut snapshots = BTreeMap::new();
        loop {
            tokio::select! {
                frames = receive.recv() => {
                    for frame in frames.context("target recovery closed")? {
                        let token = frame.raw_payload["asset_id"].as_str();
                        if frame.raw_payload["event_type"] == "book"
                            && token.is_some_and(|token| expected.contains(token))
                        {
                            snapshots.insert(token.unwrap().to_owned(), frame);
                            if snapshots.len() == expected.len() {
                                return Ok(snapshots.into_values().collect());
                            }
                        }
                    }
                }
                ended = &mut job => { ended??; anyhow::bail!("target recovery stopped"); }
            }
        }
    })
    .await
    .context("target recovery timeout")
    .and_then(|result| result);
    let _ = stop.send(true);
    // No detached socket after timeout, successful snapshot, or operator stop.
    if !job.is_finished() {
        job.abort();
        let _ = job.await;
    }
    result
}

type FreshnessResult = (
    super::Market,
    chrono::DateTime<Utc>,
    Result<Vec<marketcow_polymarket::RawTransportFrame>>,
);

type MarketRecoveryResult = (
    String,
    BTreeMap<String, String>,
    Result<Vec<marketcow_polymarket::RawTransportFrame>>,
);

fn catalog_market(record: &Value) -> Result<super::Market> {
    let identity = &record["identity"];
    let token_ids: Vec<String> = identity["outcomes"]
        .as_array()
        .context("lifecycle outcomes")?
        .iter()
        .map(|outcome| {
            outcome["token_id"]
                .as_str()
                .map(str::to_owned)
                .context("lifecycle token")
        })
        .collect::<Result<_>>()?;
    Ok(super::Market {
        market_id: identity["market_id"]
            .as_str()
            .context("lifecycle market id")?
            .to_owned(),
        condition_id: identity["condition_id"]
            .as_str()
            .context("lifecycle condition")?
            .to_owned(),
        token_ids: token_ids
            .try_into()
            .map_err(|_| anyhow::anyhow!("lifecycle market is not binary"))?,
    })
}

fn targeted_confirmation_retry_delay(confirmation_interval_seconds: u64) -> std::time::Duration {
    // A successful targeted snapshot refreshes the book at completion. Its
    // retry cooldown must remain inside the configured confirmation cadence
    // instead of consuming the entire five-second consumer freshness budget
    // before the next WS handshake even starts.
    std::time::Duration::from_secs(confirmation_interval_seconds.max(1))
}

fn recovery_retry_delay(failures: u32) -> std::time::Duration {
    let multiplier = 1_u64 << failures.saturating_sub(1).min(4);
    std::time::Duration::from_secs((5 * multiplier).min(60))
}

/// Fill the bounded targeted-confirmation worker set from a market-keyed
/// queue. The queue cannot exceed the explicit prepared scope because each
/// market owns at most one entry. Capacity pressure delays an item instead of
/// silently dropping it and letting an otherwise quiet book age out.
fn start_freshness_jobs(
    queued: &mut BTreeMap<String, super::Market>,
    pending: &mut BTreeSet<String>,
    jobs: &mut tokio::task::JoinSet<FreshnessResult>,
    maximum_jobs: usize,
) {
    while jobs.len() < maximum_jobs {
        let Some(market_id) = queued.keys().next().cloned() else {
            break;
        };
        let market = queued.remove(&market_id).expect("queued market exists");
        if !pending.insert(market_id) {
            continue;
        }
        let requested = Utc::now();
        jobs.spawn(async move {
            let result = recover_snapshots(market.token_ids.to_vec()).await;
            (market, requested, result)
        });
    }
}

/// Bound reconnect blast radius without splitting the two books of one market.
/// Keeping market pairs together preserves the market-atomic reducer contract
/// even when one upstream connection is recycled independently of its peers.
fn connection_shards(
    identities: &BTreeMap<String, (String, String)>,
    maximum_tokens: usize,
) -> Result<Vec<Vec<String>>> {
    ensure!(
        maximum_tokens >= 2,
        "WS shard must hold a complete market pair"
    );
    let mut by_market = BTreeMap::<String, Vec<String>>::new();
    for (token, (market, _)) in identities {
        by_market
            .entry(market.clone())
            .or_default()
            .push(token.clone());
    }
    ensure!(
        by_market.values().all(|tokens| tokens.len() == 2),
        "each WS market must contain exactly two token books"
    );
    let mut shards = Vec::new();
    let mut current = Vec::new();
    for mut pair in by_market.into_values() {
        pair.sort();
        if !current.is_empty() && current.len() + pair.len() > maximum_tokens {
            shards.push(std::mem::take(&mut current));
        }
        current.extend(pair);
    }
    if !current.is_empty() {
        shards.push(current);
    }
    Ok(shards)
}

fn eligible_markets(adapter:&Adapter)->BTreeSet<String> {
    adapter.identities.values().map(|(market,_)|market.clone()).collect()
}
fn eligible_pairs(adapter:&Adapter)->Result<Vec<[String;2]>> {
    let mut pairs=BTreeMap::<String,Vec<String>>::new();
    for (token,(market,_)) in &adapter.identities {pairs.entry(market.clone()).or_default().push(token.clone());}
    pairs.into_values().map(|mut pair|{pair.sort();pair.try_into().map_err(|_|anyhow::anyhow!("eligible acquisition market pair"))}).collect()
}
fn pause_acquisition(adapter:&mut Adapter,pipeline:&pipeline::Pipeline,publication:&Publication,jobs:&mut super::source_acquisition_shards::Shards)->Result<()> {
    publication.pause_source()?;pipeline.pause_all(adapter)?;
    let tokens=jobs.tokens();if !tokens.is_empty(){let _=jobs.request_retire(&tokens)?;}
    publication.set_acquisition_statistics(jobs.statistics());Ok(())
}
fn lease_status(leases:&super::source_acquisition_lease::Leases,adapter:&Adapter,publication:&Publication,now:std::time::Instant)->Value {
    let mut value=leases.status(&eligible_markets(adapter),now);
    value["source_ready"]=json!(publication.is_source_ready());value
}

struct ConfirmationBatch {
    markets: Vec<super::Market>,
    request_started_at: chrono::DateTime<Utc>,
    result: Result<(Vec<Value>, chrono::DateTime<Utc>)>,
}

fn audit_batch_offset(period_seconds: u64, index: usize, batches: usize) -> std::time::Duration {
    if period_seconds == 0 || batches <= 1 || index == 0 {
        return std::time::Duration::ZERO;
    }
    let period_millis = u128::from(period_seconds) * 1_000;
    let offset_millis = period_millis * index as u128 / batches as u128;
    std::time::Duration::from_millis(offset_millis.min(u128::from(u64::MAX)) as u64)
}

async fn run_confirmations(
    markets: Vec<super::Market>,
    market_batch_size: usize,
    concurrency: usize,
    response_byte_limit: usize,
    request_timeout_seconds: u64,
    interval_seconds: u64,
    sender: tokio::sync::mpsc::Sender<ConfirmationBatch>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    network:Arc<tokio::sync::Semaphore>,
    membership:tokio::sync::watch::Receiver<BTreeSet<String>>,
) -> Result<()> {
    // Healthy WebSocket operation must not imply a parallel full-book REST
    // downloader. Zero is the production default and performs no network I/O.
    if interval_seconds == 0 {
        return Ok(());
    }
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(request_timeout_seconds))
        .build()?;
    ensure!(concurrency>0,"confirmation concurrency");
    let mut jobs = tokio::task::JoinSet::new();
    let batch_count = markets.len().div_ceil(market_batch_size);
    for (batch_index, batch) in markets.chunks(market_batch_size).enumerate() {
        let batch = batch.to_vec();
        let client = client.clone();
        let network = network.clone();
        let sender = sender.clone();
        let mut shutdown = shutdown.clone();
        let mut membership=membership.clone();
        jobs.spawn(async move {
            let offset = audit_batch_offset(interval_seconds, batch_index, batch_count);
            if !offset.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(offset) => {},
                    value = shutdown.changed() => {
                        value?;
                        if *shutdown.borrow() { return Ok(()); }
                    },
                }
            }
            loop {
                if *shutdown.borrow() {
                    return Ok::<(), anyhow::Error>(());
                }
                let batch:Vec<_>=batch.iter().filter(|market|membership.borrow().contains(&market.market_id)).cloned().collect();
                if batch.is_empty(){
                    tokio::select! {
                        value=membership.changed()=>{value?;continue;},
                        value=shutdown.changed()=>{value?;if *shutdown.borrow(){return Ok(());}},
                    }
                    continue;
                }
                let cycle_started = tokio::time::Instant::now();
                let permit = tokio::select! {
                    permit = network.clone().acquire_owned() => permit?,
                    value = shutdown.changed() => {
                        value?;
                        if *shutdown.borrow() { return Ok(()); }
                        continue;
                    }
                };
                let request_started_at = Utc::now();
                let result = super::fetch(client.clone(), &batch, response_byte_limit).await;
                drop(permit);
                sender
                    .send(ConfirmationBatch {
                        markets: batch.clone(),
                        request_started_at,
                        result,
                    })
                    .await
                    .context("confirmation consumer stopped")?;
                tokio::select! {
                    _ = tokio::time::sleep_until(
                        cycle_started + std::time::Duration::from_secs(interval_seconds)
                    ) => {},
                    value = shutdown.changed() => {
                        value?;
                        if *shutdown.borrow() { return Ok(()); }
                    },
                }
            }
        });
    }
    drop(sender);
    while let Some(result) = jobs.join_next().await {
        result??;
    }
    Ok(())
}

fn pending_market_recoveries(adapter: &Adapter) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut pending = BTreeMap::<String, BTreeMap<String, String>>::new();
    for (token, attempt) in &adapter.recoveries {
        let Some((market_id, _)) = adapter.identities.get(token) else {
            continue;
        };
        pending
            .entry(market_id.clone())
            .or_default()
            .insert(token.clone(), attempt.clone());
    }
    pending
}

async fn run_connection(
    adapter: &mut Adapter,
    publication: &mut Publication,
    websocket_shard_tokens: usize,
    websocket_recovery_concurrency: usize,
    market_workers: usize,
    websocket_confirmation_seconds: u64,
    request_market_batch_size: usize,
    network_concurrency: usize,
    response_byte_limit: usize,
    request_timeout_seconds: u64,
    mut confirmation_markets: Vec<super::Market>,
    lifecycle_candidates: Vec<super::Market>,
    mut lifecycle_markets: BTreeMap<String, Value>,
    mut acquisition:Option<tokio::sync::mpsc::Receiver<super::source_scope_control::AcquisitionRequest>>,
    acquisition_token_budget:usize,
    acquisition_socket_budget:usize,
    acquisition_lease_required:bool,
    acquisition_lease_capacity:usize,
    acquisition_lease_max_seconds:u64,
) -> Result<()> {
    let (mut pipeline, mut completed_markets) = pipeline::Pipeline::start_with_workers(adapter, market_workers)?;
    let mut acquisition_tokens:BTreeSet<_>=adapter.identities.keys().cloned().collect();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let mut jobs=super::source_acquisition_shards::Shards::new(PolymarketTransportConfig::production(),
        sender.clone(),acquisition_token_budget,acquisition_socket_budget,websocket_shard_tokens)?;
    let mut initial_pairs=BTreeMap::<String,Vec<String>>::new();
    for (token,(market,_)) in &adapter.identities {initial_pairs.entry(market.clone()).or_default().push(token.clone());}
    let initial_pairs:Vec<[String;2]>=initial_pairs.into_values().map(|pair|pair.try_into()
        .map_err(|_|anyhow::anyhow!("initial acquisition market pair"))).collect::<Result<_>>()?;
    let mut leases=super::source_acquisition_lease::Leases::new(acquisition_lease_required,
        acquisition_lease_capacity,std::time::Duration::from_secs(acquisition_lease_max_seconds))?;
    if leases.enabled(){jobs.add_pairs(&initial_pairs)?;}else{pause_acquisition(adapter,&pipeline,publication,&mut jobs)?;}
    publication.set_acquisition_statistics(jobs.statistics());
    drop(sender);
    let (confirmation_sender, mut confirmation_receiver) =
        tokio::sync::mpsc::channel(network_concurrency);
    let confirmation_shutdown = shutdown.clone();
    let confirmation_network=Arc::new(tokio::sync::Semaphore::new(network_concurrency));
    let (confirmation_membership,confirmation_members)=tokio::sync::watch::channel(
        if leases.enabled(){confirmation_markets.iter().map(|market|market.market_id.clone()).collect::<BTreeSet<_>>()}
        else{BTreeSet::new()});
    let mut added_confirmation_jobs=tokio::task::JoinSet::new();
    let confirmation_job = tokio::spawn(run_confirmations(
        confirmation_markets.clone(),
        request_market_batch_size,
        network_concurrency,
        response_byte_limit,
        request_timeout_seconds,
        websocket_confirmation_seconds,
        confirmation_sender.clone(),
        confirmation_shutdown,
        confirmation_network.clone(),
        confirmation_members.clone(),
    ));
    let lifecycle_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(request_timeout_seconds))
        .build()?;
    let lifecycle_network = Arc::new(tokio::sync::Semaphore::new(network_concurrency));
    let mut lifecycle_jobs = tokio::task::JoinSet::<(super::Market, Result<Option<Value>>)>::new();
    let mut lifecycle_pending = BTreeSet::new();
    // The durable lifecycle overlay is authoritative across process restarts.
    // Seed the runtime fence from it instead of waiting for another Gamma read.
    let mut lifecycle_terminal: BTreeSet<String> = lifecycle_markets
        .iter()
        .filter(|(_, market)| {
            matches!(
                market["lifecycle_state"].as_str(),
                Some("closed" | "resolved" | "invalid")
            )
        })
        .map(|(market, _)| market.clone())
        .collect();
    let mut lifecycle_retry_at = BTreeMap::<String, tokio::time::Instant>::new();
    // Terminal publication fences reducers immediately. Wire unsubscribe is
    // reconciled independently because its receipt can be delayed by another
    // bounded shard command. One entry per market keeps this state bounded.
    let mut terminal_unsubscribes = BTreeMap::<String, BTreeSet<String>>::new();
    let mut recovery_jobs = tokio::task::JoinSet::<MarketRecoveryResult>::new();
    let mut freshness_jobs = tokio::task::JoinSet::<FreshnessResult>::new();
    let mut recovering_markets = BTreeSet::new();
    let mut freshness_pending = BTreeSet::new();
    let mut freshness_queued = BTreeMap::<String, super::Market>::new();
    let mut freshness_retry_at = BTreeMap::<String, tokio::time::Instant>::new();
    let mut rest_submitted = BTreeMap::<String, String>::new();
    let mut recovery_retry_at = BTreeMap::<String, tokio::time::Instant>::new();
    let mut recovery_failures = BTreeMap::<String, u32>::new();
    let mut retry_tick = tokio::time::interval(std::time::Duration::from_secs(1));
    retry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let shutdown_requested = super::shutdown_signal();
    tokio::pin!(shutdown_requested);
    // A copied generation can already contain lifecycle-only terminal rows
    // from an earlier run. Re-observe only those bounded plan members whose
    // payout remained unresolved; never rescan the catalog.
    for market in if leases.enabled(){lifecycle_candidates}else{Vec::new()} {
        let seed_market = lifecycle_markets
            .get(&market.market_id)
            .context("lifecycle candidate missing from seed")?;
        let terminal = matches!(
            seed_market["lifecycle_state"].as_str(),
            Some("closed" | "resolved" | "invalid")
        );
        if terminal && seed_market["resolution"].is_null() {
            lifecycle_pending.insert(market.market_id.clone());
            let client = lifecycle_client.clone();
            let network = lifecycle_network.clone();
            lifecycle_jobs.spawn(async move {
                let _permit = network.acquire_owned().await;
                let result =
                    super::source_lifecycle::refresh(client, market.clone(), response_byte_limit)
                        .await;
                (market, result)
            });
        }
    }
    publication.set_source_ready(leases.enabled());
    let result:Result<()>=async {
        loop {
            let frames=tokio::select! {
                request=async {match acquisition.as_mut(){Some(rx)=>rx.recv().await,None=>std::future::pending().await}}=>{
                    let Some(request)=request else {acquisition=None;continue;};
                    let result=(||->Result<Value>{
                        use super::source_scope_control::AcquisitionAction;
                        let command_now=std::time::Instant::now();
                        if leases.required()&&leases.expire(command_now)>0&&!leases.enabled(){
                            confirmation_membership.send_replace(BTreeSet::new());
                            pause_acquisition(adapter,&pipeline,publication,&mut jobs)?;
                        }
                        let request=match request.action {
                            AcquisitionAction::LeaseStatus=>return Ok(lease_status(&leases,adapter,publication,command_now)),
                            AcquisitionAction::AcquireLease{lease_id,ttl_seconds,market_ids}=>{
                                let now=std::time::Instant::now();let eligible=eligible_markets(adapter);let was_enabled=leases.enabled();
                                leases.acquire(lease_id,std::time::Duration::from_secs(ttl_seconds),market_ids,&eligible,now)?;
                                if !was_enabled&&jobs.tokens().is_empty() {
                                    jobs.add_pairs(&eligible_pairs(adapter)?)?;
                                    confirmation_membership.send_replace(eligible.clone());
                                    publication.set_acquisition_statistics(jobs.statistics());
                                }
                                if !was_enabled {
                                    for record in lifecycle_markets.values().filter(|record|matches!(record["lifecycle_state"].as_str(),
                                        Some("closed"|"resolved"|"invalid"))&&record["resolution"].is_null()) {
                                        let market=catalog_market(record)?;
                                        if !lifecycle_pending.insert(market.market_id.clone()){continue;}
                                        let client=lifecycle_client.clone();let network=lifecycle_network.clone();
                                        lifecycle_jobs.spawn(async move {let _permit=network.acquire_owned().await;
                                            let result=super::source_lifecycle::refresh(client,market.clone(),response_byte_limit).await;
                                            (market,result)});
                                    }
                                }
                                return Ok(lease_status(&leases,adapter,publication,now));
                            },
                            AcquisitionAction::RenewLease{lease_id,ttl_seconds}=>{
                                let now=std::time::Instant::now();leases.renew(&lease_id,std::time::Duration::from_secs(ttl_seconds),now)?;
                                return Ok(lease_status(&leases,adapter,publication,now));
                            },
                            AcquisitionAction::ReleaseLease{lease_id}=>{
                                let now=std::time::Instant::now();leases.release(&lease_id)?;
                                if !leases.enabled(){
                                    confirmation_membership.send_replace(BTreeSet::new());
                                    pause_acquisition(adapter,&pipeline,publication,&mut jobs)?;
                                }
                                return Ok(lease_status(&leases,adapter,publication,now));
                            },
                            AcquisitionAction::Scope{validate_only,catalog_revision,records,evidence_sha256,
                                acquisition_market_ids,retire_market_ids}=>ScopeAcquisition{validate_only,catalog_revision,
                                    records,evidence_sha256,acquisition_market_ids,retire_market_ids},
                        };
                        if !request.retire_market_ids.is_empty() {
                            let removed=&request.retire_market_ids;
                            publication.check_retirement(removed.clone())?;
                            if request.validate_only{return Ok(json!({"retirement_validated":true,"publication_applied":false}));}
                            let retired_tokens:BTreeSet<_>=adapter.identities.iter().filter(|(_, (market,_))|removed.contains(market)).map(|(t,_)|t.clone()).collect();
                            if !retired_tokens.is_empty(){jobs.begin_retire(&retired_tokens)?;}
                            for market in removed {
                                if pipeline.has_market(market){pipeline.remove_market(adapter,market)?;}
                                lifecycle_markets.remove(market);lifecycle_pending.remove(market);lifecycle_terminal.remove(market);
                                lifecycle_retry_at.remove(market);freshness_queued.remove(market);freshness_pending.remove(market);freshness_retry_at.remove(market);
                            }
                            confirmation_markets.retain(|m|!removed.contains(&m.market_id));
                            confirmation_membership.send_replace(confirmation_markets.iter().map(|m|m.market_id.clone()).collect());
                            acquisition_tokens=adapter.identities.keys().cloned().collect();
                            publication.retire_catalog_markets(removed.clone())?;
                            publication.set_acquisition_statistics(jobs.statistics());
                            return Ok(json!({"retirement_queued":true,"retired_market_ids":removed,
                                "source_cursor":publication.cursor()?,"wire_unsubscribe_confirmed":false}));
                        }
                        ensure!(!request.records.is_empty()&&request.records.len()<=4096,"acquisition record budget");
                        let mut ids=BTreeSet::new();let mut added=vec![];
                        let subscribed=jobs.tokens();
                        for record in &request.records {
                            let identity=&record["identity"];
                            let id=identity["market_id"].as_str().context("acquisition market id")?;
                            ensure!(ids.insert(id.to_owned()),"duplicate acquisition market");
                            if record["lifecycle_state"]!="active" || !request.acquisition_market_ids.contains(id) {continue;}
                            let tokens:Vec<String>=identity["outcomes"].as_array().context("acquisition outcomes")?.iter()
                                .map(|outcome|outcome["token_id"].as_str().map(str::to_owned).context("acquisition token")).collect::<Result<_>>()?;
                            let market=super::Market{market_id:id.into(),condition_id:identity["condition_id"].as_str().context("acquisition condition")?.into(),
                                token_ids:tokens.try_into().map_err(|_|anyhow::anyhow!("acquisition binary pair"))?};
                            let actor_absent=market.token_ids.iter().all(|t|!adapter.identities.contains_key(t));
                            if !actor_absent {ensure!(market.token_ids.iter().all(|t|adapter.identities.get(t)==Some(&(market.market_id.clone(),market.condition_id.clone()))),
                                "retained acquisition identity differs");}
                            if market.token_ids.iter().all(|t|!subscribed.contains(t)){added.push(market);}
                            else {ensure!(!actor_absent&&market.token_ids.iter().all(|t|subscribed.contains(t)),"acquisition retirement still pending");}
                        }
                        let pairs:Vec<_>=added.iter().map(|m|m.token_ids.clone()).collect();
                        jobs.validate_add_pairs(&pairs)?;
                        if request.validate_only {
                            publication.check_catalog_admission(&request.catalog_revision,request.records.clone(),&request.evidence_sha256,&request.acquisition_market_ids,acquisition_token_budget)?;
                            return Ok(json!({"acquisition_validated":true,"publication_applied":false}));
                        }
                        publication.admit_catalog_markets_scoped(&request.catalog_revision,request.records.clone(),&request.evidence_sha256,
                            &request.acquisition_market_ids,acquisition_token_budget)?;
                        for market in &added {pipeline.install_admitted_market(adapter,publication,market)?;}
                        if leases.enabled(){jobs.add_pairs(&pairs)?;}
                        publication.set_acquisition_statistics(jobs.statistics());
                        acquisition_tokens=adapter.identities.keys().cloned().collect();
                        for record in request.records {lifecycle_markets.insert(record["identity"]["market_id"].as_str().unwrap().to_owned(),record);}
                        let added_ids:Vec<_>=added.iter().map(|m|m.market_id.clone()).collect();
                        if !added.is_empty()&&leases.enabled() {
                            confirmation_markets.extend(added.clone());
                            confirmation_membership.send_replace(confirmation_markets.iter().map(|m|m.market_id.clone()).collect());
                            added_confirmation_jobs.spawn(run_confirmations(added,request_market_batch_size,network_concurrency,
                                response_byte_limit,request_timeout_seconds,websocket_confirmation_seconds,
                                confirmation_sender.clone(),shutdown.clone(),confirmation_network.clone(),confirmation_members.clone()));
                        }
                        Ok(json!({"acquisition_installed":true,"added_market_ids":added_ids,
                            "requested_market_ids":ids,"source_cursor":publication.cursor()?,
                            "books_all_ready_required":false,"publication_applied":false}))
                    })();
                    let _=request.receipt.send(result);continue;
                },
                result=added_confirmation_jobs.join_next(),if !added_confirmation_jobs.is_empty()=>{
                    result.context("added confirmation task missing")???;
                    continue;
                },
                value=receiver.recv()=>value.context("WS transport stopped")?,
                value=jobs.failure()=>{value?;anyhow::bail!("WS transport ended");},
                completed=completed_markets.recv()=>{
                    pipeline.publish(adapter, publication, completed.context("market workers stopped")?).await?;
                    continue;
                },
                confirmation=confirmation_receiver.recv()=>{
                    let confirmation = confirmation.context("REST confirmation stopped")?;
                    if !leases.enabled(){continue;}
                    match confirmation.result {
                        Err(error) => eprintln!("{}", json!({"stage":"rest_confirmation_request_failed","detail":error.to_string()})),
                        Ok((books, received_at)) => {
                            for market in confirmation.markets {
                                if !confirmation_membership.borrow().contains(&market.market_id){continue;}
                                for _ in 0..market_workers * 2 {
                                    match completed_markets.try_recv() {
                                        Ok(done) => pipeline.publish(adapter, publication, done).await?,
                                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => anyhow::bail!("market workers stopped"),
                                    }
                                }
                                tokio::task::yield_now().await;
                                let selected:Vec<_> = books.iter().filter(|book| market.token_ids.iter()
                                    .any(|token| book["asset_id"] == *token)).cloned().collect();
                                if selected.len() != 2 {
                                    eprintln!("{}",json!({"stage":"rest_confirmation_incomplete",
                                        "market_id":market.market_id,"returned_token_count":selected.len()}));
                                    let due = lifecycle_retry_at.get(&market.market_id)
                                        .is_none_or(|at| *at <= tokio::time::Instant::now());
                                    if !lifecycle_terminal.contains(&market.market_id)
                                        && !lifecycle_pending.contains(&market.market_id) && due
                                    {
                                        lifecycle_pending.insert(market.market_id.clone());
                                        let client = lifecycle_client.clone();
                                        let network = lifecycle_network.clone();
                                        lifecycle_jobs.spawn(async move {
                                            let _permit = network.acquire_owned().await;
                                            let result = super::source_lifecycle::refresh(
                                                client, market.clone(), response_byte_limit,
                                            ).await;
                                            (market, result)
                                        });
                                    }
                                    continue;
                                }
                                let pending:Vec<_> = market.token_ids.iter().filter_map(|token| adapter.recoveries.get(token)
                                    .map(|attempt|(token.clone(),attempt.clone()))).collect();
                                if !pending.is_empty() {
                                    if pending.iter().any(|(token,attempt)| rest_submitted.get(token) != Some(attempt)) {
                                        if pipeline.submit_rest_recovery(adapter, publication, selected, received_at,
                                            pending.clone()).await? {
                                            for (token,attempt) in pending { rest_submitted.insert(token, attempt); }
                                        }
                                    }
                                    continue;
                                }
                                match publication.confirm_market(&market.market_id, &market.condition_id, &market.token_ids,
                                    &selected, confirmation.request_started_at, received_at) {
                                    Ok(ConfirmationOutcome::Confirmed) => {},
                                    Ok(ConfirmationOutcome::Superseded) => {
                                        eprintln!("{}",json!({
                                            "stage":"rest_confirmation_superseded","market_id":market.market_id
                                        }));
                                        // REST and WS are independent official read
                                        // surfaces. If REST remains behind a newer WS
                                        // book, obtain a fresh complete pair from one
                                        // bounded temporary WS subscription instead of
                                        // aging out the market or overwriting newer state.
                                        let due = freshness_retry_at.get(&market.market_id)
                                            .is_none_or(|at| *at <= tokio::time::Instant::now());
                                        if !freshness_pending.contains(&market.market_id) && due {
                                            freshness_queued
                                                .insert(market.market_id.clone(), market.clone());
                                            start_freshness_jobs(
                                                &mut freshness_queued,
                                                &mut freshness_pending,
                                                &mut freshness_jobs,
                                                websocket_recovery_concurrency,
                                            );
                                        }
                                    },
                                    Ok(ConfirmationOutcome::Mismatch) => {
                                        eprintln!("{}",json!({"stage":"rest_confirmation_mismatch","market_id":market.market_id}));
                                        // REST and the main WS stream can expose different
                                        // current surfaces around the same update. REST is a
                                        // freshness witness, not permission to invalidate a
                                        // healthy WS-primary book. Resolve the divergence with
                                        // one bounded official WS full snapshot; only that
                                        // snapshot may atomically replace the pair.
                                        let due = freshness_retry_at.get(&market.market_id)
                                            .is_none_or(|at| *at <= tokio::time::Instant::now());
                                        if !freshness_pending.contains(&market.market_id) && due {
                                            freshness_queued
                                                .insert(market.market_id.clone(), market.clone());
                                            start_freshness_jobs(
                                                &mut freshness_queued,
                                                &mut freshness_pending,
                                                &mut freshness_jobs,
                                                websocket_recovery_concurrency,
                                            );
                                        }
                                    },
                                    Err(error) => eprintln!("{}",json!({"stage":"rest_confirmation_invalid","market_id":market.market_id,"detail":error.to_string()})),
                                }
                            }
                        }
                    }
                    continue;
                },
                completed=lifecycle_jobs.join_next(), if !lifecycle_jobs.is_empty()=>{
                    let (market, result) = completed.context("lifecycle task missing")??;
                    if !lifecycle_markets.contains_key(&market.market_id){continue;}
                    lifecycle_pending.remove(&market.market_id);
                    if !leases.enabled(){continue;}
                    lifecycle_retry_at.insert(
                        market.market_id.clone(),
                        tokio::time::Instant::now() + std::time::Duration::from_secs(30),
                    );
                    match result {
                        Ok(Some(evidence)) => {
                            let catalog_market = lifecycle_markets.get(&market.market_id)
                                .context("lifecycle catalog market missing")?;
                            let event = super::source_lifecycle::terminal_event(
                                catalog_market,
                                &evidence,
                                publication.cursor()?.checked_add(1).context("cursor overflow")?,
                            )?;
                            publication.publish_with_capacity_and_evidence(
                                vec![event], Some(evidence.clone()),
                            ).await?;
                            lifecycle_terminal.insert(market.market_id.clone());
                            let retired_tokens = if pipeline.has_market(&market.market_id) {
                                pipeline
                                    .remove_market(adapter, &market.market_id)?
                                    .into_iter()
                                    .collect::<BTreeSet<_>>()
                            } else {
                                BTreeSet::new()
                            };
                            if !retired_tokens.is_empty() {
                                acquisition_tokens.retain(|token| !retired_tokens.contains(token));
                                terminal_unsubscribes
                                    .insert(market.market_id.clone(), retired_tokens.clone());
                                recovery_retry_at.remove(&market.market_id);
                                recovery_failures.remove(&market.market_id);
                                recovering_markets.remove(&market.market_id);
                                freshness_queued.remove(&market.market_id);
                                freshness_pending.remove(&market.market_id);
                                freshness_retry_at.remove(&market.market_id);
                                confirmation_markets
                                    .retain(|item| item.market_id != market.market_id);
                                confirmation_membership.send_replace(
                                    confirmation_markets
                                        .iter()
                                        .map(|item| item.market_id.clone())
                                        .collect(),
                                );
                                // Local ownership is already fenced. A busy
                                // command slot is item-local and retried by the
                                // scheduler; it must not fail the whole source.
                                if let Err(error) = jobs.request_retire(&retired_tokens) {
                                    eprintln!("{}", json!({
                                        "stage":"lifecycle_wire_retirement_retry",
                                        "market_id":market.market_id,
                                        "detail":error.to_string()
                                    }));
                                }
                            }
                            eprintln!("{}",json!({"stage":"lifecycle_terminal_published",
                                "market_id":market.market_id,
                                "evidence_sha256":evidence["raw_response_sha256"],
                                "retired_token_count":retired_tokens.len(),
                                "wire_unsubscribe_pending":!retired_tokens.is_empty()}));
                        }
                        Ok(None) => eprintln!("{}",json!({"stage":"lifecycle_not_terminal",
                            "market_id":market.market_id})),
                        Err(error) => eprintln!("{}",json!({"stage":"lifecycle_refresh_error",
                            "market_id":market.market_id,"detail":format!("{error:#}")})),
                    }
                    continue;
                },
                completed=recovery_jobs.join_next(), if !recovery_jobs.is_empty()=>{
                    let (market_id, attempts, result) = completed.context("recovery task missing")??;
                    recovering_markets.remove(&market_id);
                    if !leases.enabled(){continue;}
                    // A lifecycle result can retire a market while a bounded
                    // recovery socket is still in flight. Its eventual result
                    // must not recreate retry state or submit a late snapshot.
                    if lifecycle_terminal.contains(&market_id) {
                        recovery_failures.remove(&market_id);
                        recovery_retry_at.remove(&market_id);
                        continue;
                    }
                    match result {
                        Ok(frames) => {
                            recovery_failures.remove(&market_id);
                            recovery_retry_at.remove(&market_id);
                            let mut by_token = frames.into_iter().filter_map(|frame| {
                                let token = frame.raw_payload["asset_id"].as_str()?.to_owned();
                                Some((token, frame))
                            }).collect::<BTreeMap<_, _>>();
                            for (token, attempt) in attempts {
                                if adapter.recoveries.get(&token) != Some(&attempt) { continue; }
                                match by_token.remove(&token) {
                                    Some(frame) => pipeline.submit(
                                        adapter, publication, frame, Some((token, attempt)),
                                    ).await?,
                                    None => eprintln!("{}", json!({"stage":"market_recovery_incomplete",
                                        "market_id":market_id,"token_id":token})),
                                }
                            }
                        }
                        Err(error) => {
                            let failures = recovery_failures.entry(market_id.clone()).or_default();
                            *failures = failures.saturating_add(1);
                            recovery_retry_at.insert(
                                market_id.clone(),
                                tokio::time::Instant::now() + recovery_retry_delay(*failures),
                            );
                            eprintln!("{}", json!({"stage":"market_recovery_retry",
                                "market_id":market_id,"token_count":attempts.len(),
                                "consecutive_failures":*failures,"detail":error.to_string()}));
                            // A market that disappears from the official book
                            // stream may have become terminal. Confirmation
                            // polling can be disabled, so a failed bounded WS
                            // recovery must independently trigger one bounded
                            // lifecycle lookup. Otherwise an expired member can
                            // remain in recovery forever and keep opening retry
                            // sockets. This lookup is event-driven, coalesced by
                            // market, and observes only the failed identity.
                            let due = lifecycle_retry_at
                                .get(&market_id)
                                .is_none_or(|at| *at <= tokio::time::Instant::now());
                            if !lifecycle_terminal.contains(&market_id)
                                && !lifecycle_pending.contains(&market_id)
                                && due
                            {
                                if let Some(record) = lifecycle_markets.get(&market_id) {
                                    let market = catalog_market(record)?;
                                    lifecycle_pending.insert(market_id.clone());
                                    let client = lifecycle_client.clone();
                                    let network = lifecycle_network.clone();
                                    lifecycle_jobs.spawn(async move {
                                        let _permit = network.acquire_owned().await;
                                        let result = super::source_lifecycle::refresh(
                                            client,
                                            market.clone(),
                                            response_byte_limit,
                                        )
                                        .await;
                                        (market, result)
                                    });
                                }
                            }
                        }
                    }
                    continue;
                },
                completed=freshness_jobs.join_next(), if !freshness_jobs.is_empty()=>{
                    let (market, requested, result) = completed.context("freshness task missing")??;
                    freshness_pending.remove(&market.market_id);
                    if !leases.enabled(){continue;}
                    // The terminal transition removes confirmation membership,
                    // but an already running task may complete afterward.
                    // Fence it before it can confirm or replace retired state.
                    if lifecycle_terminal.contains(&market.market_id)
                        || !confirmation_membership.borrow().contains(&market.market_id)
                    {
                        freshness_retry_at.remove(&market.market_id);
                        continue;
                    }
                    freshness_retry_at.insert(
                        market.market_id.clone(),
                        tokio::time::Instant::now()
                            + targeted_confirmation_retry_delay(
                                websocket_confirmation_seconds,
                            ),
                    );
                    start_freshness_jobs(
                        &mut freshness_queued,
                        &mut freshness_pending,
                        &mut freshness_jobs,
                        websocket_recovery_concurrency,
                    );
                    match result {
                        Err(error) => eprintln!("{}",json!({
                            "stage":"targeted_ws_confirmation_failed",
                            "market_id":market.market_id,"detail":error.to_string()
                        })),
                        Ok(frames) => {
                            let received = frames.iter().map(|frame| frame.received_at)
                                .max().context("targeted WS confirmation empty")?;
                            let books:Vec<_> = frames.into_iter()
                                .map(|frame| frame.raw_payload).collect();
                            if market.token_ids.iter().any(|token| adapter.recoveries.contains_key(token)) {
                                eprintln!("{}",json!({"stage":"targeted_ws_confirmation_superseded_by_recovery",
                                    "market_id":market.market_id}));
                                continue;
                            }
                            match publication.confirm_targeted_market(
                                &market.market_id, &market.condition_id, &market.token_ids,
                                &books, requested, received,
                            ) {
                                Ok(ConfirmationOutcome::Confirmed) => eprintln!("{}",json!({
                                    "stage":"targeted_ws_confirmation","market_id":market.market_id
                                })),
                                Ok(ConfirmationOutcome::Superseded) => eprintln!("{}",json!({
                                    "stage":"targeted_ws_confirmation_superseded","market_id":market.market_id
                                })),
                                Ok(ConfirmationOutcome::Mismatch) => {
                                    eprintln!("{}",json!({"stage":"targeted_ws_confirmation_replacement",
                                        "market_id":market.market_id}));
                                    if let Err(error) = pipeline.submit_rest_replacement(
                                        adapter, publication, books, received, &market.market_id,
                                        "targeted_ws_confirmation_mismatch",
                                    ).await {
                                        // A malformed/capacity-rejected recovery for one
                                        // market is item-local. Keep other markets and
                                        // shards live; this market remains stale and is
                                        // retried after its cooldown.
                                        eprintln!("{}",json!({
                                            "stage":"targeted_ws_confirmation_replacement_failed",
                                            "market_id":market.market_id,"detail":error.to_string()
                                        }));
                                    }
                                }
                                Err(error) => eprintln!("{}",json!({
                                    "stage":"targeted_ws_confirmation_invalid",
                                    "market_id":market.market_id,"detail":error.to_string()
                                })),
                            }
                        }
                    }
                    continue;
                },
                _=retry_tick.tick()=>{
                    if leases.required()&&leases.expire(std::time::Instant::now())>0&&!leases.enabled(){
                        confirmation_membership.send_replace(BTreeSet::new());
                        pause_acquisition(adapter,&pipeline,publication,&mut jobs)?;
                        eprintln!("{}",json!({"stage":"acquisition_lease_expired","acquisition_enabled":false}));
                    }
                    if let Err(error)=jobs.poll_retirements(){eprintln!("{}",json!({"stage":"acquisition_retirement_error","error":error.to_string()}));}
                    if leases.enabled()&&jobs.tokens().is_empty(){
                        jobs.add_pairs(&eligible_pairs(adapter)?)?;
                        confirmation_membership.send_replace(eligible_markets(adapter));
                        publication.set_acquisition_statistics(jobs.statistics());
                    }
                    let pending_terminal_markets = terminal_unsubscribes
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>();
                    for market_id in pending_terminal_markets {
                        let tokens = terminal_unsubscribes[&market_id].clone();
                        match jobs.request_retire(&tokens) {
                            Ok(true) => {
                                terminal_unsubscribes.remove(&market_id);
                                eprintln!("{}",json!({
                                    "stage":"lifecycle_wire_retirement_complete",
                                    "market_id":market_id,
                                    "token_count":tokens.len()
                                }));
                            }
                            Ok(false) => {}
                            Err(error) => eprintln!("{}",json!({
                                "stage":"lifecycle_wire_retirement_retry",
                                "market_id":market_id,
                                "detail":error.to_string()
                            })),
                        }
                    }
                    publication.set_acquisition_statistics(jobs.statistics());
                    if leases.enabled()&&!publication.is_source_ready()&&publication.fresh_tokens_ready(&acquisition_tokens){
                        publication.set_source_ready(true);
                        eprintln!("{}",json!({"stage":"acquisition_lease_ready","source_cursor":publication.cursor()?}));
                    }
                    if !leases.enabled(){continue;}
                    rest_submitted.retain(|token, attempt| adapter.recoveries.get(token) == Some(attempt));
                    // Coalesce all pending token gaps for one market into one
                    // temporary subscription. Repeated shard boundaries cannot
                    // create a socket per token or reset an in-flight identity.
                    let pending = pending_market_recoveries(adapter);
                    recovery_retry_at.retain(|market_id, _| pending.contains_key(market_id));
                    recovery_failures.retain(|market_id, _| pending.contains_key(market_id));
                    for (market_id, attempts) in pending {
                        if recovery_jobs.len() >= websocket_recovery_concurrency { break; }
                        if recovering_markets.contains(&market_id)
                            || recovery_retry_at.get(&market_id)
                                .is_some_and(|at| *at > tokio::time::Instant::now()) { continue; }
                        let tokens = attempts.keys().cloned().collect();
                        recovering_markets.insert(market_id.clone());
                        recovery_jobs.spawn(async move {
                            let result = recover_snapshots(tokens).await;
                            (market_id, attempts, result)
                        });
                    }
                    start_freshness_jobs(
                        &mut freshness_queued,
                        &mut freshness_pending,
                        &mut freshness_jobs,
                        websocket_recovery_concurrency,
                    );
                    continue;
                },
                _=&mut shutdown_requested=>break,
            };
            for frame in frames {
                if !leases.enabled(){continue;}
                if !marketcow_polymarket::subscription::owned_or_unclassified(&frame.raw_payload,&acquisition_tokens){continue;}
                // Consume completed work between admissions, not only after
                // the whole upstream batch. A bounded drain prevents either
                // continuously ready branch from starving the other.
                for _ in 0..market_workers * 2 {
                    match completed_markets.try_recv() {
                        Ok(done) => pipeline.publish(adapter, publication, done).await?,
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => anyhow::bail!("market workers stopped"),
                    }
                }
                pipeline.submit(adapter, publication, frame, None).await?;
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }.await;
    publication.set_source_ready(false);
    let _ = stop.send(true);
    // Let recovery workers observe their deadline/stop instead of detaching
    // nested transport jobs. A drop guard below aborts any nested socket task.
    lifecycle_jobs.abort_all();
    while lifecycle_jobs.join_next().await.is_some() {}
    freshness_jobs.abort_all();
    while freshness_jobs.join_next().await.is_some() {}
    jobs.stop().await;
    added_confirmation_jobs.abort_all();while added_confirmation_jobs.join_next().await.is_some(){}
    // A confirmation mismatch submits its already-validated REST pair to the
    // market actor immediately. Apply those bounded in-flight recoveries before
    // closing; merely draining and dropping their results would persist a gap
    // that did not exist upstream at shutdown.
    let recovery_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while !adapter.recoveries.is_empty() {
        tokio::select! {
            completed = completed_markets.recv() => {
                let Some(completed) = completed else { break; };
                pipeline.publish(adapter, publication, completed).await?;
            }
            completed = recovery_jobs.join_next(), if !recovery_jobs.is_empty() => {
                let Some(completed) = completed else { continue; };
                let (market_id, attempts, result) = completed?;
                recovering_markets.remove(&market_id);
                match result {
                    Ok(frames) => {
                        let mut by_token = frames.into_iter().filter_map(|frame| {
                            let token = frame.raw_payload["asset_id"].as_str()?.to_owned();
                            Some((token, frame))
                        }).collect::<BTreeMap<_, _>>();
                        for (token, attempt) in attempts {
                            if adapter.recoveries.get(&token) != Some(&attempt) { continue; }
                            if let Some(frame) = by_token.remove(&token) {
                                pipeline.submit(adapter, publication, frame, Some((token, attempt))).await?;
                            }
                        }
                    }
                    Err(error) => eprintln!("{}", json!({
                        "stage":"shutdown_market_recovery_failed","market_id":market_id,
                        "token_count":attempts.len(),"detail":error.to_string()
                    })),
                }
            }
            _ = tokio::time::sleep_until(recovery_deadline) => break,
        }
    }
    recovery_jobs.abort_all();
    while recovery_jobs.join_next().await.is_some() {}
    pipeline
        .finish(adapter, publication, &mut completed_markets)
        .await?;
    // Missing books are valid local quality facts, including at shutdown.
    // Do not start fresh network recovery or require all markets healthy to
    // stop. Every outstanding recovery must already be published; the owner
    // subsequently drains the independent durability worker before exit.
    if !adapter.recoveries.is_empty() {
        let (cursor, facts) = publication.shutdown_recovery_facts()?;
        validate_shutdown_recovery_facts(&adapter.recoveries, &facts)?;
        eprintln!("{}", json!({"stage":"shutdown_recoveries_preserved",
            "token_count":adapter.recoveries.len(),"published_cursor":cursor}));
    }
    confirmation_job.abort();
    let _ = confirmation_job.await;
    result
}

fn validate_shutdown_recovery_facts(expected:&BTreeMap<String,String>,published:&BTreeMap<String,Value>)->Result<()> {
    for (token,attempt) in expected {
        ensure!(published.get(token).is_some_and(|fact|fact["recovery_id"]==*attempt),
            "shutdown recovery not represented in published state");
    }
    Ok(())
}

#[cfg(test)]
mod shard_tests {
    use super::*;

    #[test]
    fn shutdown_preserves_missing_book_recovery_and_rejects_unpublished_attempt() {
        let expected=BTreeMap::from([("token".into(),"attempt".into())]);
        let published=BTreeMap::from([("token".into(),json!({"recovery_id":"attempt","gap":{"resolved":false}}))]);
        validate_shutdown_recovery_facts(&expected,&published).unwrap();
        assert_eq!(published["token"]["gap"]["resolved"],false);
        assert!(validate_shutdown_recovery_facts(&expected,&BTreeMap::new()).is_err());
        let stale=BTreeMap::from([("token".into(),json!({"recovery_id":"old"}))]);
        assert!(validate_shutdown_recovery_facts(&expected,&stale).is_err());
    }

    #[test]
    fn connection_shards_are_bounded_and_never_split_market_pairs() {
        let identities = BTreeMap::from([
            ("12".into(), ("1".into(), "c1".into())),
            ("11".into(), ("1".into(), "c1".into())),
            ("22".into(), ("2".into(), "c2".into())),
            ("21".into(), ("2".into(), "c2".into())),
            ("32".into(), ("3".into(), "c3".into())),
            ("31".into(), ("3".into(), "c3".into())),
        ]);
        let shards = connection_shards(&identities, 5).unwrap();
        assert_eq!(shards, vec![vec!["11", "12", "21", "22"], vec!["31", "32"]]);
        for market in ["1", "2", "3"] {
            let pair: BTreeSet<_> = identities
                .iter()
                .filter(|(_, value)| value.0 == market)
                .map(|(token, _)| token.as_str())
                .collect();
            assert!(shards.iter().any(|shard| {
                pair.iter()
                    .all(|token| shard.iter().any(|item| item == token))
            }));
        }
        assert!(connection_shards(&identities, 1).is_err());
        let incomplete = BTreeMap::from([("11".into(), ("1".into(), "c1".into()))]);
        assert!(connection_shards(&incomplete, 2).is_err());
    }
}
