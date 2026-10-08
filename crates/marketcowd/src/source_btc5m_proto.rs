//! Typed market-local wire conversion. The producer's existing canonical
//! commitments remain semantic JSON hashes; protobuf is transport, not a new
//! hash preimage or a second live state machine.

use anyhow::{Context, Result, bail, ensure};
use marketcow_contracts::btc5m_market_local_v2::{encode_frame, wire};
use serde::{Serialize, Serializer, ser::{SerializeMap, SerializeSeq}};
use serde_json::Value;
use marketcow_runtime::discovery_source::DurableMarketGroup;
use sha2::{Digest, Sha256};

// Encode the validated source event without cloning its raw and canonical
// payload trees. Keys are sorted recursively, and the top-level event_id is
// excluded from its own commitment.
// A digest equality check below also rejects any unexpected JSON key-ordering
// behavior before a noncanonical preimage could reach the protobuf feed.
struct SourceEventPreimage<'a>(&'a Value);

struct CanonicalValue<'a>(&'a Value);

impl Serialize for CanonicalValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self.0 {
            Value::Object(values) => {
                let mut keys = values.keys().collect::<Vec<_>>();
                keys.sort_unstable();
                let mut map = serializer.serialize_map(Some(keys.len()))?;
                for key in keys { map.serialize_entry(key, &CanonicalValue(&values[key]))?; }
                map.end()
            }
            Value::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values { sequence.serialize_element(&CanonicalValue(value))?; }
                sequence.end()
            }
            value => value.serialize(serializer),
        }
    }
}

impl Serialize for SourceEventPreimage<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let object = self.0.as_object().ok_or_else(||
            serde::ser::Error::custom("source event is not an object"))?;
        let mut keys = object.keys().filter(|key| key.as_str() != "event_id")
            .collect::<Vec<_>>();
        keys.sort_unstable();
        let mut map = serializer.serialize_map(Some(keys.len()))?;
        for key in keys {
            map.serialize_entry(key, &CanonicalValue(&object[key]))?;
        }
        map.end()
    }
}

fn required_string<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value.get(key).and_then(Value::as_str).with_context(|| format!("missing {key}"))
}

fn optional_string(value: &Value, key: &str) -> Result<Option<String>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        _ => bail!("invalid {key}"),
    }
}

fn required_u64(value: &Value, key: &str) -> Result<u64> {
    value.get(key).and_then(Value::as_u64).with_context(|| format!("missing {key}"))
}

fn optional_u64(value: &Value, key: &str) -> Result<Option<u64>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number.as_u64().map(Some).with_context(|| format!("invalid {key}")),
        _ => bail!("invalid {key}"),
    }
}

pub(crate) fn hash32(value: &str) -> Result<Vec<u8>> {
    ensure!(value.len() == 64 && value.bytes().all(|byte|
        byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)), "invalid SHA-256 hex");
    Ok(hex::decode(value)?)
}

fn price_levels(value: &Value, side: &str) -> Result<Vec<wire::PriceLevel>> {
    value.get(side).and_then(Value::as_array)
        .with_context(|| format!("missing {side}"))?
        .iter().map(|level| Ok(wire::PriceLevel {
            price: required_string(level, "price")?.into(),
            size: required_string(level, "size")?.into(),
        })).collect()
}

pub(crate) fn canonical_book(value: &Value) -> Result<wire::CanonicalBook> {
    Ok(wire::CanonicalBook {
        token_id: required_string(value, "token_id")?.into(),
        condition_id: required_string(value, "condition_id")?.into(),
        book_epoch: required_string(value, "book_epoch")?.into(),
        sequence: required_u64(value, "sequence")?,
        sequence_semantics: required_string(value, "sequence_semantics")?.into(),
        exchange_at: optional_string(value, "exchange_at")?,
        received_at: required_string(value, "received_at")?.into(),
        tick_version: hash32(required_string(value, "tick_version")?)?,
        tick_size: required_string(value, "tick_size")?.into(),
        bids: price_levels(value, "bids")?,
        asks: price_levels(value, "asks")?,
        last_trade_price: optional_string(value, "last_trade_price")?,
        state_checksum: hash32(required_string(value, "state_checksum")?)?,
        source_hash: optional_string(value, "source_hash")?,
    })
}

fn post_state(value: &Value) -> Result<wire::BookPostState> {
    Ok(wire::BookPostState {
        token_id: required_string(value, "token_id")?.into(),
        condition_id: required_string(value, "condition_id")?.into(),
        book_epoch: required_string(value, "book_epoch")?.into(),
        sequence: required_u64(value, "sequence")?,
        sequence_semantics: required_string(value, "sequence_semantics")?.into(),
        exchange_at: optional_string(value, "exchange_at")?,
        received_at: required_string(value, "received_at")?.into(),
        tick_version: hash32(required_string(value, "tick_version")?)?,
        tick_size: required_string(value, "tick_size")?.into(),
        last_trade_price: optional_string(value, "last_trade_price")?,
        state_checksum: hash32(required_string(value, "state_checksum")?)?,
        source_hash: optional_string(value, "source_hash")?,
        canonical_book_sha256: hash32(required_string(value, "canonical_book_sha256")?)?,
        materialized_book_bytes: required_u64(value, "materialized_book_bytes")?,
    })
}

pub(crate) fn market_event(value: &Value) -> Result<wire::MarketEvent> {
    use wire::market_event::Payload;
    let payload = value.get("canonical_payload").context("missing canonical payload")?;
    let preimage = serde_json::to_vec(&SourceEventPreimage(value))?;
    ensure!(hex::encode(Sha256::digest(&preimage)) == required_string(value, "event_id")?,
        "source event preimage differs from event identity");
    let body = match required_string(value, "event_type")? {
        "book" => match payload.get("payload_semantics").and_then(Value::as_str) {
            None => Payload::BookSnapshot(canonical_book(payload)?),
            Some("ordered_depth_delta_with_complete_post_state_commitment") => {
                let changes = payload["delta"]["changes"].as_array()
                    .context("missing compact delta changes")?.iter()
                    .map(|change| Ok(wire::DepthChange {
                        token_id: required_string(change, "asset_id")?.into(),
                        side: required_string(change, "side")?.into(),
                        price: required_string(change, "price")?.into(),
                        size: required_string(change, "size")?.into(),
                    })).collect::<Result<Vec<_>>>()?;
                ensure!(!changes.is_empty(), "empty compact delta");
                Payload::BookDelta(wire::BookDelta { changes, post_state: Some(post_state(payload)?) })
            }
            Some("last_trade_with_unchanged_depth_commitment") => {
                Payload::LastTrade(wire::LastTrade {
                    price: required_string(&payload["trade"], "price")?.into(),
                    post_state: Some(post_state(payload)?),
                    trade_exchange_at: optional_string(&payload["trade"], "exchange_at")?,
                    trade_received_at: required_string(&payload["trade"], "received_at")?.into(),
                })
            }
            Some(other) => bail!("unknown compact book semantics {other}"),
        },
        "recovery_started" => Payload::RecoveryStarted(wire::RecoveryStarted {
            recovery_id: required_string(payload, "recovery_id")?.into(),
            recovery_scope: required_string(payload, "recovery_scope")?.into(),
            affected_token_ids: vec![required_string(payload, "token_id")?.into()],
            reason: required_string(payload, "reason")?.into(),
        }),
        "recovery_completed" => Payload::RecoveryCompleted(wire::RecoveryCompleted {
            recovery_id: required_string(payload, "recovery_id")?.into(),
            recovery_scope: required_string(payload, "recovery_scope")?.into(),
            snapshot_cursor: required_u64(payload, "snapshot_cursor")?,
            resolved_gap_token_ids: payload["resolved_gap_token_ids"].as_array()
                .context("missing resolved gap tokens")?.iter()
                .map(|token| token.as_str().map(str::to_owned).context("invalid resolved token"))
                .collect::<Result<Vec<_>>>()?,
        }),
        "source_gap" => Payload::SourceGap(wire::SourceGap {
            token_id: required_string(payload, "token_id")?.into(),
            code: required_string(payload, "code")?.into(),
            reason: required_string(payload, "reason")?.into(),
        }),
        "market_terminal" => Payload::Terminal(wire::Terminal {
            reason: required_string(payload, "reason")?.into(),
            retired_token_ids: payload["retired_token_ids"].as_array()
                .context("missing retired tokens")?.iter()
                .map(|token| token.as_str().map(str::to_owned).context("invalid retired token"))
                .collect::<Result<Vec<_>>>()?,
            lifecycle_state: required_string(&payload["market"], "lifecycle_state")?.into(),
            winning_outcome: optional_string(&payload["market"], "resolution")?,
            terminal_at: required_string(&payload["market"], "terminal_at")?.into(),
            lifecycle_evidence_sha256: hash32(required_string(&payload["market"], "lifecycle_evidence_sha256")?)?,
            metadata_revision_sha256: hash32(required_string(&payload["market"], "metadata_revision")?)?,
        }),
        other => bail!("unsupported market event type {other}"),
    };
    Ok(wire::MarketEvent {
        market_event_cursor: required_u64(value, "cursor")?,
        event_id: hash32(required_string(value, "event_id")?)?,
        raw_payload_sha256: hash32(required_string(value, "raw_payload_sha256")?)?,
        canonical_payload_sha256: hash32(required_string(value, "canonical_payload_sha256")?)?,
        market_id: required_string(value, "market_id")?.into(),
        condition_id: required_string(value, "condition_id")?.into(),
        token_id: optional_string(value, "token_id")?,
        book_epoch: optional_string(value, "book_epoch")?,
        book_sequence: optional_u64(value, "sequence")?,
        exchange_at: optional_string(value, "exchange_at")?,
        received_at: required_string(value, "received_at")?.into(),
        applied: Some(value["applied"].as_bool().context("missing applied")?),
        fail_closed_reason: optional_string(value, "fail_closed_reason")?,
        physical_archive_cursor: None,
        source_event_preimage_json: preimage,
        payload: Some(body),
    })
}

pub(crate) fn confirmation(value: &Value) -> Result<wire::BookConfirmation> {
    Ok(wire::BookConfirmation {
        token_id: required_string(value, "token_id")?.into(),
        condition_id: required_string(value, "condition_id")?.into(),
        confirmation_sequence: required_u64(value, "confirmation_sequence")?,
        book_market_cursor: required_u64(value, "market_cursor")?,
        book_epoch: required_string(value, "book_epoch")?.into(),
        book_sequence: required_u64(value, "book_sequence")?,
        state_checksum: hash32(required_string(value, "state_checksum")?)?,
        book_received_at: required_string(value, "book_received_at")?.into(),
        confirmed_at: required_string(value, "confirmed_at")?.into(),
        confirmation_source: required_string(value, "confirmation_source")?.into(),
        confirmation_evidence_sha256: hash32(required_string(value, "confirmation_evidence_sha256")?)?,
    })
}

pub(crate) fn encode_market_group(
    identity: &wire::MarketIdentity, market_cursor: u64, group_sha256: &str,
    events: &[Value], maximum_frame_bytes: usize,
) -> Result<Vec<Vec<u8>>> {
    use wire::market_stream_frame::Body;
    ensure!(!events.is_empty() && market_cursor > 0 && identity.stream_epoch.is_some(),
        "market group identity/events");
    let group_hash = hash32(group_sha256)?;
    let typed = events.iter().map(market_event).collect::<Result<Vec<_>>>()?;
    let mut ranges = vec![(0, typed.len())];
    let frame = |body| wire::MarketStreamFrame { identity: Some(identity.clone()), body: Some(body) };
    let mut index = 0;
    while index < ranges.len() {
        let (start, end) = ranges[index];
        let candidate = frame(Body::GroupChunk(wire::MarketGroupChunk {
            market_cursor, group_sha256: group_hash.clone(),
            chunk_index: u32::MAX, chunk_count: u32::MAX,
            events: typed[start..end].to_vec(),
        }));
        if encode_frame(&candidate, maximum_frame_bytes).is_ok() {
            index += 1;
        } else {
            ensure!(end - start > 1, "single market event exceeds protobuf frame budget");
            let middle = start + (end - start) / 2;
            ranges.splice(index..=index, [(start, middle), (middle, end)]);
        }
    }
    let event_count = u32::try_from(typed.len())?;
    let chunk_count = u32::try_from(ranges.len())?;
    let mut frames = Vec::with_capacity(ranges.len() + 2);
    frames.push(encode_frame(&frame(Body::GroupBegin(wire::MarketGroupBegin {
        market_cursor, group_sha256: group_hash.clone(), event_count, chunk_count,
    })), maximum_frame_bytes)?);
    for (chunk_index, (start, end)) in ranges.into_iter().enumerate() {
        frames.push(encode_frame(&frame(Body::GroupChunk(wire::MarketGroupChunk {
            market_cursor, group_sha256: group_hash.clone(),
            chunk_index: u32::try_from(chunk_index)?, chunk_count,
            events: typed[start..end].to_vec(),
        })), maximum_frame_bytes)?);
    }
    frames.push(encode_frame(&frame(Body::GroupCommit(wire::MarketGroupCommit {
        market_cursor, group_sha256: group_hash, event_count, chunk_count,
        next_after_market_cursor: market_cursor,
    })), maximum_frame_bytes)?);
    Ok(frames)
}

pub(crate) fn replay_groups(groups: &[DurableMarketGroup]) -> Result<Vec<wire::MarketReplayGroup>> {
    groups.iter().map(|group| {
        ensure!(group.events.len() == group.physical_archive_cursors.len(),
            "archive cursor count differs from market group");
        Ok(wire::MarketReplayGroup {
            market_cursor: group.market_cursor,
            group_sha256: hash32(&group.group_sha256)?,
            events: group.events.iter().zip(&group.physical_archive_cursors)
                .map(|(event, physical)| {
                    let mut typed = market_event(event)?;
                    typed.physical_archive_cursor = Some(*physical);
                    Ok(typed)
                }).collect::<Result<Vec<_>>>()?,
        })
    }).collect()
}
