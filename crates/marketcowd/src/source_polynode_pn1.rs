//! PolyNode PN1 token-depth integrity. The checksum is the vendor's PN1
//! checksum, not MarketCow's canonical-book hash or an official CLOB hash.
use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const MAX_LEVELS_PER_TOKEN: usize = 40_000;

fn decimal(raw: &str) -> Result<String> {
    ensure!(
        !raw.is_empty() && raw.len() <= 64 && raw.is_ascii(),
        "PN1 decimal syntax"
    );
    let mut points = 0;
    let mut digits = 0;
    for byte in raw.bytes() {
        if byte == b'.' {
            points += 1;
        } else if byte.is_ascii_digit() {
            digits += 1;
        } else {
            bail!("PN1 decimal syntax");
        }
    }
    ensure!(points <= 1 && digits > 0, "PN1 decimal syntax");
    let (whole, fraction) = raw.split_once('.').unwrap_or((raw, ""));
    let whole = whole.trim_start_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    let fraction = fraction.trim_end_matches('0');
    Ok(if fraction.is_empty() {
        whole.to_owned()
    } else {
        format!("{whole}.{fraction}")
    })
}

fn level_hash(side: u8, price: &str, size: &str) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"pn1:level\0");
    hash.update([side, 0]);
    hash.update(price.as_bytes());
    hash.update([0]);
    hash.update(size.as_bytes());
    hash.finalize().into()
}

fn xor_into(accumulator: &mut [u8; 32], digest: [u8; 32]) {
    for (left, right) in accumulator.iter_mut().zip(digest) {
        *left ^= right;
    }
}

fn put(
    levels: &mut BTreeMap<String, String>,
    side: u8,
    accumulator: &mut [u8; 32],
    raw_price: &str,
    raw_size: &str,
) -> Result<()> {
    let price = decimal(raw_price)?;
    let size = decimal(raw_size)?;
    if let Some(previous) = levels.remove(&price) {
        xor_into(accumulator, level_hash(side, &price, &previous));
    }
    if size != "0" {
        xor_into(accumulator, level_hash(side, &price, &size));
        levels.insert(price, size);
    }
    Ok(())
}

fn rows<'a>(value: &'a Value, field: &str) -> Result<&'a [Value]> {
    value
        .get(field)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .with_context(|| format!("PN1 {field} missing"))
}

fn apply_rows(
    levels: &mut BTreeMap<String, String>,
    side: u8,
    accumulator: &mut [u8; 32],
    rows: &[Value],
    anchor: bool,
) -> Result<()> {
    for row in rows {
        let price = row
            .get("price")
            .and_then(Value::as_str)
            .context("PN1 price")?;
        let size = row
            .get("size")
            .and_then(Value::as_str)
            .context("PN1 size")?;
        let canonical_price = decimal(price)?;
        if anchor {
            ensure!(
                !levels.contains_key(&canonical_price),
                "duplicate PN1 anchor level"
            );
        }
        put(levels, side, accumulator, price, size)?;
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct TokenBook {
    pub bids: BTreeMap<String, String>,
    pub asks: BTreeMap<String, String>,
    accumulator: [u8; 32],
    pub sequence: Option<u64>,
    pub verified: bool,
}

impl TokenBook {
    pub fn checksum(&self) -> String {
        let mut hash = Sha256::new();
        hash.update(b"pn1:book\0");
        hash.update(self.bids.len().to_string().as_bytes());
        hash.update([0]);
        hash.update(self.asks.len().to_string().as_bytes());
        hash.update([0]);
        hash.update(self.accumulator);
        hex::encode(hash.finalize())
    }

    pub fn invalidate(&mut self) {
        self.verified = false;
        self.sequence = None;
    }

    /// A failed frame fences this token. Do not publish the possibly mutated
    /// book; only a fresh anchor can make it verified again.
    pub fn apply_depth(&mut self, frame: &Value) -> Result<()> {
        let result = self.apply_depth_inner(frame);
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    fn apply_depth_inner(&mut self, frame: &Value) -> Result<()> {
        let integrity = frame.get("integrity").context("PN1 integrity missing")?;
        ensure!(
            integrity.get("protocol").and_then(Value::as_str) == Some("pn1"),
            "PN1 protocol"
        );
        let sequence = integrity
            .get("seq")
            .and_then(Value::as_u64)
            .context("PN1 seq")?;
        let expected = integrity
            .get("checksum")
            .and_then(Value::as_str)
            .context("PN1 checksum")?;
        ensure!(
            expected.len() == 64 && expected.bytes().all(|b| b.is_ascii_hexdigit()),
            "PN1 checksum format"
        );
        let kind = frame
            .get("type")
            .and_then(Value::as_str)
            .context("PN1 depth type")?;
        if integrity.get("anchor").and_then(Value::as_bool) == Some(true) {
            ensure!(kind == "book_snapshot", "PN1 anchor must be book snapshot");
            let mut next = Self::default();
            apply_rows(
                &mut next.bids,
                b'B',
                &mut next.accumulator,
                rows(frame, "bids")?,
                true,
            )?;
            apply_rows(
                &mut next.asks,
                b'A',
                &mut next.accumulator,
                rows(frame, "asks")?,
                true,
            )?;
            ensure!(
                next.bids.len() + next.asks.len() <= MAX_LEVELS_PER_TOKEN,
                "PN1 anchor level budget"
            );
            ensure!(next.checksum() == expected, "PN1 anchor checksum mismatch");
            next.sequence = Some(sequence);
            next.verified = true;
            *self = next;
            return Ok(());
        }
        ensure!(self.verified, "PN1 token has no verified anchor");
        let previous = integrity
            .get("prev_seq")
            .and_then(Value::as_u64)
            .context("PN1 prev_seq")?;
        ensure!(
            self.sequence == Some(previous) && previous.checked_add(1) == Some(sequence),
            "PN1 sequence gap"
        );
        match kind {
            "book_update" => {
                apply_rows(
                    &mut self.bids,
                    b'B',
                    &mut self.accumulator,
                    rows(frame, "bids")?,
                    false,
                )?;
                apply_rows(
                    &mut self.asks,
                    b'A',
                    &mut self.accumulator,
                    rows(frame, "asks")?,
                    false,
                )?;
            }
            "price_change" => {
                let assets = rows(frame, "assets")?;
                ensure!(assets.len() == 1, "PN1 price change must be token scoped");
                let row = &assets[0];
                let price = row
                    .get("price")
                    .and_then(Value::as_str)
                    .context("PN1 price")?;
                let size = row
                    .get("size")
                    .and_then(Value::as_str)
                    .context("PN1 size")?;
                match row.get("side").and_then(Value::as_str) {
                    Some("BUY") => put(&mut self.bids, b'B', &mut self.accumulator, price, size)?,
                    Some("SELL") => put(&mut self.asks, b'A', &mut self.accumulator, price, size)?,
                    _ => bail!("PN1 price change side"),
                }
            }
            _ => bail!("unsupported PN1 depth type"),
        }
        ensure!(
            self.bids.len() + self.asks.len() <= MAX_LEVELS_PER_TOKEN,
            "PN1 token level budget"
        );
        ensure!(self.checksum() == expected, "PN1 depth checksum mismatch");
        self.sequence = Some(sequence);
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Level {
    pub price: String,
    pub size: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangedLevel {
    pub side: &'static str,
    pub price: String,
    pub size: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DepthPayload {
    Anchor {
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    Delta {
        prev_seq: u64,
        changes: Vec<ChangedLevel>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DepthReceipt {
    pub token_id: String,
    pub condition_id: String,
    pub provider_connection_epoch: String,
    pub provider_seq: u64,
    pub provider_checksum: String,
    pub provider_batch_time_ms: Option<u64>,
    pub socket_received_at: String,
    pub raw_frame_sha256: String,
    pub is_anchor: bool,
    pub payload: DepthPayload,
}

fn changed_levels(update: &Value, kind: &str) -> Result<Vec<ChangedLevel>> {
    if kind == "price_change" {
        let assets = rows(update, "assets")?;
        ensure!(assets.len() == 1, "PN1 price change count");
        let row = &assets[0];
        let side = match row.get("side").and_then(Value::as_str) {
            Some("BUY") => "B",
            Some("SELL") => "A",
            _ => bail!("PN1 price change side"),
        };
        return Ok(vec![ChangedLevel {
            side,
            price: decimal(
                row.get("price")
                    .and_then(Value::as_str)
                    .context("PN1 price")?,
            )?,
            size: decimal(
                row.get("size")
                    .and_then(Value::as_str)
                    .context("PN1 size")?,
            )?,
        }]);
    }
    let mut changes = Vec::new();
    for (field, side) in [("bids", "B"), ("asks", "A")] {
        for row in rows(update, field)? {
            changes.push(ChangedLevel {
                side,
                price: decimal(
                    row.get("price")
                        .and_then(Value::as_str)
                        .context("PN1 price")?,
                )?,
                size: decimal(
                    row.get("size")
                        .and_then(Value::as_str)
                        .context("PN1 size")?,
                )?,
            });
        }
    }
    Ok(changes)
}

fn sorted_levels(levels: &BTreeMap<String, String>, descending: bool) -> Result<Vec<Level>> {
    use std::str::FromStr;
    let mut values = levels
        .iter()
        .map(|(price, size)| {
            Ok((
                rust_decimal::Decimal::from_str(price)?,
                Level {
                    price: price.clone(),
                    size: size.clone(),
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    values.sort_by(|(left, _), (right, _)| {
        if descending {
            right.cmp(left)
        } else {
            left.cmp(right)
        }
    });
    Ok(values.into_iter().map(|(_, level)| level).collect())
}

#[derive(Debug, Default)]
pub struct IngestOutcome {
    pub verified_depth: Vec<DepthReceipt>,
    pub invalidated_tokens: Vec<String>,
    pub controls: Vec<String>,
    pub resync_retry_after_ms: Option<u64>,
}

/// One authenticated socket generation. The owning task supplies its exact
/// profile token/condition set; no wildcard subscription is accepted here.
#[derive(Debug)]
pub struct Connection {
    pub epoch: String,
    pub tokens: BTreeMap<String, (String, TokenBook)>,
    subscribed: bool,
}

impl Connection {
    pub fn new(epoch: String, tokens: BTreeMap<String, String>) -> Result<Self> {
        ensure!(
            !epoch.is_empty() && !tokens.is_empty() && tokens.len() <= 6,
            "PN1 connection identity/budget"
        );
        ensure!(
            tokens.iter().all(|(token, condition)| token.len() > 10
                && token.bytes().all(|b| b.is_ascii_digit())
                && condition.len() == 66
                && condition.starts_with("0x")),
            "PN1 token/condition identity"
        );
        Ok(Self {
            epoch,
            tokens: tokens
                .into_iter()
                .map(|(token, condition)| (token, (condition, TokenBook::default())))
                .collect(),
            subscribed: false,
        })
    }

    pub fn invalidate_all(&mut self) {
        self.subscribed = false;
        for (_, book) in self.tokens.values_mut() {
            book.invalidate();
        }
    }

    /// The caller timestamps the raw WebSocket message immediately after recv
    /// returns and only then invokes this parser. A bad outer frame fences all
    /// tokens; a bad depth update fences only its token.
    pub fn ingest(&mut self, raw: &str, socket_received_at: &str) -> Result<IngestOutcome> {
        let result = self.ingest_inner(raw, socket_received_at);
        if result.is_err() {
            self.invalidate_all();
        }
        result
    }

    fn ingest_inner(&mut self, raw: &str, socket_received_at: &str) -> Result<IngestOutcome> {
        ensure!(raw.len() <= 2 * 1024 * 1024, "PN1 frame byte limit");
        let value: Value = match serde_json::from_str(raw) {
            Ok(value) => value,
            Err(error) => {
                self.invalidate_all();
                return Err(error.into());
            }
        };
        let mut outcome = IngestOutcome::default();
        if let Some(error) = value.get("error").and_then(Value::as_str) {
            if error == "resync_in_progress" && self.subscribed {
                outcome.resync_retry_after_ms = Some(
                    value
                        .get("retry_after_ms")
                        .and_then(Value::as_u64)
                        .unwrap_or(250)
                        .min(5_000),
                );
                outcome.controls.push("resync_in_progress".into());
                return Ok(outcome);
            }
            self.invalidate_all();
            bail!("PN1 provider rejected subscription or integrity recovery");
        }
        let Some(kind) = value.get("type").and_then(Value::as_str) else {
            self.invalidate_all();
            bail!("PN1 frame type missing");
        };
        if kind == "subscribed" {
            if value.pointer("/integrity/enabled").and_then(Value::as_bool) != Some(true)
                || value.pointer("/integrity/protocol").and_then(Value::as_str) != Some("pn1")
                || value.get("markets").and_then(Value::as_u64) != Some(self.tokens.len() as u64)
            {
                self.invalidate_all();
                bail!("PN1 subscription not acknowledged for exact tokens");
            }
            // A replacement subscription never inherits anchors from the
            // preceding token set, even if the acknowledgement count matches.
            self.invalidate_all();
            self.subscribed = true;
            outcome.controls.push("subscribed".into());
            return Ok(outcome);
        }
        if !self.subscribed {
            bail!("PN1 data before subscription acknowledgement");
        }
        if kind == "resyncing" {
            match value.get("markets").and_then(Value::as_array) {
                Some(markets) => {
                    for item in markets {
                        let token = item.as_str().context("PN1 resync token")?;
                        let (_, book) = self
                            .tokens
                            .get_mut(token)
                            .context("PN1 resync token outside subscription")?;
                        book.invalidate();
                        outcome.invalidated_tokens.push(token.to_owned());
                    }
                }
                None => {
                    for (token, (_, book)) in &mut self.tokens {
                        book.invalidate();
                        outcome.invalidated_tokens.push(token.clone());
                    }
                }
            }
            outcome.controls.push("resyncing".into());
            return Ok(outcome);
        }
        if kind == "pong" {
            outcome.controls.push("pong".into());
            return Ok(outcome);
        }
        if kind == "snapshots_done" {
            // This marker is a standalone frame on an integrity connection.
            // It closes the anchor burst but cannot itself certify a token:
            // only a checksum-verified anchor makes a book readable.
            outcome.controls.push("snapshots_done".into());
            return Ok(outcome);
        }
        if kind == "resync_in_progress" {
            // Keep the token gated while the provider completes its own
            // resync. This control is never a fresh depth fact.
            outcome.resync_retry_after_ms = Some(
                value
                    .get("retry_after_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or(250)
                    .min(5_000),
            );
            outcome.controls.push("resync_in_progress".into());
            return Ok(outcome);
        }
        let batch_time_ms = if kind == "batch" {
            Some(
                value
                    .get("ts")
                    .and_then(Value::as_u64)
                    .context("PN1 batch ts")?,
            )
        } else {
            None
        };
        let updates = if kind == "batch" {
            value
                .get("updates")
                .and_then(Value::as_array)
                .context("PN1 batch updates")?
                .iter()
                .collect::<Vec<_>>()
        } else {
            vec![&value]
        };
        if kind == "batch" {
            ensure!(
                value.get("count").and_then(Value::as_u64) == Some(u64::try_from(updates.len())?),
                "PN1 batch count mismatch"
            );
        }
        let frame_sha = hex::encode(Sha256::digest(raw.as_bytes()));
        for update in updates {
            let update_kind = update
                .get("type")
                .and_then(Value::as_str)
                .context("PN1 update type")?;
            match update_kind {
                "book_snapshot" | "book_update" | "price_change" => {}
                "last_trade_price" => {
                    ensure!(
                        update.get("integrity").is_none(),
                        "PN1 trade cannot carry depth integrity"
                    );
                    continue; // Trade does not mutate depth or PN1 seq.
                }
                "resyncing" | "snapshots_done" | "pong" => {
                    self.invalidate_all();
                    bail!("PN1 control nested inside depth batch");
                }
                _ => {
                    self.invalidate_all();
                    bail!("unknown PN1 update type");
                }
            }
            // PN1 omits integrity only for a non-depth price-change notice.
            // A row carrying size or side is a depth mutation and must be
            // rejected if it lacks the token-scoped integrity envelope.
            if update.get("integrity").is_none() {
                if update_kind == "price_change"
                    && rows(update, "assets")?
                        .iter()
                        .all(|row| row.get("size").is_none() && row.get("side").is_none())
                {
                    continue;
                }
                self.invalidate_all();
                bail!("PN1 depth update lacks integrity");
            }
            let token = update
                .get("asset_id")
                .and_then(Value::as_str)
                .context("PN1 token id")?;
            let Some((condition, book)) = self.tokens.get_mut(token) else {
                // PN1 uses an exact token subscription. An unsolicited token
                // means this socket generation cannot be trusted as a source
                // of current-market depth, even if the old profile retired.
                self.invalidate_all();
                bail!("PN1 update outside acknowledged token set");
            };
            if update_kind == "price_change"
                && update
                    .get("assets")
                    .and_then(Value::as_array)
                    .and_then(|assets| assets.first())
                    .and_then(|row| row.get("asset_id"))
                    .and_then(Value::as_str)
                    != Some(token)
            {
                book.invalidate();
                outcome.invalidated_tokens.push(token.to_owned());
                continue;
            }
            let observed_condition = update
                .get("condition_id")
                .or_else(|| update.get("market"))
                .and_then(Value::as_str);
            if observed_condition != Some(condition.as_str()) {
                book.invalidate();
                outcome.invalidated_tokens.push(token.to_owned());
                continue;
            }
            let anchor = update.pointer("/integrity/anchor").and_then(Value::as_bool) == Some(true);
            // PN1 may repeat the latest committed depth frame on recovery.
            // A same-sequence, same-checksum duplicate changes no book state.
            if !anchor
                && book.sequence == update.pointer("/integrity/seq").and_then(Value::as_u64)
                && update
                    .pointer("/integrity/checksum")
                    .and_then(Value::as_str)
                    == Some(book.checksum().as_str())
            {
                continue;
            }
            if book.apply_depth(update).is_err() {
                outcome.invalidated_tokens.push(token.to_owned());
                continue;
            }
            let payload = if anchor {
                DepthPayload::Anchor {
                    bids: sorted_levels(&book.bids, true)?,
                    asks: sorted_levels(&book.asks, false)?,
                }
            } else {
                DepthPayload::Delta {
                    prev_seq: update
                        .pointer("/integrity/prev_seq")
                        .and_then(Value::as_u64)
                        .context("verified PN1 prev_seq")?,
                    changes: changed_levels(update, update_kind)?,
                }
            };
            outcome.verified_depth.push(DepthReceipt {
                token_id: token.to_owned(),
                condition_id: condition.clone(),
                provider_connection_epoch: self.epoch.clone(),
                provider_seq: book.sequence.context("verified PN1 seq")?,
                provider_checksum: book.checksum(),
                provider_batch_time_ms: batch_time_ms,
                socket_received_at: socket_received_at.to_owned(),
                raw_frame_sha256: frame_sha.clone(),
                is_anchor: anchor,
                payload,
            });
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn vendor_checksum_vectors_and_sequence_fence() {
        let mut book = TokenBook::default();
        let empty = "f9990dc511a3f5eabf06565ee7f52801621e28d7f2d3509d6fbda91e561edecf";
        assert_eq!(book.checksum(), empty);
        book.apply_depth(&json!({"type":"book_snapshot","bids":[],"asks":[],
            "integrity":{"protocol":"pn1","anchor":true,"seq":10,"checksum":empty}}))
            .unwrap();
        assert!(book.verified);
        let filled = "a1ec7486bc53e14c4294f08e9298362fe7b5c6495c474e8c447e27ea976eb93a";
        book.apply_depth(&json!({"type":"book_snapshot",
            "bids":[{"price":"0.50","size":"10.000"}],
            "asks":[{"price":"0.6000","size":"20"}],
            "integrity":{"protocol":"pn1","anchor":true,"seq":11,"checksum":filled}}))
            .unwrap();
        assert_eq!(book.checksum(), filled);
        assert_eq!(decimal(".50").unwrap(), "0.5");
        assert_eq!(decimal("000.000").unwrap(), "0");
        assert!(
            book.apply_depth(&json!({"type":"book_update","bids":[],"asks":[],
            "integrity":{"protocol":"pn1","prev_seq":9,"seq":12,"checksum":filled}}))
                .is_err()
        );
        assert!(!book.verified);
    }

    #[test]
    fn vendor_third_vector() {
        let expected = "7829126b80f3eccac2ab4ecc0e999b90f061df3214db0d2717165834a57d66be";
        let mut book = TokenBook::default();
        book.apply_depth(&json!({"type":"book_snapshot",
            "bids":[{"price":"0.5","size":"12.500"},{"price":"00.4900","size":"3.0"}],
            "asks":[],"integrity":{"protocol":"pn1","anchor":true,"seq":1,"checksum":expected}}))
            .unwrap();
        assert_eq!(book.checksum(), expected);
    }

    #[test]
    fn connection_keeps_batch_time_separate_and_fences_one_token() {
        let empty = "f9990dc511a3f5eabf06565ee7f52801621e28d7f2d3509d6fbda91e561edecf";
        let mut connection = Connection::new(
            "epoch-a".into(),
            BTreeMap::from([
                (
                    "123456789012".into(),
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                ),
                (
                    "987654321098".into(),
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                ),
            ]),
        )
        .unwrap();
        connection.ingest(r#"{"type":"subscribed","markets":2,"integrity":{"enabled":true,"protocol":"pn1"}}"#,
            "2026-10-09T00:00:00Z").unwrap();
        let anchor = json!({"type":"book_snapshot","asset_id":"123456789012",
            "market":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bids":[],"asks":[],"integrity":{"protocol":"pn1","anchor":true,"seq":1,"checksum":empty}});
        let receipt = connection
            .ingest(&anchor.to_string(), "2026-10-09T00:00:01Z")
            .unwrap();
        assert_eq!(receipt.verified_depth[0].provider_batch_time_ms, None);
        assert!(!connection.tokens["987654321098"].1.verified);
        let invalid = json!({"type":"batch","ts":1791504002000u64,"count":1,"updates":[
            {"type":"book_update","asset_id":"123456789012","bids":[],"asks":[],
             "integrity":{"protocol":"pn1","prev_seq":3,"seq":4,"checksum":empty}}]});
        let outcome = connection
            .ingest(&invalid.to_string(), "2026-10-09T00:00:02Z")
            .unwrap();
        assert_eq!(outcome.invalidated_tokens, vec!["123456789012"]);
        assert!(!connection.tokens["123456789012"].1.verified);
    }

    #[test]
    fn duplicate_verified_depth_is_not_republished_and_resync_busy_does_not_reset_all() {
        let empty = "f9990dc511a3f5eabf06565ee7f52801621e28d7f2d3509d6fbda91e561edecf";
        let token = "123456789012";
        let condition = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mut connection = Connection::new(
            "epoch-a".into(),
            BTreeMap::from([(token.into(), condition.into())]),
        )
        .unwrap();
        connection.ingest(r#"{"type":"subscribed","markets":1,"integrity":{"enabled":true,"protocol":"pn1"}}"#,
            "2026-10-09T00:00:00Z").unwrap();
        let anchor = json!({"type":"book_snapshot","asset_id":token,"market":condition,
            "bids":[],"asks":[],"integrity":{"protocol":"pn1","anchor":true,
                "seq":10,"checksum":empty}})
        .to_string();
        assert_eq!(
            connection
                .ingest(&anchor, "2026-10-09T00:00:01Z")
                .unwrap()
                .verified_depth
                .len(),
            1
        );
        let repeated = json!({"type":"book_update","asset_id":token,"market":condition,
            "bids":[],"asks":[],"integrity":{"protocol":"pn1",
                "prev_seq":9,"seq":10,"checksum":empty}})
        .to_string();
        assert!(
            connection
                .ingest(&repeated, "2026-10-09T00:00:02Z")
                .unwrap()
                .verified_depth
                .is_empty()
        );
        let busy = connection
            .ingest(
                r#"{"error":"resync_in_progress","retry_after_ms":300}"#,
                "2026-10-09T00:00:03Z",
            )
            .unwrap();
        assert_eq!(busy.resync_retry_after_ms, Some(300));
        assert!(connection.tokens[token].1.verified);
    }
}
