//! Typed ordinary market lane. No JSON, digest, or source-switch wire events.
use anyhow::{Context, Result, ensure};
use marketcow_contracts::typed_v3::{MARKET_SCHEMA, market as wire};
use prost::Message;
use rust_decimal::Decimal;
use std::sync::Arc;

pub const MAX_LEVELS: usize = 10_000;
pub const MAX_CHANGES: usize = 40_000;
pub const MAX_BYTES: usize = 8 * 1024 * 1024;

fn decimal(value: &str, positive: bool) -> Result<Decimal> {
    ensure!(
        !value.is_empty() && value.len() <= 96 && !value.contains(['e', 'E', '+']),
        "invalid decimal syntax"
    );
    let parsed: Decimal = value.parse().context("invalid decimal")?;
    ensure!(
        if positive {
            parsed > Decimal::ZERO
        } else {
            parsed >= Decimal::ZERO
        },
        "invalid decimal range"
    );
    Ok(parsed)
}
fn source_time(value: &str) -> Result<()> {
    chrono::DateTime::parse_from_rfc3339(value).context("invalid source time")?;
    Ok(())
}
fn levels(rows: &mut [wire::PriceLevel], bids: bool) -> Result<()> {
    ensure!(rows.len() <= MAX_LEVELS, "depth capacity");
    let mut previous = None;
    for row in rows {
        let price = decimal(&row.price, false)?;
        ensure!(price <= Decimal::ONE, "outcome price range");
        let size = decimal(&row.size, true)?;
        if let Some(before) = previous {
            ensure!(
                if bids { before > price } else { before < price },
                "depth order/duplicate"
            );
        }
        previous = Some(price);
        row.price = price.normalize().to_string();
        row.size = size.normalize().to_string();
    }
    Ok(())
}
fn same_book(a: &wire::TokenBook, b: &wire::TokenBook) -> bool {
    a.bids == b.bids
        && a.asks == b.asks
        && match (&a.metadata, &b.metadata) {
            (None, None) => true,
            (Some(a), Some(b)) => {
                a.token_id == b.token_id
                    && a.outcome == b.outcome
                    && a.tick_size == b.tick_size
                    && a.quote_source_at == b.quote_source_at
                    && a.data_valid == b.data_valid
                    && a.invalid_reason == b.invalid_reason
            }
            _ => false,
        }
}

pub struct Lane {
    pub(super) key: wire::MarketKey,
    pub(super) stream_id: String,
    sequence: u64,
    snapshot: Option<Arc<wire::Snapshot>>,
    source_generation: String,
    depth_bytes: [usize; 2],
    envelope_bytes: usize,
}
pub struct State {
    pub key: wire::MarketKey,
    pub stream_id: String,
    pub sequence: u64,
    pub snapshot: Arc<wire::Snapshot>,
    source_generation: String,
}
impl Lane {
    pub fn new(key: wire::MarketKey, stream_id: String) -> Result<Self> {
        ensure!(
            !key.market_id.is_empty() && !key.window_id.is_empty() && !stream_id.is_empty(),
            "missing market identity"
        );
        ensure!(
            key.condition_id.len() == 66
                && key.condition_id.starts_with("0x")
                && key.condition_id[2..].bytes().all(|c| c.is_ascii_hexdigit()),
            "condition identity"
        );
        ensure!(
            !key.up_token_id.is_empty()
                && !key.down_token_id.is_empty()
                && key.up_token_id != key.down_token_id,
            "outcome identity"
        );
        let rules = key.rules.as_ref().context("missing rules identity")?;
        ensure!(
            !rules.id.is_empty() && rules.revision > 0,
            "invalid rules identity"
        );
        let start = chrono::DateTime::parse_from_rfc3339(&key.start_at)?;
        let end = chrono::DateTime::parse_from_rfc3339(&key.end_at)?;
        ensure!(start < end, "window interval");
        let field = |n: usize| 1 + prost::length_delimiter_len(n) + n;
        // Includes a conservative 64-byte publication timestamp and largest
        // sequence varint. Compute immutable envelope cost once, not per frame.
        let envelope_bytes = field(MARKET_SCHEMA.len())
            + field(key.encoded_len())
            + field(stream_id.len())
            + 11
            + field(64);
        Ok(Self {
            key,
            stream_id,
            sequence: 0,
            snapshot: None,
            source_generation: String::new(),
            depth_bytes: [0; 2],
            envelope_bytes,
        })
    }

    /// Input is already one owner's complete atomic pair. Source generation is
    /// private; changing it forces only an ordinary downstream snapshot.
    pub fn capture(
        &mut self,
        mut snapshot: wire::Snapshot,
        source_generation: String,
    ) -> Result<State> {
        ensure!(snapshot.books.len() == 2, "two token books required");
        ensure!(
            matches!(
                snapshot.source_status.as_str(),
                "available" | "unavailable" | "waiting_baseline" | "ended"
            ),
            "source status"
        );
        let tokens = [&self.key.up_token_id, &self.key.down_token_id];
        for (index, book) in snapshot.books.iter_mut().enumerate() {
            levels(&mut book.bids, true)?;
            levels(&mut book.asks, false)?;
            let before = self.snapshot.as_ref().and_then(|s| s.books.get(index));
            let previous_revision = before
                .and_then(|b| b.metadata.as_ref())
                .map_or(0, |m| m.book_revision);
            let meta = book.metadata.as_mut().context("missing token metadata")?;
            ensure!(
                &meta.token_id == tokens[index]
                    && meta.outcome == if index == 0 { "Up" } else { "Down" },
                "token/outcome binding"
            );
            if let Some(tick) = &mut meta.tick_size {
                *tick = decimal(tick, true)?.normalize().to_string();
            }
            if let Some(at) = &meta.quote_source_at {
                source_time(at)?;
            }
            if let Some(trade) = &mut meta.last_trade {
                trade.price = decimal(&trade.price, false)?.normalize().to_string();
                if let Some(at) = &trade.source_at {
                    source_time(at)?;
                }
            }
            if let Some(confirmation) = &mut meta.confirmation {
                source_time(&confirmation.source_at)?;
                source_time(&confirmation.received_at)?;
                ensure!(
                    meta.quote_source_at.as_deref() == Some(&confirmation.bound_quote_source_at),
                    "confirmation quote binding"
                );
                let quote =
                    chrono::DateTime::parse_from_rfc3339(&confirmation.bound_quote_source_at)?;
                let at = chrono::DateTime::parse_from_rfc3339(&confirmation.source_at)?;
                ensure!(quote <= at, "confirmation before quote");
                let received = chrono::DateTime::parse_from_rfc3339(&confirmation.received_at)?;
                ensure!(at <= received, "confirmation after receipt");
            }
            ensure!(
                !meta.data_valid || meta.quote_source_at.is_some(),
                "valid book without source time"
            );
            let changed = before.is_none_or(|b| !same_book(b, book));
            let revision = if changed {
                previous_revision
                    .checked_add(1)
                    .context("book revision overflow")?
            } else {
                previous_revision
            };
            let meta = book.metadata.as_mut().unwrap();
            meta.book_revision = revision;
            if let Some(confirmation) = &mut meta.confirmation {
                // Pre-bound evidence cannot silently follow a changed book.
                ensure!(
                    confirmation.bound_book_revision == 0
                        || confirmation.bound_book_revision == revision,
                    "confirmation book binding"
                );
                confirmation.bound_book_revision = revision;
            }
        }
        // encoded_len measures typed fields without serializing a complete
        // snapshot. Cache unchanged book sizes; never create size-only bytes.
        let mut state_bytes = 0usize;
        let mut depth_bytes = self.depth_bytes;
        for (i, book) in snapshot.books.iter().enumerate() {
            if !self
                .snapshot
                .as_ref()
                .is_some_and(|s| s.books[i].bids == book.bids && s.books[i].asks == book.asks)
            {
                depth_bytes[i] = book
                    .bids
                    .iter()
                    .chain(&book.asks)
                    .map(|level| {
                        let n = level.encoded_len();
                        1 + prost::length_delimiter_len(n) + n
                    })
                    .sum();
            }
            let m = book
                .metadata
                .as_ref()
                .context("missing token metadata")?
                .encoded_len();
            let n = depth_bytes[i] + 1 + prost::length_delimiter_len(m) + m;
            state_bytes += 1 + prost::length_delimiter_len(n) + n;
        }
        let status_bytes = snapshot.source_status.len();
        if status_bytes > 0 {
            state_bytes += 1 + prost::length_delimiter_len(status_bytes) + status_bytes;
        }
        if let Some(reason) = &snapshot.reason {
            state_bytes += 1 + prost::length_delimiter_len(reason.len()) + reason.len();
        }
        ensure!(state_bytes <= MAX_BYTES, "typed state byte capacity");
        ensure!(
            self.envelope_bytes + 1 + prost::length_delimiter_len(state_bytes) + state_bytes
                <= MAX_BYTES,
            "typed frame byte capacity"
        );
        ensure!(
            snapshot.source_status != "available"
                || snapshot
                    .books
                    .iter()
                    .all(|b| b.metadata.as_ref().is_some_and(|m| m.data_valid)),
            "available state contains invalid book"
        );
        let changed = self.snapshot.as_deref() != Some(&snapshot);
        if changed || self.source_generation != source_generation {
            self.sequence = self
                .sequence
                .checked_add(1)
                .context("state revision overflow")?;
            if changed {
                self.snapshot = Some(Arc::new(snapshot));
            }
        }
        self.source_generation = source_generation;
        self.depth_bytes = depth_bytes;
        Ok(State {
            key: self.key.clone(),
            stream_id: self.stream_id.clone(),
            sequence: self.sequence,
            snapshot: self
                .snapshot
                .as_ref()
                .context("missing captured state")?
                .clone(),
            source_generation: self.source_generation.clone(),
        })
    }
}

pub fn frame(
    state: &State,
    previous: Option<&State>,
    published_at: String,
) -> Result<wire::MarketFrame> {
    let previous = previous.filter(|p| {
        p.key == state.key
            && p.stream_id == state.stream_id
            && p.source_generation == state.source_generation
            && p.snapshot.source_status == "available"
            && state.snapshot.source_status == "available"
    });
    let mut changes = Vec::new();
    if let Some(before) = previous {
        ensure!(
            before.sequence < state.sequence,
            "state revision regression"
        );
        for (old, new) in before.snapshot.books.iter().zip(&state.snapshot.books) {
            let token = &new.metadata.as_ref().context("metadata")?.token_id;
            for (side, old, new) in [
                ("BUY", &old.bids, &new.bids),
                ("SELL", &old.asks, &new.asks),
            ] {
                if old == new {
                    continue;
                }
                let (mut i, mut j) = (0, 0);
                while i < old.len() || j < new.len() {
                    let order = match (old.get(i), new.get(j)) {
                        (Some(a), Some(b)) => {
                            let order = decimal(&a.price, false)?.cmp(&decimal(&b.price, false)?);
                            if side == "BUY" {
                                order.reverse()
                            } else {
                                order
                            }
                        }
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        _ => break,
                    };
                    let update = match order {
                        std::cmp::Ordering::Equal => {
                            let value = (old[i].size != new[j].size)
                                .then(|| (new[j].price.clone(), new[j].size.clone()));
                            i += 1;
                            j += 1;
                            value
                        }
                        std::cmp::Ordering::Less => {
                            let value = Some((old[i].price.clone(), "0".into()));
                            i += 1;
                            value
                        }
                        std::cmp::Ordering::Greater => {
                            let value = Some((new[j].price.clone(), new[j].size.clone()));
                            j += 1;
                            value
                        }
                    };
                    if let Some((price, size)) = update {
                        changes.push(wire::DepthChange {
                            token_id: token.clone(),
                            side: side.into(),
                            price,
                            size,
                        });
                    }
                }
            }
        }
    }
    let body = if let Some(before) = previous.filter(|_| changes.len() <= MAX_CHANGES) {
        wire::market_frame::Body::PriceChange(wire::PriceChange {
            base_sequence: before.sequence,
            changes,
            post_metadata: state
                .snapshot
                .books
                .iter()
                .filter_map(|b| b.metadata.clone())
                .collect(),
            source_status: state.snapshot.source_status.clone(),
            reason: state.snapshot.reason.clone(),
            base_books: before
                .snapshot
                .books
                .iter()
                .filter_map(|b| b.metadata.as_ref())
                .map(|m| wire::BookBase {
                    token_id: m.token_id.clone(),
                    book_revision: m.book_revision,
                })
                .collect(),
        })
    } else {
        wire::market_frame::Body::Snapshot((*state.snapshot).clone())
    };
    let mut message = wire::MarketFrame {
        schema_version: MARKET_SCHEMA.into(),
        key: Some(state.key.clone()),
        stream_id: state.stream_id.clone(),
        state_sequence: state.sequence,
        published_at,
        body: Some(body),
    };
    if message.encoded_len() > MAX_BYTES
        && matches!(
            &message.body,
            Some(wire::market_frame::Body::PriceChange(_))
        )
    {
        // A net delta can exceed the complete book (many removals plus adds).
        // Switch to the same ordinary snapshot message, never chunk/control.
        message.body = Some(wire::market_frame::Body::Snapshot(
            (*state.snapshot).clone(),
        ));
    }
    ensure!(
        message.encoded_len() <= MAX_BYTES,
        "typed frame byte capacity"
    );
    Ok(message)
}

#[cfg(test)]
mod native_tests {
    use super::*;
    fn lane() -> Lane {
        Lane::new(
            wire::MarketKey {
                market_id: "123".into(),
                condition_id: format!("0x{}", "1".repeat(64)),
                window_id: "btc-updown-5m-1".into(),
                start_at: "2026-10-15T00:00:00Z".into(),
                end_at: "2026-10-15T00:05:00Z".into(),
                up_token_id: "1".into(),
                down_token_id: "2".into(),
                rules: Some(marketcow_contracts::typed_v3::common::RecordIdentity {
                    id: "rule-id".into(),
                    revision: 1,
                }),
            },
            "public-stream".into(),
        )
        .unwrap()
    }
    fn snapshot() -> wire::Snapshot {
        wire::Snapshot {
            books: [("1", "Up"), ("2", "Down")]
                .into_iter()
                .map(|(token, outcome)| wire::TokenBook {
                    metadata: Some(wire::TokenMetadata {
                        token_id: token.into(),
                        outcome: outcome.into(),
                        tick_size: Some("0.01".into()),
                        quote_source_at: Some("2026-10-15T00:00:01Z".into()),
                        data_valid: true,
                        ..Default::default()
                    }),
                    bids: vec![wire::PriceLevel {
                        price: "0.40".into(),
                        size: "5.00".into(),
                    }],
                    asks: vec![wire::PriceLevel {
                        price: "0.50".into(),
                        size: "6.00".into(),
                    }],
                })
                .collect(),
            source_status: "available".into(),
            reason: None,
        }
    }
    #[test]
    fn native_confirmation_only_update_preserves_book_revision() {
        let mut lane = lane();
        let first = lane.capture(snapshot(), "private-a".into()).unwrap();
        let mut next = (*first.snapshot).clone();
        let metadata = next.books[0].metadata.as_mut().unwrap();
        metadata.received_at = Some("2026-10-15T00:00:02Z".into());
        metadata.confirmation = Some(wire::Confirmation {
            source_at: "2026-10-15T00:00:02Z".into(),
            received_at: "2026-10-15T00:00:02Z".into(),
            source: "supplier".into(),
            bound_quote_source_at: metadata.quote_source_at.clone().unwrap(),
            bound_book_revision: metadata.book_revision,
            evidence_id: None,
        });
        let second = lane.capture(next, "private-a".into()).unwrap();
        assert_eq!(
            second.snapshot.books[0]
                .metadata
                .as_ref()
                .unwrap()
                .book_revision,
            1
        );
        let message = frame(&second, Some(&first), "2026-10-15T00:00:02Z".into()).unwrap();
        let Some(wire::market_frame::Body::PriceChange(delta)) = message.body else {
            panic!("expected ordinary metadata delta")
        };
        assert!(delta.changes.is_empty());
        assert_eq!(delta.base_sequence, first.sequence);
        assert_eq!(delta.base_books.len(), 2);
    }
    #[test]
    fn native_private_handoff_is_only_snapshot_on_same_public_stream() {
        let mut lane = lane();
        let first = lane.capture(snapshot(), "private-a".into()).unwrap();
        let second = lane
            .capture((*first.snapshot).clone(), "private-b".into())
            .unwrap();
        assert_eq!(first.stream_id, second.stream_id);
        assert!(second.sequence > first.sequence);
        assert!(matches!(
            frame(&second, Some(&first), "2026-10-15T00:00:02Z".into())
                .unwrap()
                .body,
            Some(wire::market_frame::Body::Snapshot(_))
        ));
        assert_eq!(
            first.snapshot.books[0]
                .metadata
                .as_ref()
                .unwrap()
                .book_revision,
            second.snapshot.books[0]
                .metadata
                .as_ref()
                .unwrap()
                .book_revision
        );
    }
    #[test]
    fn native_depth_removal_is_zero_size_delta_and_bad_confirmation_is_atomic() {
        let mut lane = lane();
        let first = lane.capture(snapshot(), "private-a".into()).unwrap();
        let mut invalid = (*first.snapshot).clone();
        invalid.books[0].metadata.as_mut().unwrap().confirmation = Some(wire::Confirmation {
            source_at: "2026-10-15T00:00:02Z".into(),
            received_at: "2026-10-15T00:00:02Z".into(),
            source: "supplier".into(),
            bound_quote_source_at: "2026-10-15T00:00:01Z".into(),
            bound_book_revision: 99,
            evidence_id: None,
        });
        assert!(lane.capture(invalid, "private-a".into()).is_err());
        assert_eq!(lane.sequence, first.sequence);
        let mut future = (*first.snapshot).clone();
        future.books[0].metadata.as_mut().unwrap().confirmation = Some(wire::Confirmation {
            source_at: "2026-10-15T00:00:03Z".into(),
            received_at: "2026-10-15T00:00:02Z".into(),
            source: "supplier".into(),
            bound_quote_source_at: "2026-10-15T00:00:01Z".into(),
            bound_book_revision: 0,
            evidence_id: None,
        });
        assert!(lane.capture(future, "private-a".into()).is_err());
        assert_eq!(lane.sequence, first.sequence);
        let mut next = (*first.snapshot).clone();
        next.books[0].bids.clear();
        let second = lane.capture(next, "private-a".into()).unwrap();
        let Some(wire::market_frame::Body::PriceChange(delta)) =
            frame(&second, Some(&first), "2026-10-15T00:00:02Z".into())
                .unwrap()
                .body
        else {
            panic!("expected price change")
        };
        assert_eq!(delta.changes.len(), 1);
        assert_eq!(delta.changes[0].size, "0");
        assert_eq!(delta.changes[0].side, "BUY");
        assert_eq!(
            second.snapshot.books[0]
                .metadata
                .as_ref()
                .unwrap()
                .book_revision,
            2
        );
    }
}
