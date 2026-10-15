//! Compatibility serializer for the existing v6 mandatory state commitment.
//! This boundary does not construct serde_json::Value or a second book model.
use marketcow_contracts::btc5m_market_local_v6::wire;
use serde::{Serialize, Serializer, ser::SerializeSeq};

#[derive(Serialize)]
struct Level<'a> {
    price: &'a str,
    size: &'a str,
}
pub struct Levels<'a>(pub &'a [wire::PriceLevel]);
impl Serialize for Levels<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for row in self.0 {
            seq.serialize_element(&Level {
                price: &row.price,
                size: &row.size,
            })?;
        }
        seq.end()
    }
}
#[derive(Serialize)]
struct Confirmation<'a> {
    bound_depth_sha256: String,
    bound_quote_source_at: &'a str,
    evidence_sha256: String,
    received_at: &'a str,
    source: &'a str,
    source_at: &'a str,
}
#[derive(Serialize)]
struct Trade<'a> {
    price: &'a str,
    source_at: Option<&'a str>,
}
#[derive(Serialize)]
struct Metadata<'a> {
    confirmation: Option<Confirmation<'a>>,
    data_valid: bool,
    invalid_reason: Option<&'a str>,
    last_trade: Option<Trade<'a>>,
    outcome: &'a str,
    quote_source_at: Option<&'a str>,
    received_at: Option<&'a str>,
    tick_size: Option<&'a str>,
    token_id: &'a str,
}
fn metadata(m: &wire::TokenMetadata) -> Metadata<'_> {
    Metadata {
        confirmation: m.confirmation.as_ref().map(|f| Confirmation {
            bound_depth_sha256: hex::encode(&f.bound_depth_sha256),
            bound_quote_source_at: &f.bound_quote_source_at,
            evidence_sha256: hex::encode(&f.evidence_sha256),
            received_at: &f.received_at,
            source: &f.source,
            source_at: &f.source_at,
        }),
        data_valid: m.data_valid,
        invalid_reason: m.invalid_reason.as_deref(),
        last_trade: m.last_trade.as_ref().map(|t| Trade {
            price: &t.price,
            source_at: t.source_at.as_deref(),
        }),
        outcome: &m.outcome,
        quote_source_at: m.quote_source_at.as_deref(),
        received_at: m.received_at.as_deref(),
        tick_size: m.tick_size.as_deref(),
        token_id: &m.token_id,
    }
}
#[derive(Serialize)]
struct Book<'a> {
    asks: Levels<'a>,
    bids: Levels<'a>,
    metadata: Option<Metadata<'a>>,
}
struct Books<'a>(&'a [wire::TokenBook]);
impl Serialize for Books<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for b in self.0 {
            seq.serialize_element(&Book {
                asks: Levels(&b.asks),
                bids: Levels(&b.bids),
                metadata: b.metadata.as_ref().map(metadata),
            })?;
        }
        seq.end()
    }
}
#[derive(Serialize)]
struct Key<'a> {
    condition_id: &'a str,
    down_token_id: &'a str,
    end_at: &'a str,
    market_id: &'a str,
    rule_sha256: String,
    start_at: &'a str,
    up_token_id: &'a str,
    window_id: &'a str,
}
#[derive(Serialize)]
struct State<'a> {
    books: Books<'a>,
    key: Key<'a>,
    reason: Option<&'a str>,
    schema_version: &'a str,
    source_status: &'a str,
    state_sequence: u64,
    stream_id: &'a str,
}
pub fn state(
    key: &wire::MarketKey,
    stream: &str,
    sequence: u64,
    snapshot: &wire::Snapshot,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&State {
        books: Books(&snapshot.books),
        key: Key {
            condition_id: &key.condition_id,
            down_token_id: &key.down_token_id,
            end_at: &key.end_at,
            market_id: &key.market_id,
            rule_sha256: hex::encode(&key.rule_sha256),
            start_at: &key.start_at,
            up_token_id: &key.up_token_id,
            window_id: &key.window_id,
        },
        reason: snapshot.reason.as_deref(),
        schema_version: marketcow_contracts::btc5m_market_local_v6::SCHEMA_VERSION,
        source_status: &snapshot.source_status,
        state_sequence: sequence,
        stream_id: stream,
    })
}
#[derive(Serialize)]
struct Depth<'a> {
    asks: Levels<'a>,
    bids: Levels<'a>,
    tick_size: Option<&'a str>,
    token_id: &'a str,
}
pub fn depth(
    token: &str,
    bids: &[wire::PriceLevel],
    asks: &[wire::PriceLevel],
    tick: Option<&str>,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&Depth {
        asks: Levels(asks),
        bids: Levels(bids),
        tick_size: tick,
        token_id: token,
    })
}
