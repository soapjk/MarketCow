//! Ordinary snapshot replacement / absolute depth deltas. No whole-book hash,
//! JSON staging, or whole-book clone for an ordinary price change.
use crate::{Error, Result, error::require, market, price::decimal};
use marketcow_wire::MARKET_SCHEMA;
use prost::Message;
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq)]
pub struct Level {
    pub price: String,
    pub size: String,
    numeric_size: Decimal,
    encoded_bytes: usize,
}

#[derive(Debug)]
pub struct Book {
    pub metadata: market::TokenMetadata,
    pub bids: BTreeMap<Decimal, Level>,
    pub asks: BTreeMap<Decimal, Level>,
    depth_bytes: usize,
}

impl Book {
    pub fn best_bid(&self) -> Option<&Level> {
        self.bids.last_key_value().map(|(_, v)| v)
    }
    pub fn best_ask(&self) -> Option<&Level> {
        self.asks.first_key_value().map(|(_, v)| v)
    }
}

pub struct MarketState {
    pub key: market::MarketKey,
    pub stream_id: String,
    pub sequence: Option<u64>,
    pub books: Vec<Book>,
    pub source_status: String,
    pub reason: Option<String>,
    pub published_at: String,
    budget: market::DeliveryBudget,
}

/// A stable explicitly requested view; truncation is not a full wire snapshot.
pub struct DepthSnapshot {
    pub key: market::MarketKey,
    pub stream_id: String,
    pub sequence: Option<u64>,
    pub published_at: String,
    pub source_status: String,
    pub reason: Option<String>,
    pub books: Vec<BookDepth>,
}
pub struct BookDepth {
    pub metadata: market::TokenMetadata,
    pub bids: Vec<market::PriceLevel>,
    pub asks: Vec<market::PriceLevel>,
    pub truncated: bool,
}
#[derive(Debug)]
pub struct MarketTop {
    pub available: bool,
    pub levels: [Option<(String, String)>; 4],
}

fn metadata(value: &market::TokenMetadata, token: &str, outcome: &str) -> Result<()> {
    require(
        value.token_id == token && value.outcome == outcome && value.book_revision > 0,
        "book token/outcome/revision",
    )?;
    if let Some(tick) = &value.tick_size {
        decimal(tick, true)?;
    }
    if let Some(trade) = &value.last_trade {
        decimal(&trade.price, false)?;
    }
    if let Some(confirm) = &value.confirmation {
        require(
            confirm.bound_book_revision == value.book_revision
                && Some(confirm.bound_quote_source_at.as_str()) == value.quote_source_at.as_deref()
                && !confirm.source_at.is_empty(),
            "confirmation book binding",
        )?;
    }
    Ok(())
}

fn economic_metadata_equal(a: &market::TokenMetadata, b: &market::TokenMetadata) -> bool {
    let tick = |v: &Option<String>| v.as_ref().and_then(|s| decimal(s, true).ok());
    tick(&a.tick_size) == tick(&b.tick_size)
        && a.quote_source_at == b.quote_source_at
        && a.data_valid == b.data_valid
        && a.invalid_reason == b.invalid_reason
}

fn level(price: &str, size: &str) -> Result<(Decimal, Level)> {
    let numeric_price = decimal(price, false)?;
    require(
        numeric_price <= Decimal::ONE,
        "prediction price exceeds one",
    )?;
    let numeric_size = decimal(size, false)?;
    let encoded_bytes = market::PriceLevel {
        price: price.into(),
        size: size.into(),
    }
    .encoded_len();
    Ok((
        numeric_price,
        Level {
            price: price.into(),
            size: size.into(),
            numeric_size,
            encoded_bytes,
        },
    ))
}

fn varint_bytes(mut n: usize) -> usize {
    let mut bytes = 1;
    while n >= 128 {
        n >>= 7;
        bytes += 1;
    }
    bytes
}

// All enclosing field tags here are one byte. Exact encoded lengths can be
// maintained from touched levels, without serializing/walking the whole book.
fn nested_bytes(n: usize) -> usize {
    1 + varint_bytes(n) + n
}
fn string_bytes(value: &str, optional: bool) -> usize {
    if value.is_empty() && !optional {
        0
    } else {
        nested_bytes(value.len())
    }
}

fn side(values: &[market::PriceLevel], maximum: u64) -> Result<(BTreeMap<Decimal, Level>, usize)> {
    if values.len() as u64 > maximum {
        return Err(Error::Limit("book levels"));
    }
    let mut map = BTreeMap::new();
    let mut bytes = 0;
    for value in values {
        let (price, value) = level(&value.price, &value.size)?;
        require(
            value.numeric_size > Decimal::ZERO && !map.contains_key(&price),
            "snapshot zero/duplicate level",
        )?;
        bytes += nested_bytes(value.encoded_bytes);
        map.insert(price, value);
    }
    Ok((map, bytes))
}

fn same_side(a: &BTreeMap<Decimal, Level>, b: &BTreeMap<Decimal, Level>) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|((pa, a), (pb, b))| pa == pb && a.numeric_size == b.numeric_size)
}

impl MarketState {
    pub fn top(&self) -> MarketTop {
        let at = |index: usize, ask: bool| {
            self.books
                .get(index)
                .and_then(|b| if ask { b.best_ask() } else { b.best_bid() })
                .map(|v| (v.price.clone(), v.size.clone()))
        };
        MarketTop {
            available: self.available(),
            levels: [at(0, false), at(0, true), at(1, false), at(1, true)],
        }
    }
    pub fn depth_view(&self, depth: Option<usize>) -> Result<DepthSnapshot> {
        if depth.is_some_and(|d| d as u64 > self.budget.maximum_levels_per_side) {
            return Err(Error::Limit("requested depth"));
        }
        let n = depth.unwrap_or(usize::MAX);
        let books = self
            .books
            .iter()
            .map(|b| BookDepth {
                metadata: b.metadata.clone(),
                truncated: b.bids.len() > n || b.asks.len() > n,
                bids: b
                    .bids
                    .values()
                    .rev()
                    .take(n)
                    .map(|l| market::PriceLevel {
                        price: l.price.clone(),
                        size: l.size.clone(),
                    })
                    .collect(),
                asks: b
                    .asks
                    .values()
                    .take(n)
                    .map(|l| market::PriceLevel {
                        price: l.price.clone(),
                        size: l.size.clone(),
                    })
                    .collect(),
            })
            .collect();
        Ok(DepthSnapshot {
            key: self.key.clone(),
            stream_id: self.stream_id.clone(),
            sequence: self.sequence,
            published_at: self.published_at.clone(),
            source_status: self.source_status.clone(),
            reason: self.reason.clone(),
            books,
        })
    }
    pub fn new(binding: market::MarketBinding, budget: market::DeliveryBudget) -> Result<Self> {
        let key = binding
            .key
            .ok_or_else(|| Error::Protocol("missing market key".into()))?;
        require(
            !key.market_id.is_empty()
                && !key.condition_id.is_empty()
                && !key.up_token_id.is_empty()
                && !key.down_token_id.is_empty()
                && key.up_token_id != key.down_token_id
                && key
                    .rules
                    .as_ref()
                    .is_some_and(|r| !r.id.is_empty() && r.revision > 0)
                && !binding.stream_id.is_empty(),
            "market identity",
        )?;
        require(
            budget.maximum_frame_bytes > 0
                && budget.maximum_state_bytes > 0
                && budget.maximum_levels_per_side > 0
                && budget.maximum_changes > 0,
            "market budget",
        )?;
        Ok(Self {
            key,
            stream_id: binding.stream_id,
            budget,
            sequence: None,
            books: Vec::new(),
            source_status: "unavailable".into(),
            reason: None,
            published_at: String::new(),
        })
    }

    pub fn available(&self) -> bool {
        self.source_status == "available"
    }

    pub fn apply(&mut self, frame: &market::MarketFrame) -> Result<()> {
        require(
            frame.schema_version == MARKET_SCHEMA
                && frame.key.as_ref() == Some(&self.key)
                && frame.stream_id == self.stream_id
                && self.sequence.is_none_or(|s| frame.state_sequence > s),
            "market frame identity/sequence",
        )?;
        match frame
            .body
            .as_ref()
            .ok_or_else(|| Error::Protocol("missing market body".into()))?
        {
            market::market_frame::Body::Snapshot(snapshot) => self.replace(snapshot)?,
            market::market_frame::Body::PriceChange(change) => self.change(change)?,
        }
        self.sequence = Some(frame.state_sequence);
        self.published_at = frame.published_at.clone();
        Ok(())
    }

    /// Explicit snapshot creation, never used on the ordinary delta hot path.
    pub fn snapshot(&self) -> Option<market::MarketFrame> {
        let sequence = self.sequence?;
        let books = self
            .books
            .iter()
            .map(|b| market::TokenBook {
                metadata: Some(b.metadata.clone()),
                bids: b
                    .bids
                    .values()
                    .rev()
                    .map(|l| market::PriceLevel {
                        price: l.price.clone(),
                        size: l.size.clone(),
                    })
                    .collect(),
                asks: b
                    .asks
                    .values()
                    .map(|l| market::PriceLevel {
                        price: l.price.clone(),
                        size: l.size.clone(),
                    })
                    .collect(),
            })
            .collect();
        Some(market::MarketFrame {
            schema_version: MARKET_SCHEMA.into(),
            key: Some(self.key.clone()),
            stream_id: self.stream_id.clone(),
            state_sequence: sequence,
            published_at: self.published_at.clone(),
            body: Some(market::market_frame::Body::Snapshot(market::Snapshot {
                books,
                source_status: self.source_status.clone(),
                reason: self.reason.clone(),
            })),
        })
    }

    fn replace(&mut self, snapshot: &market::Snapshot) -> Result<()> {
        require(snapshot.books.len() == 2, "snapshot must contain two books")?;
        if snapshot.encoded_len() as u64 > self.budget.maximum_state_bytes {
            return Err(Error::Limit("snapshot state bytes"));
        }
        let mut books = Vec::with_capacity(2);
        for (index, value) in snapshot.books.iter().enumerate() {
            let token = if index == 0 {
                &self.key.up_token_id
            } else {
                &self.key.down_token_id
            };
            let meta = value
                .metadata
                .as_ref()
                .ok_or_else(|| Error::Protocol("book metadata missing".into()))?;
            metadata(meta, token, if index == 0 { "Up" } else { "Down" })?;
            let (bids, bid_bytes) = side(&value.bids, self.budget.maximum_levels_per_side)?;
            let (asks, ask_bytes) = side(&value.asks, self.budget.maximum_levels_per_side)?;
            if let Some(old) = self.books.get(index) {
                require(
                    meta.book_revision >= old.metadata.book_revision,
                    "snapshot book revision regression",
                )?;
                if meta.book_revision == old.metadata.book_revision {
                    require(
                        economic_metadata_equal(meta, &old.metadata)
                            && same_side(&bids, &old.bids)
                            && same_side(&asks, &old.asks),
                        "same revision changed book facts",
                    )?;
                }
            }
            books.push(Book {
                metadata: meta.clone(),
                bids,
                asks,
                depth_bytes: bid_bytes + ask_bytes,
            });
        }
        // All validation happened off the current state. O(1) container swap.
        self.books = books;
        self.source_status = snapshot.source_status.clone();
        self.reason = snapshot.reason.clone();
        Ok(())
    }

    fn change(&mut self, change: &market::PriceChange) -> Result<()> {
        require(
            self.sequence == Some(change.base_sequence)
                && self.books.len() == 2
                && change.base_books.len() == 2
                && change.post_metadata.len() == 2,
            "delta base/shape",
        )?;
        if change.changes.len() as u64 > self.budget.maximum_changes {
            return Err(Error::Limit("depth changes"));
        }
        let mut changed = [false; 2];
        let mut bytes = [self.books[0].depth_bytes, self.books[1].depth_bytes];
        let mut counts = [
            [self.books[0].bids.len(), self.books[0].asks.len()],
            [self.books[1].bids.len(), self.books[1].asks.len()],
        ];
        let mut seen = BTreeSet::new();
        let mut staged = Vec::with_capacity(change.changes.len());
        for delta in &change.changes {
            let index = if delta.token_id == self.key.up_token_id {
                0
            } else if delta.token_id == self.key.down_token_id {
                1
            } else {
                return Err(Error::Protocol("unknown depth token".into()));
            };
            let ask = match delta.side.as_str() {
                "BUY" => false,
                "SELL" => true,
                _ => return Err(Error::Protocol("unknown depth side".into())),
            };
            let (price, value) = level(&delta.price, &delta.size)?;
            require(seen.insert((index, ask, price)), "duplicate depth change")?;
            let side = if ask {
                &self.books[index].asks
            } else {
                &self.books[index].bids
            };
            let old = side.get(&price);
            changed[index] |= old.map_or(Decimal::ZERO, |v| v.numeric_size) != value.numeric_size;
            if let Some(old) = old {
                bytes[index] -= nested_bytes(old.encoded_bytes);
            }
            if value.numeric_size > Decimal::ZERO {
                bytes[index] += nested_bytes(value.encoded_bytes);
            }
            let count = &mut counts[index][usize::from(ask)];
            match (old.is_some(), value.numeric_size > Decimal::ZERO) {
                (false, true) => *count += 1,
                (true, false) => *count -= 1,
                _ => {}
            }
            if *count as u64 > self.budget.maximum_levels_per_side {
                return Err(Error::Limit("book levels"));
            }
            staged.push((index, ask, price, value));
        }
        let mut state_bytes = string_bytes(&change.source_status, false)
            + change.reason.as_ref().map_or(0, |s| string_bytes(s, true));
        for index in 0..2 {
            let old = &self.books[index].metadata;
            let post = &change.post_metadata[index];
            metadata(post, &old.token_id, &old.outcome)?;
            require(
                change.base_books[index].token_id == old.token_id
                    && change.base_books[index].book_revision == old.book_revision,
                "token book base",
            )?;
            changed[index] |= !economic_metadata_equal(old, post);
            require(
                if changed[index] {
                    post.book_revision > old.book_revision
                } else {
                    post.book_revision == old.book_revision
                },
                "book revision/facts relationship",
            )?;
            state_bytes += nested_bytes(bytes[index] + nested_bytes(post.encoded_len()));
        }
        if state_bytes as u64 > self.budget.maximum_state_bytes {
            return Err(Error::Limit("book state bytes"));
        }
        // Commit only touched levels, after every failure-producing check.
        for (index, ask, price, value) in staged {
            let side = if ask {
                &mut self.books[index].asks
            } else {
                &mut self.books[index].bids
            };
            if value.numeric_size.is_zero() {
                side.remove(&price);
            } else {
                side.insert(price, value);
            }
        }
        for (index, bytes) in bytes.into_iter().enumerate() {
            self.books[index].metadata = change.post_metadata[index].clone();
            self.books[index].depth_bytes = bytes;
        }
        self.source_status = change.source_status.clone();
        self.reason = change.reason.clone();
        Ok(())
    }
}
