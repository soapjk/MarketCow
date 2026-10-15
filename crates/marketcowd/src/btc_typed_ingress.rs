//! Supplier-specific JSON is parsed once here into owned typed records.
//! The owner, indexed writer and public API never parse that body again.
use anyhow::{Context, Result, ensure};
use marketcow_contracts::typed_v3::{btc, common::Source};
use serde::Deserialize;

#[derive(Deserialize)]
struct BinanceTrade {
    e: String,
    s: String,
    t: u64,
    p: String,
    q: String,
    m: bool,
    #[serde(rename = "T")]
    time: i64,
    #[serde(rename = "E")]
    event: i64,
}
#[derive(Deserialize)]
struct RecentTrade {
    id: u64,
    price: String,
    qty: String,
    time: i64,
    #[serde(rename = "isBuyerMaker")]
    maker: bool,
}
pub fn binance_trade(raw: &[u8], received_at_ns: i64) -> Result<btc::Trade> {
    ensure!(raw.len() <= 64 * 1024, "Binance frame capacity");
    let row: BinanceTrade = serde_json::from_slice(raw)?;
    ensure!(
        row.e == "trade" && row.s == "BTCUSDT",
        "Binance source identity"
    );
    let trade = btc::Trade {
        trade_id: row.t,
        trade_time_ms: row.time,
        event_time_ms: Some(row.event),
        price: row.p,
        quantity: row.q,
        buyer_is_maker: Some(row.m),
        received_at_ns: Some(received_at_ns),
        evidence: None,
    };
    super::btc_typed::validate_trade(&trade)?;
    Ok(trade)
}
pub fn binance_recent(raw: &[u8], received_at_ns: i64) -> Result<Vec<btc::Trade>> {
    ensure!(raw.len() <= 1024 * 1024, "Binance REST capacity");
    let rows: Vec<RecentTrade> = serde_json::from_slice(raw)?;
    ensure!(
        !rows.is_empty() && rows.len() <= 1000,
        "Binance REST row capacity"
    );
    let trades = rows
        .into_iter()
        .map(|r| btc::Trade {
            trade_id: r.id,
            trade_time_ms: r.time,
            event_time_ms: None,
            price: r.price,
            quantity: r.qty,
            buyer_is_maker: Some(r.maker),
            received_at_ns: Some(received_at_ns),
            evidence: None,
        })
        .collect::<Vec<_>>();
    for row in &trades {
        super::btc_typed::validate_trade(row)?;
    }
    for pair in trades.windows(2) {
        ensure!(
            pair[0].trade_id.checked_add(1) == Some(pair[1].trade_id),
            "Binance REST ID gap"
        );
    }
    Ok(trades)
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Dropped {
    Bool(bool),
    Count(u64),
}
#[derive(Deserialize)]
struct ChainlinkFrame {
    v: u32,
    channel: String,
    seq: u64,
    #[serde(default)]
    snapshot: bool,
    dropped: Option<Dropped>,
    payload: ChainlinkPayload,
}
#[derive(Deserialize)]
struct Point {
    timestamp: i64,
    full_accuracy_value: String,
}
#[derive(Deserialize)]
struct ChainlinkPayload {
    symbol: String,
    source: String,
    window_seconds: Option<u32>,
    data: Option<Vec<Point>>,
    timestamp: Option<i64>,
    full_accuracy_value: Option<String>,
}
pub struct Observations {
    pub snapshot: bool,
    pub source: Source,
    pub rows: Vec<btc::Observation>,
}
pub fn chainlink(raw: &[u8], received_at_ns: i64, expected: Source) -> Result<Observations> {
    ensure!(raw.len() <= 1024 * 1024, "Chainlink frame capacity");
    let frame: ChainlinkFrame = serde_json::from_slice(raw)?;
    ensure!(frame.v == 1 && frame.seq > 0, "Chainlink version/sequence");
    ensure!(
        match frame.dropped {
            None | Some(Dropped::Bool(false)) | Some(Dropped::Count(0)) => true,
            _ => false,
        },
        "Chainlink source dropped"
    );
    let payload = frame.payload;
    ensure!(
        payload.symbol == "btcusd" && payload.source == "chainlink",
        "Chainlink source identity"
    );
    match expected {
        Source::ChainlinkBtcUsdTwap60 => ensure!(
            frame.channel == "price.crypto.twap" && payload.window_seconds == Some(60),
            "Chainlink TWAP identity"
        ),
        Source::ChainlinkBtcUsd => ensure!(
            frame.channel == "price.crypto" && payload.window_seconds.is_none(),
            "Chainlink ordinary identity"
        ),
        _ => anyhow::bail!("Chainlink source unsupported"),
    }
    let points = if frame.snapshot {
        payload.data.context("Chainlink snapshot points missing")?
    } else {
        vec![Point {
            timestamp: payload.timestamp.context("Chainlink source time missing")?,
            full_accuracy_value: payload
                .full_accuracy_value
                .context("Chainlink exact price missing")?,
        }]
    };
    ensure!(
        !points.is_empty() && points.len() <= 4096,
        "Chainlink point capacity"
    );
    let mut rows = Vec::with_capacity(points.len());
    let mut previous = None;
    for (index, point) in points.into_iter().enumerate() {
        ensure!(
            point.timestamp > 0 && previous.is_none_or(|t| point.timestamp >= t),
            "Chainlink source time regression"
        );
        super::btc_typed::positive_decimal(&point.full_accuracy_value)?;
        previous = Some(point.timestamp);
        rows.push(btc::Observation {
            source_time_ms: point.timestamp,
            price: point.full_accuracy_value,
            source_sequence: Some(frame.seq),
            row_index: Some(index as u32),
            received_at_ns: Some(received_at_ns),
            evidence: None,
        });
    }
    Ok(Observations {
        snapshot: frame.snapshot,
        source: expected,
        rows,
    })
}
