//! Offline local decode/apply benchmark. No I/O, supplier clocks or network SLA.
use marketcow_sdk::{PriceState, book::MarketState, btc, common, market};
use prost::Message;
use std::time::Instant;
fn trade(id: u64) -> btc::Trade {
    btc::Trade {
        trade_id: id,
        trade_time_ms: 1000 + id as i64,
        price: "100.00000000".into(),
        quantity: "0.01000000".into(),
        buyer_is_maker: Some(false),
        ..Default::default()
    }
}
fn frame(id: u64) -> btc::PriceFrame {
    btc::PriceFrame {
        schema_version: "marketcow.btc-price-stream.v3".into(),
        producer_instance_id: "bench".into(),
        stream_id: "a".into(),
        stream_sequence: id,
        source: common::Source::BinanceSpotTrade as i32,
        instrument_id: "BTCUSDT.BINANCE".into(),
        source_sequence: Some(id),
        payload: Some(btc::price_frame::Payload::Trade(btc::TradeDelta {
            trade: Some(trade(id)),
        })),
        ..Default::default()
    }
}
fn main() {
    let count = 100_000u64;
    let frames = (2..=count + 1)
        .map(|id| frame(id).encode_to_vec())
        .collect::<Vec<_>>();
    let mut state = PriceState::new(common::Source::BinanceSpotTrade).unwrap();
    let mut initial = frame(1);
    initial.payload = Some(btc::price_frame::Payload::Snapshot(btc::Snapshot {
        trades: vec![trade(1)],
        first_trade_id: Some(1),
        last_trade_id: Some(1),
        retained_count: 1,
        ..Default::default()
    }));
    state.apply(&initial).unwrap();
    let mut times = Vec::with_capacity(count as usize);
    let started = Instant::now();
    for bytes in &frames {
        let at = Instant::now();
        let frame = btc::PriceFrame::decode(bytes.as_slice()).unwrap();
        state.apply(&frame).unwrap();
        times.push(at.elapsed().as_nanos());
    }
    let elapsed = started.elapsed();
    times.sort_unstable();
    let q = |p: usize| times[(times.len() - 1) * p / 100];
    println!(
        "stage=trade_decode_apply samples={count} p50_ns={} p95_ns={} p99_ns={} max_ns={} elapsed_ms={} throughput_per_s={:.0}",
        q(50),
        q(95),
        q(99),
        times[times.len() - 1],
        elapsed.as_millis(),
        count as f64 / elapsed.as_secs_f64()
    );
    println!(
        "scope=local_synthetic_decode_apply excludes=network_python_delivery_and_bar_or_book_processing"
    );
    book_bench();
}
fn book_bench() {
    let key = market::MarketKey {
        market_id: "benchmark".into(),
        condition_id: "condition".into(),
        up_token_id: "up".into(),
        down_token_id: "down".into(),
        rules: Some(common::RecordIdentity {
            id: "rules".into(),
            revision: 1,
        }),
        ..Default::default()
    };
    let metadata = |token: &str, outcome: &str| market::TokenMetadata {
        token_id: token.into(),
        outcome: outcome.into(),
        book_revision: 1,
        data_valid: true,
        ..Default::default()
    };
    let books = [("up", "Up"), ("down", "Down")]
        .into_iter()
        .map(|(token, outcome)| market::TokenBook {
            metadata: Some(metadata(token, outcome)),
            bids: (1..=1000)
                .map(|n| market::PriceLevel {
                    price: format!("0.{n:04}"),
                    size: "1".into(),
                })
                .collect(),
            asks: Vec::new(),
        })
        .collect();
    let mut state = MarketState::new(
        market::MarketBinding {
            key: Some(key.clone()),
            stream_id: "a".into(),
            role: "current".into(),
        },
        market::DeliveryBudget {
            maximum_frame_bytes: 8 * 1024 * 1024,
            maximum_state_bytes: 8 * 1024 * 1024,
            maximum_levels_per_side: 1000,
            maximum_changes: 1000,
        },
    )
    .unwrap();
    let mut frame = market::MarketFrame {
        schema_version: "marketcow.polymarket.btc5m.market-local.v7".into(),
        key: Some(key),
        stream_id: "a".into(),
        state_sequence: 1,
        body: Some(market::market_frame::Body::Snapshot(market::Snapshot {
            books,
            source_status: "available".into(),
            reason: None,
        })),
        published_at: String::new(),
    };
    state.apply(&frame).unwrap();
    let count = 50_000u64;
    let mut frames = Vec::with_capacity(count as usize);
    for sequence in 2..=count + 1 {
        let mut up = metadata("up", "Up");
        up.book_revision = sequence;
        frame.state_sequence = sequence;
        frame.body = Some(market::market_frame::Body::PriceChange(
            market::PriceChange {
                base_sequence: sequence - 1,
                changes: vec![market::DepthChange {
                    token_id: "up".into(),
                    side: "BUY".into(),
                    price: "0.0500".into(),
                    size: if sequence % 2 == 0 { "2" } else { "1" }.into(),
                }],
                post_metadata: vec![up, metadata("down", "Down")],
                base_books: vec![
                    market::BookBase {
                        token_id: "up".into(),
                        book_revision: sequence - 1,
                    },
                    market::BookBase {
                        token_id: "down".into(),
                        book_revision: 1,
                    },
                ],
                source_status: "available".into(),
                reason: None,
            },
        ));
        frames.push(frame.encode_to_vec());
    }
    let mut times = Vec::with_capacity(count as usize);
    let started = Instant::now();
    for bytes in frames {
        let at = Instant::now();
        let frame = market::MarketFrame::decode(bytes.as_slice()).unwrap();
        state.apply(&frame).unwrap();
        times.push(at.elapsed().as_nanos());
    }
    let elapsed = started.elapsed();
    times.sort_unstable();
    let q = |p: usize| times[(times.len() - 1) * p / 100];
    println!(
        "stage=book_decode_apply samples={count} retained_levels=2000 changes_per_frame=1 p50_ns={} p95_ns={} p99_ns={} max_ns={} elapsed_ms={} throughput_per_s={:.0}",
        q(50),
        q(95),
        q(99),
        times[times.len() - 1],
        elapsed.as_millis(),
        count as f64 / elapsed.as_secs_f64()
    );
}
