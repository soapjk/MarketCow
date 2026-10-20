//! Offline acceptance cases. Written with the first implementation batch;
//! execution is reserved for the consolidated SDK verification phase.
use marketcow_wire::{MARKET_SCHEMA, PRICE_SCHEMA};
use marketcow_sdk::{Client, PriceState, book::MarketState, btc, common, market};

fn trade(id: u64) -> btc::Trade {
    btc::Trade {
        trade_id: id,
        trade_time_ms: 1000 + id as i64,
        price: "100.00".into(),
        quantity: "0.0100".into(),
        buyer_is_maker: Some(false),
        ..Default::default()
    }
}

fn snapshot(sequence: u64, generation: &str) -> btc::PriceFrame {
    btc::PriceFrame {
        schema_version: PRICE_SCHEMA.into(),
        producer_instance_id: "producer".into(),
        source: common::Source::BinanceSpotTrade as i32,
        instrument_id: "BTCUSDT.BINANCE".into(),
        stream_id: generation.into(),
        stream_sequence: sequence,
        source_sequence: Some(41),
        latest_source_age_ms_at_emit: Some(-2),
        payload: Some(btc::price_frame::Payload::Snapshot(btc::Snapshot {
            trades: vec![trade(40), trade(41)],
            first_trade_id: Some(40),
            last_trade_id: Some(41),
            retained_count: 2,
            ..Default::default()
        })),
        ..Default::default()
    }
}

#[test]
fn signed_age_is_not_a_transport_failure() {
    let mut state = PriceState::new(common::Source::BinanceSpotTrade).unwrap();
    state.apply(&snapshot(100, "a")).unwrap();
    assert!(state.available);
    assert_eq!(state.signed_age_ms, Some(-2));
    assert_eq!(state.latest_trade.unwrap().price, "100.00");
}

#[test]
fn queued_initial_frames_are_ignored_but_later_gaps_rejected() {
    let mut state = PriceState::new(common::Source::BinanceSpotTrade).unwrap();
    state.apply(&snapshot(100, "a")).unwrap();
    assert!(!state.apply(&snapshot(99, "older")).unwrap());
    assert!(state.apply(&snapshot(102, "a")).is_err());
    assert_eq!(state.sequence, Some(100));
}

#[test]
fn unavailable_requires_snapshot_for_recovery() {
    let mut state = PriceState::new(common::Source::BinanceSpotTrade).unwrap();
    let mut frame = snapshot(100, "a");
    state.apply(&frame).unwrap();
    frame.stream_sequence = 101;
    frame.payload = Some(btc::price_frame::Payload::Status(btc::Status {
        phase: btc::Phase::Unavailable as i32,
        reason: Some("upstream_reset".into()),
    }));
    state.apply(&frame).unwrap();
    assert!(!state.available);
    frame.stream_sequence = 102;
    frame.payload = Some(btc::price_frame::Payload::Trade(btc::TradeDelta {
        trade: Some(trade(42)),
    }));
    assert!(state.apply(&frame).is_err());
    state.apply(&snapshot(102, "b")).unwrap();
    assert!(state.available);
    assert_eq!(state.stream_id.as_deref(), Some("b"));
}

fn market_state() -> (MarketState, market::MarketFrame) {
    let key = market::MarketKey {
        market_id: "market".into(),
        condition_id: "condition".into(),
        up_token_id: "up".into(),
        down_token_id: "down".into(),
        rules: Some(common::RecordIdentity {
            id: "rules".into(),
            revision: 1,
        }),
        ..Default::default()
    };
    let books = [("up", "Up"), ("down", "Down")]
        .into_iter()
        .map(|(id, outcome)| market::TokenBook {
            metadata: Some(market::TokenMetadata {
                token_id: id.into(),
                outcome: outcome.into(),
                book_revision: 1,
                data_valid: true,
                ..Default::default()
            }),
            bids: vec![market::PriceLevel {
                price: "0.4".into(),
                size: "5".into(),
            }],
            asks: vec![],
        })
        .collect();
    let state = MarketState::new(
        market::MarketBinding {
            key: Some(key.clone()),
            stream_id: "stream".into(),
            role: "current".into(),
        },
        market::DeliveryBudget {
            maximum_frame_bytes: 8 * 1024 * 1024,
            maximum_state_bytes: 8 * 1024 * 1024,
            maximum_levels_per_side: 100,
            maximum_changes: 100,
        },
    )
    .unwrap();
    let frame = market::MarketFrame {
        schema_version: MARKET_SCHEMA.into(),
        key: Some(key),
        stream_id: "stream".into(),
        state_sequence: 1,
        published_at: String::new(),
        body: Some(market::market_frame::Body::Snapshot(market::Snapshot {
            books,
            source_status: "available".into(),
            reason: None,
        })),
    };
    (state, frame)
}

#[test]
fn metadata_updates_do_not_advance_book_revision() {
    let (mut state, mut frame) = market_state();
    state.apply(&frame).unwrap();
    let mut post: Vec<_> = state.books.iter().map(|b| b.metadata.clone()).collect();
    post[0].received_at = Some("diagnostic-only".into());
    frame.state_sequence = 3; // Ordinary state revisions may skip.
    frame.body = Some(market::market_frame::Body::PriceChange(
        market::PriceChange {
            base_sequence: 1,
            post_metadata: post,
            base_books: vec![
                market::BookBase {
                    token_id: "up".into(),
                    book_revision: 1,
                },
                market::BookBase {
                    token_id: "down".into(),
                    book_revision: 1,
                },
            ],
            source_status: "available".into(),
            ..Default::default()
        },
    ));
    state.apply(&frame).unwrap();
    assert_eq!(state.books[0].metadata.book_revision, 1);
    assert_eq!(state.sequence, Some(3));
}

#[test]
fn ordinary_snapshot_replaces_absent_levels_without_retaining_old_depth() {
    let (mut state, mut frame) = market_state();
    state.apply(&frame).unwrap();
    let mut books = state
        .books
        .iter()
        .map(|b| market::TokenBook {
            metadata: Some(b.metadata.clone()),
            bids: Vec::new(),
            asks: Vec::new(),
        })
        .collect::<Vec<_>>();
    books[0].metadata.as_mut().unwrap().book_revision = 2;
    books[1].metadata.as_mut().unwrap().book_revision = 2;
    frame.state_sequence = 5;
    frame.body = Some(market::market_frame::Body::Snapshot(market::Snapshot {
        books,
        source_status: "available".into(),
        reason: None,
    }));
    state.apply(&frame).unwrap();
    assert_eq!(state.sequence, Some(5));
    assert!(
        state
            .books
            .iter()
            .all(|b| b.bids.is_empty() && b.asks.is_empty())
    );
}

#[test]
fn bad_delta_never_partially_mutates_current_books() {
    let (mut state, mut frame) = market_state();
    state.apply(&frame).unwrap();
    frame.state_sequence = 2;
    frame.body = Some(market::market_frame::Body::PriceChange(
        market::PriceChange {
            base_sequence: 1,
            changes: vec![
                market::DepthChange {
                    token_id: "up".into(),
                    side: "BUY".into(),
                    price: "0.4".into(),
                    size: "0".into(),
                },
                market::DepthChange {
                    token_id: "invalid".into(),
                    side: "BUY".into(),
                    price: "0.5".into(),
                    size: "2".into(),
                },
            ],
            post_metadata: state.books.iter().map(|b| b.metadata.clone()).collect(),
            base_books: vec![market::BookBase::default(), market::BookBase::default()],
            ..Default::default()
        },
    ));
    assert!(state.apply(&frame).is_err());
    assert_eq!(state.sequence, Some(1));
    assert_eq!(state.books[0].bids.len(), 1);
}

#[test]
fn credentials_in_endpoint_are_rejected() {
    assert!(
        Client::builder("https://secret:token@example.com")
            .build()
            .is_err()
    );
}

#[test]
fn explicit_depth_view_is_stable_after_owner_updates() {
    let (mut state, mut frame) = market_state();
    state.apply(&frame).unwrap();
    let view = state.depth_view(Some(1)).unwrap();
    let old_top = state.top();
    if let Some(market::market_frame::Body::Snapshot(snapshot)) = &mut frame.body {
        snapshot.books[0].metadata.as_mut().unwrap().book_revision = 2;
        snapshot.books[0].bids[0].size = "7".into();
    }
    frame.state_sequence = 2;
    state.apply(&frame).unwrap();
    assert_eq!(view.books[0].bids[0].size, "5");
    assert_eq!(old_top.levels[0].as_ref().unwrap().1, "5");
    assert_eq!(
        state.depth_view(Some(1)).unwrap().books[0].bids[0].size,
        "7"
    );
}
