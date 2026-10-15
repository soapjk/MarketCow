//! Fixed v4 wire example. Run only in the final, combined acceptance phase.
use marketcow_contracts::btc5m_market_local_v4::{self as contract, wire};
use serde_json::json;
use sha2::{Digest, Sha256};

fn sha(bytes: &[u8]) -> Vec<u8> {
    Sha256::digest(bytes).to_vec()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn pn1_checksum(bids: &[(&str, &str)], asks: &[(&str, &str)]) -> Vec<u8> {
    let mut accumulator = [0_u8; 32];
    for (side, levels) in [(b'B', bids), (b'A', asks)] {
        for (price, size) in levels {
            let mut input = b"pn1:level\0".to_vec();
            input.extend_from_slice(&[side, 0]);
            input.extend_from_slice(price.as_bytes());
            input.push(0);
            input.extend_from_slice(size.as_bytes());
            for (left, right) in accumulator.iter_mut().zip(sha(&input)) {
                *left ^= right;
            }
        }
    }
    let mut input = b"pn1:book\0".to_vec();
    input.extend_from_slice(bids.len().to_string().as_bytes());
    input.push(0);
    input.extend_from_slice(asks.len().to_string().as_bytes());
    input.push(0);
    input.extend_from_slice(&accumulator);
    sha(&input)
}

fn main() {
    const UP_TOKEN: &str = "12345678901234567890";
    const DOWN_TOKEN: &str = "98765432109876543210";
    let condition = format!("0x{}", "a".repeat(64));
    let identity = wire::MarketIdentity {
        market_id: "5400001".into(),
        condition_id: condition.clone(),
        window_id: "btc-updown-5m-1791504000".into(),
        profile_id: "b".repeat(64),
        profile_revision: 7,
        profile_epoch: "c".repeat(32),
        stream_epoch: "d".repeat(32),
        role: "current".into(),
        start_at: "2026-10-09T00:00:00Z".into(),
        end_at: "2026-10-09T00:05:00Z".into(),
        rule_sha256: "f".repeat(64),
    };
    let outcomes = vec![
        wire::OutcomeBinding {
            outcome: "Up".into(),
            token_id: UP_TOKEN.into(),
        },
        wire::OutcomeBinding {
            outcome: "Down".into(),
            token_id: DOWN_TOKEN.into(),
        },
    ];
    let raw = format!(
        "{{\"type\":\"book_snapshot\",\"asset_id\":\"{UP_TOKEN}\",\"market\":\"{condition}\",\"bids\":[],\"asks\":[],\"integrity\":{{\"protocol\":\"pn1\",\"anchor\":true,\"seq\":1,\"checksum\":\"f9990dc511a3f5eabf06565ee7f52801621e28d7f2d3509d6fbda91e561edecf\"}}}}"
    );
    let checksum = (0..64)
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(
                &"f9990dc511a3f5eabf06565ee7f52801621e28d7f2d3509d6fbda91e561edecf"
                    [index..index + 2],
                16,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let book = wire::TokenBookState {
        token_id: UP_TOKEN.into(),
        provider_seq: 1,
        pn1_checksum: checksum,
        bids: Vec::new(),
        asks: Vec::new(),
        raw_frame_sha256: sha(raw.as_bytes()),
        raw_frame: raw.as_bytes().to_vec(),
        provider_batch_ts_ms: None,
        socket_received_at_unix_ns: 1_791_504_000_100_000_000,
        quote_source_at_ms: None,
    };
    let snapshot = wire::MarketSnapshot {
        identity: Some(identity.clone()),
        outcomes,
        source_epoch: "e".repeat(32),
        market_serial: 1,
        replay_floor_serial: Some(0),
        books: vec![book.clone()],
        ready: false,
        unavailable_reason: "awaiting_pn1_anchor".into(),
        snapshot_at_unix_ns: 1_791_504_000_101_000_000,
        execution_readiness: false,
    };
    let frame = wire::MarketStreamFrame {
        identity: Some(identity.clone()),
        source_epoch: "e".repeat(32),
        market_serial: 1,
        body: Some(wire::market_stream_frame::Body::DepthBatch(
            wire::MarketDepthBatch {
                updates: vec![wire::MarketDepthItem {
                    body: Some(wire::market_depth_item::Body::TokenAnchor(
                        wire::TokenBookState {
                            raw_frame: Vec::new(),
                            ..book.clone()
                        },
                    )),
                }],
                raw_frame_sha256: sha(raw.as_bytes()),
                raw_frame: raw.as_bytes().to_vec(),
                provider_batch_ts_ms: None,
                socket_received_at_unix_ns: 1_791_504_000_100_000_000,
                published_at_unix_ns: 1_791_504_000_100_500_000,
                quote_source_at_ms: None,
            },
        )),
    };
    let down_raw = raw.replace(UP_TOKEN, DOWN_TOKEN);
    let down_book = wire::TokenBookState {
        token_id: DOWN_TOKEN.into(),
        raw_frame_sha256: sha(down_raw.as_bytes()),
        raw_frame: down_raw.as_bytes().to_vec(),
        socket_received_at_unix_ns: 1_791_504_000_200_000_000,
        ..book.clone()
    };
    let mut down_frame = frame.clone();
    down_frame.market_serial = 2;
    if let Some(wire::market_stream_frame::Body::DepthBatch(batch)) = down_frame.body.as_mut() {
        batch.updates = vec![wire::MarketDepthItem {
            body: Some(wire::market_depth_item::Body::TokenAnchor(
                wire::TokenBookState {
                    raw_frame: Vec::new(),
                    ..down_book.clone()
                },
            )),
        }];
        batch.raw_frame_sha256 = sha(down_raw.as_bytes());
        batch.raw_frame = down_raw.as_bytes().to_vec();
        batch.socket_received_at_unix_ns = 1_791_504_000_200_000_000;
        batch.published_at_unix_ns = 1_791_504_000_200_500_000;
    }
    let mut ready = frame.clone();
    ready.market_serial = 3;
    ready.body = Some(wire::market_stream_frame::Body::Ready(wire::MarketReady {
        market_serial: 3,
        verified_token_ids: vec![UP_TOKEN.into(), DOWN_TOKEN.into()],
        execution_readiness: false,
    }));
    let mut invalidated = frame.clone();
    invalidated.market_serial = 4;
    invalidated.body = Some(wire::market_stream_frame::Body::Unavailable(
        wire::MarketInvalidated {
            token_ids: vec![UP_TOKEN.into()],
            reason: "integrity_failed".into(),
            requires_new_snapshot: true,
        },
    ));
    let mut recovered = frame.clone();
    recovered.market_serial = 5;
    let mut recovered_ready = ready.clone();
    recovered_ready.market_serial = 6;
    if let Some(wire::market_stream_frame::Body::Ready(body)) = recovered_ready.body.as_mut() {
        body.market_serial = 6;
    }
    let ready_snapshot = wire::MarketSnapshot {
        market_serial: 3,
        books: vec![book, down_book],
        ready: true,
        unavailable_reason: String::new(),
        ..snapshot.clone()
    };
    // Non-empty, single-sided-after-delta fixture. The snapshot deliberately
    // carries the latest delta as raw evidence; a full-sync is not a claim
    // that its latest raw frame was the original anchor.
    let up_anchor_checksum = pn1_checksum(&[("0.44", "12")], &[("0.46", "7")]);
    let down_anchor_checksum = pn1_checksum(&[("0.53", "9")], &[("0.55", "3")]);
    let up_delta_checksum = pn1_checksum(&[("0.44", "10")], &[]);
    let up_anchor_update = json!({"type":"book_snapshot","asset_id":UP_TOKEN,
        "market":condition,"bids":[{"price":"0.44","size":"12"}],
        "asks":[{"price":"0.46","size":"7"}],
        "integrity":{"protocol":"pn1","anchor":true,"seq":7,
            "checksum":hex(&up_anchor_checksum)}});
    let down_anchor_update = json!({"type":"book_snapshot","asset_id":DOWN_TOKEN,
        "market":condition,"bids":[{"price":"0.53","size":"9"}],
        "asks":[{"price":"0.55","size":"3"}],
        "integrity":{"protocol":"pn1","anchor":true,"seq":11,
            "checksum":hex(&down_anchor_checksum)}});
    let up_delta_update = json!({"type":"book_update","asset_id":UP_TOKEN,
        "market":condition,"bids":[{"price":"0.44","size":"10"}],
        "asks":[{"price":"0.46","size":"0"}],
        "integrity":{"protocol":"pn1","anchor":false,"prev_seq":7,"seq":8,
            "checksum":hex(&up_delta_checksum)}});
    let bad_seq_update = json!({"type":"book_update","asset_id":UP_TOKEN,
        "market":condition,"bids":[{"price":"0.44","size":"8"}],"asks":[],
        "integrity":{"protocol":"pn1","anchor":false,"prev_seq":8,"seq":10,
            "checksum":hex(&up_delta_checksum)}});
    let up_anchor_raw = json!({"type":"batch","ts":1_791_504_000_300_u64,
        "count":1,"updates":[up_anchor_update]}).to_string();
    let down_anchor_raw = json!({"type":"batch","ts":1_791_504_000_400_u64,
        "count":1,"updates":[down_anchor_update]}).to_string();
    let up_delta_raw = json!({"type":"batch","ts":1_791_504_000_500_u64,
        "count":1,"updates":[up_delta_update]}).to_string();
    let bad_seq_raw = json!({"type":"batch","ts":1_791_504_000_600_u64,
        "count":1,"updates":[bad_seq_update]}).to_string();
    let up_anchor = wire::TokenBookState {
        token_id: UP_TOKEN.into(),
        provider_seq: 7,
        pn1_checksum: up_anchor_checksum,
        bids: vec![wire::DepthLevel { price: "0.44".into(), size: "12".into() }],
        asks: vec![wire::DepthLevel { price: "0.46".into(), size: "7".into() }],
        raw_frame_sha256: sha(up_anchor_raw.as_bytes()),
        raw_frame: up_anchor_raw.as_bytes().to_vec(),
        provider_batch_ts_ms: Some(1_791_504_000_300),
        socket_received_at_unix_ns: 1_791_504_000_300_000_000,
        quote_source_at_ms: None,
    };
    let down_anchor = wire::TokenBookState {
        token_id: DOWN_TOKEN.into(),
        provider_seq: 11,
        pn1_checksum: down_anchor_checksum,
        bids: vec![wire::DepthLevel { price: "0.53".into(), size: "9".into() }],
        asks: vec![wire::DepthLevel { price: "0.55".into(), size: "3".into() }],
        raw_frame_sha256: sha(down_anchor_raw.as_bytes()),
        raw_frame: down_anchor_raw.as_bytes().to_vec(),
        provider_batch_ts_ms: Some(1_791_504_000_400),
        socket_received_at_unix_ns: 1_791_504_000_400_000_000,
        quote_source_at_ms: None,
    };
    let up_post_delta = wire::TokenBookState {
        provider_seq: 8,
        pn1_checksum: up_delta_checksum.clone(),
        bids: vec![wire::DepthLevel { price: "0.44".into(), size: "10".into() }],
        asks: Vec::new(),
        raw_frame_sha256: sha(up_delta_raw.as_bytes()),
        raw_frame: up_delta_raw.as_bytes().to_vec(),
        provider_batch_ts_ms: Some(1_791_504_000_500),
        socket_received_at_unix_ns: 1_791_504_000_500_000_000,
        ..up_anchor.clone()
    };
    let source_epoch = "1".repeat(32);
    let depth_frame = |serial: u64, update: wire::MarketDepthItem, raw: &str, at_ns: u64| {
        wire::MarketStreamFrame {
            identity: Some(identity.clone()),
            source_epoch: source_epoch.clone(),
            market_serial: serial,
            body: Some(wire::market_stream_frame::Body::DepthBatch(wire::MarketDepthBatch {
                updates: vec![update],
                raw_frame_sha256: sha(raw.as_bytes()),
                raw_frame: raw.as_bytes().to_vec(),
                provider_batch_ts_ms: Some(at_ns / 1_000_000),
                socket_received_at_unix_ns: at_ns,
                published_at_unix_ns: at_ns + 500_000,
                quote_source_at_ms: None,
            })),
        }
    };
    let anchor_item = |book: &wire::TokenBookState| wire::MarketDepthItem {
        body: Some(wire::market_depth_item::Body::TokenAnchor(wire::TokenBookState {
            raw_frame: Vec::new(),
            ..book.clone()
        })),
    };
    let nonempty_frames = vec![
        depth_frame(1, anchor_item(&up_anchor), &up_anchor_raw, 1_791_504_000_300_000_000),
        depth_frame(2, anchor_item(&down_anchor), &down_anchor_raw, 1_791_504_000_400_000_000),
        wire::MarketStreamFrame {
            identity: Some(identity.clone()), source_epoch: source_epoch.clone(), market_serial: 3,
            body: Some(wire::market_stream_frame::Body::Ready(wire::MarketReady {
                market_serial: 3,
                verified_token_ids: vec![UP_TOKEN.into(), DOWN_TOKEN.into()],
                execution_readiness: false,
            })),
        },
        depth_frame(4, wire::MarketDepthItem {
            body: Some(wire::market_depth_item::Body::Delta(wire::MarketDepthDelta {
                token_id: UP_TOKEN.into(), prev_provider_seq: 7, provider_seq: 8,
                pn1_checksum: up_delta_checksum,
                changes: vec![
                    wire::DepthChange { side: "B".into(), price: "0.44".into(), size: "10".into() },
                    wire::DepthChange { side: "A".into(), price: "0.46".into(), size: "0".into() },
                ],
            })),
        }, &up_delta_raw, 1_791_504_000_500_000_000),
        wire::MarketStreamFrame {
            identity: Some(identity.clone()), source_epoch: source_epoch.clone(), market_serial: 5,
            body: Some(wire::market_stream_frame::Body::Unavailable(wire::MarketInvalidated {
                token_ids: vec![UP_TOKEN.into()], reason: "integrity_failed".into(),
                requires_new_snapshot: true,
            })),
        },
    ];
    let nonempty_snapshot = wire::MarketSnapshot {
        identity: Some(identity.clone()),
        outcomes: ready_snapshot.outcomes.clone(),
        source_epoch: source_epoch.clone(),
        market_serial: 4,
        replay_floor_serial: Some(0),
        books: vec![up_post_delta, down_anchor.clone()],
        ready: true,
        unavailable_reason: String::new(),
        snapshot_at_unix_ns: 1_791_504_000_501_000_000,
        execution_readiness: false,
    };
    let reconnected_snapshot = wire::MarketSnapshot {
        source_epoch: "2".repeat(32), market_serial: 2,
        books: vec![up_anchor, down_anchor],
        snapshot_at_unix_ns: 1_791_504_001_000_000_000,
        ..nonempty_snapshot.clone()
    };
    let nonempty_snapshot_bytes = contract::encode_document(&nonempty_snapshot, 8 * 1024 * 1024).unwrap();
    let reconnected_snapshot_bytes = contract::encode_document(&reconnected_snapshot, 8 * 1024 * 1024).unwrap();
    let nonempty_frames = nonempty_frames.into_iter().map(|frame| {
        let bytes = contract::encode_frame(&frame, 4 * 1024 * 1024).unwrap();
        json!({"market_serial":frame.market_serial,"hex":hex(&bytes),"sha256":hex(&sha(&bytes))})
    }).collect::<Vec<_>>();
    let snapshot_bytes = contract::encode_document(&snapshot, 8 * 1024 * 1024).unwrap();
    let ready_snapshot_bytes = contract::encode_document(&ready_snapshot, 8 * 1024 * 1024).unwrap();
    let frames = [
        frame,
        down_frame,
        ready,
        invalidated,
        recovered,
        recovered_ready,
    ];
    let frames = frames
        .into_iter()
        .map(|frame| {
            let bytes = contract::encode_frame(&frame, 4 * 1024 * 1024).unwrap();
            json!({"market_serial":frame.market_serial,"hex":hex(&bytes),
            "sha256":hex(&sha(&bytes))})
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        json!({
            "schema_version":contract::SCHEMA_VERSION,
            "snapshot_hex":hex(&snapshot_bytes),
            "snapshot_sha256":hex(&sha(&snapshot_bytes)),
            "ready_snapshot_hex":hex(&ready_snapshot_bytes),
            "ready_snapshot_sha256":hex(&sha(&ready_snapshot_bytes)),
            "frames":frames,
            "nonempty_case":{
                "ready_snapshot_hex":hex(&nonempty_snapshot_bytes),
                "ready_snapshot_sha256":hex(&sha(&nonempty_snapshot_bytes)),
                "reconnected_snapshot_hex":hex(&reconnected_snapshot_bytes),
                "reconnected_snapshot_sha256":hex(&sha(&reconnected_snapshot_bytes)),
                "frames":nonempty_frames,
                "invalid_source_seq_frame":{"text":bad_seq_raw,
                    "sha256":hex(&sha(bad_seq_raw.as_bytes())),
                    "expected_market_invalidation_reason":"integrity_failed"},
                "midstream_replay_expired_close":{"code":1012,"reason":"replay_expired"},
            },
            "raw_frames":[{"text":raw,"sha256":hex(&sha(raw.as_bytes()))},
                {"text":down_raw,"sha256":hex(&sha(down_raw.as_bytes()))}],
            "official_relay_non_200":{"status":504,"body":
                contract::official_relay_error_json(
                    "upstream_timeout", true, "11111111111111111111111111111111")},
            "market_stream_expired":{"status":410,"body":
                contract::market_error_json("replay_expired", true)},
        })
    );
}
