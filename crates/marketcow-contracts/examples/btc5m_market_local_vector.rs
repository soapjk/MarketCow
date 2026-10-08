//! Deterministic serializer vector generator; not a live feed or test runner.
use marketcow_contracts::btc5m_market_local_v2::{encode_frame, wire};
use serde::{Serialize, Serializer, ser::{SerializeMap, SerializeSeq}};
use serde_json::Value;
use sha2::{Digest, Sha256};
use serde_json::json;

fn sha256(bytes: &[u8]) -> Vec<u8> {
    Sha256::digest(bytes).to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn canonical_json(value: &Value) -> Vec<u8> {
    struct Sorted<'a>(&'a Value);
    impl Serialize for Sorted<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            match self.0 {
                Value::Object(values) => {
                    let mut keys = values.keys().collect::<Vec<_>>();
                    keys.sort_unstable();
                    let mut map = serializer.serialize_map(Some(keys.len()))?;
                    for key in keys {
                        map.serialize_entry(key, &Sorted(&values[key]))?;
                    }
                    map.end()
                }
                Value::Array(values) => {
                    let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                    for value in values {
                        sequence.serialize_element(&Sorted(value))?;
                    }
                    sequence.end()
                }
                value => value.serialize(serializer),
            }
        }
    }
    serde_json::to_vec(&Sorted(value)).expect("canonical JSON")
}

// Keep this vector in the same encoder as the ordinary market-local frames.
// The JSON commitments are canonical semantic preimages; protobuf bytes are
// transport and must never be used as the cross-language book/event hash.
fn depth_conflation_vector(identity: &wire::MarketIdentity) {
    let condition_id = identity.condition_id.as_str();
    let first_at = "2026-10-04T00:00:00.000100Z";
    let last_at = "2026-10-04T00:00:00.000900Z";
    let first_raw = json!({
        "event_type":"price_change","market":condition_id,"timestamp":first_at,
        "price_changes":[{"asset_id":"10001","side":"BUY","price":"0.30",
            "size":"5","best_bid":"0.40","best_ask":"0.60"}]
    });
    let last_raw = json!({
        "event_type":"price_change","market":condition_id,"timestamp":last_at,
        "price_changes":[
            {"asset_id":"10001","side":"BUY","price":"0.30",
                "size":"6","best_bid":"0.40","best_ask":"0.60"},
            {"asset_id":"10001","side":"BUY","price":"0.20",
                "size":"1","best_bid":"0.40","best_ask":"0.60"}
        ]
    });
    // BTreeMap's (side, price) order is the producer's deterministic order.
    let final_changes = json!([
        {"asset_id":"10001","side":"BUY","price":"0.20",
            "size":"1","best_bid":"0.40","best_ask":"0.60"},
        {"asset_id":"10001","side":"BUY","price":"0.30",
            "size":"6","best_bid":"0.40","best_ask":"0.60"}
    ]);
    let raw_payload = json!({
        "event_type":"price_change","market":condition_id,"timestamp":last_at,
        "price_changes":final_changes,
        "marketcow_conflation":{
            "schema_version":"marketcow.polymarket.depth-conflation.v2",
            "market_id":identity.market_id,"token_ids":["10001"],
            "first_received_at":first_at,"last_received_at":last_at,
            "source_frame_count":2,
            "source_frames":[
                {"received_at":first_at,"raw_payload":first_raw},
                {"received_at":last_at,"raw_payload":last_raw}
            ]
        }
    });
    let source_hash = hex(&sha256(&canonical_json(&last_raw)));
    let tick_version_bytes = sha256(&canonical_json(&json!({"tick_size":"0.01"})));
    let tick_version = hex(&tick_version_bytes);
    let checksum_bytes = sha256(&canonical_json(&json!({
        "token_id":"10001","tick_size":"0.01",
        "bids":[{"price":"0.40","size":"8"},
            {"price":"0.30","size":"6"},{"price":"0.20","size":"1"}],
        "asks":[{"price":"0.60","size":"7"}]
    })));
    let checksum = hex(&checksum_bytes);
    let book = json!({
        "token_id":"10001","condition_id":condition_id,"book_epoch":"e".repeat(64),
        "sequence":43,"sequence_semantics":"deterministic_normalized",
        "exchange_at":last_at,"received_at":last_at,
        "tick_version":tick_version,"tick_size":"0.01",
        "bids":[{"price":"0.40","size":"8"},
            {"price":"0.30","size":"6"},{"price":"0.20","size":"1"}],
        "asks":[{"price":"0.60","size":"7"}],
        "last_trade_price":null,"state_checksum":checksum,"source_hash":source_hash
    });
    let book_bytes = canonical_json(&book);
    let book_hash_bytes = sha256(&book_bytes);
    let book_hash = hex(&book_hash_bytes);
    let compact_payload = json!({
        "payload_semantics":"ordered_depth_delta_with_complete_post_state_commitment",
        "token_id":"10001","condition_id":condition_id,"book_epoch":"e".repeat(64),
        "sequence":43,"sequence_semantics":"deterministic_normalized",
        "exchange_at":last_at,"received_at":last_at,
        "tick_version":tick_version,"tick_size":"0.01",
        "last_trade_price":null,"state_checksum":checksum,"source_hash":source_hash,
        "canonical_book_sha256":book_hash,"materialized_book_bytes":book_bytes.len(),
        "delta":{"changes":[
            {"asset_id":"10001","side":"BUY","price":"0.20","size":"1"},
            {"asset_id":"10001","side":"BUY","price":"0.30","size":"6"}
        ]}
    });
    let raw_hash = sha256(&canonical_json(&raw_payload));
    let compact_hash = sha256(&canonical_json(&compact_payload));
    let source_event = json!({
        "contract_version":"marketcow.prediction_market.v1",
        "schema_version":"marketcow.polymarket.live.v2",
        "event_type":"book","cursor":2,"market_id":identity.market_id,
        "condition_id":condition_id,"token_id":"10001",
        "book_epoch":"e".repeat(64),"sequence":43,
        "exchange_at":last_at,"received_at":last_at,
        "canonical_payload_sha256":hex(&compact_hash),"canonical_payload":compact_payload,
        "raw_payload_sha256":hex(&raw_hash),"raw_payload":raw_payload,
        "applied":true,"fail_closed_reason":null,"gaps":[]
    });
    let source_event_preimage_json = canonical_json(&source_event);
    let event_id = sha256(&source_event_preimage_json);
    let group_preimage = json!({
        "schema_version":"marketcow.polymarket.market-group.v2",
        "market_id":identity.market_id,
        "stream_epoch":identity.stream_epoch.as_deref().expect("published vector"),
        "market_cursor":2,"event_ids":[hex(&event_id)]
    });
    let group_preimage_json = canonical_json(&group_preimage);
    let group_sha256 = sha256(&group_preimage_json);
    let event = wire::MarketEvent {
        market_event_cursor: 2, event_id, raw_payload_sha256: raw_hash.clone(),
        canonical_payload_sha256: compact_hash.clone(),
        market_id: identity.market_id.clone(), condition_id: identity.condition_id.clone(),
        token_id: Some("10001".into()), book_epoch: Some("e".repeat(64)),
        book_sequence: Some(43), exchange_at: Some(last_at.into()), received_at: last_at.into(),
        applied: Some(true), fail_closed_reason: None, physical_archive_cursor: None,
        source_event_preimage_json: source_event_preimage_json.clone(),
        payload: Some(wire::market_event::Payload::BookDelta(wire::BookDelta {
            changes: vec![
                wire::DepthChange { token_id:"10001".into(), side:"BUY".into(),
                    price:"0.20".into(), size:"1".into() },
                wire::DepthChange { token_id:"10001".into(), side:"BUY".into(),
                    price:"0.30".into(), size:"6".into() },
            ],
            post_state: Some(wire::BookPostState {
                token_id:"10001".into(), condition_id:identity.condition_id.clone(),
                book_epoch:"e".repeat(64), sequence:43,
                sequence_semantics:"deterministic_normalized".into(),
                exchange_at:Some(last_at.into()), received_at:last_at.into(),
                tick_version:tick_version_bytes,
                tick_size:"0.01".into(), last_trade_price:None,
                state_checksum:checksum_bytes.clone(),
                source_hash:Some(source_hash.clone()),
                canonical_book_sha256:book_hash_bytes.clone(),
                materialized_book_bytes:book_bytes.len() as u64,
            }),
        })),
    };
    let frames = [
        wire::MarketStreamFrame { identity:Some(identity.clone()),
            body:Some(wire::market_stream_frame::Body::GroupBegin(wire::MarketGroupBegin {
                market_cursor:2, group_sha256:group_sha256.clone(), event_count:1, chunk_count:1,
            })) },
        wire::MarketStreamFrame { identity:Some(identity.clone()),
            body:Some(wire::market_stream_frame::Body::GroupChunk(wire::MarketGroupChunk {
                market_cursor:2, group_sha256:group_sha256.clone(),
                chunk_index:0, chunk_count:1, events:vec![event],
            })) },
        wire::MarketStreamFrame { identity:Some(identity.clone()),
            body:Some(wire::market_stream_frame::Body::GroupCommit(wire::MarketGroupCommit {
                market_cursor:2, group_sha256, event_count:1, chunk_count:1,
                next_after_market_cursor:2,
            })) },
    ];
    println!("depth_conflation_source_event_preimage_json={}",
        String::from_utf8_lossy(&source_event_preimage_json));
    println!("depth_conflation_raw_payload_sha256={}", hex(&raw_hash));
    println!("depth_conflation_canonical_payload_sha256={}", hex(&compact_hash));
    println!("depth_conflation_canonical_book_sha256={}", hex(&book_hash_bytes));
    println!("depth_conflation_state_checksum={}", hex(&checksum_bytes));
    println!("depth_conflation_last_supplier_sha256={}", source_hash);
    println!("depth_conflation_event_id={}", hex(&sha256(&source_event_preimage_json)));
    println!("depth_conflation_group_preimage={}",
        String::from_utf8_lossy(&group_preimage_json));
    for (index, frame) in frames.iter().enumerate() {
        let encoded = encode_frame(frame, 1_048_576).expect("conflation vector fits");
        println!("depth_conflation_frame_{index}_hex={}", hex(&encoded));
        println!("depth_conflation_frame_{index}_sha256={}", hex(&sha256(&encoded)));
    }
}

fn main() {
    let identity = wire::MarketIdentity {
        market_id: "5200001".into(),
        condition_id: format!("0x{}", "a".repeat(64)),
        profile_id: "b".repeat(64),
        profile_revision: 7,
        profile_epoch: "c".repeat(32),
        stream_epoch: Some("d".repeat(32)),
    };
    let tick_size = "0.01";
    let bids = vec![wire::PriceLevel { price: "0.40".into(), size: "8".into() }];
    let asks = vec![wire::PriceLevel { price: "0.60".into(), size: "7".into() }];
    let checksum = sha256(&canonical_json(&json!({
        "token_id":"10001","tick_size":tick_size,
        "bids":[{"price":"0.40","size":"8"}],
        "asks":[{"price":"0.60","size":"7"}]
    })));
    let tick_version = sha256(&canonical_json(&json!({"tick_size":tick_size})));
    let book = wire::CanonicalBook {
        token_id: "10001".into(), condition_id: identity.condition_id.clone(),
        book_epoch: "e".repeat(64), sequence: 42,
        sequence_semantics: "deterministic_normalized".into(),
        exchange_at: Some("2026-10-04T00:00:00.000000Z".into()),
        received_at: "2026-10-04T00:00:00.000000Z".into(),
        tick_version, tick_size: tick_size.into(), bids, asks,
        last_trade_price: None, state_checksum: checksum.clone(),
        source_hash: Some("f".repeat(40)),
    };
    let canonical_payload = json!({
        "token_id":"10001","condition_id":identity.condition_id.clone(),
        "book_epoch":"e".repeat(64),"sequence":42,
        "sequence_semantics":"deterministic_normalized",
        "exchange_at":"2026-10-04T00:00:00.000000Z",
        "received_at":"2026-10-04T00:00:00.000000Z",
        "tick_version":hex(&book.tick_version),"tick_size":"0.01",
        "bids":[{"price":"0.40","size":"8"}],
        "asks":[{"price":"0.60","size":"7"}],
        "last_trade_price":null,"state_checksum":hex(&checksum),
        "source_hash":"f".repeat(40),
    });
    let canonical_book_sha256 = sha256(&canonical_json(&canonical_payload));
    let raw_payload = json!({
        "event_type":"book","asset_id":"10001","market":identity.condition_id,
        "bids":[{"price":"0.40","size":"8"}],
        "asks":[{"price":"0.60","size":"7"}],
        "timestamp":"2026-10-04T00:00:00.000000Z"
    });
    let raw_payload_sha256 = sha256(&canonical_json(&raw_payload));
    let source_event = json!({
        "contract_version":"marketcow.prediction_market.v1",
        "schema_version":"marketcow.polymarket.live.v2",
        "event_type":"book","cursor":1,"market_id":identity.market_id,
        "condition_id":identity.condition_id,"token_id":"10001",
        "book_epoch":"e".repeat(64),"sequence":42,
        "exchange_at":"2026-10-04T00:00:00.000000Z",
        "received_at":"2026-10-04T00:00:00.000000Z",
        "canonical_payload_sha256":hex(&canonical_book_sha256),
        "canonical_payload":canonical_payload,
        "raw_payload_sha256":hex(&raw_payload_sha256),"raw_payload":raw_payload,
        "applied":true,"fail_closed_reason":null,"gaps":[]
    });
    let source_event_preimage_json = canonical_json(&source_event);
    let event_id = sha256(&source_event_preimage_json);
    println!("source_event_preimage_json={}",
        String::from_utf8_lossy(&source_event_preimage_json));
    println!("source_event_id={}", hex(&event_id));
    let preimage = format!(
        "{{\"event_ids\":[\"{}\"],\"market_cursor\":1,\"market_id\":\"5200001\",\"schema_version\":\"marketcow.polymarket.market-group.v2\",\"stream_epoch\":\"{}\"}}",
        hex(&event_id), identity.stream_epoch.as_deref().expect("published vector")
    );
    let group_sha256 = sha256(preimage.as_bytes());
    let event = wire::MarketEvent {
        market_event_cursor: 1,
        event_id,
        raw_payload_sha256,
        canonical_payload_sha256: canonical_book_sha256,
        market_id: identity.market_id.clone(),
        condition_id: identity.condition_id.clone(),
        token_id: Some("10001".into()),
        book_epoch: Some("e".repeat(64)),
        book_sequence: Some(42),
        exchange_at: Some("2026-10-04T00:00:00.000000Z".into()),
        received_at: "2026-10-04T00:00:00.000000Z".into(),
        applied: Some(true),
        fail_closed_reason: None,
        physical_archive_cursor: None,
        source_event_preimage_json,
        payload: Some(wire::market_event::Payload::BookSnapshot(book)),
    };
    let frames = [
        wire::MarketStreamFrame {
            identity: Some(identity.clone()),
            body: Some(wire::market_stream_frame::Body::GroupBegin(wire::MarketGroupBegin {
                market_cursor: 1, group_sha256: group_sha256.clone(),
                event_count: 1, chunk_count: 1,
            })),
        },
        wire::MarketStreamFrame {
            identity: Some(identity.clone()),
            body: Some(wire::market_stream_frame::Body::GroupChunk(wire::MarketGroupChunk {
                market_cursor: 1, group_sha256: group_sha256.clone(),
                chunk_index: 0, chunk_count: 1, events: vec![event],
            })),
        },
        wire::MarketStreamFrame {
            identity: Some(identity.clone()),
            body: Some(wire::market_stream_frame::Body::GroupCommit(wire::MarketGroupCommit {
                market_cursor: 1, group_sha256, event_count: 1,
                chunk_count: 1, next_after_market_cursor: 1,
            })),
        },
        wire::MarketStreamFrame {
            identity: Some(identity.clone()),
            body: Some(wire::market_stream_frame::Body::Ready(wire::MarketReady {
                market_cursor: Some(1), last_market_event_cursor: Some(1),
                confirmation_sequence: Some(2),
                confirmations: vec![wire::BookConfirmation {
                    token_id: "10001".into(), condition_id: identity.condition_id.clone(),
                    confirmation_sequence: 2, book_market_cursor: 1,
                    book_epoch: "e".repeat(64), book_sequence: 42,
                    state_checksum: checksum.clone(),
                    book_received_at: "2026-10-04T00:00:00.000000Z".into(),
                    confirmed_at: "2026-10-04T00:00:00.100000Z".into(),
                    confirmation_source: "polymarket_websocket".into(),
                    confirmation_evidence_sha256: sha256(b"synthetic-confirmation-v1"),
                }],
                execution_readiness: Some(false),
            })),
        },
        wire::MarketStreamFrame {
            identity: Some(identity.clone()),
            body: Some(wire::market_stream_frame::Body::BookConfirmations(wire::BookConfirmations {
                sent_market_cursor: Some(1), confirmation_sequence: 3,
                confirmations: vec![wire::BookConfirmation {
                    token_id: "10001".into(), condition_id: identity.condition_id.clone(),
                    confirmation_sequence: 3, book_market_cursor: 1,
                    book_epoch: "e".repeat(64), book_sequence: 42,
                    state_checksum: checksum,
                    book_received_at: "2026-10-04T00:00:00.000000Z".into(),
                    confirmed_at: "2026-10-04T00:00:00.200000Z".into(),
                    confirmation_source: "polymarket_websocket".into(),
                    confirmation_evidence_sha256: sha256(b"synthetic-confirmation-v2"),
                }],
            })),
        },
    ];
    println!("group_hash_preimage={preimage}");
    for (index, frame) in frames.iter().enumerate() {
        let encoded = encode_frame(frame, 1_048_576).expect("fixed vector fits");
        println!("frame_{index}_hex={}", hex(&encoded));
        println!("frame_{index}_sha256={}", hex(&sha256(&encoded)));
    }
    depth_conflation_vector(&identity);
}
