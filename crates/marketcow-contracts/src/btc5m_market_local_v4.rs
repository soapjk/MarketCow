//! Market-local PN1 observation contract. The .proto is authoritative.
//! A protobuf frame is transport, never a PN1 checksum or official book hash.

use prost::Message;

pub const SCHEMA_VERSION: &str = "marketcow.polymarket.btc5m.market-local.v4";
pub const CONTENT_TYPE: &str = "application/x-protobuf";
pub const WS_SUBPROTOCOL: &str = "marketcow.btc5m.market-local.v4";
pub const ROUTE_PREFIX: &str = "/v1/prediction-markets/polymarket/research/btc-5m";
pub const ERROR_SCHEMA_VERSION: &str = "marketcow.polymarket.btc5m.market-local-error.v4";
pub const OFFICIAL_RELAY_SCHEMA_VERSION: &str = "marketcow.polymarket.official-book-relay.v2";
pub const OFFICIAL_RELAY_ERROR_SCHEMA_VERSION: &str =
    "marketcow.polymarket.official-book-relay-error.v2";

pub fn market_error_json(code: &str, retryable: bool) -> serde_json::Value {
    serde_json::json!({"schema_version":ERROR_SCHEMA_VERSION,
        "code":code,"retryable":retryable})
}

pub fn official_relay_error_json(
    code: &str,
    retryable: bool,
    request_id: &str,
) -> serde_json::Value {
    serde_json::json!({"schema_version":OFFICIAL_RELAY_ERROR_SCHEMA_VERSION,
        "code":code,"retryable":retryable,"request_id":request_id})
}

pub mod wire {
    include!(concat!(
        env!("OUT_DIR"),
        "/marketcow.polymarket.btc5m.v4.rs"
    ));
}

pub fn encode_document<T: Message>(document: &T, maximum_bytes: usize) -> std::io::Result<Vec<u8>> {
    let bytes = document.encode_to_vec();
    if bytes.len() > maximum_bytes {
        return Err(std::io::Error::other("PN1 document byte budget exceeded"));
    }
    Ok(bytes)
}

pub fn encode_frame(
    frame: &wire::MarketStreamFrame,
    maximum_bytes: usize,
) -> std::io::Result<Vec<u8>> {
    if frame.identity.is_none() || frame.body.is_none() || frame.source_epoch.is_empty() {
        return Err(std::io::Error::other(
            "PN1 frame identity, epoch and body required",
        ));
    }
    encode_document(frame, maximum_bytes)
}
