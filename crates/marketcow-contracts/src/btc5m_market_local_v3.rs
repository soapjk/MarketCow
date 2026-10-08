//! Single market-local Protobuf contract shared with Tradude.
//! The `.proto` file is the authoritative field and presence specification.

use prost::Message;

pub const SCHEMA_VERSION: &str = "marketcow.polymarket.btc5m.market-local.v3";
pub const CONTENT_TYPE: &str = "application/x-protobuf";

pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/marketcow.polymarket.btc5m.v3.rs"));
}

/// Encode exactly one frame. Message encoding is transport only, never the
/// cross-language event/group hash preimage.
pub fn encode_frame(frame: &wire::MarketStreamFrame, maximum_bytes: usize) -> std::io::Result<Vec<u8>> {
    if frame.identity.is_none() || frame.body.is_none() {
        return Err(std::io::Error::other("market frame identity/body required"));
    }
    let bytes = frame.encode_to_vec();
    if bytes.len() > maximum_bytes {
        return Err(std::io::Error::other("market frame byte budget exceeded"));
    }
    Ok(bytes)
}

pub fn encode_document<T: Message>(document: &T, maximum_bytes: usize) -> std::io::Result<Vec<u8>> {
    let bytes = document.encode_to_vec();
    if bytes.len() > maximum_bytes {
        return Err(std::io::Error::other("market document byte budget exceeded"));
    }
    Ok(bytes)
}
