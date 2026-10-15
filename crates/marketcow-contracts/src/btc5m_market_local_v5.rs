//! Official market-local live delivery with atomic connection/checkpoint boundaries.
use prost::Message;
pub const SCHEMA_VERSION: &str = "marketcow.polymarket.btc5m.market-local.v5";
pub const CONTENT_TYPE: &str = "application/x-protobuf";
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/marketcow.polymarket.btc5m.v5.rs"));
}
pub fn encode_frame(frame: &wire::MarketStreamFrame, maximum_bytes: usize) -> std::io::Result<Vec<u8>> {
    if frame.identity.is_none() || frame.body.is_none() {
        return Err(std::io::Error::other("market frame identity/body required"));
    }
    encode_document(frame, maximum_bytes)
}
pub fn encode_document<T: Message>(document: &T, maximum_bytes: usize) -> std::io::Result<Vec<u8>> {
    let bytes = document.encode_to_vec();
    if bytes.len() > maximum_bytes {
        return Err(std::io::Error::other("market document byte budget exceeded"));
    }
    Ok(bytes)
}
