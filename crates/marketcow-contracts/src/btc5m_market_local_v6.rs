//! Ordinary snapshot/absolute-depth live delivery. No internal switch protocol.
use prost::Message;
pub const SCHEMA_VERSION: &str = "marketcow.polymarket.btc5m.market-local.v6";
pub const SUBPROTOCOL: &str = "marketcow.btc5m.market-local.v6";
pub const CONTENT_TYPE: &str = "application/x-protobuf";
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/marketcow.polymarket.btc5m.v6.rs"));
}
pub fn encode<T: Message>(document: &T, maximum_bytes: usize) -> std::io::Result<Vec<u8>> {
    if document.encoded_len() > maximum_bytes {
        return Err(std::io::Error::other("ordinary market message byte budget"));
    }
    Ok(document.encode_to_vec())
}
