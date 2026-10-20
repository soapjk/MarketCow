//! No-hash typed data contracts. Encoding occurs only at transport boundaries.
use prost::Message;
/// Build-time descriptor shared by bindings; no runtime reflection is required.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/schema.bin"));
pub const CONTENT_TYPE: &str = "application/x-protobuf";
pub const PRICE_SCHEMA: &str = "marketcow.btc-price-stream.v3";
pub const PRICE_SUBPROTOCOL: &str = "marketcow.btc-price.v3";
pub const MARKET_SCHEMA: &str = "marketcow.polymarket.btc5m.market-local.v7";
pub const MARKET_SUBPROTOCOL: &str = "marketcow.btc5m.market-local.v7";
pub const MARKET_STREAMS_SCHEMA: &str = "marketcow.polymarket.btc5m.market-streams.v7";
pub const REFERENCE_SCHEMA: &str = "marketcow.facts.reference.v1";
pub const EXECUTION_SCHEMA: &str = "marketcow.facts.execution.v1";
pub const FINALITY_SCHEMA: &str = "marketcow.facts.finality.v1";
pub const BOOK_REST_SCHEMA: &str = "marketcow.facts.official-book.v1";
pub const BTC_REST_SCHEMA: &str = "marketcow.facts.btc-rest.v1";
mod packages {
    pub mod facts {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/marketcow.facts.v1.rs"));
        }
    }
    pub mod common {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/marketcow.common.v1.rs"));
        }
    }
    pub mod btc {
        pub mod v3 {
            include!(concat!(env!("OUT_DIR"), "/marketcow.btc.v3.rs"));
        }
        pub mod history {
            pub mod v3 {
                include!(concat!(env!("OUT_DIR"), "/marketcow.btc.history.v3.rs"));
            }
        }
    }
    pub mod polymarket {
        pub mod btc5m {
            pub mod v7 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/marketcow.polymarket.btc5m.v7.rs"
                ));
            }
        }
    }
}
pub use packages::{
    btc::history::v3 as history, btc::v3 as btc, common::v1 as common, facts::v1 as facts,
    polymarket::btc5m::v7 as market,
};

pub fn encode<T: Message>(message: &T, maximum_bytes: usize) -> std::io::Result<Vec<u8>> {
    if message.encoded_len() > maximum_bytes {
        return Err(std::io::Error::other("typed message byte budget"));
    }
    Ok(message.encode_to_vec())
}
