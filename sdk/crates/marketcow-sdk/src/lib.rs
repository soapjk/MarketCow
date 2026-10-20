//! Native client core shared by Rust applications and the Python extension.
//! No data hashing, JSON intermediates, archive scanning, or trading operations.
pub mod bars;
pub mod book;
pub mod client;
pub mod error;
mod fanout;
pub mod follow_market;
pub mod history;
mod leases;
pub mod market_stream;
pub mod observation_history;
pub mod persistence;
pub mod price;
pub mod public_data;
mod retry;
pub mod shared_market;
pub mod shared_price;
pub mod time_history;
pub mod usage;

pub use client::{Client, ClientBuilder, Limits};
pub use error::{Error, Result};
pub use follow_market::{CurrentMarket, MarketDelivery};
pub use market_stream::MarketSubscription;
pub use marketcow_wire::{btc, common, facts, history as wire_history, market};
pub use price::{PriceState, PriceSubscription};
pub use shared_market::SharedMarket;
pub use shared_price::{PriceDelivery, SharedPrices};
