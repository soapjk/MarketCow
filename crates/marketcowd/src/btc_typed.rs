//! Typed BTC owner shared by publication and asynchronous persistence.
//! Supplier JSON parsing belongs to the ingress adapter, never this owner.
use anyhow::{Context, Result, ensure};
use marketcow_contracts::typed_v3::btc as wire;
use rust_decimal::Decimal;
use std::sync::Arc;

/// One boundary encoding shared by every subscriber; the source owner remains
/// the existing btc_trade/btc_chainlink service, never a second acquisition.
pub struct Published {
    pub frame: wire::PriceFrame,
    pub bytes: axum::body::Bytes,
}
impl Published {
    pub fn new(frame: wire::PriceFrame) -> Result<Arc<Self>> {
        let bytes = marketcow_contracts::typed_v3::encode(&frame, 4 * 1024 * 1024)?;
        Ok(Arc::new(Self {
            frame,
            bytes: bytes.into(),
        }))
    }
}

pub fn positive_decimal(text: &str) -> Result<Decimal> {
    ensure!(
        !text.is_empty() && text.len() <= 96 && !text.contains(['e', 'E', '+']),
        "decimal syntax"
    );
    let value: Decimal = text.parse().context("decimal parse")?;
    ensure!(value > Decimal::ZERO, "decimal range");
    Ok(value)
}
pub fn validate_trade(row: &wire::Trade) -> Result<()> {
    ensure!(
        row.trade_id > 0
            && row.trade_time_ms > 0
            && row.event_time_ms.is_none_or(|t| t > 0)
            && row.buyer_is_maker.is_some(),
        "trade identity/time"
    );
    positive_decimal(&row.price)?;
    positive_decimal(&row.quantity)?;
    Ok(())
}
