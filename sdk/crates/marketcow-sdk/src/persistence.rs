//! Explicit cold-path Proto persistence. Never invoked by market-data getters,
//! subscriptions, validation or IDs. Decoding is syntax, not proof of supplier
//! provenance, coverage, freshness, binding or execution eligibility.
use crate::{Error, Result};
use prost::Message;

pub const MAX_FACT_BYTES: usize = 8 * 1024 * 1024;
pub fn export<T: Message>(value: &T) -> Result<Vec<u8>> {
    if value.encoded_len() > MAX_FACT_BYTES {
        return Err(Error::Limit("cold Proto exceeds 8MiB"));
    }
    Ok(value.encode_to_vec())
}
pub fn restore<T: Message + Default>(data: &[u8]) -> Result<T> {
    if data.len() > MAX_FACT_BYTES {
        return Err(Error::Limit("cold Proto exceeds 8MiB"));
    }
    Ok(T::decode(data)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{common, facts, market};
    #[test]
    fn cold_proto_preserves_explicit_zero_presence_and_full_archived_identity() {
        let original = facts::ReferenceResponse {
            schema_version: "marketcow.facts.reference.v1".into(),
            meta: Some(facts::FactMeta {
                key: Some(market::MarketKey {
                    market_id: "ended-market".into(),
                    rules: Some(common::RecordIdentity {
                        id: "persistent-rule".into(),
                        revision: 7,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            binance_opening: Some(facts::ReadingState {
                available: true,
                reading: Some(facts::ReferenceReading {
                    price: "100.00".into(),
                    interval_start_ms: Some(0),
                    supplier_report_time_ms: None,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let restored: facts::ReferenceResponse = restore(&export(&original).unwrap()).unwrap();
        assert_eq!(original, restored);
        assert!(restore::<facts::ReferenceResponse>(&[255]).is_err());
        assert!(matches!(
            restore::<facts::ReferenceResponse>(&vec![0; MAX_FACT_BYTES + 1]),
            Err(Error::Limit(_))
        ));
    }
}
