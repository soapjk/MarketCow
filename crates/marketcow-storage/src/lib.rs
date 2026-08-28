//! Narrow repository ports and an explicit workload router.
//! Realtime projection is always memory-owned and cannot route to SQLite or a remote DB.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    MemoryProjection,
    PostgreSql,
    ClickHouse,
    SqliteDerived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Workload {
    RealtimeProjection,
    Instrument,
    Job,
    Audit,
    ArtifactManifest,
    Fundamental,
    MarketBar,
    QuoteHistory,
    MigrationCheckpoint,
    DerivedOfflineIndex,
}

pub fn backend_for(workload: Workload) -> Backend {
    match workload {
        Workload::RealtimeProjection => Backend::MemoryProjection,
        Workload::Instrument
        | Workload::Job
        | Workload::Audit
        | Workload::ArtifactManifest
        | Workload::Fundamental
        | Workload::MigrationCheckpoint => Backend::PostgreSql,
        Workload::MarketBar | Workload::QuoteHistory => Backend::ClickHouse,
        Workload::DerivedOfflineIndex => Backend::SqliteDerived,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuoteRecord {
    pub instrument_id: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub bid: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub ask: Decimal,
    pub currency: String,
    pub scale: u32,
    pub observed_at: DateTime<Utc>,
    pub source: String,
    pub raw_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyKey(pub String);

pub trait InstrumentRepository: Send + Sync {
    fn exists(&self, instrument_id: &str) -> Result<bool, RepositoryError>;
}
pub trait JobRepository: Send + Sync {
    fn create_once(&self, key: &IdempotencyKey) -> Result<String, RepositoryError>;
}
pub trait AuditRepository: Send + Sync {
    fn append(&self, actor: &str, action: &str) -> Result<(), RepositoryError>;
}
pub trait ArtifactManifestRepository: Send + Sync {
    fn register_verified(&self, sha256: &str, size: u64) -> Result<(), RepositoryError>;
}
pub trait FundamentalRepository: Send + Sync {}
pub trait MarketBarRepository: Send + Sync {}
pub trait QuoteRepository: Send + Sync {
    fn latest(&self, instrument_id: &str) -> Result<Option<QuoteRecord>, RepositoryError>;
}
pub trait MigrationCheckpointRepository: Send + Sync {}
pub trait DerivedIndexRepository: Send + Sync {
    fn rebuild(&self, persisted_cursor: u64) -> Result<(), RepositoryError>;
}

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("repository timeout")]
    Timeout,
    #[error("idempotency conflict")]
    IdempotencyConflict,
    #[error("invalid repository input")]
    InvalidInput,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_workload_has_one_owner_and_hot_path_is_memory_only() {
        assert_eq!(
            backend_for(Workload::RealtimeProjection),
            Backend::MemoryProjection
        );
        assert_ne!(
            backend_for(Workload::RealtimeProjection),
            Backend::SqliteDerived
        );
        assert_eq!(
            backend_for(Workload::DerivedOfflineIndex),
            Backend::SqliteDerived
        );
        assert_eq!(backend_for(Workload::Instrument), Backend::PostgreSql);
        assert_eq!(backend_for(Workload::MarketBar), Backend::ClickHouse);
    }

    #[test]
    fn quote_decimal_json_round_trip_preserves_precision_and_currency() {
        let record = QuoteRecord {
            instrument_id: "POLY:condition:yes".into(),
            bid: "0.100000000000000001".parse().unwrap(),
            ask: "0.200000000000000002".parse().unwrap(),
            currency: "USDC".into(),
            scale: 18,
            observed_at: Utc::now(),
            source: "polymarket".into(),
            raw_sha256: "a".repeat(64),
        };
        let value = serde_json::to_value(&record).unwrap();
        assert_eq!(value["bid"], "0.100000000000000001");
        assert_eq!(value["currency"], "USDC");
        assert_eq!(
            serde_json::from_value::<QuoteRecord>(value).unwrap(),
            record
        );
    }
}
