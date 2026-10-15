//! Durable non-content-derived fact identities and immutable baseline latches.
//! This cold path stores protobuf bytes; no JSON or business hash is generated.
use anyhow::{Context, Result, ensure};
use marketcow_contracts::typed_v3::{common::RecordIdentity, market::MarketKey};
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params};

pub fn finality(
    db: &mut Connection,
    key: &MarketKey,
    value: Option<marketcow_contracts::typed_v3::facts::FinalityResponse>,
) -> Result<Option<marketcow_contracts::typed_v3::facts::FinalityResponse>> {
    use marketcow_contracts::typed_v3::facts::FinalityResponse;
    // The finality import permission is backed by a FULL synchronous commit,
    // not by a queued publication or by the general fact revision counter.
    db.pragma_update(None, "synchronous", "FULL")?;
    db.execute_batch("CREATE TABLE IF NOT EXISTS typed_finality_decisions(
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,market_key BLOB NOT NULL UNIQUE,payload BLOB NOT NULL)")?;
    let key = key.encode_to_vec();
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let prior: Option<Vec<u8>> = tx
        .query_row(
            "SELECT payload FROM typed_finality_decisions WHERE market_key=?",
            [&key],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(raw) = prior {
        let old = FinalityResponse::decode(raw.as_slice())?;
        if let Some(new) = value {
            ensure!(
                old.payouts == new.payouts && old.denominator == new.denominator,
                "finality_durable_conflict"
            );
        }
        tx.commit()?;
        return Ok(Some(old));
    }
    let Some(mut value) = value else {
        tx.commit()?;
        return Ok(None);
    };
    ensure!(
        value.status == "awaiting_durability"
            && value.valid_provider_count > 0
            && value.provider_reads_completed_before_decision,
        "finality_not_committable"
    );
    tx.execute(
        "INSERT INTO typed_finality_decisions(market_key,payload) VALUES (?,?)",
        params![key, Vec::<u8>::new()],
    )?;
    let sequence = u64::try_from(tx.last_insert_rowid())?;
    value.status = "verified_final".into();
    value.durable = true;
    value.settlement_import_allowed = true;
    value.durable_sequence = Some(sequence);
    value.request_id.clear();
    tx.execute(
        "UPDATE typed_finality_decisions SET payload=? WHERE sequence=?",
        params![value.encode_to_vec(), i64::try_from(sequence)?],
    )?;
    tx.commit()?;
    Ok(Some(value))
}

pub fn initialize(db: &Connection) -> Result<()> {
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS typed_fact_records (
        namespace TEXT NOT NULL,business_key BLOB NOT NULL,record_id TEXT NOT NULL,
        revision INTEGER NOT NULL,payload BLOB NOT NULL,
        PRIMARY KEY(namespace,business_key,revision));
        CREATE TABLE IF NOT EXISTS typed_baseline_latches (
        market_key BLOB NOT NULL,instrument TEXT NOT NULL,method TEXT NOT NULL,
        baseline_id TEXT NOT NULL,price TEXT NOT NULL,
        PRIMARY KEY(market_key,instrument,method));",
    )?;
    Ok(())
}
pub fn record(
    db: &mut Connection,
    namespace: &str,
    business_key: &[u8],
    payload: &[u8],
) -> Result<RecordIdentity> {
    ensure!(
        !namespace.is_empty()
            && namespace.len() <= 128
            && !business_key.is_empty()
            && business_key.len() <= 4096
            && payload.len() <= 4 * 1024 * 1024,
        "fact_record_capacity"
    );
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let prior:Option<(String,i64,Vec<u8>)>=tx.query_row(
        "SELECT record_id,revision,payload FROM typed_fact_records WHERE namespace=? AND business_key=? ORDER BY revision DESC LIMIT 1",
        params![namespace,business_key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    if let Some((id, revision, bytes)) = &prior {
        if bytes == payload {
            let result = RecordIdentity {
                id: id.clone(),
                revision: *revision as u64,
            };
            tx.commit()?;
            return Ok(result);
        }
    }
    let (id, revision) = match prior {
        Some((id, r, _)) => (id, r.checked_add(1).context("fact_revision_overflow")?),
        None => (uuid::Uuid::new_v4().to_string(), 1),
    };
    tx.execute(
        "INSERT INTO typed_fact_records VALUES (?,?,?,?,?)",
        params![namespace, business_key, id, revision, payload],
    )?;
    tx.commit()?;
    Ok(RecordIdentity {
        id,
        revision: revision as u64,
    })
}
pub fn latch_baseline(
    db: &mut Connection,
    key: &MarketKey,
    instrument: &str,
    method: &str,
    price: &str,
) -> Result<String> {
    super::source_facts_typed::validate_key(key)?;
    ensure!(
        !instrument.is_empty()
            && instrument.len() <= 128
            && !method.is_empty()
            && method.len() <= 128,
        "baseline_method_missing"
    );
    let value = super::btc_typed::positive_decimal(price)?;
    let key = key.encode_to_vec();
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let old: Option<(String, String)> = tx
        .query_row(
            "SELECT baseline_id,price FROM typed_baseline_latches
        WHERE market_key=? AND instrument=? AND method=?",
            params![key, instrument, method],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let id = if let Some((id, old)) = old {
        ensure!(
            super::btc_typed::positive_decimal(&old)? == value,
            "baseline_price_conflict"
        );
        id
    } else {
        let id = uuid::Uuid::new_v4().to_string();
        tx.execute(
            "INSERT INTO typed_baseline_latches VALUES (?,?,?,?,?)",
            params![key, instrument, method, id, price],
        )?;
        id
    };
    tx.commit()?;
    Ok(id)
}

#[cfg(test)]
mod native_tests {
    use super::*;
    use marketcow_contracts::typed_v3::{common::RecordIdentity, facts};
    fn key() -> MarketKey {
        MarketKey {
            market_id: "123".into(),
            condition_id: format!("0x{}", "1".repeat(64)),
            window_id: "btc-updown-5m-1".into(),
            start_at: "2026-10-15T00:00:00Z".into(),
            end_at: "2026-10-15T00:05:00Z".into(),
            up_token_id: "1".into(),
            down_token_id: "2".into(),
            rules: Some(RecordIdentity {
                id: "registered-rule".into(),
                revision: 1,
            }),
        }
    }
    fn provider(index: u8, resolved: bool) -> facts::FinalityProvider {
        facts::FinalityProvider {
            provider_id: format!("provider-{index}"),
            operator_id: format!("operator-{index}"),
            failure_domain: format!("domain-{index}"),
            configuration: Some(RecordIdentity {
                id: format!("config-{index}"),
                revision: 1,
            }),
            status: if resolved {
                "valid_resolved"
            } else {
                "timed_out"
            }
            .into(),
            block: Some(facts::FinalizedBlock {
                hash: format!("0x{}", "2".repeat(64)),
                number: "10".into(),
                source_time_ms: Some(1000),
            }),
            observed_at_ns: Some(2000000000),
            chain_id: Some("0x89".into()),
            ctf_address: Some(format!("0x{}", "3".repeat(40))),
            collateral: Some(format!("0x{}", "4".repeat(40))),
            payouts: vec![
                facts::TokenPayout {
                    token_id: "1".into(),
                    numerator: "1".into(),
                },
                facts::TokenPayout {
                    token_id: "2".into(),
                    numerator: "0".into(),
                },
            ],
            denominator: Some("1".into()),
            token_binding_verified: true,
            deployment_code_verified: true,
            endpoint_identity_verified: true,
            ..Default::default()
        }
    }
    fn pending(providers: [facts::FinalityProvider; 2]) -> facts::FinalityResponse {
        let key = key();
        let request = facts::MarketRequest {
            request_id: "caller-request".into(),
            key: Some(key.clone()),
        };
        let meta = facts::FactMeta {
            identity: Some(RecordIdentity {
                id: "fact-id".into(),
                revision: 1,
            }),
            key: Some(key),
            observed_at_ns: 2000000000,
            evidence: Vec::new(),
        };
        crate::source_facts_typed::decide_finality(&request, meta, providers, None, 15000).unwrap()
    }
    #[test]
    fn native_finality_one_valid_is_not_durable_until_real_commit() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("facts.sqlite3");
        let key = key();
        let value = pending([provider(0, true), provider(1, false)]);
        assert_eq!(value.status, "awaiting_durability");
        assert!(!value.settlement_import_allowed);
        let mut db = crate::source_typed_identity::open(&path).unwrap();
        let committed = finality(&mut db, &key, Some(value)).unwrap().unwrap();
        assert!(committed.durable && committed.settlement_import_allowed);
        assert_eq!(committed.valid_provider_count, 1);
        drop(db);
        let mut db = crate::source_typed_identity::open(&path).unwrap();
        let restored = finality(&mut db, &key, None).unwrap().unwrap();
        assert_eq!(restored, committed);
        let mut conflict = pending([provider(0, true), provider(1, false)]);
        conflict.payouts.reverse();
        assert!(finality(&mut db, &key, Some(conflict)).is_err());
    }
    #[test]
    fn native_finality_conflict_or_failure_is_never_importable() {
        let mut other = provider(1, true);
        other.payouts[0].numerator = "0".into();
        other.payouts[1].numerator = "1".into();
        let conflict = pending([provider(0, true), other]);
        assert_eq!(conflict.status, "conflict");
        assert!(conflict.payouts.is_empty());
        assert!(!conflict.settlement_import_allowed);
        let failed = pending([provider(0, false), provider(1, false)]);
        assert_eq!(failed.status, "provider_failed");
        assert!(!failed.durable);
        let mut db = Connection::open_in_memory().unwrap();
        assert!(finality(&mut db, &key(), Some(failed)).is_err());
    }
    #[test]
    fn native_persistent_identity_revision_and_baseline_conflict() {
        let mut db = Connection::open_in_memory().unwrap();
        initialize(&db).unwrap();
        let first = record(&mut db, "test", b"key", b"one").unwrap();
        assert_eq!(first, record(&mut db, "test", b"key", b"one").unwrap());
        let next = record(&mut db, "test", b"key", b"two").unwrap();
        assert_eq!(next.id, first.id);
        assert_eq!(next.revision, first.revision + 1);
        let key = key();
        let id =
            latch_baseline(&mut db, &key, "BTCUSDT.BINANCE", "trade_twap_60s", "100.00").unwrap();
        assert_eq!(
            id,
            latch_baseline(&mut db, &key, "BTCUSDT.BINANCE", "trade_twap_60s", "100").unwrap()
        );
        assert!(latch_baseline(&mut db, &key, "BTCUSDT.BINANCE", "trade_twap_60s", "101").is_err());
    }
}
