//! Persistent business identities; never content-derived or hash-checked.
use anyhow::{Context, Result, ensure};
use marketcow_contracts::typed_v3::{common::RecordIdentity, market::RulesRecord};
use prost::Message;
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

pub fn open(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let db = Connection::open(path)?;
    db.busy_timeout(std::time::Duration::from_secs(2))?;
    db.pragma_update(None, "journal_mode", "WAL")?;
    db.pragma_update(None, "synchronous", "FULL")?;
    initialize(&db)?;
    Ok(db)
}
pub fn memory() -> Result<Connection> {
    let db = Connection::open_in_memory()?;
    initialize(&db)?;
    Ok(db)
}
fn initialize(db: &Connection) -> Result<()> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS typed_rules (
      rules_id TEXT NOT NULL,revision INTEGER NOT NULL,
      market_id TEXT NOT NULL,condition_id TEXT NOT NULL,window_id TEXT NOT NULL,
      start_at TEXT NOT NULL,end_at TEXT NOT NULL,up_token_id TEXT NOT NULL,down_token_id TEXT NOT NULL,
      description TEXT NOT NULL,resolution_source TEXT NOT NULL,
      PRIMARY KEY(rules_id,revision));
    CREATE UNIQUE INDEX IF NOT EXISTS typed_rules_market_revision ON typed_rules(market_id,revision);
    CREATE TABLE IF NOT EXISTS typed_legacy_identity (
      namespace TEXT NOT NULL,legacy_id TEXT NOT NULL,record_id TEXT NOT NULL,revision INTEGER NOT NULL,
      PRIMARY KEY(namespace,legacy_id));")?;
    let columns = db
        .prepare("PRAGMA table_info(typed_rules)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !columns.iter().any(|c| c == "resolution_proto") {
        db.execute_batch(
            "ALTER TABLE typed_rules ADD COLUMN resolution_proto BLOB;
            ALTER TABLE typed_rules ADD COLUMN evidence_proto BLOB;",
        )?;
    }
    Ok(())
}
fn read(row: &rusqlite::Row<'_>) -> rusqlite::Result<RulesRecord> {
    let decode = |index| -> rusqlite::Result<Option<RulesRecord>> {
        let bytes: Option<Vec<u8>> = row.get(index)?;
        bytes
            .map(|bytes| {
                RulesRecord::decode(bytes.as_slice()).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        index,
                        rusqlite::types::Type::Blob,
                        Box::new(e),
                    )
                })
            })
            .transpose()
    };
    let resolution = decode(11)?.and_then(|r| r.resolution);
    let evidence = decode(12)?.map(|r| r.evidence).unwrap_or_default();
    Ok(RulesRecord {
        identity: Some(RecordIdentity {
            id: row.get(0)?,
            revision: row.get::<_, i64>(1)? as u64,
        }),
        market_id: row.get(2)?,
        condition_id: row.get(3)?,
        window_id: row.get(4)?,
        start_at: row.get(5)?,
        end_at: row.get(6)?,
        up_token_id: row.get(7)?,
        down_token_id: row.get(8)?,
        description: row.get(9)?,
        resolution_source: row.get(10)?,
        resolution,
        evidence,
    })
}
pub fn bind_rules(db: &mut Connection, mut rules: RulesRecord) -> Result<RulesRecord> {
    ensure!(
        rules.identity.is_none(),
        "rules identity must be allocated by store"
    );
    ensure!(
        !rules.market_id.is_empty()
            && !rules.condition_id.is_empty()
            && !rules.window_id.is_empty()
            && !rules.description.is_empty()
            && rules.description.len() <= 256 * 1024
            && !rules.resolution_source.is_empty(),
        "rules fields"
    );
    ensure!(
        !rules.up_token_id.is_empty()
            && !rules.down_token_id.is_empty()
            && rules.up_token_id != rules.down_token_id,
        "rules token binding"
    );
    let start = chrono::DateTime::parse_from_rfc3339(&rules.start_at)?;
    let end = chrono::DateTime::parse_from_rfc3339(&rules.end_at)?;
    ensure!(start < end, "rules interval");
    if let Some(terms) = &rules.resolution {
        ensure!(
            matches!(
                terms.verification.as_str(),
                "verified" | "unverified" | "unsupported"
            ),
            "rule verification"
        );
        if terms.verification == "verified" {
            ensure!(
                terms
                    .source_instrument
                    .as_ref()
                    .is_some_and(|s| !s.is_empty())
                    && terms.quote_currency.as_ref().is_some_and(|s| !s.is_empty())
                    && terms
                        .price_method
                        .as_deref()
                        .is_some_and(|s| matches!(s, "spot" | "twap"))
                    && terms
                        .opening_selection
                        .as_ref()
                        .is_some_and(|s| !s.is_empty())
                    && terms
                        .closing_selection
                        .as_ref()
                        .is_some_and(|s| !s.is_empty())
                    && terms
                        .interval_convention
                        .as_ref()
                        .is_some_and(|s| !s.is_empty())
                    && terms.comparison.as_ref().is_some_and(|s| !s.is_empty())
                    && terms
                        .equality_outcome
                        .as_deref()
                        .is_some_and(|s| matches!(s, "Up" | "Down"))
                    && terms
                        .provenance
                        .as_deref()
                        .is_some_and(|s| matches!(s, "official_rule" | "explicit_configuration"))
                    && terms.evidence_id.as_ref().is_some_and(|s| !s.is_empty()),
                "verified rule terms incomplete"
            );
            ensure!(
                terms
                    .report_time_semantics
                    .as_ref()
                    .is_some_and(|s| !s.is_empty())
                    && terms.opening_boundary_offset_ms.is_some()
                    && terms.closing_boundary_offset_ms.is_some()
                    && terms.maximum_report_lookback_ms.is_some(),
                "rule report selection incomplete"
            );
            ensure!(
                terms.price_method.as_deref() != Some("twap")
                    || terms.averaging_seconds.is_some_and(|s| s > 0),
                "TWAP interval missing"
            );
        }
    }
    // Immediate transaction arbitrates identity creation across processes.
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let previous = tx
        .query_row(
            "SELECT * FROM typed_rules WHERE market_id=? ORDER BY revision DESC LIMIT 1",
            [&rules.market_id],
            read,
        )
        .optional()?;
    if let Some(mut prior) = previous.clone() {
        let identity = prior.identity.take();
        if prior == rules {
            rules.identity = identity;
            tx.commit()?;
            return Ok(rules);
        }
    }
    let identity = if let Some(prior) = previous {
        let identity = prior.identity.context("stored rules identity")?;
        RecordIdentity {
            id: identity.id,
            revision: identity
                .revision
                .checked_add(1)
                .context("rules revision overflow")?,
        }
    } else {
        RecordIdentity {
            id: uuid::Uuid::new_v4().to_string(),
            revision: 1,
        }
    };
    tx.execute(
        "INSERT INTO typed_rules VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        params![
            identity.id,
            i64::try_from(identity.revision)?,
            rules.market_id,
            rules.condition_id,
            rules.window_id,
            rules.start_at,
            rules.end_at,
            rules.up_token_id,
            rules.down_token_id,
            rules.description,
            rules.resolution_source,
            RulesRecord {
                resolution: rules.resolution.clone(),
                ..Default::default()
            }
            .encode_to_vec(),
            RulesRecord {
                evidence: rules.evidence.clone(),
                ..Default::default()
            }
            .encode_to_vec()
        ],
    )?;
    tx.commit()?;
    rules.identity = Some(identity);
    Ok(rules)
}
pub fn rules(db: &Connection, identity: &RecordIdentity) -> Result<Option<RulesRecord>> {
    ensure!(
        !identity.id.is_empty() && identity.revision > 0,
        "rules identity"
    );
    Ok(db
        .query_row(
            "SELECT * FROM typed_rules WHERE rules_id=? AND revision=?",
            params![identity.id, i64::try_from(identity.revision)?],
            read,
        )
        .optional()?)
}
/// Explicit migration only: the opaque old ID is recorded verbatim and never
/// recomputed. A conflicting remap is rejected rather than rewriting history.
pub fn map_legacy(
    db: &mut Connection,
    namespace: &str,
    old: &str,
    new: &RecordIdentity,
) -> Result<()> {
    ensure!(
        !namespace.is_empty() && !old.is_empty() && !new.id.is_empty() && new.revision > 0,
        "legacy mapping identity"
    );
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let prior: Option<(String, i64)> = tx
        .query_row(
            "SELECT record_id,revision FROM typed_legacy_identity
        WHERE namespace=? AND legacy_id=?",
            params![namespace, old],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((id, revision)) = prior {
        ensure!(
            id == new.id && revision == i64::try_from(new.revision)?,
            "legacy identity mapping conflict"
        );
    } else {
        tx.execute(
            "INSERT INTO typed_legacy_identity VALUES (?,?,?,?)",
            params![namespace, old, new.id, i64::try_from(new.revision)?],
        )?;
    }
    tx.commit()?;
    Ok(())
}
