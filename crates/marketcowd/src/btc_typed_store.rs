//! Indexed typed records and explicit audit evidence. No content hashes or
//! normal-query supplier-body parsing. Only ingress adapters parse raw JSON.
use anyhow::{Context, Result, ensure};
use marketcow_contracts::typed_v3::{
    btc,
    common::{EvidenceReference, Source},
    history,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use std::{path::Path, time::Duration};

pub struct EvidenceInput<'a> {
    pub raw: &'a [u8],
    pub acquisition: &'a str,
    pub upstream_url: &'a str,
    pub received_at_ns: Option<i64>,
    pub source_generation: Option<&'a str>,
    pub content_type: &'a str,
}
pub fn open(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let db = Connection::open(path)?;
    db.busy_timeout(Duration::from_secs(2))?;
    db.pragma_update(None, "journal_mode", "WAL")?;
    db.pragma_update(None, "synchronous", "NORMAL")?;
    db.pragma_update(None, "foreign_keys", "ON")?;
    initialize(&db)?;
    Ok(db)
}
pub fn initialize(db: &Connection) -> Result<()> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS typed_evidence (
      evidence_id TEXT PRIMARY KEY,raw BLOB NOT NULL,acquisition TEXT NOT NULL,
      upstream_url TEXT NOT NULL,received_at_ns INTEGER,source_generation TEXT,content_type TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS typed_binance_trades (
      trade_id INTEGER PRIMARY KEY,trade_time_ms INTEGER NOT NULL,event_time_ms INTEGER,
      price TEXT NOT NULL,quantity TEXT NOT NULL,buyer_is_maker INTEGER NOT NULL,
      received_at_ns INTEGER,evidence_id TEXT NOT NULL,source_row_index INTEGER NOT NULL,
      FOREIGN KEY(evidence_id) REFERENCES typed_evidence(evidence_id));
    CREATE INDEX IF NOT EXISTS typed_binance_time_id ON typed_binance_trades(trade_time_ms,trade_id);
    CREATE TABLE IF NOT EXISTS typed_chainlink_observations (
      observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
      source INTEGER NOT NULL,source_generation TEXT NOT NULL,source_sequence INTEGER NOT NULL,
      row_index INTEGER NOT NULL,source_time_ms INTEGER NOT NULL,price TEXT NOT NULL,
      received_at_ns INTEGER,evidence_id TEXT NOT NULL,
      UNIQUE(source,source_generation,source_sequence,row_index),
      FOREIGN KEY(evidence_id) REFERENCES typed_evidence(evidence_id));
    CREATE INDEX IF NOT EXISTS typed_chainlink_time ON typed_chainlink_observations(source,source_time_ms);
    CREATE INDEX IF NOT EXISTS typed_chainlink_id ON typed_chainlink_observations(source,observation_id);
    CREATE TABLE IF NOT EXISTS typed_database_meta (key TEXT PRIMARY KEY,value INTEGER NOT NULL);
    INSERT OR IGNORE INTO typed_database_meta VALUES ('repair_revision',1);
    CREATE TABLE IF NOT EXISTS typed_trade_repairs (
      revision INTEGER PRIMARY KEY AUTOINCREMENT, trade_id INTEGER NOT NULL);
    CREATE INDEX IF NOT EXISTS typed_trade_repairs_id ON typed_trade_repairs(trade_id,revision);")?;
    Ok(())
}
pub fn open_reader(path: &Path) -> Result<Connection> {
    ensure!(
        path.is_file() && !path.is_symlink(),
        "typed_database_unavailable"
    );
    let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    db.busy_timeout(Duration::from_secs(2))?;
    Ok(db)
}
fn evidence(tx: &rusqlite::Transaction<'_>, input: &EvidenceInput<'_>) -> Result<String> {
    ensure!(
        !input.raw.is_empty() && input.raw.len() <= 1024 * 1024,
        "evidence byte capacity"
    );
    ensure!(
        !input.acquisition.is_empty() && !input.upstream_url.is_empty(),
        "evidence provenance"
    );
    let id = uuid::Uuid::new_v4().to_string();
    tx.execute(
        "INSERT INTO typed_evidence VALUES (?,?,?,?,?,?,?)",
        params![
            id,
            input.raw,
            input.acquisition,
            input.upstream_url,
            input.received_at_ns,
            input.source_generation,
            input.content_type
        ],
    )?;
    Ok(id)
}
fn trade(row: &rusqlite::Row<'_>) -> rusqlite::Result<btc::Trade> {
    Ok(btc::Trade {
        trade_id: row.get::<_, i64>(0)? as u64,
        trade_time_ms: row.get(1)?,
        event_time_ms: row.get(2)?,
        price: row.get(3)?,
        quantity: row.get(4)?,
        buyer_is_maker: Some(row.get::<_, i64>(5)? != 0),
        received_at_ns: row.get(6)?,
        evidence: Some(EvidenceReference {
            evidence_id: row.get(7)?,
            row_index: Some(row.get(8)?),
        }),
    })
}
const SELECT_TRADE: &str = "SELECT trade_id,trade_time_ms,event_time_ms,price,quantity,
    buyer_is_maker,received_at_ns,evidence_id,source_row_index FROM typed_binance_trades";
pub fn insert_trades(
    db: &mut Connection,
    rows: &[btc::Trade],
    input: &EvidenceInput<'_>,
) -> Result<usize> {
    ensure!(
        !rows.is_empty() && rows.len() <= 1000,
        "trade batch capacity"
    );
    for row in rows {
        super::btc_typed::validate_trade(row)?;
    }
    let tx = db.transaction()?;
    let mut evidence_id = None;
    let mut inserted = 0;
    let high: Option<i64> =
        tx.query_row("SELECT MAX(trade_id) FROM typed_binance_trades", [], |r| {
            r.get(0)
        })?;
    // Prepare once per batch and reuse the connection's cache across batches.
    // Supplier facts and audit bodies are never serialized for these lookups.
    {
        let mut select = tx.prepare_cached(&format!("{SELECT_TRADE} WHERE trade_id=?"))?;
        let mut insert =
            tx.prepare_cached("INSERT INTO typed_binance_trades VALUES (?,?,?,?,?,?,?,?,?)")?;
        let mut update = tx.prepare_cached("UPDATE typed_binance_trades SET event_time_ms=?,price=?,quantity=?,received_at_ns=?,evidence_id=?,source_row_index=? WHERE trade_id=?")?;
        let mut repair =
            tx.prepare_cached("INSERT INTO typed_trade_repairs(trade_id) VALUES (?)")?;
        for (index, row) in rows.iter().enumerate() {
            let id = i64::try_from(row.trade_id).context("trade ID database range")?;
            let prior = select.query_row([id], trade).optional()?;
            if let Some(prior) = prior {
                ensure!(
                    prior.trade_time_ms == row.trade_time_ms
                        && (prior.event_time_ms.is_none()
                            || row.event_time_ms.is_none()
                            || prior.event_time_ms == row.event_time_ms)
                        && super::btc_typed::positive_decimal(&prior.price)?
                            == super::btc_typed::positive_decimal(&row.price)?
                        && super::btc_typed::positive_decimal(&prior.quantity)?
                            == super::btc_typed::positive_decimal(&row.quantity)?
                        && prior.buyer_is_maker == row.buyer_is_maker,
                    "trade identity conflict"
                );
                if prior.event_time_ms.is_none() && row.event_time_ms.is_some() {
                    let ev = match &evidence_id {
                        Some(id) => id,
                        None => evidence_id.insert(evidence(&tx, input)?),
                    };
                    update.execute(params![
                        row.event_time_ms,
                        row.price,
                        row.quantity,
                        row.received_at_ns,
                        ev,
                        index as i64,
                        id
                    ])?;
                    repair.execute([id])?;
                }
                continue;
            }
            let ev = match &evidence_id {
                Some(id) => id,
                None => evidence_id.insert(evidence(&tx, input)?),
            };
            insert.execute(params![
                id,
                row.trade_time_ms,
                row.event_time_ms,
                row.price,
                row.quantity,
                i64::from(row.buyer_is_maker.context("maker missing")?),
                row.received_at_ns,
                ev,
                index as i64
            ])?;
            inserted += 1;
            if high.is_some_and(|before| id <= before) {
                repair.execute([id])?;
            }
        }
    }
    tx.commit()?;
    Ok(inserted)
}
pub fn insert_observations(
    db: &mut Connection,
    source: Source,
    generation: &str,
    rows: &[btc::Observation],
    input: &EvidenceInput<'_>,
) -> Result<usize> {
    ensure!(
        matches!(
            source,
            Source::ChainlinkBtcUsd | Source::ChainlinkBtcUsdTwap60
        ) && !generation.is_empty()
            && !rows.is_empty()
            && rows.len() <= 4096,
        "observation batch identity/capacity"
    );
    let tx = db.transaction()?;
    let mut evidence_id = None;
    let mut inserted = 0;
    {
        let mut select = tx.prepare_cached("SELECT source_time_ms,price FROM typed_chainlink_observations WHERE source=? AND source_generation=? AND source_sequence=? AND row_index=?")?;
        let mut insert = tx.prepare_cached("INSERT INTO typed_chainlink_observations(source,source_generation,source_sequence,row_index,source_time_ms,price,received_at_ns,evidence_id) VALUES (?,?,?,?,?,?,?,?)")?;
        for row in rows {
            ensure!(
                row.source_time_ms > 0
                    && row.source_sequence.is_some_and(|s| s > 0)
                    && row.row_index.is_some(),
                "observation identity/time"
            );
            super::btc_typed::positive_decimal(&row.price)?;
            let sequence = i64::try_from(row.source_sequence.context("sequence missing")?)?;
            let prior: Option<(i64, String)> = select
                .query_row(
                    params![source as i32, generation, sequence, row.row_index],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if let Some((time, price)) = prior {
                ensure!(
                    time == row.source_time_ms
                        && super::btc_typed::positive_decimal(&price)?
                            == super::btc_typed::positive_decimal(&row.price)?,
                    "observation identity conflict"
                );
                continue;
            }
            let ev = match &evidence_id {
                Some(id) => id,
                None => evidence_id.insert(evidence(&tx, input)?),
            };
            insert.execute(params![
                source as i32,
                generation,
                sequence,
                row.row_index,
                row.source_time_ms,
                row.price,
                row.received_at_ns,
                ev
            ])?;
            inserted += 1;
        }
    }
    tx.commit()?;
    Ok(inserted)
}
pub fn read_trade(db: &Connection, id: u64) -> Result<Option<btc::Trade>> {
    Ok(db
        .query_row(
            &format!("{SELECT_TRADE} WHERE trade_id=?"),
            [i64::try_from(id)?],
            trade,
        )
        .optional()?)
}
pub fn read_trades_with_generations(
    db: &Connection,
    first: u64,
    through: u64,
    limit: usize,
) -> Result<Vec<(btc::Trade, Option<String>)>> {
    ensure!(
        first > 0 && first <= through && (1..=1000).contains(&limit),
        "typed page range"
    );
    let mut stmt=db.prepare("SELECT t.trade_id,t.trade_time_ms,t.event_time_ms,t.price,t.quantity,t.buyer_is_maker,t.received_at_ns,t.evidence_id,t.source_row_index,e.source_generation FROM typed_binance_trades t JOIN typed_evidence e ON e.evidence_id=t.evidence_id WHERE t.trade_id>=? AND t.trade_id<=? ORDER BY t.trade_id LIMIT ?")?;
    Ok(stmt
        .query_map(
            params![i64::try_from(first)?, i64::try_from(through)?, limit as i64],
            |r| Ok((trade(r)?, r.get(9)?)),
        )?
        .collect::<rusqlite::Result<_>>()?)
}
pub fn read_trades(
    db: &Connection,
    first: u64,
    through: u64,
    limit: usize,
) -> Result<Vec<btc::Trade>> {
    ensure!(
        first > 0 && first <= through && (1..=1000).contains(&limit),
        "typed page range"
    );
    let mut stmt = db.prepare(&format!(
        "{SELECT_TRADE} WHERE trade_id>=? AND trade_id<=? ORDER BY trade_id LIMIT ?"
    ))?;
    let rows = stmt.query_map(
        params![i64::try_from(first)?, i64::try_from(through)?, limit as i64],
        trade,
    )?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}
pub fn read_evidence(db: &Connection, id: &str) -> Result<Option<history::Evidence>> {
    ensure!(uuid::Uuid::parse_str(id).is_ok(), "evidence identity");
    Ok(db
        .query_row(
            "SELECT evidence_id,raw,acquisition,upstream_url,received_at_ns,
      source_generation,content_type FROM typed_evidence WHERE evidence_id=?",
            [id],
            |r| {
                Ok(history::Evidence {
                    schema_version: "marketcow.btc-evidence.v3".into(),
                    evidence_id: r.get(0)?,
                    raw_body: r.get(1)?,
                    acquisition: r.get(2)?,
                    upstream_url: r.get(3)?,
                    received_at_ns: r.get(4)?,
                    source_generation: r.get(5)?,
                    content_type: r.get(6)?,
                })
            },
        )
        .optional()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> EvidenceInput<'static> {
        EvidenceInput {
            raw: b"opaque audit body: business queries do not parse this",
            acquisition: "ws_archive",
            upstream_url: "supplier-test",
            received_at_ns: Some(1000),
            source_generation: Some("generation-a"),
            content_type: "application/json",
        }
    }

    fn database() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        initialize(&db).unwrap();
        db
    }

    #[test]
    fn cached_trade_batch_preserves_lexical_identity_repair_and_atomic_rollback() {
        let mut db = database();
        let rows = (1..=32)
            .map(|id| btc::Trade {
                trade_id: id,
                trade_time_ms: 1000 + id as i64,
                price: "100.00".into(),
                quantity: "0.0100".into(),
                buyer_is_maker: Some(false),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        assert_eq!(insert_trades(&mut db, &rows, &input()).unwrap(), 32);
        assert_eq!(insert_trades(&mut db, &rows, &input()).unwrap(), 0);
        let original = read_trade(&db, 1).unwrap().unwrap();
        assert_eq!(original.price, "100.00");
        assert_eq!(original.quantity, "0.0100");
        assert_eq!(
            read_evidence(&db, &original.evidence.unwrap().evidence_id)
                .unwrap()
                .unwrap()
                .raw_body,
            input().raw
        );
        let mut enrichment = rows[0].clone();
        enrichment.event_time_ms = Some(1010);
        enrichment.price = "100.0000".into();
        insert_trades(&mut db, &[enrichment.clone()], &input()).unwrap();
        let enriched = read_trade(&db, 1).unwrap().unwrap();
        assert_eq!(enriched.event_time_ms, Some(1010));
        assert_eq!(enriched.price, "100.0000");
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM typed_trade_repairs", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        let evidence_count: i64 = db
            .query_row("SELECT COUNT(*) FROM typed_evidence", [], |r| r.get(0))
            .unwrap();
        let mut fresh = rows[0].clone();
        fresh.trade_id = 33;
        let mut conflict = enrichment;
        conflict.price = "101".into();
        assert!(
            insert_trades(&mut db, &[fresh.clone(), conflict], &input())
                .unwrap_err()
                .to_string()
                .contains("identity conflict")
        );
        assert!(read_trade(&db, 33).unwrap().is_none());
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM typed_evidence", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            evidence_count
        );
        assert_eq!(insert_trades(&mut db, &[fresh], &input()).unwrap(), 1);
    }

    #[test]
    fn cached_observation_batch_preserves_kind_and_atomic_rollback() {
        let mut db = database();
        let rows = (0..32)
            .map(|index| btc::Observation {
                source_sequence: Some(1),
                row_index: Some(index),
                source_time_ms: 1000,
                price: "100.00".into(),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let source = Source::ChainlinkBtcUsdTwap60;
        assert_eq!(
            insert_observations(&mut db, source, "a", &rows, &input()).unwrap(),
            32
        );
        assert_eq!(
            insert_observations(&mut db, source, "a", &rows, &input()).unwrap(),
            0
        );
        // Ordinary and averaged observations cannot deduplicate into each other.
        assert_eq!(
            insert_observations(&mut db, Source::ChainlinkBtcUsd, "a", &rows[..1], &input())
                .unwrap(),
            1
        );
        let mut fresh = rows[0].clone();
        fresh.source_sequence = Some(2);
        let mut conflict = rows[0].clone();
        conflict.price = "101".into();
        assert!(
            insert_observations(&mut db, source, "a", &[fresh.clone(), conflict], &input())
                .is_err()
        );
        assert_eq!(
            db.query_row(
                "SELECT COUNT(*) FROM typed_chainlink_observations",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            33
        );
        assert_eq!(
            insert_observations(&mut db, source, "a", &[fresh], &input()).unwrap(),
            1
        );
    }
}
