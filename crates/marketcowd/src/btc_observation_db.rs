//! Queryable BTC observations. JSONL is an independent forensic archive, not a read index.
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{path::Path, time::Duration};

pub fn open(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
    let db = Connection::open(path)?;
    db.busy_timeout(Duration::from_secs(2))?;
    db.pragma_update(None, "journal_mode", "WAL")?;
    // The independent JSONL writer retains original evidence. WAL NORMAL
    // avoids an fsync per trade on the derived query index; a crash can lose
    // recent index commits, which are reported as coverage gaps and repaired
    // only by an explicit archive import.
    db.pragma_update(None, "synchronous", "NORMAL")?;
    db.pragma_update(None, "journal_size_limit", 256 * 1024 * 1024)?;
    db.execute_batch("CREATE TABLE IF NOT EXISTS evidence (
        sha256 TEXT PRIMARY KEY, raw BLOB NOT NULL, received_at TEXT,
        source_epoch TEXT, acquisition TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS binance_trades (
        trade_id INTEGER PRIMARY KEY, trade_time_ms INTEGER NOT NULL,
        event_time_ms INTEGER, price TEXT NOT NULL, quantity TEXT NOT NULL,
        buyer_is_maker INTEGER NOT NULL, evidence_sha256 TEXT NOT NULL,
        source_row_index INTEGER NOT NULL, source_epoch TEXT,
        FOREIGN KEY(evidence_sha256) REFERENCES evidence(sha256));
      CREATE INDEX IF NOT EXISTS binance_time_id ON binance_trades(trade_time_ms,trade_id);
      CREATE TABLE IF NOT EXISTS binance_trade_versions (
        version INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id INTEGER NOT NULL,trade_time_ms INTEGER NOT NULL,event_time_ms INTEGER,
        price TEXT NOT NULL,quantity TEXT NOT NULL,buyer_is_maker INTEGER NOT NULL,
        evidence_sha256 TEXT NOT NULL,source_row_index INTEGER NOT NULL,source_epoch TEXT);
      CREATE INDEX IF NOT EXISTS binance_version_id ON binance_trade_versions(trade_id,version);
      CREATE TRIGGER IF NOT EXISTS binance_history_insert AFTER INSERT ON binance_trades BEGIN
        INSERT INTO binance_trade_versions(trade_id,trade_time_ms,event_time_ms,price,quantity,
          buyer_is_maker,evidence_sha256,source_row_index,source_epoch)
        VALUES(NEW.trade_id,NEW.trade_time_ms,NEW.event_time_ms,NEW.price,NEW.quantity,
          NEW.buyer_is_maker,NEW.evidence_sha256,NEW.source_row_index,NEW.source_epoch);
      END;
      CREATE TRIGGER IF NOT EXISTS binance_history_update AFTER UPDATE ON binance_trades BEGIN
        INSERT INTO binance_trade_versions(trade_id,trade_time_ms,event_time_ms,price,quantity,
          buyer_is_maker,evidence_sha256,source_row_index,source_epoch)
        VALUES(NEW.trade_id,NEW.trade_time_ms,NEW.event_time_ms,NEW.price,NEW.quantity,
          NEW.buyer_is_maker,NEW.evidence_sha256,NEW.source_row_index,NEW.source_epoch);
      END;
      CREATE TABLE IF NOT EXISTS observation_schema_migrations (name TEXT PRIMARY KEY);
      CREATE TABLE IF NOT EXISTS chainlink_observations (
        kind TEXT NOT NULL, source_epoch TEXT NOT NULL, source_sequence INTEGER NOT NULL,
        source_time_ms INTEGER NOT NULL, price TEXT NOT NULL,
        evidence_sha256 TEXT NOT NULL, source_row_index INTEGER NOT NULL,
        PRIMARY KEY(kind,source_epoch,source_sequence,source_row_index),
        FOREIGN KEY(evidence_sha256) REFERENCES evidence(sha256));
      CREATE INDEX IF NOT EXISTS chainlink_time_kind ON chainlink_observations(kind,source_time_ms);
      CREATE TABLE IF NOT EXISTS ingest_health (
        source TEXT PRIMARY KEY, last_success_at TEXT, last_source_time_ms INTEGER,
        last_source_sequence INTEGER, last_error TEXT, failed_at TEXT);")?;
    db.pragma_update(None, "foreign_keys", "ON")?;
    // One explicit database schema backfill, never an archive scan. Existing
    // rows become immutable history versions before indexed sessions use them.
    let migrated:bool=db.query_row("SELECT EXISTS(SELECT 1 FROM observation_schema_migrations
      WHERE name='binance_versions_v1')",[],|r|r.get(0))?;
    if !migrated {
        db.execute_batch("BEGIN IMMEDIATE;
          INSERT INTO binance_trade_versions(trade_id,trade_time_ms,event_time_ms,price,quantity,
            buyer_is_maker,evidence_sha256,source_row_index,source_epoch)
          SELECT t.trade_id,t.trade_time_ms,t.event_time_ms,t.price,t.quantity,t.buyer_is_maker,
            t.evidence_sha256,t.source_row_index,t.source_epoch FROM binance_trades t
          WHERE NOT EXISTS(SELECT 1 FROM binance_trade_versions v WHERE v.trade_id=t.trade_id);
          INSERT INTO observation_schema_migrations VALUES('binance_versions_v1'); COMMIT;")?;
    }
    Ok(db)
}

fn digest(raw: &[u8]) -> String { hex::encode(Sha256::digest(raw)) }
fn decimal(value: &Value) -> Result<String> {
    let s = value.as_str().context("database_price_or_quantity_missing")?;
    let n: rust_decimal::Decimal = s.parse()?;
    ensure!(n > rust_decimal::Decimal::ZERO, "database_nonpositive_decimal");
    Ok(n.normalize().to_string())
}

fn ingest_binance_tx(tx: &rusqlite::Transaction<'_>, raw: &[u8], epoch: &str,
    received_at: &str) -> Result<usize> {
    let value: Value = serde_json::from_slice(raw)?;
    let rows = value.as_array().cloned().unwrap_or_else(|| vec![value]);
    ensure!(!rows.is_empty() && rows.len() <= 1000, "database_trade_frame_capacity");
    let sha = digest(raw);
    tx.execute("INSERT OR IGNORE INTO evidence VALUES (?1,?2,?3,?4,?5)",
        params![sha,raw,received_at,epoch,if rows[0]["e"]=="trade" {"ws_trade"} else {"official_recent_trades"}])?;
    let mut last = None;
    for (index,row) in rows.iter().enumerate() {
        let ws = row["e"] == "trade" && row["s"] == "BTCUSDT";
        let id = if ws {row["t"].as_u64()} else {row["id"].as_u64()}
            .context("database_trade_id_missing")?;
        let id=i64::try_from(id).context("database_trade_id_out_of_range")?;
        let time = if ws {row["T"].as_i64()} else {row["time"].as_i64()}
            .context("database_trade_time_missing")?;
        ensure!(time > 0, "database_trade_time_invalid");
        let event = if ws {Some(row["E"].as_i64().context("database_event_time_missing")?)} else {None};
        let price = decimal(if ws {&row["p"]} else {&row["price"]})?;
        let quantity = decimal(if ws {&row["q"]} else {&row["qty"]})?;
        let maker = if ws {row["m"].as_bool()} else {row["isBuyerMaker"].as_bool()}
            .context("database_maker_missing")?;
        let previous: Option<(i64,Option<i64>,String,String,i64)> = tx.query_row(
            "SELECT trade_time_ms,event_time_ms,price,quantity,buyer_is_maker FROM binance_trades WHERE trade_id=?1",
            params![id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
        if let Some((t,e,p,q,m))=previous {
            ensure!(t==time && p==price && q==quantity && m==i64::from(maker)
                && (e.is_none() || event.is_none() || e==event), "database_trade_identity_conflict");
            if e.is_none() && event.is_some() {
                tx.execute("UPDATE binance_trades SET event_time_ms=?1,evidence_sha256=?2,
                  source_row_index=?3,source_epoch=?4 WHERE trade_id=?5",
                  params![event,sha,index as i64,epoch,id])?;
            }
        } else {
            tx.execute("INSERT INTO binance_trades VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![id,time,event,price,quantity,i64::from(maker),sha,index as i64,epoch])?;
        }
        last=Some((id,time));
    }
    if let Some((id,time))=last {
        tx.execute("INSERT INTO ingest_health(source,last_success_at,last_source_time_ms,last_source_sequence,last_error,failed_at)
          VALUES ('binance_spot_trade',datetime('now'),?1,?2,NULL,NULL)
          ON CONFLICT(source) DO UPDATE SET last_success_at=excluded.last_success_at,
          last_source_time_ms=CASE WHEN excluded.last_source_sequence > COALESCE(ingest_health.last_source_sequence,-1)
            THEN excluded.last_source_time_ms ELSE ingest_health.last_source_time_ms END,
          last_source_sequence=MAX(COALESCE(ingest_health.last_source_sequence,-1),excluded.last_source_sequence),
          last_error=NULL,failed_at=NULL",params![time,id])?;
    }
    Ok(rows.len())
}

pub fn ingest_binance(db: &mut Connection, raw: &[u8], epoch: &str,
    received_at: &str) -> Result<usize> {
    let tx=db.transaction()?;
    let count=ingest_binance_tx(&tx,raw,epoch,received_at)?;
    tx.commit()?;
    Ok(count)
}

pub fn ingest_binance_batch<R:AsRef<[u8]>>(db: &mut Connection, frames: &[(R,String,String)])
    -> Result<usize> {
    ensure!(!frames.is_empty() && frames.len()<=1024,"database_migration_batch_capacity");
    let tx=db.transaction()?;
    let mut count=0;
    for (raw,epoch,received_at) in frames {
        count+=ingest_binance_tx(&tx,raw.as_ref(),epoch,received_at)?;
    }
    tx.commit()?;
    Ok(count)
}

fn ingest_chainlink_tx(tx: &rusqlite::Transaction<'_>, raw: &[u8], epoch: &str,
    received_at: &str) -> Result<usize> {
    let frame: Value = serde_json::from_slice(raw)?;
    ensure!(frame["v"]==1, "database_chainlink_version_invalid");
    let seq = frame["seq"].as_u64().context("database_chainlink_sequence_missing")?;
    let seq=i64::try_from(seq).context("database_chainlink_sequence_out_of_range")?;
    let payload=&frame["payload"];
    ensure!(payload["symbol"]=="btcusd" && payload["source"]=="chainlink",
        "database_chainlink_identity_invalid");
    let kind=match frame["channel"].as_str() {
        Some("price.crypto.twap") if payload["window_seconds"]==60=>"twap60",
        Some("price.crypto") if payload.get("window_seconds").is_none()=>"ordinary",
        _=>anyhow::bail!("database_chainlink_price_kind_unknown")
    };
    let points=payload["data"].as_array().cloned().unwrap_or_else(||vec![payload.clone()]);
    ensure!(!points.is_empty() && points.len()<=600,"database_chainlink_frame_capacity");
    let sha=digest(raw);
    tx.execute("INSERT OR IGNORE INTO evidence VALUES (?1,?2,?3,?4,'authenticated_chainlink_ws')",
        params![sha,raw,received_at,epoch])?;
    let mut last=None;
    for (index,point) in points.iter().enumerate() {
        let time=point["timestamp"].as_i64().context("database_chainlink_time_missing")?;
        ensure!(time>0,"database_chainlink_time_invalid");
        let price=decimal(&point["full_accuracy_value"])?;
        let previous: Option<(i64,String,String)>=tx.query_row("SELECT source_time_ms,price,evidence_sha256 FROM chainlink_observations
          WHERE kind=?1 AND source_epoch=?2 AND source_sequence=?3 AND source_row_index=?4",
          params![kind,epoch,seq,index as i64],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        if let Some((old_time,existing,old_sha))=previous {
            ensure!(old_time==time && existing==price && old_sha==sha,
                "database_chainlink_identity_conflict")
        } else {
            tx.execute("INSERT INTO chainlink_observations VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![kind,epoch,seq,time,price,sha,index as i64])?;
        }
        last=Some(time);
    }
    if let Some(time)=last {
        tx.execute("INSERT INTO ingest_health(source,last_success_at,last_source_time_ms,last_source_sequence,last_error,failed_at)
          VALUES (?1,datetime('now'),?2,?3,NULL,NULL)
          ON CONFLICT(source) DO UPDATE SET last_success_at=excluded.last_success_at,
          last_source_time_ms=MAX(COALESCE(ingest_health.last_source_time_ms,0),excluded.last_source_time_ms),
          last_source_sequence=excluded.last_source_sequence,
          last_error=NULL,failed_at=NULL",params![format!("chainlink_{kind}"),time,seq])?;
    }
    Ok(points.len())
}

pub fn ingest_chainlink(db: &mut Connection, raw: &[u8], epoch: &str,
    received_at: &str) -> Result<usize> {
    let tx=db.transaction()?;
    let count=ingest_chainlink_tx(&tx,raw,epoch,received_at)?;
    tx.commit()?;
    Ok(count)
}

pub fn ingest_chainlink_batch<R:AsRef<[u8]>>(db: &mut Connection, frames: &[(R,String,String)])
    -> Result<usize> {
    ensure!(!frames.is_empty() && frames.len()<=1024,"database_migration_batch_capacity");
    let tx=db.transaction()?;
    let mut count=0;
    for (raw,epoch,received_at) in frames {
        count+=ingest_chainlink_tx(&tx,raw.as_ref(),epoch,received_at)?;
    }
    tx.commit()?;
    Ok(count)
}

pub fn mark_error(db:&Connection, source:&str, error:&str) {
    let _=db.execute("INSERT INTO ingest_health(source,last_error,failed_at) VALUES (?1,?2,datetime('now'))
      ON CONFLICT(source) DO UPDATE SET last_error=excluded.last_error,failed_at=excluded.failed_at",
      params![source,error.chars().take(256).collect::<String>()]);
}

pub fn health(db:&Connection, source:&str) -> Result<Value> {
    let row:Option<(Option<String>,Option<i64>,Option<i64>,Option<String>,Option<String>)>=db.query_row(
        "SELECT last_success_at,last_source_time_ms,last_source_sequence,last_error,failed_at FROM ingest_health WHERE source=?1",
        params![source],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
    Ok(match row {Some((at,t,seq,error,failed))=>json!({"last_success_at":at,"last_source_time_ms":t,
        "last_source_sequence":seq,"last_error":error,"failed_at":failed,
        "state":if failed.as_deref()>at.as_deref() {"failed"} else {"writing"}}),
        None=>json!({"state":"not_started"})})
}

pub struct Evidence { pub raw:Vec<u8>, pub epoch:String, pub received_at:Option<String> }
pub fn binance_evidence(db:&Connection,start_ms:i64,through:u64,max_rows:usize) -> Result<Vec<Evidence>> {
    let through=i64::try_from(through).context("history_trade_id_out_of_range")?;
    let first:Option<i64>=db.query_row("SELECT MIN(trade_id) FROM binance_trades
      WHERE trade_time_ms>=?1 AND trade_id<=?2",params![start_ms,through],|r|r.get(0))?;
    let predecessor:Option<i64>=db.query_row("SELECT MAX(trade_id) FROM binance_trades
      WHERE trade_time_ms<?1 AND trade_id<=?2",params![start_ms,through],|r|r.get(0))?;
    let Some(first)=first else {return Ok(Vec::new())};
    let low=predecessor.unwrap_or(first);
    let count:i64=db.query_row("SELECT COUNT(*) FROM binance_trades WHERE trade_id BETWEEN ?1 AND ?2",
        params![low,through],|r|r.get(0))?;
    ensure!(count>=0 && count as usize<=max_rows+1,"history_trade_capacity");
    let mut stmt=db.prepare("SELECT DISTINCT e.raw,t.source_epoch,e.received_at
      FROM binance_trades t JOIN evidence e ON e.sha256=t.evidence_sha256
      WHERE t.trade_id BETWEEN ?1 AND ?2 ORDER BY t.trade_id")?;
    let rows=stmt.query_map(params![low,through],|r|Ok((r.get::<_,Vec<u8>>(0)?,
        r.get::<_,Option<String>>(1)?.unwrap_or_default(),r.get::<_,Option<String>>(2)?)))?;
    let mut out=Vec::new();
    for row in rows {let (raw,epoch,received)=row?;
        out.push(Evidence{raw,epoch,received_at:received});
    }
    Ok(out)
}

pub fn chainlink_history(db:&Connection,kind:&str,start_ms:i64,end_ms:i64,limit:usize)->Result<Value> {
    ensure!(matches!(kind,"ordinary"|"twap60") && start_ms>0 && end_ms>start_ms && limit<=10000,
        "database_history_request_invalid");
    let mut stmt=db.prepare("SELECT c.source_epoch,c.source_sequence,c.source_time_ms,c.price,
      c.evidence_sha256,c.source_row_index,e.received_at FROM chainlink_observations c
      JOIN evidence e ON e.sha256=c.evidence_sha256 WHERE c.kind=?1 AND c.source_time_ms>=?2
      AND c.source_time_ms<?3 ORDER BY c.source_time_ms,c.source_epoch,c.source_sequence LIMIT ?4")?;
    let rows=stmt.query_map(params![kind,start_ms,end_ms,(limit+1) as i64],|r|Ok(json!({
      "source_epoch":r.get::<_,String>(0)?,"source_sequence":r.get::<_,i64>(1)?,
      "source_time_ms":r.get::<_,i64>(2)?,"price":r.get::<_,String>(3)?,
      "raw_sha256":r.get::<_,String>(4)?,"source_row_index":r.get::<_,i64>(5)?,
      "received_at":r.get::<_,Option<String>>(6)?})))?;
    let points=rows.collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(points.len()<=limit,"database_history_page_capacity");
    let database_health=health(db,&format!("chainlink_{kind}"))?;
    let code=if database_health["state"]=="not_started" {Some("source_not_collected")}
        else if points.is_empty() {Some("range_has_no_observations")}
        else {None};
    Ok(json!({"schema_version":"marketcow.chainlink-observation-history.v1","kind":kind,
      "source":"chainlink","start_time_ms":start_ms,"end_time_ms":end_ms,
      "points":points,"code":code,"database_health":database_health,
      "coverage_complete":false,"execution_readiness":false}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn trade_identity_and_distinct_chainlink_kinds() {
        let dir=tempfile::tempdir().unwrap();
        let mut db=open(&dir.path().join("observations.sqlite3")).unwrap();
        let raw=serde_json::to_vec(&json!({"e":"trade","s":"BTCUSDT","t":7,
            "T":1000,"E":1001,"p":"100.00","q":"0.02","m":true})).unwrap();
        assert_eq!(ingest_binance(&mut db,&raw,"epoch-1","receipt").unwrap(),1);
        assert_eq!(ingest_binance(&mut db,&raw,"epoch-2","receipt").unwrap(),1);
        let older=serde_json::to_vec(&json!({"e":"trade","s":"BTCUSDT","t":6,
            "T":900,"E":901,"p":"99.00","q":"0.0100","m":false})).unwrap();
        ingest_binance(&mut db,&older,"epoch-0","receipt").unwrap();
        assert_eq!(health(&db,"binance_spot_trade").unwrap()["last_source_sequence"],7);
        let changed=serde_json::to_vec(&json!({"e":"trade","s":"BTCUSDT","t":7,
            "T":1000,"E":1001,"p":"101","q":"0.02","m":true})).unwrap();
        assert!(ingest_binance(&mut db,&changed,"epoch-2","receipt").is_err());
        let twap=serde_json::to_vec(&json!({"v":1,"seq":1,"channel":"price.crypto.twap","payload":{"symbol":"btcusd",
            "source":"chainlink","window_seconds":60,"timestamp":1000,
            "full_accuracy_value":"100.01"}})).unwrap();
        let ordinary=serde_json::to_vec(&json!({"v":1,"seq":1,"channel":"price.crypto","payload":{"symbol":"btcusd",
            "source":"chainlink","timestamp":1000,
            "full_accuracy_value":"99.99"}})).unwrap();
        ingest_chainlink(&mut db,&twap,"epoch-1","receipt").unwrap();
        ingest_chainlink(&mut db,&ordinary,"epoch-1","receipt").unwrap();
        let a=chainlink_history(&db,"twap60",900,1100,10).unwrap();
        let b=chainlink_history(&db,"ordinary",900,1100,10).unwrap();
        assert_eq!(a["points"][0]["price"],"100.01");
        assert_eq!(b["points"][0]["price"],"99.99");
        let unavailable=chainlink_history(&db,"ordinary",1200,1300,10).unwrap();
        assert_eq!(unavailable["code"],"range_has_no_observations");
    }

    #[test]
    fn uncollected_chainlink_kind_is_explicit() {
        let dir=tempfile::tempdir().unwrap();
        let db=open(&dir.path().join("observations.sqlite3")).unwrap();
        let result=chainlink_history(&db,"ordinary",1000,2000,10).unwrap();
        assert_eq!(result["code"],"source_not_collected");
        assert_eq!(result["coverage_complete"],false);
    }

    #[test]
    fn chainlink_snapshot_points_share_sequence_but_keep_row_identity() {
        let dir=tempfile::tempdir().unwrap();
        let mut db=open(&dir.path().join("observations.sqlite3")).unwrap();
        let raw=serde_json::to_vec(&json!({"v":1,"seq":42,"channel":"price.crypto.twap",
            "snapshot":true,"payload":{"symbol":"btcusd","source":"chainlink",
            "window_seconds":60,"data":[
                {"timestamp":1000,"full_accuracy_value":"100.01"},
                {"timestamp":1100,"full_accuracy_value":"100.02"}]}})).unwrap();
        assert_eq!(ingest_chainlink(&mut db,&raw,"epoch-a","receipt").unwrap(),2);
        let result=chainlink_history(&db,"twap60",900,1200,10).unwrap();
        assert_eq!(result["points"].as_array().unwrap().len(),2);
        assert_eq!(result["points"][0]["source_sequence"],42);
        assert_eq!(result["points"][1]["source_sequence"],42);
        assert_eq!(result["points"][0]["source_row_index"],0);
        assert_eq!(result["points"][1]["source_row_index"],1);
    }

    #[test]
    fn migration_batch_conflict_rolls_back_every_row() {
        let dir=tempfile::tempdir().unwrap();
        let mut db=open(&dir.path().join("observations.sqlite3")).unwrap();
        let raw=|price:&str|serde_json::to_vec(&json!({"e":"trade","s":"BTCUSDT",
            "t":7,"T":1000,"E":1001,"p":price,"q":"0.02","m":true})).unwrap();
        let batch=vec![(raw("100"),"epoch-a".into(),"receipt".into()),
            (raw("101"),"epoch-b".into(),"receipt".into())];
        assert!(ingest_binance_batch(&mut db,&batch).unwrap_err().to_string()
            .contains("database_trade_identity_conflict"));
        let count:i64=db.query_row("SELECT count(*) FROM binance_trades",[],|r|r.get(0)).unwrap();
        assert_eq!(count,0);
        let good=vec![(raw("100"),"epoch-a".into(),"receipt".into())];
        assert_eq!(ingest_binance_batch(&mut db,&good).unwrap(),1);
        assert_eq!(ingest_binance_batch(&mut db,&good).unwrap(),1);
        let count:i64=db.query_row("SELECT count(*) FROM binance_trades",[],|r|r.get(0)).unwrap();
        assert_eq!(count,1);
    }
}
