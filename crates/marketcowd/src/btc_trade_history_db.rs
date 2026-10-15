//! Frozen metadata and bounded indexed pages; no per-request JSONL scan or full-range copy.
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path, os::unix::fs::MetadataExt};

#[derive(Clone)]
pub struct Page { first_id:i64, end_id:i64, rows:usize }
pub struct Session {
    pub created:std::time::Instant,
    pub id:String, pub start:i64, pub through:u64, pub limit:usize,
    pages:Vec<Page>, predecessor:Option<i64>, reason:Option<&'static str>,
    missing:Vec<Value>, first_time:Option<i64>, last_time:Option<i64>,
    count:usize, archive_health:Value, database_health:Value,
    database_version:i64,
    files:DatabaseFiles,
}
#[derive(PartialEq,Eq)]
struct DatabaseFiles { live:(u64,u64), historical:Option<(u64,u64,u64,i64,i64)>,
    historical_wal:Option<(u64,u64,u64,i64,i64)> }
fn database_files(root:&Path)->Result<DatabaseFiles> {
    let live=std::fs::metadata(root.join("trades.sqlite3"))?;
    let historical=match std::fs::metadata(root.join("trades-historical.sqlite3")) {
        Ok(m)=>Some((m.dev(),m.ino(),m.len(),m.mtime(),m.mtime_nsec())),
        Err(e) if e.kind()==std::io::ErrorKind::NotFound=>None,
        Err(e)=>return Err(e.into()),
    };
    let historical_wal=match std::fs::metadata(root.join("trades-historical.sqlite3-wal")) {
        Ok(m)=>Some((m.dev(),m.ino(),m.len(),m.mtime(),m.mtime_nsec())),
        Err(e) if e.kind()==std::io::ErrorKind::NotFound=>None,
        Err(e)=>return Err(e.into()),
    };
    Ok(DatabaseFiles {live:(live.dev(),live.ino()),historical,historical_wal})
}
impl Session {
    pub fn page_count(&self)->usize {self.pages.len()}
    pub fn reusable(&self)->bool {self.reason.is_none()}
}

#[derive(Clone)]
struct Row { id:i64, time:i64, event:Option<i64>, price:String, quantity:String,
    maker:bool, evidence:String, index:i64, origin:i64 }
impl Row {
    fn read(row:&rusqlite::Row<'_>)->rusqlite::Result<Self> {Ok(Self{
        id:row.get(0)?,time:row.get(1)?,event:row.get(2)?,price:row.get(3)?,
        quantity:row.get(4)?,maker:row.get::<_,i64>(5)?!=0,evidence:row.get(6)?,index:row.get(7)?,
        origin:row.get(8)?,
    })}
    fn typed(&self)->Value {json!({"trade_id":self.id,"trade_time_ms":self.time,
        "event_time_ms":self.event,"price":self.price,"quantity":self.quantity,
        "buyer_is_maker":self.maker,"evidence_ref":self.evidence,"source_row_index":self.index})}
}
fn database(root:&Path)->Result<Connection> {
    database_at(root,None)
}
fn database_at(root:&Path,version:Option<i64>)->Result<Connection> {
    let path=root.join("trades.sqlite3");
    ensure!(path.is_file() && !path.is_symlink(),"history_database_unavailable");
    let db=Connection::open_with_flags(path,OpenFlags::SQLITE_OPEN_READ_ONLY|OpenFlags::SQLITE_OPEN_URI)
        .context("history_database_unavailable")?;
    db.busy_timeout(std::time::Duration::from_secs(2))?;
    let version=version.unwrap_or(db.query_row("SELECT COALESCE(MAX(version),0)
        FROM binance_trade_versions",[],|r|r.get(0))?);
    // The session's watermark selects immutable row versions. No long read
    // transaction or per-request copy pins the live writer's WAL.
    db.execute_batch(&format!("CREATE TEMP VIEW frozen_live_trades AS
      SELECT trade_id,trade_time_ms,event_time_ms,price,quantity,buyer_is_maker,
        evidence_sha256,source_row_index,source_epoch FROM binance_trade_versions v
      WHERE v.version <= {version} AND NOT EXISTS(SELECT 1 FROM binance_trade_versions n
        WHERE n.trade_id=v.trade_id AND n.version>v.version AND n.version<={version})"))?;
    db.execute_batch(&format!("CREATE TEMP VIEW frozen_database_version AS SELECT {version} AS version"))?;
    let historical=root.join("trades-historical.sqlite3");
    let ready=if historical.is_file() && !historical.is_symlink() {
        let check=Connection::open_with_flags(&historical,OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        check.query_row("SELECT value FROM migration_meta WHERE key='import_complete'",
            [],|r|r.get::<_,String>(0)).optional()
            .context("history_historical_database_invalid")?.as_deref()==Some("1")
    } else {false};
    if ready {
        let uri=format!("file:{}?mode=ro",historical.display());
        db.execute("ATTACH DATABASE ?1 AS historical",[uri])?;
        db.execute_batch("CREATE TEMP VIEW all_binance_trades AS
          SELECT trade_id,trade_time_ms,event_time_ms,price,quantity,buyer_is_maker,
            evidence_sha256,source_row_index,0 AS origin FROM frozen_live_trades
          UNION ALL
          SELECT h.trade_id,h.trade_time_ms,h.event_time_ms,h.price,h.quantity,
            h.buyer_is_maker,h.evidence_sha256,h.source_row_index,1 AS origin
            FROM historical.binance_trades h WHERE NOT EXISTS
            (SELECT 1 FROM frozen_live_trades m WHERE m.trade_id=h.trade_id)")?;
        db.execute_batch("CREATE TEMP VIEW trade_identity_conflicts AS
          SELECT m.trade_id FROM frozen_live_trades m
          JOIN historical.binance_trades h USING(trade_id)
          WHERE m.trade_time_ms<>h.trade_time_ms OR m.price<>h.price
            OR m.quantity<>h.quantity OR m.buyer_is_maker<>h.buyer_is_maker
            OR (m.event_time_ms IS NOT NULL AND h.event_time_ms IS NOT NULL
                AND m.event_time_ms<>h.event_time_ms)")?;
    } else {
        db.execute_batch("CREATE TEMP VIEW all_binance_trades AS
          SELECT trade_id,trade_time_ms,event_time_ms,price,quantity,buyer_is_maker,
            evidence_sha256,source_row_index,0 AS origin FROM frozen_live_trades;
          CREATE TEMP VIEW trade_identity_conflicts AS SELECT 0 AS trade_id WHERE 0")?;
    }
    Ok(db)
}
fn row_by_id(db:&Connection,id:i64)->Result<Option<Row>> {
    Ok(db.query_row("SELECT trade_id,trade_time_ms,event_time_ms,price,quantity,
      buyer_is_maker,evidence_sha256,source_row_index,origin FROM all_binance_trades WHERE trade_id=?1",
      params![id],Row::read).optional()?)
}
pub fn freeze(root:&Path,id:String,start:i64,through:u64,limit:usize,archive_health:Value)->Result<Session> {
    let files=database_files(root)?;
    let db=database(root)?;
    let database_version=db.query_row("SELECT version FROM frozen_database_version",[],|r|r.get(0))?;
    let through_i=i64::try_from(through).context("history_trade_id_out_of_range")?;
    let predecessor:Option<i64>=db.query_row("SELECT trade_id FROM all_binance_trades
      WHERE trade_time_ms<?1 AND trade_id<=?2 ORDER BY trade_id DESC LIMIT 1",
      params![start,through_i],|r|r.get(0)).optional()?;
    // A supplier-time regression can put an in-window trade *before* the
    // selected boundary predecessor in ID order. Without this check the
    // range after that predecessor could look contiguous while silently
    // omitting earlier, valid in-window trades.
    let skipped_in_window:Option<i64>=if let Some(before)=predecessor {
        db.query_row("SELECT trade_id FROM all_binance_trades
          WHERE trade_time_ms>=?1 AND trade_id<=?2 LIMIT 1",
          params![start,before],|r|r.get(0)).optional()?
    } else {None};
    let conflict:Option<i64>=db.query_row("SELECT trade_id FROM trade_identity_conflicts
      WHERE trade_id>?1 AND trade_id<=?2 LIMIT 1",
      params![predecessor.unwrap_or(0),through_i],|r|r.get(0)).optional()?;
    ensure!(conflict.is_none(),"historical_live_trade_identity_conflict");
    let mut database_health=crate::btc_trade::observation_db::health(&db,"binance_spot_trade")?;
    let indexed_max:Option<i64>=db.query_row("SELECT trade_id FROM all_binance_trades
      ORDER BY trade_id DESC LIMIT 1",[],|r|r.get(0)).optional()?;
    database_health["maximum_indexed_trade_id"]=json!(indexed_max);
    let mut stmt=db.prepare("SELECT trade_id,trade_time_ms,event_time_ms,price,quantity,
      buyer_is_maker,evidence_sha256,source_row_index,origin FROM all_binance_trades
      WHERE trade_id>?1 AND trade_id<=?2 ORDER BY trade_id")?;
    let mut cursor=stmt.query(params![predecessor.unwrap_or(0),through_i])?;
    let mut pages=Vec::new();let mut first_id=0;let mut in_page=0usize;
    let mut count=0usize;let mut first_time=None;let mut last_time=None;
    let mut previous_id=predecessor;let mut previous_time=None;let mut missing=Vec::new();
    let mut reason=skipped_in_window.map(|_|"supplier_time_regression");
    let deadline=std::time::Instant::now()+super::BUILD_TIME;
    while let Some(raw)=cursor.next()? {
        ensure!(std::time::Instant::now()<deadline,"history_deadline");
        let row=Row::read(raw)?;
        ensure!(row.id>0 && row.time>0,"history_database_row_invalid");
        if row.time<start {anyhow::bail!("supplier_time_regression")}
        ensure!(previous_id.is_none_or(|before|row.id>before),"history_database_id_regression");
        if in_page==0 {first_id=row.id;}
        if let Some(before)=previous_id {
            if row.id!=before+1 {
                ensure!(missing.len()<1024,"history_gap_capacity");
                missing.push(json!({"from_trade_id":before+1,"through_trade_id":row.id-1}));
                reason=Some("trade_id_gap");
            }
        }
        if previous_time.is_some_and(|before|row.time<before) ||
           predecessor.is_some_and(|before|row.id<=before) {
            reason=Some("supplier_time_regression");
        }
        in_page+=1;count+=1;
        ensure!(count<=super::MAX_TRADES,"history_trade_capacity");
        first_time.get_or_insert(row.time);last_time=Some(row.time);
        previous_id=Some(row.id);previous_time=Some(row.time);
        if in_page==limit {
            pages.push(Page{first_id,end_id:row.id+1,rows:in_page});
            in_page=0;
        }
    }
    // No long-lived SQLite read transaction may span the expensive per-page
    // evidence validation below. Otherwise the live writer's WAL checkpoint
    // is pinned for the entire frozen-session build.
    drop(cursor);
    drop(stmt);
    drop(db);
    if in_page>0 {pages.push(Page{first_id,end_id:through_i.saturating_add(1),rows:in_page});}
    if pages.is_empty(){pages.push(Page{first_id:through_i,end_id:through_i.saturating_add(1),rows:0});reason=Some("range_has_no_trades");}
    if predecessor.is_none() {reason=Some("boundary_predecessor_missing");}
    if previous_id!=Some(through_i) {
        reason=Some(if database_health["maximum_indexed_trade_id"].as_u64().unwrap_or(0)<through
            {"database_write_lag"}else{"through_trade_missing"});
    }
    if last_time.is_some_and(|time|time-start>960000) {anyhow::bail!("history_span_capacity");}
    ensure!(pages.len()<=super::MAX_PAGES,"history_page_capacity");
    let session=Session{created:std::time::Instant::now(),id,start,through,limit,pages,predecessor,reason,missing,
        first_time,last_time,count,archive_health,database_health,database_version,files};
    // Count with the real serializer, but do not construct or Base64-encode
    // every response page just to discard it before the first GET.
    let mut bytes=0usize;
    for index in 0..session.pages.len() {
        ensure!(std::time::Instant::now()<deadline,"history_deadline");
        bytes=bytes.checked_add(page_budget(root,&session,index)?)
            .context("history_session_bytes")?;
        ensure!(bytes<=super::SESSION_BYTES,"history_session_bytes");
    }
    Ok(session)
}

struct EvidenceRecord { raw:Vec<u8>, received:Option<String>, epoch:Option<String>, acquisition:String }
fn evidence_wire(sha:&str, ev:&EvidenceRecord, raw_base64:String)->Value {
    json!({"evidence_ref":sha,"raw_sha256":sha,"raw_base64":raw_base64,
        "raw_bytes":ev.raw.len(),"source_epoch":ev.epoch,"received_at":ev.received,
        "acquisition":if ev.acquisition=="ws_trade" {"ws_archive"}else{"official_recent_trades"},
        "upstream_url":if ev.acquisition=="ws_trade" {crate::btc_trade::WS_URL}else{crate::btc_trade::REST_URL}})
}
fn evidence(db:&Connection,sha:&str,origin:i64)->Result<EvidenceRecord> {
    ensure!(matches!(origin,0|1),"history_database_origin_invalid");
    let table=if origin==0 {"main.evidence"}else{"historical.evidence"};
    let (raw,received,epoch,acquisition):(Vec<u8>,Option<String>,Option<String>,String)=db.query_row(
        &format!("SELECT raw,received_at,source_epoch,acquisition FROM {table} WHERE sha256=?1"),
        params![sha],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    ensure!(raw.len()<=super::LINE_BYTES,"history_page_bytes");
    Ok(EvidenceRecord {raw,received,epoch,acquisition})
}
pub fn page(root:&Path,session:&Session,index:usize)->Result<Value> {
    ensure!(database_files(root)?==session.files,"history_database_changed_during_session");
    let selected=session.pages.get(index).context("page_out_of_range")?;
    let db=database_at(root,Some(session.database_version))?;
    let conflict:Option<i64>=db.query_row("SELECT trade_id FROM trade_identity_conflicts
      WHERE trade_id>=?1 AND trade_id<?2 LIMIT 1",
      params![selected.first_id,selected.end_id],|r|r.get(0)).optional()?;
    ensure!(conflict.is_none(),"historical_live_trade_identity_conflict");
    let mut stmt=db.prepare("SELECT trade_id,trade_time_ms,event_time_ms,price,quantity,
      buyer_is_maker,evidence_sha256,source_row_index,origin FROM all_binance_trades
      WHERE trade_time_ms>=?1 AND trade_id>=?2 AND trade_id<?3 ORDER BY trade_id")?;
    let rows=stmt.query_map(params![session.start,selected.first_id,selected.end_id],Row::read)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(rows.len()==selected.rows,"history_database_changed_during_session");
    let predecessor=if index==0 {session.predecessor.map(|id|row_by_id(&db,id)).transpose()?.flatten()}else{None};
    if index==0 {
        let current:Option<i64>=db.query_row("SELECT trade_id FROM all_binance_trades
          WHERE trade_time_ms<?1 AND trade_id<=?2 ORDER BY trade_id DESC LIMIT 1",
          params![session.start,i64::try_from(session.through)?],|r|r.get(0)).optional()?;
        ensure!(current==session.predecessor && predecessor.as_ref().map(|r|r.id)==current,
            "history_database_changed_during_session");
    }
    let mut refs=BTreeMap::new();
    let mut originals=BTreeMap::<String,Vec<Value>>::new();
    let mut supplier_trades=Vec::with_capacity(rows.len());
    let mut supplier_predecessor=None;
    for row in rows.iter().chain(predecessor.iter()) {
        if !refs.contains_key(&row.evidence) {
            let ev=evidence(&db,&row.evidence,row.origin)?;
            let source=super::raw_rows(&ev.raw)?;
            let wire=evidence_wire(&row.evidence,&ev,STANDARD.encode(&ev.raw));
            originals.insert(row.evidence.clone(),source);
            refs.insert(row.evidence.clone(),wire);
        }
        let source=originals.get(&row.evidence).unwrap();
        let original=source.get(row.index as usize).context("database_original_row_missing")?;
        let typed=super::point(original,row.index as usize,&row.evidence)?
            .context("database_original_row_invalid")?;
        ensure!(super::same_identity(&typed,&row.typed()) && typed["trade_id"]==row.id,
            "database_original_row_conflict");
        // SQLite indexes canonical decimal values, but the history wire retains
        // exact supplier lexemes from the original row.
        if supplier_trades.len()<rows.len() {supplier_trades.push(typed);}
        else {supplier_predecessor=Some(typed);}
    }
    let result=page_document(session,index,supplier_predecessor,supplier_trades,refs.into_values().collect());
    ensure!(crate::source_snapshot::json_bytes(&result)?<=super::PAGE_BYTES,"history_page_bytes");
    Ok(result)
}
fn page_document(session:&Session,index:usize,predecessor:Option<Value>,trades:Vec<Value>,evidence:Vec<Value>)->Value {
    let final_page=index+1==session.pages.len();
    json!({"schema_version":super::SCHEMA,"source":"binance_spot_trade",
      "instrument_id":"BTCUSDT.BINANCE","session_id":session.id,
      "start_time_ms":session.start,"through_trade_id":session.through,"limit":session.limit,
      "page_index":index,"page_count":session.pages.len(),"session_ttl_seconds":super::TTL.as_secs(),
      "predecessor":predecessor,
      "trades":trades,
      "evidence":evidence,
      "next_page_token":if final_page {None}else{Some(format!("{}:{}",session.id,index+1))},
      "complete":final_page&&session.reason.is_none(),"coverage_verified":session.reason.is_none(),
      "code":session.reason,"missing_ranges":session.missing,
      "coverage_first_T":session.first_time,"coverage_last_T":session.last_time,
      "total_trade_count":session.count,"time_unit":"integer_unix_milliseconds",
      "fallback_used":false,"execution_readiness":false,
      "database_health_at_freeze":session.database_health,
      "archive_health_at_freeze":session.archive_health,
      "budgets":{"page_bytes":super::PAGE_BYTES,"session_bytes":super::SESSION_BYTES,
        "max_trades":super::MAX_TRADES,"max_pages":super::MAX_PAGES,"maximum_span_ms":960000}})
}

fn page_budget(root:&Path,session:&Session,index:usize)->Result<usize> {
    ensure!(database_files(root)?==session.files,"history_database_changed_during_session");
    let selected=&session.pages[index];
    let db=database_at(root,Some(session.database_version))?;
    let mut statement=db.prepare("SELECT trade_id,trade_time_ms,event_time_ms,price,quantity,
      buyer_is_maker,evidence_sha256,source_row_index,origin FROM all_binance_trades
      WHERE trade_time_ms>=?1 AND trade_id>=?2 AND trade_id<?3 ORDER BY trade_id")?;
    let rows=statement.query_map(params![session.start,selected.first_id,selected.end_id],Row::read)?;
    let predecessor=if index==0 {session.predecessor.map(|id|row_by_id(&db,id)).transpose()?.flatten()}else{None};
    let mut parsed=BTreeMap::new();
    let mut total=crate::source_snapshot::json_bytes(&page_document(session,index,None,vec![],vec![]))?;
    let mut trade_count=0usize;
    for row in rows.chain(predecessor.into_iter().map(Ok)) {
        let row=row?;
        if !parsed.contains_key(&row.evidence) {
            let ev=evidence(&db,&row.evidence,row.origin)?;
            let encoded_len=ev.raw.len().checked_add(2).context("history_session_bytes")?/3*4;
            let overhead=crate::source_snapshot::json_bytes(&evidence_wire(&row.evidence,&ev,String::new()))?;
            total=total.checked_add(overhead+encoded_len+usize::from(!parsed.is_empty()))
                .context("history_session_bytes")?;
            ensure!(total<=super::PAGE_BYTES,"history_page_bytes");
            parsed.insert(row.evidence.clone(),super::raw_rows(&ev.raw)?);
        }
        let original=parsed[&row.evidence].get(row.index as usize).context("database_original_row_missing")?;
        let typed=super::point(original,row.index as usize,&row.evidence)?.context("database_original_row_invalid")?;
        ensure!(super::same_identity(&typed,&row.typed()),"database_original_row_conflict");
        let size=crate::source_snapshot::json_bytes(&typed)?;
        let addition=if trade_count<selected.rows {size+usize::from(trade_count>0)} else {size-4};
        total=total.checked_add(addition).context("history_session_bytes")?;
        ensure!(total<=super::PAGE_BYTES,"history_page_bytes");
        trade_count+=1;
    }
    ensure!(total<=super::PAGE_BYTES,"history_page_bytes");
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn indexed_pages_verify_originals_without_archive_and_detect_changes() {
        let root=tempfile::tempdir().unwrap();
        let mut db=crate::btc_trade::observation_db::open(&root.path().join("trades.sqlite3")).unwrap();
        for (id,t) in [(1,900),(2,1000),(3,1100)] {
            let raw=serde_json::to_vec(&json!({"e":"trade","s":"BTCUSDT","t":id,
                "T":t,"E":t+1,"p":"100.00","q":"0.0100","m":false})).unwrap();
            crate::btc_trade::observation_db::ingest_binance(&mut db,&raw,"epoch-a","receipt").unwrap();
        }
        db.execute("UPDATE ingest_health SET last_success_at='2026-10-15T00:00:00Z' \
            WHERE source='binance_spot_trade'",[]).unwrap();
        let frozen=freeze(root.path(),"session-a".into(),1000,3,1,Value::Null).unwrap();
        let first=page(root.path(),&frozen,0).unwrap();
        let second=page(root.path(),&frozen,1).unwrap();
        assert_eq!(page_budget(root.path(),&frozen,0).unwrap(),serde_json::to_vec(&first).unwrap().len());
        assert_eq!(page_budget(root.path(),&frozen,1).unwrap(),serde_json::to_vec(&second).unwrap().len());
        let vector=json!({"first":first,"second":second});
        let vector_sha256=hex::encode(Sha256::digest(serde_json::to_vec(&vector).unwrap()));
        let fixed:Value=serde_json::from_str(include_str!("../../../tests/fixtures/btc-trade-history-db-lexical-v1.json")).unwrap();
        assert_eq!(vector,fixed,"history serializer differs from fixed cross-language vector");
        if std::env::var_os("MARKETCOW_PRINT_HISTORY_VECTOR").is_some() {
            println!("HISTORY_DB_VECTOR_SHA256={vector_sha256}");
            println!("HISTORY_DB_VECTOR={vector}");
        }
        assert_eq!(vector_sha256,"be2a6a5d2a1a9f8b0ee7e741d9ce7cb019aaf5fca1dee98b8c921c910f28b604");
        assert_eq!(first["predecessor"]["trade_id"],1);
        assert_eq!(first["predecessor"]["price"],"100.00");
        assert_eq!(first["predecessor"]["quantity"],"0.0100");
        assert_eq!(first["trades"][0]["trade_id"],2);
        assert_eq!(first["trades"][0]["price"],"100.00");
        assert_eq!(first["trades"][0]["quantity"],"0.0100");
        assert_eq!(first["complete"],false);
        assert_eq!(second["complete"],true);
        assert!(first["evidence"].as_array().unwrap().iter().any(|ev|
            ev["raw_sha256"]==first["trades"][0]["evidence_ref"]));
        db.execute("UPDATE binance_trades SET price='101' WHERE trade_id=3",[]).unwrap();
        assert_eq!(page(root.path(),&frozen,1).unwrap(),second);
        assert!(freeze(root.path(),"new-version".into(),1000,3,1,Value::Null)
            .err().unwrap().to_string().contains("database_original_row_conflict"));
        db.execute("UPDATE binance_trades SET price='102' WHERE trade_id=1",[]).unwrap();
        assert_eq!(page(root.path(),&frozen,0).unwrap(),first);
    }

    #[test]
    fn completed_historical_shard_joins_live_without_archive_read() {
        let root=tempfile::tempdir().unwrap();
        let mut live=crate::btc_trade::observation_db::open(&root.path().join("trades.sqlite3")).unwrap();
        let mut historical=crate::btc_trade::observation_db::open(
            &root.path().join("trades-historical.sqlite3")).unwrap();
        for (id,time,target) in [(1,900,0),(2,1000,0),(2,1000,1),(3,1100,1)] {
            let raw=serde_json::to_vec(&json!({"e":"trade","s":"BTCUSDT","t":id,
                "T":time,"E":time+1,"p":"100.00","q":"0.0100","m":false})).unwrap();
            if target==0 {crate::btc_trade::observation_db::ingest_binance(
                &mut historical,&raw,"historical-epoch","receipt").unwrap()}
            else {crate::btc_trade::observation_db::ingest_binance(
                &mut live,&raw,"live-epoch","receipt").unwrap()};
        }
        historical.execute_batch("CREATE TABLE migration_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            INSERT INTO migration_meta VALUES ('import_complete','1')").unwrap();
        let frozen=freeze(root.path(),"joined-a".into(),1000,3,1,Value::Null).unwrap();
        assert_eq!(frozen.page_count(),2);
        let first=page(root.path(),&frozen,0).unwrap();
        let final_page=page(root.path(),&frozen,1).unwrap();
        assert_eq!(first["predecessor"]["trade_id"],1);
        assert_eq!(first["trades"][0]["trade_id"],2);
        assert_eq!(final_page["trades"][0]["trade_id"],3);
        assert_eq!(final_page["complete"],true);
        live.execute("UPDATE binance_trades SET price='101' WHERE trade_id=2",[]).unwrap();
        assert_eq!(page(root.path(),&frozen,0).unwrap(),first);
        assert!(freeze(root.path(),"new-version".into(),1000,3,1,Value::Null)
            .err().unwrap().to_string().contains("historical_live_trade_identity_conflict"));
        live.execute("UPDATE binance_trades SET price='100' WHERE trade_id=2",[]).unwrap();
        historical.execute("UPDATE binance_trades SET price='101' WHERE trade_id=1",[]).unwrap();
        assert!(page(root.path(),&frozen,0).unwrap_err().to_string()
            .contains("history_database_changed_during_session"));
    }

    #[test]
    fn supplier_time_regression_before_predecessor_cannot_certify_coverage() {
        let root=tempfile::tempdir().unwrap();
        let mut db=crate::btc_trade::observation_db::open(&root.path().join("trades.sqlite3")).unwrap();
        for (id,time) in [(1,1000),(2,1100),(3,900),(4,1200)] {
            let raw=serde_json::to_vec(&json!({"e":"trade","s":"BTCUSDT","t":id,
                "T":time,"E":time+1,"p":"100.00","q":"0.0100","m":false})).unwrap();
            crate::btc_trade::observation_db::ingest_binance(&mut db,&raw,"epoch","receipt").unwrap();
        }
        let frozen=freeze(root.path(),"regression".into(),1000,4,100,Value::Null).unwrap();
        let page=page(root.path(),&frozen,0).unwrap();
        assert_eq!(page["predecessor"]["trade_id"],3);
        assert_eq!(page["code"],"supplier_time_regression");
        assert_eq!(page["coverage_verified"],false);
        assert_eq!(page["complete"],false);
    }
}
