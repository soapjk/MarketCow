//! Explicit one-shot archive -> query database import. Never called by a read API.
#[path = "btc_observation_db.rs"]
mod observation_db;
#[path="btc_typed.rs"]
mod btc_typed;
#[path="btc_typed_store.rs"]
mod btc_typed_store;
#[path="btc_typed_ingress.rs"]
mod btc_typed_ingress;

use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::Value;
#[cfg(test)]
use sha2::{Digest, Sha256};
use std::{io::{BufRead, BufReader}, path::{Path, PathBuf}};

fn audit(db:&rusqlite::Connection,source:&str)->Result<Value> {
    let sql=if source=="binance" {
        "SELECT trade_id,trade_time_ms,NULL FROM binance_trades ORDER BY trade_id"
    } else {
        "SELECT source_sequence,MAX(source_time_ms),source_epoch FROM chainlink_observations WHERE kind='twap60'
          GROUP BY source_epoch,source_sequence ORDER BY source_epoch,source_sequence"
    };
    let mut stmt=db.prepare(sql)?;
    let mut rows=stmt.query([])?;
    let mut count=0u64;let mut first=None;let mut last=None;let mut previous=None;
    let mut gaps=0u64;let mut regressions=0u64;let mut examples=Vec::new();
    while let Some(row)=rows.next()? {
        let id:i64=row.get(0)?;let time:i64=row.get(1)?;
        let epoch:Option<String>=row.get(2)?;
        if let Some((prev,prior_time,prior_epoch))=&previous {
            if prior_epoch==&epoch {
                if id!=prev+1 {gaps+=1;if examples.len()<16 {examples.push(serde_json::json!({"after":prev,"before":id,"epoch":epoch}));}}
                if time<*prior_time {regressions+=1;}
            }
        }
        first.get_or_insert(serde_json::json!({"id":id,"source_time_ms":time}));
        last=Some(serde_json::json!({"id":id,"source_time_ms":time}));
        previous=Some((id,time,epoch));count+=1;
    }
    let (earliest,latest):(Option<i64>,Option<i64>)=if source=="binance" {
        db.query_row("SELECT MIN(trade_time_ms),MAX(trade_time_ms) FROM binance_trades",[],
            |r|Ok((r.get(0)?,r.get(1)?)))?
    } else {
        db.query_row("SELECT MIN(source_time_ms),MAX(source_time_ms) FROM chainlink_observations
            WHERE kind='twap60'",[],|r|Ok((r.get(0)?,r.get(1)?)))?
    };
    Ok(serde_json::json!({"indexed_rows":count,"first_in_identity_order":first,
        "last_in_identity_order":last,"earliest_source_time_ms":earliest,
        "latest_source_time_ms":latest,
        "observed_internal_id_gaps":gaps,"supplier_time_regressions":regressions,
        "gap_examples":examples,"observed_span_contiguous":gaps==0&&regressions==0,
        "historical_coverage_complete":false}))
}

fn flush_pending(db:&mut rusqlite::Connection, source:&str,
    pending:&mut Vec<(Vec<u8>,String,String)>, pending_bytes:&mut usize,
    rows:&mut u64, batch_delay:std::time::Duration)->Result<()> {
    if !pending.is_empty() {
        let mut committed=None;
        for attempt in 0..4 {
            let result=if source=="binance" {observation_db::ingest_binance_batch(db,pending)}
                else {observation_db::ingest_chainlink_batch(db,pending)};
            match result {
                Ok(count)=>{committed=Some(count);break},
                Err(error) if attempt<3 && error.chain().any(|cause|
                    matches!(cause.downcast_ref::<rusqlite::Error>(),
                        Some(rusqlite::Error::SqliteFailure(inner,_)) if matches!(inner.code,
                            rusqlite::ErrorCode::DatabaseBusy|rusqlite::ErrorCode::DatabaseLocked)))=>{
                    std::thread::sleep(std::time::Duration::from_millis(100*(attempt+1)));
                }
                Err(error)=>return Err(error),
            }
        }
        *rows+=committed.context("database_migration_retries_exhausted")? as u64;
        pending.clear();
        *pending_bytes=0;
        if !batch_delay.is_zero() {std::thread::sleep(batch_delay);}
    }
    Ok(())
}

fn verify_live_overlap(db:&rusqlite::Connection,folder:&Path)->Result<i64> {
    let live=folder.join("trades.sqlite3");
    ensure!(live.is_file() && !live.is_symlink(),"live_trade_database_missing");
    db.execute("ATTACH DATABASE ?1 AS live",[live.to_string_lossy().as_ref()])?;
    // Drive the much smaller live index first. CROSS JOIN fixes the loop order
    // even when the offline historical shard has no ANALYZE statistics yet.
    let overlap:i64=db.query_row("SELECT COUNT(*) FROM live.binance_trades l
        CROSS JOIN binance_trades h ON h.trade_id=l.trade_id",[],|r|r.get(0))?;
    let conflicts:i64=db.query_row("SELECT COUNT(*) FROM live.binance_trades l
        CROSS JOIN binance_trades h ON h.trade_id=l.trade_id
        WHERE h.trade_time_ms<>l.trade_time_ms OR h.price<>l.price
          OR h.quantity<>l.quantity OR h.buyer_is_maker<>l.buyer_is_maker
          OR (h.event_time_ms IS NOT NULL AND l.event_time_ms IS NOT NULL
              AND h.event_time_ms<>l.event_time_ms)",[],|r|r.get(0))?;
    ensure!(conflicts==0,"historical_live_trade_identity_conflict");
    Ok(overlap)
}

fn finalize(root:&Path,staged:bool)->Result<()> {
    let folder=root.join("binance-trades");
    let path=folder.join(if staged {"trades-historical-staged.sqlite3"}
        else {"trades-historical.sqlite3"});
    ensure!(path.is_file() && !path.is_symlink(),"historical_database_missing");
    let db=observation_db::open(&path)?;
    let status:String=db.query_row("SELECT value FROM migration_meta WHERE key='import_complete'",
        [],|r|r.get(0))?;
    ensure!(status=="0","historical_database_not_staged");
    let overlap=verify_live_overlap(&db,&folder)?;
    ensure!(overlap>0,"historical_live_overlap_missing");
    db.execute("INSERT OR REPLACE INTO migration_meta VALUES ('import_complete','1')",[])?;
    println!("{}",serde_json::json!({"source":"binance","stage":"activated_after_live_overlap",
        "staged_file":staged,
        "verified_live_overlap_rows":overlap,"historical_coverage_complete":false}));
    Ok(())
}

fn import_with_options(root:&Path, source:&str, resume_after:Option<&str>,
    include_earlier:&[String],batch_delay:std::time::Duration,stage_only:bool) -> Result<()> {
    let (folder,prefix,database)=match source {
        "binance"=>("binance-trades","raw-","trades-historical.sqlite3"),
        "chainlink"=>("chainlink-twap","facts-","observations.sqlite3"),
        _=>anyhow::bail!("source must be binance or chainlink"),
    };
    let folder=root.join(folder);
    ensure!(folder.is_dir() && !folder.is_symlink(),"archive_directory_invalid");
    let mut files=std::fs::read_dir(&folder)?.map(|entry|entry.map(|e|e.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    files.retain(|path|path.file_name().and_then(|v|v.to_str())
        .is_some_and(|name|name.starts_with(prefix) && name.ends_with(".jsonl")));
    files.sort();
    let skipped_files=if let Some(name)=resume_after {
        ensure!(source=="binance" && name.starts_with("raw-") && name.ends_with(".jsonl")
            && Path::new(name).file_name().is_some_and(|part|part==name),
            "migration_resume_name_invalid");
        let index=files.iter().position(|path|path.file_name().is_some_and(|part|part==name))
            .context("migration_resume_file_not_found")?;
        let mut earlier=files.drain(..=index).collect::<Vec<_>>();
        let skipped=earlier.len().saturating_sub(include_earlier.len());
        for included in include_earlier {
            ensure!(included.starts_with(prefix) && included.ends_with(".jsonl")
                && Path::new(included).file_name().is_some_and(|part|part==included.as_str()),
                "migration_include_name_invalid");
            let selected=earlier.iter().position(|path|path.file_name()
                .is_some_and(|part|part==included.as_str())).context("migration_include_file_not_found")?;
            files.push(earlier.swap_remove(selected));
        }
        files.sort();
        skipped
    } else {0};
    ensure!(resume_after.is_some() || include_earlier.is_empty(),"migration_include_requires_resume");
    let mut db=observation_db::open(&folder.join(database))?;
    // This explicit, idempotent import can be replayed from immutable JSONL
    // after a power loss. WAL+NORMAL keeps the database consistent without an
    // fsync for every historical frame. The live ingestion worker has a
    // separate connection; its bounded queue keeps this scan off the WS loop.
    db.pragma_update(None,"synchronous","NORMAL")?;
    if source=="binance" {
        db.execute_batch("CREATE TABLE IF NOT EXISTS migration_meta(
            key TEXT PRIMARY KEY,value TEXT NOT NULL)")?;
        db.execute("INSERT OR REPLACE INTO migration_meta VALUES ('import_complete','0')",[])?;
    }
    let mut rows=0u64;
    let mut files_complete=0u64;
    for file in files {
        ensure!(file.is_file() && !file.is_symlink(),"archive_file_invalid");
        let mut reader=BufReader::new(std::fs::File::open(&file)?);
        let mut line=Vec::new();
        let mut line_number=0u64;
        let mut pending=Vec::new();
        let mut pending_bytes=0usize;
        loop {
            line.clear();
            let n=reader.read_until(b'\n',&mut line)?;
            if n==0 {break}
            line_number+=1;
            ensure!(n<=2*1024*1024,"archive_line_capacity");
            // The currently open segment can end in a partial append. It is
            // not imported or advertised as covered until a later explicit run.
            if !line.ends_with(b"\n") {break}
            let record:Value=serde_json::from_slice(&line)
                .with_context(||format!("invalid archive record {}:{line_number}",file.display()))?;
            let expected=if source=="binance" {"marketcow.binance-trade-raw.v1"}
                else {"marketcow.chainlink-twap-raw.v1"};
            ensure!(record["schema_version"]==expected,"archive_schema_invalid");
            let raw=STANDARD.decode(record["raw_base64"].as_str()
                .context("archive_raw_missing")?)?;
            let epoch=record[if source=="binance" {"source_epoch"}else{"epoch"}]
                .as_str().context("archive_epoch_missing")?.to_owned();
            let received=record["received_at"].as_str().unwrap_or("").to_owned();
            pending_bytes+=raw.len();
            pending.push((raw,epoch,received));
            // Binance writes an isolated historical shard. Chainlink still
            // shares its live DB, so keep those write locks much shorter.
            let (batch_rows,batch_bytes)=if source=="binance" {(512,4*1024*1024)}
                else {(32,256*1024)};
            if pending.len()>=batch_rows || pending_bytes>=batch_bytes {
                flush_pending(&mut db,source,&mut pending,&mut pending_bytes,&mut rows,batch_delay)
                    .with_context(||format!("archive import {}:{line_number}",file.display()))?;
            }
        }
        flush_pending(&mut db,source,&mut pending,&mut pending_bytes,&mut rows,batch_delay)
            .with_context(||format!("archive import {}:{line_number}",file.display()))?;
        files_complete+=1;
        eprintln!("imported source={source} files={files_complete} rows_seen={rows} latest_file={}",file.display());
    }
    let health=observation_db::health(&db,if source=="binance" {"binance_spot_trade"}else{"chainlink_twap60"})?;
    let coverage_audit=audit(&db,source)?;
    let mut overlap=0i64;
    if source=="binance" && !stage_only {
        overlap=verify_live_overlap(&db,&folder)?;
        db.execute("INSERT OR REPLACE INTO migration_meta VALUES ('import_complete','1')",[])?;
    }
    println!("{}",serde_json::json!({"source":source,"files_scanned":files_complete,
        "files_skipped_from_verified_prior_run":skipped_files,"resume_after":resume_after,
        "included_earlier_files":include_earlier,
        "stage_only":stage_only,
        "rows_seen":rows,"database_health":health,"coverage_audit":coverage_audit,
        "verified_live_overlap_rows":overlap,
        "coverage_complete":false,
        "note":"import success is not a continuity or history-completeness certificate"}));
    Ok(())
}

fn main() -> Result<()> {
    let mut args=std::env::args_os().skip(1);
    let root=PathBuf::from(args.next().context("usage: marketcow-btc-db-migrate ROOT binance|chainlink")?);
    let source=args.next().context("source missing")?;
    if source=="binance-typed" || source=="chainlink-typed" {
        let from=PathBuf::from(args.next().context("typed migration requires explicit existing source database")?);
        ensure!(args.next().is_none(),"unexpected typed migration option");
        return import_typed_database(&root,&from,source=="binance-typed");
    }
    if source=="binance-finalize" || source=="binance-finalize-staged" {
        ensure!(args.next().is_none(),"unexpected finalize option");
        return finalize(&root,source=="binance-finalize-staged");
    }
    let mut resume_after=None;
    let mut include_earlier=Vec::new();
    let mut batch_delay=std::time::Duration::ZERO;
    let mut stage_only=false;
    for option in args {
        let option=option.to_str().context("option not utf8")?;
        if let Some(name)=option.strip_prefix("--resume-after=") {
            ensure!(resume_after.replace(name.to_owned()).is_none(),"duplicate resume option");
        } else if let Some(name)=option.strip_prefix("--include-earlier=") {
            ensure!(!include_earlier.iter().any(|prior|prior==name),"duplicate include option");
            include_earlier.push(name.to_owned());
        } else if let Some(ms)=option.strip_prefix("--batch-delay-ms=") {
            let ms:u64=ms.parse()?;
            ensure!(ms<=1000,"migration_batch_delay_invalid");
            batch_delay=std::time::Duration::from_millis(ms);
        } else if option=="--stage-only" {
            ensure!(!stage_only,"duplicate stage-only option");
            stage_only=true;
        } else {anyhow::bail!("unexpected option {option}")}
    }
    import_with_options(&root,source.to_str().context("source not utf8")?,
        resume_after.as_deref(),&include_earlier,batch_delay,stage_only)
}

/// Explicit, restartable database migration. It is never called by a business
/// read or by WS ingestion. Old digest keys are opaque source row identities;
/// no digest is computed or checked, and no archive file is opened.
fn import_typed_database(root:&Path,from:&Path,binance:bool)->Result<()> {
    use marketcow_contracts::typed_v3::common::Source;
    use rusqlite::{OptionalExtension,params};
    ensure!(root.is_absolute()&&from.is_absolute()&&from.is_file()&&!from.is_symlink(),"migration_source_invalid");
    let source=rusqlite::Connection::open_with_flags(from,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    source.busy_timeout(std::time::Duration::from_secs(2))?;
    let destination=root.join(if binance{"binance-trades/trades.sqlite3"}else{"chainlink-twap/observations.sqlite3"});
    let mut target=btc_typed_store::open(&destination)?;
    target.execute_batch("CREATE TABLE IF NOT EXISTS typed_legacy_imports(
        source_database TEXT NOT NULL,source_evidence_key TEXT NOT NULL,
        PRIMARY KEY(source_database,source_evidence_key))")?;
    let source_identity=std::fs::canonicalize(from)?.to_string_lossy().into_owned();
    let mut after=String::new();let mut frames=0u64;let mut inserted=0usize;let mut skipped=0u64;
    loop {
        // A short read transaction and at most 8MiB owned payloads per batch.
        let mut stmt=source.prepare("SELECT sha256,length(raw) FROM evidence WHERE sha256>? ORDER BY sha256 LIMIT 64")?;
        let keys=stmt.query_map([&after],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);if keys.is_empty(){break;}
        let mut batch_bytes=0usize;
        for (key,size) in keys {
            ensure!(size>0 &&size<=1024*1024,"migration_evidence_capacity");
            if batch_bytes>0 &&batch_bytes+size as usize>8*1024*1024 {break;}
            batch_bytes+=size as usize;
            let done:Option<i64>=target.query_row("SELECT 1 FROM typed_legacy_imports WHERE source_database=? AND source_evidence_key=?",params![source_identity,key],|r|r.get(0)).optional()?;
            if done.is_some(){after=key;skipped+=1;continue;}
            let (raw,received,generation,acquisition):(Vec<u8>,Option<String>,Option<String>,String)=source.query_row("SELECT raw,received_at,source_epoch,acquisition FROM evidence WHERE sha256=?",[&key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
            let received=received.filter(|s|!s.is_empty()).map(|s|chrono::DateTime::parse_from_rfc3339(&s).map(|t|t.timestamp_nanos_opt())).transpose()?.flatten();
            // Missing legacy receipt stays absent; it never becomes source T.
            let input=btc_typed_store::EvidenceInput{raw:&raw,acquisition:&acquisition,
                upstream_url:if binance {if acquisition=="ws_trade"{"wss://data-stream.binance.vision/ws/btcusdt@trade"}else{"https://data-api.binance.vision/api/v3/trades?symbol=BTCUSDT&limit=1000"}}else{"wss://ws-live-v2.polymarket.com/ws"},
                received_at_ns:received,source_generation:generation.as_deref(),content_type:"application/json"};
            if binance {
                let mut rows=if acquisition=="ws_trade" {vec![btc_typed_ingress::binance_trade(&raw,received.unwrap_or(0))?]}
                    else if acquisition=="official_recent_trades"{btc_typed_ingress::binance_recent(&raw,received.unwrap_or(0))?}
                    else{anyhow::bail!("migration_acquisition_unsupported")};
                for row in &mut rows{row.received_at_ns=received;}
                inserted+=btc_typed_store::insert_trades(&mut target,&rows,&input)?;
            }else {
                ensure!(acquisition=="authenticated_chainlink_ws","migration_acquisition_unsupported");
                let mut parsed=btc_typed_ingress::chainlink(&raw,received.unwrap_or(0),Source::ChainlinkBtcUsdTwap60)?;
                for row in &mut parsed.rows{row.received_at_ns=received;}
                inserted+=btc_typed_store::insert_observations(&mut target,parsed.source,generation.as_deref().context("migration_generation_missing")?,&parsed.rows,&input)?;
            }
            // Crash between data commit and this checkpoint safely replays
            // semantic identity checks, rather than skipping an unwritten row.
            target.execute("INSERT OR IGNORE INTO typed_legacy_imports VALUES (?,?)",params![source_identity,key])?;
            after=key;frames+=1;
        }
        eprintln!("typed migration committed_frames={frames} inserted_rows={inserted} already_imported={skipped}");
    }
    println!("typed migration complete: frames={frames} inserted_rows={inserted} skipped={skipped}; coverage is not certified");
    Ok(())
}

#[cfg(test)]
fn import(root:&Path,source:&str)->Result<()> {
    import_with_options(root,source,None,&[],std::time::Duration::ZERO,false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn typed_database_migration_is_explicit_idempotent_and_preserves_lexical_fields() {
        let root=tempfile::tempdir().unwrap();let source=root.path().join("existing.sqlite3");
        let db=observation_db::open(&source).unwrap();
        let raw=br#"{"e":"trade","s":"BTCUSDT","t":3,"T":1000,"E":1001,"p":"100.00","q":"0.0100","m":false}"#;
        db.execute("INSERT INTO evidence VALUES (?,?,?,?,?)",rusqlite::params!["opaque-prior-row-id",raw.as_slice(),"2026-10-15T00:00:00Z","source-generation","ws_trade"]).unwrap();
        import_typed_database(root.path(),&source,true).unwrap();import_typed_database(root.path(),&source,true).unwrap();
        let target=btc_typed_store::open_reader(&root.path().join("binance-trades/trades.sqlite3")).unwrap();
        let row=btc_typed_store::read_trade(&target,3).unwrap().unwrap();assert_eq!(row.price,"100.00");assert_eq!(row.quantity,"0.0100");assert_eq!(row.event_time_ms,Some(1001));
        let evidence=btc_typed_store::read_evidence(&target,&row.evidence.unwrap().evidence_id).unwrap().unwrap();assert_eq!(evidence.raw_body,raw);
        assert_eq!(target.query_row("SELECT COUNT(*) FROM typed_evidence",[],|r|r.get::<_,i64>(0)).unwrap(),1);
    }

    #[test]
    fn explicit_import_is_idempotent_and_does_not_certify_coverage() {
        let root=tempfile::tempdir().unwrap();
        let folder=root.path().join("binance-trades");
        std::fs::create_dir(&folder).unwrap();
        let raw=serde_json::to_vec(&json!({"e":"trade","s":"BTCUSDT","t":7,
            "T":1000,"E":1001,"p":"100.00","q":"0.0200","m":false})).unwrap();
        let record=json!({"schema_version":"marketcow.binance-trade-raw.v1",
            "source_epoch":"epoch-1","received_at":"receipt",
            "raw_sha256":hex::encode(Sha256::digest(&raw)),"raw_base64":STANDARD.encode(raw)});
        std::fs::write(folder.join("raw-000.jsonl"),
            format!("{}\n",serde_json::to_string(&record).unwrap())).unwrap();
        observation_db::open(&folder.join("trades.sqlite3")).unwrap();
        import(root.path(),"binance").unwrap();
        import(root.path(),"binance").unwrap();
        let db=observation_db::open(&folder.join("trades-historical.sqlite3")).unwrap();
        let report=audit(&db,"binance").unwrap();
        assert_eq!(report["indexed_rows"],1);
        assert_eq!(report["historical_coverage_complete"],false);
        assert_eq!(db.query_row("SELECT value FROM migration_meta WHERE key='import_complete'",
            [],|r|r.get::<_,String>(0)).unwrap(),"1");
    }

    #[test]
    fn explicit_resume_skips_verified_prefix_and_imports_new_file() {
        let root=tempfile::tempdir().unwrap();
        let folder=root.path().join("binance-trades");
        std::fs::create_dir(&folder).unwrap();
        observation_db::open(&folder.join("trades.sqlite3")).unwrap();
        for id in [7,6,8] {
            let raw=serde_json::to_vec(&json!({"e":"trade","s":"BTCUSDT","t":id,
                "T":1000+id,"E":1001+id,"p":"100.00","q":"0.0200","m":false})).unwrap();
            let record=json!({"schema_version":"marketcow.binance-trade-raw.v1",
                "source_epoch":"epoch-1","received_at":"receipt",
                "raw_sha256":hex::encode(Sha256::digest(&raw)),"raw_base64":STANDARD.encode(raw)});
            std::fs::write(folder.join(format!("raw-{id}.jsonl")),
                format!("{}\n",serde_json::to_string(&record).unwrap())).unwrap();
            if id==7 {import(root.path(),"binance").unwrap();}
        }
        import_with_options(root.path(),"binance",Some("raw-7.jsonl"),
            &["raw-6.jsonl".to_owned()],
            std::time::Duration::from_millis(1),false).unwrap();
        let db=observation_db::open(&folder.join("trades-historical.sqlite3")).unwrap();
        assert_eq!(db.query_row("SELECT COUNT(*) FROM binance_trades",[],|r|r.get::<_,i64>(0)).unwrap(),3);
        assert!(import_with_options(root.path(),"binance",Some("../raw-7.jsonl"),&[],
            std::time::Duration::ZERO,false).unwrap_err().to_string().contains("migration_resume_name_invalid"));
    }

    #[test]
    fn offline_stage_is_not_queryable_until_live_overlap_finalization() {
        let root=tempfile::tempdir().unwrap();
        let folder=root.path().join("binance-trades");
        std::fs::create_dir(&folder).unwrap();
        let raw=serde_json::to_vec(&json!({"e":"trade","s":"BTCUSDT","t":7,
            "T":1000,"E":1001,"p":"100.00","q":"0.0200","m":false})).unwrap();
        let record=json!({"schema_version":"marketcow.binance-trade-raw.v1",
            "source_epoch":"epoch-1","received_at":"receipt",
            "raw_sha256":hex::encode(Sha256::digest(&raw)),"raw_base64":STANDARD.encode(&raw)});
        std::fs::write(folder.join("raw-7.jsonl"),
            format!("{}\n",serde_json::to_string(&record).unwrap())).unwrap();
        import_with_options(root.path(),"binance",None,&[],std::time::Duration::ZERO,true).unwrap();
        let db=observation_db::open(&folder.join("trades-historical.sqlite3")).unwrap();
        assert_eq!(db.query_row("SELECT value FROM migration_meta WHERE key='import_complete'",
            [],|r|r.get::<_,String>(0)).unwrap(),"0");
        let mut live=observation_db::open(&folder.join("trades.sqlite3")).unwrap();
        observation_db::ingest_binance(&mut live,&raw,"live","receipt").unwrap();
        drop(live);
        drop(db);
        std::fs::rename(folder.join("trades-historical.sqlite3"),
            folder.join("trades-historical-staged.sqlite3")).unwrap();
        finalize(root.path(),true).unwrap();
        assert!(!folder.join("trades-historical.sqlite3").exists());
        let db=observation_db::open(&folder.join("trades-historical-staged.sqlite3")).unwrap();
        assert_eq!(db.query_row("SELECT value FROM migration_meta WHERE key='import_complete'",
            [],|r|r.get::<_,String>(0)).unwrap(),"1");
    }
}
