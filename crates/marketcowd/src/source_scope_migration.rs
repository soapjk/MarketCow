//! Offline copy-only v1 -> v2 hot/cold migration. A live source is rejected by
//! its writer leases; input files and accounts are never changed or removed.
use anyhow::{Context, Result, ensure};
use marketcow_polymarket::discovery_source::canonical_hash;
use marketcow_runtime::discovery_source::PreparedSourceWriter;
use rusqlite::{Connection, OpenFlags};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fs::{self, File, OpenOptions}, io::{Read, Write},
    os::{fd::AsRawFd, unix::fs::{OpenOptionsExt, PermissionsExt}}, path::Path};

pub struct Inputs<'a> {
    pub source_root: &'a Path,
    pub state_path: &'a Path,
    pub output_root: &'a Path,
    pub state_max_bytes: usize,
    pub batch_bytes: usize,
    pub expected_state_sha256: &'a str,
    pub expected_index_sha256: &'a str,
    pub expected_events_sha256: Option<&'a str>,
    pub copy_budget_bytes: u64,
}

fn sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut chunk = [0_u8; 1024 * 1024];
    loop {
        let n = file.read(&mut chunk)?;
        if n == 0 { break; }
        digest.update(&chunk[..n]);
    }
    Ok(hex::encode(digest.finalize()))
}

struct Lease(File);
impl Drop for Lease {
    fn drop(&mut self) {
        unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN); }
    }
}
fn quiesced_lease(path: &Path) -> Result<Lease> {
    let info = fs::symlink_metadata(path)?;
    ensure!(info.is_file() && info.permissions().mode() & 0o077 == 0,
        "offline migration writer lease file is not private and regular");
    let file = OpenOptions::new().read(true).open(path)?;
    ensure!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
        "offline migration source writer is active");
    Ok(Lease(file))
}

pub fn migrate(input: Inputs<'_>) -> Result<Value> {
    let source = input.source_root.canonicalize()?;
    let parent = input.output_root.parent().context("offline output parent")?.canonicalize()?;
    ensure!(source.is_absolute() && input.state_path.is_absolute()
        && input.output_root.is_absolute() && !parent.starts_with(&source)
        && !input.output_root.exists(), "offline migration requires a new separate output root");
    ensure!(input.copy_budget_bytes > 0 && input.state_max_bytes >= 1024,
        "offline migration explicit byte budgets");
    for hash in [Some(input.expected_state_sha256),Some(input.expected_index_sha256),
        input.expected_events_sha256].into_iter().flatten() {
        ensure!(hash.len() == 64 && hash.bytes().all(|c| c.is_ascii_hexdigit()),
            "offline migration expected SHA256");
    }
    let _collector = quiesced_lease(&source.join(".collector.lock"))?;
    let _publication = quiesced_lease(&source.join(".publication.lock"))?;
    let index = source.join("indexes/latest-state.sqlite3");
    let events = source.join("events.jsonl");
    let manifest = source.join("state-index.json");
    let catalog = source.join("catalog.json");
    let wal = source.join("indexes/latest-state.sqlite3-wal");
    ensure!(!wal.exists() || fs::metadata(&wal)?.len() == 0,
        "offline migration requires checkpointed SQLite snapshot without WAL");
    let bounded_history: bool = {
        let db = Connection::open_with_flags(&index, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.query_row("SELECT count(*) FROM metadata WHERE key='bounded_history_bytes'",
            [], |row| row.get::<_, i64>(0))? == 1
    };
    ensure!(bounded_history == input.expected_events_sha256.is_none(),
        "offline history mode and expected JSONL hash disagree");
    ensure!(bounded_history || events.is_file(),
        "offline unbounded history JSONL missing");
    let mut files = vec![input.state_path, index.as_path(), manifest.as_path(), catalog.as_path()];
    if !bounded_history { files.push(events.as_path()); }
    let total = files.iter().try_fold(0_u64, |sum, path| {
        sum.checked_add(fs::metadata(path)?.len()).context("offline input byte overflow")
    })?;
    ensure!(total <= input.copy_budget_bytes, "offline migration copy byte budget exceeded");
    ensure!(sha256(input.state_path)? == input.expected_state_sha256
        && sha256(&index)? == input.expected_index_sha256
        && (bounded_history || sha256(&events)? == input.expected_events_sha256.unwrap()),
        "offline migration input hash mismatch");
    let state = super::source_scope_journal::read(input.state_path, input.state_max_bytes)?
        .context("offline scope state absent")?;
    let input_manifest_sha = sha256(&manifest)?;
    ensure!(sha256(&catalog)? == state.catalog_manifest_sha256,
        "offline catalog manifest differs from scope state");
    let planned = super::source_scope_journal::plan_offline_cold_migration(
        &state, &BTreeSet::new())?;
    ensure!(!planned.cold_records.is_empty(),
        "offline migration has no cold records; no source rewrite is needed");
    // This is a copy, not a delete or an in-place SQLite migration. A failed
    // operation leaves an incomplete output for inspection and never switches
    // the running unit to it.
    fs::create_dir(input.output_root)?;
    fs::set_permissions(input.output_root, fs::Permissions::from_mode(0o700))?;
    fs::create_dir(input.output_root.join("indexes"))?;
    fs::set_permissions(input.output_root.join("indexes"), fs::Permissions::from_mode(0o700))?;
    let mut copies = vec![
        (&index, input.output_root.join("indexes/latest-state.sqlite3")),
        (&manifest, input.output_root.join("state-index.json")),
        (&catalog, input.output_root.join("catalog.json")),
    ];
    if !bounded_history { copies.push((&events, input.output_root.join("events.jsonl"))); }
    for (from, to) in copies {
        fs::copy(from, &to)?;
        ensure!(sha256(from)? == sha256(&to)?, "offline copied file hash mismatch");
    }
    ensure!(sha256(input.state_path)? == input.expected_state_sha256
        && sha256(&index)? == input.expected_index_sha256
        && (bounded_history || sha256(&events)? == input.expected_events_sha256.unwrap())
        && sha256(&manifest)? == input_manifest_sha
        && sha256(&catalog)? == state.catalog_manifest_sha256,
        "offline input changed during snapshot copy");
    let mut writer = PreparedSourceWriter::open(input.output_root, input.batch_bytes)?;
    let cursor = writer.cursor()?;
    let cold_ids = planned.cold_records.keys().cloned().collect::<BTreeSet<_>>();
    if !cold_ids.is_empty() {
        writer.retire_catalog_markets(cursor, &cold_ids, &planned.cold_records)?;
    }
    for (id, record) in &planned.cold_records {
        ensure!(writer.cold_market_record(id)?.as_ref().is_some_and(|saved|
            canonical_hash(saved) == canonical_hash(record)),
            "offline cold evidence verification failed");
    }
    drop(writer);
    // The writer uses WAL. A receipt for the main SQLite file is not a
    // complete snapshot until every committed cold row is checkpointed.
    let output_index = input.output_root.join("indexes/latest-state.sqlite3");
    let checkpoint = Connection::open_with_flags(&output_index,
        OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    let (busy, _, _): (i64, i64, i64) = checkpoint.query_row(
        "PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    ensure!(busy == 0, "offline output WAL checkpoint was busy");
    drop(checkpoint);
    let output_wal = input.output_root.join("indexes/latest-state.sqlite3-wal");
    ensure!(!output_wal.exists() || fs::metadata(&output_wal)?.len() == 0,
        "offline output WAL still contains unreported data");
    let mut hot = planned.hot_state;
    hot.schema_version = "marketcow.runtime-scope-state.v2".into();
    let output_state = input.output_root.join("runtime-scope-state.json");
    let _journal = super::source_scope_journal::Journal::open(
        output_state.clone(), input.state_max_bytes, hot.clone())?;
    let output_state_read = super::source_scope_journal::read(&output_state, input.state_max_bytes)?
        .context("offline output state absent")?;
    ensure!(canonical_hash(&serde_json::to_value(output_state_read)?)
        == canonical_hash(&serde_json::to_value(&hot)?),
        "offline output state differs");
    let report = json!({
        "schema_version":"marketcow.runtime-cold-migration.v1",
        "status":"complete_copy_only","source_cursor":cursor,
        "source_state_sha256":input.expected_state_sha256,
        "source_index_sha256":input.expected_index_sha256,
        "history_storage":if bounded_history {"bounded_sqlite"} else {"jsonl"},
        "source_events_sha256":input.expected_events_sha256,
        "source_catalog_sha256":state.catalog_manifest_sha256,
        "output_state_sha256":sha256(&output_state)?,
        "output_index_sha256":sha256(&output_index)?,
        "output_events_sha256":if bounded_history {None} else {
            Some(sha256(&input.output_root.join("events.jsonl"))?)},
        "hot_market_count":hot.acquisition_market_ids.len(),
        "hot_token_count":hot.acquisition_market_ids.len()*2,
        "cold_market_count":cold_ids.len(),
        "cold_record_map_sha256":canonical_hash(&json!(planned.cold_records)),
        "pending_retirement_ticket":hot.retirement_ticket,
        "leases_restored":false,"accounts_modified":false,"service_switched":false,
        "external_finality_root_copied":false,
    });
    let output_report = input.output_root.join("migration-report.json");
    let mut file = OpenOptions::new().create_new(true).write(true).mode(0o600).open(&output_report)?;
    file.write_all(&serde_json::to_vec(&report)?)?;
    file.sync_all()?;
    File::open(input.output_root)?.sync_all()?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn offline_copy_retains_original_and_cools_only_unreferenced_market() {
        let workspace = tempfile::tempdir().unwrap();
        let source = workspace.path().join("source");
        fs::create_dir(&source).unwrap();
        let source = source.canonicalize().unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(source.join("indexes")).unwrap();
        fs::write(source.join("events.jsonl"), b"").unwrap();
        fs::write(source.join("state-index.json"), b"{}").unwrap();
        fs::write(source.join("catalog.json"), b"{}").unwrap();
        for name in [".collector.lock", ".publication.lock"] {
            let _ = OpenOptions::new().create_new(true).write(true).mode(0o600)
                .open(source.join(name)).unwrap();
        }
        let db = Connection::open(source.join("indexes/latest-state.sqlite3")).unwrap();
        db.execute_batch("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);\
            CREATE TABLE gaps(resolved INTEGER NOT NULL);\
            CREATE TABLE books(token_id TEXT PRIMARY KEY,market_id TEXT,cursor INTEGER,\
                payload_json BLOB,payload_sha256 TEXT);\
            CREATE TABLE book_confirmations(token_id TEXT PRIMARY KEY);\
            CREATE TABLE event_offsets(cursor INTEGER PRIMARY KEY,byte_offset INTEGER,\
                byte_length INTEGER,event_id TEXT,market_id TEXT,token_id TEXT,line_sha256 TEXT);").unwrap();
        for (key, value) in [
            ("schema_version","marketcow.polymarket.state-index.v1"),
            ("unresolved_gap_count","0"),("event_log_size","0"),
            ("latest_cursor","0"),("catalog_revision","c"),
            ("active_recovery_id","r"),("book_token_count","0"),
            ("book_complete_market_count","0"),("oldest_book_received_at",""),
        ] {
            db.execute("INSERT INTO metadata VALUES (?,?)", [key,value]).unwrap();
        }
        drop(db);
        let record = |id: &str| json!({"identity":{"market_id":id,
            "condition_id":format!("condition-{id}"),"outcomes":[
                {"token_id":format!("{id}-up")},{"token_id":format!("{id}-down")}]}});
        let records: BTreeMap<String, Value> = (1..=2048)
            .map(|n| (n.to_string(),record(&n.to_string()))).collect();
        let state = super::super::source_scope_journal::State {
            schema_version:"marketcow.runtime-scope-state.v1".into(),
            catalog_revision:"c".into(), catalog_manifest_sha256:sha256(&source.join("catalog.json")).unwrap(),
            pool:"live".into(), active_config:json!({"catalog_revision":"c",
                "configured_markets":[{"market_id":"1"}]}), active_revision:1,
            catalog_source:Value::Null,
            acquisition_market_ids:records.keys().cloned().collect(),records,
            retiring_records:Default::default(),retirement_ticket:None,
            retiring_acquisition_market_ids:None,
        };
        let state_path = source.join("runtime-scope-state.json");
        let _ = super::super::source_scope_journal::Journal::open(
            state_path.clone(),2 * 1024 * 1024,state).unwrap();
        let state_hash = sha256(&state_path).unwrap();
        let index_hash = sha256(&source.join("indexes/latest-state.sqlite3")).unwrap();
        let events_hash = sha256(&source.join("events.jsonl")).unwrap();
        let output = workspace.path().canonicalize().unwrap().join("output");
        let receipt = migrate(Inputs {source_root:&source,state_path:&state_path,
            output_root:&output,state_max_bytes:2 * 1024 * 1024,batch_bytes:16384,
            expected_state_sha256:&state_hash,expected_index_sha256:&index_hash,
            expected_events_sha256:Some(&events_hash),copy_budget_bytes:2_000_000}).unwrap();
        assert_eq!(receipt["hot_market_count"],1);
        assert_eq!(receipt["cold_market_count"],2047);
        assert_eq!(sha256(&state_path).unwrap(),state_hash);
        assert_eq!(receipt["output_index_sha256"],
            sha256(&output.join("indexes/latest-state.sqlite3")).unwrap());
        assert!(!output.join("indexes/latest-state.sqlite3-wal").exists()
            || fs::metadata(output.join("indexes/latest-state.sqlite3-wal")).unwrap().len() == 0);
        let migrated = super::super::source_scope_journal::read(
            &output.join("runtime-scope-state.json"),2 * 1024 * 1024).unwrap().unwrap();
        assert_eq!(migrated.schema_version,"marketcow.runtime-scope-state.v2");
        assert_eq!(migrated.acquisition_market_ids,BTreeSet::from(["1".into()]));
        let writer = PreparedSourceWriter::open(&output,16384).unwrap();
        assert_eq!(writer.cold_market_record("2").unwrap(),Some(record("2")));
        assert_eq!(writer.cold_market_evidence("2").unwrap().unwrap()["books"]
            .as_object().unwrap().len(),0);
        assert!(writer.cold_market_record("1").unwrap().is_none());
        drop(writer);

        // Restart from the saturated legacy input after copy-only migration,
        // then rotate its one hot scope through the real journal/writer APIs.
        // The 2,047 already-cold records stay addressable and do not consume
        // either the process-local acquisition budget or the new hot binding.
        assert_eq!(migrated.restart_markets(true).unwrap().len(),1);
        let migrated_state_path = output.join("runtime-scope-state.json");
        let mut journal = super::super::source_scope_journal::Journal::open(
            migrated_state_path.clone(),2 * 1024 * 1024,migrated.clone()).unwrap();
        journal.admit(&[record("2049")],&BTreeSet::from(["2049".into()])).unwrap();
        let mut rotated = journal.state.clone();
        rotated.active_revision += 1;
        rotated.active_config["configured_markets"] =
            serde_json::json!([{"market_id":"2049"}]);
        journal.commit(rotated).unwrap();
        assert_eq!(journal.state.restart_markets(true).unwrap().len(),1);
        assert_eq!(journal.state.restart_markets(true).unwrap()[0].market_id,"2049");
        journal.retire(&BTreeSet::from(["1".into()]),2).unwrap();
        let mut rotated_writer = PreparedSourceWriter::open(&output,16384).unwrap();
        rotated_writer.retire_catalog_markets(rotated_writer.cursor().unwrap(),
            &BTreeSet::from(["1".into()]),&journal.state.retiring_records).unwrap();
        journal.settled(2).unwrap();
        drop(rotated_writer);
        drop(journal);
        let restarted = super::super::source_scope_journal::read(
            &migrated_state_path,2 * 1024 * 1024).unwrap().unwrap();
        assert_eq!(restarted.acquisition_market_ids,BTreeSet::from(["2049".into()]));
        assert_eq!(restarted.restart_markets(true).unwrap().len(),1);
        let cold = PreparedSourceWriter::open(&output,16384).unwrap();
        assert_eq!(cold.cold_market_record("1").unwrap(),Some(record("1")));
        assert_eq!(cold.cold_market_record("2").unwrap(),Some(record("2")));
        assert_eq!(cold.cold_market_record("2048").unwrap(),Some(record("2048")));
        assert!(cold.cold_market_record("2049").unwrap().is_none());
        drop(cold);

        // The formal bounded-history source stores its event tail in SQLite
        // and may have no events.jsonl at all. It still needs a complete,
        // hash-pinned copy and the same cold evidence guarantees.
        let mut bounded = PreparedSourceWriter::open(&source,16384).unwrap();
        bounded.enable_bounded_history(16384).unwrap();
        drop(bounded);
        fs::remove_file(source.join("events.jsonl")).unwrap();
        let db = Connection::open(source.join("indexes/latest-state.sqlite3")).unwrap();
        let (busy, _, _): (i64, i64, i64) = db.query_row(
            "PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?,row.get(1)?,row.get(2)?))
            }).unwrap();
        assert_eq!(busy,0);
        drop(db);
        let bounded_index_hash = sha256(&source.join("indexes/latest-state.sqlite3")).unwrap();
        let bounded_output = workspace.path().canonicalize().unwrap().join("bounded-output");
        let bounded_receipt = migrate(Inputs {source_root:&source,state_path:&state_path,
            output_root:&bounded_output,state_max_bytes:2 * 1024 * 1024,batch_bytes:16384,
            expected_state_sha256:&state_hash,expected_index_sha256:&bounded_index_hash,
            expected_events_sha256:None,copy_budget_bytes:2_000_000}).unwrap();
        assert_eq!(bounded_receipt["history_storage"],"bounded_sqlite");
        assert!(bounded_receipt["output_events_sha256"].is_null());
        assert!(!bounded_output.join("events.jsonl").exists());
        assert_eq!(PreparedSourceWriter::open(&bounded_output,16384).unwrap()
            .cold_market_record("2").unwrap(),Some(record("2")));
        assert!(PreparedSourceWriter::open(&bounded_output,16384).unwrap()
            .cold_market_evidence("2").unwrap().is_some());
    }
}
