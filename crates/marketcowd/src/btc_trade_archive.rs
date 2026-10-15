//! Permanent raw segments. Blocking IO belongs only to the archive worker.
use anyhow::{Context, Result, ensure};
use base64::Engine;
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{fs::{File, OpenOptions}, io::{BufRead, BufReader, Read, Seek, SeekFrom, Write}, path::{Path, PathBuf}};

pub const SEGMENT_BYTES: u64 = 512 * 1024 * 1024;
fn sql_id(id:Option<u64>)->Result<Option<i64>> {Ok(id.map(i64::try_from).transpose()?)}

fn diagnostic(value: Value) {
    marketcow_polymarket::diagnostic::emit(value.to_string());
}

pub fn open_index(root: &Path) -> Result<Connection> {
    std::fs::create_dir_all(root)?;
    let db = Connection::open(root.join("segments.sqlite3"))?;
    db.busy_timeout(std::time::Duration::from_secs(2))?;
    db.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS segments(\
        path TEXT PRIMARY KEY, first_id INTEGER,last_id INTEGER,first_t INTEGER,last_t INTEGER,\
        bytes INTEGER NOT NULL,closed INTEGER NOT NULL);\
        CREATE INDEX IF NOT EXISTS segments_time ON segments(last_t,first_t);\
        CREATE INDEX IF NOT EXISTS segments_id ON segments(first_id,last_id);\
        CREATE TABLE IF NOT EXISTS index_health(key TEXT PRIMARY KEY,value TEXT NOT NULL);")?;
    Ok(db)
}

fn endpoints(line: &[u8],last:bool) -> Result<(Option<u64>, Option<i64>)> {
    let v: Value=serde_json::from_slice(line)?;
    let raw=base64::engine::general_purpose::STANDARD.decode(v["raw_base64"].as_str().context("archive raw missing")?)?;
    let r:Value=serde_json::from_slice(&raw)?;
    if let Some(rows)=r.as_array() {
        let row=if last {rows.last()}else{rows.first()}.context("empty raw trades")?;
        Ok((row["id"].as_u64(),row["time"].as_i64()))
    } else { Ok((r["t"].as_u64(),r["T"].as_i64())) }
}

// Upgrade old immutable originals once at startup. No total-file-count limit.
// Endpoint reads are bounded per file; malformed records are never invented.
pub fn index_existing(root: &Path, db: &Connection) -> Result<()> {
    for entry in std::fs::read_dir(root)? {
        let path=entry?.path();
        let Some(name)=path.file_name().and_then(|v|v.to_str()) else {continue};
        if !name.starts_with("raw-") || !name.ends_with(".jsonl") {continue}
        ensure!(!path.is_symlink(),"archive path invalid");
        let size=path.metadata()?.len();
        let active:Option<i64>=db.query_row("SELECT closed FROM segments WHERE path=?",[name],|r|r.get(0)).ok();
        if active==Some(0) {continue}
        let indexed:Option<i64>=db.query_row("SELECT bytes FROM segments WHERE path=?",[name],|r|r.get(0)).ok();
        if indexed==Some(size as i64) {continue}
        let mut reader=BufReader::new(File::open(&path)?);
        let mut first=Vec::new();
        reader.by_ref().take(2*1024*1024).read_until(b'\n',&mut first)?;
        if first.is_empty(){continue}
        let (first_id,first_t)=endpoints(&first,false)?;
        reader.seek(SeekFrom::Start(size.saturating_sub(2*1024*1024)))?;
        if size>2*1024*1024 {let mut skip=Vec::new();reader.read_until(b'\n',&mut skip)?;}
        let mut last_id=first_id;let mut last_t=first_t;
        loop {
            let mut row=Vec::new();reader.by_ref().take(2*1024*1024).read_until(b'\n',&mut row)?;
            if row.is_empty(){break}
            if row.last()!=Some(&b'\n'){break}
            let (id,t)=endpoints(&row,true)?;last_id=id.or(last_id);last_t=t.or(last_t);
        }
        db.execute("INSERT OR REPLACE INTO segments VALUES (?,?,?,?,?,?,1)",params![name,sql_id(first_id)?,sql_id(last_id)?,first_t,last_t,i64::try_from(size)?])?;
    }
    db.execute("INSERT OR REPLACE INTO index_health VALUES ('ready','1')",[])?;
    Ok(())
}

pub fn selected_paths(root:&Path,start:i64,through:u64)->Result<Vec<PathBuf>> {
    let path=root.join("segments.sqlite3");
    if !path.exists() {let db=open_index(root)?;index_existing(root,&db)?;}
    let db=Connection::open_with_flags(path,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    db.busy_timeout(std::time::Duration::from_secs(2))?;
    let ready:Option<String>=db.query_row("SELECT value FROM index_health WHERE key='ready'",[],|r|r.get(0)).ok();
    ensure!(ready.as_deref()==Some("1"),"archive_index_building_or_failed");
    // Only relevant segments plus one predecessor segment. An old archive's
    // total segment count never spends this request's selected-file budget.
    let mut stmt=db.prepare("SELECT path FROM segments WHERE \
        (first_id IS NULL OR first_id<=?1) AND (last_t>=?2 OR closed=0) \
        UNION SELECT path FROM (SELECT path FROM segments WHERE last_t<?2 AND first_id<=?1 ORDER BY last_t DESC LIMIT 1) LIMIT 129")?;
    let names=stmt.query_map(params![i64::try_from(through)?,start-1000],|r|r.get::<_,String>(0))?
        .collect::<std::result::Result<Vec<_>,_>>()?;
    ensure!(names.len()<=128,"archive_selected_file_capacity");
    names.into_iter().map(|name|{
        ensure!(Path::new(&name).components().count()==1 && name.starts_with("raw-") && name.ends_with(".jsonl"),"archive path invalid");
        Ok(root.join(name))
    }).collect()
}

pub struct Writer {
    root:PathBuf,prefix:String,file:File,name:String,bytes:u64,segment_bytes:u64,
    db:Option<Connection>,first_id:Option<u64>,last_id:Option<u64>,first_t:Option<i64>,last_t:Option<i64>,since_sync:usize,
}
impl Writer {
    pub fn open(root:PathBuf,prefix:&str,indexed:bool,segment_bytes:u64)->Result<Self> {
        std::fs::create_dir_all(&root)?;
        let db=if indexed {Some(open_index(&root)?)} else {None};
        let (name,file)=Self::new_file(&root,prefix)?;
        let this=Self{root,prefix:prefix.into(),file,name,bytes:0,segment_bytes,db,
            first_id:None,last_id:None,first_t:None,last_t:None,since_sync:0};
        this.index(false)?;
        Ok(this)
    }
    fn new_file(root:&Path,prefix:&str)->Result<(String,File)> {
        let name=format!("{prefix}-{}.jsonl",uuid::Uuid::new_v4().simple());
        let file=OpenOptions::new().create_new(true).write(true).open(root.join(&name))?;
        diagnostic(json!({"stage":"btc_archive_segment_opened","observed_at":chrono::Utc::now(),"path":root.join(&name),"retention":"permanent_no_delete"}));
        Ok((name,file))
    }
    fn index(&self,closed:bool)->Result<()> {
        if let Some(db)=&self.db {db.execute("INSERT OR REPLACE INTO segments VALUES (?,?,?,?,?,?,?)",params![self.name,sql_id(self.first_id)?,sql_id(self.last_id)?,self.first_t,self.last_t,i64::try_from(self.bytes)?,closed])?;}
        Ok(())
    }
    pub fn append(&mut self,line:&[u8],id:Option<u64>,t:Option<i64>)->Result<()> {
        ensure!(line.len() as u64 +1<=self.segment_bytes,"archive record exceeds segment budget");
        if self.bytes>0 && self.bytes+line.len() as u64+1>self.segment_bytes {
            self.file.sync_data()?;self.index(true)?;
            let (name,file)=Self::new_file(&self.root,&self.prefix)?;
            self.name=name;self.file=file;self.bytes=0;self.first_id=None;self.last_id=None;self.first_t=None;self.last_t=None;self.since_sync=0;
            self.index(false)?;
        }
        // A failed partial append is abandoned; originals are never truncated.
        self.file.write_all(line)?;self.file.write_all(b"\n")?;
        if self.bytes==0 && self.db.is_some() {let (first_id,first_t)=endpoints(line,false)?;self.first_id=first_id;self.first_t=first_t;}
        self.bytes+=line.len() as u64+1;self.first_id=self.first_id.or(id);self.last_id=id.or(self.last_id);
        self.first_t=self.first_t.or(t);self.last_t=t.or(self.last_t);self.since_sync+=1;
        if self.since_sync>=256 {self.file.sync_data()?;self.index(false)?;self.since_sync=0;}
        Ok(())
    }
    pub fn finish(&self)->Result<()> {self.file.sync_data()?;self.index(true)}
}

pub fn failure(error:&anyhow::Error)->Value {
    let io=error.chain().find_map(|e|e.downcast_ref::<std::io::Error>());
    let code=match io.and_then(|e|e.raw_os_error()) {Some(28)=>"disk_full",Some(13)|Some(1)=>"permission_denied",_=>"archive_io_or_index_failed"};
    json!({"reason":code,"failed_at":chrono::Utc::now(),"os_error":io.and_then(|e|e.raw_os_error()),"detail":format!("{error:#}").chars().take(1024).collect::<String>()})
}
pub fn save_health(root:&Path,health:&Value)->Result<()> {
    let bytes=serde_json::to_vec(health)?;
    let temp=root.join(format!("health-{}.pending",uuid::Uuid::new_v4().simple()));
    let result=(||->Result<()> {
        let mut file=OpenOptions::new().create_new(true).write(true).open(&temp)?;
        file.write_all(&bytes)?;file.sync_data()?;
        std::fs::rename(&temp,root.join("archive-health.json"))?;Ok(())
    })();
    if result.is_err(){let _=std::fs::remove_file(&temp);}
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rotation_never_deletes_old_segments() {
        let root=tempfile::tempdir().unwrap();let mut w=Writer::open(root.path().to_owned(),"facts",false,12).unwrap();
        for _ in 0..8 {w.append(b"123456",None,None).unwrap();}w.finish().unwrap();
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(),8);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().map(|e|e.unwrap().metadata().unwrap().len()).sum::<u64>(),56);
    }
    #[test]
    fn old_segment_count_does_not_limit_query_lifetime() {
        let root=tempfile::tempdir().unwrap();let db=open_index(root.path()).unwrap();
        for i in 0..300 {db.execute("INSERT INTO segments VALUES (?,?,?,?,?,?,1)",params![format!("raw-{i}.jsonl"),i,i,i*1000,i*1000,1]).unwrap();}
        db.execute("INSERT INTO index_health VALUES ('ready','1')",[]).unwrap();
        assert!(selected_paths(root.path(),299000,299).unwrap().len()<=3);
    }
    #[test]
    fn disk_full_is_explicit_not_generic_capacity() {
        let error=anyhow::Error::new(std::io::Error::from_raw_os_error(28));assert_eq!(failure(&error)["reason"],"disk_full");
    }
}
