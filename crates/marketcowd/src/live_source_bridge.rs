//! Read-only, bounded live-stream.v1 bridge over the durable legacy source.
//! No collection, order/account endpoints, historical replay cache or source writes.
use anyhow::{Context, Result, ensure};
use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    routing::get,
};
use clap::Parser;
use rusqlite::{Connection, OpenFlags};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;

const SCHEMA: &str = "marketcow.polymarket.live-stream.v1";
const MAX_DEPENDENCY_MARKETS: usize = 1024;
const MAX_SCOPED_TOKEN_IDENTITIES: usize = MAX_DEPENDENCY_MARKETS * 2;

/// Startup recovery only. Online publication must use the collector's memory stream.
pub(crate) fn bootstrap(root: PathBuf, plan_path: PathBuf, sha: &str, cap: usize) -> Result<Value> {
    ensure!(
        fs::metadata(&plan_path)?.len() <= cap as u64,
        "plan byte limit"
    );
    let bytes = fs::read(&plan_path)?;
    ensure!(
        hex::encode(Sha256::digest(&bytes)) == sha,
        "bridge plan hash differs"
    );
    let plan: Plan = serde_json::from_slice(&bytes)?;
    ensure!(
        plan.schema_version == "marketcow.polymarket.live-bridge-plan.v1"
            && !plan.markets.is_empty()
            && plan.markets.len() <= MAX_DEPENDENCY_MARKETS,
        "invalid bridge plan"
    );
    ensure!(
        hex::encode(Sha256::digest(fs::read(root.join("catalog.json"))?))
            == plan.catalog_manifest_sha256,
        "catalog manifest changed"
    );
    let scope: Value = serde_json::from_slice(&fs::read(root.join("scope-runtime.json"))?)?;
    ensure!(scope["scope_id"] == plan.scope_id, "scope changed");
    let bridge = Bridge {
        args: Args {
            root: fs::canonicalize(root)?,
            plan: plan_path,
            plan_sha256: sha.into(),
            listen: "127.0.0.1:1".parse()?,
            maximum_frame_bytes: cap,
            maximum_clients: 1,
            poll_ms: 1,
        },
        plan,
        permits: Arc::new(Semaphore::new(1)),
    };
    Ok(serde_json::from_str(&bridge.snapshot()?.0)?)
}

#[derive(Parser)]
struct Args {
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    plan: PathBuf,
    #[arg(long)]
    plan_sha256: String,
    #[arg(long)]
    listen: SocketAddr,
    #[arg(long)]
    maximum_frame_bytes: usize,
    #[arg(long)]
    maximum_clients: usize,
    #[arg(long)]
    poll_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    schema_version: String,
    catalog_revision: String,
    scope_id: String,
    catalog_manifest_sha256: String,
    markets: Vec<Value>,
    catalog_source: Value,
}

struct Bridge {
    args: Args,
    plan: Plan,
    permits: Arc<Semaphore>,
}

fn metadata(db: &Connection) -> Result<BTreeMap<String, String>> {
    Ok(db
        .prepare("SELECT key,value FROM metadata")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?)
}

impl Bridge {
    fn database(&self) -> Result<Connection> {
        let path = self.args.root.join("indexes/latest-state.sqlite3");
        ensure!(
            fs::canonicalize(&path)?.starts_with(&self.args.root),
            "index escapes root"
        );
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.busy_timeout(Duration::from_secs(5))?;
        db.execute_batch("BEGIN")?;
        Ok(db)
    }

    fn boundary(&self, db: &Connection) -> Result<(u64, BTreeMap<String, String>)> {
        let values = metadata(db)?;
        ensure!(
            values.get("catalog_revision") == Some(&self.plan.catalog_revision),
            "catalog changed; reprepare bridge"
        );
        let token_recoveries = marketcow_runtime::discovery_source::load_token_recoveries(db)?;
        ensure!(
            values
                .get("unresolved_gap_count")
                .context("missing gap metadata")?
                .parse::<usize>()?
                == token_recoveries.len(),
            "source has unaccounted gaps"
        );
        let gaps: i64 = db.query_row("SELECT count(*) FROM gaps WHERE resolved=0", [], |r| {
            r.get(0)
        })?;
        ensure!(gaps == 0, "source gap ledger differs");
        let cursor = values
            .get("latest_cursor")
            .context("missing cursor")?
            .parse()?;
        Ok((cursor, values))
    }

    fn snapshot(&self) -> Result<(String, u64)> {
        let db = self.database()?;
        let (cursor, values) = self.boundary(&db)?;
        let mut books = Vec::new();
        let mut bytes = 0usize;
        let mut statement =
            db.prepare("SELECT cursor,payload_json,payload_sha256 FROM books ORDER BY token_id")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let at: i64 = row.get(0)?;
            ensure!(u64::try_from(at)? <= cursor, "future book");
            let body: Vec<u8> = row.get(1)?;
            bytes = bytes.checked_add(body.len()).context("byte overflow")?;
            ensure!(
                bytes <= self.args.maximum_frame_bytes && books.len() < MAX_SCOPED_TOKEN_IDENTITIES,
                "snapshot budget exceeded"
            );
            let expected: String = row.get(2)?;
            ensure!(
                hex::encode(Sha256::digest(&body)) == expected,
                "book hash mismatch"
            );
            books.push(serde_json::from_slice::<Value>(&body)?);
        }
        let recovery = values
            .get("active_recovery_id")
            .context("missing recovery state")?;
        let mut markets: BTreeMap<String, Value> = self
            .plan
            .markets
            .iter()
            .map(|m| {
                Ok((
                    m["identity"]["market_id"]
                        .as_str()
                        .context("missing market id")?
                        .to_owned(),
                    m.clone(),
                ))
            })
            .collect::<Result<_>>()?;
        let mut statement = db
            .prepare("SELECT market_id,cursor,payload_json,payload_sha256 FROM market_lifecycle")?;
        let mut rows = statement.query([])?;
        let mut count = 0;
        while let Some(row) = rows.next()? {
            count += 1;
            ensure!(
                count <= MAX_DEPENDENCY_MARKETS,
                "terminal market budget exceeded"
            );
            let id: String = row.get(0)?;
            let at: i64 = row.get(1)?;
            let body: Vec<u8> = row.get(2)?;
            let sha: String = row.get(3)?;
            ensure!(
                u64::try_from(at)? <= cursor
                    && body.len() <= 1024 * 1024
                    && hex::encode(Sha256::digest(&body)) == sha,
                "terminal boundary/hash mismatch"
            );
            let terminal: Value = serde_json::from_slice(&body)?;
            let previous = markets
                .get(&id)
                .context("terminal market outside prepared catalog")?;
            ensure!(
                terminal["identity"] == previous["identity"],
                "terminal identity mismatch"
            );
            markets.insert(id, terminal);
        }
        let token_recoveries = marketcow_runtime::discovery_source::load_token_recoveries(&db)?;
        let payload = json!({"schema_version":SCHEMA,"type":"state", "catalog_revision":self.plan.catalog_revision,
            "scope_id":self.plan.scope_id,"catalog_source":self.plan.catalog_source,
            "latest_cursor":cursor,"persisted_cursor":cursor,"persistence_queue_depth":0,
            "persistence_error":null,"derived_index_error":null,
            "active_recovery_id":if recovery.is_empty(){Value::Null}else{json!(recovery)},
            "history_oldest_cursor":cursor.checked_add(1).context("cursor overflow")?,
            "markets":markets.values().collect::<Vec<_>>(),"books":books,
            "gaps":token_recoveries.values().map(|r|r["gap"].clone()).collect::<Vec<_>>(),
            "token_recoveries":token_recoveries.values().collect::<Vec<_>>()});
        let encoded = serde_json::to_string(&payload)?;
        ensure!(
            encoded.len() <= self.args.maximum_frame_bytes,
            "state frame budget exceeded"
        );
        Ok((encoded, cursor))
    }

    fn page(&self, after: u64) -> Result<(String, u64)> {
        let db = self.database()?;
        let (boundary, values) = self.boundary(&db)?;
        ensure!(boundary >= after, "source cursor regressed");
        let bounded = values.contains_key("bounded_history_bytes");
        if bounded {
            let floor: u64 = values
                .get("history_floor_cursor")
                .context("missing history floor")?
                .parse()?;
            ensure!(
                after >= floor,
                "resume_cursor_expired: obtain new full-sync"
            );
        }
        let log_path = self.args.root.join("events.jsonl");
        let mut log = if bounded {
            None
        } else {
            ensure!(
                fs::canonicalize(&log_path)?.starts_with(&self.args.root),
                "log escapes root"
            );
            Some(File::open(log_path)?)
        };
        let mut events = Vec::new();
        let mut next = after;
        let mut bytes = 0usize;
        let mut statement = db.prepare(if bounded {
            "SELECT cursor,0,length(payload),sha256,payload FROM recent_events WHERE cursor>? AND cursor<=? ORDER BY cursor LIMIT 64"
        } else { "SELECT cursor,byte_offset,byte_length,line_sha256,NULL FROM event_offsets WHERE cursor>? AND cursor<=? ORDER BY cursor LIMIT 64" })?;
        let mut rows = statement.query([i64::try_from(after)?, i64::try_from(boundary)?])?;
        while let Some(row) = rows.next()? {
            let at = u64::try_from(row.get::<_, i64>(0)?)?;
            ensure!(
                Some(at) == next.checked_add(1),
                "noncontiguous durable events"
            );
            let offset = u64::try_from(row.get::<_, i64>(1)?)?;
            let size = usize::try_from(row.get::<_, i64>(2)?)?;
            ensure!(
                size <= self.args.maximum_frame_bytes,
                "event budget exceeded"
            );
            if bytes.checked_add(size).context("byte overflow")? > self.args.maximum_frame_bytes / 2
                && !events.is_empty()
            {
                break;
            }
            let mut line = if bounded {
                row.get::<_, Vec<u8>>(4)?
            } else {
                vec![0; size]
            };
            if !bounded {
                let log = log.as_mut().context("legacy log missing")?;
                log.seek(SeekFrom::Start(offset))?;
                log.read_exact(&mut line)?;
            }
            let hash: String = row.get(3)?;
            ensure!(
                line.last() == Some(&b'\n') && hex::encode(Sha256::digest(&line)) == hash,
                "durable event hash mismatch"
            );
            let event: Value = serde_json::from_slice(&line)?;
            ensure!(event["cursor"].as_u64() == Some(at), "event cursor differs");
            events.push(event);
            next = at;
            bytes += size;
        }
        ensure!(boundary == after || next > after, "missing durable events");
        let mut frame = json!({"schema_version":SCHEMA,"type":"persistence","persisted_cursor":boundary,
            "persistence_queue_depth":0,"persistence_error":null,"derived_index_error":null});
        if !events.is_empty() {
            frame["type"] = json!("events");
            frame["events"] = json!(events);
        }
        let encoded = serde_json::to_string(&frame)?;
        ensure!(
            encoded.len() <= self.args.maximum_frame_bytes,
            "event frame budget exceeded"
        );
        Ok((encoded, next))
    }
}

async fn handle(
    State(bridge): State<Arc<Bridge>>,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    ws.max_message_size(4096)
        .on_upgrade(move |socket| async move {
            let Ok(_permit) = bridge.permits.clone().try_acquire_owned() else {
                return;
            };
            if let Err(error) = serve(socket, bridge).await {
                eprintln!("live bridge closed: {error:#}");
            }
        })
}

async fn serve(mut socket: WebSocket, bridge: Arc<Bridge>) -> Result<()> {
    let request = tokio::time::timeout(Duration::from_secs(5), socket.recv())
        .await?
        .context("missing subscribe")??;
    let Message::Text(text) = request else {
        anyhow::bail!("subscribe required")
    };
    ensure!(
        serde_json::from_str::<Value>(&text)?["type"] == "subscribe",
        "subscribe required"
    );
    let source = bridge.clone();
    let (state, mut cursor) = tokio::task::spawn_blocking(move || source.snapshot()).await??;
    socket.send(Message::Text(state.into())).await?;
    socket
        .send(Message::Text(
            json!({"schema_version":SCHEMA,"type":"ready","latest_cursor":cursor})
                .to_string()
                .into(),
        ))
        .await?;
    loop {
        let source = bridge.clone();
        let (frame, next) = tokio::task::spawn_blocking(move || source.page(cursor)).await??;
        socket.send(Message::Text(frame.into())).await?;
        cursor = next;
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(bridge.args.poll_ms)) => {},
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Ping(body))) => socket.send(Message::Pong(body)).await?,
                Some(Ok(Message::Pong(_))) => {},
                _ => return Ok(()),
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();
    ensure!(
        args.listen.ip().is_loopback()
            && (1..=4).contains(&args.maximum_clients)
            && args.poll_ms > 0
            && (4096..=64 * 1024 * 1024).contains(&args.maximum_frame_bytes),
        "explicit bounded loopback configuration required"
    );
    args.root = fs::canonicalize(&args.root)?;
    ensure!(
        fs::metadata(&args.plan)?.len() <= args.maximum_frame_bytes as u64,
        "plan byte limit"
    );
    let bytes = fs::read(&args.plan)?;
    ensure!(
        hex::encode(Sha256::digest(&bytes)) == args.plan_sha256,
        "bridge plan hash differs"
    );
    let plan: Plan = serde_json::from_slice(&bytes)?;
    ensure!(
        plan.schema_version == "marketcow.polymarket.live-bridge-plan.v1"
            && !plan.markets.is_empty()
            && plan.markets.len() <= MAX_DEPENDENCY_MARKETS,
        "invalid bounded bridge plan"
    );
    ensure!(
        hex::encode(Sha256::digest(fs::read(args.root.join("catalog.json"))?))
            == plan.catalog_manifest_sha256,
        "catalog manifest changed"
    );
    let scope: Value = serde_json::from_slice(&fs::read(args.root.join("scope-runtime.json"))?)?;
    ensure!(scope["scope_id"] == plan.scope_id, "scope changed");
    let listen = args.listen;
    let permits = Arc::new(Semaphore::new(args.maximum_clients));
    let bridge = Arc::new(Bridge {
        args,
        plan,
        permits,
    });
    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(
        listener,
        Router::new().route("/", get(handle)).with_state(bridge),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (tempfile::TempDir, Bridge) {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir(temp.path().join("indexes")).unwrap();
        let db = Connection::open(temp.path().join("indexes/latest-state.sqlite3")).unwrap();
        db.execute_batch("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
            INSERT INTO metadata VALUES ('latest_cursor','0'),('catalog_revision','catalog'),('unresolved_gap_count','0'),('active_recovery_id','');
            CREATE TABLE gaps(resolved INTEGER);
            CREATE TABLE books(cursor INTEGER,payload_json BLOB,payload_sha256 TEXT,token_id TEXT);
            CREATE TABLE market_lifecycle(market_id TEXT PRIMARY KEY,cursor INTEGER,payload_json BLOB,payload_sha256 TEXT);
            CREATE TABLE event_offsets(cursor INTEGER,byte_offset INTEGER,byte_length INTEGER,line_sha256 TEXT);").unwrap();
        fs::write(temp.path().join("events.jsonl"), b"").unwrap();
        let args = Args {
            root: fs::canonicalize(temp.path()).unwrap(),
            plan: PathBuf::new(),
            plan_sha256: String::new(),
            listen: "127.0.0.1:8794".parse().unwrap(),
            maximum_frame_bytes: 8192,
            maximum_clients: 1,
            poll_ms: 100,
        };
        let bridge = Bridge {
            args,
            permits: Arc::new(Semaphore::new(1)),
            plan: Plan {
                schema_version: "marketcow.polymarket.live-bridge-plan.v1".into(),
                catalog_revision: "catalog".into(),
                scope_id: "scope".into(),
                catalog_manifest_sha256: String::new(),
                markets: vec![],
                catalog_source: json!({}),
            },
        };
        (temp, bridge)
    }

    #[test]
    fn snapshots_and_events_follow_committed_boundary_and_checksums() {
        let (_temp, bridge) = fixture();
        let (state, at) = bridge.snapshot().unwrap();
        assert_eq!(at, 0);
        assert_eq!(
            serde_json::from_str::<Value>(&state).unwrap()["history_oldest_cursor"],
            1
        );
        let body = b"{\"cursor\":1}\n";
        fs::write(bridge.args.root.join("events.jsonl"), body).unwrap();
        let db = Connection::open(bridge.args.root.join("indexes/latest-state.sqlite3")).unwrap();
        db.execute(
            "INSERT INTO event_offsets VALUES (1,0,?,?)",
            rusqlite::params![
                i64::try_from(body.len()).unwrap(),
                hex::encode(Sha256::digest(body))
            ],
        )
        .unwrap();
        assert_eq!(bridge.page(0).unwrap().1, 0); // Appended but unpublished tail is not sent.
        db.execute(
            "UPDATE metadata SET value='1' WHERE key='latest_cursor'",
            [],
        )
        .unwrap();
        let (page, at) = bridge.page(0).unwrap();
        assert_eq!(at, 1);
        assert_eq!(
            serde_json::from_str::<Value>(&page).unwrap()["events"][0]["cursor"],
            1
        );
        fs::write(bridge.args.root.join("events.jsonl"), b"{\"cursor\":2}\n").unwrap();
        assert!(bridge.page(0).is_err());
    }

    #[test]
    fn bounded_replay_expires_only_old_resume_without_legacy_log() {
        let (_temp, bridge) = fixture();
        let db = Connection::open(bridge.args.root.join("indexes/latest-state.sqlite3")).unwrap();
        db.execute_batch("CREATE TABLE recent_events(cursor INTEGER PRIMARY KEY,payload BLOB,sha256 TEXT);
            INSERT INTO metadata VALUES ('bounded_history_bytes','8192'),('history_floor_cursor','10');
            UPDATE metadata SET value='11' WHERE key='latest_cursor';").unwrap();
        let body = b"{\"cursor\":11}\n";
        db.execute("INSERT INTO recent_events VALUES(11,?,?)",rusqlite::params![body.as_slice(),hex::encode(Sha256::digest(body))]).unwrap();
        fs::remove_file(bridge.args.root.join("events.jsonl")).unwrap();
        assert!(bridge.page(9).unwrap_err().to_string().contains("resume_cursor_expired"));
        assert_eq!(bridge.snapshot().unwrap().1,11);
        let (page, cursor) = bridge.page(10).unwrap();
        assert_eq!(cursor,11);
        assert_eq!(serde_json::from_str::<Value>(&page).unwrap()["events"][0]["cursor"],11);
        assert_eq!(bridge.page(11).unwrap().1,11);
        db.execute("UPDATE recent_events SET payload=x'00'",[]).unwrap();
        assert!(bridge.page(10).is_err());
    }

    #[test]
    fn gaps_and_regression_close_instead_of_publishing_ready() {
        let (_temp, bridge) = fixture();
        assert!(bridge.page(1).is_err());
        let db = Connection::open(bridge.args.root.join("indexes/latest-state.sqlite3")).unwrap();
        db.execute("INSERT INTO gaps VALUES (0)", []).unwrap();
        assert!(bridge.snapshot().is_err());
        assert!(bridge.page(0).is_err());
    }
}
