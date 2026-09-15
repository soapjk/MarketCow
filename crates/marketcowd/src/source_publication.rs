//! Memory-first publication. The disk worker never owns the publication lock during I/O.
use anyhow::{Context, Result, ensure};
use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    routing::get,
};
use marketcow_polymarket::discovery_source::{SnapshotBoundary, canonical_hash, prepare_snapshot};
#[cfg(test)]
use marketcow_runtime::discovery_source::validate_source_events;
use marketcow_runtime::discovery_source::{
    ValidatedSourceBatch, apply_token_recovery, is_token_recovery,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, mpsc},
    thread,
};
use tokio::sync::{Semaphore, watch};

fn advances_book_confirmation_fence(event_type: Option<&str>) -> bool {
    matches!(
        event_type,
        Some("book" | "price_change" | "tick_size_change")
    )
}

fn preserve_confirmed_book_freshness(
    event_type: Option<&str>,
    current: Option<&Value>,
    next: &mut Value,
) -> Result<()> {
    if advances_book_confirmation_fence(event_type) {
        return Ok(());
    }
    let Some(current) = current else {
        return Ok(());
    };
    let current_at = chrono::DateTime::parse_from_rfc3339(
        current["received_at"]
            .as_str()
            .context("current book received_at")?,
    )?;
    let next_at = chrono::DateTime::parse_from_rfc3339(
        next["received_at"]
            .as_str()
            .context("next book received_at")?,
    )?;
    if current_at > next_at {
        // last_trade_price and best_bid_ask do not replace the complete
        // order book. Preserve the newer independent confirmation receipt
        // while still applying their trade/top-quote fields.
        next["received_at"] = current["received_at"].clone();
    }
    Ok(())
}

pub struct Batch {
    pub validated: ValidatedSourceBatch,
    pub evidence: Option<Value>,
    /// Explicit owner-admitted identities, ordered before these exact events
    /// on the existing disk worker. Never inferred from an untrusted event.
    pub catalog_bindings: BTreeMap<String, String>,
    bytes: usize,
}
pub struct Retirement {
    pub ticket:u64,
    pub after:u64,
    pub markets:std::collections::BTreeSet<String>,
    pub catalog_bindings:BTreeMap<String,String>,
    bytes:usize,
}
enum QueuedWork { Events(Arc<Batch>), Retire(Retirement) }
pub enum PersistenceWork<'a> { Events(&'a [Arc<Batch>]), Retire(&'a Retirement) }
struct Memory {
    retirement_submitted:u64,retirement_persisted:u64,
    acquisition_statistics:Option<(usize,usize,usize)>,
    recoveries: BTreeMap<String, Value>,
    source_ready: bool,
    resume_floor:u64,
    cursor: u64,
    persisted: u64,
    queued: usize,
    queued_bytes: usize,
    error: Option<String>,
    base: Option<Value>,
    books: BTreeMap<String, Arc<Value>>,
    book_cursors: BTreeMap<String, u64>,
    // Venue time of the latest order-book mutation. `LiveBook.exchange_at`
    // also advances on last_trade_price, so it cannot fence REST book
    // confirmations without falsely treating an independent trade as a newer
    // book.
    book_exchanges: BTreeMap<String, chrono::DateTime<chrono::Utc>>,
    markets: BTreeMap<String, Arc<Value>>,
    // Authoritative lifecycle tombstones. This bounded set prevents any late
    // transport frame from resurrecting recovery/book state after terminal
    // publication, even if an unsubscribe receipt is still in flight.
    terminal_markets: std::collections::BTreeSet<String>,
    replay: VecDeque<Arc<Batch>>,
    replay_bytes: usize,
    snapshot_leases: BTreeMap<u64, std::time::Instant>,
    state_bytes: usize,
    state_sizes: BTreeMap<(bool, String), usize>,
    confirmation_cursor: u64,
    confirmations: BTreeMap<String, u64>,
}
pub struct Publication {
    memory: Arc<Mutex<Memory>>,
    changed: watch::Sender<u64>,
    sender: Option<mpsc::Sender<QueuedWork>>,
    worker: Option<thread::JoinHandle<()>>,
    tokens: BTreeMap<String, String>,
    pending_bindings: BTreeMap<String, String>,
    retirement_enabled:bool,
    max_batches: usize,
    max_bytes: usize,
    batch_cap: usize,
}

/// One atomic memory boundary. Books/markets are immutable versions: holding a
/// view cannot block ingestion and later confirmations cannot mutate this view.
/// Callers must bound concurrent views and their lifetime independently.
#[derive(Clone)]
pub struct MemoryView {
    pub cursor: u64,
    pub persisted_cursor: u64,
    pub confirmation_sequence: u64,
    pub books: BTreeMap<String, Arc<Value>>,
    pub markets: BTreeMap<String, Arc<Value>>,
    pub recoveries: BTreeMap<String, Value>,
    pub book_cursors: BTreeMap<String, u64>,
    pub confirmation_versions: BTreeMap<String, u64>,
    pub base: Value,
    pub queued: usize,
}

/// Read-only shared handle. It cannot publish events or perform persistence.
#[derive(Clone)]
pub struct MemoryReader {
    memory: Arc<Mutex<Memory>>,
    changed: watch::Sender<u64>,
}
pub struct PublicReplay {
    pub batches: Vec<Arc<Batch>>,
    pub next: u64,
    pub boundary_cursor: u64,
    pub caught_up: bool,
    pub confirmation_sequence: u64,
    pub confirmation_books: Vec<Arc<Value>>,
}
impl MemoryReader {
    /// Control-plane identity remains readable while acquisition is
    /// intentionally paused. This does not expose books, cursors or a usable
    /// market-data snapshot and therefore cannot bypass the readiness fence.
    pub fn installed_markets(&self)->Result<BTreeMap<String,Arc<Value>>> {
        let memory=self.memory.lock().map_err(|_|anyhow::anyhow!("publication poisoned"))?;
        ensure!(memory.error.is_none(),"publication persistence failed");
        Ok(memory.markets.clone())
    }
    pub fn acquisition_statistics(&self)->Option<Value> {
        self.memory.lock().ok()?.acquisition_statistics.map(|(tokens,sockets,retiring)|
            json!({"tokens":tokens,"sockets":sockets,"retiring_shards":retiring}))
    }
    pub fn retirement_status(&self)->Result<(u64,u64)> {
        let m=self.memory.lock().map_err(|_|anyhow::anyhow!("publication poisoned"))?;
        ensure!(m.error.is_none(),"publication persistence failed");
        Ok((m.retirement_submitted,m.retirement_persisted))
    }
    pub fn capture(&self) -> Result<MemoryView> {
        MemoryView::capture(&self.memory.lock().unwrap())
    }
    /// Pin a full-sync boundary against count-based eviction until the
    /// existing response timeout. The global byte cap still wins: overflow
    /// expires the affected resume rather than blocking publication.
    pub fn capture_for_resume(&self, ttl:std::time::Duration, capacity:usize)->Result<MemoryView> {
        ensure!(!ttl.is_zero() && (1..=4).contains(&capacity),"snapshot lease budget");
        let mut m=self.memory.lock().unwrap();
        let now=std::time::Instant::now();
        m.snapshot_leases.retain(|_,until|*until>now);
        let view=MemoryView::capture(&m)?;
        ensure!(m.snapshot_leases.contains_key(&view.cursor)||m.snapshot_leases.len()<capacity,"snapshot resume capacity");
        // Re-reading the same boundary cannot extend a lease indefinitely.
        m.snapshot_leases.entry(view.cursor).or_insert(now+ttl);
        Ok(view)
    }
    pub fn subscribe(&self) -> watch::Receiver<u64> { self.changed.subscribe() }

    /// Shared immutable batches, not per-client copies. Confirmation baseline
    /// is captured under the same lock only if this page reaches its boundary.
    pub fn replay(&self, after: u64, confirmation_after: u64, max_batches: usize, max_bytes: usize) -> Result<PublicReplay> {
        ensure!((1..=64).contains(&max_batches), "replay batch budget");
        ensure!(max_bytes > 0, "replay byte budget");
        let m = self.memory.lock().unwrap();
        ensure!(m.source_ready && m.error.is_none(), "source unavailable");
        ensure!(after <= m.cursor, "future cursor");
        ensure!(after>=m.resume_floor,"memory replay expired");
        let mut batches = Vec::new();
        let mut next = after;
        let mut bytes = 0usize;
        for batch in &m.replay {
            let end = batch.validated.events().last().context("empty batch")?["cursor"].as_u64().context("cursor")?;
            if end <= after { continue; }
            if batch.bytes > max_bytes.saturating_sub(bytes) {
                ensure!(!batches.is_empty(), "single batch exceeds public replay budget");
                break;
            }
            let first = batch.validated.events().first().context("empty batch")?["cursor"].as_u64().context("cursor")?;
            ensure!(first <= next.checked_add(1).context("cursor overflow")?, "memory replay expired");
            batches.push(batch.clone());
            bytes += batch.bytes;
            next = end;
            if batches.len() == max_batches { break; }
        }
        ensure!(after == m.cursor || next > after, "memory replay expired");
        let caught_up = next == m.cursor;
        let confirmation_books = if caught_up {
            m.confirmations.iter().filter(|(_,seq)| **seq > confirmation_after)
                .filter_map(|(token,_)| m.books.get(token).cloned()).collect()
        } else { Vec::new() };
        Ok(PublicReplay {batches,next,boundary_cursor:m.cursor,caught_up,confirmation_books,
            confirmation_sequence:if caught_up {m.confirmation_cursor}else{confirmation_after}})
    }
}

impl MemoryView {
    fn capture(m: &Memory) -> Result<Self> {
        ensure!(m.source_ready, "upstream WebSocket recovery pending");
        ensure!(m.error.is_none(), "persistence failed");
        // A pause establishes a new acquisition generation.  Do not expose a
        // pre-pause book as fresh merely because another market has proved the
        // resumed source connection alive.  Markets that have not crossed the
        // resume floor are omitted and remain locally unavailable.
        let fresh = |token: &&String| {
            m.book_cursors
                .get(*token)
                .is_some_and(|cursor| *cursor >= m.resume_floor)
        };
        Ok(Self {
            cursor: m.cursor,
            persisted_cursor: m.persisted,
            confirmation_sequence: m.confirmation_cursor,
            books: m.books.iter().filter(|(token, _)| fresh(token)).map(|(token, book)| (token.clone(), book.clone())).collect(),
            markets: m.markets.clone(),
            recoveries: m.recoveries.clone(),
            book_cursors: m.book_cursors.iter().filter(|(token, _)| fresh(token)).map(|(token, cursor)| (token.clone(), *cursor)).collect(),
            confirmation_versions: m.confirmations.iter().filter(|(token, _)| fresh(token)).map(|(token, version)| (token.clone(), *version)).collect(),
            base: m.base.clone().context("missing stream seed")?,
            queued: m.queued,
        })
    }

    /// Legacy internal shape, built outside the ingestion lock. The public
    /// adapter consumes this same boundary without a network/SQLite bridge.
    fn into_source_state(self) -> Result<Value> {
        let mut state = self.base;
        state["latest_cursor"] = json!(self.cursor);
        state["persisted_cursor"] = json!(self.persisted_cursor);
        state["persistence_queue_depth"] = json!(self.queued);
        state["history_oldest_cursor"] = json!(self.cursor.checked_add(1).context("cursor overflow")?);
        state["books"] = json!(self.books.values().collect::<Vec<_>>());
        state["token_recoveries"] = json!(self.recoveries.values().collect::<Vec<_>>());
        let mut gaps = state["gaps"].as_array().context("base gaps")?.clone();
        gaps.extend(self.recoveries.values().map(|r| r["gap"].clone()));
        state["gaps"] = json!(gaps);
        state["markets"] = json!(self.markets.values().collect::<Vec<_>>());
        Ok(state)
    }
}
impl Publication {
    pub fn reader(&self) -> MemoryReader { MemoryReader { memory: self.memory.clone(), changed:self.changed.clone() } }
    pub fn capture_view(&self) -> Result<MemoryView> {
        MemoryView::capture(&self.memory.lock().unwrap())
    }
    /// Internal shutdown audit only: source readiness is deliberately false
    /// after intake stops. This does not expose a usable consumer baseline.
    pub fn shutdown_recovery_facts(&self) -> Result<(u64,BTreeMap<String,Value>)> {
        let memory=self.memory.lock().map_err(|_|anyhow::anyhow!("publication poisoned"))?;
        ensure!(memory.error.is_none(),"publication persistence failed");
        Ok((memory.cursor,memory.recoveries.clone()))
    }
    pub fn start(
        cursor: u64,
        base: Option<Value>,
        tokens: BTreeMap<String, String>,
        max_batches: usize,
        max_bytes: usize,
        batch_cap: usize,
        mut persist: impl FnMut(&[Arc<Batch>]) -> Result<()> + Send + 'static,
    ) -> Result<Self> {
        let mut publication=Self::start_managed(cursor,base,tokens,max_batches,max_bytes,batch_cap,move |work|match work {
            PersistenceWork::Events(batches)=>persist(batches),
            PersistenceWork::Retire(_)=>anyhow::bail!("retirement persistence not installed"),
        })?;
        publication.retirement_enabled=false;
        Ok(publication)
    }

    pub fn start_managed(
        cursor:u64,
        mut base:Option<Value>,
        tokens:BTreeMap<String,String>,
        max_batches:usize,
        max_bytes:usize,
        batch_cap:usize,
        mut persist:impl FnMut(PersistenceWork<'_>)->Result<()> + Send + 'static,
    ) -> Result<Self> {
        ensure!(
            (1..=4096).contains(&max_batches)
                && max_bytes >= batch_cap
                && max_bytes <= 256 * 1024 * 1024
                && batch_cap > 0
                && batch_cap <= 32 * 1024 * 1024,
            "explicit bounded queue limits required"
        );
        let mut books = BTreeMap::new();
        let mut book_cursors = BTreeMap::new();
        let mut book_exchanges = BTreeMap::new();
        let mut markets: BTreeMap<String, Arc<Value>> = BTreeMap::new();
        let mut state_sizes = BTreeMap::new();
        if let Some(state) = &base {
            ensure!(
                state["latest_cursor"].as_u64() == Some(cursor),
                "startup boundary differs"
            );
            for book in state["books"].as_array().context("missing books")? {
                let token = book["token_id"].as_str().context("token")?.to_owned();
                state_sizes.insert((false, token.clone()), serde_json::to_vec(book)?.len());
                book_exchanges.insert(
                    token.clone(),
                    chrono::DateTime::parse_from_rfc3339(
                        book["exchange_at"].as_str().context("book exchange_at")?,
                    )?
                    .with_timezone(&chrono::Utc),
                );
                // The initial state is one atomic snapshot. Every included
                // book is causally safe at the snapshot's cursor.
                book_cursors.insert(token.clone(), cursor);
                books.insert(token, Arc::new(book.clone()));
            }
            for market in state["markets"].as_array().context("missing markets")? {
                state_sizes.insert(
                    (
                        true,
                        market["identity"]["market_id"]
                            .as_str()
                            .context("market")?
                            .into(),
                    ),
                    serde_json::to_vec(market)?.len(),
                );
                markets.insert(
                    market["identity"]["market_id"]
                        .as_str()
                        .context("market id")?
                        .into(),
                    Arc::new(market.clone()),
                );
            }
        }
        let state_bytes = base
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()?
            .map_or(0, |v| v.len());
        ensure!(
            state_bytes <= max_bytes,
            "initial memory state exceeds byte budget"
        );
        if let Some(state) = &mut base {
            state["books"] = json!([]);
            state["markets"] = json!([]);
        }
        let mut recoveries = BTreeMap::new();
        if let Some(items) = base.as_ref().and_then(|b| b["token_recoveries"].as_array()) {
            for item in items {
                let token = item["token_id"].as_str().context("recovery token")?;
                ensure!(
                    tokens.contains_key(token) && recoveries.len() < marketcow_runtime::discovery_source::MAX_SOURCE_TOKEN_IDENTITIES,
                    "recovery scope/cap"
                );
                recoveries.insert(token.into(), item.clone());
            }
        }
        if let Some(state) = &mut base {
            if let Some(gaps) = state["gaps"].as_array_mut() {
                gaps.retain(|gap| {
                    !gap["token_id"]
                        .as_str()
                        .is_some_and(|t| recoveries.contains_key(t))
                });
            }
            state["token_recoveries"] = json!([]);
        }
        let terminal_markets = markets
            .iter()
            .filter(|(_, market)| {
                matches!(
                    market["lifecycle_state"].as_str(),
                    Some("closed" | "resolved" | "invalid")
                )
            })
            .map(|(market, _)| market.clone())
            .collect();
        let memory = Arc::new(Mutex::new(Memory {
            retirement_submitted:0,retirement_persisted:0,
            acquisition_statistics:None,
            recoveries,
            source_ready: true,
            resume_floor:0,
            cursor,
            persisted: cursor,
            queued: 0,
            queued_bytes: 0,
            error: None,
            base,
            books,
            book_cursors,
            book_exchanges,
            markets,
            terminal_markets,
            replay: VecDeque::new(),
            replay_bytes: 0,
            snapshot_leases: BTreeMap::new(),
            state_bytes,
            state_sizes,
            confirmation_cursor: 0,
            confirmations: BTreeMap::new(),
        }));
        let (sender, receiver) = mpsc::channel::<QueuedWork>();
        let (changed, _) = watch::channel(cursor);
        let state = memory.clone();
        let notify = changed.clone();
        let worker = thread::Builder::new()
            .name("polymarket-durability".into())
            .spawn(move || {
                let mut pending = None;
                while let Some(work) = pending.take().or_else(|| receiver.recv().ok()) {
                    let batch=match work {
                        QueuedWork::Events(batch)=>batch,
                        QueuedWork::Retire(retirement)=>{
                            let result=persist(PersistenceWork::Retire(&retirement));
                            let mut memory=state.lock().expect("publication mutex poisoned");
                            match result {
                                Ok(())=>{memory.queued-=1;memory.queued_bytes-=retirement.bytes;memory.retirement_persisted=retirement.ticket;},
                                Err(error)=>{memory.error=Some(format!("{error:#}"));notify.send_modify(|n|*n=n.wrapping_add(1));break;},
                            }
                            notify.send_modify(|n|*n=n.wrapping_add(1));
                            continue;
                        }
                    };
                    let terminal = batch.validated.events()[0]["event_type"] == "market_terminal";
                    let mut count = batch.validated.events().len();
                    let mut bytes = batch.bytes;
                    let mut group = vec![batch];
                    // Drain only available work, without a batching delay. Keep terminal
                    // evidence separate and count in-flight work against the same limits.
                    while !terminal && count < 256 {
                        let Ok(work) = receiver.try_recv() else { break };
                        let next=match work {
                            QueuedWork::Events(batch)=>batch,
                            other=>{pending=Some(other);break;},
                        };
                        if next.validated.events()[0]["event_type"] == "market_terminal"
                            || count + next.validated.events().len() > 256
                            || bytes + next.bytes > batch_cap
                        {
                            pending = Some(QueuedWork::Events(next));
                            break;
                        }
                        count += next.validated.events().len();
                        bytes += next.bytes;
                        group.push(next);
                    }
                    // Deliberately outside the memory mutex, including fsync and SQLite commit.
                    let disk_started=std::time::Instant::now();
                    let result = persist(PersistenceWork::Events(&group));
                    eprintln!("{}",json!({"stage":"durability_worker","elapsed_us":disk_started.elapsed().as_micros(),"event_count":count}));
                    let mut memory = state.lock().expect("publication mutex poisoned");
                    match result {
                        Ok(()) => {
                            memory.persisted = group.last().unwrap().validated.events().last().unwrap()["cursor"]
                                .as_u64()
                                .unwrap();
                            memory.queued -= group.len();
                            memory.queued_bytes -= bytes;
                        }
                        Err(error) => {
                            memory.error = Some(format!("{error:#}"));
                            notify.send_modify(|n| *n = n.wrapping_add(1));
                            break;
                        }
                    }
                    notify.send_modify(|n| *n = n.wrapping_add(1));
                }
            })?;
        Ok(Self {
            memory,
            changed,
            sender: Some(sender),
            worker: Some(worker),
            tokens,
            pending_bindings: BTreeMap::new(),
            retirement_enabled:true,
            max_batches,
            max_bytes,
            batch_cap,
        })
    }
    pub fn cursor(&self) -> Result<u64> {
        let m = self.memory.lock().unwrap();
        ensure!(m.error.is_none(), "persistence failed: {:?}", m.error);
        Ok(m.cursor)
    }
    /// The acquisition owner must first expire all scopes referencing these
    /// markets and fence their actors/recovery jobs. This drops current state,
    /// not source history, and queues disk retirement without awaiting I/O.
    pub fn retire_catalog_markets(&mut self,markets:std::collections::BTreeSet<String>)->Result<()> {
        self.retire_catalog_markets_inner(markets,false)
    }
    pub fn check_retirement(&mut self,markets:std::collections::BTreeSet<String>)->Result<()> {
        self.retire_catalog_markets_inner(markets,true)
    }
    fn retire_catalog_markets_inner(&mut self,markets:std::collections::BTreeSet<String>,check_only:bool)->Result<()> {
        ensure!(self.retirement_enabled,"retirement persistence not installed");
        ensure!(!markets.is_empty() && markets.len()<=4096,"retirement market capacity");
        let tokens:std::collections::BTreeSet<String>=self.tokens.iter().filter(|(_,id)|markets.contains(*id)).map(|(token,_)|token.clone()).collect();
        let mut m=self.memory.lock().unwrap();
        ensure!(markets.iter().all(|id|m.markets.contains_key(id)||self.tokens.values().any(|market|market==id)),"retired market not admitted");
        ensure!(m.error.is_none(),"publication unavailable");
        let bytes=serde_json::to_vec(&json!({"after":m.cursor,"markets":markets,"catalog_bindings":self.pending_bindings}))?.len();
        ensure!(bytes<=self.batch_cap && m.queued<self.max_batches && bytes<=self.max_bytes-m.queued_bytes,"retirement queue capacity");
        let removed_keys:Vec<_>=m.state_sizes.keys().filter(|(market,id)|if *market {markets.contains(id)}else{tokens.contains(id)}).cloned().collect();
        let removed_bytes=removed_keys.iter().map(|key|m.state_sizes[key]).sum::<usize>();
        let state_bytes=m.state_bytes.checked_sub(removed_bytes).context("retirement size accounting")?;
        if check_only {return Ok(());}
        let ticket=m.retirement_submitted.checked_add(1).context("retirement ticket overflow")?;
        self.sender.as_ref().context("writer closed")?.send(QueuedWork::Retire(Retirement {
            ticket,after:m.cursor,markets:markets.clone(),catalog_bindings:self.pending_bindings.clone(),bytes,
        }))?;
        self.pending_bindings.clear();
        m.retirement_submitted=ticket;
        for key in removed_keys {m.state_sizes.remove(&key);}
        for token in &tokens {
            m.books.remove(token);m.book_cursors.remove(token);m.book_exchanges.remove(token);
            m.confirmations.remove(token);m.recoveries.remove(token);
            self.tokens.remove(token);
        }
        for market in &markets {
            m.markets.remove(market);
            m.terminal_markets.remove(market);
        }
        if let Some(gaps)=m.base.as_mut().and_then(|base|base["gaps"].as_array_mut()) {
            gaps.retain(|gap|!gap["token_id"].as_str().is_some_and(|t|tokens.contains(t))
                && !gap["market_id"].as_str().is_some_and(|id|markets.contains(id)));
        }
        m.state_bytes=state_bytes;m.queued+=1;m.queued_bytes+=bytes;
        self.changed.send_modify(|n|*n=n.wrapping_add(1));
        Ok(())
    }
    /// Admission is an in-memory control operation, not scope activation or
    /// proof of durable metadata. The owner supplies catalog-verified records;
    /// its persisted active plan must reconstruct them on restart. New books
    /// remain missing until authoritative source events arrive. Existing
    /// market/lifecycle records and confirmations are never overwritten here.
    pub fn admit_catalog_markets(&mut self, catalog_revision:&str, records:Vec<Value>, evidence_sha256:&str)->Result<()> {
        let ids=records.iter().map(|record|record["identity"]["market_id"].as_str()
            .map(str::to_owned).context("catalog admission market id")).collect::<Result<std::collections::BTreeSet<_>>>()?;
        self.admit_catalog_markets_scoped(catalog_revision,records,evidence_sha256,&ids,2048)
    }
    pub fn admit_catalog_markets_scoped(&mut self,catalog_revision:&str,records:Vec<Value>,evidence_sha256:&str,
        acquisition:&std::collections::BTreeSet<String>,maximum_tokens:usize)->Result<()> {
        self.admit_catalog_markets_inner(catalog_revision,records,evidence_sha256,acquisition,maximum_tokens,false)
    }
    pub fn check_catalog_admission(&mut self,catalog_revision:&str,records:Vec<Value>,evidence_sha256:&str,
        acquisition:&std::collections::BTreeSet<String>,maximum_tokens:usize)->Result<()> {
        self.admit_catalog_markets_inner(catalog_revision,records,evidence_sha256,acquisition,maximum_tokens,true)
    }
    fn admit_catalog_markets_inner(&mut self,catalog_revision:&str,records:Vec<Value>,evidence_sha256:&str,
        acquisition:&std::collections::BTreeSet<String>,maximum_tokens:usize,check_only:bool)->Result<()> {
        ensure!(!records.is_empty() && records.len()<=4096,"catalog admission count");
        ensure!((2..=marketcow_runtime::discovery_source::MAX_SOURCE_TOKEN_IDENTITIES).contains(&maximum_tokens),"explicit acquisition token limit");
        ensure!(canonical_hash(&json!(records))==evidence_sha256,"catalog admission evidence hash");
        let mut m=self.memory.lock().unwrap();
        ensure!(m.error.is_none() && self.sender.is_some(),"publication unavailable");
        ensure!(m.base.as_ref().is_some_and(|b|b["catalog_revision"]==catalog_revision),"catalog admission revision");
        let mut additions=BTreeMap::new();
        let mut metadata_owners=BTreeMap::new();
        for (market,record) in &m.markets {
            for outcome in record["identity"]["outcomes"].as_array().context("existing metadata outcomes")? {
                let token=outcome["token_id"].as_str().context("existing metadata token")?;
                if let Some(previous)=metadata_owners.insert(token,market.as_str()) {
                    ensure!(previous==market,"ambiguous existing metadata ownership");
                }
            }
        }
        let mut ids=std::collections::BTreeSet::new();
        let mut sizes=Vec::new();
        let mut state_bytes=m.state_bytes;
        for record in &records {
            let identity=&record["identity"];
            let market=identity["market_id"].as_str().filter(|s|!s.is_empty()).context("admitted market identity")?;
            ensure!(ids.insert(market),"duplicate admitted market");
            ensure!(identity["condition_id"].as_str().is_some_and(|s|!s.is_empty()),"admitted condition");
            let outcomes=identity["outcomes"].as_array().context("admitted outcomes")?;
            ensure!(outcomes.len()==2,"admitted binary market");
            let mut pair=std::collections::BTreeSet::new();
            for outcome in outcomes {
                let token=outcome["token_id"].as_str().filter(|s|!s.is_empty()&&s.len()<=128).context("admitted token")?;
                ensure!(pair.insert(token),"duplicate admitted token");
                ensure!(self.tokens.get(token).is_none_or(|id|id==market),"admitted token ownership changed");
                ensure!(metadata_owners.get(token).is_none_or(|id|*id==market),"admitted token conflicts with dependency metadata");
                if let Some(id)=additions.insert(token.to_owned(),market.to_owned()) {
                    ensure!(id==market,"duplicate admitted token owner");
                }
            }
            if let Some(existing)=m.markets.get(market) {
                ensure!(existing["identity"]==*identity,"admitted market identity changed");
                // Keep current typed lifecycle and metadata intact.
            } else {
                let size=serde_json::to_vec(record)?.len();
                state_bytes=state_bytes.checked_add(size).context("admission size overflow")?;
                sizes.push((market.to_owned(),size,Arc::new(record.clone())));
            }
        }
        ensure!(acquisition.iter().all(|id|ids.contains(id.as_str())),"acquisition lacks submitted metadata");
        additions.retain(|token,market|acquisition.contains(market)&&!self.tokens.contains_key(token));
        ensure!(self.tokens.len().checked_add(additions.len()).is_some_and(|n|n<=maximum_tokens),"admitted token capacity");
        ensure!(state_bytes<=self.max_bytes,"admitted metadata byte capacity");
        let mut pending=self.pending_bindings.clone();
        pending.extend(additions.clone());
        ensure!(serde_json::to_vec(&pending)?.len()<=self.batch_cap,"admitted binding byte capacity");
        if check_only {return Ok(());}
        // All validation/capacity checks precede mutation.
        for (market,size,record) in sizes {
            m.state_sizes.insert((true,market.clone()),size);
            m.markets.insert(market,record);
        }
        m.state_bytes=state_bytes;
        self.tokens.extend(additions);
        self.pending_bindings=pending;
        Ok(())
    }
    pub fn set_source_ready(&self, ready: bool) {
        self.memory.lock().unwrap().source_ready = ready;
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
    }
    pub fn is_source_ready(&self)->bool {self.memory.lock().unwrap().source_ready}
    pub fn fresh_acquisition_started(&self,tokens:&std::collections::BTreeSet<String>)->bool {
        let Ok(memory)=self.memory.lock()else{return false;};
        memory.error.is_none()&&!tokens.is_empty()&&tokens.iter().any(|token|
            memory.book_cursors.get(token).is_some_and(|cursor|*cursor>=memory.resume_floor)
                && !memory.recoveries.contains_key(token))
    }
    /// Intentionally stop acquisition without manufacturing an upstream gap.
    /// Existing consumers cannot resume across this boundary; the next lease
    /// must establish fresh books and obtain a new full-sync.
    pub fn pause_source(&self)->Result<()> {
        let mut memory=self.memory.lock().map_err(|_|anyhow::anyhow!("publication poisoned"))?;
        memory.source_ready=false;
        memory.resume_floor=memory.cursor.checked_add(1).context("resume floor overflow")?;
        memory.replay.clear();memory.replay_bytes=0;memory.snapshot_leases.clear();
        drop(memory);
        self.changed.send_modify(|n|*n=n.wrapping_add(1));
        Ok(())
    }
    pub fn set_acquisition_statistics(&self,statistics:(usize,usize,usize)) {
        self.memory.lock().unwrap().acquisition_statistics=Some(statistics);
    }
    pub fn persisted_cursor(&self) -> u64 {
        self.memory.lock().unwrap().persisted
    }
    /// Confirm a complete market pair from an independent authoritative REST
    /// observation. Matching content refreshes only the hot projection and is
    /// broadcast without consuming a causal event cursor or touching disk.
    /// A concurrent newer WS update fences a stale REST response.
    pub fn confirm_market(
        &self,
        market: &str,
        condition: &str,
        tokens: &[String; 2],
        raw_books: &[Value],
        request_started_at: chrono::DateTime<chrono::Utc>,
        received_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<ConfirmationOutcome> {
        self.confirm_market_with_policy(
            market,
            condition,
            tokens,
            raw_books,
            request_started_at,
            received_at,
            true,
        )
    }

    /// Confirm a complete pair obtained from a fresh, bounded official WS
    /// subscription. Unlike REST, the first full WS book is an authoritative
    /// current baseline even when its venue timestamp predates the main
    /// stream's latest mutation. The local receipt fence still prevents a
    /// snapshot from replacing a concurrent main-stream update.
    pub fn confirm_targeted_market(
        &self,
        market: &str,
        condition: &str,
        tokens: &[String; 2],
        raw_books: &[Value],
        request_started_at: chrono::DateTime<chrono::Utc>,
        received_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<ConfirmationOutcome> {
        self.confirm_market_with_policy(
            market,
            condition,
            tokens,
            raw_books,
            request_started_at,
            received_at,
            false,
        )
    }

    fn confirm_market_with_policy(
        &self,
        market: &str,
        condition: &str,
        tokens: &[String; 2],
        raw_books: &[Value],
        request_started_at: chrono::DateTime<chrono::Utc>,
        received_at: chrono::DateTime<chrono::Utc>,
        require_monotonic_exchange: bool,
    ) -> Result<ConfirmationOutcome> {
        ensure!(
            raw_books.len() == 2,
            "confirmation requires complete market pair"
        );
        // Transport receipt timestamps use the platform clock's native
        // precision. The durable wire contract is microsecond-precise, so
        // canonicalize before using this as an explicit snapshot boundary.
        let received_at = chrono::DateTime::from_timestamp_micros(received_at.timestamp_micros())
            .context("confirmation receipt timestamp")?;
        let mut candidates = BTreeMap::new();
        let mut evidence_hashes = BTreeMap::new();
        for raw in raw_books {
            let token = raw["asset_id"].as_str().context("confirmation token")?;
            evidence_hashes.insert(token.to_owned(), canonical_hash(raw));
            ensure!(
                tokens.iter().any(|expected| expected == token),
                "unrequested confirmation token"
            );
            let candidate = prepare_snapshot(
                raw,
                SnapshotBoundary {
                    market_id: market,
                    condition_id: condition,
                    token_id: token,
                    recovery_id: "rest-freshness-confirmation",
                    cursor: 1,
                    received_at,
                },
            )?
            .finalize(1)?["canonical_payload"]
                .clone();
            ensure!(
                candidates.insert(token.to_owned(), candidate).is_none(),
                "duplicate confirmation token"
            );
        }
        ensure!(
            tokens.iter().all(|token| candidates.contains_key(token)),
            "confirmation pair incomplete"
        );
        let mut memory = self.memory.lock().unwrap();
        let mut confirmed = Vec::new();
        let mut superseded = 0usize;
        for token in tokens {
            let current = memory
                .books
                .get(token)
                .context("confirmation book unavailable")?;
            let current_received = chrono::DateTime::parse_from_rfc3339(
                current["received_at"]
                    .as_str()
                    .context("book received_at")?,
            )?
            .with_timezone(&chrono::Utc);
            // An identical REST/targeted-WS response may finish decoding after
            // a newer WS book or confirmation was already installed. Matching
            // price/size alone does not make its older receipt a fresh proof.
            // Never move either the original-book or confirmation time back.
            let mut freshness_fence = current_received;
            for field in ["book_received_at", "confirmed_at"] {
                if let Some(value) = current[field].as_str() {
                    freshness_fence = freshness_fence.max(
                        chrono::DateTime::parse_from_rfc3339(value)?.with_timezone(&chrono::Utc)
                    );
                }
            }
            if received_at <= freshness_fence {
                superseded += 1;
                continue;
            }
            let candidate = &candidates[token];
            let current_book_exchange = memory
                .book_exchanges
                .get(token)
                .context("book exchange fence unavailable")?;
            let candidate_exchange = chrono::DateTime::parse_from_rfc3339(
                candidate["exchange_at"]
                    .as_str()
                    .context("confirmation exchange_at")?,
            )?
            .with_timezone(&chrono::Utc);
            // REST book confirmation proves the freshness of the price/size
            // book.  last_trade_price is a separate WS event stream and the
            // venue's REST snapshot can legitimately lag it while carrying an
            // identical current book.  Do not turn that independent lag into
            // a book mismatch or let it prevent a quiet book from being
            // freshness-confirmed; preserve the causally newer WS trade value.
            let book_differs = current["state_checksum"] != candidate["state_checksum"]
                || current["tick_size"] != candidate["tick_size"];
            if book_differs {
                if (require_monotonic_exchange && current_book_exchange > &candidate_exchange)
                    || current_received > request_started_at
                {
                    // This token advanced while the pair request was in flight.
                    // Preserve its newer WS state, but continue: a quiet sibling
                    // whose content exactly matches the same authoritative REST
                    // response can still be freshness-confirmed independently.
                    superseded += 1;
                    continue;
                }
                // A non-racing divergence still requires an atomic two-token
                // replacement. Do not partially apply confirmations collected
                // earlier in this loop.
                return Ok(ConfirmationOutcome::Mismatch);
            }
            let mut next = current.as_ref().clone();
            if next["book_received_at"].is_null() {
                next["book_received_at"] = current["received_at"].clone();
            }
            next["received_at"] = candidate["received_at"].clone();
            next["confirmed_at"] = candidate["received_at"].clone();
            next["confirmation_source"] = json!(if require_monotonic_exchange {
                "polymarket_rest"
            } else {
                "polymarket_websocket"
            });
            next["confirmation_evidence_sha256"] = json!(evidence_hashes[token]);
            if current["last_trade_price"] == candidate["last_trade_price"] {
                next["exchange_at"] = candidate["exchange_at"].clone();
                next["source_hash"] = candidate["source_hash"].clone();
            }
            confirmed.push((token.clone(), next));
        }
        if confirmed.is_empty() {
            ensure!(
                superseded == tokens.len(),
                "confirmation outcome unavailable"
            );
            return Ok(ConfirmationOutcome::Superseded);
        }
        for (token, book) in confirmed {
            let candidate_exchange = chrono::DateTime::parse_from_rfc3339(
                candidates[&token]["exchange_at"]
                    .as_str()
                    .context("confirmation book exchange_at")?,
            )?
            .with_timezone(&chrono::Utc);
            let book_exchange = memory
                .book_exchanges
                .get(&token)
                .cloned()
                .map_or(candidate_exchange, |current| {
                    current.max(candidate_exchange)
                });
            let old_size = memory
                .state_sizes
                .get(&(false, token.clone()))
                .copied()
                .unwrap_or(0);
            let new_size = serde_json::to_vec(&book)?.len();
            memory.state_bytes = memory
                .state_bytes
                .checked_sub(old_size)
                .context("state accounting underflow")?
                .checked_add(new_size)
                .context("state bytes overflow")?;
            ensure!(
                memory.state_bytes <= self.max_bytes,
                "memory state byte budget exhausted"
            );
            memory.state_sizes.insert((false, token.clone()), new_size);
            memory.books.insert(token.clone(), Arc::new(book));
            memory.book_exchanges.insert(token.clone(), book_exchange);
            memory.confirmation_cursor = memory
                .confirmation_cursor
                .checked_add(1)
                .context("confirmation cursor overflow")?;
            let confirmation_cursor = memory.confirmation_cursor;
            memory.confirmations.insert(token, confirmation_cursor);
        }
        drop(memory);
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
        Ok(ConfirmationOutcome::Confirmed)
    }
    /// Only capacity exhaustion waits. The normal memory publication path never
    /// waits for durability. One bounded incoming batch is retained while the
    /// transport's bounded channel propagates pressure upstream.
    pub async fn publish_with_capacity(&mut self, events: Vec<Value>) -> Result<()> {
        self.publish_with_capacity_and_evidence(events, None).await
    }

    /// Terminal lifecycle evidence uses the same bounded asynchronous durability
    /// path as books. The evidence is committed atomically with its terminal event;
    /// normal memory publication still precedes disk I/O.
    pub async fn publish_with_capacity_and_evidence(
        &mut self,
        events: Vec<Value>,
        evidence: Option<Value>,
    ) -> Result<()> {
        let terminal = events
            .first()
            .is_some_and(|e| e["event_type"] == "market_terminal");
        let validated = ValidatedSourceBatch::new(
            events,
            terminal,
            self.cursor()?,
            &self.tokens,
            self.batch_cap,
        )?;
        let bytes = validated
            .encoded_bytes()
            .checked_add(if self.pending_bindings.is_empty(){0}else{serde_json::to_vec(&self.pending_bindings)?.len()})
            .context("binding byte overflow")?
            .checked_add(
                evidence
                    .as_ref()
                    .map(serde_json::to_vec)
                    .transpose()?
                    .map_or(0, |value| value.len()),
            )
            .context("byte overflow")?;
        ensure!(bytes <= self.batch_cap, "batch byte limit");
        let mut changed = self.changed.subscribe();
        let started = std::time::Instant::now();
        let mut waited = false;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                {
                    let m = self.memory.lock().unwrap();
                    ensure!(m.error.is_none(), "persistence failed: {:?}", m.error);
                    if m.queued < self.max_batches && m.queued_bytes + bytes <= self.max_bytes {
                        break;
                    }
                }
                waited = true;
                changed
                    .changed()
                    .await
                    .context("durability worker disconnected")?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("persistence capacity exhausted for 10 seconds")??;
        if waited {
            eprintln!(
                "{}",
                json!({"stage":"persistence_capacity_wait","elapsed_us":started.elapsed().as_micros(),"batch_bytes":bytes})
            );
        }
        self.publish_validated(validated, evidence)
    }

    pub fn publish(&mut self, events: Vec<Value>, evidence: Option<Value>) -> Result<()> {
        let cursor = self.cursor()?;
        let terminal = events
            .first()
            .is_some_and(|e| e["event_type"] == "market_terminal");
        let validated =
            ValidatedSourceBatch::new(events, terminal, cursor, &self.tokens, self.batch_cap)?;
        self.publish_validated(validated, evidence)
    }
    fn publish_validated(
        &mut self,
        validated: ValidatedSourceBatch,
        evidence: Option<Value>,
    ) -> Result<()> {
        let events = validated.events();
        let terminal = events
            .first()
            .is_some_and(|e| e["event_type"] == "market_terminal");
        let bytes = validated
            .encoded_bytes()
            .checked_add(
                evidence
                    .as_ref()
                    .map(serde_json::to_vec)
                    .transpose()?
                    .map_or(0, |v| v.len()),
            )
            .context("byte overflow")?;
        ensure!(bytes <= self.batch_cap, "batch byte limit");
        let mut m = self.memory.lock().unwrap();
        ensure!(m.error.is_none(), "persistence failed: {:?}", m.error);
        if !terminal {
            ensure!(
                events.iter().all(|event| event["market_id"]
                    .as_str()
                    .is_some_and(|market| !m.terminal_markets.contains(market))),
                "event targets terminal market"
            );
        }
        let mut recoveries = m.recoveries.clone();
        if terminal {
            for token in events[0]["canonical_payload"]["retired_token_ids"]
                .as_array()
                .context("missing retired terminal tokens")?
            {
                recoveries.remove(token.as_str().context("invalid retired token")?);
            }
        }
        for event in events {
            apply_token_recovery(&mut recoveries, event)?;
        }
        ensure!(
            serde_json::to_vec(&recoveries)?.len() <= self.batch_cap,
            "recovery state byte cap"
        );
        // Limits include the batch currently being persisted, not just pending channel entries.
        ensure!(
            m.queued < self.max_batches && bytes <= self.max_bytes - m.queued_bytes,
            "persistence queue capacity exhausted; publication rejected without advancing cursor"
        );
        let mut state_bytes = m.state_bytes;
        let mut size_updates = Vec::new();
        if m.base.is_some() {
            for event in events {
                if is_token_recovery(event) {
                    continue;
                }
                let (key, model) = if terminal {
                    (&event["market_id"], &event["canonical_payload"]["market"])
                } else {
                    (&event["token_id"], &event["canonical_payload"])
                };
                let key = key.as_str().context("missing state identity")?;
                let cache_key = (terminal, key.to_owned());
                let old = m.state_sizes.get(&cache_key).copied().unwrap_or(0);
                let new_size = serde_json::to_vec(model)?.len();
                size_updates.push((cache_key, new_size));
                state_bytes = state_bytes
                    .checked_sub(old)
                    .context("state accounting underflow")?
                    .checked_add(new_size)
                    .context("state bytes overflow")?;
            }
            ensure!(
                state_bytes <= self.max_bytes,
                "memory state byte budget exhausted"
            );
        }
        let batch = Arc::new(Batch {
            validated,
            evidence,
            catalog_bindings:self.pending_bindings.clone(),
            bytes,
        });
        self.sender
            .as_ref()
            .context("writer closed")?
            .send(QueuedWork::Events(batch.clone()))?;
        self.pending_bindings.clear();
        for event in batch.validated.events() {
            if is_token_recovery(event) {
                continue;
            }
            if terminal {
                m.terminal_markets
                    .insert(event["market_id"].as_str().unwrap().to_owned());
            }
            if m.base.is_some() {
                if terminal {
                    let market_id = event["market_id"].as_str().unwrap().to_owned();
                    m.markets.insert(
                        market_id,
                        Arc::new(event["canonical_payload"]["market"].clone()),
                    );
                } else {
                    let token = event["token_id"].as_str().unwrap().to_owned();
                    m.book_cursors.insert(
                        token.clone(),
                        event["cursor"].as_u64().context("book event cursor")?,
                    );
                    // best_bid_ask authenticates only the top quote already
                    // present in the local book; it neither refreshes full
                    // depth nor supersedes an independent REST full-book
                    // observation. last_trade_price is likewise separate.
                    if advances_book_confirmation_fence(event["event_type"].as_str()) {
                        m.book_exchanges.insert(
                            token.clone(),
                            chrono::DateTime::parse_from_rfc3339(
                                event["canonical_payload"]["exchange_at"]
                                    .as_str()
                                    .context("book event exchange_at")?,
                            )?
                            .with_timezone(&chrono::Utc),
                        );
                    }
                    let mut next = event["canonical_payload"].clone();
                    preserve_confirmed_book_freshness(
                        event["event_type"].as_str(),
                        m.books.get(&token).map(Arc::as_ref),
                        &mut next,
                    )?;
                    m.books.insert(token, Arc::new(next));
                }
            }
        }
        m.state_bytes = state_bytes;
        m.recoveries = recoveries;
        m.state_sizes.extend(size_updates);
        m.cursor = batch.validated.events().last().unwrap()["cursor"]
            .as_u64()
            .unwrap();
        m.queued += 1;
        m.queued_bytes += bytes;
        m.replay.push_back(batch);
        m.replay_bytes += bytes;
        m.snapshot_leases.retain(|_,until|*until>std::time::Instant::now());
        while m.replay.len() > self.max_batches || m.replay_bytes > self.max_bytes {
            let pinned=m.snapshot_leases.keys().next().is_some_and(|after| {
                m.replay.front().and_then(|b|b.validated.events().last())
                    .and_then(|e|e["cursor"].as_u64()).is_some_and(|end|end>*after)
            });
            if pinned && m.replay_bytes<=self.max_bytes {break;}
            let old = m.replay.pop_front().unwrap();
            m.replay_bytes -= old.bytes;
        }
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
        Ok(())
    }
    pub async fn finish(mut self) -> Result<()> {
        self.sender.take();
        let worker = self.worker.take().unwrap();
        tokio::task::spawn_blocking(move || {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("disk worker panicked"))
        })
        .await??;
        let m = self.memory.lock().unwrap();
        ensure!(
            m.error.is_none() && m.persisted == m.cursor,
            "durability drain failed: {:?}",
            m.error
        );
        Ok(())
    }
    pub fn router(&self, frame_cap: usize, clients: usize) -> Result<Router> {
        ensure!(
            self.memory.lock().unwrap().base.is_some()
                && clients > 0
                && clients <= 4
                && frame_cap >= self.batch_cap * 2,
            "explicit stream seed/client/frame bounds required"
        );
        let stream = Arc::new(Stream {
            memory: self.memory.clone(),
            changed: self.changed.clone(),
            frame_cap,
            permits: Arc::new(Semaphore::new(clients)),
        });
        Ok(Router::new().route("/", get(upgrade)).with_state(stream))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmationOutcome {
    Confirmed,
    Superseded,
    Mismatch,
}
struct Stream {
    memory: Arc<Mutex<Memory>>,
    changed: watch::Sender<u64>,
    frame_cap: usize,
    permits: Arc<Semaphore>,
}
impl Stream {
    fn window(&self, after: u64) -> Value {
        let m = self.memory.lock().unwrap();
        json!({"after_cursor":after,"current_cursor":m.cursor,"persisted_cursor":m.persisted,
            "oldest_cursor":m.replay.front().map(|b|b.validated.events()[0]["cursor"].clone()),
            "retained_batches":m.replay.len(),"retained_bytes":m.replay_bytes,
            "persistence_queue_batches":m.queued,"persistence_queue_bytes":m.queued_bytes})
    }
    fn snapshot(&self) -> Result<(Value, u64, u64)> {
        let view = MemoryView::capture(&self.memory.lock().unwrap())?;
        let cursor = view.cursor;
        let confirmation = view.confirmation_sequence;
        Ok((view.into_source_state()?, cursor, confirmation))
    }
    #[cfg(test)]
    fn page(&self, after: u64, confirmation_after: u64) -> Result<(Value, u64, u64)> {
        self.page_with_priority(after, confirmation_after, false)
    }
    fn page_with_priority(&self, after: u64, confirmation_after: u64, prefer_events: bool) -> Result<(Value, u64, u64)> {
        let m = self.memory.lock().unwrap();
        ensure!(m.source_ready, "upstream WebSocket recovery pending");
        ensure!(m.error.is_none(), "persistence failed: {:?}", m.error);
        // A freshness confirmation has no causal cursor of its own, but its
        // book still does. Never deliver a confirmed book before the consumer
        // has applied that book's causal cursor. Keep confirmation order as a
        // prefix so advancing confirmation_after cannot skip a blocked item.
        let mut pending_confirmations: Vec<_> = m
            .confirmations
            .iter()
            .filter(|(_, cursor)| **cursor > confirmation_after)
            .collect();
        pending_confirmations.sort_by_key(|(_, cursor)| **cursor);
        let confirmations: Vec<_> = pending_confirmations
            .into_iter()
            .take_while(|(token, _)| {
                m.book_cursors
                    .get(*token)
                    .is_some_and(|cursor| *cursor <= after)
            })
            .collect();
        if let Some((_, next_confirmation)) = confirmations.last().filter(|_| !(prefer_events && after < m.cursor)) {
            let next_confirmation = **next_confirmation;
            return Ok((
                json!({"schema_version":"marketcow.polymarket.live-stream.v1",
                    "type":"book_confirmations",
                    "persisted_cursor":m.persisted,
                    "persistence_queue_depth":m.queued,
                    "persistence_error":null,"derived_index_error":null,
                    "books":confirmations.into_iter().map(|(token, _)| &m.books[token]).collect::<Vec<_>>() }),
                after,
                next_confirmation,
            ));
        }
        let mut events = Vec::new();
        let mut next = after;
        let mut bytes = 0;
        for batch in &m.replay {
            if batch.validated.events().last().unwrap()["cursor"]
                .as_u64()
                .unwrap()
                <= after
            {
                continue;
            }
            if bytes + batch.bytes > self.frame_cap / 2 {
                break;
            }
            for event in batch.validated.events() {
                let at = event["cursor"].as_u64().context("cursor")?;
                if at <= after {
                    continue;
                }
                ensure!(
                    Some(at) == next.checked_add(1),
                    "memory replay expired; full resync required"
                );
                events.push(event.clone());
                next = at;
            }
            bytes += batch.bytes;
        }
        ensure!(
            after == m.cursor || next > after,
            "memory replay expired; full resync required"
        );
        let mut frame = json!({"schema_version":"marketcow.polymarket.live-stream.v1", "type":"persistence",
            "persisted_cursor":m.persisted,"persistence_queue_depth":m.queued,"persistence_error":null,"derived_index_error":null});
        if !events.is_empty() {
            frame["type"] = json!("events");
            frame["events"] = json!(events);
        }
        Ok((frame, next, confirmation_after))
    }
}
async fn upgrade(
    State(state): State<Arc<Stream>>,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    ws.max_message_size(4096)
        .on_upgrade(move |socket| async move {
            let Ok(_permit) = state.permits.clone().try_acquire_owned() else {
                return;
            };
            if let Err(error) = serve(socket, state).await {
                eprintln!("memory stream closed: {error:#}");
            }
        })
}
async fn send(socket: &mut WebSocket, frame: Value, cap: usize) -> Result<(u128,u128,usize)> {
    let started = std::time::Instant::now();
    let body = serde_json::to_string(&frame)?;
    let encode_us = started.elapsed().as_micros();
    let bytes = body.len();
    ensure!(body.len() <= cap, "frame byte cap");
    let sending = std::time::Instant::now();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        socket.send(Message::Text(body.into())),
    )
    .await??;
    Ok((encode_us,sending.elapsed().as_micros(),bytes))
}
async fn serve(mut socket: WebSocket, stream: Arc<Stream>) -> Result<()> {
    let request = tokio::time::timeout(std::time::Duration::from_secs(5), socket.recv())
        .await?
        .context("subscribe required")??;
    let Message::Text(body) = request else {
        anyhow::bail!("subscribe required")
    };
    ensure!(
        serde_json::from_str::<Value>(&body)?["type"] == "subscribe",
        "subscribe required"
    );
    let mut changed = stream.changed.subscribe();
    let stream_id = format!("{}-{}",std::process::id(),chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default());
    let mut last_report = std::time::Instant::now();
    let mut frames = 0u64;
    let mut stage_us = [0u128;3];
    let mut stage_max_us = [0u128;3];
    let mut sent_bytes = 0usize;
    // Per-consumer alternation: a confirmation frame cannot starve queued events.
    let mut prefer_events = false;
    let (state, mut cursor, mut confirmation_cursor) = stream.snapshot()?;
    send(&mut socket, state, stream.frame_cap).await?;
    send(&mut socket,json!({"schema_version":"marketcow.polymarket.live-stream.v1","type":"ready","latest_cursor":cursor}),stream.frame_cap).await?;
    loop {
        let building = std::time::Instant::now();
        let (frame, next, next_confirmation) = match stream.page_with_priority(cursor, confirmation_cursor, prefer_events) {
            Ok(page) => page,
            Err(error) => {
                eprintln!("{}",json!({"stage":"source_stream_error","at":chrono::Utc::now(),
                    "stream_id":stream_id,"window":stream.window(cursor),"error":format!("{error:#}")}));
                return Err(error);
            }
        };
        prefer_events = frame["type"] == "book_confirmations";
        let build_us = building.elapsed().as_micros();
        let (encode_us, send_us, bytes) = send(&mut socket, frame, stream.frame_cap).await?;
        frames += 1;
        sent_bytes += bytes;
        for (i,value) in [build_us,encode_us,send_us].into_iter().enumerate() {
            stage_us[i] += value;
            stage_max_us[i] = stage_max_us[i].max(value);
        }
        if last_report.elapsed() >= std::time::Duration::from_secs(5) {
            eprintln!("{}",json!({"stage":"source_stream_stages","at":chrono::Utc::now(),
                "stream_id":stream_id,"window":stream.window(next),"frames":frames,"bytes":sent_bytes,
                "interval_ms":last_report.elapsed().as_millis(),"stage_order":["build","encode","send"],
                "sum_us":stage_us,"max_us":stage_max_us}));
            frames=0; sent_bytes=0; stage_us=[0;3];stage_max_us=[0;3];last_report=std::time::Instant::now();
        }
        let advanced = next > cursor;
        let confirmation_advanced = next_confirmation > confirmation_cursor;
        cursor = next;
        confirmation_cursor = next_confirmation;
        if advanced || confirmation_advanced {
            continue;
        }
        tokio::select! {
            value = changed.changed() => {value?;},
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Ping(body))) => socket.send(Message::Pong(body)).await?,
                Some(Ok(Message::Pong(_))) => {}, _ => return Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use futures_util::{SinkExt, StreamExt};
    use marketcow_polymarket::discovery_source::{SnapshotBoundary, snapshot_event};
    fn events(after: u64) -> Vec<Value> {
        ["11","12"].iter().enumerate().map(|(i,token)| snapshot_event(
            &json!({"asset_id":token,"market":"condition","tick_size":"0.01","timestamp":"1700000000000",
                "hash":"source","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
            SnapshotBoundary {market_id:"1",condition_id:"condition",token_id:token,recovery_id:"test",
                cursor:after+i as u64+1, received_at:chrono::DateTime::from_timestamp(1700000001,0).unwrap()}).unwrap()).collect()
    }
    fn terminal_event(cursor: u64) -> Value {
        let mut terminal = events(cursor - 1).remove(0);
        terminal["event_type"] = json!("market_terminal");
        terminal["token_id"] = Value::Null;
        terminal["canonical_payload"] = json!({"retired_token_ids":["11","12"],"market":{
            "identity":{"market_id":"1","condition_id":"condition","outcomes":[{"token_id":"11"},{"token_id":"12"}]},
            "lifecycle_state":"closed","closed":true,"accepting_orders":false,"resolution":null,
            "lifecycle_source":"polymarket_gamma","terminal_at":"2026-01-01T00:00:00Z",
            "lifecycle_evidence_sha256":"a".repeat(64)}});
        terminal["canonical_payload_sha256"] =
            json!(canonical_hash(&terminal["canonical_payload"]));
        terminal.as_object_mut().unwrap().remove("event_id");
        terminal["event_id"] = json!(canonical_hash(&terminal));
        terminal
    }
    fn tokens() -> BTreeMap<String, String> {
        [("11".into(), "1".into()), ("12".into(), "1".into())].into()
    }
    fn seed() -> Value {
        json!({"latest_cursor":0,"books":[],"markets":[],"gaps":[],"schema_version":"marketcow.polymarket.live-stream.v1","type":"state"})
    }
    #[tokio::test]
    async fn admitted_identity_is_ordered_on_disk_worker_and_survives_queue_rejection() {
        let (started,wait)=mpsc::channel();
        let (release,gate)=mpsc::channel();
        let mut bound=tokens();
        let mut cursor=0;
        let mut base=seed();base["catalog_revision"]=json!("catalog");
        let mut p=Publication::start(0,Some(base),tokens(),1,65536,16384,move |batches| {
            for batch in batches {
                bound.extend(batch.catalog_bindings.clone());
                validate_source_events(batch.validated.events(),false,cursor,&bound,16384)?;
                if cursor==0 {started.send(()).unwrap();gate.recv().unwrap();}
                cursor=batch.validated.events().last().unwrap()["cursor"].as_u64().unwrap();
            }
            Ok(())
        }).unwrap();
        p.publish(events(0),None).unwrap();
        wait.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        let record=json!({"identity":{"market_id":"2","condition_id":"condition2","outcomes":[{"token_id":"21"},{"token_id":"22"}]}});
        let records=vec![record.clone()];let hash=canonical_hash(&json!(records));
        assert!(p.admit_catalog_markets("catalog",records.clone(),"wrong").is_err());
        assert!(!p.capture_view().unwrap().markets.contains_key("2"));
        p.admit_catalog_markets("catalog",records.clone(),&hash).unwrap();
        p.admit_catalog_markets("catalog",records,&hash).unwrap();
        let view=p.capture_view().unwrap();
        assert!(view.markets.contains_key("2"));
        assert!(!view.books.contains_key("21"));
        assert_eq!(view.cursor,2);assert_eq!(view.persisted_cursor,0);
        let next=|| ["21","22"].iter().enumerate().map(|(i,token)|snapshot_event(
            &json!({"asset_id":token,"market":"condition2","tick_size":"0.01","timestamp":"1700000000000","hash":"source",
                "bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
            SnapshotBoundary{market_id:"2",condition_id:"condition2",token_id:token,recovery_id:"admitted",cursor:3+i as u64,
                received_at:chrono::DateTime::from_timestamp(1700000001,0).unwrap()}).unwrap()).collect();
        assert!(p.publish(next(),None).is_err());
        assert_eq!(p.pending_bindings.len(),2);
        assert_eq!(p.cursor().unwrap(),2);
        release.send(()).unwrap();
        let mut changed=p.reader().subscribe();
        while p.persisted_cursor()<2 {changed.changed().await.unwrap();}
        p.publish(next(),None).unwrap();
        assert!(p.pending_bindings.is_empty());
        assert_eq!(p.capture_view().unwrap().books["21"]["condition_id"],"condition2");
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn scope_retirement_queues_after_books_without_waiting_for_disk() {
        let (started,wait)=mpsc::channel();let (release,gate)=mpsc::channel();
        let (observed,rows)=mpsc::channel();
        let mut p=Publication::start_managed(0,Some(seed()),tokens(),2,65536,16384,move |work| {
            match work {
                PersistenceWork::Events(batches)=>{
                    observed.send("events").unwrap();
                    assert_eq!(batches.last().unwrap().validated.events().last().unwrap()["cursor"],2);
                    started.send(()).unwrap();gate.recv().unwrap();
                },
                PersistenceWork::Retire(retirement)=>{
                    assert_eq!(retirement.after,2);
                    assert_eq!(retirement.markets,std::collections::BTreeSet::from(["1".into()]));
                    observed.send("retired").unwrap();
                },
            }
            Ok(())
        }).unwrap();
        p.publish(events(0),None).unwrap();
        wait.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        p.retire_catalog_markets(std::collections::BTreeSet::from(["1".into()])).unwrap();
        let view=p.capture_view().unwrap();
        assert_eq!(view.cursor,2);assert_eq!(view.persisted_cursor,0);assert!(view.books.is_empty());
        assert_eq!(view.queued,2);assert!(p.tokens.is_empty());
        assert!(p.publish(events(2),None).is_err());
        release.send(()).unwrap();
        p.finish().await.unwrap();
        assert_eq!(rows.into_iter().collect::<Vec<_>>(),vec!["events","retired"]);
    }

    #[tokio::test]
    async fn terminal_market_cannot_be_resurrected_by_late_book_or_gap() {
        let mut p = Publication::start(0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(()))
            .unwrap();
        p.publish(events(0), None).unwrap();
        p.publish(vec![terminal_event(3)], None).unwrap();
        let view = p.capture_view().unwrap();
        assert_eq!(view.cursor, 3);
        assert_eq!(view.markets["1"]["lifecycle_state"], "closed");

        let error = p.publish(events(3), None).unwrap_err().to_string();
        assert!(error.contains("terminal market"), "{error}");

        let mut late_gap = events(3).remove(0);
        late_gap["event_type"] = json!("recovery_started");
        late_gap["canonical_payload"] = json!({"recovery_scope":"token","token_id":"11",
            "recovery_id":"late-gap","reason":"source_connection_gap"});
        late_gap["gaps"] = json!([{"token_id":"11","code":"coverage_gap","resolved":false,
            "detected_at":"2026-01-01T00:00:00Z","event_at":null,"expected":null,
            "observed":null,"resolution":null}]);
        late_gap["canonical_payload_sha256"] =
            json!(canonical_hash(&late_gap["canonical_payload"]));
        late_gap.as_object_mut().unwrap().remove("event_id");
        late_gap["event_id"] = json!(canonical_hash(&late_gap));
        let error = p.publish(vec![late_gap], None).unwrap_err().to_string();
        assert!(error.contains("terminal market"), "{error}");
        assert_eq!(p.capture_view().unwrap().cursor, 3);
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn dependency_metadata_does_not_implicitly_subscribe_or_spend_acquisition_budget() {
        let mut base=seed();base["catalog_revision"]=json!("catalog");
        let mut p=Publication::start(0,Some(base),tokens(),2,65536,16384,|_|Ok(())).unwrap();
        let records:Vec<_>=["2","3"].into_iter().map(|id|json!({"identity":{"market_id":id,
            "condition_id":format!("c{id}"),"outcomes":[{"token_id":format!("{id}1")},{"token_id":format!("{id}2")}]}})).collect();
        let hash=canonical_hash(&json!(records));let selected=std::collections::BTreeSet::from(["2".into()]);
        assert!(p.admit_catalog_markets_scoped("catalog",records.clone(),&hash,&selected,2).is_err());
        assert!(p.capture_view().unwrap().markets.is_empty());
        p.admit_catalog_markets_scoped("catalog",records.clone(),&hash,&selected,4).unwrap();
        assert_eq!(p.tokens.len(),4);assert!(!p.tokens.contains_key("31"));
        let view=p.capture_view().unwrap();assert_eq!(view.markets.len(),2);assert!(view.books.is_empty());
        // Later promotion binds the already admitted metadata without replacing it.
        let selected=std::collections::BTreeSet::from(["3".into()]);
        p.admit_catalog_markets_scoped("catalog",records,&hash,&selected,6).unwrap();
        assert_eq!(p.tokens.len(),6);assert_eq!(p.cursor().unwrap(),0);
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn catalog_admission_cannot_steal_unsubscribed_dependency_token() {
        let mut base=seed();base["catalog_revision"]=json!("catalog");
        base["markets"]=json!([{"identity":{"market_id":"dependency","outcomes":[{"token_id":"x"},{"token_id":"y"}]}}]);
        let mut p=Publication::start(0,Some(base),tokens(),2,65536,16384,|_|Ok(())).unwrap();
        let records=vec![json!({"identity":{"market_id":"new","condition_id":"new","outcomes":[{"token_id":"x"},{"token_id":"z"}]}})];
        let hash=canonical_hash(&json!(records));
        assert!(p.admit_catalog_markets("catalog",records,&hash).unwrap_err().to_string().contains("dependency metadata"));
        assert_eq!(p.tokens,tokens());assert!(p.pending_bindings.is_empty());
        assert!(!p.capture_view().unwrap().markets.contains_key("new"));
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn expired_public_reader_does_not_poison_current_reader_or_publication() {
        // Exercises the exact MemoryReader used by both public Rust APIs, not
        // a synthetic socket queue. A separate HTTP/WS test is still required.
        let mut p=Publication::start(0,Some(seed()),tokens(),2,65536,16384, |_|Ok(())).unwrap();
        let fast=p.reader();
        let slow=p.reader();
        let mut fast_cursor=0;
        let mut changed=fast.subscribe();
        for at in (0..20).step_by(2) {
            p.publish_with_capacity(events(at)).await.unwrap();
            let page=fast.replay(fast_cursor,0,64,65536).unwrap();
            assert_eq!(page.next,at+2);
            fast_cursor=page.next;
            while p.persisted_cursor()<at+2 {changed.changed().await.unwrap();}
        }
        assert!(slow.replay(0,0,64,65536).err().unwrap().to_string().contains("expired"));
        assert_eq!(fast.capture().unwrap().cursor,20);
        assert_eq!(slow.capture().unwrap().cursor,20);
        p.publish_with_capacity(events(20)).await.unwrap();
        assert_eq!(fast.replay(20,0,64,65536).unwrap().next,22);
        assert_eq!(slow.replay(20,0,64,65536).unwrap().next,22);
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn blocked_disk_does_not_block_publication_or_memory_stream() {
        let (started, wait) = mpsc::channel();
        let (release, gate) = mpsc::channel();
        let mut first = true;
        let mut p = Publication::start(0, Some(seed()), tokens(), 4, 65536, 16384, move |_| {
            if first {
                first = false;
                started.send(()).unwrap();
                gate.recv_timeout(std::time::Duration::from_secs(3))?;
            }
            Ok(())
        })
        .unwrap();
        p.publish(events(0), None).unwrap();
        wait.recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        let start = std::time::Instant::now();
        p.publish(events(2), None).unwrap();
        let stream = Stream {
            memory: p.memory.clone(),
            changed: p.changed.clone(),
            frame_cap: 65536,
            permits: Arc::new(Semaphore::new(1)),
        };
        let (page, cursor, _) = stream.page(0, 0).unwrap();
        assert!(start.elapsed() < std::time::Duration::from_millis(100));
        assert_eq!(cursor, 4);
        assert_eq!(page["persisted_cursor"], 0);
        assert_eq!(page["events"].as_array().unwrap().len(), 4);
        let (snapshot, _, _) = stream.snapshot().unwrap();
        assert_eq!(snapshot["latest_cursor"], 4);
        assert_eq!(snapshot["persisted_cursor"], 0);
        release.send(()).unwrap();
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn queued_work_coalesces_after_blocked_disk_without_losing_cursors() {
        let (entered, wait) = mpsc::channel();
        let (release, gate) = mpsc::channel();
        let counts = Arc::new(Mutex::new(Vec::new()));
        let captured = counts.clone();
        let mut first = true;
        let mut p = Publication::start(0, None, tokens(), 8, 131072, 65536, move |group| {
            captured.lock().unwrap().push(
                group
                    .iter()
                    .map(|b| b.validated.events().len())
                    .sum::<usize>(),
            );
            if first {
                first = false;
                entered.send(())?;
                gate.recv_timeout(std::time::Duration::from_secs(3))?;
            }
            Ok(())
        })
        .unwrap();
        p.publish(events(0), None).unwrap();
        wait.recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        for after in [2, 4, 6] {
            p.publish(events(after), None).unwrap();
        }
        assert_eq!(p.cursor().unwrap(), 8);
        assert_eq!(p.persisted_cursor(), 0);
        release.send(()).unwrap();
        let memory = p.memory.clone();
        p.finish().await.unwrap();
        assert_eq!(*counts.lock().unwrap(), vec![2, 6]);
        let m = memory.lock().unwrap();
        assert_eq!((m.persisted, m.queued, m.queued_bytes), (8, 0, 0));
    }
    #[tokio::test]
    async fn capacity_wait_resumes_without_dropping_or_advancing_early() {
        let (release, gate) = mpsc::channel();
        let mut first = true;
        let mut p = Publication::start(0, None, tokens(), 1, 65536, 16384, move |_| {
            if first {
                first = false;
                gate.recv_timeout(std::time::Duration::from_secs(3))?;
            }
            Ok(())
        })
        .unwrap();
        p.publish_with_capacity(events(0)).await.unwrap();
        let memory = p.memory.clone();
        {
            let waiting = p.publish_with_capacity(events(2));
            tokio::pin!(waiting);
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiting)
                    .await
                    .is_err()
            );
            assert_eq!(memory.lock().unwrap().cursor, 2);
            release.send(()).unwrap();
            waiting.await.unwrap();
        }
        assert_eq!(memory.lock().unwrap().cursor, 4);
        p.finish().await.unwrap();
        assert_eq!(memory.lock().unwrap().persisted, 4);
    }
    #[tokio::test]
    async fn token_gap_is_streamed_without_stopping_other_books_or_snapshot() {
        use marketcow_polymarket::discovery_source::canonical_hash;
        let mut p =
            Publication::start(0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(())).unwrap();
        p.publish(events(0), None).unwrap();
        let mut status = events(2).remove(0);
        status["event_type"] = json!("recovery_started");
        status["canonical_payload"] = json!({"recovery_scope":"token","token_id":"11","recovery_id":"attempt1","reason":"source_data_delayed"});
        status["gaps"] = json!([{"token_id":"11","code":"coverage_gap","resolved":false,
            "detected_at":"2026-01-01T00:00:00Z","event_at":null,"expected":null,"observed":null,"resolution":null}]);
        status["canonical_payload_sha256"] = json!(canonical_hash(&status["canonical_payload"]));
        status.as_object_mut().unwrap().remove("event_id");
        status["event_id"] = json!(canonical_hash(&status));
        p.publish_with_capacity(vec![status]).await.unwrap();
        let mut healthy = events(2).remove(1);
        healthy["raw_payload"]["event_type"] = json!("book");
        healthy["raw_payload_sha256"] = json!(canonical_hash(&healthy["raw_payload"]));
        healthy.as_object_mut().unwrap().remove("event_id");
        healthy["event_id"] = json!(canonical_hash(&healthy));
        p.publish_with_capacity(vec![healthy]).await.unwrap();
        let stream = Stream {
            memory: p.memory.clone(),
            changed: p.changed.clone(),
            frame_cap: 131072,
            permits: Arc::new(Semaphore::new(1)),
        };
        let (page, cursor, _) = stream.page(2, 0).unwrap();
        assert_eq!(cursor, 4);
        assert_eq!(page["events"][0]["event_type"], "recovery_started");
        assert_eq!(page["events"][1]["token_id"], "12");
        let (state, _, _) = stream.snapshot().unwrap();
        assert_eq!(state["gaps"].as_array().unwrap().len(), 1);
        assert_eq!(state["gaps"][0]["token_id"], "11");
        assert_eq!(state["books"].as_array().unwrap().len(), 2);
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn queue_limit_includes_inflight_and_does_not_advance_cursor() {
        let (release, gate) = mpsc::channel();
        let mut p = Publication::start(0, None, tokens(), 1, 16384, 16384, move |_| {
            gate.recv_timeout(std::time::Duration::from_secs(3))?;
            Ok(())
        })
        .unwrap();
        p.publish(events(0), None).unwrap();
        assert!(
            p.publish(events(2), None)
                .unwrap_err()
                .to_string()
                .contains("capacity")
        );
        assert_eq!(p.cursor().unwrap(), 2);
        release.send(()).unwrap();
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn disk_failure_is_visible_and_drain_fails() {
        let mut p = Publication::start(0, None, tokens(), 4, 65536, 16384, |_| {
            anyhow::bail!("injected disk failure")
        })
        .unwrap();
        let mut changed = p.changed.subscribe();
        p.publish(events(0), None).unwrap();
        loop {
            if p.memory.lock().unwrap().error.is_some() {
                break;
            }
            changed.changed().await.unwrap();
        }
        assert!(p.cursor().is_err());
        assert!(p.publish(events(2), None).is_err());
        assert!(p.finish().await.is_err());
    }
    #[tokio::test]
    async fn byte_budget_rejects_before_item_budget() {
        let batch = events(0);
        let cap = validate_source_events(&batch, false, 0, &tokens(), 65536)
            .unwrap()
            .iter()
            .map(Vec::len)
            .sum::<usize>();
        let (release, gate) = mpsc::channel();
        let mut p = Publication::start(0, None, tokens(), 100, cap, cap, move |_| {
            gate.recv_timeout(std::time::Duration::from_secs(3))?;
            Ok(())
        })
        .unwrap();
        p.publish(batch, None).unwrap();
        assert!(
            p.publish(events(2), None)
                .unwrap_err()
                .to_string()
                .contains("capacity")
        );
        assert_eq!(p.cursor().unwrap(), 2);
        release.send(()).unwrap();
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn corrupt_batch_never_publishes_or_reaches_disk() {
        let mut p = Publication::start(0, None, tokens(), 4, 65536, 16384, |_| {
            panic!("invalid event persisted")
        })
        .unwrap();
        let mut batch = events(0);
        batch[0]["cursor"] = json!(7);
        assert!(p.publish(batch, None).is_err());
        assert_eq!(p.cursor().unwrap(), 0);
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn bounded_disk_eviction_does_not_interrupt_connected_memory_ws() {
        use marketcow_runtime::discovery_source::PreparedSourceWriter;
        use rusqlite::{Connection, params};
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("indexes")).unwrap();
        std::fs::write(root.path().join("events.jsonl"), b"").unwrap();
        let db = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        db.execute_batch("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE gaps(resolved INTEGER NOT NULL);
            CREATE TABLE books(token_id TEXT PRIMARY KEY,market_id TEXT,cursor INTEGER,payload_json BLOB,payload_sha256 TEXT);
            CREATE TABLE book_confirmations(token_id TEXT PRIMARY KEY);
            CREATE TABLE event_offsets(cursor INTEGER PRIMARY KEY,byte_offset INTEGER,byte_length INTEGER,event_id TEXT,market_id TEXT,token_id TEXT,line_sha256 TEXT);").unwrap();
        for (k,v) in [("schema_version","marketcow.polymarket.state-index.v1"),
            ("latest_cursor","0"),("event_log_size","0"),("unresolved_gap_count","0"),
            ("catalog_revision","catalog"),("active_recovery_id","")] {
            db.execute("INSERT INTO metadata VALUES(?,?)",[k,v]).unwrap();
        }
        for event in events(0) {
            let book = &event["canonical_payload"];
            let bytes = serde_json::to_vec(book).unwrap();
            db.execute("INSERT INTO books VALUES(?,?,0,?,?)",params![
                event["token_id"].as_str().unwrap(),event["market_id"].as_str().unwrap(),
                bytes,canonical_hash(book)]).unwrap();
        }
        let mut writer = PreparedSourceWriter::open(root.path(),16384).unwrap();
        writer.append_market_batch(&events(0)).unwrap();
        writer.enable_bounded_history(16384).unwrap();
        std::fs::remove_file(root.path().join("events.jsonl")).unwrap();
        let mut base = seed();
        base["latest_cursor"] = json!(2); base["persisted_cursor"] = json!(2);
        base["books"] = json!(events(0).into_iter().map(|e|e["canonical_payload"].clone()).collect::<Vec<_>>());
        let mut p = Publication::start(2,Some(base),tokens(),8,131072,16384,move |batches| {
            writer.append_validated_group(&batches.iter().map(|b|&b.validated).collect::<Vec<_>>())
        }).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = p.router(131072,1).unwrap();
        let server = tokio::spawn(async move { axum::serve(listener,router).await.unwrap() });
        let (mut ws,_) = tokio_tungstenite::connect_async(format!("ws://{addr}/")).await.unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(json!({"type":"subscribe"}).to_string().into())).await.unwrap();
        for _ in 0..2 { ws.next().await.unwrap().unwrap(); }
        let mut cursor = 2;
        for after in (2..102).step_by(2) {
            p.publish_with_capacity(events(after)).await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(2),async {
                while cursor < after+2 {
                    let frame: Value = serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
                    assert_ne!(frame["type"],"error");
                    if let Some(items) = frame["events"].as_array() {
                        for item in items { cursor+=1; assert_eq!(item["cursor"],cursor); }
                    }
                }
            }).await.unwrap();
        }
        p.finish().await.unwrap();
        let floor: String = db.query_row("SELECT value FROM metadata WHERE key='history_floor_cursor'",[],|r|r.get(0)).unwrap();
        assert!(floor.parse::<u64>().unwrap()>2);
        assert_eq!(cursor,102);
        assert!(!root.path().join("events.jsonl").exists());
        assert_eq!(PreparedSourceWriter::open(root.path(),16384).unwrap().cursor().unwrap(),102);
        ws.close(None).await.unwrap(); server.abort();
    }

    #[tokio::test]
    async fn raw_websocket_delivers_before_blocked_disk_completes() {
        let (release, gate) = mpsc::channel();
        let mut p = Publication::start(0, Some(seed()), tokens(), 4, 65536, 16384, move |_| {
            gate.recv_timeout(std::time::Duration::from_secs(3))?;
            Ok(())
        })
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = p.router(65536, 1).unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/"))
            .await
            .unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            json!({"type":"subscribe"}).to_string().into(),
        ))
        .await
        .unwrap();
        for _ in 0..2 {
            ws.next().await.unwrap().unwrap();
        }
        let start = std::time::Instant::now();
        p.publish(events(0), None).unwrap();
        let frame = tokio::time::timeout(std::time::Duration::from_millis(100), async {
            loop {
                let frame: Value =
                    serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap())
                        .unwrap();
                if frame["type"] == "events" {
                    break frame;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(frame["persisted_cursor"], 0);
        assert_eq!(frame["events"][0]["cursor"], 1);
        assert_eq!(frame["events"][1]["cursor"], 2);
        eprintln!(
            "blocked-disk raw WS latency_us={} published=2 persisted=0",
            start.elapsed().as_micros()
        );
        release.send(()).unwrap();
        p.finish().await.unwrap();
        task.abort();
    }
    #[tokio::test]
    async fn matching_rest_pair_refreshes_memory_without_cursor_or_disk_event() {
        let mut p =
            Publication::start(0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(())).unwrap();
        p.publish(events(0), None).unwrap();
        let mut changed = p.changed.subscribe();
        while p.persisted_cursor() < 2 {
            changed.changed().await.unwrap();
        }
        let raw = vec![
            json!({"asset_id":"11","market":"condition","tick_size":"0.01","timestamp":"1700000000000","hash":"confirm-11","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
            json!({"asset_id":"12","market":"condition","tick_size":"0.01","timestamp":"1700000000000","hash":"confirm-12","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
        ];
        let tokens = ["11".to_owned(), "12".to_owned()];
        let requested = chrono::DateTime::from_timestamp(1_700_000_002, 0).unwrap();
        let received = chrono::DateTime::from_timestamp(1_700_000_003, 0).unwrap();
        let before_confirmation = p.capture_view().unwrap();
        let old_book = before_confirmation.books["11"].clone();
        assert_eq!(
            p.confirm_market("1", "condition", &tokens, &raw, requested, received)
                .unwrap(),
            ConfirmationOutcome::Confirmed
        );
        assert_eq!((p.cursor().unwrap(), p.persisted_cursor()), (2, 2));
        let after_confirmation = p.capture_view().unwrap();
        assert_eq!(before_confirmation.confirmation_sequence, 0);
        assert_eq!(after_confirmation.confirmation_sequence, 2);
        assert!(Arc::ptr_eq(&old_book, &before_confirmation.books["11"]));
        assert!(!Arc::ptr_eq(&old_book, &after_confirmation.books["11"]));
        assert!(old_book["confirmed_at"].is_null());
        assert!(!after_confirmation.books["11"]["confirmed_at"].is_null());
        // Same-content responses that arrive at this stage late cannot roll
        // back timestamps or allocate a new confirmation sequence/cursor.
        for stale in [requested, received] {
            assert_eq!(p.confirm_market("1", "condition", &tokens, &raw, requested, stale).unwrap(),
                ConfirmationOutcome::Superseded);
            let unchanged = p.capture_view().unwrap();
            assert_eq!(unchanged.confirmation_sequence, after_confirmation.confirmation_sequence);
            assert_eq!(unchanged.cursor, after_confirmation.cursor);
            assert_eq!(unchanged.books, after_confirmation.books);
        }
        let page = p.reader().replay(0,0,1,131072).unwrap();
        assert_eq!(page.next,2);
        assert!(page.caught_up);
        assert_eq!(page.confirmation_sequence,2);
        assert_eq!(page.confirmation_books.len(),2);
        assert_eq!(page.batches[0].validated.events().last().unwrap()["cursor"],2);
        let no_repeat = p.reader().replay(2,2,1,131072).unwrap();
        assert!(no_repeat.batches.is_empty());
        assert!(no_repeat.confirmation_books.is_empty());
        assert!(p.reader().replay(3,2,1,131072).is_err());
        assert!(p.reader().replay(0,0,1,1).is_err());
        let stream = Stream {
            memory: p.memory.clone(),
            changed: p.changed.clone(),
            frame_cap: 131072,
            permits: Arc::new(Semaphore::new(1)),
        };
        let (frame, cursor, confirmation_cursor) = stream.page(2, 0).unwrap();
        assert_eq!(cursor, 2);
        assert_eq!(confirmation_cursor, 2);
        assert_eq!(frame["type"], "book_confirmations");
        assert_eq!(frame["books"].as_array().unwrap().len(), 2);
        assert_eq!(
            frame["books"][0]["confirmation_evidence_sha256"],
            canonical_hash(&raw[0])
        );
        assert_ne!(
            frame["books"][0]["confirmation_evidence_sha256"],
            raw[0]["hash"]
        );
        assert_eq!(frame["books"][0]["received_at"], "2023-11-14T22:13:23Z");
        let (causal, event_cursor, confirmation_cursor) = stream.page(0, 0).unwrap();
        assert_eq!(causal["type"], "events");
        assert_eq!(event_cursor, 2);
        assert_eq!(confirmation_cursor, 0);
        let (priority, event_cursor, confirmation_cursor) =
            stream.page(event_cursor, confirmation_cursor).unwrap();
        assert_eq!(priority["type"], "book_confirmations");
        assert_eq!(event_cursor, 2);
        assert_eq!(confirmation_cursor, 2);
        // The venue sends last trades independently from order-book changes.
        // A REST book may therefore have the same levels while its trade field
        // lags the newer WS value.  That still confirms book freshness and
        // must not overwrite the causally newer trade.
        {
            let mut memory = p.memory.lock().unwrap();
            Arc::make_mut(memory.books.get_mut("11").unwrap())["last_trade_price"] = json!("0.55");
            Arc::make_mut(memory.books.get_mut("11").unwrap())["exchange_at"] = json!("2023-11-14T22:13:24Z");
        }
        assert_eq!(
            p.confirm_market(
                "1",
                "condition",
                &tokens,
                &raw,
                chrono::DateTime::from_timestamp(1_700_000_005, 0).unwrap(),
                chrono::DateTime::from_timestamp(1_700_000_006, 0).unwrap(),
            )
            .unwrap(),
            ConfirmationOutcome::Confirmed
        );
        let (confirmation, _, next_confirmation_cursor) =
            stream.page(2, confirmation_cursor).unwrap();
        assert_eq!(confirmation["type"], "book_confirmations");
        assert_eq!(confirmation["books"][0]["last_trade_price"], "0.55");
        assert_eq!(
            confirmation["books"][0]["exchange_at"],
            "2023-11-14T22:13:24Z"
        );
        assert_eq!(next_confirmation_cursor, confirmation_cursor + 2);
        let mut divergent = raw.clone();
        divergent[0]["bids"][0]["size"] = json!("9");
        assert_eq!(
            p.confirm_market(
                "1",
                "condition",
                &tokens,
                &divergent,
                chrono::DateTime::from_timestamp(1_700_000_007, 0).unwrap(),
                chrono::DateTime::from_timestamp(1_700_000_008, 0).unwrap()
            )
            .unwrap(),
            ConfirmationOutcome::Mismatch
        );
        // A newer, divergent WS state for one token must not prevent its quiet,
        // exactly matching sibling from receiving an independent freshness
        // confirmation. The newer token itself is never overwritten.
        let mut partly_superseded = raw.clone();
        partly_superseded[0]["bids"][0]["size"] = json!("9");
        assert_eq!(
            p.confirm_market(
                "1",
                "condition",
                &tokens,
                &partly_superseded,
                chrono::DateTime::from_timestamp(1_700_000_001, 0).unwrap(),
                chrono::DateTime::from_timestamp(1_700_000_007, 0).unwrap()
            )
            .unwrap(),
            ConfirmationOutcome::Confirmed
        );
        let (_, _, partial_confirmation_cursor) = stream.page(2, next_confirmation_cursor).unwrap();
        assert_eq!(partial_confirmation_cursor, next_confirmation_cursor + 1);
        divergent[1]["bids"][0]["size"] = json!("9");
        assert_eq!(
            p.confirm_market(
                "1",
                "condition",
                &tokens,
                &divergent,
                chrono::DateTime::from_timestamp(1_700_000_001, 0).unwrap(),
                chrono::DateTime::from_timestamp(1_700_000_006, 0).unwrap()
            )
            .unwrap(),
            ConfirmationOutcome::Superseded
        );
        // Venue time fences a REST snapshot older than the current WS state,
        // even when the WS frame arrived before this request began.
        let mut stale = divergent.clone();
        stale[0]["timestamp"] = json!("1699999999000");
        stale[1]["timestamp"] = json!("1699999999000");
        assert_eq!(
            p.confirm_market(
                "1",
                "condition",
                &tokens,
                &stale,
                chrono::DateTime::from_timestamp(1_700_000_010, 0).unwrap(),
                chrono::DateTime::from_timestamp(1_700_000_011, 0).unwrap()
            )
            .unwrap(),
            ConfirmationOutcome::Superseded
        );
        // A newly opened targeted official WS stream is itself the current
        // baseline. Its venue timestamp may be older than the main stream's
        // last mutation, so only a concurrent local receipt fences it.
        assert_eq!(
            p.confirm_targeted_market(
                "1",
                "condition",
                &tokens,
                &stale,
                chrono::DateTime::from_timestamp(1_700_000_010, 0).unwrap(),
                chrono::DateTime::from_timestamp(1_700_000_011, 0).unwrap()
            )
            .unwrap(),
            ConfirmationOutcome::Mismatch
        );
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn websocket_precision_is_canonicalized_for_confirmation_boundary() {
        let mut p =
            Publication::start(0, Some(seed()), tokens(), 8, 131072, 65536, |_| Ok(())).unwrap();
        p.publish(events(0), None).unwrap();
        let raw = vec![
            json!({"asset_id":"11","market":"condition","tick_size":"0.01","timestamp":"1700000000000","hash":"confirm-11","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
            json!({"asset_id":"12","market":"condition","tick_size":"0.01","timestamp":"1700000000000","hash":"confirm-12","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
        ];
        let tokens = ["11".to_owned(), "12".to_owned()];
        let requested = chrono::DateTime::from_timestamp(1_700_000_002, 123_456_789).unwrap();
        let received = chrono::DateTime::from_timestamp(1_700_000_003, 987_654_321).unwrap();
        assert_eq!(
            p.confirm_market("1", "condition", &tokens, &raw, requested, received)
                .unwrap(),
            ConfirmationOutcome::Confirmed
        );
        let stream = Stream {
            memory: p.memory.clone(),
            changed: p.changed.clone(),
            frame_cap: 131072,
            permits: Arc::new(Semaphore::new(1)),
        };
        let (frame, _, _) = stream.page(2, 0).unwrap();
        assert_eq!(
            frame["books"][0]["received_at"],
            "2023-11-14T22:13:23.987654Z"
        );
        p.finish().await.unwrap();
    }
    #[test]
    fn only_full_book_mutations_advance_the_confirmation_fence() {
        assert!(advances_book_confirmation_fence(Some("book")));
        assert!(advances_book_confirmation_fence(Some("price_change")));
        assert!(advances_book_confirmation_fence(Some("tick_size_change")));
        assert!(!advances_book_confirmation_fence(Some("best_bid_ask")));
        assert!(!advances_book_confirmation_fence(Some("last_trade_price")));
    }
    #[test]
    fn non_book_event_cannot_roll_back_confirmed_book_freshness() {
        let current = json!({"received_at":"2026-09-06T01:02:03.900000Z"});
        let mut trade = json!({"received_at":"2026-09-06T01:01:00Z","last_trade_price":"0.5"});
        preserve_confirmed_book_freshness(Some("last_trade_price"), Some(&current), &mut trade)
            .unwrap();
        assert_eq!(trade["received_at"], current["received_at"]);

        let mut book = json!({"received_at":"2026-09-06T01:01:00Z"});
        preserve_confirmed_book_freshness(Some("price_change"), Some(&current), &mut book).unwrap();
        assert_eq!(book["received_at"], "2026-09-06T01:01:00Z");
    }
    #[tokio::test]
    async fn replay_eviction_requires_resync_instead_of_skipping() {
        let mut p =
            Publication::start(0, Some(seed()), tokens(), 1, 16384, 16384, |_| Ok(())).unwrap();
        p.publish(events(0), None).unwrap();
        let mut changed = p.changed.subscribe();
        while p.persisted_cursor() < 2 {
            changed.changed().await.unwrap();
        }
        p.publish(events(2), None).unwrap();
        let stream = Stream {
            memory: p.memory.clone(),
            changed: p.changed.clone(),
            frame_cap: 65536,
            permits: Arc::new(Semaphore::new(1)),
        };
        assert!(
            stream
                .page(0, 0)
                .unwrap_err()
                .to_string()
                .contains("resync")
        );
        assert_eq!(stream.page(2, 0).unwrap().1, 4);
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn snapshot_lease_survives_count_eviction_then_expires_without_blocking_source() {
        let mut p=Publication::start(0,Some(seed()),tokens(),1,65536,16384, |_|Ok(())).unwrap();
        let reader=p.reader();
        assert_eq!(reader.capture_for_resume(std::time::Duration::from_secs(5),1).unwrap().cursor,0);
        let mut changed=p.changed.subscribe();
        for at in [0,2] {
            p.publish(events(at),None).unwrap();
            while p.persisted_cursor()<at+2 {changed.changed().await.unwrap();}
        }
        assert_eq!(reader.replay(0,0,64,65536).unwrap().next,4);
        assert!(reader.capture_for_resume(std::time::Duration::from_secs(5),1).is_err());
        p.memory.lock().unwrap().snapshot_leases.insert(0,std::time::Instant::now());
        p.publish(events(4),None).unwrap();
        assert!(reader.replay(0,0,64,65536).is_err());
        assert_eq!(reader.replay(4,0,64,65536).unwrap().next,6);
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn snapshot_lease_never_exceeds_existing_byte_cap() {
        let mut p=Publication::start(0,Some(seed()),tokens(),1,16384,16384, |_|Ok(())).unwrap();
        let reader=p.reader();
        reader.capture_for_resume(std::time::Duration::from_secs(60),1).unwrap();
        let mut changed=p.changed.subscribe();
        for at in (0..24).step_by(2) {
            p.publish(events(at),None).unwrap();
            while p.persisted_cursor()<at+2 {changed.changed().await.unwrap();}
            assert!(p.memory.lock().unwrap().replay_bytes<=16384);
        }
        assert!(reader.replay(0,0,64,16384).is_err());
        assert_eq!(reader.capture().unwrap().cursor,24);
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_audit_does_not_reenable_public_baseline() {
        let p=Publication::start(0,Some(seed()),tokens(),4,65536,16384, |_|Ok(())).unwrap();
        p.set_source_ready(false);
        assert!(p.capture_view().is_err());
        assert_eq!(p.shutdown_recovery_facts().unwrap().0,0);
        assert!(p.reader().capture().is_err());
        p.finish().await.unwrap();
    }
    #[tokio::test]
    async fn intentional_pause_fences_old_resume_without_creating_a_gap() {
        let mut p=Publication::start(0,Some(seed()),tokens(),4,65536,16384, |_|Ok(())).unwrap();
        p.publish(events(0),None).unwrap();
        let mut changed=p.changed.subscribe();while p.persisted_cursor()<2{changed.changed().await.unwrap();}
        let reader=p.reader();assert!(reader.replay(0,0,64,65536).is_ok());
        assert!(p.fresh_acquisition_started(&BTreeSet::from(["11".into(),"12".into()])));
        p.pause_source().unwrap();
        assert!(!p.fresh_acquisition_started(&BTreeSet::from(["11".into(),"12".into()])));
        assert!(reader.installed_markets().is_ok());
        assert!(reader.capture().is_err());assert!(reader.replay(0,0,64,65536).is_err());
        assert!(p.shutdown_recovery_facts().unwrap().1.is_empty());
        p.set_source_ready(true);assert!(reader.capture().unwrap().books.is_empty());
        assert!(reader.replay(0,0,64,65536).is_err());
        p.finish().await.unwrap();
    }

    #[tokio::test]
    async fn continuous_confirmations_cannot_starve_pending_events() {
        let mut bindings=tokens(); bindings.insert("13".into(),"2".into()); bindings.insert("14".into(),"2".into());
        let mut p = Publication::start(0,Some(seed()),bindings,8,131072,16384, |_| Ok(())).unwrap();
        p.publish(events(0),None).unwrap();
        let other = ["13","14"].iter().enumerate().map(|(i,token)| snapshot_event(
            &json!({"asset_id":token,"market":"condition2","tick_size":"0.01","timestamp":"1700000000000",
                "hash":"source","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
            SnapshotBoundary{market_id:"2",condition_id:"condition2",token_id:token,recovery_id:"test",
                cursor:3+i as u64,received_at:chrono::DateTime::from_timestamp(1700000001,0).unwrap()}).unwrap()).collect();
        p.publish(other,None).unwrap();
        // Synthetic confirmation on unchanged token12 is causally eligible at2,
        // while another market's atomic pair is pending. No live evidence claim.
        p.memory.lock().unwrap().confirmations.insert("12".into(),1);
        let stream = Stream{memory:p.memory.clone(),changed:p.changed.clone(),frame_cap:65536,permits:Arc::new(Semaphore::new(1))};
        let (confirmation,after,c) = stream.page_with_priority(2,0,false).unwrap();
        assert_eq!(confirmation["type"],"book_confirmations");
        assert_eq!(after,2);
        p.memory.lock().unwrap().confirmations.insert("12".into(),2);
        let (event,after,c2) = stream.page_with_priority(after,c,true).unwrap();
        assert_eq!(event["type"],"events"); assert_eq!(after,4); assert_eq!(c2,c);
        assert_eq!(stream.page_with_priority(after,c2,false).unwrap().0["type"],"book_confirmations");
        let window = stream.window(2);
        assert_eq!(window["current_cursor"],4);
        assert_eq!(window["retained_batches"],2);
        p.finish().await.unwrap();
    }
}
