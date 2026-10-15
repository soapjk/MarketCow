//! Market FIFO actors own their publication lanes. Queue exhaustion is an
//! explicit market invalidation barrier; results from before it cannot land.
use super::*;
use crate::source_dispatch::{Completion, Dispatcher, MarketCompletions};
use crate::source_publication::{MarketPublisher, PreparedProjection,
    SourceBatchTiming, prepare_market_projection};
use crate::source_market_persistence::MarketCapacityExhausted;
use marketcow_polymarket::RawTransportFrame;
use marketcow_runtime::discovery_source::ValidatedSourceBatch;
use std::collections::VecDeque;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

const INPUT_BYTES: usize = 64 * 1024 * 1024;
const FRAME_BYTES: usize = 8 * 1024 * 1024;
// One market cannot consume the final two maximum-size input frames needed
// by the other current/next/retiring market lanes.
const MARKET_INPUT_BYTES: usize = INPUT_BYTES - 2 * FRAME_BYTES;
pub(super) const OUTPUT_BYTES: usize = 16 * 1024 * 1024;
// The upstream emits liquid-market microbursts far above the five-second
// average. A tiny record count used to reject at ~34 KiB despite the explicit
// 64 MiB shared byte budget, creating false source gaps. Keep both limits: a
// finite 4096 records per market for roughly ten seconds of measured burst
// absorption and the stricter shared
// byte semaphore for actual memory consumption.
const MARKET_QUEUE_CAPACITY: usize = 4096;
// Amortize blocking-pool scheduling for a liquid market. Entity ordering is
// unchanged: one actor still owns and reduces one market serially.
// One actor still owns a market and preserves its input order. A larger CPU
// batch amortizes spawn-blocking and semaphore scheduling during liquid BTC5m
// bursts without introducing intra-market parallelism.
// One market must not reserve half of the 128-slot completion channel while
// the owner is publishing a burst. Keep batching, but release owner-visible
// results in smaller FIFO chunks, reducing cross-market head-of-line waits.
const ACTOR_CPU_BATCH: usize = 16;
const INPUT_FRAME_BATCH: usize = 64;

// Rejected admissions are already fail-closed. Keep the additional evidence
// bounded even if one market repeatedly hits the same limit during recovery.
static CAPACITY_REJECTION_LOGS: [AtomicU64; 9] =
    [const { AtomicU64::new(0) }; 9];

fn capacity_log_attempt(limiter: &str) -> u64 {
    let index = match limiter {
        "persistence_market_item_bytes" => 0,
        "persistence_aggregate_item_bytes" => 1,
        "persistence_market_batches" => 2,
        "persistence_aggregate_batches" => 3,
        "persistence_market_bytes" => 4,
        "persistence_aggregate_bytes" => 5,
        "replay_market_item_bytes" => 6,
        "replay_aggregate_item_bytes" => 7,
        _ => 8,
    };
    CAPACITY_REJECTION_LOGS[index].fetch_add(1, Ordering::Relaxed) + 1
}

fn next_market_input_pass(
    grouped: &mut BTreeMap<String, VecDeque<RawTransportFrame>>,
) -> Vec<(String, Vec<RawTransportFrame>)> {
    let markets = grouped.keys().cloned().collect::<Vec<_>>();
    let mut pass = Vec::with_capacity(markets.len());
    for market in markets {
        let pending = grouped.get_mut(&market).expect("market pending in pass");
        let frames = pending.drain(..pending.len().min(INPUT_FRAME_BATCH))
            .collect::<Vec<_>>();
        if pending.is_empty() { grouped.remove(&market); }
        pass.push((market, frames));
    }
    pass
}

#[cfg(test)]
mod capacity_tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_burst_window_fits_next_rejected_and_fifo_preserved() {
        let (mut dispatch, mut output) = Dispatcher::start(
            [("market".into(), ())].into(),
            MARKET_QUEUE_CAPACITY,
            6,
            12,
            |_, item: usize| Ok(item),
        )
        .unwrap();
        for item in 0..MARKET_QUEUE_CAPACITY {
            dispatch.try_submit("market", item).unwrap();
        }
        assert_eq!(
            dispatch.occupancy(),
            (MARKET_QUEUE_CAPACITY, MARKET_QUEUE_CAPACITY, 6, 0)
        );
        assert_eq!(
            dispatch.try_submit("market", MARKET_QUEUE_CAPACITY),
            Err(MARKET_QUEUE_CAPACITY)
        );
        dispatch.close();
        for expected in 0..MARKET_QUEUE_CAPACITY {
            assert_eq!(output.recv().await.unwrap().result.unwrap(), expected);
        }
        dispatch.join().await;
        assert!(output.recv().await.is_none());
    }
}

#[derive(Clone, Default)]
struct Control {
    generation: u64,
    attempts: BTreeMap<String, String>,
    // Once any post-barrier input is accepted, another loss must fence it.
    // Before that, repeated capacity rejection cannot invalidate a new book.
    baseline_admitted: bool,
    pending_barrier: Vec<Value>,
}
struct State {
    adapter: Adapter,
    generation: u64,
    control: Arc<Mutex<Control>>,
    publisher: Arc<MarketPublisher>,
    trace_budget: (std::time::Instant, u32, u32),
}
struct Input {
    handoff_receipt: Option<tokio::sync::oneshot::Sender<bool>>,
    generation: u64,
    frames: Vec<RawTransportFrame>,
    admitted_at: chrono::DateTime<Utc>,
    admitted_mono: std::time::Instant,
    recovery: Vec<(String, String)>,
    rest_recovery: Option<Vec<Value>>,
    replacement_reason: Option<String>,
    _bytes: OwnedSemaphorePermit,
    _market_bytes: OwnedSemaphorePermit,
}
pub(super) struct Output {
    generation: u64,
    published_groups: usize,
    #[cfg(test)]
    published_event_types: Vec<String>,
    recovery_transitions: Vec<(bool, String, String)>,
}
struct ActorObservation {
    validated: ValidatedSourceBatch,
    materialized_books: BTreeMap<u64, Value>,
    prepared: PreparedProjection,
}
#[derive(Clone, Copy)]
struct BatchTrace {
    raw_batch_first_received_at: chrono::DateTime<Utc>,
    raw_batch_last_received_at: chrono::DateTime<Utc>,
    admitted_at: chrono::DateTime<Utc>,
    actor_started_at: chrono::DateTime<Utc>,
    actor_completed_at: chrono::DateTime<Utc>,
    admitted_mono: std::time::Instant,
    actor_started_mono: std::time::Instant,
    actor_completed_mono: std::time::Instant,
}
pub(super) struct Pipeline {
    dispatch: Dispatcher<Input, State, Output>,
    controls: BTreeMap<String, Arc<Mutex<Control>>>,
    market_conditions: BTreeMap<String, String>,
    bytes: Arc<Semaphore>,
    market_bytes: BTreeMap<String, Arc<Semaphore>>,
    admission: Mutex<(std::time::Instant, BTreeMap<&'static str, u64>)>,
    timing: Arc<PipelineTiming>,
}

#[derive(Default)]
struct TimingSample {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

impl TimingSample {
    fn record(&self, elapsed: std::time::Duration) {
        let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(us, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
    }

    fn take(&self) -> Value {
        json!({
            "count":self.count.swap(0, Ordering::Relaxed),
            "total_us":self.total_us.swap(0, Ordering::Relaxed),
            "max_us":self.max_us.swap(0, Ordering::Relaxed),
        })
    }
}

#[derive(Default)]
struct PipelineTiming {
    actor_cpu: TimingSample,
    completion_wait: TimingSample,
    owner_publish: TimingSample,
    owner_prepare: TimingSample,
    owner_publication: TimingSample,
    owner_finalize: TimingSample,
}

struct Timed<'a> {
    started: std::time::Instant,
    sample: &'a TimingSample,
}

impl<'a> Timed<'a> {
    fn new(sample: &'a TimingSample) -> Self {
        Self { started: std::time::Instant::now(), sample }
    }
}

impl Drop for Timed<'_> {
    fn drop(&mut self) {
        self.sample.record(self.started.elapsed());
    }
}

// Measure without allocating another serialized copy of each full order book.
fn bounded_size(value: &impl serde::Serialize, limit: usize) -> Result<usize> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, value: &[u8]) -> std::io::Result<usize> {
            if value.len() > self.limit.saturating_sub(self.bytes) {
                return Err(std::io::Error::other("frame byte limit"));
            }
            self.bytes += value.len();
            Ok(value.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.bytes.max(1))
}

impl Pipeline {
    pub(super) fn take_hot_path_timing(&self) -> Value {
        json!({
            "actor_cpu":self.timing.actor_cpu.take(),
            "completion_wait":self.timing.completion_wait.take(),
            "owner_publish":self.timing.owner_publish.take(),
            "owner_prepare":self.timing.owner_prepare.take(),
            "owner_publication":self.timing.owner_publication.take(),
            "owner_finalize":self.timing.owner_finalize.take(),
            "actor_wait":self.dispatch.take_wait_timing(),
        })
    }

    pub(super) fn has_market(&self, market: &str) -> bool {
        self.controls.contains_key(market)
    }
    /// Connect catalog admission to the actual reducer pool, not just a list
    /// in the API. No subscription is sent until this method succeeds.
    pub(super) fn install_admitted_market(
        &mut self,
        router: &mut Adapter,
        publication: &Publication,
        market: &super::super::Market,
    ) -> Result<bool> {
        // Catalog admission is a control-plane operation and must also work
        // while on-demand acquisition is intentionally paused.  Requiring a
        // public market-data snapshot here creates a bootstrap cycle: the
        // source cannot become ready until the admitted pair is installed and
        // subscribed, while capture_view deliberately rejects until the source
        // is ready.  Read only the already validated installed identity; book,
        // cursor and freshness reads remain behind the readiness fence.
        let installed = publication.reader().installed_markets()?;
        let metadata = installed
            .get(&market.market_id)
            .context("market metadata not admitted")?;
        let identity = &metadata["identity"];
        ensure!(
            identity["condition_id"] == market.condition_id,
            "admitted market condition differs"
        );
        ensure!(!self.market_conditions.iter().any(|(owner, condition)|
            owner != &market.market_id && condition == &market.condition_id),
            "admitted market condition already belongs to another market");
        let tokens: BTreeSet<_> = identity["outcomes"]
            .as_array()
            .context("admitted outcomes")?
            .iter()
            .map(|outcome| outcome["token_id"].as_str().context("admitted token"))
            .collect::<Result<_>>()?;
        ensure!(
            tokens == market.token_ids.iter().map(String::as_str).collect(),
            "admitted market tokens differ"
        );
        ensure!(
            metadata["lifecycle_state"] == "active",
            "no actor for inactive market"
        );
        // Fail before changing reducer ownership if the per-market publisher
        // has not been admitted by the same catalog transaction.
        publication.market_publisher(&market.market_id)?;
        let added = router.admit_market(market)?;
        if !added {
            ensure!(
                self.controls.contains_key(&market.market_id),
                "admitted actor missing"
            );
            return Ok(false);
        }
        if let Err(error) = self.add_market(router, publication, &market.market_id) {
            for token in &market.token_ids {
                router.identities.remove(token);
                router.waiting.remove(token);
            }
            return Err(error);
        }
        Ok(true)
    }
    /// Metadata and token ownership must be admitted by the acquisition owner
    /// before this method. Retained actors keep their reducer state unchanged.
    pub(super) fn add_market(&mut self, router: &Adapter,
        publication: &Publication, market: &str) -> Result<()> {
        ensure!(
            !self.controls.contains_key(market),
            "market actor already installed"
        );
        let adapter = router.market_actor(market);
        ensure!(
            !adapter.identities.is_empty(),
            "market has no admitted tokens"
        );
        let condition = adapter.identities.values().next()
            .context("market condition absent")?.1.clone();
        ensure!(adapter.identities.values().all(|(owner, value)|
            owner.as_str() == market && value == &condition),
            "admitted market condition differs across tokens");
        ensure!(!self.market_conditions.iter().any(|(owner, known)|
            owner.as_str() != market && known == &condition),
            "admitted market condition already belongs to another market");
        let control = Arc::new(Mutex::new(Control::default()));
        self.dispatch.add_entity(
            market.into(),
            State {
                adapter,
                generation: 0,
                control: control.clone(),
                publisher: publication.market_publisher(market)?,
                trace_budget: (std::time::Instant::now(), 0, 0),
            },
        )?;
        self.controls.insert(market.into(), control);
        self.market_conditions.insert(market.into(), condition);
        self.market_bytes.insert(market.into(),
            Arc::new(Semaphore::new(MARKET_INPUT_BYTES)));
        Ok(())
    }

    /// A reducer task can terminate after a market-local panic or output
    /// failure. Rebuild only that actor from the reviewed token identity;
    /// its old input FIFO and completion incarnation are discarded. A fresh
    /// authoritative book is still required before this market is readable.
    pub(super) async fn restart_closed_actors(
        &mut self,
        router: &mut Adapter,
        publication: &Publication,
    ) -> Result<()> {
        let closed = self.controls.keys()
            .filter(|market| self.dispatch.queue_closed(market))
            .cloned().collect::<Vec<_>>();
        for market in closed {
            let control = self.controls.get(&market)
                .context("closed market control absent")?.clone();
            let already_unavailable = {
                let state = control.lock()
                    .map_err(|_| anyhow::anyhow!("market control poisoned"))?;
                let market_tokens = router.identities.iter()
                    .filter(|(_, (owner, _))| owner.as_str() == market.as_str())
                    .map(|(token, _)| token).collect::<Vec<_>>();
                !market_tokens.is_empty() && !state.baseline_admitted
                    && market_tokens.iter().all(|token| state.attempts.contains_key(*token))
                    && state.attempts.iter().all(|(token, attempt)|
                        router.recoveries.get(token) == Some(attempt))
            };
            if !already_unavailable {
                self.invalidate_or_fence_market(router, publication, &market,
                    "market_actor_closed").await?;
            }
            self.dispatch.remove_entity(&market);
            let generation = control.lock()
                .map_err(|_| anyhow::anyhow!("market control poisoned"))?.generation;
            let adapter = router.market_actor(&market);
            ensure!(!adapter.identities.is_empty(), "closed market has no reviewed tokens");
            if let Err(error) = self.dispatch.add_entity(market.clone(), State {
                adapter, generation, control,
                publisher: publication.market_publisher(&market)?,
                trace_budget: (std::time::Instant::now(), 0, 0),
            }) {
                // Leave this market fenced and retry on the next owner tick.
                // A temporary retiring-task budget cannot restart the source.
                marketcow_polymarket::async_eprintln!("{}", json!({
                    "stage":"market_actor_restart_deferred",
                    "market_id":market,"detail":format!("{error:#}"),
                }));
            }
        }
        Ok(())
    }

    /// This is final scope retirement, not terminal lifecycle observation.
    /// In-flight results are fenced by dispatcher incarnation before metadata
    /// disappears. The owner must stop token transport/recovery jobs as well.
    pub(super) fn remove_market(
        &mut self,
        router: &mut Adapter,
        market: &str,
    ) -> Result<Vec<String>> {
        ensure!(self.controls.contains_key(market), "market actor absent");
        self.retire_market(router, market)?;
        ensure!(
            self.dispatch.remove_entity(market),
            "market actor queue absent"
        );
        self.controls.remove(market);
        self.market_conditions.remove(market);
        self.market_bytes.remove(market);
        let tokens: Vec<_> = router
            .identities
            .iter()
            .filter(|(_, (id, _))| id == market)
            .map(|(t, _)| t.clone())
            .collect();
        for token in &tokens {
            router.identities.remove(token);
            router.ticks.remove(token);
            router.engines.remove(token);
            router.epochs.remove(token);
            router.sequences.remove(token);
            router.waiting.remove(token);
            router.book_received.remove(token);
            router.depth_checksums.remove(token);
            router.last_trades.remove(token);
            router.recoveries.remove(token);
        }
        Ok(tokens)
    }

    pub(super) fn start(router: &Adapter, publication: &Publication) -> Result<(Self, MarketCompletions<Output>)> {
        Self::start_with_workers(router, publication, 4)
    }

    pub(super) fn start_with_workers(
        router: &Adapter,
        publication: &Publication,
        workers: usize,
    ) -> Result<(Self, MarketCompletions<Output>)> {
        ensure!((1..=8).contains(&workers), "market workers must be 1..=8");
        let mut market_conditions = BTreeMap::new();
        for (market, condition) in router.identities.values() {
            if let Some(previous) = market_conditions.insert(market.clone(), condition.clone()) {
                ensure!(previous == *condition,
                    "market condition differs across admitted tokens");
            }
        }
        ensure!(market_conditions.values().collect::<BTreeSet<_>>().len()
            == market_conditions.len(),
            "market condition shared across admitted markets");
        let markets: BTreeSet<_> = router.identities.values().map(|(m, _)| m.clone()).collect();
        let mut controls = BTreeMap::new();
        let states = markets
            .into_iter()
            .map(|market| {
                let control = Arc::new(Mutex::new(Control::default()));
                controls.insert(market.clone(), control.clone());
                Ok((
                    market.clone(),
                    State {
                        adapter: router.market_actor(&market),
                        generation: 0,
                        control,
                        publisher: publication.market_publisher(&market)?,
                        trace_budget: (std::time::Instant::now(), 0, 0),
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        // At most workers CPU jobs, a finite burst window per market, and a
        // bounded scheduling batch per worker (including in-flight results).
        // Input payloads retain the existing shared byte ceiling. Published
        // book batches stay in their market replay/persistence lanes: only a
        // bounded control summary enters each market's completion slots.
        // Successful book-only batches need no coordinator receipt: their
        // market actor has already committed them. Only recovery transitions,
        // bounded diagnostics and errors enter the independent control lanes.
        let output_capacity = (workers * 32).min(128);
        let timing = Arc::new(PipelineTiming::default());
        let actor_timing = timing.clone();
        let mut dispatch = Dispatcher::start_partitioned(
            states,
            MARKET_QUEUE_CAPACITY,
            workers,
            output_capacity,
            ACTOR_CPU_BATCH.min(output_capacity),
            |state: &State, input: &Input| {
                state
                    .control
                    .lock()
                    .is_ok_and(|c| input.generation != c.generation)
            },
            move |state, input: Input| {
                let _timed = Timed::new(&actor_timing.actor_cpu);
                let actor_started_mono = std::time::Instant::now();
                let actor_started_at = Utc::now();
                let mut trace = BatchTrace {
                    raw_batch_first_received_at: input.frames.first().context("market input receipt")?.received_at,
                    raw_batch_last_received_at: input.frames.last().context("market input receipt")?.received_at,
                    admitted_at: input.admitted_at,
                    actor_started_at,
                    actor_completed_at: actor_started_at,
                    admitted_mono: input.admitted_mono,
                    actor_started_mono,
                    actor_completed_mono: actor_started_mono,
                };
                let control = state
                    .control
                    .lock()
                    .map_err(|_| anyhow::anyhow!("market control poisoned"))?
                    .clone();
                if input.generation != control.generation {
                    return Ok(Output {
                        generation: input.generation,
                        published_groups: 0,
                        #[cfg(test)]
                        published_event_types: vec![],
                        recovery_transitions: vec![],
                    });
                }
                if state.generation != control.generation {
                    state.adapter.reset_connection();
                    state.adapter.recoveries = control.attempts;
                    state.generation = control.generation;
                }
                if !input.recovery.is_empty()
                    && input.recovery.iter().all(|(token, attempt)| {
                        state.adapter.recoveries.get(token) != Some(attempt)
                    })
                {
                    return Ok(Output {
                        generation: input.generation,
                        published_groups: 0,
                        #[cfg(test)]
                        published_event_types: vec![],
                        recovery_transitions: vec![],
                    });
                }
                let mut handoff_receipt = input.handoff_receipt;
                let handoff = handoff_receipt.is_some();
                let handoff_context = handoff.then(||input.frames.first().map(|frame|json!({
                    "market_id":state.adapter.identities.values().next().map(|(market,_)|market),
                    "condition_id":frame.raw_payload["market"],
                    "rotation_id":frame.raw_payload["rotation_id"],
                    "attempt":frame.raw_payload["attempt"],
                    "connection_id":frame.raw_payload["connection_id"],
                }))).flatten();
                let mut staged_adapter = None;
                let atomic_recovery = input.rest_recovery.is_some() || handoff;
                let events = if handoff {
                    let staged = input.frames.first().context("checkpoint input")
                        .and_then(|frame| state.adapter.stage_connection_checkpoint(frame,Utc::now()));
                    match staged {
                        Ok((adapter, events)) if input.frames.len() == 1 => {
                            staged_adapter = Some(adapter);
                            events
                        }
                        other => {
                            marketcow_polymarket::async_eprintln!("{}",json!({
                                "stage":"market_hot_checkpoint_rejected",
                                "observed_at":Utc::now(),
                                "handoff":&handoff_context,"rejection_phase":"standby_reduce",
                                "detail":other.err().map(|error|error.to_string())
                                    .unwrap_or_else(||"checkpoint must be isolated".into()),
                            }));
                            if let Some(receipt) = handoff_receipt.take() { let _ = receipt.send(false); }
                            return Ok(Output { generation:input.generation,published_groups:0,
                                #[cfg(test)] published_event_types:vec![],recovery_transitions:vec![] });
                        }
                    }
                } else if let Some(raw_books) = input.rest_recovery {
                    let received_at = input
                        .frames
                        .first()
                        .context("REST recovery receipt frame")?
                        .received_at;
                    if let Some(reason) = input.replacement_reason {
                        let tokens = state.adapter.identities.keys().cloned().collect::<Vec<_>>();
                        let mut events = Vec::new();
                        for token in tokens {
                            events.push(state.adapter.invalidate(
                                &token,
                                &reason,
                                received_at,
                                events.len() as u64 + 1,
                            )?);
                        }
                        let attempts = state.adapter.recoveries.clone();
                        events.extend(state.adapter.apply_rest_recovery(
                            raw_books,
                            received_at,
                            events.len() as u64,
                            &attempts,
                        )?);
                        events
                    } else {
                        let attempts = input.recovery.into_iter().collect();
                        state
                            .adapter
                            .apply_rest_recovery(raw_books, received_at, 0, &attempts)?
                    }
                } else {
                    let mut events = Vec::new();
                    // The market actor still owns this FIFO. Conflate only
                    // validated deep-level replacements before reducer/book
                    // materialization; every unsafe boundary remains ordered.
                    for frame in state.adapter.conflate_depth_frames(input.frames, true) {
                        events.extend(state.adapter.apply_isolated(
                            frame.raw_payload,
                            frame.received_at,
                            events.len() as u64,
                        )?);
                    }
                    events
                };
                // Confirmation facts are applied after this input's ordered
                // mutations. A later mutation/gap can supersede an earlier
                // identical-depth observation in the same coalesced input.
                // It must not confirm the new state (or abort the owner).
                let confirmation_adapter = staged_adapter.as_mut().unwrap_or(&mut state.adapter);
                let confirmations: Vec<ReducedBookConfirmation> = std::mem::take(&mut confirmation_adapter.confirmations)
                    .into_iter().filter(|fact| {
                        !confirmation_adapter.recoveries.contains_key(&fact.token_id)
                            && confirmation_adapter.depth_checksums.get(&fact.token_id)
                                == Some(&fact.state_checksum)
                            && confirmation_adapter.epochs.get(&fact.token_id)
                                == Some(&fact.book_epoch)
                            && confirmation_adapter.sequences.get(&fact.token_id)
                                == Some(&fact.book_sequence)
                            && confirmation_adapter.book_received.get(&fact.token_id)
                                == Some(&fact.book_received_at)
                    }).collect();
                let observations = (|| -> Result<Vec<ActorObservation>> {
                  let (events, materialized_books) = compact_incremental_events(events)?;
                  bounded_size(&events, OUTPUT_BYTES)?;
                  if handoff {
                    // Reject a standby before it mutates the live lane if its
                    // ordinary messages cannot be encoded by the unchanged
                    // production contract. No post-commit protocol surprise
                    // may turn a successful source rotation into a disconnect.
                    use prost::Message as _;
                    let typed = events.iter().map(crate::source_btc5m_proto::market_event)
                        .collect::<Result<Vec<_>>>()?;
                    // These are the already deployed consumer limits, not a
                    // request to enlarge its configuration for source rotation.
                    ensure!(typed.len() <= 4096,"ordinary handoff event budget");
                    let bundle = marketcow_contracts::btc5m_market_local_v5::wire::MarketEventBundle {
                        events:typed,
                    };
                    // Publication rebases actor-local cursors afterwards.
                    // Reserve the bounded lexical/varint growth of u64 event
                    // positions so that a near-limit candidate remains safe.
                    let encoded_bound = bundle.encoded_len().checked_add(events.len()*64)
                        .context("ordinary handoff encoding budget overflow")?;
                    ensure!(encoded_bound <= OUTPUT_BYTES,
                        "ordinary handoff exceeds existing consumer group budget");
                    let part_bytes = state.publisher.ordinary_handoff_frame_bytes()
                        .checked_sub(512).context("ordinary handoff frame envelope budget")?;
                    ensure!(encoded_bound.div_ceil(part_bytes) <= 256,
                        "ordinary handoff exceeds existing consumer chunk budget");
                  }
                  let token_markets = state.adapter.identities.iter()
                    .map(|(token, (market, _))| (token.clone(), market.clone()))
                    .collect::<BTreeMap<_, _>>();
                  let batches = partition_observation_batches(
                    events, materialized_books, atomic_recovery,
                  )?;
                  // A handoff is one transaction, not several independently
                  // visible groups with a possible rejection between them.
                  ensure!(!handoff || batches.len()==1,
                    "ordinary handoff must produce exactly one atomic group");
                  batches.into_iter().map(|(events, materialized_books)| {
                    let prepared = prepare_market_projection(&events, &materialized_books)?;
                    let after = events.first().and_then(|event| event["cursor"].as_u64())
                        .context("actor observation cursor")?.checked_sub(1)
                        .context("actor observation zero cursor")?;
                    let terminal = events[0]["event_type"] == "market_terminal";
                    let validated = if handoff {
                        ValidatedSourceBatch::new_handoff(events,after,&token_markets,OUTPUT_BYTES)?
                    } else {ValidatedSourceBatch::new(
                        events, terminal, after, &token_markets, OUTPUT_BYTES,
                    )?};
                    Ok(ActorObservation { validated, materialized_books, prepared })
                  }).collect::<Result<Vec<_>>>()
                })();
                let observations = match observations {
                    Ok(observations) => observations,
                    Err(error) if handoff => {
                        marketcow_polymarket::async_eprintln!("{}",json!({
                            "stage":"market_hot_checkpoint_rejected",
                            "observed_at":Utc::now(),"detail":error.to_string(),
                            "handoff":&handoff_context,"rejection_phase":"ordinary_wire_admission",
                        }));
                        if let Some(receipt) = handoff_receipt.take() { let _ = receipt.send(false); }
                        return Ok(Output {generation:input.generation,published_groups:0,
                            #[cfg(test)] published_event_types:vec![],recovery_transitions:vec![]});
                    }
                    Err(error) => return Err(error),
                };
                trace.actor_completed_at = Utc::now();
                trace.actor_completed_mono = std::time::Instant::now();
                // The control fence is held across this market's publication.
                // An invalidation or retirement can never overtake an older
                // actor result; an unrelated market has a different fence.
                let mut control = state.control.lock()
                    .map_err(|_| anyhow::anyhow!("market control poisoned"))?;
                if input.generation != control.generation {
                    return Ok(Output { generation: input.generation, published_groups: 0,
                        #[cfg(test)]
                        published_event_types: vec![],
                        recovery_transitions: vec![] });
                }
                let mut published = Vec::with_capacity(observations.len());
                let mut recovery_transitions = Vec::new();
                let handoff_deadline = if let Some(adapter)=staged_adapter.as_ref() {
                    match adapter.ordinary_handoff_deadline(&confirmations,Utc::now()) {
                        Ok(deadline)=>Some(deadline),
                        Err(error)=>{
                            marketcow_polymarket::async_eprintln!("{}",json!({
                                "stage":"market_hot_checkpoint_rejected",
                                "observed_at":Utc::now(),"detail":error.to_string(),
                                "rejection_phase":"pre_commit_source_freshness",
                                "handoff":&handoff_context,
                            }));
                            if let Some(receipt)=handoff_receipt.take(){let _=receipt.send(false);}
                            return Ok(Output{generation:input.generation,published_groups:0,
                                #[cfg(test)] published_event_types:vec![],recovery_transitions:vec![]});
                        }
                    }
                } else {None};
                for observation in observations {
                    let publish_call_started_mono = std::time::Instant::now();
                    let publish_call_started_at = Utc::now();
                    let result = state.publisher.publish_before(
                        observation.validated, observation.materialized_books,
                        observation.prepared, None, Some(SourceBatchTiming {
                            raw_batch_first_received_at: trace.raw_batch_first_received_at,
                            raw_batch_last_received_at: trace.raw_batch_last_received_at,
                            actor_admitted_at: trace.admitted_at,
                            actor_started_at: trace.actor_started_at,
                            actor_completed_at: trace.actor_completed_at,
                            actor_admitted_mono: trace.admitted_mono,
                            actor_started_mono: trace.actor_started_mono,
                            actor_completed_mono: trace.actor_completed_mono,
                            publish_call_started_at, publish_call_started_mono,
                            shared_replay_visible_at: None, shared_replay_visible_mono: None,
                            durable_at: None,
                        }),handoff_deadline);
                    let batch = match result {
                        Ok(batch) => batch,
                        Err(error) if handoff => {
                            marketcow_polymarket::async_eprintln!("{}",json!({
                                "stage":"market_hot_checkpoint_not_committed",
                                "observed_at":Utc::now(),"detail":error.to_string(),
                                "handoff":&handoff_context,"rejection_phase":"publication_prepare",
                            }));
                            if let Some(receipt) = handoff_receipt.take() { let _ = receipt.send(false); }
                            return Ok(Output {generation:input.generation,published_groups:0,
                                #[cfg(test)] published_event_types:vec![],recovery_transitions:vec![]});
                        }
                        Err(error) => return Err(error),
                    };
                    for event in batch.validated.events() {
                        if marketcow_runtime::discovery_source::is_token_recovery(event) {
                            let token = event["token_id"].as_str()
                                .expect("validated recovery token identity missing").to_owned();
                            if event["event_type"] == "recovery_started" {
                                let attempt = event["canonical_payload"]["recovery_id"]
                                    .as_str().expect("validated recovery identity missing").to_owned();
                                control.attempts.insert(token.clone(), attempt.clone());
                                recovery_transitions.push((true, token, attempt));
                            } else {
                                control.attempts.remove(&token);
                                recovery_transitions.push((false, token,
                                    event["canonical_payload"]["recovery_id"]
                                        .as_str().expect("validated recovery identity missing").to_owned()));
                            }
                        }
                    }
                    published.push(batch);
                }
                if let Some(adapter) = staged_adapter {
                    state.adapter = adapter;
                    marketcow_polymarket::async_eprintln!("{}",json!({
                        "stage":"market_hot_ordinary_handoff_committed",
                        "observed_at":Utc::now(),"handoff":&handoff_context,
                        "published_groups":published.len(),
                        "supplier_freshness_deadline":handoff_deadline,
                    }));
                    if let Some(receipt) = handoff_receipt.take() { let _ = receipt.send(true); }
                }
                for confirmation in confirmations {
                    if let Err(error) = state.publisher.confirm_reduced_token(
                        &confirmation.market_id, &confirmation.condition_id,
                        &confirmation.token_id, &confirmation.state_checksum,
                        confirmation.exchange_at, confirmation.received_at,
                        &confirmation.source_hash) {
                        // Freshness evidence is memory-only and optional. A
                        // rejected/stale fact cannot roll back an already
                        // published observation or turn its successful actor
                        // completion into a second recovery barrier.
                        marketcow_polymarket::async_eprintln!("{}", json!({
                            "stage":"market_confirmation_rejected",
                            "market_id":confirmation.market_id,
                            "token_id":confirmation.token_id,
                            "detail":error.to_string(),
                        }));
                    }
                }
                drop(control);
                let slow_batch = (Utc::now() - trace.raw_batch_first_received_at)
                    .num_milliseconds() >= 500;
                // Diagnostics remain in this market's actor and use the
                // bounded non-blocking writer. They never occupy a control
                // completion slot or another market's output lane.
                if state.trace_budget.0.elapsed() >= std::time::Duration::from_secs(60) {
                    state.trace_budget = (std::time::Instant::now(), 0, 0);
                }
                let mut trace_events = Vec::new();
                for batch in &published {
                    if state.trace_budget.1 >= 60 && state.trace_budget.2 >= 32 { break; }
                    let (market_id, stream_epoch, market_cursor) = batch.validated.single_market_group()
                        .expect("published native market group missing");
                    for event in batch.validated.events() {
                        if trace_events.len() >= 96 { break; }
                        let sampled = event["cursor"].as_u64()
                            .is_some_and(crate::source_publication::btc5m_trace_sample)
                            && state.trace_budget.2 < 32;
                        let source_lag = event["exchange_at"].as_str()
                            .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
                            .zip(event["received_at"].as_str()
                                .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok()))
                            .is_some_and(|(source_at, received_at)|
                                received_at.signed_duration_since(source_at)
                                    .num_milliseconds() > 5_000);
                        let slow = !sampled && (slow_batch || source_lag)
                            && state.trace_budget.1 < 60;
                        if sampled || slow {
                            if sampled { state.trace_budget.2 += 1; }
                            else { state.trace_budget.1 += 1; }
                            trace_events.push((sampled, source_lag, json!({
                                "market_id":market_id,
                                "market_event_cursor":event["cursor"],
                                "market_cursor":market_cursor,
                                "stream_epoch":stream_epoch,
                                "event_id":event["event_id"],
                                "raw_payload_sha256":event["raw_payload_sha256"],
                                "token_id":event["token_id"],
                                "book_epoch":event["book_epoch"],
                                "sequence":event["sequence"],
                                "canonical_book_received_at":event["canonical_payload"]["received_at"],
                                "event_received_at":event["received_at"],
                                "event_source_at":event["exchange_at"],
                            })));
                        }
                    }
                    if trace_events.len() >= 96 { break; }
                }
                for (sampled, source_lag, event) in trace_events {
                    marketcow_polymarket::async_eprintln!("{}", json!({
                        "stage":"market_event_timeline",
                        "trace_reason":if sampled {"cursor_sample"} else if source_lag {"source_lag"} else {"slow_batch"},
                        "market_id":event["market_id"],
                        "event":event,
                        "raw_batch_first_received_at":trace.raw_batch_first_received_at,
                        "raw_batch_last_received_at":trace.raw_batch_last_received_at,
                        "actor_admitted_at":trace.admitted_at,
                        "actor_started_at":trace.actor_started_at,
                        "actor_completed_at":trace.actor_completed_at,
                        "logged_at":Utc::now(),
                        "durability":"published_not_necessarily_durable",
                    }));
                }
                let published_groups = published.len();
                #[cfg(test)]
                let published_event_types = published.iter()
                    .flat_map(|batch| batch.validated.events())
                    .filter_map(|event| event["event_type"].as_str().map(str::to_owned))
                    .collect();
                Ok(Output {
                    generation: input.generation,
                    published_groups,
                    #[cfg(test)]
                    published_event_types,
                    recovery_transitions,
                })
            },
        )?;
        // The in-module reducer tests inspect every completion; the live
        // owner needs only recovery transitions and failures. Diagnostics use
        // the bounded non-blocking log writer directly from each market actor.
        #[cfg(not(test))]
        dispatch.set_completion_filter(|result: &Result<Output>| match result {
            Ok(output) => !output.recovery_transitions.is_empty(),
            Err(_) => true,
        })?;
        let output = dispatch.take_completion_set()?;
        let market_bytes = controls.keys().map(|market|
            (market.clone(), Arc::new(Semaphore::new(MARKET_INPUT_BYTES))))
            .collect();
        Ok((
            Self {
                dispatch,
                controls,
                market_conditions,
                bytes: Arc::new(Semaphore::new(INPUT_BYTES)),
                market_bytes,
                admission: Mutex::new((std::time::Instant::now(), BTreeMap::new())),
                timing,
            },
            output,
        ))
    }

    fn note(&self, category: &'static str) {
        let mut stats = self.admission.lock().expect("admission metrics poisoned");
        *stats.1.entry(category).or_default() += 1;
        if stats.0.elapsed() < std::time::Duration::from_secs(5) {
            return;
        }
        let (queued, maximum, cpu_available, output_queued) = self.dispatch.occupancy();
        let row = json!({"at":Utc::now(),"stage":"market_admission",
            "interval_ms":stats.0.elapsed().as_millis(),"counts":stats.1,
            "queued_inputs":queued,"maximum_market_queued":maximum,
            "cpu_permits_available":cpu_available,"input_bytes_used":INPUT_BYTES-self.bytes.available_permits(),
            "queue_snapshot_excludes_actor_held_input":true,"output_completions_queued":output_queued,
            "completion_payload":"bounded_control_summary_no_book_batch",
            "busiest_market_queues":self.dispatch.busiest_queues(4)});
        stats.0 = std::time::Instant::now();
        stats.1.clear();
        drop(stats);
        marketcow_polymarket::async_eprintln!("{row}");
    }

    fn try_reserve_input_bytes(&self, market: &str, bytes: usize)
        -> std::result::Result<(OwnedSemaphorePermit, OwnedSemaphorePermit), &'static str> {
        let amount = u32::try_from(bytes).map_err(|_| "input_bytes_invalid")?;
        let shared = self.bytes.clone().try_acquire_many_owned(amount)
            .map_err(|_| "shared_input_bytes_full")?;
        let local = self.market_bytes.get(market)
            .ok_or("market_input_lane_missing")?.clone()
            .try_acquire_many_owned(amount)
            .map_err(|_| "market_input_bytes_full")?;
        Ok((shared, local))
    }

    fn route<'a>(router: &'a Adapter, frame: &RawTransportFrame) -> Result<&'a str> {
        if let Some(token) = frame.raw_payload["asset_id"].as_str()
            && let Some((market, _)) = router.identities.get(token)
        {
            return Ok(market);
        }
        if let Some(condition) = frame.raw_payload["market"].as_str()
            && let Some((market, _)) = router.identities.values().find(|(_, c)| c == condition)
        {
            return Ok(market);
        }
        anyhow::bail!("unroutable source frame")
    }

    fn belongs_to_market(&self, router: &Adapter, expected: &str,
        frame: &RawTransportFrame) -> bool {
        if let Some(token) = frame.raw_payload["asset_id"].as_str() {
            return router.identities.get(token)
                .is_some_and(|(market, _)| market.as_str() == expected);
        }
        frame.raw_payload["market"].as_str().is_some_and(|condition|
            self.market_conditions.get(expected).is_some_and(|known| known.as_str() == condition))
    }

    /// Candidate admission is optional work: failure rejects the candidate,
    /// never installs a recovery barrier on the still-live active connection.
    pub(super) fn submit_connection_checkpoint(&self, router: &Adapter,
        market: String, frames: Vec<RawTransportFrame>,
        receipt: tokio::sync::oneshot::Sender<bool>) -> Result<()> {
        let admission=(|| -> Result<usize> {
            ensure!(frames.len() == 1
                && frames[0].raw_payload["event_type"] == "marketcow_connection_checkpoint"
                && self.belongs_to_market(router, &market, &frames[0]), "checkpoint route");
            bounded_size(&frames, FRAME_BYTES)
        })();
        let bytes=match admission {
            Ok(bytes)=>bytes,
            Err(error)=>{
                marketcow_polymarket::async_eprintln!("{}",json!({
                    "stage":"market_hot_checkpoint_rejected","market_id":market,
                    "observed_at":Utc::now(),"rejection_phase":"input_admission",
                    "detail":error.to_string(),
                }));
                let _=receipt.send(false);
                return Ok(());
            }
        };
        let generation = {
            let control = self.controls.get(&market).context("checkpoint market owner")?
                .lock().map_err(|_|anyhow::anyhow!("checkpoint control poisoned"))?;
            if !control.attempts.is_empty() || !control.pending_barrier.is_empty() {
                let _ = receipt.send(false);
                return Ok(());
            }
            control.generation
        };
        let (shared, local) = match self.try_reserve_input_bytes(&market, bytes) {
            Ok(permits) => permits,
            Err(_) => { let _ = receipt.send(false); return Ok(()); }
        };
        let input = Input { handoff_receipt:Some(receipt), generation, frames,
            admitted_at:Utc::now(),admitted_mono:std::time::Instant::now(),
            recovery:vec![],rest_recovery:None,replacement_reason:None,
            _bytes:shared,_market_bytes:local };
        // A returned input drops its receipt: the old socket remains active.
        let _ = self.dispatch.try_submit(&market, input);
        Ok(())
    }

    pub(super) async fn submit(
        &self,
        router: &mut Adapter,
        publication: &Publication,
        frame: RawTransportFrame,
        recovery: Option<(String, String)>,
    ) -> Result<()> {
        // Frozen scopes deliberately ignore new-market announcements.
        if frame.raw_payload["event_type"] == "new_market" {
            return Ok(());
        }
        let market = Self::route(router, &frame)?.to_owned();
        self.submit_market_frames(router, publication, market, vec![frame], recovery)
            .await
    }

    /// Admit one upstream transport message without spending one actor queue
    /// slot per JSON object. Frames are grouped only by market, retain their
    /// source order inside that market, and are split by an explicit bound.
    /// The reducer still emits and persists every individual source event.
    pub(super) async fn submit_batch(
        &self,
        router: &mut Adapter,
        publication: &Publication,
        frames: Vec<RawTransportFrame>,
    ) -> Result<()> {
        let mut grouped = BTreeMap::<String, VecDeque<RawTransportFrame>>::new();
        for frame in frames {
            if frame.raw_payload["event_type"] == "new_market" {
                continue;
            }
            let market = Self::route(router, &frame)?.to_owned();
            grouped.entry(market).or_default().push_back(frame);
        }
        // A burst from the first market must not occupy every actor input
        // slot before another market in the same upstream batch is admitted.
        // Take one bounded FIFO chunk per market on each pass; this changes no
        // order within a market and does not copy the raw frames.
        while !grouped.is_empty() {
            for (market, frames) in next_market_input_pass(&mut grouped) {
                self.submit_market_frames(router, publication, market, frames, None).await?;
            }
        }
        Ok(())
    }

    /// The socket admission layer pins each batch to one reviewed market
    /// pair. A malformed or cross-market venue frame invalidates that market
    /// alone; it must not tear down another market's socket or actor.
    pub(super) async fn submit_routed_batches(
        &self,
        router: &mut Adapter,
        publication: &Publication,
        batches: Vec<(String, Vec<RawTransportFrame>)>,
    ) -> Result<()> {
        let mut grouped = BTreeMap::<String, VecDeque<Vec<RawTransportFrame>>>::new();
        let mut invalid = BTreeSet::<String>::new();
        for (expected, frames) in batches {
            let mut segments = VecDeque::<Vec<RawTransportFrame>>::new();
            for frame in frames {
                if frame.raw_payload["event_type"] == "new_market" {
                    continue;
                }
                // Only the market actor may create this internal aggregate.
                // A supplier frame carrying that marker cannot impersonate
                // already-validated source evidence.
                if frame.raw_payload.get("marketcow_conflation").is_some() {
                    invalid.insert(expected.clone());
                    continue;
                }
                if !self.belongs_to_market(router, &expected, &frame) {
                    invalid.insert(expected.clone());
                    continue;
                }
                let is_gap = frame.raw_payload["event_type"] == "source_gap";
                if let Some(last) = segments.back_mut()
                    && last.len() < INPUT_FRAME_BATCH
                    && (last[0].raw_payload["event_type"] == "source_gap") == is_gap
                {
                    last.push(frame);
                    continue;
                }
                segments.push_back(vec![frame]);
            }
            if !segments.is_empty() {
                let pending = grouped.entry(expected).or_default();
                let mut consumed_first = false;
                if let (Some(last), Some(first)) = (pending.back_mut(), segments.front_mut())
                    && (last[0].raw_payload["event_type"] == "source_gap")
                        == (first[0].raw_payload["event_type"] == "source_gap")
                    && last.len() < INPUT_FRAME_BATCH
                {
                    let count = (INPUT_FRAME_BATCH - last.len()).min(first.len());
                    last.extend(first.drain(..count));
                    consumed_first = first.is_empty();
                }
                if consumed_first { segments.pop_front(); }
                pending.append(&mut segments);
            }
        }
        for market in invalid {
            grouped.remove(&market);
            self.invalidate_or_fence_market(router, publication, &market, "market_transport_identity")
                .await?;
        }
        while !grouped.is_empty() {
            let markets = grouped.keys().cloned().collect::<Vec<_>>();
            for market in markets {
                let pending = grouped.get_mut(&market).expect("routed market pending");
                let frames = pending.pop_front().expect("routed market segment");
                if pending.is_empty() { grouped.remove(&market); }
                self.submit_market_frames(router, publication, market, frames, None).await?;
            }
        }
        Ok(())
    }

    async fn submit_market_frames(
        &self,
        router: &mut Adapter,
        publication: &Publication,
        market: String,
        mut frames: Vec<RawTransportFrame>,
        recovery: Option<(String, String)>,
    ) -> Result<()> {
        ensure!(!frames.is_empty(), "empty market input batch");
        if !self.flush_pending_barrier(publication, &market)? {
            return Ok(());
        }
        {
            let control = self.controls[&market]
                .lock()
                .map_err(|_| anyhow::anyhow!("market control poisoned"))?;
            if !control.attempts.is_empty()
                && !control.baseline_admitted
                && frames.iter().all(|frame| {
                    matches!(
                        frame.raw_payload["event_type"].as_str(),
                        Some("price_change" | "best_bid_ask" | "last_trade_price")
                    )
                })
            {
                // Explicitly unavailable already. Incrementals cannot install
                // a baseline; do not consume the recovery admission slots.
                self.note("unavailable_incremental_ignored");
                return Ok(());
            }
        }
        let generation = self.controls[&market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?
            .generation;
        let bytes = bounded_size(&frames, FRAME_BYTES);
        let rejection;
        if let Ok(bytes) = bytes {
            match self.try_reserve_input_bytes(&market, bytes) {
                Ok((permit, market_permit)) => {
                let input = Input {
                    handoff_receipt: None,
                    generation,
                    frames: std::mem::take(&mut frames),
                    admitted_at: Utc::now(),
                    admitted_mono: std::time::Instant::now(),
                    recovery: recovery.into_iter().collect(),
                    rest_recovery: None,
                    replacement_reason: None,
                    _bytes: permit,
                    _market_bytes: market_permit,
                };
                if self.dispatch.try_submit(&market, input).is_ok() {
                    self.controls[&market]
                        .lock()
                        .map_err(|_| anyhow::anyhow!("market control poisoned"))?
                        .baseline_admitted = true;
                    self.note("accepted");
                    return Ok(());
                }
                rejection = if self.dispatch.queue_closed(&market) {
                    "queue_closed_or_unknown"
                } else {
                    "market_input_slots_full"
                };
                }
                Err(reason) => rejection = reason,
            }
        } else {
            rejection = "frame_size_or_encoding_rejected";
        }
        self.note(rejection);
        if rejection == "queue_closed_or_unknown" {
            return self
                .invalidate_or_fence_market(router, publication, &market, "market_actor_closed")
                .await;
        }
        self.invalidate_or_fence_market(router, publication, &market, "market_queue_capacity")
            .await
    }

    pub(super) async fn submit_rest_recovery(
        &self,
        router: &mut Adapter,
        publication: &Publication,
        raw_books: Vec<Value>,
        received_at: chrono::DateTime<Utc>,
        attempts: Vec<(String, String)>,
    ) -> Result<bool> {
        let received_at = chrono::DateTime::from_timestamp_micros(received_at.timestamp_micros())
            .context("REST recovery receipt timestamp")?;
        let token = attempts
            .first()
            .context("REST recovery attempt missing")?
            .0
            .clone();
        let market = router
            .identities
            .get(&token)
            .context("REST recovery token")?
            .0
            .clone();
        if !self.flush_pending_barrier(publication, &market)? {
            self.note("rest_recovery_waiting_for_capacity_barrier");
            return Ok(false);
        }
        let generation = self.controls[&market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?
            .generation;
        let bytes = match bounded_size(&raw_books, FRAME_BYTES) {
            Ok(bytes) => bytes,
            Err(_) => {
                self.note("rest_frame_size_or_encoding_rejected");
                self.invalidate_or_fence_market(router, publication, &market, "market_snapshot_oversize")
                    .await?;
                return Ok(false);
            }
        };
        let frame = RawTransportFrame {
            raw_payload: raw_books
                .first()
                .context("REST recovery pair missing")?
                .clone(),
            received_at,
        };
        let (permit, market_permit) = match self.try_reserve_input_bytes(&market, bytes) {
            Ok(permits) => permits,
            Err(reason) => {
                self.note(reason);
                self.invalidate_or_fence_market(router, publication, &market, "market_queue_capacity")
                    .await?;
                return Ok(false);
            }
        };
        let input = Input {
            handoff_receipt: None,
            generation,
            frames: vec![frame],
            admitted_at: Utc::now(),
            admitted_mono: std::time::Instant::now(),
            recovery: attempts,
            rest_recovery: Some(raw_books),
            replacement_reason: None,
            _bytes: permit,
            _market_bytes: market_permit,
        };
        match self.dispatch.try_submit(&market, input) {
            Ok(()) => {
                self.controls[&market]
                    .lock()
                    .map_err(|_| anyhow::anyhow!("market control poisoned"))?
                    .baseline_admitted = true;
                self.note("rest_recovery_accepted");
                Ok(true)
            }
            Err(_) => {
                self.note("rest_market_input_slots_full_or_closed");
                self.invalidate_or_fence_market(router, publication, &market, "market_queue_capacity")
                    .await?;
                Ok(false)
            }
        }
    }

    pub(super) async fn submit_rest_replacement(
        &self,
        router: &mut Adapter,
        publication: &Publication,
        raw_books: Vec<Value>,
        received_at: chrono::DateTime<Utc>,
        market: &str,
        reason: &str,
    ) -> Result<()> {
        let received_at = chrono::DateTime::from_timestamp_micros(received_at.timestamp_micros())
            .context("REST replacement receipt timestamp")?;
        if !self.flush_pending_barrier(publication, market)? {
            self.note("rest_replacement_waiting_for_capacity_barrier");
            return Ok(());
        }
        let generation = self.controls[market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?
            .generation;
        let bytes = match bounded_size(&raw_books, FRAME_BYTES) {
            Ok(bytes) => bytes,
            Err(_) => {
                self.note("replacement_frame_size_or_encoding_rejected");
                return self.invalidate_or_fence_market(router, publication, market,
                    "market_snapshot_oversize").await;
            }
        };
        let frame = RawTransportFrame {
            raw_payload: raw_books
                .first()
                .context("REST replacement pair missing")?
                .clone(),
            received_at,
        };
        let (permit, market_permit) = match self.try_reserve_input_bytes(market, bytes) {
            Ok(permits) => permits,
            Err(reason) => {
                self.note(reason);
                return self.invalidate_or_fence_market(router, publication, market,
                    "market_queue_capacity").await;
            }
        };
        let input = Input {
            handoff_receipt: None,
            generation,
            frames: vec![frame],
            admitted_at: Utc::now(),
            admitted_mono: std::time::Instant::now(),
            recovery: Vec::new(),
            rest_recovery: Some(raw_books),
            replacement_reason: Some(reason.to_owned()),
            _bytes: permit,
            _market_bytes: market_permit,
        };
        match self.dispatch.try_submit(market, input) {
            Ok(()) => {
                self.controls[market]
                    .lock()
                    .map_err(|_| anyhow::anyhow!("market control poisoned"))?
                    .baseline_admitted = true;
                Ok(())
            }
            Err(_) => {
                self.invalidate_or_fence_market(router, publication, market, "market_queue_capacity")
                    .await
            }
        }
    }

    pub(super) async fn invalidate_market(
        &self,
        router: &mut Adapter,
        publication: &Publication,
        market: &str,
        reason: &str,
    ) -> Result<()> {
        let mut control = self.controls[market]
            .lock().map_err(|_| anyhow::anyhow!("market control poisoned"))?;
        if reason == "market_queue_capacity"
            && !control.attempts.is_empty()
            && !control.baseline_admitted
        {
            self.note("capacity_recovery_coalesced");
            return Ok(());
        }
        let tokens: Vec<_> = router
            .identities
            .iter()
            .filter(|(_, (m, _))| m == market)
            .map(|(t, _)| t.clone())
            .collect();
        self.note("new_recovery_barrier");
        let mut events = Vec::new();
        let first_cursor = publication.next_market_event_cursor(market)?;
        for token in &tokens {
            events.push(
                router.invalidate(
                    token,
                    reason,
                    Utc::now(),
                    first_cursor.checked_add(events.len() as u64)
                        .context("market event cursor overflow")?,
                )?,
            );
        }
        if let Err(error) = publication.publish_native_events(events.clone(), None) {
            if error.downcast_ref::<MarketCapacityExhausted>().is_none() {
                return Err(error);
            }
            publication.fence_market_capacity(market)?;
            control.pending_barrier = events;
        }
        control.generation = control
            .generation
            .checked_add(1)
            .context("market generation overflow")?;
        control.baseline_admitted = false;
        control.attempts = tokens
            .into_iter()
            .map(|token| {
                let attempt = router.recoveries[&token].clone();
                (token, attempt)
            })
            .collect();
        Ok(())
    }

    /// Last-resort, market-local fence when a failed socket's recovery event
    /// cannot be published. The generation change discards already queued
    /// actor work; the replay state wakes this market's readers as unavailable.
    /// It does not manufacture a recovery-completed fact or affect another
    /// market's actor.
    pub(super) fn fence_failed_market(
        &self,
        router: &Adapter,
        publication: &Publication,
        market: &str,
    ) -> Result<()> {
        let mut control = self.controls.get(market)
            .context("failed market actor absent")?
            .lock().map_err(|_| anyhow::anyhow!("market control poisoned"))?;
        control.generation = control.generation.checked_add(1)
            .context("market generation overflow")?;
        control.baseline_admitted = false;
        // An unpublished marker from an older attempt must never be flushed
        // after this fence with a superseded recovery identity or cursor.
        // Readers receive MarketUnavailable and re-seed from full sync.
        control.pending_barrier.clear();
        control.attempts = router.recoveries.iter()
            .filter(|(token, _)| router.identities.get(*token)
                .is_some_and(|(owner, _)| owner == market))
            .map(|(token, attempt)| (token.clone(), attempt.clone()))
            .collect();
        publication.fence_market_capacity(market)
    }

    /// A malformed or failed observation belongs to one market. If its
    /// ordered recovery marker cannot be admitted, still invalidate that
    /// market's readable projection and queued actor generation. The error
    /// remains diagnostic; it is never interpreted as a successful recovery.
    pub(super) async fn invalidate_or_fence_market(
        &self,
        router: &mut Adapter,
        publication: &Publication,
        market: &str,
        reason: &str,
    ) -> Result<()> {
        if let Err(error) = self.invalidate_market(router, publication, market, reason).await {
            self.fence_failed_market(router, publication, market)?;
            marketcow_polymarket::async_eprintln!("{}", json!({
                "stage":"market_recovery_marker_fenced",
                "market_id":market,"reason":reason,"detail":format!("{error:#}"),
            }));
        }
        Ok(())
    }

    /// A failed durability admission has not installed a public gap group.
    /// Retry that exact market barrier before accepting any replacement book;
    /// another market never waits for this lane. A full queue is not a
    /// process-wide persistence failure.
    fn flush_pending_barrier(&self, publication: &Publication,
        market: &str) -> Result<bool> {
        let mut control = self.controls[market].lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?;
        if control.pending_barrier.is_empty() { return Ok(true); }
        match publication.publish_native_events(control.pending_barrier.clone(), None) {
            Ok(()) => {
                control.pending_barrier.clear();
                Ok(true)
            }
            Err(error) if error.downcast_ref::<MarketCapacityExhausted>().is_some() => {
                let limiter = error.downcast_ref::<MarketCapacityExhausted>()
                    .expect("capacity error checked").limiter;
                self.note(match limiter {
                    "replay_market_slots" => "capacity_barrier_replay_market_slots",
                    name if name.starts_with("replay_market_") =>
                        "capacity_barrier_replay_market",
                    name if name.starts_with("replay_aggregate_") =>
                        "capacity_barrier_replay_aggregate",
                    name if name.starts_with("persistence_market_") =>
                        "capacity_barrier_persistence_market",
                    name if name.starts_with("persistence_aggregate_") =>
                        "capacity_barrier_persistence_aggregate",
                    _ => "capacity_barrier_other",
                });
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn retire_market(&self, router: &mut Adapter, market: &str) -> Result<()> {
        let mut control = self.controls[market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?;
        control.generation = control
            .generation
            .checked_add(1)
            .context("market generation overflow")?;
        control.attempts.clear();
        control.pending_barrier.clear();
        let retired: Vec<_> = router
            .identities
            .iter()
            .filter(|(_, (owner, _))| owner == market)
            .map(|(token, _)| token.clone())
            .collect();
        for token in retired {
            router.recoveries.remove(&token);
        }
        Ok(())
    }

    /// Fence every queued/in-flight frame at an intentional acquisition
    /// boundary without creating recovery facts. Existing genuine recovery
    /// identities remain attached and can only complete from a fresh book.
    pub(super) fn pause_all(&self, router: &mut Adapter) -> Result<()> {
        router.reset_connection();
        for (market, owner) in &self.controls {
            let mut control = owner
                .lock()
                .map_err(|_| anyhow::anyhow!("market control poisoned"))?;
            control.generation = control
                .generation
                .checked_add(1)
                .context("market generation overflow")?;
            control.baseline_admitted = false;
            control.attempts = router
                .recoveries
                .iter()
                .filter(|(token, _)| {
                    router
                        .identities
                        .get(*token)
                        .is_some_and(|(owner, _)| owner == market)
                })
                .map(|(token, attempt)| (token.clone(), attempt.clone()))
                .collect();
        }
        Ok(())
    }

    pub(super) async fn publish(
        &self,
        router: &mut Adapter,
        publication: &Publication,
        completion: Completion<Output>,
    ) -> Result<()> {
        self.timing.completion_wait.record(completion.completed_at.elapsed());
        let _timed = Timed::new(&self.timing.owner_publish);
        // Retirement/re-addition must not accept a completion from a former
        // actor even if its local recovery generation happens to be equal.
        if !self.dispatch.is_current(&completion) {
            return Ok(());
        }
        let market = completion.entity;
        let output = match completion.result {
            Ok(output) => output,
            Err(error) => {
                let capacity = error.downcast_ref::<MarketCapacityExhausted>();
                if let Some(capacity) = capacity {
                    let attempt = capacity_log_attempt(capacity.limiter);
                    if attempt == 1 || attempt % 64 == 0 {
                        marketcow_polymarket::async_eprintln!("{}", json!({
                            "at":Utc::now(), "stage":"market_capacity_rejected", "market_id":market,
                            "limiter":capacity.limiter, "limiter_rejections":attempt,
                            "requested_bytes":capacity.requested_bytes,
                            "used_bytes":capacity.used_bytes, "limit_bytes":capacity.limit_bytes,
                            "used_batches":capacity.used_batches,
                            "limit_batches":capacity.limit_batches,
                            "sampling":"first_and_every_64th_per_limiter",
                        }));
                    }
                } else {
                    marketcow_polymarket::async_eprintln!("{}", json!({
                        "stage":"market_worker_rejected","market_id":market,
                        "detail":error.to_string(),
                    }));
                }
                return self
                    .invalidate_or_fence_market(router, publication, &market,
                        if capacity.is_some() { "market_queue_capacity" } else { "market_worker_rejected" })
                    .await;
            }
        };
        if output.generation
            != self.controls[&market]
                .lock()
                .map_err(|_| anyhow::anyhow!("market control poisoned"))?
                .generation
        {
            return Ok(()); // Older CPU results cannot cross the invalidation barrier.
        }
        if output.published_groups == 0 && output.recovery_transitions.is_empty() {
            return Ok(());
        }
        let prepare_started = std::time::Instant::now();
        // The actor already committed this market and prepared bounded
        // recovery summaries. Only control-plane mirroring remains;
        // the coordinator does not rescan every published event.
        self.timing.owner_prepare.record(prepare_started.elapsed());
        let _finalize_timed = Timed::new(&self.timing.owner_finalize);
        for (started, token, attempt) in &output.recovery_transitions {
            if *started {
                router.recoveries.insert(token.clone(), attempt.clone());
            } else {
                router.recoveries.remove(token);
            }
        }
        Ok(())
    }

    pub(super) async fn finish(
        mut self,
        router: &mut Adapter,
        publication: &Publication,
        output: &mut MarketCompletions<Output>,
    ) -> Result<()> {
        // Stop accepting actor input, then publish every already-accepted
        // completion while actors drain. Accepted work must not disappear just
        // because shutdown raced its CPU reducer.
        self.dispatch.close();
        while let Some(value) = output.recv().await {
            self.publish(router, publication, value).await?;
        }
        self.dispatch.join().await;
        Ok(())
    }
}

/// Reestablish the single-observation publication contract after reducing a
/// group of WS frames in one market actor input. REST recovery is deliberately
/// atomic; no later WS observation may be folded into that transaction.
fn partition_observation_batches(
    events: Vec<Value>,
    mut materialized_books: BTreeMap<u64, Value>,
    atomic_recovery: bool,
) -> Result<Vec<(Vec<Value>, BTreeMap<u64, Value>)>> {
    if atomic_recovery {
        return Ok(vec![(events, materialized_books)]);
    }
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_books = BTreeMap::new();
    let mut received: Option<String> = None;
    let mut tokens = BTreeSet::new();
    for event in events {
        let event_type = event["event_type"].as_str().context("observation event type")?;
        let recovery_started = event_type == "recovery_started";
        let recovery_completed = event_type == "recovery_completed";
        let terminal = event_type == "market_terminal";
        if terminal {
            if !current.is_empty() {
                batches.push((std::mem::take(&mut current), std::mem::take(&mut current_books)));
                tokens.clear();
            }
            batches.push((vec![event], BTreeMap::new()));
            received = None;
            continue;
        }
        let token = event["token_id"].as_str().context("observation token")?;
        let observed = event["received_at"].as_str().context("observation receipt")?;
        if !current.is_empty()
            && (received.as_deref() != Some(observed)
                || (!recovery_started && !recovery_completed && tokens.contains(token))
                || (recovery_started && !tokens.is_empty()))
        {
            batches.push((std::mem::take(&mut current), std::mem::take(&mut current_books)));
            tokens.clear();
        }
        received = Some(observed.to_owned());
        if !recovery_started && !recovery_completed {
            tokens.insert(token.to_owned());
        }
        if recovery_completed {
            ensure!(tokens.contains(token), "recovery completion without same-batch snapshot");
        }
        let cursor = event["cursor"].as_u64().context("observation cursor")?;
        if let Some(book) = materialized_books.remove(&cursor) {
            current_books.insert(cursor, book);
        }
        current.push(event);
    }
    if !current.is_empty() {
        batches.push((current, current_books));
    }
    ensure!(materialized_books.is_empty(), "orphaned materialized observation book");
    Ok(batches)
}

fn rebase(
    events: &mut [Value],
    materialized_books: &mut BTreeMap<u64, Value>,
    after: u64,
) -> Result<()> {
    for event in events {
        let local = event["cursor"].as_u64().context("local cursor")?;
        event["cursor"] = json!(after.checked_add(local).context("cursor overflow")?);
        if event["event_type"] == "recovery_completed" {
            let snapshot = event["canonical_payload"]["snapshot_cursor"]
                .as_u64()
                .context("snapshot cursor")?;
            ensure!(
                snapshot > 0 && snapshot < local,
                "snapshot must belong to this actor result"
            );
            event["canonical_payload"]["snapshot_cursor"] =
                json!(after.checked_add(snapshot).context("snapshot overflow")?);
            event["canonical_payload_sha256"] = json!(canonical_hash(&event["canonical_payload"]));
        }
        event.as_object_mut().context("event")?.remove("event_id");
        event["event_id"] = json!(canonical_hash(event));
    }
    if !materialized_books.is_empty() {
        *materialized_books = std::mem::take(materialized_books)
            .into_iter()
            .map(|(cursor, book)| {
                Ok((
                    after
                        .checked_add(cursor)
                        .context("materialized cursor overflow")?,
                    book,
                ))
            })
            .collect::<Result<_>>()?;
    }
    Ok(())
}

pub(crate) fn compact_incremental_events(
    mut events: Vec<Value>,
) -> Result<(Vec<Value>, BTreeMap<u64, Value>)> {
    let mut materialized = BTreeMap::new();
    for event in &mut events {
        if event["event_type"] != "book" {
            continue;
        }
        let source_type = event["raw_payload"]["event_type"].as_str();
        if !matches!(source_type, Some("price_change" | "last_trade_price")) {
            continue;
        }
        let cursor = event["cursor"].as_u64().context("compact local cursor")?;
        let token = event["token_id"]
            .as_str()
            .context("compact token")?
            .to_owned();
        let book = event["canonical_payload"].clone();
        let book_hash = event["canonical_payload_sha256"].clone();
        let book_bytes = bounded_size(&book, FRAME_BYTES)?;
        let mut compact = json!({
            "payload_semantics":if source_type == Some("price_change") {
                "ordered_depth_delta_with_complete_post_state_commitment"
            } else {
                "last_trade_with_unchanged_depth_commitment"
            },
            "token_id":book["token_id"],"condition_id":book["condition_id"],
            "book_epoch":book["book_epoch"],"sequence":book["sequence"],
            "sequence_semantics":book["sequence_semantics"],
            "exchange_at":book["exchange_at"],"received_at":book["received_at"],
            "tick_version":book["tick_version"],"tick_size":book["tick_size"],
            "last_trade_price":book["last_trade_price"],
            "state_checksum":book["state_checksum"],"source_hash":book["source_hash"],
            "canonical_book_sha256":book_hash,"materialized_book_bytes":book_bytes,
        });
        if source_type == Some("price_change") {
            let delta = event
                .as_object_mut()
                .context("compact event")?
                .remove("_replay_complete_delta")
                .context("replay-complete compact delta absent")?;
            let changes = delta["changes"]
                .as_array()
                .context("compact replay changes")?;
            ensure!(!changes.is_empty(), "compact token delta absent");
            ensure!(
                changes.iter().all(|change| change["asset_id"] == token),
                "compact replay delta crosses token"
            );
            compact["delta"] = delta;
        } else {
            compact["trade"] = json!({"price":book["last_trade_price"],
                "exchange_at":event["exchange_at"],"received_at":event["received_at"]});
        }
        event["canonical_payload"] = compact;
        event["canonical_payload_sha256"] = json!(canonical_hash(&event["canonical_payload"]));
        event
            .as_object_mut()
            .context("compact event")?
            .remove("event_id");
        event["event_id"] = json!(canonical_hash(event));
        materialized.insert(cursor, book);
    }
    Ok((events, materialized))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};

    fn fixture_markets() -> Vec<Value> {
        [("1", "c1", ["11", "12"]), ("2", "c2", ["21", "22"])]
            .into_iter().map(|(market, condition, tokens)| json!({
                "identity":{"market_id":market,"condition_id":condition,
                    "outcomes":[{"outcome":"Up","token_id":tokens[0]},
                        {"outcome":"Down","token_id":tokens[1]}]},
                "lifecycle_state":"active"
            })).collect()
    }
    fn fixture_publication(router: &Adapter) -> Publication {
        let tokens = router.identities.iter()
            .map(|(token, (market, _))| (token.clone(), market.clone())).collect();
        Publication::start(0, Some(json!({"latest_cursor":0,
            "catalog_revision":"catalog","markets":fixture_markets(),
            "books":[],"gaps":[]})), tokens, 32, 1_048_576, 65_536,
            |_| Ok(())).unwrap()
    }
    fn market_event_cursor(publication: &Publication, market: &str) -> u64 {
        publication.next_market_event_cursor(market).unwrap() - 1
    }
    fn market_durable_cursor(publication: &Publication, market: &str) -> u64 {
        publication.reader().market_stream_watermark(market).unwrap()
            .map_or(0, |watermark| watermark.durable_cursor)
    }

    #[test]
    fn observation_partition_preserves_order_and_materialized_books() {
        let events = vec![
            json!({"event_type":"book","token_id":"11","received_at":"at-1","cursor":1}),
            json!({"event_type":"book","token_id":"12","received_at":"at-1","cursor":2}),
            json!({"event_type":"book","token_id":"11","received_at":"at-1","cursor":3}),
            json!({"event_type":"book","token_id":"12","received_at":"at-2","cursor":4}),
        ];
        let books = BTreeMap::from([(2, json!({"book":2})), (4, json!({"book":4}))]);
        let groups = partition_observation_batches(events, books, false).unwrap();
        assert_eq!(groups.iter().map(|(events, _)| events.len()).collect::<Vec<_>>(), [2, 1, 1]);
        assert_eq!(groups[0].1.keys().copied().collect::<Vec<_>>(), [2]);
        assert_eq!(groups[2].1.keys().copied().collect::<Vec<_>>(), [4]);
        assert_eq!(groups.iter().flat_map(|(events, _)| events.iter())
            .map(|event| event["cursor"].as_u64().unwrap()).collect::<Vec<_>>(), [1, 2, 3, 4]);
    }

    #[test]
    fn observation_partition_keeps_recovery_triple_and_rejects_orphan_completion() {
        let events = vec![
            json!({"event_type":"recovery_started","token_id":"11","received_at":"at-1","cursor":1}),
            json!({"event_type":"book","token_id":"11","received_at":"at-1","cursor":2}),
            json!({"event_type":"recovery_completed","token_id":"11","received_at":"at-1","cursor":3}),
            json!({"event_type":"book","token_id":"11","received_at":"at-1","cursor":4}),
        ];
        let groups = partition_observation_batches(events, BTreeMap::new(), false).unwrap();
        assert_eq!(groups.iter().map(|(events, _)| events.len()).collect::<Vec<_>>(), [3, 1]);
        assert!(partition_observation_batches(vec![json!({"event_type":"recovery_completed",
            "token_id":"11","received_at":"at-1","cursor":1})], BTreeMap::new(), false).is_err());
    }

    #[tokio::test]
    async fn coalesced_ws_recovery_and_later_book_publish() {
        let mut router = fixture();
        let tokens = router.identities.iter()
            .map(|(token, (market, _))| (token.clone(), market.clone())).collect();
        let mut publication = Publication::start(0,
            Some(json!({"latest_cursor":0,"catalog_revision":"catalog",
                "markets":fixture_markets(),"books":[],"gaps":[]})),
            tokens, 8, 1_048_576, 65_536, |_| Ok(())).unwrap();
        let (mut pipeline, mut output) = Pipeline::start_with_workers(&router, &publication, 1).unwrap();
        pipeline.submit_batch(&mut router, &mut publication, vec![book("11"), book("12")]).await.unwrap();
        pipeline.publish(&mut router, &mut publication, next(&mut output).await).await.unwrap();
        let mut invalid = book("11");
        invalid.raw_payload["event_type"] = json!("price_change");
        invalid.raw_payload["market"] = json!("wrong-condition");
        invalid.raw_payload["price_changes"] = json!([{"asset_id":"11","side":"BUY","price":"0.4","size":"7"}]);
        let mut snapshot = book("11");
        snapshot.received_at = invalid.received_at;
        snapshot.raw_payload["hash"] = json!("recovery-snapshot");
        let mut later = snapshot.clone();
        later.raw_payload["hash"] = json!("later-book");
        pipeline.submit_batch(&mut router, &mut publication, vec![invalid, snapshot, later]).await.unwrap();
        let completion = next(&mut output).await;
        assert_eq!(completion.result.as_ref().unwrap().published_event_types.iter()
            .map(String::as_str).collect::<Vec<_>>(),
            ["recovery_started", "book", "recovery_completed", "book"]);
        pipeline.publish(&mut router, &mut publication, completion).await.unwrap();
        assert_eq!(market_event_cursor(&publication, "1"), 6);
        pipeline.dispatch.close();
        pipeline.dispatch.join().await;
        publication.finish().await.unwrap();
    }

    #[tokio::test]
    async fn coalesced_confirmation_then_mutation_does_not_confirm_obsolete_depth() {
        let mut router = fixture();
        let tokens = router.identities.iter()
            .map(|(token, (market, _))| (token.clone(), market.clone())).collect();
        let mut publication = Publication::start(0,
            Some(json!({"latest_cursor":0,"catalog_revision":"catalog",
                "markets":fixture_markets(),"books":[],"gaps":[]})),
            tokens, 8, 1_048_576, 65_536, |_| Ok(())).unwrap();
        let (mut pipeline, mut output) = Pipeline::start_with_workers(&router, &publication, 1).unwrap();
        pipeline.submit_batch(&mut router, &mut publication, vec![book("11"), book("12")]).await.unwrap();
        pipeline.publish(&mut router, &mut publication, next(&mut output).await).await.unwrap();
        let now = Utc::now().with_nanosecond(0).unwrap() + chrono::Duration::seconds(2);
        let delta = |token: &str, size: &str, offset: i64| {
            let at = now + chrono::Duration::milliseconds(offset);
            RawTransportFrame {received_at: at,
                raw_payload: json!({"event_type":"price_change","market":"c1",
                    "timestamp":at.timestamp_millis().to_string(),
                    "price_changes":[{"asset_id":token,"side":"BUY","price":"0.4",
                        "size":size,"best_bid":"0.4","best_ask":"0.6"}]})}
        };
        pipeline.submit_batch(&mut router, &mut publication,
            vec![delta("11", "10", 0), delta("12", "10", 1), delta("11", "9", 2)]).await.unwrap();
        let completion = next(&mut output).await;
        let confirmed = publication.reader().capture_market("1", &["11".into(), "12".into()]).unwrap();
        assert!(!confirmed.confirmation_facts.contains_key("11"));
        assert!(confirmed.confirmation_facts.contains_key("12"));
        pipeline.publish(&mut router, &mut publication, completion).await.unwrap();
        let view = publication.reader().capture_market("1", &["11".into(), "12".into()]).unwrap();
        assert_eq!(view.books["11"]["bids"][0]["size"], "9");
        assert!(!view.confirmation_facts.contains_key("11"));
        assert!(view.confirmation_facts.contains_key("12"));
        // Mutation followed by a matching observation still confirms normally.
        pipeline.submit_batch(&mut router, &mut publication,
            vec![delta("11", "8", 2000), delta("11", "8", 4000)]).await.unwrap();
        pipeline.publish(&mut router, &mut publication, next(&mut output).await).await.unwrap();
        let view = publication.reader().capture_market("1", &["11".into(), "12".into()]).unwrap();
        assert_eq!(view.books["11"]["bids"][0]["size"], "8");
        assert!(view.confirmation_facts.contains_key("11"));
        pipeline.dispatch.close();
        pipeline.dispatch.join().await;
        publication.finish().await.unwrap();
    }

    #[tokio::test]
    async fn bad_market_in_shared_transport_batch_does_not_block_other_market_publication() {
        let mut router = fixture();
        let tokens = router.identities.iter()
            .map(|(token, (market, _))| (token.clone(), market.clone())).collect();
        let mut publication = Publication::start(0,
            Some(json!({"latest_cursor":0,"catalog_revision":"catalog",
                "markets":fixture_markets(),"books":[],"gaps":[]})),
            tokens, 8, 1_048_576, 65_536, |_| Ok(())).unwrap();
        let (mut pipeline, mut output) = Pipeline::start_with_workers(&router, &publication, 2).unwrap();
        let mut bad_a = book("11");
        bad_a.raw_payload["market"] = json!("wrong-condition");
        pipeline.submit_batch(&mut router, &mut publication,
            vec![bad_a, book("21")]).await.unwrap();
        for _ in 0..2 {
            pipeline.publish(&mut router, &mut publication,
                next(&mut output).await).await.unwrap();
        }
        let reader = publication.reader();
        let b = reader.market_stream_watermark("2").unwrap().unwrap();
        assert_eq!(b.published_cursor, 1);
        let page = reader.replay_market("2", &b.stream_epoch, 0, 1, 65_536).unwrap();
        assert_eq!(page.groups.len(), 1);
        assert_eq!(page.groups[0].market_id, "2");
        assert!(page.groups[0].payload.validated.events().iter()
            .all(|event| event["market_id"] == "2"));
        assert!(router.recoveries.contains_key("11"));
        assert!(!router.recoveries.contains_key("21"));
        pipeline.dispatch.close();
        pipeline.dispatch.join().await;
        publication.finish().await.unwrap();
    }

    #[test]
    fn one_liquid_market_cannot_monopolize_a_shared_transport_batch() {
        let mut grouped = BTreeMap::<String, VecDeque<RawTransportFrame>>::new();
        for index in 0..(INPUT_FRAME_BATCH * 2 + 1) {
            let mut frame = book("11");
            frame.raw_payload["hash"] = json!(format!("a-{index}"));
            grouped.entry("1".into()).or_default().push_back(frame);
        }
        grouped.entry("2".into()).or_default().push_back(book("21"));
        let first = next_market_input_pass(&mut grouped);
        assert_eq!(first.iter().map(|(market, frames)| (market.as_str(), frames.len()))
            .collect::<Vec<_>>(), [("1", INPUT_FRAME_BATCH), ("2", 1)]);
        assert_eq!(first[0].1[0].raw_payload["hash"], "a-0");
        assert_eq!(first[0].1.last().unwrap().raw_payload["hash"], "a-63");
        let second = next_market_input_pass(&mut grouped);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].0, "1");
        assert_eq!(second[0].1[0].raw_payload["hash"], "a-64");
        assert_eq!(second[0].1.last().unwrap().raw_payload["hash"], "a-127");
        let last = next_market_input_pass(&mut grouped);
        assert_eq!(last[0].1[0].raw_payload["hash"], "a-128");
        assert!(grouped.is_empty());
    }

    #[tokio::test]
    async fn sixty_four_frames_use_one_market_actor_input() {
        let mut router = fixture();
        let tokens = router.identities.iter()
            .map(|(token, (market, _))| (token.clone(), market.clone())).collect();
        let mut publication = Publication::start(0,
            Some(json!({"latest_cursor":0,"catalog_revision":"catalog",
                "markets":fixture_markets(),"books":[],"gaps":[]})),
            tokens, 8, 1_048_576, 65_536, |_| Ok(())).unwrap();
        let (mut pipeline, mut output) = Pipeline::start_with_workers(&router, &publication, 1).unwrap();
        let frames = (0..64).map(|sequence| {
            let mut frame = book("11");
            frame.raw_payload["hash"] = json!(format!("book-{sequence}"));
            frame
        }).collect();
        pipeline.submit_batch(&mut router, &mut publication, frames).await.unwrap();
        assert_eq!(pipeline.admission.lock().unwrap().1["accepted"], 1);
        pipeline.publish(&mut router, &mut publication, next(&mut output).await).await.unwrap();
        // Unchanged full-book observations may become memory-only freshness
        // confirmations; the actor input still admitted all 64 frames once.
        assert!((1..=64).contains(&market_event_cursor(&publication, "1")));
        pipeline.dispatch.close();
        pipeline.dispatch.join().await;
        publication.finish().await.unwrap();
    }

    /// Manual characterization only; machine timing is not an acceptance gate.
    #[tokio::test]
    #[ignore = "run explicitly to characterize the owner publication path"]
    async fn owner_batch_throughput_characterization() {
        async fn run(group_size: usize) -> std::time::Duration {
            let mut router = fixture();
            let tokens = router.identities.iter()
                .map(|(token, (market, _))| (token.clone(), market.clone())).collect();
            let mut publication = Publication::start(0,
                Some(json!({"latest_cursor":0,"catalog_revision":"catalog",
                    "markets":fixture_markets(),"books":[],"gaps":[]})),
                tokens, 2048, 64 * 1024 * 1024, 65_536, |_| Ok(())).unwrap();
            let (mut pipeline, mut output) = Pipeline::start_with_workers(&router, &publication, 1).unwrap();
            let started = std::time::Instant::now();
            for block in (0..1024).step_by(group_size) {
                let frames = (block..block + group_size).map(|sequence| {
                    let mut frame = book("11");
                    frame.raw_payload["hash"] = json!(format!("book-{sequence}"));
                    frame
                }).collect();
                pipeline.submit_batch(&mut router, &mut publication, frames).await.unwrap();
                pipeline.publish(&mut router, &mut publication,
                    next(&mut output).await).await.unwrap();
            }
            assert_eq!(market_event_cursor(&publication, "1"), 1024);
            let elapsed = started.elapsed();
            marketcow_polymarket::async_eprintln!("owner_batch_stage group={} pipeline={} publication={}",
                group_size, pipeline.take_hot_path_timing(), publication.take_hot_path_timing());
            pipeline.dispatch.close();
            pipeline.dispatch.join().await;
            publication.finish().await.unwrap();
            elapsed
        }
        let single = run(1).await;
        let coalesced = run(64).await;
        marketcow_polymarket::async_eprintln!("owner_batch_characterization single_ms={} coalesced_ms={}",
            single.as_millis(), coalesced.as_millis());
    }
    fn fixture() -> Adapter {
        let plan = Plan {
            schema_version: "test".into(),
            catalog_revision: "catalog".into(),
            markets: vec![
                super::super::super::Market {
                    market_id: "1".into(),
                    condition_id: "c1".into(),
                    token_ids: ["11".into(), "12".into()],
                },
                super::super::super::Market {
                    market_id: "2".into(),
                    condition_id: "c2".into(),
                    token_ids: ["21".into(), "22".into()],
                },
            ],
        };
        Adapter::new(&plan, &json!({"scope_id":"scope","markets":[{"identity":{"market_id":"1"},"lifecycle_state":"active"},
            {"identity":{"market_id":"2"},"lifecycle_state":"active"}],"books":[{"token_id":"11","tick_size":"0.01"},
            {"token_id":"12","tick_size":"0.01"},{"token_id":"21","tick_size":"0.01"},{"token_id":"22","tick_size":"0.01"}]})).unwrap()
    }
    fn book(token: &str) -> RawTransportFrame {
        let now = Utc::now().with_nanosecond(0).unwrap();
        RawTransportFrame {
            received_at: now,
            raw_payload: json!({"event_type":"book","asset_id":token,
            "market":if token.starts_with('1') {"c1"} else {"c2"},"timestamp":now.timestamp_millis().to_string(),
            "hash":"source","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}]}),
        }
    }
    async fn next(output: &mut MarketCompletions<Output>) -> Completion<Output> {
        tokio::time::timeout(std::time::Duration::from_secs(2), output.recv())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn connection_checkpoint_receipt_requires_atomic_publication_and_preserves_other_market() {
        let mut router = fixture();
        let publication = fixture_publication(&router);
        let (mut pipeline, mut output) = Pipeline::start_with_workers(&router,&publication,2).unwrap();
        let up = book("11");
        let down = book("12");
        pipeline.submit_routed_batches(&mut router,&publication,
            vec![("1".into(),vec![up.clone(),down.clone()])]).await.unwrap();
        next(&mut output).await.result.unwrap();
        let before = publication.reader().capture_market("1",&["11".into(),"12".into()]).unwrap();
        let other = publication.reader().market_stream_watermark("2").unwrap();
        let frame = RawTransportFrame {received_at:down.received_at,raw_payload:json!({
            "event_type":"marketcow_connection_checkpoint",
            "schema_version":"marketcow.polymarket.connection-checkpoint.v1",
            "market":"c1","token_ids":["11","12"],
            "rotation_id":1,"attempt":1,"connection_id":"test-standby",
            "source_frames":[
                {"raw_payload":up.raw_payload,"received_at":up.received_at},
                {"raw_payload":down.raw_payload,"received_at":down.received_at},
            ]})};
        let (tx,rx) = tokio::sync::oneshot::channel();
        pipeline.submit_connection_checkpoint(&router,"1".into(),vec![frame.clone()],tx).unwrap();
        assert!(tokio::time::timeout(std::time::Duration::from_secs(2),rx).await.unwrap().unwrap());
        let done = next(&mut output).await.result.unwrap();
        assert_eq!(done.published_groups,1);
        assert_eq!(done.published_event_types,vec!["book","book"]);
        let after = publication.reader().capture_market("1",&["11".into(),"12".into()]).unwrap();
        assert_eq!(after.watermark.published_cursor,before.watermark.published_cursor+1);
        assert!(after.recoveries.is_empty());
        assert_eq!(after.market_book_cursors["11"],after.market_book_cursors["12"]);
        assert_eq!(after.watermark.stream_epoch,before.watermark.stream_epoch);
        let page = publication.reader().replay_market("1",&after.watermark.stream_epoch,
            before.watermark.published_cursor,1,OUTPUT_BYTES).unwrap();
        assert_eq!(page.groups.len(),1);
        for event in page.groups[0].payload.validated.events() {
            assert_eq!(event["raw_payload"]["event_type"],"book");
            assert!(matches!(crate::source_btc5m_proto::market_event(event).unwrap().payload,
                Some(marketcow_contracts::btc5m_market_local_v5::wire::market_event::Payload::BookSnapshot(_))));
        }
        assert_eq!(publication.reader().market_stream_watermark("2").unwrap(),other);
        let mut broken = frame;
        broken.raw_payload["source_frames"][1]["raw_payload"]["market"] = json!("wrong");
        let (tx,rx) = tokio::sync::oneshot::channel();
        pipeline.submit_connection_checkpoint(&router,"1".into(),vec![broken],tx).unwrap();
        assert!(!tokio::time::timeout(std::time::Duration::from_secs(2),rx).await.unwrap().unwrap());
        assert_eq!(next(&mut output).await.result.unwrap().published_groups,0);
        let rejected = publication.reader().capture_market("1",&["11".into(),"12".into()]).unwrap();
        assert_eq!(rejected.watermark.published_cursor,after.watermark.published_cursor);
        assert!(rejected.recoveries.is_empty());
        pipeline.dispatch.close();
        while output.recv().await.is_some() {}
        pipeline.dispatch.join().await;
        publication.finish().await.unwrap();
    }

    #[test]
    fn incremental_history_is_compact_but_keeps_exact_materialized_post_state() {
        let original = super::super::btc5m_reducer_fixture_sequence().unwrap();
        let full_change = original
            .iter()
            .find(|event| event["raw_payload"]["event_type"] == "price_change")
            .unwrap()["canonical_payload"]
            .clone();
        let full_trade = original
            .iter()
            .find(|event| event["raw_payload"]["event_type"] == "last_trade_price")
            .unwrap()["canonical_payload"]
            .clone();
        let (compact, materialized) = compact_incremental_events(original).unwrap();
        let change = compact
            .iter()
            .find(|event| event["raw_payload"]["event_type"] == "price_change")
            .unwrap();
        let trade = compact
            .iter()
            .find(|event| event["raw_payload"]["event_type"] == "last_trade_price")
            .unwrap();
        assert!(change["canonical_payload"].get("bids").is_none());
        assert!(trade["canonical_payload"].get("asks").is_none());
        assert_eq!(
            change["canonical_payload"]["payload_semantics"],
            "ordered_depth_delta_with_complete_post_state_commitment"
        );
        assert_eq!(
            trade["canonical_payload"]["payload_semantics"],
            "last_trade_with_unchanged_depth_commitment"
        );
        assert_eq!(
            materialized[&change["cursor"].as_u64().unwrap()],
            full_change
        );
        assert_eq!(materialized[&trade["cursor"].as_u64().unwrap()], full_trade);
        crate::source_publication::validate_materialized_books(&compact, &materialized).unwrap();
    }


    #[tokio::test]
    async fn scope_actor_retirement_preserves_other_market_and_readd_is_new() {
        let mut router = fixture();
        let admitted = fixture();
        let publication = fixture_publication(&router);
        let (mut pipeline, mut output) = Pipeline::start_with_workers(&router, &publication, 6).unwrap();
        let retained = pipeline.controls["2"].clone();
        let old = pipeline.controls["1"].clone();
        let removed = pipeline.remove_market(&mut router, "1").unwrap();
        assert_eq!(removed, vec!["11", "12"]);
        assert!(!router.identities.contains_key("11"));
        assert!(router.ticks.contains_key("21"));
        assert!(Arc::ptr_eq(&retained, &pipeline.controls["2"]));
        assert!(pipeline.add_market(&router, &publication, "1").is_err());
        for token in removed {
            router
                .identities
                .insert(token.clone(), admitted.identities[&token].clone());
            router
                .ticks
                .insert(token.clone(), admitted.ticks[&token].clone());
        }
        pipeline.add_market(&router, &publication, "1").unwrap();
        assert!(!Arc::ptr_eq(&old, &pipeline.controls["1"]));
        assert!(pipeline.add_market(&router, &publication, "1").is_err());
        assert!(Arc::ptr_eq(&retained, &pipeline.controls["2"]));
        pipeline.dispatch.close();
        pipeline.dispatch.join().await;
        assert!(output.recv().await.is_none());
        publication.finish().await.unwrap();
    }

    #[tokio::test]
    async fn installed_market_requires_admitted_metadata_and_starts_without_fabricated_book() {
        let mut router = fixture();
        let tokens = router
            .identities
            .iter()
            .map(|(token, (market, _))| (token.clone(), market.clone()))
            .collect();
        let mut publication=Publication::start(0,Some(json!({"latest_cursor":0,"catalog_revision":"catalog","markets":fixture_markets(),"books":[],"gaps":[]})),tokens,4,65536,16384,|_|Ok(())).unwrap();
        let (mut pipeline, mut output) = Pipeline::start_with_workers(&router, &publication, 6).unwrap();
        let retained = pipeline.controls["1"].clone();
        let market = super::super::super::Market {
            market_id: "3".into(),
            condition_id: "c3".into(),
            token_ids: ["31".into(), "32".into()],
        };
        assert!(
            pipeline
                .install_admitted_market(&mut router, &publication, &market)
                .is_err()
        );
        assert!(!router.identities.contains_key("31"));
        let records = vec![
            json!({"identity":{"market_id":"3","condition_id":"c3","outcomes":[{"token_id":"31"},{"token_id":"32"}]},"lifecycle_state":"active"}),
        ];
        let hash = canonical_hash(&json!(records));
        publication
            .admit_catalog_markets("catalog", records, &hash)
            .unwrap();
        publication.pause_source().unwrap();
        assert!(publication.capture_view().is_err());
        assert!(
            pipeline
                .install_admitted_market(&mut router, &publication, &market)
                .unwrap()
        );
        assert!(
            !pipeline
                .install_admitted_market(&mut router, &publication, &market)
                .unwrap()
        );
        assert!(Arc::ptr_eq(&retained, &pipeline.controls["1"]));
        for token in ["31", "32"] {
            assert!(router.waiting.contains(token));
            assert!(!router.ticks.contains_key(token));
            assert!(!router.book_received.contains_key(token));
        }
        assert!(publication.capture_view().is_err());
        pipeline.dispatch.close();
        pipeline.dispatch.join().await;
        assert!(output.recv().await.is_none());
        publication.finish().await.unwrap();
    }

    #[tokio::test]
    async fn intentional_pause_fences_actor_results_without_creating_recovery() {
        let mut router = fixture();
        let tokens = router
            .identities
            .iter()
            .map(|(token, (market, _))| (token.clone(), market.clone()))
            .collect();
        let mut publication = Publication::start(
            0,
            Some(json!({"latest_cursor":0,"catalog_revision":"catalog",
            "markets":fixture_markets(),"books":[],"gaps":[]})),
            tokens,
            4,
            65536,
            16384,
            |_| Ok(()),
        )
        .unwrap();
        let (mut pipeline, mut output) = Pipeline::start_with_workers(&router, &publication, 1).unwrap();
        pipeline
            .submit(&mut router, &mut publication, book("11"), None)
            .await
            .unwrap();
        pipeline.pause_all(&mut router).unwrap();
        if let Ok(Some(completed)) =
            tokio::time::timeout(std::time::Duration::from_millis(100), output.recv()).await
        {
            pipeline
                .publish(&mut router, &mut publication, completed)
                .await
                .unwrap();
        }
        assert_eq!(market_event_cursor(&publication, "1"), 0);
        assert!(router.recoveries.is_empty());
        pipeline.dispatch.close();
        pipeline.dispatch.join().await;
        publication.finish().await.unwrap();
    }

    #[tokio::test]
    async fn state_identical_new_ws_observation_confirms_without_cursor_or_durability() {
        let mut router = fixture();
        let tokens = router
            .identities
            .iter()
            .map(|(token, (market, _))| (token.clone(), market.clone()))
            .collect();
        let mut publication = Publication::start(
            0,
            Some(json!({"latest_cursor":0,"catalog_revision":"catalog",
                "markets":fixture_markets(),"books":[],"gaps":[]})),
            tokens,
            8,
            1048576,
            65536,
            |_| Ok(()),
        )
        .unwrap();
        let (mut pipeline, mut output) = Pipeline::start_with_workers(&router, &publication, 1).unwrap();
        for token in ["11", "12"] {
            pipeline
                .submit(&mut router, &mut publication, book(token), None)
                .await
                .unwrap();
            let completed = next(&mut output).await;
            pipeline
                .publish(&mut router, &mut publication, completed)
                .await
                .unwrap();
        }
        let cursor = market_event_cursor(&publication, "1");
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let watermark = publication.reader().market_stream_watermark("1")
                    .unwrap().unwrap();
                if watermark.durable_cursor > 0
                    && watermark.durable_cursor == watermark.published_cursor {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }).await.expect("initial book writes become durable");
        let now = Utc::now().with_nanosecond(0).unwrap() + chrono::Duration::seconds(2);
        let delta = RawTransportFrame {
            received_at: now,
            raw_payload: json!({"event_type":"price_change","market":"c1",
                "timestamp":now.timestamp_millis().to_string(),
                "price_changes":[{"asset_id":"11","side":"BUY","price":"0.4",
                    "size":"10","best_bid":"0.4","best_ask":"0.6"}]}),
        };
        pipeline
            .submit(&mut router, &mut publication, delta, None)
            .await
            .unwrap();
        let completed = next(&mut output).await;
        pipeline
            .publish(&mut router, &mut publication, completed)
            .await
            .unwrap();
        let view = publication.reader().capture_market("1", &["11".into(), "12".into()]).unwrap();
        assert_eq!(market_event_cursor(&publication, "1"), cursor);
        assert_eq!(view.confirmation_sequence, 1);
        assert_eq!(view.market_confirmation_versions.get("11"), Some(&1));
        assert!(!view.market_confirmation_versions.contains_key("12"));
        assert_eq!(
            view.confirmation_facts["11"]["confirmation_source"],
            "polymarket_websocket_state_confirmation"
        );
        pipeline.dispatch.close();
        pipeline.dispatch.join().await;
        publication.finish().await.unwrap();
    }

    #[tokio::test]
    async fn admitted_recovery_is_fenced_again_and_unadmitted_retry_is_coalesced() {
        let mut router = fixture();
        let tokens = router
            .identities
            .iter()
            .map(|(t, (m, _))| (t.clone(), m.clone()))
            .collect();
        let base = json!({"latest_cursor":0,"books":[],"markets":fixture_markets(),"gaps":[],
            "schema_version":"marketcow.polymarket.live-stream.v1","type":"state"});
        let mut publication =
            Publication::start(0, Some(base), tokens, 32, 1048576, 65536, |_| Ok(())).unwrap();
        let (pipeline, mut output) = Pipeline::start_with_workers(&router, &publication, 8).unwrap();
        assert!(Pipeline::start_with_workers(&router, &publication, 0).is_err());
        assert!(Pipeline::start_with_workers(&router, &publication, 9).is_err());
        pipeline
            .invalidate_market(&mut router, &mut publication, "1", "market_queue_capacity")
            .await
            .unwrap();
        let first = router.recoveries["11"].clone();
        pipeline
            .invalidate_market(&mut router, &mut publication, "1", "market_queue_capacity")
            .await
            .unwrap();
        assert_eq!(router.recoveries["11"], first);
        assert_eq!(market_event_cursor(&publication, "1"), 2);
        let mut delta = book("11");
        delta.raw_payload["event_type"] = json!("price_change");
        pipeline
            .submit(&mut router, &mut publication, delta, None)
            .await
            .unwrap();
        assert!(!pipeline.controls["1"].lock().unwrap().baseline_admitted);
        pipeline
            .submit(
                &mut router,
                &mut publication,
                book("11"),
                Some(("11".into(), first)),
            )
            .await
            .unwrap();
        let stale = next(&mut output).await;
        pipeline
            .invalidate_market(&mut router, &mut publication, "1", "market_queue_capacity")
            .await
            .unwrap();
        let after_invalidation = market_event_cursor(&publication, "1");
        assert!(after_invalidation >= 4,
            "actor-local publication may precede its control receipt");
        let current = router.recoveries["11"].clone();
        pipeline
            .publish(&mut router, &mut publication, stale)
            .await
            .unwrap();
        assert_eq!(market_event_cursor(&publication, "1"), after_invalidation);
        assert_eq!(router.recoveries["11"], current);
        pipeline
            .submit(
                &mut router,
                &mut publication,
                book("11"),
                Some(("11".into(), current)),
            )
            .await
            .unwrap();
        pipeline
            .publish(&mut router, &mut publication, next(&mut output).await)
            .await
            .unwrap();
        assert_eq!(market_event_cursor(&publication, "1"), after_invalidation + 2);
        assert!(!router.recoveries.contains_key("11"));
        pipeline
            .finish(&mut router, &mut publication, &mut output)
            .await
            .unwrap();
        publication.finish().await.unwrap();
    }
    #[tokio::test]
    async fn rest_replacement_is_one_atomic_two_token_recovery_batch() {
        let mut router = fixture();
        let tokens = router
            .identities
            .iter()
            .map(|(token, (market, _))| (token.clone(), market.clone()))
            .collect();
        let written = Arc::new(Mutex::new(Vec::<Vec<Value>>::new()));
        let capture = written.clone();
        let base = json!({"latest_cursor":0,"books":[],"markets":fixture_markets(),"gaps":[],
            "schema_version":"marketcow.polymarket.live-stream.v1","type":"state"});
        let mut publication =
            Publication::start(0, Some(base), tokens, 32, 1048576, 65536, move |batches| {
                for batch in batches {
                    capture
                        .lock()
                        .unwrap()
                        .push(batch.validated.events().to_vec());
                }
                Ok(())
            })
            .unwrap();
        let (pipeline, mut output) = Pipeline::start(&router, &publication).unwrap();
        // Transport clocks commonly carry nanoseconds while the wire/durable
        // contract is microsecond precise. The pipeline owns that boundary
        // normalization for both REST and targeted-WS replacement inputs.
        let now = Utc::now().with_nanosecond(123_456_789).unwrap();
        let mut left = book("11");
        let mut right = book("12");
        for frame in [&mut left, &mut right] {
            frame.raw_payload["timestamp"] = json!(now.timestamp_millis().to_string());
            frame
                .raw_payload
                .as_object_mut()
                .unwrap()
                .remove("event_type");
            frame.raw_payload["tick_size"] = json!("0.01");
        }
        pipeline
            .submit_rest_replacement(
                &mut router,
                &mut publication,
                vec![left.raw_payload, right.raw_payload],
                now,
                "1",
                "rest_confirmation_mismatch",
            )
            .await
            .unwrap();
        let completion = next(&mut output).await;
        assert_eq!(
            completion.result.as_ref().unwrap().published_event_types.iter()
                .map(String::as_str).collect::<Vec<_>>(),
            vec![
                "recovery_started",
                "recovery_started",
                "book",
                "recovery_completed",
                "book",
                "recovery_completed"
            ]
        );
        pipeline
            .publish(&mut router, &mut publication, completion)
            .await
            .unwrap();
        assert_eq!(market_event_cursor(&publication, "1"), 6);
        assert!(router.recoveries.is_empty());
        assert!(pipeline.controls["1"].lock().unwrap().attempts.is_empty());
        pipeline
            .finish(&mut router, &mut publication, &mut output)
            .await
            .unwrap();
        publication.finish().await.unwrap();
        let batches = written.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 6);
    }

    #[tokio::test]
    async fn market_gap_does_not_hold_other_market_publication() {
        let mut router = fixture();
        let tokens = router.identities.iter()
            .map(|(token, (market, _))| (token.clone(), market.clone())).collect();
        let written = Arc::new(Mutex::new(Vec::<Value>::new()));
        let capture = written.clone();
        let mut publication = Publication::start(0,
            Some(json!({"latest_cursor":0,"books":[],"markets":fixture_markets(),
                "gaps":[],"schema_version":"marketcow.polymarket.live-stream.v1",
                "type":"state"})), tokens, 32, 1_048_576, 65_536,
            move |batches| {
                for batch in batches {
                    capture.lock().unwrap()
                        .extend_from_slice(batch.validated.events());
                }
                Ok(())
            }).unwrap();
        let (pipeline, mut output) = Pipeline::start(&router, &publication).unwrap();
        pipeline.submit(&mut router, &mut publication, book("11"), None).await.unwrap();
        pipeline.publish(&mut router, &mut publication, next(&mut output).await)
            .await.unwrap();
        let a_before = market_event_cursor(&publication, "1");
        let b_before = market_event_cursor(&publication, "2");
        pipeline.invalidate_market(&mut router, &mut publication, "1",
            "market_queue_capacity").await.unwrap();
        let a_gap = market_event_cursor(&publication, "1");
        assert!(a_gap > a_before);
        pipeline.submit(&mut router, &mut publication, book("21"), None).await.unwrap();
        pipeline.publish(&mut router, &mut publication, next(&mut output).await)
            .await.unwrap();
        assert_eq!(market_event_cursor(&publication, "1"), a_gap);
        assert!(market_event_cursor(&publication, "2") > b_before);
        let attempts = router.recoveries.iter()
            .filter(|(token, _)| token.as_str() == "11" || token.as_str() == "12")
            .map(|(token, attempt)| (token.clone(), attempt.clone()))
            .collect();
        assert!(pipeline.submit_rest_recovery(&mut router, &mut publication,
            vec![book("11").raw_payload, book("12").raw_payload],
            Utc::now(), attempts).await.unwrap());
        pipeline.publish(&mut router, &mut publication, next(&mut output).await)
            .await.unwrap();
        assert!(market_event_cursor(&publication, "1") > a_gap);
        assert_eq!(market_event_cursor(&publication, "2"), b_before + 1);
        pipeline.finish(&mut router, &mut publication, &mut output).await.unwrap();
        publication.finish().await.unwrap();
        let events = written.lock().unwrap();
        assert!(events.iter().any(|event| event["market_id"] == "1"
            && event["event_type"] == "recovery_started"));
        assert!(events.iter().any(|event| event["market_id"] == "2"
            && event["token_id"] == "21"));
    }
    #[test]
    fn byte_measurement_and_cursor_rebase_are_bounded() {
        assert!(bounded_size(&json!({"large":"abcdef"}), 4).is_err());
        assert!(bounded_size(&json!({"small":1}), 64).is_ok());
        let mut a = fixture();
        let frame = book("11");
        a.invalidate("11", "test", frame.received_at, 1).unwrap();
        let mut events = a
            .apply_isolated(frame.raw_payload, frame.received_at, 0)
            .unwrap();
        rebase(&mut events, &mut BTreeMap::new(), 99).unwrap();
        assert_eq!(events[0]["cursor"], 100);
        assert_eq!(events[1]["cursor"], 101);
        assert_eq!(events[1]["canonical_payload"]["snapshot_cursor"], 100);
        assert_eq!(
            events[1]["canonical_payload_sha256"],
            canonical_hash(&events[1]["canonical_payload"])
        );
    }
}
