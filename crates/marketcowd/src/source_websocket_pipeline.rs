//! Market FIFO CPU actors and a single cursor allocator. Queue exhaustion is
//! an explicit market invalidation barrier; results from before it cannot land.
use super::*;
use crate::source_dispatch::{Completion, Dispatcher};
use crate::source_publication::SourceBatchTiming;
use marketcow_polymarket::RawTransportFrame;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

const INPUT_BYTES: usize = 64 * 1024 * 1024;
const FRAME_BYTES: usize = 8 * 1024 * 1024;
const OUTPUT_BYTES: usize = 16 * 1024 * 1024;
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
const ACTOR_CPU_BATCH: usize = 64;
const INPUT_FRAME_BATCH: usize = 64;

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
}
struct State {
    adapter: Adapter,
    generation: u64,
    control: Arc<Mutex<Control>>,
}
struct Input {
    generation: u64,
    frames: Vec<RawTransportFrame>,
    admitted_at: chrono::DateTime<Utc>,
    admitted_mono: std::time::Instant,
    recovery: Vec<(String, String)>,
    rest_recovery: Option<Vec<Value>>,
    replacement_reason: Option<String>,
    _bytes: OwnedSemaphorePermit,
}
pub(super) struct Output {
    generation: u64,
    events: Vec<Value>,
    materialized_books: BTreeMap<u64, Value>,
    confirmations: Vec<ReducedBookConfirmation>,
    trace: BatchTrace,
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
    bytes: Arc<Semaphore>,
    admission: Mutex<(std::time::Instant, BTreeMap<&'static str, u64>)>,
    timing: Arc<PipelineTiming>,
    trace_budget: Mutex<(std::time::Instant, u32, u32)>,
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
        let added = router.admit_market(market)?;
        if !added {
            ensure!(
                self.controls.contains_key(&market.market_id),
                "admitted actor missing"
            );
            return Ok(false);
        }
        if let Err(error) = self.add_market(router, &market.market_id) {
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
    pub(super) fn add_market(&mut self, router: &Adapter, market: &str) -> Result<()> {
        ensure!(
            !self.controls.contains_key(market),
            "market actor already installed"
        );
        let adapter = router.market_actor(market);
        ensure!(
            !adapter.identities.is_empty(),
            "market has no admitted tokens"
        );
        let control = Arc::new(Mutex::new(Control::default()));
        self.dispatch.add_entity(
            market.into(),
            State {
                adapter,
                generation: 0,
                control: control.clone(),
            },
        )?;
        self.controls.insert(market.into(), control);
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

    pub(super) fn start(router: &Adapter) -> Result<(Self, mpsc::Receiver<Completion<Output>>)> {
        Self::start_with_workers(router, 4)
    }

    pub(super) fn start_with_workers(
        router: &Adapter,
        workers: usize,
    ) -> Result<(Self, mpsc::Receiver<Completion<Output>>)> {
        ensure!((1..=8).contains(&workers), "market workers must be 1..=8");
        let markets: BTreeSet<_> = router.identities.values().map(|(m, _)| m.clone()).collect();
        let mut controls = BTreeMap::new();
        let states = markets
            .into_iter()
            .map(|market| {
                let control = Arc::new(Mutex::new(Control::default()));
                controls.insert(market.clone(), control.clone());
                (
                    market.clone(),
                    State {
                        adapter: router.market_actor(&market),
                        generation: 0,
                        control,
                    },
                )
            })
            .collect();
        // At most workers CPU jobs, a finite burst window per market, and a
        // bounded scheduling batch per worker (including in-flight results).
        // Input and output payloads retain independent shared byte ceilings.
        // Two liquid markets may each reserve one complete actor batch. Keep
        // that burst bounded while avoiding an artificial one-market global
        // barrier in the shared completion queue.
        let output_capacity = (workers * 32).min(128);
        let timing = Arc::new(PipelineTiming::default());
        let actor_timing = timing.clone();
        let (dispatch, output) = Dispatcher::start_filtered_batched(
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
                        events: vec![],
                        materialized_books: BTreeMap::new(),
                        confirmations: vec![],
                        trace,
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
                        events: vec![],
                        materialized_books: BTreeMap::new(),
                        confirmations: vec![],
                        trace,
                    });
                }
                let events = if let Some(raw_books) = input.rest_recovery {
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
                    for frame in input.frames {
                        events.extend(state.adapter.apply_isolated(
                            frame.raw_payload,
                            frame.received_at,
                            events.len() as u64,
                        )?);
                    }
                    events
                };
                let confirmations = std::mem::take(&mut state.adapter.confirmations);
                let (events, materialized_books) = compact_incremental_events(events)?;
                bounded_size(&events, OUTPUT_BYTES)?;
                trace.actor_completed_at = Utc::now();
                trace.actor_completed_mono = std::time::Instant::now();
                Ok(Output {
                    generation: input.generation,
                    events,
                    materialized_books,
                    confirmations,
                    trace,
                })
            },
        )?;
        Ok((
            Self {
                dispatch,
                controls,
                bytes: Arc::new(Semaphore::new(INPUT_BYTES)),
                admission: Mutex::new((std::time::Instant::now(), BTreeMap::new())),
                timing,
                trace_budget: Mutex::new((std::time::Instant::now(), 0, 0)),
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
            "queue_snapshot_excludes_actor_held_input":true,"output_completions_queued":output_queued});
        stats.0 = std::time::Instant::now();
        stats.1.clear();
        drop(stats);
        eprintln!("{row}");
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

    pub(super) async fn submit(
        &self,
        router: &mut Adapter,
        publication: &mut Publication,
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
        publication: &mut Publication,
        frames: Vec<RawTransportFrame>,
    ) -> Result<()> {
        let mut grouped = BTreeMap::<String, Vec<RawTransportFrame>>::new();
        for frame in frames {
            if frame.raw_payload["event_type"] == "new_market" {
                continue;
            }
            let market = Self::route(router, &frame)?.to_owned();
            grouped.entry(market).or_default().push(frame);
        }
        for (market, frames) in grouped {
            for chunk in frames.chunks(INPUT_FRAME_BATCH) {
                self.submit_market_frames(
                    router,
                    publication,
                    market.clone(),
                    chunk.to_vec(),
                    None,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn submit_market_frames(
        &self,
        router: &mut Adapter,
        publication: &mut Publication,
        market: String,
        mut frames: Vec<RawTransportFrame>,
        recovery: Option<(String, String)>,
    ) -> Result<()> {
        ensure!(!frames.is_empty(), "empty market input batch");
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
            if let Ok(permit) = self.bytes.clone().try_acquire_many_owned(bytes as u32) {
                let input = Input {
                    generation,
                    frames: std::mem::take(&mut frames),
                    admitted_at: Utc::now(),
                    admitted_mono: std::time::Instant::now(),
                    recovery: recovery.into_iter().collect(),
                    rest_recovery: None,
                    replacement_reason: None,
                    _bytes: permit,
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
            } else {
                rejection = "shared_input_bytes_full";
            }
        } else {
            rejection = "frame_size_or_encoding_rejected";
        }
        self.note(rejection);
        if rejection == "queue_closed_or_unknown" {
            return self
                .invalidate_market(router, publication, &market, "market_actor_closed")
                .await;
        }
        self.invalidate_market(router, publication, &market, "market_queue_capacity")
            .await
    }

    pub(super) async fn submit_rest_recovery(
        &self,
        router: &mut Adapter,
        publication: &mut Publication,
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
        let generation = self.controls[&market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?
            .generation;
        let bytes = match bounded_size(&raw_books, FRAME_BYTES) {
            Ok(bytes) => bytes,
            Err(_) => {
                self.note("rest_frame_size_or_encoding_rejected");
                self.invalidate_market(router, publication, &market, "market_queue_capacity")
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
        let permit = match self.bytes.clone().try_acquire_many_owned(bytes as u32) {
            Ok(permit) => permit,
            Err(_) => {
                self.note("rest_shared_input_bytes_full");
                self.invalidate_market(router, publication, &market, "market_queue_capacity")
                    .await?;
                return Ok(false);
            }
        };
        let input = Input {
            generation,
            frames: vec![frame],
            admitted_at: Utc::now(),
            admitted_mono: std::time::Instant::now(),
            recovery: attempts,
            rest_recovery: Some(raw_books),
            replacement_reason: None,
            _bytes: permit,
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
                self.invalidate_market(router, publication, &market, "market_queue_capacity")
                    .await?;
                Ok(false)
            }
        }
    }

    pub(super) async fn submit_rest_replacement(
        &self,
        router: &mut Adapter,
        publication: &mut Publication,
        raw_books: Vec<Value>,
        received_at: chrono::DateTime<Utc>,
        market: &str,
        reason: &str,
    ) -> Result<()> {
        let received_at = chrono::DateTime::from_timestamp_micros(received_at.timestamp_micros())
            .context("REST replacement receipt timestamp")?;
        let generation = self.controls[market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?
            .generation;
        let bytes = bounded_size(&raw_books, FRAME_BYTES)?;
        let frame = RawTransportFrame {
            raw_payload: raw_books
                .first()
                .context("REST replacement pair missing")?
                .clone(),
            received_at,
        };
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| anyhow::anyhow!("REST replacement byte capacity"))?;
        let input = Input {
            generation,
            frames: vec![frame],
            admitted_at: Utc::now(),
            admitted_mono: std::time::Instant::now(),
            recovery: Vec::new(),
            rest_recovery: Some(raw_books),
            replacement_reason: Some(reason.to_owned()),
            _bytes: permit,
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
                self.invalidate_market(router, publication, market, "market_queue_capacity")
                    .await
            }
        }
    }

    pub(super) async fn invalidate_market(
        &self,
        router: &mut Adapter,
        publication: &mut Publication,
        market: &str,
        reason: &str,
    ) -> Result<()> {
        {
            let control = self.controls[market]
                .lock()
                .map_err(|_| anyhow::anyhow!("market control poisoned"))?;
            if reason == "market_queue_capacity"
                && !control.attempts.is_empty()
                && !control.baseline_admitted
            {
                // No new baseline/work crossed this barrier. Preserve the
                // attempt identity; accepted recovery is NEVER coalesced here.
                self.note("capacity_recovery_coalesced");
                return Ok(());
            }
        }
        let tokens: Vec<_> = router
            .identities
            .iter()
            .filter(|(_, (m, _))| m == market)
            .map(|(t, _)| t.clone())
            .collect();
        self.note("new_recovery_barrier");
        let mut events = Vec::new();
        for token in &tokens {
            events.push(
                router.invalidate(
                    token,
                    reason,
                    Utc::now(),
                    publication
                        .cursor()?
                        .checked_add(events.len() as u64 + 1)
                        .context("cursor overflow")?,
                )?,
            );
        }
        publication.publish_with_capacity(events).await?;
        let mut control = self.controls[market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?;
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

    pub(super) fn retire_market(&self, router: &mut Adapter, market: &str) -> Result<()> {
        let mut control = self.controls[market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?;
        control.generation = control
            .generation
            .checked_add(1)
            .context("market generation overflow")?;
        control.attempts.clear();
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
        publication: &mut Publication,
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
        let mut output = match completion.result {
            Ok(output) => output,
            Err(error) => {
                eprintln!(
                    "{}",
                    json!({"stage":"market_worker_rejected","market_id":market,"detail":error.to_string()})
                );
                return self
                    .invalidate_market(router, publication, &market, "market_worker_rejected")
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
        if output.events.is_empty() && output.confirmations.is_empty() {
            return Ok(());
        }
        if !output.events.is_empty() {
            rebase(
                &mut output.events,
                &mut output.materialized_books,
                publication.cursor()?,
            )?;
        }
        // Keep only the scheduler's attempt map here; book reducers stay in
        // their actors. Publication performs the authoritative transition check.
        let transitions: Vec<_> = output
            .events
            .iter()
            .filter(|e| marketcow_runtime::discovery_source::is_token_recovery(e))
            .map(|e| {
                (
                    e["event_type"] == "recovery_started",
                    e["token_id"].as_str().unwrap().to_owned(),
                    e["canonical_payload"]["recovery_id"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                )
            })
            .collect();
        let slow_batch = (Utc::now() - output.trace.raw_batch_first_received_at).num_milliseconds() >= 500;
        // A shared deterministic cursor sample joins the actor and WS-send
        // stages even when only the downstream stage is slow. Keep its budget
        // separate from threshold-triggered diagnostics.
        let trace_events = output.events.iter().filter_map(|event| {
            let sampled = event["cursor"].as_u64().is_some_and(crate::source_publication::btc5m_trace_sample);
            (sampled || slow_batch).then(|| (sampled, json!({
                "publisher_cursor":event["cursor"],
                "event_id":event["event_id"],
                "raw_payload_sha256":event["raw_payload_sha256"],
                "token_id":event["token_id"],
                "book_epoch":event["book_epoch"],
                "sequence":event["sequence"],
                "canonical_book_received_at":event["canonical_payload"]["received_at"],
                "event_received_at":event["received_at"],
            })))
        }).collect::<Vec<_>>();
        if !output.events.is_empty() {
            let publish_call_started_mono = std::time::Instant::now();
            let publish_call_started_at = Utc::now();
            publication
                .publish_with_capacity_and_materialized_timed(
                    output.events,
                    output.materialized_books,
                    None,
                    Some(SourceBatchTiming {
                        raw_batch_first_received_at: output.trace.raw_batch_first_received_at,
                        raw_batch_last_received_at: output.trace.raw_batch_last_received_at,
                        actor_admitted_at: output.trace.admitted_at,
                        actor_started_at: output.trace.actor_started_at,
                        actor_completed_at: output.trace.actor_completed_at,
                        actor_admitted_mono: output.trace.admitted_mono,
                        actor_started_mono: output.trace.actor_started_mono,
                        actor_completed_mono: output.trace.actor_completed_mono,
                        publish_call_started_at,
                        publish_call_started_mono,
                        shared_replay_visible_at: None,
                        shared_replay_visible_mono: None,
                        durable_at: None,
                    }),
                )
                .await?;
        }
        let published_at = Utc::now();
        for (sampled, event) in trace_events {
            let mut budget = self.trace_budget.lock().expect("trace budget poisoned");
            if budget.0.elapsed() >= std::time::Duration::from_secs(60) {
                *budget = (std::time::Instant::now(), 0, 0);
            }
            if sampled {
                if budget.2 >= 32 { continue; }
                budget.2 += 1;
            } else {
                if budget.1 >= 60 { continue; }
                budget.1 += 1;
            }
            drop(budget);
            eprintln!("{}", json!({
                "stage":"market_event_timeline",
                "trace_reason":if sampled {"cursor_sample"} else {"slow_batch"},
                "market_id":market,
                "event":event,
                "raw_batch_first_received_at":output.trace.raw_batch_first_received_at,
                "raw_batch_last_received_at":output.trace.raw_batch_last_received_at,
                "actor_admitted_at":output.trace.admitted_at,
                "actor_started_at":output.trace.actor_started_at,
                "actor_completed_at":output.trace.actor_completed_at,
                "published_at":published_at,
                "durability":"published_not_necessarily_durable",
            }));
        }
        for confirmation in output.confirmations {
            publication.confirm_reduced_token(
                &confirmation.market_id,
                &confirmation.condition_id,
                &confirmation.token_id,
                &confirmation.state_checksum,
                confirmation.exchange_at,
                confirmation.received_at,
                &confirmation.source_hash,
            )?;
        }
        let mut control = self.controls[&market]
            .lock()
            .map_err(|_| anyhow::anyhow!("market control poisoned"))?;
        for (started, token, attempt) in transitions {
            if started {
                router.recoveries.insert(token.clone(), attempt.clone());
                control.attempts.insert(token, attempt);
            } else {
                router.recoveries.remove(&token);
                control.attempts.remove(&token);
            }
        }
        Ok(())
    }

    pub(super) async fn finish(
        mut self,
        router: &mut Adapter,
        publication: &mut Publication,
        output: &mut mpsc::Receiver<Completion<Output>>,
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
    async fn next(output: &mut mpsc::Receiver<Completion<Output>>) -> Completion<Output> {
        tokio::time::timeout(std::time::Duration::from_secs(2), output.recv())
            .await
            .unwrap()
            .unwrap()
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
        let (mut pipeline, mut output) = Pipeline::start_with_workers(&router, 6).unwrap();
        let retained = pipeline.controls["2"].clone();
        let old = pipeline.controls["1"].clone();
        let removed = pipeline.remove_market(&mut router, "1").unwrap();
        assert_eq!(removed, vec!["11", "12"]);
        assert!(!router.identities.contains_key("11"));
        assert!(router.ticks.contains_key("21"));
        assert!(Arc::ptr_eq(&retained, &pipeline.controls["2"]));
        assert!(pipeline.add_market(&router, "1").is_err());
        for token in removed {
            router
                .identities
                .insert(token.clone(), admitted.identities[&token].clone());
            router
                .ticks
                .insert(token.clone(), admitted.ticks[&token].clone());
        }
        pipeline.add_market(&router, "1").unwrap();
        assert!(!Arc::ptr_eq(&old, &pipeline.controls["1"]));
        assert!(pipeline.add_market(&router, "1").is_err());
        assert!(Arc::ptr_eq(&retained, &pipeline.controls["2"]));
        pipeline.dispatch.close();
        pipeline.dispatch.join().await;
        assert!(output.recv().await.is_none());
    }

    #[tokio::test]
    async fn installed_market_requires_admitted_metadata_and_starts_without_fabricated_book() {
        let mut router = fixture();
        let (mut pipeline, mut output) = Pipeline::start_with_workers(&router, 6).unwrap();
        let retained = pipeline.controls["1"].clone();
        let tokens = router
            .identities
            .iter()
            .map(|(token, (market, _))| (token.clone(), market.clone()))
            .collect();
        let mut publication=Publication::start(0,Some(json!({"latest_cursor":0,"catalog_revision":"catalog","markets":[],"books":[],"gaps":[]})),tokens,4,65536,16384,|_|Ok(())).unwrap();
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
            "markets":[],"books":[],"gaps":[]})),
            tokens,
            4,
            65536,
            16384,
            |_| Ok(()),
        )
        .unwrap();
        let (mut pipeline, mut output) = Pipeline::start_with_workers(&router, 1).unwrap();
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
        assert_eq!(publication.cursor().unwrap(), 0);
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
                "markets":[],"books":[],"gaps":[]})),
            tokens,
            8,
            1048576,
            65536,
            |_| Ok(()),
        )
        .unwrap();
        let (mut pipeline, mut output) = Pipeline::start_with_workers(&router, 1).unwrap();
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
        let cursor = publication.cursor().unwrap();
        for _ in 0..100 {
            if publication.persisted_cursor() == cursor {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(publication.persisted_cursor(), cursor);
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
        let view = publication.capture_view().unwrap();
        assert_eq!(view.cursor, cursor);
        assert_eq!(view.persisted_cursor, cursor);
        assert_eq!(view.confirmation_sequence, 1);
        assert_eq!(view.confirmation_versions.get("11"), Some(&1));
        assert!(!view.confirmation_versions.contains_key("12"));
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
        let base = json!({"latest_cursor":0,"books":[],"markets":[],"gaps":[],
            "schema_version":"marketcow.polymarket.live-stream.v1","type":"state"});
        let mut publication =
            Publication::start(0, Some(base), tokens, 32, 1048576, 65536, |_| Ok(())).unwrap();
        let (pipeline, mut output) = Pipeline::start_with_workers(&router, 8).unwrap();
        assert!(Pipeline::start_with_workers(&router, 0).is_err());
        assert!(Pipeline::start_with_workers(&router, 9).is_err());
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
        assert_eq!(publication.cursor().unwrap(), 2);
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
        assert_eq!(publication.cursor().unwrap(), 4);
        let current = router.recoveries["11"].clone();
        pipeline
            .publish(&mut router, &mut publication, stale)
            .await
            .unwrap();
        assert_eq!(publication.cursor().unwrap(), 4);
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
        assert_eq!(publication.cursor().unwrap(), 6);
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
        let base = json!({"latest_cursor":0,"books":[],"markets":[],"gaps":[],
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
        let (pipeline, mut output) = Pipeline::start(&router).unwrap();
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
        let events = &completion.result.as_ref().unwrap().events;
        assert_eq!(
            events
                .iter()
                .map(|event| event["event_type"].as_str().unwrap())
                .collect::<Vec<_>>(),
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
        assert_eq!(publication.cursor().unwrap(), 6);
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
    async fn exhausted_byte_budget_fences_old_output_and_other_market_continues() {
        let mut router = fixture();
        let tokens = router
            .identities
            .iter()
            .map(|(t, (m, _))| (t.clone(), m.clone()))
            .collect();
        let written = Arc::new(Mutex::new(Vec::<Value>::new()));
        let capture = written.clone();
        let base = json!({"latest_cursor":0,"books":[],"markets":[],"gaps":[],"schema_version":"marketcow.polymarket.live-stream.v1","type":"state"});
        let mut publication =
            Publication::start(0, Some(base), tokens, 32, 1048576, 65536, move |batches| {
                for batch in batches {
                    capture
                        .lock()
                        .unwrap()
                        .extend_from_slice(batch.validated.events());
                }
                Ok(())
            })
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = publication.router(131072, 1).unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{address}/"))
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
        let (pipeline, mut output) = Pipeline::start(&router).unwrap();
        pipeline
            .submit(&mut router, &mut publication, book("11"), None)
            .await
            .unwrap();
        let old = next(&mut output).await;
        // A completed CPU result is delayed in the coordinator while A's input
        // budget is exhausted. Its old generation must not resurrect A later.
        let all_bytes = pipeline
            .bytes
            .clone()
            .acquire_many_owned(INPUT_BYTES as u32)
            .await
            .unwrap();
        pipeline
            .submit(&mut router, &mut publication, book("11"), None)
            .await
            .unwrap();
        assert_eq!(publication.cursor().unwrap(), 2); // Explicit A gaps only.
        let first_attempts = router.recoveries.clone();
        // Rejected snapshots while no post-barrier input was admitted cannot
        // invalidate another version. Reuse the exact recovery and cursor.
        for _ in 0..20 {
            pipeline
                .submit(&mut router, &mut publication, book("11"), None)
                .await
                .unwrap();
        }
        assert_eq!(router.recoveries, first_attempts);
        assert_eq!(publication.cursor().unwrap(), 2);
        let attempts = router
            .recoveries
            .iter()
            .map(|(t, a)| (t.clone(), a.clone()))
            .collect();
        assert!(
            !pipeline
                .submit_rest_recovery(
                    &mut router,
                    &mut publication,
                    vec![book("11").raw_payload, book("12").raw_payload],
                    Utc::now(),
                    attempts
                )
                .await
                .unwrap()
        );
        assert_eq!(publication.cursor().unwrap(), 2);
        assert_eq!(router.recoveries, first_attempts);
        pipeline
            .publish(&mut router, &mut publication, old)
            .await
            .unwrap();
        assert_eq!(publication.cursor().unwrap(), 2);
        drop(all_bytes);
        pipeline
            .submit(&mut router, &mut publication, book("21"), None)
            .await
            .unwrap();
        pipeline
            .publish(&mut router, &mut publication, next(&mut output).await)
            .await
            .unwrap();
        assert_eq!(publication.cursor().unwrap(), 3);
        let attempt = router.recoveries["11"].clone();
        pipeline
            .submit(
                &mut router,
                &mut publication,
                book("11"),
                Some(("11".into(), "obsolete".into())),
            )
            .await
            .unwrap();
        pipeline
            .publish(&mut router, &mut publication, next(&mut output).await)
            .await
            .unwrap();
        assert_eq!(publication.cursor().unwrap(), 3);
        pipeline
            .submit(
                &mut router,
                &mut publication,
                book("11"),
                Some(("11".into(), attempt.clone())),
            )
            .await
            .unwrap();
        pipeline
            .publish(&mut router, &mut publication, next(&mut output).await)
            .await
            .unwrap();
        assert_eq!(publication.cursor().unwrap(), 5);
        assert!(!router.recoveries.contains_key("11"));
        assert!(router.recoveries.contains_key("12"));
        let received = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut received = Vec::new();
            while received.len() < 5 {
                let frame: Value =
                    serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap())
                        .unwrap();
                if frame["type"] == "events" {
                    received.extend(frame["events"].as_array().unwrap().iter().cloned());
                }
            }
            received
        })
        .await
        .unwrap();
        assert_eq!(
            received
                .iter()
                .map(|e| e["cursor"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(received[2]["token_id"], "21");
        assert_eq!(received[4]["canonical_payload"]["snapshot_cursor"], 4);
        server.abort();
        pipeline
            .finish(&mut router, &mut publication, &mut output)
            .await
            .unwrap();
        publication.finish().await.unwrap();
        let events = written.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .map(|e| e["cursor"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(events[2]["token_id"], "21");
        assert_eq!(events[4]["canonical_payload"]["snapshot_cursor"], 4);
        assert_eq!(events[4]["canonical_payload"]["recovery_id"], attempt);
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
