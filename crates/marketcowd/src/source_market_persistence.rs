//! Bounded market-local persistence admission and fair writer selection.
//! This queue is a resource boundary, NOT a durability acknowledgement. The
//! writer commits facts before advancing the selected market's durable prefix
//! or dropping a taken work item. Physical archive order is independent of
//! each market's published event cursor and group cursor.
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use super::source_market_replay::{MarketGroup, MarketReplay};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Condvar, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}},
};

/// Admission evidence travels with the error to the existing asynchronous
/// diagnostic site. Do not format/write logs while holding a market lane.
#[derive(Debug)]
pub struct MarketCapacityExhausted {
    pub limiter: &'static str,
    pub requested_bytes: usize,
    pub used_bytes: usize,
    pub limit_bytes: usize,
    pub used_batches: Option<usize>,
    pub limit_batches: Option<usize>,
}

impl MarketCapacityExhausted {
    pub fn new(limiter: &'static str, requested_bytes: usize, used_bytes: usize,
        limit_bytes: usize, used_batches: Option<usize>, limit_batches: Option<usize>) -> Self {
        Self { limiter, requested_bytes, used_bytes, limit_bytes, used_batches, limit_batches }
    }
}
impl std::fmt::Display for MarketCapacityExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("market persistence or replay capacity exhausted")
    }
}
impl std::error::Error for MarketCapacityExhausted {}

/// The physical writer is no longer accepting work. A live publisher may
/// continue only as an explicitly non-durable suffix; no queued item may be
/// silently treated as committed.
#[derive(Debug)]
pub struct MarketWriterUnavailable;
impl std::fmt::Display for MarketWriterUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("market persistence writer unavailable")
    }
}
impl std::error::Error for MarketWriterUnavailable {}

/// No writer outcome is implied by a deadline while waiting for an admitted
/// market's older groups. The retirement intent remains pending and may be
/// retried under the same ticket after the fair writer advances.
#[derive(Debug)]
pub struct MarketDrainTimeout;
impl std::fmt::Display for MarketDrainTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("market retirement durability drain expired")
    }
}
impl std::error::Error for MarketDrainTimeout {}

struct Wake {
    generation: Mutex<u64>,
    changed: Condvar,
}

impl Wake {
    fn notify(&self) {
        if let Ok(mut generation) = self.generation.lock() {
            *generation = generation.wrapping_add(1);
            self.changed.notify_all();
        }
    }
    fn notify_all(&self) {
        if let Ok(mut generation) = self.generation.lock() {
            *generation = generation.wrapping_add(1);
            self.changed.notify_all();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub batches: usize,
    pub bytes: usize,
}

struct Budget {
    limits: Limits,
    batches: AtomicUsize,
    bytes: AtomicUsize,
}

impl Budget {
    fn new(limits: Limits) -> Result<Self> {
        ensure!(limits.batches > 0 && limits.bytes > 0, "persistence queue limits");
        Ok(Self { limits, batches: AtomicUsize::new(0), bytes: AtomicUsize::new(0) })
    }

    fn reserve(self: &Arc<Self>, bytes: usize, scope: &'static str) -> Result<Reservation> {
        ensure!(bytes > 0, "empty persistence item");
        let rejected = |limiter| MarketCapacityExhausted::new(limiter, bytes,
            self.bytes.load(Ordering::Acquire), self.limits.bytes,
            Some(self.batches.load(Ordering::Acquire)), Some(self.limits.batches));
        if bytes > self.limits.bytes {
            return Err(rejected(if scope == "market" { "persistence_market_item_bytes" }
                else { "persistence_aggregate_item_bytes" }).into());
        }
        self.batches.fetch_update(Ordering::AcqRel, Ordering::Acquire,
            |used| used.checked_add(1).filter(|next| *next <= self.limits.batches))
            .map_err(|_| rejected(if scope == "market" { "persistence_market_batches" }
                else { "persistence_aggregate_batches" }))?;
        if self.bytes.fetch_update(Ordering::AcqRel, Ordering::Acquire,
            |used| used.checked_add(bytes).filter(|next| *next <= self.limits.bytes)).is_err() {
            self.batches.fetch_sub(1, Ordering::AcqRel);
            return Err(rejected(if scope == "market" { "persistence_market_bytes" }
                else { "persistence_aggregate_bytes" }).into());
        }
        Ok(Reservation { budget: self.clone(), bytes })
    }

    fn used(&self) -> Limits {
        Limits { batches: self.batches.load(Ordering::Acquire), bytes: self.bytes.load(Ordering::Acquire) }
    }
}

struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.budget.batches.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct Work<T> {
    pub market_id: String,
    pub stream_epoch: String,
    pub market_cursor: Option<u64>,
    pub payload: Arc<T>,
    ready_for_disk: Arc<AtomicBool>,
    // These remain charged while the writer is doing I/O, not merely while
    // the item is waiting in its market FIFO.
    _market: Reservation,
    _aggregate: Reservation,
    _writer: Option<WriterPermit>,
}

struct WriterPermit {
    active: Arc<AtomicBool>,
    wake: Arc<Wake>,
}
impl Drop for WriterPermit {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
        self.wake.notify();
    }
}

struct State<T> {
    pending: VecDeque<Work<T>>,
    draining: bool,
    retired: bool,
}

struct Lane<T> {
    epoch: String,
    budget: Arc<Budget>,
    state: Mutex<State<T>>,
    writer_active: Arc<AtomicBool>,
}

struct Registry<T> {
    lanes: BTreeMap<String, Arc<Lane<T>>>,
    scheduled: BTreeSet<String>,
    turn: VecDeque<String>,
}

pub struct MarketPersistence<T> {
    registry: Mutex<Registry<T>>,
    aggregate: Arc<Budget>,
    accepting: AtomicBool,
    wake: Arc<Wake>,
}

impl<T> MarketPersistence<T> {
    pub fn new(aggregate: Limits) -> Result<Self> {
        Ok(Self { registry: Mutex::new(Registry {
            lanes: BTreeMap::new(), scheduled: BTreeSet::new(), turn: VecDeque::new(),
        }),
            aggregate: Arc::new(Budget::new(aggregate)?), accepting: AtomicBool::new(true),
            wake: Arc::new(Wake { generation: Mutex::new(0), changed: Condvar::new() }) })
    }

    fn lane(&self, market: &str, epoch: &str) -> Result<Arc<Lane<T>>> {
        let lane = self.registry.lock().map_err(|_| anyhow::anyhow!("persistence registry poisoned"))?
            .lanes.get(market).cloned().context("persistence market not registered")?;
        ensure!(lane.epoch == epoch, "persistence market epoch changed");
        Ok(lane)
    }

    pub fn register(&self, market: &str, epoch: &str, limits: Limits) -> Result<()> {
        ensure!(self.accepting.load(Ordering::Acquire), "persistence queue closed");
        ensure!(!market.is_empty() && !epoch.is_empty(), "persistence market identity");
        ensure!(limits.batches <= self.aggregate.limits.batches && limits.bytes <= self.aggregate.limits.bytes,
            "market persistence limits exceed aggregate");
        let budget = Arc::new(Budget::new(limits)?);
        let mut registry = self.registry.lock().map_err(|_| anyhow::anyhow!("persistence registry poisoned"))?;
        ensure!(self.accepting.load(Ordering::Acquire), "persistence queue closed");
        if let Some(existing) = registry.lanes.get(market) {
            ensure!(existing.epoch == epoch && existing.budget.limits == limits,
                "persistence registration differs");
            return Ok(());
        }
        ensure!(registry.lanes.len() < 2048, "persistence market limit");
        registry.lanes.insert(market.to_owned(), Arc::new(Lane {
            epoch: epoch.to_owned(), budget,
            state: Mutex::new(State { pending: VecDeque::new(), draining: false, retired: false }),
            writer_active: Arc::new(AtomicBool::new(false)),
        }));
        Ok(())
    }

    /// Non-waiting admission to just the selected FIFO. An explicit rejection
    /// leaves its previous queue and both budgets unchanged.
    pub fn try_push(&self, market: &str, epoch: &str, payload: Arc<T>, bytes: usize) -> Result<()> {
        self.try_push_inner(market, epoch, None, payload, bytes,
            Arc::new(AtomicBool::new(true)))
    }

    fn try_push_inner(&self, market: &str, epoch: &str, cursor: Option<u64>,
        payload: Arc<T>, bytes: usize, ready_for_disk: Arc<AtomicBool>) -> Result<()> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(MarketWriterUnavailable.into());
        }
        let lane = self.lane(market, epoch)?;
        let mut state = lane.state.lock().map_err(|_| anyhow::anyhow!("persistence lane poisoned"))?;
        ensure!(!state.draining && !state.retired, "persistence market draining");
        if !self.accepting.load(Ordering::Acquire) {
            return Err(MarketWriterUnavailable.into());
        }
        let local = lane.budget.reserve(bytes, "market")?;
        let aggregate = self.aggregate.reserve(bytes, "aggregate")?;
        let mut registry = self.registry.lock()
            .map_err(|_| anyhow::anyhow!("persistence registry poisoned"))?;
        ensure!(registry.lanes.get(market).is_some_and(|current| Arc::ptr_eq(current, &lane)),
            "persistence lane changed during admission");
        state.pending.push_back(Work { market_id: market.to_owned(), stream_epoch: epoch.to_owned(),
            market_cursor: cursor,
            payload, ready_for_disk, _market: local, _aggregate: aggregate, _writer: None });
        // Only markets with pending work take writer turns. Historical cold
        // registrations must not add an O(catalog-size) scan per hot group.
        if registry.scheduled.insert(market.to_owned()) {
            registry.turn.push_back(market.to_owned());
        }
        drop(registry);
        drop(state);
        self.wake.notify();
        Ok(())
    }

    /// Selected-market hot publication and bounded writer admission share one
    /// market lane transaction. A rejected persistence reservation leaves that
    /// market's public cursor and projection untouched; it never waits for a
    /// different market's writer or a global publication mutex.
    pub fn publish_with_replay<S, C>(
        &self,
        replay: &MarketReplay<T, S>,
        market: &str,
        epoch: &str,
        expected_cursor: u64,
        events: &[Value],
        payload: Arc<T>,
        wire_bytes: usize,
        retained_bytes: usize,
        commit: C,
    ) -> Result<Arc<MarketGroup<T>>>
    where
        S: Default + Clone,
        C: FnOnce(&mut S),
    {
        self.publish_with_replay_prepared(replay, market, epoch, expected_cursor,
            events, payload, wire_bytes, retained_bytes, |_| Ok(()),
            |state, ()| commit(state))
    }

    /// Project one market before writer admission. Validation failure changes
    /// neither its replay cursor nor either persistence budget. The ordinary
    /// path admits the writer item before the atomic live commit; a selected
    /// production publisher may instead declare a one-way live-only segment
    /// when the bounded queue rejects capacity or the physical writer has
    /// failed. It must never treat that segment as durable or enqueue a later
    /// group behind the gap.
    pub fn publish_with_replay_prepared<S, P, C, U>(
        &self, replay: &MarketReplay<T, S>, market: &str, epoch: &str,
        expected_cursor: u64, events: &[Value], payload: Arc<T>,
        wire_bytes: usize, retained_bytes: usize, prepare: P, commit: C,
    ) -> Result<Arc<MarketGroup<T>>>
    where
        S: Default + Clone,
        P: FnOnce(&S) -> Result<U>,
        C: FnOnce(&mut S, U),
    {
        self.publish_with_replay_prepared_charged(replay, market, epoch,
            expected_cursor, events, payload, wire_bytes, retained_bytes,
            retained_bytes, None, prepare, commit)
    }

    pub fn publish_with_replay_prepared_charged<S, P, C, U>(
        &self, replay: &MarketReplay<T, S>, market: &str, epoch: &str,
        expected_cursor: u64, events: &[Value], payload: Arc<T>,
        wire_bytes: usize, retained_bytes: usize, durable_retained_bytes: usize,
        volatile: Option<&AtomicBool>,
        prepare: P, commit: C,
    ) -> Result<Arc<MarketGroup<T>>>
    where
        S: Default + Clone,
        P: FnOnce(&S) -> Result<U>,
        C: FnOnce(&mut S, U),
    {
        let ready = Arc::new(AtomicBool::new(false));
        let admission_ready = ready.clone();
        let group = replay.publish_with_state_prepare_charged(
            market, expected_cursor, events, payload, wire_bytes, retained_bytes,
            durable_retained_bytes,
            prepare,
            |group| {
                ensure!(group.stream_epoch == epoch, "persistence market epoch differs");
                if volatile.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                    return Ok(());
                }
                match self.try_push_inner(market, epoch, Some(group.market_cursor),
                    group.payload.clone(), retained_bytes, admission_ready) {
                    Err(error) if (error.downcast_ref::<MarketCapacityExhausted>().is_some()
                        || error.downcast_ref::<MarketWriterUnavailable>().is_some())
                        && volatile.is_some() => {
                            volatile.expect("checked above").store(true, Ordering::Release);
                            Ok(())
                        }
                    result => result,
                }
            },
            commit,
        )?;
        // Publication and the selected market's replay cursor are visible
        // before the disk worker can begin the physical append.
        ready.store(true, Ordering::Release);
        self.wake.notify();
        Ok(group)
    }

    /// One FIFO item per selected active-market turn. Directory lock is
    /// dropped BEFORE trying a lane. A busy/blocked A cannot block a ready B;
    /// a cold registered market is never scanned by the physical writer.
    pub fn try_pop(&self) -> Result<Option<Work<T>>> {
        let count = self.registry.lock().map_err(|_| anyhow::anyhow!("persistence registry poisoned"))?
            .turn.len();
        for _ in 0..count {
            let selected = {
                let mut registry = self.registry.lock().map_err(|_| anyhow::anyhow!("persistence registry poisoned"))?;
                let Some(market) = registry.turn.pop_front() else { return Ok(None); };
                registry.scheduled.remove(&market);
                let lane = registry.lanes.get(&market).cloned();
                (market, lane)
            };
            let (market, lane) = selected;
            if let Some(lane) = lane {
                let mut retry = false;
                let mut selected_work = None;
                match lane.state.try_lock() {
                    Ok(mut state) => {
                        if !state.retired && !lane.writer_active.load(Ordering::Acquire)
                            && state.pending.front().is_some_and(|work|
                                work.ready_for_disk.load(Ordering::Acquire)) {
                            if let Some(mut work) = state.pending.pop_front() {
                                lane.writer_active.store(true, Ordering::Release);
                                work._writer = Some(WriterPermit {
                                    active: lane.writer_active.clone(), wake: self.wake.clone(),
                                });
                                selected_work = Some(work);
                            }
                        }
                        retry = !state.retired && !state.pending.is_empty();
                    },
                    Err(std::sync::TryLockError::WouldBlock) => retry = true,
                    Err(std::sync::TryLockError::Poisoned(_)) => anyhow::bail!("persistence lane poisoned"),
                }
                if retry {
                    let mut registry = self.registry.lock()
                        .map_err(|_| anyhow::anyhow!("persistence registry poisoned"))?;
                    if registry.lanes.get(&market).is_some_and(|current| Arc::ptr_eq(current, &lane))
                        && registry.scheduled.insert(market.clone()) {
                        registry.turn.push_back(market);
                    }
                }
                if selected_work.is_some() { return Ok(selected_work); }
            }
        }
        Ok(None)
    }

    /// Sleep without a per-item notification queue. A generation check closes
    /// the race between scanning market lanes and waiting on the condition.
    /// Writer completion also wakes this selector when a lane becomes free.
    pub fn wait_pop(&self) -> Result<Option<Work<T>>> {
        let mut observed = self.wake.generation.lock()
            .map_err(|_| anyhow::anyhow!("persistence wake poisoned"))?;
        loop {
            let before = *observed;
            drop(observed);
            if let Some(work) = self.try_pop()? { return Ok(Some(work)); }
            if !self.accepting.load(Ordering::Acquire) && self.used().batches == 0 {
                return Ok(None);
            }
            observed = self.wake.generation.lock()
                .map_err(|_| anyhow::anyhow!("persistence wake poisoned"))?;
            if *observed == before {
                observed = self.wake.changed.wait(observed)
                    .map_err(|_| anyhow::anyhow!("persistence wake poisoned"))?;
            }
        }
    }

    /// The physical writer may choose markets fairly, but each selected group
    /// is durable only after its append returns. Never acknowledge a market's
    /// memory replay cursor on enqueue, dequeue, or an unknown write outcome.
    pub fn run_writer<S, F>(&self, replay: &MarketReplay<T, S>, mut persist: F) -> Result<()>
    where
        S: Default + Clone,
        F: FnMut(&Work<T>) -> Result<()>,
    {
        while let Some(work) = self.wait_pop()? {
            let cursor = work.market_cursor.context("writer item missing market cursor")?;
            if let Err(error) = persist(&work) {
                self.close();
                return Err(error).with_context(|| format!(
                    "market physical append failed market={} stream_epoch={} cursor={}",
                    work.market_id, work.stream_epoch, cursor,
                ));
            }
            if let Err(error) = replay.acknowledge_durable(
                &work.market_id, &work.stream_epoch, cursor,
            ) {
                self.close();
                return Err(error).with_context(|| format!(
                    "market durable acknowledgement failed market={} stream_epoch={} cursor={}",
                    work.market_id, work.stream_epoch, cursor,
                ));
            }
        }
        Ok(())
    }

    pub fn begin_drain(&self, market: &str, epoch: &str) -> Result<()> {
        let lane = self.lane(market, epoch)?;
        let mut state = lane.state.lock().map_err(|_| anyhow::anyhow!("persistence lane poisoned"))?;
        ensure!(!state.retired, "persistence market retired");
        state.draining = true;
        drop(state);
        self.wake.notify();
        Ok(())
    }

    pub fn finish_retire(&self, market: &str, epoch: &str) -> Result<()> {
        let lane = self.lane(market, epoch)?;
        let mut state = lane.state.lock().map_err(|_| anyhow::anyhow!("persistence lane poisoned"))?;
        ensure!(state.draining && !state.retired && state.pending.is_empty()
            && lane.budget.used().batches == 0 && !lane.writer_active.load(Ordering::Acquire),
            "persistence retirement has outstanding work");
        let mut registry = self.registry.lock().map_err(|_| anyhow::anyhow!("persistence registry poisoned"))?;
        ensure!(registry.lanes.get(market).is_some_and(|current| Arc::ptr_eq(current, &lane)),
            "persistence retirement incarnation changed");
        state.retired = true;
        registry.lanes.remove(market);
        registry.scheduled.remove(market);
        registry.turn.retain(|id| id != market);
        Ok(())
    }

    pub fn used(&self) -> Limits { self.aggregate.used() }

    pub fn market_used(&self, market: &str, epoch: &str) -> Result<Limits> {
        Ok(self.lane(market, epoch)?.budget.used())
    }

    pub fn is_registered(&self, market: &str) -> Result<bool> {
        Ok(self.registry.lock().map_err(|_| anyhow::anyhow!("persistence registry poisoned"))?
            .lanes.contains_key(market))
    }

    pub fn cancel_unstarted_drain(&self, market: &str, epoch: &str) -> Result<()> {
        let lane = self.lane(market, epoch)?;
        let mut state = lane.state.lock().map_err(|_| anyhow::anyhow!("persistence lane poisoned"))?;
        ensure!(!state.retired, "persistence market retired");
        state.draining = false;
        drop(state);
        self.wake.notify();
        Ok(())
    }

    /// Called by the disk retirement worker, not a live publisher. Draining
    /// was fenced before the retirement request entered the control queue.
    pub fn wait_drained(&self, market: &str, epoch: &str,
        timeout: std::time::Duration) -> Result<()> {
        ensure!(!timeout.is_zero(), "retirement drain deadline required");
        let lane = self.lane(market, epoch)?;
        let deadline = std::time::Instant::now() + timeout;
        let mut observed = self.wake.generation.lock()
            .map_err(|_| anyhow::anyhow!("persistence wake poisoned"))?;
        loop {
            let before = *observed;
            drop(observed);
            if lane.budget.used().batches == 0
                && !lane.writer_active.load(Ordering::Acquire) {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(MarketDrainTimeout.into());
            }
            observed = self.wake.generation.lock()
                .map_err(|_| anyhow::anyhow!("persistence wake poisoned"))?;
            if *observed == before {
                observed = self.wake.changed.wait_timeout(observed, remaining)
                    .map_err(|_| anyhow::anyhow!("persistence wake poisoned"))?.0;
            }
        }
    }

    pub fn close(&self) {
        self.accepting.store(false, Ordering::Release);
        self.wake.notify_all();
    }

    /// After an unknown physical-write outcome, no queued successor may be
    /// presented as durable. Release only the not-yet-selected work; the
    /// failed selected item is dropped by `run_writer` itself. Replay retains
    /// the published facts under its own independent bound and durable stays
    /// at the last acknowledged prefix.
    pub fn abandon_pending(&self) -> Result<()> {
        self.close();
        let lanes = self.registry.lock()
            .map_err(|_| anyhow::anyhow!("persistence registry poisoned"))?
            .lanes.values().cloned().collect::<Vec<_>>();
        for lane in lanes {
            lane.state.lock()
                .map_err(|_| anyhow::anyhow!("persistence lane poisoned"))?
                .pending.clear();
        }
        self.wake.notify_all();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketcow_polymarket::discovery_source::{SnapshotBoundary, snapshot_event};
    use marketcow_runtime::discovery_source::{PreparedSourceWriter, ValidatedSourceBatch,
        read_durable_market_page};
    use rusqlite::{Connection, params};
    use serde_json::json;

    fn fixture() -> MarketPersistence<usize> {
        let queue = MarketPersistence::new(Limits { batches: 4, bytes: 40 }).unwrap();
        for market in ["a", "b"] {
            queue.register(market, "epoch", Limits { batches: 2, bytes: 20 }).unwrap();
        }
        queue
    }

    #[test]
    fn rejected_market_writer_admission_cannot_advance_only_that_public_cursor() {
        let queue = fixture();
        let replay = MarketReplay::<usize, Vec<usize>>::new_with_state(8, 100, 200).unwrap();
        for market in ["a", "b"] { replay.register(market, "epoch").unwrap(); }
        let event = |market: &str, identity: &str| vec![
            json!({"market_id":market,"event_id":identity})
        ];
        for cursor in 1..=2 {
            queue.publish_with_replay(&replay, "a", "epoch", cursor,
                &event("a", &format!("a-{cursor}")), Arc::new(cursor as usize), 1, 10,
                |state| state.push(cursor as usize)).unwrap();
        }
        assert!(queue.publish_with_replay(&replay, "a", "epoch", 3,
            &event("a", "a-3"), Arc::new(3), 1, 10,
            |state| state.push(3)).is_err());
        replay.read_state("a", |state, watermark| {
            assert_eq!(watermark.published_cursor, 2);
            assert_eq!(state, &vec![1, 2]);
            Ok(())
        }).unwrap();
        queue.publish_with_replay(&replay, "b", "epoch", 1,
            &event("b", "b-1"), Arc::new(10), 1, 10,
            |state| state.push(10)).unwrap();
        replay.read_state("b", |state, watermark| {
            assert_eq!(watermark.published_cursor, 1);
            assert_eq!(state, &vec![10]);
            Ok(())
        }).unwrap();
        assert_eq!(queue.used(), Limits { batches: 3, bytes: 30 });
    }

    #[test]
    fn full_disk_lane_keeps_live_market_order_and_expires_only_bounded_replay() {
        let queue = fixture();
        let replay = MarketReplay::<usize, Vec<usize>>::new_partitioned_with_state(
            2, 20, 40, 2).unwrap();
        replay.register("a", "epoch").unwrap();
        replay.register("b", "epoch").unwrap();
        let volatile = AtomicBool::new(false);
        for cursor in 1..=5 {
            queue.publish_with_replay_prepared_charged(&replay, "a", "epoch", cursor,
                &[json!({"market_id":"a","event_id":format!("a-{cursor}")})],
                Arc::new(cursor as usize), 1, 10, 10, Some(&volatile),
                |_| Ok(cursor as usize), |state, value| state.push(value)).unwrap();
        }
        assert!(volatile.load(Ordering::Acquire));
        assert_eq!(queue.market_used("a", "epoch").unwrap(),
            Limits { batches: 2, bytes: 20 });
        replay.read_state("a", |state, watermark| {
            assert_eq!(state, &vec![1, 2, 3, 4, 5]);
            assert_eq!((watermark.published_cursor, watermark.durable_cursor,
                watermark.replay_floor), (5, 0, 3));
            Ok(())
        }).unwrap();
        assert!(replay.replay("a", "epoch", 2, 2, 20).is_err());
        assert_eq!(replay.replay("a", "epoch", 3, 2, 20).unwrap().next_cursor, 5);
        queue.close();
        queue.run_writer(&replay, |_| Ok(())).unwrap();
        assert_eq!(replay.watermark("a").unwrap().durable_cursor, 2);
        assert_eq!(replay.watermark("a").unwrap().published_cursor, 5);
        assert_eq!(queue.used(), Limits { batches: 0, bytes: 0 });
    }

    #[test]
    fn invalid_market_projection_never_enters_writer_or_advances_cursor() {
        let queue = fixture();
        let replay = MarketReplay::<usize, Vec<usize>>::new_with_state(8, 100, 200).unwrap();
        for market in ["a", "b"] { replay.register(market, "epoch").unwrap(); }
        let invalid = queue.publish_with_replay_prepared(&replay, "a", "epoch", 1,
            &[json!({"market_id":"a","event_id":"a-1"})], Arc::new(1), 1, 10,
            |_| anyhow::bail!("projection identity differs"),
            |state: &mut Vec<usize>, _: ()| state.push(1));
        assert!(invalid.unwrap_err().to_string().contains("projection identity differs"));
        assert_eq!(queue.used(), Limits { batches: 0, bytes: 0 });
        assert!(queue.try_pop().unwrap().is_none());
        replay.read_state("a", |_, _| Ok(())).unwrap_err();
        queue.publish_with_replay_prepared(&replay, "b", "epoch", 1,
            &[json!({"market_id":"b","event_id":"b-1"})], Arc::new(2), 1, 10,
            |_| Ok(2), |state, value| state.push(value)).unwrap();
        replay.read_state("b", |state, watermark| {
            assert_eq!(state, &vec![2]);
            assert_eq!((watermark.published_cursor, watermark.durable_cursor), (1, 0));
            Ok(())
        }).unwrap();
        assert_eq!(queue.used(), Limits { batches: 1, bytes: 10 });
    }

    #[test]
    fn slow_projection_preparation_in_a_does_not_hold_b_market_lane() {
        let queue = Arc::new(fixture());
        let replay = Arc::new(MarketReplay::<usize, Vec<usize>>::new_with_state(
            8, 100, 200).unwrap());
        for market in ["a", "b"] { replay.register(market, "epoch").unwrap(); }
        let (started, started_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let a_queue = queue.clone();
        let a_replay = replay.clone();
        let a = std::thread::spawn(move || a_queue.publish_with_replay_prepared(
            &a_replay, "a", "epoch", 1,
            &[json!({"market_id":"a","event_id":"a-1"})], Arc::new(1), 1, 10,
            |_| { started.send(()).unwrap(); release_rx.recv().unwrap(); Ok(1) },
            |state, value| state.push(value)).map(|_| ()));
        started_rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        let (finished, finished_rx) = std::sync::mpsc::channel();
        let b_queue = queue.clone();
        let b_replay = replay.clone();
        let b = std::thread::spawn(move || {
            let result = b_queue.publish_with_replay_prepared(
                &b_replay, "b", "epoch", 1,
                &[json!({"market_id":"b","event_id":"b-1"})], Arc::new(2), 1, 10,
                |_| Ok(2), |state, value| state.push(value)).map(|_| ());
            finished.send(result).unwrap();
        });
        let b_result = finished_rx.recv_timeout(std::time::Duration::from_secs(2));
        release.send(()).unwrap();
        a.join().unwrap().unwrap();
        b.join().unwrap();
        b_result.expect("B must publish before A preparation is released").unwrap();
        for market in ["a", "b"] {
            replay.read_state(market, |state, watermark| {
                assert_eq!(watermark.published_cursor, 1);
                assert_eq!(state.len(), 1);
                Ok(())
            }).unwrap();
        }
    }

    #[test]
    fn sleeping_fair_writer_wakes_on_admission_and_inflight_release() {
        let queue = Arc::new(fixture());
        let (sender, receiver) = std::sync::mpsc::channel();
        let waiting = queue.clone();
        let thread = std::thread::spawn(move || {
            let work = waiting.wait_pop().unwrap().unwrap();
            sender.send((work.market_id.clone(), *work.payload)).unwrap();
        });
        queue.try_push("b", "epoch", Arc::new(10), 10).unwrap();
        assert_eq!(receiver.recv_timeout(std::time::Duration::from_secs(2)).unwrap(),
            ("b".to_owned(), 10));
        thread.join().unwrap();
        queue.try_push("a", "epoch", Arc::new(1), 10).unwrap();
        queue.try_push("a", "epoch", Arc::new(2), 10).unwrap();
        let inflight = queue.try_pop().unwrap().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let waiting = queue.clone();
        let thread = std::thread::spawn(move || {
            let work = waiting.wait_pop().unwrap().unwrap();
            sender.send(*work.payload).unwrap();
        });
        drop(inflight);
        assert_eq!(receiver.recv_timeout(std::time::Duration::from_secs(2)).unwrap(), 2);
        thread.join().unwrap();
        queue.close();
        assert!(queue.wait_pop().unwrap().is_none());
    }

    #[test]
    fn fair_writer_acks_only_after_each_selected_market_commit() {
        let queue = fixture();
        let replay = MarketReplay::<usize>::new(8, 100, 200).unwrap();
        for market in ["a", "b"] { replay.register(market, "epoch").unwrap(); }
        for (market, cursor, payload) in [("a", 1, 11), ("a", 2, 12), ("b", 1, 21)] {
            queue.publish_with_replay(&replay, market, "epoch", cursor,
                &[json!({"market_id":market,"event_id":format!("{market}-{cursor}")})],
                Arc::new(payload), 1, 10, |_| {}).unwrap();
        }
        replay.read_state("a", |_, watermark| {
            assert_eq!((watermark.published_cursor, watermark.durable_cursor), (2, 0));
            Ok(())
        }).unwrap();
        queue.close();
        let mut written = Vec::new();
        queue.run_writer(&replay, |work| {
            written.push((work.market_id.clone(), work.market_cursor.unwrap(), *work.payload));
            Ok(())
        }).unwrap();
        assert_eq!(written, [("a".to_owned(), 1, 11), ("b".to_owned(), 1, 21),
            ("a".to_owned(), 2, 12)]);
        for (market, cursor) in [("a", 2), ("b", 1)] {
            replay.read_state(market, |_, watermark| {
                assert_eq!(watermark.durable_cursor, cursor);
                Ok(())
            }).unwrap();
        }
        assert_eq!(queue.used(), Limits { batches: 0, bytes: 0 });
    }

    #[test]
    fn failed_write_closes_admission_without_false_durable_ack() {
        let queue = fixture();
        let replay = MarketReplay::<usize>::new(8, 100, 200).unwrap();
        replay.register("a", "epoch").unwrap();
        queue.publish_with_replay(&replay, "a", "epoch", 1,
            &[json!({"market_id":"a","event_id":"a-1"})], Arc::new(1), 1, 10,
            |_| {}).unwrap();
        queue.close();
        assert!(queue.run_writer(&replay, |_| anyhow::bail!("disk failed")).is_err());
        replay.read_state("a", |_, watermark| {
            assert_eq!((watermark.published_cursor, watermark.durable_cursor), (1, 0));
            Ok(())
        }).unwrap();
        assert!(queue.try_push("a", "epoch", Arc::new(2), 10).is_err());
    }

    #[test]
    fn failed_writer_keeps_bounded_live_suffix_without_inventing_durability() {
        let queue = fixture();
        let replay = MarketReplay::<usize>::new(8, 100, 200).unwrap();
        replay.register("a", "epoch").unwrap();
        for cursor in 1..=2 {
            queue.publish_with_replay(&replay, "a", "epoch", cursor,
                &[json!({"market_id":"a","event_id":format!("a-{cursor}")})],
                Arc::new(cursor as usize), 1, 10, |_| {}).unwrap();
        }
        let error = queue.run_writer(&replay, |_| anyhow::bail!("injected disk failure"))
            .unwrap_err();
        assert!(format!("{error:#}").contains("market=a stream_epoch=epoch cursor=1"));
        queue.abandon_pending().unwrap();
        assert_eq!(queue.used(), Limits { batches: 0, bytes: 0 });
        let volatile = AtomicBool::new(false);
        queue.publish_with_replay_prepared_charged(&replay, "a", "epoch", 3,
            &[json!({"market_id":"a","event_id":"a-3"})], Arc::new(3),
            1, 10, 10, Some(&volatile), |_| Ok(()), |_, ()| {}).unwrap();
        assert!(volatile.load(Ordering::Acquire));
        replay.read_state("a", |_, watermark| {
            assert_eq!((watermark.published_cursor, watermark.durable_cursor), (3, 0));
            Ok(())
        }).unwrap();
    }

    #[test]
    fn retirement_drain_deadline_keeps_original_work_until_writer_completes() {
        let queue = fixture();
        let replay = MarketReplay::<usize>::new(8, 100, 200).unwrap();
        replay.register("a", "epoch").unwrap();
        queue.publish_with_replay(&replay, "a", "epoch", 1,
            &[json!({"market_id":"a","event_id":"a-1"})], Arc::new(1),
            1, 10, |_| {}).unwrap();
        queue.begin_drain("a", "epoch").unwrap();
        let error = queue.wait_drained("a", "epoch",
            std::time::Duration::from_millis(1)).unwrap_err();
        assert!(error.downcast_ref::<MarketDrainTimeout>().is_some());
        assert_eq!(queue.used(), Limits { batches: 1, bytes: 10 });
        queue.close();
        queue.run_writer(&replay, |_| Ok(())).unwrap();
        queue.wait_drained("a", "epoch", std::time::Duration::from_millis(1)).unwrap();
        replay.read_state("a", |_, watermark| {
            assert_eq!(watermark.durable_cursor, 1);
            Ok(())
        }).unwrap();
        queue.finish_retire("a", "epoch").unwrap();
        assert_eq!(queue.used(), Limits { batches: 0, bytes: 0 });
    }

    #[test]
    fn fair_market_writer_persists_interleaved_public_groups_without_rebasing() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("indexes")).unwrap();
        std::fs::File::create(root.path().join("events.jsonl")).unwrap();
        let db = Connection::open(root.path().join("indexes/latest-state.sqlite3")).unwrap();
        db.execute_batch("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE gaps(resolved INTEGER NOT NULL);
            CREATE TABLE books(token_id TEXT PRIMARY KEY,market_id TEXT,cursor INTEGER,payload_json BLOB,payload_sha256 TEXT);
            CREATE TABLE book_confirmations(token_id TEXT PRIMARY KEY);
            CREATE TABLE event_offsets(cursor INTEGER PRIMARY KEY,byte_offset INTEGER,byte_length INTEGER,event_id TEXT,market_id TEXT,token_id TEXT,line_sha256 TEXT);").unwrap();
        for (key, value) in [("schema_version", "marketcow.polymarket.state-index.v1"),
            ("latest_cursor", "0"), ("event_log_size", "0"), ("unresolved_gap_count", "0"),
            ("catalog_revision", "catalog-test"), ("active_recovery_id", "recovery-test")] {
            db.execute("INSERT INTO metadata VALUES (?,?)", params![key, value]).unwrap();
        }
        drop(db);
        let tokens: BTreeMap<String, String> = [
            ("11".into(), "a".into()), ("12".into(), "a".into()),
            ("21".into(), "b".into()), ("22".into(), "b".into()),
        ].into();
        let mut writer = PreparedSourceWriter::open(root.path(), 65536).unwrap();
        writer.bind_catalog_tokens(tokens.clone()).unwrap();
        let make = |market: &str, after: u64, market_cursor: u64| {
            let pair = if market == "a" { ["11", "12"] } else { ["21", "22"] };
            let events = pair.into_iter().enumerate().map(|(index, token)| {
                snapshot_event(&json!({"asset_id":token,"market":"condition",
                    "tick_size":"0.01","timestamp":"1700000000000","hash":"source-hash",
                    "bids":[{"price":"0.4","size":"10"}],
                    "asks":[{"price":"0.6","size":"10"}]}),
                    SnapshotBoundary { market_id: market, condition_id: "condition",
                        token_id: token, recovery_id: "recovery-test", cursor: after + index as u64 + 1,
                        received_at: chrono::DateTime::from_timestamp(1700000001, 0).unwrap(),
                    }).unwrap()
            }).collect();
            Arc::new(ValidatedSourceBatch::new_native(events, false, after, &tokens, 65536)
                .unwrap().with_native_market_group("epoch", market_cursor).unwrap())
        };
        let queue = MarketPersistence::new(Limits { batches: 4, bytes: 262144 }).unwrap();
        let replay = MarketReplay::<ValidatedSourceBatch>::new(4, 262144, 524288).unwrap();
        for market in ["a", "b"] {
            queue.register(market, "epoch", Limits { batches: 3, bytes: 196608 }).unwrap();
            replay.register(market, "epoch").unwrap();
        }
        let a1 = make("a", 0, 1);
        let a2 = make("a", 2, 2);
        let b1 = make("b", 0, 1);
        for (market, cursor, batch) in [("a", 1, a1.clone()), ("a", 2, a2.clone()),
            ("b", 1, b1.clone())] {
            queue.publish_with_replay(&replay, market, "epoch", cursor, batch.events(),
                batch.clone(), batch.encoded_bytes(), batch.retained_bytes(), |_| {}).unwrap();
        }
        queue.close();
        let mut order = Vec::new();
        queue.run_writer(&replay, |work| {
            order.push((work.market_id.clone(), work.market_cursor.unwrap()));
            writer.append_validated_group_with_books(&[(work.payload.as_ref(), &BTreeMap::new())])
        }).unwrap();
        assert_eq!(order, [("a".to_owned(), 1), ("b".to_owned(), 1), ("a".to_owned(), 2)]);
        assert_eq!(writer.cursor().unwrap(), 6);
        drop(writer);
        for (market, expected) in [("a", vec![a1, a2]), ("b", vec![b1])] {
            let page = read_durable_market_page(root.path(), market, "epoch", 0, 4, 65536).unwrap();
            assert_eq!(page.groups.len(), expected.len());
            for (group, batch) in page.groups.iter().zip(expected) {
                assert_eq!(group.events, batch.events());
            }
        }
    }

    #[test]
    fn fair_fifo_inflight_and_rejection_accounting() {
        let queue = fixture();
        queue.try_push("a", "epoch", Arc::new(1), 10).unwrap();
        queue.try_push("a", "epoch", Arc::new(2), 10).unwrap();
        assert!(queue.try_push("a", "epoch", Arc::new(3), 10).is_err());
        queue.try_push("b", "epoch", Arc::new(10), 10).unwrap();
        queue.try_push("b", "epoch", Arc::new(11), 10).unwrap();
        let first = queue.try_pop().unwrap().unwrap();
        let second = queue.try_pop().unwrap().unwrap();
        assert_eq!((&*first.payload, &*second.payload), (&1, &10));
        assert_eq!(queue.used(), Limits { batches: 4, bytes: 40 }, "I/O still owns both reservations");
        assert!(queue.try_push("a", "epoch", Arc::new(3), 1).is_err());
        assert!(queue.try_pop().unwrap().is_none(), "each market has one writer operation in flight");
        drop(first);
        queue.try_push("a", "epoch", Arc::new(3), 10).unwrap();
        assert_eq!(*queue.try_pop().unwrap().unwrap().payload, 2);
        drop(second);
        assert_eq!(*queue.try_pop().unwrap().unwrap().payload, 11);
        assert_eq!(*queue.try_pop().unwrap().unwrap().payload, 3);
        assert_eq!(queue.used(), Limits { batches: 0, bytes: 0 });
    }

    #[test]
    fn blocked_lane_does_not_hold_directory_or_writer_selection() {
        let queue = fixture();
        queue.try_push("a", "epoch", Arc::new(1), 10).unwrap();
        let a = queue.lane("a", "epoch").unwrap();
        let held = a.state.lock().unwrap();
        queue.try_push("b", "epoch", Arc::new(10), 10).unwrap();
        let work = queue.try_pop().unwrap().unwrap();
        assert_eq!(work.market_id, "b");
        drop(held);
        assert_eq!(queue.try_pop().unwrap().unwrap().market_id, "a");
    }

    #[test]
    fn drain_fences_reacquire_and_old_epoch_after_inflight_completion() {
        let queue = fixture();
        queue.try_push("a", "epoch", Arc::new(1), 10).unwrap();
        let work = queue.try_pop().unwrap().unwrap();
        queue.begin_drain("a", "epoch").unwrap();
        assert!(queue.try_push("a", "epoch", Arc::new(2), 10).is_err());
        assert!(queue.finish_retire("a", "epoch").is_err());
        assert!(queue.register("a", "new", Limits { batches: 2, bytes: 20 }).is_err());
        drop(work);
        queue.finish_retire("a", "epoch").unwrap();
        queue.register("a", "new", Limits { batches: 2, bytes: 20 }).unwrap();
        assert!(queue.try_push("a", "epoch", Arc::new(2), 10).is_err());
        assert!(queue.finish_retire("a", "epoch").is_err());
        queue.try_push("a", "new", Arc::new(3), 10).unwrap();
        assert_eq!(queue.try_pop().unwrap().unwrap().stream_epoch, "new");
    }

    #[test]
    fn failed_aggregate_reservation_cannot_refund_another_markets_inflight_work() {
        let queue = MarketPersistence::new(Limits { batches: 4, bytes: 10 }).unwrap();
        for market in ["a", "b"] {
            queue.register(market, "epoch", Limits { batches: 4, bytes: 10 }).unwrap();
        }
        queue.try_push("a", "epoch", Arc::new(1), 10).unwrap();
        let work = queue.try_pop().unwrap().unwrap();
        assert!(queue.try_push("b", "epoch", Arc::new(2), 1).is_err());
        assert_eq!(queue.market_used("b", "epoch").unwrap(), Limits { batches: 0, bytes: 0 });
        assert_eq!(queue.used(), Limits { batches: 1, bytes: 10 });
        drop(work);
        assert_eq!(queue.used(), Limits { batches: 0, bytes: 0 });
    }

    #[test]
    fn close_rejects_admission_but_keeps_accepted_work_for_drain() {
        let queue = fixture();
        queue.try_push("a", "epoch", Arc::new(1), 10).unwrap();
        queue.close();
        assert!(queue.try_push("b", "epoch", Arc::new(2), 1).is_err());
        assert!(queue.register("c", "epoch", Limits { batches: 1, bytes: 1 }).is_err());
        assert_eq!(*queue.try_pop().unwrap().unwrap().payload, 1);
        assert!(queue.try_pop().unwrap().is_none());
        assert_eq!(queue.used(), Limits { batches: 0, bytes: 0 });
    }

    #[test]
    fn four_markets_10000_items_preserve_fifo_and_exact_inflight_budget() {
        let queue = Arc::new(MarketPersistence::new(Limits { batches: 8, bytes: 80 }).unwrap());
        let markets = ["a", "b", "c", "d"];
        for market in markets {
            queue.register(market, "epoch", Limits { batches: 2, bytes: 20 }).unwrap();
        }
        let producers = markets.map(|market| {
            let queue = queue.clone();
            std::thread::spawn(move || {
                for sequence in 0..2500usize {
                    while queue.try_push(market, "epoch", Arc::new(sequence), 10).is_err() {
                        std::thread::yield_now();
                    }
                }
            })
        });
        let mut next = BTreeMap::<String, usize>::new();
        let started = std::time::Instant::now();
        let mut peak = Limits { batches: 0, bytes: 0 };
        for _ in 0..10000 {
            let work = loop {
                if let Some(work) = queue.try_pop().unwrap() { break work; }
                assert!(started.elapsed() < std::time::Duration::from_secs(10), "fair queue stalled");
                std::thread::yield_now();
            };
            let expected = next.entry(work.market_id.clone()).or_default();
            assert_eq!(*work.payload, *expected);
            *expected += 1;
            let used = queue.used();
            assert!(used.batches <= 8 && used.bytes <= 80);
            peak.batches = peak.batches.max(used.batches);
            peak.bytes = peak.bytes.max(used.bytes);
            drop(work); // Simulated successful writer completion, NOT a fsync proof.
        }
        for producer in producers { producer.join().unwrap(); }
        assert!(markets.iter().all(|market| next.get(*market) == Some(&2500)));
        assert_eq!(queue.used(), Limits { batches: 0, bytes: 0 });
        assert!(peak.batches > 0 && peak.bytes > 0);
    }
}
