//! Bounded per-entity FIFO actors. CPU work is concurrent across entities;
//! failures are results for that entity, never a global worker termination.
use anyhow::{Result, ensure};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{Semaphore, mpsc};

struct CompletionSet<O> {
    lanes: VecDeque<mpsc::Receiver<Completion<O>>>,
    accepting: bool,
    waiter: Option<std::task::Waker>,
}

/// Control-only selector over independent market completion FIFOs. Book-only
/// success stays in its market actor; no shared payload queue is introduced.
/// A direct market owner may take its receiver with take_market_output.
pub struct MarketCompletions<O> {
    shared: Arc<Mutex<CompletionSet<O>>>,
}

impl<O> MarketCompletions<O> {
    pub fn try_recv(&mut self) -> std::result::Result<Completion<O>, mpsc::error::TryRecvError> {
        let mut set = self.shared.lock().expect("market completion registry poisoned");
        let count = set.lanes.len();
        for _ in 0..count {
            let mut lane = set.lanes.pop_front().unwrap();
            match lane.try_recv() {
                Ok(value) => { set.lanes.push_back(lane); return Ok(value); }
                Err(mpsc::error::TryRecvError::Empty) => set.lanes.push_back(lane),
                Err(mpsc::error::TryRecvError::Disconnected) => {},
            }
        }
        Err(if !set.accepting && set.lanes.is_empty() {
            mpsc::error::TryRecvError::Disconnected
        } else { mpsc::error::TryRecvError::Empty })
    }

    pub async fn recv(&mut self) -> Option<Completion<O>> {
        std::future::poll_fn(|cx| {
            let mut set = self.shared.lock().expect("market completion registry poisoned");
            let count = set.lanes.len();
            for _ in 0..count {
                let mut lane = set.lanes.pop_front().unwrap();
                match lane.poll_recv(cx) {
                    std::task::Poll::Ready(Some(value)) => {
                        set.lanes.push_back(lane);
                        return std::task::Poll::Ready(Some(value));
                    },
                    std::task::Poll::Ready(None) => {},
                    std::task::Poll::Pending => set.lanes.push_back(lane),
                }
            }
            if !set.accepting && set.lanes.is_empty() {
                std::task::Poll::Ready(None)
            } else {
                set.waiter = Some(cx.waker().clone());
                std::task::Poll::Pending
            }
        }).await
    }
}

pub struct Completion<O> {
    pub entity: String,
    pub incarnation: u64,
    /// Time the complete actor batch became available to the owner.
    pub completed_at: std::time::Instant,
    pub result: Result<O>,
}

pub struct Dispatcher<I, S, O> {
    queues: BTreeMap<String, mpsc::Sender<I>>,
    jobs: tokio::task::JoinSet<()>,
    workers: Arc<Semaphore>,
    output: Option<mpsc::Sender<Completion<O>>>,
    partitioned_output_capacity: Option<usize>,
    market_outputs: BTreeMap<String, mpsc::Sender<Completion<O>>>,
    market_receivers: BTreeMap<String, mpsc::Receiver<Completion<O>>>,
    completion_set: Option<Arc<Mutex<CompletionSet<O>>>>,
    process: Arc<dyn Fn(&mut S, I) -> Result<O> + Send + Sync>,
    obsolete: Arc<dyn Fn(&S, &I) -> bool + Send + Sync>,
    per_entity_capacity: usize,
    actor_batch: usize,
    incarnations: BTreeMap<String, u64>,
    next_incarnation: u64,
    waits: Arc<DispatchWaits>,
    completion_filter: Arc<OnceLock<Arc<dyn Fn(&Result<O>) -> bool + Send + Sync>>>,
}

#[derive(Default)]
struct WaitSample {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

impl WaitSample {
    fn record(&self, elapsed: std::time::Duration) {
        let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(us, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
    }

    fn take(&self) -> serde_json::Value {
        serde_json::json!({
            "count":self.count.swap(0, Ordering::Relaxed),
            "total_us":self.total_us.swap(0, Ordering::Relaxed),
            "max_us":self.max_us.swap(0, Ordering::Relaxed),
        })
    }
}

#[derive(Default)]
struct DispatchWaits {
    output_reserve: WaitSample,
    cpu_permit: WaitSample,
}

impl<I: Send + 'static, S: Send + 'static, O: Send + 'static> Dispatcher<I, S, O> {
    pub fn take_wait_timing(&self) -> serde_json::Value {
        serde_json::json!({
            "output_reserve":self.waits.output_reserve.take(),
            "cpu_permit":self.waits.cpu_permit.take(),
        })
    }

    pub fn start(
        states: BTreeMap<String, S>,
        per_entity_capacity: usize,
        cpu_workers: usize,
        output_capacity: usize,
        process: impl Fn(&mut S, I) -> Result<O> + Send + Sync + 'static,
    ) -> Result<(Self, mpsc::Receiver<Completion<O>>)> {
        Self::start_filtered(
            states,
            per_entity_capacity,
            cpu_workers,
            output_capacity,
            |_, _| false,
            process,
        )
    }

    pub fn start_filtered(
        states: BTreeMap<String, S>,
        per_entity_capacity: usize,
        cpu_workers: usize,
        output_capacity: usize,
        obsolete: impl Fn(&S, &I) -> bool + Send + Sync + 'static,
        process: impl Fn(&mut S, I) -> Result<O> + Send + Sync + 'static,
    ) -> Result<(Self, mpsc::Receiver<Completion<O>>)> {
        Self::start_filtered_batched(
            states,
            per_entity_capacity,
            cpu_workers,
            output_capacity,
            1,
            obsolete,
            process,
        )
    }

    /// Like [`Self::start_filtered`], but drains a bounded number of already
    /// queued inputs for one entity into one blocking CPU dispatch. Each item
    /// still produces its own ordered completion and retains its own failure;
    /// batching only removes scheduler and semaphore churn during bursts.
    pub fn start_filtered_batched(
        states: BTreeMap<String, S>,
        per_entity_capacity: usize,
        cpu_workers: usize,
        output_capacity: usize,
        actor_batch: usize,
        obsolete: impl Fn(&S, &I) -> bool + Send + Sync + 'static,
        process: impl Fn(&mut S, I) -> Result<O> + Send + Sync + 'static,
    ) -> Result<(Self, mpsc::Receiver<Completion<O>>)> {
        ensure!(!states.is_empty() && states.len() <= 2048, "entity limit");
        ensure!(
            (1..=4096).contains(&per_entity_capacity),
            "entity queue limit"
        );
        ensure!((1..=32).contains(&cpu_workers), "CPU worker limit");
        ensure!((1..=256).contains(&output_capacity), "output queue limit");
        ensure!(
            (1..=output_capacity).contains(&actor_batch),
            "actor batch must fit output queue"
        );
        let process = Arc::new(process);
        let obsolete = Arc::new(obsolete);
        let workers = Arc::new(Semaphore::new(cpu_workers));
        let (output, receive) = mpsc::channel(output_capacity);
        let mut dispatcher = Self {
            queues: BTreeMap::new(),
            jobs: tokio::task::JoinSet::new(),
            workers,
            output: Some(output),
            partitioned_output_capacity: None,
            market_outputs: BTreeMap::new(),
            market_receivers: BTreeMap::new(),
            completion_set: None,
            process,
            obsolete,
            per_entity_capacity,
            actor_batch,
            incarnations: BTreeMap::new(),
            next_incarnation: 1,
            waits: Arc::new(DispatchWaits::default()),
            completion_filter: Arc::new(OnceLock::new()),
        };
        for (entity, state) in states {
            dispatcher.add_entity(entity, state)?;
        }
        Ok((dispatcher, receive))
    }

    /// Independent market-owner completion channels. A stalled owner can
    /// consume only its market's output slots. A filtered actor reserves those
    /// slots after CPU work only if a control receipt is needed. The caller
    /// must also enforce payload byte budgets; slot limits are not byte limits.
    pub fn start_partitioned(
        states: BTreeMap<String, S>,
        per_entity_capacity: usize,
        cpu_workers: usize,
        output_capacity_per_market: usize,
        actor_batch: usize,
        obsolete: impl Fn(&S, &I) -> bool + Send + Sync + 'static,
        process: impl Fn(&mut S, I) -> Result<O> + Send + Sync + 'static,
    ) -> Result<Self> {
        ensure!(!states.is_empty() && states.len() <= 2048, "entity limit");
        ensure!((1..=4096).contains(&per_entity_capacity), "entity queue limit");
        ensure!((1..=32).contains(&cpu_workers), "CPU worker limit");
        ensure!((1..=256).contains(&output_capacity_per_market), "market output queue limit");
        ensure!((1..=output_capacity_per_market).contains(&actor_batch),
            "actor batch must fit market output queue");
        let mut dispatcher = Self {
            queues: BTreeMap::new(), jobs: tokio::task::JoinSet::new(),
            workers: Arc::new(Semaphore::new(cpu_workers)), output: None,
            partitioned_output_capacity: Some(output_capacity_per_market),
            market_outputs: BTreeMap::new(), market_receivers: BTreeMap::new(),
            completion_set: None,
            process: Arc::new(process), obsolete: Arc::new(obsolete),
            per_entity_capacity, actor_batch, incarnations: BTreeMap::new(),
            next_incarnation: 1, waits: Arc::new(DispatchWaits::default()),
            completion_filter: Arc::new(OnceLock::new()),
        };
        for (entity, state) in states { dispatcher.add_entity(entity, state)?; }
        Ok(dispatcher)
    }

    /// Transfers this incarnation's channel to its market owner exactly once.
    /// Retirement/re-add creates a fresh channel; old completions remain fenced
    /// by `is_current` rather than entering the replacement owner's channel.
    pub fn take_market_output(&mut self, entity: &str) -> Option<mpsc::Receiver<Completion<O>>> {
        self.market_receivers.remove(entity)
    }

    /// Suppress successful, control-free completions after the market actor
    /// has already committed them. Install before submitting any input. Errors
    /// always reach the owner so it can invalidate only the affected market.
    pub fn set_completion_filter(
        &self,
        filter: impl Fn(&Result<O>) -> bool + Send + Sync + 'static,
    ) -> Result<()> {
        self.completion_filter.set(Arc::new(filter))
            .map_err(|_| anyhow::anyhow!("market completion filter already installed"))
    }

    /// Attach the existing coordinator without merging market payload buffers.
    /// Dynamic admission joins the selector; old incarnations drain separately.
    pub fn take_completion_set(&mut self) -> Result<MarketCompletions<O>> {
        ensure!(self.partitioned_output_capacity.is_some() && self.completion_set.is_none(),
            "market completion selector already taken or dispatcher not partitioned");
        ensure!(self.market_receivers.len() == self.queues.len(),
            "market receiver already assigned to a direct owner");
        let shared = Arc::new(Mutex::new(CompletionSet {
            lanes: std::mem::take(&mut self.market_receivers).into_values().collect(),
            accepting: true, waiter: None,
        }));
        self.completion_set = Some(shared.clone());
        Ok(MarketCompletions { shared })
    }

    /// Add an admitted actor without restarting retained actors or replacing
    /// the shared CPU/output budgets. Retiring tasks also count toward the cap.
    pub fn add_entity(&mut self, entity: String, mut state: S) -> Result<()> {
        while let Some(result) = self.jobs.try_join_next() {
            result?;
        }
        ensure!(
            !entity.is_empty() && !self.queues.contains_key(&entity),
            "duplicate/empty actor"
        );
        ensure!(self.jobs.len() < 2048, "active and retiring actor limit");
        let output = if let Some(capacity) = self.partitioned_output_capacity {
            if let Some(shared) = &self.completion_set {
                let mut set = shared.lock().expect("market completion registry poisoned");
                set.lanes.retain(|lane| !lane.is_closed() || !lane.is_empty());
                ensure!(set.lanes.len() < 2048, "active and draining completion lane limit");
            }
            let (send, receive) = mpsc::channel(capacity);
            self.market_outputs.insert(entity.clone(), send.clone());
            if let Some(shared) = &self.completion_set {
                let mut set = shared.lock().expect("market completion registry poisoned");
                set.lanes.push_back(receive);
                if let Some(waiter) = set.waiter.take() { waiter.wake(); }
            } else {
                self.market_receivers.insert(entity.clone(), receive);
            }
            send
        } else {
            self.output.as_ref().ok_or_else(|| anyhow::anyhow!("dispatcher closed"))?.clone()
        };
        let incarnation = self.next_incarnation;
        self.next_incarnation = incarnation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("actor identity exhausted"))?;
        self.incarnations.insert(entity.clone(), incarnation);
        let (send, mut input) = mpsc::channel(self.per_entity_capacity);
        self.queues.insert(entity.clone(), send);
        let process = self.process.clone();
        let obsolete = self.obsolete.clone();
        let workers = self.workers.clone();
        let waits = self.waits.clone();
        let completion_filter = self.completion_filter.clone();
        let actor_batch = self.actor_batch;
        self.jobs.spawn(async move {
            while let Some(first) = input.recv().await {
                let mut items = Vec::with_capacity(actor_batch);
                items.push(first);
                while items.len() < actor_batch {
                    match input.try_recv() {
                        Ok(item) => items.push(item),
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }
                // Only caller-proven fenced work can bypass execution.
                items.retain(|item| !obsolete(&state, item));
                if items.is_empty() {
                    continue;
                }
                // The unfiltered dispatcher preserves its original
                // reserve-before-work contract. Filtered market actors may
                // commit a book without producing a control receipt, so they
                // reserve only after knowing whether one is required.
                let pre_reserved = if completion_filter.get().is_none() {
                    let reserve_started = std::time::Instant::now();
                    let Ok(slots) = output.reserve_many(items.len()).await else {
                        break;
                    };
                    waits.output_reserve.record(reserve_started.elapsed());
                    Some(slots)
                } else {
                    None
                };
                items.retain(|item| !obsolete(&state, item));
                if items.is_empty() {
                    continue;
                }
                let permit_started = std::time::Instant::now();
                let Ok(permit) = workers.clone().acquire_owned().await else {
                    break;
                };
                waits.cpu_permit.record(permit_started.elapsed());
                let process = process.clone();
                let processed = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    let results = items
                        .into_iter()
                        .map(|item| process(&mut state, item))
                        .collect::<Vec<_>>();
                    (state, results)
                })
                .await;
                match processed {
                    Ok((next, results)) => {
                        state = next;
                        let results: Vec<_> = results.into_iter().filter(|result|
                            result.is_err() || completion_filter.get()
                                .is_none_or(|filter| filter(result))).collect();
                        if results.is_empty() { continue; }
                        let slots = if let Some(slots) = pre_reserved {
                            slots
                        } else {
                            let reserve_started = std::time::Instant::now();
                            let Ok(slots) = output.reserve_many(results.len()).await else {
                                break;
                            };
                            waits.output_reserve.record(reserve_started.elapsed());
                            slots
                        };
                        for (slot, result) in slots.zip(results) {
                            slot.send(Completion {
                                entity: entity.clone(),
                                incarnation,
                                completed_at: std::time::Instant::now(),
                                result,
                            });
                        }
                    }
                    Err(error) => {
                        let slot = if let Some(mut slots) = pre_reserved {
                            slots.next()
                        } else {
                            let reserve_started = std::time::Instant::now();
                            let slot = output.reserve().await.ok();
                            waits.output_reserve.record(reserve_started.elapsed());
                            slot
                        };
                        if let Some(slot) = slot {
                            slot.send(Completion {
                                entity: entity.clone(),
                                incarnation,
                                completed_at: std::time::Instant::now(),
                                result: Err(error.into()),
                            });
                        }
                        break; // Only this actor lost its state; others remain alive.
                    }
                }
            }
        });
        Ok(())
    }

    /// Caller must fence old completions BEFORE retirement. Closing admission
    /// drains bounded queued work; it never kills shared workers or other actors.
    pub fn remove_entity(&mut self, entity: &str) -> bool {
        self.incarnations.remove(entity);
        self.market_outputs.remove(entity);
        self.market_receivers.remove(entity);
        self.queues.remove(entity).is_some()
    }

    pub fn is_current(&self, completion: &Completion<O>) -> bool {
        self.incarnations.get(&completion.entity) == Some(&completion.incarnation)
    }

    /// Full/unknown queues return ownership to the caller for explicit entity
    /// invalidation/recovery. Never wait for A's capacity before submitting B.
    pub fn try_submit(&self, entity: &str, item: I) -> std::result::Result<(), I> {
        match self.queues.get(entity) {
            Some(queue) => queue.try_send(item).map_err(|error| error.into_inner()),
            None => Err(item),
        }
    }

    pub fn queue_closed(&self, entity: &str) -> bool {
        self.queues.get(entity).is_none_or(|q| q.is_closed())
    }

    /// Diagnostic snapshot; waiting actors holding one input are not counted
    /// as queued. CPU permits bound admitted blocking work, not CPU utilization.
    pub fn occupancy(&self) -> (usize, usize, usize, usize) {
        let queued = self
            .queues
            .values()
            .map(|q| q.max_capacity() - q.capacity());
        let output_used = self
            .output
            .as_ref()
            .map_or(0, |q| q.max_capacity() - q.capacity())
            + self.market_outputs.values().map(|q| q.max_capacity() - q.capacity()).sum::<usize>();
        (
            queued.clone().sum(),
            queued.max().unwrap_or(0),
            self.workers.available_permits(),
            output_used,
        )
    }

    /// Bounded diagnostic only: identify which entity owns a saturated FIFO
    /// without changing admission, ordering, or the shared output budget.
    pub fn busiest_queues(&self, limit: usize) -> Vec<(String, usize)> {
        let mut queues = self
            .queues
            .iter()
            .filter_map(|(entity, queue)| {
                let used = queue.max_capacity() - queue.capacity();
                (used > 0).then(|| (entity.clone(), used))
            })
            .collect::<Vec<_>>();
        queues.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        queues.truncate(limit);
        queues
    }

    pub fn close(&mut self) {
        self.queues.clear();
        self.output.take();
        self.partitioned_output_capacity.take();
        self.market_outputs.clear();
        if let Some(shared) = &self.completion_set {
            let mut set = shared.lock().expect("market completion registry poisoned");
            set.accepting = false;
            if let Some(waiter) = set.waiter.take() { waiter.wake(); }
        }
    }

    pub async fn join(&mut self) {
        while self.jobs.join_next().await.is_some() {}
    }

    pub async fn finish(mut self) {
        self.close();
        self.join().await;
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn filtered_book_only_completions_do_not_fill_control_slots() {
        let processed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = processed.clone();
        let mut dispatch = Dispatcher::start_partitioned(
            [("a".into(), ()), ("b".into(), ())].into(),
            64, 1, 1, 1, |_, _| false,
            move |_, item: usize| {
                observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(item)
            },
        ).unwrap();
        dispatch.set_completion_filter(|result: &anyhow::Result<usize>|
            result.as_ref().is_ok_and(|value| *value >= 100)).unwrap();
        let mut a = dispatch.take_market_output("a").unwrap();
        let mut b = dispatch.take_market_output("b").unwrap();
        for item in 0..32 { dispatch.try_submit("a", item).unwrap(); }
        dispatch.try_submit("b", 100).unwrap();
        let done = tokio::time::timeout(std::time::Duration::from_secs(2), b.recv())
            .await.unwrap().unwrap();
        assert_eq!(done.result.unwrap(), 100);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while processed.load(std::sync::atomic::Ordering::Relaxed) != 33 {
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert!(a.try_recv().is_err(), "book-only results need no control receipt");
        dispatch.close();
        dispatch.join().await;
    }

    #[tokio::test]
    async fn partitioned_selector_is_fair_and_survives_retire_readd_and_close() {
        let mut dispatch = Dispatcher::start_partitioned(
            [("a".into(), ()), ("b".into(), ())].into(),
            8, 2, 4, 1, |_, _| false, |_, item: usize| Ok(item),
        ).unwrap();
        let mut output = dispatch.take_completion_set().unwrap();
        assert!(dispatch.take_completion_set().is_err());
        for item in 0..4 { dispatch.try_submit("a", item).unwrap(); }
        dispatch.try_submit("b", 10).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if dispatch.occupancy().3 == 5 { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let first = output.recv().await.unwrap();
        let second = output.recv().await.unwrap();
        assert_ne!(first.entity, second.entity, "busy A cannot starve ready B");
        let mut old_a = Vec::new();
        if first.entity == "a" { old_a.push(first); } else { old_a.push(second); }
        assert!(dispatch.remove_entity("a"));
        dispatch.add_entity("a".into(), ()).unwrap();
        dispatch.try_submit("a", 20).unwrap();
        let mut replacement = None;
        for _ in 0..4 {
            let value = tokio::time::timeout(std::time::Duration::from_secs(2), output.recv())
                .await.unwrap().unwrap();
            if dispatch.is_current(&value) { replacement = Some(value); }
            else { old_a.push(value); }
        }
        assert_eq!(old_a.len(), 4);
        assert!(old_a.iter().all(|old| !dispatch.is_current(old)));
        assert_eq!(replacement.unwrap().result.unwrap(), 20);
        dispatch.close();
        assert!(tokio::time::timeout(std::time::Duration::from_secs(2), output.recv())
            .await.unwrap().is_none());
        dispatch.join().await;
    }

    #[tokio::test]
    async fn empty_selector_wakes_for_dynamic_admission() {
        let mut dispatch = Dispatcher::start_partitioned(
            [("a".into(), ())].into(), 2, 1, 1, 1,
            |_, _| false, |_, item: usize| Ok(item),
        ).unwrap();
        let mut output = dispatch.take_completion_set().unwrap();
        assert!(dispatch.remove_entity("a"));
        // Register the waiter before admission, including when all old lanes close.
        assert!(tokio::time::timeout(std::time::Duration::from_millis(10), output.recv())
            .await.is_err());
        dispatch.add_entity("b".into(), ()).unwrap();
        dispatch.try_submit("b", 42).unwrap();
        assert_eq!(tokio::time::timeout(std::time::Duration::from_secs(2), output.recv())
            .await.unwrap().unwrap().result.unwrap(), 42);
        dispatch.close();
        assert!(output.recv().await.is_none());
        dispatch.join().await;
    }

    #[tokio::test]
    async fn stalled_market_owner_does_not_hold_other_market_or_cpu_permit() {
        let mut dispatch = Dispatcher::start_partitioned(
            [("a".into(), ()), ("b".into(), ())].into(),
            8, 1, 1, 1, |_, _| false, |_, item: usize| Ok(item),
        ).unwrap();
        let mut a = dispatch.take_market_output("a").unwrap();
        let mut b = dispatch.take_market_output("b").unwrap();
        assert!(dispatch.take_market_output("a").is_none());
        dispatch.try_submit("a", 1).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while a.len() != 1 { tokio::task::yield_now().await; }
        }).await.unwrap();
        // A cannot reserve another completion slot. B must still run using
        // the single CPU permit without waiting for A's owner to consume.
        dispatch.try_submit("a", 2).unwrap();
        for expected in 10..14 {
            dispatch.try_submit("b", expected).unwrap();
            let completion = tokio::time::timeout(std::time::Duration::from_secs(2), b.recv())
                .await.unwrap().unwrap();
            assert!(dispatch.is_current(&completion));
            assert_eq!(completion.result.unwrap(), expected);
            assert_eq!(a.len(), 1);
        }
        dispatch.close();
        assert_eq!(a.recv().await.unwrap().result.unwrap(), 1);
        assert_eq!(a.recv().await.unwrap().result.unwrap(), 2);
        dispatch.join().await;
        assert!(a.recv().await.is_none());
        assert!(b.recv().await.is_none());
    }

    #[tokio::test]
    async fn partitioned_readded_market_has_new_channel_and_incarnation() {
        let mut dispatch = Dispatcher::start_partitioned(
            [("a".into(), ())].into(), 2, 1, 1, 1,
            |_, _| false, |_, item: usize| Ok(item),
        ).unwrap();
        let mut old = dispatch.take_market_output("a").unwrap();
        dispatch.try_submit("a", 1).unwrap();
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), old.recv())
            .await.unwrap().unwrap();
        assert!(dispatch.remove_entity("a"));
        dispatch.add_entity("a".into(), ()).unwrap();
        assert!(!dispatch.is_current(&first));
        let mut new = dispatch.take_market_output("a").unwrap();
        dispatch.try_submit("a", 2).unwrap();
        let second = tokio::time::timeout(std::time::Duration::from_secs(2), new.recv())
            .await.unwrap().unwrap();
        assert!(dispatch.is_current(&second));
        assert_ne!(first.incarnation, second.incarnation);
        assert_eq!(second.result.unwrap(), 2);
        dispatch.close();
        dispatch.join().await;
        assert!(old.recv().await.is_none());
        assert!(new.recv().await.is_none());
    }

    use super::*;
    use std::sync::mpsc as sync;

    #[tokio::test]
    async fn add_remove_keeps_retained_actor_state_and_budgets() {
        let (mut d, mut rx) = Dispatcher::start(
            [("a".into(), 0_usize)].into(),
            2,
            2,
            4,
            |state, item: usize| {
                *state += item;
                Ok(*state)
            },
        )
        .unwrap();
        d.try_submit("a", 1).unwrap();
        assert_eq!(rx.recv().await.unwrap().result.unwrap(), 1);
        d.add_entity("b".into(), 10).unwrap();
        assert!(d.add_entity("a".into(), 999).is_err());
        d.try_submit("b", 2).unwrap();
        assert_eq!(rx.recv().await.unwrap().result.unwrap(), 12);
        assert!(d.remove_entity("b"));
        assert_eq!(d.try_submit("b", 3), Err(3));
        d.try_submit("a", 4).unwrap();
        assert_eq!(rx.recv().await.unwrap().result.unwrap(), 5);
        let timing = d.take_wait_timing();
        assert_eq!(timing["output_reserve"]["count"], 3);
        assert_eq!(timing["cpu_permit"]["count"], 3);
        assert_eq!(d.take_wait_timing()["output_reserve"]["count"], 0);
        assert!(d.workers.available_permits() <= 2);
        d.close();
        assert!(d.add_entity("c".into(), 0).is_err());
        d.join().await;
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn retired_completion_cannot_land_after_same_market_readded() {
        let (mut d, mut rx) =
            Dispatcher::start([("a".into(), ())].into(), 2, 2, 4, |_, item: usize| {
                Ok(item)
            })
            .unwrap();
        d.try_submit("a", 1).unwrap();
        let old = rx.recv().await.unwrap();
        assert!(d.is_current(&old));
        assert!(d.remove_entity("a"));
        d.add_entity("a".into(), ()).unwrap();
        assert!(!d.is_current(&old));
        d.try_submit("a", 2).unwrap();
        let new = rx.recv().await.unwrap();
        assert!(d.is_current(&new));
        assert_ne!(old.incarnation, new.incarnation);
        d.close();
        d.join().await;
    }

    #[tokio::test]
    async fn bounded_actor_batch_preserves_per_item_fifo_and_failures() {
        let (mut dispatch, mut output) = Dispatcher::start_filtered_batched(
            [("a".into(), 0_i64), ("b".into(), 100_i64)].into(),
            8,
            2,
            8,
            4,
            |_, _| false,
            |state, item: i64| {
                if item < 0 {
                    anyhow::bail!("item rejected");
                }
                *state += item;
                Ok(*state)
            },
        )
        .unwrap();
        for item in [1, 2, -1, 3] {
            dispatch.try_submit("a", item).unwrap();
        }
        dispatch.try_submit("b", 7).unwrap();
        let mut a = Vec::new();
        let mut b = Vec::new();
        for _ in 0..5 {
            let completion = output.recv().await.unwrap();
            if completion.entity == "a" {
                a.push(completion.result.map_err(|error| error.to_string()));
            } else {
                b.push(completion.result.map_err(|error| error.to_string()));
            }
        }
        assert_eq!(a[0], Ok(1));
        assert_eq!(a[1], Ok(3));
        assert!(a[2].as_ref().unwrap_err().contains("item rejected"));
        assert_eq!(a[3], Ok(6));
        assert_eq!(b, vec![Ok(107)]);
        dispatch.close();
        dispatch.join().await;
    }

    // Diagnostic reproduction: input slots can reject a burst before any CPU
    // work starts. More CPU permits do not add same-entity admission capacity.
    #[tokio::test(flavor = "current_thread")]
    async fn burst_rejects_with_all_eight_cpu_permits_idle() {
        let (mut dispatch, mut output) = Dispatcher::start(
            [("a".into(), ()), ("b".into(), ())].into(),
            2,
            8,
            16,
            |_, item: usize| Ok(item),
        )
        .unwrap();
        dispatch.try_submit("a", 1).unwrap();
        dispatch.try_submit("a", 2).unwrap();
        assert_eq!(dispatch.occupancy(), (2, 2, 8, 0));
        assert_eq!(dispatch.try_submit("a", 3), Err(3));
        dispatch.try_submit("b", 4).unwrap();
        assert_eq!(dispatch.busiest_queues(1), vec![("a".into(), 2)]);
        assert_eq!(dispatch.busiest_queues(2), vec![("a".into(), 2), ("b".into(), 1)]);
        assert!(dispatch.busiest_queues(0).is_empty());
        dispatch.close();
        let mut a = Vec::new();
        let mut b = Vec::new();
        for _ in 0..3 {
            let item = output.recv().await.unwrap();
            if item.entity == "a" {
                a.push(item.result.unwrap());
            } else {
                b.push(item.result.unwrap());
            }
        }
        assert_eq!(a, [1, 2]);
        assert_eq!(b, [4]);
        dispatch.join().await;
    }

    #[tokio::test]
    async fn fenced_queued_work_is_discarded_before_cpu_and_fresh_work_survives() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let generation = Arc::new(AtomicUsize::new(0));
        let fence = generation.clone();
        let (release, gate) = sync::channel();
        let (entered, wait) = sync::channel();
        let states = [("a".into(), Some(gate))].into();
        let (mut dispatcher, mut output) = Dispatcher::start_filtered(
            states,
            2,
            6,
            12,
            move |_, item: &(usize, usize)| item.0 != fence.load(Ordering::SeqCst),
            move |gate, item| {
                if item.1 == 0 {
                    entered.send(()).unwrap();
                    gate.take()
                        .unwrap()
                        .recv_timeout(std::time::Duration::from_secs(2))
                        .unwrap();
                }
                Ok(item.1)
            },
        )
        .unwrap();
        dispatcher.try_submit("a", (0, 0)).unwrap();
        tokio::task::spawn_blocking(move || {
            wait.recv_timeout(std::time::Duration::from_secs(1))
                .unwrap()
        })
        .await
        .unwrap();
        dispatcher.try_submit("a", (0, 1)).unwrap();
        dispatcher.try_submit("a", (1, 2)).unwrap();
        generation.store(1, Ordering::SeqCst);
        release.send(()).unwrap();
        assert_eq!(output.recv().await.unwrap().result.unwrap(), 0);
        assert_eq!(output.recv().await.unwrap().result.unwrap(), 2);
        dispatcher.close();
        dispatcher.join().await;
        assert!(output.recv().await.is_none());
    }

    #[tokio::test]
    async fn slow_and_failed_a_do_not_hold_b_and_fifo_survives_failure() {
        let (release, gate) = sync::channel();
        let (entered, wait) = sync::channel();
        let states = [("a".into(), Some(gate)), ("b".into(), None)].into();
        let (dispatch, mut output) = Dispatcher::start(states, 4, 2, 8, move |gate, n| {
            if n == 1
                && let Some(gate) = gate
            {
                entered.send(()).unwrap();
                gate.recv_timeout(std::time::Duration::from_secs(2))
                    .unwrap();
                anyhow::bail!("A data rejected");
            }
            Ok(n)
        })
        .unwrap();
        dispatch.try_submit("a", 1).unwrap();
        tokio::task::spawn_blocking(move || {
            wait.recv_timeout(std::time::Duration::from_secs(1))
                .unwrap()
        })
        .await
        .unwrap();
        dispatch.try_submit("a", 2).unwrap();
        dispatch.try_submit("b", 3).unwrap();
        let b = tokio::time::timeout(std::time::Duration::from_millis(200), output.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b.entity, "b");
        assert_eq!(b.result.unwrap(), 3);
        release.send(()).unwrap();
        let a = output.recv().await.unwrap();
        assert_eq!(a.entity, "a");
        assert!(a.result.is_err());
        assert_eq!(output.recv().await.unwrap().result.unwrap(), 2);
        dispatch.finish().await;
    }

    #[tokio::test]
    async fn full_entity_queue_returns_item_and_does_not_block_another_entity() {
        let (release, gate) = sync::channel();
        let (entered, wait) = sync::channel();
        let states = [("a".into(), Some(gate)), ("b".into(), None)].into();
        let (dispatch, mut output) = Dispatcher::start(states, 1, 2, 8, move |gate, n| {
            if n == 1
                && let Some(gate) = gate
            {
                entered.send(()).unwrap();
                gate.recv_timeout(std::time::Duration::from_secs(2))
                    .unwrap();
            }
            Ok(n)
        })
        .unwrap();
        dispatch.try_submit("a", 1).unwrap();
        tokio::task::spawn_blocking(move || {
            wait.recv_timeout(std::time::Duration::from_secs(1))
                .unwrap()
        })
        .await
        .unwrap();
        dispatch.try_submit("a", 2).unwrap();
        assert_eq!(dispatch.try_submit("a", 4), Err(4));
        dispatch.try_submit("b", 3).unwrap();
        assert_eq!(output.recv().await.unwrap().result.unwrap(), 3);
        release.send(()).unwrap();
        output.recv().await.unwrap();
        output.recv().await.unwrap();
        dispatch.finish().await;
    }
}
