//! Bounded, independent replay and durability boundaries for one market at a time.
//!
//! Production publication shares each accepted immutable batch with this
//! replay cache and the writer. The selected-market HTTP snapshot/replay
//! contracts use this boundary. No global publisher cursor is part of a
//! market stream's public identity.
//!
//! This is deliberately not a projection of the legacy global publication
//! cursor. A market's identity, sequence, replay floor, and durable prefix are
//! meaningful without observing any other market.
use anyhow::{Context, Result, ensure};
use super::source_market_persistence::MarketCapacityExhausted;
use marketcow_runtime::discovery_source::market_group_hash;
use serde_json::Value;
use tokio::sync::watch;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Debug)]
pub struct MarketGroup<T> {
    pub market_id: String,
    pub stream_epoch: String,
    pub market_cursor: u64,
    pub group_sha256: String,
    /// The already validated immutable publication batch is shared with the
    /// writer and any other read view; depth is never copied for replay.
    pub payload: Arc<T>,
    bytes: usize,
    retained_bytes: AtomicUsize,
    release_after_durable: usize,
}

pub struct MarketPage<T> {
    pub groups: Vec<Arc<MarketGroup<T>>>,
    pub next_cursor: u64,
    pub published_cursor: u64,
    pub durable_cursor: u64,
    pub caught_up: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketWatermark {
    pub market_id: String,
    pub stream_epoch: String,
    pub published_cursor: u64,
    pub durable_cursor: u64,
    pub replay_floor: u64,
}

struct Lane<T, S> {
    state: S,
    epoch: String,
    retired: bool,
    published: u64,
    durable: u64,
    replay_floor: u64,
    replay_bytes: usize,
    replay_slot: bool,
    groups: VecDeque<Arc<MarketGroup<T>>>,
    changed: watch::Sender<u64>,
}

pub struct MarketReplay<T, S = ()> {
    // Registry mutations are rare. Hot publishing/replay locks only the
    // selected market; two unrelated markets never wait for one lane mutex.
    lanes: RwLock<BTreeMap<String, Arc<Mutex<Lane<T, S>>>>>,
    maximum_groups_per_market: usize,
    maximum_bytes_per_market: usize,
    maximum_total_bytes: usize,
    maximum_market_slots: Option<usize>,
    // A live market may outlive its bounded disk queue. Expire old memory
    // replay groups (including not-yet-durable ones) rather than stopping its
    // current-book projection. Expired readers must take a fresh full sync.
    expire_undurable_groups: bool,
    // Only eviction planning and aggregate reservation use this short lock.
    // Admission/commit callbacks never hold it. Validation, replay and
    // durability remain market-local.
    budget_lock: Mutex<()>,
    total_bytes: AtomicUsize,
    occupied_market_slots: AtomicUsize,
}

/// Pending admissions consume aggregate capacity, but cannot donate proposed
/// eviction capacity before commit. Rejection refunds only this reservation;
/// it cannot overwrite accounting updated by another market in the meantime.
struct ReplayReservation<'a> {
    total: &'a AtomicUsize,
    growth: usize,
    refund_on_commit: usize,
    committed: bool,
}

struct ReplaySlotReservation<'a> {
    occupied: &'a AtomicUsize,
    reserved: bool,
}

impl ReplaySlotReservation<'_> {
    fn commit(mut self) { self.reserved = false; }
}

impl Drop for ReplaySlotReservation<'_> {
    fn drop(&mut self) {
        if self.reserved { self.occupied.fetch_sub(1, Ordering::AcqRel); }
    }
}

impl ReplayReservation<'_> {
    fn commit(mut self) {
        self.total.fetch_sub(self.refund_on_commit, Ordering::AcqRel);
        self.committed = true;
    }
}

impl Drop for ReplayReservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.total.fetch_sub(self.growth, Ordering::AcqRel);
        }
    }
}

impl<T> MarketReplay<T> {
    pub fn new(
        maximum_groups_per_market: usize,
        maximum_bytes_per_market: usize,
        maximum_total_bytes: usize,
    ) -> Result<Self> {
        Self::new_with_state(maximum_groups_per_market, maximum_bytes_per_market, maximum_total_bytes)
    }
}

impl<T, S: Default> MarketReplay<T, S> {
    pub fn new_with_state(
        maximum_groups_per_market: usize,
        maximum_bytes_per_market: usize,
        maximum_total_bytes: usize,
    ) -> Result<Self> {
        ensure!(
            maximum_groups_per_market > 0
                && maximum_bytes_per_market > 0
                && maximum_total_bytes >= maximum_bytes_per_market,
            "market replay limits"
        );
        Ok(Self {
            lanes: RwLock::new(BTreeMap::new()),
            maximum_groups_per_market,
            maximum_bytes_per_market,
            maximum_total_bytes,
            maximum_market_slots: None,
            expire_undurable_groups: false,
            budget_lock: Mutex::new(()),
            total_bytes: AtomicUsize::new(0),
            occupied_market_slots: AtomicUsize::new(0),
        })
    }

    /// Production capacity is pre-partitioned: all occupied market lanes can
    /// reach their own byte limit without borrowing from another market.
    pub fn new_partitioned_with_state(
        maximum_groups_per_market: usize,
        maximum_bytes_per_market: usize,
        maximum_total_bytes: usize,
        maximum_market_slots: usize,
    ) -> Result<Self> {
        ensure!(maximum_market_slots > 0, "market replay slots");
        ensure!(maximum_bytes_per_market.checked_mul(maximum_market_slots)
            .is_some_and(|bytes| bytes <= maximum_total_bytes),
            "market replay partitions exceed aggregate budget");
        let mut replay = Self::new_with_state(maximum_groups_per_market,
            maximum_bytes_per_market, maximum_total_bytes)?;
        replay.maximum_market_slots = Some(maximum_market_slots);
        replay.expire_undurable_groups = true;
        Ok(replay)
    }

    fn lane(&self, market_id: &str) -> Result<Arc<Mutex<Lane<T, S>>>> {
        self.lanes
            .read()
            .map_err(|_| anyhow::anyhow!("market registry poisoned"))?
            .get(market_id)
            .cloned()
            .context("market replay not registered")
    }

    pub fn register(&self, market_id: &str, stream_epoch: &str) -> Result<()> {
        self.register_from_durable(market_id, stream_epoch, 0)
    }

    /// Production preparation may repeat when a current window is carried
    /// forward. It must never silently reset a previously published stream.
    pub fn ensure_registered(&self, market_id: &str, stream_epoch: &str) -> Result<()> {
        ensure!(
            !market_id.is_empty() && !stream_epoch.is_empty(),
            "market replay identity"
        );
        // Existing markets are the hot path. A registry write lock on every
        // group would serialize unrelated markets before they even reach
        // their independent lane locks.
        {
            let handle = self.lanes.read()
                .map_err(|_| anyhow::anyhow!("market registry poisoned"))?
                .get(market_id).cloned();
            if let Some(handle) = handle {
                let lane = handle.lock()
                    .map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
                ensure!(!lane.retired && lane.epoch == stream_epoch,
                    "market replay incumbent epoch differs");
                return Ok(());
            }
        }
        let mut lanes = self
            .lanes
            .write()
            .map_err(|_| anyhow::anyhow!("market registry poisoned"))?;
        if let Some(handle) = lanes.get(market_id).cloned() {
            drop(lanes);
            let lane = handle
                .lock()
                .map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
            ensure!(
                !lane.retired && lane.epoch == stream_epoch,
                "market replay incumbent epoch differs"
            );
            return Ok(());
        }
        lanes.insert(
            market_id.to_owned(),
            Arc::new(Mutex::new(Lane {
                state: S::default(),
                epoch: stream_epoch.to_owned(),
                retired: false,
                published: 0,
                durable: 0,
                replay_floor: 0,
                replay_bytes: 0,
                replay_slot: false,
                groups: VecDeque::new(),
                changed: watch::channel(0).0,
            })),
        );
        Ok(())
    }

    /// The caller must obtain `durable_cursor` from a verified writer
    /// checkpoint, never from a client or an uncommitted publication. In-memory
    /// replay starts at that boundary; older ranges require durable archive
    /// replay or a fresh full-sync, never a fabricated memory history.
    pub fn register_from_durable(
        &self,
        market_id: &str,
        stream_epoch: &str,
        durable_cursor: u64,
    ) -> Result<()> {
        ensure!(
            !market_id.is_empty() && !stream_epoch.is_empty(),
            "market replay identity"
        );
        let mut lanes = self
            .lanes
            .write()
            .map_err(|_| anyhow::anyhow!("market registry poisoned"))?;
        ensure!(
            !lanes.contains_key(market_id),
            "market replay already registered"
        );
        lanes.insert(
            market_id.to_owned(),
            Arc::new(Mutex::new(Lane {
                state: S::default(),
                epoch: stream_epoch.to_owned(),
                retired: false,
                published: durable_cursor,
                durable: durable_cursor,
                replay_floor: durable_cursor,
                replay_bytes: 0,
                replay_slot: false,
                groups: VecDeque::new(),
                changed: watch::channel(durable_cursor).0,
            })),
        );
        Ok(())
    }

    /// Validate and reserve one market-local cursor, admit the immutable group
    /// to the bounded asynchronous writer, then make it visible to replay.
    /// Admission must be non-blocking: a full queue rejects without advancing
    /// this market's cursor or consuming replay capacity. The production fair
    /// writer is released only after the in-memory projection is visible.
    pub fn publish_with_admit<F>(
        &self,
        market_id: &str,
        expected_cursor: u64,
        events: &[Value],
        payload: Arc<T>,
        bytes: usize,
        admit: F,
    ) -> Result<Arc<MarketGroup<T>>>
    where
        F: FnOnce(Arc<MarketGroup<T>>) -> Result<()>,
    {
        self.publish_with_admit_and_commit(
            market_id,
            expected_cursor,
            events,
            payload,
            bytes,
            bytes,
            admit,
            || {},
        )
    }

    /// `commit` installs the already admitted post-state while this market's
    /// lane remains locked. The aggregate budget lock is released first, so a
    /// large book projection for A cannot serialize publication of B.
    pub fn publish_with_admit_and_commit<F, C>(
        &self,
        market_id: &str,
        expected_cursor: u64,
        events: &[Value],
        payload: Arc<T>,
        bytes: usize,
        retained_charge: usize,
        admit: F,
        commit: C,
    ) -> Result<Arc<MarketGroup<T>>>
    where
        F: FnOnce(Arc<MarketGroup<T>>) -> Result<()>,
        C: FnOnce(),
    {
        self.publish_with_state_commit(market_id, expected_cursor, events, payload, bytes,
            retained_charge, admit, |_| commit())
    }

    /// State and replay become visible under the same selected-market lock.
    /// The caller must install a prevalidated projection, not perform I/O.
    pub fn publish_with_state_commit<F, C>(
        &self, market_id: &str, expected_cursor: u64, events: &[Value], payload: Arc<T>,
        bytes: usize, retained_charge: usize, admit: F, commit: C,
    ) -> Result<Arc<MarketGroup<T>>>
    where
        F: FnOnce(Arc<MarketGroup<T>>) -> Result<()>,
        C: FnOnce(&mut S),
    {
        self.publish_with_state_prepare(market_id, expected_cursor, events, payload,
            bytes, retained_charge, |_| Ok(()), admit, |state, ()| commit(state))
    }

    /// Validate and prepare a selected market's next projection while holding
    /// only its lane. No replay cursor or writer admission changes on a failed
    /// preparation. The final commit is infallible and shares the replay
    /// visibility boundary after admission decides writer-owned or live-only
    /// publication. A live-only group still consumes bounded replay capacity.
    pub fn publish_with_state_prepare<P, F, C, U>(
        &self, market_id: &str, expected_cursor: u64, events: &[Value], payload: Arc<T>,
        bytes: usize, retained_charge: usize, prepare: P, admit: F, commit: C,
    ) -> Result<Arc<MarketGroup<T>>>
    where
        P: FnOnce(&S) -> Result<U>,
        F: FnOnce(Arc<MarketGroup<T>>) -> Result<()>,
        C: FnOnce(&mut S, U),
    {
        self.publish_with_state_prepare_charged(market_id, expected_cursor, events,
            payload, bytes, retained_charge, retained_charge, prepare, admit, commit)
    }

    /// The durable charge excludes writer-only materialized books. Until the
    /// writer acknowledges the group, its larger transient charge still uses
    /// this market's replay reservation.
    pub fn publish_with_state_prepare_charged<P, F, C, U>(
        &self, market_id: &str, expected_cursor: u64, events: &[Value], payload: Arc<T>,
        bytes: usize, retained_charge: usize, durable_charge: usize,
        prepare: P, admit: F, commit: C,
    ) -> Result<Arc<MarketGroup<T>>>
    where
        P: FnOnce(&S) -> Result<U>,
        F: FnOnce(Arc<MarketGroup<T>>) -> Result<()>,
        C: FnOnce(&mut S, U),
    {
        ensure!(!events.is_empty(), "empty market observation");
        ensure!(durable_charge >= bytes && durable_charge <= retained_charge,
            "invalid durable market replay charge");
        let lane_handle = self.lane(market_id)?;
        let mut lane = lane_handle
            .lock()
            .map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
        ensure!(!lane.retired, "market replay retired");
        ensure!(
            events.iter().all(|event| event["market_id"] == market_id),
            "cross-market observation group"
        );
        let next = lane
            .published
            .checked_add(1)
            .context("market cursor overflow")?;
        ensure!(
            next == expected_cursor,
            "market replay publication cursor differs"
        );
        let prepared = prepare(&lane.state)?;
        let group_sha256 = market_group_hash(market_id, &lane.epoch, next, &events)?;
        ensure!(bytes > 0 && retained_charge >= bytes,
            "invalid market observation charge");
        if retained_charge > self.maximum_bytes_per_market {
            return Err(MarketCapacityExhausted::new("replay_market_item_bytes",
                retained_charge, lane.replay_bytes, self.maximum_bytes_per_market,
                Some(lane.groups.len()), Some(self.maximum_groups_per_market)).into());
        }
        if retained_charge > self.maximum_total_bytes {
            return Err(MarketCapacityExhausted::new("replay_aggregate_item_bytes",
                retained_charge, self.total_bytes.load(Ordering::Acquire),
                self.maximum_total_bytes, None, None).into());
        }
        let mut retained_bytes = lane.replay_bytes + retained_charge;
        let mut retained_groups = lane.groups.len() + 1;
        let mut evicted_bytes = 0usize;
        let mut evicted_groups = 0usize;
        for old in &lane.groups {
            if retained_groups <= self.maximum_groups_per_market
                && retained_bytes <= self.maximum_bytes_per_market
            {
                break;
            }
            if old.market_cursor > lane.durable && !self.expire_undurable_groups { break; }
            retained_groups -= 1;
            let charge = old.retained_bytes.load(Ordering::Acquire);
            retained_bytes -= charge;
            evicted_bytes += charge;
            evicted_groups += 1;
        }
        if retained_groups > self.maximum_groups_per_market
            || retained_bytes > self.maximum_bytes_per_market {
            return Err(MarketCapacityExhausted::new("replay_market_undurable_prefix",
                retained_charge, lane.replay_bytes, self.maximum_bytes_per_market,
                Some(lane.groups.len()), Some(self.maximum_groups_per_market)).into());
        }
        let _budget = self
            .budget_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("market replay budget poisoned"))?;
        let slot_reservation = if !lane.replay_slot {
            if let Some(limit) = self.maximum_market_slots {
                let used = self.occupied_market_slots.load(Ordering::Acquire);
                if used >= limit {
                    return Err(MarketCapacityExhausted::new("replay_market_slots",
                        retained_charge, self.total_bytes.load(Ordering::Acquire),
                        self.maximum_total_bytes, Some(used), Some(limit)).into());
                }
                self.occupied_market_slots.fetch_add(1, Ordering::AcqRel);
                Some(ReplaySlotReservation { occupied: &self.occupied_market_slots,
                    reserved: true })
            } else { None }
        } else { None };
        // Only this market's own replay may be evicted for its next group.
        // An A admission must never expire B's resumable prefix. Aggregate
        // exhaustion fences A and requests an authoritative A recovery; it
        // does not mutate an unrelated lane.
        let total = self.total_bytes.load(Ordering::Acquire);
        let mut projected_total = total.checked_sub(evicted_bytes)
            .and_then(|remaining| remaining.checked_add(retained_charge))
            .context("aggregate market replay byte accounting overflow")?;
        for old in lane.groups.iter().skip(evicted_groups) {
            if projected_total <= self.maximum_total_bytes {
                break;
            }
            if old.market_cursor > lane.durable && !self.expire_undurable_groups { break; }
            projected_total = projected_total.checked_sub(old.retained_bytes.load(Ordering::Acquire))
                .context("aggregate market replay eviction underflow")?;
            evicted_groups += 1;
        }
        if projected_total > self.maximum_total_bytes {
            return Err(MarketCapacityExhausted::new("replay_aggregate_bytes_after_market_eviction",
                retained_charge, total,
                self.maximum_total_bytes, Some(lane.groups.len()),
                Some(self.maximum_groups_per_market)).into());
        }
        let growth = projected_total.saturating_sub(total);
        let refund_on_commit = total.saturating_sub(projected_total);
        self.total_bytes.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(growth).filter(|next| *next <= self.maximum_total_bytes)
        }).map_err(|_| MarketCapacityExhausted::new("replay_aggregate_reservation_race",
            retained_charge, self.total_bytes.load(Ordering::Acquire),
            self.maximum_total_bytes, None, None))?;
        let reservation = ReplayReservation {
            total: &self.total_bytes, growth, refund_on_commit, committed: false,
        };
        // Admission belongs to this market. Even if a caller stalls here,
        // unrelated markets must not wait for this aggregate-accounting lock.
        drop(_budget);
        let group = Arc::new(MarketGroup {
            market_id: market_id.to_owned(),
            stream_epoch: lane.epoch.clone(),
            market_cursor: next,
            group_sha256,
            payload,
            bytes,
            retained_bytes: AtomicUsize::new(retained_charge),
            release_after_durable: retained_charge - durable_charge,
        });
        if let Err(error) = admit(group.clone()) {
            return Err(error);
        }
        lane.published = next;
        lane.replay_bytes += retained_charge;
        if let Some(reservation) = slot_reservation {
            lane.replay_slot = true;
            reservation.commit();
        }
        lane.groups.push_back(group.clone());
        for _ in 0..evicted_groups {
            // The exact eviction count was staged while this lane remained
            // locked. No fallible step is allowed after writer admission:
            // otherwise its not-yet-ready work item could strand the FIFO.
            let evicted = lane.groups.pop_front()
                .expect("staged market replay eviction missing");
            lane.replay_floor = evicted.market_cursor;
            lane.replay_bytes -= evicted.retained_bytes.load(Ordering::Acquire);
        }
        reservation.commit();
        debug_assert!(self.total_bytes() <= self.maximum_total_bytes);
        commit(&mut lane.state, prepared);
        lane.changed.send_modify(|version| *version = version.wrapping_add(1));
        Ok(group)
    }

    /// Capture only this market's projection and causal boundary. Never acquire
    /// a global publication mutex or hold the directory while waiting for a lane.
    pub fn read_state<R>(&self, market_id: &str,
        read: impl FnOnce(&S, MarketWatermark) -> Result<R>) -> Result<R> {
        let handle = self.lane(market_id)?;
        let lane = handle.lock().map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
        ensure!(!lane.retired && lane.published > 0, "market not published");
        read(&lane.state, MarketWatermark {
            market_id: market_id.to_owned(), stream_epoch: lane.epoch.clone(),
            published_cursor: lane.published, durable_cursor: lane.durable,
            replay_floor: lane.replay_floor,
        })
    }

    /// Memory-only projection updates (confirmation/control) use the same fence
    /// as publication. A not-yet-published market has no readable state to update.
    pub fn update_state(&self, market_id: &str, update: impl FnOnce(&mut S)) -> Result<()> {
        let handle = self.lanes.read().map_err(|_| anyhow::anyhow!("market registry poisoned"))?
            .get(market_id).cloned().context("market replay lane absent")?;
        let mut lane = handle.lock().map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
        ensure!(!lane.retired, "market replay retired");
        update(&mut lane.state);
        lane.changed.send_modify(|version| *version = version.wrapping_add(1));
        Ok(())
    }

    /// Retirement has already entered the writer queue when this is called.
    /// A metadata-only market has no replay lane; for an existing lane, clearing
    /// its public projection must not return an error after that enqueue.
    pub fn clear_state_after_retirement_enqueue(&self, market_id: &str) {
        let handle = self.lanes.read().unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(market_id).cloned();
        if let Some(handle) = handle {
            let mut lane = handle.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            lane.state = S::default();
            lane.changed.send_modify(|version| *version = version.wrapping_add(1));
        }
    }

    /// Read the selected lane even before its first group. Cursor allocation
    /// uses this boundary; it must never borrow a process-wide archive cursor.
    /// The caller still serializes admission for this market, and the later
    /// publish transaction checks the expected group cursor again.
    pub fn inspect_lane<R>(&self, market_id: &str, inspect: impl FnOnce(&S, &MarketWatermark) -> Result<R>) -> Result<R> {
        let handle = self.lane(market_id)?;
        let lane = handle.lock().map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
        ensure!(!lane.retired, "market replay retired");
        inspect(&lane.state, &MarketWatermark {
            market_id: market_id.to_owned(), stream_epoch: lane.epoch.clone(),
            published_cursor: lane.published, durable_cursor: lane.durable,
            replay_floor: lane.replay_floor,
        })
    }

    /// A memory-only market transition (for example, freshness evidence) is
    /// checked against the current book under that market's lane. A stale or
    /// rejected transition leaves the state untouched; unrelated markets do
    /// not wait for its validation or serialization.
    pub fn transition_state<R>(&self, market_id: &str,
        transition: impl FnOnce(&S) -> Result<(Option<S>, R)>) -> Result<R> {
        let handle = self.lane(market_id)?;
        let mut lane = handle.lock().map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
        ensure!(!lane.retired && lane.published > 0, "market not published");
        let (next, result) = transition(&lane.state)?;
        if let Some(next) = next {
            lane.state = next;
            lane.changed.send_modify(|version| *version = version.wrapping_add(1));
        }
        Ok(result)
    }

    pub fn acknowledge_durable(
        &self,
        market_id: &str,
        stream_epoch: &str,
        market_cursor: u64,
    ) -> Result<u64> {
        let handle = self.lane(market_id)?;
        let mut lane = handle
            .lock()
            .map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
        ensure!(!lane.retired, "market replay retired");
        ensure!(lane.epoch == stream_epoch, "stale market durability epoch");
        ensure!(
            market_cursor > 0 && market_cursor <= lane.published,
            "future market durability cursor"
        );
        if market_cursor > lane.durable {
            ensure!(
                market_cursor
                    == lane
                        .durable
                        .checked_add(1)
                        .context("durable cursor overflow")?,
                "market durability completion is not contiguous"
            );
            if let Some(group) = lane.groups.iter()
                .find(|group| group.market_cursor == market_cursor).cloned() {
                let release = group.release_after_durable;
                if release > 0 {
                    let current = group.retained_bytes.load(Ordering::Acquire);
                    ensure!(release <= current && release <= lane.replay_bytes,
                        "market durable replay refund exceeds retained bytes");
                    let _budget = self.budget_lock.lock()
                        .map_err(|_| anyhow::anyhow!("market replay budget poisoned"))?;
                    group.retained_bytes.store(current - release, Ordering::Release);
                    lane.replay_bytes -= release;
                    self.total_bytes.fetch_sub(release, Ordering::AcqRel);
                }
            }
            lane.durable = market_cursor;
            lane.changed.send_modify(|version| *version = version.wrapping_add(1));
        }
        Ok(lane.durable)
    }

    pub fn replay(
        &self,
        market_id: &str,
        stream_epoch: &str,
        after: u64,
        maximum_groups: usize,
        maximum_bytes: usize,
    ) -> Result<MarketPage<T>> {
        ensure!(
            maximum_groups > 0 && maximum_bytes > 0,
            "market replay page budget"
        );
        let handle = self.lane(market_id)?;
        let lane = handle
            .lock()
            .map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
        ensure!(!lane.retired, "market replay retired");
        ensure!(lane.epoch == stream_epoch, "market replay epoch changed");
        ensure!(after <= lane.published, "future market replay cursor");
        ensure!(after >= lane.replay_floor, "market replay expired");
        let mut groups = Vec::new();
        let mut bytes = 0usize;
        let mut next = after;
        for group in lane
            .groups
            .iter()
            .filter(|group| group.market_cursor > after)
        {
            if groups.len() == maximum_groups || group.bytes > maximum_bytes - bytes {
                ensure!(
                    !groups.is_empty(),
                    "single market group exceeds page budget"
                );
                break;
            }
            ensure!(group.market_cursor == next + 1, "market replay gap");
            bytes += group.bytes;
            next = group.market_cursor;
            groups.push(group.clone());
        }
        ensure!(
            after == lane.published || next > after,
            "market replay expired"
        );
        Ok(MarketPage {
            groups,
            next_cursor: next,
            published_cursor: lane.published,
            durable_cursor: lane.durable,
            caught_up: next == lane.published,
        })
    }

    /// Exact diagnostic lookup over the same bounded market replay retained
    /// for consumers. No global event position or cross-market scan is used.
    pub fn find_payload<R>(&self, market_id: &str, stream_epoch: &str,
        mut inspect: impl FnMut(&Arc<T>) -> Option<R>) -> Result<Option<R>> {
        let handle = self.lane(market_id)?;
        let lane = handle.lock().map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
        ensure!(!lane.retired && lane.epoch == stream_epoch,
            "market diagnostic stream epoch changed");
        Ok(lane.groups.iter().find_map(|group| inspect(&group.payload)))
    }

    pub fn replay_floor(&self, market_id: &str) -> Result<u64> {
        let handle = self.lane(market_id)?;
        Ok(handle
            .lock()
            .map_err(|_| anyhow::anyhow!("market lane poisoned"))?
            .replay_floor)
    }

    pub fn watermark(&self, market_id: &str) -> Result<MarketWatermark> {
        let handle = self.lane(market_id)?;
        let lane = handle
            .lock()
            .map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
        ensure!(!lane.retired, "market replay retired");
        Ok(MarketWatermark {
            market_id: market_id.to_owned(),
            stream_epoch: lane.epoch.clone(),
            published_cursor: lane.published,
            durable_cursor: lane.durable,
            replay_floor: lane.replay_floor,
        })
    }

    pub fn watermark_optional(&self, market_id: &str) -> Result<Option<MarketWatermark>> {
        let handle = self.lanes.read()
            .map_err(|_| anyhow::anyhow!("market registry poisoned"))?
            .get(market_id).cloned();
        let Some(handle) = handle else { return Ok(None); };
        let lane = handle.lock().map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
        ensure!(!lane.retired, "market replay retired");
        Ok((lane.published > 0).then(|| MarketWatermark {
            market_id: market_id.to_owned(),stream_epoch:lane.epoch.clone(),
            published_cursor:lane.published,durable_cursor:lane.durable,
            replay_floor:lane.replay_floor,
        }))
    }

    pub fn is_registered(&self, market_id: &str) -> Result<bool> {
        Ok(self.lanes.read().map_err(|_| anyhow::anyhow!("market registry poisoned"))?
            .contains_key(market_id))
    }

    pub fn subscribe(&self, market_id: &str, stream_epoch: &str) -> Result<watch::Receiver<u64>> {
        let handle = self.lane(market_id)?;
        let lane = handle.lock().map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
        ensure!(!lane.retired && lane.epoch == stream_epoch, "market replay epoch changed");
        Ok(lane.changed.subscribe())
    }

    /// Upstream-wide state changes are rare, but must wake each selected
    /// market so a paused source cannot leave a live client waiting forever.
    pub fn notify_all(&self) -> Result<()> {
        let lanes = self.lanes.read().map_err(|_| anyhow::anyhow!("market registry poisoned"))?
            .values().cloned().collect::<Vec<_>>();
        for handle in lanes {
            let lane = handle.lock().map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
            lane.changed.send_modify(|version| *version = version.wrapping_add(1));
        }
        Ok(())
    }

    /// Memory-only confirmation wakes only the selected market, without
    /// creating a group cursor or durable work item.
    pub fn notify_market(&self, market_id: &str) -> Result<()> {
        let handle = self.lanes.read().map_err(|_| anyhow::anyhow!("market registry poisoned"))?
            .get(market_id).cloned();
        if let Some(handle) = handle {
            let lane = handle.lock().map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
            lane.changed.send_modify(|version| *version = version.wrapping_add(1));
        }
        Ok(())
    }

    /// A source-wide acquisition pause ends every in-memory resume range.
    /// Durable archives remain intact, but no client may stitch a pre-pause
    /// market group directly onto post-resume live books.
    pub fn invalidate_all(&self) -> Result<()> {
        self.invalidate_all_with(|state| *state = S::default())
    }

    pub fn invalidate_all_with(&self, invalidate: impl Fn(&mut S)) -> Result<()> {
        let lanes = self.lanes.read().map_err(|_| anyhow::anyhow!("market registry poisoned"))?
            .values().cloned().collect::<Vec<_>>();
        for handle in lanes {
            let mut lane = handle.lock().map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
            let _budget = self.budget_lock.lock()
                .map_err(|_| anyhow::anyhow!("market replay budget poisoned"))?;
            self.total_bytes.fetch_sub(lane.replay_bytes, Ordering::AcqRel);
            lane.groups.clear();
            if lane.replay_slot {
                self.occupied_market_slots.fetch_sub(1, Ordering::AcqRel);
                lane.replay_slot = false;
            }
            invalidate(&mut lane.state);
            lane.replay_bytes = 0;
            lane.replay_floor = lane.published;
            lane.changed.send_modify(|version| *version = version.wrapping_add(1));
        }
        Ok(())
    }

    /// Release only hot replay memory after the writer has durably committed
    /// the entire market prefix. Cold market/evidence records live in the
    /// writer and are never deleted by this operation.
    pub fn retire(&self, market_id: &str, stream_epoch: &str) -> Result<()> {
        self.retire_with_durability_gap(market_id, stream_epoch, false)
    }

    /// A live-only market has an explicit irreversible persistence gap. Its
    /// unwritten suffix cannot become durable, but retiring the hot replay is
    /// still safe after the writer has drained the earlier durable prefix.
    pub fn retire_with_durability_gap(&self, market_id: &str, stream_epoch: &str,
        allow_gap: bool) -> Result<()> {
        let handle = self.lane(market_id)?;
        let mut lane = handle
            .lock()
            .map_err(|_| anyhow::anyhow!("market lane poisoned"))?;
        ensure!(!lane.retired && lane.epoch == stream_epoch, "stale market retirement epoch");
        ensure!(allow_gap || lane.published == lane.durable,
            "market retirement awaiting durability");
        let _budget = self
            .budget_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("market replay budget poisoned"))?;
        let mut lanes = self.lanes.write()
            .map_err(|_| anyhow::anyhow!("market registry poisoned"))?;
        ensure!(lanes.get(market_id).is_some_and(|current| Arc::ptr_eq(current, &handle)),
            "market retirement registry incarnation changed");
        ensure!(self.total_bytes.load(Ordering::Acquire) >= lane.replay_bytes,
            "market replay retirement byte accounting underflow");
        ensure!(!lane.replay_slot || self.occupied_market_slots.load(Ordering::Acquire) > 0,
            "market replay retirement slot accounting underflow");
        lane.retired = true;
        self.total_bytes
            .fetch_sub(lane.replay_bytes, Ordering::AcqRel);
        if lane.replay_slot {
            self.occupied_market_slots.fetch_sub(1, Ordering::AcqRel);
            lane.replay_slot = false;
        }
        lane.replay_bytes = 0;
        lane.groups.clear();
        lane.state = S::default();
        lane.changed.send_modify(|version| *version = version.wrapping_add(1));
        lanes.remove(market_id);
        Ok(())
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes.load(Ordering::Acquire)
    }
}

#[cfg(test)]
impl MarketReplay<Vec<Value>> {
    fn publish(&self, market_id: &str, events: Vec<Value>) -> Result<Arc<MarketGroup<Vec<Value>>>> {
        let next = self.watermark(market_id)?.published_cursor + 1;
        let bytes = serde_json::to_vec(&events)?.len();
        let payload = Arc::new(events);
        self.publish_with_admit(
            market_id,
            next,
            payload.as_slice(),
            payload.clone(),
            bytes,
            |_| Ok(()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(market: &str, token: &str) -> Value {
        json!({"market_id":market,"token_id":token,"event_id":format!("{market}-{token}")})
    }

    #[test]
    fn selected_state_commit_is_atomic_and_does_not_lock_other_markets() {
        let replay = Arc::new(MarketReplay::<Vec<Value>, u64>::new_with_state(4, 4096, 8192).unwrap());
        replay.register("a", "epoch-a").unwrap();
        replay.register("b", "epoch-b").unwrap();
        let commit = |market: &str, cursor| {
            let events = Arc::new(vec![event(market, "up")]);
            replay.publish_with_state_commit(market, cursor, &events, events.clone(), 100, 100,
                |_| Ok(()), |state| *state = cursor).unwrap();
        };
        commit("a", 1);
        commit("b", 1);
        let (started, wait) = std::sync::mpsc::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let worker = replay.clone();
        let task = std::thread::spawn(move || {
            let events = Arc::new(vec![event("a", "up")]);
            worker.publish_with_state_commit("a", 2, &events, events.clone(), 100, 100,
                |_| Ok(()), |state| {
                    started.send(()).unwrap();
                    gate.recv_timeout(std::time::Duration::from_secs(3)).unwrap();
                    *state = 2;
                }).unwrap();
        });
        wait.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        // A has staged cursor 2, but state/cursor readers cannot see a torn
        // boundary. B's read, commit and fsync acknowledgement still progress.
        commit("b", 2);
        replay.acknowledge_durable("b", "epoch-b", 1).unwrap();
        replay.acknowledge_durable("b", "epoch-b", 2).unwrap();
        assert_eq!(replay.read_state("b", |state, watermark| {
            assert_eq!(*state, watermark.published_cursor);
            Ok((*state, watermark.durable_cursor))
        }).unwrap(), (2, 2));
        release.send(()).unwrap();
        task.join().unwrap();
        assert_eq!(replay.read_state("a", |state, watermark| {
            assert_eq!(*state, watermark.published_cursor); Ok(*state)
        }).unwrap(), 2);
        let events = Arc::new(vec![event("a", "up")]);
        assert!(replay.publish_with_state_commit("a", 3, &events, events.clone(), 100, 100,
            |_| anyhow::bail!("explicit admission rejection"), |state| *state = 3).is_err());
        assert_eq!(replay.read_state("a", |state, watermark| {
            assert_eq!(*state, watermark.published_cursor); Ok(*state)
        }).unwrap(), 2);
        replay.invalidate_all().unwrap();
        assert_eq!(replay.read_state("a", |state, _| Ok(*state)).unwrap(), 0);
    }

    #[test]
    fn retained_charge_evicts_without_inflating_wire_page_size() {
        let replay = MarketReplay::new(10, 1024, 1024).unwrap();
        replay.register("a", "epoch-a").unwrap();
        for cursor in 1..=2 {
            let payload = Arc::new(vec![event("a", "up")]);
            replay.publish_with_admit_and_commit("a", cursor, &payload,
                payload.clone(), 100, 900, |_| Ok(()), || {}).unwrap();
            replay.acknowledge_durable("a", "epoch-a", cursor).unwrap();
        }
        assert_eq!(replay.total_bytes.load(Ordering::Acquire), 900);
        assert!(replay.replay("a", "epoch-a", 0, 10, 1024).is_err());
        let page = replay.replay("a", "epoch-a", 1, 10, 100).unwrap();
        assert_eq!(page.next_cursor, 2);
        assert_eq!(page.groups.len(), 1);
    }

    #[test]
    fn partitioned_markets_have_independent_fifo_and_bounded_total() {
        let replay = MarketReplay::<Vec<Value>>::new_partitioned_with_state(
            2, 100, 500, 4).unwrap();
        for market in ["a", "b", "c", "d", "e"] {
            replay.register(market, "epoch").unwrap();
        }
        for market in ["a", "b", "c", "d"] {
            let events = Arc::new(vec![event(market, "up")]);
            replay.publish_with_admit_and_commit(market, 1, &events,
                events.clone(), 50, 90, |_| Ok(()), || {}).unwrap();
            replay.acknowledge_durable(market, "epoch", 1).unwrap();
        }
        let next = Arc::new(vec![event("a", "up")]);
        replay.publish_with_admit_and_commit("a", 2, &next,
            next.clone(), 50, 90, |_| Ok(()), || {}).unwrap();
        assert_eq!(replay.replay_floor("a").unwrap(), 1);
        assert_eq!(replay.replay_floor("b").unwrap(), 0);
        assert!(replay.total_bytes() <= 400);
        let fifth = Arc::new(vec![event("e", "up")]);
        let error = replay.publish_with_admit_and_commit("e", 1, &fifth,
            fifth.clone(), 50, 90, |_| Ok(()), || {}).unwrap_err();
        assert_eq!(error.downcast_ref::<MarketCapacityExhausted>().unwrap().limiter,
            "replay_market_slots");
        assert_eq!(replay.watermark("e").unwrap().published_cursor, 0);
        replay.acknowledge_durable("a", "epoch", 2).unwrap();
        replay.retire("a", "epoch").unwrap();
        replay.publish_with_admit_and_commit("e", 1, &fifth,
            fifth.clone(), 50, 90, |_| Ok(()), || {}).unwrap();
        assert_eq!(replay.occupied_market_slots.load(Ordering::Acquire), 4);
        assert!(replay.total_bytes() <= 400);
    }

    #[test]
    fn undurable_group_can_expire_from_bounded_replay_without_advancing_durable() {
        let replay = MarketReplay::<Vec<Value>>::new_partitioned_with_state(
            1, 100, 200, 1).unwrap();
        replay.register("a", "epoch").unwrap();
        let events = Arc::new(vec![event("a", "up")]);
        replay.publish_with_admit_and_commit("a", 1, &events,
            events.clone(), 50, 90, |_| Ok(()), || {}).unwrap();
        replay.publish_with_admit_and_commit("a", 2, &events,
            events.clone(), 50, 90, |_| Ok(()), || {}).unwrap();
        assert_eq!(replay.watermark("a").unwrap().published_cursor, 2);
        assert_eq!(replay.watermark("a").unwrap().durable_cursor, 0);
        assert_eq!(replay.replay_floor("a").unwrap(), 1);
        assert!(replay.replay("a", "epoch", 0, 1, 100).is_err());
        replay.acknowledge_durable("a", "epoch", 1).unwrap();
        assert_eq!(replay.watermark("a").unwrap().durable_cursor, 1);
        assert_eq!(replay.replay_floor("a").unwrap(), 1);
        assert!(replay.replay("a", "epoch", 0, 1, 100).is_err());
    }

    #[test]
    fn durable_ack_refunds_writer_only_retained_bytes() {
        let replay = MarketReplay::<Vec<Value>>::new_partitioned_with_state(
            2, 100, 200, 1).unwrap();
        replay.register("a", "epoch").unwrap();
        let events = Arc::new(vec![event("a", "up")]);
        replay.publish_with_state_prepare_charged("a", 1, &events,
            events.clone(), 50, 100, 60, |_| Ok(()), |_| Ok(()), |_, ()| {})
            .unwrap();
        assert_eq!(replay.total_bytes(), 100);
        replay.acknowledge_durable("a", "epoch", 1).unwrap();
        assert_eq!(replay.total_bytes(), 60);
        assert_eq!(replay.lane("a").unwrap().lock().unwrap().replay_bytes, 60);
        assert_eq!(replay.acknowledge_durable("a", "epoch", 1).unwrap(), 1);
        assert_eq!(replay.total_bytes(), 60);
    }

    #[test]
    fn independent_market_cursors_and_selective_replay() {
        let replay = MarketReplay::new(3, 4096, 8192).unwrap();
        replay.register("a", "epoch-a").unwrap();
        replay.register("b", "epoch-b").unwrap();
        replay
            .publish("a", vec![event("a", "up"), event("a", "down")])
            .unwrap();
        replay.publish("a", vec![event("a", "up")]).unwrap();
        replay.publish("b", vec![event("b", "up")]).unwrap();
        assert_eq!(
            replay
                .replay("a", "epoch-a", 0, 3, 4096)
                .unwrap()
                .next_cursor,
            2
        );
        let b = replay.replay("b", "epoch-b", 0, 3, 4096).unwrap();
        assert_eq!(b.next_cursor, 1);
        assert_eq!(b.groups[0].payload.len(), 1);
        assert_eq!(b.groups[0].market_id, "b");
        assert_eq!(b.groups[0].stream_epoch, "epoch-b");
        assert_eq!(b.groups[0].group_sha256.len(), 64);
        assert!(b.caught_up);
        assert_eq!(b.published_cursor, 1);
        assert!(replay.publish("a", vec![event("b", "down")]).is_err());
    }

    #[test]
    fn existing_market_registration_does_not_wait_for_registry_writer() {
        let replay = Arc::new(MarketReplay::<Vec<Value>>::new(3, 4096, 8192).unwrap());
        replay.register("a", "epoch-a").unwrap();
        let held_reader = replay.lanes.read().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = replay.clone();
        let task = std::thread::spawn(move || {
            sender.send(worker.ensure_registered("a", "epoch-a")).unwrap();
        });
        let result = receiver.recv_timeout(std::time::Duration::from_millis(500));
        drop(held_reader);
        task.join().unwrap();
        result.expect("existing market must use registry read path").unwrap();
        assert!(replay.ensure_registered("a", "wrong-epoch").is_err());
    }

    #[test]
    fn subscribers_wake_only_for_their_market_until_source_wide_invalidation() {
        let replay = MarketReplay::new(3, 4096, 8192).unwrap();
        replay.register("a", "epoch").unwrap();
        replay.register("b", "epoch").unwrap();
        let a = replay.subscribe("a", "epoch").unwrap();
        let mut b = replay.subscribe("b", "epoch").unwrap();
        replay.publish("b", vec![event("b", "up")]).unwrap();
        assert!(!a.has_changed().unwrap());
        assert!(b.has_changed().unwrap());
        b.borrow_and_update();
        replay.acknowledge_durable("b", "epoch", 1).unwrap();
        assert!(!a.has_changed().unwrap());
        assert!(b.has_changed().unwrap());
        replay.invalidate_all().unwrap();
        assert!(a.has_changed().unwrap());
        assert!(replay.replay("b", "epoch", 0, 1, 4096).is_err());
        assert_eq!(replay.replay_floor("b").unwrap(), 1);
        assert_eq!(replay.total_bytes(), 0);
    }

    #[test]
    fn expiry_and_durability_are_market_local() {
        let replay = MarketReplay::new(2, 4096, 8192).unwrap();
        replay.register("a", "epoch-a").unwrap();
        replay.register("b", "epoch-b").unwrap();
        for cursor in 1..=3 {
            replay.publish("a", vec![event("a", "up")]).unwrap();
            replay.acknowledge_durable("a", "epoch-a", cursor).unwrap();
        }
        replay.publish("b", vec![event("b", "up")]).unwrap();
        assert_eq!(replay.replay_floor("a").unwrap(), 1);
        assert_eq!(replay.replay_floor("b").unwrap(), 0);
        assert!(replay.replay("a", "epoch-a", 0, 2, 4096).is_err());
        assert_eq!(
            replay
                .replay("b", "epoch-b", 0, 2, 4096)
                .unwrap()
                .next_cursor,
            1
        );
        assert_eq!(replay.acknowledge_durable("a", "epoch-a", 3).unwrap(), 3);
        assert_eq!(
            replay
                .replay("b", "epoch-b", 0, 2, 4096)
                .unwrap()
                .durable_cursor,
            0
        );
    }

    #[test]
    fn aggregate_pressure_rejects_a_without_expiring_b_replay() {
        let event_size = serde_json::to_vec(&vec![event("a", "up")]).unwrap().len();
        let replay = MarketReplay::new(2, event_size, event_size).unwrap();
        replay.register("a", "epoch-a").unwrap();
        replay.register("b", "epoch-b").unwrap();
        replay.publish("a", vec![event("a", "up")]).unwrap();
        assert!(replay.publish("b", vec![event("b", "up")]).is_err());
        assert_eq!(replay.replay_floor("a").unwrap(), 0);
        assert_eq!(replay.replay("a", "epoch-a", 0, 2, 4096).unwrap().next_cursor, 1);
        assert_eq!(replay.watermark("b").unwrap().published_cursor, 0);
        assert_eq!(replay.total_bytes(), event_size);
    }

    #[test]
    fn writer_rejection_under_aggregate_pressure_preserves_other_market_replay() {
        let size = serde_json::to_vec(&vec![event("a", "up")]).unwrap().len();
        let replay = MarketReplay::new(2, size, size).unwrap();
        replay.register("a", "epoch-a").unwrap();
        replay.register("b", "epoch-b").unwrap();
        replay.publish("a", vec![event("a", "up")]).unwrap();
        let events = Arc::new(vec![event("b", "up")]);
        assert!(replay.publish_with_admit("b", 1, events.as_slice(), events.clone(), size,
            |_| anyhow::bail!("writer queue full")).is_err());
        assert_eq!(replay.replay_floor("a").unwrap(), 0);
        assert_eq!(replay.replay("a", "epoch-a", 0, 1, size).unwrap().next_cursor, 1);
        assert_eq!(replay.watermark("b").unwrap().published_cursor, 0);
        assert_eq!(replay.total_bytes(), size);
    }

    #[test]
    fn aggregate_pressure_evicts_only_hot_markets_own_old_groups() {
        let size = serde_json::to_vec(&vec![event("a", "up")]).unwrap().len();
        let replay = MarketReplay::new(5, size * 3, size * 3).unwrap();
        replay.register("a", "epoch-a").unwrap();
        replay.register("b", "epoch-b").unwrap();
        replay.publish("b", vec![event("b", "up")]).unwrap();
        replay.acknowledge_durable("b", "epoch-b", 1).unwrap();
        replay.publish("a", vec![event("a", "up")]).unwrap();
        replay.acknowledge_durable("a", "epoch-a", 1).unwrap();
        replay.publish("a", vec![event("a", "up")]).unwrap();
        replay.acknowledge_durable("a", "epoch-a", 2).unwrap();
        replay.publish("a", vec![event("a", "up")]).unwrap();
        assert_eq!(replay.total_bytes(), size * 3);
        assert_eq!(replay.replay_floor("a").unwrap(), 1);
        assert_eq!(replay.replay_floor("b").unwrap(), 0);
        assert_eq!(
            replay
                .replay("b", "epoch-b", 0, 1, size)
                .unwrap()
                .next_cursor,
            1
        );
        assert_eq!(
            replay
                .replay("a", "epoch-a", 1, 2, size * 2)
                .unwrap()
                .next_cursor,
            3
        );
    }

    #[test]
    fn replay_shares_exact_payload_arc_without_copying_depth() {
        let replay = MarketReplay::new(2, 4096, 4096).unwrap();
        replay.register("a", "epoch-a").unwrap();
        let payload = Arc::new(vec![event("a", "up"), event("a", "down")]);
        let bytes = serde_json::to_vec(payload.as_ref()).unwrap().len();
        let group = replay
            .publish_with_admit("a", 1, payload.as_slice(), payload.clone(), bytes, |_| {
                Ok(())
            })
            .unwrap();
        let from_page = replay.replay("a", "epoch-a", 0, 1, 4096).unwrap();
        assert!(Arc::ptr_eq(&group.payload, &payload));
        assert!(Arc::ptr_eq(&from_page.groups[0].payload, &payload));
    }

    #[test]
    fn writer_admission_rejection_keeps_cursor_replay_and_budget_unchanged() {
        let replay = MarketReplay::new(1, 4096, 8192).unwrap();
        replay.register("a", "epoch-a").unwrap();
        let first = replay
            .publish("a", vec![event("a", "up"), event("a", "down")])
            .unwrap();
        replay.acknowledge_durable("a", "epoch-a", 1).unwrap();
        let before = replay.total_bytes();
        let rejected_events = vec![event("a", "up")];
        let rejected_bytes = serde_json::to_vec(&rejected_events).unwrap().len();
        let rejected_payload = Arc::new(rejected_events);
        let rejection = replay.publish_with_admit(
            "a",
            2,
            rejected_payload.as_slice(),
            rejected_payload.clone(),
            rejected_bytes,
            |group| {
                assert_eq!(group.market_cursor, 2);
                anyhow::bail!("writer queue full")
            },
        );
        assert!(
            rejection
                .unwrap_err()
                .to_string()
                .contains("writer queue full")
        );
        assert_eq!(replay.total_bytes(), before);
        assert_eq!(replay.watermark("a").unwrap().published_cursor, 1);
        assert_eq!(
            replay.replay("a", "epoch-a", 0, 1, 4096).unwrap().groups[0].group_sha256,
            first.group_sha256
        );
        let accepted_events = vec![event("a", "up")];
        let accepted_bytes = serde_json::to_vec(&accepted_events).unwrap().len();
        let accepted_payload = Arc::new(accepted_events);
        let accepted = replay
            .publish_with_admit(
                "a",
                2,
                accepted_payload.as_slice(),
                accepted_payload.clone(),
                accepted_bytes,
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(accepted.market_cursor, 2);
    }

    #[test]
    fn one_market_lock_does_not_hold_another_market() {
        let replay = Arc::new(MarketReplay::new(4, 4096, 8192).unwrap());
        replay.register("a", "epoch-a").unwrap();
        replay.register("b", "epoch-b").unwrap();
        let a_handle = replay.lane("a").unwrap();
        let _a_guard = a_handle.lock().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = replay.clone();
        let handle = std::thread::spawn(move || {
            sender
                .send(
                    worker
                        .publish("b", vec![event("b", "up")])
                        .map(|group| group.market_cursor),
                )
                .unwrap();
        });
        assert_eq!(
            receiver
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap()
                .unwrap(),
            1
        );
        handle.join().unwrap();
    }

    #[test]
    fn blocked_retirement_does_not_hold_registry_against_other_market() {
        let replay = Arc::new(MarketReplay::<Vec<Value>>::new(4, 4096, 8192).unwrap());
        replay.register("a", "epoch-a").unwrap();
        let a_handle = replay.lane("a").unwrap();
        let held = a_handle.lock().unwrap();
        let retirement = replay.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let retiring = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            retirement.retire("a", "epoch-a")
        });
        started_rx.recv().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        let other = replay.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let b = std::thread::spawn(move || {
            done_tx.send(other.register("b", "epoch-b")
                .and_then(|_| other.publish("b", vec![event("b", "up")]))).unwrap();
        });
        let result = done_rx.recv_timeout(std::time::Duration::from_secs(2));
        drop(held);
        retiring.join().unwrap().unwrap();
        b.join().unwrap();
        assert_eq!(result.unwrap().unwrap().market_cursor, 1);
        assert!(!replay.is_registered("a").unwrap());
        assert!(replay.is_registered("b").unwrap());
        assert_eq!(replay.total_bytes(), replay.lane("b").unwrap().lock().unwrap().replay_bytes);
    }

    #[test]
    fn slow_market_commit_does_not_hold_aggregate_budget_or_other_lane() {
        let replay = Arc::new(MarketReplay::new(4, 4096, 8192).unwrap());
        replay.register("a", "epoch-a").unwrap();
        replay.register("b", "epoch-b").unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first = replay.clone();
        let a = std::thread::spawn(move || {
            let payload = Arc::new(vec![event("a", "up")]);
            let bytes = serde_json::to_vec(payload.as_ref()).unwrap().len();
            first
                .publish_with_admit_and_commit(
                    "a",
                    1,
                    payload.as_slice(),
                    payload.clone(),
                    bytes,
                    bytes,
                    |_| Ok(()),
                    || {
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                    },
                )
                .unwrap();
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let second = replay.clone();
        let b = std::thread::spawn(move || {
            done_tx
                .send(
                    second
                        .publish("b", vec![event("b", "up")])
                        .map(|g| g.market_cursor),
                )
                .unwrap();
        });
        let outcome = done_rx.recv_timeout(std::time::Duration::from_secs(1));
        release_tx.send(()).unwrap();
        a.join().unwrap();
        b.join().unwrap();
        assert_eq!(outcome.unwrap().unwrap(), 1);
    }

    #[test]
    fn stalled_market_admission_does_not_block_other_market_or_overwrite_its_charge() {
        for reject in [false, true] {
            let replay = Arc::new(MarketReplay::new(4, 4096, 8192).unwrap());
            replay.register("a", "epoch-a").unwrap();
            replay.register("b", "epoch-b").unwrap();
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let a_replay = replay.clone();
            let a = std::thread::spawn(move || {
                let payload = Arc::new(vec![event("a", "up")]);
                let bytes = serde_json::to_vec(payload.as_ref()).unwrap().len();
                a_replay.publish_with_admit(
                    "a", 1, payload.as_slice(), payload.clone(), bytes,
                    |_| {
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        ensure!(!reject, "explicit admission rejection");
                        Ok(())
                    },
                )
            });
            entered_rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
            let reserved_a = replay.total_bytes();
            assert!(reserved_a > 0);
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let b_replay = replay.clone();
            let b = std::thread::spawn(move || {
                done_tx.send(b_replay.publish("b", vec![event("b", "up")])).unwrap();
            });
            let result = done_rx.recv_timeout(std::time::Duration::from_secs(2));
            // Always release the stalled callback before making assertions.
            release_tx.send(()).unwrap();
            let a_result = a.join().unwrap();
            b.join().unwrap();
            let b_group = result.unwrap().unwrap();
            assert_eq!(b_group.market_cursor, 1);
            assert_eq!(a_result.is_err(), reject);
            assert_eq!(replay.watermark("a").unwrap().published_cursor, u64::from(!reject));
        assert_eq!(replay.total_bytes(), b_group.retained_bytes.load(Ordering::Acquire)
            + if reject { 0 } else { reserved_a });
        }
    }

    #[test]
    fn rejected_eviction_does_not_donate_capacity_to_another_market() {
        let replay = Arc::new(MarketReplay::new(1, 100, 100).unwrap());
        replay.register("a", "epoch-a").unwrap();
        replay.register("b", "epoch-b").unwrap();
        let events = Arc::new(vec![event("a", "up")]);
        replay.publish_with_admit("a", 1, &events, events.clone(), 100, |_| Ok(())).unwrap();
        replay.acknowledge_durable("a", "epoch-a", 1).unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = replay.clone();
        let a = std::thread::spawn(move || {
            worker.publish_with_admit("a", 2, &events, events.clone(), 10, |_| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                anyhow::bail!("writer rejection")
            })
        });
        entered_rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        assert_eq!(replay.total_bytes(), 100);
        let b_events = Arc::new(vec![event("b", "up")]);
        let result = replay.publish_with_admit("b", 1, &b_events, b_events.clone(), 10, |_| Ok(()));
        release_tx.send(()).unwrap();
        assert!(a.join().unwrap().is_err());
        assert!(result.is_err());
        assert_eq!(replay.total_bytes(), 100);
        assert_eq!(replay.watermark("a").unwrap().published_cursor, 1);
        assert_eq!(replay.watermark("b").unwrap().published_cursor, 0);
    }

    #[test]
    fn concurrent_market_reservations_refunds_and_evictions_keep_exact_total() {
        let replay = Arc::new(MarketReplay::new(2, 1024, 4096).unwrap());
        let mut workers = Vec::new();
        for market in ["a", "b", "c", "d"] {
            replay.register(market, &format!("epoch-{market}")).unwrap();
            let replay = replay.clone();
            workers.push(std::thread::spawn(move || {
                let payload = Arc::new(vec![event(market, "up")]);
                for index in 0..2500 {
                    let next = replay.watermark(market).unwrap().published_cursor + 1;
                    let charge = if index % 2 == 0 { 90 } else { 110 };
                    let rejected = index % 7 == 0;
                    let result = replay.publish_with_admit_and_commit(
                        market, next, &payload, payload.clone(), 50, charge,
                        |_| { ensure!(!rejected, "test writer rejection"); Ok(()) }, || {},
                    );
                    assert_eq!(result.is_err(), rejected);
                    if result.is_ok() {
                        replay.acknowledge_durable(market, &format!("epoch-{market}"), next).unwrap();
                    }
                    assert!(replay.total_bytes() <= 4096);
                }
            }));
        }
        for worker in workers { worker.join().unwrap(); }
        let sum = ["a", "b", "c", "d"].iter().map(|market| {
            let handle = replay.lane(market).unwrap();
            let lane = handle.lock().unwrap();
            assert!(lane.groups.len() <= 2);
            assert_eq!(lane.replay_bytes, lane.groups.iter()
                .map(|group| group.retained_bytes.load(Ordering::Acquire)).sum::<usize>());
            lane.replay_bytes
        }).sum::<usize>();
        assert_eq!(replay.total_bytes(), sum);
    }

    #[test]
    fn retirement_requires_durable_prefix_and_does_not_affect_other_markets() {
        let replay = MarketReplay::new(2, 4096, 8192).unwrap();
        replay.register("a", "epoch-a").unwrap();
        replay.register("b", "epoch-b").unwrap();
        replay.publish("a", vec![event("a", "up")]).unwrap();
        replay.publish("b", vec![event("b", "up")]).unwrap();
        assert!(replay.retire("a", "epoch-a").is_err());
        assert_eq!(replay.watermark("a").unwrap().published_cursor, 1);
        replay.acknowledge_durable("a", "epoch-a", 1).unwrap();
        replay.retire("a", "epoch-a").unwrap();
        assert!(replay.replay("a", "epoch-a", 0, 1, 4096).is_err());
        assert_eq!(
            replay
                .replay("b", "epoch-b", 0, 1, 4096)
                .unwrap()
                .next_cursor,
            1
        );
        replay.register("a", "new-epoch-a").unwrap();
        assert_eq!(replay.watermark("a").unwrap().published_cursor, 0);
        assert!(replay.replay("a", "epoch-a", 0, 1, 4096).is_err());
        assert!(replay.acknowledge_durable("a", "epoch-a", 1).is_err());
    }

    #[test]
    fn explicit_live_only_gap_can_retire_without_claiming_durability() {
        let replay = MarketReplay::new(2, 4096, 8192).unwrap();
        replay.register("a", "epoch-a").unwrap();
        replay.publish("a", vec![event("a", "up")]).unwrap();
        assert_eq!(replay.watermark("a").unwrap().durable_cursor, 0);
        assert!(replay.retire("a", "epoch-a").is_err());
        assert!(replay.retire_with_durability_gap("a", "wrong-epoch", true).is_err());
        replay.retire_with_durability_gap("a", "epoch-a", true).unwrap();
        assert!(replay.watermark("a").is_err());
        assert_eq!(replay.total_bytes(), 0);
        replay.register("a", "fresh-epoch").unwrap();
        assert_eq!(replay.watermark("a").unwrap().published_cursor, 0);
    }

    #[test]
    fn restart_resumes_only_verified_durable_boundary() {
        let replay = MarketReplay::new(2, 4096, 8192).unwrap();
        replay.register_from_durable("a", "epoch-a", 17).unwrap();
        let watermark = replay.watermark("a").unwrap();
        assert_eq!(
            (
                watermark.published_cursor,
                watermark.durable_cursor,
                watermark.replay_floor
            ),
            (17, 17, 17)
        );
        assert!(replay.replay("a", "epoch-a", 16, 2, 4096).is_err());
        assert_eq!(
            replay
                .replay("a", "epoch-a", 17, 2, 4096)
                .unwrap()
                .groups
                .len(),
            0
        );
        assert_eq!(
            replay
                .publish("a", vec![event("a", "up")])
                .unwrap()
                .market_cursor,
            18
        );
        assert_eq!(replay.acknowledge_durable("a", "epoch-a", 18).unwrap(), 18);
        assert!(replay.register_from_durable("a", "epoch-b", 0).is_err());
    }

    #[test]
    fn repeated_preparation_carries_market_cursor_without_epoch_reset() {
        let replay = MarketReplay::new(2, 4096, 8192).unwrap();
        replay.ensure_registered("a", "epoch-a").unwrap();
        replay.publish("a", vec![event("a", "up")]).unwrap();
        replay.ensure_registered("a", "epoch-a").unwrap();
        assert_eq!(replay.watermark("a").unwrap().published_cursor, 1);
        assert!(replay.ensure_registered("a", "epoch-b").is_err());
        assert_eq!(replay.watermark("a").unwrap().stream_epoch, "epoch-a");
    }
}
