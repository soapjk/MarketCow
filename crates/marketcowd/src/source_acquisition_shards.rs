//! Collector-owned, bounded upstream connections. Each market pair has its
//! own socket/reconnect boundary; retained pairs are not restarted when a
//! different market is installed. No detached socket tasks.
use anyhow::{Context, Result, ensure};
use futures_util::FutureExt;
use serde_json::{Value, json};
use marketcow_polymarket::{
    PolymarketTransportConfig, RawTransportBatch, TransportBudget, TransportError,
    TransportReadyHandle,
    subscription::{SubscriptionUpdate, subscription_commands},
    supervise_polymarket_transport_managed,
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};

struct Shard {
    tokens: BTreeSet<String>,
    stop: watch::Sender<bool>,
    commands: mpsc::Sender<SubscriptionUpdate>,
    pending: Option<(
        BTreeSet<String>,
        oneshot::Receiver<Result<(), TransportError>>,
    )>,
    stopping: bool,
    retirement_target: Option<BTreeSet<String>>,
}

enum ShardExit {
    Finished(Result<(), TransportError>),
    Panicked,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source_acquisition_lease::Leases;
    use std::time::Instant;
    fn output() -> TransportBudget {
        marketcow_polymarket::transport_budget(1, 65_536, 65_536).unwrap()
    }
    #[tokio::test]
    async fn additions_preserve_owned_shards_and_capacity_rejects_before_mutation() {
        let output = output();
        let mut pool =
            Shards::new(PolymarketTransportConfig::production(), output, 6, 2, 4).unwrap();
        let original = pool.add_pairs(&[["11".into(), "12".into()]]).unwrap()[0];
        pool.add_pairs(&[["21".into(), "22".into()]]).unwrap();
        assert_eq!(
            pool.shards[&original].tokens,
            BTreeSet::from(["11".into(), "12".into()])
        );
        let before = pool.tokens();
        assert!(pool.add_pairs(&[["31".into(), "32".into()]]).is_err());
        assert_eq!(pool.tokens(), before);
        assert_eq!(pool.jobs.len(), 2);
        assert!(pool.add_pairs(&[["11".into(), "99".into()]]).is_err());
        // No await before abort: these are ownership tests, not Internet tests.
        pool.stop().await;
        assert!(pool.tokens().is_empty());
        assert!(pool.jobs.is_empty());
    }
    #[tokio::test]
    async fn isolated_current_and_next_pairs_never_share_a_reconnect_boundary() {
        let output = output();
        let mut pool =
            Shards::new(PolymarketTransportConfig::production(), output, 6, 3, 128).unwrap();
        let ids = pool
            .add_pairs(&[
                ["current-up".into(), "current-down".into()],
                ["next-up".into(), "next-down".into()],
            ])
            .unwrap();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
        assert_eq!(pool.shards[&ids[0]].tokens.len(), 2);
        assert_eq!(pool.shards[&ids[1]].tokens.len(), 2);
        assert!(
            pool.shards[&ids[0]]
                .tokens
                .is_disjoint(&pool.shards[&ids[1]].tokens)
        );
        let before = pool.tokens();
        assert!(
            pool.add_pairs(&[
                ["third-up".into(), "third-down".into()],
                ["fourth-up".into(), "fourth-down".into(),]
            ])
            .is_err()
        );
        assert_eq!(pool.tokens(), before);
        assert_eq!(pool.jobs.len(), 2);
        pool.stop().await;
    }
    #[tokio::test]
    async fn retirement_counts_tokens_until_actual_success_and_failed_receipt_keeps_them() {
        let output = output();
        let mut pool =
            Shards::new(PolymarketTransportConfig::production(), output, 4, 2, 4).unwrap();
        let (commands, mut receiver) = subscription_commands();
        let (stop, _) = watch::channel(false);
        let original = BTreeSet::from(["11".into(), "12".into(), "21".into(), "22".into()]);
        pool.shards.insert(
            0,
            Shard {
                tokens: original.clone(),
                stop,
                commands,
                pending: None,
                stopping: false,
                retirement_target: None,
            },
        );
        let removed = BTreeSet::from(["11".into(), "12".into()]);
        assert_eq!(pool.management_statistics()["active_tokens"], 4);
        assert_eq!(pool.management_statistics()["draining_tokens"], 0);
        assert_eq!(pool.begin_retire(&removed).unwrap(), vec![0]);
        assert_eq!(pool.tokens(), original);
        assert_eq!(pool.management_statistics()["active_tokens"], 2);
        assert_eq!(pool.management_statistics()["draining_tokens"], 2);
        assert!(pool.begin_retire(&removed).unwrap_err().to_string()
            .contains("shard retirement in flight"));
        // A second request was rejected before any additional command or
        // ownership mutation; the first unanswered retirement remains intact.
        assert_eq!(pool.tokens(), original);
        assert_eq!(pool.management_statistics()["draining_tokens"], 2);
        assert_eq!(receiver.len(), 1);
        let command = receiver.try_recv().unwrap();
        assert_eq!(command.expected_tokens, original);
        command
            .receipt
            .send(Err(TransportError::SendFailed))
            .unwrap();
        assert!(pool.poll_retirements().is_err());
        assert_eq!(pool.tokens(), original);
        assert!(pool.begin_retire(&removed).is_err());
        pool.poll_retirements().unwrap();
        let command = receiver.try_recv().unwrap();
        command.receipt.send(Ok(())).unwrap();
        assert_eq!(pool.poll_retirements().unwrap(), vec![0]);
        assert_eq!(pool.tokens(), BTreeSet::from(["21".into(), "22".into()]));
        assert_eq!(pool.management_statistics()["active_tokens"], 2);
        assert_eq!(pool.management_statistics()["draining_tokens"], 0);
        pool.stop().await;
    }
    #[tokio::test]
    async fn terminal_retirement_is_idempotent_while_wire_receipt_is_pending() {
        let output = output();
        let mut pool =
            Shards::new(PolymarketTransportConfig::production(), output, 4, 2, 4).unwrap();
        let (commands, mut receiver) = subscription_commands();
        let (stop, _) = watch::channel(false);
        let original = BTreeSet::from(["11".into(), "12".into(), "21".into(), "22".into()]);
        pool.shards.insert(
            0,
            Shard {
                tokens: original.clone(),
                stop,
                commands,
                pending: None,
                stopping: false,
                retirement_target: None,
            },
        );
        let removed = BTreeSet::from(["11".into(), "12".into()]);
        assert!(!pool.request_retire(&removed).unwrap());
        // Reconciliation can run on every scheduler tick without replacing an
        // in-flight command or manufacturing an "unknown token" failure.
        assert!(!pool.request_retire(&removed).unwrap());
        let command = receiver.try_recv().unwrap();
        assert!(receiver.try_recv().is_err());
        command.receipt.send(Ok(())).unwrap();
        assert_eq!(pool.poll_retirements().unwrap(), vec![0]);
        assert!(pool.request_retire(&removed).unwrap());
        pool.stop().await;
    }
    #[tokio::test]
    async fn pair_rotation_removes_before_add_and_never_counts_eight_tokens() {
        let output = output();
        let mut pool =
            Shards::new(PolymarketTransportConfig::production(), output, 6, 2, 6).unwrap();
        let (commands, mut receiver) = subscription_commands();
        let (stop, _) = watch::channel(false);
        let original = BTreeSet::from([
            "retiring-up".into(),
            "retiring-down".into(),
            "current-up".into(),
            "current-down".into(),
            "next-up".into(),
            "next-down".into(),
        ]);
        pool.shards.insert(
            0,
            Shard {
                tokens: original.clone(),
                stop,
                commands,
                pending: None,
                stopping: false,
                retirement_target: None,
            },
        );
        let removed = ["retiring-up".into(), "retiring-down".into()];
        let added = ["new-next-up".into(), "new-next-down".into()];
        assert!(!pool.request_pair_replacement(&removed, &added).unwrap());
        assert_eq!(pool.tokens().len(), 6);
        let command = receiver.try_recv().unwrap();
        assert!(command.remove_first);
        assert_eq!(command.expected_tokens, original);
        assert_eq!(command.token_ids.len(), 6);
        assert!(command.token_ids.contains("new-next-up"));
        assert!(!command.token_ids.contains("retiring-up"));
        let expected = command.token_ids.clone();
        command.receipt.send(Ok(())).unwrap();
        pool.poll_retirements().unwrap();
        assert_eq!(pool.tokens(), expected);
        assert!(pool.request_pair_replacement(&removed, &added).unwrap());
        pool.stop().await;
    }
    #[tokio::test]
    async fn pair_rotation_accepts_lifecycle_pair_already_retired() {
        let output = output();
        let mut pool =
            Shards::new(PolymarketTransportConfig::production(), output, 4, 2, 4).unwrap();
        pool.add_pairs(&[["current-up".into(), "current-down".into()]])
            .unwrap();
        let removed = ["expired-up".into(), "expired-down".into()];
        let added = ["next-up".into(), "next-down".into()];
        assert!(pool.request_pair_replacement(&removed, &added).unwrap());
        assert_eq!(
            pool.tokens(),
            BTreeSet::from([
                "current-down".into(),
                "current-up".into(),
                "next-down".into(),
                "next-up".into(),
            ])
        );
        // A lost control reply is reconciled without opening another shard.
        assert!(pool.request_pair_replacement(&removed, &added).unwrap());
        assert_eq!(pool.tokens().len(), 4);
        pool.stop().await;
    }
    #[tokio::test]
    async fn ten_thousand_current_next_rotations_preserve_shared_lease_and_wire_budget() {
        // Exercise the same remove-first wire receipt -> lease CAS ordering as
        // the owner loop, without opening Internet sockets. This is not a
        // supplier transport or profile-publication endurance test.
        let output = output();
        let mut pool =
            Shards::new(PolymarketTransportConfig::production(), output, 6, 1, 6).unwrap();
        let (commands, mut receiver) = subscription_commands();
        let (stop, _) = watch::channel(false);
        let pair = |market: usize| [format!("{market}-up"), format!("{market}-down")];
        let mut current = 0usize;
        let mut next = 1usize;
        let stable = "shared".to_string();
        let mut tokens = BTreeSet::from(pair(current));
        tokens.extend(pair(next));
        tokens.extend(pair(10_002));
        pool.shards.insert(0, Shard {
            tokens: tokens.clone(), stop, commands, pending: None,
            stopping: false, retirement_target: None,
        });
        let mut eligible: BTreeSet<String> = (0..=10_001).map(|id| id.to_string()).collect();
        eligible.insert(stable.clone());
        let now = Instant::now();
        let ttl = Duration::from_secs(120);
        let mut leases = Leases::new(true, 2, ttl).unwrap();
        leases.acquire("btc5m".into(), ttl,
            BTreeSet::from([current.to_string(), next.to_string()]), &eligible, now).unwrap();
        leases.acquire("shared".into(), ttl,
            BTreeSet::from([stable.clone()]), &eligible, now).unwrap();
        for rotation in 0..10_000 {
            let added = rotation + 2;
            let expected = BTreeSet::from([current.to_string(), next.to_string()]);
            let desired = BTreeSet::from([next.to_string(), added.to_string()]);
            let union = leases.union_after_replacement("btc5m", &desired).unwrap();
            assert_eq!(union, BTreeSet::from([stable.clone(), next.to_string(), added.to_string()]));
            leases.validate_replace("btc5m", ttl, &expected, &desired, &eligible, now).unwrap();
            assert!(!pool.request_pair_replacement(&pair(current), &pair(added)).unwrap());
            // Until an acknowledged result arrives, the old six tokens still
            // consume the complete budget; no eighth-token transient exists.
            assert_eq!(pool.tokens().len(), 6);
            let command = receiver.try_recv().unwrap();
            assert!(command.remove_first);
            assert_eq!(command.expected_tokens, tokens);
            assert_eq!(command.token_ids.len(), 6);
            assert_eq!(pool.management_statistics()["draining_tokens"], 2);
            tokens = command.token_ids.clone();
            command.receipt.send(Ok(())).unwrap();
            assert_eq!(pool.poll_retirements().unwrap(), vec![0]);
            leases.replace("btc5m", ttl, &expected, desired, &eligible, now).unwrap();
            assert_eq!(pool.tokens(), tokens);
            assert_eq!(pool.tokens().len(), 6);
            assert_eq!(pool.management_statistics()["draining_tokens"], 0);
            assert_eq!(pool.shards.len(), 1);
            assert_eq!(leases.market_ids().len(), 3);
            current = next;
            next = added;
        }
        assert_eq!(leases.market_ids(), BTreeSet::from([
            stable, "10000".to_string(), "10001".to_string(),
        ]));
        // Process-local leases intentionally do not survive a fresh owner.
        assert!(Leases::new(true, 2, ttl).unwrap().market_ids().is_empty());
        pool.stop().await;
    }
}
pub struct Shards {
    config: PolymarketTransportConfig,
    output: TransportBudget,
    maximum_tokens: usize,
    maximum_shards: usize,
    shard_tokens: usize,
    next_id: u64,
    shards: BTreeMap<u64, Shard>,
    inputs: BTreeMap<u64, mpsc::Receiver<RawTransportBatch>>,
    ready: BTreeMap<u64, TransportReadyHandle>,
    ready_sender: mpsc::Sender<u64>,
    ready_receiver: mpsc::Receiver<u64>,
    jobs: tokio::task::JoinSet<(u64, ShardExit)>,
    failed: VecDeque<(BTreeSet<String>, &'static str)>,
}

pub enum AcquisitionEvent {
    Batch(RawTransportBatch),
    ShardFailed { tokens: BTreeSet<String>, reason: &'static str },
}
#[cfg(test)]
pub(crate) fn unanswered_retirement_fixture() -> (Shards, mpsc::Receiver<SubscriptionUpdate>) {
    // The real owner and receipt state machine are used; only the venue side
    // of the command channel is a fixture, so no external socket is opened.
    let output = marketcow_polymarket::transport_budget(1, 65_536, 65_536).unwrap();
    let mut pool = Shards::new(
        PolymarketTransportConfig::production(), output, 4, 1, 4,
    ).unwrap();
    let (commands, receiver) = subscription_commands();
    let (stop, _) = watch::channel(false);
    pool.shards.insert(0, Shard {
        tokens: BTreeSet::from(["11".into(), "12".into(), "21".into(), "22".into()]),
        stop, commands, pending: None, stopping: false, retirement_target: None,
    });
    (pool, receiver)
}
impl Shards {
    pub fn new(
        config: PolymarketTransportConfig,
        output: TransportBudget,
        maximum_tokens: usize,
        maximum_shards: usize,
        shard_tokens: usize,
    ) -> Result<Self> {
        ensure!(
            (2..=500).contains(&shard_tokens)
                && (2..=8192).contains(&maximum_tokens)
                && maximum_shards > 0
                && maximum_shards <= 1024,
            "explicit acquisition shard budgets"
        );
        let (ready_sender, ready_receiver) = mpsc::channel(maximum_shards);
        Ok(Self {
            config,
            output,
            maximum_tokens,
            maximum_shards,
            shard_tokens,
            next_id: 0,
            shards: BTreeMap::new(),
            inputs: BTreeMap::new(),
            ready: BTreeMap::new(),
            ready_sender,
            ready_receiver,
            jobs: tokio::task::JoinSet::new(),
            failed: VecDeque::new(),
        })
    }
    pub fn tokens(&self) -> BTreeSet<String> {
        self.shards
            .values()
            .flat_map(|s| s.tokens.iter().cloned())
            .collect()
    }
    pub fn statistics(&self) -> (usize, usize, usize) {
        (
            self.tokens().len(),
            self.jobs.len(),
            self.shards
                .values()
                .filter(|s| s.retirement_target.is_some() || s.stopping)
                .count(),
        )
    }
    /// Process-local phase view; a retiring token remains budgeted until the
    /// wire receipt completes. Token sets are deduplicated across shared jobs.
    pub fn management_statistics(&self) -> Value {
        let wire = self.tokens();
        let draining = self.draining_tokens();
        json!({"wire_tokens":wire.len(),"active_tokens":wire.len()-draining.len(),
            "draining_tokens":draining.len(),"token_limit":self.maximum_tokens,
            "socket_limit":self.maximum_shards})
    }
    pub fn draining_tokens(&self) -> BTreeSet<String> {
        self.shards.values().flat_map(|shard| {
            if shard.stopping {
                shard.tokens.clone()
            } else if let Some(target) = &shard.retirement_target {
                shard.tokens.difference(target).cloned().collect()
            } else {
                BTreeSet::new()
            }
        }).collect()
    }
    pub fn add_pairs(&mut self, pairs: &[[String; 2]]) -> Result<Vec<u64>> {
        self.reap()?;
        let batches = self.validate_add_pairs(pairs)?;
        ensure!(
            self.jobs.len() + batches.len() <= self.maximum_shards,
            "acquisition socket capacity"
        );
        self.next_id
            .checked_add(batches.len() as u64)
            .context("shard identity overflow")?;
        batches.into_iter().map(|tokens| self.spawn_owned_tokens(tokens, false)).collect()
    }

    fn spawn_owned_tokens(&mut self, tokens: BTreeSet<String>, recovering: bool) -> Result<u64> {
        ensure!(!tokens.is_empty() && tokens.len() <= self.shard_tokens,
            "acquisition socket membership capacity");
        ensure!(self.jobs.len() < self.maximum_shards, "acquisition socket capacity");
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1).context("shard identity overflow")?;
        let (stop, shutdown) = watch::channel(false);
        let (commands, updates) = subscription_commands();
        let config = self.config.clone();
        // Every market has an independent FIFO and socket backpressure.
        // The parent output supplies the shared absolute byte ceiling.
        let (output, input, ready) = self.output.market_lane(
            id, self.ready_sender.clone(), 2048, 32 * 1024 * 1024)?;
        let initial = tokens.iter().cloned().collect();
        self.jobs.spawn(async move {
            // A failed socket gets an independent bounded cooldown. Never
            // sleep the coordinator or retry in a tight loop when the shared
            // transport budget is temporarily full.
            if recovering { tokio::time::sleep(Duration::from_secs(1)).await; }
            let result = std::panic::AssertUnwindSafe(supervise_polymarket_transport_managed(
                config, initial, output, shutdown, updates, Duration::from_secs(1),
            )).catch_unwind().await;
            (id, match result {
                Ok(result) => ShardExit::Finished(result),
                Err(_) => ShardExit::Panicked,
            })
        });
        self.shards.insert(id, Shard {
            tokens, stop, commands, pending: None, stopping: false,
            retirement_target: None,
        });
        self.inputs.insert(id, input);
        self.ready.insert(id, ready);
        Ok(id)
    }
    pub fn validate_add_pairs(&self, pairs: &[[String; 2]]) -> Result<Vec<BTreeSet<String>>> {
        let mut tokens = self.tokens();
        let mut batches: Vec<BTreeSet<String>> = Vec::with_capacity(pairs.len());
        for pair in pairs {
            ensure!(
                pair.iter().all(|t| !t.is_empty() && t.len() <= 128) && pair[0] != pair[1],
                "market token pair"
            );
            for token in pair {
                ensure!(
                    tokens.insert(token.clone()),
                    "already-owned acquisition token"
                );
            }
            // One market pair owns one transport connection. The retired
            // market can reconnect or drain without resetting another pair.
            batches.push(pair.iter().cloned().collect());
        }
        ensure!(
            tokens.len() <= self.maximum_tokens,
            "acquisition token capacity"
        );
        // Retiring sockets count until their tasks have actually exited.
        ensure!(
            self.jobs.len() + batches.len() <= self.maximum_shards,
            "acquisition socket capacity"
        );
        self.next_id
            .checked_add(batches.len() as u64)
            .context("shard identity overflow")?;
        Ok(batches)
    }
    /// Fencing reducers/metadata is the caller's responsibility. This only
    /// schedules bounded wire changes and returns every receipt; it does not
    /// claim that an unsubscribe was acknowledged by the exchange.
    pub fn begin_retire(&mut self, removed: &BTreeSet<String>) -> Result<Vec<u64>> {
        ensure!(
            !removed.is_empty() && removed.is_subset(&self.tokens()),
            "unknown retirement token"
        );
        let mut changes = vec![];
        // Reserve all affected command slots before changing local ownership.
        for (id, shard) in &self.shards {
            let next: BTreeSet<_> = shard.tokens.difference(removed).cloned().collect();
            if next == shard.tokens {
                continue;
            }
            ensure!(
                shard.retirement_target.is_none() && !shard.stopping,
                "shard retirement in flight"
            );
            let permit = if next.is_empty() {
                None
            } else {
                Some(
                    shard
                        .commands
                        .clone()
                        .try_reserve_owned()
                        .context("shard update in flight")?,
                )
            };
            changes.push((*id, next, permit));
        }
        let mut receipts = vec![];
        for (id, next, permit) in changes {
            if let Some(permit) = permit {
                let shard = self.shards.get_mut(&id).unwrap();
                let (receipt, done) = oneshot::channel();
                permit.send(SubscriptionUpdate {
                    expected_tokens: shard.tokens.clone(),
                    token_ids: next.clone(),
                    remove_first: true,
                    receipt,
                });
                shard.retirement_target = Some(next.clone());
                shard.pending = Some((next, done));
            } else {
                let shard = self.shards.get_mut(&id).unwrap();
                shard.stopping = true;
                let _ = shard.stop.send(true);
            }
            receipts.push(id);
        }
        Ok(receipts)
    }
    /// Idempotent retirement reconciliation for lifecycle-terminal markets.
    ///
    /// A terminal fact fences the local reducer immediately, while the venue
    /// unsubscribe is an eventually acknowledged wire operation. Repeated
    /// scheduler ticks must therefore be able to observe an in-flight command
    /// without replacing it, and a completed command must make the same request
    /// a no-op. `true` means none of the requested tokens remain wire-owned.
    pub fn request_retire(&mut self, removed: &BTreeSet<String>) -> Result<bool> {
        ensure!(!removed.is_empty(), "empty retirement token set");
        let owned = self.tokens();
        let remaining: BTreeSet<_> = removed.intersection(&owned).cloned().collect();
        if remaining.is_empty() {
            return Ok(true);
        }
        if self.shards.values().any(|shard| {
            !shard.tokens.is_disjoint(&remaining)
                && (shard.retirement_target.is_some() || shard.stopping)
        }) {
            return Ok(false);
        }
        self.begin_retire(&remaining)?;
        Ok(false)
    }
    /// Capacity-preserving one-market rotation. The transport command is
    /// explicitly remove-first, so a fixed six-token profile never creates an
    /// eight-token transient union. `true` means the requested replacement is
    /// already reflected in acknowledged wire ownership.
    pub fn request_pair_replacement(
        &mut self,
        removed: &[String; 2],
        added: &[String; 2],
    ) -> Result<bool> {
        ensure!(
            removed[0] != removed[1]
                && added[0] != added[1]
                && removed.iter().all(|token| !token.is_empty())
                && added.iter().all(|token| !token.is_empty()),
            "replacement market token pair"
        );
        let added_pair = added.clone();
        let removed: BTreeSet<_> = removed.iter().cloned().collect();
        let added: BTreeSet<_> = added.iter().cloned().collect();
        let owned = self.tokens();
        if removed.is_disjoint(&owned) && added.is_subset(&owned) {
            return Ok(true);
        }
        // Lifecycle processing may have already retired the expired pair
        // before the UTC manager submits its lease/profile CAS. That is a
        // valid remove-first state, not a membership conflict. Add only the
        // reviewed next pair; add_pairs applies the same token/shard capacity
        // checks used by initial acquisition, and never resurrects expiry.
        if removed.is_disjoint(&owned) && added.is_disjoint(&owned) {
            self.add_pairs(&[added_pair])?;
            return Ok(true);
        }
        ensure!(
            removed.is_subset(&owned) && added.is_disjoint(&owned),
            "replacement membership differs"
        );
        if self.shards.values().any(|shard| {
            !shard.tokens.is_disjoint(&removed)
                && (shard.retirement_target.is_some() || shard.stopping)
        }) {
            return Ok(false);
        }
        let matching = self
            .shards
            .iter()
            .filter(|(_, shard)| removed.is_subset(&shard.tokens))
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        ensure!(
            matching.len() == 1,
            "replacement pair must be owned by one shard"
        );
        let id = matching[0];
        let shard = self.shards.get(&id).unwrap();
        let mut next: BTreeSet<_> = shard.tokens.difference(&removed).cloned().collect();
        next.extend(added);
        ensure!(
            next.len() <= self.shard_tokens
                && owned.len() - removed.len() + 2 <= self.maximum_tokens,
            "replacement acquisition capacity"
        );
        let permit = shard
            .commands
            .clone()
            .try_reserve_owned()
            .context("shard update in flight")?;
        let shard = self.shards.get_mut(&id).unwrap();
        let (receipt, done) = oneshot::channel();
        permit.send(SubscriptionUpdate {
            expected_tokens: shard.tokens.clone(),
            token_ids: next.clone(),
            remove_first: true,
            receipt,
        });
        shard.retirement_target = Some(next.clone());
        shard.pending = Some((next, done));
        Ok(false)
    }
    /// Only actual successful wire receipts release partial-shard ownership.
    /// Until then, the old token set continues counting against capacity.
    pub fn poll_retirements(&mut self) -> Result<Vec<u64>> {
        let mut completed = vec![];
        for (id, shard) in &mut self.shards {
            if shard.pending.is_none()
                && let Some(next) = &shard.retirement_target
            {
                let (receipt, done) = oneshot::channel();
                shard
                    .commands
                    .try_send(SubscriptionUpdate {
                        expected_tokens: shard.tokens.clone(),
                        token_ids: next.clone(),
                        remove_first: true,
                        receipt,
                    })
                    .map_err(|_| anyhow::anyhow!("retirement retry busy or stopped"))?;
                shard.pending = Some((next.clone(), done));
            }
            if let Some((next, receipt)) = &mut shard.pending {
                match receipt.try_recv() {
                    Ok(Ok(())) => {
                        shard.tokens = next.clone();
                        shard.pending = None;
                        shard.retirement_target = None;
                        completed.push(*id);
                    }
                    Ok(Err(error)) => {
                        shard.pending = None;
                        anyhow::bail!("shard unsubscribe failed: {error:?}");
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {}
                    Err(oneshot::error::TryRecvError::Closed) => {
                        shard.pending = None;
                        anyhow::bail!("shard unsubscribe receipt lost");
                    }
                }
            }
        }
        self.reap()?;
        Ok(completed)
    }
    fn reap(&mut self) -> Result<()> {
        while let Some(result) = self.jobs.try_join_next() {
            self.handle_finished(result)?;
        }
        self.purge_retired_notifications()?;
        Ok(())
    }
    pub fn reap_finished(&mut self) -> Result<()> { self.reap() }

    pub fn take_failure(&mut self) -> Option<(BTreeSet<String>, &'static str)> {
        self.failed.pop_front()
    }

    fn handle_finished(&mut self,
        result: Result<(u64, ShardExit), tokio::task::JoinError>,
    ) -> Result<()> {
        let (id, result) = result.context("acquisition socket task missing")?;
        let shard = self.shards.remove(&id).context("finished acquisition socket absent")?;
        // Dropping the old FIFO releases its permits. No pre-failure frame may
        // arrive after the market's new recovery barrier.
        self.inputs.remove(&id);
        self.ready.remove(&id);
        if shard.stopping { return Ok(()); }
        let tokens = shard.tokens;
        let reason = match result {
            ShardExit::Finished(Ok(())) => "socket_task_ended",
            ShardExit::Finished(Err(error)) => error.reason_code(),
            ShardExit::Panicked => "socket_task_panicked",
        };
        // The failed socket no longer owns a live subscription. Recreate only
        // its reviewed token set; another market's socket and actor stay put.
        // A pending remove-first command had an unknown receipt. The old
        // channel is gone, so the control owner retries its durable retirement
        // target against this fresh socket rather than assuming completion.
        let replacement = self.spawn_owned_tokens(tokens.clone(), true)?;
        if let Some(target) = shard.retirement_target {
            self.shards.get_mut(&replacement)
                .context("replacement acquisition socket absent")?
                .retirement_target = Some(target);
        }
        self.failed.push_back((tokens, reason));
        Ok(())
    }
    fn purge_retired_notifications(&mut self) -> Result<()> {
        let mut retained = Vec::new();
        while let Ok(id) = self.ready_receiver.try_recv() {
            if self.ready.contains_key(&id) { retained.push(id); }
        }
        for id in retained {
            self.ready_sender.try_send(id)
                .context("market-ready notification budget differs")?;
        }
        Ok(())
    }
    fn take_ready(&mut self, id: u64) -> Result<Option<RawTransportBatch>> {
        let Some(ready) = self.ready.get(&id) else { return Ok(None); };
        ready.consumed();
        let Some(input) = self.inputs.get_mut(&id) else { return Ok(None); };
        let batch = input.try_recv().ok();
        if !input.is_empty() { ready.signal()?; }
        Ok(batch)
    }
    pub fn try_next_batch(&mut self) -> Result<Option<RawTransportBatch>> {
        if !self.failed.is_empty() { return Ok(None); }
        while let Ok(id) = self.ready_receiver.try_recv() {
            if let Some(batch) = self.take_ready(id)? { return Ok(Some(batch)); }
        }
        Ok(None)
    }
    /// Consume edge-triggered market readiness in FIFO order without scanning
    /// every socket on each batch. A failed live socket remains fail-closed.
    pub async fn next_batch_or_failure(&mut self) -> Result<AcquisitionEvent> {
        loop {
            if let Some((tokens, reason)) = self.failed.pop_front() {
                return Ok(AcquisitionEvent::ShardFailed { tokens, reason });
            }
            let has_jobs = !self.jobs.is_empty();
            tokio::select! {
                id = self.ready_receiver.recv() => {
                    if let Some(id) = id
                        && let Some(batch) = self.take_ready(id)?
                    { return Ok(AcquisitionEvent::Batch(batch)); }
                },
                result = self.jobs.join_next(), if has_jobs => {
                    self.handle_finished(result.context("acquisition socket task missing")?)?;
                    self.purge_retired_notifications()?;
                }
            }
        }
    }
    pub async fn stop(&mut self) {
        for shard in self.shards.values() {
            let _ = shard.stop.send(true);
        }
        self.shards.clear();
        self.inputs.clear();
        self.ready.clear();
        self.failed.clear();
        self.jobs.abort_all();
        while self.jobs.join_next().await.is_some() {}
    }
}
