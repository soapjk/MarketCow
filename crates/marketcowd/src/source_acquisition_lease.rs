//! Process-local demand leases for expensive upstream acquisition.
//!
//! Leases deliberately do not survive a process restart: a dead consumer must
//! never cause the collector to resume paid/network-heavy acquisition. The
//! active scope and durable market facts remain owned by their existing stores.
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

struct Lease {
    markets: BTreeSet<String>,
    expires: Instant,
}

pub struct Leases {
    required: bool,
    maximum: usize,
    maximum_ttl: Duration,
    leases: BTreeMap<String, Lease>,
}

impl Leases {
    pub fn new(required: bool, maximum: usize, maximum_ttl: Duration) -> Result<Self> {
        ensure!(
            !required || (1..=64).contains(&maximum),
            "acquisition lease capacity"
        );
        ensure!(
            !required || (!maximum_ttl.is_zero() && maximum_ttl <= Duration::from_secs(3600)),
            "acquisition lease TTL"
        );
        Ok(Self {
            required,
            maximum,
            maximum_ttl,
            leases: BTreeMap::new(),
        })
    }
    pub fn required(&self) -> bool {
        self.required
    }
    pub fn enabled(&self) -> bool {
        !self.required || !self.leases.is_empty()
    }
    pub fn market_ids(&self) -> BTreeSet<String> {
        self.leases
            .values()
            .flat_map(|lease| lease.markets.iter().cloned())
            .collect()
    }
    pub fn lease_market_ids(&self, id: &str, now: Instant) -> Result<BTreeSet<String>> {
        let lease = self
            .leases
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("acquisition lease not found"))?;
        ensure!(lease.expires > now, "acquisition lease expired");
        Ok(lease.markets.clone())
    }
    pub fn acquire(
        &mut self,
        id: String,
        ttl: Duration,
        markets: BTreeSet<String>,
        eligible: &BTreeSet<String>,
        now: Instant,
    ) -> Result<()> {
        self.validate_acquire(&id, ttl, &markets, eligible, now)?;
        if let Some(existing) = self.leases.get_mut(&id) {
            existing.expires = now + ttl;
            return Ok(());
        }
        self.leases.insert(
            id,
            Lease {
                markets,
                expires: now + ttl,
            },
        );
        Ok(())
    }
    pub fn validate_acquire(
        &self,
        id: &str,
        ttl: Duration,
        markets: &BTreeSet<String>,
        eligible: &BTreeSet<String>,
        now: Instant,
    ) -> Result<()> {
        ensure!(self.required, "acquisition leases are not enabled");
        self.validate_ttl(ttl)?;
        ensure!(
            !markets.is_empty() && markets.is_subset(eligible),
            "lease markets must be a nonempty eligible subset"
        );
        if let Some(existing) = self.leases.get(id) {
            ensure!(existing.expires > now, "acquisition lease expired");
            ensure!(
                existing.markets == *markets,
                "acquisition lease identity conflict"
            );
        } else {
            ensure!(
                self.leases.len() < self.maximum,
                "acquisition lease capacity exhausted"
            );
        }
        Ok(())
    }
    pub fn renew(&mut self, id: &str, ttl: Duration, now: Instant) -> Result<()> {
        ensure!(self.required, "acquisition leases are not enabled");
        self.validate_ttl(ttl)?;
        let lease = self
            .leases
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("acquisition lease not found"))?;
        ensure!(lease.expires > now, "acquisition lease expired");
        lease.expires = now + ttl;
        Ok(())
    }
    /// Atomically change one component lease after the shared wire union has
    /// been reconciled. The expected set is a CAS fence; other component
    /// leases remain untouched and overlapping markets stay subscribed.
    pub fn replace(
        &mut self,
        id: &str,
        ttl: Duration,
        expected_markets: &BTreeSet<String>,
        markets: BTreeSet<String>,
        eligible: &BTreeSet<String>,
        now: Instant,
    ) -> Result<()> {
        self.validate_replace(id, ttl, expected_markets, &markets, eligible, now)?;
        let lease = self
            .leases
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("acquisition lease not found"))?;
        lease.markets = markets;
        lease.expires = now + ttl;
        Ok(())
    }
    pub fn validate_replace(
        &self,
        id: &str,
        ttl: Duration,
        expected_markets: &BTreeSet<String>,
        markets: &BTreeSet<String>,
        eligible: &BTreeSet<String>,
        now: Instant,
    ) -> Result<()> {
        ensure!(self.required, "acquisition leases are not enabled");
        self.validate_ttl(ttl)?;
        ensure!(
            !markets.is_empty() && markets.is_subset(eligible),
            "replacement lease markets must be an eligible subset"
        );
        let lease = self
            .leases
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("acquisition lease not found"))?;
        ensure!(lease.expires > now, "acquisition lease expired");
        ensure!(
            lease.markets == *expected_markets,
            "acquisition lease replacement incumbent conflict"
        );
        Ok(())
    }
    pub fn union_after_replacement(
        &self,
        id: &str,
        markets: &BTreeSet<String>,
    ) -> Result<BTreeSet<String>> {
        ensure!(self.leases.contains_key(id), "acquisition lease not found");
        Ok(self
            .leases
            .iter()
            .flat_map(|(lease_id, lease)| {
                if lease_id == id {
                    markets.iter()
                } else {
                    lease.markets.iter()
                }
            })
            .cloned()
            .collect())
    }
    pub fn union_after_release(&self, id: &str) -> Result<BTreeSet<String>> {
        ensure!(self.leases.contains_key(id), "acquisition lease not found");
        Ok(self
            .leases
            .iter()
            .filter(|(lease_id, _)| lease_id.as_str() != id)
            .flat_map(|(_, lease)| lease.markets.iter().cloned())
            .collect())
    }
    pub fn release(&mut self, id: &str) -> Result<()> {
        ensure!(self.required, "acquisition leases are not enabled");
        ensure!(
            self.leases.remove(id).is_some(),
            "acquisition lease not found"
        );
        Ok(())
    }
    pub fn expire(&mut self, now: Instant) -> usize {
        let before = self.leases.len();
        self.leases.retain(|_, lease| lease.expires > now);
        before - self.leases.len()
    }
    pub fn status(&self, eligible: &BTreeSet<String>, now: Instant) -> Value {
        json!({"schema_version":"marketcow.acquisition-lease-status.v1",
            "required":self.required,"acquisition_enabled":self.enabled(),"active_leases":self.leases.len(),
            "maximum_leases":self.maximum,"maximum_ttl_seconds":self.maximum_ttl.as_secs(),
            "eligible_market_ids":eligible,"leases":self.leases.iter().map(|(id,lease)|json!({
                "lease_id":id,"market_count":lease.markets.len(),
                "market_ids":lease.markets,
                "remaining_milliseconds":lease.expires.saturating_duration_since(now).as_millis() as u64
            })).collect::<Vec<_>>()})
    }
    fn validate_ttl(&self, ttl: Duration) -> Result<()> {
        ensure!(
            !ttl.is_zero() && ttl <= self.maximum_ttl,
            "acquisition lease TTL outside profile"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn leases_are_bounded_idempotent_and_expire() {
        let now = Instant::now();
        let markets = BTreeSet::from(["1".into(), "2".into()]);
        let mut leases = Leases::new(true, 2, Duration::from_secs(30)).unwrap();
        leases
            .acquire(
                "a".into(),
                Duration::from_secs(10),
                markets.clone(),
                &markets,
                now,
            )
            .unwrap();
        leases
            .acquire(
                "a".into(),
                Duration::from_secs(20),
                markets.clone(),
                &markets,
                now,
            )
            .unwrap();
        assert_eq!(leases.status(&markets, now)["active_leases"], 1);
        assert!(
            leases
                .acquire(
                    "a".into(),
                    Duration::from_secs(1),
                    BTreeSet::from(["1".into()]),
                    &markets,
                    now
                )
                .is_err()
        );
        leases.renew("a", Duration::from_secs(2), now).unwrap();
        assert_eq!(leases.expire(now + Duration::from_secs(3)), 1);
        assert!(!leases.enabled());
    }
    #[test]
    fn legacy_mode_is_always_enabled_and_rejects_commands() {
        let mut leases = Leases::new(false, 0, Duration::ZERO).unwrap();
        assert!(leases.enabled());
        assert!(leases.release("missing").is_err());
    }
    #[test]
    fn profile_rotation_replaces_the_complete_union_without_a_fourth_market() {
        let now = Instant::now();
        let old = BTreeSet::from(["retiring".into(), "current".into(), "next".into()]);
        let new = BTreeSet::from(["current".into(), "next".into(), "new-next".into()]);
        let mut leases = Leases::new(true, 1, Duration::from_secs(120)).unwrap();
        leases
            .acquire(
                "btc5m-manager".into(),
                Duration::from_secs(120),
                old.clone(),
                &old,
                now,
            )
            .unwrap();
        leases
            .replace(
                "btc5m-manager",
                Duration::from_secs(120),
                &old,
                new.clone(),
                &new,
                now,
            )
            .unwrap();
        assert_eq!(leases.market_ids(), new);
        assert_eq!(leases.status(&new, now)["active_leases"], 1);
        assert!(
            leases
                .replace(
                    "btc5m-manager",
                    Duration::from_secs(120),
                    &old,
                    old.clone(),
                    &old,
                    now
                )
                .is_err()
        );
    }
    #[test]
    fn component_leases_form_a_deduplicated_union() {
        let now = Instant::now();
        let eligible = BTreeSet::from([
            "base-1".into(),
            "base-2".into(),
            "btc-current".into(),
            "btc-next".into(),
            "btc-new-next".into(),
        ]);
        let mut leases = Leases::new(true, 4, Duration::from_secs(120)).unwrap();
        leases
            .acquire(
                "paper".into(),
                Duration::from_secs(120),
                BTreeSet::from(["base-1".into(), "base-2".into()]),
                &eligible,
                now,
            )
            .unwrap();
        let btc = BTreeSet::from(["btc-current".into(), "btc-next".into()]);
        leases
            .acquire(
                "btc".into(),
                Duration::from_secs(120),
                btc.clone(),
                &eligible,
                now,
            )
            .unwrap();
        assert_eq!(leases.market_ids().len(), 4);
        let next = BTreeSet::from(["btc-next".into(), "btc-new-next".into()]);
        leases
            .validate_replace("btc", Duration::from_secs(120), &btc, &next, &eligible, now)
            .unwrap();
        assert_eq!(
            leases.union_after_replacement("btc", &next).unwrap().len(),
            4
        );
        leases
            .replace("btc", Duration::from_secs(120), &btc, next, &eligible, now)
            .unwrap();
        assert_eq!(leases.union_after_release("paper").unwrap().len(), 2);
    }
    #[test]
    fn freshness_scope_is_exactly_the_live_component_lease() {
        let now = Instant::now();
        let eligible = BTreeSet::from(["current".into(), "next".into(), "unrelated".into()]);
        let owned = BTreeSet::from(["current".into(), "next".into()]);
        let mut leases = Leases::new(true, 2, Duration::from_secs(120)).unwrap();
        leases
            .acquire(
                "btc5m-manager".into(),
                Duration::from_secs(120),
                owned.clone(),
                &eligible,
                now,
            )
            .unwrap();
        assert_eq!(
            leases.lease_market_ids("btc5m-manager", now).unwrap(),
            owned
        );
        assert!(
            leases
                .lease_market_ids("btc5m-manager", now + Duration::from_secs(121))
                .is_err()
        );
        assert!(leases.lease_market_ids("other", now).is_err());
    }
    #[test]
    fn ten_thousand_rotations_do_not_accumulate_component_demand() {
        let start = Instant::now();
        let ttl = Duration::from_secs(120);
        let mut leases = Leases::new(true, 2, ttl).unwrap();
        let mut btc = BTreeSet::from(["btc-0".to_owned(), "btc-1".to_owned()]);
        let shared = BTreeSet::from(["btc-0".to_owned()]);
        leases.acquire("btc".into(), ttl, btc.clone(), &btc, start).unwrap();
        leases.acquire("shared".into(), ttl, shared.clone(), &btc, start).unwrap();
        for index in 1..=10_000 {
            let now = start + Duration::from_secs(index);
            leases.renew("shared", ttl, now).unwrap();
            let next = BTreeSet::from([
                format!("btc-{index}"), format!("btc-{}", index + 1),
            ]);
            let eligible = btc.union(&next).chain(shared.iter()).cloned().collect();
            leases.replace("btc", ttl, &btc, next.clone(), &eligible, now).unwrap();
            assert_eq!(leases.market_ids().len(), 3);
            assert_eq!(leases.union_after_release("shared").unwrap(), next);
            btc = next;
        }
        leases.release("shared").unwrap();
        assert_eq!(leases.market_ids().len(), 2);
        // A process restart starts with no lease objects, regardless of the
        // previous process's historical admitted market identities.
        let restarted = Leases::new(true, 2, ttl).unwrap();
        assert!(!restarted.enabled());
        assert!(restarted.market_ids().is_empty());
    }
}
