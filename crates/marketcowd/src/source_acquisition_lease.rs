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
        self.leases.values().flat_map(|lease|lease.markets.iter().cloned()).collect()
    }
    pub fn acquire(
        &mut self,
        id: String,
        ttl: Duration,
        markets: BTreeSet<String>,
        eligible: &BTreeSet<String>,
        now: Instant,
    ) -> Result<()> {
        ensure!(self.required, "acquisition leases are not enabled");
        self.validate_ttl(ttl)?;
        ensure!(
            !markets.is_empty() && markets == *eligible,
            "lease must bind the complete eligible market set"
        );
        if let Some(existing) = self.leases.get_mut(&id) {
            ensure!(
                existing.markets == markets,
                "acquisition lease identity conflict"
            );
            existing.expires = now + ttl;
            return Ok(());
        }
        ensure!(
            self.leases.len() < self.maximum,
            "acquisition lease capacity exhausted"
        );
        self.leases.insert(
            id,
            Lease {
                markets,
                expires: now + ttl,
            },
        );
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
}
