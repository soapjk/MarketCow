//! Same-release publication ownership. Never starts processes or reads disk.
//! Each HTTP request / WS pins one immutable scope. A retired scope has a hard
//! deadline even if a slow reader still owns its Arc; at most one is retained.
use anyhow::{ensure, Result};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;

pub struct ScopeLease<T> {
    pub id: String,
    pub revision: u64,
    pub value: T,
    retired: watch::Sender<Option<Instant>>,
}

impl<T> ScopeLease<T> {
    pub fn retirement(&self) -> watch::Receiver<Option<Instant>> {
        self.retired.subscribe()
    }
    pub fn expires(&self) -> Option<Instant> { *self.retired.borrow() }

    pub async fn expired(&self) {
        let mut retired=self.retirement();
        loop {
            let deadline=*retired.borrow_and_update();
            if let Some(deadline)=deadline {
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                return;
            }
            if retired.changed().await.is_err() {return;}
        }
    }
}

struct Scopes<T> {
    active: Arc<ScopeLease<T>>,
    previous: Option<Arc<ScopeLease<T>>>,
}

pub struct ScopeRegistry<T> {
    scopes: Mutex<Scopes<T>>,
    grace: Duration,
}

impl<T> ScopeRegistry<T> {
    pub fn new(id: String, revision: u64, value: T, grace: Duration) -> Result<Self> {
        ensure!(!id.is_empty() && revision > 0 && !grace.is_zero()
            && grace <= Duration::from_secs(300), "explicit scope identity/grace required");
        let (retired, _) = watch::channel(None);
        Ok(Self { scopes: Mutex::new(Scopes { active: Arc::new(ScopeLease {
            id, revision, value, retired,
        }), previous: None }), grace })
    }

    pub fn active(&self) -> Result<Arc<ScopeLease<T>>> {
        Ok(self.scopes.lock().map_err(|_| anyhow::anyhow!("scope registry poisoned"))?.active.clone())
    }
    pub fn readable(&self,now:Instant)->Result<Vec<Arc<ScopeLease<T>>>> {
        let mut state=self.scopes.lock().map_err(|_|anyhow::anyhow!("scope registry poisoned"))?;
        Self::reap(&mut state,now);
        let mut scopes=vec![state.active.clone()];
        scopes.extend(state.previous.iter().cloned());Ok(scopes)
    }

    pub fn get(&self, id: &str, now: Instant) -> Result<Arc<ScopeLease<T>>> {
        let mut state = self.scopes.lock().map_err(|_| anyhow::anyhow!("scope registry poisoned"))?;
        Self::reap(&mut state, now);
        if state.active.id == id { return Ok(state.active.clone()); }
        if let Some(old) = &state.previous {
            if old.id == id { return Ok(old.clone()); }
        }
        anyhow::bail!("scope unavailable; new full-sync required")
    }

    // All expensive validation and preparation must finish BEFORE this call.
    // Dropped candidates and stale requests never mutate the incumbent.
    pub fn check_activation(&self, expected_id:&str, expected_revision:u64, id:&str, now:Instant)->Result<()> {
        let mut state=self.scopes.lock().map_err(|_|anyhow::anyhow!("scope registry poisoned"))?;
        Self::reap(&mut state,now);
        ensure!(state.active.id==expected_id && state.active.revision==expected_revision,"scope revision conflict");
        ensure!(!id.is_empty() && id!=state.active.id,"unchanged/empty scope");
        ensure!(state.previous.is_none(),"scope retirement capacity");
        expected_revision.checked_add(1).ok_or_else(||anyhow::anyhow!("scope revision overflow"))?;
        now.checked_add(self.grace).ok_or_else(||anyhow::anyhow!("scope deadline overflow"))?;
        Ok(())
    }
    pub fn activate(&self, expected_id: &str, expected_revision: u64,
        id: String, value: T, now: Instant) -> Result<Arc<ScopeLease<T>>> {
        let mut state = self.scopes.lock().map_err(|_| anyhow::anyhow!("scope registry poisoned"))?;
        Self::reap(&mut state, now);
        ensure!(state.active.id == expected_id && state.active.revision == expected_revision,
            "scope revision conflict");
        ensure!(!id.is_empty() && id != state.active.id, "unchanged/empty scope");
        ensure!(state.previous.is_none(), "scope retirement capacity");
        let revision = expected_revision.checked_add(1).ok_or_else(||anyhow::anyhow!("scope revision overflow"))?;
        let expires = now.checked_add(self.grace).ok_or_else(||anyhow::anyhow!("scope deadline overflow"))?;
        let (retired, _) = watch::channel(None);
        let next = Arc::new(ScopeLease { id, revision, value, retired });
        let old = std::mem::replace(&mut state.active, next.clone());
        old.retired.send_replace(Some(expires));
        state.previous = Some(old);
        Ok(next)
    }

    fn reap(state: &mut Scopes<T>, now: Instant) {
        if state.previous.as_ref().is_some_and(|p|p.expires().is_some_and(|e|e <= now)) {
            state.previous = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cas_retains_pinned_state_then_expires_without_resetting_active() {
        let now = Instant::now();
        let registry = ScopeRegistry::new("A".into(), 1, 11, Duration::from_secs(2)).unwrap();
        let pinned = registry.active().unwrap();
        let mut notification = pinned.retirement();
        let b = registry.activate("A", 1, "B".into(), 22, now).unwrap();
        assert_eq!(b.revision, 2);
        assert!(notification.has_changed().unwrap());
        assert_eq!(*notification.borrow_and_update(), Some(now+Duration::from_secs(2)));
        assert_eq!(pinned.value, 11);
        assert_eq!(registry.get("A", now).unwrap().value, 11);
        assert!(registry.activate("A", 1, "C".into(), 33, now).is_err());
        assert!(registry.activate("B", 2, "C".into(), 33, now).is_err());
        assert!(registry.check_activation("B",2,"C",now).is_err());
        assert_eq!(registry.active().unwrap().id,"B");
        assert!(registry.check_activation("B",2,"C",now+Duration::from_secs(2)).is_ok());
        assert!(registry.get("A", now+Duration::from_secs(2)).is_err());
        let a = registry.activate("B", 2, "A".into(), 44, now+Duration::from_secs(2)).unwrap();
        assert_eq!(a.revision, 3); // A→B→A cannot accept the original revision.
        assert_eq!(pinned.value, 11); // Existing request never observes new A.
        assert_eq!(a.value, 44);
    }

    #[test]
    fn concurrent_candidates_have_one_winner() {
        let r = Arc::new(ScopeRegistry::new("A".into(), 1, 0, Duration::from_secs(1)).unwrap());
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let tasks: Vec<_> = ["B", "C"].into_iter().map(|id| {
            let r=r.clone(); let barrier=barrier.clone();
            std::thread::spawn(move || { barrier.wait(); r.activate("A",1,id.into(),1,Instant::now()).is_ok() })
        }).collect();
        barrier.wait();
        assert_eq!(tasks.into_iter().filter_map(|t|t.join().ok()).filter(|v|*v).count(),1);
    }
}
