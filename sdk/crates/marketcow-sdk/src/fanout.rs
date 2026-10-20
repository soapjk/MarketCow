//! Bounded local fanout; terminal errors cannot be hidden behind a full queue.
use crate::{Error, Result};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Notify, mpsc, watch};

struct Queued<T> {
    value: T,
    bytes: usize,
    local: Arc<AtomicUsize>,
    global: Arc<AtomicUsize>,
}
impl<T> Drop for Queued<T> {
    fn drop(&mut self) {
        self.local.fetch_sub(self.bytes, Ordering::AcqRel);
        self.global.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
struct Consumer<T> {
    tx: mpsc::Sender<Queued<T>>,
    terminal: watch::Sender<Option<Arc<str>>>,
    bytes: Arc<AtomicUsize>,
}
struct State<T> {
    consumers: BTreeMap<u64, Consumer<T>>,
    next: u64,
    stopped: bool,
}
pub(crate) struct Fanout<T> {
    state: Mutex<State<T>>,
    global: Arc<AtomicUsize>,
    pub changed: Notify,
}
pub(crate) struct Receiver<T> {
    hub: Arc<Fanout<T>>,
    id: u64,
    rx: mpsc::Receiver<Queued<T>>,
    terminal: watch::Receiver<Option<Arc<str>>>,
    closed: bool,
}

impl<T: Clone> Fanout<T> {
    pub fn new(global: Arc<AtomicUsize>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                consumers: BTreeMap::new(),
                next: 1,
                stopped: false,
            }),
            global,
            changed: Notify::new(),
        })
    }
    fn enqueue(&self, consumer: &Consumer<T>, value: T, bytes: usize) -> bool {
        if self
            .global
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(bytes).filter(|n| *n <= 64 * 1024 * 1024)
            })
            .is_err()
        {
            consumer
                .terminal
                .send_replace(Some(Arc::from("client_event_byte_budget")));
            return false;
        }
        if consumer
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(bytes).filter(|n| *n <= 16 * 1024 * 1024)
            })
            .is_err()
        {
            self.global.fetch_sub(bytes, Ordering::AcqRel);
            consumer
                .terminal
                .send_replace(Some(Arc::from("slow_consumer_byte_budget")));
            return false;
        }
        let queued = Queued {
            value,
            bytes,
            local: consumer.bytes.clone(),
            global: self.global.clone(),
        };
        if consumer.tx.try_send(queued).is_err() {
            consumer
                .terminal
                .send_replace(Some(Arc::from("slow_consumer_item_budget")));
            return false;
        }
        true
    }
    pub fn attach(
        self: &Arc<Self>,
        seed: impl FnOnce() -> Result<Option<(T, usize)>>,
    ) -> Result<Receiver<T>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Protocol("fanout poisoned".into()))?;
        if state.stopped {
            return Err(Error::Cancelled);
        }
        if state.consumers.len() >= 128 {
            return Err(Error::Limit("market local consumers"));
        }
        let (tx, rx) = mpsc::channel(64);
        let (terminal, signal) = watch::channel(None);
        let consumer = Consumer {
            tx,
            terminal,
            bytes: Arc::new(AtomicUsize::new(0)),
        };
        if let Some((seed, bytes)) = seed()? {
            if !self.enqueue(&consumer, seed, bytes) {
                return Err(Error::Limit("market seed queue"));
            }
        }
        let id = state.next;
        state.next = state
            .next
            .checked_add(1)
            .ok_or(Error::Limit("subscriber identity"))?;
        state.consumers.insert(id, consumer);
        Ok(Receiver {
            hub: self.clone(),
            id,
            rx,
            terminal: signal,
            closed: false,
        })
    }
    pub fn send(&self, value: T, bytes: usize) {
        if let Ok(mut state) = self.state.lock() {
            state
                .consumers
                .retain(|_, c| self.enqueue(c, value.clone(), bytes));
            if state.consumers.is_empty() {
                self.changed.notify_one();
            }
        }
    }
    pub fn active(&self) -> bool {
        self.state.lock().is_ok_and(|mut s| {
            if s.consumers.is_empty() {
                s.stopped = true;
            }
            !s.stopped
        })
    }
    pub fn finish(&self, reason: Option<Arc<str>>) {
        if let Ok(mut s) = self.state.lock() {
            s.stopped = true;
            for c in s.consumers.values() {
                c.terminal.send_replace(reason.clone());
            }
            s.consumers.clear();
        }
    }
}
impl<T: Clone> Receiver<T> {
    pub async fn next(&mut self) -> Result<Option<T>> {
        if self.closed {
            return Ok(None);
        }
        let error = self.terminal.borrow().clone();
        if let Some(error) = error {
            self.close();
            return Err(Error::Protocol(error.to_string()));
        }
        tokio::select! { biased;
            _ = self.terminal.changed() => {
                let error = self.terminal.borrow().clone(); self.close();
                match error { Some(e) => Err(Error::Protocol(e.to_string())), None => Ok(None) }
            }
            next = self.rx.recv() => Ok(next.map(|q| q.value.clone())),
        }
    }
}
impl<T> Receiver<T> {
    pub fn close(&mut self) {
        self.closed = true;
        if let Ok(mut state) = self.hub.state.lock() {
            state.consumers.remove(&self.id);
        }
        self.rx.close();
        while self.rx.try_recv().is_ok() {}
        self.hub.changed.notify_one();
    }
}
impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn slow_receiver_isolated_and_byte_credits_returned() {
        let bytes = Arc::new(AtomicUsize::new(0));
        let fanout = Fanout::new(bytes.clone());
        let mut slow = fanout.attach(|| Ok(None)).unwrap();
        let mut fast = fanout.attach(|| Ok(Some((0u64, 8)))).unwrap();
        assert_eq!(fast.next().await.unwrap(), Some(0));
        for n in 1..=65 {
            fanout.send(n, 8);
            assert_eq!(fast.next().await.unwrap(), Some(n));
        }
        assert!(slow.next().await.is_err());
        fast.close();
        assert_eq!(bytes.load(Ordering::Acquire), 0);
        assert!(!fanout.active());
    }
}
