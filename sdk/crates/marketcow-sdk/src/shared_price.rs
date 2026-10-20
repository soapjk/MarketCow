//! One source connection per Client, bounded independently for each consumer.
//! A slow consumer is closed explicitly; no silent drop of trade deltas.
use crate::{Client, Error, Result, btc, common};
use prost::Message;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, mpsc, watch};

// Price deltas are small and may arrive in a buffered TCP burst. Item capacity
// must not reject a healthy consumer before its Python/event-loop turn runs.
// This is only burst headroom: byte budgets and explicit overflow still apply.
const QUEUE_ITEMS: usize = 4096;
const QUEUE_BYTES: usize = 16 * 1024 * 1024;
const READY_FRAME_BATCH: usize = 32;

#[derive(Clone, Debug)]
pub enum PriceDelivery {
    Frame {
        frame: Arc<btc::PriceFrame>,
        available: bool,
    },
    Recovering {
        reason: Arc<str>,
        retry_after_ms: u64,
    },
}

struct Queued {
    delivery: PriceDelivery,
    bytes: usize,
    pending: Arc<AtomicUsize>,
    global: Arc<AtomicUsize>,
}
impl Drop for Queued {
    fn drop(&mut self) {
        self.pending.fetch_sub(self.bytes, Ordering::AcqRel);
        self.global.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct Consumer {
    tx: mpsc::Sender<Queued>,
    terminal: watch::Sender<Option<Arc<str>>>,
    pending: Arc<AtomicUsize>,
    global: Arc<AtomicUsize>,
}

struct HubState {
    consumers: BTreeMap<usize, Consumer>,
    next: usize,
    seed: Option<PriceDelivery>,
    stopped: bool,
}

pub(crate) struct PriceHub {
    state: Mutex<HubState>,
    changed: Notify,
    global: Arc<AtomicUsize>,
}

pub struct SharedPrices {
    hub: Arc<PriceHub>,
    id: usize,
    rx: mpsc::Receiver<Queued>,
    terminal: watch::Receiver<Option<Arc<str>>>,
    stopped: bool,
}

impl Consumer {
    fn send(&self, delivery: PriceDelivery, bytes: usize) -> bool {
        if self
            .global
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                pending
                    .checked_add(bytes)
                    .filter(|n| *n <= 64 * 1024 * 1024)
            })
            .is_err()
        {
            self.terminal
                .send_replace(Some(Arc::from("client_event_byte_budget")));
            return false;
        }
        if self
            .pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pending| {
                pending.checked_add(bytes).filter(|n| *n <= QUEUE_BYTES)
            })
            .is_err()
        {
            self.global.fetch_sub(bytes, Ordering::AcqRel);
            self.terminal
                .send_replace(Some(Arc::from("slow_consumer_byte_budget")));
            return false;
        }
        let queued = Queued {
            delivery,
            bytes,
            pending: self.pending.clone(),
            global: self.global.clone(),
        };
        if self.tx.try_send(queued).is_err() {
            self.terminal
                .send_replace(Some(Arc::from("slow_consumer_item_budget")));
            return false;
        }
        true
    }
}

impl PriceHub {
    fn attach(self: &Arc<Self>) -> Result<SharedPrices> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Protocol("price hub poisoned".into()))?;
        if state.stopped {
            return Err(Error::Cancelled);
        }
        if state.consumers.len() >= 128 {
            return Err(Error::Limit("source local consumers"));
        }
        let (tx, rx) = mpsc::channel(QUEUE_ITEMS);
        let (terminal, signal) = watch::channel(None);
        let consumer = Consumer {
            tx,
            terminal,
            pending: Arc::new(AtomicUsize::new(0)),
            global: self.global.clone(),
        };
        if let Some(seed) = &state.seed {
            let bytes = match seed {
                PriceDelivery::Frame { frame, .. } => frame.encoded_len(),
                _ => 128,
            };
            if !consumer.send(seed.clone(), bytes) {
                return Err(Error::Limit("price seed queue"));
            }
        }
        let id = state.next;
        state.next = state
            .next
            .checked_add(1)
            .ok_or(Error::Limit("subscription identity"))?;
        state.consumers.insert(id, consumer);
        Ok(SharedPrices {
            hub: self.clone(),
            id,
            rx,
            terminal: signal,
            stopped: false,
        })
    }

    fn broadcast(&self, event: PriceDelivery, seed: Option<PriceDelivery>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if let Some(seed) = seed {
            state.seed = Some(seed);
        }
        let bytes = match &event {
            PriceDelivery::Frame { frame, .. } => frame.encoded_len(),
            PriceDelivery::Recovering { reason, .. } => reason.len() + 32,
        };
        state.consumers.retain(|_, c| c.send(event.clone(), bytes));
        if state.consumers.is_empty() {
            self.changed.notify_one();
        }
    }

    fn count(&self) -> usize {
        self.state.lock().map_or(0, |mut s| {
            if s.consumers.is_empty() {
                s.stopped = true;
            }
            s.consumers.len()
        })
    }

    fn finish(&self, reason: Option<Arc<str>>) {
        if let Ok(mut state) = self.state.lock() {
            state.stopped = true;
            for consumer in state.consumers.values() {
                consumer.terminal.send_replace(reason.clone());
            }
            state.consumers.clear();
        }
    }
}

impl Client {
    /// Call inside an async runtime. Multiple callers share one owner/socket.
    /// Low-level subscribe_prices remains available for controlled diagnostics.
    pub fn shared_prices(&self, source: common::Source) -> Result<SharedPrices> {
        if self.is_closed() {
            return Err(Error::Cancelled);
        }
        crate::PriceState::new(source)?;
        let _runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            Error::Configuration("shared subscription requires Tokio runtime".into())
        })?;
        let mut hubs = self
            .0
            .price_hubs
            .lock()
            .map_err(|_| Error::Protocol("price registry poisoned".into()))?;
        if let Some(hub) = hubs.get(&(source as i32)).and_then(|w| w.upgrade()) {
            match hub.attach() {
                Ok(subscription) => return Ok(subscription),
                Err(Error::Cancelled) => {}
                Err(error) => return Err(error),
            }
        }
        let hub = Arc::new(PriceHub {
            state: Mutex::new(HubState {
                consumers: BTreeMap::new(),
                next: 1,
                seed: None,
                stopped: false,
            }),
            changed: Notify::new(),
            global: self.0.event_bytes.clone(),
        });
        let subscription = hub.attach()?;
        hubs.insert(source as i32, Arc::downgrade(&hub));
        self.spawn_tracked(run(self.clone(), source, hub), false)?;
        Ok(subscription)
    }
}

pub(crate) fn terminal(error: &Error) -> bool {
    match error {
        Error::Remote {
            status, retryable, ..
        } => matches!(status, 401 | 403 | 404 | 426 | 501) || !retryable,
        Error::WebSocket(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            matches!(
                response.status().as_u16(),
                400 | 401 | 403 | 404 | 426 | 501
            )
        }
        Error::Decode(_) => true,
        Error::WebSocket(tokio_tungstenite::tungstenite::Error::Capacity(_)) => true,
        Error::Protocol(message) => matches!(
            message.as_str(),
            "price schema/source/identity" | "market frame identity/sequence"
        ),
        Error::Configuration(_) => true,
        _ => false,
    }
}

async fn run(client: Client, source: common::Source, hub: Arc<PriceHub>) {
    let mut delay_ms = 250u64;
    'outer: loop {
        let wait_ms;
        if hub.count() == 0 || client.is_closed() {
            break;
        }
        let connect = client.subscribe_prices(source);
        let mut stream = tokio::select! {
            _ = client.wait_closed() => break,
            _ = hub.changed.notified() => { continue; }
            result = connect => match result {
                Ok(stream) => stream,
                Err(error) => {
                    if terminal(&error) { hub.finish(Some(Arc::from(error.to_string()))); return; }
                    wait_ms = crate::retry::delay_ms(delay_ms, Some(&error));
                    hub.broadcast(PriceDelivery::Recovering { reason: Arc::from(error.to_string()), retry_after_ms: wait_ms }, None);
                    tokio::select! { _ = client.wait_closed() => break,
                        _ = hub.changed.notified() => {}, _ = tokio::time::sleep(Duration::from_millis(wait_ms)) => {} }
                    delay_ms = (delay_ms * 2).min(30_000);
                    continue;
                }
            },
        };
        let mut healthy_since = None;
        let mut ready_frames = 0usize;
        loop {
            if hub.count() == 0 {
                break 'outer;
            }
            let next = tokio::select! {
                _ = client.wait_closed() => break 'outer,
                _ = hub.changed.notified() => { continue; },
                result = stream.next() => result,
            };
            match next {
                Ok(Some(frame)) => {
                    // New consumers atomically join at the current serial with
                    // a one-point real snapshot, not the entire historic prefix.
                    let mut seed = frame.clone();
                    if stream.state().available {
                        seed.payload = Some(btc::price_frame::Payload::Snapshot(btc::Snapshot {
                            trades: stream.state().latest_trade.iter().cloned().collect(),
                            observations: stream
                                .state()
                                .latest_observation
                                .iter()
                                .cloned()
                                .collect(),
                            first_trade_id: stream
                                .state()
                                .latest_trade
                                .as_ref()
                                .map(|t| t.trade_id),
                            last_trade_id: stream.state().latest_trade.as_ref().map(|t| t.trade_id),
                            truncated: true,
                            retained_count: 1,
                        }));
                        let since = healthy_since.get_or_insert_with(tokio::time::Instant::now);
                        if since.elapsed() >= Duration::from_secs(10) {
                            delay_ms = 250;
                        }
                    } else {
                        healthy_since = None;
                    }
                    let available = stream.state().available;
                    hub.broadcast(
                        PriceDelivery::Frame {
                            frame: Arc::new(frame),
                            available,
                        },
                        Some(PriceDelivery::Frame {
                            frame: Arc::new(seed),
                            available,
                        }),
                    );
                    ready_frames += 1;
                    if ready_frames == READY_FRAME_BATCH {
                        // No timer, batching delay, loss, or waiting on a slow
                        // consumer. Give other ready tasks a scheduling turn
                        // while a socket has many immediately available frames.
                        tokio::task::yield_now().await;
                        ready_frames = 0;
                    }
                }
                Ok(None) => break 'outer,
                Err(error) => {
                    if terminal(&error) {
                        hub.finish(Some(Arc::from(error.to_string())));
                        return;
                    }
                    // Never seed a newly joining consumer from the old live
                    // snapshot while the transport is recovering.
                    if let Ok(mut state) = hub.state.lock() {
                        state.seed = None;
                    }
                    wait_ms = crate::retry::delay_ms(delay_ms, Some(&error));
                    hub.broadcast(
                        PriceDelivery::Recovering {
                            reason: Arc::from(error.to_string()),
                            retry_after_ms: wait_ms,
                        },
                        None,
                    );
                    break;
                }
            }
        }
        tokio::select! { _ = client.wait_closed() => break,
        _ = hub.changed.notified() => {}, _ = tokio::time::sleep(Duration::from_millis(wait_ms)) => {} }
        delay_ms = (delay_ms * 2).min(30_000);
    }
    hub.finish(None);
}

impl SharedPrices {
    pub async fn next(&mut self) -> Result<Option<PriceDelivery>> {
        if self.stopped {
            return Ok(None);
        }
        let terminal = self.terminal.borrow().clone();
        if let Some(reason) = terminal {
            self.close();
            return Err(Error::Protocol(reason.to_string()));
        }
        tokio::select! {
            biased;
            _ = self.terminal.changed() => {
                let reason = self.terminal.borrow().clone();
                self.close();
                match reason { Some(reason) => Err(Error::Protocol(reason.to_string())), None => Ok(None) }
            }
            value = self.rx.recv() => Ok(value.map(|queued| queued.delivery.clone())),
        }
    }
    pub fn close(&mut self) {
        self.stopped = true;
        if let Ok(mut state) = self.hub.state.lock() {
            state.consumers.remove(&self.id);
        }
        self.rx.close();
        while self.rx.try_recv().is_ok() {}
        self.hub.changed.notify_one();
    }
}
impl Drop for SharedPrices {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hub() -> Arc<PriceHub> {
        Arc::new(PriceHub {
            state: Mutex::new(HubState {
                consumers: BTreeMap::new(),
                next: 1,
                seed: None,
                stopped: false,
            }),
            changed: Notify::new(),
            global: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn frame(sequence: u64) -> PriceDelivery {
        PriceDelivery::Frame {
            frame: Arc::new(btc::PriceFrame {
                stream_sequence: sequence,
                ..Default::default()
            }),
            available: true,
        }
    }

    #[tokio::test]
    async fn joining_consumer_gets_seed_before_next_update() {
        let hub = hub();
        let _first = hub.attach().unwrap();
        hub.broadcast(frame(10), Some(frame(10)));
        let mut second = hub.attach().unwrap();
        hub.broadcast(frame(11), Some(frame(11)));
        let sequence = |event: PriceDelivery| match event {
            PriceDelivery::Frame { frame, .. } => frame.stream_sequence,
            _ => panic!("expected frame"),
        };
        assert_eq!(sequence(second.next().await.unwrap().unwrap()), 10);
        assert_eq!(sequence(second.next().await.unwrap().unwrap()), 11);
    }

    #[tokio::test]
    async fn slow_consumer_is_explicit_and_does_not_stop_fast_consumer() {
        let hub = hub();
        let mut slow = hub.attach().unwrap();
        let mut fast = hub.attach().unwrap();
        for serial in 1..=QUEUE_ITEMS as u64 + 1 {
            hub.broadcast(frame(serial), Some(frame(serial)));
            assert!(fast.next().await.unwrap().is_some());
        }
        assert!(slow.next().await.is_err());
        assert_eq!(hub.count(), 1);
        fast.close();
        assert_eq!(hub.global.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn buffered_burst_survives_consumer_scheduling_gap_without_dropping_frames() {
        let hub = hub();
        let mut consumer = hub.attach().unwrap();
        // No consumer poll during the burst: the former 64-slot queue failed
        // before a foreign-runtime consumer could resume its next() call.
        for serial in 1..=512 {
            hub.broadcast(frame(serial), Some(frame(serial)));
        }
        for expected in 1..=512 {
            match consumer.next().await.unwrap().unwrap() {
                PriceDelivery::Frame { frame, .. } => assert_eq!(frame.stream_sequence, expected),
                _ => panic!("burst must retain every frame"),
            }
        }
        assert_eq!(hub.global.load(Ordering::Acquire), 0);
        consumer.close();
        assert_eq!(hub.count(), 0);
    }

    #[tokio::test]
    async fn larger_item_budget_never_relaxes_byte_budget() {
        let hub = hub();
        let mut consumer = hub.attach().unwrap();
        let mut large = btc::PriceFrame::default();
        large.instrument_id = "x".repeat(QUEUE_BYTES + 1);
        hub.broadcast(PriceDelivery::Frame { frame: Arc::new(large), available: true }, None);
        let error = consumer.next().await.unwrap_err();
        assert!(error.to_string().contains("slow_consumer_byte_budget"));
        assert_eq!(hub.global.load(Ordering::Acquire), 0);
    }

    #[test]
    fn consumer_limit_cannot_be_bypassed_by_creating_another_owner() {
        let hub = hub();
        let consumers: Vec<_> = (0..128).map(|_| hub.attach().unwrap()).collect();
        assert!(matches!(hub.attach(), Err(Error::Limit(_))));
        drop(consumers);
        assert_eq!(hub.count(), 0);
        assert!(matches!(hub.attach(), Err(Error::Cancelled)));
    }
}
