use crate::{
    Client, Error, Result,
    book::MarketState,
    fanout::{Fanout, Receiver},
    follow_market::MarketDelivery,
    market,
    market_stream::StateHandle,
};
use prost::Message;
use std::{
    sync::{
        Arc, RwLock, RwLockReadGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub(crate) struct MarketHub {
    channel: Arc<Fanout<MarketDelivery>>,
    state: StateHandle,
    connected: AtomicBool,
    binding: market::MarketBinding,
    budget: market::DeliveryBudget,
}
pub struct SharedMarket {
    hub: Arc<MarketHub>,
    rx: Receiver<MarketDelivery>,
    seed_sequence: Option<u64>,
    seed_pending: bool,
}
impl MarketHub {
    fn attach(self: &Arc<Self>) -> Result<SharedMarket> {
        let mut sequence = None;
        let rx = self.channel.attach(|| {
            if !self.connected.load(Ordering::Acquire) {
                return Ok(None);
            }
            let state = self
                .state
                .read()
                .map_err(|_| Error::Protocol("market state poisoned".into()))?;
            let Some(frame) = state.snapshot() else {
                return Ok(None);
            };
            sequence = Some(frame.state_sequence);
            let bytes = frame.encoded_len();
            Ok(Some((
                MarketDelivery::Frame(Arc::new(frame), Arc::new(state.top())),
                bytes + 512,
            )))
        })?;
        Ok(SharedMarket {
            hub: self.clone(),
            rx,
            seed_sequence: sequence,
            seed_pending: sequence.is_some(),
        })
    }
}
impl Client {
    /// Same immutable key+stream shares one decoder and book owner. Role alone
    /// does not split the owner. No unbounded weak registry across window rolls.
    pub fn shared_market(
        &self,
        binding: market::MarketBinding,
        budget: market::DeliveryBudget,
    ) -> Result<SharedMarket> {
        if self.is_closed() {
            return Err(Error::Cancelled);
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Configuration("subscription requires Tokio runtime".into()))?;
        let key = binding
            .key
            .as_ref()
            .ok_or_else(|| Error::Protocol("market key required".into()))?;
        let registry_key = (key.market_id.clone(), binding.stream_id.clone());
        let mut hubs = self
            .0
            .market_hubs
            .lock()
            .map_err(|_| Error::Protocol("market registry poisoned".into()))?;
        hubs.retain(|_, hub| hub.strong_count() > 0);
        if let Some(hub) = hubs.get(&registry_key).and_then(|h| h.upgrade()) {
            crate::error::require(
                hub.binding.key == binding.key && hub.budget == budget,
                "same stream identity/budget changed",
            )?;
            match hub.attach() {
                Ok(receiver) => return Ok(receiver),
                Err(Error::Cancelled) => {}
                Err(e) => return Err(e),
            }
        }
        if hubs.len() >= 128 {
            return Err(Error::Limit("client shared markets"));
        }
        let state = Arc::new(RwLock::new(MarketState::new(
            binding.clone(),
            budget.clone(),
        )?));
        let hub = Arc::new(MarketHub {
            state,
            binding,
            budget,
            connected: AtomicBool::new(false),
            channel: Fanout::new(self.0.event_bytes.clone()),
        });
        let receiver = hub.attach()?;
        hubs.insert(registry_key, Arc::downgrade(&hub));
        let _ = runtime;
        self.spawn_tracked(run(self.clone(), hub), false)?;
        Ok(receiver)
    }
}
impl SharedMarket {
    pub fn snapshot(&self, depth: Option<usize>) -> Result<crate::book::DepthSnapshot> {
        self.state()?.depth_view(depth)
    }
    pub fn state_handle(&self) -> StateHandle {
        self.hub.state.clone()
    }
    pub fn state(&self) -> Result<RwLockReadGuard<'_, MarketState>> {
        self.hub
            .state
            .read()
            .map_err(|_| Error::Protocol("market state poisoned".into()))
    }
    pub async fn next(&mut self) -> Result<Option<MarketDelivery>> {
        loop {
            let event = self.rx.next().await?;
            if let Some(MarketDelivery::Frame(frame, _)) = &event {
                if !self.seed_pending
                    && self
                        .seed_sequence
                        .is_some_and(|s| frame.state_sequence <= s)
                {
                    continue;
                }
                self.seed_pending = false;
            }
            return Ok(event);
        }
    }
    pub fn close(&mut self) {
        self.rx.close();
    }
}

async fn run(client: Client, hub: Arc<MarketHub>) {
    let mut delay = 250u64;
    'outer: loop {
        let wait;
        if !hub.channel.active() || client.is_closed() {
            break;
        }
        let connect = client.market_transport(hub.state.clone(), hub.budget.clone());
        let mut stream = tokio::select! {
            _=client.wait_closed()=>break,
            _=hub.channel.changed.notified()=>continue,
            result=connect=>match result {
                Ok(stream)=>stream,
                Err(error)=>{
                    if crate::shared_price::terminal(&error) {hub.channel.finish(Some(Arc::from(error.to_string())));return;}
                    wait=crate::retry::delay_ms(delay,Some(&error));
                    hub.channel.send(MarketDelivery::Recovering {reason:error.to_string(),retry_after_ms:wait},256);
                    tokio::select! {_=client.wait_closed()=>break,
                        _=hub.channel.changed.notified()=>{},_=tokio::time::sleep(Duration::from_millis(wait))=>{}}
                    delay=(delay*2).min(30_000);continue;
                }
            },
        };
        let healthy_since = tokio::time::Instant::now();
        loop {
            if !hub.channel.active() {
                break 'outer;
            }
            let next = tokio::select! {
                _=client.wait_closed()=>break 'outer,
                _=hub.channel.changed.notified()=>continue,
                result=stream.next()=>result,
            };
            match next {
                Ok(Some(frame)) => {
                    hub.connected.store(true, Ordering::Release);
                    let top = match stream.state() {
                        Ok(state) => Arc::new(state.top()),
                        Err(e) => {
                            hub.channel.finish(Some(Arc::from(e.to_string())));
                            return;
                        }
                    };
                    let bytes = frame.encoded_len() + 512;
                    hub.channel
                        .send(MarketDelivery::Frame(Arc::new(frame), top), bytes);
                    if healthy_since.elapsed() >= Duration::from_secs(10) {
                        delay = 250;
                    }
                }
                Ok(None) => break 'outer,
                Err(error) => {
                    hub.connected.store(false, Ordering::Release);
                    if crate::shared_price::terminal(&error) {
                        hub.channel.finish(Some(Arc::from(error.to_string())));
                        return;
                    }
                    wait = crate::retry::delay_ms(delay, Some(&error));
                    hub.channel.send(
                        MarketDelivery::Recovering {
                            reason: error.to_string(),
                            retry_after_ms: wait,
                        },
                        256,
                    );
                    break;
                }
            }
        }
        tokio::select! {_=client.wait_closed()=>break,
        _=hub.channel.changed.notified()=>{},_=tokio::time::sleep(Duration::from_millis(wait))=>{}}
        delay = (delay * 2).min(30_000);
    }
    hub.connected.store(false, Ordering::Release);
    if let Ok(mut state) = hub.state.write() {
        state.source_status = "unavailable".into();
    }
    hub.channel.finish(None);
}
