use crate::{Client, Error, Result, book::MarketState, client::Socket, market};
use futures_util::{SinkExt, StreamExt};
use marketcow_wire::MARKET_SUBPROTOCOL;
use prost::Message;
use std::{
    sync::{Arc, RwLock, RwLockReadGuard},
    time::Duration,
};
use tokio_tungstenite::tungstenite;

/// Fixed immutable market subscription. Role-following discovery is a separate
/// lifecycle operation, never a silent key change inside this state owner.
pub type StateHandle = Arc<RwLock<MarketState>>;
pub struct MarketSubscription {
    client: Client,
    state: StateHandle,
    socket: Option<Socket>,
    maximum_frame: usize,
    closed: bool,
}

impl Client {
    pub(crate) async fn market_transport(
        &self,
        state: StateHandle,
        budget: market::DeliveryBudget,
    ) -> Result<MarketSubscription> {
        let maximum_frame = usize::try_from(budget.maximum_frame_bytes)
            .map_err(|_| Error::Limit("market frame budget"))?
            .min(self.limits().market_frame_bytes);
        let mut stream = MarketSubscription {
            client: self.clone(),
            state,
            socket: None,
            maximum_frame,
            closed: false,
        };
        stream.connect().await?;
        Ok(stream)
    }
    pub async fn subscribe_market(
        &self,
        binding: market::MarketBinding,
        budget: market::DeliveryBudget,
    ) -> Result<MarketSubscription> {
        let maximum_frame = usize::try_from(budget.maximum_frame_bytes)
            .map_err(|_| Error::Limit("market frame budget"))?
            .min(self.limits().market_frame_bytes);
        let state = Arc::new(RwLock::new(MarketState::new(binding, budget.clone())?));
        let mut stream = MarketSubscription {
            client: self.clone(),
            state,
            socket: None,
            maximum_frame,
            closed: false,
        };
        stream.connect().await?;
        Ok(stream)
    }
}

impl MarketSubscription {
    pub fn state(&self) -> Result<RwLockReadGuard<'_, MarketState>> {
        self.state
            .read()
            .map_err(|_| Error::Protocol("market state poisoned".into()))
    }
    pub fn state_handle(&self) -> StateHandle {
        self.state.clone()
    }

    async fn connect(&mut self) -> Result<()> {
        let (market, stream) = {
            let state = self.state()?;
            (state.key.market_id.clone(), state.stream_id.clone())
        };
        let socket = self
            .client
            .socket(
                "/v7/polymarket/btc-5m/market-stream",
                &[("market_id", &market), ("stream_id", &stream)],
                MARKET_SUBPROTOCOL,
                self.maximum_frame,
            )
            .await?;
        // Preserve display books, but the new connection requires its own full
        // snapshot. Cannot apply a delta to an unproved resumed prefix.
        {
            let mut state = self
                .state
                .write()
                .map_err(|_| Error::Protocol("market state poisoned".into()))?;
            state.sequence = None;
            state.source_status = "unavailable".into();
        }
        self.socket = Some(socket);
        Ok(())
    }

    pub async fn next(&mut self) -> Result<Option<market::MarketFrame>> {
        if self.closed || self.client.is_closed() {
            return Ok(None);
        }
        if self.socket.is_none() {
            self.connect().await?;
        }
        loop {
            let result = tokio::select! {
                _=self.client.wait_closed()=>{self.disconnect();return Ok(None);},
                result=tokio::time::timeout(
                self.client.limits().stream_idle_timeout,
                self.socket.as_mut().unwrap().next(),
            )
            =>result };
            let message = match result {
                Ok(Some(Ok(message))) => message,
                Ok(Some(Err(error))) => {
                    self.disconnect();
                    return Err(error.into());
                }
                _ => {
                    self.disconnect();
                    return Err(Error::Protocol("market stream closed/idle".into()));
                }
            };
            match message {
                tungstenite::Message::Binary(bytes) => {
                    self.client
                        .0
                        .market_message_bytes
                        .fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    let result = market::MarketFrame::decode(bytes)
                        .map_err(Error::from)
                        .and_then(|frame| {
                            self.state
                                .write()
                                .map_err(|_| Error::Protocol("market state poisoned".into()))?
                                .apply(&frame)?;
                            Ok(frame)
                        });
                    if result.is_err() {
                        self.disconnect();
                    }
                    return result.map(Some);
                }
                tungstenite::Message::Ping(_) => {
                    if let Err(error) = self.socket.as_mut().unwrap().flush().await {
                        self.disconnect();
                        return Err(error.into());
                    }
                }
                tungstenite::Message::Pong(_) => {}
                _ => {
                    self.disconnect();
                    return Err(Error::Protocol("nonbinary market message/close".into()));
                }
            }
        }
    }

    fn disconnect(&mut self) {
        self.socket = None;
        if let Ok(mut state) = self.state.write() {
            state.source_status = "unavailable".into();
        }
    }

    pub async fn close(&mut self) {
        self.closed = true;
        if let Ok(mut state) = self.state.write() {
            state.source_status = "unavailable".into();
        }
        if let Some(mut socket) = self.socket.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), socket.close(None)).await;
        }
    }
}
