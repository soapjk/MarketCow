use crate::{Client, Error, Result, SharedMarket, market, market_stream::StateHandle};
use std::{sync::Arc, time::Duration};
use tokio::time::{Instant, Interval};

#[derive(Clone, Debug)]
pub enum MarketDelivery {
    MarketChanged(market::MarketBinding),
    Frame(Arc<market::MarketFrame>, Arc<crate::book::MarketTop>),
    Recovering { reason: String, retry_after_ms: u64 },
}

/// Follow the public "current" role, without treating next->current changes on
/// the same immutable key/stream as a new owner. Does not subscribe to next.
pub struct CurrentMarket {
    client: Client,
    active: Option<SharedMarket>,
    binding: Option<market::MarketBinding>,
    poll: Interval,
    retry_at: Instant,
    delay_ms: u64,
    closed: bool,
}

impl Client {
    pub fn current_market(&self) -> CurrentMarket {
        let mut poll = tokio::time::interval(Duration::from_secs(5));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        CurrentMarket {
            client: self.clone(),
            active: None,
            binding: None,
            poll,
            retry_at: Instant::now(),
            delay_ms: 250,
            closed: false,
        }
    }
}

impl CurrentMarket {
    pub fn state(
        &self,
    ) -> Result<Option<std::sync::RwLockReadGuard<'_, crate::book::MarketState>>> {
        self.active.as_ref().map(|s| s.state()).transpose()
    }
    pub fn state_handle(&self) -> Option<StateHandle> {
        self.active.as_ref().map(|s| s.state_handle())
    }

    async fn refresh(&mut self) -> Result<Option<MarketDelivery>> {
        let discovery = tokio::time::timeout(Duration::from_secs(3), self.client.markets())
            .await
            .map_err(|_| Error::Protocol("market discovery timeout".into()))??;
        let binding = discovery
            .markets
            .into_iter()
            .find(|b| b.role == "current")
            .ok_or_else(|| Error::Protocol("current market unavailable".into()))?;
        if self
            .binding
            .as_ref()
            .is_some_and(|old| old.key == binding.key && old.stream_id == binding.stream_id)
        {
            return Ok(None);
        }
        let budget = discovery
            .delivery_budget
            .ok_or_else(|| Error::Protocol("missing market delivery budget".into()))?;
        let next = self.client.shared_market(binding.clone(), budget)?;
        if let Some(old) = &mut self.active {
            old.close();
        }
        self.active = Some(next);
        self.binding = Some(binding.clone());
        self.retry_at = Instant::now();
        self.delay_ms = 250;
        Ok(Some(MarketDelivery::MarketChanged(binding)))
    }

    pub async fn next(&mut self) -> Result<Option<MarketDelivery>> {
        if self.closed || self.client.is_closed() {
            return Ok(None);
        }
        loop {
            if Instant::now() < self.retry_at {
                tokio::select! { _ = self.client.wait_closed() => return Ok(None),
                _ = tokio::time::sleep_until(self.retry_at) => {} }
            }
            if self.active.is_none() {
                tokio::select! { _ = self.client.wait_closed() => return Ok(None), _ = self.poll.tick() => {} }
                match self.refresh().await {
                    Ok(Some(event)) => return Ok(Some(event)),
                    Ok(None) => continue,
                    Err(error) => return self.recover(error),
                }
            }
            let next = tokio::select! {
                _ = self.client.wait_closed() => return Ok(None),
                _ = self.poll.tick() => {
                    match self.refresh().await {
                        Ok(Some(event)) => return Ok(Some(event)),
                        Ok(None) => continue,
                        Err(error) => return self.recover(error),
                    }
                },
                frame = self.active.as_mut().unwrap().next() => frame,
            };
            match next {
                Ok(Some(event)) => {
                    self.delay_ms = 250;
                    let ended = match &event {
                        MarketDelivery::Frame(frame, _) => match &frame.body {
                            Some(market::market_frame::Body::Snapshot(s)) => {
                                s.source_status == "ended"
                            }
                            Some(market::market_frame::Body::PriceChange(c)) => {
                                c.source_status == "ended"
                            }
                            _ => false,
                        },
                        _ => false,
                    };
                    if ended {
                        self.client.invalidate_discovery();
                        self.poll.reset_immediately();
                    }
                    return Ok(Some(event));
                }
                Ok(None) => return Ok(None),
                Err(error) => return self.recover(error),
            }
        }
    }

    fn recover(&mut self, error: Error) -> Result<Option<MarketDelivery>> {
        if crate::shared_price::terminal(&error) {
            return Err(error);
        }
        let delay = crate::retry::delay_ms(self.delay_ms, Some(&error));
        self.delay_ms = (self.delay_ms * 2).min(30_000);
        self.retry_at = Instant::now() + Duration::from_millis(delay);
        self.poll.reset_immediately();
        Ok(Some(MarketDelivery::Recovering {
            reason: error.to_string(),
            retry_after_ms: delay,
        }))
    }

    pub async fn close(&mut self) {
        self.closed = true;
        if let Some(stream) = &mut self.active {
            stream.close();
        }
    }
}
