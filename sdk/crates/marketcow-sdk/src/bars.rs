//! Existing finalized owner windows, never a substitute for real trade history.
use crate::{Client, Result, btc, error::require, price::decimal};
use reqwest::Method;

#[derive(Clone, Copy, Debug)]
pub enum Interval {
    Second,
    Minute,
}
impl Interval {
    pub fn millis(self) -> u32 {
        match self {
            Self::Second => 1000,
            Self::Minute => 60000,
        }
    }
}
impl Client {
    pub async fn finalized_bars(&self, interval: Interval) -> Result<btc::FinalizedWindow> {
        self.finalized_bar_updates(interval, None).await
    }
    pub async fn finalized_bar_updates(
        &self,
        interval: Interval,
        after: Option<i64>,
    ) -> Result<btc::FinalizedWindow> {
        let path = match interval {
            Interval::Second => "/v3/btc/bars/1s",
            Interval::Minute => "/v3/btc/bars/1m",
        };
        let query = after
            .map(|n| vec![("after_open_ms", n.to_string())])
            .unwrap_or_default();
        let (window, _) = self
            .request::<btc::FinalizedWindow>(Method::GET, path, &query, None, false, false)
            .await?;
        validate(&window, interval)?;
        require(window.after_open_ms == after, "bar update request identity")?;
        Ok(window)
    }
    pub fn subscribe_bars(&self, interval: Interval) -> Result<BarSubscription> {
        tokio::runtime::Handle::try_current().map_err(|_| {
            crate::Error::Configuration("bar subscription requires Tokio runtime".into())
        })?;
        if self.is_closed() {
            return Err(crate::Error::Cancelled);
        }
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Ok(BarSubscription {
            client: self.clone(),
            interval,
            tick,
            last: None,
            stream: None,
            last_status: None,
            closed: false,
        })
    }
}
/// Initial cached window, then only newly finalized bars. This bounded cache
/// polling adds no upstream feed and never redownloads a historic prefix.
pub struct BarSubscription {
    client: Client,
    interval: Interval,
    tick: tokio::time::Interval,
    last: Option<i64>,
    stream: Option<String>,
    last_status: Option<(bool, Option<String>, String, u64)>,
    closed: bool,
}
impl BarSubscription {
    pub async fn next(&mut self) -> Result<Option<btc::FinalizedWindow>> {
        loop {
            if self.closed || self.client.is_closed() {
                return Ok(None);
            }
            tokio::select! {_=self.client.wait_closed()=>return Ok(None),_=self.tick.tick()=>{}}
            let mut value = self
                .client
                .finalized_bar_updates(self.interval, self.last)
                .await?;
            if self
                .stream
                .as_ref()
                .is_some_and(|id| id != &value.stream_id)
            {
                value = self.client.finalized_bars(self.interval).await?;
            }
            if value.gap {
                self.closed = true;
                return Err(crate::Error::HistoryGap {
                    page: None,
                    reason: "finalized_bar_retention_gap".into(),
                    missing: Vec::new(),
                });
            }
            let status = (
                value.ready,
                value.reason.clone(),
                value.stream_id.clone(),
                value.durable_sequence,
            );
            let changed = self.last_status.as_ref() != Some(&status);
            self.stream = Some(value.stream_id.clone());
            self.last_status = Some(status);
            if let Some(bar) = value.bars.last() {
                self.last = Some(bar.open_time_ms);
            }
            if changed || !value.bars.is_empty() {
                return Ok(Some(value));
            }
        }
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
}
fn validate(value: &btc::FinalizedWindow, interval: Interval) -> Result<()> {
    let count = match interval {
        Interval::Second => 300,
        Interval::Minute => 60,
    };
    let step = i64::from(interval.millis());
    require(
        value.schema_version == "marketcow.binance-finalized-window.v3"
            && value.instrument_id == "BTCUSDT.BINANCE"
            && value.interval_ms == interval.millis()
            && !value.stream_id.is_empty()
            && value.bars.len() <= count
            && value.checkpoint_sequence <= value.durable_sequence,
        "finalized bar identity/budget",
    )?;
    require(
        !value.ready
            || (value.reason.is_none()
                && (value.after_open_ms.is_some() || value.bars.len() == count)
                && value
                    .bars
                    .last()
                    .is_none_or(|b| b.open_time_ms == value.expected_last_open_ms)),
        "finalized bar readiness",
    )?;
    let mut prior: Option<i64> = None;
    if let Some(after) = value.after_open_ms {
        require(
            value.bars.first().is_none_or(|b| b.open_time_ms > after),
            "bar updates before requested boundary",
        )?;
        if !value.gap {
            prior = Some(after);
        }
    }
    for bar in &value.bars {
        require(
            bar.open_time_ms > 0
                && bar.open_time_ms % step == 0
                && bar.open_time_ms.checked_add(step - 1) == Some(bar.close_time_ms)
                && prior.is_none_or(|p| p.checked_add(step) == Some(bar.open_time_ms)),
            "finalized bar boundaries",
        )?;
        let open = decimal(&bar.open, true)?;
        let high = decimal(&bar.high, true)?;
        let low = decimal(&bar.low, true)?;
        let close = decimal(&bar.close, true)?;
        require(
            low <= open && low <= close && high >= open && high >= close && high >= low,
            "finalized bar OHLC",
        )?;
        decimal(&bar.base_volume, false)?;
        decimal(&bar.quote_volume, false)?;
        decimal(&bar.taker_buy_base_volume, false)?;
        decimal(&bar.taker_buy_quote_volume, false)?;
        prior = Some(bar.open_time_ms);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_bars_are_facts_not_ready_and_noncontiguous_windows_reject() {
        let mut window = btc::FinalizedWindow {
            schema_version: "marketcow.binance-finalized-window.v3".into(),
            instrument_id: "BTCUSDT.BINANCE".into(),
            interval_ms: 1000,
            stream_id: "a".into(),
            ready: false,
            reason: Some("stale".into()),
            durable_sequence: 2,
            checkpoint_sequence: 1,
            bars: vec![btc::FinalizedBar {
                open_time_ms: 1000,
                close_time_ms: 1999,
                open: "100.00".into(),
                high: "101".into(),
                low: "99".into(),
                close: "100".into(),
                base_volume: "0".into(),
                quote_volume: "0".into(),
                taker_buy_base_volume: "0".into(),
                taker_buy_quote_volume: "0".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        validate(&window, Interval::Second).unwrap();
        window.ready = true;
        assert!(validate(&window, Interval::Second).is_err());
        window.ready = false;
        let mut next = window.bars[0].clone();
        next.open_time_ms = 3000;
        next.close_time_ms = 3999;
        window.bars.push(next);
        assert!(validate(&window, Interval::Second).is_err());
    }
}
