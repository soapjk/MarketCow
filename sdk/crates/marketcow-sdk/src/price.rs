use crate::client::Socket;
use crate::{Client, Error, Result, error::require};
use futures_util::{SinkExt, StreamExt};
use marketcow_wire::{self as wire, btc, common};
use prost::Message;
use rust_decimal::Decimal;
use std::{str::FromStr, time::Duration};
use tokio_tungstenite::tungstenite;

pub(crate) fn decimal(value: &str, positive: bool) -> Result<Decimal> {
    require(
        !value.is_empty()
            && value.len() <= 96
            && !value.contains(['e', 'E', '+'])
            && value.bytes().all(|c| c.is_ascii_digit() || c == b'.'),
        "plain decimal required",
    )?;
    let value = Decimal::from_str_exact(value)
        .map_err(|_| Error::Protocol("decimal precision/value".into()))?;
    require(
        if positive {
            value > Decimal::ZERO
        } else {
            value >= Decimal::ZERO
        },
        "decimal sign",
    )?;
    Ok(value)
}

pub(crate) fn validate_trade(trade: &btc::Trade) -> Result<()> {
    require(
        trade.trade_id > 0
            && trade.trade_time_ms > 0
            && trade.buyer_is_maker.is_some()
            && trade.event_time_ms.is_none_or(|t| t > 0),
        "trade identity/time/maker presence",
    )?;
    decimal(&trade.price, true)?;
    decimal(&trade.quantity, true)?;
    Ok(())
}

pub(crate) fn same_trade(a: &btc::Trade, b: &btc::Trade) -> bool {
    a.trade_id == b.trade_id
        && a.trade_time_ms == b.trade_time_ms
        && a.event_time_ms == b.event_time_ms
        && a.buyer_is_maker == b.buyer_is_maker
        && Decimal::from_str(&a.price).ok() == Decimal::from_str(&b.price).ok()
        && Decimal::from_str(&a.quantity).ok() == Decimal::from_str(&b.quantity).ok()
}

/// O(1) live tail, not a cumulative strategy history or freshness gate.
#[derive(Clone, Debug)]
pub struct PriceState {
    pub source: common::Source,
    pub instrument: String,
    pub stream_id: Option<String>,
    pub sequence: Option<u64>,
    pub available: bool,
    pub latest_trade: Option<btc::Trade>,
    pub latest_observation: Option<btc::Observation>,
    pub signed_age_ms: Option<i64>,
    initial_sequence: Option<u64>,
    producer: Option<String>,
}

impl PriceState {
    pub fn new(source: common::Source) -> Result<Self> {
        require(source != common::Source::Unspecified, "source required")?;
        let instrument = match source {
            common::Source::BinanceSpotTrade => "BTCUSDT.BINANCE",
            common::Source::ChainlinkBtcUsdTwap60 => "BTCUSD.CHAINLINK.TWAP60",
            _ => "BTCUSD.CHAINLINK",
        };
        Ok(Self {
            source,
            instrument: instrument.into(),
            stream_id: None,
            sequence: None,
            available: false,
            latest_trade: None,
            latest_observation: None,
            signed_age_ms: None,
            initial_sequence: None,
            producer: None,
        })
    }

    /// A new physical subscription establishes its own first publication serial.
    pub fn reset_transport(&mut self) {
        self.available = false;
        self.sequence = None;
        self.initial_sequence = None;
        self.producer = None;
        self.stream_id = None;
    }

    fn observations(&self, values: &[btc::Observation], snapshot: bool) -> Result<()> {
        require(!values.is_empty(), "empty observation payload")?;
        let mut previous = if snapshot {
            None
        } else {
            self.latest_observation.as_ref().map(|p| p.source_time_ms)
        };
        for (i, point) in values.iter().enumerate() {
            require(
                point.source_time_ms > 0
                    && point.source_sequence.is_some_and(|n| n > 0)
                    && point.row_index.is_some(),
                "observation presence/time",
            )?;
            decimal(&point.price, true)?;
            require(
                previous.is_none_or(|t| point.source_time_ms >= t),
                "observation time regression",
            )?;
            if !snapshot {
                require(
                    point.row_index == Some(i as u32)
                        && point.source_sequence == values[0].source_sequence,
                    "observation frame rows",
                )?;
            }
            previous = Some(point.source_time_ms);
        }
        Ok(())
    }

    /// Validate the full update before committing the tiny live state. No hashes.
    /// false means an initial-snapshot queued publication was ignored.
    pub fn apply(&mut self, frame: &btc::PriceFrame) -> Result<bool> {
        use btc::price_frame::Payload;
        require(
            frame.schema_version == wire::PRICE_SCHEMA
                && frame.source == self.source as i32
                && frame.instrument_id == self.instrument
                && !frame.producer_instance_id.is_empty()
                && !frame.stream_id.is_empty(),
            "price schema/source/identity",
        )?;
        if self
            .initial_sequence
            .is_some_and(|s| frame.stream_sequence <= s)
        {
            return Ok(false);
        }
        if let Some(previous) = self.sequence {
            require(
                previous.checked_add(1) == Some(frame.stream_sequence),
                "price publication gap",
            )?;
            require(
                self.producer.as_ref() == Some(&frame.producer_instance_id),
                "producer changed without reconnect",
            )?;
        }
        let payload = frame
            .payload
            .as_ref()
            .ok_or_else(|| Error::Protocol("missing price payload".into()))?;
        if !matches!(payload, Payload::Snapshot(_)) {
            require(
                self.stream_id
                    .as_ref()
                    .is_none_or(|s| s == &frame.stream_id),
                "generation requires snapshot",
            )?;
        }
        match payload {
            Payload::Snapshot(snapshot) if self.source == common::Source::BinanceSpotTrade => {
                require(
                    snapshot.observations.is_empty() && !snapshot.trades.is_empty(),
                    "Binance snapshot shape",
                )?;
                let mut previous: Option<&btc::Trade> = None;
                for trade in &snapshot.trades {
                    validate_trade(trade)?;
                    require(
                        previous.is_none_or(|p| {
                            p.trade_id.checked_add(1) == Some(trade.trade_id)
                                && p.trade_time_ms <= trade.trade_time_ms
                        }),
                        "snapshot trade continuity",
                    )?;
                    previous = Some(trade);
                }
                let first = snapshot.trades.first().unwrap();
                let last = snapshot.trades.last().unwrap();
                require(
                    snapshot.first_trade_id == Some(first.trade_id)
                        && snapshot.last_trade_id == Some(last.trade_id)
                        && frame.source_sequence == Some(last.trade_id),
                    "snapshot tail binding",
                )?;
                self.latest_trade = Some(last.clone());
                self.available = true;
            }
            Payload::Snapshot(snapshot) => {
                require(snapshot.trades.is_empty(), "mixed observation snapshot")?;
                self.observations(&snapshot.observations, true)?;
                require(
                    frame.source_sequence
                        == snapshot.observations.last().and_then(|o| o.source_sequence),
                    "observation tail binding",
                )?;
                self.latest_observation = snapshot.observations.last().cloned();
                self.available = true;
            }
            Payload::Trade(delta) => {
                require(
                    self.source == common::Source::BinanceSpotTrade && self.available,
                    "trade before snapshot",
                )?;
                let trade = delta
                    .trade
                    .as_ref()
                    .ok_or_else(|| Error::Protocol("missing trade".into()))?;
                validate_trade(trade)?;
                let previous = self
                    .latest_trade
                    .as_ref()
                    .ok_or_else(|| Error::Protocol("missing previous trade".into()))?;
                require(
                    previous.trade_id.checked_add(1) == Some(trade.trade_id)
                        && previous.trade_time_ms <= trade.trade_time_ms
                        && frame.source_sequence == Some(trade.trade_id),
                    "trade ID/time/source binding",
                )?;
                self.latest_trade = Some(trade.clone());
            }
            Payload::Observations(delta) => {
                require(
                    self.source != common::Source::BinanceSpotTrade && self.available,
                    "observations before snapshot",
                )?;
                self.observations(&delta.observations, false)?;
                let last = delta.observations.last().unwrap();
                require(
                    self.latest_observation
                        .as_ref()
                        .and_then(|o| o.source_sequence)
                        .is_none_or(|n| last.source_sequence.is_some_and(|next| next > n))
                        && frame.source_sequence == last.source_sequence,
                    "observation sequence",
                )?;
                self.latest_observation = Some(last.clone());
            }
            Payload::Status(status) => {
                let phase = btc::Phase::try_from(status.phase)
                    .map_err(|_| Error::Protocol("unknown source phase".into()))?;
                require(phase != btc::Phase::Unspecified, "missing source phase")?;
                // A status LIVE alone cannot repair an unavailable data prefix.
                if phase != btc::Phase::Live {
                    self.available = false;
                }
            }
            Payload::Error(error) => {
                return Err(Error::Remote {
                    status: 0,
                    code: error.code.clone(),
                    retryable: error.retryable,
                    retry_after_ms: error.retry_after_ms,
                    detail: error.detail.clone(),
                    failure: None,
                });
            }
        }
        if self.initial_sequence.is_none() {
            self.initial_sequence = Some(frame.stream_sequence);
        }
        self.sequence = Some(frame.stream_sequence);
        self.producer = Some(frame.producer_instance_id.clone());
        self.stream_id = Some(frame.stream_id.clone());
        self.signed_age_ms = frame.latest_source_age_ms_at_emit;
        Ok(true)
    }
}

/// Pull-based: bounded tungstenite frames, no hidden unbounded delivery queue.
pub struct PriceSubscription {
    client: Client,
    socket: Option<Socket>,
    state: PriceState,
    closed: bool,
}

impl Client {
    pub async fn subscribe_prices(&self, source: common::Source) -> Result<PriceSubscription> {
        let state = PriceState::new(source)?;
        let mut value = PriceSubscription {
            client: self.clone(),
            socket: None,
            state,
            closed: false,
        };
        value.connect().await?;
        Ok(value)
    }
}

impl PriceSubscription {
    pub fn state(&self) -> &PriceState {
        &self.state
    }

    async fn connect(&mut self) -> Result<()> {
        if self.client.is_closed() || self.closed {
            return Err(Error::Cancelled);
        }
        let source = match self.state.source {
            common::Source::BinanceSpotTrade => "binance_spot_trade",
            common::Source::ChainlinkBtcUsd => "chainlink_btc_usd",
            common::Source::ChainlinkBtcUsdTwap60 => "chainlink_twap_60s",
            _ => return Err(Error::Configuration("unknown source".into())),
        };
        let socket = self
            .client
            .socket(
                "/v3/btc/price-stream",
                &[("source", source)],
                wire::PRICE_SUBPROTOCOL,
                self.client.limits().price_frame_bytes,
            )
            .await?;
        self.state.reset_transport();
        self.socket = Some(socket);
        Ok(())
    }

    /// Every error is returned to the caller; the next call reconnects. No hidden
    /// retry loop that hides gaps or turns an authentication failure into traffic.
    pub async fn next(&mut self) -> Result<Option<btc::PriceFrame>> {
        if self.closed || self.client.is_closed() {
            return Ok(None);
        }
        if self.socket.is_none() {
            self.connect().await?;
        }
        loop {
            let next = tokio::time::timeout(
                self.client.limits().stream_idle_timeout,
                self.socket.as_mut().unwrap().next(),
            )
            .await;
            let message = match next {
                Ok(Some(Ok(value))) => value,
                Ok(Some(Err(error))) => {
                    self.socket = None;
                    self.state.available = false;
                    return Err(error.into());
                }
                _ => {
                    self.socket = None;
                    self.state.available = false;
                    return Err(Error::Protocol("price stream closed/idle".into()));
                }
            };
            match message {
                tungstenite::Message::Binary(bytes) => {
                    self.client
                        .0
                        .price_message_bytes
                        .fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    let result = btc::PriceFrame::decode(bytes)
                        .map_err(Error::from)
                        .and_then(|frame| self.state.apply(&frame).map(|applied| (frame, applied)));
                    match result {
                        Ok((frame, true)) => return Ok(Some(frame)),
                        Ok((_, false)) => continue,
                        Err(error) => {
                            self.socket = None;
                            self.state.available = false;
                            return Err(error);
                        }
                    }
                }
                tungstenite::Message::Ping(_) => {
                    self.socket.as_mut().unwrap().flush().await?;
                }
                tungstenite::Message::Pong(_) => {}
                _ => {
                    self.socket = None;
                    self.state.available = false;
                    return Err(Error::Protocol("nonbinary price message/close".into()));
                }
            }
        }
    }

    pub async fn close(&mut self) -> Result<()> {
        self.closed = true;
        self.state.available = false;
        if let Some(mut socket) = self.socket.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), socket.close(None)).await;
        }
        Ok(())
    }
}
