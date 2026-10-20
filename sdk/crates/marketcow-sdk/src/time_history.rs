use crate::{
    Client, Error, Result, common, error::require, history::TradeHistory, wire_history as wire,
};
use prost::Message;
use reqwest::Method;
use tokio::time::Instant;

/// A time-range query streams bounded, ordered freezes rather than building a
/// giant temporary history copy. Per-segment durability is explicit; not one
/// multi-hour SQLite snapshot transaction.
pub struct TimeHistory {
    client: Client,
    start: i64,
    fence: wire::RevisionFence,
    cursor: i64,
    end: i64,
    high: u64,
    limit: u32,
    active: Option<TradeHistory>,
    active_end: i64,
    index: u32,
    bytes: u64,
    rows: u64,
    pages: u32,
    deadline: Instant,
    closed: bool,
}
pub struct TimePage {
    pub page: wire::Page,
    pub segment_index: u32,
    pub query_complete: bool,
}
impl Client {
    pub async fn resolve_trade_range(
        &self,
        query: wire::TimeRangeQuery,
    ) -> Result<wire::ResolvedRange> {
        let (value, _) = self
            .request::<wire::ResolvedRange>(
                Method::POST,
                "/v3/btc/trade-history/resolve",
                &[],
                Some(query.encode_to_vec()),
                false,
                false,
            )
            .await?;
        require(
            value.schema_version == "marketcow.btc-trade-range.v3"
                && value.query.as_ref() == Some(&query),
            "time range response identity",
        )?;
        let range = value
            .range
            .as_ref()
            .ok_or_else(|| Error::Protocol("resolved range missing".into()))?;
        require(
            range.source == query.source
                && range.instrument_id == query.instrument_id
                && range.start_time_ms == query.start_time_ms
                && range.limit == query.limit
                && range.through_trade_id > 0
                && range.through_trade_id <= value.database_high_trade_id
                && value.end_time_ms > query.start_time_ms
                && query.end_time_ms.is_none_or(|end| end == value.end_time_ms)
                && query
                    .maximum_trade_id
                    .is_none_or(|id| id == value.database_high_trade_id),
            "resolved range binding",
        )?;
        require(
            value.range_revision.as_ref().is_some_and(|f| {
                f.first_trade_id <= f.last_trade_id
                    && f.last_trade_id == range.through_trade_id
                    && f.revision > 0
            }),
            "resolved range revision",
        )?;
        Ok(value)
    }
    pub async fn history_between(
        &self,
        start_time_ms: i64,
        end_time_ms: Option<i64>,
        limit: u32,
    ) -> Result<TimeHistory> {
        require(
            start_time_ms > 0
                && end_time_ms.is_none_or(|end| end > start_time_ms)
                && (1..=1000).contains(&limit),
            "invalid time range",
        )?;
        let deadline = Instant::now()
            + self
                .limits()
                .history_timeout
                .saturating_sub(crate::history::CLEANUP_TIME);
        let query = wire::TimeRangeQuery {
            source: common::Source::BinanceSpotTrade as i32,
            instrument_id: "BTCUSDT.BINANCE".into(),
            start_time_ms,
            end_time_ms,
            limit,
            maximum_trade_id: None,
            revision_fence: None,
        };
        let resolved = tokio::time::timeout_at(deadline, self.resolve_trade_range(query))
            .await
            .map_err(|_| Error::Limit("history query deadline"))??;
        Ok(TimeHistory {
            client: self.clone(),
            start: start_time_ms,
            fence: resolved.range_revision.unwrap(),
            cursor: start_time_ms,
            end: resolved.end_time_ms,
            high: resolved.database_high_trade_id,
            limit,
            active: None,
            active_end: start_time_ms,
            index: 0,
            bytes: 0,
            rows: 0,
            pages: 0,
            deadline,
            closed: false,
        })
    }
}
impl TimeHistory {
    pub async fn next_page(&mut self) -> Result<Option<TimePage>> {
        if self.closed || self.cursor >= self.end {
            return Ok(None);
        }
        let result = tokio::time::timeout_at(self.deadline, self.next_inner()).await;
        match result {
            Ok(Ok(page)) => Ok(page),
            other => {
                let error = match other {
                    Ok(Err(e)) => e,
                    _ => Error::Limit("history query deadline"),
                };
                let _ = self.close().await;
                Err(error)
            }
        }
    }
    async fn next_inner(&mut self) -> Result<Option<TimePage>> {
        if let Some(active) = &mut self.active {
            if active.is_complete() {
                // A failed release is retried before allocating another slot.
                active.close().await?;
                self.cursor = self.active_end;
                self.active = None;
                self.index += 1;
                if self.cursor >= self.end {
                    self.closed = true;
                    return Ok(None);
                }
            }
        }
        if self.active.is_none() {
            let end = self.cursor.saturating_add(900_000).min(self.end);
            let resolved = self
                .client
                .resolve_trade_range(wire::TimeRangeQuery {
                    source: common::Source::BinanceSpotTrade as i32,
                    instrument_id: "BTCUSDT.BINANCE".into(),
                    start_time_ms: self.cursor,
                    end_time_ms: Some(end),
                    limit: self.limit,
                    maximum_trade_id: Some(self.high),
                    revision_fence: Some(self.fence.clone()),
                })
                .await?;
            let range = resolved.range.unwrap();
            self.active = Some(self.client.trade_history(
                range.start_time_ms,
                range.through_trade_id,
                range.limit,
            )?);
            self.active_end = end;
        }
        let active = self.active.as_mut().unwrap();
        let before = active.bytes_received();
        let page = active
            .next_page()
            .await?
            .ok_or_else(|| Error::Protocol("history segment ended without final page".into()))?;
        self.bytes = self
            .bytes
            .checked_add(active.bytes_received() - before)
            .ok_or(Error::Limit("query bytes"))?;
        self.rows = self
            .rows
            .checked_add(page.trades.len() as u64)
            .ok_or(Error::Limit("query rows"))?;
        self.pages += 1;
        if self.bytes > self.client.limits().history_bytes
            || self.rows > self.client.limits().history_rows
            || self.pages > self.client.limits().history_pages
        {
            return Err(Error::Limit("history query cumulative budget"));
        }
        let final_page = page.complete && self.active_end == self.end;
        if final_page {
            // Recheck the whole requested interval after the last page: a
            // repair to an already yielded segment must prevent query_complete.
            self.client
                .resolve_trade_range(wire::TimeRangeQuery {
                    source: common::Source::BinanceSpotTrade as i32,
                    instrument_id: "BTCUSDT.BINANCE".into(),
                    start_time_ms: self.start,
                    end_time_ms: Some(self.end),
                    limit: self.limit,
                    maximum_trade_id: Some(self.high),
                    revision_fence: Some(self.fence.clone()),
                })
                .await?;
        }
        Ok(Some(TimePage {
            query_complete: page.complete && self.active_end == self.end,
            segment_index: self.index,
            page,
        }))
    }
    pub async fn close(&mut self) -> Result<()> {
        self.closed = true;
        if let Some(active) = &mut self.active {
            active.close().await?;
        }
        self.active = None;
        Ok(())
    }
}
