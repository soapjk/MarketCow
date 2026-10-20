use crate::{
    Client, Error, Result,
    error::require,
    price::{same_trade, validate_trade},
};
use marketcow_wire::{btc, common, history};
use prost::Message;
use reqwest::Method;
use std::time::Duration;
use tokio::time::Instant;
pub(crate) const CLEANUP_TIME: Duration = Duration::from_secs(2);
mod checkpoint;

/// One frozen DB range. It owns its release identity, never the entire result.
/// Complete coverage is established only after the last page is validated.
pub struct TradeHistory {
    pub(crate) client: Client,
    range: history::Range,
    request_id: String,
    session_id: Option<String>,
    token: Option<String>,
    page_count: Option<u32>,
    total_count: Option<u64>,
    revision: Option<u64>,
    next_index: u32,
    rows: u64,
    bytes: u64,
    last: Option<btc::Trade>,
    deadline: Instant,
    expires: Option<Instant>,
    complete: bool,
    stopped: bool,
    released: bool,
    release_error: Option<String>,
    release_on_complete: bool,
    release_receipt: Option<history::ReleaseReceipt>,
}

impl Client {
    /// Low-level fixed boundary. The time-only range resolver will wrap this
    /// method; it must not infer arbitrary history boundaries from current WS.
    pub fn trade_history(
        &self,
        start_time_ms: i64,
        through_trade_id: u64,
        limit: u32,
    ) -> Result<TradeHistory> {
        if self.is_closed() {
            return Err(Error::Cancelled);
        }
        require(
            start_time_ms > 0 && through_trade_id > 0 && (1..=1000).contains(&limit),
            "invalid history range",
        )?;
        Ok(TradeHistory {
            client: self.clone(),
            range: history::Range {
                source: common::Source::BinanceSpotTrade as i32,
                instrument_id: "BTCUSDT.BINANCE".into(),
                start_time_ms,
                through_trade_id,
                limit,
            },
            session_id: None,
            request_id: uuid::Uuid::new_v4().to_string(),
            token: None,
            page_count: None,
            total_count: None,
            revision: None,
            next_index: 0,
            rows: 0,
            bytes: 0,
            last: None,
            deadline: Instant::now() + self.limits().history_timeout.saturating_sub(CLEANUP_TIME),
            expires: None,
            complete: false,
            stopped: false,
            released: false,
            release_error: None,
            release_on_complete: true,
            release_receipt: None,
        })
    }
}

impl TradeHistory {
    /// Opt-in installation boundary. Validated pages stay usable regardless of
    /// release, but this mode keeps the freeze until explicit close/abandonment.
    /// Set before reading the first page; errors and Client close still clean up.
    pub fn retain_until_close(&mut self) -> Result<()> {
        require(
            self.next_index == 0 && self.session_id.is_none(),
            "release policy must precede first page",
        )?;
        self.release_on_complete = false;
        Ok(())
    }
    pub fn is_complete(&self) -> bool {
        self.complete
    }
    pub fn is_released(&self) -> bool {
        self.released
    }
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }
    pub fn release_error(&self) -> Option<&str> {
        self.release_error.as_deref()
    }
    pub fn release_receipt(&self) -> Option<&history::ReleaseReceipt> {
        self.release_receipt.as_ref()
    }
    pub fn bytes_received(&self) -> u64 {
        self.bytes
    }

    pub fn verify_live_overlap(&self, live: &btc::Trade) -> Result<()> {
        require(
            self.complete && self.last.as_ref().is_some_and(|p| same_trade(p, live)),
            "history/live tail overlap",
        )
    }

    fn validate(&mut self, page: &history::Page, bytes: usize) -> Result<()> {
        require(
            page.schema_version == "marketcow.btc-trade-history.v3"
                && page.range.as_ref() == Some(&self.range),
            "history schema/range",
        )?;
        require(
            page.page_index == self.next_index
                && page.page_count > 0
                && page.page_count <= self.client.limits().history_pages
                && page.page_index < page.page_count
                && page.session_ttl_seconds == 600,
            "history pagination/TTL",
        )?;
        require(
            page.total_trade_count <= self.client.limits().history_rows
                && page.trades.len() <= self.range.limit as usize,
            "history row budget",
        )?;
        require(
            self.page_count.is_none_or(|n| n == page.page_count)
                && self.total_count.is_none_or(|n| n == page.total_trade_count)
                && self.revision.is_none_or(|n| n == page.database_revision),
            "frozen history metadata changed",
        )?;
        require(
            page.page_index == 0 || page.predecessor.is_none(),
            "predecessor outside first page",
        )?;
        if !page.coverage_verified || page.reason.is_some() || !page.missing_ranges.is_empty() {
            return Err(Error::HistoryGap {
                page: Some(Box::new(page.clone())),
                reason: page
                    .reason
                    .clone()
                    .unwrap_or_else(|| "coverage_unverified".into()),
                missing: page
                    .missing_ranges
                    .iter()
                    .map(|m| (m.first_trade_id, m.last_trade_id))
                    .collect(),
            });
        }
        let byte_total = self
            .bytes
            .checked_add(bytes as u64)
            .ok_or(Error::Limit("history bytes"))?;
        let row_total = self
            .rows
            .checked_add(page.trades.len() as u64)
            .ok_or(Error::Limit("history rows"))?;
        if byte_total > self.client.limits().history_bytes
            || row_total > self.client.limits().history_rows
        {
            return Err(Error::Limit("history cumulative budget"));
        }
        let mut previous = self.last.as_ref();
        if page.page_index == 0 {
            let predecessor = page.predecessor.as_ref().ok_or_else(|| Error::HistoryGap {
                page: Some(Box::new(page.clone())),
                reason: "predecessor_missing".into(),
                missing: Vec::new(),
            })?;
            validate_trade(predecessor)?;
            require(
                predecessor.trade_time_ms < self.range.start_time_ms,
                "predecessor source time",
            )?;
            previous = Some(predecessor);
        }
        for trade in &page.trades {
            validate_trade(trade)?;
            require(
                trade.trade_time_ms >= self.range.start_time_ms
                    && trade.trade_id <= self.range.through_trade_id,
                "history trade outside requested range",
            )?;
            require(
                previous.is_some_and(|p| {
                    p.trade_id.checked_add(1) == Some(trade.trade_id)
                        && p.trade_time_ms <= trade.trade_time_ms
                }),
                "history trade continuity",
            )?;
            previous = Some(trade);
        }
        let final_page = page.page_index + 1 == page.page_count;
        require(
            page.complete == final_page
                && if final_page {
                    page.next_page_token.is_none()
                } else {
                    page.next_page_token.as_ref().is_some_and(|t| !t.is_empty())
                },
            "history terminal semantics",
        )?;
        if final_page {
            require(
                row_total == page.total_trade_count
                    && previous.is_some_and(|p| p.trade_id == self.range.through_trade_id),
                "history final count/tail",
            )?;
        }
        // Commit after all validation; malformed pages cannot advance the prefix.
        self.page_count = Some(page.page_count);
        self.total_count = Some(page.total_trade_count);
        self.revision = Some(page.database_revision);
        self.last = previous.cloned();
        self.next_index += 1;
        self.token = page.next_page_token.clone();
        self.bytes = byte_total;
        self.rows = row_total;
        self.complete = final_page;
        Ok(())
    }

    async fn fetch(&mut self) -> Result<history::Page> {
        let request = history::PageRequest {
            range: Some(self.range.clone()),
            page_token: self.token.clone(),
            request_id: Some(self.request_id.clone()),
        };
        let deadline = self.expires.map_or(self.deadline, |t| t.min(self.deadline));
        // A repeated request identity finds the same freeze after a lost response.
        let attempts = 4;
        for attempt in 0..attempts {
            let result = tokio::time::timeout_at(
                deadline,
                self.client.request::<history::Page>(
                    Method::POST,
                    "/v3/btc/trade-history",
                    &[],
                    Some(request.encode_to_vec()),
                    false,
                    false,
                ),
            )
            .await;
            let result =
                result.unwrap_or_else(|_| Err(Error::Limit("history task/session deadline")));
            match result {
                Ok((page, bytes)) => {
                    require(
                        page.request_id.as_deref() == Some(&self.request_id),
                        "history request identity unsupported/mismatched",
                    )?;
                    require(
                        !page.session_id.is_empty() && page.session_id.len() <= 128,
                        "history session identity",
                    )?;
                    if let Some(id) = &self.session_id {
                        require(id == &page.session_id, "history session changed")?;
                    } else {
                        // Bind before coverage validation so even an incomplete
                        // page is released; only an exact echoed range is safe.
                        require(
                            page.range.as_ref() == Some(&self.range),
                            "history identity range",
                        )?;
                        self.session_id = Some(page.session_id.clone());
                        self.client.register_lease(crate::leases::Lease::Trades(
                            history::ReleaseRequest {
                                session_id: page.session_id.clone(),
                                range: Some(self.range.clone()),
                                request_id: Some(self.request_id.clone()),
                            },
                        ))?;
                        self.expires = Some(Instant::now() + Duration::from_secs(600));
                    }
                    self.validate(&page, bytes)?;
                    return Ok(page);
                }
                Err(error) if error.retryable() && attempt + 1 < attempts => {
                    let delay = crate::retry::duration(250 * (1 << attempt), &error);
                    if Instant::now()
                        .checked_add(delay)
                        .is_none_or(|t| t >= deadline)
                    {
                        return Err(error);
                    }
                    tokio::time::sleep(delay).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!()
    }

    pub async fn next_page(&mut self) -> Result<Option<history::Page>> {
        if self.stopped {
            // A previous failed release remains observable/retryable.
            if !self.released && self.release_on_complete {
                self.release().await?;
            }
            return Ok(None);
        }
        let result = self.fetch().await;
        match result {
            Ok(page) => {
                if self.complete {
                    self.stopped = true;
                    // Page delivery is not lost when release temporarily fails.
                    // The caller can inspect release_error and retry close().
                    if self.release_on_complete {
                        let _ = self.release().await;
                    }
                }
                Ok(Some(page))
            }
            Err(error) => {
                self.stopped = true;
                let _ = self.release().await;
                Err(error)
            }
        }
    }

    pub async fn close(&mut self) -> Result<()> {
        self.stopped = true;
        self.release().await
    }

    async fn release(&mut self) -> Result<()> {
        if self.released {
            return Ok(());
        }
        // No known identity is NOT an acknowledged release (e.g. lost first response).
        let Some(id) = &self.session_id else {
            return Ok(());
        };
        let request = history::ReleaseRequest {
            session_id: id.clone(),
            range: Some(self.range.clone()),
            request_id: Some(self.request_id.clone()),
        };
        let operation = self.client.request::<history::ReleaseReceipt>(
            Method::DELETE,
            "/v3/btc/trade-history/session",
            &[],
            Some(request.encode_to_vec()),
            false,
            true,
        );
        let result = tokio::time::timeout(CLEANUP_TIME, operation)
            .await
            .unwrap_or_else(|_| Err(Error::Limit("history release deadline")));
        let result = result.and_then(|(receipt, _)| {
            require(
                receipt.schema_version == "marketcow.btc-history-release.v3"
                    && receipt.session_id == *id
                    && receipt.range == request.range
                    && receipt.request_id == request.request_id,
                "release receipt identity",
            )?;
            Ok(receipt)
        });
        match result {
            Ok(receipt) => {
                self.release_receipt = Some(receipt);
                self.client.forget_lease(id);
                self.released = true;
                self.release_error = None;
                Ok(())
            }
            Err(error) => {
                self.release_error = Some(error.to_string());
                Err(error)
            }
        }
    }
}

impl Drop for TradeHistory {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        let Some(id) = self.session_id.clone() else {
            return;
        };
        let Ok(_runtime) = tokio::runtime::Handle::try_current() else {
            return;
        }; // TTL fallback, never panic.
        let client = self.client.clone();
        let Ok(permit) = client.0.cleanup_slots.clone().try_acquire_owned() else {
            return;
        };
        let range = self.range.clone();
        let request_id = self.request_id.clone();
        let _ = client.clone().spawn_tracked(
            async move {
                let _permit = permit; // Hard cap: Drop never creates an unbounded cleanup queue.
                let request = history::ReleaseRequest {
                    session_id: id,
                    range: Some(range),
                    request_id: Some(request_id),
                };
                let operation = client.request::<history::ReleaseReceipt>(
                    Method::DELETE,
                    "/v3/btc/trade-history/session",
                    &[],
                    Some(request.encode_to_vec()),
                    false,
                    true,
                );
                if let Ok(Ok((receipt, _))) = tokio::time::timeout(CLEANUP_TIME, operation).await {
                    if receipt.schema_version == "marketcow.btc-history-release.v3"
                        && receipt.session_id == request.session_id
                        && receipt.range == request.range
                        && receipt.request_id == request.request_id
                    {
                        client.forget_lease(&request.session_id);
                    }
                }
            },
            true,
        );
    }
}
