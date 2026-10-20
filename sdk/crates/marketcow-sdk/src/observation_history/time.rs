use super::*;
pub struct ObservationTimeHistory {
    client: Client,
    source: Source,
    cursor: i64,
    end: i64,
    limit: u32,
    high: u64,
    total: u64,
    active: Option<ObservationHistory>,
    active_end: i64,
    index: u32,
    rows: u64,
    pages: u32,
    bytes: u64,
    deadline: Instant,
    closed: bool,
}
pub struct ObservationTimePage {
    pub page: history::ObservationPage,
    pub segment_index: u32,
    pub query_complete: bool,
}
impl Client {
    pub async fn observations_between(
        &self,
        source: Source,
        start: i64,
        end: i64,
        limit: u32,
    ) -> Result<ObservationTimeHistory> {
        require(
            start > 0 && end > start && (1..=1000).contains(&limit),
            "observation query range",
        )?;
        let instrument = match source {
            Source::ChainlinkBtcUsd => "BTCUSD.CHAINLINK",
            Source::ChainlinkBtcUsdTwap60 => "BTCUSD.CHAINLINK.TWAP60",
            _ => {
                return Err(Error::Configuration(
                    "observation history requires Chainlink source".into(),
                ));
            }
        };
        let deadline = Instant::now() + self.limits().history_timeout.saturating_sub(CLEANUP_TIME);
        let query = history::ObservationRangeQuery {
            source: source as i32,
            instrument_id: instrument.into(),
            start_time_ms: start,
            end_time_ms: end,
            limit,
        };
        let (resolved, _) = tokio::time::timeout_at(
            deadline,
            self.request::<history::ResolvedObservationRange>(
                Method::POST,
                "/v3/btc/observation-history/resolve",
                &[],
                Some(query.encode_to_vec()),
                false,
                false,
            ),
        )
        .await
        .unwrap_or_else(|_| Err(Error::Limit("observation resolve deadline")))?;
        require(
            resolved.schema_version == "marketcow.btc-observation-range.v3"
                && resolved.query.as_ref() == Some(&query)
                && resolved.maximum_observation_id > 0
                && resolved.total_observations <= self.limits().history_rows,
            "observation query freeze identity",
        )?;
        Ok(ObservationTimeHistory {
            client: self.clone(),
            source,
            cursor: start,
            end,
            limit,
            high: resolved.maximum_observation_id,
            total: resolved.total_observations,
            active: None,
            active_end: start,
            index: 0,
            rows: 0,
            pages: 0,
            bytes: 0,
            deadline,
            closed: false,
        })
    }
}
impl ObservationTimeHistory {
    pub async fn next_page(&mut self) -> Result<Option<ObservationTimePage>> {
        if self.closed {
            self.close().await?;
            return Ok(None);
        }
        let result = tokio::time::timeout_at(self.deadline, self.next_inner())
            .await
            .unwrap_or_else(|_| Err(Error::Limit("observation query deadline")));
        if result.is_err() {
            let _ = self.close().await;
        }
        result
    }
    async fn next_inner(&mut self) -> Result<Option<ObservationTimePage>> {
        loop {
            if self.cursor >= self.end {
                require(
                    self.rows == self.total,
                    "observation frozen query count changed",
                )?;
                self.closed = true;
                return Ok(None);
            }
            if self.active.is_none() {
                if self.pages >= self.client.limits().history_pages {
                    return Err(Error::Limit("observation query pages"));
                }
                self.active_end = self.cursor.saturating_add(900_000).min(self.end);
                let mut owner = self.client.observation_history(
                    self.source,
                    self.cursor,
                    self.active_end,
                    self.limit,
                )?;
                owner.range.maximum_observation_id = Some(self.high);
                owner.deadline = owner.deadline.min(self.deadline);
                self.active = Some(owner);
            }
            let owner = self.active.as_mut().unwrap();
            let before = owner.bytes;
            let page = owner.next_page().await?;
            let Some(page) = page else {
                owner.close().await?; // Never open the next range with release pending.
                self.active = None;
                self.cursor = self.active_end;
                self.index += 1;
                continue;
            };
            self.rows = self
                .rows
                .checked_add(page.observations.len() as u64)
                .ok_or(Error::Limit("observation query rows"))?;
            self.bytes = self
                .bytes
                .checked_add(owner.bytes - before)
                .ok_or(Error::Limit("observation query bytes"))?;
            self.pages += 1;
            require(
                self.rows <= self.total
                    && self.rows <= self.client.limits().history_rows
                    && self.bytes <= self.client.limits().history_bytes
                    && self.pages <= self.client.limits().history_pages,
                "observation query cumulative budget",
            )?;
            let complete = page.complete && self.active_end == self.end;
            if complete {
                require(
                    self.rows == self.total,
                    "observation frozen query count changed",
                )?;
            }
            return Ok(Some(ObservationTimePage {
                page,
                segment_index: self.index,
                query_complete: complete,
            }));
        }
    }
    pub async fn close(&mut self) -> Result<()> {
        self.closed = true;
        if let Some(owner) = &mut self.active {
            owner.close().await?;
        }
        self.active = None;
        Ok(())
    }
}
