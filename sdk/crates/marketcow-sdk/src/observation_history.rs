//! Indexed Chainlink observations. Exhausting stored rows is not proof of
//! continuous supplier coverage, and never grants decision freshness.
use crate::{Client, Error, Result, error::require, history::CLEANUP_TIME};
use marketcow_wire::{common::Source, history};
use prost::Message;
use reqwest::Method;
use std::time::Duration;
use tokio::time::Instant;
mod time;
pub use time::{ObservationTimeHistory, ObservationTimePage};

pub struct ObservationHistory {
    client: Client,
    range: history::ObservationRange,
    request_id: String,
    session: Option<String>,
    token: Option<String>,
    high: Option<u64>,
    last_id: u64,
    pages: u32,
    rows: u64,
    bytes: u64,
    deadline: Instant,
    complete: bool,
    stopped: bool,
    released: bool,
    release_error: Option<String>,
}

impl Client {
    /// One frozen range of at most sixteen minutes, [start,end).
    /// Ordinary price and TWAP60 are separate sources; neither substitutes for
    /// the other. This call does not start collection or repair missing data.
    pub fn observation_history(
        &self,
        source: Source,
        start: i64,
        end: i64,
        limit: u32,
    ) -> Result<ObservationHistory> {
        if self.is_closed() {
            return Err(Error::Cancelled);
        }
        let instrument = match source {
            Source::ChainlinkBtcUsd => "BTCUSD.CHAINLINK",
            Source::ChainlinkBtcUsdTwap60 => "BTCUSD.CHAINLINK.TWAP60",
            _ => {
                return Err(Error::Configuration(
                    "observation history requires Chainlink source".into(),
                ));
            }
        };
        require(
            start > 0 && end > start && end - start <= 960_000 && (1..=1000).contains(&limit),
            "observation history range",
        )?;
        Ok(ObservationHistory {
            client: self.clone(),
            range: history::ObservationRange {
                source: source as i32,
                instrument_id: instrument.into(),
                start_time_ms: start,
                end_time_ms: end,
                limit,
                maximum_observation_id: None,
            },
            request_id: uuid::Uuid::new_v4().to_string(),
            session: None,
            token: None,
            high: None,
            last_id: 0,
            pages: 0,
            rows: 0,
            bytes: 0,
            deadline: Instant::now() + self.limits().history_timeout.saturating_sub(CLEANUP_TIME),
            complete: false,
            stopped: false,
            released: false,
            release_error: None,
        })
    }
}

impl ObservationHistory {
    pub fn is_complete(&self) -> bool {
        self.complete
    }
    pub fn is_released(&self) -> bool {
        self.released
    }
    pub fn release_error(&self) -> Option<&str> {
        self.release_error.as_deref()
    }

    fn accept(&mut self, page: &history::ObservationPage, size: usize) -> Result<()> {
        require(
            page.schema_version == "marketcow.btc-observation-history.v3"
                && page.range.as_ref() == Some(&self.range)
                && page.request_id.as_deref() == Some(self.request_id.as_str())
                && uuid::Uuid::parse_str(&page.session_id).is_ok()
                && page.session_ttl_seconds == 600,
            "observation page identity",
        )?;
        if let Some(id) = &self.session {
            require(id == &page.session_id, "observation session changed")?;
        } else {
            self.session = Some(page.session_id.clone());
            self.client
                .register_lease(crate::leases::Lease::Observations(
                    history::ObservationReleaseRequest {
                        session_id: page.session_id.clone(),
                        range: Some(self.range.clone()),
                        request_id: Some(self.request_id.clone()),
                    },
                ))?;
            self.deadline = self.deadline.min(Instant::now() + Duration::from_secs(600));
        }
        require(
            self.high
                .is_none_or(|h| h == page.frozen_high_observation_id),
            "observation freeze changed",
        )?;
        require(
            page.complete == page.next_page_token.is_none()
                && (page.complete || !page.observations.is_empty())
                && page.observations.len() <= self.range.limit as usize,
            "observation pagination",
        )?;
        // Current source explicitly cannot prove the supplier prefix. Do not
        // silently upgrade that assertion when simply reading all DB rows.
        require(
            !page.coverage_verified
                && page.reason.as_deref() == Some("observation_prefix_coverage_unverified"),
            "unsupported observation coverage assertion",
        )?;
        let rows = self
            .rows
            .checked_add(page.observations.len() as u64)
            .ok_or(Error::Limit("observation rows"))?;
        let bytes = self
            .bytes
            .checked_add(size as u64)
            .ok_or(Error::Limit("observation bytes"))?;
        require(
            self.pages < self.client.limits().history_pages
                && rows <= self.client.limits().history_rows
                && bytes <= self.client.limits().history_bytes,
            "observation query budget",
        )?;
        let mut last = self.last_id;
        for record in &page.observations {
            require(
                record.observation_id > last
                    && record.observation_id <= page.frozen_high_observation_id
                    && !record.source_generation.is_empty(),
                "observation row identity",
            )?;
            let value = record
                .observation
                .as_ref()
                .ok_or_else(|| Error::Protocol("missing observation".into()))?;
            require(
                value.source_sequence.is_some_and(|s| s > 0)
                    && value.row_index.is_some()
                    && value.source_time_ms >= self.range.start_time_ms
                    && value.source_time_ms < self.range.end_time_ms,
                "observation source binding",
            )?;
            crate::price::decimal(&value.price, true)?;
            let evidence = value
                .evidence
                .as_ref()
                .ok_or_else(|| Error::Protocol("missing observation evidence".into()))?;
            require(
                !evidence.evidence_id.is_empty() && evidence.row_index == value.row_index,
                "observation evidence binding",
            )?;
            last = record.observation_id;
        }
        if let Some(token) = &page.next_page_token {
            require(
                token == &format!("{}:{}", page.session_id, last),
                "observation page token",
            )?;
        }
        self.high = Some(page.frozen_high_observation_id);
        self.last_id = last;
        self.rows = rows;
        self.bytes = bytes;
        self.pages += 1;
        self.token = page.next_page_token.clone();
        self.complete = page.complete;
        Ok(())
    }

    async fn fetch(&mut self) -> Result<history::ObservationPage> {
        let request = history::ObservationPageRequest {
            range: Some(self.range.clone()),
            page_token: self.token.clone(),
            request_id: Some(self.request_id.clone()),
        };
        for attempt in 0..4 {
            let result = tokio::time::timeout_at(
                self.deadline,
                self.client.request::<history::ObservationPage>(
                    Method::POST,
                    "/v3/btc/observation-history",
                    &[],
                    Some(request.encode_to_vec()),
                    false,
                    false,
                ),
            )
            .await
            .unwrap_or_else(|_| Err(Error::Limit("observation deadline")));
            match result {
                Ok((page, size)) => {
                    self.accept(&page, size)?;
                    return Ok(page);
                }
                Err(error) if error.retryable() && attempt < 3 => {
                    let delay = crate::retry::duration(250 * (1 << attempt), &error);
                    if Instant::now()
                        .checked_add(delay)
                        .is_none_or(|t| t >= self.deadline)
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

    pub async fn next_page(&mut self) -> Result<Option<history::ObservationPage>> {
        if self.stopped {
            self.close().await?;
            return Ok(None);
        }
        match self.fetch().await {
            Ok(page) => {
                if self.complete {
                    self.stopped = true;
                    let _ = self.close().await;
                }
                Ok(Some(page))
            }
            Err(error) => {
                let _ = self.close().await;
                Err(error)
            }
        }
    }
    pub async fn close(&mut self) -> Result<()> {
        self.stopped = true;
        if self.released {
            return Ok(());
        }
        let Some(id) = &self.session else {
            return Ok(());
        };
        let request = history::ObservationReleaseRequest {
            session_id: id.clone(),
            range: Some(self.range.clone()),
            request_id: Some(self.request_id.clone()),
        };
        let result = tokio::time::timeout(CLEANUP_TIME, release(&self.client, &request))
            .await
            .unwrap_or_else(|_| Err(Error::Limit("observation release deadline")));
        match result {
            Ok(()) => {
                self.client.forget_lease(id);
                self.released = true;
                self.release_error = None;
                Ok(())
            }
            Err(e) => {
                self.release_error = Some(e.to_string());
                Err(e)
            }
        }
    }
}

async fn release(client: &Client, request: &history::ObservationReleaseRequest) -> Result<()> {
    let (receipt, _) = client
        .request::<history::ObservationReleaseReceipt>(
            Method::DELETE,
            "/v3/btc/observation-history/session",
            &[],
            Some(request.encode_to_vec()),
            false,
            true,
        )
        .await?;
    require(
        receipt.schema_version == "marketcow.btc-observation-history-release.v3"
            && receipt.session_id == request.session_id
            && receipt.range == request.range
            && receipt.request_id == request.request_id,
        "observation release identity",
    )?;
    client.forget_lease(&request.session_id);
    Ok(())
}

impl Drop for ObservationHistory {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        let Some(id) = self.session.clone() else {
            return;
        };
        let Ok(_runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let client = self.client.clone();
        let Ok(permit) = client.0.cleanup_slots.clone().try_acquire_owned() else {
            return;
        };
        let request = history::ObservationReleaseRequest {
            session_id: id,
            range: Some(self.range.clone()),
            request_id: Some(self.request_id.clone()),
        };
        let _ = client.clone().spawn_tracked(
            async move {
                let _permit = permit;
                let _ = tokio::time::timeout(CLEANUP_TIME, release(&client, &request)).await;
            },
            true,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketcow_wire::{btc, common};

    fn owner() -> ObservationHistory {
        Client::builder("http://127.0.0.1:8793")
            .build()
            .unwrap()
            .observation_history(Source::ChainlinkBtcUsdTwap60, 1000, 3000, 1)
            .unwrap()
    }
    fn page(owner: &ObservationHistory, id: u64, complete: bool) -> history::ObservationPage {
        let session_id = "0d711d81-c50f-42aa-83bc-08504d81ccca".to_string();
        history::ObservationPage {
            schema_version: "marketcow.btc-observation-history.v3".into(),
            range: Some(owner.range.clone()),
            request_id: Some(owner.request_id.clone()),
            next_page_token: (!complete).then(|| format!("{session_id}:{id}")),
            session_id,
            session_ttl_seconds: 600,
            complete,
            coverage_verified: false,
            reason: Some("observation_prefix_coverage_unverified".into()),
            frozen_high_observation_id: 9,
            database_health_at_freeze: None,
            observations: vec![history::ObservationRecord {
                observation_id: id,
                source_generation: "generation-a".into(),
                observation: Some(btc::Observation {
                    source_sequence: Some(2),
                    row_index: Some(0),
                    source_time_ms: 2000,
                    price: "100.00".into(),
                    received_at_ns: None,
                    evidence: Some(common::EvidenceReference {
                        evidence_id: "original-report".into(),
                        row_index: Some(0),
                    }),
                }),
            }],
        }
    }
    #[tokio::test]
    async fn storage_ids_can_skip_without_claiming_supplier_coverage() {
        let mut owner = owner();
        let first = page(&owner, 2, false);
        owner.accept(&first, first.encoded_len()).unwrap();
        let last = page(&owner, 9, true);
        owner.accept(&last, last.encoded_len()).unwrap();
        assert!(owner.is_complete());
        assert!(!last.coverage_verified);
        owner.released = true; // Pure validator case, no HTTP cleanup.
    }
    #[tokio::test]
    async fn malformed_page_does_not_advance_prefix() {
        let mut owner = owner();
        let first = page(&owner, 2, false);
        owner.accept(&first, first.encoded_len()).unwrap();
        let mut last = page(&owner, 9, true);
        last.observations[0]
            .observation
            .as_mut()
            .unwrap()
            .source_time_ms = 3000;
        assert!(owner.accept(&last, last.encoded_len()).is_err());
        assert_eq!(owner.last_id, 2);
        assert_eq!(owner.pages, 1);
        last.observations[0]
            .observation
            .as_mut()
            .unwrap()
            .source_time_ms = 2000;
        last.frozen_high_observation_id = 10;
        assert!(owner.accept(&last, last.encoded_len()).is_err());
        owner.released = true;
    }
}
