//! Rust-owned provider job state machine. Workers are untrusted executors: only the holder of an
//! unexpired lease may advance a job or submit a content-addressed staging result.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use thiserror::Error;
use uuid::Uuid;

pub const JOB_SCHEMA_VERSION: &str = "marketcow.job.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Claimed,
    Running,
    Succeeded,
    FailedRetryable,
    FailedTerminal,
    Canceled,
}

impl JobStatus {
    pub fn terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::FailedTerminal | Self::Canceled
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobError {
    pub code: String,
    pub classification: String,
    pub redacted_message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedResult {
    pub relative_path: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub media_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderJob {
    pub schema_version: String,
    pub job_id: String,
    pub idempotency_key: String,
    pub job_type: String,
    pub request_schema: String,
    pub request_sha256: String,
    pub request: serde_json::Value,
    pub status: JobStatus,
    pub revision: u64,
    pub owner_id: Option<String>,
    pub lease_token: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub deadline: DateTime<Utc>,
    pub attempt: u32,
    pub max_attempts: u32,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub error: Option<JobError>,
    pub result: Option<StagedResult>,
    pub audit_actor: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitJob {
    pub idempotency_key: String,
    pub job_type: String,
    pub request_schema: String,
    pub request: serde_json::Value,
    pub deadline: DateTime<Utc>,
    pub max_attempts: u32,
    pub audit_actor: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum JobEngineError {
    #[error("job input is invalid")]
    InvalidInput,
    #[error("idempotency key conflicts with a different request")]
    IdempotencyConflict,
    #[error("job was not found")]
    NotFound,
    #[error("job transition is invalid")]
    InvalidTransition,
    #[error("lease token is invalid or expired")]
    LeaseRejected,
    #[error("job deadline has elapsed")]
    DeadlineElapsed,
    #[error("worker result is invalid")]
    InvalidResult,
}

#[derive(Debug, Default)]
pub struct JobEngine {
    jobs: BTreeMap<String, ProviderJob>,
    idempotency: HashMap<String, String>,
}

impl JobEngine {
    pub fn submit(
        &mut self,
        input: SubmitJob,
        now: DateTime<Utc>,
    ) -> Result<&ProviderJob, JobEngineError> {
        validate_submit(&input, now)?;
        let request_sha256 = sha256_json(&input.request)?;
        if let Some(job_id) = self.idempotency.get(&input.idempotency_key) {
            let existing = &self.jobs[job_id];
            if existing.job_type == input.job_type
                && existing.request_schema == input.request_schema
                && existing.request_sha256 == request_sha256
            {
                return Ok(existing);
            }
            return Err(JobEngineError::IdempotencyConflict);
        }
        let job_id = Uuid::new_v4().to_string();
        let job = ProviderJob {
            schema_version: JOB_SCHEMA_VERSION.into(),
            job_id: job_id.clone(),
            idempotency_key: input.idempotency_key.clone(),
            job_type: input.job_type,
            request_schema: input.request_schema,
            request_sha256,
            request: input.request,
            status: JobStatus::Pending,
            revision: 1,
            owner_id: None,
            lease_token: None,
            lease_expires_at: None,
            deadline: input.deadline,
            attempt: 0,
            max_attempts: input.max_attempts,
            created_at: now,
            started_at: None,
            finished_at: None,
            error: None,
            result: None,
            audit_actor: input.audit_actor,
        };
        self.idempotency
            .insert(input.idempotency_key, job_id.clone());
        self.jobs.insert(job_id.clone(), job);
        Ok(&self.jobs[&job_id])
    }

    pub fn claim(
        &mut self,
        job_id: &str,
        worker_id: &str,
        lease_duration: Duration,
        now: DateTime<Utc>,
    ) -> Result<&ProviderJob, JobEngineError> {
        if worker_id.is_empty() || lease_duration <= Duration::zero() {
            return Err(JobEngineError::InvalidInput);
        }
        let job = self.jobs.get_mut(job_id).ok_or(JobEngineError::NotFound)?;
        if now >= job.deadline {
            return Err(JobEngineError::DeadlineElapsed);
        }
        if job.status != JobStatus::Pending || job.attempt >= job.max_attempts {
            return Err(JobEngineError::InvalidTransition);
        }
        job.status = JobStatus::Claimed;
        job.revision += 1;
        job.attempt += 1;
        job.owner_id = Some(worker_id.into());
        job.lease_token = Some(Uuid::new_v4().to_string());
        job.lease_expires_at = Some((now + lease_duration).min(job.deadline));
        Ok(job)
    }

    pub fn claim_next(
        &mut self,
        worker_id: &str,
        capabilities: &[String],
        lease_duration: Duration,
        now: DateTime<Utc>,
    ) -> Result<Option<&ProviderJob>, JobEngineError> {
        let job_id = self
            .jobs
            .values()
            .find(|job| {
                job.status == JobStatus::Pending
                    && job.attempt < job.max_attempts
                    && now < job.deadline
                    && capabilities
                        .iter()
                        .any(|capability| capability == &job.job_type)
            })
            .map(|job| job.job_id.clone());
        match job_id {
            Some(job_id) => self
                .claim(&job_id, worker_id, lease_duration, now)
                .map(Some),
            None => Ok(None),
        }
    }

    pub fn start(
        &mut self,
        job_id: &str,
        lease_token: &str,
        now: DateTime<Utc>,
    ) -> Result<&ProviderJob, JobEngineError> {
        let job = self.leased_job_mut(job_id, lease_token, now)?;
        if job.status != JobStatus::Claimed {
            return Err(JobEngineError::InvalidTransition);
        }
        job.status = JobStatus::Running;
        job.revision += 1;
        job.started_at.get_or_insert(now);
        Ok(job)
    }

    pub fn succeed(
        &mut self,
        job_id: &str,
        lease_token: &str,
        result: StagedResult,
        now: DateTime<Utc>,
    ) -> Result<&ProviderJob, JobEngineError> {
        validate_result(&result)?;
        let job = self.leased_job_mut(job_id, lease_token, now)?;
        if job.status != JobStatus::Running {
            return Err(JobEngineError::InvalidTransition);
        }
        job.status = JobStatus::Succeeded;
        job.revision += 1;
        job.finished_at = Some(now);
        job.result = Some(result);
        clear_lease(job);
        Ok(job)
    }

    pub fn fail(
        &mut self,
        job_id: &str,
        lease_token: &str,
        error: JobError,
        retryable: bool,
        now: DateTime<Utc>,
    ) -> Result<&ProviderJob, JobEngineError> {
        if error.code.is_empty()
            || error.classification.is_empty()
            || error.redacted_message.len() > 2048
        {
            return Err(JobEngineError::InvalidInput);
        }
        let job = self.leased_job_mut(job_id, lease_token, now)?;
        if job.status != JobStatus::Claimed && job.status != JobStatus::Running {
            return Err(JobEngineError::InvalidTransition);
        }
        job.status = if retryable && job.attempt < job.max_attempts {
            JobStatus::FailedRetryable
        } else {
            JobStatus::FailedTerminal
        };
        job.revision += 1;
        job.error = Some(error);
        if job.status.terminal() {
            job.finished_at = Some(now);
        }
        clear_lease(job);
        Ok(job)
    }

    pub fn requeue(
        &mut self,
        job_id: &str,
        now: DateTime<Utc>,
    ) -> Result<&ProviderJob, JobEngineError> {
        let job = self.jobs.get_mut(job_id).ok_or(JobEngineError::NotFound)?;
        if job.status != JobStatus::FailedRetryable || now >= job.deadline {
            return Err(JobEngineError::InvalidTransition);
        }
        job.status = JobStatus::Pending;
        job.revision += 1;
        Ok(job)
    }

    pub fn requeue_retryable(&mut self, now: DateTime<Utc>) -> Vec<String> {
        let mut requeued = Vec::new();
        for job in self.jobs.values_mut() {
            if job.status == JobStatus::FailedRetryable && now < job.deadline {
                job.status = JobStatus::Pending;
                job.revision += 1;
                requeued.push(job.job_id.clone());
            }
        }
        requeued
    }

    pub fn expire_leases(&mut self, now: DateTime<Utc>) -> Vec<String> {
        let mut expired = Vec::new();
        for job in self.jobs.values_mut() {
            if matches!(job.status, JobStatus::Claimed | JobStatus::Running)
                && job.lease_expires_at.is_some_and(|deadline| now >= deadline)
            {
                job.status = if job.attempt < job.max_attempts && now < job.deadline {
                    JobStatus::FailedRetryable
                } else {
                    JobStatus::FailedTerminal
                };
                job.revision += 1;
                job.error = Some(JobError {
                    code: "worker_lease_expired".into(),
                    classification: "worker_timeout".into(),
                    redacted_message: "worker lease expired".into(),
                });
                if job.status.terminal() {
                    job.finished_at = Some(now);
                }
                clear_lease(job);
                expired.push(job.job_id.clone());
            }
        }
        expired
    }

    pub fn cancel(
        &mut self,
        job_id: &str,
        actor: &str,
        now: DateTime<Utc>,
    ) -> Result<&ProviderJob, JobEngineError> {
        if actor.is_empty() {
            return Err(JobEngineError::InvalidInput);
        }
        let job = self.jobs.get_mut(job_id).ok_or(JobEngineError::NotFound)?;
        if job.status.terminal() {
            return Err(JobEngineError::InvalidTransition);
        }
        job.status = JobStatus::Canceled;
        job.revision += 1;
        job.finished_at = Some(now);
        job.audit_actor = actor.into();
        clear_lease(job);
        Ok(job)
    }

    pub fn get(&self, job_id: &str) -> Option<&ProviderJob> {
        self.jobs.get(job_id)
    }

    fn leased_job_mut(
        &mut self,
        job_id: &str,
        lease_token: &str,
        now: DateTime<Utc>,
    ) -> Result<&mut ProviderJob, JobEngineError> {
        let job = self.jobs.get_mut(job_id).ok_or(JobEngineError::NotFound)?;
        if now >= job.deadline {
            return Err(JobEngineError::DeadlineElapsed);
        }
        if job.lease_token.as_deref() != Some(lease_token)
            || job.lease_expires_at.is_none_or(|expires| now >= expires)
        {
            return Err(JobEngineError::LeaseRejected);
        }
        Ok(job)
    }
}

fn validate_submit(input: &SubmitJob, now: DateTime<Utc>) -> Result<(), JobEngineError> {
    if input.idempotency_key.is_empty()
        || input.idempotency_key.len() > 256
        || input.job_type.is_empty()
        || input.request_schema.is_empty()
        || input.audit_actor.is_empty()
        || input.max_attempts == 0
        || input.max_attempts > 20
        || input.deadline <= now
    {
        return Err(JobEngineError::InvalidInput);
    }
    Ok(())
}

fn validate_result(result: &StagedResult) -> Result<(), JobEngineError> {
    let path = std::path::Path::new(&result.relative_path);
    if path.is_absolute()
        || result.relative_path.is_empty()
        || path.components().count() != 1
        || result.sha256.len() != 64
        || !result.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        || result.media_type.is_empty()
    {
        return Err(JobEngineError::InvalidResult);
    }
    Ok(())
}

fn sha256_json(value: &serde_json::Value) -> Result<String, JobEngineError> {
    serde_json::to_vec(value)
        .map(|bytes| hex::encode(Sha256::digest(bytes)))
        .map_err(|_| JobEngineError::InvalidInput)
}

fn clear_lease(job: &mut ProviderJob) {
    job.owner_id = None;
    job.lease_token = None;
    job.lease_expires_at = None;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 28, 4, 0, 0).unwrap()
    }

    fn submit() -> SubmitJob {
        SubmitJob {
            idempotency_key: "idem-1".into(),
            job_type: "provider.history".into(),
            request_schema: "marketcow.provider.history.v1".into(),
            request: serde_json::json!({"symbol":"AAPL.XNAS"}),
            deadline: now() + Duration::minutes(5),
            max_attempts: 2,
            audit_actor: "service:marketcowd".into(),
        }
    }

    #[test]
    fn idempotency_reuses_identical_job_and_rejects_changed_request() {
        let mut engine = JobEngine::default();
        let first = engine.submit(submit(), now()).unwrap().clone();
        let same = engine.submit(submit(), now()).unwrap();
        assert_eq!(same.job_id, first.job_id);
        let mut changed = submit();
        changed.request = serde_json::json!({"symbol":"MSFT.XNAS"});
        assert_eq!(
            engine.submit(changed, now()),
            Err(JobEngineError::IdempotencyConflict)
        );
    }

    #[test]
    fn lease_owner_completes_with_content_addressed_relative_result() {
        let mut engine = JobEngine::default();
        let job_id = engine.submit(submit(), now()).unwrap().job_id.clone();
        let claimed = engine
            .claim(&job_id, "worker-1", Duration::seconds(30), now())
            .unwrap()
            .clone();
        let token = claimed.lease_token.unwrap();
        engine.start(&job_id, &token, now()).unwrap();
        let done = engine
            .succeed(
                &job_id,
                &token,
                StagedResult {
                    relative_path: "result.json".into(),
                    sha256: "a".repeat(64),
                    size_bytes: 42,
                    media_type: "application/json".into(),
                },
                now() + Duration::seconds(1),
            )
            .unwrap();
        assert_eq!(done.status, JobStatus::Succeeded);
        assert!(done.lease_token.is_none());
    }

    #[test]
    fn expired_worker_cannot_submit_late_result_and_retry_is_bounded() {
        let mut engine = JobEngine::default();
        let job_id = engine.submit(submit(), now()).unwrap().job_id.clone();
        let token = engine
            .claim(&job_id, "worker-1", Duration::seconds(10), now())
            .unwrap()
            .lease_token
            .clone()
            .unwrap();
        engine.start(&job_id, &token, now()).unwrap();
        let expired = engine.expire_leases(now() + Duration::seconds(10));
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], job_id);
        assert_eq!(
            engine.get(&job_id).unwrap().status,
            JobStatus::FailedRetryable
        );
        assert_eq!(
            engine.succeed(
                &job_id,
                &token,
                StagedResult {
                    relative_path: "late.json".into(),
                    sha256: "b".repeat(64),
                    size_bytes: 1,
                    media_type: "application/json".into(),
                },
                now() + Duration::seconds(11),
            ),
            Err(JobEngineError::LeaseRejected)
        );
        engine
            .requeue(&job_id, now() + Duration::seconds(11))
            .unwrap();
        let second = engine
            .claim(
                &job_id,
                "worker-2",
                Duration::seconds(10),
                now() + Duration::seconds(11),
            )
            .unwrap();
        assert_eq!(second.attempt, 2);
        assert!(
            engine
                .expire_leases(now() + Duration::seconds(21))
                .contains(&job_id)
        );
        assert_eq!(
            engine.get(&job_id).unwrap().status,
            JobStatus::FailedTerminal
        );
    }

    #[test]
    fn path_escape_and_cancel_after_terminal_are_rejected() {
        let mut engine = JobEngine::default();
        let job_id = engine.submit(submit(), now()).unwrap().job_id.clone();
        let token = engine
            .claim(&job_id, "worker", Duration::seconds(30), now())
            .unwrap()
            .lease_token
            .clone()
            .unwrap();
        engine.start(&job_id, &token, now()).unwrap();
        assert_eq!(
            engine.succeed(
                &job_id,
                &token,
                StagedResult {
                    relative_path: "../escape".into(),
                    sha256: "a".repeat(64),
                    size_bytes: 1,
                    media_type: "application/json".into(),
                },
                now(),
            ),
            Err(JobEngineError::InvalidResult)
        );
        engine.cancel(&job_id, "operator", now()).unwrap();
        assert_eq!(
            engine.cancel(&job_id, "operator", now()),
            Err(JobEngineError::InvalidTransition)
        );
    }

    #[test]
    fn claim_next_requires_an_explicit_worker_capability() {
        let mut engine = JobEngine::default();
        let job_id = engine.submit(submit(), now()).unwrap().job_id.clone();
        assert!(
            engine
                .claim_next(
                    "worker",
                    &["provider.fundamentals".into()],
                    Duration::seconds(30),
                    now(),
                )
                .unwrap()
                .is_none()
        );
        let claimed = engine
            .claim_next(
                "worker",
                &["provider.history".into()],
                Duration::seconds(30),
                now(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(claimed.job_id, job_id);
    }
}
