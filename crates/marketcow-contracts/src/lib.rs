//! Versioned public and worker-facing contracts; no runtime, storage, or provider dependency.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const PREDICTION_MARKET_CONTRACT: &str = "marketcow.prediction_market.v1";
pub const LIVE_SCHEMA_VERSION: &str = "marketcow.polymarket.live.v2";
pub const WORKER_PROTOCOL_VERSION: &str = "marketcow.worker.v1";
pub const MAX_WORKER_FRAME_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineErrorDetail {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineErrorEnvelope {
    pub detail: MachineErrorDetail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeDiscovery {
    pub schema_version: String,
    pub active_scope_id: String,
    pub scope_status: String,
    pub real_order_submission_enabled: bool,
}

impl ScopeDiscovery {
    pub fn shadow(scope_id: String) -> Self {
        Self {
            schema_version: "marketcow.polymarket.scope-discovery.v1".into(),
            active_scope_id: scope_id,
            scope_status: "shadow".into(),
            real_order_submission_enabled: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventContractFields {
    pub cursor: u64,
    pub event_id: String,
    pub event_type: String,
    pub canonical_payload: serde_json::Value,
    pub canonical_payload_sha256: String,
    pub raw_payload: serde_json::Value,
    pub raw_payload_sha256: String,
    pub applied: bool,
    pub fail_closed_reason: Option<String>,
    pub gaps: Vec<serde_json::Value>,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerFrame {
    pub protocol_version: String,
    pub message_id: String,
    #[serde(flatten)]
    pub message: WorkerMessage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "message_type", rename_all = "snake_case")]
pub enum WorkerMessage {
    Hello {
        worker_id: String,
        worker_revision: String,
        nonce: String,
        capabilities: Vec<String>,
    },
    HelloAck {
        daemon_revision: String,
        nonce: String,
        maximum_frame_bytes: usize,
    },
    Poll,
    NoWork,
    Task {
        job_id: String,
        lease_token: String,
        deadline: DateTime<Utc>,
        job_type: String,
        request_schema: String,
        request_sha256: String,
        request: serde_json::Value,
        staging_path: String,
    },
    Start {
        job_id: String,
        lease_token: String,
    },
    Complete {
        job_id: String,
        lease_token: String,
        relative_path: String,
        sha256: String,
        size_bytes: u64,
        media_type: String,
    },
    Fail {
        job_id: String,
        lease_token: String,
        code: String,
        classification: String,
        redacted_message: String,
        retryable: bool,
    },
    JobState {
        job_id: String,
        status: String,
        revision: u64,
    },
    Error {
        code: String,
        retryable: bool,
    },
}

impl WorkerFrame {
    pub fn new(message_id: impl Into<String>, message: WorkerMessage) -> Self {
        Self {
            protocol_version: WORKER_PROTOCOL_VERSION.into(),
            message_id: message_id.into(),
            message,
        }
    }
}

pub fn validate_provider_neutral_fixture(value: &serde_json::Value) -> Result<(), String> {
    if value.get("contract_version").and_then(|item| item.as_str())
        != Some(PREDICTION_MARKET_CONTRACT)
    {
        return Err("prediction-market contract version mismatch".into());
    }
    if value.get("schema_version").and_then(|item| item.as_str()) != Some(LIVE_SCHEMA_VERSION) {
        return Err("live schema version mismatch".into());
    }
    let required = value
        .pointer("/events/required_envelope_fields")
        .and_then(|item| item.as_array())
        .ok_or_else(|| "fixture lacks required event fields".to_string())?;
    for field in [
        "cursor",
        "event_id",
        "event_type",
        "canonical_payload",
        "canonical_payload_sha256",
        "raw_payload",
        "raw_payload_sha256",
        "applied",
        "gaps",
    ] {
        if !required.iter().any(|item| item.as_str() == Some(field)) {
            return Err(format!("fixture does not require {field}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_provider_neutral_golden_contract_is_accepted() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/polymarket-live-provider-neutral-v2.json"
        ))
        .unwrap();
        validate_provider_neutral_fixture(&fixture).unwrap();
        assert_eq!(fixture["checkpoint_resume"]["checkpoint_cursor"], 11);
    }

    #[test]
    fn scope_and_error_shapes_are_stable_and_orders_are_disabled() {
        let scope = serde_json::to_value(ScopeDiscovery::shadow("scope".into())).unwrap();
        assert_eq!(
            scope["schema_version"],
            "marketcow.polymarket.scope-discovery.v1"
        );
        assert_eq!(scope["real_order_submission_enabled"], false);
        let error = MachineErrorEnvelope {
            detail: MachineErrorDetail {
                code: "cursor_gap".into(),
                message: "gap".into(),
                retryable: true,
                request_id: "request-1".into(),
            },
        };
        assert_eq!(
            serde_json::to_value(error).unwrap()["detail"]["code"],
            "cursor_gap"
        );
    }

    #[test]
    fn worker_protocol_is_versioned_typed_and_decimal_agnostic() {
        let frame = WorkerFrame::new(
            "message-1",
            WorkerMessage::Task {
                job_id: "job-1".into(),
                lease_token: "lease-secret".into(),
                deadline: DateTime::parse_from_rfc3339("2026-08-28T04:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                job_type: "provider.history".into(),
                request_schema: "marketcow.provider.history.v1".into(),
                request_sha256: "a".repeat(64),
                request: serde_json::json!({"price":"0.100000000000000001"}),
                staging_path: "/tmp/staging/job-1".into(),
            },
        );
        let value = serde_json::to_value(&frame).unwrap();
        assert_eq!(value["protocol_version"], WORKER_PROTOCOL_VERSION);
        assert_eq!(value["message_type"], "task");
        assert_eq!(value["request"]["price"], "0.100000000000000001");
        assert_eq!(serde_json::from_value::<WorkerFrame>(value).unwrap(), frame);
    }
}
