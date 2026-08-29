use super::{
    DurabilityError, DurableRealtimeWriter, RealtimeCheckpoint, RealtimeError, ReplayFrame,
    StreamEvent, SubscriptionFilter, TransportOutput,
};
use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use thiserror::Error;
use tokio::sync::mpsc;

pub const REALTIME_HUB_PROJECTION_VERSION: &str = "marketcow.realtime.hub-projection.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RealtimeHubHealth {
    Starting,
    Connected {
        attempt: u32,
    },
    Ready {
        attempt: u32,
    },
    Degraded {
        attempt: u32,
        reason: String,
        retryable: bool,
    },
    FailedClosed {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealtimeHubProjection {
    pub schema_version: String,
    pub stream_id: String,
    pub config_revision: String,
    pub public_sequence: u64,
    pub wal_cursor: u64,
    pub health: RealtimeHubHealth,
    pub replay: Vec<StreamEvent>,
    pub updated_at: DateTime<Utc>,
}

impl RealtimeHubProjection {
    pub fn replay_after(
        &self,
        stream_id: &str,
        after: u64,
        filter: &SubscriptionFilter,
    ) -> Result<Vec<ReplayFrame>, RealtimeError> {
        if stream_id != self.stream_id {
            return Err(RealtimeError::StreamChanged);
        }
        if after > self.public_sequence {
            return Err(RealtimeError::CursorAhead {
                current_sequence: self.public_sequence,
            });
        }
        let earliest = self
            .replay
            .first()
            .map_or(self.public_sequence.saturating_add(1), |event| {
                event.sequence
            });
        if after.saturating_add(1) < earliest {
            return Err(RealtimeError::GapUnrecoverable {
                earliest_sequence: earliest,
            });
        }
        Ok(self
            .replay
            .iter()
            .filter(|event| event.sequence > after)
            .map(|event| {
                if filter.matches(&event.event) {
                    ReplayFrame::Event {
                        stream: Box::new(event.clone()),
                    }
                } else {
                    ReplayFrame::SequenceWatermark {
                        stream_id: self.stream_id.clone(),
                        sequence: event.sequence,
                    }
                }
            })
            .collect())
    }
}

#[derive(Clone)]
pub struct RealtimeHubReader {
    projection: Arc<ArcSwap<RealtimeHubProjection>>,
}

impl RealtimeHubReader {
    pub fn snapshot(&self) -> Arc<RealtimeHubProjection> {
        self.projection.load_full()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Starting,
    Connected { attempt: u32 },
    Ready { attempt: u32 },
    Degraded { attempt: u32, retryable: bool },
    FailedClosed,
}

pub struct DurableRealtimeHub {
    writer: DurableRealtimeWriter,
    projection: Arc<ArcSwap<RealtimeHubProjection>>,
    lifecycle: Lifecycle,
    last_attempt: u32,
}

impl DurableRealtimeHub {
    pub fn open(
        root: impl AsRef<Path>,
        stream_id: &str,
        config_revision: &str,
        max_segment_bytes: u64,
        replay_capacity: usize,
    ) -> Result<Self, RealtimeHubError> {
        let writer = DurableRealtimeWriter::open(
            root,
            stream_id,
            config_revision,
            max_segment_bytes,
            replay_capacity,
        )?;
        let projection = Arc::new(ArcSwap::from_pointee(projection_from_writer(
            &writer,
            stream_id,
            config_revision,
            RealtimeHubHealth::Starting,
        )));
        Ok(Self {
            writer,
            projection,
            lifecycle: Lifecycle::Starting,
            last_attempt: 0,
        })
    }

    pub fn reader(&self) -> RealtimeHubReader {
        RealtimeHubReader {
            projection: self.projection.clone(),
        }
    }

    /// Consume a transport output and publish only durably committed events to a bounded gateway
    /// queue. An event batch reserves its full worst-case capacity before the first WAL append, so
    /// backpressure rejects the entire upstream frame without partial ingestion.
    pub fn ingest(
        &mut self,
        output: TransportOutput,
        gateway: &mpsc::Sender<StreamEvent>,
    ) -> Result<usize, RealtimeHubError> {
        if self.lifecycle == Lifecycle::FailedClosed {
            return Err(RealtimeHubError::HubFailedClosed);
        }
        match output {
            TransportOutput::Connected { attempt } => {
                let valid = match self.lifecycle {
                    Lifecycle::Starting => attempt == 1,
                    Lifecycle::Degraded {
                        attempt: previous,
                        retryable: true,
                    } => previous.checked_add(1) == Some(attempt),
                    _ => false,
                };
                if !valid || attempt <= self.last_attempt {
                    return self.fail("invalid_connected_transition");
                }
                self.lifecycle = Lifecycle::Connected { attempt };
                self.last_attempt = attempt;
                self.store(RealtimeHubHealth::Connected { attempt });
                Ok(0)
            }
            TransportOutput::SubscriptionsReady { attempt } => {
                if self.lifecycle != (Lifecycle::Connected { attempt }) {
                    return self.fail("subscriptions_ready_before_connected");
                }
                self.lifecycle = Lifecycle::Ready { attempt };
                self.store(RealtimeHubHealth::Ready { attempt });
                Ok(0)
            }
            TransportOutput::Events { attempt, events } => {
                if self.lifecycle != (Lifecycle::Ready { attempt }) || events.is_empty() {
                    return self.fail("events_before_subscriptions_ready");
                }
                let permits = match gateway.try_reserve_many(events.len()) {
                    Ok(permits) => permits,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        return self.fail("gateway_backpressure");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        return self.fail("gateway_closed");
                    }
                };
                let outcomes = match self.writer.apply_batch(events) {
                    Ok(outcomes) => outcomes,
                    Err(error) => {
                        self.fail_state("authoritative_persistence_failed");
                        return Err(RealtimeHubError::Durability(error));
                    }
                };
                self.store(RealtimeHubHealth::Ready { attempt });
                let mut published = 0;
                for (outcome, permit) in outcomes.into_iter().zip(permits) {
                    if let Some(event) = outcome.published {
                        permit.send(event);
                        published += 1;
                    }
                }
                Ok(published)
            }
            TransportOutput::Degraded {
                attempt,
                reason,
                retryable,
            } => {
                let valid = match self.lifecycle {
                    Lifecycle::Starting => attempt == 1,
                    Lifecycle::Connected { attempt: active }
                    | Lifecycle::Ready { attempt: active } => active == attempt,
                    _ => false,
                };
                if !valid || reason.is_empty() || attempt < self.last_attempt {
                    return self.fail("invalid_degraded_transition");
                }
                self.last_attempt = attempt;
                self.lifecycle = Lifecycle::Degraded { attempt, retryable };
                self.store(RealtimeHubHealth::Degraded {
                    attempt,
                    reason,
                    retryable,
                });
                Ok(0)
            }
        }
    }

    pub fn checkpoint(&self) -> Result<RealtimeCheckpoint, RealtimeHubError> {
        if self.lifecycle == Lifecycle::FailedClosed {
            return Err(RealtimeHubError::HubFailedClosed);
        }
        Ok(self.writer.checkpoint()?)
    }

    /// Permanently close this in-process writer after an external owner task fails. Recovery must
    /// reopen and replay the durable state in a fresh process; callers cannot clear this state.
    pub fn fail_closed(&mut self, reason: &str) {
        let reason = if reason.is_empty() {
            "unspecified_owner_failure"
        } else {
            reason
        };
        self.lifecycle = Lifecycle::FailedClosed;
        self.store(RealtimeHubHealth::FailedClosed {
            reason: reason.into(),
        });
    }

    fn fail<T>(&mut self, reason: &'static str) -> Result<T, RealtimeHubError> {
        self.fail_state(reason);
        Err(RealtimeHubError::InvalidTransition(reason))
    }

    fn fail_state(&mut self, reason: &'static str) {
        self.lifecycle = Lifecycle::FailedClosed;
        self.store(RealtimeHubHealth::FailedClosed {
            reason: reason.into(),
        });
    }

    fn store(&self, health: RealtimeHubHealth) {
        let current = self.projection.load();
        self.projection.store(Arc::new(projection_from_writer(
            &self.writer,
            &current.stream_id,
            &current.config_revision,
            health,
        )));
    }
}

fn projection_from_writer(
    writer: &DurableRealtimeWriter,
    stream_id: &str,
    config_revision: &str,
    health: RealtimeHubHealth,
) -> RealtimeHubProjection {
    RealtimeHubProjection {
        schema_version: REALTIME_HUB_PROJECTION_VERSION.into(),
        stream_id: stream_id.into(),
        config_revision: config_revision.into(),
        public_sequence: writer.sequence(),
        wal_cursor: writer.wal_cursor(),
        health,
        replay: writer.replay_snapshot(),
        updated_at: Utc::now(),
    }
}

#[derive(Debug, Error)]
pub enum RealtimeHubError {
    #[error("realtime hub is fail-closed and requires restart/replay")]
    HubFailedClosed,
    #[error("invalid realtime transport lifecycle transition: {0}")]
    InvalidTransition(&'static str),
    #[error(transparent)]
    Durability(#[from] DurabilityError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataType, HyperliquidNormalizer};
    use chrono::TimeZone;
    use serde_json::json;
    use std::collections::BTreeSet;
    use tempfile::tempdir;

    fn event(sequence: i64) -> super::super::NormalizedProviderEvent {
        let received = Utc.timestamp_millis_opt(1_700_000_001_000).unwrap();
        HyperliquidNormalizer::new([("BTC-PERP.HYPL".into(), "BTC".into())], 10_000)
            .unwrap()
            .normalize(
                json!({"channel":"trades","data":[{
                    "coin":"BTC","time":1_700_000_000_000_i64 + sequence,
                    "px":format!("65000.{sequence}"),"sz":"0.1","side":"B","tid":sequence
                }]}),
                received,
            )
            .unwrap()
            .remove(0)
    }

    fn open(root: &Path) -> DurableRealtimeHub {
        DurableRealtimeHub::open(root, "hyperliquid-main", "config-v1", 1_024, 2).unwrap()
    }

    fn ready(hub: &mut DurableRealtimeHub, gateway: &mpsc::Sender<StreamEvent>, attempt: u32) {
        hub.ingest(TransportOutput::Connected { attempt }, gateway)
            .unwrap();
        hub.ingest(TransportOutput::SubscriptionsReady { attempt }, gateway)
            .unwrap();
    }

    #[test]
    fn immutable_projection_advances_only_after_durable_apply() {
        let directory = tempdir().unwrap();
        let mut hub = open(&directory.path().join("runtime"));
        let reader = hub.reader();
        let before = reader.snapshot();
        let (gateway, mut receiver) = mpsc::channel(4);
        ready(&mut hub, &gateway, 1);
        assert_eq!(
            hub.ingest(
                TransportOutput::Events {
                    attempt: 1,
                    events: vec![event(1)],
                },
                &gateway,
            )
            .unwrap(),
            1
        );
        let after = reader.snapshot();
        assert_eq!(before.public_sequence, 0);
        assert_eq!(before.wal_cursor, 0);
        assert_eq!(after.public_sequence, 1);
        assert_eq!(after.wal_cursor, 1);
        assert_eq!(receiver.try_recv().unwrap().sequence, 1);
    }

    #[test]
    fn full_gateway_rejects_entire_frame_before_wal_append() {
        let directory = tempdir().unwrap();
        let mut hub = open(&directory.path().join("runtime"));
        let reader = hub.reader();
        let (gateway, _receiver) = mpsc::channel(1);
        ready(&mut hub, &gateway, 1);
        assert!(matches!(
            hub.ingest(
                TransportOutput::Events {
                    attempt: 1,
                    events: vec![event(1), event(2)],
                },
                &gateway,
            ),
            Err(RealtimeHubError::InvalidTransition("gateway_backpressure"))
        ));
        let snapshot = reader.snapshot();
        assert_eq!(snapshot.public_sequence, 0);
        assert_eq!(snapshot.wal_cursor, 0);
        assert!(matches!(
            snapshot.health,
            RealtimeHubHealth::FailedClosed { .. }
        ));
    }

    #[test]
    fn lifecycle_is_strict_and_failed_hub_cannot_resume_in_place() {
        let directory = tempdir().unwrap();
        let mut hub = open(&directory.path().join("runtime"));
        let (gateway, _receiver) = mpsc::channel(4);
        assert!(matches!(
            hub.ingest(
                TransportOutput::Events {
                    attempt: 1,
                    events: vec![event(1)]
                },
                &gateway
            ),
            Err(RealtimeHubError::InvalidTransition(_))
        ));
        assert!(matches!(
            hub.ingest(TransportOutput::Connected { attempt: 1 }, &gateway),
            Err(RealtimeHubError::HubFailedClosed)
        ));
    }

    #[test]
    fn connection_failure_before_connected_can_reconnect_without_skipping_attempts() {
        let directory = tempdir().unwrap();
        let mut hub = open(&directory.path().join("runtime"));
        let (gateway, _receiver) = mpsc::channel(4);
        hub.ingest(
            TransportOutput::Degraded {
                attempt: 1,
                reason: "connect_failed".into(),
                retryable: true,
            },
            &gateway,
        )
        .unwrap();
        assert_eq!(
            hub.reader().snapshot().health,
            RealtimeHubHealth::Degraded {
                attempt: 1,
                reason: "connect_failed".into(),
                retryable: true,
            }
        );
        hub.ingest(TransportOutput::Connected { attempt: 2 }, &gateway)
            .unwrap();
    }

    #[test]
    fn reconnect_duplicate_and_restart_preserve_contiguous_gateway_state() {
        let directory = tempdir().unwrap();
        let root = directory.path().join("runtime");
        let (gateway, mut receiver) = mpsc::channel(8);
        let mut hub = open(&root);
        ready(&mut hub, &gateway, 1);
        let original = event(1);
        hub.ingest(
            TransportOutput::Events {
                attempt: 1,
                events: vec![original.clone()],
            },
            &gateway,
        )
        .unwrap();
        hub.ingest(
            TransportOutput::Degraded {
                attempt: 1,
                reason: "upstream_disconnected".into(),
                retryable: true,
            },
            &gateway,
        )
        .unwrap();
        ready(&mut hub, &gateway, 2);
        assert_eq!(
            hub.ingest(
                TransportOutput::Events {
                    attempt: 2,
                    events: vec![original],
                },
                &gateway,
            )
            .unwrap(),
            0
        );
        hub.checkpoint().unwrap();
        assert_eq!(receiver.try_recv().unwrap().sequence, 1);
        assert!(receiver.try_recv().is_err());
        drop(hub);

        let recovered = open(&root);
        let snapshot = recovered.reader().snapshot();
        assert_eq!(snapshot.public_sequence, 1);
        assert_eq!(snapshot.wal_cursor, 2);
        let frames = snapshot
            .replay_after(
                "hyperliquid-main",
                0,
                &SubscriptionFilter {
                    instruments: BTreeSet::from(["BTC-PERP.HYPL".into()]),
                    data_types: BTreeSet::from([DataType::Trade]),
                },
            )
            .unwrap();
        assert!(matches!(frames.as_slice(), [ReplayFrame::Event { .. }]));
    }
}
