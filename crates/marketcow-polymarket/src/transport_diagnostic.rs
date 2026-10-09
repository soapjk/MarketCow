//! Bounded receive-side diagnostics for the existing market WebSocket.
//!
//! These observations are never source facts.  They use the process-wide
//! non-blocking diagnostic writer and cannot delay admission for log I/O.

use crate::{RawTransportFrame, content_sha256};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

const WINDOW: Duration = Duration::from_secs(5);
const SLOW_SAMPLES_PER_WINDOW: u8 = 8;
const BUCKETS_MS: [u64; 11] = [1, 5, 10, 50, 100, 250, 500, 1_000, 5_000, 30_000, 120_000];
static NEXT_CONNECTION: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct Distribution {
    count: u64,
    sum: u128,
    max: u64,
    buckets: [u64; BUCKETS_MS.len() + 1],
}

impl Distribution {
    fn add(&mut self, value: u64) {
        self.count += 1;
        self.sum += u128::from(value);
        self.max = self.max.max(value);
        let bucket = BUCKETS_MS.partition_point(|bound| value > *bound);
        self.buckets[bucket] += 1;
    }

    fn json(&self) -> Value {
        json!({
            "count":self.count,
            "mean_ms":if self.count == 0 { None } else { Some((self.sum / u128::from(self.count)) as u64) },
            "max_ms":self.max,
            "bucket_upper_ms":BUCKETS_MS,
            "bucket_counts":self.buckets,
        })
    }
}

#[derive(Default)]
pub(super) struct SendTiming {
    pub shared_slot_wait: Duration,
    pub shared_byte_wait: Duration,
    pub lane_byte_wait: Duration,
    pub queue_slot_wait: Duration,
}

pub(super) struct TransportDiagnostic {
    connection_id: String,
    source_tokens: Vec<String>,
    window_started: Instant,
    window_started_at: DateTime<Utc>,
    frame_sequence: u64,
    frames: u64,
    wire_bytes: u64,
    source_time_missing: u64,
    source_time_invalid: u64,
    source_time_future: u64,
    slow_samples: u8,
    source_age: Distribution,
    socket_next_poll: Distribution,
    json_decode: Distribution,
    shared_slot_wait: Distribution,
    shared_byte_wait: Distribution,
    lane_byte_wait: Distribution,
    queue_slot_wait: Distribution,
}

impl TransportDiagnostic {
    pub(super) fn new(tokens: &std::collections::BTreeSet<String>) -> Self {
        let opened_at = Utc::now();
        Self {
            connection_id: format!(
                "{}-{}",
                opened_at.timestamp_micros(),
                NEXT_CONNECTION.fetch_add(1, Ordering::Relaxed)
            ),
            source_tokens: tokens.iter().cloned().collect(),
            window_started: Instant::now(),
            window_started_at: opened_at,
            frame_sequence: 0,
            frames: 0,
            wire_bytes: 0,
            source_time_missing: 0,
            source_time_invalid: 0,
            source_time_future: 0,
            slow_samples: 0,
            source_age: Distribution::default(),
            socket_next_poll: Distribution::default(),
            json_decode: Distribution::default(),
            shared_slot_wait: Distribution::default(),
            shared_byte_wait: Distribution::default(),
            lane_byte_wait: Distribution::default(),
            queue_slot_wait: Distribution::default(),
        }
    }

    pub(super) fn record_batch(
        &mut self,
        frames: &[RawTransportFrame],
        wire_bytes: usize,
        poll: Duration,
        decode: Duration,
    ) {
        self.frames += frames.len() as u64;
        self.wire_bytes = self.wire_bytes.saturating_add(wire_bytes as u64);
        self.socket_next_poll.add(duration_ms(poll));
        self.json_decode.add(duration_ms(decode));
        for frame in frames {
            self.frame_sequence += 1;
            let Some(timestamp) = frame.raw_payload.get("timestamp") else {
                self.source_time_missing += 1;
                continue;
            };
            // A missing/invalid supplier clock is not replaced by receipt time.
            let source_at = timestamp
                .as_i64()
                .or_else(|| timestamp.as_str().and_then(|s| s.parse::<i64>().ok()))
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .or_else(|| {
                    timestamp
                        .as_str()
                        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                        .map(|at| at.with_timezone(&Utc))
                });
            let Some(source_at) = source_at else {
                self.source_time_invalid += 1;
                continue;
            };
            let age_ms = frame
                .received_at
                .signed_duration_since(source_at)
                .num_milliseconds();
            if age_ms < 0 {
                self.source_time_future += 1;
                continue;
            }
            self.source_age.add(age_ms as u64);
            if age_ms >= 1_000
                && self.slow_samples < SLOW_SAMPLES_PER_WINDOW
                && (self.slow_samples == 0 || self.frame_sequence % 256 == 0)
            {
                self.slow_samples += 1;
                // This is the same canonical raw JSON hash used by the
                // normalizer, calculated for at most eight frames per window.
                let raw_sha256 = content_sha256(&frame.raw_payload).ok();
                crate::async_eprintln!(
                    "{}",
                    json!({
                        "stage":"polymarket_receive_lag_sample",
                        "connection_id":self.connection_id,
                        "source_tokens":self.source_tokens,
                        "frame_sequence":self.frame_sequence,
                        "event_type":frame.raw_payload.get("event_type"),
                        "raw_payload_sha256":raw_sha256,
                        "source_at":source_at,
                        "socket_next_return_received_at":frame.received_at,
                        "source_to_receive_ms_uncalibrated":age_ms,
                        "clock_error_bound_ms":Value::Null,
                    })
                );
            }
        }
    }

    pub(super) fn record_send(&mut self, timing: &SendTiming) {
        self.shared_slot_wait
            .add(duration_ms(timing.shared_slot_wait));
        self.shared_byte_wait
            .add(duration_ms(timing.shared_byte_wait));
        self.lane_byte_wait.add(duration_ms(timing.lane_byte_wait));
        self.queue_slot_wait
            .add(duration_ms(timing.queue_slot_wait));
    }

    pub(super) fn set_tokens(&mut self, tokens: &std::collections::BTreeSet<String>) {
        self.flush();
        self.source_tokens = tokens.iter().cloned().collect();
    }

    pub(super) fn flush_if_due(&mut self) {
        if self.window_started.elapsed() >= WINDOW {
            self.flush();
        }
    }

    pub(super) fn flush(&mut self) {
        if self.frames == 0 {
            return;
        }
        crate::async_eprintln!(
            "{}",
            json!({
                "stage":"polymarket_receive_stage_window",
                "connection_id":self.connection_id,
                "source_tokens":self.source_tokens,
                "started_at":self.window_started_at,
                "ended_at":Utc::now(),
                "elapsed_ms":duration_ms(self.window_started.elapsed()),
                "frames":self.frames,
                "wire_bytes":self.wire_bytes,
                "source_time_missing":self.source_time_missing,
                "source_time_invalid":self.source_time_invalid,
                "source_time_future":self.source_time_future,
                "source_to_receive_ms_uncalibrated":self.source_age.json(),
                "socket_next_poll_ms_not_network_time":self.socket_next_poll.json(),
                "json_decode_ms":self.json_decode.json(),
                "shared_slot_wait_ms":self.shared_slot_wait.json(),
                "shared_byte_wait_ms":self.shared_byte_wait.json(),
                "lane_byte_wait_ms":self.lane_byte_wait.json(),
                "queue_slot_wait_ms":self.queue_slot_wait.json(),
                "clock_error_bound_ms":Value::Null,
            })
        );
        self.window_started = Instant::now();
        self.window_started_at = Utc::now();
        self.frames = 0;
        self.wire_bytes = 0;
        self.source_time_missing = 0;
        self.source_time_invalid = 0;
        self.source_time_future = 0;
        self.slow_samples = 0;
        self.source_age = Distribution::default();
        self.socket_next_poll = Distribution::default();
        self.json_decode = Distribution::default();
        self.shared_slot_wait = Distribution::default();
        self.shared_byte_wait = Distribution::default();
        self.lane_byte_wait = Distribution::default();
        self.queue_slot_wait = Distribution::default();
    }
}

impl Drop for TransportDiagnostic {
    fn drop(&mut self) {
        self.flush();
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros() / 1_000).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn source_age_never_uses_receipt_as_missing_supplier_time() {
        let mut diagnostic =
            TransportDiagnostic::new(&["up".to_owned(), "down".to_owned()].into_iter().collect());
        let received_at = DateTime::from_timestamp_millis(1_000_000).unwrap();
        diagnostic.record_batch(
            &[
                RawTransportFrame {
                    raw_payload: json!({"event_type":"book"}),
                    received_at,
                },
                RawTransportFrame {
                    raw_payload: json!({"event_type":"book","timestamp":"invalid"}),
                    received_at,
                },
                RawTransportFrame {
                    raw_payload: json!({"event_type":"book","timestamp":"999000"}),
                    received_at,
                },
            ],
            1024,
            Duration::from_millis(7),
            Duration::from_millis(2),
        );
        assert_eq!(diagnostic.source_time_missing, 1);
        assert_eq!(diagnostic.source_time_invalid, 1);
        assert_eq!(diagnostic.source_age.count, 1);
        assert_eq!(diagnostic.source_age.max, 1_000);
        assert_eq!(diagnostic.socket_next_poll.max, 7);
        assert_eq!(diagnostic.json_decode.max, 2);
    }

    #[test]
    fn histogram_buckets_keep_all_samples() {
        let mut distribution = Distribution::default();
        for value in [0, 1, 2, 1_000, 120_001] {
            distribution.add(value);
        }
        assert_eq!(distribution.count, 5);
        assert_eq!(distribution.buckets.iter().sum::<u64>(), 5);
        assert_eq!(distribution.max, 120_001);
    }
}
