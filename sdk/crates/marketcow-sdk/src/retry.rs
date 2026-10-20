use crate::Error;
use std::time::{Duration, SystemTime};

pub(crate) fn retry_after(value: &str) -> Option<u64> {
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return seconds.checked_mul(1000);
    }
    let at = httpdate::parse_http_date(value).ok()?;
    Some(
        at.duration_since(SystemTime::now())
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64,
    )
}
pub(crate) fn delay_ms(base: u64, error: Option<&Error>) -> u64 {
    let minimum = error
        .and_then(|e| match e {
            Error::Remote { retry_after_ms, .. } => *retry_after_ms,
            Error::WebSocket(tokio_tungstenite::tungstenite::Error::Http(response)) => response
                .headers()
                .get("retry-after")
                .and_then(|h| h.to_str().ok())
                .and_then(retry_after),
            _ => None,
        })
        .unwrap_or(0);
    // Randomness is for de-synchronizing retries, never a content hash or ID.
    let random = uuid::Uuid::new_v4();
    let sample = u16::from_le_bytes([random.as_bytes()[0], random.as_bytes()[1]]) as u64;
    base.saturating_add(sample % (base / 4 + 1)).max(minimum)
}
pub(crate) fn duration(base: u64, error: &Error) -> Duration {
    Duration::from_millis(delay_ms(base, Some(error)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_after_supports_seconds_and_dates_without_ignoring_minimum() {
        assert_eq!(retry_after("10"), Some(10000));
        assert_eq!(retry_after("Wed, 21 Oct 2015 07:28:00 GMT"), Some(0));
        assert_eq!(retry_after("invalid"), None);
        let error = Error::Remote {
            status: 429,
            code: "capacity".into(),
            retryable: true,
            retry_after_ms: Some(60000),
            detail: None,
            failure: None,
        };
        assert!(delay_ms(250, Some(&error)) >= 60000);
    }
}
