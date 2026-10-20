use crate::Client;
use std::sync::atomic::Ordering;
/// Local application-payload counters, not TLS/IP bytes or a provider bill.
#[derive(Clone, Copy, Debug, Default)]
pub struct Usage {
    pub http_requests: u64,
    pub http_response_bytes: u64,
    pub price_message_bytes: u64,
    pub market_message_bytes: u64,
    pub queued_event_bytes: usize,
}
impl Client {
    pub fn usage(&self) -> Usage {
        Usage {
            http_requests: self.0.http_requests.load(Ordering::Relaxed),
            http_response_bytes: self.0.http_response_bytes.load(Ordering::Relaxed),
            price_message_bytes: self.0.price_message_bytes.load(Ordering::Relaxed),
            market_message_bytes: self.0.market_message_bytes.load(Ordering::Relaxed),
            queued_event_bytes: self.0.event_bytes.load(Ordering::Relaxed),
        }
    }
}
