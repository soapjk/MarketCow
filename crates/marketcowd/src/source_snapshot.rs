//! Latest-only immutable publication. No reader can acquire a mutable owner.
use arc_swap::ArcSwap;
use serde::Serialize;
use std::{io, sync::Arc};
use tokio::sync::watch;

pub struct Published<T> {
    latest: ArcSwap<T>,
    changed: watch::Sender<u64>,
}

impl<T> Published<T> {
    pub fn new(value: T) -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            latest: ArcSwap::from_pointee(value),
            changed,
        }
    }
    pub fn load(&self) -> Arc<T> {
        self.latest.load_full()
    }
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
    pub fn publish(&self, value: T) {
        self.latest.store(Arc::new(value));
        self.changed
            .send_modify(|version| *version = version.wrapping_add(1));
    }
}

/// Count using exactly the real serializer, without allocating encoded JSON.
/// This is a byte budget, not an estimate of retained heap memory.
pub fn json_bytes<T: Serialize + ?Sized>(value: &T) -> Result<usize, serde_json::Error> {
    struct Count(usize);
    impl io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("JSON byte count overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Count(0);
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn old_reader_does_not_hold_publication_and_notifications_coalesce() {
        let published = Published::new(vec![1u64]);
        let held = published.load();
        let changed = published.subscribe();
        for n in 2..1000 {
            published.publish(vec![n]);
        }
        assert_eq!(&*held, &[1]);
        assert_eq!(&*published.load(), &[999]);
        assert!(changed.has_changed().unwrap());
    }
    #[test]
    fn counting_uses_exact_json_escaping_without_encoded_buffer() {
        let value = serde_json::json!({"unicode":"雪\n\"\\","n":123,"empty":[],"null":null});
        assert_eq!(
            json_bytes(&value).unwrap(),
            serde_json::to_vec(&value).unwrap().len()
        );
    }
}
