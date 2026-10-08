//! Bounded, non-blocking diagnostic output for the live source process.
//!
//! Market actors and publication workers may enqueue a short diagnostic, but
//! only this dedicated thread performs stderr I/O. Diagnostics are not source
//! facts: a stalled log sink may lose log lines, never market observations.
use std::{
    io::Write,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
};

const QUEUE_LINES: usize = 1024;
const MAX_LINE_BYTES: usize = 4096;

struct Queue {
    sender: SyncSender<String>,
    dropped: AtomicU64,
}

impl Queue {
    fn submit(&self, line: String) {
        match self.sender.try_send(line) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

static QUEUE: OnceLock<Queue> = OnceLock::new();

fn queue() -> &'static Queue {
    QUEUE.get_or_init(|| {
        let (sender, receiver) = mpsc::sync_channel::<String>(QUEUE_LINES);
        std::thread::Builder::new()
            .name("marketcow-diagnostics".into())
            .spawn(move || {
                let stderr = std::io::stderr();
                while let Ok(line) = receiver.recv() {
                    let dropped = QUEUE.get().map_or(0, |queue| queue.dropped.swap(0, Ordering::Relaxed));
                    // Never retain the process-wide stderr lock while waiting
                    // for another diagnostic; panic hooks and unrelated code
                    // must remain able to report independently.
                    let mut writer = stderr.lock();
                    if dropped > 0 {
                        let _ = writeln!(writer, "{{\"stage\":\"diagnostic_log_dropped\",\"count\":{dropped}}}");
                    }
                    let _ = writeln!(writer, "{line}");
                }
            })
            .ok();
        Queue { sender, dropped: AtomicU64::new(0) }
    })
}

/// Start the writer before accepting source data, so no market actor pays the
/// one-time thread startup cost.
pub fn install() {
    let _ = queue();
}

/// Never waits for stderr or for channel capacity. Every enqueued diagnostic
/// is at most 4096 bytes; oversized records become a small JSON notice.
pub fn emit(line: String) {
    let queue = queue();
    let line = if line.len() > MAX_LINE_BYTES {
        format!("{{\"stage\":\"diagnostic_log_truncated\",\"original_bytes\":{}}}", line.len())
    } else { line };
    queue.submit(line);
}

/// A `tracing_subscriber` writer that buffers one event only in memory. Its
/// drop enqueues the line; the caller never writes to a file descriptor.
#[derive(Default)]
pub struct DiagnosticWriter {
    bytes: Vec<u8>,
    original_bytes: usize,
}

impl Write for DiagnosticWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.original_bytes = self.original_bytes.saturating_add(bytes.len());
        let room = MAX_LINE_BYTES.saturating_sub(self.bytes.len());
        self.bytes.extend_from_slice(&bytes[..bytes.len().min(room)]);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

impl Drop for DiagnosticWriter {
    fn drop(&mut self) {
        if self.original_bytes > MAX_LINE_BYTES {
            emit(format!("{{\"stage\":\"diagnostic_log_truncated\",\"original_bytes\":{}}}",
                self.original_bytes));
        } else if !self.bytes.is_empty() {
            emit(String::from_utf8_lossy(&self.bytes).trim_end_matches('\n').to_owned());
        }
    }
}

#[macro_export]
macro_rules! async_eprintln {
    ($($arg:tt)*) => {
        $crate::diagnostic::emit(format!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_submission_never_waits_for_a_stalled_sink() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let queue = Queue { sender, dropped: AtomicU64::new(0) };
        queue.submit("first".to_owned());
        let began = std::time::Instant::now();
        queue.submit("second".to_owned());
        assert!(began.elapsed() < std::time::Duration::from_millis(100));
        assert_eq!(queue.dropped.load(Ordering::Relaxed), 1);
        assert_eq!(receiver.try_recv().unwrap(), "first");
    }

    #[test]
    fn tracing_writer_bounds_per_event_memory() {
        let mut writer = DiagnosticWriter::default();
        writer.write_all(&vec![b'x'; MAX_LINE_BYTES * 2]).unwrap();
        assert_eq!(writer.bytes.len(), MAX_LINE_BYTES);
        assert_eq!(writer.original_bytes, MAX_LINE_BYTES * 2);
    }

    #[test]
    fn idle_diagnostic_thread_does_not_hold_stderr_lock() {
        install();
        let (sender, receiver) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let _lock = std::io::stderr().lock();
            let _ = sender.send(());
        });
        receiver.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
    }
}
