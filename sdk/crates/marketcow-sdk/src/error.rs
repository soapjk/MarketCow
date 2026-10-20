use marketcow_wire::facts;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("local I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("timeout: {0}")]
    Timeout(&'static str),
    #[error("invalid configuration: {0}")]
    Configuration(String),
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("websocket: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("protobuf: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("resource limit: {0}")]
    Limit(&'static str),
    #[error("HTTP {status}: {code}")]
    Remote {
        status: u16,
        code: String,
        retryable: bool,
        retry_after_ms: Option<u64>,
        detail: Option<String>,
        // Keep upstream failure identity without canonical JSON or flattening.
        failure: Option<Box<facts::Failure>>,
    },
    #[error("history incomplete: {reason}")]
    HistoryGap {
        reason: String,
        missing: Vec<(u64, u64)>,
        page: Option<Box<marketcow_wire::history::Page>>,
    },
    #[error("operation cancelled")]
    Cancelled,
}

impl Error {
    pub fn retryable(&self) -> bool {
        match self {
            Self::Timeout(_) => true,
            Self::Transport(e) => e.is_timeout() || e.is_connect() || e.is_body(),
            Self::WebSocket(_) => true,
            Self::Remote { retryable, .. } => *retryable,
            _ => false,
        }
    }
}

pub(crate) fn require(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Protocol(message.into()))
    }
}
