use crate::{Error, Result, error::require};
use futures_util::StreamExt;
use marketcow_wire::{self as wire, common, facts, history, market};
use prost::Message;
use reqwest::{Method, header};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, watch};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::client::IntoClientRequest};
use url::Url;

pub(crate) type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
static HTTP_DECODE_SLOTS: std::sync::OnceLock<Arc<Semaphore>> = std::sync::OnceLock::new();

/// Hard client budgets. Server-advertised budgets may be stricter, never larger.
#[derive(Clone, Debug)]
pub struct Limits {
    pub response_bytes: usize,
    pub price_frame_bytes: usize,
    pub market_frame_bytes: usize,
    pub history_bytes: u64,
    pub history_pages: u32,
    pub history_rows: u64,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub history_timeout: Duration,
    pub stream_idle_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            response_bytes: 4 * 1024 * 1024,
            price_frame_bytes: 4 * 1024 * 1024,
            market_frame_bytes: 8 * 1024 * 1024,
            history_bytes: 1024 * 1024 * 1024,
            history_pages: 1024,
            history_rows: 1_024_000,
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(125),
            history_timeout: Duration::from_secs(725),
            stream_idle_timeout: Duration::from_secs(30),
        }
    }
}

pub struct ClientBuilder {
    endpoint: String,
    token: Option<String>,
    limits: Limits,
}

pub(crate) struct Inner {
    pub endpoint: Url,
    pub token: Option<header::HeaderValue>,
    pub http: reqwest::Client,
    pub limits: Limits,
    pub closed: AtomicBool,
    pub cleanup_slots: Arc<Semaphore>,
    pub request_slots: Arc<Semaphore>,
    pub shutdown: watch::Sender<bool>,
    pub price_hubs: Mutex<BTreeMap<i32, Weak<crate::shared_price::PriceHub>>>,
    pub market_hubs: Mutex<BTreeMap<(String, String), Weak<crate::shared_market::MarketHub>>>,
    pub event_bytes: Arc<AtomicUsize>,
    pub tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    discovery: tokio::sync::Mutex<Option<(tokio::time::Instant, market::MarketStreams)>>,
    rules_cache: Mutex<BTreeMap<(String, u64), market::RulesRecord>>,
    pub http_response_bytes: AtomicU64,
    pub price_message_bytes: AtomicU64,
    pub market_message_bytes: AtomicU64,
    pub http_requests: AtomicU64,
    pub leases: Mutex<BTreeMap<String, crate::leases::Lease>>,
}

#[derive(Clone)]
pub struct Client(pub(crate) Arc<Inner>);

impl ClientBuilder {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            token: None,
            limits: Limits::default(),
        }
    }
    pub fn api_key(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }
    pub fn limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    pub async fn connect(self) -> Result<Client> {
        let client = self.build()?;
        client.negotiate().await?;
        Ok(client)
    }
    pub fn build(self) -> Result<Client> {
        let endpoint =
            Url::parse(&self.endpoint).map_err(|e| Error::Configuration(e.to_string()))?;
        require(
            matches!(endpoint.scheme(), "http" | "https"),
            "HTTP(S) endpoint required",
        )?;
        require(
            endpoint.username().is_empty()
                && endpoint.password().is_none()
                && endpoint.query().is_none()
                && endpoint.fragment().is_none()
                && endpoint.path() == "/",
            "endpoint must be an origin without credentials/path/query",
        )?;
        require(
            self.limits.response_bytes > 0
                && self.limits.price_frame_bytes > 0
                && self.limits.market_frame_bytes > 0
                && self.limits.history_pages > 0
                && self.limits.history_rows > 0
                && self.limits.history_bytes > 0,
            "positive budgets required",
        )?;
        let token = self
            .token
            .map(|s| {
                let mut value = header::HeaderValue::from_str(&format!("Bearer {s}"))
                    .map_err(|_| Error::Configuration("invalid bearer token".into()))?;
                value.set_sensitive(true);
                Ok::<_, Error>(value)
            })
            .transpose()?;
        let http = reqwest::Client::builder()
            .no_proxy() // SDK history must not silently use an ambient paid proxy.
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(self.limits.connect_timeout)
            .timeout(self.limits.request_timeout)
            .build()?;
        Ok(Client(Arc::new(Inner {
            endpoint,
            token,
            http,
            limits: self.limits,
            closed: AtomicBool::new(false),
            cleanup_slots: Arc::new(Semaphore::new(4)),
            request_slots: Arc::new(Semaphore::new(4)),
            shutdown: watch::channel(false).0,
            price_hubs: Mutex::new(BTreeMap::new()),
            market_hubs: Mutex::new(BTreeMap::new()),
            event_bytes: Arc::new(AtomicUsize::new(0)),
            tasks: Mutex::new(Vec::new()),
            discovery: tokio::sync::Mutex::new(None),
            rules_cache: Mutex::new(BTreeMap::new()),
            http_response_bytes: AtomicU64::new(0),
            price_message_bytes: AtomicU64::new(0),
            market_message_bytes: AtomicU64::new(0),
            http_requests: AtomicU64::new(0),
            leases: Mutex::new(BTreeMap::new()),
        })))
    }
}

impl Client {
    /// Capability/authentication check only; subscribes no stream or history.
    pub async fn negotiate(&self) -> Result<wire::btc::BtcCapabilities> {
        let value = self.btc_capabilities().await?;
        require(
            value.history_session_ttl_seconds == 600
                && value.history_explicit_release
                && value.history_request_identity
                && value.history_page_limit > 0,
            "unsupported SDK history capabilities",
        )?;
        Ok(value)
    }
    pub async fn server_usage(&self) -> Result<common::SdkServerUsage> {
        let (value, _) = self
            .request::<common::SdkServerUsage>(
                Method::GET,
                "/sdk/v1/usage",
                &[],
                None,
                false,
                false,
            )
            .await?;
        require(
            value.schema_version == "marketcow.sdk-usage.v1" && !value.customer_id.is_empty(),
            "server usage identity",
        )?;
        Ok(value)
    }
    pub fn builder(endpoint: impl Into<String>) -> ClientBuilder {
        ClientBuilder::new(endpoint)
    }
    pub fn limits(&self) -> &Limits {
        &self.0.limits
    }
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::Acquire)
    }
    pub fn close(&self) {
        self.0.closed.store(true, Ordering::Release);
        self.0.shutdown.send_replace(true);
    }

    /// Stop owner tasks and await them for at most five seconds, then abort
    /// remaining tasks. No runtime is created and no caller task is joined.
    pub async fn close_and_wait(&self) -> Result<()> {
        self.close();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let release = tokio::time::timeout_at(deadline, self.release_leases())
            .await
            .unwrap_or_else(|_| Err(Error::Limit("client session shutdown deadline")));
        loop {
            let mut tasks = {
                let mut tasks = self
                    .0
                    .tasks
                    .lock()
                    .map_err(|_| Error::Protocol("task registry poisoned".into()))?;
                std::mem::take(&mut *tasks)
            };
            if tasks.is_empty() {
                return release;
            }
            for index in 0..tasks.len() {
                if tokio::time::timeout_at(deadline, &mut tasks[index])
                    .await
                    .is_err()
                {
                    for task in &tasks[index..] {
                        task.abort();
                    }
                    return Err(Error::Limit("client shutdown deadline"));
                }
            }
        }
    }
    pub(crate) fn spawn_tracked(
        &self,
        future: impl std::future::Future<Output = ()> + Send + 'static,
        cleanup: bool,
    ) -> Result<()> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Configuration("Tokio runtime required".into()))?;
        let mut tasks = self
            .0
            .tasks
            .lock()
            .map_err(|_| Error::Protocol("task registry poisoned".into()))?;
        tasks.retain(|t| !t.is_finished());
        if self.is_closed() && !cleanup {
            return Err(Error::Cancelled);
        }
        if tasks.len() >= 256 {
            return Err(Error::Limit("client background tasks"));
        }
        tasks.push(runtime.spawn(future));
        Ok(())
    }

    pub(crate) async fn wait_closed(&self) {
        let mut signal = self.0.shutdown.subscribe();
        let _ = signal.wait_for(|closed| *closed).await;
    }

    pub(crate) fn url(&self, path: &str) -> Result<Url> {
        self.0
            .endpoint
            .join(path)
            .map_err(|e| Error::Configuration(e.to_string()))
    }

    pub(crate) async fn socket(
        &self,
        path: &str,
        query: &[(&str, &str)],
        protocol: &'static str,
        maximum: usize,
    ) -> Result<Socket> {
        if self.is_closed() {
            return Err(Error::Cancelled);
        }
        let mut url = self.url(path)?;
        url.set_scheme(if self.0.endpoint.scheme() == "https" {
            "wss"
        } else {
            "ws"
        })
        .map_err(|_| Error::Configuration("WS scheme".into()))?;
        url.query_pairs_mut().extend_pairs(query.iter().copied());
        let mut request = url.as_str().into_client_request()?;
        request
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", protocol.parse().unwrap());
        if let Some(token) = &self.0.token {
            request
                .headers_mut()
                .insert(http::header::AUTHORIZATION, token.clone());
        }
        let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
            .max_message_size(Some(maximum))
            .max_frame_size(Some(maximum));
        let (socket, response) = tokio::time::timeout(
            self.limits().connect_timeout,
            tokio_tungstenite::connect_async_with_config(request, Some(config), false),
        )
        .await
        .map_err(|_| Error::Protocol("WebSocket connect timeout".into()))??;
        require(
            response
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|s| s.to_str().ok())
                == Some(protocol),
            "WebSocket subprotocol",
        )?;
        Ok(socket)
    }

    pub(crate) async fn request<T: Message + Default + 'static>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<Vec<u8>>,
        fact: bool,
        allow_closed: bool,
    ) -> Result<(T, usize)> {
        let operation = async {
            tokio::time::timeout(
                self.limits().request_timeout,
                self.request_inner(method, path, query, body, fact, allow_closed),
            )
            .await
            .unwrap_or_else(|_| Err(Error::Timeout("HTTP request/decode deadline")))
        };
        if allow_closed {
            return operation.await;
        }
        tokio::select! { biased;
            _ = self.wait_closed() => Err(Error::Cancelled),
            result = operation => result,
        }
    }

    async fn request_inner<T: Message + Default + 'static>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<Vec<u8>>,
        fact: bool,
        allow_closed: bool,
    ) -> Result<(T, usize)> {
        if self.is_closed() && !allow_closed {
            return Err(Error::Cancelled);
        }
        let _permit = self
            .0
            .request_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Limit("client HTTP concurrency"))?;
        let mut req = self
            .0
            .http
            .request(method, self.url(path)?)
            .query(query)
            .header(header::ACCEPT, wire::CONTENT_TYPE);
        if let Some(token) = &self.0.token {
            req = req.header(header::AUTHORIZATION, token);
        }
        if let Some(body) = body {
            if body.len() > 64 * 1024 {
                return Err(Error::Limit("request bytes"));
            }
            req = req
                .header(header::CONTENT_TYPE, wire::CONTENT_TYPE)
                .body(body);
        }
        self.0.http_requests.fetch_add(1, Ordering::Relaxed);
        let response = req.send().await?;
        let status = response.status().as_u16();
        let retry_after_ms = response
            .headers()
            .get(header::RETRY_AFTER)
            .and_then(|s| s.to_str().ok())
            .and_then(crate::retry::retry_after);
        let is_proto = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|s| s.to_str().ok())
            .and_then(|s| s.split(';').next())
            .is_some_and(|s| s.trim().eq_ignore_ascii_case(wire::CONTENT_TYPE));
        let finality_response = path == "/v1/facts/polymarket/finality"
            && response
                .headers()
                .get("x-marketcow-payload-type")
                .and_then(|s| s.to_str().ok())
                == Some("marketcow.facts.v1.FinalityResponse")
            && matches!(status, 202 | 409 | 503);
        if response
            .content_length()
            .is_some_and(|n| n > self.0.limits.response_bytes as u64)
        {
            return Err(Error::Limit("HTTP response bytes"));
        }
        let mut bytes = Vec::new();
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            self.0
                .http_response_bytes
                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
            if chunk.len() > self.0.limits.response_bytes.saturating_sub(bytes.len()) {
                return Err(Error::Limit("HTTP response bytes"));
            }
            bytes.extend_from_slice(&chunk);
        }
        // Finality's 202/409/503 bodies are domain responses, not Failure.
        if status == 200 || finality_response {
            require(is_proto, "expected protobuf response")?;
        } else {
            let failure = if fact && is_proto {
                facts::Failure::decode(bytes.as_slice()).ok()
            } else {
                None
            };
            let error = if fact {
                failure.as_ref().and_then(|f| f.error.clone()).or_else(|| {
                    is_proto
                        .then(|| common::Error::decode(bytes.as_slice()).ok())
                        .flatten()
                })
            } else if is_proto {
                common::Error::decode(bytes.as_slice()).ok()
            } else {
                None
            };
            return Err(Error::Remote {
                status,
                code: error
                    .as_ref()
                    .map(|e| e.code.clone())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| format!("http_{status}")),
                retryable: error
                    .as_ref()
                    .map(|e| e.retryable)
                    .unwrap_or(matches!(status, 429 | 502 | 503 | 504)),
                retry_after_ms: error
                    .as_ref()
                    .and_then(|e| e.retry_after_ms)
                    .or(retry_after_ms),
                detail: error.and_then(|e| e.detail),
                failure: failure.map(Box::new),
            });
        }
        let size = bytes.len();
        let value = if size > 64 * 1024 {
            let permit = HTTP_DECODE_SLOTS
                .get_or_init(|| Arc::new(Semaphore::new(2)))
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Error::Cancelled)?;
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                T::decode(bytes.as_slice())
            })
            .await
            .map_err(|_| Error::Protocol("HTTP decoder worker failed".into()))??
        } else {
            T::decode(bytes.as_slice())?
        };
        Ok((value, size))
    }

    pub async fn btc_capabilities(&self) -> Result<wire::btc::BtcCapabilities> {
        let (value, _) = self
            .request::<wire::btc::BtcCapabilities>(
                Method::GET,
                "/v3/btc/capabilities",
                &[],
                None,
                false,
                false,
            )
            .await?;
        require(
            value.schema_version == "marketcow.btc-capabilities.v3",
            "BTC capabilities schema",
        )?;
        Ok(value)
    }
    pub async fn btc_health(&self) -> Result<wire::btc::BtcHealth> {
        let (value, _) = self
            .request::<wire::btc::BtcHealth>(Method::GET, "/v3/btc/health", &[], None, false, false)
            .await?;
        require(
            value.schema_version == "marketcow.btc-health.v3",
            "BTC health schema",
        )?;
        Ok(value)
    }
    pub async fn markets(&self) -> Result<market::MarketStreams> {
        if self.is_closed() {
            return Err(Error::Cancelled);
        }
        // One bounded in-flight discovery request for concurrent followers.
        // This is a query lock, never held by a market/price receiver.
        let mut cached = self.0.discovery.lock().await;
        if let Some((at, value)) = cached.as_ref() {
            if at.elapsed() < Duration::from_secs(1) {
                return Ok(value.clone());
            }
        }
        let (value, _) = self
            .request::<market::MarketStreams>(
                Method::GET,
                "/v7/polymarket/btc-5m/market-streams",
                &[],
                None,
                false,
                false,
            )
            .await?;
        require(
            value.schema_version == wire::MARKET_STREAMS_SCHEMA,
            "market discovery schema",
        )?;
        *cached = Some((tokio::time::Instant::now(), value.clone()));
        Ok(value)
    }
    pub(crate) fn invalidate_discovery(&self) {
        if let Ok(mut cached) = self.0.discovery.try_lock() {
            *cached = None;
        }
    }
    pub async fn rules(&self, id: &str, revision: u64) -> Result<market::RulesRecord> {
        if self.is_closed() {
            return Err(Error::Cancelled);
        }
        require(!id.is_empty() && revision > 0, "rules identity required")?;
        let key = (id.to_owned(), revision);
        if let Some(value) = self
            .0
            .rules_cache
            .lock()
            .map_err(|_| Error::Protocol("rules cache poisoned".into()))?
            .get(&key)
            .cloned()
        {
            return Ok(value);
        }
        let (value, _) = self
            .request::<market::RulesRecord>(
                Method::GET,
                "/v7/polymarket/btc-5m/rules",
                &[("rules_id", id.into()), ("revision", revision.to_string())],
                None,
                false,
                false,
            )
            .await?;
        require(
            value
                .identity
                .as_ref()
                .is_some_and(|v| v.id == id && v.revision == revision),
            "rules response identity",
        )?;
        let mut cache = self
            .0
            .rules_cache
            .lock()
            .map_err(|_| Error::Protocol("rules cache poisoned".into()))?;
        if cache.len() >= 256 {
            cache.pop_first();
        }
        cache.insert(key, value.clone());
        Ok(value)
    }
    pub async fn reference(&self, key: market::MarketKey) -> Result<facts::ReferenceResponse> {
        let request = facts::MarketRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            key: Some(key.clone()),
        };
        let (value, _) = self
            .request::<facts::ReferenceResponse>(
                Method::POST,
                "/v1/facts/polymarket/reference",
                &[],
                Some(request.encode_to_vec()),
                true,
                false,
            )
            .await?;
        require(
            value.schema_version == wire::REFERENCE_SCHEMA
                && value.request_id == request.request_id,
            "reference response identity/schema",
        )?;
        require(
            value.meta.as_ref().and_then(|m| m.key.as_ref()) == Some(&key),
            "reference market identity",
        )?;
        Ok(value)
    }
    pub async fn evidence(&self, id: &str) -> Result<history::Evidence> {
        require(!id.is_empty(), "evidence identity required")?;
        let mut url = self.url("/v3/btc/evidence/")?;
        url.path_segments_mut()
            .map_err(|_| Error::Configuration("evidence URL".into()))?
            .pop_if_empty()
            .push(id);
        let (value, _) = self
            .request::<history::Evidence>(Method::GET, url.path(), &[], None, false, false)
            .await?;
        require(
            value.evidence_id == id && value.schema_version == "marketcow.btc-evidence.v3",
            "evidence schema/identity mismatch",
        )?;
        Ok(value)
    }

    /// Public payout fact, never redemption/account credit. Pending, provider
    /// failure and conflict retain their typed domain response and evidence.
    pub async fn finality(&self, key: market::MarketKey) -> Result<facts::FinalityResponse> {
        let request = facts::MarketRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            key: Some(key.clone()),
        };
        let (value, _) = self
            .request::<facts::FinalityResponse>(
                Method::POST,
                "/v1/facts/polymarket/finality",
                &[],
                Some(request.encode_to_vec()),
                true,
                false,
            )
            .await?;
        require(
            value.schema_version == wire::FINALITY_SCHEMA
                && value.request_id == request.request_id
                && value.meta.as_ref().and_then(|m| m.key.as_ref()) == Some(&key),
            "finality schema/identity",
        )?;
        Ok(value)
    }
}
