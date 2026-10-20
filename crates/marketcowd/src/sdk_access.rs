//! Optional embedded SDK access boundary, disabled unless explicitly configured.
//! No market owner, supplier connection, custom signatures, or data hashing.
use anyhow::{Context, Result, ensure};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use marketcow_contracts::typed_v3::common::{SdkAccessPolicy, SdkCustomer, SdkServerUsage};
use prost::Message;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub fn validate_listener(internal: Option<std::net::SocketAddr>, sdk: std::net::SocketAddr) -> Result<()> {
    ensure!(sdk.ip().is_loopback() && sdk.port()!=0, "SDK listener must bind explicit nonzero loopback port");
    ensure!(internal.is_some() && internal!=Some(sdk), "SDK listener requires a distinct internal listener");
    Ok(())
}

struct Account {
    policy: SdkCustomer,
    revision: u64,
    revoked: AtomicBool,
    http: Arc<Semaphore>,
    streams: Arc<Semaphore>,
    usage: Mutex<Window>,
}
struct Window {
    started: i64,
    bytes: u64,
    requests: u64,
}
#[derive(Clone)]
pub struct Lease(Arc<Account>, #[allow(dead_code)] Arc<OwnedSemaphorePermit>);
impl Lease {
    pub fn customer_id(&self) -> &str {
        &self.0.policy.customer_id
    }
    pub fn charge(&self, bytes: usize) -> Result<()> {
        ensure!(!self.0.revoked.load(Ordering::Acquire), "token_revoked");
        let now = chrono::Utc::now().timestamp();
        ensure!(now < self.0.policy.expires_at_unix_seconds, "token_expired");
        let mut usage = self
            .0
            .usage
            .lock()
            .map_err(|_| anyhow::anyhow!("usage owner poisoned"))?;
        reset(&mut usage, &self.0.policy, now);
        let total = usage
            .bytes
            .checked_add(bytes as u64)
            .context("quota_exceeded")?;
        ensure!(
            total <= self.0.policy.maximum_response_bytes_per_window,
            "quota_exceeded"
        );
        usage.bytes = total;
        Ok(())
    }
    pub async fn expired(&self) {
        loop {
            if self.charge(0).is_err() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
    fn usage(&self) -> Result<SdkServerUsage> {
        let mut u = self
            .0
            .usage
            .lock()
            .map_err(|_| anyhow::anyhow!("usage owner poisoned"))?;
        reset(&mut u, &self.0.policy, chrono::Utc::now().timestamp());
        Ok(SdkServerUsage {
            schema_version: "marketcow.sdk-usage.v1".into(),
            customer_id: self.customer_id().into(),
            policy_revision: self.0.revision,
            window_started_at_unix_seconds: u.started,
            response_payload_bytes: u.bytes,
            requests: u.requests,
            maximum_response_payload_bytes: self.0.policy.maximum_response_bytes_per_window,
            quota_window_seconds: self.0.policy.quota_window_seconds,
        })
    }
}
fn reset(window: &mut Window, policy: &SdkCustomer, now: i64) {
    if now.saturating_sub(window.started) >= i64::from(policy.quota_window_seconds) {
        *window = Window {
            started: now,
            bytes: 0,
            requests: 0,
        };
    }
}
struct Gate {
    accounts: RwLock<Vec<Arc<Account>>>,
}
fn read(path: &PathBuf) -> Result<SdkAccessPolicy> {
    let meta = std::fs::symlink_metadata(path)?;
    ensure!(
        meta.is_file() && meta.len() <= 64 * 1024,
        "SDK policy file size/type"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            meta.permissions().mode() & 0o077 == 0,
            "SDK policy must be owner-private"
        );
    }
    let bytes = std::fs::read(path)?;
    ensure!(bytes.len() <= 64 * 1024, "SDK policy byte budget");
    let policy = SdkAccessPolicy::decode(bytes.as_slice())?;
    ensure!(
        policy.schema_version == "marketcow.sdk-access-policy.v1"
            && policy.revision > 0
            && !policy.customers.is_empty()
            && policy.customers.len() <= 64,
        "SDK policy identity/capacity"
    );
    let mut customers = BTreeMap::new();
    let mut tokens = BTreeMap::new();
    for c in &policy.customers {
        ensure!(
            !c.customer_id.is_empty()
                && c.customer_id.len() <= 128
                && (32..=256).contains(&c.bearer_token.len())
                && c.bearer_token.is_ascii()
                && c.bearer_token.bytes().all(|b| b.is_ascii_graphic())
                && c.expires_at_unix_seconds > 0
                && (1..=16).contains(&c.maximum_http_requests)
                && (1..=16).contains(&c.maximum_streams)
                && c.maximum_response_bytes_per_window > 0
                && (1..=2_678_400).contains(&c.quota_window_seconds)
                && c.maximum_requests_per_window > 0,
            "invalid SDK customer policy"
        );
        ensure!(
            customers.insert(&c.customer_id, ()).is_none()
                && tokens.insert(&c.bearer_token, ()).is_none(),
            "duplicate SDK customer/token"
        );
    }
    Ok(policy)
}
impl Gate {
    fn install(&self, policy: SdkAccessPolicy) -> Result<()> {
        let mut accounts = self
            .accounts
            .write()
            .map_err(|_| anyhow::anyhow!("SDK policy owner poisoned"))?;
        if let Some(current) = accounts.first() {
            ensure!(
                policy.revision >= current.revision,
                "SDK policy revision rollback"
            );
            if policy.revision == current.revision {
                ensure!(
                    policy.customers.len() == accounts.len()
                        && policy
                            .customers
                            .iter()
                            .all(|c| accounts.iter().any(|a| a.policy == *c)),
                    "SDK policy changed without a new revision"
                );
            }
        }
        let mut next = Vec::with_capacity(policy.customers.len());
        for c in policy.customers {
            if let Some(old) = accounts.iter().find(|a| {
                a.policy == c && a.revision == policy.revision && !a.revoked.load(Ordering::Acquire)
            }) {
                next.push(old.clone());
                continue;
            }
            // Preserve current counters on permission/token revision. A policy
            // reload must not reset a customer's quota by accident.
            let prior = accounts
                .iter()
                .find(|a| a.policy.customer_id == c.customer_id)
                .and_then(|a| {
                    a.usage.lock().ok().map(|u| Window {
                        started: u.started,
                        bytes: u.bytes,
                        requests: u.requests,
                    })
                });
            next.push(Arc::new(Account {
                http: Arc::new(Semaphore::new(c.maximum_http_requests as usize)),
                streams: Arc::new(Semaphore::new(c.maximum_streams as usize)),
                policy: c,
                revision: policy.revision,
                revoked: AtomicBool::new(false),
                usage: Mutex::new(prior.unwrap_or(Window {
                    started: chrono::Utc::now().timestamp(),
                    bytes: 0,
                    requests: 0,
                })),
            }));
        }
        for old in accounts.iter() {
            if !next.iter().any(|a| Arc::ptr_eq(a, old)) {
                old.revoked.store(true, Ordering::Release);
            }
        }
        *accounts = next;
        Ok(())
    }
}
fn constant_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |diff, (a, b)| diff | (a ^ b)) == 0
}
fn failure(status: StatusCode, code: &str) -> Response {
    crate::btc_typed_api::failure(status, code, status == StatusCode::TOO_MANY_REQUESTS)
}
fn quota_failure(code: &str, seconds: u64) -> Response {
    let seconds = seconds.max(1);
    (
        StatusCode::TOO_MANY_REQUESTS,
        [
            (header::CONTENT_TYPE, "application/x-protobuf".to_string()),
            (header::CACHE_CONTROL, "no-store".to_string()),
            (header::RETRY_AFTER, seconds.to_string()),
        ],
        marketcow_contracts::typed_v3::common::Error {
            code: code.into(),
            retryable: true,
            retry_after_ms: Some(seconds.saturating_mul(1000)),
            detail: None,
        }
        .encode_to_vec(),
    )
        .into_response()
}
async fn authorize(State(gate): State<Arc<Gate>>, mut request: Request, next: Next) -> Response {
    let cleanup = request.method() == axum::http::Method::DELETE
        && matches!(
            request.uri().path(),
            "/v3/btc/trade-history/session" | "/v3/btc/observation-history/session"
        );
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    let account = gate.accounts.read().ok().and_then(|accounts| {
        accounts
            .iter()
            .find(|a| {
                token.is_some_and(|t| constant_eq(t.as_bytes(), a.policy.bearer_token.as_bytes()))
            })
            .cloned()
    });
    let Some(account) = account else {
        return failure(StatusCode::UNAUTHORIZED, "authentication_required");
    };
    if !cleanup
        && (account.revoked.load(Ordering::Acquire)
            || chrono::Utc::now().timestamp() >= account.policy.expires_at_unix_seconds)
    {
        return failure(StatusCode::UNAUTHORIZED, "token_expired_or_revoked");
    }
    let path = request.uri().path();
    let permitted = match path {
        "/v3/btc/capabilities" | "/v3/btc/health" | "/sdk/v1/usage" => true,
        "/v3/btc/price-stream" | "/v3/btc/bars/1s" | "/v3/btc/bars/1m" => account.policy.prices,
        "/v3/btc/trade-history"
        | "/v3/btc/trade-history/session"
        | "/v3/btc/trade-history/resolve"
        | "/v3/btc/observation-history"
        | "/v3/btc/observation-history/session"
        | "/v3/btc/observation-history/resolve" => account.policy.history,
        "/v7/polymarket/btc-5m/market-streams"
        | "/v7/polymarket/btc-5m/market-stream"
        | "/v7/polymarket/btc-5m/rules"
        | "/v1/facts/polymarket/reference"
        | "/v1/facts/polymarket/finality" => account.policy.markets,
        "/v1/facts/polymarket/execution"
        | "/v1/facts/polymarket/official-book"
        | "/v1/facts/btc/rest" => account.policy.public_rest,
        p if p.starts_with("/v3/btc/evidence/") => account.policy.evidence,
        _ => false, // Old direct/JSON routes are not part of the customer surface.
    };
    if !permitted && !cleanup {
        return failure(StatusCode::FORBIDDEN, "permission_denied");
    }
    let websocket = request
        .headers()
        .get(header::UPGRADE)
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket"));
    let semaphore = if websocket {
        account.streams.clone()
    } else {
        account.http.clone()
    };
    let Ok(permit) = semaphore.try_acquire_owned() else {
        return failure(StatusCode::TOO_MANY_REQUESTS, "customer_concurrency");
    };
    {
        let Ok(mut u) = account.usage.lock() else {
            return failure(StatusCode::SERVICE_UNAVAILABLE, "usage_unavailable");
        };
        reset(&mut u, &account.policy, chrono::Utc::now().timestamp());
        let wait = (u.started + i64::from(account.policy.quota_window_seconds)
            - chrono::Utc::now().timestamp())
        .max(1) as u64;
        if !cleanup && u.bytes >= account.policy.maximum_response_bytes_per_window {
            return quota_failure("response_quota_exceeded", wait);
        }
        if !cleanup && u.requests >= u64::from(account.policy.maximum_requests_per_window) {
            return quota_failure("request_quota_exceeded", wait);
        }
        u.requests = u.requests.saturating_add(1);
    }
    let lease = Lease(account, Arc::new(permit));
    if path == "/sdk/v1/usage" {
        return match lease.usage() {
            Ok(v) => (
                [
                    ("content-type", "application/x-protobuf"),
                    ("cache-control", "no-store"),
                ],
                v.encode_to_vec(),
            )
                .into_response(),
            Err(_) => failure(StatusCode::SERVICE_UNAVAILABLE, "usage_unavailable"),
        };
    }
    request.extensions_mut().insert(lease.clone());
    let response = next.run(request).await;
    if websocket {
        return response;
    } // Upgrade handler retains lease for socket lifetime.
    let (parts, body) = response.into_parts();
    let stream = body.into_data_stream();
    use futures_util::StreamExt;
    let stream = stream.map(move |chunk| -> Result<Bytes, std::io::Error> {
        let bytes = chunk.map_err(std::io::Error::other)?;
        if !cleanup {
            lease.charge(bytes.len()).map_err(std::io::Error::other)?;
        }
        Ok(bytes)
    });
    Response::from_parts(parts, Body::from_stream(stream))
}
/// Returns the embedded router. The original internal surface is unchanged
/// unless the operator explicitly supplies this private policy file.
pub fn protect(router: Router, path: PathBuf) -> Result<Router> {
    let gate = Arc::new(Gate {
        accounts: RwLock::new(Vec::new()),
    });
    let initial = read(&path)?;
    gate.install(initial)?;
    let reload = gate.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let path = path.clone();
            let result = tokio::task::spawn_blocking(move || read(&path)).await;
            match result {
                Ok(Ok(policy)) => {
                    if reload.install(policy).is_err() {
                        if let Ok(accounts) = reload.accounts.read() {
                            for a in accounts.iter() {
                                a.revoked.store(true, Ordering::Release);
                            }
                        }
                    }
                }
                _ => {
                    if let Ok(accounts) = reload.accounts.read() {
                        for a in accounts.iter() {
                            a.revoked.store(true, Ordering::Release);
                        }
                    }
                }
            }
        }
    });
    // Usage is intercepted in middleware, but still needs a matched route.
    Ok(router
        .route(
            "/sdk/v1/usage",
            axum::routing::get(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .layer(middleware::from_fn_with_state(gate, authorize)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;
    #[test]
    fn sdk_listener_never_binds_public_or_replaces_internal() {
        let internal=Some("192.168.124.3:8793".parse().unwrap());
        assert!(validate_listener(internal,"127.0.0.1:18793".parse().unwrap()).is_ok());
        for sdk in ["0.0.0.0:18793","192.168.124.3:18793","127.0.0.1:0"] {
            assert!(validate_listener(internal,sdk.parse().unwrap()).is_err());
        }
        let same="127.0.0.1:18793".parse().unwrap();
        assert!(validate_listener(Some(same),same).is_err());
        assert!(validate_listener(None,same).is_err());
    }
    #[tokio::test]
    async fn protecting_router_clone_does_not_protect_internal_routes() {
        let internal=Router::new().route("/v3/btc/health",axum::routing::get(||async{"healthy"}));
        let gate=Arc::new(Gate {accounts:RwLock::new(vec![account()])});
        let sdk=internal.clone().layer(middleware::from_fn_with_state(gate,authorize));
        let request=||Request::builder().uri("/v3/btc/health").body(Body::empty()).unwrap();
        assert_eq!(internal.oneshot(request()).await.unwrap().status(),StatusCode::OK);
        assert_eq!(sdk.oneshot(request()).await.unwrap().status(),StatusCode::UNAUTHORIZED);
    }
    #[tokio::test]
    async fn two_real_listeners_share_state_but_not_authentication_boundary() {
        use std::sync::atomic::AtomicUsize;
        let count=Arc::new(AtomicUsize::new(0));
        let owner=count.clone();
        let internal=Router::new().route("/v3/btc/health",axum::routing::get(move || {
            let owner=owner.clone();
            async move { owner.fetch_add(1,Ordering::Relaxed).to_string() }
        }));
        let gate=Arc::new(Gate {accounts:RwLock::new(vec![account()])});
        let sdk=internal.clone().layer(middleware::from_fn_with_state(gate,authorize));
        let a=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let b=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let lan_url=format!("http://{}/v3/btc/health",a.local_addr().unwrap());
        let sdk_url=format!("http://{}/v3/btc/health",b.local_addr().unwrap());
        let lan_task=tokio::spawn(async move { axum::serve(a,internal).await.unwrap(); });
        let sdk_task=tokio::spawn(async move { axum::serve(b,sdk).await.unwrap(); });
        let client=reqwest::Client::builder().no_proxy().timeout(std::time::Duration::from_secs(3)).build().unwrap();
        assert_eq!(client.get(&sdk_url).send().await.unwrap().status(),StatusCode::UNAUTHORIZED);
        assert_eq!(client.get(&lan_url).send().await.unwrap().text().await.unwrap(),"0");
        assert_eq!(client.get(&sdk_url).bearer_auth("0123456789abcdef0123456789abcdef").send().await.unwrap().text().await.unwrap(),"1");
        assert_eq!(count.load(Ordering::Relaxed),2);
        lan_task.abort();sdk_task.abort();
    }
    fn customer() -> SdkCustomer {
        SdkCustomer {
            customer_id: "customer-a".into(),
            bearer_token: "0123456789abcdef0123456789abcdef".into(),
            expires_at_unix_seconds: chrono::Utc::now().timestamp() + 1000,
            prices: true,
            markets: false,
            history: true,
            evidence: false,
            public_rest: false,
            maximum_http_requests: 1,
            maximum_streams: 1,
            maximum_response_bytes_per_window: 1024,
            quota_window_seconds: 600,
            maximum_requests_per_window: 100,
        }
    }
    fn account() -> Arc<Account> {
        Arc::new(Account {
            policy: customer(),
            revision: 1,
            revoked: AtomicBool::new(false),
            http: Arc::new(Semaphore::new(1)),
            streams: Arc::new(Semaphore::new(1)),
            usage: Mutex::new(Window {
                started: chrono::Utc::now().timestamp(),
                bytes: 0,
                requests: 0,
            }),
        })
    }
    #[tokio::test]
    async fn lease_holds_stream_slot_and_stops_after_revocation_or_budget() {
        let account = account();
        let lease = Lease(
            account.clone(),
            Arc::new(account.streams.clone().try_acquire_owned().unwrap()),
        );
        assert!(account.streams.clone().try_acquire_owned().is_err());
        lease.charge(1024).unwrap();
        assert!(lease.charge(1).is_err());
        account.revoked.store(true, Ordering::Release);
        assert!(lease.charge(0).is_err());
        drop(lease);
        assert!(account.streams.clone().try_acquire_owned().is_ok());
    }
    #[tokio::test]
    async fn embedded_boundary_denies_legacy_and_separates_permissions() {
        let gate = Arc::new(Gate {
            accounts: RwLock::new(vec![account()]),
        });
        let router = Router::new()
            .route("/v3/btc/health", axum::routing::get(|| async { "healthy" }))
            .route(
                "/v1/btc-research/status",
                axum::routing::get(|| async { "old" }),
            )
            .route(
                "/v7/polymarket/btc-5m/market-streams",
                axum::routing::get(|| async { "market" }),
            )
            .layer(middleware::from_fn_with_state(gate, authorize));
        let auth = "Bearer 0123456789abcdef0123456789abcdef";
        let anonymous = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v3/btc/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
        for (path, status) in [
            ("/v3/btc/health", StatusCode::OK),
            ("/v1/btc-research/status", StatusCode::FORBIDDEN),
            (
                "/v7/polymarket/btc-5m/market-streams",
                StatusCode::FORBIDDEN,
            ),
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .header(header::AUTHORIZATION, auth)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status);
            // Drain the HTTP body to release its actual lifetime permit.
            axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
        }
    }
    #[test]
    fn reload_preserves_quota_and_revokes_existing_leases() {
        let gate = Gate {
            accounts: RwLock::new(Vec::new()),
        };
        gate.install(SdkAccessPolicy {
            schema_version: "marketcow.sdk-access-policy.v1".into(),
            revision: 1,
            customers: vec![customer()],
        })
        .unwrap();
        let old = gate.accounts.read().unwrap()[0].clone();
        old.usage.lock().unwrap().bytes = 999;
        let mut c = customer();
        c.markets = true;
        gate.install(SdkAccessPolicy {
            schema_version: "marketcow.sdk-access-policy.v1".into(),
            revision: 2,
            customers: vec![c],
        })
        .unwrap();
        assert!(old.revoked.load(Ordering::Acquire));
        assert_eq!(
            gate.accounts.read().unwrap()[0].usage.lock().unwrap().bytes,
            999
        );
    }
}
