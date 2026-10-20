//! Self-contained loopback tests. No U1, supplier, account or orders.
use axum::{
    Router,
    body::Bytes,
    extract::{
        State,
        ws::{Message as Ws, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use marketcow_sdk::{Client, btc, common, facts, market, wire_history as wire};
use prost::Message;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

fn trade(id: u64) -> btc::Trade {
    btc::Trade {
        trade_id: id,
        trade_time_ms: 900 + id as i64 * 100,
        price: "100.00".into(),
        quantity: "0.0100".into(),
        buyer_is_maker: Some(false),
        ..Default::default()
    }
}
fn snapshot() -> btc::PriceFrame {
    btc::PriceFrame {
        schema_version: "marketcow.btc-price-stream.v3".into(),
        producer_instance_id: "local-test".into(),
        stream_id: "generation-a".into(),
        stream_sequence: 100,
        source: common::Source::BinanceSpotTrade as i32,
        instrument_id: "BTCUSDT.BINANCE".into(),
        source_sequence: Some(3),
        published_at_ns: 1_200_000_000,
        latest_source_age_ms_at_emit: Some(0),
        payload: Some(btc::price_frame::Payload::Snapshot(btc::Snapshot {
            trades: vec![trade(2), trade(3)],
            first_trade_id: Some(2),
            last_trade_id: Some(3),
            retained_count: 2,
            ..Default::default()
        })),
    }
}
fn encoded(value: &impl Message) -> Response {
    (
        [("content-type", "application/x-protobuf")],
        value.encode_to_vec(),
    )
        .into_response()
}
#[derive(Default)]
struct Server {
    corrupt_release: AtomicBool,
    attempts: AtomicUsize,
    released: AtomicUsize,
    sockets: AtomicUsize,
    request_id: std::sync::Mutex<Option<String>>,
}
async fn history(State(state): State<Arc<Server>>, headers: HeaderMap, body: Bytes) -> Response {
    assert_eq!(headers["authorization"], "Bearer test-token");
    let request = wire::PageRequest::decode(body).unwrap();
    let mut identity = state.request_id.lock().unwrap();
    if let Some(id) = &*identity {
        assert_eq!(request.request_id.as_ref(), Some(id));
    } else {
        *identity = request.request_id.clone();
    }
    if state.attempts.fetch_add(1, Ordering::Relaxed) == 0 {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("content-type", "application/x-protobuf")],
            common::Error {
                code: "temporary".into(),
                retryable: true,
                retry_after_ms: Some(1),
                detail: None,
            }
            .encode_to_vec(),
        )
            .into_response();
    }
    let page = if request.page_token.is_some() { 1 } else { 0 };
    encoded(&wire::Page {
        schema_version: "marketcow.btc-trade-history.v3".into(),
        session_id: "0d711d81-c50f-42aa-83bc-08504d81ccca".into(),
        range: request.range,
        request_id: request.request_id,
        page_index: page,
        page_count: 2,
        session_ttl_seconds: 600,
        predecessor: (page == 0).then(|| trade(1)),
        trades: vec![trade(u64::from(page) + 2)],
        next_page_token: (page == 0).then(|| "0d711d81-c50f-42aa-83bc-08504d81ccca:1".into()),
        complete: page == 1,
        coverage_verified: true,
        total_trade_count: 2,
        database_revision: 1,
        coverage_first_time_ms: Some(1100),
        coverage_last_time_ms: Some(1200),
        ..Default::default()
    })
}
async fn release(State(state): State<Arc<Server>>, body: Bytes) -> Response {
    let r = wire::ReleaseRequest::decode(body).unwrap();
    let n = state.released.fetch_add(1, Ordering::Relaxed);
    encoded(&wire::ReleaseReceipt {
        schema_version: "marketcow.btc-history-release.v3".into(),
        session_id: r.session_id,
        range: r.range,
        request_id: if state.corrupt_release.swap(false, Ordering::Relaxed) {
            Some("wrong-request".into())
        } else {
            r.request_id
        },
        released: n == 0,
    })
}
async fn stream(State(state): State<Arc<Server>>, ws: WebSocketUpgrade) -> Response {
    ws.protocols(["marketcow.btc-price.v3"])
        .on_upgrade(move |mut socket| async move {
            state.sockets.fetch_add(1, Ordering::Relaxed);
            socket
                .send(Ws::Binary(snapshot().encode_to_vec().into()))
                .await
                .unwrap();
            while let Some(Ok(message)) = socket.recv().await {
                if matches!(message, Ws::Close(_)) {
                    break;
                }
            }
        })
}
async fn server() -> (String, Arc<Server>, tokio::task::JoinHandle<()>) {
    let state = Arc::new(Server::default());
    let router = Router::new()
        .route("/v3/btc/trade-history", post(history))
        .route("/v3/btc/trade-history/session", delete(release))
        .route("/v3/btc/price-stream", get(stream))
        .route(
            "/v3/btc/evidence/{id}",
            get(
                |axum::extract::Path(id): axum::extract::Path<String>| async move {
                    encoded(&wire::Evidence {
                        schema_version: "marketcow.btc-evidence.v3".into(),
                        evidence_id: id,
                        raw_body: vec![b'x'; 128 * 1024],
                        ..Default::default()
                    })
                },
            ),
        )
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (endpoint, state, task)
}
#[tokio::test]
async fn same_request_retry_checkpoint_resume_and_release_use_real_http() {
    let (endpoint, state, server) = server().await;
    let client = Client::builder(endpoint)
        .api_key("test-token")
        .build()
        .unwrap();
    let mut job = client.trade_history(1100, 3, 1).unwrap();
    let first = job.next_page().await.unwrap().unwrap();
    assert_eq!(first.trades[0].trade_id, 2);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("job.pb");
    job.save_checkpoint(&path).await.unwrap();
    let mut resumed = client.resume_trade_history(&path).await.unwrap();
    let last = resumed.next_page().await.unwrap().unwrap();
    assert!(last.complete);
    assert_eq!(last.trades[0].trade_id, 3);
    assert!(resumed.is_released());
    assert_eq!(state.attempts.load(Ordering::Relaxed), 3);
    assert_eq!(state.released.load(Ordering::Relaxed), 1);
    job.close().await.unwrap(); // Idempotent release of the pre-crash handle.
    client.close_and_wait().await.unwrap();
    server.abort();
}
#[tokio::test]
async fn client_close_releases_retained_job_and_shared_prices_use_one_socket() {
    let (endpoint, state, server) = server().await;
    let client = Client::builder(endpoint)
        .api_key("test-token")
        .build()
        .unwrap();
    let mut a = client
        .shared_prices(common::Source::BinanceSpotTrade)
        .unwrap();
    let mut b = client
        .shared_prices(common::Source::BinanceSpotTrade)
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), a.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), b.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(state.sockets.load(Ordering::Relaxed), 1);
    let mut job = client.trade_history(1100, 3, 1).unwrap();
    job.next_page().await.unwrap();
    client.close_and_wait().await.unwrap();
    assert_eq!(state.released.load(Ordering::Relaxed), 1);
    assert_eq!(client.usage().queued_event_bytes, 0);
    assert!(client.usage().price_message_bytes > 0);
    server.abort();
}

#[tokio::test]
async fn price_socket_burst_preserves_every_delta_while_application_is_not_polling() {
    let sent = Arc::new(tokio::sync::Notify::new());
    let signal = sent.clone();
    let router = Router::new().route("/v3/btc/price-stream", get(
        move |ws: WebSocketUpgrade| {
            let signal = signal.clone();
            async move {
                ws.protocols(["marketcow.btc-price.v3"]).on_upgrade(move |mut socket| async move {
                    socket.send(Ws::Binary(snapshot().encode_to_vec().into())).await.unwrap();
                    for offset in 1..=512 {
                        let mut frame = snapshot();
                        frame.stream_sequence = 100 + offset;
                        frame.source_sequence = Some(3 + offset);
                        frame.payload = Some(btc::price_frame::Payload::Trade(btc::TradeDelta {
                            trade: Some(trade(3 + offset)),
                        }));
                        socket.send(Ws::Binary(frame.encode_to_vec().into())).await.unwrap();
                    }
                    signal.notify_one();
                    while let Some(Ok(message)) = socket.recv().await {
                        if matches!(message, Ws::Close(_)) { break; }
                    }
                })
            }
        }
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap(); });
    let client = Client::builder(endpoint).build().unwrap();
    let mut stream = client.shared_prices(common::Source::BinanceSpotTrade).unwrap();
    // Simulate application/foreign-runtime scheduling lag, not server pacing.
    tokio::time::timeout(std::time::Duration::from_secs(2), sent.notified()).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    for expected in 100..=612 {
        match tokio::time::timeout(std::time::Duration::from_secs(2), stream.next()).await.unwrap().unwrap().unwrap() {
            marketcow_sdk::PriceDelivery::Frame { frame, .. } => assert_eq!(frame.stream_sequence, expected),
            _ => panic!("unexpected recovery during local burst"),
        }
    }
    stream.close();
    assert_eq!(client.usage().queued_event_bytes, 0);
    client.close_and_wait().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn deferred_freeze_survives_full_read_until_install_boundary_and_close() {
    let (endpoint, state, server) = server().await;
    let client = Client::builder(endpoint)
        .api_key("test-token")
        .build()
        .unwrap();
    let mut job = client.trade_history(1100, 3, 1).unwrap();
    job.retain_until_close().unwrap();
    job.next_page().await.unwrap().unwrap();
    let last = job.next_page().await.unwrap().unwrap();
    assert!(last.complete && job.is_complete());
    assert!(!job.is_released() && job.release_receipt().is_none());
    assert!(job.next_page().await.unwrap().is_none());
    assert_eq!(state.released.load(Ordering::Relaxed), 0);
    job.verify_live_overlap(&trade(3)).unwrap(); // Application join/install goes here.
    job.close().await.unwrap();
    assert!(job.is_released());
    let receipt = job.release_receipt().unwrap();
    assert!(receipt.released);
    assert_eq!(receipt.range.as_ref().unwrap().through_trade_id, 3);
    job.close().await.unwrap();
    assert_eq!(state.released.load(Ordering::Relaxed), 1);
    client.close_and_wait().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn invalid_release_receipt_is_observable_and_close_retries_same_identity() {
    let (endpoint, state, server) = server().await;
    let client = Client::builder(endpoint)
        .api_key("test-token")
        .build()
        .unwrap();
    let mut job = client.trade_history(1100, 3, 1).unwrap();
    job.retain_until_close().unwrap();
    job.next_page().await.unwrap();
    job.next_page().await.unwrap();
    state.corrupt_release.store(true, Ordering::Relaxed);
    assert!(job.close().await.is_err());
    assert!(!job.is_released() && job.release_error().is_some());
    assert!(job.release_receipt().is_none());
    job.close().await.unwrap();
    assert!(job.is_released() && job.release_error().is_none());
    assert!(!job.release_receipt().unwrap().released); // Idempotent second DELETE.
    assert_eq!(state.released.load(Ordering::Relaxed), 2);
    client.close_and_wait().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn large_proto_decode_is_bounded_and_byte_budget_rejects_without_full_read() {
    let (endpoint, _state, server) = server().await;
    let client = Client::builder(&endpoint).build().unwrap();
    let evidence = client
        .evidence("0d711d81-c50f-42aa-83bc-08504d81ccca")
        .await
        .unwrap();
    assert_eq!(evidence.raw_body.len(), 128 * 1024);
    let usage = client.usage();
    assert!(usage.http_response_bytes > 128 * 1024);
    let mut limits = marketcow_sdk::Limits::default();
    limits.response_bytes = 64 * 1024;
    let small = Client::builder(endpoint).limits(limits).build().unwrap();
    assert!(matches!(
        small.evidence("0d711d81-c50f-42aa-83bc-08504d81ccca").await,
        Err(marketcow_sdk::Error::Limit("HTTP response bytes"))
    ));
    client.close_and_wait().await.unwrap();
    small.close_and_wait().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn public_facts_bind_requests_and_preserve_pending_and_missing_facts() {
    async fn reference(body: Bytes) -> Response {
        let r = facts::MarketRequest::decode(body).unwrap();
        encoded(&facts::ReferenceResponse {
            schema_version: "marketcow.facts.reference.v1".into(),
            request_id: r.request_id,
            meta: Some(facts::FactMeta {
                key: r.key,
                ..Default::default()
            }),
            binance_opening: Some(facts::ReadingState {
                available: false,
                reason: Some("coverage_missing".into()),
                reading: None,
            }),
            ..Default::default()
        })
    }
    async fn finality(body: Bytes) -> Response {
        let r = facts::MarketRequest::decode(body).unwrap();
        let (status, kind) = match r.key.as_ref().unwrap().market_id.as_str() {
            "conflict" => (StatusCode::CONFLICT, "conflict"),
            "failed" => (StatusCode::SERVICE_UNAVAILABLE, "provider_failed"),
            _ => (StatusCode::ACCEPTED, "unresolved"),
        };
        let mut response = encoded(&facts::FinalityResponse {
            schema_version: "marketcow.facts.finality.v1".into(),
            request_id: r.request_id,
            meta: Some(facts::FactMeta {
                key: r.key,
                ..Default::default()
            }),
            status: kind.into(),
            ..Default::default()
        });
        *response.status_mut() = status;
        response.headers_mut().insert(
            "x-marketcow-payload-type",
            "marketcow.facts.v1.FinalityResponse".parse().unwrap(),
        );
        response
    }
    async fn terms(body: Bytes) -> Response {
        let r = facts::MarketRequest::decode(body).unwrap();
        encoded(&facts::ExecutionFactsResponse {
            schema_version: "marketcow.facts.execution.v1".into(),
            request_id: r.request_id,
            meta: Some(facts::FactMeta {
                key: r.key,
                ..Default::default()
            }),
            ..Default::default()
        })
    }
    async fn book(body: Bytes) -> Response {
        let r = facts::OfficialBookRequest::decode(body).unwrap();
        encoded(&facts::OfficialBookResponse {
            schema_version: "marketcow.facts.official-book.v1".into(),
            request_id: r.request_id,
            acquisition: "official_rest".into(),
            dispatch: Some(facts::DispatchBinding {
                key: r.key.clone(),
                stream_id: r.stream_id,
                token_id: r.token_id.clone(),
                ..Default::default()
            }),
            body: Some(facts::official_book_response::Body::Book(
                facts::OfficialBook {
                    market: r.key.unwrap().condition_id,
                    asset_id: r.token_id,
                    ..Default::default()
                },
            )),
            ..Default::default()
        })
    }
    async fn rest(body: Bytes) -> Response {
        let r = facts::BtcRestRequest::decode(body).unwrap();
        encoded(&facts::BtcRestResponse {
            schema_version: "marketcow.facts.btc-rest.v1".into(),
            request_id: r.request_id,
            source: r.source,
            instrument_id: "BTCUSDT.BINANCE".into(),
            acquisition: "official_rest".into(),
            latest_trade: Some(trade(3)),
            ..Default::default()
        })
    }
    let router = Router::new()
        .route("/v1/facts/polymarket/reference", post(reference))
        .route("/v1/facts/polymarket/finality", post(finality))
        .route("/v1/facts/polymarket/execution", post(terms))
        .route("/v1/facts/polymarket/official-book", post(book))
        .route("/v1/facts/btc/rest", post(rest));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = Client::builder(endpoint).build().unwrap();
    let key = market::MarketKey {
        market_id: "test".into(),
        condition_id: "condition".into(),
        up_token_id: "up".into(),
        down_token_id: "down".into(),
        ..Default::default()
    };
    let reference = client.reference(key.clone()).await.unwrap();
    assert_eq!(
        reference.binance_opening.unwrap().reason.as_deref(),
        Some("coverage_missing")
    );
    let pending = client.finality(key.clone()).await.unwrap();
    assert_eq!(pending.status, "unresolved");
    assert!(!pending.settlement_import_allowed);
    for (id, expected) in [("conflict", "conflict"), ("failed", "provider_failed")] {
        let mut k = key.clone();
        k.market_id = id.into();
        let response = client.finality(k).await.unwrap();
        assert_eq!(response.status, expected);
        assert!(!response.settlement_import_allowed);
    }
    client.terms(key.clone()).await.unwrap();
    let book = client
        .official_book(key, "stream".into(), "up".into())
        .await
        .unwrap();
    assert!(!book.cache_used && !book.ws_dependency);
    let rest = client
        .official_btc_trade(common::Source::BinanceSpotTrade)
        .await
        .unwrap();
    assert_eq!(rest.latest_trade.unwrap().price, "100.00");
    assert!(!rest.cache_used && !rest.ws_dependency);
    client.close_and_wait().await.unwrap();
    server.abort();
}
