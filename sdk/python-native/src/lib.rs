//! Thin bindings: all transport, sequence and lifecycle logic lives in Rust SDK.
mod cold;
mod public_data;
mod resources;
mod service;
mod fact_views {
    include!(concat!(env!("OUT_DIR"), "/fact_views.rs"));
}
use marketcow_sdk::{
    Client, Error, PriceDelivery, SharedPrices, btc, common, history::TradeHistory, wire_history,
};
use pyo3::{create_exception, exceptions::PyException, prelude::*};
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

create_exception!(_native, NativeError, PyException);

fn error(value: Error) -> PyErr {
    let exception = NativeError::new_err(value.to_string());
    Python::attach(|py| {
        let object = exception.value(py);
        let _ = object.setattr("page", py.None());
        let _ = object.setattr("failure", py.None());
        let _ = object.setattr("retryable", value.retryable());
        let _ = object.setattr(
            "code",
            match &value {
                Error::Io(_) => "local_io",
                Error::Timeout(_) => "timeout",
                Error::Configuration(_) => "invalid_configuration",
                Error::Transport(_) => "transport_error",
                Error::WebSocket(_) => "websocket_error",
                Error::Decode(_) => "invalid_protobuf",
                Error::Protocol(_) => "protocol_error",
                Error::Limit(_) => "resource_limit",
                Error::Cancelled => "cancelled",
                Error::Remote { .. } => "remote_error",
                Error::HistoryGap { .. } => "history_gap",
            },
        );
        match &value {
            Error::Remote {
                status,
                code,
                retry_after_ms,
                detail,
                ..
            } => {
                let _ = object.setattr("status", *status);
                let _ = object.setattr("code", code.as_str());
                let _ = object.setattr("retry_after_ms", *retry_after_ms);
                let _ = object.setattr("detail", detail.as_deref());
                if let Error::Remote {
                    failure: Some(failure),
                    ..
                } = &value
                {
                    let _ = object.setattr("request_id", failure.request_id.as_str());
                    let _ = object.setattr(
                        "failure",
                        fact_views::FactFactsFailure(Arc::new((**failure).clone())),
                    );
                }
            }
            Error::HistoryGap {
                reason,
                missing,
                page,
            } => {
                let _ = object.setattr("code", "history_gap");
                let _ = object.setattr("reason", reason.as_str());
                let _ = object.setattr("missing_ranges", missing.clone());
                if let Some(page) = page {
                    let _ = object.setattr(
                        "page",
                        fact_views::FactWireHistoryPage(Arc::new((**page).clone())),
                    );
                }
            }
            _ => {}
        }
    });
    exception
}

#[pyclass(name = "Trade", frozen, module = "marketcow_sdk._native")]
#[derive(Clone)]
struct Trade(btc::Trade);

#[pymethods]
impl Trade {
    #[getter]
    fn fact(&self) -> fact_views::FactBtcTrade {
        fact_views::FactBtcTrade(Arc::new(self.0.clone()))
    }
    #[getter]
    fn trade_id(&self) -> u64 {
        self.0.trade_id
    }
    #[getter]
    fn trade_time_ms(&self) -> i64 {
        self.0.trade_time_ms
    }
    #[getter]
    fn event_time_ms(&self) -> Option<i64> {
        self.0.event_time_ms
    }
    #[getter]
    fn price(&self) -> &str {
        &self.0.price
    }
    #[getter]
    fn quantity(&self) -> &str {
        &self.0.quantity
    }
    #[getter]
    fn buyer_is_maker(&self) -> Option<bool> {
        self.0.buyer_is_maker
    }
    #[getter]
    fn received_at_ns(&self) -> Option<i64> {
        self.0.received_at_ns
    }
    #[getter]
    fn evidence_id(&self) -> Option<&str> {
        self.0.evidence.as_ref().map(|e| e.evidence_id.as_str())
    }
}

#[pyclass(name = "Observation", frozen, module = "marketcow_sdk._native")]
#[derive(Clone)]
struct Observation(btc::Observation);

#[pymethods]
impl Observation {
    #[getter]
    fn fact(&self) -> fact_views::FactBtcObservation {
        fact_views::FactBtcObservation(Arc::new(self.0.clone()))
    }
    #[getter]
    fn received_at_ns(&self) -> Option<i64> {
        self.0.received_at_ns
    }
    #[getter]
    fn evidence_id(&self) -> Option<&str> {
        self.0.evidence.as_ref().map(|e| e.evidence_id.as_str())
    }
    #[getter]
    fn source_time_ms(&self) -> i64 {
        self.0.source_time_ms
    }
    #[getter]
    fn price(&self) -> &str {
        &self.0.price
    }
    #[getter]
    fn source_sequence(&self) -> Option<u64> {
        self.0.source_sequence
    }
    #[getter]
    fn row_index(&self) -> Option<u32> {
        self.0.row_index
    }
}

#[pyclass(name = "PriceEvent", frozen, module = "marketcow_sdk._native")]
struct PriceEvent {
    frame: Arc<btc::PriceFrame>,
    available: bool,
}

#[pymethods]
impl PriceEvent {
    #[getter]
    fn frame(&self) -> fact_views::FactBtcPriceFrame {
        fact_views::FactBtcPriceFrame(self.frame.clone())
    }
    #[getter]
    fn phase(&self) -> Option<i32> {
        match &self.frame.payload {
            Some(btc::price_frame::Payload::Status(s)) => Some(s.phase),
            _ => None,
        }
    }
    #[getter]
    fn kind(&self) -> &'static str {
        match &self.frame.payload {
            Some(btc::price_frame::Payload::Snapshot(_)) => "snapshot",
            Some(btc::price_frame::Payload::Trade(_)) => "trade",
            Some(btc::price_frame::Payload::Observations(_)) => "observations",
            Some(btc::price_frame::Payload::Status(_)) => "status",
            _ => "error",
        }
    }
    #[getter]
    fn source(&self) -> i32 {
        self.frame.source
    }
    #[getter]
    fn instrument(&self) -> &str {
        &self.frame.instrument_id
    }
    #[getter]
    fn stream_id(&self) -> &str {
        &self.frame.stream_id
    }
    #[getter]
    fn stream_sequence(&self) -> u64 {
        self.frame.stream_sequence
    }
    #[getter]
    fn source_sequence(&self) -> Option<u64> {
        self.frame.source_sequence
    }
    #[getter]
    fn published_at_ns(&self) -> i64 {
        self.frame.published_at_ns
    }
    #[getter]
    fn signed_age_ms(&self) -> Option<i64> {
        self.frame.latest_source_age_ms_at_emit
    }
    #[getter]
    fn available(&self) -> bool {
        self.available
    }
    #[getter]
    fn trade(&self) -> Option<Trade> {
        match &self.frame.payload {
            Some(btc::price_frame::Payload::Trade(delta)) => delta.trade.clone().map(Trade),
            _ => None,
        }
    }
    // Full snapshot copies occur only when explicitly requested, never per delta.
    #[getter]
    fn trades(&self) -> Vec<Trade> {
        match &self.frame.payload {
            Some(btc::price_frame::Payload::Snapshot(s)) => {
                s.trades.iter().cloned().map(Trade).collect()
            }
            _ => Vec::new(),
        }
    }
    #[getter]
    fn observations(&self) -> Vec<Observation> {
        let rows = match &self.frame.payload {
            Some(btc::price_frame::Payload::Snapshot(s)) => &s.observations,
            Some(btc::price_frame::Payload::Observations(s)) => &s.observations,
            _ => return Vec::new(),
        };
        rows.iter().cloned().map(Observation).collect()
    }
    #[getter]
    fn reason(&self) -> Option<&str> {
        match &self.frame.payload {
            Some(btc::price_frame::Payload::Status(s)) => s.reason.as_deref(),
            _ => None,
        }
    }
}

#[pyclass(name = "HistoryPage", frozen, module = "marketcow_sdk._native")]
struct HistoryPage(Arc<wire_history::Page>, Option<bool>, u32);

#[pymethods]
impl HistoryPage {
    #[getter]
    fn fact(&self) -> fact_views::FactWireHistoryPage {
        fact_views::FactWireHistoryPage(self.0.clone())
    }
    #[getter]
    fn coverage_verified(&self) -> bool {
        self.0.coverage_verified
    }
    #[getter]
    fn coverage_first_time_ms(&self) -> Option<i64> {
        self.0.coverage_first_time_ms
    }
    #[getter]
    fn coverage_last_time_ms(&self) -> Option<i64> {
        self.0.coverage_last_time_ms
    }
    #[getter]
    fn database_revision(&self) -> u64 {
        self.0.database_revision
    }
    #[getter]
    fn start_time_ms(&self) -> Option<i64> {
        self.0.range.as_ref().map(|r| r.start_time_ms)
    }
    #[getter]
    fn through_trade_id(&self) -> Option<u64> {
        self.0.range.as_ref().map(|r| r.through_trade_id)
    }
    #[getter]
    fn session_id(&self) -> &str {
        &self.0.session_id
    }
    #[getter]
    fn page_index(&self) -> u32 {
        self.0.page_index
    }
    #[getter]
    fn page_count(&self) -> u32 {
        self.0.page_count
    }
    #[getter]
    fn complete(&self) -> bool {
        self.1.unwrap_or(self.0.complete)
    }
    #[getter]
    fn segment_complete(&self) -> bool {
        self.0.complete
    }
    #[getter]
    fn segment_index(&self) -> u32 {
        self.2
    }
    #[getter]
    fn database_state(&self) -> Option<&str> {
        self.0
            .database_health_at_freeze
            .as_ref()
            .map(|h| h.state.as_str())
    }
    #[getter]
    fn database_reason(&self) -> Option<&str> {
        self.0.database_health_at_freeze.as_ref()?.reason.as_deref()
    }
    #[getter]
    fn total_count(&self) -> u64 {
        self.0.total_trade_count
    }
    #[getter]
    fn predecessor(&self) -> Option<Trade> {
        self.0.predecessor.clone().map(Trade)
    }
    #[getter]
    fn trades(&self) -> Vec<Trade> {
        self.0.trades.iter().cloned().map(Trade).collect()
    }
}

#[pyclass(name = "NativeClient", module = "marketcow_sdk._native")]
struct NativeClient(Client);

#[pymethods]
impl NativeClient {
    #[new]
    #[pyo3(signature = (endpoint, api_key=None))]
    fn new(endpoint: &str, api_key: Option<String>) -> PyResult<Self> {
        // Construct reqwest within the shared native runtime, not a new runtime
        // per request or a Python local daemon.
        let _guard = pyo3_async_runtimes::tokio::get_runtime().enter();
        let mut builder = Client::builder(endpoint);
        if let Some(token) = api_key {
            builder = builder.api_key(token);
        }
        Ok(Self(builder.build().map_err(error)?))
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { client.close_and_wait().await.map_err(error) })
        })
    }
    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.0.close();
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.close_and_wait().await.map_err(error)
        })
    }
    #[getter]
    fn closed(&self) -> bool {
        self.0.is_closed()
    }
    fn usage(&self) -> service::Usage {
        self.0.usage().into()
    }
    fn server_usage(&self, py: Python<'_>) -> PyResult<service::ServerUsage> {
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                client
                    .server_usage()
                    .await
                    .map(service::ServerUsage::from)
                    .map_err(error)
            })
        })
    }
    fn server_usage_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .server_usage()
                .await
                .map(service::ServerUsage::from)
                .map_err(error)
        })
    }
    fn connect(&self, py: Python<'_>) -> PyResult<service::Capabilities> {
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                client
                    .negotiate()
                    .await
                    .map(service::Capabilities)
                    .map_err(error)
            })
        })
    }
    fn connect_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .negotiate()
                .await
                .map(service::Capabilities)
                .map_err(error)
        })
    }
    fn latest_price(&self, py: Python<'_>, source: i32) -> PyResult<PriceEvent> {
        let source =
            common::Source::try_from(source).map_err(|_| NativeError::new_err("unknown source"))?;
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                client
                    .latest_price(source)
                    .await
                    .map(|frame| PriceEvent {
                        available: matches!(
                            &frame.payload,
                            Some(btc::price_frame::Payload::Snapshot(_))
                        ),
                        frame,
                    })
                    .map_err(error)
            })
        })
    }
    fn latest_price_async<'py>(&self, py: Python<'py>, source: i32) -> PyResult<Bound<'py, PyAny>> {
        let source =
            common::Source::try_from(source).map_err(|_| NativeError::new_err("unknown source"))?;
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .latest_price(source)
                .await
                .map(|frame| PriceEvent {
                    available: matches!(
                        &frame.payload,
                        Some(btc::price_frame::Payload::Snapshot(_))
                    ),
                    frame,
                })
                .map_err(error)
        })
    }

    fn market_directory(&self, py: Python<'_>) -> PyResult<resources::MarketDirectory> {
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(resources::directory(client))
        })
    }
    fn market_directory_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, resources::directory(self.0.clone()))
    }
    fn markets(&self, py: Python<'_>) -> PyResult<Vec<resources::Market>> {
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(resources::list(client))
        })
    }
    fn markets_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, resources::list(self.0.clone()))
    }
    fn subscribe_market(
        &self,
        py: Python<'_>,
        market: PyRef<'_, resources::Market>,
    ) -> PyResult<resources::MarketStream> {
        let client = self.0.clone();
        let market = market.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(resources::subscribe(client, market))
        })
    }
    fn subscribe_market_async<'py>(
        &self,
        py: Python<'py>,
        market: PyRef<'_, resources::Market>,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            resources::subscribe(self.0.clone(), market.clone()),
        )
    }
    fn follow_market(&self) -> resources::MarketStream {
        let _guard = pyo3_async_runtimes::tokio::get_runtime().enter();
        resources::follow(self.0.clone())
    }
    fn rules(&self, py: Python<'_>, id: String, revision: u64) -> PyResult<resources::Rules> {
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                client
                    .rules(&id, revision)
                    .await
                    .map(resources::Rules)
                    .map_err(error)
            })
        })
    }
    fn rules_async<'py>(
        &self,
        py: Python<'py>,
        id: String,
        revision: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .rules(&id, revision)
                .await
                .map(resources::Rules)
                .map_err(error)
        })
    }
    fn reference(
        &self,
        py: Python<'_>,
        market: PyRef<'_, resources::Market>,
    ) -> PyResult<resources::Reference> {
        let client = self.0.clone();
        let key = market
            .binding
            .key
            .clone()
            .ok_or_else(|| NativeError::new_err("missing market key"))?;
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                client
                    .reference(key)
                    .await
                    .map(resources::Reference)
                    .map_err(error)
            })
        })
    }
    fn reference_async<'py>(
        &self,
        py: Python<'py>,
        market: PyRef<'_, resources::Market>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.0.clone();
        let key = market
            .binding
            .key
            .clone()
            .ok_or_else(|| NativeError::new_err("missing market key"))?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .reference(key)
                .await
                .map(resources::Reference)
                .map_err(error)
        })
    }
    fn finality(
        &self,
        py: Python<'_>,
        market: PyRef<'_, resources::Market>,
    ) -> PyResult<resources::Finality> {
        let client = self.0.clone();
        let key = market
            .binding
            .key
            .clone()
            .ok_or_else(|| NativeError::new_err("missing market key"))?;
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                client
                    .finality(key)
                    .await
                    .map(resources::Finality)
                    .map_err(error)
            })
        })
    }
    fn finality_async<'py>(
        &self,
        py: Python<'py>,
        market: PyRef<'_, resources::Market>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.0.clone();
        let key = market
            .binding
            .key
            .clone()
            .ok_or_else(|| NativeError::new_err("missing market key"))?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .finality(key)
                .await
                .map(resources::Finality)
                .map_err(error)
        })
    }
    fn evidence(&self, py: Python<'_>, id: String) -> PyResult<resources::Evidence> {
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                client
                    .evidence(&id)
                    .await
                    .map(resources::Evidence)
                    .map_err(error)
            })
        })
    }
    fn evidence_async<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .evidence(&id)
                .await
                .map(resources::Evidence)
                .map_err(error)
        })
    }

    fn subscribe(&self, py: Python<'_>, source: i32) -> PyResult<NativePriceStream> {
        let source =
            common::Source::try_from(source).map_err(|_| NativeError::new_err("unknown source"))?;
        let client = self.0.clone();
        let stream = py
            .detach(move || {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(async move { client.shared_prices(source) })
            })
            .map_err(error)?;
        Ok(NativePriceStream(
            Arc::new(Mutex::new(stream)),
            watch::channel(false).0,
        ))
    }
    fn subscribe_async<'py>(&self, py: Python<'py>, source: i32) -> PyResult<Bound<'py, PyAny>> {
        let source =
            common::Source::try_from(source).map_err(|_| NativeError::new_err("unknown source"))?;
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stream = client.shared_prices(source).map_err(error)?;
            Ok(NativePriceStream(
                Arc::new(Mutex::new(stream)),
                watch::channel(false).0,
            ))
        })
    }
    fn finalized_bars(&self, py: Python<'_>, interval_ms: u32) -> PyResult<service::BarWindow> {
        let interval = service::interval(interval_ms)?;
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                client
                    .finalized_bars(interval)
                    .await
                    .map(service::BarWindow)
                    .map_err(error)
            })
        })
    }
    fn subscribe_bars(&self, interval_ms: u32) -> PyResult<service::Bars> {
        let _guard = pyo3_async_runtimes::tokio::get_runtime().enter();
        let interval = service::interval(interval_ms)?;
        Ok(service::Bars(
            Arc::new(Mutex::new(self.0.subscribe_bars(interval).map_err(error)?)),
            watch::channel(false).0,
        ))
    }
    fn finalized_bars_async<'py>(
        &self,
        py: Python<'py>,
        interval_ms: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let interval = service::interval(interval_ms)?;
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .finalized_bars(interval)
                .await
                .map(service::BarWindow)
                .map_err(error)
        })
    }
    fn btc_capabilities(&self, py: Python<'_>) -> PyResult<service::Capabilities> {
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                client
                    .btc_capabilities()
                    .await
                    .map(service::Capabilities)
                    .map_err(error)
            })
        })
    }
    fn btc_capabilities_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .btc_capabilities()
                .await
                .map(service::Capabilities)
                .map_err(error)
        })
    }
    fn btc_health(&self, py: Python<'_>) -> PyResult<service::Health> {
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                client
                    .btc_health()
                    .await
                    .map(service::Health)
                    .map_err(error)
            })
        })
    }
    fn btc_health_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .btc_health()
                .await
                .map(service::Health)
                .map_err(error)
        })
    }
    #[pyo3(signature = (source, start_time_ms, end_time_ms, limit=1000))]
    fn observation_history(
        &self,
        source: i32,
        start_time_ms: i64,
        end_time_ms: i64,
        limit: u32,
    ) -> PyResult<NativeObservationHistory> {
        let _guard = pyo3_async_runtimes::tokio::get_runtime().enter();
        let source =
            common::Source::try_from(source).map_err(|_| NativeError::new_err("unknown source"))?;
        Ok(NativeObservationHistory(Arc::new(Mutex::new(
            ObservationOwner::Fixed(
                self.0
                    .observation_history(source, start_time_ms, end_time_ms, limit)
                    .map_err(error)?,
            ),
        ))))
    }
    fn observation_range(
        &self,
        py: Python<'_>,
        source: i32,
        start_time_ms: i64,
        end_time_ms: i64,
        limit: u32,
    ) -> PyResult<NativeObservationHistory> {
        let source =
            common::Source::try_from(source).map_err(|_| NativeError::new_err("unknown source"))?;
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let owner = client
                    .observations_between(source, start_time_ms, end_time_ms, limit)
                    .await
                    .map_err(error)?;
                Ok(NativeObservationHistory(Arc::new(Mutex::new(
                    ObservationOwner::Time(owner),
                ))))
            })
        })
    }
    fn observation_range_async<'py>(
        &self,
        py: Python<'py>,
        source: i32,
        start_time_ms: i64,
        end_time_ms: i64,
        limit: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let source =
            common::Source::try_from(source).map_err(|_| NativeError::new_err("unknown source"))?;
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let owner = client
                .observations_between(source, start_time_ms, end_time_ms, limit)
                .await
                .map_err(error)?;
            Ok(NativeObservationHistory(Arc::new(Mutex::new(
                ObservationOwner::Time(owner),
            ))))
        })
    }
    fn resume_history(&self, py: Python<'_>, path: String) -> PyResult<NativeHistory> {
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                client
                    .resume_trade_history(path)
                    .await
                    .map(|h| NativeHistory(Arc::new(Mutex::new(HistoryOwner::Fixed(h)))))
                    .map_err(error)
            })
        })
    }
    fn resume_history_async<'py>(
        &self,
        py: Python<'py>,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .resume_trade_history(path)
                .await
                .map(|h| NativeHistory(Arc::new(Mutex::new(HistoryOwner::Fixed(h)))))
                .map_err(error)
        })
    }
    #[pyo3(signature = (start_time_ms, through_trade_id, limit=1000))]
    fn trade_history(
        &self,
        start_time_ms: i64,
        through_trade_id: u64,
        limit: u32,
    ) -> PyResult<NativeHistory> {
        let _guard = pyo3_async_runtimes::tokio::get_runtime().enter();
        let history = self
            .0
            .trade_history(start_time_ms, through_trade_id, limit)
            .map_err(error)?;
        Ok(NativeHistory(Arc::new(Mutex::new(HistoryOwner::Fixed(
            history,
        )))))
    }
    #[pyo3(signature=(start_time_ms,end_time_ms=None,limit=1000))]
    fn time_history(
        &self,
        py: Python<'_>,
        start_time_ms: i64,
        end_time_ms: Option<i64>,
        limit: u32,
    ) -> PyResult<NativeHistory> {
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let history = client
                    .history_between(start_time_ms, end_time_ms, limit)
                    .await
                    .map_err(error)?;
                Ok(NativeHistory(Arc::new(Mutex::new(HistoryOwner::Time(
                    history,
                )))))
            })
        })
    }
    #[pyo3(signature=(start_time_ms,end_time_ms=None,limit=1000))]
    fn time_history_async<'py>(
        &self,
        py: Python<'py>,
        start_time_ms: i64,
        end_time_ms: Option<i64>,
        limit: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let history = client
                .history_between(start_time_ms, end_time_ms, limit)
                .await
                .map_err(error)?;
            Ok(NativeHistory(Arc::new(Mutex::new(HistoryOwner::Time(
                history,
            )))))
        })
    }
}

#[pyclass(name = "NativePriceStream", module = "marketcow_sdk._native")]
struct NativePriceStream(Arc<Mutex<SharedPrices>>, watch::Sender<bool>);

#[pyclass(name = "ConnectionEvent", frozen, module = "marketcow_sdk._native")]
struct ConnectionEvent {
    #[pyo3(get)]
    kind: &'static str,
    #[pyo3(get)]
    reason: String,
    #[pyo3(get)]
    retry_after_ms: u64,
    #[pyo3(get)]
    available: bool,
}

async fn next_price(
    stream: Arc<Mutex<SharedPrices>>,
    mut cancel: watch::Receiver<bool>,
) -> PyResult<Option<Py<PyAny>>> {
    let mut stream = stream.lock().await;
    let event = tokio::select! { biased;
        _ = cancel.wait_for(|closed| *closed) => { stream.close(); return Ok(None); },
        result = stream.next() => result.map_err(error)?,
    };
    Python::attach(|py| {
        event
            .map(|event| match event {
                PriceDelivery::Frame { frame, available } => {
                    Py::new(py, PriceEvent { frame, available }).map(|v| v.into_any())
                }
                PriceDelivery::Recovering {
                    reason,
                    retry_after_ms,
                } => Py::new(
                    py,
                    ConnectionEvent {
                        kind: "recovering",
                        reason: reason.to_string(),
                        retry_after_ms,
                        available: false,
                    },
                )
                .map(|v| v.into_any()),
            })
            .transpose()
    })
}

#[pymethods]
impl NativePriceStream {
    fn next(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let stream = self.0.clone();
        let cancel = self.1.subscribe();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(next_price(stream, cancel))
        })
    }
    fn next_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            next_price(self.0.clone(), self.1.subscribe()),
        )
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.1.send_replace(true);
        let stream = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                stream.lock().await.close();
                Ok(())
            })
        })
    }
    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.1.send_replace(true);
        let stream = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            stream.lock().await.close();
            Ok(())
        })
    }
}

#[pyclass(name = "NativeHistory", module = "marketcow_sdk._native")]
struct NativeHistory(Arc<Mutex<HistoryOwner>>);
#[pyclass(
    name = "HistoryLifecycle",
    frozen,
    get_all,
    module = "marketcow_sdk._native"
)]
struct HistoryLifecycle {
    session_id: Option<String>,
    retrieval_complete: bool,
    released: bool,
    release_error: Option<String>,
    release_receipt: Option<fact_views::FactWireHistoryReleaseReceipt>,
}
enum HistoryOwner {
    Fixed(TradeHistory),
    Time(marketcow_sdk::time_history::TimeHistory),
}
impl HistoryOwner {
    async fn next_page(&mut self) -> marketcow_sdk::Result<Option<HistoryPage>> {
        match self {
            Self::Fixed(history) => history
                .next_page()
                .await
                .map(|p| p.map(|p| HistoryPage(Arc::new(p), None, 0))),
            Self::Time(history) => history.next_page().await.map(|p| {
                p.map(|p| HistoryPage(Arc::new(p.page), Some(p.query_complete), p.segment_index))
            }),
        }
    }
    async fn close(&mut self) -> marketcow_sdk::Result<()> {
        match self {
            Self::Fixed(history) => history.close().await,
            Self::Time(history) => history.close().await,
        }
    }
}

#[pymethods]
impl NativeHistory {
    fn lifecycle(&self, py: Python<'_>) -> PyResult<HistoryLifecycle> {
        let state = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { lifecycle(&*state.lock().await) })
        })
    }
    fn lifecycle_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            async move { lifecycle(&*state.lock().await) },
        )
    }
    fn defer_release(&self, py: Python<'_>) -> PyResult<()> {
        let state = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                match &mut *state.lock().await {
                    HistoryOwner::Fixed(h) => h.retain_until_close().map_err(error),
                    HistoryOwner::Time(_) => Err(NativeError::new_err(
                        "deferred release requires fixed-ID trades_through",
                    )),
                }
            })
        })
    }
    fn save_checkpoint(&self, py: Python<'_>, path: String) -> PyResult<()> {
        let state = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                match &*state.lock().await {
                    HistoryOwner::Fixed(h) => h.save_checkpoint(path).await.map_err(error),
                    HistoryOwner::Time(_) => Err(NativeError::new_err(
                        "checkpoint requires fixed-ID trades_through job",
                    )),
                }
            })
        })
    }
    fn save_checkpoint_async<'py>(
        &self,
        py: Python<'py>,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match &*state.lock().await {
                HistoryOwner::Fixed(h) => h.save_checkpoint(path).await.map_err(error),
                HistoryOwner::Time(_) => Err(NativeError::new_err(
                    "checkpoint requires fixed-ID trades_through job",
                )),
            }
        })
    }
    fn next(&self, py: Python<'_>) -> PyResult<Option<HistoryPage>> {
        let history = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { history.lock().await.next_page().await.map_err(error) })
        })
    }
    fn next_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let history = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            history.lock().await.next_page().await.map_err(error)
        })
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let history = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { history.lock().await.close().await.map_err(error) })
        })
    }
    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let history = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            history.lock().await.close().await.map_err(error)
        })
    }
}

fn lifecycle(owner: &HistoryOwner) -> PyResult<HistoryLifecycle> {
    match owner {
        HistoryOwner::Fixed(h) => Ok(HistoryLifecycle {
            session_id: h.session_id().map(str::to_owned),
            retrieval_complete: h.is_complete(),
            released: h.is_released(),
            release_error: h.release_error().map(str::to_owned),
            release_receipt: h
                .release_receipt()
                .cloned()
                .map(|r| fact_views::FactWireHistoryReleaseReceipt(Arc::new(r))),
        }),
        HistoryOwner::Time(_) => Err(NativeError::new_err(
            "lifecycle receipt requires fixed-ID trades_through",
        )),
    }
}

#[pyclass(name = "ObservationRecord", frozen, module = "marketcow_sdk._native")]
struct ObservationRecord(wire_history::ObservationRecord);
#[pymethods]
impl ObservationRecord {
    #[getter]
    fn observation_id(&self) -> u64 {
        self.0.observation_id
    }
    #[getter]
    fn source_generation(&self) -> &str {
        &self.0.source_generation
    }
    #[getter]
    fn observation(&self) -> Option<Observation> {
        self.0.observation.clone().map(Observation)
    }
}
#[pyclass(name = "ObservationPage", frozen, module = "marketcow_sdk._native")]
struct ObservationPage(Arc<wire_history::ObservationPage>, Option<bool>, u32);
#[pymethods]
impl ObservationPage {
    #[getter]
    fn fact(&self) -> fact_views::FactWireHistoryObservationPage {
        fact_views::FactWireHistoryObservationPage(self.0.clone())
    }
    #[getter]
    fn observations(&self) -> Vec<ObservationRecord> {
        self.0
            .observations
            .iter()
            .cloned()
            .map(ObservationRecord)
            .collect()
    }
    #[getter]
    fn complete(&self) -> bool {
        self.1.unwrap_or(self.0.complete)
    }
    #[getter]
    fn segment_complete(&self) -> bool {
        self.0.complete
    }
    #[getter]
    fn segment_index(&self) -> u32 {
        self.2
    }
    #[getter]
    fn coverage_verified(&self) -> bool {
        self.0.coverage_verified
    }
    #[getter]
    fn reason(&self) -> Option<&str> {
        self.0.reason.as_deref()
    }
    #[getter]
    fn source(&self) -> Option<i32> {
        self.0.range.as_ref().map(|r| r.source)
    }
    #[getter]
    fn start_time_ms(&self) -> Option<i64> {
        self.0.range.as_ref().map(|r| r.start_time_ms)
    }
    #[getter]
    fn end_time_ms(&self) -> Option<i64> {
        self.0.range.as_ref().map(|r| r.end_time_ms)
    }
    #[getter]
    fn frozen_high_observation_id(&self) -> u64 {
        self.0.frozen_high_observation_id
    }
    #[getter]
    fn session_id(&self) -> &str {
        &self.0.session_id
    }
    #[getter]
    fn database_state(&self) -> Option<&str> {
        self.0
            .database_health_at_freeze
            .as_ref()
            .map(|h| h.state.as_str())
    }
}
#[pyclass(name = "NativeObservationHistory", module = "marketcow_sdk._native")]
struct NativeObservationHistory(Arc<Mutex<ObservationOwner>>);
enum ObservationOwner {
    Fixed(marketcow_sdk::observation_history::ObservationHistory),
    Time(marketcow_sdk::observation_history::ObservationTimeHistory),
}
impl ObservationOwner {
    async fn next_page(&mut self) -> marketcow_sdk::Result<Option<ObservationPage>> {
        match self {
            Self::Fixed(owner) => owner
                .next_page()
                .await
                .map(|p| p.map(|p| ObservationPage(Arc::new(p), None, 0))),
            Self::Time(owner) => owner.next_page().await.map(|p| {
                p.map(|p| {
                    ObservationPage(Arc::new(p.page), Some(p.query_complete), p.segment_index)
                })
            }),
        }
    }
    async fn close(&mut self) -> marketcow_sdk::Result<()> {
        match self {
            Self::Fixed(owner) => owner.close().await,
            Self::Time(owner) => owner.close().await,
        }
    }
}
#[pymethods]
impl NativeObservationHistory {
    fn next(&self, py: Python<'_>) -> PyResult<Option<ObservationPage>> {
        let state = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { state.lock().await.next_page().await.map_err(error) })
        })
    }
    fn next_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            state.lock().await.next_page().await.map_err(error)
        })
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let state = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { state.lock().await.close().await.map_err(error) })
        })
    }
    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            state.lock().await.close().await.map_err(error)
        })
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    fact_views::register(module)?;
    service::register(module)?;
    public_data::register(module)?;
    resources::register(module)?;
    module.add("NativeError", module.py().get_type::<NativeError>())?;
    module.add_class::<NativeClient>()?;
    module.add_class::<NativePriceStream>()?;
    module.add_class::<NativeHistory>()?;
    module.add_class::<HistoryLifecycle>()?;
    module.add_class::<Trade>()?;
    module.add_class::<Observation>()?;
    module.add_class::<PriceEvent>()?;
    module.add_class::<ConnectionEvent>()?;
    module.add_class::<HistoryPage>()?;
    module.add_class::<ObservationRecord>()?;
    module.add_class::<ObservationPage>()?;
    module.add_class::<NativeObservationHistory>()?;
    Ok(())
}
