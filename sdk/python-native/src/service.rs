//! Small typed diagnostic results; never convert through JSON/dicts.
use marketcow_sdk::{btc, wire_history};
use pyo3::prelude::*;

#[pyclass(name = "SourceCapability", frozen, module = "marketcow_sdk._native")]
pub(crate) struct SourceCapability(btc::SourceCapability);
#[pymethods]
impl SourceCapability {
    #[getter]
    fn source(&self) -> i32 {
        self.0.source
    }
    #[getter]
    fn instrument_id(&self) -> &str {
        &self.0.instrument_id
    }
    #[getter]
    fn price_stream(&self) -> bool {
        self.0.price_stream
    }
    #[getter]
    fn indexed_history(&self) -> bool {
        self.0.indexed_history
    }
    #[getter]
    fn collector_present(&self) -> bool {
        self.0.collector_present
    }
}
#[pyclass(name = "BtcCapabilities", frozen, module = "marketcow_sdk._native")]
pub(crate) struct Capabilities(pub btc::BtcCapabilities);
#[pymethods]
impl Capabilities {
    #[getter]
    fn sources(&self) -> Vec<SourceCapability> {
        self.0
            .sources
            .iter()
            .cloned()
            .map(SourceCapability)
            .collect()
    }
    #[getter]
    fn history_page_limit(&self) -> u32 {
        self.0.history_page_limit
    }
    #[getter]
    fn history_maximum_pages(&self) -> u32 {
        self.0.history_maximum_pages
    }
    #[getter]
    fn history_maximum_rows(&self) -> u64 {
        self.0.history_maximum_rows
    }
    #[getter]
    fn history_maximum_bytes(&self) -> u64 {
        self.0.history_maximum_bytes
    }
    #[getter]
    fn history_maximum_span_ms(&self) -> u32 {
        self.0.history_maximum_span_ms
    }
    #[getter]
    fn history_session_ttl_seconds(&self) -> u32 {
        self.0.history_session_ttl_seconds
    }
    #[getter]
    fn history_explicit_release(&self) -> bool {
        self.0.history_explicit_release
    }
    #[getter]
    fn history_request_identity(&self) -> bool {
        self.0.history_request_identity
    }
}
#[pyclass(name = "WriterHealth", frozen, module = "marketcow_sdk._native")]
pub(crate) struct WriterHealth(wire_history::Health);
#[pymethods]
impl WriterHealth {
    #[getter]
    fn state(&self) -> &str {
        &self.0.state
    }
    #[getter]
    fn reason(&self) -> Option<&str> {
        self.0.reason.as_deref()
    }
    #[getter]
    fn last_success_sequence(&self) -> Option<u64> {
        self.0.last_success_sequence
    }
    #[getter]
    fn last_success_source_time_ms(&self) -> Option<i64> {
        self.0.last_success_source_time_ms
    }
    #[getter]
    fn failed_at_ns(&self) -> Option<i64> {
        self.0.failed_at_ns
    }
    #[getter]
    fn dropped_records(&self) -> u64 {
        self.0.dropped_records
    }
}
#[pyclass(name = "SourceHealth", frozen, module = "marketcow_sdk._native")]
pub(crate) struct SourceHealth(btc::SourceHealth);
#[pymethods]
impl SourceHealth {
    #[getter]
    fn source(&self) -> i32 {
        self.0.source
    }
    #[getter]
    fn instrument_id(&self) -> &str {
        &self.0.instrument_id
    }
    #[getter]
    fn phase(&self) -> &str {
        &self.0.phase
    }
    #[getter]
    fn reason(&self) -> Option<&str> {
        self.0.reason.as_deref()
    }
    #[getter]
    fn stream_id(&self) -> Option<&str> {
        self.0.stream_id.as_deref()
    }
    #[getter]
    fn stream_sequence(&self) -> Option<u64> {
        self.0.stream_sequence
    }
    #[getter]
    fn source_sequence(&self) -> Option<u64> {
        self.0.source_sequence
    }
    #[getter]
    fn latest_source_time_ms(&self) -> Option<i64> {
        self.0.latest_source_time_ms
    }
    #[getter]
    fn signed_source_age_ms(&self) -> Option<i64> {
        self.0.signed_source_age_ms
    }
    #[getter]
    fn database(&self) -> Option<WriterHealth> {
        self.0.database.clone().map(WriterHealth)
    }
    #[getter]
    fn archive(&self) -> Option<WriterHealth> {
        self.0.archive.clone().map(WriterHealth)
    }
}
#[pyclass(name = "BtcHealth", frozen, module = "marketcow_sdk._native")]
pub(crate) struct Health(pub btc::BtcHealth);
#[pymethods]
impl Health {
    #[getter]
    fn reported_at_ns(&self) -> i64 {
        self.0.reported_at_ns
    }
    #[getter]
    fn sources(&self) -> Vec<SourceHealth> {
        self.0.sources.iter().cloned().map(SourceHealth).collect()
    }
}
pub(crate) fn interval(ms: u32) -> PyResult<marketcow_sdk::bars::Interval> {
    match ms {
        1000 => Ok(marketcow_sdk::bars::Interval::Second),
        60000 => Ok(marketcow_sdk::bars::Interval::Minute),
        _ => Err(super::NativeError::new_err(
            "existing finalized intervals are 1000 or 60000ms",
        )),
    }
}
#[pyclass(name = "FinalizedBar", frozen, module = "marketcow_sdk._native")]
pub(crate) struct Bar(btc::FinalizedBar);
#[pymethods]
impl Bar {
    #[getter]
    fn open_time_ms(&self) -> i64 {
        self.0.open_time_ms
    }
    #[getter]
    fn close_time_ms(&self) -> i64 {
        self.0.close_time_ms
    }
    #[getter]
    fn open(&self) -> &str {
        &self.0.open
    }
    #[getter]
    fn high(&self) -> &str {
        &self.0.high
    }
    #[getter]
    fn low(&self) -> &str {
        &self.0.low
    }
    #[getter]
    fn close(&self) -> &str {
        &self.0.close
    }
    #[getter]
    fn base_volume(&self) -> &str {
        &self.0.base_volume
    }
    #[getter]
    fn quote_volume(&self) -> &str {
        &self.0.quote_volume
    }
    #[getter]
    fn trade_count(&self) -> u64 {
        self.0.trade_count
    }
    #[getter]
    fn taker_buy_base_volume(&self) -> &str {
        &self.0.taker_buy_base_volume
    }
    #[getter]
    fn taker_buy_quote_volume(&self) -> &str {
        &self.0.taker_buy_quote_volume
    }
    #[getter]
    fn source_event_time_ms(&self) -> Option<i64> {
        self.0.source_event_time_ms
    }
    #[getter]
    fn available_at(&self) -> Option<&str> {
        self.0.available_at.as_deref()
    }
    #[getter]
    fn provenance_kind(&self) -> Option<&str> {
        self.0.provenance_kind.as_deref()
    }
}
#[pyclass(name = "FinalizedWindow", frozen, module = "marketcow_sdk._native")]
pub(crate) struct BarWindow(pub btc::FinalizedWindow);
#[pymethods]
impl BarWindow {
    #[getter]
    fn after_open_ms(&self) -> Option<i64> {
        self.0.after_open_ms
    }
    #[getter]
    fn gap(&self) -> bool {
        self.0.gap
    }
    #[getter]
    fn ready(&self) -> bool {
        self.0.ready
    }
    #[getter]
    fn reason(&self) -> Option<&str> {
        self.0.reason.as_deref()
    }
    #[getter]
    fn interval_ms(&self) -> u32 {
        self.0.interval_ms
    }
    #[getter]
    fn stream_id(&self) -> &str {
        &self.0.stream_id
    }
    #[getter]
    fn checkpoint_sequence(&self) -> u64 {
        self.0.checkpoint_sequence
    }
    #[getter]
    fn durable_sequence(&self) -> u64 {
        self.0.durable_sequence
    }
    #[getter]
    fn expected_last_open_ms(&self) -> i64 {
        self.0.expected_last_open_ms
    }
    #[getter]
    fn bars(&self) -> Vec<Bar> {
        self.0.bars.iter().cloned().map(Bar).collect()
    }
}
#[pyclass(name = "Usage", frozen, get_all, module = "marketcow_sdk._native")]
pub(crate) struct Usage {
    pub http_requests: u64,
    pub http_response_bytes: u64,
    pub price_message_bytes: u64,
    pub market_message_bytes: u64,
    pub queued_event_bytes: usize,
}
#[pyclass(
    name = "ServerUsage",
    frozen,
    get_all,
    module = "marketcow_sdk._native"
)]
pub(crate) struct ServerUsage {
    customer_id: String,
    policy_revision: u64,
    window_started_at_unix_seconds: i64,
    response_payload_bytes: u64,
    requests: u64,
    maximum_response_payload_bytes: u64,
    quota_window_seconds: u32,
}
impl From<marketcow_sdk::common::SdkServerUsage> for ServerUsage {
    fn from(v: marketcow_sdk::common::SdkServerUsage) -> Self {
        Self {
            customer_id: v.customer_id,
            policy_revision: v.policy_revision,
            window_started_at_unix_seconds: v.window_started_at_unix_seconds,
            response_payload_bytes: v.response_payload_bytes,
            requests: v.requests,
            maximum_response_payload_bytes: v.maximum_response_payload_bytes,
            quota_window_seconds: v.quota_window_seconds,
        }
    }
}
#[pyclass(name = "NativeBars", module = "marketcow_sdk._native")]
pub(crate) struct Bars(
    pub std::sync::Arc<tokio::sync::Mutex<marketcow_sdk::bars::BarSubscription>>,
    pub tokio::sync::watch::Sender<bool>,
);
async fn next_bar(
    state: std::sync::Arc<tokio::sync::Mutex<marketcow_sdk::bars::BarSubscription>>,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) -> PyResult<Option<BarWindow>> {
    tokio::select! {biased;
        _=cancel.wait_for(|v|*v)=>Ok(None),
        result=async{state.lock().await.next().await}=>result.map(|v|v.map(BarWindow)).map_err(super::error),
    }
}
#[pymethods]
impl Bars {
    fn next(&self, py: Python<'_>) -> PyResult<Option<BarWindow>> {
        let state = self.0.clone();
        let cancel = self.1.subscribe();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(next_bar(state, cancel))
        })
    }
    fn next_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, next_bar(self.0.clone(), self.1.subscribe()))
    }
    fn close(&self) {
        self.1.send_replace(true);
    }
    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.1.send_replace(true);
        pyo3_async_runtimes::tokio::future_into_py(py, async { Ok(()) })
    }
}
impl From<marketcow_sdk::usage::Usage> for Usage {
    fn from(v: marketcow_sdk::usage::Usage) -> Self {
        Self {
            http_requests: v.http_requests,
            http_response_bytes: v.http_response_bytes,
            price_message_bytes: v.price_message_bytes,
            market_message_bytes: v.market_message_bytes,
            queued_event_bytes: v.queued_event_bytes,
        }
    }
}
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ServerUsage>()?;
    module.add_class::<Bars>()?;
    module.add_class::<Usage>()?;
    module.add_class::<Bar>()?;
    module.add_class::<BarWindow>()?;
    module.add_class::<SourceCapability>()?;
    module.add_class::<Capabilities>()?;
    module.add_class::<WriterHealth>()?;
    module.add_class::<SourceHealth>()?;
    module.add_class::<Health>()?;
    Ok(())
}
