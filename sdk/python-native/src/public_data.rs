use super::{NativeError, Trade, error, resources::Market};
use marketcow_sdk::{common, facts};
use pyo3::prelude::*;

#[pyclass(
    name = "InstrumentFacts",
    frozen,
    get_all,
    module = "marketcow_sdk._native"
)]
#[derive(Clone)]
struct Instrument {
    price_increment: Option<String>,
    minimum_order_size: Option<String>,
    size_increment: Option<String>,
    price_unit: String,
    size_unit: String,
    missing_fields: Vec<String>,
}
#[pyclass(name = "FeeFacts", frozen, get_all, module = "marketcow_sdk._native")]
#[derive(Clone)]
struct Fees {
    model: String,
    currency: Option<String>,
    maker_rate: Option<String>,
    taker_rate: Option<String>,
    exponent: Option<u32>,
    taker_only: Option<bool>,
    quantum: Option<String>,
    rounding_mode: Option<String>,
    effective_at_ns: Option<i64>,
    maker_base_fee_bps: Option<String>,
    taker_base_fee_bps: Option<String>,
    missing_fields: Vec<String>,
    applicability: String,
}
#[pyclass(
    name = "PublicOrderTerms",
    frozen,
    get_all,
    module = "marketcow_sdk._native"
)]
#[derive(Clone)]
struct OrderTerms {
    taker_order_delay_enabled: Option<bool>,
    marketable_hold_ms: Option<u64>,
    missing_fields: Vec<String>,
    documentation_evidence_ids: Vec<String>,
}
#[pyclass(
    name = "ExplicitLocalPolicy",
    frozen,
    get_all,
    module = "marketcow_sdk._native"
)]
#[derive(Clone)]
struct Policy {
    policy_id: Option<String>,
    revision: Option<u64>,
    origin: String,
    purpose: String,
    fee_currency: Option<String>,
    fee_quantum: Option<String>,
    rounding_mode: Option<String>,
    size_increment: Option<String>,
    fee_upper_bound_method: Option<String>,
}
#[pyclass(
    name = "PublicTerms",
    frozen,
    get_all,
    module = "marketcow_sdk._native"
)]
pub(crate) struct Terms {
    request_id: String,
    source: String,
    source_url: String,
    observed_at_ns: Option<i64>,
    instrument: Option<Instrument>,
    fees: Option<Fees>,
    order_execution: Option<OrderTerms>,
    explicit_policies: Vec<Policy>,
    unavailable_capabilities: Vec<String>,
}
impl From<facts::ExecutionFactsResponse> for Terms {
    fn from(v: facts::ExecutionFactsResponse) -> Self {
        Self {
            request_id: v.request_id,
            source: v.source,
            source_url: v.source_url,
            observed_at_ns: v.meta.map(|m| m.observed_at_ns),
            instrument: v.instrument.map(|i| Instrument {
                price_increment: i.price_increment,
                minimum_order_size: i.minimum_order_size,
                size_increment: i.size_increment,
                price_unit: i.price_unit,
                size_unit: i.size_unit,
                missing_fields: i.missing_fields,
            }),
            fees: v.fees.map(|f| Fees {
                model: f.model,
                currency: f.currency,
                maker_rate: f.maker_rate,
                taker_rate: f.taker_rate,
                exponent: f.exponent,
                taker_only: f.taker_only,
                quantum: f.quantum,
                rounding_mode: f.rounding_mode,
                effective_at_ns: f.effective_at_ns,
                maker_base_fee_bps: f.maker_base_fee_bps,
                taker_base_fee_bps: f.taker_base_fee_bps,
                missing_fields: f.missing_fields,
                applicability: f.applicability,
            }),
            order_execution: v.order_execution.map(|o| OrderTerms {
                taker_order_delay_enabled: o.taker_order_delay_enabled,
                marketable_hold_ms: o.marketable_hold_ms,
                missing_fields: o.missing_fields,
                documentation_evidence_ids: o
                    .documentation
                    .into_iter()
                    .map(|e| e.evidence_id)
                    .collect(),
            }),
            explicit_policies: v
                .explicit_policies
                .into_iter()
                .map(|p| Policy {
                    policy_id: p.identity.as_ref().map(|i| i.id.clone()),
                    revision: p.identity.map(|i| i.revision),
                    origin: p.origin,
                    purpose: p.purpose,
                    fee_currency: p.fee_currency,
                    fee_quantum: p.fee_quantum,
                    rounding_mode: p.rounding_mode,
                    size_increment: p.size_increment,
                    fee_upper_bound_method: p.fee_upper_bound_method,
                })
                .collect(),
            unavailable_capabilities: v.unavailable_capabilities,
        }
    }
}
#[pyclass(
    name = "UpstreamExchange",
    frozen,
    get_all,
    module = "marketcow_sdk._native"
)]
#[derive(Clone)]
struct Exchange {
    url: String,
    status: Option<u32>,
    content_type: Option<String>,
    retry_after: Option<String>,
    started_at_ns: Option<i64>,
    received_at_ns: Option<i64>,
    duration_us: Option<u64>,
    body_complete: bool,
}
impl From<facts::UpstreamExchange> for Exchange {
    fn from(v: facts::UpstreamExchange) -> Self {
        Self {
            url: v.url,
            status: v.status,
            content_type: v.content_type,
            retry_after: v.retry_after,
            started_at_ns: v.started_at_ns,
            received_at_ns: v.received_at_ns,
            duration_us: v.duration_us,
            body_complete: v.body_complete,
        }
    }
}
#[pyclass(name = "OfficialBtcTrade", frozen, module = "marketcow_sdk._native")]
pub(crate) struct BtcRest(pub facts::BtcRestResponse);
#[pymethods]
impl BtcRest {
    #[getter]
    fn request_id(&self) -> &str {
        &self.0.request_id
    }
    #[getter]
    fn source(&self) -> i32 {
        self.0.source
    }
    #[getter]
    fn instrument_id(&self) -> &str {
        &self.0.instrument_id
    }
    #[getter]
    fn acquisition(&self) -> &str {
        &self.0.acquisition
    }
    #[getter]
    fn cache_used(&self) -> bool {
        self.0.cache_used
    }
    #[getter]
    fn ws_dependency(&self) -> bool {
        self.0.ws_dependency
    }
    #[getter]
    fn latest_trade(&self) -> Option<Trade> {
        self.0.latest_trade.clone().map(Trade)
    }
    #[getter]
    fn signed_age_ms_at_receive(&self) -> Option<i64> {
        self.0.latest_source_age_ms_at_receive
    }
    #[getter]
    fn upstream(&self) -> Option<Exchange> {
        self.0.upstream.clone().map(Exchange::from)
    }
    #[getter]
    fn evidence_id(&self) -> Option<&str> {
        self.0.evidence.as_ref().map(|e| e.evidence_id.as_str())
    }
}
#[pyclass(name = "OfficialBook", frozen, module = "marketcow_sdk._native")]
pub(crate) struct Book(pub facts::OfficialBookResponse);
impl Book {
    fn book(&self) -> Option<&facts::OfficialBook> {
        match &self.0.body {
            Some(facts::official_book_response::Body::Book(b)) => Some(b),
            _ => None,
        }
    }
}
#[pymethods]
impl Book {
    #[getter]
    fn request_id(&self) -> &str {
        &self.0.request_id
    }
    #[getter]
    fn condition_id(&self) -> Option<&str> {
        self.book().map(|b| b.market.as_str())
    }
    #[getter]
    fn token_id(&self) -> Option<&str> {
        self.book().map(|b| b.asset_id.as_str())
    }
    #[getter]
    fn supplier_timestamp_ms(&self) -> Option<i64> {
        self.book().and_then(|b| b.supplier_timestamp_ms)
    }
    #[getter]
    fn bids(&self) -> Vec<(String, String)> {
        self.book()
            .map(|b| {
                b.bids
                    .iter()
                    .map(|p| (p.price.clone(), p.size.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }
    #[getter]
    fn asks(&self) -> Vec<(String, String)> {
        self.book()
            .map(|b| {
                b.asks
                    .iter()
                    .map(|p| (p.price.clone(), p.size.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }
    #[getter]
    fn tick_size(&self) -> Option<&str> {
        self.book().and_then(|b| b.tick_size.as_deref())
    }
    #[getter]
    fn min_order_size(&self) -> Option<&str> {
        self.book().and_then(|b| b.min_order_size.as_deref())
    }
    #[getter]
    fn neg_risk(&self) -> Option<bool> {
        self.book().and_then(|b| b.neg_risk)
    }
    #[getter]
    fn last_trade_price(&self) -> Option<&str> {
        self.book().and_then(|b| b.last_trade_price.as_deref())
    }
    #[getter]
    fn acquisition(&self) -> &str {
        &self.0.acquisition
    }
    #[getter]
    fn cache_used(&self) -> bool {
        self.0.cache_used
    }
    #[getter]
    fn ws_dependency(&self) -> bool {
        self.0.ws_dependency
    }
    #[getter]
    fn upstream(&self) -> Option<Exchange> {
        self.0.upstream.clone().map(Exchange::from)
    }
    #[getter]
    fn evidence_id(&self) -> Option<&str> {
        self.0.evidence.as_ref().map(|e| e.evidence_id.as_str())
    }
}
// Bind methods directly to the original native client; no parallel transport.
#[pymethods]
impl super::NativeClient {
    fn terms(&self, py: Python<'_>, market: PyRef<'_, Market>) -> PyResult<Terms> {
        let key = market
            .binding
            .key
            .clone()
            .ok_or_else(|| NativeError::new_err("missing market key"))?;
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { client.terms(key).await.map(Terms::from).map_err(error) })
        })
    }
    fn terms_async<'py>(
        &self,
        py: Python<'py>,
        market: PyRef<'_, Market>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let key = market
            .binding
            .key
            .clone()
            .ok_or_else(|| NativeError::new_err("missing market key"))?;
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.terms(key).await.map(Terms::from).map_err(error)
        })
    }
    fn official_book(
        &self,
        py: Python<'_>,
        market: PyRef<'_, Market>,
        token_id: String,
    ) -> PyResult<Book> {
        let key = market
            .binding
            .key
            .clone()
            .ok_or_else(|| NativeError::new_err("missing market key"))?;
        let stream = market.binding.stream_id.clone();
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                client
                    .official_book(key, stream, token_id)
                    .await
                    .map(Book)
                    .map_err(error)
            })
        })
    }
    fn official_book_async<'py>(
        &self,
        py: Python<'py>,
        market: PyRef<'_, Market>,
        token_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let key = market
            .binding
            .key
            .clone()
            .ok_or_else(|| NativeError::new_err("missing market key"))?;
        let stream = market.binding.stream_id.clone();
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .official_book(key, stream, token_id)
                .await
                .map(Book)
                .map_err(error)
        })
    }
    fn official_btc_trade(&self, py: Python<'_>, source: i32) -> PyResult<BtcRest> {
        let source =
            common::Source::try_from(source).map_err(|_| NativeError::new_err("unknown source"))?;
        let client = self.0.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                client
                    .official_btc_trade(source)
                    .await
                    .map(BtcRest)
                    .map_err(error)
            })
        })
    }
    fn official_btc_trade_async<'py>(
        &self,
        py: Python<'py>,
        source: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let source =
            common::Source::try_from(source).map_err(|_| NativeError::new_err("unknown source"))?;
        let client = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .official_btc_trade(source)
                .await
                .map(BtcRest)
                .map_err(error)
        })
    }
}
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Instrument>()?;
    module.add_class::<Fees>()?;
    module.add_class::<OrderTerms>()?;
    module.add_class::<Policy>()?;
    module.add_class::<Terms>()?;
    module.add_class::<Exchange>()?;
    module.add_class::<BtcRest>()?;
    module.add_class::<Book>()?;
    Ok(())
}
