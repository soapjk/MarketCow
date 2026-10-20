use super::error;
use marketcow_sdk::{
    Client, CurrentMarket, MarketDelivery, SharedMarket, facts, market, wire_history,
};
use pyo3::{prelude::*, types::PyBytes};
use std::sync::{Arc, OnceLock};
use tokio::sync::Semaphore;
use tokio::sync::{Mutex, watch};

// Explicit cold persistence, separate from ordinary field reads. Schema checks
// prevent accidental wrong-type restore; business verification remains caller-owned.
macro_rules! cold_facade {
    ($name:ident,$wire:ty,$valid:expr) => {
        #[pymethods]
        impl $name {
            fn proto_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
                super::cold::export(py, Arc::new(self.0.clone()))
            }
            #[staticmethod]
            fn from_proto_bytes(py: Python<'_>, data: &Bound<'_, PyBytes>) -> PyResult<Self> {
                let value = super::cold::decode::<$wire>(py, data)?;
                if !($valid)(&value) {
                    return Err(super::NativeError::new_err(
                        "cold fact schema/identity missing or wrong type",
                    ));
                }
                Ok(Self(value))
            }
        }
    };
}
cold_facade!(
    Reference,
    facts::ReferenceResponse,
    |v: &facts::ReferenceResponse| v.schema_version == "marketcow.facts.reference.v1"
        && v.meta.as_ref().and_then(|m| m.key.as_ref()).is_some()
);
cold_facade!(
    Finality,
    facts::FinalityResponse,
    |v: &facts::FinalityResponse| v.schema_version == "marketcow.facts.finality.v1"
        && v.meta.as_ref().and_then(|m| m.key.as_ref()).is_some()
);
cold_facade!(Rules, market::RulesRecord, |v: &market::RulesRecord| v
    .identity
    .as_ref()
    .is_some_and(|i| !i.id.is_empty() && i.revision > 0));

#[pyclass(name = "Market", frozen, module = "marketcow_sdk._native")]
#[derive(Clone)]
pub(crate) struct Market {
    pub binding: market::MarketBinding,
    pub budget: market::DeliveryBudget,
    read_only: bool,
}
#[pymethods]
impl Market {
    /// Archived identity only. Does not discover markets or open a subscription.
    #[staticmethod]
    #[pyo3(signature=(*, market_id, condition_id, window_id, start_at, end_at, up_token_id, down_token_id, rules_id, rules_revision))]
    fn from_key(
        market_id: String,
        condition_id: String,
        window_id: String,
        start_at: String,
        end_at: String,
        up_token_id: String,
        down_token_id: String,
        rules_id: String,
        rules_revision: u64,
    ) -> PyResult<Self> {
        Self::data_identity(market::MarketBinding {
            key: Some(market::MarketKey {
                market_id,
                condition_id,
                window_id,
                start_at,
                end_at,
                up_token_id,
                down_token_id,
                rules: Some(marketcow_sdk::common::RecordIdentity {
                    id: rules_id,
                    revision: rules_revision,
                }),
            }),
            role: "archived".into(),
            stream_id: String::new(),
        })
    }
    #[staticmethod]
    fn from_binding(
        binding: PyRef<'_, super::fact_views::FactMarketMarketBinding>,
    ) -> PyResult<Self> {
        Self::data_identity((*binding.0).clone())
    }
    #[staticmethod]
    fn from_proto_bytes(py: Python<'_>, data: &Bound<'_, PyBytes>) -> PyResult<Self> {
        Self::data_identity(super::cold::decode::<market::MarketBinding>(py, data)?)
    }
    fn proto_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        super::cold::export(py, Arc::new(self.binding.clone()))
    }
    #[getter]
    fn read_only(&self) -> bool {
        self.read_only
    }
    #[getter]
    fn binding(&self) -> super::fact_views::FactMarketMarketBinding {
        super::fact_views::FactMarketMarketBinding(Arc::new(self.binding.clone()))
    }
    #[getter]
    fn market_id(&self) -> Option<&str> {
        self.binding.key.as_ref().map(|k| k.market_id.as_str())
    }
    #[getter]
    fn condition_id(&self) -> Option<&str> {
        self.binding.key.as_ref().map(|k| k.condition_id.as_str())
    }
    #[getter]
    fn window_id(&self) -> Option<&str> {
        self.binding.key.as_ref().map(|k| k.window_id.as_str())
    }
    #[getter]
    fn start_at(&self) -> Option<&str> {
        self.binding.key.as_ref().map(|k| k.start_at.as_str())
    }
    #[getter]
    fn end_at(&self) -> Option<&str> {
        self.binding.key.as_ref().map(|k| k.end_at.as_str())
    }
    #[getter]
    fn up_token_id(&self) -> Option<&str> {
        self.binding.key.as_ref().map(|k| k.up_token_id.as_str())
    }
    #[getter]
    fn down_token_id(&self) -> Option<&str> {
        self.binding.key.as_ref().map(|k| k.down_token_id.as_str())
    }
    #[getter]
    fn rules_id(&self) -> Option<&str> {
        self.binding
            .key
            .as_ref()?
            .rules
            .as_ref()
            .map(|r| r.id.as_str())
    }
    #[getter]
    fn rules_revision(&self) -> Option<u64> {
        self.binding
            .key
            .as_ref()?
            .rules
            .as_ref()
            .map(|r| r.revision)
    }
    #[getter]
    fn role(&self) -> &str {
        &self.binding.role
    }
    #[getter]
    fn stream_id(&self) -> &str {
        &self.binding.stream_id
    }
}

impl Market {
    fn data_identity(binding: market::MarketBinding) -> PyResult<Self> {
        let k = binding
            .key
            .as_ref()
            .ok_or_else(|| super::NativeError::new_err("complete archived market key required"))?;
        let rules = k
            .rules
            .as_ref()
            .ok_or_else(|| super::NativeError::new_err("persisted rules identity required"))?;
        let values = [
            &k.market_id,
            &k.condition_id,
            &k.window_id,
            &k.start_at,
            &k.end_at,
            &k.up_token_id,
            &k.down_token_id,
            &rules.id,
        ];
        if values.iter().any(|v| v.is_empty() || v.len() > 4096)
            || rules.revision == 0
            || k.up_token_id == k.down_token_id
        {
            return Err(super::NativeError::new_err(
                "incomplete archived market/rules identity",
            ));
        }
        Ok(Self {
            binding,
            budget: market::DeliveryBudget::default(),
            read_only: true,
        })
    }
}

#[pyclass(name = "MarketDirectory", frozen, module = "marketcow_sdk._native")]
pub(crate) struct MarketDirectory(Arc<market::MarketStreams>);
#[pymethods]
impl MarketDirectory {
    #[getter]
    fn fact(&self) -> super::fact_views::FactMarketMarketStreams {
        super::fact_views::FactMarketMarketStreams(self.0.clone())
    }
    #[getter]
    fn delivery_budget(&self) -> Option<super::fact_views::FactMarketDeliveryBudget> {
        self.0
            .delivery_budget
            .clone()
            .map(|v| super::fact_views::FactMarketDeliveryBudget(Arc::new(v)))
    }
    #[getter]
    fn schema_version(&self) -> &str {
        &self.0.schema_version
    }
}
pub(crate) async fn directory(client: Client) -> PyResult<MarketDirectory> {
    client
        .markets()
        .await
        .map(|v| MarketDirectory(Arc::new(v)))
        .map_err(error)
}

pub(crate) async fn list(client: Client) -> PyResult<Vec<Market>> {
    let value = client.markets().await.map_err(error)?;
    let budget = value
        .delivery_budget
        .ok_or_else(|| super::NativeError::new_err("delivery budget missing"))?;
    Ok(value
        .markets
        .into_iter()
        .map(|binding| Market {
            binding,
            budget: budget.clone(),
            read_only: false,
        })
        .collect())
}

#[pyclass(name = "Rules", frozen, module = "marketcow_sdk._native")]
pub(crate) struct Rules(pub market::RulesRecord);
#[pymethods]
impl Rules {
    #[getter]
    fn fact(&self) -> super::fact_views::FactMarketRulesRecord {
        super::fact_views::FactMarketRulesRecord(Arc::new(self.0.clone()))
    }
    #[getter]
    fn rules_id(&self) -> Option<&str> {
        self.0.identity.as_ref().map(|i| i.id.as_str())
    }
    #[getter]
    fn revision(&self) -> Option<u64> {
        self.0.identity.as_ref().map(|i| i.revision)
    }
    #[getter]
    fn market_id(&self) -> &str {
        &self.0.market_id
    }
    #[getter]
    fn description(&self) -> &str {
        &self.0.description
    }
    #[getter]
    fn resolution_source(&self) -> &str {
        &self.0.resolution_source
    }
    #[getter]
    fn verification(&self) -> Option<&str> {
        self.0.resolution.as_ref().map(|r| r.verification.as_str())
    }
    #[getter]
    fn quote_currency(&self) -> Option<&str> {
        self.0.resolution.as_ref()?.quote_currency.as_deref()
    }
    #[getter]
    fn price_method(&self) -> Option<&str> {
        self.0.resolution.as_ref()?.price_method.as_deref()
    }
    #[getter]
    fn averaging_seconds(&self) -> Option<u32> {
        self.0.resolution.as_ref()?.averaging_seconds
    }
    #[getter]
    fn opening_selection(&self) -> Option<&str> {
        self.0.resolution.as_ref()?.opening_selection.as_deref()
    }
    #[getter]
    fn closing_selection(&self) -> Option<&str> {
        self.0.resolution.as_ref()?.closing_selection.as_deref()
    }
    #[getter]
    fn comparison(&self) -> Option<&str> {
        self.0.resolution.as_ref()?.comparison.as_deref()
    }
    #[getter]
    fn equality_outcome(&self) -> Option<&str> {
        self.0.resolution.as_ref()?.equality_outcome.as_deref()
    }
}

#[pyclass(name = "ReferenceReading", frozen, module = "marketcow_sdk._native")]
pub(crate) struct Reading(facts::ReadingState);
#[pymethods]
impl Reading {
    #[getter]
    fn fact(&self) -> super::fact_views::FactFactsReadingState {
        super::fact_views::FactFactsReadingState(Arc::new(self.0.clone()))
    }
    #[getter]
    fn available(&self) -> bool {
        self.0.available
    }
    #[getter]
    fn reason(&self) -> Option<&str> {
        self.0.reason.as_deref()
    }
    #[getter]
    fn price(&self) -> Option<&str> {
        self.0.reading.as_ref().map(|r| r.price.as_str())
    }
    #[getter]
    fn purpose(&self) -> Option<&str> {
        self.0.reading.as_ref().map(|r| r.purpose.as_str())
    }
    #[getter]
    fn quote_currency(&self) -> Option<&str> {
        self.0.reading.as_ref().map(|r| r.quote_currency.as_str())
    }
    #[getter]
    fn price_method(&self) -> Option<&str> {
        self.0.reading.as_ref().map(|r| r.price_method.as_str())
    }
    #[getter]
    fn boundary_time_ms(&self) -> Option<i64> {
        self.0.reading.as_ref().map(|r| r.boundary_time_ms)
    }
    #[getter]
    fn supplier_report_time_ms(&self) -> Option<i64> {
        self.0.reading.as_ref()?.supplier_report_time_ms
    }
    #[getter]
    fn covered_ms(&self) -> Option<u64> {
        self.0.reading.as_ref()?.covered_ms
    }
    #[getter]
    fn baseline_id(&self) -> Option<&str> {
        self.0.reading.as_ref()?.baseline_id.as_deref()
    }
}

#[pyclass(name = "Reference", frozen, module = "marketcow_sdk._native")]
pub(crate) struct Reference(pub facts::ReferenceResponse);
#[pymethods]
impl Reference {
    #[getter]
    fn fact(&self) -> super::fact_views::FactFactsReferenceResponse {
        super::fact_views::FactFactsReferenceResponse(Arc::new(self.0.clone()))
    }
    #[getter]
    fn official_opening(&self) -> Option<Reading> {
        self.0.official_opening.clone().map(Reading)
    }
    #[getter]
    fn binance_opening(&self) -> Option<Reading> {
        self.0.binance_opening.clone().map(Reading)
    }
    #[getter]
    fn binance_current(&self) -> Option<Reading> {
        self.0.binance_current.clone().map(Reading)
    }
    #[getter]
    fn binance_is_settlement_source(&self) -> bool {
        self.0.binance_is_settlement_source
    }
    #[getter]
    fn mapped_official_scale_price(&self) -> Option<&str> {
        self.0.mapped_official_scale_price.as_deref()
    }
}

#[pyclass(name = "Finality", frozen, module = "marketcow_sdk._native")]
pub(crate) struct Finality(pub facts::FinalityResponse);
#[pymethods]
impl Finality {
    #[getter]
    fn fact(&self) -> super::fact_views::FactFactsFinalityResponse {
        super::fact_views::FactFactsFinalityResponse(Arc::new(self.0.clone()))
    }
    #[getter]
    fn status(&self) -> &str {
        &self.0.status
    }
    #[getter]
    fn reason(&self) -> Option<&str> {
        self.0.reason.as_deref()
    }
    #[getter]
    fn policy(&self) -> &str {
        &self.0.policy
    }
    #[getter]
    fn durable(&self) -> bool {
        self.0.durable
    }
    #[getter]
    fn settlement_import_allowed(&self) -> bool {
        self.0.settlement_import_allowed
    }
    #[getter]
    fn valid_provider_count(&self) -> u32 {
        self.0.valid_provider_count
    }
    #[getter]
    fn denominator(&self) -> Option<&str> {
        self.0.denominator.as_deref()
    }
    #[getter]
    fn payouts(&self) -> Vec<(String, String)> {
        self.0
            .payouts
            .iter()
            .map(|p| (p.token_id.clone(), p.numerator.clone()))
            .collect()
    }
}

#[pyclass(name = "Evidence", frozen, module = "marketcow_sdk._native")]
pub(crate) struct Evidence(pub wire_history::Evidence);
#[pymethods]
impl Evidence {
    #[getter]
    fn schema_version(&self) -> &str {
        &self.0.schema_version
    }
    #[getter]
    fn received_at_ns(&self) -> Option<i64> {
        self.0.received_at_ns
    }
    #[getter]
    fn source_generation(&self) -> Option<&str> {
        self.0.source_generation.as_deref()
    }
    #[getter]
    fn evidence_id(&self) -> &str {
        &self.0.evidence_id
    }
    #[getter]
    fn acquisition(&self) -> &str {
        &self.0.acquisition
    }
    #[getter]
    fn upstream_url(&self) -> &str {
        &self.0.upstream_url
    }
    #[getter]
    fn content_type(&self) -> &str {
        &self.0.content_type
    }
    #[getter]
    fn raw_body<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.0.raw_body)
    }
}

enum Owner {
    Fixed(SharedMarket),
    Current(CurrentMarket),
}
#[pyclass(name = "NativeMarketStream", module = "marketcow_sdk._native")]
pub(crate) struct MarketStream {
    inner: Arc<Mutex<Owner>>,
    cancel: watch::Sender<bool>,
    state: watch::Sender<Option<marketcow_sdk::market_stream::StateHandle>>,
}

pub(crate) async fn subscribe(client: Client, market: Market) -> PyResult<MarketStream> {
    if market.read_only {
        return Err(super::NativeError::new_err(
            "data-only archived identity cannot open a subscription; use discovery",
        ));
    }
    let owner = client
        .shared_market(market.binding, market.budget)
        .map_err(error)?;
    let state = watch::channel(Some(owner.state_handle())).0;
    Ok(MarketStream {
        inner: Arc::new(Mutex::new(Owner::Fixed(owner))),
        cancel: watch::channel(false).0,
        state,
    })
}
pub(crate) fn follow(client: Client) -> MarketStream {
    MarketStream {
        inner: Arc::new(Mutex::new(Owner::Current(client.current_market()))),
        cancel: watch::channel(false).0,
        state: watch::channel(None).0,
    }
}

#[pyclass(name = "MarketEvent", frozen, module = "marketcow_sdk._native")]
pub(crate) struct MarketEvent {
    event: MarketDelivery,
    #[pyo3(get)]
    available: bool,
    #[pyo3(get)]
    up_bid: Option<(String, String)>,
    #[pyo3(get)]
    up_ask: Option<(String, String)>,
    #[pyo3(get)]
    down_bid: Option<(String, String)>,
    #[pyo3(get)]
    down_ask: Option<(String, String)>,
}

#[pyclass(name = "BookSnapshot", frozen, module = "marketcow_sdk._native")]
pub(crate) struct BookSnapshot {
    view: Arc<marketcow_sdk::book::DepthSnapshot>,
    index: usize,
}
#[pymethods]
impl BookSnapshot {
    #[getter]
    fn metadata(&self) -> super::fact_views::FactMarketTokenMetadata {
        super::fact_views::FactMarketTokenMetadata(Arc::new(
            self.view.books[self.index].metadata.clone(),
        ))
    }
    #[getter]
    fn token_id(&self) -> &str {
        &self.view.books[self.index].metadata.token_id
    }
    #[getter]
    fn outcome(&self) -> &str {
        &self.view.books[self.index].metadata.outcome
    }
    #[getter]
    fn book_revision(&self) -> u64 {
        self.view.books[self.index].metadata.book_revision
    }
    #[getter]
    fn data_valid(&self) -> bool {
        self.view.books[self.index].metadata.data_valid
    }
    #[getter]
    fn tick_size(&self) -> Option<&str> {
        self.view.books[self.index].metadata.tick_size.as_deref()
    }
    #[getter]
    fn quote_source_at(&self) -> Option<&str> {
        self.view.books[self.index]
            .metadata
            .quote_source_at
            .as_deref()
    }
    #[getter]
    fn received_at(&self) -> Option<&str> {
        self.view.books[self.index].metadata.received_at.as_deref()
    }
    #[getter]
    fn invalid_reason(&self) -> Option<&str> {
        self.view.books[self.index]
            .metadata
            .invalid_reason
            .as_deref()
    }
    #[getter]
    fn confirmation_source_at(&self) -> Option<&str> {
        self.view.books[self.index]
            .metadata
            .confirmation
            .as_ref()
            .map(|c| c.source_at.as_str())
    }
    #[getter]
    fn truncated(&self) -> bool {
        self.view.books[self.index].truncated
    }
    #[getter]
    fn bids(&self) -> Vec<(String, String)> {
        self.view.books[self.index]
            .bids
            .iter()
            .map(|l| (l.price.clone(), l.size.clone()))
            .collect()
    }
    #[getter]
    fn asks(&self) -> Vec<(String, String)> {
        self.view.books[self.index]
            .asks
            .iter()
            .map(|l| (l.price.clone(), l.size.clone()))
            .collect()
    }
}
#[pyclass(name = "MarketSnapshot", frozen, module = "marketcow_sdk._native")]
pub(crate) struct MarketSnapshot(Arc<marketcow_sdk::book::DepthSnapshot>);
#[pymethods]
impl MarketSnapshot {
    #[getter]
    fn key(&self) -> super::fact_views::FactMarketMarketKey {
        super::fact_views::FactMarketMarketKey(Arc::new(self.0.key.clone()))
    }
    #[getter]
    fn market_id(&self) -> &str {
        &self.0.key.market_id
    }
    #[getter]
    fn stream_id(&self) -> &str {
        &self.0.stream_id
    }
    #[getter]
    fn state_sequence(&self) -> Option<u64> {
        self.0.sequence
    }
    #[getter]
    fn source_status(&self) -> &str {
        &self.0.source_status
    }
    #[getter]
    fn reason(&self) -> Option<&str> {
        self.0.reason.as_deref()
    }
    #[getter]
    fn published_at(&self) -> &str {
        &self.0.published_at
    }
    #[getter]
    fn books(&self) -> Vec<BookSnapshot> {
        (0..self.0.books.len())
            .map(|index| BookSnapshot {
                view: self.0.clone(),
                index,
            })
            .collect()
    }
}
fn depth_view(
    state: Option<marketcow_sdk::market_stream::StateHandle>,
    depth: Option<usize>,
) -> PyResult<Option<MarketSnapshot>> {
    state
        .map(|state| {
            state
                .read()
                .map_err(|_| super::NativeError::new_err("market state poisoned"))?
                .depth_view(depth)
                .map(|v| MarketSnapshot(Arc::new(v)))
                .map_err(error)
        })
        .transpose()
}
#[pymethods]
impl MarketEvent {
    #[getter]
    fn binding(&self) -> Option<super::fact_views::FactMarketMarketBinding> {
        match &self.event {
            MarketDelivery::MarketChanged(binding) => Some(
                super::fact_views::FactMarketMarketBinding(Arc::new(binding.clone())),
            ),
            _ => None,
        }
    }
    #[getter]
    fn key(&self) -> Option<super::fact_views::FactMarketMarketKey> {
        let key = match &self.event {
            MarketDelivery::MarketChanged(binding) => binding.key.as_ref(),
            MarketDelivery::Frame(frame, _) => frame.key.as_ref(),
            _ => None,
        };
        key.cloned()
            .map(|k| super::fact_views::FactMarketMarketKey(Arc::new(k)))
    }
    #[getter]
    fn frame(&self) -> Option<super::fact_views::FactMarketMarketFrame> {
        match &self.event {
            MarketDelivery::Frame(frame, _) => {
                Some(super::fact_views::FactMarketMarketFrame(frame.clone()))
            }
            _ => None,
        }
    }
    #[getter]
    fn kind(&self) -> &'static str {
        match &self.event {
            MarketDelivery::MarketChanged(_) => "market_changed",
            MarketDelivery::Recovering { .. } => "recovering",
            MarketDelivery::Frame(frame, _) => match &frame.body {
                Some(market::market_frame::Body::Snapshot(_)) => "snapshot",
                _ => "price_change",
            },
        }
    }
    #[getter]
    fn market_id(&self) -> Option<&str> {
        match &self.event {
            MarketDelivery::MarketChanged(binding) => {
                binding.key.as_ref().map(|k| k.market_id.as_str())
            }
            MarketDelivery::Frame(frame, _) => frame.key.as_ref().map(|k| k.market_id.as_str()),
            _ => None,
        }
    }
    #[getter]
    fn state_sequence(&self) -> Option<u64> {
        match &self.event {
            MarketDelivery::Frame(f, _) => Some(f.state_sequence),
            _ => None,
        }
    }
    #[getter]
    fn stream_id(&self) -> Option<&str> {
        match &self.event {
            MarketDelivery::Frame(f, _) => Some(&f.stream_id),
            MarketDelivery::MarketChanged(b) => Some(&b.stream_id),
            _ => None,
        }
    }
    #[getter]
    fn reason(&self) -> Option<&str> {
        match &self.event {
            MarketDelivery::Recovering { reason, .. } => Some(reason),
            _ => None,
        }
    }
    #[getter]
    fn changes(&self) -> Vec<(String, String, String, String)> {
        match &self.event {
            MarketDelivery::Frame(frame, _) => match &frame.body {
                Some(market::market_frame::Body::PriceChange(d)) => d
                    .changes
                    .iter()
                    .map(|c| {
                        (
                            c.token_id.clone(),
                            c.side.clone(),
                            c.price.clone(),
                            c.size.clone(),
                        )
                    })
                    .collect(),
                _ => Vec::new(),
            },
            _ => Vec::new(),
        }
    }
}

async fn next(
    inner: Arc<Mutex<Owner>>,
    mut cancel: watch::Receiver<bool>,
    target: watch::Sender<Option<marketcow_sdk::market_stream::StateHandle>>,
) -> PyResult<Option<MarketEvent>> {
    let mut owner = inner.lock().await;
    let operation = async {
        match &mut *owner {
            Owner::Fixed(stream) => stream.next().await,
            Owner::Current(stream) => stream.next().await,
        }
    };
    let event = tokio::select! { biased;
        _ = cancel.wait_for(|closed| *closed) => return Ok(None),
        event = operation => event.map_err(error)?,
    };
    let Some(event) = event else {
        return Ok(None);
    };
    let handle = match &*owner {
        Owner::Fixed(s) => Some(s.state_handle()),
        Owner::Current(s) => s.state_handle(),
    };
    target.send_replace(handle.clone());
    // Quotes are captured by the shared owner at this exact frame, not read
    // from a later state after a slow consumer dequeues an older event.
    let top = match &event {
        MarketDelivery::Frame(_, top) => Some(top),
        _ => None,
    };
    Ok(Some(MarketEvent {
        available: top.is_some_and(|t| t.available),
        up_bid: top.and_then(|t| t.levels[0].clone()),
        up_ask: top.and_then(|t| t.levels[1].clone()),
        down_bid: top.and_then(|t| t.levels[2].clone()),
        down_ask: top.and_then(|t| t.levels[3].clone()),
        event,
    }))
}

async fn close(inner: Arc<Mutex<Owner>>) {
    match &mut *inner.lock().await {
        Owner::Fixed(s) => s.close(),
        Owner::Current(s) => s.close().await,
    }
}
#[pymethods]
impl MarketStream {
    #[pyo3(signature=(depth=None))]
    fn snapshot(&self, py: Python<'_>, depth: Option<usize>) -> PyResult<Option<MarketSnapshot>> {
        let state = self.state.borrow().clone();
        py.detach(move || depth_view(state, depth))
    }
    #[pyo3(signature=(depth=None))]
    fn snapshot_async<'py>(
        &self,
        py: Python<'py>,
        depth: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
        let permit = SLOTS
            .get_or_init(|| Arc::new(Semaphore::new(2)))
            .clone()
            .try_acquire_owned()
            .map_err(|_| super::NativeError::new_err("snapshot_copy_capacity"))?;
        let state = self.state.borrow().clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                depth_view(state, depth)
            })
            .await
            .map_err(|_| super::NativeError::new_err("snapshot_worker_failed"))?
        })
    }
    fn next(&self, py: Python<'_>) -> PyResult<Option<MarketEvent>> {
        let inner = self.inner.clone();
        let cancel = self.cancel.subscribe();
        let target = self.state.clone();
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(next(inner, cancel, target))
        })
    }
    fn next_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            next(
                self.inner.clone(),
                self.cancel.subscribe(),
                self.state.clone(),
            ),
        )
    }
    fn close(&self, py: Python<'_>) {
        self.cancel.send_replace(true);
        self.state.send_replace(None);
        let inner = self.inner.clone();
        py.detach(move || pyo3_async_runtimes::tokio::get_runtime().block_on(close(inner)))
    }
    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.cancel.send_replace(true);
        self.state.send_replace(None);
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            close(inner).await;
            Ok(())
        })
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<MarketDirectory>()?;
    module.add_class::<Market>()?;
    module.add_class::<Rules>()?;
    module.add_class::<Reading>()?;
    module.add_class::<Reference>()?;
    module.add_class::<Finality>()?;
    module.add_class::<Evidence>()?;
    module.add_class::<MarketStream>()?;
    module.add_class::<MarketEvent>()?;
    module.add_class::<MarketSnapshot>()?;
    module.add_class::<BookSnapshot>()?;
    Ok(())
}
