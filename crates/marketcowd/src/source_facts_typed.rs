//! Native fact projections and decisions. Only supplier ingress parses JSON;
//! no JSON generation, local digest, opaque legacy-hash validation or WS cache.
use anyhow::{Context, Result, ensure};
use marketcow_contracts::typed_v3::{self as contract, common, facts, market};
use rust_decimal::Decimal;
use serde::Deserialize;

pub fn validate_key(key: &market::MarketKey) -> Result<()> {
    ensure!(
        !key.market_id.is_empty()
            && key.market_id.len() <= 20
            && key.market_id.bytes().all(|b| b.is_ascii_digit()),
        "invalid_market_id"
    );
    ensure!(hex_identity(&key.condition_id, 32), "invalid_condition_id");
    ensure!(
        !key.window_id.is_empty() && key.window_id.len() <= 160,
        "invalid_window_id"
    );
    let start = chrono::DateTime::parse_from_rfc3339(&key.start_at)?;
    let end = chrono::DateTime::parse_from_rfc3339(&key.end_at)?;
    ensure!(start < end, "invalid_window");
    ensure!(key.up_token_id != key.down_token_id, "invalid_token_pair");
    ensure!(
        key.up_token_id != "0" && key.down_token_id != "0",
        "invalid_token_id"
    );
    uint256(&key.up_token_id)?;
    uint256(&key.down_token_id)?;
    let rules = key.rules.as_ref().context("missing_rules_identity")?;
    validate_identity(rules)?;
    Ok(())
}
pub fn validate_identity(id: &common::RecordIdentity) -> Result<()> {
    ensure!(
        !id.id.is_empty() && id.id.len() <= 128 && id.revision > 0,
        "invalid_record_identity"
    );
    Ok(())
}
pub fn validate_request(request: &facts::MarketRequest) -> Result<&market::MarketKey> {
    ensure!(
        !request.request_id.is_empty() && request.request_id.len() <= 128,
        "invalid_request_id"
    );
    let key = request.key.as_ref().context("missing_market_key")?;
    validate_key(key)?;
    Ok(key)
}
pub fn validate_meta(meta: &facts::FactMeta, key: &market::MarketKey) -> Result<()> {
    validate_identity(meta.identity.as_ref().context("missing_record_identity")?)?;
    ensure!(
        meta.key.as_ref() == Some(key) && meta.observed_at_ns > 0,
        "fact_identity_mismatch"
    );
    Ok(())
}
fn hex_identity(value: &str, bytes: usize) -> bool {
    value.len() == 2 + bytes * 2
        && value.starts_with("0x")
        && value[2..].bytes().all(|b| b.is_ascii_hexdigit())
}
/// External uint256 decimal identities, not f64 or a local content digest.
pub fn uint256(value: &str) -> Result<()> {
    const MAX: &str =
        "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    ensure!(
        !value.is_empty()
            && value.bytes().all(|c| c.is_ascii_digit())
            && (value == "0" || !value.starts_with('0'))
            && (value.len() < MAX.len() || (value.len() == MAX.len() && value <= MAX)),
        "invalid_uint256"
    );
    Ok(())
}
fn decimal(value: &str, zero: bool) -> Result<Decimal> {
    ensure!(
        !value.is_empty() && value.len() <= 96 && !value.contains(['e', 'E', '+']),
        "invalid_decimal"
    );
    let value: Decimal = value.parse()?;
    ensure!(
        if zero {
            value >= Decimal::ZERO
        } else {
            value > Decimal::ZERO
        },
        "invalid_decimal"
    );
    Ok(value)
}
#[derive(Clone)]
struct ExactDecimal(String);
impl<'de> Deserialize<'de> for ExactDecimal {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let raw = Box::<serde_json::value::RawValue>::deserialize(d)?;
        let value = if raw.get().starts_with('"') {
            serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?
        } else {
            raw.get().to_owned()
        };
        Ok(Self(value))
    }
}
#[derive(Deserialize)]
struct CryptoConfig {
    id: String,
    asset: String,
    duration: String,
    #[serde(rename = "twapEnabled")]
    twap_enabled: bool,
    #[serde(rename = "twapLookbackSeconds")]
    twap_lookback_seconds: u32,
}
#[derive(Deserialize)]
struct GammaMarket {
    id: String,
    #[serde(rename = "conditionId")]
    condition: String,
    #[serde(rename = "eventStartTime")]
    start: String,
    #[serde(rename = "endDate")]
    end: String,
    description: String,
    #[serde(rename = "resolutionSource")]
    resolution_source: String,
    #[serde(rename = "cryptoMarketConfig")]
    config: CryptoConfig,
}
#[derive(Deserialize)]
struct EventMetadata {
    #[serde(rename = "priceToBeat")]
    price: Option<ExactDecimal>,
}
#[derive(Deserialize)]
struct GammaEvent {
    slug: String,
    markets: Vec<GammaMarket>,
    #[serde(rename = "eventMetadata")]
    metadata: Option<EventMetadata>,
}
#[derive(Deserialize)]
struct WindowPrice {
    #[serde(rename = "openPrice")]
    price: Option<ExactDecimal>,
    timestamp: Option<Box<serde_json::value::RawValue>>,
}
/// Supplier boundary only. Official report selection remains venue-published;
/// the response timestamp is not silently labeled a report/quote timestamp.
pub fn official_reference(
    request: &facts::MarketRequest,
    meta: facts::FactMeta,
    rules: &market::RulesRecord,
    gamma_raw: &[u8],
    window_raw: &[u8],
) -> Result<facts::ReferenceResponse> {
    const RESOLUTION: &str = "https://data.chain.link/streams/btc-usd-twap-60s-streams";
    let key = validate_request(request)?;
    validate_meta(&meta, key)?;
    ensure!(
        rules.identity == key.rules
            && rules.market_id == key.market_id
            && rules.condition_id == key.condition_id
            && rules.window_id == key.window_id
            && rules.start_at == key.start_at
            && rules.end_at == key.end_at
            && rules.up_token_id == key.up_token_id
            && rules.down_token_id == key.down_token_id,
        "rule_identity_mismatch"
    );
    ensure!(
        gamma_raw.len() <= 512 * 1024 && window_raw.len() <= 16 * 1024,
        "upstream_body_too_large"
    );
    let gamma: GammaEvent = serde_json::from_slice(gamma_raw)?;
    ensure!(gamma.slug == key.window_id, "upstream_identity_mismatch");
    let candidates = gamma
        .markets
        .iter()
        .filter(|m| m.id == key.market_id)
        .collect::<Vec<_>>();
    ensure!(candidates.len() == 1, "upstream_identity_mismatch");
    let m = candidates[0];
    ensure!(
        m.condition == key.condition_id
            && m.start == key.start_at
            && m.end == key.end_at
            && m.description == rules.description
            && m.resolution_source == rules.resolution_source
            && m.resolution_source == RESOLUTION,
        "upstream_rule_mismatch"
    );
    ensure!(
        m.config.id == "btc-5m-twap-60"
            && m.config.asset == "btc"
            && m.config.duration == "5m"
            && m.config.twap_enabled
            && m.config.twap_lookback_seconds == 60,
        "unsupported_reference_rule"
    );
    let window: WindowPrice = serde_json::from_slice(window_raw)?;
    let price = window.price.context("official_not_published")?;
    let parsed = decimal(&price.0, false)?;
    let verification = if let Some(p) = gamma.metadata.and_then(|m| m.price) {
        ensure!(
            decimal(&p.0, false)? == parsed,
            "official_reference_conflict"
        );
        "gamma_matched"
    } else {
        "venue_window_bound"
    };
    let start = chrono::DateTime::parse_from_rfc3339(&key.start_at)?.timestamp_millis();
    let absent = || facts::ReadingState {
        available: false,
        reason: Some("not_requested".into()),
        reading: None,
    };
    let reading = facts::ReferenceReading {
        identity: meta.identity.clone(),
        purpose: "official_price_to_beat".into(),
        instrument_id: "BTCUSD.CHAINLINK.TWAP60".into(),
        quote_currency: "USD".into(),
        price_method: "venue_published_chainlink_twap_60s".into(),
        price: price.0,
        boundary_time_ms: start,
        interval_start_ms: Some(start.checked_sub(60_000).context("reference_time_range")?),
        interval_end_ms: Some(start),
        covered_ms: None,
        selection_rule: "venue_window_openPrice_bound_to_eventStartTime".into(),
        verification: verification.into(),
        supplier_report_time_ms: None,
        supplier_response_timestamp: window.timestamp.map(|v| v.get().to_owned()),
        supplier_response_timestamp_unit: None,
        source_generations: Vec::new(),
        evidence: meta.evidence.clone(),
        baseline_id: None,
    };
    Ok(facts::ReferenceResponse {
        schema_version: contract::REFERENCE_SCHEMA.into(),
        request_id: request.request_id.clone(),
        meta: Some(meta),
        official_opening: Some(facts::ReadingState {
            available: true,
            reason: None,
            reading: Some(reading),
        }),
        binance_opening: Some(absent()),
        binance_current: Some(absent()),
        binance_is_settlement_source: false,
        mapped_official_scale_price: None,
        mapping_reason: Some("not_requested".into()),
    })
}

/// The existing proxy method, unchanged: last trade carried across intervals,
/// complete preceding 60000ms, closed by a genuine trade at/after the endpoint.
/// Call once when building a baseline/current completed window, not per tick.
pub fn binance_proxy(
    identity: common::RecordIdentity,
    rows: &[contract::btc::Trade],
    end_ms: i64,
    purpose: &str,
    source_generations: Vec<String>,
    evidence: Vec<common::EvidenceReference>,
    baseline_id: Option<String>,
) -> Result<facts::ReferenceReading> {
    validate_identity(&identity)?;
    ensure!(
        matches!(purpose, "binance_preopen_proxy" | "current_proxy")
            && end_ms > 60_000
            && end_ms % 1000 == 0
            && rows.len() >= 2
            && rows.len() <= 1_024_000
            && source_generations.len() <= 64,
        "invalid_proxy_range"
    );
    let start = end_ms - 60_000;
    ensure!(
        rows[0].trade_time_ms <= start && rows.last().is_some_and(|r| r.trade_time_ms >= end_ms),
        "complete_60s_coverage_unavailable"
    );
    let mut area = Decimal::ZERO;
    let mut covered = 0i64;
    for (i, row) in rows.iter().enumerate() {
        super::btc_typed::validate_trade(row)?;
        if i == 0 {
            continue;
        }
        let before = &rows[i - 1];
        ensure!(
            before.trade_id.checked_add(1) == Some(row.trade_id)
                && before.trade_time_ms <= row.trade_time_ms,
            "proxy_history_gap"
        );
        let left = before.trade_time_ms.max(start);
        let right = row.trade_time_ms.min(end_ms);
        if right > left {
            let duration = right - left;
            area = area
                .checked_add(
                    decimal(&before.price, false)?
                        .checked_mul(Decimal::from(duration))
                        .context("proxy_area_overflow")?,
                )
                .context("proxy_area_overflow")?;
            covered += duration;
        }
    }
    ensure!(covered == 60_000, "complete_60s_coverage_unavailable");
    let price = area
        .checked_div(Decimal::from(60_000))
        .context("proxy_area_overflow")?;
    Ok(facts::ReferenceReading {
        identity: Some(identity),
        purpose: purpose.into(),
        instrument_id: "BTCUSDT.BINANCE".into(),
        quote_currency: "USDT".into(),
        price_method: "last_trade_price_time_weighted_over_preceding_60000ms".into(),
        price: price.normalize().to_string(),
        boundary_time_ms: end_ms,
        interval_start_ms: Some(start),
        interval_end_ms: Some(end_ms),
        covered_ms: Some(60_000),
        selection_rule: "carry_last_trade_between_contiguous_trade_ids".into(),
        verification: "binance_proxy_not_official".into(),
        supplier_report_time_ms: None,
        supplier_response_timestamp: None,
        supplier_response_timestamp_unit: None,
        source_generations,
        evidence,
        baseline_id,
    })
}
#[derive(Deserialize)]
struct FeeInput {
    r: ExactDecimal,
    e: u32,
    to: bool,
}
#[derive(Deserialize)]
struct OutcomeInput {
    o: String,
    t: String,
}
#[derive(Deserialize)]
struct ExecutionInput {
    c: String,
    t: Vec<OutcomeInput>,
    mos: ExactDecimal,
    mts: ExactDecimal,
    mbf: ExactDecimal,
    tbf: ExactDecimal,
    fd: FeeInput,
    itode: bool,
}
pub fn execution(
    request: &facts::MarketRequest,
    meta: facts::FactMeta,
    raw: &[u8],
) -> Result<facts::ExecutionFactsResponse> {
    let key = validate_request(request)?;
    validate_meta(&meta, key)?;
    ensure!(raw.len() <= 256 * 1024, "upstream_body_too_large");
    let v: ExecutionInput = serde_json::from_slice(raw)?;
    ensure!(
        v.c == key.condition_id
            && v.t.len() == 2
            && v.t[0].o == "Up"
            && v.t[1].o == "Down"
            && v.t[0].t == key.up_token_id
            && v.t[1].t == key.down_token_id,
        "upstream_identity_mismatch"
    );
    decimal(&v.mos.0, false)?;
    decimal(&v.mts.0, false)?;
    decimal(&v.mbf.0, true)?;
    decimal(&v.tbf.0, true)?;
    decimal(&v.fd.r.0, true)?;
    ensure!(v.fd.e > 0 && v.fd.to, "unsupported_fee_model");
    Ok(facts::ExecutionFactsResponse {
        schema_version: contract::EXECUTION_SCHEMA.into(),
        request_id: request.request_id.clone(),
        meta: Some(meta),
        source: "polymarket_clob_v2".into(),
        source_url: format!(
            "https://clob.polymarket.com/clob-markets/{}",
            key.condition_id
        ),
        instrument: Some(facts::InstrumentFacts {
            price_increment: Some(v.mts.0),
            minimum_order_size: Some(v.mos.0),
            size_increment: None,
            price_unit: "probability".into(),
            size_unit: "shares".into(),
            missing_fields: vec!["size_increment".into()],
        }),
        fees: Some(facts::FeeFacts {
            model: "clob_v2_dynamic".into(),
            taker_rate: Some(v.fd.r.0),
            exponent: Some(v.fd.e),
            taker_only: Some(v.fd.to),
            maker_base_fee_bps: Some(v.mbf.0),
            taker_base_fee_bps: Some(v.tbf.0),
            missing_fields: [
                "currency",
                "maker_rate",
                "quantum",
                "rounding_mode",
                "effective_at",
            ]
            .map(str::to_owned)
            .into(),
            applicability: "fd.r/fd.e/fd.to; base-fee fields preserved, not used for dynamic fee"
                .into(),
            ..Default::default()
        }),
        order_execution: Some(facts::OrderExecutionFacts {
            taker_order_delay_enabled: Some(v.itode),
            marketable_hold_ms: None,
            missing_fields: vec!["marketable_hold_ms".into()],
            documentation: Vec::new(),
        }),
        explicit_policies: Vec::new(),
        unavailable_capabilities: Vec::new(),
    })
}
#[derive(Deserialize)]
struct LevelInput {
    price: String,
    size: String,
}
#[derive(Deserialize)]
struct BookInput {
    market: String,
    asset_id: String,
    timestamp: Option<String>,
    hash: Option<String>,
    bids: Vec<LevelInput>,
    asks: Vec<LevelInput>,
    min_order_size: Option<String>,
    tick_size: Option<String>,
    neg_risk: Option<bool>,
    last_trade_price: Option<String>,
}
pub fn official_book(
    raw: &[u8],
    key: &market::MarketKey,
    token: &str,
) -> Result<facts::OfficialBook> {
    validate_key(key)?;
    ensure!(
        token == key.up_token_id || token == key.down_token_id,
        "invalid_token_id"
    );
    ensure!(raw.len() <= 512 * 1024, "upstream_body_too_large");
    let b: BookInput = serde_json::from_slice(raw)?;
    ensure!(
        b.market == key.condition_id && b.asset_id == token,
        "upstream_identity_mismatch"
    );
    let levels = |rows: Vec<LevelInput>| -> Result<Vec<market::PriceLevel>> {
        ensure!(rows.len() <= 10_000, "upstream_depth_capacity");
        rows.into_iter()
            .map(|r| {
                ensure!(
                    decimal(&r.price, true)? <= Decimal::ONE,
                    "invalid_probability"
                );
                decimal(&r.size, true)?;
                Ok(market::PriceLevel {
                    price: r.price,
                    size: r.size,
                })
            })
            .collect()
    };
    let at = b
        .timestamp
        .map(|t| -> Result<i64> {
            ensure!(
                !t.is_empty() && t.bytes().all(|c| c.is_ascii_digit()),
                "invalid_supplier_timestamp"
            );
            let at = t.parse()?;
            ensure!(at > 0, "invalid_supplier_timestamp");
            Ok(at)
        })
        .transpose()?;
    if let Some(v) = &b.min_order_size {
        decimal(v, false)?;
    }
    if let Some(v) = &b.tick_size {
        decimal(v, false)?;
    }
    if let Some(v) = &b.last_trade_price {
        ensure!(decimal(v, true)? <= Decimal::ONE, "invalid_probability");
    }
    Ok(facts::OfficialBook {
        market: b.market,
        asset_id: b.asset_id,
        supplier_timestamp_ms: at,
        supplier_hash: b.hash,
        bids: levels(b.bids)?,
        asks: levels(b.asks)?,
        min_order_size: b.min_order_size,
        tick_size: b.tick_size,
        neg_risk: b.neg_risk,
        last_trade_price: b.last_trade_price,
    })
}
fn add_unsigned(a: &str, b: &str) -> String {
    let mut out = Vec::with_capacity(a.len().max(b.len()) + 1);
    let (mut a, mut b) = (a.bytes().rev(), b.bytes().rev());
    let mut carry = 0u8;
    loop {
        let (x, y) = (a.next(), b.next());
        if x.is_none() && y.is_none() && carry == 0 {
            break;
        }
        let v = x.map_or(0, |v| v - b'0') + y.map_or(0, |v| v - b'0') + carry;
        out.push(b'0' + v % 10);
        carry = v / 10;
    }
    if out.is_empty() {
        out.push(b'0');
    }
    out.reverse();
    String::from_utf8(out).unwrap()
}
fn valid_provider(p: &facts::FinalityProvider, key: &market::MarketKey) -> Result<()> {
    ensure!(
        !p.provider_id.is_empty() && !p.operator_id.is_empty() && !p.failure_domain.is_empty(),
        "provider_identity_missing"
    );
    validate_identity(
        p.configuration
            .as_ref()
            .context("provider_configuration_missing")?,
    )?;
    ensure!(
        matches!(
            p.status.as_str(),
            "valid_resolved" | "unresolved" | "failed" | "timed_out" | "invalid"
        ),
        "nonterminal_provider_read"
    );
    if p.status != "valid_resolved" {
        return Ok(());
    }
    ensure!(
        p.token_binding_verified && p.deployment_code_verified && p.endpoint_identity_verified,
        "provider_not_verified"
    );
    let block = p.block.as_ref().context("finalized_block_missing")?;
    ensure!(hex_identity(&block.hash, 32), "invalid_finalized_block");
    uint256(&block.number)?;
    ensure!(
        p.observed_at_ns.is_some_and(|t| t > 0)
            && p.chain_id.as_ref().is_some_and(|s| !s.is_empty())
            && p.ctf_address.as_ref().is_some_and(|s| hex_identity(s, 20))
            && p.collateral.as_ref().is_some_and(|s| hex_identity(s, 20)),
        "provider_facts_missing"
    );
    ensure!(
        p.payouts.len() == 2
            && p.payouts[0].token_id == key.up_token_id
            && p.payouts[1].token_id == key.down_token_id,
        "payout_token_binding"
    );
    let denominator = p
        .denominator
        .as_deref()
        .context("payout_denominator_missing")?;
    uint256(denominator)?;
    ensure!(denominator != "0", "provider_not_resolved");
    for p in &p.payouts {
        uint256(&p.numerator)?;
    }
    ensure!(
        add_unsigned(&p.payouts[0].numerator, &p.payouts[1].numerator) == denominator,
        "payout_sum_mismatch"
    );
    Ok(())
}
pub fn decide_finality(
    request: &facts::MarketRequest,
    meta: facts::FactMeta,
    providers: [facts::FinalityProvider; 2],
    durable_sequence: Option<u64>,
    per_provider_deadline_ms: u32,
) -> Result<facts::FinalityResponse> {
    ensure!(
        (5000..=120_000).contains(&per_provider_deadline_ms),
        "invalid_provider_deadline"
    );
    let key = validate_request(request)?;
    validate_meta(&meta, key)?;
    ensure!(
        providers[0].provider_id != providers[1].provider_id,
        "duplicate_provider"
    );
    for p in &providers {
        valid_provider(p, key)?;
    }
    let valid = providers
        .iter()
        .filter(|p| p.status == "valid_resolved")
        .collect::<Vec<_>>();
    let conflict = valid.len() == 2
        && (valid[0].chain_id != valid[1].chain_id
            || valid[0].ctf_address != valid[1].ctf_address
            || valid[0].collateral != valid[1].collateral
            || valid[0].payouts != valid[1].payouts
            || valid[0].denominator != valid[1].denominator);
    let durable = durable_sequence.is_some_and(|s| s > 0) && !valid.is_empty() && !conflict;
    let status = if conflict {
        "conflict"
    } else if valid.is_empty() {
        if providers.iter().any(|p| p.status == "unresolved") {
            "unresolved"
        } else {
            "provider_failed"
        }
    } else if durable {
        "verified_final"
    } else {
        "awaiting_durability"
    };
    Ok(facts::FinalityResponse {
        schema_version: contract::FINALITY_SCHEMA.into(),
        request_id: request.request_id.clone(),
        meta: Some(meta),
        status: status.into(),
        policy: "parallel_two_endpoints_at_least_one_valid_finalized_no_conflict_v2".into(),
        provider_reads_requested: 2,
        provider_reads_completed_before_decision: true,
        valid_provider_count: valid.len() as u32,
        decision_strategy: "wait_both_bounded_reads_no_late_commit".into(),
        payouts: if conflict {
            Vec::new()
        } else {
            valid.first().map(|p| p.payouts.clone()).unwrap_or_default()
        },
        denominator: if conflict {
            None
        } else {
            valid.first().and_then(|p| p.denominator.clone())
        },
        settlement_import_allowed: durable,
        durable,
        durable_sequence: if durable { durable_sequence } else { None },
        reason: if conflict {
            Some("finality_provider_disagreement".into())
        } else {
            None
        },
        identity_assurance: "registry_bound_https_operator_endpoints".into(),
        per_provider_deadline_ms,
        providers: providers.into(),
    })
}

pub fn discovery(markets: Vec<market::MarketBinding>) -> Result<market::MarketStreams> {
    ensure!(markets.len() <= 3, "market_discovery_capacity");
    let mut ids = std::collections::BTreeSet::new();
    let (mut current, mut next, mut retiring) = (0, 0, 0);
    for m in &markets {
        let key = m.key.as_ref().context("missing_market_key")?;
        validate_key(key)?;
        ensure!(
            !m.stream_id.is_empty() && ids.insert(key.market_id.clone()),
            "duplicate_market_binding"
        );
        match m.role.as_str() {
            "current" => current += 1,
            "next" => next += 1,
            "retiring" => retiring += 1,
            _ => anyhow::bail!("invalid_market_role"),
        };
    }
    ensure!(
        current <= 1 && next <= 1 && retiring <= 1,
        "duplicate_market_role"
    );
    Ok(market::MarketStreams {
        schema_version: contract::MARKET_STREAMS_SCHEMA.into(),
        markets,
        delivery_budget: Some(market::DeliveryBudget {
            maximum_frame_bytes: 8 * 1024 * 1024,
            maximum_state_bytes: 8 * 1024 * 1024,
            maximum_levels_per_side: 10_000,
            maximum_changes: 40_000,
        }),
    })
}
