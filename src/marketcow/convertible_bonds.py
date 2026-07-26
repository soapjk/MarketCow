from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from statistics import median
from typing import Any, Callable, Iterable, Mapping


SCHEMA = "marketcow.convertible-bond.v1"
BASIC_DOC = "https://tushare.pro/document/2?doc_id=185"
ISSUE_DOC = "https://tushare.pro/document/2?doc_id=186"
DAILY_DOC = "https://tushare.pro/document/2?doc_id=187"
SOURCE = "tushare_pro"
RATING_ORDER = {
    "AAA": 10, "AA+": 9, "AA": 8, "AA-": 7, "A+": 6, "A": 5,
    "A-": 4, "BBB+": 3, "BBB": 2, "BBB-": 1,
}

FACT_FIELDS = (
    "issue_price", "par_value", "initial_conversion_price",
    "latest_conversion_price", "term_years", "maturity_date",
    "redemption_terms", "put_terms", "downward_revision_terms",
    "issuer_rating", "bond_rating", "audit_opinion",
    "debt_overdue_or_default", "major_violation_or_fraud",
    "going_concern_risk", "issue_size_billion", "remaining_size_billion",
    "shareholder_placement_pct", "record_date", "subscription_date",
    "winning_date", "payment_date", "listing_date",
)

SCORER_FIELDS = (
    "bond_name", "issuer", "assessment_date", "issue_price", "stock_price",
    "conversion_price", "expected_market_premium_pct", "credit_rating",
    "audit_opinion", "st_or_delisting_risk", "debt_overdue_or_default",
    "major_violation_or_fraud", "going_concern_risk",
    "ocf_3y_sum_positive", "core_profit_positive", "cash_to_short_term_debt",
    "stock_quality_score", "industry_outlook_score", "stock_valuation_score",
    "comparable_relevance_score", "premium_confidence_score",
    "pricing_attractiveness_score", "issue_size_billion",
    "shareholder_placement_pct", "scarcity_score", "cb_valuation_percentile",
    "equity_market_regime", "days_to_listing_estimate",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _date(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    try:
        return date.fromisoformat(raw[:10]).isoformat()
    except ValueError:
        return None


def _number(value: Any, scale: float = 1.0) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value) * scale
    except (TypeError, ValueError):
        return None


def _canonical_code(ts_code: str) -> str:
    code, _, exchange = str(ts_code).strip().upper().partition(".")
    mic = {"SH": "XSHG", "SZ": "XSHE", "XSHG": "XSHG", "XSHE": "XSHE"}.get(
        exchange, exchange
    )
    return f"{code}.{mic}" if code and mic else code


def _provider_code(canonical_code: str) -> str:
    code, _, mic = canonical_code.partition(".")
    return f"{code}.{'SH' if mic == 'XSHG' else 'SZ'}"


def normalize_rating(raw: Any) -> str | None:
    value = str(raw or "").strip().upper()
    for suffix in (" STI", "STI", " PI", "PI"):
        if value.endswith(suffix):
            value = value[: -len(suffix)].strip()
    accepted = {
        "AAA", "AA+", "AA", "AA-", "A+", "A", "A-",
        "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-", "B", "CCC", "CC", "C",
    }
    return value if value in accepted else None


def _fact(
    value: Any,
    *,
    source_url: str,
    observed_at: str,
    published_at: str | None,
    quality_status: str,
    raw_value: Any = None,
    missing_reason: str | None = None,
    point_in_time: bool | None = None,
) -> dict[str, Any]:
    available = value is not None
    pit = bool(published_at) if point_in_time is None else point_in_time
    return {
        "value": value,
        "raw_value": raw_value if raw_value is not None else value,
        "status": "available" if available else "data_missing",
        "missing_reason": None if available else (missing_reason or "source_value_missing"),
        "source": SOURCE,
        "source_url": source_url,
        "observed_at": observed_at,
        "published_at": published_at,
        "ingested_at": observed_at,
        "quality_status": quality_status,
        "cache_status": "runtime_snapshot",
        "point_in_time": pit,
    }


def _missing(observed_at: str, reason: str = "not_supported_by_source") -> dict[str, Any]:
    return _fact(
        None, source_url=BASIC_DOC, observed_at=observed_at, published_at=None,
        quality_status="data_missing", missing_reason=reason, point_in_time=False,
    )


def records_from_tushare(
    basic_rows: Iterable[Mapping[str, Any]],
    issue_rows: Iterable[Mapping[str, Any]],
    stock_rows: Iterable[Mapping[str, Any]],
    observed_at: str | None = None,
) -> tuple[dict[str, Any], ...]:
    """Build a broad normalized snapshot from Tushare reference datasets."""
    observed = observed_at or utc_now()
    issues = {
        _canonical_code(str(row.get("ts_code") or "")): dict(row)
        for row in issue_rows if row.get("ts_code")
    }
    stocks = {
        _canonical_code(str(row.get("ts_code") or "")): dict(row)
        for row in stock_rows if row.get("ts_code")
    }
    records: list[dict[str, Any]] = []
    for source_row in basic_rows:
        raw = dict(source_row)
        provider_code = str(raw.get("ts_code") or raw.get("cb_code") or "").strip()
        bond_id = _canonical_code(provider_code)
        if "." not in bond_id:
            continue
        issue = issues.get(bond_id, {})
        stock_id = _canonical_code(str(raw.get("stk_code") or ""))
        stock = stocks.get(stock_id, {})
        ann_date = _date(issue.get("ann_date"))
        result_date = _date(issue.get("res_ann_date"))
        raw_rating = raw.get("newest_rating") or raw.get("issue_rating")
        normalized_rating = normalize_rating(raw_rating)
        code = bond_id.partition(".")[0]
        name = str(raw.get("bond_short_name") or issue.get("onl_name") or code)
        aliases = sorted({
            name, str(issue.get("onl_name") or ""), str(issue.get("shd_ration_name") or ""),
            code, provider_code, bond_id,
        } - {""})

        def basic(value: Any, *, quality: str = "provider_reference",
                  reason: str = "source_value_missing") -> dict[str, Any]:
            return _fact(
                value, source_url=BASIC_DOC, observed_at=observed,
                published_at=None, quality_status=quality, missing_reason=reason,
                point_in_time=False,
            )

        def issued(value: Any, *, result: bool = False,
                   reason: str = "source_value_missing") -> dict[str, Any]:
            return _fact(
                value, source_url=ISSUE_DOC, observed_at=observed,
                published_at=result_date if result else ann_date,
                quality_status="provider_reported_disclosure",
                missing_reason=reason, point_in_time=bool(result_date if result else ann_date),
            )

        issue_size_raw = _number(issue.get("issue_size"))
        issue_size = (
            issue_size_raw / 100_000_000
            if issue_size_raw is not None and issue_size_raw > 10_000
            else issue_size_raw
        )
        if issue_size is None:
            issue_size = _number(raw.get("issue_size"))
        issue_amount_yuan = (
            issue_size_raw if issue_size_raw is not None and issue_size_raw > 10_000
            else None
        )
        placement_volume = _number(issue.get("shd_ration_vol"))
        placement_price = _number(issue.get("shd_ration_price"))
        placement_pct = (
            placement_volume * placement_price / issue_amount_yuan * 100.0
            if placement_volume is not None and placement_price is not None
            and issue_amount_yuan not in (None, 0) else None
        )
        facts = {key: _missing(observed) for key in FACT_FIELDS}
        facts.update({
            "issue_price": issued(
                _number(issue.get("issue_price")) if issue else _number(raw.get("issue_price"))
            ),
            "par_value": basic(_number(raw.get("par"))),
            "initial_conversion_price": basic(_number(raw.get("first_conv_price"))),
            "latest_conversion_price": basic(_number(raw.get("conv_price"))),
            "term_years": basic(_number(raw.get("maturity"))),
            "maturity_date": basic(_date(raw.get("maturity_date"))),
            "redemption_terms": basic(raw.get("call_clause") or None),
            "put_terms": basic(raw.get("put_clause") or None),
            "downward_revision_terms": basic(raw.get("reset_clause") or None),
            "issuer_rating": _fact(
                normalized_rating, raw_value=raw_rating, source_url=BASIC_DOC,
                observed_at=observed, published_at=None,
                quality_status="normalized_provider_reference",
                missing_reason="rating_not_in_scorer_enum", point_in_time=False,
            ),
            "bond_rating": _fact(
                normalized_rating, raw_value=raw_rating, source_url=BASIC_DOC,
                observed_at=observed, published_at=None,
                quality_status="normalized_provider_reference",
                missing_reason="rating_not_in_scorer_enum", point_in_time=False,
            ),
            "issue_size_billion": issued(issue_size),
            "remaining_size_billion": basic(_number(raw.get("remain_size"))),
            "shareholder_placement_pct": issued(placement_pct, result=True),
            "record_date": issued(_date(issue.get("shd_ration_record_date"))),
            "subscription_date": issued(_date(issue.get("onl_date"))),
            "winning_date": issued(result_date, result=True),
            "payment_date": issued(_date(issue.get("shd_ration_pay_date")), result=True),
            # cb_basic supplies the date but no disclosure timestamp. It is usable in
            # current snapshots and deliberately hidden from as-of responses.
            "listing_date": basic(_date(raw.get("list_date"))),
        })
        issuer = stock.get("fullname") or stock.get("name") or raw.get("stk_short_name")
        records.append({
            "schema": SCHEMA,
            "bond_id": bond_id,
            "provider_code": _provider_code(bond_id),
            "code": code,
            "name": name,
            "full_name": raw.get("bond_full_name") or name,
            "aliases": aliases,
            "issuer": issuer,
            "issuer_quality_status": (
                "provider_legal_name" if stock.get("fullname") else "provider_display_name"
            ),
            "underlying_instrument_id": stock_id or None,
            "facts": facts,
            "source": SOURCE,
            "source_url": BASIC_DOC,
            "observed_at": observed,
            "published_at": ann_date,
            "ingested_at": observed,
            "quality_status": "provider_reference_join",
            "cache_status": "runtime_snapshot",
            "point_in_time": False,
        })
    return tuple(records)


def _scorer_map(record: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}

    def available(field: str, value: Any, source_path: str, raw: Any = None) -> None:
        mapping[field] = {
            "status": "available" if value is not None else "data_missing",
            "value": value,
            "raw_value": value if raw is None else raw,
            "source_path": source_path,
            "missing_reason": None if value is not None else "source_value_missing",
        }

    available("bond_name", record.get("name"), "name")
    available("issuer", record.get("issuer"), "issuer")
    available("issue_price", record["facts"]["issue_price"]["value"], "facts.issue_price")
    rating = record["facts"]["bond_rating"]
    available("credit_rating", rating["value"], "facts.bond_rating", rating["raw_value"])
    available(
        "conversion_price", record["facts"]["latest_conversion_price"]["value"],
        "facts.latest_conversion_price",
    )
    for field in (
        "audit_opinion", "debt_overdue_or_default", "major_violation_or_fraud",
        "going_concern_risk", "issue_size_billion", "shareholder_placement_pct",
    ):
        available(field, record["facts"][field]["value"], f"facts.{field}")
    external = {
        "assessment_date": "caller_supplied",
        "stock_price": "get_convertible_bond_market.stock_quote",
        "expected_market_premium_pct": (
            "get_convertible_bond_market.comparable_premium_median_pct"
        ),
        "st_or_delisting_risk": "get_fundamental/get_instrument",
        "ocf_3y_sum_positive": "get_financial_statements",
        "core_profit_positive": "get_financial_statements",
        "cash_to_short_term_debt": "get_financial_statements",
        "days_to_listing_estimate": "facts.subscription_date/facts.listing_date",
    }
    scorer_judgments = {
        "stock_quality_score", "industry_outlook_score", "stock_valuation_score",
        "comparable_relevance_score", "premium_confidence_score",
        "pricing_attractiveness_score", "scarcity_score", "equity_market_regime",
    }
    for field in SCORER_FIELDS:
        if field in mapping:
            continue
        if field == "cb_valuation_percentile":
            mapping[field] = {
                "status": "data_missing",
                "value": None,
                "source_path": None,
                "missing_reason": (
                    "historical_market_valuation_percentile_not_available"
                ),
            }
            continue
        reason = "scorer_judgment_required" if field in scorer_judgments else "query_required"
        mapping[field] = {
            "status": "data_missing", "value": None,
            "source_path": external.get(field), "missing_reason": reason,
        }
    return mapping


class ConvertibleBondCatalog:
    """Lazy catalog backed by a broad provider snapshot; fixtures are opt-in."""

    def __init__(
        self,
        loader: Callable[[], Iterable[Mapping[str, Any]]] | None = None,
        records: Iterable[Mapping[str, Any]] | None = None,
    ):
        self._loader = loader
        self._records = (
            tuple(deepcopy(dict(row)) for row in records) if records is not None else None
        )
        self.load_error: str | None = None

    @property
    def records(self) -> tuple[dict[str, Any], ...]:
        if self._records is None:
            try:
                if self._loader is None:
                    raise RuntimeError("convertible-bond catalog source is not configured")
                self._records = tuple(deepcopy(dict(row)) for row in self._loader())
            except Exception as exc:  # surfaced as structured data-unavailable output
                self.load_error = f"{type(exc).__name__}: {exc}"
                return ()
        return self._records

    @staticmethod
    def _key(value: str) -> str:
        return "".join(str(value).strip().upper().split())

    def search(self, query: str, limit: int = 12) -> dict[str, Any]:
        key = self._key(query)
        items = []
        records = self.records
        for row in records:
            candidates = [
                row["bond_id"], row["provider_code"], row["code"], row["name"],
                row["full_name"], *row.get("aliases", []),
            ]
            if any(key in self._key(value) for value in candidates):
                items.append({
                    field: row.get(field) for field in (
                        "bond_id", "provider_code", "code", "name", "full_name",
                        "issuer", "underlying_instrument_id", "source", "source_url",
                        "observed_at", "published_at", "ingested_at", "quality_status",
                        "cache_status",
                    )
                })
                items[-1]["match_status"] = "matched"
        return {
            "schema": SCHEMA,
            "query": query,
            "catalog_size": len(records),
            "catalog_status": "available" if records else "data_missing",
            "catalog_error": self.load_error,
            "count": len(items[:limit]),
            "items": items[:limit],
            "empty_result_semantics": (
                "no match in the current provider snapshot; this does not prove that "
                "the security does not exist"
            ),
        }

    def get(self, bond_id: str, as_of: str = "") -> dict[str, Any]:
        key = self._key(bond_id)
        records = self.records
        for row in records:
            candidates = [
                row["bond_id"], row["provider_code"], row["code"], row["name"],
                *row.get("aliases", []),
            ]
            if key not in {self._key(value) for value in candidates}:
                continue
            result = deepcopy(row)
            result["status"] = "available"
            result["as_of"] = as_of or None
            hidden = 0
            if as_of:
                cutoff = as_of[:10]
                for field, fact in result["facts"].items():
                    published = fact.get("published_at")
                    if not fact.get("point_in_time") or not published or published > cutoff:
                        result["facts"][field] = {
                            **fact, "value": None, "status": "data_missing",
                            "missing_reason": (
                                "point_in_time_unavailable" if not published
                                else "not_published_as_of_cutoff"
                            ),
                        }
                        hidden += 1
            result["future_leakage_status"] = (
                "field_level_cutoff_applied" if as_of else
                "mixed; inspect each fact.point_in_time before historical use"
            )
            result["point_in_time_hidden_fact_count"] = hidden
            result["scorer_input_map"] = _scorer_map(result)
            return result
        return {
            "schema": SCHEMA,
            "status": "data_missing",
            "bond_id": bond_id,
            "catalog_size": len(records),
            "catalog_error": self.load_error,
            "missing_reason": "catalog_match_not_found",
            "empty_result_semantics": (
                "no match in the current provider snapshot; security existence is unknown"
            ),
        }


def percentile(values: list[float], value: float) -> float:
    if not values:
        raise ValueError("percentile sample is empty")
    return round(100.0 * sum(item <= value for item in values) / len(values), 2)


def build_market_snapshot(
    catalog: ConvertibleBondCatalog,
    cb_daily_rows: Iterable[Mapping[str, Any]],
    stock_daily_rows: Iterable[Mapping[str, Any]],
    observed_at: str | None = None,
) -> dict[str, Any]:
    observed = observed_at or utc_now()
    cb_rows = [dict(row) for row in cb_daily_rows if row.get("trade_date")]
    if not cb_rows:
        return {"trade_date": None, "observed_at": observed, "items": {}}
    trade_date = max(str(row["trade_date"]) for row in cb_rows)
    bonds = {
        _canonical_code(str(row.get("ts_code") or "")): row
        for row in cb_rows if str(row.get("trade_date")) == trade_date
    }
    stocks = {
        _canonical_code(str(row.get("ts_code") or "")): dict(row)
        for row in stock_daily_rows if str(row.get("trade_date")) == trade_date
    }
    items: dict[str, dict[str, Any]] = {}
    for record in catalog.records:
        bond = bonds.get(record["bond_id"])
        stock = stocks.get(record.get("underlying_instrument_id"))
        conversion_price = record["facts"]["latest_conversion_price"]["value"]
        stock_price = _number(stock.get("close")) if stock else None
        bond_price = _number(bond.get("close")) if bond else None
        conversion_value = (
            100.0 * stock_price / float(conversion_price)
            if stock_price is not None and conversion_price not in (None, 0) else None
        )
        premium = (
            (bond_price / conversion_value - 1.0) * 100.0
            if bond_price is not None and conversion_value not in (None, 0) else None
        )
        if bond_price is None and stock_price is None:
            continue
        items[record["bond_id"]] = {
            "bond_id": record["bond_id"],
            "underlying_instrument_id": record["underlying_instrument_id"],
            "price": bond_price,
            "stock_price": stock_price,
            "conversion_value": conversion_value,
            "conversion_premium_pct": premium,
            "bond_rating": record["facts"]["bond_rating"]["value"],
            "issue_size_billion": record["facts"]["issue_size_billion"]["value"],
            "trade_date": _date(trade_date),
            "observed_at": observed,
            "source": SOURCE,
            "source_url": DAILY_DOC,
            "published_at": _date(trade_date),
            "ingested_at": observed,
            "quality_status": "derived_from_daily_closes",
            "cache_status": "runtime_snapshot",
        }
    return {"trade_date": _date(trade_date), "observed_at": observed, "items": items}


def market_view(
    record: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    minimum_comparables: int = 3,
) -> dict[str, Any]:
    target = (snapshot.get("items") or {}).get(str(record["bond_id"]))
    market = [
        row for bond_id, row in (snapshot.get("items") or {}).items()
        if bond_id != record["bond_id"] and row.get("conversion_premium_pct") is not None
    ]
    target_rating = None if not target else target.get("bond_rating")
    target_size = None if not target else target.get("issue_size_billion")
    target_parity = None if not target else target.get("conversion_value")

    def matched(row: Mapping[str, Any], parity_band: float, rating_gap: int,
                minimum_size_ratio: float, maximum_size_ratio: float) -> bool:
        row_parity = row.get("conversion_value")
        row_rating = row.get("bond_rating")
        row_size = row.get("issue_size_billion")
        if None in (target_parity, row_parity, target_rating, row_rating, target_size, row_size):
            return False
        if target_parity == 0 or target_size == 0:
            return False
        parity_ratio = float(row_parity) / float(target_parity)
        size_ratio = float(row_size) / float(target_size)
        rating_distance = abs(
            RATING_ORDER.get(str(row_rating), -99)
            - RATING_ORDER.get(str(target_rating), 99)
        )
        return (
            1 - parity_band <= parity_ratio <= 1 + parity_band
            and minimum_size_ratio <= size_ratio <= maximum_size_ratio
            and rating_distance <= rating_gap
        )

    tiers = (
        ("strict", 0.20, 1, 0.33, 3.0),
        ("balanced", 0.35, 2, 0.20, 5.0),
        ("broad", 0.50, 3, 0.10, 10.0),
    )
    candidates: list[Mapping[str, Any]] = []
    comparable_tier = "unavailable"
    for tier, parity_band, rating_gap, minimum_ratio, maximum_ratio in tiers:
        candidates = [
            row for row in market
            if matched(row, parity_band, rating_gap, minimum_ratio, maximum_ratio)
        ]
        if len(candidates) >= minimum_comparables:
            comparable_tier = tier
            break
    candidate_prices = [float(row["price"]) for row in candidates]
    candidate_premiums = [float(row["conversion_premium_pct"]) for row in candidates]
    market_prices = [float(row["price"]) for row in market]
    market_premiums = [float(row["conversion_premium_pct"]) for row in market]
    enough = len(candidates) >= minimum_comparables
    comparable_premium_median = median(candidate_premiums) if enough else None
    target_bond_price = None if not target else target.get("price")
    target_premium = None if not target else target.get("conversion_premium_pct")
    stock_price = None if not target else target.get("stock_price")
    scorer_market_map = {
        "stock_price": {
            "status": "available" if stock_price is not None else "data_missing",
            "value": stock_price,
            "source_path": "stock_quote.price",
            "missing_reason": None if stock_price is not None else "stock_daily_data_missing",
        },
        "expected_market_premium_pct": {
            "status": "available" if enough else "data_missing",
            "value": comparable_premium_median,
            "source_path": "comparable_premium_median_pct",
            "missing_reason": (
                None if enough else
                f"fewer_than_{minimum_comparables}_complete_comparables"
            ),
        },
        "cb_valuation_percentile": {
            "status": "data_missing",
            "value": None,
            "source_path": None,
            "missing_reason": (
                "historical_market_valuation_percentile_not_available"
            ),
        },
    }
    return {
        "schema": "marketcow.convertible-bond-market.v1",
        "bond_id": record["bond_id"],
        "status": "available" if target else "data_missing",
        "bond_quote": None if target_bond_price is None else {
            "symbol": target["bond_id"], "price": target["price"],
            "trade_date": target["trade_date"], "source": target["source"],
            "source_url": target["source_url"], "observed_at": target["observed_at"],
            "published_at": target["published_at"], "ingested_at": target["ingested_at"],
            "quality_status": target["quality_status"],
            "cache_status": target["cache_status"],
        },
        "stock_quote": None if not target else {
            "symbol": target["underlying_instrument_id"], "price": target["stock_price"],
            "trade_date": target["trade_date"], "source": target["source"],
            "source_url": target["source_url"], "observed_at": target["observed_at"],
            "published_at": target["published_at"], "ingested_at": target["ingested_at"],
            "quality_status": target["quality_status"],
            "cache_status": target["cache_status"],
        },
        "conversion_value": None if not target else target["conversion_value"],
        "conversion_premium_pct": (
            target_premium
        ),
        "yield_to_maturity_pct": None,
        "yield_to_maturity_status": "data_missing",
        "yield_to_maturity_missing_reason": "cash_flow_engine_not_implemented",
        "issue_size_billion": record["facts"]["issue_size_billion"],
        "remaining_size_billion": record["facts"]["remaining_size_billion"],
        "comparable_method": (
            "same-date bonds matched progressively on conversion-value band, "
            "normalized rating distance and issue-size ratio"
        ),
        "comparable_tier": comparable_tier,
        "comparable_tier_rules": {
            "strict": {"conversion_value_band_pct": 20, "rating_notches": 1,
                       "issue_size_ratio": [0.33, 3.0]},
            "balanced": {"conversion_value_band_pct": 35, "rating_notches": 2,
                         "issue_size_ratio": [0.2, 5.0]},
            "broad": {"conversion_value_band_pct": 50, "rating_notches": 3,
                      "issue_size_ratio": [0.1, 10.0]},
        },
        "market_sample_size": len(market),
        "sample_size": len(candidates),
        "sample_bond_ids": [row["bond_id"] for row in candidates],
        "comparable_sample": [
            {
                "bond_id": row["bond_id"],
                "conversion_premium_pct": row["conversion_premium_pct"],
            }
            for row in candidates
        ],
        "comparable_premium_median_pct": comparable_premium_median,
        "comparable_premium_statistic_status": (
            "available" if enough else "data_missing"
        ),
        "comparable_premium_statistic_missing_reason": (
            None if enough else
            f"fewer_than_{minimum_comparables}_complete_comparables"
        ),
        "target_cross_sectional_comparable_price_percentile": (
            percentile(candidate_prices, float(target_bond_price))
            if enough and target_bond_price is not None else None
        ),
        "target_cross_sectional_comparable_premium_percentile": (
            percentile(candidate_premiums, float(target_premium))
            if enough and target_premium is not None else None
        ),
        "target_cross_sectional_market_price_percentile": (
            percentile(market_prices, float(target_bond_price))
            if market and target_bond_price is not None else None
        ),
        "target_cross_sectional_market_premium_percentile": (
            percentile(market_premiums, float(target_premium))
            if market and target_premium is not None else None
        ),
        "cross_sectional_percentile_status": (
            "available" if enough and target_premium is not None else "data_missing"
        ),
        "cross_sectional_percentile_missing_reason": (
            None if enough and target_premium is not None else
            "target_daily_data_missing" if not target else
            "target_bond_daily_data_missing" if target_bond_price is None else
            f"fewer_than_{minimum_comparables}_complete_comparables"
        ),
        "historical_market_valuation_percentile": None,
        "historical_market_valuation_percentile_status": "data_missing",
        "historical_market_valuation_percentile_missing_reason": (
            "historical_market_valuation_percentile_not_available"
        ),
        "scorer_input_map": scorer_market_map,
        "trade_date": snapshot.get("trade_date"),
        "source": SOURCE,
        "source_url": DAILY_DOC,
        "observed_at": snapshot.get("observed_at"),
        "published_at": snapshot.get("trade_date"),
        "ingested_at": snapshot.get("observed_at"),
        "quality_status": "derived_from_same_date_daily_closes",
        "cache_status": "runtime_snapshot",
    }
