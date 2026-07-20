"""Backend-neutral transactional column contracts."""

FUNDAMENTAL_COLUMNS = [
    "instrument_id", "symbol", "exchange", "name", "is_active", "report_period",
    "published_at", "valuation_as_of", "price", "change_pct", "pe_dynamic",
    "pb", "total_market_cap", "float_market_cap", "roe_weighted", "eps",
    "revenue", "revenue_yoy", "revenue_qoq", "net_profit", "net_profit_yoy",
    "net_profit_qoq", "book_value_per_share", "ocf_per_share", "gross_margin",
    "industry", "cash", "accounts_receivable", "inventory", "total_assets",
    "total_assets_yoy", "accounts_payable", "advance_receipts",
    "total_liabilities", "total_liabilities_yoy", "debt_ratio", "total_equity",
    "operating_cost", "sales_expense", "admin_expense", "financial_expense",
    "total_operating_expense", "operating_profit", "total_profit", "net_cashflow",
    "net_cashflow_yoy", "operating_cashflow", "investing_cashflow",
    "financing_cashflow", "source", "source_url", "observed_at", "ingested_at",
    "raw_response_locator", "raw_path", "raw_artifact_id", "quality_status", "fetched_at",
]

PROVENANCE_COLUMNS = [
    "source", "source_url", "observed_at", "ingested_at",
    "raw_response_locator", "raw_path", "raw_artifact_id",
]

TDX_COLUMNS = [
    "symbol", "report_period", "published_at", "roe_weighted", "eps",
    "eps_adjusted", "book_value_per_share", "ocf_per_share", "cash",
    "accounts_receivable", "inventory", "total_assets", "total_liabilities",
    "total_equity", "revenue", "revenue_ttm", "net_profit_parent",
    "net_profit_parent_ttm", "operating_cashflow", "capex", "source_file",
    "source", "source_url", "observed_at", "ingested_at", "raw_response_locator",
    "raw_path", "raw_artifact_id", "fetched_at",
]
