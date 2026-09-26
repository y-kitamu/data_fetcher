"""Map the normalized TDnet taxonomy concepts (taxonomy_group.py) onto the
balance_sheet / income_statement / cash_flow keys of the report snapshot
schema

`taxonomy_group.py` already groups every JGAAP/US-GAAP/IFRS taxonomy variant
of a given line item under one concept name (e.g. "total_assets" covers
jppfs_cor:Assets, jpigp_cor:AssetsIFRS and tse-ed-t:TotalAssets). Most schema
keys are therefore a straight 1:1 rename of an existing concept. A handful of
schema keys are coarser than the taxonomy grouping (e.g. the schema has one
`lease_obligations` field but the taxonomy splits current/noncurrent) and
need summing; a couple of debt fields also need a same-company-only-uses-one
variant fallback because some IFRS filers report a single combined
"bonds and borrowings" line instead of splitting it the way JGAAP filers do.
"""

from __future__ import annotations

BALANCE_SHEET_SIMPLE: dict[str, str] = {
    "cash": "cash_and_equivalents",
    "receivables": "receivables",
    "securities_current": "securities_current",
    "inventory": "inventory",
    "current_assets_other": "current_assets_other",
    "current_assets_total": "current_assets",
    "ppe": "ppe",
    "intangibles": "intangible_assets",
    "investment_securities": "investment_securities",
    "investments_other": "investments_other",
    "noncurrent_assets_total": "non_current_assets",
    "total_assets": "total_assets",
    "payables": "payables",
    "current_portion_long_term_debt": "current_portion_long_term_debt",
    "current_liabilities_other": "current_liabilities_other",
    "current_liabilities_total": "current_liabilities",
    "noncurrent_liabilities_other": "noncurrent_liabilities_other",
    "noncurrent_liabilities_total": "non_current_liabilities",
    "total_liabilities": "liabilities",
    "bonds": "bonds",
    "shareholders_equity": "shareholders_equity",
    "net_assets": "net_assets",
}

# §6のキーの方が粒度が粗く、複数concept名の合算が必要なもの。
# 合算は「非nullの値だけを足す。全てnullならnull」(coalesce-sum)。
BALANCE_SHEET_COMPOSITE: dict[str, list[str]] = {
    "current_portion_bonds": ["current_portion_bonds", "short_term_bonds"],
    "lease_obligations": ["lease_obligations_current", "lease_obligations_noncurrent"],
}

# 同一企業はJGAAPかIFRSかのどちらか一方のタクソノミしか使わないため、
# 主たるconceptがnullのときに限りIFRS結合科目にフォールバックする
# (合算はしない。二重計上を避けるため)。
BALANCE_SHEET_FALLBACK: dict[str, list[str]] = {
    "short_term_debt": ["short_term_borrowings", "bonds_and_borrowings_current"],
    "long_term_debt": ["long_term_borrowings", "bonds_and_borrowings_noncurrent"],
}

INCOME_STATEMENT_SIMPLE: dict[str, str] = {
    "revenue": "net_sales",
    "cost_of_sales": "cost_of_sales",
    "sga": "sga_expenses",
    "operating_income": "operating_profit",
    "ordinary_income": "ordinary_profit",
    "net_income": "net_income",
}

CASH_FLOW_SIMPLE: dict[str, str] = {
    "operating_cf": "cash_flow_from_operating_activities",
    "investing_cf": "cash_flow_from_investing_activities",
    "capex": "capital_investment",
    "depreciation": "depreciation",
    "financing_cf": "cash_flow_from_financial_activities",
}

# 株式数(発行済・自己株式)。§7.1のshares_outstanding解決に使う。
# financial_periods.pyのFIELD_SPECSはnumber_of_sharesのみでtreasury_sharesを
# 含まないため、ここで別途まとめて取得する。
SHARES_SIMPLE: dict[str, str] = {
    "number_of_shares": "number_of_shares",
    "treasury_shares": "treasury_shares",
}
