"""Merge TDnet (primary) and EDINET (fallback-only) statement periods into
per-fiscal-year arrays for the report snapshot (section 6 of the report
generator instructions), following section 5.1's source-priority rules:
TDnet is authoritative wherever it has a value; EDINET fills only the years
TDnet lacks; a same-year value present in both that disagrees beyond a small
tolerance is recorded as a SOURCE_MISMATCH warning (TDnet's value still
wins).

TDnet's and EDINET's raw XBRL facts are yen; section 6 requires all
monetary amounts in millions of yen ("金額の単位は百万円"). This module is
the single place that conversion happens - every monetary series it
produces (balance_sheet/income_statement/cash_flow/quarterly figures) is
already in millions, so every downstream consumer (domains/jp_stocks/*,
snapshot_builder.py) can assume that unit without re-converting.
"""

from __future__ import annotations

import math
from collections import defaultdict

from pydantic import BaseModel

from ..tdnet.constants.statement_keys import (
    BALANCE_SHEET_COMPOSITE,
    BALANCE_SHEET_FALLBACK,
    BALANCE_SHEET_SIMPLE,
    CASH_FLOW_SIMPLE,
    INCOME_STATEMENT_SIMPLE,
)
from ..tdnet.statement_periods import StatementPeriod

BALANCE_SHEET_KEYS: tuple[str, ...] = tuple(
    [*BALANCE_SHEET_SIMPLE, *BALANCE_SHEET_COMPOSITE, *BALANCE_SHEET_FALLBACK]
)
INCOME_STATEMENT_BASE_KEYS: tuple[str, ...] = tuple(INCOME_STATEMENT_SIMPLE)
CASH_FLOW_BASE_KEYS: tuple[str, ...] = tuple(CASH_FLOW_SIMPLE)

_RELATIVE_TOLERANCE = 0.01
YEN_TO_MILLION = 1_000_000


class SnapshotWarning(BaseModel):
    code: str
    field: str
    message: str


class AnnualStatementsResult(BaseModel):
    fiscal_years: list[str]
    balance_sheet: dict[str, list[float | None]]
    income_statement: dict[str, list[float | None]]
    cash_flow: dict[str, list[float | None]]
    by_series: dict[str, dict[str, list[str]]]
    warnings: list[SnapshotWarning]


class QuarterlyStatementsResult(BaseModel):
    periods: list[str]
    revenue: list[float | None]
    operating_income: list[float | None]
    net_income: list[float | None]


def _latest_by_fiscal_year(periods: list[StatementPeriod]) -> dict[str, StatementPeriod]:
    """Pick the most-recently-submitted period per fiscal year (a later
    submission at the same fiscal year is a correction; the newer value
    wins per section 5.1's "TDnet, corrected value if corrected" rule).
    """
    by_fy: dict[str, StatementPeriod] = {}
    for p in periods:
        if p.quarter_number != 0:
            continue
        existing = by_fy.get(p.fiscal_year)
        if existing is None or p.submitted_at >= existing.submitted_at:
            by_fy[p.fiscal_year] = p
    return by_fy


def build_annual_statements(
    tdnet_periods: list[StatementPeriod],
    edinet_periods: list[StatementPeriod],
    max_years: int = 15,
) -> AnnualStatementsResult:
    tdnet_by_fy = _latest_by_fiscal_year(tdnet_periods)
    edinet_by_fy = _latest_by_fiscal_year(edinet_periods)
    fiscal_years = sorted(set(tdnet_by_fy) | set(edinet_by_fy))[-max_years:]

    balance_sheet: dict[str, list[float | None]] = {k: [] for k in BALANCE_SHEET_KEYS}
    income_statement: dict[str, list[float | None]] = {k: [] for k in INCOME_STATEMENT_BASE_KEYS}
    cash_flow: dict[str, list[float | None]] = {k: [] for k in CASH_FLOW_BASE_KEYS}
    by_series_acc: dict[str, dict[str, list[str]]] = {
        "balance_sheet": defaultdict(list),
        "income_statement": defaultdict(list),
        "cash_flow": defaultdict(list),
    }
    warnings: list[SnapshotWarning] = []

    sections: tuple[tuple[str, tuple[str, ...], dict[str, list[float | None]]], ...] = (
        ("balance_sheet", BALANCE_SHEET_KEYS, balance_sheet),
        ("income_statement", INCOME_STATEMENT_BASE_KEYS, income_statement),
        ("cash_flow", CASH_FLOW_BASE_KEYS, cash_flow),
    )

    for fy in fiscal_years:
        td = tdnet_by_fy.get(fy)
        ed = edinet_by_fy.get(fy)
        for section_name, keys, target in sections:
            td_values = getattr(td, section_name) if td is not None else {}
            ed_values = getattr(ed, section_name) if ed is not None else {}
            for key in keys:
                v_td = td_values.get(key)
                v_ed = ed_values.get(key)
                used = "missing"
                value: float | None = None
                if v_td is not None:
                    value, used = v_td, "tdnet"
                    if v_ed is not None and not math.isclose(
                        v_td, v_ed, rel_tol=_RELATIVE_TOLERANCE
                    ):
                        warnings.append(
                            SnapshotWarning(
                                code="SOURCE_MISMATCH",
                                field=f"{section_name}.{key}",
                                message=f"{fy}: tdnet={v_td} edinet={v_ed}",
                            )
                        )
                elif v_ed is not None:
                    value, used = v_ed, "edinet"
                if value is not None:
                    value = value / YEN_TO_MILLION
                target[key].append(value)
                if used != "missing":
                    # One entry per fiscal year (not per key); fiscal_years is
                    # sorted, so the tail check is enough to dedupe.
                    fys = by_series_acc[section_name][used]
                    if not fys or fys[-1] != fy:
                        fys.append(fy)

    return AnnualStatementsResult(
        fiscal_years=fiscal_years,
        balance_sheet=balance_sheet,
        income_statement=income_statement,
        cash_flow=cash_flow,
        by_series={name: dict(sources) for name, sources in by_series_acc.items()},
        warnings=warnings,
    )


def build_quarterly_statements(
    tdnet_periods: list[StatementPeriod], max_quarters: int = 12
) -> QuarterlyStatementsResult:
    """Compute standalone-quarter (not cumulative) revenue/operating_income/
    net_income by differencing TDnet's cumulative quarterly figures:
    Q1 standalone = Q1 cumulative, Q2 = Q2cum - Q1cum, Q3 = Q3cum - Q2cum,
    Q4 = annual - Q3cum (there is no standalone Q4 tanshin).
    """
    annuals: dict[str, StatementPeriod] = {}
    by_key: dict[tuple[str, int], StatementPeriod] = {}
    for p in tdnet_periods:
        if p.quarter_number == 0:
            existing = annuals.get(p.fiscal_year)
            if existing is None or p.submitted_at >= existing.submitted_at:
                annuals[p.fiscal_year] = p
        else:
            key = (p.fiscal_year, p.quarter_number)
            existing = by_key.get(key)
            if existing is None or p.submitted_at >= existing.submitted_at:
                by_key[key] = p

    quarters: list[tuple[str, int]] = []
    for fy in sorted(set(fy for fy, _ in by_key) | set(annuals)):
        for q in (1, 2, 3, 4):
            if q == 4:
                if fy in annuals and (fy, 3) in by_key:
                    quarters.append((fy, 4))
            elif (fy, q) in by_key:
                quarters.append((fy, q))

    keys = ("revenue", "operating_income", "net_income")
    values: dict[str, list[float | None]] = {key: [] for key in keys}
    labels: list[str] = []

    for fy, q in quarters:
        labels.append(f"{fy}-Q{q}")
        for key in keys:
            if q == 1:
                value = by_key[(fy, 1)].income_statement.get(key)
            elif q == 4:
                annual_value = annuals[fy].income_statement.get(key)
                q3_value = by_key[(fy, 3)].income_statement.get(key)
                value = (
                    annual_value - q3_value
                    if annual_value is not None and q3_value is not None
                    else None
                )
            else:
                cur_period = by_key[(fy, q)]
                prev_period = by_key.get((fy, q - 1))
                cur_value = cur_period.income_statement.get(key)
                prev_value = prev_period.income_statement.get(key) if prev_period else None
                value = cur_value - prev_value if cur_value is not None and prev_value is not None else None
            if value is not None:
                value = value / YEN_TO_MILLION
            values[key].append(value)

    trim = max(0, len(labels) - max_quarters)
    return QuarterlyStatementsResult(
        periods=labels[trim:],
        revenue=values["revenue"][trim:],
        operating_income=values["operating_income"][trim:],
        net_income=values["net_income"][trim:],
    )
