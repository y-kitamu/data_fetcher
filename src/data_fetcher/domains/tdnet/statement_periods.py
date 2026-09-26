"""Shape TDnet's and EDINET's tidy financial data into full balance-sheet /
income-statement / cash-flow line items per filing.
"""

from __future__ import annotations

import datetime as dt

import polars as pl
from pydantic import BaseModel, Field

from .constants.statement_keys import (
    BALANCE_SHEET_COMPOSITE,
    BALANCE_SHEET_FALLBACK,
    BALANCE_SHEET_SIMPLE,
    CASH_FLOW_SIMPLE,
    INCOME_STATEMENT_SIMPLE,
    SHARES_SIMPLE,
)
from .financial_periods import _detect_window, _extract

_ORIGINAL_DOC_STYLES = ["edjp", "edus", "edif", "edit", "rejp", "efjp"]
_JST = dt.timezone(dt.timedelta(hours=9))


class SegmentValue(BaseModel):
    """A per-segment revenue/operating income figure.

    `member` is the raw XBRL segment-member string with the `Member` suffix
    stripped, not a translated business-segment name: TDnet's own-taxonomy
    business segment labels are not resolved by the existing ingestion
    pipeline (they live in a company-specific label linkbase that isn't
    parsed today), and IFRS filers additionally only tag "ReportableSegments"
    / "OtherReportableSegments" aggregates rather than each named segment.
    Callers should treat this as a best-effort identifier and surface a
    MISSING_SEGMENT_LABEL warning alongside it.
    """

    member: str
    revenue: float | None
    operating_income: float | None


class StatementPeriod(BaseModel):
    """One filing's full balance-sheet / income-statement / cash-flow figures.

    Unlike FinancialPeriod (financial_periods.py), values here are plain
    floats rather than ActualForecast: the financial-statement XBRL attached
    to a tanshin only ever tags actuals, never forecasts.
    """

    source: str = "tdnet"
    fiscal_year_end: dt.date
    doc_period: str  # "a" | "s" | "q"
    quarter_number: int  # 0=年次, 1-3=四半期
    submitted_at: dt.datetime
    is_consolidated: bool | None
    balance_sheet: dict[str, float | None] = Field(default_factory=dict)
    income_statement: dict[str, float | None] = Field(default_factory=dict)
    cash_flow: dict[str, float | None] = Field(default_factory=dict)
    shares: dict[str, float | None] = Field(default_factory=dict)
    segments: list[SegmentValue] = Field(default_factory=list)

    @property
    def fiscal_year(self) -> str:
        """決算期末年月 'YYYY-MM' (指示書§6の年度ラベル形式)。"""
        return f"{self.fiscal_year_end.year:04d}-{self.fiscal_year_end.month:02d}"


def shape_statement_periods(df: pl.DataFrame) -> list[StatementPeriod]:
    """Shape one symbol's tidy TDnet facts into per-filing StatementPeriod
    records (original settlements only - standalone forecast-revision
    notices never carry a financial statement).
    """
    if df.height == 0:
        return []
    settlements_df = df.filter(pl.col("doc_style").is_in(_ORIGINAL_DOC_STYLES))
    periods = [
        _build_statement_period(filing_df)
        for _, filing_df in settlements_df.group_by("source_file", maintain_order=True)
    ]
    return sorted(
        (p for p in periods if p is not None),
        key=lambda p: p.submitted_at,
        reverse=True,
    )


def _build_statement_period(filing_df: pl.DataFrame) -> StatementPeriod | None:
    if filing_df["fiscal_year_end"][0] is None:
        # collect_documents() couldn't find a FiscalYearEnd tag for this zip
        # (logs a warning at ingestion time) - without it we can't label the
        # period, so skip rather than crash.
        return None
    doc_period = filing_df["doc_period"][0]
    window = _detect_window(filing_df, doc_period, "ConsolidatedMember")
    is_consolidated = True
    if window is None:
        window = _detect_window(filing_df, doc_period, "NonConsolidatedMember")
        is_consolidated = False
    if window is None:
        return None
    window_periods, _member, quarter_number = window
    consolidateds = [
        "ConsolidatedMember" if is_consolidated else "NonConsolidatedMember",
        "",
    ]
    forecasts = ["ResultMember", ""]

    balance_sheet = _resolve_balance_sheet(
        filing_df, window_periods, consolidateds, forecasts
    )
    income_statement = {
        key: _extract(
            filing_df,
            concept=concept,
            periods=window_periods,
            consolidateds=consolidateds,
            forecasts=forecasts,
        )
        for key, concept in INCOME_STATEMENT_SIMPLE.items()
    }
    cash_flow = {
        key: _extract(
            filing_df,
            concept=concept,
            periods=window_periods,
            consolidateds=consolidateds,
            forecasts=forecasts,
        )
        for key, concept in CASH_FLOW_SIMPLE.items()
    }
    shares = {
        key: _extract(
            filing_df,
            concept=concept,
            periods=window_periods,
            consolidateds=consolidateds,
            forecasts=forecasts,
        )
        for key, concept in SHARES_SIMPLE.items()
    }
    segments = _extract_segments(filing_df, window_periods, consolidateds)

    fiscal_year_end = dt.date.fromisoformat(filing_df["fiscal_year_end"][0])
    submitted_at = dt.datetime.fromisoformat(filing_df["filing_datetime"][0])
    if submitted_at.tzinfo is None:
        submitted_at = submitted_at.replace(tzinfo=_JST)

    return StatementPeriod(
        fiscal_year_end=fiscal_year_end,
        doc_period=doc_period,
        quarter_number=quarter_number,
        submitted_at=submitted_at,
        is_consolidated=is_consolidated,
        balance_sheet=balance_sheet,
        income_statement=income_statement,
        cash_flow=cash_flow,
        shares=shares,
        segments=segments,
    )


def _resolve_balance_sheet(
    filing_df: pl.DataFrame,
    periods: list[str],
    consolidateds: list[str],
    forecasts: list[str],
) -> dict[str, float | None]:
    all_concepts = (
        set(BALANCE_SHEET_SIMPLE.values())
        | {c for cs in BALANCE_SHEET_COMPOSITE.values() for c in cs}
        | {c for cs in BALANCE_SHEET_FALLBACK.values() for c in cs}
    )
    raw = {
        concept: _extract(
            filing_df,
            concept=concept,
            periods=periods,
            consolidateds=consolidateds,
            forecasts=forecasts,
        )
        for concept in all_concepts
    }

    result: dict[str, float | None] = {
        key: raw.get(concept) for key, concept in BALANCE_SHEET_SIMPLE.items()
    }
    for key, concepts in BALANCE_SHEET_COMPOSITE.items():
        result[key] = _coalesce_sum(raw.get(c) for c in concepts)
    for key, concepts in BALANCE_SHEET_FALLBACK.items():
        result[key] = next(
            (raw.get(c) for c in concepts if raw.get(c) is not None), None
        )
    return result


def _coalesce_sum(values) -> float | None:
    present = [v for v in values if v is not None]
    return sum(present) if present else None


def _extract_segments(
    filing_df: pl.DataFrame, periods: list[str], consolidateds: list[str]
) -> list[SegmentValue]:
    revenue_by_segment = _segment_values(filing_df, "net_sales", periods, consolidateds)
    op_income_by_segment = _segment_values(
        filing_df, "operating_profit", periods, consolidateds
    )
    members = sorted(set(revenue_by_segment) | set(op_income_by_segment))
    return [
        SegmentValue(
            member=_strip_member_suffix(member),
            revenue=revenue_by_segment.get(member),
            operating_income=op_income_by_segment.get(member),
        )
        for member in members
    ]


def _strip_member_suffix(member: str) -> str:
    return member[: -len("Member")] if member.endswith("Member") else member


def _segment_values(
    filing_df: pl.DataFrame, concept: str, periods: list[str], consolidateds: list[str]
) -> dict[str, float]:
    condition = (
        (pl.col("concept") == concept)
        & (pl.col("segments") != "")
        & (pl.col("previous_current") != "PreviousMember")
        & (pl.col("value").is_not_null())
    )
    rows = filing_df.filter(condition)
    if consolidateds:
        rows = rows.filter(pl.col("consolidated").is_in(consolidateds))
    if periods:
        rows = rows.filter(pl.col("period").is_in(periods))
    result: dict[str, float] = {}
    for member, value in zip(rows["segments"].to_list(), rows["value"].to_list()):
        if value is not None and member not in result:
            result[member] = float(value)
    return result


def shape_edinet_statement_periods(df: pl.DataFrame) -> list[StatementPeriod]:
    """Shape EDINET's flat summary CSV into StatementPeriod records.

    Only revenue/cost_of_sales/operating_income/ordinary_income/net_income
    (income_statement) and total_assets/net_assets (balance_sheet) are ever
    populated; cash_flow and segments are always empty. `CurrentYear`,
    `Prior1Year`, ... all describe the same real fiscal year (identified by
    `end_date`) as seen from different filings, so rows are grouped directly
    by end_date rather than by that relative label.
    """
    if df.height == 0:
        return []
    consolidated = df.filter(
        ~pl.col("period").str.contains("NonConsolidatedMember")
    ).with_columns(pl.col("value").cast(pl.Float64, strict=False))
    periods: list[StatementPeriod] = []
    for (end_date,), group in consolidated.group_by(["end_date"]):
        values: dict[str, float] = {}
        for key, value in zip(group["key"].to_list(), group["value"].to_list()):
            if value is not None and key not in values:
                values[key] = value
        submitted_at = group["announce_date"].min()
        if submitted_at.tzinfo is None:
            submitted_at = submitted_at.replace(tzinfo=_JST)
        periods.append(
            StatementPeriod(
                source="edinet",
                fiscal_year_end=dt.date.fromisoformat(end_date),
                doc_period="a",
                quarter_number=0,
                submitted_at=submitted_at,
                is_consolidated=True,
                balance_sheet={
                    "total_assets": values.get("total_asset"),
                    "net_assets": values.get("net_asset"),
                },
                income_statement={
                    "revenue": values.get("net_sales"),
                    "cost_of_sales": values.get("cost_of_sales"),
                    "sga": None,
                    "operating_income": values.get("operating_income"),
                    "ordinary_income": values.get("ordinary_income"),
                    "net_income": values.get("net_income"),
                },
                cash_flow={},
                segments=[],
            )
        )
    return sorted(periods, key=lambda p: p.submitted_at, reverse=True)
