"""Shape TDnet's tidy long-format XBRL facts into structured per-filing
financial figures.

`gateway.get_financials(symbol, source="tdnet")` returns one row per
(element/concept, context) per filing - a tidy, long-format table. This
module turns that into one `FinancialPeriod` record per filing (actual vs.
forecast P&L/BS figures, category, period label), resolving TDnet-specific
XBRL quirks along the way: which context tag ("window") holds this filing's
actual vs. forecast figures, and which of several matching rows is the
"total" line item rather than a sub-component tagged with an extra segment
axis.

Pure data-in/data-out - no I/O beyond the one gateway call in
`get_financial_periods`, no knowledge of price or valuation ratios (see
domains.jp_stocks.valuation for those).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import numpy as np
import polars as pl
from pydantic import BaseModel, Field

from ..jp_stocks.valuation import ActualForecast

_ORIGINAL_DOC_STYLES = ["edjp", "edus", "edif", "edit", "rejp", "efjp"]
_FORECAST_REVISION_DOC_STYLES = ["rvfc"]
_FORECAST_REVISION_CATEGORY = "業績予想修正"

_JST = dt.timezone(dt.timedelta(hours=9))


@dataclass(frozen=True)
class _FieldSpec:
    concept: str
    field: str


# Public (not underscore-prefixed): a future consumer that needs additional
# TDnet concepts shaped the same way can append entries here rather than
# duplicating _build_period/_extract.
FIELD_SPECS: tuple[_FieldSpec, ...] = (
    _FieldSpec("net_sales", "total_revenue"),
    _FieldSpec("operating_profit", "operating_income"),
    _FieldSpec("ordinary_profit", "ordinary_profit"),
    _FieldSpec("net_income", "net_income"),
    _FieldSpec("eps", "eps"),
    _FieldSpec("revenue_yoy", "revenue_yoy"),
    _FieldSpec("operating_income_yoy", "operating_income_yoy"),
    _FieldSpec("ordinary_profit_yoy", "ordinary_profit_yoy"),
    _FieldSpec("net_income_yoy", "net_income_yoy"),
    _FieldSpec("bps", "bps"),
    _FieldSpec("net_assets", "net_assets"),
    _FieldSpec("total_assets", "total_assets"),
    _FieldSpec("dividend_per_share", "dividend"),
    _FieldSpec("number_of_shares", "number_of_shares"),
)


class FinancialPeriod(BaseModel):
    """One filing's shaped figures.

    `eps` is a plain `ActualForecast` here - estimating a forecast-revision
    filing's missing EPS forecast from the previously known shares
    outstanding is cross-filing timeline state, which belongs to whichever
    caller walks a symbol's full history (e.g. stock_viewer's
    FundamentalsService), not to this per-filing shaper.
    """

    source: str = "tdnet"
    category: str = ""
    period_label: str = ""
    submitted_at: dt.datetime
    is_consolidated: bool | None = None
    is_forecast_revision: bool = False

    total_revenue: ActualForecast = Field(default_factory=ActualForecast)
    revenue_yoy: ActualForecast = Field(default_factory=ActualForecast)
    operating_income: ActualForecast = Field(default_factory=ActualForecast)
    operating_income_yoy: ActualForecast = Field(default_factory=ActualForecast)
    ordinary_profit: ActualForecast = Field(default_factory=ActualForecast)
    ordinary_profit_yoy: ActualForecast = Field(default_factory=ActualForecast)
    net_income: ActualForecast = Field(default_factory=ActualForecast)
    net_income_yoy: ActualForecast = Field(default_factory=ActualForecast)
    dividend: ActualForecast = Field(default_factory=ActualForecast)
    bps: ActualForecast = Field(default_factory=ActualForecast)
    net_assets: ActualForecast = Field(default_factory=ActualForecast)
    total_assets: ActualForecast = Field(default_factory=ActualForecast)
    number_of_shares: ActualForecast = Field(default_factory=ActualForecast)
    eps: ActualForecast = Field(default_factory=ActualForecast)


def get_financial_periods(symbol: str, source: str = "tdnet") -> list[FinancialPeriod]:
    """Fetch a symbol's TDnet financials via the gateway and shape them.

    Convenience one-shot wrapper for data_fetcher-internal callers. Callers
    that already hold the raw DataFrame (e.g. because they fetched it
    through their own cached gateway call) should call
    `shape_financial_periods` directly instead of refetching.
    """
    # Imported lazily: gateway.py imports readers.tdnet, which imports
    # domains.tdnet.constants.taxonomy_group - a module-level import here
    # would cycle back into this package while it's still being initialized.
    from ... import gateway

    df = gateway.get_financials(symbol, source=source).get(source, pl.DataFrame())
    return shape_financial_periods(df)


def shape_financial_periods(df: pl.DataFrame) -> list[FinancialPeriod]:
    """Shape one symbol's tidy TDnet facts into structured filing records.

    `df` is expected to already be filtered to a single symbol (as returned
    by `TdnetReader.read_financial`/`gateway.get_financials`). Returns
    filings newest-first (original settlements ordered before standalone
    forecast-revision notices, matching TDnet's own doc_style grouping).
    """
    if df.height == 0:
        return []
    settlements_df = df.filter(pl.col("doc_style").is_in(_ORIGINAL_DOC_STYLES))
    items = [
        _build_period(filing_df)
        for _, filing_df in settlements_df.group_by("source_file", maintain_order=True)
    ]
    revisions_df = df.filter(pl.col("doc_style").is_in(_FORECAST_REVISION_DOC_STYLES))
    items += [
        _build_period(filing_df)
        for _, filing_df in revisions_df.group_by("source_file", maintain_order=True)
    ]
    periods = [item for item in items if item is not None]
    periods.sort(key=lambda i: i.submitted_at, reverse=True)
    return periods


def _build_period(filing_df: pl.DataFrame) -> FinancialPeriod | None:
    if filing_df["fiscal_year_end"][0] is None:
        return None
    is_revision = filing_df["doc_style"][0] in _FORECAST_REVISION_DOC_STYLES
    doc_period = "a" if is_revision else filing_df["doc_period"][0]
    window = _detect_window(filing_df, doc_period, "ConsolidatedMember")
    is_consolidated = True
    if window is None:
        window = _detect_window(filing_df, doc_period, "NonConsolidatedMember")
        is_consolidated = False
    if window is None:
        return None
    periods, _quarter_value, quarter_number = window
    consolidateds = [
        "ConsolidatedMember" if is_consolidated else "NonConsolidatedMember",
        "",
    ]
    if is_revision:
        forecast_periods = ["CurrentYear"]
    else:
        forecast_periods = ["NextYear" if doc_period == "a" else "CurrentYear"]

    actual_values = {
        spec.field: _extract(
            filing_df,
            concept=spec.concept,
            periods=periods,
            consolidateds=consolidateds,
            forecasts=["ResultMember", ""],
        )
        for spec in FIELD_SPECS
    }
    forecast_values = {
        spec.field: _extract(
            filing_df,
            concept=spec.concept,
            periods=forecast_periods,
            consolidateds=consolidateds,
            forecasts=["ForecastMember"],
        )
        for spec in FIELD_SPECS
    }
    values = {
        spec.field: ActualForecast(
            actual=actual_values.get(spec.field),
            forecast=forecast_values.get(spec.field),
        )
        for spec in FIELD_SPECS
    }

    category, period_label = _category_and_period_label(
        filing_df, doc_period, quarter_number
    )
    submitted_at = dt.datetime.fromisoformat(filing_df["filing_datetime"][0])
    if submitted_at.tzinfo is None:
        submitted_at = submitted_at.replace(tzinfo=_JST)

    return FinancialPeriod(
        source="tdnet",
        category=category,
        submitted_at=submitted_at,
        period_label=period_label,
        is_consolidated=is_consolidated,
        is_forecast_revision=category == _FORECAST_REVISION_CATEGORY,
        **values,
    )


def _detect_window(
    filing_df: pl.DataFrame, doc_period: str, consolidated: str
) -> tuple[list[str], str, int] | None:
    if doc_period == "a":
        candidates = [(["CurrentYear"], "AnnualMember", 0)]
    else:
        candidates = [
            (["CurrentAccumulatedQ1", "CurrentYTD"], "FirstQuarterMember", 1),
            (["CurrentAccumulatedQ2", "CurrentYTD"], "SecondQuarterMember", 2),
            (["CurrentAccumulatedQ3", "CurrentYTD"], "ThirdQuarterMember", 3),
        ]

    filing_df = filing_df.filter(pl.col("consolidated") == consolidated)
    counts = [
        len(filing_df.filter(pl.col("context_id").str.contains(periods[0])))
        for periods, _quarter_value, _quarter_number in candidates
    ]
    if np.max(counts) == 0:
        return None
    return candidates[np.argmax(counts)]


def _extract(
    filing_df: pl.DataFrame,
    concept: str,
    periods: list[str],
    consolidateds: list[str],
    forecasts: list[str] = [],
) -> float | None:
    base_condition = (
        (pl.col("concept") == concept)
        & (pl.col("segments") == "")
        & (pl.col("previous_current") != "PreviousMember")
        & (pl.col("value").is_not_null())
    )
    if forecasts:
        base_condition &= pl.col("forecast").is_in(forecasts)
    base_rows = filing_df.filter(base_condition)
    rows = base_rows
    if consolidateds:
        rows = rows.filter(pl.col("consolidated").is_in(consolidateds))
        if len(rows) == 0:
            rows = base_rows
    if periods:
        pre_period_rows = rows
        rows = rows.filter(pl.col("period").is_in(periods))
        # If no accumulated data for the quarter, fall back to the quarterly data
        if len(rows) == 0 and "CurrentYTD" in periods:
            rows = pre_period_rows.filter(pl.col("period").is_in(["CurrentQuarter"]))
    return _pick_total_value(rows)


def _pick_total_value(rows: pl.DataFrame) -> float | None:
    if rows.height == 0:
        return None
    expected_segments = (
        1
        + (pl.col("consolidated") != "").cast(pl.Int32)
        + (pl.col("forecast") != "").cast(pl.Int32)
    )
    totals = rows.filter(
        (pl.col("context_id").str.count_matches("_") + 1) == expected_segments
    )
    target = totals if totals.height > 0 else rows
    value = target["value"][0]
    return float(value) if value is not None else None


def _category_and_period_label(
    filing_df: pl.DataFrame, doc_period: str, quarter_number: int
) -> tuple[str, str]:
    fy_end = dt.date.fromisoformat(filing_df["fiscal_year_end"][0])

    doc_style = filing_df["doc_style"][0]
    if doc_style in _FORECAST_REVISION_DOC_STYLES:
        return (
            _FORECAST_REVISION_CATEGORY,
            f"{fy_end.year}年{fy_end.month}月期（予想修正）",
        )

    category = _category(doc_period, quarter_number)
    base = f"{fy_end.year}年{fy_end.month}月期"
    if doc_period == "s":
        return category, f"{base} 中間"
    if doc_period == "q":
        return category, f"{base} 第{quarter_number}四半期"
    return category, base


def _category(doc_period: str, quarter_number: int) -> str:
    if doc_period == "a":
        return "本決算"
    if doc_period == "s":
        return "中間決算"
    if doc_period == "q":
        return f"第{quarter_number}四半期決算短信"
    return "決算短信"
