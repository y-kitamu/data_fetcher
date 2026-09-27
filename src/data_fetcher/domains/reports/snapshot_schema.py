"""Pydantic models mirroring the snapshot JSON schema (section 6 of
docs/20260925_claude_code_instructions/claude_code_instructions_A_report_generator.md
- THE authoritative schema). Field names here must match section 6 exactly:
stock-viewer's display side (a separate repository) depends on them, so they
are never renamed for Python-side convenience.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from ..jp_stocks.cost_structure import CostStructure
from ..jp_stocks.dcf import DcfResult
from ..jp_stocks.liquidation import LiquidationValue
from ..jp_stocks.normalized_value import NormalizedValue
from ..jp_stocks.ratios import CycleStats
from ..jp_stocks.survival import SurvivalMetrics
from .rubric import AutoRatingDetail
from .statements import SnapshotWarning


class SourcesInfo(BaseModel):
    last_ingested: dict[str, str] = Field(default_factory=dict)
    by_series: dict[str, dict[str, list[str]]] = Field(default_factory=dict)


class CompanyInfo(BaseModel):
    sector33_code: str | None = None
    sector33_name: str | None = None
    market: str | None = None
    fiscal_year_end_month: int | None = None
    consolidated: bool | None = None


class MarketInfo(BaseModel):
    price: float | None = None
    price_date: str | None = None
    shares_issued: float | None = None
    treasury_shares: float | None = None
    shares_outstanding: float | None = None
    market_cap: float | None = None
    peak_price: float | None = None
    peak_date: str | None = None
    drawdown_from_peak: float | None = None


class QuarterlyData(BaseModel):
    periods: list[str] = Field(default_factory=list)
    revenue: list[float | None] = Field(default_factory=list)
    operating_income: list[float | None] = Field(default_factory=list)
    net_income: list[float | None] = Field(default_factory=list)
    operating_margin: list[float | None] = Field(default_factory=list)


class SegmentData(BaseModel):
    name: str
    revenue: list[float | None] = Field(default_factory=list)
    operating_income: list[float | None] = Field(default_factory=list)


class ForecastRevision(BaseModel):
    disclosed_at: str
    field: str
    before: float | None
    after: float | None


class ForecastInfo(BaseModel):
    fiscal_year: str | None = None
    disclosed_at: str | None = None
    revenue: float | None = None
    operating_income: float | None = None
    ordinary_income: float | None = None
    net_income: float | None = None
    eps: float | None = None
    dividend_per_share: float | None = None
    revisions: list[ForecastRevision] = Field(default_factory=list)


class RatiosData(BaseModel):
    equity_ratio: list[float | None] = Field(default_factory=list)
    current_ratio: list[float | None] = Field(default_factory=list)
    net_cash: list[float | None] = Field(default_factory=list)
    debt_to_ebitda: list[float | None] = Field(default_factory=list)
    operating_margin: list[float | None] = Field(default_factory=list)
    roe: list[float | None] = Field(default_factory=list)
    roa: list[float | None] = Field(default_factory=list)
    roic: list[float | None] = Field(default_factory=list)
    revenue_growth: list[float | None] = Field(default_factory=list)
    operating_income_growth: list[float | None] = Field(default_factory=list)
    cycle_stats: dict[str, CycleStats] = Field(default_factory=dict)
    consecutive_loss_years: dict[str, int] = Field(default_factory=dict)


class ValuationMetrics(BaseModel):
    per_forecast: float | None = None
    per_trailing: float | None = None
    per_normalized: float | None = None
    pcfr: float | None = None
    psr: float | None = None
    pbr: float | None = None
    earnings_yield: float | None = None
    per_x_pbr: float | None = None
    ev: float | None = None
    ev_ebitda: float | None = None
    roic: float | None = None
    accruals_to_assets: float | None = None
    market_cap: float | None = None
    drawdown_from_peak: float | None = None


class SnapshotDcf(DcfResult):
    fcf_base_year: str | None = None


class SnapshotSurvival(SurvivalMetrics):
    """SurvivalMetrics(section 7.9の数値部分)に、T3/T22/T23/T32の開示から
    判定する定性フラグ(section 7.9後半)を足したもの。"""

    going_concern_note: bool = False
    going_concern_events: bool = False
    covenant_breach: bool = False
    commitment_line: float | None = None
    exchange_designation: str | None = None


class ValueRange(BaseModel):
    liquidation: float | None = None
    price: float | None = None
    normalized: float | None = None
    peak: float | None = None
    risk_reward: float | str | None = None


class LeadingIndicator(BaseModel):
    name: str
    unit: str | None = None
    latest: float | None = None
    yoy: float | None = None
    series_ref: str | None = None


class PeerInfo(BaseModel):
    ticker: str
    name: str | None = None
    operating_margin: list[float | None] = Field(default_factory=list)
    pbr: float | None = None
    psr: float | None = None
    margin_change_latest: float | None = None


class CycleData(BaseModel):
    peer_tickers: list[str] = Field(default_factory=list)
    peer_selection: Literal["auto", "manual"] = "auto"
    peers: list[PeerInfo] = Field(default_factory=list)
    industry_median_operating_margin: list[float | None] = Field(default_factory=list)
    peer_margin_deterioration_share: float | None = None
    industry_margin_ar1_phi: float | None = None
    industry_margin_half_life_years: float | None = None
    leading_indicators: list[LeadingIndicator] = Field(default_factory=list)


class ReversalSignals(BaseModel):
    loss_narrowing_qoq: bool | None = None
    loss_narrowing_yoy: bool | None = None
    upward_revision_last_6m: bool = False
    industry_capex_to_depreciation: float | None = None
    restructuring_last_12m: list[dict] = Field(default_factory=list)


class ShareholderTop10(BaseModel):
    name: str
    ratio: float
    as_of: str


class LargeShareholdingReport(BaseModel):
    filer: str
    ratio: float
    change_pt: float | None = None
    date: str


class CapitalAction(BaseModel):
    date: str
    type: str
    detail: str | None = None
    dilution_ratio: float | None = None
    url: str | None = None


class ShareholderReturnPolicy(BaseModel):
    disclosed_at: str | None = None
    text: str | None = None


class CapitalCostDisclosure(BaseModel):
    disclosed_at: str | None = None
    url: str | None = None


class ShareholdersData(BaseModel):
    top10: list[ShareholderTop10] = Field(default_factory=list)
    large_shareholding_reports: list[LargeShareholdingReport] = Field(
        default_factory=list
    )
    payout_ratio: list[float | None] = Field(default_factory=list)
    dividend_per_share: list[float | None] = Field(default_factory=list)
    capital_actions: list[CapitalAction] = Field(default_factory=list)
    shareholder_return_policy: ShareholderReturnPolicy = Field(
        default_factory=ShareholderReturnPolicy
    )
    capital_cost_disclosure: CapitalCostDisclosure = Field(
        default_factory=CapitalCostDisclosure
    )


class DisclosureItem(BaseModel):
    code: str
    disclosed_at: str
    title: str
    url: str | None = None


class AutoRatings(BaseModel):
    asset_value: str | None = None
    earnings_value: str | None = None
    financial_health: str | None = None
    profitability: str | None = None
    growth: str | None = None
    cyclicality: str | None = None
    survival: str | None = None
    reversal: str | None = None
    business_quality: str | None = None
    shareholder_policy: str | None = None
    details: dict[str, AutoRatingDetail] = Field(default_factory=dict)


class KillCriterionResult(BaseModel):
    text: str
    metric: str | None = None
    op: str | None = None
    value: float | None = None
    actual: float | None = None
    hit: bool | None = None


class TradeRecord(BaseModel):
    date: str
    side: Literal["buy", "sell"]
    qty: float
    price: float
    fee: float = 0.0


class TradeResultData(BaseModel):
    avg_buy_price: float | None = None
    avg_sell_price: float | None = None
    return_pct: float | None = None
    holding_days: int | None = None
    benchmark: str = "TOPIX"
    benchmark_return_pct: float | None = None
    excess_return_pct: float | None = None
    max_drawdown_pct: float | None = None


class Snapshot(BaseModel):
    schema_version: Literal[1] = 1
    ticker: str
    company_name: str | None = None
    as_of: str
    generated_at: str
    report_type: Literal["initial", "review", "exit"]
    rubric_version: int = 1
    sources: SourcesInfo = Field(default_factory=SourcesInfo)
    warnings: list[SnapshotWarning] = Field(default_factory=list)

    company: CompanyInfo = Field(default_factory=CompanyInfo)
    market: MarketInfo = Field(default_factory=MarketInfo)

    fiscal_years: list[str] = Field(default_factory=list)
    balance_sheet: dict[str, list[float | None]] = Field(default_factory=dict)
    income_statement: dict[str, list[float | None]] = Field(default_factory=dict)
    cash_flow: dict[str, list[float | None]] = Field(default_factory=dict)
    quarterly: QuarterlyData = Field(default_factory=QuarterlyData)
    segments: list[SegmentData] = Field(default_factory=list)
    forecast: ForecastInfo | None = None

    ratios: RatiosData = Field(default_factory=RatiosData)
    valuation_metrics: ValuationMetrics = Field(default_factory=ValuationMetrics)
    cost_structure: CostStructure = Field(default_factory=CostStructure)
    liquidation_value: LiquidationValue | None = None
    dcf: SnapshotDcf | None = None
    normalized_value: NormalizedValue | None = None
    value_range: ValueRange = Field(default_factory=ValueRange)
    cycle: CycleData = Field(default_factory=CycleData)
    survival: SnapshotSurvival | None = None
    reversal_signals: ReversalSignals = Field(default_factory=ReversalSignals)
    shareholders: ShareholdersData = Field(default_factory=ShareholdersData)
    disclosures: list[DisclosureItem] = Field(default_factory=list)
    auto_ratings: AutoRatings = Field(default_factory=AutoRatings)
    next_earnings_date: str | None = None

    # review / exit のときだけ
    previous_snapshot: str | None = None
    kill_criteria_check: list[KillCriterionResult] = Field(default_factory=list)
    # exit のときだけ
    trades: list[TradeRecord] = Field(default_factory=list)
    result: TradeResultData | None = None
