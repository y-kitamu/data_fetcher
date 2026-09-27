"""Assemble a full Snapshot
for a ticker as of a given date. Orchestrates data_access.py (I/O) and the
domains/jp_stocks calculation modules; contains no direct I/O itself beyond
what data_access.py exposes, so its control flow can be exercised with fake
data_access functions in tests without touching the filesystem or network.

Deliberately incomplete relative to the full section 6 schema in a few
places that would need substantial new data-collection work beyond this
task's time budget (documented at each such spot): shareholders.top10
(needs a new EDINET 有価証券報告書 XBRL parser for the major-shareholders
table, which doesn't exist anywhere in this codebase), dividend_per_share /
payout_ratio history, forecast.revisions history, and next_earnings_date
(T7's value is in the PDF body, not the title). These are left as empty
lists / None rather than guessed.
"""

from __future__ import annotations

import datetime as dt

from ..jp_stocks import cost_structure as cost_structure_module
from ..jp_stocks import cycle as cycle_module
from ..jp_stocks import dcf as dcf_module
from ..jp_stocks import liquidation as liquidation_module
from ..jp_stocks import normalized_value as normalized_value_module
from ..jp_stocks import ratios as ratios_module
from ..jp_stocks import reversal as reversal_module
from ..jp_stocks import survival as survival_module
from ..jp_stocks import valuation as valuation_module
from ..jp_stocks.growth import peak_to_peak_revenue_cagr
from . import data_access
from . import peers as peers_module
from .kill_criteria import evaluate_kill_criteria
from .rubric import compute_auto_ratings
from .snapshot_schema import (
    AutoRatings,
    CapitalAction,
    CapitalCostDisclosure,
    CompanyInfo,
    CycleData,
    DisclosureItem,
    ForecastInfo,
    KillCriterionResult,
    LargeShareholdingReport,
    MarketInfo,
    PeerInfo,
    QuarterlyData,
    RatiosData,
    ReversalSignals,
    SegmentData,
    ShareholderReturnPolicy,
    ShareholdersData,
    Snapshot,
    SnapshotDcf,
    SnapshotSurvival,
    SourcesInfo,
    TradeRecord,
    TradeResultData,
    ValuationMetrics,
    ValueRange,
)
from .statements import (
    YEN_TO_MILLION,
    SnapshotWarning,
    build_annual_statements,
    build_quarterly_statements,
)
from .trades import compute_trade_result

TAX_RATE = 0.30
MAX_YEARS = 15
_JST = dt.timezone(dt.timedelta(hours=9))
_TOPIX_PROXY_TICKER = "1306"  # TOPIXデータが無い場合の代用(指示書§9.3)


def build_snapshot(
    ticker: str,
    as_of: dt.date,
    report_type: str,
    rubric_config: dict,
    *,
    peer_tickers: list[str] | None = None,
    leading_indicators_config: dict | None = None,
    previous_snapshot_filename: str | None = None,
    kill_criteria: list | None = None,
    trades_df=None,
    generated_at: dt.datetime | None = None,
) -> Snapshot:
    warnings: list[SnapshotWarning] = []
    generated_at = generated_at or dt.datetime.now(_JST)
    leading_indicators_config = leading_indicators_config or {}

    company_info = peers_module.get_company_info(ticker) or {}
    if not company_info:
        warnings.append(
            SnapshotWarning(
                code="TICKER_MASTER_MISSING",
                field="company",
                message=f"{ticker} が銘柄マスタ(jp_tickers.csv)に見つかりません",
            )
        )

    tdnet_periods, edinet_periods = data_access.get_statement_periods(ticker, as_of)
    annual = build_annual_statements(tdnet_periods, edinet_periods, max_years=MAX_YEARS)
    quarterly = build_quarterly_statements(tdnet_periods)
    warnings.extend(annual.warnings)

    bs_latest = _latest_row(annual.balance_sheet)
    is_latest = _latest_row(annual.income_statement)
    cf_latest = _latest_row(annual.cash_flow)

    shares_issued, treasury_shares, shares_outstanding = (
        data_access.resolve_shares_outstanding(tdnet_periods)
    )
    if shares_outstanding is None:
        warnings.append(
            SnapshotWarning(
                code="SHARES_OUTSTANDING_MISSING",
                field="market.shares_outstanding",
                message="発行済株式数(TDnetサマリー)が取得できませんでした",
            )
        )

    price, price_date = data_access.get_latest_price(ticker, as_of)
    price_history = data_access.get_price_history(ticker, as_of)
    peak_price, peak_date = data_access.compute_peak_price(price_history, as_of)
    drawdown_from_peak = (
        (price - peak_price) / peak_price if price and peak_price else None
    )
    market_cap = (
        price * shares_outstanding / 1e6 if price and shares_outstanding else None
    )

    capital_actions_raw = data_access.get_capital_actions(ticker, as_of)
    lookback_10y = as_of.replace(year=as_of.year - 10)
    if any(
        a.get("category_code") == "T17"
        and a.get("split_ratio")
        and a["disclosed_at"].date() >= lookback_10y
        for a in capital_actions_raw
    ):
        warnings.append(
            SnapshotWarning(
                code="PRICE_NOT_SPLIT_ADJUSTED",
                field="market.peak_price",
                message="対象期間に株式分割・併合があり、株価は分割調整されていません(既知の制約)",
            )
        )

    market_info = MarketInfo(
        price=price,
        price_date=price_date,
        shares_issued=shares_issued,
        treasury_shares=treasury_shares,
        shares_outstanding=shares_outstanding,
        market_cap=market_cap,
        peak_price=peak_price,
        peak_date=peak_date,
        drawdown_from_peak=drawdown_from_peak,
    )

    company = CompanyInfo(
        sector33_code=company_info.get("sector33_code"),
        sector33_name=company_info.get("sector33_name"),
        market=company_info.get("market"),
        fiscal_year_end_month=tdnet_periods[0].fiscal_year_end.month
        if tdnet_periods
        else None,
        consolidated=tdnet_periods[0].is_consolidated if tdnet_periods else None,
    )

    ratios = _build_ratios(annual)
    debt_series = ratios_module.interest_bearing_debt_series(annual.balance_sheet)
    debt_latest = debt_series[-1] if debt_series else None
    net_cash_latest = ratios.net_cash[-1] if ratios.net_cash else None
    ebitda_latest = ratios_module.ebitda(
        is_latest.get("operating_income"), cf_latest.get("depreciation")
    )

    cost_structure_data = cost_structure_module.fit_cost_structure(
        annual.income_statement.get("revenue", []),
        annual.income_statement.get("operating_income", []),
    )

    liquidation_result = liquidation_module.compute_liquidation_value(
        bs_latest, shares_outstanding
    )
    if (
        liquidation_result.value_total is not None
        and liquidation_result.value_total <= 0
    ):
        warnings.append(
            SnapshotWarning(
                code="LIQUIDATION_VALUE_NEGATIVE",
                field="liquidation_value.value_total",
                message="清算価値が0以下です",
            )
        )

    fcf_base = None
    if cf_latest.get("operating_cf") is not None and cf_latest.get("capex") is not None:
        fcf_base = cf_latest["operating_cf"] - cf_latest["capex"]
    if fcf_base is not None and fcf_base <= 0:
        warnings.append(
            SnapshotWarning(
                code="NEGATIVE_FCF",
                field="dcf",
                message="直近FCFがマイナスのためDCFはnullにしました(normalized_valueを参照)",
            )
        )
    dcf_result = dcf_module.compute_dcf(net_cash_latest, fcf_base, shares_outstanding)
    dcf_data = SnapshotDcf(
        **dcf_result.model_dump(),
        fcf_base_year=annual.fiscal_years[-1] if annual.fiscal_years else None,
    )

    depreciation_ratio_series = _ratio_series(
        annual.cash_flow.get("depreciation", []),
        annual.income_statement.get("revenue", []),
    )
    capex_ratio_series = _ratio_series(
        annual.cash_flow.get("capex", []), annual.income_statement.get("revenue", [])
    )
    normalized_result = normalized_value_module.compute_normalized_value(
        annual.income_statement.get("revenue", []),
        ratios.operating_margin,
        depreciation_ratio_series,
        capex_ratio_series,
        net_cash_latest,
        shares_outstanding,
    )

    value_range = _build_value_range(price, liquidation_result, normalized_result)

    financial_periods_summary = data_access.get_financial_periods_summary(ticker, as_of)
    # financial_periods.py (summary XBRL) reports raw yen, unlike statements.py
    # (section 6 arrays), which already converts to millions of yen.
    forecast_net_income_raw = next(
        (
            p.net_income.forecast
            for p in financial_periods_summary
            if p.net_income.forecast is not None
        ),
        None,
    )
    forecast_net_income = (
        forecast_net_income_raw / YEN_TO_MILLION
        if forecast_net_income_raw is not None
        else None
    )
    ttm_net_income = (
        sum(quarterly.net_income[-4:])
        if len(quarterly.net_income) >= 4
        and all(v is not None for v in quarterly.net_income[-4:])
        else None
    )
    valuation_metrics = _build_valuation_metrics(
        market_cap,
        forecast_net_income,
        ttm_net_income,
        normalized_result,
        is_latest,
        cf_latest,
        bs_latest,
        shares_outstanding,
        price,
        debt_latest,
        ebitda_latest,
        ratios,
        drawdown_from_peak,
    )

    disclosures = data_access.get_disclosures(ticker, as_of, lookback_months=24)
    flags_18m = [
        d for d in disclosures if (as_of - d["disclosed_at"].date()).days <= 18 * 30
    ]
    going_concern_note = any(d["category_code"] == "T3" for d in flags_18m)
    covenant_breach = any(d["category_code"] == "T23" for d in flags_18m)
    commitment_line_hit = any(d["category_code"] == "T22" for d in flags_18m)
    exchange_designation_hit = next(
        (d for d in flags_18m if d["category_code"] == "T32"), None
    )

    survival_result = survival_module.compute_survival(
        operating_cf=cf_latest.get("operating_cf"),
        cash_and_securities=_sum_present(
            [bs_latest.get("cash"), bs_latest.get("securities_current")]
        ),
        debt_due_within_1y=_debt_due_within_1y(bs_latest),
        cash=bs_latest.get("cash"),
    )
    if commitment_line_hit:
        warnings.append(
            SnapshotWarning(
                code="TDNET_VALUE_MISSING",
                field="survival.commitment_line",
                message="コミットメントライン契約の開示はあるが、金額はタイトルから取得できません(PDF本文解析は対象外)",
            )
        )
    survival = SnapshotSurvival(
        **survival_result.model_dump(),
        going_concern_note=going_concern_note,
        going_concern_events=going_concern_note,
        covenant_breach=covenant_breach,
        commitment_line=None,
        exchange_designation=exchange_designation_hit["category_label"]
        if exchange_designation_hit
        else None,
    )

    reversal_signals = ReversalSignals(
        loss_narrowing_qoq=reversal_module.loss_narrowing(
            _at(quarterly.operating_income, -1), _at(quarterly.operating_income, -2)
        ),
        loss_narrowing_yoy=reversal_module.loss_narrowing(
            _at(quarterly.operating_income, -1), _at(quarterly.operating_income, -5)
        ),
        upward_revision_last_6m=_has_upward_revision(financial_periods_summary, as_of),
        industry_capex_to_depreciation=reversal_module.industry_capex_to_depreciation(
            [cf_latest.get("capex")], [cf_latest.get("depreciation")]
        ),
        restructuring_last_12m=_restructuring_events(disclosures, as_of),
    )

    cycle_data = _build_cycle(
        ticker,
        company,
        as_of,
        ratios.operating_margin,
        annual.fiscal_years,
        peer_tickers,
        leading_indicators_config,
    )

    large_shareholding_df = data_access.get_large_shareholding(ticker, as_of)
    shareholders = ShareholdersData(
        top10=[],
        large_shareholding_reports=_build_large_shareholding_reports(
            large_shareholding_df
        ),
        payout_ratio=[],
        dividend_per_share=[],
        capital_actions=_build_capital_actions(capital_actions_raw),
        shareholder_return_policy=ShareholderReturnPolicy(),
        capital_cost_disclosure=CapitalCostDisclosure(),
    )

    disclosure_items = [
        DisclosureItem(
            code=d["category_code"],
            disclosed_at=d["disclosed_at"].isoformat(),
            title=d["title"],
            url=d.get("pdf_url"),
        )
        for d in disclosures
        if d["category_code"] is not None
    ]

    segments_data, segment_warning = _build_segments(tdnet_periods, annual.fiscal_years)
    if segment_warning:
        warnings.append(segment_warning)

    forecast_data = _build_forecast(financial_periods_summary)

    metrics = _build_rubric_metrics(
        market_cap,
        liquidation_result,
        normalized_result,
        valuation_metrics,
        ratios,
        survival,
        cycle_data,
        reversal_signals,
        annual.income_statement.get("revenue", []),
    )
    auto_ratings_result = compute_auto_ratings(rubric_config, metrics)
    if (
        liquidation_result.value_total is not None
        and liquidation_result.value_total <= 0
    ):
        auto_ratings_result.ratings["asset_value"] = "E"
    auto_ratings = AutoRatings(
        **auto_ratings_result.ratings, details=auto_ratings_result.details
    )

    last_ingested: dict[str, str] = {}
    if tdnet_periods:
        last_ingested["tdnet"] = max(p.submitted_at for p in tdnet_periods).isoformat()
    if edinet_periods:
        last_ingested["edinet"] = max(
            p.submitted_at for p in edinet_periods
        ).isoformat()

    snapshot = Snapshot(
        ticker=ticker,
        company_name=company_info.get("company_name"),
        as_of=as_of.isoformat(),
        generated_at=generated_at.isoformat(),
        report_type=report_type,
        rubric_version=rubric_config.get("rubric_version", 1),
        sources=SourcesInfo(last_ingested=last_ingested, by_series=annual.by_series),
        warnings=warnings,
        company=company,
        market=market_info,
        fiscal_years=annual.fiscal_years,
        balance_sheet=annual.balance_sheet,
        income_statement=annual.income_statement,
        cash_flow=annual.cash_flow,
        quarterly=QuarterlyData(
            periods=quarterly.periods,
            revenue=quarterly.revenue,
            operating_income=quarterly.operating_income,
            net_income=quarterly.net_income,
            operating_margin=_ratio_series(
                quarterly.operating_income, quarterly.revenue
            ),
        ),
        segments=segments_data,
        forecast=forecast_data,
        ratios=ratios,
        valuation_metrics=valuation_metrics,
        cost_structure=cost_structure_data,
        liquidation_value=liquidation_result,
        dcf=dcf_data,
        normalized_value=normalized_result,
        value_range=value_range,
        cycle=cycle_data,
        survival=survival,
        reversal_signals=reversal_signals,
        shareholders=shareholders,
        disclosures=disclosure_items,
        auto_ratings=auto_ratings,
        next_earnings_date=None,
        previous_snapshot=previous_snapshot_filename,
    )

    if kill_criteria:
        checks = evaluate_kill_criteria(kill_criteria, snapshot.model_dump())
        snapshot.kill_criteria_check = [
            KillCriterionResult(**c.model_dump()) for c in checks
        ]

    if report_type == "exit" and trades_df is not None:
        benchmark_history = data_access.get_price_history(_TOPIX_PROXY_TICKER, as_of)
        trade_result = compute_trade_result(trades_df, price_history, benchmark_history)
        snapshot.trades = [
            TradeRecord(
                date=str(r["date"]),
                side=r["side"],
                qty=r["qty"],
                price=r["price"],
                fee=r.get("fee", 0.0) or 0.0,
            )
            for r in trades_df.to_dicts()
        ]
        snapshot.result = TradeResultData(**trade_result.model_dump())

    return snapshot


def _latest_row(series_dict: dict[str, list]) -> dict:
    return {
        key: (values[-1] if values else None) for key, values in series_dict.items()
    }


def _at(values: list, index: int):
    return values[index] if -len(values) <= index < len(values) else None


def _sum_present(values: list[float | None]) -> float | None:
    present = [v for v in values if v is not None]
    return sum(present) if present else None


def _ratio_series(
    numerators: list[float | None], denominators: list[float | None]
) -> list[float | None]:
    return [
        (n / d if n is not None and d else None)
        for n, d in zip(numerators, denominators)
    ]


def _debt_due_within_1y(bs_latest: dict) -> float | None:
    return _sum_present(
        [
            bs_latest.get("short_term_debt"),
            bs_latest.get("current_portion_long_term_debt"),
            bs_latest.get("current_portion_bonds"),
        ]
    )


def _per_share(
    total_millions: float | None, shares_outstanding: float | None
) -> float | None:
    if total_millions is None or not shares_outstanding:
        return None
    return total_millions * 1e6 / shares_outstanding


def _build_ratios(annual) -> RatiosData:
    bs, is_, cf = annual.balance_sheet, annual.income_statement, annual.cash_flow
    equity_ratio = ratios_module.equity_ratio_series(
        bs["shareholders_equity"], bs["total_assets"]
    )
    current_ratio = ratios_module.current_ratio_series(
        bs["current_assets_total"], bs["current_liabilities_total"]
    )
    debt_series = ratios_module.interest_bearing_debt_series(bs)
    net_cash_series = ratios_module.net_cash_series(
        bs["cash"], bs["securities_current"], debt_series
    )
    ebitda_series = ratios_module.ebitda_series(
        is_["operating_income"], cf["depreciation"]
    )
    debt_to_ebitda_series = ratios_module.debt_to_ebitda_series(
        debt_series, ebitda_series
    )
    operating_margin = ratios_module.operating_margin_series(
        is_["operating_income"], is_["revenue"]
    )
    roe = ratios_module.roe_series(is_["net_income"], bs["shareholders_equity"])
    roa = ratios_module.roa_series(is_["net_income"], bs["total_assets"])
    roic = ratios_module.roic_series(
        is_["operating_income"], debt_series, bs["net_assets"]
    )
    revenue_growth = ratios_module.growth_series(is_["revenue"])
    operating_income_growth = ratios_module.growth_series(
        is_["operating_income"], null_if_prior_negative=True
    )

    return RatiosData(
        equity_ratio=equity_ratio,
        current_ratio=current_ratio,
        net_cash=net_cash_series,
        debt_to_ebitda=debt_to_ebitda_series,
        operating_margin=operating_margin,
        roe=roe,
        roa=roa,
        roic=roic,
        revenue_growth=revenue_growth,
        operating_income_growth=operating_income_growth,
        cycle_stats={
            "operating_margin": ratios_module.cycle_stats(operating_margin),
            "roe": ratios_module.cycle_stats(roe),
            "roic": ratios_module.cycle_stats(roic),
            "equity_ratio": ratios_module.cycle_stats(equity_ratio),
        },
        consecutive_loss_years={
            "operating": ratios_module.consecutive_loss_years(is_["operating_income"]),
            "net": ratios_module.consecutive_loss_years(is_["net_income"]),
        },
    )


def _build_value_range(price, liquidation_result, normalized_result) -> ValueRange:
    value_range = ValueRange(
        liquidation=liquidation_result.value_per_share,
        price=price,
        normalized=normalized_result.value_per_share,
        peak=normalized_result.peak_value_per_share,
    )
    liquidation, normalized = value_range.liquidation, value_range.normalized
    if price is not None and liquidation is not None and price <= liquidation:
        value_range.risk_reward = "price_below_liquidation"
    elif (
        price is not None
        and normalized is not None
        and liquidation is not None
        and price > liquidation
    ):
        value_range.risk_reward = (normalized - price) / (price - liquidation)
    return value_range


def _build_valuation_metrics(
    market_cap,
    forecast_net_income,
    ttm_net_income,
    normalized_result,
    is_latest,
    cf_latest,
    bs_latest,
    shares_outstanding,
    price,
    debt_latest,
    ebitda_latest,
    ratios,
    drawdown_from_peak,
) -> ValuationMetrics:
    per_forecast_value = valuation_module.per_forecast(market_cap, forecast_net_income)
    bps = _per_share(bs_latest.get("shareholders_equity"), shares_outstanding)
    pbr_value = valuation_module.pbr(price, bps)
    ev_value = valuation_module.ev(
        market_cap,
        debt_latest,
        bs_latest.get("cash"),
        bs_latest.get("securities_current"),
    )
    normalized_after_tax_income = (
        normalized_result.normalized_operating_income * (1 - TAX_RATE)
        if normalized_result.normalized_operating_income is not None
        else None
    )

    return ValuationMetrics(
        per_forecast=per_forecast_value,
        per_trailing=valuation_module.per_trailing(market_cap, ttm_net_income),
        per_normalized=valuation_module.per_normalized(
            market_cap, normalized_after_tax_income
        ),
        pcfr=valuation_module.pcfr(
            market_cap, is_latest.get("net_income"), cf_latest.get("depreciation")
        ),
        psr=valuation_module.psr(market_cap, is_latest.get("revenue")),
        pbr=pbr_value,
        earnings_yield=valuation_module.earnings_yield(per_forecast_value),
        per_x_pbr=valuation_module.per_x_pbr(per_forecast_value, pbr_value),
        ev=ev_value,
        ev_ebitda=valuation_module.ev_ebitda(ev_value, ebitda_latest),
        roic=ratios.roic[-1] if ratios.roic else None,
        accruals_to_assets=valuation_module.accruals_to_assets(
            is_latest.get("net_income"),
            cf_latest.get("operating_cf"),
            bs_latest.get("total_assets"),
        ),
        market_cap=market_cap,
        drawdown_from_peak=drawdown_from_peak,
    )


def _has_upward_revision(financial_periods, as_of: dt.date) -> bool:
    cutoff = as_of - dt.timedelta(days=182)
    ordered = sorted(financial_periods, key=lambda p: p.submitted_at)
    for i, period in enumerate(ordered):
        if (
            not period.is_forecast_revision
            or period.submitted_at.date() < cutoff
            or i == 0
        ):
            continue
        prior = ordered[i - 1]
        after, before = (
            period.operating_income.forecast,
            prior.operating_income.forecast,
        )
        if after is not None and before is not None and after > before:
            return True
    return False


def _restructuring_events(disclosures: list[dict], as_of: dt.date) -> list[dict]:
    cutoff = as_of - dt.timedelta(days=365)
    return [
        {
            "code": d["category_code"],
            "disclosed_at": d["disclosed_at"].isoformat(),
            "title": d["title"],
            "url": d.get("pdf_url"),
        }
        for d in disclosures
        if d["category_code"] == "T25" and d["disclosed_at"].date() >= cutoff
    ]


def _build_large_shareholding_reports(df) -> list[LargeShareholdingReport]:
    if df.height == 0:
        return []
    reports = []
    for row in df.sort("filing_date", descending=True).head(20).to_dicts():
        ratio = row.get("holding_ratio")
        if ratio is None:
            continue
        prior = row.get("holding_ratio_prev")
        reports.append(
            LargeShareholdingReport(
                filer=row.get("holder_name") or "",
                ratio=ratio,
                change_pt=(ratio - prior) if prior is not None else None,
                date=str(row.get("filing_date")),
            )
        )
    return reports


def _build_capital_actions(capital_actions_raw: list[dict]) -> list[CapitalAction]:
    return [
        CapitalAction(
            date=a["disclosed_at"].date().isoformat(),
            type=a["type"],
            detail=a.get("title"),
            dilution_ratio=None,
            url=a.get("url"),
        )
        for a in capital_actions_raw
    ]


def _build_segments(
    tdnet_periods, fiscal_years: list[str]
) -> tuple[list[SegmentData], SnapshotWarning | None]:
    if not tdnet_periods:
        return [], None
    latest = next((p for p in tdnet_periods if p.quarter_number == 0), tdnet_periods[0])
    if not latest.segments:
        return [], None
    n = len(fiscal_years)
    segments_data = [
        SegmentData(
            name=seg.member,
            revenue=[None] * (n - 1) + [_to_millions(seg.revenue)] if n else [],
            operating_income=[None] * (n - 1) + [_to_millions(seg.operating_income)]
            if n
            else [],
        )
        for seg in latest.segments
    ]
    warning = SnapshotWarning(
        code="MISSING_SEGMENT_LABEL",
        field="segments",
        message="セグメント名は正式なラベルではなくXBRLメンバーIDから機械的に生成した暫定名です",
    )
    return segments_data, warning


def _to_millions(value: float | None) -> float | None:
    return value / YEN_TO_MILLION if value is not None else None


def _build_forecast(financial_periods_summary) -> ForecastInfo | None:
    """financial_periods.py(サマリーXBRL)は生の円。集計額(revenue等)は
    百万円に変換するが、1株あたりの値(eps/dividend_per_share)は円のまま。
    """
    latest = next(
        (p for p in financial_periods_summary if p.net_income.forecast is not None),
        None,
    )
    if latest is None:
        return None
    return ForecastInfo(
        disclosed_at=latest.submitted_at.isoformat(),
        revenue=_to_millions(latest.total_revenue.forecast),
        operating_income=_to_millions(latest.operating_income.forecast),
        ordinary_income=_to_millions(latest.ordinary_profit.forecast),
        net_income=_to_millions(latest.net_income.forecast),
        eps=latest.eps.forecast,
        dividend_per_share=latest.dividend.forecast,
    )


def _align_series(
    source_years: list[str], source_values: list, target_years: list[str]
) -> list:
    lookup = dict(zip(source_years, source_values))
    return [lookup.get(year) for year in target_years]


def _build_cycle(
    ticker: str,
    company: CompanyInfo,
    as_of: dt.date,
    own_operating_margin: list[float | None],
    fiscal_years: list[str],
    peer_tickers_arg: list[str] | None,
    leading_indicators_config: dict,
) -> CycleData:
    peer_selection = "manual" if peer_tickers_arg else "auto"
    selected: list[str] = []
    if peer_tickers_arg:
        selected = peer_tickers_arg
    elif company.sector33_code:
        sector_tickers = [
            t
            for t in peers_module.tickers_in_sector(company.sector33_code)
            if t != ticker
        ]
        market_caps = data_access.get_sector_market_caps(
            company.sector33_code, as_of, sector_tickers
        )
        selected = peers_module.select_peers_by_market_cap(ticker, market_caps)

    peer_infos: list[PeerInfo] = []
    margin_series_by_ticker = [own_operating_margin]
    for peer_ticker in selected:
        peer_tdnet, peer_edinet = data_access.get_statement_periods(peer_ticker, as_of)
        peer_annual = build_annual_statements(
            peer_tdnet, peer_edinet, max_years=MAX_YEARS
        )
        peer_margin_full = ratios_module.operating_margin_series(
            peer_annual.income_statement.get("operating_income", []),
            peer_annual.income_statement.get("revenue", []),
        )
        aligned = _align_series(
            peer_annual.fiscal_years, peer_margin_full, fiscal_years
        )
        margin_series_by_ticker.append(aligned)
        peer_infos.append(PeerInfo(ticker=peer_ticker, operating_margin=aligned))

    industry_median = cycle_module.industry_median_series(margin_series_by_ticker)
    ar1 = cycle_module.ar1_half_life(industry_median)

    peer_latest = [
        p.operating_margin[-1] if p.operating_margin else None for p in peer_infos
    ]
    peer_prior = [
        p.operating_margin[-2] if len(p.operating_margin) >= 2 else None
        for p in peer_infos
    ]
    peer_median_10y = [
        ratios_module.cycle_stats(p.operating_margin).median_10y for p in peer_infos
    ]
    deterioration_share = cycle_module.peer_margin_deterioration_share(
        peer_latest, peer_prior, peer_median_10y
    )

    leading_indicators = [
        {
            "name": item["name"],
            "unit": item.get("unit"),
            "latest": None,
            "yoy": None,
            "series_ref": None,
        }
        for item in leading_indicators_config.get(company.sector33_code, [])
    ]

    return CycleData(
        peer_tickers=selected,
        peer_selection=peer_selection,
        peers=peer_infos,
        industry_median_operating_margin=industry_median,
        peer_margin_deterioration_share=deterioration_share,
        industry_margin_ar1_phi=ar1.phi,
        industry_margin_half_life_years=ar1.half_life_years,
        leading_indicators=leading_indicators,
    )


def _build_rubric_metrics(
    market_cap,
    liquidation_result,
    normalized_result,
    valuation_metrics,
    ratios,
    survival,
    cycle_data,
    reversal_signals,
    revenue_series,
) -> dict:
    market_cap_to_liquidation = None
    if (
        market_cap is not None
        and liquidation_result.value_total
    ):
        # 清算価値が負なら負の比率になり、rubricのnonpositive: Eで評価される
        market_cap_to_liquidation = market_cap / liquidation_result.value_total

    return {
        "market_cap_to_liquidation": market_cap_to_liquidation,
        "per_normalized": valuation_metrics.per_normalized,
        "median_operating_margin": normalized_result.median_operating_margin,
        "peak_to_peak_revenue_cagr": peak_to_peak_revenue_cagr(revenue_series),
        "equity_ratio": ratios.equity_ratio[-1] if ratios.equity_ratio else None,
        "net_cash_to_mcap": (
            ratios.net_cash[-1] / market_cap
            if ratios.net_cash and ratios.net_cash[-1] is not None and market_cap
            else None
        ),
        "runway_months": survival.runway_months,
        "ocf_positive": survival.ocf_positive,
        "going_concern_note": survival.going_concern_note,
        "exchange_designation": survival.exchange_designation is not None,
        "covenant_breach": survival.covenant_breach,
        "debt_due_to_cash": survival.debt_due_to_cash,
        "peer_margin_deterioration_share": cycle_data.peer_margin_deterioration_share,
        "industry_margin_half_life_years": cycle_data.industry_margin_half_life_years,
        "loss_narrowing_qoq": reversal_signals.loss_narrowing_qoq,
        "loss_narrowing_yoy": reversal_signals.loss_narrowing_yoy,
        "upward_revision_last_6m": reversal_signals.upward_revision_last_6m,
        "industry_capex_to_depreciation": reversal_signals.industry_capex_to_depreciation,
    }
