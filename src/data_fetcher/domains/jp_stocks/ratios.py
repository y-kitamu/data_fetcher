"""Time-series financial ratios computed across a company's annual history.
Series functions take and return lists the same length as the assembled
fiscal_years array; a None at a given index propagates to a None result at that index.
"""

from __future__ import annotations

import numpy as np
from pydantic import BaseModel

from ._guard import safe_avg, safe_div

TAX_RATE = 0.30


def equity_ratio_series(
    shareholders_equity: list[float | None], total_assets: list[float | None]
) -> list[float | None]:
    return [safe_div(e, a) for e, a in zip(shareholders_equity, total_assets)]


def current_ratio_series(
    current_assets: list[float | None], current_liabilities: list[float | None]
) -> list[float | None]:
    return [safe_div(a, liab) for a, liab in zip(current_assets, current_liabilities)]


def interest_bearing_debt(
    short_term_debt: float | None,
    current_portion_long_term_debt: float | None,
    current_portion_bonds: float | None,
    long_term_debt: float | None,
    bonds: float | None,
    lease_obligations: float | None,
) -> float | None:
    parts = [
        short_term_debt,
        current_portion_long_term_debt,
        current_portion_bonds,
        long_term_debt,
        bonds,
        lease_obligations,
    ]
    present = [p for p in parts if p is not None]
    return sum(present) if present else None


def interest_bearing_debt_series(
    balance_sheet: dict[str, list[float | None]],
) -> list[float | None]:
    n = len(balance_sheet["total_assets"])
    return [
        interest_bearing_debt(
            balance_sheet["short_term_debt"][i],
            balance_sheet["current_portion_long_term_debt"][i],
            balance_sheet["current_portion_bonds"][i],
            balance_sheet["long_term_debt"][i],
            balance_sheet["bonds"][i],
            balance_sheet["lease_obligations"][i],
        )
        for i in range(n)
    ]


def net_cash(
    cash: float | None, securities_current: float | None, debt: float | None
) -> float | None:
    if cash is None or debt is None:
        return None
    return cash + (securities_current or 0) - debt


def net_cash_series(
    cash: list[float | None],
    securities_current: list[float | None],
    debt: list[float | None],
) -> list[float | None]:
    return [net_cash(c, s, d) for c, s, d in zip(cash, securities_current, debt)]


def ebitda(operating_income: float | None, depreciation: float | None) -> float | None:
    if operating_income is None or depreciation is None:
        return None
    return operating_income + depreciation


def ebitda_series(
    operating_income: list[float | None], depreciation: list[float | None]
) -> list[float | None]:
    return [ebitda(oi, dep) for oi, dep in zip(operating_income, depreciation)]


def debt_to_ebitda(debt: float | None, ebitda_value: float | None) -> float | None:
    if debt is None or ebitda_value is None or ebitda_value <= 0:
        return None
    return debt / ebitda_value


def debt_to_ebitda_series(
    debt: list[float | None], ebitda_values: list[float | None]
) -> list[float | None]:
    return [debt_to_ebitda(d, e) for d, e in zip(debt, ebitda_values)]


def operating_margin_series(
    operating_income: list[float | None], revenue: list[float | None]
) -> list[float | None]:
    return [safe_div(oi, r) for oi, r in zip(operating_income, revenue)]


def roe_series(
    net_income: list[float | None], shareholders_equity: list[float | None]
) -> list[float | None]:
    result: list[float | None] = []
    for i, income in enumerate(net_income):
        prior_equity = shareholders_equity[i - 1] if i > 0 else None
        avg_equity = safe_avg(prior_equity, shareholders_equity[i])
        result.append(safe_div(income, avg_equity))
    return result


def roa_series(
    net_income: list[float | None], total_assets: list[float | None]
) -> list[float | None]:
    result: list[float | None] = []
    for i, income in enumerate(net_income):
        prior_assets = total_assets[i - 1] if i > 0 else None
        avg_assets = safe_avg(prior_assets, total_assets[i])
        result.append(safe_div(income, avg_assets))
    return result


def roic(
    operating_income: float | None,
    debt: float | None,
    net_assets: float | None,
    tax_rate: float = TAX_RATE,
) -> float | None:
    if operating_income is None or debt is None or net_assets is None:
        return None
    denom = debt + net_assets
    return safe_div(operating_income * (1 - tax_rate), denom)


def roic_series(
    operating_income: list[float | None],
    debt: list[float | None],
    net_assets: list[float | None],
    tax_rate: float = TAX_RATE,
) -> list[float | None]:
    return [
        roic(oi, d, na, tax_rate)
        for oi, d, na in zip(operating_income, debt, net_assets)
    ]


def growth_series(
    values: list[float | None], *, null_if_prior_negative: bool = False
) -> list[float | None]:
    """前年比の成長率シリーズ。先頭は前年が無いため常にnull。"""
    result: list[float | None] = [None] if values else []
    for i in range(1, len(values)):
        prev, cur = values[i - 1], values[i]
        if prev is None or cur is None or prev == 0:
            result.append(None)
        elif null_if_prior_negative and prev < 0:
            result.append(None)
        else:
            result.append((cur - prev) / prev)
    return result


class CycleStats(BaseModel):
    median_10y: float | None
    latest: float | None
    percentile_latest: float | None


def cycle_stats(
    series: list[float | None], *, median_years: int = 10, percentile_years: int = 15
) -> CycleStats:
    """直近median_years年の中央値、最新値、直近percentile_years年分布内での最新値のパーセンタイル。"""
    recent_for_median = [v for v in series[-median_years:] if v is not None]
    median_10y = float(np.median(recent_for_median)) if recent_for_median else None

    latest = next((v for v in reversed(series) if v is not None), None)

    window = [v for v in series[-percentile_years:] if v is not None]
    if latest is None or len(window) < 2:
        percentile_latest = None
    else:
        rank = sum(1 for v in window if v <= latest)
        percentile_latest = rank / len(window)

    return CycleStats(
        median_10y=median_10y, latest=latest, percentile_latest=percentile_latest
    )


def consecutive_loss_years(values: list[float | None]) -> int:
    """最新期から遡って値が負であり続けた期数(nullが出た時点で打ち切り)。"""
    count = 0
    for v in reversed(values):
        if v is None:
            break
        if v < 0:
            count += 1
        else:
            break
    return count
