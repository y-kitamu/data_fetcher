"""Valuation ratio calculations for Japanese stocks.

Pure functions only: every ratio is a guarded division over already-fetched
scalar values (price, EPS, BPS, net income, net assets, total assets). This
module has no knowledge of any particular data source's raw format (TDnet,
EDINET, Kabutan, ...) - callers are responsible for extracting those scalar
values from whichever source they use (see domains.tdnet.financial_periods
for the TDnet extraction pipeline) and passing them in here.

Designed for extension: a future report-generation feature will need
additional ratios (per_trailing, per_normalized, psr, roic, ev_ebitda) built
from TTM/normalized inputs computed by the caller. Those should be added as
new module-level functions here, reusing the same guarded-division pattern,
rather than reimplemented at each call site.
"""

from pydantic import BaseModel


class ActualForecast(BaseModel):
    """A value paired with its company-forecast counterpart.

    Not tied to any one data source's format - any disclosure system that
    distinguishes a company's own forecast from the realized actual value
    can be represented this way.
    """

    actual: float | None = None
    forecast: float | None = None


def per(price: float | None, eps: float | None) -> float | None:
    """Price-to-earnings ratio: price / EPS."""
    return price / eps if price and eps else None


def pbr(price: float | None, bps: float | None) -> float | None:
    """Price-to-book ratio: price / BPS."""
    return price / bps if price and bps else None


def roe(net_income: float | None, net_assets_actual: float | None) -> float | None:
    """Return on equity, as a percentage: net_income / net_assets * 100.

    `net_income` may be an actual or forecast figure; `net_assets_actual` is
    always the actual (disclosed) net assets, since forecast balance sheets
    are never disclosed.
    """
    if not net_assets_actual or net_income is None:
        return None
    return net_income / net_assets_actual * 100


def roa(net_income: float | None, total_assets_actual: float | None) -> float | None:
    """Return on assets, as a percentage: net_income / total_assets * 100.

    `net_income` may be an actual or forecast figure; `total_assets_actual`
    is always the actual (disclosed) total assets, since forecast balance
    sheets are never disclosed.
    """
    if not total_assets_actual or net_income is None:
        return None
    return net_income / total_assets_actual * 100


def market_cap(price: float | None, number_of_shares: float | None) -> float | None:
    """Market capitalization: price * number_of_shares."""
    return price * number_of_shares if price and number_of_shares else None


def per_forecast(market_cap_value: float | None, forecast_net_income: float | None) -> float | None:
    """Approximate PER: market_cap / company-forecast net income (section 7.3)."""
    return market_cap_value / forecast_net_income if market_cap_value and forecast_net_income else None


def per_trailing(market_cap_value: float | None, ttm_net_income: float | None) -> float | None:
    """PER (TTM): market_cap / trailing-twelve-months net income."""
    return market_cap_value / ttm_net_income if market_cap_value and ttm_net_income else None


def per_normalized(market_cap_value: float | None, normalized_net_income: float | None) -> float | None:
    """PER against normalized (cycle-median-margin, after-tax) earnings."""
    return (
        market_cap_value / normalized_net_income if market_cap_value and normalized_net_income else None
    )


def pcfr(market_cap_value: float | None, net_income: float | None, depreciation: float | None) -> float | None:
    """Price-to-cash-flow ratio: market_cap / (net_income + depreciation)."""
    if not market_cap_value or net_income is None or depreciation is None:
        return None
    denom = net_income + depreciation
    return market_cap_value / denom if denom else None


def psr(market_cap_value: float | None, revenue: float | None) -> float | None:
    """Price-to-sales ratio: market_cap / revenue."""
    return market_cap_value / revenue if market_cap_value and revenue else None


def earnings_yield(per_forecast_value: float | None) -> float | None:
    """予想収益率 = 1 / per_forecast."""
    return 1 / per_forecast_value if per_forecast_value else None


def per_x_pbr(per_forecast_value: float | None, pbr_value: float | None) -> float | None:
    if per_forecast_value is None or pbr_value is None:
        return None
    return per_forecast_value * pbr_value


def ev(
    market_cap_value: float | None,
    interest_bearing_debt: float | None,
    cash: float | None,
    securities_current: float | None,
) -> float | None:
    """Enterprise value: market_cap + interest-bearing debt - cash - current securities."""
    if market_cap_value is None or interest_bearing_debt is None or cash is None:
        return None
    return market_cap_value + interest_bearing_debt - cash - (securities_current or 0)


def ev_ebitda(ev_value: float | None, ebitda_value: float | None) -> float | None:
    if not ev_value or not ebitda_value:
        return None
    return ev_value / ebitda_value


def accruals_to_assets(
    net_income: float | None, operating_cf: float | None, total_assets: float | None
) -> float | None:
    """(net_income - operating_cf) / total_assets (直近年度)."""
    if net_income is None or operating_cf is None or not total_assets:
        return None
    return (net_income - operating_cf) / total_assets


class ValuationRatios(BaseModel):
    per: ActualForecast
    pbr: ActualForecast
    roe: ActualForecast
    roa: ActualForecast


def compute_valuation_ratios(
    price: float | None,
    eps: ActualForecast,
    bps: ActualForecast,
    net_income: ActualForecast,
    net_assets_actual: float | None,
    total_assets_actual: float | None,
) -> ValuationRatios:
    """Compute PER/PBR/ROE/ROA (actual and forecast variants) for one priced
    fiscal period.

    Convenience wrapper over per()/pbr()/roe()/roa() for the common case of
    pricing one filing's actual and forecast figures at a single point in
    time.
    """
    return ValuationRatios(
        per=ActualForecast(
            actual=per(price, eps.actual),
            forecast=per(price, eps.forecast),
        ),
        pbr=ActualForecast(
            actual=pbr(price, bps.actual),
            forecast=pbr(price, bps.forecast),
        ),
        roe=ActualForecast(
            actual=roe(net_income.actual, net_assets_actual),
            forecast=roe(net_income.forecast, net_assets_actual),
        ),
        roa=ActualForecast(
            actual=roa(net_income.actual, total_assets_actual),
            forecast=roa(net_income.forecast, total_assets_actual),
        ),
    )
