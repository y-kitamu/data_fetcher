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
