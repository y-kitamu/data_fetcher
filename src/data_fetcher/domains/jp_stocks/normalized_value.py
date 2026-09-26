"""Normalized earnings value for cyclical stocks : latest
revenue times the 10-year median operating margin, tax- and
capex/depreciation-adjusted into a normalized FCF, then valued the same way
as the DCF bear case (net_cash + fcf / R). A "peak" variant uses the highest
operating margin of the last 15 years instead of the median.
"""

from __future__ import annotations

import numpy as np
from pydantic import BaseModel

TAX_RATE = 0.30
_MEDIAN_YEARS = 10
_PEAK_YEARS = 15


class NormalizedValue(BaseModel):
    method: str
    median_operating_margin: float | None
    normalized_operating_income: float | None
    tax_rate: float
    normalized_fcf: float | None
    value_per_share: float | None
    peak_operating_margin: float | None
    peak_fcf: float | None
    peak_value_per_share: float | None


def compute_normalized_value(
    revenue: list[float | None],
    operating_margin: list[float | None],
    depreciation_ratio: list[float | None],
    capex_ratio: list[float | None],
    net_cash: float | None,
    shares_outstanding: float | None,
    discount_rate: float = 0.10,
    tax_rate: float = TAX_RATE,
) -> NormalizedValue:
    latest_revenue = next((r for r in reversed(revenue) if r is not None), None)
    median_margin = _median(
        [m for m in operating_margin[-_MEDIAN_YEARS:] if m is not None]
    )
    peak_margin = max(
        (m for m in operating_margin[-_PEAK_YEARS:] if m is not None), default=None
    )
    median_dep_ratio = _median(
        [d for d in depreciation_ratio[-_MEDIAN_YEARS:] if d is not None]
    )
    median_capex_ratio = _median(
        [c for c in capex_ratio[-_MEDIAN_YEARS:] if c is not None]
    )

    def value_for(
        margin: float | None,
    ) -> tuple[float | None, float | None, float | None]:
        if latest_revenue is None or margin is None:
            return None, None, None
        operating_income = latest_revenue * margin
        fcf = operating_income * (1 - tax_rate)
        if median_dep_ratio is not None and median_capex_ratio is not None:
            fcf += (median_dep_ratio - median_capex_ratio) * latest_revenue
        if net_cash is None:
            return operating_income, fcf, None
        value_total = net_cash + fcf / discount_rate
        value_per_share = (
            value_total * 1e6 / shares_outstanding if shares_outstanding else None
        )
        return operating_income, fcf, value_per_share

    normalized_operating_income, normalized_fcf, normalized_value_per_share = value_for(
        median_margin
    )
    _peak_operating_income, peak_fcf, peak_value_per_share = value_for(peak_margin)

    return NormalizedValue(
        method="latest_revenue_x_median_margin_10y",
        median_operating_margin=median_margin,
        normalized_operating_income=normalized_operating_income,
        tax_rate=tax_rate,
        normalized_fcf=normalized_fcf,
        value_per_share=normalized_value_per_share,
        peak_operating_margin=peak_margin,
        peak_fcf=peak_fcf,
        peak_value_per_share=peak_value_per_share,
    )


def _median(values: list[float]) -> float | None:
    return float(np.median(values)) if values else None
