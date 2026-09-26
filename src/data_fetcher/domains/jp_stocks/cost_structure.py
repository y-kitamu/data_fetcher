"""Break-even analysis via a linear regression of operating expenses on
revenue : operating_expenses = fixed_cost + variable_cost_ratio
* revenue, fit by least squares over the last 10 years (minimum 6).
"""

from __future__ import annotations

import numpy as np
from pydantic import BaseModel

_MIN_YEARS = 6
_WINDOW_YEARS = 10


class CostStructure(BaseModel):
    fixed_cost: float | None
    variable_cost_ratio: float | None
    r2: float | None
    n_years: int
    breakeven_revenue: float | None
    latest_revenue: float | None
    gap_to_breakeven_pct: float | None


def fit_cost_structure(
    revenue: list[float | None], operating_income: list[float | None]
) -> CostStructure:
    windowed_revenue = revenue[-_WINDOW_YEARS:]
    windowed_operating_income = operating_income[-_WINDOW_YEARS:]
    pairs = [
        (r, r - oi)
        for r, oi in zip(windowed_revenue, windowed_operating_income)
        if r is not None and oi is not None
    ]
    latest_revenue = next((r for r in reversed(revenue) if r is not None), None)

    if len(pairs) < _MIN_YEARS:
        return CostStructure(
            fixed_cost=None,
            variable_cost_ratio=None,
            r2=None,
            n_years=len(pairs),
            breakeven_revenue=None,
            latest_revenue=latest_revenue,
            gap_to_breakeven_pct=None,
        )

    xs = np.array([p[0] for p in pairs], dtype=float)
    ys = np.array([p[1] for p in pairs], dtype=float)
    variable_cost_ratio, fixed_cost = np.polyfit(xs, ys, 1)
    predicted = variable_cost_ratio * xs + fixed_cost
    ss_res = float(np.sum((ys - predicted) ** 2))
    ss_tot = float(np.sum((ys - ys.mean()) ** 2))
    r2 = 1 - ss_res / ss_tot if ss_tot else None

    breakeven_revenue = (
        fixed_cost / (1 - variable_cost_ratio) if variable_cost_ratio < 1 else None
    )
    gap_to_breakeven_pct = (
        (breakeven_revenue - latest_revenue) / latest_revenue
        if breakeven_revenue is not None and latest_revenue
        else None
    )

    return CostStructure(
        fixed_cost=float(fixed_cost),
        variable_cost_ratio=float(variable_cost_ratio),
        r2=r2,
        n_years=len(pairs),
        breakeven_revenue=breakeven_revenue,
        latest_revenue=latest_revenue,
        gap_to_breakeven_pct=gap_to_breakeven_pct,
    )
