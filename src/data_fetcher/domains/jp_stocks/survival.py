"""Survival metrics : months of cash runway if operating cash
flow is negative, and near-term debt coverage.
"""

from __future__ import annotations

from pydantic import BaseModel


class SurvivalMetrics(BaseModel):
    runway_months: float | None
    runway_basis: str
    ocf_positive: bool | None
    debt_due_within_1y: float | None
    debt_due_to_cash: float | None


def compute_survival(
    operating_cf: float | None,
    cash_and_securities: float | None,
    debt_due_within_1y: float | None,
    cash: float | None,
    runway_basis: str = "annual_ocf",
) -> SurvivalMetrics:
    ocf_positive = operating_cf >= 0 if operating_cf is not None else None

    runway_months: float | None = None
    if (
        operating_cf is not None
        and operating_cf < 0
        and cash_and_securities is not None
    ):
        monthly_burn = -operating_cf / 12
        runway_months = cash_and_securities / monthly_burn if monthly_burn else None

    debt_due_to_cash = (
        debt_due_within_1y / cash if debt_due_within_1y is not None and cash else None
    )

    return SurvivalMetrics(
        runway_months=runway_months,
        runway_basis=runway_basis,
        ocf_positive=ocf_positive,
        debt_due_within_1y=debt_due_within_1y,
        debt_due_to_cash=debt_due_to_cash,
    )
