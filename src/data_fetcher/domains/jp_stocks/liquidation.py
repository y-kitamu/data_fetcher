"""Liquidation value :
adjusted assets (book value x haircut per line item) minus total
liabilities.
"""

from __future__ import annotations

from pydantic import BaseModel

DEFAULT_HAIRCUTS: dict[str, float] = {
    "cash": 1.0,
    "receivables": 0.85,
    "securities_current": 1.0,
    "inventory": 0.5,
    "current_assets_other": 0.0,
    "ppe": 0.5,
    "intangibles": 0.0,
    "investments": 0.5,
}

# 清算価値の掛け目対象科目。"investments"だけはinvestment_securities+investments_otherの合算。
_DIRECT_ASSET_KEYS = [
    "cash",
    "receivables",
    "securities_current",
    "inventory",
    "current_assets_other",
    "ppe",
    "intangibles",
]


class LiquidationComponent(BaseModel):
    book: float
    haircut: float
    adjusted: float


class LiquidationValue(BaseModel):
    haircuts: dict[str, float]
    components: dict[str, LiquidationComponent]
    adjusted_assets: float | None
    total_liabilities: float | None
    value_total: float | None
    value_per_share: float | None


def compute_liquidation_value(
    balance_sheet: dict[str, float | None],
    shares_outstanding: float | None,
    haircuts: dict[str, float] | None = None,
) -> LiquidationValue:
    haircuts = haircuts or DEFAULT_HAIRCUTS
    components: dict[str, LiquidationComponent] = {}
    adjusted_total = 0.0
    any_asset = False

    for key in _DIRECT_ASSET_KEYS:
        book = balance_sheet.get(key)
        if book is None:
            continue
        any_asset = True
        haircut = haircuts.get(key, 0.0)
        adjusted = book * haircut
        components[key] = LiquidationComponent(
            book=book, haircut=haircut, adjusted=adjusted
        )
        adjusted_total += adjusted

    investment_securities = balance_sheet.get("investment_securities")
    investments_other = balance_sheet.get("investments_other")
    if investment_securities is not None or investments_other is not None:
        any_asset = True
        investments_book = (investment_securities or 0) + (investments_other or 0)
        haircut = haircuts.get("investments", 0.0)
        adjusted = investments_book * haircut
        components["investments"] = LiquidationComponent(
            book=investments_book, haircut=haircut, adjusted=adjusted
        )
        adjusted_total += adjusted

    total_liabilities = balance_sheet.get("total_liabilities")
    if not any_asset or total_liabilities is None:
        return LiquidationValue(
            haircuts=haircuts,
            components=components,
            adjusted_assets=None,
            total_liabilities=total_liabilities,
            value_total=None,
            value_per_share=None,
        )

    value_total = adjusted_total - total_liabilities
    value_per_share = (
        value_total * 1e6 / shares_outstanding if shares_outstanding else None
    )
    return LiquidationValue(
        haircuts=haircuts,
        components=components,
        adjusted_assets=adjusted_total,
        total_liabilities=total_liabilities,
        value_total=value_total,
        value_per_share=value_per_share,
    )
