"""Reversal signals : narrowing losses and industry
capex/depreciation as a supply-discipline proxy.
"""

from __future__ import annotations


def loss_narrowing(current: float | None, prior: float | None) -> bool | None:
    """直近が赤字の場合にのみ、営業損益が改善したかどうかを返す。黒字ならNone。"""
    if current is None or prior is None or current >= 0:
        return None
    return current > prior


def industry_capex_to_depreciation(
    capex_values: list[float | None], depreciation_values: list[float | None]
) -> float | None:
    """同業＋自社の直近年度の設備投資合計 ÷ 減価償却費合計。"""
    has_capex = any(v is not None for v in capex_values)
    has_dep = any(v is not None for v in depreciation_values)
    if not has_capex or not has_dep:
        return None
    dep_total = sum(v for v in depreciation_values if v is not None)
    if dep_total == 0:
        return None
    capex_total = sum(v for v in capex_values if v is not None)
    return capex_total / dep_total
