"""Shared guarded-arithmetic helpers for domains/jp_stocks calculations.

Every calculation in this package returns None (rather than raising or
returning NaN/inf) when its inputs are missing or the operation is
undefined (e.g. division by zero), following the report generator's
"欠損はnull" principle - callers are responsible for turning a None into a
warning.
"""

from __future__ import annotations


def safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def safe_avg(a: float | None, b: float | None) -> float | None:
    """Average of two values, or whichever one is present if only one is."""
    if a is None:
        return b
    if b is None:
        return a
    return (a + b) / 2
