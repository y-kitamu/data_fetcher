"""Cycle indicators across peers : the industry median
operating-margin series, its AR(1) mean-reversion half-life, and the share
of peers whose margin is currently deteriorating both year-over-year and
versus their own 10-year median. Peer *selection* lives in peers.py; this
module only aggregates already-collected margin series.
"""

from __future__ import annotations

import math

import numpy as np
from pydantic import BaseModel

_MIN_YEARS_FOR_AR1 = 10


class Ar1Result(BaseModel):
    phi: float | None
    half_life_years: float | None


def industry_median_series(
    margin_series_by_ticker: list[list[float | None]],
) -> list[float | None]:
    """自社を含む各社の年次営業利益率シリーズ(全て同じ長さ)から年ごとの中央値を計算する。"""
    if not margin_series_by_ticker:
        return []
    length = len(margin_series_by_ticker[0])
    result: list[float | None] = []
    for i in range(length):
        values = [
            series[i] for series in margin_series_by_ticker if series[i] is not None
        ]
        result.append(float(np.median(values)) if values else None)
    return result


def ar1_half_life(
    series: list[float | None], *, min_years: int = _MIN_YEARS_FOR_AR1
) -> Ar1Result:
    """(m_t - mu) = phi*(m_{t-1} - mu) + eps を最小二乗推定し、
    0 < phi < 1 のときだけ半減期 ln(0.5)/ln(phi) を返す。
    """
    values = [v for v in series if v is not None]
    if len(values) < min_years:
        return Ar1Result(phi=None, half_life_years=None)

    arr = np.array(values, dtype=float)
    mu = arr.mean()
    x = arr[:-1] - mu
    y = arr[1:] - mu
    denom = float(np.sum(x * x))
    if denom == 0:
        return Ar1Result(phi=None, half_life_years=None)
    phi = float(np.sum(x * y) / denom)

    if not (0 < phi < 1):
        return Ar1Result(phi=phi, half_life_years=None)
    return Ar1Result(phi=phi, half_life_years=math.log(0.5) / math.log(phi))


def peer_margin_deterioration_share(
    peer_latest_margin: list[float | None],
    peer_prior_margin: list[float | None],
    peer_median_10y: list[float | None],
) -> float | None:
    """最新期の営業利益率が前期未満、かつ自社(各社)の10年中央値未満を
    満たす同業の割合。判定できる同業が1社もいなければnull。
    """
    flags: list[bool] = []
    for latest, prior, median in zip(
        peer_latest_margin, peer_prior_margin, peer_median_10y
    ):
        if latest is None or prior is None or median is None:
            continue
        flags.append(latest < prior and latest < median)
    return sum(flags) / len(flags) if flags else None
