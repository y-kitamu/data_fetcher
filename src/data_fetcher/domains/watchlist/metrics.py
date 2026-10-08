"""Metric names usable in watchlist buy/sell conditions (docs/20261004_watchlist.md §5).

Single source of truth for `new_watch.py check --list-metrics`, the entry
validator and the generated JSON Schema's `metric` enum, so the three can
never disagree. Values are computed from daily close prices (step 2 of the
design doc); this module only declares the names.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..reports.rubric import OPERATORS


@dataclass(frozen=True)
class MetricDef:
    name: str
    description: str
    hold_only: bool = False


METRICS: dict[str, MetricDef] = {
    m.name: m
    for m in [
        MetricDef("price.close", "終値（円）"),
        MetricDef("price.vs_ma25", "25日移動平均からの乖離率（0.05 = +5%）"),
        MetricDef("price.vs_ma75", "75日移動平均からの乖離率"),
        MetricDef("price.vs_ma200", "200日移動平均からの乖離率"),
        MetricDef("price.return_1w", "1週間の騰落率"),
        MetricDef("price.return_1m", "1か月の騰落率"),
        MetricDef("price.return_3m", "3か月の騰落率"),
        MetricDef("price.drawdown_from_peak", "直近高値からの下落率（-0.15 = -15%）"),
        MetricDef("price.drawdown_from_52w_high", "52週高値からの下落率"),
        MetricDef(
            "position.return_from_cost",
            "取得単価からの損益率（trades.csv から算出。hold のみ）",
            hold_only=True,
        ),
    ]
}

OPS: list[str] = list(OPERATORS)
