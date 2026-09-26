"""Automatic kill_criteria evaluation for review reports :
resolve a dotted metric path against the snapshot (e.g.
"survival.runway_months", "ratios.cycle_stats.operating_margin.latest") and
compare it to a threshold using one of <,<=,>,>=,==,!=. Shares its
comparison-operator implementation with rubric.py rather than reimplementing
it.
"""

from __future__ import annotations

from pydantic import BaseModel

from .rubric import OPERATORS


class KillCriterionCheck(BaseModel):
    text: str
    metric: str
    op: str
    value: float
    actual: float | None
    hit: bool | None


def resolve_metric_path(snapshot: dict, path: str) -> float | None:
    node: object = snapshot
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return (
        node if isinstance(node, (int, float)) and not isinstance(node, bool) else None
    )


def evaluate_kill_criterion(criterion: dict, snapshot: dict) -> KillCriterionCheck:
    op = criterion["op"]
    if op not in OPERATORS:
        raise ValueError(f"Unsupported kill_criteria operator: {op!r}")
    actual = resolve_metric_path(snapshot, criterion["metric"])
    hit = None if actual is None else OPERATORS[op](actual, criterion["value"])
    return KillCriterionCheck(
        text=criterion["text"],
        metric=criterion["metric"],
        op=op,
        value=criterion["value"],
        actual=actual,
        hit=hit,
    )


def evaluate_kill_criteria(criteria: list, snapshot: dict) -> list[KillCriterionCheck]:
    """文字列だけのkill_criteria(オブジェクト形式でないもの)は自動判定の対象外としてスキップする。"""
    return [
        evaluate_kill_criterion(c, snapshot)
        for c in criteria
        if isinstance(c, dict) and "metric" in c
    ]
