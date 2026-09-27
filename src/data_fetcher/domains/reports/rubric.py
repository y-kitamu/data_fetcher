"""Safe, minimal condition evaluator for rubric_v1.yaml  and
the auto-rating grades it drives.

Deliberately not `eval`: only identifiers, numbers, the six comparison
operators and and/or are recognized (a hand-written recursive-descent parser
over a token list), so a condition string from a YAML config can never
execute arbitrary code. Missing metrics evaluate to None, and Kleene
three-valued logic propagates that through and/or (`None and True -> None`,
`None or True -> True`, `None or False -> None`), matching the report
generator's treatment of missing data as "unknown", not "false".

kill_criteria.py (section 9.4) reuses OPERATORS rather than reimplementing
comparison logic.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Union

from pydantic import BaseModel
from ruamel.yaml import YAML

OPERATORS: dict[str, Callable[[float, float], bool]] = {
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}

_TOKEN_RE = re.compile(r"<=|>=|==|!=|<|>|\(|\)|-?\d+(?:\.\d+)?|[A-Za-z_][A-Za-z0-9_.]*")
_GRADES = ["A", "B", "C", "D", "E"]


class ConditionSyntaxError(ValueError):
    pass


@dataclass(frozen=True)
class _Comparison:
    ident: str
    op: str | None
    value: float | None


@dataclass(frozen=True)
class _BoolExpr:
    kind: str  # "and" | "or"
    terms: tuple["_Comparison | _BoolExpr", ...]


_ConditionNode = Union[_Comparison, _BoolExpr]


def _tokenize(expr: str) -> list[str]:
    tokens = _TOKEN_RE.findall(expr)
    if "".join(tokens) != re.sub(r"\s+", "", expr):
        raise ConditionSyntaxError(f"Unrecognized token in condition: {expr!r}")
    return tokens


class _Parser:
    def __init__(self, tokens: list[str]) -> None:
        self.tokens = tokens
        self.pos = 0

    def peek(self) -> str | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def advance(self) -> str:
        token = self.peek()
        if token is None:
            raise ConditionSyntaxError("Unexpected end of condition")
        self.pos += 1
        return token

    def parse(self) -> _ConditionNode:
        expr = self._or_expr()
        if self.pos != len(self.tokens):
            raise ConditionSyntaxError(
                f"Unexpected trailing tokens: {self.tokens[self.pos :]}"
            )
        return expr

    def _or_expr(self) -> _ConditionNode:
        terms = [self._and_expr()]
        while self.peek() == "or":
            self.advance()
            terms.append(self._and_expr())
        return terms[0] if len(terms) == 1 else _BoolExpr(kind="or", terms=tuple(terms))

    def _and_expr(self) -> _ConditionNode:
        terms = [self._term()]
        while self.peek() == "and":
            self.advance()
            terms.append(self._term())
        return (
            terms[0] if len(terms) == 1 else _BoolExpr(kind="and", terms=tuple(terms))
        )

    def _term(self) -> _ConditionNode:
        if self.peek() == "(":
            self.advance()
            expr = self._or_expr()
            if self.advance() != ")":
                raise ConditionSyntaxError("Expected ')'")
            return expr
        ident = self.advance()
        if not re.match(r"^[A-Za-z_]", ident):
            raise ConditionSyntaxError(f"Expected identifier, got {ident!r}")
        if self.peek() in OPERATORS:
            op = self.advance()
            value_token = self.advance()
            try:
                value = float(value_token)
            except ValueError as exc:
                raise ConditionSyntaxError(
                    f"Expected number after operator, got {value_token!r}"
                ) from exc
            return _Comparison(ident=ident, op=op, value=value)
        return _Comparison(ident=ident, op=None, value=None)


def parse_condition(expr: str) -> _ConditionNode:
    tokens = _tokenize(expr)
    if not tokens:
        raise ConditionSyntaxError("Empty condition")
    return _Parser(tokens).parse()


def evaluate_condition(node: _ConditionNode, metrics: dict) -> bool | None:
    if isinstance(node, _Comparison):
        value = metrics.get(node.ident)
        if node.op is None:
            return bool(value) if value is not None else None
        if value is None:
            return None
        return OPERATORS[node.op](value, node.value)

    results = [evaluate_condition(term, metrics) for term in node.terms]
    if node.kind == "and":
        if any(r is False for r in results):
            return False
        return None if any(r is None for r in results) else True
    if any(r is True for r in results):
        return True
    return None if any(r is None for r in results) else False


def evaluate_bins(
    value: float | None,
    bins: list[float],
    order: str,
    nonpositive: str | None = None,
) -> str | None:
    """Grade value against bins. nonpositive, if given, is returned for
    value <= 0 before the bins are consulted: for ratios like PER a negative
    value means a loss, not "cheaper than any threshold".
    """
    if value is None:
        return None
    if nonpositive is not None and value <= 0:
        return nonpositive
    if order == "asc":
        for grade, threshold in zip(_GRADES, bins):
            if value <= threshold:
                return grade
    else:
        for grade, threshold in zip(_GRADES, bins):
            if value >= threshold:
                return grade
    return "E"


def _evaluate_component(component_config: dict, metric_name: str, metrics: dict) -> int:
    null_if_ocf_positive = component_config.get("null_if_ocf_positive")
    if null_if_ocf_positive is not None and metrics.get("ocf_positive") is True:
        return null_if_ocf_positive
    value = metrics.get(metric_name)
    if value is None:
        return component_config.get("else", 0)
    for threshold, points in component_config.get("points", []):
        if value >= threshold:
            return points
    return component_config.get("else", 0)


def evaluate_components(
    components_config: dict, metrics: dict
) -> tuple[int, dict[str, int]]:
    scores = {
        name: _evaluate_component(config, name, metrics)
        for name, config in components_config.items()
    }
    return sum(scores.values()), scores


def grade_from_buckets(value: int, grade_by_value: dict[str, list[int]]) -> str | None:
    for grade, values in grade_by_value.items():
        if value in values:
            return grade
    return None


def evaluate_rules(rules: list[dict], metrics: dict) -> str | None:
    for rule in rules:
        if "else" in rule:
            return rule["else"]
        if evaluate_condition(parse_condition(rule["if"]), metrics) is True:
            return rule["grade"]
    return None


def evaluate_score_items(items: list[str], metrics: dict) -> int:
    return sum(
        1
        for item in items
        if evaluate_condition(parse_condition(item), metrics) is True
    )


_BINS_ITEMS = ["asset_value", "earnings_value", "profitability", "growth"]
_RULES_ITEMS = ["survival", "cyclicality"]
_HUMAN_ITEMS = ["business_quality", "shareholder_policy"]


class AutoRatingDetail(BaseModel):
    metric: float | int | None
    points: float | int | None
    note: str | None


class AutoRatingsResult(BaseModel):
    ratings: dict[str, str | None]
    details: dict[str, AutoRatingDetail]


def compute_auto_ratings(config: dict, metrics: dict) -> AutoRatingsResult:
    """rubric_v1.yamlの4種のルール形式(bins/components/rules/score_items)を
    適用して10項目の自動評価を計算する。business_quality/shareholder_policy
    は常に人が評価するためNoneのまま。
    """
    ratings: dict[str, str | None] = {}
    details: dict[str, AutoRatingDetail] = {}

    for item in _BINS_ITEMS:
        item_config = config.get(item)
        if item_config is None:
            ratings[item] = None
            continue
        metric_name = item_config["metric"]
        value = metrics.get(metric_name)
        nonpositive = item_config.get("nonpositive")
        ratings[item] = evaluate_bins(
            value, item_config["bins"], item_config["order"], nonpositive
        )
        note = metric_name
        if nonpositive is not None and value is not None and value <= 0:
            note = f"{metric_name} (nonpositive)"
        details[item] = AutoRatingDetail(metric=value, points=None, note=note)

    fh_config = config.get("financial_health")
    if fh_config is not None:
        total, scores = evaluate_components(fh_config["components"], metrics)
        ratings["financial_health"] = grade_from_buckets(
            total, fh_config["grade_by_total"]
        )
        details["financial_health"] = AutoRatingDetail(
            metric=total, points=total, note=str(scores)
        )
    else:
        ratings["financial_health"] = None

    for item in _RULES_ITEMS:
        item_config = config.get(item)
        if item_config is None:
            ratings[item] = None
            continue
        ratings[item] = evaluate_rules(item_config["rules"], metrics)

    reversal_config = config.get("reversal")
    if reversal_config is not None:
        count = evaluate_score_items(reversal_config["score_items"], metrics)
        ratings["reversal"] = grade_from_buckets(
            count, reversal_config["grade_by_count"]
        )
        details["reversal"] = AutoRatingDetail(metric=count, points=count, note=None)
    else:
        ratings["reversal"] = None

    for item in _HUMAN_ITEMS:
        ratings[item] = None

    return AutoRatingsResult(ratings=ratings, details=details)


def load_rubric_config(path: Path, expected_version: int | None = None) -> dict:
    with path.open(encoding="utf-8") as f:
        config = YAML(typ="safe").load(f)
    if (
        expected_version is not None
        and config.get("rubric_version") != expected_version
    ):
        raise ValueError(
            f"rubric_version mismatch: file has {config.get('rubric_version')}, "
            f"expected {expected_version}"
        )
    return config
