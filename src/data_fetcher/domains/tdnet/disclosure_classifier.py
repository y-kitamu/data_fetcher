"""Classify TDnet disclosure titles into the T1-T34 categories
 using a keyword dictionary (constants/disclosure_categories.yaml).

This is a lightweight, best-effort classifier: TDnet titles are free text and
the vast majority of disclosures fall outside T1-T34 entirely, so an
unmatched title is a normal outcome, not an error.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pydantic import BaseModel
from ruamel.yaml import YAML

_CONFIG_PATH = Path(__file__).parent / "constants" / "disclosure_categories.yaml"


class ClassificationResult(BaseModel):
    category_code: str | None
    category_label: str | None
    is_correction: bool


@dataclass(frozen=True)
class _CategoryRule:
    code: str
    label: str
    keywords: tuple[str, ...]
    exclude: tuple[str, ...]


def load_config(path: Path = _CONFIG_PATH) -> dict:
    with path.open(encoding="utf-8") as f:
        return YAML(typ="safe").load(f)


def _build_rules(config: dict) -> list[_CategoryRule]:
    return [
        _CategoryRule(
            code=code,
            label=entry["label"],
            keywords=tuple(entry.get("keywords", [])),
            exclude=tuple(entry.get("exclude", [])),
        )
        for code, entry in config["categories"].items()
    ]


_CONFIG = load_config()
_RULES = _build_rules(_CONFIG)
_CORRECTION_KEYWORDS: tuple[str, ...] = tuple(_CONFIG.get("correction_keywords", []))
_CORRECTION_APPLIES_TO: frozenset[str] = frozenset(
    _CONFIG.get("correction_applies_to", [])
)
CAPITAL_ACTION_TYPE_BY_CATEGORY: dict[str, str] = _CONFIG.get(
    "capital_action_type_by_category", {}
)


def is_correction_title(
    title: str, *, correction_keywords: tuple[str, ...] = _CORRECTION_KEYWORDS
) -> bool:
    return any(keyword in title for keyword in correction_keywords)


def classify(
    title: str, *, rules: list[_CategoryRule] | None = None
) -> ClassificationResult:
    """Classify a TDnet disclosure title into a T1-T34 category.

    Rules are evaluated top to bottom (the order in
    constants/disclosure_categories.yaml); the first rule whose keywords
    match (and whose exclude terms do not) wins. Corrections of T1/T2/T4/T5/T6
    are reported as T8 per the spec, since the instructions treat corrections
    of those categories as their own category.
    """
    rules = _RULES if rules is None else rules
    correction = is_correction_title(title)
    for rule in rules:
        matched = any(keyword in title for keyword in rule.keywords)
        excluded = any(keyword in title for keyword in rule.exclude)
        if matched and not excluded:
            if correction and rule.code in _CORRECTION_APPLIES_TO:
                return ClassificationResult(
                    category_code="T8", category_label="訂正報告", is_correction=True
                )
            return ClassificationResult(
                category_code=rule.code,
                category_label=rule.label,
                is_correction=correction,
            )
    return ClassificationResult(
        category_code=None, category_label=None, is_correction=correction
    )
