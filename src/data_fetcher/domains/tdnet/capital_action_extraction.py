"""Lightweight regex-based extraction of capital-action details
from TDnet disclosure titles.

PDF body parsing is explicitly out of scope for this project (per the
project's report-generator decision log): amounts, share counts and
effective dates are almost never present in the title itself, so those
fields are left None and the caller should record a TDNET_VALUE_MISSING
warning. Only the split/reverse-split ratio (T17) is reliably present in a
standardized title phrasing and is worth extracting here.
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl
from pydantic import BaseModel

from ...core.constants import PROJECT_ROOT
from ...core.csv_store import append_and_save_csv
from .disclosure_classifier import CAPITAL_ACTION_TYPE_BY_CATEGORY
from .disclosure_list import DisclosureRow

CAPITAL_ACTIONS_DIR = PROJECT_ROOT / "data" / "tdnet" / "capital_actions"

_SPLIT_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*株\s*(?:につき|を|に対し(?:て)?)\s*(\d+(?:\.\d+)?)\s*株"
)

ROW_SCHEMA = {
    "code": pl.Utf8,
    "disclosed_at": pl.Utf8,
    "category_code": pl.Utf8,
    "type": pl.Utf8,
    "title": pl.Utf8,
    "split_ratio": pl.Float64,
    "effective_date": pl.Utf8,
    "url": pl.Utf8,
    "extraction_method": pl.Utf8,
}


class CapitalActionRow(BaseModel):
    code: str
    disclosed_at: str
    category_code: str
    type: str
    title: str
    split_ratio: float | None
    effective_date: str | None
    url: str | None
    extraction_method: str


def extract_split_ratio(title: str) -> float | None:
    """Extract the post-split-per-pre-split share ratio from a title.

    "1株につき2株の割合をもって株式分割" -> 2.0 (a 1-for-2 split).
    "10株を1株に併合" -> 0.1 (a 10-for-1 reverse split).
    Returns None if no such phrase is found (e.g. the ratio is only in the PDF body).
    """
    match = _SPLIT_RE.search(title)
    if match is None:
        return None
    before, after = float(match.group(1)), float(match.group(2))
    if before == 0:
        return None
    return after / before


def build_capital_action_record(
    row: DisclosureRow, category_code: str
) -> CapitalActionRow | None:
    """Build a capital-action record if category_code is one we track.

    Returns None for categories not in CAPITAL_ACTION_TYPE_BY_CATEGORY (i.e.
    most T1-T34 categories are not capital actions).
    """
    action_type = CAPITAL_ACTION_TYPE_BY_CATEGORY.get(category_code)
    if action_type is None:
        return None

    split_ratio: float | None = None
    extraction_method = "title_only"
    if category_code == "T17":
        split_ratio = extract_split_ratio(row.title)
        if split_ratio is not None:
            extraction_method = "title_regex"
            if split_ratio < 1:
                action_type = "reverse_split"

    return CapitalActionRow(
        code=row.code,
        disclosed_at=row.disclosed_at.isoformat(),
        category_code=category_code,
        type=action_type,
        title=row.title,
        split_ratio=split_ratio,
        effective_date=None,
        url=row.pdf_url,
        extraction_method=extraction_method,
    )


def append_capital_actions_to_csv(
    records: list[CapitalActionRow], output_dir: Path = CAPITAL_ACTIONS_DIR
) -> None:
    if not records:
        return
    by_code: dict[str, list[CapitalActionRow]] = {}
    for record in records:
        by_code.setdefault(record.code, []).append(record)
    for code, code_records in by_code.items():
        df = pl.DataFrame([r.model_dump() for r in code_records], schema=ROW_SCHEMA)
        append_and_save_csv(df, output_dir / f"{code}.csv", sort_col="disclosed_at")
