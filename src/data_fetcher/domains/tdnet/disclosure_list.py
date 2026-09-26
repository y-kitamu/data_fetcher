"""Parse TDnet's daily disclosure list page into per-row metadata, regardless
of whether the disclosure has an XBRL attachment.

The existing ingestion pipeline (fetch_data_from_tdnet.py / csv_export.py)
only looks at rows carrying an XBRL zip (financial statements, forecast
revisions). Every other disclosure (capital actions, business risk notices,
M&A, officer changes, ...) is currently invisible to the system: not even its
title or date is stored. This module captures every row's metadata (date,
title, category, PDF/XBRL URLs) so the report generator can build the
`disclosures[]` list (section 6) and, for a handful of categories, extract a
value directly from the title (see capital_action_extraction.py).

IMPORTANT: TDnet's list pages omit a charset in the Content-Type header, so
`requests`'s automatic encoding detection (`Response.encoding`) picks Latin-1
and `Response.text` silently mojibake-decodes the Japanese title text. Always
build the BeautifulSoup tree from `response.content` (raw bytes), never
`response.text`, so BeautifulSoup's own encoding sniffing (which reads the
page's <meta charset> tag) is used instead.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl
from bs4.element import Tag
from pydantic import BaseModel

from ...core.constants import PROJECT_ROOT
from ...core.csv_store import append_and_save_csv
from .disclosure_classifier import classify

TDNET_BASE_URL = "https://www.release.tdnet.info/inbs/"
DISCLOSURES_DIR = PROJECT_ROOT / "data" / "tdnet" / "disclosures"

_JST = dt.timezone(dt.timedelta(hours=9))

ROW_SCHEMA = {
    "code": pl.Utf8,
    "disclosed_at": pl.Utf8,
    "category_code": pl.Utf8,
    "category_label": pl.Utf8,
    "is_correction": pl.Boolean,
    "title": pl.Utf8,
    "pdf_url": pl.Utf8,
    "xbrl_url": pl.Utf8,
    "has_xbrl": pl.Boolean,
    "page_date": pl.Utf8,
}


class DisclosureRow(BaseModel):
    code: str
    disclosed_at: dt.datetime
    title: str
    pdf_url: str | None
    xbrl_url: str | None
    has_xbrl: bool
    page_date: str


def _parse_disclosed_at(time_text: str, page_date: str) -> dt.datetime | None:
    try:
        naive = dt.datetime.strptime(f"{page_date} {time_text}", "%Y%m%d %H:%M")
    except ValueError:
        return None
    return naive.replace(tzinfo=_JST)


def parse_disclosure_row(row: Tag, page_date: str) -> DisclosureRow | None:
    """Parse one <tr> of TDnet's #main-list-table into a DisclosureRow.

    Returns None for rows that don't look like a disclosure row (e.g. header
    rows) rather than raising, since a page's exact row set can't be
    guaranteed ahead of time.
    """
    code_td = row.find("td", attrs={"class": "kjCode"})
    time_td = row.find("td", attrs={"class": "kjTime"})
    title_td = row.find("td", attrs={"class": "kjTitle"})
    if code_td is None or time_td is None or title_td is None:
        return None

    link = title_td.find("a")
    if link is None or link.get("href") is None:
        return None

    disclosed_at = _parse_disclosed_at(time_td.get_text(strip=True), page_date)
    if disclosed_at is None:
        return None

    code = code_td.get_text(strip=True)[:4]
    title = link.get_text(strip=True)
    pdf_url = TDNET_BASE_URL + link["href"]

    xbrl_url: str | None = None
    has_xbrl = False
    xbrl_td = row.find("td", attrs={"class": "kjXbrl"})
    if xbrl_td is not None:
        xbrl_link = xbrl_td.find("a")
        if xbrl_link is not None and xbrl_link.get("href"):
            has_xbrl = True
            xbrl_url = TDNET_BASE_URL + xbrl_link["href"]

    return DisclosureRow(
        code=code,
        disclosed_at=disclosed_at,
        title=title,
        pdf_url=pdf_url,
        xbrl_url=xbrl_url,
        has_xbrl=has_xbrl,
        page_date=page_date,
    )


def parse_disclosure_rows(table: Tag, page_date: str) -> list[DisclosureRow]:
    rows = [parse_disclosure_row(tr, page_date) for tr in table.find_all("tr")]
    return [row for row in rows if row is not None]


def build_disclosure_record(row: DisclosureRow) -> dict:
    result = classify(row.title)
    return {
        "code": row.code,
        "disclosed_at": row.disclosed_at.isoformat(),
        "category_code": result.category_code,
        "category_label": result.category_label,
        "is_correction": result.is_correction,
        "title": row.title,
        "pdf_url": row.pdf_url,
        "xbrl_url": row.xbrl_url,
        "has_xbrl": row.has_xbrl,
        "page_date": row.page_date,
    }


def append_disclosures_to_csv(
    rows: list[DisclosureRow], output_dir: Path = DISCLOSURES_DIR
) -> None:
    """Append parsed rows to per-ticker CSVs, grouped by security code.

    Idempotent via append_and_save_csv's whole-row dedup: since every column
    here is derived deterministically from the (date, title, url) of a given
    disclosure, re-processing the same page twice collapses back to one row.
    """
    if not rows:
        return
    by_code: dict[str, list[DisclosureRow]] = {}
    for row in rows:
        by_code.setdefault(row.code, []).append(row)
    for code, code_rows in by_code.items():
        records = [build_disclosure_record(r) for r in code_rows]
        append_and_save_csv(
            pl.DataFrame(records, schema=ROW_SCHEMA),
            output_dir / f"{code}.csv",
            sort_col="disclosed_at",
        )
