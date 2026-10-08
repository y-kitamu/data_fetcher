"""Derive each ticker's status (watch / hold / sold / rejected) from its reports.

The report is the source of truth (docs/20261004_watchlist.md §4): the
status in the frontmatter of the latest initial / review / exit report,
ordered by the date prefix of the file name. The watchlist never stores it.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from ..reports.writer import split_frontmatter

STATUSES = ("watch", "hold", "sold", "rejected")
ARCHIVED_STATUSES = ("sold", "rejected")
_REPORT_TYPES = ("initial", "review", "exit")


@dataclass(frozen=True)
class ReportStatus:
    ticker: str
    status: object  # STATUSES のいずれか。frontmatter が不正なら None や想定外の値
    path: Path


def _report_files(ticker_dir: Path) -> list[Path]:
    files = []
    for order, report_type in enumerate(_REPORT_TYPES):
        files.extend((p.name[:10], order, p) for p in ticker_dir.glob(f"*_{report_type}.md"))
    return [p for _date, _order, p in sorted(files)]


def latest_report_status(ticker_dir: Path) -> ReportStatus | None:
    files = _report_files(ticker_dir)
    if not files:
        return None
    path = files[-1]
    try:
        fm_text, _body = split_frontmatter(path.read_text(encoding="utf-8"))
        frontmatter = YAML(typ="safe").load(fm_text) or {}
    except (ValueError, YAMLError):
        frontmatter = {}
    status = frontmatter.get("status") if isinstance(frontmatter, dict) else None
    return ReportStatus(ticker=ticker_dir.name, status=status, path=path)


def report_statuses(reports_dir: Path) -> dict[str, ReportStatus]:
    """レポートがある銘柄の status。`_` / `.` で始まるディレクトリは対象外。"""
    result = {}
    if not reports_dir.exists():
        return result
    for ticker_dir in sorted(reports_dir.iterdir()):
        if not ticker_dir.is_dir() or ticker_dir.name.startswith(("_", ".")):
            continue
        status = latest_report_status(ticker_dir)
        if status is not None:
            result[ticker_dir.name] = status
    return result
