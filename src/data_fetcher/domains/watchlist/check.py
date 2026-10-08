"""`new_watch.py check`: format validation of the active / archive files and
consistency warnings against the report status (docs/20261004_watchlist.md §4).
Only reports problems; never rewrites a file.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from .metrics import METRICS
from .report_status import ARCHIVED_STATUSES, STATUSES, report_statuses
from .schema import render_json_schema, validate_entry
from .store import (
    ARCHIVE_DIR,
    WATCHLIST_FILE,
    WatchlistFileError,
    archive_files,
    load_entries,
    schema_path,
    watchlist_path,
)


@dataclass
class CheckResult:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _label(file_label: str, index: int, entry: object) -> str:
    ticker = entry.get("ticker") if isinstance(entry, dict) else None
    return f"{file_label}[{index}]" + (f"({ticker})" if ticker is not None else "")


def _check_active_entry(entry: dict, where: str, status: object, result: CheckResult) -> None:
    buy = entry.get("buy_conditions") or []
    sell = entry.get("sell_conditions") or []
    scenario = entry.get("scenario")
    if not scenario or not str(scenario).strip() or str(scenario).strip() == "TODO":
        result.warnings.append(f"{where}: scenario が未記入です")

    if status in ARCHIVED_STATUSES:
        result.warnings.append(
            f"{where}: レポートの status が {status} なのに active に残っています"
            "（new_watch.py remove または sync で archive へ移す）"
        )
    elif status == "hold":
        if not sell:
            result.warnings.append(f"{where}: status が hold なのに sell_conditions が空です")
    elif not buy:
        result.warnings.append(f"{where}: buy_conditions が空です（通知されません）")

    for cond in buy:
        metric = METRICS.get(cond.get("metric"))
        if metric is not None and metric.hold_only:
            result.warnings.append(
                f"{where}.buy_conditions: {metric.name} は hold のときしか計算されません"
            )


def check_watchlist(reports_dir: Path) -> CheckResult:
    result = CheckResult()
    statuses = report_statuses(reports_dir)

    for ticker, rs in statuses.items():
        if rs.status not in STATUSES:
            result.warnings.append(
                f"{rs.path}: status が不正です: {rs.status!r}（{' / '.join(STATUSES)}）"
            )

    active_tickers: set = set()
    try:
        active = load_entries(watchlist_path(reports_dir))
    except WatchlistFileError as e:
        result.errors.append(str(e))
        active = None

    if active is not None:
        for i, entry in enumerate(active):
            where = _label(WATCHLIST_FILE, i, entry)
            errors = validate_entry(entry, where, archived=False)
            result.errors.extend(errors)
            if errors:
                continue
            rs = statuses.get(entry["ticker"])
            _check_active_entry(entry, where, rs.status if rs else None, result)
        counts = Counter(e.get("ticker") for e in active if isinstance(e, dict))
        for ticker, n in counts.items():
            if n > 1:
                result.errors.append(f"{WATCHLIST_FILE}: {ticker} が {n} 件あります")
        active_tickers = set(counts)

        for ticker, rs in statuses.items():
            if ticker in active_tickers:
                continue
            if rs.status == "watch":
                result.warnings.append(f"{ticker}: レポートの status が watch ですが active にありません（sync で追加できます）")
            elif rs.status == "hold":
                result.warnings.append(f"{ticker}: レポートの status が hold ですが active にありません（add で追加する）")

    for path in archive_files(reports_dir):
        file_label = f"{ARCHIVE_DIR}/{path.name}"
        try:
            entries = load_entries(path)
        except WatchlistFileError as e:
            result.errors.append(str(e))
            continue
        for i, entry in enumerate(entries):
            where = _label(file_label, i, entry)
            errors = validate_entry(entry, where, archived=True)
            result.errors.extend(errors)
            if not errors and str(entry["removed"].year) != path.stem:
                result.warnings.append(f"{where}: removed の年 ({entry['removed']}) とファイル名が一致しません")

    schema = schema_path(reports_dir)
    if not schema.exists() or schema.read_text(encoding="utf-8") != render_json_schema():
        result.warnings.append(f"{schema} が最新ではありません（new_watch.py schema で更新）")
    return result
