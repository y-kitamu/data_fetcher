"""data_health.py - Classify data sources by storage pattern and update cadence,
and flag files/tickers that look like they stopped being fetched.

Used by scripts/notify_data_status.py to add a "要確認" section to the daily
digest email. Deliberately independent from the existing digest functions
(`_describe_dir` etc.) so this stays additive and never changes their output.
"""

import datetime
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Literal

from .constants import PROJECT_ROOT
from .ticker_list import get_jp_ticker_list

_DATA_FILE_PATTERNS = ("*.csv*", "*.json*")
_HEADER_ONLY_SIZE_BYTES = 200

Pattern = Literal["per_ticker", "per_date"]
Cadence = Literal["daily", "weekday", "weekly", "monthly", "irregular"]
Severity = Literal["critical", "warning", "info"]

# For "weekday" the unit is weekdays elapsed (see count_missed_weekdays), for
# everything else it's calendar days.
_CADENCE_THRESHOLD: dict[Cadence, int] = {
    "daily": 2,
    "weekday": 6,
    "weekly": 14,
    "monthly": 45,
    "irregular": 45,
}
_GRACE_DAYS: dict[Cadence, int] = {"daily": 0, "weekday": 2}
_SEVERITY_ORDER: dict[Severity, int] = {"critical": 0, "warning": 1, "info": 2}


@dataclass(frozen=True)
class SourceRule:
    pattern: Pattern
    cadence: Cadence
    ticker_universe: Literal["jp_stock"] | None = None
    code_from_stem: Callable[[str], str] = lambda stem: stem
    enabled: bool = True


# Keys must match the result keys produced by get_latest_dates_data_number() /
# get_news_number() / get_fxea_data_number() / get_kabus_data_number() in
# scripts/notify_data_status.py. Sources not listed here are shown in the
# existing digest table but are not judged (avoids false positives).
SOURCE_RULES: dict[str, SourceRule] = {
    "kabutan/daily": SourceRule(
        pattern="per_ticker", cadence="weekday", ticker_universe="jp_stock"
    ),
    "kabutan/financial": SourceRule(
        pattern="per_ticker", cadence="irregular", ticker_universe="jp_stock"
    ),
    "rakuten/daily": SourceRule(
        pattern="per_ticker",
        cadence="weekday",
        ticker_universe="jp_stock",
        enabled=False,
    ),
    "rakuten/minutes": SourceRule(pattern="per_date", cadence="weekday", enabled=False),
    "yfinance/minutes": SourceRule(pattern="per_date", cadence="weekday"),
    "yfinance/financial": SourceRule(pattern="per_ticker", cadence="irregular"),
    "binance/tick": SourceRule(pattern="per_date", cadence="daily"),
    "binance/klines": SourceRule(pattern="per_date", cadence="daily", enabled=False),
    "gmo/tick": SourceRule(pattern="per_date", cadence="daily"),
    "gmo/book": SourceRule(pattern="per_date", cadence="daily"),
    "bitflyer/tick": SourceRule(pattern="per_date", cadence="daily"),
    "bitflyer/book": SourceRule(pattern="per_date", cadence="daily"),
    "sbi/tick": SourceRule(pattern="per_date", cadence="weekday"),
    "taisyaku/history": SourceRule(pattern="per_date", cadence="weekday"),
    "taisyaku/history_by_ticker": SourceRule(
        pattern="per_ticker",
        cadence="weekday",
        ticker_universe="jp_stock",
        code_from_stem=lambda stem: stem.split("-")[0],
    ),
    "taisyaku/zandaka": SourceRule(pattern="per_date", cadence="weekday"),
    "jpx_stats/investor_type": SourceRule(pattern="per_date", cadence="weekly"),
    "jpx_stats/margin_daily_disclosure": SourceRule(
        pattern="per_date", cadence="weekday"
    ),
    "jp_ticker_themes": SourceRule(pattern="per_date", cadence="daily"),
    "histdata/tick": SourceRule(pattern="per_date", cadence="monthly"),
    "news/kabutan": SourceRule(pattern="per_date", cadence="weekday"),
    "news/yfinance": SourceRule(pattern="per_date", cadence="irregular", enabled=False),
    "news/gnews": SourceRule(pattern="per_date", cadence="irregular", enabled=False),
    "tdnet": SourceRule(
        pattern="per_ticker", cadence="irregular", ticker_universe="jp_stock"
    ),
    "edinet/financial": SourceRule(
        pattern="per_ticker",
        cadence="irregular",
        ticker_universe="jp_stock",
        code_from_stem=lambda stem: stem[:4],
    ),
    "edinet/schedule13": SourceRule(pattern="per_ticker", cadence="irregular"),
    "edinet/large_shareholding": SourceRule(pattern="per_date", cadence="irregular"),
    "google_trends": SourceRule(
        pattern="per_ticker", cadence="irregular", ticker_universe="jp_stock"
    ),
    "fxea/data": SourceRule(pattern="per_date", cadence="daily", enabled=False),
    "kabus": SourceRule(pattern="per_date", cadence="weekday"),
}


@dataclass
class AnomalyItem:
    source: str
    severity: Severity
    kind: str
    message: str
    examples: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class FileInfo:
    stem: str
    date: datetime.date
    size_bytes: int


def count_missed_weekdays(start: datetime.date, end: datetime.date) -> int:
    """Number of weekdays (Mon-Fri) strictly after `start`, up to and including
    `end`. No holiday calendar: a long weekday-only streak (year-end/GW) is
    absorbed by the "weekday" cadence threshold instead."""
    if end <= start:
        return 0
    count = 0
    current = start + datetime.timedelta(days=1)
    while current <= end:
        if current.weekday() < 5:
            count += 1
        current += datetime.timedelta(days=1)
    return count


def _elapsed_units(
    latest: datetime.date, today: datetime.date, cadence: Cadence
) -> int:
    if cadence == "weekday":
        return count_missed_weekdays(latest, today)
    return (today - latest).days


def list_file_dates(dir_path: Path) -> list[FileInfo]:
    """Only ever called on per-ticker directories (see analyze_per_ticker_source),
    so filenames are ticker codes, never date-prefixed: mtime is the only
    signal available, unlike the date-or-mtime guess _describe_dir makes for
    directories that could be either shape."""
    files = [
        p
        for pattern in _DATA_FILE_PATTERNS
        for p in dir_path.glob(pattern)
        if p.is_file()
    ]
    return [
        FileInfo(
            stem=p.stem,
            date=datetime.datetime.fromtimestamp(p.stat().st_mtime).date(),
            size_bytes=p.stat().st_size,
        )
        for p in files
    ]


def _parse_date_str(date_str: str) -> datetime.date | None:
    digits = date_str.replace("-", "")
    if len(digits) == 8 and digits.isdigit():
        return datetime.datetime.strptime(digits, "%Y%m%d").date()
    if len(digits) == 6 and digits.isdigit():
        return datetime.datetime.strptime(digits + "01", "%Y%m%d").date()
    return None


def analyze_per_ticker_source(
    source: str, dir_path: Path, rule: SourceRule, today: datetime.date
) -> list[AnomalyItem]:
    files = list_file_dates(dir_path)
    if not files:
        return []

    if rule.ticker_universe == "jp_stock":
        universe = set(get_jp_ticker_list(include_etf=True))
        files = [f for f in files if rule.code_from_stem(f.stem) in universe]
    if not files:
        return []

    untracked = [f for f in files if f.size_bytes < _HEADER_ONLY_SIZE_BYTES]
    tracked = [f for f in files if f.size_bytes >= _HEADER_ONLY_SIZE_BYTES]

    items: list[AnomalyItem] = []

    if tracked:
        if rule.cadence == "irregular":
            newest = max(f.date for f in tracked)
            elapsed = (today - newest).days
            threshold = _CADENCE_THRESHOLD["irregular"]
            if elapsed > threshold:
                items.append(
                    AnomalyItem(
                        source=source,
                        severity="critical",
                        kind="全体停止",
                        message=(
                            f"全{len(files)}件中、直近の更新が{newest.isoformat()}"
                            f"({elapsed}日前)までありません(閾値{threshold}日)"
                        ),
                    )
                )
        else:
            date_counts = Counter(f.date for f in tracked)
            mode_date = max(date_counts.items(), key=lambda kv: (kv[1], kv[0]))[0]
            elapsed = _elapsed_units(mode_date, today, rule.cadence)
            threshold = _CADENCE_THRESHOLD[rule.cadence]
            if elapsed > threshold:
                items.append(
                    AnomalyItem(
                        source=source,
                        severity="critical",
                        kind="全体停止",
                        message=f"基準日{mode_date.isoformat()}が古すぎます(本日から{elapsed}経過、閾値{threshold})",
                    )
                )
            else:
                grace = _GRACE_DAYS.get(rule.cadence, 0)
                delayed = [f for f in tracked if (mode_date - f.date).days > grace]
                if delayed:
                    pct = len(delayed) / len(files) * 100
                    examples = sorted(rule.code_from_stem(f.stem) for f in delayed)[:10]
                    items.append(
                        AnomalyItem(
                            source=source,
                            severity="warning",
                            kind="部分遅延",
                            message=(
                                f"{len(files)}銘柄中{len(delayed)}銘柄({pct:.1f}%)が"
                                f"最新日{mode_date.isoformat()}に届いていません"
                            ),
                            examples=examples,
                        )
                    )

    if untracked:
        examples = sorted(rule.code_from_stem(f.stem) for f in untracked)[:10]
        items.append(
            AnomalyItem(
                source=source,
                severity="info",
                kind="未取得",
                message=f"{len(untracked)}銘柄が一度も取得できていません(ヘッダー行のみ)",
                examples=examples,
            )
        )

    return items


def check_per_date_source(
    source: str, date_str: str, rule: SourceRule, today: datetime.date
) -> AnomalyItem | None:
    if date_str == "No data":
        return AnomalyItem(
            source=source,
            severity="critical",
            kind="全体停止",
            message="データが1件も見つかりません",
        )
    latest = _parse_date_str(date_str)
    if latest is None:
        return None
    elapsed = _elapsed_units(latest, today, rule.cadence)
    threshold = _CADENCE_THRESHOLD[rule.cadence]
    if elapsed <= threshold:
        return None
    unit = "平日" if rule.cadence == "weekday" else "日"
    return AnomalyItem(
        source=source,
        severity="critical",
        kind="全体停止",
        message=f"最終更新{latest.isoformat()}から{elapsed}{unit}経過しています(閾値{threshold}{unit})",
    )


def build_anomaly_items(
    data_nums: dict[str, tuple[str, int]], today: datetime.date | None = None
) -> list[AnomalyItem]:
    today = today or datetime.date.today()
    data_root = PROJECT_ROOT / "data"
    items: list[AnomalyItem] = []

    for source, rule in SOURCE_RULES.items():
        if not rule.enabled:
            continue
        if rule.pattern == "per_ticker":
            items.extend(
                analyze_per_ticker_source(source, data_root / source, rule, today)
            )
        else:
            described = data_nums.get(source)
            if described is None:
                continue
            date_str, _count = described
            item = check_per_date_source(source, date_str, rule, today)
            if item is not None:
                items.append(item)

    items.sort(key=lambda item: (_SEVERITY_ORDER[item.severity], item.source))
    return items


_SEVERITY_CLASS: dict[Severity, str] = {
    "critical": "alert-critical",
    "warning": "alert-warning",
    "info": "alert-info",
}


def render_anomaly_table_html(items: list[AnomalyItem]) -> str:
    if not items:
        body = "<tr><td colspan='4'>異常は検出されませんでした。</td></tr>"
    else:
        rows = []
        for item in items:
            examples = ", ".join(item.examples) if item.examples else "-"
            rows.append(
                f"<tr class='{_SEVERITY_CLASS[item.severity]}'>"
                f"<td>{item.source}</td><td>{item.kind}</td>"
                f"<td>{item.message}</td><td>{examples}</td></tr>"
            )
        body = "\n".join(rows)
    return f"""<h3>要確認 (取得失敗の可能性)</h3>
<table>
  <thead>
    <tr><th>Source</th><th>種別</th><th>内容</th><th>代表例</th></tr>
  </thead>
  <tbody>
{body}
  </tbody>
</table>"""
