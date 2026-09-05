"""Tests for the data_health anomaly-detection helpers used by
notify_data_status.py."""

import datetime
import os
from typing import Any

from data_fetcher.core import data_health
from data_fetcher.core.data_health import SourceRule


# --- count_missed_weekdays --------------------------------------------------


def test_count_missed_weekdays_same_day_returns_zero() -> None:
    d = datetime.date(2026, 9, 4)  # Friday
    assert data_health.count_missed_weekdays(d, d) == 0


def test_count_missed_weekdays_end_before_start_returns_zero() -> None:
    start = datetime.date(2026, 9, 4)
    end = datetime.date(2026, 9, 1)
    assert data_health.count_missed_weekdays(start, end) == 0


def test_count_missed_weekdays_skips_weekend() -> None:
    # Friday 9/4 -> Monday 9/7: only Monday should count as a missed weekday.
    start = datetime.date(2026, 9, 4)
    end = datetime.date(2026, 9, 7)
    assert data_health.count_missed_weekdays(start, end) == 1


def test_count_missed_weekdays_counts_each_weekday() -> None:
    start = datetime.date(2026, 9, 1)  # Tuesday
    end = datetime.date(2026, 9, 4)  # Friday
    assert data_health.count_missed_weekdays(start, end) == 3


# --- check_per_date_source ---------------------------------------------------


def test_check_per_date_source_within_threshold_returns_none() -> None:
    rule = SourceRule(pattern="per_date", cadence="daily")
    today = datetime.date(2026, 9, 5)
    result = data_health.check_per_date_source("binance/tick", "20260904", rule, today)
    assert result is None


def test_check_per_date_source_exceeds_threshold_flags_critical() -> None:
    rule = SourceRule(pattern="per_date", cadence="daily")
    today = datetime.date(2026, 9, 5)
    result = data_health.check_per_date_source("binance/tick", "20260901", rule, today)
    assert result is not None
    assert result.severity == "critical"
    assert result.kind == "全体停止"


def test_check_per_date_source_weekday_cadence_tolerates_long_weekend() -> None:
    # Friday 8/28 -> Wednesday 9/2 (following week): 3 missed weekdays (Mon,
    # Tue, Wed), well under the threshold of 6.
    rule = SourceRule(pattern="per_date", cadence="weekday")
    today = datetime.date(2026, 9, 2)
    result = data_health.check_per_date_source("sbi/tick", "20260828", rule, today)
    assert result is None


def test_check_per_date_source_no_data_sentinel_flags_critical() -> None:
    rule = SourceRule(pattern="per_date", cadence="daily")
    today = datetime.date(2026, 9, 5)
    result = data_health.check_per_date_source("fxea/data", "No data", rule, today)
    assert result is not None
    assert result.severity == "critical"


def test_check_per_date_source_monthly_yyyymm_format() -> None:
    rule = SourceRule(pattern="per_date", cadence="monthly")
    today = datetime.date(2026, 9, 5)
    # 202608 -> 2026-08-01, ~35 days before 2026-09-05: under the 45-day threshold.
    assert (
        data_health.check_per_date_source("histdata/tick", "202608", rule, today)
        is None
    )
    # 202601 -> far in the past: over threshold.
    result = data_health.check_per_date_source("histdata/tick", "202601", rule, today)
    assert result is not None


# --- analyze_per_ticker_source -----------------------------------------------


def _touch(path, content: str, mtime: datetime.datetime | None = None) -> None:
    path.write_text(content)
    if mtime is not None:
        ts = mtime.timestamp()
        os.utime(path, (ts, ts))


def test_analyze_per_ticker_source_flags_delayed_and_never_fetched(
    tmp_path, monkeypatch: Any
) -> None:
    now = datetime.datetime.now()
    stale = now - datetime.timedelta(days=10)
    padding = "x" * 250  # keep tracked files above the header-only size cutoff

    _touch(tmp_path / "1301.csv", f"date,close\n2026/09/04,100\n{padding}\n", now)
    _touch(tmp_path / "1302.csv", f"date,close\n2026/09/04,200\n{padding}\n", now)
    _touch(tmp_path / "133A.csv", f"date,close\n2026/08/25,300\n{padding}\n", stale)
    _touch(tmp_path / "9999.csv", "date,close\n", now)  # header only
    _touch(
        tmp_path / "8888.csv", f"date,close\n2026/09/04,400\n{padding}\n", now
    )  # orphan

    monkeypatch.setattr(
        data_health,
        "get_jp_ticker_list",
        lambda include_etf=False: ["1301", "1302", "133A", "9999"],
    )

    rule = SourceRule(
        pattern="per_ticker", cadence="weekday", ticker_universe="jp_stock"
    )
    items = data_health.analyze_per_ticker_source(
        "kabutan/daily", tmp_path, rule, datetime.date.today()
    )

    kinds = {item.kind: item for item in items}
    assert "部分遅延" in kinds
    assert kinds["部分遅延"].examples == ["133A"]
    assert "未取得" in kinds
    assert kinds["未取得"].examples == ["9999"]
    assert "全体停止" not in kinds
    # Orphan file (not in the current ticker universe) must never surface.
    assert all(
        "8888" not in item.message and "8888" not in item.examples for item in items
    )


def test_analyze_per_ticker_source_all_fresh_reports_nothing(
    tmp_path, monkeypatch: Any
) -> None:
    now = datetime.datetime.now()
    padding = "x" * 250
    _touch(tmp_path / "1301.csv", f"date,close\n2026/09/04,100\n{padding}\n", now)
    _touch(tmp_path / "1302.csv", f"date,close\n2026/09/04,200\n{padding}\n", now)

    monkeypatch.setattr(
        data_health, "get_jp_ticker_list", lambda include_etf=False: ["1301", "1302"]
    )

    rule = SourceRule(
        pattern="per_ticker", cadence="weekday", ticker_universe="jp_stock"
    )
    items = data_health.analyze_per_ticker_source(
        "kabutan/daily", tmp_path, rule, datetime.date.today()
    )
    assert items == []


def test_analyze_per_ticker_source_irregular_cadence_ignores_stale_majority(
    tmp_path, monkeypatch: Any
) -> None:
    """Financial-style data: most tickers legitimately go untouched for a long
    time, so only a pipeline-wide freeze (nobody updated recently) should
    trigger, never a per-ticker delay."""
    now = datetime.datetime.now()
    long_ago = now - datetime.timedelta(days=200)
    padding = "x" * 250

    _touch(tmp_path / "1301.csv", f'{{"q": 1}}\n{padding}\n', now)  # one recent filer
    for code in ["1302", "1303", "1304"]:
        _touch(tmp_path / f"{code}.csv", f'{{"q": 1}}\n{padding}\n', long_ago)

    monkeypatch.setattr(
        data_health,
        "get_jp_ticker_list",
        lambda include_etf=False: ["1301", "1302", "1303", "1304"],
    )

    rule = SourceRule(
        pattern="per_ticker", cadence="irregular", ticker_universe="jp_stock"
    )
    items = data_health.analyze_per_ticker_source(
        "kabutan/financial", tmp_path, rule, datetime.date.today()
    )
    assert items == []  # newest file (1301) is recent enough, so pipeline looks alive


def test_analyze_per_ticker_source_irregular_cadence_flags_total_freeze(
    tmp_path, monkeypatch: Any
) -> None:
    now = datetime.datetime.now()
    long_ago = now - datetime.timedelta(days=200)
    padding = "x" * 250
    for code in ["1301", "1302"]:
        _touch(tmp_path / f"{code}.csv", f'{{"q": 1}}\n{padding}\n', long_ago)

    monkeypatch.setattr(
        data_health, "get_jp_ticker_list", lambda include_etf=False: ["1301", "1302"]
    )

    rule = SourceRule(
        pattern="per_ticker", cadence="irregular", ticker_universe="jp_stock"
    )
    items = data_health.analyze_per_ticker_source(
        "kabutan/financial", tmp_path, rule, datetime.date.today()
    )
    assert len(items) == 1
    assert items[0].kind == "全体停止"
    assert items[0].severity == "critical"


# --- build_anomaly_items orchestration ---------------------------------------


def test_build_anomaly_items_skips_disabled_sources(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        data_health,
        "SOURCE_RULES",
        {
            "news/yfinance": SourceRule(
                pattern="per_date", cadence="irregular", enabled=False
            )
        },
    )
    data_nums = {"news/yfinance": ("20240101", 1)}
    items = data_health.build_anomaly_items(data_nums, datetime.date(2026, 9, 5))
    assert items == []


def test_build_anomaly_items_sorts_by_severity_then_source() -> None:
    fake_rules = {
        "b_source": SourceRule(pattern="per_date", cadence="daily"),
        "a_source": SourceRule(pattern="per_date", cadence="daily"),
    }
    data_nums = {
        "b_source": ("20260101", 1),  # very stale -> critical
        "a_source": ("20260101", 1),  # very stale -> critical
    }
    import data_fetcher.core.data_health as module

    orig_rules = module.SOURCE_RULES
    module.SOURCE_RULES = fake_rules
    try:
        items = module.build_anomaly_items(data_nums, datetime.date(2026, 9, 5))
    finally:
        module.SOURCE_RULES = orig_rules

    assert [item.source for item in items] == ["a_source", "b_source"]


# --- render_anomaly_table_html -----------------------------------------------


def test_render_anomaly_table_html_no_anomalies() -> None:
    html = data_health.render_anomaly_table_html([])
    assert "異常は検出されませんでした" in html


def test_render_anomaly_table_html_includes_examples() -> None:
    item = data_health.AnomalyItem(
        source="kabutan/daily",
        severity="warning",
        kind="部分遅延",
        message="test message",
        examples=["1301", "1302"],
    )
    html = data_health.render_anomaly_table_html([item])
    assert "kabutan/daily" in html
    assert "1301, 1302" in html
    assert "alert-warning" in html
