import datetime as dt
import json
import sys
from pathlib import Path

import pytest

from data_fetcher.domains.watchlist.check import check_watchlist
from data_fetcher.domains.watchlist.metrics import METRICS
from data_fetcher.domains.watchlist.report_status import report_statuses
from data_fetcher.domains.watchlist.schema import build_json_schema, render_json_schema, validate_entry
from data_fetcher.domains.watchlist.store import (
    WatchlistFileError,
    add_to_text,
    cut_entry,
    parse_entries,
    remove_from_text,
    schema_path,
    watchlist_path,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
import new_watch  # noqa: E402

_DAY = dt.date(2026, 10, 4)

_ACTIVE = """\
# 先頭コメント
- ticker: "7014"
  added: 2026-10-01
  scenario: |
    受注残の増加で来期増益を想定。
  buy_conditions:
    - {text: 75日線を下回る, metric: price.vs_ma75, op: "<", value: 0}
  sell_conditions:   # hold のときだけ

# 次の銘柄のメモ
- ticker: "278A"
  added: 2026-10-02
  scenario: 押し目狙い
  buy_conditions:
  - {text: ピークから-30%, metric: price.drawdown_from_peak, op: "<=", value: -0.3}
"""


def _write_report(reports_dir: Path, ticker: str, date: str, report_type: str, status: str) -> None:
    path = reports_dir / ticker / f"{date}_{ticker}_{report_type}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"---\ntype: {report_type}\nstatus: {status}  # comment\n---\n本文\n", encoding="utf-8")


def test_add_to_text_appends_template_without_touching_existing_text():
    new_text = add_to_text(_ACTIVE, "9999", _DAY)
    assert new_text.startswith(_ACTIVE)
    entries = parse_entries(new_text, "t")
    assert entries[-1]["ticker"] == "9999"
    assert entries[-1]["added"] == _DAY
    assert entries[-1]["buy_conditions"] is None
    assert validate_entry(entries[-1], "e", archived=False) == []


def test_add_to_text_creates_header_with_schema_modeline():
    new_text = add_to_text("", "7014", _DAY)
    assert new_text.startswith("# yaml-language-server: $schema=./watchlist.schema.json")
    assert [e["ticker"] for e in parse_entries(new_text, "t")] == ["7014"]


def test_add_to_text_rejects_duplicate():
    with pytest.raises(WatchlistFileError):
        add_to_text(_ACTIVE, "7014", _DAY)


def test_cut_entry_keeps_comments_of_other_entries():
    rest, block = cut_entry(_ACTIVE, "7014", "t")
    assert "# 先頭コメント" in rest
    assert "# 次の銘柄のメモ" in rest
    assert "# hold のときだけ" in block
    assert [e["ticker"] for e in parse_entries(rest, "t")] == ["278A"]


def test_remove_from_text_moves_entry_to_archive_with_reason():
    active, archive = remove_from_text(_ACTIVE, "", "278A", _DAY, 'rejected: "高すぎ"')
    assert [e["ticker"] for e in parse_entries(active, "t")] == ["7014"]
    archived = parse_entries(archive, "t")
    assert archived[0]["ticker"] == "278A"
    assert archived[0]["removed"] == _DAY
    assert archived[0]["removed_reason"] == 'rejected: "高すぎ"'
    assert len(archived[0]["buy_conditions"]) == 1
    assert validate_entry(archived[0], "e", archived=True) == []


def test_remove_from_text_appends_to_existing_archive():
    _, archive = remove_from_text(_ACTIVE, "", "278A", _DAY, "a")
    _, archive = remove_from_text(_ACTIVE, archive, "7014", _DAY, "b")
    assert [e["ticker"] for e in parse_entries(archive, "t")] == ["278A", "7014"]


def test_cut_entry_unknown_ticker_raises():
    with pytest.raises(WatchlistFileError):
        cut_entry(_ACTIVE, "1234", "t")


def test_cut_entry_rejects_layout_it_cannot_split():
    flow = '[{ticker: "7014", added: 2026-10-01}]\n'
    with pytest.raises(WatchlistFileError):
        cut_entry(flow, "7014", "t")


def test_validate_entry_reports_format_errors():
    entry = {
        "ticker": 7014,
        "added": "yesterday",
        "removed": _DAY,
        "buy_conditions": [
            {"text": "t", "metric": "price.unknown", "op": "=<", "value": "0"},
            {"text": "t", "metric": "price.close"},
        ],
    }
    errors = validate_entry(entry, "e", archived=False)
    joined = "\n".join(errors)
    assert "e.ticker" in joined
    assert "e.added" in joined
    assert "'removed'" in joined
    assert "price.unknown" in joined
    assert "=<" in joined
    assert "value: 数値" in joined
    assert "buy_conditions[1]: op がありません" in joined


def test_validate_entry_archive_requires_removed_fields():
    errors = validate_entry({"ticker": "7014", "added": _DAY}, "e", archived=True)
    assert "e: removed がありません" in errors
    assert "e: removed_reason がありません" in errors


def test_json_schema_metric_enum_matches_definitions():
    schema = build_json_schema()
    assert schema["definitions"]["condition"]["properties"]["metric"]["enum"] == list(METRICS)
    assert json.loads(render_json_schema()) == schema


def test_report_status_uses_latest_report(tmp_path):
    _write_report(tmp_path, "7014", "2026-01-01", "initial", "watch")
    _write_report(tmp_path, "7014", "2026-05-01", "review", "hold")
    _write_report(tmp_path, "278A", "2026-02-01", "initial", "watch")
    _write_report(tmp_path, "278A", "2026-02-01", "exit", "sold")
    (tmp_path / "_config").mkdir()
    statuses = report_statuses(tmp_path)
    assert set(statuses) == {"7014", "278A"}
    assert statuses["7014"].status == "hold"
    assert statuses["278A"].status == "sold"


def test_check_warns_on_status_inconsistencies(tmp_path):
    path = watchlist_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_text(_ACTIVE, encoding="utf-8")
    schema_path(tmp_path).write_text(render_json_schema(), encoding="utf-8")
    _write_report(tmp_path, "7014", "2026-10-01", "review", "hold")
    _write_report(tmp_path, "278A", "2026-10-01", "initial", "rejected")
    _write_report(tmp_path, "1111", "2026-10-01", "initial", "watch")

    result = check_watchlist(tmp_path)
    assert result.errors == []
    joined = "\n".join(result.warnings)
    assert "(7014): status が hold なのに sell_conditions が空" in joined
    assert "(278A): レポートの status が rejected" in joined
    assert "1111: レポートの status が watch ですが active にありません" in joined
    assert "最新ではありません" not in joined


def test_check_reports_errors_and_duplicates(tmp_path):
    path = watchlist_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_text(_ACTIVE + "- ticker: 7014\n  added: 2026-10-03\n", encoding="utf-8")
    result = check_watchlist(tmp_path)
    assert any("文字列で書いてください" in e for e in result.errors)
    assert any("最新ではありません" in w for w in result.warnings)


def test_cli_add_remove_sync_check(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("REPORTS_DIR", str(tmp_path))
    monkeypatch.setattr(new_watch, "_today", lambda: _DAY)
    _write_report(tmp_path, "278A", "2026-10-02", "initial", "watch")
    _write_report(tmp_path, "7003", "2026-10-02", "initial", "rejected")

    assert new_watch.main(["schema"]) == 0
    assert new_watch.main(["add", "7003"]) == 0
    assert new_watch.main(["sync"]) == 0

    active = parse_entries(watchlist_path(tmp_path).read_text(encoding="utf-8"), "t")
    assert [e["ticker"] for e in active] == ["278A"]
    archive = parse_entries((tmp_path / "_config/watchlist_archive/2026.yaml").read_text(encoding="utf-8"), "t")
    assert archive[0]["ticker"] == "7003"
    assert archive[0]["removed_reason"].startswith("sync: レポートの status が rejected")

    assert new_watch.main(["remove", "278A", "--reason", "見送り"]) == 0
    capsys.readouterr()
    assert new_watch.main(["add", "7003"]) == 0
    assert "過去に 1 回ウォッチしています" in capsys.readouterr().out

    assert new_watch.main(["check"]) == 0
    assert new_watch.main(["remove", "9999", "--reason", "x"]) == 1


def test_cli_dry_run_writes_nothing(tmp_path, monkeypatch):
    monkeypatch.setenv("REPORTS_DIR", str(tmp_path))
    assert new_watch.main(["add", "7014", "--dry-run"]) == 0
    assert not watchlist_path(tmp_path).exists()
