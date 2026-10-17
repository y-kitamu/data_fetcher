"""Tests for the Claude Code drafting step used by scripts/draft_report.py.
`claude` itself is never invoked: subprocess.run is replaced by a fake."""

import datetime as dt
import json
import subprocess
from pathlib import Path

import pytest

from data_fetcher.domains.reports import ai_draft
from data_fetcher.domains.reports.ai_draft import DraftError

TODAY = dt.date(2026, 10, 10)

REPORT = """---
ticker: "7014"
thesis: ""
---

# report

## 1. サマリー

<!-- snapshot-summary:start 生成スクリプトが書き込む。手で編集しない -->
| 株価 |
<!-- snapshot-summary:end -->

<!-- auto:summary_card -->

（記入）
"""

PAYLOAD = {
    "summary": "下書きしました",
    "filled_sections": ["2.1 沿革・事業内容"],
    "unresolved": ["市場規模"],
    "sources": [
        {"title": "有報", "url": "https://example.com", "accessed": "2026-10-10"}
    ],
}


def _envelope(payload, **extra) -> str:
    return json.dumps(
        {
            "type": "result",
            "is_error": False,
            "result": "",
            "structured_output": payload,
        }
        | extra
    )


@pytest.fixture
def report(tmp_path: Path) -> Path:
    path = tmp_path / "7014" / "2026-10-02_7014_initial.md"
    path.parent.mkdir()
    path.write_text(REPORT, encoding="utf-8")
    return path


@pytest.fixture
def fake_claude(monkeypatch: pytest.MonkeyPatch):
    """claude の代わりに `edit(text) -> text` でファイルを書き換え、stdout を返す。"""

    def install(report_path: Path, edit, stdout: str = "", returncode: int = 0):
        def run(cmd, **kwargs):
            if cmd[0] == "git":
                return subprocess.CompletedProcess(cmd, 0, "", "")
            run.cmd = cmd
            report_path.write_text(
                edit(report_path.read_text(encoding="utf-8")), encoding="utf-8"
            )
            return subprocess.CompletedProcess(cmd, returncode, stdout, "error detail")

        monkeypatch.setattr(ai_draft.shutil, "which", lambda _: "/usr/bin/claude")
        monkeypatch.setattr(ai_draft.subprocess, "run", run)
        return run

    return install


# --- find_target_report -------------------------------------------------------


def test_find_target_report_latest_and_as_of(tmp_path: Path) -> None:
    ticker_dir = tmp_path / "7014"
    ticker_dir.mkdir()
    for name in [
        "2026-01-05_7014_initial.md",
        "2026-04-10_7014_review.md",
        "2026-04-10_7014_tables.md",
        "2026-05-01_7014_exit.md",
    ]:
        (ticker_dir / name).write_text("", encoding="utf-8")
    assert ai_draft.find_target_report(ticker_dir).name == "2026-04-10_7014_review.md"
    assert (
        ai_draft.find_target_report(ticker_dir, "2026-01-05").name
        == "2026-01-05_7014_initial.md"
    )
    with pytest.raises(DraftError):
        ai_draft.find_target_report(ticker_dir, "2026-05-01")
    with pytest.raises(DraftError):
        ai_draft.find_target_report(tmp_path / "9999")


# --- build_prompt -------------------------------------------------------------


def test_build_prompt_initial(report: Path) -> None:
    prompt = ai_draft.build_prompt(report, "2026-10-02", TODAY)
    assert str(report) in prompt
    assert "2026-10-02_7014_snapshot.json" in prompt
    assert "2026-10-02_7014_tables.md" in prompt
    assert "基準日 2026-10-02 以前" in prompt
    assert "## 出典（AI下書き）" in prompt
    assert "参照日 2026-10-10" in prompt
    assert "（AI下書き・要確認）" in prompt
    assert "kill_criteria 案" in prompt
    assert "親の初回レポート" not in prompt


def test_build_prompt_review_includes_parent(tmp_path: Path) -> None:
    ticker_dir = tmp_path / "7014"
    ticker_dir.mkdir()
    for name in [
        "2026-01-05_7014_initial.md",
        "2026-01-05_7014_snapshot.json",
        "2026-04-10_7014_review.md",
        "2026-04-10_7014_snapshot.json",
    ]:
        (ticker_dir / name).write_text("", encoding="utf-8")
    prompt = ai_draft.build_prompt(
        ticker_dir / "2026-04-10_7014_review.md", "2026-04-10", TODAY
    )
    assert "親の初回レポート(仮説・予測・撤退条件): " in prompt
    assert "2026-01-05_7014_initial.md" in prompt
    assert "2026-01-05_7014_snapshot.json" in prompt
    assert "thesis_status 案" in prompt


# --- validate_draft -----------------------------------------------------------


def test_validate_draft_accepts_body_edit() -> None:
    after = REPORT.replace("（記入）", "（AI下書き・要確認）造船会社[^1]。")
    assert ai_draft.validate_draft(REPORT, after) == []


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ('thesis: ""', 'thesis: "買い"', "frontmatter"),
        ("| 株価 |", "| 株価 | x |", "snapshot-summary"),
        ("<!-- auto:summary_card -->", "", "auto コメント"),
    ],
)
def test_validate_draft_detects_forbidden_changes(
    old: str, new: str, message: str
) -> None:
    problems = ai_draft.validate_draft(REPORT, REPORT.replace(old, new))
    assert len(problems) == 1
    assert message in problems[0]


# --- ensure_uncommitted -------------------------------------------------------


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
    )


def test_ensure_uncommitted(report: Path) -> None:
    ai_draft.ensure_uncommitted(report)  # git 管理外
    repo = report.parent.parent
    _git(repo, "init")
    ai_draft.ensure_uncommitted(report)  # 未コミット
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "add")
    with pytest.raises(DraftError, match="既にコミット"):
        ai_draft.ensure_uncommitted(report)


# --- run_draft ----------------------------------------------------------------


def test_run_draft_success(report: Path, fake_claude) -> None:
    run = fake_claude(
        report,
        lambda t: t.replace("（記入）", "（AI下書き・要確認）本文[^1]"),
        _envelope(PAYLOAD),
    )
    result = ai_draft.run_draft(report, "2026-10-02", TODAY)
    assert result.error is None
    assert result.summary == "下書きしました"
    assert result.sources[0].url == "https://example.com"
    assert "AI下書き" in report.read_text(encoding="utf-8")
    assert run.cmd[run.cmd.index("--model") + 1] == "opus"
    assert run.cmd[run.cmd.index("--permission-mode") + 1] == "dontAsk"


def test_run_draft_restores_on_forbidden_change(report: Path, fake_claude) -> None:
    fake_claude(
        report, lambda t: t.replace('thesis: ""', 'thesis: "x"'), _envelope(PAYLOAD)
    )
    result = ai_draft.run_draft(report, "2026-10-02", TODAY)
    assert result.error is not None and "元の内容に戻しました" in result.error
    assert report.read_text(encoding="utf-8") == REPORT


def test_run_draft_unchanged_file_is_error(report: Path, fake_claude) -> None:
    fake_claude(report, lambda t: t, _envelope(PAYLOAD))
    result = ai_draft.run_draft(report, "2026-10-02", TODAY)
    assert result.error == "ファイルが変更されませんでした"


def test_run_draft_nonzero_exit(report: Path, fake_claude) -> None:
    fake_claude(report, lambda t: t, "", returncode=1)
    result = ai_draft.run_draft(report, "2026-10-02", TODAY)
    assert result.error is not None and "終了コード 1" in result.error


def test_run_draft_timeout(report: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def run(cmd, **kwargs):
        if cmd[0] == "git":
            return subprocess.CompletedProcess(cmd, 0, "", "")
        raise subprocess.TimeoutExpired(cmd, 1)

    monkeypatch.setattr(ai_draft.shutil, "which", lambda _: "/usr/bin/claude")
    monkeypatch.setattr(ai_draft.subprocess, "run", run)
    result = ai_draft.run_draft(report, "2026-10-02", TODAY)
    assert result.error is not None and "タイムアウト" in result.error


def test_run_draft_without_claude(
    report: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ai_draft.shutil, "which", lambda _: None)
    result = ai_draft.run_draft(report, "2026-10-02", TODAY)
    assert result.error == "claude コマンドが見つかりません"
