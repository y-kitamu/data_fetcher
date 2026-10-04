"""Tests for the Claude Code review step used by notify_data_status.py.
`claude` itself is never invoked: subprocess.run is replaced by a fake."""

import datetime
import json
import os
import subprocess
from typing import Any

import pytest

from data_fetcher.core import ai_review
from data_fetcher.core.data_health import AnomalyItem

TODAY = datetime.date(2026, 10, 2)


def _envelope(payload: dict[str, Any] | None, **extra: Any) -> str:
    return json.dumps(
        {
            "type": "result",
            "is_error": False,
            "result": "",
            "structured_output": payload,
        }
        | extra
    )


def _fake_run(stdout: str = "", returncode: int = 0, stderr: str = ""):
    def run(cmd, **kwargs):
        run.cmd = cmd
        run.kwargs = kwargs
        return subprocess.CompletedProcess(cmd, returncode, stdout, stderr)

    return run


@pytest.fixture
def claude_on_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ai_review.shutil, "which", lambda _: "/usr/bin/claude")
    monkeypatch.setattr(ai_review, "list_log_files", lambda today: [])


# --- build_prompt -----------------------------------------------------------


def test_build_prompt_includes_status_and_anomalies() -> None:
    prompt = ai_review.build_prompt(
        {"gmo/tick": ("20261001", 3)},
        [AnomalyItem("fred", "critical", "全体停止", "データなし", ["DGS10"])],
        [],
        TODAY,
    )
    assert "gmo/tick: latest=20261001, count=3" in prompt
    assert "[critical] fred 全体停止: データなし (例: DGS10)" in prompt
    assert "2026-10-02" in prompt


# --- run_review -------------------------------------------------------------


def test_run_review_parses_structured_output(
    monkeypatch: pytest.MonkeyPatch, claude_on_path: None
) -> None:
    payload = {
        "status": "critical",
        "summary": "FRED が停止しています",
        "findings": [
            {
                "severity": "critical",
                "source": "fred",
                "issue": "スクリプトが見つからない",
                "evidence": "cron_fred.txt: not found",
                "action": "crontab を修正",
            }
        ],
    }
    fake = _fake_run(_envelope(payload))
    monkeypatch.setattr(ai_review.subprocess, "run", fake)

    result = ai_review.run_review({}, [], TODAY)

    assert result.error is None
    assert result.status == "critical"
    assert result.findings[0].source == "fred"
    assert fake.cmd[fake.cmd.index("--permission-mode") + 1] == "auto"
    assert fake.kwargs["timeout"] == ai_review.TIMEOUT_SECONDS


def test_run_review_falls_back_to_result_text(
    monkeypatch: pytest.MonkeyPatch, claude_on_path: None
) -> None:
    payload = {"status": "ok", "summary": "問題なし", "findings": []}
    stdout = _envelope(None, result=json.dumps(payload))
    monkeypatch.setattr(ai_review.subprocess, "run", _fake_run(stdout))

    result = ai_review.run_review({}, [], TODAY)

    assert result.status == "ok"
    assert result.findings == []


def test_run_review_reports_missing_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ai_review.shutil, "which", lambda _: None)
    result = ai_review.run_review({}, [], TODAY)
    assert result.status is None
    assert result.error is not None


def test_run_review_reports_timeout(
    monkeypatch: pytest.MonkeyPatch, claude_on_path: None
) -> None:
    def run(cmd, **kwargs):
        raise subprocess.TimeoutExpired(cmd, kwargs["timeout"])

    monkeypatch.setattr(ai_review.subprocess, "run", run)
    result = ai_review.run_review({}, [], TODAY)
    assert result.error is not None and "タイムアウト" in result.error


def test_run_review_reports_nonzero_exit(
    monkeypatch: pytest.MonkeyPatch, claude_on_path: None
) -> None:
    monkeypatch.setattr(
        ai_review.subprocess, "run", _fake_run(returncode=1, stderr="boom")
    )
    result = ai_review.run_review({}, [], TODAY)
    assert result.error is not None and "boom" in result.error


@pytest.mark.parametrize(
    "stdout",
    [
        "not json",
        _envelope({"status": "bogus", "summary": "", "findings": []}),
        _envelope({"summary": "status missing", "findings": []}),
        json.dumps({"is_error": True, "result": "api error"}),
    ],
)
def test_run_review_reports_bad_output(
    monkeypatch: pytest.MonkeyPatch, claude_on_path: None, stdout: str
) -> None:
    monkeypatch.setattr(ai_review.subprocess, "run", _fake_run(stdout))
    result = ai_review.run_review({}, [], TODAY)
    assert result.status is None
    assert result.error is not None


# --- rendering ----------------------------------------------------------------


def test_render_review_html_escapes_claude_output() -> None:
    result = ai_review.ReviewResult(
        status="warning",
        summary="<script>x</script>",
        findings=[ai_review.Finding("warning", "gmo", "<b>", "a&b", "-")],
    )
    rendered = ai_review.render_review_html(result)
    assert "<script>" not in rendered
    assert "&lt;script&gt;" in rendered
    assert "a&amp;b" in rendered
    assert "要確認" in rendered


def test_render_review_html_on_error() -> None:
    rendered = ai_review.render_review_html(ai_review.ReviewResult(error="timeout"))
    assert "AI チェックに失敗しました" in rendered


def test_subject_prefix() -> None:
    assert ai_review.subject_prefix(ai_review.ReviewResult(status="ok")) == ""
    assert ai_review.subject_prefix(ai_review.ReviewResult(error="x")) == ""
    assert (
        ai_review.subject_prefix(ai_review.ReviewResult(status="critical"))
        == "[要対応] "
    )


# --- list_log_files -------------------------------------------------------------


def test_list_log_files_skips_stale_logs(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    logs = tmp_path / "logs"
    logs.mkdir()
    fresh = logs / "cron_gmo.txt"
    stale = logs / "cron_gcp.txt"
    fresh.write_text("x")
    stale.write_text("x")
    old = datetime.datetime(2026, 4, 4).timestamp()
    os.utime(stale, (old, old))
    monkeypatch.setattr(ai_review, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(ai_review, "EXTERNAL_LOG_FILES", ())

    assert ai_review.list_log_files(datetime.date.today()) == [fresh]


# --- auto fixes ---------------------------------------------------------------


def test_run_review_parses_fixes(
    monkeypatch: pytest.MonkeyPatch, claude_on_path: None
) -> None:
    payload = {
        "status": "ok",
        "summary": "JPX のセレクタを修正しました",
        "findings": [],
        "fixes": [
            {
                "source": "jpx_stats",
                "problem": "リンクが見つからない",
                "change": "セレクタを修正",
                "files": "src/data_fetcher/domains/jpx_stats/api.py",
                "verification": "uv run python scripts/fetch_data_from_jpx_stats.py: OK",
                "verified": True,
            }
        ],
    }
    monkeypatch.setattr(ai_review.subprocess, "run", _fake_run(_envelope(payload)))

    result = ai_review.run_review({}, [], TODAY)

    assert result.error is None
    assert result.fixes[0].verified is True
    assert ai_review.subject_prefix(result) == "[自動修正あり] "
    rendered = ai_review.render_review_html(result)
    assert "自動修正" in rendered and "確認済み" in rendered


def test_subject_prefix_combines_status_and_fixes() -> None:
    result = ai_review.ReviewResult(
        status="warning",
        fixes=[ai_review.Fix("gmo", "p", "c", "f", "v", False)],
    )
    assert ai_review.subject_prefix(result) == "[要確認] [自動修正あり] "
    assert "未確認" in ai_review.render_review_html(result)


def test_build_prompt_forbids_crontab_and_other_repos() -> None:
    prompt = ai_review.build_prompt({}, [], [], TODAY)
    assert "crontab の変更" in prompt
    assert "src/ と scripts/ のみ" in prompt
