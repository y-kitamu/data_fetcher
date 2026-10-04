"""ai_review.py - Ask Claude Code (`claude -p`) to review how the daily data
collection went, using the digest table, the rule-based anomalies from
`data_health`, the cron logs and, at its own discretion, the source websites.
Claude fixes simple problems in this repo's src/ and scripts/ itself and
re-runs the affected fetcher to verify; changes are left uncommitted for the
user to review. crontab, other repositories and anything needing a human
decision are only reported.

Used by scripts/notify_data_status.py to prepend an "AI チェック" section to
the daily digest email. Best-effort like `notify_to_gmail`: any failure
(CLI missing, timeout, non-zero exit, unparsable output) returns a
`ReviewResult` with `error` set so the digest is still sent without it.
"""

import datetime
import html
import json
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from .constants import PROJECT_ROOT
from .data_health import AnomalyItem

Status = Literal["ok", "warning", "critical"]

CLAUDE_MODEL = "sonnet"
TIMEOUT_SECONDS = 3600
STOCK_ROOT = PROJECT_ROOT.parent / "stock"
# boj/estat/fred/oecd are still scheduled from the stock repo's crontab entries.
EXTERNAL_LOG_FILES = tuple(
    STOCK_ROOT / "logs" / f"cron_{name}.txt"
    for name in ("boj", "estat", "fred", "oecd")
)
# Logs untouched for this long belong to retired/one-off jobs (cron_gcp.txt,
# *_backfill_*.log) and would only distract the review.
STALE_LOG_DAYS = 7

_STATUS_LABEL: dict[Status, str] = {
    "ok": "問題なし",
    "warning": "要確認",
    "critical": "要対応",
}
_STATUS_CLASS: dict[Status, str] = {
    "ok": "alert-info",
    "warning": "alert-warning",
    "critical": "alert-critical",
}
_SEVERITY_CLASS = {
    "critical": "alert-critical",
    "warning": "alert-warning",
    "info": "alert-info",
}

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["ok", "warning", "critical"]},
        "summary": {"type": "string"},
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "severity": {
                        "type": "string",
                        "enum": ["critical", "warning", "info"],
                    },
                    "source": {"type": "string"},
                    "issue": {"type": "string"},
                    "evidence": {"type": "string"},
                    "action": {"type": "string"},
                },
                "required": ["severity", "source", "issue", "evidence", "action"],
            },
        },
        "fixes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "source": {"type": "string"},
                    "problem": {"type": "string"},
                    "change": {"type": "string"},
                    "files": {"type": "string"},
                    "verification": {"type": "string"},
                    "verified": {"type": "boolean"},
                },
                "required": [
                    "source",
                    "problem",
                    "change",
                    "files",
                    "verification",
                    "verified",
                ],
            },
        },
    },
    "required": ["status", "summary", "findings", "fixes"],
}


@dataclass
class Finding:
    severity: str
    source: str
    issue: str
    evidence: str
    action: str


@dataclass
class Fix:
    source: str
    problem: str
    change: str
    files: str
    verification: str
    verified: bool


@dataclass
class ReviewResult:
    status: Status | None = None
    summary: str = ""
    findings: list[Finding] = field(default_factory=list)
    fixes: list[Fix] = field(default_factory=list)
    error: str | None = None


def list_log_files(today: datetime.date) -> list[Path]:
    candidates = sorted((PROJECT_ROOT / "logs").glob("*.txt")) + sorted(
        (PROJECT_ROOT / "logs").glob("*.log")
    )
    candidates += [p for p in EXTERNAL_LOG_FILES if p.exists()]
    threshold = today - datetime.timedelta(days=STALE_LOG_DAYS)
    return [
        p
        for p in candidates
        if datetime.date.fromtimestamp(p.stat().st_mtime) >= threshold
    ]


def _describe_log_files(paths: list[Path]) -> str:
    lines = []
    for p in paths:
        stat = p.stat()
        mtime = datetime.datetime.fromtimestamp(stat.st_mtime)
        lines.append(
            f"- {p.resolve()} (size: {stat.st_size / 1024 / 1024:.1f}MB, "
            f"last modified: {mtime:%Y-%m-%d %H:%M})"
        )
    return "\n".join(lines) if lines else "- (対象ログなし)"


def build_prompt(
    data_nums: dict[str, tuple[str, int]],
    anomaly_items: list[AnomalyItem],
    log_files: list[Path],
    today: datetime.date,
) -> str:
    status_lines = "\n".join(
        f"- {key}: latest={date}, count={count}"
        for key, (date, count) in sorted(data_nums.items())
    )
    anomaly_lines = (
        "\n".join(
            f"- [{item.severity}] {item.source} {item.kind}: {item.message}"
            + (f" (例: {', '.join(item.examples)})" if item.examples else "")
            for item in anomaly_items
        )
        or "- (なし)"
    )
    return f"""本日は {today.isoformat()} ({today:%a}) です。
data_fetcher リポジトリ ({PROJECT_ROOT}) は cron で各種データを定期収集しています。
直近の収集がうまくいっているかを調査し、簡単に直せる問題は修正して再収集で動作確認し、
結果を報告してください。

## 収集状況 (data/ 配下の最新日付・件数。per-ticker ディレクトリは最終更新日時と件数)
{status_lines}

## ルールベースの異常検知結果 (src/data_fetcher/core/data_health.py)
{anomaly_lines}

## 対象ログ
{_describe_log_files(log_files)}

## 調査手順
1. `crontab -l` と scripts/cron_*.sh で各収集ジョブのスケジュールと出力先ログを把握する。
2. 各ログの直近の実行分を確認し、例外・Traceback・HTTP エラー・認証エラー・
   "not found" などの失敗や、Start はあるが Finish がない中断を探す。
   ログは数百 MB あるものもあるため、ファイル全体を読まず `tail -n` や `grep` で
   直近の実行分に絞ること。
3. 収集状況の日付・件数が、各ジョブのスケジュール・市場の営業日 (土日祝)・
   過去の件数推移から見て妥当か確認する (data/ 配下を ls などで確認してよい)。
4. 必要に応じて取得元のウェブページ (JPX・日銀・FRED・OECD・e-Stat・TDnet など) を
   確認し、公開済みの最新データが取得できているかを確かめる。
5. ルールベースの異常検知結果が誤検知か、本当の問題かを判断する。

## 修正と再収集
見つけた問題が「簡単に直せる」場合は修正し、該当する取得スクリプトを再実行して
データが取得できることを確認すること。
- 修正してよいのは data_fetcher リポジトリ ({PROJECT_ROOT}) の src/ と scripts/ のみ。
- 簡単に直せる例: 取得元サイトの HTML 構造変更に伴うセレクタ・URL の修正、
  スクリプト内のパス誤り、明らかなバグの小さな修正。
- 以下は修正せず findings で報告するだけにすること:
  - crontab の変更が必要なもの (ジョブの追加・削除・パス修正・時刻変更)
  - stock リポジトリなど data_fetcher 以外の変更が必要なもの
  - ホストの停止・ネットワーク到達性・認証情報 (トークン・パスワード) の期限切れや不足
  - ジョブを意図的に止めたのか判断できないもの
  - 仕様判断が必要なもの、影響範囲の大きい修正 (複数モジュールにまたがる設計変更など)
  - データの削除や上書きを伴うもの
  - 複数ソースが共有する基盤コード (src/data_fetcher/core/) の変更が必要なもの
  - 常駐プロセス (WebSocket 収集など) の再起動が必要なもの
- 再収集は修正したソースのジョブだけを対象とし、scripts/cron_*.sh と同じコマンド
  (`uv run python scripts/...`) で実行すること。修正していないジョブは実行しないこと。
  同じジョブが cron で実行中でないか `ps` で確認してから実行すること。
- 再収集はフォアグラウンドで実行して終了を待つこと。nohup・& などでバックグラウンドに
  プロセスを残したまま終了しないこと。プロセスの kill・再起動もしないこと。
- 修正後に tests/ に関連テストがあれば `uv run pytest` で実行して確認すること。

## 制約
- 修正はすべて未コミット・未ステージのまま残すこと。git add・commit・push・stash・
  reset・checkout など、git の状態を変える操作は一切行わないこと。作業ツリーに既存の
  未コミット変更があっても触らないこと。
- crontab の変更、data_fetcher の src/・scripts/ 以外のファイルの変更 (/tmp への
  ファイル作成を含む。一時ファイルが必要なら logs/ を使うこと)、data/ 配下のファイルの
  削除は一切行わないこと。再収集の取得結果として data/ にファイルが追加・更新されるのは可。
- ユーザーへの質問はせず、最後まで自力で判断して結論を出すこと。

## 出力
指定の JSON スキーマで回答すること。すべて日本語で記述すること。
- status: 修正後の全体判定。未解決で対応が必要な失敗があれば critical、
  様子見でよい懸念のみなら warning、問題なし (すべて修正済みを含む) なら ok
- summary: 全体の要約 (2〜4 文)
- findings: 修正していない (またはできなかった) 問題点。問題がなければ空配列。
  evidence にはログの該当行やファイルパスなど根拠を、action にはユーザーに求める
  確認・推奨対応を書くこと
- fixes: 自動で行った修正。修正していなければ空配列。
  change に修正内容、files に変更したファイル、verification に再収集・テストの
  実行コマンドと結果、verified に再収集で取得を確認できたかを書くこと
"""


def _parse_output(stdout: str) -> ReviewResult:
    envelope = json.loads(stdout)
    if envelope.get("is_error"):
        return ReviewResult(error=f"claude returned an error: {envelope.get('result')}")
    payload = envelope.get("structured_output")
    if payload is None:
        payload = json.loads(envelope.get("result", ""))
    status = payload["status"]
    if status not in _STATUS_LABEL:
        raise ValueError(f"unknown status: {status}")
    return ReviewResult(
        status=status,
        summary=payload["summary"],
        findings=[Finding(**f) for f in payload.get("findings", [])],
        fixes=[Fix(**f) for f in payload.get("fixes", [])],
    )


def run_review(
    data_nums: dict[str, tuple[str, int]],
    anomaly_items: list[AnomalyItem],
    today: datetime.date | None = None,
) -> ReviewResult:
    today = today or datetime.date.today()
    claude_path = shutil.which("claude")
    if claude_path is None:
        return ReviewResult(error="claude コマンドが見つかりません")

    prompt = build_prompt(data_nums, anomaly_items, list_log_files(today), today)
    cmd = [
        claude_path,
        "-p",
        prompt,
        "--model",
        CLAUDE_MODEL,
        "--permission-mode",
        "auto",
        "--output-format",
        "json",
        "--json-schema",
        json.dumps(OUTPUT_SCHEMA),
        "--no-session-persistence",
    ]
    if STOCK_ROOT.exists():
        cmd += ["--add-dir", str(STOCK_ROOT / "logs")]
    try:
        proc = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=TIMEOUT_SECONDS,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        return ReviewResult(error=f"{TIMEOUT_SECONDS} 秒でタイムアウトしました")
    except OSError as e:
        return ReviewResult(error=f"claude の起動に失敗しました: {e}")
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip()[-500:]
        return ReviewResult(
            error=f"claude が終了コード {proc.returncode} で失敗しました: {detail}"
        )
    try:
        return _parse_output(proc.stdout)
    except (ValueError, KeyError, TypeError) as e:
        return ReviewResult(error=f"claude の出力を解析できませんでした: {e}")


def render_review_html(result: ReviewResult) -> str:
    if result.error is not None or result.status is None:
        return (
            "<h3>AI チェック</h3>\n"
            f"<p class='alert-warning'>AI チェックに失敗しました: "
            f"{html.escape(result.error or 'unknown error')}</p>"
        )
    esc = html.escape
    if result.findings:
        rows = "\n".join(
            f"<tr class='{_SEVERITY_CLASS.get(f.severity, 'alert-info')}'>"
            f"<td>{esc(f.severity)}</td><td>{esc(f.source)}</td><td>{esc(f.issue)}</td>"
            f"<td>{esc(f.evidence)}</td><td>{esc(f.action)}</td></tr>"
            for f in result.findings
        )
        table = f"""<table>
  <thead>
    <tr><th>重要度</th><th>Source</th><th>問題</th><th>根拠</th><th>推奨対応</th></tr>
  </thead>
  <tbody>
{rows}
  </tbody>
</table>"""
    else:
        table = ""
    if result.fixes:
        fix_rows = "\n".join(
            f"<tr class='{'alert-info' if f.verified else 'alert-warning'}'>"
            f"<td>{esc(f.source)}</td><td>{esc(f.problem)}</td><td>{esc(f.change)}</td>"
            f"<td>{esc(f.files)}</td><td>{esc(f.verification)}</td>"
            f"<td>{'確認済み' if f.verified else '未確認'}</td></tr>"
            for f in result.fixes
        )
        fix_table = f"""<h4>自動修正 (未コミット。レビューしてからコミットしてください)</h4>
<table>
  <thead>
    <tr><th>Source</th><th>問題</th><th>修正内容</th><th>変更ファイル</th><th>動作確認</th><th>再収集</th></tr>
  </thead>
  <tbody>
{fix_rows}
  </tbody>
</table>"""
    else:
        fix_table = ""
    return f"""<h3>AI チェック: <span class='{_STATUS_CLASS[result.status]}'>{_STATUS_LABEL[result.status]}</span></h3>
<p>{esc(result.summary)}</p>
{table}
{fix_table}"""


def subject_prefix(result: ReviewResult) -> str:
    prefix = ""
    if result.status in ("warning", "critical"):
        prefix += f"[{_STATUS_LABEL[result.status]}] "
    if result.fixes:
        prefix += "[自動修正あり] "
    return prefix
