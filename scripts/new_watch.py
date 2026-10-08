"""ウォッチリスト（reports/_config/watchlist.yaml）を編集・検証するCLI。

使い方:
    uv run python scripts/new_watch.py add    <ticker> [--dry-run]
    uv run python scripts/new_watch.py remove <ticker> --reason "..." [--dry-run]
    uv run python scripts/new_watch.py sync   [--dry-run]
    uv run python scripts/new_watch.py check  [--list-metrics]
    uv run python scripts/new_watch.py schema
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

import data_fetcher
from data_fetcher.domains.watchlist.check import check_watchlist
from data_fetcher.domains.watchlist.metrics import METRICS
from data_fetcher.domains.watchlist.report_status import (
    ARCHIVED_STATUSES,
    report_statuses,
)
from data_fetcher.domains.watchlist.schema import TICKER_RE, render_json_schema
from data_fetcher.domains.watchlist.store import (
    WatchlistFileError,
    add_to_text,
    archive_path,
    load_active,
    load_archive,
    read_text,
    remove_from_text,
    schema_path,
    watchlist_path,
)

_JST = dt.timezone(dt.timedelta(hours=9))


def _reports_dir() -> Path:
    override = os.environ.get("REPORTS_DIR")
    return (
        Path(override) if override else data_fetcher.constants.PROJECT_ROOT / "reports"
    )


def _today() -> dt.date:
    return dt.datetime.now(_JST).date()


class _Edits:
    """複数ファイルへの変更をメモリ上で積み上げ、最後にまとめて書く（途中で失敗したら何も書かない）。"""

    def __init__(self, reports_dir: Path):
        self.reports_dir = reports_dir
        self.texts: dict[Path, str] = {}

    def text(self, path: Path) -> str:
        if path not in self.texts:
            self.texts[path] = read_text(path)
        return self.texts[path]

    def add(self, ticker: str, added: dt.date) -> None:
        path = watchlist_path(self.reports_dir)
        self.texts[path] = add_to_text(self.text(path), ticker, added)

    def remove(self, ticker: str, removed: dt.date, reason: str) -> None:
        active = watchlist_path(self.reports_dir)
        archive = archive_path(self.reports_dir, removed.year)
        self.texts[active], self.texts[archive] = remove_from_text(
            self.text(active), self.text(archive), ticker, removed, reason
        )

    def commit(self, dry_run: bool) -> None:
        # archive を先に書く。active の書き込みで失敗しても、エントリが消えることはない
        active = watchlist_path(self.reports_dir)
        for path in sorted(self.texts, key=lambda p: p == active):
            if read_text(path) == self.texts[path]:
                continue
            if dry_run:
                print(f"[dry-run] 更新予定: {path}")
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(self.texts[path], encoding="utf-8")
            print(f"更新しました: {path}")


def _print_past_watches(reports_dir: Path, ticker: str) -> None:
    past = [e for e in load_archive(reports_dir) if isinstance(e, dict) and e.get("ticker") == ticker]
    if not past:
        return
    print(f"{ticker} は過去に {len(past)} 回ウォッチしています:")
    for e in sorted(past, key=lambda e: str(e.get("removed"))):
        print(f"  {e.get('added')} 〜 {e.get('removed')}: {e.get('removed_reason')}")


def _run_add(args, reports_dir: Path) -> int:
    if not TICKER_RE.match(args.ticker):
        print(f'エラー: ticker は4桁のコードで指定してください（例: 7014, 278A）: {args.ticker}')
        return 1
    status = report_statuses(reports_dir).get(args.ticker)
    if status is not None and status.status in ARCHIVED_STATUSES:
        print(f"警告: {args.ticker} のレポートの status は {status.status} です（{status.path.name}）。")
    _print_past_watches(reports_dir, args.ticker)
    edits = _Edits(reports_dir)
    edits.add(args.ticker, _today())
    edits.commit(args.dry_run)
    print("scenario と buy_conditions / sell_conditions を記入してください。")
    return 0


def _run_remove(args, reports_dir: Path) -> int:
    edits = _Edits(reports_dir)
    edits.remove(args.ticker, _today(), args.reason)
    edits.commit(args.dry_run)
    return 0


def _run_sync(args, reports_dir: Path) -> int:
    active = {e.get("ticker") for e in load_active(reports_dir) if isinstance(e, dict)}
    today = _today()
    edits = _Edits(reports_dir)
    changed = False
    for ticker, rs in report_statuses(reports_dir).items():
        if rs.status == "watch" and ticker not in active:
            print(f"追加: {ticker}（レポートの status: watch）")
            edits.add(ticker, today)
            changed = True
        elif rs.status in ARCHIVED_STATUSES and ticker in active:
            print(f"archive へ移動: {ticker}（レポートの status: {rs.status}）")
            edits.remove(ticker, today, f"sync: レポートの status が {rs.status}（{rs.path.name}）")
            changed = True
        elif rs.status == "hold" and ticker not in active:
            print(f"警告: {ticker} は hold ですが active にありません。add で追加し sell_conditions を記入してください。")
    if not changed:
        print("変更はありません。")
    edits.commit(args.dry_run)
    return 0


def _run_check(args, reports_dir: Path) -> int:
    if args.list_metrics:
        width = max(len(name) for name in METRICS)
        for m in METRICS.values():
            print(f"{m.name:<{width}}  {m.description}")
        return 0
    result = check_watchlist(reports_dir)
    for message in result.errors:
        print(f"エラー: {message}")
    for message in result.warnings:
        print(f"警告: {message}")
    print(f"エラー {len(result.errors)}件 / 警告 {len(result.warnings)}件")
    return 1 if result.errors else 0


def _run_schema(args, reports_dir: Path) -> int:
    path = schema_path(reports_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_json_schema(), encoding="utf-8")
    print(f"書き出しました: {path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="new_watch", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p_add = sub.add_parser("add", help="雛形を watchlist.yaml の末尾に追記する")
    p_add.add_argument("ticker")
    p_add.add_argument("--dry-run", action="store_true")

    p_remove = sub.add_parser("remove", help="エントリに removed / removed_reason を付けて archive へ移す")
    p_remove.add_argument("ticker")
    p_remove.add_argument("--reason", required=True)
    p_remove.add_argument("--dry-run", action="store_true")

    p_sync = sub.add_parser("sync", help="レポートの status に合わせて追加・archive 移動する")
    p_sync.add_argument("--dry-run", action="store_true")

    p_check = sub.add_parser("check", help="書式の検証とレポート status との不整合の警告")
    p_check.add_argument("--list-metrics", action="store_true", help="使える metric の一覧を表示する")

    sub.add_parser("schema", help="JSON Schema（watchlist.schema.json）を書き出す")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    reports_dir = _reports_dir()
    commands = {
        "add": _run_add,
        "remove": _run_remove,
        "sync": _run_sync,
        "check": _run_check,
        "schema": _run_schema,
    }
    try:
        return commands[args.command](args, reports_dir)
    except WatchlistFileError as e:
        print(f"エラー: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
