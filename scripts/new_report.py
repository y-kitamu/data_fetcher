"""企業分析レポートのスナップショット(JSON)とMarkdown雛形を生成するCLI。

使い方:
    uv run python scripts/new_report.py initial <ticker> [--as-of YYYY-MM-DD] [--peers 7003,7014] [--dry-run]
    uv run python scripts/new_report.py review  <ticker> [--as-of YYYY-MM-DD] [--trigger earnings|disclosure|price_move|scheduled] [--dry-run]
    uv run python scripts/new_report.py exit    <ticker> [--as-of YYYY-MM-DD] [--dry-run]
    uv run python scripts/new_report.py snapshot <ticker> [--as-of YYYY-MM-DD] [--peers ...]
    uv run python scripts/new_report.py tables  <ticker> [--as-of YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

from ruamel.yaml import YAML

import data_fetcher
from data_fetcher.domains.reports import data_access
from data_fetcher.domains.reports.rubric import load_rubric_config
from data_fetcher.domains.reports.snapshot_builder import build_snapshot
from data_fetcher.domains.reports.snapshot_schema import Snapshot
from data_fetcher.domains.reports.tables import render_tables_markdown
from data_fetcher.domains.reports.trades import read_trades_csv, remaining_shares
from data_fetcher.domains.reports.writer import (
    check_not_exists,
    insert_summary_block,
    render_summary_table,
    split_frontmatter,
    update_frontmatter,
    write_new_file,
)

_JST = dt.timezone(dt.timedelta(hours=9))


def _reports_dir() -> Path:
    override = os.environ.get("REPORTS_DIR")
    return (
        Path(override) if override else data_fetcher.constants.PROJECT_ROOT / "reports"
    )


def _resolve_as_of(as_of_arg: str | None, ticker: str) -> dt.date:
    if as_of_arg:
        return dt.date.fromisoformat(as_of_arg)
    today = dt.datetime.now(_JST).date()
    _price, price_date = data_access.get_latest_price(ticker, today)
    if price_date is not None and price_date != today.isoformat():
        print(
            f"当日の株価終値がまだ無いため、直近の営業日({price_date})をas_ofとして使用します。"
        )
        return dt.date.fromisoformat(price_date)
    return today


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as f:
        return YAML(typ="safe").load(f) or {}


def _parse_frontmatter_dict(path: Path) -> dict:
    fm_text, _body = split_frontmatter(path.read_text(encoding="utf-8"))
    return YAML(typ="safe").load(fm_text) or {}


def _find_latest(ticker_dir: Path, pattern: str) -> Path | None:
    if not ticker_dir.exists():
        return None
    candidates = sorted(ticker_dir.glob(pattern))
    return candidates[-1] if candidates else None


def _dump_snapshot_json(snapshot) -> str:
    return json.dumps(snapshot.model_dump(mode="json"), ensure_ascii=False, indent=2)


def _tables_path(snapshot_path: Path) -> Path:
    return snapshot_path.with_name(
        snapshot_path.name.replace("_snapshot.json", "_tables.md")
    )


def _write_tables(snapshot: Snapshot, snapshot_path: Path) -> None:
    """snapshot.json の表をMarkdown(_tables.md)に書き出す。snapshot.json から
    導出するだけのファイルなので、上書き禁止の対象にはせず再生成で上書きする。
    """
    previous_path = (
        snapshot_path.with_name(snapshot.previous_snapshot)
        if snapshot.previous_snapshot
        else None
    )
    previous_snapshot_data = (
        json.loads(previous_path.read_text(encoding="utf-8"))
        if previous_path is not None and previous_path.exists()
        else None
    )
    tables_path = _tables_path(snapshot_path)
    tables_path.write_text(
        render_tables_markdown(snapshot, previous_snapshot_data), encoding="utf-8"
    )
    print(f"書き出しました: {tables_path}")


def _run_tables(args, ticker_dir: Path) -> int:
    pattern = f"{args.as_of}_snapshot.json" if args.as_of else "*_snapshot.json"
    snapshot_paths = sorted(ticker_dir.glob(pattern)) if ticker_dir.exists() else []
    if not snapshot_paths:
        print(f"エラー: {ticker_dir} に {pattern} が見つかりません。")
        return 1
    for snapshot_path in snapshot_paths:
        snapshot = Snapshot.model_validate_json(
            snapshot_path.read_text(encoding="utf-8")
        )
        _write_tables(snapshot, snapshot_path)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="new_report", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("ticker")
        p.add_argument("--as-of", dest="as_of", default=None)
        p.add_argument("--dry-run", action="store_true")

    p_initial = sub.add_parser("initial")
    add_common(p_initial)
    p_initial.add_argument("--peers", default=None)

    p_review = sub.add_parser("review")
    add_common(p_review)
    p_review.add_argument(
        "--trigger",
        choices=["earnings", "disclosure", "price_move", "scheduled"],
        default="scheduled",
    )

    p_exit = sub.add_parser("exit")
    add_common(p_exit)

    p_snapshot = sub.add_parser("snapshot")
    add_common(p_snapshot)
    p_snapshot.add_argument("--peers", default=None)

    p_tables = sub.add_parser("tables")
    p_tables.add_argument("ticker")
    p_tables.add_argument("--as-of", dest="as_of", default=None)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    reports_dir = _reports_dir()
    if args.command == "tables":
        return _run_tables(args, reports_dir / args.ticker)

    as_of = _resolve_as_of(args.as_of, args.ticker)
    peer_tickers = args.peers.split(",") if getattr(args, "peers", None) else None

    rubric_config = load_rubric_config(reports_dir / "_config" / "rubric_v1.yaml")
    leading_indicators_config = _load_yaml(
        reports_dir / "_config" / "leading_indicators.yaml"
    )

    ticker_dir = reports_dir / args.ticker
    date_str = as_of.isoformat()

    if args.command == "snapshot":
        snapshot = build_snapshot(
            args.ticker,
            as_of,
            "initial",
            rubric_config,
            peer_tickers=peer_tickers,
            leading_indicators_config=leading_indicators_config,
        )
        print(_dump_snapshot_json(snapshot))
        return 0

    if args.command == "initial":
        return _run_initial(
            args,
            reports_dir,
            ticker_dir,
            date_str,
            as_of,
            peer_tickers,
            rubric_config,
            leading_indicators_config,
        )
    if args.command == "review":
        return _run_review(
            args,
            reports_dir,
            ticker_dir,
            date_str,
            as_of,
            rubric_config,
            leading_indicators_config,
        )
    if args.command == "exit":
        return _run_exit(
            args,
            reports_dir,
            ticker_dir,
            date_str,
            as_of,
            rubric_config,
            leading_indicators_config,
        )
    return 1


def _run_initial(
    args,
    reports_dir,
    ticker_dir,
    date_str,
    as_of,
    peer_tickers,
    rubric_config,
    leading_indicators_config,
) -> int:
    existing_initials = (
        sorted(ticker_dir.glob("*_initial.md")) if ticker_dir.exists() else []
    )
    if existing_initials:
        print(
            f"警告: {args.ticker} には既に initial レポートがあります"
            f"({[p.name for p in existing_initials]})。再分析として続行します。"
        )

    snapshot = build_snapshot(
        args.ticker,
        as_of,
        "initial",
        rubric_config,
        peer_tickers=peer_tickers,
        leading_indicators_config=leading_indicators_config,
    )

    snapshot_path = ticker_dir / f"{date_str}_snapshot.json"
    md_path = ticker_dir / f"{date_str}_initial.md"
    template_text = (reports_dir / "_templates" / "initial.md").read_text(
        encoding="utf-8"
    )

    next_review = (as_of + dt.timedelta(days=90)).isoformat()
    risk_reward = snapshot.value_range.risk_reward
    updates: dict[str, object] = {
        "ticker": args.ticker,
        "as_of": date_str,
        "snapshot": snapshot_path.name,
        "company_name": snapshot.company_name or "",
        "next_review": next_review,
        "valuation.price": snapshot.market.price,
        "valuation.liquidation": snapshot.value_range.liquidation,
        "valuation.dcf_bear": snapshot.dcf.bear_per_share if snapshot.dcf else None,
        "valuation.dcf_bull": snapshot.dcf.bull_per_share if snapshot.dcf else None,
        "valuation.normalized": snapshot.value_range.normalized,
        "valuation.peak": snapshot.value_range.peak,
        "valuation.risk_reward": risk_reward
        if isinstance(risk_reward, (int, float))
        else None,
    }
    for item in [
        "asset_value",
        "earnings_value",
        "financial_health",
        "profitability",
        "growth",
        "cyclicality",
        "survival",
        "reversal",
    ]:
        updates[f"ratings.{item}.auto"] = getattr(snapshot.auto_ratings, item)

    final_md = _apply_updates_and_summary(template_text, updates, snapshot)

    if args.dry_run:
        print(f"[dry-run] 作成予定: {snapshot_path}")
        print(f"[dry-run] 作成予定: {md_path}")
        print(f"[dry-run] 作成予定: {_tables_path(snapshot_path)}")
        print(render_summary_table(snapshot))
        return 0

    check_not_exists(snapshot_path, md_path)
    write_new_file(snapshot_path, _dump_snapshot_json(snapshot))
    write_new_file(md_path, final_md)
    print(f"作成しました: {snapshot_path}")
    print(f"作成しました: {md_path}")
    _write_tables(snapshot, snapshot_path)
    print(f"warnings: {len(snapshot.warnings)}件")
    return 0


def _run_review(
    args,
    reports_dir,
    ticker_dir,
    date_str,
    as_of,
    rubric_config,
    leading_indicators_config,
) -> int:
    parent_path = _find_latest(ticker_dir, "*_initial.md")
    if parent_path is None:
        print(
            f"エラー: {args.ticker} の initial レポートが見つかりません。先に new_report initial を実行してください。"
        )
        return 1
    parent_data = _parse_frontmatter_dict(parent_path)

    prev_snapshot_path = _find_latest(ticker_dir, "*_snapshot.json")
    previous_snapshot_data = (
        json.loads(prev_snapshot_path.read_text(encoding="utf-8"))
        if prev_snapshot_path
        else None
    )

    snapshot = build_snapshot(
        args.ticker,
        as_of,
        "review",
        rubric_config,
        leading_indicators_config=leading_indicators_config,
        previous_snapshot_filename=prev_snapshot_path.name
        if prev_snapshot_path
        else None,
        kill_criteria=parent_data.get("kill_criteria", []),
    )

    snapshot_path = ticker_dir / f"{date_str}_snapshot.json"
    md_path = ticker_dir / f"{date_str}_review.md"
    template_text = (reports_dir / "_templates" / "review.md").read_text(
        encoding="utf-8"
    )

    period = f"{snapshot.fiscal_years[-1]}期" if snapshot.fiscal_years else ""
    updates = {
        "ticker": args.ticker,
        "as_of": date_str,
        "snapshot": snapshot_path.name,
        "parent": parent_path.name,
        "trigger": args.trigger,
        "period": period,
        "next_review": (as_of + dt.timedelta(days=90)).isoformat(),
        "kill_criteria_hit": [c.text for c in snapshot.kill_criteria_check if c.hit],
    }

    final_md = _apply_updates_and_summary(
        template_text, updates, snapshot, previous_snapshot_data
    )

    if args.dry_run:
        print(f"[dry-run] 作成予定: {snapshot_path}")
        print(f"[dry-run] 作成予定: {md_path}")
        print(f"[dry-run] 作成予定: {_tables_path(snapshot_path)}")
        print(render_summary_table(snapshot, previous_snapshot_data))
        return 0

    check_not_exists(snapshot_path, md_path)
    write_new_file(snapshot_path, _dump_snapshot_json(snapshot))
    write_new_file(md_path, final_md)
    print(f"作成しました: {snapshot_path}")
    print(f"作成しました: {md_path}")
    _write_tables(snapshot, snapshot_path)
    print(f"warnings: {len(snapshot.warnings)}件")
    return 0


def _run_exit(
    args,
    reports_dir,
    ticker_dir,
    date_str,
    as_of,
    rubric_config,
    leading_indicators_config,
) -> int:
    parent_path = _find_latest(ticker_dir, "*_initial.md")
    if parent_path is None:
        print(f"エラー: {args.ticker} の initial レポートが見つかりません。")
        return 1

    trades_path = ticker_dir / "trades.csv"
    if not trades_path.exists():
        print(
            f"エラー: {trades_path} が見つかりません。exitには売買記録(trades.csv)が必要です。"
        )
        return 1
    trades_df = read_trades_csv(trades_path)
    if remaining_shares(trades_df) != 0:
        print(
            "エラー: 保有株数が0ではありません(全部売っていません)。exitレポートは作成できません。"
        )
        return 1

    prev_snapshot_path = _find_latest(ticker_dir, "*_snapshot.json")

    snapshot = build_snapshot(
        args.ticker,
        as_of,
        "exit",
        rubric_config,
        leading_indicators_config=leading_indicators_config,
        previous_snapshot_filename=prev_snapshot_path.name
        if prev_snapshot_path
        else None,
        trades_df=trades_df,
    )

    snapshot_path = ticker_dir / f"{date_str}_snapshot.json"
    md_path = ticker_dir / f"{date_str}_exit.md"
    template_text = (reports_dir / "_templates" / "exit.md").read_text(encoding="utf-8")

    updates: dict[str, object] = {
        "ticker": args.ticker,
        "as_of": date_str,
        "snapshot": snapshot_path.name,
        "parent": parent_path.name,
    }
    if snapshot.result is not None:
        for key in [
            "avg_buy_price",
            "avg_sell_price",
            "return_pct",
            "holding_days",
            "benchmark_return_pct",
            "excess_return_pct",
            "max_drawdown_pct",
        ]:
            updates[f"result.{key}"] = getattr(snapshot.result, key)

    final_md = _apply_updates(template_text, updates)

    if args.dry_run:
        print(f"[dry-run] 作成予定: {snapshot_path}")
        print(f"[dry-run] 作成予定: {md_path}")
        print(f"[dry-run] 作成予定: {_tables_path(snapshot_path)}")
        return 0

    check_not_exists(snapshot_path, md_path)
    write_new_file(snapshot_path, _dump_snapshot_json(snapshot))
    write_new_file(md_path, final_md)
    print(f"作成しました: {snapshot_path}")
    print(f"作成しました: {md_path}")
    _write_tables(snapshot, snapshot_path)
    print(f"warnings: {len(snapshot.warnings)}件")
    return 0


def _apply_updates_and_summary(
    template_text, updates, snapshot, previous_snapshot_data=None
) -> str:
    """initial/review用: frontmatterを更新し、summary-blockマーカーに表を挿入する。
    exit.mdにはsummary-blockマーカーが無い(§9.2は initial/review のみが対象)ため、
    exitでは代わりに _apply_updates を使うこと。
    """
    updated_text = update_frontmatter(template_text, updates)
    fm, body = split_frontmatter(updated_text)
    body_with_summary = insert_summary_block(
        body, render_summary_table(snapshot, previous_snapshot_data)
    )
    return f"---\n{fm}---\n{body_with_summary}"


def _apply_updates(template_text, updates) -> str:
    return update_frontmatter(template_text, updates)


if __name__ == "__main__":
    sys.exit(main())
