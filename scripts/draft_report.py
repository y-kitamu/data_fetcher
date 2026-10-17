"""new_report.py で生成した initial / review レポートの記入欄を、Claude Code
(`claude -p`) に事実を調査させて下書きするCLI。

使い方:
    uv run python scripts/draft_report.py <ticker> [--as-of YYYY-MM-DD] [--print-prompt]

--as-of を省略すると、その銘柄の initial / review のうちファイル名の日付が最新のものを対象にする。
レポートはその場で編集され、未コミットのまま残る。git diff で確認してからコミットすること。
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

import data_fetcher
from data_fetcher.domains.reports.ai_draft import (
    DraftError,
    build_prompt,
    find_target_report,
    run_draft,
)

_JST = dt.timezone(dt.timedelta(hours=9))


def _reports_dir() -> Path:
    override = os.environ.get("REPORTS_DIR")
    return (
        Path(override) if override else data_fetcher.constants.PROJECT_ROOT / "reports"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="draft_report", description=__doc__)
    parser.add_argument("ticker")
    parser.add_argument("--as-of", dest="as_of", default=None)
    parser.add_argument(
        "--print-prompt",
        action="store_true",
        help="claude を呼ばずにプロンプトを表示する",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    today = dt.datetime.now(_JST).date()
    try:
        md_path = find_target_report(_reports_dir() / args.ticker, args.as_of)
        as_of = md_path.name[:10]
        if args.print_prompt:
            print(build_prompt(md_path, as_of, today))
            return 0
        print(f"下書きを作成します（数十分かかることがあります）: {md_path}")
        result = run_draft(md_path, as_of, today)
    except DraftError as e:
        print(f"エラー: {e}")
        return 1

    if result.error is not None:
        print(f"エラー: {result.error}")
        return 1
    print(f"\n下書きを書き込みました: {md_path}\n")
    print(f"要約: {result.summary}\n")
    print("下書きした節:")
    for section in result.filled_sections:
        print(f"  - {section}")
    print("\n要確認事項:")
    for item in result.unresolved or ["(なし)"]:
        print(f"  - {item}")
    print("\n出典:")
    for s in result.sources:
        print(f"  - {s.title} {s.url}（参照日 {s.accessed}）")
    print(
        "\n「AI下書き」で検索して内容と出典を確認・修正し、採用する AI案 は frontmatter に"
        "転記してからコミットしてください（git diff で差分を確認できます）。"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
