"""Reading the watchlist YAML files and editing them as text.

reports/_config/watchlist.yaml holds active entries only; removed entries
move to reports/_config/watchlist_archive/<year removed>.yaml. Edits are
done on the raw text (append a block / cut a block) instead of
load-and-dump so that the comments people write in the files survive.
Every edit is re-parsed and checked before it is returned, so a layout the
cutter does not understand is rejected instead of silently corrupting the
file.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

CONFIG_DIR = "_config"
WATCHLIST_FILE = "watchlist.yaml"
ARCHIVE_DIR = "watchlist_archive"
SCHEMA_FILE = "watchlist.schema.json"

ACTIVE_HEADER = (
    "# yaml-language-server: $schema=./watchlist.schema.json\n"
    "# ウォッチリスト（active な銘柄のみ）。書式は reports/README.md を参照。\n"
    "# 追加: new_watch.py add <ticker> / 外す: new_watch.py remove <ticker> --reason \"...\"\n"
    "# 検証: new_watch.py check\n"
)
ARCHIVE_HEADER = (
    "# yaml-language-server: $schema=../watchlist.schema.json\n"
    "# ウォッチリストから外した銘柄の履歴（外した年ごと）。new_watch.py remove が追記する。\n"
)

_ENTRY_START_RE = re.compile(r"^-( +)\S")


class WatchlistFileError(ValueError):
    pass


def watchlist_path(reports_dir: Path) -> Path:
    return reports_dir / CONFIG_DIR / WATCHLIST_FILE


def archive_dir(reports_dir: Path) -> Path:
    return reports_dir / CONFIG_DIR / ARCHIVE_DIR


def archive_path(reports_dir: Path, year: int) -> Path:
    return archive_dir(reports_dir) / f"{year}.yaml"


def schema_path(reports_dir: Path) -> Path:
    return reports_dir / CONFIG_DIR / SCHEMA_FILE


def archive_files(reports_dir: Path) -> list[Path]:
    directory = archive_dir(reports_dir)
    return sorted(directory.glob("*.yaml")) if directory.exists() else []


def parse_entries(text: str, source: str) -> list:
    """YAMLテキストをエントリのリストとして読む。空ファイル（コメントのみ）は空リスト。"""
    try:
        data = YAML(typ="safe").load(text)
    except YAMLError as e:
        raise WatchlistFileError(f"{source}: YAMLとして読めません: {e}") from e
    if data is None:
        return []
    if not isinstance(data, list):
        raise WatchlistFileError(f"{source}: 先頭から `- ticker: ...` のリストで書いてください")
    return data


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def load_entries(path: Path) -> list:
    return parse_entries(read_text(path), str(path))


def load_active(reports_dir: Path) -> list:
    return load_entries(watchlist_path(reports_dir))


def load_archive(reports_dir: Path) -> list:
    entries = []
    for path in archive_files(reports_dir):
        entries.extend(load_entries(path))
    return entries


def render_entry_template(ticker: str, added: dt.date) -> str:
    return (
        f'- ticker: "{ticker}"\n'
        f"  added: {added.isoformat()}\n"
        f"  # 想定シナリオと買いの根拠を2〜3行で。詳しい根拠は reports/{ticker}/ のレポートに書く\n"
        "  scenario: |\n"
        "    TODO\n"
        "  # 全て満たしたら通知（AND）。metric 一覧: new_watch.py check --list-metrics\n"
        '  # 例: - {text: 75日線を下回る, metric: price.vs_ma75, op: "<", value: 0}\n'
        "  buy_conditions:\n"
        "  # hold のときだけ評価する\n"
        '  # 例: - {text: ピークから-15%, metric: price.drawdown_from_peak, op: "<=", value: -0.15}\n'
        "  sell_conditions:\n"
    )


def append_block(text: str, block: str, header: str) -> str:
    """既存テキストの末尾にエントリのブロックを追記する（既存部分には触れない）。"""
    if not text.strip():
        return header + "\n" + block
    if not text.endswith("\n"):
        text += "\n"
    return text + "\n" + block


def _entry_blocks(lines: list[str]) -> list[tuple[int, int]]:
    """`- ` で始まる行から次のエントリ直前までを1ブロックとする。ブロック末尾の空行と
    行頭コメントは次のエントリの見出しとみなしてブロックに含めない。
    """
    starts = [i for i, line in enumerate(lines) if _ENTRY_START_RE.match(line)]
    blocks = []
    for n, start in enumerate(starts):
        end = starts[n + 1] if n + 1 < len(starts) else len(lines)
        while end > start + 1 and (not lines[end - 1].strip() or lines[end - 1].startswith("#")):
            end -= 1
        blocks.append((start, end))
    return blocks


def _tickers(entries: list) -> list:
    return [e.get("ticker") if isinstance(e, dict) else None for e in entries]


def cut_entry(text: str, ticker: str, source: str) -> tuple[str, str]:
    """テキストから ticker のエントリを切り取り、(残りのテキスト, 切り取ったブロック) を返す。"""
    entries = parse_entries(text, source)
    lines = text.splitlines(keepends=True)
    blocks = _entry_blocks(lines)
    if len(blocks) != len(entries):
        raise WatchlistFileError(
            f"{source}: エントリの区切りを判定できません（各エントリは行頭の `- ticker:` で始めてください）"
        )
    matches = [n for n, t in enumerate(_tickers(entries)) if t == ticker]
    if not matches:
        raise WatchlistFileError(f"{source}: {ticker} は登録されていません")
    if len(matches) > 1:
        raise WatchlistFileError(f"{source}: {ticker} が重複しています。手で1件にまとめてから実行してください")

    start, end = blocks[matches[0]]
    block = "".join(lines[start:end])
    if not block.endswith("\n"):
        block += "\n"
    rest = "".join(lines[:start] + lines[end:])

    expected = _tickers(entries)
    del expected[matches[0]]
    if _tickers(parse_entries(rest, source)) != expected or _tickers(
        parse_entries(block, source)
    ) != [ticker]:
        raise WatchlistFileError(f"{source}: {ticker} のエントリを安全に切り取れません。手で確認してください")
    return rest, block


def mark_removed(block: str, removed: dt.date, reason: str) -> str:
    match = _ENTRY_START_RE.match(block)
    if match is None:
        raise WatchlistFileError("エントリのブロックが `- ` で始まっていません")
    indent = " " * (1 + len(match.group(1)))
    marked = (
        block.rstrip("\n")
        + f"\n{indent}removed: {removed.isoformat()}"
        + f"\n{indent}removed_reason: {json.dumps(reason, ensure_ascii=False)}\n"
    )
    entries = parse_entries(marked, "archive")
    if len(entries) != 1 or entries[0].get("removed") != removed or entries[0].get("removed_reason") != reason:
        raise WatchlistFileError("removed / removed_reason を安全に追記できません。手で確認してください")
    return marked


def add_to_text(active_text: str, ticker: str, added: dt.date) -> str:
    if ticker in _tickers(parse_entries(active_text, WATCHLIST_FILE)):
        raise WatchlistFileError(f"{ticker} は既にウォッチリストにあります")
    return append_block(active_text, render_entry_template(ticker, added), ACTIVE_HEADER)


def remove_from_text(
    active_text: str, archive_text: str, ticker: str, removed: dt.date, reason: str
) -> tuple[str, str]:
    """(新しい watchlist.yaml, 新しい archive/<年>.yaml) のテキストを返す。"""
    rest, block = cut_entry(active_text, ticker, WATCHLIST_FILE)
    new_archive = append_block(archive_text, mark_removed(block, removed, reason), ARCHIVE_HEADER)
    parse_entries(new_archive, f"{ARCHIVE_DIR}/{removed.year}.yaml")
    return rest, new_archive
