"""Entry validation and JSON Schema generation for the watchlist YAML.

`validate_entry` is the authoritative check (used by `new_watch.py check`);
`build_json_schema` mirrors it for editor completion via the VS Code YAML
extension. Both read the field and metric definitions from this package.
"""

from __future__ import annotations

import datetime as dt
import json
import re

from .metrics import METRICS, OPS

TICKER_RE = re.compile(r"^[0-9][0-9A-Z]{3}$")
ENTRY_KEYS = ["ticker", "added", "scenario", "buy_conditions", "sell_conditions"]
ARCHIVE_KEYS = ["removed", "removed_reason"]
CONDITION_KEYS = ["text", "metric", "op", "value"]
CONDITION_LISTS = ["buy_conditions", "sell_conditions"]


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_condition(cond: object, where: str) -> list[str]:
    if not isinstance(cond, dict):
        return [f"{where}: {{text, metric, op, value}} の形式で書いてください"]
    errors = []
    for key in cond:
        if key not in CONDITION_KEYS:
            errors.append(f"{where}: 不明なキー {key!r}")
    for key in CONDITION_KEYS:
        if key not in cond:
            errors.append(f"{where}: {key} がありません")
    if "text" in cond and not isinstance(cond["text"], str):
        errors.append(f"{where}.text: 文字列で書いてください")
    if "metric" in cond and cond["metric"] not in METRICS:
        errors.append(
            f"{where}.metric: 未定義の metric {cond['metric']!r}"
            "（一覧: new_watch.py check --list-metrics）"
        )
    if "op" in cond and cond["op"] not in OPS:
        errors.append(f"{where}.op: {cond['op']!r} は使えません（{' '.join(OPS)}）")
    if "value" in cond and not _is_number(cond["value"]):
        errors.append(f"{where}.value: 数値で書いてください")
    return errors


def validate_entry(entry: object, where: str, archived: bool) -> list[str]:
    """エントリ1件の書式エラーを返す。archived=True なら removed / removed_reason を必須にする。"""
    if not isinstance(entry, dict):
        return [f"{where}: `- ticker: ...` で始まるマッピングで書いてください"]
    errors = []
    allowed = ENTRY_KEYS + (ARCHIVE_KEYS if archived else [])
    for key in entry:
        if key not in allowed:
            hint = "（外すときは new_watch.py remove を使う）" if key in ARCHIVE_KEYS else ""
            errors.append(f"{where}: 不明なキー {key!r}{hint}")
    required = ["ticker", "added"] + (ARCHIVE_KEYS if archived else [])
    for key in required:
        if key not in entry:
            errors.append(f"{where}: {key} がありません")

    ticker = entry.get("ticker")
    if "ticker" in entry and not (isinstance(ticker, str) and TICKER_RE.match(ticker)):
        errors.append(f'{where}.ticker: 4桁のコードを文字列で書いてください（例: "7014"）: {ticker!r}')
    for key in ["added", "removed"]:
        if key in entry and not isinstance(entry[key], dt.date):
            errors.append(f"{where}.{key}: YYYY-MM-DD で書いてください: {entry[key]!r}")
    for key in ["scenario", "removed_reason"]:
        if entry.get(key) is not None and not isinstance(entry[key], str):
            errors.append(f"{where}.{key}: 文字列で書いてください")
    for key in CONDITION_LISTS:
        conds = entry.get(key)
        if conds is None:
            continue
        if not isinstance(conds, list):
            errors.append(f"{where}.{key}: リストで書いてください")
            continue
        for i, cond in enumerate(conds):
            errors.extend(_validate_condition(cond, f"{where}.{key}[{i}]"))
    return errors


def build_json_schema() -> dict:
    """reports/_config/watchlist.schema.json の内容。active と archive の両方で使うため
    removed / removed_reason は任意キーとして定義し、必須かどうかは check が判定する。
    """
    metric_table = "\n".join(f"- `{m.name}`: {m.description}" for m in METRICS.values())
    condition = {
        "type": "object",
        "required": CONDITION_KEYS,
        "additionalProperties": False,
        "properties": {
            "text": {"type": "string", "description": "条件の説明（通知に表示する）"},
            "metric": {
                "enum": list(METRICS),
                "markdownDescription": metric_table,
            },
            "op": {"enum": OPS},
            "value": {"type": "number"},
        },
    }
    conditions = {"type": ["array", "null"], "items": {"$ref": "#/definitions/condition"}}
    date = {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}$"}
    entry = {
        "type": "object",
        "required": ["ticker", "added"],
        "additionalProperties": False,
        "properties": {
            "ticker": {
                "type": "string",
                "pattern": TICKER_RE.pattern,
                "description": '証券コード。文字列で書く（"7014", "278A"）',
            },
            "added": {**date, "description": "追加日"},
            "scenario": {
                "type": ["string", "null"],
                "description": "想定シナリオ・買い条件の要約（2〜3行）。詳しい根拠はレポートに書く",
            },
            "buy_conditions": {
                **conditions,
                "description": "レポートなし / watch のとき評価する。全て満たしたら成立（AND）",
            },
            "sell_conditions": {
                **conditions,
                "description": "hold のときだけ評価する。全て満たしたら成立（AND）",
            },
            "removed": {**date, "description": "外した日（archive のみ。remove が付ける）"},
            "removed_reason": {
                "type": "string",
                "description": "外した理由（archive のみ。remove が付ける）",
            },
        },
    }
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "watchlist",
        "description": "ウォッチリスト（docs/20261004_watchlist.md）。new_watch.py schema で生成。手で編集しない",
        "type": ["array", "null"],
        "items": {"$ref": "#/definitions/entry"},
        "definitions": {"entry": entry, "condition": condition},
    }


def render_json_schema() -> str:
    return json.dumps(build_json_schema(), ensure_ascii=False, indent=2) + "\n"
