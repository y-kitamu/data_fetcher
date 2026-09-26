"""JSON/Markdown output for the report generator (section 9): writes the
snapshot JSON, updates a copied template's frontmatter in place (preserving
comments and key order via ruamel.yaml's round-trip mode), fills in the
summary-block placeholder in the body, and refuses to overwrite existing
files (section 0: 上書き禁止).
"""

from __future__ import annotations

import io
import re
from pathlib import Path

from ruamel.yaml import YAML

from .snapshot_schema import Snapshot

_FRONTMATTER_RE = re.compile(r"\A---\n(.*?\n)---\n(.*)\Z", re.DOTALL)
_SUMMARY_START = "<!-- snapshot-summary:start 生成スクリプトが書き込む。手で編集しない -->"
_SUMMARY_END = "<!-- snapshot-summary:end -->"

_GRADE_LABELS: dict[str, str] = {
    "asset_value": "資産",
    "earnings_value": "収益",
    "financial_health": "健全性",
    "profitability": "収益性",
    "growth": "成長性",
    "cyclicality": "循環性",
    "survival": "生存力",
    "reversal": "反転",
}


def split_frontmatter(text: str) -> tuple[str, str]:
    match = _FRONTMATTER_RE.match(text)
    if match is None:
        raise ValueError("frontmatterが見つかりません(先頭が'---'で始まっていません)")
    return match.group(1), match.group(2)


def apply_frontmatter_updates(data, updates: dict[str, object]) -> None:
    """ドット区切りキー(例: "ratings.asset_value.auto")で指定した既存キーだけを
    上書きする。雛形に無いキーは追加しない(誤って構造を変えないため)。
    """
    for dotted_key, value in updates.items():
        parts = dotted_key.split(".")
        node = data
        for part in parts[:-1]:
            node = node[part]
        last = parts[-1]
        if last not in node:
            raise KeyError(f"雛形に存在しないキーです: {dotted_key}")
        node[last] = value


def update_frontmatter(template_text: str, updates: dict[str, object]) -> str:
    fm_text, body = split_frontmatter(template_text)
    yaml = YAML()
    yaml.preserve_quotes = True
    data = yaml.load(fm_text)
    apply_frontmatter_updates(data, updates)
    buf = io.StringIO()
    yaml.dump(data, buf)
    return f"---\n{buf.getvalue()}---\n{body}"


def insert_summary_block(body: str, table_markdown: str) -> str:
    if _SUMMARY_START not in body or _SUMMARY_END not in body:
        raise ValueError("summaryブロックのマーカーが見つかりません")
    pattern = re.compile(re.escape(_SUMMARY_START) + r".*?" + re.escape(_SUMMARY_END), re.DOTALL)
    return pattern.sub(lambda _m: f"{_SUMMARY_START}\n{table_markdown}\n{_SUMMARY_END}", body, count=1)


def write_new_file(path: Path, content: str) -> None:
    if path.exists():
        raise FileExistsError(f"{path} は既に存在します。上書きはできません。")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def check_not_exists(*paths: Path) -> None:
    """複数ファイルを書く前にまとめて存在チェックする。片方だけ書いて
    中断する事故を防ぐ(§0 上書き禁止)。
    """
    existing = [p for p in paths if p.exists()]
    if existing:
        names = ", ".join(str(p) for p in existing)
        raise FileExistsError(f"既に存在するファイルがあります: {names}")


def _format_yen(value: float | None) -> str:
    return f"{value:,.0f}円" if value is not None else "—"


def _format_oku(value_millions: float | None) -> str:
    return f"{value_millions / 100:,.0f}億円" if value_millions is not None else "—"


def _format_ratio(value: float | None, digits: int = 2) -> str:
    return f"{value:.{digits}f}" if isinstance(value, (int, float)) else "—"


def _format_pct(value: float | None) -> str:
    return f"{value * 100:+.0f}%" if value is not None else "—"


def _diffed(current: str, previous_value, fmt) -> str:
    if previous_value is None:
        return current
    return f"{current}（前回{fmt(previous_value)}）"


def _get_path(data: dict, path: str):
    node = data
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def render_auto_ratings_line(snapshot: Snapshot) -> str:
    parts = [
        f"{label}{getattr(snapshot.auto_ratings, key) or '—'}" for key, label in _GRADE_LABELS.items()
    ]
    return "自動評価: " + " / ".join(parts) + f"（rubric v{snapshot.rubric_version}）"


def render_summary_table(snapshot: Snapshot, previous_snapshot: dict | None = None) -> str:
    """§9.2 の要約ブロックのMarkdown表を組み立てる。reviewレポートでは
    previous_snapshot(前回のsnapshot.jsonをdictでロードしたもの)を渡すと、
    1行目の各値に前回との差分を括弧で併記する。
    """
    vm, vr, survival = snapshot.valuation_metrics, snapshot.value_range, snapshot.survival
    consecutive = snapshot.ratios.consecutive_loss_years

    pbr_cell = _format_ratio(vm.pbr)
    psr_cell = _format_ratio(vm.psr)
    per_normalized_cell = _format_ratio(vm.per_normalized, 1)
    drawdown_cell = _format_pct(vm.drawdown_from_peak)
    if previous_snapshot is not None:
        pbr_cell = _diffed(pbr_cell, _get_path(previous_snapshot, "valuation_metrics.pbr"), lambda v: _format_ratio(v))
        psr_cell = _diffed(psr_cell, _get_path(previous_snapshot, "valuation_metrics.psr"), lambda v: _format_ratio(v))
        per_normalized_cell = _diffed(
            per_normalized_cell,
            _get_path(previous_snapshot, "valuation_metrics.per_normalized"),
            lambda v: _format_ratio(v, 1),
        )
        drawdown_cell = _diffed(
            drawdown_cell, _get_path(previous_snapshot, "valuation_metrics.drawdown_from_peak"), _format_pct
        )

    row1 = (
        "| 株価 | 時価総額 | PBR | PSR | 正常化PER | ピーク比 |\n"
        "|---|---|---|---|---|---|\n"
        f"| {_format_yen(snapshot.market.price)} | {_format_oku(vm.market_cap)} | "
        f"{pbr_cell} | {psr_cell} | {per_normalized_cell} | {drawdown_cell} |"
    )

    if survival is not None and survival.ocf_positive:
        runway_cell = "営業CF黒字"
    elif survival is not None and survival.runway_months is not None:
        runway_cell = f"{survival.runway_months:.0f}か月"
    else:
        runway_cell = "—"

    if vr.risk_reward == "price_below_liquidation":
        risk_reward_cell = "株価が清算価値を下回る"
    elif isinstance(vr.risk_reward, (int, float)):
        risk_reward_cell = _format_ratio(vr.risk_reward, 1)
    else:
        risk_reward_cell = "—"

    loss_years = max(consecutive.get("operating", 0), consecutive.get("net", 0))
    row2 = (
        "| 清算価値 | 正常化価値 | ピーク価値 | リスクリワード | 持ちこたえ月数 | 連続赤字 |\n"
        "|---|---|---|---|---|---|\n"
        f"| {_format_yen(vr.liquidation)} | {_format_yen(vr.normalized)} | {_format_yen(vr.peak)} | "
        f"{risk_reward_cell} | {runway_cell} | {loss_years}期 |"
    )

    return "\n\n".join(
        [
            row1,
            row2,
            render_auto_ratings_line(snapshot),
            f"警告: {len(snapshot.warnings)}件（詳細は snapshot.json の warnings）",
        ]
    )
