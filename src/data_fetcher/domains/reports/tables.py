"""Markdown rendering of a snapshot's tables, written next to the report as
`YYYY-MM-DD_tables.md` so the numbers can be read in an editor without
stock-viewer. Sections follow the headings of reports/_templates/initial.md
and the auto-block specs (section 5 of claude_code_instructions_B_report_viewer.md).

Only snapshot-derived blocks are rendered: blocks that depend on the report's
frontmatter (summary_card, predictions_table, ratings_table's final column,
plan_table, ...) are edited by hand after generation and would go stale here.
Values are taken from the snapshot as-is; the only display-side derivations
are the ones the viewer spec also leaves to the display side (前年比, 原価率,
販管費率, FCF, 株価に対する比率).
"""

from __future__ import annotations

from collections.abc import Callable

from ..jp_stocks._guard import safe_div
from ..jp_stocks.ratios import growth_series
from .snapshot_schema import Snapshot
from .writer import _GRADE_LABELS, _get_path, render_summary_table

_DASH = "—"

_BALANCE_SHEET_ROWS: tuple[tuple[str, str], ...] = (
    ("cash", "現金及び預金"),
    ("receivables", "受取手形及び売掛金"),
    ("securities_current", "有価証券"),
    ("inventory", "棚卸資産"),
    ("current_assets_other", "その他流動資産"),
    ("current_assets_total", "**流動資産合計**"),
    ("ppe", "有形固定資産"),
    ("intangibles", "無形固定資産"),
    ("investment_securities", "投資有価証券"),
    ("investments_other", "投資その他の資産"),
    ("noncurrent_assets_total", "**固定資産合計**"),
    ("total_assets", "**資産合計**"),
    ("payables", "支払手形及び買掛金"),
    ("short_term_debt", "短期借入金"),
    ("current_portion_long_term_debt", "1年内返済予定の長期借入金"),
    ("current_portion_bonds", "1年内償還予定の社債"),
    ("current_liabilities_other", "その他流動負債"),
    ("current_liabilities_total", "**流動負債合計**"),
    ("long_term_debt", "長期借入金"),
    ("bonds", "社債"),
    ("lease_obligations", "リース債務"),
    ("noncurrent_liabilities_other", "その他固定負債"),
    ("noncurrent_liabilities_total", "**固定負債合計**"),
    ("total_liabilities", "**負債合計**"),
    ("shareholders_equity", "株主資本"),
    ("net_assets", "**純資産合計**"),
)

_LIQUIDATION_LABELS: dict[str, str] = {
    "cash": "現金及び預金",
    "receivables": "受取手形及び売掛金",
    "securities_current": "有価証券",
    "inventory": "棚卸資産",
    "current_assets_other": "その他流動資産",
    "ppe": "有形固定資産",
    "intangibles": "無形固定資産",
    "investments": "投資等",
}

_CAPITAL_ACTION_LABELS: dict[str, str] = {
    "buyback_decision": "自社株買い（決議）",
    "buyback_done": "自社株買い（取得）",
    "cancellation": "自己株式の消却",
    "disposal": "自己株式の処分",
    "equity_issue": "増資",
    "msc_warrant": "MSワラント",
    "cb": "転換社債",
    "split": "株式分割",
    "reverse_split": "株式併合",
}


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _num(value, digits: int = 0) -> str:
    return f"{value:,.{digits}f}" if _is_number(value) else _DASH


def _pct(value, digits: int = 1) -> str:
    return f"{value * 100:.{digits}f}%" if _is_number(value) else _DASH


def _signed_pct(value, digits: int = 1) -> str:
    return f"{value * 100:+.{digits}f}%" if _is_number(value) else _DASH


def _yen(value) -> str:
    return f"{value:,.0f}円" if _is_number(value) else _DASH


def _times(value, digits: int = 2) -> str:
    return f"{value:.{digits}f}倍" if _is_number(value) else _DASH


def _text(value) -> str:
    if value is None or value == "":
        return _DASH
    return str(value).replace("|", "\\|").replace("\n", " ")


def _flag(value: bool | None) -> str:
    if value is None:
        return _DASH
    return "該当" if value else "なし"


def _link(label: str | None, url: str | None) -> str:
    return f"[{_text(label)}]({url})" if url else _text(label)


def _table(header: list[str], rows: list[list[str]], numeric: bool = True) -> str:
    """GFMの表。numeric=True なら2列目以降を右寄せにする(1列目は常に左寄せ)。"""
    lines = [
        "| " + " | ".join(header) + " |",
        "|---|" + ("---:|" if numeric else "---|") * (len(header) - 1),
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def _key_value_table(rows: list[tuple[str, str]], header: tuple[str, str] = ("項目", "値")) -> str:
    return _table(list(header), [[label, value] for label, value in rows])


def _cells(series: list | None, n: int, fmt: Callable[[object], str]) -> list[str]:
    """年次系列をfiscal_yearsと同じ長さのセルにする。系列が無い・短い場合は「—」で埋める。"""
    series = series or []
    return [fmt(series[i]) if i < len(series) else _DASH for i in range(n)]


def _yearly_table(
    years: list[str], rows: list[tuple[str, list[str]]], extra_header: list[str] | None = None,
    limit: int | None = None,
) -> str:
    """行=年度(古い順)、列=科目の表。limitを渡すと直近limit年だけにする。
    各科目のセルは年度の数 + extra_header の数だけ並んでいること。
    extra_header の項目は年度の行の下に続けて並べる。
    """
    extra = extra_header or []
    skip = max(len(years) - limit, 0) if limit else 0
    n = len(years)
    header = ["期", *[label for label, _ in rows]]
    body = [[year, *[cells[i] for _label, cells in rows]] for i, year in enumerate(years) if i >= skip]
    body.extend([name, *[cells[n + j] for _label, cells in rows]] for j, name in enumerate(extra))
    return _table(header, body)


def _section(title: str, *blocks: str, level: int = 2) -> str:
    return "\n\n".join([f"{'#' * level} {title}", *[b for b in blocks if b]])


def _with_yoy(values: list | None, yoy: list | None, n: int) -> list[str]:
    values, yoy = values or [], yoy or []
    cells = []
    for i in range(n):
        value = values[i] if i < len(values) else None
        change = yoy[i] if i < len(yoy) else None
        cell = _num(value)
        if _is_number(value) and _is_number(change):
            cell += f" ({_signed_pct(change)})"
        cells.append(cell)
    return cells


def _render_segments(snapshot: Snapshot, years: int = 10) -> str:
    if not snapshot.segments:
        return "データなし"
    n = len(snapshot.fiscal_years)
    rows = []
    for segment in snapshot.segments:
        name = _text(segment.name)
        rows.append((f"{name} 売上高", _cells(segment.revenue, n, _num)))
        rows.append((f"{name} 営業利益", _cells(segment.operating_income, n, _num)))
    return _yearly_table(snapshot.fiscal_years, rows, limit=years)


def _render_cost_structure(snapshot: Snapshot) -> str:
    cs = snapshot.cost_structure
    return _key_value_table(
        [
            ("固定費 F", _num(cs.fixed_cost)),
            ("変動費率 v", _pct(cs.variable_cost_ratio)),
            ("決定係数 R²", _num(cs.r2, 3)),
            ("回帰に使った年数", f"{cs.n_years}年"),
            ("損益分岐点の売上高", _num(cs.breakeven_revenue)),
            ("最新の売上高", _num(cs.latest_revenue)),
            ("損益分岐点と最新売上の差", _signed_pct(cs.gap_to_breakeven_pct)),
        ]
    )


def _render_balance_sheet(snapshot: Snapshot) -> str:
    n = len(snapshot.fiscal_years)
    bs = snapshot.balance_sheet
    known = {key for key, _label in _BALANCE_SHEET_ROWS}
    rows = [(label, _cells(bs.get(key), n, _num)) for key, label in _BALANCE_SHEET_ROWS]
    rows.extend((key, _cells(bs[key], n, _num)) for key in bs if key not in known)
    return _yearly_table(snapshot.fiscal_years, rows)


def _render_income_statement(snapshot: Snapshot) -> str:
    n = len(snapshot.fiscal_years)
    income = snapshot.income_statement
    revenue = income.get("revenue") or []

    def ratio_to_revenue(key: str) -> list[float | None]:
        if income.get(f"{key}_ratio"):
            return income[f"{key}_ratio"]
        return [safe_div(v, r) for v, r in zip(income.get(key) or [], revenue)]

    def yoy(key: str, from_ratios: list | None = None) -> list[float | None]:
        if income.get(f"{key}_yoy"):
            return income[f"{key}_yoy"]
        return from_ratios or growth_series(income.get(key) or [], null_if_prior_negative=True)

    rows = [
        ("売上高", _with_yoy(revenue, yoy("revenue", snapshot.ratios.revenue_growth), n)),
        ("原価率", _cells(ratio_to_revenue("cost_of_sales"), n, _pct)),
        ("販管費率", _cells(ratio_to_revenue("sga"), n, _pct)),
        (
            "営業利益",
            _with_yoy(
                income.get("operating_income"),
                yoy("operating_income", snapshot.ratios.operating_income_growth),
                n,
            ),
        ),
        ("経常利益", _with_yoy(income.get("ordinary_income"), yoy("ordinary_income"), n)),
        ("当期利益", _with_yoy(income.get("net_income"), yoy("net_income"), n)),
    ]
    return _yearly_table(snapshot.fiscal_years, rows)


def _render_cash_flow(snapshot: Snapshot) -> str:
    n = len(snapshot.fiscal_years)
    cf = snapshot.cash_flow
    fcf = cf.get("fcf") or [
        o - c if o is not None and c is not None else None
        for o, c in zip(cf.get("operating_cf") or [], cf.get("capex") or [])
    ]
    rows = [
        ("営業CF", _cells(cf.get("operating_cf"), n, _num)),
        ("投資CF", _cells(cf.get("investing_cf"), n, _num)),
        ("　うち設備投資", _cells(cf.get("capex"), n, _num)),
        ("財務CF", _cells(cf.get("financing_cf"), n, _num)),
        ("FCF（営業CF − 設備投資）", _cells(fcf, n, _num)),
        ("減価償却費", _cells(cf.get("depreciation"), n, _num)),
    ]
    return _yearly_table(snapshot.fiscal_years, rows)


def _render_financial_ratios(snapshot: Snapshot) -> str:
    n = len(snapshot.fiscal_years)
    ratios = snapshot.ratios

    def row(label: str, key: str, fmt: Callable[[object], str]) -> tuple[str, list[str]]:
        stats = ratios.cycle_stats.get(key)
        return (
            label,
            [
                *_cells(getattr(ratios, key), n, fmt),
                fmt(stats.median_10y) if stats else _DASH,
                _pct(stats.percentile_latest, 0) if stats else _DASH,
            ],
        )

    groups = [
        (
            "(1) 健全性",
            [
                row("株主資本比率", "equity_ratio", _pct),
                row("流動比率", "current_ratio", _pct),
                row("ネットキャッシュ", "net_cash", _num),
                row("有利子負債/EBITDA", "debt_to_ebitda", _times),
            ],
        ),
        (
            "(2) 収益性",
            [
                row("営業利益率", "operating_margin", _pct),
                row("ROE", "roe", _pct),
                row("ROA", "roa", _pct),
                row("ROIC", "roic", _pct),
            ],
        ),
        (
            "(3) 成長性",
            [
                row("売上成長", "revenue_growth", _signed_pct),
                row("営業利益成長", "operating_income_growth", _signed_pct),
            ],
        ),
    ]
    extra = ["10年中央値", "現在の位置"]
    return "\n\n".join(
        f"**{title}**\n\n{_yearly_table(snapshot.fiscal_years, rows, extra)}" for title, rows in groups
    )


def _render_valuation_metrics(snapshot: Snapshot) -> str:
    vm, market = snapshot.valuation_metrics, snapshot.market
    caption = f"株価 {_yen(market.price)}（{market.price_date or _DASH}）で計算"
    table = _key_value_table(
        [
            ("近似PER（会社予想）", _times(vm.per_forecast, 1)),
            ("PCFR", _times(vm.pcfr, 1)),
            ("PSR", _times(vm.psr)),
            ("PBR", _times(vm.pbr)),
            ("予想収益率（1/PER）", _pct(vm.earnings_yield)),
            ("PER×PBR", _num(vm.per_x_pbr, 1)),
            ("EV/EBITDA", _times(vm.ev_ebitda, 1)),
            ("ROIC", _pct(vm.roic)),
            ("アクルーアル/総資産", _pct(vm.accruals_to_assets)),
            ("時価総額", _num(vm.market_cap)),
            ("正常化PER", _times(vm.per_normalized, 1)),
            ("実績PER", _times(vm.per_trailing, 1)),
            ("ピークからの下落率", _signed_pct(vm.drawdown_from_peak)),
        ],
        header=("指標", "値"),
    )
    return f"{caption}\n\n{table}"


def _latest(series: list | None):
    return next((v for v in reversed(series or []) if v is not None), None)


def _render_peer_comparison(snapshot: Snapshot) -> str:
    cycle, vm = snapshot.cycle, snapshot.valuation_metrics
    own_label = f"{snapshot.ticker} {_text(snapshot.company_name)}（自社）"
    rows = [
        [
            own_label,
            _times(vm.pbr),
            _times(vm.psr),
            _pct(_latest(snapshot.ratios.operating_margin)),
            _DASH,
        ]
    ]
    for peer in cycle.peers:
        rows.append(
            [
                f"{peer.ticker} {_text(peer.name)}" if peer.name else peer.ticker,
                _times(peer.pbr),
                _times(peer.psr),
                _pct(_latest(peer.operating_margin)),
                _signed_pct(peer.margin_change_latest),
            ]
        )
    summary = _table(["銘柄", "PBR", "PSR", "営業利益率（最新）", "前期からの変化"], rows)

    n = len(snapshot.fiscal_years)
    margin_rows = [(own_label, _cells(snapshot.ratios.operating_margin, n, _pct))]
    margin_rows.extend((peer.ticker, _cells(peer.operating_margin, n, _pct)) for peer in cycle.peers)
    margin_rows.append(("業界中央値", _cells(cycle.industry_median_operating_margin, n, _pct)))
    margins = _yearly_table(snapshot.fiscal_years, margin_rows)

    selection = "同業は自動選定" if cycle.peer_selection == "auto" else "同業は手動指定"
    return f"{selection}\n\n{summary}\n\n**営業利益率の推移**\n\n{margins}"


def _render_shareholders(snapshot: Snapshot) -> str:
    sh = snapshot.shareholders
    n = len(snapshot.fiscal_years)

    top10 = (
        _table(
            ["株主", "保有比率", "基準日"],
            [[_text(s.name), _pct(s.ratio, 2), s.as_of] for s in sh.top10],
        )
        if sh.top10
        else "データなし"
    )
    large = (
        _table(
            ["提出者", "保有比率", "増減", "日付"],
            [
                [_text(r.filer), _pct(r.ratio, 2), _signed_pct(r.change_pt, 2), r.date]
                for r in sh.large_shareholding_reports
            ],
        )
        if sh.large_shareholding_reports
        else "データなし"
    )
    dividends = (
        _yearly_table(
            snapshot.fiscal_years,
            [
                ("配当性向", _cells(sh.payout_ratio, n, _pct)),
                ("1株配当（円）", _cells(sh.dividend_per_share, n, lambda v: _num(v, 1))),
            ],
        )
        if sh.payout_ratio or sh.dividend_per_share
        else "データなし"
    )
    actions = (
        _table(
            ["日付", "種類", "内容", "希薄化率"],
            [
                [
                    a.date,
                    _CAPITAL_ACTION_LABELS.get(a.type, _text(a.type)),
                    _link(a.detail, a.url),
                    _pct(a.dilution_ratio),
                ]
                for a in sh.capital_actions
            ],
            numeric=False,
        )
        if sh.capital_actions
        else "データなし"
    )
    policy, cost = sh.shareholder_return_policy, sh.capital_cost_disclosure
    policy_line = f"{_text(policy.text)}（{policy.disclosed_at}）" if policy.text else "データなし"
    cost_line = _link(cost.disclosed_at, cost.url) if cost.disclosed_at or cost.url else "データなし"

    return "\n\n".join(
        [
            f"**大株主上位10**\n\n{top10}",
            f"**大量保有報告書**\n\n{large}",
            f"**配当性向・1株配当の推移**\n\n{dividends}",
            f"**資本政策の履歴**\n\n{actions}",
            f"**株主還元方針**: {policy_line}",
            f"**資本コストに関する開示**: {cost_line}",
        ]
    )


def _render_liquidation_value(snapshot: Snapshot) -> str:
    lv = snapshot.liquidation_value
    if lv is None:
        return "データなし"
    components = _table(
        ["科目", "簿価", "掛け目", "修正後"],
        [
            [_LIQUIDATION_LABELS.get(key, key), _num(c.book), _pct(c.haircut, 0), _num(c.adjusted)]
            for key, c in lv.components.items()
        ],
    )
    totals = _key_value_table(
        [
            ("修正資産合計", _num(lv.adjusted_assets)),
            ("負債合計", _num(lv.total_liabilities)),
            ("清算価値（総額）", _num(lv.value_total)),
            ("清算価値（1株あたり）", _yen(lv.value_per_share)),
            ("株価に対する比率", _pct(safe_div(lv.value_per_share, snapshot.market.price))),
        ]
    )
    return f"{components}\n\n{totals}"


def _render_dcf(snapshot: Snapshot) -> str:
    dcf = snapshot.dcf
    if dcf is None:
        return "データなし"
    table = _key_value_table(
        [
            ("割引率 R", _pct(dcf.discount_rate, 0)),
            ("ネットキャッシュ", _num(dcf.net_cash)),
            (f"FCF（{dcf.fcf_base_year or _DASH}期）", _num(dcf.fcf_base)),
            ("弱気: ネットキャッシュ + FCF ÷ R（総額）", _num(dcf.bear_total)),
            ("弱気（1株あたり）", _yen(dcf.bear_per_share)),
            (
                f"強気: {dcf.bull_growth_years}年間 年{_pct(dcf.bull_growth_rate, 0)}成長（総額）",
                _num(dcf.bull_total),
            ),
            ("強気（1株あたり）", _yen(dcf.bull_per_share)),
        ]
    )
    if dcf.bear_per_share is None:
        table += (
            "\n\n算出せず（FCF がマイナス、または FCF・ネットキャッシュが取得できない）。"
            "7.3 正常化収益バリューを参照。"
        )
    return table


def _render_normalized_value(snapshot: Snapshot) -> str:
    nv = snapshot.normalized_value
    if nv is None:
        return "データなし"
    return _key_value_table(
        [
            ("営業利益率の中央値（10年）", _pct(nv.median_operating_margin)),
            ("正常化営業利益", _num(nv.normalized_operating_income)),
            ("税率", _pct(nv.tax_rate, 0)),
            ("正常化FCF", _num(nv.normalized_fcf)),
            ("正常化価値（1株あたり）", _yen(nv.value_per_share)),
            ("ピーク時の営業利益率", _pct(nv.peak_operating_margin)),
            ("ピークFCF", _num(nv.peak_fcf)),
            ("ピーク価値（1株あたり）", _yen(nv.peak_value_per_share)),
        ]
    )


def _risk_reward(value) -> str:
    if value == "price_below_liquidation":
        return "株価が清算価値を下回る"
    return _num(value, 1)


def _render_value_range(snapshot: Snapshot) -> str:
    vr = snapshot.value_range
    return _table(
        ["清算価値", "株価", "正常化価値", "ピーク価値", "リスクリワード"],
        [[_yen(vr.liquidation), _yen(vr.price), _yen(vr.normalized), _yen(vr.peak), _risk_reward(vr.risk_reward)]],
    )


def _render_cycle_position(snapshot: Snapshot) -> str:
    cycle = snapshot.cycle
    losses = snapshot.ratios.consecutive_loss_years
    cards = _key_value_table(
        [
            ("ピークからの下落率", _signed_pct(snapshot.market.drawdown_from_peak)),
            ("連続営業赤字", f"{losses.get('operating', 0)}期"),
            ("連続純損失", f"{losses.get('net', 0)}期"),
            ("同業で同時に悪化している割合", _pct(cycle.peer_margin_deterioration_share, 0)),
            ("業界の利益率の半減期（年）", _num(cycle.industry_margin_half_life_years, 1)),
        ]
    )
    indicators = (
        _table(
            ["先行指標", "単位", "最新値", "前年比"],
            [
                [_text(i.name), _text(i.unit), _num(i.latest, 2), _signed_pct(i.yoy)]
                for i in cycle.leading_indicators
            ],
        )
        if cycle.leading_indicators
        else "先行指標: 未登録"
    )
    return f"{cards}\n\n{indicators}\n\n自社と業界中央値の営業利益率の推移は「5. 株価指標」の同業比較を参照。"


def _render_survival(snapshot: Snapshot) -> str:
    survival = snapshot.survival
    if survival is None:
        return "データなし"
    if survival.ocf_positive:
        runway = "営業CF黒字"
    elif survival.runway_months is not None:
        runway = f"{survival.runway_months:.0f}か月"
    else:
        runway = _DASH
    return _key_value_table(
        [
            ("持ちこたえられる月数", runway),
            ("1年以内返済の借入", _num(survival.debt_due_within_1y)),
            ("1年以内返済の借入 ÷ 現預金", _times(survival.debt_due_to_cash)),
            ("コミットメントライン", _num(survival.commitment_line)),
            ("継続企業の前提に関する注記", _flag(survival.going_concern_note)),
            ("継続企業の前提に関する重要事象", _flag(survival.going_concern_events)),
            ("財務制限条項への抵触", _flag(survival.covenant_breach)),
            ("監理・整理銘柄等の指定", _text(survival.exchange_designation) if survival.exchange_designation else "なし"),
        ]
    )


def _render_reversal_signals(snapshot: Snapshot) -> str:
    rs = snapshot.reversal_signals
    return _key_value_table(
        [
            ("赤字幅の縮小（前四半期比）", _flag(rs.loss_narrowing_qoq)),
            ("赤字幅の縮小（前年同期比）", _flag(rs.loss_narrowing_yoy)),
            ("直近6か月の上方修正", _flag(rs.upward_revision_last_6m)),
            ("業界の設備投資 ÷ 減価償却費", _times(rs.industry_capex_to_depreciation)),
            ("直近12か月の再編・リストラ開示", f"{len(rs.restructuring_last_12m)}件"),
        ]
    )


def _render_auto_ratings(snapshot: Snapshot) -> str:
    ratings = snapshot.auto_ratings
    rows = []
    for key, label in _GRADE_LABELS.items():
        detail = ratings.details.get(key)
        rows.append(
            [
                label,
                getattr(ratings, key) or _DASH,
                f"{detail.metric:.3g}" if detail and _is_number(detail.metric) else _DASH,
                _num(detail.points) if detail else _DASH,
                _text(detail.note) if detail else _DASH,
            ]
        )
    table = _table(["項目", "自動評価", "指標値", "点数", "根拠"], rows, numeric=False)
    return f"{table}\n\n最終評価はレポート本文の frontmatter（ratings.*.final）を参照。"


def _render_warnings(snapshot: Snapshot) -> str:
    if not snapshot.warnings:
        return "なし"
    return _table(
        ["コード", "対象", "内容"],
        [[w.code, f"`{w.field}`", _text(w.message)] for w in snapshot.warnings],
        numeric=False,
    )


_DIFF_ROWS: tuple[tuple[str, str, Callable[[object], str]], ...] = (
    ("売上高（年次）", "income_statement.revenue", _num),
    ("営業利益（年次）", "income_statement.operating_income", _num),
    ("営業利益率（年次）", "ratios.operating_margin", _pct),
    ("営業利益率（直近四半期）", "quarterly.operating_margin", _pct),
    ("現金及び預金", "balance_sheet.cash", _num),
    ("ネットキャッシュ", "ratios.net_cash", _num),
    ("持ちこたえられる月数", "survival.runway_months", _num),
    ("株価", "market.price", _yen),
    ("PBR", "valuation_metrics.pbr", _times),
    ("PSR", "valuation_metrics.psr", _times),
    ("正常化PER", "valuation_metrics.per_normalized", lambda v: _times(v, 1)),
    ("清算価値（1株あたり）", "value_range.liquidation", _yen),
    ("正常化価値（1株あたり）", "value_range.normalized", _yen),
    ("ピーク価値（1株あたり）", "value_range.peak", _yen),
    ("リスクリワード", "value_range.risk_reward", _risk_reward),
    *((f"自動評価: {label}", f"auto_ratings.{key}", _text) for key, label in _GRADE_LABELS.items()),
)


def _render_diff_since_last(snapshot: Snapshot, previous_snapshot: dict) -> str:
    """auto:diff_since_last。年次・四半期の系列は最新(配列の末尾)同士を比べる。"""
    current = snapshot.model_dump(mode="json")

    def lookup(data: dict, path: str):
        value = _get_path(data, path)
        return (value[-1] if value else None) if isinstance(value, list) else value

    rows = [
        [label, fmt(lookup(previous_snapshot, path)), fmt(lookup(current, path))]
        for label, path, fmt in _DIFF_ROWS
    ]
    previous_as_of = previous_snapshot.get("as_of") or _DASH
    return _table(["指標", f"前回（{previous_as_of}）", f"今回（{snapshot.as_of}）"], rows)


def _render_kill_criteria_check(snapshot: Snapshot) -> str:
    rows = []
    for criterion in snapshot.kill_criteria_check:
        if criterion.metric is None:
            verdict = "人が判定"
        elif criterion.hit is None:
            verdict = "判定不能（値なし）"
        else:
            verdict = "**該当**" if criterion.hit else "非該当"
        threshold = f"{criterion.op} {criterion.value:g}" if criterion.value is not None else _DASH
        rows.append(
            [
                _text(criterion.text),
                f"`{criterion.metric}`" if criterion.metric else _DASH,
                threshold,
                _num(criterion.actual, 2),
                verdict,
            ]
        )
    return _table(["撤退条件", "指標", "閾値", "実際の値", "判定"], rows, numeric=False)


def _render_trades(snapshot: Snapshot) -> str:
    trades = _table(
        ["日付", "売買", "株数", "価格", "手数料"],
        [
            [t.date, "買い" if t.side == "buy" else "売り", _num(t.qty), _yen(t.price), _yen(t.fee)]
            for t in snapshot.trades
        ],
    )
    result = snapshot.result
    if result is None:
        return trades
    summary = _key_value_table(
        [
            ("平均買値", _yen(result.avg_buy_price)),
            ("平均売値", _yen(result.avg_sell_price)),
            ("リターン", _signed_pct(result.return_pct)),
            ("保有日数", f"{result.holding_days}日" if result.holding_days is not None else _DASH),
            (f"{result.benchmark} のリターン", _signed_pct(result.benchmark_return_pct)),
            ("超過リターン", _signed_pct(result.excess_return_pct)),
            ("最大ドローダウン", _signed_pct(result.max_drawdown_pct)),
        ]
    )
    return f"{trades}\n\n{summary}"


def render_tables_markdown(snapshot: Snapshot, previous_snapshot: dict | None = None) -> str:
    """スナップショットの表をまとめたMarkdown(`YYYY-MM-DD_tables.md` の中身)を組み立てる。
    previous_snapshot(前回のsnapshot.jsonをdictでロードしたもの)を渡すと、
    前回からの変化の表(auto:diff_since_last)を加える。
    """
    company = snapshot.company
    fiscal_month = f"{company.fiscal_year_end_month}月" if company.fiscal_year_end_month else _DASH
    intro = "\n".join(
        [
            f"> `{snapshot.as_of}_snapshot.json` から生成した表。手で編集しない"
            f"（`new_report.py tables {snapshot.ticker}` で再生成できる）。",
            "> 金額は百万円、1株あたりの値は円。「—」はデータなし。グラフは stock-viewer で確認する。",
            ">",
            f"> 業種: {_text(company.sector33_name)} / 市場: {_text(company.market)} / 決算月: {fiscal_month}"
            f" / 生成日時: {snapshot.generated_at}",
        ]
    )

    sections = [
        f"# {snapshot.ticker} {_text(snapshot.company_name)} 表（{snapshot.as_of}）",
        intro,
        _section("1. サマリー", render_summary_table(snapshot, previous_snapshot)),
    ]
    if previous_snapshot is not None:
        sections.append(_section("前回からの変化", _render_diff_since_last(snapshot, previous_snapshot)))
    if snapshot.kill_criteria_check:
        sections.append(_section("撤退条件の確認", _render_kill_criteria_check(snapshot)))
    if snapshot.trades:
        sections.append(_section("売買の結果", _render_trades(snapshot)))

    sections.extend(
        [
            _section(
                "2. 事業理解",
                _section("2.2 業績推移（セグメント別）", _render_segments(snapshot), level=3),
                _section("2.4 費用構造", _render_cost_structure(snapshot), level=3),
            ),
            _section(
                "3. 要約財務諸表",
                _section("貸借対照表", _render_balance_sheet(snapshot), level=3),
                _section("損益計算書", "括弧内は前年比。", _render_income_statement(snapshot), level=3),
                _section("キャッシュフロー計算書", _render_cash_flow(snapshot), level=3),
            ),
            _section(
                "4. 財務指標",
                "「現在の位置」は直近15年の分布の中での最新値のパーセンタイル。",
                _render_financial_ratios(snapshot),
            ),
            _section(
                "5. 株価指標",
                _render_valuation_metrics(snapshot),
                _section("同業比較", _render_peer_comparison(snapshot), level=3),
            ),
            _section("6. 株主動向・資本政策", _render_shareholders(snapshot)),
            _section(
                "7. 企業価値",
                _section("7.1 資産バリューチェック（清算価値）", _render_liquidation_value(snapshot), level=3),
                _section("7.2 収益バリューチェック（DCF）", _render_dcf(snapshot), level=3),
                _section("7.3 正常化収益バリュー", _render_normalized_value(snapshot), level=3),
                _section("7.4 企業価値レンジとリスクリワード", _render_value_range(snapshot), level=3),
            ),
            _section(
                "8. サイクル分析",
                _render_cycle_position(snapshot),
                _section("8.2 生存力", _render_survival(snapshot), level=3),
                _section("8.3 反転兆候", _render_reversal_signals(snapshot), level=3),
            ),
            _section("11. 自動評価", _render_auto_ratings(snapshot)),
            _section("警告", _render_warnings(snapshot)),
        ]
    )
    return "\n\n".join(sections) + "\n"
