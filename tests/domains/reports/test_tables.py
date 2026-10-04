from data_fetcher.domains.jp_stocks.ratios import CycleStats
from data_fetcher.domains.reports.snapshot_schema import (
    KillCriterionResult,
    MarketInfo,
    RatiosData,
    Snapshot,
    ValuationMetrics,
    ValueRange,
)
from data_fetcher.domains.reports.tables import render_tables_markdown


def _sample_snapshot() -> Snapshot:
    return Snapshot(
        ticker="9999",
        company_name="テスト|工業",
        as_of="2026-10-01",
        generated_at="2026-10-01T20:00:00+09:00",
        report_type="initial",
        market=MarketInfo(price=1234, price_date="2026-10-01"),
        fiscal_years=["2024-03", "2025-03", "2026-03"],
        balance_sheet={"cash": [1000, 1200, None], "total_assets": [5000, 5500, 6000], "custom_item": [1, 2, 3]},
        income_statement={
            "revenue": [10000, 12000, 9000],
            "cost_of_sales": [8000, 9000, 7200],
            "sga": [1000, 1200, 900],
            "operating_income": [1000, 1800, 900],
            "ordinary_income": [-100, 200, 300],
            "net_income": [500, 1000, 250],
        },
        cash_flow={"operating_cf": [900, 1500, 400], "capex": [300, None, 600]},
        ratios=RatiosData(
            operating_margin=[0.1, 0.15, 0.1],
            revenue_growth=[None, 0.2, -0.25],
            operating_income_growth=[None, 0.8, -0.5],
            cycle_stats={"operating_margin": CycleStats(median_10y=0.1, latest=0.1, percentile_latest=0.667)},
        ),
        valuation_metrics=ValuationMetrics(market_cap=36403, pbr=0.45, psr=0.30),
        value_range=ValueRange(liquidation=900, price=1234, normalized=2400, peak=3800, risk_reward=3.5),
    )


def _row(markdown: str, label: str) -> str:
    return next(line for line in markdown.splitlines() if line.startswith(f"| {label} |"))


def _cell(markdown: str, year: str, column: str) -> str:
    """「期」を先頭列にもつ表のうち column を列にもつ最初の表から、year の行のその列を返す。"""
    lines = markdown.splitlines()
    for i, line in enumerate(lines):
        cols = [c.strip() for c in line.strip("|").split("|")]
        if line.startswith("| 期 |") and column in cols:
            idx = cols.index(column)
            for row in lines[i + 2 :]:
                if not row.startswith("|"):
                    break
                cells = [c.strip() for c in row.strip("|").split("|")]
                if cells[0] == year:
                    return cells[idx]
    raise AssertionError(f"{year} / {column} not found")


def test_render_tables_markdown_has_statement_tables_with_year_rows():
    markdown = render_tables_markdown(_sample_snapshot())
    assert "| 期 | 現金及び預金 | 受取手形及び売掛金 |" in markdown
    assert _cell(markdown, "2024-03", "現金及び預金") == "1,000"
    assert _cell(markdown, "2026-03", "現金及び預金") == "—"
    # スキーマに無いキーは捨てずにキー名のまま末尾の列に出す
    assert _cell(markdown, "2025-03", "custom_item") == "2"
    # スナップショットに系列が無い科目は「—」で埋める
    assert _cell(markdown, "2025-03", "棚卸資産") == "—"


def test_income_statement_shows_yoy_and_cost_ratios():
    markdown = render_tables_markdown(_sample_snapshot())
    assert _cell(markdown, "2024-03", "売上高") == "10,000"
    assert _cell(markdown, "2025-03", "売上高") == "12,000 (+20.0%)"
    assert _cell(markdown, "2026-03", "売上高") == "9,000 (-25.0%)"
    assert _cell(markdown, "2024-03", "原価率") == "80.0%"
    assert _cell(markdown, "2025-03", "原価率") == "75.0%"
    assert _cell(markdown, "2026-03", "販管費率") == "10.0%"
    # 前年が赤字のときの前年比は出さない
    assert _cell(markdown, "2025-03", "経常利益") == "200"
    assert _cell(markdown, "2026-03", "経常利益") == "300 (+50.0%)"


def test_cash_flow_fcf_is_operating_cf_minus_capex_and_missing_series_are_dashes():
    markdown = render_tables_markdown(_sample_snapshot())
    fcf = "FCF（営業CF − 設備投資）"
    assert [_cell(markdown, y, fcf) for y in ("2024-03", "2025-03", "2026-03")] == ["600", "—", "-200"]
    assert _cell(markdown, "2025-03", "財務CF") == "—"


def test_financial_ratios_append_median_and_percentile_rows():
    markdown = render_tables_markdown(_sample_snapshot())
    assert _cell(markdown, "2025-03", "営業利益率") == "15.0%"
    assert _cell(markdown, "10年中央値", "営業利益率") == "10.0%"
    assert _cell(markdown, "現在の位置", "営業利益率") == "67%"
    assert _cell(markdown, "10年中央値", "ROE") == "—"


def test_pipe_in_text_is_escaped_so_tables_do_not_break():
    markdown = render_tables_markdown(_sample_snapshot())
    assert "# 9999 テスト\\|工業 表（2026-10-01）" in markdown


def test_optional_blocks_render_as_no_data_when_missing():
    markdown = render_tables_markdown(_sample_snapshot())
    assert "### 7.1 資産バリューチェック（清算価値）\n\nデータなし" in markdown
    assert "前回からの変化" not in markdown
    assert "撤退条件の確認" not in markdown


def test_previous_snapshot_adds_diff_table_using_latest_values():
    previous = {
        "as_of": "2026-07-01",
        "income_statement": {"revenue": [10000, 12000]},
        "valuation_metrics": {"pbr": 0.52},
    }
    markdown = render_tables_markdown(_sample_snapshot(), previous)
    assert "| 指標 | 前回（2026-07-01） | 今回（2026-10-01） |" in markdown
    assert _row(markdown, "売上高（年次）") == "| 売上高（年次） | 12,000 | 9,000 |"
    assert _row(markdown, "PBR") == "| PBR | 0.52倍 | 0.45倍 |"
    assert _row(markdown, "PSR") == "| PSR | — | 0.30倍 |"


def test_kill_criteria_check_shows_verdict():
    snapshot = _sample_snapshot()
    snapshot.kill_criteria_check = [
        KillCriterionResult(text="PBRが1倍超", metric="valuation_metrics.pbr", op=">", value=1, actual=0.45, hit=False),
        KillCriterionResult(text="月数不足", metric="survival.runway_months", op="<", value=12, actual=None, hit=None),
        KillCriterionResult(text="主要顧客を失う"),
    ]
    markdown = render_tables_markdown(snapshot)
    assert _row(markdown, "PBRが1倍超") == "| PBRが1倍超 | `valuation_metrics.pbr` | > 1 | 0.45 | 非該当 |"
    assert _row(markdown, "月数不足").endswith("| 判定不能（値なし） |")
    assert _row(markdown, "主要顧客を失う").endswith("| 人が判定 |")
