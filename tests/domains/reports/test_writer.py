from pathlib import Path

import pytest

from data_fetcher.domains.reports.writer import (
    apply_frontmatter_updates,
    check_not_exists,
    insert_summary_block,
    render_summary_table,
    split_frontmatter,
    update_frontmatter,
    write_new_file,
)
from data_fetcher.domains.reports.snapshot_schema import (
    AutoRatings,
    MarketInfo,
    RatiosData,
    Snapshot,
    ValuationMetrics,
    ValueRange,
)
from data_fetcher.domains.reports.snapshot_schema import SnapshotSurvival

_TEMPLATE_PATH = Path(__file__).parents[3] / "reports" / "_templates" / "initial.md"


def _sample_snapshot() -> Snapshot:
    return Snapshot(
        ticker="9999",
        as_of="2026-10-01",
        generated_at="2026-10-01T20:00:00+09:00",
        report_type="initial",
        market=MarketInfo(price=1234),
        valuation_metrics=ValuationMetrics(market_cap=36403, pbr=0.45, psr=0.30, per_normalized=7.4, drawdown_from_peak=-0.732),
        value_range=ValueRange(liquidation=900, normalized=2400, peak=3800, risk_reward=3.5),
        survival=SnapshotSurvival(runway_months=28, runway_basis="annual_ocf", ocf_positive=False, debt_due_within_1y=None, debt_due_to_cash=None),
        ratios=RatiosData(consecutive_loss_years={"operating": 2, "net": 1}),
        auto_ratings=AutoRatings(
            asset_value="B", earnings_value="B", financial_health="C", profitability="C",
            growth="D", cyclicality="A", survival="B", reversal=None,
        ),
    )


def test_split_frontmatter_extracts_yaml_and_body():
    text = "---\na: 1\n---\nbody here\n"
    fm, body = split_frontmatter(text)
    assert fm == "a: 1\n"
    assert body == "body here\n"


def test_split_frontmatter_raises_without_leading_marker():
    with pytest.raises(ValueError):
        split_frontmatter("no frontmatter here")


def test_update_frontmatter_preserves_comments_and_key_order_and_only_updates_given_keys():
    template = (
        "---\n"
        "# a comment\n"
        "ticker: \"\"\n"
        "company_name: \"\"\n"
        "nested:\n"
        "  x: null\n"
        "  y: 1\n"
        "---\n"
        "body\n"
    )
    result = update_frontmatter(template, {"ticker": "9999", "nested.x": 5})
    assert "# a comment" in result
    assert "9999" in result
    fm, body = split_frontmatter(result)
    assert body == "body\n"
    # key order preserved: ticker appears before company_name before nested
    assert fm.index("ticker") < fm.index("company_name") < fm.index("nested")
    assert "y: 1" in fm  # untouched key survives


def test_update_frontmatter_raises_for_unknown_key():
    template = "---\nticker: \"\"\n---\nbody\n"
    with pytest.raises(KeyError):
        update_frontmatter(template, {"not_a_real_key": 1})


def test_apply_frontmatter_updates_supports_dotted_paths():
    data = {"a": {"b": {"c": None}}}
    apply_frontmatter_updates(data, {"a.b.c": 42})
    assert data["a"]["b"]["c"] == 42


def test_insert_summary_block_replaces_only_between_markers():
    body = (
        "before\n"
        "<!-- snapshot-summary:start 生成スクリプトが書き込む。手で編集しない -->\n"
        "old content\n"
        "<!-- snapshot-summary:end -->\n"
        "after\n"
    )
    result = insert_summary_block(body, "new table")
    assert "old content" not in result
    assert "new table" in result
    assert result.startswith("before\n")
    assert result.endswith("after\n")


def test_insert_summary_block_raises_without_markers():
    with pytest.raises(ValueError):
        insert_summary_block("no markers here", "table")


def test_write_new_file_refuses_to_overwrite(tmp_path):
    path = tmp_path / "out.txt"
    write_new_file(path, "hello")
    assert path.read_text() == "hello"
    with pytest.raises(FileExistsError):
        write_new_file(path, "overwritten")
    assert path.read_text() == "hello"


def test_check_not_exists_raises_if_any_path_exists(tmp_path):
    existing = tmp_path / "a.json"
    existing.write_text("{}")
    missing = tmp_path / "b.md"
    with pytest.raises(FileExistsError):
        check_not_exists(existing, missing)
    assert not missing.exists()


def test_check_not_exists_passes_when_all_missing(tmp_path):
    check_not_exists(tmp_path / "a.json", tmp_path / "b.md")


def test_render_summary_table_matches_expected_format():
    table = render_summary_table(_sample_snapshot())
    assert "1,234円" in table
    assert "364億円" in table
    assert "0.45" in table
    assert "0.30" in table
    assert "7.4" in table
    assert "-73%" in table
    assert "900円" in table
    assert "2,400円" in table
    assert "3,800円" in table
    assert "3.5" in table
    assert "28か月" in table
    assert "2期" in table
    assert "資産B" in table
    assert "反転—" in table
    assert "rubric v1" in table
    assert "警告: 0件" in table


def test_render_summary_table_with_previous_snapshot_shows_diff():
    previous = {"valuation_metrics": {"pbr": 0.52}}
    table = render_summary_table(_sample_snapshot(), previous_snapshot=previous)
    assert "0.45（前回0.52）" in table


def test_render_summary_table_price_below_liquidation_shown_as_text():
    snapshot = _sample_snapshot()
    snapshot.value_range.risk_reward = "price_below_liquidation"
    table = render_summary_table(snapshot)
    assert "株価が清算価値を下回る" in table


@pytest.mark.skipif(not _TEMPLATE_PATH.exists(), reason="reports/_templates/initial.md not present")
def test_update_frontmatter_and_insert_summary_on_real_template():
    text = _TEMPLATE_PATH.read_text()
    updated = update_frontmatter(text, {"ticker": "9999", "as_of": "2026-10-01"})
    fm, body = split_frontmatter(updated)
    assert "9999" in fm
    body_with_summary = insert_summary_block(body, render_summary_table(_sample_snapshot()))
    assert "資産B" in body_with_summary
    assert "<!-- auto:summary_card -->" in body_with_summary  # rest of body untouched
