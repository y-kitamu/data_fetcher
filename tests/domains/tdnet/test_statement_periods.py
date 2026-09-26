import polars as pl

from data_fetcher.domains.tdnet.statement_periods import (
    shape_edinet_statement_periods,
    shape_statement_periods,
)

_COLUMNS = [
    "concept",
    "segments",
    "period",
    "consolidated",
    "forecast",
    "previous_current",
    "context_id",
    "value",
    "doc_period",
    "doc_style",
    "source_file",
    "fiscal_year_end",
    "filing_datetime",
]


def _context_id(period: str, consolidated: str, forecast: str) -> str:
    return "_".join(part for part in (period, consolidated, forecast) if part)


def _row(
    concept: str,
    period: str,
    value: float,
    *,
    consolidated: str = "ConsolidatedMember",
    forecast: str = "ResultMember",
    previous_current: str = "",
    segments: str = "",
    doc_period: str = "a",
    doc_style: str = "edjp",
    source_file: str = "file1",
    fiscal_year_end: str = "2024-03-31",
    filing_datetime: str = "2024-05-10T15:00:00+09:00",
) -> dict:
    return {
        "concept": concept,
        "segments": segments,
        "period": period,
        "consolidated": consolidated,
        "forecast": forecast,
        "previous_current": previous_current,
        "context_id": _context_id(period, consolidated, forecast),
        "value": value,
        "doc_period": doc_period,
        "doc_style": doc_style,
        "source_file": source_file,
        "fiscal_year_end": fiscal_year_end,
        "filing_datetime": filing_datetime,
    }


def _df(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema={c: pl.Utf8 for c in _COLUMNS} | {"value": pl.Float64})


def test_shape_statement_periods_empty_dataframe_returns_empty_list():
    assert shape_statement_periods(pl.DataFrame()) == []


def test_shape_statement_periods_extracts_balance_sheet_income_statement_cash_flow():
    items = shape_statement_periods(
        _df(
            [
                _row("cash_and_equivalents", "CurrentYear", 500.0),
                _row("total_assets", "CurrentYear", 10_000.0),
                _row("liabilities", "CurrentYear", 4_000.0),
                _row("net_sales", "CurrentYear", 8_000.0),
                _row("operating_profit", "CurrentYear", 600.0),
                _row("cash_flow_from_operating_activities", "CurrentYear", 700.0),
                _row("capital_investment", "CurrentYear", 300.0),
            ]
        )
    )
    assert len(items) == 1
    item = items[0]
    assert item.fiscal_year == "2024-03"
    assert item.balance_sheet["cash"] == 500.0
    assert item.balance_sheet["total_assets"] == 10_000.0
    assert item.balance_sheet["total_liabilities"] == 4_000.0
    assert item.income_statement["revenue"] == 8_000.0
    assert item.income_statement["operating_income"] == 600.0
    assert item.cash_flow["operating_cf"] == 700.0
    assert item.cash_flow["capex"] == 300.0
    assert item.is_consolidated is True


def test_shape_statement_periods_extracts_share_counts():
    items = shape_statement_periods(
        _df(
            [
                _row("number_of_shares", "CurrentYear", 1_000_000.0),
                _row("treasury_shares", "CurrentYear", 50_000.0),
            ]
        )
    )
    assert items[0].shares["number_of_shares"] == 1_000_000.0
    assert items[0].shares["treasury_shares"] == 50_000.0


def test_shape_statement_periods_composite_key_sums_component_concepts():
    items = shape_statement_periods(
        _df(
            [
                _row("current_portion_bonds", "CurrentYear", 100.0),
                _row("short_term_bonds", "CurrentYear", 50.0),
                _row("lease_obligations_current", "CurrentYear", 10.0),
                _row("lease_obligations_noncurrent", "CurrentYear", 20.0),
            ]
        )
    )
    assert items[0].balance_sheet["current_portion_bonds"] == 150.0
    assert items[0].balance_sheet["lease_obligations"] == 30.0


def test_shape_statement_periods_fallback_key_uses_ifrs_combined_tag_when_split_tag_absent():
    items = shape_statement_periods(
        _df([_row("bonds_and_borrowings_current", "CurrentYear", 999.0)])
    )
    assert items[0].balance_sheet["short_term_debt"] == 999.0


def test_shape_statement_periods_fallback_key_prefers_split_tag_over_combined():
    items = shape_statement_periods(
        _df(
            [
                _row("short_term_borrowings", "CurrentYear", 111.0),
                _row("bonds_and_borrowings_current", "CurrentYear", 999.0),
            ]
        )
    )
    assert items[0].balance_sheet["short_term_debt"] == 111.0


def test_shape_statement_periods_skips_filing_with_missing_fiscal_year_end():
    row = _row("net_sales", "CurrentYear", 100.0)
    row["fiscal_year_end"] = None
    assert shape_statement_periods(_df([row])) == []


def test_shape_statement_periods_ignores_standalone_forecast_revision_notices():
    items = shape_statement_periods(
        _df([_row("net_sales", "CurrentYear", 100.0, doc_style="rvfc", forecast="ForecastMember")])
    )
    assert items == []


def test_shape_statement_periods_extracts_segments_with_member_suffix_stripped():
    items = shape_statement_periods(
        _df(
            [
                _row("net_sales", "CurrentYear", 5_000.0),
                _row(
                    "net_sales",
                    "CurrentYear",
                    3_000.0,
                    segments="ReportableSegmentsMember",
                ),
                _row(
                    "operating_profit",
                    "CurrentYear",
                    400.0,
                    segments="ReportableSegmentsMember",
                ),
            ]
        )
    )
    segments = items[0].segments
    assert len(segments) == 1
    assert segments[0].member == "ReportableSegments"
    assert segments[0].revenue == 3_000.0
    assert segments[0].operating_income == 400.0


_EDINET_COLUMNS = ["key", "start_date", "end_date", "edinet_key", "period", "value", "announce_date"]


def _edinet_df(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={c: pl.Utf8 for c in _EDINET_COLUMNS if c != "value" and c != "announce_date"}
        | {"value": pl.Utf8, "announce_date": pl.Datetime},
    )


def test_shape_edinet_statement_periods_groups_by_end_date_across_relative_labels():
    import datetime as dt

    rows = [
        {
            "key": "net_sales",
            "start_date": "2013-04-01",
            "end_date": "2014-03-31",
            "edinet_key": "jpcrp_cor:NetSalesSummaryOfBusinessResults",
            "period": "Prior1YearDuration",
            "value": "202387000000",
            "announce_date": dt.datetime(2015, 6, 24, 16, 20),
        },
        {
            "key": "net_sales",
            "start_date": "2014-04-01",
            "end_date": "2015-03-31",
            "edinet_key": "jpcrp_cor:NetSalesSummaryOfBusinessResults",
            "period": "CurrentYearDuration",
            "value": "218350000000",
            "announce_date": dt.datetime(2015, 6, 24, 16, 20),
        },
        {
            "key": "total_asset",
            "start_date": "2014-04-01",
            "end_date": "2015-03-31",
            "edinet_key": "jppfs_cor:Assets",
            "period": "CurrentYearInstant_NonConsolidatedMember",
            "value": "999999999",
            "announce_date": dt.datetime(2015, 6, 24, 16, 20),
        },
    ]
    periods = shape_edinet_statement_periods(_edinet_df(rows))
    by_fy = {p.fiscal_year: p for p in periods}
    assert by_fy["2014-03"].income_statement["revenue"] == 202387000000.0
    assert by_fy["2015-03"].income_statement["revenue"] == 218350000000.0
    # NonConsolidatedMember rows are excluded, so total_assets stays None.
    assert by_fy["2015-03"].balance_sheet["total_assets"] is None
    assert by_fy["2015-03"].source == "edinet"


def test_shape_edinet_statement_periods_empty_dataframe_returns_empty_list():
    assert shape_edinet_statement_periods(pl.DataFrame()) == []
