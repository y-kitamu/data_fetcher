import polars as pl

from data_fetcher.domains.edinet.csv_export import build_fact_rows
from data_fetcher.domains.tdnet.constants.taxonomy_group import ELEMENT_TO_CONCEPT
from data_fetcher.domains.tdnet.csv_export import ROW_SCHEMA
from data_fetcher.domains.tdnet.statement_periods import shape_statement_periods

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


def _edinet_df(edinet_rows: list[list[str]]) -> pl.DataFrame:
    """EDINET CSV行をdata/edinet/csvの形式に変換し、Readerと同じくconcept列を付ける。"""
    doc = {
        "docID": "S100TEST",
        "edinetCode": "E00012",
        "secCode": "13010",
        "submitDateTime": "2025-06-25 16:11",
        "periodStart": "2024-04-01",
        "periodEnd": "2025-03-31",
    }
    df = pl.DataFrame(build_fact_rows(doc, edinet_rows), schema=ROW_SCHEMA)
    return df.with_columns(
        pl.col("element_id")
        .replace_strict(ELEMENT_TO_CONCEPT, default=None, return_dtype=pl.Utf8)
        .alias("concept")
    )


def _edinet_row(element_id, context_id, consolidation, value, unit="JPY"):
    return [element_id, "", context_id, "", consolidation, "", unit, "", value]


def _dei(name, value):
    return _edinet_row(f"jpdei_cor:{name}", "FilingDateInstant", "その他", value, "－")


def test_shape_statement_periods_handles_edinet_rows():
    rows = [
        _dei("AccountingStandardsDEI", "Japan GAAP"),
        _dei("WhetherConsolidatedFinancialStatementsArePreparedDEI", "true"),
        _dei("TypeOfCurrentPeriodDEI", "FY"),
        _dei("CurrentFiscalYearStartDateDEI", "2024-04-01"),
        _dei("CurrentFiscalYearEndDateDEI", "2025-03-31"),
        _dei("CurrentPeriodEndDateDEI", "2025-03-31"),
        # 経営指標等(jpcrp_cor)は連結・個別が「その他」で、conceptには解決されない
        _edinet_row("jpcrp_cor:NetSalesSummaryOfBusinessResults", "CurrentYearDuration", "その他", "1"),
        _edinet_row("jppfs_cor:NetSales", "Prior1YearDuration", "連結", "261604000000"),
        _edinet_row("jppfs_cor:NetSales", "CurrentYearDuration", "連結", "302681000000"),
        _edinet_row(
            "jppfs_cor:NetSales",
            "CurrentYearDuration_jpcrp030000-asr_E00012-000MarineProductsBusinessReportableSegmentMember",
            "連結",
            "154035000000",
        ),
        _edinet_row("jppfs_cor:NetSales", "CurrentYearDuration_NonConsolidatedMember", "個別", "272790000000"),
        _edinet_row("jppfs_cor:OperatingIncome", "CurrentYearDuration", "連結", "11079000000"),
        _edinet_row("jppfs_cor:Assets", "CurrentYearInstant", "連結", "182125000000"),
        _edinet_row("jppfs_cor:Assets", "CurrentYearInstant_NonConsolidatedMember", "個別", "148550000000"),
    ]

    periods = shape_statement_periods(_edinet_df(rows), source="edinet")

    assert len(periods) == 1
    period = periods[0]
    assert period.source == "edinet"
    assert period.fiscal_year == "2025-03"
    assert period.quarter_number == 0
    assert period.is_consolidated is True
    assert period.income_statement["revenue"] == 302681000000.0
    assert period.income_statement["operating_income"] == 11079000000.0
    assert period.balance_sheet["total_assets"] == 182125000000.0
    assert [s.member for s in period.segments] == [
        "jpcrp030000-asr_E00012-000MarineProductsBusinessReportableSegment"
    ]


def test_shape_statement_periods_does_not_take_nonconsolidated_values_for_edinet():
    rows = [
        _dei("WhetherConsolidatedFinancialStatementsArePreparedDEI", "true"),
        _dei("TypeOfCurrentPeriodDEI", "FY"),
        _dei("AccountingStandardsDEI", "IFRS"),
        _dei("CurrentFiscalYearEndDateDEI", "2025-03-31"),
        # 連結売上高が提出者独自の要素のみで、conceptに解決されるのは個別の売上高だけ
        _edinet_row("jpcrp030000-asr_E02144-000:OperatingRevenuesIFRS", "CurrentYearDuration", "その他", "48"),
        _edinet_row("jppfs_cor:NetSales", "CurrentYearDuration_NonConsolidatedMember", "個別", "18"),
        _edinet_row("jpigp_cor:AssetsIFRS", "CurrentYearInstant", "その他", "93"),
    ]

    period = shape_statement_periods(_edinet_df(rows), source="edinet")[0]

    assert period.is_consolidated is True
    assert period.income_statement["revenue"] is None
    assert period.balance_sheet["total_assets"] == 93.0
