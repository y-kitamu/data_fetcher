import datetime as dt

from data_fetcher.domains.reports.statements import (
    build_annual_statements,
    build_quarterly_statements,
)
from data_fetcher.domains.tdnet.statement_periods import StatementPeriod

_JST = dt.timezone(dt.timedelta(hours=9))
YEN = 1_000_000  # StatementPeriod inputs are raw yen; snapshot outputs are millions of yen.


def _period(
    fiscal_year_end: str,
    *,
    source: str = "tdnet",
    quarter_number: int = 0,
    submitted_at: dt.datetime = dt.datetime(2024, 5, 10, tzinfo=_JST),
    balance_sheet: dict | None = None,
    income_statement: dict | None = None,
    cash_flow: dict | None = None,
) -> StatementPeriod:
    return StatementPeriod(
        source=source,
        fiscal_year_end=dt.date.fromisoformat(fiscal_year_end),
        doc_period="a" if quarter_number == 0 else "q",
        quarter_number=quarter_number,
        submitted_at=submitted_at,
        is_consolidated=True,
        balance_sheet=balance_sheet or {},
        income_statement=income_statement or {},
        cash_flow=cash_flow or {},
    )


def test_build_annual_statements_converts_yen_to_millions():
    tdnet = [_period("2024-03-31", income_statement={"revenue": 1000.0 * YEN})]
    result = build_annual_statements(tdnet, [])
    assert result.income_statement["revenue"] == [1000.0]


def test_build_annual_statements_prefers_tdnet_and_fills_gap_from_edinet():
    tdnet = [_period("2024-03-31", income_statement={"revenue": 1000.0 * YEN})]
    edinet = [
        _period("2024-03-31", source="edinet", income_statement={"revenue": 999.0 * YEN}),
        _period("2012-03-31", source="edinet", income_statement={"revenue": 500.0 * YEN}),
    ]
    result = build_annual_statements(tdnet, edinet, max_years=15)

    assert result.fiscal_years == ["2012-03", "2024-03"]
    assert result.income_statement["revenue"] == [500.0, 1000.0]
    assert result.by_series["income_statement"]["tdnet"] == ["2024-03"]
    assert result.by_series["income_statement"]["edinet"] == ["2012-03"]


def test_build_annual_statements_records_source_mismatch_but_keeps_tdnet_value():
    tdnet = [_period("2024-03-31", income_statement={"revenue": 1000.0 * YEN})]
    edinet = [_period("2024-03-31", source="edinet", income_statement={"revenue": 1500.0 * YEN})]
    result = build_annual_statements(tdnet, edinet)

    assert result.income_statement["revenue"] == [1000.0]
    assert len(result.warnings) == 1
    assert result.warnings[0].code == "SOURCE_MISMATCH"
    assert result.warnings[0].field == "income_statement.revenue"


def test_build_annual_statements_within_tolerance_is_not_a_mismatch():
    tdnet = [_period("2024-03-31", income_statement={"revenue": 1000.0 * YEN})]
    edinet = [_period("2024-03-31", source="edinet", income_statement={"revenue": 1005.0 * YEN})]
    result = build_annual_statements(tdnet, edinet)
    assert result.warnings == []


def test_build_annual_statements_uses_latest_submission_for_corrections():
    tdnet = [
        _period(
            "2024-03-31",
            submitted_at=dt.datetime(2024, 5, 10, tzinfo=_JST),
            income_statement={"revenue": 1000.0 * YEN},
        ),
        _period(
            "2024-03-31",
            submitted_at=dt.datetime(2024, 6, 1, tzinfo=_JST),
            income_statement={"revenue": 1100.0 * YEN},
        ),
    ]
    result = build_annual_statements(tdnet, [])
    assert result.income_statement["revenue"] == [1100.0]


def test_build_annual_statements_caps_to_max_years():
    tdnet = [_period(f"{2000 + i}-03-31") for i in range(20)]
    result = build_annual_statements(tdnet, [], max_years=15)
    assert len(result.fiscal_years) == 15
    assert result.fiscal_years[0] == "2005-03"
    assert result.fiscal_years[-1] == "2019-03"


def test_build_quarterly_statements_differences_cumulative_to_standalone():
    periods = [
        _period(
            "2024-03-31", quarter_number=1,
            income_statement={"revenue": 100.0 * YEN, "operating_income": 10.0 * YEN, "net_income": 5.0 * YEN},
        ),
        _period(
            "2024-03-31", quarter_number=2,
            income_statement={"revenue": 220.0 * YEN, "operating_income": 25.0 * YEN, "net_income": 12.0 * YEN},
        ),
        _period(
            "2024-03-31", quarter_number=3,
            income_statement={"revenue": 330.0 * YEN, "operating_income": 40.0 * YEN, "net_income": 18.0 * YEN},
        ),
        _period(
            "2024-03-31", quarter_number=0,
            income_statement={"revenue": 500.0 * YEN, "operating_income": 60.0 * YEN, "net_income": 30.0 * YEN},
        ),
    ]
    result = build_quarterly_statements(periods)

    assert result.periods == ["2024-03-Q1", "2024-03-Q2", "2024-03-Q3", "2024-03-Q4"]
    assert result.revenue == [100.0, 120.0, 110.0, 170.0]
    assert result.operating_income == [10.0, 15.0, 15.0, 20.0]
    assert result.net_income == [5.0, 7.0, 6.0, 12.0]


def test_build_quarterly_statements_missing_q3_means_no_q4_value():
    periods = [
        _period("2024-03-31", quarter_number=1, income_statement={"revenue": 100.0 * YEN}),
        _period("2024-03-31", quarter_number=0, income_statement={"revenue": 500.0 * YEN}),
    ]
    result = build_quarterly_statements(periods)
    assert result.periods == ["2024-03-Q1"]


def test_build_quarterly_statements_caps_to_max_quarters():
    periods = []
    for year in range(2020, 2025):
        for q in (1, 2, 3):
            periods.append(
                _period(f"{year}-03-31", quarter_number=q, income_statement={"revenue": float(q) * YEN})
            )
        periods.append(_period(f"{year}-03-31", quarter_number=0, income_statement={"revenue": 10.0 * YEN}))
    result = build_quarterly_statements(periods, max_quarters=12)
    assert len(result.periods) == 12
