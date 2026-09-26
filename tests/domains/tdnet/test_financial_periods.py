import polars as pl

from data_fetcher.domains.tdnet.financial_periods import (
    _extract,
    _pick_total_value,
    shape_financial_periods,
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
    # Mirrors a real TDnet context id closely enough for _pick_total_value's
    # segment-count disambiguation and _detect_window's substring match to
    # behave the same way they do on real data.
    return "_".join(part for part in (period, consolidated, forecast) if part)


def _row(
    concept: str,
    period: str,
    value: float,
    *,
    consolidated: str = "ConsolidatedMember",
    forecast: str = "ResultMember",
    previous_current: str = "",
    context_id: str | None = None,
    doc_period: str = "a",
    doc_style: str = "edjp",
    source_file: str = "file1",
    fiscal_year_end: str = "2024-03-31",
    filing_datetime: str = "2024-05-10T15:00:00+09:00",
) -> dict:
    return {
        "concept": concept,
        "segments": "",
        "period": period,
        "consolidated": consolidated,
        "forecast": forecast,
        "previous_current": previous_current,
        "context_id": context_id or _context_id(period, consolidated, forecast),
        "value": value,
        "doc_period": doc_period,
        "doc_style": doc_style,
        "source_file": source_file,
        "fiscal_year_end": fiscal_year_end,
        "filing_datetime": filing_datetime,
    }


def _df(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema={c: pl.Utf8 for c in _COLUMNS} | {"value": pl.Float64})


def test_shape_financial_periods_empty_dataframe_returns_empty_list() -> None:
    assert shape_financial_periods(pl.DataFrame()) == []


def test_shape_financial_periods_assembles_actual_and_forecast_for_settlement() -> None:
    items = shape_financial_periods(
        _df(
            [
                _row("net_sales", "CurrentYear", 1000.0),
                _row("net_sales", "NextYear", 1200.0, forecast="ForecastMember"),
                _row("revenue_yoy", "CurrentYear", 0.05),
                _row("eps", "CurrentYear", 10.0),
            ]
        )
    )
    assert len(items) == 1
    item = items[0]
    assert item.total_revenue.actual == 1000.0
    assert item.total_revenue.forecast == 1200.0
    # revenue_yoy has no forecast tag in real TDnet data - forecast should be
    # None rather than requiring a hand-maintained whitelist to skip it.
    assert item.revenue_yoy.actual == 0.05
    assert item.revenue_yoy.forecast is None
    assert item.category == "本決算"
    assert item.is_consolidated is True
    assert item.eps.actual == 10.0


def test_shape_financial_periods_builds_revision_item_with_pl_forecast_fields() -> None:
    # Regression test for the 9984 bug: a forecast-revision notice used to
    # only extract net_income/eps forecasts. total_revenue/operating_income/
    # ordinary_profit forecasts should now be populated the same way.
    items = shape_financial_periods(
        _df(
            [
                _row(
                    "net_income",
                    "CurrentYear",
                    -750_000_000_000.0,
                    forecast="ForecastMember",
                    doc_style="rvfc",
                    source_file="rev1",
                ),
                _row(
                    "net_sales",
                    "CurrentYear",
                    6_150_000_000_000.0,
                    forecast="ForecastMember",
                    doc_style="rvfc",
                    source_file="rev1",
                ),
                _row(
                    "operating_profit",
                    "CurrentYear",
                    -1_350_000_000_000.0,
                    forecast="ForecastMember",
                    doc_style="rvfc",
                    source_file="rev1",
                ),
            ]
        )
    )
    assert len(items) == 1
    item = items[0]
    assert item.is_forecast_revision is True
    assert item.category == "業績予想修正"
    assert item.net_income.forecast == -750_000_000_000.0
    assert item.total_revenue.forecast == 6_150_000_000_000.0
    assert item.operating_income.forecast == -1_350_000_000_000.0
    # No actuals exist on a standalone revision notice.
    assert item.net_income.actual is None
    assert item.total_revenue.actual is None


def test_extract_falls_back_to_current_quarter_when_ytd_missing() -> None:
    # Some filers tag balance-sheet (instant) concepts under the generic
    # "CurrentQuarter" label rather than the duration-style
    # "CurrentAccumulatedQ2" window that was actually detected.
    value = _extract(
        _df([_row("net_assets", "CurrentQuarter", 12345.0, forecast="")]),
        concept="net_assets",
        periods=["CurrentAccumulatedQ2", "CurrentYTD"],
        consolidateds=["ConsolidatedMember"],
        forecasts=["ResultMember", ""],
    )
    assert value == 12345.0


def test_pick_total_value_prefers_row_matching_segment_count() -> None:
    # An IFRS filer can tag a sub-component (e.g. minority interest) under the
    # same concept/period/consolidated/forecast as the total, distinguished
    # only by an extra context dimension. The row whose context_id segment
    # count matches consolidated+forecast+1 should win over a stray extra row.
    rows = pl.DataFrame(
        {
            "consolidated": ["ConsolidatedMember", "ConsolidatedMember"],
            "forecast": ["ResultMember", "ResultMember"],
            "context_id": ["a_b_c_d", "a_b_c"],  # 4 segments (sub-component), 3 segments (total)
            "value": [999.0, 500.0],
        }
    )
    assert _pick_total_value(rows) == 500.0


def test_pick_total_value_empty_rows_returns_none() -> None:
    rows = pl.DataFrame({"consolidated": [], "forecast": [], "context_id": [], "value": []})
    assert _pick_total_value(rows) is None
