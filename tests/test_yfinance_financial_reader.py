"""Tests for YFinanceFinancialReader."""

import datetime
import json

from data_fetcher.readers.yfinance import YFinanceFinancialReader


def test_read_financial_flattens_to_long_format(tmp_path):
    d = tmp_path / "financial"
    d.mkdir()
    (d / "A.json").write_text(
        json.dumps(
            {
                "2023-04-30": {"Net Income": 100.0, "Total Revenue": 1000.0},
                "2023-01-31": {"Net Income": 90.0, "Total Revenue": 900.0},
            }
        )
    )

    reader = YFinanceFinancialReader(data_dir=d)
    assert reader.available_tickers == ["A"]

    df = reader.read_financial("A")
    assert len(df) == 4
    assert df["period_end"].to_list() == [
        datetime.date(2023, 1, 31),
        datetime.date(2023, 1, 31),
        datetime.date(2023, 4, 30),
        datetime.date(2023, 4, 30),
    ]

    df_ranged = reader.read_financial(
        "A", start_date=datetime.datetime(2023, 4, 1), end_date=datetime.datetime(2023, 12, 31)
    )
    assert set(df_ranged["period_end"].to_list()) == {datetime.date(2023, 4, 30)}


def test_read_financial_missing_symbol_returns_empty(tmp_path):
    d = tmp_path / "financial"
    d.mkdir()
    reader = YFinanceFinancialReader(data_dir=d)
    assert len(reader.read_financial("NOPE")) == 0
