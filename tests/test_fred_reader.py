"""Tests for FredReader."""

import datetime

from data_fetcher.readers.fred import FredReader

_HEADER = "date,value,fetched_at\n"


def test_available_tickers_and_read_ticker_filters_by_date(tmp_path):
    d = tmp_path / "fred"
    d.mkdir()
    (d / "HOUST.csv").write_text(
        _HEADER + "2024-01-01,1300.0,2026-09-26\n" + "2024-02-01,1350.0,2026-09-26\n"
    )

    reader = FredReader(data_dir=d)
    assert reader.available_tickers == ["HOUST"]

    df = reader.read_ticker(
        "HOUST",
        start_date=datetime.datetime(2024, 2, 1),
        end_date=datetime.datetime(2024, 2, 28),
    )
    assert df["value"].to_list() == [1350.0]
    assert reader.get_earliest_date("HOUST") == datetime.datetime(2024, 1, 1)
    assert reader.get_latest_date("HOUST") == datetime.datetime(2024, 2, 1)


def test_read_ticker_keeps_revision_history(tmp_path):
    d = tmp_path / "fred"
    d.mkdir()
    (d / "HOUST.csv").write_text(
        _HEADER + "2024-01-01,1300.0,2026-08-01\n" + "2024-01-01,1310.0,2026-09-26\n"
    )

    reader = FredReader(data_dir=d)
    df = reader.read_ticker("HOUST")
    assert df.height == 2
    assert df["value"].to_list() == [1300.0, 1310.0]
    assert df["fetched_at"].to_list()[-1] == "2026-09-26"


def test_read_ticker_missing_symbol_returns_empty(tmp_path):
    d = tmp_path / "fred"
    d.mkdir()
    reader = FredReader(data_dir=d)
    assert len(reader.read_ticker("NOT_EXIST")) == 0
