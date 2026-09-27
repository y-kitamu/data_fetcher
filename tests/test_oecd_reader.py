"""Tests for OecdReader."""

import datetime

from data_fetcher.readers.oecd import OecdReader

_HEADER = "date,value,fetched_at\n"


def test_available_tickers_and_read_ticker_filters_by_date(tmp_path):
    d = tmp_path / "oecd"
    d.mkdir()
    (d / "CLI_JPN.csv").write_text(
        _HEADER + "2025-05-01,99.4,2026-09-26\n" + "2025-06-01,99.5,2026-09-26\n"
    )

    reader = OecdReader(data_dir=d)
    assert reader.available_tickers == ["CLI_JPN"]

    df = reader.read_ticker(
        "CLI_JPN",
        start_date=datetime.datetime(2025, 6, 1),
        end_date=datetime.datetime(2025, 6, 30),
    )
    assert df["value"].to_list() == [99.5]
    assert reader.get_earliest_date("CLI_JPN") == datetime.datetime(2025, 5, 1)
    assert reader.get_latest_date("CLI_JPN") == datetime.datetime(2025, 6, 1)


def test_read_ticker_keeps_revision_history(tmp_path):
    d = tmp_path / "oecd"
    d.mkdir()
    (d / "CLI_JPN.csv").write_text(
        _HEADER + "2025-05-01,99.4,2026-08-01\n" + "2025-05-01,99.6,2026-09-26\n"
    )

    reader = OecdReader(data_dir=d)
    df = reader.read_ticker("CLI_JPN")
    assert df.height == 2
    assert df["value"].to_list() == [99.4, 99.6]


def test_read_ticker_missing_symbol_returns_empty(tmp_path):
    d = tmp_path / "oecd"
    d.mkdir()
    reader = OecdReader(data_dir=d)
    assert len(reader.read_ticker("NOT_EXIST")) == 0
