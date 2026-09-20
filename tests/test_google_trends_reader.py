"""Tests for GoogleTrendsReader."""

import datetime

from data_fetcher.readers.google_trends import GoogleTrendsReader

_HEADER = "date,keyword,interest,is_partial,fetched_at,window_start,window_end,code\n"


def test_read_ticker_filters_by_date(tmp_path):
    d = tmp_path / "google_trends"
    d.mkdir()
    (d / "1301.csv").write_text(
        _HEADER
        + "2016-09-06,極洋,0,false,2026-09-08,2016-09-06,2017-02-06,1301\n"
        + "2016-09-07,極洋,5,false,2026-09-08,2016-09-06,2017-02-06,1301\n"
    )

    reader = GoogleTrendsReader(data_dir=d)
    assert reader.available_tickers == ["1301"]
    assert reader.get_earliest_date("1301") == datetime.datetime(2016, 9, 6)
    assert reader.get_latest_date("1301") == datetime.datetime(2016, 9, 7)

    df = reader.read_ticker(
        "1301", start_date=datetime.datetime(2016, 9, 7), end_date=datetime.datetime(2016, 9, 7)
    )
    assert df["interest"].to_list() == [5]


def test_read_ticker_missing_symbol_returns_empty(tmp_path):
    d = tmp_path / "google_trends"
    d.mkdir()
    reader = GoogleTrendsReader(data_dir=d)
    assert len(reader.read_ticker("9999")) == 0
