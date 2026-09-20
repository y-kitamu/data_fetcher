"""Tests for JpTickerThemesReader."""

import datetime

from data_fetcher.readers.jp_ticker_themes import JpTickerThemesReader


def test_read_returns_latest_snapshot(tmp_path):
    d = tmp_path / "jp_ticker_themes"
    d.mkdir()
    (d / "20260602.csv").write_text('ticker,themes\n1301,"[\'水産\']"\n')
    (d / "20260603.csv").write_text('ticker,themes\n1301,"[\'水産\', \'冷凍食品\']"\n')

    reader = JpTickerThemesReader(data_dir=d)

    assert reader.available_tickers == ["1301"]

    df = reader.read()
    assert df["themes"].to_list() == [["水産", "冷凍食品"]]

    df_ticker = reader.read(ticker="1301")
    assert len(df_ticker) == 1

    df_asof = reader.read(as_of=datetime.date(2026, 6, 2))
    assert df_asof["themes"].to_list() == [["水産"]]


def test_read_with_no_files_returns_empty(tmp_path):
    d = tmp_path / "jp_ticker_themes"
    d.mkdir()
    reader = JpTickerThemesReader(data_dir=d)
    assert reader.available_tickers == []
    assert len(reader.read()) == 0
