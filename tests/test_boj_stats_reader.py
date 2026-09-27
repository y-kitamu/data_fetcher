"""Tests for BojStatsReader and domains.boj_stats.parser."""

import datetime

from data_fetcher.domains.boj_stats import parser as boj_parser
from data_fetcher.readers.boj_stats import BojStatsReader

_WIDE_SAMPLE = (
    ",,,202001,202002,202003\n"
    'PRCG20_2200000000,"企業物価指数 2020年基準/国内企業物価指数",'
    '"[国内企業物価指数] 総平均",102.1,101.7,100.8\n'
    'PRCG20_2200010001,"企業物価指数 2020年基準/国内企業物価指数",'
    '"大類別/工業製品",102.2,,100.9\n'
)

_TANKAN_SAMPLE = (
    "TK99F0000102BCQ00000,Q,202602,8444\n"
    "TK99F0000102BCQ01000,Q,202602,1355\n"
)


def test_parse_wide_csv_unpivots_to_long_format():
    df = boj_parser.parse_wide_csv(_WIDE_SAMPLE)
    assert df.columns == ["series_code", "date", "value"]
    assert df.height == 6  # 2 series x 3 periods
    row = df.filter(
        (df["series_code"] == "PRCG20_2200000000") & (df["date"] == "2020-01-01")
    )
    assert row["value"].to_list() == ["102.1"]

    # 欠損値(空文字)はnullに変換される
    missing = df.filter(
        (df["series_code"] == "PRCG20_2200010001") & (df["date"] == "2020-02-01")
    )
    assert missing["value"].to_list() == [None]


def test_parse_tankan_csv_converts_period_to_date():
    df = boj_parser.parse_tankan_csv(_TANKAN_SAMPLE)
    assert df.columns == ["series_code", "date", "value"]
    assert df.height == 2
    assert df.filter(df["series_code"] == "TK99F0000102BCQ00000")["date"].to_list() == [
        "2026-02-01"
    ]
    assert df.filter(df["series_code"] == "TK99F0000102BCQ00000")["value"].to_list() == [
        "8444"
    ]


_READER_HEADER = "series_code,date,value,fetched_at\n"


def test_reader_available_tickers_across_multiple_files(tmp_path):
    d = tmp_path / "boj_stats"
    d.mkdir()
    (d / "cgpi.csv").write_text(
        _READER_HEADER + "PRCG20_2200000000,2020-01-01,102.1,2026-09-26\n"
    )
    (d / "tankan.csv").write_text(
        _READER_HEADER + "TK99F0000102BCQ00000,2026-02-01,8444,2026-09-26\n"
    )

    reader = BojStatsReader(data_dir=d)
    assert reader.available_tickers == ["PRCG20_2200000000", "TK99F0000102BCQ00000"]


def test_reader_read_ticker_filters_by_series_code_and_date(tmp_path):
    d = tmp_path / "boj_stats"
    d.mkdir()
    (d / "cgpi.csv").write_text(
        _READER_HEADER
        + "PRCG20_2200000000,2020-01-01,102.1,2026-09-26\n"
        + "PRCG20_2200000000,2020-02-01,101.7,2026-09-26\n"
        + "OTHER_SERIES,2020-01-01,50.0,2026-09-26\n"
    )

    reader = BojStatsReader(data_dir=d)
    df = reader.read_ticker(
        "PRCG20_2200000000",
        start_date=datetime.datetime(2020, 2, 1),
        end_date=datetime.datetime(2020, 2, 28),
    )
    assert df.height == 1
    assert df["value"].to_list() == [101.7]

    assert reader.get_earliest_date("PRCG20_2200000000") == datetime.datetime(2020, 1, 1)
    assert reader.get_latest_date("PRCG20_2200000000") == datetime.datetime(2020, 2, 1)


def test_reader_read_ticker_keeps_revision_history_ordered_by_fetched_at(tmp_path):
    """同一dateに複数行(改定履歴)がある場合、fetched_at最大の行が最新値として
    最後に来る(sort後)ことを確認する。"""
    d = tmp_path / "boj_stats"
    d.mkdir()
    (d / "cgpi.csv").write_text(
        _READER_HEADER
        + "PRCG20_2200000000,2020-01-01,102.1,2026-08-01\n"
        + "PRCG20_2200000000,2020-01-01,102.5,2026-09-26\n"
    )

    reader = BojStatsReader(data_dir=d)
    df = reader.read_ticker("PRCG20_2200000000")
    assert df.height == 2
    assert df["value"].to_list() == [102.1, 102.5]
    assert df["fetched_at"].to_list()[-1] == "2026-09-26"


def test_reader_missing_series_returns_empty(tmp_path):
    d = tmp_path / "boj_stats"
    d.mkdir()
    (d / "cgpi.csv").write_text(
        _READER_HEADER + "PRCG20_2200000000,2020-01-01,102.1,2026-09-26\n"
    )

    reader = BojStatsReader(data_dir=d)
    assert len(reader.read_ticker("NOT_EXIST")) == 0


def test_reader_with_no_files_returns_empty(tmp_path):
    d = tmp_path / "boj_stats"
    d.mkdir()
    reader = BojStatsReader(data_dir=d)
    assert reader.available_tickers == []
    assert len(reader.read_ticker("ANY")) == 0
