"""Tests for TaisyakuHistoryReader / TaisyakuZandakaReader."""

import datetime

from data_fetcher.readers.taisyaku import TaisyakuHistoryReader, TaisyakuZandakaReader

_HISTORY_HEADER = '"銘柄コード","銘柄名","市場区分","貸借区分","基準日","融資残高（株）"'
_ZANDAKA_HEADER = '"銘柄コード","銘柄名","取引所区分名","融資新規株数"'


def _write(path, header, rows):
    with open(path, "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for row in rows:
            f.write(",".join(f'"{v}"' for v in row) + "\n")


def test_history_reader_filters_by_symbol_and_date(tmp_path):
    d = tmp_path / "history"
    d.mkdir()
    _write(d / "20230403.csv", _HISTORY_HEADER, [["1301", "極洋", "東証", "貸借", "20230331", "5400"]])
    _write(d / "20230404.csv", _HISTORY_HEADER, [["1301", "極洋", "東証", "貸借", "20230403", "5500"]])
    _write(d / "20230405.csv", _HISTORY_HEADER, [["7203", "トヨタ", "東証", "貸借", "20230404", "100"]])

    reader = TaisyakuHistoryReader(data_dir=d)

    assert reader.available_tickers == ["7203"]  # latest file's tickers only
    assert reader.get_earliest_date("1301") == datetime.datetime(2023, 4, 3)
    assert reader.get_latest_date("1301") == datetime.datetime(2023, 4, 5)

    df = reader.read_ticker("1301")
    assert df["融資残高（株）"].to_list() == [5400, 5500]
    assert df["datetime"].to_list() == [
        datetime.datetime(2023, 4, 3),
        datetime.datetime(2023, 4, 4),
    ]

    df_ranged = reader.read_ticker(
        "1301", start_date=datetime.datetime(2023, 4, 4), end_date=datetime.datetime(2023, 4, 4)
    )
    assert len(df_ranged) == 1


def test_zandaka_reader_glob_pattern(tmp_path):
    d = tmp_path / "zandaka"
    d.mkdir()
    _write(d / "20260827_kakuho.csv", _ZANDAKA_HEADER, [["1301", "極洋", "東証およびＰＴＳ", "900"]])

    reader = TaisyakuZandakaReader(data_dir=d)
    assert reader.available_tickers == ["1301"]
    df = reader.read_ticker("1301")
    assert len(df) == 1
    assert df["融資新規株数"].to_list() == [900]


def test_unknown_symbol_returns_empty(tmp_path):
    d = tmp_path / "history"
    d.mkdir()
    _write(d / "20230403.csv", _HISTORY_HEADER, [["1301", "極洋", "東証", "貸借", "20230331", "5400"]])

    reader = TaisyakuHistoryReader(data_dir=d)
    assert len(reader.read_ticker("9999")) == 0


def test_history_reader_defaults_to_tosho_only(tmp_path):
    d = tmp_path / "history"
    d.mkdir()
    _write(
        d / "20260918.csv",
        _HISTORY_HEADER,
        [
            ["7203", "トヨタ自動車", "東証", "貸借", "20260930", "978400"],
            ["7203", "トヨタ自動車", "名証", "貸借", "20260930", "0"],
        ],
    )

    reader = TaisyakuHistoryReader(data_dir=d)

    df_default = reader.read_ticker("7203")
    assert df_default["市場区分"].to_list() == ["東証"]

    df_meisho = reader.read_ticker("7203", market="名証")
    assert df_meisho["市場区分"].to_list() == ["名証"]

    df_both = reader.read_ticker("7203", market=("東証", "名証"))
    assert sorted(df_both["市場区分"].to_list()) == ["名証", "東証"]

    df_all = reader.read_ticker("7203", market=None)
    assert sorted(df_all["市場区分"].to_list()) == ["名証", "東証"]


def test_zandaka_reader_matches_tosho_with_pts_suffix(tmp_path):
    d = tmp_path / "zandaka"
    d.mkdir()
    _write(
        d / "20260917_kakuho.csv",
        _ZANDAKA_HEADER,
        [
            ["7203", "トヨタ自動車", "東証およびＰＴＳ", "1000"],
            ["7203", "トヨタ自動車", "名証", "0"],
        ],
    )

    reader = TaisyakuZandakaReader(data_dir=d)

    df_default = reader.read_ticker("7203")
    assert df_default["取引所区分名"].to_list() == ["東証およびＰＴＳ"]

    df_meisho = reader.read_ticker("7203", market="名証")
    assert df_meisho["取引所区分名"].to_list() == ["名証"]

