"""Tests for KabuTickReader."""

import datetime
import gzip
import json

import pytest

from data_fetcher.readers.kabu_tick import KabuTickReader


def _write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _write_jsonl_gz(path, rows):
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


@pytest.fixture
def tick_dir(tmp_path):
    d = tmp_path / "ticks"
    d.mkdir()
    return d


def test_read_ticker_filters_and_diffs_volume(tick_dir):
    day1_rows = [
        {
            "received_at": "2026-07-13T09:00:00.100",
            "Symbol": "1458",
            "TradingVolume": 100.0,
            "TradingValue": 9000000.0,
        },  # board-only update, no CurrentPrice -> not a tick, but updates cumulative
        {
            "received_at": "2026-07-13T09:00:01.100",
            "Symbol": "1458",
            "CurrentPrice": 90000.0,
            "CurrentPriceTime": "2026-07-13T09:00:01+09:00",
            "CurrentPriceChangeStatus": "0057",
            "TradingVolume": 150.0,
            "TradingValue": 13500000.0,
        },
        {
            "received_at": "2026-07-13T09:00:02.100",
            "Symbol": "1458",
            "CurrentPrice": 90000.0,
            "CurrentPriceTime": "2026-07-13T09:00:02+09:00",
            "CurrentPriceChangeStatus": "0000",  # no-op, must be filtered out
            "TradingVolume": 150.0,
            "TradingValue": 13500000.0,
        },
        {
            "received_at": "2026-07-13T09:00:03.100",
            "Symbol": "1458",
            "CurrentPrice": 90100.0,
            "CurrentPriceTime": "2026-07-13T09:00:03+09:00",
            "CurrentPriceChangeStatus": "0058",
            "TradingVolume": 200.0,
            "TradingValue": 18000000.0,
        },
    ]
    _write_jsonl(tick_dir / "1458_2026-07-13.jsonl", day1_rows)

    day2_rows = [
        {
            "received_at": "2026-07-14T09:00:00.100",
            "Symbol": "1458",
            "CurrentPrice": 90200.0,
            "CurrentPriceTime": "2026-07-14T09:00:00+09:00",
            "CurrentPriceChangeStatus": "0057",
            "TradingVolume": 50.0,
            "TradingValue": 4500000.0,
        },
    ]
    _write_jsonl_gz(tick_dir / "1458_2026-07-14.jsonl.gz", day2_rows)

    reader = KabuTickReader(data_dir=tick_dir)
    df = reader.read_ticker(
        "1458", end_date=datetime.datetime(2026, 7, 15)
    )

    assert df.columns == ["datetime", "price", "volume", "amount"]
    # Only 3 real ticks: line1 has no price, line3 is "0000"-filtered.
    assert len(df) == 3
    assert df["price"].to_list() == [90000.0, 90100.0, 90200.0]
    # forward-filled cumulative TradingVolume across all rows: [100, 150, 150, 200]
    # -> diff: [null, 50, 0, 50] -> fill_null(raw): [100, 50, 0, 50]
    # kept rows (status 0057, 0058) -> volume [50, 50]; day2 first tick -> diff against implicit 0 -> 50
    assert df["volume"].to_list() == [50.0, 50.0, 50.0]
    assert df["amount"].to_list() == [4500000.0, 4500000.0, 4500000.0]
    assert df["datetime"].to_list() == [
        datetime.datetime(2026, 7, 13, 9, 0, 1),
        datetime.datetime(2026, 7, 13, 9, 0, 3),
        datetime.datetime(2026, 7, 14, 9, 0, 0),
    ]


def test_available_tickers_and_dates(tick_dir):
    _write_jsonl(
        tick_dir / "1458_2026-07-13.jsonl",
        [
            {
                "CurrentPrice": 100.0,
                "CurrentPriceTime": "2026-07-13T09:00:00+09:00",
            }
        ],
    )
    _write_jsonl_gz(
        tick_dir / "7203_2026-07-10.jsonl.gz",
        [
            {
                "CurrentPrice": 2000.0,
                "CurrentPriceTime": "2026-07-10T09:00:00+09:00",
            }
        ],
    )
    reader = KabuTickReader(data_dir=tick_dir)
    assert reader.available_tickers == ["1458", "7203"]
    assert reader.get_earliest_date("1458") == datetime.datetime(2026, 7, 13)
    assert reader.get_latest_date("1458") == datetime.datetime(2026, 7, 13)
    assert reader.get_earliest_date("9999") == datetime.datetime(1970, 1, 1)
    assert reader.get_latest_date("9999") == datetime.datetime(1970, 1, 1)


def test_read_ticker_tolerates_truncated_last_line(tick_dir):
    path = tick_dir / "1458_2026-07-13.jsonl"
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {
                    "CurrentPrice": 100.0,
                    "CurrentPriceTime": "2026-07-13T09:00:00+09:00",
                    "TradingVolume": 10.0,
                    "TradingValue": 1000.0,
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        # Truncated line simulating a live writer mid-flush, no trailing newline.
        f.write('{"CurrentPrice": 101.0, "CurrentPriceTime": "2026-07-13T09:0')

    reader = KabuTickReader(data_dir=tick_dir)
    df = reader.read_ticker("1458")
    assert len(df) == 1


def test_read_ticker_no_data_returns_empty_dataframe(tick_dir):
    reader = KabuTickReader(data_dir=tick_dir)
    df = reader.read_ticker("9999")
    assert len(df) == 0


def test_read_ohlc_impl(tick_dir):
    rows = [
        {
            "CurrentPrice": 100.0,
            "CurrentPriceTime": "2026-07-13T09:00:00+09:00",
            "CurrentPriceChangeStatus": "0057",
            "TradingVolume": 10.0,
            "TradingValue": 1000.0,
        },
        {
            "CurrentPrice": 105.0,
            "CurrentPriceTime": "2026-07-13T09:00:30+09:00",
            "CurrentPriceChangeStatus": "0057",
            "TradingVolume": 20.0,
            "TradingValue": 3000.0,
        },
        {
            "CurrentPrice": 95.0,
            "CurrentPriceTime": "2026-07-13T09:01:10+09:00",
            "CurrentPriceChangeStatus": "0058",
            "TradingVolume": 25.0,
            "TradingValue": 3500.0,
        },
    ]
    _write_jsonl(tick_dir / "1458_2026-07-13.jsonl", rows)

    reader = KabuTickReader(data_dir=tick_dir)
    ohlc = reader.read_ohlc_impl(
        "1458",
        datetime.timedelta(minutes=1),
        datetime.datetime(2026, 7, 13),
        datetime.datetime(2026, 7, 14),
    )
    assert ohlc.columns == ["datetime", "open", "high", "low", "close", "volume"]
    assert len(ohlc) == 2
    assert ohlc["open"][0] == 100.0
    assert ohlc["high"][0] == 105.0
    assert ohlc["low"][0] == 100.0
    assert ohlc["close"][0] == 105.0
    assert ohlc["volume"][0] == 20.0  # diff(10-0)=10 + diff(20-10)=10
    assert ohlc["close"][1] == 95.0


def test_date_range_filtering_skips_files_outside_range(tick_dir):
    _write_jsonl(
        tick_dir / "1458_2026-01-01.jsonl",
        [{"CurrentPrice": 1.0, "CurrentPriceTime": "2026-01-01T09:00:00+09:00"}],
    )
    _write_jsonl(
        tick_dir / "1458_2026-07-13.jsonl",
        [{"CurrentPrice": 2.0, "CurrentPriceTime": "2026-07-13T09:00:00+09:00"}],
    )
    reader = KabuTickReader(data_dir=tick_dir)

    opened = []
    orig = reader._read_raw_lines

    def _tracking_read(path):
        opened.append(path)
        return orig(path)

    reader._read_raw_lines = _tracking_read

    df = reader.read_ticker(
        "1458",
        start_date=datetime.datetime(2026, 7, 1),
        end_date=datetime.datetime(2026, 7, 31),
    )
    assert len(opened) == 1
    assert len(df) == 1
