"""Regression test for RakutenReader.read_ohlc_impl.

Previously the group_by_dynamic(...).agg(...) result was never assigned or
returned, so this method always returned None instead of a DataFrame.
"""

import datetime

import pytest

from data_fetcher.readers.rakuten import RakutenReader


def test_read_ohlc_impl_returns_dataframe_not_none(tmp_path):
    d = tmp_path / "fxea"
    d.mkdir()
    # symbol,timestamp,timestamp_ms,bid,ask,size,size2 (no header)
    epoch_ms_1 = int(datetime.datetime(2023, 4, 3, 0, 0, 0).timestamp() * 1000)
    epoch_ms_2 = epoch_ms_1 + 1000
    (d / "20230403_USDJPY.txt").write_text(
        f"USDJPY,20230403000000,{epoch_ms_1},133.10,133.12,1,1\n"
        f"USDJPY,20230403000001,{epoch_ms_2},133.20,133.22,1,1\n"
    )

    reader = RakutenReader(data_dir=d)
    assert "USDJPY" in reader.available_tickers

    ohlc = reader.read_ohlc_impl(
        "USDJPY",
        datetime.timedelta(days=1),
        datetime.datetime(2023, 4, 1),
        datetime.datetime(2023, 4, 10),
    )

    assert ohlc is not None
    assert len(ohlc) == 1
    assert set(["open", "high", "low", "close", "volume", "max_spread"]).issubset(ohlc.columns)
    assert ohlc["open"].to_list() == pytest.approx([133.11])
    assert ohlc["close"].to_list() == pytest.approx([133.21])


def test_read_ohlc_impl_out_of_range_returns_empty_dataframe(tmp_path):
    d = tmp_path / "fxea"
    d.mkdir()
    epoch_ms = int(datetime.datetime(2023, 4, 3).timestamp() * 1000)
    (d / "20230403_USDJPY.txt").write_text(f"USDJPY,20230403000000,{epoch_ms},133.10,133.12,1,1\n")

    reader = RakutenReader(data_dir=d)
    # Query a date range that doesn't overlap the only file's date at all
    # (pre_start_date guard is only 1 day, so 2024 is safely out of range).
    ohlc = reader.read_ohlc_impl(
        "USDJPY",
        datetime.timedelta(days=1),
        datetime.datetime(2024, 1, 1),
        datetime.datetime(2024, 1, 10),
    )
    assert ohlc is not None
    assert len(ohlc) == 0
