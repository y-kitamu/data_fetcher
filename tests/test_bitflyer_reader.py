"""Tests for BitflyerReader (raw tick executions)."""

import datetime
import gzip

from data_fetcher.readers.bitflyer import BitflyerReader

_HEADER = "id,side,price,size,exec_date,buy_child_order_acceptance_id,sell_child_order_acceptance_id\n"


def _write_gz(path, rows_csv):
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write(_HEADER + rows_csv)


def test_read_ticker_and_ohlc(tmp_path):
    d = tmp_path / "tick"
    day_dir = d / "20260506"
    day_dir.mkdir(parents=True)
    _write_gz(
        day_dir / "20260506_BTC_JPY.csv.gz",
        "1,SELL,12761904.0,0.01,2026-05-06T00:00:01.1607229Z,A,B\n"
        "2,BUY,12762000.0,0.02,2026-05-06T00:00:02.0000000Z,C,D\n",
    )

    reader = BitflyerReader(data_dir=d)
    assert reader.available_tickers == ["BTC_JPY"]
    assert reader.get_earliest_date("BTC_JPY") == datetime.datetime(2026, 5, 6)
    assert reader.get_latest_date("BTC_JPY") == datetime.datetime(2026, 5, 6)

    df = reader.read_ticker("BTC_JPY", timezone_delta=datetime.timedelta(hours=0))
    assert df["side"].to_list() == ["SELL", "BUY"]
    assert df["price"].to_list() == [12761904.0, 12762000.0]

    ohlc = reader.read_ohlc_impl(
        "BTC_JPY",
        datetime.timedelta(days=1),
        datetime.datetime(2026, 5, 1),
        datetime.datetime(2026, 5, 10),
    )
    assert ohlc["open"].to_list() == [12761904.0]
    assert ohlc["close"].to_list() == [12762000.0]
    assert ohlc["volume"].to_list() == [0.03]


def test_unknown_symbol_raises(tmp_path):
    d = tmp_path / "tick"
    d.mkdir()
    reader = BitflyerReader(data_dir=d)
    try:
        reader.read_ticker("NOPE")
        assert False, "expected ValueError"
    except ValueError:
        pass
