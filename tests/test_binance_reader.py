"""Regression tests for BinanceReader.

Previously this class implemented fetch_ticker/fetch_ohlc instead of
read_ticker/read_ohlc_impl, so it didn't conform to BaseReader and calling
fetch_ohlc with read_interval chunking raised AttributeError (super().fetch_ohlc
doesn't exist on BaseReader).
"""

import datetime
import gzip

import polars as pl

from data_fetcher.readers.binance import BinanceReader

_SYMBOL = "BTCUSDT"


def _write_agg_trades(path, rows):
    df = pl.DataFrame(rows)
    with gzip.open(path, "wb") as f:
        df.write_csv(f)


# BinanceReader.read_ticker treats file dates >= 2025-01-01 as microsecond
# epochs and earlier dates as millisecond epochs (see readers/binance.py);
# using a 2025 date keeps the fixture's microsecond Timestamp values consistent
# with that rule.
_DAY = datetime.datetime(2025, 1, 1)
_DAY_UTC = _DAY.replace(tzinfo=datetime.timezone.utc)


def test_read_ticker_renamed_from_fetch_ticker(tmp_path):
    d = tmp_path / "binance"
    day_dir = d / "20250101"
    day_dir.mkdir(parents=True)
    epoch_us_1 = int(_DAY_UTC.timestamp() * 1_000_000)
    _write_agg_trades(
        day_dir / f"{_SYMBOL}-aggTrades-2025-01-01.csv.gz",
        {
            "Timestamp": [epoch_us_1, epoch_us_1 + 1_000_000],
            "price": [42000.0, 42010.0],
            "quantity": [0.1, 0.2],
            "isBuyerMaker": [True, False],
        },
    )

    reader = BinanceReader(data_dir=d)
    assert hasattr(reader, "read_ticker")

    df = reader.read_ticker(
        _SYMBOL,
        start_date=_DAY,
        end_date=_DAY + datetime.timedelta(days=1),
        timezone_delta=datetime.timedelta(0),
    )
    assert df["side"].to_list() == ["BUY", "SELL"]
    assert df["price"].to_list() == [42000.0, 42010.0]


def test_read_ohlc_with_chunked_read_interval_does_not_raise(tmp_path):
    d = tmp_path / "binance"
    day_dir = d / "20250101"
    day_dir.mkdir(parents=True)
    epoch_us_1 = int(_DAY_UTC.timestamp() * 1_000_000)
    _write_agg_trades(
        day_dir / f"{_SYMBOL}-aggTrades-2025-01-01.csv.gz",
        {
            "Timestamp": [epoch_us_1],
            "price": [42000.0],
            "quantity": [0.1],
            "isBuyerMaker": [True],
        },
    )

    reader = BinanceReader(data_dir=d)
    # This previously raised AttributeError via the removed fetch_ohlc ->
    # super().fetch_ohlc call once read_interval chunking kicked in.
    ohlc = reader.read_ohlc(
        _SYMBOL,
        datetime.timedelta(days=1),
        start_date=_DAY,
        end_date=_DAY + datetime.timedelta(days=1),
        read_interval=datetime.timedelta(days=1),
    )
    assert set(["open", "high", "low", "close", "volume"]).issubset(ohlc.columns)
