import datetime
from pathlib import Path
from typing import override

import polars as pl

from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT


class BinanceReader(BaseReader):
    def __init__(self, data_dir: Path = PROJECT_ROOT / "data/binance"):
        self.data_dir = data_dir

    @property
    @override
    def available_tickers(self) -> list[str]:
        return [
            "BTCUSDT",
            "ETHUSDT",
            "XRPUSDT",
            "BNBUSDT",
            "SOLUSDT",
            "DOGEUSDT",
            "ADAUSDT",
            "TRXUSDT",
            "AVAXUSDT",
            "LINKUSDT",
            "WBTCUSDT",
            "XLMUSDT",
            "DOTUSDT",
        ]

    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"{symbol}-*.csv.gz"))
        if len(ticker_file_list) == 0:
            raise ValueError(f"No data for {symbol}")
        return datetime.datetime.strptime(ticker_file_list[0].parent.name, "%Y%m%d")

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"{symbol}-*.csv.gz"))
        if len(ticker_file_list) == 0:
            raise ValueError(f"No data for {symbol}")
        return datetime.datetime.strptime(ticker_file_list[-1].parent.name, "%Y%m%d")

    @override
    def fetch_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
        aggregate: bool = True,
    ) -> pl.DataFrame:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        if aggregate:
            ticker_file_list = sorted(
                self.data_dir.rglob(f"{symbol}-aggTrades-*.csv.gz")
            )
        else:
            ticker_file_list = sorted(self.data_dir.rglob(f"{symbol}-trades-*.csv.gz"))

        if start_date is None:
            start_date = datetime.datetime(1970, 1, 1)
        if end_date is None:
            end_date = datetime.datetime.now()

        dfs = []
        pre_start_date = start_date - datetime.timedelta(days=1)
        time_key = "Timestamp" if aggregate else "time"
        for file_path in ticker_file_list:
            if "monthly" in file_path.parts:
                continue
            date = datetime.datetime.strptime(file_path.parent.name, "%Y%m%d")
            if pre_start_date.date() <= date.date() <= end_date.date():
                # print(file_path)
                epoch_unit = "us" if date.date() >= datetime.date(2025, 1, 1) else "ms"
                df = pl.read_csv(file_path)
                dfs.append(
                    df.select(
                        pl.lit(symbol).alias("symbol"),
                        pl.when(pl.col("isBuyerMaker"))
                        .then(pl.lit("BUY"))
                        .otherwise(pl.lit("SELL"))
                        .alias("side"),
                        pl.col("price"),
                        pl.col("quantity").alias("size"),
                        pl.from_epoch(pl.col(time_key), epoch_unit)
                        .alias("datetime")
                        .dt.cast_time_unit("us")
                        + timezone_delta,
                    )
                )

        if len(dfs) == 0:
            return pl.DataFrame()

        df = pl.concat(dfs).filter(
            pl.col("datetime").is_between(start_date, end_date, closed="left")
        )
        return df

    @override
    def fetch_ohlc(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        fill_missing_date: bool = False,
        fetch_interval: datetime.timedelta | None = datetime.timedelta(days=10),
    ) -> pl.DataFrame:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        return super().fetch_ohlc(
            symbol,
            interval,
            start_date,
            end_date,
            fill_missing_date,
            fetch_interval=fetch_interval,
        )
