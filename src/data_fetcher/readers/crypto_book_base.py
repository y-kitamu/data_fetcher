import datetime
from pathlib import Path

import polars as pl

from ..core.base_reader import BaseReader


class BaseCryptoBookReader(BaseReader):
    """Base reader for crypto book data."""
    
    def __init__(self, data_dir: Path, timestamp_col: str, is_utc: bool = True):
        self.data_dir = data_dir
        self.timestamp_col = timestamp_col
        self.is_utc = is_utc
        self._available_tickers = self.get_available_tickers()

    @property
    def available_tickers(self) -> list[str]:
        return self._available_tickers

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"*_{symbol}.csv*"))
        if len(ticker_file_list) == 0:
            raise ValueError(f"No data for {symbol}")
        return datetime.datetime.strptime(ticker_file_list[-1].parent.name, "%Y%m%d")

    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"*_{symbol}.csv*"))
        if len(ticker_file_list) == 0:
            raise ValueError(f"No data for {symbol}")
        return datetime.datetime.strptime(ticker_file_list[0].parent.name, "%Y%m%d")

    def get_available_tickers(self) -> list[str]:
        tickers = set()
        for f in self.data_dir.rglob("*.csv*"):
            filename = f.name
            if filename.endswith(".csv"):
                ticker = filename.split("_", 1)[1][:-4]
            elif filename.endswith(".csv.gz"):
                ticker = filename.split("_", 1)[1][:-7]
            else:
                continue
            tickers.add(ticker)
        return sorted(list(tickers))

    def read_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"*_{symbol}.csv*"))
        if start_date is None:
            start_date = datetime.datetime(1970, 1, 1)
        if end_date is None:
            end_date = datetime.datetime.now()

        dfs = []
        pre_start_date = start_date - datetime.timedelta(days=1)
        for file_path in ticker_file_list:
            date = datetime.datetime.strptime(file_path.parent.name, "%Y%m%d")
            if pre_start_date.date() <= date.date() <= end_date.date():
                df = pl.read_csv(file_path, null_values=["None", ""])
                if self.is_utc:
                    df = df.with_columns(
                        pl.col(self.timestamp_col)
                        .str.to_datetime(time_zone="UTC", strict=False)
                        .dt.replace_time_zone(None)
                        .alias("datetime")
                        + timezone_delta,
                    )
                else:
                    df = df.with_columns(
                        pl.col(self.timestamp_col)
                        .str.to_datetime(strict=False)
                        .alias("datetime")
                    )
                dfs.append(df)
        if len(dfs) == 0:
            return pl.DataFrame()

        df = pl.concat(dfs).filter(
            pl.col("datetime").is_between(start_date, end_date, closed="left")
        ).sort("datetime")
        return df

    def read_ohlc_impl(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
    ) -> pl.DataFrame:
        raise NotImplementedError("read_ohlc_impl is not strictly defined for book updates.")
