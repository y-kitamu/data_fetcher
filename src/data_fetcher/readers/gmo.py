import datetime
from pathlib import Path
from typing import override

import polars as pl

from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT


class GMOReader(BaseReader):
    _API_ENDPOINT = "https://api.coin.z.com"

    def __init__(self, data_dir: Path = PROJECT_ROOT / "data" / "gmo" / "tick"):
        self.data_dir = data_dir
        self._available_tickers = self.get_available_tickers()

    @property
    @override
    def available_tickers(self) -> list[str]:
        return self._available_tickers

    @override
    def get_latest_date(self, symbol: str) -> datetime.datetime:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"*_{symbol}.csv.gz"))
        if len(ticker_file_list) == 0:
            raise ValueError(f"No data for {symbol}")
        return datetime.datetime.strptime(ticker_file_list[-1].parent.name, "%Y%m%d")

    @override
    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"*_{symbol}.csv.gz"))
        if len(ticker_file_list) == 0:
            raise ValueError(f"No data for {symbol}")
        return datetime.datetime.strptime(ticker_file_list[0].parent.name, "%Y%m%d")

    def get_available_tickers(self) -> list[str]:
        tickers = set(
            [f.name[9:].replace(".csv.gz", "") for f in self.data_dir.rglob("*.csv.gz")]
        )
        return sorted(tickers)

    @override
    def fetch_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"*_{symbol}.csv.gz"))
        if start_date is None:
            start_date = datetime.datetime(1970, 1, 1)
        if end_date is None:
            end_date = datetime.datetime.now()

        dfs = []
        pre_start_date = start_date - datetime.timedelta(days=1)
        for file_path in ticker_file_list:
            date = datetime.datetime.strptime(file_path.parent.name, "%Y%m%d")
            if pre_start_date.date() <= date.date() <= end_date.date():
                dfs.append(
                    pl.read_csv(file_path).select(
                        pl.col("symbol"),
                        pl.col("side"),
                        pl.col("price"),
                        pl.col("size"),
                        pl.col("timestamp")
                        .str.to_datetime("%Y-%m-%d %H:%M:%S.%3f")
                        .alias("datetime")
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
    ) -> pl.DataFrame:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")
        return super().fetch_ohlc(
            symbol, interval, start_date, end_date, fill_missing_date
        )

    @override
    def fetch_volume_bar(
        self,
        symbol: str,
        volume_size: float,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
    ) -> pl.DataFrame:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")
        return super().fetch_volume_bar(symbol, volume_size, start_date, end_date)

    @override
    def fetch_TIB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        pass

    @override
    def fetch_VIB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        pass

    @override
    def fetch_TRB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        pass

    @override
    def fetch_VRB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        pass

    def start_websocket(self):
        pass
