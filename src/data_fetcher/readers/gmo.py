import datetime
from pathlib import Path

import polars as pl

from ..core import convert_timedelta_to_str
from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT

GMO_DATA_DIR = PROJECT_ROOT / "data" / "gmo" / "tick"


class GMOReader(BaseReader):
    _API_ENDPOINT = "https://api.coin.z.com"

    def __init__(self, data_dir: Path = GMO_DATA_DIR):
        self.data_dir = data_dir
        self._available_tickers = self.get_available_tickers()

    @property
    def available_tickers(self) -> list[str]:
        return self._available_tickers

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"*_{symbol}.csv.gz"))
        if len(ticker_file_list) == 0:
            raise ValueError(f"No data for {symbol}")
        return datetime.datetime.strptime(ticker_file_list[-1].parent.name, "%Y%m%d")

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

    def read_ticker(
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
                df = pl.read_csv(file_path)
                df = df.with_columns(
                    pl.col("timestamp")
                    .str.to_datetime(time_zone="UTC")
                    .dt.replace_time_zone(None)
                    .alias("datetime")
                    + timezone_delta,
                )
                dfs.append(df)
        if len(dfs) == 0:
            return pl.DataFrame()

        df = pl.concat(dfs).filter(
            pl.col("datetime").is_between(start_date, end_date, closed="left")
        )
        if "price" in df.columns:
            df = df.with_columns(pl.lit(0).alias("spread"))
        else:
            df = df.with_columns(
                ((pl.col("bid") + pl.col("ask")) / 2.0).alias("price"),
                (pl.col("ask") - pl.col("bid")).alias("spread"),
                pl.lit(0).alias("size"),
            )

        return df

    def read_ohlc_impl(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
    ) -> pl.DataFrame:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        df = self.read_ticker(symbol, start_date, end_date)
        df.group_by_dynamic("datetime", every=convert_timedelta_to_str(interval)).agg(
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("size").sum().alias("volume"),
            pl.col("spread").max().alias("max_spread"),
        )

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
