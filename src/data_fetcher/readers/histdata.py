import datetime
from pathlib import Path

import polars as pl

from ..base_fetcher import convert_timedelta_to_str
from ..base_reader import BaseReader
from ..constants import PROJECT_ROOT

DATA_DIR = PROJECT_ROOT / "data" / "histdata" / "tick"


class HistDataReader(BaseReader):
    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = data_dir
        self._available_tickers = []

    @property
    def available_tickers(self) -> list[str]:
        if len(self._available_tickers) == 0:
            # Get unique tickers from all CSV files
            ticker_files = sorted(self.data_dir.rglob("*.csv.gz"))
            tickers = set()
            for file_path in ticker_files:
                # File format: {ticker}_{year}_{month}.csv.gz
                ticker = file_path.stem.replace(".csv", "").split("_")[0]
                tickers.add(ticker)
            self._available_tickers = sorted(tickers)
        return self._available_tickers

    def _get_dates(self, symbol: str) -> list[datetime.datetime]:
        """Get all available dates for a given symbol"""
        ticker_file_list = sorted(self.data_dir.rglob(f"{symbol}_*.csv.gz"))
        dates = [
            datetime.datetime.strptime(path.parent.name, "%Y%m")
            for path in ticker_file_list
        ]
        return sorted(dates)

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        dates = self._get_dates(symbol)
        if len(dates) == 0:
            return datetime.datetime(1970, 1, 1)
        return dates[-1]

    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        dates = self._get_dates(symbol)
        if len(dates) == 0:
            return datetime.datetime(1970, 1, 1)
        return dates[0]

    def read_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        """Read tick data for a given symbol"""
        pre_start_date = start_date - datetime.timedelta(days=31)
        post_end_date = end_date + datetime.timedelta(days=31)

        # Load all relevant files
        dfs: list[pl.DataFrame] = []
        ticker_file_list = sorted(self.data_dir.rglob(f"{symbol}_*.csv.gz"))
        for file_path in ticker_file_list:
            file_date = datetime.datetime.strptime(file_path.parent.name, "%Y%m")
            if pre_start_date <= file_date <= post_end_date:
                dfs.append(
                    pl.read_csv(
                        file_path,
                        has_header=False,
                        new_columns=["timestamp", "bid", "ask", "volume"],
                    )
                )

        if len(dfs) == 0:
            return pl.DataFrame()

        # EST -> UTC -> Target timezone
        timezone_delta += datetime.timedelta(hours=5)
        df = (
            pl.concat(dfs)
            .with_columns(
                pl.lit(symbol).alias("symbol"),
                pl.col("volume").alias("size"),
                ((pl.col("ask") + pl.col("bid")) / 2.0).alias("price"),
                (pl.col("ask") - pl.col("bid")).alias("spread"),
                pl.col("timestamp")
                .str.to_datetime("%Y%m%d %H%M%S%3f")
                .alias("datetime")
                + timezone_delta,
            )
            .filter(pl.col("datetime").is_between(start_date, end_date))
        )
        return df.sort("datetime")

    def read_ohlc_impl(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
    ) -> pl.DataFrame:
        """Read OHLC data for a given symbol by aggregating tick data"""
        # Read tick data
        tick_df = self.read_ticker(symbol, start_date, end_date)

        if tick_df.is_empty():
            return pl.DataFrame()

        # Aggregate to OHLC
        ohlc_df = tick_df.group_by_dynamic(
            pl.col("datetime"), every=convert_timedelta_to_str(interval)
        ).agg(
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("spread").max().alias("max_spread"),
            pl.col("size").sum().alias("volume"),
        )

        return ohlc_df.sort("datetime")
