import datetime

import polars as pl

from ..core.base_fetcher import convert_timedelta_to_str
from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT

DATA_DIR = PROJECT_ROOT / "data" / "yfinance" / "minutes"


class YFinanceReader(BaseReader):
    def __init__(self):
        self._available_tickers = []

    @property
    def available_tickers(self) -> list[str]:
        if len(self._available_tickers) == 0:
            self._available_tickers = sorted(
                set([path.name.split("_")[0] for path in DATA_DIR.rglob("*.csv")])
            )
        return self._available_tickers

    def _get_dates(self, symbol: str):
        dates = sorted(
            [
                datetime.datetime.strptime(path.stem.split("_")[-1], "%Y%m%d")
                for path in DATA_DIR.rglob(f"{symbol}*.csv")
            ]
        )
        return dates

    def get_latest_date(self, symbol: str):
        dates = self._get_dates(symbol)
        if len(dates) == 0:
            return datetime.datetime(1970, 1, 1)
        return dates[-1]

    def get_earliest_date(self, symbol: str):
        dates = self._get_dates(symbol)
        if len(dates) == 0:
            return datetime.datetime(1970, 1, 1)
        return dates[0]

    def read_ohlc(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
        fill_missing_date: bool = False,
        read_interval: datetime.timedelta | None = None,
    ) -> pl.DataFrame:
        dfs = []
        for dir in DATA_DIR.glob("*"):
            if not dir.is_dir():
                continue

            dir_date = datetime.datetime.strptime(dir.name, "%Y%m%d")
            if dir_date < start_date or dir_date > end_date:
                continue

            csv_path = dir / f"{symbol}_{dir_date.strftime('%Y%m%d')}.csv"
            if not csv_path.exists():
                continue

            dfs.append(pl.read_csv(csv_path))

        if len(dfs) == 0:
            return pl.DataFrame()

        df = (
            pl.concat(dfs)
            .select(
                pl.col("datetime").str.to_datetime().alias("datetime"),
                pl.col("open").alias("open"),
                pl.col("high").alias("high"),
                pl.col("low").alias("low"),
                pl.col("close").alias("close"),
                pl.col("volume").alias("volume"),
            )
            .sort(pl.col("datetime"))
        )
        df = df.group_by_dynamic(
            pl.col("datetime"), every=convert_timedelta_to_str(interval)
        ).agg(
            pl.col("open").first(),
            pl.col("high").max(),
            pl.col("low").min(),
            pl.col("close").last(),
            pl.col("volume").sum(),
        )
        return df
