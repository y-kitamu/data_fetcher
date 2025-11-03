"""rakuten_fetcher.py"""

import datetime
from pathlib import Path

import polars as pl

from ...core.base_fetcher import BaseFetcher, convert_timedelta_to_str
from ...core.constants import PROJECT_ROOT


def get_available_tickers(data_dir: Path) -> list[str]:
    """ """
    return sorted(
        set([csv_path.stem.split("_")[0] for csv_path in data_dir.rglob("*.csv")])
    )


class RakutenFetcher(BaseFetcher):
    def __init__(self, data_dir: Path = PROJECT_ROOT / "data/rakuten/minutes"):
        super().__init__()
        self.data_dir = data_dir
        self._available_tickers = get_available_tickers(data_dir)

    @property
    def available_tickers(self) -> list[str]:
        """
        Returns a list of available tickers.
        """
        return self._available_tickers

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        csv_list = sorted(self.data_dir.rglob(f"{symbol}_*.csv"))
        if not csv_list:
            raise ValueError(f"No data found for symbol: {symbol}")

        return datetime.datetime.strptime(
            csv_list[-1].parent.name, "%Y%m%d"
        ) + datetime.timedelta(days=1)

    def get_earliest_date(self, symbol):
        csv_list = sorted(self.data_dir.rglob(f"{symbol}_*.csv"))
        if not csv_list:
            raise ValueError(f"No data found for symbol: {symbol}")

        return datetime.datetime.strptime(csv_list[0].parent.name, "%Y%m%d")

    def fetch_ohlc(
        self,
        symbol,
        interval,
        start_date=None,
        end_date=None,
        fill_missing_date=False,
        fetch_interval=None,
    ):
        """ """
        if interval < datetime.timedelta(minutes=1):
            raise ValueError("Interval must be at least 1 minute.")

        if start_date is None:
            start_date = self.get_earliest_date(symbol)
        if end_date is None:
            end_date = self.get_latest_date(symbol)

        cur_date = start_date.date()
        dfs = []
        while cur_date <= end_date.date():
            csv_path = (
                self.data_dir
                / cur_date.strftime("%Y%m%d")
                / f"{symbol}_{cur_date.strftime('%Y%m%d')}.csv"
            )
            if csv_path.exists():
                dfs.append(self._read_csv(csv_path))
            cur_date += datetime.timedelta(days=1)

        if len(dfs) == 0:
            return pl.DataFrame()

        df = (
            pl.concat(dfs)
            .sort("datetime")
            .filter(pl.col("datetime").is_between(start_date, end_date, closed="both"))
        )

        if len(df) == 0:
            return df

        df = (
            df.group_by_dynamic("datetime", every=convert_timedelta_to_str(interval))
            .agg(
                pl.col("close").last().alias("close"),
                pl.col("open").first().alias("open"),
                pl.col("high").max().alias("high"),
                pl.col("low").min().alias("low"),
                pl.col("volume").sum().alias("volume"),
            )
            .sort("datetime")
        )

        return df

    def _read_csv(self, csv_path: Path) -> pl.DataFrame:
        df = pl.read_csv(
            csv_path,
            schema_overrides=[
                pl.String,
                pl.String,
                pl.Float64,
                pl.Float64,
                pl.Float64,
                pl.Float64,
                pl.Float64,
            ],
        ).select(
            (pl.col("date") + "_" + pl.col("minutes"))
            .str.to_datetime("%Y/%m/%d_%H:%M")
            .alias("datetime"),
            pl.col("open"),
            pl.col("high"),
            pl.col("low"),
            pl.col("close"),
            pl.col("volume"),
        )
        return df
