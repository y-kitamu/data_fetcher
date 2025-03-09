"""base_fetcher.py"""

import datetime

import polars as pl

from .volume_bar import convert_ticker_to_volume_bar


def convert_timedelta_to_str(interval: datetime.timedelta):
    interval_str = ""
    # if interval.weeks > 0:
    #     interval_str += f"{interval.weeks}w"
    if interval.days > 0:
        interval_str += f"{interval.days}d"
    hours = interval.seconds // 3600
    minutes = (interval.seconds % 3600) // 60
    seconds = interval.seconds % 60
    if hours > 0:
        interval_str += f"{hours}h"
    if minutes > 0:
        interval_str += f"{minutes}m"
    if seconds > 0:
        interval_str += f"{seconds}s"
    return interval_str


def convert_str_to_timedelta(interval: str) -> datetime.timedelta:
    if interval[-1] == "s":
        return datetime.timedelta(seconds=int(interval[:-1]))
    elif interval[-1] == "m":
        return datetime.timedelta(minutes=int(interval[:-1]))
    elif interval[-1] == "h":
        return datetime.timedelta(hours=int(interval[:-1]))
    elif interval[-1] == "d":
        return datetime.timedelta(days=int(interval[:-1]))
    elif interval[-1] == "w":
        return datetime.timedelta(weeks=int(interval[:-1]))
    raise ValueError(f"Unknown interval: {interval}")


class BaseFetcher:

    @property
    def available_tickers(self) -> list[str]:
        raise NotImplementedError

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        raise NotImplementedError

    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        raise NotImplementedError

    def fetch_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        raise NotImplementedError

    def fetch_ohlc(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        fill_missing_date: bool = False,
        fetch_interval: datetime.timedelta | None = None,
    ) -> pl.DataFrame:
        if fetch_interval is None:
            df = self.fetch_ticker(symbol, start_date, end_date)
            ohlc_df = (
                df.group_by_dynamic(
                    pl.col("datetime"), every=convert_timedelta_to_str(interval)
                )
                .agg(
                    pl.col("price").first().alias("open"),
                    pl.col("price").max().alias("high"),
                    pl.col("price").min().alias("low"),
                    pl.col("price").last().alias("close"),
                    pl.col("size").sum().alias("volume"),
                )
                .sort(pl.col("datetime"))
            )
        else:
            if start_date is None:
                start_date = datetime.datetime(1970, 1, 1)
            if end_date is None:
                end_date = datetime.datetime.now()

            date = start_date
            dfs: list[pl.DataFrame] = []
            while date < end_date:
                next_date = min(end_date, date + fetch_interval)
                df = self.fetch_ohlc(
                    symbol,
                    interval,
                    date,
                    next_date,
                    fill_missing_date,
                    fetch_interval=None,
                )
                if len(df) > 0:
                    dfs.append(df)
                date = next_date

            ohlc_df = pl.concat(dfs)
        return ohlc_df

    def fetch_volume_bar(
        self,
        symbol: str,
        volume_size: float,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
    ) -> pl.DataFrame:
        df = self.fetch_ticker(symbol, start_date, end_date)
        return convert_ticker_to_volume_bar(df, volume_size)

    def fetch_TIB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        raise NotImplementedError

    def fetch_VIB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        raise NotImplementedError

    def fetch_TRB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        raise NotImplementedError

    def fetch_VRB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        raise NotImplementedError
