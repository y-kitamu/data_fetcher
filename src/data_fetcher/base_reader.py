import datetime

import polars as pl


class BaseReader:
    rows = ["datetime", "open", "high", "low", "close", "volume"]

    @property
    def available_tickers(self) -> list[str]:
        raise NotImplementedError

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        raise NotImplementedError

    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        raise NotImplementedError

    def read_ohlc(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
        fill_missing_date: bool = False,
        read_interval: datetime.timedelta | None = None,
    ) -> pl.DataFrame:
        raise NotImplementedError

    def read_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
        timezone: datetime.tzinfo = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        raise NotImplementedError
