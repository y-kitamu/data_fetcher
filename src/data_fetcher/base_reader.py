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

    def read_ohlc_impl(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> pl.DataFrame:
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
        if read_interval is None:
            return self.read_ohlc_impl(symbol, interval, start_date, end_date)
        else:
            if start_date is None:
                start_date = datetime.datetime(1970, 1, 1)
            if end_date is None:
                end_date = datetime.datetime.now()

            date = start_date
            dfs: list[pl.DataFrame] = []
            while date < end_date:
                next_date = min(end_date, date + read_interval)
                df = self.read_ohlc_impl(symbol, interval, date, next_date)
                if len(df) > 0:
                    dfs.append(df)
                date = next_date

            ohlc_df = pl.concat(dfs)
        return ohlc_df

    def read_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
        timezone: datetime.tzinfo = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        raise NotImplementedError
