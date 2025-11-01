"""base_fetcher.py"""

import datetime

import polars as pl

from .volume_bar import convert_ticker_to_volume_bar


def convert_timedelta_to_str(interval: datetime.timedelta) -> str:
    """Convert timedelta to string format used by polars.
    
    Args:
        interval: Time interval as timedelta
        
    Returns:
        str: String representation (e.g., "1d2h30m15s")
    """
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
    """Convert string interval to timedelta.
    
    Args:
        interval: Interval string (e.g., "5m", "1h", "1d")
        
    Returns:
        datetime.timedelta: Timedelta object
        
    Raises:
        ValueError: If interval format is unknown
    """
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


def convert_tick_to_ohlc(
    tick_df: pl.DataFrame, interval: datetime.timedelta
) -> pl.DataFrame:
    """Convert tick data to OHLC bars.
    
    Args:
        tick_df: Tick data with columns: datetime, price, size
        interval: Time interval for OHLC bars
        
    Returns:
        pl.DataFrame: OHLC data with columns: datetime, open, high, low, close, volume
    """
    ohlc_df = (
        tick_df.group_by_dynamic(
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
    return ohlc_df


class BaseFetcher:
    """Base class for all data fetchers.
    
    Provides common interface for fetching financial data from various sources.
    """

    @property
    def available_tickers(self) -> list[str]:
        """Get list of available ticker symbols.
        
        Returns:
            list[str]: List of available ticker symbols
        """
        raise NotImplementedError

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        """Get the latest available date for a symbol.
        
        Args:
            symbol: Ticker symbol
            
        Returns:
            datetime.datetime: Latest available date
        """
        raise NotImplementedError

    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        """Get the earliest available date for a symbol.
        
        Args:
            symbol: Ticker symbol
            
        Returns:
            datetime.datetime: Earliest available date
        """
        raise NotImplementedError

    def fetch_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        """Fetch tick data for a symbol.
        
        Args:
            symbol: Ticker symbol
            start_date: Start date (default: None)
            end_date: End date (default: None)
            timezone_delta: Timezone offset (default: 9 hours for JST)
            
        Returns:
            pl.DataFrame: Tick data with columns: symbol, side, price, size, datetime
        """
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
        """Fetch OHLC (Open, High, Low, Close) data for a symbol.
        
        Args:
            symbol: Ticker symbol
            interval: Time interval for OHLC bars
            start_date: Start date (default: None)
            end_date: End date (default: None)
            fill_missing_date: Whether to fill missing dates (default: False)
            fetch_interval: Interval for chunked fetching (default: None)
            
        Returns:
            pl.DataFrame: OHLC data with columns: datetime, open, high, low, close, volume
        """
        if fetch_interval is None:
            df = self.fetch_ticker(symbol, start_date, end_date)
            if len(df) == 0:
                return pl.DataFrame()
            ohlc_df = convert_tick_to_ohlc(df, interval)
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
        """Fetch volume bar data for a symbol.
        
        Args:
            symbol: Ticker symbol
            volume_size: Target volume size for each bar
            start_date: Start date (default: None)
            end_date: End date (default: None)
            
        Returns:
            pl.DataFrame: Volume bar data
        """
        df = self.fetch_ticker(symbol, start_date, end_date)
        return convert_ticker_to_volume_bar(df, volume_size)

    def fetch_TIB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        """Fetch Time Imbalance Bars (TIB) for a symbol.
        
        Args:
            symbol: Ticker symbol
            start_date: Start date
            end_date: End date
            
        Returns:
            pl.DataFrame: TIB data
        """
        raise NotImplementedError

    def fetch_VIB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        """Fetch Volume Imbalance Bars (VIB) for a symbol.
        
        Args:
            symbol: Ticker symbol
            start_date: Start date
            end_date: End date
            
        Returns:
            pl.DataFrame: VIB data
        """
        raise NotImplementedError

    def fetch_TRB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        """Fetch Tick Run Bars (TRB) for a symbol.
        
        Args:
            symbol: Ticker symbol
            start_date: Start date
            end_date: End date
            
        Returns:
            pl.DataFrame: TRB data
        """
        raise NotImplementedError

    def fetch_VRB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        """Fetch Volume Run Bars (VRB) for a symbol.
        
        Args:
            symbol: Ticker symbol
            start_date: Start date
            end_date: End date
            
        Returns:
            pl.DataFrame: VRB data
        """
        raise NotImplementedError
