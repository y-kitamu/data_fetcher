import datetime

import polars as pl


class BaseReader:
    """Base class for all data readers.
    
    Provides common interface for reading stored financial data.
    """

    rows = ["datetime", "open", "high", "low", "close", "volume"]

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

    def read_ohlc(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
        fill_missing_date: bool = False,
        read_interval: datetime.timedelta | None = None,
    ) -> pl.DataFrame:
        """Read OHLC data for a symbol.
        
        Args:
            symbol: Ticker symbol
            interval: Time interval for OHLC bars
            start_date: Start date (default: 1970-01-01)
            end_date: End date (default: now)
            fill_missing_date: Whether to fill missing dates (default: False)
            read_interval: Interval for chunked reading (default: None)
            
        Returns:
            pl.DataFrame: OHLC data
        """
        raise NotImplementedError

    def read_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        """Read tick data for a symbol.
        
        Args:
            symbol: Ticker symbol
            start_date: Start date (default: 1970-01-01)
            end_date: End date (default: now)
            timezone_delta: Timezone offset (default: 9 hours for JST)
            
        Returns:
            pl.DataFrame: Tick data
        """
        raise NotImplementedError
