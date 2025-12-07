"""base_fetcher.py"""


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
