"""Fetchers module - unified data fetching interface.

Provides factory functions to get fetchers for various data sources.
"""

from ..core.base_fetcher import BaseFetcher
from .crypto import BinanceFetcher, BitflyerFetcher, GMOFetcher
from .forex import HistDataFetcher
from .stocks import KabutanFetcher, RakutenFetcher

__all__ = [
    "get_fetcher",
    "get_available_sources",
    "BinanceFetcher",
    "BitflyerFetcher",
    "GMOFetcher",
    "HistDataFetcher",
    "KabutanFetcher",
    "RakutenFetcher",
]


def get_fetcher(source: str) -> BaseFetcher:
    """Get a fetcher instance for the specified data source.

    Args:
        source: Data source name (binance, gmo, bitflyer, histdata, kabutan, rakuten)

    Returns:
        BaseFetcher: Fetcher instance for the specified source

    Raises:
        ValueError: If the source is unknown
    """
    fetchers = {
        "binance": BinanceFetcher,
        "gmo": GMOFetcher,
        "bitflyer": BitflyerFetcher,
        "histdata": HistDataFetcher,
        "kabutan": KabutanFetcher,
        "rakuten": RakutenFetcher,
    }

    if source not in fetchers:
        raise ValueError(
            f"Unknown source: {source}. Available sources: {list(fetchers.keys())}"
        )

    return fetchers[source]()


def get_available_sources() -> list[str]:
    """Get list of all available data sources.

    Returns:
        list[str]: List of available source names
    """
    return ["binance", "gmo", "bitflyer", "histdata", "kabutan", "rakuten"]
