"""Fetchers module - unified data fetching interface.

Provides factory functions to get fetchers for various data sources.
"""

from ..core.base_fetcher import BaseFetcher
from .crypto import (
    BinanceFetcher,
    BitflyerBookFetcher,
    BitflyerFetcher,
    GMOBookFetcher,
    GMOFetcher,
)
from .forex import (
    ForexFactoryFetcher,
    GMOFetcherFX,
    GMOFetcherFXWithTimestamp,
    HistDataFetcher,
)
from .stocks import KabutanFetcher, RakutenFetcher

__all__ = [
    "get_fetcher",
    "get_available_sources",
    "BinanceFetcher",
    "BitflyerBookFetcher",
    "BitflyerFetcher",
    "ForexFactoryFetcher",
    "GMOFetcher",
    "GMOBookFetcher",
    "GMOFetcherFX",
    "GMOFetcherFXWithTimestamp",
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
        "gmo_fx": GMOFetcherFX,
        "gmo_fx_with_timestamp": GMOFetcherFXWithTimestamp,
        "bitflyer": BitflyerFetcher,
        "bitflyer_book": BitflyerBookFetcher,
        "forex_factory": ForexFactoryFetcher,
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
    return [
        "binance",
        "gmo",
        "bitflyer",
        "bitflyer_book",
        "forex_factory",
        "histdata",
        "kabutan",
        "rakuten",
    ]
