"""Fetchers module - live-API/scrape fetchers that write local data stores.

Read access to what these write is provided by src/data_fetcher/gateway.py
and src/data_fetcher/readers/*.py, not by this module.
"""

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
from .stocks import (
    GNewsFetcher,
    KabutanFetcher,
    KabutanNewsFetcher,
    RakutenFetcher,
    YfinanceNewsFetcher,
)

__all__ = [
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
    "KabutanNewsFetcher",
    "GNewsFetcher",
    "RakutenFetcher",
    "YfinanceNewsFetcher",
]
