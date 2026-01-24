"""Forex data fetchers."""

from .gmo import GMOFetcher as GMOFetcherFX
from .histdata import HistDataFetcher

__all__ = ["HistDataFetcher", "GMOFetcherFX"]
