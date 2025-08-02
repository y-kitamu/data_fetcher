"""__init__.py"""

import sys

from loguru import logger

from . import (
    base_fetcher,
    binance,
    bitflyer,
    constants,
    debug,
    edinet,
    fetcher,
    gmo,
    histdata,
    kabutan,
    notification,
    rakuten,
    session,
    tdnet,
    ticker_list,
    utils,
    yfinance,
)

logger.remove()
logger.add(
    sys.stdout,
    format="[{time:YYYY-MM-DD HH:mm:ss} {level} {file.path} at line {line}] {message}",
    level="DEBUG",
)
logger.debug("data_fetcher package initialized")

__all__ = [
    "binance",
    "bitflyer",
    "constants",
    "debug",
    "edinet",
    "fetcher",
    "gmo",
    "histdata",
    "kabutan",
    "base_fetcher",
    "session",
    "ticker_list",
    "yfinance",
    "notification",
    "tdnet",
    "utils",
    "rakuten",
]
