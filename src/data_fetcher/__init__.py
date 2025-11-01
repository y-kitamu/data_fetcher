"""__init__.py"""

import sys

from loguru import logger

from . import (
    base_fetcher,
    binance,
    bitflyer,
    cftc,
    constants,
    debug,
    edinet,
    fetcher,
    gmo,
    histdata,
    kabutan,
    notification,
    rakuten,
    readers,
    sbi,
    session,
    tdnet,
    ticker_list,
    utils,
    yfinance,
)

__all__ = [
    "base_fetcher",
    "binance",
    "bitflyer",
    "cftc",
    "constants",
    "debug",
    "edinet",
    "fetcher",
    "gmo",
    "histdata",
    "kabutan",
    "notification",
    "rakuten",
    "readers",
    "sbi",
    "session",
    "tdnet",
    "ticker_list",
    "utils",
    "yfinance",
    "logger",
]

logger.remove()
logger.add(
    sys.stdout,
    format="[{time:YYYY-MM-DD HH:mm:ss} {level} {file.path} at line {line}] {message}",
    level="DEBUG",
)
# logger.debug("data_fetcher package initialized")
