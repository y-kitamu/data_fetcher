"""Data Fetcher - Financial data collection and reading package.

This package provides a unified interface for fetching and reading financial data
from various sources including cryptocurrency exchanges, stock markets, forex, and
disclosure databases.
"""

import sys

from loguru import logger

# Import core modules
# Import utility modules (kept for backward compatibility)
from . import core, domains, fetchers, processors, readers

# Import commonly used functions and classes
from .core import (
    PROJECT_ROOT,
    BaseFetcher,
    BaseReader,
    constants,
    debug,
    get_session,
)
from .fetchers import get_available_sources as get_available_fetcher_sources
from .fetchers import get_fetcher
from .readers import get_reader

__all__ = [
    # Core modules
    "core",
    "domains",
    "fetchers",
    "processors",
    "readers",
    # Core classes and functions
    "BaseFetcher",
    "BaseReader",
    "PROJECT_ROOT",
    "get_session",
    "constants",
    "debug",
    "get_fetcher",
    "get_reader",
    "get_available_fetcher_sources",
    # Logger
    "logger",
]

# Configure logger
logger.remove()
logger.add(
    sys.stdout,
    format="[{time:YYYY-MM-DD HH:mm:ss} {level} {file.path} at line {line}] {message}",
    level="DEBUG",
)
