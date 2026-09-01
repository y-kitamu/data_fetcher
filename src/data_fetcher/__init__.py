"""Data Fetcher - Financial data collection and reading package.

This package provides a unified interface for fetching and reading financial data
from various sources including cryptocurrency exchanges, stock markets, forex, and
disclosure databases.
"""

from importlib import import_module
import sys

from loguru import logger

# Import commonly used functions and classes
from .core import (
    PROJECT_ROOT,
    BaseFetcher,
    BaseReader,
    constants,
    debug,
    get_session,
    notify_to_gmail,
    notify_to_line,
    retry_with_backoff,
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
    "notify_to_line",
    "notify_to_gmail",
    "retry_with_backoff",
    # Logger
    "logger",
    # Database
    "db",
]

# Configure logger
logger.remove()
logger.add(
    sys.stdout,
    format="[{time:YYYY-MM-DD HH:mm:ss} {level} {file.path} at line {line}] {message}",
    level="DEBUG",
)

_LAZY_SUBMODULES = {"core", "db", "domains", "fetchers", "processors", "readers"}


def __getattr__(name: str):
    if name in _LAZY_SUBMODULES:
        module = import_module(f".{name}", __name__)
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
