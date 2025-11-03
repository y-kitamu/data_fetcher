"""Data readers for accessing stored financial data.

This module provides reader classes for accessing data that has been
previously fetched and stored locally. Readers implement the BaseReader
interface and provide efficient access to historical data.

Available Readers:
    - HistDataReader: Read stored forex tick data from HistData
    - YFinanceReader: Read stored minute-level data from Yahoo Finance
"""

from .histdata import HistDataReader
from .yfinance import YFinanceReader

__all__ = ["HistDataReader", "YFinanceReader"]
