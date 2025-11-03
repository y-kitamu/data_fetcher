from ..core.base_reader import BaseReader
from .histdata import HistDataReader
from .kabutan import KabutanReader
from .yfinance import YFinanceReader

__all__ = ["BaseReader", "HistDataReader", "KabutanReader", "YFinanceReader"]


def get_reader(source: str) -> BaseReader:
    """Get a reader instance for the specified data source.

    Args:
        source: Data source name (histdata, yfinance, kabutan)

    Returns:
        BaseReader: Reader instance for the specified source

    Raises:
        ValueError: If the source is unknown
    """
    readers = {
        "histdata": HistDataReader,
        "yfinance": YFinanceReader,
        "kabutan": KabutanReader,
    }

    if source not in readers:
        raise ValueError(
            f"Unknown source: {source}. Available sources: {list(readers.keys())}"
        )

    return readers[source]()
