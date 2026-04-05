from ..core.base_reader import BaseReader
from .binance import BinanceReader
from .bitflyer import BitflyerBookReader
from .gmo import GMOReader, GMOBookReader
from .histdata import HistDataReader
from .kabutan import KabutanReader
from .news import JpNewsReader
from .rakuten import RakutenReader
from .sbi import SBIReader
from .yfinance import YFinanceReader

__all__ = [
    "BaseReader",
    "HistDataReader",
    "JpNewsReader",
    "KabutanReader",
    "YFinanceReader",
    "GMOReader",
    "GMOBookReader",
    "RakutenReader",
    "BinanceReader",
    "SBIReader",
    "BitflyerBookReader",
]


def get_reader(source: str) -> BaseReader:
    """Get a reader instance for the specified data source.

    Args:
        source: Data source name (histdata, yfinance, kabutan, sbi, etc.)

    Returns:
        BaseReader: Reader instance for the specified source

    Raises:
        ValueError: If the source is unknown
    """
    readers = {
        "histdata": HistDataReader,
        "yfinance": YFinanceReader,
        "kabutan": KabutanReader,
        "gmo": GMOReader,
        "gmo_book": GMOBookReader,
        "binance": BinanceReader,
        "sbi": SBIReader,
        "bitflyer_book": BitflyerBookReader,
    }

    if source not in readers:
        raise ValueError(
            f"Unknown source: {source}. Available sources: {list(readers.keys())}"
        )

    return readers[source]()
