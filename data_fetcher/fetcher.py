"""fetcher.py"""

import datetime

from . import base_fetcher, binance, gmo, histdata, kabutan


def get_fetcher(source: str) -> base_fetcher.BaseFetcher:
    if source == "gmo":
        return gmo.gmo_fetcher.GMOFetcher()
    elif source == "binance":
        return binance.binance_fetcher.BinanceFetcher()
    elif source == "histdata":
        return histdata.histdata_fetcher.HistDataFetcher()
    elif source == "kabutan":
        return kabutan.KabutanFetcher()
    else:
        raise ValueError(f"Unknown source: {source}")


def get_available_sources() -> list[str]:
    return ["gmo", "binance", "histdata", "kabutan"]


def convert_str_to_datetime(date_str: str) -> datetime.datetime:
    if len(date_str) == 8:
        return datetime.datetime.strptime(date_str, "%Y%m%d")
    elif len(date_str) == 14:
        return datetime.datetime.strptime(date_str, "%Y%m%d%H%M%S")
    else:
        raise ValueError(f"Unknown date format: {date_str}")


def convert_datetime_to_str(date: datetime.datetime, include_time: bool = True) -> str:
    date_str = date.strftime("%Y%m%d%H%M%S")
    if not include_time:
        date_str = date_str[:8]
    return date_str
