"""__init__.py"""

from fastapi import APIRouter
from pydantic import BaseModel

from ..base_fetcher import convert_str_to_timedelta
from ..fetcher import get_available_sources, get_fetcher
from ..logging import logger

router = APIRouter()


class AvailableSources(BaseModel):
    sources: list[str] = []


class AvailableTickers(BaseModel):
    tickers: list[str] = []


class AvailableDates(BaseModel):
    dates: dict[str, tuple[str, str]] = {}


@router.get("/sources/", response_model=AvailableSources)
def read_available_sources():
    return {"sources": get_available_sources()}


@router.get("/{source}/tickers/", response_model=AvailableTickers)
def read_available_tickers(source: str):
    tickers = []
    try:
        tickers = get_fetcher(source).available_tickers
    except Exception:
        logger.exception(f"Failed to get available tickers of source : {source}")
    return {"tickers": tickers}


@router.get("/{source}/tickers/dates", response_model=AvailableDates)
def read_available_dates(source: str):
    dates: dict[str, tuple[str, str]] = {}
    try:
        fetcher = get_fetcher(source)
        for ticker in fetcher.available_tickers:
            dates[ticker] = (
                fetcher.get_earliest_date(ticker).isoformat(),
                fetcher.get_latest_date(ticker).isoformat(),
            )
    except Exception:
        logger.exception(f"Failed to get available dates of source : {source}")
    return {"dates": dates}


@router.get("/{source}/{ticker}/ohlcv")
def read_ohlcv_data(
    source: str, ticker: str, start_date: str, end_date: str, interval: str = "1h"
):
    try:
        interval_dt = convert_str_to_timedelta(interval)
        fetcher = get_fetcher(source)
        df = fetcher.fetch_ohlc(
            symbol=ticker,
            start_date=start_date,
            end_date=end_date,
            interval=interval_dt,
        )
    except:
        logger.exception(f"Failed to get fetcher of source : {source}")

    return
