"""__init__.py"""
import datetime

from fastapi import APIRouter
from pydantic import BaseModel

from ..base_fetcher import convert_str_to_timedelta
from ..fetcher import convert_str_to_datetime, get_available_sources, get_fetcher, convert_datetime_to_str
from ..logging import logger

router = APIRouter()


class AvailableSources(BaseModel):
    sources: list[str] = []


class AvailableTickers(BaseModel):
    tickers: list[str] = []


class AvailableDates(BaseModel):
    start_date: str | None = None
    end_date: str | None = None

class Ohlcv(BaseModel):
    dates: list[str] = []
    ohlcs: list[list[float]] = []
    volumes: list[float] = []


@router.get("/sources/", response_model=AvailableSources)
async def read_available_sources():
    return {"sources": get_available_sources()}


@router.get("/{source}/tickers/", response_model=AvailableTickers)
async def read_available_tickers(source: str):
    tickers = []
    try:
        tickers = get_fetcher(source).available_tickers
    except Exception:
        logger.exception(f"Failed to get available tickers of source : {source}")
    return {"tickers": tickers}


@router.get("/{source}/{ticker}/dates", response_model=AvailableDates)
async def read_available_dates(source: str, ticker: str):
    dates = AvailableDates()
    try:
        fetcher = get_fetcher(source)
        dates.start_date = convert_datetime_to_str(fetcher.get_earliest_date(ticker), include_time=False)
        dates.end_date = convert_datetime_to_str(fetcher.get_latest_date(ticker), include_time=False)
    except Exception:
        logger.exception(f"Failed to get available dates of source : {source}")
    return dates


@router.get("/{source}/{ticker}/ohlcv")
async def read_ohlcv_data(
    source: str, ticker: str, start: str, end: str, interval: str = "1h"
):
    data = Ohlcv()
    try:
        interval_dt = convert_str_to_timedelta(interval)
        fetcher = get_fetcher(source)
        df = fetcher.fetch_ohlc(
            symbol=ticker,
            start_date=convert_str_to_datetime(start),
            end_date=convert_str_to_datetime(end),
            interval=interval_dt,
        )
        data.dates = [convert_datetime_to_str(dt) for dt in df["datetime"].to_list()]
        data.ohlcs =  df.select("open", "close", "low", "high").to_numpy().tolist()
        data.volumes = df["volume"].to_list()
    except:
        logger.exception(f"Failed to get fetcher of source : {source}")

    return data
