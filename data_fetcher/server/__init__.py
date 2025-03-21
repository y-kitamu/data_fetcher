"""__init__.py"""

import io
import json

import polars as pl
from fastapi import APIRouter, File, Response
from pydantic import BaseModel

from ..base_fetcher import convert_str_to_timedelta
from ..fetcher import (
    convert_datetime_to_str,
    convert_str_to_datetime,
    get_available_sources,
    get_fetcher,
)
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
    ticker: str = ""
    dataType: str = "candlestick"  # candlestick or tick
    data: list[list[float]] = []


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
        dates.start_date = convert_datetime_to_str(
            fetcher.get_earliest_date(ticker), include_time=False
        )
        dates.end_date = convert_datetime_to_str(
            fetcher.get_latest_date(ticker), include_time=False
        )
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
        data.ticker = ticker
        data.dataType = "candlestick"
        data.data = (
            df.select(
                pl.col("datetime").dt.timestamp("ms"),
                pl.col("open"),
                pl.col("high"),
                pl.col("low"),
                pl.col("close"),
                pl.col("volume"),
            )
            .to_numpy()
            .tolist()
        )
        print(df)
    except:
        logger.exception(f"Failed to get fetcher of source : {source}")

    return data


@router.post("/upload/ohlcv")
async def upload_ohlcv(
    file: bytes = File(),
):
    try:
        df = pl.read_csv(io.BytesIO(file))
        ticker = "uploaded"
        data_type = "candlestick"
        data = (
            df.select(
                pl.col("datetime").str.to_datetime().dt.timestamp("ms"),
                "open",
                "high",
                "low",
                "close",
                "volume",
            )
            .to_numpy()
            .tolist()
        )
        return Response(
            json.dumps({"ticker": ticker, "dataType": data_type, "data": data}),
            media_type="application/json",
        )
    except:
        logger.exception("Failed to upload file")

    return Ohlcv()

    # To speed up the process, we are returning Response object.
    # return await read_ohlcv_data(source, ticker, start, end, interval)
