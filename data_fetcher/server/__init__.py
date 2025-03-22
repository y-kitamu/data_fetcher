"""__init__.py"""

import io
import json
from typing import Annotated

import polars as pl
from fastapi import APIRouter, File, Form, Response
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


class DataHeader(BaseModel):
    columns: list[str] = []


class UploadData(BaseModel):
    file: Annotated[bytes, File()]
    dataType: str
    tickKeys: list[str]
    candleKeys: list[str]
    additionalKeys: list[str] = []


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
    data: Annotated[UploadData, Form()],
):
    try:
        df = pl.read_csv(io.BytesIO(data.file))
        print(df)
        if data.dataType == "candlestick":
            dt_key = data.candleKeys[0]
            data_key = data.candleKeys[1:]
        elif data.dataType == "tick":
            dt_key = data.tickKeys[0]
            data_key = data.tickKeys[1:]

        print(f"dt_key = {dt_key}, data_key = {data_key}")
        return_data = (
            df.select(pl.col(dt_key).str.to_datetime().dt.timestamp("ms"), *data_key)
            .to_numpy()
            .tolist()
        )
        return Response(
            json.dumps(
                {"ticker": "uploaded", "dataType": data.dataType, "data": return_data}
            ),
            media_type="application/json",
        )
    except:
        logger.exception("Failed to upload file")

    return Ohlcv()


@router.post("/upload/header")
async def upload_header(
    file: bytes = File(),
):
    header = DataHeader()
    try:
        df = pl.read_csv(io.BytesIO(file))
        header.columns = df.columns
    except:
        logger.exception("Failed to upload file")

    return header
