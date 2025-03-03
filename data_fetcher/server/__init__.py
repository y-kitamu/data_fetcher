"""__init__.py"""

from fastapi import APIRouter
from pydantic import BaseModel

from .. import binance, gmo

router = APIRouter()


class AvailableTickers(BaseModel):
    tickers: list[str] = []


@router.get("/available-tickers/{source}/", response_model=AvailableTickers)
def read_available_tickers(source: str):
    print(source)
    if source == "gmo":
        tickers = gmo.gmo_fetcher.GMOFetcher().available_tickers
    elif source == "binance":
        tickers = binance.binance_fetcher.BinanceFetcher().available_tickers
    else:
        tickers = []
    return {"tickers": ["test"]}
