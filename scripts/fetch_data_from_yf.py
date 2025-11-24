"""fetch_data_from_yf.py
yahoo finance apiを使用してデータを取得する
"""

import datetime
from pathlib import Path

import polars as pl
import yfinance as yf
from loguru import logger
from tqdm import tqdm

import data_fetcher

# session = data_fetcher.session.get_session(max_requests_per_second=1)
FX_TICKERS = [
    "USDJPY=X",
    "EURJPY=X",
]


def fetch_data(symbol: str, date: datetime.date, output_path: Path):
    if date.weekday() >= 5:  # 土日はスキップ
        return

    try:
        df = yf.Ticker(symbol).history(
            interval="1m",
            start=date.strftime("%Y-%m-%d"),
            end=(date + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
        )
    except KeyboardInterrupt as e:
        raise e
    except:
        logger.exception(f"Failed to fetch data for {symbol} on {date}. Skipping.")
        return

    if len(df) > 0:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df = data_fetcher.processors.yfinance.pd_to_pl(df)
        df.write_csv(output_path)


def collect_fx_data():
    start_date = datetime.date.today() - datetime.timedelta(days=30)
    end_date = datetime.date.today() - datetime.timedelta(
        days=1
    )  # datetime.date.today()
    date = start_date
    for symbol in FX_TICKERS:
        date = start_date
        while date < end_date:
            date_str = date.strftime("%Y%m%d")
            output_path = (
                data_fetcher.constants.PROJECT_ROOT
                / "data/yfinance/minutes"
                / date_str
                / f"{symbol.replace('=X', '')}_{date_str}.csv"
            )
            if output_path.exists() or date.weekday() >= 5:
                date += datetime.timedelta(days=1)
                continue

            fetch_data(symbol, date, output_path)
            date += datetime.timedelta(days=1)


def collect_stock_data():
    start_date = datetime.date.today() - datetime.timedelta(days=14)
    end_date = datetime.date.today() - datetime.timedelta(
        days=1
    )  # datetime.date.today()
    date = start_date

    ticker_csv_path = data_fetcher.constants.PROJECT_ROOT / "data" / "us_tickers.csv"
    df = data_fetcher.core.ticker_list.update_us_ticker_list(ticker_csv_path)
    if df is None:
        df = pl.read_csv(ticker_csv_path)

    symbol_list = df["symbol"].to_list()
    for symbol in tqdm(symbol_list):
        if "^" in symbol:
            continue
        date = start_date
        while date <= end_date:
            date_str = date.strftime("%Y%m%d")
            output_path = (
                data_fetcher.constants.PROJECT_ROOT
                / "data/yfinance/minutes"
                / date_str
                / f"{symbol}_{date_str}.csv"
            )
            if output_path.exists() or date.weekday() >= 5:
                date += datetime.timedelta(days=1)
                continue

            fetch_data(symbol, date, output_path)
            date += datetime.timedelta(days=1)


if __name__ == "__main__":
    collect_fx_data()
    collect_stock_data()
