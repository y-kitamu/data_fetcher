"""update_financial_data.py

Update financial data from Yahoo Finance.
Data is saved in ${PROJECT_ROOT}/data/codes/${ticker}.json.
"""

import argparse
import csv
import functools
import json
from pathlib import Path

import requests
import yfinance as yf
from pyrate_limiter import Duration, Limiter, RequestRate
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket

import stock

TICKER_LIST_URL = "https://www.trkd-asia.com/rakutensec/exportcsvus?all=on&vall=on&forwarding=na&target=0&theme=na&returns=na&head_office=na&name=&code=&sector=na&pageNo=&c=us&p=result&r1=on"


class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass


session = CachedLimiterSession(
    limiter=Limiter(RequestRate(1, Duration.SECOND * 0.2)),
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)
session.request = functools.partial(session.request, timeout=10)


def update_ticker_list(ticker_list_path: Path, ticker_list_url: str = TICKER_LIST_URL):
    try:
        res = requests.get(ticker_list_url)
        if res.status_code == 200:
            with open(ticker_list_path, "w") as f:
                f.write(res.text)
        else:
            stock.logger.error(f"Failed to get ticker list. Status code: {res.status_code}\n{res.text}")
    except requests.exceptions.ConnectionError:
        raise ConnectionError("Connection Error. Check your network connection.")
    except requests.exceptions.Timeout:
        raise TimeoutError("Timeout Error. Check your network connection.")
    except:
        raise Exception("Unknown Error.")


def update_financial_data(ticker: str, output_dir: Path):
    """Update financial data (fundamentals) from Yahoo Finance.
    If the output file already exists, update it.
    """
    output_path = output_dir / f"{ticker}.json"
    data = {}
    if output_path.exists():
        try:
            with open(output_path, "r") as f:
                data = json.load(f)
        except:
            stock.logger.error(f"Failed to load {output_path}.")

    new_data = yf.Ticker(ticker, session=session).quarterly_financials
    for key, value in new_data.to_dict().items():
        data[key.strftime("%Y-%m-%d")] = value

    with open(output_path, "w") as f:
        json.dump(data, f, indent=4)
    stock.logger.info(f"Saved {output_path}.")


def main(ticker_list_path: Path, output_dir: Path = stock.DATA_DIR / "codes"):
    """ """
    update_ticker_list(ticker_list_path=ticker_list_path)

    with open(ticker_list_path, "r") as f:
        reader = csv.reader(f)
        next(reader)
        tickers = [row[0] for row in reader]

    for ticker in tickers:
        try:
            update_financial_data(ticker=ticker, output_dir=output_dir)
        except KeyboardInterrupt:
            stock.logger.exception("Keyboard Interrupt.")
            break
        except:
            stock.logger.exception(f"Failed to update financial data. : {ticker}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker_list", type=Path, default=stock.DATA_DIR / "us_stock_codes.csv")
    parser.add_argument("--output_dir", type=Path, default=stock.DATA_DIR / "codes")
    args = parser.parse_args()

    main(ticker_list_path=args.ticker_list, output_dir=args.output_dir)
