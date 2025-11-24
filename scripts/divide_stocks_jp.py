"""divide_stocks_jp.py"""

import re
from datetime import date, datetime, timedelta

import polars as pl
import requests
from bs4 import BeautifulSoup

import data_fetcher


def divide_stock(code: str, divide_date: date, rate: float):
    """Divide stock price and volume by rate.
    Args:
        code (str): stock code
        divide_date (date): date of divide (この日より前の株価を修正する)
        rate (float): divide rate
    """
    csv_path = (
        data_fetcher.constants.PROJECT_ROOT / "data/kabutan/daily" / f"{code}.csv"
    )
    if not csv_path.exists():
        return
    df = data_fetcher.domains.kabutan.io.read_data_csv(csv_path, exclude_none=False)

    def _get_expression(key: str, date: date, rate):
        return (
            pl.when(pl.col("date") < date)
            .then(pl.col(key) / rate)
            .otherwise(pl.col(key))
        )

    target_df = df.with_columns(
        pl.col("date").dt.strftime("%Y/%m/%d"),
        _get_expression("open", divide_date, rate),
        _get_expression("high", divide_date, rate),
        _get_expression("low", divide_date, rate),
        _get_expression("close", divide_date, rate),
        _get_expression("volume", divide_date, 1.0 / rate),
    )
    target_df.write_csv(csv_path)


def main():
    target_date = date.today() + timedelta(days=1)
    res = requests.get(
        "https://www.sbisec.co.jp/ETGate/WPLETmgR001Control?OutSide=on&getFlg=on&burl=search_domestic&cat1=domestic&cat2=corporate&dir=corporate&file=stock_ca_bunkatsu.html"
    )
    soup = BeautifulSoup(res.content, features="lxml")

    re_date = re.compile("\d\d/\d\d/\d\d")
    main_ = soup.find("div", {"id": "main"})
    if main_ is not None:
        for tr in main_.find_all("tr", {"align": "center"}):
            tds = tr.find_all("td")
            if len(tds) != 4:
                continue
            # print([td.text for td in tds])
            res = re_date.search(tds[0].text)
            if res is None:
                continue
            divide_date = datetime.strptime(res.group(0), "%y/%m/%d").date()
            if divide_date == target_date:
                code = tds[1].text.split("\xa0")[1]
                splits = tds[3].text.split("\xa0")
                rate = float(splits[3]) / float(splits[1])
                divide_stock(code, divide_date, rate)
                print(f"Divide stock : code = {code}, rate = {rate}")


if __name__ == "__main__":
    main()
