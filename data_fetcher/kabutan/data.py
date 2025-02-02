"""data.py

Author : Yusuke Kitamura
Create Date : 2024-03-24 17:27:13
Copyright (c) 2019- Yusuke Kitamura <ymyk6602@gmail.com>
"""

import datetime
from typing import Any

import polars as pl
import requests
from bs4 import BeautifulSoup

from ..util import convert_to_number
from .io import read_data_csv, read_financial_csv


def get_stock_data(
    code: str, base_url: str = "https://kabutan.jp/stock/kabuka?code={}&ashi=day"
) -> list[list[Any]]:
    res = requests.get(base_url.format(code))
    soup = BeautifulSoup(res.text, features="lxml")

    daily_data: list[list[Any]] = []
    stock_tables = soup.find("div", {"id": "stock_kabuka_table"})
    if stock_tables is None:
        return daily_data
    for table in stock_tables.find_all("table"):
        tbody = table.find("tbody")
        if tbody is None:
            continue
        for table_row in tbody.find_all("tr"):
            thead = table_row.find("th")
            tdata = table_row.find_all("td")
            if thead is None or len(tdata) < 7:
                break
            date = datetime.datetime.strptime(thead.text, "%y/%m/%d")
            start = convert_to_number(tdata[0].text)
            high = convert_to_number(tdata[1].text)
            low = convert_to_number(tdata[2].text)
            end = convert_to_number(tdata[3].text)
            volume = convert_to_number(tdata[6].text)
            daily_data.append([date, start, high, low, end, volume])

    return daily_data


def get_market_capitalization(
    code: str, base_url: str = "https://kabutan.jp/stock/?code={}"
) -> int | float | None:
    res = requests.get(base_url.format(code))
    soup = BeautifulSoup(res.text, features="lxml")

    market_cap = 0
    market_cap_div = soup.find("div", {"id": "stockinfo_i3"})
    if market_cap_div is None:
        return market_cap
    market_cap_table = market_cap_div.find("table")
    if market_cap_table is None:
        return market_cap
    for table_row in market_cap_table.find_all("tr"):
        tds = table_row.find_all(["th", "td"])
        if len(tds) > 0 and tds[0].text == "時価総額":
            market_cap = convert_to_number(tds[1].text.replace("兆", "").replace("億", ""))
            if market_cap is not None:
                break

    return market_cap


def get_number_of_shares(
    code: str, base_url: str = "https://kabutan.jp/stock/?code={}"
) -> float | int | None:
    res = requests.get(base_url.format(code))
    soup = BeautifulSoup(res.text, features="lxml")

    number_of_shares = 0
    div = soup.find("div", {"id": "kobetsu_left"})
    if div is None:
        return 0
    for table in div.findAll("table"):
        for table_row in table.find_all("tr"):
            th = table_row.find("th")
            if th is not None and th.text == "発行済株式数":
                td = table_row.find("td")
                number_of_shares = convert_to_number(td.text)
                break

    return number_of_shares


def calc_estimated_capitalization(
    code: str, current_date: datetime.date = datetime.date.today()
) -> float:
    # eps、純利益から時価総額を計算する
    fdf = (
        read_financial_csv(code)
        .filter((pl.col("duration") == 3) & (pl.col("eps").abs() > 1e-5))
        .sort(pl.col("annoounce_date"))
    )
    df = read_data_csv(code, end_date=current_date).sort(pl.col("date"))

    if len(fdf) == 0:
        return -1
    num_stock = fdf["net_income"][-1] * 1000000 / fdf["eps"][-1]
    est_capit = num_stock * df["close"][-1]
    return est_capit
