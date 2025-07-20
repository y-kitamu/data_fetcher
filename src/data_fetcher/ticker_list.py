"""ticker_list.py"""

import csv
from pathlib import Path

import polars as pl
import requests
import xlrd
from fake_useragent import UserAgent

from .constants import PROJECT_ROOT
from .logging import logger


def update_us_ticker_list(output_path: Path = PROJECT_ROOT / "data" / "us_tickers.csv"):
    ua = UserAgent()
    headers = {"User-Agent": str(ua.chrome)}

    url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0&download=true"
    response = requests.get(url, headers=headers)
    res = response.json()
    df = pl.from_dicts(res["data"]["rows"])
    if len(df) < 100:
        logger.debug("Something went wrong. Failed to update symbol ticker list.")
        return
    df.write_csv(output_path)
    return df


def update_jp_ticker_list(
    output_path: Path = PROJECT_ROOT / "data" / "jp_tickers.csv",
):
    source_url: str = (
        "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    )
    res = requests.get(source_url)
    workbook = xlrd.open_workbook(file_contents=res.content)
    sheets = workbook.sheets()

    rows = []
    for i in range(sheets[0].nrows):
        rows.append([str(col).replace(".0", "") for col in sheets[0].row_values(i)[1:]])
    workbook.release_resources()

    output_path.parent.mkdir(exist_ok=True, parents=True)
    with open(output_path, "w", encoding="utf-8") as f:
        csv_writer = csv.writer(f, lineterminator="\n")
        csv_writer.writerows(rows)


def get_jp_ticker_df(include_etf: bool = False) -> pl.DataFrame:
    code_csv_path = PROJECT_ROOT / "data/jp_tickers.csv"
    code_df = pl.read_csv(code_csv_path)
    if not include_etf:
        code_df = code_df.filter(pl.col("市場・商品区分").str.contains("内国株式"))
    code_df = code_df.filter(pl.col("コード").str.len_chars() == 4)
    return code_df


def get_jp_ticker_list(include_etf: bool = False) -> list[str]:
    code_df = get_jp_ticker_df(include_etf=include_etf)
    return [code for code in code_df["コード"].to_list() if len(code) == 4]
