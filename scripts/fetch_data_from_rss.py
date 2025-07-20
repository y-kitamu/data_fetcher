"""RSSからデータを収集するスクリプト
"""

import csv
import re
import time
from pathlib import Path

import polars as pl
import xlwings as xw
from tqdm import tqdm

import data_fetcher

domestic_market_indices = [
    "N225",
    "N300",
    "TOPX",
    "JN400",
    "TSPM",
    "TSSM",
    "TSGM",
    "N500",
    "TSGMC",
    "TS1EX",
    "TSM20",
    "NVI",
    "TREIT",
    "JGB:F",
    "JGB:FL",
    "JGB:FI",
    "JGB:FDI",
]

jp_futures = [
    "N225.FUT01.OS",
    "N225.FUT02.OS",
    "N225.FUT03.OS",
    "N225.FUT04.OS",
    "N225.FUT05.OS",
    "N225M.FUT01.OS",
    "N225M.FUT02.OS",
    "N225M.FUT03.OS",
    "N225M.FUT04.OS",
    "N225M.FUT05.OS",
    "N225U.FUT01.OS",
    "N225U.FUT02.OS",
    "N225U.FUT03.OS",
    "N225U.FUT04.OS",
    "N225U.FUT05.OS",
    "TOPX.FUT01.OS",
    "TOPX.FUT02.OS",
    "TOPX.FUT03.OS",
    "TOPX.FUT04.OS",
    "TOPX.FUT05.OS",
    "MOT.FUT01.OS",
    "MOT.FUT02.OS",
    "MOT.FUT03.OS",
    "MOT.FUT04.OS",
    "MOT.FUT05.OS",
    "JN400.FUT01.OS",
    "JN400.FUT02.OS",
    "JN400.FUT03.OS",
    "JN400.FUT04.OS",
    "JN400.FUT05.OS",
]

us_market_indices = [
    "DJIA",
    "NQ",
    "NQ100",
    "SP100",
    "SP500",
]

fx_pairs = [
    "USD/JPY",
    "EUR/JPY",
    "GBP/JPY",
    "AUD/JPY",
    "NZD/JPY",
    "ZAR/JPY",
    "CAD/JPY",
    "CHF/JPY",
    "HKD/JPY",
    "SGD/JPY",
    "EUR/USD",
    "GBP/USD",
    "AUD/USD",
    "NZD/USD",
    "USD/CHF",
    "GBP/CHF",
    "EUR/GBP",
    "EUR/CHF",
    "AUD/CHF",
    "NZD/CHF",
    "AUD/NZD",
    "NOK/JPY",
    "TRY/JPY",
    "CNH/JPY",
    "MXN/JPY",
    "USD/CAD",
]


def fetch_stock_data(code_list, valid_data_len=302, max_days=8, merge=False):
    """国内株式データを取得"""
    excel_path = data_fetcher.constants.PROJECT_ROOT / "data" / "rss.xlsx"
    wb = xw.Book(excel_path)
    sheet = wb.sheets[0]

    data_num = valid_data_len * max_days
    output_root_dir = Path(r"D:\stock\data\minutes")
    if not output_root_dir.exists():
        output_root_dir = data_fetcher.constants.PROJECT_ROOT / "data/minutes"
    # output_root_dir.mkdir(exist_ok=True)

    re_date = re.compile("[0-9]+/[0-9]+/[0-9]+")

    for code in tqdm(code_list):
        sheet["A1"].formula = f'=RssChart(A2:J2,"{code}", "1M", {data_num})'
        for _ in range(10):
            time.sleep(0.5)
            value = sheet["A1"].value
            if isinstance(value, str) and value[-3:] == "配信中":
                break
        else:
            continue
        data = sheet[f"D3:J{3 + data_num - 1}"].value

        cnt = 0
        while data[0][0] is None and cnt < 5:
            time.sleep(0.5)
            cnt += 1
            data = sheet[f"D3:J{3 + data_num - 1}"].value

        if data[0][0] is None:
            print("Failed to fetch data : {}".format(code))

        for date in set([d[0] for d in data]):
            if date is None or re_date.search(date) is None:
                continue
            day_data = [d for d in data if d[0] == date]
            if len(day_data) < valid_data_len and not merge:
                print("Invalid day data : {} - {}, length = {}".format(code, date, len(day_data)))
                continue
            date = date.replace("/", "")
            output_path = (
                output_root_dir
                / date
                / "{}_{}.csv".format(code.replace(":", "_").replace("/", "_"), date)
            )
            output_path.parent.mkdir(exist_ok=True)

            if output_path.exists() and merge:
                merge_data(output_path, day_data)
            else:
                with open(output_path, "w", encoding="utf-8") as f:
                    writer = csv.writer(f, lineterminator="\n")
                    writer.writerow(["date", "minutes", "open", "high", "low", "close", "volume"])
                    writer.writerows(day_data)
        # break


def merge_data(output_path, data):
    """ """
    with open(output_path, "r") as f:
        reader = csv.reader(f)
        rows = list(reader)[1:]

    new_data = []
    for d in data:
        for r in rows:
            if r[0] == d[0] and r[1] == d[1]:
                break
        else:
            new_data.append(d)

    with open(output_path, "w", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["date", "minutes", "open", "high", "low", "close", "volume"])
        writer.writerows(rows)
        writer.writerows(new_data)


if __name__ == "__main__":
    #stock.data.update_jp_ticker_list()
    data_fetcher.ticker_list.update_jp_ticker_list()

    # 日本株
    code_list = domestic_market_indices + data_fetcher.ticker_list.get_jp_ticker_list(include_etf=True)
    fetch_stock_data(code_list, 332, 8)

    # # us
    code_list = us_market_indices
    fetch_stock_data(code_list, 391, 6, merge=True)

    # # 先物
    code_list = jp_futures
    fetch_stock_data(code_list, 3000, 1, merge=True)

    # fx
    code_list = fx_pairs
    fetch_stock_data(code_list, 3000, 1, merge=True)
