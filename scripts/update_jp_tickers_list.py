"""update_jp_tickers_list.py
銘柄一覧リストを更新する
"""

import csv
from pathlib import Path

import requests
import xlrd

import data_fetcher

STOCK_DATA_LIST = data_fetcher.constants.PROJECT_ROOT / "data" / "jp_tickers.csv"


def save_code_list_to_csv(
    output_csv_path: Path,
    source_url: str = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls",
):
    res = requests.get(source_url)
    workbook = xlrd.open_workbook(file_contents=res.content)
    sheets = workbook.sheets()

    rows = []
    for i in range(sheets[0].nrows):
        rows.append([str(col).replace(".0", "") for col in sheets[0].row_values(i)[1:]])
    workbook.release_resources()

    output_csv_path.parent.mkdir(exist_ok=True, parents=True)
    with open(output_csv_path, "w", encoding="utf-8") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerows(rows)


def main():
    save_code_list_to_csv(STOCK_DATA_LIST)


if __name__ == "__main__":
    main()
