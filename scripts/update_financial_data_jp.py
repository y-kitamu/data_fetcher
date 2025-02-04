"""update_financial_data_jp.py
日本の企業の財務データを更新する
"""

import csv
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import data_fetcher
from data_fetcher.kabutan.financial import (FinancialStatement,
                                            get_annual_results,
                                            get_quarter_results)


def get_financial_data(
    code: str, output_dir: Path = data_fetcher.PROJECT_ROOT / "data/kabutan/financial/"
):
    output_dir.mkdir(exist_ok=True, parents=True)
    output_path = output_dir / "{}.csv".format(code)

    results = []
    if output_path.exists():
        with open(output_path, "r", encoding="utf-8") as f:
            csv_reader = csv.reader(f)
            next(csv_reader)
            results += [FinancialStatement.from_csv(code, row) for row in csv_reader]
    num_results = len(results)

    res = requests.get(f"https://kabutan.jp/stock/finance?code={code}")
    soup = BeautifulSoup(res.text.replace("\r", ""), features="lxml")
    results += get_annual_results(soup, code)
    results += get_quarter_results(soup, code)
    results = sorted(set(results))

    if num_results == len(results):
        data_fetcher.logger.info(f"Already up to date. Code: {code}")
        return

    with open(output_path, "w", encoding="utf-8") as f:
        csv_writer = csv.writer(f, lineterminator="\n")
        csv_writer.writerow(FinancialStatement.get_csv_header())
        csv_writer.writerows([res.to_csv_row() for res in results])
    data_fetcher.logger.info(f"Save to {output_path}. Number : {len(results)}")


def main():
    code_list_csv = data_fetcher.PROJECT_ROOT / "data" / "jp_tickers.csv"
    with open(code_list_csv, "r", encoding="utf-8") as f:
        csv_reader = csv.reader(f)
        next(csv_reader)
        codes = [row[0] for row in csv_reader]

    for code in codes:
        data_fetcher.logger.info(f"Code: {code}")
        get_financial_data(code)
        time.sleep(0.5)


if __name__ == "__main__":
    main()
