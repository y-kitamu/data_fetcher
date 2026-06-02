"""update_jp_tickers_list.py
銘柄一覧リストを更新する
"""

import csv
from pathlib import Path

import requests
import tqdm
import xlrd
from bs4 import BeautifulSoup

import data_fetcher


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


def extract_themes(html):
    soup = BeautifulSoup(html)
    kobetu_right = soup.find("div", attrs={"id": "kobetsu_right"})
    if kobetu_right is None:
        return []
    company_block = kobetu_right.find("div", attrs={"class": "company_block"})
    if company_block is None:
        return []
    themes = []
    for trow in company_block.find_all("tr"):
        header = trow.find("th", attrs={"scope": "row"})
        if header is not None and header.text == "テーマ":
            themes += [li.text for li in trow.find_all("li")]
    return themes


def update_themes_csv():
    session = data_fetcher.core.get_session(max_requests_per_second=5)
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

    tickers = data_fetcher.core.ticker_list.get_jp_ticker_list()
    base_url = "https://kabutan.jp/stock/?code={}"
    ticker_themes = []
    for ticker in tqdm.tqdm(tickers):
        url = base_url.format(ticker)
        response = session.get(url)
        ticker_themes.append([ticker, extract_themes(response.text)])

    output_file = data_fetcher.constants.PROJECT_ROOT / "data/jp_ticker_themes.csv"
    with open(output_file, "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ticker", "themes"])
        writer.writerows(ticker_themes)


def main():
    save_code_list_to_csv(data_fetcher.constants.JP_TICKERS_PATH)
    update_themes_csv()


if __name__ == "__main__":
    main()
