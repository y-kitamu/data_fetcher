import csv
import datetime
import time
from pathlib import Path

import tqdm
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from loguru import logger

from ...core.session import get_session
from ...ticker_list import get_jp_ticker_list

session = get_session()


def collect_intra_market_announces(
    symbol: str,
    date: datetime.date,
    market_open=datetime.time(9, 0, 0),
    market_close=datetime.time(15, 0, 0),
    margin_duration=datetime.timedelta(minutes=5),
) -> list[list[str | datetime.datetime]]:
    """
    指定された銘柄の取引時間内発表を収集する。
    """

    # Kabutanのニュースページからデータを取得
    url = f"https://kabutan.jp/stock/news?code={symbol}&nmode=0&date={date.strftime('%Y%m')}00"
    res = session.get(url)

    retry = 0
    while retry < 5:
        if res.status_code == 200:
            break
        logger.debug("リトライ中: {} / 5,  {}".format(retry, res.status_code))
        retry += 1
        time.sleep(1)
        res = session.get(url)

    soup = BeautifulSoup(res.text)
    news_contents = soup.find("div", attrs={"id": "news_contents"})

    intra_market_announces = []

    news_list = news_contents.find("table", attrs={"class": "s_news_list"})
    if news_list is None:
        return intra_market_announces

    for row in news_list.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 3:
            continue

        category = [
            div_cls
            for div_cls in tds[1].find("div").attrs["class"]
            if div_cls.startswith("newsctg")
        ]
        if len(category) == 0:
            logger.debug("カテゴリが見つかりませんでした: {}".format(tds[1].text))
            continue

        category = category[0].replace("newsctg", "")
        date = datetime.datetime.fromisoformat(tds[0].find("time").attrs["datetime"])

        if date.time() < market_open or (date + margin_duration).time() > market_close:
            continue

        # print("category : {}, {} date : {}".format(category, tds[1].text, date))
        intra_market_announces.append(
            [symbol, category, date, tds[1].text, tds[2].text]
        )
    return intra_market_announces


def collect_all(
    start_date=datetime.date(2024, 8, 19),
    end_date=datetime.date.today(),
    output_file=Path("./intra_market_announces.csv"),
):
    # 取引時間中に発表されたニュースを収集する
    if not output_file.exists():
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["symbol", "category", "date", "category_title", "announce_title"]
            )

    with open(output_file, "r") as f:
        reader = csv.reader(f)
        _ = next(reader)  # Skip header
        datas = [row for row in reader]

    tickers = get_jp_ticker_list()
    for symbol in tqdm.tqdm(tickers):
        collected = [d for d in datas if d[0] == symbol]

        date = start_date.replace(day=1)
        while date <= end_date:
            if any(d[2].startswith(date.strftime("%Y%m")) for d in collected):
                date += relativedelta(months=1)
                continue
            announces = collect_intra_market_announces(symbol, date)
            announces = [
                [d[0], d[1], d[2].strftime("%Y%m%d%H%M%S"), d[3], d[4]]
                for d in announces
            ]
            if len(announces) > 0:
                with open(output_file, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(announces)
            date += relativedelta(months=1)
