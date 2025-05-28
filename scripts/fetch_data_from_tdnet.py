import datetime
from pathlib import Path

from bs4 import BeautifulSoup

import data_fetcher
from data_fetcher.session import get_session

base_url = "https://www.release.tdnet.info/inbs/"
session = get_session()


def download_page_data(soup, output_dir: Path, date: str):
    table = soup.find("table", attrs={"id": "main-list-table"})
    if table is None:
        print(f"No data found for the given date. {date}")
        return

    for row in table.find_all("tr"):
        xbrl = row.find("div", attrs={"class": "xbrl-mask"})
        if xbrl is not None:
            zip_path = xbrl.find("a")["href"]

            code = row.find("td", attrs={"class": "kjCode"}).text.strip()[:4]
            uid = zip_path.replace(".zip", "")
            save_path = output_dir / code / f"{code}_{date}_{uid}.zip"
            save_path.parent.mkdir(exist_ok=True)
            if save_path.exists():
                print(f"File {save_path} already exists, skipping download.")
                continue

            respons = session.get(base_url + zip_path)
            with open(save_path, "wb") as f:
                f.write(respons.content)
            print(f"Downloaded {save_path}.")


def collect_daily_data(date: datetime.date, output_dir: Path):
    date_str = date.strftime("%Y%m%d")
    idx = 1
    while True:
        url = base_url + f"I_list_{idx:03d}_{date_str}.html"
        res = session.get(url)
        if res.status_code == 404:
            break
        soup = BeautifulSoup(res.text)
        download_page_data(soup, output_dir, date_str)
        idx += 1


if __name__ == "__main__":
    output_dir = data_fetcher.constants.PROJECT_ROOT / "data" / "tdnet" / "raw"
    output_dir.mkdir(exist_ok=True)

    end_date = datetime.datetime.now().date()
    date = end_date - datetime.timedelta(days=30)

    while date <= end_date:
        data_fetcher.debug.run_debug(collect_daily_data, date, output_dir)
        date = datetime.timedelta(days=1) + date
