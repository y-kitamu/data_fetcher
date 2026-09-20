import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from requests import Session

import data_fetcher
from data_fetcher.core.session import get_session
from data_fetcher.domains.tdnet.csv_export import append_zip_to_csv
from data_fetcher.domains.tdnet.taxonomy_element import collect_all_taxonomies
from data_fetcher.domains.tdnet.taxonomy_index import TaxonomyIndex

base_url = "https://www.release.tdnet.info/inbs/"
work_dir = data_fetcher.constants.PROJECT_ROOT / "data/tdnet/tmp"
session: Session = get_session()


def download_page_data(
    soup, output_dir: Path, date: str, taxonomy_index: TaxonomyIndex
) -> list[Path]:
    table = soup.find("table", attrs={"id": "main-list-table"})
    if table is None:
        print(f"No data found for the given date. {date}")
        return []

    saved_files = []
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
            else:
                respons = session.get(base_url + zip_path)
                if respons.status_code != 200:
                    print(
                        f"Failed to download {zip_path}. Status code: {respons.status_code}"
                    )
                    continue

                if len(respons.content) == 0:
                    print(f"No content in {zip_path}, skipping.")
                    continue

                with open(save_path, "wb") as f:
                    f.write(respons.content)
                print(f"Downloaded {save_path}.")
                saved_files.append(save_path)

            # ダウンロード済みでまだCSVに変換されていないzip(前回実行が変換前に
            # 中断した場合など)も含めて、必ず変換を試みる(append_zip_to_csvが
            # 変換済みかどうかを内部で判定するため冪等)。
            try:
                append_zip_to_csv(save_path, session, taxonomy_index, work_dir)
            except Exception as e:
                data_fetcher.logger.warning(
                    f"Failed to convert {save_path.name} to csv: {e}"
                )
    return saved_files


def collect_daily_data(
    date: datetime.date, output_dir: Path, taxonomy_index: TaxonomyIndex
) -> list[Path]:
    saved_files = []
    date_str = date.strftime("%Y%m%d")
    idx = 1
    while True:
        url = base_url + f"I_list_{idx:03d}_{date_str}.html"
        res = session.get(url)
        if res.status_code == 404:
            break
        soup = BeautifulSoup(res.text)
        saved_files += download_page_data(soup, output_dir, date_str, taxonomy_index)
        idx += 1
    return saved_files


def main():
    output_zip_dir = data_fetcher.constants.PROJECT_ROOT / "data" / "tdnet" / "raw"
    output_zip_dir.mkdir(exist_ok=True)

    taxonomy_index = TaxonomyIndex.from_elements(collect_all_taxonomies())

    end_date = datetime.datetime.now().date()
    date = end_date - datetime.timedelta(days=30)

    while date <= end_date:
        collect_daily_data(date, output_zip_dir, taxonomy_index)
        date = datetime.timedelta(days=1) + date


if __name__ == "__main__":
    data_fetcher.debug.run_debug(main)
