import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from requests import Session

import data_fetcher
from data_fetcher.session import get_session

base_url = "https://www.release.tdnet.info/inbs/"
work_dir = data_fetcher.constants.PROJECT_ROOT / "data/tdnet/tmp"
session: Session = get_session()


def download_page_data(soup, output_dir: Path, date: str) -> list[Path]:
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
                continue

            respons = session.get(base_url + zip_path)
            if respons.status_code != 200:
                print(
                    f"Failed to download {zip_path}. Status code: {respons.status_code}"
                )
                continue

            if len(respons.content) > 0:
                with open(save_path, "wb") as f:
                    f.write(respons.content)
                print(f"Downloaded {save_path}.")
                saved_files.append(save_path)
            else:
                print(f"No content in {zip_path}, skipping.")
    return saved_files


def collect_daily_data(date: datetime.date, output_dir: Path) -> list[Path]:
    saved_files = []
    date_str = date.strftime("%Y%m%d")
    idx = 1
    while True:
        url = base_url + f"I_list_{idx:03d}_{date_str}.html"
        res = session.get(url)
        if res.status_code == 404:
            break
        soup = BeautifulSoup(res.text)
        saved_files += download_page_data(soup, output_dir, date_str)
        idx += 1
    return saved_files


# def save_to_csv(zip_path: Path, output_path: Path, work_dir: Path = work_dir):
#     """ZIPファイルから決算短信を読み込み、csvに追記する"""
#     # work_dir = Path(f"./{zip_path.stem}")
#     if work_dir.exists():
#         shutil.rmtree(work_dir)
#     work_dir.mkdir(exist_ok=True)
#     shutil.unpack_archive(zip_path, extract_dir=work_dir)

#     report_date = datetime.datetime.strptime(
#         zip_path.name.split("_")[1], "%Y%m%d"
#     ).date()
#     df = data_fetcher.tdnet.convert.create_df(work_dir, report_date=report_date)

#     if output_path.exists():
#         old_df = pl.read_csv(output_path, infer_schema_length=0)
#         df = old_df.vstack(df)

#     if len(df) > 0:
#         df.write_csv(output_path)
#         data_fetcher.logger.debug(f"Saved to {output_path}")

#     shutil.rmtree(work_dir)
#     return


# def run_directory(
#     dir_path: Path,
#     output_dir: Path = data_fetcher.constants.PROJECT_ROOT / "data" / "tdnet" / "csv",
# ):
#     output_dir.mkdir(exist_ok=True)
#     for zip_path in sorted(dir_path.rglob("*.zip")):
#         output_path = output_dir / f"{zip_path.parent.name}.csv"
#         save_to_csv(zip_path, output_path, work_dir)


# def convert_directory(
#     data_root_dir: Path = data_fetcher.constants.PROJECT_ROOT
#     / "data"
#     / "tdnet"
#     / "raw",
# ):
#     """ディレクトリ内のZIPファイルをすべてCSVに変換する"""
#     dirs = [d for d in sorted(data_root_dir.rglob("*")) if d.is_dir()]
#     t = tqdm.tqdm(total=len(dirs))
#     with mp.Pool(mp.cpu_count()) as pool:
#         for i in pool.imap_unordered(run_directory, dirs):
#             t.update(1)


def main():
    output_zip_dir = data_fetcher.constants.PROJECT_ROOT / "data" / "tdnet" / "raw"
    # output_csv_dir = data_fetcher.constants.PROJECT_ROOT / "data" / "tdnet" / "csv"
    output_zip_dir.mkdir(exist_ok=True)
    # output_csv_dir.mkdir(exist_ok=True)

    end_date = datetime.datetime.now().date()
    date = end_date - datetime.timedelta(days=30)

    while date <= end_date:
        saved_files = collect_daily_data(date, output_zip_dir)
        # for zipfile_path in saved_files:
        #     output_csv_path = output_csv_dir / f"{zipfile_path.parent.name}.csv"
        #     save_to_csv(zipfile_path, output_csv_path, work_dir)
        date = datetime.timedelta(days=1) + date


if __name__ == "__main__":
    data_fetcher.debug.run_debug(main)
    # data_fetcher.debug.run_debug(convert_directory)
