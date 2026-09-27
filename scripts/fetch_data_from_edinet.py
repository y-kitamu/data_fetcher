"""fetch_data_from_edinet.py"""

import csv
import datetime
from pathlib import Path

from dateutil.relativedelta import relativedelta
from requests.exceptions import Timeout

import data_fetcher
from data_fetcher.domains.edinet.csv_export import (
    OUTPUT_DIR,
    DocumentUnavailableError,
    append_document_to_csv,
    output_code,
)
from data_fetcher.domains.tdnet.taxonomy_element import collect_all_taxonomies
from data_fetcher.domains.tdnet.taxonomy_index import TaxonomyIndex

api_key = "c528ad6f91db40468bf86c3f080daaff"
endpoint = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
session = data_fetcher.get_session(max_requests_per_second=5)
doc_dir = data_fetcher.constants.PROJECT_ROOT / Path("data/edinet")

timeout = 5.0


def get_target_docs_info(target_date: datetime.date):
    params = {
        "date": target_date.strftime("%Y-%m-%d"),
        "type": "2",
        "Subscription-Key": api_key,
    }
    params_txt = "&".join([f"{key}={value}" for key, value in params.items()])
    url = f"{endpoint}?{params_txt}"

    try:
        res = session.get(url, timeout=timeout)
    except Timeout:
        print("Failed to get document list from the Edinet. Retry.")
        return get_target_docs_info(target_date)

    doc_list = res.json()["results"]
    target_doc_codes = ["120", "130", "140", "150", "160", "170"]
    target_ordinance_codes = ["010"]

    def is_target(doc):
        return (
            doc["docTypeCode"] in target_doc_codes
            and doc["ordinanceCode"] in target_ordinance_codes
            and doc["csvFlag"] == "1"
        )

    target_docs = [doc for doc in doc_list if is_target(doc)]
    return target_docs


def update_document_list(target_docs: list[dict], output_dir: Path):
    # 取得したドキュメントの情報をCSVに保存
    doc_list_path = output_dir / "doc_list.csv"

    doc_keys = [
        "docID",
        "edinetCode",
        "secCode",
        "submitDateTime",
        "periodStart",
        "periodEnd",
        "parentDocID",
    ]
    doc_info = [[doc[key] for key in doc_keys] for doc in target_docs]

    rows = []
    if doc_list_path.exists():
        with open(doc_list_path, "r") as f:
            csv_reader = csv.reader(f)
            rows = list(csv_reader)
    else:
        with open(doc_list_path, "w") as f:
            csv_writer = csv.writer(f, lineterminator="\n")
            csv_writer.writerow(doc_keys)

    # 既存のドキュメント情報と新しいドキュメント情報をマージ
    ids = [doc[0] for doc in rows]
    new_rows = [info for info in doc_info if info[0] not in ids]
    with open(doc_list_path, "a") as f:
        csv_writer = csv.writer(f, lineterminator="\n")
        csv_writer.writerows(new_rows)

    return rows + new_rows


def main(
    target_date: datetime.date, output_dir: Path, taxonomy_index: TaxonomyIndex
):
    target_docs = get_target_docs_info(target_date)
    update_document_list(target_docs, doc_dir)

    for doc in target_docs:
        try:
            if append_document_to_csv(doc, session, taxonomy_index, output_dir):
                print("Saved : ", output_dir / f"{output_code(doc)}.csv")
        except DocumentUnavailableError as e:
            print(f"Failed to get a document : {e}")


def run_all():
    # 10年前から現在までのデータを取得
    output_dir = OUTPUT_DIR
    taxonomy_index = TaxonomyIndex.from_elements(collect_all_taxonomies())
    doc_list = update_document_list([], doc_dir)
    if len(doc_list) > 0:
        # 2016-08-12 10:10
        start_date = datetime.datetime.strptime(
            doc_list[-1][3], "%Y-%m-%d %H:%M"
        ).date()
    else:
        today = datetime.date.today()
        start_date = today - relativedelta(years=10)

    end_date = datetime.date.today()
    date = start_date
    while date < end_date:
        print("Fetching data for date : ", date)
        main(date, output_dir, taxonomy_index)
        date += datetime.timedelta(days=1)


if __name__ == "__main__":
    data_fetcher.debug.run_debug(run_all)
