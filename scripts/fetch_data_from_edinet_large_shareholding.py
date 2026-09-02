"""fetch_data_from_edinet_large_shareholding.py
EDINETの大量保有報告書（5%ルール、docTypeCode 350/360）を日付単位で取得し、
銘柄別・報告日別の保有状況テーブルに整形する。

既存の scripts/fetch_data_from_edinet.py（財務サマリー、docTypeCode 120系）とは
別のドキュメント種別・別ディレクトリで、処理済みdocIDの重複防止リスト
(data/edinet/large_shareholding/doc_list.csv) も独立して管理する。
"""

import argparse
import csv
import datetime

import polars as pl

import data_fetcher
from data_fetcher.domains.edinet import api as edinet_api
from data_fetcher.domains.edinet import large_shareholding

OUTPUT_DIR = data_fetcher.constants.PROJECT_ROOT / "data/edinet/large_shareholding"
DOC_LIST_PATH = OUTPUT_DIR / "doc_list.csv"


def _load_processed_doc_ids() -> set[str]:
    if not DOC_LIST_PATH.exists():
        return set()
    with open(DOC_LIST_PATH, encoding="utf-8") as f:
        return {row[0] for row in csv.reader(f)}


def _append_processed_doc_ids(doc_ids: list[str]) -> None:
    if not doc_ids:
        return
    DOC_LIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DOC_LIST_PATH, "a", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerows([[doc_id] for doc_id in doc_ids])


def update_for_date(target_date: datetime.date) -> None:
    session = data_fetcher.get_session(max_requests_per_second=2)
    res = edinet_api.get_document_list(target_date, session)
    all_docs = res.get("results", []) if res else []

    target_docs = [
        doc
        for doc in all_docs
        if doc["docTypeCode"] in large_shareholding.TARGET_DOC_TYPE_CODES
        and doc["csvFlag"] == "1"
    ]
    if not target_docs:
        data_fetcher.logger.info(f"No large-shareholding documents on {target_date}.")
        return

    processed = _load_processed_doc_ids()
    new_docs = [doc for doc in target_docs if doc["docID"] not in processed]
    if not new_docs:
        data_fetcher.logger.info(
            f"{len(target_docs)} document(s) on {target_date}, all already processed."
        )
        return

    records = []
    for doc in new_docs:
        rows = edinet_api.get_document(doc["docID"], session)
        record = large_shareholding.parse_large_shareholding_document(doc["docID"], rows)
        if record is None:
            data_fetcher.logger.warning(f"Empty/unparseable document: {doc['docID']}")
            continue
        record["doc_type_code"] = doc["docTypeCode"]
        record["doc_description"] = doc["docDescription"]
        record["submit_datetime"] = doc["submitDateTime"]
        record["filer_name_raw"] = doc["filerName"]
        records.append(record)

    _append_processed_doc_ids([doc["docID"] for doc in new_docs])

    if not records:
        data_fetcher.logger.warning(f"No parseable records on {target_date}.")
        return

    df = pl.from_dicts(records)
    output_path = OUTPUT_DIR / f"{target_date.strftime('%Y%m%d')}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_path)
    data_fetcher.logger.info(
        f"Saved {output_path} ({df.height} rows, "
        f"{len(target_docs) - len(new_docs)} already processed, skipped)"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="今日から遡って処理する日数（デフォルト: 1 = 昨日のみ）",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="この日付(YYYY-MM-DD)まで遡って処理する。指定時は--daysより優先する",
    )
    args = parser.parse_args()

    today = datetime.date.today()
    if args.start_date:
        start_date = datetime.date.fromisoformat(args.start_date)
        num_days = (today - start_date).days
    else:
        num_days = args.days
    for offset in range(1, num_days + 1):
        target_date = today - datetime.timedelta(days=offset)
        update_for_date(target_date)


if __name__ == "__main__":
    main()
