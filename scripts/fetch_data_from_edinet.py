"""fetch_data_from_edinet.py"""

import csv
import datetime
import zipfile
from io import BytesIO
from pathlib import Path

from dateutil.relativedelta import relativedelta
from requests.exceptions import Timeout

import data_fetcher

api_key = "c528ad6f91db40468bf86c3f080daaff"
endpoint = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
session = data_fetcher.session.get_session(max_requests_per_second=5)
doc_dir = data_fetcher.constants.PROJECT_ROOT / Path("data/edinet")

timeout = 5.0

target_summary_taxonomy = dict(
    net_sales="jpcrp_cor:NetSalesSummaryOfBusinessResults",
    ordinary_income="jpcrp_cor:OrdinaryIncomeLossSummaryOfBusinessResults",
    net_income="jpcrp_cor:NetIncomeLossSummaryOfBusinessResults",
    comprehensive_income="jpcrp_cor:NetIncomeLossSummaryOfBusinessResults",
    net_asset="jpcrp_cor:NetAssetsSummaryOfBusinessResults",
    total_asset="jpcrp_cor:TotalAssetsSummaryOfBusinessResults",
    bps="jpcrp_cor:NetAssetsPerShareSummaryOfBusinessResults",  # １株当たり純資産額
    eps="jpcrp_cor:BasicEarningsLossPerShareSummaryOfBusinessResults",  # earnings per share
    diluted_eps="jpcrp_cor:DilutedEarningsPerShareSummaryOfBusinessResults",
    equity_ratio="jpcrp_cor:EquityToAssetRatioSummaryOfBusinessResults",  # 自己資本比率
    roe="jpcrp_cor:RateOfReturnOnEquitySummaryOfBusinessResults",  # 自己資本利益率
    number_of_employee="jpcrp_cor:NumberOfEmployees",  # 従業員数
    number_of_temporary_worker="jpcrp_cor:AverageNumberOfTemporaryWorkers",  # 平均臨時雇用人員
    number_of_shares="jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults",  # 発行済株式総数
    cost_of_sales="jppfs_cor:CostOfSales",
    gross_profit="jppfs_cor:GrossProfit",
    operating_income="jppfs_cor:OperatingIncome",
)


def get_document(doc_id):
    # edinetからzipファイルを取得
    doc_endpoint = f"https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
    doc_params = {"type": "5", "Subscription-Key": api_key}
    doc_param_txt = "&".join([f"{key}={value}" for key, value in doc_params.items()])
    url = f"{doc_endpoint}?{doc_param_txt}"

    try:
        res = session.get(url, timeout=timeout)
    except Timeout:
        print(f"Failed to get a document of the id : {doc_id}. Retry.")
        # return get_document(doc_id)
        return []

    # zipファイルからcsvを抜き出す
    filebuffer = BytesIO(res.content)
    rows = []
    with zipfile.ZipFile(filebuffer, mode="r") as zip:
        for filename in zip.namelist():
            with zip.open(filename, "r") as f:
                txt = f.read().decode("utf-16").replace("\t", ",")
            rows += [
                [col.replace('"', "") for col in row.strip().split(",")]
                for row in txt.split("\n")
            ]
    return rows


def append_date_period(
    row: [str], start_date: datetime.datetime, end_date: datetime.datetime
):
    period_str = row[0]
    split_char = "Q" if "Q" in period_str else "Y"
    delta = 3 if split_char == "Q" else 12
    if "Q" not in period_str and "Y" not in period_str:
        if "Interim" in period_str:
            split_char = "I"
            delta = 3
        else:
            print(f"Invalid period string : {period_str}")
            return [None, None] + row[1:]

    key = period_str.split(split_char)[0]
    if key == "Current" or key == "":
        sdate, edate = start_date, end_date
    else:
        delta_month = relativedelta(months=int(key.replace("Prior", "")) * delta)
        sdate = start_date - delta_month
        edate = end_date - delta_month
    return [sdate.strftime("%Y-%m-%d"), edate.strftime("%Y-%m-%d")] + row[1:]


def extract_data(
    rows: list[list[str]],
    target_keys: dict[str, str],
    start_date: datetime.datetime,
    end_date: datetime.datetime,
) -> list[list[str]]:
    data = []
    for key, value in target_keys.items():
        extracted = [row for row in rows if row[0] == value]
        consolidated = [d for d in extracted if "_" not in d[2]]
        if len(consolidated) > 0:
            extracted = consolidated

        extracted = [[d[2].split("_")[0]] + [d[0], d[2], d[-1]] for d in extracted]
        data += [
            [key] + append_date_period(row, start_date, end_date) for row in extracted
        ]
    return data


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


def get_parent_root_doc_info(doc_id: str | None, doc_list: list[list[str]]):
    if doc_id is None:
        return None

    for doc in doc_list:
        if doc[0] == doc_id:
            if doc[-1] is not None:
                return get_parent_root_doc_info(doc[-1], doc_list)
            return doc
    return None


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


def main(target_date: datetime.date, output_dir: Path):
    header = [
        "key",
        "start_date",
        "end_date",
        "edinet_key",
        "period",
        "value",
        "announce_date",
    ]
    target_docs = get_target_docs_info(target_date)
    doc_list = update_document_list(target_docs, doc_dir)

    for doc in target_docs:
        doc_id = doc["docID"]
        rows = get_document(doc_id)

        date_str = datetime.datetime.strptime(
            doc["submitDateTime"], "%Y-%m-%d %H:%M"
        ).strftime("%Y%m%d%H%M")
        code = doc["secCode"] or doc["edinetCode"]

        if doc["periodStart"] is None or doc["periodEnd"] is None:
            parent_doc = get_parent_root_doc_info(doc["parentDocID"], doc_list)
            if parent_doc is not None:
                doc["periodStart"] = doc["periodStart"] or parent_doc[4]
                doc["periodEnd"] = doc["periodEnd"] or parent_doc[5]
        if doc["periodStart"] is None or doc["periodEnd"] is None:
            print("No period start or end date")
            continue

        start_date = datetime.datetime.strptime(doc["periodStart"], "%Y-%m-%d")
        end_date = datetime.datetime.strptime(doc["periodEnd"], "%Y-%m-%d")

        output_path = output_dir / f"{code}.csv"
        data = [
            row + [date_str]
            for row in extract_data(rows, target_summary_taxonomy, start_date, end_date)
        ]

        if len(rows) > 0:
            if output_path.exists():
                with open(output_path, "r", encoding="utf-8") as f:
                    csv_reader = csv.reader(f)
                    header_row = next(csv_reader)
                    rows = list(csv_reader)

                # exclude duplicate
                filtered = []
                for d in data:
                    for row in rows:
                        if d[3] == row[3] and d[4] == row[4] and d[5] == row[5]:
                            break
                    else:
                        filtered.append(d)

                with open(output_path, "a", encoding="utf-8") as f:
                    csv_writer = csv.writer(f, lineterminator="\n")
                    csv_writer.writerows(filtered)
            else:
                with open(output_path, "w", encoding="utf-8") as f:
                    csv_writer = csv.writer(f, lineterminator="\n")
                    csv_writer.writerow(header)
                    csv_writer.writerows(data)
            print("Saved : ", output_path)


def run_all():
    # 10年前から現在までのデータを取得
    output_dir = data_fetcher.constants.PROJECT_ROOT / Path("data/edinet/financial")
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
        main(date, output_dir)
        date += datetime.timedelta(days=1)


if __name__ == "__main__":
    data_fetcher.debug.run_debug(run_all)
