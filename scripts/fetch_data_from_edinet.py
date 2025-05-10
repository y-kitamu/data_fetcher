"""fetch_data_from_edinet.py"""

import csv
import datetime
import zipfile
from io import BytesIO
from pathlib import Path

import data_fetcher

api_key = "c528ad6f91db40468bf86c3f080daaff"
endpoint = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
session = data_fetcher.session.get_session(max_requests_per_second=1)


target_summary_taxonomy = dict(
    net_sales="jpcrp_cor:NetSalesSummaryOfBusinessResults",
    ordinary_incom="jpcrp_cor:OrdinaryIncomeLossSummaryOfBusinessResults",
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
    ordinary_income="jppfs_cor:OrdinaryIncome",
)


def get_document(doc_id):
    # edinetからzipファイルを取得
    doc_endpoint = f"https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
    doc_params = {"type": "5", "Subscription-Key": api_key}
    doc_param_txt = "&".join([f"{key}={value}" for key, value in doc_params.items()])
    url = f"{doc_endpoint}?{doc_param_txt}"
    res = session.get(url)

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


def extract_data(rows, target_keys):
    data = {}
    for key, value in target_keys.items():
        extracted = [row for row in rows if row[0] == value]
        consolidated = [d for d in extracted if "_NonConsolidatedMember" not in d[2]]
        if len(consolidated) > 0:
            extracted = consolidated

        extracted = [
            [d[2].replace("_NonConsolidatedMember", ""), d[-1]] for d in extracted
        ]
        data[key] = extracted
    return data


def create_csv_rows(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    data: dict[str, list[list[str]]],
):
    q_data = {}
    y_data = {}

    for key, val in data.items():
        if len(val) > 0:
            if "Q" in val[0]:
                q_data[key] = val
            elif "Y" in val[0]:
                y_data[key] = val

    rows = []
    if len(q_data) > 0:
        rows += create_csv_rows_impl(start_date, end_date, q_data, 3)
    if len(y_data) > 0:
        rows += create_csv_rows_impl(start_date, end_date, y_data, 12)
    return rows


def create_csv_rows_impl(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    data: dict[str, list[list[str]]],
    delta: int,
):
    header = [key for key in data.keys()]

    def split(d: list[str]):
        split_str = "Q" if delta == 3 else "Y"
        splits = d[0].split(split_str)
        return splits

    keys = set(sum([[split(d)[0] for d in val] for val in data.values()], []))
    dates_dict = {}
    for key in keys:
        if key == "Current":
            dates_dict[key] = [start_date, end_date]
        else:
            delta_month = int(key.replace("Prior", "")) * delta
            dyear = delta_month // 12
            dmonth = delta_month % 12

            if delta_month >= start_date.month:
                sdyear = dyear + 1
                sdmonth = dmonth - 12
            else:
                sdyear = dyear
                sdmonth = dmonth
            sdate = datetime.datetime(
                start_date.year - sdyear, start_date.month - sdmonth, start_date.day
            )

            if delta_month >= end_date.month:
                edyear = dyear + 1
                edmonth = dmonth - 12
            else:
                edyear = dyear
                edmonth = dmonth
            edate = datetime.datetime(
                end_date.year - edyear, end_date.month - edmonth, end_date.day
            )
            dates_dict[key] = [sdate, edate]

    rows = []
    for key in keys:
        row = [d.strftime("%Y-%m-%d") for d in dates_dict[key]]
        for head in header:
            for d in data[head]:
                if key in d[0]:
                    if d[1] == "－":
                        row.append("")
                    else:
                        row.append(d[1])
                    break
        rows.append(row)
    return rows


def get_target_docs_info(target_date: datetime.date):
    params = {
        "date": target_date.strftime("%Y-%m-%d"),
        "type": "2",
        "Subscription-Key": api_key,
    }
    params_txt = "&".join([f"{key}={value}" for key, value in params.items()])
    url = f"{endpoint}?{params_txt}"
    res = session.get(url)
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


def get_parent_root_doc_info(doc_id: str | None, target_docs: list[dict] | None):
    if doc_id is None:
        return None

    if target_docs is None:
        return None

    for doc in target_docs:
        if doc["docID"] == doc_id:
            if doc["parentDocID"] is not None:
                return get_parent_root_doc_info(doc["parentDocID"], target_docs)
            return doc
    return None


def main(target_date: datetime.date, output_dir: Path):
    target_docs = get_target_docs_info(target_date)

    for doc in target_docs:
        doc_id = doc["docID"]
        rows = get_document(doc_id)

        date_str = datetime.datetime.strptime(
            doc["submitDateTime"], "%Y-%m-%d %H:%M"
        ).strftime("%Y%m%d%H%M")
        code = doc["secCode"] or doc["edinetCode"]

        if doc["periodStart"] is None or doc["periodEnd"] is None:
            parent_doc = get_parent_root_doc_info(doc["parentDocID"], target_docs)
            doc["periodStart"] = doc["periodStart"] or parent_doc["periodStart"]
            doc["periodEnd"] = doc["periodEnd"] or parent_doc["periodEnd"]
        start_date = datetime.datetime.strptime(doc["periodStart"], "%Y-%m-%d")
        end_date = datetime.datetime.strptime(doc["periodEnd"], "%Y-%m-%d")

        output_path = output_dir / f"{code}_{date_str}.csv"
        data = extract_data(rows, target_summary_taxonomy)
        header = [key for key in data.keys()]
        rows = create_csv_rows(start_date, end_date, data)
        rows = sorted(rows, key=lambda x: x[0])

        if len(rows) > 0:
            with open(output_path, "w", encoding="utf-8") as f:
                csv_writer = csv.writer(f, lineterminator="\n")
                csv_writer.writerow(["start_date", "end_date"] + header)
                csv_writer.writerows(rows)
            print("Saved : ", output_path)


def run_all():
    # 10年前から現在までのデータを取得
    output_dir = Path("data/edinet/")
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=365 * 10)
    end_date = today
    date = start_date
    while date < end_date:
        print("Fetching data for date : ", date)
        main(date, output_dir)
        date += datetime.timedelta(days=1)


if __name__ == "__main__":
    data_fetcher.debug.run_debug(run_all)
