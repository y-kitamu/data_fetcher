"""api.py"""

import datetime
import time
import zipfile
from io import BytesIO

import requests
from requests.exceptions import Timeout

api_key = "c528ad6f91db40468bf86c3f080daaff"
endpoint = "https://api.edinet-fsa.go.jp/api/v2/documents.json"

timeout = 5.0


def get_document_list(target_date: datetime.date, session: requests.Session):
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
        return get_document_list(target_date)

    return res.json()


def get_document(doc_id: str, session: requests.Session):
    # edinetからzipファイルを取得
    doc_endpoint = f"https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
    doc_params = {"type": "5", "Subscription-Key": api_key}
    doc_param_txt = "&".join([f"{key}={value}" for key, value in doc_params.items()])
    url = f"{doc_endpoint}?{doc_param_txt}"

    for i in range(3):
        try:
            res = session.get(url, timeout=timeout)
            break
        except Timeout:
            print(f"Failed to get a document of the id : {doc_id}. Retry.")
            time.sleep(2)
            # return get_document(doc_id)
    else:
        print(f"Failed to get a document of the id : {doc_id} after 3 retries.")
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
