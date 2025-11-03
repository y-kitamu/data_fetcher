"""api.py"""

import datetime
import time
import zipfile
from io import BytesIO

import requests
from loguru import logger
from requests.exceptions import Timeout

api_key = "c528ad6f91db40468bf86c3f080daaff"
endpoint = "https://api.edinet-fsa.go.jp/api/v2/documents.json"

timeout = 5.0


def get_document_list(target_date: datetime.date, session: requests.Session, max_retries: int = 3):
    """Get document list from EDINET API with retry logic.
    
    Args:
        target_date: Date to fetch documents for
        session: Requests session to use
        max_retries: Maximum number of retry attempts (default: 3)
        
    Returns:
        JSON response containing document list
        
    Raises:
        Timeout: If all retry attempts fail
    """
    params = {
        "date": target_date.strftime("%Y-%m-%d"),
        "type": "2",
        "Subscription-Key": api_key,
    }
    params_txt = "&".join([f"{key}={value}" for key, value in params.items()])
    url = f"{endpoint}?{params_txt}"

    for attempt in range(max_retries):
        try:
            res = session.get(url, timeout=timeout)
            return res.json()
        except Timeout:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                logger.warning(
                    f"Failed to get document list from the Edinet. "
                    f"Retry attempt {attempt + 1}/{max_retries} after {wait_time}s..."
                )
                time.sleep(wait_time)
            else:
                logger.error(
                    f"Failed to get document list after {max_retries} attempts"
                )
                raise


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
            logger.warning(f"Failed to get a document of the id : {doc_id}. Retry.")
            time.sleep(2)
            # return get_document(doc_id)
    else:
        logger.error(f"Failed to get a document of the id : {doc_id} after 3 retries.")
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
