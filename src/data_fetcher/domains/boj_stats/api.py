"""api.py - 日銀時系列統計データ検索サイトの一括ダウンロードZIPを取得する

企業物価指数(cgpi_m_jp.zip)・短観(co.zip)は
https://www.stat-search.boj.or.jp/info/dload.html で公開されているZIPファイルを
認証不要でそのままダウンロードできる。
"""

import io
import zipfile

import requests

from ...core.retry import retry_with_backoff

BASE_URL = "https://www.stat-search.boj.or.jp/info"
TIMEOUT = 30.0
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}


@retry_with_backoff(
    max_retries=4, base_delay=3.0, exceptions=(requests.exceptions.RequestException,)
)
def download_zip(session: requests.Session, zip_filename: str) -> bytes:
    """`dload.html`配下のZIPファイルをダウンロードする（例: "cgpi_m_jp.zip"）。"""
    res = session.get(f"{BASE_URL}/{zip_filename}", headers=HEADERS, timeout=TIMEOUT)
    res.raise_for_status()
    return res.content


def extract_csv_from_zip(zip_bytes: bytes, csv_filename: str) -> bytes:
    """ZIP内の指定CSVファイルの生バイト列を取り出す（Shift_JISのままデコードしない）。"""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        return zf.read(csv_filename)
