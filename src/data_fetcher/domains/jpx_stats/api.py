"""api.py - JPX統計情報ページ（登録不要）のダウンローダー

投資部門別売買状況・個別銘柄信用取引残高表（日々公表銘柄）を、JPXサイトの
一覧ページから直接ダウンロードする。

ダウンロードリンクは `<ハッシュ>-att/<ファイル名>` の形式で、ハッシュ部分は
週・日ごとに変わり予測できない。そのため必ず一覧ページ(index.html)をスクレイピングし、
掲載されている最新のリンクを都度特定する（URLを日付から直接組み立てない）。

注意（日々公表銘柄について）:
このページの「個別銘柄信用取引残高表」は信用規制等により日々の開示が義務付けられた
銘柄のみを含むサブセットであり、全銘柄の週末信用残高ではない。全銘柄分は
`domains.taisyaku`（日証金 taisyaku.jp）で既に取得している。
"""

import re

import requests
from bs4 import BeautifulSoup

from ...core.retry import retry_with_backoff

BASE_URL = "https://www.jpx.co.jp"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}
TIMEOUT = 15.0

INVESTOR_TYPE_INDEX_URL = f"{BASE_URL}/markets/statistics-equities/investor-type/index.html"
# 年別アーカイブページ。archives-00 = 当年、archives-10 = 10年前（2026年時点で2016年）。
# 年選択プルダウン（ページ内 <select> の <option>）から存在を確認済み。
INVESTOR_TYPE_ARCHIVE_URL_TEMPLATE = (
    f"{BASE_URL}/markets/statistics-equities/investor-type/00-00-archives-{{idx:02d}}.html"
)
INVESTOR_TYPE_ARCHIVE_YEAR_COUNT = 11  # 当年 + 過去10年分
MARGIN_INDEX_URL = f"{BASE_URL}/markets/statistics-equities/margin/index.html"

_VAL_LINK_RE = re.compile(r"stock_val_1_\d{6}\.xls$")
_MARGIN_LINK_RE = re.compile(r"mtdailyk\d{10}\.xls$")
_DATE_SUFFIX_RE = re.compile(r"(\d{6,10})\.xls$")


@retry_with_backoff(max_retries=4, base_delay=3.0, exceptions=(requests.exceptions.RequestException,))
def _get_soup(session: requests.Session, url: str) -> BeautifulSoup:
    res = session.get(url, headers=HEADERS, timeout=TIMEOUT)
    res.raise_for_status()
    return BeautifulSoup(res.text, features="lxml")


def get_investor_type_archive_page_urls() -> list[str]:
    """投資部門別売買状況の年別アーカイブページURL一覧（当年+過去10年分）を返す。"""
    return [
        INVESTOR_TYPE_ARCHIVE_URL_TEMPLATE.format(idx=i)
        for i in range(INVESTOR_TYPE_ARCHIVE_YEAR_COUNT)
    ]


def get_investor_type_urls_from_page(
    session: requests.Session, page_url: str
) -> list[dict[str, str]]:
    """指定した一覧／アーカイブページから、週ごとの金額(value)・株数(volume)ファイルURLの
    ペアを全て抽出する。金額・株数ファイルは同じハッシュ付きディレクトリに同居しているため、
    ファイル名の `stock_val_1_` を `stock_vol_1_` に置換するだけでペアを特定できる。
    """
    soup = _get_soup(session, page_url)
    val_links = sorted({a["href"] for a in soup.find_all("a", href=_VAL_LINK_RE)})
    return [
        {
            "value_url": BASE_URL + val_href,
            "volume_url": BASE_URL + val_href.replace("stock_val_1_", "stock_vol_1_"),
        }
        for val_href in val_links
    ]


def get_latest_investor_type_urls(session: requests.Session) -> dict[str, str]:
    """投資部門別売買状況の最新週の金額(value)・株数(volume)ファイルURLを取得する。"""
    pairs = get_investor_type_urls_from_page(session, INVESTOR_TYPE_INDEX_URL)
    if not pairs:
        raise RuntimeError(
            "投資部門別売買状況の一覧ページからダウンロードリンクが見つかりません。"
            "ページ構造が変更された可能性があります。"
        )
    return max(pairs, key=lambda p: _DATE_SUFFIX_RE.search(p["value_url"]).group(1))


def get_latest_margin_url(session: requests.Session) -> str:
    """個別銘柄信用取引残高表（日々公表銘柄）の最新ファイルURLを取得する。"""
    soup = _get_soup(session, MARGIN_INDEX_URL)
    links = soup.find_all("a", href=_MARGIN_LINK_RE)
    if not links:
        raise RuntimeError(
            "信用取引残高の一覧ページからダウンロードリンクが見つかりません。"
            "ページ構造が変更された可能性があります。"
        )
    return BASE_URL + links[0]["href"]


@retry_with_backoff(max_retries=4, base_delay=3.0, exceptions=(requests.exceptions.RequestException,))
def download(session: requests.Session, url: str) -> bytes:
    res = session.get(url, headers=HEADERS, timeout=TIMEOUT)
    res.raise_for_status()
    return res.content
