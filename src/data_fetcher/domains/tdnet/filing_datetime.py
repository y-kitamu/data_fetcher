"""提出日時の解決。

kabutan.jpのニュース一覧から正確な提出時刻を推定することを試み、取得できない場合
(レート制限・一時的な通信エラー・該当ニュースなし等)はzipファイル名に埋め込まれた日付に
フォールバックする。XBRLの中身とは無関係な処理のため、ここでの失敗が解析全体を止めないよう
例外を投げない契約とする。
"""

import datetime
import re
import unicodedata
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from ixbrlparse import IXBRL
from loguru import logger

from .ixbrl_io import open_ixbrl


def _normalize_string(src: str) -> str:
    normalized = unicodedata.normalize("NFKC", src).replace(" ", "")
    pattern = r"\(.*?\)|〔.*?〕|\[.*?]〕|【.*?】|（.*?）"
    return re.sub(pattern, "", normalized)


def _parse_zip_filename_date(zip_filepath: Path) -> datetime.datetime:
    ymd = zip_filepath.name.split("_")[1]
    return datetime.datetime.strptime(ymd, "%Y%m%d")


def _search_document_datetime(
    session: requests.Session, zip_filepath: Path, ixbrl: IXBRL
) -> datetime.datetime | None:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    base_url = "https://kabutan.jp/stock/news?code={code}&nmode=3&date={yearmonth}00"
    code = zip_filepath.parent.name
    ymd = zip_filepath.name.split("_")[1]
    ymd_str = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"
    yearmonth = ymd[:-2]
    url = base_url.format(code=code, yearmonth=yearmonth)

    try:
        res = session.get(url, headers=headers)
        if res.status_code != 200:
            logger.warning(f"kabutan returned status {res.status_code} for {url}")
            return None

        document_names = [
            row for row in ixbrl.to_table("nonnumeric") if row["name"] == "DocumentName"
        ]
        if len(document_names) == 0:
            return None
        document_name = _normalize_string(document_names[0]["value"])

        soup = BeautifulSoup(res.text, "html.parser")
        news_contents = soup.find("div", attrs={"id": "news_contents"})
        if news_contents is None:
            return None

        is_financial_report = "決算短信" in document_name
        for row in news_contents.find_all("tr"):
            href = row.find("a") or row.find("span", attrs={"class": "fin_modal"})
            if href is None:
                continue
            news_title = _normalize_string(href.text)
            if document_name in news_title or (
                is_financial_report and "決算短信" in news_title
            ):
                if ymd_str in row.find("time")["datetime"]:
                    return datetime.datetime.fromisoformat(row.find("time")["datetime"])
    except Exception as e:
        logger.warning(
            f"Failed to resolve filing datetime via kabutan for {zip_filepath.name}: {e}"
        )
    return None


def resolve_filing_datetime(
    zip_filepath: Path, archive_dir: Path, session: requests.Session
) -> datetime.datetime:
    """提出日時を解決する。kabutanから取得できればその正確な時刻を、できなければ
    zipファイル名に埋め込まれた日付を返す(この関数は例外を投げない)。"""
    for xbrl_path in sorted(archive_dir.rglob("*xbrl.htm")):
        try:
            ixbrl = open_ixbrl(xbrl_path)
        except Exception as e:
            logger.warning(
                f"Failed to open {xbrl_path.name} while resolving filing datetime: {e}"
            )
            continue

        doc_dt = _search_document_datetime(session, zip_filepath, ixbrl)
        if doc_dt is not None:
            return doc_dt

    fallback = _parse_zip_filename_date(zip_filepath)
    logger.warning(
        f"Could not resolve exact filing datetime for {zip_filepath.name} via kabutan; "
        f"falling back to date embedded in filename ({fallback.date()})"
    )
    return fallback
