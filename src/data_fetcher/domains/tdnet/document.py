""" """

import datetime
import re
import shutil
import unicodedata
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from ixbrlparse import IXBRL
from loguru import logger

from .constants import consolidated_types, document_types, periods, report_styles
from .constants.schema import Document

# ファイル名のregexの用意
doc_type_re = "|".join(sum([dt.ident_categories for dt in document_types], []))
report_style_re = "|".join(report_styles)
period_re = "[{}]".format("".join(periods))
consolidated_re = "[{}]".format("".join(consolidated_types))
report_regex = re.compile(
    r"\d+-({period})({consolidated})({doc_type})\d\d-tse-({period})({consolidated})({report_style}).*-ixbrl.htm".format(
        period=period_re,
        consolidated=consolidated_re,
        doc_type=doc_type_re,
        report_style=report_style_re,
    )
)
summary_regex = re.compile(
    "^tse-({period}|)({consolidated}|)({report_style}).*-ixbrl.htm".format(
        period=period_re,
        consolidated=consolidated_re,
        report_style=report_style_re,
    )
)


def normalize_string(src: str) -> str:
    str = unicodedata.normalize("NFKC", src).replace(" ", "")
    pattern = r"\(.*?\)|〔.*?〕|\[.*?]〕|【.*?】|（.*?）"
    result = re.sub(pattern, "", str)
    return result


def search_document_datetiime(
    session: requests.Session, zip_filepath: Path, ixbrl: IXBRL
):
    # fetch news list from kabutan
    base_url = "https://kabutan.jp/stock/news?code={code}&nmode=3&date={yearmonth}00"
    code = zip_filepath.parent.name
    ymd = zip_filepath.name.split("_")[1]
    ymd_str = ymd[:4] + "-" + ymd[4:6] + "-" + ymd[6:]
    yearmonth = ymd[:-2]
    url = base_url.format(code=code, yearmonth=yearmonth)
    res = session.get(url)
    if res.status_code != 200:
        raise ValueError(
            f"Failed to fetch news list from {url}: status code {res.status_code}"
        )
    soup = BeautifulSoup(res.text, "html.parser")
    try:
        document_names = [
            schema
            for schema in ixbrl.to_table("nonnumeric")
            if schema["name"] == "DocumentName"
        ]
        if len(document_names) == 0:
            return
        document_name = document_names[0]["value"]
        document_name = normalize_string(document_name)
        news_contents = soup.find("div", attrs={"id": "news_contents"})
        is_financial_report = "決算短信" in document_name
        for row in news_contents.find_all("tr"):
            href = row.find("a")
            if href is None:
                href = row.find("span", attrs={"class": "fin_modal"})
                if href is None:
                    continue
            news_title = normalize_string(href.text)
            if document_name in news_title or (
                is_financial_report and "決算短信" in news_title
            ):
                if ymd_str in row.find("time")["datetime"]:
                    return datetime.datetime.fromisoformat(row.find("time")["datetime"])
    except Exception as e:
        logger.exception(
            f"Failed to search document datetime of {zip_filepath.name}: {e}"
        )
    return


def search_zip_filing_datetime(
    zip_filepath: Path, work_dir: Path, session: requests.Session
):
    # zipファイルの更新日時を提出日時とみなす
    if not work_dir.exists():
        work_dir.mkdir(exist_ok=True)
        shutil.unpack_archive(zip_filepath, extract_dir=work_dir)
    for xbrl_path in work_dir.rglob("*xbrl.htm"):
        with open(xbrl_path) as f:
            ixbrl = IXBRL(f)

        doc_dt = search_document_datetiime(session, zip_filepath, ixbrl)
        if doc_dt is not None:
            return doc_dt

    raise ValueError(f"Failed to find document datetime for {zip_filepath.name}")


def collect_documents(archive_dir: Path, zip_file: Path, session: requests.Session):
    """tdnetからダウンロードしたzipを解凍したフォルダにある報告書の情報を取得"""
    xbrl_files = sorted(archive_dir.rglob("*-ixbrl.htm"))

    security_code = zip_file.name.split("_")[0]
    filing_dt = search_zip_filing_datetime(zip_file, archive_dir, session)
    fiscal_year_end = None
    documents = []
    for xbrl_file in xbrl_files:
        # logger.debug(f"Processing {xbrl_file}")
        # ファイル名からメタ情報を抽出
        report_res = report_regex.search(xbrl_file.name)
        summary_res = summary_regex.search(xbrl_file.name)
        if report_res is None and summary_res is None:
            logger.warning(f"Cannot parse: {xbrl_file.name}")
            continue

        if report_res is not None:
            # 決算短信サマリー以外の報告書の処理
            doc_types = [
                dt
                for dt in document_types
                if report_res.group(3) in dt.ident_categories
            ]
            period = report_res.group(1)
            consolidated = report_res.group(2)
            style = report_res.group(6)
        else:
            # 決算短信サマリーの処理
            doc_types = [
                dt
                for dt in document_types
                if summary_res.group(3) in dt.ident_categories
            ]
            period = summary_res.group(1)
            consolidated = summary_res.group(2)
            style = summary_res.group(3)

        # 銘柄コード、提出日、会計年度末の取得
        with open(xbrl_file, "r") as f:
            x = IXBRL(f, raise_on_error=False)
        if len(x.errors) > 0:
            logger.warning(f"IXBRL parsing errors in {xbrl_file.name}")
        for nonnumeric in x.nonnumeric:
            if nonnumeric.name == "FiscalYearEnd":
                fiscal_year_end = datetime.datetime.strptime(
                    nonnumeric.value, "%Y-%m-%d"
                ).date()

        document = Document(
            filepath=xbrl_file,
            doc_type=doc_types,
            period=period,
            consolidated=consolidated,
            style=style,
            security_code=security_code,
            filing_date=filing_dt,
            fiscal_year_end=datetime.date.today(),
        )
        documents.append(document)

    if fiscal_year_end is None:
        raise ValueError("Cannot find summary document to get fiscal_year_end etc.")

    for doc in documents:
        doc.fiscal_year_end = fiscal_year_end

    return documents
