""" """

import datetime
import re
from pathlib import Path

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


def collect_documents(archive_dir: Path, zip_file: Path):
    """tdnetからダウンロードしたzipを解凍したフォルダにある報告書の情報を取得"""
    xbrl_files = sorted(archive_dir.rglob("*-ixbrl.htm"))
    logger.debug(f"Found {len(xbrl_files)} ixbrl files in {archive_dir}")

    security_code = zip_file.name.split("_")[0]
    filing_date = datetime.datetime.strptime(
        zip_file.name.split("_")[1], "%Y%m%d"
    ).date()
    fiscal_year_end = None
    documents = []
    for xbrl_file in xbrl_files:
        logger.debug(f"Processing {xbrl_file}")
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
            filing_date=filing_date,
            fiscal_year_end=datetime.date.today(),
        )
        documents.append(document)

    if fiscal_year_end is None:
        raise ValueError("Cannot find summary document to get fiscal_year_end etc.")

    for doc in documents:
        doc.security_code = security_code
        doc.filing_date = filing_date
        doc.fiscal_year_end = fiscal_year_end

    return documents
