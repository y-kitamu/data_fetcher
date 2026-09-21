""" """

import datetime
from pathlib import Path

import requests
from loguru import logger

from .constants.schema import Document
from .filename_metadata import parse_filename
from .filing_datetime import resolve_filing_datetime
from .ixbrl_io import open_ixbrl


def collect_documents(
    archive_dir: Path, zip_file: Path, session: requests.Session
) -> list[Document]:
    """tdnetからダウンロードしたzipを解凍したフォルダにある報告書の情報を取得"""
    xbrl_files = sorted(archive_dir.rglob("*-ixbrl.htm"))

    security_code = zip_file.name.split("_")[0]
    filing_dt = resolve_filing_datetime(zip_file, archive_dir, session)
    fiscal_year_end: datetime.date | None = None
    documents = []
    for xbrl_file in xbrl_files:
        metadata = parse_filename(xbrl_file.name)
        if metadata is None:
            logger.warning(f"Cannot parse: {xbrl_file.name}")
            continue

        try:
            x = open_ixbrl(xbrl_file)
        except Exception as e:
            logger.warning(
                f"Failed to open {xbrl_file.name} while collecting documents: {e}"
            )
            continue
        for nonnumeric in x.nonnumeric:
            if nonnumeric.name == "FiscalYearEnd":
                fiscal_year_end = datetime.datetime.strptime(
                    nonnumeric.value, "%Y-%m-%d"
                ).date()

        documents.append(
            Document(
                filepath=xbrl_file,
                doc_type=metadata.doc_types,
                period=metadata.period,
                consolidated=metadata.consolidated,
                style=metadata.style,
                security_code=security_code,
                filing_date=filing_dt,
                fiscal_year_end=None,
            )
        )

    if fiscal_year_end is None:
        logger.warning(
            f"Cannot find FiscalYearEnd in {zip_file.name}; proceeding without it"
        )
    else:
        for doc in documents:
            doc.fiscal_year_end = fiscal_year_end

    return documents
