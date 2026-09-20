"""iXBRLファイル名から報告書の種別・期間・連結区分等を抽出する。"""

import re

from pydantic import BaseModel

from .constants import (
    DocumentType,
    consolidated_types,
    document_types,
    periods,
    report_styles,
)

_doc_type_re = "|".join(sum([dt.ident_categories for dt in document_types], []))
_report_style_re = "|".join(report_styles)
_period_re = "[{}]".format("".join(periods))
_consolidated_re = "[{}]".format("".join(consolidated_types))

_REPORT_RE = re.compile(
    r"\d+-({period})({consolidated})({doc_type})\d\d-tse-({period})({consolidated})({report_style}).*-ixbrl.htm".format(
        period=_period_re,
        consolidated=_consolidated_re,
        doc_type=_doc_type_re,
        report_style=_report_style_re,
    )
)
_SUMMARY_RE = re.compile(
    "^tse-({period}|)({consolidated}|)({report_style}).*-ixbrl.htm".format(
        period=_period_re,
        consolidated=_consolidated_re,
        report_style=_report_style_re,
    )
)


class FilenameMetadata(BaseModel):
    doc_types: list[DocumentType]
    period: str
    consolidated: str
    style: str


def parse_filename(name: str) -> FilenameMetadata | None:
    """報告書ファイル名から種別・期間・連結区分・スタイルを抽出する。マッチしない場合はNone。"""
    report_match = _REPORT_RE.search(name)
    if report_match is not None:
        ident = report_match.group(3)
        return FilenameMetadata(
            doc_types=[dt for dt in document_types if ident in dt.ident_categories],
            period=report_match.group(1),
            consolidated=report_match.group(2),
            style=report_match.group(6),
        )

    summary_match = _SUMMARY_RE.search(name)
    if summary_match is not None:
        ident = summary_match.group(3)
        return FilenameMetadata(
            doc_types=[dt for dt in document_types if ident in dt.ident_categories],
            period=summary_match.group(1),
            consolidated=summary_match.group(2),
            style=ident,
        )

    return None
