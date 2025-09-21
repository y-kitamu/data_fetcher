"""報告書の数値データを取得する"""

import re

from ixbrlparse import IXBRL, ixbrlContext
from loguru import logger

from .constants.schema import Document, NumericData, TaxonomyElement
from .taxonomy_element import collect_all_taxonomies

context_ids = [
    "CurrentYear",
    "CurrentQuarter",
    "CurrentAccumulatedQ1",
    "CurrentAccumulatedQ2",
    "CurrentAccumulatedQ3",
    "NextYear",
    "Next1Year",
    "Next2Year",
    "NextAccumulatedQ1",
    "NextAccumulatedQ2",
    "NextAccumulatedQ3",
    "PriorYear",
    "Prior1Year",
    "Prior2Year",
    "PriorAccumulatedQ1",
    "PriorAccumulatedQ2",
    "PriorAccumulatedQ3",
    "CurrentYTD",
    "PriorYTD",
    "Prior1YTD",
    "Prior2YTD",
]
AnnualDividendPaymentScheduleAxi = [
    "FirstQuarterMember",
    "SecondQuarterMember",
    "ThirdQuarterMember",
    "YearEndMember",
    "AnnualMember",
]
ConsolidatedNonconsolidatedAxis = [
    "ConsolidatedMember",
    "NonConsolidatedMember",
]
PreviousCurrentAxis = [
    "PreviousMember",
    "CurrentMember",
]
ResultForecastAxis = [
    "ResultMember",
    "ForecastMember",
    "UpperMember",
    "LowerMember",
]


def extract_context(context_id: str):
    regex = re.compile(
        "({})(Instant|Duration)({})({})({})({})(|_.*Member)".format(
            "|".join(context_ids),
            "|".join(["_" + txt for txt in AnnualDividendPaymentScheduleAxi] + [""]),
            "|".join(["_" + txt for txt in ConsolidatedNonconsolidatedAxis] + [""]),
            "|".join(["_" + txt for txt in PreviousCurrentAxis] + [""]),
            "|".join(["_" + txt for txt in ResultForecastAxis] + [""]),
        )
    )
    res = regex.search(context_id)
    if res is None:
        raise ValueError(f"Invalid context_id: {context_id}")

    return res


def collect_numeric_data(
    documents: list[Document],
    taxonomy_elems: dict[str, list[TaxonomyElement]] | None = None,
) -> list[NumericData]:
    """報告書一覧から数値データを収集する"""
    if taxonomy_elems is None:
        taxonomy_elems = collect_all_taxonomies()
    all_data = []
    for document in documents:
        data = _collect_numeric_data_impl(document, taxonomy_elems)
        all_data += data
    return all_data


def _collect_numeric_data_impl(
    document: Document, taxonomy_elems: dict[str, list[TaxonomyElement]]
) -> list[NumericData]:
    """単一の報告書から数値データを収集する"""
    data = []
    taxonomy_elems = sum(
        [taxonomy_elems[dtype.name] for dtype in document.doc_type], []
    )
    with open(document.filepath, "r") as f:
        x = IXBRL(f, raise_on_error=False)
    if len(x.errors) > 0:
        logger.warning(f"IXBRL parsing errors in {document.filepath.name}")

    for numeric in x.numeric:
        try:
            if isinstance(numeric.context, ixbrlContext):
                res = extract_context(numeric.context.id)
            else:
                res = extract_context(numeric.context)
        except Exception:
            logger.error(
                f"Error parsing context: {numeric.context} in {document.filepath.name}"
            )
            continue

        elem_name = numeric.name if res.group(7) == "" else res.group(7)[1:]
        element = [
            e
            for e in taxonomy_elems
            if e.element_id == elem_name and e.namespace == numeric.schema
        ]
        if len(element) == 0:
            logger.debug(
                f"Element not found: {elem_name}, {numeric.name}, {numeric.context}, {numeric.schema}, {[d.name for d in document.doc_type]}"
            )
            continue
        elif len(element) > 1:
            logger.debug(
                f"Multiple elements found: {elem_name}, {numeric.name}, {numeric.context}, {numeric.schema}"
            )
            continue

        data.append(
            NumericData(
                document=document,
                element=element[0],
                period=res.group(1),
                consolidated=res.group(3),
                forecast=res.group(4),
                value=numeric.value,
            )
        )
    return data
