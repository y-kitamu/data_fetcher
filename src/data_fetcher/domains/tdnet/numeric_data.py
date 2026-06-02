"""報告書の数値データを取得する"""

import datetime
import re

from ixbrlparse import IXBRL, ixbrlContext
from loguru import logger

from .constants.schema import Document, NonNumericData, NumericData, TaxonomyElement
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
    "Interim",
    "Prior1Interim",
    "Prior2Interim",
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
        "({})(Instant|Duration)({})({})({})({})(_.*Member|)".format(
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


def collect_numeric_datas(
    documents: list[Document],
    taxonomy_elems: list[TaxonomyElement] | None = None,
) -> list[NumericData]:
    """報告書一覧から数値データを収集する"""
    if taxonomy_elems is None:
        taxonomy_elems = collect_all_taxonomies()

    all_data = []
    for document in documents:
        numerics, nonnumerics = collect_data_from_document(document, taxonomy_elems)
        all_data += numerics
    return all_data


def collect_data_from_document(
    document: Document,
    taxonomy_elems: list[TaxonomyElement],
) -> tuple[list[NumericData], list[NonNumericData]]:
    with open(document.filepath, "r") as f:
        x = IXBRL(f, raise_on_error=False)
    if len(x.errors) > 0:
        logger.warning(f"IXBRL parsing errors in {document.filepath.name}")

    numerics = _collect_data_impl(document, taxonomy_elems, x.numeric, NumericData)
    nonnumerics = _collect_data_impl(
        document, taxonomy_elems, x.nonnumeric, NonNumericData
    )
    return numerics, nonnumerics


def _collect_data_impl(
    document: Document,
    taxonomy_elems: list[TaxonomyElement],
    financial_data: list,
    data_type,
):
    """単一の報告書から数値データを収集する"""

    def convert_dt(date_str: datetime.date | str | None) -> datetime.date | None:
        if date_str is None:
            return None
        if isinstance(date_str, datetime.date):
            return date_str
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

    data = []
    for numeric in financial_data:
        if isinstance(numeric.context, ixbrlContext):
            context_id = numeric.context.id
            start_date = convert_dt(numeric.context.startdate)
            end_date = convert_dt(numeric.context.enddate)
            instant_date = convert_dt(numeric.context.instant)
            segments = numeric.context.segments
        else:
            context_id = numeric.context
            start_date = None
            end_date = None
            instant_date = None
            segments = []

        try:
            res = extract_context(context_id)
            if segments is None or len(segments) == 0:
                if res.group(7) is not None and res.group(7).endswith("SegmentsMember"):
                    segments = [res.group(7)[1:]]
                else:
                    segments = []

        except Exception:
            if context_id != "FilingDateInstant":
                logger.info(
                    f"Error parsing context: {numeric.context} in {document.filepath.name}"
                )
            continue

        elem_name = numeric.name
        element = [
            e for e in taxonomy_elems if e.element_id == f"{numeric.schema}:{elem_name}"
        ]
        if len(element) == 0:
            if not numeric.schema.startswith("tse-"):
                logger.debug(
                    f"Element not found: {elem_name}, {numeric.name}, {numeric.context}, {numeric.schema}, {[d.name for d in document.doc_type]}"
                )
            continue
        elif len(element) > 1:
            logger.debug(
                f"Multiple elements found: {elem_name}, {numeric.name}, {numeric.context}, {numeric.schema}"
            )
            continue
        else:
            element = element[0]

        data.append(
            data_type(
                document=document,
                element=element,
                context_id=context_id,
                start_date=start_date,
                end_date=end_date,
                instant_date=instant_date,
                segments=segments,
                period=res.group(1),
                quarter=res.group(3)[1:],
                consolidated=res.group(4)[1:],
                forecast=res.group(6)[1:],
                value=numeric.value,
            )
        )
    return data
