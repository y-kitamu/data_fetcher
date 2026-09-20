"""報告書の数値データを取得する"""

import datetime

from ixbrlparse import ixbrlContext
from loguru import logger

from .constants.schema import Document, NonNumericData, NumericData, TaxonomyElement
from .context import ContextAxes, parse_context_id
from .ixbrl_io import open_ixbrl
from .taxonomy_element import collect_all_taxonomies
from .taxonomy_index import Ambiguous, Found, TaxonomyIndex


def collect_numeric_datas(
    documents: list[Document],
    taxonomy_index: TaxonomyIndex | None = None,
) -> list[NumericData]:
    """報告書一覧から数値データを収集する"""
    if taxonomy_index is None:
        taxonomy_index = TaxonomyIndex.from_elements(collect_all_taxonomies())

    all_data = []
    for document in documents:
        numerics, _ = collect_data_from_document(document, taxonomy_index)
        all_data += numerics
    return all_data


def collect_data_from_document(
    document: Document,
    taxonomy_index: TaxonomyIndex,
) -> tuple[list[NumericData], list[NonNumericData]]:
    x = open_ixbrl(document.filepath)

    numerics = [
        fact
        for numeric in x.numeric
        if (fact := _build_numeric_fact(document, taxonomy_index, numeric)) is not None
    ]
    nonnumerics = [
        fact
        for nonnumeric in x.nonnumeric
        if (fact := _build_nonnumeric_fact(document, taxonomy_index, nonnumeric))
        is not None
    ]
    return numerics, nonnumerics


def _convert_date(date_value: datetime.date | str | None) -> datetime.date | None:
    if date_value is None:
        return None
    if isinstance(date_value, datetime.date):
        return date_value
    return datetime.datetime.strptime(date_value, "%Y-%m-%d").date()


def _resolve_context(
    fact, document: Document
) -> tuple[
    str,
    datetime.date | None,
    datetime.date | None,
    datetime.date | None,
    list[str],
    ContextAxes | None,
]:
    if isinstance(fact.context, ixbrlContext):
        context_id = fact.context.id
        start_date = _convert_date(fact.context.startdate)
        end_date = _convert_date(fact.context.enddate)
        instant_date = _convert_date(fact.context.instant)
        segments = fact.context.segments or []
    else:
        context_id = fact.context
        start_date = None
        end_date = None
        instant_date = None
        segments = []

    try:
        axes = parse_context_id(context_id)
    except ValueError:
        if context_id != "FilingDateInstant":
            logger.warning(
                f"Error parsing context: {fact.context} in {document.filepath.name}"
            )
        return context_id, start_date, end_date, instant_date, segments, None

    if not segments and axes.segment is not None and axes.segment.endswith("SegmentsMember"):
        segments = [axes.segment]

    return context_id, start_date, end_date, instant_date, segments, axes


def _lookup_element(
    taxonomy_index: TaxonomyIndex,
    schema: str,
    name: str,
    document: Document,
    context,
) -> TaxonomyElement | None:
    result = taxonomy_index.lookup(schema, name)
    if isinstance(result, Found):
        return result.element
    if isinstance(result, Ambiguous):
        logger.debug(f"Multiple elements found: {name}, {context}, {schema}")
        return None
    if not schema.startswith("tse-"):
        logger.debug(
            f"Element not found: {name}, {context}, {schema}, {[d.name for d in document.doc_type]}"
        )
    return None


def _build_numeric_fact(
    document: Document, taxonomy_index: TaxonomyIndex, numeric
) -> NumericData | None:
    context_id, start_date, end_date, instant_date, segments, axes = _resolve_context(
        numeric, document
    )
    if axes is None:
        return None

    element = _lookup_element(
        taxonomy_index, numeric.schema, numeric.name, document, numeric.context
    )
    if element is None:
        return None

    is_nil = numeric.soup_tag is not None and numeric.soup_tag.get("xsi:nil") == "true"

    return NumericData(
        document=document,
        element=element,
        context_id=context_id,
        start_date=start_date,
        end_date=end_date,
        instant_date=instant_date,
        segments=segments,
        period=axes.period,
        quarter=axes.dividend_schedule,
        consolidated=axes.consolidated,
        previous_current=axes.previous_current,
        forecast=axes.forecast,
        is_nil=is_nil,
        value=None if is_nil else numeric.value,
    )


def _build_nonnumeric_fact(
    document: Document, taxonomy_index: TaxonomyIndex, nonnumeric
) -> NonNumericData | None:
    context_id, start_date, end_date, instant_date, segments, axes = _resolve_context(
        nonnumeric, document
    )
    if axes is None:
        return None

    element = _lookup_element(
        taxonomy_index, nonnumeric.schema, nonnumeric.name, document, nonnumeric.context
    )
    if element is None:
        return None

    is_nil = (
        nonnumeric.soup_tag is not None and nonnumeric.soup_tag.get("xsi:nil") == "true"
    )

    return NonNumericData(
        document=document,
        element=element,
        context_id=context_id,
        start_date=start_date,
        end_date=end_date,
        instant_date=instant_date,
        segments=segments,
        period=axes.period,
        quarter=axes.dividend_schedule,
        consolidated=axes.consolidated,
        previous_current=axes.previous_current,
        forecast=axes.forecast,
        is_nil=is_nil,
        value=nonnumeric.value,
    )
