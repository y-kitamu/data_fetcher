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
    """報告書一覧から数値データを収集する。

    documentsは同一zip(1件のfiling)由来であること。異なるfiling由来のdocumentsを
    混在させると、context_idの偶然の一致により誤ったcontextが結合される恐れがある。
    """
    if taxonomy_index is None:
        taxonomy_index = TaxonomyIndex.from_elements(collect_all_taxonomies())

    shared_contexts, shared_segment_axes = collect_shared_context_data(documents)

    all_data = []
    for document in documents:
        numerics, _ = collect_data_from_document(
            document, taxonomy_index, shared_contexts, shared_segment_axes
        )
        all_data += numerics
    return all_data


def collect_shared_context_data(
    documents: list[Document],
) -> tuple[dict[str, ixbrlContext], dict[str, str]]:
    """フィリング内の兄弟ドキュメント全体からcontext定義とセグメント軸をマージする。

    TDnetの決算短信zipは複数の-ixbrl.htm添付ファイルで1つの論理XBRLインスタンスを
    構成するが、<xbrli:context>定義(日付・ディメンション)は最初のファイルにしか
    物理的に存在せず、他の添付ファイルはcontextRefで参照するのみで再定義しない。
    そのため各ファイルを独立にパースすると、最初のファイル以外のfactで日付と
    セグメント情報が失われる。ここで兄弟ファイル全体のcontext定義を1回だけマージする。

    documentsは同一zip(1件のfiling)由来であること(collect_numeric_datasの注記を参照)。
    """
    contexts: dict[str, ixbrlContext] = {}
    segment_axes: dict[str, str] = {}
    for document in documents:
        ixbrl = open_ixbrl(document.filepath)
        contexts.update(ixbrl.contexts)
        segment_axes.update(_extract_segment_axis_members(ixbrl.soup))
    return contexts, segment_axes


def _extract_segment_axis_members(soup) -> dict[str, str]:
    """context_id -> 事業セグメント軸のmember値。

    セグメント別数値(jpcrp_cor:RevenuesFromExternalCustomers等)のディメンションは
    <xbrli:segment>ではなく<xbrli:scenario>配下のxbrldi:explicitMemberに格納されている
    (JGAAP/IFRS双方の実ファイルで確認済み)。ixbrlparseはxbrli:segmentしか見ないため、
    ここで直接抽出する。dimensionが"OperatingSegmentsAxis"(事業セグメント[軸]、EDINET
    タクソノミ上でセグメント情報に使われる唯一の軸)で終わるものだけを信頼することで、
    株主資本等変動計算書の内訳メンバー等、無関係なディメンションの混入を防ぐ。
    """
    result: dict[str, str] = {}
    resources = soup.find(["ix:resources", "resources"])
    if resources is None:
        return result
    for context in resources.find_all(["xbrli:context", "context"]):
        context_id = context.get("id")
        if not context_id:
            continue
        for member in context.find_all(["xbrldi:explicitMember", "explicitMember"]):
            dimension = member.get("dimension") or ""
            if dimension.endswith("OperatingSegmentsAxis") and member.text.strip():
                result[context_id] = member.text.strip()
    return result


def collect_data_from_document(
    document: Document,
    taxonomy_index: TaxonomyIndex,
    shared_contexts: dict[str, ixbrlContext] | None = None,
    shared_segment_axes: dict[str, str] | None = None,
) -> tuple[list[NumericData], list[NonNumericData]]:
    x = open_ixbrl(document.filepath)
    contexts: dict[str, ixbrlContext] = (
        shared_contexts if shared_contexts is not None else x.contexts
    )
    segment_axes: dict[str, str] = (
        shared_segment_axes
        if shared_segment_axes is not None
        else _extract_segment_axis_members(x.soup)
    )

    numerics = [
        fact
        for numeric in x.numeric
        if (
            fact := _build_numeric_fact(document, taxonomy_index, numeric, contexts, segment_axes)
        )
        is not None
    ]
    nonnumerics = [
        fact
        for nonnumeric in x.nonnumeric
        if (
            fact := _build_nonnumeric_fact(
                document, taxonomy_index, nonnumeric, contexts, segment_axes
            )
        )
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
    fact,
    document: Document,
    shared_contexts: dict[str, ixbrlContext],
    shared_segment_axes: dict[str, str],
) -> tuple[
    str,
    datetime.date | None,
    datetime.date | None,
    datetime.date | None,
    list[str],
    ContextAxes | None,
]:
    context = fact.context
    if isinstance(context, str):
        # 兄弟の添付ファイル(例: 損益計算書・セグメント情報)ではcontextがこのファイル内で
        # 定義されておらず生の文字列のままのことがある。フィリング全体でマージした
        # shared_contextsから引き直す。
        context = shared_contexts.get(context, context)

    if isinstance(context, ixbrlContext):
        context_id = context.id
        start_date = _convert_date(context.startdate)
        end_date = _convert_date(context.enddate)
        instant_date = _convert_date(context.instant)
        # ixbrlContext.segmentsは<xbrli:segment>のdict表現(通常は空)。
        # NumericData/NonNumericData.segmentsはlist[str]のため文字列化する。
        segments = [str(s) for s in (context.segments or [])]
    else:
        context_id = context
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

    segment_member = shared_segment_axes.get(context_id)
    if not segments and segment_member:
        segments = [segment_member]

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
    document: Document,
    taxonomy_index: TaxonomyIndex,
    numeric,
    shared_contexts: dict[str, ixbrlContext],
    shared_segment_axes: dict[str, str],
) -> NumericData | None:
    context_id, start_date, end_date, instant_date, segments, axes = _resolve_context(
        numeric, document, shared_contexts, shared_segment_axes
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
    document: Document,
    taxonomy_index: TaxonomyIndex,
    nonnumeric,
    shared_contexts: dict[str, ixbrlContext],
    shared_segment_axes: dict[str, str],
) -> NonNumericData | None:
    context_id, start_date, end_date, instant_date, segments, axes = _resolve_context(
        nonnumeric, document, shared_contexts, shared_segment_axes
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
