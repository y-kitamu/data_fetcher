import datetime
from pathlib import Path

from data_fetcher.domains.tdnet.constants.schema import Document, DocumentType, TaxonomyElement
from data_fetcher.domains.tdnet.numeric_data import (
    collect_data_from_document,
    collect_shared_context_data,
)
from data_fetcher.domains.tdnet.taxonomy_index import TaxonomyIndex

FIXTURES_DIR = Path(__file__).parent / "fixtures"
_DUMMY_DOC_TYPE = [DocumentType(name="決算短信サマリー", aliases=[], ident_categories=["edjp"])]


def _elem(element_id: str) -> TaxonomyElement:
    return TaxonomyElement(
        japanese_label="ラベル",
        english_label="Label",
        element_id=element_id,
        period_type="duration",
        abstract=False,
        balance="",
    )


def _document(filename: str) -> Document:
    return Document(
        filepath=FIXTURES_DIR / filename,
        doc_type=_DUMMY_DOC_TYPE,
        security_code="00000",
        filing_date=datetime.datetime(2024, 5, 10),
        fiscal_year_end=datetime.date(2024, 3, 31),
        period="a",
        consolidated="c",
        style="edjp",
    )


def test_normal_fact_is_collected():
    index = TaxonomyIndex.from_elements([_elem("test-cor:NetSales")])
    document = _document("minimal_edjp_ixbrl.htm")

    numerics, nonnumerics = collect_data_from_document(document, index)

    assert len(numerics) == 1
    fact = numerics[0]
    assert fact.value == 117340000000.0
    assert fact.is_nil is False
    assert fact.consolidated == "ConsolidatedMember"


def test_reit_prior_quarter_fact_is_collected():
    """以前はPrior1QuarterInstantがcontext_idsに未定義で例外になり、factが失われていた。"""
    index = TaxonomyIndex.from_elements([_elem("test-cor:TotalAssets")])
    document = _document("minimal_reit_ixbrl.htm")

    numerics, _ = collect_data_from_document(document, index)

    periods = {fact.period for fact in numerics}
    assert "CurrentQuarter" in periods
    assert "Prior1Quarter" in periods


def test_sibling_document_without_own_contexts_resolves_dates_via_shared_registry():
    """decision短信のセグメント情報等の添付ファイルは自身のix:resourcesを持たず、
    兄弟ファイル(貸借対照表等)で定義されたcontext_idを参照するのみのことがある。
    documents全体でcontextをマージしなければ、参照専用ファイルのfactは日付を失う。"""
    index = TaxonomyIndex.from_elements(
        [_elem("jpcrp_cor:RevenuesFromExternalCustomers"), _elem("test-cor:NetAssets")]
    )
    doc_with_contexts = _document("sibling_with_contexts_ixbrl.htm")
    doc_refs_only = _document("sibling_refs_only_ixbrl.htm")
    documents = [doc_with_contexts, doc_refs_only]

    shared_contexts, shared_segment_axes = collect_shared_context_data(documents)
    numerics, _ = collect_data_from_document(
        doc_refs_only, index, shared_contexts, shared_segment_axes
    )

    by_value = {fact.value: fact for fact in numerics}
    segment_fact = by_value[80000000000.0]
    assert segment_fact.start_date is not None
    assert segment_fact.end_date is not None
    assert segment_fact.segments == ["test-cor:FooReportableSegmentsMember"]

    reconciling_fact = by_value[-5000000000.0]
    assert reconciling_fact.segments == ["jpcrp_cor:ReconcilingItemsMember"]

    equity_fact = by_value[1000000000.0]
    assert equity_fact.segments == []


def test_collect_data_from_document_without_shared_registry_falls_back_to_own_file():
    """shared_contexts未指定時は自ファイル内のcontextのみで解決する(後方互換)。"""
    index = TaxonomyIndex.from_elements([_elem("test-cor:NetSales")])
    document = _document("minimal_edjp_ixbrl.htm")

    numerics, _ = collect_data_from_document(document, index)

    assert len(numerics) == 1
    assert numerics[0].start_date is not None


def test_nil_fact_becomes_none_not_zero():
    """以前はxsi:nil="true"のfactが0として保存されていた。"""
    index = TaxonomyIndex.from_elements([_elem("test-cor:OperatingRevenues")])
    document = _document("minimal_nil_fact_ixbrl.htm")

    numerics, _ = collect_data_from_document(document, index)

    assert len(numerics) == 1
    fact = numerics[0]
    assert fact.is_nil is True
    assert fact.value is None
    assert fact.previous_current == "PreviousMember"
    assert fact.consolidated == "NonConsolidatedMember"
    assert fact.forecast == "LowerMember"
