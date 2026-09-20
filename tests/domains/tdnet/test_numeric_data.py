import datetime
from pathlib import Path

from data_fetcher.domains.tdnet.constants.schema import Document, DocumentType, TaxonomyElement
from data_fetcher.domains.tdnet.numeric_data import collect_data_from_document
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
