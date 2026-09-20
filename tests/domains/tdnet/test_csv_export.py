import datetime
from pathlib import Path

from data_fetcher.domains.tdnet.constants.schema import Document, DocumentType, TaxonomyElement
from data_fetcher.domains.tdnet.csv_export import build_fact_rows
from data_fetcher.domains.tdnet.numeric_data import collect_data_from_document
from data_fetcher.domains.tdnet.taxonomy_index import TaxonomyIndex

FIXTURES_DIR = Path(__file__).parent / "fixtures"
_DUMMY_DOC_TYPE = [DocumentType(name="決算短信サマリー", aliases=[], ident_categories=["edjp"])]


def _elem(element_id: str) -> TaxonomyElement:
    return TaxonomyElement(
        japanese_label="売上高",
        english_label="Net sales",
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
        filing_date=datetime.datetime(2024, 5, 10, 8, 12),
        fiscal_year_end=datetime.date(2024, 3, 31),
        period="a",
        consolidated="c",
        style="edjp",
    )


def test_build_fact_rows_flattens_numeric_fact():
    index = TaxonomyIndex.from_elements([_elem("test-cor:NetSales")])
    document = _document("minimal_edjp_ixbrl.htm")
    numerics, nonnumerics = collect_data_from_document(document, index)

    rows = build_fact_rows(numerics, nonnumerics, Path("0000_20240510_uid.zip"))

    assert len(rows) == 1
    row = rows[0]
    assert row["code"] == "00000"
    assert row["filing_date"] == "2024-05-10"
    assert row["filing_datetime"] == "2024-05-10T08:12:00"
    assert row["fiscal_year_end"] == "2024-03-31"
    assert row["source_file"] == "0000_20240510_uid.zip"
    assert row["element_id"] == "test-cor:NetSales"
    assert row["consolidated"] == "ConsolidatedMember"
    assert row["is_nil"] is False
    assert row["value"] == 117340000000.0
    assert row["text"] is None


def test_build_fact_rows_stores_nil_fact_as_null_value():
    index = TaxonomyIndex.from_elements([_elem("test-cor:OperatingRevenues")])
    document = _document("minimal_nil_fact_ixbrl.htm")
    numerics, nonnumerics = collect_data_from_document(document, index)

    rows = build_fact_rows(numerics, nonnumerics, Path("0000_20240510_uid.zip"))

    assert len(rows) == 1
    assert rows[0]["is_nil"] is True
    assert rows[0]["value"] is None
    assert rows[0]["text"] is None
