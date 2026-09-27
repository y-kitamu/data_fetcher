import io
import zipfile
from unittest.mock import MagicMock

import polars as pl
import pytest

from data_fetcher.domains.edinet.csv_export import (
    DocumentUnavailableError,
    append_document_to_csv,
    build_fact_rows,
    download_document_rows,
    output_code,
)
from data_fetcher.domains.tdnet.csv_export import ROW_SCHEMA

_HEADER = ["要素ID", "項目名", "コンテキストID", "相対年度", "連結・個別", "期間・時点", "ユニットID", "単位", "値"]

_DOC = {
    "docID": "S100TEST",
    "edinetCode": "E00012",
    "secCode": "13010",
    "submitDateTime": "2025-06-25 16:11",
    "periodStart": "2024-04-01",
    "periodEnd": "2025-03-31",
}


def _row(element_id, context_id, consolidation, value, unit="JPY", label=""):
    return [element_id, label, context_id, "", consolidation, "", unit, "", value]


def _dei(name, value):
    return _row(f"jpdei_cor:{name}", "FilingDateInstant", "その他", value, "－")


_DEI_ROWS = [
    _dei("AccountingStandardsDEI", "IFRS"),
    _dei("WhetherConsolidatedFinancialStatementsArePreparedDEI", "true"),
    _dei("TypeOfCurrentPeriodDEI", "Q2"),
    _dei("CurrentFiscalYearStartDateDEI", "2024-04-01"),
    _dei("CurrentFiscalYearEndDateDEI", "2025-03-31"),
    _dei("CurrentPeriodEndDateDEI", "2024-09-30"),
]


def _zip_bytes(rows: list[list[str]]) -> bytes:
    text = io.StringIO()
    for row in [_HEADER, *rows]:
        text.write("\t".join(f'"{col}"' for col in row) + "\n")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("XBRL_TO_CSV/jpcrp040300-q2r-001.csv", text.getvalue().encode("utf-16"))
    return buffer.getvalue()


def _session(content: bytes) -> MagicMock:
    session = MagicMock()
    session.get.return_value.content = content
    session.get.return_value.text = content.decode("utf-8", errors="ignore")
    return session


def test_output_code_uses_four_digit_sec_code_or_edinet_code():
    assert output_code(_DOC) == "1301"
    assert output_code({**_DOC, "secCode": None}) == "E00012"


def test_build_fact_rows_sets_document_fields_from_dei():
    rows = build_fact_rows(_DOC, [*_DEI_ROWS, _row("jppfs_cor:NetSales", "CurrentYTDDuration", "連結", "100")])
    row = rows[-1]
    assert row["code"] == "1301"
    assert row["filing_date"] == "2025-06-25"
    assert row["filing_datetime"] == "2025-06-25T16:11:00+09:00"
    assert row["fiscal_year_end"] == "2025-03-31"
    assert row["doc_period"] == "q"
    assert row["doc_consolidated"] == "c"
    assert row["doc_style"] == "edif"
    assert row["source_file"] == "S100TEST"
    assert list(row) == list(ROW_SCHEMA)


@pytest.mark.parametrize(
    ("context_id", "start", "end", "instant"),
    [
        ("CurrentYTDDuration", "2024-04-01", "2024-09-30", None),
        ("Prior1YTDDuration", "2023-04-01", "2023-09-30", None),
        ("CurrentQuarterDuration", "2024-07-01", "2024-09-30", None),
        ("CurrentQuarterInstant", None, None, "2024-09-30"),
        ("Prior1YearInstant", None, None, "2024-03-31"),
        ("Prior4YearDuration", "2020-04-01", "2021-03-31", None),
        ("FilingDateInstant", None, None, "2025-06-25"),
    ],
)
def test_build_fact_rows_resolves_context_dates(context_id, start, end, instant):
    row = build_fact_rows(_DOC, [*_DEI_ROWS, _row("jppfs_cor:NetSales", context_id, "連結", "1")])[-1]
    assert (row["start_date"], row["end_date"], row["instant_date"]) == (start, end, instant)


def test_build_fact_rows_falls_back_to_api_periods_without_dei():
    row = build_fact_rows(_DOC, [_row("jppfs_cor:NetSales", "CurrentYearDuration", "連結", "1")])[0]
    assert row["fiscal_year_end"] == "2025-03-31"
    assert (row["start_date"], row["end_date"]) == ("2024-04-01", "2025-03-31")
    assert row["doc_period"] is None


def test_build_fact_rows_splits_consolidation_and_segments():
    rows = build_fact_rows(
        _DOC,
        [
            _row("jppfs_cor:NetSales", "CurrentYTDDuration", "連結", "1"),
            _row("jppfs_cor:NetSales", "CurrentYTDDuration_NonConsolidatedMember", "個別", "2"),
            _row(
                "jppfs_cor:NetSales",
                "CurrentYTDDuration_jpcrp040300-q2r_E00012-000FoodReportableSegmentMember",
                "連結",
                "3",
            ),
            _row(
                "jppfs_cor:RetainedEarnings",
                "CurrentYTDDuration_NonConsolidatedMember_RetainedEarningsMember",
                "個別",
                "4",
            ),
            _row("jpcrp_cor:NetSalesSummaryOfBusinessResults", "CurrentYTDDuration", "その他", "5"),
        ],
    )
    assert [(r["period"], r["consolidated"], r["segments"]) for r in rows] == [
        ("CurrentYTD", "ConsolidatedMember", ""),
        ("CurrentYTD", "NonConsolidatedMember", ""),
        ("CurrentYTD", "ConsolidatedMember", "jpcrp040300-q2r_E00012-000FoodReportableSegmentMember"),
        ("CurrentYTD", "NonConsolidatedMember", "RetainedEarningsMember"),
        ("CurrentYTD", "", ""),
    ]


@pytest.mark.parametrize(
    ("prepared", "expected"),
    [("true", "ConsolidatedMember"), ("false", "NonConsolidatedMember")],
)
def test_build_fact_rows_uses_dei_consolidation_when_column_is_other(prepared, expected):
    rows = build_fact_rows(
        _DOC,
        [
            _dei("WhetherConsolidatedFinancialStatementsArePreparedDEI", prepared),
            _row("jpigp_cor:RevenueIFRS", "CurrentYTDDuration", "その他", "1"),
            _row("jppfs_cor:NetSales", "CurrentYTDDuration_NonConsolidatedMember", "その他", "2"),
        ],
    )
    assert [r["consolidated"] for r in rows[1:]] == [expected, "NonConsolidatedMember"]


def test_build_fact_rows_separates_numeric_nil_and_text_values():
    rows = build_fact_rows(
        _DOC,
        [
            _row("jppfs_cor:NetSales", "CurrentYTDDuration", "連結", "-1.5"),
            _row("jppfs_cor:GrossProfit", "CurrentYTDDuration", "連結", "－"),
            _row("jpcrp_cor:BusinessPolicyTextBlock", "FilingDateInstant", "その他", "方針, 1行目\n2行目", "－"),
        ],
    )
    assert [(r["is_nil"], r["value"], r["text"]) for r in rows] == [
        (False, -1.5, None),
        (True, None, None),
        (False, None, "方針, 1行目\n2行目"),
    ]


def test_download_document_rows_parses_tab_separated_utf16_with_commas_and_newlines():
    rows = [_row("jpcrp_cor:BusinessPolicyTextBlock", "FilingDateInstant", "その他", "a,b\nc", "－")]
    assert download_document_rows("S100TEST", _session(_zip_bytes(rows))) == rows


def test_download_document_rows_raises_when_document_is_unavailable():
    with pytest.raises(DocumentUnavailableError):
        download_document_rows("S100TEST", _session(b'{"metadata": {"status": "404"}}'))


def test_append_document_to_csv_is_idempotent(tmp_path):
    session = _session(_zip_bytes([*_DEI_ROWS, _row("jppfs_cor:NetSales", "CurrentYTDDuration", "連結", "100")]))

    assert append_document_to_csv(_DOC, session, None, output_dir=tmp_path) is True
    assert append_document_to_csv(_DOC, session, None, output_dir=tmp_path) is False

    df = pl.read_csv(tmp_path / "1301.csv", infer_schema_length=0)
    assert df.columns == list(ROW_SCHEMA)
    assert df.height == len(_DEI_ROWS) + 1
    assert session.get.call_count == 1
