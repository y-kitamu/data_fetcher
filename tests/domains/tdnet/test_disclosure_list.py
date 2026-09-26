import datetime as dt

from bs4 import BeautifulSoup

from data_fetcher.domains.tdnet.disclosure_list import (
    append_disclosures_to_csv,
    parse_disclosure_row,
    parse_disclosure_rows,
)

_JST = dt.timezone(dt.timedelta(hours=9))

_ROW_WITH_XBRL = """
<tr>
  <td class="evennew-L kjTime" nowrap="">13:00</td>
  <td class="evennew-M kjCode" nowrap="">52680</td>
  <td class="evennew-M kjName" nowrap="">テスト株式会社</td>
  <td align="left" class="evennew-M kjTitle">
    <a href="140120260907532276.pdf" target="_blank">2027年3月期第２四半期決算短信〔日本基準〕（連結）</a>
  </td>
  <td align="center" class="evennew-M kjXbrl" nowrap="">
    <div class="xbrl-mask"><div class="xbrl-button">
      <a class="style002" href="091220260907532276.zip">XBRL</a>
    </div></div>
  </td>
  <td align="left" class="evennew-M kjPlace" nowrap="">東</td>
  <td align="left" class="evennew-R kjHistroy"></td>
</tr>
"""

_ROW_WITHOUT_XBRL = """
<tr>
  <td class="oddnew-L kjTime" nowrap="">15:00</td>
  <td class="oddnew-M kjCode" nowrap="">23320</td>
  <td class="oddnew-M kjName" nowrap="">サンプル</td>
  <td align="left" class="oddnew-M kjTitle">
    <a href="140120260907532346.pdf" target="_blank">代表取締役の異動に関するお知らせ</a>
  </td>
  <td align="center" class="oddnew-M kjXbrl" nowrap=""></td>
  <td align="left" class="oddnew-M kjPlace" nowrap="">東</td>
  <td align="left" class="oddnew-R kjHistroy"></td>
</tr>
"""


def _row(html: str):
    return BeautifulSoup(html, "html.parser").find("tr")


def test_parse_disclosure_row_with_xbrl_extracts_all_fields():
    row = parse_disclosure_row(_row(_ROW_WITH_XBRL), "20260907")
    assert row is not None
    assert row.code == "5268"
    assert row.disclosed_at == dt.datetime(2026, 9, 7, 13, 0, tzinfo=_JST)
    assert row.title == "2027年3月期第２四半期決算短信〔日本基準〕（連結）"
    assert row.pdf_url == "https://www.release.tdnet.info/inbs/140120260907532276.pdf"
    assert row.has_xbrl is True
    assert row.xbrl_url == "https://www.release.tdnet.info/inbs/091220260907532276.zip"


def test_parse_disclosure_row_without_xbrl_still_captures_title():
    row = parse_disclosure_row(_row(_ROW_WITHOUT_XBRL), "20260907")
    assert row is not None
    assert row.code == "2332"
    assert row.title == "代表取締役の異動に関するお知らせ"
    assert row.has_xbrl is False
    assert row.xbrl_url is None


def test_parse_disclosure_row_returns_none_for_row_without_expected_cells():
    assert parse_disclosure_row(_row("<tr><td>header</td></tr>"), "20260907") is None


def test_parse_disclosure_rows_from_table_extracts_all_rows():
    table_html = f"<table id='main-list-table'>{_ROW_WITH_XBRL}{_ROW_WITHOUT_XBRL}</table>"
    table = BeautifulSoup(table_html, "html.parser").find("table")
    rows = parse_disclosure_rows(table, "20260907")
    assert len(rows) == 2
    assert {row.code for row in rows} == {"5268", "2332"}


def test_append_disclosures_to_csv_is_idempotent(tmp_path):
    row = parse_disclosure_row(_row(_ROW_WITHOUT_XBRL), "20260907")
    append_disclosures_to_csv([row], output_dir=tmp_path)
    append_disclosures_to_csv([row], output_dir=tmp_path)

    import polars as pl

    df = pl.read_csv(tmp_path / "2332.csv")
    assert df.height == 1
    assert df["category_code"].to_list() == ["T31"]


def test_append_disclosures_to_csv_classifies_and_groups_by_code(tmp_path):
    rows = [
        parse_disclosure_row(_row(_ROW_WITH_XBRL), "20260907"),
        parse_disclosure_row(_row(_ROW_WITHOUT_XBRL), "20260907"),
    ]
    append_disclosures_to_csv(rows, output_dir=tmp_path)

    import polars as pl

    assert (tmp_path / "5268.csv").exists()
    assert (tmp_path / "2332.csv").exists()
    df = pl.read_csv(tmp_path / "5268.csv")
    assert df["category_code"].to_list() == ["T2"]
    assert df["has_xbrl"].to_list() == [True]
