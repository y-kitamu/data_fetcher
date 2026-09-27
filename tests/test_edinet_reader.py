"""Tests for EdinetFinancialReader / EdinetLargeShareholdingReader."""

import datetime

from data_fetcher.readers.edinet import EdinetFinancialReader, EdinetLargeShareholdingReader

_SHAREHOLDING_HEADER = (
    "doc_id,issuer_name,issuer_sec_code,filing_date,holding_ratio,shares_held,holder_name\n"
)


def test_financial_reader_reads_tidy_csv(tmp_path):
    d = tmp_path / "csv"
    d.mkdir()
    (d / "1301.csv").write_text(
        "code,filing_date,element_id,value\n"
        "1301,2025-06-25,jppfs_cor:NetSales,302681000000.0\n"
        "1301,2015-06-24,jppfs_cor:NetSales,218350000000.0\n"
    )

    reader = EdinetFinancialReader(data_dir=d)
    assert reader.available_tickers == ["1301"]

    df = reader.read_financial("1301", start_date=datetime.datetime(2016, 1, 1))
    assert df["code"].to_list() == ["1301"]
    assert df["filing_date"].to_list() == [datetime.date(2025, 6, 25)]
    assert df["concept"].to_list() == ["net_sales"]

    assert len(reader.read_financial("9999")) == 0


def test_large_shareholding_reader_filters_by_symbol_and_date(tmp_path):
    d = tmp_path / "large_shareholding"
    d.mkdir()
    (d / "20210902.csv").write_text(
        _SHAREHOLDING_HEADER + "S100MD5R,Foo,6890,2021-09-02,0.0674,2700900,Bar\n"
    )
    (d / "20210910.csv").write_text(
        _SHAREHOLDING_HEADER + "S100XX,Baz,1301,2021-09-10,0.05,100,Qux\n"
    )

    reader = EdinetLargeShareholdingReader(data_dir=d)

    df_all = reader.read()
    assert len(df_all) == 2

    df_symbol = reader.read(symbol="6890")
    assert len(df_symbol) == 1

    df_ranged = reader.read(start_date=datetime.date(2021, 9, 5), end_date=datetime.date(2021, 9, 30))
    assert len(df_ranged) == 1
    assert df_ranged["issuer_sec_code"].to_list() == ["1301"]
