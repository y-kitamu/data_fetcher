"""Tests for EdinetFinancialReader / EdinetLargeShareholdingReader."""

import datetime

from data_fetcher.readers.edinet import EdinetFinancialReader, EdinetLargeShareholdingReader

_FINANCIAL_HEADER = "key,start_date,end_date,edinet_key,period,value,announce_date\n"
_SHAREHOLDING_HEADER = (
    "doc_id,issuer_name,issuer_sec_code,filing_date,holding_ratio,shares_held,holder_name\n"
)


def test_financial_reader_truncates_stem_for_available_tickers(tmp_path):
    d = tmp_path / "financial"
    d.mkdir()
    (d / "13010.csv").write_text(
        _FINANCIAL_HEADER + "net_sales,2010-04-01,2011-03-31,jpcrp_cor:X,P,162731000000,201506241620\n"
    )

    reader = EdinetFinancialReader(data_dir=d)
    assert reader.available_tickers == ["1301"]

    df = reader.read_financial("1301")
    assert df["value"].to_list() == [162731000000]
    assert df["announce_date"].to_list() == [datetime.datetime(2015, 6, 24, 16, 20)]

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
