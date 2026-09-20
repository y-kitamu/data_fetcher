"""Tests for TdnetReader.

Note: the real data/tdnet/csv store is stale (see readers/tdnet.py docstring),
so these tests use fixture files and do not assert anything about freshness.
"""

import datetime

from data_fetcher.readers.tdnet import TdnetReader

_HEADER = "filing_date,code,net_sales\n"


def test_read_financial_filters_by_date(tmp_path):
    d = tmp_path / "csv"
    d.mkdir()
    (d / "1301.csv").write_text(
        _HEADER + "2015-05-08,13010,218350000000\n2015-08-03,13010,220000000000\n"
    )

    reader = TdnetReader(data_dir=d)
    assert reader.available_tickers == ["1301"]

    df = reader.read_financial("1301")
    assert df["net_sales"].to_list() == [218350000000, 220000000000]

    df_ranged = reader.read_financial(
        "1301", start_date=datetime.datetime(2015, 6, 1), end_date=datetime.datetime(2015, 12, 31)
    )
    assert len(df_ranged) == 1


def test_read_financial_missing_symbol_returns_empty(tmp_path):
    d = tmp_path / "csv"
    d.mkdir()
    reader = TdnetReader(data_dir=d)
    assert len(reader.read_financial("9999")) == 0
