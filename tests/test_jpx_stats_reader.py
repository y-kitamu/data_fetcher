"""Tests for JpxInvestorTypeReader / JpxMarginDisclosureReader / JpxArbitrage*Reader."""

import datetime

from data_fetcher.readers.jpx_stats import (
    JpxArbitrageByParticipantReader,
    JpxArbitrageStatusReader,
    JpxInvestorTypeReader,
    JpxMarginDisclosureReader,
)


def test_investor_type_reader_filters_by_week_range(tmp_path):
    d = tmp_path / "investor_type"
    d.mkdir()
    (d / "20160104_20160108.csv").write_text(
        "market,metric,week_start,week_end,category,item,value,ratio_pct\n"
        "TSE 1st,value,2016-01-04,2016-01-08,自己計,sell,100.0,10.0\n"
    )
    (d / "20160111_20160115.csv").write_text(
        "market,metric,week_start,week_end,category,item,value,ratio_pct\n"
        "TSE 1st,value,2016-01-11,2016-01-15,自己計,sell,200.0,20.0\n"
    )

    reader = JpxInvestorTypeReader(data_dir=d)

    df_all = reader.read()
    assert len(df_all) == 2

    df_ranged = reader.read(
        start_date=datetime.date(2016, 1, 10), end_date=datetime.date(2016, 1, 20)
    )
    assert len(df_ranged) == 1
    assert df_ranged["value"].to_list() == [200.0]

    df_market = reader.read(market="TSE 1st")
    assert len(df_market) == 2


def test_margin_disclosure_reader_normalizes_5digit_code(tmp_path):
    d = tmp_path / "margin_daily_disclosure"
    d.mkdir()
    header = "report_date,code,new_sec_code,sell_outstanding,buy_outstanding\n"
    (d / "20260828.csv").write_text(header + "2026-08-28,39070,JP3369550003,100,200\n")
    (d / "20260903.csv").write_text(header + "2026-09-03,39070,JP3369550003,150,250\n")

    reader = JpxMarginDisclosureReader(data_dir=d)

    assert reader.available_tickers == ["3907"]
    assert reader.get_earliest_date("3907") == datetime.datetime(2026, 8, 28)
    assert reader.get_latest_date("3907") == datetime.datetime(2026, 9, 3)

    df = reader.read_ticker("3907")
    assert df["sell_outstanding"].to_list() == [100, 150]

    assert len(reader.read_ticker("0000")) == 0


def test_arbitrage_status_reader_filters_by_date_range(tmp_path):
    d = tmp_path / "arbitrage_status"
    d.mkdir()
    header = "trade_date,sell_volume,buy_volume\n"
    (d / "20260916.csv").write_text(header + "2026-09-16,1000.0,2000.0\n")
    (d / "20260917.csv").write_text(header + "2026-09-17,4581.0,29096.0\n")

    reader = JpxArbitrageStatusReader(data_dir=d)

    df_all = reader.read()
    assert len(df_all) == 2

    df_ranged = reader.read(start_date=datetime.date(2026, 9, 17))
    assert len(df_ranged) == 1
    assert df_ranged["buy_volume"].to_list() == [29096.0]


def test_arbitrage_by_participant_reader_filters_by_date_and_broker(tmp_path):
    d = tmp_path / "arbitrage_by_participant"
    d.mkdir()
    header = "trade_date,rank,broker_name,sell_volume,buy_volume,total_volume\n"
    (d / "20260916.csv").write_text(
        header + "2026-09-16,1,ブローカーA,100.0,200.0,300.0\n"
    )
    (d / "20260917.csv").write_text(
        header
        + "2026-09-17,1,ブローカーA,0.0,22639.0,22639.0\n"
        + "2026-09-17,2,ブローカーB,4581.0,3059.0,7640.0\n"
    )

    reader = JpxArbitrageByParticipantReader(data_dir=d)

    df_all = reader.read()
    assert len(df_all) == 3

    df_ranged = reader.read(start_date=datetime.date(2026, 9, 17))
    assert len(df_ranged) == 2

    df_broker = reader.read(broker="ブローカーB")
    assert df_broker["total_volume"].to_list() == [7640.0]
