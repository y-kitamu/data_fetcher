"""Tests for EstatReader and domains.estat.api pure functions."""

import datetime

import pytest

from data_fetcher.domains.estat import api as estat_api
from data_fetcher.readers.estat import EstatReader


def test_time_label_to_date_year_month():
    assert estat_api.time_label_to_date("2026年7月") == "2026-07-01"
    assert estat_api.time_label_to_date("2020年12月") == "2020-12-01"


def test_time_label_to_date_year_only():
    assert estat_api.time_label_to_date("2024年") == "2024-01-01"


def test_time_label_to_date_invalid_raises():
    with pytest.raises(ValueError):
        estat_api.time_label_to_date("not a label")


def test_assert_single_series_passes_for_uniform_dimension_values():
    values = [
        {"@tab": "100", "@cat01": "100", "@time": "2026000101"},
        {"@tab": "100", "@cat01": "100", "@time": "2026000202"},
    ]
    estat_api.assert_single_series(values, ["@tab", "@cat01"])  # should not raise


def test_assert_single_series_raises_when_multiple_series_mixed():
    values = [
        {"@tab": "100", "@cat01": "100", "@time": "2026000101"},
        {"@tab": "100", "@cat01": "110", "@time": "2026000101"},
    ]
    with pytest.raises(ValueError, match="複数系列が混在"):
        estat_api.assert_single_series(values, ["@tab", "@cat01"])


def test_build_time_label_map_extracts_time_dimension():
    class_inf = [
        {"@id": "cat01", "CLASS": [{"@code": "100", "@name": "合計"}]},
        {
            "@id": "time",
            "CLASS": [
                {"@code": "2026000101", "@name": "2026年1月"},
                {"@code": "2026000202", "@name": "2026年2月"},
            ],
        },
    ]
    label_map = estat_api.build_time_label_map(class_inf)
    assert label_map == {"2026000101": "2026年1月", "2026000202": "2026年2月"}


_HEADER = "date,value,fetched_at\n"


def test_reader_available_tickers_and_read_ticker_filters_by_date(tmp_path):
    d = tmp_path / "estat"
    d.mkdir()
    (d / "cpi_national_all_items.csv").write_text(
        _HEADER + "2026-07-01,114.2,2026-09-26\n" + "2026-08-01,114.3,2026-09-26\n"
    )

    reader = EstatReader(data_dir=d)
    assert reader.available_tickers == ["cpi_national_all_items"]

    df = reader.read_ticker(
        "cpi_national_all_items",
        start_date=datetime.datetime(2026, 8, 1),
        end_date=datetime.datetime(2026, 8, 31),
    )
    assert df["value"].to_list() == [114.3]
    assert reader.get_earliest_date("cpi_national_all_items") == datetime.datetime(
        2026, 7, 1
    )
    assert reader.get_latest_date("cpi_national_all_items") == datetime.datetime(
        2026, 8, 1
    )


def test_reader_read_ticker_keeps_revision_history(tmp_path):
    d = tmp_path / "estat"
    d.mkdir()
    (d / "cpi_national_all_items.csv").write_text(
        _HEADER + "2026-07-01,114.2,2026-08-01\n" + "2026-07-01,114.5,2026-09-26\n"
    )

    reader = EstatReader(data_dir=d)
    df = reader.read_ticker("cpi_national_all_items")
    assert df.height == 2
    assert df["value"].to_list() == [114.2, 114.5]


def test_reader_missing_symbol_returns_empty(tmp_path):
    d = tmp_path / "estat"
    d.mkdir()
    reader = EstatReader(data_dir=d)
    assert len(reader.read_ticker("NOT_EXIST")) == 0
