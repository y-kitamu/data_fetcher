"""Tests for base_fetcher module to validate logic before refactoring"""

import datetime

import polars as pl
import pytest


def test_convert_timedelta_to_str():
    """Test convert_timedelta_to_str function"""
    from data_fetcher.core.base_fetcher import convert_timedelta_to_str

    # Test seconds
    assert convert_timedelta_to_str(datetime.timedelta(seconds=30)) == "30s"

    # Test minutes
    assert convert_timedelta_to_str(datetime.timedelta(minutes=5)) == "5m"

    # Test hours
    assert convert_timedelta_to_str(datetime.timedelta(hours=2)) == "2h"

    # Test days
    assert convert_timedelta_to_str(datetime.timedelta(days=3)) == "3d"

    # Test combinations
    assert (
        convert_timedelta_to_str(datetime.timedelta(days=1, hours=2, minutes=30))
        == "1d2h30m"
    )
    assert convert_timedelta_to_str(datetime.timedelta(hours=1, seconds=15)) == "1h15s"


def test_convert_str_to_timedelta():
    """Test convert_str_to_timedelta function"""
    from data_fetcher.core.base_fetcher import convert_str_to_timedelta

    # Test seconds
    assert convert_str_to_timedelta("30s") == datetime.timedelta(seconds=30)

    # Test minutes
    assert convert_str_to_timedelta("5m") == datetime.timedelta(minutes=5)

    # Test hours
    assert convert_str_to_timedelta("2h") == datetime.timedelta(hours=2)

    # Test days
    assert convert_str_to_timedelta("3d") == datetime.timedelta(days=3)

    # Test weeks
    assert convert_str_to_timedelta("1w") == datetime.timedelta(weeks=1)

    # Test invalid input
    with pytest.raises(ValueError, match="Unknown interval"):
        convert_str_to_timedelta("5x")


def test_convert_tick_to_ohlc():
    """Test convert_tick_to_ohlc function"""
    from data_fetcher.core.base_fetcher import convert_tick_to_ohlc

    # Create sample tick data
    tick_data = {
        "datetime": [
            datetime.datetime(2024, 1, 1, 0, 0, 0),
            datetime.datetime(2024, 1, 1, 0, 0, 10),
            datetime.datetime(2024, 1, 1, 0, 0, 20),
            datetime.datetime(2024, 1, 1, 0, 1, 0),
            datetime.datetime(2024, 1, 1, 0, 1, 10),
        ],
        "price": [100.0, 101.0, 99.0, 102.0, 103.0],
        "size": [1.0, 2.0, 1.5, 3.0, 2.5],
    }
    tick_df = pl.DataFrame(tick_data)

    # Test 1-minute interval
    result = convert_tick_to_ohlc(tick_df, datetime.timedelta(minutes=1))

    assert len(result) == 2  # Should have 2 bars
    assert result["open"][0] == 100.0  # First bar open
    assert result["high"][0] == 101.0  # First bar high
    assert result["low"][0] == 99.0  # First bar low
    assert result["close"][0] == 99.0  # First bar close
    assert result["volume"][0] == 4.5  # First bar volume

    assert result["open"][1] == 102.0  # Second bar open
    assert result["close"][1] == 103.0  # Second bar close
    assert result["volume"][1] == 5.5  # Second bar volume
