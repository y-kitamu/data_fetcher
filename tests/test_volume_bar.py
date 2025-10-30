"""Tests for volume_bar module to validate logic before refactoring"""

import datetime

import polars as pl
import pytest


def test_convert_ticker_to_volume_bar():
    """Test convert_ticker_to_volume_bar function"""
    from data_fetcher.volume_bar import convert_ticker_to_volume_bar

    # Create sample ticker data
    ticker_data = {
        "datetime": [
            datetime.datetime(2024, 1, 1, 0, 0, 0),
            datetime.datetime(2024, 1, 1, 0, 0, 10),
            datetime.datetime(2024, 1, 1, 0, 0, 20),
            datetime.datetime(2024, 1, 1, 0, 0, 30),
            datetime.datetime(2024, 1, 1, 0, 0, 40),
        ],
        "price": [100.0, 101.0, 99.0, 102.0, 103.0],
        "size": [5.0, 3.0, 2.0, 6.0, 4.0],  # Total: 20.0
    }
    ticker_df = pl.DataFrame(ticker_data)

    # Test with volume_size = 10.0
    result = convert_ticker_to_volume_bar(ticker_df, volume_size=10.0)

    assert len(result) > 0
    assert "open" in result.columns
    assert "high" in result.columns
    assert "low" in result.columns
    assert "close" in result.columns
    assert "volume" in result.columns
    assert "start_date" in result.columns
    assert "end_date" in result.columns

    # Check that volumes are close to target size
    for vol in result["volume"]:
        assert vol > 0
        # Volume should be close to 10.0 or the remainder
        assert vol <= 10.0 * 1.1  # Allow 10% margin


def test_convert_ticker_to_volume_bar_with_last_row():
    """Test convert_ticker_to_volume_bar with last_row parameter"""
    from data_fetcher.volume_bar import convert_ticker_to_volume_bar

    # Create sample ticker data
    ticker_data = {
        "datetime": [
            datetime.datetime(2024, 1, 1, 0, 0, 10),
            datetime.datetime(2024, 1, 1, 0, 0, 20),
        ],
        "price": [101.0, 102.0],
        "size": [3.0, 2.0],
    }
    ticker_df = pl.DataFrame(ticker_data)

    # Create a last_row representing an incomplete bar
    # Format: [open, high, low, close, volume, start_date, end_date]
    last_row = [
        100.0,  # open
        100.0,  # high
        100.0,  # low
        100.0,  # close (not used)
        5.0,  # volume
        datetime.datetime(2024, 1, 1, 0, 0, 0),  # start_date
        datetime.datetime(2024, 1, 1, 0, 0, 0),  # end_date (not used)
    ]

    result = convert_ticker_to_volume_bar(ticker_df, volume_size=10.0, last_row=last_row)

    assert len(result) > 0
    # First bar should start with the price from last_row
    assert result["open"][0] == 100.0
