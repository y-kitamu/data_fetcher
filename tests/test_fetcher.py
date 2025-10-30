"""Tests for fetcher module to validate logic before refactoring"""

import datetime

import pytest


def test_convert_str_to_datetime():
    """Test convert_str_to_datetime function"""
    from data_fetcher.fetcher import convert_str_to_datetime

    # Test 8-character date format (YYYYMMDD)
    result = convert_str_to_datetime("20240101")
    assert result == datetime.datetime(2024, 1, 1)

    # Test 14-character datetime format (YYYYMMDDHHMMSS)
    result = convert_str_to_datetime("20240101123045")
    assert result == datetime.datetime(2024, 1, 1, 12, 30, 45)

    # Test invalid format
    with pytest.raises(ValueError, match="Unknown date format"):
        convert_str_to_datetime("2024-01-01")


def test_convert_datetime_to_str():
    """Test convert_datetime_to_str function"""
    from data_fetcher.fetcher import convert_datetime_to_str

    dt = datetime.datetime(2024, 1, 1, 12, 30, 45)

    # Test with time included
    result = convert_datetime_to_str(dt, include_time=True)
    assert result == "20240101123045"

    # Test without time
    result = convert_datetime_to_str(dt, include_time=False)
    assert result == "20240101"


def test_get_available_sources():
    """Test get_available_sources function"""
    from data_fetcher.fetcher import get_available_sources

    sources = get_available_sources()
    assert isinstance(sources, list)
    assert "gmo" in sources
    assert "binance" in sources
    assert "histdata" in sources
    assert "kabutan" in sources


def test_get_fetcher():
    """Test get_fetcher function"""
    from data_fetcher.fetcher import get_fetcher

    # Test valid sources
    fetcher = get_fetcher("gmo")
    assert fetcher is not None

    # Test invalid source
    with pytest.raises(ValueError, match="Unknown source"):
        get_fetcher("invalid_source")
