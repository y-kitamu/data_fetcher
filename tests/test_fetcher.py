"""Tests for fetchers module to validate logic after refactoring"""

import pytest


def test_get_available_sources():
    """Test get_available_sources function"""
    from data_fetcher.fetchers import get_available_sources

    sources = get_available_sources()
    assert isinstance(sources, list)
    assert "gmo" in sources
    assert "binance" in sources
    assert "histdata" in sources
    assert "kabutan" in sources
    assert "rakuten" in sources
    assert "bitflyer" in sources


def test_get_fetcher():
    """Test get_fetcher function"""
    from data_fetcher.fetchers import get_fetcher

    # Test valid sources
    fetcher = get_fetcher("gmo")
    assert fetcher is not None

    fetcher = get_fetcher("binance")
    assert fetcher is not None

    fetcher = get_fetcher("histdata")
    assert fetcher is not None

    # Test invalid source
    with pytest.raises(ValueError, match="Unknown source"):
        get_fetcher("invalid_source")
