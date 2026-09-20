"""Tests for the unified data gateway (src/data_fetcher/gateway.py)."""

import datetime

import polars as pl
import pytest

from data_fetcher import gateway
from data_fetcher.core.base_reader import BaseReader
from data_fetcher.core.constants import PROJECT_ROOT

_DATA_DIR = PROJECT_ROOT / "data"


def _has_real_data(*parts) -> bool:
    path = _DATA_DIR
    for p in parts:
        path = path / p
    return path.exists() and any(path.iterdir())


class _FakeTickReaderA(BaseReader):
    SOURCE_NAME = "fake_a"

    @property
    def available_tickers(self):
        return ["DUP"]

    def read_ticker(self, symbol, start_date=None, end_date=None, timezone_delta=None):
        return pl.DataFrame({"price": [1.0]})


class _FakeTickReaderB(BaseReader):
    SOURCE_NAME = "fake_b"

    @property
    def available_tickers(self):
        return ["DUP"]

    def read_ticker(self, symbol, start_date=None, end_date=None, timezone_delta=None):
        return pl.DataFrame({"price": [2.0]})


class _FakeTickReaderOnlyOther(BaseReader):
    SOURCE_NAME = "fake_c"

    @property
    def available_tickers(self):
        return ["OTHER"]

    def read_ticker(self, symbol, start_date=None, end_date=None, timezone_delta=None):
        return pl.DataFrame({"price": [3.0]})


@pytest.fixture
def dup_tick_catalog(monkeypatch):
    monkeypatch.setitem(
        gateway._CATALOG,
        "tick",
        [_FakeTickReaderA, _FakeTickReaderB, _FakeTickReaderOnlyOther],
    )
    yield


def test_catalog_source_names_are_unique_and_set_per_kind():
    for kind, reader_classes in gateway._CATALOG.items():
        names = [cls.SOURCE_NAME for cls in reader_classes]
        assert all(name is not None for name in names), f"{kind} has an unnamed reader"
        assert len(names) == len(set(names)), f"{kind} has duplicate SOURCE_NAME: {names}"


def test_list_sources_matches_catalog():
    for kind in gateway._CATALOG:
        assert gateway.list_sources(kind) == [
            cls.SOURCE_NAME for cls in gateway._CATALOG[kind]
        ]


def test_list_sources_unknown_kind_raises():
    with pytest.raises(ValueError, match="Unknown kind"):
        gateway.list_sources("no_such_kind")


def test_unknown_symbol_raises_with_tried_sources():
    with pytest.raises(ValueError, match="Tried:"):
        gateway.get_ohlc("NO_SUCH_SYMBOL_XYZ", datetime.timedelta(days=1))


def test_explicit_source_override_bad_symbol_raises():
    with pytest.raises(ValueError, match="not found in source"):
        gateway.get_tick("NO_SUCH_SYMBOL_XYZ", source="binance")


def test_explicit_unknown_source_raises():
    with pytest.raises(ValueError, match="Unknown source"):
        gateway.get_tick("BTCUSDT", source="not_a_real_source")


def test_get_tick_returns_dict_with_one_entry_per_matching_source(dup_tick_catalog):
    result = gateway.get_tick("DUP")
    assert set(result.keys()) == {"fake_a", "fake_b"}
    assert result["fake_a"]["price"].to_list() == [1.0]
    assert result["fake_b"]["price"].to_list() == [2.0]
    assert result["fake_a"]["source"].to_list() == ["fake_a"]


def test_get_tick_returns_single_entry_dict_when_only_one_source_matches(dup_tick_catalog):
    result = gateway.get_tick("OTHER")
    assert set(result.keys()) == {"fake_c"}


def test_get_tick_with_explicit_source_still_returns_dict(dup_tick_catalog):
    result = gateway.get_tick("DUP", source="fake_b")
    assert set(result.keys()) == {"fake_b"}
    assert result["fake_b"]["price"].to_list() == [2.0]


def test_market_wide_functions_return_single_entry_dict():
    result = gateway.get_ticker_themes(ticker="NO_SUCH_TICKER")
    assert set(result.keys()) == {"jp_ticker_themes"}


@pytest.mark.skipif(not _has_real_data("kabutan", "daily"), reason="requires fetched kabutan data")
def test_get_ohlc_resolves_to_kabutan_for_jp_stock():
    result = gateway.get_ohlc("1301", datetime.timedelta(days=1))
    assert set(result.keys()) == {"kabutan"}
    df = result["kabutan"]
    assert len(df) > 0
    assert df["source"].unique().to_list() == ["kabutan"]


@pytest.mark.skipif(not _has_real_data("binance"), reason="requires fetched binance data")
def test_get_tick_resolves_to_binance_for_crypto_pair():
    result = gateway.get_tick(
        "BTCUSDT",
        start_date=datetime.datetime(2024, 1, 1),
        end_date=datetime.datetime(2024, 1, 2),
    )
    assert "binance" in result


@pytest.mark.skipif(not _has_real_data("kabutan", "daily"), reason="requires fetched kabutan data")
def test_get_financials_attaches_source_column():
    result = gateway.get_financials("1301")
    for name, df in result.items():
        if len(df) > 0:
            assert df["source"].unique().to_list() == [name]


@pytest.mark.skipif(not _has_real_data("jp_ticker_themes"), reason="requires fetched theme data")
def test_get_ticker_themes_returns_latest_snapshot():
    result = gateway.get_ticker_themes(ticker="1301")
    df = result["jp_ticker_themes"]
    if len(df) > 0:
        assert df["source"].unique().to_list() == ["jp_ticker_themes"]


@pytest.mark.skipif(not _has_real_data("news"), reason="requires fetched news data")
def test_get_news_splits_by_source():
    result = gateway.get_news()
    for name, df in result.items():
        if len(df) > 0:
            assert df["source"].unique().to_list() == [name]
