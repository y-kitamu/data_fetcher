"""Unified read-access gateway across all data sources.

Callers ask for a *kind* of data (OHLC, tick, financials, ...) for a symbol
and get it back without needing to know which underlying Reader/source
actually holds it. Every ``get_*`` function always returns
``dict[str, pl.DataFrame]`` — keyed by source name — never a bare
``pl.DataFrame``, so the return type never depends on how many sources
happened to have data:

1. resolves every registered reader that can serve the symbol (by checking
   ``reader.available_tickers``), in priority order per kind, unless the
   caller passes an explicit ``source=`` override (which narrows the result
   to that one source's entry),
2. calls each matching reader's existing read method,
3. tags each DataFrame with a ``source`` column too, so provenance survives
   even if a caller later concatenates entries together.

Adding a new source for an existing kind is: write one Reader-shaped class
(see src/data_fetcher/readers/*.py) and append it to the right list in
``_CATALOG`` below. No ``get_*`` function needs to change.
"""

import datetime
from collections.abc import Sequence

import polars as pl

from .core.base_reader import BaseReader
from .readers import (
    BinanceReader,
    BitflyerBookReader,
    BitflyerReader,
    EdinetFinancialReader,
    EdinetLargeShareholdingReader,
    GMOBookReader,
    GMOReader,
    GoogleTrendsReader,
    HistDataReader,
    JpNewsReader,
    JpTickerThemesReader,
    JpxArbitrageByParticipantReader,
    JpxArbitrageStatusReader,
    JpxInvestorTypeReader,
    JpxMarginDisclosureReader,
    KabutanReader,
    KabuTickReader,
    SBIReader,
    TaisyakuHistoryReader,
    TaisyakuZandakaReader,
    TdnetReader,
    YFinanceFinancialReader,
    YFinanceReader,
)

# kind -> ordered list of reader classes that can serve it, highest priority first.
_CATALOG: dict[str, list[type[BaseReader]]] = {
    "ohlc": [KabutanReader, YFinanceReader],
    "tick": [
        BinanceReader,
        GMOReader,
        BitflyerReader,
        SBIReader,
        HistDataReader,
        KabuTickReader,
    ],
    "order_book": [GMOBookReader, BitflyerBookReader],
    "financials": [
        KabutanReader,
        EdinetFinancialReader,
        TdnetReader,
        YFinanceFinancialReader,
    ],
    "margin_balance": [
        TaisyakuHistoryReader,
        TaisyakuZandakaReader,
        JpxMarginDisclosureReader,
    ],
    "search_trend": [GoogleTrendsReader],
}

# Market-wide / no-symbol kinds: a single reader each, exposing a plain
# `.read(...)` method instead of the symbol-scoped BaseReader contract.
_investor_type_reader = JpxInvestorTypeReader()
_large_shareholding_reader = EdinetLargeShareholdingReader()
_ticker_themes_reader = JpTickerThemesReader()
_news_reader = JpNewsReader()
_arbitrage_status_reader = JpxArbitrageStatusReader()
_arbitrage_by_participant_reader = JpxArbitrageByParticipantReader()

KINDS: list[str] = sorted(
    [
        *_CATALOG.keys(),
        "investor_flow",
        "large_shareholding",
        "ticker_theme",
        "news",
        "arbitrage_status",
        "arbitrage_by_participant",
    ]
)

_instances: dict[type, BaseReader] = {}


def _get_instance(reader_cls: type[BaseReader]) -> BaseReader:
    """Lazily construct and memoize a reader instance.

    Several readers do non-trivial I/O in __init__ (e.g. GMOReader globs its
    whole data directory), so the catalog stores classes, not instances, and
    only pays that cost the first time a kind is actually queried.
    """
    if reader_cls not in _instances:
        _instances[reader_cls] = reader_cls()
    return _instances[reader_cls]


def _attach_source(df: pl.DataFrame, reader: BaseReader) -> pl.DataFrame:
    if len(df) == 0:
        return df
    return df.with_columns(pl.lit(reader.SOURCE_NAME).alias("source"))


def list_sources(kind: str) -> list[str]:
    """List the source names registered for a given kind, in priority order."""
    if kind not in _CATALOG:
        raise ValueError(f"Unknown kind={kind!r}. Available: {KINDS}")
    return [reader_cls.SOURCE_NAME for reader_cls in _CATALOG[kind]]


def _resolve_by_source(kind: str, source: str) -> BaseReader:
    for reader_cls in _CATALOG[kind]:
        if reader_cls.SOURCE_NAME == source:
            return _get_instance(reader_cls)
    raise ValueError(
        f"Unknown source={source!r} for kind={kind!r}. Available: {list_sources(kind)}"
    )


def _resolve_symbol_readers(
    kind: str, symbol: str, source: str | None
) -> list[BaseReader]:
    """Return every reader in `kind` that has `symbol`, in priority order.

    If `source` is given, narrows to that one reader (still returned as a
    single-item list) and raises if it doesn't actually have the symbol.
    """
    if kind not in _CATALOG:
        raise ValueError(f"Unknown kind={kind!r}. Available: {KINDS}")

    if source is not None:
        reader = _resolve_by_source(kind, source)
        if symbol not in reader.available_tickers:
            raise ValueError(
                f"{symbol!r} not found in source={source!r} for kind={kind!r}"
            )
        return [reader]

    matched = [
        _get_instance(reader_cls)
        for reader_cls in _CATALOG[kind]
        if symbol in _get_instance(reader_cls).available_tickers
    ]
    if len(matched) == 0:
        raise ValueError(
            f"No reader in kind={kind!r} has data for symbol={symbol!r}. "
            f"Tried: {list_sources(kind)}"
        )
    return matched


def get_ohlc(
    symbol: str,
    interval: datetime.timedelta,
    start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
    end_date: datetime.datetime = datetime.datetime.now(),
    fill_missing_date: bool = False,
    read_interval: datetime.timedelta | None = None,
    source: str | None = None,
) -> dict[str, pl.DataFrame]:
    readers = _resolve_symbol_readers("ohlc", symbol, source)
    return {
        reader.SOURCE_NAME: _attach_source(
            reader.read_ohlc(
                symbol,
                interval,
                start_date=start_date,
                end_date=end_date,
                fill_missing_date=fill_missing_date,
                read_interval=read_interval,
            ),
            reader,
        )
        for reader in readers
    }


def get_tick(
    symbol: str,
    start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
    end_date: datetime.datetime = datetime.datetime.now(),
    timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    source: str | None = None,
) -> dict[str, pl.DataFrame]:
    readers = _resolve_symbol_readers("tick", symbol, source)
    return {
        reader.SOURCE_NAME: _attach_source(
            reader.read_ticker(symbol, start_date, end_date, timezone_delta), reader
        )
        for reader in readers
    }


def get_order_book(
    symbol: str,
    start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
    end_date: datetime.datetime = datetime.datetime.now(),
    timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    source: str | None = None,
) -> dict[str, pl.DataFrame]:
    readers = _resolve_symbol_readers("order_book", symbol, source)
    return {
        reader.SOURCE_NAME: _attach_source(
            reader.read_ticker(symbol, start_date, end_date, timezone_delta), reader
        )
        for reader in readers
    }


def get_financials(
    symbol: str,
    start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
    end_date: datetime.datetime = datetime.datetime.now(),
    source: str | None = None,
) -> dict[str, pl.DataFrame]:
    readers = _resolve_symbol_readers("financials", symbol, source)
    return {
        reader.SOURCE_NAME: _attach_source(
            reader.read_financial(symbol, start_date=start_date, end_date=end_date),
            reader,
        )
        for reader in readers
    }


def get_margin_balance(
    symbol: str,
    start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
    end_date: datetime.datetime = datetime.datetime.now(),
    source: str | None = None,
    market: str | Sequence[str] | None = None,
) -> dict[str, pl.DataFrame]:
    readers = _resolve_symbol_readers("margin_balance", symbol, source)
    result = {}
    for reader in readers:
        # Only taisyaku readers know about exchange (東証/名証/...); others
        # (e.g. JpxMarginDisclosureReader) have no such concept, so market is
        # forwarded only when the reader opts in and the caller asked for it.
        kwargs = {}
        if market is not None and getattr(reader, "SUPPORTS_MARKET_FILTER", False):
            kwargs["market"] = market
        result[reader.SOURCE_NAME] = _attach_source(
            reader.read_ticker(symbol, start_date, end_date, **kwargs), reader
        )
    return result


def get_search_trend(
    symbol: str,
    start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
    end_date: datetime.datetime = datetime.datetime.now(),
    source: str | None = None,
) -> dict[str, pl.DataFrame]:
    readers = _resolve_symbol_readers("search_trend", symbol, source)
    return {
        reader.SOURCE_NAME: _attach_source(
            reader.read_ticker(symbol, start_date, end_date), reader
        )
        for reader in readers
    }


def get_investor_flow(
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    market: str | None = None,
) -> dict[str, pl.DataFrame]:
    df = _investor_type_reader.read(
        start_date=start_date, end_date=end_date, market=market
    )
    return {
        _investor_type_reader.SOURCE_NAME: _attach_source(df, _investor_type_reader)
    }


def get_arbitrage_status(
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> dict[str, pl.DataFrame]:
    df = _arbitrage_status_reader.read(start_date=start_date, end_date=end_date)
    return {
        _arbitrage_status_reader.SOURCE_NAME: _attach_source(df, _arbitrage_status_reader)
    }


def get_arbitrage_by_participant(
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    broker: str | None = None,
) -> dict[str, pl.DataFrame]:
    df = _arbitrage_by_participant_reader.read(
        start_date=start_date, end_date=end_date, broker=broker
    )
    return {
        _arbitrage_by_participant_reader.SOURCE_NAME: _attach_source(
            df, _arbitrage_by_participant_reader
        )
    }


def get_large_shareholding(
    symbol: str | None = None,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> dict[str, pl.DataFrame]:
    df = _large_shareholding_reader.read(
        symbol=symbol, start_date=start_date, end_date=end_date
    )
    return {
        _large_shareholding_reader.SOURCE_NAME: _attach_source(
            df, _large_shareholding_reader
        )
    }


def get_ticker_themes(
    ticker: str | None = None, as_of: datetime.date | None = None
) -> dict[str, pl.DataFrame]:
    df = _ticker_themes_reader.read(ticker=ticker, as_of=as_of)
    return {
        _ticker_themes_reader.SOURCE_NAME: _attach_source(df, _ticker_themes_reader)
    }


def get_news(
    symbols: list[str] | None = None,
    sources: list[str] | None = None,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
) -> dict[str, pl.DataFrame]:
    kwargs = {"start_date": start_date, "end_date": end_date, "symbols": symbols}
    if sources is not None:
        kwargs["sources"] = sources
    df = _news_reader.read(**kwargs)
    # JpNewsReader already emits a per-row `source` column (kabutan/gnews/
    # yfinance); split on it so get_news matches every other get_* function's
    # dict[str, DataFrame] contract instead of returning a bare DataFrame.
    if len(df) == 0:
        return {}
    return {
        name: df.filter(pl.col("source") == name)
        for name in df["source"].unique().sort().to_list()
    }
