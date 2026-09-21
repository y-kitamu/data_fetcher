"""Readers for taisyaku.jp margin/short-selling balance data."""

import datetime
from collections.abc import Sequence
from pathlib import Path

import polars as pl

from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT

HISTORY_DIR = PROJECT_ROOT / "data" / "taisyaku" / "history"
ZANDAKA_DIR = PROJECT_ROOT / "data" / "taisyaku" / "zandaka"

# Same ticker code can appear once per listed exchange (e.g. 7203 lists on
# both 東証 and 名証); default to 東証 only so callers don't silently double-count.
DEFAULT_MARKET = "東証"


class _TaisyakuCsvReader(BaseReader):
    """Shared per-date CSV reading logic for taisyaku/history and taisyaku/zandaka.

    Both stores are one CSV per business day with a 銘柄コード column; only the
    filename glob pattern, date-parsing prefix, and market-column name differ
    between the two.
    """

    _GLOB: str
    _DATE_LEN: int = 8
    _MARKET_COLUMN: str

    #: Marks this class to the gateway as accepting read_ticker(..., market=...).
    SUPPORTS_MARKET_FILTER = True

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self._available_tickers: list[str] = []

    def _filter_market(
        self, df: pl.DataFrame, market: str | Sequence[str] | None
    ) -> pl.DataFrame:
        """Filter rows by exchange (東証/名証/...); None means no filtering.

        Values in _MARKET_COLUMN sometimes carry a suffix (e.g. taisyaku's
        "東証およびＰＴＳ"), so match by prefix rather than exact equality.
        """
        if market is None:
            return df
        markets = (market,) if isinstance(market, str) else tuple(market)
        return df.filter(
            pl.any_horizontal(
                [pl.col(self._MARKET_COLUMN).str.starts_with(m) for m in markets]
            )
        )

    def _file_date(self, path: Path) -> datetime.date:
        return datetime.datetime.strptime(path.stem[: self._DATE_LEN], "%Y%m%d").date()

    def _files(self) -> list[Path]:
        return sorted(self.data_dir.glob(self._GLOB))

    @property
    def available_tickers(self) -> list[str]:
        if len(self._available_tickers) == 0:
            files = self._files()
            if len(files) == 0:
                return []
            latest = files[-1]
            df = pl.read_csv(latest, schema_overrides={"銘柄コード": pl.Utf8})
            self._available_tickers = sorted(df["銘柄コード"].unique().to_list())
        return self._available_tickers

    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        files = self._files()
        if len(files) == 0:
            return datetime.datetime(1970, 1, 1)
        return datetime.datetime.combine(self._file_date(files[0]), datetime.time.min)

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        files = self._files()
        if len(files) == 0:
            return datetime.datetime(1970, 1, 1)
        return datetime.datetime.combine(self._file_date(files[-1]), datetime.time.min)

    def read_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
        market: str | Sequence[str] | None = DEFAULT_MARKET,
    ) -> pl.DataFrame:
        dfs = []
        for path in self._files():
            file_date = self._file_date(path)
            if start_date.date() <= file_date <= end_date.date():
                df = pl.read_csv(path, schema_overrides={"銘柄コード": pl.Utf8})
                df = df.filter(pl.col("銘柄コード") == symbol)
                df = self._filter_market(df, market)
                if len(df) > 0:
                    dfs.append(
                        df.with_columns(
                            pl.lit(
                                datetime.datetime.combine(file_date, datetime.time.min)
                            ).alias("datetime")
                        )
                    )

        if len(dfs) == 0:
            return pl.DataFrame()

        return pl.concat(dfs, how="diagonal_relaxed").sort("datetime")


class TaisyakuHistoryReader(_TaisyakuCsvReader):
    """Reader for data/taisyaku/history/{YYYYMMDD}.csv (whole-market margin/short
    balance history)."""

    SOURCE_NAME = "taisyaku_history"
    _GLOB = "*.csv"
    _MARKET_COLUMN = "市場区分"

    def __init__(self, data_dir: Path = HISTORY_DIR):
        super().__init__(data_dir)


class TaisyakuZandakaReader(_TaisyakuCsvReader):
    """Reader for data/taisyaku/zandaka/{YYYYMMDD}_kakuho.csv (margin balance
    disclosure)."""

    SOURCE_NAME = "taisyaku_zandaka"
    _GLOB = "*_kakuho.csv"
    _MARKET_COLUMN = "取引所区分名"

    def __init__(self, data_dir: Path = ZANDAKA_DIR):
        super().__init__(data_dir)
