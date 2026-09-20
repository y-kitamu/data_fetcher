"""Readers for JPX-published statistics (data/jpx_stats)."""

import datetime
from pathlib import Path

import polars as pl

from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT

INVESTOR_TYPE_DIR = PROJECT_ROOT / "data" / "jpx_stats" / "investor_type"
MARGIN_DIR = PROJECT_ROOT / "data" / "jpx_stats" / "margin_daily_disclosure"


class JpxInvestorTypeReader(BaseReader):
    """Reader for data/jpx_stats/investor_type/{week_start}_{week_end}.csv.

    Market-wide weekly investor-type buy/sell stats; there is no ticker column,
    so this does not implement BaseReader's symbol-scoped contract.
    """

    SOURCE_NAME = "jpx_investor_type"

    def __init__(self, data_dir: Path = INVESTOR_TYPE_DIR):
        self.data_dir = data_dir

    def read(
        self,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
        market: str | None = None,
    ) -> pl.DataFrame:
        dfs = []
        for path in sorted(self.data_dir.glob("*.csv")):
            try:
                week_start_str, week_end_str = path.stem.split("_")
                week_start = datetime.datetime.strptime(week_start_str, "%Y%m%d").date()
                week_end = datetime.datetime.strptime(week_end_str, "%Y%m%d").date()
            except ValueError:
                continue
            if start_date is not None and week_end < start_date:
                continue
            if end_date is not None and week_start > end_date:
                continue
            dfs.append(pl.read_csv(path, try_parse_dates=True))

        if len(dfs) == 0:
            return pl.DataFrame()

        df = pl.concat(dfs, how="diagonal_relaxed").sort("week_start")
        if market is not None:
            df = df.filter(pl.col("market") == market)
        return df


class JpxMarginDisclosureReader(BaseReader):
    """Reader for data/jpx_stats/margin_daily_disclosure/{YYYYMMDD}.csv."""

    SOURCE_NAME = "jpx_margin_daily_disclosure"

    def __init__(self, data_dir: Path = MARGIN_DIR):
        self.data_dir = data_dir
        self._available_tickers: list[str] = []

    def _files(self) -> list[Path]:
        return sorted(self.data_dir.glob("*.csv"))

    def _read(self, path: Path) -> pl.DataFrame:
        return pl.read_csv(path, schema_overrides={"code": pl.Utf8}).with_columns(
            pl.col("code").str.slice(0, 4).alias("ticker")
        )

    @property
    def available_tickers(self) -> list[str]:
        if len(self._available_tickers) == 0:
            files = self._files()
            if len(files) == 0:
                return []
            df = self._read(files[-1])
            self._available_tickers = sorted(df["ticker"].unique().to_list())
        return self._available_tickers

    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        files = self._files()
        if len(files) == 0:
            return datetime.datetime(1970, 1, 1)
        return datetime.datetime.strptime(files[0].stem, "%Y%m%d")

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        files = self._files()
        if len(files) == 0:
            return datetime.datetime(1970, 1, 1)
        return datetime.datetime.strptime(files[-1].stem, "%Y%m%d")

    def read_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        dfs = []
        for path in self._files():
            file_date = datetime.datetime.strptime(path.stem, "%Y%m%d")
            if start_date.date() <= file_date.date() <= end_date.date():
                df = self._read(path).filter(pl.col("ticker") == symbol)
                if len(df) > 0:
                    dfs.append(df.with_columns(pl.lit(file_date).alias("datetime")))

        if len(dfs) == 0:
            return pl.DataFrame()

        return pl.concat(dfs, how="diagonal_relaxed").sort("datetime")
