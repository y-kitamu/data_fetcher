"""Reader for JP ticker theme-tag snapshots (data/jp_ticker_themes)."""

import ast
import datetime
from pathlib import Path

import polars as pl

from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT

DATA_DIR = PROJECT_ROOT / "data" / "jp_ticker_themes"


class JpTickerThemesReader(BaseReader):
    """Reader for data/jp_ticker_themes/{YYYYMMDD}.csv.

    Each file is a full daily snapshot of every JP ticker's theme tags, not a
    time series per ticker, so the natural query is "latest snapshot" (or
    latest as-of a given date) rather than a date-ranged read_ticker.
    """

    SOURCE_NAME = "jp_ticker_themes"

    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = data_dir

    def _files(self) -> list[Path]:
        return sorted(self.data_dir.glob("*.csv"))

    def _latest_file(self, as_of: datetime.date | None) -> Path | None:
        files = self._files()
        if as_of is not None:
            files = [
                f
                for f in files
                if datetime.datetime.strptime(f.stem, "%Y%m%d").date() <= as_of
            ]
        return files[-1] if files else None

    @property
    def available_tickers(self) -> list[str]:
        path = self._latest_file(None)
        if path is None:
            return []
        df = pl.read_csv(path, schema_overrides={"ticker": pl.Utf8})
        return sorted(df["ticker"].unique().to_list())

    def read(
        self, ticker: str | None = None, as_of: datetime.date | None = None
    ) -> pl.DataFrame:
        path = self._latest_file(as_of)
        if path is None:
            return pl.DataFrame()

        df = pl.read_csv(path, schema_overrides={"ticker": pl.Utf8}).with_columns(
            pl.col("themes").map_elements(
                ast.literal_eval, return_dtype=pl.List(pl.Utf8)
            )
        )
        if ticker is not None:
            df = df.filter(pl.col("ticker") == ticker)
        return df
