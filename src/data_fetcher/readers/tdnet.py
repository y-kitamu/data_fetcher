"""Reader for TDnet extracted financial numeric data (data/tdnet/csv).

NOTE: as of this writing, data/tdnet/csv has not been refreshed since ~2025-09
even though the raw XBRL downloads under data/tdnet/raw keep growing daily —
the numeric-extraction step of the pipeline has stalled. This reader serves
whatever is in the (frozen) csv store; resuming extraction is a separate,
pre-existing follow-up outside this reader's scope.
"""

import datetime
from pathlib import Path

import polars as pl

from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT

DATA_DIR = PROJECT_ROOT / "data" / "tdnet" / "csv"


class TdnetReader(BaseReader):
    SOURCE_NAME = "tdnet"

    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = data_dir
        self._available_tickers: list[str] = []

    @property
    def available_tickers(self) -> list[str]:
        if len(self._available_tickers) == 0:
            self._available_tickers = sorted(
                path.stem for path in self.data_dir.glob("*.csv")
            )
        return self._available_tickers

    def read_financial(
        self,
        symbol: str,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
    ) -> pl.DataFrame:
        csv_path = self.data_dir / f"{symbol}.csv"
        if not csv_path.exists():
            return pl.DataFrame()

        df = pl.read_csv(csv_path, schema_overrides={"code": pl.Utf8}).with_columns(
            pl.col("filing_date").str.to_date("%Y-%m-%d")
        )
        if start_date is not None:
            df = df.filter(pl.col("filing_date") >= start_date.date())
        if end_date is not None:
            df = df.filter(pl.col("filing_date") <= end_date.date())
        return df.sort("filing_date")
