"""Readers for EDINET disclosure data (data/edinet)."""

import datetime
from pathlib import Path

import polars as pl

from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT

FINANCIAL_DIR = PROJECT_ROOT / "data" / "edinet" / "financial"
LARGE_SHAREHOLDING_DIR = PROJECT_ROOT / "data" / "edinet" / "large_shareholding"


class EdinetFinancialReader(BaseReader):
    """Reader for data/edinet/financial/{code}0.csv (e.g. 13010.csv = code 1301)."""

    SOURCE_NAME = "edinet_financial"

    def __init__(self, data_dir: Path = FINANCIAL_DIR):
        self.data_dir = data_dir
        self._available_tickers: list[str] = []

    @property
    def available_tickers(self) -> list[str]:
        if len(self._available_tickers) == 0:
            self._available_tickers = sorted(
                {path.stem[:4] for path in self.data_dir.glob("*.csv")}
            )
        return self._available_tickers

    def _path_for(self, symbol: str) -> Path | None:
        matches = sorted(self.data_dir.glob(f"{symbol}*.csv"))
        return matches[0] if matches else None

    def read_financial(
        self,
        symbol: str,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
    ) -> pl.DataFrame:
        csv_path = self._path_for(symbol)
        if csv_path is None:
            return pl.DataFrame()

        df = pl.read_csv(csv_path).with_columns(
            pl.col("announce_date").cast(pl.Utf8).str.to_datetime("%Y%m%d%H%M")
        )
        if start_date is not None:
            df = df.filter(pl.col("announce_date") >= start_date)
        if end_date is not None:
            df = df.filter(pl.col("announce_date") <= end_date)
        return df.sort("announce_date")


class EdinetLargeShareholdingReader(BaseReader):
    """Reader for data/edinet/large_shareholding/{YYYYMMDD}.csv.

    Market-wide per-date filing feed; symbol is an optional filter, not routing
    (a single filing can name multiple issuers/holders across files).
    """

    SOURCE_NAME = "edinet_large_shareholding"

    def __init__(self, data_dir: Path = LARGE_SHAREHOLDING_DIR):
        self.data_dir = data_dir

    def read(
        self,
        symbol: str | None = None,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
    ) -> pl.DataFrame:
        dfs = []
        for path in sorted(self.data_dir.glob("*.csv")):
            try:
                file_date = datetime.datetime.strptime(path.stem, "%Y%m%d").date()
            except ValueError:
                continue
            if start_date is not None and file_date < start_date:
                continue
            if end_date is not None and file_date > end_date:
                continue
            dfs.append(
                pl.read_csv(
                    path,
                    schema_overrides={
                        "issuer_sec_code": pl.Utf8,
                        "holding_ratio": pl.Float64,
                        "holding_ratio_prev": pl.Float64,
                        "shares_held": pl.Float64,
                        "shares_outstanding": pl.Float64,
                    },
                )
            )

        if len(dfs) == 0:
            return pl.DataFrame()

        df = pl.concat(dfs, how="diagonal_relaxed").sort("filing_date")
        if symbol is not None:
            df = df.filter(pl.col("issuer_sec_code") == symbol)
        return df
