"""Readers for taisyaku.jp margin/short-selling balance data."""

import datetime
from pathlib import Path

import polars as pl

from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT

HISTORY_DIR = PROJECT_ROOT / "data" / "taisyaku" / "history"
ZANDAKA_DIR = PROJECT_ROOT / "data" / "taisyaku" / "zandaka"


class _TaisyakuCsvReader(BaseReader):
    """Shared per-date CSV reading logic for taisyaku/history and taisyaku/zandaka.

    Both stores are one CSV per business day with a 銘柄コード column; only the
    filename glob pattern and date-parsing prefix differ between the two.
    """

    _GLOB: str
    _DATE_LEN: int = 8

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self._available_tickers: list[str] = []

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
    ) -> pl.DataFrame:
        dfs = []
        for path in self._files():
            file_date = self._file_date(path)
            if start_date.date() <= file_date <= end_date.date():
                df = pl.read_csv(path, schema_overrides={"銘柄コード": pl.Utf8})
                df = df.filter(pl.col("銘柄コード") == symbol)
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

    def __init__(self, data_dir: Path = HISTORY_DIR):
        super().__init__(data_dir)


class TaisyakuZandakaReader(_TaisyakuCsvReader):
    """Reader for data/taisyaku/zandaka/{YYYYMMDD}_kakuho.csv (margin balance
    disclosure)."""

    SOURCE_NAME = "taisyaku_zandaka"
    _GLOB = "*_kakuho.csv"

    def __init__(self, data_dir: Path = ZANDAKA_DIR):
        super().__init__(data_dir)
