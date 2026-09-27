"""Reader for OECD CLI macro series (data/oecd).

ファイル名は`CLI_{REF_AREA}.csv`（例: `CLI_JPN.csv`）で、symbolはそのファイル名の
stem（例: "CLI_JPN"）そのもの。

値の改定履歴を`fetched_at`列で保持しているため、同一`date`に複数行がある場合は
`fetched_at`が最大の行が最新の確定値になる。呼び出し側が最新値だけ欲しい場合は
`read_ticker`の戻り値を`date`ごとに`fetched_at`最大の行へ絞り込むこと。
"""

import datetime
from pathlib import Path

import polars as pl

from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT

DATA_DIR = PROJECT_ROOT / "data" / "oecd"


class OecdReader(BaseReader):
    SOURCE_NAME = "oecd"

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

    def _read(self, symbol: str) -> pl.DataFrame:
        csv_path = self.data_dir / f"{symbol}.csv"
        if not csv_path.exists():
            return pl.DataFrame()
        return pl.read_csv(csv_path, infer_schema_length=0).with_columns(
            pl.col("date").str.to_datetime(strict=False),
            pl.col("value").cast(pl.Float64, strict=False),
        )

    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        df = self._read(symbol)
        if len(df) == 0:
            return datetime.datetime(1970, 1, 1)
        return df["date"].min()

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        df = self._read(symbol)
        if len(df) == 0:
            return datetime.datetime(1970, 1, 1)
        return df["date"].max()

    def read_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        df = self._read(symbol)
        if len(df) == 0:
            return df
        return df.filter(
            pl.col("date").is_between(start_date, end_date, closed="both")
        ).sort(["date", "fetched_at"])
