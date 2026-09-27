"""Reader for BOJ bulk statistics (data/boj_stats).

企業物価指数(cgpi.csv)・短観(tankan.csv)は「1系列=1ファイル」ではなく
「1データセット=1ファイル」（`series_code`列付きのlong形式）で保存されている
（企業物価指数3,042系列・短観48,430系列と非常に多く、1系列=1ファイルは非現実的
なため）。そのため`google_trends.py`のような1ファイル=1symbolの前提ではなく、
`jpx_stats.py`の市場全体読み込み＋in-memoryフィルタと同じ考え方で、
`series_code`をsymbolとみなして`BaseReader`契約（`available_tickers`/
`read_ticker`）を満たす。

値の改定履歴を`fetched_at`列で保持しているため、同一`date`に複数行がある場合は
`fetched_at`が最大の行が最新の確定値になる。呼び出し側が最新値だけ欲しい場合は
`read_ticker`の戻り値を`date`ごとに`fetched_at`最大の行へ絞り込むこと。
"""

import datetime
from pathlib import Path

import polars as pl

from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT

DATA_DIR = PROJECT_ROOT / "data" / "boj_stats"


class BojStatsReader(BaseReader):
    SOURCE_NAME = "boj_stats"

    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = data_dir
        self._available_tickers: list[str] = []

    def _read_all(self) -> pl.DataFrame:
        dfs = [
            pl.read_csv(path, infer_schema_length=0)
            for path in sorted(self.data_dir.glob("*.csv"))
        ]
        if not dfs:
            return pl.DataFrame(
                schema={
                    "series_code": pl.Utf8,
                    "date": pl.Utf8,
                    "value": pl.Utf8,
                    "fetched_at": pl.Utf8,
                }
            )
        return pl.concat(dfs, how="diagonal_relaxed").with_columns(
            pl.col("date").str.to_datetime(strict=False),
            pl.col("value").cast(pl.Float64, strict=False),
        )

    @property
    def available_tickers(self) -> list[str]:
        if len(self._available_tickers) == 0:
            df = self._read_all()
            if len(df) > 0:
                self._available_tickers = sorted(df["series_code"].unique().to_list())
        return self._available_tickers

    def _read(self, series_code: str) -> pl.DataFrame:
        df = self._read_all()
        if len(df) == 0:
            return df
        return df.filter(pl.col("series_code") == series_code)

    def get_earliest_date(self, series_code: str) -> datetime.datetime:
        df = self._read(series_code)
        if len(df) == 0:
            return datetime.datetime(1970, 1, 1)
        return df["date"].min()

    def get_latest_date(self, series_code: str) -> datetime.datetime:
        df = self._read(series_code)
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
