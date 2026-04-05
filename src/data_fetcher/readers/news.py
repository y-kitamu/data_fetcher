"""news.py - Unified JP news reader across all sources."""

import datetime
from pathlib import Path

import polars as pl
from loguru import logger

from ..core.constants import PROJECT_ROOT

_NEWS_ROOT = PROJECT_ROOT / "data" / "news"
_KNOWN_SOURCES = ["kabutan", "gnews", "yfinance"]
_NEWS_SCHEMA = {
    col: pl.Utf8
    for col in ["published_at", "source", "symbol", "title", "body", "url", "category"]
}


class JpNewsReader:
    """Reads saved JP news CSVs from all sources into a unified Polars DataFrame.

    Storage layout per source (all sources use date-based filenames):
      kabutan:  data/news/kabutan/{YYYY-MM-DD}.csv
      gnews:    data/news/gnews/{YYYY-MM-DD}.csv
      yfinance: data/news/yfinance/{YYYY-MM-DD}.csv

    When start_date / end_date are given only the CSV files whose filenames fall
    within that range are loaded, avoiding unnecessary I/O.
    """

    def read(
        self,
        sources: list[str] = _KNOWN_SOURCES,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
        symbols: list[str] | None = None,
    ) -> pl.DataFrame:
        """Return a unified DataFrame of news filtered by the given criteria.

        Args:
            sources: Which sources to include (default: all).
            start_date: Earliest published_at date to include.
            end_date: Latest published_at date to include.
            symbols: If given, only return rows for these ticker symbols.
        """
        frames: list[pl.DataFrame] = []
        for source in sources:
            source_dir = _NEWS_ROOT / source
            if not source_dir.exists():
                logger.debug(f"News source directory not found: {source_dir}")
                continue
            frames.extend(_read_csvs_in_range(source_dir, start_date, end_date))

        if not frames:
            return pl.DataFrame(schema=_NEWS_SCHEMA)

        df = (
            pl.concat(frames, how="diagonal_relaxed")
            .unique(subset=["url"], keep="first")
            .with_columns(
                pl.col("published_at")
                .str.to_datetime(format=None, strict=False)
                .alias("published_at")
            )
            .filter(pl.col("published_at").is_not_null())
            .sort("published_at")
        )

        if start_date is not None:
            cutoff = datetime.datetime.combine(
                start_date, datetime.time.min, tzinfo=datetime.timezone.utc
            )
            df = df.filter(pl.col("published_at") >= cutoff)
        if end_date is not None:
            cutoff = datetime.datetime.combine(
                end_date, datetime.time.max, tzinfo=datetime.timezone.utc
            )
            df = df.filter(pl.col("published_at") <= cutoff)
        if symbols is not None:
            df = df.filter(pl.col("symbol").is_in(symbols))

        return df


def _parse_filename_date(path: Path) -> datetime.date | None:
    """Return the date encoded in a YYYY-MM-DD stem, or None if not parseable."""
    try:
        return datetime.date.fromisoformat(path.stem)
    except ValueError:
        return None


def _read_csvs_in_range(
    directory: Path,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
) -> list[pl.DataFrame]:
    """Read CSVs from *directory* whose filenames fall in [start_date, end_date].

    Files whose stems are not YYYY-MM-DD dates are included only when no date
    range is specified (backward-compatibility with old symbol-named files).
    """
    frames = []
    for csv_path in sorted(directory.glob("*.csv")):
        file_date = _parse_filename_date(csv_path)
        if file_date is not None:
            if start_date is not None and file_date < start_date:
                continue
            if end_date is not None and file_date > end_date:
                continue
        else:
            # Non-date filename (e.g. old symbol.csv): skip when a range is requested
            if start_date is not None or end_date is not None:
                continue

        try:
            df = pl.read_csv(csv_path, infer_schema_length=0)
            if len(df) > 0:
                frames.append(df)
        except Exception as e:
            logger.debug(f"Failed to read {csv_path}: {e}")
    return frames
