from pathlib import Path

import polars as pl
from loguru import logger

from ..core.constants import PROJECT_ROOT


def read_calendar(
    year: int, month: int, data_dir: Path = PROJECT_ROOT / "data/forex_factory"
) -> pl.DataFrame:
    """Read economic calendar data for a given year and month."""
    file_path = data_dir / f"calendar_{year}_{month:02d}.csv"
    if not file_path.exists():
        logger.warning(f"Calendar data not found for {year}-{month:02d}")
        return pl.DataFrame()
    df = pl.read_csv(file_path).with_columns(pl.col("datetime").str.to_datetime())
    return df
