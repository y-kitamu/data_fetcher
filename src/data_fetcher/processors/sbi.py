"""SBI data processors.

Utility functions for processing SBI format CSV data.
"""

from pathlib import Path

import polars as pl


def read_csv(filepath: Path) -> pl.DataFrame:
    """Read SBI format CSV file.

    Args:
        filepath: Path to SBI CSV file

    Returns:
        pl.DataFrame: Processed DataFrame with price, volume, amount, time columns
    """
    df = (
        pl.read_csv(filepath, infer_schema_length=None)
        .select(
            pl.col("値段").alias("price"),
            pl.col("株数").alias("volume"),
            pl.col("金額").alias("amount"),
            pl.col("時刻").str.strptime(pl.Time, "%H:%M:%S").alias("time"),
        )
        .sort(pl.col("time"))
        .with_columns(
            (
                pl.col("time").dt.hour().cast(pl.Int64) * 3600
                + pl.col("time").dt.minute().cast(pl.Int64) * 60
                + pl.col("time").dt.second().cast(pl.Int64)
            ).alias("time_in_seconds")
        )
    )
    return df


def convert_to_ohlcv(df: pl.DataFrame, freq: str = "5m") -> pl.DataFrame:
    """Convert tick data to OHLCV format.

    Args:
        df: DataFrame with price, volume, time columns
        freq: Frequency for grouping (e.g., "5m", "1h")

    Returns:
        pl.DataFrame: OHLCV DataFrame
    """
    ohlcv = (
        df.with_columns(time=pl.date(2024, 1, 1).dt.combine(pl.col("time").dt.time()))
        .group_by_dynamic("time", every=freq, period=freq)
        .agg(
            [
                pl.first("price").alias("open"),
                pl.max("price").alias("high"),
                pl.min("price").alias("low"),
                pl.last("price").alias("close"),
                pl.sum("volume").alias("volume"),
            ]
        )
        .drop_nulls()
        .with_columns(pl.col("time").dt.time().alias("time"))
        .sort(pl.col("time"))
    )
    return ohlcv
