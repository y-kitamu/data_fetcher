"""io.py"""

import datetime
from pathlib import Path

import polars as pl

from ..constants import PROJECT_ROOT

INDEX_CODE_LIST = ["0000", "0010"]


def read_data_csv(
    csv_path: Path | str,
    exclude_none: bool = True,
    with_rs: bool = True,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    weekly: bool = False,
) -> pl.DataFrame:
    if not Path(csv_path).exists():
        csv_path = PROJECT_ROOT / f"data/daily/{csv_path}.csv"

    df = pl.read_csv(csv_path)
    columns = [
        pl.col("date").str.to_datetime("%Y/%m/%d").cast(pl.Date),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Int64),
    ]
    if with_rs:
        columns.append(pl.col("rs_nikkei").cast(pl.Float64))
        columns.append(pl.col("rs_topix").cast(pl.Float64))
        columns.append(pl.col("rs").cast(pl.Float64))
    df = df.select(columns)

    if Path(csv_path).stem not in INDEX_CODE_LIST and exclude_none:
        df = df.filter(
            (pl.col("volume").is_not_nan().is_not_null()) & (pl.col("volume") > 0)
        )

    if start_date is not None:
        df = df.filter(pl.col("date") >= start_date)
    if end_date is not None:
        df = df.filter(pl.col("date") <= end_date)

    df = df.sort("date")
    if weekly:
        df = convert_to_weekly(df)

    return df


def convert_to_weekly(df: pl.DataFrame) -> pl.DataFrame:
    return df.group_by_dynamic(pl.col("date"), every="1w", start_by="monday").agg(
        pl.col("date").first().alias("start_date"),
        pl.col("date").last().alias("end_date"),
        pl.col("open").first(),
        pl.col("close").last(),
        pl.col("high").max(),
        pl.col("low").min(),
        pl.col("volume").sum(),
    )


def write_data_csv(df: pl.DataFrame, csv_path: Path):
    df.with_columns(pl.col("date").dt.to_string("%Y/%m/%d")).write_csv(csv_path)


def read_financial_csv(csv_path: Path | str) -> pl.DataFrame:
    if not Path(csv_path).exists():
        csv_path = PROJECT_ROOT / f"data/financial/{csv_path}.csv"

    df = pl.read_csv(csv_path)
    df = df.select(
        [
            pl.col("year").cast(pl.Int64),
            pl.col("month").cast(pl.Int64),
            pl.col("duration").cast(pl.Int64),
            pl.col("annoounce_date").str.to_datetime("%y/%m/%d").cast(pl.Date),
            pl.col("is_prediction").cast(pl.Boolean),
            pl.col("total_revenue").cast(pl.Float64),
            pl.col("operating_income").cast(pl.Float64),
            pl.col("ordinary_profit").cast(pl.Float64),
            pl.col("net_income").cast(pl.Float64),
            pl.col("eps").cast(pl.Float64),
            pl.col("divident").cast(pl.Float64),
        ]
    )
    return df
