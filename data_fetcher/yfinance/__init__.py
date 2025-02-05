"""__init__.py"""

import datetime

import pandas as pd
import polars as pl


def pd_to_pl(df: pd.DataFrame) -> pl.DataFrame:
    """yfinanceで取得したのpd.DataFrameをpolarsのDataFrameに変換する"""
    df = df.reset_index()
    if len(df) == 0:
        return pl.DataFrame()
    if "Date" in df:
        pdf = pl.DataFrame(
            {
                "date": df["Date"].to_list(),
                "open": df["Open"],
                "high": df["High"],
                "low": df["Low"],
                "close": df["Close"],
                "volume": df["Volume"],
                "dividends": df["Dividends"],
                "stock_splits": df["Stock Splits"],
            }
        ).with_columns(pl.col("date").cast(pl.Date))
    elif "Datetime" in df:
        pdf = pl.DataFrame(
            {
                "datetime": [
                    d.astimezone(datetime.timezone.utc)
                    for d in df["Datetime"].to_list()
                ],
                "open": df["Open"],
                "high": df["High"],
                "low": df["Low"],
                "close": df["Close"],
                "volume": df["Volume"],
                "dividends": df["Dividends"],
                "stock_splits": df["Stock Splits"],
            }
        ).with_columns(pl.col("datetime").cast(pl.Datetime))
    else:
        raise ValueError("Date or Datetime column is not found in the DataFrame.")
    return pdf
