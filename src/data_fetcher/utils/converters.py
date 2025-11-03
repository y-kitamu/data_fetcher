"""Utility functions for data conversion between different formats."""

import datetime

import pandas as pd
import polars as pl


def pd_to_pl(df: pd.DataFrame) -> pl.DataFrame:
    """Convert yfinance pandas DataFrame to polars DataFrame.
    
    Args:
        df: Pandas DataFrame from yfinance with either Date or Datetime index
        
    Returns:
        pl.DataFrame: Polars DataFrame with datetime column and OHLCV data
        
    Raises:
        ValueError: If neither Date nor Datetime column is found
    """
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
