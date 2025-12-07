import datetime

import polars as pl


def convert_timedelta_to_str(interval: datetime.timedelta) -> str:
    """Convert timedelta to string format used by polars.

    Args:
        interval: Time interval as timedelta

    Returns:
        str: String representation (e.g., "1d2h30m15s")
    """
    interval_str = ""
    # if interval.weeks > 0:
    #     interval_str += f"{interval.weeks}w"
    if interval.days > 0:
        interval_str += f"{interval.days}d"
    hours = interval.seconds // 3600
    minutes = (interval.seconds % 3600) // 60
    seconds = interval.seconds % 60
    if hours > 0:
        interval_str += f"{hours}h"
    if minutes > 0:
        interval_str += f"{minutes}m"
    if seconds > 0:
        interval_str += f"{seconds}s"
    return interval_str


def convert_str_to_timedelta(interval: str) -> datetime.timedelta:
    """Convert string interval to timedelta.

    Args:
        interval: Interval string (e.g., "5m", "1h", "1d")

    Returns:
        datetime.timedelta: Timedelta object

    Raises:
        ValueError: If interval format is unknown
    """
    if interval[-1] == "s":
        return datetime.timedelta(seconds=int(interval[:-1]))
    elif interval[-1] == "m":
        return datetime.timedelta(minutes=int(interval[:-1]))
    elif interval[-1] == "h":
        return datetime.timedelta(hours=int(interval[:-1]))
    elif interval[-1] == "d":
        return datetime.timedelta(days=int(interval[:-1]))
    elif interval[-1] == "w":
        return datetime.timedelta(weeks=int(interval[:-1]))
    raise ValueError(f"Unknown interval: {interval}")


def convert_tick_to_ohlc(
    tick_df: pl.DataFrame,
    interval: datetime.timedelta,
    date_key="datetime",
    price_key="price",
    size_key="size",
) -> pl.DataFrame:
    """Convert tick data to OHLC bars.

    Args:
        tick_df: Tick data with columns: datetime, price, size
        interval: Time interval for OHLC bars

    Returns:
        pl.DataFrame: OHLC data with columns: datetime, open, high, low, close, volume
    """
    ohlc_df = (
        tick_df.group_by_dynamic(
            pl.col(date_key), every=convert_timedelta_to_str(interval)
        )
        .agg(
            pl.col(price_key).first().alias("open"),
            pl.col(price_key).max().alias("high"),
            pl.col(price_key).min().alias("low"),
            pl.col(price_key).last().alias("close"),
            pl.col(size_key).sum().alias("volume"),
        )
        .sort(pl.col(date_key))
    )
    return ohlc_df
