"""SBI data reader for stored tick data."""

import datetime
from pathlib import Path

import polars as pl

from ..core import convert_timedelta_to_str
from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT


class SBIReader(BaseReader):
    """Reader for SBI stored tick data."""

    def __init__(self):
        self.data_dir = PROJECT_ROOT / "data/sbi/tick"
        self._available_tickers = []

    @property
    def available_tickers(self) -> list[str]:
        """Get list of available ticker symbols.

        Returns:
            list[str]: List of available ticker symbols
        """
        if len(self._available_tickers) == 0:
            tickers = set()
            for date_dir in self.data_dir.glob("*"):
                if not date_dir.is_dir():
                    continue
                for csv_path in date_dir.glob("qr-*.csv"):
                    # Extract symbol from filename: qr-{symbol}-{date}.csv
                    parts = csv_path.stem.split("-")
                    if len(parts) >= 2:
                        symbol = parts[1]
                        tickers.add(symbol)
            self._available_tickers = sorted(list(tickers))
        return self._available_tickers

    def _get_dates(self, symbol: str) -> list[datetime.datetime]:
        """Get all available dates for a symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            list[datetime.datetime]: List of available dates
        """
        dates = []
        for date_dir in sorted(self.data_dir.glob("*")):
            if not date_dir.is_dir():
                continue
            csv_path = date_dir / f"qr-{symbol}-{date_dir.name}.csv"
            if csv_path.exists():
                try:
                    date = datetime.datetime.strptime(date_dir.name, "%Y%m%d")
                    dates.append(date)
                except ValueError:
                    continue
        return dates

    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        """Get the earliest available date for a symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            datetime.datetime: Earliest available date
        """
        dates = self._get_dates(symbol)
        if len(dates) == 0:
            return datetime.datetime(1970, 1, 1)
        return dates[0]

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        """Get the latest available date for a symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            datetime.datetime: Latest available date
        """
        dates = self._get_dates(symbol)
        if len(dates) == 0:
            return datetime.datetime(1970, 1, 1)
        return dates[-1]

    def _read_csv(self, filepath: Path) -> pl.DataFrame:
        """Read SBI format CSV file.

        Args:
            filepath: Path to SBI CSV file

        Returns:
            pl.DataFrame: Processed DataFrame with price, volume, amount, time columns
        """
        if not filepath.exists():
            return pl.DataFrame()

        schema = {
            "値段": pl.Float64,
            "株数": pl.Float64,
            "金額": pl.Float64,
            "時刻": pl.Utf8,
        }
        df = (
            pl.read_csv(filepath, schema_overrides=schema, infer_schema_length=0)
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

    def _read_html_csv(self, filepath: Path) -> pl.DataFrame:
        """Read SBI HTML-scraped CSV file (includes trade direction).

        The _html.csv format has columns: price, volume, amount, is_uptick, time
        where is_uptick=True means buyer-initiated (ask-side) trade.

        Args:
            filepath: Path to _html.csv file

        Returns:
            pl.DataFrame: DataFrame with price, volume, amount, is_uptick, time, time_in_seconds
        """
        if not filepath.exists():
            return pl.DataFrame()

        schema = {
            "price": pl.Float64,
            "volume": pl.Float64,
            "amount": pl.Float64,
            "is_uptick": pl.Utf8,
            "time": pl.Utf8,
        }
        df = (
            pl.read_csv(filepath, schema_overrides=schema, infer_schema_length=0)
            .select(
                pl.col("price"),
                pl.col("volume"),
                pl.col("amount"),
                pl.col("is_uptick").str.to_lowercase().eq("true").alias("is_uptick"),
                pl.col("time").str.strptime(pl.Time, "%H:%M:%S").alias("time"),
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

    def read_ticker_with_direction(
        self,
        symbol: str,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
    ) -> pl.DataFrame:
        """Read tick data with trade direction (buyer/seller initiated).

        Reads _html.csv files which include is_uptick flag.
        is_uptick=True means the trade was buyer-initiated (executed at ask price).

        Args:
            symbol: Ticker symbol
            start_date: Start date (default: 1970-01-01)
            end_date: End date (default: now)

        Returns:
            pl.DataFrame: Tick data with datetime, price, volume, amount, is_uptick columns.
                         Returns empty DataFrame if no _html.csv files are found.
        """
        dfs = []
        for date_dir in sorted(self.data_dir.glob("*")):
            if not date_dir.is_dir():
                continue

            try:
                dir_date = datetime.datetime.strptime(date_dir.name, "%Y%m%d")
            except ValueError:
                continue

            # 日付部分のみで比較（時刻を持つstart_date/end_dateに対応）
            if dir_date.date() < start_date.date() or dir_date.date() > end_date.date():
                continue

            html_csv_path = date_dir / f"qr-{symbol}-{date_dir.name}_html.csv"
            if not html_csv_path.exists():
                continue

            df = self._read_html_csv(html_csv_path)
            if len(df) > 0:
                df = df.with_columns(
                    pl.datetime(
                        dir_date.year,
                        dir_date.month,
                        dir_date.day,
                        hour=pl.col("time").dt.hour(),
                        minute=pl.col("time").dt.minute(),
                        second=pl.col("time").dt.second(),
                    ).alias("datetime")
                ).select(["datetime", "price", "volume", "amount", "is_uptick"])
                dfs.append(df)

        if len(dfs) == 0:
            return pl.DataFrame()

        return pl.concat(dfs).sort("datetime")

    def read_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        """Read tick data for a symbol.

        Args:
            symbol: Ticker symbol
            start_date: Start date (default: 1970-01-01)
            end_date: End date (default: now)
            timezone_delta: Timezone offset (default: 9 hours for JST)

        Returns:
            pl.DataFrame: Tick data with datetime, price, volume, amount columns
        """
        dfs = []
        for date_dir in sorted(self.data_dir.glob("*")):
            if not date_dir.is_dir():
                continue

            try:
                dir_date = datetime.datetime.strptime(date_dir.name, "%Y%m%d")
            except ValueError:
                continue

            if dir_date < start_date or dir_date > end_date:
                continue

            csv_path = date_dir / f"qr-{symbol}-{date_dir.name}.csv"
            if not csv_path.exists():
                continue

            df = self._read_csv(csv_path)
            if len(df) > 0:
                # Add date to time to create datetime
                df = df.with_columns(
                    pl.datetime(
                        dir_date.year,
                        dir_date.month,
                        dir_date.day,
                        hour=pl.col("time").dt.hour(),
                        minute=pl.col("time").dt.minute(),
                        second=pl.col("time").dt.second(),
                    ).alias("datetime")
                ).select(["datetime", "price", "volume", "amount"])
                dfs.append(df)

        if len(dfs) == 0:
            return pl.DataFrame()

        return pl.concat(dfs).sort("datetime")

    def read_html_csv_for_date(self, symbol: str, date: datetime.date) -> pl.DataFrame:
        """特定日の _html.csv を直接読み込む（高速版: ディレクトリスキャン不要）。

        Args:
            symbol: Ticker symbol
            date: Target date

        Returns:
            pl.DataFrame: Tick data with datetime, price, volume, amount, is_uptick columns.
        """
        date_str = date.strftime("%Y%m%d")
        html_csv_path = self.data_dir / date_str / f"qr-{symbol}-{date_str}_html.csv"
        df = self._read_html_csv(html_csv_path)
        if df.is_empty():
            return pl.DataFrame()
        return df.with_columns(
            pl.datetime(date.year, date.month, date.day,
                        hour=pl.col("time").dt.hour(),
                        minute=pl.col("time").dt.minute(),
                        second=pl.col("time").dt.second()).alias("datetime")
        ).select(["datetime", "price", "volume", "amount", "is_uptick"])

    def read_ohlc_impl(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> pl.DataFrame:
        """Read OHLC data for a symbol.

        Args:
            symbol: Ticker symbol
            interval: Time interval for OHLC bars
            start_date: Start date
            end_date: End date

        Returns:
            pl.DataFrame: OHLC data with datetime, open, high, low, close, volume columns
        """
        tick_df = self.read_ticker(symbol, start_date, end_date)

        if len(tick_df) == 0:
            return pl.DataFrame()

        # Convert tick data to OHLCV
        ohlcv = (
            tick_df.group_by_dynamic(
                "datetime", every=convert_timedelta_to_str(interval)
            )
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
            .sort("datetime")
        )

        return ohlcv
