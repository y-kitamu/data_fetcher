"""Kabutan data reader for stored stock data."""

import datetime

import polars as pl

from ..core import convert_timedelta_to_str
from ..core.base_reader import BaseReader
from ..core.constants import PROJECT_ROOT


class KabutanReader(BaseReader):
    """Reader for Kabutan stored stock data."""

    def __init__(self):
        self.daily_data_dir = PROJECT_ROOT / "data/kabutan/daily"
        self.financial_data_dir = PROJECT_ROOT / "data/kabutan/financial"
        self.ticker_csv_path = PROJECT_ROOT / "data/jp_tickers.csv"
        self._available_tickers = []
        self.ticker_symbol_dict = self._get_symbol_names()

    def _get_symbol_names(self) -> dict[str, str]:
        """Get ticker to symbol name mapping."""
        if not self.ticker_csv_path.exists():
            return {}
        df = pl.read_csv(self.ticker_csv_path)
        ticker_symbol_dict = {}
        for i in range(len(df)):
            ticker_symbol_dict[df["コード"][i]] = df["銘柄名"][i]
        return ticker_symbol_dict

    @property
    def available_tickers(self) -> list[str]:
        if len(self._available_tickers) == 0:
            self._available_tickers = sorted(
                [csv_path.stem for csv_path in self.daily_data_dir.glob("*.csv")]
            )
        return self._available_tickers

    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        csv_path = self.daily_data_dir / f"{symbol}.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file {csv_path} does not exist.")

        df = pl.read_csv(csv_path)
        earliest_date = df["date"].drop_nulls().drop_nans().min()
        if earliest_date is None:
            raise ValueError(f"No valid date found in {csv_path}.")
        return datetime.datetime.strptime(earliest_date, "%Y/%m/%d")

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        csv_path = self.daily_data_dir / f"{symbol}.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file {csv_path} does not exist.")

        df = pl.read_csv(csv_path)
        latest_date = df["date"].drop_nulls().drop_nans().max()
        if latest_date is None:
            raise ValueError(f"No valid date found in {csv_path}.")
        return datetime.datetime.strptime(latest_date, "%Y/%m/%d")

    def read_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime = datetime.datetime(1970, 1, 1),
        end_date: datetime.datetime = datetime.datetime.now(),
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        """Kabutan does not provide tick data."""
        raise NotImplementedError("KabutanReader does not support tick data.")

    def read_ohlc_impl(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> pl.DataFrame:
        """Read OHLC data for a symbol."""
        if interval.seconds > 0:
            raise ValueError("Intraday intervals are not supported.")

        csv_path = self.daily_data_dir / f"{symbol}.csv"
        if not csv_path.exists():
            return pl.DataFrame()

        df = pl.read_csv(csv_path).with_columns(
            pl.col("date")
            .str.strptime(pl.Datetime, format="%Y/%m/%d")
            .alias("datetime")
        )

        df = df.filter(
            pl.col("datetime").is_between(start_date, end_date, closed="both")
        )

        df = (
            df.group_by_dynamic(
                pl.col("datetime"), every=convert_timedelta_to_str(interval)
            )
            .agg(
                pl.col("open").first().alias("open"),
                pl.col("high").max().alias("high"),
                pl.col("low").min().alias("low"),
                pl.col("close").last().alias("close"),
                pl.col("volume").cast(pl.Float64).sum().alias("volume"),
            )
            .sort(pl.col("datetime"))
        )
        return df

    def read_financial(self, symbol: str) -> pl.DataFrame:
        """Read financial data for a symbol."""
        csv_path = self.financial_data_dir / f"{symbol}.csv"
        if not csv_path.exists():
            return pl.DataFrame()

        df = pl.read_csv(csv_path)
        if len(df) == 0:
            return df
        return df.with_columns(
            pl.col("annoounce_date").str.strptime(pl.Date, format="%y/%m/%d"),
        )

    def get_ticker_symbol_name(self, ticker: str) -> str:
        """Get symbol name for a ticker."""
        if ticker in self.ticker_symbol_dict:
            return self.ticker_symbol_dict[ticker]
        else:
            raise ValueError(f"Ticker {ticker} not found in symbol dictionary.")

    def get_domestic_stocks(self, tickers: list[str] | None = None) -> list[str]:
        """Get list of domestic stock tickers (excluding ETFs)."""
        if tickers is None:
            tickers = self.available_tickers
        if not self.ticker_csv_path.exists():
            return tickers
        df = pl.read_csv(self.ticker_csv_path)
        etf_tickers = df.filter(~pl.col("市場・商品区分").str.contains("内国株式"))
        etf_list = etf_tickers["コード"].to_list()
        return [ticker for ticker in tickers if ticker not in etf_list]
