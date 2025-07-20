"""kabutan_fetcher.py"""

import datetime
from pathlib import Path

import polars as pl

from ..base_fetcher import BaseFetcher, convert_timedelta_to_str
from ..constants import PROJECT_ROOT


def get_available_tickers(data_dir: Path) -> list[str]:
    return sorted([csv_path.stem for csv_path in data_dir.glob("*.csv")])


def get_symbol_names(csv_path: Path) -> dict[str, str]:
    df = pl.read_csv(csv_path)

    ticker_symbol_dict = {}
    for i in range(len(df)):
        ticker_symbol_dict[df["コード"][i]] = df["銘柄名"][i]
    return ticker_symbol_dict


class KabutanFetcher(BaseFetcher):
    def __init__(self):
        self.daily_data_dir = PROJECT_ROOT / "data/kabutan/daily"
        self.financial_data_dir = PROJECT_ROOT / "data/kabutan/financial"
        self.ticker_csv_path = PROJECT_ROOT / "data/jp_tickers.csv"
        self._available_tickers = get_available_tickers(self.daily_data_dir)
        self.ticker_symbol_dict = get_symbol_names(self.ticker_csv_path)

    @property
    def available_tickers(self) -> list[str]:
        return self._available_tickers

    def get_domestic_stocks(self, tickers: list[str] | None = None) -> list[str]:
        if tickers is None:
            tickers = self.available_tickers
        df = pl.read_csv(self.ticker_csv_path)
        etf_tickers = df.filter(~pl.col("市場・商品区分").str.contains("内国株式"))
        etf_list = etf_tickers["コード"].to_list()
        return [ticker for ticker in tickers if ticker not in etf_list]

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

    def get_ticker_symbol_name(self, ticker: str) -> str:
        if ticker in self.ticker_symbol_dict:
            return self.ticker_symbol_dict[ticker]
        else:
            raise ValueError(f"Ticker {ticker} not found in symbol dictionary.")

    def fetch_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        timezone_delta: datetime.timedelta = ...,
    ) -> pl.DataFrame:
        raise RuntimeError("KabutanFetcher does not support fetch_ticker.")

    def fetch_ohlc(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        fill_missing_date: bool = False,
        fetch_interval: datetime.timedelta | None = None,
    ) -> pl.DataFrame:
        if interval.seconds > 0:
            raise ValueError("Inter-day interval does not support.")

        csv_path = self.daily_data_dir / f"{symbol}.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file {csv_path} does not exist.")

        df = pl.read_csv(csv_path).with_columns(
            pl.col("date")
            .str.strptime(pl.Datetime, format="%Y/%m/%d")
            .alias("datetime")
        )
        if start_date is None:
            start_date = datetime.datetime(1970, 1, 1)
        if end_date is None:
            end_date = datetime.datetime.now()

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

    def fetch_financial(self, symbol):
        csv_path = self.financial_data_dir / f"{symbol}.csv"
        if not csv_path.exists():
            raise RuntimeError(f"File not found {csv_path}.")

        df = pl.read_csv(csv_path)
        if len(df) == 0:
            return df
        return df.with_columns(
            pl.col("annoounce_date").str.strptime(pl.Date, format="%y/%m/%d"),
        )
