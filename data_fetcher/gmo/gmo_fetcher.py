"""gmo_fetcher.py
gmoの過去データを取得する
"""
import datetime
import io
from pathlib import Path
from typing import override

import polars as pl
import requests
from tqdm import tqdm

from ..base_fetcher import BaseFetcher
from ..constants import PROJECT_ROOT
from ..session import get_session


def add_maker_fee(df: pl.DataFrame):
    df = df.with_columns(
        pl.when(pl.col("datetime") < datetime.datetime(2020, 8, 5, 6, 0, 0))
        .then(0.0)
        .when(pl.col("datetime") < datetime.datetime(2020, 9, 9, 6, 0, 0))
        .then(-0.00035)
        .when(pl.col("datetime") < datetime.datetime(2020, 11, 4, 6, 0, 0))
        .then(-0.00025)
        .otherwise(0.0)
        .alias("maker_fee")
    )
    return df


class GMOFetcher(BaseFetcher):
    _API_ENDPOINT = "https://api.coin.z.com"

    def __init__(self, data_dir: Path = PROJECT_ROOT / "data" / "gmo" / "tick"):
        self.data_dir = data_dir
        self._available_tickers = self.get_available_tickers()
        self.session = get_session()

    @property
    @override
    def available_tickers(self) -> list[str]:
        return self._available_tickers

    @override
    def get_latest_date(self, symbol: str) -> datetime.datetime:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"*_{symbol}.csv.gz"))
        if len(ticker_file_list) == 0:
            raise ValueError(f"No data for {symbol}")
        return datetime.datetime.strptime(ticker_file_list[-1].parent.name, "%Y%m%d")

    @override
    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"*_{symbol}.csv.gz"))
        if len(ticker_file_list) == 0:
            raise ValueError(f"No data for {symbol}")
        return datetime.datetime.strptime(ticker_file_list[0].parent.name, "%Y%m%d")

    def get_available_tickers(self) -> list[str]:
        path = "/public/v1/ticker"
        response = requests.get(self._API_ENDPOINT + path)
        response.raise_for_status()
        res_json = response.json()
        if "data" in res_json:
            return [row["symbol"] for row in response.json()["data"]]

        # apiでの取得に失敗した場合は過去のデータから推定する
        print("Failed to fetch available tickers. {}".format(response.text))
        tickers = set(
            [f.name[9:].replace(".csv.gz", "") for f in self.data_dir.rglob("*.csv.gz")]
        )
        return sorted(tickers)

    def download_all(self):
        for ticker in tqdm(self.available_tickers):
            date = datetime.date.today() - datetime.timedelta(days=2)
            while True:
                output_path = self.data_dir / "{date}/{date}_{ticker}.csv.gz".format(
                    date=date.strftime("%Y%m%d"), ticker=ticker
                )
                if output_path.exists():  # Already downloaded
                    break
                df = self.download(ticker, date, output_path)
                if len(df) == 0:  # No data
                    break
                date -= datetime.timedelta(days=1)

    def download(
        self, ticker: str, date: datetime.date, output_path: Path | None = None
    ) -> pl.DataFrame:
        """Download tick data for the specified date"""
        path = "/data/trades/{ticker}/{year}/{month:02d}/{date}_{ticker}.csv.gz".format(
            ticker=ticker,
            year=date.year,
            month=date.month,
            date=date.strftime("%Y%m%d"),
        )
        response = self.session.get(self._API_ENDPOINT + path)
        if response.status_code != 200:
            print(response.status_code)
            print(self._API_ENDPOINT + path)
            print(response.content)
            return pl.DataFrame()

        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with output_path.open("wb") as f:
                f.write(response.content)
        try:
            return pl.read_csv(io.BytesIO(response.content))
        except Exception as e:
            print(e)
            return pl.DataFrame()

    @override
    def fetch_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"*_{symbol}.csv.gz"))
        if start_date is None:
            start_date = datetime.datetime(1970, 1, 1)
        if end_date is None:
            end_date = datetime.datetime.now()

        dfs = []
        pre_start_date = start_date - datetime.timedelta(days=1)
        for file_path in ticker_file_list:
            date = datetime.datetime.strptime(file_path.parent.name, "%Y%m%d")
            if pre_start_date.date() <= date.date() <= end_date.date():
                dfs.append(
                    pl.read_csv(file_path).select(
                        pl.col("symbol"),
                        pl.col("side"),
                        pl.col("price"),
                        pl.col("size"),
                        pl.col("timestamp")
                        .str.to_datetime("%Y-%m-%d %H:%M:%S.%3f")
                        .alias("datetime")
                        + timezone_delta,
                    )
                )
        if len(dfs) == 0:
            return pl.DataFrame()

        df = pl.concat(dfs).filter(
            pl.col("datetime").is_between(start_date, end_date, closed="left")
        )
        return df

    @override
    def fetch_ohlc(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        fill_missing_date: bool = False,
    ) -> pl.DataFrame:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")
        return super().fetch_ohlc(
            symbol, interval, start_date, end_date, fill_missing_date
        )

    @override
    def fetch_volume_bar(
        self,
        symbol: str,
        volume_size: float,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
    ) -> pl.DataFrame:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")
        return super().fetch_volume_bar(symbol, volume_size, start_date, end_date)

    @override
    def fetch_TIB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        pass

    @override
    def fetch_VIB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        pass

    @override
    def fetch_TRB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        pass

    @override
    def fetch_VRB(
        self, symbol: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> pl.DataFrame:
        pass
