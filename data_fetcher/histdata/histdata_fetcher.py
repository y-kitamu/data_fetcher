"""histadata_fetcher.py"""

import datetime
import gzip
import shutil
from pathlib import Path
from typing import override

import polars as pl
import requests
from histdata import download_hist_data as dl
from histdata.api import Platform as P
from histdata.api import TimeFrame as TF

from ..base_fetcher import BaseFetcher
from ..constants import PROJECT_ROOT
from ..session import get_session


class HistDataFetcher(BaseFetcher):

    def __init__(self, data_dir: Path = PROJECT_ROOT / "data" / "histdata" / "tick"):
        self.data_dir = data_dir
        self.session = get_session()
        self._available_tickers = self.get_available_tickers()

    @property
    @override
    def available_tickers(self) -> list[str]:
        return [
            "usdjpy",
            "eurjpy",
            "nsxusd",
            "jpxjpy",
        ]

    def get_available_tickers(self) -> list[str]:
        res = requests.get(
            "https://raw.githubusercontent.com/philipperemy/FX-1-Minute-Data/refs/heads/master/pairs.csv"
        )
        tickers = [row.split(",")[1] for row in res.text.split("\n")[1:] if row != ""]
        return sorted(tickers)

    @override
    def get_latest_date(self, symbol: str) -> datetime.datetime:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"{symbol}_*.csv.gz"))
        if len(ticker_file_list) == 0:
            raise ValueError(f"No data for {symbol}")
        return datetime.datetime.strptime(ticker_file_list[-1].parent.name, "%Y%m")

    @override
    def get_earliest_date(self, symbol: str) -> datetime.datetime:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"{symbol}_*.csv.gz"))
        if len(ticker_file_list) == 0:
            raise ValueError(f"No data for {symbol}")
        return datetime.datetime.strptime(ticker_file_list[0].parent.name, "%Y%m")

    def get_original_filestem(self, ticker: str, year: int, month: int) -> str:
        return f"DAT_ASCII_{ticker.upper()}_T_{year}{month:02d}"

    def get_gz_filestem(self, ticker: str, year: int, month: int) -> str:
        return f"{ticker}_{year}_{month:02d}.csv.gz"

    def download_all(self):
        work_dir = self.data_dir / "work"
        work_dir.mkdir(parents=True, exist_ok=True)

        for ticker in self.available_tickers:
            date = datetime.date.today() - datetime.timedelta(days=31)
            while True:
                output_dir = self.data_dir / f"{date.year}{date.month:02d}"
                output_dir.mkdir(parents=True, exist_ok=True)
                gz_path = output_dir / self.get_gz_filestem(
                    ticker, date.year, date.month
                )
                if gz_path.exists():
                    date -= datetime.timedelta(days=30)
                    continue

                try:
                    output_path = Path(
                        dl(
                            year=str(date.year),
                            month=str(date.month),
                            pair=ticker,
                            platform=P.GENERIC_ASCII,
                            time_frame=TF.TICK_DATA,
                            output_directory=work_dir.as_posix(),
                        )
                    )
                    extract_dir = output_path.parent / output_path.stem
                    shutil.unpack_archive(output_path, extract_dir=extract_dir)
                    csv_path = extract_dir / (extract_dir.name + ".csv")
                except Exception as e:
                    print(e)
                    break

                with open(csv_path, "rb") as f_in:
                    with gzip.open(gz_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                # Remove the original csv file
                output_path.unlink()
                shutil.rmtree(extract_dir)

                date -= datetime.timedelta(days=30)

    def fetch_ticker(
        self,
        symbol: str,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        timezone_delta: datetime.timedelta = datetime.timedelta(hours=9),
    ) -> pl.DataFrame:
        if start_date is None:
            start_date = datetime.datetime(1970, 1, 1)
        if end_date is None:
            end_date = datetime.datetime.today()
        pre_start_date = start_date - datetime.timedelta(days=31)
        post_end_date = end_date + datetime.timedelta(days=31)

        # file読み込み
        dfs: list[pl.DataFrame] = []
        ticker_file_list = sorted(self.data_dir.rglob(f"{symbol}_*.csv.gz"))
        for file_path in ticker_file_list:
            file_date = datetime.datetime.strptime(file_path.parent.name, "%Y%m")
            if pre_start_date <= file_date <= post_end_date:
                dfs.append(
                    pl.read_csv(
                        file_path,
                        has_header=False,
                        new_columns=["timestamp", "bid", "ask", "volume"],
                    )
                )
        timezone_delta += datetime.timedelta(hours=5)  # EST -> UTC -> JTC
        df = (
            pl.concat(dfs)
            .with_columns(
                pl.lit(symbol).alias("symbol"),
                pl.col("volume").alias("size"),
                ((pl.col("ask") + pl.col("bid")) / 2.0).alias("price"),
                pl.col("timestamp")
                .str.to_datetime("%Y%m%d %H%M%S%3f")
                .alias("datetime")
                + timezone_delta,
            )
            .filter(pl.col("datetime").is_between(start_date, end_date))
        )
        return df.sort("datetime")

    def fetch_ohlc(
        self,
        symbol: str,
        interval: datetime.timedelta,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        fill_missing_date: bool = False,
        fetch_interval: datetime.timedelta | None = datetime.timedelta(days=28),
    ) -> pl.DataFrame:
        if symbol not in self.available_tickers:
            raise ValueError(f"Invalid symbol: {symbol}")

        return super().fetch_ohlc(
            symbol,
            interval,
            start_date,
            end_date,
            fill_missing_date,
            fetch_interval=fetch_interval,
        )
