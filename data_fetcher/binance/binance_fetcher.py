"""binance_fetcher.py
"""

import json
from pathlib import Path
import datetime
import shutil
import gzip

import polars as pl
from dateutil.relativedelta import relativedelta

from ..session import get_session
from ..constants import PROJECT_ROOT


def get_available_tickers() -> list[str]:
    """
    Reference: https://github.com/binance/binance-public-data/blob/master/shell/fetch-all-trading-pairs.sh
    """
    session = get_session()
    res = session.get("https://api.binance.com/api/v3/exchangeInfo")
    info = json.loads(res.content)
    symbols = [data["symbol"] for data in info["symbols"]]
    return symbols


def zip_to_gz(
    byte_object: bytes,
    zip_filename: str,
    work_dir: Path,
    output_path: Path,
    header: list[str] = [],
):
    work_dir.mkdir(exist_ok=True, parents=True)
    # zipオブジェクトをファイルに保存
    zip_file = work_dir / "tmp.csv.zip"
    idx = 0
    while zip_file.exists():
        zip_file = work_dir / f"{zip_file.stem}_{idx}.zip"
        idx += 1
    with open(zip_file, "wb") as f:
        _ = f.write(byte_object)
    # zipを解凍し、csvファイル作成
    shutil.unpack_archive(zip_file, extract_dir=work_dir)
    csv_path = work_dir / zip_filename.replace(".zip", ".csv")
    # headerを付加
    if len(header) > 0:
        df = pl.read_csv(csv_path, has_header=False, new_columns=header)
        df.write_csv(csv_path)
    # csvファイルをgzに圧縮して保存
    with open(csv_path, "rb") as f_in:
        with gzip.open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    # 中間ファイルを削除
    zip_file.unlink()
    csv_path.unlink()


class BinanceFetcher:
    _API_ENDPOINT = "https://data.binance.vision/data/spot/"
    _DATATYPE_HEADERS = {
        "trades": [
            "tradeId",
            "price",
            "quantity",
            "quoteQty",
            "time",
            "isBuyerMaker",
            "isBestMatch",
        ],
        "aggTrades": [
            "aggTradeId",
            "price",
            "quantity",
            "first tradeId",
            "last tradeId",
            "Timestamp",
            "isBuyerMaker",
            "isBestMatch",
        ],
        "klines": [
            "Open time",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "Close time",
            "Quote asset volume",
            "Number of trades",
            "Taker buy base asset volume",
            "Taker buy quote asset volume",
            "Ignore",
        ],
    }

    def __init__(
        self,
        data_dir: Path = PROJECT_ROOT / f"data/binance",
        target_tickers: list[str] = [],
    ):
        self.data_dir = data_dir
        self.work_dir = PROJECT_ROOT / "data/tmp"
        self.session = get_session()
        self.available_tickers = get_available_tickers()
        if len(target_tickers) == 0:
            self.target_tickers = [
                "BTCUSDT",
                "ETHUSDT",
                "XRPUSDT",
                "BNBUSDT",
                "SOLUSDT",
                "DOGEUSDT",
                "ADAUSDT",
                "TRXUSDT",
                "AVAXUSDT",
                "LINKUSDT",
                "WBTCUSDT",
                "XLMUSDT",
                "DOTUSDT",
            ]
        else:
            self.target_tickers = target_tickers

    def get_output_stem(
        self, ticker: str, date: datetime.date, data_type: str, monthly: bool = False
    ):
        date_str = date.strftime("%Y-%m") if monthly else date.strftime("%Y-%m-%d")
        return f"{ticker}-{data_type}-{date_str}"

    def download_all_trades_monthly(self):
        """
        https://data.binance.vision/data/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2024-11.zip
        """
        for ticker in self.target_tickers:
            for trade_type in ["trades", "aggTrades"]:
                date = datetime.date.today() - relativedelta(months=1)
                while True:
                    output_path = self.data_dir / "tick/{}/{}.csv.gz".format(
                        date.strftime("%Y%m"),
                        self.get_output_stem(ticker, date, trade_type, monthly=True),
                    )
                    if output_path.exists():
                        break
                    df = self.download_ticker(
                        ticker, date, trade_type, output_path.parent, duration="monthly"
                    )
                    if len(df) == 0:
                        break
                    date -= relativedelta(months=1)
            print(ticker)

    def download_all_klines(self):
        # for data_type in self._DATATYPE_HEADERS.keys():
        intervals = ["1s"]
        for interval in intervals:
            for ticker in self.target_tickers:
                date = datetime.date.today() - datetime.timedelta(days=2)
                while True:
                    output_path = self.data_dir / "klines/{date}/{stem}.csv.gz".format(
                        date=date.strftime("%Y%m%d"),
                        stem=self.get_output_stem(ticker, date, interval),
                    )
                    if output_path.exists():
                        break
                    df = self.download_klines(
                        ticker, date, interval, output_path.parent
                    )
                    if len(df) == 0:
                        break
                    date -= datetime.timedelta(days=1)

    def download_klines(
        self,
        ticker: str,
        date: datetime.date,
        interval: str,
        output_dir: Path | None = None,
        overwrite: bool = False,
    ) -> pl.DataFrame:
        valid_intervals = [
            "1s",
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
        ]
        if interval not in valid_intervals:
            raise RuntimeError(
                "Invalid interval : {}. interval should be one of {}".format(
                    interval, valid_intervals
                )
            )
        zip_filename = f"{self.get_output_stem(ticker, date, interval)}.zip"
        target_url = (
            self._API_ENDPOINT + f"daily/klines/{ticker}/{interval}/{zip_filename}"
        )
        if output_dir is None:
            output_dir = self.data_dir / "kilnes" / date.strftime("%Y%m%d")
        output_dir.mkdir(exist_ok=True, parents=True)
        output_path = output_dir / zip_filename.replace(".zip", ".csv.gz")

        return self.download_impl(
            output_path, target_url, zip_filename, ticker, date, "klines", overwrite
        )

    def download_ticker(
        self,
        ticker: str,
        date: datetime.date,
        data_type: str,
        output_dir: Path | None = None,
        overwrite: bool = False,
        duration: str = "monthly",
    ) -> pl.DataFrame:
        if data_type not in self._DATATYPE_HEADERS.keys():
            raise RuntimeError(
                "Invalid datatype : {}. data_type should be one of {}".format(
                    data_type, list(self._DATATYPE_HEADERS.keys())
                )
            )
        is_monthly = duration == "monthly"
        zip_filename = (
            f"{self.get_output_stem(ticker, date, data_type, monthly=is_monthly)}.zip"
        )
        target_url = (
            self._API_ENDPOINT + f"{duration}/{data_type}/{ticker}/{zip_filename}"
        )
        if output_dir is None:
            output_dir = self.data_dir / "tick" / date.strftime("%Y%m")
        output_dir.mkdir(exist_ok=True, parents=True)
        output_path = output_dir / zip_filename.replace(".zip", ".csv.gz")
        return self.download_impl(
            output_path, target_url, zip_filename, ticker, date, data_type, overwrite
        )

    def download_impl(
        self,
        output_path: Path,
        target_url: str,
        zip_filename: str,
        ticker: str,
        date: datetime.date,
        data_type: str,
        overwrite: bool,
    ) -> pl.DataFrame:
        if overwrite or not output_path.exists():
            response = self.session.get(target_url)
            if response.status_code != 200:
                print(response.status_code, ticker, date, target_url)
                return pl.DataFrame()
            zip_to_gz(
                response.content,
                zip_filename=zip_filename,
                work_dir=self.work_dir,
                output_path=output_path,
                header=self._DATATYPE_HEADERS[data_type],
            )
        return pl.read_csv(output_path)


if __name__ == "__main__":
    fetcher = BinanceFetcher()
    fetcher.download_all_trades_monthly()
