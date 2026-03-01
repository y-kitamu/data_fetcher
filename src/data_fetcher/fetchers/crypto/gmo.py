"""gmo_fetcher.py
gmoの過去データを取得する
"""

import datetime
import io
import json
from pathlib import Path

import polars as pl
import requests
from loguru import logger
from tqdm import tqdm

from ...core.base_fetcher import BaseFetcher, BaseWebsocketFetcher
from ...core.book_stats import calculate_book_stats
from ...core.constants import PROJECT_ROOT
from ...core.session import get_session


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
    def available_tickers(self) -> list[str]:
        return self._available_tickers

    def get_latest_date(self, symbol: str) -> datetime.datetime:
        if symbol not in self.available_tickers:
            raise ValueError(f"{symbol} is not available")

        ticker_file_list = sorted(self.data_dir.rglob(f"*_{symbol}.csv.gz"))
        if len(ticker_file_list) == 0:
            raise ValueError(f"No data for {symbol}")
        return datetime.datetime.strptime(ticker_file_list[-1].parent.name, "%Y%m%d")

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
        logger.warning("Failed to fetch available tickers. {}".format(response.text))
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
            logger.warning(
                f"Failed to download data: {response.status_code} - {self._API_ENDPOINT}{path}"
            )
            logger.debug(f"Response content: {response.content}")
            return pl.DataFrame()

        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with output_path.open("wb") as f:
                f.write(response.content)
        try:
            return pl.read_csv(io.BytesIO(response.content))
        except Exception as e:
            logger.error(f"Failed to parse CSV: {e}")
            return pl.DataFrame()

    def start_websocket(self):
        pass


class GMOBookFetcher(BaseWebsocketFetcher):
    def __init__(
        self,
        output_root_dir: Path = PROJECT_ROOT / "data" / "gmo" / "book",
        num_levels: int = 5,
    ):
        super().__init__(
            data_dir=output_root_dir,
            api_endpoint="wss://api.coin.z.com/ws/public/v1",
            on_open_message=json.dumps(
                {
                    "command": "subscribe",
                    "channel": "orderbooks",
                    "symbol": "{ticker}",
                }
            ),
            target_tickers=self.available_tickers,
        )
        self.num_levels = num_levels

    @property
    def available_tickers(self):
        return [
            "BTC_JPY",
            "ETH_JPY",
            "BCH_JPY",
            "LTC_JPY",
            "XRP_JPY",
        ]

    def _on_message(self, ws, message):
        """
        {
          "channel":"orderbooks",
          "asks": [
            {"price": "455659","size": "0.1"},
            {"price": "455658","size": "0.2"}
          ],
          "bids": [
            {"price": "455665","size": "0.1"},
            {"price": "455655","size": "0.3"}
          ],
          "symbol": "BTC",
          "timestamp": "2018-03-30T12:34:56.789Z"
        }
        """
        try:
            raw_data = json.loads(message)
            data = {}
            data["timestamp"] = raw_data["timestamp"]
            data["received_timestamp"] = datetime.datetime.now().isoformat()
            data["symbol"] = raw_data["symbol"]
            timestamp = datetime.datetime.fromisoformat(raw_data["timestamp"])
            asks = sorted(raw_data["asks"], key=lambda x: float(x["price"]))
            bids = sorted(
                raw_data["bids"], key=lambda x: float(x["price"]), reverse=True
            )

            # calculate book stats using all available levels
            parsed_asks = [
                {"price": float(a["price"]), "size": float(a["size"])} for a in asks
            ]
            parsed_bids = [
                {"price": float(b["price"]), "size": float(b["size"])} for b in bids
            ]
            mid_price = (
                (parsed_asks[0]["price"] + parsed_bids[0]["price"]) / 2
                if parsed_asks and parsed_bids
                else 0.0
            )

            stats = calculate_book_stats(parsed_bids, parsed_asks, mid_price)
            data.update(stats)

            headers = (
                ["timestamp", "received_timestamp", "symbol"]
                + [f"ask_price_{i + 1}" for i in range(self.num_levels)]
                + [f"ask_size_{i + 1}" for i in range(self.num_levels)]
                + [f"bid_price_{i + 1}" for i in range(self.num_levels)]
                + [f"bid_size_{i + 1}" for i in range(self.num_levels)]
                + list(stats.keys())
            )

            for i in range(self.num_levels):
                data[f"ask_price_{i + 1}"] = (
                    float(asks[i]["price"]) if i < len(asks) else None
                )
                data[f"ask_size_{i + 1}"] = (
                    float(asks[i]["size"]) if i < len(asks) else None
                )
                data[f"bid_price_{i + 1}"] = (
                    float(bids[i]["price"]) if i < len(bids) else None
                )
                data[f"bid_size_{i + 1}"] = (
                    float(bids[i]["size"]) if i < len(bids) else None
                )
            self.write_data(data["symbol"], timestamp, data, headers)
        except Exception as e:
            logger.exception(f"Failed to parse message: {message}")
            raise e
