"""bitflyer_fetcher.py
bitFlyerの約定データ・板情報をWebSocket経由で取得する
"""

import datetime
import gzip
import json
import shutil
import time
from pathlib import Path

import requests
import websocket
from loguru import logger

from ...core.base_fetcher import BaseFetcher, BaseWebsocketFetcher
from ...core.book_stats import calculate_book_stats
from ...core.constants import PROJECT_ROOT


def get_available_tickers():
    res = requests.get("https://api.bitflyer.com/v1/markets")
    if res.status_code == 200:
        return [row["product_code"] for row in json.loads(res.text)]
    return []


class BitflyerFetcher(BaseFetcher):
    _API_ENDPOINT = "wss://ws.lightstream.bitflyer.com/json-rpc"
    HEADER = [
        "id",
        "side",
        "price",
        "size",
        "exec_date",
        "buy_child_order_acceptance_id",
        "sell_child_order_acceptance_id",
    ]

    def __init__(self, data_dir=PROJECT_ROOT / "data" / "bitflyer" / "tick"):
        super().__init__()
        self.data_dir = data_dir
        self.data_dir.mkdir(exist_ok=True, parents=True)
        self._available_tickers = get_available_tickers()
        self.ws = None

    @property
    def available_tickers(self):
        return self._available_tickers

    def start_websocket(self):
        self.close_websocket()
        self.ws = websocket.WebSocketApp(
            f"{self._API_ENDPOINT}",
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self.ws.run_forever()

    def close_websocket(self):
        if self.ws is not None:
            self.ws.close()
        self.ws = None

    def _get_output_path(self, ticker: str, date: datetime.date, suffix=".csv.gz"):
        date_str = date.strftime("%Y%m%d")
        return self.data_dir / date_str / f"{date_str}_{ticker}{suffix}"

    def _on_open(self, ws):
        """"""
        for ticker in self.available_tickers:
            output_json = json.dumps(
                {
                    "method": "subscribe",
                    "params": {"channel": f"lightning_executions_{ticker}"},
                }
            )
            ws.send(output_json)

    def _on_close(self, ws, close_status_code, close_msg):
        logger.debug(
            f"Websocket closed. status_code: {close_status_code}, msg: {close_msg}"
        )
        if close_status_code == 1012:  # scheduled maintanance
            time.sleep(60)
            self.start_websocket()

    def _on_error(self, ws, error):
        logger.error(error)
        time.sleep(30)
        self.start_websocket()

    def _on_message(self, ws, message):
        message = json.loads(message)["params"]
        # logger.debug(json.dumps(message))
        try:
            channel = message["channel"]
            data = message["message"]

            ticker = channel.split("_", 2)[-1]
            for execution in data:
                exec_dt = datetime.datetime.fromisoformat(execution["exec_date"])
                output_path = self._get_output_path(ticker, exec_dt, suffix=".csv")
                output_path.parent.mkdir(exist_ok=True)
                if not output_path.exists():
                    with open(output_path, "w") as f:
                        f.write(",".join(self.HEADER) + "\n")
                with open(output_path, "a") as f:
                    f.write(
                        ",".join([str(execution[key]) for key in self.HEADER]) + "\n"
                    )
        except Exception:
            logger.exception("Error occured in on_message")


class BitflyerBookFetcher(BaseWebsocketFetcher):
    """bitFlyer Lightning板情報の差分データを取得するFetcher.

    WebSocket (JSON-RPC 2.0) の lightning_board_{product_code} チャネルを購読し、
    板情報の差分データをCSV形式で保存する。

    保存形式: local_timestamp,mid_price,side,price,size
        - local_timestamp: ローカルのタイムスタンプ (ISO 8601)
        - mid_price: 仲値
        - side: "bid" or "ask"
        - price: 価格
        - size: 数量 (0の場合はその価格の注文が板から消えたことを意味する)
    """

    HEADER = ["local_timestamp", "mid_price", "side", "price", "size"]

    def __init__(
        self,
        output_root_dir: Path = PROJECT_ROOT / "data" / "bitflyer" / "book",
        target_tickers: list[str] | None = None,
        num_levels: int = 5,
        warmup_seconds: int = 5,
    ):
        self._available_tickers = get_available_tickers()
        super().__init__(
            data_dir=output_root_dir,
            api_endpoint="wss://ws.lightstream.bitflyer.com/json-rpc",
            on_open_message=json.dumps(
                {
                    "method": "subscribe",
                    "params": {"channel": "lightning_board_{ticker}"},
                }
            ),
            target_tickers=target_tickers or self.available_tickers,
        )
        self.num_levels = num_levels
        self.warmup_seconds = warmup_seconds
        self.start_date = None
        self.warmup_completed = False
        self.book = {"bids": [], "asks": []}

    @property
    def available_tickers(self) -> list[str]:
        return self._available_tickers

    def compress_old_data_files(self, offset_days: int = 2):
        """古いデータファイルをgzip圧縮する。

        Args:
            offset_days: 何日前以前のファイルを圧縮対象にするか
        """
        cutoff_date = datetime.datetime.today() - datetime.timedelta(days=offset_days)
        for file_path in self.data_dir.glob("*/**/*.csv"):
            file_date_str = file_path.parent.name
            try:
                file_date = datetime.datetime.strptime(file_date_str, "%Y%m%d")
            except ValueError:
                continue
            if file_date < cutoff_date:
                self._compress_file(file_path)

    def _compress_file(self, file_path: Path):
        """ファイルをgzip圧縮して元ファイルを削除する。"""
        gzip_path = file_path.with_suffix(file_path.suffix + ".gz")
        with open(file_path, "rb") as f_in:
            with gzip.open(gzip_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        file_path.unlink()
        logger.info(f"Compressed {file_path} to {gzip_path}")

    def _on_message(self, ws, message):
        """板情報の差分メッセージを処理してCSVに書き込む。

        受信メッセージ例 (JSON-RPC 2.0):
        {
            "jsonrpc": "2.0",
            "method": "channelMessage",
            "params": {
                "channel": "lightning_board_BTC_JPY",
                "message": {
                    "mid_price": 35625,
                    "bids": [{"price": 33350, "size": 1}],
                    "asks": [{"price": 36000, "size": 0.5}]
                }
            }
        }
        """
        if self.start_date is None:
            self.start_date = datetime.datetime.now()

        if not self.warmup_completed:
            self.warmup_completed = (
                datetime.datetime.now() - self.start_date
            ).total_seconds() > self.warmup_seconds

        data = {}

        try:
            parsed = json.loads(message)
            params = parsed["params"]
            channel = params["channel"]
            symbol = channel.replace("lightning_board_", "")
            timestamp = datetime.datetime.now()
            updates = params["message"]

            data["mid_price"] = updates["mid_price"]
            data["symbol"] = symbol
            data["received_timestamp"] = timestamp.isoformat(timespec="milliseconds")

            for side in ["bids", "asks"]:
                for update in updates[side]:
                    price = update["price"]
                    size = update["size"]
                    if size == 0:
                        self.book[side] = [
                            order
                            for order in self.book[side]
                            if order["price"] != price
                        ]
                    else:
                        existing_order = next(
                            (
                                order
                                for order in self.book[side]
                                if order["price"] == price
                            ),
                            None,
                        )
                        if existing_order:
                            existing_order["size"] = size
                        else:
                            self.book[side].append({"price": price, "size": size})
                self.book[side] = sorted(
                    self.book[side], key=lambda x: x["price"], reverse=(side == "bids")
                )[
                    : max(self.num_levels * 3, 500)
                ]  # 統計量計算のためにある程度深く板を保持する

                for i in range(self.num_levels):
                    data[f"{side[:-1]}_price_{i + 1}"] = (
                        self.book[side][i]["price"]
                        if i < len(self.book[side])
                        else None
                    )
                    data[f"{side[:-1]}_size_{i + 1}"] = (
                        self.book[side][i]["size"] if i < len(self.book[side]) else None
                    )

            stats = calculate_book_stats(
                self.book["bids"], self.book["asks"], data["mid_price"]
            )
            data.update(stats)

            headers = (
                ["received_timestamp", "symbol", "mid_price"]
                + [f"ask_price_{i + 1}" for i in range(self.num_levels)]
                + [f"ask_size_{i + 1}" for i in range(self.num_levels)]
                + [f"bid_price_{i + 1}" for i in range(self.num_levels)]
                + [f"bid_size_{i + 1}" for i in range(self.num_levels)]
                + list(stats.keys())
            )

            if self.warmup_completed:
                self.write_data(
                    ticker=symbol, exec_dt=timestamp, data=data, header=headers
                )
        except Exception:
            logger.exception("Error processing board message")
