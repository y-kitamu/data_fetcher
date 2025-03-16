"""bitflyer_fether.py"""

import datetime
import json

import requests
import websocket

from ..base_fetcher import BaseFetcher
from ..constants import PROJECT_ROOT
from ..logging import logger


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

    def _on_error(self, ws, error):
        logger.error(error)
        self.start_websocket()

    def _on_message(self, ws, message):
        message = json.loads(message)["params"]
        logger.debug(json.dumps(message))
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
