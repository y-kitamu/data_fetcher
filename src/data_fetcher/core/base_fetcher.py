"""base_fetcher.py"""

import datetime
import time
from pathlib import Path

import websocket
from loguru import logger


class BaseFetcher:
    """Base class for all data fetchers.

    Provides common interface for fetching financial data from various sources.
    """

    @property
    def available_tickers(self) -> list[str]:
        """Get list of available ticker symbols.

        Returns:
            list[str]: List of available ticker symbols
        """
        raise NotImplementedError


class BaseWebsocketFetcher(BaseFetcher):
    def __init__(
        self,
        data_dir: Path,
        api_endpoint: str,
        on_open_message: str,
        target_tickers: list[str] | None = None,
        placeholder: str = "{ticker}",
    ):
        super().__init__()
        self.data_dir = data_dir
        self.data_dir.mkdir(exist_ok=True, parents=True)
        self.api_endpoint = api_endpoint
        self.ws = None
        self.on_open_message = on_open_message
        self.placeholder = placeholder
        self.target_tickers = target_tickers or self.available_tickers

    def start_websocket(self):
        self.close_websocket()
        self.ws = websocket.WebSocketApp(
            f"{self.api_endpoint}",
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
        for ticker in self.target_tickers:
            ws.send(self.on_open_message.replace(self.placeholder, ticker))

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
        raise NotImplementedError

    def write_data(
        self, ticker: str, exec_dt: datetime.datetime, data: dict, header: list[str]
    ):
        output_path = self._get_output_path(ticker, exec_dt, suffix=".csv")
        output_path.parent.mkdir(exist_ok=True)
        if not output_path.exists():
            with open(output_path, "w") as f:
                f.write(",".join(header) + "\n")
        with open(output_path, "a") as f:
            f.write(",".join([str(data[key]) for key in header]) + "\n")
