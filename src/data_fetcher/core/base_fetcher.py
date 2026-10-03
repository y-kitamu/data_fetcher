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
        self.max_retry = 20
        self.current_retry = 0
        self._reconnect_delay: float | None = None

    def start_websocket(self):
        # on_error/on_close are invoked by websocket-client from inside run_forever().
        # They used to call start_websocket() directly, nesting a new run_forever()
        # inside the callback's own call stack on every reconnect; a run of
        # back-to-back failures eventually exceeded Python's recursion limit
        # (see gmo.log "maximum recursion depth exceeded"). Loop here instead so each
        # reconnect attempt returns to this frame rather than stacking a new one.
        while True:
            self.close_websocket()
            self.ws = websocket.WebSocketApp(
                f"{self.api_endpoint}",
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._reconnect_delay = None
            self.ws.run_forever()
            if self._reconnect_delay is None:
                break
            time.sleep(self._reconnect_delay)

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
            time.sleep(2)
        self.current_retry = 0

    def _on_close(self, ws, close_status_code, close_msg):
        logger.debug(
            f"Websocket closed. status_code: {close_status_code}, msg: {close_msg}"
        )
        if close_status_code == 1012:  # scheduled maintanance
            self._reconnect_delay = 60
        else:
            self.current_retry += 1
            self._reconnect_delay = 600 if self.current_retry <= self.max_retry else None

    def _on_error(self, ws, error):
        logger.error(error)
        self._reconnect_delay = 30

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
