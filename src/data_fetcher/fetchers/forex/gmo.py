import datetime
import gzip
import json
import shutil
import time
from pathlib import Path

from loguru import logger

from ...core import BaseWebsocketFetcher
from ...core.constants import PROJECT_ROOT


class GMOFetcher(BaseWebsocketFetcher):
    def __init__(self, output_root_dir: Path = PROJECT_ROOT / "data/gmo/tick"):
        super().__init__(
            data_dir=output_root_dir,
            api_endpoint="wss://forex-api.coin.z.com/ws/public/v1",
            on_open_message=json.dumps(
                {"command": "subscribe", "channel": "ticker", "symbol": "{ticker}"}
            ),
            target_tickers=self.available_tickers,
        )

    def compress_old_data_files(self, offset_days: int = 2):
        """Compress old data files using gzip."""
        start_date = datetime.datetime.today() - datetime.timedelta(days=offset_days)
        for file_path in self.data_dir.glob("*/**/*.csv"):
            file_date_str = file_path.parent.name
            file_date = datetime.datetime.strptime(file_date_str, "%Y%m%d")
            if file_date < start_date:
                self._compress_file(file_path)

    def _compress_file(self, file_path: Path):
        gzip_path = file_path.with_suffix(file_path.suffix + ".gz")
        with open(file_path, "rb") as f_in:
            with gzip.open(gzip_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        file_path.unlink()
        logger.info(f"Compressed {file_path} to {gzip_path}")

    @property
    def available_tickers(self) -> list[str]:
        """Get list of available ticker symbols.

        Returns:
            list[str]: List of available ticker symbols
        """
        return [
            "USD_JPY",
            "EUR_JPY",
            "GBP_JPY",
            "AUD_JPY",
            "NZD_JPY",
            "CAD_JPY",
            "CHF_JPY",
            "TRY_JPY",
            "ZAR_JPY",
            "MXN_JPY",
            "EUR_USD",
            "GBP_USD",
            "AUD_USD",
            "NZD_USD",
        ]

    def _on_open(self, ws):
        logger.info("WebSocket connection opening...")
        for ticker in self.target_tickers:
            ws.send(self.on_open_message.replace(self.placeholder, ticker))
            logger.info(f"Subscribed to {ticker}")
            time.sleep(2)
        logger.info("Subscribed to tickers.")

    def _on_message(self, ws, message):
        try:
            header = ["symbol", "ask", "bid", "timestamp", "status"]
            data = json.loads(message)
            exec_dt = datetime.datetime.fromisoformat(data["timestamp"])
            self.write_data(data["symbol"], exec_dt, data, header)
        except Exception as e:
            logger.error(f"Error processing message: {message}")
            raise e
