"""fetch_data_from_bitflyer.py"""

import asyncio
import datetime
import gzip
import threading
from pathlib import Path

from loguru import logger

import data_fetcher


def get_is_continue(thread: threading.Thread):
    def is_continue():
        return thread.is_alive()

    return is_continue


async def compress_csvs(
    data_dir: Path = data_fetcher.constants.PROJECT_ROOT / "data" / "bitflyer" / "tick",
    check_continue: callable = lambda: True,
):
    try:
        while check_continue():
            current_date = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y%m%d"
            )
            for dirpath in data_dir.glob("*"):
                if not dirpath.is_dir():
                    continue

                if dirpath.name < current_date:
                    for csv_path in dirpath.glob("*.csv"):
                        gz_path = csv_path.with_suffix(".csv.gz")
                        with open(csv_path, "rb") as f:
                            data = f.read()
                        with gzip.open(gz_path, "wb") as f:
                            f.write(data)
                        csv_path.unlink()
                        data_fetcher.logger.debug(f"Compressed {csv_path} to {gz_path}")
            await asyncio.sleep(60)

        data_fetcher.logger.debug("Finish csv search loop.")
    except:
        data_fetcher.logger.exception("Failed to compress csv files.")


if __name__ == "__main__":
    log_path = data_fetcher.constants.PROJECT_ROOT / "logs" / "bitflyer.log"
    try:
        log_path.parent.mkdir(exist_ok=True)
        logger.add(
            log_path,
            rotation="30 MB",
            format="[{time:YYYY-MM-DD HH:mm:ss} {level} {file} at line {line}] {message}",
            level="DEBUG",
        )

        fetcher = data_fetcher.bitflyer.BitflyerFetcher()
        thread = threading.Thread(target=fetcher.start_websocket)
        thread.daemon = True
        thread.start()

        asyncio.run(compress_csvs(check_continue=get_is_continue(thread)))

        thread.join()
    except:
        data_fetcher.logger.exception("Failed to fetch data from bitflyer.")
    data_fetcher.notification.notify_to_line("Bitflyer fetcher stopped.")
