"""fetch_data_from_bitflyer.py"""

import argparse
import asyncio
import datetime
import gzip
import threading
import time
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
    parser = argparse.ArgumentParser(description="Fetch data from Bitflyer.")
    args = parser.parse_args()

    log_path = data_fetcher.constants.PROJECT_ROOT / "logs" / "bitflyer.log"
    try:
        threads = []
        while True:
            try:
                log_path.parent.mkdir(exist_ok=True)
                logger.add(
                    log_path,
                    rotation="30 MB",
                    format="[{time:YYYY-MM-DD HH:mm:ss} {level} {file} at line {line}] {message}",
                    level="DEBUG",
                )
                tickers = data_fetcher.fetchers.BitflyerBookFetcher().available_tickers
                for ticker in tickers:
                    if ticker in [thread[2] for thread in threads]:
                        continue
                    fetcher = data_fetcher.fetchers.BitflyerBookFetcher(
                        target_tickers=[ticker]
                    )
                    thread = threading.Thread(target=fetcher.start_websocket)
                    thread.daemon = True
                    thread.start()
                    threads.append((thread, fetcher, ticker))

                asyncio.run(
                    compress_csvs(
                        data_dir=fetcher.data_dir,
                        check_continue=get_is_continue(thread),
                    )
                )

                for thread, fetcher, ticker in threads:
                    thread.join(timeout=5)
            except Exception:
                data_fetcher.logger.exception("Failed to fetch data from bitflyer.")
            finally:
                for thread, fetcher, ticker in threads:
                    thread.join(timeout=5)

                threads = [t for t in threads if t[0].is_alive()]

            logger.debug("Restarting fetcher...")
            time.sleep(5)
    except Exception:
        data_fetcher.logger.exception("Bitflyer fetcher stopped unexpectedly.")

    data_fetcher.notify_to_line("Bitflyer fetcher stopped.")
