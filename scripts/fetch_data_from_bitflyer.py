"""fetch_data_from_bitflyer.py"""

import asyncio
import datetime
import gzip
import threading
from pathlib import Path

import data_fetcher


async def compress_csvs(
    data_dir: Path = data_fetcher.constants.PROJECT_ROOT / "data" / "bitflyer" / "tick",
):
    while True:
        current_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
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
        await asyncio.sleep(60 * 60)


if __name__ == "__main__":
    try:
        fetcher = data_fetcher.bitflyer.BitflyerFetcher()
        thread = threading.Thread(target=fetcher.start_websocket)
        thread.daemon = True
        thread.start()

        asyncio.run(compress_csvs())
        data_fetcher.logger.exception("Failed to compress csv files.")

        thread.join()
    except:
        data_fetcher.logger.exception("Failed to fetch data from bitflyer.")
    data_fetcher.notification.notify_to_line("Bitflyer fetcher stopped.")
