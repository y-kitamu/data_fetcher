"""fetch_data_from_gmo.py"""

import argparse

from loguru import logger

import data_fetcher

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch forex data from GMO using WebSocket."
    )
    parser.add_argument(
        "--gzip", action="store_true", help="Run gzip compression for old data files."
    )
    args = parser.parse_args()

    # fetcher = data_fetcher.gmo.GMOFetcher()
    # run_debug(fetcher.download_all)
    if args.gzip:
        fetcher = data_fetcher.fetchers.GMOFetcherFX()
        fetcher.compress_old_data_files(offset_days=1)
    else:
        try:
            fetcher = data_fetcher.fetchers.GMOFetcherFX()
            fetcher.start_websocket()
        except Exception as e:
            logger.exception(f"Error occurred while running GMOFetcherFX - {e}")

        data_fetcher.notify_to_line("GMOFetcherFX has stopped unexpectedly.")
