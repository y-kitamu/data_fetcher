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
    parser.add_argument(
        "--fx", action="store_true", help="Fetch forex data instead of crypto data."
    )
    parser.add_argument(
        "--crypto", action="store_true", help="Fetch crypto data instead of forex data."
    )
    parser.add_argument(
        "--histrical", action="store_true", help="Fetch historical data"
    )

    args = parser.parse_args()

    if args.gzip:
        fetcher = data_fetcher.fetchers.GMOFetcherFX()
        fetcher.compress_old_data_files(offset_days=1)
    elif args.histrical:
        fetcher = data_fetcher.fetchers.GMOFetcher()
        fetcher.download_all()
    else:
        try:
            if args.fx:
                fetcher = data_fetcher.fetchers.GMOFetcherFX()
                fetcher.start_websocket()
            if args.crypto:
                fetcher = data_fetcher.fetchers.GMOBookFetcher()
                fetcher.start_websocket()
        except Exception as e:
            logger.exception(f"Error occurred while running GMOFetcherFX - {e}")

        data_fetcher.notify_to_line("GMOFetcherFX has stopped unexpectedly.")
