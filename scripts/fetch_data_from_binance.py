"""fetch_data_from_binance.py
"""

import data_fetcher

if __name__ == "__main__":
    fetcher = data_fetcher.binance.BinanceFetcher()
    fetcher.download_all()
