"""fetch_data_from_histdata.py"""

import data_fetcher

if __name__ == "__main__":
    fetcher = data_fetcher.histdata.HistDataFetcher()
    fetcher.download_all()
