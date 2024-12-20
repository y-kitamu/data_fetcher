"""fetch_data_from_gmo.py
"""

import data_fetcher

if __name__ == "__main__":
    fetcher = data_fetcher.gmo.GMOFethcer()
    fetcher.download_all()
