"""fetch_jp_news.py - Fetch JP stock news from Kabutan, GNews, and yfinance.

Collects news for the past 30 days by default.
Incremental: already-collected data is skipped automatically.

Usage:
    uv run python scripts/fetch_jp_news.py
"""

import datetime

from loguru import logger

from data_fetcher.fetchers.stocks.gnews_fetcher import GNewsFetcher
from data_fetcher.fetchers.stocks.kabutan_news import KabutanNewsFetcher
from data_fetcher.fetchers.stocks.yfinance_news import YfinanceNewsFetcher


def main() -> None:
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=30)

    logger.info(f"Fetching JP news: {start_date} → {end_date}")

    logger.info("=== [1/3] Kabutan News ===")
    KabutanNewsFetcher().run(start_date, end_date)

    logger.info("=== [2/3] GNews ===")
    GNewsFetcher().run(start_date, end_date)

    logger.info("=== [3/3] yfinance News ===")
    YfinanceNewsFetcher().run(start_date, end_date)

    logger.info("Done.")


if __name__ == "__main__":
    main()
