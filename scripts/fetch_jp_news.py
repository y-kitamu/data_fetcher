"""fetch_jp_news.py - Fetch JP stock news from Kabutan, GNews, and yfinance.

Collects news for the past 30 days by default.
Incremental: already-collected data is skipped automatically.

Usage:
    uv run python scripts/fetch_jp_news.py
"""

import argparse
import datetime

from loguru import logger

from data_fetcher.fetchers.stocks.kabutan_news import KabutanNewsFetcher


def main(days: int) -> None:
    end_date = datetime.date.today() - datetime.timedelta(
        days=1
    )  # avoid partial data for today
    start_date = end_date - datetime.timedelta(days=days)

    logger.info(f"Fetching JP news: {start_date} → {end_date}")

    logger.info("=== [1/3] Kabutan News ===")
    KabutanNewsFetcher().run(start_date, end_date)

    # logger.info("=== [2/3] GNews ===")
    # GNewsFetcher().run(start_date, end_date)

    # logger.info("=== [3/3] yfinance News ===")
    # YfinanceNewsFetcher().run(start_date, end_date)

    logger.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch JP stock news from multiple sources."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of past days to fetch (default: 30)",
    )
    args = parser.parse_args()

    main(days=args.days)
