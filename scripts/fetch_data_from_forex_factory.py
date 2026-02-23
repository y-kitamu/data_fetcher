#!/usr/bin/env python3
"""Fetch economic calendar data from Forex Factory.

This script downloads historical economic calendar data from Forex Factory
and saves it to CSV files.

Examples:
    # Fetch data for a specific month
    python fetch_data_from_forex_factory.py --year 2024 --month 1

    # Fetch data for a date range
    python fetch_data_from_forex_factory.py --start-date 2024-01-01 --end-date 2024-03-31

    # Fetch data for the last 3 months
    python fetch_data_from_forex_factory.py --last-months 3
"""

import argparse
import datetime
from pathlib import Path

import dateutil.relativedelta as relativedelta
from loguru import logger

from data_fetcher.fetchers import ForexFactoryFetcher


def main(year: int, month: int):
    """Main function."""

    # Initialize fetcher
    fetcher = ForexFactoryFetcher()

    # Single month mode
    logger.info(f"Fetching data for {year}-{month:02d}")
    output_path = fetcher.save_calendar(year, month)
    if output_path:
        logger.info(f"Data saved to: {output_path}")
    else:
        logger.warning(f"No data found for {year}-{month:02d}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch economic calendar data from Forex Factory"
    )

    # Single month mode
    parser.add_argument("--year", type=int, help="Year to fetch (e.g., 2024)")
    parser.add_argument("--month", type=int, help="Month to fetch (1-12)")

    # Output directory
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory (default: data/forex_factory)",
    )
    args = parser.parse_args()

    # main(args.year, args.month)
    start_date = datetime.date(2013, 1, 1)
    end_date = datetime.datetime.now().date()

    while start_date <= end_date:
        main(start_date.year, start_date.month)
        start_date += relativedelta.relativedelta(months=1)
