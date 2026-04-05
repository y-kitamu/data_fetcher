"""gnews_fetcher.py - Google News (GNews) Japan stock news fetcher."""

import datetime
import time
from typing import Any

import polars as pl
from dateutil.parser import parse as parse_date
from gnews import GNews
from loguru import logger

from ...core.selenium_options import get_driver
from ..news_base import NEWS_COLUMNS, BaseNewsFetcher, extract_body_with_driver

# General queries for Japan stock market news (no per-symbol query to keep volume manageable)
_JP_QUERIES = [
    "日本株 株式市場",
    "日経平均 株価",
]
_REQUEST_SLEEP = 2.0  # seconds between GNews API calls


def _parse_gnews_date(date_str: str) -> str:
    """Parse RFC 2822 date string from GNews to ISO format."""
    try:
        return parse_date(date_str).isoformat()
    except Exception:
        return datetime.datetime.now().isoformat()


class GNewsFetcher(BaseNewsFetcher):
    """Fetches Japanese market news via Google News (GNews).

    Storage: data/news/gnews/{YYYY-MM-DD}.csv
    One file per day. symbol column is empty (general market news).
    Skips days that already have a saved file.
    """

    SOURCE = "gnews"

    def _fetch_day(self, date: datetime.date) -> list[dict]:
        """Fetch all matching news articles for a single calendar day."""
        next_day = date + datetime.timedelta(days=1)
        gnews = GNews(
            language="ja",
            country="JP",
            start_date=(date.year, date.month, date.day),
            end_date=(next_day.year, next_day.month, next_day.day),
            max_results=100,
        )

        all_articles: list[dict[str, Any]] = []
        for query in _JP_QUERIES:
            try:
                results = gnews.get_news(query) or []
                all_articles.extend(results)
                time.sleep(_REQUEST_SLEEP)
            except Exception as e:
                logger.warning(f"GNews query failed: {query!r} ({e})")

        # Deduplicate by URL
        seen_urls: set[str] = set()
        unique: list[dict[str, Any]] = []
        for a in all_articles:
            url = a.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique.append(a)

        return [
            {
                "published_at": _parse_gnews_date(a.get("published date", "")),
                "source": "gnews",
                "symbol": "",
                "title": a.get("title", ""),
                # GNews returns Google News proxy URLs; article body cannot be
                # extracted via plain HTTP. Use description as body (title+publisher).
                "body": a.get("description", ""),
                "url": a.get("url", ""),
                "category": "",
            }
            for a in unique
            if a.get("url")
        ]

    def run(self, start_date: datetime.date, end_date: datetime.date) -> None:
        """Fetch GNews for each day in [start_date, end_date], skipping existing days.

        Phase 1: fetch article metadata (title, description, proxy URL) via GNews API.
        Phase 2: resolve proxy URLs and extract bodies via a single Selenium session.
          - Proxy URL is resolved to the real article URL.
          - body = extracted article text if available and not a paywall message,
            else falls back to the GNews description field.
        """
        # ── Phase 1: collect rows for all missing days ────────────────────────
        pending: dict[datetime.date, list[dict]] = {}
        current = start_date
        while current <= end_date:
            csv_path = self.data_dir / f"{current.isoformat()}.csv"
            if csv_path.exists():
                current += datetime.timedelta(days=1)
                continue

            logger.info(f"GNews: fetching {current}")
            rows = self._fetch_day(current)

            if not rows:
                logger.info(f"GNews: no results for {current}")
                pl.DataFrame(
                    {col: [] for col in NEWS_COLUMNS},
                    schema={col: pl.Utf8 for col in NEWS_COLUMNS},
                ).write_csv(csv_path)
            else:
                pending[current] = rows
                logger.info(f"GNews: {len(rows)} articles collected for {current}")

            current += datetime.timedelta(days=1)
            time.sleep(_REQUEST_SLEEP)

        if not pending:
            return

        # ── Phase 2: Selenium URL resolution and body extraction ──────────────
        total = sum(len(rows) for rows in pending.values())
        logger.info(
            f"GNews: resolving URLs and extracting bodies for {total} articles via Selenium…"
        )
        for rows in pending.values():
            for row in rows:
                try:
                    with get_driver() as driver:
                        if not row["url"]:
                            continue
                        # Navigate to Google News proxy URL; browser follows JS redirect.
                        # Guard against page-load timeouts on individual proxy URLs.
                        try:
                            driver.get(row["url"])
                        except Exception as nav_err:
                            logger.debug(
                                f"GNews: proxy URL navigation error ({nav_err}); trying current URL"
                            )
                        # Wait up to 6 seconds for JS redirect away from Google News
                        for _ in range(12):
                            time.sleep(0.5)
                            if "news.google.com" not in driver.current_url:
                                break
                        final_url = driver.current_url
                        if "news.google.com" not in final_url:
                            row["url"] = final_url  # Update to resolved URL
                            # Try article body extraction from the final URL
                            body = extract_body_with_driver(final_url, driver)
                            # Keep description if body is empty or looks like a paywall message
                            if (
                                body
                                and len(body) >= 50
                                and "会員限定" not in body[:100]
                            ):
                                row["body"] = body
                except Exception as e:
                    logger.warning(
                        f"GNews: Selenium URL resolution failed ({e}); using proxy URLs and descriptions."
                    )
        logger.info("GNews: Selenium URL resolution complete.")

        # ── Phase 3: persist to CSV ───────────────────────────────────────────
        for date, rows in pending.items():
            csv_path = self.data_dir / f"{date.isoformat()}.csv"
            df = pl.DataFrame({col: [r[col] for r in rows] for col in NEWS_COLUMNS})
            df.write_csv(csv_path)
            logger.info(f"GNews: saved {len(df)} rows → {csv_path.name}")
