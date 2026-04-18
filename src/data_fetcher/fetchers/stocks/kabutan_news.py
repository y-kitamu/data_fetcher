"""kabutan_news.py - Kabutan market news fetcher (date-based)."""

import datetime
import time
import urllib.parse

import polars as pl
import tqdm
from bs4 import BeautifulSoup
from loguru import logger

from ...core.session import get_session
from ..news_base import NEWS_COLUMNS, BaseNewsFetcher

_BASE_URL = "https://kabutan.jp"
_MARKET_NEWS_URL = _BASE_URL + "/news/marketnews/?date={date}&page={page}"
_REQUEST_SLEEP = 0.5


class KabutanNewsFetcher(BaseNewsFetcher):
    """Fetches market-wide news from Kabutan market news listing.

    Storage: data/news/kabutan/{YYYY-MM-DD}.csv
    One file per date. Skips dates that already have a saved file.
    Fetches all paginated results for each date.
    """

    SOURCE = "kabutan"

    def __init__(self) -> None:
        super().__init__()
        self.session = get_session(max_requests_per_second=2)
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )

    def _extract_body(self, url: str) -> str:
        """Extract article body text using requests + BeautifulSoup."""
        if not url:
            return ""
        try:
            resp = self.session.get(url, timeout=20)
            if resp.status_code != 200:
                logger.debug(f"HTTP {resp.status_code} for {url[:80]}")
                return ""
            soup = BeautifulSoup(resp.text, "html.parser")
            article_tag = soup.find("article")
            return article_tag.get_text(strip=True) if article_tag else ""
        except Exception as e:
            logger.debug(f"Body extraction failed for {url[:80]}: {e}")
            return ""

    def _scrape_date(self, date: datetime.date) -> list[dict]:
        """Scrape all news entries for a single date across all pages (no body yet)."""
        date_str = date.strftime("%Y%m%d")
        rows: list[dict] = []
        page = 1

        while True:
            url = _MARKET_NEWS_URL.format(date=date_str, page=page)
            try:
                resp = self.session.get(url, timeout=20)
            except Exception as e:
                logger.warning(f"Request failed: {url} ({e})")
                break

            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code}: {url}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            news_tables = soup.find_all("table", attrs={"class": "s_news_list"})
            if news_tables is None or len(news_tables) == 0:
                break  # No table means no more pages

            for news_table in news_tables:
                page_rows = news_table.find_all("tr")
                if not page_rows:
                    break

                for tr in page_rows:
                    tds = tr.find_all("td")
                    if len(tds) < 3:
                        continue

                    time_tag = tds[0].find("time")
                    if time_tag is None:
                        continue
                    try:
                        published_at = datetime.datetime.fromisoformat(
                            time_tag.attrs["datetime"]
                        ).isoformat()
                    except (KeyError, ValueError):
                        continue

                    cat_div = tds[1].find("div")
                    category = cat_div.get_text(strip=True) if cat_div else ""

                    a_tag = tds[2].find("a")
                    article_url = ""
                    if a_tag and a_tag.get("href"):
                        article_url = urllib.parse.urljoin(_BASE_URL, a_tag["href"])

                    if any([article_url == row["url"] for row in rows]):
                        continue

                    rows.append(
                        {
                            "published_at": published_at,
                            "source": "kabutan",
                            "symbol": "",
                            "title": tds[2].get_text(strip=True),
                            "body": "",
                            "url": article_url,
                            "category": category,
                        }
                    )

            time.sleep(_REQUEST_SLEEP)
            page += 1
        logger.debug(f"Scraped {page - 1} pages {len(rows)} articles for {date_str}")
        return rows

    def run(self, start_date: datetime.date, end_date: datetime.date) -> None:
        """Fetch kabutan market news for each date in [start_date, end_date].

        Phase 1: scrape article metadata (title, URL, datetime) via plain HTTP.
        Phase 2: extract article body text via a single Selenium session.
        Phase 3: persist results to per-date CSV files.
        """
        # ── Phase 1: collect rows for all missing dates ────────────────────
        pending: dict[datetime.date, list[dict]] = {}
        current = start_date
        while current <= end_date:
            csv_path = self.data_dir / f"{current.isoformat()}.csv"
            if csv_path.exists():
                current += datetime.timedelta(days=1)
                continue

            logger.info(f"Kabutan: scraping {current}")
            rows = self._scrape_date(current)
            if not rows:
                logger.info(f"Kabutan: no results for {current}")
                pl.DataFrame(
                    {col: [] for col in NEWS_COLUMNS},
                    schema={col: pl.Utf8 for col in NEWS_COLUMNS},
                ).write_csv(csv_path)
            else:
                pending[current] = rows
                logger.info(f"Kabutan: {len(rows)} articles collected for {current}")

            current += datetime.timedelta(days=1)

        if not pending:
            logger.info("Kabutan: no new articles to process.")
            return

        # ── Phase 2: extract bodies via requests + BeautifulSoup ─────────────
        total = sum(len(rows) for rows in pending.values())
        # PDF disclosure links cannot be rendered as article pages; skip them.
        article_count = sum(
            1
            for rows in pending.values()
            for row in rows
            if row["url"] and "/disclosures/pdf/" not in row["url"]
        )
        logger.info(f"Kabutan: extracting bodies for {article_count}/{total} articles…")
        for date, rows in tqdm.tqdm(pending.items(), desc="Kabutan news (body)"):
            for row in tqdm.tqdm(rows, desc=f"Processing {date}", leave=False):
                if row["url"] and "/disclosures/pdf/" not in row["url"]:
                    row["body"] = self._extract_body(row["url"])
        logger.info("Kabutan: body extraction complete.")

        # ── Phase 3: persist to CSV ──────────────────────────────────────────
        for date, rows in pending.items():
            csv_path = self.data_dir / f"{date.isoformat()}.csv"
            df = pl.DataFrame({col: [r[col] for r in rows] for col in NEWS_COLUMNS})
            df.write_csv(csv_path)
            logger.info(f"Kabutan: saved {len(df)} rows → {csv_path.name}")
