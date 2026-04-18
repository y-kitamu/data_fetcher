"""yfinance_news.py - yfinance news fetcher for JP stocks."""

import datetime
import time

import polars as pl
import tqdm
import yfinance as yf
from loguru import logger

from ...core.ticker_list import get_jp_ticker_list
from ..news_base import NEWS_COLUMNS, BaseNewsFetcher

_REQUEST_SLEEP = 0.5


def _parse_yfinance_item(item: dict, symbol: str) -> dict | None:
    """Parse a single yfinance news item from the new API structure (v0.2.x).

    The new yfinance returns: {'id': ..., 'content': {...}}
    content keys: title, summary, pubDate, canonicalUrl, provider, ...
    """
    content = item.get("content") or {}
    title = content.get("title", "")
    if not title:
        return None

    summary = content.get("summary", "")
    pub_date_str = content.get("pubDate", "")
    try:
        published_at = datetime.datetime.fromisoformat(
            pub_date_str.replace("Z", "+00:00")
        ).isoformat()
    except (ValueError, AttributeError):
        return None

    canonical = content.get("canonicalUrl") or {}
    url = canonical.get("url", "")
    if not url:
        return None

    return {
        "published_at": published_at,
        "source": "yfinance",
        "symbol": symbol,
        "title": title,
        "body": summary,
        "url": url,
        "category": "",
    }


def _row_date(row: dict) -> datetime.date:
    return datetime.datetime.fromisoformat(row["published_at"]).date()


class YfinanceNewsFetcher(BaseNewsFetcher):
    """Fetches recent news for all JP stocks via yfinance Ticker.news.

    Storage: data/news/yfinance/{YYYY-MM-DD}.csv
    One file per publish date. Articles are grouped by their published_at date
    and saved to the corresponding date file. Existing files are merged and
    deduplicated by URL.
    Note: yfinance.news only returns recent articles regardless of date range.
    The 'body' column is populated with the article summary from yfinance.
    """

    SOURCE = "yfinance"

    def run(self, start_date: datetime.date, end_date: datetime.date) -> None:
        """Fetch news for all JP symbols and save one CSV file per publish date.

        If a date's CSV already exists, new rows are merged in and deduplicated
        by URL (keeping the first occurrence).
        """
        all_dates = [
            start_date + datetime.timedelta(days=i)
            for i in range((end_date - start_date).days + 1)
        ]

        target_start = datetime.datetime.combine(
            start_date, datetime.time.min, tzinfo=datetime.timezone.utc
        )
        target_end = datetime.datetime.combine(
            end_date + datetime.timedelta(days=1),
            datetime.time.min,
            tzinfo=datetime.timezone.utc,
        )

        tickers = get_jp_ticker_list()
        rows: list[dict] = []
        seen_urls: set[str] = set()

        for symbol in tqdm.tqdm(tickers, desc="yfinance news"):
            try:
                news_items = yf.Ticker(f"{symbol}.T").news or []
            except Exception as e:
                logger.debug(f"yfinance.news failed: {symbol} ({e})")
                continue

            for item in news_items:
                parsed = _parse_yfinance_item(item, symbol)
                if parsed is None:
                    continue
                url = parsed["url"]
                if url in seen_urls:
                    continue
                try:
                    pub_dt = datetime.datetime.fromisoformat(parsed["published_at"])
                except ValueError:
                    continue
                if not (target_start <= pub_dt < target_end):
                    continue
                seen_urls.add(url)
                rows.append(parsed)

            time.sleep(_REQUEST_SLEEP)

        # Group articles by publish date
        by_date: dict[datetime.date, list[dict]] = {d: [] for d in all_dates}
        for row in rows:
            try:
                d = _row_date(row)
            except (ValueError, KeyError):
                continue
            if d in by_date:
                by_date[d].append(row)

        for date, date_rows in by_date.items():
            csv_path = self.data_dir / f"{date.isoformat()}.csv"
            if date_rows:
                new_df = pl.DataFrame(
                    {col: [r[col] for r in date_rows] for col in NEWS_COLUMNS}
                )
            else:
                new_df = pl.DataFrame(
                    {col: [] for col in NEWS_COLUMNS},
                    schema={col: pl.Utf8 for col in NEWS_COLUMNS},
                )
            if csv_path.exists():
                try:
                    existing = pl.read_csv(csv_path, infer_schema_length=0)
                    df = pl.concat([existing, new_df], how="diagonal_relaxed").unique(
                        subset=["url"], keep="first"
                    )
                except Exception:
                    df = new_df
            else:
                df = new_df
            df.write_csv(csv_path)
            logger.info(f"yfinance: saved {len(df)} rows → {csv_path.name}")
