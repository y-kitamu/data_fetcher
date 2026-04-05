"""news_base.py - Base class and shared utilities for news fetchers."""

import datetime
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, ClassVar

from loguru import logger
from newspaper import Article

from ..core.base_fetcher import BaseFetcher
from ..core.constants import PROJECT_ROOT

if TYPE_CHECKING:
    from selenium.webdriver.remote.webdriver import WebDriver

NEWS_COLUMNS = ["published_at", "source", "symbol", "title", "body", "url", "category"]
_BODY_WORKERS = 5


def extract_body_with_driver(url: str, driver: "WebDriver") -> str:
    """Extract article body using an existing Selenium WebDriver.

    Loads the URL, waits for the ``article`` CSS selector to appear, then returns
    its text.  If ``driver.get()`` times out (page load timeout), the function
    still attempts extraction from the partially loaded DOM before giving up.
    Falls back to an empty string on any unrecoverable error.
    """
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    if not url:
        return ""
    try:
        driver.get(url)
    except TimeoutException:
        # Page load timed out; DOM may be partially available – try extraction anyway.
        logger.debug(f"Page load timed out for {url[:80]}; attempting partial extraction…")
    except Exception as e:
        logger.debug(f"Selenium navigation failed for {url[:80]}: {e}")
        return ""
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "article"))
        )
        article = driver.find_element(By.CSS_SELECTOR, "article")
        return article.text.strip()
    except Exception as e:
        logger.debug(f"Selenium body extraction failed for {url[:80]}: {e}")
        return ""


def extract_body(url: str) -> str:
    """Extract article body text from a URL using newspaper4k."""
    if not url:
        return ""
    try:
        article = Article(url, language="ja", request_timeout=10)
        article.download()
        article.parse()
        return article.text or ""
    except Exception as e:
        logger.debug(f"Body extraction failed: {url[:80]} ({e})")
        return ""


def extract_bodies_concurrent(
    urls: list[str], max_workers: int = _BODY_WORKERS
) -> list[str]:
    """Extract bodies for multiple URLs concurrently. Returns list aligned to input."""
    bodies = [""] * len(urls)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(extract_body, url): i for i, url in enumerate(urls)
        }
        for future in as_completed(futures):
            bodies[futures[future]] = future.result()
    return bodies


class BaseNewsFetcher(BaseFetcher):
    """Abstract base class for all news fetchers.

    Subclasses must set SOURCE and implement run().
    Storage layout is determined by each subclass independently.
    """

    SOURCE: ClassVar[str]

    def __init__(self) -> None:
        self.data_dir = PROJECT_ROOT / "data" / "news" / self.SOURCE
        self.data_dir.mkdir(exist_ok=True, parents=True)

    @property
    def available_tickers(self) -> list[str]:
        return []

    @abstractmethod
    def run(self, start_date: datetime.date, end_date: datetime.date) -> None:
        """Collect news for the given date range, skip already-collected data."""
        ...
