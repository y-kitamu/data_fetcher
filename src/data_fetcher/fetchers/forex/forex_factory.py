"""Forex Factory Calendar Data Fetcher

Fetches economic calendar data from Forex Factory website using curl_cffi.
curl_cffi impersonates a real browser's TLS fingerprint to bypass Cloudflare.
"""

import datetime
import re
import time
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests
from loguru import logger

from ...core.base_fetcher import BaseFetcher
from ...core.constants import PROJECT_ROOT


class ForexFactoryFetcher(BaseFetcher):
    """Fetcher for Forex Factory economic calendar data.

    Downloads historical economic calendar data including:
    - Event time
    - Currency
    - Impact level (Low/Medium/High)
    - Event name
    - Actual/Forecast/Previous values

    Examples:
        >>> fetcher = ForexFactoryFetcher()
        >>> # Fetch data for January 2024
        >>> df = fetcher.fetch(year=2024, month=1)
        >>> # Save to CSV
        >>> fetcher.save_calendar(year=2024, month=1)
    """

    BASE_URL = "https://www.forexfactory.com/calendar"

    def __init__(
        self,
        data_dir: Path = PROJECT_ROOT / "data" / "forex_factory",
    ):
        """Initialize the Forex Factory fetcher.

        Args:
            data_dir: Directory to save downloaded calendar data
        """
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

    @property
    def available_tickers(self) -> list[str]:
        """Not applicable for calendar data. Returns empty list."""
        return []

    def _get_month_abbr(self, month: int) -> str:
        """Convert month number to abbreviation (jan, feb, etc.)."""
        months = [
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
        ]
        return months[month - 1]

    def save_calendar(self, year: int, month: int) -> Path | None:
        date = datetime.date(year, month, 1)
        dfs = []
        while date.month == month:
            df = self.fetch(year, month, date.day)
            if not df.is_empty():
                dfs.append(df)
            date += datetime.timedelta(days=1)
            time.sleep(3)

        if not dfs:
            logger.warning(f"No data found for {year}-{month:02d}")
            return None

        output_path = self.data_dir / f"calendar_{year}_{month:02d}.csv"
        df = pl.concat(dfs)
        df.write_csv(output_path)
        return output_path

    def fetch(self, year: int, month: int, day: int) -> pl.DataFrame:
        """Fetch calendar data for a single day.

        Args:
            year: Year (e.g., 2024)
            month: Month (1-12)
            day: Day (1-31)

        Returns:
            Polars DataFrame with columns:
                - datetime: Event datetime
                - currency: Currency pair (e.g., USD, EUR, JPY)
                - impact: Impact level (Low, Medium, High, Holiday)
                - event: Event name
                - actual: Actual value
                - forecast: Forecast value
                - previous: Previous value
        """
        month_abbr = self._get_month_abbr(month)
        logger.info(f"Fetching Forex Factory calendar for {month_abbr} {day} {year}")
        url = f"{self.BASE_URL}?day={month_abbr}{day}.{year}"
        try:
            resp = cffi_requests.get(url, impersonate="chrome131", timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            all_data = self._parse_calendar_table(soup, year, month, day)

        except Exception as e:
            logger.error(
                f"Failed to fetch calendar data for {month_abbr} {day} {year}: {e}"
            )
            return pl.DataFrame()

        if not all_data:
            logger.warning(f"No calendar data found for {month_abbr} {day} {year}")
            return pl.DataFrame()

        # Convert to Polars DataFrame
        df = pl.DataFrame(all_data)

        # Filter to only include events in the target month
        df = df.filter(
            (pl.col("datetime").dt.month() == month)
            & (pl.col("datetime").dt.year() == year)
        )

        logger.info(f"Fetched {len(df)} calendar events for {month_abbr} {day} {year}")
        return df

    def _parse_calendar_table(
        self, soup: BeautifulSoup, year: int, month: int, day: int
    ) -> list[dict]:
        """Parse the calendar table from HTML.

        The table has no tbody; rows are direct children of table.
        Day breaker rows indicate the start of a new day.
        Data rows may omit time/date cells, inheriting from previous rows.

        Returns:
            List of dictionaries with calendar event data
        """
        events = []

        table = soup.find("table", {"class": "calendar__table"})
        if not table:
            logger.warning("Calendar table not found in HTML")
            return events

        current_date = datetime.date(year, month, day)
        time_str = ""

        for row in table.find_all("tr", {"class": "calendar__row"}):
            classes = row.get("class", [])

            # Day breaker row: extract the day number
            if "calendar__row--day-breaker" in classes:
                td = row.find("td")
                if td:
                    # Text like "TueMar 10" - extract the trailing number
                    m = re.search(r"(\d+)\s*$", td.get_text(strip=True))
                    if m:
                        current_date = datetime.date(year, month, int(m.group(1)))
                continue

            # Get time
            time_cell = row.find("td", class_="calendar__time")
            if time_cell:
                cell_str = time_cell.get_text(strip=True)
                if cell_str:
                    time_str = cell_str

            # Get currency
            currency_cell = row.find("td", class_="calendar__currency")
            currency = currency_cell.get_text(strip=True) if currency_cell else ""

            # Get impact
            impact_cell = row.find("td", class_="calendar__impact")
            impact = ""
            if impact_cell:
                impact_span = impact_cell.find("span")
                if impact_span:
                    span_classes = impact_span.get("class", [])
                    if "icon--ff-impact-red" in span_classes:
                        impact = "High"
                    elif (
                        "icon--ff-impact-ora" in span_classes
                        or "icon--ff-impact-yel" in span_classes
                    ):
                        impact = "Medium"
                    elif "icon--ff-impact-gra" in span_classes:
                        impact = "Low"
                    elif "icon--ff-impact-hol" in span_classes:
                        impact = "Holiday"

            # Get event name
            event_cell = row.find("td", class_="calendar__event")
            event_name = event_cell.get_text(strip=True) if event_cell else ""

            # Get actual/forecast/previous values
            actual_cell = row.find("td", class_="calendar__actual")
            actual = actual_cell.get_text(strip=True) if actual_cell else ""

            forecast_cell = row.find("td", class_="calendar__forecast")
            forecast = forecast_cell.get_text(strip=True) if forecast_cell else ""

            previous_cell = row.find("td", class_="calendar__previous")
            previous = previous_cell.get_text(strip=True) if previous_cell else ""

            # Construct datetime
            if time_str and time_str.lower() not in ("all day", "tentative", "day 1"):
                try:
                    time_obj = datetime.datetime.strptime(time_str, "%I:%M%p").time()
                    event_datetime = datetime.datetime.combine(current_date, time_obj)
                except ValueError:
                    event_datetime = datetime.datetime.combine(
                        current_date, datetime.time(0, 0)
                    )
            else:
                event_datetime = datetime.datetime.combine(
                    current_date, datetime.time(0, 0)
                )

            if not event_name:
                continue

            events.append(
                {
                    "datetime": event_datetime,
                    "currency": currency,
                    "impact": impact,
                    "event": event_name,
                    "actual": actual,
                    "forecast": forecast,
                    "previous": previous,
                }
            )

        return events
