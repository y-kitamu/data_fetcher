"""Forex Factory Calendar Data Fetcher

Fetches economic calendar data from Forex Factory website using Selenium.
Note: Forex Factory uses Cloudflare protection, requiring browser automation.
"""

import datetime
import time
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup
from loguru import logger
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ...core.base_fetcher import BaseFetcher
from ...core.constants import PROJECT_ROOT
from ...core.selenium_options import get_driver


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

        output_path = self.data_dir / f"calendar_{year}_{month:02d}.csv"
        df = pl.concat(dfs)
        df.write_csv(output_path)
        return output_path

    def fetch(self, year: int, month: int, day: int) -> pl.DataFrame:
        """Fetch calendar data for specified period.

        Args:
            year: Year (e.g., 2024)
            month: Month (1-12)

        Returns:
            Polars DataFrame with columns:
                - datetime: Event datetime
                - currency: Currency pair (e.g., USD, EUR, JPY)
                - impact: Impact level (Low, Medium, High, Holiday)
                - event: Event name
                - actual: Actual value
                - forecast: Forecast value
                - previous: Previous value

        Raises:
            requests.RequestException: If HTTP request fails
        """
        month_abbr = self._get_month_abbr(month)
        logger.info(f"Fetching Forex Factory calendar for {month_abbr} {day} {year}")
        url = f"{self.BASE_URL}?day={month_abbr}{day}.{year}"
        try:
            with get_driver() as driver:
                driver.get(url)
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "calendar__table"))
                )
                # Get page source and parse with BeautifulSoup
                page_source = driver.execute_script(
                    "return document.documentElement.outerHTML;"
                )
                time.sleep(3)

            soup = BeautifulSoup(page_source, "html.parser")

            # Parse using the month from week_start
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

        Returns:
            List of dictionaries with calendar event data
        """
        events = []

        # Find the calendar table
        table = soup.find("table", {"class": "calendar__table"})
        if not table:
            logger.warning("Calendar table not found in HTML")
            return events

        for tbody in table.find_all("tbody"):
            day_row = tbody.find("tr", {"class": "calendar__row--day-breaker"})
            day = int(day_row.get_text(strip=True).split(" ")[-1])
            current_date = datetime.date(year, month, day)

            time_str = ""
            for row in tbody.find_all("tr", {"class": "calendar__row"}):
                if "calendar__row--day-breaker" in row.get("class", []):
                    continue

                # Get time
                time_cell = row.find("td", class_="calendar__time")
                if time_cell:
                    cell_str = time_cell.get_text(strip=True)
                    if cell_str != "":
                        time_str = cell_str

                # Get currency
                currency_cell = row.find("td", class_="calendar__currency")
                currency = currency_cell.get_text(strip=True) if currency_cell else ""

                # Get impact
                impact_cell = row.find("td", class_="calendar__impact")
                impact = ""
                if impact_cell:
                    # Impact is indicated by span classes
                    impact_span = impact_cell.find("span")
                    if impact_span:
                        classes = impact_span.get("class", [])
                        if "icon--ff-impact-red" in classes:
                            impact = "High"
                        elif (
                            "icon--ff-impact-ora" in classes
                            or "icon--ff-impact-yel" in classes
                        ):
                            impact = "Medium"
                        elif "icon--ff-impact-gra" in classes:
                            impact = "Low"
                        elif "icon--ff-impact-hol" in classes:
                            impact = "Holiday"

                # Get event name
                event_cell = row.find("td", class_="calendar__event")
                event_name = event_cell.get_text(strip=True) if event_cell else ""

                # Get actual value
                actual_cell = row.find("td", class_="calendar__actual")
                actual = actual_cell.get_text(strip=True) if actual_cell else ""

                # Get forecast value
                forecast_cell = row.find("td", class_="calendar__forecast")
                forecast = forecast_cell.get_text(strip=True) if forecast_cell else ""

                # Get previous value
                previous_cell = row.find("td", class_="calendar__previous")
                previous = previous_cell.get_text(strip=True) if previous_cell else ""

                # Construct datetime
                event_datetime = None
                if time_str and time_str.lower() not in ["all day", "tentative", ""]:
                    try:
                        # Parse time (format: "12:30am" or "12:30pm")
                        time_obj = datetime.datetime.strptime(
                            time_str, "%I:%M%p"
                        ).time()
                        event_datetime = datetime.datetime.combine(
                            current_date, time_obj
                        )
                    except ValueError:
                        # If time parsing fails, use date only
                        event_datetime = datetime.datetime.combine(
                            current_date, datetime.time(0, 0)
                        )
                else:
                    event_datetime = datetime.datetime.combine(
                        current_date, datetime.time(0, 0)
                    )

                print(
                    event_datetime,
                    currency,
                    impact,
                    event_name,
                    actual,
                    forecast,
                    previous,
                )
                # Skip rows without event name
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
