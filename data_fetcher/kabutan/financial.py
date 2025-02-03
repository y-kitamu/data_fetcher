"""financial.py"""

import csv
import datetime
import re
from pathlib import Path

from bs4 import BeautifulSoup
from pydantic import BaseModel

from .util import convert_to_number


class FinancialStatement(BaseModel):
    code: str
    year: int
    month: int
    duration: int
    announce_date: datetime.date | None
    is_prediction: bool
    total_revenue: int | float | None
    operating_income: int | float | None
    ordinary_profit: int | float | None
    net_income: int | float | None
    eps: int | float | None
    divident: int | float | None

    def __hash__(self):
        return hash(
            (
                self.code,
                self.year,
                self.month,
                self.duration,
                self.announce_date,
                self.is_prediction,
            )
        )

    def __lt__(self, other):
        if self.year != other.year:
            return self.year < other.year
        if self.month != other.month:
            return self.month < other.month
        if self.duration != other.duration:
            return self.duration < other.duration
        if self.announce_date != other.announce_date:
            if self.announce_date is not None and other.announce_date is not None:
                return self.announce_date < other.announce_date
        return self.is_prediction < other.is_prediction

    def __gt__(self, other):
        if self.__lt__(other) or self.__eq__(other):
            return False
        return True

    def __eq__(self, other):
        keys = ["code", "year", "month", "duration", "announce_date", "is_prediction"]
        return all([getattr(self, key) == getattr(other, key) for key in keys])

    def to_csv_row(self):
        return [
            self.year,
            self.month,
            self.duration,
            (
                ""
                if self.announce_date is None
                else self.announce_date.strftime("%y/%m/%d")
            ),
            self.is_prediction,
            self.total_revenue,
            self.operating_income,
            self.ordinary_profit,
            self.net_income,
            self.eps,
            self.divident,
        ]

    @classmethod
    def from_csv(cls, code: str, row: list[str]):
        row = [col.strip() for col in row]
        return cls(
            code=code,
            year=int(row[0]),
            month=int(row[1]),
            duration=int(row[2]),
            announce_date=(
                None
                if row[3] == ""
                else datetime.datetime.strptime(row[3], "%y/%m/%d").date()
            ),
            is_prediction=row[4] == "True",
            total_revenue=convert_to_number(row[5]),
            operating_income=convert_to_number(row[6]),
            ordinary_profit=convert_to_number(row[7]),
            net_income=convert_to_number(row[8]),
            eps=convert_to_number(row[9]),
            divident=convert_to_number(row[10]),
        )

    @staticmethod
    def get_csv_header():
        return [
            "year",
            "month",
            "duration",
            "annoounce_date",
            "is_prediction",
            "total_revenue",
            "operating_income",
            "ordinary_profit",
            "net_income",
            "eps",
            "divident",
        ]


def results_to_csv(results: list[FinancialStatement], output_path: Path):
    """ """
    rows = [res.to_csv_row() for res in results]
    with open(output_path, "w", encoding="utf-8") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(FinancialStatement.get_csv_header())
        csv_writer.writerows(rows)


def get_annual_results(soup: BeautifulSoup, code: str):
    headers = [
        "決算期",
        "売上高",
        "営業益",
        "経常益",
        "最終益",
        "修正1株益",
        "修正1株配",
        "発表日",
    ]

    year_result_div = soup.find("div", {"class": "fin_year_result_d"})
    if year_result_div is None:
        return []
    regex = re.compile("(\d+)\.(\d+)")
    table = year_result_div.find("table")
    if table is None:
        return []
    prev_year = -1
    prev_month = -1
    indices = [
        headers.index(header.text) for header in table.find("thead").find_all("th")
    ]
    indices = [idx if idx < indices[0] else idx - 1 for idx in indices[1:]]

    results = []
    for row in table.find("tbody").find_all("tr"):
        th = row.find("th")
        if th is None:
            continue
        res = regex.search(th.text)
        if res is None:
            continue
        year, month = int(res.group(1)), int(res.group(2))
        duration = 12
        if prev_year > 0 and prev_month > 0:
            duration = (year - prev_year) * 12 + month - prev_month

        cols = [col.text for col in row.find_all("td")]
        if len(cols) < len(headers) - 1:
            continue

        results.append(
            FinancialStatement(
                code=code,
                year=year,
                month=month,
                duration=duration,
                announce_date=(
                    None
                    if cols[indices[6]] == "－"
                    else datetime.datetime.strptime(cols[indices[6]], "%y/%m/%d").date()
                ),
                is_prediction="予" in th.text,
                total_revenue=convert_to_number(cols[indices[0]]),
                operating_income=convert_to_number(cols[indices[1]]),
                ordinary_profit=convert_to_number(cols[indices[2]]),
                net_income=convert_to_number(cols[indices[3]]),
                eps=convert_to_number(cols[indices[4]]),
                divident=convert_to_number(cols[indices[5]]),
            )
        )
        prev_year, prev_month = year, month
    return results


def get_quarter_results(soup: BeautifulSoup, code: str):
    headers = [
        "決算期",
        "売上高",
        "営業益",
        "経常益",
        "最終益",
        "修正1株益",
        "売上営業損益率",
        "発表日",
    ]

    quarter_result_div = soup.find("div", {"class": "fin_quarter_result_d"})
    if quarter_result_div is None:
        return []
    regex = re.compile("(\d+)\.(\d+)-(\d+)")
    table = quarter_result_div.find("table")
    indices = [
        headers.index(header.text) for header in table.find("thead").find_all("th")
    ]
    indices = [idx if idx < indices[0] else idx - 1 for idx in indices[1:]]

    results = []
    for row in table.find("tbody").find_all("tr"):
        th = row.find("th")
        if th is None:
            continue
        res = regex.search(th.text)
        if res is None:
            continue
        year, start_month, end_month = (
            int(res.group(1)),
            int(res.group(2)),
            int(res.group(3)),
        )
        duration = end_month + 1 - start_month
        if duration < 0:
            year += 1
            duration += 12

        cols = [col.text for col in row.find_all("td")]
        if len(cols) < len(headers) - 1:
            continue

        results.append(
            FinancialStatement(
                code=code,
                year=2000 + year,
                month=end_month,
                duration=duration,
                announce_date=(
                    None
                    if cols[indices[6]] == "－"
                    else datetime.datetime.strptime(cols[indices[6]], "%y/%m/%d")
                ),
                is_prediction="予" in th.text,
                total_revenue=convert_to_number(cols[indices[0]]),
                operating_income=convert_to_number(cols[indices[1]]),
                ordinary_profit=convert_to_number(cols[indices[2]]),
                net_income=convert_to_number(cols[indices[3]]),
                eps=convert_to_number(cols[indices[4]]),
                divident=None,
            )
        )
    return results
