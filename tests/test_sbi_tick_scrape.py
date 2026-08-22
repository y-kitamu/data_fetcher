"""Tests for the SBI 歩み値 scrape/reconciliation path.

The two halves of the same defect are covered here:

* :func:`collect_all_tick_data` used to key rows by their absolute pixel
  position, which is not stable across scroll positions (``offsetTop`` is
  integer-rounded, ``translateY`` is not) — the same trade was recorded
  several times.
* :meth:`SBIReader._attach_direction` repairs the files already on disk by
  taking the official CSV export as the authoritative trade list and pulling
  only ``is_uptick`` off the scraped one.
"""

import datetime
import sys
import types
from pathlib import Path

import polars as pl
import pytest

from data_fetcher.readers.sbi import SBIReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))


@pytest.fixture(scope="module")
def collect_all_tick_data():
    """``scripts/fetch_data_from_sbi`` imports selenium/gmail at module level;
    stub what it only needs at import time."""
    stub_names = (
        "selenium",
        "selenium.webdriver",
        "selenium.webdriver.common",
        "selenium.webdriver.common.action_chains",
        "selenium.webdriver.common.by",
        "selenium.webdriver.common.keys",
        "selenium.webdriver.support",
        "selenium.webdriver.support.ui",
        "selenium.webdriver.support.expected_conditions",
    )
    for name in stub_names:
        sys.modules[name] = types.ModuleType(name)
    by = sys.modules["selenium.webdriver.common.by"]
    by.By = types.SimpleNamespace(CSS_SELECTOR="css selector", ID="id",
                                  TAG_NAME="tag name", LINK_TEXT="link text",
                                  CLASS_NAME="class name")
    for mod, attr in (
        ("selenium.webdriver", "Remote"),
        ("selenium.webdriver.common.action_chains", "ActionChains"),
        ("selenium.webdriver.common.keys", "Keys"),
        ("selenium.webdriver.support.ui", "WebDriverWait"),
    ):
        setattr(sys.modules[mod], attr, object)
    sys.modules["selenium.webdriver"].webdriver = sys.modules["selenium.webdriver"]

    import fetch_data_from_sbi

    return fetch_data_from_sbi.collect_all_tick_data


class _FakeGrid:
    """A CDK fixed-size virtual scroll viewport, faithful in the ways that matter.

    ``translateY`` is exactly ``rendered_start * item_size`` (what CDK writes),
    while each row's ``offsetTop`` is integer-rounded (what the browser
    returns) — that mismatch is what used to break the dedup key.
    """

    def __init__(self, rows, item_size=23.5, rendered=40, client_height=500):
        self._rows = rows
        self._item_size = item_size
        self._rendered = rendered
        self.client_height = client_height
        self.scroll_height = int(len(rows) * item_size)
        self._scroll_top = 0

    @property
    def _start(self) -> int:
        max_start = max(0, len(self._rows) - self._rendered)
        return min(int(self._scroll_top / self._item_size), max_start)

    def snapshot(self) -> dict:
        start = self._start
        window = self._rows[start : start + self._rendered]
        return {
            "translateY": start * self._item_size,
            "rowHeight": (round((len(window) - 1) * self._item_size) / (len(window) - 1))
            if len(window) > 1
            else self._item_size,
            "rows": [
                {
                    "ordinal": j,
                    "price": row["price"],
                    "volume": row["volume"],
                    "amount": row["amount"],
                    "isUptick": row["isUptick"],
                    "time": row["time"],
                }
                for j, row in enumerate(window)
            ],
        }


class _FakeDriver:
    def __init__(self, grid):
        self._grid = grid

    def execute_script(self, script, *args):
        if "scrollTop = arguments[1]" in script:
            self._grid._scroll_top = args[1]
            return None
        if "scrollTop = 0" in script:
            self._grid._scroll_top = 0
            return None
        if "scrollHeight" in script:
            return self._grid.scroll_height
        if "clientHeight" in script:
            return self._grid.client_height
        return self._grid.snapshot()


class _FakeElement:
    def __init__(self, grid):
        self._grid = grid

    def find_element(self, by, selector):
        return self


def _make_rows(n: int) -> list[dict]:
    return [
        {
            "price": f"{5000 + (i % 7)}",
            "volume": "100",
            "amount": "0.5",
            "isUptick": i % 3 == 0,
            "time": f"09:{i // 60 % 60:02d}:{i % 60:02d}",
        }
        for i in range(n)
    ]


@pytest.mark.parametrize("item_size", [23.5, 24.0, 27.33, 21.6])
def test_every_row_is_collected_exactly_once(collect_all_tick_data, item_size):
    """No duplicates and no gaps, whatever the (possibly fractional) row height.

    Keying on the rounded pixel position produced both: duplicates from the
    ±1px jitter, and — once divided by a slightly-off measured row height —
    collisions between genuinely different rows near the bottom of the grid.
    """
    rows = _make_rows(3000)
    grid = _FakeGrid(rows, item_size=item_size)
    df = collect_all_tick_data(_FakeDriver(grid), _FakeElement(grid))

    assert df.height == len(rows)
    # 行の並びも保たれている (index 0 = グリッド最上段 = 最新の約定)
    assert df["time"].to_list() == [r["time"] for r in rows]
    assert df["is_uptick"].to_list() == [r["isUptick"] for r in rows]


def test_attach_direction_drops_scrape_duplicates():
    """公式 CSV にある約定だけが残り、向きは歩み値の並び順に引き当てられる。"""
    official = pl.DataFrame(
        {
            "price": [100.0, 101.0, 100.0],
            "volume": [200.0, 300.0, 200.0],
            "amount": [1.0, 2.0, 1.0],
            "time": [datetime.time(9, 0, 0)] * 3,
            "time_in_seconds": [32400] * 3,
        }
    )
    # スクレイプ側は 1 本目と 3 本目が重複して 5 行になっている
    html = pl.DataFrame(
        {
            "price": [100.0, 101.0, 100.0, 100.0, 100.0],
            "volume": [200.0, 300.0, 200.0, 200.0, 200.0],
            "amount": [1.0, 2.0, 1.0, 1.0, 1.0],
            "is_uptick": [True, False, True, True, True],
            "time": [datetime.time(9, 0, 0)] * 5,
            "time_in_seconds": [32400] * 5,
        }
    )

    out = SBIReader._attach_direction(official, html)

    assert out.height == 3
    assert out["volume"].sum() == 700.0
    assert out["is_uptick"].to_list() == [True, False, True]


def test_attach_direction_leaves_unmatched_trades_null():
    """歩み値側に無い約定は None のまま。片側に倒すと不均衡統計が歪む。"""
    official = pl.DataFrame(
        {
            "price": [100.0, 999.0],
            "volume": [200.0, 100.0],
            "amount": [1.0, 1.0],
            "time": [datetime.time(9, 0, 0)] * 2,
            "time_in_seconds": [32400] * 2,
        }
    )
    html = pl.DataFrame(
        {
            "price": [100.0],
            "volume": [200.0],
            "amount": [1.0],
            "is_uptick": [True],
            "time": [datetime.time(9, 0, 0)],
            "time_in_seconds": [32400],
        }
    )

    out = SBIReader._attach_direction(official, html)

    assert out["is_uptick"].to_list() == [True, None]
