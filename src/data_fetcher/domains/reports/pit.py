"""Point-in-time filtering (section 0 of the report generator instructions):
a snapshot may only use data disclosed at or before 23:59:59 JST on as_of.
A correction's own disclosure datetime (not the original filing's) governs
whether the corrected value may be used - callers get this for free as long
as they pass the correction's own submitted_at/disclosed_at through this
filter like any other record.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from typing import TypeVar

_JST = dt.timezone(dt.timedelta(hours=9))

T = TypeVar("T")


def cutoff_datetime(as_of: dt.date) -> dt.datetime:
    """as_ofの日付の終わり(23:59:59 JST)。"""
    return dt.datetime.combine(as_of, dt.time(23, 59, 59), tzinfo=_JST)


def is_available_at(disclosed_at: dt.datetime, as_of: dt.date) -> bool:
    if disclosed_at.tzinfo is None:
        disclosed_at = disclosed_at.replace(tzinfo=_JST)
    return disclosed_at <= cutoff_datetime(as_of)


def filter_available(
    items: list[T], as_of: dt.date, *, disclosed_at: Callable[[T], dt.datetime]
) -> list[T]:
    return [item for item in items if is_available_at(disclosed_at(item), as_of)]
