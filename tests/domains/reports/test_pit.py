import datetime as dt

from data_fetcher.domains.reports.pit import filter_available, is_available_at

_JST = dt.timezone(dt.timedelta(hours=9))


def test_is_available_at_boundary_inclusive():
    as_of = dt.date(2026, 10, 1)
    assert is_available_at(dt.datetime(2026, 10, 1, 23, 59, 59, tzinfo=_JST), as_of) is True
    assert is_available_at(dt.datetime(2026, 10, 2, 0, 0, 0, tzinfo=_JST), as_of) is False


def test_is_available_at_assumes_jst_when_naive():
    as_of = dt.date(2026, 10, 1)
    assert is_available_at(dt.datetime(2026, 10, 1, 12, 0, 0), as_of) is True
    assert is_available_at(dt.datetime(2026, 10, 2, 12, 0, 0), as_of) is False


def test_filter_available_excludes_future_disclosures():
    class Item:
        def __init__(self, disclosed_at: dt.datetime) -> None:
            self.disclosed_at = disclosed_at

    as_of = dt.date(2026, 10, 1)
    items = [
        Item(dt.datetime(2026, 9, 30, 15, 0, tzinfo=_JST)),  # in
        Item(dt.datetime(2026, 10, 1, 15, 0, tzinfo=_JST)),  # in (same day)
        Item(dt.datetime(2026, 10, 2, 8, 0, tzinfo=_JST)),  # out (next day)
    ]
    result = filter_available(items, as_of, disclosed_at=lambda item: item.disclosed_at)
    assert len(result) == 2


def test_filter_available_excludes_correction_disclosed_after_as_of():
    """訂正開示は「訂正の開示日時」で判定する: 元の決算短信がas_of以前でも、
    その訂正がas_of後に出た場合は訂正後の値をスナップショットに混入させない
    (呼び出し側は訂正レコードのdisclosed_atをそのまま渡すだけでよい)。
    """

    class Correction:
        def __init__(self, disclosed_at: dt.datetime, value: float) -> None:
            self.disclosed_at = disclosed_at
            self.value = value

    as_of = dt.date(2026, 10, 1)
    original = Correction(dt.datetime(2026, 5, 10, 15, 0, tzinfo=_JST), value=100.0)
    correction = Correction(dt.datetime(2026, 10, 5, 15, 0, tzinfo=_JST), value=200.0)
    result = filter_available(
        [original, correction], as_of, disclosed_at=lambda item: item.disclosed_at
    )
    assert result == [original]
