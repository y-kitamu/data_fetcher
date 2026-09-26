import datetime as dt

from data_fetcher.domains.tdnet.capital_action_extraction import (
    build_capital_action_record,
    extract_split_ratio,
)
from data_fetcher.domains.tdnet.disclosure_list import DisclosureRow

_JST = dt.timezone(dt.timedelta(hours=9))


def _row(title: str) -> DisclosureRow:
    return DisclosureRow(
        code="1301",
        disclosed_at=dt.datetime(2026, 9, 1, 15, 0, tzinfo=_JST),
        title=title,
        pdf_url="https://example.com/x.pdf",
        xbrl_url=None,
        has_xbrl=False,
        page_date="20260901",
    )


def test_extract_split_ratio_for_forward_split():
    assert extract_split_ratio("1株につき2株の割合をもって株式分割") == 2.0


def test_extract_split_ratio_for_reverse_split():
    assert extract_split_ratio("10株を1株に併合") == 0.1


def test_extract_split_ratio_returns_none_when_not_in_title():
    assert extract_split_ratio("株式分割に関するお知らせ") is None


def test_build_capital_action_record_for_split_sets_type_and_ratio():
    record = build_capital_action_record(_row("1株につき2株の割合をもって株式分割"), "T17")
    assert record is not None
    assert record.type == "split"
    assert record.split_ratio == 2.0
    assert record.extraction_method == "title_regex"


def test_build_capital_action_record_for_reverse_split_sets_type():
    record = build_capital_action_record(_row("10株を1株に併合"), "T17")
    assert record is not None
    assert record.type == "reverse_split"
    assert record.split_ratio == 0.1


def test_build_capital_action_record_without_extractable_ratio_is_null_with_title_only():
    record = build_capital_action_record(_row("株式分割に関するお知らせ"), "T17")
    assert record is not None
    assert record.split_ratio is None
    assert record.extraction_method == "title_only"
    assert record.effective_date is None


def test_build_capital_action_record_returns_none_for_untracked_category():
    assert build_capital_action_record(_row("代表取締役の異動に関するお知らせ"), "T31") is None
