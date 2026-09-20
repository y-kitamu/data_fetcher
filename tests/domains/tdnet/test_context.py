import pytest

from data_fetcher.domains.tdnet.context import parse_context_id


def test_simple_instant_context():
    axes = parse_context_id("CurrentYearInstant")
    assert axes.period == "CurrentYear"
    assert axes.duration_type == "Instant"
    assert axes.consolidated == ""
    assert axes.segment is None


def test_consolidated_duration_context():
    axes = parse_context_id("CurrentYearDuration_ConsolidatedMember")
    assert axes.period == "CurrentYear"
    assert axes.duration_type == "Duration"
    assert axes.consolidated == "ConsolidatedMember"
    assert axes.previous_current == ""
    assert axes.forecast == ""


def test_reit_prior_quarter_context():
    """REIT/投資法人の決算短信で使われるQuarter系context。以前はcontext_idsに未定義で例外になっていた。"""
    axes = parse_context_id("Prior1QuarterInstant")
    assert axes.period == "Prior1Quarter"
    assert axes.duration_type == "Instant"


def test_previous_current_axis_is_captured():
    """予想修正報告書のPrevious/Current軸。以前は正規表現で捕捉されても格納されていなかった。"""
    axes = parse_context_id("CurrentYearDuration_PreviousMember")
    assert axes.previous_current == "PreviousMember"


def test_full_combination_of_axes():
    axes = parse_context_id(
        "CurrentYearDuration_AnnualMember_ConsolidatedMember_CurrentMember_ForecastMember_SomeSegmentsMember"
    )
    assert axes.period == "CurrentYear"
    assert axes.dividend_schedule == "AnnualMember"
    assert axes.consolidated == "ConsolidatedMember"
    assert axes.previous_current == "CurrentMember"
    assert axes.forecast == "ForecastMember"
    assert axes.segment == "SomeSegmentsMember"


def test_unknown_context_id_raises():
    with pytest.raises(ValueError):
        parse_context_id("FilingDateInstant")
