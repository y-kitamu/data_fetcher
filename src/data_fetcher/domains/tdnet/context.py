"""iXBRLコンテキストIDから期間・連結区分などの軸情報を取り出す。"""

import re

from pydantic import BaseModel

# tdnetのcontext_idで使われる期間軸。REIT/投資法人の決算短信では
# Prior1QuarterInstant等の「Quarter」系が使われるため、Year/AccumulatedQ*/YTD/Interim系と
# あわせて定義する。
context_periods = [
    "CurrentYear",
    "CurrentQuarter",
    "CurrentAccumulatedQ1",
    "CurrentAccumulatedQ2",
    "CurrentAccumulatedQ3",
    "NextYear",
    "Next1Year",
    "Next2Year",
    "NextQuarter",
    "Next1Quarter",
    "Next2Quarter",
    "NextAccumulatedQ1",
    "NextAccumulatedQ2",
    "NextAccumulatedQ3",
    "PriorYear",
    "Prior1Year",
    "Prior2Year",
    "PriorQuarter",
    "Prior1Quarter",
    "Prior2Quarter",
    "PriorAccumulatedQ1",
    "PriorAccumulatedQ2",
    "PriorAccumulatedQ3",
    "CurrentYTD",
    "PriorYTD",
    "Prior1YTD",
    "Prior2YTD",
    "Interim",
    "Prior1Interim",
    "Prior2Interim",
]
_dividend_schedule_members = [
    "FirstQuarterMember",
    "SecondQuarterMember",
    "ThirdQuarterMember",
    "YearEndMember",
    "AnnualMember",
]
_consolidation_members = ["ConsolidatedMember", "NonConsolidatedMember"]
_previous_current_members = ["PreviousMember", "CurrentMember"]
_forecast_members = ["ResultMember", "ForecastMember", "UpperMember", "LowerMember"]

_CONTEXT_ID_RE = re.compile(
    "(?P<period>{period})(?P<duration_type>Instant|Duration)"
    "(?P<dividend_schedule>{dividend}|)"
    "(?P<consolidated>{consolidated}|)"
    "(?P<previous_current>{previous_current}|)"
    "(?P<forecast>{forecast}|)"
    "(?P<segment>_.*Member|)".format(
        period="|".join(context_periods),
        dividend="|".join(f"_{m}" for m in _dividend_schedule_members),
        consolidated="|".join(f"_{m}" for m in _consolidation_members),
        previous_current="|".join(f"_{m}" for m in _previous_current_members),
        forecast="|".join(f"_{m}" for m in _forecast_members),
    )
)


class ContextAxes(BaseModel):
    """context_idを分解した各軸。存在しない軸は空文字列(segmentのみNone)。"""

    period: str
    duration_type: str
    dividend_schedule: str
    consolidated: str
    previous_current: str
    forecast: str
    segment: str | None


def parse_context_id(context_id: str) -> ContextAxes:
    match = _CONTEXT_ID_RE.search(context_id)
    if match is None:
        raise ValueError(f"Invalid context_id: {context_id}")

    segment_raw = match.group("segment")
    segment = segment_raw[1:] if segment_raw else None

    return ContextAxes(
        period=match.group("period"),
        duration_type=match.group("duration_type"),
        dividend_schedule=match.group("dividend_schedule")[1:],
        consolidated=match.group("consolidated")[1:],
        previous_current=match.group("previous_current")[1:],
        forecast=match.group("forecast")[1:],
        segment=segment,
    )
