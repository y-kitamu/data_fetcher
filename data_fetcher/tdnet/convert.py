"""convert.py"""

import calendar
import datetime
import re
from collections import defaultdict
from pathlib import Path

import polars as pl
from arelle import Cntlr
from dateutil.relativedelta import relativedelta

from .constants import target_english_labels
from .excel import get_target_taxonomy

context_ids = [
    "CurrentYear",
    "CurrentAccumulatedQ1",
    "CurrentAccumulatedQ2",
    "CurrentAccumulatedQ3",
    "NextYear",
    "Next2Year",
    "NextAccumulatedQ1",
    "NextAccumulatedQ2",
    "NextAccumulatedQ3",
    "PriorYear",
    "PriorAccumulatedQ1",
    "PriorAccumulatedQ2",
    "PriorAccumulatedQ3",
]
AnnualDividendPaymentScheduleAxi = [
    "FirstQuarterMember",
    "SecondQuarterMember",
    "ThirdQuarterMember",
    "YearEndMember",
    "AnnualMember",
]
ConsolidatedNonconsolidatedAxis = [
    "ConsolidatedMember",
    "NonConsolidatedMember",
]
PreviousCurrentAxis = [
    "PreviousMember",
    "CurrentMember",
]
ResultForecastAxis = [
    "ResultMember",
    "ForecastMember",
    "UpperMember",
    "LowerMember",
]


def _get_context_info(context_id: str):
    """tdnetのcontextIdから情報を取得する"""
    regex = re.compile(
        "({})(Instant|Duration)({})({})({})({})".format(
            "|".join(context_ids),
            "|".join(["_" + txt for txt in AnnualDividendPaymentScheduleAxi] + [""]),
            "|".join(["_" + txt for txt in ConsolidatedNonconsolidatedAxis] + [""]),
            "|".join(["_" + txt for txt in PreviousCurrentAxis] + [""]),
            "|".join(["_" + txt for txt in ResultForecastAxis] + [""]),
        )
    )
    res = regex.search(context_id)
    if res is None:
        raise ValueError(f"Invalid context_id: {context_id}")

    period = res.group(1)
    duration = 12  # yearly
    if "AccumulatedQ1" in period:
        duration = 3
    elif "AccumulatedQ2" in period:
        duration = 6
    elif "AccumulatedQ3" in period:
        duration = 9

    if period.startswith("Current"):  # 現在の期間
        period = 0
    elif period.startswith("Next2"):  # 次の期間
        period = 2
    elif period.startswith("Next"):  # 次の期間
        period = 1
    elif period.startswith("Prior"):  # 前の期間
        period = -1
    else:
        raise ValueError(f"Invalid period in context_id: {context_id}")

    is_consolidated = True
    if res.group(4) == "_NonConsolidatedMember":
        is_consolidated = False

    forecast = res.group(6)
    if len(forecast) == 0:
        forecast = "ResultMember"
    else:
        forecast = forecast[1:]
    return period, duration, is_consolidated, forecast


def _collect_data(
    ixbrl_path: Path,
) -> tuple[dict[str, str], dict[tuple, dict[str, str]]]:
    """ixbrlファイルからデータを抽出する"""
    regex = re.compile(
        "^tse-([asq][cn]|)(edjp|edus|edif|rvdf|rvfc)(sm|sy|).*-ixbrl.htm"
    )  # edit|rejp|rrdf|rrfc|efjp は対象外

    res = regex.search(ixbrl_path.name)
    if res is None:
        return None, None
    report_type = res.group(2)
    target_taxonomy = get_target_taxonomy(report_type)

    ctrl = Cntlr.Cntlr(logFileName="logToPrint")
    model_xbrl = ctrl.modelManager.load(Path(ixbrl_path).as_posix())
    data = defaultdict(dict)
    report_info = {
        "report_type": report_type,
        # "period": res.group(1)[0],
        # "consolidated": res.group(1)[1],
    }
    report_keys = ["filing_date", "code", "fiscal_year_end", "quarterly_period"]

    for key, value in target_taxonomy.items():
        for fact in model_xbrl.facts:
            if str(fact.qname) in value and str(fact.value) != "":
                if key in report_keys:
                    report_info[key] = fact.value
                else:
                    period, duration, is_consolidated, forecast = _get_context_info(
                        fact.contextID
                    )
                    data[(period, duration, is_consolidated, forecast)][key] = (
                        fact.value
                    )
    return report_info, data


def _get_period(
    fiscal_year_end: str, diff: int, duration: int
) -> tuple[datetime.date, datetime.date]:
    """期の開始日と終了日を取得する"""
    year_end = datetime.datetime.strptime(fiscal_year_end, "%Y-%m-%d").date()
    year_end = year_end + diff * relativedelta(years=1)  # diff年分ずらす
    start = year_end - relativedelta(months=11)
    start = start.replace(day=1)  # 月初にする
    end = year_end - relativedelta(months=12 - duration)
    end = end.replace(day=calendar.monthrange(end.year, end.month)[1])
    return start, end


def create_df(data_dir: Path, report_date: datetime.date):
    rows = []
    columns = list(target_english_labels.keys()) + [
        "is_consolidated",
        "is_forecast",
        "duration",
        "period_start",
        "period_end",
        "period_pos",
    ]
    for ixbrl_path in sorted(data_dir.rglob("*-ixbrl.htm")):
        report_info, data = _collect_data(ixbrl_path)
        if report_info is None or data is None:
            continue

        # report_infoの情報をdataに追加する
        for key in data.keys():
            for key2 in ["filing_date", "code", "fiscal_year_end", "quarterly_period"]:
                if key2 in report_info:
                    data[key][key2] = report_info[key2]
                    if key2 == "fiscal_year_end":
                        dt = datetime.datetime.strptime(
                            report_info[key2], "%Y-%m-%d"
                        ).date()
                        data[key][key2] = (
                            dt + key[0] * relativedelta(years=1)
                        ).strftime("%Y-%m-%d")

            data[key]["filing_date"] = report_date.strftime("%Y-%m-%d")
            data[key]["is_consolidated"] = key[2]
            data[key]["is_forecast"] = key[3] != "ResultMember"
            data[key]["duration"] = key[1]
            start, end = _get_period(report_info["fiscal_year_end"], key[0], key[1])
            data[key]["period_start"] = start.strftime("%Y-%m-%d")
            data[key]["period_end"] = end.strftime("%Y-%m-%d")
            data[key]["period_pos"] = key[0]
            rows.append(data[key])

    df = pl.from_dicts(
        rows, schema=columns, schema_overrides={col: pl.Utf8 for col in columns}
    )
    return df
