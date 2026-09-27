"""I/O orchestration for the report generator: wraps gateway/reader calls and
applies point-in-time filtering . Kept separate from
snapshot_builder.py so the builder's calculation-orchestration logic can be
tested against fake data_access functions without touching the filesystem.
"""

from __future__ import annotations

import datetime as dt

import polars as pl
from loguru import logger

from ... import gateway
from ..tdnet.capital_action_extraction import CAPITAL_ACTIONS_DIR
from ..tdnet.disclosure_list import DISCLOSURES_DIR
from ..tdnet.financial_periods import FinancialPeriod, shape_financial_periods
from ..tdnet.statement_periods import (
    StatementPeriod,
    shape_statement_periods,
)
from .pit import filter_available, is_available_at


def _get_financials_or_empty(ticker: str, source: str) -> pl.DataFrame:
    try:
        return gateway.get_financials(ticker, source=source).get(source, pl.DataFrame())
    except ValueError:
        return pl.DataFrame()
    except Exception as exc:
        # 個別銘柄のCSVが壊れている/読み込み時に想定外の型推論エラーになる
        # ケースがある(既知の問題。完了報告を参照)。同業選定などで多数の
        # 銘柄を横断的に読む処理が1銘柄のデータ不良で全体停止しないよう、
        # 該当銘柄だけデータなし扱いにする。
        logger.warning(f"Failed to read {source} financials for {ticker}: {exc}")
        return pl.DataFrame()


def get_statement_periods(
    ticker: str, as_of: dt.date
) -> tuple[list[StatementPeriod], list[StatementPeriod]]:
    """TDnet(第1ソース)とEDINET(補完ソース)のas_of時点PITフィルタ済み
    StatementPeriodを返す。
    """
    tdnet_periods = shape_statement_periods(_get_financials_or_empty(ticker, "tdnet"))
    tdnet_periods = filter_available(
        tdnet_periods, as_of, disclosed_at=lambda p: p.submitted_at
    )

    edinet_periods = shape_statement_periods(
        _get_financials_or_empty(ticker, "edinet_financial"), source="edinet"
    )
    edinet_periods = filter_available(
        edinet_periods, as_of, disclosed_at=lambda p: p.submitted_at
    )

    return tdnet_periods, edinet_periods


def resolve_shares_outstanding(
    tdnet_periods: list[StatementPeriod],
) -> tuple[float | None, float | None, float | None]:
    """as_of時点で最新の開示から発行済株式数・自己株式数を解決する。
    `tdnet_periods`はPIT済み・submitted_at降順ソート済みであること
    (get_statement_periodsの戻り値がそのままこの前提を満たす)。

    Returns: (shares_issued, treasury_shares, shares_outstanding)
    """
    for period in tdnet_periods:
        shares_issued = period.shares.get("number_of_shares")
        if shares_issued is not None:
            treasury = period.shares.get("treasury_shares") or 0.0
            return shares_issued, treasury, shares_issued - treasury
    return None, None, None


def get_disclosures(
    ticker: str, as_of: dt.date, lookback_months: int = 24
) -> list[dict]:
    """as_of前lookback_months分の開示メタデータ(新しい順)。存在しない
    ticker(まだ一件も開示メタデータが取れていない銘柄)は空リストを返す。
    """
    path = DISCLOSURES_DIR / f"{ticker}.csv"
    if not path.exists():
        return []
    cutoff_start = as_of - dt.timedelta(days=lookback_months * 30)
    rows = []
    for row in pl.read_csv(path, schema_overrides={"code": pl.Utf8}).to_dicts():
        disclosed_at = dt.datetime.fromisoformat(row["disclosed_at"])
        if (
            not is_available_at(disclosed_at, as_of)
            or disclosed_at.date() < cutoff_start
        ):
            continue
        rows.append({**row, "disclosed_at": disclosed_at})
    rows.sort(key=lambda r: r["disclosed_at"], reverse=True)
    return rows


def get_capital_actions(ticker: str, as_of: dt.date) -> list[dict]:
    path = CAPITAL_ACTIONS_DIR / f"{ticker}.csv"
    if not path.exists():
        return []
    rows = []
    for row in pl.read_csv(path, schema_overrides={"code": pl.Utf8}).to_dicts():
        disclosed_at = dt.datetime.fromisoformat(row["disclosed_at"])
        if not is_available_at(disclosed_at, as_of):
            continue
        rows.append({**row, "disclosed_at": disclosed_at})
    rows.sort(key=lambda r: r["disclosed_at"])
    return rows


def get_price_history(ticker: str, as_of: dt.date) -> pl.DataFrame:
    """as_of以前のOHLC日足を返す。列: date, close。kabutan優先(gatewayの
    既定優先順位: kabutan -> yfinance)。"""
    end = dt.datetime.combine(as_of, dt.time(23, 59, 59))
    try:
        dfs = gateway.get_ohlc(ticker, interval=dt.timedelta(days=1), end_date=end)
    except Exception as exc:
        logger.warning(f"Failed to read OHLC for {ticker}: {exc}")
        return pl.DataFrame({"date": [], "close": []})

    for df in dfs.values():
        df = next(iter(dfs.values()))
        if df.height > 0:
            return df.select(
                pl.col("datetime").dt.date().alias("date"), pl.col("close")
            ).drop_nulls("close")
    return pl.DataFrame({"date": [], "close": []})


def compute_peak_price(
    price_history: pl.DataFrame, as_of: dt.date, years: int = 10
) -> tuple[float | None, str | None]:
    """as_of以前years年の最高終値とその日付(未分割調整。price_historyに
    split_ratioが未知の分割が含まれる場合は呼び出し側でwarningを残すこと)。
    """
    if price_history.height == 0:
        return None, None
    start = as_of.replace(year=as_of.year - years)
    window = price_history.filter((pl.col("date") >= start) & (pl.col("date") <= as_of))
    if window.height == 0:
        return None, None
    peak_row = window.sort("close", descending=True).head(1)
    return float(peak_row["close"][0]), peak_row["date"][0].isoformat()


def get_financial_periods_summary(ticker: str, as_of: dt.date) -> list[FinancialPeriod]:
    """会社予想・実績サマリー(TTM計算・近似PER用)。financial_periods.py の
    既存パイプラインをそのまま再利用する。
    """
    periods = shape_financial_periods(_get_financials_or_empty(ticker, "tdnet"))
    return filter_available(periods, as_of, disclosed_at=lambda p: p.submitted_at)


def get_large_shareholding(ticker: str, as_of: dt.date) -> pl.DataFrame:
    dfs = gateway.get_large_shareholding(symbol=ticker, end_date=as_of)
    return dfs.get("edinet_large_shareholding", pl.DataFrame())


def get_latest_price(ticker: str, as_of: dt.date) -> tuple[float | None, str | None]:
    price_history = get_price_history(ticker, as_of)
    if price_history.height == 0:
        return None, None
    last = price_history.sort("date").tail(1)
    return float(last["close"][0]), last["date"][0].isoformat()


def get_sector_market_caps(
    sector33_code: str, as_of: dt.date, tickers: list[str], limit: int = 20
) -> dict[str, float | None]:
    """同業候補(tickers)ごとの時価総額(百万円)を計算する。
    候補が多い業種でも実行時間を抑えるため、走査件数をlimitで打ち切る
    (打ち切られた候補は市場全体の代表性を保証しない: 実用上の妥協点)。
    """
    market_caps: dict[str, float | None] = {}
    for ticker in tickers[:limit]:
        try:
            price, _ = get_latest_price(ticker, as_of)
            if price is None:
                market_caps[ticker] = None
                continue
            tdnet_periods, _edinet_periods = get_statement_periods(ticker, as_of)
            _issued, _treasury, outstanding = resolve_shares_outstanding(tdnet_periods)
            market_caps[ticker] = price * outstanding / 1e6 if outstanding else None
        except Exception as exc:
            logger.warning(f"Skipping peer candidate {ticker} due to error: {exc}")
            market_caps[ticker] = None
    return market_caps
