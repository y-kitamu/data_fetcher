"""Trade history aggregation for exit reports (section 9.3): trades.csv
(date,side,qty,price,fee) -> quantity-weighted average prices, realized
return, holding days, and max drawdown while held.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from pydantic import BaseModel


class TradeResult(BaseModel):
    avg_buy_price: float | None
    avg_sell_price: float | None
    return_pct: float | None
    holding_days: int | None
    benchmark: str = "TOPIX"
    benchmark_return_pct: float | None = None
    excess_return_pct: float | None = None
    max_drawdown_pct: float | None = None


def read_trades_csv(path: Path) -> pl.DataFrame:
    return pl.read_csv(path, try_parse_dates=True)


def remaining_shares(trades: pl.DataFrame) -> float:
    bought = trades.filter(pl.col("side") == "buy")["qty"].sum() if trades.height else 0.0
    sold = trades.filter(pl.col("side") == "sell")["qty"].sum() if trades.height else 0.0
    return bought - sold


def _weighted_average_price(side_trades: pl.DataFrame) -> float | None:
    if side_trades.height == 0:
        return None
    total_qty = side_trades["qty"].sum()
    if not total_qty:
        return None
    return float((side_trades["qty"] * side_trades["price"]).sum() / total_qty)


def _window_return(history: pl.DataFrame, start, end) -> float | None:
    window = history.filter((pl.col("date") >= start) & (pl.col("date") <= end)).sort("date")
    if window.height < 2:
        return None
    start_price, end_price = window["close"][0], window["close"][-1]
    return (end_price - start_price) / start_price if start_price else None


def compute_trade_result(
    trades: pl.DataFrame,
    price_history: pl.DataFrame | None = None,
    benchmark_history: pl.DataFrame | None = None,
    benchmark: str = "TOPIX",
) -> TradeResult:
    """`price_history`/`benchmark_history` は列 date, close を持つDataFrame
    (保有期間を含む範囲であればよい)。
    """
    buys = trades.filter(pl.col("side") == "buy")
    sells = trades.filter(pl.col("side") == "sell")
    avg_buy = _weighted_average_price(buys)
    avg_sell = _weighted_average_price(sells)

    buy_total = float((buys["qty"] * buys["price"]).sum()) if buys.height else 0.0
    sell_total = float((sells["qty"] * sells["price"]).sum()) if sells.height else 0.0
    total_fee = float(trades["fee"].sum()) if "fee" in trades.columns and trades.height else 0.0
    return_pct = (sell_total - total_fee - buy_total) / buy_total if buy_total else None

    holding_days: int | None = None
    max_drawdown_pct: float | None = None
    benchmark_return_pct: float | None = None
    if buys.height and sells.height:
        first_buy, last_sell = buys["date"].min(), sells["date"].max()
        holding_days = (last_sell - first_buy).days

        if price_history is not None and avg_buy:
            held = price_history.filter(
                (pl.col("date") >= first_buy) & (pl.col("date") <= last_sell)
            )
            if held.height:
                # "最大含み損率": 一度も取得単価を下回らなかった場合は
                # 含み損自体が発生していない(0)ため、正の値にはしない。
                max_drawdown_pct = min(0.0, (float(held["close"].min()) - avg_buy) / avg_buy)

        if benchmark_history is not None:
            benchmark_return_pct = _window_return(benchmark_history, first_buy, last_sell)

    excess_return_pct = (
        return_pct - benchmark_return_pct
        if return_pct is not None and benchmark_return_pct is not None
        else None
    )

    return TradeResult(
        avg_buy_price=avg_buy,
        avg_sell_price=avg_sell,
        return_pct=return_pct,
        holding_days=holding_days,
        benchmark=benchmark,
        benchmark_return_pct=benchmark_return_pct,
        excess_return_pct=excess_return_pct,
        max_drawdown_pct=max_drawdown_pct,
    )
