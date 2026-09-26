"""Peer selection : same 33-sector code, nearest 5 by
market cap. Ticker-master lookups are separated from the ranking so the
ranking itself stays a pure, easily-testable function.
"""

from __future__ import annotations

import polars as pl

from ...core.constants import JP_TICKERS_PATH


def load_ticker_master(path=JP_TICKERS_PATH) -> pl.DataFrame:
    return pl.read_csv(path, schema_overrides={"コード": pl.Utf8})


def get_company_info(ticker: str, master: pl.DataFrame | None = None) -> dict | None:
    master = master if master is not None else load_ticker_master()
    rows = master.filter(pl.col("コード") == ticker)
    if rows.height == 0:
        return None
    row = rows.row(0, named=True)
    return {
        "company_name": row["銘柄名"],
        "sector33_code": row["33業種コード"],
        "sector33_name": row["33業種区分"],
        "market": row["市場・商品区分"],
    }


def tickers_in_sector(
    sector33_code: str, master: pl.DataFrame | None = None
) -> list[str]:
    master = master if master is not None else load_ticker_master()
    return master.filter(pl.col("33業種コード") == sector33_code)["コード"].to_list()


def select_peers_by_market_cap(
    ticker: str, market_caps: dict[str, float | None], n: int = 5
) -> list[str]:
    """自社を除き、時価総額が近い順にn社選ぶ。自社の時価総額が
    不明なら選定できないため空リストを返す。
    """
    own_cap = market_caps.get(ticker)
    if own_cap is None:
        return []
    candidates = [
        (candidate, cap)
        for candidate, cap in market_caps.items()
        if candidate != ticker and cap is not None
    ]
    candidates.sort(key=lambda pair: abs(pair[1] - own_cap))
    return [candidate for candidate, _ in candidates[:n]]
