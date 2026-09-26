import datetime as dt

import polars as pl
import pytest

from data_fetcher.domains.reports.trades import compute_trade_result, remaining_shares

_TRADES = pl.DataFrame(
    {
        "date": [dt.date(2026, 1, 10), dt.date(2026, 2, 10), dt.date(2026, 6, 1)],
        "side": ["buy", "buy", "sell"],
        "qty": [100, 100, 200],
        "price": [1000.0, 1200.0, 1500.0],
        "fee": [50.0, 50.0, 100.0],
    }
)


def test_remaining_shares_zero_when_fully_sold():
    assert remaining_shares(_TRADES) == 0


def test_remaining_shares_positive_when_still_holding():
    trades = _TRADES.filter(pl.col("side") == "buy")
    assert remaining_shares(trades) == 200


def test_compute_trade_result_weighted_averages_and_return():
    result = compute_trade_result(_TRADES)
    assert result.avg_buy_price == pytest.approx((100 * 1000.0 + 100 * 1200.0) / 200)
    assert result.avg_sell_price == pytest.approx(1500.0)

    buy_total = 100 * 1000.0 + 100 * 1200.0
    sell_total = 200 * 1500.0
    total_fee = 50.0 + 50.0 + 100.0
    expected_return = (sell_total - total_fee - buy_total) / buy_total
    assert result.return_pct == pytest.approx(expected_return)
    assert result.holding_days == (dt.date(2026, 6, 1) - dt.date(2026, 1, 10)).days


def test_compute_trade_result_max_drawdown_and_benchmark():
    price_history = pl.DataFrame(
        {
            "date": [dt.date(2026, 1, 10), dt.date(2026, 3, 1), dt.date(2026, 6, 1)],
            "close": [1000.0, 800.0, 1500.0],
        }
    )
    benchmark_history = pl.DataFrame(
        {
            "date": [dt.date(2026, 1, 10), dt.date(2026, 6, 1)],
            "close": [2000.0, 2200.0],
        }
    )
    result = compute_trade_result(_TRADES, price_history, benchmark_history)
    avg_buy = (100 * 1000.0 + 100 * 1200.0) / 200
    assert result.max_drawdown_pct == pytest.approx((800.0 - avg_buy) / avg_buy)
    assert result.benchmark_return_pct == pytest.approx((2200.0 - 2000.0) / 2000.0)
    assert result.excess_return_pct == pytest.approx(result.return_pct - result.benchmark_return_pct)


def test_compute_trade_result_max_drawdown_is_clamped_to_zero_when_never_underwater():
    price_history = pl.DataFrame(
        {
            "date": [dt.date(2026, 1, 10), dt.date(2026, 3, 1), dt.date(2026, 6, 1)],
            "close": [1200.0, 1300.0, 1500.0],  # always above avg_buy (1100)
        }
    )
    result = compute_trade_result(_TRADES, price_history)
    assert result.max_drawdown_pct == 0.0


def test_compute_trade_result_still_holding_has_no_return_or_holding_days():
    trades = _TRADES.filter(pl.col("side") == "buy")
    result = compute_trade_result(trades)
    assert result.avg_sell_price is None
    assert result.holding_days is None
