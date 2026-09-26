import pytest

from data_fetcher.domains.jp_stocks.ratios import (
    consecutive_loss_years,
    cycle_stats,
    debt_to_ebitda,
    ebitda,
    growth_series,
    interest_bearing_debt,
    net_cash,
    roa_series,
    roe_series,
    roic,
)


def test_interest_bearing_debt_sums_present_components_and_treats_all_none_as_null():
    assert interest_bearing_debt(100.0, None, 50.0, None, None, 10.0) == 160.0
    assert interest_bearing_debt(None, None, None, None, None, None) is None


def test_net_cash_formula():
    assert net_cash(cash=100.0, securities_current=50.0, debt=80.0) == 70.0
    assert net_cash(cash=100.0, securities_current=None, debt=80.0) == 20.0
    assert net_cash(cash=None, securities_current=50.0, debt=80.0) is None


def test_ebitda_and_debt_to_ebitda():
    assert ebitda(100.0, 20.0) == 120.0
    assert debt_to_ebitda(240.0, 120.0) == 2.0
    assert debt_to_ebitda(240.0, -10.0) is None
    assert debt_to_ebitda(240.0, None) is None


def test_roic_formula():
    # operating_income*(1-tax_rate) / (debt+net_assets)
    result = roic(operating_income=100.0, debt=400.0, net_assets=600.0, tax_rate=0.30)
    assert result == pytest.approx(70.0 / 1000.0)


def test_roe_series_uses_average_of_beginning_and_ending_equity():
    net_income = [None, 100.0, 110.0]
    equity = [900.0, 1000.0, 1100.0]
    result = roe_series(net_income, equity)
    assert result[0] is None
    assert result[1] == pytest.approx(100.0 / ((900.0 + 1000.0) / 2))
    assert result[2] == pytest.approx(110.0 / ((1000.0 + 1100.0) / 2))


def test_roa_series_uses_average_of_beginning_and_ending_assets():
    net_income = [100.0]
    assets = [2000.0]
    result = roa_series(net_income, assets)
    # No prior year available -> average falls back to the single known value.
    assert result[0] == pytest.approx(100.0 / 2000.0)


def test_growth_series_first_element_is_null_and_zero_prior_is_null():
    values = [100.0, 110.0, 0.0, 50.0]
    result = growth_series(values)
    assert result[0] is None
    assert result[1] == pytest.approx(0.10)
    assert result[3] is None  # prior (index 2) is 0


def test_growth_series_null_if_prior_negative_flag():
    values = [-10.0, 5.0]
    assert growth_series(values, null_if_prior_negative=True) == [None, None]
    assert growth_series(values, null_if_prior_negative=False)[1] == pytest.approx((5.0 - -10.0) / -10.0)


def test_cycle_stats_median_latest_and_percentile():
    series = [0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.11, 0.12, 0.13, 0.14]
    result = cycle_stats(series, median_years=10, percentile_years=10)
    assert result.median_10y == pytest.approx(0.095)
    assert result.latest == pytest.approx(0.14)
    assert result.percentile_latest == pytest.approx(1.0)  # it's the max


def test_cycle_stats_all_null_series_returns_all_null():
    result = cycle_stats([None, None])
    assert result.median_10y is None
    assert result.latest is None
    assert result.percentile_latest is None


def test_consecutive_loss_years_counts_from_the_end():
    assert consecutive_loss_years([100.0, -5.0, -3.0, -1.0]) == 3
    assert consecutive_loss_years([-5.0, -3.0, 1.0]) == 0
    assert consecutive_loss_years([-5.0, -3.0, None]) == 0
