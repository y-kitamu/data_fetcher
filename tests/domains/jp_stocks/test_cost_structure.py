import pytest

from data_fetcher.domains.jp_stocks.cost_structure import fit_cost_structure


def test_fit_cost_structure_recovers_exact_linear_relationship():
    revenue = [100.0, 110.0, 120.0, 130.0, 140.0, 150.0]
    # opex = 20 + 0.7*revenue  =>  operating_income = 0.3*revenue - 20
    operating_income = [0.3 * r - 20 for r in revenue]

    result = fit_cost_structure(revenue, operating_income)

    assert result.n_years == 6
    assert result.fixed_cost == pytest.approx(20.0, abs=1e-6)
    assert result.variable_cost_ratio == pytest.approx(0.7, abs=1e-6)
    assert result.r2 == pytest.approx(1.0, abs=1e-6)
    assert result.breakeven_revenue == pytest.approx(20.0 / 0.3, abs=1e-6)
    assert result.latest_revenue == 150.0
    assert result.gap_to_breakeven_pct == pytest.approx((20.0 / 0.3 - 150.0) / 150.0, abs=1e-6)


def test_fit_cost_structure_below_minimum_years_returns_null():
    revenue = [100.0, 110.0, 120.0, 130.0, 140.0]  # only 5 years
    operating_income = [0.3 * r - 20 for r in revenue]

    result = fit_cost_structure(revenue, operating_income)

    assert result.n_years == 5
    assert result.fixed_cost is None
    assert result.variable_cost_ratio is None
    assert result.breakeven_revenue is None
    assert result.latest_revenue == 140.0


def test_fit_cost_structure_uses_only_last_10_years():
    # 5 wildly inconsistent "too old" years, followed by exactly 10 years
    # (the window size) that follow the true 20/0.7 linear relationship. If
    # the old years leaked into the fit, the recovered parameters would be
    # nowhere near 20/0.7.
    old_revenue = [1000.0] * 5
    old_operating_income = [-999_999.0] * 5
    recent_revenue = [100.0 + 10.0 * i for i in range(10)]
    recent_operating_income = [0.3 * r - 20 for r in recent_revenue]

    result = fit_cost_structure(
        old_revenue + recent_revenue, old_operating_income + recent_operating_income
    )
    assert result.n_years == 10
    assert result.fixed_cost == pytest.approx(20.0, abs=1e-6)
