import pytest

from data_fetcher.domains.jp_stocks.normalized_value import compute_normalized_value


def test_compute_normalized_value_matches_hand_calculation():
    revenue = [100.0] * 9 + [200.0]
    operating_margin = [0.05] * 9 + [0.10]
    depreciation_ratio = [0.02] * 10
    capex_ratio = [0.03] * 10

    result = compute_normalized_value(
        revenue=revenue,
        operating_margin=operating_margin,
        depreciation_ratio=depreciation_ratio,
        capex_ratio=capex_ratio,
        net_cash=500.0,
        shares_outstanding=1_000_000,
    )

    assert result.median_operating_margin == pytest.approx(0.05)
    assert result.normalized_operating_income == pytest.approx(10.0)  # 200 * 0.05
    assert result.normalized_fcf == pytest.approx(5.0)  # 10*0.7 + (0.02-0.03)*200
    assert result.value_per_share == pytest.approx(550.0 * 1e6 / 1_000_000)

    assert result.peak_operating_margin == pytest.approx(0.10)
    assert result.peak_fcf == pytest.approx(12.0)  # 20*0.7 + (0.02-0.03)*200
    assert result.peak_value_per_share == pytest.approx(620.0 * 1e6 / 1_000_000)


def test_compute_normalized_value_missing_revenue_is_null():
    result = compute_normalized_value(
        revenue=[None] * 10,
        operating_margin=[0.05] * 10,
        depreciation_ratio=[0.02] * 10,
        capex_ratio=[0.03] * 10,
        net_cash=500.0,
        shares_outstanding=1_000_000,
    )
    assert result.normalized_operating_income is None
    assert result.value_per_share is None
