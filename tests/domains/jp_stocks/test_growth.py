import pytest

from data_fetcher.domains.jp_stocks.growth import peak_to_peak_revenue_cagr


def test_peak_to_peak_revenue_cagr_matches_hand_calculation():
    # Local peak at index 2 (120 > 100,110 and > 115,90), global peak at
    # index 6 (200). 4 years between them: (200/120)^(1/4) - 1.
    revenue = [100.0, 110.0, 120.0, 115.0, 90.0, 150.0, 200.0]
    result = peak_to_peak_revenue_cagr(revenue)
    assert result == pytest.approx((200.0 / 120.0) ** (1 / 4) - 1)


def test_peak_to_peak_revenue_cagr_no_prior_local_peak_returns_null():
    # Monotonically increasing: the global peak has no local peak before it.
    revenue = [100.0, 110.0, 120.0, 130.0, 140.0]
    assert peak_to_peak_revenue_cagr(revenue) is None


def test_peak_to_peak_revenue_cagr_too_few_points_returns_null():
    assert peak_to_peak_revenue_cagr([100.0, 110.0, None, None]) is None
