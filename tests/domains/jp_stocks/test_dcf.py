import pytest

from data_fetcher.domains.jp_stocks.dcf import compute_dcf


def test_compute_dcf_matches_hand_calculation():
    result = compute_dcf(net_cash=2000.0, fcf_base=1000.0, shares_outstanding=1_000_000)

    assert result.bear_total == pytest.approx(12000.0)
    assert result.bull_total == pytest.approx(23991.12082508024)
    assert result.bear_per_share == pytest.approx(12000.0 * 1e6 / 1_000_000)
    assert result.bull_per_share == pytest.approx(23991.12082508024 * 1e6 / 1_000_000)


def test_compute_dcf_negative_fcf_returns_null_scenarios():
    result = compute_dcf(net_cash=2000.0, fcf_base=-500.0, shares_outstanding=1_000_000)
    assert result.bear_total is None
    assert result.bear_per_share is None
    assert result.bull_total is None
    assert result.bull_per_share is None


def test_compute_dcf_zero_fcf_returns_null_scenarios():
    result = compute_dcf(net_cash=2000.0, fcf_base=0.0, shares_outstanding=1_000_000)
    assert result.bear_total is None


def test_compute_dcf_missing_net_cash_returns_null():
    result = compute_dcf(net_cash=None, fcf_base=1000.0, shares_outstanding=1_000_000)
    assert result.bear_total is None


def test_compute_dcf_missing_shares_outstanding_leaves_per_share_null():
    result = compute_dcf(net_cash=2000.0, fcf_base=1000.0, shares_outstanding=None)
    assert result.bear_total == pytest.approx(12000.0)
    assert result.bear_per_share is None
