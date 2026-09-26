import pytest

from data_fetcher.domains.jp_stocks.cycle import (
    ar1_half_life,
    industry_median_series,
    peer_margin_deterioration_share,
)


def _noiseless_ar1_series(mu: float, phi: float, m0: float, n: int) -> list[float]:
    values = [m0]
    for _ in range(n - 1):
        values.append(mu + phi * (values[-1] - mu))
    return values


def test_ar1_half_life_recovers_known_phi_from_noiseless_series():
    series = _noiseless_ar1_series(mu=0.10, phi=0.5, m0=0.20, n=60)
    result = ar1_half_life(series)

    assert result.phi == pytest.approx(0.5, abs=0.05)
    assert result.half_life_years == pytest.approx(1.0, abs=0.2)


def test_ar1_half_life_returns_null_when_phi_outside_zero_one():
    # phi > 1: explosive, mean-reversion undefined.
    series = _noiseless_ar1_series(mu=0.10, phi=1.5, m0=0.11, n=15)
    result = ar1_half_life(series)
    assert result.half_life_years is None


def test_ar1_half_life_returns_null_when_fewer_than_min_years():
    series = _noiseless_ar1_series(mu=0.10, phi=0.5, m0=0.20, n=8)
    result = ar1_half_life(series)
    assert result.phi is None
    assert result.half_life_years is None


def test_industry_median_series_computes_per_year_median():
    company_a = [0.10, 0.20, None]
    company_b = [0.20, 0.10, 0.05]
    company_c = [0.30, None, 0.15]
    result = industry_median_series([company_a, company_b, company_c])
    assert result == pytest.approx([0.20, 0.15, 0.10])


def test_peer_margin_deterioration_share_counts_peers_below_prior_and_median():
    latest = [0.03, 0.10, 0.02]
    prior = [0.05, 0.08, 0.01]
    median_10y = [0.06, 0.09, 0.015]
    # peer 0: 0.03 < 0.05 and 0.03 < 0.06 -> True
    # peer 1: 0.10 < 0.08 is False -> False
    # peer 2: 0.02 < 0.01 is False -> False
    result = peer_margin_deterioration_share(latest, prior, median_10y)
    assert result == pytest.approx(1 / 3)


def test_peer_margin_deterioration_share_null_when_no_peer_judgeable():
    result = peer_margin_deterioration_share([None], [None], [None])
    assert result is None
