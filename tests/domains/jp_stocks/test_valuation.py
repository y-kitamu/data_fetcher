from data_fetcher.domains.jp_stocks.valuation import (
    ActualForecast,
    compute_valuation_ratios,
    market_cap,
    pbr,
    per,
    roa,
    roe,
)


def test_compute_valuation_ratios_computes_per_pbr_roe_roa() -> None:
    ratios = compute_valuation_ratios(
        price=100.0,
        eps=ActualForecast(actual=10.0, forecast=20.0),
        bps=ActualForecast(actual=50.0),
        net_income=ActualForecast(actual=100.0, forecast=200.0),
        net_assets_actual=1000.0,
        total_assets_actual=2000.0,
    )
    assert ratios.per.actual == 10.0  # 100 / 10
    assert ratios.per.forecast == 5.0  # 100 / 20
    assert ratios.pbr.actual == 2.0  # 100 / 50
    assert ratios.roe.actual == 10.0  # 100 / 1000 * 100
    assert ratios.roe.forecast == 20.0  # 200 / 1000 * 100
    assert ratios.roa.actual == 5.0  # 100 / 2000 * 100
    assert ratios.roa.forecast == 10.0  # 200 / 2000 * 100


def test_compute_valuation_ratios_without_price_leaves_ratios_none() -> None:
    ratios = compute_valuation_ratios(
        price=None,
        eps=ActualForecast(actual=10.0),
        bps=ActualForecast(actual=50.0),
        net_income=ActualForecast(actual=100.0),
        net_assets_actual=None,
        total_assets_actual=None,
    )
    assert ratios.per.actual is None
    assert ratios.pbr.actual is None


def test_per_none_when_price_or_eps_missing() -> None:
    assert per(None, 10.0) is None
    assert per(100.0, None) is None
    assert per(100.0, 0.0) is None
    assert per(100.0, 10.0) == 10.0


def test_pbr_none_when_price_or_bps_missing() -> None:
    assert pbr(None, 50.0) is None
    assert pbr(100.0, None) is None
    assert pbr(100.0, 50.0) == 2.0


def test_roe_none_when_net_assets_missing_but_zero_income_computes() -> None:
    assert roe(100.0, None) is None
    assert roe(100.0, 0.0) is None
    # Existing behaviour: only net_assets_actual is truthy-guarded; net_income
    # of exactly 0 still yields a computed (zero) ratio, since the guard is
    # `net_income is None`, not a truthiness check.
    assert roe(0.0, 1000.0) == 0.0
    assert roe(None, 1000.0) is None


def test_roa_none_when_total_assets_missing_but_zero_income_computes() -> None:
    assert roa(100.0, None) is None
    assert roa(100.0, 0.0) is None
    assert roa(0.0, 2000.0) == 0.0
    assert roa(None, 2000.0) is None


def test_market_cap_none_when_price_or_shares_missing() -> None:
    assert market_cap(None, 1000.0) is None
    assert market_cap(100.0, None) is None
    assert market_cap(100.0, 1000.0) == 100_000.0
